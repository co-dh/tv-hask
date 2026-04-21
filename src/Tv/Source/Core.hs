{-# LANGUAGE ScopedTypeVariables #-}
{-
  Source.Core: shared glue for per-source modules.

  Every source needs the same primitives: template expansion, shell safety,
  JSON-to-table loading, caching, setup-once, parent-row insertion, and
  struct auto-expansion. They live here so each source file can focus on
  its own URL/SQL/cmd specifics.
-}
module Tv.Source.Core where

import Tv.Prelude
import Control.Exception (SomeException, try)
import qualified Data.ByteString.Lazy as LBS
import Data.IORef (atomicModifyIORef')
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import GHC.Clock (getMonotonicTime)
import Network.HTTP.Client
  ( Manager, Request, Response, httpLbs, parseRequest, responseBody, responseStatus )
import Network.HTTP.Client.TLS (newTlsManager)
import Network.HTTP.Types.Status (statusCode)
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.IO.Unsafe (unsafePerformIO)
import System.Process (readProcessWithExitCode)

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable, tmpName)
import qualified Tv.Log as Log
import qualified Tv.Render as Render
import qualified Tv.Tmp as Tmp

-- | A remote directory backend: prefix match + two behavior closures.
-- Folder and App.Main treat every source uniformly through this record.
-- Per-source modules (Tv.Source.S3, Tv.Source.Ftp, …) each export a
-- `Source` value built here; `Tv.Source` just aggregates them.
--
-- `open` unifies what used to be `enter`/`enterUrl`/`download`/`resolve`:
-- it takes a fully-joined path (with trailing '/' if the row was a dir)
-- and returns an `OpenResult` telling the caller how to use it.
-- One-time setup (e.g. DuckDB ATTACH, python script) happens inside
-- `list`/`open` via `Core.onceFor` keyed on `pfx`, so the closures
-- stay idempotent without a separate `setup` field.
data Source = Source
  { pfx    :: Text                                     -- URI prefix, e.g. "s3://"
  , parent :: Text -> Maybe Text                       -- parent URI or Nothing at root
  , grpCol :: Maybe Text                               -- default group column (Nothing = none)
  , list   :: Bool -> Text -> IO (Maybe AdbcTable)     -- noSign, path → listing
  , open   :: Bool -> Text -> IO OpenResult            -- noSign, fullPath → what to do
  }

-- | What a source's `open` produced for a path.
data OpenResult
  = OpenAsTable AdbcTable       -- open-row-as-table (osquery script, pg ATTACH table)
  | OpenAsFile  FilePath        -- local path ready for FileFormat.openFile
  | OpenAsDir   Text            -- URI to re-enter as a folder (e.g. "hf://datasets/foo/")
  | OpenNothing                 -- can't open / source doesn't support it

-- | AdbcTable has no Show instance, so we summarise the variant only.
instance Show OpenResult where
  show (OpenAsTable _) = "OpenAsTable <adbc>"
  show (OpenAsFile p)  = "OpenAsFile " <> show p
  show (OpenAsDir u)   = "OpenAsDir "  <> show u
  show OpenNothing     = "OpenNothing"

-- | Expand template placeholders: {path}, {name}, {tmp}, {extra}, {1}, {2+}, …
-- For empty values, also removes a preceding "/" so "tree/main/{3+}" with
-- empty {3+} becomes "tree/main" — no trailing slash artifact.
expand :: Text -> Vector (Text, Text) -> Text
expand tmpl vars = V.foldl' step tmpl vars
  where
    step s (k, v) =
      if T.null v
        then T.replace ("{" <> k <> "}") ""
               (T.replace ("/{" <> k <> "}") "" s)
        else T.replace ("{" <> k <> "}") v s

-- | Split a path into its components under the given prefix (drops trailing /).
pathParts :: Text -> Text -> Vector Text
pathParts pfx_ path_ =
  let rest0 = T.drop (T.length pfx_) path_
      rest  = if T.isSuffixOf "/" rest0 then T.dropEnd 1 rest0 else rest0
  in if T.null rest then V.empty else V.fromList $ T.splitOn "/" rest

-- | Last non-empty segment, or "file" if path is empty.
fromPath :: Text -> Text
fromPath path_ =
  let parts = filter (not . T.null) (T.splitOn "/" path_)
  in if null parts then "file" else last parts

-- | Standard template vars: path/tmp/name/extra/dsn + numbered {1}..{9} and {n+}.
mkVars :: Text -> Text -> Text -> Text -> Text -> Vector (Text, Text)
mkVars pfx_ path_ tmp name extra =
  let parts = pathParts pfx_ path_
      dsn   = T.drop (T.length pfx_) path_
      base  = V.fromList
                [ ("path", path_), ("tmp", tmp), ("name", name)
                , ("extra", extra), ("dsn", dsn) ]
      getD i = fromMaybe "" $ parts V.!? i
      numbered = V.fromList [ (T.pack (show (i + 1)), getD i) | i <- [0 .. 8] ]
      plus = V.fromList
               [ ( T.pack (show (i + 1)) <> "+"
                 , T.intercalate "/" (drop i (V.toList parts))
                 )
               | i <- [0 .. 8] ]
  in base V.++ numbered V.++ plus

-- | Reject shell metacharacters in user-supplied values before template expansion.
-- Blocklist approach: reject only dangerous chars, allow everything else (unicode, #, etc.)
shellMeta :: Text -> Bool
shellMeta s = T.any bad s
  where
    bad c =
      c == '$' || c == '`' || c == ';' || c == '&' || c == '|'
        || c == '(' || c == ')' || c == '!' || c == '{' || c == '}'
        || c == '<' || c == '>' || c == '\\' || c == '"' || c == '\''
        || c == '\n'

checkShell :: Text -> Text -> IO ()
checkShell s label =
  when (shellMeta s) $ do
    Log.write "src" ("rejected unsafe " <> label <> ": " <> s)
    ioError (userError ("Path contains shell metacharacters: " <> T.unpack s))

-- | Swallow exceptions from a best-effort IO action.
ignoreErrs :: IO a -> IO ()
ignoreErrs m = try m >>= \(_ :: Either SomeException a) -> pure ()

-- | Exception-to-Maybe. Replaces the `try (...) :: IO (Either SomeException x)`
-- noise at call sites.
try_ :: forall a. IO a -> IO (Maybe a)
try_ m = do
  r <- try m :: IO (Either SomeException a)
  pure $ either (const Nothing) Just r

homeText :: IO Text
homeText = T.pack . fromMaybe "/tmp" <$> lookupEnv "HOME"

-- | Run a shell cmd; return (exitCode, stdout, stderr).
-- Logs the cmd, lets the caller decide how to handle failures.
runCmd :: Text -> Text -> IO (ExitCode, Text, Text)
runCmd label cmd = do
  Log.write "src" (label <> ": " <> cmd)
  (ec, out, err) <- readProcessWithExitCode "sh" ["-c", T.unpack cmd] ""
  pure (ec, T.pack out, T.pack err)

-- | Run a shell cmd, write its stdout to a fresh tmp file, return the path.
-- Returns Nothing if the cmd fails or produces empty output. Caller is
-- responsible for rm-ing the returned file after use.
writeCmdOut :: Text -> Text -> IO (Maybe FilePath)
writeCmdOut label cmd = do
  (ec, out, err) <- runCmd label cmd
  case ec of
    ExitFailure code -> do
      Log.write "src" ( label <> " failed (exit "
                     <> T.pack (show code) <> "): " <> T.strip err )
      pure Nothing
    ExitSuccess ->
      if T.null (T.strip out)
        then pure Nothing
        else do
          tmpFile <- Tmp.tmpPath "src-list.json"
          TIO.writeFile tmpFile out
          pure (Just tmpFile)

-- | CREATE TEMP TABLE ... AS SELECT * FROM read_json_auto(path).
loadJsonToTbl :: Text -> FilePath -> IO ()
loadJsonToTbl tbl path = do
  _ <- Conn.query
         ( "CREATE TEMP TABLE " <> tbl
        <> " AS SELECT * FROM read_json_auto('" <> T.pack path <> "')" )
  pure ()

-- | Load enter-script JSON into a fresh temp table; return its name.
loadEnterJson :: Text -> IO Text
loadEnterJson json = do
  tbl <- tmpName "src"
  tmpFile <- Tmp.tmpPath ("src-enter-" <> T.unpack tbl <> ".json")
  TIO.writeFile tmpFile json
  loadJsonToTbl tbl tmpFile
  Tmp.rmFile tmpFile
  pure tbl

-- ## HTTP client: lazy process-wide TLS manager.
-- http-client's Manager pools connections, so sharing one across all
-- sources gives keepalive + TLS session reuse. `newTlsManager` uses
-- `tlsManagerSettings` which follows redirects (needed for HF CDN 302s).
{-# NOINLINE mgrRef #-}
mgrRef :: IORef (Maybe Manager)
mgrRef = unsafePerformIO (newIORef Nothing)

httpMgr :: IO Manager
httpMgr = do
  m <- readIORef mgrRef
  case m of
    Just mgr -> pure mgr
    Nothing  -> do
      fresh <- newTlsManager
      -- Race-safe: if another thread already installed one, keep theirs and
      -- drop ours. The stranded `fresh` has no open connections yet.
      atomicModifyIORef' mgrRef $ \cur -> case cur of
        Just existing -> (Just existing, existing)
        Nothing       -> (Just fresh, fresh)

-- | Run a GET, log outcome, return the Response or Nothing on exception
-- or non-2xx status. Caller decides what to do with the body.
httpGet :: Text -> IO (Maybe LBS.ByteString)
httpGet url = do
  Log.write "src" ("http GET: " <> url)
  mReq <- try (parseRequest (T.unpack url)) :: IO (Either SomeException Request)
  case mReq of
    Left e -> do
      Log.write "src" ("http parse failed: " <> T.pack (show e))
      pure Nothing
    Right req -> do
      mgr <- httpMgr
      r <- try (httpLbs req mgr)
             :: IO (Either SomeException (Response LBS.ByteString))
      case r of
        Left e -> do
          Log.write "src" ("http failed: " <> T.pack (show e))
          pure Nothing
        Right resp -> do
          let sc = statusCode (responseStatus resp)
          if sc >= 200 && sc < 300
            then pure (Just (responseBody resp))
            else do
              Log.write "src"
                ("http status " <> T.pack (show sc) <> ": " <> url)
              pure Nothing

-- | Fetch a URL over HTTP/HTTPS, returning body bytes or Nothing on failure.
fetchBytes :: Text -> IO (Maybe LBS.ByteString)
fetchBytes = httpGet

-- | Fetch a URL and write the body to a file (download). Returns True on success.
fetchFile :: Text -> FilePath -> IO Bool
fetchFile url path_ = do
  m <- httpGet url
  case m of
    Nothing   -> pure False
    Just body -> do
      r <- try (LBS.writeFile path_ body) :: IO (Either SomeException ())
      case r of
        Left e -> do
          Log.write "src" ("fetchFile write failed: " <> T.pack (show e))
          pure False
        Right _ -> pure True

-- ## Listing cache: keep newest entries; on overflow drop oldest.
cacheCap, evictKeep :: Int
cacheCap  = 64
evictKeep = 32

{-# NOINLINE listCache #-}
listCache :: IORef (Vector (Text, AdbcTable))
listCache = unsafePerformIO (newIORef V.empty)

cacheLookup :: Text -> IO (Maybe AdbcTable)
cacheLookup path_ = do
  arr <- readIORef listCache
  pure $ fmap snd $ V.find (\(k, _) -> k == path_) arr

cacheStore :: Text -> AdbcTable -> IO ()
cacheStore path_ tbl =
  modifyIORef' listCache $ \arr ->
    let arr' = V.filter (\(k, _) -> k /= path_) arr
        drop_ = V.length arr' - evictKeep
    in if V.length arr' >= cacheCap
         then V.snoc (V.drop drop_ arr') (path_, tbl)
         else V.snoc arr' (path_, tbl)

-- | Listing wrapper: cache hit → return; else run inner, status msg, cache if slow (>3s).
withCache :: Text -> IO (Maybe AdbcTable) -> IO (Maybe AdbcTable)
withCache path_ run = do
  mc <- cacheLookup path_
  case mc of
    Just cached -> do
      Log.write "src" ("runList cache hit: " <> path_)
      pure (Just cached)
    Nothing -> do
      Render.statusMsg ("Loading " <> path_ <> " ...")
      t0 <- getMonotonicTime
      result <- run
      case result of
        Just adbc -> do
          t1 <- getMonotonicTime
          let elapsedMs = (t1 - t0) * 1000.0
          when (elapsedMs > 3000) $ do
            Log.write "src" ( "runList cached ("
                           <> T.pack (show (round elapsedMs :: Int))
                           <> "ms): " <> path_ )
            cacheStore path_ adbc
        Nothing -> pure ()
      pure result

-- ## Run-once ledger: keyed actions run at most once per process.
{-# NOINLINE onceDone #-}
onceDone :: IORef (Vector Text)
onceDone = unsafePerformIO (newIORef V.empty)

-- | Run `act` the first time for `key`; subsequent calls are no-ops.
onceFor :: Text -> IO () -> IO ()
onceFor key act = do
  done <- readIORef onceDone
  if V.elem key done
    then pure ()
    else do
      act
      modifyIORef' onceDone (`V.snoc` key)

-- | Add ".." parent row when parent nav is allowed and table has folder cols.
-- Silent on failure — tables without name/size/date/type cols just skip it.
addParentRow :: Maybe Text -> Text -> IO ()
addParentRow parentOk tbl = case parentOk of
  Nothing -> pure ()
  Just _  -> ignoreErrs $ do
    _ <- Conn.query ( "INSERT INTO " <> tbl
                   <> " SELECT '..' as name, 0 as size, '' as date, 'dir' as type" )
    pure ()

prqlRun :: Text -> IO (Maybe Conn.QueryResult)
prqlRun q = do
  m <- Prql.compile q
  case m of
    Nothing  -> pure Nothing
    Just sql -> Just <$> Conn.query sql

-- | If listing produced 1 row with a single struct[] column, expand it.
-- Handles APIs that wrap rows in an envelope (e.g. S3 Contents).
unnestStruct :: Text -> IO ()
unnestStruct tbl = ignoreErrs go
  where
    go = do
      qrM  <- prqlRun ("from dcols | struct_col '" <> tbl <> "'")
      qr   <- maybe (ioError (userError "struct_col PRQL failed")) pure qrM
      cntM <- prqlRun ("from " <> tbl <> " | cnt")
      cntR <- maybe (ioError (userError "count PRQL failed")) pure cntM
      col  <- Conn.cellStr qr 0 0
      n    <- Conn.cellInt cntR 0 0
      when (n == 1 && not (T.null col)) $ do
        _ <- Conn.query ( "CREATE OR REPLACE TEMP TABLE " <> tbl
                       <> " AS SELECT unnest(\"" <> col
                       <> "\", recursive:=true) FROM " <> tbl )
        pure ()

-- | Copy typed columns from a DuckDB stub view onto the freshly loaded temp
-- table. Non-VARCHAR columns are applied via ALTER … TYPE with TRY_CAST so
-- bad rows become NULL rather than erroring.
applyStubTypes :: Text -> Text -> IO ()
applyStubTypes tbl stubName = do
  qr <- Conn.queryParam
    "SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = $1 AND data_type != 'VARCHAR'"
    stubName
  nr <- Conn.nrows qr
  let n = fromIntegral nr :: Int
  cols <- V.generateM n $ \i -> do
    colName <- Conn.cellStr qr (fromIntegral i) 0
    colType <- Conn.cellStr qr (fromIntegral i) 1
    pure (colName, colType)
  forM_ cols $ \(colName, colType) -> do
    let alter = "ALTER TABLE " <> tbl
              <> " ALTER COLUMN \"" <> colName <> "\" TYPE "
              <> colType <> " USING TRY_CAST(\"" <> colName
              <> "\" AS " <> colType <> ")"
    ignoreErrs (Conn.query alter)
