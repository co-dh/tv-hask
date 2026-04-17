{-# LANGUAGE ScopedTypeVariables #-}
{-
  SourceConfig: config-driven file/folder handling for remote sources.

  Each source is one `Source` value whose axis-like behavior (list, setup,
  enter, download) is a sum type. Runners pattern-match once on the mode
  and dispatch — no empty-string sentinels, no scattered T.null checks.

  Literal port of Tc/Tc/SourceConfig.lean. Same functions, same order.
-}
module Tv.SourceConfig
  ( -- Types
    Source (..)
  , List (..)
  , Parse (..)
  , Setup (..)
  , Enter (..)
  , Download (..)
  , Fallback (..)
    -- Accessors that pierce the mode sums (kept so external callers
    -- don't have to case-match just to ask "is there a script?").
  , script
  , enterUrl
  , attach
  , grp
    -- Helpers + data
  , s3Extra
  , pathParts
  , expand
  , mkVars
  , sources
  , findSource
  , configParent
  , setupDone
  , runSetup
  , listCache
  , runList
  , runDl
  , configResolve
  , runEnter
  ) where

import Control.Exception (SomeException, try)
import Control.Monad (when, unless, forM_)
import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef')
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import GHC.Clock (getMonotonicTime)
import System.Directory (createDirectoryIfMissing)
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.IO.Unsafe (unsafePerformIO)
import System.Process (readProcessWithExitCode)

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table
  ( AdbcTable
  , fromTmp
  , loadExt
  , tmpName
  , stripSemi
  )
import qualified Tv.Ftp as Ftp
import qualified Tv.Render as Render
import Tv.Types (escSql)
import qualified Tv.Log as Log
import qualified Tv.Remote as Remote
import qualified Tv.Tmp as Tmp

-- | Cmd stdout format. ParseFtp parses curl-fetched `ls -l` output;
-- ParseSql wraps stdout in read_json/read_csv via a template with {src}.
data Parse
  = ParseFtp
  | ParseSql Text
  deriving Show

-- | Last-resort cmd + SQL transform for APIs that 404 on missing paths
-- (e.g. HF org-level listing when a dataset repo doesn't exist).
data Fallback = Fallback { fbCmd, fbSql :: Text } deriving Show

-- | How to obtain the listing for a path.
--
--  * ListSql:    run a SQL query directly (e.g. pre-populated attached DB)
--  * ListAttach: auto-generate ATTACH + tbl_info query from path + extType
--  * ListCmd:    run a shell cmd, pipe stdout through Parse, optional fb
data List
  = ListSql    Text
  | ListAttach { extType :: Text, extName :: Text }
  | ListCmd    { listCmd :: Text, listParse :: Parse, listFb :: Maybe Fallback }
  deriving Show

-- | One-time setup before the first listing for this prefix.
--
--  * NoSetup:  nothing to do
--  * AttachDb: just run SQL (idempotent ATTACH etc.)
--  * SetupCmd: just run a shell cmd (no SQL follow-up)
--  * CachedDb: run buildCmd to populate a cached DuckDB file, then attachSql
--              to mount it. attachSql is tried first for fast restarts; on
--              failure (first run), buildCmd runs and attachSql is retried.
data Setup
  = NoSetup
  | AttachDb Text
  | SetupCmd Text
  | CachedDb { buildCmd, attachSql :: Text }
  deriving Show

-- | What happens when the user presses Enter on a row.
data Enter
  = NoEnter
  | EnterUrl Text  -- ^ navigate to an expanded URL template (e.g. hf://)
  | EnterCmd Text  -- ^ run a shell cmd whose stdout is JSON rows (e.g. osquery)
  deriving Show

-- | How DuckDB reads a file URI.
data Download
  = DlInline       -- ^ DuckDB reads the URI directly (httpfs, etc.)
  | DlBy    Text   -- ^ shell cmd template downloads to tmp first
  deriving Show

-- | Source config. One record per backend; 7 of them currently.
data Source = Source
  { pfx      :: Text      -- ^ URI prefix match ("s3://"); longest wins
  , minParts :: Int       -- ^ min URI parts before parent nav returns Nothing
  , parentFb :: Text      -- ^ fallback parent path at root; "" = none
  , grpCol   :: Text      -- ^ default group-by column; "" = none
  , dirSfx   :: Bool      -- ^ append "/" when joining child directory paths
  , urlEnc   :: Bool      -- ^ URL-encode path segments in cmd templates (FTP)
  , listMode :: List
  , setupMode :: Setup
  , enterMode :: Enter
  , dlMode    :: Download
  } deriving Show

-- ## Accessors across mode sums
--
-- Callers outside this module want "does this source have a script / enter
-- URL / attach listing" without pattern-matching the sum. These helpers
-- collapse each axis into the simple question asked at call sites.

-- | Enter cmd for EnterCmd; "" otherwise. Used by Folder/Main to decide
-- whether to call runEnter.
script :: Source -> Text
script s = case enterMode s of EnterCmd c -> c; _ -> ""

-- | Enter URL template for EnterUrl; "" otherwise.
enterUrl :: Source -> Text
enterUrl s = case enterMode s of EnterUrl u -> u; _ -> ""

-- | True iff listing is ATTACH-generated (callers treat enter as table-open).
attach :: Source -> Bool
attach s = case listMode s of ListAttach{} -> True; _ -> False

-- | Default group column ("" = none).
grp :: Source -> Text
grp = grpCol

-- ## Cache tunables
cacheCap, evictKeep :: Int
cacheCap  = 64
evictKeep = 32

-- | Best-effort IO: swallow exceptions where there's no recovery (e.g.
-- optional post-processing on loaded listings).
ignoreErrs :: IO a -> IO ()
ignoreErrs m = try m >>= \(_ :: Either SomeException a) -> pure ()

-- ## Module state
--
-- IORefs for cross-invocation caching: setup-once flags, slow-listing
-- results, and a compiled-once PRQL string.
{-# NOINLINE setupDone #-}
setupDone :: IORef (Vector Text)
setupDone = unsafePerformIO (newIORef V.empty)

{-# NOINLINE listCache #-}
listCache :: IORef (Vector (Text, AdbcTable))
listCache = unsafePerformIO (newIORef V.empty)

{-# NOINLINE filteredSql #-}
filteredSql :: IORef Text
filteredSql = unsafePerformIO (newIORef "")

-- | Skeleton Source with safe defaults; each entry in `sources` overrides
-- the fields it actually uses.
defaultSource :: Source
defaultSource = Source
  { pfx       = ""
  , minParts  = 0
  , parentFb  = ""
  , grpCol    = ""
  , dirSfx    = False
  , urlEnc    = False
  , listMode  = ListCmd "" (ParseSql "") Nothing
  , setupMode = NoSetup
  , enterMode = NoEnter
  , dlMode    = DlInline
  }

s3Extra :: Bool -> Text
s3Extra ns = if ns then "--no-sign-request" else ""

pathParts :: Text -> Text -> Vector Text
pathParts pfx_ path_ =
  let rest0 = T.drop (T.length pfx_) path_
      rest  = if T.isSuffixOf "/" rest0
                then T.dropEnd 1 rest0
                else rest0
  in if T.null rest then V.empty else V.fromList $ T.splitOn "/" rest

-- | Expand template placeholders: {path}, {name}, {tmp}, {extra}, {1}, {2}, {2+}, etc.
-- For empty values, also removes a preceding "/" to avoid trailing slashes
-- (e.g., "tree/main/{3+}" with empty {3+} becomes "tree/main" not "tree/main/").
expand :: Text -> Vector (Text, Text) -> Text
expand tmpl vars = V.foldl' step tmpl vars
  where
    step s (k, v) =
      if T.null v
        then T.replace ("{" <> k <> "}") ""
               (T.replace ("/{" <> k <> "}") "" s)
        else T.replace ("{" <> k <> "}") v s

mkVars :: Source -> Text -> Text -> Text -> Text -> Vector (Text, Text)
mkVars src path_ tmp name extra =
  let parts = pathParts (pfx src) path_
      dsn   = T.drop (T.length (pfx src)) path_
      baseVars = V.fromList
        [ ("path", path_), ("tmp", tmp), ("name", name)
        , ("extra", extra), ("dsn", dsn)
        ]
      getD i = fromMaybe "" $ parts V.!? i
      numbered = V.fromList
        [ (T.pack (show (i + 1)), getD i) | i <- [0 .. 8] ]
      plus = V.fromList
        [ ( T.pack (show (i + 1)) <> "+"
          , T.intercalate "/" (drop i (V.toList parts))
          )
        | i <- [0 .. 8]
        ]
  in baseVars V.++ numbered V.++ plus

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
    ioError $ userError ("Path contains shell metacharacters: " <> T.unpack s)

-- ## Source configs — inline array

-- | All source configs. Longest prefix match wins.
sources :: Vector Source
sources = V.fromList
  [ -- S3: aws s3api JSON output, download via aws s3 cp.
    -- read_json with explicit columns: missing keys become NULL, unnest(NULL) -> 0 rows.
    defaultSource
      { pfx      = "s3://"
      , minParts = 3
      , dirSfx   = True
      , listMode = ListCmd
          { listCmd   = "aws s3api list-objects-v2 --bucket {1} --delimiter / --prefix {2+}/ {extra} --output json"
          , listParse = ParseSql "WITH j AS MATERIALIZED (SELECT * FROM read_json('{src}', columns={\"Contents\": 'STRUCT(\"Key\" VARCHAR, \"Size\" BIGINT, \"LastModified\" VARCHAR)[]', \"CommonPrefixes\": 'STRUCT(\"Prefix\" VARCHAR)[]'})) SELECT split_part(c.\"Key\", '/', -1) as name, c.\"Size\" as size, c.\"LastModified\" as date, 'file' as type FROM j, unnest(j.Contents) as t(c) WHERE c.\"Key\" IS NOT NULL UNION ALL SELECT split_part(c.\"Prefix\", '/', -2) as name, 0 as size, '' as date, 'dir' as type FROM j, unnest(j.CommonPrefixes) as t(c) WHERE c.\"Prefix\" IS NOT NULL"
          , listFb    = Nothing
          }
      , dlMode = DlBy "aws s3 cp {extra} {path} {tmp}/{name}"
      }
  , -- HF dataset browser: curl HF Hub API; fallback to org-level listing on 404.
    defaultSource
      { pfx      = "hf://datasets/"
      , minParts = 5
      , dirSfx   = True
      , parentFb = "hf://"
      , listMode = ListCmd
          { listCmd   = "curl -sf https://huggingface.co/api/datasets/{1}/{2}/tree/main/{3+}"
          , listParse = ParseSql "SELECT split_part(path, '/', -1) as name, size, type FROM read_json_auto('{src}')"
          , listFb    = Just Fallback
              { fbCmd = "curl -sf 'https://huggingface.co/api/datasets?author={1}'"
              , fbSql = "SELECT split_part(id, '/', -1) as name, downloads, likes, description, 'directory' as type FROM read_json_auto('{src}')"
              }
          }
      , dlMode = DlBy "curl -sfL -o {tmp}/{name} https://huggingface.co/datasets/{1}/{2}/resolve/main/{3+}"
      }
  , -- HF root: dataset listing from a pre-populated DuckDB file.
    defaultSource
      { pfx       = "hf://"
      , grpCol    = "id"
      , listMode  = ListSql "SELECT id, downloads, likes, description, license, task, language, created, modified FROM hf.listing ORDER BY downloads DESC"
      , setupMode = CachedDb
          { buildCmd  = "python3 scripts/hf_datasets.py"
          , attachSql = "ATTACH '{home}/.cache/tv/hf_datasets.duckdb' AS hf (READ_ONLY)"
          }
      , enterMode = EnterUrl "hf://datasets/{name}/"
      }
  , -- Generic REST API: curl any JSON endpoint.
    defaultSource
      { pfx      = "rest://"
      , minParts = 1
      , listMode = ListCmd
          { listCmd   = "curl -sfL https://{1+}"
          , listParse = ParseSql "SELECT * FROM read_json_auto('{src}', auto_detect=true)"
          , listFb    = Nothing
          }
      }
  , -- Osquery: stub views in osq schema provide types + column comments.
    defaultSource
      { pfx       = "osquery://"
      , grpCol    = "name"
      , listMode  = ListSql "SELECT name, safety, rows, description FROM osq.listing ORDER BY name"
      , setupMode = CachedDb
          { buildCmd  = "python3 scripts/osquery_tables.py"
          , attachSql = "ATTACH '{home}/.cache/tv/osquery.duckdb' AS osq (READ_ONLY)"
          }
      , enterMode = EnterCmd "osqueryi --json \"SELECT * FROM {name}\""
      }
  , -- FTP: curl fetches `ls -l`, Haskell parses it.
    defaultSource
      { pfx      = "ftp://"
      , minParts = 3
      , dirSfx   = True
      , urlEnc   = True
      , listMode = ListCmd
          { listCmd   = "curl -sf {path}"
          , listParse = ParseFtp
          , listFb    = Nothing
          }
      , dlMode = DlBy "curl -sfL -o '{tmp}/{name}' {path}"
      }
  , -- PostgreSQL: attach-generated listing via the postgres extension.
    defaultSource
      { pfx      = "pg://"
      , minParts = 99
      , grpCol   = "name"
      , listMode = ListAttach { extType = "POSTGRES", extName = "postgres" }
      }
  ]

-- | Longest prefix wins.
findSource :: Text -> IO (Maybe Source)
findSource path_ =
  pure $ V.foldl' step Nothing sources
  where
    step best src =
      if not (T.null (pfx src)) && T.isPrefixOf (pfx src) path_
        then case best of
               Just b ->
                 if T.length (pfx src) > T.length (pfx b)
                   then Just src
                   else best
               Nothing -> Just src
        else best

-- ## Generic Operations

configParent :: Source -> Text -> Maybe Text
configParent src path_ =
  case Remote.parent path_ (minParts src) of
    Just p  -> Just p
    Nothing -> if T.null (parentFb src) then Nothing else Just (parentFb src)

-- | Load the ext (if any) that this source needs for listing.
loadListExt :: Source -> IO ()
loadListExt src = case listMode src of
  ListAttach{extName = e} -> loadExt e
  _                       -> pure ()

-- | Run one-time setup for a source, idempotent.
--
-- CachedDb tries the ATTACH first so a warm cache needs no rebuild;
-- on failure we rebuild via buildCmd and retry the ATTACH.
runSetup :: Source -> IO ()
runSetup src = do
  loadListExt src
  done <- readIORef setupDone
  unless (V.elem (pfx src) done) $ do
    homeT <- T.pack . fromMaybe "/tmp" <$> lookupEnv "HOME"
    runSetupMode homeT (setupMode src)
    modifyIORef' setupDone (`V.snoc` pfx src)

runSetupMode :: Text -> Setup -> IO ()
runSetupMode _ NoSetup = pure ()
runSetupMode home (AttachDb sql) = do
  let s = expand sql (V.singleton ("home", home))
  Log.write "src" ("setup sql: " <> s)
  _ <- Conn.query s
  pure ()
runSetupMode _ (SetupCmd c) = do
  Log.write "src" ("setup cmd: " <> c)
  _ <- readProcessWithExitCode "sh" ["-c", T.unpack c] ""
  pure ()
runSetupMode home (CachedDb bcmd asql) = do
  let s = expand asql (V.singleton ("home", home))
  -- Fast path: DB already built.
  r <- try (Conn.query s) :: IO (Either SomeException Conn.QueryResult)
  case r of
    Right _ -> Log.write "src" ("setup sql ok (skipped build): " <> s)
    Left _ -> do
      Log.write "src" "setup sql failed, building db first"
      Log.write "src" ("setup cmd: " <> bcmd)
      _ <- readProcessWithExitCode "sh" ["-c", T.unpack bcmd] ""
      Log.write "src" ("setup sql: " <> s)
      _ <- Conn.query s
      pure ()

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

fromPath :: Text -> Text
fromPath path_ =
  let parts = filter (not . T.null) (T.splitOn "/" path_)
  in if null parts then "file" else last parts

-- | Build template vars for a source + path (shared by runList/runDl).
-- Returns (vars, tmpDir) so callers don't re-walk the sources array.
cmdVars :: Bool -> Source -> Text -> IO (Vector (Text, Text), Text)
cmdVars noSign_ src path_ = do
  checkShell path_ "path"
  tmpDir <- Tmp.tmpPath "src"
  createDirectoryIfMissing True tmpDir
  _ <- Log.run "src" "mkdir" ["-p", tmpDir]
  let extra = if pfx src == "s3://" then s3Extra noSign_ else ""
      -- URL-encode path segments for curl when configured (e.g. FTP).
      cmdPath = if urlEnc src then Ftp.encodeUrl (pfx src) path_ else path_
      tmpT = T.pack tmpDir
  pure (mkVars src cmdPath tmpT (fromPath path_) extra, tmpT)

getFiltSql :: IO Text
getFiltSql = do
  cached <- readIORef filteredSql
  if not (T.null cached)
    then pure cached
    else do
      m <- Prql.compile Prql.ducktabsF
      case m of
        Nothing -> ioError $ userError $ "Failed to compile PRQL: " <> T.unpack Prql.ducktabsF
        Just sql -> do
          let sql' = stripSemi sql
          writeIORef filteredSql sql'
          pure sql'

-- | Build DETACH/ATTACH/SELECT SQL for a ListAttach source. The SELECT is
-- the compiled-once tbl_info_filtered PRQL.
attachListSql :: Text -> Text -> IO Text
attachListSql typ connStr = do
  let typClause = if T.null typ then "" else "TYPE " <> typ <> ", "
      ddl = "DETACH DATABASE IF EXISTS extdb;\nATTACH '" <> escSql connStr
              <> "' AS extdb (" <> typClause <> "READ_ONLY)"
  sql <- getFiltSql
  pure (ddl <> ";\n" <> sql)

-- | Execute a multi-statement SQL where the last statement is a SELECT
-- whose result becomes the temp table `tbl`.
execListSql :: Text -> Text -> IO (Maybe AdbcTable)
execListSql sql tbl = do
  let stmts = filter (not . T.null) (map T.strip (T.splitOn ";\n" sql))
  case reverse stmts of
    [] -> do
      _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
      fromTmp tbl
    (selectSql : rest) -> do
      forM_ (reverse rest) $ \stmt -> do
        _ <- Conn.query stmt
        pure ()
      _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> selectSql)
      fromTmp tbl

prqlRun :: Text -> IO (Maybe Conn.QueryResult)
prqlRun q = do
  m <- Prql.compile q
  case m of
    Nothing  -> pure Nothing
    Just sql -> Just <$> Conn.query sql

-- | Fallback cmd path: run `fbCmd`, apply `fbSql` on its output.
-- Used e.g. for HF org-level listing when repo listing 404s.
-- Returns `Just _` iff fallback ran successfully (distinguishes from
-- "failed" so the caller skips the error popup on empty fallback output).
runFallback :: Fallback -> Vector (Text, Text) -> Text -> IO (Maybe (Maybe AdbcTable))
runFallback fb vars tbl = do
  let cmd = expand (fbCmd fb) vars
  Log.write "src" ("fallback: " <> cmd)
  (ec, out, _) <- readProcessWithExitCode "sh" ["-c", T.unpack cmd] ""
  case ec of
    ExitSuccess | not (T.null (T.strip (T.pack out))) -> do
      tmpFile <- Tmp.tmpPath "src-list.json"
      TIO.writeFile tmpFile (T.pack out)
      let sql = expand (fbSql fb) (V.singleton ("src", T.pack tmpFile))
      _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
      Tmp.rmFile tmpFile
      Just <$> fromTmp tbl
    _ -> pure Nothing

-- | Parse raw cmd output via Parse and CREATE TEMP TABLE `tbl`.
loadListing :: Parse -> Text -> Text -> IO ()
loadListing parseMode raw tbl = do
  tmpFile <- Tmp.tmpPath "src-list.json"
  let content = case parseMode of ParseFtp -> Ftp.parseLs raw; ParseSql _ -> raw
  TIO.writeFile tmpFile content
  let tmpT = T.pack tmpFile
      sql = case parseMode of
        ParseFtp    -> "SELECT * FROM read_csv('" <> tmpT <> "', header=true, delim='\t')"
        ParseSql s  -> expand s (V.singleton ("src", tmpT))
  _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
  Tmp.rmFile tmpFile

-- | If the listing has 1 row with a single struct[] column, expand it.
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

-- | Add ".." parent row when source allows parent nav and the table has
-- folder cols (name/size/date/type). Silent on failure.
addParentRow :: Source -> Text -> Text -> IO ()
addParentRow src path_ tbl =
  case configParent src path_ of
    Nothing -> pure ()
    Just _  -> ignoreErrs insertRow
  where
    insertRow = do
      _ <- Conn.query ( "INSERT INTO " <> tbl
                     <> " SELECT '..' as name, 0 as size, '' as date, 'dir' as type" )
      pure ()

-- | Cmd mode: run cmd, save stdout, parse via listSql or FTP, auto-unnest,
-- add parent row. Fallback runs on exit failure when configured.
runListCmd :: Bool -> Source -> Text -> Parse -> Maybe Fallback -> Text -> Text
           -> IO (Maybe AdbcTable)
runListCmd noSign_ src cmdTmpl parseMode fb path_ tbl = do
  let p = if T.isSuffixOf "/" path_ then path_ else path_ <> "/"
  (vars, _) <- cmdVars noSign_ src p
  let cmd = expand cmdTmpl vars
  Log.write "src" ("list: " <> cmd)
  (ec, out, err) <- readProcessWithExitCode "sh" ["-c", T.unpack cmd] ""
  case ec of
    ExitFailure code -> do
      r <- case fb of
        Just f  -> runFallback f vars tbl
        Nothing -> pure Nothing
      case r of
        Just got -> pure got
        Nothing -> do
          let errMsg = T.strip (T.pack err)
          Log.write "src" ("list failed (exit " <> T.pack (show code) <> "): " <> errMsg)
          Render.errorPopup ("List failed: " <> errMsg)
          pure Nothing
    ExitSuccess -> do
      let raw = T.pack out
      if T.null (T.strip raw)
        then pure Nothing
        else do
          loadListing parseMode raw tbl
          unnestStruct tbl
          addParentRow src path_ tbl
          fromTmp tbl

-- | SQL mode: run source's literal listSql.
runListSqlMode :: Bool -> Source -> Text -> Text -> Text -> IO (Maybe AdbcTable)
runListSqlMode noSign_ src sqlTmpl path_ tbl = do
  (vars, _) <- cmdVars noSign_ src path_
  execListSql (expand sqlTmpl vars) tbl

-- | Attach mode: drop prefix from path to get the connStr, generate ATTACH
-- + filtered tbl_info SELECT, execute.
runListAttach :: Source -> Text -> Text -> Text -> IO (Maybe AdbcTable)
runListAttach src typ path_ tbl = do
  let connStr = if T.null (pfx src) then path_ else T.drop (T.length (pfx src)) path_
  sql <- attachListSql typ connStr
  execListSql sql tbl

-- | Run listing: cache check -> setup -> dispatch by listMode -> cache store.
-- Results are cached in-memory when listing takes > 3 seconds (slow S3 buckets).
runList :: Bool -> Source -> Text -> IO (Maybe AdbcTable)
runList noSign_ src path_ = do
  mc <- cacheLookup path_
  case mc of
    Just cached -> do
      Log.write "src" ("runList cache hit: " <> path_)
      pure (Just cached)
    Nothing -> do
      Log.write "src" ("runList: pfx=" <> pfx src <> " path=" <> path_)
      Render.statusMsg ("Loading " <> path_ <> " ...")
      t0 <- getMonotonicTime
      runSetup src
      tbl <- tmpName "src"
      result <- case listMode src of
        ListSql sql       -> runListSqlMode noSign_ src sql path_ tbl
        ListAttach{extType = t} -> runListAttach src t path_ tbl
        ListCmd c p fb    -> runListCmd noSign_ src c p fb path_ tbl
      -- Cache slow listings (> 3s) so navigating back is instant.
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

-- | Download via shell cmd; returns the local path. DlInline is not
-- expected to be called (configResolve short-circuits) but we treat it
-- as a passthrough for safety.
runDl :: Bool -> Source -> Text -> IO Text
runDl noSign_ src path_ = case dlMode src of
  DlInline -> pure path_
  DlBy tmpl -> do
    Render.statusMsg ("Downloading " <> path_ <> " ...")
    (vars, tmpDir) <- cmdVars noSign_ src path_
    let cmd = expand tmpl vars
    Log.write "src" ("download: " <> cmd)
    _ <- readProcessWithExitCode "sh" ["-c", T.unpack cmd] ""
    pure (tmpDir <> "/" <> fromPath path_)

-- | Download if needed, else pass the URI through (DuckDB reads it directly).
configResolve :: Bool -> Source -> Text -> IO Text
configResolve noSign_ src path_ = case dlMode src of
  DlInline -> pure path_
  DlBy _   -> runDl noSign_ src path_

-- | Copy column types from a DuckDB stub view onto the freshly-loaded temp table.
-- Non-VARCHAR columns from the stub (e.g. osq.<name> has typed cols) are applied
-- via ALTER ... TYPE with TRY_CAST so bad rows become NULL rather than erroring.
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
  V.forM_ cols $ \(colName, colType) -> do
    let alter = "ALTER TABLE " <> tbl
              <> " ALTER COLUMN \"" <> colName <> "\" TYPE "
              <> colType <> " USING TRY_CAST(\"" <> colName
              <> "\" AS " <> colType <> ")"
    rA <- try (Conn.query alter) :: IO (Either SomeException Conn.QueryResult)
    case rA of
      _ -> pure ()

-- | Load JSON output from an enter cmd into a fresh temp table, return name.
loadEnterJson :: Text -> IO Text
loadEnterJson json = do
  tbl <- tmpName "src"
  tmpFile <- Tmp.tmpPath ("src-enter-" <> T.unpack tbl <> ".json")
  TIO.writeFile tmpFile json
  _ <- Conn.query ( "CREATE TEMP TABLE " <> tbl
                 <> " AS SELECT * FROM read_json_auto('"
                 <> T.pack tmpFile <> "')" )
  Tmp.rmFile tmpFile
  pure tbl

-- | Run enter: EnterCmd runs a shell cmd whose stdout is JSON, loads into
-- a temp table, applies stub column types. Other enter modes return Nothing
-- (the Folder layer handles EnterUrl by navigating).
runEnter :: Source -> Text -> IO (Maybe AdbcTable)
runEnter src name = case enterMode src of
  EnterCmd tmpl -> do
    checkShell name "name"
    runSetup src
    let vars = mkVars src (pfx src <> name) "" name ""
        cmd  = expand tmpl vars
    Log.write "src" ("enter: " <> cmd)
    (ec, out, err) <- readProcessWithExitCode "sh" ["-c", T.unpack cmd] ""
    case ec of
      ExitFailure _ -> do
        Log.write "src" ("enter failed: " <> T.strip (T.pack err))
        pure Nothing
      ExitSuccess -> do
        let json = T.pack out
            trimmed = T.strip json
        if T.null trimmed || trimmed == "[]"
          then pure Nothing
          else do
            tbl <- loadEnterJson json
            rT <- try (applyStubTypes tbl name) :: IO (Either SomeException ())
            case rT of
              Left e  -> Log.write "src" ("enter types: " <> T.pack (show e))
              Right _ -> pure ()
            fromTmp tbl
  _ -> pure Nothing
