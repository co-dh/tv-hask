{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-
  SourceConfig: config-driven file/folder handling for remote sources.
  Inline Haskell config — no SQL file or DuckDB table needed.

  Flow: CLI cmd -> JSON -> tmp file -> listSql -> DuckDB temp table
  Or:   setupCmd -> setupSql -> listSql directly (no CLI, e.g. HF root)

  Literal port of Tc/Tc/SourceConfig.lean. Same functions, same order.
-}
module Tv.SourceConfig
  ( Config (..)
  , defaultConfig
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

import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Data.ADBC.Prql as Prql
import Tv.Data.ADBC.Table
  ( AdbcTable
  , fromTmp
  , loadExt
  , tmpName
  , stripSemi
  )
import qualified Tv.Ftp as Ftp
import qualified Tv.Render as Render
import Tv.Types (escSql)
import qualified Tv.Util as Log
import qualified Tv.Util as Remote  -- Remote.parent lives in Tv.Util
import Optics.TH (makeFieldLabelsNoPrefix)

-- | Config entry for a source
data Config = Config
  { pfx            :: Text  -- URI prefix: "s3://", "hf://", etc. Empty = catch-all.
  , minParts       :: Int   -- min URI parts before parent returns none
  , listCmd        :: Text  -- shell cmd template -> stdout JSON. Empty = run listSql directly.
  , listSql        :: Text  -- SQL to transform JSON (with {src}) or query directly. Sentinels like "FTP" are intentional: configs are data-driven (originally a DB table) so new backends don't need recompilation.
  , downloadCmd    :: Text  -- shell cmd template to download a file
  , needsDownload  :: Bool  -- true: download before DuckDB read. false: DuckDB reads URI
  , dirSuffix      :: Bool  -- true: append "/" when joining child dir paths
  , parentFallback :: Text  -- fallback parent when at minParts root. Empty = none.
  , setupCmd       :: Text  -- shell cmd to run before first listing. Empty = skip.
  , setupSql       :: Text  -- SQL to run before first listing (e.g. ATTACH). Empty = skip.
  , grp            :: Text  -- default group column name. Empty = none.
  , enterUrl       :: Text  -- URL template for entering file rows. Empty = default.
  , script         :: Text  -- shell cmd template for enter: stdout = JSON rows. Empty = none.
  , attach         :: Bool  -- true: enter uses fromTable (for attached databases)
  , duckdbExt      :: Text  -- DuckDB extension to auto INSTALL/LOAD (e.g. "postgres"). Empty = none.
  , attachType     :: Text  -- ATTACH TYPE clause (e.g. "POSTGRES"). Empty = native DuckDB.
  , urlEncode      :: Bool  -- true: URL-encode path segments for curl commands (e.g. FTP)
  , fallbackCmd    :: Text  -- shell cmd template if listCmd fails (e.g. org-level listing)
  , fallbackSql    :: Text  -- SQL transform for fallbackCmd output
  }
makeFieldLabelsNoPrefix ''Config

defaultConfig :: Config
defaultConfig = Config
  { pfx            = ""
  , minParts       = 0
  , listCmd        = ""
  , listSql        = ""
  , downloadCmd    = ""
  , needsDownload  = False
  , dirSuffix      = False
  , parentFallback = ""
  , setupCmd       = ""
  , setupSql       = ""
  , grp            = ""
  , enterUrl       = ""
  , script         = ""
  , attach         = False
  , duckdbExt      = ""
  , attachType     = ""
  , urlEncode      = False
  , fallbackCmd    = ""
  , fallbackSql    = ""
  }

-- | Get S3 extra args string from noSign flag
s3Extra :: Bool -> Text
s3Extra ns = if ns then "--no-sign-request" else ""

-- | Split path into components after stripping prefix
pathParts :: Text -> Text -> Vector Text
pathParts pfx_ path_ =
  let rest0 = T.drop (T.length pfx_) path_
      rest  = if T.isSuffixOf "/" rest0
                then T.dropEnd 1 rest0
                else rest0
  in if T.null rest then V.empty else V.fromList (T.splitOn "/" rest)

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

-- | Build template variables from a config and path
mkVars :: Config -> Text -> Text -> Text -> Text -> Vector (Text, Text)
mkVars cfg path_ tmp name extra =
  let parts = pathParts (pfx cfg) path_
      dsn   = T.drop (T.length (pfx cfg)) path_
      baseVars =
        V.fromList
          [ ("path", path_)
          , ("tmp", tmp)
          , ("name", name)
          , ("extra", extra)
          , ("dsn", dsn)
          ]
      getD i = fromMaybe "" (parts V.!? i)
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
    ioError (userError ("Path contains shell metacharacters: " <> T.unpack s))

-- ## Source configs — inline array

-- | All source configs. Longest prefix match wins.
sources :: Vector Config
sources = V.fromList
  [ -- S3: aws s3api JSON output, download via aws s3 cp
    defaultConfig
      { pfx = "s3://"
      , minParts = 3
      , listCmd = "aws s3api list-objects-v2 --bucket {1} --delimiter / --prefix {2+}/ {extra} --output json"
      -- read_json with explicit columns: missing keys become NULL, unnest(NULL) -> 0 rows
      , listSql = "WITH j AS MATERIALIZED (SELECT * FROM read_json('{src}', columns={\"Contents\": 'STRUCT(\"Key\" VARCHAR, \"Size\" BIGINT, \"LastModified\" VARCHAR)[]', \"CommonPrefixes\": 'STRUCT(\"Prefix\" VARCHAR)[]'})) SELECT split_part(c.\"Key\", '/', -1) as name, c.\"Size\" as size, c.\"LastModified\" as date, 'file' as type FROM j, unnest(j.Contents) as t(c) WHERE c.\"Key\" IS NOT NULL UNION ALL SELECT split_part(c.\"Prefix\", '/', -2) as name, 0 as size, '' as date, 'dir' as type FROM j, unnest(j.CommonPrefixes) as t(c) WHERE c.\"Prefix\" IS NOT NULL"
      , downloadCmd = "aws s3 cp {extra} {path} {tmp}/{name}"
      , needsDownload = True
      , dirSuffix = True
      }
  , -- HF dataset browser: curl HF Hub API; fallback to org-level listing
    defaultConfig
      { pfx = "hf://datasets/"
      , minParts = 5
      , listCmd = "curl -sf https://huggingface.co/api/datasets/{1}/{2}/tree/main/{3+}"
      , listSql = "SELECT split_part(path, '/', -1) as name, size, type FROM read_json_auto('{src}')"
      , downloadCmd = "curl -sfL -o {tmp}/{name} https://huggingface.co/datasets/{1}/{2}/resolve/main/{3+}"
      , dirSuffix = True
      , parentFallback = "hf://"
      , fallbackCmd = "curl -sf 'https://huggingface.co/api/datasets?author={1}'"
      , fallbackSql = "SELECT split_part(id, '/', -1) as name, downloads, likes, description, 'directory' as type FROM read_json_auto('{src}')"
      }
  , -- HF root: dataset listing from pre-populated DuckDB
    defaultConfig
      { pfx = "hf://"
      , listSql = "SELECT id, downloads, likes, description, license, task, language, created, modified FROM hf.listing ORDER BY downloads DESC"
      , setupCmd = "python3 scripts/hf_datasets.py"
      , setupSql = "ATTACH '{home}/.cache/tv/hf_datasets.duckdb' AS hf (READ_ONLY)"
      , grp = "id"
      , enterUrl = "hf://datasets/{name}/"
      }
  , -- Generic REST API: curl any JSON endpoint
    defaultConfig
      { pfx = "rest://"
      , minParts = 1
      , listCmd = "curl -sfL https://{1+}"
      , listSql = "SELECT * FROM read_json_auto('{src}', auto_detect=true)"
      }
  , -- Osquery: stub views in osq schema provide types + column comments
    defaultConfig
      { pfx = "osquery://"
      , listSql = "SELECT name, safety, rows, description FROM osq.listing ORDER BY name"
      , setupCmd = "python3 scripts/osquery_tables.py"
      , setupSql = "ATTACH '{home}/.cache/tv/osquery.duckdb' AS osq (READ_ONLY)"
      , grp = "name"
      , script = "osqueryi --json \"SELECT * FROM {name}\""
      }
  , -- FTP: curl fetches ls -l, Haskell parses it
    defaultConfig
      { pfx = "ftp://"
      , minParts = 3
      , listCmd = "curl -sf {path}"
      , listSql = "FTP"
      , downloadCmd = "curl -sfL -o '{tmp}/{name}' {path}"
      , needsDownload = True
      , dirSuffix = True
      , urlEncode = True
      }
  , -- PostgreSQL: attach=true + duckdbExt auto-generates ATTACH SQL
    defaultConfig
      { pfx = "pg://"
      , minParts = 99
      , grp = "name"
      , attach = True
      , duckdbExt = "postgres"
      , attachType = "POSTGRES"
      }
  ]

-- | Find config for a path by prefix match (longest prefix wins)
findSource :: Text -> IO (Maybe Config)
findSource path_ =
  pure $ V.foldl' step Nothing sources
  where
    step best cfg =
      if not (T.null (pfx cfg)) && T.isPrefixOf (pfx cfg) path_
        then case best of
               Just b ->
                 if T.length (pfx cfg) > T.length (pfx b)
                   then Just cfg
                   else best
               Nothing -> Just cfg
        else best

-- ## Generic Operations

-- | Parent path navigation using Remote.parent with config's minParts
configParent :: Config -> Text -> Maybe Text
configParent cfg path_ =
  case Remote.parent path_ (minParts cfg) of
    Just p  -> Just p
    Nothing -> if T.null (parentFallback cfg) then Nothing else Just (parentFallback cfg)

-- | Track which prefixes/extensions have completed setup
{-# NOINLINE setupDone #-}
setupDone :: IORef (Vector Text)
setupDone = unsafePerformIO (newIORef V.empty)

-- | Run one-time setup for a config (duckdbExt + setupCmd + setupSql), idempotent.
-- Tries setupSql first; if it fails (e.g. DB doesn't exist), runs setupCmd to create it.
runSetup :: Config -> IO ()
runSetup cfg = do
  loadExt (duckdbExt cfg)
  done <- readIORef setupDone
  if V.elem (pfx cfg) done
    then pure ()
    else do
      homeDir <- fromMaybe "/tmp" <$> lookupEnv "HOME"
      let homeT = T.pack homeDir
      skipCmd <- if not (T.null (setupSql cfg))
        then do
          let sql = expand (setupSql cfg) (V.singleton ("home", homeT))
          r <- try (Adbc.query sql) :: IO (Either SomeException Adbc.QueryResult)
          case r of
            Right _ -> do
              Log.write "src" ("setup sql ok (skipped cmd): " <> sql)
              modifyIORef' setupDone (`V.snoc` pfx cfg)
              pure True
            Left _ -> do
              Log.write "src" "setup sql failed, running cmd first"
              pure False
        else pure False
      unless skipCmd $ do
        when (not (T.null (setupCmd cfg))) $ do
          Log.write "src" ("setup cmd: " <> setupCmd cfg)
          let cmd = expand (setupCmd cfg) (V.singleton ("home", homeT))
          _ <- readProcessWithExitCode "sh" ["-c", T.unpack cmd] ""
          pure ()
        when (not (T.null (setupSql cfg))) $ do
          let sql = expand (setupSql cfg) (V.singleton ("home", homeT))
          Log.write "src" ("setup sql: " <> sql)
          _ <- Adbc.query sql
          pure ()
        modifyIORef' setupDone (`V.snoc` pfx cfg)

-- | Cache for slow listings (> 3s). Keyed by path.
{-# NOINLINE listCache #-}
listCache :: IORef (Vector (Text, AdbcTable))
listCache = unsafePerformIO (newIORef V.empty)

cacheLookup :: Text -> IO (Maybe AdbcTable)
cacheLookup path_ = do
  arr <- readIORef listCache
  pure $ fmap snd (V.find (\(k, _) -> k == path_) arr)

cacheStore :: Text -> AdbcTable -> IO ()
cacheStore path_ tbl =
  modifyIORef' listCache $ \arr ->
    let arr' = V.filter (\(k, _) -> k /= path_) arr
    in if V.length arr' >= 64
         then V.snoc (V.slice 32 (V.length arr' - 32) arr') (path_, tbl)
         else V.snoc arr' (path_, tbl)

-- | Build AdbcTable from a temp table name
fromTbl :: Text -> IO (Maybe AdbcTable)
fromTbl tbl = fromTmp tbl

-- | Extract last path component as a filename
fromPath :: Text -> Text
fromPath path_ =
  let parts = filter (not . T.null) (T.splitOn "/" path_)
  in if null parts then "file" else last parts

-- | Build template vars for a config + path (shared by runList/runDownload).
-- Returns (vars, tmpDir) so callers don't need to search the array.
cmdVars :: Bool -> Config -> Text -> IO (Vector (Text, Text), Text)
cmdVars noSign_ cfg path_ = do
  checkShell path_ "path"
  tmpDir <- Log.tmpPath "src"
  createDirectoryIfMissing True tmpDir
  _ <- Log.run "src" "mkdir" ["-p", tmpDir]
  let extra = if pfx cfg == "s3://" then s3Extra noSign_ else ""
  -- URL-encode path segments for curl when configured (e.g. FTP)
  let cmdPath = if urlEncode cfg then Ftp.encodeUrl (pfx cfg) path_ else path_
      tmpT = T.pack tmpDir
  pure (mkVars cfg cmdPath tmpT (fromPath path_) extra, tmpT)

-- | Cached compiled SQL for tbl_info_filtered (fixed query, compile once)
{-# NOINLINE filteredSql #-}
filteredSql :: IORef Text
filteredSql = unsafePerformIO (newIORef "")

getFiltSql :: IO Text
getFiltSql = do
  cached <- readIORef filteredSql
  if not (T.null cached)
    then pure cached
    else do
      m <- Prql.compile Prql.ducktabsF
      case m of
        Nothing -> ioError (userError ("Failed to compile PRQL: " <> T.unpack Prql.ducktabsF))
        Just sql -> do
          let sql' = stripSemi sql
          writeIORef filteredSql sql'
          pure sql'

-- | Generate attach SQL from config fields (DRY: DETACH/ATTACH/SELECT pattern)
-- The SELECT portion is compiled from PRQL tbl_info_filtered function.
attachSql :: Config -> Text -> IO Text
attachSql cfg connStr = do
  let typClause = if T.null (attachType cfg) then "" else "TYPE " <> attachType cfg <> ", "
      ddl = "DETACH DATABASE IF EXISTS extdb;\nATTACH '" <> escSql connStr
              <> "' AS extdb (" <> typClause <> "READ_ONLY)"
  sql <- getFiltSql
  pure (ddl <> ";\n" <> sql)

-- | Direct SQL mode: auto-generate attach SQL or expand listSql, execute multi-statement
runListSql :: Bool -> Config -> Text -> Text -> IO (Maybe AdbcTable)
runListSql noSign_ cfg path_ tbl = do
  sql <- if attach cfg && T.null (listSql cfg)
    then do
      let connStr =
            if T.null (pfx cfg) then path_
            else T.drop (T.length (pfx cfg)) path_
      attachSql cfg connStr
    else do
      (vars, _) <- cmdVars noSign_ cfg path_
      pure (expand (listSql cfg) vars)
  let stmts = filter (not . T.null)
                (map T.strip (T.splitOn ";\n" sql))
  case reverse stmts of
    [] -> do
      _ <- Adbc.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
      fromTbl tbl
    (selectSql : rest) -> do
      forM_ (reverse rest) $ \stmt -> do
        _ <- Adbc.query stmt
        pure ()
      _ <- Adbc.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> selectSql)
      fromTbl tbl

-- | CLI mode: run command, save JSON, transform via listSql, auto-unnest, add parent row
runListCmd :: Bool -> Config -> Text -> Text -> IO (Maybe AdbcTable)
runListCmd noSign_ cfg path_ tbl = do
  let p = if T.isSuffixOf "/" path_ then path_ else path_ <> "/"
  (vars, _) <- cmdVars noSign_ cfg p
  let cmd = expand (listCmd cfg) vars
  Log.write "src" ("list: " <> cmd)
  (ec, out, err) <- readProcessWithExitCode "sh" ["-c", T.unpack cmd] ""
  case ec of
    ExitFailure code -> do
      -- Try fallback command (e.g. org-level listing when repo listing fails)
      fb <- if not (T.null (fallbackCmd cfg))
        then do
          let fbCmd = expand (fallbackCmd cfg) vars
          Log.write "src" ("fallback: " <> fbCmd)
          (ec2, out2, _) <- readProcessWithExitCode "sh" ["-c", T.unpack fbCmd] ""
          case ec2 of
            ExitSuccess | not (T.null (T.strip (T.pack out2))) -> do
              tmpFile <- Log.tmpPath "src-list.json"
              TIO.writeFile tmpFile (T.pack out2)
              let fbSql = expand (fallbackSql cfg)
                            (V.singleton ("src", T.pack tmpFile))
              _ <- Adbc.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> fbSql)
              Log.rmFile tmpFile
              Just <$> fromTbl tbl
            _ -> pure Nothing
        else pure Nothing
      case fb of
        Just r -> pure r
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
          tmpFile <- Log.tmpPath "src-list.json"
          -- FTP mode: parse ls -l in Haskell; otherwise use SQL transform
          let content = if listSql cfg == "FTP" then Ftp.parseLs raw else raw
          TIO.writeFile tmpFile content
          let tmpT = T.pack tmpFile
              lSql =
                if listSql cfg == "FTP"
                  then "SELECT * FROM read_csv('" <> tmpT <> "', header=true, delim='\t')"
                  else expand (listSql cfg) (V.singleton ("src", tmpT))
          _ <- Adbc.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> lSql)
          -- Auto-unnest: if result is 1 row with a struct[] column, expand it
          rU <- try (do
            mq <- Prql.compile ("from dcols | struct_col '" <> tbl <> "'")
            case mq of
              Nothing -> ioError (userError "struct_col PRQL failed")
              Just _  -> pure ()
            qrM <- prqlRun ("from dcols | struct_col '" <> tbl <> "'")
            qr  <- case qrM of
                     Just q -> pure q
                     Nothing -> ioError (userError "struct_col PRQL failed")
            cntM <- prqlRun ("from " <> tbl <> " | cnt")
            cntR <- case cntM of
                      Just q -> pure q
                      Nothing -> ioError (userError "count PRQL failed")
            col <- Adbc.cellStr qr 0 0
            n   <- Adbc.cellInt cntR 0 0
            when (n == 1 && not (T.null col)) $ do
              _ <- Adbc.query ( "CREATE OR REPLACE TEMP TABLE " <> tbl
                             <> " AS SELECT unnest(\"" <> col
                             <> "\", recursive:=true) FROM " <> tbl )
              pure ()) :: IO (Either SomeException ())
          case rU of
            _ -> pure ()
          -- Add ".." parent row if table has standard folder columns (name,size,date,type)
          case configParent cfg path_ of
            Just _ -> do
              rP <- try (do
                _ <- Adbc.query ( "INSERT INTO " <> tbl
                               <> " SELECT '..' as name, 0 as size, '' as date, 'dir' as type" )
                pure ()) :: IO (Either SomeException ())
              case rP of
                _ -> pure ()
            Nothing -> pure ()
          Log.rmFile tmpFile
          fromTbl tbl
  where
    -- compile+run PRQL, returning the QueryResult (matches Lean `Prql.query`)
    prqlRun :: Text -> IO (Maybe Adbc.QueryResult)
    prqlRun q = do
      m <- Prql.compile q
      case m of
        Nothing -> pure Nothing
        Just sql -> Just <$> Adbc.query sql

-- | Run listing: cache check -> setup -> dispatch to sql/cmd mode -> cache store.
-- Results are cached in-memory when listing takes > 3 seconds (e.g. slow S3 buckets).
runList :: Bool -> Config -> Text -> IO (Maybe AdbcTable)
runList noSign_ cfg path_ = do
  mc <- cacheLookup path_
  case mc of
    Just cached -> do
      Log.write "src" ("runList cache hit: " <> path_)
      pure (Just cached)
    Nothing -> do
      Log.write "src" ( "runList: pfx=" <> pfx cfg
                     <> " path=" <> path_
                     <> " listCmd=" <> listCmd cfg )
      Render.statusMsg ("Loading " <> path_ <> " ...")
      t0 <- getMonotonicTime
      runSetup cfg
      tbl <- tmpName "src"
      result <- if T.null (listCmd cfg)
        then runListSql noSign_ cfg path_ tbl
        else runListCmd noSign_ cfg path_ tbl
      -- Cache slow listings (> 3s) so navigating back is instant
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

-- | Download a remote file to local temp path
runDl :: Bool -> Config -> Text -> IO Text
runDl noSign_ cfg path_ = do
  Render.statusMsg ("Downloading " <> path_ <> " ...")
  (vars, tmpDir) <- cmdVars noSign_ cfg path_
  let cmd = expand (downloadCmd cfg) vars
  Log.write "src" ("download: " <> cmd)
  _ <- readProcessWithExitCode "sh" ["-c", T.unpack cmd] ""
  pure (tmpDir <> "/" <> fromPath path_)

-- | Resolve data file path: download if needed, or return URI for DuckDB
configResolve :: Bool -> Config -> Text -> IO Text
configResolve noSign_ cfg path_ =
  if needsDownload cfg then runDl noSign_ cfg path_ else pure path_

-- | Run enter: script cmd -> JSON -> DuckDB temp table, apply types from stub view
runEnter :: Config -> Text -> IO (Maybe AdbcTable)
runEnter cfg name =
  if T.null (script cfg)
    then pure Nothing
    else do
      checkShell name "name"
      runSetup cfg
      let vars = mkVars cfg (pfx cfg <> name) "" name ""
          cmd  = expand (script cfg) vars
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
              tbl <- tmpName "src"
              tmpFile <- Log.tmpPath ("src-enter-" <> T.unpack tbl <> ".json")
              TIO.writeFile tmpFile json
              _ <- Adbc.query ( "CREATE TEMP TABLE " <> tbl
                             <> " AS SELECT * FROM read_json_auto('"
                             <> T.pack tmpFile <> "')" )
              Log.rmFile tmpFile
              -- Apply types from DuckDB stub view (e.g. osq.groups has typed columns)
              let typeApply :: IO ()
                  typeApply = do
                    qr <- Adbc.queryParam
                      "SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = $1 AND data_type != 'VARCHAR'"
                      name
                    nr <- Adbc.nrows qr
                    let n = fromIntegral nr :: Int
                    cols <- V.generateM n $ \i -> do
                      colName <- Adbc.cellStr qr (fromIntegral i) 0
                      colType <- Adbc.cellStr qr (fromIntegral i) 1
                      pure (colName, colType)
                    V.forM_ cols $ \(colName, colType) -> do
                      let alter = "ALTER TABLE " <> tbl
                                <> " ALTER COLUMN \"" <> colName <> "\" TYPE "
                                <> colType <> " USING TRY_CAST(\"" <> colName
                                <> "\" AS " <> colType <> ")"
                      rA <- try (Adbc.query alter) :: IO (Either SomeException Adbc.QueryResult)
                      case rA of
                        _ -> pure ()
              rT <- try typeApply :: IO (Either SomeException ())
              case rT of
                Left e  -> Log.write "src" ("enter types: " <> T.pack (show e))
                Right _ -> pure ()
              fromTbl tbl
