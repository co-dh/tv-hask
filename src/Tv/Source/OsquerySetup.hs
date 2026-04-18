{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Populate ~/.cache/tv/osquery.duckdb with osquery table metadata.
--
-- Haskell port of scripts/osquery_tables.py. Queries osqueryi for the list
-- of tables, downloads the osquery schema JSON (30-day cached), builds
-- typed stub views with COMMENT ON COLUMN under schema @osq@, and counts
-- rows in parallel for non-dangerous tables (24h cache).
--
-- Produced objects inside osquery.duckdb:
--   - main.listing  (name, safety, rows, description, updated_at)
--   - osq.<table>   stub VIEWs with typed NULL columns + column comments
module Tv.Source.OsquerySetup (run) where

import Control.Concurrent.Async (forConcurrently)
import Control.Concurrent.QSemN (QSemN, newQSemN, signalQSemN, waitQSemN)
import Control.Exception (SomeException, bracket_, try)
import Control.Monad (forM_, unless)
import Data.Aeson (Value (..), decode, (.:?))
import qualified Data.Aeson as A
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import Data.Aeson.Types (parseMaybe)
import qualified Data.ByteString.Lazy as BL
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Read as TR
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector as V
import Network.HTTP.Client (httpLbs, newManager, parseRequest, responseBody, responseStatus)
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Network.HTTP.Types.Status (statusCode)
import System.Directory (createDirectoryIfMissing, doesFileExist, getModificationTime, removeFile)
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode)

import qualified Tv.Data.DuckDB as DB
import qualified Tv.Log as Log

-- | Tables that can hang, consume lots of memory, or spawn subprocesses.
-- Mirrors the Python DANGEROUS_TABLES list; skipped during row counting,
-- reported as "input-required" in the listing.
dangerous :: [Text]
dangerous =
  [ "hash", "file", "augeas", "yara", "curl", "curl_certificate"
  , "magic", "device_file", "carves", "suid_bin"
  , "file_events", "process_events", "process_file_events", "socket_events"
  , "hardware_events", "selinux_events", "seccomp_events", "syslog_events"
  , "user_events", "apparmor_events"
  ]

schemaUrlBase :: Text
schemaUrlBase =
  "https://raw.githubusercontent.com/osquery/osquery-site/"
  <> "source/src/data/osquery_schema_versions/"

-- | osquery type → DuckDB type. Unknown → VARCHAR (fallback in lookup).
typeMap :: Text -> Text
typeMap t = case T.toUpper t of
  "TEXT"            -> "VARCHAR"
  "INTEGER"         -> "BIGINT"
  "BIGINT"          -> "BIGINT"
  "DOUBLE"          -> "DOUBLE"
  "BLOB"            -> "BLOB"
  "UNSIGNED_BIGINT" -> "UBIGINT"
  _                 -> "VARCHAR"

-- | 30 days for the downloaded schema; 24 h for the row-count table.
schemaAge, countsAge :: Double
schemaAge = 30 * 24 * 3600
countsAge = 24 * 3600

concLimit :: Int
concLimit = 32

-- | Quote-escape for a SQL string literal.
esc :: Text -> Text
esc = T.replace "'" "''"

-- ----------------------------------------------------------------------------
-- Cache paths
-- ----------------------------------------------------------------------------

cacheDir :: IO FilePath
cacheDir = do
  d <- Log.dir   -- ~/.cache/tv
  createDirectoryIfMissing True d
  pure d

dbPath, schemaPath :: IO FilePath
dbPath     = (<> "/osquery.duckdb")      <$> cacheDir
schemaPath = (<> "/osquery_schema.json") <$> cacheDir

-- ----------------------------------------------------------------------------
-- osqueryi shell wrappers
-- ----------------------------------------------------------------------------

-- | Run osqueryi --json. @timeoutSec@ wraps via the @timeout@ shell util
-- so a stuck osqueryi gets SIGKILLed instead of hanging the whole setup.
osqJson :: Int -> Text -> IO (Maybe Value)
osqJson timeoutSec sql = do
  let ts = show timeoutSec
  (ec, out, _) <- readProcessWithExitCode "timeout"
                    ["-k", "1", ts, "osqueryi", "--json"
                    , "--disable_events=true", T.unpack sql] ""
  case ec of
    ExitSuccess -> pure $ decode (BL.fromStrict (TE.encodeUtf8 (T.pack out)))
    _           -> pure Nothing

-- | Get version string (e.g. "5.21.0"), fallback "5.21.0".
osqVersion :: IO Text
osqVersion = do
  r <- try (readProcessWithExitCode "osqueryi" ["--version"] "")
       :: IO (Either SomeException (ExitCode, String, String))
  case r of
    Right (ExitSuccess, out, _) ->
      let ws = words out
      in if null ws then pure "5.21.0" else pure (T.pack (last ws))
    _ -> pure "5.21.0"

-- | All registered table names (alphabetical).
tableNames :: IO [Text]
tableNames = do
  mVal <- osqJson 5
    "SELECT name FROM osquery_registry WHERE registry='table' ORDER BY name"
  pure $ fromMaybe [] (mVal >>= jsonStrCol "name")

-- | `timeout -k 1 2 osqueryi …` — SIGKILL 1s after the 2s grace period.
-- Returns (name, Just count) on success, (name, Nothing) on timeout / error.
countOne :: Text -> IO (Text, Maybe Int)
countOne name = do
  r <- try (readProcessWithExitCode "timeout"
              ["-k", "1", "2", "osqueryi", "--json"
              , "SELECT count(*) AS n FROM " <> T.unpack name] "")
       :: IO (Either SomeException (ExitCode, String, String))
  case r of
    Right (ExitSuccess, out, _) ->
      case decode (BL.fromStrict (TE.encodeUtf8 (T.pack out))) of
        Just v  -> pure (name, jsonIntCol "n" v)
        Nothing -> pure (name, Nothing)
    _ -> pure (name, Nothing)

-- ----------------------------------------------------------------------------
-- JSON helpers
-- ----------------------------------------------------------------------------

-- | Pull a column from @[{k: v, ...}, ...]@. Stringifies ints transparently.
jsonStrCol :: Text -> Value -> Maybe [Text]
jsonStrCol k (Array arr) = Just $ mapMaybe (jsonStr k) (V.toList arr)
jsonStrCol _ _           = Nothing

jsonStr :: Text -> Value -> Maybe Text
jsonStr k (Object o) = case KM.lookup (K.fromText k) o of
  Just (String s)  -> Just s
  Just (Number n)  -> Just (T.pack (show n))
  _                -> Nothing
jsonStr _ _ = Nothing

-- | First row's @k@ as Int. osqueryi returns numbers as strings in JSON.
jsonIntCol :: Text -> Value -> Maybe Int
jsonIntCol k (Array arr) = case V.toList arr of
  (row : _) -> jsonStr k row >>= parseInt
  _         -> Nothing
jsonIntCol _ _ = Nothing

parseInt :: Text -> Maybe Int
parseInt t = case TR.decimal t of
  Right (n, rest) | T.null rest -> Just n
  _                             -> Nothing

-- | One schema entry @{ "name": ..., "columns": [...], "description": ... }@.
data SchemaTbl = SchemaTbl
  { stName :: Text
  , stDesc :: Text
  , stCols :: [SchemaCol]
  } deriving Show

data SchemaCol = SchemaCol
  { scName :: Text
  , scType :: Text
  , scDesc :: Text
  } deriving Show

parseSchema :: Value -> [SchemaTbl]
parseSchema (Array arr) = mapMaybe pEntry (V.toList arr)
  where
    pEntry = parseMaybe $ A.withObject "entry" $ \o -> do
      nm <- o .:? "name"
      case nm of
        Nothing -> fail "no name"
        Just n  -> do
          d  <- fromMaybe "" <$> o .:? "description"
          cs <- fromMaybe [] <$> o .:? "columns"
          pure $ SchemaTbl n d (mapMaybe pCol cs)
    pCol :: Value -> Maybe SchemaCol
    pCol = parseMaybe $ A.withObject "col" $ \o -> do
      cn <- fromMaybe "" <$> o .:? "name"
      ct <- fromMaybe "TEXT" <$> o .:? "type"
      cd <- fromMaybe "" <$> o .:? "description"
      pure $ SchemaCol cn ct cd
parseSchema _ = []

-- ----------------------------------------------------------------------------
-- Schema download (30-day cache)
-- ----------------------------------------------------------------------------

-- | True iff file exists AND is newer than @maxAge@ seconds.
fresh :: FilePath -> Double -> IO Bool
fresh p maxAge = do
  ex <- doesFileExist p
  if not ex then pure False else do
    mt <- getModificationTime p
    now <- getCurrentTime
    pure $ realToFrac (diffUTCTime now mt) < maxAge

-- | Download @url@ to @dst@ via http-client-tls; write only on 2xx.
download :: Text -> FilePath -> IO Bool
download url dst = do
  r <- try go :: IO (Either SomeException Bool)
  case r of
    Right ok -> pure ok
    Left e -> do
      Log.write "osquery-setup" ("download failed: " <> T.pack (show e))
      pure False
  where
    go = do
      mgr <- newManager tlsManagerSettings
      req <- parseRequest (T.unpack url)
      resp <- httpLbs req mgr
      let sc = statusCode (responseStatus resp)
      if sc >= 200 && sc < 300
        then do BL.writeFile dst (responseBody resp); pure True
        else do
          Log.write "osquery-setup" ("schema HTTP " <> T.pack (show sc))
          pure False

-- | Returns the parsed schema, using the local cache when fresh.
loadSchema :: Text -> IO (Maybe Value)
loadSchema version = do
  p <- schemaPath
  ok <- fresh p schemaAge
  unless ok $ do
    _ <- download (schemaUrlBase <> version <> ".json") p
    pure ()
  ex <- doesFileExist p
  if not ex
    then pure Nothing
    else decode <$> BL.readFile p

-- ----------------------------------------------------------------------------
-- DuckDB writes (direct connection to the persistent file)
-- ----------------------------------------------------------------------------

-- | Execute @sql@; swallow any error and log it. Used for best-effort
-- statements (DROP VIEW IF EXISTS, per-chunk CREATE VIEW, COMMENT …).
tryExec :: DB.Conn -> Text -> IO ()
tryExec c sql = do
  r <- try (DB.query c sql) :: IO (Either SomeException DB.Result)
  case r of
    Left e  -> Log.write "osquery-setup" ("sql err: " <> T.pack (show e)
                                         <> " <= " <> T.take 200 sql)
    Right _ -> pure ()

-- | Run multiple statements as one string; rollback on any error is
-- implicit (no transactions here — DuckDB auto-commits each statement).
-- Used only for the stub views + comments, which are idempotent.
chunkExec :: DB.Conn -> [Text] -> IO ()
chunkExec _ [] = pure ()
chunkExec c stmts = do
  let go [] = pure ()
      go xs = do
        let (h, t) = splitAt 100 xs
        tryExec c (T.intercalate "; " h)
        go t
  go stmts

-- | CREATE SCHEMA osq + one CREATE VIEW per table + COMMENT ON COLUMN per
-- column. Must drop stale views first (DROP IF EXISTS is per-view).
populateViews :: DB.Conn -> [SchemaTbl] -> IO ()
populateViews c schema = do
  _ <- DB.query c "CREATE SCHEMA IF NOT EXISTS osq"
  -- Drop stale views so removed tables don't linger.
  existing <- listOsqViews c
  forM_ existing $ \v ->
    tryExec c ("DROP VIEW IF EXISTS osq.\"" <> esc v <> "\"")
  let views    = map buildView schema
      comments = concatMap buildComments schema
  chunkExec c (filter (not . T.null) views)
  chunkExec c comments

-- | @SELECT table_name FROM duckdb_views() WHERE schema_name='osq'@.
listOsqViews :: DB.Conn -> IO [Text]
listOsqViews c = do
  r <- try (DB.query c
              "SELECT view_name FROM duckdb_views() WHERE schema_name='osq'")
       :: IO (Either SomeException DB.Result)
  case r of
    Left _  -> pure []
    Right res -> do
      cs <- DB.chunks res
      pure $ concatMap (\ch -> readCol0 ch (DB.chunkSize ch)) cs
  where
    readCol0 ch n =
      let cv = DB.chunkColumn ch 0
      in [ fromMaybe "" (DB.cellText cv i) | i <- [0 .. n - 1] ]

-- | @CREATE VIEW osq."<name>" AS SELECT NULL::T AS "c1", ...@.
buildView :: SchemaTbl -> Text
buildView (SchemaTbl tn _ cols)
  | T.null tn || null cols = ""
  | otherwise =
      let colDefs = T.intercalate ", "
            [ "NULL::" <> typeMap (scType c) <> " AS \"" <> scName c <> "\""
            | c <- cols, not (T.null (scName c)) ]
      in "CREATE VIEW osq.\"" <> tn <> "\" AS SELECT " <> colDefs

-- | @COMMENT ON COLUMN osq."<t>"."<c>" IS '<desc>'@ for every non-blank desc.
buildComments :: SchemaTbl -> [Text]
buildComments (SchemaTbl tn _ cols) =
  [ "COMMENT ON COLUMN osq.\"" <> tn <> "\".\""
    <> scName c <> "\" IS '" <> esc (scDesc c) <> "'"
  | c <- cols, not (T.null tn), not (T.null (scName c)), not (T.null (scDesc c)) ]

-- ----------------------------------------------------------------------------
-- Row count cache
-- ----------------------------------------------------------------------------

-- | If an existing @listing@ has fresh @updated_at@, reuse the counts.
cachedCounts :: DB.Conn -> IO (Maybe [(Text, Int)])
cachedCounts c = do
  r <- try (DB.query c
              "SELECT name, rows, updated_at FROM listing WHERE rows IS NOT NULL")
       :: IO (Either SomeException DB.Result)
  case r of
    Left _    -> pure Nothing
    Right res -> do
      cs <- DB.chunks res
      let rows = concatMap readRows cs
      case rows of
        []                  -> pure Nothing
        ((_, _, ts) : _) -> do
          now <- realToFrac <$> getPOSIXTime :: IO Double
          if now - ts > countsAge
            then pure Nothing
            else pure $ Just [(n, r') | (n, r', _) <- rows]
  where
    readRows ch =
      let n       = DB.chunkSize ch
          nameCV  = DB.chunkColumn ch 0
          rowsCV  = DB.chunkColumn ch 1
          tsCV    = DB.chunkColumn ch 2
      in [ ( fromMaybe "" (DB.cellText nameCV i)
           , maybe 0 fromIntegral (DB.cellInt rowsCV i)
           , fromMaybe 0 (DB.cellDbl tsCV i)
           )
         | i <- [0 .. n - 1] ]

-- | Count rows concurrently (cap @concLimit@) for every non-dangerous name.
countAll :: [Text] -> IO [(Text, Int)]
countAll names = do
  sem <- newQSemN concLimit
  results <- forConcurrently safe $ \n -> withSem sem (countOne n)
  pure [(n, v) | (n, Just v) <- results]
  where
    safe = [n | n <- names, n `notElem` dangerous]

withSem :: QSemN -> IO a -> IO a
withSem sem = bracket_ (waitQSemN sem 1) (signalQSemN sem 1)

-- ----------------------------------------------------------------------------
-- Listing table
-- ----------------------------------------------------------------------------

-- | Build and write the @main.listing@ table via temp JSON + read_json_auto.
writeListing :: DB.Conn -> [Text] -> [(Text, Int)] -> [(Text, Text)] -> IO ()
writeListing c names counts descs = do
  now <- realToFrac <$> getPOSIXTime :: IO Double
  let safety n = if n `elem` dangerous then "input-required" else "safe" :: Text
      row n = Object $ KM.fromList
        [ (K.fromText "name",        String n)
        , (K.fromText "safety",      String (safety n))
        , (K.fromText "rows",        maybe Null (A.toJSON @Int) (lookup n counts))
        , (K.fromText "description", String (fromMaybe "" (lookup n descs)))
        , (K.fromText "updated_at",  A.toJSON @Double now)
        ]
      listing = A.Array (V.fromList (map row names))
  d <- cacheDir
  let tmp = d <> "/tmp_listing.json"
  BL.writeFile tmp (A.encode listing)
  tryExec c "DROP TABLE IF EXISTS listing"
  tryExec c ("CREATE TABLE listing AS SELECT * FROM read_json_auto('"
             <> T.pack tmp <> "')")
  _ <- try (removeFile tmp) :: IO (Either SomeException ())
  pure ()

-- ----------------------------------------------------------------------------
-- Entry point
-- ----------------------------------------------------------------------------

-- | Full setup: names → schema → views/comments → counts → listing table.
-- Matches scripts/osquery_tables.py step-for-step; called by
-- "Tv.Source.Osquery" when its ATTACH on an absent/stale file fails.
run :: IO ()
run = do
  names <- tableNames
  if null names
    then Log.errorLog "osquery-setup: no osquery tables found"
    else do
      p <- dbPath   -- also ensures ~/.cache/tv exists via cacheDir
      c <- DB.connect p
      _ <- DB.query c "SET memory_limit='1GB'"
      version <- osqVersion
      mSchema <- loadSchema version
      let schema = maybe [] parseSchema mSchema
          descs  = [(stName t, stDesc t) | t <- schema]
      unless (null schema) (populateViews c schema)
      mCached <- cachedCounts c
      counts <- maybe (countAll names) pure mCached
      writeListing c names counts descs
      DB.disconnect c
      Log.write "osquery-setup"
        ("populated " <> T.pack p <> ": " <> T.pack (show (length names)) <> " tables")
