{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
-- | Populate ~/.cache/tv/osquery.duckdb with osquery table metadata:
-- `main.listing` (name, safety, rows, description) and `osq.<tbl>` stub
-- views carrying typed columns + COMMENT ON COLUMN. Schema is fetched from
-- the osquery-site repo and cached 30 days; row counts are cached 24 h.
module Tv.Source.OsquerySetup (run) where

import Control.Concurrent.Async (forConcurrently)
import Control.Concurrent.QSemN (newQSemN, signalQSemN, waitQSemN)
import Control.Exception (bracket, bracket_)
import Control.Monad (join, unless)
import Data.Aeson (Value (..), decode, encode, object, toJSON, (.=))
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString.Lazy as BL
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Read as TR
import Data.Time.Clock (diffUTCTime, getCurrentTime)
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector as V
import System.Directory (createDirectoryIfMissing, doesFileExist, getModificationTime, removeFile)
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode)

import qualified Tv.Data.DuckDB as DB
import qualified Tv.Log as Log
import qualified Tv.Source.Core as Core

dangerous :: [Text]
dangerous =
  [ "hash", "file", "augeas", "yara", "curl", "curl_certificate"
  , "magic", "device_file", "carves", "suid_bin"
  , "file_events", "process_events", "process_file_events", "socket_events"
  , "hardware_events", "selinux_events", "seccomp_events", "syslog_events"
  , "user_events", "apparmor_events"
  ]

schemaUrlBase :: Text
schemaUrlBase = "https://raw.githubusercontent.com/osquery/osquery-site/"
             <> "source/src/data/osquery_schema_versions/"

schemaAge, countsAge :: Double
schemaAge = 30 * 24 * 3600
countsAge = 24 * 3600

concLimit :: Int
concLimit = 32

typeMap :: Text -> Text
typeMap t = case T.toUpper t of
  "TEXT"            -> "VARCHAR"
  "INTEGER"         -> "BIGINT"
  "BIGINT"          -> "BIGINT"
  "DOUBLE"          -> "DOUBLE"
  "BLOB"            -> "BLOB"
  "UNSIGNED_BIGINT" -> "UBIGINT"
  _                 -> "VARCHAR"

esc :: Text -> Text
esc = T.replace "'" "''"

cache :: FilePath -> IO FilePath
cache name = do
  d <- Log.dir  -- ~/.cache/tv
  createDirectoryIfMissing True d
  pure (d <> "/" <> name)

now_ :: IO Double
now_ = realToFrac <$> getPOSIXTime

-- ## osqueryi subprocess (timeout-wrapped JSON)

-- | Run @timeout -k 1 <secs> osqueryi --json <sql>@; decode stdout as Value.
osq :: Int -> Text -> IO (Maybe Value)
osq secs sql = do
  r <- Core.try_ $ readProcessWithExitCode "timeout"
         [ "-k", "1", show secs, "osqueryi", "--json"
         , "--disable_events=true", T.unpack sql ] ""
  pure $ case r of
    Just (ExitSuccess, out, _) -> decode (BL.fromStrict (TE.encodeUtf8 (T.pack out)))
    _                          -> Nothing

tableNames :: IO [Text]
tableNames =
  fromMaybe [] . fmap (jsonStrCol "name") <$>
    osq 5 "SELECT name FROM osquery_registry WHERE registry='table' ORDER BY name"

-- | Count rows via osqueryi. Returns Nothing on timeout/error. Table name is
-- double-quoted so odd registry names can't break the SQL.
countOne :: Text -> IO (Text, Maybe Int)
countOne name = do
  m <- osq 2 $ "SELECT count(*) AS n FROM \"" <> name <> "\""
  pure (name, m >>= jsonIntCol "n")

osqVersion :: IO Text
osqVersion = do
  r <- Core.try_ (readProcessWithExitCode "osqueryi" ["--version"] "")
  pure $ case r of
    Just (ExitSuccess, out, _) ->
      let ws = words out in if null ws then "5.21.0" else T.pack (last ws)
    _ -> "5.21.0"

-- ## JSON helpers

jsonStr :: Text -> Value -> Maybe Text
jsonStr k (Object o) = case KM.lookup (K.fromText k) o of
  Just (String s) -> Just s
  Just (Number n) -> Just (T.pack (show n))
  _               -> Nothing
jsonStr _ _ = Nothing

jsonStrCol :: Text -> Value -> [Text]
jsonStrCol k (Array a) = mapMaybe (jsonStr k) (V.toList a)
jsonStrCol _ _         = []

-- | First row's @k@ as Int. osqueryi emits numbers as strings in JSON.
jsonIntCol :: Text -> Value -> Maybe Int
jsonIntCol k (Array a) = case V.toList a of
  r:_ -> jsonStr k r >>= parseInt
  _   -> Nothing
  where
    parseInt t = case TR.decimal t of
      Right (n, rest) | T.null rest -> Just n
      _                             -> Nothing
jsonIntCol _ _ = Nothing

-- ## Schema (30-day cache)

type Col = (Text, Text, Text)  -- (name, type, description)
type Tbl = (Text, Text, [Col]) -- (name, description, cols)

parseSchema :: Value -> [Tbl]
parseSchema (Array arr) = mapMaybe pTbl (V.toList arr)
  where
    pTbl v = do
      nm <- jsonStr "name" v
      let d  = fromMaybe "" (jsonStr "description" v)
          cs = case v of Object o -> case KM.lookup "columns" o of
                           Just (Array a) -> V.toList a; _ -> []
                         _ -> []
      pure (nm, d, mapMaybe pCol cs)
    pCol v = do
      cn <- jsonStr "name" v
      pure (cn, fromMaybe "TEXT" (jsonStr "type" v), fromMaybe "" (jsonStr "description" v))
parseSchema _ = []

isFresh :: FilePath -> Double -> IO Bool
isFresh p maxAge_ = do
  ex <- doesFileExist p
  if not ex then pure False else do
    t <- getModificationTime p
    u <- getCurrentTime
    pure $ realToFrac (diffUTCTime u t) < maxAge_

loadSchema :: Text -> IO (Maybe Value)
loadSchema version = do
  p <- cache "osquery_schema.json"
  ok <- isFresh p schemaAge
  unless ok $ do
    _ <- Core.fetchFile (schemaUrlBase <> version <> ".json") p
    pure ()
  ex <- doesFileExist p
  if ex then decode <$> BL.readFile p else pure Nothing

-- ## DuckDB writes

-- | Run a ;-joined batch of statements 100 at a time, swallowing errors
-- (idempotent DDL only). Empty statements are dropped.
silentBatch :: DB.Conn -> [Text] -> IO ()
silentBatch c = go . filter (not . T.null)
  where
    go [] = pure ()
    go xs = let (h, t) = splitAt 100 xs
            in Core.ignoreErrs (DB.query c (T.intercalate "; " h)) >> go t

listOsqViews :: DB.Conn -> IO [Text]
listOsqViews c = fmap (fromMaybe []) $ Core.try_ $ do
  r <- DB.query c "SELECT view_name FROM duckdb_views() WHERE schema_name='osq'"
  cs <- DB.chunks r
  pure $ concatMap (\ch -> let n = DB.chunkSize ch
                               cv = DB.chunkColumn ch 0
                           in [fromMaybe "" (DB.cellText cv i) | i <- [0 .. n - 1]]) cs

populateViews :: DB.Conn -> [Tbl] -> IO ()
populateViews c schema = do
  _ <- DB.query c "CREATE SCHEMA IF NOT EXISTS osq"
  stale <- listOsqViews c
  silentBatch c ["DROP VIEW IF EXISTS osq.\"" <> esc v <> "\"" | v <- stale]
  silentBatch c (map viewStmt schema)
  silentBatch c [cmnt tn cn cd | (tn, _, cs) <- schema, (cn, _, cd) <- cs
                              , not (T.null tn), not (T.null cn), not (T.null cd)]
  where
    viewStmt (tn, _, cs)
      | T.null tn || null cs = ""
      | otherwise = "CREATE VIEW osq.\"" <> tn <> "\" AS SELECT " <>
          T.intercalate ", " [ "NULL::" <> typeMap ct <> " AS \"" <> cn <> "\""
                             | (cn, ct, _) <- cs, not (T.null cn) ]
    cmnt tn cn cd = "COMMENT ON COLUMN osq.\"" <> tn <> "\".\""
                 <> cn <> "\" IS '" <> esc cd <> "'"

-- ## Row-count cache

-- | Reuse row counts from @listing@ if @updated_at@ is <countsAge old.
cachedCounts :: DB.Conn -> IO (Maybe [(Text, Int)])
cachedCounts c = fmap join $ Core.try_ $ do
  r <- DB.query c "SELECT name, rows, updated_at FROM listing WHERE rows IS NOT NULL"
  cs <- DB.chunks r
  let rows = concatMap readRows cs
  case rows of
    []              -> pure Nothing
    (_, _, ts) : _ -> do
      t <- now_
      pure $ if t - ts > countsAge
               then Nothing
               else Just [(n, v) | (n, v, _) <- rows]
  where
    readRows ch =
      let n = DB.chunkSize ch
          nameCV = DB.chunkColumn ch 0
          rowsCV = DB.chunkColumn ch 1
          tsCV   = DB.chunkColumn ch 2
      in [ ( fromMaybe "" (DB.cellText nameCV i)
           , maybe 0 fromIntegral (DB.cellInt rowsCV i)
           , fromMaybe 0 (DB.cellDbl tsCV i) )
         | i <- [0 .. n - 1] ]

countAll :: [Text] -> IO [(Text, Int)]
countAll names = do
  sem <- newQSemN concLimit
  rs <- forConcurrently safe $ \n ->
          bracket_ (waitQSemN sem 1) (signalQSemN sem 1) (countOne n)
  pure [(n, v) | (n, Just v) <- rs]
  where safe = filter (`notElem` dangerous) names

-- ## Listing table

writeListing :: DB.Conn -> [Text] -> [(Text, Int)] -> [(Text, Text)] -> IO ()
writeListing c names counts descs = do
  t <- now_
  let safety n | n `elem` dangerous = "input-required"
               | otherwise          = "safe" :: Text
      row n = object
        [ "name"        .= n
        , "safety"      .= safety n
        , "rows"        .= (lookup n counts :: Maybe Int)
        , "description" .= fromMaybe "" (lookup n descs)
        , "updated_at"  .= toJSON @Double t
        ]
      listing = toJSON (map row names)
  tmp <- cache "tmp_listing.json"
  BL.writeFile tmp (encode listing)
  Core.ignoreErrs (DB.query c "DROP TABLE IF EXISTS listing")
  Core.ignoreErrs (DB.query c ("CREATE TABLE listing AS SELECT * FROM read_json_auto('"
                              <> T.pack tmp <> "')"))
  Core.ignoreErrs (removeFile tmp)

-- ## Entry point

-- | Full setup: names → schema → views/comments → counts → listing table.
-- `bracket` on the Conn so disconnect runs even on exceptions.
run :: IO ()
run = do
  names <- tableNames
  if null names
    then Log.errorLog "osquery-setup: no osquery tables found"
    else do
      p <- cache "osquery.duckdb"
      bracket (DB.connect p) DB.disconnect $ \c -> do
        _ <- DB.query c "SET memory_limit='1GB'"
        ver <- osqVersion
        schema <- maybe [] parseSchema <$> loadSchema ver
        let descs = [(tn, td) | (tn, td, _) <- schema]
        unless (null schema) (populateViews c schema)
        counts <- cachedCounts c >>= maybe (countAll names) pure
        writeListing c names counts descs
        Log.write "osquery-setup" $ "populated " <> T.pack p <> ": "
                                 <> T.pack (show (length names)) <> " tables"
