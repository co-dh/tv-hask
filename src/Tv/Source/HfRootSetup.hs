{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Populate ~/.cache/tv/hf_datasets.duckdb with a fresh listing of the top
-- Hugging Face datasets (sorted by downloads). Native Haskell populator — no
-- external python/curl/duckdb-CLI subprocess needed.
--
-- Re-runs only when the existing listing is >24h old (or absent). Uses the main
-- DuckDB Conn: ATTACHes the file as writable, rewrites `listing` via
-- read_json_auto on a tmp file, DETACHes. The caller then ATTACHes read-only.
module Tv.Source.HfRootSetup
  ( run
  , cleanDesc
  , findTag
  , linkNext
  ) where

import Control.Exception (SomeException, try)
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as E
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import Data.Maybe (mapMaybe)
import Data.Scientific (toRealFloat)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector as V
import Network.HTTP.Client (Manager, httpLbs, parseRequest, responseBody, responseHeaders)
import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Types.Header (HeaderName)
import System.Directory (createDirectoryIfMissing, removeFile)

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Log as Log
import qualified Tv.Source.Core as Core

apiBase :: String
apiBase = "https://huggingface.co/api/datasets?limit=1000&sort=downloads&direction=-1&full=true"

-- | Hard cap. Matches the Python script: prevents unbounded growth if the API
-- ever yields millions of pages.
maxDatasets :: Int
maxDatasets = 50000

-- | Cache max age in seconds — refetch if the listing is older than this.
maxAge :: Double
maxAge = 24 * 3600

dbPath :: IO Text
dbPath = do
  home <- Core.homeText
  pure $ home <> "/.cache/tv/hf_datasets.duckdb"

tmpJson :: IO Text
tmpJson = do
  home <- Core.homeText
  pure $ home <> "/.cache/tv/tmp_hf_listing.json"

-- | Ensure ~/.cache/tv exists.
ensureDir :: IO ()
ensureDir = do
  home <- Core.homeText
  createDirectoryIfMissing True $ T.unpack home <> "/.cache/tv"

-- ============================================================================
-- Freshness check
-- ============================================================================

-- | Is the listing <24h old and non-empty? Attaches read-only, queries, detaches.
-- Any failure (file missing, table missing, bad cells) -> not fresh.
checkFresh :: IO Bool
checkFresh = do
  p <- dbPath
  let attach = "ATTACH '" <> p <> "' AS hfchk (READ_ONLY)"
      detach = "DETACH DATABASE IF EXISTS hfchk"
  _ <- try (Conn.query detach) :: IO (Either SomeException Conn.QueryResult)
  r <- try $ do
    _ <- Conn.query attach
    Conn.query "SELECT count(*) AS n, max(updated_at) AS ts FROM hfchk.listing"
  _ <- try (Conn.query detach) :: IO (Either SomeException Conn.QueryResult)
  case r of
    Left (_ :: SomeException) -> pure False
    Right qr -> do
      nr <- Conn.nrows qr
      if nr == 0
        then pure False
        else do
          n  <- Conn.cellInt   qr 0 0
          ts <- Conn.cellFloat qr 0 1
          now <- realToFrac <$> getPOSIXTime
          pure $ n > 0 && (now - ts) < maxAge

-- ============================================================================
-- HTTP paginated fetch
-- ============================================================================

-- | Fetch all dataset entries, following Link: rel="next" until exhausted or cap hit.
-- Accumulates pages in reverse order (O(1) prepend) and flattens once at the end
-- to avoid O(n²) list concatenation across ~50 pages × 1000 entries.
fetchAll :: Manager -> IO [A.Value]
fetchAll mgr = go apiBase [] 0 (0 :: Int)
  where
    flatten = concat . reverse
    go url pages !total page = do
      Log.write "src" $ "hf fetch page " <> T.pack (show (page + 1))
      r <- try (fetchPage mgr url) :: IO (Either SomeException ([A.Value], Maybe String))
      case r of
        Left e -> do
          Log.write "src" $ "hf fetch err: " <> T.pack (show e)
          pure (flatten pages)
        Right (entries, _) | null entries -> pure (flatten pages)
        Right (entries, mNext) ->
          let pages' = entries : pages
              total' = total + length entries
          in if total' >= maxDatasets
               then pure (take maxDatasets (flatten pages'))
               else case mNext of
                      Just next -> go next pages' total' (page + 1)
                      Nothing   -> pure (flatten pages')

-- | One page: GET, parse body as JSON array, extract Link header next URL.
fetchPage :: Manager -> String -> IO ([A.Value], Maybe String)
fetchPage mgr url = do
  req <- parseRequest url
  resp <- httpLbs req { HTTP.responseTimeout = HTTP.responseTimeoutMicro (60 * 1000000) } mgr
  let body = responseBody resp
      entries = case A.decode body :: Maybe [A.Value] of
        Just es -> es
        Nothing -> []
      nextUrl = linkNext (responseHeaders resp)
  pure (entries, nextUrl)

-- | Parse "Link: <url>; rel=\"next\", <url>; rel=\"prev\"" headers for next URL.
-- HeaderName has a case-insensitive Eq instance, so `k == "link"` DTRT.
--
-- >>> linkNext [("link", "<https://a/p2>; rel=\"next\", <https://a/p0>; rel=\"prev\"")]
-- Just "https://a/p2"
-- >>> linkNext [("link", "<https://a/p0>; rel=\"prev\"")]
-- Nothing
-- >>> linkNext []
-- Nothing
linkNext :: [(HeaderName, BS.ByteString)] -> Maybe String
linkNext hs =
  let links = [ v | (k, v) <- hs, k == "link" ]
  in foldr (\b acc -> case acc of Just _ -> acc; Nothing -> parseNext b) Nothing links
  where
    parseNext b =
      -- Split on "," to get "<url>; rel=\"next\"" chunks; find one ending in rel="next".
      let chunks = BS.split ',' b
          match c =
            let c' = BS.dropWhile (== ' ') c
            in if ";" `BS.isInfixOf` c' && "rel=\"next\"" `BS.isInfixOf` c'
                 then extractUrl c'
                 else Nothing
      in foldr (\c a -> case a of Just _ -> a; Nothing -> match c) Nothing chunks
    extractUrl c =
      -- c looks like "<https://...>; rel=\"next\""
      let afterLt = BS.drop 1 $ BS.dropWhile (/= '<') c
          (u, _) = BS.break (== '>') afterLt
      in if BS.null u then Nothing else Just (BS.unpack u)

-- ============================================================================
-- Row extraction
-- ============================================================================

-- | Strip HTML tags, markdown links, markdown punctuation, collapse whitespace,
-- cap at 200 chars. Mirrors the Python regex passes (no HTML parser needed for
-- the dataset card snippets we see).
--
-- >>> cleanDesc "<b>Hello</b>   world"
-- "Hello world"
-- >>> cleanDesc "see [the docs](https://x.io) for more"
-- "see the docs for more"
-- >>> cleanDesc "**bold** _ital_ #hash"
-- "bold ital hash"
cleanDesc :: Text -> Text
cleanDesc = T.take 200 . collapseWs . stripMdChars . stripMdLinks . stripTags
  where
    -- <...> -> ' '.  Scan char-by-char; inside a tag, drop everything until '>'.
    stripTags t = T.pack (go (T.unpack t))
      where
        go []       = []
        go ('<':xs) = ' ' : go (drop 1 (dropWhile (/= '>') xs))
        go (c:xs)   = c : go xs
    -- [text](url) -> text.  Mirrors the Python regex \[([^\]]*)\]\([^)]*\):
    -- only rewrites if we find both "]" and the following "(url)" pair.
    stripMdLinks t = T.pack (goL (T.unpack t))
      where
        goL []       = []
        goL ('[':xs)
          | (inner, ']':'(':rest) <- break (== ']') xs
          , (_    , ')':rest2)    <- break (== ')') rest = inner ++ goL rest2
        goL (c:xs)   = c : goL xs
    stripMdChars = T.filter (`notElem` ("#*_~`>" :: String))
    collapseWs = T.strip . T.unwords . T.words

-- | First tag matching a prefix (e.g. "license:mit" with prefix "license:" -> "mit").
--
-- >>> findTag "license:" ["foo", "license:mit", "bar"]
-- "mit"
-- >>> findTag "license:" ["foo", "bar"]
-- ""
-- >>> findTag "language:" ["language:en", "language:fr"]
-- "en"
findTag :: Text -> [Text] -> Text
findTag pfx ts = case mapMaybe (T.stripPrefix pfx) ts of
  (x:_) -> x
  []    -> ""

-- | Get an entry field as Text, defaulting to "".
getText :: Text -> A.Value -> Text
getText k (A.Object o) = case KM.lookup (K.fromText k) o of
  Just (A.String s) -> s
  _                 -> ""
getText _ _ = ""

-- | Get an entry field as Int, defaulting to 0.
getInt :: Text -> A.Value -> Int
getInt k (A.Object o) = case KM.lookup (K.fromText k) o of
  Just (A.Number s) -> truncate (toRealFloat s :: Double)
  _                 -> 0
getInt _ _ = 0

-- | Get an entry field as Bool, defaulting to False.
-- The HF API returns the `gated` field as either a bool or a string like
-- "auto" / "manual"; we treat any truthy string as gated=True.
getBool :: Text -> A.Value -> Bool
getBool k (A.Object o) = case KM.lookup (K.fromText k) o of
  Just (A.Bool b)   -> b
  Just (A.String s) -> not (T.null s) && T.toLower s /= "false"
  _                 -> False
getBool _ _ = False

-- | Get an entry field as a list of strings (for tags), defaulting to [].
getStrs :: Text -> A.Value -> [Text]
getStrs k (A.Object o) = case KM.lookup (K.fromText k) o of
  Just (A.Array a) -> [ s | A.String s <- V.toList a ]
  _                -> []
getStrs _ _ = []

-- | Get a nested object field (e.g. cardData); defaults to empty Object.
getObj :: Text -> A.Value -> A.Value
getObj k (A.Object o) = case KM.lookup (K.fromText k) o of
  Just v@(A.Object _) -> v
  _                   -> A.Object KM.empty
getObj _ _ = A.Object KM.empty

-- | First 19 chars (ISO-8601 date+time without fractional seconds).
dateHead :: Text -> Text
dateHead = T.take 19

-- | Build a single row JSON object matching the Python schema.
toRow :: Double -> A.Value -> A.Value
toRow now e =
  let tags   = getStrs "tags" e
      card   = getObj  "cardData" e
      pretty = getText "pretty_name" card
      desc0  = if T.null pretty then getText "description" e else pretty
      licTag = findTag "license:" tags
      lic    = if T.null licTag then getText "license" card else licTag
  in obj
       [ ("id",          A.String (getText "id" e))
       , ("author",      A.String (getText "author" e))
       , ("downloads",   intNum (getInt "downloads" e))
       , ("likes",       intNum (getInt "likes" e))
       , ("description", A.String (cleanDesc desc0))
       , ("created",     A.String (dateHead (getText "createdAt" e)))
       , ("modified",    A.String (dateHead (getText "lastModified" e)))
       , ("license",     A.String lic)
       , ("task",        A.String (findTag "task_categories:" tags))
       , ("language",    A.String (findTag "language:" tags))
       , ("gated",       A.Bool (getBool "gated" e))
       , ("updated_at",  A.Number (realToFrac now))
       ]
  where
    obj ps = A.Object $ KM.fromList [ (K.fromText k, v) | (k, v) <- ps ]
    intNum n = A.Number (fromIntegral n)

-- ============================================================================
-- DuckDB write
-- ============================================================================

-- | ATTACH writable, CREATE OR REPLACE TABLE hf.listing AS SELECT * FROM read_json_auto,
-- DETACH. The main Conn is shared, so we name the handle `hfw` to avoid clashing
-- with the read-only `hf` used elsewhere.
writeListing :: FilePath -> IO ()
writeListing jsonPath = do
  p <- dbPath
  let attach = "ATTACH '" <> p <> "' AS hfw"
      create = "CREATE OR REPLACE TABLE hfw.listing AS SELECT * FROM read_json_auto('"
             <> T.pack jsonPath <> "')"
      detach = "DETACH DATABASE IF EXISTS hfw"
  _ <- try (Conn.query detach) :: IO (Either SomeException Conn.QueryResult)
  _ <- Conn.query attach
  _ <- Conn.query create
  _ <- Conn.query detach
  pure ()

-- ============================================================================
-- Top-level entry point
-- ============================================================================

-- | Populate the HF listing DuckDB file if stale/missing. No-op when fresh.
run :: IO ()
run = do
  ensureDir
  fresh <- checkFresh
  if fresh
    then Log.write "src" "hf listing fresh, skipping fetch"
    else do
      Log.write "src" "hf listing stale, fetching"
      mgr <- Core.httpMgr
      entries <- fetchAll mgr
      if null entries
        then Log.write "src" "hf fetch yielded 0 entries"
        else do
          now <- realToFrac <$> getPOSIXTime
          let rows = map (toRow now) entries
              encoded = E.encodingToLazyByteString $ E.list E.value rows
          tmp <- tmpJson
          let tmpS = T.unpack tmp
          BL.writeFile tmpS encoded
          writeListing tmpS
          _ <- try (removeFile tmpS) :: IO (Either SomeException ())
          Log.write "src" $ "hf listing written: " <> T.pack (show (length rows)) <> " rows"
