{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
-- | Populate ~/.cache/tv/hf_datasets.duckdb with the top Hugging Face datasets
-- (sorted by downloads). Paginates the API, cleans descriptions, writes via
-- read_json_auto on a temp file. Refreshes when existing listing is >24h old.
module Tv.Source.HfRootSetup where

import Tv.Prelude
import Control.Exception (bracket_)
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as E
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import Data.Scientific (toRealFloat)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector as V
import Network.HTTP.Client (httpLbs, parseRequest, responseBody, responseHeaders, responseTimeout, responseTimeoutMicro)
import Network.HTTP.Types.Header (HeaderName)
import System.Directory (createDirectoryIfMissing, removeFile)

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Util as Log
import qualified Tv.Source.Core as Core

apiBase :: String
apiBase = "https://huggingface.co/api/datasets?limit=1000&sort=downloads&direction=-1&full=true"

maxDatasets :: Int
maxDatasets = 50000

maxAge :: Double
maxAge = 24 * 3600

cache :: FilePath -> IO FilePath
cache name = do
  d <- (<> "/.cache/tv") . T.unpack <$> Core.homeText
  createDirectoryIfMissing True d
  pure (d <> "/" <> name)

now_ :: IO Double
now_ = realToFrac <$> getPOSIXTime

-- | Listing is fresh iff it exists and has updated_at < maxAge seconds old.
-- Uses a separate ATTACH handle (@hfchk@) so we don't clash with the
-- read-only @hf@ alias the main source uses at query time.
checkFresh :: IO Bool
checkFresh = fmap (fromMaybe False) $ Core.try_ $ do
  p <- cache "hf_datasets.duckdb"
  bracket_
    (q_ ("ATTACH '" <> T.pack p <> "' AS hfchk (READ_ONLY)"))
    (q_ "DETACH DATABASE IF EXISTS hfchk")
    $ do
      qr <- Conn.query "SELECT count(*), max(updated_at) FROM hfchk.listing"
      if Conn.nrows qr == 0 then pure False else do
        n  <- Conn.cellInt   qr 0 0
        ts <- Conn.cellFloat qr 0 1
        t  <- now_
        pure $ n > 0 && t - ts < maxAge

-- ## HTTP paginated fetch

-- | Accumulate pages in reverse (O(1) cons), flatten once at the end.
fetchAll :: IO [A.Value]
fetchAll = do
  mgr <- Core.httpMgr
  let page url = fmap (fromMaybe ([], Nothing)) $ Core.try_ $ do
        req <- parseRequest url
        r <- httpLbs req { responseTimeout = responseTimeoutMicro (60 * 1000000) } mgr
        let es = fromMaybe [] (A.decode (responseBody r))
        pure (es, linkNext (responseHeaders r))
      go url !pages !total !n = do
        Log.write "src" $ "hf fetch page " <> T.pack (show (n + 1))
        (es, next) <- page url
        if null es
          then pure (concat (reverse pages))
          else do
            let pages' = es : pages
                total' = total + length es
            if total' >= maxDatasets
              then pure (take maxDatasets (concat (reverse pages')))
              else maybe (pure (concat (reverse pages')))
                         (\u -> go u pages' total' (n + 1))
                         next
  go apiBase [] 0 (0 :: Int)

-- | Parse @Link@ header for @rel="next"@.
--
-- >>> linkNext [("link", "<https://a/p2>; rel=\"next\", <https://a/p0>; rel=\"prev\"")]
-- Just "https://a/p2"
-- >>> linkNext [("link", "<https://a/p0>; rel=\"prev\"")]
-- Nothing
-- >>> linkNext []
-- Nothing
linkNext :: [(HeaderName, BS.ByteString)] -> Maybe String
linkNext hs = listToMaybe
  [ BS.unpack u
  | (k, v) <- hs, k == "link"
  , c <- BS.split ',' v
  , "rel=\"next\"" `BS.isInfixOf` c
  , let u = BS.takeWhile (/= '>') (BS.drop 1 (BS.dropWhile (/= '<') c))
  , not (BS.null u)
  ]

-- ## Entry → row

-- | Pick a field from a JSON object and coerce via @pick@; default when absent.
get :: (A.Value -> Maybe a) -> a -> Text -> A.Value -> a
get pick d k (A.Object o) = fromMaybe d (KM.lookup (K.fromText k) o >>= pick)
get _    d _ _            = d

getT :: Text -> A.Value -> Text
getT = get (\case A.String s -> Just s; _ -> Nothing) ""

getI :: Text -> A.Value -> Int
getI = get (\case A.Number s -> Just (truncate (toRealFloat s :: Double)); _ -> Nothing) 0

-- | @gated@ is bool-or-string in the API; any non-empty non-\"false\" → True.
getB :: Text -> A.Value -> Bool
getB = get pick False
  where
    pick (A.Bool b)   = Just b
    pick (A.String s) = Just (not (T.null s) && T.toLower s /= "false")
    pick _            = Nothing

getTags :: A.Value -> [Text]
getTags = get (\case A.Array a -> Just [s | A.String s <- V.toList a]; _ -> Nothing) [] "tags"

card :: A.Value -> A.Value
card = get (\case v@(A.Object _) -> Just v; _ -> Nothing) (A.Object KM.empty) "cardData"

-- | Clean HTML/markdown noise from description, collapse whitespace, cap at 200.
--
-- >>> cleanDesc "<b>Hello</b>   world"
-- "Hello world"
-- >>> cleanDesc "see [the docs](https://x.io) for more"
-- "see the docs for more"
-- >>> cleanDesc "**bold** _ital_ #hash"
-- "bold ital hash"
cleanDesc :: Text -> Text
cleanDesc = T.take 200 . T.unwords . T.words . T.filter (`notElem` md) . links . tags
  where
    md = "#*_~`>" :: String
    tags t = case T.breakOn "<" t of
      (a, "") -> a
      (a, b)  -> a <> " " <> tags (T.drop 1 (T.dropWhile (/= '>') (T.drop 1 b)))
    links t = case T.breakOn "[" t of
      (a, "") -> a
      (a, b)  -> case T.breakOn "](" (T.drop 1 b) of
        (_, "")       -> a <> b
        (inner, rest) -> a <> inner <> links (T.drop 1 (T.dropWhile (/= ')') rest))

-- | First tag matching a prefix (e.g. "license:mit" + "license:" → "mit").
--
-- >>> findTag "license:" ["foo", "license:mit", "bar"]
-- "mit"
-- >>> findTag "license:" ["foo", "bar"]
-- ""
-- >>> findTag "language:" ["language:en", "language:fr"]
-- "en"
findTag :: Text -> [Text] -> Text
findTag p = fromMaybe "" . listToMaybe . mapMaybe (T.stripPrefix p)

toRow :: Double -> A.Value -> A.Value
toRow t e = A.object
  [ "id"          A..= getT "id" e
  , "author"      A..= getT "author" e
  , "downloads"   A..= getI "downloads" e
  , "likes"       A..= getI "likes" e
  , "description" A..= cleanDesc desc
  , "created"     A..= T.take 19 (getT "createdAt"    e)
  , "modified"    A..= T.take 19 (getT "lastModified" e)
  , "license"     A..= license
  , "task"        A..= findTag "task_categories:" tags
  , "language"    A..= findTag "language:"        tags
  , "gated"       A..= getB "gated" e
  , "updated_at"  A..= t
  ]
  where
    tags    = getTags e
    c       = card e
    pretty  = getT "pretty_name" c
    desc    = if T.null pretty then getT "description" e else pretty
    licTag  = findTag "license:" tags
    license = if T.null licTag then getT "license" c else licTag

-- ## DuckDB write

-- | Conn.query returns QueryResult; discard so bracket_ can type at IO ().
q_ :: Text -> IO ()
q_ sql = Conn.query sql >> pure ()

writeListing :: FilePath -> IO ()
writeListing tmp = do
  p <- cache "hf_datasets.duckdb"
  bracket_
    (q_ ("ATTACH '" <> T.pack p <> "' AS hfw"))
    (q_ "DETACH DATABASE IF EXISTS hfw")
    (q_ $ "CREATE OR REPLACE TABLE hfw.listing AS SELECT * FROM read_json_auto('"
        <> T.pack tmp <> "')")

-- | Refetch the HF listing if stale or missing; no-op when fresh.
run :: IO ()
run = do
  fresh <- checkFresh
  if fresh then Log.write "src" "hf listing fresh, skipping fetch" else do
    Log.write "src" "hf listing stale, fetching"
    es <- fetchAll
    if null es then Log.write "src" "hf fetch yielded 0 entries" else do
      t   <- now_
      tmp <- cache "tmp_hf_listing.json"
      BL.writeFile tmp $ E.encodingToLazyByteString (E.list E.value (map (toRow t) es))
      writeListing tmp
      Core.ignoreErrs (removeFile tmp)
      Log.write "src" $ "hf listing written: " <> T.pack (show (length es)) <> " rows"
