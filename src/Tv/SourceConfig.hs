{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | SourceConfig: remote data source routing.
--
-- Port of Tc/SourceConfig.lean. Handles non-local URIs: HTTP(S), S3, FTP,
-- HuggingFace (hf://), REST (rest://), PostgreSQL (pg://), osquery.
--
-- Flow mirrors Tc: longest-prefix match picks a 'Config'; the config says
-- whether to list via a shell command (curl/aws) + SQL transform or via
-- direct SQL (ATTACH for pg://). For the MVP we ship:
--   * parseUri    — classify a string, return the matching config
--   * uriToSql    — map a file-ish URI to a DuckDB read_* SELECT
--   * listRemote  — run the list command, parse JSON into a folder-like
--                   TblOps (name/size/date/type)
--   * loadFromUri — open an in-memory DuckDB, run uriToSql, wrap via
--                   mkDbOps. Falls back to listRemote for browsable roots.
--
-- Network-bearing operations shell out to curl / aws via 'process' (no
-- http-conduit dep). Invalid URIs / offline systems return Left, never
-- throw.
module Tv.SourceConfig
  ( Config (..)
  , sources
  , findSource
  , parseUri
  , uriToSql
  , loadFromUri
  , listRemote
  , expand
  , mkVars
  , pathParts
  ) where

import Control.Exception (SomeException, try)
import Data.Aeson (Value (..), decodeStrict, (.:), (.:?))
import qualified Data.Aeson as A
import qualified Data.Aeson.Key as AK
import qualified Data.Aeson.Types as A
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import Data.Char (isDigit)
import Data.List (find, isPrefixOf, maximumBy)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Ord (comparing)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode)

import Tv.Types
import qualified Tv.Data.DuckDB as DB

-- ============================================================================
-- Config: mirrors Tc.SourceConfig.Config (subset of fields we actually use)
-- ============================================================================

data Config = Config
  { cfgPfx           :: !Text  -- URI prefix match
  , cfgMinParts      :: !Int
  , cfgListCmd       :: !Text  -- shell template; empty = run listSql directly
  , cfgListSql       :: !Text  -- SQL to transform JSON (with {src}) or direct query
  , cfgDownloadCmd   :: !Text
  , cfgNeedsDownload :: !Bool
  , cfgParentFallback:: !Text
  , cfgDuckdbExt     :: !Text  -- extension to INSTALL/LOAD
  , cfgAttachType    :: !Text  -- POSTGRES, etc.
  , cfgAttach        :: !Bool  -- uses ATTACH flow
  , cfgFallbackCmd   :: !Text
  , cfgFallbackSql   :: !Text
  } deriving (Show)

defCfg :: Config
defCfg = Config "" 0 "" "" "" False "" "" "" False "" ""

-- | Source configs, longest-prefix match wins. Shape matches the Lean
-- `sources` array — see Tc/SourceConfig.lean for the full list.
sources :: [Config]
sources =
  [ -- S3: aws s3api listing; DuckDB reads via read_parquet('s3://...').
    defCfg { cfgPfx = "s3://", cfgMinParts = 3
           , cfgListCmd = "aws s3api list-objects-v2 --bucket {1} --delimiter / --prefix {2+}/ --output json"
           , cfgListSql = s3ListSql }
  , -- HuggingFace dataset browser: curl the Hub tree API.
    defCfg { cfgPfx = "hf://datasets/", cfgMinParts = 5
           , cfgListCmd = "curl -sf https://huggingface.co/api/datasets/{1}/{2}/tree/main/{3+}"
           , cfgListSql = "hf-tree"
           , cfgDownloadCmd = "curl -sfL -o {tmp}/{name} https://huggingface.co/datasets/{1}/{2}/resolve/main/{3+}"
           , cfgParentFallback = "hf://"
           , cfgFallbackCmd = "curl -sf 'https://huggingface.co/api/datasets?author={1}'"
           , cfgFallbackSql = "hf-author" }
  , -- HF root: listing comes from a pre-populated DuckDB cache. Stub in Haskell
    -- (returns an informational table) until we port hf_datasets.py.
    defCfg { cfgPfx = "hf://", cfgListSql = "hf-root" }
  , -- Generic REST: curl + read_json_auto.
    defCfg { cfgPfx = "rest://", cfgMinParts = 1
           , cfgListCmd = "curl -sfL https://{1+}"
           , cfgListSql = "rest" }
  , -- osquery: requires Python-populated DuckDB cache + osqueryi at enter.
    defCfg { cfgPfx = "osquery://", cfgListSql = "osquery-root" }
  , -- FTP: curl ls; parser for `ls -l` output lives below.
    defCfg { cfgPfx = "ftp://", cfgMinParts = 3
           , cfgListCmd = "curl -sf {path}", cfgListSql = "ftp"
           , cfgDownloadCmd = "curl -sfL -o '{tmp}/{name}' {path}"
           , cfgNeedsDownload = True }
  , -- PostgreSQL: ATTACH via duckdb postgres extension.
    defCfg { cfgPfx = "pg://", cfgMinParts = 99
           , cfgAttach = True, cfgDuckdbExt = "postgres", cfgAttachType = "POSTGRES" }
  , -- HTTP(S) catch-all: handled by uriToSql via read_csv/read_parquet/read_json.
    defCfg { cfgPfx = "https://" }
  , defCfg { cfgPfx = "http://" }
  ]
  where
    s3ListSql = "s3"  -- sentinel: real SQL lives inline in listRemote

-- | Longest-prefix match over 'sources'.
findSource :: Text -> Maybe Config
findSource path =
  let hits = [c | c <- sources, not (T.null (cfgPfx c)), cfgPfx c `T.isPrefixOf` path]
  in if null hits then Nothing else Just (maximumBy (comparing (T.length . cfgPfx)) hits)

-- | Lighter-weight classifier — just returns the prefix's config. Alias of
-- 'findSource' to match the name mentioned in the porting task.
parseUri :: Text -> Maybe Config
parseUri = findSource

-- ============================================================================
-- Path helpers (mirrors pathParts / mkVars / expand in the Lean source)
-- ============================================================================

-- | Split path into components after stripping the prefix and trailing slash.
pathParts :: Text -> Text -> [Text]
pathParts pfx path =
  let rest0 = T.drop (T.length pfx) path
      rest  = if "/" `T.isSuffixOf` rest0 then T.dropEnd 1 rest0 else rest0
  in if T.null rest then [] else T.splitOn "/" rest

-- | Build template vars for (cfg, path). Matches Lean 'mkVars'.
mkVars :: Config -> Text -> Text -> Text -> Text -> [(Text, Text)]
mkVars cfg path tmp nm extra =
  let parts = pathParts (cfgPfx cfg) path
      dsn   = T.drop (T.length (cfgPfx cfg)) path
      base  = [("path", path), ("tmp", tmp), ("name", nm), ("extra", extra), ("dsn", dsn)]
      num   = [(T.pack (show (i + 1)), at parts i) | i <- [0 .. 8]]
      plus  = [(T.pack (show (i + 1)) <> "+", T.intercalate "/" (drop i parts)) | i <- [0 .. 8]]
  in base ++ num ++ plus
  where
    at xs i = if i < length xs then xs !! i else ""

-- | Replace {k} placeholders. Empty values also strip a preceding "/" so
-- "tree/main/{3+}" with empty {3+} becomes "tree/main".
expand :: Text -> [(Text, Text)] -> Text
expand = foldl step
  where
    step acc (k, v)
      | T.null v  = T.replace ("{" <> k <> "}") "" (T.replace ("/{" <> k <> "}") "" acc)
      | otherwise = T.replace ("{" <> k <> "}") v acc

-- ============================================================================
-- URI → DuckDB SQL (for read-time dispatch)
-- ============================================================================

-- | Build a SELECT for a file-like URI by sniffing extension. Returns
-- Nothing for non-file URIs (directories, attach-only schemes).
uriToSql :: Text -> Maybe Text
uriToSql uri
  | ".parquet" `T.isSuffixOf` lo = Just (wrap "read_parquet")
  | ".pq"      `T.isSuffixOf` lo = Just (wrap "read_parquet")
  | ".csv"     `T.isSuffixOf` lo = Just (wrap "read_csv_auto")
  | ".csv.gz"  `T.isSuffixOf` lo = Just (wrap "read_csv_auto")
  | ".tsv"     `T.isSuffixOf` lo = Just ("SELECT * FROM read_csv_auto('" <> esc uri <> "', delim='\t') LIMIT 1000")
  | ".json"    `T.isSuffixOf` lo = Just (wrap "read_json_auto")
  | ".ndjson"  `T.isSuffixOf` lo = Just (wrap "read_ndjson_auto")
  | ".jsonl"   `T.isSuffixOf` lo = Just (wrap "read_ndjson_auto")
  | otherwise                    = Nothing
  where
    lo = T.toLower uri
    esc = T.replace "'" "''"
    wrap fn = "SELECT * FROM " <> fn <> "('" <> esc uri <> "') LIMIT 1000"

-- ============================================================================
-- listRemote: run list command + JSON parse → folder-like TblOps
-- ============================================================================

-- | Fetch a folder-like listing for a remote URI. For protocols where we
-- can actually list (hf://datasets/, s3://bucket/prefix, ftp://), this
-- shells out and parses the response. For configured-but-unimplemented
-- protocols (hf:// root, pg://, osquery://) we return an informative stub
-- so the UI has something to show.
listRemote :: Text -> IO (Either String TblOps)
listRemote uri = case findSource uri of
  Nothing  -> pure (Left ("no source config matches " <> T.unpack uri))
  Just cfg -> dispatch cfg uri

dispatch :: Config -> Text -> IO (Either String TblOps)
dispatch cfg uri = case cfgListSql cfg of
  "s3"          -> listS3 cfg uri
  "hf-tree"     -> listHfTree cfg uri
  "hf-root"     -> stubTbl "hf://" "HF root listing needs scripts/hf_datasets.py — not yet ported"
  "rest"        -> listRest cfg uri
  "osquery-root"-> stubTbl "osquery://" "osquery listing needs the python helper — not yet ported"
  "ftp"         -> listFtp cfg uri
  ""            -> case uriToSql uri of
                     Just sql -> loadViaDuckDb sql
                     Nothing  -> stubTbl uri "no listing and no recognized file extension"
  other         -> pure (Left ("TODO: listSql=" <> T.unpack other))

-- | Attach handler / pg:// — the port would need to spawn DuckDB with the
-- postgres extension. For now, return a clear error.
_loadAttach :: Config -> Text -> IO (Either String TblOps)
_loadAttach _ _ = pure (Left "TODO: pg:// / attach mode")

-- ============================================================================
-- loadFromUri: best-effort open
-- ============================================================================

-- | Open a URI as a TblOps. For file-ish URIs this means
-- `SELECT * FROM read_*('uri')` in an in-memory DuckDB. Browsable roots
-- fall through to 'listRemote'.
loadFromUri :: Text -> IO (Either String TblOps)
loadFromUri uri = case uriToSql uri of
  Just sql -> loadViaDuckDb sql
  Nothing  -> listRemote uri

-- | Run SQL against an in-memory DuckDB and wrap the result. Catches all
-- exceptions so bad URIs degrade to Left.
loadViaDuckDb :: Text -> IO (Either String TblOps)
loadViaDuckDb sql = do
  r <- try $ do
    conn <- DB.connect ":memory:"
    res  <- DB.query conn sql
    ops  <- DB.mkDbOps res
    DB.disconnect conn
    pure ops
  case r of
    Right ops -> pure (Right ops)
    Left (e :: SomeException) -> pure (Left (show e))

-- ============================================================================
-- Protocol-specific listings
-- ============================================================================

-- | S3 list via `aws s3api`. Needs `aws` on PATH. JSON shape:
--   { "Contents": [{"Key":..,"Size":..,"LastModified":..}]
--   , "CommonPrefixes": [{"Prefix":..}] }
listS3 :: Config -> Text -> IO (Either String TblOps)
listS3 cfg uri = do
  let parts = pathParts (cfgPfx cfg) uri
  case parts of
    (bucket : rest) -> do
      let prefix = T.intercalate "/" rest
          pfx    = if T.null prefix then "" else prefix <> "/"
      out <- runCmd "aws"
        [ "s3api", "list-objects-v2"
        , "--bucket", T.unpack bucket
        , "--delimiter", "/"
        , "--prefix", T.unpack pfx
        , "--output", "json" ]
      case out of
        Left err -> pure (Left ("s3 list failed: " <> err))
        Right bs -> case decodeStrict bs :: Maybe Value of
          Nothing -> pure (Left "s3 list: invalid JSON")
          Just v  -> do
            let files = parseList v "Contents" s3File
                dirs  = parseList v "CommonPrefixes" s3Dir
                rows  = parentRow : dirs ++ files
            Right <$> mkFolderTbl rows
    _ -> pure (Left "s3: need bucket")
  where
    s3File o =
      let k = lookupStr "Key" o
          name = last (T.splitOn "/" k)
      in if T.null name then Nothing
         else Just (Row name (lookupStr "Size" o) (lookupStr "LastModified" o) "file")
    s3Dir o =
      let p = lookupStr "Prefix" o
          name = case reverse (filter (not . T.null) (T.splitOn "/" p)) of
                   (n:_) -> n; _ -> ""
      in if T.null name then Nothing
         else Just (Row name "0" "" "dir")

-- | HF dataset tree. API returns a JSON array of entries with fields
-- path / size / type (file|directory).
listHfTree :: Config -> Text -> IO (Either String TblOps)
listHfTree cfg uri = do
  let parts = pathParts (cfgPfx cfg) uri
  case parts of
    (org : repo : rest) -> do
      let sub = T.intercalate "/" rest
          url = "https://huggingface.co/api/datasets/" <> org <> "/" <> repo <> "/tree/main/" <> sub
      out <- runCmd "curl" ["-sf", T.unpack url]
      case out of
        Left _err -> -- try org-level fallback listing
          fallbackAuthor cfg org
        Right bs -> case decodeStrict bs :: Maybe Value of
          Nothing -> pure (Left "hf tree: invalid JSON")
          Just v -> do
            let items = case v of Array a -> V.toList a; _ -> []
                row o  =
                  let path = lookupStr "path" o
                      name = last (T.splitOn "/" path)
                      sz   = lookupStr "size" o
                      ty   = lookupStr "type" o
                      ty'  = if ty == "directory" then "dir" else ty
                  in if T.null name then Nothing
                     else Just (Row name sz "" ty')
                rows = parentRow : mapMaybe (objRow row) items
            Right <$> mkFolderTbl rows
    _ -> pure (Left "hf: need datasets/org/repo")

fallbackAuthor :: Config -> Text -> IO (Either String TblOps)
fallbackAuthor _ org = do
  out <- runCmd "curl" ["-sf", "https://huggingface.co/api/datasets?author=" <> T.unpack org]
  case out of
    Left err -> pure (Left ("hf fallback failed: " <> err))
    Right bs -> case decodeStrict bs :: Maybe Value of
      Nothing -> pure (Left "hf fallback: invalid JSON")
      Just v  -> do
        let items = case v of Array a -> V.toList a; _ -> []
            row o =
              let i    = lookupStr "id" o
                  name = last (T.splitOn "/" i)
              in if T.null name then Nothing
                 else Just (Row name (lookupStr "downloads" o) (lookupStr "likes" o) "dir")
            rows = parentRow : mapMaybe (objRow row) items
        Right <$> mkFolderTbl rows

-- | REST protocol — curl the URL (with scheme replaced), return raw JSON as
-- a single-column text table via DuckDB's read_json_auto. This mirrors
-- Lean's path through listSql (but we can't easily get a tempfile into
-- DuckDB without a scratch directory, so we delegate to a temp file).
listRest :: Config -> Text -> IO (Either String TblOps)
listRest cfg uri = do
  let parts = pathParts (cfgPfx cfg) uri
      url   = "https://" <> T.intercalate "/" parts
  out <- runCmd "curl" ["-sfL", T.unpack url]
  case out of
    Left err -> pure (Left ("rest failed: " <> err))
    Right bs ->
      let t = TE.decodeUtf8 bs
      in Right <$> mkTextTbl "body" (T.lines t)

-- | FTP: curl dumps an `ls -l` style listing, parse it row-wise.
listFtp :: Config -> Text -> IO (Either String TblOps)
listFtp _ uri = do
  out <- runCmd "curl" ["-sf", T.unpack uri]
  case out of
    Left err -> pure (Left ("ftp failed: " <> err))
    Right bs ->
      let t = TE.decodeUtf8 bs
          rows = parentRow : mapMaybe parseLsLine (T.lines t)
      in Right <$> mkFolderTbl rows

-- | Parse one line of `ls -l`. Format:
--   drwxr-xr-x 1 u g   SIZE Mon DD HH:MM name
-- We want name/size/date/type. Be lenient: split on whitespace, take
-- perms[0] for type (d/l/-), cols [4] = size, [5..7] = date, rest = name.
parseLsLine :: Text -> Maybe Row
parseLsLine line =
  let ws = T.words line
  in case ws of
    (perms : _ : _ : _ : sz : m : d : hhmm : rest) | not (null rest) ->
      let ty = case T.uncons perms of
                 Just ('d', _) -> "dir"
                 Just ('l', _) -> "symlink"
                 _             -> "file"
          name = T.intercalate " " rest
          date = T.unwords [m, d, hhmm]
      in Just (Row name sz date ty)
    _ -> Nothing

-- ============================================================================
-- Stub / generic table builders
-- ============================================================================

-- | A single row for folder-style listings.
data Row = Row !Text !Text !Text !Text  -- name size date type

parentRow :: Row
parentRow = Row ".." "0" "" "dir"

-- | Build a 4-column TblOps from a list of Rows. Shape matches
-- 'Tv.Folder.listFolder' (name/size/modified/type).
mkFolderTbl :: [Row] -> IO TblOps
mkFolderTbl rs = do
  let rows = V.fromList rs
      nr   = V.length rows
      cols = V.fromList ["name", "size", "date", "type"]
      nc   = V.length cols
      cell (Row a _ _ _) 0 = a
      cell (Row _ b _ _) 1 = b
      cell (Row _ _ c _) 2 = c
      cell (Row _ _ _ d) 3 = d
      cell _             _ = T.empty
  let ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = cols
        , _tblTotalRows   = nr
        , _tblFilter      = \_ -> pure Nothing
        , _tblDistinct    = \_ -> pure V.empty
        , _tblFindRow     = \_ _ _ _ -> pure Nothing
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = \_ -> CTStr
        , _tblBuildFilter = \_ _ _ _ -> T.empty
        , _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr     = \r c ->
            if r < 0 || r >= nr || c < 0 || c >= nc
              then pure T.empty
              else pure (cell (rows V.! r) c)
        , _tblFetchMore   = pure Nothing
        , _tblHideCols    = \_ -> pure ops
        , _tblSortBy      = \_ _ -> pure ops
        }
  pure ops

-- | Single-column TblOps of text lines. Used for best-effort REST bodies.
mkTextTbl :: Text -> [Text] -> IO TblOps
mkTextTbl colName ls = do
  let rows = V.fromList ls
      nr   = V.length rows
      cols = V.fromList [colName]
  let ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = cols
        , _tblTotalRows   = nr
        , _tblFilter      = \_ -> pure Nothing
        , _tblDistinct    = \_ -> pure V.empty
        , _tblFindRow     = \_ _ _ _ -> pure Nothing
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = \_ -> CTStr
        , _tblBuildFilter = \_ _ _ _ -> T.empty
        , _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr     = \r c ->
            if r < 0 || r >= nr || c /= 0 then pure T.empty else pure (rows V.! r)
        , _tblFetchMore   = pure Nothing
        , _tblHideCols    = \_ -> pure ops
        , _tblSortBy      = \_ _ -> pure ops
        }
  pure ops

-- | Informational one-row folder table (used for hf://, osquery:// roots
-- that need the Python helpers we haven't ported yet).
stubTbl :: Text -> Text -> IO (Either String TblOps)
stubTbl name msg = Right <$> mkFolderTbl [parentRow, Row name "0" msg "info"]

-- ============================================================================
-- Small utilities
-- ============================================================================

-- | Run an external process, returning stdout as ByteString or the
-- failure reason (non-zero exit or missing binary). Matches 'readProcess'
-- semantics but catches exceptions for robustness.
runCmd :: FilePath -> [String] -> IO (Either String BS.ByteString)
runCmd bin args = do
  r <- try (readProcessWithExitCode bin args "") :: IO (Either SomeException (ExitCode, String, String))
  pure $ case r of
    Left e -> Left (show e)
    Right (ExitSuccess, o, _)   -> Right (BSC.pack o)
    Right (ExitFailure c, _, e) -> Left (bin <> " exit " <> show c <> ": " <> e)

-- | Field access for aeson 'Value' with string fallback. Numbers and bools
-- are stringified so folder columns stay uniform.
lookupStr :: Text -> A.Object -> Text
lookupStr k o = case A.parseMaybe (\x -> x .: AK.fromText k :: A.Parser Value) o of
  Just (String s) -> s
  Just (Number n) -> T.pack (show n)
  Just (Bool b)   -> if b then "true" else "false"
  _               -> ""

-- | Pull an array field from a top-level object and map 'f' over its
-- object elements.
parseList :: Value -> Text -> (A.Object -> Maybe Row) -> [Row]
parseList (Object o) key f =
  case A.parseMaybe (\x -> x .:? AK.fromText key :: A.Parser (Maybe Value)) o of
    Just (Just (Array xs)) -> mapMaybe (objRow f) (V.toList xs)
    _                      -> []
parseList _ _ _ = []

objRow :: (A.Object -> Maybe Row) -> Value -> Maybe Row
objRow f (Object o) = f o
objRow _ _          = Nothing
