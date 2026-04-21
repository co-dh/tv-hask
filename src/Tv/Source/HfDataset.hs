{-# LANGUAGE OverloadedStrings #-}
-- | HuggingFace dataset browser (hf://datasets/<org>/<name>/…): lists files
-- inside a dataset via the Hub tree API. When the tree API 404s at the
-- org level, falls back to the `?author=` API for org-level dataset lists.
module Tv.Source.HfDataset (hfDataset) where

import Tv.Prelude
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Data.Vector as V
import System.Directory (createDirectoryIfMissing)

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.FileFormat as FileFormat
import qualified Tv.Remote as Remote
import qualified Tv.Render as Render
import qualified Tv.Tmp as Tmp
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "hf://datasets/"

parentFallback :: Text
parentFallback = "hf://"

-- Tree API lists the repo contents at a subpath (empty {3+} means repo root).
urlTmpl :: Text
urlTmpl = "https://huggingface.co/api/datasets/{1}/{2}/tree/main/{3+}"

-- Fallback: when org-level listing fails (no repo), fetch author's dataset list.
fbUrlTmpl :: Text
fbUrlTmpl = "https://huggingface.co/api/datasets?author={1}"

-- File download URL (http-client follows the 302 to the CDN automatically).
dlUrlTmpl :: Text
dlUrlTmpl = "https://huggingface.co/datasets/{1}/{2}/resolve/main/{3+}"

listSqlTmpl :: Text
listSqlTmpl = "SELECT split_part(path, '/', -1) as name, size, type FROM read_json_auto('{src}')"

fbSqlTmpl :: Text
fbSqlTmpl = "SELECT split_part(id, '/', -1) as name, downloads, likes, description, "
          <> "'directory' as type FROM read_json_auto('{src}')"

hfVars :: Text -> IO (V.Vector (Text, Text), Text)
hfVars path_ = do
  Core.checkShell path_ "path"
  tmpDir <- Tmp.tmpPath "src"
  createDirectoryIfMissing True tmpDir
  let tmpT = T.pack tmpDir
  pure (Core.mkVars pfx_ path_ tmpT (Core.fromPath path_) "", tmpT)

-- | Write fetched JSON body to a temp file, CREATE TEMP TABLE via the given
-- SQL template (which references '{src}'), clean up the tmp file, and return
-- the temp-table name so the caller can apply extra shaping before wrapping.
loadJson :: LBS.ByteString -> Text -> IO Text
loadJson body sqlTmpl = do
  tmpFile <- Tmp.tmpPath "src-list.json"
  LBS.writeFile tmpFile body
  tbl <- tmpName "src"
  let sql = Core.expand sqlTmpl (V.singleton ("src", T.pack tmpFile))
  _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
  Tmp.rmFile tmpFile
  pure tbl

-- | Fallback: author's dataset list when the tree API 404s.
tryFallback :: V.Vector (Text, Text) -> IO (Maybe AdbcTable)
tryFallback vars = do
  mBody <- Core.fetchBytes (Core.expand fbUrlTmpl vars)
  case mBody of
    Nothing   -> pure Nothing
    Just body -> do
      tbl <- loadJson body fbSqlTmpl
      fromTmp tbl

hfList :: Bool -> Text -> IO (Maybe AdbcTable)
hfList _ path_ = do
  let p = if T.isSuffixOf "/" path_ then path_ else path_ <> "/"
  (vars, _) <- hfVars p
  mBody <- Core.fetchBytes (Core.expand urlTmpl vars)
  case mBody of
    Nothing -> do
      mfb <- tryFallback vars
      case mfb of
        Just adbc -> pure (Just adbc)
        Nothing   -> do
          Render.errorPopup "List failed"
          pure Nothing
    Just body -> do
      tbl <- loadJson body listSqlTmpl
      Core.unnestStruct tbl
      Core.addParentRow (hfParent path_) tbl
      fromTmp tbl

-- | hf://datasets/ path: 5-component root (hf:/datasets/<org>/<name>/…);
-- above root falls back to hf:// to land in the dataset index.
hfParent :: Text -> Maybe Text
hfParent p = case Remote.parent p 5 of
  Just par -> Just par
  Nothing  -> Just parentFallback

-- | Data files: DuckDB httpfs reads hf:// URIs directly (pass-through).
-- Non-data files (README, .gitattributes …): download so the text viewer works.
hfOpen :: Bool -> Text -> IO OpenResult
hfOpen _ path_
  | T.isSuffixOf "/" path_ = pure (OpenAsDir path_)
  | FileFormat.isData path_ = pure $ OpenAsFile $ T.unpack path_
  | otherwise = do
      Render.statusMsg ("Downloading " <> path_ <> " ...")
      (vars, tmpDir) <- hfVars path_
      let url  = Core.expand dlUrlTmpl vars
          dest = T.unpack $ tmpDir <> "/" <> Core.fromPath path_
      _ <- Core.fetchFile url dest
      pure (OpenAsFile dest)

hfDataset :: Source
hfDataset = Source
  { pfx    = pfx_
  , parent = hfParent
  , grpCol = Nothing
  , list   = hfList
  , open   = hfOpen
  }
