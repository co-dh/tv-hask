{-# LANGUAGE OverloadedStrings #-}
-- | HuggingFace dataset browser (hf://datasets/<org>/<name>/…): lists files
-- inside a dataset via the Hub tree API. When the tree API 404s at the
-- org level, falls back to the `?author=` API for org-level dataset lists.
module Tv.Source.HfDataset (hfDataset) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Directory (createDirectoryIfMissing)
import System.Exit (ExitCode (..))

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Log as Log
import qualified Tv.Remote as Remote
import qualified Tv.Render as Render
import qualified Tv.Tmp as Tmp
import Tv.Source.Core (Source (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "hf://datasets/"

parentFallback :: Text
parentFallback = "hf://"

cmdTmpl :: Text
cmdTmpl = "curl -sf https://huggingface.co/api/datasets/{1}/{2}/tree/main/{3+}"

-- Fallback: when org-level listing fails (no repo), fetch author's dataset list.
fbCmdTmpl :: Text
fbCmdTmpl = "curl -sf 'https://huggingface.co/api/datasets?author={1}'"

listSqlTmpl :: Text
listSqlTmpl = "SELECT split_part(path, '/', -1) as name, size, type FROM read_json_auto('{src}')"

fbSqlTmpl :: Text
fbSqlTmpl = "SELECT split_part(id, '/', -1) as name, downloads, likes, description, "
          <> "'directory' as type FROM read_json_auto('{src}')"

dlTmpl :: Text
dlTmpl = "curl -sfL -o {tmp}/{name} https://huggingface.co/datasets/{1}/{2}/resolve/main/{3+}"

hfVars :: Text -> IO (V.Vector (Text, Text), Text)
hfVars path_ = do
  Core.checkShell path_ "path"
  tmpDir <- Tmp.tmpPath "src"
  createDirectoryIfMissing True tmpDir
  _ <- Log.run "src" "mkdir" ["-p", tmpDir]
  let tmpT = T.pack tmpDir
  pure (Core.mkVars pfx_ path_ tmpT (Core.fromPath path_) "", tmpT)

-- | Fallback path: run fbCmdTmpl, apply fbSqlTmpl to its JSON.
-- Returns Just even on empty fallback output so caller skips the error popup.
tryFallback :: V.Vector (Text, Text) -> IO (Maybe AdbcTable)
tryFallback vars = do
  let cmd = Core.expand fbCmdTmpl vars
  mFile <- Core.writeCmdOut "fallback" cmd
  case mFile of
    Nothing      -> pure Nothing
    Just tmpFile -> do
      tbl <- tmpName "src"
      let sql = Core.expand fbSqlTmpl (V.singleton ("src", T.pack tmpFile))
      _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
      Tmp.rmFile tmpFile
      fromTmp tbl

hfList :: Bool -> Text -> IO (Maybe AdbcTable)
hfList _ path_ = do
  let p = if T.isSuffixOf "/" path_ then path_ else path_ <> "/"
  (vars, _) <- hfVars p
  let cmd = Core.expand cmdTmpl vars
  (ec, out, err) <- Core.runCmd "list" cmd
  case ec of
    ExitFailure _ -> do
      mfb <- tryFallback vars
      case mfb of
        Just adbc -> pure (Just adbc)
        Nothing   -> do
          Render.errorPopup ("List failed: " <> T.strip err)
          pure Nothing
    ExitSuccess
      | T.null (T.strip out) -> pure Nothing
      | otherwise -> loadTreeJson vars path_ out

loadTreeJson :: V.Vector (Text, Text) -> Text -> Text -> IO (Maybe AdbcTable)
loadTreeJson _ path_ raw = do
  tmpFile <- Tmp.tmpPath "src-list.json"
  TIO.writeFile tmpFile raw
  tbl <- tmpName "src"
  let sql = Core.expand listSqlTmpl (V.singleton ("src", T.pack tmpFile))
  _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
  Tmp.rmFile tmpFile
  Core.unnestStruct tbl
  Core.addParentRow (hfParent path_) tbl
  fromTmp tbl

-- | hf://datasets/ path: 5-component root (hf:/datasets/<org>/<name>/…);
-- above root falls back to hf:// to land in the dataset index.
hfParent :: Text -> Maybe Text
hfParent p = case Remote.parent p 5 of
  Just par -> Just par
  Nothing  -> Just parentFallback

hfDl :: Bool -> Text -> IO Text
hfDl _ path_ = do
  Render.statusMsg ("Downloading " <> path_ <> " ...")
  (vars, tmpDir) <- hfVars path_
  let cmd = Core.expand dlTmpl vars
  _ <- Core.runCmd "download" cmd
  pure (tmpDir <> "/" <> Core.fromPath path_)

hfDataset :: Source
hfDataset = Source
  { pfx       = pfx_
  , list      = hfList
  , enter     = \_ -> pure Nothing
  , enterUrl  = \_ -> Nothing
  , download  = hfDl
  , resolve   = \_ p -> pure p        -- DuckDB reads hf:// URLs natively (httpfs)
  , setup     = pure ()
  , parent    = hfParent
  , grpCol    = ""
  , attach    = False
  , dirSuffix = True
  }
