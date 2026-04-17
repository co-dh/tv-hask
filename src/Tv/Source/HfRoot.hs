{-# LANGUAGE OverloadedStrings #-}
-- | HuggingFace root (hf://): lists all public datasets from a pre-populated
-- DuckDB file under ~/.cache/tv. The listing script runs once per process
-- if the cache is stale or missing. Selecting a row jumps to hf://datasets/<id>/.
module Tv.Source.HfRoot (hfRoot) where

import Control.Exception (SomeException, try)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Log as Log
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "hf://"

-- Keyed on pfx so re-entering hf:// within one process doesn't re-run
-- the python script (dataset listing is slow).
setupKey :: Text
setupKey = pfx_

setupCmd :: Text
setupCmd = "python3 scripts/hf_datasets.py"

attachSqlTmpl :: Text
attachSqlTmpl = "ATTACH '{home}/.cache/tv/hf_datasets.duckdb' AS hf (READ_ONLY)"

listSql :: Text
listSql = "SELECT id, downloads, likes, description, license, task, language, "
        <> "created, modified FROM hf.listing ORDER BY downloads DESC"

hfSetup :: IO ()
hfSetup = Core.onceFor setupKey $ do
  home <- Core.homeText
  let attach = Core.expand attachSqlTmpl (V.singleton ("home", home))
  -- Try ATTACH first; if the DuckDB file is missing, run the generator.
  r <- try (Conn.query attach) :: IO (Either SomeException Conn.QueryResult)
  case r of
    Right _ -> Log.write "src" "hf:// attach ok"
    Left _  -> do
      Log.write "src" "hf:// attach failed, running hf_datasets.py"
      _ <- Core.runCmd "setup" setupCmd
      _ <- Conn.query attach
      pure ()

hfRootList :: Bool -> Text -> IO (Maybe AdbcTable)
hfRootList _ _ = do
  hfSetup
  tbl <- tmpName "src"
  _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> listSql)
  fromTmp tbl

-- | Row names are dataset ids (e.g. "openai/gsm8k"); entering jumps to
-- hf://datasets/<id>/ which HfDataset then lists.
hfRootOpen :: Bool -> Text -> IO OpenResult
hfRootOpen _ path_ = pure $ OpenAsDir $ "hf://datasets/" <> name <> "/"
  where name = if T.isPrefixOf pfx_ path_ then T.drop (T.length pfx_) path_ else path_

hfRoot :: Source
hfRoot = Source
  { pfx    = pfx_
  , parent = \_ -> Nothing
  , grpCol = Just "id"
  , list   = hfRootList
  , open   = hfRootOpen
  }
