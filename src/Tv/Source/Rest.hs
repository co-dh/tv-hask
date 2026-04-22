{-# LANGUAGE OverloadedStrings #-}
-- | REST (rest://) backend: fetch a JSON URL over HTTPS, read as a table.
module Tv.Source.Rest where

import Tv.Prelude
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Remote as Remote
import qualified Tv.Util as Tmp
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "rest://"

-- | Build the https:// URL from a rest:// path ("rest://api.x.com/y" → "https://api.x.com/y").
restUrl :: Text -> Text
restUrl path_ = "https://" <> T.drop (T.length pfx_) path_

-- | REST paths are URIs — no download, just passed to FileFormat / re-entered.
restOpen :: Bool -> Text -> IO OpenResult
restOpen _ path_
  | T.isSuffixOf "/" path_ = pure (OpenAsDir path_)
  | otherwise              = pure $ OpenAsFile $ T.unpack path_

rest :: Source
rest = Source
  { pfx    = pfx_
  , parent = \p -> Remote.parent p 1
  , grpCol = Nothing
  , list   = \_ p -> listRest p
  , open   = restOpen
  }

listRest :: Text -> IO (Maybe AdbcTable)
listRest path_ = do
  Core.checkShell path_ "path"
  mBody <- Core.fetchBytes (restUrl path_)
  case mBody of
    Nothing   -> pure Nothing
    Just body -> do
      tmpFile <- Tmp.tmpPath "src-list.json"
      LBS.writeFile tmpFile body
      tbl <- tmpName "src"
      _ <- Conn.query
             ( "CREATE TEMP TABLE " <> tbl
            <> " AS SELECT * FROM read_json_auto('" <> T.pack tmpFile
            <> "', auto_detect=true)" )
      Tmp.rmFile tmpFile
      Core.unnestStruct tbl
      Core.addParentRow (Remote.parent path_ 1) tbl
      fromTmp tbl
