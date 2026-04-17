{-# LANGUAGE OverloadedStrings #-}
-- STUB: full migration in round 2. Keeps behavior via legacy adapter in `open`.
-- | REST (rest://) backend: curl a JSON URL, read as a table.
module Tv.Source.Rest (rest) where

import Data.Text (Text)
import qualified Data.Text as T

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Remote as Remote
import qualified Tv.Tmp as Tmp
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "rest://"

cmdTmpl :: Text
cmdTmpl = "curl -sfL https://{1+}"

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
  let vars = Core.mkVars pfx_ path_ "" (Core.fromPath path_) ""
      cmd  = Core.expand cmdTmpl vars
  mFile <- Core.writeCmdOut "list" cmd
  case mFile of
    Nothing      -> pure Nothing
    Just tmpFile -> do
      tbl <- tmpName "src"
      _ <- Conn.query
             ( "CREATE TEMP TABLE " <> tbl
            <> " AS SELECT * FROM read_json_auto('" <> T.pack tmpFile
            <> "', auto_detect=true)" )
      Tmp.rmFile tmpFile
      Core.unnestStruct tbl
      Core.addParentRow (Remote.parent path_ 1) tbl
      fromTmp tbl
