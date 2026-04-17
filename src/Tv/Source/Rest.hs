{-# LANGUAGE OverloadedStrings #-}
-- | REST (rest://) backend: curl a JSON URL, read as a table.
module Tv.Source.Rest (rest) where

import Data.Text (Text)
import qualified Data.Text as T

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Remote as Remote
import qualified Tv.Tmp as Tmp
import Tv.Source.Core (Source (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "rest://"

cmdTmpl :: Text
cmdTmpl = "curl -sfL https://{1+}"

rest :: Source
rest = Source
  { pfx       = pfx_
  , list      = \_ path_ -> listRest path_
  , enter     = \_ -> pure Nothing
  , enterUrl  = \_ -> Nothing
  , download  = \_ p -> pure p           -- REST URIs aren't downloaded, DuckDB reads them
  , resolve   = \_ p -> pure p
  , setup     = pure ()
  , parent    = \p -> Remote.parent p 1
  , grpCol    = ""
  , attach    = False
  , dirSuffix = False
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
