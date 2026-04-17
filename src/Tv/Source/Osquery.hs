{-# LANGUAGE OverloadedStrings #-}
-- | Osquery (osquery://) backend: lists all osqueryi tables from a
-- pre-populated DuckDB file, entering one runs `osqueryi --json` and
-- applies typed columns from the corresponding stub view.
module Tv.Source.Osquery (osquery) where

import Control.Exception (SomeException, try)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import System.Exit (ExitCode (..))

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Log as Log
import Tv.Source.Core (Source (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "osquery://"

setupKey :: Text
setupKey = pfx_

setupCmd :: Text
setupCmd = "python3 scripts/osquery_tables.py"

attachSqlTmpl :: Text
attachSqlTmpl = "ATTACH '{home}/.cache/tv/osquery.duckdb' AS osq (READ_ONLY)"

listSql :: Text
listSql = "SELECT name, safety, rows, description FROM osq.listing ORDER BY name"

enterCmdTmpl :: Text
enterCmdTmpl = "osqueryi --json \"SELECT * FROM {name}\""

osqSetup :: IO ()
osqSetup = Core.onceFor setupKey $ do
  home <- Core.homeText
  let attach = Core.expand attachSqlTmpl (V.singleton ("home", home))
  r <- try (Conn.query attach) :: IO (Either SomeException Conn.QueryResult)
  case r of
    Right _ -> Log.write "src" "osquery:// attach ok"
    Left _  -> do
      Log.write "src" "osquery:// attach failed, running osquery_tables.py"
      _ <- Core.runCmd "setup" setupCmd
      _ <- Conn.query attach
      pure ()

osqList :: Bool -> Text -> IO (Maybe AdbcTable)
osqList _ _ = do
  tbl <- tmpName "src"
  _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> listSql)
  fromTmp tbl

-- | Enter: `osqueryi --json "SELECT * FROM <name>"` → JSON → temp table,
-- then copy typed columns from the stub view (osq.<name>) for display.
osqEnter :: Text -> IO (Maybe AdbcTable)
osqEnter name = do
  Core.checkShell name "name"
  let vars = Core.mkVars pfx_ (pfx_ <> name) "" name ""
      cmd  = Core.expand enterCmdTmpl vars
  (ec, out, err) <- Core.runCmd "enter" cmd
  case ec of
    ExitFailure _ -> do
      Log.write "src" ("enter failed: " <> T.strip err)
      pure Nothing
    ExitSuccess ->
      let trimmed = T.strip out
      in if T.null trimmed || trimmed == "[]"
           then pure Nothing
           else do
             tbl <- Core.loadEnterJson out
             Core.ignoreErrs (Core.applyStubTypes tbl name)
             fromTmp tbl

osquery :: Source
osquery = Source
  { pfx       = pfx_
  , list      = osqList
  , enter     = osqEnter
  , enterUrl  = \_ -> Nothing
  , download  = \_ p -> pure p
  , resolve   = \_ p -> pure p
  , setup     = osqSetup
  , parent    = \_ -> Nothing
  , grpCol    = "name"
  , attach    = False
  , dirSuffix = False
  }
