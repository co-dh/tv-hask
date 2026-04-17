{-# LANGUAGE OverloadedStrings #-}
-- | PostgreSQL (pg://) backend: ATTACH DSN as a read-only DuckDB database,
-- list its tables via the shared `tbl_info_filtered` PRQL helper.
module Tv.Source.Pg (pg) where

import Control.Monad (forM_)
import Data.Text (Text)
import qualified Data.Text as T

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, loadExt, stripSemi, tmpName)
import Tv.Types (escSql)
import Tv.Source.Core (Source (..))

pfx_ :: Text
pfx_ = "pg://"

-- ATTACH … AS extdb (TYPE POSTGRES, READ_ONLY); tbl_info_filtered.
-- extdb is a shared alias so re-entering a different DSN detaches first.
attachSql :: Text -> IO Text
attachSql connStr = do
  let ddl = "DETACH DATABASE IF EXISTS extdb;\nATTACH '"
          <> escSql connStr <> "' AS extdb (TYPE POSTGRES, READ_ONLY)"
  mSql <- Prql.compile Prql.ducktabsF
  case mSql of
    Nothing  -> ioError $ userError "Pg: failed to compile tbl_info_filtered PRQL"
    Just sql -> pure (ddl <> ";\n" <> stripSemi sql)

pgList :: Bool -> Text -> IO (Maybe AdbcTable)
pgList _ path_ = do
  let dsn = T.drop (T.length pfx_) path_
  sql <- attachSql dsn
  tbl <- tmpName "src"
  -- Split on ";\n" because ATTACH and SELECT must run as separate statements
  -- (DuckDB doesn't wrap multi-statement scripts in a single query).
  let stmts = filter (not . T.null) (map T.strip (T.splitOn ";\n" sql))
  case reverse stmts of
    [] -> pure Nothing
    (selectSql : rest) -> do
      forM_ (reverse rest) $ \stmt -> do
        _ <- Conn.query stmt
        pure ()
      _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> selectSql)
      fromTmp tbl

pg :: Source
pg = Source
  { pfx       = pfx_
  , list      = pgList
  , enter     = \_ -> pure Nothing    -- enter dispatches via Folder.enterAttach when attach=True
  , enterUrl  = \_ -> Nothing
  , download  = \_ p -> pure p
  , resolve   = \_ p -> pure p
  , setup     = loadExt "postgres"
  , parent    = \_ -> Nothing          -- minParts 99 means always at root
  , grpCol    = "name"
  , attach    = True
  , dirSuffix = False
  }
