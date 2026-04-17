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
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Types (escSql)
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "pg://"

setupKey :: Text
setupKey = pfx_

pgSetup :: IO ()
pgSetup = Core.onceFor setupKey (loadExt "postgres")

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
  pgSetup
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

-- | fullPath for pg is `pg://<dsn>/<tableName>`; we take the trailing
-- path component as the table name. DSNs containing `/` are not supported.
pgOpen :: Bool -> Text -> IO OpenResult
pgOpen _ path_ = do
  pgSetup
  let rest = if T.isPrefixOf pfx_ path_ then T.drop (T.length pfx_) path_ else path_
      tbl  = case reverse (T.splitOn "/" rest) of
               (x:_) -> x
               []    -> rest
  m <- Table.fromTable tbl
  case m of
    Just (adbc, _) -> pure (OpenAsTable adbc)
    Nothing        -> pure OpenNothing

pg :: Source
pg = Source
  { pfx    = pfx_
  , parent = \_ -> Nothing                 -- always at root (minParts 99 in the old code)
  , grpCol = Just "name"
  , list   = pgList
  , open   = pgOpen
  }
