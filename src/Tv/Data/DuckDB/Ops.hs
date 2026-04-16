{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-
  AdbcTable operations: render, getCols, cellStr, modify, and meta queries.

  Literal port of Tc/Data/ADBC/Ops.lean. Each Lean def maps to a Haskell
  top-level def.
-}
module Tv.Data.DuckDB.Ops
  ( -- * Table operations (were typeclass methods, now plain functions)
    getCols
  , colType
  , cellStr
  , modifyTableHide
  , modifyTableSort
    -- * Meta / helpers
  , toText
  , queryMeta
  , metaIdxs
  , metaNames
  , quoteId
  , columnComment
  , enrichComments
  ) where

import Prelude hiding (filter)
import Control.Exception (SomeException, try)
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word64)

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Prql (Query(..))
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable(..))
import Data.List (nub)
import Tv.Types (ColType(..), colText, escSql, keepCols)
import qualified Tv.Util as Log

-- ----------------------------------------------------------------------------
-- Table operations (were typeclass methods, now plain functions)
-- ----------------------------------------------------------------------------

getCols :: AdbcTable -> Vector Int -> Int -> Int -> IO (Vector (Vector Text))
getCols t idxs r0_ r1_ =
  V.mapM (\i -> V.generateM (r1_ - r0_) $ \ri ->
    Conn.cellStr (Table.qr t) (fromIntegral (r0_ + ri) :: Word64) (fromIntegral i :: Word64)
  ) idxs

colType :: AdbcTable -> Int -> ColType
colType t col = fromMaybe ColTypeOther (Table.colTypes t V.!? col)

cellStr :: AdbcTable -> Int -> Int -> IO Text
cellStr t row col =
  Conn.cellStr (Table.qr t) (fromIntegral row :: Word64) (fromIntegral col :: Word64)

-- | Hide columns at cursor + selections, return new table and filtered group
modifyTableHide :: AdbcTable -> Int -> Vector Int -> Vector Text -> IO (AdbcTable, Vector Text)
modifyTableHide tbl_ cursor sels grp = do
  let idxs = if V.elem cursor sels then sels else V.snoc sels cursor
      names = Table.colNames tbl_
      hideNames = V.map (\i -> fromMaybe "" (names V.!? i)) idxs
  newTbl <- Table.hideCols tbl_ idxs
  pure (newTbl, V.filter (not . (`V.elem` hideNames)) grp)

-- | Sort table by selected columns + cursor column, excluding group (key) columns
modifyTableSort :: AdbcTable -> Int -> Vector Int -> Vector Int -> Bool -> IO AdbcTable
modifyTableSort tbl_ cursor selIdxs grpIdxs asc =
  let cols = V.fromList . nub . V.toList
             . V.filter (not . (`V.elem` grpIdxs))
             $ selIdxs V.++ V.singleton cursor
  in if V.null cols then pure tbl_ else Table.sortBy tbl_ cols asc

-- ----------------------------------------------------------------------------
-- | Format table as plain text
-- ----------------------------------------------------------------------------

toText :: AdbcTable -> IO Text
toText t = do
  let nc = V.length (Table.colNames t)
  cols <- V.generateM nc $ \i ->
    V.generateM (Table.nRows t) $ \r ->
      Conn.cellStr (Table.qr t) (fromIntegral r :: Word64) (fromIntegral i :: Word64)
  pure (colText (Table.colNames t) cols (Table.nRows t))

-- ----------------------------------------------------------------------------
-- ## Meta: column statistics from parquet metadata or SQL aggregation
-- ----------------------------------------------------------------------------

-- | Extract file path from PRQL base: "from `path`" -> Just "path"
extractPath :: Text -> Maybe Text
extractPath base =
  case T.splitOn "`" base of
    _ : p : _ | not (T.null p) -> Just p
    _                          -> Nothing

-- | PRQL: column stats from parquet file metadata (instant, no data scan)
metaPrql :: Text -> Text
metaPrql path_ =
  let p = escSql path_
  in "from s\"SELECT * FROM parquet_metadata('" <> p <> "')\" | pqmeta"

-- | Double-quote identifier for DuckDB
quoteId :: Text -> Text
quoteId s = "\"" <> T.replace "\"" "\"\"" s <> "\""

-- | SQL: per-column stats via UNION ALL (for non-parquet sources).
-- Cannot use DuckDB SUMMARIZE: its null_percentage uses approximate counting
-- which miscounts NULLs for some column types, giving wrong null_pct values.
statsSql :: Text -> Vector Text -> Vector ColType -> Text
statsSql baseSql names types =
  let one i =
        let nm = escSql (fromMaybe "" (names V.!? i))
            tp = escSql (T.pack (show (fromMaybe ColTypeOther (types V.!? i))))
            q  = quoteId (fromMaybe "" (names V.!? i))
        in "SELECT '" <> nm <> "' AS \"column\", '" <> tp <> "' AS coltype, "
           <> "CAST(COUNT(" <> q <> ") AS BIGINT) AS cnt, "
           <> "CAST(COUNT(DISTINCT " <> q <> ") AS BIGINT) AS dist, "
           <> "CAST(ROUND((1.0 - COUNT(" <> q <> ")::FLOAT / NULLIF(COUNT(*),0)) * 100) AS BIGINT) AS null_pct, "
           <> "CAST(MIN(" <> q <> ") AS VARCHAR) AS mn, CAST(MAX(" <> q <> ") AS VARCHAR) AS mx FROM __src"
      unions = T.intercalate " UNION ALL "
                 (map one [0 .. V.length names - 1])
  in "WITH __src AS (" <> baseSql <> ") " <> unions

-- | Query meta: parquet uses file metadata (instant), others use SQL aggregation.
queryMeta :: AdbcTable -> IO (Maybe AdbcTable)
queryMeta t = do
  let names = Table.colNames t
      types = Table.colTypes t
  if V.null names
    then pure Nothing
    else do
      let parquetPath =
            case extractPath (Prql.base (Table.query t)) of
              Just p | T.isSuffixOf ".parquet" p -> Just p
              _ -> Nothing
      mMetaSql <- case parquetPath of
        Just p -> Prql.compile (metaPrql p)
        Nothing -> do
          mBase <- Prql.compile (Prql.base (Table.query t))
          case mBase of
            Nothing -> pure Nothing
            Just baseSql ->
              pure (Just (statsSql (T.stripEnd baseSql) names types))
      case mMetaSql of
        Nothing -> pure Nothing
        Just metaSql -> do
          tblName <- Table.tmpName "meta"
          _ <- Conn.query ("CREATE OR REPLACE TEMP TABLE " <> tblName
                          <> " AS (" <> metaSql <> ")")
          qr_ <- Conn.query ("SELECT * FROM " <> tblName)
          Just <$> Table.ofResult qr_ (Prql.defaultQuery { Prql.base = "from " <> tblName }) 0

-- | Query row indices matching PRQL filter on meta table
metaIdxs :: Text -> Text -> IO (Vector Int)
metaIdxs tblName flt = do
  m <- Table.prqlQuery ("from " <> tblName <> " | rowidx | filter " <> flt <> " | select {idx}")
  case m of
    Nothing -> pure V.empty
    Just qr_ -> do
      nr <- Conn.nrows qr_
      let n = fromIntegral nr :: Int
      V.generateM n $ \r -> do
        v <- Conn.cellInt qr_ (fromIntegral r :: Word64) 0
        pure (fromIntegral v :: Int)

-- | Query column names from meta table at given row indices
metaNames :: Text -> Vector Int -> IO (Vector Text)
metaNames tblName rows = do
  if V.null rows
    then pure V.empty
    else do
      let idxs = T.intercalate ", " (V.toList (V.map (T.pack . show) rows))
      m <- Table.prqlQuery
             ("from " <> tblName
              <> " | rowidx | filter (idx | in [" <> idxs <> "]) | select {column, idx}")
      case m of
        Nothing -> pure V.empty
        Just qr_ -> do
          nr <- Conn.nrows qr_
          let n = fromIntegral nr :: Int
          V.generateM n $ \r -> Conn.cellStr qr_ (fromIntegral r :: Word64) 0

-- | Extract table name from path: last component after last "://" prefix strip.
--   "osquery://groups" -> "groups", "duckdb://osq.groups" -> "groups"
pathTable :: Text -> Text
pathTable path_ =
  case T.splitOn "://" path_ of
    [_, rest] ->
      let parts = [s | s <- T.splitOn "/" rest, not (T.null s)]
      in if null parts then "" else last parts
    _ -> ""

-- | Get column comment from DuckDB metadata. Searches all attached databases
-- for a matching table.
columnComment :: Text -> Text -> IO Text
columnComment path_ colName =
  if T.null tbl
    then pure ""
    else do
      r <- try action :: IO (Either SomeException Text)
      case r of
        Left e  -> do
          Log.write "ops" ("columnComment " <> tbl <> "." <> colName <> ": " <> T.pack (show e))
          pure ""
        Right s -> pure s
  where
    tbl = pathTable path_
    action = do
      m <- Table.prqlQuery
             ("from dcols | col_comment '" <> escSql tbl
              <> "' '" <> escSql colName <> "'")
      case m of
        Nothing -> pure ""
        Just qr_ -> do
          n <- Conn.nrows qr_
          if fromIntegral n == (0 :: Int)
            then pure ""
            else Conn.cellStr qr_ 0 0

-- | Enrich meta table with column descriptions from DuckDB metadata.
--   Returns True if enriched.
enrichComments :: Text -> Text -> IO Bool
enrichComments metaTbl path_ =
  if T.null tbl
    then pure False
    else do
      r <- try action :: IO (Either SomeException ())
      case r of
        Left e  -> do
          Log.write "ops" ("enrichComments " <> metaTbl <> ": " <> T.pack (show e))
          pure False
        Right _ -> pure True
  where
    tbl = pathTable path_
    action = do
      _ <- Conn.query
             ("ALTER TABLE " <> metaTbl
              <> " ADD COLUMN IF NOT EXISTS description VARCHAR DEFAULT ''")
      _ <- Conn.query
             ("UPDATE " <> metaTbl
              <> " SET description = COALESCE((SELECT comment FROM duckdb_columns() WHERE table_name='"
              <> escSql tbl
              <> "' AND column_name = " <> metaTbl
              <> ".\"column\" AND comment IS NOT NULL), '')")
      pure ()
