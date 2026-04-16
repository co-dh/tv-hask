{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-
  TblOps/ModifyTable instances for AdbcTable.
  Includes meta (column statistics) functionality.

  Literal port of Tc/Data/ADBC/Ops.lean. The Lean file has two typeclass
  instances followed by a block of top-level helpers (toText, queryMeta,
  queryMetaIndices, queryMetaColNames, pathTable, columnComment,
  enrichComments) with their private helpers (extractPath, parquetMetaPrql,
  quoteId, colStatsSql). Each Lean def maps to a Haskell top-level def.
-}
module Tv.Data.ADBC.Ops
  ( -- re-export instance surface (instances auto-export)
    toText
  , queryMeta
  , queryMetaIndices
  , queryMetaColNames
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

import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Data.ADBC.Prql as Prql
import Tv.Data.ADBC.Prql (Query(..))
import qualified Tv.Data.ADBC.Table as Table
import Tv.Data.ADBC.Table (AdbcTable(..))
import qualified Tv.Render as Render
import Tv.Types
  ( ColType(..)
  , RenderCtx(..)
  , TblOps(..)
  , ModifyTable(..)
  , colsToText
  , escSql
  )
import qualified Tv.Types as Types

-- ----------------------------------------------------------------------------
-- instance : TblOps AdbcTable
-- ----------------------------------------------------------------------------

instance TblOps AdbcTable where
  nRows     = Table.nRows
  colNames  = Table.colNames
  totalRows = Table.totalRows
  filter_   = Table.filter
  distinct  = Table.distinct
  findRow   = Table.findRow
  getCols t idxs r0_ r1_ =
    V.mapM (\i -> V.generateM (r1_ - r0_) $ \ri ->
      Adbc.cellStr (Table.qr t) (fromIntegral (r0_ + ri) :: Word64) (fromIntegral i :: Word64)
    ) idxs
  colType t col =
    fromMaybe ColTypeOther (Table.colTypes t V.!? col)
  cellStr t row col =
    Adbc.cellStr (Table.qr t) (fromIntegral row :: Word64) (fromIntegral col :: Word64)
  plotExport = Table.plotExport
  fetchMore  = Table.fetchMore
  fromFile   = Table.fromFile
  render t ctx = do
    texts <- Adbc.fetchRows (Table.qr t) (r0 ctx) (r1 ctx) (prec ctx)
    heatDs <- if heatMode ctx == 0
      then pure V.empty
      else Adbc.fetchHeatDoubles (Table.qr t) (r0 ctx) (r1 ctx)
    Render.renderCols texts (Table.colNames t) (Table.colFmts t) (Table.colTypes t)
      (Table.nRows t) ctx (r0 ctx) (r1 ctx - r0 ctx) heatDs

-- ----------------------------------------------------------------------------
-- instance : ModifyTable AdbcTable
-- ----------------------------------------------------------------------------

instance ModifyTable AdbcTable where
  hideCols hideIdxs t = Table.hideCols t hideIdxs
  sortBy   idxs asc t = Table.sortBy t idxs asc

-- ----------------------------------------------------------------------------
-- | Format table as plain text
-- ----------------------------------------------------------------------------

toText :: AdbcTable -> IO Text
toText t = do
  let nc = V.length (Table.colNames t)
  cols <- V.generateM nc $ \i ->
    V.generateM (Table.nRows t) $ \r ->
      Adbc.cellStr (Table.qr t) (fromIntegral r :: Word64) (fromIntegral i :: Word64)
  pure (colsToText (Table.colNames t) cols (Table.nRows t))

-- ----------------------------------------------------------------------------
-- ## Meta: column statistics from parquet metadata or SQL aggregation
-- ----------------------------------------------------------------------------

-- | Extract file path from PRQL base: "from `path`" -> Just "path"
extractPath :: Text -> Maybe Text
extractPath base =
  let parts = T.splitOn "`" base
  in if length parts >= 3
       then let p = parts !! 1
            in if T.null p then Nothing else Just p
       else Nothing

-- | PRQL: column stats from parquet file metadata (instant, no data scan)
parquetMetaPrql :: Text -> Text
parquetMetaPrql path_ =
  let p = escSql path_
  in "from s\"SELECT * FROM parquet_metadata('" <> p <> "')\" | pqmeta"

-- | Double-quote identifier for DuckDB
quoteId :: Text -> Text
quoteId s = "\"" <> T.replace "\"" "\"\"" s <> "\""

-- | SQL: per-column stats via UNION ALL (for non-parquet sources).
-- Cannot use DuckDB SUMMARIZE: its null_percentage uses approximate counting
-- which miscounts NULLs for some column types, giving wrong null_pct values.
colStatsSql :: Text -> Vector Text -> Vector ColType -> Text
colStatsSql baseSql names types =
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
        Just p -> Prql.compile (parquetMetaPrql p)
        Nothing -> do
          mBase <- Prql.compile (Prql.base (Table.query t))
          case mBase of
            Nothing -> pure Nothing
            Just baseSql ->
              pure (Just (colStatsSql (T.stripEnd baseSql) names types))
      case mMetaSql of
        Nothing -> pure Nothing
        Just metaSql -> do
          tblName <- Table.nextTmpName "meta"
          _ <- Adbc.query ("CREATE OR REPLACE TEMP TABLE " <> tblName
                          <> " AS (" <> metaSql <> ")")
          qr_ <- Adbc.query ("SELECT * FROM " <> tblName)
          Just <$> Table.ofQueryResult qr_ (Prql.defaultQuery { Prql.base = "from " <> tblName }) 0

-- | Query row indices matching PRQL filter on meta table
queryMetaIndices :: Text -> Text -> IO (Vector Int)
queryMetaIndices tblName flt = do
  m <- Table.prqlQuery ("from " <> tblName <> " | rowidx | filter " <> flt <> " | select {idx}")
  case m of
    Nothing -> pure V.empty
    Just qr_ -> do
      nr <- Adbc.nrows qr_
      let n = fromIntegral nr :: Int
      V.generateM n $ \r -> do
        v <- Adbc.cellInt qr_ (fromIntegral r :: Word64) 0
        pure (fromIntegral v :: Int)

-- | Query column names from meta table at given row indices
queryMetaColNames :: Text -> Vector Int -> IO (Vector Text)
queryMetaColNames tblName rows = do
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
          nr <- Adbc.nrows qr_
          let n = fromIntegral nr :: Int
          V.generateM n $ \r -> Adbc.cellStr qr_ (fromIntegral r :: Word64) 0

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
        Left _  -> pure ""
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
          n <- Adbc.nrows qr_
          if fromIntegral n == (0 :: Int)
            then pure ""
            else Adbc.cellStr qr_ 0 0

-- | Enrich meta table with column descriptions from DuckDB metadata.
--   Returns True if enriched.
enrichComments :: Text -> Text -> IO Bool
enrichComments metaTbl path_ =
  if T.null tbl
    then pure False
    else do
      r <- try action :: IO (Either SomeException ())
      case r of
        Left _  -> pure False
        Right _ -> pure True
  where
    tbl = pathTable path_
    action = do
      _ <- Adbc.query
             ("ALTER TABLE " <> metaTbl
              <> " ADD COLUMN IF NOT EXISTS description VARCHAR DEFAULT ''")
      _ <- Adbc.query
             ("UPDATE " <> metaTbl
              <> " SET description = COALESCE((SELECT comment FROM duckdb_columns() WHERE table_name='"
              <> escSql tbl
              <> "' AND column_name = " <> metaTbl
              <> ".\"column\" AND comment IS NOT NULL), '')")
      pure ()
