{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-
  AdbcTable operations: render, getCols, cellStr, modify, and meta queries.

  Literal port of Tc/Data/ADBC/Ops.lean. Each Lean def maps to a Haskell
  top-level def.
-}
module Tv.Data.DuckDB.Ops where

import Prelude hiding (filter)
import Tv.Prelude
import Control.Exception (SomeException, try)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.HashSet as Set

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable(..))
import Data.List (nub)
import Tv.Types (ColType(..), colText, escSql, isNumeric)
import qualified Tv.Util as Log
import Optics.Core ((^.))

-- ----------------------------------------------------------------------------
-- Table operations (were typeclass methods, now plain functions)
-- ----------------------------------------------------------------------------

getCols :: AdbcTable -> Vector Int -> Int -> Int -> IO (Vector (Vector Text))
getCols t idxs r0_ r1_ =
  V.mapM (\i -> V.generateM (r1_ - r0_) $ \ri ->
    Conn.cellStr (t ^. #qr) (r0_ + ri) i
  ) idxs

colType :: AdbcTable -> Int -> ColType
colType t col = fromMaybe ColTypeOther ((t ^. #colTypes) V.!? col)

cellStr :: AdbcTable -> Int -> Int -> IO Text
cellStr t row col =
  Conn.cellStr (t ^. #qr) row col

-- | Hide columns at cursor + selections, return new table and filtered group
modifyTableHide :: AdbcTable -> Int -> Vector Int -> Vector Text -> IO (AdbcTable, Vector Text)
modifyTableHide tbl_ cursor sels grp = do
  let idxs = if V.elem cursor sels then sels else V.snoc sels cursor
      names = tbl_ ^. #colNames
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
  let nc = V.length (t ^. #colNames)
  cols <- V.generateM nc $ \i ->
    V.generateM (t ^. #nRows) $ \r ->
      Conn.cellStr (t ^. #qr) r i
  pure $ colText (t ^. #colNames) cols (t ^. #nRows)

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
--
-- 'withStats' controls whether expensive aggregates (mean/std/percentiles)
-- are emitted. 'selSet' gates those aggregates row-by-row: only selected
-- numeric columns run the heavy scans; others get NULL placeholders so
-- the schema stays uniform.
statsSql :: Bool -> Set.HashSet Text -> Text -> Vector Text -> Vector ColType -> Text
statsSql withStats selSet baseSql names types =
  let one i =
        let nm = fromMaybe "" (names V.!? i)
            tp = fromMaybe ColTypeOther (types V.!? i)
            q  = quoteId nm
            heavy = withStats && isNumeric tp && Set.member nm selSet
            extras =
              if not withStats then ""
              else if heavy
                then ", CAST(AVG(" <> q <> ") AS VARCHAR) AS mean"
                  <> ", CAST(STDDEV_POP(" <> q <> ") AS VARCHAR) AS std"
                  <> ", CAST(QUANTILE_CONT(" <> q <> ", 0.25) AS VARCHAR) AS p25"
                  <> ", CAST(QUANTILE_CONT(" <> q <> ", 0.50) AS VARCHAR) AS p50"
                  <> ", CAST(QUANTILE_CONT(" <> q <> ", 0.75) AS VARCHAR) AS p75"
                else ", NULL::VARCHAR AS mean, NULL::VARCHAR AS std"
                  <> ", NULL::VARCHAR AS p25, NULL::VARCHAR AS p50, NULL::VARCHAR AS p75"
        in "SELECT '" <> escSql nm <> "' AS \"column\", '"
           <> escSql (T.pack (show tp)) <> "' AS coltype, "
           <> "CAST(COUNT(" <> q <> ") AS BIGINT) AS cnt, "
           <> "CAST(COUNT(DISTINCT " <> q <> ") AS BIGINT) AS dist, "
           <> "CAST(ROUND((1.0 - COUNT(" <> q <> ")::FLOAT / NULLIF(COUNT(*),0)) * 100) AS BIGINT) AS null_pct, "
           <> "CAST(MIN(" <> q <> ") AS VARCHAR) AS mn, CAST(MAX(" <> q <> ") AS VARCHAR) AS mx"
           <> extras <> " FROM __src"
      unions = T.intercalate " UNION ALL "
                 (map one [0 .. V.length names - 1])
  in "WITH __src AS (" <> baseSql <> ") " <> unions

-- | Query meta: parquet uses file metadata (instant), others use SQL aggregation.
queryMeta :: AdbcTable -> IO (Maybe AdbcTable)
queryMeta = queryMetaImpl False Set.empty

-- | Query meta with expensive stats (mean/std/p25/p50/p75) for the given
-- columns — only those are scanned for the heavy aggregates; the rest
-- get NULLs so the schema stays uniform. Bypasses the parquet_metadata
-- fast path because it doesn't carry the needed aggregates.
queryMetaStats :: AdbcTable -> Vector Text -> IO (Maybe AdbcTable)
queryMetaStats t sel = queryMetaImpl True (V.foldr Set.insert Set.empty sel) t

queryMetaImpl :: Bool -> Set.HashSet Text -> AdbcTable -> IO (Maybe AdbcTable)
queryMetaImpl withStats selSet t = do
  let names = t ^. #colNames
      types = t ^. #colTypes
  if V.null names
    then pure Nothing
    else do
      let parquetPath =
            case extractPath ((t ^. #query) ^. #base) of
              Just p | T.isSuffixOf ".parquet" p -> Just p
              _ -> Nothing
      mMetaSql <- case parquetPath of
        -- parquet_metadata covers the cheap cols only; when stats are
        -- requested, fall through to the aggregate path.
        Just p | not withStats -> Prql.compile (metaPrql p)
        _ -> do
          mBase <- Prql.compile ((t ^. #query) ^. #base)
          case mBase of
            Nothing -> pure Nothing
            Just baseSql ->
              pure $ Just $ statsSql withStats selSet (T.stripEnd baseSql) names types
      case mMetaSql of
        Nothing -> pure Nothing
        Just metaSql -> do
          tblName <- Table.tmpName "meta"
          _ <- Conn.query ("CREATE OR REPLACE TEMP TABLE " <> tblName
                          <> " AS (" <> metaSql <> ")")
          qr_ <- Conn.query ("SELECT * FROM " <> tblName)
          Just <$> Table.ofResult qr_ (Prql.defaultQuery { Prql.base = "from " <> tblName }) 0

-- | Pearson correlation matrix for the given columns against the base
-- table. Produces a square table: one row per column, one data column
-- per column, cells are CORR(row_col, col_col) rounded to 3 dp.
-- Non-numeric column names are silently dropped from the input.
queryCorrMatrix :: AdbcTable -> Vector Text -> IO (Maybe AdbcTable)
queryCorrMatrix t rawSel = do
  let typeOf nm = fromMaybe ColTypeOther $ V.find (== nm) (t ^. #colNames)
                    *> (fmap ((t ^. #colTypes) V.!) (V.findIndex (== nm) (t ^. #colNames)))
      sel = V.filter (\nm -> isNumeric (typeOf nm)) rawSel
  if V.length sel < 2
    then pure Nothing
    else do
      mBase <- Prql.compile ((t ^. #query) ^. #base)
      case mBase of
        Nothing -> pure Nothing
        Just baseSql -> do
          let sql = corrSql (T.stripEnd baseSql) sel
          tblName <- Table.tmpName "corr"
          _ <- Conn.query ("CREATE OR REPLACE TEMP TABLE " <> tblName
                           <> " AS (" <> sql <> ")")
          qr_ <- Conn.query ("SELECT * FROM " <> tblName)
          Just <$> Table.ofResult qr_
            (Prql.defaultQuery { Prql.base = "from " <> tblName }) 0

-- | Build one-scan SQL for an N×N correlation matrix. All CORR() calls
-- share the same WITH __src scan; rows are N literal labels pivoted out
-- of the single aggregate row via UNION ALL so the result schema stays
-- (col, <col_1>, <col_2>, …).
corrSql :: Text -> Vector Text -> Text
corrSql baseSql sel =
  let n = V.length sel
      name i = fromMaybe "" (sel V.!? i)
      qn i = quoteId (name i)
      aggField i j = "CAST(ROUND(CORR(" <> qn i <> ", " <> qn j <> "), 3) AS VARCHAR)"
                     <> " AS c_" <> T.pack (show i) <> "_" <> T.pack (show j)
      aggCols = T.intercalate ", "
        [ aggField i j | i <- [0 .. n - 1], j <- [0 .. n - 1] ]
      row i = "SELECT '" <> escSql (name i) <> "' AS \"col\", "
              <> T.intercalate ", "
                   [ "c_" <> T.pack (show i) <> "_" <> T.pack (show j)
                     <> " AS " <> quoteId (name j)
                   | j <- [0 .. n - 1] ]
              <> " FROM __corr"
      rows = T.intercalate " UNION ALL " [ row i | i <- [0 .. n - 1] ]
  in "WITH __src AS (" <> baseSql <> "), "
     <> "__corr AS (SELECT " <> aggCols <> " FROM __src) "
     <> rows

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
        v <- Conn.cellInt qr_ r 0
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
          V.generateM n $ \r -> Conn.cellStr qr_ r 0

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

-- ----------------------------------------------------------------------------
-- SQL builders / runners
-- (Feature modules compose SQL via these; keeps raw SQL strings in Data.DuckDB.)
-- ----------------------------------------------------------------------------

-- | Run "CREATE OR REPLACE TEMP VIEW <name> AS <sql>".
createTempView :: Text -> Text -> IO ()
createTempView name sql = do
  _ <- Conn.query ("CREATE OR REPLACE TEMP VIEW " <> name <> " AS " <> sql)
  pure ()

-- | Run "CREATE OR REPLACE TEMP TABLE <name> AS <sql>".
createTempTable :: Text -> Text -> IO ()
createTempTable name sql = do
  _ <- Conn.query ("CREATE OR REPLACE TEMP TABLE " <> name <> " AS " <> sql)
  pure ()

-- | Max parts from splitting @col@ by regex @ep@ in query over @baseSql@.
--   Clamped to 20. Returns 0 on SQL error.
maxSplitParts :: Text -> Text -> Text -> IO Int
maxSplitParts baseSql col ep = do
  let countSql =
        "SELECT COALESCE(max(array_length(string_split_regex("
        <> quoteId col <> ", '" <> ep <> "'))), 0) FROM (" <> baseSql <> ")"
  r <- try $ do
    qr_ <- Conn.query countSql
    v <- Conn.cellInt qr_ 0 0
    pure (min (fromIntegral v :: Int) 20)
  case r of
    Left (e :: SomeException) -> do
      Log.write "split" ("maxParts: " <> T.pack (show e))
      pure 0
    Right n -> pure n

-- | Build UNPIVOT+PIVOT SQL to transpose a table: rows become columns, up to @n@.
--   All cells cast to VARCHAR (types collapse). Original column order preserved
--   via an explicit CASE mapping in ORDER BY (GROUP BY alone would reorder).
transposeSql :: Text -> Vector Text -> Int -> Text
transposeSql baseSql colNames n =
  let castCols = T.intercalate ", "
        (V.toList (V.map (\c -> "CAST(" <> quoteId c <> " AS VARCHAR) AS " <> quoteId c) colNames))
      unpivotCols = T.intercalate ", " (V.toList (V.map quoteId colNames))
      pivotCols = T.intercalate ", "
        [ "MAX(CASE WHEN _rn = " <> T.pack (show i) <> " THEN _val END) AS \"row_" <> T.pack (show i) <> "\""
        | i <- [0 .. n - 1]
        ]
      ordCases = T.intercalate " "
        [ "WHEN \"column\" = '" <> escSql (fromMaybe "" (colNames V.!? i)) <> "' THEN " <> T.pack (show i)
        | i <- [0 .. V.length colNames - 1]
        ]
  in "WITH __src AS (SELECT * FROM (" <> baseSql <> ") LIMIT " <> T.pack (show n) <> "), "
  <> "__num AS (SELECT ROW_NUMBER() OVER () - 1 AS _rn, " <> castCols <> " FROM __src), "
  <> "__unp AS (UNPIVOT __num ON " <> unpivotCols <> " INTO NAME \"column\" VALUE _val) "
  <> "SELECT \"column\", " <> pivotCols <> " FROM __unp GROUP BY \"column\" "
  <> "ORDER BY CASE " <> ordCases <> " END"

-- | Per-column aggregation SQL for the status bar: returns one row with SUM/AVG/COUNT
--   (numeric) or COUNT (other). Caller wraps @baseSql@ via @SELECT ... FROM (<baseSql>)@.
colAggSql :: Text -> Text -> Bool -> Text
colAggSql baseSql colName isNum =
  let q = quoteId colName
      aggs = if isNum
        then "CAST(SUM(" <> q <> ") AS DOUBLE) AS s, CAST(AVG(" <> q <> ") AS DOUBLE) AS a, COUNT(" <> q <> ") AS c"
        else "COUNT(" <> q <> ") AS c"
  in "SELECT " <> aggs <> " FROM (" <> baseSql <> ")"

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
