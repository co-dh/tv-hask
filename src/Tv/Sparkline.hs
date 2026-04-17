{-# LANGUAGE OverloadedStrings #-}
{-
  Sparkline: compute per-column distribution strings (Unicode block chars)
  for display as an extra header row in table view.

  Literal port of Tc/Tc/Sparkline.lean.
-}
module Tv.Sparkline
  ( compute
  ) where

import Control.Exception (SomeException, try)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word64)

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Ops (quoteId)
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)
import Tv.Types (isNumeric, joinWith)
import qualified Tv.Log as Log

-- 9 levels: space + 8 Unicode block elements
blocks :: Vector Char
blocks = V.fromList [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█']

-- | Per-column histogram SELECT: bucket + count tagged with the column index.
-- LEAST clamps bucket to [0, nBars-1]; CASE handles constant columns.
bucketSql :: Int -> Int -> Text -> Text
bucketSql nBars i name =
  let q = quoteId name
      inner = "SELECT CASE WHEN mx = mn THEN " <> T.pack (show (nBars `div` 2)) <> " "
           <> "ELSE LEAST(CAST(FLOOR((" <> q <> "::DOUBLE - mn) / (mx - mn + 1e-30) * "
           <> T.pack (show nBars) <> ") AS INTEGER), " <> T.pack (show (nBars - 1)) <> ") "
           <> "END AS bucket, COUNT(*) AS cnt "
           <> "FROM __src, (SELECT MIN(" <> q <> "::DOUBLE) AS mn, MAX("
           <> q <> "::DOUBLE) AS mx FROM __src) "
           <> "WHERE " <> q <> " IS NOT NULL GROUP BY bucket ORDER BY bucket"
  in "SELECT " <> T.pack (show i) <> " AS col_idx, bucket, cnt FROM (" <> inner <> ")"

-- | Parse UNION ALL result rows (col_idx, bucket, cnt) into per-column bucket lists.
foldRows :: Conn.QueryResult -> Vector Int -> IO (Vector (Vector (Int, Int)))
foldRows qr_ numIdxs = do
  nr <- Conn.nrows qr_
  let nrI = fromIntegral nr :: Int
      empty = V.replicate (V.length numIdxs) (V.empty :: Vector (Int, Int))
      step cb r_ = do
        let rW = fromIntegral r_ :: Word64
        colIdx <- Conn.cellInt qr_ rW 0
        bucket <- Conn.cellInt qr_ rW 1
        cnt    <- Conn.cellInt qr_ rW 2
        pure $ case V.findIndex (== fromIntegral colIdx) numIdxs of
          Just j ->
            let cur = fromMaybe V.empty $ cb V.!? j
            in cb V.// [(j, V.snoc cur (fromIntegral bucket, fromIntegral cnt))]
          Nothing -> cb
  V.foldM' step empty (V.enumFromN (0 :: Int) nrI)

-- | Render one column's bucket list as a sparkline string (Nothing = skip column).
sparkFor :: Int -> Vector (Int, Int) -> Maybe Text
sparkFor nBars buckets
  | V.null buckets = Nothing
  | otherwise =
      let counts0 = V.replicate nBars (0 :: Int)
          counts = V.foldl'
            (\acc (b, c) -> if b < nBars then acc V.// [(b, c)] else acc)
            counts0 buckets
          maxCnt = V.foldl' max 0 counts
      in Just $ if maxCnt == 0
           then T.replicate nBars " "
           else T.pack $ V.toList $ V.map
             (\c ->
               let level = (c * 8 + maxCnt - 1) `div` maxCnt  -- ceil: 0->0, >0->1..8
               in fromMaybe ' ' (blocks V.!? level))
             counts

-- | Compute sparkline strings for all columns via DuckDB histogram.
-- Returns one string per column (empty for non-numeric).
-- nBars = number of histogram buckets.
compute :: AdbcTable -> Int -> IO (Vector Text)
compute t nBars = do
  let names = Table.colNames t
      types = Table.colTypes t
      empty = V.map (const "") names
      stepQuery (ps, is) i name = case types V.!? i of
        Just tp | isNumeric tp -> (V.snoc ps (bucketSql nBars i name), V.snoc is i)
        _                      -> (ps, is)
      (parts, numIdxs) = V.ifoldl' stepQuery (V.empty :: Vector Text, V.empty :: Vector Int) names
  if V.null names || Table.nRows t == 0 || V.null parts
    then pure empty
    else do
      -- Sample for sparklines: DuckDB can't use parquet metadata for histogram bucketing,
      -- so a full scan on large files is catastrophic. LIMIT is fast (sequential read).
      let prql = if Table.totalRows t > Table.prqlLimit
                   then Prql.queryRender (Table.query t) <> " | take " <> T.pack (show Table.prqlLimit)
                   else Prql.queryRender (Table.query t)
      mBase <- Prql.compile prql
      case mBase of
        Nothing -> empty <$ Log.errorLog "sparkline: PRQL compile failed"
        Just baseSql -> do
          let unionSql = "WITH __src AS (" <> Table.stripSemi baseSql <> ") "
                      <> joinWith parts " UNION ALL "
          r <- try (Conn.query unionSql) :: IO (Either SomeException Conn.QueryResult)
          case r of
            Left e    -> empty <$ Log.errorLog ("sparkline: " <> T.pack (show e))
            Right qr_ -> do
              colBuckets <- foldRows qr_ numIdxs
              let updates = V.toList $ V.mapMaybe
                    (\(i, bs) -> (,) i <$> sparkFor nBars bs)
                    (V.zip numIdxs colBuckets)
              pure (empty V.// updates)
