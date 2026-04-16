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

import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Data.ADBC.Prql as Prql
import Tv.Data.ADBC.Ops (quoteId)
import qualified Tv.Data.ADBC.Table as Table
import Tv.Data.ADBC.Table (AdbcTable)
import Tv.Types (colTypeIsNumeric, joinWith)
import qualified Tv.Util as Log

-- 9 levels: space + 8 Unicode block elements
blocks :: Vector Char
blocks = V.fromList [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█']

-- | Compute sparkline strings for all columns via DuckDB histogram.
-- Returns one string per column (empty for non-numeric).
-- nBars = number of histogram buckets.
compute :: AdbcTable -> Int -> IO (Vector Text)
compute t nBars = do
  let names = Table.colNames t
      types = Table.colTypes t
      empty = V.map (const "") names
  if V.null names || Table.nRows t == 0
    then pure empty
    else do
      let stepQuery (ps, is) i name = case types V.!? i of
            Just tp | colTypeIsNumeric tp ->
              let q = quoteId name
                  -- LEAST clamps bucket to [0, nBars-1]; CASE handles constant columns
                  sql = "SELECT CASE WHEN mx = mn THEN " <> T.pack (show (nBars `div` 2)) <> " "
                     <> "ELSE LEAST(CAST(FLOOR((" <> q <> "::DOUBLE - mn) / (mx - mn + 1e-30) * "
                     <> T.pack (show nBars) <> ") AS INTEGER), " <> T.pack (show (nBars - 1)) <> ") "
                     <> "END AS bucket, COUNT(*) AS cnt "
                     <> "FROM __src, (SELECT MIN(" <> q <> "::DOUBLE) AS mn, MAX("
                     <> q <> "::DOUBLE) AS mx FROM __src) "
                     <> "WHERE " <> q <> " IS NOT NULL GROUP BY bucket ORDER BY bucket"
              in ( V.snoc ps ("SELECT " <> T.pack (show i) <> " AS col_idx, bucket, cnt FROM (" <> sql <> ")")
                 , V.snoc is i )
            _ -> (ps, is)
          (parts, numIdxs) = V.ifoldl' stepQuery (V.empty :: Vector Text, V.empty :: Vector Int) names
      if V.null parts
        then pure empty
        else do
          -- Sample for sparklines: DuckDB can't use parquet metadata for histogram bucketing,
          -- so a full scan on large files is catastrophic. LIMIT is fast (sequential read).
          let prql = if Table.totalRows t > Table.prqlLimit
                       then Prql.queryRender (Table.query t) <> " | take " <> T.pack (show Table.prqlLimit)
                       else Prql.queryRender (Table.query t)
          mBase <- Prql.compile prql
          case mBase of
            Nothing -> do
              Log.errorLog "sparkline: PRQL compile failed"
              pure empty
            Just baseSql -> do
              let unionSql = "WITH __src AS (" <> Table.stripSemi baseSql <> ") "
                          <> joinWith parts " UNION ALL "
              r <- try (Adbc.query unionSql) :: IO (Either SomeException Adbc.QueryResult)
              case r of
                Left e -> do
                  Log.errorLog ("sparkline: " <> T.pack (show e))
                  pure empty
                Right qr_ -> do
                  nr <- Adbc.nrows qr_
                  let nrI = fromIntegral nr :: Int
                      emptyBuckets = V.map (const (V.empty :: Vector (Int, Int))) numIdxs
                      stepRow cb r_ = do
                        let rW = fromIntegral r_ :: Word64
                        colIdx <- Adbc.cellInt qr_ rW 0
                        bucket <- Adbc.cellInt qr_ rW 1
                        cnt    <- Adbc.cellInt qr_ rW 2
                        pure $ case V.findIndex (== fromIntegral colIdx) numIdxs of
                          Just j ->
                            let cur = fromMaybe V.empty (cb V.!? j)
                            in cb V.// [(j, V.snoc cur (fromIntegral bucket, fromIntegral cnt))]
                          Nothing -> cb
                  -- Parse results into bucket->count arrays per column
                  colBuckets <- V.foldM' stepRow emptyBuckets (V.enumFromN (0 :: Int) nrI)
                  -- Build sparkline string per column
                  let sparkFor buckets =
                        if V.null buckets
                          then Nothing
                          else
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
                      updates = V.toList $ V.mapMaybe id $ V.zipWith
                        (\i buckets -> (,) i <$> sparkFor buckets)
                        numIdxs colBuckets
                  pure (empty V.// updates)
