{-# LANGUAGE ScopedTypeVariables #-}
-- | Sparkline: compute per-column distribution strings (Unicode block chars)
-- for display as an extra header row in table view.
--
-- The Lean version runs DuckDB histogram SQL on the backing query. Here we
-- read cells from the materialized TblOps, bucket locally, and build the
-- sparkline strings. For large tables (> prqlLimit), we sample only the
-- first prqlLimit rows (matching Lean's LIMIT approach).
module Tv.Sparkline
  ( compute
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import Control.Monad (forM_)

import Tv.Types

-- 9 levels: space + 8 Unicode block elements
blocks :: Vector Char
blocks = V.fromList [' ', '\x2581', '\x2582', '\x2583', '\x2584', '\x2585', '\x2586', '\x2587', '\x2588']

-- | Row limit beyond which we sample (matches Lean Tc.prqlLimit)
prqlLimit :: Int
prqlLimit = 1000

-- | Compute sparkline strings for all columns.
-- Returns one string per column (empty for non-numeric).
-- nBars = number of histogram buckets.
compute :: TblOps -> Int -> IO (Vector Text)
compute tbl nBars = do
  let names = _tblColNames tbl
      nc = V.length names
      nr = min (_tblNRows tbl) prqlLimit
      empty = V.replicate nc ""
  if nc == 0 || nr == 0 then pure empty
  else do
    -- identify numeric columns
    let numIdxs = V.fromList [i | i <- [0..nc-1], isNumeric (_tblColType tbl i)]
    if V.null numIdxs then pure empty
    else do
      -- for each numeric column, read values, compute min/max, bucket, build sparkline
      result <- V.thaw empty
      V.forM_ numIdxs $ \ci -> do
        vals <- readNumericCol tbl ci nr
        let spark = buildSparkline vals nBars
        MV.write result ci spark
      V.freeze result

-- | Read non-null numeric values with their row indices (for bucketing)
readNumericCol :: TblOps -> Int -> Int -> IO [Double]
readNumericCol tbl col nr = go 0 []
  where
    go r acc
      | r >= nr = pure (reverse acc)
      | otherwise = do
          v <- _tblCellStr tbl r col
          if T.null v then go (r + 1) acc
          else case readDouble v of
            Just d  -> go (r + 1) (d : acc)
            Nothing -> go (r + 1) acc

readDouble :: Text -> Maybe Double
readDouble t = case reads (T.unpack t) of
  [(d, "")] -> Just d
  _         -> Nothing

-- | Build a sparkline string from a list of numeric values.
-- Buckets values into nBars bins, maps counts to Unicode block chars.
buildSparkline :: [Double] -> Int -> Text
buildSparkline [] _ = ""
buildSparkline vals nBars =
  let mn = minimum vals; mx = maximum vals
      -- bucket each value; LEAST clamps to [0, nBars-1]; constant columns → middle
      bucket v
        | mx == mn  = nBars `div` 2
        | otherwise = min (nBars - 1) (floor ((v - mn) / (mx - mn + 1e-30) * fromIntegral nBars))
      -- count per bucket
      counts0 = V.replicate nBars (0 :: Int)
      counts = foldl (\cs v -> let b = bucket v in cs V.// [(b, cs V.! b + 1)]) counts0 vals
      maxCnt = V.maximum counts
      spark
        | maxCnt == 0 = T.replicate nBars " "
        | otherwise = T.pack $ V.toList $ V.map (\c ->
            let level = (c * 8 + maxCnt - 1) `div` maxCnt  -- ceil: 0->0, >0->1..8
            in blocks V.! level) counts
  in spark
