{-# LANGUAGE ScopedTypeVariables #-}
-- | StatusAgg: column aggregation stats (sum/avg/count) for the status bar.
-- Computes via local cell reads, cached per column to avoid per-frame work.
module Tv.StatusAgg
  ( Cache
  , emptyCache
  , compute
  , update
  ) where

import Control.Exception (SomeException, try)
import Data.Text (Text)
import qualified Data.Text as T

import Tv.Types

-- | Cache: (path, colIdx, formatted agg string)
type Cache = (Text, Int, Text)

emptyCache :: Cache
emptyCache = ("", 0, "")

-- | Row limit beyond which we skip expensive SUM/AVG (matches Lean Tc.prqlLimit)
prqlLimit :: Int
prqlLimit = 1000

-- | Aggregate stats for a single column: sum, avg, count (non-null).
-- Returns "S1234 u12.3 #100" for numeric, "#100" for non-numeric.
-- For large tables, skip SUM/AVG and show count only.
compute :: TblOps -> Int -> IO Text
compute tbl colIdx = do
  let nr = _tblNRows tbl
      ct = _tblColType tbl colIdx
  if _tblTotalRows tbl > prqlLimit
    then pure ("#" <> T.pack (show (_tblTotalRows tbl)))
    else if isNumeric ct then do
      -- read all cells, parse as Double, compute sum/avg/count
      vals <- readNumericCol tbl colIdx nr
      let n = length vals
      if n == 0 then pure ""
      else do
        let s = sum vals; a = s / fromIntegral n
            fmt x = let t = T.pack (show x) in if T.length t > 8 then T.take 8 t else t
        pure ("\931" <> fmt s <> " \956" <> fmt a <> " #" <> T.pack (show n))
    else
      -- non-numeric: count non-null
      pure ("#" <> T.pack (show nr))

-- | Read non-null numeric values from a column
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

-- | Update cache if column changed. Returns (updated cache, agg text for rendering).
-- The caller is responsible for rendering the agg string on the status bar.
update :: Cache -> TblOps -> Text -> Int -> IO Cache
update cache@(cachedPath, cachedCol, _) tbl path colIdx = do
  cache' <-
    if cachedPath == path && cachedCol == colIdx then pure cache
    else do
      r <- try (compute tbl colIdx)
      let agg' = case r of Left (_ :: SomeException) -> ""; Right a -> a
      pure (path, colIdx, agg')
  pure cache'
