{-# LANGUAGE ScopedTypeVariables #-}
-- | Freq: group by columns, COUNT(*), sort by count desc. Mirrors
-- Tc.Freq (Tc/Tc/Runner.lean) + Tc.AdbcTable.freqTable.
--
-- The Lean version pipes a PRQL @freq@ step onto the backing query and
-- lets DuckDB compute the aggregation in place (the source has a live
-- query context). Here the TblOps is an already-materialized grid, so
-- we round-trip the rows through an in-memory DuckDB table via
-- 'Tv.Derive.rebuildWith' and wrap the outer SELECT in a GROUP BY.
--
-- The resulting TblOps is fully materialized (via 'Tv.Data.DuckDB.mkDbOps')
-- with columns @[groupCol1, groupCol2, ..., count]@ and rows sorted by
-- @count@ descending.
module Tv.Freq
  ( mkFreqOps
  , filterExpr
  ) where

import Control.Exception (SomeException, handle)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Tv.Types
import qualified Tv.Derive as Derive

-- | Build a frequency-table TblOps. Given a source table and the column
-- indices to group on (from @NavState._nsGrp@ + current column), produce
-- a new read-only TblOps with columns @[grouped cols..., "count"]@ and
-- rows = distinct groupings sorted by count desc.
--
-- Returns the source table unchanged if @colIdxs@ is empty or any index
-- is out of range; falls back to the source on any DuckDB error too (the
-- Lean version is similarly fail-soft — AdbcTable.freqTable returns
-- 'none' and the caller leaves the stack alone).
mkFreqOps :: TblOps -> Vector Int -> IO TblOps
mkFreqOps src colIdxs
  | V.null colIdxs = pure src
  | V.any outOfRange colIdxs = pure src
  | otherwise = do
      let groupNames = V.map (names V.!) colIdxs
          groupSql   = T.intercalate ", " (V.toList (V.map Derive.quoteId groupNames))
          -- GROUP BY + COUNT(*) over the reconstructed source. LIMIT 1000
          -- mirrors Lean's `| take 1000`. ORDER BY count DESC makes the
          -- high-frequency groupings appear at the top of the view.
          wrap sub =
            "SELECT " <> groupSql <> ", COUNT(*) AS count FROM ("
              <> sub <> ") GROUP BY " <> groupSql
              <> " ORDER BY count DESC LIMIT 1000"
      handle (\(_ :: SomeException) -> pure src) (Derive.rebuildWith src wrap)
  where
    names = _tblColNames src
    nc    = V.length names
    outOfRange i = i < 0 || i >= nc

-- | Build a PRQL filter expression matching a freq view row. Given the
-- freq table, group column names, and row index, produces
-- @"`col1` == 'v1' && `col2` == 'v2'"@. Lean Tc.Freq.filterExprIO.
filterExpr :: TblOps -> Vector Text -> Int -> IO Text
filterExpr freqOps cols row = do
  let nc = V.length cols
  parts <- V.generateM nc $ \i -> do
    v <- _tblCellStr freqOps row i
    let n = "`" <> (cols V.! i) <> "`"
    pure $ if T.null v
             then n <> " == null"
             else n <> " == '" <> T.replace "'" "''" v <> "'"
  pure (T.intercalate " && " (V.toList parts))
