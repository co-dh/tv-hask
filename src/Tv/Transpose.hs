-- | Transpose: swap rows ↔ columns of a small TblOps.
--
-- Ported from Tc/Transpose.lean. The Lean version generates an UNPIVOT+PIVOT
-- SQL against DuckDB; here the input TblOps is already a materialized grid,
-- so we transpose it directly. Capped at 'maxRows' source rows (each becomes
-- a column). Original column order is preserved via the "column" name column.
module Tv.Transpose (mkTransposedOps, xposeH) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Tv.Types
import Tv.View (vPath)
import Tv.Render (headView)
import Tv.Eff (Eff, IOE, (:>), use, liftIO)
import Tv.Handler (Handler, pushOps, curOps)
import Optics.Core ((^.), (%))

-- | Push a transposed-table view.
xposeH :: Handler
xposeH _ = do
  ops  <- curOps
  path <- use (headView % vPath)
  ops' <- mkTransposedOps ops
  pushOps (path <> " [T]") VTbl ops'

-- | Max source rows to transpose; matches Tc.Transpose.maxRows.
maxRows :: Int
maxRows = 200

-- | Build a transposed TblOps. Output layout:
--     col 0       = "column"  (original column name, VARCHAR)
--     col 1..n    = "row_0" .. "row_{n-1}" (original row i's values)
--
--   One output row per source column, in original column order. Returns an
--   empty-shaped ops when the source has no rows or no columns (mirrors Lean
--   returning 'none' from push).
mkTransposedOps :: IOE :> es => TblOps -> Eff es TblOps
mkTransposedOps src = do
  let names = (src ^. tblColNames)
      nc    = V.length names
      nr    = min maxRows ((src ^. tblNRows))
  -- Materialize only the rows we'll use; cells are read as Text so mixed
  -- source types collapse uniformly (Lean CASTs to VARCHAR for the same
  -- reason).
  rows <- liftIO $ V.generateM nc $ \c -> do
    vals <- V.generateM nr (\r -> (src ^. tblCellStr) r c)
    pure (V.cons (names V.! c) vals)
  let outCols = V.cons "column"
              $ V.generate nr (\i -> "row_" <> T.pack (show i))
  pure (mkGridOps outCols rows)

-- | Read-only grid TblOps. All mutation / query hooks are no-ops.
mkGridOps :: Vector Text -> Vector (Vector Text) -> TblOps
mkGridOps cols rows =
  let nr = V.length rows
      nc = V.length cols
      ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = cols
        , _tblTotalRows   = nr
        , _tblQueryOps    = V.empty
        , _tblFilter      = \_ -> pure Nothing
        , _tblDistinct    = \_ -> pure V.empty
        , _tblFindRow     = \_ _ _ _ -> pure Nothing
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = \_ -> CTStr
        , _tblBuildFilter = \_ _ _ _ -> T.empty
        , _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr     = \r c ->
            if r < 0 || r >= nr || c < 0 || c >= nc
              then pure T.empty
              else pure ((rows V.! r) V.! c)
        , _tblFetchMore   = pure Nothing
        , _tblHideCols    = \_ -> pure ops
        , _tblSortBy      = \_ _ -> pure ops
        }
  in ops
