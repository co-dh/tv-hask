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
  , freqOpenH
  , freqFilterH
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.Core ((^.), (%), (&), (.~))

import Tv.Types
import Tv.View (vNav, vPath, vsTl)
import Tv.Render (asStack, headNav, headView)
import qualified Tv.Derive as Derive
import Tv.Eff (Eff, IOE, (:>), use, liftIO)
import Tv.Handler (Handler, curOps, pushOpsAt, setMsg)

-- | Open a frequency view on (nav.grp ++ current col). The grouping
-- columns are resolved to indices against the parent table's column
-- names and passed to 'mkFreqOps'. The new view is pushed with a VFreq
-- vkind carrying the group column names and total distinct-group count.
freqOpenH :: Handler
freqOpenH _ = do
  ns   <- use headNav
  path <- use (headView % vPath)
  let ops     = ns ^. nsTbl
      names   = ops ^. tblColNames
      grp     = ns ^. nsGrp
      curName = curColName ns
      colNames =
        if V.elem curName grp then grp else V.snoc grp curName
      colIdxs = V.mapMaybe (`V.elemIndex` names) colNames
  if V.null colIdxs
    then setMsg "freq: no columns selected"
    else do
      ops' <- mkFreqOps ops colIdxs
      let total = ops' ^. tblNRows
          vk    = VFreq colNames total
          path' = path <> " [freq " <> T.intercalate "," (V.toList colNames) <> "]"
      pushOpsAt path' 0 colNames "freq: empty result" vk ops'

-- | Filter the parent table by the current freq-view row. Reads the
-- group-key values of the focused row, builds a WHERE clause via
-- 'filterExpr', and calls the parent table's _tblFilter. The filter is
-- applied to the PARENT table (stack tail), not the freq table itself;
-- the filtered result is pushed on top of the parent view.
freqFilterH :: Handler
freqFilterH _ = do
  vk  <- use (headNav % nsVkind)
  case vk of
    VFreq cols _ -> do
      tl <- use (asStack % vsTl)
      case tl of
        [] -> setMsg "freq.filter: no parent view"
        (parent:_) -> do
          freqOps <- curOps
          row     <- use (headNav % nsRow % naCur)
          let parentOps = parent ^. vNav % nsTbl
          expr <- filterExpr freqOps cols row
          mOps <- liftIO ((parentOps ^. tblFilter) expr)
          case mOps of
            Nothing -> setMsg "freq.filter: filter failed"
            Just ops' ->
              pushOpsAt (parent ^. vPath) 0 (parent ^. vNav % nsGrp)
                        "freq.filter: empty result" VTbl ops'
    _ -> setMsg "freq.filter: not a freq view"

-- | Build a frequency-table TblOps. Given a source table and the column
-- indices to group on (from @NavState._nsGrp@ + current column), produce
-- a new read-only TblOps with columns @[grouped cols..., "count"]@ and
-- rows = distinct groupings sorted by count desc.
--
-- Returns the source table unchanged if @colIdxs@ is empty or any index
-- is out of range; falls back to the source on any DuckDB error too (the
-- Lean version is similarly fail-soft — AdbcTable.freqTable returns
-- 'none' and the caller leaves the stack alone).
mkFreqOps :: IOE :> es => TblOps -> Vector Int -> Eff es TblOps
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
      Derive.rebuildOrKeep src wrap
  where
    names = src ^. tblColNames
    nc    = V.length names
    outOfRange i = i < 0 || i >= nc

-- | Build a PRQL filter expression matching a freq view row. Given the
-- freq table, group column names, and row index, produces
-- @"`col1` == 'v1' && `col2` == 'v2'"@. Lean Tc.Freq.filterExprIO.
filterExpr :: IOE :> es => TblOps -> Vector Text -> Int -> Eff es Text
filterExpr freqOps cols row = liftIO $ do
  let nc = V.length cols
  parts <- V.generateM nc $ \i -> do
    v <- (freqOps ^. tblCellStr) row i
    let n = "`" <> (cols V.! i) <> "`"
    pure $ if T.null v
             then n <> " == null"
             else n <> " == '" <> T.replace "'" "''" v <> "'"
  pure (T.intercalate " && " (V.toList parts))
