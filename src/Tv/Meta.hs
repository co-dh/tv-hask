-- | Meta: column metadata view for an underlying TblOps.
--
-- Ported from Tc/Meta.lean. The Lean version materializes column stats via a
-- DuckDB temp table; here the input TblOps is already a materialized grid, so
-- we compute the same columns (name, coltype, cnt, dist, null_pct, mn, mx)
-- directly from 'readRow'. Row order follows the input column order so row
-- index == original column index, matching Lean's rowidx→col mapping used by
-- meta.setKey / meta.selNull / meta.selSingle.
module Tv.Meta
  ( mkMetaOps
  , metaPushH
  , metaSetKeyH
  , metaSelNullH
  , metaSelSingleH
  , metaSelByColValue
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Set as Set

import Tv.Types
import Tv.View (vNav, vPath, vsHd, vsTl)
import Tv.Render (asStack, headNav, headView)
import Tv.Eff (Eff, IOE, (:>), use, (.=), (%=), liftIO)
import Tv.Handler (Handler, curOps, pushOps, refresh, refreshGrid, setMsg)
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- | Push a column-metadata view computed from the current table.
metaPushH :: Handler
metaPushH _ = do
  tbl  <- curOps
  path <- use (headView % vPath)
  ops' <- mkMetaOps tbl
  pushOps (path <> " [meta]") VColMeta ops'

-- | MetaSetKey: in a colMeta view, take the row selections (which are
-- column indices in the parent) and set them as the parent's group
-- columns, then pop back to the parent.
metaSetKeyH :: Handler
metaSetKeyH _ = do
  vk <- use (headNav % nsVkind)
  case vk of
    VColMeta -> do
      tl <- use (asStack % vsTl)
      case tl of
        [] -> setMsg "meta.key: no parent view"
        (parent:_) -> do
          sels <- use (headNav % nsRow % naSels)
          let parentNames = parent ^. vNav % nsTbl % tblColNames
              keyNames = V.mapMaybe (\i -> parentNames V.!? i) sels
              parent' = parent & vNav % nsGrp .~ keyNames
                               & vNav % nsDispIdxs .~ dispOrder keyNames parentNames
          asStack % vsHd .= parent'
          asStack % vsTl %= drop 1
          refresh
    _ -> setMsg "meta.key: not a meta view"

-- | Meta "select rows where null_pct == 100".
metaSelNullH :: Handler
metaSelNullH = metaSelByColValue "null_pct" (== 100) "no null columns"

-- | Meta "select rows where dist == 1".
metaSelSingleH :: Handler
metaSelSingleH = metaSelByColValue "dist" (== 1) "no single-value columns"

metaSelByColValue :: Text -> (Int -> Bool) -> Text -> Handler
metaSelByColValue colName predFn emptyMsg _ = do
  vk <- use (headNav % nsVkind)
  case vk of
    VColMeta -> do
      ns <- use headNav
      let ops = ns ^. nsTbl
          names = ops ^. tblColNames
      case V.elemIndex colName names of
        Nothing -> setMsg ("meta.sel: column '" <> colName <> "' missing")
        Just colIdx -> do
          let nr = ops ^. tblNRows
          matches <- liftIO (V.filterM (\r -> do
              cell <- (ops ^. tblCellStr) r colIdx
              let v = case reads (T.unpack cell) of [(n,"")] -> n; _ -> (0 :: Int)
              pure (predFn v)) (V.enumFromN 0 nr))
          if V.null matches then setMsg emptyMsg
          else do
            headNav % nsRow % naSels .= matches
            refreshGrid
            setMsg (T.pack (show (V.length matches)) <> " columns selected")
    _ -> setMsg "meta.sel: not a meta view"

-- | Column type name matching Lean's ColType.toString (lowercased tag).
colTypeName :: ColType -> Text
colTypeName = \case
  CTInt -> "int"; CTFloat -> "float"; CTDecimal -> "decimal"
  CTStr -> "str"; CTDate -> "date"; CTTime -> "time"
  CTTimestamp -> "timestamp"; CTBool -> "bool"; CTOther -> "other"

-- | Meta output columns, matching Tc.Data.ADBC.Ops.colStatsSql.
metaCols :: Vector Text
metaCols = V.fromList ["column", "coltype", "cnt", "dist", "null_pct", "mn", "mx"]

-- | Stats for one source column: (non-null count, distinct count, null %, min, max).
--   Empty strings are treated as NULL (matches _tblCellStr's NULL rendering).
colStats :: Vector Text -> (Int, Int, Int, Text, Text)
colStats cells =
  let nr      = V.length cells
      nonNull = V.filter (not . T.null) cells
      cnt     = V.length nonNull
      distN   = Set.size (Set.fromList (V.toList nonNull))
      nullPct | nr == 0   = 0
              | otherwise = round ((fromIntegral (nr - cnt) * 100 :: Double) / fromIntegral nr)
      mn = if V.null nonNull then "" else V.minimum nonNull
      mx = if V.null nonNull then "" else V.maximum nonNull
  in (cnt, distN, nullPct, mn, mx)

-- | Read one full column from the source table as a Text vector.
readCol :: TblOps -> Int -> IO (Vector Text)
readCol t c = V.generateM ((t ^. tblNRows)) (\r -> (t ^. tblCellStr) r c)

-- | Build column-metadata TblOps from an underlying table.
mkMetaOps :: IOE :> es => TblOps -> Eff es TblOps
mkMetaOps src = liftIO $ do
  let names = (src ^. tblColNames)
      nc    = V.length names
  rows <- V.generateM nc $ \i -> do
    cells <- readCol src i
    let (cnt, distN, nullPct, mn, mx) = colStats cells
        ty = colTypeName ((src ^. tblColType) i)
    pure $ V.fromList
      [ names V.! i, ty
      , T.pack (show cnt), T.pack (show distN), T.pack (show nullPct)
      , mn, mx ]
  pure (mkMetaRowOps rows)

-- | Read-only grid TblOps for the meta view. All mutation hooks are no-ops.
mkMetaRowOps :: Vector (Vector Text) -> TblOps
mkMetaRowOps rows =
  let nr = V.length rows
      nc = V.length metaCols
      ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = metaCols
        , _tblTotalRows   = nr
        , _tblQueryOps    = V.empty
        , _tblFilter      = \_ -> pure Nothing
        , _tblDistinct    = \_ -> pure V.empty
        , _tblFindRow     = \_ _ _ _ -> pure Nothing
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = \i -> if i >= 2 && i <= 4 then CTInt else CTStr
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
