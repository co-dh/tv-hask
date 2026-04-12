-- | Meta: column metadata view for an underlying TblOps.
--
-- Ported from Tc/Meta.lean. The Lean version materializes column stats via a
-- DuckDB temp table; here the input TblOps is already a materialized grid, so
-- we compute the same columns (name, coltype, cnt, dist, null_pct, mn, mx)
-- directly from 'readRow'. Row order follows the input column order so row
-- index == original column index, matching Lean's rowidx→col mapping used by
-- meta.setKey / meta.selNull / meta.selSingle.
module Tv.Meta (mkMetaOps) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Set as Set

import Tv.Types

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
readCol t c = V.generateM (_tblNRows t) (\r -> _tblCellStr t r c)

-- | Build column-metadata TblOps from an underlying table.
mkMetaOps :: TblOps -> IO TblOps
mkMetaOps src = do
  let names = _tblColNames src
      nc    = V.length names
  rows <- V.generateM nc $ \i -> do
    cells <- readCol src i
    let (cnt, distN, nullPct, mn, mx) = colStats cells
        ty = colTypeName (_tblColType src i)
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
