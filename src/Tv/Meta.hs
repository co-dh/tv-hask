{-
  Meta view: column statistics via DuckDB temp table tc_meta_N.
  Selection and key operations use composable PRQL (via DuckDB/Meta).
  Each meta view gets a unique temp table so concurrent views don't collide.

  Literal port of Tc/Tc/Meta.lean — same function names, same order, same
  comments. No invented abstractions.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Meta where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.App.Types (HandlerFn, onStk, tryStk, viewUp)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import qualified Tv.Nav as Nav
import Tv.Types (Cmd (..), ViewKind (..))
import Tv.View (ViewStack)
import qualified Tv.View as View

import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)

-- | Extract meta table name from the current view's AdbcTable query base.
--   e.g. "from tc_meta_3" -> "tc_meta_3"
tblName :: ViewStack AdbcTable -> Text
tblName s = T.strip (T.drop 5 ((View.tbl s ^. #query) ^. #base))  -- drop "from "

-- | Push column metadata view onto stack
push :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
push s = do
  mAdbc <- Ops.queryMeta (View.tbl s)
  case mAdbc of
    Nothing -> pure Nothing
    Just adbc0 -> do
      -- Enrich meta with DuckDB column comments (e.g. osquery views with COMMENT ON COLUMN)
      let metaBase = T.strip $ T.drop 5 ((adbc0 ^. #query) ^. #base)
      enriched <- Ops.enrichComments metaBase (View.cur s ^. #path)
      adbc <-
        if enriched
          then fromMaybe adbc0 <$> Table.requery (adbc0 ^. #query) (adbc0 ^. #totalRows)
          else pure adbc0
      let mV = View.fromTbl adbc (View.cur s ^. #path) 0 mempty 0
      pure $ fmap (\v -> View.push s (v & #vkind .~ VkColMeta & #disp .~ "meta")) mV

-- | Select meta rows matching PRQL filter
selBy :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
selBy s flt =
  if View.cur s ^. #vkind /= VkColMeta then pure s
  else do
    rows <- Ops.metaIdxs (tblName s) flt
    pure (View.setCur s (View.cur s & #nav % #row % #sels .~ rows))

-- Data-driven selection predicates: (Cmd, key, label, PRQL filter)
metaSels :: [(Cmd, Text, Text, Text)]
metaSels =
  [ (CmdMetaSelNull,   "0", "Select columns with null values",  "null_pct == 100")
  , (CmdMetaSelSingle, "1", "Select columns with single value", "dist == 1")
  ]

selByH :: Text -> HandlerFn
selByH flt = onStk (fmap Just . (`selBy` flt))

-- | Recompute the meta view with expensive aggregates (mean/std/p25/p50/p75)
-- for the currently-selected rows whose column is numeric. Non-selected or
-- non-numeric rows keep NULL in the heavy columns — one-scan cost is bounded
-- by the selection, not the full table width.
stats :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
stats s
  | View.cur s ^. #vkind /= VkColMeta = pure (Just s)
  | not (View.hasParent s)            = pure (Just s)
  | otherwise = do
      colNames <- Ops.metaNames (tblName s) (View.cur s ^. #nav % #row % #sels)
      case View.pop s of
        Nothing -> pure (Just s)
        Just parent -> do
          mAdbc <- Ops.queryMetaStats (View.tbl parent) colNames
          case mAdbc of
            Nothing -> pure (Just s)
            Just adbc ->
              let mV = View.fromTbl adbc (View.cur s ^. #path) 0 mempty 0
              in pure $ fmap
                   (\v -> View.setCur s
                            (v & #vkind .~ VkColMeta & #disp .~ "meta+stats"))
                   mV

-- | Pearson correlation matrix across the selected numeric columns.
-- Pushes a new view tagged VkCorr with one row per column and one data
-- column per column — cells are CORR to 3 dp, ideal for heat mode 1
-- (the dispatcher enables it automatically for this view kind).
corr :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
corr s
  | View.cur s ^. #vkind /= VkColMeta = pure (Just s)
  | not (View.hasParent s)            = pure (Just s)
  | otherwise = do
      colNames <- Ops.metaNames (tblName s) (View.cur s ^. #nav % #row % #sels)
      case View.pop s of
        Nothing -> pure (Just s)
        Just parent -> do
          mAdbc <- Ops.queryCorrMatrix (View.tbl parent) colNames
          case mAdbc of
            Nothing -> pure (Just s)
            Just adbc ->
              let mV = View.fromTbl adbc (View.cur s ^. #path) 0 mempty 0
              in pure $ fmap
                   (\v -> View.setCur s
                            (v & #vkind .~ VkCorr & #disp .~ "corr"))
                   mV

-- | Set key cols from meta view selections, pop to parent, select cols
setKey :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
setKey s =
  if View.cur s ^. #vkind /= VkColMeta then pure (Just s)
  else if not (View.hasParent s) then pure (Just s)
  else do
    colNames <- Ops.metaNames (tblName s) (View.cur s ^. #nav % #row % #sels)
    case View.pop s of
      Just s' -> do
        let di = Nav.dispOrder colNames (View.tbl s' ^. #colNames)
            v  = View.cur s'
                 & #nav % #grp      .~ colNames
                 & #nav % #dispIdxs .~ di
                 & #nav % #col % #sels .~ colNames
        pure (Just (View.setCur s' v))
      Nothing -> pure (Just s)

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList $
  [ hdl (mkEntry CmdMetaPush      ""  "M"     "Open column metadata view"       True  "")        (onStk push)
  , hdl (mkEntry CmdMetaSetKey    "s" "<ret>" "Set selected rows as key columns" True  "colMeta")
        (\a ci _ -> if View.cur (a ^. #stk) ^. #vkind == VkColMeta then tryStk a ci (setKey (a ^. #stk)) else viewUp a ci)
  , hdl (mkEntry CmdMetaStats     "s" "S"     "Compute stats for selected numeric cols" True "colMeta")
        (\a ci _ -> if View.cur (a ^. #stk) ^. #vkind == VkColMeta then tryStk a ci (stats (a ^. #stk)) else viewUp a ci)
  , hdl (mkEntry CmdMetaCorr      "s" "C"     "Correlation matrix for selected numeric cols" True "colMeta")
        (\a ci _ -> if View.cur (a ^. #stk) ^. #vkind == VkColMeta then tryStk a ci (corr (a ^. #stk)) else viewUp a ci)
  ] ++ [ hdl (mkEntry c "" k lbl True "") (selByH flt) | (c, k, lbl, flt) <- metaSels ]
