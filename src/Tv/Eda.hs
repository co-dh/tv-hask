{-# LANGUAGE OverloadedStrings #-}
{-
  Eda: exploratory-data-analysis primitives that don't fit into the
  existing views — random sampling, duplicate detection, and 2-way
  crosstab. Each pushes a new view tagged with its own disp label so
  the user can pop back to the source table.
-}
module Tv.Eda
  ( sample
  , dupes
  , crosstab
  , commands
  ) where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.App.Types (AppState (stk), HandlerFn, tryStk)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Prql as Prql
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable, stripSemi, tmpName, fromTmp)
import qualified Tv.Nav as Nav
import Tv.Types (Cmd (..), escSql, isNumeric)
import Tv.View (ViewStack)
import qualified Tv.View as View

-- | Default row count for `?` sample. Big enough to be useful as an
-- EDA snapshot, small enough to avoid pulling GB of data.
sampleSize :: Int
sampleSize = 1000

-- | Push a sample view onto the stack. Uses DuckDB's reservoir sampler
-- so row selection is uniform regardless of table size.
sample :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
sample = pushWith "sample" $ \baseSql _s ->
  pure $ Just $
    "SELECT * FROM (" <> stripSemi baseSql <> ") USING SAMPLE "
      <> T.pack (show sampleSize) <> " ROWS"

-- | Push a duplicates view: rows that share their group-column tuple
-- with at least one other row. Falls back to every column if no group
-- is set (i.e. full-row duplicates).
dupes :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
dupes = pushWith "dupes" $ \baseSql s ->
  let grp    = View.cur s ^. #nav % #grp
      allCol = Table.colNames (View.tbl s)
      keys   = if V.null grp then allCol else grp
  in if V.null keys
       then pure Nothing
       else pure $ Just $
         "SELECT * FROM (" <> stripSemi baseSql <> ") "
           <> "QUALIFY COUNT(*) OVER (PARTITION BY "
           <> T.intercalate ", " (map Ops.quoteId (V.toList keys))
           <> ") > 1"

-- | Push a 2-way crosstab view: rows = distinct values of group[0],
-- columns = distinct values of group[1], cells = count. Falls back to
-- 'Nothing' unless exactly two group columns are set; that matches the
-- intent — 1-way is already the existing F view.
crosstab :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
crosstab = pushWith "xtab" $ \baseSql s ->
  let grp = View.cur s ^. #nav % #grp
  in case V.toList grp of
       [rowKey, colKey] -> pure $ Just $
         "PIVOT (SELECT " <> Ops.quoteId rowKey <> ", " <> Ops.quoteId colKey
           <> " FROM (" <> stripSemi baseSql <> ")) "
           <> "ON " <> Ops.quoteId colKey
           <> " USING COUNT(*) "
           <> "GROUP BY " <> Ops.quoteId rowKey
       _ -> pure Nothing

-- | Shared plumbing: compile the current base PRQL → SQL, hand the
-- SQL and stack to @mk@ (which may return @Nothing@ to abort), wrap
-- the result in a temp table, rebuild as AdbcTable, push a fresh view.
pushWith
  :: Text
  -> (Text -> ViewStack AdbcTable -> IO (Maybe Text))
  -> ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
pushWith label mk s = do
  mBase <- Prql.compile (Prql.queryRender (Table.query (View.tbl s)))
  case mBase of
    Nothing -> pure Nothing
    Just baseSql -> do
      mSql <- mk baseSql s
      case mSql of
        Nothing  -> pure Nothing
        Just sql -> do
          tbl <- tmpName label
          Ops.createTempTable tbl ("(" <> sql <> ")")
          mAdbc <- fromTmp tbl
          pure $ mAdbc >>= \adbc ->
            fmap (\v -> View.push s (v & #disp .~ label))
              (View.fromTbl adbc (View.path (View.cur s)) 0 V.empty 0)

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdSample   ""  "?" "Random sample 1000 rows" True "")
        (\a ci _ -> tryStk a ci (sample (stk a)))
  , hdl (mkEntry CmdDupes    ""  "u" "Rows sharing group-column values with others" True "")
        (\a ci _ -> tryStk a ci (dupes (stk a)))
  , hdl (mkEntry CmdCrosstab ""  "P" "2-way crosstab/pivot (requires exactly two grouped cols)" True "")
        (\a ci _ -> tryStk a ci (crosstab (stk a)))
  ]
