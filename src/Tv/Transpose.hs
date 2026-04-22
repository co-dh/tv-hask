{-# LANGUAGE OverloadedStrings #-}
{-
  Transpose: swap rows and columns via DuckDB UNPIVOT+PIVOT.
  Useful for wide tables with few rows. Capped at 200 rows (each becomes a column).

  Literal port of Tc/Tc/Transpose.lean.
-}
module Tv.Transpose where

import Tv.Prelude
import qualified Data.Vector as V

import Tv.App.Types (HandlerFn, tryStk)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import Tv.Data.DuckDB.Ops (transposeSql, createTempTable)
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable, stripSemi, tmpName, fromTmp)
import Tv.Types (Cmd(..))
import Tv.View (ViewStack)
import qualified Tv.View as View

-- | Max rows to transpose (each original row becomes a column).
--   DuckDB UNPIVOT+PIVOT blows out column count otherwise.
maxRows :: Int
maxRows = 200

-- | Push transposed view onto stack
push :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
push s = do
  let t = View.tbl s
  if V.null (t ^. #colNames) || t ^. #nRows == 0
    then pure Nothing
    else do
      mBase <- Prql.compile (Prql.queryRender (t ^. #query))
      case mBase of
        Nothing -> pure Nothing
        Just baseSql -> do
          let n   = min (t ^. #nRows) maxRows
              sql = transposeSql (stripSemi baseSql) (t ^. #colNames) n
          tblName <- tmpName "xpose"
          createTempTable tblName ("(" <> sql <> ")")
          mAdbc <- fromTmp tblName
          case mAdbc of
            Nothing -> pure Nothing
            Just adbc ->
              pure $ fmap (\v -> View.push s (v & #disp .~ "xpose"))
                         $ View.fromTbl adbc (View.cur s ^. #path) 0 V.empty 0

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdTblXpose "" "X" "Transpose table (rows <-> columns)" False "")
        (\a ci _ -> tryStk a ci (push (a ^. #stk)))
  ]
