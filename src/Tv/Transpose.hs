{-# LANGUAGE OverloadedStrings #-}
{-
  Transpose: swap rows and columns via DuckDB UNPIVOT+PIVOT.
  Useful for wide tables with few rows. Capped at 200 rows (each becomes a column).

  Literal port of Tc/Tc/Transpose.lean.
-}
module Tv.Transpose
  ( push
  , commands
  ) where

import Tv.Prelude
import qualified Data.Vector as V

import Tv.App.Types (AppState(..), HandlerFn, tryStk)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import Tv.Data.DuckDB.Ops (transposeSql, createTempTable)
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable, stripSemi, tmpName, fromTmp)
import qualified Tv.Data.DuckDB.Table as Table
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
  if V.null (Table.colNames t) || Table.nRows t == 0
    then pure Nothing
    else do
      mBase <- Prql.compile (Prql.queryRender (Table.query t))
      case mBase of
        Nothing -> pure Nothing
        Just baseSql -> do
          let n   = min (Table.nRows t) maxRows
              sql = transposeSql (stripSemi baseSql) (Table.colNames t) n
          tblName <- tmpName "xpose"
          createTempTable tblName ("(" <> sql <> ")")
          mAdbc <- fromTmp tblName
          case mAdbc of
            Nothing -> pure Nothing
            Just adbc ->
              pure $ fmap (\v -> View.push s (v & #disp .~ "xpose"))
                         $ View.fromTbl adbc (View.path $ View.cur s) 0 V.empty 0

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdTblXpose "" "X" "Transpose table (rows <-> columns)" False "")
        (\a ci _ -> tryStk a ci (push (stk a)))
  ]
