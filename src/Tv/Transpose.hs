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

import Data.Text (Text)
import qualified Data.Text as T
import Data.Maybe (fromMaybe)
import Data.Vector (Vector)
import qualified Data.Vector as V

import Optics.Core ((&), (.~))

import Tv.App.Types (AppState(..), HandlerFn, tryStk)
import Tv.CmdConfig (Entry, CmdInfo(..), mkEntry, hdl)
import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Ops (quoteId)
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable, stripSemi, tmpName, fromTmp)
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Types (Cmd(..), escSql)
import Tv.View (View(..), ViewStack)
import qualified Tv.View as View

-- | Max rows to transpose (each original row becomes a column)
maxRows :: Int
maxRows = 200

-- | Build UNPIVOT+PIVOT SQL for transposing a table.
--   All columns are cast to VARCHAR since mixed types collapse into one column per original row.
--   Preserves original column order via _ord (not alphabetical).
transposeSql :: Text -> Vector Text -> Int -> Text
transposeSql baseSql colNames nRows =
  let n = min nRows maxRows
      qid = quoteId
      castCols = T.intercalate ", "
        (V.toList (V.map (\c -> "CAST(" <> qid c <> " AS VARCHAR) AS " <> qid c) colNames))
      unpivotCols = T.intercalate ", " (V.toList (V.map qid colNames))
      pivotCols = T.intercalate ", "
        [ "MAX(CASE WHEN _rn = " <> T.pack (show i) <> " THEN _val END) AS \"row_" <> T.pack (show i) <> "\""
        | i <- [0 .. n - 1]
        ]
      -- _ord preserves original column order (UNPIVOT emits names in declaration order,
      -- but GROUP BY would lose that without an explicit ordering column)
      ordCases = T.intercalate " "
        [ "WHEN \"column\" = '" <> escSql (fromMaybe "" (colNames V.!? i)) <> "' THEN " <> T.pack (show i)
        | i <- [0 .. V.length colNames - 1]
        ]
  in "WITH __src AS (SELECT * FROM (" <> baseSql <> ") LIMIT " <> T.pack (show n) <> "), "
  <> "__num AS (SELECT ROW_NUMBER() OVER () - 1 AS _rn, " <> castCols <> " FROM __src), "
  <> "__unp AS (UNPIVOT __num ON " <> unpivotCols <> " INTO NAME \"column\" VALUE _val) "
  <> "SELECT \"column\", " <> pivotCols <> " FROM __unp GROUP BY \"column\" "
  -- Preserve original column order via CASE mapping column name -> position
  <> "ORDER BY CASE " <> ordCases <> " END"

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
          let sql = transposeSql (stripSemi baseSql) (Table.colNames t) (Table.nRows t)
          tblName <- tmpName "xpose"
          _ <- Conn.query ("CREATE OR REPLACE TEMP TABLE " <> tblName <> " AS (" <> sql <> ")")
          mAdbc <- fromTmp tblName
          case mAdbc of
            Nothing -> pure Nothing
            Just adbc ->
              pure (fmap (\v -> View.push s (v & #disp .~ "xpose"))
                         (View.fromTbl adbc (View.path (View.cur s)) 0 V.empty 0))

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdTblXpose "" "X" "Transpose table (rows <-> columns)" False "")
        (\a ci _ -> tryStk a ci (push (stk a)))
  ]
