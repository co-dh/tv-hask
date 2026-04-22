{-
  Freq: group by columns, count, pct, bar.
  Pure update returns residual Effect; dispatch executes IO inline.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Freq where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified Tv.Nav as Nav
import Tv.Types
  ( Cmd (..)
  , Effect (..)
  , ViewKind (..)
  , toPrql
  )
import qualified Tv.View as View
import Tv.View (ViewStack)
import qualified Tv.Data.DuckDB.Ops as Ops
import Tv.Data.DuckDB.Table (AdbcTable)

-- | Build filter expression from freq view row
filterIO :: AdbcTable -> Vector Text -> Int -> IO Text
filterIO tbl cols row =
  let names = tbl ^. #colNames
      idxs  = V.mapMaybe (Nav.idxOf names) cols
      fetchedCols = Ops.getCols tbl idxs row (row + 1)
      vals = V.zipWith (\txtCol colIdx ->
        toPrql (Ops.colType tbl colIdx) (fromMaybe "" (txtCol V.!? 0))
        ) fetchedCols idxs
      exprs = V.zipWith (\c v -> c <> " == " <> v) cols vals
  in pure (T.intercalate " && " (V.toList exprs))

-- | Pure update by Cmd. Returns residual Effect for dispatch to execute.
update :: ViewStack AdbcTable -> Cmd -> Maybe (ViewStack AdbcTable, Effect)
update s h =
  let n        = View.cur s ^. #nav
      curName  = Nav.colName n
      colNames_ = if V.elem curName (n ^. #grp)
                    then n ^. #grp
                    else V.snoc (n ^. #grp) curName
  in case h of
       CmdFreqOpen   -> Just (s, EffectFreq colNames_)
       CmdFreqFilter -> case View.cur s ^. #vkind of
         VkFreqV cols _ ->
           Just (s, EffectFreqFilter cols (View.cur s ^. #nav % #row % #cur))
         _ -> Nothing
       _ -> Nothing
