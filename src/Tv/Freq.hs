{-
  Freq: group by columns, count, pct, bar.
  Pure update returns residual Effect; dispatch executes IO inline.
  execFreq is the actual compute; runs on a DataFrame via Tv.Df.Freq
  and bridges to/from AdbcTable while the View stack still holds one.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Freq
  ( filterIO
  , update
  , execFreq
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Maybe (fromMaybe)
import Data.Vector (Vector)
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
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)

import qualified Tv.Df as Df
import qualified Tv.Df.Bridge as Bridge
import qualified Tv.Df.Freq as DfFreq

-- Truncate freq results to this many rows for display (matches the
-- `| take 1000` clause from the previous PRQL implementation).
freqDisplayLimit :: Int
freqDisplayLimit = 1000

-- | Build filter expression from freq view row
filterIO :: AdbcTable -> Vector Text -> Int -> IO Text
filterIO tbl cols row = do
  let names = Table.colNames tbl
      idxs  = V.mapMaybe (Nav.idxOf names) cols
  fetchedCols <- Ops.getCols tbl idxs row (row + 1)
  let vals = V.zipWith (\txtCol colIdx ->
        toPrql (Ops.colType tbl colIdx) (fromMaybe "" (txtCol V.!? 0))
        ) fetchedCols idxs
      exprs = V.zipWith (\c v -> c <> " == " <> v) cols vals
  pure (T.intercalate " && " (V.toList exprs))

-- | Run the freq computation end-to-end on an AdbcTable.
--
-- Lowers the AdbcTable to a DataFrame via Tv.Df.Bridge, runs the
-- frequency pipeline (group/count/pct/bar) purely on the DataFrame,
-- caps the result at @freqDisplayLimit@, and bridges back to an
-- AdbcTable for the View stack. Returns @Nothing@ when the group
-- columns are empty or the bridge round-trip fails.
execFreq :: AdbcTable -> Vector Text -> IO (Maybe (AdbcTable, Int))
execFreq t cNames
  | V.null cNames = pure Nothing
  | otherwise = do
      src <- Bridge.fromAdbc t
      let fullFreq = DfFreq.freq (V.toList cNames) src
          nGroups  = Df.nRows fullFreq
          shown    = Df.take freqDisplayLimit fullFreq
      mAdbc <- Bridge.toAdbc shown
      pure (fmap (\adbc -> (adbc, nGroups)) mAdbc)

-- | Pure update by Cmd. Returns residual Effect for dispatch to execute.
update :: ViewStack AdbcTable -> Cmd -> Maybe (ViewStack AdbcTable, Effect)
update s h =
  let n        = View.nav (View.cur s)
      curName  = Nav.colName n
      colNames_ = if V.elem curName (Nav.grp n)
                    then Nav.grp n
                    else V.snoc (Nav.grp n) curName
  in case h of
       CmdFreqOpen   -> Just (s, EffectFreq colNames_)
       CmdFreqFilter -> case View.vkind (View.cur s) of
         VkFreqV cols _ ->
           Just (s, EffectFreqFilter cols (Nav.cur (Nav.row (View.nav (View.cur s)))))
         _ -> Nothing
       _ -> Nothing
