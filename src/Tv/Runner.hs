{-
  Freq: group by columns, count, pct, bar.
  Pure update returns residual Effect; dispatch executes IO inline.

  Literal port of Tc/Tc/Runner.lean. Lean namespace Tc.Freq lives here so
  the file name matches the Lean source (Runner.lean).
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Runner
  ( filterExprIO
  , update
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import qualified Tv.Nav as Nav
import Tv.Types
  ( Cmd (..)
  , Effect (..)
  , ViewKind (..)
  , cellToPrql
  , columnGet
  )
import qualified Tv.Types as TblOps
import qualified Tv.View as View
import Tv.View (ViewStack)
import Tv.Data.ADBC.Table (AdbcTable)
import Tv.Data.ADBC.Ops ()  -- TblOps AdbcTable instance

-- | Build filter expression from freq view row
filterExprIO :: AdbcTable -> Vector Text -> Int -> IO Text
filterExprIO tbl cols row = do
  let names = TblOps.colNames tbl
      idxs  = V.mapMaybe (Nav.idxOf names) cols
  fetchedCols <- TblOps.getCols tbl idxs row (row + 1)
  let vals  = V.map (\col -> cellToPrql (columnGet col 0)) fetchedCols
      exprs = V.zipWith (\c v -> c <> " == " <> v) cols vals
  pure (T.intercalate " && " (V.toList exprs))

-- | Pure update by Cmd. Returns residual Effect for dispatch to execute.
update :: ViewStack AdbcTable -> Cmd -> Maybe (ViewStack AdbcTable, Effect)
update s h =
  let n        = View.nav (View.cur s)
      curName  = Nav.curColName n
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
