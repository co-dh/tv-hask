-- | Freq dispatch: pure update by Cmd, returns residual Effect for
-- App to execute. Port of Tc/Runner.lean (Tc.Freq namespace).
--
-- filterExprIO lives in Tv.Freq.filterExpr (already ported); this
-- module adds the pure command→effect dispatch that the Lean version
-- co-locates in Runner.lean.
module Tv.Runner
  ( freqUpdate
  ) where

import Data.Vector (Vector)
import qualified Data.Vector as V

import Tv.Types
import Tv.View

-- | Pure update by Cmd. Returns residual Effect for dispatch to execute.
-- Mirrors Tc.Freq.update (Runner.lean): builds the column name list
-- from nav.grp + current column, then pattern-matches the command.
freqUpdate :: ViewStack -> Cmd -> Maybe (ViewStack, Effect)
freqUpdate s cmd =
  let n = _vNav (_vsHd s)
      curName = curColName n
      colNames = if V.elem curName (_nsGrp n) then _nsGrp n else V.snoc (_nsGrp n) curName
  in case cmd of
    FreqOpen -> Just (s, EFreq colNames)
    FreqFilter -> case _nsVkind n of
      VFreq cols _ -> Just (s, EFreqFilter cols (_naCur (_nsRow n)))
      _ -> Nothing
    _ -> Nothing
