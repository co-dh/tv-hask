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
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- | Pure update by Cmd. Returns residual Effect for dispatch to execute.
-- Mirrors Tc.Freq.update (Runner.lean): builds the column name list
-- from nav.grp + current column, then pattern-matches the command.
freqUpdate :: ViewStack -> Cmd -> Maybe (ViewStack, Effect)
freqUpdate s cmd =
  let n = s ^. vsHd % vNav
      curName = curColName n
      grp = n ^. nsGrp
      colNames = if V.elem curName grp then grp else V.snoc grp curName
  in case cmd of
    FreqOpen -> Just (s, EFreq colNames)
    FreqFilter -> case n ^. nsVkind of
      VFreq cols _ -> Just (s, EFreqFilter cols (n ^. nsRow % naCur))
      _ -> Nothing
    _ -> Nothing
