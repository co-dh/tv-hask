-- | Navigation: pure cursor/selection updates dispatched by Cmd.
-- All functions are pure (NavState -> NavState or Maybe NavState).
module Tv.Nav where

import qualified Data.Vector as V
import Tv.Types
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- | Execute a navigation command. Returns Nothing if command not handled.
execNav :: Cmd -> Int -> NavState -> Maybe NavState
execNav cmd rowPg ns = case cmd of
  RowInc   -> Just $ moveRow 1
  RowDec   -> Just $ moveRow (-1)
  RowPgdn  -> Just $ moveRow rowPg
  RowPgup  -> Just $ moveRow (-rowPg)
  RowTop   -> Just $ moveRow (negate (ns ^. nsRow % naCur))
  RowBot   -> Just $ moveRow (nr - 1 - ns ^. nsRow % naCur)
  ColInc   -> Just $ moveCol 1
  ColDec   -> Just $ moveCol (-1)
  ColFirst -> Just $ moveCol (negate (ns ^. nsCol % naCur))
  ColLast  -> Just $ moveCol (nc - 1 - ns ^. nsCol % naCur)
  RowSel   -> Just $ ns & nsRow % naSels %~ vToggle (ns ^. nsRow % naCur)
  ColGrp   -> Just $ ns & nsGrp .~ g' & nsDispIdxs .~ dispOrder g' names
    where g'    = vToggle (curColName ns) (ns ^. nsGrp)
          names = ns ^. nsTbl % tblColNames
  ColHide   -> Just $ ns & nsHidden %~ vToggle (curColName ns)
  ColShiftL -> shiftGrp (-1)
  ColShiftR -> shiftGrp 1
  _         -> Nothing
  where
    nr = ns ^. nsTbl % tblNRows
    nc = V.length (ns ^. nsTbl % tblColNames)
    moveRow d = ns & nsRow %~ axisMove nr d
    moveCol d = ns & nsCol %~ axisMove nc d

    shiftGrp dir =
      let name = curColName ns
          grp  = ns ^. nsGrp
      in case V.elemIndex name grp of
        Nothing -> Nothing
        Just i  ->
          let j = i + dir in
          if j < 0 || j >= V.length grp then Nothing
          else let g'    = grp V.// [(i, grp V.! j), (j, grp V.! i)]
                   names = ns ^. nsTbl % tblColNames
               in Just $ ns & nsGrp .~ g'
                            & nsDispIdxs .~ dispOrder g' names
                            & nsCol %~ axisMove nc dir
