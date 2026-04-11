-- | Navigation: pure cursor/selection updates dispatched by Cmd.
-- All functions are pure (NavState -> NavState or Maybe NavState).
module Tv.Nav where

import qualified Data.Vector as V
import Data.Text (Text)
import Tv.Types

-- | Execute a navigation command. Returns Nothing if command not handled.
execNav :: Cmd -> Int -> NavState -> Maybe NavState
execNav cmd rowPg ns = case cmd of
  RowInc  -> Just $ moveRow 1
  RowDec  -> Just $ moveRow (-1)
  RowPgdn -> Just $ moveRow rowPg
  RowPgup -> Just $ moveRow (-rowPg)
  RowTop  -> Just $ moveRow (negate $ _naCur (_nsRow ns))
  RowBot  -> Just $ moveRow (nr - 1 - _naCur (_nsRow ns))
  ColInc  -> Just $ moveCol 1
  ColDec  -> Just $ moveCol (-1)
  ColFirst -> Just $ moveCol (negate $ _naCur (_nsCol ns))
  ColLast  -> Just $ moveCol (nc - 1 - _naCur (_nsCol ns))
  RowSel  -> Just $ ns { _nsRow = (_nsRow ns) { _naSels = vToggle (_naCur $ _nsRow ns) (_naSels $ _nsRow ns) } }
  ColGrp  -> Just $ let g' = vToggleText (curColName ns) (_nsGrp ns)
                        names = _tblColNames (_nsTbl ns)
                    in ns { _nsGrp = g', _nsDispIdxs = dispOrder g' names }
  ColHide -> Just $ ns { _nsHidden = vToggleText (curColName ns) (_nsHidden ns) }
  ColShiftL -> shiftGrp (-1)
  ColShiftR -> shiftGrp 1
  _ -> Nothing
  where
    nr = _tblNRows (_nsTbl ns)
    nc = V.length (_tblColNames (_nsTbl ns))
    moveRow d = ns { _nsRow = axisMove nr d (_nsRow ns) }
    moveCol d = ns { _nsCol = axisMove nc d (_nsCol ns) }

    shiftGrp dir =
      let name = curColName ns
          grp = _nsGrp ns
      in case V.elemIndex name grp of
        Nothing -> Nothing
        Just i ->
          let j = i + dir in
          if j < 0 || j >= V.length grp then Nothing
          else let g' = grp V.// [(i, grp V.! j), (j, grp V.! i)]
                   names = _tblColNames (_nsTbl ns)
               in Just $ ns { _nsGrp = g', _nsDispIdxs = dispOrder g' names
                            , _nsCol = axisMove nc dir (_nsCol ns) }

-- | Toggle text element in vector
vToggleText :: Text -> V.Vector Text -> V.Vector Text
vToggleText x xs = if V.elem x xs then V.filter (/= x) xs else V.snoc xs x
