-- | View: wraps NavState + metadata. ViewStack: non-empty stack of views.
module Tv.View where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.TH (makeLenses)
import Tv.Nav (execNav)
import Tv.Types

-- ============================================================================
-- View
-- ============================================================================

data View = View
  { _vNav      :: !NavState
  , _vPath     :: !Text           -- source file/command
  , _vDisp     :: !Text           -- custom display name
  , _vWidths   :: !(Vector Int)   -- cached column widths
  , _vSearch   :: !(Maybe (Int, Text))  -- last search: (colIdx, value)
  , _vSameHide :: !(Vector Text)  -- diff: columns with identical values
  } deriving (Show)

makeLenses ''View

-- | Create from NavState + path
mkView :: NavState -> Text -> View
mkView ns path = View ns path "" V.empty Nothing V.empty

-- | Tab display name: custom disp or filename from path
tabName :: View -> Text
tabName v = case _nsVkind (_vNav v) of
  VFld p _ | T.null (_vDisp v) || "/" `T.isPrefixOf` p -> p
  _ | T.null (_vDisp v) -> last (T.splitOn "/" (_vPath v))
    | otherwise -> _vDisp v

-- | Build View from TblOps + path (returns Nothing if empty)
fromTbl :: TblOps -> Text -> Int -> Vector Text -> Int -> Maybe View
fromTbl ops path col grp row
  | nc <= 0 || nr <= 0 = Nothing
  | otherwise = Just $ mkView ns path
  where
    nr = _tblNRows ops
    nc = V.length (_tblColNames ops)
    ns = NavState ops (mkAxis { _naCur = clamp 0 nr row })
                      (mkAxis { _naCur = clamp 0 nc col })
                      grp V.empty (dispOrder grp (_tblColNames ops))
                      VTbl "" 0 0 0

-- | Rebuild with new TblOps, preserving old view attributes
rebuildView :: View -> TblOps -> Int -> Vector Text -> Int -> Maybe View
rebuildView old ops col grp row = inherit <$> fromTbl ops (_vPath old) col grp row
  where
    oldNs = _vNav old
    inherit v = v { _vNav = (_vNav v) { _nsHidden   = _nsHidden oldNs
                                      , _nsVkind    = _nsVkind oldNs
                                      , _nsSearch   = _nsSearch oldNs
                                      , _nsPrecAdj  = _nsPrecAdj oldNs
                                      , _nsWidthAdj = _nsWidthAdj oldNs
                                      , _nsHeatMode = _nsHeatMode oldNs }
                  , _vDisp = _vDisp old, _vSameHide = _vSameHide old }

-- | Pure update by command
updateView :: View -> Cmd -> Int -> Maybe (View, Effect)
updateView v cmd rowPg =
  let ns = _vNav v
      curCol = curColIdx ns
      nr = _tblNRows (_nsTbl ns)
  in case cmd of
    SortAsc  -> Just (v, ESort curCol (selColIdxs' ns) (grpIdxs ns) True)
    SortDesc -> Just (v, ESort curCol (selColIdxs' ns) (grpIdxs ns) False)
    _ -> case execNav cmd rowPg ns of
      Nothing -> Nothing
      Just ns' ->
        let needsMore = _naCur (_nsRow ns') + 1 >= nr
              && _tblTotalRows (_nsTbl ns) > nr
              && cmd `elem` [RowInc, RowPgdn, RowBot]
        in Just (v { _vNav = ns' }, if needsMore then EFetchMore else ENone)
  where
    selColIdxs' ns = V.mapMaybe (`V.elemIndex` _tblColNames (_nsTbl ns))
                      (V.map (\i -> _tblColNames (_nsTbl ns) V.! (_nsDispIdxs ns V.! i)) (_naSels (_nsCol ns)))
    grpIdxs ns = V.mapMaybe (`V.elemIndex` _tblColNames (_nsTbl ns)) (_nsGrp ns)

-- ============================================================================
-- ViewStack: non-empty view stack
-- ============================================================================

data ViewStack = ViewStack
  { _vsHd :: !View
  , _vsTl :: ![View]
  } deriving (Show)

-- | Placeholder Eq: View contains TblOps (record of functions) so a real
-- structural Eq is impossible. This one compares only _vPath/_vDisp so
-- assertions like @?= Nothing in PureSpec can type-check; equality of two
-- live Views/ViewStacks is not semantically meaningful.
instance Eq View where
  a == b = _vPath a == _vPath b && _vDisp a == _vDisp b
instance Eq ViewStack where
  a == b = _vsHd a == _vsHd b && length (_vsTl a) == length (_vsTl b)

makeLenses ''ViewStack

vsCur :: ViewStack -> View
vsCur = _vsHd

vsHasParent :: ViewStack -> Bool
vsHasParent = not . null . _vsTl

vsPush :: View -> ViewStack -> ViewStack
vsPush v (ViewStack hd tl) = ViewStack v (hd : tl)

vsPop :: ViewStack -> Maybe ViewStack
vsPop (ViewStack _ []) = Nothing
vsPop (ViewStack _ (h:t)) = Just (ViewStack h t)

vsSwap :: ViewStack -> ViewStack
vsSwap (ViewStack hd []) = ViewStack hd []
vsSwap (ViewStack hd (h:t)) = ViewStack h (hd : t)

vsDup :: ViewStack -> ViewStack
vsDup (ViewStack hd tl) = ViewStack hd (hd : tl)

-- | Pure stack-level dispatch.  Returns Nothing for commands not handled here
-- (the caller should fall through to per-view dispatch).
updateViewStack :: ViewStack -> Cmd -> Maybe (ViewStack, Effect)
updateViewStack vs = \case
  StkSwap -> Just (vsSwap vs, ENone)
  StkDup  -> Just (vsDup vs, ENone)
  StkPop  -> case vsPop vs of
    Nothing  -> Just (vs, EQuit)   -- last view → quit
    Just vs' -> Just (vs', ENone)
  _ -> Nothing
