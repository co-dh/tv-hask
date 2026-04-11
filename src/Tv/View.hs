-- | View: wraps NavState + metadata. ViewStack: non-empty stack of views.
module Tv.View where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.TH (makeLenses)
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
  _ | T.null (_vDisp v) -> maybe (_vPath v) id (safeLast $ T.splitOn "/" (_vPath v))
    | otherwise -> _vDisp v
  where safeLast xs = if null xs then Nothing else Just (last xs)

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
                      VTbl "" 0 0 1

-- | Rebuild with new TblOps, preserving old view attributes
rebuildView :: View -> TblOps -> Int -> Vector Text -> Int -> Maybe View
rebuildView old ops col grp row = case fromTbl ops (_vPath old) col grp row of
  Nothing -> Nothing
  Just v -> Just $ v { _vNav = (_vNav v) { _nsHidden = _nsHidden (_vNav old)
                                          , _nsVkind = _nsVkind (_vNav old)
                                          , _nsSearch = _nsSearch (_vNav old)
                                          , _nsPrecAdj = _nsPrecAdj (_vNav old)
                                          , _nsWidthAdj = _nsWidthAdj (_vNav old)
                                          , _nsHeatMode = _nsHeatMode (_vNav old) }
                     , _vDisp = _vDisp old, _vSameHide = _vSameHide old }

-- | Pure update by command
updateView :: View -> Cmd -> Int -> Maybe (View, Effect)
updateView v cmd rowPg =
  let ns = _vNav v
      curCol = curColIdx ns
      names = _tblColNames (_nsTbl ns)
      nr = _tblNRows (_nsTbl ns)
  in case cmd of
    SortAsc  -> Just (v, ESort curCol (selColIdxs' ns) (grpIdxs ns) True)
    SortDesc -> Just (v, ESort curCol (selColIdxs' ns) (grpIdxs ns) False)
    ColExclude ->
      let name = curColName ns
          cols = if V.null (_nsHidden ns) then V.singleton name
                 else if V.elem name (_nsHidden ns) then _nsHidden ns
                 else V.snoc (_nsHidden ns) name
      in Just (v, EExclude (V.map (names V.!) cols'))
      where cols' = V.empty  -- TODO: map to indices properly
    _ -> case execNav' cmd rowPg ns of
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
    -- inline nav exec to avoid circular import
    execNav' cmd' pg ns' = execNavInline cmd' pg ns'

-- TODO: move to Nav to avoid duplication
execNavInline :: Cmd -> Int -> NavState -> Maybe NavState
execNavInline cmd rowPg ns =
  let nr = _tblNRows (_nsTbl ns)
      nc = V.length (_tblColNames (_nsTbl ns))
      moveRow d = Just $ ns { _nsRow = axisMove nr d (_nsRow ns) }
      moveCol d = Just $ ns { _nsCol = axisMove nc d (_nsCol ns) }
  in case cmd of
    RowInc  -> moveRow 1;        RowDec  -> moveRow (-1)
    RowPgdn -> moveRow rowPg;    RowPgup -> moveRow (-rowPg)
    RowTop  -> moveRow (negate $ _naCur (_nsRow ns))
    RowBot  -> moveRow (nr - 1 - _naCur (_nsRow ns))
    ColInc  -> moveCol 1;        ColDec  -> moveCol (-1)
    ColFirst -> moveCol (negate $ _naCur (_nsCol ns))
    ColLast  -> moveCol (nc - 1 - _naCur (_nsCol ns))
    RowSel  -> Just $ ns { _nsRow = (_nsRow ns) { _naSels = vToggle (_naCur $ _nsRow ns) (_naSels $ _nsRow ns) } }
    ColGrp  ->
      let g' = vToggleT (curColName ns) (_nsGrp ns)
          names = _tblColNames (_nsTbl ns)
      in Just $ ns { _nsGrp = g', _nsDispIdxs = dispOrder g' names }
    ColHide -> Just $ ns { _nsHidden = vToggleT (curColName ns) (_nsHidden ns) }
    _ -> Nothing
  where
    vToggleT x xs = if V.elem x xs then V.filter (/= x) xs else V.snoc xs x

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
