-- | View: wraps NavState + metadata. ViewStack: non-empty stack of views.
module Tv.View where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.TH (makeLenses)
import Tv.Nav (execNav)
import Tv.Types
import Optics.Core ((^.), (%), (&), (.~), (%~))

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
tabName v = case v ^. vNav % nsVkind of
  VFld p _ | T.null (v ^. vDisp) || "/" `T.isPrefixOf` p -> p
  _ | T.null (v ^. vDisp) -> last (T.splitOn "/" (v ^. vPath))
    | otherwise -> v ^. vDisp

-- | Build View from TblOps + path (returns Nothing if empty)
fromTbl :: TblOps -> Text -> Int -> Vector Text -> Int -> Maybe View
fromTbl ops path col grp row
  | nc <= 0 || nr <= 0 = Nothing
  | otherwise = Just $ mkView ns path
  where
    nr = ops ^. tblNRows
    nc = V.length (ops ^. tblColNames)
    ns = NavState ops (mkAxis & naCur .~ clamp 0 nr row)
                      (mkAxis & naCur .~ clamp 0 nc col)
                      grp V.empty (dispOrder grp (ops ^. tblColNames))
                      VTbl "" 0 0 0

-- | Rebuild with new TblOps, preserving old view attributes
rebuildView :: View -> TblOps -> Int -> Vector Text -> Int -> Maybe View
rebuildView old ops col grp row = inherit <$> fromTbl ops (old ^. vPath) col grp row
  where
    inherit v = v & vNav % nsHidden   .~ old ^. vNav % nsHidden
                  & vNav % nsVkind    .~ old ^. vNav % nsVkind
                  & vNav % nsSearch   .~ old ^. vNav % nsSearch
                  & vNav % nsPrecAdj  .~ old ^. vNav % nsPrecAdj
                  & vNav % nsWidthAdj .~ old ^. vNav % nsWidthAdj
                  & vNav % nsHeatMode .~ old ^. vNav % nsHeatMode
                  & vDisp     .~ old ^. vDisp
                  & vSameHide .~ old ^. vSameHide

-- | Pure update by command
updateView :: View -> Cmd -> Int -> Maybe (View, Effect)
updateView v cmd rowPg =
  let ns     = v ^. vNav
      curCol = curColIdx ns
      nr     = ns ^. nsTbl % tblNRows
  in case cmd of
    SortAsc  -> Just (v, ESort curCol (selColIdxs' ns) (grpIdxs ns) True)
    SortDesc -> Just (v, ESort curCol (selColIdxs' ns) (grpIdxs ns) False)
    _ -> case execNav cmd rowPg ns of
      Nothing -> Nothing
      Just ns' ->
        let needsMore = ns' ^. nsRow % naCur + 1 >= nr
              && ns ^. nsTbl % tblTotalRows > nr
              && cmd `elem` [RowInc, RowPgdn, RowBot]
        in Just (v & vNav .~ ns', if needsMore then EFetchMore else ENone)
  where
    selColIdxs' ns = V.mapMaybe (`V.elemIndex` (ns ^. nsTbl % tblColNames))
                      (V.map (\i -> (ns ^. nsTbl % tblColNames) V.! ((ns ^. nsDispIdxs) V.! i))
                             (ns ^. nsCol % naSels))
    grpIdxs ns = V.mapMaybe (`V.elemIndex` (ns ^. nsTbl % tblColNames)) (ns ^. nsGrp)

-- ============================================================================
-- ViewStack: non-empty view stack
-- ============================================================================

data ViewStack = ViewStack
  { _vsHd :: !View
  , _vsTl :: ![View]
  } deriving (Show)

makeLenses ''ViewStack

-- | Placeholder Eq: View contains TblOps (record of functions) so a real
-- structural Eq is impossible. This one compares only vPath / vDisp so
-- assertions like @?= Nothing in PureSpec can type-check; equality of two
-- live Views/ViewStacks is not semantically meaningful.
instance Eq View where
  a == b = (a ^. vPath) == (b ^. vPath) && (a ^. vDisp) == (b ^. vDisp)
instance Eq ViewStack where
  a == b = (a ^. vsHd) == (b ^. vsHd) && length (a ^. vsTl) == length (b ^. vsTl)

vsCur :: ViewStack -> View
vsCur vs = vs ^. vsHd

vsHasParent :: ViewStack -> Bool
vsHasParent vs = not (null (vs ^. vsTl))

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
