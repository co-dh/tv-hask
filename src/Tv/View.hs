{-
  View: wraps NavState + metadata for table type t
  Generic over t to support different build variants (Core, DuckDB, Full).

  Literal port of Tc/Tc/View.lean — same record fields, same function names,
  same order, same comments. Refactor only after parity.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.View
  ( View(..)
  , pathL
  , vkindL
  , dispL
  , precL
  , widthAdjL
  , widthsL
  , searchL
  , sameHideL
  , new
  , curDir
  , tabName
  , doRender
  , fromTbl
  , rebuild
  , update
  , ViewStack(..)
  , hdL
  , cur
  , tbl
  , hasParent
  , setCur
  , push
  , pop
  , swap
  , dup
  , tabNames
  , updateStack
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8, Word32)

import Tv.Lens (Lens'(..))
import Tv.Nav (NavState)
import qualified Tv.Nav as Nav
import Tv.Render (ViewState)
import qualified Tv.Render as Render
import Tv.Types
  ( Cmd(..)
  , Effect(..)
  , TblOps
  , ViewKind(..)
  )
import qualified Tv.Types as TblOps

-- | View: wraps NavState for table type t
data View t = View
  { nRows    :: Int
  , nCols    :: Int
  , nav      :: NavState t
  , path     :: Text                   -- source file/command (for tab display)
  , vkind    :: ViewKind               -- default VkTbl
  , disp     :: Text                   -- custom display name (overrides filename)
  , prec     :: Int                    -- float decimal count (0-17)
  , widthAdj :: Int                    -- width adjustment offset (-=narrower, +=wider)
  , widths   :: Vector Int             -- cached column widths (per-view for type safety)
  , search   :: Maybe (Int, Text)      -- last search: (colIdx, value)
  , sameHide :: Vector Text            -- diff: columns with identical values (hidden separately from user hide)
  }

-- | Field lenses for non-dependent View fields, auto-generated in Lean via `gen_lenses`.
-- (nRows/nCols/nav are skipped — their types are mutually dependent, so they can't
-- be expressed as simple Lens' (View t) a — the codomain would depend on the source.)
pathL :: Lens' (View t) Text
pathL = Lens' { get = path, set = \a s -> s { path = a } }

vkindL :: Lens' (View t) ViewKind
vkindL = Lens' { get = vkind, set = \a s -> s { vkind = a } }

dispL :: Lens' (View t) Text
dispL = Lens' { get = disp, set = \a s -> s { disp = a } }

precL :: Lens' (View t) Int
precL = Lens' { get = prec, set = \a s -> s { prec = a } }

widthAdjL :: Lens' (View t) Int
widthAdjL = Lens' { get = widthAdj, set = \a s -> s { widthAdj = a } }

widthsL :: Lens' (View t) (Vector Int)
widthsL = Lens' { get = widths, set = \a s -> s { widths = a } }

searchL :: Lens' (View t) (Maybe (Int, Text))
searchL = Lens' { get = search, set = \a s -> s { search = a } }

sameHideL :: Lens' (View t) (Vector Text)
sameHideL = Lens' { get = sameHide, set = \a s -> s { sameHide = a } }

-- | Create from NavState + path
new :: TblOps t => NavState t -> Text -> View t
new nav_ path_ = View
  { nRows    = TblOps.nRows (Nav.tbl nav_)
  , nCols    = V.length (TblOps.colNames (Nav.tbl nav_))
  , nav      = nav_
  , path     = path_
  , vkind    = VkTbl
  , disp     = ""
  , prec     = 3
  , widthAdj = 0
  , widths   = V.empty
  , search   = Nothing
  , sameHide = V.empty
  }

-- | Current folder directory (or "." for non-folder views)
curDir :: View t -> Text
curDir v = case vkind v of
  VkFld dir _ -> dir
  _           -> "."

-- | Tab display name: custom disp or filename from path
tabName :: View t -> Text
tabName v = case vkind v of
  VkFld p _ ->
    if T.null (disp v) || T.isPrefixOf "/" p then p else disp v
  _ ->
    if T.null (disp v)
      then case reverse (T.splitOn "/" (path v)) of
             (x:_) -> x
             []    -> path v
      else disp v

-- | Render the view, returns (ViewState, updated View with new widths)
doRender
  :: TblOps t
  => View t -> ViewState -> Vector Word32
  -> Word8 -> Vector Text
  -> IO (ViewState, View t)
doRender v vs styles heatMode sparklines = do
  let names = TblOps.colNames (Nav.tbl (nav v))
  let extraHidden = V.mapMaybe (Nav.idxOf names) (sameHide v)
  (vs', widths_) <-
    Render.render (nav v) vs (widths v) styles (prec v) (widthAdj v)
      (vkind v) heatMode sparklines extraHidden
  pure (vs', v { widths = widths_ })

-- | Create View from table + path (returns Nothing if empty)
fromTbl
  :: TblOps t
  => t -> Text -> Int -> Vector Text -> Int -> Maybe (View t)
fromTbl tbl_ path_ col_ grp_ row_ =
  let nCols_ = V.length (TblOps.colNames tbl_)
      nRows_ = TblOps.nRows tbl_
  in if nCols_ > 0 && nRows_ > 0
       then Just (new (Nav.newAt tbl_ col_ grp_ row_) path_)
       else Nothing

-- | Rebuild view with new table, preserving all attributes from old view.
-- Only nRows/nCols/nav change; everything else (vkind, disp, prec, etc.) is kept.
rebuild
  :: TblOps t
  => View t -> t -> Int -> Vector Text -> Int -> Maybe (View t)
rebuild old tbl_ col_ grp_ row_ =
  let nCols_ = V.length (TblOps.colNames tbl_)
      nRows_ = TblOps.nRows tbl_
  in if nCols_ > 0 && nRows_ > 0
       then
         let nav0 = Nav.newAt tbl_ col_ grp_ row_
             nav1 = set Nav.hiddenL (Nav.hidden (nav old)) nav0
         in Just old { nRows = nRows_, nCols = nCols_, nav = nav1, widths = V.empty }
       else Nothing

-- | Pure update by command
update :: TblOps t => View t -> Cmd -> Int -> Maybe (View t, Effect)
update v h rowPg =
  case h of
    CmdSortAsc  -> sortEff True
    CmdSortDesc -> sortEff False
    CmdColExclude ->
      let name = Nav.curColName n
          hid  = Nav.hidden n
          cols =
            if V.null hid then V.singleton name
            else if V.elem name hid then hid
            else V.snoc hid name
      in Just (v, EffectExclude cols)
    _ -> case Nav.exec h n rowPg of
           Nothing -> Nothing
           Just nav' ->
             let needsMore = Nav.cur (Nav.row nav') + 1 >= nRows v
                           && TblOps.totalRows (Nav.tbl n) > nRows v
                           && (h == CmdRowInc || h == CmdRowPgdn || h == CmdRowBot)
             in Just (v { nav = nav' }, if needsMore then EffectFetchMore else EffectNone)
  where
    n       = nav v
    names   = Nav.colNames n
    curCol  = Nav.curColIdx n
    sortEff asc =
      let selIdxs = V.mapMaybe (Nav.idxOf names) (Nav.sels (Nav.col n))
          grpIdxs = V.mapMaybe (Nav.idxOf names) (Nav.grp n)
      in Just (v, EffectSort curCol selIdxs grpIdxs asc)

{-! ## ViewStack: non-empty view stack -/-}

-- | Non-empty view stack: hd = current, tl = parents (List for O(1) push/pop)
data ViewStack t = ViewStack
  { hd :: View t
  , tl :: [View t]
  }

-- | Field lens for the current (head) view on the stack.
hdL :: Lens' (ViewStack t) (View t)
hdL = Lens' { get = hd, set = \a s -> s { hd = a } }

cur :: ViewStack t -> View t
cur s = hd s

tbl :: ViewStack t -> t
tbl s = Nav.tbl (nav (hd s))

hasParent :: ViewStack t -> Bool
hasParent s = not (null (tl s))

setCur :: ViewStack t -> View t -> ViewStack t
setCur s v = s { hd = v }

push :: ViewStack t -> View t -> ViewStack t
push s v = ViewStack { hd = v, tl = hd s : tl s }

pop :: ViewStack t -> Maybe (ViewStack t)
pop s = case tl s of
  h0 : t0 -> Just (ViewStack { hd = h0, tl = t0 })
  []      -> Nothing

swap :: ViewStack t -> ViewStack t
swap s = case tl s of
  h0 : t0 -> ViewStack { hd = h0, tl = hd s : t0 }
  []      -> s

dup :: ViewStack t -> ViewStack t
dup s = ViewStack { hd = hd s, tl = hd s : tl s }

tabNames :: ViewStack t -> Vector Text
tabNames s = V.fromList (map tabName (hd s : tl s))

-- | Pure update by command. q on empty stack -> quit
updateStack :: ViewStack t -> Cmd -> Maybe (ViewStack t, Effect)
updateStack s h = case h of
  CmdStkDup  -> Just (dup s, EffectNone)
  CmdStkPop  -> case pop s of
    Just s' -> Just (s', EffectNone)
    Nothing -> Just (s, EffectQuit)
  CmdStkSwap -> Just (swap s, EffectNone)
  _          -> Nothing
