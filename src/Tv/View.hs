{-
  View: wraps NavState + metadata for table type t
  Generic over t to support different build variants (Core, DuckDB, Full).

  Literal port of Tc/Tc/View.lean — same record fields, same function names,
  same order, same comments. Refactor only after parity.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.View
  ( View(..)
  , new
  , curDir
  , tabName
  , doRender
  , fromTbl
  , rebuild
  , update
  , ViewStack(..)
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
  , opsStr
  ) where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V

import Optics.TH (makeFieldLabelsNoPrefix)
import qualified Tv.Data.DuckDB.Prql as Prql
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)
import Tv.Nav (NavState)
import qualified Tv.Nav as Nav
import Tv.Render (ViewState)
import qualified Tv.Render as Render
import Tv.Types (Cmd(..), Effect(..), ViewKind(..))

-- $setup
-- >>> :set -XOverloadedStrings
-- >>> import qualified Data.Text as Text
-- >>> import qualified Data.Vector as V
-- >>> import qualified Tv.Nav as Nav
-- >>> import Tv.Types (Cmd(..), ColType(..), Effect(..), ViewKind(..))
-- >>> data MockTable = MockTable { mockRows :: Int, mockNames :: V.Vector Text.Text }
-- >>> let mockNames53 = V.fromList ["c0","c1","c2"]
-- >>> let mockTypes53 = V.fromList [ColTypeStr, ColTypeStr, ColTypeStr]
-- >>> let mock53 = MockTable { mockRows = 5, mockNames = mockNames53 }
-- >>> let testNav = Nav.new 5 5 mockNames53 mockTypes53 mock53
-- >>> let testView = new testNav "data/test.csv"
-- >>> let testStack = ViewStack { hd = testView, tl = [] }

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
makeFieldLabelsNoPrefix ''View

-- | Create from NavState + path
new :: NavState t -> Text -> View t
new nav_ path_ = View
  { nRows    = nav_ ^. #tblRows
  , nCols    = V.length (nav_ ^. #tblNames)
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
curDir v = case v ^. #vkind of
  VkFld dir _ -> dir
  _           -> "."

-- | Tab display name: custom disp or filename from path
--
-- >>> tabName (new testNav "data/sample.parquet")
-- "sample.parquet"
-- >>> tabName ((new testNav "/home/user/Tc") { vkind = VkFld "/home/user/Tc" 1, path = "/home/user/Tc" })
-- "/home/user/Tc"
-- >>> tabName (testView { vkind = VkColMeta, disp = "meta" })
-- "meta"
-- >>> tabName (testView { vkind = VkFreqV (V.singleton "c0") 5, disp = "freq" })
-- "freq"
tabName :: View t -> Text
tabName v = case v ^. #vkind of
  VkFld p _ ->
    if T.null (v ^. #disp) || T.isPrefixOf "/" p then p else v ^. #disp
  _ ->
    if T.null (v ^. #disp)
      then case reverse (T.splitOn "/" $ v ^. #path) of
             (x:_) -> x
             []    -> v ^. #path
      else v ^. #disp

-- | Render the view, returns (ViewState, updated View with new widths)
doRender
  :: View AdbcTable -> ViewState -> Vector Word32
  -> Word8 -> Vector Text
  -> IO (ViewState, View AdbcTable)
doRender v vs styles heatMode sparklines = do
  let names = v ^. #nav % #tblNames
  let extraHidden = V.mapMaybe (Nav.idxOf names) (v ^. #sameHide)
  -- VkCorr is useless without a color gradient: force numeric heat
  -- regardless of the AppState toggle.
  let heatMode' = if (v ^. #vkind) == VkCorr then 1 else heatMode
  (vs', widths_) <-
    Render.render (v ^. #nav) vs (v ^. #widths) styles (v ^. #prec) (v ^. #widthAdj)
      (v ^. #vkind) heatMode' sparklines extraHidden
  pure (vs', v & #widths .~ widths_)

-- | Create View from AdbcTable + path (returns Nothing if empty).
-- Extracts nRows/totalRows/colNames/colTypes from the table.
fromTbl
  :: AdbcTable -> Text -> Int -> Vector Text -> Int -> Maybe (View AdbcTable)
fromTbl tbl_ path_ col_ grp_ row_ =
  let names  = Table.colNames tbl_
      types  = Table.colTypes tbl_
      nRows_ = Table.nRows tbl_
      total  = Table.totalRows tbl_
      nCols_ = V.length names
  in if nCols_ > 0 && nRows_ > 0
       then Just (new (Nav.newAt nRows_ total names types tbl_ col_ grp_ row_) path_)
       else Nothing

-- | Rebuild view with new table, preserving all attributes from old view.
-- Only nRows/nCols/nav change; everything else (vkind, disp, prec, etc.) is kept.
rebuild
  :: View AdbcTable -> AdbcTable -> Int -> Vector Text -> Int -> Maybe (View AdbcTable)
rebuild old tbl_ col_ grp_ row_ =
  let names  = Table.colNames tbl_
      types  = Table.colTypes tbl_
      nRows_ = Table.nRows tbl_
      total  = Table.totalRows tbl_
      nCols_ = V.length names
  in if nCols_ > 0 && nRows_ > 0
       then
         let nav0 = Nav.newAt nRows_ total names types tbl_ col_ grp_ row_
             nav1 = nav0 & #hidden .~ (old ^. #nav % #hidden)
         in Just (old & #nRows .~ nRows_ & #nCols .~ nCols_ & #nav .~ nav1 & #widths .~ V.empty)
       else Nothing

-- | Pure update by command
--
-- >>> fmap snd (update testView CmdSortAsc 1)
-- Just (EffectSort 0 [] [] True)
-- >>> fmap snd (update testView CmdSortDesc 1)
-- Just (EffectSort 0 [] [] False)
-- >>> fmap (Nav.cur . Nav.row . nav . fst) (update testView CmdRowInc 1)
-- Just 1
-- >>> fmap snd (update testView CmdRowInc 1)
-- Just EffectNone
-- >>> fmap (Nav.cur . Nav.row . nav . fst) (update testView CmdRowDec 1)
-- Just 0
update :: View t -> Cmd -> Int -> Maybe (View t, Effect)
update v h rowPg =
  case h of
    CmdSortAsc  -> sortEff True
    CmdSortDesc -> sortEff False
    CmdColExclude ->
      let name = Nav.colName n
          hid  = n ^. #hidden
          cols =
            if V.null hid then V.singleton name
            else if V.elem name hid then hid
            else V.snoc hid name
      in Just (v, EffectExclude cols)
    _ -> case Nav.exec h n rowPg of
           Nothing -> Nothing
           Just nav' ->
             let needsMore = nav' ^. #row % #cur + 1 >= v ^. #nRows
                           && n ^. #tblTotal > v ^. #nRows
                           && (h == CmdRowInc || h == CmdRowPgdn || h == CmdRowBot)
             in Just (v & #nav .~ nav', if needsMore then EffectFetchMore else EffectNone)
  where
    n       = v ^. #nav
    names   = Nav.colNames n
    curCol  = Nav.colIdx n
    sortEff asc =
      let selIdxs = V.mapMaybe (Nav.idxOf names) $ n ^. #col % #sels
          grpIdxs = V.mapMaybe (Nav.idxOf names) $ n ^. #grp
      in Just (v, EffectSort curCol selIdxs grpIdxs asc)

{-! ## ViewStack: non-empty view stack -/-}

-- | Non-empty view stack: hd = current, tl = parents (List for O(1) push/pop)
data ViewStack t = ViewStack
  { hd :: View t
  , tl :: [View t]
  }
makeFieldLabelsNoPrefix ''ViewStack

cur :: ViewStack t -> View t
cur s = s ^. #hd

tbl :: ViewStack t -> t
tbl s = s ^. #hd % #nav % #tbl

hasParent :: ViewStack t -> Bool
hasParent s = not (null (s ^. #tl))

setCur :: ViewStack t -> View t -> ViewStack t
setCur s v = s & #hd .~ v

push :: ViewStack t -> View t -> ViewStack t
push s v = ViewStack { hd = v, tl = s ^. #hd : s ^. #tl }

pop :: ViewStack t -> Maybe (ViewStack t)
pop s = case s ^. #tl of
  h0 : t0 -> Just (ViewStack { hd = h0, tl = t0 })
  []      -> Nothing

swap :: ViewStack t -> ViewStack t
swap s = case s ^. #tl of
  h0 : t0 -> ViewStack { hd = h0, tl = s ^. #hd : t0 }
  []      -> s

dup :: ViewStack t -> ViewStack t
dup s = ViewStack { hd = s ^. #hd, tl = s ^. #hd : s ^. #tl }

tabNames :: ViewStack t -> Vector Text
tabNames s = V.fromList $ map tabName $ s ^. #hd : s ^. #tl

-- | Pure update by command. q on empty stack -> quit
--
-- >>> fmap (path . hd . fst) (updateStack testStack CmdStkSwap)
-- Just "data/test.csv"
-- >>> fmap snd (updateStack testStack CmdStkSwap)
-- Just EffectNone
-- >>> fmap (length . tl . fst) (updateStack testStack CmdStkDup)
-- Just 1
-- >>> fmap snd (updateStack testStack CmdStkDup)
-- Just EffectNone
-- >>> fmap snd (updateStack testStack CmdStkPop)
-- Just EffectQuit
-- >>> let twoStack = dup testStack in fmap (length . tl . fst) (updateStack twoStack CmdStkPop)
-- Just 0
-- >>> let twoStack = dup testStack in fmap snd (updateStack twoStack CmdStkPop)
-- Just EffectNone
-- >>> Data.Maybe.isNothing (updateStack testStack CmdRowInc)
-- True
updateStack :: ViewStack t -> Cmd -> Maybe (ViewStack t, Effect)
updateStack s h = case h of
  CmdStkDup  -> Just (dup s, EffectNone)
  CmdStkPop  -> case pop s of
    Just s' -> Just (s', EffectNone)
    Nothing -> Just (s, EffectQuit)
  CmdStkSwap -> Just (swap s, EffectNone)
  _          -> Nothing

-- | PRQL pipeline ops string from view's query (for tab line display).
opsStr :: View AdbcTable -> Text
opsStr v = Prql.renderOps $ Table.query $ Nav.tbl $ nav v
