-- | Handler type + shared combinators over 'AppEff'. Feature modules
-- call these to plug command handlers into the dispatch table.
module Tv.Handler
  ( Handler
  , cont
  , refresh
  , setMsg
  , withNav
  , pushOps
  , pushOpsAt
  , curOps
  , refreshGrid
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.Core ((^.), (%), (&), (.~))

import Tv.Types
import Tv.View (fromTbl, vNav, vsPush)
import Tv.Render
  ( asStack, asGrid, asVisRow0, asVisCol0, asVisColN, asVisH, asVisW
  , asMsg, headNav, headTbl, maxColW, minColW )
import Tv.Eff (Eff, AppEff, get, use, (.=), (%=), liftIO)

-- | Handler: @arg → Eff AppEff Bool@. True = continue, False = halt.
type Handler = Text -> Eff AppEff Bool

-- | Continue signal (True).
cont :: Eff AppEff Bool
cont = pure True

-- | Refresh the grid and continue.
refresh :: Eff AppEff Bool
refresh = refreshGrid >> cont

-- | Set the status message and continue (no grid refresh).
setMsg :: Text -> Eff AppEff Bool
setMsg t = asMsg .= t >> cont

-- | Apply a NavState transform to the head view.
withNav :: (NavState -> Maybe NavState) -> Handler
withNav f _ = do
  ns <- use headNav
  case f ns of
    Nothing  -> cont
    Just ns' -> headNav .= ns' >> refresh

-- | Push a new View (start col 0, no grp) onto the stack. Sets the
-- view kind and refreshes. Emits "empty result" on 'fromTbl' failure.
pushOps :: Text -> ViewKind -> TblOps -> Eff AppEff Bool
pushOps path = pushOpsAt path 0 V.empty "empty result"

-- | Like 'pushOps' but with explicit @startCol@, @grp@, and error
-- message. Used by freq/split/exclude/freq-filter which need a
-- non-default start column or carry grouping through.
pushOpsAt :: Text -> Int -> Vector Text -> Text -> ViewKind -> TblOps -> Eff AppEff Bool
pushOpsAt path startCol grp emptyMsg vk ops = case fromTbl ops path startCol grp 0 of
  Nothing -> setMsg emptyMsg
  Just v  -> asStack %= vsPush (v & vNav % nsVkind .~ vk) >> refresh

-- | Read the top TblOps from state.
curOps :: Eff AppEff TblOps
curOps = use headTbl

-- | Rebuild asGrid for the visible window. Walks the TblOps tblCellStr
-- IO function for each cell. Called after any handler that could change
-- the viewport or the underlying table — the single refresh path keeps
-- drawApp pure and lets it do zero IO.
--
-- Mirrors Lean's Tc/c/render.c: compute widths for ALL columns across
-- the visible row range first, THEN pick which columns fit in visW.
-- Doing it in this order means the viewport decision uses the same
-- data-only widths that Render.visColWidths reports, so the cursor
-- never scrolls the screen while the rightmost visible col is still
-- actually visible.
refreshGrid :: Eff AppEff ()
refreshGrid = do
  st <- get
  let ns   = st ^. headNav
      tbl  = ns ^. nsTbl
      disp = ns ^. nsDispIdxs
      nr   = tbl ^. tblNRows
      nc   = V.length disp
      curR = ns ^. nsRow % naCur
      curC = ns ^. nsCol % naCur
      visH = max 1 (st ^. asVisH)
      visW = max 1 (st ^. asVisW)
      adjOff cur off page = clamp 0 (max 1 (cur + 1)) (max (cur + 1 - page) (min off cur))
      r0 = adjOff curR (st ^. asVisRow0) visH
      h = min visH (max 0 (nr - r0))
  -- Fetch cells for ALL columns in the visible row range. For wide
  -- tables this is O(nc * h), matching Lean's compute_data_width
  -- which also scans the visible rows for every column.
  fullGrid <- liftIO $ V.generateM h $ \ri ->
                V.generateM nc $ \dispCi ->
                  (tbl ^. tblCellStr) (r0 + ri) (disp V.! dispCi)
  let -- Actual data-only width for col at dispIdxs position dispCi.
      -- max over visible cells, floor at 1, then max(minColW, .) + 2.
      widthAt dispCi =
        let cellLen r = if dispCi < V.length r then T.length (r V.! dispCi) else 0
            dw = V.foldl' (\a r -> max a (cellLen r)) 1 fullGrid
        in min maxColW (max minColW dw + 2)
      slotW i = widthAt i + 1  -- +1 for column separator
      nVisFit c = go c 0
        where
          go i used
            | i >= nc = i - c
            | used + slotW i > visW && i > c = i - c
            | otherwise = go (i + 1) (used + slotW i)
      shrinkLeft lastI =
        let go i used
              | i <= 0 = 0
              | used + slotW (i - 1) > visW = i
              | otherwise = go (i - 1) (used + slotW (i - 1))
        in go lastI (slotW lastI)
      prevC0 = st ^. asVisCol0
      prevC0Clamped = clamp 0 (max 1 nc) prevC0
      lastVisAt c = c + max 0 (nVisFit c - 1)
      newC0
        | nc == 0          = 0
        | curC < prevC0Clamped = curC
        | curC > lastVisAt prevC0Clamped = shrinkLeft curC
        | otherwise        = prevC0Clamped
      visColN = nVisFit newC0
      -- Slice each row to the visible column range.
      grid = V.map (\row -> V.slice newC0 (min visColN (V.length row - newC0)) row) fullGrid
  asGrid    .= grid
  asVisRow0 .= r0
  asVisCol0 .= newC0
  asVisColN .= visColN
