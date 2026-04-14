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
refreshGrid :: Eff AppEff ()
refreshGrid = do
  st <- get
  let ns   = st ^. headNav
      tbl  = ns ^. nsTbl
      disp = ns ^. nsDispIdxs
      names = tbl ^. tblColNames
      nr   = tbl ^. tblNRows
      nc   = V.length disp
      curR = ns ^. nsRow % naCur
      curC = ns ^. nsCol % naCur
      visH = max 1 (st ^. asVisH)
      visW = max 1 (st ^. asVisW)
      -- Estimate per-column display slot width from the header name only.
      -- Matches visColWidths bounds: max(minColW, len) + 2 (padding) + 1 (sep),
      -- capped at maxColW + 3.
      slotW i =
        let n = T.length (names V.! (disp V.! i))
            w = max minColW n + 2 + 1
        in min (maxColW + 3) w
      -- nVisFit: how many columns fit in visW chars starting at c.
      nVisFit c = go c 0
        where
          go i used
            | i >= nc = i - c
            | used + slotW i > visW && i > c = i - c
            | otherwise = go (i + 1) (used + slotW i)
      -- Walk left from `last` until adding one more column would overflow
      -- visW; the resulting index is the leftmost c0 keeping `last` visible.
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
      adjOff cur off page = clamp 0 (max 1 (cur + 1)) (max (cur + 1 - page) (min off cur))
      r0 = adjOff curR (st ^. asVisRow0) visH
      h = min visH (max 0 (nr - r0))
  grid <- liftIO $ V.generateM h $ \ri ->
            V.generateM visColN $ \ci ->
              (tbl ^. tblCellStr) (r0 + ri) (disp V.! (newC0 + ci))
  asGrid    .= grid
  asVisRow0 .= r0
  asVisCol0 .= newC0
  asVisColN .= visColN
