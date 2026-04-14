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
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.Core ((^.), (%), (&), (.~))

import Tv.Types
import Tv.View (fromTbl, vNav, vsPush)
import Tv.Render (asStack, asGrid, asVisRow0, asVisCol0, asVisH, asVisW, asMsg, headNav, headTbl)
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
      nr   = tbl ^. tblNRows
      nc   = V.length disp
      curR = ns ^. nsRow % naCur
      curC = ns ^. nsCol % naCur
      visH = max 1 (st ^. asVisH)
      visW = max 1 (st ^. asVisW)
      adjOff cur off page = clamp 0 (max 1 (cur + 1)) (max (cur + 1 - page) (min off cur))
      r0 = adjOff curR (st ^. asVisRow0) visH
      c0 = adjOff curC (st ^. asVisCol0) visW
      h = min visH (max 0 (nr - r0))
      w = min visW (max 0 (nc - c0))
  grid <- liftIO $ V.generateM h $ \ri ->
            V.generateM w $ \ci ->
              (tbl ^. tblCellStr) (r0 + ri) (disp V.! (c0 + ci))
  asGrid    .= grid
  asVisRow0 .= r0
  asVisCol0 .= c0
