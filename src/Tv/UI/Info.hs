-- | Info overlay: context-specific key hints per view kind.
-- Rendered as a bottom-right-aligned block of (key, description) pairs.
-- The Lean version used termbox2 directly; here we produce a Brick Widget.
module Tv.UI.Info (viewHints, render) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Brick.Widgets.Core as C
import qualified Brick
import Tv.Types (ViewKind(..))
import Tv.Render (Name, attrHint)

-- | Context-specific key hints per view kind (no common nav keys).
viewHints :: ViewKind -> [(Text, Text)]
viewHints VColMeta =
  [("M0", "select nulls"), ("M1", "select unique"), ("\x23CE", "set as key"), ("q", "back")]
viewHints (VFreq _ _) = [("\x23CE", "filter by val"), ("q", "back")]
viewHints (VFld _ _) =
  [("\x23CE", "open"), ("D-", "trash"), ("D<", "less depth"), ("D>", "more depth")]
viewHints VTbl =
  [ ("T", "toggle row sel"), ("!", "group by"), ("c\\", "hide column")
  , ("S-\x2190\x2192", "reorder cols"), ("SPC", "command menu") ]

-- | Render info overlay. Produces a right-aligned block of hint rows,
-- styled with attrHint. Caller places this in the layout (e.g. above
-- the status line).
render :: ViewKind -> Brick.Widget Name
render vk =
  let hints = viewHints vk
      keyW = 5; hintW = 14
      fmtRow (k, d) =
        let kpad = T.justifyRight keyW ' ' k
            dpad = T.justifyLeft hintW ' ' (T.take hintW d)
        in kpad <> " " <> dpad
      rows = map (C.withAttr attrHint . C.txt . fmtRow) hints
  in C.hLimit (keyW + 1 + hintW) $ C.padLeft Brick.Max $ C.vBox rows
