{-
  Info overlay: context-specific key hints per view kind.

  Literal port of Tc/Tc/UI/Info.lean — same State, viewHints, render.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.UI.Info
  ( State(..)
  , update
  , viewHints
  , render
  ) where

import Prelude hiding (print)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word32)

import Tv.Types (Cmd(..), ViewKind(..))
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme

-- | Info state
data State = State
  { vis :: Bool
  }

-- | Pure update by Cmd
update :: State -> Cmd -> Maybe State
update s CmdInfoTog = Just s { vis = not (vis s) }
update _ _          = Nothing

-- | Context-specific key hints per view (no common navigation)
viewHints :: ViewKind -> Vector (Text, Text)
viewHints VkColMeta =
  V.fromList
    [ ("M0", "select nulls")
    , ("M1", "select unique")
    , ("\x23ce", "set as key")
    , ("q", "back")
    ]
viewHints (VkFreqV _ _) =
  V.fromList
    [ ("\x23ce", "filter by val")
    , ("q", "back")
    ]
viewHints (VkFld _ _) =
  V.fromList
    [ ("\x23ce", "open")
    , ("D-", "trash")
    , ("D<", "less depth")
    , ("D>", "more depth")
    ]
viewHints VkTbl =
  V.fromList
    [ ("T", "toggle row sel")
    , ("!", "group by")
    , ("c\\", "hide column")
    , ("S-\x2190\x2192", "reorder cols")
    , ("SPC", "command menu")
    ]

-- | Pad string s on the left with spaces to width w
padLeft :: Int -> Text -> Text
padLeft w s = T.replicate (max 0 (w - T.length s)) " " <> s

-- | Pad string s on the right with spaces to width w (after truncating to w)
padRight :: Int -> Text -> Text
padRight w s =
  let t = T.take w s
  in t <> T.replicate (max 0 (w - min (T.length s) w)) " "

-- | Render info overlay at bottom-right
render :: Int -> Int -> ViewKind -> IO ()
render screenH screenW vk = do
  let hints = viewHints vk
  let nRows = V.length hints
  let keyW = 5 :: Int
  let hintW = 14 :: Int
  let boxW = keyW + 1 + hintW
  let x0 = screenW - boxW - 2
  let y0 = screenH - nRows - 3
  V.iforM_ hints $ \i (k, d) -> do
    let kpad = padLeft keyW k
    let dpad = padRight hintW d
    s <- Theme.getStyles
    Term.print
      (fromIntegral x0 :: Word32)
      (fromIntegral (y0 + i) :: Word32)
      (Theme.styleFg s Theme.sHint)
      (Theme.styleBg s Theme.sHint)
      (kpad <> " " <> dpad)
