{-
  UI overlays: Info (context-specific key hints) and Preview (full cell
  content with word-wrap). Merged from Tv.UI.Info + Tv.UI.Preview — both
  were short single-renderer modules under the same UI/ subdir.

  Literal port of Tc/Tc/UI/Info.lean and Tc/Tc/UI/Preview.lean. The Lean
  Info `State` struct was a single `vis : Bool`; here we use the Bool
  directly. `render`/`update` were renamed to `infoRender`/`infoUpdate` /
  `prevRender` to disambiguate the merged exports.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.UI where

import Prelude hiding (print)
import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.Types (Cmd(..), ViewKind(..))
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme

-- ---------------------------------------------------------------------------
-- Info overlay
-- ---------------------------------------------------------------------------

-- | Pure update by Cmd
infoUpdate :: Bool -> Cmd -> Maybe Bool
infoUpdate vis CmdInfoTog = Just (not vis)
infoUpdate _   _          = Nothing

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
viewHints VkCorr = V.fromList [("q", "back")]

-- | Pad string s on the left with spaces to width w
padLeft :: Int -> Text -> Text
padLeft w s = T.replicate (max 0 $ w - T.length s) " " <> s

-- | Pad string s on the right with spaces to width w (after truncating to w)
padRight :: Int -> Text -> Text
padRight w s =
  let t = T.take w s
  in t <> T.replicate (max 0 $ w - min (T.length s) w) " "

-- | Render info overlay at bottom-right
infoRender :: Int -> Int -> ViewKind -> IO ()
infoRender screenH screenW vk = do
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

-- ---------------------------------------------------------------------------
-- Preview box
-- ---------------------------------------------------------------------------

-- | Word-wrap a string to fit within maxW characters
wrapText :: Text -> Int -> Vector Text
wrapText s maxW
  | maxW == 0 = V.singleton s
  | otherwise =
      let (lns, cur) = T.foldl' step ([], T.empty) s
          step (acc, c) ch
            | ch == '\n'          = (acc ++ [c], T.empty)
            | T.length c >= maxW  = (acc ++ [c], T.singleton ch)
            | otherwise           = (acc, T.snoc c ch)
          final = if not (T.null cur) || null lns then lns ++ [cur] else lns
      in V.fromList final

-- | Render preview box at bottom-left. Width scales with the screen
-- (70% up to screenW-4 safety margin) so long cell strings wrap less
-- and the box no longer looks cramped on wide terminals.
prevRender :: Int -> Int -> Text -> Int -> IO ()
prevRender screenH screenW text scroll0 = do
  let maxW = min (screenW - 4) (screenW * 7 `div` 10)
  if maxW < 4 then pure ()
  else do
    let lns = wrapText text maxW
        nLines = V.length lns
        maxVisible = max 1 (screenH - 6)
        visible = min nLines maxVisible
        maxScroll = if nLines > visible then nLines - visible else 0
        scroll = min scroll0 maxScroll
        contentW = V.foldl' (\mx l -> max mx $ T.length l) 0 lns
        innerW0 = min contentW maxW
        innerW = max innerW0 4  -- minimum inner width
        -- bottom-left: x0=0, box ends at screenH - 3 (above tab + status lines)
        x0 :: Int
        x0 = 0
        y1 = screenH - 3                  -- last row of box (bottom border)
        y0 = if y1 + 1 > visible + 2 then y1 - visible - 1 else 0  -- top border
        actualVisible = y1 - y0 - 1       -- rows between borders
    -- Fixed high-contrast colours: bright white on a dark grey panel
    -- (much more readable than the prior white-on-blue sBar style).
    let fg  = 15  :: Word32  -- bright white
        bg  = 236 :: Word32  -- dark grey panel
        dfg = 244 :: Word32  -- dim grey for bottom border + scroll indicator
        dbg = bg
    -- top border
    Term.print (fromIntegral x0 :: Word32) (fromIntegral y0 :: Word32) fg bg
      ("┌" <> T.replicate innerW "─" <> "┐")
    -- content lines with side borders
    mapM_ (\i -> do
        let line = fromMaybe T.empty $ lns V.!? (scroll + i)
            trimmed = if T.length line > innerW then T.take innerW line else line
            padded = trimmed <> T.replicate (innerW - T.length trimmed) " "
        Term.print (fromIntegral x0 :: Word32) (fromIntegral (y0 + 1 + i) :: Word32) fg bg
          ("│" <> padded <> "│")
      ) [0 .. actualVisible - 1]
    -- bottom border with scroll indicator
    let indicator =
          if nLines > actualVisible
            then " " <> T.pack (show (scroll + 1)) <> "-"
                 <> T.pack (show (scroll + actualVisible)) <> "/"
                 <> T.pack (show nLines) <> " " <> "{/}"
            else ""
        dashW = if innerW > T.length indicator then innerW - T.length indicator else 0
        border = "└" <> T.replicate dashW "─" <> indicator <> "┘"
    Term.print (fromIntegral x0 :: Word32) (fromIntegral y1 :: Word32) dfg dbg border
