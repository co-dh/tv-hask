-- | Preview box: shows full cell content when text is truncated.
-- Word-wraps long text; caller handles {/} paging by adjusting scroll.
-- The Lean version used termbox2 directly; here we produce a Brick Widget.
module Tv.UI.Preview (wrapText, render) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Brick.Widgets.Core as C
import qualified Brick
import Tv.Render (Name, attrBar, attrBarDim)

-- | Word-wrap a string to fit within maxW characters.
-- Breaks on character boundary (not word boundary) to match Lean.
wrapText :: Int -> Text -> [Text]
wrapText maxW s
  | maxW <= 0 = [s]
  | otherwise = go (T.unpack s) [] []
  where
    go [] cur ls = reverse (finish cur : ls)
    go ('\n':cs) cur ls = go cs [] (finish cur : ls)
    go (c:cs) cur ls
      | length cur >= maxW = go cs [c] (finish cur : ls)
      | otherwise = go cs (cur ++ [c]) ls
    finish = T.pack . reverse . reverse  -- cur is kept in order, just pack it

-- | Render preview box at bottom-left. Takes terminal dimensions,
-- cell text, and scroll offset. Produces a bordered box with bar/barDim
-- styling and a scroll indicator on the bottom border.
render :: Int -> Int -> Text -> Int -> Brick.Widget Name
render screenH screenW txt scroll =
  let maxW = min 60 (screenW - 4)
  in if maxW < 4 then C.emptyWidget
     else let
    ls = wrapText maxW txt
    nLines = length ls
    maxVis = max 1 (screenH - 6)
    visible = min nLines maxVis
    maxScroll = if nLines > visible then nLines - visible else 0
    sc = min scroll maxScroll
    contentW = foldl (\mx l -> max mx (T.length l)) 0 ls
    innerW = max 4 (min contentW maxW)  -- minimum inner width = 4
    visLines = take visible (drop sc ls)
    -- format a content line with side borders and padding
    mkLine l =
      let trimmed = T.take innerW l
          padded = trimmed <> T.replicate (innerW - T.length trimmed) " "
      in C.withAttr attrBar (C.txt ("\x2502" <> padded <> "\x2502"))
    -- scroll indicator on bottom border
    indicator
      | nLines > visible =
          " " <> tshow (sc + 1) <> "-" <> tshow (sc + visible)
          <> "/" <> tshow nLines <> " {/}"
      | otherwise = ""
    dashW = max 0 (innerW - T.length indicator)
    topBorder = C.withAttr attrBar
      (C.txt ("\x250C" <> T.replicate innerW "\x2500" <> "\x2510"))
    botBorder = C.withAttr attrBarDim
      (C.txt ("\x2514" <> T.replicate dashW "\x2500" <> indicator <> "\x2518"))
    box = C.vBox ([topBorder] ++ map mkLine visLines ++ [botBorder])
    in C.hLimit (innerW + 2) $ C.padLeft (Brick.Pad 0) box
  where tshow = T.pack . show
