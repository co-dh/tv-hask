{-
  Preview box: shows full cell content when text is truncated.
  Word-wraps long text, supports {/} for page up/down.

  Literal port of Tc/Tc/UI/Preview.lean.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.UI.Preview
  ( wrapText
  , render
  ) where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified Tv.Term as Term

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
render :: Int -> Int -> Text -> Int -> IO ()
render screenH screenW text scroll0 = do
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
