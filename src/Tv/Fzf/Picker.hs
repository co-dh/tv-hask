{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
-- | In-process fuzzy picker, rendered as a popup overlay on the existing
-- TUI cell buffer — not a separate full-screen mode.
--
-- UX replicated from the fzf subset we used to shell out to:
--
-- * prompt line              — editable with typing / backspace / Ctrl-U
-- * optional header line     — static, shown above the list
-- * fuzzy-filtered item list — incremental, case-insensitive, ranked,
--                              with @^prefix@ / @suffix$@ anchors
-- * arrow-key navigation     — up/down, Home/End, PgUp/PgDn, Ctrl-J/Ctrl-K
-- * Enter selects            — returns the raw line of the highlighted item,
--                              or (if 'printQuery') the current query when
--                              nothing matches
-- * Esc cancels              — returns ""
-- * 'onFocus' callback       — fires on every highlight change, replacing
--                              fzf's @--bind=focus:execute-silent(...)@ +
--                              socat shim
-- * 'poll' callback          — fires every ~30 ms while we wait for input
--
-- Rendering: we draw the popup into 'Tv.Term''s screen cell buffer via
-- 'Term.padC' and flush with 'Term.present'. The caller keeps 'Term.init'
-- active, so the popup sits on top of whatever the TUI was showing. On
-- exit we clear the buffer so the caller's next render repaints the
-- underlying view cleanly.
module Tv.Fzf.Picker
  ( PickerOpts(..)
  , defaultOpts
  , runPicker
  ) where

import Control.Monad (when, forM_)
import Data.Char (chr)
import qualified Data.IntSet as IS
import Data.List (sortOn)
import Data.Ord (Down(..))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word32)

import Tv.Fzf.Match (match)
import qualified Tv.Term as Term

-- | Options for one picker invocation.
data PickerOpts = PickerOpts
  { prompt     :: Text
  , header     :: Text
  , items      :: Vector Text
  , printQuery :: Bool
    -- ^ if True and Enter is pressed with no item highlighted, return
    -- the current query text (fzf's @--print-query@).
  , withNth    :: Bool
    -- ^ if True, display only the fields after the first tab (fzf's
    -- @--with-nth=2.. --delimiter=\\t@); selection still returns the
    -- full line.
  , onFocus    :: Int -> Text -> IO ()
  , poll       :: IO ()
  , initial    :: Text
  }

defaultOpts :: PickerOpts
defaultOpts = PickerOpts
  { prompt = "> ", header = "", items = V.empty
  , printQuery = False, withNth = False
  , onFocus = \_ _ -> pure (), poll = pure ()
  , initial = ""
  }

data PickerState = PickerState
  { psQuery  :: !Text
  , psCur    :: !Int
  , psMatch  :: !(Vector (Int, Int, [Int]))
  , psBox    :: !Box
  }

-- | Popup geometry on screen (pixel-ish cell coordinates).
data Box = Box { bx, by, bw, bh :: !Int } deriving Show

runPicker :: PickerOpts -> IO Text
runPicker opts = do
  sw <- fromIntegral <$> Term.width
  sh <- fromIntegral <$> Term.height
  let its     = items opts
      promptW = T.length (prompt opts)
      hdrW    = T.length (header opts)
      dispW   = V.foldl' (\m l -> max m (T.length (display (withNth opts) l))) 0 its
      innerW  = maximum [promptW + 20, hdrW, dispW + 2]
      boxW    = min sw (max 50 (innerW + 4))
      hasHdr  = not (T.null (header opts))
      rowsN   = V.length its
      listH   = min 12 (max 1 rowsN)
      boxH    = listH + 1 + (if hasHdr then 1 else 0)
      boxX    = max 0 ((sw - boxW) `div` 2)
      boxY    = max 0 ((sh - boxH) `div` 2)
      box     = Box { bx = boxX, by = boxY, bw = boxW, bh = boxH }
      ms0     = computeMatches (initial opts) its
      st0     = PickerState { psQuery = initial opts, psCur = 0
                            , psMatch = ms0, psBox = box }
  drawFrame opts st0
  case ms0 V.!? 0 of
    Just (i, _, _) -> onFocus opts i (its V.! i)
    Nothing        -> pure ()
  result <- loop opts st0
  -- Clear the back buffer and invalidate the front so the caller's next
  -- render repaints every cell contiguously — otherwise cells the popup
  -- happened not to touch stay "same" and the new frame emits as a
  -- cursor-jumped patch (breaks text-match demo verification and leaves
  -- visible residue if the underlying view's content line-wraps).
  Term.clear
  Term.invalidate
  pure result

data Key
  = KChar Char | KEnter | KEsc | KBackspace
  | KUp | KDown | KHome | KEnd | KPgUp | KPgDn
  | KCtrlU | KCtrlK | KCtrlJ
  | KIgnore
  deriving (Eq, Show)

-- | Translate a 'Term.Event' into the picker's 'Key' enum.
toKey :: Term.Event -> Key
toKey ev
  | Term.typ ev /= Term.eventKey     = KIgnore
  | Term.keyCode ev == Term.keyArrowUp    = KUp
  | Term.keyCode ev == Term.keyArrowDown  = KDown
  | Term.keyCode ev == Term.keyEnter      = KEnter
  | Term.keyCode ev == Term.keyEsc        = KEsc
  | Term.keyCode ev == Term.keyBackspace  = KBackspace
  | Term.keyCode ev == Term.keyBackspace2 = KBackspace
  | Term.keyCode ev == Term.keyHome       = KHome
  | Term.keyCode ev == Term.keyEnd        = KEnd
  | Term.keyCode ev == Term.keyPageUp     = KPgUp
  | Term.keyCode ev == Term.keyPageDown   = KPgDn
  | Term.ch ev == Term.ctrlU              = KCtrlU
  | Term.ch ev == 0x0B                    = KCtrlK
  | Term.ch ev == 0x0A                    = KCtrlJ
  | Term.ch ev == 0x0D                    = KEnter
  | Term.ch ev /= 0                       = KChar (chr (fromIntegral (Term.ch ev)))
  | otherwise                             = KIgnore

-- | Read-key loop. Polls for input every 30 ms, firing the caller's poll
-- callback in between so background work keeps ticking.
loop :: PickerOpts -> PickerState -> IO Text
loop opts st = do
  mev <- Term.waitEventTimeout 30
  case mev of
    Nothing -> poll opts *> loop opts st
    Just ev -> case toKey ev of
      KEsc       -> pure ""
      KEnter     -> pure (selection opts st)
      KChar c    -> typed opts st (psQuery st <> T.singleton c) >>= loop opts
      KBackspace -> typed opts st (dropLast (psQuery st))       >>= loop opts
      KCtrlU     -> typed opts st ""                            >>= loop opts
      KUp        -> moveCur opts st (-1)                        >>= loop opts
      KCtrlK     -> moveCur opts st (-1)                        >>= loop opts
      KDown      -> moveCur opts st 1                           >>= loop opts
      KCtrlJ     -> moveCur opts st 1                           >>= loop opts
      KPgUp      -> moveCur opts st (negate (listRows st))      >>= loop opts
      KPgDn      -> moveCur opts st (listRows st)               >>= loop opts
      KHome      -> setCur opts st 0                            >>= loop opts
      KEnd       -> setCur opts st (V.length (psMatch st) - 1)  >>= loop opts
      KIgnore    -> loop opts st

dropLast :: Text -> Text
dropLast t = if T.null t then t else T.init t

-- | Handle a query change: re-filter, reset highlight, redraw, fire
-- onFocus only if the top match changed so a no-op query update doesn't
-- re-trigger the caller's live preview.
typed :: PickerOpts -> PickerState -> Text -> IO PickerState
typed opts st q' = do
  let ms'     = computeMatches q' (items opts)
      st'     = st { psQuery = q', psMatch = ms', psCur = 0 }
      prevTop = (\(i,_,_) -> i) <$> (psMatch st V.!? psCur st)
      newTop  = (\(i,_,_) -> i) <$> (ms' V.!? 0)
  drawFrame opts st'
  when (prevTop /= newTop) (fireFocus opts st')
  pure st'

moveCur :: PickerOpts -> PickerState -> Int -> IO PickerState
moveCur opts st d = setCur opts st (psCur st + d)

setCur :: PickerOpts -> PickerState -> Int -> IO PickerState
setCur opts st i = do
  let n   = V.length (psMatch st)
      i'  = if n == 0 then 0 else max 0 (min (n - 1) i)
      st' = st { psCur = i' }
  drawFrame opts st'
  when (psCur st /= i') (fireFocus opts st')
  pure st'

fireFocus :: PickerOpts -> PickerState -> IO ()
fireFocus opts st = case psMatch st V.!? psCur st of
  Just (i, _, _) -> onFocus opts i (items opts V.! i)
  Nothing        -> pure ()

selection :: PickerOpts -> PickerState -> Text
selection opts st = case psMatch st V.!? psCur st of
  Just (i, _, _) -> items opts V.! i
  Nothing
    | printQuery opts -> psQuery st
    | otherwise       -> ""

-- | Re-run fuzzy filter. Empty query → all items in original order.
computeMatches :: Text -> Vector Text -> Vector (Int, Int, [Int])
computeMatches q its
  | T.null q  = V.generate (V.length its) (\i -> (i, 0, []))
  | otherwise =
      let scoreOne i line = (\(s, ps) -> (i, s, ps)) <$> match q line
      in V.fromList $ sortOn (\(_, s, _) -> Down s)
                    $ V.toList $ V.imapMaybe scoreOne its

-- Rendering --------------------------------------------------------------

listRows :: PickerState -> Int
listRows st = bh (psBox st) - 1 - (if True then 0 else 0)

-- | Strip the index field for display when 'withNth' is True.
display :: Bool -> Text -> Text
display True line = case T.splitOn "\t" line of
  _ : rest -> T.intercalate "\t" rest
  _        -> line
display False line = line

-- | Draw every cell of the popup into the cell buffer and flush.
-- Colours are hardcoded (not theme-driven) so the popup looks consistent
-- across themes and so this module can stay independent of 'Tv.Theme'
-- (Theme imports Fzf, so a Theme import here would create a cycle).
drawFrame :: PickerOpts -> PickerState -> IO ()
drawFrame opts st = do
  let box      = psBox st
      hasHdr   = not (T.null (header opts))
      rows     = bh box - 1 - (if hasHdr then 1 else 0)
      promptLn = prompt opts <> psQuery st
      scroll   = scrollOff (psCur st) rows
  drawRow (bx box) (by box) (bw box) promptLn fgPrompt bgPrompt
  when hasHdr $
    Term.padC (fromIntegral (bx box)) (fromIntegral (by box + 1))
              (fromIntegral (bw box)) fgHdr bgHdr
              (padR (bw box) (" " <> header opts)) 0
  let rowStart = by box + 1 + (if hasHdr then 1 else 0)
  forM_ [0 .. rows - 1] $ \r -> do
    let y = rowStart + r
    case psMatch st V.!? (scroll + r) of
      Nothing ->
        Term.padC (fromIntegral (bx box)) (fromIntegral y)
                  (fromIntegral (bw box)) fgRow bgRow
                  (T.replicate (bw box) " ") 0
      Just (idx, _, poses) -> do
        let selected = (scroll + r) == psCur st
            raw      = items opts V.! idx
            shown    = display (withNth opts) raw
            adj      = if withNth opts then adjustPositions raw poses else poses
            bgLine   = if selected then bgSel else bgRow
            fgLine   = if selected then fgSel else fgRow
            marker   = if selected then "> " else "  "
        drawItem (bx box) y (bw box) marker shown adj
                 fgLine bgLine fgMatch bgLine
  Term.present
  where
    drawRow :: Int -> Int -> Int -> Text -> Word32 -> Word32 -> IO ()
    drawRow x y w full fg bg = do
      let padded = padR w (" " <> full)
      Term.padC (fromIntegral x) (fromIntegral y) (fromIntegral w) fg bg padded 0

-- Hardcoded palette indices for the popup.
-- 236/8 = dark grey on bright white prompt; 244/234 = dim header;
-- 7/236 = light text on dark rows; 16/11 = black on yellow selection;
-- 11 = bright yellow match hit (foreground only).
fgPrompt, bgPrompt, fgHdr, bgHdr, fgRow, bgRow, fgSel, bgSel, fgMatch :: Word32
fgPrompt = 16;  bgPrompt = 11
fgHdr    = 244; bgHdr    = 234
fgRow    = 7;   bgRow    = 236
fgSel    = 16;  bgSel    = 14
fgMatch  = 11

-- | Right-pad a Text to exactly @n@ characters; truncate if longer.
padR :: Int -> Text -> Text
padR n t
  | T.length t >= n = T.take n t
  | otherwise       = t <> T.replicate (n - T.length t) " "

-- | Keep the highlighted row within the visible window.
scrollOff :: Int -> Int -> Int
scrollOff cur listRows_
  | listRows_ <= 0      = 0
  | cur < listRows_     = 0
  | otherwise           = cur - listRows_ + 1

-- | When display hides the index-prefix column, shift match positions.
adjustPositions :: Text -> [Int] -> [Int]
adjustPositions raw poses = case T.findIndex (== '\t') raw of
  Nothing     -> poses
  Just tabIdx ->
    let cut = tabIdx + 1
    in [ p - cut | p <- poses, p >= cut ]

-- | Draw one row with match-position highlighting. The row is always
-- exactly @width@ cells wide so the background colour fills edge-to-edge.
drawItem
  :: Int -> Int -> Int
  -> Text     -- ^ marker (two chars)
  -> Text     -- ^ shown text
  -> [Int]    -- ^ match positions in shown text
  -> Word32 -> Word32   -- ^ row fg/bg (unselected/selected)
  -> Word32 -> Word32   -- ^ hit fg/bg
  -> IO ()
drawItem x y width marker shown poses fgRow bgRow fgHit bgHit = do
  let hits     = IS.fromList poses
      markLen  = T.length marker
      bodyMax  = width - markLen
      shownT   = padR bodyMax shown
  Term.padC (fromIntegral x) (fromIntegral y) (fromIntegral markLen)
            fgRow bgRow marker 0
  -- Emit per character: cheap for our row widths (≤120).
  forM_ [0 .. bodyMax - 1] $ \i -> do
    let c       = if i < T.length shown then T.index shownT i else ' '
        isHit   = IS.member i hits && i < T.length shown
        fg      = if isHit then fgHit else fgRow
        bg      = if isHit then bgHit else bgRow
    Term.padC (fromIntegral (x + markLen + i)) (fromIntegral y) 1
              fg bg (T.singleton c) 0
