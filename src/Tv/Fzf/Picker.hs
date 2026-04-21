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
module Tv.Fzf.Picker where

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
  -- Width = 70% of screen, clamped to a reasonable range. Height grows
  -- with item count up to 12 list rows. The border takes 2 rows + 2 cols.
  let its     = items opts
      hasHdr  = not (T.null (header opts))
      boxW    = max 40 (min (sw - 2) ((sw * 7) `div` 10))
      rowsN   = V.length its
      listH   = min 12 (max 1 rowsN)
      boxH    = listH + 3 + (if hasHdr then 1 else 0)
                -- 3 = top border + prompt + bottom border
      boxX    = max 0 ((sw - boxW) `div` 2)
      boxY    = max 0 ((sh - boxH) `div` 2)
      box     = Box { bx = boxX, by = boxY, bw = boxW, bh = boxH }
      ms0     = computeMatches (initial opts) its
      st0     = PickerState { psQuery = initial opts, psCur = 0
                            , psMatch = ms0, psBox = box }
  drawFrame opts st0
  -- fireFocus instead of inlining: it does the redraw-after-callback
  -- dance so the initial popup stays visible if onFocus repaints the
  -- underlying view.
  fireFocus opts st0
  result <- loop opts st0
  -- Before handing control back, paint blank default-style cells into
  -- the popup's footprint and flush. That erases the popup on screen
  -- without touching cells outside the box, and without emitting a raw
  -- \x1b[2J (which gen_demo flags as a post-table clear anomaly). The
  -- caller's next render then repaints normally; invalidate ensures
  -- cells outside the popup area also re-emit to pick up whatever
  -- changed in the underlying view.
  erasePopup (psBox st0)
  Term.present
  Term.clear
  Term.invalidate
  pure result

-- | Overwrite the popup area with blank default-style cells.
erasePopup :: Box -> IO ()
erasePopup b =
  forM_ [0 .. bh b - 1] $ \dy ->
    Term.padC (fromIntegral (bx b)) (fromIntegral (by b + dy))
              (fromIntegral (bw b)) 0 0 (T.replicate (bw b) " ") 0

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

-- | Run the caller's onFocus callback then *redraw the popup*. The
-- callback often re-renders the underlying view (live row-search moves
-- the cursor and repaints the table; theme preview repaints with the
-- previewed style) which clears the popup's cells. Without this redraw,
-- pressing @/@ in a folder view makes the popup vanish on first focus
-- event.
fireFocus :: PickerOpts -> PickerState -> IO ()
fireFocus opts st = do
  case psMatch st V.!? psCur st of
    Just (i, _, _) -> onFocus opts i (items opts V.! i)
    Nothing        -> pure ()
  drawFrame opts st

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
listRows st = bh (psBox st) - 3  -- minus top border, prompt, bottom border

-- | Strip the index field for display when 'withNth' is True.
display :: Bool -> Text -> Text
display True line = case T.splitOn "\t" line of
  _ : rest -> T.intercalate "\t" rest
  _        -> line
display False line = line

-- | Draw the whole popup — border, prompt, optional header, item rows —
-- into the cell buffer and flush. Colours are hardcoded (not theme-driven)
-- so the popup looks consistent across themes and so this module can stay
-- independent of 'Tv.Theme' (Theme imports Fzf, which would create a
-- module cycle).
--
-- Layout:
--   row 0                 top border      ┌────────────┐
--   row 1                 prompt          │> query     │
--   row 2 (optional)      header          │header      │
--   row 2+ … bh-2         item rows       │> item …    │
--   row bh-1              bottom border   └────────────┘
drawFrame :: PickerOpts -> PickerState -> IO ()
drawFrame opts st = do
  let box      = psBox st
      hasHdr   = not (T.null (header opts))
      innerW   = bw box - 2
      promptY  = by box + 1
      hdrY     = by box + 2
      firstRow = by box + 2 + (if hasHdr then 1 else 0)
      nRows    = bh box - 3 - (if hasHdr then 1 else 0)
      scroll   = scrollOff (psCur st) nRows
      promptLn = prompt opts <> psQuery st
  drawBorder (bx box) (by box) (bw box) (bh box)
  drawLine (bx box) promptY innerW promptLn fgPrompt bgPanel
  when hasHdr $
    drawLine (bx box) hdrY innerW (header opts) fgHdr bgPanel
  forM_ [0 .. nRows - 1] $ \r -> do
    let y = firstRow + r
    case psMatch st V.!? (scroll + r) of
      Nothing ->
        drawLine (bx box) y innerW "" fgRow bgPanel
      Just (idx, _, poses) -> do
        let selected = (scroll + r) == psCur st
            raw      = items opts V.! idx
            shown    = display (withNth opts) raw
            adj      = if withNth opts then adjustPositions raw poses else poses
            bgLine   = if selected then bgSel else bgPanel
            fgLine   = if selected then fgSel else fgRow
            marker   = if selected then "> " else "  "
        drawItem (bx box) y innerW marker shown adj
                 fgLine bgLine fgMatch bgLine
  Term.present

-- | Render the outer frame.
drawBorder :: Int -> Int -> Int -> Int -> IO ()
drawBorder x y w h = do
  let innerW = w - 2
      top    = "┌" <> T.replicate innerW "─" <> "┐"
      bot    = "└" <> T.replicate innerW "─" <> "┘"
  Term.padC (fromIntegral x) (fromIntegral y)
            (fromIntegral w) fgBorder bgPanel top 0
  Term.padC (fromIntegral x) (fromIntegral (y + h - 1))
            (fromIntegral w) fgBorder bgPanel bot 0
  -- side bars on each inner row
  forM_ [1 .. h - 2] $ \dy -> do
    Term.padC (fromIntegral x) (fromIntegral (y + dy)) 1
              fgBorder bgPanel "│" 0
    Term.padC (fromIntegral (x + w - 1)) (fromIntegral (y + dy)) 1
              fgBorder bgPanel "│" 0

-- | Draw a text row inside the frame (between the two side bars).
drawLine :: Int -> Int -> Int -> Text -> Word32 -> Word32 -> IO ()
drawLine x y innerW t fg bg = do
  let padded = " " <> padR (innerW - 1) t
  Term.padC (fromIntegral (x + 1)) (fromIntegral y) (fromIntegral innerW)
            fg bg padded 0

-- Hardcoded palette indices for the popup. White/grey on a dark panel so
-- it reads cleanly over any theme without ever using white-on-blue.
fgBorder, bgPanel, fgPrompt, fgHdr, fgRow, fgSel, bgSel, fgMatch :: Word32
bgPanel  = 236  -- dark grey panel background
fgBorder = 244  -- medium grey border
fgPrompt = 15   -- bright white prompt
fgHdr    = 244  -- dim header
fgRow    = 7    -- light grey rows
bgSel    = 240  -- slightly lighter panel for selected row
fgSel    = 15   -- bright white selected fg
fgMatch  = 11   -- bright yellow match-position highlight

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

-- | Draw one item row inside the popup frame. Writes into the inner
-- region starting at column (x + 1) so it lives between the border bars.
drawItem
  :: Int -> Int -> Int
  -> Text     -- ^ marker (two chars)
  -> Text     -- ^ shown text
  -> [Int]    -- ^ match positions in shown text
  -> Word32 -> Word32   -- ^ row fg/bg
  -> Word32 -> Word32   -- ^ match-hit fg/bg
  -> IO ()
drawItem xBox y innerW marker shown poses fgRow' bgRow' fgHit bgHit = do
  let hits    = IS.fromList poses
      markLen = T.length marker
      bodyMax = innerW - markLen - 1  -- 1 cell right-margin inside border
      shownT  = padR bodyMax shown
      xInner  = xBox + 1
  -- left gutter (before marker)
  Term.padC (fromIntegral xInner) (fromIntegral y) 1 fgRow' bgRow' " " 0
  -- marker
  Term.padC (fromIntegral (xInner + 1)) (fromIntegral y) (fromIntegral markLen)
            fgRow' bgRow' marker 0
  -- body char-by-char (cheap: row width ≤ ~100)
  forM_ [0 .. bodyMax - 1] $ \i -> do
    let c     = if i < T.length shown then T.index shownT i else ' '
        isHit = IS.member i hits && i < T.length shown
        fg    = if isHit then fgHit else fgRow'
        bg    = if isHit then bgHit else bgRow'
    Term.padC (fromIntegral (xInner + 1 + markLen + i)) (fromIntegral y) 1
              fg bg (T.singleton c) 0
