{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TemplateHaskell #-}
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

import Tv.Prelude
import Data.Char (chr)
import qualified Data.IntSet as IS
import Data.List (sortOn)
import Data.Ord (Down(..))
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.Fzf.Match (matchMulti)
import qualified Tv.Term as Term
import {-# SOURCE #-} qualified Tv.Theme as Theme
import Optics.TH (makeFieldLabelsNoPrefix)

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
makeFieldLabelsNoPrefix ''PickerOpts

defaultOpts :: PickerOpts
defaultOpts = PickerOpts
  { prompt = "> ", header = "", items = V.empty
  , printQuery = False, withNth = False
  , onFocus = \_ _ -> pure (), poll = pure ()
  , initial = ""
  }

-- | Popup geometry on screen (pixel-ish cell coordinates).
data Box = Box { bx, by, bw, bh :: !Int } deriving Show
makeFieldLabelsNoPrefix ''Box

data PickerState = PickerState
  { psQuery  :: !Text
  , psCur    :: !Int
  , psMatch  :: !(Vector (Int, Int, [Int]))
  , psBox    :: !Box
  }
makeFieldLabelsNoPrefix ''PickerState

-- | Palette snapshot for one frame — read once from 'Theme.stylesRef' so a
-- mid-frame theme change can't split one popup across two colour schemes.
data Palette = Palette
  { fgBorder :: !Word32
  , bgPanel  :: !Word32
  , fgPrompt :: !Word32
  , fgHdr    :: !Word32
  , fgRow    :: !Word32
  , fgSel    :: !Word32
  , bgSel    :: !Word32
  , fgMatch  :: !Word32
  }
makeFieldLabelsNoPrefix ''Palette

runPicker :: PickerOpts -> IO Text
runPicker opts@PickerOpts{items, header, initial} = do
  sw <- fromIntegral <$> Term.width
  sh <- fromIntegral <$> Term.height
  -- Width = 70% of screen, clamped to a reasonable range. Height grows
  -- with item count up to 12 list rows. The border takes 2 rows + 2 cols.
  let its     = items
      hasHdr  = not (T.null header)
      boxW    = max 40 (min (sw - 2) ((sw * 7) `div` 10))
      rowsN   = V.length its
      listH   = min 12 (max 1 rowsN)
      boxH    = listH + 3 + (if hasHdr then 1 else 0)
                -- 3 = top border + prompt + bottom border (count is
                -- embedded in the bottom border, no separate row).
      boxX    = max 0 ((sw - boxW) `div` 2)
      boxY    = max 0 ((sh - boxH) `div` 2)
      box     = Box { bx = boxX, by = boxY, bw = boxW, bh = boxH }
      ms0     = computeMatches initial its
      st0     = PickerState { psQuery = initial, psCur = 0
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
  erasePopup (st0 ^. #psBox)
  Term.present
  Term.clear
  Term.invalidate
  pure result

-- | Overwrite the popup area with blank default-style cells.
erasePopup :: Box -> IO ()
erasePopup Box{bx, by, bw, bh} =
  forM_ [0 .. bh - 1] $ \dy ->
    Term.padC (fromIntegral bx) (fromIntegral (by + dy))
              (fromIntegral bw) 0 0 (T.replicate bw " ") 0

readPalette :: IO Palette
readPalette = do
  sty <- Theme.getStyles
  let panelFg = Theme.styleFg sty Theme.sPickerPanel
      panelBg = Theme.styleBg sty Theme.sPickerPanel
      selFg   = Theme.styleFg sty Theme.sPickerSel
      selBg   = Theme.styleBg sty Theme.sPickerSel
      matchFg = Theme.styleFg sty Theme.sPickerMatch
  pure Palette
    { fgBorder = panelFg, bgPanel = panelBg
    , fgPrompt = Term.parseColor "brYellow"
    , fgHdr    = panelFg
    , fgRow    = panelFg
    , fgSel    = selFg,   bgSel   = selBg
    , fgMatch  = matchFg
    }

data Key
  = KChar Char | KEnter | KEsc | KBackspace | KTab
  | KUp | KDown | KHome | KEnd | KPgUp | KPgDn
  | KCtrlU | KCtrlK | KCtrlJ
  | KIgnore
  deriving (Eq, Show)

-- | Translate a 'Term.Event' into the picker's 'Key' enum.
toKey :: Term.Event -> Key
toKey Term.Event{Term.typ, Term.keyCode, Term.ch}
  | typ /= Term.eventKey          = KIgnore
  | keyCode == Term.keyArrowUp    = KUp
  | keyCode == Term.keyArrowDown  = KDown
  | keyCode == Term.keyEnter      = KEnter
  | keyCode == Term.keyEsc        = KEsc
  | keyCode == Term.keyBackspace  = KBackspace
  | keyCode == Term.keyBackspace2 = KBackspace
  | keyCode == Term.keyHome       = KHome
  | keyCode == Term.keyEnd        = KEnd
  | keyCode == Term.keyPageUp     = KPgUp
  | keyCode == Term.keyPageDown   = KPgDn
  | ch == Term.ctrlU              = KCtrlU
  | ch == 0x0B                    = KCtrlK
  | ch == 0x0A                    = KCtrlJ
  | ch == 0x0D                    = KEnter
  | ch == Term.ctrlI              = KTab
  | ch /= 0                       = KChar (chr (fromIntegral ch))
  | otherwise                     = KIgnore

-- | Read-key loop. Polls for input every 30 ms, firing the caller's poll
-- callback in between so background work keeps ticking.
loop :: PickerOpts -> PickerState -> IO Text
loop opts st = do
  mev <- Term.waitEventTimeout 30
  case mev of
    Nothing -> (opts ^. #poll) *> loop opts st
    Just ev -> case toKey ev of
      KEsc       -> pure ""
      KEnter     -> pure (selection opts st)
      KChar c    -> typed opts st (st ^. #psQuery <> T.singleton c) >>= loop opts
      KBackspace -> typed opts st (dropLast (st ^. #psQuery))       >>= loop opts
      KCtrlU     -> typed opts st ""                                >>= loop opts
      KTab       -> typed opts st (currentDisplay opts st)          >>= loop opts
      KUp        -> moveCur opts st (-1)                            >>= loop opts
      KCtrlK     -> moveCur opts st (-1)                            >>= loop opts
      KDown      -> moveCur opts st 1                               >>= loop opts
      KCtrlJ     -> moveCur opts st 1                               >>= loop opts
      KPgUp      -> moveCur opts st (negate (listRows st))          >>= loop opts
      KPgDn      -> moveCur opts st (listRows st)                   >>= loop opts
      KHome      -> setCur opts st 0                                >>= loop opts
      KEnd       -> setCur opts st (V.length (st ^. #psMatch) - 1)  >>= loop opts
      KIgnore    -> loop opts st

dropLast :: Text -> Text
dropLast t = if T.null t then t else T.init t

-- | Handle a query change: re-filter, reset highlight, redraw, fire
-- onFocus only if the top match changed so a no-op query update doesn't
-- re-trigger the caller's live preview. Skip the whole pipeline if the
-- query didn't change (Tab on a row whose text already matches the query).
typed :: PickerOpts -> PickerState -> Text -> IO PickerState
typed opts st q'
  | q' == st ^. #psQuery = pure st
  | otherwise = do
      let ms'     = computeMatches q' (opts ^. #items)
          st'     = st { psQuery = q', psMatch = ms', psCur = 0 }
          prevTop = (\(i,_,_) -> i) <$> ((st ^. #psMatch) V.!? (st ^. #psCur))
          newTop  = (\(i,_,_) -> i) <$> (ms' V.!? 0)
      drawFrame opts st'
      when (prevTop /= newTop) (fireFocus opts st')
      pure st'

moveCur :: PickerOpts -> PickerState -> Int -> IO PickerState
moveCur opts st d = setCur opts st (st ^. #psCur + d)

setCur :: PickerOpts -> PickerState -> Int -> IO PickerState
setCur opts st i = do
  let n   = V.length (st ^. #psMatch)
      i'  = if n == 0 then 0 else max 0 (min (n - 1) i)
      st' = st { psCur = i' }
  drawFrame opts st'
  when (st ^. #psCur /= i') (fireFocus opts st')
  pure st'

-- | Run the caller's onFocus callback then *redraw the popup*. The
-- callback often re-renders the underlying view (live row-search moves
-- the cursor and repaints the table; theme preview repaints with the
-- previewed style) which clears the popup's cells. Without this redraw,
-- pressing @/@ in a folder view makes the popup vanish on first focus
-- event.
fireFocus :: PickerOpts -> PickerState -> IO ()
fireFocus opts st = do
  maybe (pure ()) (\(i, _, _) -> (opts ^. #onFocus) i ((opts ^. #items) V.! i))
        ((st ^. #psMatch) V.!? (st ^. #psCur))
  drawFrame opts st

selection :: PickerOpts -> PickerState -> Text
selection opts st = case (st ^. #psMatch) V.!? (st ^. #psCur) of
  Just (i, _, _) -> (opts ^. #items) V.! i
  Nothing
    | opts ^. #printQuery -> st ^. #psQuery
    | otherwise           -> ""

-- | Tab pulls the highlighted row's *displayed* text (with-nth applied)
-- into the query box. If nothing is highlighted, the query is unchanged.
currentDisplay :: PickerOpts -> PickerState -> Text
currentDisplay opts st = case (st ^. #psMatch) V.!? (st ^. #psCur) of
  Just (i, _, _) -> display (opts ^. #withNth) ((opts ^. #items) V.! i)
  Nothing        -> st ^. #psQuery

-- | Re-run fuzzy filter. Empty query → all items in original order.
-- Delegates to 'matchMulti' so space-separated terms are ANDed and
-- @!term@ inverts.
computeMatches :: Text -> Vector Text -> Vector (Int, Int, [Int])
computeMatches q its
  | T.null q  = V.generate (V.length its) (\i -> (i, 0, []))
  | otherwise =
      let scoreOne i line = (\(s, ps) -> (i, s, ps)) <$> matchMulti q line
      in V.fromList $ sortOn (\(_, s, _) -> Down s)
                    $ V.toList $ V.imapMaybe scoreOne its

-- Rendering --------------------------------------------------------------

listRows :: PickerState -> Int
listRows st = (st ^. #psBox % #bh) - 3  -- minus top border, prompt, bottom border

-- | Strip the index field for display when 'withNth' is True.
display :: Bool -> Text -> Text
display True line = case T.splitOn "\t" line of
  _ : rest -> T.intercalate "\t" rest
  _        -> line
display False line = line

-- | Draw the whole popup — border (with embedded match-count on the
-- bottom row), prompt, optional header, item rows — into the cell
-- buffer and flush. Colours come from the active theme via 'readPalette'
-- ('Theme.stylesRef'), read once per frame. The Theme↔Fzf module cycle
-- is broken with an hs-boot import.
--
-- Layout:
--   row 0                 top border      ┌────────────┐
--   row 1                 prompt          │> query     │
--   row 2 (optional)      header          │header      │
--   row 2+ … bh-2         item rows       │> item …    │
--   row bh-1              bottom border   └──── 3/100 ─┘
drawFrame :: PickerOpts -> PickerState -> IO ()
drawFrame opts st = do
  pal <- readPalette
  let Box{bx, by, bw, bh} = st ^. #psBox
      hasHdr   = not (T.null (opts ^. #header))
      innerW   = bw - 2
      promptY  = by + 1
      hdrY     = by + 2
      firstRow = by + 2 + (if hasHdr then 1 else 0)
      nRows    = bh - 3 - (if hasHdr then 1 else 0)
      scroll   = scrollOff (st ^. #psCur) nRows
      promptLn = (opts ^. #prompt) <> (st ^. #psQuery)
      total    = V.length (opts ^. #items)
      matched  = V.length (st ^. #psMatch)
      countTxt = T.pack (show matched) <> "/" <> T.pack (show total)
  drawBorder pal bx by bw bh countTxt
  drawLine bx promptY innerW promptLn (pal ^. #fgPrompt) (pal ^. #bgPanel)
  when hasHdr $
    drawLine bx hdrY innerW (opts ^. #header) (pal ^. #fgHdr) (pal ^. #bgPanel)
  forM_ [0 .. nRows - 1] $ \r -> do
    let y = firstRow + r
    case (st ^. #psMatch) V.!? (scroll + r) of
      Nothing ->
        drawLine bx y innerW "" (pal ^. #fgRow) (pal ^. #bgPanel)
      Just (idx, _, poses) -> do
        let selected = (scroll + r) == st ^. #psCur
            raw      = (opts ^. #items) V.! idx
            shown    = display (opts ^. #withNth) raw
            adj      = if opts ^. #withNth then adjustPositions raw poses else poses
            bgLine   = if selected then pal ^. #bgSel else pal ^. #bgPanel
            fgLine   = if selected then pal ^. #fgSel else pal ^. #fgRow
            marker   = if selected then "> " else "  "
        drawItem bx y innerW marker shown adj
                 fgLine bgLine (pal ^. #fgMatch) bgLine
  Term.present

-- | Render the outer frame. The bottom row has the matched/total count
-- right-embedded as ── 3/100 ─┘ so we don't burn a whole row on it.
drawBorder :: Palette -> Int -> Int -> Int -> Int -> Text -> IO ()
drawBorder pal x y w h countTxt = do
  let innerW   = w - 2
      top      = "┌" <> T.replicate innerW "─" <> "┐"
      bot      = bottomBorder innerW countTxt
      fg       = pal ^. #fgBorder
      bg       = pal ^. #bgPanel
  Term.padC (fromIntegral x) (fromIntegral y)
            (fromIntegral w) fg bg top 0
  Term.padC (fromIntegral x) (fromIntegral (y + h - 1))
            (fromIntegral w) fg bg bot 0
  -- side bars on each inner row
  forM_ [1 .. h - 2] $ \dy -> do
    Term.padC (fromIntegral x) (fromIntegral (y + dy)) 1
              fg bg "│" 0
    Term.padC (fromIntegral (x + w - 1)) (fromIntegral (y + dy)) 1
              fg bg "│" 0

-- | Bottom border with the count embedded near the right end:
-- @└──────── 3/100 ─┘@. Falls back to a plain rule if the count
-- doesn't fit (very narrow popups).
bottomBorder :: Int -> Text -> Text
bottomBorder innerW countTxt =
  let w     = T.length countTxt + 2  -- spaces around the count
      tail_ = 2                      -- trailing dashes
  in if innerW < w + tail_ + 1
       then "└" <> T.replicate innerW "─" <> "┘"
       else
         let fill = innerW - w - tail_
         in "└" <> T.replicate fill "─" <> " " <> countTxt <> " "
              <> T.replicate tail_ "─" <> "┘"

-- | Draw a text row inside the frame (between the two side bars).
drawLine :: Int -> Int -> Int -> Text -> Word32 -> Word32 -> IO ()
drawLine x y innerW t fg bg = do
  let padded = " " <> padR (innerW - 1) t
  Term.padC (fromIntegral (x + 1)) (fromIntegral y) (fromIntegral innerW)
            fg bg padded 0

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
