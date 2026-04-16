{-
  Terminal rendering: cell buffer, keyboard input, table renderer.
  renderTable is pure Haskell; no C FFI.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.Term
  ( -- keys
    keyArrowUp, keyArrowDown, keyArrowLeft, keyArrowRight
  , keyPageUp, keyPageDown, keyHome, keyEnd
  , keyEsc, keyEnter, keyBackspace, keyBackspace2
    -- event types
  , eventKey
    -- modifiers
  , modAlt, modCtrl, modShift
    -- ctrl codes
  , ctrlD, ctrlU
    -- colors / attributes
  , parseColor, underline
    -- event record
  , Event(..)
    -- tty / lifecycle
  , isattyStdin, reopenTty, init, inited, shutdown
    -- screen
  , width, height, clear, present, pollEvent, toEvent, toEvents, bufferStr
  , padC, renderTable, print
  ) where

import Prelude hiding (init, print)
import Control.Monad (forM, forM_, when, zipWithM_)
import Data.Bits ((.&.), (.|.), testBit)
import qualified Data.Bits
import Data.Char (chr)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.Int (Int32, Int64)
import qualified Data.IntSet as IS
import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef')
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TB
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM
import Data.Word (Word8, Word16, Word32, Word64)
import Foreign.C.Types (CInt(..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (Ptr)
import Foreign.Storable (Storable(..))
import System.IO (stdin, stdout, hSetBuffering, hSetEcho, hFlush,
                  hIsTerminalDevice, BufferMode(..), hGetChar, hWaitForInput)
import System.Posix.IO (stdInput)
import qualified System.Posix.Terminal as PT
import System.IO.Unsafe (unsafePerformIO)
import Tv.Types (ColType(..), isNumeric)
import Optics.TH (makeFieldLabelsNoPrefix)

-- | Key codes (from termbox2.h)
keyArrowUp, keyArrowDown, keyArrowLeft, keyArrowRight :: Word16
keyArrowUp    = 0xFFFF - 18
keyArrowDown  = 0xFFFF - 19
keyArrowLeft  = 0xFFFF - 20
keyArrowRight = 0xFFFF - 21

keyPageUp, keyPageDown, keyHome, keyEnd :: Word16
keyPageUp   = 0xFFFF - 23
keyPageDown = 0xFFFF - 24
keyHome     = 0xFFFF - 25
keyEnd      = 0xFFFF - 26

keyEsc, keyEnter, keyBackspace, keyBackspace2 :: Word16
keyEsc        = 0x1B
keyEnter      = 0x0D
keyBackspace  = 0x08
keyBackspace2 = 0x7f

-- | Event types
eventKey :: Word8
eventKey = 1

-- | Modifiers (from termbox2.h)
modAlt, modCtrl, modShift :: Word8
modAlt   = 1
modCtrl  = 2
modShift = 4

-- | Ctrl+letter codes (Ctrl+A=1, Ctrl+B=2, ...)
ctrlD, ctrlU :: Word32
ctrlD = 4   -- Ctrl+D (page down)
ctrlU = 21  -- Ctrl+U (page up)

-- | All xterm-256 color names -> index. Built once at init.
-- ANSI names (0-15, black=16), rgbRGB cube (16-231), grayN ramp (232-255).
colorMap :: HashMap Text Word32
colorMap =
  let ansi =
        [ ("default", 0), ("red", 1), ("green", 2), ("yellow", 3), ("blue", 4)
        , ("magenta", 5), ("cyan", 6), ("white", 7), ("brBlack", 8), ("brRed", 9)
        , ("brGreen", 10), ("brYellow", 11), ("brBlue", 12), ("brMagenta", 13)
        , ("brCyan", 14), ("brWhite", 15), ("black", 16)
        ]
      cube =
        [ (T.pack ("rgb" ++ show r ++ show g ++ show b), fromIntegral (16 + 36 * r + 6 * g + b))
        | r <- [0..5 :: Int], g <- [0..5 :: Int], b <- [0..5 :: Int]
        ]
      gray =
        [ (T.pack ("gray" ++ show i), fromIntegral (232 + i)) | i <- [0..23 :: Int] ]
  in HM.fromList (ansi ++ cube ++ gray)

parseColor :: Text -> Word32
parseColor s = HM.lookupDefault 0 s colorMap

-- | Attributes (OR with color)
underline :: Word32
underline = 0x02000000

-- | Terminal event from poll
data Event = Event
  { typ :: Word8
  , mods  :: Word8
  , keyCode :: Word16
  , ch   :: Word32
  , w    :: Word32  -- resize width
  , h    :: Word32  -- resize height
  } deriving (Eq, Show)
makeFieldLabelsNoPrefix ''Event

-- ============================================================================
-- Cell buffer
--
-- Storable so cbits/tv_render.c can write in place. Layout must match
-- `TvCell` in cbits/tv_render.h (3 × uint32 = 12 bytes, 4-byte aligned).
-- ============================================================================

data Cell = Cell !Word32 !Word32 !Word32  -- ch, fg, bg
  deriving (Eq)

instance Storable Cell where
  sizeOf _    = 12
  alignment _ = 4
  peek p = do
    ch <- peekByteOff p 0
    fg <- peekByteOff p 4
    bg <- peekByteOff p 8
    pure (Cell ch fg bg)
  poke p (Cell ch fg bg) = do
    pokeByteOff p 0 ch
    pokeByteOff p 4 fg
    pokeByteOff p 8 bg

cellCh :: Cell -> Char
cellCh (Cell c _ _) = chr (fromIntegral c)

-- | `emptyCell` is a default space with default style — matches termbox2's
-- tb_clear initialization. `present` skips any cell that still equals this
-- value, mirroring termbox2's front/back buffer diff: cells at the default
-- space are indistinguishable from "nothing drawn" and don't need to be
-- emitted to the terminal.
emptyCell :: Cell
emptyCell = Cell 0x20 0 0

-- | Screen state: width, height, mutable cell buffer (row-major, w*h entries).
screenBuf :: IORef (Int, Int, VSM.IOVector Cell)
screenBuf = unsafePerformIO $ do
  v <- VSM.new 0
  newIORef (0, 0, v)
{-# NOINLINE screenBuf #-}

-- | Front buffer: the most recently presented frame's cells. `present`
-- diffs the current (back) buffer against this and only emits cells that
-- actually changed, matching termbox2's tb_present. Reset to all-zero
-- cells whenever the window size changes.
frontBuf :: IORef (VSM.IOVector Cell)
frontBuf = unsafePerformIO $ VSM.new 0 >>= newIORef
{-# NOINLINE frontBuf #-}

initedRef :: IORef Bool
initedRef = unsafePerformIO (newIORef False)
{-# NOINLINE initedRef #-}

-- | Saved original termios so shutdown can restore the parent shell's mode.
-- termbox2's init_term_attrs does the same with `global.orig_tios`.
origTios :: IORef (Maybe PT.TerminalAttributes)
origTios = unsafePerformIO (newIORef Nothing)
{-# NOINLINE origTios #-}

-- | Headless flag: True when the terminal is not a tty (tests, -c mode).
-- Mirrors `headless` in Tc/c/term_core.c: when set, `present` is a no-op so
-- stdout is left clean for the test harness to read `bufferStr` instead.
headlessRef :: IORef Bool
headlessRef = unsafePerformIO (newIORef False)
{-# NOINLINE headlessRef #-}

-- | isatty(stdin)
isattyStdin :: IO Bool
isattyStdin = hIsTerminalDevice stdin

-- | Try to (re)open /dev/tty on stdin so the program can still drive a TUI
-- when its stdin was redirected. Returns True on success.
reopenTty :: IO Bool
reopenTty = hIsTerminalDevice stdin

-- | Initialize terminal: raw-ish mode, query size via TIOCGWINSZ, allocate
-- screen buffer, and emit the termbox2 init sequence (enter alt screen,
-- save title stack, application cursor keys, application keypad, hide
-- cursor, clear). Uses `tv_term_size` (ioctl) rather than
-- `ANSI.getTerminalSize` so the DSR probe doesn't leak into stdout.
init :: IO Int32
init = do
  already <- readIORef initedRef
  if already then pure 0
  else do
    hSetBuffering stdin NoBuffering
    hSetBuffering stdout NoBuffering
    hSetEcho stdin False
    isTty <- hIsTerminalDevice stdin
    -- cbreak-ish: ICANON off so bytes arrive unbuffered. Leaves OPOST on
    -- so NL→CRLF translation on stdout still works (termbox2 turns it off
    -- in cfmakeraw, but we don't emit bare `\n` during rendering).
    when isTty $ do
      orig <- PT.getTerminalAttributes stdInput
      writeIORef origTios (Just orig)
      let ta = foldl PT.withoutMode orig
                 [ PT.ProcessInput, PT.EnableEcho, PT.EchoLF
                 , PT.KeyboardInterrupts, PT.ExtendedFunctions
                 , PT.StartStopOutput, PT.StartStopInput
                 ]
          ta' = PT.withMinInput (PT.withTime ta 0) 1
      PT.setTerminalAttributes stdInput ta' PT.WhenFlushed
    (w, h) <- if isTty
                then termSize
                else pure (80, 24)
    buf <- VSM.replicate (w * h) emptyCell
    writeIORef screenBuf (w, h, buf)
    -- Front buffer starts at all-default; termbox2's tb_init does the same,
    -- so the first `present` frame emits every non-default cell.
    front <- VSM.replicate (w * h) emptyCell
    writeIORef frontBuf front
    writeIORef initedRef True
    writeIORef headlessRef (not isTty)
    when isTty $ do
      -- Match termbox2 tb_init output: alt screen + XTWINOPS title stack
      -- save + application cursor keys + keypad application mode + hide
      -- cursor + clear + home. Byte-identical to what
      -- Tc/.lake/build/bin/tc emits on startup.
      -- Lean's tb_init order: cursor hide → charset/SGR reset → home → clear
      TIO.hPutStr stdout "\x1b[?1049h\x1b[22;0;0t\x1b[?1h\x1b=\x1b[?25l\x1b(B\x1b[m\x1b[H\x1b[2J"
      hFlush stdout
    pure 0

-- | Query terminal dimensions via ioctl(TIOCGWINSZ). Falls back to 80x24
-- when the call fails (e.g. headless build host without a controlling tty).
-- struct winsize is 8 bytes: uint16 ws_row, ws_col, ws_xpixel, ws_ypixel
termSize :: IO (Int, Int)
termSize =
  allocaBytes 8 $ \pWs -> do
    -- TIOCGWINSZ = 0x5413 on Linux
    rc <- c_ioctl 0 0x5413 pWs  -- STDIN_FILENO = 0
    if rc < 0
      then pure (80, 24)
      else do
        rows <- peekByteOff pWs 0 :: IO Word16
        cols <- peekByteOff pWs 2 :: IO Word16
        if rows == 0 || cols == 0
          then pure (80, 24)
          else pure (fromIntegral cols, fromIntegral rows)

foreign import ccall unsafe "ioctl"
  c_ioctl :: CInt -> CInt -> Ptr () -> IO CInt

inited :: IO Bool
inited = readIORef initedRef

shutdown :: IO ()
shutdown = do
  on <- readIORef initedRef
  when on $ do
    -- Inverse of init: SGR reset, show cursor, exit keypad/cursor-keys
    -- modes, restore title stack, leave alt screen.
    TIO.hPutStr stdout "\x1b[m\x1b[?25h\x1b[?1l\x1b>\x1b[23;0;0t\x1b[?1049l"
    hSetEcho stdin True
    hFlush stdout
    mOrig <- readIORef origTios
    case mOrig of
      Just orig -> PT.setTerminalAttributes stdInput orig PT.WhenFlushed
      Nothing   -> pure ()
    writeIORef origTios Nothing
    writeIORef initedRef False

width :: IO Word32
width = do
  (w, _, _) <- readIORef screenBuf
  pure (fromIntegral w)

height :: IO Word32
height = do
  (_, h, _) <- readIORef screenBuf
  pure (fromIntegral h)

clear :: IO ()
clear = do
  (_, _, buf) <- readIORef screenBuf
  VSM.set buf emptyCell

-- | Fold state for `present`: last emitted style, last cursor position,
-- and the accumulated output Builder. Strict fields keep the builder
-- from stacking thunks across the w*h cells (render hot path).
data PresentAcc = PresentAcc
  !(Maybe (Word32, Word32))  -- lastStyle
  !(Int, Int)                -- lastCursor
  !TB.Builder                -- out

-- | Flush cell buffer to the terminal. No-op in headless mode (tests, -c
-- mode) so stdout stays clean for `bufferStr` capture.
--
-- Emits a termbox2-compatible stream: per contiguous run of same-style
-- cells, write `\x1b(B\x1b[m` (charset reset), then bold/underline attrs,
-- then `\x1b[38;5;FG;48;5;BGm` for 256-color fg/bg, then a cursor position,
-- then the characters. Attribute bits (bold = 0x01000000,
-- underline = 0x02000000) are encoded in the high byte of the `fg` word
-- per cbits/tv_render.c's convention.
present :: IO ()
present = do
  headless <- readIORef headlessRef
  when (not headless) $ do
    (w, h, buf) <- readIORef screenBuf
    front <- readIORef frontBuf
    -- One TIO.hPutStr per frame mirrors termbox2's tb_present flush —
    -- keeps the PTY recorder's `drain()` from slicing the frame across
    -- multiple cast frames, which breaks byte parity for multi-step demos.
    let s0 = PresentAcc Nothing (-1, -1) mempty
        -- Walk row-major, bumping (y, x) explicitly instead of dividing
        -- idx by w per cell.
        go !acc !y !x !idx
          | y == h    = pure acc
          | x == w    = go acc (y + 1) 0 idx
          | otherwise = do
              acc' <- stepCell buf front acc y x idx
              go acc' y (x + 1) (idx + 1)
    PresentAcc _ _ outB <- go s0 0 0 0
    TIO.hPutStr stdout (TL.toStrict (TB.toLazyText outB))
    hFlush stdout

stepCell
  :: VSM.IOVector Cell
  -> VSM.IOVector Cell
  -> PresentAcc
  -> Int -> Int -> Int
  -> IO PresentAcc
stepCell buf front acc@(PresentAcc lastStyle lastCursor b) y x idx = do
  cell     <- VSM.read buf idx
  prevCell <- VSM.read front idx
  if cell == prevCell
    then pure acc
    else do
      let Cell ch fg bg = cell
          style = (fg, bg)
          (b1, lastStyle') =
            if Just style /= lastStyle
              then (b <> TB.fromText (sgrFor fg bg), Just style)
              else (b, lastStyle)
          b2 = if lastCursor /= (y, x)
                 then b1 <> TB.fromText (cursorAt y x)
                 else b1
          b3 = b2 <> TB.singleton (chr (fromIntegral ch))
      VSM.write front idx cell
      pure (PresentAcc lastStyle' (y, x + 1) b3)

-- | Build the SGR + charset-reset prefix for a cell. Extracts attr bits
-- from `fg` before masking off the color. Matches Lean termbox2's output
-- byte-for-byte: `\x1b(B\x1b[m` (charset reset + SGR reset) then optional
-- bold/underline, then color codes. A 0 fg or bg is elided because the
-- preceding `\x1b[m` already set both to default — emitting `38;5;0` or
-- `48;5;0` after that would be a no-op but cost extra bytes.
sgrFor :: Word32 -> Word32 -> Text
sgrFor fg bg =
  let color   = fg .&. 0x1FF
      bgColor = bg .&. 0x1FF
      bold    = if fg .&. 0x01000000 /= 0 then "\x1b[1m" else ""
      ul      = if fg .&. 0x02000000 /= 0 then "\x1b[4m" else ""
      colors  = case (color, bgColor) of
        (0, 0) -> ""
        (f, 0) -> T.pack ("\x1b[38;5;" ++ show f ++ "m")
        (0, b) -> T.pack ("\x1b[48;5;" ++ show b ++ "m")
        (f, b) -> T.pack ("\x1b[38;5;" ++ show f ++ ";48;5;" ++ show b ++ "m")
  in "\x1b(B\x1b[m" <> bold <> ul <> colors

cursorAt :: Int -> Int -> Text
cursorAt y x = T.pack ("\x1b[" ++ show (y + 1) ++ ";" ++ show (x + 1) ++ "H")

-- | Pure byte → Event translation (factored out of pollEvent for unit tests).
-- ASCII control chars map to the termbox key constants so evToKey's keyNames
-- lookup matches (0x0D → keyEnter → "<ret>", 0x7F → keyBackspace2 → "<bs>", …).
toEvent :: Char -> Event
toEvent c =
  let code = fromIntegral (fromEnum c) :: Word16
      (kCode, kCh) = case code of
        0x0D -> (keyEnter, 0)         -- CR → Enter
        0x0A -> (keyEnter, 0)         -- LF → Enter
        0x08 -> (keyBackspace, 0)
        0x7F -> (keyBackspace2, 0)
        0x1B -> (keyEsc, 0)
        _    -> (0, fromIntegral code)
  in Event
       { typ = eventKey
       , mods = 0
       , keyCode = kCode
       , ch = kCh
       , w = 0
       , h = 0
       }

-- | Pure multi-byte → Event translation. Handles CSI (`ESC [ …`) and SS3
-- (`ESC O …`) sequences for arrows, home, end, pgup, pgdn. Any input the
-- dispatch tables don't cover degrades to the first-byte `toEvent`,
-- which for ESC yields keyEsc.
toEvents :: String -> Event
toEvents s = case s of
  ['\x1B', '[', c]      -> csiLetter c
  ['\x1B', 'O', c]      -> csiLetter c
  ('\x1B' : '[' : rest)
    | (digits, "~") <- span isDigit rest -> csiTilde digits
  [c] -> toEvent c
  _   -> toEvent '\x1B'
  where
    isDigit ch = ch >= '0' && ch <= '9'
    key k = Event { typ = eventKey, mods = 0, keyCode = k
                  , ch = 0, w = 0, h = 0 }
    csiLetter 'A' = key keyArrowUp
    csiLetter 'B' = key keyArrowDown
    csiLetter 'C' = key keyArrowRight
    csiLetter 'D' = key keyArrowLeft
    csiLetter 'H' = key keyHome
    csiLetter 'F' = key keyEnd
    csiLetter _   = toEvent '\x1B'
    csiTilde "1" = key keyHome
    csiTilde "4" = key keyEnd
    csiTilde "5" = key keyPageUp
    csiTilde "6" = key keyPageDown
    csiTilde "7" = key keyHome
    csiTilde "8" = key keyEnd
    csiTilde _   = toEvent '\x1B'

-- | Poll a single key event. After ESC, `hWaitForInput` briefly for a CSI
-- or SS3 sequence; if none arrives within 100 ms, treat it as a lone Esc.
pollEvent :: IO Event
pollEvent = do
  c <- hGetChar stdin
  if c /= '\x1B'
    then pure (toEvent c)
    else do
      more <- hWaitForInput stdin 100
      if not more
        then pure (toEvent '\x1B')
        else do
          c2 <- hGetChar stdin
          case c2 of
            '[' -> readCsi
            'O' -> do c3 <- hGetChar stdin; pure (toEvents ['\x1B', 'O', c3])
            _   -> pure (toEvent '\x1B')
  where
    -- A CSI tail is either a single letter final byte (arrows, home, end)
    -- or any run of digit parameters terminated by `~` (pgup, pgdn, …).
    readCsi = do
      c <- hGetChar stdin
      if c >= '0' && c <= '9'
        then readCsiTilde [c]
        else pure (toEvents ['\x1B', '[', c])
    readCsiTilde acc = do
      c <- hGetChar stdin
      if c >= '0' && c <= '9'
        then readCsiTilde (c : acc)
        else pure (toEvents ('\x1B' : '[' : reverse acc ++ [c]))

-- | Read termbox internal cell buffer as string (rows separated by newlines).
-- Trailing spaces on each row are trimmed to match Lean's lean_tb_buffer_str.
-- Used by tests to assert on-screen content.
bufferStr :: IO Text
bufferStr = do
  (w, h, buf) <- readIORef screenBuf
  rows <- forM [0 .. h - 1] $ \y -> do
    cs <- forM [0 .. w - 1] $ \x -> do
      Cell c _ _ <- VSM.read buf (y * w + x)
      pure (if c == 0 then ' ' else chr (fromIntegral c))
    pure (T.dropWhileEnd (== ' ') (T.pack cs))
  pure (T.intercalate "\n" rows)

-- | Write text + padding into the cell buffer at (x, y), padding to `len`.
padC :: Word32 -> Word32 -> Word32 -> Word32 -> Word32 -> Text -> Word8 -> IO ()
padC x y len fg bg s _attr = do
  (w, h, buf) <- readIORef screenBuf
  let xi = fromIntegral x
      yi = fromIntegral y
      li = fromIntegral len
      chars = T.unpack s
      padded = take li (chars ++ repeat ' ')
  when (yi >= 0 && yi < h) $ do
    zipWithM_
      (\i ch ->
        let cx = xi + i
        in when (cx >= 0 && cx < w) $
             VSM.write buf (yi * w + cx)
               (Cell (fromIntegral (fromEnum ch)) fg bg))
      [0 ..] padded

-- | Print string at position (for backwards compat)
print :: Word32 -> Word32 -> Word32 -> Word32 -> Text -> IO ()
print x y fg bg s =
  padC x y (fromIntegral (T.length s)) fg bg s 0

-- ============================================================================
-- renderTable — pure Haskell (replaces cbits/tv_render.c)
-- ============================================================================

-- Constants matching the C renderer
_MIN_HDR_WIDTH, _MAX_DISP_WIDTH :: Int
_MIN_HDR_WIDTH  = 3
_MAX_DISP_WIDTH = 50

_SEP_FG :: Word32
_SEP_FG = 240

_TB_BOLD, _TB_UNDERLINE :: Word32
_TB_BOLD      = 0x01000000
_TB_UNDERLINE = 0x02000000

_HEAT_FG :: Word32
_HEAT_FG = 16

-- Style indices
_STYLE_CURSOR, _STYLE_SEL_ROW, _STYLE_SEL_CUR, _STYLE_SEL_COL :: Int
_STYLE_CUR_ROW, _STYLE_CUR_COL, _STYLE_DEFAULT, _STYLE_HEADER, _STYLE_GROUP :: Int
_STYLE_CURSOR  = 0
_STYLE_SEL_ROW = 1
_STYLE_SEL_CUR = 2
_STYLE_SEL_COL = 3
_STYLE_CUR_ROW = 4
_STYLE_CUR_COL = 5
_STYLE_DEFAULT = 6
_STYLE_HEADER  = 7
_STYLE_GROUP   = 8

-- Heat mode types
data HeatKind = HeatNone | HeatNum | HeatStr deriving (Eq)

data HeatCol = HeatCol
  { hkKind :: !HeatKind
  , hkMn   :: !Double
  , hkMx   :: !Double
  , hkDate :: !Bool
  }

heatColNone :: HeatCol
heatColNone = HeatCol HeatNone 0 0 False

-- VisiData-style type chars: # int, % float, ? bool, @ date, space string
typeCharFmt :: Char -> Char
typeCharFmt fmt = case fmt of
  'l' -> '#'; 'i' -> '#'; 's' -> '#'; 'c' -> '#'
  'L' -> '#'; 'I' -> '#'; 'S' -> '#'; 'C' -> '#'
  'g' -> '%'; 'f' -> '%'; 'e' -> '%'
  'b' -> '?'
  't' -> '@'
  _   -> ' '

typeCharColType :: ColType -> Char
typeCharColType ColTypeInt     = '#'
typeCharColType ColTypeFloat   = '%'
typeCharColType ColTypeDecimal = '%'
typeCharColType ColTypeBool    = '?'
typeCharColType ColTypeDate    = '@'
typeCharColType ColTypeTime    = '@'
typeCharColType ColTypeTimestamp = '@'
typeCharColType _              = ' '

isDateFmt :: Char -> Bool
isDateFmt 't' = True
isDateFmt _   = False

getStyleIdx :: Bool -> Bool -> Bool -> Bool -> Bool -> Int
getStyleIdx isCursor isSelRow isSel isCurRow isCurCol
  | isCursor          = _STYLE_CURSOR
  | isSelRow          = _STYLE_SEL_ROW
  | isSel && isCurRow = _STYLE_SEL_CUR
  | isSel             = _STYLE_SEL_COL
  | isCurRow          = _STYLE_CUR_ROW
  | isCurCol          = _STYLE_CUR_COL
  | otherwise         = _STYLE_DEFAULT

-- | FNV-1a hash -> [0,1] for categorical string coloring
heatStrHash01 :: Text -> Double
heatStrHash01 s =
  let h0 = 2166136261 :: Word32
      step h c = (h `xor` fromIntegral (fromEnum c)) * 16777619
      h = T.foldl' step h0 s
  in fromIntegral (h .&. 0xFFFF) / 65535.0
  where xor = Data.Bits.xor

-- | Extract digits from date/time string -> monotonic double
heatDateToNum :: Text -> Double
heatDateToNum s = T.foldl' step 0 s
  where step v c | c >= '0' && c <= '9' = v * 10 + fromIntegral (fromEnum c - fromEnum '0')
                 | otherwise             = v

-- Viridis-inspired color ramp (xterm-256)
heatRamp :: V.Vector Word32
heatRamp = V.fromList [53,54,55,61,25,31,30,36,42,41,77,113,149,148,184,190,226]
{-# NOINLINE heatRamp #-}

heatColor :: Double -> Word32
heatColor t
  | t <= 0.0  = heatRamp V.! 0
  | t >= 1.0  = heatRamp V.! (n - 1)
  | otherwise = let pos = t * fromIntegral (n - 1)
                    lo = min (n - 2) (floor pos :: Int)
                in if pos - fromIntegral lo < 0.5 then heatRamp V.! lo else heatRamp V.! (lo + 1)
  where n = V.length heatRamp

-- | setCell: write a character into the screen buffer
setCell :: VSM.IOVector Cell -> Int -> Int -> Int -> Int -> Word32 -> Word32 -> Word32 -> IO ()
setCell buf w h x y ch fg bg
  | x < 0 || y < 0 || x >= w || y >= h = pure ()
  | otherwise = VSM.write buf (y * w + x) (Cell ch fg bg)

-- | printPad: write text with padding into screen buffer
-- right=True: right-align (pad on left), right=False: left-align (pad on right)
printPadBuf :: VSM.IOVector Cell -> Int -> Int -> Int -> Int -> Int -> Word32 -> Word32 -> Text -> Bool -> IO ()
printPadBuf buf w h x y padW fg bg s right_ = do
  let chars = T.unpack s
      len = min (length chars) padW
      pad = padW - len
      cx0 = x
  if right_
    then do
      -- pad spaces on left
      forM_ [0 .. pad - 1] $ \i ->
        setCell buf w h (cx0 + i) y (fromIntegral (fromEnum ' ')) fg bg
      -- then text
      forM_ (zip [0..] (take len chars)) $ \(i, ch) ->
        setCell buf w h (cx0 + pad + i) y (fromIntegral (fromEnum ch)) fg bg
    else do
      -- text first
      forM_ (zip [0..] (take len chars)) $ \(i, ch) ->
        setCell buf w h (cx0 + i) y (fromIntegral (fromEnum ch)) fg bg
      -- pad spaces on right
      forM_ [0 .. pad - 1] $ \i ->
        setCell buf w h (cx0 + len + i) y (fromIntegral (fromEnum ' ')) fg bg

-- | Unified table render. Pure Haskell replacement for C FFI.
renderTable
  :: Vector (Vector Text) -- texts: column-major, col -> localRow -> formatted text
  -> Vector Text          -- column names
  -> Vector Char          -- fmts (Arrow format chars)
  -> Vector ColType       -- colTypes
  -> Vector Word64        -- inWidths (cached)
  -> Vector Word64        -- colIdxs (display order)
  -> Word64               -- nTotalRows (unused but kept for compat)
  -> Word64               -- nKeys (pinned group columns)
  -> Word64               -- colOff
  -> Word64               -- r0 (always 0 -- data is pre-sliced)
  -> Word64               -- r1 (= nVisible)
  -> Word64               -- curRow
  -> Word64               -- curCol (original column index)
  -> Int64                -- moveDir
  -> Vector Word64        -- selCols
  -> Vector Word64        -- selRows
  -> Vector Word64        -- hiddenCols
  -> Vector Word32        -- styles (fg/bg pairs, 9x2)
  -> Int64                -- prec (unused -- text is pre-formatted)
  -> Int64                -- widthAdj
  -> Word8                -- heatMode
  -> Vector Text          -- sparklines
  -> Vector (Vector Double) -- heatDoubles: column-major raw doubles for heat
  -> IO (Vector Word64)   -- output base widths
renderTable
  texts names fmts colTypes inWidths colIdxs
  _nTotalRows nKeys colOff0 r0 r1 curRow curCol moveDir
  selCols selRows hiddenCols styles
  _prec widthAdj heatMode sparklines heatDoubles
  = do
  (screenW, screenH, buf) <- readIORef screenBuf
  let nCols = V.length names  -- total column count
      nRows = if r1 > r0 then fromIntegral (r1 - r0) else 0 :: Int

  -- sparkline: active if any non-empty string
  let sparkOn = V.any (\sp -> not (T.null sp)) sparklines

  let stFg si = fromMaybe 0 (styles V.!? (si * 2))
      stBg si = fromMaybe 0 (styles V.!? (si * 2 + 1))

  -- build selection bitsets
  let toIS v = V.foldl' (\acc x -> IS.insert (fromIntegral x) acc) IS.empty v
      colBits = toIS selCols
      rowBits = toIS selRows
      hidBits = toIS hiddenCols

  -- Compute base / rendered widths for ALL columns
  let computeDataWidth :: Int -> Int
      computeDataWidth c =
        let col = if c < V.length texts then texts V.! c else V.empty
        in V.foldl' (\acc t -> max acc (T.length t)) 1 col

      baseWidthsV = V.generate nCols $ \c ->
        if IS.member c hidBits then 0
        else let cached = if c < V.length inWidths
                          then fromIntegral (inWidths V.! c) else 0 :: Int
                 dw = computeDataWidth c
                 base = (max dw _MIN_HDR_WIDTH) + 2
             in max base cached

      allWidthsV = V.generate nCols $ \c ->
        if IS.member c hidBits then 3
        else let base = baseWidthsV V.! c
                 disp = min base _MAX_DISP_WIDTH
                 w = disp + fromIntegral widthAdj
             in max w 3

  -- compute x positions: key columns pinned left, then scrollable
  let nKeysI = fromIntegral nKeys :: Int
      nDispCols = V.length colIdxs

  -- pinned key width
  let keyWidth = sum [ (allWidthsV V.! fromIntegral (colIdxs V.! c)) + 1
                     | c <- [0 .. min nKeysI nDispCols - 1] ]

  -- find cursor's display index relative to non-key columns
  let curDispIdx0 = head $ [ c - nKeysI | c <- [nKeysI .. nDispCols - 1]
                            , fromIntegral (colIdxs V.! c) == curCol ] ++ [0]

  -- adjust colOff so cursor column is visible
  let scrollW = max 1 (screenW - keyWidth)
      adjColOff = adjustColOff (fromIntegral colOff0) curDispIdx0 scrollW
      adjustColOff co curDI sw
        | curDI < co = adjustColOff curDI curDI sw
        | otherwise  =
            let cumX = sum [ (allWidthsV V.! fromIntegral (colIdxs V.! (nKeysI + c'))) + 1
                           | c' <- [co .. curDI], nKeysI + c' < nDispCols ]
            in if cumX <= sw || co >= curDI then co
               else adjustColOff (co + 1) curDI sw

  let buildLayout =
        let goKey c x acc
              | c >= nKeysI || c >= nDispCols || x >= screenW = (acc, c, x)
              | otherwise =
                  let origIdx = fromIntegral (colIdxs V.! c)
                      cw = min (allWidthsV V.! origIdx) (screenW - x)
                  in goKey (c + 1) (x + cw + 1) ((c, x, cw) : acc)
            (keyList, _visKeyCount, x1) = goKey 0 0 []
            visKeys = length keyList
            nonKeyStart = nKeysI + adjColOff
            goNonKey c x acc
              | c >= nDispCols || x >= screenW = acc
              | otherwise =
                  let origIdx = fromIntegral (colIdxs V.! c)
                      cw = min (allWidthsV V.! origIdx) (screenW - x)
                  in goNonKey (c + 1) (x + cw + 1) ((c, x, cw) : acc)
            nonKeyList = goNonKey nonKeyStart x1 []
        in (V.fromList (reverse keyList ++ reverse nonKeyList), visKeys)
      (layoutV, visKeys) = buildLayout

  let nVisCols = V.length layoutV

  -- expand cursor column to absorb trailing screen whitespace
  let layoutV2 =
        if nVisCols == 0 then layoutV
        else
          let (_, lastX, lastW) = layoutV V.! (nVisCols - 1)
              usedW = lastX + lastW + 1
              slack = screenW - usedW
          in if slack <= 0 then layoutV
             else
               -- find cursor's position in visible columns
               let curVisIdx = V.findIndex (\(di, _, _) ->
                     fromIntegral (colIdxs V.! di) == curCol) layoutV
               in case curVisIdx of
                    Nothing -> layoutV
                    Just cvi ->
                      let (di, cx, cw) = layoutV V.! cvi
                          origIdx = fromIntegral (colIdxs V.! di)
                          base = max 3 (baseWidthsV V.! origIdx + fromIntegral widthAdj)
                          expand = min slack (max 0 (base - cw))
                      in if expand <= 0 then layoutV
                         else V.imap (\i (d, xx, ww) ->
                                if i == cvi then (d, xx, ww + expand)
                                else if i > cvi then (d, xx + expand, ww)
                                else (d, xx, ww)
                              ) layoutV

  -- ---- header + footer with separators and type chars ----
  let yFoot = screenH - 3

  forM_ [0 .. V.length layoutV2 - 1] $ \c -> do
    let (dispIdx, xPos, cw) = layoutV2 V.! c
        origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
        name = fromMaybe "" (names V.!? origIdx)
        isSel = IS.member origIdx colBits
        isCur = origIdx == fromIntegral curCol
        isGrp = dispIdx < nKeysI
        si = if isCur then _STYLE_CURSOR
             else if isSel then _STYLE_SEL_COL
             else if isGrp then _STYLE_GROUP
             else _STYLE_HEADER
        fg = stFg si .|. _TB_BOLD .|. _TB_UNDERLINE
        bg = if isGrp then stBg _STYLE_GROUP else stBg si

    -- leading space
    setCell buf screenW screenH xPos 0 (fromIntegral (fromEnum ' ')) fg bg
    setCell buf screenW screenH xPos yFoot (fromIntegral (fromEnum ' ')) fg bg

    -- column name
    let hw = max 0 (cw - 2)
    when (hw > 0) $ do
      printPadBuf buf screenW screenH (xPos + 1) 0 hw fg bg name False
      printPadBuf buf screenW screenH (xPos + 1) yFoot hw fg bg name False

    -- type char
    let tc = if origIdx < V.length fmts
             then typeCharFmt (fmts V.! origIdx)
             else typeCharColType (fromMaybe ColTypeOther (colTypes V.!? origIdx))
    setCell buf screenW screenH (xPos + cw - 1) 0 (fromIntegral (fromEnum tc)) fg bg
    setCell buf screenW screenH (xPos + cw - 1) yFoot (fromIntegral (fromEnum tc)) fg bg

    -- separator after column
    let sX = xPos + cw
    when (sX < screenW) $ do
      let isKey = c + 1 == visKeys
          sc = if isKey then 0x2551 else 0x2502  -- double or single vertical
          sf = if isKey then stFg _STYLE_GROUP else _SEP_FG
      setCell buf screenW screenH sX 0 sc sf (stBg _STYLE_DEFAULT)
      setCell buf screenW screenH sX yFoot sc sf (stBg _STYLE_DEFAULT)

  -- ---- sparkline row (y=1) ----
  when sparkOn $ do
    let spFg = stFg _STYLE_HEADER
        spBg = stBg _STYLE_DEFAULT
    forM_ [0 .. V.length layoutV2 - 1] $ \c -> do
      let (dispIdx, xPos, cw) = layoutV2 V.! c
          origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
          sp = fromMaybe "" (sparklines V.!? origIdx)
      printPadBuf buf screenW screenH xPos 1 cw spFg spBg sp False
      let sX = xPos + cw
      when (sX < screenW) $ do
        let isKey = c + 1 == visKeys
            sc = if isKey then 0x2551 else 0x2502
            sf = if isKey then stFg _STYLE_GROUP else _SEP_FG
        setCell buf screenW screenH sX 1 sc sf (stBg _STYLE_DEFAULT)

  let dataY0 = if sparkOn then 2 else 1

  -- ---- heat scan ----
  let heatCols = if heatMode == 0 || nVisCols > 256 then V.empty
        else V.generate nVisCols $ \c ->
          let (dispIdx, _, _) = layoutV2 V.! c
              origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
              fmt = if origIdx < V.length fmts then fmts V.! origIdx else '\0'
              typ = fromMaybe ColTypeOther (colTypes V.!? origIdx)
              -- Get text column for this origIdx
              textCol = if origIdx < V.length texts then texts V.! origIdx else V.empty
              -- Get heat doubles column for this origIdx
              hdCol = if origIdx < V.length heatDoubles then heatDoubles V.! origIdx else V.empty
          in if isNumeric typ
             then -- numeric heat: scan heatDoubles
               let (mn, mx) = V.foldl' (\(lo, hi) v ->
                      if isNaN v then (lo, hi)
                      else (min lo v, max hi v)) (1e308, -1e308) hdCol
               in if mx > mn then HeatCol HeatNum mn mx False
                  else heatColNone
             else if isDateFmt fmt
             then -- date heat: extract digits from text
               let (mn, mx) = V.foldl' (\(lo, hi) t ->
                      if T.null t then (lo, hi)
                      else let v = heatDateToNum t in (min lo v, max hi v))
                      (1e308, -1e308) textCol
               in if mx > mn then HeatCol HeatNum mn mx True
                  else heatColNone
             else -- string heat: check diversity
               let first = V.find (not . T.null) textCol
               in case first of
                    Nothing -> heatColNone
                    Just f  -> if V.any (\t -> not (T.null t) && t /= f) textCol
                               then HeatCol HeatStr 0 0 False
                               else heatColNone

  -- ---- data rows ----
  forM_ [0 .. nRows - 1] $ \ri -> do
    let row = fromIntegral r0 + ri
        y = ri + dataY0
        isSelRow = IS.member (fromIntegral row) rowBits
        isCurRow = fromIntegral row == curRow

    forM_ [0 .. V.length layoutV2 - 1] $ \c -> do
      let (dispIdx, xPos, cw) = layoutV2 V.! c
          origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
          isSel = IS.member origIdx colBits
          isCurCol = origIdx == fromIntegral curCol
          isGrp = dispIdx < nKeysI
          si = getStyleIdx (isCurRow && isCurCol) isSelRow isSel isCurRow isCurCol
          bgBase = if isGrp then stBg _STYLE_GROUP else stBg si
          fgBase = stFg si

      -- heat mode
      let (fg, bg) =
            if heatMode == 0 || V.null heatCols then (fgBase, bgBase)
            else if si == _STYLE_CURSOR || si == _STYLE_SEL_ROW || si == _STYLE_SEL_CUR
            then (fgBase, bgBase)
            else case heatCols V.!? c of
              Nothing -> (fgBase, bgBase)
              Just hc ->
                if hkKind hc == HeatNone then (fgBase, bgBase)
                else if hkKind hc == HeatNum && not (testBit heatMode 0) then (fgBase, bgBase)
                else if hkKind hc == HeatStr && not (testBit heatMode 1) then (fgBase, bgBase)
                else
                  let textCol = if origIdx < V.length texts then texts V.! origIdx else V.empty
                      cellText = fromMaybe "" (textCol V.!? ri)
                  in if hkKind hc == HeatNum
                     then if hkDate hc
                          then if T.null cellText then (fgBase, bgBase)
                               else let v = heatDateToNum cellText
                                        t = (v - hkMn hc) / (hkMx hc - hkMn hc)
                                    in (_HEAT_FG, heatColor t)
                          else -- numeric: use heatDoubles
                            let hdCol = if origIdx < V.length heatDoubles
                                        then heatDoubles V.! origIdx else V.empty
                                val = fromMaybe (0/0) (hdCol V.!? ri)
                            in if isNaN val then (fgBase, bgBase)
                               else let t = (val - hkMn hc) / (hkMx hc - hkMn hc)
                                    in (_HEAT_FG, heatColor t)
                     else -- string hash
                       if T.null cellText then (fgBase, bgBase)
                       else (_HEAT_FG, heatColor (heatStrHash01 cellText))

      -- get cell text
      let textCol = if origIdx < V.length texts then texts V.! origIdx else V.empty
          cellText = fromMaybe "" (textCol V.!? ri)
          isNum = maybe False isNumeric (colTypes V.!? origIdx)

      -- leading space
      setCell buf screenW screenH xPos y (fromIntegral (fromEnum ' ')) fg bg
      let contentW = max 0 (cw - 2)
      when (contentW > 0) $
        printPadBuf buf screenW screenH (xPos + 1) y contentW fg bg cellText isNum
      -- trailing space
      setCell buf screenW screenH (xPos + cw - 1) y (fromIntegral (fromEnum ' ')) fg bg

      -- separator
      let sX = xPos + cw
      when (sX < screenW) $ do
        let isKey = c + 1 == visKeys
            sc = if isKey then 0x2551 else 0x2502
            sf = if isKey then stFg _STYLE_GROUP else _SEP_FG
        setCell buf screenW screenH sX y sc sf (stBg _STYLE_DEFAULT)

  -- ---- tooltip: full header name if truncated ----
  forM_ [0 .. V.length layoutV2 - 1] $ \c -> do
    let (dispIdx, xPos, cw) = layoutV2 V.! c
        origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
    when (origIdx == fromIntegral curCol) $ do
      let name = fromMaybe "" (names V.!? origIdx)
          nameLen = T.length name
          colW = cw - 2
      when (nameLen > colW) $ do
        let fg = stFg _STYLE_CURSOR .|. _TB_BOLD .|. _TB_UNDERLINE
            chars = T.unpack name
        if moveDir > 0
          then do
            let endX = xPos + cw - 1
                startX = max 0 (endX - nameLen)
                tipW = endX - startX
                skip = nameLen - tipW
                visChars = drop skip chars
            forM_ (zip [0..] (take tipW visChars)) $ \(i, ch) ->
              setCell buf screenW screenH (startX + i) 0
                (fromIntegral (fromEnum ch)) fg (stBg _STYLE_CURSOR)
          else do
            let maxW = screenW - xPos - 1
                tipW = min nameLen maxW
            forM_ (zip [0..] (take tipW chars)) $ \(i, ch) ->
              setCell buf screenW screenH (xPos + 1 + i) 0
                (fromIntegral (fromEnum ch)) fg (stBg _STYLE_CURSOR)

  -- output widths (base widths, no widthAdj, 0 for hidden)
  pure (V.map fromIntegral baseWidthsV)
