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
  , width, height, clear, present, pollEvent, waitEventTimeout
  , invalidate, checkViewChange
  , toEvent, toEvents, bufferStr
  , padC, renderTable, print
  , _setScreenBufSize
  ) where

-- $setup
-- >>> :set -XOverloadedStrings

import Prelude hiding (init, print)
import Control.Monad (forM, forM_, unless, when, zipWithM_)
import Data.Bits ((.&.), (.|.), testBit)
import qualified Data.Bits
import Data.Char (chr)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.Int (Int32, Int64)
import qualified Data.IntSet as IS
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.ByteString.Builder as BSB
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Storable.Mutable as VSM
import Data.Word (Word8, Word16, Word32, Word64)
import Foreign.C.Types (CInt(..))
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Ptr (Ptr)
import Foreign.Storable (Storable(..))
import Control.Exception (SomeException, try)
import System.IO (stdin, stdout, hSetBuffering, hSetEcho, hFlush,
                  hIsTerminalDevice,
                  BufferMode(..), Handle, hGetChar, hWaitForInput)
import qualified GHC.IO.Handle.FD as HandleFD
import System.Posix.IO (stdInput, openFd, defaultFileFlags, OpenMode(ReadOnly))
import System.Posix.Types (Fd)
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
        [ (T.pack ("rgb" ++ show r ++ show g ++ show b), fromIntegral $ 16 + 36 * r + 6 * g + b)
        | r <- [0..5 :: Int], g <- [0..5 :: Int], b <- [0..5 :: Int]
        ]
      gray =
        [ (T.pack ("gray" ++ show i), fromIntegral $ 232 + i) | i <- [0..23 :: Int] ]
  in HM.fromList (ansi ++ cube ++ gray)

-- | Look up an xterm-256 color name, returning its palette index (0 = default/unknown).
--
-- >>> parseColor "default"
-- 0
-- >>> parseColor "black"
-- 16
-- >>> parseColor "red"
-- 1
-- >>> parseColor "white"
-- 7
-- >>> parseColor "brCyan"
-- 14
-- >>> parseColor "brWhite"
-- 15
-- >>> parseColor "rgb000"
-- 16
-- >>> parseColor "rgb555"
-- 231
-- >>> parseColor "rgb520"
-- 208
-- >>> parseColor "rgb234"
-- 110
-- >>> parseColor "gray0"
-- 232
-- >>> parseColor "gray23"
-- 255
-- >>> parseColor "gray4"
-- 236
-- >>> parseColor "rgb600"
-- 0
-- >>> parseColor "gray24"
-- 0
-- >>> parseColor "nosuchcolor"
-- 0
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

-- | Where the TUI reads keys and queries termios. Defaults to stdin / fd 0
-- for the normal case; 'reopenTty' swaps to a fresh /dev/tty handle when
-- stdin was consumed by a pipe (`ls | tv`). Lean solves this with
-- freopen("/dev/tty", "r", stdin), but Haskell can't resurrect a
-- semi-closed Handle — after hGetContents hits EOF, every subsequent
-- stdin op throws "handle is closed". Redirecting through a ref keeps
-- stdin untouched.
inputHandle :: IORef Handle
inputHandle = unsafePerformIO (newIORef stdin)
{-# NOINLINE inputHandle #-}

inputFd :: IORef Fd
inputFd = unsafePerformIO (newIORef stdInput)
{-# NOINLINE inputFd #-}

-- | isatty on the current input source.
isattyStdin :: IO Bool
isattyStdin = readIORef inputHandle >>= hIsTerminalDevice

-- | Open /dev/tty as the TUI's input handle/fd. Returns True on success.
reopenTty :: IO Bool
reopenTty = do
  r <- try $ do
    fd  <- openFd "/dev/tty" ReadOnly defaultFileFlags
    tty <- HandleFD.fdToHandle (fromIntegral fd)
    writeIORef inputHandle tty
    writeIORef inputFd fd
  pure $ case r :: Either SomeException () of
    Right _ -> True
    Left _  -> False

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
    inH  <- readIORef inputHandle
    inFd <- readIORef inputFd
    hSetBuffering inH NoBuffering
    hSetBuffering stdout NoBuffering
    hSetEcho inH False
    isTty <- hIsTerminalDevice inH
    -- cbreak-ish: ICANON off so bytes arrive unbuffered. Leaves OPOST on
    -- so NL→CRLF translation on stdout still works (termbox2 turns it off
    -- in cfmakeraw, but we don't emit bare `\n` during rendering).
    when isTty $ do
      orig <- PT.getTerminalAttributes inFd
      writeIORef origTios (Just orig)
      let ta = foldl PT.withoutMode orig
                 [ PT.ProcessInput, PT.EnableEcho, PT.EchoLF
                 , PT.KeyboardInterrupts, PT.ExtendedFunctions
                 , PT.StartStopOutput, PT.StartStopInput
                 ]
          ta' = PT.withMinInput (PT.withTime ta 0) 1
      PT.setTerminalAttributes inFd ta' PT.WhenFlushed
    (w, h) <- if isTty
                then termSize
                else pure (80, 24)
    -- Front buffer starts at all-default; termbox2's tb_init does the same,
    -- so the first `present` frame emits every non-default cell.
    allocBufs w h
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
    -- Byte-for-byte match of termbox2's tb_deinit (c/termbox2.h:3172)
    -- for the xterm-256color cap table (c/termbox2.h:918-934):
    -- SHOW_CURSOR, SGR0, CLEAR_SCREEN, EXIT_CA, EXIT_KEYPAD, EXIT_MOUSE.
    -- The CLEAR_SCREEN-before-EXIT_CA step matters: without it, alt-screen
    -- residue can leak into the main screen on some terminals, and fzf
    -- then renders inline on top of leftover tv content.
    TIO.hPutStr stdout $
      "\x1b[?12l\x1b[?25h"     -- SHOW_CURSOR: blink off + show
      <> "\x1b(B\x1b[m"         -- SGR0: charset + attr reset
      <> "\x1b[H\x1b[2J"        -- CLEAR_SCREEN: home + erase display
      <> "\x1b[?1049l\x1b[23;0;0t" -- EXIT_CA: leave alt screen + restore title
      <> "\x1b[?1l\x1b>"        -- EXIT_KEYPAD: cursor-keys off + keypad off
      <> "\x1b[?1006l\x1b[?1015l\x1b[?1002l\x1b[?1000l" -- EXIT_MOUSE
    inH  <- readIORef inputHandle
    inFd <- readIORef inputFd
    hSetEcho inH True
    hFlush stdout
    mOrig <- readIORef origTios
    case mOrig of
      Just orig -> PT.setTerminalAttributes inFd orig PT.WhenFlushed
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

allocBufs :: Int -> Int -> IO ()
allocBufs w h = do
  buf   <- VSM.replicate (w * h) emptyCell
  front <- VSM.replicate (w * h) emptyCell
  writeIORef screenBuf (w, h, buf)
  writeIORef frontBuf front

_setScreenBufSize :: Int -> Int -> IO ()
_setScreenBufSize = allocBufs

-- | Front buffer also resets on dim change — the diff in `present` indexes
-- by (w, h) that'd be invalid otherwise, forcing a full repaint.
syncSize :: IO ()
syncSize = do
  (curW, curH, _) <- readIORef screenBuf
  (newW, newH) <- termSize
  when (newW /= curW || newH /= curH) (allocBufs newW newH)

clear :: IO ()
clear = do
  syncSize
  (_, _, buf) <- readIORef screenBuf
  VSM.set buf emptyCell

-- | Force the next 'present' to re-emit every cell, including blanks.
-- Fills the front buffer with a sentinel cell that no real render can
-- produce (char = 0xFFFF is not in any glyph output), so the diff in
-- 'present' always fires — back cells that became 'emptyCell' also get
-- emitted as blanks, erasing any terminal residue from a previous view.
--
-- Used on picker exit and after view transitions where the new view's
-- paint footprint doesn't cover every cell the previous view drew.
invalidate :: IO ()
invalidate = do
  front <- readIORef frontBuf
  VSM.set front dirtyCell

-- | Cell value that can't collide with anything a renderer emits.
dirtyCell :: Cell
dirtyCell = Cell 0xFFFF 0 0

-- | Track the last-rendered view key (path + vkind string) and fire
-- 'invalidate' when it changes. Lets callers cheaply guard against
-- cross-view residue without threading a "previous state" around.
lastViewKey :: IORef Text
lastViewKey = unsafePerformIO (newIORef "")
{-# NOINLINE lastViewKey #-}

-- | Call from the render path with a stable identity for the current
-- view ('path|vkind' works). Triggers 'invalidate' on change, so the
-- next present emits every cell (including blanks) and cleans up any
-- residue left by the prior view's paint footprint.
checkViewChange :: Text -> IO ()
checkViewChange k = do
  prev <- readIORef lastViewKey
  when (prev /= k) $ do
    invalidate
    writeIORef lastViewKey k

-- | Fold state for `present`: last emitted style, last cursor position,
-- and the accumulated output Builder. Strict fields keep the builder
-- from stacking thunks across the w*h cells (render hot path).
-- ByteString.Builder (ASCII + charToWord8) beats Text.Builder here
-- because every byte we emit is ASCII — we avoid T.pack/show allocs
-- and the final TL.toStrict pass.
data PresentAcc = PresentAcc
  !(Maybe (Word32, Word32))  -- lastStyle
  !(Int, Int)                -- lastCursor
  !BSB.Builder               -- out

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
  unless headless $ do
    (w, h, buf) <- readIORef screenBuf
    front <- readIORef frontBuf
    -- One hPutBuilder per frame mirrors termbox2's tb_present flush —
    -- keeps the PTY recorder's `drain()` from slicing the frame across
    -- multiple cast frames, which breaks byte parity for multi-step demos.
    let s0 = PresentAcc Nothing (-1, -1) mempty
        go !acc !y !x !idx
          | y == h    = pure acc
          | x == w    = go acc (y + 1) 0 idx
          | otherwise = do
              acc' <- stepCell buf front acc y x idx
              go acc' y (x + 1) (idx + 1)
    PresentAcc _ _ outB <- go s0 0 0 0
    BSB.hPutBuilder stdout outB
    hFlush stdout

stepCell
  :: VSM.IOVector Cell
  -> VSM.IOVector Cell
  -> PresentAcc
  -> Int -> Int -> Int
  -> IO PresentAcc
stepCell buf front acc@(PresentAcc lastStyle lastCursor b) y x idx = do
  cell     <- VSM.read buf idx
  -- Previously we diffed against `front` and skipped unchanged cells,
  -- but that left scroll/width-shrink residue: a terminal cell that
  -- went from content → blank in back but matched the front-cached
  -- blank from an earlier frame got no emit, so the old pixels stayed
  -- on screen. Always emit every cell — a few KB extra per frame, cheap
  -- both locally and over SSH, and kills every residue class at once.
  do
      let Cell ch fg bg = cell
          style = (fg, bg)
          (b1, lastStyle') =
            if Just style /= lastStyle
              then (b <> sgrFor fg bg, Just style)
              else (b, lastStyle)
          b2 = if lastCursor /= (y, x)
                 then b1 <> cursorAt y x
                 else b1
          b3 = b2 <> BSB.charUtf8 (chr $ fromIntegral ch)
      VSM.write front idx cell
      pure (PresentAcc lastStyle' (y, x + 1) b3)

-- | Build the SGR + charset-reset prefix for a cell. Extracts attr bits
-- from `fg` before masking off the color. Matches Lean termbox2's output
-- byte-for-byte: `\x1b(B\x1b[m` (charset reset + SGR reset) then optional
-- bold/underline, then color codes. A 0 fg or bg is elided because the
-- preceding `\x1b[m` already set both to default — emitting `38;5;0` or
-- `48;5;0` after that would be a no-op but cost extra bytes.
sgrFor :: Word32 -> Word32 -> BSB.Builder
sgrFor fg bg =
  let color   = fg .&. 0x1FF
      bgColor = bg .&. 0x1FF
      bold    = if fg .&. 0x01000000 /= 0 then BSB.byteString "\x1b[1m" else mempty
      ul      = if fg .&. 0x02000000 /= 0 then BSB.byteString "\x1b[4m" else mempty
      colors  = case (color, bgColor) of
        (0, 0) -> mempty
        (f, 0) -> BSB.byteString "\x1b[38;5;" <> BSB.wordDec (fromIntegral f) <> BSB.char8 'm'
        (0, b) -> BSB.byteString "\x1b[48;5;" <> BSB.wordDec (fromIntegral b) <> BSB.char8 'm'
        (f, b) -> BSB.byteString "\x1b[38;5;" <> BSB.wordDec (fromIntegral f)
                    <> BSB.byteString ";48;5;" <> BSB.wordDec (fromIntegral b)
                    <> BSB.char8 'm'
  in BSB.byteString "\x1b(B\x1b[m" <> bold <> ul <> colors

cursorAt :: Int -> Int -> BSB.Builder
cursorAt y x =
  BSB.byteString "\x1b[" <> BSB.intDec (y + 1)
    <> BSB.char8 ';' <> BSB.intDec (x + 1) <> BSB.char8 'H'

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
  inH <- readIORef inputHandle
  c <- hGetChar inH
  if c /= '\x1B'
    then pure (toEvent c)
    else do
      more <- hWaitForInput inH 100
      if not more
        then pure (toEvent '\x1B')
        else do
          c2 <- hGetChar inH
          case c2 of
            '[' -> readCsi inH
            'O' -> do c3 <- hGetChar inH; pure (toEvents ['\x1B', 'O', c3])
            _   -> pure (toEvent '\x1B')
  where
    -- A CSI tail is either a single letter final byte (arrows, home, end)
    -- or any run of digit parameters terminated by `~` (pgup, pgdn, …).
    readCsi inH = do
      c <- hGetChar inH
      if c >= '0' && c <= '9'
        then readCsiTilde inH [c]
        else pure (toEvents ['\x1B', '[', c])
    readCsiTilde inH acc = do
      c <- hGetChar inH
      if c >= '0' && c <= '9'
        then readCsiTilde inH (c : acc)
        else pure (toEvents ('\x1B' : '[' : reverse acc ++ [c]))

-- | Poll for a key event, returning Nothing after @ms@ milliseconds of
-- no input. Lets callers run a periodic poll callback (e.g. the fuzzy
-- picker) alongside input reading without spinning.
waitEventTimeout :: Int -> IO (Maybe Event)
waitEventTimeout ms = do
  inH <- readIORef inputHandle
  ready <- hWaitForInput inH ms
  if not ready then pure Nothing else Just <$> pollEvent

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
    pure $ T.dropWhileEnd (== ' ') $ T.pack cs
  pure (T.intercalate "\n" rows)

-- | Write text + padding into the cell buffer at (x, y), padding to `len`.
padC :: Word32 -> Word32 -> Word32 -> Word32 -> Word32 -> Text -> Word8 -> IO ()
padC x y len fg bg s _attr = do
  (w, h, buf) <- readIORef screenBuf
  let xi = fromIntegral x
      yi = fromIntegral y
      li = fromIntegral len
      chars = T.unpack s
      padded = take li $ chars ++ repeat ' '
  when (yi >= 0 && yi < h) $ do
    zipWithM_
      (\i ch ->
        let cx = xi + i
        in when (cx >= 0 && cx < w) $
             VSM.write buf (yi * w + cx)
               (Cell (fromIntegral $ fromEnum ch) fg bg))
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

-- | printPad: write text with padding into screen buffer.
-- right=True: right-align (pad on left), right=False: left-align (pad on right).
-- Called ~40k times per frame during renderTable, so the loops walk
-- T.unpack's lazy list char-by-char with a strict index instead of
-- zipping against [0..] — kills the pair/cons allocations the profile
-- flagged as printPadBuf's cost.
printPadBuf :: VSM.IOVector Cell -> Int -> Int -> Int -> Int -> Int -> Word32 -> Word32 -> Text -> Bool -> IO ()
printPadBuf buf w h x y padW fg bg s right_ =
  let sLen = T.length s
      len  = min sLen padW
      pad  = padW - len
      spW  = fromIntegral (fromEnum ' ') :: Word32
      textStart = if right_ then x + pad else x
      padStart  = if right_ then x       else x + len
      writeSpaces !cx !n
        | n <= 0    = pure ()
        | otherwise = do
            setCell buf w h cx y spW fg bg
            writeSpaces (cx + 1) (n - 1)
      writeText !cx !n cs0 = goT 0 cs0
        where
          goT !i (c:cs) | i < n = do
            setCell buf w h (cx + i) y (fromIntegral $ fromEnum c) fg bg
            goT (i + 1) cs
          goT _ _ = pure ()
  in do
    writeSpaces padStart pad
    writeText textStart len (T.unpack s)

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
  let nRows = if r1 > r0 then fromIntegral $ r1 - r0 else 0 :: Int
      sparkOn = V.any (not . T.null) sparklines
      stFg si = fromMaybe 0 $ styles V.!? (si * 2)
      stBg si = fromMaybe 0 $ styles V.!? (si * 2 + 1)
      toIS v = V.foldl' (\acc x -> IS.insert (fromIntegral x) acc) IS.empty v
      colBits = toIS selCols
      rowBits = toIS selRows
      hidBits = toIS hiddenCols
      nKeysI  = fromIntegral nKeys :: Int
      nCols   = V.length names
      (layoutV2, visKeys, baseWidthsV) =
        layoutCols screenW nCols texts inWidths colIdxs nKeysI hidBits
                   curCol colOff0 widthAdj
      heatCols = scanHeat heatMode layoutV2 colIdxs fmts colTypes texts heatDoubles
      yFoot    = screenH - 3
      dataY0   = if sparkOn then 2 else 1

  drawHeader buf screenW screenH yFoot layoutV2 visKeys colIdxs nKeysI
             names fmts colTypes colBits curCol stFg stBg
  when sparkOn $
    drawSpark buf screenW screenH layoutV2 visKeys colIdxs sparklines stFg stBg
  drawData buf screenW screenH dataY0 nRows r0 curRow curCol rowBits colBits
           layoutV2 visKeys colIdxs nKeysI texts colTypes heatMode heatCols
           heatDoubles stFg stBg
  drawTip buf screenW screenH layoutV2 colIdxs curCol moveDir names stFg stBg

  pure $ V.map fromIntegral baseWidthsV

-- | Compute per-column layout: pin key columns at the left, lay out the
-- remaining display columns starting at `colOff0`, and expand the cursor
-- column to absorb trailing slack. Returns `(layout, visKeys, baseWidths)`
-- where `layout` is `(displayIdx, xPos, width)` per visible column and
-- `baseWidths` is the pre-widthAdj width for every original column
-- (returned so `renderTable` can hand it back as the widths cache).
layoutCols
  :: Int                    -- screenW
  -> Int                    -- nCols (= V.length names, for widths cache size)
  -> Vector (Vector Text)   -- texts
  -> Vector Word64          -- inWidths
  -> Vector Word64          -- colIdxs
  -> Int                    -- nKeysI
  -> IS.IntSet              -- hidBits
  -> Word64                 -- curCol
  -> Word64                 -- colOff0
  -> Int64                  -- widthAdj
  -> (Vector (Int, Int, Int), Int, Vector Int)
layoutCols screenW nCols texts inWidths colIdxs nKeysI hidBits curCol colOff0 widthAdj =
  (layoutV2, visKeys, baseWidthsV)
  where
    (baseWidthsV, allWidthsV) = colWidths nCols texts inWidths hidBits widthAdj
    adjColOff = scrollOff screenW allWidthsV colIdxs nKeysI curCol colOff0
    layoutV   = placeCols screenW allWidthsV colIdxs nKeysI adjColOff
    layoutV2  = expandCur screenW layoutV baseWidthsV colIdxs curCol widthAdj
    visKeys   = V.length $ V.takeWhile (\(d, _, _) -> d < nKeysI) layoutV

-- | Phase 1 of layout: compute base (data-driven) and display (clamped +
-- `widthAdj`) widths for every original column. Hidden columns collapse
-- to 0 base / 3 display. `baseWidthsV` is returned so renderTable can
-- cache it; `allWidthsV` feeds scroll/placement.
colWidths
  :: Int -> Vector (Vector Text) -> Vector Word64 -> IS.IntSet -> Int64
  -> (Vector Int, Vector Int)
colWidths nCols texts inWidths hidBits widthAdj = (baseWidthsV, allWidthsV)
  where
    dataWidth c =
      let col = if c < V.length texts then texts V.! c else V.empty
      in V.foldl' (\acc t -> max acc (T.length t)) 1 col
    baseWidthsV = V.generate nCols $ \c ->
      if IS.member c hidBits then 0
      else let cached = if c < V.length inWidths
                        then fromIntegral $ inWidths V.! c else 0 :: Int
               dw   = dataWidth c
               base = max dw _MIN_HDR_WIDTH + 2
           in max base cached
    allWidthsV = V.generate nCols $ \c ->
      if IS.member c hidBits then 3
      else let base = baseWidthsV V.! c
               disp = min base _MAX_DISP_WIDTH
               w    = disp + fromIntegral widthAdj
           in max w 3

-- | Phase 2: scroll the non-key window right until the cursor column fits
-- on screen. Key columns are pinned at the left and never scrolled.
scrollOff
  :: Int -> Vector Int -> Vector Word64 -> Int -> Word64 -> Word64 -> Int
scrollOff screenW allWidthsV colIdxs nKeysI curCol colOff0 =
  go (fromIntegral colOff0) curDispIdx0 scrollW
  where
    nDispCols = V.length colIdxs
    keyWidth = sum [ (allWidthsV V.! fromIntegral (colIdxs V.! c)) + 1
                   | c <- [0 .. min nKeysI nDispCols - 1] ]
    -- cursor's index among the non-key display slots; 0 if not found
    curDispIdx0 = head $
      [ c - nKeysI | c <- [nKeysI .. nDispCols - 1]
                   , fromIntegral (colIdxs V.! c) == curCol ] ++ [0]
    scrollW = max 1 (screenW - keyWidth)
    go co curDI sw
      | curDI < co = go curDI curDI sw
      | otherwise  =
          let cumX = sum [ (allWidthsV V.! fromIntegral (colIdxs V.! (nKeysI + c'))) + 1
                         | c' <- [co .. curDI], nKeysI + c' < nDispCols ]
          in if cumX <= sw || co >= curDI then co else go (co + 1) curDI sw

-- | Phase 3: place key prefix at x=0, then non-key columns starting from
-- `adjColOff`. Returns `(displayIdx, xPos, width)` per visible column.
placeCols
  :: Int -> Vector Int -> Vector Word64 -> Int -> Int
  -> Vector (Int, Int, Int)
placeCols screenW allWidthsV colIdxs nKeysI adjColOff =
  V.fromList $ reverse keyList ++ reverse nonKeyList
  where
    nDispCols = V.length colIdxs
    -- shared column-placement step; terminates on display-index bound
    place limit c x acc
      | c >= limit || c >= nDispCols || x >= screenW = (acc, x)
      | otherwise =
          let origIdx = fromIntegral $ colIdxs V.! c
              cw = min (allWidthsV V.! origIdx) $ screenW - x
          in place limit (c + 1) (x + cw + 1) ((c, x, cw) : acc)
    (keyList, x1)   = place nKeysI 0 0 []
    (nonKeyList, _) = place nDispCols (nKeysI + adjColOff) x1 []

-- | Phase 4: if there's trailing horizontal slack, grow the cursor column
-- toward its base width and shift everything to its right by that delta.
expandCur
  :: Int -> Vector (Int, Int, Int) -> Vector Int -> Vector Word64
  -> Word64 -> Int64 -> Vector (Int, Int, Int)
expandCur screenW layoutV baseWidthsV colIdxs curCol widthAdj
  | nVisCols == 0 = layoutV
  | slack <= 0    = layoutV
  | otherwise = case curVisIdx of
      Nothing  -> layoutV
      Just cvi ->
        let (_, _, cw) = layoutV V.! cvi
            origIdx    = fromIntegral $ colIdxs V.! (fst3 $ layoutV V.! cvi)
            base       = max 3 $ baseWidthsV V.! origIdx + fromIntegral widthAdj
            expand     = min slack $ max 0 $ base - cw
        in if expand <= 0 then layoutV
           else V.imap (\i (d, xx, ww) ->
                  if      i == cvi then (d, xx, ww + expand)
                  else if i >  cvi then (d, xx + expand, ww)
                  else                  (d, xx, ww)
                ) layoutV
  where
    nVisCols = V.length layoutV
    (_, lastX, lastW) = layoutV V.! (nVisCols - 1)
    slack = screenW - (lastX + lastW + 1)
    curVisIdx = V.findIndex
      (\(di, _, _) -> fromIntegral (colIdxs V.! di) == curCol) layoutV
    fst3 (a, _, _) = a

-- | Heat scan: for each visible column, determine whether it's numeric,
-- date-like, or categorical string, and compute the min/max needed for
-- colorization. Skipped (empty) when heatMode is 0 or too many columns
-- are visible (>256), matching the C renderer's upper bound.
scanHeat
  :: Word8
  -> Vector (Int, Int, Int)      -- layoutV2
  -> Vector Word64               -- colIdxs
  -> Vector Char                 -- fmts
  -> Vector ColType              -- colTypes
  -> Vector (Vector Text)        -- texts
  -> Vector (Vector Double)      -- heatDoubles
  -> Vector HeatCol
scanHeat heatMode layoutV2 colIdxs fmts colTypes texts heatDoubles
  | heatMode == 0 || nVisCols > 256 = V.empty
  | otherwise = V.generate nVisCols $ \c ->
      let (dispIdx, _, _) = layoutV2 V.! c
          origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
          fmt = if origIdx < V.length fmts then fmts V.! origIdx else '\0'
          typ = fromMaybe ColTypeOther $ colTypes V.!? origIdx
          textCol = if origIdx < V.length texts then texts V.! origIdx else V.empty
          hdCol = if origIdx < V.length heatDoubles then heatDoubles V.! origIdx else V.empty
      in if isNumeric typ then
           let (mn, mx) = V.foldl' (\(lo, hi) v ->
                  if isNaN v then (lo, hi)
                  else (min lo v, max hi v)) (1e308, -1e308) hdCol
           in if mx > mn then HeatCol HeatNum mn mx False else heatColNone
         else if isDateFmt fmt then
           let (mn, mx) = V.foldl' (\(lo, hi) t ->
                  if T.null t then (lo, hi)
                  else let v = heatDateToNum t in (min lo v, max hi v))
                  (1e308, -1e308) textCol
           in if mx > mn then HeatCol HeatNum mn mx True else heatColNone
         else -- string heat is active iff the column has 2+ distinct values
           case V.find (not . T.null) textCol of
             Nothing -> heatColNone
             Just f
               | V.any (\t -> not (T.null t) && t /= f) textCol
                   -> HeatCol HeatStr 0 0 False
               | otherwise -> heatColNone
  where nVisCols = V.length layoutV2

-- | Paint the header row (y=0) and footer row (y=screenH-3). Both rows
-- carry column names, a trailing type char, and a key/non-key separator.
-- Footer mirrors the header so scroll position is visible top and bottom.
drawHeader
  :: VSM.IOVector Cell -> Int -> Int -> Int
  -> Vector (Int, Int, Int) -> Int
  -> Vector Word64 -> Int
  -> Vector Text -> Vector Char -> Vector ColType
  -> IS.IntSet -> Word64
  -> (Int -> Word32) -> (Int -> Word32)
  -> IO ()
drawHeader buf screenW screenH yFoot layoutV2 visKeys colIdxs nKeysI
           names fmts colTypes colBits curCol stFg stBg =
  forM_ [0 .. V.length layoutV2 - 1] $ \c -> do
    let (dispIdx, xPos, cw) = layoutV2 V.! c
        origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
        name  = fromMaybe "" $ names V.!? origIdx
        isSel = IS.member origIdx colBits
        isCur = origIdx == fromIntegral curCol
        isGrp = dispIdx < nKeysI
        si
          | isCur = _STYLE_CURSOR
          | isSel = _STYLE_SEL_COL
          | isGrp = _STYLE_GROUP
          | otherwise = _STYLE_HEADER
        fg = stFg si .|. _TB_BOLD .|. _TB_UNDERLINE
        bg = if isGrp then stBg _STYLE_GROUP else stBg si
        sp = fromIntegral $ fromEnum ' '
        tc = fromIntegral . fromEnum $
             if origIdx < V.length fmts
             then typeCharFmt (fmts V.! origIdx)
             else typeCharColType (fromMaybe ColTypeOther $ colTypes V.!? origIdx)
        hw = max 0 (cw - 2)
    setCell buf screenW screenH xPos 0     sp fg bg
    setCell buf screenW screenH xPos yFoot sp fg bg
    when (hw > 0) $ do
      printPadBuf buf screenW screenH (xPos + 1) 0     hw fg bg name False
      printPadBuf buf screenW screenH (xPos + 1) yFoot hw fg bg name False
    setCell buf screenW screenH (xPos + cw - 1) 0     tc fg bg
    setCell buf screenW screenH (xPos + cw - 1) yFoot tc fg bg
    drawSep buf screenW screenH 0     (xPos + cw) (c + 1 == visKeys) stFg stBg
    drawSep buf screenW screenH yFoot (xPos + cw) (c + 1 == visKeys) stFg stBg

-- | Paint one vertical separator cell at (x, y). `isKey=True` draws the
-- heavier `║` between the pinned-key block and the scrollable section;
-- otherwise a thin `│` between adjacent columns.
drawSep
  :: VSM.IOVector Cell -> Int -> Int -> Int -> Int -> Bool
  -> (Int -> Word32) -> (Int -> Word32) -> IO ()
drawSep buf screenW screenH y sX isKey stFg stBg =
  when (sX < screenW) $
    setCell buf screenW screenH sX y sc sf (stBg _STYLE_DEFAULT)
  where
    sc = if isKey then 0x2551 else 0x2502
    sf = if isKey then stFg _STYLE_GROUP else _SEP_FG

-- | Paint the sparkline row at y=1. Always uses the header style so it
-- reads as metadata, not data. Column separators follow the same
-- key/non-key rule as the header above.
drawSpark
  :: VSM.IOVector Cell -> Int -> Int
  -> Vector (Int, Int, Int) -> Int
  -> Vector Word64 -> Vector Text
  -> (Int -> Word32) -> (Int -> Word32)
  -> IO ()
drawSpark buf screenW screenH layoutV2 visKeys colIdxs sparklines stFg stBg = do
  let spFg = stFg _STYLE_HEADER
      spBg = stBg _STYLE_DEFAULT
  forM_ [0 .. V.length layoutV2 - 1] $ \c -> do
    let (dispIdx, xPos, cw) = layoutV2 V.! c
        origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
        sp = fromMaybe "" $ sparklines V.!? origIdx
    printPadBuf buf screenW screenH xPos 1 cw spFg spBg sp False
    drawSep buf screenW screenH 1 (xPos + cw) (c + 1 == visKeys) stFg stBg

-- | Paint every data row. Each cell picks its style from cursor/selection
-- state, optionally overlaid by heat coloring, then draws leading space,
-- content, trailing space, and separator — matching the C renderer's
-- cell layout exactly.
drawData
  :: VSM.IOVector Cell -> Int -> Int -> Int -> Int
  -> Word64 -> Word64 -> Word64
  -> IS.IntSet -> IS.IntSet
  -> Vector (Int, Int, Int) -> Int
  -> Vector Word64 -> Int
  -> Vector (Vector Text) -> Vector ColType
  -> Word8 -> Vector HeatCol -> Vector (Vector Double)
  -> (Int -> Word32) -> (Int -> Word32)
  -> IO ()
drawData buf screenW screenH dataY0 nRows r0 curRow curCol rowBits colBits
         layoutV2 visKeys colIdxs nKeysI texts colTypes heatMode heatCols
         heatDoubles stFg stBg =
  forM_ [0 .. nRows - 1] $ \ri -> do
    let row      = fromIntegral r0 + ri
        y        = ri + dataY0
        isSelRow = IS.member (fromIntegral row) rowBits
        isCurRow = fromIntegral row == curRow
    forM_ [0 .. V.length layoutV2 - 1] $ \c -> do
      let (dispIdx, xPos, cw) = layoutV2 V.! c
          origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
          isSel   = IS.member origIdx colBits
          isCurCol = origIdx == fromIntegral curCol
          isGrp   = dispIdx < nKeysI
          si      = getStyleIdx (isCurRow && isCurCol) isSelRow isSel isCurRow isCurCol
          bgBase  = if isGrp then stBg _STYLE_GROUP else stBg si
          fgBase  = stFg si
          (fg, bg) = pickHeat heatMode heatCols c si origIdx ri
                              texts heatDoubles fgBase bgBase
          textCol  = if origIdx < V.length texts then texts V.! origIdx else V.empty
          cellText = fromMaybe "" $ textCol V.!? ri
          isNum    = maybe False isNumeric $ colTypes V.!? origIdx
          sp       = fromIntegral $ fromEnum ' '
          contentW = max 0 (cw - 2)
      setCell buf screenW screenH xPos y sp fg bg
      when (contentW > 0) $
        printPadBuf buf screenW screenH (xPos + 1) y contentW fg bg cellText isNum
      setCell buf screenW screenH (xPos + cw - 1) y sp fg bg
      drawSep buf screenW screenH y (xPos + cw) (c + 1 == visKeys) stFg stBg

-- | Decide whether to override a cell's (fg, bg) with a heat color.
-- Cursor / selected rows keep their base style so the selection overlay
-- stays readable on top of heat. Returns the base pair otherwise.
pickHeat
  :: Word8 -> Vector HeatCol -> Int -> Int -> Int -> Int
  -> Vector (Vector Text) -> Vector (Vector Double)
  -> Word32 -> Word32
  -> (Word32, Word32)
pickHeat heatMode heatCols c si origIdx ri texts heatDoubles fgBase bgBase
  | heatMode == 0 || V.null heatCols           = base
  -- Preserve the row / selection overlays on top of heat: otherwise
  -- the cursor row disappears into the gradient and navigation cues
  -- vanish. Adding _STYLE_CUR_ROW here is the /bugfix for "curRow
  -- not obvious in heat mode".
  | si == _STYLE_CURSOR
    || si == _STYLE_SEL_ROW
    || si == _STYLE_SEL_CUR
    || si == _STYLE_CUR_ROW                    = base
  | otherwise = case heatCols V.!? c of
      Nothing -> base
      Just hc
        | hkKind hc == HeatNone                        -> base
        | hkKind hc == HeatNum && not (testBit heatMode 0) -> base
        | hkKind hc == HeatStr && not (testBit heatMode 1) -> base
        | hkKind hc == HeatNum && hkDate hc ->
            if T.null cellText then base
            else let v = heatDateToNum cellText
                     t = (v - hkMn hc) / (hkMx hc - hkMn hc)
                 in (_HEAT_FG, heatColor t)
        | hkKind hc == HeatNum ->
            let hdCol = if origIdx < V.length heatDoubles
                        then heatDoubles V.! origIdx else V.empty
                val   = fromMaybe (0/0) $ hdCol V.!? ri
            in if isNaN val then base
               else let t = (val - hkMn hc) / (hkMx hc - hkMn hc)
                    in (_HEAT_FG, heatColor t)
        | otherwise ->  -- HeatStr
            if T.null cellText then base
            else (_HEAT_FG, heatColor $ heatStrHash01 cellText)
  where
    base = (fgBase, bgBase)
    textCol  = if origIdx < V.length texts then texts V.! origIdx else V.empty
    cellText = fromMaybe "" $ textCol V.!? ri

-- | Paint the truncated-header tooltip for the cursor column. When moveDir
-- is positive (cursor moved right) the tip is anchored to the right edge
-- of the column, otherwise to the left — so the reveal scrolls in the
-- direction the user is navigating.
drawTip
  :: VSM.IOVector Cell -> Int -> Int
  -> Vector (Int, Int, Int) -> Vector Word64
  -> Word64 -> Int64 -> Vector Text
  -> (Int -> Word32) -> (Int -> Word32)
  -> IO ()
drawTip buf screenW screenH layoutV2 colIdxs curCol moveDir names stFg stBg =
  forM_ [0 .. V.length layoutV2 - 1] $ \c -> do
    let (dispIdx, xPos, cw) = layoutV2 V.! c
        origIdx = fromIntegral (colIdxs V.! dispIdx) :: Int
    when (origIdx == fromIntegral curCol) $ do
      let name    = fromMaybe "" $ names V.!? origIdx
          nameLen = T.length name
          colW    = cw - 2
      when (nameLen > colW) $ do
        let fg    = stFg _STYLE_CURSOR .|. _TB_BOLD .|. _TB_UNDERLINE
            cbg   = stBg _STYLE_CURSOR
            chars = T.unpack name
        if moveDir > 0 then
          let endX    = xPos + cw - 1
              startX  = max 0 $ endX - nameLen
              tipW    = endX - startX
              skip    = nameLen - tipW
          in forM_ (zip [0..] (take tipW (drop skip chars))) $ \(i, ch) ->
               setCell buf screenW screenH (startX + i) 0
                 (fromIntegral $ fromEnum ch) fg cbg
        else
          let tipW = min nameLen $ screenW - xPos - 1
          in forM_ (zip [0..] (take tipW chars)) $ \(i, ch) ->
               setCell buf screenW screenH (xPos + 1 + i) 0
                 (fromIntegral $ fromEnum ch) fg cbg
