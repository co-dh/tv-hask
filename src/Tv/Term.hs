{-
  termbox2 FFI bindings for TUI rendering

  Literal port of Tc/Tc/Term.lean. Lean uses @[extern] termbox2 bindings;
  Haskell has no termbox2 package, so this module routes the same API names
  to Haskell-native equivalents: System.Console.ANSI for clear/present/cursor,
  System.IO + hSetBuffering for raw-ish input, System.Posix.Terminal for
  isatty, and an IORef-backed screen cell buffer for width/height/print/buffer.
  The render side (renderTable) is stubbed until Render.hs is ported.
-}
{-# LANGUAGE OverloadedStrings #-}
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
  , width, height, clear, present, pollEvent, bufferStr
  , printPadC, renderTable, print
  ) where

import Prelude hiding (init, print)
import Control.Monad (forM_, when)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.Int (Int32, Int64)
import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef')
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8, Word16, Word32, Word64)
import System.IO (stdin, stdout, hSetBuffering, hSetEcho, hFlush,
                  hIsTerminalDevice, BufferMode(..), hGetChar)
import System.IO.Unsafe (unsafePerformIO)
import qualified System.Console.ANSI as ANSI
import Tv.Types (Column)

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
  { eventType :: Word8
  , eventMod  :: Word8
  , eventKeyCode :: Word16
  , eventCh   :: Word32
  , eventW    :: Word32  -- resize width
  , eventH    :: Word32  -- resize height
  } deriving (Eq, Show)

-- ============================================================================
-- Lifecycle / screen state
--
-- termbox2 has no Haskell binding. We back the screen by a minimal cell
-- buffer held in IORefs and drive the real terminal with ANSI escapes.
-- renderTable / bufferStr remain stubs until Render.hs is ported.
-- ============================================================================

-- Positional so -Wunused-top-binds stays quiet on fg/bg slots we don't
-- read yet (present/ bufferStr only use the character).
data Cell = Cell !Char !Word32 !Word32

cellCh :: Cell -> Char
cellCh (Cell c _ _) = c

emptyCell :: Cell
emptyCell = Cell ' ' 0 0

-- Flat row-major cell buffer: (w, h, cells) where cells is length w*h.
screenBuf :: IORef (Int, Int, V.Vector Cell)
screenBuf = unsafePerformIO (newIORef (0, 0, V.empty))
{-# NOINLINE screenBuf #-}

initedRef :: IORef Bool
initedRef = unsafePerformIO (newIORef False)
{-# NOINLINE initedRef #-}

-- | isatty(stdin)
isattyStdin :: IO Bool
isattyStdin = hIsTerminalDevice stdin

-- | Try to (re)open /dev/tty on stdin so the program can still drive a TUI
-- when its stdin was redirected. Returns True on success.
-- Approximation: we just report whether stdin is currently a tty.
reopenTty :: IO Bool
reopenTty = hIsTerminalDevice stdin

-- | Initialize terminal: set raw-ish mode, allocate screen buffer.
-- Returns 0 on success, negative on failure (mirrors termbox tb_init).
init :: IO Int32
init = do
  already <- readIORef initedRef
  if already then pure 0
  else do
    hSetBuffering stdin NoBuffering
    hSetBuffering stdout NoBuffering
    hSetEcho stdin False
    -- getTerminalSize queries the tty via an ANSI DSR escape + stdin read; on a
    -- redirected stdin it blocks on EOF. Fall back to 80x24 when stdin is not
    -- a tty so the -c test harness can run without a terminal attached.
    isTty <- hIsTerminalDevice stdin
    (w, h) <- if isTty
                then ANSI.getTerminalSize >>= \case
                  Just (rows, cols) -> pure (cols, rows)
                  Nothing           -> pure (80, 24)
                else pure (80, 24)
    writeIORef screenBuf (w, h, V.replicate (w * h) emptyCell)
    writeIORef initedRef True
    when isTty $ do
      ANSI.hideCursor
      ANSI.clearScreen
      ANSI.setCursorPosition 0 0
      hFlush stdout
    pure 0

inited :: IO Bool
inited = readIORef initedRef

shutdown :: IO ()
shutdown = do
  on <- readIORef initedRef
  when on $ do
    ANSI.showCursor
    ANSI.setSGR [ANSI.Reset]
    hSetEcho stdin True
    hFlush stdout
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
  (w, h, _) <- readIORef screenBuf
  writeIORef screenBuf (w, h, V.replicate (w * h) emptyCell)

-- | Flush cell buffer to the terminal.
present :: IO ()
present = do
  (w, h, cs) <- readIORef screenBuf
  ANSI.setCursorPosition 0 0
  forM_ [0 .. h - 1] $ \y -> do
    ANSI.setCursorPosition y 0
    forM_ [0 .. w - 1] $ \x -> do
      let c = cs V.! (y * w + x)
      putChar (cellCh c)
  hFlush stdout

-- | Poll a single key event. Approximation: read one char, no escape decoding,
-- no resize events. Higher layers (Key.hs) will sit on top of this.
pollEvent :: IO Event
pollEvent = do
  c <- hGetChar stdin
  pure Event
    { eventType = eventKey
    , eventMod = 0
    , eventKeyCode = 0
    , eventCh = fromIntegral (fromEnum c)
    , eventW = 0
    , eventH = 0
    }

-- | Read termbox internal cell buffer as string (rows separated by newlines).
-- Used by tests to assert on-screen content.
bufferStr :: IO Text
bufferStr = do
  (w, h, cs) <- readIORef screenBuf
  let row y = T.pack [ cellCh (cs V.! (y * w + x)) | x <- [0 .. w - 1] ]
  pure (T.intercalate "\n" [ row y | y <- [0 .. h - 1] ])

-- | Batch print with padding (C FFI - fast).
-- Writes s into the cell buffer at (x, y), padding to `len` with spaces.
printPadC :: Word32 -> Word32 -> Word32 -> Word32 -> Word32 -> Text -> Word8 -> IO ()
printPadC x y len fg bg s _attr = do
  (w, h, cs) <- readIORef screenBuf
  let xi = fromIntegral x
      yi = fromIntegral y
      li = fromIntegral len
      chars = T.unpack s
      padded = take li (chars ++ repeat ' ')
  when (yi >= 0 && yi < h) $ do
    let updates =
          [ (yi * w + (xi + i), Cell ch fg bg)
          | (i, ch) <- zip [0 ..] padded
          , xi + i >= 0, xi + i < w
          ]
    writeIORef screenBuf (w, h, cs V.// updates)

-- | Unified table render (C reads Column directly, computes widths if needed).
-- allCols, names, fmts, inWidths, colIdxs, nTotalRows, nKeys, colOff, r0, r1,
-- curRow, curCol, moveDir, selCols, selRows, styles, prec, widthAdj
-- fmts: format chars for type indicators (empty = use Column tag)
-- moveDir: -1 = moved left, 0 = none, 1 = moved right (for tooltip direction)
-- prec: float decimal count (0-17), widthAdj: column width offset
-- Returns computed widths (Array Nat)
--
-- STUB: the real render is ported in Tv.Render (L5). This definition exists
-- so the API surface matches Lean; callers wire up Render.hs once available.
renderTable
  :: Vector Column     -- allCols
  -> Vector Text       -- names
  -> Vector Char       -- fmts
  -> Vector Word64     -- inWidths
  -> Vector Word64     -- colIdxs
  -> Word64            -- nTotalRows
  -> Word64            -- nKeys
  -> Word64            -- colOff
  -> Word64            -- r0
  -> Word64            -- r1
  -> Word64            -- curRow
  -> Word64            -- curCol
  -> Int64             -- moveDir
  -> Vector Word64     -- selCols
  -> Vector Word64     -- selRows
  -> Vector Word64     -- styles (Array Nat in Lean)
  -> Vector Word32     -- (Array UInt32 in Lean — fourth style/color array)
  -> Int64             -- prec
  -> Int64             -- widthAdj
  -> Word8             -- (final UInt8 slot)
  -> Vector Text       -- trailing Array String
  -> IO (Vector Word64)
renderTable _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ = pure V.empty

-- | Print string at position (for backwards compat)
print :: Word32 -> Word32 -> Word32 -> Word32 -> Text -> IO ()
print x y fg bg s =
  printPadC x y (fromIntegral (T.length s)) fg bg s 0
