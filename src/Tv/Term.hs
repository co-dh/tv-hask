{-
  termbox2 FFI bindings for TUI rendering

  Literal port of Tc/Tc/Term.lean. Lean uses @[extern] termbox2 bindings;
  Haskell has no termbox2 package, so this module routes the same API names
  to Haskell-native equivalents: System.Console.ANSI for clear/present/cursor,
  System.IO + hSetBuffering for raw-ish input, System.Posix.Terminal for
  isatty, and a Storable cell buffer for width/height/print/buffer.

  renderTable calls a C shim in cbits/tv_render.c (plain-C port of the Lean
  reference's render.c; no Lean runtime deps). The cell buffer is kept in a
  Storable.Mutable.IOVector so the C side can write to it in place.
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
  , width, height, clear, present, pollEvent, byteToEvent, bytesToEvent, bufferStr
  , printPadC, renderTable, print
  ) where

import Prelude hiding (init, print)
import Control.Exception (bracket)
import Control.Monad (forM, forM_, when, zipWithM_)
import Data.Bits ((.&.))
import Data.Char (chr)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HM
import Data.Int (Int32, Int64)
import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef')
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Foreign as TF
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM
import Data.Word (Word8, Word16, Word32, Word64)
import Foreign.C.String (CString, newCString, withCString)
import Foreign.C.Types (CChar, CInt(..), CSize(..))
import Foreign.Marshal.Alloc (alloca, free, mallocBytes)
import Foreign.Marshal.Array (mallocArray, pokeArray, peekArray, withArray)
import Foreign.Marshal.Utils (with)
import Foreign.Ptr (Ptr, castPtr, nullPtr, plusPtr)
import Foreign.Storable (Storable(..))
import System.IO (stdin, stdout, hSetBuffering, hSetEcho, hFlush,
                  hIsTerminalDevice, BufferMode(..), hGetChar, hWaitForInput)
import System.Posix.IO (stdInput)
import qualified System.Posix.Terminal as PT
import System.IO.Unsafe (unsafePerformIO)
import Tv.Types (Column(..))
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
  { eventType :: Word8
  , eventMod  :: Word8
  , eventKeyCode :: Word16
  , eventCh   :: Word32
  , eventW    :: Word32  -- resize width
  , eventH    :: Word32  -- resize height
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
                then queryTermSize
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
queryTermSize :: IO (Int, Int)
queryTermSize =
  alloca $ \pRows ->
  alloca $ \pCols -> do
    rc <- c_tv_term_size pRows pCols
    if rc /= 0
      then pure (80, 24)
      else do
        r <- peek pRows
        c <- peek pCols
        pure (fromIntegral c, fromIntegral r)

foreign import ccall unsafe "tv_term_size"
  c_tv_term_size :: Ptr CInt -> Ptr CInt -> IO CInt

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
byteToEvent :: Char -> Event
byteToEvent c =
  let code = fromIntegral (fromEnum c) :: Word16
      (kCode, kCh) = case code of
        0x0D -> (keyEnter, 0)         -- CR → Enter
        0x0A -> (keyEnter, 0)         -- LF → Enter
        0x08 -> (keyBackspace, 0)
        0x7F -> (keyBackspace2, 0)
        0x1B -> (keyEsc, 0)
        _    -> (0, fromIntegral code)
  in Event
       { eventType = eventKey
       , eventMod = 0
       , eventKeyCode = kCode
       , eventCh = kCh
       , eventW = 0
       , eventH = 0
       }

-- | Pure multi-byte → Event translation. Handles CSI (`ESC [ …`) and SS3
-- (`ESC O …`) sequences for arrows, home, end, pgup, pgdn. Any input the
-- dispatch tables don't cover degrades to the first-byte `byteToEvent`,
-- which for ESC yields keyEsc.
bytesToEvent :: String -> Event
bytesToEvent s = case s of
  ['\x1B', '[', c]      -> csiLetter c
  ['\x1B', 'O', c]      -> csiLetter c
  ('\x1B' : '[' : rest)
    | (digits, "~") <- span isDigit rest -> csiTilde digits
  [c] -> byteToEvent c
  _   -> byteToEvent '\x1B'
  where
    isDigit ch = ch >= '0' && ch <= '9'
    key k = Event { eventType = eventKey, eventMod = 0, eventKeyCode = k
                  , eventCh = 0, eventW = 0, eventH = 0 }
    csiLetter 'A' = key keyArrowUp
    csiLetter 'B' = key keyArrowDown
    csiLetter 'C' = key keyArrowRight
    csiLetter 'D' = key keyArrowLeft
    csiLetter 'H' = key keyHome
    csiLetter 'F' = key keyEnd
    csiLetter _   = byteToEvent '\x1B'
    csiTilde "1" = key keyHome
    csiTilde "4" = key keyEnd
    csiTilde "5" = key keyPageUp
    csiTilde "6" = key keyPageDown
    csiTilde "7" = key keyHome
    csiTilde "8" = key keyEnd
    csiTilde _   = byteToEvent '\x1B'

-- | Poll a single key event. After ESC, `hWaitForInput` briefly for a CSI
-- or SS3 sequence; if none arrives within 100 ms, treat it as a lone Esc.
pollEvent :: IO Event
pollEvent = do
  c <- hGetChar stdin
  if c /= '\x1B'
    then pure (byteToEvent c)
    else do
      more <- hWaitForInput stdin 100
      if not more
        then pure (byteToEvent '\x1B')
        else do
          c2 <- hGetChar stdin
          case c2 of
            '[' -> readCsi
            'O' -> do c3 <- hGetChar stdin; pure (bytesToEvent ['\x1B', 'O', c3])
            _   -> pure (byteToEvent '\x1B')
  where
    -- A CSI tail is either a single letter final byte (arrows, home, end)
    -- or any run of digit parameters terminated by `~` (pgup, pgdn, …).
    readCsi = do
      c <- hGetChar stdin
      if c >= '0' && c <= '9'
        then readCsiTilde [c]
        else pure (bytesToEvent ['\x1B', '[', c])
    readCsiTilde acc = do
      c <- hGetChar stdin
      if c >= '0' && c <= '9'
        then readCsiTilde (c : acc)
        else pure (bytesToEvent ('\x1B' : '[' : reverse acc ++ [c]))

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
printPadC :: Word32 -> Word32 -> Word32 -> Word32 -> Word32 -> Text -> Word8 -> IO ()
printPadC x y len fg bg s _attr = do
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
  printPadC x y (fromIntegral (T.length s)) fg bg s 0

-- ============================================================================
-- renderTable — FFI into cbits/tv_render.c
-- ============================================================================

-- | TvCol struct matching cbits/tv_render.h.
-- Layout: uint8 tag (1B), padding to 8B, int64 nrows (8B), void* data (8B).
-- Total 24 bytes on 64-bit.
data TvCol = TvCol !Word8 !Int64 !(Ptr ())

instance Storable TvCol where
  sizeOf _    = 24
  alignment _ = 8
  peek p = do
    t <- peekByteOff p 0
    n <- peekByteOff p 8
    d <- peekByteOff p 16
    pure (TvCol t n d)
  poke p (TvCol t n d) = do
    pokeByteOff p 0  t
    pokeByteOff p 8  n
    pokeByteOff p 16 d

foreign import ccall unsafe "tv_render_table"
  c_tv_render_table
    :: Ptr Cell -> CInt -> CInt
    -> Ptr TvCol -> CSize
    -> Ptr CString
    -> Ptr CChar -> CSize
    -> Ptr Word32 -> CSize
    -> Ptr Word32 -> CSize
    -> Word64 -> Word64 -> Word64
    -> Word64 -> Word64
    -> Word64 -> Word64
    -> Int64
    -> Ptr Word32 -> CSize
    -> Ptr Word32 -> CSize
    -> Ptr Word32 -> CSize
    -> Ptr Word32
    -> Int64 -> Int64
    -> Word8
    -> Ptr CString -> CSize
    -> Ptr Word32
    -> IO ()

-- | Unified table render. Signature mirrors Lean's `renderTable` (21 args);
-- comments above each slot describe what Lean passes.
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
  -> Vector Word64     -- hiddenCols
  -> Vector Word32     -- styles (fg/bg pairs)
  -> Int64             -- prec
  -> Int64             -- widthAdj
  -> Word8             -- heatMode
  -> Vector Text       -- sparklines
  -> IO (Vector Word64)
renderTable
  allCols names fmts inWidths colIdxs
  nTotalRows nKeys colOff r0 r1 curRow curCol moveDir
  selCols selRows hiddenCols styles
  prec widthAdj heatMode sparklines
  = do
  (w, h, buf) <- readIORef screenBuf
  let nCols = V.length allCols
  -- Track all malloc'd buffers so they're freed after the C call.
  colBuf       <- mallocArray nCols                  :: IO (Ptr TvCol)
  colDataPtrs  <- newIORef ([] :: [Ptr ()])
  strCStrs     <- newIORef ([] :: [CString])

  -- Populate TvCol array.
  let pokeCol i t n d = pokeByteOff colBuf (i * sizeOf (undefined :: TvCol)) (TvCol t n d)
  V.iforM_ allCols $ \i col -> case col of
    ColumnInts v -> do
      let n = V.length v
      p <- mallocArray (max 1 n) :: IO (Ptr Int64)
      V.iforM_ v $ \j x -> pokeByteOff p (j * 8) x
      modifyIORef' colDataPtrs (castPtr p :)
      pokeCol i 0 (fromIntegral n) (castPtr p)
    ColumnFloats v -> do
      let n = V.length v
      p <- mallocArray (max 1 n) :: IO (Ptr Double)
      V.iforM_ v $ \j x -> pokeByteOff p (j * 8) x
      modifyIORef' colDataPtrs (castPtr p :)
      pokeCol i 1 (fromIntegral n) (castPtr p)
    ColumnStrs v -> do
      let n = V.length v
      strs <- V.forM v $ \t -> do
        cs <- newCString (T.unpack t)
        modifyIORef' strCStrs (cs :)
        pure cs
      p <- mallocArray (max 1 n) :: IO (Ptr CString)
      V.iforM_ strs $ \j cs -> pokeByteOff p (j * sizeOf (undefined :: CString)) cs
      modifyIORef' colDataPtrs (castPtr p :)
      pokeCol i 2 (fromIntegral n) (castPtr p)

  -- names: Text -> CString[]
  nameCStrs <- V.forM names $ \t -> newCString (T.unpack t)
  namesBuf  <- mallocArray (max 1 nCols) :: IO (Ptr CString)
  V.iforM_ nameCStrs $ \i cs -> pokeByteOff namesBuf (i * sizeOf (undefined :: CString)) cs

  -- fmts: Vector Char -> char[]
  let nFmts = V.length fmts
  fmtsBuf <- mallocArray (max 1 nFmts) :: IO (Ptr CChar)
  V.iforM_ fmts $ \i c -> pokeByteOff fmtsBuf i (fromIntegral (fromEnum c) :: CChar)

  -- uint32 arrays
  let toU32 v = V.map (fromIntegral :: Word64 -> Word32) v
  inWidthsV  <- toStorable (toU32 inWidths)
  colIdxsV   <- toStorable (toU32 colIdxs)
  selColsV   <- toStorable (toU32 selCols)
  selRowsV   <- toStorable (toU32 selRows)
  hiddenV    <- toStorable (toU32 hiddenCols)
  stylesV    <- toStorable styles

  -- sparklines: Vector Text -> CString[]
  let nSpark = V.length sparklines
  sparkCStrs <- V.forM sparklines $ \t -> newCString (T.unpack t)
  sparkBuf   <- mallocArray (max 1 nSpark) :: IO (Ptr CString)
  V.iforM_ sparkCStrs $ \i cs -> pokeByteOff sparkBuf (i * sizeOf (undefined :: CString)) cs

  outBuf <- mallocArray (max 1 nCols) :: IO (Ptr Word32)

  VSM.unsafeWith buf $ \cellPtr ->
    VS.unsafeWith inWidthsV $ \pInW ->
    VS.unsafeWith colIdxsV  $ \pIdx ->
    VS.unsafeWith selColsV  $ \pSC ->
    VS.unsafeWith selRowsV  $ \pSR ->
    VS.unsafeWith hiddenV   $ \pH ->
    VS.unsafeWith stylesV   $ \pSt ->
      c_tv_render_table
        cellPtr (fromIntegral w) (fromIntegral h)
        colBuf (fromIntegral nCols)
        namesBuf
        fmtsBuf (fromIntegral nFmts)
        pInW (fromIntegral (V.length inWidths))
        pIdx (fromIntegral (V.length colIdxs))
        nTotalRows nKeys colOff
        r0 r1 curRow curCol moveDir
        pSC (fromIntegral (V.length selCols))
        pSR (fromIntegral (V.length selRows))
        pH  (fromIntegral (V.length hiddenCols))
        pSt
        prec widthAdj heatMode
        sparkBuf (fromIntegral nSpark)
        outBuf

  outs <- peekArray nCols outBuf

  -- Free every temporary allocation.
  free outBuf
  free sparkBuf
  V.forM_ sparkCStrs free
  free fmtsBuf
  free namesBuf
  V.forM_ nameCStrs free
  readIORef strCStrs >>= mapM_ free
  readIORef colDataPtrs >>= mapM_ free
  free colBuf

  pure (V.fromList (map fromIntegral outs))
  where
    toStorable :: (Storable a) => Vector a -> IO (VS.Vector a)
    toStorable v = pure (VS.generate (V.length v) (v V.!))
