{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | In-process fuzzy picker.
--
-- The UX we replicate from the fzf subset we used to shell out to:
--
-- * prompt line              — editable with typing / backspace / Ctrl-U
-- * optional header line     — static, shown above the list
-- * fuzzy-filtered item list — incremental, case-insensitive, ranked
-- * arrow-key navigation     — up/down, Home/End, Ctrl-J/Ctrl-K, wrap
-- * Enter selects            — returns the raw line of the highlighted item,
--                              or (if 'printQuery') the current query when
--                              nothing matches
-- * Esc cancels              — returns ""
-- * 'onFocus' callback       — fires on every highlight change, replacing
--                              fzf's @--bind=focus:execute-silent(...)@ +
--                              socat shim
-- * 'poll' callback          — fires every ~30 ms while we wait for input,
--                              so the main loop can service background work
--
-- We use raw stdout for our own rendering, because 'Tv.Term' is shut down
-- before we run (caller flow: @Term.shutdown@; 'runPicker'; @Term.init@).
-- This matches what the shelled-out fzf did in inline (@--height@) mode.
module Tv.Fzf.Picker
  ( PickerOpts(..)
  , defaultOpts
  , runPicker
  ) where

import Control.Exception (bracket_, SomeException, try)
import Control.Monad (when, forM_)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import qualified Data.IntSet as IS
import Data.List (sortOn)
import Data.Ord (Down(..))
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.IO (hFlush, stdout, hSetBuffering, hSetEcho, hWaitForInput,
                  hGetChar, BufferMode(..), Handle, hIsTerminalDevice)
import qualified System.IO as IO
import System.IO.Unsafe (unsafePerformIO)
import qualified System.Posix.Terminal as PT
import System.Posix.IO (stdInput, openFd, defaultFileFlags, OpenMode(ReadOnly))
import System.Posix.Types (Fd)
import qualified GHC.IO.Handle.FD as HandleFD

import Tv.Fzf.Match (match)

-- | Options for one picker invocation. Mirrors the fzf flag subset we used.
data PickerOpts = PickerOpts
  { prompt     :: Text
  , header     :: Text
  , items      :: Vector Text
  , printQuery :: Bool
    -- ^ if True and user presses Enter on empty filter result, return the
    -- current query text instead of empty (fzf's @--print-query@).
  , withNth    :: Bool
    -- ^ if True, display only the 2nd tab-delimited field (fzf's
    -- @--with-nth=2.. --delimiter=\\t@); selection still returns the full line.
  , onFocus    :: Int -> Text -> IO ()
    -- ^ fires when the highlighted row changes. Called with (itemIdx, rawLine).
  , poll       :: IO ()
    -- ^ invoked every ~30 ms while waiting for input.
  , initial    :: Text
    -- ^ initial query text.
  }

defaultOpts :: PickerOpts
defaultOpts = PickerOpts
  { prompt     = "> "
  , header     = ""
  , items      = V.empty
  , printQuery = False
  , withNth    = False
  , onFocus    = \_ _ -> pure ()
  , poll       = pure ()
  , initial    = ""
  }

-- | Picker state threaded through keystrokes.
data PickerState = PickerState
  { psQuery  :: !Text
  , psCur    :: !Int                    -- ^ highlighted row among filtered items
  , psMatch  :: ![(Int, Int, [Int])]    -- ^ (orig idx, score, match positions)
  , psHeight :: !Int                    -- ^ popup row count
  }

-- | Run the picker. Returns the raw selected line, or empty on cancel /
-- empty filter without 'printQuery'.
runPicker :: PickerOpts -> IO Text
runPicker opts = do
  (inH, inFd, closeH) <- openTtyIn
  bracket_ (enterRaw inH inFd) (leaveRaw inFd closeH) $ do
    let its    = items opts
        popupH = min (max 3 (V.length its + 2)) 15
        q0     = initial opts
        ms0    = computeMatches q0 its
    writeReserve popupH
    let st0 = PickerState { psQuery = q0, psCur = 0, psMatch = ms0
                          , psHeight = popupH }
    drawFrame opts st0
    case ms0 of
      ((i, _, _) : _) -> onFocus opts i (its V.! i)
      _               -> pure ()
    loop inH opts st0

-- | Raw key events the picker cares about.
data Key
  = KChar Char | KEnter | KEsc | KBackspace
  | KUp | KDown | KHome | KEnd | KPgUp | KPgDn
  | KCtrlU | KCtrlK | KCtrlJ
  | KIgnore
  deriving (Eq, Show)

loop :: Handle -> PickerOpts -> PickerState -> IO Text
loop inH opts st = do
  k <- readKey inH (poll opts)
  case k of
    KEsc       -> leaveRegion (psHeight st) >> pure ""
    KEnter     -> do
      leaveRegion (psHeight st)
      pure (selection opts st)
    KChar c    -> typed opts st (psQuery st <> T.singleton c) >>= loop inH opts
    KBackspace -> typed opts st (dropLast (psQuery st))       >>= loop inH opts
    KCtrlU     -> typed opts st ""                            >>= loop inH opts
    KUp        -> moveCur opts st (-1)                        >>= loop inH opts
    KCtrlK     -> moveCur opts st (-1)                        >>= loop inH opts
    KDown      -> moveCur opts st 1                           >>= loop inH opts
    KCtrlJ     -> moveCur opts st 1                           >>= loop inH opts
    KPgUp      -> moveCur opts st (negate (psHeight st))      >>= loop inH opts
    KPgDn      -> moveCur opts st (psHeight st)               >>= loop inH opts
    KHome      -> setCur opts st 0                            >>= loop inH opts
    KEnd       -> setCur opts st (max 0 (length (psMatch st) - 1)) >>= loop inH opts
    KIgnore    -> loop inH opts st

dropLast :: Text -> Text
dropLast t = if T.null t then t else T.init t

typed :: PickerOpts -> PickerState -> Text -> IO PickerState
typed opts st q' = do
  let ms' = computeMatches q' (items opts)
      st' = st { psQuery = q', psMatch = ms', psCur = 0 }
  drawFrame opts st'
  fireFocus opts st'
  pure st'

moveCur :: PickerOpts -> PickerState -> Int -> IO PickerState
moveCur opts st delta = setCur opts st (psCur st + delta)

setCur :: PickerOpts -> PickerState -> Int -> IO PickerState
setCur opts st i = do
  let n   = length (psMatch st)
      i'  = if n == 0 then 0 else clamp 0 (n - 1) i
      st' = st { psCur = i' }
  drawFrame opts st'
  when (psCur st /= i') (fireFocus opts st')
  pure st'

clamp :: Int -> Int -> Int -> Int
clamp lo hi x = max lo (min hi x)

fireFocus :: PickerOpts -> PickerState -> IO ()
fireFocus opts st = case nth (psCur st) (psMatch st) of
  Just (i, _, _) -> onFocus opts i (items opts V.! i)
  Nothing        -> pure ()

nth :: Int -> [a] -> Maybe a
nth _ []     = Nothing
nth 0 (x:_)  = Just x
nth n (_:xs) = nth (n - 1) xs

-- | What Enter returns for a given state.
selection :: PickerOpts -> PickerState -> Text
selection opts st = case nth (psCur st) (psMatch st) of
  Just (i, _, _) -> items opts V.! i
  Nothing
    | printQuery opts -> psQuery st
    | otherwise       -> ""

-- | Re-run fuzzy filter. Empty query → all items, in original order,
-- score 0, no positions.
computeMatches :: Text -> Vector Text -> [(Int, Int, [Int])]
computeMatches q its
  | T.null q  = [ (i, 0, []) | i <- [0 .. V.length its - 1] ]
  | otherwise =
      let raw = [ (i, s, ps)
                | i <- [0 .. V.length its - 1]
                , Just (s, ps) <- [match q (its V.! i)] ]
      in sortOn (\(_, s, _) -> Down s) raw

-- Rendering --------------------------------------------------------------

-- | Reserve 'n' rows at the current cursor position. Emit that many
-- newlines so the terminal scrolls if needed, then move back up so the
-- caller draws into fresh rows.
writeReserve :: Int -> IO ()
writeReserve n = do
  TIO.putStr (T.replicate n "\n")
  TIO.putStr (T.pack ("\x1b[" ++ show n ++ "A\r"))
  hFlush stdout

-- | On exit, clear the reserved region so the main TUI can reinitialize
-- with a clean scroll area.
leaveRegion :: Int -> IO ()
leaveRegion h = do
  TIO.putStr "\r"
  forM_ [0 .. h - 1] $ \_ -> TIO.putStr "\x1b[2K\n"
  TIO.putStr (T.pack ("\x1b[" ++ show h ++ "A\r"))
  hFlush stdout

-- | Redraw the whole popup in place. No diffing — the region is tiny
-- (≤15 rows), so emitting ANSI clear-line per row is cheaper than diffing.
drawFrame :: PickerOpts -> PickerState -> IO ()
drawFrame opts st = do
  let h        = psHeight st
      hdr      = header opts
      hasHdr   = not (T.null hdr)
      listRows = h - (if hasHdr then 2 else 1)
      promptLn = prompt opts <> psQuery st
      scroll   = scrollOff (psCur st) listRows
      visible  = take listRows (drop scroll (psMatch st))
  TIO.putStr "\r"
  drawLine (ansiBrightCyan <> promptLn <> ansiReset <> "\x1b[0K")
  when hasHdr $
    drawLine (ansiDim <> hdr <> ansiReset <> "\x1b[0K")
  forM_ [0 .. listRows - 1] $ \r ->
    case nth r visible of
      Nothing            -> drawLine "\x1b[2K"
      Just (idx, _, poses) ->
        let selected = (scroll + r) == psCur st
            raw      = items opts V.! idx
            shown    = display (withNth opts) raw
            adj      = if withNth opts then adjustPositions raw poses else poses
        in drawLine (renderItem selected shown adj)
  let promptLen = T.length (prompt opts) + T.length (psQuery st)
  TIO.putStr (T.pack ("\x1b[" ++ show h ++ "A\r\x1b[" ++ show promptLen ++ "C"))
  hFlush stdout

-- | Strip the index field for display when 'withNth' is True.
display :: Bool -> Text -> Text
display True line = case T.splitOn "\t" line of
  _ : rest -> T.intercalate "\t" rest
  _        -> line
display False line = line

-- | Keep the highlighted row within the visible window by scrolling.
scrollOff :: Int -> Int -> Int
scrollOff cur listRows
  | listRows <= 0     = 0
  | cur < listRows    = 0
  | otherwise         = cur - listRows + 1

-- | When display hides the index-prefix column, shift match positions so
-- they point at the visible substring. Positions inside the hidden prefix
-- are dropped (can't highlight them anyway).
adjustPositions :: Text -> [Int] -> [Int]
adjustPositions raw poses = case T.findIndex (== '\t') raw of
  Nothing     -> poses
  Just tabIdx ->
    let cut = tabIdx + 1
    in [ p - cut | p <- poses, p >= cut ]

drawLine :: Text -> IO ()
drawLine t = TIO.putStr (t <> "\n")

-- | Render one item row: marker + highlighted positions.
renderItem :: Bool -> Text -> [Int] -> Text
renderItem selected shown poses =
  let ps     = IS.fromList poses
      marker = if selected then "> " else "  "
      pre    = if selected then ansiSelected else ""
      body   = T.pack $ concat
        [ (if IS.member i ps then T.unpack ansiMatch else "")
          ++ [c]
          ++ (if IS.member i ps
                then (if selected then T.unpack ansiSelected else T.unpack ansiReset)
                else "")
        | (i, c) <- zip [0..] (T.unpack shown) ]
  in pre <> marker <> body <> ansiReset <> "\x1b[0K"

ansiSelected, ansiMatch, ansiReset, ansiDim, ansiBrightCyan :: Text
ansiSelected   = "\x1b[7m"
ansiMatch      = "\x1b[1;33m"
ansiReset      = "\x1b[0m"
ansiDim        = "\x1b[2m"
ansiBrightCyan = "\x1b[1;36m"

-- Input ------------------------------------------------------------------

-- | Open /dev/tty for reading (so we still work when stdin is piped).
openTtyIn :: IO (Handle, Fd, IO ())
openTtyIn = do
  r <- try $ do
    fd <- openFd "/dev/tty" ReadOnly defaultFileFlags
    h  <- HandleFD.fdToHandle (fromIntegral fd)
    pure (h, fd)
  case r :: Either SomeException (Handle, Fd) of
    Right (h, fd) -> pure (h, fd, pure ())
    Left _        -> pure (IO.stdin, stdInput, pure ())

-- | Put the tty into cbreak-ish mode. Mirrors 'Tv.Term.init'.
enterRaw :: Handle -> Fd -> IO ()
enterRaw h fd = do
  hSetBuffering h NoBuffering
  hSetEcho h False
  isTty <- hIsTerminalDevice h
  when isTty $ do
    orig <- PT.getTerminalAttributes fd
    writeIORef rawSaved (Just orig)
    let ta = foldl PT.withoutMode orig
               [ PT.ProcessInput, PT.EnableEcho, PT.EchoLF
               , PT.KeyboardInterrupts, PT.ExtendedFunctions
               , PT.StartStopOutput, PT.StartStopInput
               ]
        ta' = PT.withMinInput (PT.withTime ta 0) 1
    PT.setTerminalAttributes fd ta' PT.WhenFlushed

leaveRaw :: Fd -> IO () -> IO ()
leaveRaw fd closeH = do
  mOrig <- readIORef rawSaved
  case mOrig of
    Just o  -> do
      _ <- try (PT.setTerminalAttributes fd o PT.WhenFlushed)
             :: IO (Either SomeException ())
      pure ()
    Nothing -> pure ()
  writeIORef rawSaved Nothing
  _ <- try closeH :: IO (Either SomeException ())
  pure ()

-- | Saved tty attrs so we can restore them on exit.
rawSaved :: IORef (Maybe PT.TerminalAttributes)
rawSaved = unsafePerformIO (newIORef Nothing)
{-# NOINLINE rawSaved #-}

-- | Block until a key is available or 30 ms elapses (so poll can fire).
readKey :: Handle -> IO () -> IO Key
readKey h pollCB = do
  ready <- hWaitForInput h 30
  if not ready
    then pollCB *> readKey h pollCB
    else do
      c <- hGetChar h
      parseKey h c

-- | Decode one key. ESC may start a CSI / SS3 sequence; use a short timeout
-- so a lone Esc doesn't hang.
parseKey :: Handle -> Char -> IO Key
parseKey h c = case c of
  '\x1B' -> do
    more <- hWaitForInput h 50
    if not more then pure KEsc
    else do
      c2 <- hGetChar h
      case c2 of
        '[' -> readCsi h
        'O' -> do c3 <- hGetChar h; pure (csi c3)
        _   -> pure KEsc
  '\x7F' -> pure KBackspace
  '\x08' -> pure KBackspace
  '\x0A' -> pure KEnter
  '\x0D' -> pure KEnter
  '\x15' -> pure KCtrlU
  '\x0B' -> pure KCtrlK
  '\x0E' -> pure KCtrlJ
  '\x09' -> pure KIgnore
  _
    | c >= ' ' && c /= '\x7F' -> pure (KChar c)
    | otherwise               -> pure KIgnore
  where
    readCsi inH = do
      c2 <- hGetChar inH
      if c2 >= '0' && c2 <= '9'
        then readTilde inH [c2]
        else pure (csi c2)
    readTilde inH acc = do
      c2 <- hGetChar inH
      if c2 >= '0' && c2 <= '9'
        then readTilde inH (c2 : acc)
        else pure (tildeKey (reverse acc))
    csi 'A' = KUp
    csi 'B' = KDown
    csi 'H' = KHome
    csi 'F' = KEnd
    csi _   = KIgnore
    tildeKey "5" = KPgUp
    tildeKey "6" = KPgDn
    tildeKey "1" = KHome
    tildeKey "4" = KEnd
    tildeKey "7" = KHome
    tildeKey "8" = KEnd
    tildeKey _   = KIgnore
