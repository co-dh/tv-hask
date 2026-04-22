{-# LANGUAGE OverloadedStrings #-}
-- | Small utility glue: debug logging, per-process temp dir, OSC 52
-- clipboard copy, and kitty graphics protocol image emit. These were
-- four separate modules (`Tv.Log`, `Tv.Tmp`, `Tv.Clip`, `Tv.Kitty`)
-- that shared no helpers; they're consolidated here to flatten the
-- namespace. Consumers import this with the per-purpose alias they
-- always used (`import qualified Tv.Util as Log` etc.) so call sites
-- stay the same.
module Tv.Util where

import Tv.Prelude
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Char8 as BS8
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Control.Concurrent (myThreadId)
import Control.Exception (SomeException, try)
import Data.Char (isDigit)
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Data.Time.LocalTime (getCurrentTimeZone, utcToLocalTime)
import System.Directory
  (createDirectoryIfMissing, getTemporaryDirectory, removeFile, removePathForcibly)
import System.Environment (lookupEnv)
import System.Exit (ExitCode(..))
import System.IO (BufferMode (..), Handle, IOMode (..), hFlush, hPutStrLn, hSetBuffering, openFile, stdout)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Process (getProcessID)
import System.Process (readProcessWithExitCode)

-- ---- Log -----------------------------------------------------
-- Centralized error/debug logging to ~/.cache/tv/tv.log.

-- | Log dir — ~/.cache/tv/, works from any cwd (including CI)
logDir :: IORef String
logDir = unsafePerformIO $ do
  home <- fromMaybe "/tmp" <$> lookupEnv "HOME"
  let d = home ++ "/.cache/tv"
  createDirectoryIfMissing True d
  newIORef d
{-# NOINLINE logDir #-}

dir :: IO String
dir = readIORef logDir

path :: IO String
path = do
  d <- dir
  pure $ d ++ "/tv.log"

-- | Set C-side log path (stub: C FFI sink not wired in Haskell port)
setLog :: Text -> IO ()
setLog _ = pure ()

-- | Format timestamp HH:MM:SS.mmm (local time)
localTimestamp :: IO String
localTimestamp = do
  t <- getCurrentTime
  tz <- getCurrentTimeZone
  pure (formatTime defaultTimeLocale "%H:%M:%S%Q" (utcToLocalTime tz t))

timestamp :: IO String
timestamp = localTimestamp

-- One persistent append-handle per process. GHC's Handle is internally
-- mutex-protected, so concurrent writes from many threads are safe with
-- no extra locking on our side. LineBuffering flushes on each newline,
-- which (with O_APPEND) keeps cross-process writes atomic at the kernel
-- level — tagging each line with the PID lets us demux the merged stream.
logHandle :: Handle
logHandle = unsafePerformIO $ do
  p <- path
  h <- openFile p AppendMode
  hSetBuffering h LineBuffering
  pure h
{-# NOINLINE logHandle #-}

-- Cache PID once; it's fixed for the lifetime of the process.
pidStr :: String
pidStr = unsafePerformIO $ show <$> getProcessID
{-# NOINLINE pidStr #-}

-- | Write log entry
write :: Text -> Text -> IO ()
write tag msg = do
  ts <- timestamp
  hPutStrLn logHandle
    ("[" ++ ts ++ "] [pid=" ++ pidStr ++ "] [" ++ T.unpack tag ++ "] " ++ T.unpack msg)

errorLog :: Text -> IO ()
errorLog msg = write "error" msg

-- | Run command and log on failure; returns (exitCode, stdout, stderr)
run :: Text -> String -> [String] -> IO (ExitCode, String, String)
run tag cmd args = do
  r@(ec, _, se) <- readProcessWithExitCode cmd args ""
  case ec of
    ExitSuccess -> pure ()
    ExitFailure n ->
      write tag (T.pack (cmd ++ " " ++ unwords args ++ " -> exit " ++ show n ++ ": " ++ trimAscii se))
  pure r
  where
    trimAscii = T.unpack . T.strip . T.pack

-- ---- Tmp -----------------------------------------------------
-- Per-process temporary directory under <tmpdir>/tv-<pid>.

-- | Per-process temp dir: <tmpdir>/tv-<pid>. Unique per process (pid is
-- unique among live processes), so multiple tv runs don't collide.
tmpDir :: IORef String
tmpDir = unsafePerformIO $ do
  base <- getTemporaryDirectory
  pid <- getProcessID
  let d = base ++ "/tv-" ++ show pid
  createDirectoryIfMissing True d
  newIORef d
{-# NOINLINE tmpDir #-}

tmpPath :: String -> IO String
tmpPath name = do
  d <- readIORef tmpDir
  pure (d ++ "/" ++ name)

-- | Thread-scoped variant of tmpPath. Inserts "-<threadId>" before the
-- extension so parallel test threads don't clobber each other's files.
-- Production TUI is single-threaded, so all production callers land on
-- the same path within a run — behavior unchanged there.
threadPath :: String -> IO String
threadPath name = do
  tid <- myThreadId
  let tidStr = filter isDigit (show tid)
      (base, ext) = break (== '.') name
  tmpPath (base ++ "-" ++ tidStr ++ ext)

-- | Remove file, ignoring errors (file may not exist)
rmFile :: String -> IO ()
rmFile p = do
  r <- try (removeFile p) :: IO (Either SomeException ())
  case r of
    Left _  -> pure ()
    Right _ -> pure ()

cleanupTmp :: IO ()
cleanupTmp = do
  d <- readIORef tmpDir
  removePathForcibly d

-- ---- Clip ----------------------------------------------------
-- System-clipboard copy via OSC 52.
--
-- OSC 52 is the de-facto terminal escape for "put this text on the
-- clipboard"; the base64 payload rides the control stream, so it works
-- over SSH too — the terminal emulator at the end of the wire is what
-- touches the local clipboard.
--
-- Works in: kitty, WezTerm, iTerm2, xterm (with @disallowedWindowOps@
-- relaxed), Alacritty (as of 0.13), Ghostty, Contour.
--
-- Under tmux we wrap the sequence in DCS passthrough (same shape as
-- the kitty graphics block below) so tmux forwards the bytes to the
-- host terminal instead of eating them. Users still need tmux
-- @set -g set-clipboard on@ for tmux's own OSC 52 to be active, but
-- our wrapper only needs passthrough (allow-passthrough on).

-- | Copy text to the system clipboard via OSC 52.
copy :: Text -> IO ()
copy t = do
  inTmux <- isJust <$> lookupEnv "TMUX"
  let payload = B64.encode (TE.encodeUtf8 t)
      osc     = "\x1b]52;c;" <> payload <> "\x07"
      bytes
        | inTmux    = "\x1bPtmux;" <> escTmuxClip osc <> "\x1b\\"
        | otherwise = osc
  BS.hPut stdout bytes
  hFlush stdout
  where
    -- Inside tmux's DCS passthrough, every ESC byte is doubled so tmux
    -- forwards the literal ESC to the outer terminal instead of
    -- treating it as a passthrough terminator.
    escTmuxClip :: BS.ByteString -> BS.ByteString
    escTmuxClip = BS.intercalate "\x1b\x1b" . BS.split 0x1b

-- ---- Kitty ---------------------------------------------------
-- Kitty graphics protocol: transmit-and-display a PNG inline.
-- Replaces shelling out to `kitten icat` for terminals that support it
-- (kitty, WezTerm, ghostty). Format ref:
-- https://sw.kovidgoyal.net/kitty/graphics-protocol/

-- Maximum chunk size per APC sequence (kitty protocol caps at 4096
-- characters of base64 payload).
chunkBytes :: Int
chunkBytes = 4096

-- | Send a PNG to the terminal via the kitty graphics protocol. Splits
-- the base64-encoded payload into chunks framed by APC sequences.
-- Inside tmux, wraps each APC in a DCS passthrough envelope so tmux
-- forwards the bytes to the host terminal instead of stripping them.
--
-- The image is constrained to 22 rows × 78 cols (`r=22,c=78`) so it
-- leaves room below for the plot's downsample status bar even in the
-- common 24-row pane. Kitty resizes the image to fit those cells.
displayPng :: FilePath -> IO ()
displayPng pngPath = do
  bs <- BS.readFile pngPath
  inTmux <- isJust <$> lookupEnv "TMUX"
  let emit = emitWith inTmux
  case splitChunks chunkBytes (B64.encode bs) of
    []                -> pure ()
    [only]            -> emit "f=100,a=T,r=22,c=78" only
    (first : middles) -> do
      emit "f=100,a=T,r=22,c=78,m=1" first
      mapM_ (emit "m=1") (init middles)
      emit "m=0" (last middles)
  hFlush stdout
  where
    -- Build one APC chunk; if inside tmux, wrap in tmux's DCS passthrough
    -- (\x1bPtmux;<body>\x1b\\) with every embedded ESC doubled so tmux
    -- doesn't terminate the passthrough early.
    emitWith :: Bool -> String -> BS.ByteString -> IO ()
    emitWith inTmux ctrl chunk =
      let apc = "\x1b_G" <> BS8.pack ctrl <> ";" <> chunk <> "\x1b\\"
          payload = if inTmux then "\x1bPtmux;" <> escTmuxKitty apc <> "\x1b\\" else apc
      in BS.hPut stdout payload

    -- Double every ESC byte (0x1b) so the DCS-passthrough sequence reads
    -- the literal bytes through to the outer terminal.
    escTmuxKitty :: BS.ByteString -> BS.ByteString
    escTmuxKitty = BS.intercalate "\x1b\x1b" . BS.split 0x1b

-- | Delete all kitty graphics images from the terminal. Kitty stores
-- transmitted images in its own registry, independent of the screen
-- buffer; leaving the alt screen does not free them, so the plot
-- remains painted on top of the TUI after exit unless we ask kitty
-- to clear it explicitly. APC \\x1b_Ga=d\\x1b\\\\ ("action=delete,
-- default scope = all visible") is the documented call.
clearImages :: IO ()
clearImages = do
  inTmux <- isJust <$> lookupEnv "TMUX"
  let apc = "\x1b_Ga=d\x1b\\"
      payload | inTmux    = "\x1bPtmux;\x1b\x1b_Ga=d\x1b\x1b\\\x1b\\"
              | otherwise = apc
  BS.hPut stdout payload
  hFlush stdout

-- | Heuristic: detect terminals known to support the kitty graphics
-- protocol via env vars. Cheap and accurate for the common cases
-- (kitty, WezTerm, ghostty). iTerm2 uses a different image protocol
-- and isn't covered here — those users fall through to viu / xdg-open.
-- Override with TV_IMAGE_BACKEND=kitty (force-enable) or any other
-- value to force-disable.
supportsKittyGraphics :: IO Bool
supportsKittyGraphics = do
  forced <- lookupEnv "TV_IMAGE_BACKEND"
  case forced of
    Just "kitty" -> pure True
    Just _       -> pure False
    Nothing -> do
      anyKnown <- mapM lookupEnv
        [ "KITTY_WINDOW_ID"          -- kitty itself
        , "WEZTERM_PANE"             -- WezTerm
        , "GHOSTTY_RESOURCES_DIR"    -- ghostty
        ]
      pure (any isJust anyKnown)

-- | Pure chunker; exposed for doctest.
--
-- >>> import qualified Data.ByteString.Char8 as BS8
-- >>> map BS8.unpack (splitChunks 3 (BS8.pack "abcdefg"))
-- ["abc","def","g"]
-- >>> splitChunks 5 ""
-- []
splitChunks :: Int -> BS.ByteString -> [BS.ByteString]
splitChunks n bs
  | BS.null bs = []
  | otherwise  = let (h, t) = BS.splitAt n bs in h : splitChunks n t
