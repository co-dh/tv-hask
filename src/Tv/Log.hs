{-# LANGUAGE OverloadedStrings #-}
-- | Centralized error/debug logging to ~/.cache/tv/tv.log.
module Tv.Log where

import Data.IORef (IORef, newIORef, readIORef)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Data.Time.LocalTime (getCurrentTimeZone, utcToLocalTime)
import System.Directory (createDirectoryIfMissing)
import System.Environment (lookupEnv)
import System.IO (BufferMode (..), Handle, IOMode (..), hPutStrLn, hSetBuffering, openFile)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Process (getProcessID)
import System.Process (readProcessWithExitCode)
import System.Exit (ExitCode(..))

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
