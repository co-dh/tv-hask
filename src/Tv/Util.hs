{-
  Util: small utility modules consolidated into one file.
  Contents: Log (error/debug logging), TmpDir (per-process temp dir),
  Socket (unix socket IPC), Remote (URI path ops).
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Util where

import Control.Exception (SomeException, try, bracket_)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Data.Time.LocalTime (getCurrentTimeZone, utcToLocalTime)
import System.Directory (createDirectoryIfMissing, removeFile, removePathForcibly)
import System.Environment (lookupEnv, setEnv)
import System.FilePath ((</>))
import System.IO (IOMode(..), hPutStrLn, withFile)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Process (getProcessID)
import System.Process (readProcessWithExitCode)
import System.Exit (ExitCode(..))

-- ============================================================================
-- Log: centralized error/debug logging
-- ============================================================================

-- | Log dir — ~/.cache/tv/, works from any cwd (including CI)
logDir :: IORef String
logDir = unsafePerformIO $ do
  home <- maybe "/tmp" id <$> lookupEnv "HOME"
  let d = home ++ "/.cache/tv"
  createDirectoryIfMissing True d
  newIORef d
{-# NOINLINE logDir #-}

dir :: IO String
dir = readIORef logDir

path :: IO String
path = do
  d <- dir
  pure (d ++ "/tv.log")

-- | Set C-side log path (stub: C FFI sink not wired in Haskell port)
setLogPath :: Text -> IO ()
setLogPath _ = pure ()

-- | Format timestamp HH:MM:SS.mmm (local time)
localTimestamp :: IO String
localTimestamp = do
  t <- getCurrentTime
  tz <- getCurrentTimeZone
  pure (formatTime defaultTimeLocale "%H:%M:%S%Q" (utcToLocalTime tz t))

-- | Format timestamp HH:MM:SS.mmm (local time)
timestamp :: IO String
timestamp = localTimestamp

-- | Write log entry
write :: Text -> Text -> IO ()
write tag msg = do
  p <- path
  ts <- timestamp
  withFile p AppendMode $ \h ->
    hPutStrLn h ("[" ++ ts ++ "] [" ++ T.unpack tag ++ "] " ++ T.unpack msg)

-- | Log error message
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

-- ============================================================================
-- TmpDir: per-process temporary directory
-- ============================================================================

tmpDir :: IORef String
tmpDir = unsafePerformIO $ do
  (_, out, _) <- readProcessWithExitCode "mktemp" ["-d", "/tmp/tv-XXXXXX"] ""
  newIORef (T.unpack (T.strip (T.pack out)))
{-# NOINLINE tmpDir #-}

tmpPath :: String -> IO String
tmpPath name = do
  d <- readIORef tmpDir
  pure (d ++ "/" ++ name)

-- | Remove file, ignoring errors (file may not exist)
tryRemoveFile :: String -> IO ()
tryRemoveFile p = do
  r <- try (removeFile p) :: IO (Either SomeException ())
  case r of
    Left _  -> pure ()
    Right _ -> pure ()

cleanupTmp :: IO ()
cleanupTmp = do
  d <- readIORef tmpDir
  removePathForcibly d

-- ============================================================================
-- Socket: unix socket command channel
-- ============================================================================

-- C externs in Lean; stubbed here (unix socket IPC not yet wired in Haskell port)
sockStart :: String -> IO Bool
sockStart _ = pure False

sockPollCmd :: IO String
sockPollCmd = pure ""

sockClose :: IO ()
sockClose = pure ()

setEnvVar :: String -> String -> IO ()
setEnvVar = setEnv

getPid :: IO Int
getPid = fromIntegral <$> getProcessID

-- | Global socket path
sockPath :: IORef String
sockPath = unsafePerformIO (newIORef "")
{-# NOINLINE sockPath #-}

-- | Start socket listener, set TV_SOCK env var
socketInit :: IO ()
socketInit = do
  tmp <- maybe "/tmp" id <$> lookupEnv "TMPDIR"
  pid <- getPid
  let p = tmp ++ "/tv-" ++ show pid ++ ".sock"
  ok <- sockStart p
  if ok
    then do
      writeIORef sockPath p
      setEnvVar "TV_SOCK" p
    else
      write "socket" (T.pack ("failed to start: " ++ p))

-- | Poll for pending command (empty string = nothing)
pollCmd :: IO (Maybe Text)
pollCmd = do
  s <- sockPollCmd
  pure (if null s then Nothing else Just (T.pack s))

-- | Get socket path (empty if not started)
getPath :: IO String
getPath = readIORef sockPath

-- | Shutdown socket listener, cleanup
shutdown :: IO ()
shutdown = do
  sockClose
  writeIORef sockPath ""

-- | Run action with socket active (init before, shutdown after)
bracket :: Bool -> IO a -> IO a
bracket _test f = bracket_ socketInit shutdown f

-- ============================================================================
-- Remote: shared path operations for URI-based remote backends
-- ============================================================================

-- | Join URI prefix with child name
joinRemote :: Text -> Text -> Text
joinRemote pfx name =
  if T.isSuffixOf "/" pfx then pfx <> name else pfx <> "/" <> name

-- | Strip trailing slash from path
stripSlash :: Text -> Text
stripSlash p =
  if T.isSuffixOf "/" p then T.take (T.length p - 1) p else p

-- | Get parent URI: drop last path component. Returns none at root (<= minParts components).
parent :: Text -> Int -> Maybe Text
parent pth minParts =
  let parts = T.splitOn "/" (stripSlash pth)
  in if length parts <= minParts
       then Nothing
       else Just (T.intercalate "/" (init parts) <> "/")

-- | Display name: last non-empty path component (preserves protocol-only paths)
dispName :: Text -> Text
dispName pth =
  let parts = filter (not . T.null) (T.splitOn "/" (stripSlash pth))
  in if length parts <= 1 then pth else last parts
