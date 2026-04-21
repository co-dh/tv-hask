-- | Per-process temporary directory under <tmpdir>/tv-<pid>.
module Tv.Tmp
  ( tmpDir
  , tmpPath
  , threadPath
  , rmFile
  , cleanupTmp
  ) where

import Tv.Prelude
import Control.Concurrent (myThreadId)
import Control.Exception (SomeException, try)
import Data.Char (isDigit)
import System.Directory
  (createDirectoryIfMissing, getTemporaryDirectory, removeFile, removePathForcibly)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Process (getProcessID)

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
