{-# LANGUAGE OverloadedStrings #-}
-- | Per-process temporary directory under /tmp/tv-XXXXXX.
module Tv.Tmp
  ( tmpDir
  , tmpPath
  , rmFile
  , cleanupTmp
  ) where

import Control.Exception (SomeException, try)
import Data.IORef (IORef, newIORef, readIORef)
import qualified Data.Text as T
import System.Directory (removeFile, removePathForcibly)
import System.IO.Unsafe (unsafePerformIO)
import System.Process (readProcessWithExitCode)

tmpDir :: IORef String
tmpDir = unsafePerformIO $ do
  (_, out, _) <- readProcessWithExitCode "mktemp" ["-d", "/tmp/tv-XXXXXX"] ""
  newIORef $ T.unpack $ T.strip $ T.pack out
{-# NOINLINE tmpDir #-}

tmpPath :: String -> IO String
tmpPath name = do
  d <- readIORef tmpDir
  pure (d ++ "/" ++ name)

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
