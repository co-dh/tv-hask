{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
-- | Unix-domain socket command channel.
--
-- Wire protocol: bind a Unix domain socket at /tmp/tv-<pid>.sock, spawn a
-- listener thread that accepts one-shot connections, reads up to 256 bytes
-- per connection, and stores the stripped command string in a single-slot
-- mutex-guarded buffer (`bufRef`). `pollCmd` is a destructive read — it
-- returns the current buffered command and clears it in one atomic step,
-- so main-loop polling consumes each command exactly once.
--
-- Implemented in pure Haskell via the `network` package (no C FFI) to avoid
-- a second extern, and because a userland thread is enough: tv's main loop
-- polls every tick, so the listener never needs to be faster than the
-- render cadence.
module Tv.Socket where

import Tv.Prelude
import Control.Concurrent (forkIO)
import Control.Exception (SomeException, try, bracket_)
import Control.Monad (forever)
import qualified Data.ByteString as BS
import Data.IORef (atomicModifyIORef')
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Encoding.Error as TEE
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import System.Environment (lookupEnv, setEnv)
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Process (getProcessID)

import qualified Tv.Util as Log
import Tv.Util (rmFile)

bufRef :: IORef Text
bufRef = unsafePerformIO (newIORef T.empty)
{-# NOINLINE bufRef #-}

listener :: IORef (Maybe NS.Socket)
listener = unsafePerformIO (newIORef Nothing)
{-# NOINLINE listener #-}

sockStart :: String -> IO Bool
sockStart p = do
  -- Stale socket file from a previous run: ignore errors, we'll rebind.
  rmFile p
  r <- try $ do
    s <- NS.socket NS.AF_UNIX NS.Stream NS.defaultProtocol
    NS.bind s (NS.SockAddrUnix p)
    NS.listen s 4
    _ <- forkIO (acceptLoop s)
    writeIORef listener (Just s)
    pure ()
  case r of
    Right () -> pure True
    Left (e :: SomeException) -> do
      Log.write "socket" (T.pack ("sockStart " ++ p ++ ": " ++ show e))
      pure False
  where
    acceptLoop s = forever $ do
      accRes <- try (NS.accept s) :: IO (Either SomeException (NS.Socket, NS.SockAddr))
      case accRes of
        Left _          -> pure ()
        Right (conn, _) -> do
          _ <- forkIO (handleConn conn)
          pure ()
    handleConn conn = do
      bs <- try (NSB.recv conn 256) :: IO (Either SomeException BS.ByteString)
      case bs of
        Right b ->
          let cmd = T.strip (TE.decodeUtf8With TEE.lenientDecode b)
          in writeIORef bufRef cmd
        Left _  -> pure ()
      void (try (NS.close conn) :: IO (Either SomeException ()))

sockPoll :: IO String
sockPoll = do
  cmd <- atomicModifyIORef' bufRef (T.empty,)
  pure (T.unpack cmd)

sockClose :: IO ()
sockClose = do
  m <- readIORef listener
  case m of
    Nothing -> pure ()
    Just s  -> do
      void (try (NS.close s) :: IO (Either SomeException ()))
      writeIORef listener Nothing

getPid :: IO Int
getPid = fromIntegral <$> getProcessID

sockPath :: IORef String
sockPath = unsafePerformIO (newIORef "")
{-# NOINLINE sockPath #-}

-- | Start socket listener, set TV_SOCK env var
socketInit :: IO ()
socketInit = do
  tmp <- fromMaybe "/tmp" <$> lookupEnv "TMPDIR"
  pid <- getPid
  let p = tmp ++ "/tv-" ++ show pid ++ ".sock"
  ok <- sockStart p
  if ok
    then do
      writeIORef sockPath p
      setEnv "TV_SOCK" p
    else
      Log.write "socket" (T.pack ("failed to start: " ++ p))

-- | Poll for pending command (empty string = nothing)
pollCmd :: IO (Maybe Text)
pollCmd = do
  s <- sockPoll
  pure $ if null s then Nothing else Just (T.pack s)

-- | Shutdown socket listener, cleanup
shutdown :: IO ()
shutdown = do
  sockClose
  writeIORef sockPath ""

-- | Run action with socket active (init before, shutdown after)
bracket :: Bool -> IO a -> IO a
bracket _test f = bracket_ socketInit shutdown f
