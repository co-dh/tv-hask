-- | Utilities: Log (error/debug logging), TmpDir (per-process temp dir),
-- Socket (unix socket IPC via network package), Remote (URI path ops).
module Tv.Util where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.IORef
import System.IO (IOMode(..), hSetBuffering, BufferMode(..))
import System.IO.Unsafe (unsafePerformIO)
import System.Directory (createDirectoryIfMissing, removeFile, getTemporaryDirectory, removeDirectoryRecursive)
import System.FilePath ((</>))
import System.Posix.Process (getProcessID)
import System.Posix.Env (setEnv)
import System.Process (readProcess)
import System.Environment (lookupEnv)
import Data.Time.Clock (getCurrentTime)
import Data.Time.LocalTime (utcToLocalZonedTime, zonedTimeToLocalTime)
import Data.Time.Format (formatTime, defaultTimeLocale)
import Control.Exception (try, SomeException, bracket)
import Control.Monad (void, unless)
import Network.Socket (Socket, Family(..), SocketType(..), SockAddr(..), socket, bind, listen, accept, close, defaultProtocol)
import qualified Network.Socket.ByteString as NSB
import Tv.Eff (Eff, IOE, (:>), liftIO, runEff)

-- ============================================================================
-- Log
-- ============================================================================

{-# NOINLINE logDirRef #-}
logDirRef :: IORef FilePath
logDirRef = unsafePerformIO $ do
  home <- maybe "/tmp" id <$> lookupEnv "HOME"
  let dir = home </> ".cache" </> "tv"
  createDirectoryIfMissing True dir
  newIORef dir

logDir :: IOE :> es => Eff es FilePath
logDir = liftIO (readIORef logDirRef)

logPath :: IOE :> es => Eff es FilePath
logPath = (</> "tv.log") <$> logDir

logTimestamp :: IOE :> es => Eff es String
logTimestamp = liftIO $ do
  now <- getCurrentTime
  zt <- utcToLocalZonedTime now
  pure $ formatTime defaultTimeLocale "%H:%M:%S%Q" (zonedTimeToLocalTime zt)

logWrite :: IOE :> es => String -> String -> Eff es ()
logWrite tag msg = do
  p <- logPath
  ts <- logTimestamp
  liftIO $ appendFile p $ "[" ++ ts ++ "] [" ++ tag ++ "] " ++ msg ++ "\n"

logError :: IOE :> es => String -> Eff es ()
logError = logWrite "error"

-- ============================================================================
-- TmpDir
-- ============================================================================

{-# NOINLINE tmpDirRef #-}
tmpDirRef :: IORef FilePath
tmpDirRef = unsafePerformIO $ newIORef ""

initTmpDir :: IOE :> es => Eff es FilePath
initTmpDir = liftIO $ do
  out <- readProcess "mktemp" ["-d", "/tmp/tv-XXXXXX"] ""
  let dir = filter (/= '\n') out
  writeIORef tmpDirRef dir
  pure dir

tmpPath :: IOE :> es => String -> Eff es FilePath
tmpPath name = liftIO ((</> name) <$> readIORef tmpDirRef)

tryRemoveFile :: IOE :> es => FilePath -> Eff es ()
tryRemoveFile p = liftIO (void (try @SomeException $ removeFile p))

cleanupTmp :: IOE :> es => Eff es ()
cleanupTmp = liftIO $ do
  dir <- readIORef tmpDirRef
  unless (null dir) $ void (try @SomeException $ removeDirectoryRecursive dir)

-- ============================================================================
-- Socket (pure Haskell via network package, replaces C sock_shim)
-- ============================================================================

{-# NOINLINE sockRef #-}
sockRef :: IORef (Maybe (Socket, FilePath))
sockRef = unsafePerformIO $ newIORef Nothing

initSocket :: IOE :> es => Eff es ()
initSocket = liftIO $ do
  tmp <- getTemporaryDirectory
  pid <- getProcessID
  let path = tmp </> "tv-" ++ show pid ++ ".sock"
  sock <- socket AF_UNIX Stream defaultProtocol
  bind sock (SockAddrUnix path)
  listen sock 5
  writeIORef sockRef (Just (sock, path))
  setEnv "TV_SOCK" path True

-- | Poll for a command string from the socket (non-blocking via accept timeout).
-- Returns Nothing if no pending connection.
sockPollCmd :: IOE :> es => Eff es (Maybe Text)
sockPollCmd = liftIO $ do
  msock <- readIORef sockRef
  case msock of
    Nothing -> pure Nothing
    Just (sock, _) -> do
      -- TODO: use GHC's non-blocking IO / threadWaitRead for proper non-blocking
      result <- try @SomeException $ do
        (conn, _) <- accept sock
        bs <- NSB.recv conn 4096
        close conn
        pure bs
      case result of
        Left _ -> pure Nothing
        Right bs ->
          let t = T.strip (TE.decodeUtf8 bs)
          in pure (if T.null t then Nothing else Just t)

sockGetPath :: IOE :> es => Eff es FilePath
sockGetPath = liftIO (maybe "" snd <$> readIORef sockRef)

shutdownSocket :: IOE :> es => Eff es ()
shutdownSocket = do
  msock <- liftIO (readIORef sockRef)
  case msock of
    Nothing -> pure ()
    Just (sock, path) -> do
      liftIO (close sock)
      tryRemoveFile path
      liftIO (writeIORef sockRef Nothing)

bracketSocket :: IO a -> IO a
bracketSocket act = bracket (runEff initSocket) (const (runEff shutdownSocket)) (const act)

-- ============================================================================
-- Remote: URI path operations
-- ============================================================================

remoteJoin :: Text -> Text -> Text
remoteJoin pfx name = if T.isSuffixOf "/" pfx then pfx <> name else pfx <> "/" <> name

remoteParent :: Text -> Int -> Maybe Text
remoteParent path minParts =
  let parts = T.splitOn "/" (stripSlash path)
  in if length parts <= minParts then Nothing
     else Just $ T.intercalate "/" (init parts) <> "/"

remoteDispName :: Text -> Text
remoteDispName path =
  let parts = filter (not . T.null) $ T.splitOn "/" (stripSlash path)
  in if length parts <= 1 then path else last parts

stripSlash :: Text -> Text
stripSlash p = if T.isSuffixOf "/" p then T.dropEnd 1 p else p
