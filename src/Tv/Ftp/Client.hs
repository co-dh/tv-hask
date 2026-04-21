{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-
  Native FTP client wrapping the ftp-client package. Replaces shelling
  out to the `curl` CLI for `ftp://` listing and download.
  Returns (ExitCode, Text, Text) tuples to slot into the same call
  sites as Tv.Source.Core.runCmd.
-}
module Tv.Ftp.Client
  ( listFtp
  , downloadFtp
  ) where

import Tv.Prelude
import Control.Exception (SomeException, try)
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Network.FTP.Client as FTP
import System.Exit (ExitCode (..))

import qualified Tv.Log as Log

-- | Parse an "ftp://host[:port]/path[/]" URL into (host, port, path).
-- Defaults port to 21. An empty path becomes "/".
parseFtpUrl :: Text -> (String, Int, String)
parseFtpUrl url0 =
  let url      = T.drop 6 url0  -- strip "ftp://"
      (hp, p)  = T.break (== '/') url
      (h, n)   = case T.splitOn ":" hp of
                   [host, portT]
                     | [(n', "")] <- reads (T.unpack portT) -> (host, n')
                   _ -> (hp, 21)
      path     = T.unpack $ if T.null p then "/" else p
  in (T.unpack h, n, path)

-- | Run an ftp action, log + map exceptions to the (ec, out, err) shape.
runFtp :: Text -> IO BS.ByteString -> IO (ExitCode, Text, Text)
runFtp ctx action = do
  r <- try action
  case r of
    Left (e :: SomeException) -> do
      let msg = T.pack (show e)
      Log.errorLog (ctx <> ": " <> msg)
      pure (ExitFailure 1, "", msg)
    Right bs -> pure (ExitSuccess, TE.decodeUtf8 bs, "")

-- | LIST: connect, anonymous login, CWD into path, LIST.
listFtp :: Text -> IO (ExitCode, Text, Text)
listFtp url = runFtp ("ftp list " <> url) $
  FTP.withFTP host port $ \h _ -> do
    _ <- FTP.login h "anonymous" "tv@example.com"
    when (path /= "" && path /= "/") $ void $ FTP.cwd h path
    FTP.list h []
  where (host, port, path) = parseFtpUrl url

-- | RETR: split off filename, CWD into the dir, RETR, write to disk.
downloadFtp :: Text -> FilePath -> IO (ExitCode, Text, Text)
downloadFtp url dest = do
  (ec, _, err) <- runFtp ("ftp retr " <> url) $
    FTP.withFTP host port $ \h _ -> do
      _ <- FTP.login h "anonymous" "tv@example.com"
      when (dir /= "" && dir /= "/") $ void $ FTP.cwd h dir
      bs <- FTP.retr h file
      BS.writeFile dest bs
      pure ""
  pure (ec, "", err)
  where
    (host, port, path) = parseFtpUrl url
    (dir, file) = case break (== '/') (reverse path) of
      (revFile, '/' : revDir) -> (reverse revDir, reverse revFile)
      _                        -> ("/", path)
