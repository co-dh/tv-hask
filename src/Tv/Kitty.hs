{-# LANGUAGE OverloadedStrings #-}
{-
  Kitty graphics protocol: transmit-and-display a PNG inline.
  Replaces shelling out to `kitten icat` for terminals that support it
  (kitty, WezTerm, ghostty). Format ref:
  https://sw.kovidgoyal.net/kitty/graphics-protocol/
-}
module Tv.Kitty
  ( displayPng
  , supportsKittyGraphics
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Char8 as BS8
import Data.Maybe (isJust)
import System.Environment (lookupEnv)
import System.IO (hFlush, stdout)

-- Each escape-sequence chunk carries up to 4 KiB of base64 payload (kitty
-- protocol minimum supported chunk size; conservative).
chunkBytes :: Int
chunkBytes = 4096

-- | Send a PNG to the terminal via the kitty graphics protocol. Splits
-- the base64-encoded payload into chunks framed by APC sequences.
displayPng :: FilePath -> IO ()
displayPng path = do
  bs <- BS.readFile path
  case splitChunks chunkBytes (B64.encode bs) of
    []                -> pure ()
    [only]            -> emit "f=100,a=T" only
    (first : middles) -> do
      emit "f=100,a=T,m=1" first
      mapM_ (emit "m=1") (init middles)
      emit "m=0" (last middles)
  hFlush stdout
  where
    emit :: String -> BS.ByteString -> IO ()
    emit ctrl chunk = do
      BS.hPut stdout "\x1b_G"
      BS8.hPut stdout (BS8.pack ctrl)
      BS.hPut stdout ";"
      BS.hPut stdout chunk
      BS.hPut stdout "\x1b\\"

-- | Heuristic: detect terminals known to support the kitty graphics
-- protocol via env vars. Cheap and accurate for the common cases
-- (kitty, WezTerm, ghostty). For everything else, return False — caller
-- can fall back to viu / xdg-open. Override with TV_IMAGE_BACKEND=kitty
-- to force-enable.
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

splitChunks :: Int -> BS.ByteString -> [BS.ByteString]
splitChunks n bs
  | BS.null bs = []
  | otherwise  = let (h, t) = BS.splitAt n bs in h : splitChunks n t
