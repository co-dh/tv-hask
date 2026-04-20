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
  , splitChunks
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Char8 as BS8
import Data.Maybe (isJust)
import System.Environment (lookupEnv)
import System.IO (hFlush, stdout)

-- Maximum chunk size per APC sequence (kitty protocol caps at 4096
-- characters of base64 payload).
chunkBytes :: Int
chunkBytes = 4096

-- | Send a PNG to the terminal via the kitty graphics protocol. Splits
-- the base64-encoded payload into chunks framed by APC sequences.
-- Inside tmux, wraps each APC in a DCS passthrough envelope so tmux
-- forwards the bytes to the host terminal instead of stripping them.
displayPng :: FilePath -> IO ()
displayPng path = do
  bs <- BS.readFile path
  inTmux <- isJust <$> lookupEnv "TMUX"
  let emit = emitWith inTmux
  case splitChunks chunkBytes (B64.encode bs) of
    []                -> pure ()
    [only]            -> emit "f=100,a=T" only
    (first : middles) -> do
      emit "f=100,a=T,m=1" first
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
          payload = if inTmux then "\x1bPtmux;" <> escTmux apc <> "\x1b\\" else apc
      in BS.hPut stdout payload

    -- Double every ESC byte (0x1b) so the DCS-passthrough sequence reads
    -- the literal bytes through to the outer terminal.
    escTmux :: BS.ByteString -> BS.ByteString
    escTmux = BS.intercalate "\x1b\x1b" . BS.split 0x1b

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
