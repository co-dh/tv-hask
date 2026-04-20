{-# LANGUAGE OverloadedStrings #-}
-- | System-clipboard copy via OSC 52.
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
-- 'Tv.Kitty') so tmux forwards the bytes to the host terminal instead
-- of eating them. Users still need tmux @set -g set-clipboard on@ for
-- tmux's own OSC 52 to be active, but our wrapper only needs
-- passthrough (allow-passthrough on).
module Tv.Clip
  ( copy
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as B64
import Data.Maybe (isJust)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import System.Environment (lookupEnv)
import System.IO (hFlush, stdout)

-- | Copy text to the system clipboard via OSC 52.
copy :: Text -> IO ()
copy t = do
  inTmux <- isJust <$> lookupEnv "TMUX"
  let payload = B64.encode (TE.encodeUtf8 t)
      osc     = "\x1b]52;c;" <> payload <> "\x07"
      bytes
        | inTmux    = "\x1bPtmux;" <> escTmux osc <> "\x1b\\"
        | otherwise = osc
  BS.hPut stdout bytes
  hFlush stdout
  where
    -- Inside tmux's DCS passthrough, every ESC byte is doubled so tmux
    -- forwards the literal ESC to the outer terminal instead of
    -- treating it as a passthrough terminator.
    escTmux :: BS.ByteString -> BS.ByteString
    escTmux = BS.intercalate "\x1b\x1b" . BS.split 0x1b
