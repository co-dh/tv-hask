{-
  Key normalization: Term.Event -> readable key string.
  No handler names here -- all key->handler mapping lives in CmdConfig.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Key
  ( toKey
  , tokenizeKeys
  , nextKey
  ) where

import Data.Bits ((.&.))
import Data.Maybe (fromMaybe, listToMaybe)
import Data.Char (chr)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word16, Word32)
import qualified Tv.Term as Term
import Tv.Term (Event(..))

-- Special key code -> (bare name, modifier name)
-- Bare: unmodified output. Modifier: used with S-/C-/A- prefixes.
-- Arrows map to hjkl bare but left/right/up/down when modified.
keyNames :: V.Vector (Word16, Text, Text)
keyNames = V.fromList
  [ (Term.keyArrowDown, "j", "down"), (Term.keyArrowUp, "k", "up")
  , (Term.keyArrowLeft, "h", "left"), (Term.keyArrowRight, "l", "right")
  , (Term.keyEnter, "ret", "ret"), (Term.keyBackspace, "bs", "bs"), (Term.keyBackspace2, "bs", "bs")
  , (Term.keyPageDown, "pgdn", "pgdn"), (Term.keyPageUp, "pgup", "pgup")
  , (Term.keyHome, "home", "home"), (Term.keyEnd, "end", "end"), (Term.keyEsc, "esc", "esc")
  ]

modPfx :: Event -> Text
modPfx ev =
  (if mods ev .&. Term.modShift /= 0 then "S-" else "") <>
  (if mods ev .&. Term.modCtrl  /= 0 then "C-" else "") <>
  (if mods ev .&. Term.modAlt   /= 0 then "A-" else "")

-- | Normalize terminal event to readable key string.
-- Regular chars -> "j", " "; special keys -> "<ret>", "<pgdn>"; modifiers -> "<S-left>", "<C-d>", "<A-x>"
toKey :: Event -> Text
toKey ev =
  if typ ev /= Term.eventKey then "" else
  let pfx = modPfx ev
      found = V.foldr step Nothing keyNames
      step (k, bare, modN) acc =
        if keyCode ev == k then Just (bare, modN) else acc
  in case found of
    Just (bare, modN) ->
      -- termbox2 sets TB_MOD_CTRL for ASCII control chars (Enter=0x0D, Bs=0x08, Esc=0x1B).
      -- Strip that implicit ctrl so Enter produces "<ret>" not "<C-ret>".
      let pfx' = if keyCode ev < 0x20 then T.replace "C-" "" pfx else pfx
      in if T.null pfx'
           then (if T.length bare == 1 then bare else "<" <> bare <> ">")
           else "<" <> pfx' <> modN <> ">"
    Nothing ->
      let code :: Word32
          code = if ch ev > 0 then ch ev else fromIntegral (keyCode ev)
      in if code > 0 && code < 32
           then "<C-" <> T.singleton (chr (fromIntegral code + 96)) <> ">"
           else if ch ev > 0
             then let c = T.singleton (chr (fromIntegral (ch ev)))
                  in if T.null pfx then c else "<" <> pfx <> c <> ">"
             else ""

-- | Tokenize a `-c` key string into descriptive key tokens.
-- "jj<ret><C-d>" -> #["j", "j", "<ret>", "<C-d>"]
-- Aliases: arrow keys -> hjkl
tokenizeKeys :: Text -> Vector Text
tokenizeKeys s = V.fromList (go 0)
  where
    chars = T.unpack s
    n = length chars
    at i = fromMaybe '\0' (listToMaybe (drop i chars))
    go i
      | i >= n = []
      | at i == '<' =
          let tag = takeWhile (/= '>') (drop (i + 1) chars)
          in if null tag
               then "<" : go (i + 1)
               else
                 let close = i + 1 + length tag
                 in if close < n && at close == '>'
                      then
                        let tokRaw = T.pack ('<' : tag ++ ['>'])
                            tok = case tokRaw of
                              "<down>"  -> "j"
                              "<up>"    -> "k"
                              "<right>" -> "l"
                              "<left>"  -> "h"
                              t         -> t
                        in tok : go (close + 1)
                      else "<" : go (i + 1)
      | otherwise = T.singleton (at i) : go (i + 1)

nextKey :: Vector Text -> IO (Text, Vector Text)
nextKey keys =
  if not (V.null keys)
    then pure (V.head keys, V.slice 1 (V.length keys - 1) keys)
    else do
      e <- Term.pollEvent
      pure (toKey e, V.empty)
