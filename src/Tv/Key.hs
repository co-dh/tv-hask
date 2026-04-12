-- | Key normalization: Vty.Event -> readable key string + tokenizer for -c mode.
module Tv.Key
  ( evToKey, tokenizeKeys, nextKeyFromQueue
  -- Test helpers: build events without exposing Vty to callers.
  , mkArrowEv, mkCharEv, mkEnterEv, mkBsEv, mkEscEv
  , KMod(..)
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Graphics.Vty as Vty

-- | Normalize a vty Key event to a readable key string.
-- Bare arrows collapse to hjkl; modified arrows keep direction names ("<S-left>").
-- Enter/Backspace always "<ret>"/"<bs>" regardless of spurious ctrl mod (termbox quirk).
-- "j", " ", "<ret>", "<pgdn>", "<C-d>", "<S-left>", "<A-x>"
evToKey :: Vty.Event -> Text
evToKey (Vty.EvKey key mods) = case key of
  Vty.KEnter -> "<ret>"
  Vty.KBS    -> "<bs>"
  Vty.KEsc   -> "<esc>"
  Vty.KDown  | null relevantMods -> "j"
  Vty.KUp    | null relevantMods -> "k"
  Vty.KLeft  | null relevantMods -> "h"
  Vty.KRight | null relevantMods -> "l"
  Vty.KDown  -> wrap "down"
  Vty.KUp    -> wrap "up"
  Vty.KLeft  -> wrap "left"
  Vty.KRight -> wrap "right"
  Vty.KPageDown -> "<pgdn>"
  Vty.KPageUp   -> "<pgup>"
  Vty.KHome  -> "<home>"
  Vty.KEnd   -> "<end>"
  Vty.KChar c
    | null relevantMods -> T.singleton c
    | otherwise -> wrap (T.singleton c)
  _ -> ""
  where
    -- Shift on bare chars is irrelevant (already in the char itself)
    relevantMods = filter (`elem` [Vty.MCtrl, Vty.MAlt, Vty.MShift]) mods
    pfx = T.concat
      [ if Vty.MShift `elem` mods then "S-" else ""
      , if Vty.MCtrl  `elem` mods then "C-" else ""
      , if Vty.MAlt   `elem` mods then "A-" else "" ]
    wrap b = "<" <> pfx <> b <> ">"
evToKey _ = ""

-- | Tokenize a -c key string into key tokens.
-- "jj<ret><C-d>" → ["j", "j", "<ret>", "<C-d>"]
tokenizeKeys :: Text -> [Text]
tokenizeKeys = go . T.unpack
  where
    go [] = []
    go ('<':rest) = case span (/= '>') rest of
      -- Empty tag "<>" treated as two literal chars (matches Lean behavior)
      ("", '>':_) -> "<" : go rest
      (tag, '>':after) -> alias ("<" <> T.pack tag <> ">") : go after
      _ -> "<" : go rest
    go (c:rest) = T.singleton c : go rest
    -- bare arrow aliases match evToKey
    alias "<down>" = "j"; alias "<up>" = "k"
    alias "<left>" = "h"; alias "<right>" = "l"
    alias t = t

-- ----------------------------------------------------------------------------
-- Event-builder helpers (so test modules don't need to depend on vty directly).
-- ----------------------------------------------------------------------------

data KMod = KShift | KCtrl | KAlt deriving (Eq, Show)

toVtyMod :: KMod -> Vty.Modifier
toVtyMod KShift = Vty.MShift
toVtyMod KCtrl  = Vty.MCtrl
toVtyMod KAlt   = Vty.MAlt

mkArrowEv :: Char -> [KMod] -> Vty.Event
mkArrowEv dir ms = Vty.EvKey k (map toVtyMod ms)
  where k = case dir of
          'j' -> Vty.KDown; 'k' -> Vty.KUp
          'h' -> Vty.KLeft; 'l' -> Vty.KRight
          _   -> Vty.KChar dir

mkCharEv :: Char -> [KMod] -> Vty.Event
mkCharEv c ms = Vty.EvKey (Vty.KChar c) (map toVtyMod ms)

mkEnterEv, mkBsEv, mkEscEv :: [KMod] -> Vty.Event
mkEnterEv ms = Vty.EvKey Vty.KEnter (map toVtyMod ms)
mkBsEv    ms = Vty.EvKey Vty.KBS    (map toVtyMod ms)
mkEscEv   ms = Vty.EvKey Vty.KEsc   (map toVtyMod ms)

-- | Pop the first key from a queue. Matches Lean Key.nextKey.
nextKeyFromQueue :: [Text] -> (Text, [Text])
nextKeyFromQueue []     = ("", [])
nextKeyFromQueue (k:ks) = (k, ks)
