{-
  Command config: Entry type + pure lookup cache.
  Entry constructors (mkEntry, navE, hdl) live here so feature modules
  can define their own commands without importing App.Types.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.CmdConfig
  ( -- * Types
    CmdInfo(..)
  , Entry(..)
  , CmdCache
    -- * Entry constructors
  , mkEntry
  , navE
  , hdl
    -- * Cache build + pure lookup
  , buildCache
  , keyLookup
  , cmdLookup
  , handlerLookup
  , isArgCmd
  , menuItems
  ) where

import Control.Applicative ((<|>))
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Tv.Types (Cmd, StrEnum(..))
import Optics.TH (makeFieldLabelsNoPrefix)

-- | Lookup result for dispatch
data CmdInfo = CmdInfo
  { ciCmd    :: Cmd
  , ciResets :: Bool
  }

-- | Command entry: metadata for key binding, menu, and dispatch
data Entry = Entry
  { cmd      :: Cmd
  , ctx      :: Text    -- input context: r=current row, c=current column,
                             -- g=group columns, s=selected rows, a=user arg, S=stack(2+ views)
  , key      :: Text    -- key name: "j", "<ret>", "<C-d>", "<S-left>", etc.
  , label    :: Text    -- fzf menu label (empty = hidden from menu)
  , resets :: Bool
  , viewCtx  :: Text    -- context filter: "freqV", "colMeta", "fld", "tbl", or "" (global)
  }
makeFieldLabelsNoPrefix ''Entry

-- | Immutable command lookup cache, built once at init from command entries.
-- Replaces 4 global IORefs with a pure value stored in AppState.
data CmdCache = CmdCache
  { ccKeyInfo :: HashMap (Text, Text) CmdInfo
  , ccCmdInfo :: HashMap Cmd CmdInfo
  , ccArgCmds :: HashSet Cmd
  , ccMenu    :: Vector Entry
  }

-- | Build cache from command entries (pure, no IO).
buildCache :: Vector Entry -> CmdCache
buildCache cmds =
  let step (kI, cI, aS) e =
        let ci = CmdInfo { ciCmd = cmd e, ciResets = resets e }
            kI' = if T.null (key e) then kI
                  else HashMap.insert (key e, viewCtx e) ci kI
            cI' = HashMap.insert (cmd e) ci cI
            aS' = if T.any (== 'a') (ctx e)
                    then HashSet.insert (cmd e) aS else aS
        in (kI', cI', aS')
      (keyInfo, cmdInfo, argSet) =
        V.foldl' step (HashMap.empty, HashMap.empty, HashSet.empty) cmds
  in CmdCache { ccKeyInfo = keyInfo, ccCmdInfo = cmdInfo
              , ccArgCmds = argSet, ccMenu = cmds }

-- | O(1) context-aware lookup: try (key, viewCtx) first, fall back to (key, "")
keyLookup :: CmdCache -> Text -> Text -> Maybe CmdInfo
keyLookup cc key_ viewCtx_ =
  HashMap.lookup (key_, viewCtx_) (ccKeyInfo cc)
    <|> HashMap.lookup (key_, "") (ccKeyInfo cc)

-- | O(1) lookup by Cmd -> CmdInfo.
cmdLookup :: CmdCache -> Cmd -> CmdInfo
cmdLookup cc c =
  HashMap.lookupDefault (CmdInfo { ciCmd = c, ciResets = False }) c (ccCmdInfo cc)

-- | Lookup by handler name string (socket/external boundary only)
handlerLookup :: CmdCache -> Text -> Maybe CmdInfo
handlerLookup cc h = fmap (cmdLookup cc) (ofStringQ h)

-- | O(1) check if command takes user input (ctx contains 'a').
isArgCmd :: CmdCache -> Cmd -> Bool
isArgCmd cc c = HashSet.member c (ccArgCmds cc)

-- | Menu items for fzf, filtered by view context. Returns (handler, ctx, key, label).
menuItems :: CmdCache -> Text -> Vector (Text, Text, Text, Text)
menuItems cc vctx = V.mapMaybe go (ccMenu cc)
  where
    go e
      | T.null (label e) = Nothing
      | not (T.null (viewCtx e)) && viewCtx e /= vctx = Nothing
      | otherwise = Just (toString (cmd e), ctx e, key e, label e)

-- | Entry constructor shorthand.
mkEntry :: Cmd -> Text -> Text -> Text -> Bool -> Text -> Entry
mkEntry c ctx_ key_ label_ resets_ vctx = Entry
  { cmd = c, ctx = ctx_, key = key_, label = label_, resets = resets_, viewCtx = vctx }

-- | Nav/sort entry: no handler, falls through to viewUp in dispatch.
navE :: Entry -> (Entry, Maybe f)
navE e = (e, Nothing)

-- | Entry with explicit handler.
hdl :: Entry -> f -> (Entry, Maybe f)
hdl e f = (e, Just f)
