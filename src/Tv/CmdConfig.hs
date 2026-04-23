{-
  Command config: Entry type + pure lookup cache.
  Entry constructors (mkEntry, navE, hdl) live here so feature modules
  can define their own commands without importing App.Types.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.CmdConfig where

import Tv.Prelude
import Control.Applicative ((<|>))
import qualified Data.HashMap.Strict as HashMap
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.Types (Cmd, StrEnum(..))
import Optics.TH (makeFieldLabelsNoPrefix)

-- | Lookup result for dispatch
data CmdInfo = CmdInfo
  { ciCmd    :: Cmd
  , ciResets :: Bool
  }
makeFieldLabelsNoPrefix ''CmdInfo

-- | Command entry: metadata for key binding, menu, and dispatch
data Entry = Entry
  { cmd      :: Cmd
  , ctx      :: Text    -- input context: r=current row, c=current column,
                             -- g=group columns, s=selected rows, a=user arg, S=stack(2+ views)
  , key      :: Text    -- key name: "j", "<ret>", "<C-d>", "<S-left>", etc.
  , label    :: Text    -- fzf menu label (empty = hidden from menu)
  , resets   :: Bool
  , hint     :: Bool    -- surface in the info overlay (independent of menu visibility)
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
makeFieldLabelsNoPrefix ''CmdCache

-- | Build cache from command entries (pure, no IO).
buildCache :: Vector Entry -> CmdCache
buildCache cmds =
  let step (kI, cI, aS) Entry{..} =
        let ci = CmdInfo { ciCmd = cmd, ciResets = resets }
            kI' = if T.null key then kI
                  else HashMap.insert (key, viewCtx) ci kI
            cI' = HashMap.insert cmd ci cI
            aS' = if T.any (== 'a') ctx
                    then HashSet.insert cmd aS else aS
        in (kI', cI', aS')
      (keyInfo, cmdInfo, argSet) =
        V.foldl' step (HashMap.empty, HashMap.empty, HashSet.empty) cmds
  in CmdCache { ccKeyInfo = keyInfo, ccCmdInfo = cmdInfo
              , ccArgCmds = argSet, ccMenu = cmds }

-- | O(1) context-aware lookup: try (key, viewCtx) first, fall back to (key, "")
keyLookup :: CmdCache -> Text -> Text -> Maybe CmdInfo
keyLookup cc key_ viewCtx_ =
  HashMap.lookup (key_, viewCtx_) (cc ^. #ccKeyInfo)
    <|> HashMap.lookup (key_, "") (cc ^. #ccKeyInfo)

-- | O(1) lookup by Cmd -> CmdInfo.
cmdLookup :: CmdCache -> Cmd -> CmdInfo
cmdLookup cc c =
  HashMap.lookupDefault (CmdInfo { ciCmd = c, ciResets = False }) c (cc ^. #ccCmdInfo)

-- | Lookup by handler name string (socket/external boundary only)
handlerLookup :: CmdCache -> Text -> Maybe CmdInfo
handlerLookup cc h = fmap (cmdLookup cc) (ofStringQ h)

-- | O(1) check if command takes user input (ctx contains 'a').
isArgCmd :: CmdCache -> Cmd -> Bool
isArgCmd cc c = HashSet.member c (cc ^. #ccArgCmds)

-- | Menu items for fzf, filtered by view context. Returns (handler, ctx, key, label).
menuItems :: CmdCache -> Text -> Vector (Text, Text, Text, Text)
menuItems cc vctx = V.mapMaybe go (cc ^. #ccMenu)
  where
    go Entry{..}
      | T.null label = Nothing
      | not (T.null viewCtx) && viewCtx /= vctx = Nothing
      | otherwise = Just (toString cmd, ctx, key, label)

-- | Entry constructor shorthand.
mkEntry :: Cmd -> Text -> Text -> Text -> Bool -> Text -> Entry
mkEntry c ctx_ key_ label_ resets_ vctx = Entry
  { cmd = c, ctx = ctx_, key = key_, label = label_, resets = resets_
  , hint = False, viewCtx = vctx }

-- | Mark entry as hint-surfacing (shown in the info overlay).
withHint :: Entry -> Entry
withHint e = e { hint = True }

-- | Hints for the info overlay: hint-flagged entries visible in the current view.
infoHints :: CmdCache -> Text -> Vector (Text, Text)
infoHints cc vctx = V.mapMaybe go (cc ^. #ccMenu)
  where
    go Entry{..}
      | not hint                                = Nothing
      | T.null key || T.null label              = Nothing
      | not (T.null viewCtx) && viewCtx /= vctx = Nothing
      | otherwise                               = Just (key, label)

-- | Nav/sort entry: no handler, falls through to viewUp in dispatch.
navE :: Entry -> (Entry, Maybe f)
navE e = (e, Nothing)

-- | Entry with explicit handler.
hdl :: Entry -> f -> (Entry, Maybe f)
hdl e f = (e, Just f)
