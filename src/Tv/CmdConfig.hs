{-
  Command config: Entry type + cached lookups.
  Commands array lives in App/Common (single table with both metadata and handler).
-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Tv.CmdConfig where

import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import Data.Hashable (Hashable(..))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.IO.Unsafe (unsafePerformIO)

import Tv.StrEnum (StrEnum(..))
import qualified Tv.StrEnum as StrEnum
import Tv.Types (Cmd)
import qualified Tv.Util as Log

-- Orphan Hashable instance for Cmd; Types.hs doesn't derive Generic, so hash via
-- the canonical string form from StrEnum.
instance Hashable Cmd where
  hashWithSalt s c = hashWithSalt s (StrEnum.toString c :: Text)

-- | Lookup result for dispatch
data CmdInfo = CmdInfo
  { ciCmd      :: Cmd
  , ciResetsVS :: Bool
  }

-- | Command entry: metadata for key binding, menu, and dispatch
data Entry = Entry
  { entryCmd      :: Cmd
  , entryCtx      :: Text    -- input context: r=current row, c=current column,
                             -- g=group columns, s=selected rows, a=user arg, S=stack(2+ views)
  , entryKey      :: Text    -- key name: "j", "<ret>", "<C-d>", "<S-left>", etc.
  , entryLabel    :: Text    -- fzf menu label (empty = hidden from menu)
  , entryResetsVS :: Bool
  , entryViewCtx  :: Text    -- context filter: "freqV", "colMeta", "fld", "tbl", or "" (global)
  }

-- | Cached: (key, viewCtx) -> CmdInfo -- context-aware key lookup
keyInfoMap :: IORef (HashMap (Text, Text) CmdInfo)
keyInfoMap = unsafePerformIO (newIORef HashMap.empty)
{-# NOINLINE keyInfoMap #-}

-- | Cached: cmd -> CmdInfo (socket/programmatic dispatch)
cmdInfoMap :: IORef (HashMap Cmd CmdInfo)
cmdInfoMap = unsafePerformIO (newIORef HashMap.empty)
{-# NOINLINE cmdInfoMap #-}

-- | Cached: handlers that take user input (ctx contains 'a')
argCmdSet :: IORef (HashSet Cmd)
argCmdSet = unsafePerformIO (newIORef HashSet.empty)
{-# NOINLINE argCmdSet #-}

-- | Cached: menu items for fzf
menuCache :: IORef (Vector Entry)
menuCache = unsafePerformIO (newIORef V.empty)
{-# NOINLINE menuCache #-}

-- | Build caches from command entries (called by App/Common.initHandlers).
init :: Vector Entry -> IO ()
init cmds = do
  let step (kI, cI, aS) e =
        let ci = CmdInfo { ciCmd = entryCmd e, ciResetsVS = entryResetsVS e }
            kI' = if T.null (entryKey e)
                    then kI
                    else HashMap.insert (entryKey e, entryViewCtx e) ci kI
            cI' = HashMap.insert (entryCmd e) ci cI
            aS' = if T.any (== 'a') (entryCtx e)
                    then HashSet.insert (entryCmd e) aS
                    else aS
        in (kI', cI', aS')
      (keyInfo, cmdInfo, argSet) =
        V.foldl' step (HashMap.empty, HashMap.empty, HashSet.empty) cmds
  writeIORef keyInfoMap keyInfo
  writeIORef cmdInfoMap cmdInfo
  writeIORef argCmdSet argSet
  writeIORef menuCache cmds
  Log.write "init" (T.pack ("commands: " ++ show (V.length cmds) ++ " entries"))

-- | O(1) context-aware lookup: try (key, viewCtx) first, fall back to (key, "")
keyLookup :: Text -> Text -> IO (Maybe CmdInfo)
keyLookup key viewCtx = do
  m <- readIORef keyInfoMap
  case HashMap.lookup (key, viewCtx) m of
    Just ci -> pure (Just ci)
    Nothing -> pure (HashMap.lookup (key, "") m)

-- | O(1) lookup by Cmd -> CmdInfo.
cmdLookup :: Cmd -> IO CmdInfo
cmdLookup c = do
  m <- readIORef cmdInfoMap
  pure (HashMap.lookupDefault (CmdInfo { ciCmd = c, ciResetsVS = False }) c m)

-- | Lookup by handler name string (socket/external boundary only)
handlerLookup :: Text -> IO (Maybe CmdInfo)
handlerLookup h =
  case StrEnum.ofStringQ h :: Maybe Cmd of
    Just c  -> Just <$> cmdLookup c
    Nothing -> do
      Log.write "cmd" (T.pack ("unknown command: " ++ T.unpack h))
      pure Nothing

-- | O(1) check if command takes user input (ctx contains 'a').
isArgCmd :: Cmd -> IO Bool
isArgCmd c = do
  s <- readIORef argCmdSet
  pure (HashSet.member c s)

-- | Menu items for fzf, filtered by view context. Returns (handler, ctx, key, label).
menuItems :: Text -> IO (Vector (Text, Text, Text, Text))
menuItems viewCtx = do
  cmds <- readIORef menuCache
  pure (V.mapMaybe go cmds)
  where
    go e
      | T.null (entryLabel e) = Nothing
      | not (T.null (entryViewCtx e)) && entryViewCtx e /= viewCtx = Nothing
      | otherwise = Just (StrEnum.toString (entryCmd e), entryCtx e, entryKey e, entryLabel e)
