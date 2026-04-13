-- | Command config: entry type + key/cmd lookup maps.
-- The handler combinator map and entry table live in App (single source of truth).
module Tv.CmdConfig where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.IORef
import System.IO.Unsafe (unsafePerformIO)
import Tv.Types (Cmd, cmdStr, cmdFromStr)
import Tv.Eff (Eff, IOE, (:>), liftIO)

-- | Command entry: metadata for key binding, menu display, and dispatch
data Entry = Entry
  { entCmd      :: !Cmd
  , entCtx      :: !Text     -- "r"=row, "c"=col, "g"=grp, "s"=sels, "a"=user arg, "S"=stack(2+)
  , entKey      :: !Text     -- key name: "j", "<ret>", "<C-d>", etc.
  , entLabel    :: !Text     -- fzf menu label (empty = hidden)
  , entResets   :: !Bool     -- whether this resets view stack
  , entViewCtx  :: !Text     -- "freqV", "colMeta", "fld", "tbl", or "" (global)
  } deriving (Show)

data CmdInfo = CmdInfo { ciCmd :: !Cmd, ciResets :: !Bool } deriving (Show)

-- Cached lookup maps (initialized once by initCmds)
{-# NOINLINE keyInfoRef #-}
keyInfoRef :: IORef (Map (Text, Text) CmdInfo)
keyInfoRef = unsafePerformIO $ newIORef Map.empty

{-# NOINLINE cmdInfoRef #-}
cmdInfoRef :: IORef (Map Cmd CmdInfo)
cmdInfoRef = unsafePerformIO $ newIORef Map.empty

{-# NOINLINE argCmdRef #-}
argCmdRef :: IORef (Set Cmd)
argCmdRef = unsafePerformIO $ newIORef Set.empty

{-# NOINLINE menuRef #-}
menuRef :: IORef [Entry]
menuRef = unsafePerformIO $ newIORef []

-- | Build lookup caches from entry list (called once at startup)
initCmds :: IOE :> es => [Entry] -> Eff es ()
initCmds entries = liftIO $ do
  let keyM = Map.fromList [(( entKey e, entViewCtx e), ci e) | e <- entries, not (T.null (entKey e))]
      cmdM = Map.fromList [( entCmd e, ci e) | e <- entries]
      argS = Set.fromList [entCmd e | e <- entries, T.isInfixOf "a" (entCtx e)]
  writeIORef keyInfoRef keyM
  writeIORef cmdInfoRef cmdM
  writeIORef argCmdRef argS
  writeIORef menuRef entries
  where ci e = CmdInfo (entCmd e) (entResets e)

-- | Context-aware key lookup: try (key, viewCtx) first, fall back to (key, "")
keyLookup :: IOE :> es => Text -> Text -> Eff es (Maybe CmdInfo)
keyLookup key viewCtx = do
  m <- liftIO (readIORef keyInfoRef)
  pure $ case Map.lookup (key, viewCtx) m of
    Just ci -> Just ci
    Nothing -> Map.lookup (key, "") m

-- | Lookup by Cmd
cmdLookup :: IOE :> es => Cmd -> Eff es CmdInfo
cmdLookup c = do
  m <- liftIO (readIORef cmdInfoRef)
  pure $ Map.findWithDefault (CmdInfo c False) c m

-- | Lookup by command string (socket/external boundary)
handlerLookup :: IOE :> es => Text -> Eff es (Maybe CmdInfo)
handlerLookup h = case cmdFromStr h of
  Just c -> Just <$> cmdLookup c
  Nothing -> pure Nothing

-- | Check if command takes user input
isArgCmd :: IOE :> es => Cmd -> Eff es Bool
isArgCmd c = Set.member c <$> liftIO (readIORef argCmdRef)

-- | Menu items filtered by view context
menuItems :: IOE :> es => Text -> Eff es [(Text, Text, Text, Text)]
menuItems vctx = do
  es <- liftIO (readIORef menuRef)
  pure [(cmdStr (entCmd e), entCtx e, entKey e, entLabel e)
       | e <- es, not (T.null (entLabel e))
       , T.null (entViewCtx e) || entViewCtx e == vctx]
