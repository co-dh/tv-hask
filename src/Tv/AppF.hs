{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

-- Free-monad style effect machinery for the main event loop.
-- The loop body is written once as an `AppM s a` program value, then run
-- through one of two interpreters: production (real Term/Socket/fzf) or
-- test (keystroke array + preview-less render + buffer drain on empty).
--
-- Only the three operations that actually differ between test and prod
-- (render, nextKey, readArg) are captured as AppM constructors; everything
-- else lifts through `liftIO` so existing IO helpers (Socket.pollCmd,
-- dispatchHandler, CmdConfig lookups, handler dispatch) stay put.
module Tv.AppF where

import Tv.Prelude
import Control.Monad.IO.Class (MonadIO (..))
import Optics.TH (makeFieldLabelsNoPrefix)

-- | Program value: a small free monad describing what the event loop does.
-- The state type `s` (= `AppState` at call sites) is a parameter so this
-- file does not need to import `App.Common`, avoiding a cyclic import.
data AppM s a where
  -- pure return
  Pure    :: a -> AppM s a
  -- generic IO escape hatch (used via `MonadLift IO` below)
  LiftIO  :: IO b -> (b -> AppM s a) -> AppM s a
  -- render the current state and receive the updated state back
  Render  :: s -> (s -> AppM s a) -> AppM s a
  -- poll for the next keystroke; `Nothing` means "test queue drained" (prod never returns Nothing)
  NextKey :: (Maybe Text -> AppM s a) -> AppM s a
  -- read argument for an arg-command; prod returns "" (fzf handled in handler), test collects up to <ret>
  ReadArg :: (Text -> AppM s a) -> AppM s a
  -- unreachable sentinel so `AppM s a` is unconditionally Nonempty (needed to
  -- define `bind` as `partial`). Interpreters throw on this — never produced
  -- by normal programs.
  Halt    :: AppM s a

-- Structural bind on the program value. `partial` because the recursion runs
-- through the continuation function space — fine for a control flow effect.
bindApp :: AppM s a -> (a -> AppM s b) -> AppM s b
bindApp m f = case m of
  Pure x       -> f x
  LiftIO io k  -> LiftIO io  (\b  -> bindApp (k b ) f)
  Render s k   -> Render s   (\s' -> bindApp (k s') f)
  NextKey k    -> NextKey    (\o  -> bindApp (k o ) f)
  ReadArg k    -> ReadArg    (\s  -> bindApp (k s ) f)
  Halt         -> Halt

instance Functor (AppM s) where
  fmap f m = bindApp m (Pure . f)

instance Applicative (AppM s) where
  pure = Pure
  mf <*> mx = bindApp mf (\f -> bindApp mx (Pure . f))

instance Monad (AppM s) where
  (>>=) = bindApp

-- Lift arbitrary IO into AppM so `<- someIOAction` works inside do-blocks.
instance MonadIO (AppM s) where
  liftIO io = LiftIO io Pure

-- Smart constructors — the loop body uses these instead of raw ctors.
doRender :: s -> AppM s s
doRender s = Render s Pure

poll :: AppM s (Maybe Text)
poll = NextKey Pure

readArg' :: AppM s Text
readArg' = ReadArg Pure

-- | Interpreter: three operations that differ between test and prod.
-- Everything else runs as plain IO via the liftIO constructor.
data Interp s = Interp
  { render  :: s -> IO s
  , nextKey :: IO (Maybe Text)
  , readArg :: IO Text
  }
makeFieldLabelsNoPrefix ''Interp

-- Walk the program value, dispatching each op through the supplied interpreter.
run :: forall s a. Interp s -> AppM s a -> IO a
run i = go
  where
    go :: forall x. AppM s x -> IO x
    go m = case m of
      Pure x       -> pure x
      LiftIO io k  -> do b  <- io;          go (k b )
      Render s k   -> do s' <- (i ^. #render) s; go (k s')
      NextKey k    -> do o  <- i ^. #nextKey;    go (k o )
      ReadArg k    -> do s  <- i ^. #readArg;    go (k s )
      Halt         -> ioError (userError "AppM.halt reached (unreachable sentinel)")
