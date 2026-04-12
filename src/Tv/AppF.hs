{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
-- | Free-monad effect machinery for the main event loop.
-- The loop body is written once as an @AppM s a@ program value, then run
-- through one of two interpreters: production (real Term/Socket/fzf) or
-- test (keystroke array + preview-less render + buffer drain on empty).
--
-- Only the three operations that actually differ between test and prod
-- (render, nextKey, readArg) are captured as AppM constructors; everything
-- else lifts through @liftIO@ so existing IO helpers stay put.
module Tv.AppF
  ( AppM(..), doRender, poll, readArg'
  , Interp(..), run
  ) where

import Data.Text (Text)
import Control.Monad.IO.Class (MonadIO(..))

-- | Program value: a small free monad describing what the event loop does.
-- The state type @s@ (= AppState at call sites) is a parameter so this
-- module does not need to import Render, avoiding a cyclic import.
data AppM s a where
  Pure    :: a -> AppM s a
  LiftIO  :: IO b -> (b -> AppM s a) -> AppM s a
  -- render the current state and receive the updated state back
  Render  :: s -> (s -> AppM s a) -> AppM s a
  -- poll for the next keystroke; Nothing = test queue drained (prod never returns Nothing)
  NextKey :: (Maybe Text -> AppM s a) -> AppM s a
  -- read argument for an arg-command; prod returns "" (fzf handled in handler), test collects up to <ret>
  ReadArg :: (Text -> AppM s a) -> AppM s a
  -- unreachable sentinel — interpreters error on this, never produced by normal programs
  Halt    :: AppM s a

instance Functor (AppM s) where fmap f m = m >>= pure . f

instance Applicative (AppM s) where
  pure = Pure
  mf <*> ma = mf >>= \f -> fmap f ma

instance Monad (AppM s) where
  Pure x      >>= f = f x
  LiftIO io k >>= f = LiftIO io  (\b  -> k b  >>= f)
  Render s k  >>= f = Render s   (\s' -> k s' >>= f)
  NextKey k   >>= f = NextKey    (\o  -> k o  >>= f)
  ReadArg k   >>= f = ReadArg    (\t  -> k t  >>= f)
  Halt        >>= _ = Halt

instance MonadIO (AppM s) where
  liftIO io = LiftIO io Pure

-- Smart constructors — the loop body uses these instead of raw ctors.
doRender :: s -> AppM s s
doRender s = Render s Pure

poll :: AppM s (Maybe Text)
poll = NextKey Pure

readArg' :: AppM s Text
readArg' = ReadArg Pure

-- | Interpreter record: three operations that differ between test and prod.
-- Everything else runs as plain IO via the LiftIO constructor.
data Interp s = Interp
  { iRender  :: s -> IO s
  , iNextKey :: IO (Maybe Text)
  , iReadArg :: IO Text
  }

-- | Walk the program value, dispatching each op through the supplied interpreter.
run :: Interp s -> AppM s a -> IO a
run _ (Pure x)       = pure x
run i (LiftIO io k)  = io          >>= \b  -> run i (k b)
run i (Render s k)   = iRender i s >>= \s' -> run i (k s')
run i (NextKey k)    = iNextKey i  >>= \o  -> run i (k o)
run i (ReadArg k)    = iReadArg i  >>= \t  -> run i (k t)
run _ Halt           = error "AppM.Halt reached (unreachable sentinel)"
