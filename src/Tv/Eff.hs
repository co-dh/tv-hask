{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators #-}
-- | Tv.Eff — canonical effect stack for every app-level function.
--
-- Every function in @src\/Tv\/@ (except the low-level FFI primitives
-- in @Tv.Data.DuckDB@) returns @Eff es a@ for some @es@ that's a
-- subset of 'AppEff'. The stack is:
--
-- * 'State' 'AppState' — replaces Brick's implicit @StateT@. Use
--   'use', '(.=)', '(%=)' (from this module) with optics lenses for
--   the clean read\/write form.
-- * 'Error' 'SomeException' — blanket error for the ~130 legacy
--   @try \@SomeException@ sites. 'tryE' catches and discards.
-- * 'IOE' — bottom for file\/process\/FFI\/terminal. Leaf modules
--   often only need @IOE@; the @State@ and @Error@ effects are
--   free to add since effectful's subsumption is cheap.
module Tv.Eff
  ( -- * Stack
    AppEff
    -- * Runners
  , runAppEff
  , runEff
    -- * Optic helpers over 'State'
  , use
  , (.=)
  , (%=)
    -- * Error helpers
  , tryE
    -- * Re-exports from effectful
  , Eff
  , IOE
  , State
  , Error
  , (:>)
  , liftIO
  , get
  , put
  , modify
  , gets
  , throwError
  ) where

import Control.Exception (SomeException)
import Control.Monad.IO.Class (liftIO)
import Effectful (Eff, IOE, runEff, (:>))
import Effectful.Error.Static (Error, catchError, runErrorNoCallStack, throwError)
import Effectful.State.Static.Local (State, get, gets, modify, put, runState)
import Optics.Core (A_Getter, A_Setter, Is, Optic')
import qualified Optics.Core as O

import Tv.Render (AppState)

-- ============================================================================
-- Effect stack
-- ============================================================================

-- | The canonical stack for every tv-hask handler and loader.
--
-- Order matters: 'Error' is listed before 'State' so that
-- 'runErrorNoCallStack' peels the Error effect first. This way
-- 'runState' wraps the error-handling layer, and state changes
-- accumulated before an exception are preserved in the final
-- @(Either e a, AppState)@ tuple.
type AppEff = '[Error SomeException, State AppState, IOE]

-- ============================================================================
-- Runners
-- ============================================================================

-- | Discharge the full 'AppEff' stack to plain 'IO'. Returns the final
-- state alongside either the caught exception or the result. State
-- changes are preserved across errors.
runAppEff :: AppState -> Eff AppEff a -> IO (Either SomeException a, AppState)
runAppEff st =
  runEff . runState st . runErrorNoCallStack @SomeException

-- ============================================================================
-- Optics helpers on top of State
-- ============================================================================

-- | Read via an optic from the 'State' effect.
use :: (State s :> es, Is k A_Getter) => Optic' k is s a -> Eff es a
use l = gets (O.view l)

infix 4 .=
-- | Set via an optic in the 'State' effect.
(.=) :: (State s :> es, Is k A_Setter) => Optic' k is s a -> a -> Eff es ()
l .= x = modify (O.set l x)

infix 4 %=
-- | Modify via an optic in the 'State' effect.
(%=) :: (State s :> es, Is k A_Setter) => Optic' k is s a -> (a -> a) -> Eff es ()
l %= f = modify (O.over l f)

-- ============================================================================
-- Error helpers
-- ============================================================================

-- | Run an action; on error, return 'Nothing'. Equivalent to the
-- @try \@SomeException@ + @either (const Nothing) Just@ pattern that
-- litters the pre-effectful codebase.
tryE :: Error SomeException :> es => Eff es a -> Eff es (Maybe a)
tryE act = catchError (Just <$> act) (\_ (_ :: SomeException) -> pure Nothing)
