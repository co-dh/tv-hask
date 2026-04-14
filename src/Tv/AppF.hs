-- | Interpreter record for the event loop.
--
-- Previously this module hosted a free-monad GADT with three custom
-- operations (render, nextKey, readArg) plus a raw @LiftIO@ constructor,
-- together with a production and a test interpreter. With the effectful
-- migration the entire loop runs inside 'Eff AppEff' directly, so the
-- free monad was removed — the three ops remain because they are the
-- only loop operations that actually differ between production and test
-- mode.
--
-- Both interpreter actions read and write state via the 'State AppState'
-- effect, so @iRender@ has no parameter or return value.
module Tv.AppF
  ( Interp(..)
  ) where

import Data.Text (Text)
import Tv.Eff (Eff, AppEff)

-- | Three operations whose test and production implementations differ.
-- Everything else runs as plain 'Eff' actions in the loop.
data Interp = Interp
  { iRender  :: Eff AppEff ()
  , iNextKey :: Eff AppEff (Maybe Text)
  , iReadArg :: Eff AppEff Text
  }
