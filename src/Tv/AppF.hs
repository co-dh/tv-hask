-- | Three event-loop operations whose test and production implementations
-- differ: render the current state, poll the next keystroke, read an
-- argument for an arg-command. Everything else in the loop runs as plain
-- 'Eff' actions.
module Tv.AppF
  ( Interp(..)
  ) where

import Data.Text (Text)
import Tv.Eff (Eff, AppEff)

data Interp = Interp
  { iRender  :: Eff AppEff ()
  , iNextKey :: Eff AppEff (Maybe Text)
  , iReadArg :: Eff AppEff Text
  }
