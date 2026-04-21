-- | Project prelude. Re-exports common unqualified names; callers still
-- import qualified modules (Data.Text as T, Data.Vector as V, ...) for
-- the operations, but skip the per-file boilerplate for Text/Vector/
-- Word32/fromMaybe/when/unless/forM_/IORef + optics operators.
module Tv.Prelude
  ( -- * Re-exported types
    Text, Vector, ByteString, HashMap, IORef, IntSet
  , Word8, Word16, Word32, Word64, Int32, Int64
    -- * Control.Monad
  , when, unless, forM_, forM, zipWithM_, replicateM_, void
    -- * Data.Maybe
  , fromMaybe, catMaybes, mapMaybe, isJust, isNothing, listToMaybe
    -- * Data.IORef
  , newIORef, readIORef, writeIORef, modifyIORef, modifyIORef'
    -- * Optics
  , (&), (^.), (.~), (%~), (%), view, set, over
  ) where

import Control.Monad (when, unless, forM_, forM, zipWithM_, replicateM_, void)
import Data.ByteString (ByteString)
import Data.HashMap.Strict (HashMap)
import Data.Int (Int32, Int64)
import Data.IntSet (IntSet)
import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef, modifyIORef')
import Data.Maybe (fromMaybe, catMaybes, mapMaybe, isJust, isNothing, listToMaybe)
import Data.Text (Text)
import Data.Vector (Vector)
import Data.Word (Word8, Word16, Word32, Word64)
import Optics.Core ((&), (^.), (.~), (%~), (%), view, set, over)
