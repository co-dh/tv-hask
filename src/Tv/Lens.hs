-- |
-- Minimal lens library: concrete get/set encoding (not van Laarhoven).
-- Solves nested record updates like @s { a = (a s) { b = v } }@.
--
-- Usage:
--
-- > get (outerL `comp` innerL) s       -- read nested field
-- > set (outerL `comp` innerL) v s     -- write nested field
-- > modify (outerL `comp` innerL) f s  -- apply f to nested field
--
-- Field lenses are defined next to the struct by hand, e.g.
--
-- > curL :: Lens' (NavAxis n elem) (Fin n)
-- > curL = Lens' { get = cur, set = \a s -> s { cur = a } }
module Tv.Lens
  ( Lens' (..)
  , modify
  , comp
  , (|.)
  ) where

-- | Concrete lens: getter + setter pair.
-- Simpler and more Haskell-friendly than van Laarhoven (no Functor/Applicative needed).
data Lens' s a = Lens'
  { get :: s -> a
  , set :: a -> s -> s
  }

-- | Update a focused field by applying @f@.
modify :: Lens' s a -> (a -> a) -> s -> s
modify l f s = set l (f (get l s)) s
{-# INLINE modify #-}

-- | Compose lenses outer -> inner:
-- @get (comp outerL innerL) s = get innerL (get outerL s)@.
comp :: Lens' s a -> Lens' a b -> Lens' s b
comp l m = Lens'
  { get = \s -> get m (get l s)
  , set = \b s -> set l (set m b (get l s)) s
  }
{-# INLINE comp #-}

-- | Lens composition infix (ASCII rendering of Lean's @∘ₗ@).
-- @rowL |. curL :: Lens' NavState (Fin n)@.
infixr 9 |.
(|.) :: Lens' s a -> Lens' a b -> Lens' s b
(|.) = comp
{-# INLINE (|.) #-}
