{-
  StrEnum: deriving handler for simple enums where toString = constructor name.
  Usage: `inductive Foo | bar | baz deriving StrEnum`
  Generates: toString, all, ofString?
-}
module Tv.StrEnum where

import Data.Text (Text)
import Data.Vector (Vector)

-- Lean's `ofString?` renamed to `ofStringQ` (`?` is not a legal Haskell identifier char).
class StrEnum a where
  toString  :: a -> Text
  all       :: Vector a
  ofStringQ :: Text -> Maybe a
