-- | Filter: PRQL-style filter expression builder + fzf header prompt.
-- Mirrors Tc/Types.lean's buildFilterPrql / filterPrompt defaults used
-- by TblOps. Kept free of IO and rendering so it can also be called from
-- the socket/dispatch path.
module Tv.Filter
  ( buildFilter
  , buildFilterWith
  , filterPrompt
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Tv.Types (ColType, isNumeric)

-- | Build a PRQL filter expression from the raw fzf --print-query output.
-- First line is the typed query, remaining lines are the user-selected
-- rows. Only selections that appear in the original @vals@ array are
-- honored (fzf will only emit those, but we defend against stray lines).
-- If the typed query itself matches a value it's prepended as a selection
-- so typing then hitting Enter without marking picks that row.
--
-- Result shapes, matching Tc:
--   1 selection         -> @col == v@
--   > 1 selections      -> @(col == v1 || col == v2 || ...)@
--   0 selections + expr -> raw query (user typed an expression)
--   otherwise           -> empty (cancelled)
buildFilter :: Text -> Vector Text -> Text -> Bool -> Text
buildFilter col vals result numeric =
  let ls       = filter (not . T.null) (T.splitOn "\n" result)
      input    = case ls of (x:_) -> x; _ -> T.empty
      hints    = filter (`V.elem` vals) (drop 1 ls)
      selected = if V.elem input vals && input `notElem` hints
                   then input : hints else hints
      q v      = if numeric then v else "'" <> v <> "'"
      eq v     = col <> " == " <> q v
  in case selected of
       [v] -> eq v
       (_:_:_) -> "(" <> T.intercalate " || " (map eq selected) <> ")"
       _ -> if T.null input then T.empty else input

-- | Convenience wrapper taking a 'ColType' instead of a numeric flag.
buildFilterWith :: Text -> Vector Text -> Text -> ColType -> Text
buildFilterWith col vals result ty = buildFilter col vals result (isNumeric ty)

-- | Header hint shown above the fzf input line. @typ@ is the column's
-- type name ("int", "str", ...), matching Tc's untyped string form.
filterPrompt :: Text -> Text -> Text
filterPrompt col typ =
  let eg = if typ `elem` ["int","float","decimal"]
             then "e.g. " <> col <> " > 5,  " <> col <> " >= 10 && " <> col <> " < 100"
             else "e.g. " <> col <> " == 'USD',  " <> col <> " ~= 'pattern'"
  in "PRQL filter on " <> col <> " (" <> typ <> "):  " <> eg
