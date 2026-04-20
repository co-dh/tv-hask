{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
-- | Fuzzy subsequence matcher, fzf-v2 inspired.
--
-- Case-insensitive subsequence match with positional bonuses:
-- start-of-word, camelCase boundary, consecutive chars, case match,
-- start-of-string. Returns 'Nothing' on miss. The position list points at
-- the matched target chars so callers can highlight them.
--
-- Kept small and pure so it runs inside the picker hot loop (one call per
-- (item, keystroke) pair) without allocating much.
module Tv.Fzf.Match
  ( match
  , matchNoPos
  , score
  , isMatch
  ) where

import Data.Char (isUpper, isLower, isAlphaNum, toLower)
import Data.Text (Text)
import qualified Data.Text as T

-- | Word-boundary start: position 0, or prior char is a non-alnum separator,
-- or a lower→Upper camel transition.
wordStart :: Text -> Int -> Bool
wordStart t i
  | i <= 0               = True
  | not (isAlphaNum cur) = False
  | otherwise            = not (isAlphaNum prev) || (isLower prev && isUpper cur)
  where
    cur  = T.index t i
    prev = T.index t (i - 1)

-- | Per-position bonus. prev == -1 means "no previous match yet".
bonus :: Text -> Int -> Int -> Int
bonus t i prev
  | prev >= 0 && prev + 1 == i = 15
  | wordStart t i              = if i == 0 then 20 else 10
  | otherwise                  = 0

-- | Case-insensitive char equality.
eqCI :: Char -> Char -> Bool
eqCI a b = toLower a == toLower b

-- | Fuzzy subsequence match with score and match positions. Empty query
-- matches anything with score 0 and no positions.
--
-- >>> snd <$> match "abc" "a-b-c"
-- Just [0,2,4]
-- >>> snd <$> match "ABC" "abc"
-- Just [0,1,2]
-- >>> snd <$> match "fB" "fooBar"
-- Just [0,3]
-- >>> match "xyz" "abc"
-- Nothing
-- >>> match "" "anything"
-- Just (0,[])
match :: Text -> Text -> Maybe (Int, [Int])
match query target
  | T.null query = Just (0, [])
  | otherwise    = go 0 (-1) 0 []
  where
    qn = T.length query
    tn = T.length target
    go !qi !prev !acc !poses
      | qi >= qn = Just (acc, reverse poses)
      | otherwise =
          case findFrom qi (prev + 1) of
            Nothing -> Nothing
            Just ti ->
              let b     = bonus target ti prev
                  exact = if T.index target ti == T.index query qi then 2 else 0
              in go (qi + 1) ti (acc + 1 + b + exact) (ti : poses)
    findFrom qi fromIdx
      | fromIdx >= tn                                  = Nothing
      | eqCI (T.index query qi) (T.index target fromIdx) = Just fromIdx
      | otherwise                                      = findFrom qi (fromIdx + 1)

-- | Score-only variant. Used when the caller only ranks candidates.
matchNoPos :: Text -> Text -> Maybe Int
matchNoPos q t = fmap fst (match q t)

-- | Score, returning 'minBound' on miss. Convenient as a sort key.
score :: Text -> Text -> Int
score q t = case matchNoPos q t of
  Just s  -> s
  Nothing -> minBound

-- | Does the query match at all? Faster than 'match' when positions aren't needed.
--
-- >>> isMatch "ab" "a-b"
-- True
-- >>> isMatch "ba" "a-b"
-- False
isMatch :: Text -> Text -> Bool
isMatch q t = case matchNoPos q t of
  Just _ -> True; Nothing -> False
