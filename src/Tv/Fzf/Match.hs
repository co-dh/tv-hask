
{-# LANGUAGE BangPatterns #-}
-- | Fuzzy subsequence matcher, fzf-v2 inspired.
--
-- Smartcase subsequence match: all-lowercase query is case-insensitive;
-- any uppercase letter in the query flips the match to case-sensitive
-- (matches fzf's default). Positional bonuses: start-of-word, camelCase
-- boundary, consecutive chars, case match, start-of-string. Returns
-- 'Nothing' on miss. The position list points at the matched target
-- chars so callers can highlight them.
--
-- Kept small and pure so it runs inside the picker hot loop (one call per
-- (item, keystroke) pair) without allocating much.
module Tv.Fzf.Match where

import Tv.Prelude
import Data.Char (isUpper, isLower, isAlphaNum, toLower)
import qualified Data.IntSet as IS
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
-- Supports a subset of fzf's extended syntax:
--  * @^prefix@ — anchor the first match position to the start of the target
--  * @suffix$@ — anchor the match to end at the last target position
--
-- Smartcase: lowercase-only query is case-insensitive; any uppercase
-- char in the query makes the whole match case-sensitive.
--
-- >>> snd <$> match "abc" "a-b-c"
-- Just [0,2,4]
-- >>> snd <$> match "abc" "A-B-C"
-- Just [0,2,4]
-- >>> match "ABC" "abc"
-- Nothing
-- >>> snd <$> match "ABC" "ABCdef"
-- Just [0,1,2]
-- >>> snd <$> match "fB" "fooBar"
-- Just [0,3]
-- >>> match "xyz" "abc"
-- Nothing
-- >>> match "" "anything"
-- Just (0,[])
-- >>> snd <$> match "^freq" "freq.open | g | | ..."
-- Just [0,1,2,3]
-- >>> match "^freq" "xfreq"
-- Nothing
match :: Text -> Text -> Maybe (Int, [Int])
match query0 target
  | T.null query = Just (0, [])
  | otherwise    = do
      r@(_, poses) <- go 0 (-1) 0 []
      if endAnchor
        then case reverse poses of
               (p : _) | p == T.length target - 1 -> Just r
               _ -> Nothing
        else Just r
  where
    (startAnchor, afterCaret) = case T.uncons query0 of
      Just ('^', rest) -> (True, rest)
      _                -> (False, query0)
    (endAnchor, query) =
      if not (T.null afterCaret) && T.last afterCaret == '$'
        then (True, T.init afterCaret)
        else (False, afterCaret)
    -- Smartcase: query has any uppercase → exact match; else case-insensitive.
    eq = if T.any isUpper query then (==) else eqCI
    qn = T.length query
    tn = T.length target
    go !qi !prev !acc !poses
      | qi >= qn = Just (acc, reverse poses)
      | otherwise =
          case findFrom qi (prev + 1) of
            Nothing -> Nothing
            Just ti
              | startAnchor && null poses && ti /= 0 -> Nothing
              | otherwise ->
                  let b     = bonus target ti prev
                      exact = if T.index target ti == T.index query qi then 2 else 0
                  in go (qi + 1) ti (acc + 1 + b + exact) (ti : poses)
    findFrom qi fromIdx
      | fromIdx >= tn                                = Nothing
      | eq (T.index query qi) (T.index target fromIdx) = Just fromIdx
      | otherwise                                    = findFrom qi (fromIdx + 1)

-- | Score-only variant. Used when the caller only ranks candidates.
matchNoPos :: Text -> Text -> Maybe Int
matchNoPos q t = fmap fst (match q t)

-- | Score, returning 'minBound' on miss. Convenient as a sort key.
score :: Text -> Text -> Int
score q t = fromMaybe minBound (matchNoPos q t)

-- | Multi-term match: the query is split on whitespace into terms that
-- must all match (AND). A term prefixed with @!@ is a negation — the
-- item matches only if that term does NOT appear. Each positive term
-- keeps all of 'match's features (smartcase, @^prefix@, @suffix$@).
-- Score is the sum of positive-term scores; positions are the union
-- (sorted, deduped) so every matched character is highlighted.
--
-- A bare @!@ or all-negation query with no matching negation returns
-- @Just (0, [])@ — matches everything.
--
-- >>> snd <$> matchMulti "foo bar" "foo and bar"
-- Just [0,1,2,8,9,10]
-- >>> matchMulti "foo !bar" "foo and bar"
-- Nothing
-- >>> snd <$> matchMulti "foo !bar" "foo and baz"
-- Just [0,1,2]
-- >>> matchMulti "!bar" "no match"
-- Just (0,[])
-- >>> matchMulti "!bar" "bar here"
-- Nothing
-- >>> matchMulti "" "anything"
-- Just (0,[])
matchMulti :: Text -> Text -> Maybe (Int, [Int])
matchMulti q0 target
  | T.null q0 = Just (0, [])
  | otherwise = fmap finalize (go (T.words q0) 0 IS.empty)
  where
    finalize (s, ps) = (s, IS.toAscList ps)
    go []          acc ps = Just (acc, ps)
    go (term : ts) acc ps = case T.uncons term of
      Just ('!', rest)
        | T.null rest                -> go ts acc ps
        | isJust (match rest target) -> Nothing
        | otherwise                  -> go ts acc ps
      _ -> case match term target of
             Nothing       -> Nothing
             Just (s, ps') -> go ts (acc + s) (IS.union ps (IS.fromList ps'))
