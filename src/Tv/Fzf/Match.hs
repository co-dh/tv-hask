
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
-- Walks @target@ exactly once via @T.uncons@, advancing the query pointer
-- each time the current target char matches. The previous code indexed
-- the target with @T.index@ at every probe — O(L) per probe in @text@-2.x
-- (UTF-8 internal), so a target of length L cost O(L²) per match call.
-- The single-pass walk is O(L+Q), giving the picker an order-of-magnitude
-- speedup at large item counts (see bench/PickerBench.hs).
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
  | otherwise    = walk target qChars 0 '\0' [] (-1) 0
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
    -- Query is short (≤ ~20 chars typically); unpacking once per call
    -- gives us O(1) head access during the walk.
    qChars = T.unpack query

    -- Walk arguments: remaining target, remaining query chars, score,
    -- previous target char (carried so the wordStart check is O(1)),
    -- match positions in reverse order, index of previous match (-1 = none),
    -- current target index. Bang-patterns keep the loop strict — the
    -- accumulators escape into the result, so a thunk leak here would
    -- defeat the whole point.
    walk !rest !qs !score !prev !poses !prevTi !ti
      | startAnchor && null poses && ti > 0 = Nothing
      | otherwise = case qs of
          []
            | endAnchor && not (T.null rest) -> Nothing
            | otherwise                       -> Just (score, reverse poses)
          (qc : qs') -> case T.uncons rest of
            Nothing -> Nothing
            Just (c, rest')
              | eq qc c ->
                  let isWS  = ti == 0
                           || (isAlphaNum c
                               && (not (isAlphaNum prev)
                                   || (isLower prev && isUpper c)))
                      b | prevTi >= 0 && prevTi + 1 == ti = 15
                        | isWS                            = if ti == 0 then 20 else 10
                        | otherwise                       = 0
                      exact = if qc == c then 2 else 0
                  in walk rest' qs' (score + 1 + b + exact) c (ti : poses) ti (ti + 1)
              | otherwise ->
                  walk rest' qs score c poses prevTi (ti + 1)

-- | Parse a multi-term query into @(negated, stripped)@ pairs.
-- Empty-after-strip terms (bare @!@, double spaces) are dropped.
parseQuery :: Text -> [(Bool, Text)]
parseQuery q = [ (neg, t) | w <- T.words q, let (neg, t) = split w, not (T.null t) ]
  where
    split w = case T.uncons w of
      Just ('!', rest) -> (True, rest)
      _                -> (False, w)

-- | Multi-term match with terms pre-parsed. Hoist 'parseQuery' out of
-- inner loops (pickers iterate the term list per item per keystroke).
matchParsed :: [(Bool, Text)] -> Text -> Maybe (Int, [Int])
matchParsed terms0 target = fmap finalize (go terms0 0 IS.empty)
  where
    finalize (s, ps) = (s, IS.toAscList ps)
    go []                  acc ps = Just (acc, ps)
    go ((neg, t) : rest) acc ps
      | neg = if isJust (match t target) then Nothing else go rest acc ps
      | otherwise = case match t target of
          Nothing       -> Nothing
          Just (s, ps') -> go rest (acc + s) (IS.union ps (IS.fromList ps'))
