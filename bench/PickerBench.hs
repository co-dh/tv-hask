-- One-off benchmark for the picker's per-keystroke filter pipeline.
--
-- Replicates the body of Tv.Fzf.Picker.computeMatches inline (using only
-- the pure Tv.Fzf.Match module, no terminal FFI) so we can time the
-- filter+sort costs at sizes 10K / 100K / 1M without spinning up the TUI.
--
-- Run: `cabal run picker-bench`. Reports wall ms per "keystroke".

{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.DeepSeq (NFData, deepseq, force)
import Data.IORef (newIORef, readIORef, writeIORef, modifyIORef')
import Data.List (sortOn)
import Data.Ord (Down(..))
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import qualified Data.Vector.Algorithms.Intro as VAI
import Data.Time.Clock (getCurrentTime, diffUTCTime)
import System.IO (hFlush, stdout)
import Text.Printf (printf)
import System.Mem (performGC)
import qualified GHC.Stats as Stats

import Tv.Fzf.Match (matchParsed, parseQuery)

-- Mirror of Tv.Fzf.Picker.computeMatches kept inline so we can benchmark
-- without depending on the Term FFI. If the picker version drifts, this
-- benchmark loses meaning — keep them in sync until computeMatches is
-- factored into a pure module.
computeMatches :: Text -> V.Vector Text -> V.Vector (Int, Int, [Int])
computeMatches q its
  | T.null q  = V.generate (V.length its) (\i -> (i, 0, []))
  | otherwise =
      let terms         = parseQuery q
          scoreOne i ln = fmap (\(s, ps) -> (i, s, ps)) (matchParsed terms ln)
      in V.fromList $ sortOn (\(_, s, _) -> Down s)
                    $ V.toList $ V.imapMaybe scoreOne its

-- Match-only path: scores every item but skips the sort/marshaling.
-- Lets us see how much of the per-keystroke cost is the match function
-- versus the surrounding pipeline.
matchOnly :: Text -> V.Vector Text -> Int
matchOnly q its
  | T.null q  = V.length its
  | otherwise =
      let terms = parseQuery q
      in V.length (V.imapMaybe (\_ ln -> matchParsed terms ln) its)

-- Candidate replacement for computeMatches: in-place introsort over a
-- mutable vector via vector-algorithms, no list round-trip.
computeMatchesV :: Text -> V.Vector Text -> V.Vector (Int, Int, [Int])
computeMatchesV q its
  | T.null q  = V.generate (V.length its) (\i -> (i, 0, []))
  | otherwise =
      let terms         = parseQuery q
          scoreOne i ln = fmap (\(s, ps) -> (i, s, ps)) (matchParsed terms ln)
          unsorted      = V.imapMaybe scoreOne its
      in V.modify (VAI.sortBy (\(_,a,_) (_,b,_) -> compare b a)) unsorted

-- Top-K via partialSortBy: only the first K items end up sorted, the
-- rest stay in arbitrary order. Slice off the rest. Cuts the sort phase
-- from O(N log N) to O(N log K) — the win at large N.
computeMatchesK :: Int -> Text -> V.Vector Text -> V.Vector (Int, Int, [Int])
computeMatchesK k q its
  | T.null q  = V.generate (V.length its) (\i -> (i, 0, []))
  | otherwise =
      let terms         = parseQuery q
          scoreOne i ln = fmap (\(s, ps) -> (i, s, ps)) (matchParsed terms ln)
          unsorted      = V.imapMaybe scoreOne its
          n             = V.length unsorted
          k'            = min k n
      in V.take k'
       $ V.modify (\mv -> VAI.partialSortBy
                            (\(_,a,_) (_,b,_) -> compare b a) mv k')
                  unsorted

-- Streaming chunked top-K (mirror of Tv.Fzf.Picker.computeMatches).
-- Walks items into a fixed-size buffer; whenever the buffer fills,
-- partial-sorts top-K and discards the back. Peak per-call allocation
-- = O(chunkCap * 50 bytes) ≈ 50 KB regardless of how many items match,
-- versus computeMatchesK which materialises a vector sized by the match
-- count (tens of MB at 1M items with broad queries).
computeMatchesStream
  :: Int                              -- ^ topK
  -> Int                              -- ^ chunkCap (≥ topK; 4*topK is typical)
  -> Text -> V.Vector Text
  -> IO (V.Vector (Int, Int, [Int]), Int)
computeMatchesStream k cap q its
  | T.null q  =
      let n = V.length its
      in pure (V.generate (min k n) (\i -> (i, 0, [])), n)
  | otherwise = do
      let terms = parseQuery q
          cmp (_,a,_) (_,b,_) = compare b a
      buf    <- MV.unsafeNew cap
      total  <- newIORef (0 :: Int)
      filled <- newIORef (0 :: Int)
      V.iforM_ its $ \i ln -> case matchParsed terms ln of
        Nothing      -> pure ()
        Just (s, ps) -> do
          modifyIORef' total (+ 1)
          f <- readIORef filled
          MV.unsafeWrite buf f (i, s, ps)
          let f' = f + 1
          if f' == cap
            then do
              VAI.partialSortBy cmp buf k
              writeIORef filled k
            else writeIORef filled f'
      f      <- readIORef filled
      tot    <- readIORef total
      let kF = min k f
      VAI.partialSortBy cmp (MV.unsafeSlice 0 f buf) kF
      result <- V.unsafeFreeze (MV.unsafeSlice 0 kF buf)
      pure (result, tot)

-- Variant A (indexed): mirror of the original narrowFrom. Walks the
-- prior set, looks each item up via `its V.! i`. The indexed lookup is
-- random-access into items and turns out to be cache-unfriendly enough
-- that narrowing only beats a fresh rescan once the prior set is small.
narrowFromBench
  :: Text
  -> V.Vector (Int, Int, [Int])
  -> V.Vector Text
  -> (V.Vector (Int, Int, [Int]), V.Vector (Int, Int, [Int]), Int)
narrowFromBench q prev its
  | T.null q  =
      let n    = V.length its
          full = V.generate n (\i -> (i, 0, []))
      in (V.take (min 256 n) full, full, n)
  | otherwise =
      let terms = parseQuery q
          rescore (i, _, _) = fmap (\(s, ps) -> (i, s, ps))
                                   (matchParsed terms (its V.! i))
          unsorted = V.mapMaybe rescore prev
          n        = V.length unsorted
          k        = min 256 n
          sorted   = V.modify
                       (\mv -> VAI.partialSortBy
                                 (\(_,a,_) (_,b,_) -> compare b a) mv k)
                       unsorted
      in (V.take k sorted, unsorted, n)

-- Variant B: prior set carries the item text directly so rescoring is
-- a sequential walk, no `its V.! i` random-access.
type FullB = V.Vector (Int, Text)

mkFullB :: V.Vector Text -> FullB
mkFullB = V.imap (\i t -> (i, t))

narrowFromBenchB
  :: Text
  -> FullB
  -> V.Vector Text
  -> (V.Vector (Int, Int, [Int]), FullB, Int)
narrowFromBenchB q prev its
  | T.null q  =
      let n    = V.length its
          full = mkFullB its
      in (V.take (min 256 n) (V.imap (\i _ -> (i,0,[])) its), full, n)
  | otherwise =
      let terms = parseQuery q
          rescore (i, t) = case matchParsed terms t of
            Just (s, ps) -> Just ((i, s, ps), (i, t))
            Nothing      -> Nothing
          (scored, kept) = V.unzip (V.mapMaybe rescore prev)
          n              = V.length scored
          k              = min 256 n
          sorted         = V.modify
                             (\mv -> VAI.partialSortBy
                                       (\(_,a,_) (_,b,_) -> compare b a) mv k)
                             scored
      in (V.take k sorted, kept, n)

mkItems :: Int -> V.Vector Text
mkItems n = V.generate n $ \i ->
  -- Approximate a real distinct-values column: 26 buckets keyed off the
  -- low byte of i. "alpha", "bravo", "charlie", … so "a"/"al"/"alp" each
  -- narrow the match set by ~26× per char, exercising the typed-narrowing
  -- path. Trailing index makes each item unique so V.length ≈ N.
  let bucket = words26 !! (i `mod` 26)
      suff   = T.pack (show i)
  in bucket <> "_" <> suff
  where
    words26 =
      [ "alpha", "bravo", "charlie", "delta", "echo", "foxtrot"
      , "golf", "hotel", "india", "juliet", "kilo", "lima"
      , "mike", "november", "oscar", "papa", "quebec", "romeo"
      , "sierra", "tango", "uniform", "victor", "whiskey", "xray"
      , "yankee", "zulu"
      ]

time :: NFData a => String -> IO a -> IO a
time label io = do
  t0 <- getCurrentTime
  x  <- io
  let !x' = force x
  t1 <- getCurrentTime
  printf "  %-32s %.3f ms\n" label
    (1000 * realToFrac (diffUTCTime t1 t0) :: Double)
  hFlush stdout
  pure x'

-- timeIO is identical to time but doesn't force the result with NFData
-- (the streaming matcher returns a tuple containing a Vector + Int that
-- already gets fully realized inside the IO action).
timeIO :: String -> IO a -> IO a
timeIO label io = do
  t0 <- getCurrentTime
  x  <- io
  t1 <- getCurrentTime
  printf "  %-32s %.3f ms\n" label
    (1000 * realToFrac (diffUTCTime t1 t0) :: Double)
  hFlush stdout
  pure x

-- liveBytes reports the live-set size after a major GC. Requires
-- +RTS -T -RTS to enable runtime stats.
liveBytes :: IO Int
liveBytes = do
  enabled <- Stats.getRTSStatsEnabled
  if not enabled then pure 0
  else fromIntegral . Stats.gcdetails_live_bytes . Stats.gc <$> Stats.getRTSStats

runSize :: Int -> IO ()
runSize n = do
  printf "n=%d\n" n
  let items = force (mkItems n)
  items `deepseq` pure ()
  -- Cold: empty query (identity).
  _ <- time "computeMatches \"\""    (pure $! computeMatches ""    items)
  _ <- time "computeMatches \"a\""   (pure $! computeMatches "a"   items)
  _ <- time "computeMatches \"al\""  (pure $! computeMatches "al"  items)
  _ <- time "computeMatches \"alp\"" (pure $! computeMatches "alp" items)
  _ <- time "computeMatches \"row\"" (pure $! computeMatches "row" items)
  _ <- time "computeMatches \"r 5\"" (pure $! computeMatches "r 5" items)
  -- Isolate the match phase from the sort/marshal phase.
  _ <- time "matchOnly      \"a\""   (pure $! matchOnly "a"   items)
  _ <- time "matchOnly      \"row\"" (pure $! matchOnly "row" items)
  _ <- time "matchOnly      \"r 5\"" (pure $! matchOnly "r 5" items)
  -- vector-algorithms in-place sort.
  _ <- time "computeMatchesV \"a\""   (pure $! computeMatchesV "a"   items)
  _ <- time "computeMatchesV \"row\"" (pure $! computeMatchesV "row" items)
  _ <- time "computeMatchesV \"r 5\"" (pure $! computeMatchesV "r 5" items)
  -- Top-K (K=200) — what we actually need for the picker's display window.
  _ <- time "computeMatchesK \"a\""   (pure $! computeMatchesK 200 "a"   items)
  _ <- time "computeMatchesK \"row\"" (pure $! computeMatchesK 200 "row" items)
  _ <- time "computeMatchesK \"r 5\"" (pure $! computeMatchesK 200 "r 5" items)
  -- Streaming chunked top-K — current Picker.hs implementation.
  _ <- timeIO "stream         \"a\""   (computeMatchesStream 256 1024 "a"   items)
  _ <- timeIO "stream         \"row\"" (computeMatchesStream 256 1024 "row" items)
  _ <- timeIO "stream         \"r 5\"" (computeMatchesStream 256 1024 "r 5" items)
  -- Peak heap during a single streaming match.
  performGC
  before <- liveBytes
  (_, _) <- computeMatchesStream 256 1024 "a" items
  performGC
  after  <- liveBytes
  printf "  %-32s before=%d KB  after=%d KB  delta=%d KB\n"
         ("stream alloc \"a\"" :: String)
         (before `div` 1024)
         (after  `div` 1024)
         ((after - before) `div` 1024)
  -- Simulate a typed-narrowing session: each step narrows from the
  -- previous full match set instead of rescanning items.
  let (_, full1, _) = narrowFromBench "a"   (V.imap (\i _ -> (i,0,[])) items) items
  _ <- time "narrow \"a\" → \"al\""    (pure $! narrowFromBench "al"   full1 items)
  let (_, full2, _) = narrowFromBench "al"  full1 items
  _ <- time "narrow \"al\" → \"alp\""  (pure $! narrowFromBench "alp"  full2 items)
  let (_, full3, _) = narrowFromBench "alp" full2 items
  _ <- time "narrow \"alp\" → \"alph\""(pure $! narrowFromBench "alph" full3 items)
  -- Variant B: prior set stores Text directly → sequential walk in narrow.
  let (_, fullB1, _) = narrowFromBenchB "a"   (mkFullB items) items
  _ <- time "narrowB \"a\" → \"al\""    (pure $! narrowFromBenchB "al"   fullB1 items)
  let (_, fullB2, _) = narrowFromBenchB "al"  fullB1 items
  _ <- time "narrowB \"al\" → \"alp\""  (pure $! narrowFromBenchB "alp"  fullB2 items)
  let (_, fullB3, _) = narrowFromBenchB "alp" fullB2 items
  _ <- time "narrowB \"alp\" → \"alph\""(pure $! narrowFromBenchB "alph" fullB3 items)
  pure ()

main :: IO ()
main = mapM_ runSize [10000, 100000, 1000000]
