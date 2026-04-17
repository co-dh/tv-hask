{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
-- | Tests for the dataframe-based computation layer (@Tv.Df.*@).
-- These are parity / correctness tests that don't need the TUI or DuckDB.
module TestDf (tests) where

import qualified Data.Text as T

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, assertBool, assertEqual, Assertion)

import qualified DataFrame as D
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (unsafeGetColumn)

import qualified Tv.Df as Df
import qualified Tv.Df.Freq as Freq

-- | Column-as-list helper: look up by name and convert to [a].
colList :: Columnable a => T.Text -> D.DataFrame -> [a]
colList name = D.toList . unsafeGetColumn name

-- | data/multi_freq.csv
--
-- >  a,b,c
-- >  1,x,10
-- >  1,x,20
-- >  1,y,30
-- >  2,x,40
-- >  2,y,50
-- >  2,y,60
--
-- Grouping by (a, b) should give 4 groups with counts (2,1,1,2) — total 6.
-- Pct values (integer div): 33, 16, 16, 33. Bar lengths: 6, 3, 3, 6 hashes.
-- Sorted by Cnt desc, ties broken by groupBy's internal order.
test_freq_basic :: Assertion
test_freq_basic = do
  df <- Df.readCsv "data/multi_freq.csv"
  let result = Freq.freq ["a", "b"] df
      cnts   = colList @Int "Cnt" result
      pcts   = colList @Int "Pct" result
      bars   = colList @T.Text "Bar" result
  assertEqual "4 rows (one per (a,b) group)" 4 (length cnts)
  assertEqual "sorted by Cnt desc"           [2,2,1,1] cnts
  assertEqual "integer percents sum close to 100" [33,33,16,16] pcts
  assertEqual "bar length = Pct/5" ["######","######","###","###"] bars

-- | Round-trip sanity: readCsv then access columns by name.
test_readcsv_smoke :: Assertion
test_readcsv_smoke = do
  df <- Df.readCsv "data/multi_freq.csv"
  let a = colList @Int "a" df
      b = colList @T.Text "b" df
  assertEqual "6 rows in" 6 (length a)
  assertBool "b contains x and y" (elem "x" b && elem "y" b)

tests :: TestTree
tests = testGroup "TestDf"
  [ testCase "readcsv_smoke" test_readcsv_smoke
  , testCase "freq_basic"    test_freq_basic
  ]
