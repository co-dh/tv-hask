{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
-- | Tests for the dataframe-based computation layer (@Tv.Df.*@).
-- Mix of pure parser/verb tests and full-TUI integration checks
-- exercised with @-b df@.
module TestDf (tests) where

import qualified Data.Text as T

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, assertBool, assertEqual, Assertion)

import qualified DataFrame as D
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (unsafeGetColumn)

import TestUtil (runHask, contains, footer)

import qualified Tv.Df as Df
import qualified Tv.Df.Freq as Freq
import qualified Tv.Df.Parse as P
import Tv.Df.Parse (BinOp (..), Lit (..), PExpr (..), UnOp (..))

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

-- ----------------------------------------------------------------------------
-- PRQL-subset parser (Tv.Df.Parse)
-- ----------------------------------------------------------------------------

test_parse_filter_arg :: Assertion
test_parse_filter_arg =
  assertEqual "Exchange == 'P'"
    (Right (PBin Eq (PCol "Exchange") (PLit (LText "P"))))
    (P.parseExpr "Exchange == 'P'")

test_parse_derive_arg :: Assertion
test_parse_derive_arg =
  assertEqual "double = x * 2"
    (Right ("double", PBin Mul (PCol "x") (PLit (LInt 2))))
    (P.parseBinding "double = x * 2")

test_parse_compound_logic :: Assertion
test_parse_compound_logic =
  assertEqual "Price > 100 and Sym == 'AAPL'"
    (Right (PBin And
             (PBin Gt (PCol "Price") (PLit (LInt 100)))
             (PBin Eq (PCol "Sym") (PLit (LText "AAPL")))))
    (P.parseExpr "Price > 100 and Sym == 'AAPL'")

test_parse_or_precedence :: Assertion
test_parse_or_precedence =
  -- a and b or c == (a and b) or c
  assertEqual "a and b or c"
    (Right (PBin Or
             (PBin And (PCol "a") (PCol "b"))
             (PCol "c")))
    (P.parseExpr "a and b or c")

test_parse_arith_precedence :: Assertion
test_parse_arith_precedence =
  -- x + y * 2 == x + (y * 2)
  assertEqual "x + y * 2"
    (Right (PBin Add (PCol "x") (PBin Mul (PCol "y") (PLit (LInt 2)))))
    (P.parseExpr "x + y * 2")

test_parse_unary_neg :: Assertion
test_parse_unary_neg =
  assertEqual "-x < 0"
    (Right (PBin Lt (PUn Neg (PCol "x")) (PLit (LInt 0))))
    (P.parseExpr "-x < 0")

test_parse_parens :: Assertion
test_parse_parens =
  assertEqual "(x + 1) * 2"
    (Right (PBin Mul (PBin Add (PCol "x") (PLit (LInt 1))) (PLit (LInt 2))))
    (P.parseExpr "(x + 1) * 2")

test_parse_double_lit :: Assertion
test_parse_double_lit =
  assertEqual "price > 1.5"
    (Right (PBin Gt (PCol "price") (PLit (LDouble 1.5))))
    (P.parseExpr "price > 1.5")

test_parse_double_quoted :: Assertion
test_parse_double_quoted =
  assertEqual "sym == \"AAPL\""
    (Right (PBin Eq (PCol "sym") (PLit (LText "AAPL"))))
    (P.parseExpr "sym == \"AAPL\"")

test_parse_rejects_trailing :: Assertion
test_parse_rejects_trailing = case P.parseExpr "x == 1 garbage" of
  Left _  -> pure ()
  Right e -> assertBool ("expected parse failure, got " <> show e) False

-- ----------------------------------------------------------------------------
-- -b df integration tests — exercise the live dataframe freq path end-to-end
-- ----------------------------------------------------------------------------

runDf :: T.Text -> FilePath -> IO T.Text
runDf keys file = runHask keys file ["-b", "df"]

-- @F@ on a CSV with numeric group key — the basic smoke test.
test_df_freq_smoke :: Assertion
test_df_freq_smoke = do
  out <- runDf "F" "data/multi_freq.csv"
  let (tab, status) = footer out
  assertBool "freq tab marker"        (contains tab "[freq a]")
  assertBool "2 groups (a=1, a=2)"    (contains status "r0/2")

-- Derived Cnt/Pct/Bar columns are present (the view's header row
-- contains their names with the format suffixes).
test_df_freq_columns :: Assertion
test_df_freq_columns = do
  out <- runDf "F" "data/multi_freq.csv"
  assertBool "Cnt column"  (contains out "Cnt")
  assertBool "Pct column"  (contains out "Pct")
  assertBool "Bar column"  (contains out "Bar")

-- Composite group (a, b) — same input as TestDf's pure test. Four
-- distinct (a, b) combinations.
test_df_freq_composite :: Assertion
test_df_freq_composite = do
  out <- runDf "l!F" "data/multi_freq.csv"   -- move to b, add as 2nd key, freq
  let (_, status) = footer out
  assertBool "4 groups for (a, b)" (contains status "r0/4")

-- F<ret> exits freq view (same invariant as the DuckDB path).
test_df_freq_enter_pops :: Assertion
test_df_freq_enter_pops = do
  out <- runDf "F<ret>" "data/multi_freq.csv"
  let (tab, _) = footer out
  assertBool "exits freq view"        (not (contains tab "[freq"))
  assertBool "back on source tab"     (contains tab "multi_freq")

tests :: TestTree
tests = testGroup "TestDf"
  [ testCase "readcsv_smoke"            test_readcsv_smoke
  , testCase "freq_basic"               test_freq_basic
  , testCase "parse_filter_arg"         test_parse_filter_arg
  , testCase "parse_derive_arg"         test_parse_derive_arg
  , testCase "parse_compound_logic"     test_parse_compound_logic
  , testCase "parse_or_precedence"      test_parse_or_precedence
  , testCase "parse_arith_precedence"   test_parse_arith_precedence
  , testCase "parse_unary_neg"          test_parse_unary_neg
  , testCase "parse_parens"             test_parse_parens
  , testCase "parse_double_lit"         test_parse_double_lit
  , testCase "parse_double_quoted"      test_parse_double_quoted
  , testCase "parse_rejects_trailing"   test_parse_rejects_trailing
  , testCase "df_freq_smoke"            test_df_freq_smoke
  , testCase "df_freq_columns"          test_df_freq_columns
  , testCase "df_freq_composite"        test_df_freq_composite
  , testCase "df_freq_enter_pops"       test_df_freq_enter_pops
  ]
