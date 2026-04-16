{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- |
--   Tests requiring large/untracked data files (sample.parquet, nyse, pac.csv, etc.)
--   Run locally with: cabal test (requires fixture files under data/).
--   Skipped in CI since these files are gitignored.
--
--   Literal port of Tc/test/TestLargeData.lean. Tests that reference a
--   fixture file not present on disk are turned into a no-op, matching
--   Lean's "runlarge" separation from regular CI.
module TestLargeData (tests) where

import Prelude hiding (log)
import Data.Text (Text)
import qualified Data.Text as T
import Tv.Types (headD, getD)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, assertBool, Assertion)
import TestUtil
  ( log
  , run
  , contains
  , footer
  , header
  , dataLines
  , hasFile
  )

-- | Wrap a test body so it becomes a no-op when the fixture file is
--   not present on disk. Mirrors Lean's runlarge-only execution model.
withFixture :: FilePath -> Assertion -> Assertion
withFixture path body = do
  ok <- hasFile path
  if ok
    then body
    else log (T.pack ("  skip (missing fixture): " <> path))


-- === Width stability (pac.csv — gitignored) ===

-- Verify column widths don't change after toggling info panel
test_width_stable_after_info :: Assertion
test_width_stable_after_info = withFixture "data/pac.csv" $ do
  log "width_stable_after_info"
  hdr1 <- header <$> run "" "data/pac.csv" []
  hdr2 <- header <$> run "I" "data/pac.csv" []
  hdr3 <- header <$> run "II" "data/pac.csv" []
  assertBool
    ("Info toggle must not change column widths: [" <> T.unpack hdr1 <> "] vs [" <> T.unpack hdr2 <> "]")
    (hdr1 == hdr2)
  assertBool "Info double-toggle must not change column widths" (hdr1 == hdr3)

-- === Navigation tests (sample.parquet — gitignored) ===

-- Ctrl-D pages down past row 0
test_page_down :: Assertion
test_page_down = withFixture "data/sample.parquet" $ do
  log "page_down"
  (_, status) <- footer <$> run "<C-d>" "data/sample.parquet" []
  assertBool "Ctrl-D pages down past row 0" (not (contains status "r0/"))

-- Ctrl-D then Ctrl-U returns to row 0
test_page_up :: Assertion
test_page_up = withFixture "data/sample.parquet" $ do
  log "page_up"
  (_, status) <- footer <$> run "<C-d><C-u>" "data/sample.parquet" []
  assertBool "Ctrl-D then Ctrl-U returns to row 0" (contains status "r0/")

-- Page down changes status
test_page_down_scrolls :: Assertion
test_page_down_scrolls = withFixture "data/sample.parquet" $ do
  log "page_down_scrolls"
  (_, status1) <- footer <$> run "" "data/sample.parquet" []
  (_, status2) <- footer <$> run "<C-d>" "data/sample.parquet" []
  assertBool "Page down changes status" (status1 /= status2)

-- Last column shows data when scrolled far right
test_last_col_visible :: Assertion
test_last_col_visible = withFixture "data/sample.parquet" $ do
  log "last_col_visible"
  first <- headD "" . dataLines <$> run "llllllllllllllllllll" "data/sample.parquet" []
  let nonWs = T.length (T.filter (not . isWs) first)
  assertBool "Last col shows data" (nonWs > 0)
  where
    isWs c = c == ' ' || c == '\t' || c == '\n' || c == '\r'

-- === Sort tests (sample.parquet) ===

-- Sort asc on age column gives age=18 first
test_parquet_sort_asc :: Assertion
test_parquet_sort_asc = withFixture "data/sample.parquet" $ do
  log "parquet_sort_asc"
  first <- headD "" . dataLines <$> run "l[" "data/sample.parquet" []
  assertBool "[ on age sorts asc, age=18" (contains first " 18 ")

-- Sort desc on age column gives age=80 first
test_parquet_sort_desc :: Assertion
test_parquet_sort_desc = withFixture "data/sample.parquet" $ do
  log "parquet_sort_desc"
  first <- headD "" . dataLines <$> run "l]" "data/sample.parquet" []
  assertBool "] on age sorts desc, age=80" (contains first " 80 ")

-- === Meta tests (sample.parquet) ===

-- M on parquet shows meta view
test_parquet_meta :: Assertion
test_parquet_meta = withFixture "data/sample.parquet" $ do
  log "parquet_meta"
  out <- run "M" "data/sample.parquet" []
  assertBool "M on parquet shows meta" (contains (fst (footer out)) "meta")

-- === Freq tests (sample.parquet) ===

-- F on parquet shows freq view
test_freq_parquet :: Assertion
test_freq_parquet = withFixture "data/sample.parquet" $ do
  log "freq_parquet"
  out <- run "F" "data/sample.parquet" []
  assertBool "F on parquet shows freq" (contains (fst (footer out)) "freq")

-- F<ret> on parquet exits freq and filters
test_freq_enter_parquet :: Assertion
test_freq_enter_parquet = withFixture "data/sample.parquet" $ do
  log "freq_enter_parquet"
  (tab, status) <- footer <$> run "F<ret>" "data/sample.parquet" []
  assertBool "F<ret> on parquet pops to parent" (contains tab "sample.parquet")
  assertBool "F<ret> on parquet exits freq view" (not (contains tab "freq"))
  assertBool "F<ret> on parquet shows filtered rows" (contains status "r0/")

-- === Freq tests (nyse10k.parquet — gitignored) ===

-- Freq key column shows names, not counts
test_freq_parquet_key_values :: Assertion
test_freq_parquet_key_values = withFixture "data/nyse10k.parquet" $ do
  log "freq_parquet_key_values"
  first <- headD "" . dataLines <$> run "lF" "data/nyse10k.parquet" []
  assertBool
    "Freq key column shows names, not counts"
    (not (T.isPrefixOf " 5180" first) && not (T.isPrefixOf " 2592" first))

-- === Freq tests (nyse — gitignored) ===

-- Freq shows total group count
test_freq_total_count :: Assertion
test_freq_total_count = withFixture "data/nyse/1.parquet" $ do
  log "freq_total_count"
  (_, status) <- footer <$> run "l!l!hF" "data/nyse/1.parquet" []
  assertBool "Freq shows total group count (128974)" (contains status "/128974")

-- Freq sort preserves total count
test_freq_sort_preserves_total :: Assertion
test_freq_sort_preserves_total = withFixture "data/nyse/1.parquet" $ do
  log "freq_sort_total"
  (_, status) <- footer <$> run "llFll[" "data/nyse/1.parquet" []
  assertBool "Freq sort preserves total count in status" (contains status "/")

-- Freq sort asc shows data
test_freq_sort_asc_parquet :: Assertion
test_freq_sort_asc_parquet = withFixture "data/nyse/1.parquet" $ do
  log "freq_sort_asc"
  first <- headD "" . dataLines <$> run "llFll[" "data/nyse/1.parquet" []
  assertBool "Freq sort asc shows data" (contains first "\x2502")

-- === Meta selection tests (nyse) ===

-- M0 selects null columns in parquet
test_parquet_meta_0_null_cols :: Assertion
test_parquet_meta_0_null_cols = withFixture "data/nyse/1.parquet" $ do
  log "parquet_meta_0"
  (_, status) <- footer <$> run "M0" "data/nyse/1.parquet" []
  assertBool "M0 on parquet selects 9 null columns" (contains status "sel=9")

-- M0<ret> groups null columns
test_parquet_meta_0_enter_groups :: Assertion
test_parquet_meta_0_enter_groups = withFixture "data/nyse/1.parquet" $ do
  log "parquet_meta_0_enter"
  (tab, status) <- footer <$> run "M0<ret>" "data/nyse/1.parquet" []
  assertBool "M0<ret> returns to parent view" (T.isPrefixOf "[1.parquet]" tab)
  assertBool "M0<ret> groups 9 null columns" (contains status "grp=9")

-- === Scroll fetch tests (nyse10k.parquet) ===

-- Scrolling down fetches more rows
test_scroll_fetches_more :: Assertion
test_scroll_fetches_more = withFixture "data/nyse10k.parquet" $ do
  log "scroll_fetches_more"
  let keys = T.concat (replicate 105 "<C-d>")
  (_, status) <- footer <$> run keys "data/nyse10k.parquet" []
  let rpart = T.takeWhile (/= ' ') (getD (T.splitOn " r" status) 1 "")
      cursor = readNat (headD "" (T.splitOn "/" rpart))
  assertBool
    ("Scroll fetches more: cursor=" <> show cursor <> ", expected > 999")
    (cursor > 999)
  where
    -- Lean: (rpart.splitOn " ").headD "" |>.toNat?.getD 0
    readNat :: Text -> Int
    readNat t = case reads (T.unpack t) :: [(Int, String)] of
      ((n, _):_) -> n
      _          -> 0

-- === Misc (sample.parquet, nyse) ===

-- Numeric columns are right-aligned
test_numeric_right_align :: Assertion
test_numeric_right_align = withFixture "data/sample.parquet" $ do
  log "numeric_align"
  first <- headD "" . dataLines <$> run "" "data/sample.parquet" []
  assertBool "Numeric columns right-aligned" (contains first "  ")

-- Enter on parquet should not quit
test_enter_no_quit_parquet :: Assertion
test_enter_no_quit_parquet = withFixture "data/nyse/1.parquet" $ do
  log "enter_no_quit_parquet"
  (_, status) <- footer <$> run "<ret>j" "data/nyse/1.parquet" []
  assertBool "Enter on parquet should not quit (j moves to r1)" (contains status "r1/")

-- === Run all large-data tests ===

tests :: TestTree
tests = testGroup "TestLargeData"
  [ testCase "width_stable_after_info"     test_width_stable_after_info
  , testCase "page_down"                   test_page_down
  , testCase "page_up"                     test_page_up
  , testCase "page_down_scrolls"           test_page_down_scrolls
  , testCase "last_col_visible"            test_last_col_visible
  , testCase "parquet_sort_asc"            test_parquet_sort_asc
  , testCase "parquet_sort_desc"           test_parquet_sort_desc
  , testCase "parquet_meta"                test_parquet_meta
  , testCase "freq_parquet"                test_freq_parquet
  , testCase "freq_enter_parquet"          test_freq_enter_parquet
  , testCase "freq_parquet_key_values"     test_freq_parquet_key_values
  , testCase "freq_total_count"            test_freq_total_count
  , testCase "freq_sort_preserves_total"   test_freq_sort_preserves_total
  , testCase "freq_sort_asc_parquet"       test_freq_sort_asc_parquet
  , testCase "parquet_meta_0_null_cols"    test_parquet_meta_0_null_cols
  , testCase "parquet_meta_0_enter_groups" test_parquet_meta_0_enter_groups
  , testCase "scroll_fetches_more"         test_scroll_fetches_more
  , testCase "numeric_right_align"         test_numeric_right_align
  , testCase "enter_no_quit_parquet"       test_enter_no_quit_parquet
  ]
