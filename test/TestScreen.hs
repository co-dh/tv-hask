{-# LANGUAGE OverloadedStrings #-}
-- |
--   Screen tests whose core logic is now covered by pure theorems in TestPure.
--   Kept as backup — these additionally test the rendering pipeline (termbox
--   buffer). Literal port of Tc/test/TestScreen.lean.
module TestScreen (tests) where

import Prelude hiding (log)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, assertBool)
import qualified Data.Text as T
import System.Process (readProcessWithExitCode)
import System.Exit (ExitCode (..))

import TestUtil
  ( tvHaskBin
  , log
  , runHask
  , contains
  , footer
  , header
  )

-- === Navigation ===
-- Pure: key_j, key_k, key_l, key_h (key→cmd mapping)
-- Pure: nav_row_inverse (j then k = identity)
-- Pure: nav_col_inverse (l then h = identity)

test_nav_down :: IO ()
test_nav_down = do
  log "nav_down"
  out <- runHask "j" "data/basic.csv" []
  assertBool "j moves to row 1" (contains (snd (footer out)) "r1/")

test_nav_right :: IO ()
test_nav_right = do
  log "nav_right"
  out <- runHask "l" "data/basic.csv" []
  assertBool "l moves to col 1" (contains (snd (footer out)) "c1/")

test_nav_up :: IO ()
test_nav_up = do
  log "nav_up"
  out <- runHask "jk" "data/basic.csv" []
  assertBool "jk returns to row 0" (contains (snd (footer out)) "r0/")

test_nav_left :: IO ()
test_nav_left = do
  log "nav_left"
  out <- runHask "lh" "data/basic.csv" []
  assertBool "lh returns to col 0" (contains (snd (footer out)) "c0/")

-- === Key columns ===
-- Pure: key_bang (! → grp.ent)
-- Pure: grp_toggle_inverse (!! returns to empty groups)
-- Pure: nav_grp_col (l! groups c1)
-- Pure: nav_disp_grp_first (grouped col appears at position 0)
-- Pure: dispOrder_grp_first in Nav.lean

test_key_toggle :: IO ()
test_key_toggle = do
  log "key_col"
  out <- runHask "!" "data/basic.csv" []
  assertBool "! adds key separator" (contains (header out) "\x2551")

test_key_remove :: IO ()
test_key_remove = do
  log "key_remove"
  out <- runHask "!!" "data/basic.csv" []
  assertBool "!! removes key separator" (not (contains (header out) "\x2551"))

test_key_reorder :: IO ()
test_key_reorder = do
  log "key_reorder"
  out <- runHask "l!" "data/basic.csv" []
  assertBool "Key col moves to front" (T.any (== 'b') (T.take 5 (header out)))

-- === Hide ===
-- H key bound to nav.colHide.
-- Logic covered by pure theorems: hidden_toggle_inverse, nav_hide in TestPure.

-- === Selection ===
-- Pure: key_T (T → rowSel.ent)
-- Pure: sel_accumulation (TjT produces #[0, 1])

test_row_select :: IO ()
test_row_select = do
  log "row_select"
  out <- runHask "T" "data/basic.csv" []
  assertBool "T selects row" (contains (snd (footer out)) "sel=1")

test_multi_select :: IO ()
test_multi_select = do
  log "multi_row_select"
  out <- runHask "TjT" "data/full.csv" []
  assertBool "TjT selects 2 rows" (contains (snd (footer out)) "sel=2")

-- === Stack ===
-- Pure: key_S (S → stk.ent), key_q (q → stk.dec)
-- Pure: ViewStack.update stk.ent (swap is identity on single stack)
-- Pure: ViewStack.update stk.dec (pop with parent, quit on empty)

test_stack_swap :: IO ()
test_stack_swap = do
  log "stack_swap"
  out <- runHask "S" "data/basic.csv" []
  assertBool "S swaps/dups view" (contains (fst (footer out)) "basic.csv")

test_meta_quit :: IO ()
test_meta_quit = do
  log "meta_quit"
  out <- runHask "Mq" "data/basic.csv" []
  assertBool "Mq returns from meta" (not (contains (fst (footer out)) "meta"))

test_freq_quit :: IO ()
test_freq_quit = do
  log "freq_quit"
  out <- runHask "Fq" "data/basic.csv" []
  assertBool "Fq returns from freq" (not (contains (fst (footer out)) "freq"))

-- === Info overlay ===
-- Pure: key_I (I → info.ent)
-- Pure: Info.State.update (toggle vis on/off, returns Effect.none)

-- Info overlay hidden by default; I toggles it on
test_info :: IO ()
test_info = do
  log "info"
  out <- runHask "I" "data/basic.csv" []
  assertBool "Info overlay shown after I toggle"
    (contains out "derive" || contains out "export")

-- === Cursor tracking ===
-- Pure: nav_grp_col (l! groups c1, cursor tracks)

test_key_cursor :: IO ()
test_key_cursor = do
  log "key_cursor"
  out <- runHask "l!" "data/basic.csv" []
  assertBool "Cursor tracks after key toggle" (contains (snd (footer out)) "c0/")

-- === Quit ===
-- Pure: ViewStack.update stk.dec on empty stack → Effect.quit

test_q_quit :: IO ()
test_q_quit = do
  log "q_quit_empty_stack"
  (code, _, _) <- readProcessWithExitCode tvHaskBin ["data/basic.csv", "-c", "q"] ""
  assertBool "q on empty stack exits cleanly" (code == ExitSuccess)

-- === Run all backup screen tests ===

tests :: TestTree
tests = testGroup "TestScreen"
  [ testCase "nav_down"     test_nav_down
  , testCase "nav_right"    test_nav_right
  , testCase "nav_up"       test_nav_up
  , testCase "nav_left"     test_nav_left
  , testCase "key_toggle"   test_key_toggle
  , testCase "key_remove"   test_key_remove
  , testCase "key_reorder"  test_key_reorder
  , testCase "row_select"   test_row_select
  , testCase "multi_select" test_multi_select
  , testCase "stack_swap"   test_stack_swap
  , testCase "meta_quit"    test_meta_quit
  , testCase "freq_quit"    test_freq_quit
  , testCase "info"         test_info
  , testCase "key_cursor"   test_key_cursor
  , testCase "q_quit"       test_q_quit
  ]
