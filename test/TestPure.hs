-- |
--   Pure tests that don't fit in doctests: the optics round-trip (tied to
--   Nav internals), the IO resize regression, and QuickCheck property
--   tests for Nav + View pure core (navigation invariants, stack ops).
{-# LANGUAGE OverloadedStrings #-}
module TestPure (tests) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import System.Timeout (timeout)

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, assertFailure, (@?=))
import Test.Tasty.QuickCheck (testProperty, forAll, choose, Property, (===), (==>))

import Optics.Core ((^.), (.~), (&), (%))

import qualified Tv.CmdConfig as CmdConfig
import qualified Tv.Nav as Nav
import qualified Tv.View as View
import qualified Tv.Fzf as Fzf
import qualified Tv.Fzf.Match as Match
import Tv.Types (Cmd(..), ColType(..), ViewKind(..))

data MockTable = MockTable
  { mockRows  :: Int
  , mockNames :: Vector Text
  }

mockNames53 :: Vector Text
mockNames53 = V.fromList ["c0", "c1", "c2"]

mockTypes53 :: Vector ColType
mockTypes53 = V.fromList [ColTypeStr, ColTypeStr, ColTypeStr]

mock53 :: MockTable
mock53 = MockTable { mockRows = 5, mockNames = mockNames53 }

-- Build a mock NavState with nRows rows and nCols columns ("c0".."c{n-1}").
mkNav :: Int -> Int -> Nav.NavState MockTable
mkNav nRows_ nCols_ =
  let names = V.generate nCols_ (\i -> T.pack ("c" ++ show i))
      types = V.replicate nCols_ ColTypeStr
      tbl   = MockTable { mockRows = nRows_, mockNames = names }
  in Nav.newAt nRows_ nRows_ names types tbl 0 V.empty 0

-- optics-core + optics-th sanity: the generated labels on NavAxis/NavState
-- must round-trip view/set. A regression here means makeFieldLabelsNoPrefix
-- silently failed to emit a LabelOptic instance and no other code will work.
opticsTests :: TestTree
opticsTests = testGroup "optics-th labels"
  [ testCase "NavAxis #cur read + write round-trip" $ do
      -- pull an empty axis out of a fresh nav, then write #cur via label
      let n = Nav.newAt 5 5 mockNames53 mockTypes53 mock53 0 V.empty 0
          a0 = (n ^. #row) & #cur .~ 5
      (a0 ^. #cur) @?= 5
      ((a0 & #cur .~ 42) ^. #cur) @?= 42
  , testCase "NavState #row % #cur composed optic" $ do
      let n0 = Nav.newAt 5 5 mockNames53 mockTypes53 mock53 0 V.empty 0
          n1 = n0 & #row % #cur .~ 3
      (n1 ^. #row % #cur) @?= 3
  ]

-- Property tests: invariants that must hold across the pure core.

-- Nav.clamp: result is always in [lo, hi) when lo < hi, else lo.
propClampRange :: Property
propClampRange =
  forAll (choose (-100, 100)) $ \val ->
  forAll (choose (-50, 50)) $ \lo ->
  forAll (choose (1, 50)) $ \width ->
    let hi = lo + width
        r  = Nav.clamp val lo hi
    in r >= lo && r < hi

propClampEmpty :: Property
propClampEmpty =
  forAll (choose (-100, 100)) $ \val ->
  forAll (choose (-50, 50)) $ \lo ->
    Nav.clamp val lo lo === lo

-- Nav.adjOff: off is in [0, cur+1] and cur is visible [off, off+page).
propAdjOffBounds :: Property
propAdjOffBounds =
  forAll (choose (0, 1000)) $ \cur ->
  forAll (choose (-10, 1000)) $ \off ->
  forAll (choose (1, 50)) $ \page ->
    let o = Nav.adjOff cur off page
    in o >= 0 && o <= cur + 1

propAdjOffVisible :: Property
propAdjOffVisible =
  forAll (choose (0, 1000)) $ \cur ->
  forAll (choose (-10, 1000)) $ \off ->
  forAll (choose (1, 50)) $ \page ->
    let o = Nav.adjOff cur off page
    in o <= cur && cur < o + page

-- Chain two exec calls; returns Nothing if either fails.
exec2 :: Cmd -> Cmd -> Nav.NavState MockTable -> Maybe (Nav.NavState MockTable)
exec2 c1 c2 n = Nav.exec c1 n 1 >>= \n' -> Nav.exec c2 n' 1

-- Nav.exec round-trip: Inc then Dec restores the cursor in the interior.
propRowIncDec :: Property
propRowIncDec =
  forAll (choose (3, 20)) $ \nRows_ ->
  forAll (choose (2, 10)) $ \nCols_ ->
  forAll (choose (1, nRows_ - 2)) $ \r ->
    let nav0 = mkNav nRows_ nCols_ & Nav.rowCur .~ r
    in fmap (^. Nav.rowCur) (exec2 CmdRowInc CmdRowDec nav0) === Just r

propColIncDec :: Property
propColIncDec =
  forAll (choose (3, 20)) $ \nRows_ ->
  forAll (choose (3, 10)) $ \nCols_ ->
  forAll (choose (1, nCols_ - 2)) $ \c ->
    let nav0 = mkNav nRows_ nCols_ & Nav.colCur .~ c
    in fmap (^. Nav.colCur) (exec2 CmdColInc CmdColDec nav0) === Just c

-- Nav.exec CmdColGrp: toggle twice is identity when cursor stays on the
-- same column. After the first toggle, the grouped column moves to
-- display position 0; toggling again at position 0 removes it.
propColGrpToggle :: Property
propColGrpToggle =
  forAll (choose (2, 20)) $ \nRows_ ->
  forAll (choose (2, 8)) $ \nCols_ ->
  forAll (choose (0, nCols_ - 1)) $ \c ->
    let nav0 = mkNav nRows_ nCols_ & Nav.colCur .~ c
        -- After first toggle, cursor position 0 holds the newly grouped col.
        step = Nav.exec CmdColGrp nav0 1 >>= \n1 ->
               Nav.exec CmdColGrp (n1 & Nav.colCur .~ 0) 1
    in fmap (^. #grp) step === Just (nav0 ^. #grp)

-- ViewStack: pop (push s v) == Just s (stack shape preserved).
propPushPop :: Property
propPushPop =
  forAll (choose (2, 10)) $ \nRows_ ->
  forAll (choose (2, 6)) $ \nCols_ ->
    let nav0 = mkNav nRows_ nCols_
        v0   = View.new nav0 "a.csv"
        v1   = View.new nav0 "b.csv"
        s0   = View.ViewStack { View.hd = v0, View.tl = [] }
        s1   = View.push s0 v1
    in fmap (^. #hd % #path) (View.pop s1) === Just (v0 ^. #path)

-- ViewStack.swap: swap (swap s) == s for non-empty (hasParent) stack.
propSwapInvolutive :: Property
propSwapInvolutive =
  forAll (choose (2, 10)) $ \nRows_ ->
  forAll (choose (2, 6)) $ \nCols_ ->
    let nav0 = mkNav nRows_ nCols_
        v0   = View.new nav0 "a.csv"
        v1   = View.new nav0 "b.csv"
        s    = View.ViewStack { View.hd = v1, View.tl = [v0] }
        s'   = View.swap (View.swap s)
    in View.hasParent s ==>
         (s' ^. #hd % #path === s ^. #hd % #path)

-- ViewStack.dup: tl length increases by exactly 1.
propDupLength :: Property
propDupLength =
  forAll (choose (2, 10)) $ \nRows_ ->
  forAll (choose (2, 6)) $ \nCols_ ->
  forAll (choose (0, 5)) $ \tlLen ->
    let nav0 = mkNav nRows_ nCols_
        v0   = View.new nav0 "a.csv"
        s0   = View.ViewStack { View.hd = v0, View.tl = replicate tlLen v0 }
        s1   = View.dup s0
    in length (s1 ^. #tl) === length (s0 ^. #tl) + 1

-- Nav.dispOrder / idxOf: for each index i in dispOrder, names !? i is some
-- name, and idxOf names name returns Just i (names are unique).
propDispOrderIdx :: Property
propDispOrderIdx =
  forAll (choose (1, 10)) $ \nCols_ ->
  forAll (choose (0, nCols_)) $ \nGrp_ ->
    let names = V.generate nCols_ (\i -> T.pack ("c" ++ show i))
        grp   = V.take nGrp_ names
        order = Nav.dispOrder grp names
        check i = case names V.!? i of
          Nothing   -> False
          Just name -> Nav.idxOf names name == Just i
    in V.all check order

propertyTests :: TestTree
propertyTests = testGroup "properties"
  [ testProperty "clamp val lo hi in [lo, hi)"         propClampRange
  , testProperty "clamp val lo lo == lo"               propClampEmpty
  , testProperty "adjOff in [0, cur+1]"                propAdjOffBounds
  , testProperty "adjOff keeps cursor visible"         propAdjOffVisible
  , testProperty "row Inc+Dec restores interior cur"   propRowIncDec
  , testProperty "col Inc+Dec restores interior cur"   propColIncDec
  , testProperty "col grp toggle twice is identity"    propColGrpToggle
  , testProperty "pop (push s v) == Just s"            propPushPop
  , testProperty "swap involutive on non-empty stack"  propSwapInvolutive
  , testProperty "dup increases tl length by 1"        propDupLength
  , testProperty "dispOrder indices round-trip idxOf"  propDispOrderIdx
  ]

-- Regression: Fzf.cmdMode hardcoded testMode=False, so pressing space
-- in tests spawned a real fzf popup that grabbed /dev/tty and blocked
-- the user's terminal until they escaped. The contract is that with
-- testMode on, cmdMode returns the first menu handler without invoking
-- fzf at all.
test_cmdMode_testmode_skips_fzf :: IO ()
test_cmdMode_testmode_skips_fzf = do
  let cc = CmdConfig.buildCache $ V.singleton
        (CmdConfig.mkEntry CmdStkSwap "" "S" "Swap views" False "")
  mR <- timeout 2000000 $ Fzf.cmdMode True cc VkTbl (pure ())
  case mR of
    Nothing -> assertFailure "cmdMode True blocked — should return without spawning fzf"
    Just r  -> r @?= Just "stk.swap"

fzfTests :: TestTree
fzfTests = testGroup "Fzf"
  [ testCase "cmdMode testMode skips picker"    test_cmdMode_testmode_skips_fzf
  ]

-- Fuzzy match correctness. The picker ranks by score; the assertions below
-- are on which candidates match at all and which positions get highlighted —
-- those are stable across tweaks to the bonus weights.
matchTests :: TestTree
matchTests = testGroup "Fzf.Match"
  [ testCase "empty query matches everything" $
      Match.match "" "anything" @?= Just (0, [])
  , testCase "subsequence match, separator-skipped" $
      (snd <$> Match.match "abc" "a-b-c") @?= Just [0, 2, 4]
  , testCase "smartcase: lowercase query matches any case" $
      (snd <$> Match.match "abc" "ABC") @?= Just [0, 1, 2]
  , testCase "smartcase: uppercase query is case-sensitive (miss)" $
      Match.match "ABC" "abc" @?= Nothing
  , testCase "smartcase: uppercase query matches uppercase target" $
      (snd <$> Match.match "ABC" "ABCdef") @?= Just [0, 1, 2]
  , testCase "camelCase boundary picked" $
      (snd <$> Match.match "fB" "fooBar") @?= Just [0, 3]
  , testCase "no match returns Nothing" $
      Match.match "xyz" "abc" @?= Nothing
  , testCase "start-of-word ranks higher than mid-word" $ do
      let score q t = maybe minBound fst (Match.match q t)
          s1 = score "a" "alpha"      -- 'a' at word start
          s2 = score "a" "zza"        -- 'a' mid-string
      assertFailureIf (s1 <= s2) ("word-start should outrank mid, got " ++ show (s1, s2))
  , testCase "consecutive match ranks higher than split match" $ do
      -- Both candidates have the query chars at the same word-start so the
      -- only difference is whether the 2nd char is adjacent to the 1st.
      let score q t = maybe minBound fst (Match.match q t)
          s1 = score "ab" "_ab"       -- 'a','b' consecutive
          s2 = score "ab" "_a_b"      -- 'a','b' separated
      assertFailureIf (s1 <= s2) ("consecutive should outrank split, got " ++ show (s1, s2))
  , testProperty "match positions strictly increasing and in bounds" $
      forAll (choose (0 :: Int, 4)) $ \qn ->
      forAll (choose (qn, 20 :: Int)) $ \tn ->
        let q = T.take qn (T.replicate qn "a")
            t = T.take tn (T.replicate tn "a")
        in case Match.match q t of
             Just (_, poses) -> strictlyIncreasing poses && all (\p -> p >= 0 && p < tn) poses
             Nothing         -> True
  ]
  where
    assertFailureIf False _ = pure ()
    assertFailureIf True msg = assertFailure msg
    strictlyIncreasing xs = and (zipWith (<) xs (drop 1 xs))

tests :: TestTree
tests = testGroup "TestPure"
  [ opticsTests
  , propertyTests
  , fzfTests
  , matchTests
  ]
