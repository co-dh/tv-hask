-- |
--   Pure tests that don't fit in doctests: the optics round-trip (tied to
--   Nav internals) and the IO resize regression.
--   Everything else moved to `-- >>> ...` doctests in src/Tv/*.hs.
{-# LANGUAGE OverloadedStrings #-}
module TestPure (tests) where

import Data.Text (Text)
import Data.Vector (Vector)
import qualified Data.Vector as V

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

import Optics.Core ((^.), (.~), (&), (%))

import qualified Tv.Nav as Nav
import qualified Tv.Term as Term
import Tv.Types (ColType(..))

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

-- optics-core + optics-th sanity: the generated labels on NavAxis/NavState
-- must round-trip view/set. A regression here means makeFieldLabelsNoPrefix
-- silently failed to emit a LabelOptic instance and no other code will work.
opticsTests :: TestTree
opticsTests = testGroup "optics-th labels"
  [ testCase "NavAxis #cur read + write round-trip" $ do
      let a0 = Nav.defAxis & #cur .~ 5 :: Nav.NavAxis Int
      (a0 ^. #cur) @?= 5
      ((a0 & #cur .~ 42) ^. #cur) @?= 42
  , testCase "NavState #row % #cur composed optic" $ do
      let n0 = Nav.new 5 5 mockNames53 mockTypes53 mock53
          n1 = n0 & #row % #cur .~ 3
      (n1 ^. #row % #cur) @?= 3
  ]

-- Regression: width/height were only written in Term.init, so resizes
-- left every caller on pre-resize dims until restart.
resizeTests :: TestTree
resizeTests = testGroup "Term.clear resize"
  [ testCase "clear resyncs buffer dims after simulated resize" $ do
      _ <- Term.init
      Term._setScreenBufSize 40 10
      Term.clear
      w <- Term.width
      h <- Term.height
      (fromIntegral w, fromIntegral h) @?= (80 :: Int, 24 :: Int)
  ]

tests :: TestTree
tests = testGroup "TestPure"
  [ opticsTests
  , resizeTests
  ]
