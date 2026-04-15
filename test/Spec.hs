module Main where

import Test.Tasty

import qualified Test
import qualified TestPure
import qualified TestScreen
import qualified TestLargeData

main :: IO ()
main = defaultMain $ testGroup "tv-hask"
  [ TestPure.tests
  , TestScreen.tests
  , TestLargeData.tests
  , Test.tests
  ]
