module Main where

import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main = defaultMain $ testGroup "tv-hask"
  [ testCase "placeholder" (pure ()) ]
