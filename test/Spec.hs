module Main where

import Test.Tasty
import qualified PureSpec
import qualified RenderSpec
import qualified DuckDBSpec
import qualified ScreenSpec
import qualified LargeDataSpec
import qualified MainSpec

main :: IO ()
main = defaultMain $ testGroup "tv-hask"
  [ PureSpec.tests
  , RenderSpec.tests
  , DuckDBSpec.tests
  , ScreenSpec.tests
  , LargeDataSpec.tests
  , MainSpec.tests
  ]
