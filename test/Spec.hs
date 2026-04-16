module Main where

import qualified Data.Text as T
import System.Exit (exitFailure)
import Test.Tasty

import qualified Tv.Data.DuckDB.Table as AdbcTable

import qualified Test
import qualified TestPure
import qualified TestScreen
import qualified TestLargeData

main :: IO ()
main = do
  -- Mirrors Tc/test/Test.lean's main: open the DuckDB connection before any
  -- test spawns an AdbcTable.* call. Without this, tests that exercise the
  -- table API directly (plot_export, plot_render, …) fall over on
  -- "connection not initialized".
  err <- AdbcTable.init
  if not (T.null err)
    then do
      putStrLn ("Backend init failed: " <> T.unpack err)
      exitFailure
    else
      defaultMain $ testGroup "tv-hask"
        [ TestPure.tests
        , TestScreen.tests
        , TestLargeData.tests
        , Test.tests
        ]
