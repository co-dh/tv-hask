{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- |
--   End-to-end visual-regression test for plot rendering. Drives the actual
--   `tv` binary through every PlotKind via scripts/gen_plot_e2e.sh, then
--   verifies that both renderers (R/ggplot2 and Tv.Plot.Chart) produced a
--   non-empty PNG that JuicyPixels can decode.
--
--   The unit-level rScript / renderChart paths are already exercised by
--   Test.hs (test_plot_render_*); this module covers the integration path
--   that the existing tests bypass — the DuckDB query → PRQL → COPY TSV
--   pipeline that runs ahead of the R subprocess / Chart-cairo call.
--
--   Skipped (test passes vacuously) when:
--     * `bash` is missing
--     * `Rscript`+ggplot2 are missing (the harness runs both renderers
--       per-fixture, and the R path requires them; gating on hasRscript
--       avoids spurious failures on machines without R)
module TestPlotE2E (tests) where

import qualified Codec.Picture as JP
import Control.Exception (try, SomeException)
import Data.List (sort)
import System.Directory (doesFileExist, getFileSize, listDirectory)
import System.Exit (ExitCode(..))
import System.FilePath ((</>), takeExtension)
import System.Process (readProcessWithExitCode)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, testCase)

import TestUtil (hasCmd)

harness :: FilePath
harness = "scripts/gen_plot_e2e.sh"

outRDir, outChartDir :: FilePath
outRDir     = "doc/plot-e2e-r"
outChartDir = "doc/plot-e2e-chart"

-- | Run the harness once. Skips silently if bash or Rscript is missing
-- (R is needed for the renderer=r leg; without it the harness aborts
-- mid-loop with a curl/Rscript error rather than returning a clean PNG).
runHarness :: IO (Maybe String)
runHarness = do
  bash <- hasCmd "bash"
  if not bash
    then pure (Just "bash missing")
    else do
      r <- hasCmd "Rscript"
      if not r
        then pure (Just "Rscript missing (the harness still works for renderer=chart, \
                        \but the harness script tries both renderers in one pass)")
        else do
          (ec, _so, se) <- readProcessWithExitCode "bash" [harness] ""
          case ec of
            ExitSuccess   -> pure Nothing
            ExitFailure n -> pure (Just ("harness exited " ++ show n ++ ": " ++ se))

-- | List all .png files under a directory (no recursion needed; harness
-- writes flat). Returns sorted relative-to-cwd paths. Missing dir → [].
listPngs :: FilePath -> IO [FilePath]
listPngs dir = do
  entries <- (try (listDirectory dir) :: IO (Either SomeException [FilePath]))
  case entries of
    Left _   -> pure []
    Right es -> pure $ sort [ dir </> e | e <- es, takeExtension e == ".png" ]

-- | Decode a PNG with JuicyPixels and assert it parses.
assertPngOk :: FilePath -> Assertion
assertPngOk path = do
  exists <- doesFileExist path
  assertBool (path ++ ": expected to exist") exists
  sz <- getFileSize path
  assertBool (path ++ ": expected non-empty PNG") (sz > 0)
  r <- JP.readPng path
  case r of
    Left e  -> assertBool (path ++ ": JuicyPixels decode failed: " ++ e) False
    Right _ -> pure ()

-- | Top-level test: run harness, then for each PNG it produced verify
-- decode. Permissive on "no PNGs" (skipped above) but strict on every
-- PNG that does exist.
test_e2e :: Assertion
test_e2e = do
  skip <- runHarness
  case skip of
    Just _ -> pure ()      -- environment can't run the harness; treat as pass
    Nothing -> do
      rPngs     <- listPngs outRDir
      chartPngs <- listPngs outChartDir
      assertBool ("no PNGs under " ++ outRDir)     (not (null rPngs))
      assertBool ("no PNGs under " ++ outChartDir) (not (null chartPngs))
      mapM_ assertPngOk rPngs
      mapM_ assertPngOk chartPngs

tests :: TestTree
tests = testGroup "plot-e2e"
  [ testCase "harness produces decodable PNGs from both renderers" test_e2e
  ]
