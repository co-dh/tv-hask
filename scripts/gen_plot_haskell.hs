-- Driver: render each Chart-cairo-supported plot kind from the same
-- fixture CSVs the R reference script uses, saving to doc/plot-hs/.
-- Run: cabal exec -- runghc -isrc scripts/gen_plot_haskell.hs
{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Control.Monad (forM_)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import System.Directory (createDirectoryIfMissing, doesFileExist, removeFile)

import qualified Tv.Plot.Chart as Chart
import Tv.Types (ColType(..), PlotKind(..))

outDir :: FilePath
outDir = "doc/plot-hs"

-- (PlotKind, fixtureCsv, xName, yName, hasCat, catName, title)
-- Finance plots ignore fixtureCsv and read data/finance/sample_ohlc.csv
-- internally; we still pass the CSV for the prepTsv side effect.
cases :: [(PlotKind, FilePath, T.Text, T.Text, Bool, T.Text, T.Text)]
cases =
  [ (PlotLine,     "data/plot/line.csv",  "x", "y", False, "",    "y by x")
  , (PlotBar,      "data/plot/line.csv",  "x", "y", False, "",    "y by x")
  , (PlotScatter,  "data/plot/mixed.csv", "x", "y", True,  "cat", "y by x (by cat)")
  , (PlotArea,     "data/plot/line.csv",  "x", "y", False, "",    "y by x")
  , (PlotStep,     "data/plot/line.csv",  "x", "y", False, "",    "y by x")
  , (PlotHist,     "data/plot/hist.csv",  "",  "price", False, "", "price distribution")
  , (PlotDensity,  "data/plot/hist.csv",  "",  "price", False, "", "price density")
  , (PlotBox,      "data/plot/mixed.csv", "cat", "y", True, "cat", "y by cat")
  , (PlotViolin,   "data/plot/mixed.csv", "cat", "y", True, "cat", "y by cat")
    -- Finance: only Close column matters; xName/yName cosmetic.
  , (PlotReturns,  "data/finance/sample_ohlc.csv", "Date", "Close", False, "", "Close")
  , (PlotCumRet,   "data/finance/sample_ohlc.csv", "Date", "Close", False, "", "Close")
  , (PlotDrawdown, "data/finance/sample_ohlc.csv", "Date", "Close", False, "", "Close")
  , (PlotMA,       "data/finance/sample_ohlc.csv", "Date", "Close", False, "", "Close")
  , (PlotVol,      "data/finance/sample_ohlc.csv", "Date", "Close", False, "", "Close")
  , (PlotQQ,       "data/finance/sample_ohlc.csv", "Date", "Close", False, "", "Close")
  , (PlotBB,       "data/finance/sample_ohlc.csv", "Date", "Close", False, "", "Close")
  , (PlotCandle,   "data/finance/sample_ohlc.csv", "Date", "Close", False, "", "OHLC candlestick")
  ]

main :: IO ()
main = do
  createDirectoryIfMissing True outDir
  -- Each fixture is x,y[,cat] CSV. Tv.Plot.Chart expects TSV with header.
  -- Convert each to a tmp TSV then render.
  forM_ cases $ \(kind, csv, x, y, hasCat, catName, title) -> do
    tmp <- prepTsv csv
    let png = outDir <> "/" <> show kind <> ".png"
    err <- Chart.renderChart tmp png kind x y hasCat catName ColTypeOther title
    case err of
      Nothing -> putStrLn ("OK " <> show kind <> " -> " <> png)
      Just e  -> putStrLn ("ERR " <> show kind <> ": " <> T.unpack e)
    exists <- doesFileExist tmp
    if exists then removeFile tmp else pure ()

prepTsv :: FilePath -> IO FilePath
prepTsv csv = do
  txt <- TIO.readFile csv
  let -- swap commas to tabs; keep header line
      tsv = T.replace "," "\t" txt
      tmp = "/tmp/tv-plot-fixture.tsv"
  TIO.writeFile tmp tsv
  pure tmp
