-- | Plot: R/ggplot2 chart rendering via subprocess.
--
-- Exports data from a 'TblOps' to a temp TSV, invokes an R script (the
-- Tc-project plot.R if present, else a minimal stub dropped in $TMPDIR)
-- and returns the output PNG path. Unlike the Lean port we don't drive
-- an interactive downsample loop — one plot, one PNG.
module Tv.Plot
  ( runPlot
  , rScript
  , PlotOpt(..)
  , maxPoints
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Directory (doesFileExist, getTemporaryDirectory)
import System.FilePath ((</>))
import System.Process (readProcessWithExitCode)
import System.Exit (ExitCode(..))
import Control.Exception (try, SomeException)

import Tv.Types
import Tv.Eff (Eff, IOE, (:>), liftIO)
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- | Downsampling threshold: rows above this are stride-sampled before plotting.
-- Mirrors Tc/Tc/Plot.lean's @maxPoints@ (2000).
maxPoints :: Int
maxPoints = 2000

-- | Secondary layer hint for 'rScript'. @POther@ = plain geom, no smoother.
data PlotOpt = POther | POsmooth
  deriving (Eq, Show)

-- | Plot types that only need a single numeric column (the cursor column).
-- Per task spec: hist/density/violin. Violin here degenerates to a
-- single-category violin — the Lean version uses a group column as x.
isSingleColPlot :: PlotKind -> Bool
isSingleColPlot PKHist    = True
isSingleColPlot PKDensity = True
isSingleColPlot PKViolin  = True
isSingleColPlot _         = False

-- | Run a plot. For single-col kinds the @Int@ is the numeric column
-- index and @grp@ is ignored; for xy kinds the @Int@ is the y column
-- (cursor) and @grp@ holds the x/category indices with the first entry
-- used as the x-axis. Returns @Just pngPath@ on success or @Nothing@
-- on validation/render failure.
runPlot :: IOE :> es => PlotKind -> TblOps -> Int -> Vector Int -> Eff es (Maybe FilePath)
runPlot kind tbl curCol grp = liftIO $ do
  tmp <- getTemporaryDirectory
  let datPath = tmp </> "tv-plot.dat"
      pngPath = tmp </> "tv-plot.png"
      names   = (tbl ^. tblColNames)
      nr      = (tbl ^. tblNRows)
      stride  = max 1 (nr `div` maxPoints)
  if isSingleColPlot kind
    then do
      if not (isNumeric ((tbl ^. tblColType) curCol))
        then pure Nothing
        else do
          let yName = names V.! curCol
          writeSingleCol tbl datPath yName curCol nr stride
          renderR datPath pngPath kind "" yName

    else
      if V.null grp
        then pure Nothing  -- xy plots need at least one group column
        else do
          let xIdx  = V.head grp
              xName = names V.! xIdx
              yName = names V.! curCol
          if not (isNumeric ((tbl ^. tblColType) curCol))
            then pure Nothing
            else do
              writeXYCols tbl datPath grp curCol nr stride
              renderR datPath pngPath kind xName yName

-- | Write a single-column TSV (header + values). Empty cells are dropped
-- so geom_histogram/geom_density don't warn about NA coercion.
writeSingleCol :: TblOps -> FilePath -> Text -> Int -> Int -> Int -> IO ()
writeSingleCol tbl path yName col nr stride = do
  let rows = [0, stride .. nr - 1]
  vals <- mapM (\r -> (tbl ^. tblCellStr) r col) rows
  let body = T.unlines (filter (not . T.null) vals)
  TIO.writeFile path (yName <> "\n" <> body)

-- | Write an XY TSV with columns @grp ++ [curCol]@ and a header row.
-- Tabs/CR/LF inside cells are replaced with spaces to keep the downstream
-- read.delim() parse dumb.
writeXYCols :: TblOps -> FilePath -> Vector Int -> Int -> Int -> Int -> IO ()
writeXYCols tbl path grp curCol nr stride = do
  let cols   = V.toList grp ++ [curCol]
      names  = (tbl ^. tblColNames)
      header = T.intercalate "\t" [names V.! c | c <- cols]
      rows   = [0, stride .. nr - 1]
  body <- mapM (\r -> do
                   cells <- mapM ((tbl ^. tblCellStr) r) cols
                   pure (T.intercalate "\t" (map clean cells))) rows
  TIO.writeFile path (T.unlines (header : body))
  where
    clean = T.map (\c -> if c == '\t' || c == '\n' || c == '\r' then ' ' else c)

-- | Invoke Rscript on a resolved R file, passing positional args
-- @dat png kind x y@. x/y are empty strings for single-col plots.
renderR :: FilePath -> FilePath -> PlotKind -> Text -> Text
        -> IO (Maybe FilePath)
renderR datPath pngPath kind xName yName = do
  script <- resolveScript
  let kindArg = T.unpack (plotKindArg kind)
  r <- try (readProcessWithExitCode "Rscript"
             [script, datPath, pngPath, kindArg, T.unpack xName, T.unpack yName] "")
         :: IO (Either SomeException (ExitCode, String, String))
  case r of
    Right (ExitSuccess, _, _) -> pure (Just pngPath)
    _                         -> pure Nothing

-- | Short kind name passed to the R script as argv[3].
plotKindArg :: PlotKind -> Text
plotKindArg PKLine    = "line"
plotKindArg PKBar     = "bar"
plotKindArg PKScatter = "scatter"
plotKindArg PKHist    = "hist"
plotKindArg PKBox     = "box"
plotKindArg PKArea    = "area"
plotKindArg PKDensity = "density"
plotKindArg PKStep    = "step"
plotKindArg PKViolin  = "violin"

-- | Locate an R script. Prefer the Tc project script (sibling repo) so
-- any improvements made there flow through; fall back to a tiny inline
-- stub dropped in $TMPDIR.
resolveScript :: IO FilePath
resolveScript = do
  let tcScript = "/home/dh/repo/Tc/scripts/plot.R"
  ok <- doesFileExist tcScript
  if ok then pure tcScript else writeStubScript

-- | Drop a minimal ggplot2 script in $TMPDIR and return its path. Only
-- called when the Tc-project script is missing.
writeStubScript :: IO FilePath
writeStubScript = do
  tmp <- getTemporaryDirectory
  let p = tmp </> "tv-plot.R"
  writeFile p stubScript
  pure p

-- | Inline fallback script. Argv: @dat png kind x y@.
stubScript :: String
stubScript = unlines
  [ "#!/usr/bin/env Rscript"
  , "suppressMessages(library(ggplot2))"
  , "args <- commandArgs(trailingOnly=TRUE)"
  , "dat <- args[1]; png <- args[2]; kind <- args[3]"
  , "xn <- if (length(args) >= 4) args[4] else ''"
  , "yn <- if (length(args) >= 5) args[5] else ''"
  , "d <- read.delim(dat, header=TRUE, sep='\\t', colClasses='character', check.names=FALSE)"
  , "if (nzchar(yn)) d[[yn]] <- suppressWarnings(as.numeric(d[[yn]]))"
  , "single <- kind %in% c('hist','density','violin')"
  , "if (single) {"
  , "  col <- names(d)[1]"
  , "  d[[col]] <- suppressWarnings(as.numeric(d[[col]]))"
  , "  p <- ggplot(d, aes_string(x=sprintf('`%s`', col)))"
  , "  if (kind == 'hist')    p <- p + geom_histogram(bins=30, fill='steelblue', color='white')"
  , "  if (kind == 'density') p <- p + geom_density(fill='steelblue', alpha=0.5)"
  , "  if (kind == 'violin')  p <- p + aes_string(y=sprintf('`%s`', col), x='factor(\"\")') + geom_violin()"
  , "} else {"
  , "  tryCatch(d[[xn]] <- as.numeric(d[[xn]]), warning=function(w) NULL)"
  , "  p <- ggplot(d, aes_string(x=sprintf('`%s`', xn), y=sprintf('`%s`', yn)))"
  , "  if (kind == 'line')    p <- p + geom_line()"
  , "  if (kind == 'bar')     p <- p + geom_col()"
  , "  if (kind == 'scatter') p <- p + geom_point()"
  , "  if (kind == 'box')     p <- p + geom_boxplot()"
  , "  if (kind == 'area')    p <- p + geom_area(alpha=0.4)"
  , "  if (kind == 'step')    p <- p + geom_step()"
  , "}"
  , "p <- p + theme_minimal()"
  , "ggsave(png, p, width=12, height=7, dpi=100)"
  ]

-- ============================================================================
-- Legacy pure helper kept for potential future use by callers that want
-- to build an R script inline instead of shelling out to a file. Not used
-- by 'runPlot'.
-- ============================================================================

-- | Generate an R/ggplot2 script. Mirrors the Lean positional API.
rScript
  :: Text -> Text -> PlotKind -> Text -> Text -> Bool -> Text
  -> Bool -> Text -> PlotOpt -> Text -> Text
rScript dat png kind x y hasGrp _grp _hasFacet _facet _opt title = T.unlines $
  [ "library(ggplot2)"
  , "d <- read.table('" <> dat <> "', header=TRUE, sep='\\t')"
  , "p <- ggplot(d, aes(" <> aes <> "))"
  , geom
  ] ++ titleLines ++
  [ "ggsave('" <> png <> "', p, width=8, height=5)" ]
  where
    aes
      | T.null x && not hasGrp = "x=" <> y
      | T.null x = "x=" <> y <> ", color=grp"
      | hasGrp = "x=" <> x <> ", y=" <> y <> ", color=grp"
      | otherwise = "x=" <> x <> ", y=" <> y
    geom = case kind of
      PKLine     -> "p <- p + geom_line()"
      PKBar      -> "p <- p + geom_bar(stat='identity')"
      PKScatter  -> "p <- p + geom_point()"
      PKHist     -> "p <- p + geom_histogram()"
      PKBox      -> "p <- p + geom_boxplot()"
      PKArea     -> "p <- p + geom_area()"
      PKDensity  -> "p <- p + geom_density()"
      PKStep     -> "p <- p + geom_step()"
      PKViolin   -> "p <- p + geom_violin()"
    titleLines
      | T.null title = []
      | otherwise =
          [ "p <- p + ggtitle('" <> title <> "')"
          , "p <- p + theme(plot.title = element_text(hjust = 0.5))" ]
