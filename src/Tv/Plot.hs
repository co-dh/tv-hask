{-
  Plot: export table data, render via ggplot2 (R), display via kitty graphics or viu.
  X-axis = first group column, category = second group column (optional),
  Y-axis = current column under cursor (must be numeric).
  Facet = third group column (optional, small multiples).
  Interactive: in-place re-rendering with downsampling interval control.

  Literal port of Tc/Tc/Plot.lean.

  Test hooks (env vars, all optional):
    TV_PLOT_RENDERER  "r" (default) → ggplot2; "chart" → Tv.Plot.Chart (cairo)
    TV_PLOT_OUT       file path → also copy the rendered PNG here
                                  (for non-interactive visual-regression harness)
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Plot
  ( maxPoints
  , Interval(..)
  , plotTitle
  , KeyAction(..)
  , handleKey
  , rScript
  , run
  , commands
  ) where

import Control.Exception (SomeException, try)
import Control.Monad (unless, when)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Directory (copyFile, createDirectoryIfMissing, doesFileExist, findExecutable)
import System.Environment (lookupEnv)
import System.FilePath (takeDirectory)
import System.IO (IOMode(..), hClose, openFile, hGetChar)
import System.IO.Unsafe (unsafePerformIO)
import qualified System.Posix.IO as Posix
import qualified System.Posix.Terminal as TermAttrs
import qualified System.Posix.Types as Posix
import System.Process
  ( CreateProcess(..), StdStream(..), proc
  , createProcess, waitForProcess, readProcessWithExitCode
  )
import System.Exit (ExitCode(..))

import qualified Tv.Nav as Nav
import qualified Tv.Plot.Chart as Chart
import qualified Tv.Render as Render
import qualified Tv.Term as Term
import Tv.App.Types (HandlerFn, onStk)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)
import Tv.Types
  ( Cmd(..)
  , ColType(..)
  , PlotKind(..)
  , StrEnum(toString)
  , isNumeric
  , isTime
  , getD
  )
import qualified Tv.Kitty as Kitty
import qualified Tv.Log as Log
import qualified Tv.Tmp as Tmp
import Tv.View (ViewStack)
import qualified Tv.View as View

-- $setup
-- >>> :set -XOverloadedStrings
-- >>> import qualified Data.Text as T
-- >>> import Tv.Types (PlotKind(..), ColType(..))

-- | Max data points for plot (more is slow and unreadable)
--
-- >>> 100 > maxPoints
-- False
-- >>> 3000 > maxPoints
-- True
maxPoints :: Int
maxPoints = 2000

-- | Downsampling interval for interactive plot control
data Interval = Interval
  { label    :: Text  -- display label (e.g. "1s", "1m", "2x")
  , truncLen :: Int   -- SUBSTRING length for time; step for non-time
  }

-- truncLen = bucket size in seconds for time_bucket
timeIntervals :: Vector Interval
timeIntervals = V.fromList
  [ Interval "1s" 1, Interval "1m" 60, Interval "5m" 300, Interval "30m" 1800 ]

dateIntervals :: Vector Interval
dateIntervals = V.fromList
  [ Interval "1d" 86400, Interval "1M" 2592000, Interval "1Y" 31536000 ]

stepIntervals :: Int -> Vector Interval
stepIntervals baseStep =
  let s0 = if baseStep == 0 then 1 else baseStep
  in V.map (\s -> Interval (T.pack (show s) <> "x") s)
       (V.fromList [s0, s0 * 2, s0 * 4, s0 * 8, s0 * 16])

getIntervals :: ColType -> Int -> Vector Interval
getIntervals xType baseStep = case xType of
  ColTypeTime      -> timeIntervals
  ColTypeTimestamp -> timeIntervals
  ColTypeDate      -> dateIntervals
  _                -> stepIntervals baseStep

-- | Try running cmd with args, return true if it ran successfully
tryDisplay :: String -> [String] -> IO Bool
tryDisplay cmd args = do
  mExe <- findExecutable cmd
  case mExe of
    Nothing -> pure False
    Just _  -> do
      (_, _, _, ph) <- createProcess
        (proc cmd args) { std_in = Inherit, std_out = Inherit, std_err = Inherit }
      rc <- waitForProcess ph
      pure (rc == ExitSuccess)

-- | Display PNG. Native kitty graphics protocol when supported (kitty,
-- WezTerm, ghostty), then viu, then xdg-open. Drops the `kitten` external
-- dep — Tv.Kitty emits the APC sequence directly.
showPng :: String -> IO ()
showPng png = do
  kitty <- Kitty.supportsKittyGraphics
  if kitty
    then Kitty.displayPng png
    else do
      ok2 <- tryDisplay "viu" [png]
      unless ok2 $ do
        _ <- tryDisplay "xdg-open" [png]
        pure ()

-- | ANSI: clear screen and move cursor to top-left
clearScreen :: IO ()
clearScreen = TIO.putStr "\x1b[2J\x1b[H"

-- | Enter/leave alternate screen buffer so plot output doesn't bleed into other panes
altEnter :: IO ()
altEnter = TIO.putStr "\x1b[?1049h\x1b[2J\x1b[H"

-- | Saved terminal attrs from the most recent setRaw, restored by exitPlot.
-- Top-level IORef keeps the imperative setRaw/exitPlot pair (multiple call
-- sites). Empty between plot sessions; populated only while in raw mode.
savedTtyAttrs :: IORef (Maybe TermAttrs.TerminalAttributes)
savedTtyAttrs = unsafePerformIO (newIORef Nothing)
{-# NOINLINE savedTtyAttrs #-}

-- | Open /dev/tty for reading + writing.
openTty :: IO Posix.Fd
openTty = Posix.openFd "/dev/tty" Posix.ReadWrite Posix.defaultFileFlags

-- | Restore terminal after plot mode: sane + leave alt screen + re-init TUI.
-- Sane = restore the attrs setRaw saved (no-op if setRaw wasn't called).
exitPlot :: IO ()
exitPlot = do
  saved <- readIORef savedTtyAttrs
  case saved of
    Just orig -> do
      fd <- openTty
      TermAttrs.setTerminalAttributes fd orig TermAttrs.Immediately
      Posix.closeFd fd
      writeIORef savedTtyAttrs Nothing
    Nothing -> pure ()
  TIO.putStr "\x1b[?1049l"
  _ <- Term.init
  pure ()

-- | Set terminal to raw mode (single keypress without Enter). Saves the
-- previous attrs to savedTtyAttrs so exitPlot can restore them.
setRaw :: IO ()
setRaw = do
  fd   <- openTty
  orig <- TermAttrs.getTerminalAttributes fd
  writeIORef savedTtyAttrs (Just orig)
  let raw = foldl TermAttrs.withoutMode orig
              [ TermAttrs.ProcessInput
              , TermAttrs.EnableEcho
              , TermAttrs.EchoLF
              , TermAttrs.KeyboardInterrupts
              , TermAttrs.ExtendedFunctions
              , TermAttrs.StartStopOutput
              , TermAttrs.MapCRtoLF
              , TermAttrs.IgnoreCR
              , TermAttrs.ProcessOutput
              ]
  TermAttrs.setTerminalAttributes fd raw TermAttrs.Immediately
  Posix.closeFd fd

-- | Read one byte from /dev/tty (caller must be in raw mode)
readKey :: IO Char
readKey = do
  r <- try $ do
    tty <- openFile "/dev/tty" ReadMode
    c <- hGetChar tty
    hClose tty
    pure c
  case r :: Either SomeException Char of
    Right c -> pure c
    Left _  -> pure 'q'

err :: ViewStack AdbcTable -> Text -> IO (Maybe (ViewStack AdbcTable))
err s msg = do
  Log.write "plot" msg
  Render.errorPopup msg
  pure (Just s)

-- | Plot title: "density of Close" or "line: Price vs Date by Ticker"
plotTitle :: PlotKind -> Text -> Text -> Bool -> Text -> Text
plotTitle kind xName yName hasCat catName =
  let k = toString kind
  in if T.null xName
       then k <> " of " <> yName
       else k <> ": " <> yName <> " vs " <> xName
              <> (if hasCat then " by " <> catName else "")

-- | Single-column plots: no group needed, just cursor on a numeric column.
-- Returns and QQ live here because they consume just one numeric vector
-- (returns = diff/lag of y; QQ = sample quantiles of y).
singleCol :: PlotKind -> Bool
singleCol kind = kind `elem`
  [PlotHist, PlotDensity, PlotReturns, PlotQQ]

-- | Plot types that use category column as the x-axis (box/violin)
catAsX :: PlotKind -> Bool
catAsX kind = kind == PlotBox || kind == PlotViolin

-- | Plot types that use fill instead of color for category aesthetics
addsFill :: PlotKind -> Bool
addsFill kind = kind == PlotBox || kind == PlotViolin || kind == PlotArea

-- | Result of handling a keypress in interactive plot mode
data KeyAction
  = KeyQuit                 -- q: exit plot mode
  | KeyInterval Int         -- ,/.: change downsample interval
  | KeyNoop                 -- unknown key: do nothing
  deriving (Eq, Show)

-- | Pure key dispatch for interactive plot mode (testable)
handleKey :: Char -> KeyAction
handleKey key
  | key == 'q'                 = KeyQuit
  | key == '.' || key == '>'   = KeyInterval 1
  | key == ',' || key == '<'   = KeyInterval (-1)
  | otherwise                  = KeyNoop

-- | Generate R script for ggplot2 rendering
--
-- >>> "ggtitle('density of Close')" `T.isInfixOf` rScript "d.dat" "p.png" PlotDensity "" "Close" False "" False "" ColTypeOther "density of Close"
-- True
-- >>> "hjust = 0.5" `T.isInfixOf` rScript "d.dat" "p.png" PlotDensity "" "Close" False "" False "" ColTypeOther "density of Close"
-- True
-- >>> "ggtitle('line: Price vs Date by Ticker')" `T.isInfixOf` rScript "d.dat" "p.png" PlotLine "Date" "Price" True "Ticker" False "" ColTypeOther "line: Price vs Date by Ticker"
-- True
-- >>> "ggtitle" `T.isInfixOf` rScript "d.dat" "p.png" PlotLine "Date" "Price" False "" False "" ColTypeOther ""
-- False
rScript
  :: Text       -- dataPath
  -> Text       -- pngPath
  -> PlotKind
  -> Text       -- xName
  -> Text       -- yName
  -> Bool       -- hasCat
  -> Text       -- catName
  -> Bool       -- hasFacet
  -> Text       -- facetName
  -> ColType    -- xType
  -> Text       -- title
  -> Text
rScript dataPath pngPath kind xName yName hasCat catName hasFacet facetName xType title =
  let rq s = "`" <> s <> "`"
      xR = rq xName; yR = rq yName; catR = rq catName; facetR = rq facetName
      readData = "d <- read.delim('" <> dataPath
               <> "', header=TRUE, sep='\\t', colClasses='character', check.names=FALSE)\n"
      convY = "d[['" <> yName <> "']] <- as.numeric(d[['" <> yName <> "']])\n"
      -- "time" is HH:MM:SS only — prepend dummy date for as.POSIXct
      convX = case xType of
        ColTypeTime      ->
          "d[['" <> xName <> "']] <- as.POSIXct(paste('1970-01-01', d[['" <> xName <> "']]))\n"
        ColTypeTimestamp ->
          "d[['" <> xName <> "']] <- as.POSIXct(d[['" <> xName <> "']])\n"
        ColTypeDate      ->
          "d[['" <> xName <> "']] <- as.Date(d[['" <> xName <> "']])\n"
        _ ->
          if not (singleCol kind)
            then "tryCatch(d[['" <> xName <> "']] <- as.numeric(d[['" <> xName
                   <> "']]), warning=function(w) NULL)\n"
            else ""
      -- Finance pre-transforms: compute new d$ columns before ggplot so
      -- the plot itself stays a plain aes+geom. All use base R (c, diff,
      -- cumprod, cummax, filter, embed) — no extra dep on tidyquant.
      yc = "d[['" <> yName <> "']]"
      preXform = case kind of
        PlotReturns  ->
          "d[['" <> yName <> "']] <- c(NA, diff(" <> yc <> ") / head(" <> yc <> ", -1))\n"
          <> "d <- d[!is.na(d[['" <> yName <> "']]), ]\n"
        PlotCumRet   ->
          "r <- c(0, diff(" <> yc <> ") / head(" <> yc <> ", -1))\n"
          <> "d[['" <> yName <> "']] <- cumprod(1 + r) - 1\n"
        PlotDrawdown ->
          "m <- cummax(" <> yc <> ")\n"
          <> "d[['" <> yName <> "']] <- (m - " <> yc <> ") / m\n"
        PlotMA       ->
          "d$ma <- as.vector(stats::filter(" <> yc <> ", rep(1/20, 20), sides = 1))\n"
        PlotVol      ->
          "r <- c(NA, diff(" <> yc <> ") / head(" <> yc <> ", -1))\n"
          <> "d[['" <> yName <> "']] <- as.vector(stats::filter((r - mean(r, na.rm=TRUE))^2, rep(1/20, 20), sides = 1))\n"
          <> "d[['" <> yName <> "']] <- sqrt(d[['" <> yName <> "']])\n"
        PlotBB       ->
          "m <- as.vector(stats::filter(" <> yc <> ", rep(1/20, 20), sides = 1))\n"
          <> "sd20 <- as.vector(stats::filter((" <> yc <> " - m)^2, rep(1/20, 20), sides = 1))\n"
          <> "d$ma <- m; d$upper <- m + 2 * sqrt(sd20); d$lower <- m - 2 * sqrt(sd20)\n"
        _ -> ""
      aes
        | kind == PlotQQ = "aes(sample = " <> yR <> ")"
        | singleCol kind = "aes(x = " <> yR <> ")"
        | catAsX kind =
            if hasCat
              then "aes(x = " <> catR <> ", y = " <> yR <> ")"
              else "aes(x = factor(''), y = " <> yR <> ")"
        | otherwise            = "aes(x = " <> xR <> ", y = " <> yR <> ")"
      colorAes = if hasCat && not (addsFill kind)
                   then ", color = " <> catR else ""
      fillAes  = if addsFill kind
                   then ", fill = " <> (if hasCat then catR else yR)
                   else ""
      geom = case kind of
        PlotLine    -> "geom_line(linewidth = 0.5)"
        PlotBar     -> "geom_col()"
        PlotScatter -> "geom_point(size = 1.5, alpha = 0.7)"
        PlotHist    -> "geom_histogram(bins = 30, fill = 'steelblue', color = 'white')"
        PlotBox     -> "geom_boxplot()"
        PlotArea    -> "geom_area(alpha = 0.4)"
        PlotDensity -> "geom_density(fill = 'steelblue', alpha = 0.5)"
        PlotStep    -> "geom_step(linewidth = 0.5)"
        PlotViolin  -> "geom_violin() + geom_boxplot(width = 0.1, fill = 'white')"
        PlotReturns -> "geom_histogram(bins = 40, fill = 'steelblue', color = 'white')"
        PlotCumRet  -> "geom_line(linewidth = 0.5, color = 'steelblue') + scale_y_continuous(labels = scales::percent)"
        PlotDrawdown -> "geom_area(alpha = 0.5, fill = 'firebrick') + scale_y_reverse(labels = scales::percent)"
        PlotMA      -> "geom_line(linewidth = 0.4, alpha = 0.6) + geom_line(aes(y = ma), color = 'orange', linewidth = 0.8, na.rm = TRUE)"
        PlotVol     -> "geom_line(linewidth = 0.5, color = 'orange', na.rm = TRUE)"
        PlotQQ      -> "geom_qq(alpha = 0.6) + geom_qq_line(color = 'firebrick')"
        PlotBB      -> "geom_ribbon(aes(ymin = lower, ymax = upper), fill = 'steelblue', alpha = 0.2, na.rm = TRUE) + geom_line(aes(y = ma), color = 'steelblue', linewidth = 0.5, na.rm = TRUE) + geom_line(linewidth = 0.4)"
        PlotCandle  -> ""  -- handled elsewhere; needs 4-column pipeline
      timeScale = if xType == ColTypeTime
                    then " + scale_x_datetime(date_labels = '%H:%M:%S')" else ""
      facet = if hasFacet
                then " + facet_wrap(vars(" <> facetR <> "), scales = 'free_y')"
                else ""
      fillScale = if addsFill kind && not hasCat
                    then "scale_fill_viridis_c()" else "scale_fill_viridis_d()"
      titleR = if T.null title
                 then ""
                 else " + ggtitle('" <> title
                        <> "') + theme(plot.title = element_text(hjust = 0.5))"
  in "library(ggplot2)\n" <> readData <> convY <> convX <> preXform
       <> "p <- ggplot(d, " <> aes <> colorAes <> fillAes <> ") + "
       <> geom <> facet <> " + "
       <> "labs(x = '" <> xName <> "', y = '" <> yName
       <> "') + theme_minimal() + scale_color_viridis_d() + "
       <> fillScale <> timeScale <> titleR <> "\n"
       <> "ggsave('" <> pngPath <> "', p, width = 12, height = 7, dpi = 100)\n"

-- | Candlestick R script. Expects a 5-column TSV: x, open, high, low, close.
-- Wick (low..high) drawn with linerange; body (min(O,C)..max(O,C)) with
-- a thicker linerange, colored green (close >= open) or red (close < open).
rScriptCandle :: Text -> Text -> Text -> ColType -> Text -> Text
rScriptCandle dataPath pngPath xName xType title =
  let xq = "`" <> xName <> "`"
      convX = case xType of
        ColTypeTime ->
          "d$" <> xq <> " <- as.POSIXct(paste('1970-01-01', d$" <> xq <> "))\n"
        ColTypeTimestamp ->
          "d$" <> xq <> " <- as.POSIXct(d$" <> xq <> ")\n"
        ColTypeDate ->
          "d$" <> xq <> " <- as.Date(d$" <> xq <> ")\n"
        _ ->
          "tryCatch(d$" <> xq <> " <- as.numeric(d$" <> xq
            <> "), warning=function(w) NULL)\n"
      timeScale = if xType == ColTypeTime
                    then " + scale_x_datetime(date_labels = '%H:%M:%S')" else ""
      titleR = if T.null title
                 then ""
                 else " + ggtitle('" <> title
                        <> "') + theme(plot.title = element_text(hjust = 0.5))"
  in "library(ggplot2)\n"
     <> "d <- read.delim('" <> dataPath
     <> "', header=TRUE, sep='\\t', colClasses='character', check.names=FALSE)\n"
     <> "for (cn in c('open','high','low','close')) d[[cn]] <- as.numeric(d[[cn]])\n"
     <> convX
     <> "d$dir <- ifelse(d$close >= d$open, 'up', 'down')\n"
     <> "p <- ggplot(d, aes(x = " <> xq <> ")) + "
     <> "geom_linerange(aes(ymin = low, ymax = high), color = 'gray40') + "
     <> "geom_linerange(aes(ymin = pmin(open, close), ymax = pmax(open, close), color = dir), linewidth = 2.5) + "
     <> "scale_color_manual(values = c(up = 'forestgreen', down = 'firebrick'), guide = 'none') + "
     <> "labs(x = '" <> xName <> "', y = 'price') + theme_minimal()"
     <> timeScale <> titleR <> "\n"
     <> "ggsave('" <> pngPath <> "', p, width = 12, height = 7, dpi = 100)\n"

-- | Run Rscript to render plot; returns error message on failure
renderR :: Text -> IO (Maybe Text)
renderR script = do
  Log.write "plot-R" script
  rPath <- Tmp.threadPath "plot.R"
  TIO.writeFile rPath script
  (ec, _, se) <- readProcessWithExitCode "Rscript" [rPath] ""
  case ec of
    ExitFailure _ -> do
      let msg = "Rscript failed: " <> T.strip (T.pack se)
      Log.write "plot" msg
      pure (Just msg)
    ExitSuccess -> pure Nothing

-- | Render dispatch: chooses Tv.Plot.Chart (cairo) when TV_PLOT_RENDERER=chart,
-- otherwise the R/ggplot subprocess. After a successful render also copies
-- the PNG to TV_PLOT_OUT (if set) so non-interactive harnesses can grab it.
renderDispatch
  :: FilePath -- dataPath (headered TSV — already prepared by exportWithHeaders)
  -> FilePath -- pngPath  (where the renderer writes the PNG)
  -> PlotKind
  -> Text     -- xName
  -> Text     -- yName
  -> Bool     -- hasCat
  -> Text     -- catName
  -> ColType  -- xType
  -> Text     -- title
  -> Text     -- pre-built R script (used when renderer = "r")
  -> IO (Maybe Text)
renderDispatch dataPath pngPath kind xName yName hasCat catName xType title script = do
  rend <- lookupEnv "TV_PLOT_RENDERER"
  result <- case rend of
    Just "chart" ->
      Chart.renderChart dataPath pngPath kind xName yName hasCat catName xType title
    _            -> renderR script
  -- Copy to TV_PLOT_OUT regardless of renderer choice; only on success
  -- (otherwise the user-visible error message is more useful than a stale PNG).
  case result of
    Nothing -> do
      mOut <- lookupEnv "TV_PLOT_OUT"
      case mOut of
        Just outPath | not (null outPath) -> do
          createDirectoryIfMissing True (takeDirectory outPath)
          ok <- doesFileExist pngPath
          when ok $ copyFile pngPath outPath
        _ -> pure ()
    Just _  -> pure ()
  pure result

-- | Export plot data with headers for R (prepends column names to plotExport output)
exportWithHeaders
  :: AdbcTable -> Text -> Text -> Maybe Text -> Bool -> Int -> Int
  -> IO (Maybe (Vector Text))
exportWithHeaders t xName yName catName_ xIsTime step truncLen_ = do
  mCats <- Table.plotExport t xName yName catName_ xIsTime step truncLen_
  case mCats of
    Nothing -> pure Nothing
    Just cats -> do
      datPath <- Table.plotDatPath
      content <- TIO.readFile datPath
      let header = case catName_ of
            Just cn -> xName <> "\t" <> yName <> "\t" <> cn
            Nothing -> xName <> "\t" <> yName
      TIO.writeFile datPath (header <> "\n" <> content)
      pure (Just cats)

-- | Export OHLC + prepend header for R's read.delim. Matches the
-- prepend trick in 'exportWithHeaders' since copyPlot doesn't write
-- column names.
exportCandleWithHeaders
  :: AdbcTable -> Text -> (Text, Text, Text, Text) -> IO Bool
exportCandleWithHeaders t xName (o, h, l, c) = do
  ok <- Table.plotExportOhlc t xName o h l c
  if not ok then pure False else do
    datPath <- Table.plotDatPath
    content <- TIO.readFile datPath
    let header = xName <> "\topen\thigh\tlow\tclose"
    TIO.writeFile datPath (header <> "\n" <> content)
    pure True

-- | Candlestick run: grp must be [x, open, high, low, close] (5 cols).
-- Quits back to the TUI after the first render; no interactive downsample
-- loop because OHLC data is already time-bucketed.
runCandle :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
runCandle s = do
  let n  = View.nav (View.cur s)
      gp = Nav.grp n
  if V.length gp < 5
    then err s "candle: group 5 columns first — x, open, high, low, close (!)"
    else do
      let [xName, oName, hName, lName, cName] = V.toList (V.take 5 gp)
          tbl    = Nav.tbl n
          names  = Nav.colNames n
      case Nav.idxOf names xName of
        Nothing -> err s ("candle: x column '" <> xName <> "' not found")
        Just xIdx -> do
          xType0 <- pure (Ops.colType tbl xIdx)
          xType  <- sniffXType tbl xIdx xType0
          Term.shutdown
          altEnter
          setRaw
          pngPath <- Tmp.tmpPath "plot_candle.png"
          datPath <- Table.plotDatPath
          ok <- exportCandleWithHeaders tbl xName (oName, hName, lName, cName)
          if not ok
            then do exitPlot; err s "candle: data export failed"
            else do
              let title  = "Candlestick: " <> xName
                  script = rScriptCandle (T.pack datPath) (T.pack pngPath)
                             xName xType title
              renderErr <- renderDispatch datPath pngPath PlotCandle
                             xName "" False "" xType title script
              renderFrame pngPath (V.singleton (Interval "all" 1)) 0 renderErr
              -- wait for q to exit
              let waitQ = do c <- readKey
                             if c == 'q' then pure () else waitQ
              waitQ
              exitPlot
              pure (Just s)

-- | Render plot image and interval bar (clear → image → interval selector if needed)
renderFrame :: String -> Vector Interval -> Int -> Maybe Text -> IO ()
renderFrame pngPath intervals idx err_ = do
  clearScreen
  case err_ of
    Nothing  -> showPng pngPath
    Just msg -> TIO.putStrLn msg
  when (V.length intervals > 1) $ do
    -- Kitty graphics leaves the cursor at the bottom row of the image,
    -- so the status line crowds the chart. Push it down by 2 lines.
    TIO.putStr "\n\n"
    let hi s = "\x1b[33m" <> s <> "\x1b[0m"
        bar =
          T.intercalate " "
            (V.toList (V.imap
              (\i iv ->
                 if i == idx
                   then "\x1b[1;7m " <> label iv <> " \x1b[0m"
                   else " " <> label iv <> " ")
              intervals))
    TIO.putStrLn ("\r                                  " <> hi "," <> "/" <> hi "." <> ":" <> bar)

-- | Sniff xType from a sample string when DuckDB reports ColTypeStr.
-- Checks common date/time/timestamp patterns in the first non-null row.
sniffXType :: AdbcTable -> Int -> ColType -> IO ColType
sniffXType _ _ xType0 | xType0 /= ColTypeStr = pure xType0
sniffXType tbl xIdx xType0 = do
  cols <- Ops.getCols tbl (V.singleton xIdx) 0 1
  let v = T.strip $ fromMaybe "" $ cols V.!? 0 >>= (V.!? 0)
      cs = T.unpack v
      at_ i = getD cs i ' '
  pure $
    if length cs >= 19 && at_ 4 == '-' && at_ 10 == ' ' then ColTypeTimestamp
    else if length cs >= 10 && at_ 4 == '-' && at_ 7 == '-' then ColTypeDate
    else if length cs >= 8 && at_ 2 == ':' && at_ 5 == ':' then ColTypeTime
    else xType0

-- | Single-column plot (histogram/density): write the y-column to TSV, render once,
-- then block on keys until the user quits.
runSingle :: ViewStack AdbcTable -> PlotKind -> IO (Maybe (ViewStack AdbcTable))
runSingle s kind = do
  let n = View.nav (View.cur s)
      yIdx = Nav.colIdx n
      yName = Nav.colName n
      yType = Nav.colType n
  if not (isNumeric yType)
    then err s $ toString kind <> " needs a numeric column"
    else do
      Term.shutdown
      altEnter
      datPath <- Table.plotDatPath
      pngPath <- Tmp.tmpPath "plot.png"
      let nr = min (Table.nRows (Nav.tbl n)) maxPoints
      cols <- Ops.getCols (Nav.tbl n) (V.singleton yIdx) 0 nr
      let vals = fromMaybe V.empty $ cols V.!? 0
      TIO.writeFile datPath $
        yName <> "\n"
          <> T.intercalate "\n" (V.toList (V.filter (not . T.null) vals))
          <> "\n"
      let title  = plotTitle kind "" yName False ""
          script = rScript (T.pack datPath) (T.pack pngPath) kind "" yName
                     False "" False "" ColTypeOther title
      err_ <- renderDispatch datPath pngPath kind "" yName False "" ColTypeOther title script
      clearScreen
      case err_ of
        Nothing  -> showPng pngPath
        Just msg -> TIO.putStrLn msg
      setRaw
      let loop = do
            key <- readKey
            if handleKey key == KeyQuit then pure () else loop
      loop
      exitPlot
      pure (Just s)

-- | Interactive render/key loop for group plots. Re-exports + re-renders on
-- interval changes, skips re-render on unknown keys, exits on 'q'.
plotLoop
  :: AdbcTable -> PlotKind -> Text -> Text -> Bool -> Text -> Maybe Text
  -> ColType -> Bool -> Int -> String -> Vector Interval -> Text -> Text -> IO ()
plotLoop tbl kind xName yName hasCat catName exportCat xType xIsTime baseStep
         pngPath intervals script title = do
  idxRef        <- newIORef (0 :: Int)
  continueRef   <- newIORef True
  needRenderRef <- newIORef True
  let maxIdx = V.length intervals - 1
      loop = do
        cont <- readIORef continueRef
        when cont $ do
          nr_ <- readIORef needRenderRef
          when nr_ $ do
            idx <- readIORef idxRef
            let iv = fromMaybe (Interval "" 0) $ intervals V.!? idx
            Log.write "plot" $
              "kind=" <> T.pack (show kind)
                <> " interval=" <> label iv
                <> " truncLen=" <> T.pack (show (truncLen iv))
                <> " idx=" <> T.pack (show idx)
            -- export may fail (type mismatch, empty filter) — surface as overlay, not crash
            exportResult <- do
              r <- try $
                exportWithHeaders tbl xName yName exportCat xIsTime baseStep (truncLen iv)
              case r :: Either SomeException (Maybe (Vector Text)) of
                Right (Just _)  -> pure (Nothing :: Maybe Text)
                Right Nothing   -> pure (Just "export returned no data")
                Left e          -> pure $ Just $ T.pack (show e)
            err_ <- case exportResult of
              Just msg -> pure (Just msg)
              Nothing  -> do
                dp <- Table.plotDatPath
                renderDispatch dp pngPath kind xName yName hasCat catName xType title script
            renderFrame pngPath intervals idx err_
          key <- readKey
          case handleKey key of
            KeyQuit       -> writeIORef continueRef False
            KeyInterval d -> do
              idx <- readIORef idxRef
              let newIdx
                    | d > 0     = min (idx + 1) maxIdx
                    | idx > 0   = idx - 1
                    | otherwise = 0
              writeIORef needRenderRef (newIdx /= idx)
              writeIORef idxRef newIdx
            KeyNoop       -> writeIORef needRenderRef False
          loop
  loop

-- | Group plot (line/bar/scatter/box/…): needs >=1 group column for x-axis.
-- Validates args, sniffs xType if string, then enters the interactive loop.
runGroup :: ViewStack AdbcTable -> PlotKind -> IO (Maybe (ViewStack AdbcTable))
runGroup s kind = do
  let n = View.nav (View.cur s)
      names = Nav.colNames n
  if V.null (Nav.grp n)
    then err s "group a column first (!)"
    else do
      let xName = fromMaybe "" $ Nav.grp n V.!? 0
      case Nav.idxOf names xName of
        Nothing -> err s $ "x-axis column '" <> xName <> "' not found"
        Just xIdx -> do
          let hasFacet = V.length (Nav.grp n) > 2
              facetName = if hasFacet then fromMaybe "" $ Nav.grp n V.!? 1 else ""
              catName
                | hasFacet  = fromMaybe "" $ Nav.grp n V.!? 2
                | otherwise = fromMaybe "" $ Nav.grp n V.!? 1
              exportCat
                | V.length (Nav.grp n) > 2 = Just facetName
                | V.length (Nav.grp n) > 1 = Just catName
                | otherwise                = Nothing
              yName = Nav.colName n
              yType = Nav.colType n
          if V.elem yName (Nav.grp n)
            then err s "move cursor to a non-group column"
          else if not (isNumeric yType)
            then err s $ "y-axis '" <> yName <> "' must be numeric (got "
                         <> toString yType <> ")"
          else do
            let tbl = Nav.tbl n
                nr = Table.totalRows tbl
                xType0 = Ops.colType tbl xIdx
            xType <- sniffXType tbl xIdx xType0
            Log.write "plot" $
              "xType=" <> toString xType
                <> " (raw=" <> toString xType0 <> ")"
                <> " xIdx=" <> T.pack (show xIdx)
                <> " xName=" <> xName
            let xIsTime = isTime xType
                needsDownsample = nr > maxPoints
                baseStep = if needsDownsample then nr `div` maxPoints else 1
                hasCat = V.length (Nav.grp n) > 1 && not hasFacet
                intervals = if needsDownsample
                              then getIntervals xType baseStep
                              else V.singleton (Interval "all" 1)
            -- enter plot mode: shutdown TUI, alternate screen, raw mode
            Term.shutdown
            altEnter
            setRaw
            datPath <- Table.plotDatPath
            pngPath <- Tmp.tmpPath "plot.png"
            let title = plotTitle kind xName yName hasCat catName
                script = rScript (T.pack datPath) (T.pack pngPath) kind
                           xName yName hasCat catName hasFacet facetName xType title
            plotLoop tbl kind xName yName hasCat catName exportCat xType xIsTime baseStep
              pngPath intervals script title
            exitPlot
            pure (Just s)

-- | Run plot with interactive controls (in-place re-rendering).
-- Dispatches to 'runSingle' for histogram/density, 'runGroup' for others.
run :: ViewStack AdbcTable -> PlotKind -> IO (Maybe (ViewStack AdbcTable))
run s kind = do
  Log.write "plot" ("run entered, kind=" <> T.pack (show kind))
  case kind of
    PlotCandle -> runCandle s
    _ | singleCol kind -> runSingle s kind
      | otherwise      -> runGroup s kind

plotCmd :: PlotKind -> HandlerFn
plotCmd k = onStk (`run` k)

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdPlotArea    "cg" "" "Area (g=x numeric, c=y numeric)"        False "") (plotCmd PlotArea)
  , hdl (mkEntry CmdPlotLine    "cg" "" "Line (g=x numeric, c=y numeric)"        False "") (plotCmd PlotLine)
  , hdl (mkEntry CmdPlotScatter "cg" "" "Scatter (g=x numeric, c=y numeric)"     False "") (plotCmd PlotScatter)
  , hdl (mkEntry CmdPlotBar     "cg" "" "Bar (g=x categorical, c=y numeric)"     False "") (plotCmd PlotBar)
  , hdl (mkEntry CmdPlotBox     "cg" "" "Boxplot (g=x categorical, c=y numeric)" False "") (plotCmd PlotBox)
  , hdl (mkEntry CmdPlotStep    "cg" "" "Step (g=x numeric, c=y numeric)"        False "") (plotCmd PlotStep)
  , hdl (mkEntry CmdPlotHist    "c"  "" "Histogram (c=numeric column)"           False "") (plotCmd PlotHist)
  , hdl (mkEntry CmdPlotDensity "c"  "" "Density (c=numeric column)"             False "") (plotCmd PlotDensity)
  , hdl (mkEntry CmdPlotViolin  "cg" "" "Violin (g=x categorical, c=y numeric)"  False "") (plotCmd PlotViolin)
    -- finance plots: single numeric y column, time x
  , hdl (mkEntry CmdPlotReturns  "c"  "" "Finance: returns histogram"             False "") (plotCmd PlotReturns)
  , hdl (mkEntry CmdPlotCumRet   "cg" "" "Finance: cumulative returns line"       False "") (plotCmd PlotCumRet)
  , hdl (mkEntry CmdPlotDrawdown "cg" "" "Finance: drawdown curve"                False "") (plotCmd PlotDrawdown)
  , hdl (mkEntry CmdPlotMA       "cg" "" "Finance: price with 20-period SMA"      False "") (plotCmd PlotMA)
  , hdl (mkEntry CmdPlotVol      "cg" "" "Finance: rolling 20-period volatility"  False "") (plotCmd PlotVol)
  , hdl (mkEntry CmdPlotQQ       "c"  "" "Finance: Q-Q plot vs normal"            False "") (plotCmd PlotQQ)
  , hdl (mkEntry CmdPlotBB       "cg" "" "Finance: Bollinger bands (SMA ± 2σ)"    False "") (plotCmd PlotBB)
  , hdl (mkEntry CmdPlotCandle   "g"  "" "Finance: OHLC candlestick (grp = x,O,H,L,C)" False "") (plotCmd PlotCandle)
  ]
