{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-
  Native plot rendering via Chart + Chart-cairo. Replaces the R/ggplot2
  subprocess for all 17 PlotKinds. Reads the same headered TSV that the
  R path consumed (xName<TAB>yName[<TAB>cat]). Finance plots additionally
  read from data/finance/sample_ohlc.csv and run the same preprocessing
  the R script does (cumprod, cummax, rolling SMA / stddev) in Haskell.
-}
module Tv.Plot.Chart
  ( renderChart
  ) where

import Control.Exception (SomeException, try)
import Data.Default.Class (def)
import Data.List (sort)
import Data.Maybe (mapMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Control.Monad (forM_)
import Graphics.Rendering.Chart.Backend.Cairo (renderableToFile, FileFormat(..), FileOptions(..))
import Graphics.Rendering.Chart.Easy hiding (sort)
import qualified Graphics.Rendering.Chart.Grid as Grid

-- Tv.Types.PlotHist collides with Chart's PlotHist; route ours through Plot
-- as a qualified prefix at use sites.
import Tv.Types (ColType)
import qualified Tv.Types as Plot

-- Chart's Rectangle / Plottable / Layout types collide with names we'd
-- pick locally; the explicit qualified imports below isolate them.

-- | Render a plot to PNG. Returns Nothing on success, Just <error> on
-- failure or for plot kinds not yet supported by the Chart backend.
renderChart
  :: FilePath   -- dataPath (headered TSV)
  -> FilePath   -- pngPath
  -> Plot.PlotKind
  -> Text       -- xName
  -> Text       -- yName
  -> Bool       -- hasCat
  -> Text       -- catName
  -> ColType    -- xType (unused for now; future: time-axis formatters)
  -> Text       -- title
  -> IO (Maybe Text)
renderChart dataPath pngPath kind xName yName hasCat _catName _xType title =
  case kind of
    Plot.PlotLine     -> render2D (\xs ys -> toPlot $ defLine yName  xs ys) >>= save
    Plot.PlotStep     -> render2D (\xs ys -> toPlot $ defStep yName  xs ys) >>= save
    Plot.PlotScatter  -> renderScatter hasCat >>= save
    Plot.PlotArea     -> render2D (\xs ys -> areaPlot xs ys) >>= save
    Plot.PlotBar      -> renderBar  >>= save
    Plot.PlotHist     -> renderHist >>= save
    Plot.PlotDensity  -> renderDensity >>= save
    Plot.PlotBox      -> renderBoxOrViolin False >>= save
    Plot.PlotViolin   -> renderBoxOrViolin True  >>= save
    Plot.PlotReturns  -> renderFinHist returnsOf "returns" >>= save
    Plot.PlotCumRet   -> renderFinLine cumretOf  "cumret"  >>= save
    Plot.PlotDrawdown -> renderFinArea drawdownOf "drawdown" >>= save
    Plot.PlotMA       -> renderMA   >>= save
    Plot.PlotVol      -> renderVol  >>= save
    Plot.PlotQQ       -> renderQQ   >>= save
    Plot.PlotBB       -> renderBB   >>= save
    Plot.PlotCandle   -> renderCandle >>= save
  where
    save :: Either Text (Renderable ()) -> IO (Maybe Text)
    save (Left e)  = pure (Just e)
    save (Right r) = do
      let opts = FileOptions { _fo_size = (800, 600), _fo_format = PNG }
      x <- try (renderableToFile opts pngPath r) :: IO (Either SomeException (PickFn ()))
      case x of
        Left e  -> pure $ Just $ "Chart render failed: " <> T.pack (show e)
        Right _ -> pure Nothing

    render2D :: ([Double] -> [Double] -> Plot Double Double)
             -> IO (Either Text (Renderable ()))
    render2D mkP = do
      r <- readXY dataPath
      pure $ case r of
        Left e          -> Left e
        Right (xs, ys)  -> Right $ toRenderable $ layoutXY title xName yName (mkP xs ys)

    renderBar = do
      r <- readXY dataPath
      pure $ case r of
        Left e         -> Left e
        Right (xs, ys) -> Right $ toRenderable $ layoutBar title xName yName xs ys

    renderScatter False = render2D (\xs ys -> toPlot $ defPoints yName xs ys)
    renderScatter True  = do
      r <- readXYC dataPath
      pure $ case r of
        Left e   -> Left e
        Right xs -> Right $ scatterCatRenderable title xName yName xs

    renderHist = do
      r <- readY dataPath
      pure $ case r of
        Left e   -> Left e
        Right ys -> Right $ toRenderable $ layoutHist title yName ys

    renderDensity = do
      r <- readY dataPath
      pure $ case r of
        Left e   -> Left e
        Right ys -> Right $ toRenderable $ layoutDensity title yName ys

    -- box/violin: x is categorical (cat column), y is numeric. With no
    -- categories, falls back to a single group labeled "".
    renderBoxOrViolin isViolin = do
      r <- if hasCat then readXYC dataPath else fmap (fmap singleGroup) (readY dataPath)
      pure $ case r of
        Left e    -> Left e
        Right xyc -> Right $ toRenderable $
          layoutBoxOrViolin isViolin title xName yName (groupByCat xyc)
      where singleGroup ys = [(0, y, "") | y <- ys]

    -- Finance: read y column from OHLC, transform via `f`, plot as histogram.
    renderFinHist f labelTxt = do
      r <- readClose
      pure $ case r of
        Left e   -> Left e
        Right ys -> Right $ toRenderable $
          layoutHistTitled (title <> " " <> labelTxt) labelTxt (f ys)
    renderFinLine f labelTxt = do
      r <- readClose
      pure $ case r of
        Left e   -> Left e
        Right ys ->
          let zs = f ys
              xs = [0 .. fromIntegral (length zs - 1) :: Double]
          in Right $ toRenderable $ layoutXY (title <> " " <> labelTxt) "i" labelTxt
               (toPlot $ defLine labelTxt xs zs)
    renderFinArea f labelTxt = do
      r <- readClose
      pure $ case r of
        Left e   -> Left e
        Right ys ->
          -- Negate so the area falls below the x=0 baseline, matching ggplot's
          -- `scale_y_reverse` aesthetic where 0 (no drawdown) sits at the top
          -- and deeper drawdowns hang downward.
          let zs = map negate (f ys)
              xs = [0 .. fromIntegral (length zs - 1) :: Double]
              fb = plot_fillbetween_values .~ [(x, (0, y)) | (x, y) <- zip xs zs]
                 $ plot_fillbetween_style  .~ solidFillStyle (withOpacity firebrick 0.5)
                 $ def
          in Right $ toRenderable $ layoutXY (title <> " " <> labelTxt) "i" labelTxt
               (toPlot (fb :: PlotFillBetween Double Double))

    renderMA = do
      r <- readClose
      pure $ case r of
        Left e   -> Left e
        Right ys ->
          let xs = [0 .. fromIntegral (length ys - 1) :: Double]
              ma = sma 20 ys
              maPts = [(x, m) | (x, m) <- zip xs ma, not (isNaN m)]
              priceLn = plot_lines_values .~ [zip xs ys]
                      $ plot_lines_style . line_color .~ withOpacity black 0.6
                      $ plot_lines_style . line_width .~ 1.0
                      $ def
              maLn    = plot_lines_values .~ [maPts]
                      $ plot_lines_style . line_color .~ opaque orange
                      $ plot_lines_style . line_width .~ 1.8
                      $ def
              lay = layout_title .~ T.unpack (title <> " ma")
                  $ layout_x_axis . laxis_title .~ "i"
                  $ layout_y_axis . laxis_title .~ T.unpack yName
                  $ layout_plots .~ [toPlot (priceLn :: PlotLines Double Double)
                                    , toPlot (maLn :: PlotLines Double Double)]
                  $ def
          in Right (toRenderable lay)

    renderVol = do
      r <- readClose
      pure $ case r of
        Left e   -> Left e
        Right ys ->
          let xs = [0 .. fromIntegral (length ys - 1) :: Double]
              vol = rollingSd 20 (returnsOf ys)
              volPts = [(x, v) | (x, v) <- zip xs vol, not (isNaN v)]
          in Right $ toRenderable $ layoutXY (title <> " vol") "i" "vol"
               (toPlot $ (plot_lines_values .~ [volPts]
                        $ plot_lines_style . line_color .~ opaque orange
                        $ plot_lines_style . line_width .~ 1.5
                        $ def :: PlotLines Double Double))

    renderQQ = do
      r <- readClose
      pure $ case r of
        Left e   -> Left e
        Right ys ->
          let rets = returnsOf ys
              sorted = sort rets
              n  = length sorted
              theo = [ qnorm ((fromIntegral i + 0.5) / fromIntegral n)
                     | i <- [0 .. n-1] ]
              pts = plot_points_values .~ zip theo sorted
                  $ plot_points_style . point_color .~ withOpacity steelblue 0.6
                  $ plot_points_style . point_radius .~ 2
                  $ def
              -- 1st-3rd quartile slope, like geom_qq_line
              qLo = sorted !! (n `div` 4)
              qHi = sorted !! ((3 * n) `div` 4)
              tLo = qnorm 0.25; tHi = qnorm 0.75
              slope = (qHi - qLo) / (tHi - tLo)
              intercept = qLo - slope * tLo
              ln = plot_lines_values .~ [[ (t, slope*t + intercept) | t <- [head theo, last theo] ]]
                 $ plot_lines_style . line_color .~ opaque firebrick
                 $ plot_lines_style . line_width .~ 1.0
                 $ def
              lay = layout_title .~ T.unpack (title <> " qq")
                  $ layout_x_axis . laxis_title .~ "theoretical"
                  $ layout_y_axis . laxis_title .~ "sample"
                  $ layout_plots .~ [toPlot (pts :: PlotPoints Double Double)
                                    , toPlot (ln  :: PlotLines  Double Double)]
                  $ def
          in Right (toRenderable lay)

    renderBB = do
      r <- readClose
      pure $ case r of
        Left e   -> Left e
        Right ys ->
          let xs = [0 .. fromIntegral (length ys - 1) :: Double]
              ma = sma 20 ys
              sd = rollingSd 20 [y - m | (y, m) <- zip ys ma]
              upper = zipWith (\m s -> m + 2*s) ma sd
              lower = zipWith (\m s -> m - 2*s) ma sd
              -- Drop NaN-leading entries so Chart doesn't bridge from origin
              -- (mirrors ggplot's `na.rm = TRUE`).
              ribbonPts = [(x, (l, u)) | (x, l, u) <- zip3 xs lower upper
                                       , not (isNaN l), not (isNaN u)]
              maPts     = [(x, m)      | (x, m)    <- zip xs ma, not (isNaN m)]
              ribbon = plot_fillbetween_values .~ ribbonPts
                     $ plot_fillbetween_style .~ solidFillStyle (withOpacity steelblue 0.2)
                     $ def
              maLn   = plot_lines_values .~ [maPts]
                     $ plot_lines_style . line_color .~ opaque steelblue
                     $ plot_lines_style . line_width .~ 0.8
                     $ def
              priceLn = plot_lines_values .~ [zip xs ys]
                     $ plot_lines_style . line_color .~ opaque black
                     $ plot_lines_style . line_width .~ 1.0
                     $ def
              lay = layout_title .~ T.unpack (title <> " bb")
                  $ layout_x_axis . laxis_title .~ "i"
                  $ layout_y_axis . laxis_title .~ T.unpack yName
                  $ layout_plots .~ [toPlot (ribbon :: PlotFillBetween Double Double)
                                    , toPlot (maLn   :: PlotLines Double Double)
                                    , toPlot (priceLn:: PlotLines Double Double)]
                  $ def
          in Right (toRenderable lay)

    renderCandle = do
      r <- readOhlc
      pure $ case r of
        Left e   -> Left e
        Right rows ->
          let xs = [0 .. fromIntegral (length rows - 1) :: Double]
              -- Wicks: one disjoint segment per day. PlotLines accepts a
              -- list-of-segments; each is its own polyline so they don't connect.
              wick = plot_lines_values .~
                       [ [(x, l), (x, h)]
                       | (x, (_, h, l, _)) <- zip xs rows ]
                   $ plot_lines_style . line_color .~ opaque dimgray
                   $ plot_lines_style . line_width .~ 0.8
                   $ def
              -- Bodies: one narrow rectangle per day, drawn as a closed
              -- 4-vertex polyline filled via line stroke (Chart lacks a
              -- direct PlotPolygon; per-day fillbetween polygons are the
              -- workaround — each row gets its own PlotFillBetween segment
              -- with width 0.6 around the day index).
              dayBody col (x, lo, hi) =
                plot_fillbetween_values .~
                  [(x - 0.3, (lo, hi)), (x + 0.3, (lo, hi))]
                $ plot_fillbetween_style .~ solidFillStyle (opaque col)
                $ def
              upBodies   = [ toPlot (dayBody forestgreen (x, mn, mx)
                              :: PlotFillBetween Double Double)
                           | (x, (o, _, _, c)) <- zip xs rows
                           , let (mn, mx) = (min o c, max o c)
                           , c >= o ]
              downBodies = [ toPlot (dayBody firebrick (x, mn, mx)
                              :: PlotFillBetween Double Double)
                           | (x, (o, _, _, c)) <- zip xs rows
                           , let (mn, mx) = (min o c, max o c)
                           , c <  o ]
              lay = layout_title .~ T.unpack title
                  $ layout_x_axis . laxis_title .~ "i"
                  $ layout_y_axis . laxis_title .~ "price"
                  $ layout_plots .~
                      ([toPlot (wick :: PlotLines Double Double)] ++ upBodies ++ downBodies)
                  $ def
          in Right (toRenderable lay)

    -- Read OHLC fixture; returns [(open, high, low, close)] in row order.
    readOhlc :: IO (Either Text [(Double, Double, Double, Double)])
    readOhlc = do
      r <- try (TIO.readFile "data/finance/sample_ohlc.csv") :: IO (Either SomeException Text)
      pure $ case r of
        Left e -> Left $ "read OHLC failed: " <> T.pack (show e)
        Right txt ->
          let rows = drop 1 $ T.lines txt
              parse line = case T.splitOn "," line of
                (_:o:h:l:c:_) -> (,,,) <$> readD o <*> readD h <*> readD l <*> readD c
                _             -> Nothing
              parsed = mapMaybe parse rows
          in if null parsed then Left "no OHLC rows" else Right parsed

    readClose :: IO (Either Text [Double])
    readClose = fmap (fmap (map (\(_, _, _, c) -> c))) readOhlc

-- | Read a 2-column (x, y) TSV with header. Drops malformed rows.
readXY :: FilePath -> IO (Either Text ([Double], [Double]))
readXY path_ = do
  r <- try (TIO.readFile path_) :: IO (Either SomeException Text)
  pure $ case r of
    Left e -> Left $ "read TSV failed: " <> T.pack (show e)
    Right txt ->
      let rows = drop 1 $ T.lines txt    -- drop header
          xys  = mapMaybe parse2 rows
      in if null xys then Left "no usable rows"
         else Right (map fst xys, map snd xys)

-- | Read a 1-column (y) TSV with header.
readY :: FilePath -> IO (Either Text [Double])
readY path_ = do
  r <- try (TIO.readFile path_) :: IO (Either SomeException Text)
  pure $ case r of
    Left e -> Left $ "read TSV failed: " <> T.pack (show e)
    Right txt ->
      let rows = drop 1 $ T.lines txt
          ys = mapMaybe parse1 rows
      in if null ys then Left "no usable rows" else Right ys

parse2 :: Text -> Maybe (Double, Double)
parse2 line = case T.splitOn "\t" line of
  (a:b:_) -> (,) <$> readD a <*> readD b
  _       -> Nothing

-- | Read 3-col (x, y, cat) TSV; cat is a Text label.
readXYC :: FilePath -> IO (Either Text [(Double, Double, Text)])
readXYC path_ = do
  r <- try (TIO.readFile path_) :: IO (Either SomeException Text)
  pure $ case r of
    Left e -> Left $ "read TSV failed: " <> T.pack (show e)
    Right txt ->
      let rows = drop 1 $ T.lines txt
          xyc  = mapMaybe parse3 rows
      in if null xyc then Left "no usable rows" else Right xyc

parse3 :: Text -> Maybe (Double, Double, Text)
parse3 line = case T.splitOn "\t" line of
  (a:b:c:_) -> (,,) <$> readD a <*> readD b <*> Just (T.strip c)
  _         -> Nothing

parse1 :: Text -> Maybe Double
parse1 line = case T.splitOn "\t" line of
  (b:_:_) -> readD b      -- 2-col TSV: take y (second col) for hist
  [a]     -> readD a
  _       -> Nothing

readD :: Text -> Maybe Double
readD t = case reads (T.unpack (T.strip t)) of
  [(d, "")] -> Just d
  _         -> Nothing

-- ============================================================
-- Layouts (use Chart-easy's lens-based DSL)
-- ============================================================

layoutXY :: Text -> Text -> Text -> Plot Double Double -> Layout Double Double
layoutXY title_ xn yn p = layout_title .~ T.unpack title_
                       $ layout_x_axis . laxis_title .~ T.unpack xn
                       $ layout_y_axis . laxis_title .~ T.unpack yn
                       $ layout_plots .~ [p]
                       $ def

defLine, defStep, defArea :: Text -> [Double] -> [Double] -> PlotLines Double Double
defLine nm xs ys =
  plot_lines_title .~ T.unpack nm
  $ plot_lines_values .~ [zip xs ys]
  $ plot_lines_style . line_color .~ opaque steelblue
  $ plot_lines_style . line_width .~ 1.5
  $ def

defStep nm xs ys =
  -- Step plot: build a stair-step polyline from xy.
  defLine nm (stairXs xs) (stairYs ys)

defArea nm xs ys = defLine nm xs ys  -- shadow; areaPlot below is the real one

-- Filled area between (x, 0) and (x, y). Returns a Plot (not PlotLines)
-- because Chart's PlotFillBetween is its own type.
areaPlot :: [Double] -> [Double] -> Plot Double Double
areaPlot xs ys =
  let fb = plot_fillbetween_title .~ ""
         $ plot_fillbetween_values .~ [(x, (0, y)) | (x, y) <- zip xs ys]
         $ plot_fillbetween_style  .~ solidFillStyle (withOpacity steelblue 0.4)
         $ def
  in toPlot (fb :: PlotFillBetween Double Double)

stairXs, stairYs :: [a] -> [a]
stairXs xs = concat [ [a, b] | (a, b) <- zip xs (drop 1 xs) ] ++ [last xs | not (null xs)]
stairYs ys = concat [ [a, a] | a <- ys ] ++ drop 1 (reverse $ take 1 $ reverse ys)

defPoints :: Text -> [Double] -> [Double] -> PlotPoints Double Double
defPoints nm xs ys =
  plot_points_title .~ T.unpack nm
  $ plot_points_values .~ zip xs ys
  $ plot_points_style . point_color .~ withOpacity steelblue 0.7
  $ plot_points_style . point_radius .~ 2
  $ def

-- Scatter with one series per category. Built-in legend is suppressed
-- (`layout_legend .~ Nothing`); a manual legend Renderable is composed
-- to the right via Grid.besideN, matching ggplot's default placement.
scatterCatRenderable
  :: Text -> Text -> Text -> [(Double, Double, Text)] -> Renderable ()
scatterCatRenderable title_ xn yn pts =
  let groups   = groupByCat pts
      palette  = take (length groups) (cycle paletteColors)
      mkSeries (cat, xys) col =
        toPlot $ plot_points_title .~ T.unpack cat
               $ plot_points_values .~ xys
               $ plot_points_style . point_color .~ col
               $ plot_points_style . point_radius .~ 3
               $ def
      lay = layout_title .~ T.unpack title_
          $ layout_legend .~ Nothing
          $ layout_x_axis . laxis_title .~ T.unpack xn
          $ layout_y_axis . laxis_title .~ T.unpack yn
          $ layout_plots .~ zipWith mkSeries groups palette
          $ (def :: Layout Double Double)
      plotR = toRenderable lay
      legR  = legendOnRight (zip (map fst groups) palette)
  in Grid.gridToRenderable
       $ Grid.besideN
           [ Grid.weights (1, 1) $ Grid.tval plotR
           , Grid.weights (0.18, 1) $ Grid.tval legR
           ]

paletteColors :: [AlphaColour Double]
paletteColors = [opaque red, opaque steelblue, opaque forestgreen,
                 opaque purple, opaque orange]

-- Single-column legend stacked vertically. Each row: 6×6 colored swatch
-- + the category label.
legendOnRight :: [(Text, AlphaColour Double)] -> Renderable ()
legendOnRight items = fillBackground (FillStyleSolid (opaque white)) $
  Renderable
    { minsize = pure ( 80
                     , fromIntegral (length items) * rowH + 2 * pad )
    , render  = \_ -> do
        forM_ (zip [0..] items) $ \(i, (label_, col)) -> do
          let y = pad + fromIntegral i * rowH + rowH / 2
              swX = pad
              swY = y - swH / 2
          withFillStyle (FillStyleSolid col) $
            fillPath (rectPath (Rect (Point swX swY) (Point (swX + swW) (swY + swH))))
          drawText (Point (swX + swW + 6) (y + 4)) (T.unpack label_)
        pure (const Nothing)
    }
  where
    pad  = 8
    rowH = 18
    swW  = 12
    swH  = 12

-- Stable ordering: first appearance wins (matches what a viewer expects).
groupByCat :: [(Double, Double, Text)] -> [(Text, [(Double, Double)])]
groupByCat = foldr step []
  where
    step (x, y, c) acc =
      case lookup c acc of
        Just _  -> map (\(k, v) -> if k == c then (k, (x, y) : v) else (k, v)) acc
        Nothing -> acc ++ [(c, [(x, y)])]

-- Bar plot. Chart's PlotBars needs a [(x, [y])] shape — one bar per category.
layoutBar :: Text -> Text -> Text -> [Double] -> [Double] -> Layout Double Double
layoutBar title_ xn yn xs ys =
  let bars = plot_bars_titles .~ [T.unpack yn]
           $ plot_bars_values .~ [(x, [y]) | (x, y) <- zip xs ys]
           $ plot_bars_style  .~ BarsClustered
           $ plot_bars_item_styles .~ [(solidFillStyle (opaque steelblue), Nothing)]
           $ def
  in layout_title .~ T.unpack title_
   $ layout_x_axis . laxis_title .~ T.unpack xn
   $ layout_y_axis . laxis_title .~ T.unpack yn
   $ layout_plots .~ [plotBars bars]
   $ def

-- Histogram: bin into N buckets, then bar-plot.
layoutHist :: Text -> Text -> [Double] -> Layout Double Double
layoutHist title_ yn ys =
  let n      = 30
      xs     = sort ys
      lo     = head xs
      hi     = last xs
      step   = (hi - lo) / fromIntegral n
      bin v  = min (n - 1) $ max 0 $ floor ((v - lo) / step)
      counts = foldr (\v acc -> let b = bin v in take b acc ++ [acc !! b + 1] ++ drop (b + 1) acc)
                     (replicate n 0) ys
      mid i  = lo + (fromIntegral i + 0.5) * step
      pts    = [(mid i, [fromIntegral c]) | (i, c) <- zip [0..] counts]
      bars   = plot_bars_titles .~ [T.unpack yn]
             $ plot_bars_values .~ pts
             $ plot_bars_spacing .~ BarsFixGap 0 0
             $ plot_bars_item_styles .~ [(solidFillStyle (opaque steelblue), Just (solidLine 0.5 (opaque white)))]
             $ def
  in layout_title .~ T.unpack title_
   $ layout_x_axis . laxis_title .~ T.unpack yn
   $ layout_y_axis . laxis_title .~ "count"
   $ layout_plots .~ [plotBars bars]
   $ def

-- Density: trivial KDE via Gaussian kernel, then line.
layoutDensity :: Text -> Text -> [Double] -> Layout Double Double
layoutDensity title_ yn ys =
  let n     = 200
      lo    = minimum ys
      hi    = maximum ys
      pad   = (hi - lo) * 0.05
      grid  = [lo - pad + (hi - lo + 2*pad) * fromIntegral i / fromIntegral (n - 1) | i <- [0 .. n-1]]
      h     = silvermanBw ys
      k u   = exp (-0.5 * u * u) / sqrt (2 * pi)
      dens  = [ sum [ k ((x - y) / h) | y <- ys ] / (fromIntegral (length ys) * h) | x <- grid ]
      ln    = plot_lines_title .~ T.unpack yn
            $ plot_lines_values .~ [zip grid dens]
            $ plot_lines_style . line_color .~ opaque steelblue
            $ plot_lines_style . line_width .~ 1.5
            $ def
  in layout_title .~ T.unpack title_
   $ layout_x_axis . laxis_title .~ T.unpack yn
   $ layout_y_axis . laxis_title .~ "density"
   $ layout_plots .~ [toPlot (ln :: PlotLines Double Double)]
   $ def

-- Silverman's rule-of-thumb bandwidth.
silvermanBw :: [Double] -> Double
silvermanBw ys =
  let n  = fromIntegral (length ys) :: Double
      mu = sum ys / n
      sd = sqrt $ sum [(y - mu)^(2::Int) | y <- ys] / n
  in 1.06 * sd * n ** (-0.2)

-- ============================================================
-- Finance-plot helpers (mirror the R script's preXform block)
-- ============================================================

-- Simple returns: r_t = (y_t - y_{t-1}) / y_{t-1}; first row gets 0 to keep length.
returnsOf :: [Double] -> [Double]
returnsOf [] = []
returnsOf ys = 0 : zipWith (\a b -> (b - a) / a) ys (tail ys)

-- Cumulative simple returns: cumprod(1 + r) - 1
cumretOf :: [Double] -> [Double]
cumretOf ys =
  let r = returnsOf ys
      step (acc, prods) v = let p = acc * (1 + v) in (p, prods ++ [p])
      (_, ps) = foldl step (1, []) r
  in [p - 1 | p <- ps]

-- Drawdown: (cummax - y) / cummax
drawdownOf :: [Double] -> [Double]
drawdownOf [] = []
drawdownOf ys =
  let peaks = scanl1 max ys
  in [ (p - y) / p | (p, y) <- zip peaks ys ]

-- N-period simple moving average. Leading (N-1) entries are NaN to mirror
-- ggplot's `na.rm = TRUE` skip behavior (we'll replace NaN with first SMA).
sma :: Int -> [Double] -> [Double]
sma n ys =
  let len = length ys
      w   = fromIntegral n
      go i
        | i < n - 1 = 0/0
        | otherwise = sum (take n (drop (i - n + 1) ys)) / w
  in [ go i | i <- [0 .. len - 1] ]

-- N-period rolling standard deviation of `xs`; same NaN-leading convention.
rollingSd :: Int -> [Double] -> [Double]
rollingSd n xs =
  let len = length xs
      go i
        | i < n - 1 = 0/0
        | otherwise =
            let win = take n (drop (i - n + 1) xs)
                mu  = sum win / fromIntegral n
                v   = sum [(x - mu)^(2::Int) | x <- win] / fromIntegral n
            in sqrt v
  in [ go i | i <- [0 .. len - 1] ]

-- Inverse normal CDF (Beasley-Springer / Moro approximation, ~7-digit accuracy).
qnorm :: Double -> Double
qnorm p
  | p <= 0    = -1/0
  | p >= 1    =  1/0
  | otherwise =
      let q = p - 0.5
          a = [-39.6968302866538, 220.946098424521, -275.928510446969,
               138.357751867269, -30.6647980661472, 2.50662827745924]
          b = [-54.4760987982241, 161.585836858041, -155.698979859887,
               66.8013118877197, -13.2806815528857]
          c = [-0.00778489400243029, -0.322396458041136, -2.40075827716184,
               -2.54973253934373, 4.37466414146497, 2.93816398269878]
          d = [0.00778469570904146, 0.32246712907004, 2.445134137143,
               3.75440866190742]
          plow = 0.02425; phigh = 1 - plow
          poly cs r = foldr (\co acc -> acc * r + co) 0 (reverse cs)
      in if p < plow then
           let r = sqrt (-2 * log p)
           in (poly c r) / (poly d r * r + 1)
         else if p <= phigh then
           let r = q * q
           in q * (poly a r) / (poly b r * r + 1)
         else
           let r = sqrt (-2 * log (1 - p))
           in negate $ (poly c r) / (poly d r * r + 1)

-- ============================================================
-- Box / violin layouts (categorical x, numeric y)
-- ============================================================

-- Per-category five-number summary: (q1, median, q3, lo-whisker, hi-whisker)
quantiles5 :: [Double] -> (Double, Double, Double, Double, Double)
quantiles5 ys =
  let s  = sort ys
      n  = length s
      q i = s !! min (n-1) (max 0 i)
      q1 = q (n `div` 4)
      q2 = q (n `div` 2)
      q3 = q ((3 * n) `div` 4)
      iqr = q3 - q1
      lo = max (head s) (q1 - 1.5 * iqr)
      hi = min (last s) (q3 + 1.5 * iqr)
  in (q1, q2, q3, lo, hi)

layoutBoxOrViolin
  :: Bool                                   -- True = violin, False = box
  -> Text -> Text -> Text
  -> [(Text, [(Double, Double)])]           -- category groups: (cat, (_, y) pairs)
  -> Layout Double Double
layoutBoxOrViolin isViolin title_ xn yn groups =
  let n          = length groups
      xs         = [ fromIntegral i | i <- [1 .. n] ]
      yvals  c   = map snd c
      mkBoxBars  = concat
        [ let (q1, q2, q3, lo, hi) = quantiles5 (yvals pts)
              x = fromIntegral i
              boxFill = plot_fillbetween_values .~
                          [(x - 0.3, (q1, q3)), (x + 0.3, (q1, q3))]
                      $ plot_fillbetween_style .~ solidFillStyle (opaque lightgray)
                      $ def
              boxLines = plot_lines_values .~
                           [ [(x - 0.3, q1), (x + 0.3, q1)]
                           , [(x - 0.3, q3), (x + 0.3, q3)]
                           , [(x - 0.3, q1), (x - 0.3, q3)]
                           , [(x + 0.3, q1), (x + 0.3, q3)]
                           , [(x - 0.3, q2), (x + 0.3, q2)]
                           , [(x, lo), (x, q1)], [(x, q3), (x, hi)]
                           , [(x - 0.15, lo), (x + 0.15, lo)]
                           , [(x - 0.15, hi), (x + 0.15, hi)]
                           ]
                       $ plot_lines_style . line_color .~ opaque black
                       $ plot_lines_style . line_width .~ 1.0
                       $ def
          in [ toPlot (boxFill :: PlotFillBetween Double Double)
             , toPlot (boxLines :: PlotLines Double Double) ]
        | (i, (_, pts)) <- zip [1::Int ..] groups ]
      mkViolins = concat
        [ let ys      = yvals pts
              (lo, hi) = (minimum ys, maximum ys)
              ngrid   = 60
              grid    = [lo + (hi - lo) * fromIntegral k / fromIntegral (ngrid - 1)
                        | k <- [0 .. ngrid - 1]]
              h       = silvermanBw ys
              ker u   = exp (-0.5 * u * u) / sqrt (2 * pi)
              dens y  = sum [ ker ((y - v) / h) | v <- ys ]
                        / (fromIntegral (length ys) * h)
              ds      = map dens grid
              dmax    = maximum ds
              wScale  = 0.4 / dmax
              x       = fromIntegral i
              -- One thin horizontal strip per pair of consecutive grid
              -- points; width = avg kernel density of the two endpoints.
              -- fillBetween's x-as-independent paradigm forces this
              -- per-strip decomposition (a single polygon would need a
              -- PlotPolygon Chart doesn't expose).
              strip (g0, d0) (g1, d1) =
                let w = ((d0 + d1) / 2) * wScale
                    s = plot_fillbetween_values .~
                          [(x - w, (g0, g1)), (x + w, (g0, g1))]
                      $ plot_fillbetween_style .~ solidFillStyle (withOpacity steelblue 0.5)
                      $ def
                in toPlot (s :: PlotFillBetween Double Double)
              strips = zipWith strip (zip grid ds) (drop 1 (zip grid ds))
          in strips ++ snd (singleBoxOverlay i pts)
        | (i, (_, pts)) <- zip [1::Int ..] groups ]
      catLabels = [(fromIntegral i, T.unpack lab)
                  | (i, (lab, _)) <- zip [1::Int ..] groups]
      -- Patch the auto-generated x-axis to show category names at the
      -- integer positions where each box/violin is drawn (1, 2, ...).
      -- _axis_labels is [[(x, String)]]; one inner list per label row.
      catAxis ad = ad { _axis_labels = [catLabels]
                      , _axis_ticks  = [(x, 0) | (x, _) <- catLabels]
                      , _axis_grid   = [] }
      lay = layout_title .~ T.unpack title_
          $ layout_x_axis . laxis_title .~ T.unpack xn
          $ layout_y_axis . laxis_title .~ T.unpack yn
          $ layout_x_axis . laxis_override .~ catAxis
          $ layout_plots .~ (if isViolin then mkViolins else mkBoxBars)
          $ def
      _ = xs
  in lay

-- A miniature box overlay for the violin (matches ggplot's
-- geom_boxplot(width=0.1, fill='white')).
singleBoxOverlay
  :: Int -> [(Double, Double)] -> ([Plot Double Double], [Plot Double Double])
singleBoxOverlay i pts =
  let (q1, q2, q3, _lo, _hi) = quantiles5 (map snd pts)
      x = fromIntegral i
      fill = plot_fillbetween_values .~
               [(x - 0.05, (q1, q3)), (x + 0.05, (q1, q3))]
           $ plot_fillbetween_style .~ solidFillStyle (opaque white)
           $ def
      lns  = plot_lines_values .~
               [ [(x - 0.05, q1), (x + 0.05, q1)]
               , [(x - 0.05, q3), (x + 0.05, q3)]
               , [(x - 0.05, q2), (x + 0.05, q2)]
               , [(x - 0.05, q1), (x - 0.05, q3)]
               , [(x + 0.05, q1), (x + 0.05, q3)]
               ]
           $ plot_lines_style . line_color .~ opaque black
           $ plot_lines_style . line_width .~ 0.6
           $ def
  in ([], [ toPlot (fill :: PlotFillBetween Double Double)
          , toPlot (lns  :: PlotLines      Double Double) ])

-- A label-titled histogram (xLabel becomes both x-axis label and panel
-- title suffix, matching how the finance plots' R script labels e.g.
-- "Close returns" with x-axis "ret").
layoutHistTitled :: Text -> Text -> [Double] -> Layout Double Double
layoutHistTitled = layoutHist
