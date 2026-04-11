-- | Plot: R/ggplot2 chart rendering via subprocess + kitty/viu display.
-- Only pure helpers exported here; subprocess logic lives in App.hs.
module Tv.Plot where

import Data.Text (Text)
import qualified Data.Text as T
import Tv.Types (PlotKind(..))

-- | Downsampling threshold: datasets larger than this are subsampled before plotting.
maxPoints :: Int
maxPoints = 1000

-- | Secondary axis/layer kind for rScript. ".other" means no smoother/trend overlay.
data PlotOpt = POther | POsmooth
  deriving (Eq, Show)

-- | Generate an R/ggplot2 script.
-- Args mirror the Lean positional API: datFile pngFile kind xCol yCol hasGrp grpCol
-- hasFacet facetCol opt title.  Empty title omits ggtitle() entirely; otherwise
-- adds a centered title via theme(plot.title = element_text(hjust = 0.5)).
rScript
  :: Text      -- ^ data file path
  -> Text      -- ^ output png path
  -> PlotKind  -- ^ kind (line/bar/density/...)
  -> Text      -- ^ x column (may be empty for 1-col plots)
  -> Text      -- ^ y column
  -> Bool      -- ^ has group column
  -> Text      -- ^ group column name
  -> Bool      -- ^ has facet column
  -> Text      -- ^ facet column name
  -> PlotOpt   -- ^ extra layer opt
  -> Text      -- ^ title (empty → no ggtitle)
  -> Text
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
