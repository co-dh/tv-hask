#!/usr/bin/env Rscript
# Generate reference PNG for each tv plot type using R + ggplot2.
# Mirrors the geom_* invocations from src/Tv/Plot.hs:rScript so that the
# Haskell port can be visually diffed against this baseline.
suppressPackageStartupMessages({
  library(ggplot2)
  library(scales)
})

W  <- 800
H  <- 600
DPI <- 96
OUT <- "doc/plot-ref"
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

save_plot <- function(name, p) {
  p <- p + theme_gray() + theme(plot.title = element_text(hjust = 0.5))
  ggsave(file.path(OUT, paste0(name, ".png")), p, width = W/DPI, height = H/DPI, dpi = DPI)
}

xy   <- read.csv("data/plot/line.csv")
mix  <- read.csv("data/plot/mixed.csv")
ohlc <- read.csv("data/finance/sample_ohlc.csv")
ohlc$Date <- as.Date(ohlc$Date)

# Read the same hist fixture the Haskell driver uses, so the two
# rendered PNGs can be diffed apples-to-apples (same data, different
# rendering engines).
hist_data <- read.csv("data/plot/hist.csv")
names(hist_data)[1] <- "y"
returns_data <- data.frame(r = c(0, diff(ohlc$Close) / head(ohlc$Close, -1)))
ohlc$ret     <- returns_data$r
ohlc$cumret  <- cumprod(1 + ohlc$ret) - 1
ohlc$peak    <- cummax(ohlc$Close)
ohlc$dd      <- (ohlc$peak - ohlc$Close) / ohlc$peak
ohlc$ma      <- stats::filter(ohlc$Close, rep(1/20, 20), sides = 1)
ohlc$ma      <- as.numeric(ohlc$ma)
ohlc$vol     <- as.numeric(stats::filter(ohlc$ret^2, rep(1/20, 20), sides = 1))
ohlc$vol     <- sqrt(ohlc$vol)
ohlc$upper   <- ohlc$ma + 2 * ohlc$vol
ohlc$lower   <- ohlc$ma - 2 * ohlc$vol
ohlc$dir     <- ifelse(ohlc$Close >= ohlc$Open, "up", "down")

# === plain x/y plots (data/plot/line.csv) ===
save_plot("line",     ggplot(xy, aes(x, y)) + geom_line(linewidth = 0.5)             + labs(title = "y by x"))
save_plot("bar",      ggplot(xy, aes(x, y)) + geom_col()                              + labs(title = "y by x"))
save_plot("area",     ggplot(xy, aes(x, y)) + geom_area(alpha = 0.4)                  + labs(title = "y by x"))
save_plot("step",     ggplot(xy, aes(x, y)) + geom_step(linewidth = 0.5)              + labs(title = "y by x"))

# === scatter with categorical color ===
save_plot("scatter",  ggplot(mix, aes(x, y, color = cat)) + geom_point(size = 1.5, alpha = 0.7) + labs(title = "y by x (by cat)"))

# === univariate plots ===
save_plot("hist",     ggplot(hist_data, aes(y))   + geom_histogram(bins = 30, fill = "steelblue", color = "white") + labs(title = "y distribution"))
save_plot("density",  ggplot(hist_data, aes(y))   + geom_density(fill = "steelblue", alpha = 0.5)                  + labs(title = "y density"))
save_plot("box",      ggplot(mix, aes(cat, y))    + geom_boxplot()                                                  + labs(title = "y by cat"))
save_plot("violin",   ggplot(mix, aes(cat, y))    + geom_violin() + geom_boxplot(width = 0.1, fill = "white")       + labs(title = "y by cat"))

# === finance ===
save_plot("returns",  ggplot(ohlc, aes(ret))    + geom_histogram(bins = 40, fill = "steelblue", color = "white")        + labs(title = "Close returns"))
save_plot("cumret",   ggplot(ohlc, aes(Date, cumret)) + geom_line(linewidth = 0.5, color = "steelblue") + scale_y_continuous(labels = percent) + labs(title = "Close cumulative returns"))
save_plot("drawdown", ggplot(ohlc, aes(Date, dd))      + geom_area(alpha = 0.5, fill = "firebrick") + scale_y_reverse(labels = percent) + labs(title = "Close drawdown"))
save_plot("ma",       ggplot(ohlc, aes(Date, Close)) + geom_line(linewidth = 0.4, alpha = 0.6) + geom_line(aes(y = ma), color = "orange", linewidth = 0.8, na.rm = TRUE) + labs(title = "Close with 20-period SMA"))
save_plot("vol",      ggplot(ohlc, aes(Date, vol))    + geom_line(linewidth = 0.5, color = "orange", na.rm = TRUE) + labs(title = "Close 20-period volatility"))
save_plot("qq",       ggplot(ohlc, aes(sample = ret)) + geom_qq(alpha = 0.6) + geom_qq_line(color = "firebrick") + labs(title = "Close returns Q-Q"))
save_plot("bb",
  ggplot(ohlc, aes(Date, Close))
    + geom_ribbon(aes(ymin = lower, ymax = upper), fill = "steelblue", alpha = 0.2, na.rm = TRUE)
    + geom_line(aes(y = ma), color = "steelblue", linewidth = 0.5, na.rm = TRUE)
    + geom_line(linewidth = 0.4)
    + labs(title = "Close Bollinger bands"))
save_plot("candle",
  ggplot(ohlc, aes(x = Date))
    + geom_linerange(aes(ymin = Low, ymax = High), color = "gray40")
    + geom_linerange(aes(ymin = pmin(Open, Close), ymax = pmax(Open, Close), color = dir), linewidth = 2.5)
    + scale_color_manual(values = c(up = "forestgreen", down = "firebrick"), guide = "none")
    + labs(title = "OHLC candlestick"))

cat("Wrote 17 reference PNGs to", OUT, "\n")
