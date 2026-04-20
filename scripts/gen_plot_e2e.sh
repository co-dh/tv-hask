#!/usr/bin/env bash
# End-to-end plot harness: drive the actual `tv` binary through every
# PlotKind, capturing the rendered PNG for both renderers (R/ggplot and
# Tv.Plot.Chart). For each kind we invoke tv twice — once with
# TV_PLOT_RENDERER=r, once with TV_PLOT_RENDERER=chart — into separate
# output directories under doc/plot-e2e-{r,chart}/.
#
# Why drive the real binary instead of calling Tv.Plot.rScript directly:
#   the existing diff at doc/plot-hs/ skips tv's data pipeline (DuckDB
#   query → PRQL → COPY TSV). This harness exercises that whole path so
#   the comparison is end-to-end.
#
# Test hooks (set per-invocation, see src/Tv/Plot.hs and src/Tv/Fzf.hs):
#   TV_CMD            forces the menu to dispatch a specific handler in
#                     test mode (otherwise the first menu item wins)
#   TV_PLOT_RENDERER  "r" (default) or "chart"
#   TV_PLOT_OUT       path to copy the rendered PNG to
#
# Run:
#   cabal build tv      # populate dist-newstyle/.../tv
#   bash scripts/gen_plot_e2e.sh
#
# Exit nonzero iff at least one (kind × renderer) failed to produce a
# non-empty PNG — the tasty test (test/TestPlotE2E.hs) calls this then
# inspects the resulting files.

set -uo pipefail
unset CDPATH

repo_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo_root"

tv_bin=$(cabal -v0 list-bin tv 2>/dev/null || true)
if [ -z "$tv_bin" ] || [ ! -x "$tv_bin" ]; then
  echo "ERROR: tv binary not found. Build it first: cabal build tv" >&2
  exit 2
fi

out_r="doc/plot-e2e-r"
out_chart="doc/plot-e2e-chart"
mkdir -p "$out_r" "$out_chart"

# Kind layout: <kind> | <fixture> | <key sequence> | <handler>
# - kind     : PlotArea, PlotLine, ... (used as PNG basename)
# - fixture  : input CSV (tv's data pipeline ingests + PRQL-pipes it)
# - keys     : token sequence sent via -c. `!` groups the current column
#              and advances the cursor; `<sp>` opens the menu (which the
#              TV_CMD env var then re-routes to the named handler). For
#              candle (5 grouping columns) we group five times in a row.
# - handler  : exact handler name from CmdConfig (plot.area / plot.line /
#              plot.candle / etc.) — fed to the menu via TV_CMD.
cases=(
  "PlotArea     | data/plot/line.csv             | !          | plot.area"
  "PlotLine     | data/plot/line.csv             | !          | plot.line"
  "PlotScatter  | data/plot/mixed.csv            | !          | plot.scatter"
  "PlotBar      | data/plot/line.csv             | !          | plot.bar"
  "PlotBox      | data/plot/mixed.csv            | !!         | plot.box"
  "PlotStep     | data/plot/line.csv             | !          | plot.step"
  "PlotHist     | data/plot/hist.csv             |            | plot.hist"
  "PlotDensity  | data/plot/hist.csv             |            | plot.density"
  "PlotViolin   | data/plot/mixed.csv            | !!         | plot.violin"
  "PlotReturns  | data/finance/sample_ohlc.csv   |            | plot.returns"
  "PlotCumRet   | data/finance/sample_ohlc.csv   | !          | plot.cumret"
  "PlotDrawdown | data/finance/sample_ohlc.csv   | !          | plot.drawdown"
  "PlotMA       | data/finance/sample_ohlc.csv   | !          | plot.ma"
  "PlotVol      | data/finance/sample_ohlc.csv   | !          | plot.vol"
  "PlotQQ       | data/finance/sample_ohlc.csv   |            | plot.qq"
  "PlotBB       | data/finance/sample_ohlc.csv   | !          | plot.bb"
  "PlotCandle   | data/finance/sample_ohlc.csv   | !!!!!      | plot.candle"
)

run_one() {
  local kind="$1" fixture="$2" keys="$3" handler="$4" renderer="$5" out_dir="$6"
  local out_path="$out_dir/$kind.png"
  rm -f "$out_path"
  # `<sp>` opens the menu; in test mode TV_CMD short-circuits the menu
  # to the named handler. Append a trailing `q` so plot.run's interactive
  # readKey loop returns instead of blocking on /dev/tty.
  local seq="${keys}<sp>q"
  TV_CMD="$handler" \
    TV_PLOT_RENDERER="$renderer" \
    TV_PLOT_OUT="$out_path" \
    "$tv_bin" "$fixture" -c "$seq" >/dev/null 2>&1
  if [ ! -s "$out_path" ]; then
    echo "FAIL  $kind ($renderer): no PNG at $out_path" >&2
    return 1
  fi
  printf 'OK    %-13s %-7s %s\n' "$kind" "$renderer" "$out_path"
}

failed=0
for entry in "${cases[@]}"; do
  IFS='|' read -r kind_raw fixture_raw keys_raw handler_raw <<<"$entry"
  kind=$(echo "$kind_raw"      | xargs)
  fixture=$(echo "$fixture_raw" | xargs)
  keys=$(echo "$keys_raw"       | xargs)
  handler=$(echo "$handler_raw" | xargs)
  if [ ! -f "$fixture" ]; then
    echo "SKIP  $kind: fixture missing ($fixture)" >&2
    continue
  fi
  run_one "$kind" "$fixture" "$keys" "$handler" "r"     "$out_r"     || failed=$((failed + 1))
  run_one "$kind" "$fixture" "$keys" "$handler" "chart" "$out_chart" || failed=$((failed + 1))
done

if [ "$failed" -gt 0 ]; then
  echo "Plot harness: $failed renders failed" >&2
  exit 1
fi
echo "Plot harness: all renders succeeded ($out_r, $out_chart)"
