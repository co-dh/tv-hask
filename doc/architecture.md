# tv-hask Architecture

One diagram + one-paragraph description of how `src/Tv/` is layered. Modules
lower in a layer may import from the same layer or any layer above; no module
imports downward. Every module has an explicit export list.

## Module layering

```
┌──────────────────────────────────────────────────────────────────────────┐
│ ENTRY                                                                    │
│   app/Main.hs  →  Tv.App.Main                                            │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ ORCHESTRATION   (IO shell around the pure core)                          │
│   Tv.App.Main       — CLI parsing, init, source dispatch                 │
│   Tv.App.Common     — handler registry, dispatch, render, main loop      │
│   Tv.App.Types      — AppState + handler combinators                     │
│   Tv.AppF           — free-monad AppM + two-interpreter split (prod/test)│
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ FEATURES   (one module per user-visible capability; each exports         │
│             its own `commands` vector)                                   │
│                                                                          │
│   Tv.Nav        — cursor + selection + grouping                          │
│   Tv.Filter     — fzf search / PRQL filter / jumpCol                     │
│   Tv.Ops        — split, derive, join/union/diff                         │
│   Tv.Freq       — group-by frequency view                                │
│   Tv.Meta       — column metadata view + set-key                         │
│   Tv.Transpose  — UNPIVOT+PIVOT rows↔cols                                │
│   Tv.Diff       — stack-top pairwise Δ view                              │
│   Tv.Plot       — ggplot2 via R, kitty/viu display                       │
│   Tv.Export     — COPY TO csv/parquet/json/ndjson                        │
│   Tv.Session    — save/load ViewStack JSON                               │
│   Tv.Folder     — directory browser, find-depth control                  │
│   Tv.StatusAgg  — per-column sum/avg/count (status bar)                  │
│   Tv.Sparkline  — per-column Unicode block distribution                  │
│   Tv.FileFormat — file-ext → reader dispatch                             │
│   Tv.SourceConfig — config-driven remote sources (S3, HF, osquery…)      │
│   Tv.Ftp        — FTP `ls -l` parse helper                               │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ VIEW MODEL   (pure; no IO)                                               │
│   Tv.View       — View t + ViewStack t, rebuild, pure update+Effect      │
│   Tv.CmdConfig  — Entry/CmdInfo, key→Cmd hash cache                      │
│   Tv.Theme      — CSV theme loader + live picker                         │
│   Tv.UI.Info    — context-specific key-hint overlay                      │
│   Tv.UI.Preview — full-cell preview box (word-wrap + scroll)             │
│   Tv.Render     — NavState → RenderCtx → Term.renderTable                │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ DATA   (only place allowed to build / execute SQL)                       │
│   Tv.Data.DuckDB       — FFI primitives (Conn, Result, DataChunk)        │
│   Tv.Data.DuckDB.Conn  — query lifecycle, cellStr/cellInt/cellDbl        │
│   Tv.Data.DuckDB.Prql  — PRQL Query + `prqlc` compile shell-out          │
│   Tv.Data.DuckDB.Table — AdbcTable record + builders (fromFile, …)       │
│   Tv.Data.DuckDB.Ops   — high-level SQL helpers: transposeSql,           │
│                          colAggSql, maxSplitParts, createTempView/Table, │
│                          queryMeta, columnComment                        │
│   Tv.Data.Text         — TSV parser (from stdin / string)                │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ LEAF     (no internal Tv imports; pure utilities + FFI to libterminal)   │
│   Tv.Types      — ColType, Cmd, Effect, Op, RenderCtx, ColCache, headD…  │
│   Tv.Key        — keystroke tokenization                                 │
│   Tv.Term       — libterminal-ui FFI (render, events, colors)            │
│   Tv.Log        — ~/.cache/tv/tv.log append logger                       │
│   Tv.Tmp        — /tmp/tv-XXXXXX per-process scratch dir                 │
│   Tv.Socket     — unix-socket command channel (IPC)                      │
│   Tv.Remote     — URI path ops (join/parent/dispName)                    │
└──────────────────────────────────────────────────────────────────────────┘
```

## Invariants

1. **PRQL-only for feature modules.** Feature modules never build SQL strings.
   They call `Tv.Data.DuckDB.Prql.pipe` to compose queries and
   `Tv.Data.DuckDB.Ops.{transposeSql,colAggSql,maxSplitParts,createTempView,
   createTempTable}` for the few cases PRQL can't express (UNPIVOT,
   per-column aggregates, temp view/table plumbing).

2. **Pure core, IO shell.** `View.update` / `Freq.update` return
   `(State, Effect)` — the state is already updated; the `Effect` is a
   residual instruction the IO shell (`App.Types.runViewEffect`) executes.
   This keeps the dispatch graph testable.

3. **Two interpreters for one loop.** `Tv.AppF.AppM` is a free monad with
   three operations that differ test-vs-prod (`render`, `nextKey`,
   `readArg`). Everything else threads through `liftIO`. Production uses
   `Tv.Term` + `Tv.Socket`; tests use a keystroke vector + render-less
   interpreter.

4. **Handler registry by concatenation.** `App.Common.commands` is a plain
   `Vector (Entry, Maybe HandlerFn)` built from each feature module's own
   `commands` export. Adding a feature is a one-line concat, no central
   table to edit.

## Recent simplification (this PR)

- **Split `Tv.Util` (309 LOC, 4-subsystem grab-bag)** → `Tv.Log`, `Tv.Tmp`,
  `Tv.Socket`, `Tv.Remote`. Removes `import qualified Tv.Util as Log` /
  `as Socket` dual-alias of the same module from the same file.
- **`renderSnap` helper in `App.Common`** replaces two ~10-line duplicated
  live-preview render blocks (CmdRowSearch, CmdThemeOpen).
- **Dropped `module Tv.App.Types` re-export from `App.Common`** — feature
  modules already import `App.Types` directly.
- **Raw SQL consolidation** into `Tv.Data.DuckDB.Ops`. UNPIVOT+PIVOT,
  SUM/AVG/COUNT, `array_length(string_split_regex)`, `CREATE TEMP
  VIEW/TABLE` previously inlined in `Tv.Ops`, `Tv.Transpose`, `Tv.StatusAgg`
  now live behind named helpers.
- **Unified `ColCache k v`** in `Tv.Types` replaces two different
  `(Text, …, Text)` tuple types (`AppState.statusCache`,
  `StatusAgg.Cache`).

All behavior-preserving: no change to key bindings, cast output, or
public API beyond these internal moves.
