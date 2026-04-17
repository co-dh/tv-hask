# tv-hask Architecture

## Main loop — state machine

Every frame goes through this cycle. Arrows are mandatory; branches labelled.
Primary dispatch code is `App.Common.loopProg` (src/Tv/App/Common.hs:298).

```
                                         ┌──► return final AppState ◄── ActQuit / key=Nothing
                                         │
        ┌─────────────────────────┐      │
        │  doRender               │      │   reads AppState, paints cell buffer,
        │   (renderFrame)         │      │   flushes via Term.present
        └───────────┬─────────────┘      │
                    ▼                    │
        ┌─────────────────────────┐      │
        │  nextKey :: AppM        │      │   Prod: Term.pollEvent → Key.toKey
        │    → Maybe Text         │      │   Test: pop from keystroke Vector
        └───────────┬─────────────┘      │
                    │                    │
         Nothing ───┤                    │
                    │                    │
         Just key ──┤                    │
                    ▼                    │
        ┌─────────────────────────┐      │   drains external IPC (CLI -c / socket)
        │  Socket.pollCmd         │      │   may short-circuit into dispatchHandler
        └───────────┬─────────────┘      │
                    ▼                    │
        ┌─────────────────────────┐      │
        │  CmdConfig.keyLookup    │      │   (key, viewKind) → Maybe CmdInfo
        │    key, View.vkind      │      │   cache hit is the hot path
        └───────────┬─────────────┘      │
          Nothing ──┘                    │        (unbound key → loop)
          Just ci                        │
                    ▼                    │
        ┌─────────────────────────┐      │
        │  readArg (if isArgCmd)  │      │   prompt user for a string arg
        └───────────┬─────────────┘      │   (filter expr, file path, …)
                    ▼                    │
        ┌─────────────────────────┐      │   registry is a Vector (Entry,
        │  dispatch               │      │   Maybe HandlerFn) built by
        │   registry[ciCmd ci]    │      │   concatenating each feature
        └───────────┬─────────────┘      │   module's `commands`
                    ▼                    │
        ┌─────────────────────────┐      │   handler shape varies by feature,
        │  HandlerFn runs         │      │   but the canonical pure path is:
        │   State → CmdInfo →     │      │     View.update cur cmd arg
        │   arg → IO Action       │      │   returning (View', Effect)
        └───────────┬─────────────┘      │
                    ▼                    │
        ┌─────────────────────────┐      │
        │  runViewEffect          │      │   EffectNone   → store new View
        │   interprets Effect     │      │   EffectQuit   → ActQuit
        │                         │      │   EffectSort   → Ops.sort + rebuild
        │                         │      │   EffectFreq   → AdbcTable.freqTable
        │                         │      │   EffectExclude, EffectFetchMore…
        └───────────┬─────────────┘      │
                    │                    │
                    ▼                    │
              Action ──┬──► ActOk a''  ──┼── recurse with a''
                       │                 │
                       ├──► ActQuit    ──┘
                       │
                       └──► ActUnhandled ── recurse with a' (pre-handler state)
```

The free monad `Tv.AppF.AppM` abstracts three operations that differ
test-vs-prod: `doRender`, `nextKey`, `readArg`. Everything else threads
through `liftIO`. Two interpreters live in `App.Common` (`prodInterp`,
`testInterp`). The main loop body is interpreter-agnostic.

### Pure core / IO shell

`View.update` and `Freq.update` are pure: they return `(State, Effect)`
where the state is already updated and `Effect` is a residual instruction.
`runViewEffect` is the only place that touches DuckDB / the filesystem.
This keeps the dispatch graph testable — `TestPure` exercises the state
transitions directly, without a DB or a terminal.

## Module layering

Modules lower in a layer may import from the same layer or layers above;
no module imports downward. Every module has an explicit export list.

```
┌──────────────────────────────────────────────────────────────────────────┐
│ ENTRY                                                                    │
│   app/Main.hs  →  Tv.App.Main                                            │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ ORCHESTRATION   (the loop above lives here)                              │
│   Tv.App.Main       — CLI parsing, init, source dispatch                 │
│   Tv.App.Common     — handler registry, dispatch, render, main loop      │
│   Tv.App.Types      — AppState, Effect interpreter (runViewEffect)       │
│   Tv.AppF           — free-monad AppM + two-interpreter split            │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ FEATURES   (one module per user-visible capability; each exports         │
│             its own `commands` Vector for the registry concat)           │
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
│   Tv.Source     — per-module remote sources (S3, HF, osquery, …)         │
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
│   Tv.Data.DuckDB.Ops   — named SQL helpers: transposeSql, colAggSql,     │
│                          maxSplitParts, createTempView/Table,            │
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

1. **Typed query API for feature modules.** Feature modules never
   touch `Tv.Data.DuckDB.Prql.pipe` or the `Op` ADT directly. They
   compose queries through `Tv.Df.Prql` (stages: `filter_`, `derive`,
   `groupAgg`, `sortAsc/Desc`, `take_`, `select`, `exclude`, `distinct`,
   `append`, `join_`, `window`) with expressions built from dataframe's
   typed `Expr` GADT, then hand off to `Tv.Data.DuckDB.Table.fromPrqlText`
   which runs the result through prqlc + DuckDB. `rawStage` is an
   escape hatch for PRQL idioms dataframe's Expr can't encode yet —
   currently used for: the user's free-form text in filter/derive
   prompts, the Pct/Bar derive in Freq (window sum + s-string repeat),
   and Split's dynamic-N column bindings (s-string for
   `string_split_regex`).

2. **Pure core, IO shell.** `View.update` / `Freq.update` return
   `(State, Effect)` — the state is already updated; the `Effect` is a
   residual instruction the IO shell (`App.Types.runViewEffect`) executes.
   This is what makes the dispatch graph testable without DuckDB.

3. **Two interpreters for one loop.** `Tv.AppF.AppM` is a free monad with
   three operations that differ test-vs-prod (`render`, `nextKey`,
   `readArg`). Everything else threads through `liftIO`. Production uses
   `Tv.Term` + `Tv.Socket`; tests use a keystroke vector + render-less
   interpreter.

4. **Handler registry by concatenation.** `App.Common.commands` is a plain
   `Vector (Entry, Maybe HandlerFn)` built from each feature module's own
   `commands` export. Adding a feature is a one-line concat, no central
   table to edit.
