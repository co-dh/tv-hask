# tv-hask

Haskell port of the Lean `Tc` project at `/home/dh/repo/Tc/`.

## Porting discipline (read first)

**Port from Lean exactly — file by file, function by function, line by line.**
Make the Haskell output byte-for-byte identical to the Lean reference and make
the tests pass. Only after that do we refactor into more idiomatic Haskell.

Rules while porting:

- **One Lean file → one Haskell module.** Keep the same name, the same set of
  functions, the same order, the same comments. If Lean has `samples`,
  `colHints`, `parseDerive`, `runWith`, `run` in `Tc/Derive.lean`, then
  `src/Tv/Derive.hs` exports the same five names in the same order.
- **Don't invent abstractions Lean doesn't have.** No new record fields, no
  new helper layers, no new `Op` constructors, no new method dictionaries. If
  Lean's `AdbcTable` exposes `conn` + `Prql.Query` directly, the Haskell port
  exposes `conn` + `Prql.Query` directly — not a `_tblPipe` closure.
- **Don't skip helper functions.** If Lean calls `fzf`, the port calls `fzf`.
  If Lean builds a header from `samples ++ colHints`, the port builds it the
  same way. Missing the fzf call or the samples helper is a bug, not a
  simplification.
- **No raw SQL in feature modules.** All queries go via PRQL. Exactly one
  place compiles PRQL → SQL (`Tv.Data.DuckDB`). Feature modules call
  `Prql.pipe` / `requery`, matching how Lean calls `t.query.pipe` /
  `AdbcTable.requery`.
- **Don't disable tests to make them pass.** If a test fails after a port,
  the port is wrong — fix the port, don't mark the test `pending`. The only
  legitimate reason to skip is if the Lean reference itself skips it.
- **Output parity is the acceptance criterion.** `gen_demo.py` cast files
  from the Haskell binary must match the Lean reference casts. Render
  output, status text, tab lines, overlays — all identical.

Refactoring to idiomatic Haskell happens **after** parity is reached and
tests pass. Not before, not interleaved.

## Field access: OverloadedLabels + optics-core

Where Lean uses `gen_lenses` to generate `fieldL` bindings, the Haskell
port uses `optics-th`'s `makeFieldLabelsNoPrefix ''Record` splice and
accesses fields via `OverloadedLabels`:

```haskell
nav ^. #row % #cur         -- read cursor row (optics-core)
nav & #row % #cur .~ 5     -- set cursor row
over #hidden (`toggle` name) nav
```

This is a deliberate surface-syntax divergence from Lean — `set View.precL v s`
and `s & #prec .~ v` are the same lens applied, so parity stays intact at
the semantic level. The few places where a composed lens is used as an atom
across multiple call sites (e.g. `rowCurL`, `curViewL`, `curPrecL`) are
kept as top-level `Lens'` bindings defined via `(%)`.

Only external library in the optics layer: **`optics-core`** + **`optics-th`**.
No hand-rolled `Tv.Lens` module — it was deleted after the refactor.

## Project layout

- `src/Tv/` — library modules, one per Lean source file under `Tc/Tc/`
- `src/Tv/Data/DuckDB.hs` — the only module that may contain raw SQL / FFI
- `app/Main.hs` — entry point
- `test/` — tasty test suite; `MainSpec`, `ScreenSpec`, `RenderSpec`,
  `PureSpec`, `DuckDBSpec`
- `/home/dh/repo/Tc/` — Lean reference (read-only; this is the source of
  truth for every port decision)

## Build and test

- Build: `cabal build all`
- Test: `cabal test` (or the binary at
  `dist-newstyle/.../tv-hask-test --color=never +RTS -M2G -RTS`)
- Cast comparison: `python3 doc/gen_demo.py <name>` generates a `.cast`
  under `doc/`; compare against `/home/dh/repo/Tc/doc/<name>.cast`
