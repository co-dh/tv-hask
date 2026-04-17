# tv-hask

Haskell port of the Lean `Tc` project at `/home/dh/repo/Tc/`. The Lean code
is a read-only reference — consult it when behavior is ambiguous, but the
Haskell tree is free to diverge in structure, abstraction, and idiom.

## Project rules

- **Output parity with the Lean reference.** `gen_demo.py` cast files from
  the Haskell binary must match the Lean reference casts. Render output,
  status text, tab lines, overlays — all identical. This is the acceptance
  criterion regardless of how the internals are organized.
- **No raw SQL in feature modules.** All queries go via PRQL. Exactly one
  place compiles PRQL → SQL (`Tv.Data.DuckDB`). Feature modules call
  `Prql.pipe` / `requery`.
- **Don't disable tests to make them pass.** If a test fails, fix the code
  or the test — don't mark it `pending`.
- **Prefer `$` to redundant parens.** Write `f $ g x` instead of `f (g x)`
  when `(g x)` is the last/only argument and contains a function
  application or operator. Nest: `guard $ not $ T.null x`, not
  `guard (not (T.null x))`. Don't use `$` for tuples (`(x, y)`),
  list literals, operator sections, or when the parens are changing
  precedence — there the parens are load-bearing.
- **Short names.** Identifiers we define (functions, types, fields,
  modules) should be at most 2 words — `queryMeta`, not `queryMetadataFromTable`.
  Stdlib/library names like `readProcessWithExitCode` are out of our
  control; alias them (`import qualified ... as Proc`) if a call site reads
  poorly.

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

**Records earn their keep at 4+ fields.** Below that, don't use
`makeFieldLabelsNoPrefix` / optics — use plain record syntax or a bare
type alias. A 1-field record is just the inner type; a 2-field record
used primarily at construction sites is clearer with `{ base = X }` than
`& #base .~ X`. Reserve optics for records where named-field access
across many call sites justifies the TH splice + import overhead.

## SourceConfig is data-driven

`Tv.SourceConfig.Config` is a plain record of `Text` fields — not a sum
type. New backends (S3, FTP, HuggingFace, …) are added by inserting a
config row, not a new constructor. The configs were originally stored in a
database table and may move to a config file. String sentinels in fields
like `listSql` (e.g. `"FTP"`) are intentional: they keep dispatch
extensible at runtime without recompilation.

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
