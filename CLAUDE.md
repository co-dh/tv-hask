# tv-hask

Terminal table viewer (originally a Haskell port of the Lean `Tc` project,
now developed independently). The Lean tree at `/home/dh/repo/Tc/` is no
longer a parity target — it can be consulted as historical reference but
the Haskell version is free to merge modules, restructure, and refactor
without preserving any 1:1 mapping.

## Project rules

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
- **No `$var` / `$(...)` / brace-expansion / quoted-var in Bash tool calls.**
  Claude Code's permission classifier rejects these and prompts every time.
  Use `xargs`, `printf | while read`, static literal values, or enumerate
  commands with `;` instead. Applies to all shell metacharacters requiring
  expansion — global rule, do not prompt.

## Field access: OverloadedLabels + optics-core

Records use `optics-th`'s `makeFieldLabelsNoPrefix ''Record` splice and
field access goes through `OverloadedLabels`:

```haskell
nav ^. #row % #cur         -- read cursor row (optics-core)
nav & #row % #cur .~ 5     -- set cursor row
over #hidden (`toggle` name) nav
```

The few places where a composed lens is used as an atom across multiple
call sites (e.g. `rowCurL`, `curViewL`, `curPrecL`) are kept as top-level
`Lens'` bindings defined via `(%)`.

Only external library in the optics layer: **`optics-core`** + **`optics-th`**.
No hand-rolled `Tv.Lens` module — it was deleted after the refactor.

**Records earn their keep at 4+ fields.** Below that, don't use
`makeFieldLabelsNoPrefix` / optics — use plain record syntax or a bare
type alias. A 1-field record is just the inner type; a 2-field record
used primarily at construction sites is clearer with `{ base = X }` than
`& #base .~ X`. Reserve optics for records where named-field access
across many call sites justifies the TH splice + import overhead.

## Sources are per-module closures

Each remote backend lives under `Tv.Source.*` and exports a `Source`
value. The record is 5 fields: `pfx`, `parent`, `grpCol`, `list`, and
`open`. `list` handles listings (noSign, path → table); `open` unifies
what used to be `enter`/`enterUrl`/`download`/`resolve` — it receives a
fully-joined path (with trailing `/` if it names a directory) and
returns an `OpenResult`:

```haskell
data OpenResult
  = OpenAsTable AdbcTable  -- open-row-as-table (osquery, pg ATTACH table)
  | OpenAsFile  FilePath   -- local path / URI ready for FileFormat.openFile
  | OpenAsDir   Text       -- URI to re-enter as a folder
  | OpenNothing            -- can't open / source doesn't support it
```

One-time setup (DuckDB ATTACH, python helper scripts) happens inside
`list`/`open` via `Core.onceFor` keyed on `pfx`. `Tv.Source` does
longest-prefix lookup plus thin runner wrappers; `Tv.Source.Core` holds
shared glue (template expansion, shell safety, cache wrapper, once-for-key
ledger). Adding a backend means dropping a new module under
`Tv.Source.*` — no central dispatcher to extend, no field sentinels.

## Project layout

- `src/Tv/` — library modules
- `src/Tv/Data/DuckDB.hs` — the only module that may contain raw SQL / FFI
- `app/Main.hs` — entry point
- `test/` — tasty test suite; `MainSpec`, `ScreenSpec`, `RenderSpec`,
  `PureSpec`, `DuckDBSpec`

## Build and test

- Build: `cabal build all`
- Test: `cabal test` (or the binary at
  `dist-newstyle/.../tv-hask-test --color=never +RTS -M2G -RTS`)
