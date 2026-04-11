# tv-hask — Haskell port of Tc (status & handoff)

Port of [Tc](https://github.com/co-dh/Tc), a Lean 4 terminal table viewer, to Haskell.
Experimental. Goal: "see what Haskell looks like" for the same TUI domain — compare
ergonomics against Lean 4, not to replace the Lean build.

Source of truth (Lean): `/home/dh/repo/Tc/Tc/*.lean` — ~10.8k lines across 80 files.
Port target (Haskell): `/home/dh/repo/tv-hask/src/Tv/*.hs` — ~1.1k lines, mostly stubs.

## Date of this snapshot
2026-04-11. Picking up from port started 2026-04-09 (see `ls -la src/Tv/`).

---

## 1. High-level design decisions (already made, keep unless a blocker appears)

### 1.1 Rendering: brick + vty, NOT termbox2 FFI
Tc uses termbox2 via C FFI (`c/tb2_shim.c` + `tb_cell_buffer` direct reads). We
abandon that for the port and use `brick` (declarative TUI widgets) over `vty-unix`.

**Why:** termbox2's appeal in Tc is the headless cell-buffer trick for tests
(`lean_tb_buffer_str` reads the internal cell array so `-c` tests capture what
would render without a tty). In Haskell, brick's widget tree is already pure data —
you can snapshot it for golden tests without needing a fake cell buffer.

**Cost:** brick imposes a `Brick.App AppState e Name` event loop. Test harness
needs to drive events, not keys-as-strings. Adapter lives in `Tv.Key.tokenizeKeys`
(already ported) → feed synthesized `Vty.EvKey` events to the app.

### 1.2 Database: `duckdb-ffi` + native data-chunk API, NOT ADBC, NOT `duckdb-simple`
Tc wires DuckDB via ADBC + nanoarrow (`c/adbc_core.c`, `c/nanoarrow.c` vendored —
170k lines of C). That's an Arrow C Data Interface detour the port refuses.

`duckdb-simple` was the earlier plan but it **materializes every result row into
Haskell values on the heap**, which kills the zero-copy render path (§1.8). The
port instead binds directly to libduckdb via `duckdb-ffi` (or `inline-c` as
fallback) and uses DuckDB's native data-chunk streaming API:

```
duckdb_query   → duckdb_result
duckdb_fetch_chunk(result)         → duckdb_data_chunk   (ForeignPtr, keeps chunk alive)
duckdb_data_chunk_get_vector(…, i) → duckdb_vector        (one column of the chunk)
duckdb_vector_get_data(…)          → Ptr a                (raw values)
duckdb_vector_get_validity(…)      → Ptr Word64           (null bitmap)
```

This is simpler than Arrow C Data Interface — no schema struct to parse, no
nanoarrow. Each chunk holds STANDARD_VECTOR_SIZE (2048) rows, and we keep a
`ForeignPtr duckdb_data_chunk` alive for as long as any reader needs it.
`duckdb_destroy_data_chunk` runs from the finalizer.

**`Tv.Types.Column` redesign:** becomes a handle, not a storage:
```haskell
data Column = Column
  { colChunks :: !(Vector DataChunk)   -- ForeignPtrs, zero-copy
  , colType   :: !DuckType
  , colName   :: !Text
  }
```
Readers (`colInt :: Column -> Int -> Maybe Int64`) compute `(chunkIdx, rowIdx)`
and do a `peekElemOff` on the vector's data pointer. No `Vector Int64`
materialization, ever.

**Why not ADBC?** Same reason as before: the C Data Interface + nanoarrow is
170k LoC of pain for a feature (cross-engine portability) we don't need with
one backend.

**Cruft to delete:** `c/nanoarrow.{h,c}` and `c/adbc_core.c` — copied from Tc
speculatively, now definitely dead. See §7.

### 1.3 TblOps: record-of-functions, NOT typeclass
Lean's `TblOps` is a typeclass with `MemOps`, `AdbcTable`, `KdbTable` instances.
The port has **only one backend — DuckDB** — so `MemOps`/`KdbTable` are dropped.
We still use a plain record of functions (`Tv.Types.TblOps`) with a single
`mkDbOps` constructor. Rationale: mockable for tests, no dispatch overhead from
dictionaries, and leaves a clean seam if a second backend ever reappears.

### 1.4 Lenses: `optics` (not `microlens`, not `lens`)
`optics` / `optics-th` on every record. Picked over `microlens` for the richer
combinator set (prisms, affine traversals, `Each`/`Ixed` out of the box) and
over `lens` for a smaller dep closure and no operator-soup naming clashes. Use
`Optics.State.Operators` for stateful updates inside the effectful handler.

### 1.5 Strictness: default Haskell laziness
No global `StrictData`. Let the runtime float thunks where it wants; add bangs
(`!`) or strict fields on a case-by-case basis when profiling shows a leak.
This diverges from Lean's strict-by-default semantics deliberately — matching
Lean's evaluation model is not a goal, and laziness is occasionally useful
(e.g. cell formatting that's never rendered for off-screen columns).

### 1.6 Effects: `effectful`
Run the app in `Eff es a` from the `effectful` package. Base effects:
`IOE`, `State AppState`, `Reader AppEnv` (carrying `test :: Bool`, log handle,
theme, etc.), `Error AppError`. DuckDB connection lives in a dedicated
`DuckDB :: Effect`. `effectful` over `mtl`/`fused-effects` for the speed (it's
`ReaderT IO` under the hood), good error story, and simple interpreter split
for test vs prod. This supersedes the earlier "explicit parameters, no effect
system" plan.

### 1.7 Free monad / interpreter split: NOT carrying over
Tc PR #111 added an `AppF` free monad to separate test/prod interpreters (the
three differing ops: `render`, `nextKey`, `readArg`). Between `brick`'s own
app loop and `effectful`'s interpreter story (swap a `DuckDB` handler for a
fake, swap an `Input` handler to replay `asTestKeys`), we already get the
test/prod split for free. **No hand-rolled free monad in the port.**

### 1.8 Zero-copy: yes on ingest, bounded on render
Tc's path: DuckDB writes result batches into Arrow buffers, `ArrowArrayView`
indexes into them without copying, termbox2's cell buffer is written from C.
One copy, from DuckDB's column buffer to terminal cells.

The Haskell port matches Tc on the **ingest side** and pays a bounded per-frame
cost on the **render side**:

- **Ingest (zero-copy):** §1.2's `duckdb-ffi` + data-chunk API. We hold
  `ForeignPtr duckdb_data_chunk` for each fetched chunk; `Column` readers do
  `peekElemOff` into the vector's raw `Ptr a` plus a bit-test on the validity
  mask. No `Vector Int64`/`Text` materialization of the result set — the only
  per-row allocation is the `ForeignPtr` wrapper per chunk (one per 2048 rows).
- **Render (bounded copy):** brick builds a widget tree of `Text` cells, vty
  lowers to an `Image`, then writes escape sequences. Three copies per visible
  cell. There is no brick API to hand vty a pre-rendered cell grid — the
  widget tree is the interface. But the cost is bounded by *visible* cells
  (≤ terminal size, ~3k cells), not result-set size. Even at 120Hz it's a few
  hundred KB/frame. `Brick.Widgets.Core.cached` keeps unchanged cells off the
  hot path between frames (header, status bar, scrolled-but-still-visible rows).

**Net:** the port's worst case (10M-row parquet, no pagination) no longer
blows up — chunks stream lazily and only the visible window allocates `Text`.
The earlier "materialize everything into `Column`" regression from the
`duckdb-simple` plan is gone.

**When render cost would actually matter:** a wide terminal (400+ cols) doing
sustained scrolling faster than ~30Hz. Not a realistic workload. If it ever
becomes one, the fix is a custom brick widget that batches cell formatting,
not going after vty.

---

## 2. Module port status

Legend: ★ = ported, ◐ = stub with useful skeleton, · = empty placeholder

| Module            | Lean LoC | Haskell LoC | Status | Notes                                          |
|-------------------|---------:|------------:|--------|------------------------------------------------|
| Types             |      620 |         317 | ★      | Has a known bug: `cmdFromStr` uses `Map.` unqualified. See §3.1. |
| Util              |      ~   |         158 | ★      | Log / TmpDir / Socket / Remote — pure network package, no C shim. |
| Theme             |      ~   |         177 | ★      | Builtin CSV not yet embedded — `builtinCsv = ""`. Needs `file-embed`. |
| Nav               |      201 |          50 | ★      | Covers row/col move + ColGrp/Hide/Shift. Missing: `RowSel` variants, clamp edge cases. |
| View              |      147 |         148 | ★      | `updateView` has TODO in ColExclude path; `execNavInline` duplicates Nav — collapse once Nav is complete. |
| Key               |      ~   |          50 | ★      | evToKey + tokenizeKeys. Missing: function keys (F1-F12), `<space>` token. |
| CmdConfig         |      ~   |          86 | ★      | IORef-backed caches; no entry table yet — needs `initCmds` call site with full `[Entry]` list. |
| Render            |      ~   |          35 | ◐      | `drawApp` is TODO placeholders. AppState struct defined. Needs: header row, cell grid, status bar widgets. |
| App               |      ~   |          74 | ◐      | Brick wiring skeleton; `handleEvent` only handles Esc + Quit. Needs handler map dispatch. |
| Data.DuckDB       |      ~   |           2 | ·      | Not started. Needs: connect, query, stream `duckdb_data_chunk`s, wrap as zero-copy `Column` handles. See §1.2 / §3.3. |
| Data.Prql         |      ~   |           2 | ·      | Not started. Shell out to `prqlc compile -t sql.duckdb` (same as Lean). |
| Filter            |      160 |           2 | ·      | Fzf-driven filter UI. Blocked on Fzf.hs. |
| Folder            |      ~   |           2 | ·      | Backend pattern (list/parent/resolve/download). |
| Fzf               |      ~   |           2 | ·      | Subprocess wrapper; no helper-script workaround needed since we don't use `--tmux`. |
| Plot              |      ~   |           2 | ·      | Shells out to R/ggplot2 like Lean does. |
| Join/Split/Derive |      ~   |           2 | ·      | All PRQL-op builders. Small. |
| Meta              |      ~   |           2 | ·      | parquet_metadata() view + enrichComments. |
| Session           |      ~   |           2 | ·      | JSON save/restore via aeson. |
| Export            |      ~   |           2 | ·      | COPY ... TO shellout. |
| Diff              |      ~   |           2 | ·      | Two-view column-equality detection. |
| Transpose         |      ~   |           2 | ·      | UNPIVOT in DuckDB. |
| SourceConfig      |      ~   |           2 | ·      | tc_sources config table — port at same time as Data.DuckDB. |

**The big three not-yet-done:** Data.DuckDB, Render (real widgets), App (handler map).

---

## 3. Known blockers

### 3.1 Types.hs compile bug
```haskell
cmdFromStr t = Map.lookup t _cmdStrMap
  where _cmdStrMap = Map.fromList [(cmdStr c, c) | c <- [minBound..maxBound]]
```
`Data.Map.Strict` is imported as `Map` type only, not qualified. Add:
```haskell
import qualified Data.Map.Strict as Map
```
at the top of `Types.hs`. Also `Data.Map.Strict (Map)` import of the *type*
may conflict — keep both (`import Data.Map.Strict (Map); import qualified Data.Map.Strict as Map`).

### 3.2 Arch Linux dyn_hi / cabal build wall  **← CURRENT BLOCKER**

`cabal build` fails during dep-compile of `time-compat-1.9.9`, `uuid-types-1.0.6`,
`QuickCheck`, `scientific`, `these` etc. Error:
```
Could not find module 'Data.Hashable'
There are files missing in the 'hashable-1.4.7.0' package,
try running 'ghc-pkg check'.
```

**Root cause:** Arch Linux ships `haskell-hashable` and `haskell-random` (the
packages cabal picks from the global DB) with **only `.dyn_hi` files, no `.hi`**.
When cabal compiles a transitive Hackage dep like `time-compat` from source, ghc
runs in default (static) mode and looks for `Data.Hashable.hi`. That file doesn't
exist → build fails.

`cabal.project` already has:
```
library-vanilla: False
shared: True
executable-dynamic: True
```
These settings only affect which artifacts cabal produces, not how ghc searches
for interfaces in deps. Passing `--ghc-options=-dynamic` globally is what would
force ghc to use `.dyn_hi` interface resolution.

**Cabal-only for deps — pacman-for-Haskell-packages is off the table** (the
Arch `haskell-*` packages are exactly what caused this mess, and mixing cabal
source builds with pacman-provided interface files is how we got here). Every
option below keeps cabal as the single source of truth.

**Options, ranked by effort:**

1. **Add `program-default-options: ghc-options: -dynamic` to cabal.project.**
   Tells cabal to pass `-dynamic` to every invocation. This makes ghc use
   `.dyn_hi` for import resolution, which is what the pacman-installed
   `hashable`/`random` actually ship. **Try this first.** Single 2-line edit.
   Fragile because it depends on the pacman globals continuing to line up
   with whatever ghc version cabal picks — treat as a stopgap.

2. **Force every dep to rebuild from Hackage source.** Add to `cabal.project`:
   ```
   constraints: hashable -installed, random -installed
   ```
   (extend the list as more `-installed` conflicts surface.) This bypasses
   the broken pacman globals entirely for those packages — cabal downloads and
   builds them into `~/.cabal/store`, producing both `.hi` and `.dyn_hi`.
   Slower first build but clean and reproducible. Combines with option 1.

3. **Nix shell / ghcup / stack** instead of system ghc+cabal. `ghcup` gives a
   clean ghc toolchain outside `/usr`, completely sidestepping the pacman
   global DB. `stack` additionally pins an LTS snapshot with binary caches.
   Biggest diff from current setup but the most durable fix. Recommended if
   options 1+2 don't converge in a reasonable time.

4. **Static build** (`executable-static`, drop `executable-dynamic`). Dead end:
   `ghc-static` is installed but doesn't ship a static `hashable` (`pacman -Ql
   ghc-static | grep hashable` is empty), so we'd still need cabal to build
   hashable from source — which is already what option 2 does.

**Recommended next action:** option 1 (60 seconds) → option 2 (few minutes) →
option 3 (ghcup install, ~10 minutes of toolchain setup).

### 3.3 `duckdb-ffi` availability
Per §1.2, the port binds directly to libduckdb via `duckdb-ffi` and DuckDB's
native data-chunk API. Open questions:

1. **Is `duckdb-ffi` on Hackage in a recent-enough version?** Check with
   `cabal info duckdb-ffi` once §3.2 is past. The data-chunk API
   (`duckdb_fetch_chunk`, `duckdb_data_chunk_get_vector`,
   `duckdb_vector_get_data`, `duckdb_vector_get_validity`) was added in
   DuckDB 0.10.0 — the Hackage package must expose at least those symbols.
2. **If the Hackage version is too old or missing**, fall back to `inline-c`
   in `Tv.Data.DuckDB.Raw`. Arch has `duckdb` as a system package providing
   `libduckdb.so` + `duckdb.h`, so `extra-libraries: duckdb` in the cabal file
   plus a small `inline-c` capi shim is enough. We only need ~15 symbols.
3. **What about `duckdb-haskell` (the higher-level package)?** Do not use it —
   it materializes results into Haskell values, same problem as `duckdb-simple`,
   same reason to avoid (§1.2 / §1.8 zero-copy).

**Action when unblocked:** run `cabal info duckdb-ffi` and check the exposed
modules for `duckdb_fetch_chunk`. If absent, scaffold
`Tv.Data.DuckDB.Raw` with `inline-c` and skip `duckdb-ffi` entirely.

---

## 4. Architectural observations (from the Lean side, keep in mind for port)

These are lessons from `~/diary/2026-04-10.md` and `~/diary/2026-04-11.md` that
directly shape port design:

- **There is no `Expr` AST** in Tc. Filters are opaque strings compiled by
  DuckDB. Port accordingly — don't invent a Haskell `Expr` datatype unless we
  want to diverge from Tc's "let the DB do it" architecture.
- **Op is declarative** — lives in `Prql.Query.ops : Array Op`, rendered as one
  PRQL string per query. No imperative Op loop. The Haskell `Tv.Types.Op`
  constructor set already matches this shape.
- **`TblOps.filter/sortBy/hideCols` each do their own DB round trip.** Port
  should keep this shape — resist the urge to batch into one "pipe" until
  profiling says it matters.
- **Error handling: `runEffect` wraps every effect in `try/catch`, logs to
  `~/.cache/tv/tv.log`, shows popup.** Errors NEVER crash the TUI. Port must
  preserve this invariant — every `Handler` path goes through a `try`.
- **Width ownership:** in Tc, C is the single source of truth for column widths.
  In the port, Haskell owns widths (no C side). Simplifies things — just store
  as `Vector Int` on the View.
- **PRQL is central.** All queries go through `Prql.query` (compile + log +
  execute). Port must do the same — `Tv.Data.Prql` module should wrap
  `prqlc compile -t sql.duckdb` + log + hand to DuckDB.

---

## 5. Recommended next session path

Ordered by dependency, not difficulty:

1. **Unblock the build** — §3.2 option 1 → 2 → 3.
2. **Fix the `Types.hs` import bug** — §3.1. Then `cabal build` should actually
   reach our sources.
3. **Verify `duckdb-ffi` resolves with the data-chunk API** (§3.3). If not,
   scaffold `Tv.Data.DuckDB.Raw` with `inline-c` against system `libduckdb`.
4. **Minimal `Tv.Data.DuckDB`**: `connect :: FilePath -> Eff es Conn`,
   `query :: Conn -> Text -> Eff es Result`, where `Result` streams
   `duckdb_data_chunk`s wrapped in `ForeignPtr`s. `Column` is a handle over
   those chunks (§1.2). Enough to load a parquet file and read rows lazily
   without materializing.
5. **Minimal `Tv.Data.Prql`**: shell out to `prqlc`, read stdout. No error
   recovery yet.
6. **Flesh out `Render.drawApp`**: header + cell grid (first window) + status
   bar. Use `Brick.Widgets.Border` + `Brick.renderTable` or hand-roll with
   `hBox`/`vBox` of `txt` widgets, whichever cooperates with the grouping/cursor
   styling we need. **First goal: `tv file.parquet` opens and shows 20 rows.**
7. **Handler map in `App.handleEvent`**: populate `[Entry]`, call `initCmds`,
   wire `keyLookup` → `Handler` dispatch. First three handlers: `RowInc`,
   `RowDec`, `TblQuit` — prove the loop works.
8. Everything else is incremental file-by-file.

Once step 6 works, this port graduates from "paper sketch" to "can load a file".
That's the minimum milestone worth deciding whether to keep going on.

---

## 6. Not a git repo
As of this writing, `/home/dh/repo/tv-hask` is not initialized as a git repo.
Decide whether to `git init` before making more changes. Tc lives in `../Tc`
(separate repo); the port can be its own repo or a subdirectory — no decision
committed yet.

## 7. Files that might be cruft
- `c/adbc_core.c`, `c/nanoarrow.{h,c}` — **delete.** We bind libduckdb directly
  via `duckdb-ffi` (§1.2); no ADBC, no Arrow C Data Interface, no nanoarrow.
- `c/sock_shim.c` — **delete.** Tc uses it for termbox test IPC; we use
  `effectful` interpreter swap for the test/prod split (§1.6, §1.7), so no
  socket shim is needed.
- `dist-newstyle/` — cabal build cache. Add to .gitignore before committing.
- `data/funcs.prql` and `theme.csv` — legitimate, keep.
