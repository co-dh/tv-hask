# Haskell Expert Review: tv-hask

## Architecture: A

Clean 7-layer DAG, zero circular dependencies, all 41 modules have explicit export lists. The "one module per Lean
file" rule is honored with documented exceptions. App/Common (805 LOC) is a hub module importing 30+ others —
intentional and acceptable for a TUI command dispatcher.

## Type Safety & Idioms: B+

**Good:**
- `V.!?` used consistently for safe vector access
- Proper `try`/`catch` error handling throughout
- No `unsafeCoerce`, no sketchy FFI
- `unsafePerformIO` for global IORefs is justified — single-threaded TUI, all `{-# NOINLINE #-}`

**Fixed:**
- `!!` without bounds check in Key.hs, Ftp.hs, Folder.hs, Ops.hs, Theme.hs, Plot.hs — replaced with safe indexing
- `headMay`/`headD`/`getD`/`eraseDups` defined independently in 6+ modules — consolidated to `Tv.Util`
- Hand-rolled quicksort in Nav.hs — replaced with `Data.List.sortBy`

## Performance: B+

**Good:**
- Zero-copy DuckDB cell readers via direct pointer arithmetic
- Lazy chunk streaming with proper ForeignPtr finalizer chains
- Render loop is O(rows x cols) — correct complexity

**Fixed:**
- `V.toList` to build IntSets every frame in Term.hs — replaced with `V.foldl'` direct insertion

**Acceptable:**
- Lazy chunk materialization capped by PRQL 1000-row limit
- ANSI `T.pack (show f)` in `sgrFor` only runs on style changes, not per-cell
- No space leaks detected — frame-driven architecture prevents accumulation

## Resource Management: A

DuckDB FFI is solid:
- `mask_` for atomic connection init
- Finalizer chain: DataChunk retains Result retains Conn
- `zeroBytes` before foreign alloc, 2GB memory cap
- `atomicModifyIORef'` on shared counter
- No resource leaks, no race conditions

## Tests: B-

**Strong:** 75+ integration tests via CLI, 99 pure assertions, graceful fixture skipping

**Weak:**
- Integration tests rely on fragile `contains` substring checks (190+ uses)
- Hardcoded binary path in TestUtil.hs (breaks on GHC upgrade)
- 3 disabled tests (heat mode, 2x socket)
- 30 of 39 modules have no direct unit tests (exercised only via CLI integration)

## Priority Fixes (remaining)

| Priority | Issue                          | Location           |
|----------|--------------------------------|--------------------|
| Med      | Hardcoded GHC path in tests    | TestUtil.hs:39     |
| Low      | Column lookup per-row in render | Term.hs:890-950   |
