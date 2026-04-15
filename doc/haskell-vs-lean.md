# Haskell vs Lean for Application Code

Evaluation based on porting tv (tabular data viewer) from Lean to Haskell — same app, same features, compared side-by-side.

## Where Haskell wins

### Compositionality via CT abstractions

Lean application code reads imperative — `do` blocks manually threading state, pattern-matching on actions, explicit loops.
Haskell's ecosystem is built around composable abstractions: `Functor`, `Foldable`, `Traversable`, `Bifunctor`, `Profunctor`. These
aren't academic — they're the standard vocabulary for combining operations. `traverse` over columns, `bimap` over error paths,
`contramap` for sorting are idiomatic one-liners.

### Optics

Lean's codebase includes a hand-rolled `Lens.lean` library for nested state updates. In Haskell, `lens`/`optics`/`generic-lens` are
mature, compose with `.`, and handle the `NavState` inside `View` inside `AppState` nesting that dominates this app. What takes manual
getter/setter pairs in Lean is a composed optic in Haskell.

### Effect systems

Lean's `AppF.lean` hand-rolls a free monad (`AppM`) with `render`/`nextKey`/`readArg`/`halt` constructors, plus an `Interp` record to
swap prod vs test interpreters. This is reinventing what Haskell provides off the shelf — `effectful`, `polysemy`, `cleff`, or even
`ReaderT env IO` with `Has` constraints. The Lean ecosystem doesn't provide effect libraries at this level of maturity.

### Streaming / constant-memory traversal

DuckDB returns data in chunks. Lean materializes or manually walks chunks. Haskell's `streaming`, `conduit`, or `pipes` give composable,
constant-memory traversal with fusion. The zero-copy chunk iteration in DuckDB.hs could be a `Stream (Of Row) IO ()` that composes with
filtering, mapping, and folding — all without materializing.

### Property testing

QuickCheck and Hedgehog provide shrinking, composable generators, and coverage checking. Lean has basic test infrastructure but nothing
comparable. For an app with complex navigation state (`dispOrder` permutations, viewport clamping, filter composition), property tests
catch edge cases that example-based tests miss.

### Package ecosystem breadth

Hackage has mature packages for: TUI (brick/vty), databases (HDBC, hasql, postgresql-simple), serialization (aeson, cassava),
concurrent IO (async, stm), CLI parsing (optparse-applicative), and hundreds more. Lean's package ecosystem is growing but young.

## Where Lean wins

### Dependent types for correctness

`NavState nRows nCols` with `Fin n` cursors that *cannot* go out of bounds at the type level. `dispOrder_size` is a theorem proving the
display permutation preserves length. In Haskell this requires singletons/GADTs and becomes painful — the type-level programming
experience is bolted on, not native.

### Totality checking

Lean verifies every pattern match is exhaustive and every recursion terminates. Haskell's `-Wincomplete-patterns` is a warning, not a
proof. For algorithm-heavy code (navigation permutations, filter composition), Lean's totality checker catches bugs at compile time that
Haskell can't.

### Compilation model

Lean compiles to C — single binary, no GC pauses, predictable memory. Haskell's GHC runtime has GC pauses (usually <1ms for TUI apps,
but real for hot paths) and larger binaries. For a TUI app this difference is minor; for a high-throughput data pipeline it matters more.

### Proof-carrying code

When Lean's `Array.toggle_toggle_not_mem` proves that toggling twice returns to the original state, that's a machine-checked invariant.
Haskell relies on QuickCheck to probabilistically test such properties. For critical invariants (financial, safety), proofs beat tests.

## Verdict for application code

**Haskell**, if you value compositional style and CT abstractions as organizing principles.

The complexity in tv/tc is state management, IO plumbing, and rendering — not algorithmic correctness. Haskell's abstraction vocabulary
(optics, effects, streaming, typeclasses) pays off more here than Lean's proof capabilities. The Lean codebase hand-rolls infrastructure
that Haskell provides as libraries.

Lean's strengths (proofs, totality, dependent types) shine in library/algorithm code where you want machine-checked correctness
guarantees. For applications, Lean currently reads like imperative code in a functional language — the community hasn't built the
abstraction layer that makes Haskell application code compositional.

**The practical test:** can the Haskell port be shorter and clearer? Yes — specifically via optics for nested state, effect management
instead of hand-rolled `AppM`, and composable streaming for the data backend.
