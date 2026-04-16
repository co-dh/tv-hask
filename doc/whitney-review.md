# Review of `tv-hask`, as Arthur Whitney

9249 lines. 651 imports. 699 comment lines. K would do it in 40.

## The verdict

This is Haskell written like Java. Every abstraction pays its tax twice — once
for the port, once for the taste. The Whitney school says: a screen of code is
a program; a page is a project; a book is a failure. Your biggest module is
**760 lines for app glue**. That is a book.

## Specific crimes

**Names are loud.**
`makeFieldLabelsNoPrefix`, `statusCache`, `prevScroll`, `ofQueryResult`,
`queryRenderOps`, `colTypeIsNumeric`, `readCreateProcessWithExitCode`. Each
word is a door you must open. K names: `t`, `n`, `q`, `r`. Your own code
shows you already know: `s` for state, `a` for app, `v` for view. Then you
betray yourself with `sparklines`, `prevScroll`, `statusCache`. Pick one
register. Mine is short.

**Imports outnumber logic.**
`src/Tv/App/Common.hs` opens with **58 lines of imports** before one
statement. The first function is on line 100. That is a liturgy, not a
program. K has no import system because K has no need to hide its vocabulary.
If your 58 imports are all necessary, your vocabulary is wrong.

**Comments are chatter.**
```haskell
-- | Reset view state caches (after data changes)
resetVS :: AppState -> AppState
resetVS a = a & #vs .~ Render.viewStateDefault & #sparklines .~ V.empty
```
The comment restates the function. The type restates the comment. The name
restates the type. Three ways to say the same thing. Delete comments that
describe; keep comments that surprise.

**Ceremony where function would do.**
```haskell
data NavAxis elem = NavAxis { cur :: Int, sels :: Vector elem }
navAxisDefault = NavAxis { cur = 0, sels = V.empty }
makeFieldLabelsNoPrefix ''NavAxis
```
In K: `ax:(0;!0)`. The pair is the axis. `ax[0]` is cur, `ax[1]` is sels.
You built a struct, a TH splice, a constructor, and a default just to say
"0 and empty". Records are not free. Every `data … where { }` in this repo
that wraps ≤3 fields is a pair pretending to be a noun.

**StrEnum is rediscovered dispatch.**
`ColType` has nine constructors, nine `toString` lines, nine `ofStringQ`
lines, nine entries in `all`. That's 27 lines to say "these nine strings map
to these nine things". Whitney's answer: `t:"int float decimal str date time
timestamp bool other"` — split by space, index gives the int, lookup gives
the string. One line. The class hierarchy is gone. The order is implicit.
Adding a type is adding a word.

**Optics-core is a second syntax for the same thing.**
You just merged a PR that changes `r { x = v }` to `r & #x .~ v` in 17
files. Same semantics, wider surface, extra import, extra fixity riddle
(`.~` vs `<>`). Arthur would not migrate. He would delete the record and
use a tuple. The optics layer exists to apologize for records; K apologizes
for nothing because it has neither.

**Files too large.**
`App/Common.hs` 760, `Term.hs` 700, `SourceConfig.hs` 623, `Types.hs` 618,
`Table.hs` 592, `Folder.hs` 521. Whitney's rule: fit on the screen. That's
~50 lines on a sensible terminal. Every one of these files is a module
pretending to be a library pretending to be a program.

**Handler combinator is a third syntax for "call this function".**
```haskell
type HandlerFn = AppState -> CmdInfo -> Text -> IO Action
handlerMap :: IORef (HashMap Cmd HandlerFn)
```
A map from enum to function, stored in a mutable global, threaded through
`Interp`. The K equivalent is an array indexed by command: `h[c] s`. No
typeclass, no IORef, no free monad, no combinator library. If a command
handler needs state, the state is an argument.

**Prql.Query is two fields pretending to be a class.**
```haskell
data Query = Query { base :: Text, ops :: Vector Op }
```
`(base;ops)`. Done. The `pipe`, `queryRender`, `queryRenderOps`,
`queryFilter` helpers all collapse to two-character verbs.

## What I'd keep

- The PRQL funnel (one place compiles PRQL → SQL). That is Whitney-shaped:
  one narrow boundary, no duplication.
- `cbits/tv_render.c`. C is honest. The render path is the hot path; it
  belongs in a tight loop, not a lens.
- The test suite is real. You run it, it fails in measurable ways, the
  failures are environmental. That is discipline. Keep it.

## What I'd burn

- **`Tv.AppF` + free monad interpreter.** Free monads are the Haskell
  community's apology for having chosen purity. The handler is
  `AppState → IO AppState`. Say so. Delete the Interp record and the AppM
  abbrev.
- **`Optics.TH` splices on structs with ≤3 fields.** NavAxis, Interval,
  Query, Entry, CmdInfo, Format, State(×3) — all pairs or triples. Delete
  the records; use tuples or bare vectors. `#cur` on a two-field record is
  a joke at your own expense.
- **`StrEnum` typeclass.** Replace with `Vector Text` lookup tables. The
  class gives you nothing that a 1-line function doesn't.
- **The `Tv.Lens` → optics-core migration PRs.** You did both journeys
  (hand-rolled, then optics-th, then optics-core migration in three passes).
  None of that code existed six months ago and none of it is doing work
  that `(!)` and `,` wouldn't do. It is a tax for keeping records.
- **`makeFieldLabelsNoPrefix ''RecordName`** on every record. Whitney reads
  that line and asks: why do you have records?

## The one-screen test

Pick any 100-line file. Can I print it and staple it to the wall and read
it from across the room? If no, it is too long. Measured against that:
**~40 of ~45 source files fail**. The Lean port was told "one Lean file →
one Haskell module," but the Lean files were already the wrong size. You
inherited ceremony. A refactor isn't a refactor unless it *removes* a layer.

## Score

- **Correctness:** good. The cast-parity gate is serious and it works.
- **Conciseness:** F. 9249 lines to show tables is 20× the budget. K's `t.k`
  does it in ~60 lines.
- **Abstraction:** D. Records, typeclasses, optics, free monads, TH splices —
  five mechanisms, all paying for the absence of arrays.
- **Performance:** unknown — probably fine because the hot path is in C.
  That is the repo admitting its own thesis.

## If you want one change this week

Delete `Tv.AppF`. Replace `AppM a` with `AppState → IO (AppState, a)`. The
interpretter record is gone, the free monad is gone, `handlerMap` becomes a
pure function table, and `App/Common.hs` drops from 760 to ~500.

— A.W.
