{-
  Typeclass-based navigation state for tabular viewer.

  Key abstractions:
  - NavState: generic over table type + navigation state
  - NavAxis: cursor (Int) + selections (Vector)

  Port note: Lean uses `Fin n` for length-indexed cursors; Haskell uses plain
  `Int` with implicit bounds enforced at call sites (see `finClamp`, `exec`).
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Nav where

import Data.Text (Text)
import Data.Vector (Vector)
import qualified Data.Vector as V
import Tv.Lens (Lens'(..), modify, (|.))
import Tv.Types
  ( Cmd(..)
  , ColType
  , TblOps
  , toggle
  )
import qualified Tv.Types as TblOps

-- Clamp value to [lo, hi)
clamp :: Int -> Int -> Int -> Int
clamp val lo hi = if hi <= lo then lo else max lo (min val (hi - 1))

-- Adjust offset to keep cursor visible: off <= cur < off + page
adjOff :: Int -> Int -> Int -> Int
adjOff cur off page = clamp off (cur + 1 - page) (cur + 1)

-- Clamp cursor by delta, staying in [0, n). Equivalent to Lean's `Fin.clamp`.
-- Bound `n` passed explicitly since Haskell `Int` has no length-indexing.
finClamp :: Int -> Int -> Int -> Int
finClamp n f d =
  let v  = max 0 (f + d)
      v' = min v (n - 1)
  in v'

-- ## Structures

-- NavAxis: cursor + selection for one axis (row or col)
data NavAxis elem = NavAxis
  { cur  :: Int          -- cursor position
  , sels :: Vector elem  -- selected elements
  }

-- Default NavAxis (cursor at 0, no selections).
-- Lean requires `h : n > 0`; caller guarantees this in Haskell.
navAxisDefault :: NavAxis elem
navAxisDefault = NavAxis { cur = 0, sels = V.empty }

-- | Field lenses for NavAxis — enable composable nested updates via `|.`.
curL :: Lens' (NavAxis elem) Int
curL = Lens' { get = cur, set = \a s -> s { cur = a } }

selsL :: Lens' (NavAxis elem) (Vector elem)
selsL = Lens' { get = sels, set = \a s -> s { sels = a } }

-- Type aliases: Row uses Int (index), Col uses Text (name, stable across deletion)
type RowNav = NavAxis Int
type ColNav = NavAxis Text

-- Find index of element in vector (O(n) linear scan)
idxOf :: Eq a => Vector a -> a -> Maybe Int
idxOf a x = V.findIndex (== x) a

-- Compute display order: group names first (in grp array order), then rest
-- Group order matters for join key ordering (Shift+Arrow reorders grp)
dispOrder :: Vector Text -> Vector Text -> Vector Int
dispOrder group names =
  let n = V.length names
      getD i = maybe "" id (names V.!? i)
      isGrp i = V.elem (getD i) group
      -- Sort group indices by position in grp array (respects grp add/reorder order)
      grpSorted =
        let filtered = V.filter isGrp (V.enumFromN 0 n)
            key i = maybe 0 id (idxOf group (getD i))
        in V.fromList (sortByKey key (V.toList filtered))
  in grpSorted V.++ V.filter (not . isGrp) (V.enumFromN 0 n)
  where
    sortByKey :: (Int -> Int) -> [Int] -> [Int]
    sortByKey _ [] = []
    sortByKey k (x:xs) =
      let kx = k x
          lt = [y | y <- xs, k y <  kx]
          ge = [y | y <- xs, k y >= kx]
      in sortByKey k lt ++ [x] ++ sortByKey k ge

-- Get column index at display position
colIdxAt :: Vector Text -> Vector Text -> Int -> Int
colIdxAt group names i = maybe 0 id (dispOrder group names V.!? i)

-- NavState: generic over table type + navigation state
-- Lean parameterizes on nRows/nCols type params for `Fin` bounds. Haskell uses
-- plain Int cursors, so those proof fields (`hRows`, `hCols`) are dropped;
-- bounds are fetched at call sites from `TblOps.nRows`/`colNames`.
data NavState t = NavState
  { tbl      :: t                -- underlying table
  , row      :: RowNav           -- row cursor + selections
  , col      :: ColNav           -- col cursor + selections
  , grp      :: Vector Text      -- grouped column names (stable)
  , hidden   :: Vector Text      -- hidden column names (width=1)
  , dispIdxs :: Vector Int       -- cached display order
  }

-- | Column names from table
colNames :: TblOps t => NavState t -> Vector Text
colNames nav = TblOps.colNames (tbl nav)

-- | Current column index in data order
curColIdx :: TblOps t => NavState t -> Int
curColIdx nav = colIdxAt (grp nav) (colNames nav) (cur (col nav))

-- | Current column name
curColName :: TblOps t => NavState t -> Text
curColName nav = maybe "" id (colNames nav V.!? curColIdx nav)

-- | Current column type
curColType :: TblOps t => NavState t -> ColType
curColType nav = TblOps.colType (tbl nav) (curColIdx nav)

-- | Column names in display order (grouped first, then rest)
dispColNames :: TblOps t => NavState t -> Vector Text
dispColNames nav =
  grp nav V.++ V.filter (not . (`V.elem` grp nav)) (colNames nav)

-- | Selected column indices
selColIdxs :: TblOps t => NavState t -> Vector Int
selColIdxs nav =
  let names = colNames nav
  in V.mapMaybe (idxOf names) (sels (col nav))

-- | Hidden column indices (for C render)
hiddenIdxs :: TblOps t => NavState t -> Vector Int
hiddenIdxs nav =
  let names = colNames nav
  in V.mapMaybe (idxOf names) (hidden nav)

-- Constructor for external use. Lean requires `hr : nRows > 0` / `hc : nCols > 0`;
-- Haskell callers must likewise ensure the table is non-empty.
new :: TblOps t => t -> NavState t
new t = NavState
  { tbl      = t
  , row      = navAxisDefault
  , col      = navAxisDefault
  , grp      = V.empty
  , hidden   = V.empty
  , dispIdxs = dispOrder V.empty (TblOps.colNames t)
  }

-- Constructor with initial row/col cursor and group (clamped to valid range)
newAt :: TblOps t => t -> Int -> Vector Text -> Int -> NavState t
newAt t colIx grp_ rowIx =
  let nCols = V.length (TblOps.colNames t)
      nRows = TblOps.nRows t
      c = min colIx (nCols - 1)
      r = min rowIx (nRows - 1)
  in NavState
       { tbl      = t
       , row      = NavAxis { cur = r, sels = V.empty }
       , col      = NavAxis { cur = c, sels = V.empty }
       , grp      = grp_
       , hidden   = V.empty
       , dispIdxs = dispOrder grp_ (TblOps.colNames t)
       }

-- | Field lenses for NavState. Used to express nested `row.cur` / `col.cur`
-- updates as single-line compositions. (`tbl` is skipped — not independently
-- updatable in the lens-driven path.)
rowL :: Lens' (NavState t) RowNav
rowL = Lens' { get = row, set = \a s -> s { row = a } }

colL :: Lens' (NavState t) ColNav
colL = Lens' { get = col, set = \a s -> s { col = a } }

grpL :: Lens' (NavState t) (Vector Text)
grpL = Lens' { get = grp, set = \a s -> s { grp = a } }

hiddenL :: Lens' (NavState t) (Vector Text)
hiddenL = Lens' { get = hidden, set = \a s -> s { hidden = a } }

dispIdxsL :: Lens' (NavState t) (Vector Int)
dispIdxsL = Lens' { get = dispIdxs, set = \a s -> s { dispIdxs = a } }

-- | Composite lenses: cursor through row/col axis
rowCurL :: Lens' (NavState t) Int
rowCurL = rowL |. curL

colCurL :: Lens' (NavState t) Int
colCurL = colL |. curL

rowSelsL :: Lens' (NavState t) (Vector Int)
rowSelsL = rowL |. selsL

colSelsL :: Lens' (NavState t) (Vector Text)
colSelsL = colL |. selsL

-- Execute by command, no (obj,verb) chars
exec :: TblOps t => Cmd -> NavState t -> Int -> Maybe (NavState t)
exec h nav rowPg =
  let nRows_ = TblOps.nRows (tbl nav)
      nCols_ = V.length (TblOps.colNames (tbl nav))
      r d = Just (modify rowCurL (\f -> finClamp nRows_ f d) nav)
      c d = Just (modify colCurL (\f -> finClamp nCols_ f d) nav)
  in case h of
       CmdRowInc   -> r 1
       CmdRowDec   -> r (-1)
       CmdColInc   -> c 1
       CmdColDec   -> c (-1)
       CmdRowPgdn  -> r rowPg
       CmdRowPgup  -> r (-rowPg)
       CmdRowBot   -> r (nRows_ - 1 - cur (row nav))
       CmdRowTop   -> r (- cur (row nav))
       CmdColFirst -> c (- cur (col nav))
       CmdColLast  -> c (nCols_ - 1 - cur (col nav))
       CmdRowSel   -> Just (modify rowSelsL (`toggle` cur (row nav)) nav)
       CmdColGrp   ->
         let newGrp = toggle (grp nav) (curColName nav)
         in Just nav { grp = newGrp
                     , dispIdxs = dispOrder newGrp (colNames nav)
                     }
       CmdColHide  -> Just (modify hiddenL (`toggle` curColName nav) nav)
       CmdColShiftL -> shiftGrp False
       CmdColShiftR -> shiftGrp True
       _ -> Nothing
  where
    shiftGrp fwd =
      let name = curColName nav
      in case idxOf (grp nav) name of
           Nothing -> Nothing
           Just i
             | fwd && i + 1 >= V.length (grp nav) -> Nothing
             | not fwd && i == 0 -> Nothing
             | otherwise ->
                 let j  = if fwd then i + 1 else i - 1
                     gi = maybe "" id (grp nav V.!? i)
                     gj = maybe "" id (grp nav V.!? j)
                     newGrp = (grp nav V.// [(i, gj), (j, gi)])
                     d = if fwd then 1 else -1
                     nav' = nav { grp = newGrp
                                , dispIdxs = dispOrder newGrp (colNames nav)
                                }
                     nCols_ = V.length (TblOps.colNames (tbl nav))
                 in Just (modify colCurL (\f -> finClamp nCols_ f d) nav')
