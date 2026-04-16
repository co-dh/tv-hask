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
{-# LANGUAGE TemplateHaskell #-}
module Tv.Nav where

import Data.Text (Text)
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.Core (Lens', (%), (&), (.~), (^.), over)
import Optics.TH (makeFieldLabelsNoPrefix)
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
makeFieldLabelsNoPrefix ''NavAxis

-- Default NavAxis (cursor at 0, no selections).
-- Lean requires `h : n > 0`; caller guarantees this in Haskell.
navAxisDefault :: NavAxis elem
navAxisDefault = NavAxis { cur = 0, sels = V.empty }

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
makeFieldLabelsNoPrefix ''NavState

-- | Column names from table
colNames :: TblOps t => NavState t -> Vector Text
colNames nav = TblOps.colNames (nav ^. #tbl)

-- | Current column index in data order
curColIdx :: TblOps t => NavState t -> Int
curColIdx nav = colIdxAt (nav ^. #grp) (colNames nav) (nav ^. #col % #cur)

-- | Current column name
curColName :: TblOps t => NavState t -> Text
curColName nav = maybe "" id (colNames nav V.!? curColIdx nav)

-- | Current column type
curColType :: TblOps t => NavState t -> ColType
curColType nav = TblOps.colType (nav ^. #tbl) (curColIdx nav)

-- | Column names in display order (grouped first, then rest)
dispColNames :: TblOps t => NavState t -> Vector Text
dispColNames nav =
  let grp_ = nav ^. #grp
  in grp_ V.++ V.filter (not . (`V.elem` grp_)) (colNames nav)

-- | Selected column indices
selColIdxs :: TblOps t => NavState t -> Vector Int
selColIdxs nav =
  let names = colNames nav
  in V.mapMaybe (idxOf names) (nav ^. #col % #sels)

-- | Hidden column indices (for C render)
hiddenIdxs :: TblOps t => NavState t -> Vector Int
hiddenIdxs nav =
  let names = colNames nav
  in V.mapMaybe (idxOf names) (nav ^. #hidden)

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

-- | Named composites reused as atoms by `exec` and by Filter (both hot
-- paths); `rowSelsL` is the atom for `CmdRowSel`.
rowCurL :: Lens' (NavState t) Int
rowCurL = #row % #cur

colCurL :: Lens' (NavState t) Int
colCurL = #col % #cur

rowSelsL :: Lens' (NavState t) (Vector Int)
rowSelsL = #row % #sels

-- Execute by command, no (obj,verb) chars
exec :: TblOps t => Cmd -> NavState t -> Int -> Maybe (NavState t)
exec h nav rowPg =
  let tbl_   = nav ^. #tbl
      nRows_ = TblOps.nRows tbl_
      nCols_ = V.length (TblOps.colNames tbl_)
      rCur   = nav ^. #row % #cur
      cCur   = nav ^. #col % #cur
      grp_   = nav ^. #grp
      r d = Just (over rowCurL (\f -> finClamp nRows_ f d) nav)
      c d = Just (over colCurL (\f -> finClamp nCols_ f d) nav)
  in case h of
       CmdRowInc   -> r 1
       CmdRowDec   -> r (-1)
       CmdColInc   -> c 1
       CmdColDec   -> c (-1)
       CmdRowPgdn  -> r rowPg
       CmdRowPgup  -> r (-rowPg)
       CmdRowBot   -> r (nRows_ - 1 - rCur)
       CmdRowTop   -> r (- rCur)
       CmdColFirst -> c (- cCur)
       CmdColLast  -> c (nCols_ - 1 - cCur)
       CmdRowSel   -> Just (over rowSelsL (`toggle` rCur) nav)
       CmdColGrp   ->
         let newGrp = toggle grp_ (curColName nav)
         in Just (nav & #grp .~ newGrp & #dispIdxs .~ dispOrder newGrp (colNames nav))
       CmdColHide  -> Just (over #hidden (`toggle` curColName nav) nav)
       CmdColShiftL -> shiftGrp False grp_ nCols_
       CmdColShiftR -> shiftGrp True  grp_ nCols_
       _ -> Nothing
  where
    shiftGrp fwd grp_ nCols_ =
      let name = curColName nav
      in case idxOf grp_ name of
           Nothing -> Nothing
           Just i
             | fwd && i + 1 >= V.length grp_ -> Nothing
             | not fwd && i == 0 -> Nothing
             | otherwise ->
                 let j  = if fwd then i + 1 else i - 1
                     gi = maybe "" id (grp_ V.!? i)
                     gj = maybe "" id (grp_ V.!? j)
                     newGrp = grp_ V.// [(i, gj), (j, gi)]
                     d = if fwd then 1 else -1
                     nav' = nav & #grp .~ newGrp & #dispIdxs .~ dispOrder newGrp (colNames nav)
                 in Just (over colCurL (\f -> finClamp nCols_ f d) nav')
