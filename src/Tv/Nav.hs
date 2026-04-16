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
module Tv.Nav
  ( -- * Utility
    clamp
  , adjOff
  , finClamp
    -- * NavAxis
  , NavAxis(..)
  , defAxis
  , RowNav
  , ColNav
    -- * Display order
  , idxOf
  , dispOrder
  , idxAt
    -- * NavState
  , NavState(..)
  , colNames
  , colIdx
  , colName
  , colType
  , dispNames
  , selIdxs
  , hiddenIdxs
    -- * Constructors
  , new
  , newAt
    -- * Lenses
  , rowCur
  , colCur
  , rowSels
    -- * Exec
  , exec
    -- * Commands
  , commands
  ) where

import Data.Text (Text)
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.Core (Lens', (%), (&), (.~), (^.), over)
import Optics.TH (makeFieldLabelsNoPrefix)
import Data.List (sortBy)
import Data.Maybe (fromMaybe)
import Data.Ord (comparing)
import Tv.CmdConfig (Entry, mkEntry, navE)
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
defAxis :: NavAxis elem
defAxis = NavAxis { cur = 0, sels = V.empty }

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
      getD i = fromMaybe "" (names V.!? i)
      isGrp i = V.elem (getD i) group
      -- Sort group indices by position in grp array (respects grp add/reorder order)
      grpSorted =
        let filtered = V.filter isGrp (V.enumFromN 0 n)
            key i = fromMaybe 0 (idxOf group (getD i))
        in V.fromList (sortBy (comparing key) (V.toList filtered))
  in grpSorted V.++ V.filter (not . isGrp) (V.enumFromN 0 n)

-- Get column index at display position
idxAt :: Vector Text -> Vector Text -> Int -> Int
idxAt group names i = fromMaybe 0 (dispOrder group names V.!? i)

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
colIdx :: TblOps t => NavState t -> Int
colIdx nav = idxAt (nav ^. #grp) (colNames nav) (nav ^. #col % #cur)

-- | Current column name
colName :: TblOps t => NavState t -> Text
colName nav = fromMaybe "" (colNames nav V.!? colIdx nav)

-- | Current column type
colType :: TblOps t => NavState t -> ColType
colType nav = TblOps.colType (nav ^. #tbl) (colIdx nav)

-- | Column names in display order (grouped first, then rest)
dispNames :: TblOps t => NavState t -> Vector Text
dispNames nav =
  let grp_ = nav ^. #grp
  in grp_ V.++ V.filter (not . (`V.elem` grp_)) (colNames nav)

-- | Selected column indices
selIdxs :: TblOps t => NavState t -> Vector Int
selIdxs nav =
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
  , row      = defAxis
  , col      = defAxis
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
-- paths); `rowSels` is the atom for `CmdRowSel`.
rowCur :: Lens' (NavState t) Int
rowCur = #row % #cur

colCur :: Lens' (NavState t) Int
colCur = #col % #cur

rowSels :: Lens' (NavState t) (Vector Int)
rowSels = #row % #sels

-- Execute by command, no (obj,verb) chars
exec :: TblOps t => Cmd -> NavState t -> Int -> Maybe (NavState t)
exec h nav rowPg =
  let tbl_   = nav ^. #tbl
      nRows_ = TblOps.nRows tbl_
      nCols_ = V.length (TblOps.colNames tbl_)
      rCur   = nav ^. #row % #cur
      cCur   = nav ^. #col % #cur
      grp_   = nav ^. #grp
      r d = Just (over rowCur (\f -> finClamp nRows_ f d) nav)
      c d = Just (over colCur (\f -> finClamp nCols_ f d) nav)
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
       CmdRowSel   -> Just (over rowSels (`toggle` rCur) nav)
       CmdColGrp   ->
         let newGrp = toggle grp_ (colName nav)
         in Just (nav & #grp .~ newGrp & #dispIdxs .~ dispOrder newGrp (colNames nav))
       CmdColHide  -> Just (over #hidden (`toggle` colName nav) nav)
       CmdColShiftL -> shiftGrp False grp_ nCols_
       CmdColShiftR -> shiftGrp True  grp_ nCols_
       _ -> Nothing
  where
    shiftGrp fwd grp_ nCols_ =
      let name = colName nav
      in case idxOf grp_ name of
           Nothing -> Nothing
           Just i
             | fwd && i + 1 >= V.length grp_ -> Nothing
             | not fwd && i == 0 -> Nothing
             | otherwise ->
                 let j  = if fwd then i + 1 else i - 1
                     gi = fromMaybe "" (grp_ V.!? i)
                     gj = fromMaybe "" (grp_ V.!? j)
                     newGrp = grp_ V.// [(i, gj), (j, gi)]
                     d = if fwd then 1 else -1
                     nav' = nav & #grp .~ newGrp & #dispIdxs .~ dispOrder newGrp (colNames nav)
                 in Just (over colCur (\f -> finClamp nCols_ f d) nav')

commands :: V.Vector (Entry, Maybe f)
commands = V.fromList
  [ navE (mkEntry CmdRowInc     "r"  "j"         ""                                False "")
  , navE (mkEntry CmdRowDec     "r"  "k"         ""                                False "")
  , navE (mkEntry CmdRowPgdn    "r"  "<pgdn>"    ""                                False "")
  , navE (mkEntry CmdRowPgup    "r"  "<pgup>"    ""                                False "")
  , navE (mkEntry CmdRowPgdn    "r"  "<C-d>"     ""                                False "")
  , navE (mkEntry CmdRowPgup    "r"  "<C-u>"     ""                                False "")
  , navE (mkEntry CmdRowTop     "r"  "<home>"    ""                                False "")
  , navE (mkEntry CmdRowBot     "r"  "<end>"     ""                                False "")
  , navE (mkEntry CmdRowSel     "r"  "T"         "Select/deselect current row"      False "")
  , navE (mkEntry CmdColInc     "c"  "l"         ""                                False "")
  , navE (mkEntry CmdColDec     "c"  "h"         ""                                False "")
  , navE (mkEntry CmdColFirst   "c"  ""          ""                                False "")
  , navE (mkEntry CmdColLast    "c"  ""          ""                                False "")
  , navE (mkEntry CmdColGrp     "c"  "!"         "Toggle group on current column"   False "")
  , navE (mkEntry CmdColHide    "c"  "H"         "Hide/unhide current column"       False "")
  , navE (mkEntry CmdColExclude "c"  "x"         "Delete column(s) from query"      True  "")
  , navE (mkEntry CmdColShiftL  "c"  "<S-left>"  "Shift key column left"            False "")
  , navE (mkEntry CmdColShiftR  "c"  "<S-right>" "Shift key column right"           False "")
  , navE (mkEntry CmdSortAsc    "c"  "["         "Sort ascending"                   True  "")
  , navE (mkEntry CmdSortDesc   "c"  "]"         "Sort descending"                  True  "")
  ]
