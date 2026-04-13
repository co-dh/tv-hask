{-# LANGUAGE ScopedTypeVariables #-}
-- | Join: combine two tables via inner/left/right join, union, or set-diff.
-- Ported from Tc/Join.lean. The Lean version drives the operation through a
-- PRQL-compiled-to-SQL pipeline against DuckDB; here we work directly on the
-- materialized row grid of TblOps since that's what the Haskell port exposes.
--
-- Key columns are supplied by the caller (matching Lean's nav.grp) and
-- used for equality-based joining. Union concatenates rows with the
-- superset of columns; set-diff keeps left rows whose key-tuple doesn't
-- appear in right.
module Tv.Join
  ( JoinOp (..)
  , joinInner
  , joinLeft
  , joinRight
  , joinUnion
  , joinDiff
  , joinWith
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set

import Tv.Types
import Tv.Eff (Eff, IOE, (:>), liftIO)
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- | The five join operations exposed by the fzf menu in Tc/Join.lean.
data JoinOp = JInner | JLeft | JRight | JUnion | JDiff
  deriving (Eq, Show)

-- | Materialize every row of a table.
readAll :: TblOps -> IO (Vector (Vector Text))
readAll tbl = V.generateM ((tbl ^. tblNRows)) readRow
  where readRow r = V.generateM (V.length ((tbl ^. tblColNames))) ((tbl ^. tblCellStr) r)

-- | Project the values of the named key columns out of a row.
keyTuple :: Map.Map Text Int -> Vector Text -> Vector Text -> Vector Text
keyTuple idx row = V.map lookup1
  where
    lookup1 n = case Map.lookup n idx of
      Just i | i < V.length row -> row V.! i
      _                         -> T.empty

-- | Index names → column index.
nameIdx :: Vector Text -> Map.Map Text Int
nameIdx ns = Map.fromList (zip (V.toList ns) [0 ..])

-- | Build a read-only TblOps from a rectangular Text grid.
mkRowOps :: Vector Text -> Vector (Vector Text) -> TblOps
mkRowOps cols rows =
  let nr = V.length rows
      nc = V.length cols
      ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = cols
        , _tblTotalRows   = nr
        , _tblQueryOps    = V.empty
        , _tblFilter      = \_ -> pure Nothing
        , _tblDistinct    = \_ -> pure V.empty
        , _tblFindRow     = \_ _ _ _ -> pure Nothing
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = \_ -> CTStr
        , _tblBuildFilter = \_ _ _ _ -> T.empty
        , _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr     = \r c ->
            if r < 0 || r >= nr || c < 0 || c >= nc
              then pure T.empty
              else pure ((rows V.! r) V.! c)
        , _tblFetchMore   = pure Nothing
        , _tblHideCols    = \_ -> pure ops
        , _tblSortBy      = \_ _ -> pure ops
        }
  in ops

-- ============================================================================
-- Inner / left / right: column layout is left cols ++ right cols (right-side
-- key columns are suffixed with "_r" to avoid collisions, mirroring what a
-- PRQL "join {side:X} right (==k)" produces in Tc).
-- ============================================================================

-- | Run an equi-join with the given policy on key columns @keys@.
runJoin :: (Bool -> Bool -> Bool) -> TblOps -> TblOps -> Vector Text -> IO TblOps
runJoin keep left right keys = do
  lRows <- readAll left
  rRows <- readAll right
  let lIdx = nameIdx ((left ^. tblColNames))
      rIdx = nameIdx ((right ^. tblColNames))
      keyL = V.map (\row -> keyTuple lIdx row keys) lRows
      keyR = V.map (\row -> keyTuple rIdx row keys) rRows
      -- Right groups: key → list of row indices (preserves order).
      rMap :: Map.Map (Vector Text) [Int]
      rMap = Map.fromListWith (++)
               [ (keyR V.! i, [i]) | i <- [V.length keyR - 1, V.length keyR - 2 .. 0] ]
      -- Right rows that matched at least one left row (for right-outer).
      matched = V.foldl' ins Set.empty keyL
        where ins s k = maybe s (foldr Set.insert s) (Map.lookup k rMap)
      emptyL = V.replicate (V.length ((left ^. tblColNames)))  T.empty
      emptyR = V.replicate (V.length ((right ^. tblColNames))) T.empty
      mk l r = l V.++ r
      innerRows = V.concatMap
        (\li -> let k = keyL V.! li
                    matches = Map.findWithDefault [] k rMap
                in V.fromList [ mk (lRows V.! li) (rRows V.! ri) | ri <- matches ])
        (V.enumFromN 0 (V.length lRows))
      leftOnlyRows = V.mapMaybe
        (\li -> let k = keyL V.! li
                in case Map.lookup k rMap of
                     Just (_ : _) -> Nothing
                     _            -> Just (mk (lRows V.! li) emptyR))
        (V.enumFromN 0 (V.length lRows))
      rightOnlyRows = V.mapMaybe
        (\ri -> if Set.member ri matched
                  then Nothing
                  else Just (mk emptyL (rRows V.! ri)))
        (V.enumFromN 0 (V.length rRows))
      -- 'keep l r' picks which outer sides to include:
      --   inner  = keep False False
      --   left   = keep True  False
      --   right  = keep False True
      rows = innerRows
           V.++ (if keep True  False then leftOnlyRows  else V.empty)
           V.++ (if keep False True  then rightOnlyRows else V.empty)
      -- Right column names get "_r" suffix when they would collide with
      -- a left column name. Prevents duplicate header rendering.
      lNames = (left ^. tblColNames)
      lSet   = Set.fromList (V.toList lNames)
      rNames = V.map (\n -> if Set.member n lSet then n <> "_r" else n)
                     ((right ^. tblColNames))
      cols   = lNames V.++ rNames
  pure (mkRowOps cols rows)

-- | Inner join on @keys@: keep only rows whose key tuple appears in both.
joinInner :: IOE :> es => TblOps -> TblOps -> Vector Text -> Eff es TblOps
joinInner l r k = liftIO (runJoin (\_ _ -> False) l r k)

-- | Left outer join: every left row, plus right columns (blank if no match).
joinLeft :: IOE :> es => TblOps -> TblOps -> Vector Text -> Eff es TblOps
joinLeft l r k = liftIO (runJoin (\l' _ -> l') l r k)

-- | Right outer join: every right row, plus left columns (blank if no match).
joinRight :: IOE :> es => TblOps -> TblOps -> Vector Text -> Eff es TblOps
joinRight l r k = liftIO (runJoin (\_ r' -> r') l r k)

-- ============================================================================
-- Union / diff: operate on rows as a whole.
-- ============================================================================

-- | Union: concat rows. Column layout follows left; right rows are
-- projected onto left's column order (missing cols become empty). Matches
-- PRQL @append@ semantics where both inputs are assumed to share a schema.
joinUnion :: IOE :> es => TblOps -> TblOps -> Eff es TblOps
joinUnion left right = liftIO $ do
  lRows <- readAll left
  rRows <- readAll right
  let lNames = (left ^. tblColNames)
      rIdx = nameIdx ((right ^. tblColNames))
      -- Project each right row onto left's column order.
      rProjected = V.map
        (\row -> V.map (\n -> case Map.lookup n rIdx of
                                Just i | i < V.length row -> row V.! i
                                _                         -> T.empty) lNames)
        rRows
  pure (mkRowOps lNames (lRows V.++ rProjected))

-- | Set diff: rows in left whose key tuple does not appear in right.
-- With no keys, compares entire rows (matching PRQL @remove@).
joinDiff :: IOE :> es => TblOps -> TblOps -> Vector Text -> Eff es TblOps
joinDiff left right keys = liftIO $ do
  lRows <- readAll left
  rRows <- readAll right
  let lIdx = nameIdx ((left ^. tblColNames))
      rIdx = nameIdx ((right ^. tblColNames))
      effKeys = if V.null keys then (left ^. tblColNames) else keys
      keyR = V.map (\row -> keyTuple rIdx row effKeys) rRows
      rSet = Set.fromList (V.toList keyR)
      kept = V.filter (\row -> not (Set.member (keyTuple lIdx row effKeys) rSet)) lRows
  pure (mkRowOps ((left ^. tblColNames)) kept)

-- | Dispatch by 'JoinOp'. Keys are required for inner/left/right; for
-- union and diff they're optional (diff falls back to full-row equality).
joinWith :: IOE :> es => JoinOp -> TblOps -> TblOps -> Vector Text -> Eff es TblOps
joinWith JInner l r k = joinInner l r k
joinWith JLeft  l r k = joinLeft  l r k
joinWith JRight l r k = joinRight l r k
joinWith JUnion l r _ = joinUnion l r
joinWith JDiff  l r k = joinDiff  l r k
