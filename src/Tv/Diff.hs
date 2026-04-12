{-# LANGUAGE ScopedTypeVariables #-}
-- | Diff: FULL OUTER JOIN of two tables on auto-detected categorical keys.
-- Produces a TblOps with key columns, Δ-prefixed differing value columns,
-- and side-suffixed exclusive columns. Columns whose left/right values
-- agree on every row are returned in a separate 'sameHide' vector so
-- callers can keep the view compact.
--
-- Ported from Tc/Diff.lean. The Lean version pushes work down to DuckDB
-- via a generated SQL query; here we operate directly on the materialized
-- row grid exposed by TblOps, since the Haskell TblOps is already a
-- read-only in-memory view without a live query context.
module Tv.Diff (diffTables, diffTablesSameHide) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Map.Strict as Map
import Data.Set (Set)
import qualified Data.Set as Set

import Tv.Types

-- | Column shared by both tables with identical name and type.
data Common = Common { cmName :: !Text, cmType :: !ColType }

-- | Intersect columns of left and right by name; keep only pairs whose
-- types match. Order follows left (same as Lean filterMap over colNames).
commonCols :: TblOps -> TblOps -> Vector Common
commonCols l r = V.mapMaybe mk (_tblColNames l)
  where
    rIdx = Map.fromList (zip (V.toList (_tblColNames r)) [0 :: Int ..])
    mk n = do
      ri <- Map.lookup n rIdx
      let li = maybe (-1) id (V.elemIndex n (_tblColNames l))
          lt = _tblColType l li
          rt = _tblColType r ri
      if lt == rt then Just (Common n lt) else Nothing

-- | Columns of tbl whose name is neither in common nor in keys. Returns
-- (originalIndex, name).
onlyCols :: TblOps -> Vector Common -> Vector Text -> Vector (Int, Text)
onlyCols tbl common keys = V.ifilter (\_ (_, n) -> keep n)
  (V.indexed (_tblColNames tbl))
  where
    cset = Set.fromList (V.toList (V.map cmName common))
    kset = Set.fromList (V.toList keys)
    keep n = not (Set.member n cset) && not (Set.member n kset)

-- | Choose join keys: existing group cols plus any non-numeric common
-- column that isn't already grouped. Nothing if the resulting key set
-- is empty (diff is then ill-defined).
resolveKeys
  :: Vector Text -> Vector Text -> Vector Common
  -> Maybe (Vector Text, Vector Text)  -- ^ (allKeys, valCols)
resolveKeys parentGrp curGrp common
  | V.null allKeys = Nothing
  | otherwise      = Just (allKeys, valCols)
  where
    existing = parentGrp V.++ curGrp
    existSet = Set.fromList (V.toList existing)
    auto = V.map cmName
         $ V.filter (\c -> not (isNumeric (cmType c)) && not (Set.member (cmName c) existSet)) common
    allKeys = dedup (existing V.++ auto)
    keySet  = Set.fromList (V.toList allKeys)
    valCols = V.filter (\n -> not (Set.member n keySet)) (V.map cmName common)
    dedup v = V.fromList (go Set.empty (V.toList v))
      where
        go _ []     = []
        go s (x:xs) = if Set.member x s then go s xs else x : go (Set.insert x s) xs

-- | Materialize one row of a table as a Vector Text.
readRow :: TblOps -> Int -> IO (Vector Text)
readRow tbl r = V.generateM (V.length (_tblColNames tbl)) (_tblCellStr tbl r)

-- | Materialize every row of a table.
readAll :: TblOps -> IO (Vector (Vector Text))
readAll tbl = V.generateM (_tblNRows tbl) (readRow tbl)

-- | Project named columns out of a materialized row.
project :: Map.Map Text Int -> Vector Text -> Vector Text -> Vector Text
project idx row = V.map (\n -> case Map.lookup n idx of
                                 Just i | i < V.length row -> row V.! i
                                 _                         -> T.empty)

-- | Compare two TblOps, returning a materialized diff table and the set of
-- same-value column names that callers may hide by default.
diffTablesSameHide :: TblOps -> TblOps -> IO (TblOps, Vector Text)
diffTablesSameHide left right = do
  let common = commonCols left right
  case resolveKeys V.empty V.empty common of
    Nothing -> pure (mkRowOps (V.singleton "diff") V.empty, V.empty)
    Just (allKeys, valCols) -> do
      lRows <- readAll left
      rRows <- readAll right
      let lIdx = Map.fromList (zip (V.toList (_tblColNames left))  [0 :: Int ..])
          rIdx = Map.fromList (zip (V.toList (_tblColNames right)) [0 :: Int ..])
          keyL = V.map (\row -> project lIdx row allKeys) lRows
          keyR = V.map (\row -> project rIdx row allKeys) rRows
          -- Right-side index: key → first right row with that key. The Lean
          -- version relies on SQL FULL OUTER JOIN which deduplicates keys;
          -- we replicate that by keeping the first occurrence.
          rMap :: Map.Map (Vector Text) Int
          rMap = Map.fromList (reverse [ (keyR V.! i, i) | i <- [0 .. V.length keyR - 1] ])
          leftOnly  = onlyCols left  common allKeys
          rightOnly = onlyCols right common allKeys
          -- Pick one named value out of a materialized row (empty if absent).
          cell idx mRow n = case mRow of
            Nothing  -> T.empty
            Just row -> case Map.lookup n idx of
              Just i | i < V.length row -> row V.! i
              _                         -> T.empty
          -- Build one output row from (maybe left row, maybe right row).
          -- Key cells COALESCE left → right (Lean semantics).
          buildRow mL mR =
            let keyCells = V.map
                  (\k -> case mL of
                           Just _  -> cell lIdx mL k
                           Nothing -> cell rIdx mR k) allKeys
                valCells = V.concatMap
                  (\v -> V.fromList [cell lIdx mL v, cell rIdx mR v]) valCols
                lOnly = V.map (\(i, _) -> maybe T.empty (V.! i) mL) leftOnly
                rOnly = V.map (\(i, _) -> maybe T.empty (V.! i) mR) rightOnly
            in keyCells V.++ valCells V.++ lOnly V.++ rOnly
          -- Matched right indices: any right row that joined to at least one left.
          matched :: Set Int
          matched = V.foldl' ins Set.empty keyL
            where ins s k = case Map.lookup k rMap of
                              Just i  -> Set.insert i s
                              Nothing -> s
          joinedLeft = V.imap
            (\li lrow -> let k = keyL V.! li in
              case Map.lookup k rMap of
                Just ri -> buildRow (Just lrow) (Just (rRows V.! ri))
                Nothing -> buildRow (Just lrow) Nothing) lRows
          unmatchedRight = V.ifilter (\ri _ -> not (Set.member ri matched)) rRows
          joinedRight = V.map (\rrow -> buildRow Nothing (Just rrow)) unmatchedRight
          allRows = joinedLeft V.++ joinedRight
          -- Column layout, before Δ-renaming.
          colNames0 = allKeys
                  V.++ V.concatMap (\v -> V.fromList [v <> "_left", v <> "_right"]) valCols
                  V.++ V.map (\(_, n) -> n <> "_L") leftOnly
                  V.++ V.map (\(_, n) -> n <> "_R") rightOnly
          -- Detect same-valued value columns → sameHide.
          colIndex n = V.elemIndex n colNames0
          isSame v = case (colIndex (v <> "_left"), colIndex (v <> "_right")) of
            (Just i, Just j) -> V.all (\row -> (row V.! i) == (row V.! j)) allRows
            _                -> True
          sameHide = V.concatMap
            (\v -> if isSame v
                     then V.fromList [v <> "_left", v <> "_right"]
                     else V.empty) valCols
          sameSet = Set.fromList (V.toList sameHide)
          -- Rename differing value columns with Δ prefix.
          renameMap = Map.fromList $ concat
            [ [ (v <> "_left",  "Δ" <> v <> "_L")
              , (v <> "_right", "Δ" <> v <> "_R") ]
            | v <- V.toList valCols
            , not (Set.member (v <> "_left") sameSet) ]
          renamedCols = V.map (\n -> Map.findWithDefault n n renameMap) colNames0
      pure (mkRowOps renamedCols allRows, sameHide)

-- | Simple wrapper matching the sliver spec: just the diff table.
diffTables :: TblOps -> TblOps -> IO TblOps
diffTables l r = fst <$> diffTablesSameHide l r

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
