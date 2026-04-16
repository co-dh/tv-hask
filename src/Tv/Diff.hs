{-
  Diff: compare top 2 stack views via FULL OUTER JOIN.
  Auto-keys categorical columns, hides same-value columns, Δ-prefixes diffs.

  Literal port of Tc/Tc/Diff.lean — same private helpers in the same order,
  same SQL shape, same status messages.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Diff
  ( run
  , showSame
  , commands
  ) where

import Control.Monad (forM_)
import Data.Either (partitionEithers)
import Data.List (nub)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word64)

import Optics.Core ((&), (.~))

import qualified Tv.Util as Log
import Tv.App.Types (AppState(..), Action(..), HandlerFn, tryStk, resetVS)
import Tv.CmdConfig (Entry, CmdInfo(..), mkEntry, hdl)
import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Data.ADBC.Prql as Prql
import Tv.Data.ADBC.Prql (Query (..))
import Tv.Data.ADBC.Table (AdbcTable)
import qualified Tv.Data.ADBC.Table as Table
import qualified Tv.Nav as Nav
import qualified Tv.Render as Render
import qualified Tv.Util as Log
import Tv.Types
  ( Cmd(..)
  , ColType (..)
  , isNumeric
  )
import Tv.View (View (..), ViewStack (..))
import qualified Tv.View as View

-- | DuckDB double-quoted identifier (escapes " → "")
quoted :: Text -> Text
quoted s = "\"" <> T.replace "\"" "\"\"" s <> "\""

-- | Find columns present in both tables with same name and type
commonCols :: AdbcTable -> AdbcTable -> Vector (Text, ColType)
commonCols left right =
  V.mapMaybe
    (\name -> do
       li <- Nav.idxOf (Table.colNames left) name
       ri <- Nav.idxOf (Table.colNames right) name
       let lt = fromMaybe ColTypeOther (Table.colTypes left V.!? li)
           rt = fromMaybe ColTypeOther (Table.colTypes right V.!? ri)
       if lt == rt then Just (name, lt) else Nothing)
    (Table.colNames left)

-- | Columns in `tbl` that don't appear in `common` or `keys`
onlyCols :: AdbcTable -> Vector (Text, ColType) -> Vector Text -> Vector Text
onlyCols tbl common keys =
  V.filter
    (\n -> not (V.any (\(c, _) -> c == n) common) && not (V.elem n keys))
    (Table.colNames tbl)

-- | Compile PRQL query to SQL for creating temp views
prepareView :: AdbcTable -> Text -> IO (Text, Text)
prepareView tbl sfx = do
  name <- Table.tmpName sfx
  mSql <- Prql.compile (Prql.queryRender (Table.query tbl))
  case mSql of
    Nothing  -> ioError (userError "PRQL compile failed")
    Just sql -> pure (name, Table.stripSemi sql)

-- | Compute join keys: existing group cols + auto-detected categorical common cols.
-- Returns (allKeys, valCols) or none if no keys found.
resolveKeys
  :: Vector Text -> Vector Text -> Vector (Text, ColType)
  -> Maybe (Vector Text, Vector Text)
resolveKeys parentGrp curGrp common =
  let existingGrp = parentGrp V.++ curGrp
      autoKeys    = V.filter (\n -> not (V.elem n existingGrp))
                  . V.map fst
                  . V.filter (\(_, typ) -> not (isNumeric typ))
                  $ common
      allKeys     = V.fromList (nub (V.toList (existingGrp V.++ autoKeys)))
  in if V.null allKeys
       then Nothing
       else Just
              ( allKeys
              , V.filter (\n -> not (V.elem n allKeys)) (V.map fst common)
              )

-- | Build FULL OUTER JOIN SQL, execute it, return temp table name.
buildJoinTbl
  :: AdbcTable -> AdbcTable
  -> Vector Text -> Vector Text
  -> Vector (Text, ColType)
  -> IO Text
buildJoinTbl left right allKeys valCols common = do
  (lName, lSql) <- prepareView left "dl"
  (rName, rSql) <- prepareView right "dr"
  _ <- Adbc.query ("CREATE OR REPLACE TEMP VIEW " <> lName <> " AS " <> lSql)
  _ <- Adbc.query ("CREATE OR REPLACE TEMP VIEW " <> rName <> " AS " <> rSql)
  let q = quoted
      keySel = V.map (\k -> "COALESCE(L." <> q k <> ", R." <> q k <> ") AS " <> q k) allKeys
      valSel = V.map
        (\v -> "L." <> q v <> " AS " <> q (v <> "_left")
            <> ", R." <> q v <> " AS " <> q (v <> "_right"))
        valCols
      sideSel side cols = V.map
        (\n -> side <> "." <> q n <> " AS " <> q (n <> "_" <> side))
        cols
      leftOnlySel  = sideSel "L" (onlyCols left  common allKeys)
      rightOnlySel = sideSel "R" (onlyCols right common allKeys)
      joinCond = T.intercalate " AND "
                   (V.toList (V.map (\k -> "L." <> q k <> " IS NOT DISTINCT FROM R." <> q k) allKeys))
      selCols = T.intercalate ", "
                  (V.toList (keySel V.++ valSel V.++ leftOnlySel V.++ rightOnlySel))
  tblName <- Table.tmpName "diff"
  let sql = "CREATE OR REPLACE TEMP TABLE " <> tblName <> " AS SELECT " <> selCols
         <> " FROM " <> lName <> " L FULL OUTER JOIN " <> rName
         <> " R ON " <> joinCond
  Log.write "diff-sql" sql
  _ <- Adbc.query sql
  pure tblName

-- | Detect same-value column pairs → sameHide; rename differing pairs with Δ prefix.
renameCols :: Text -> Vector Text -> IO (Vector Text)
renameCols tblName valCols = do
  let q = quoted
  classified <- V.forM valCols $ \v -> do
    let leftCol  = v <> "_left"
        rightCol = v <> "_right"
    qr_ <- Adbc.query
             ("SELECT COUNT(*) FROM " <> tblName
              <> " WHERE NOT (" <> q leftCol
              <> " IS NOT DISTINCT FROM " <> q rightCol <> ")")
    cnt <- Adbc.cellInt qr_ 0 0
    pure $ if cnt == 0
      then Left [leftCol, rightCol]
      else Right [(leftCol, "Δ" <> v <> "_L"), (rightCol, "Δ" <> v <> "_R")]
  let (hides, rens) = partitionEithers (V.toList classified)
      sameHide = V.fromList (concat hides)
      renames  = concat rens
  forM_ renames $ \(oldN, renamed) ->
    () <$ Adbc.query
            ("ALTER TABLE " <> tblName
             <> " RENAME COLUMN " <> q oldN <> " TO " <> q renamed)
  pure sameHide

-- | FULL OUTER JOIN top 2 stack views on shared categorical columns.
run :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
run s = case tl s of
  [] -> pure Nothing
  (parent : _) -> do
    let left  = Nav.tbl (View.nav parent)
        right = View.tbl s
        common = commonCols left right
    if V.null common
      then do
        Render.statusMsg "diff: no common columns"
        pure Nothing
      else case resolveKeys (Nav.grp (View.nav parent)) (Nav.grp (View.nav (View.cur s))) common of
        Nothing -> do
          Render.statusMsg "diff: no key columns (need categorical columns with same name+type)"
          pure Nothing
        Just (allKeys, valCols) -> do
          tblName <- buildJoinTbl left right allKeys valCols common
          sameHide_ <- renameCols tblName valCols
          let query_ = Prql.defaultQuery { Prql.base = "from " <> tblName }
          total <- Table.queryCount query_
          mAdbc <- Table.requery query_ total
          case mAdbc of
            Nothing -> pure Nothing
            Just adbc -> case View.pop s of
              Nothing -> pure Nothing
              Just s' ->
                pure $ fmap
                  (\v -> View.setCur s' (v & #disp .~ "diff" & #sameHide .~ sameHide_))
                  (View.fromTbl adbc (View.path (View.cur s')) 0 allKeys 0)

-- | Clear sameHide to reveal identical-value columns (toggle)
showSame :: View AdbcTable -> View AdbcTable
showSame v = v & #sameHide .~ V.empty

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdTblDiff "S" "d" "Diff top two views" False "")
        (\a ci _ ->
          if V.null (View.sameHide (View.cur (stk a)))
            then tryStk a ci (run (stk a))
            else pure (ActOk (resetVS
                    (a & #stk .~ View.setCur (stk a) (showSame (View.cur (stk a)))))))
  ]
