{-
  Table operations: split column, derive column, join/union/diff tables.
  All three follow the same shape: prompt → PRQL → rebuild view.
  Merged from Split.hs, Derive.hs, Join.hs.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Ops
  ( -- * Split
    splitRunWith
  , splitRun
    -- * Derive (exported for tests/hints)
  , samples
  , toString
  , colHints
  , parseDerive
  , deriveRunWith
  , deriveRun
    -- * Join
  , JoinOp(..)
  , joinRun
  , joinRunWith
    -- * Commands
  , commands
  ) where

import Data.Maybe (fromMaybe, listToMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Text.Read (readMaybe)

import Optics.Core ((&), (.~))

import Tv.App.Types (AppState(..), HandlerFn, stackIO)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import Tv.Types (Cmd(..), ColType(..), toString, escSql)
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable, tmpName, stripSemi)
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Ops (maxSplitParts, createTempView)
import qualified Tv.Df.Prql as DfQ
import Tv.Fzf (fzfIdx)
import qualified Tv.Fzf as Fzf
import qualified Tv.Nav as Nav
import Tv.View (ViewStack)
import qualified Tv.View as View
import qualified Tv.Log as Log

-- ============================================================================
-- Shared: pipe PRQL op → requery → rebuild → push
-- ============================================================================

-- | Compile a 'Tv.Df.Prql.Query' through DuckDB, then rebuild + push
-- the View onto the stack. Shared tail used by split and derive.
pushQuery :: ViewStack AdbcTable -> DfQ.Query -> Text -> (AdbcTable -> Int)
          -> IO (ViewStack AdbcTable)
pushQuery s q label colIdxFn = do
  Log.write "ops" (DfQ.compile q)
  mTbl <- Table.fromPrqlInline (DfQ.compile q)
  case mTbl of
    Nothing -> pure s
    Just tbl' ->
      case View.rebuild (View.cur s) tbl' (colIdxFn tbl')
             (Nav.grp (View.nav (View.cur s))) 0 of
        Nothing -> pure s
        Just v  -> pure (View.push s (v & #disp .~ label))

-- ============================================================================
-- Split
-- ============================================================================

splitSuggestions :: Text
splitSuggestions = "-\n,\n;\n:\n|\n\\s+\n_\n/"

splitBindings :: Text -> Text -> Text -> Int -> Vector (Text, Text)
splitBindings col ep qc n =
  V.generate n $ \i ->
    let idx = T.pack (show (i + 1))
    in ( col <> "_" <> idx
       , "s\"string_split_regex({" <> qc <> "}, '" <> ep <> "')[" <> idx <> "]\""
       )

splitRunWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
splitRunWith s pat = do
  let nav = View.nav (View.cur s)
      curName = Nav.colName nav
  if Nav.colType nav /= ColTypeStr || T.null pat then pure s
  else do
    let ep = escSql pat
    mBase <- Prql.compile (Prql.queryRender (Table.query (View.tbl s)))
    case mBase of
      Nothing -> pure s
      Just baseSql -> do
        n <- maxSplitParts (stripSemi baseSql) curName ep
        if n <= 1 then pure s
        else do
          let bs   = splitBindings curName ep (Prql.quote curName) n
              bsT  = T.intercalate ", "
                       [ Prql.quote nm <> " = " <> e
                       | (nm, e) <- V.toList bs ]
              baseRend = Prql.queryRender (Table.query (View.tbl s))
              q = DfQ.fromBase baseRend
                    DfQ.|> DfQ.rawStage ("derive {" <> bsT <> "}")
          pushQuery s q (":" <> curName) (\t -> V.length (Table.colNames t) - n)

splitRun :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
splitRun tm s = do
  let curName = Nav.colName (View.nav (View.cur s))
      header = "Split '" <> curName <> "' by delimiter or regex"
  mRaw <- Fzf.fzf tm
    (V.fromList ["--print-query", "--prompt=split: ", "--header=" <> header])
    splitSuggestions
  case mRaw of
    Nothing  -> pure s
    Just raw -> splitRunWith s (T.strip raw)

-- ============================================================================
-- Derive
-- ============================================================================

samples :: Text -> ColType -> Text
samples col ty = case ty of
  ColTypeInt     -> numSamples
  ColTypeFloat   -> numSamples
  ColTypeDecimal -> numSamples
  ColTypeStr ->
    "d = " <> col <> " != null | d = f\"{col}-{other}\" | d = " <> col <> " | text.upper"
  ColTypeDate ->
    "d = " <> col <> " != null | d = " <> col <> " | date.year | d = " <> col <> " - @2024-01-01"
  ColTypeTime      -> timeSamples
  ColTypeTimestamp -> timeSamples
  ColTypeBool ->
    "d = " <> col <> " != null | d = " <> col <> " == false | d = !" <> col
  ColTypeOther ->
    "d = " <> col <> " != null | d = " <> col <> " > 0 | d = f\"{col}\""
  where
    numSamples =
      "d = " <> col <> " * 2 | d = " <> col <> " != null | d = math.round 2 " <> col
    timeSamples =
      "d = " <> col <> " != null | d = " <> col <> " | date.hour | d = " <> col <> " | date.minute"

colHints :: Vector Text -> Vector ColType -> Text
colHints names types =
  let maxLen = V.foldl' (\mx n -> max mx (T.length n)) 0 names
      line i n =
        let pad = T.replicate (maxLen - T.length n) " "
            ty  = fromMaybe ColTypeOther $ types V.!? i
        in n <> pad <> " : " <> toString ty
  in T.intercalate "\n" (V.toList (V.imap line names))

parseDerive :: Text -> Maybe (Text, Text)
parseDerive input =
  case T.splitOn " = " input of
    []            -> Nothing
    (name : rest) ->
      let n = T.strip name
          e = T.strip (T.intercalate " = " rest)
      in if T.null n || T.null e then Nothing else Just (n, e)

deriveRunWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
deriveRunWith s input = case parseDerive input of
  Nothing           -> pure s
  Just (name, expr) -> do
    let baseRend = Prql.queryRender (Table.query (View.tbl s))
        q = DfQ.fromBase baseRend
              DfQ.|> DfQ.rawStage
                       ("derive {" <> Prql.quote name <> " = " <> expr <> "}")
    mTbl <- Table.fromPrqlInline (DfQ.compile q)
    case mTbl of
      Nothing   -> pure s
      Just tbl' ->
        let colIdx = V.length (Table.colNames tbl') - 1
            grp'   = Nav.grp (View.nav (View.cur s))
        in case View.rebuild (View.cur s) tbl' colIdx grp' 0 of
             Nothing -> pure s
             Just v  -> pure (View.push s (v & #disp .~ ("=" <> name)))


deriveRun :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
deriveRun tm s = do
  let nav     = View.nav (View.cur s)
      names   = Nav.colNames nav
      curName = Nav.colName nav
      typ     = Nav.colType nav
      header  = "name = expr\n" <> samples curName typ
      hint    = colHints names (Table.colTypes (View.tbl s))
  mRaw <- Fzf.fzf tm
    (V.fromList ["--print-query", "--prompt=derive: ", "--header=" <> header])
    hint
  case mRaw of
    Nothing  -> pure s
    Just raw -> deriveRunWith s (T.strip raw)

-- ============================================================================
-- Join
-- ============================================================================

data JoinOp = JoinInner | JoinLeft | JoinRight | JoinUnion | JoinDiff
  deriving (Eq, Show)

joinCond :: Vector Text -> Text
joinCond cols = T.intercalate " && " (V.toList (V.map (\c -> "==" <> Prql.quote c) cols))

opLabel :: JoinOp -> Text
opLabel JoinInner = "join inner"
opLabel JoinLeft  = "join left"
opLabel JoinRight = "join right"
opLabel JoinUnion = "union"
opLabel JoinDiff  = "set diff"

joinSide :: JoinOp -> Text
joinSide JoinInner = ""
joinSide JoinLeft  = "side:left "
joinSide JoinRight = "side:right "
joinSide _         = ""

-- | Build the join/append/remove pipeline through 'Tv.Df.Prql'.
-- Union/Diff use typed 'append' / a raw stage for PRQL's @remove@
-- (no typed variant yet); joins use rawStage for the PRQL @==col@
-- shorthand, which has no dataframe Expr equivalent.
joinQuery :: Text -> Text -> Vector Text -> JoinOp -> DfQ.Query
joinQuery lName rName cols op =
  DfQ.fromBase ("from " <> lName) DfQ.|> case op of
    JoinUnion -> DfQ.append rName
    JoinDiff  -> DfQ.rawStage ("remove " <> rName)
    _         -> DfQ.rawStage
                   ("join " <> joinSide op <> rName
                    <> " (" <> joinCond cols <> ")")

allOps :: Vector JoinOp
allOps = V.fromList [JoinInner, JoinLeft, JoinRight, JoinUnion, JoinDiff]

prepareView :: AdbcTable -> Text -> IO (Text, Text)
prepareView tbl suffix = do
  name <- tmpName ("j" <> suffix)
  let prql = Prql.queryRender (Table.query tbl)
  Log.write "prql" prql
  mSql <- Prql.compile prql
  case mSql of
    Nothing  -> ioError (userError "PRQL compile failed")
    Just sql -> pure (name, stripSemi sql)

execJoin :: ViewStack AdbcTable -> JoinOp -> Vector Text -> IO (Maybe (ViewStack AdbcTable))
execJoin s op leftGrp = do
  case listToMaybe (View.tl s) of
    Nothing -> pure Nothing
    Just parent -> do
      (lName, lSql) <- prepareView (Nav.tbl (View.nav parent)) "l"
      (rName, rSql) <- prepareView (View.tbl s) "r"
      createTempView lName lSql
      createTempView rName rSql
      let q = joinQuery lName rName leftGrp op
      -- Materialize: join results are the shape users scroll and re-
      -- filter; paying the CREATE TEMP TABLE once beats re-running
      -- the join on every fetchMore.
      mAdbc <- Table.fromPrqlMaterialized "join" (DfQ.compile q)
      case mAdbc of
        Nothing -> pure Nothing
        Just adbc -> case View.pop s of
          Nothing -> pure Nothing
          Just s' ->
            let disp_ = case op of
                  JoinUnion -> "union"
                  JoinDiff  -> "diff"
                  _ -> "⋈ (" <> T.intercalate ", " (V.toList leftGrp) <> ")"
                mView = View.fromTbl adbc (View.path (View.cur s')) 0 V.empty 0
            in pure $ fmap (\v -> View.setCur s' (v & #disp .~ disp_)) mView

resolveOps :: ViewStack AdbcTable -> Maybe (Vector JoinOp, Vector Text)
resolveOps s = do
  parent <- listToMaybe (View.tl s)
  let leftGrp = Nav.grp (View.nav parent)
      joinOk  = not (V.null leftGrp) && leftGrp == Nav.grp (View.nav (View.cur s))
  pure (if joinOk then allOps else V.fromList [JoinUnion, JoinDiff], leftGrp)

joinRun :: Bool -> ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
joinRun tm s = case resolveOps s of
  Nothing -> pure Nothing
  Just (ops, leftGrp) -> case listToMaybe (View.tl s) of
    Nothing -> pure Nothing
    Just parent -> do
      (lName, _) <- prepareView (Nav.tbl (View.nav parent)) "l"
      (rName, _) <- prepareView (View.tbl s) "r"
      let items = V.map (\op -> opLabel op <> "  |  " <> DfQ.compile (joinQuery lName rName leftGrp op)) ops
      mIdx <- fzfIdx tm (V.fromList ["--prompt=join> "]) items
      case mIdx of
        Nothing  -> pure Nothing
        Just idx ->
          let op = fromMaybe JoinInner $ ops V.!? idx
          in execJoin s op leftGrp

joinRunWith :: ViewStack AdbcTable -> Text -> IO (Maybe (ViewStack AdbcTable))
joinRunWith s idxStr = case resolveOps s of
  Nothing -> pure Nothing
  Just (ops, leftGrp) -> do
    let idx = fromMaybe 0 (readMaybe (T.unpack idxStr) :: Maybe Int)
    if idx >= V.length ops
      then pure Nothing
      else execJoin s (fromMaybe JoinInner (ops V.!? idx)) leftGrp

-- ============================================================================
-- Commands
-- ============================================================================

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdColSplit "ca" ":" "Split column by delimiter" False "")
        (\a _ arg -> stackIO a (if T.null arg then splitRun (testMode a) (stk a) else splitRunWith (stk a) arg))
  , hdl (mkEntry CmdColDerive "a" "=" "Derive new column (name = expr)" False "")
        (\a _ arg -> stackIO a $
           if T.null arg then deriveRun (testMode a) (stk a) else deriveRunWith (stk a) arg)
  , hdl (mkEntry CmdTblJoin "Sa" "J" "Join tables" False "")
        (\a _ arg -> stackIO a (do
          ms <- joinRunWith (stk a) arg
          case ms of
            Just s' -> pure s'
            Nothing -> pure (stk a)))
  ]
