{-
  Table operations: split column, derive column, join/union/diff tables.
  All three follow the same shape: prompt → PRQL → rebuild view.
  Merged from Split.hs, Derive.hs, Join.hs.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Ops where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V
import Text.Read (readMaybe)

import Tv.App.Types (HandlerFn, stackIO)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import Tv.Types (Cmd(..), ColType(..), Op(..), toString, escSql, exprError)
import qualified Tv.Render as Render
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable, tmpName, stripSemi)
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Ops (maxSplitParts, createTempView, createTempTable)
import Tv.Fzf (fzfIdx)
import qualified Tv.Fzf as Fzf
import qualified Tv.Nav as Nav
import Tv.View (ViewStack)
import qualified Tv.View as View
import qualified Tv.Util as Log

-- ============================================================================
-- Shared: pipe PRQL op → requery → rebuild → push
-- ============================================================================

-- | Pipe an Op onto the current query, requery, rebuild view at given column,
-- and push with a display label. The common tail of split and derive.
pipeAndPush :: ViewStack AdbcTable -> Op -> Text -> (AdbcTable -> Int) -> IO (ViewStack AdbcTable)
pipeAndPush s op label colIdxFn = do
  let q = Prql.pipe (View.tbl s ^. #query) op
  Log.write "ops" (Prql.queryRender q)
  mTbl <- Table.requery q (View.tbl s ^. #totalRows)
  case mTbl of
    Nothing -> pure s
    Just tbl' ->
      case View.rebuild (View.cur s) tbl' (colIdxFn tbl') (View.cur s ^. #nav % #grp) 0 of
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
  let nav = View.cur s ^. #nav
      curName = Nav.colName nav
  if Nav.colType nav /= ColTypeStr || T.null pat then pure s
  else do
    let ep = escSql pat
    mBase <- Prql.compile (Prql.queryRender (View.tbl s ^. #query))
    case mBase of
      Nothing -> pure s
      Just baseSql -> do
        n <- maxSplitParts (stripSemi baseSql) curName ep
        if n <= 1 then pure s
        else pipeAndPush s (OpDerive (splitBindings curName ep (Prql.ref curName) n))
               (":" <> curName) (\t -> V.length (t ^. #colNames) - n)

splitRun :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
splitRun tm s = do
  let curName = Nav.colName (View.cur s ^. #nav)
      header = "Split '" <> curName <> "' by delimiter or regex"
  mRaw <- Fzf.fzf tm
    (V.fromList ["--print-query", "--prompt=split: ", "--header=" <> header])
    splitSuggestions
  maybe (pure s) (\raw -> splitRunWith s (T.strip raw)) mRaw

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
  Nothing -> pure s
  Just (name, expr) -> case exprError expr of
    Just msg -> Render.errorPopup ("derive: " <> msg) >> pure s
    Nothing  ->
      pipeAndPush s (OpDerive (V.singleton (name, expr)))
        ("=" <> name) (\t -> V.length (t ^. #colNames) - 1)

deriveRun :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
deriveRun tm s = do
  let nav     = View.cur s ^. #nav
      names   = Nav.colNames nav
      curName = Nav.colName nav
      typ     = Nav.colType nav
      header  = "name = expr\n" <> samples curName typ
      hint    = colHints names (View.tbl s ^. #colTypes)
  mRaw <- Fzf.fzf tm
    (V.fromList ["--print-query", "--prompt=derive: ", "--header=" <> header])
    hint
  maybe (pure s) (\raw -> deriveRunWith s (T.strip raw)) mRaw

-- ============================================================================
-- Join
-- ============================================================================

data JoinOp = JoinInner | JoinLeft | JoinRight | JoinUnion | JoinDiff
  deriving (Eq, Show)

joinCond :: Vector Text -> Text
-- | PRQL join condition. Uses the @(==col)@ shorthand which expands to
-- @left.col == right.col@; the shorthand takes a bare identifier, so
-- keep the backtick-quoted form — prefixing @this.@ here would turn the
-- expression into a non-join boolean that PRQL accepts but yields
-- empty results.
joinCond cols = T.intercalate " && " (V.toList (V.map (\c -> "==" <> Prql.quote c) cols))


joinSide :: JoinOp -> Text
joinSide JoinInner = ""
joinSide JoinLeft  = "side:left "
joinSide JoinRight = "side:right "
joinSide _         = ""

prqlStr :: Text -> Text -> Vector Text -> JoinOp -> Text
prqlStr lName rName _    JoinUnion = "from " <> lName <> " | append " <> rName
prqlStr lName rName _    JoinDiff  = "from " <> lName <> " | remove " <> rName
prqlStr lName rName cols op        =
  "from " <> lName <> " | join " <> joinSide op <> rName
    <> " (" <> joinCond cols <> ")"

allOps :: Vector JoinOp
allOps = V.fromList [JoinInner, JoinLeft, JoinRight, JoinUnion, JoinDiff]

prepareView :: AdbcTable -> Text -> IO (Text, Text)
prepareView tbl suffix = do
  name <- tmpName ("j" <> suffix)
  let prql = Prql.queryRender (tbl ^. #query)
  Log.write "prql" prql
  mSql <- Prql.compile prql
  maybe (ioError (userError "PRQL compile failed"))
        (\sql -> pure (name, stripSemi sql))
        mSql

execJoin :: ViewStack AdbcTable -> JoinOp -> Vector Text -> IO (Maybe (ViewStack AdbcTable))
execJoin s op leftGrp = do
  case listToMaybe (s ^. #tl) of
    Nothing -> pure Nothing
    Just parent -> do
      (lName, lSql) <- prepareView (parent ^. #nav % #tbl) "l"
      (rName, rSql) <- prepareView (View.tbl s) "r"
      createTempView lName lSql
      createTempView rName rSql
      let prql = prqlStr lName rName leftGrp op
      Log.write "prql" prql
      tblName <- tmpName "join"
      mSql <- Prql.compile prql
      case mSql of
        Nothing  -> ioError (userError ("join PRQL compile failed: " <> T.unpack prql))
        Just sql -> do
          createTempTable tblName (stripSemi sql)
          mAdbc <- Table.fromTmp tblName
          case mAdbc of
            Nothing -> pure Nothing
            Just adbc -> case View.pop s of
              Nothing -> pure Nothing
              Just s' ->
                let disp_ = case op of
                      JoinUnion -> "union"
                      JoinDiff  -> "diff"
                      _ -> "⋈ (" <> T.intercalate ", " (V.toList leftGrp) <> ")"
                    mView = View.fromTbl adbc (View.cur s' ^. #path) 0 V.empty 0
                in pure $ fmap (\v -> View.setCur s' (v & #disp .~ disp_)) mView

resolveOps :: ViewStack AdbcTable -> Maybe (Vector JoinOp, Vector Text)
resolveOps s = do
  parent <- listToMaybe (s ^. #tl)
  let leftGrp = parent ^. #nav % #grp
      joinOk  = not (V.null leftGrp) && leftGrp == (View.cur s ^. #nav % #grp)
  pure (if joinOk then allOps else V.fromList [JoinUnion, JoinDiff], leftGrp)


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
        (\a _ arg -> stackIO a (if T.null arg then splitRun (a ^. #testMode) (a ^. #stk) else splitRunWith (a ^. #stk) arg))
  , hdl (mkEntry CmdColDerive "a" "=" "Derive new column (name = expr)" False "")
        (\a _ arg -> stackIO a (if T.null arg then deriveRun (a ^. #testMode) (a ^. #stk) else deriveRunWith (a ^. #stk) arg))
  , hdl (mkEntry CmdTblJoin "Sa" "J" "Join tables" False "")
        (\a _ arg -> stackIO a (do
          ms <- joinRunWith (a ^. #stk) arg
          pure (fromMaybe (a ^. #stk) ms)))
  ]
