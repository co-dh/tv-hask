{-
  Join: combine top 2 stack views via PRQL join/append/remove.
  Key columns (nav.grp) determine join condition; fzf picks operation.

  Literal port of Tc/Tc/Join.lean — same names, same order, same comments.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Join
  ( JoinOp(..)
  , run
  , runWith
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Text.Read (readMaybe)

import Optics.Core ((&), (.~))

import qualified Tv.Data.ADBC.Adbc as Adbc
import Tv.Data.ADBC.Ops ()
import qualified Tv.Data.ADBC.Prql as Prql
import Tv.Data.ADBC.Table (AdbcTable, nextTmpName, stripSemi)
import qualified Tv.Data.ADBC.Table as Table
import Tv.Fzf (fzfIdx)
import qualified Tv.Nav as Nav
import Tv.View (ViewStack)
import qualified Tv.View as View
import qualified Tv.Util as Log

data JoinOp = JoinInner | JoinLeft | JoinRight | JoinUnion | JoinDiff
  deriving (Eq, Show)

-- PRQL join condition: ==col1 && ==col2
joinCond :: Vector Text -> Text
joinCond cols =
  T.intercalate " && " (V.toList (V.map (\c -> "==" <> Prql.quote c) cols))

opLabel :: JoinOp -> Text
opLabel JoinInner = "join inner"
opLabel JoinLeft  = "join left"
opLabel JoinRight = "join right"
opLabel JoinUnion = "union"
opLabel JoinDiff  = "set diff"

-- PRQL side modifier for join ops
joinSide :: JoinOp -> Text
joinSide JoinInner = ""
joinSide JoinLeft  = "side:left "
joinSide JoinRight = "side:right "
joinSide _         = ""  -- union/diff don't use join syntax

prqlStr :: Text -> Text -> Vector Text -> JoinOp -> Text
prqlStr lName rName _    JoinUnion = "from " <> lName <> " | append " <> rName
prqlStr lName rName _    JoinDiff  = "from " <> lName <> " | remove " <> rName
prqlStr lName rName cols op        =
  "from " <> lName <> " | join " <> joinSide op <> rName
    <> " (" <> joinCond cols <> ")"

allOps :: Vector JoinOp
allOps = V.fromList [JoinInner, JoinLeft, JoinRight, JoinUnion, JoinDiff]

-- Generate unique view name and compile PRQL to SQL (deferred DDL)
prepareView :: AdbcTable -> Text -> IO (Text, Text)
prepareView tbl suffix = do
  name <- nextTmpName ("j" <> suffix)
  let prql = Prql.queryRender (Table.query tbl)
  Log.write "prql" prql
  mSql <- Prql.compile prql
  case mSql of
    Nothing  -> ioError (userError "PRQL compile failed")
    Just sql -> pure (name, stripSemi sql)

-- | Execute join with resolved op. Shared by run (fzf) and runWith (socket).
execJoin :: ViewStack AdbcTable -> JoinOp -> Vector Text -> IO (Maybe (ViewStack AdbcTable))
execJoin s op leftGrp = do
  case headMay (View.tl s) of
    Nothing -> pure Nothing
    Just parent -> do
      (lName, lSql) <- prepareView (Nav.tbl (View.nav parent)) "l"
      (rName, rSql) <- prepareView (View.tbl s) "r"
      _ <- Adbc.query ("CREATE OR REPLACE TEMP VIEW " <> lName <> " AS " <> lSql)
      _ <- Adbc.query ("CREATE OR REPLACE TEMP VIEW " <> rName <> " AS " <> rSql)
      let prql = prqlStr lName rName leftGrp op
      Log.write "prql" prql
      tblName <- nextTmpName "join"
      mSql <- Prql.compile prql
      case mSql of
        Nothing  -> ioError (userError ("join PRQL compile failed: " <> T.unpack prql))
        Just sql -> do
          _ <- Adbc.query
                 ("CREATE OR REPLACE TEMP TABLE " <> tblName
                  <> " AS " <> stripSemi sql)
          mAdbc <- Table.fromTmpTbl tblName
          case mAdbc of
            Nothing -> pure Nothing
            Just adbc -> case View.pop s of
              Nothing -> pure Nothing
              Just s' ->
                let disp_ = case op of
                      JoinUnion -> "union"
                      JoinDiff  -> "diff"
                      _ -> "⋈ (" <> T.intercalate ", " (V.toList leftGrp) <> ")"
                    curV  = View.cur s'
                    mView = View.fromTbl adbc (View.path curV) 0 V.empty 0
                in pure (fmap (\v -> View.setCur s' (v & #disp .~ disp_)) mView)

-- | Resolve available ops from stack state
resolveOps :: ViewStack AdbcTable -> Maybe (Vector JoinOp, Vector Text)
resolveOps s = do
  parent <- headMay (View.tl s)
  let leftGrp = Nav.grp (View.nav parent)
      joinOk  = V.length leftGrp > 0 && leftGrp == Nav.grp (View.nav (View.cur s))
  pure (if joinOk then allOps else V.fromList [JoinUnion, JoinDiff], leftGrp)

-- Full workflow: validate stack, show fzf menu, execute, push result
run :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
run s = case resolveOps s of
  Nothing -> pure Nothing
  Just (ops, leftGrp) -> case headMay (View.tl s) of
    Nothing -> pure Nothing
    Just parent -> do
      (lName, _) <- prepareView (Nav.tbl (View.nav parent)) "l"
      (rName, _) <- prepareView (View.tbl s) "r"
      let items = V.map (\op -> opLabel op <> "  |  " <> prqlStr lName rName leftGrp op) ops
      mIdx <- fzfIdx (V.fromList ["--prompt=join> "]) items
      case mIdx of
        Nothing  -> pure Nothing
        Just idx ->
          let op = maybe JoinInner id (ops V.!? idx)
          in execJoin s op leftGrp

-- | Join by operation index directly (no fzf). Called by socket/dispatch.
runWith :: ViewStack AdbcTable -> Text -> IO (Maybe (ViewStack AdbcTable))
runWith s idxStr = case resolveOps s of
  Nothing -> pure Nothing
  Just (ops, leftGrp) -> do
    let idx = maybe 0 id (readMaybe (T.unpack idxStr) :: Maybe Int)
    if idx >= V.length ops
      then pure Nothing
      else
        let op = maybe JoinInner id (ops V.!? idx)
        in execJoin s op leftGrp

-- local helper (Lean: Array.head?/List.head?)
headMay :: [a] -> Maybe a
headMay []    = Nothing
headMay (x:_) = Just x
