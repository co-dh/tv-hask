{-
  Split: split column by delimiter/regex into multiple new columns.
  Press ':' -> fzf prompt with common delimiters -> detect part count ->
  derive split columns -> push view.

  Literal port of Tc/Tc/Split.lean. Same function names and order, same
  comments, no invented abstractions.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Split
  ( runWith
  , run
  ) where

import Control.Exception (SomeException, try)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Optics.Core ((&), (.~))

import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Data.ADBC.Prql as Prql
import qualified Tv.Data.ADBC.Table as Table
import Tv.Data.ADBC.Table (AdbcTable, stripSemi)
import Tv.Data.ADBC.Ops (quoteId)
import qualified Tv.Fzf as Fzf
import qualified Tv.Nav as Nav
import Tv.Types (ColType(..), Op(..), escSql)
import qualified Tv.Util as Log
import Tv.View (ViewStack)
import qualified Tv.View as View

-- Common delimiters offered as suggestions (first is used in test mode)
suggestions :: Text
suggestions = "-\n,\n;\n:\n|\n\\s+\n_\n/"

-- | Find max number of parts when splitting column by pattern.
-- Compiles current PRQL query to SQL, wraps in aggregate to count max split parts.
maxParts :: Prql.Query -> Text -> Text -> IO Int
maxParts q col ep = do
  mBase <- Prql.compile (Prql.queryRender q)
  case mBase of
    Nothing -> pure 0
    Just baseSql -> do
      let sql = stripSemi baseSql
          qid = quoteId col
          countSql =
            "SELECT COALESCE(max(array_length(string_split_regex("
            <> qid <> ", '" <> ep <> "'))), 0) FROM (" <> sql <> ")"
      r <- try $ do
        qr_ <- Adbc.query countSql
        v <- Adbc.cellInt qr_ 0 0
        pure (min (fromIntegral v :: Int) 20)  -- cap at 20 columns
      case r of
        Left (e :: SomeException) -> do
          Log.write "split" ("maxParts: " <> T.pack (show e))
          pure 0
        Right n -> pure n

-- | Build derive bindings: col_1 = s"string_split_regex({col}, 'pat')[1]", ...
splitBindings :: Text -> Text -> Text -> Int -> Vector (Text, Text)
splitBindings col ep qc n =
  V.generate n $ \i ->
    let idx = i + 1
        idxT = T.pack (show idx)
    in ( col <> "_" <> idxT
       , "s\"string_split_regex({" <> qc <> "}, '" <> ep <> "')[" <> idxT <> "]\""
       )

-- | Split column by pattern (no fzf). Called by socket/dispatch and by `run` after fzf.
runWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
runWith s pat = do
  let nav = View.nav (View.cur s)
      curName = Nav.curColName nav
      typ = Nav.curColType nav
  if typ /= ColTypeStr then pure s  -- only split string columns
  else if T.null pat then pure s
  else do
    let ep = escSql pat
        qc = Prql.quote curName
    n <- maxParts (Table.query (View.tbl s)) curName ep
    if n <= 1 then pure s  -- nothing to split
    else do
      let bindings = splitBindings curName ep qc n
          q = Prql.pipe (Table.query (View.tbl s)) (OpDerive bindings)
      Log.write "split" (Prql.queryRender q)
      mTbl <- Table.requery q (Table.totalRows (View.tbl s))
      case mTbl of
        Nothing -> pure s
        Just tbl' ->
          let nCols = V.length (Table.colNames tbl')
              firstSplitCol = nCols - n
              oldView = View.cur s
          in case View.rebuild oldView tbl' firstSplitCol (Nav.grp (View.nav oldView)) 0 of
               Nothing -> pure s
               Just v  -> pure (View.push s (v & #disp .~ ":" <> curName))

-- | Prompt for pattern via fzf, then split.
run :: ViewStack AdbcTable -> IO (ViewStack AdbcTable)
run s = do
  let curName = Nav.curColName (View.nav (View.cur s))
      header = "Split '" <> curName <> "' by delimiter or regex"
  mRaw <- Fzf.fzf
    (V.fromList ["--print-query", "--prompt=split: ", "--header=" <> header])
    suggestions
  case mRaw of
    Nothing  -> pure s
    Just raw -> runWith s (T.strip raw)
