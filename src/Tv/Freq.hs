{-
  Freq: group by columns, count, pct, bar.
  Pure update returns residual Effect; dispatch executes IO inline.
  execFreq is the @-b df@ compute path: builds the PRQL through the
  q-like Tv.Df.Query API instead of raw-text concatenation, then runs
  through DuckDB like the default path.
-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Freq
  ( filterIO
  , update
  , execFreq
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Read as TR
import Data.Maybe (fromMaybe)
import Data.Vector (Vector)
import qualified Data.Vector as V

import qualified Tv.Nav as Nav
import Tv.Types
  ( Cmd (..)
  , Effect (..)
  , ViewKind (..)
  , toPrql
  )
import qualified Tv.View as View
import Tv.View (ViewStack)
import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Prql as Prql
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)

import qualified DataFrame.Functions as F
import DataFrame.Operators (as)
import qualified Tv.Df.Prql as DfQ

-- Truncate freq results to this many rows for display (matches the
-- DuckDB path's @| take 1000@).
freqDisplayLimit :: Int
freqDisplayLimit = 1000

-- | Build filter expression from freq view row
filterIO :: AdbcTable -> Vector Text -> Int -> IO Text
filterIO tbl cols row = do
  let names = Table.colNames tbl
      idxs  = V.mapMaybe (Nav.idxOf names) cols
  fetchedCols <- Ops.getCols tbl idxs row (row + 1)
  let vals = V.zipWith (\txtCol colIdx ->
        toPrql (Ops.colType tbl colIdx) (fromMaybe "" (txtCol V.!? 0))
        ) fetchedCols idxs
      exprs = V.zipWith (\c v -> c <> " == " <> v) cols vals
  pure (T.intercalate " && " (V.toList exprs))

-- | Freq via the q-like API. Reachable as the @-b df@ backend. Builds
-- the PRQL pipeline through 'Tv.Df.Query' + 'Tv.Df.Expr' combinators
-- instead of string concatenation, then runs it through DuckDB like
-- the regular path. Total group count comes from a separate
-- @cntdist@ pipeline, same as 'Tv.Data.DuckDB.Table.freqTable'.
execFreq :: AdbcTable -> Vector Text -> IO (Maybe (AdbcTable, Int))
execFreq t cNames
  | V.null cNames = pure Nothing
  | otherwise = do
      let baseRend = Prql.queryRender (Table.query t)
          cs       = V.toList cNames
          -- Group by the user-picked key columns, count rows. Pct is
          -- derived inside a window so std.sum Cnt is the total over
          -- the result set, not the per-group sum. Bar stays as a raw
          -- SQL fragment — repeat(chr, n) has no dataframe-Expr form.
          freqQ = DfQ.fromBase baseRend
            DfQ.|> DfQ.groupAgg cs
                     [F.count DfQ.this `as` "Cnt"]
            -- Pct needs a window sum (std.sum over the outer frame), and
            -- Bar uses a DuckDB s-string for repeat(); neither is in
            -- dataframe's Expr today. One rawStage covers both.
            DfQ.|> DfQ.rawStage
                     "derive {Pct = Cnt * 100 / std.sum Cnt, \
                     \Bar = s\"repeat('#', CAST(Pct/5 AS INTEGER))\"}"
            DfQ.|> DfQ.sortDesc ["Cnt"]
            DfQ.|> DfQ.take_ freqDisplayLimit
          prql = DfQ.compile freqQ
      totalGroups <- countDistinctGroups baseRend cNames
      Table.fromPrqlMaterialized "freq" prql >>= \case
        Just t' -> pure (Just (t', totalGroups))
        Nothing -> pure Nothing

-- Count of distinct group tuples — used for the "/N" status display.
-- Matches the existing DuckDB path's `cntdist {cs}` query.
countDistinctGroups :: Text -> Vector Text -> IO Int
countDistinctGroups baseRend cNames = do
  let cols = T.intercalate ", " (V.toList (V.map Prql.quote cNames))
      prql = baseRend <> " | cntdist {" <> cols <> "}"
  m <- Prql.compile prql
  case m of
    Nothing -> pure 0
    Just sql -> do
      qr <- Conn.query sql
      nr <- Conn.nrows qr
      if fromIntegral nr == (0 :: Int)
        then pure 0
        else do
          v <- Conn.cellStr qr 0 0
          pure $ case TR.decimal v of
            Right (n, _) -> n
            Left _       -> 0

-- | Pure update by Cmd. Returns residual Effect for dispatch to execute.
update :: ViewStack AdbcTable -> Cmd -> Maybe (ViewStack AdbcTable, Effect)
update s h =
  let n        = View.nav (View.cur s)
      curName  = Nav.colName n
      colNames_ = if V.elem curName (Nav.grp n)
                    then Nav.grp n
                    else V.snoc (Nav.grp n) curName
  in case h of
       CmdFreqOpen   -> Just (s, EffectFreq colNames_)
       CmdFreqFilter -> case View.vkind (View.cur s) of
         VkFreqV cols _ ->
           Just (s, EffectFreqFilter cols (Nav.cur (Nav.row (View.nav (View.cur s)))))
         _ -> Nothing
       _ -> Nothing
