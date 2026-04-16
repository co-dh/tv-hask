{-
  StatusAgg: column aggregation stats (sum/avg/count) for the status bar.
  Computes via DuckDB SQL, cached per column to avoid per-frame queries.

  Literal port of Tc/Tc/StatusAgg.lean.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.StatusAgg
  ( Cache
  , cacheEmpty
  , update
  ) where

import Control.Exception (SomeException, try)
import Control.Monad (unless)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Data.ADBC.Prql as Prql
import qualified Tv.Data.ADBC.Table as Table
import Tv.Data.ADBC.Table (AdbcTable)
import qualified Tv.Data.ADBC.Ops as Ops
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import Tv.Types (ColType (..), isNumeric)

-- | Cache: (path, colIdx, formatted agg string)
type Cache = (Text, Int, Text)

cacheEmpty :: Cache
cacheEmpty = ("", 0, "")

-- | Aggregate stats for a single column: sum, avg, count (non-null).
-- Returns formatted string like "Σ1234 μ12.3 #100" for numeric, "#100" for non-numeric.
-- For large files, skip expensive SUM/AVG (no parquet shortcut) and show count only.
compute :: AdbcTable -> Int -> IO Text
compute t colIdx = do
  if Table.totalRows t > Table.prqlLimit
    then pure (T.pack ("#" ++ show (Table.totalRows t)))
    else do
      let colName = fromMaybe "" (Table.colNames t V.!? colIdx)
          colTy   = fromMaybe ColTypeOther (Table.colTypes t V.!? colIdx)
          q       = Ops.quoteId colName
          isNum   = isNumeric colTy
      mSql <- do
        mBase <- Prql.compile (Prql.queryRender (Table.query t))
        case mBase of
          Nothing      -> pure Nothing
          Just baseSql ->
            let aggs = if isNum
                  then "CAST(SUM(" <> q <> ") AS DOUBLE) AS s, CAST(AVG(" <> q <> ") AS DOUBLE) AS a, COUNT(" <> q <> ") AS c"
                  else "COUNT(" <> q <> ") AS c"
            in pure (Just ("SELECT " <> aggs <> " FROM (" <> Table.stripSemi baseSql <> ")"))
      case mSql of
        Nothing  -> pure ""
        Just sql -> do
          r <- try (Adbc.query sql) :: IO (Either SomeException Adbc.QueryResult)
          case r of
            Left _   -> pure ""
            Right qr_ ->
              if isNum then do
                sm <- Adbc.cellStr qr_ 0 0
                av <- Adbc.cellStr qr_ 0 1
                ct <- Adbc.cellStr qr_ 0 2
                let fmt s = if T.length s > 8 then T.take 8 s else s
                pure ("Σ" <> fmt sm <> " μ" <> fmt av <> " #" <> ct)
              else do
                ct <- Adbc.cellStr qr_ 0 0
                pure ("#" <> ct)

-- | Render aggregation stats on the status bar at 1/3 width
render :: Text -> IO ()
render agg = unless (T.null agg) $ do
  ht <- Term.height
  w  <- Term.width
  let pos = w `div` 3
  s <- Theme.getStyles
  Term.print pos (ht - 1) (Theme.styleFg s Theme.sStatusDim) (Theme.styleBg s Theme.sStatusDim) agg

-- | Update cache if column changed, then render. Returns updated cache.
update :: Cache -> AdbcTable -> Text -> Int -> IO Cache
update cache tbl path colIdx = do
  let (cachedPath, cachedCol, _) = cache
  cache' <-
    if cachedPath == path && cachedCol == colIdx
      then pure cache
      else do
        agg <- do
          r <- try (compute tbl colIdx) :: IO (Either SomeException Text)
          case r of
            Left _  -> pure ""
            Right a -> pure a
        pure (path, colIdx, agg)
  let (_, _, agg) = cache'
  render agg
  pure cache'
