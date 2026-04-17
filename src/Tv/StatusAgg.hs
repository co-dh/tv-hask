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

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import Tv.Types (ColCache(..), ColType (..), isNumeric)

-- | Cache: path + colIdx keyed → formatted agg string.
type Cache = ColCache Int Text

cacheEmpty :: Cache
cacheEmpty = ColCache "" 0 ""

-- | Aggregate stats for a single column: sum, avg, count (non-null).
-- Returns formatted string like "Σ1234 μ12.3 #100" for numeric, "#100" for non-numeric.
-- For large files, skip expensive SUM/AVG (no parquet shortcut) and show count only.
compute :: AdbcTable -> Int -> IO Text
compute t colIdx = do
  if Table.totalRows t > Table.prqlLimit
    then pure (T.pack ("#" ++ show (Table.totalRows t)))
    else do
      let colName = fromMaybe "" $ Table.colNames t V.!? colIdx
          colTy   = fromMaybe ColTypeOther $ Table.colTypes t V.!? colIdx
          isNum   = isNumeric colTy
      mBase <- Prql.compile $ Prql.queryRender $ Table.query t
      case mBase of
        Nothing -> pure ""
        Just baseSql -> do
          let sql = Ops.colAggSql (Table.stripSemi baseSql) colName isNum
          r <- try (Conn.query sql) :: IO (Either SomeException Conn.QueryResult)
          case r of
            Left _   -> pure ""
            Right qr_ ->
              if isNum then do
                sm <- Conn.cellStr qr_ 0 0
                av <- Conn.cellStr qr_ 0 1
                ct <- Conn.cellStr qr_ 0 2
                let fmt s = if T.length s > 8 then T.take 8 s else s
                pure ("Σ" <> fmt sm <> " μ" <> fmt av <> " #" <> ct)
              else do
                ct <- Conn.cellStr qr_ 0 0
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
  cache' <-
    if cachedPath cache == path && cachedCol cache == colIdx
      then pure cache
      else do
        agg <- do
          r <- try (compute tbl colIdx) :: IO (Either SomeException Text)
          case r of
            Left _  -> pure ""
            Right a -> pure a
        pure (ColCache path colIdx agg)
  render (cachedVal cache')
  pure cache'
