{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Derive: add a computed column by re-querying the underlying table
-- with @SELECT *, <expr> AS <name> FROM (<original>)@ via DuckDB. Mirrors
-- Tc.Derive.runWith — the Lean version pipes a PRQL @derive@ step onto
-- the backing query; here we round-trip the materialized TblOps through
-- an in-memory DuckDB table so the user's SQL expression runs on real
-- typed columns.
module Tv.Derive
  ( addDerived
  , parseDerive
  , rebuildWith
  , quoteId
  ) where

import Control.Exception (try, SomeException)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Optics.Core ((^.))

import Tv.Types
import qualified Tv.Data.DuckDB as DB

-- | Parse "name = expr". Splits on the first @" = "@; rejoins the rest
-- so expressions containing @=@ survive. Returns Nothing if either side
-- is empty. Matches Tc.Derive.parseDerive.
parseDerive :: Text -> Maybe (Text, Text)
parseDerive input = case T.splitOn " = " input of
  (n : rest@(_:_)) ->
    let name = T.strip n
        expr = T.strip (T.intercalate " = " rest)
    in if T.null name || T.null expr then Nothing else Just (name, expr)
  _ -> Nothing

-- | Add a derived column. Constructs a temp DuckDB table from the
-- current TblOps (one text column per source column, cast via the
-- recorded ColType), then issues @SELECT *, <expr> AS <name> FROM t@.
-- On any failure the original TblOps is returned unchanged — same
-- fail-soft behaviour as Tc.Derive.runWith.
addDerived :: TblOps -> Text -> Text -> IO TblOps
addDerived ops name expr = do
  r <- try (rebuildWith ops (\sub -> "SELECT *, " <> expr <> " AS " <> quoteId name <> " FROM (" <> sub <> ")"))
  case r of
    Right ops' -> pure ops'
    Left (_ :: SomeException) -> pure ops

-- | Rebuild a TblOps by running @f sub@ where @sub@ is a SELECT that
-- reconstructs the current table rows. Shared between derive and split.
rebuildWith :: TblOps -> (Text -> Text) -> IO TblOps
rebuildWith ops wrap = do
  sub <- reconstructSql ops
  let sql = wrap sub
  conn <- DB.connect ":memory:"
  res  <- DB.query conn sql
  DB.mkDbOps res
  -- conn stays alive via chunk ForeignPtr retention chain.

-- | Build a SELECT whose rows/columns match the given TblOps. Each
-- source row becomes a VALUES tuple; each column is CAST to its
-- original ColType so expressions see typed data.
reconstructSql :: TblOps -> IO Text
reconstructSql ops = do
  let names = ops ^. tblColNames
      nr    = ops ^. tblNRows
      nc    = V.length names
      tys   = V.generate nc (ops ^. tblColType)
  rows <- V.generateM nr $ \r ->
            V.generateM nc $ \c -> (ops ^. tblCellStr) r c
  let castExpr i =
        let n = names V.! i; t = tys V.! i
        in "CAST(c" <> T.pack (show i) <> " AS " <> sqlType t <> ") AS " <> quoteId n
      selectList = T.intercalate ", " [castExpr i | i <- [0 .. nc - 1]]
      colAliases = T.intercalate ", " ["c" <> T.pack (show i) | i <- [0 .. nc - 1]]
      rowLit row = "(" <> T.intercalate ", " [cellLit (tys V.! i) ((row V.! i)) | i <- [0 .. nc - 1]] <> ")"
      valuesBody
        | nr == 0  = "SELECT " <> emptyNulls nc <> " WHERE 1=0"
        | otherwise = "VALUES " <> T.intercalate ", " (map rowLit (V.toList rows))
      inner = "(" <> valuesBody <> ") AS t(" <> colAliases <> ")"
  pure ("SELECT " <> selectList <> " FROM " <> inner)
  where
    emptyNulls n = T.intercalate ", " (replicate (max 1 n) "NULL")

-- | SQL type for a ColType. Falls back to VARCHAR for CTOther so any
-- text round-trips safely.
sqlType :: ColType -> Text
sqlType CTInt       = "BIGINT"
sqlType CTFloat     = "DOUBLE"
sqlType CTDecimal   = "DOUBLE"
sqlType CTStr       = "VARCHAR"
sqlType CTDate      = "DATE"
sqlType CTTime      = "TIME"
sqlType CTTimestamp = "TIMESTAMP"
sqlType CTBool      = "BOOLEAN"
sqlType CTOther     = "VARCHAR"

-- | Render a single cell as a SQL literal. Empty cells become NULL so
-- missing data doesn't break CAST.
cellLit :: ColType -> Text -> Text
cellLit _ "" = "NULL"
cellLit t v  = case t of
  CTInt      -> v
  CTFloat    -> v
  CTDecimal  -> v
  CTBool     -> if v == "true" || v == "1" then "TRUE" else "FALSE"
  _          -> "'" <> T.replace "'" "''" v <> "'"

-- | Quote a DuckDB identifier (double-quote, escape embedded quotes).
quoteId :: Text -> Text
quoteId t = "\"" <> T.replace "\"" "\"\"" t <> "\""
