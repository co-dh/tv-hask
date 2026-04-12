-- | Replay: render compact PRQL ops string from view state.
-- Displayed on tab line; can be replayed with @tv file -p "ops"@.
-- Port of Tc/Replay.lean + Op.render from Tc/Data/ADBC/Prql.lean.
module Tv.Replay
  ( renderOp
  , renderOps
  , opsStr
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Tv.Types

-- | Quote column name with backticks (PRQL identifier quoting)
quote :: Text -> Text
quote s = "`" <> s <> "`"

-- | DuckDB-quote for use inside PRQL s-strings
dqQuote :: Text -> Text
dqQuote s = "\\\"" <> s <> "\\\""

renderSort :: (Text, Bool) -> Text
renderSort (col, asc) = let qc = quote col in if asc then qc else "-" <> qc

-- | Render single Op to PRQL string. Mirrors Tc.Data.ADBC.Prql.Op.render.
renderOp :: Op -> Text
renderOp = \case
  OpFilter e -> "filter " <> e
  OpSort cols -> "sort {" <> T.intercalate ", " (V.toList (V.map renderSort cols)) <> "}"
  OpSel cols -> "select {" <> T.intercalate ", " (V.toList (V.map quote cols)) <> "}"
  OpExclude cols -> "select s\"* EXCLUDE (" <> T.intercalate ", " (V.toList (V.map dqQuote cols)) <> ")\""
  OpDerive bs -> "derive {" <> T.intercalate ", " (V.toList (V.map (\(n, e) -> quote n <> " = " <> e) bs)) <> "}"
  OpGroup keys aggs ->
    let ks = T.intercalate ", " (V.toList (V.map quote keys))
        as = T.intercalate ", " (V.toList (V.map (\(fn, name, col) -> name <> " = " <> prqlAgg fn <> " " <> quote col) aggs))
    in "group {" <> ks <> "} (aggregate {" <> as <> "})"
  OpTake n -> "take " <> T.pack (show n)

-- | PRQL aggregate function names (std. prefix)
prqlAgg :: Agg -> Text
prqlAgg = \case
  ACount -> "std.count"; ASum -> "std.sum"; AAvg -> "std.average"
  AMin -> "std.min"; AMax -> "std.max"; AStddev -> "std.stddev"; ADist -> "std.count_distinct"

-- | Render ops portion only (no base/from). Empty string if no ops.
renderOps :: Vector Op -> Text
renderOps ops
  | V.null ops = ""
  | otherwise  = T.intercalate " | " (V.toList (V.map renderOp ops))

-- | Extract PRQL pipeline ops string from a saved ops vector.
-- Returns empty string if no ops applied. Mirrors Tc.Replay.opsStr
-- which reads v.nav.tbl.query.renderOps — in the Haskell port TblOps
-- doesn't carry a query, so the caller passes the ops directly
-- (e.g. from SavedView.svQueryOps or a future query field).
opsStr :: Vector Op -> Text
opsStr = renderOps
