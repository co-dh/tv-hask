{-
  PRQL rendering and compilation
  Uses common Op types from Tv/Types.hs

  Pure PRQL query representation + renderer. PRQL -> SQL compilation and
  execution live in Tv.Data.DuckDB (the single place allowed to touch SQL).
-}
module Tv.Data.ADBC.Prql where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Tv.Types
  ( Agg (..)
  , Op (..)
  , joinWith
  )

-- | PRQL query: base table + operations (PRQL-specific base format)
data Query = Query
  { base :: Text      -- PRQL from clause
  , ops  :: Vector Op
  }

-- | Default query: "from df" with no ops
defaultQuery :: Query
defaultQuery = Query { base = "from df", ops = V.empty }

-- | Quote column name — always backtick to avoid PRQL keyword collisions
quote :: Text -> Text
quote s = "`" <> s <> "`"

-- | Render sort column (asc = col, desc = -col)
renderSort :: Text -> Bool -> Text
renderSort col asc =
  let qc = quote col
  in if asc then qc else "-" <> qc

-- | Render aggregate function name (std. prefix for PRQL)
aggName :: Agg -> Text
aggName AggCount  = "std.count"
aggName AggSum    = "std.sum"
aggName AggAvg    = "std.average"
aggName AggMin    = "std.min"
aggName AggMax    = "std.max"
aggName AggStddev = "std.stddev"
aggName AggDist   = "std.count_distinct"

-- | DuckDB-quote column name for use inside PRQL s-string (\" escapes)
dqQuote :: Text -> Text
dqQuote s = "\\\"" <> s <> "\\\""

-- | Render single operation to PRQL string
opRender :: Op -> Text
opRender (OpFilter e) = "filter " <> e
opRender (OpSort cols) =
  "sort {" <> joinWith (V.map (\(c, asc) -> renderSort c asc) cols) ", " <> "}"
opRender (OpSel cols) =
  "select {" <> joinWith (V.map quote cols) ", " <> "}"
opRender (OpExclude cols) =
  "select s\"* EXCLUDE (" <> joinWith (V.map dqQuote cols) ", " <> ")\""
opRender (OpDerive bs) =
  "derive {" <> joinWith (V.map (\(n, e) -> quote n <> " = " <> e) bs) ", " <> "}"
opRender (OpGroup keys aggs) =
  let as = V.map (\(fn, name, col) -> name <> " = " <> aggName fn <> " " <> quote col) aggs
  in "group {" <> joinWith (V.map quote keys) ", " <> "} (aggregate {" <> joinWith as ", " <> "})"
opRender (OpTake n) = "take " <> T.pack (show n)

-- | Render just the ops portion (no base/from clause)
queryRenderOps :: Query -> Text
queryRenderOps q =
  if V.null (ops q)
    then ""
    else joinWith (V.map opRender (ops q)) " | "

-- | Render full query to PRQL string
queryRender :: Query -> Text
queryRender q =
  let os = queryRenderOps q
  in if T.null os then base q else base q <> " | " <> os

-- | Pipe: append operation to query
pipe :: Query -> Op -> Query
pipe q op = q { ops = V.snoc (ops q) op }

-- | Filter helper
queryFilter :: Query -> Text -> Query
queryFilter q expr = pipe q (OpFilter expr)

-- | Common PRQL query strings (avoid duplication across modules)
ducktabs :: Text
ducktabs = "from dtabs | tbl_info"

ducktabsF :: Text
ducktabsF = "from dtabs | tbl_info_filtered"
