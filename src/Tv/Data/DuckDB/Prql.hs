{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-
  PRQL rendering and compilation
  Uses common Op types from Tv/Types.hs

  Pure PRQL query representation + renderer. This module also hosts
  `compile`, which shells out to the `prqlc` CLI — matching Lean's layering
  where `compile` lives in `Tc/Data/ADBC/Prql.lean`, not the FFI module.
-}
module Tv.Data.DuckDB.Prql where

import Tv.Prelude
import Control.Exception (SomeException, try)
import Data.FileEmbed (embedFile)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Tv.Data.DuckDB.PrqlC as PrqlC
import Tv.Types
  ( Agg (..)
  , Op (..)
  , joinWith
  )
import qualified Tv.Log as Log
import Optics.TH (makeFieldLabelsNoPrefix)

-- | PRQL query: base table + operations (PRQL-specific base format)
data Query = Query
  { base :: Text      -- PRQL from clause
  , ops  :: Vector Op
  }
makeFieldLabelsNoPrefix ''Query

-- | Default query: "from df" with no ops
defaultQuery :: Query
defaultQuery = Query { base = "from df", ops = V.empty }

-- | Quote a column *declaration* (LHS of a derive binding, new name in
-- a group alias, etc.). Backticks let PRQL accept arbitrary identifiers
-- without tripping its tokenizer.
quote :: Text -> Text
quote s = "`" <> s <> "`"

-- | Reference an *existing* column. Uses @this.`col`@ so column names
-- that collide with PRQL keywords or module names ('date', 'text',
-- 'math', 'time', 'std', 'case') resolve to the row column instead of
-- the builtin. Always use 'ref' where PRQL expects a column expression
-- (sort, select, group keys, aggregate source, join conditions);
-- use 'quote' only when introducing a *new* name.
ref :: Text -> Text
ref s = "this." <> quote s

-- | Render sort column (asc = col, desc = -col)
renderSort :: Text -> Bool -> Text
renderSort col asc =
  let qc = ref col
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

-- | Render single operation to PRQL string.
--
-- Column *references* (sort, select, group keys, aggregate sources) use
-- 'ref' so keyword-colliding names ('date', 'text', 'math', …) resolve
-- to row columns. *Declarations* (derive LHS, group alias) stay plain
-- 'quote' — PRQL doesn't accept @this.x@ on the LHS of an assignment.
opRender :: Op -> Text
opRender (OpFilter e) = "filter " <> e
opRender (OpSort cols) =
  "sort {" <> joinWith (V.map (\(c, asc) -> renderSort c asc) cols) ", " <> "}"
opRender (OpSel cols) =
  "select {" <> joinWith (V.map ref cols) ", " <> "}"
opRender (OpExclude cols) =
  "select s\"* EXCLUDE (" <> joinWith (V.map dqQuote cols) ", " <> ")\""
opRender (OpDerive bs) =
  "derive {" <> joinWith (V.map (\(n, e) -> quote n <> " = " <> e) bs) ", " <> "}"
opRender (OpGroup keys aggs) =
  let as = V.map (\(fn, name, col) -> name <> " = " <> aggName fn <> " " <> ref col) aggs
  in "group {" <> joinWith (V.map ref keys) ", " <> "} (aggregate {" <> joinWith as ", " <> "})"
opRender (OpTake n) = "take " <> T.pack (show n)

-- | Render just the ops portion (no base/from clause)
renderOps :: Query -> Text
renderOps Query{ops} =
  if V.null ops
    then ""
    else joinWith (V.map opRender ops) " | "

-- | Render full query to PRQL string
queryRender :: Query -> Text
queryRender q@Query{base} =
  let os = renderOps q
  in if T.null os then base else base <> " | " <> os

-- | Pipe: append operation to query
pipe :: Query -> Op -> Query
pipe q@Query{ops} op = q { ops = V.snoc ops op }

-- | Filter helper
queryFilter :: Query -> Text -> Query
queryFilter q expr = pipe q (OpFilter expr)

-- | Common PRQL query strings (avoid duplication across modules)
ducktabs :: Text
ducktabs = "from dtabs | tbl_info"

ducktabsF :: Text
ducktabsF = "from dtabs | tbl_info_filtered"

-- | PRQL function definitions, embedded at compile time. Mirrors Lean's
-- @include_str "funcs.prql"@.
funcsBytes :: ByteString
funcsBytes = $(embedFile "src/Tv/Data/DuckDB/funcs.prql")

funcs :: Text
funcs = TE.decodeUtf8 funcsBytes

-- | Compile PRQL to SQL via static-linked libprqlc_c. The @funcs@ prelude
-- is prepended so user queries can call ds_trunc, freq, etc.
compile :: Text -> IO (Maybe Text)
compile prql = do
  let full = funcs <> "\n" <> prql
  r <- try (PrqlC.compileFFI full "sql.duckdb")
  case r of
    Left (e :: SomeException) -> do
      Log.errorLog (T.pack ("prqlc FFI failed: " <> show e))
      pure Nothing
    Right out -> pure out
