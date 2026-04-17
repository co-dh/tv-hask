{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-
  PRQL rendering and compilation
  Uses common Op types from Tv/Types.hs

  Pure PRQL query representation + renderer. This module also hosts
  `compile`, which shells out to the `prqlc` CLI — matching Lean's layering
  where `compile` lives in `Tc/Data/ADBC/Prql.lean`, not the FFI module.
-}
module Tv.Data.DuckDB.Prql
  ( -- * Query type
    Query(..)
  , defaultQuery
    -- * Quoting / rendering
  , quote
  , renderSort
  , aggName
  , dqQuote
  , opRender
  , renderOps
  , queryRender
    -- * Query operations
  , pipe
  , queryFilter
    -- * Common queries
  , ducktabs
  , ducktabsF
    -- * PRQL prelude
  , funcsBytes
  , funcs
    -- * Compilation
  , compile
  ) where

import Control.Exception (SomeException, try)
import Data.ByteString (ByteString)
import Data.FileEmbed (embedFile)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode
  )
import Tv.Types
  ( Agg (..)
  , Op (..)
  , joinWith
  )
import qualified Tv.Log as Log

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
renderOps :: Query -> Text
renderOps q =
  if V.null (ops q)
    then ""
    else joinWith (V.map opRender (ops q)) " | "

-- | Render full query to PRQL string
queryRender :: Query -> Text
queryRender q =
  let os = renderOps q
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

-- | PRQL function definitions, embedded at compile time. Mirrors Lean's
-- @include_str "funcs.prql"@.
funcsBytes :: ByteString
funcsBytes = $(embedFile "src/Tv/Data/DuckDB/funcs.prql")

funcs :: Text
funcs = TE.decodeUtf8 funcsBytes

-- | Compile PRQL to SQL by shelling out to the @prqlc@ CLI. Returns
-- @Nothing@ on compile failure (with stderr logged). The @funcs@ prelude
-- is prepended so user queries can call ds_trunc, freq, etc.
-- Uses readProcessWithExitCode which drains stdout/stderr concurrently,
-- avoiding deadlock when pipe buffers fill.
compile :: Text -> IO (Maybe Text)
compile prql = do
  let full = T.unpack (funcs <> "\n" <> prql)
  r <- try (readProcessWithExitCode "prqlc"
              ["compile", "--hide-signature-comment", "-t", "sql.duckdb"] full)
  case r of
    Left (e :: SomeException) -> do
      Log.errorLog (T.pack ("prqlc spawn failed: " <> show e))
      pure Nothing
    Right (ExitSuccess, out, _) -> pure $ Just $ T.pack out
    Right (_, _, err) -> do
      Log.errorLog ("prqlc: " <> T.pack err)
      pure Nothing
