{-# LANGUAGE OverloadedStrings #-}
-- | Pipeline-style query builder that renders to PRQL.
--
-- The shape mirrors dataframe's lazy 'LogicalPlan' — a base table plus
-- a sequence of stages — but each stage is a piece of PRQL text
-- (rendered from 'Tv.Df.Expr.Expr' values) rather than a typed plan
-- node. We keep things pre-rendered because the existing DuckDB path
-- already consumes PRQL strings; no need for another IR.
--
-- Usage:
--
-- > q |> Q.groupAgg ["a"] [("Cnt", E.stdCount E.this)]
-- >   |> Q.derive   [("Pct", E.col "Cnt" E..* E.lit 100)]
-- >   |> Q.sortDesc ["Cnt"]
-- >   |> Q.take_ 1000
-- >   |> Q.compile  -- :: Text (PRQL)
module Tv.Df.Query
  ( -- * Query
    Query
  , fromBase
  , compile
    -- * Stages
  , filter_
  , derive
  , groupAgg
  , sortAsc
  , sortDesc
  , sortBy
  , take_
  , select
  , exclude
    -- * Utilities
  , (|>)
  ) where

import Data.Function ((&))
import Data.Text (Text)
import qualified Data.Text as T

import qualified Tv.Df.Expr as E

-- | A PRQL pipeline: base table + a list of rendered stages.
-- Stages are kept as text so we can emit directly; structure is
-- preserved by the builder API, not by this record.
data Query = Query
  { qBase   :: Text     -- "from t"  (PRQL base)
  , qStages :: [Text]   -- each stage body without the leading "| "
  }
  deriving (Eq, Show)

-- | Start a pipeline from an existing PRQL @from@ clause (the exact
-- text that would follow the @from@ keyword is what tv's existing
-- @Prql.Query.base@ holds).
fromBase :: Text -> Query
fromBase b = Query { qBase = b, qStages = [] }

-- | Emit the full PRQL text.
compile :: Query -> Text
compile q = T.intercalate " | " (qBase q : qStages q)

-- | Pipe forward operator. Re-exported so callers don't import
-- 'Data.Function' separately.
(|>) :: a -> (a -> b) -> b
(|>) = (&)
infixl 1 |>

-- ----------------------------------------------------------------------------
-- Stages
-- ----------------------------------------------------------------------------

-- | @filter expr@.
filter_ :: E.Expr -> Query -> Query
filter_ e q = q { qStages = qStages q ++ ["filter " <> E.render e] }

-- | @derive { n1 = e1, n2 = e2, … }@.
derive :: [(Text, E.Expr)] -> Query -> Query
derive bs q = q { qStages = qStages q ++ [stage] }
  where
    stage = "derive {" <> T.intercalate ", " (map one bs) <> "}"
    one (n, e) = quote n <> " = " <> E.render e

-- | @group { keys } (aggregate { n1 = agg1, n2 = agg2, … })@. Aggregate
-- expressions go inline — use 'E.stdCount', 'E.stdSum', etc.
groupAgg :: [Text] -> [(Text, E.Expr)] -> Query -> Query
groupAgg keys aggs q = q { qStages = qStages q ++ [stage] }
  where
    keysT = T.intercalate ", " (map quote keys)
    aggsT = T.intercalate ", " (map one aggs)
    stage = "group {" <> keysT <> "} (aggregate {" <> aggsT <> "})"
    one (n, e) = quote n <> " = " <> E.render e

-- | @sort { col1, col2 }@ — ascending.
sortAsc :: [Text] -> Query -> Query
sortAsc cs = sortBy [(c, True) | c <- cs]

-- | @sort { -col1, -col2 }@ — descending.
sortDesc :: [Text] -> Query -> Query
sortDesc cs = sortBy [(c, False) | c <- cs]

-- | @sort { col | -col }@ with per-column direction. True = ascending.
sortBy :: [(Text, Bool)] -> Query -> Query
sortBy cs q = q { qStages = qStages q ++ [stage] }
  where
    stage = "sort {" <> T.intercalate ", " (map one cs) <> "}"
    one (c, True)  = quote c
    one (c, False) = "-" <> quote c

-- | @take N@.
take_ :: Int -> Query -> Query
take_ n q = q { qStages = qStages q ++ ["take " <> T.pack (show n)] }

-- | @select { a, b, c }@.
select :: [Text] -> Query -> Query
select cs q = q { qStages = qStages q ++ ["select {" <> T.intercalate ", " (map quote cs) <> "}"] }

-- | @select s"* EXCLUDE (…)"@ — PRQL's s-string fall-back.
exclude :: [Text] -> Query -> Query
exclude cs q = q { qStages = qStages q ++ [stage] }
  where
    stage = "select s\"* EXCLUDE (" <> T.intercalate ", " (map dq cs) <> ")\""
    dq c = "\\\"" <> c <> "\\\""

-- PRQL column-name quoting — backtick for safety across reserved words.
quote :: Text -> Text
quote c = "`" <> c <> "`"
