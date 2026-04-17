{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
-- | Translate dataframe's typed 'Expr' GADT to PRQL text, and build
-- PRQL pipelines from typed stages.
--
-- The dataframe package already designed a nice typed expression DSL.
-- Instead of reinventing it we adopt it, and emit PRQL so DuckDB
-- executes the plan. The same 'Expr' values can be interpreted
-- directly by dataframe for parity testing — see 'TestDf'.
module Tv.Df.Prql
  ( -- * Expression rendering
    renderExpr
  , renderUExpr
  , this
    -- * Pipeline
  , Query (..)
  , Stage (..)
  , SortDir (..)
  , JoinKind (..)
  , compile
  , fromBase
  , (|>)
    -- * Stage smart constructors
  , filter_
  , derive
  , aggregate
  , groupAgg
  , sortAsc
  , sortDesc
  , sortBy
  , take_
  , select
  , exclude
  , distinct
  , append
  , join_
  , window
  , rawStage
  ) where

import Data.Function ((&))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (Refl))
import Type.Reflection (typeRep)

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression
  ( AggStrategy (..)
  , BinaryOp (..)
  , Expr (..)
  , NamedExpr
  , UExpr (..)
  , UnaryOp (..)
  )

-- ============================================================================
-- Expression → PRQL
-- ============================================================================

-- | PRQL's @this@ keyword: refers to the current row as a tuple.
-- Used inside aggregates (@std.count this@) to count rows. Typed as
-- Int so it composes with counting; dataframe's eager engine won't
-- interpret this, but the renderer handles it specially.
this :: Expr Int
this = Col "this"

-- | Render a dataframe 'Expr' as PRQL expression text. Shares the
-- precedence structure with 'DataFrame.Internal.Expression.prettyPrint'
-- but uses PRQL operator tokens.
renderExpr :: forall a. Expr a -> Text
renderExpr = go 0
  where
    go :: Int -> Expr b -> Text
    go _    (Col "this")       = "this"    -- PRQL keyword, never backtick
    go _    (Col name)         = quoteCol name
    go _    (Lit v)            = renderLit v
    go _    (CastWith name _ _) = "/* cast " <> name <> " not supported */"
    go p    (CastExprWith _ _ e) = go p e     -- drop the cast; PRQL doesn't expose it
    go p    (Unary op arg)     = renderUnary p op arg
    go p    (Binary op l r)    = renderBinary p op l r
    go _    (Agg strat arg)    = renderAgg strat arg
    go p    (If c t e)         =
      let body = "(if " <> go 0 c <> " then " <> go 0 t <> " else " <> go 0 e <> ")"
      in if p > 0 then "(" <> body <> ")" else body

renderUExpr :: UExpr -> Text
renderUExpr (UExpr e) = renderExpr e

-- | Pretty-print a binary op inside precedence @p@.
renderBinary :: Int -> BinaryOp c b a -> Expr c -> Expr b -> Text
renderBinary p op l r =
  let pr    = binaryPrecedence op
      tok   = binaryPrql (binaryName op)
      inner = renderExpr' pr l <> " " <> tok <> " " <> renderExpr' (pr + 1) r
  in if pr < p then "(" <> inner <> ")" else inner
  where
    renderExpr' :: Int -> Expr b' -> Text
    renderExpr' n e = case e of
      Lit v -> renderLit v
      Col c -> quoteCol c
      _     -> renderExpr e `withPrec` n
    withPrec t _ = t  -- precedence already handled by nested renderExpr

-- Map dataframe's binaryName to a PRQL operator token.
binaryPrql :: Text -> Text
binaryPrql = \case
  "add"          -> "+"
  "sub"          -> "-"
  "mult"         -> "*"
  "divide"       -> "/"
  "eq"           -> "=="
  "neq"          -> "!="
  "lt"           -> "<"
  "leq"          -> "<="
  "gt"           -> ">"
  "geq"          -> ">="
  "and"          -> "&&"
  "or"           -> "||"
  -- Nullable-aware names have a different internal spelling; reuse the
  -- same PRQL tokens (PRQL does 3-valued logic at the SQL layer).
  "nulladd"      -> "+"
  "nullsub"      -> "-"
  "nullmul"      -> "*"
  "nulldiv"      -> "/"
  "nulland"      -> "&&"
  "nullor"       -> "||"
  other          -> other

renderUnary :: Int -> UnaryOp b a -> Expr b -> Text
renderUnary _ op arg =
  let x = renderExpr arg
      call1 fn = fn <> " " <> x
  in case unaryName op of
    "negate" -> "-(" <> x <> ")"
    -- PRQL's math stdlib: math.abs, math.sqrt, math.exp, etc.
    "abs"    -> call1 "math.abs"
    "signum" -> call1 "math.sign"
    "exp"    -> call1 "math.exp"
    "sqrt"   -> call1 "math.sqrt"
    "log"    -> call1 "math.ln"      -- Haskell `log` = natural log
    "sin"    -> call1 "math.sin"
    "cos"    -> call1 "math.cos"
    "tan"    -> call1 "math.tan"
    "asin"   -> call1 "math.asin"
    "acos"   -> call1 "math.acos"
    "atan"   -> call1 "math.atan"
    "sinh"   -> call1 "math.sinh"
    "cosh"   -> call1 "math.cosh"
    "asinh"  -> call1 "math.asinh"
    "acosh"  -> call1 "math.acosh"
    "atanh"  -> call1 "math.atanh"
    "floor"  -> call1 "math.floor"
    "ceil"   -> call1 "math.ceil"
    "round"  -> call1 "math.round"
    other    -> "/* unary " <> other <> " unsupported */"

renderAgg :: AggStrategy a b -> Expr b -> Text
renderAgg strat arg =
  let name = case strat of
        CollectAgg n _   -> n
        FoldAgg    n _ _ -> n
        MergeAgg   n _ _ _ _ -> n
      prql = aggPrql name
  in prql <> " " <> renderExpr arg

-- Map dataframe agg names to PRQL @std.*@ function names.
aggPrql :: Text -> Text
aggPrql = \case
  "count" -> "std.count"
  "sum"   -> "std.sum"
  "mean"  -> "std.average"
  "min"   -> "std.min"
  "max"   -> "std.max"
  other   -> "std." <> other

-- ----------------------------------------------------------------------------
-- Literals: dispatch on the column-element type via Typeable.
-- ----------------------------------------------------------------------------

renderLit :: forall a. Columnable a => a -> Text
renderLit v
  | Just Refl <- testEquality (typeRep @a) (typeRep @Int)     = T.pack (show v)
  | Just Refl <- testEquality (typeRep @a) (typeRep @Integer) = T.pack (show v)
  | Just Refl <- testEquality (typeRep @a) (typeRep @Double)  = T.pack (show v)
  | Just Refl <- testEquality (typeRep @a) (typeRep @Float)   = T.pack (show v)
  | Just Refl <- testEquality (typeRep @a) (typeRep @Text)    = "'" <> escText v <> "'"
  | Just Refl <- testEquality (typeRep @a) (typeRep @Bool)    = if v then "true" else "false"
  | Just Refl <- testEquality (typeRep @a) (typeRep @String)  = "'" <> escText (T.pack v) <> "'"
  | otherwise                                                  = T.pack (show v)

escText :: Text -> Text
escText = T.replace "'" "\\'"

quoteCol :: Text -> Text
quoteCol c = "`" <> c <> "`"

-- ============================================================================
-- Pipeline / Query
-- ============================================================================

data SortDir = Asc | Desc deriving (Eq, Show)

data JoinKind = JInner | JLeft | JRight | JFull deriving (Eq, Show)

data Stage
  = StFilter    (Expr Bool)
  | StDerive    [NamedExpr]
  | StAggregate [NamedExpr]   -- scalar aggregate (one row out, no group keys)
  | StGroupAgg  [Text] [NamedExpr]
  | StSort     [(Text, SortDir)]
  | StTake     Int
  | StSelect   [Text]
  | StExclude  [Text]
  | StDistinct [Text]    -- unique on columns; empty = distinct on all
  | StAppend   Text      -- base table name to union
  | StJoin     JoinKind Text (Expr Bool)   -- other-table base + predicate
  | StWindow   [Stage]   -- stages executed in a PRQL `window` context
  | StRaw      Text  -- literal PRQL stage text, for idioms dataframe's Expr can't encode

data Query = Query
  { qBase   :: Text     -- "from t" body (text following the `from` keyword)
  , qStages :: [Stage]
  }

fromBase :: Text -> Query
fromBase b = Query { qBase = b, qStages = [] }

compile :: Query -> Text
compile q = T.intercalate " | " (qBase q : map renderStage (qStages q))

renderStage :: Stage -> Text
renderStage = \case
  StFilter e      -> "filter " <> renderExpr e
  StDerive bs     -> "derive {" <> commaMap renderBind bs <> "}"
  StAggregate bs  -> "aggregate {" <> commaMap renderBind bs <> "}"
  StGroupAgg ks aggs ->
    "group {" <> commaMap quoteCol ks <> "} " <>
    "(aggregate {" <> commaMap renderBind aggs <> "})"
  StSort cs       -> "sort {" <> commaMap renderSort cs <> "}"
  StTake n        -> "take " <> T.pack (show n)
  StSelect cs     -> "select {" <> commaMap quoteCol cs <> "}"
  StExclude cs    -> "select s\"* EXCLUDE (" <> commaMap dqCol cs <> ")\""
  StDistinct cs   ->
    let ks = if null cs then "this" else commaMap quoteCol cs
    in "group {" <> ks <> "} (take 1)"
  StAppend tbl    -> "append " <> tbl
  StJoin k tbl p  -> "join side:" <> joinKindText k <> " " <> tbl <> " (" <> renderExpr p <> ")"
  StWindow stages -> "window (" <> T.intercalate " | " (map renderStage stages) <> ")"
  StRaw t         -> t
  where
    renderBind (n, ue) = quoteCol n <> " = " <> renderUExpr ue
    renderSort (c, Asc)  = quoteCol c
    renderSort (c, Desc) = "-" <> quoteCol c
    commaMap f = T.intercalate ", " . map f
    dqCol c = "\\\"" <> c <> "\\\""
    joinKindText JInner = "inner"
    joinKindText JLeft  = "left"
    joinKindText JRight = "right"
    joinKindText JFull  = "full"

-- | Pipe forward. Imported from 'Data.Function' under a friendly name.
(|>) :: a -> (a -> b) -> b
(|>) = (&)
infixl 1 |>

-- ----------------------------------------------------------------------------
-- Stage smart constructors — match the dataframe-side verb names.
-- ----------------------------------------------------------------------------

filter_ :: Expr Bool -> Query -> Query
filter_ e q = q { qStages = qStages q ++ [StFilter e] }

derive :: [NamedExpr] -> Query -> Query
derive bs q = q { qStages = qStages q ++ [StDerive bs] }

-- | Scalar aggregate: @aggregate { … }@ with no group keys. Produces
-- a single output row with the listed bindings.
aggregate :: [NamedExpr] -> Query -> Query
aggregate bs q = q { qStages = qStages q ++ [StAggregate bs] }

groupAgg :: [Text] -> [NamedExpr] -> Query -> Query
groupAgg ks aggs q = q { qStages = qStages q ++ [StGroupAgg ks aggs] }

sortAsc :: [Text] -> Query -> Query
sortAsc cs = sortBy [(c, Asc) | c <- cs]

sortDesc :: [Text] -> Query -> Query
sortDesc cs = sortBy [(c, Desc) | c <- cs]

sortBy :: [(Text, SortDir)] -> Query -> Query
sortBy cs q = q { qStages = qStages q ++ [StSort cs] }

take_ :: Int -> Query -> Query
take_ n q = q { qStages = qStages q ++ [StTake n] }

select :: [Text] -> Query -> Query
select cs q = q { qStages = qStages q ++ [StSelect cs] }

-- | Drop columns. Emits a PRQL s-string around DuckDB's @* EXCLUDE@.
exclude :: [Text] -> Query -> Query
exclude cs q = q { qStages = qStages q ++ [StExclude cs] }

-- | Deduplicate. @[]@ means distinct on all columns; a non-empty list
-- keeps one row per combination of the given columns.
distinct :: [Text] -> Query -> Query
distinct cs q = q { qStages = qStages q ++ [StDistinct cs] }

-- | Concatenate another table (union) by its PRQL base name.
append :: Text -> Query -> Query
append tbl q = q { qStages = qStages q ++ [StAppend tbl] }

-- | Join another table with a predicate expression. The other table's
-- base is passed as PRQL text (usually just a table identifier).
join_ :: JoinKind -> Text -> Expr Bool -> Query -> Query
join_ k tbl pr q = q { qStages = qStages q ++ [StJoin k tbl pr] }

-- | Wrap stages in PRQL's @window@ context — the wrapped stages see
-- the outer frame as their grouping, so @std.sum@ inside window derive
-- acts as a window function. Used to compute percentages over totals.
window :: [Stage] -> Query -> Query
window stages q = q { qStages = qStages q ++ [StWindow stages] }

-- | Append a raw PRQL stage. Escape hatch for idioms the typed stages
-- can't express (rare remaining cases — see the architecture doc).
rawStage :: Text -> Query -> Query
rawStage t q = q { qStages = qStages q ++ [StRaw t] }
