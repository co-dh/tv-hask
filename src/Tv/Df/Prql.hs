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
    -- * Pipeline
  , Query (..)
  , Stage (..)
  , SortDir (..)
  , compile
  , fromBase
  , (|>)
    -- * Stage smart constructors
  , filter_
  , derive
  , groupAgg
  , sortAsc
  , sortDesc
  , sortBy
  , take_
  , select
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

-- | Render a dataframe 'Expr' as PRQL expression text. Shares the
-- precedence structure with 'DataFrame.Internal.Expression.prettyPrint'
-- but uses PRQL operator tokens.
renderExpr :: forall a. Expr a -> Text
renderExpr = go 0
  where
    go :: Int -> Expr b -> Text
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
renderUnary _ op arg = case unaryName op of
  "negate" -> "-(" <> renderExpr arg <> ")"
  "abs"    -> "s\"ABS(\" + " <> renderExpr arg <> " + \")\""
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

data Stage
  = StFilter   (Expr Bool)
  | StDerive   [NamedExpr]
  | StGroupAgg [Text] [NamedExpr]
  | StSort     [(Text, SortDir)]
  | StTake     Int
  | StSelect   [Text]
  | StRaw      Text  -- literal PRQL stage text, for idioms dataframe's Expr can't encode (e.g. window sums in derive)

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
  StGroupAgg ks aggs ->
    "group {" <> commaMap quoteCol ks <> "} " <>
    "(aggregate {" <> commaMap renderBind aggs <> "})"
  StSort cs       -> "sort {" <> commaMap renderSort cs <> "}"
  StTake n        -> "take " <> T.pack (show n)
  StSelect cs     -> "select {" <> commaMap quoteCol cs <> "}"
  StRaw t         -> t
  where
    renderBind (n, ue) = quoteCol n <> " = " <> renderUExpr ue
    renderSort (c, Asc)  = quoteCol c
    renderSort (c, Desc) = "-" <> quoteCol c
    commaMap f = T.intercalate ", " . map f

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

-- | Append a raw PRQL stage. Escape hatch for idioms the typed stages
-- can't express (e.g. window sums in derive, or s-string SQL escapes).
rawStage :: Text -> Query -> Query
rawStage t q = q { qStages = qStages q ++ [StRaw t] }
