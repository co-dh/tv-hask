{-# LANGUAGE OverloadedStrings #-}
-- | Expression DSL that renders to PRQL text.
--
-- This is the programmatic half of tv's q-like API. 'Tv.Df.Parse' parses
-- *user-typed* expressions into the same shape; this module builds them
-- through combinators so feature modules can compose predicates and
-- derived-column formulae in Haskell.
--
-- Rendering target is PRQL, which prqlc compiles to SQL for DuckDB.
-- That keeps the DuckDB compute path intact while giving us a typed,
-- composable front end instead of raw-text PRQL concatenation.
module Tv.Df.Expr
  ( Expr (..)
  , Op (..)
  , UOp (..)
    -- * Building
  , col
  , lit
  , litInt
  , litDbl
  , litText
  , raw
    -- * Operators
  , (.==), (.!=), (.<), (.<=), (.>), (.>=)
  , (.&&), (.||), neg, not_
  , (.+), (.-), (.*), (./), (.%)
    -- * Aggregates / function calls
  , stdCount, stdSum, stdAvg, stdMin, stdMax, stdCntDist
  , call, this
    -- * Rendering
  , render
  ) where

import Data.Text (Text)
import qualified Data.Text as T

-- | Untyped expression tree. Typing would make the DSL more
-- error-proof but the PRQL back end doesn't yet need it; keeping
-- things in one shape lets the parser (Tv.Df.Parse) and the
-- programmatic builder share the same AST.
data Expr
  = ECol  Text
  | ELitI Integer
  | ELitD Double
  | ELitT Text
  | EBin  Op  Expr Expr
  | EUn   UOp Expr
  | ECall Text [Expr]   -- std.count, std.sum, repeat('#', …), …
  | ERaw  Text          -- escape hatch: raw PRQL fragment
  deriving (Eq, Show)

data Op
  = OEq | ONe | OLt | OLe | OGt | OGe
  | OAdd | OSub | OMul | ODiv | OMod
  | OAnd | OOr
  deriving (Eq, Show)

data UOp = UNeg | UNot deriving (Eq, Show)

-- ----------------------------------------------------------------------------
-- Constructors
-- ----------------------------------------------------------------------------

col :: Text -> Expr
col = ECol

lit :: Integer -> Expr
lit = ELitI

litInt :: Integer -> Expr
litInt = ELitI

litDbl :: Double -> Expr
litDbl = ELitD

litText :: Text -> Expr
litText = ELitT

raw :: Text -> Expr
raw = ERaw

-- PRQL's "this" refers to the current row as a tuple — used in std.count this
-- to count rows regardless of null values.
this :: Expr
this = ECol "this"

-- ----------------------------------------------------------------------------
-- Operators (dot-prefixed to avoid clashing with Prelude)
-- ----------------------------------------------------------------------------

infix 4 .==, .!=, .<, .<=, .>, .>=
infixl 6 .+, .-
infixl 7 .*, ./, .%
infixr 3 .&&
infixr 2 .||

(.==), (.!=), (.<), (.<=), (.>), (.>=) :: Expr -> Expr -> Expr
(.==) = EBin OEq
(.!=) = EBin ONe
(.<)  = EBin OLt
(.<=) = EBin OLe
(.>)  = EBin OGt
(.>=) = EBin OGe

(.+), (.-), (.*), (./), (.%) :: Expr -> Expr -> Expr
(.+) = EBin OAdd
(.-) = EBin OSub
(.*) = EBin OMul
(./) = EBin ODiv
(.%) = EBin OMod

(.&&), (.||) :: Expr -> Expr -> Expr
(.&&) = EBin OAnd
(.||) = EBin OOr

neg :: Expr -> Expr
neg = EUn UNeg

not_ :: Expr -> Expr
not_ = EUn UNot

-- ----------------------------------------------------------------------------
-- Function calls (PRQL std.* family + generic call)
-- ----------------------------------------------------------------------------

call :: Text -> [Expr] -> Expr
call = ECall

stdCount, stdSum, stdAvg, stdMin, stdMax, stdCntDist :: Expr -> Expr
stdCount e = ECall "std.count" [e]
stdSum   e = ECall "std.sum"   [e]
stdAvg   e = ECall "std.average" [e]
stdMin   e = ECall "std.min"   [e]
stdMax   e = ECall "std.max"   [e]
stdCntDist e = ECall "std.count_distinct" [e]

-- ----------------------------------------------------------------------------
-- Rendering to PRQL text
-- ----------------------------------------------------------------------------

-- | Emit a PRQL expression. Adds parentheses around sub-expressions
-- whose operator precedence is lower than the parent's. PRQL's own
-- precedence rules broadly match the dot-operator precedences above.
render :: Expr -> Text
render = go (-1)
  where
    go _    (ECol c)      = quote c
    go _    (ELitI n)     = T.pack (show n)
    go _    (ELitD d)     = T.pack (show d)
    go _    (ELitT t)     = "'" <> escText t <> "'"
    go _    (ERaw t)      = t
    go p    (EUn UNeg e)  = paren p 10 ("-" <> go 10 e)
    go p    (EUn UNot e)  = paren p 10 ("!" <> go 10 e)
    go _    (ECall fn []) = fn
    go _    (ECall fn as) = fn <> " " <> T.intercalate " " (map (go 11) as)
    go p    (EBin op l r) =
      let pr = prec op
      in paren p pr (go pr l <> " " <> opText op <> " " <> go (pr + 1) r)

    paren p pr body = if pr < p then "(" <> body <> ")" else body

-- Operator precedence; higher = binds tighter.
prec :: Op -> Int
prec OOr  = 2
prec OAnd = 3
prec OEq  = 4
prec ONe  = 4
prec OLt  = 4
prec OLe  = 4
prec OGt  = 4
prec OGe  = 4
prec OAdd = 6
prec OSub = 6
prec OMul = 7
prec ODiv = 7
prec OMod = 7

opText :: Op -> Text
opText OEq  = "=="
opText ONe  = "!="
opText OLt  = "<"
opText OLe  = "<="
opText OGt  = ">"
opText OGe  = ">="
opText OAdd = "+"
opText OSub = "-"
opText OMul = "*"
opText ODiv = "/"
opText OMod = "%"
opText OAnd = "&&"
opText OOr  = "||"

-- PRQL uses backticks for any column name, safe across reserved words.
quote :: Text -> Text
quote c
  | c == "this" = c    -- the `this` keyword must not be quoted
  | otherwise   = "`" <> c <> "`"

-- Minimal escaping for single-quoted PRQL string literals.
escText :: Text -> Text
escText = T.replace "'" "\\'"
