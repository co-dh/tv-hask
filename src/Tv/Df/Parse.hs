{-# LANGUAGE OverloadedStrings #-}
-- | Parser for the PRQL expression subset tv-hask actually uses at
-- filter and derive prompts. Purely syntactic: produces an untyped
-- 'PExpr' AST. Type-checking and lowering to dataframe's typed 'Expr'
-- GADT happens in the feature-level modules that consume this
-- (Tv.Filter / Tv.Ops), where the 'DataFrame' schema is in scope.
--
-- Grammar (tightest acceptable superset of what tests + prior tv use):
--
-- @
--   expr    := orExpr
--   orExpr  := andExpr ('or'  andExpr)*
--   andExpr := cmpExpr ('and' cmpExpr)*
--   cmpExpr := addExpr (cmpOp addExpr)?
--   addExpr := mulExpr (('+' | '-') mulExpr)*
--   mulExpr := unary (('*' | '/' | '%') unary)*
--   unary   := '-' unary | '!' unary | atom
--   atom    := literal | column | '(' expr ')'
--   cmpOp   := '==' | '!=' | '<=' | '>=' | '<' | '>'
--   literal := int | double | single-quoted string | double-quoted string
--   column  := identifier   (Haskell-style: alpha|underscore then alnum|underscore|dot)
-- @
--
-- A derive binding is parsed separately: @name = expr@.
module Tv.Df.Parse
  ( PExpr (..)
  , BinOp (..)
  , UnOp (..)
  , Lit (..)
  , parseExpr
  , parseBinding
  ) where

import Data.Char (isAlpha, isAlphaNum, isDigit, isSpace)
import Data.Text (Text)
import qualified Data.Text as T

-- | Literals supported at the surface syntax. Numeric literals are
-- kept untyped so the elaborator can coerce to the column's type.
data Lit
  = LInt    Integer
  | LDouble Double
  | LText   Text
  deriving (Eq, Show)

data BinOp
  = Eq | Neq | Lt | Leq | Gt | Geq
  | Add | Sub | Mul | Div | Mod
  | And | Or
  deriving (Eq, Show)

data UnOp
  = Neg | Not
  deriving (Eq, Show)

-- | Untyped expression AST.
data PExpr
  = PLit  Lit
  | PCol  Text
  | PBin  BinOp PExpr PExpr
  | PUn   UnOp  PExpr
  deriving (Eq, Show)

-- | Parse a standalone expression (e.g. a filter predicate).
parseExpr :: Text -> Either String PExpr
parseExpr src = case runP pExpr (dropSpaces src) of
  Right (e, rest) | T.null (dropSpaces rest) -> Right e
                  | otherwise -> Left $ "trailing input: " <> T.unpack (dropSpaces rest)
  Left err -> Left err

-- | Parse a @name = expr@ binding (for derive). Whitespace around @=@ ignored.
parseBinding :: Text -> Either String (Text, PExpr)
parseBinding src = case runP (do n <- pIdent; pEq; e <- pExpr; pure (n, e)) (dropSpaces src) of
  Right ((n, e), rest) | T.null (dropSpaces rest) -> Right (n, e)
                       | otherwise -> Left $ "trailing input: " <> T.unpack (dropSpaces rest)
  Left err -> Left err

-- ----------------------------------------------------------------------------
-- Hand-rolled parser combinators (no megaparsec dep to keep the step small)
-- ----------------------------------------------------------------------------

-- | A parser consumes 'Text' and produces either an error or a
-- (result, remaining) pair. All primitives in this module expect the
-- remaining text to have no leading whitespace (callers use
-- 'dropSpaces' before/after tokens).
newtype P a = P { runP :: Text -> Either String (a, Text) }

instance Functor P where
  fmap f (P p) = P (fmap (\(x, r) -> (f x, r)) . p)

instance Applicative P where
  pure x = P (\s -> Right (x, s))
  P pf <*> P pa = P $ \s -> do
    (f, s')  <- pf s
    (a, s'') <- pa s'
    pure (f a, s'')

instance Monad P where
  return = pure
  P p >>= k = P $ \s -> do
    (a, s') <- p s
    runP (k a) s'

dropSpaces :: Text -> Text
dropSpaces = T.dropWhile isSpace

-- | Succeed if the input starts with @tok@ (after dropping whitespace).
lit_ :: Text -> P ()
lit_ tok = P $ \s0 ->
  let s = dropSpaces s0
  in if tok `T.isPrefixOf` s
       then Right ((), T.drop (T.length tok) s)
       else Left $ "expected " <> show tok <> " at " <> previewT s

-- | First parser that succeeds wins. Standard backtracking combinator.
orElse :: P a -> P a -> P a
orElse (P p) (P q) = P $ \s -> case p s of
  Right r -> Right r
  Left _  -> q s

previewT :: Text -> String
previewT s = show (T.take 20 s)

-- ----------------------------------------------------------------------------
-- Grammar
-- ----------------------------------------------------------------------------

pExpr :: P PExpr
pExpr = pOr

pOr :: P PExpr
pOr = do
  l <- pAnd
  let step acc = orElse (do pKeyword "or"; r <- pAnd; step (PBin Or acc r)) (pure acc)
  step l

pAnd :: P PExpr
pAnd = do
  l <- pCmp
  let step acc = orElse (do pKeyword "and"; r <- pCmp; step (PBin And acc r)) (pure acc)
  step l

pCmp :: P PExpr
pCmp = do
  l <- pAdd
  orElse
    (do op <- pCmpOp
        r  <- pAdd
        pure (PBin op l r))
    (pure l)

pAdd :: P PExpr
pAdd = do
  l <- pMul
  let step acc = orElse
        (do op <- pAddOp; r <- pMul; step (PBin op acc r))
        (pure acc)
  step l

pMul :: P PExpr
pMul = do
  l <- pUnary
  let step acc = orElse
        (do op <- pMulOp; r <- pUnary; step (PBin op acc r))
        (pure acc)
  step l

pUnary :: P PExpr
pUnary = orElse
  (do lit_ "-"; PUn Neg <$> pUnary)
  (orElse (do lit_ "!"; PUn Not <$> pUnary) pAtom)

pAtom :: P PExpr
pAtom = orElse pParens (orElse pLiteral pColRef)

pParens :: P PExpr
pParens = do lit_ "("; e <- pExpr; lit_ ")"; pure e

pLiteral :: P PExpr
pLiteral = orElse (fmap PLit pStringLit) (fmap PLit pNumLit)

pStringLit :: P Lit
pStringLit = P $ \s0 ->
  let s = dropSpaces s0
  in case T.uncons s of
    Just (q, rest) | q == '\'' || q == '"' ->
      let (body, remainder) = T.break (== q) rest
      in case T.uncons remainder of
        Just (_, after) -> Right (LText body, after)
        Nothing         -> Left $ "unterminated string literal at " <> previewT s
    _ -> Left $ "expected string literal at " <> previewT s

pNumLit :: P Lit
pNumLit = P $ \s0 ->
  let s = dropSpaces s0
      (digits, s1) = T.span isDigit s
  in if T.null digits
       then Left $ "expected numeric literal at " <> previewT s
       else case T.uncons s1 of
         Just ('.', after) ->
           let (frac, s2) = T.span isDigit after
           in if T.null frac
                then Left $ "expected digits after '.' at " <> previewT s1
                else Right (LDouble (read (T.unpack digits <> "." <> T.unpack frac)), s2)
         _ -> Right (LInt (read (T.unpack digits)), s1)

pColRef :: P PExpr
pColRef = fmap PCol pIdent

pIdent :: P Text
pIdent = P $ \s0 ->
  let s = dropSpaces s0
  in case T.uncons s of
    Just (c, rest) | isAlpha c || c == '_' ->
      let (tail_, s') = T.span (\x -> isAlphaNum x || x == '_' || x == '.') rest
      in Right (T.cons c tail_, s')
    _ -> Left $ "expected identifier at " <> previewT s

pKeyword :: Text -> P ()
pKeyword kw = P $ \s0 ->
  let s = dropSpaces s0
  in case T.stripPrefix kw s of
    Just rest | not (startsIdent rest) -> Right ((), rest)
    _ -> Left $ "expected keyword " <> show kw <> " at " <> previewT s
  where
    startsIdent t = case T.uncons t of
      Just (c, _) -> isAlphaNum c || c == '_'
      Nothing     -> False

pCmpOp :: P BinOp
pCmpOp = pToks [("==", Eq), ("!=", Neq), ("<=", Leq), (">=", Geq), ("<", Lt), (">", Gt)]
           "expected comparison op"

pAddOp :: P BinOp
pAddOp = pToks [("+", Add), ("-", Sub)] "expected + or -"

pMulOp :: P BinOp
pMulOp = pToks [("*", Mul), ("/", Div), ("%", Mod)] "expected *, /, or %"

-- | Try tokens in order; the first prefix match wins. Longer tokens
-- must come first in the list so e.g. "==" beats "=".
pToks :: [(Text, a)] -> String -> P a
pToks pairs msg = P $ \s0 ->
  let s = dropSpaces s0
      go []              = Left (msg <> " at " <> previewT s)
      go ((tok, v) : xs) = case T.stripPrefix tok s of
        Just rest -> Right (v, rest)
        Nothing   -> go xs
  in go pairs

pEq :: P ()
pEq = P $ \s0 ->
  let s = dropSpaces s0
  in case T.stripPrefix "=" s of
    Just rest | not (T.isPrefixOf "=" rest) -> Right ((), rest)
    _ -> Left $ "expected '=' (not '==') at " <> previewT s

