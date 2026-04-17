{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
-- | Elaborate 'Tv.Df.Parse.PExpr' against a 'DataFrame's schema into the
-- typed expressions dataframe's @filterWhere@ and @derive@ consume.
--
-- The scope is deliberately narrow — covers what the filter/derive
-- prompts produce in tv-hask today:
--
--   * comparisons: @col cmpOp literal@ (either side literal)
--   * logical: @and@, @or@ compose filter predicates
--   * arithmetic for derive: @col + literal@, @col * literal@, …
--     (numeric; Int or Double, inferred from the column)
--   * bare @col@ or @literal@ projections in derive
--
-- Numeric literals coerce to the column's type (Int or Double). Text
-- literals stay Text. Unsupported forms return a 'Left' with a hint;
-- the caller (Tv.Filter / Tv.Ops) can fall back to the DuckDB path.
module Tv.Df.Elab
  ( elabFilter
  , elabDerive
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (Refl))
import Type.Reflection (typeRep)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Operators ((.==.), (./=.), (.<.), (.<=.), (.>.), (.>=.), (.&&.), (.||.))
import DataFrame.Internal.Column (Column (..), Columnable)
import DataFrame.Internal.DataFrame (unsafeGetColumn, columnIndices)
import DataFrame.Internal.Expression (Expr (..), NamedExpr, UExpr (..))

import qualified Data.Map.Strict as M

import Tv.Df.Parse (BinOp (..), Lit (..), PExpr (..), UnOp (..))

-- ----------------------------------------------------------------------------
-- Filter
-- ----------------------------------------------------------------------------

elabFilter :: D.DataFrame -> PExpr -> Either String (Expr Bool)
elabFilter df = go
  where
    go (PBin And l r) = (.&&.) <$> go l <*> go r
    go (PBin Or  l r) = (.||.) <$> go l <*> go r
    go (PUn  Not e)   = do
      _ <- go e
      Left "`not` is not yet supported in df filter"
    go (PBin op l r)
      | op `elem` [Eq, Neq, Lt, Leq, Gt, Geq] = elabCmp df op l r
    go other = Left $ "unsupported filter structure: " <> show other

-- | @col OP literal@ or @literal OP col@. We normalize to col-on-left by
-- flipping the operator when needed.
elabCmp :: D.DataFrame -> BinOp -> PExpr -> PExpr -> Either String (Expr Bool)
elabCmp df op (PCol c) (PLit lit) = cmpColLit df op c lit
elabCmp df op (PLit lit) (PCol c) = cmpColLit df (flipCmp op) c lit
elabCmp _ _ l r =
  Left $ "comparison must be col <op> literal (got " <> show l <> ", " <> show r <> ")"

flipCmp :: BinOp -> BinOp
flipCmp Lt  = Gt
flipCmp Gt  = Lt
flipCmp Leq = Geq
flipCmp Geq = Leq
flipCmp op  = op  -- Eq, Neq are commutative

-- | Dispatch on the column's element type, coerce the literal, build
-- the typed comparison.
cmpColLit :: D.DataFrame -> BinOp -> Text -> Lit -> Either String (Expr Bool)
cmpColLit df op colName lit = case colElemType df colName of
  Just ElemInt    -> onInt
  Just ElemDouble -> onDouble
  Just ElemText   -> onText
  Just _          -> Left ("df filter: unsupported column type for " <> T.unpack colName)
  Nothing         -> Left ("df filter: unknown column " <> T.unpack colName)
  where
    onInt = case lit of
      LInt n    -> Right $ mkOrd op (F.col @Int colName) (F.lit (fromIntegral n))
      LDouble _ -> Left "df filter: Int column compared against non-integer literal"
      LText _   -> Left "df filter: Int column compared against string literal"
    onDouble = case lit of
      LDouble d -> Right $ mkOrd op (F.col @Double colName) (F.lit d)
      LInt n    -> Right $ mkOrd op (F.col @Double colName) (F.lit (fromIntegral n))
      LText _   -> Left "df filter: Double column compared against string literal"
    onText = case lit of
      LText t   -> Right $ mkEq op (F.col @Text colName) (F.lit t)
      _         -> Left "df filter: Text column compared against numeric literal"

-- Comparison operators restricted to @Eq@ (Eq class). Used for Text.
mkEq :: (Columnable a, Eq a) => BinOp -> Expr a -> Expr a -> Expr Bool
mkEq Eq  = (.==.)
mkEq Neq = (./=.)
mkEq op  = \_ _ -> error ("mkEq: non-equality op " <> show op)

-- Full ordering set. Used for Int/Double.
mkOrd :: (Columnable a, Ord a) => BinOp -> Expr a -> Expr a -> Expr Bool
mkOrd Eq  = (.==.)
mkOrd Neq = (./=.)
mkOrd Lt  = (.<.)
mkOrd Leq = (.<=.)
mkOrd Gt  = (.>.)
mkOrd Geq = (.>=.)
mkOrd op  = \_ _ -> error ("mkOrd: non-comparison op " <> show op)

-- ----------------------------------------------------------------------------
-- Derive
-- ----------------------------------------------------------------------------

elabDerive :: D.DataFrame -> Text -> PExpr -> Either String NamedExpr
elabDerive df nameOut pe = do
  ty <- inferType df pe
  case ty of
    ElemInt    -> (\e -> (nameOut, UExpr e)) <$> elabInt  df pe
    ElemDouble -> (\e -> (nameOut, UExpr e)) <$> elabFrac @Double df pe
    ElemText   -> (\e -> (nameOut, UExpr e)) <$> elabText df pe
    _          -> Left "df derive: only Int/Double/Text supported"

-- Walk the expression to infer a result element type. Arithmetic
-- widens Int + Double → Double. Text is pass-through.
inferType :: D.DataFrame -> PExpr -> Either String ElemType
inferType _ (PLit LInt{})    = Right ElemInt
inferType _ (PLit LDouble{}) = Right ElemDouble
inferType _ (PLit LText{})   = Right ElemText
inferType df (PCol c)        = case colElemType df c of
  Just t  -> Right t
  Nothing -> Left $ "df derive: unknown column " <> T.unpack c
inferType df (PUn Neg e)     = inferType df e
inferType _  (PUn Not _)     = Right ElemBool
inferType df (PBin op l r)
  | op `elem` [Eq, Neq, Lt, Leq, Gt, Geq, And, Or] = Right ElemBool
  | otherwise = do
      tl <- inferType df l
      tr <- inferType df r
      pure (widen tl tr)

widen :: ElemType -> ElemType -> ElemType
widen ElemDouble _          = ElemDouble
widen _ ElemDouble          = ElemDouble
widen ElemInt ElemInt       = ElemInt
widen a _                   = a

-- Integer-context numeric elaboration. Add/Sub/Mul/Neg supported;
-- Div rejected because Expr Int has no Fractional instance. A future
-- Cast step could coerce to Double for division.
elabInt :: D.DataFrame -> PExpr -> Either String (Expr Int)
elabInt _  (PLit (LInt n))    = Right (F.lit (fromIntegral n))
elabInt _  (PLit LDouble{})   = Left "df derive: Double literal in Int context"
elabInt _  (PLit LText{})     = Left "df derive: Text literal in Int context"
elabInt df (PCol c)           = case colElemType df c of
  Just ElemInt -> Right (F.col @Int c)
  Just _       -> Left $ "df derive: column " <> T.unpack c <> " is not Int"
  Nothing      -> Left $ "df derive: unknown column " <> T.unpack c
elabInt df (PUn Neg e)        = negate <$> elabInt df e
elabInt _  (PUn Not _)        = Left "df derive: `not` in numeric context"
elabInt df (PBin Add l r)     = (+) <$> elabInt df l <*> elabInt df r
elabInt df (PBin Sub l r)     = (-) <$> elabInt df l <*> elabInt df r
elabInt df (PBin Mul l r)     = (*) <$> elabInt df l <*> elabInt df r
elabInt _  (PBin Div _ _)     = Left "df derive: / not supported in Int context (cast to Double first)"
elabInt _  (PBin op _ _)      =
  Left $ "df derive: unsupported op in Int context: " <> show op

-- Fractional-context numeric elaboration. Handles Div.
elabFrac
  :: forall n. (Columnable n, Num n, Fractional n)
  => D.DataFrame -> PExpr -> Either String (Expr n)
elabFrac _  (PLit (LInt n))    = Right (F.lit (fromIntegral n))
elabFrac _  (PLit (LDouble d)) = Right (F.lit (realToFrac d))
elabFrac _  (PLit LText{})     = Left "df derive: Text literal in numeric context"
elabFrac df (PCol c)           = case colElemType df c of
  Just t | matchesNum @n t -> Right (F.col @n c)
  Just _                   -> Left $ "df derive: column " <> T.unpack c <> " has wrong type"
  Nothing                  -> Left $ "df derive: unknown column " <> T.unpack c
elabFrac df (PUn Neg e)        = negate <$> elabFrac df e
elabFrac _  (PUn Not _)        = Left "df derive: `not` in numeric context"
elabFrac df (PBin Add l r)     = (+) <$> elabFrac df l <*> elabFrac df r
elabFrac df (PBin Sub l r)     = (-) <$> elabFrac df l <*> elabFrac df r
elabFrac df (PBin Mul l r)     = (*) <$> elabFrac df l <*> elabFrac df r
elabFrac df (PBin Div l r)     = (/) <$> elabFrac df l <*> elabFrac df r
elabFrac _  (PBin op _ _)      =
  Left $ "df derive: unsupported op in numeric context: " <> show op

elabText :: D.DataFrame -> PExpr -> Either String (Expr Text)
elabText _  (PLit (LText t)) = Right (F.lit t)
elabText df (PCol c)         = case colElemType df c of
  Just ElemText -> Right (F.col @Text c)
  _             -> Left $ "df derive: column " <> T.unpack c <> " is not Text"
elabText _ other = Left $ "df derive: unsupported Text expression: " <> show other

-- | Runtime check: does the column's element type match the requested
-- numeric type @n@?
matchesNum :: forall n. Columnable n => ElemType -> Bool
matchesNum ElemInt    = case testEquality (typeRep @n) (typeRep @Int)    of Just Refl -> True; _ -> False
matchesNum ElemDouble = case testEquality (typeRep @n) (typeRep @Double) of Just Refl -> True; _ -> False
matchesNum _          = False

-- ----------------------------------------------------------------------------
-- Column schema reflection
-- ----------------------------------------------------------------------------

-- | The narrow set of element types tv's elaborator reasons about.
-- 'ElemOther' covers anything else (Maybe Int, tuples, …) that the
-- elaborator refuses to handle rather than guess at.
data ElemType = ElemInt | ElemDouble | ElemText | ElemBool | ElemOther
  deriving (Eq, Show)

-- | Look up a column and classify its element type.
colElemType :: D.DataFrame -> Text -> Maybe ElemType
colElemType df name
  | not (M.member name (columnIndices df)) = Nothing
  | otherwise = Just $ classify (unsafeGetColumn name df)

classify :: Column -> ElemType
classify (UnboxedColumn _ (_ :: VU.Vector a))
  | Just Refl <- testEquality (typeRep @a) (typeRep @Int)    = ElemInt
  | Just Refl <- testEquality (typeRep @a) (typeRep @Double) = ElemDouble
  | otherwise                                                = ElemOther
classify (BoxedColumn _ (_ :: V.Vector a))
  | Just Refl <- testEquality (typeRep @a) (typeRep @Text)   = ElemText
  | Just Refl <- testEquality (typeRep @a) (typeRep @Bool)   = ElemBool
  | otherwise                                                = ElemOther
