{-# LANGUAGE TemplateHaskell #-}
-- | Derive 'StrEnum' for plain sum types:
--
-- > data ColType = ColTypeInt | ColTypeFloat | ... | ColTypeOther
-- > $(deriveStrEnum ''ColType)
--
-- Strips the common constructor prefix and lowercases the remainder to
-- produce each label ('ColTypeInt' → @"int"@). Emits all three class
-- methods — 'toString', 'all', 'ofStringQ' — matching the hand-written
-- boilerplate this splice replaces.
--
-- Only nullary constructors are supported. Types with payloads (e.g.
-- 'Tv.Types.ViewKind') or irregular labels (e.g. 'Tv.Types.Cmd', which
-- dot-joins segments: @"row.inc"@) keep their hand-written instances.
--
-- References to 'StrEnum' / 'toString' / 'all' / 'ofStringQ' /
-- 'V.fromList' are emitted as bare 'mkName' so they resolve at the
-- splice use site (Tv.Types) — this avoids a Tv.Types ↔ Tv.Types.TH
-- import cycle. 'OverloadedStrings' at the splice site converts the
-- generated @StringL@ literals / patterns to 'Text'.
module Tv.Types.TH (deriveStrEnum) where

import Data.Char (toLower)
import Data.List (isPrefixOf)
import Language.Haskell.TH

-- | Generate @instance StrEnum T@ for a plain enum @T@.
deriveStrEnum :: Name -> Q [Dec]
deriveStrEnum tyName = do
  info <- reify tyName
  ctors <- case info of
    TyConI (DataD _ _ _ _ cs _)   -> pure cs
    TyConI (NewtypeD _ _ _ _ c _) -> pure [c]
    _ -> fail $ "deriveStrEnum: " <> show tyName <> " is not a data/newtype"
  names <- mapM nullaryName ctors
  -- Prefer the longest common prefix of constructor names over the type
  -- name — PlotKind / ExportFmt constructors share 'Plot' / 'Export',
  -- not the full type name. ColType / Agg fall back to the type name
  -- via choosePrefix when it's already a prefix of every ctor.
  let prefix = choosePrefix (nameBase tyName) (map nameBase names)
      labels = [ (n, toLabel prefix (nameBase n)) | n <- names ]
  body <- mkBody labels
  let inst = InstanceD Nothing [] (AppT (ConT (mkName "StrEnum")) (ConT tyName)) body
  pure [inst]
  where
    nullaryName (NormalC n []) = pure n
    nullaryName (RecC n [])    = pure n
    nullaryName c = fail $ "deriveStrEnum: non-nullary constructor " <> show c

-- | Build the three method declarations for @instance StrEnum T@.
mkBody :: [(Name, String)] -> Q [Dec]
mkBody labels = do
  let toStringN  = mkName "toString"
      allN       = mkName "all"
      ofStringQN = mkName "ofStringQ"
      fromListN  = mkName "V.fromList"
  toStrClauses <- mapM toStringClause labels
  allBody <- normalB $ appE (varE fromListN) (listE $ map (conE . fst) labels)
  -- ofStringQ s = case s of "l1" -> Just C1; ...; _ -> Nothing.
  -- A single 'case' (vs multiple clauses) keeps the generated tree
  -- small; OverloadedStrings rewrites the String-typed patterns to
  -- Text patterns at splice expansion.
  sName <- newName "s"
  let mkMatch (c, lbl) =
        match (litP (StringL lbl)) (normalB [| Just $(conE c) |]) []
      matches = map mkMatch labels ++ [match wildP (normalB [| Nothing |]) []]
  caseExpr <- caseE (varE sName) matches
  let ofStringQClause = Clause [VarP sName] (NormalB caseExpr) []
  pure
    [ FunD toStringN toStrClauses
    , ValD (VarP allN) allBody []
    , FunD ofStringQN [ofStringQClause]
    ]

-- | Pick the best prefix to strip from each constructor name.
--   Prefer the type name when it's a prefix of every ctor; otherwise
--   fall back to the longest common prefix of the constructors.
choosePrefix :: String -> [String] -> String
choosePrefix tyName ctors
  | not (null tyName), all (tyName `isPrefixOf`) ctors = tyName
  | otherwise = longestCommonPrefix ctors

longestCommonPrefix :: [String] -> String
longestCommonPrefix []     = ""
longestCommonPrefix (x:xs) = foldr cp x xs
  where cp a b = map fst $ takeWhile (uncurry (==)) $ zip a b

-- | Strip the chosen prefix and lowercase the remainder. If the
--   constructor name equals the prefix (empty remainder), fall back to
--   the lowercased full name.
toLabel :: String -> String -> String
toLabel prefix ctor
  | prefix `isPrefixOf` ctor, rest <- drop (length prefix) ctor, not (null rest)
  = map toLower rest
  | otherwise = map toLower ctor

toStringClause :: (Name, String) -> Q Clause
toStringClause (ctor, lbl) =
  clause [conP ctor []] (normalB $ litE (StringL lbl)) []
