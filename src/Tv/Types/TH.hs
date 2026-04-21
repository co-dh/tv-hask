{-# LANGUAGE TemplateHaskell #-}
-- | @$(deriveStrEnum ''T)@ — emits @instance StrEnum T@ for an enum of
-- nullary ctors. Strips the longest common ctor prefix (or type name
-- when that's a prefix of every ctor), lowercases the remainder.
module Tv.Types.TH (deriveStrEnum) where

import Data.Char (toLower)
import Data.List (isPrefixOf)
import Language.Haskell.TH

deriveStrEnum :: Name -> Q [Dec]
deriveStrEnum ty = do
  cs <- reify ty >>= \case
    TyConI (DataD    _ _ _ _ cs' _) -> traverse nullary cs'
    TyConI (NewtypeD _ _ _ _ c   _) -> (:[]) <$> nullary c
    _ -> fail $ "deriveStrEnum: " <> show ty <> " not data/newtype"
  let base  = nameBase ty
      ns    = map nameBase cs
      pfx | not (null base), all (base `isPrefixOf`) ns = base
          | otherwise = foldr1 lcp ns
      lcp a b = map fst $ takeWhile (uncurry (==)) $ zip a b
      lab n = let r = drop (length pfx) (nameBase n)
              in map toLower $ if null r then nameBase n else r
      ps = [ (c, lab c) | c <- cs ]
      toStrD = funD (mkName "toString")
                 [ clause [conP c []] (normalB $ litE $ StringL l) [] | (c, l) <- ps ]
      allD   = valD (varP $ mkName "all")
                 (normalB $ appE (varE $ mkName "V.fromList") (listE $ map (conE . fst) ps)) []
      ofStrD = funD (mkName "ofStringQ") $
                 [ clause [litP $ StringL l] (normalB [| Just $(conE c) |]) [] | (c, l) <- ps ]
                 ++ [ clause [wildP] (normalB [| Nothing |]) [] ]
  (:[]) <$> instanceD (cxt []) (appT (conT $ mkName "StrEnum") (conT ty)) [toStrD, allD, ofStrD]
 where
  nullary (NormalC n []) = pure n
  nullary (RecC    n []) = pure n
  nullary c              = fail $ "deriveStrEnum: non-nullary " <> show c
