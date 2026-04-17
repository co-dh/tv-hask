{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
-- | Frequency computation on a 'DataFrame', mirroring the PRQL @freq@
-- function in @src/Tv/Data/DuckDB/funcs.prql@.
--
-- Given group-by columns @cs@ and a dataframe @df@, returns one row per
-- group with columns: @<cs>, Cnt, Pct, Bar@. @Cnt@ is the row count,
-- @Pct@ is the integer percentage of the total, @Bar@ is a string of
-- @Pct / 5@ hash characters. Sorted by @Cnt@ descending.
--
-- Not yet integrated with 'Tv.View' / 'Tv.Freq' — this is a side-by-side
-- implementation to validate the dataframe API fits tv's needs. See the
-- test suite for parity checks against the DuckDB path.
module Tv.Df.Freq
  ( freq
  ) where

import Data.Text (Text)
import qualified Data.Text as T

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Operators (as, liftDecorated, (|>))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.DataFrame (unsafeGetColumn)
import DataFrame.Internal.Expression (NamedExpr)

-- | One-shot freq. Eager; fine for datasets that fit in memory.
-- @cs@ must be non-empty and each name must exist in @df@.
freq :: [Text] -> D.DataFrame -> D.DataFrame
freq cs df =
  let
    -- Count rows per group. `count` requires knowing the element type of the
    -- column it counts, so reflect it from the actual column at runtime.
    -- Pattern cribbed from DataFrame.IR.countExpr in the upstream FFI module.
    counted = D.groupBy cs df |> D.aggregate [cntNamed (head cs) df "Cnt"]
    total   = D.sum (F.col @Int "Cnt") counted
    pctExpr = liftDecorated (\c -> c * 100 `div` max 1 total) "pct" Nothing
                (F.col @Int "Cnt")
    barExpr = liftDecorated (\p -> T.replicate (max 0 (p `div` 5)) "#")
                "bar" Nothing (F.col @Int "Pct")
  in counted
       |> D.derive "Pct" pctExpr
       |> D.derive "Bar" barExpr
       |> D.sortBy [D.Desc (F.col @Int "Cnt")]

-- | Build a @count(col)@ 'NamedExpr' that works regardless of the column's
-- element type. @col@ must exist in @df@.
cntNamed :: Text -> D.DataFrame -> Text -> NamedExpr
cntNamed colName df asName = case unsafeGetColumn colName df of
  UnboxedColumn Nothing  (_ :: VU.Vector a) -> F.count (F.col @a         colName) `as` asName
  UnboxedColumn (Just _) (_ :: VU.Vector a) -> F.count (F.col @(Maybe a) colName) `as` asName
  BoxedColumn   Nothing  (_ :: V.Vector  a) -> F.count (F.col @a         colName) `as` asName
  BoxedColumn   (Just _) (_ :: V.Vector  a) -> F.count (F.col @(Maybe a) colName) `as` asName
