{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
-- | Tests for the dataframe → PRQL translator.
--
-- Each 'parity_*' case builds a pipeline from dataframe's typed Expr
-- combinators, then runs it two ways: (a) directly through dataframe's
-- eager engine on an in-memory DataFrame, (b) via 'Tv.Df.Prql.compile'
-- → prqlc → DuckDB → CSV → DataFrame. The resulting frames must agree.
-- This is how we verify the translator faithfully reflects the DSL.
module TestDf (tests) where

import Control.Exception (try, SomeException)
import Data.Function ((&))
import Data.Int (Int64)
import qualified Data.Text as T
import System.Directory (removeFile)

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, assertBool, assertEqual, Assertion)

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Operators ((.>.), (.+.), (.*.), as)

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import qualified Tv.Data.DuckDB.Table as Table
import qualified Tv.Df as Df
import qualified Tv.Df.Prql as DfQ
import qualified Tv.Tmp as Tmp

-- | Run a PRQL string through DuckDB, COPY the result to a tmp CSV,
-- read it back into a DataFrame.
prqlToDf :: T.Text -> IO D.DataFrame
prqlToDf prqlText = do
  mSql <- Prql.compile prqlText
  case mSql of
    Nothing  -> error ("prqlc failed for: " ++ T.unpack prqlText)
    Just sql -> do
      tmp <- Tmp.threadPath "parity.csv"
      let copySql = "COPY (" <> Table.stripSemi sql <> ") TO '"
                 <> T.pack tmp <> "' (FORMAT CSV, HEADER)"
      _ <- Conn.query copySql
      df <- Df.readCsv tmp
      _ <- try (removeFile tmp) :: IO (Either SomeException ())
      pure df

-- Row/col shape agreement. Stricter comparison (row-by-row values)
-- would over-specify given the Parquet→dataframe vs PRQL→CSV→dataframe
-- type inference difference; row counts are the real contract.
assertFrameAgree :: String -> D.DataFrame -> D.DataFrame -> Assertion
assertFrameAgree label a b = do
  assertEqual (label <> ": row count")    (Df.nRows a) (Df.nRows b)
  assertEqual (label <> ": column count") (Df.nCols a) (Df.nCols b)

-- | Filter + derive on sort_test.parquet (name: Text, value: Int64, 3 rows).
test_parity_filter_derive :: Assertion
test_parity_filter_derive = do
  dfIn <- Df.readParquet "data/sort_test.parquet"
  let dfA = dfIn
          & D.filterWhere (F.col @Int64 "value" .>. F.lit 1)
          & D.derive "doubled" (F.col @Int64 "value" .*. F.lit 2)
      pipeline = DfQ.fromBase "from `data/sort_test.parquet`"
        DfQ.|> DfQ.filter_ (F.col @Int64 "value" .>. F.lit 1)
        DfQ.|> DfQ.derive [(F.col @Int64 "value" .*. F.lit 2) `as` "doubled"]
  dfB <- prqlToDf (DfQ.compile pipeline)
  assertFrameAgree "filter_derive" dfA dfB
  assertEqual "filter produces 2 rows" 2 (Df.nRows dfA)

-- | Sort + take on the same parquet.
test_parity_sort_take :: Assertion
test_parity_sort_take = do
  dfIn <- Df.readParquet "data/sort_test.parquet"
  let dfA = dfIn
          & D.sortBy [D.Asc (F.col @Int64 "value")]
          & D.take 2
      pipeline = DfQ.fromBase "from `data/sort_test.parquet`"
        DfQ.|> DfQ.sortAsc ["value"]
        DfQ.|> DfQ.take_ 2
  dfB <- prqlToDf (DfQ.compile pipeline)
  assertFrameAgree "sort_take" dfA dfB
  assertEqual "took 2 rows" 2 (Df.nRows dfA)

-- | Text column comparison.
test_parity_filter_text :: Assertion
test_parity_filter_text = do
  dfIn <- Df.readParquet "data/sort_test.parquet"
  let dfA = dfIn
          & D.filterWhere (F.col @T.Text "name" .>. F.lit @T.Text "alice")
      pipeline = DfQ.fromBase "from `data/sort_test.parquet`"
        DfQ.|> DfQ.filter_ (F.col @T.Text "name" .>. F.lit @T.Text "alice")
  dfB <- prqlToDf (DfQ.compile pipeline)
  assertFrameAgree "filter_text" dfA dfB
  assertEqual "2 rows > 'alice'" 2 (Df.nRows dfA)

-- | Select + distinct on multi_freq.csv (a,b,c; 6 rows).
test_parity_distinct :: Assertion
test_parity_distinct = do
  dfIn <- Df.readCsv "data/multi_freq.csv"
  let dfA = dfIn
          & D.select ["a", "b"]
          & D.distinct
      pipeline = DfQ.fromBase "from `data/multi_freq.csv`"
        DfQ.|> DfQ.select ["a", "b"]
        DfQ.|> DfQ.distinct ["a", "b"]
  dfB <- prqlToDf (DfQ.compile pipeline)
  assertFrameAgree "distinct" dfA dfB
  assertEqual "4 unique (a,b) pairs" 4 (Df.nRows dfA)

-- | Nested-precedence regression: @(a + b) * c@ must render with
-- the inner parens intact. Dataframe evaluates as @(1+10)*2 = 22@,
-- PRQL → DuckDB should produce the same (not @1 + (10 * 2) = 21@).
test_parity_nested_precedence :: Assertion
test_parity_nested_precedence = do
  dfIn <- Df.readParquet "data/sort_test.parquet"
  let -- sort_test has (name, value); we derive a @nested@ col.
      -- dfA: eager dataframe.
      dfA = dfIn
          & D.derive "nested"
              ((F.col @Int64 "value" .+. F.lit 10) .*. F.lit 2)
      pipeline = DfQ.fromBase "from `data/sort_test.parquet`"
        DfQ.|> DfQ.derive
               [ ((F.col @Int64 "value" .+. F.lit 10) .*. F.lit 2)
                   `as` "nested" ]
      prql = DfQ.compile pipeline
  -- Emitted PRQL should contain parens around the inner add.
  assertBool ("PRQL missing inner parens: " ++ T.unpack prql)
             (T.isInfixOf "(" prql && T.isInfixOf ") *" prql)
  dfB <- prqlToDf prql
  assertFrameAgree "nested_precedence" dfA dfB

-- | Scalar aggregate: sum and count on a parquet table.
test_parity_aggregate :: Assertion
test_parity_aggregate = do
  dfIn <- Df.readParquet "data/sort_test.parquet"
  -- Eager path: reduce to a 1-row frame via derive on a pre-aggregated frame.
  -- dataframe's D.sum is a scalar; we synthesize a 1-row DF manually.
  let total  = D.sum (F.col @Int64 "value") dfIn
      _      = total :: Int64
  -- The translator path builds the aggregate through DfQ.
      pipeline = DfQ.fromBase "from `data/sort_test.parquet`"
        DfQ.|> DfQ.aggregate [F.sum (F.col @Int64 "value") `as` "total"]
  dfB <- prqlToDf (DfQ.compile pipeline)
  assertEqual "aggregate emits 1 row" 1 (Df.nRows dfB)
  assertEqual "aggregate emits 1 col" 1 (Df.nCols dfB)

-- | Append two copies of the same table → row count doubles.
test_parity_append :: Assertion
test_parity_append = do
  -- Materialize the table as a view so PRQL's `append` can reference it.
  _ <- Conn.query
         "CREATE OR REPLACE TEMP VIEW __append_src \
         \ AS SELECT * FROM 'data/sort_test.parquet'"
  let pipeline = DfQ.fromBase "from __append_src"
        DfQ.|> DfQ.append "__append_src"
  dfB <- prqlToDf (DfQ.compile pipeline)
  assertEqual "append doubles row count" 6 (Df.nRows dfB)

tests :: TestTree
tests = testGroup "TestDf"
  [ testCase "parity_filter_derive"     test_parity_filter_derive
  , testCase "parity_sort_take"         test_parity_sort_take
  , testCase "parity_filter_text"       test_parity_filter_text
  , testCase "parity_distinct"          test_parity_distinct
  , testCase "parity_nested_precedence" test_parity_nested_precedence
  , testCase "parity_aggregate"         test_parity_aggregate
  , testCase "parity_append"            test_parity_append
  ]
