{-# LANGUAGE OverloadedStrings #-}
-- | Canonical import of the @dataframe@ package for tv-hask.
--
-- Convention inside tv-hask:
--
-- > import qualified Tv.Df   as Df      -- core table verbs (readCsv, select, filter, …)
-- > import qualified Tv.Df.F as F       -- expression DSL (col, lit, mean, gt, …)
--
-- This module is a thin pass-through: types and verbs come straight from
-- 'DataFrame' so we don't drift from upstream. Anything we add on top
-- (tv-specific helpers, FileFormat dispatch, glue to "Tv.Types") lives
-- here so the rest of the tree imports a single name.
module Tv.Df
  ( -- * Core types
    DataFrame
  , GroupedDataFrame
  , Column
  , SortOrder (..)
  , JoinType (..)
    -- * Readers
  , readCsv
  , readCsvWithOpts
  , readTsv
  , readParquet
  , readParquetWithOpts
    -- * Exploration
  , take
  , takeLast
  , describeColumns
  , summarize
    -- * Row ops
  , filterWhere
  , sortBy
    -- * Column ops
  , select
  , exclude
  , rename
  , derive
    -- * Group / aggregate
  , groupBy
  , aggregate
  , distinct
  , frequencies
    -- * Joins
  , innerJoin
  , leftJoin
  , rightJoin
  , fullOuterJoin
  , join
    -- * I/O
  , writeCsv
  ) where

import Prelude hiding (take)

import DataFrame
  ( DataFrame
  , GroupedDataFrame
  , Column
  , SortOrder (..)
  , JoinType (..)
  , readCsv, readCsvWithOpts, readTsv
  , readParquet, readParquetWithOpts
  , take, takeLast, describeColumns, summarize
  , filterWhere, sortBy
  , select, exclude, rename, derive
  , groupBy, aggregate, distinct, frequencies
  , innerJoin, leftJoin, rightJoin, fullOuterJoin, join
  , writeCsv
  )
