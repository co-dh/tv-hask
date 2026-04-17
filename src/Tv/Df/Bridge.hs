{-# LANGUAGE OverloadedStrings #-}
-- | Bridge between DuckDB-backed 'AdbcTable' and pure-Haskell
-- 'Df.DataFrame'. Let feature modules migrate one at a time without
-- immediately overhauling the View stack's table type.
--
-- The conversion is disk-mediated today: Parquet on the way to
-- DataFrame (preserves types), CSV on the way back (all dataframe has
-- natively). That's wasteful for large results; acceptable for the
-- port's validation milestones.
module Tv.Df.Bridge
  ( fromAdbc
  , toAdbc
  ) where

import System.Directory (removeFile)
import Control.Exception (try, SomeException)

import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)
import qualified Tv.Df as Df
import qualified Tv.Tmp as Tmp

-- | Materialize an AdbcTable as a DataFrame via a tmp Parquet round-trip.
-- Parquet preserves the column types DuckDB reports (Int/Double/Text/…),
-- which CSV would lose.
fromAdbc :: AdbcTable -> IO Df.DataFrame
fromAdbc t = do
  tmp <- Tmp.threadPath "bridge.parquet"
  Table.copyToParquet t tmp
  df <- Df.readParquet tmp
  _ <- try (removeFile tmp) :: IO (Either SomeException ())
  pure df

-- | Convert a DataFrame to an AdbcTable by serializing CSV and
-- re-ingesting through DuckDB's read_csv_auto.
toAdbc :: Df.DataFrame -> IO (Maybe AdbcTable)
toAdbc df = Table.fromCsv (Df.toCsv df)
