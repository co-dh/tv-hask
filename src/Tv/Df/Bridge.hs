{-# LANGUAGE OverloadedStrings #-}
-- | Bridge between DuckDB-backed 'AdbcTable' and pure-Haskell
-- 'Df.DataFrame'. Lets feature modules try the dataframe backend for a
-- computation while the View stack still holds AdbcTable.
--
-- Disk-mediated: Parquet on the way out (preserves types), CSV on the
-- way back (all dataframe's writers). Slow but simple; reserved for
-- cases where the alternative is a full migration of the View type.
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
