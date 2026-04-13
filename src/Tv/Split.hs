{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Split: break a string column into multiple columns by a delimiter
-- or regex. Mirrors Tc.Split.runWith — probe DuckDB for the max number
-- of parts (capped at 20), then derive @{col}_1 .. {col}_N@ via
-- @string_split_regex@. Non-string columns are a no-op.
module Tv.Split
  ( splitColumn
  , maxParts
  ) where

import Control.Exception (try, SomeException)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.Types
import Tv.Derive (rebuildWithIO, quoteId)
import qualified Tv.Data.DuckDB as DB
import Tv.Eff (Eff, IOE, (:>), liftIO)
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- | Split the column at @colIdx@ by regex @pat@ into multiple new
-- columns. Returns the original TblOps if the column isn't a string,
-- the pattern is empty, the max-parts probe fails, or nothing would
-- actually split (n <= 1). Mirrors Tc.Split.runWith.
splitColumn :: IOE :> es => TblOps -> Int -> Text -> Eff es TblOps
splitColumn ops colIdx pat
  | colIdx < 0 || colIdx >= V.length ((ops ^. tblColNames)) = pure ops
  | (ops ^. tblColType) colIdx /= CTStr = pure ops
  | T.null pat = pure ops
  | otherwise = liftIO $ do
      let col = (ops ^. tblColNames) V.! colIdx
          ep  = escSql pat
      n <- maxPartsIO ops col ep
      if n <= 1
        then pure ops
        else do
          r <- try (rebuildWithIO ops (wrapSplit col ep n))
          case r of
            Right ops' -> pure ops'
            Left (_ :: SomeException) -> pure ops

-- | Build @SELECT *, string_split_regex(col,'ep')[i] AS col_i, ...@.
wrapSplit :: Text -> Text -> Int -> Text -> Text
wrapSplit col ep n sub =
  let qc = quoteId col
      parts = [ "string_split_regex(" <> qc <> ", '" <> ep <> "')[" <> T.pack (show i)
                <> "] AS " <> quoteId (col <> "_" <> T.pack (show i))
              | i <- [1 .. n] ]
  in "SELECT *, " <> T.intercalate ", " parts <> " FROM (" <> sub <> ")"

-- | Probe DuckDB for the maximum number of split parts for @col@ under
-- pattern @ep@. Caps at 20 to match Tc.Split.maxParts. Any error (bad
-- regex, unsupported func, etc.) returns 0, which makes splitColumn a
-- no-op upstream.
maxParts :: IOE :> es => TblOps -> Text -> Text -> Eff es Int
maxParts ops col ep = liftIO (maxPartsIO ops col ep)

maxPartsIO :: TblOps -> Text -> Text -> IO Int
maxPartsIO ops col ep = do
  -- Re-materialize the current TblOps into a DuckDB table, then run the
  -- count query on it. We do the whole thing in one go by wrapping the
  -- same rebuild logic as derive, but swapping the final SELECT for a
  -- COALESCE/max aggregate. Failure degrades to 0.
  let qc = quoteId col
      wrap sub =
        "SELECT COALESCE(max(array_length(string_split_regex(" <> qc <> ", '"
        <> ep <> "'))), 0) AS n FROM (" <> sub <> ")"
  r <- try (rebuildWithIO ops wrap)
  case r of
    Left (_ :: SomeException) -> pure 0
    Right ops' -> do
      cell <- (ops' ^. tblCellStr) 0 0
      let v = fromMaybe 0 (readIntMaybe cell)
      pure (min v 20)

-- | Parse a Text as Int; Nothing on failure.
readIntMaybe :: Text -> Maybe Int
readIntMaybe t = case reads (T.unpack t) of
  [(n, "")] -> Just n
  _         -> Nothing
