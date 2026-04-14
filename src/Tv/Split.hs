{-# LANGUAGE OverloadedStrings #-}
-- | Split: break a string column into multiple columns by a delimiter
-- or regex. Mirrors Tc.Split.runWith — probe DuckDB for the max number
-- of parts (capped at 20), then derive @{col}_1 .. {col}_N@ via
-- @string_split_regex@. Non-string columns are a no-op.
module Tv.Split
  ( splitColumn
  , maxParts
  ) where

import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.Types
import Tv.Derive (rebuildWith, quoteId)
import Tv.Eff (Eff, IOE, (:>), liftIO, tryE)
import Optics.Core ((^.))

-- | Split the column at @colIdx@ by regex @pat@ into multiple new
-- columns. Returns the original TblOps if the column isn't a string,
-- the pattern is empty, the max-parts probe fails, or nothing would
-- actually split (n <= 1). Mirrors Tc.Split.runWith.
splitColumn :: IOE :> es => TblOps -> Int -> Text -> Eff es TblOps
splitColumn ops colIdx pat
  | colIdx < 0 || colIdx >= V.length (ops ^. tblColNames) = pure ops
  | (ops ^. tblColType) colIdx /= CTStr = pure ops
  | T.null pat = pure ops
  | otherwise = do
      let col = (ops ^. tblColNames) V.! colIdx
          ep  = escSql pat
      n <- maxParts ops col ep
      if n <= 1
        then pure ops
        else do
          r <- tryE (rebuildWith ops (wrapSplit col ep n))
          pure (maybe ops id r)

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
maxParts ops col ep = do
  let qc = quoteId col
      wrap sub =
        "SELECT COALESCE(max(array_length(string_split_regex(" <> qc <> ", '"
        <> ep <> "'))), 0) AS n FROM (" <> sub <> ")"
  r <- tryE (rebuildWith ops wrap)
  case r of
    Nothing   -> pure 0
    Just ops' -> do
      cell <- liftIO ((ops' ^. tblCellStr) 0 0)
      pure (min (fromMaybe 0 (readIntMaybe cell)) 20)

-- | Parse a Text as Int; Nothing on failure.
readIntMaybe :: Text -> Maybe Int
readIntMaybe t = case reads (T.unpack t) of
  [(n, "")] -> Just n
  _         -> Nothing
