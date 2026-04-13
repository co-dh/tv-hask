{-# LANGUAGE ScopedTypeVariables #-}
-- | Export: materialize a 'TblOps' to disk in csv/parquet/json/ndjson.
--
-- The Lean port drives this through DuckDB @COPY (<prql>) TO '...'@ against
-- the live connection; our 'TblOps' record-of-functions doesn't carry the
-- source query, so we materialize via '_tblCellStr' and write the file
-- ourselves. The on-disk output shape matches what Tc's tests assert
-- against (CSV with header row, newline-delimited JSON, etc.).
module Tv.Export
  ( exportTable
  , exportFmtExt
  , exportFmtFromText
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.IO (IOMode(..), withFile, hPutStr)

import Tv.Types
import Tv.Eff (Eff, IOE, (:>), liftIO)
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- | File extension for an 'ExportFmt', matching Tc's @ExportFmt.ext@.
exportFmtExt :: ExportFmt -> Text
exportFmtExt EFCsv = "csv"
exportFmtExt EFParquet = "parquet"
exportFmtExt EFJson = "json"
exportFmtExt EFNdjson = "ndjson"

-- | Parse a format name (as produced by the fzf picker). Returns
-- 'Nothing' for unknown strings.
exportFmtFromText :: Text -> Maybe ExportFmt
exportFmtFromText t = case T.strip t of
  "csv"     -> Just EFCsv
  "parquet" -> Just EFParquet
  "json"    -> Just EFJson
  "ndjson"  -> Just EFNdjson
  _         -> Nothing

-- | Write the current table to @path@ in the given format.
-- 'EFParquet' is rejected here — writing parquet requires a DuckDB
-- connection which this module intentionally doesn't carry; callers
-- must dispatch parquet exports through the DB backend directly.
exportTable :: IOE :> es => TblOps -> ExportFmt -> FilePath -> Eff es ()
exportTable tbl fmt path = liftIO $ do
  rows <- materialize tbl
  let cols = (tbl ^. tblColNames)
  case fmt of
    EFCsv    -> TIO.writeFile path (renderCsv cols rows)
    EFJson   -> TIO.writeFile path (renderJson cols rows)
    EFNdjson -> TIO.writeFile path (renderNdjson cols rows)
    EFParquet -> ioError (userError "export: parquet requires DuckDB backend (not wired)")

-- | Materialize every cell. Uses the column count from the header so we
-- pad ragged rows with empty strings.
materialize :: TblOps -> IO (Vector (Vector Text))
materialize tbl =
  let nc = V.length ((tbl ^. tblColNames))
      nr = (tbl ^. tblNRows)
  in V.generateM nr $ \r -> V.generateM nc ((tbl ^. tblCellStr) r)

-- ============================================================================
-- CSV (RFC 4180-ish: quote cells with ", ,, CR or LF)
-- ============================================================================

renderCsv :: Vector Text -> Vector (Vector Text) -> Text
renderCsv cols rows = T.unlines (header : V.toList (V.map rowLine rows))
  where
    header = T.intercalate "," (V.toList (V.map csvCell cols))
    rowLine r = T.intercalate "," (V.toList (V.map csvCell r))

csvCell :: Text -> Text
csvCell t
  | T.any needQuote t = "\"" <> T.replace "\"" "\"\"" t <> "\""
  | otherwise = t
  where needQuote c = c == ',' || c == '"' || c == '\n' || c == '\r'

-- ============================================================================
-- JSON: single top-level array of objects. NDJSON: one object per line.
-- ============================================================================

renderJson :: Vector Text -> Vector (Vector Text) -> Text
renderJson cols rows =
  "[" <> T.intercalate "," (V.toList (V.map (rowObj cols) rows)) <> "]\n"

renderNdjson :: Vector Text -> Vector (Vector Text) -> Text
renderNdjson cols rows =
  T.unlines (V.toList (V.map (rowObj cols) rows))

rowObj :: Vector Text -> Vector Text -> Text
rowObj cols row =
  let pairs = V.zipWith (\k v -> jsonStr k <> ":" <> jsonStr v) cols row
  in "{" <> T.intercalate "," (V.toList pairs) <> "}"

jsonStr :: Text -> Text
jsonStr t = "\"" <> T.concatMap esc t <> "\""
  where
    esc '"'  = "\\\""
    esc '\\' = "\\\\"
    esc '\n' = "\\n"
    esc '\r' = "\\r"
    esc '\t' = "\\t"
    esc c
      | c < '\x20' = T.pack (printf4 c)
      | otherwise  = T.singleton c
    printf4 c = "\\u" <> pad (showHex (fromEnum c))
    pad s = replicate (4 - length s) '0' ++ s
    showHex n = let (q,r) = n `divMod` 16
                in (if q == 0 then "" else showHex q) ++ [hexDig r]
    hexDig d | d < 10 = toEnum (fromEnum '0' + d)
             | otherwise = toEnum (fromEnum 'a' + d - 10)
