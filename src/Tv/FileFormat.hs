{-
  FileFormat: file format detection, opening, and viewing.
  Maps file extensions -> DuckDB readers, handles ATTACH for database files.

  Literal port of Tc/Tc/FileFormat.lean — same record fields, same function
  names, same order, same comments. Refactor only after parity.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.FileFormat
  ( Format(..)
  , formats
  , find
  , isData
  , viewFile
  , readCsv
  , openFile
  ) where

import Tv.Prelude
import Codec.Compression.GZip (decompress)
import Control.Exception (SomeException, try)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T
import qualified Data.Vector as V
import System.Directory (canonicalizePath)
import System.IO (stdout)

import qualified Tv.Util as Log
import Tv.View (View)
import qualified Tv.View as View
import Tv.Types (escSql)
import qualified Tv.Types as Types
import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable)
import qualified Tv.Data.DuckDB.Table as Table
import Optics.TH (makeFieldLabelsNoPrefix)

-- | How DuckDB should handle a file extension
data Format = Format
  { exts       :: Vector Text  -- file extensions (e.g. [".csv", ".parquet"])
  , reader     :: Text         -- DuckDB reader function. Empty = auto-detect.
  , duckdbExt  :: Text         -- DuckDB extension to INSTALL/LOAD. Empty = none.
  , attach     :: Bool         -- true = ATTACH as database, list tables
  , attachType :: Text         -- ATTACH TYPE clause (e.g. "SQLITE"). Empty = native.
  }
makeFieldLabelsNoPrefix ''Format

-- | All file formats supported by DuckDB
formats :: Vector Format
formats = V.fromList
  [ Format (V.fromList [".csv", ".parquet", ".json", ".jsonl", ".ndjson"]) "" "" False ""
  , Format (V.fromList [".arrow", ".feather"]) "read_arrow" "arrow" False ""
  , Format (V.fromList [".xlsx", ".xls"]) "read_xlsx" "excel" False ""
  , Format (V.fromList [".avro"]) "read_avro" "avro" False ""
  , Format (V.fromList [".duckdb", ".db"]) "" "" True ""
  , Format (V.fromList [".sqlite", ".sqlite3"]) "" "sqlite" True "SQLITE"
  ]

-- | Strip .gz suffix for extension matching
stripGz :: Text -> Text
stripGz p = if T.isSuffixOf ".gz" p then T.dropEnd 3 p else p

-- | Find format by file extension (handles .gz: strip suffix, match inner ext)
find :: Text -> Maybe Format
find path_ =
  let p = stripGz path_
  in V.find (\fmt -> V.any (`T.isSuffixOf` p) (fmt ^. #exts)) formats

-- | Is file a recognized data format?
isData :: Text -> Bool
isData = isJust . find

-- | Resolve absolute path; falls back to the input on error (missing file).
absPath :: Text -> IO Text
absPath path_ = do
  r <- try (canonicalizePath (T.unpack path_)) :: IO (Either SomeException FilePath)
  pure $ case r of
    Right p -> T.pack p
    Left _  -> path_

-- | Read .gz file and return decompressed bytes
readGz :: Text -> IO LBS.ByteString
readGz path_ = decompress <$> LBS.readFile (T.unpack path_)

-- | View file: read it (decompressing .gz via zlib) and dump to stdout.
-- The `tm` flag is the test-mode marker — ignored now that there's no
-- external pager to switch on/off. In interactive use the terminal
-- handles scrollback; users who want paging can pipe (`tv ... | less`).
-- Lazy ByteString streamed directly: no String/Text intermediates.
viewFile :: Bool -> Text -> IO ()
viewFile _ path_ = do
  bs <- if T.isSuffixOf ".gz" path_
          then readGz path_
          else LBS.readFile (T.unpack path_)
  LBS.hPut stdout bs

-- | Try to ingest as CSV via DuckDB read_csv (handles .gz). Nothing = not valid CSV.
readCsv :: Text -> IO (Maybe (View AdbcTable))
readCsv path_ = do
  r <- try action :: IO (Either SomeException (Maybe (View AdbcTable)))
  case r of
    Right v -> pure v
    Left e  -> do
      Log.write "readCsv" (path_ <> ": " <> T.pack (show e))
      pure Nothing
  where
    action = do
      ap <- absPath path_
      mt <- Table.fileWith ap "read_csv" ""
      pure $ mt >>= \t -> View.fromTbl t path_ 0 V.empty 0

-- | ATTACH database file and list its tables as a folder view
attachFile :: Text -> Format -> IO (Maybe (View AdbcTable))
attachFile ap Format{duckdbExt, attachType} = do
  Table.loadExt duckdbExt
  let typClause = if T.null attachType then "" else "TYPE " <> attachType <> ", "
  _ <- Conn.query "DETACH DATABASE IF EXISTS extdb"
  _ <- Conn.query ("ATTACH '" <> escSql ap <> "' AS extdb (" <> typClause <> "READ_ONLY)")
  mQr <- Table.prqlQuery Prql.ducktabs
  case mQr of
    Nothing -> pure Nothing
    Just qr ->
      let totalN = Conn.nrows qr
      in if totalN == 0
           then pure Nothing
           else do
             adbc <- Table.ofResult qr (Prql.defaultQuery { Prql.base = Prql.ducktabs }) totalN
             let disp_ = case reverse (T.splitOn "/" ap) of
                           (x:_) -> x
                           []    -> ap
             pure $ ((\v -> v & #vkind .~ Types.VkFld ap 1 & #disp .~ disp_) <$> View.fromTbl adbc ap 0 (V.singleton "name") 0)

-- | Open any supported data file as a View (attach for DB, reader for data files)
openFile :: Text -> IO (Maybe (View AdbcTable))
openFile path_ = do
  ap <- absPath path_
  case find path_ of
    Just fmt@Format{attach, reader, duckdbExt} ->
      if attach
        then attachFile ap fmt
        else do
          mt <- Table.fileWith ap reader duckdbExt
          pure $ mt >>= \t -> View.fromTbl t path_ 0 V.empty 0
    Nothing -> do
      mt <- Table.fromFile ap
      pure $ mt >>= \t -> View.fromTbl t path_ 0 V.empty 0
