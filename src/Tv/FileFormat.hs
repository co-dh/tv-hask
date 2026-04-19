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
  , isTxt
  , viewFile
  , readCsv
  , openFile
  ) where

import Codec.Compression.GZip (decompress)
import Control.Exception (SomeException, try)
import Control.Monad (void)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Lazy.Char8 as LBSC
import Data.Maybe (isJust)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Directory (canonicalizePath, findExecutable)
import System.Exit (ExitCode(..))
import System.IO (hClose, stdout)
import System.Process
  ( CreateProcess(..), StdStream(..), proc
  , createProcess, waitForProcess, readProcessWithExitCode
  )

import qualified Tv.Term as Term
import qualified Tv.Log as Log
import Tv.View (View)
import qualified Tv.View as View
import Tv.Types (escSql)
import qualified Tv.Types as Types
import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable)
import qualified Tv.Data.DuckDB.Table as Table
import Optics.Core ((&), (.~))
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
  in V.find (\fmt -> V.any (`T.isSuffixOf` p) (exts fmt)) formats

-- | Is file a recognized data format?
isData :: Text -> Bool
isData = isJust . find

-- | Is file a .txt (or .txt.gz)?
isTxt :: Text -> Bool
isTxt p = T.isSuffixOf ".txt" (stripGz p)

-- | Resolve absolute path; falls back to the input on error (missing file).
absPath :: Text -> IO Text
absPath path_ = do
  r <- try (canonicalizePath (T.unpack path_)) :: IO (Either SomeException FilePath)
  pure $ case r of
    Right p -> T.pack p
    Left _  -> path_

-- | Spawn interactive process inheriting terminal streams (bat/less)
spawn :: String -> [String] -> IO ()
spawn cmd args = do
  (_, _, _, ph) <- createProcess
    (proc cmd args) { std_in = Inherit, std_out = Inherit, std_err = Inherit }
  void (waitForProcess ph)

-- | Spawn process, feed lazy bytestring into stdin, inherit stdout/stderr
pipeIn :: String -> [String] -> LBS.ByteString -> IO ExitCode
pipeIn cmd args bs = do
  (Just hin, _, _, ph) <- createProcess
    (proc cmd args) { std_in = CreatePipe, std_out = Inherit, std_err = Inherit }
  LBS.hPut hin bs
  hClose hin
  waitForProcess ph

-- | Read .gz file and return decompressed bytes
readGz :: Text -> IO LBS.ByteString
readGz path_ = decompress <$> LBS.readFile (T.unpack path_)

-- | View file with bat (if available) or less. .gz decompressed via zlib.
viewFile :: Bool -> Text -> IO ()
viewFile tm path_ = do
  let gz = T.isSuffixOf ".gz" path_
  if tm
    then do
      r <- try action :: IO (Either SomeException (ExitCode, String, String))
      case r of
        Right (ExitSuccess, out, _) -> TIO.hPutStr stdout (T.pack out)
        _ ->
          if gz
            then TIO.hPutStr stdout . T.pack . LBSC.unpack =<< readGz path_
            else TIO.hPutStr stdout =<< TIO.readFile (T.unpack path_)
    else do
      Term.shutdown
      hasBat <- isJust <$> findExecutable "bat"
      let viewer = if hasBat then "bat" else "less"
          viewerArgs = if hasBat then ["--paging=always"] else []
      if gz
        then do
          bs <- readGz path_
          void (pipeIn viewer viewerArgs bs)
        else spawn viewer (viewerArgs <> [T.unpack path_])
      void Term.init
  where
    action =
      if T.isSuffixOf ".gz" path_
        then do
          bs <- readGz path_
          -- bat reads decompressed bytes from stdin; capture its stdout
          readProcessWithExitCode "bat" ["--paging=never", "--plain"] (LBSC.unpack bs)
        else readProcessWithExitCode "bat" ["--paging=never", "--plain", T.unpack path_] ""

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
attachFile ap fmt = do
  Table.loadExt (duckdbExt fmt)
  let typClause = if T.null (attachType fmt) then "" else "TYPE " <> attachType fmt <> ", "
  _ <- Conn.query "DETACH DATABASE IF EXISTS extdb"
  _ <- Conn.query ("ATTACH '" <> escSql ap <> "' AS extdb (" <> typClause <> "READ_ONLY)")
  mQr <- Table.prqlQuery Prql.ducktabs
  case mQr of
    Nothing -> pure Nothing
    Just qr -> do
      total <- Conn.nrows qr
      let totalN = fromIntegral total :: Int
      if totalN == 0
        then pure Nothing
        else do
          adbc <- Table.ofResult qr (Prql.defaultQuery { Prql.base = Prql.ducktabs }) totalN
          let disp_ = case reverse (T.splitOn "/" ap) of
                        (x:_) -> x
                        []    -> ap
          pure $ fmap
            (\v -> v & #vkind .~ Types.VkFld ap 1 & #disp .~ disp_)
            $ View.fromTbl adbc ap 0 (V.singleton "name") 0

-- | Open any supported data file as a View (attach for DB, reader for data files)
openFile :: Text -> IO (Maybe (View AdbcTable))
openFile path_ = do
  ap <- absPath path_
  case find path_ of
    Just fmt ->
      if attach fmt
        then attachFile ap fmt
        else do
          mt <- Table.fileWith ap (reader fmt) (duckdbExt fmt)
          pure $ mt >>= \t -> View.fromTbl t path_ 0 V.empty 0
    Nothing -> do
      mt <- Table.fromFile ap
      pure $ mt >>= \t -> View.fromTbl t path_ 0 V.empty 0
