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
  , isDataFile
  , isTxtFile
  , viewFile
  , tryReadCsv
  , openFile
  ) where

import Control.Exception (SomeException, try)
import Control.Monad (void)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Exit (ExitCode(..))
import System.IO (stdout)
import System.Process
  ( CreateProcess(..), StdStream(..), proc
  , createProcess, waitForProcess, readProcessWithExitCode
  )

import qualified Tv.Fzf as Fzf
import qualified Tv.Term as Term
import qualified Tv.Util as Log
import Tv.View (View)
import qualified Tv.View as View
import Tv.Types (escSql)
import qualified Tv.Types as Types
import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Data.ADBC.Prql as Prql
import Tv.Data.ADBC.Table (AdbcTable)
import qualified Tv.Data.ADBC.Table as Table
import Tv.Data.ADBC.Ops ()  -- TblOps AdbcTable instance
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
stripGz p = if T.isSuffixOf ".gz" p then T.take (T.length p - 3) p else p

-- | Find format by file extension (handles .gz: strip suffix, match inner ext)
find :: Text -> Maybe Format
find path_ =
  let p = stripGz path_
  in V.find (\fmt -> V.any (`T.isSuffixOf` p) (exts fmt)) formats

-- | Is file a recognized data format?
isDataFile :: Text -> Bool
isDataFile p = case find p of { Just _ -> True; Nothing -> False }

-- | Is file a .txt (or .txt.gz)?
isTxtFile :: Text -> Bool
isTxtFile p = T.isSuffixOf ".txt" (stripGz p)

-- | Resolve absolute path via realpath
absPath :: Text -> IO Text
absPath path_ = do
  (ec, out, _) <- readProcessWithExitCode "realpath" [T.unpack path_] ""
  pure $ case ec of
    ExitSuccess -> T.strip (T.pack out)
    _           -> path_

-- | Spawn interactive process (bat/less/zcat)
spawn :: String -> [String] -> IO ()
spawn cmd args = do
  (_, _, _, ph) <- createProcess
    (proc cmd args) { std_in = Inherit, std_out = Inherit, std_err = Inherit }
  void (waitForProcess ph)

-- | View file with bat (if available) or less. .gz files piped through zcat.
viewFile :: Text -> IO ()
viewFile path_ = do
  let gz = T.isSuffixOf ".gz" path_
      esc = T.replace "'" "'\\''" path_
  tm <- Fzf.getTestMode
  if tm
    then do
      (ec, out, _) <-
        if gz
          then readProcessWithExitCode "sh" ["-c", T.unpack ("zcat '" <> esc <> "' | bat --paging=never --plain")] ""
          else readProcessWithExitCode "bat" ["--paging=never", "--plain", T.unpack path_] ""
      case ec of
        ExitSuccess -> TIO.hPutStr stdout (T.pack out)
        _ ->
          if gz
            then do
              (_, zout, _) <- readProcessWithExitCode "zcat" [T.unpack path_] ""
              TIO.hPutStr stdout (T.pack zout)
            else TIO.hPutStr stdout =<< TIO.readFile (T.unpack path_)
    else do
      Term.shutdown
      (whichEc, _, _) <- readProcessWithExitCode "which" ["bat"] ""
      let hasBat = whichEc == ExitSuccess
      if gz
        then spawn "sh" ["-c", T.unpack ("zcat '" <> esc <> "' | "
               <> (if hasBat then "bat --paging=always" else "less"))]
        else if hasBat
          then spawn "bat" ["--paging=always", T.unpack path_]
          else spawn "less" [T.unpack path_]
      void Term.init

-- | Try to ingest as CSV via DuckDB read_csv (handles .gz). Nothing = not valid CSV.
tryReadCsv :: Text -> IO (Maybe (View AdbcTable))
tryReadCsv path_ = do
  r <- try action :: IO (Either SomeException (Maybe (View AdbcTable)))
  case r of
    Right v -> pure v
    Left e  -> do
      Log.write "tryReadCsv" (path_ <> ": " <> T.pack (show e))
      pure Nothing
  where
    action = do
      ap <- absPath path_
      mt <- Table.fromFileWith ap "read_csv" ""
      pure (mt >>= \t -> View.fromTbl t path_ 0 V.empty 0)

-- | ATTACH database file and list its tables as a folder view
attachFile :: Text -> Format -> IO (Maybe (View AdbcTable))
attachFile ap fmt = do
  Table.loadDuckExt (duckdbExt fmt)
  let typClause = if T.null (attachType fmt) then "" else "TYPE " <> attachType fmt <> ", "
  _ <- Adbc.query "DETACH DATABASE IF EXISTS extdb"
  _ <- Adbc.query ("ATTACH '" <> escSql ap <> "' AS extdb (" <> typClause <> "READ_ONLY)")
  mQr <- Table.prqlQuery Prql.ducktabs
  case mQr of
    Nothing -> pure Nothing
    Just qr -> do
      total <- Adbc.nrows qr
      let totalN = fromIntegral total :: Int
      if totalN == 0
        then pure Nothing
        else do
          adbc <- Table.ofQueryResult qr (Prql.defaultQuery { Prql.base = Prql.ducktabs }) totalN
          let disp_ = case reverse (T.splitOn "/" ap) of
                        (x:_) -> x
                        []    -> ap
          pure $ fmap
            (\v -> v { View.vkind = Types.VkFld ap 1, View.disp = disp_ })
            (View.fromTbl adbc ap 0 (V.singleton "name") 0)

-- | Open any supported data file as a View (attach for DB, reader for data files)
openFile :: Text -> IO (Maybe (View AdbcTable))
openFile path_ = do
  ap <- absPath path_
  case find path_ of
    Just fmt ->
      if attach fmt
        then attachFile ap fmt
        else do
          mt <- Table.fromFileWith ap (reader fmt) (duckdbExt fmt)
          pure (mt >>= \t -> View.fromTbl t path_ 0 V.empty 0)
    Nothing -> do
      mt <- Table.fromFile ap
      pure (mt >>= \t -> View.fromTbl t path_ 0 V.empty 0)
