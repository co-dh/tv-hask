{-# LANGUAGE ScopedTypeVariables #-}
-- | FileFormat: file format detection, opening, and viewing.
-- Maps file extensions -> DuckDB readers, handles ATTACH for database files.
module Tv.FileFormat
  ( Format (..)
  , formats
  , findFormat
  , isDataFile
  , isTxtFile
  , absPath
  , viewFile
  , tryReadCsv
  , openFile
  ) where

import Control.Exception (SomeException, try, displayException)
import Data.Char (toLower)
import Data.List (find)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Exit (ExitCode(..))
import System.Process (readProcessWithExitCode, CreateProcess(..), StdStream(..), createProcess, proc, waitForProcess)

import Tv.Types
import Tv.View (View(..), fromTbl)
import qualified Tv.Data.DuckDB as DB
import Tv.Util (logWrite)

-- ============================================================================
-- Format
-- ============================================================================

-- | How DuckDB should handle a file extension.
data Format = Format
  { fmtExts       :: ![String]  -- file extensions (e.g. [".csv", ".parquet"])
  , fmtReader     :: !String    -- DuckDB reader function. Empty = auto-detect.
  , fmtDuckdbExt  :: !String    -- DuckDB extension to INSTALL/LOAD. Empty = none.
  , fmtAttach     :: !Bool      -- true = ATTACH as database, list tables
  , fmtAttachType :: !String    -- ATTACH TYPE clause (e.g. "SQLITE"). Empty = native.
  } deriving (Show)

-- | All file formats supported by DuckDB.
formats :: [Format]
formats =
  [ Format [".csv", ".parquet", ".json", ".jsonl", ".ndjson"] "" "" False ""
  , Format [".arrow", ".feather"] "read_arrow" "arrow" False ""
  , Format [".xlsx", ".xls"]      "read_xlsx"  "excel" False ""
  , Format [".avro"]              "read_avro"  "avro"  False ""
  , Format [".duckdb", ".db"]     ""           ""      True  ""
  , Format [".sqlite", ".sqlite3"] ""          "sqlite" True "SQLITE"
  ]

-- | Strip .gz suffix for extension matching.
stripGz :: String -> String
stripGz p | hasSuffix ".gz" p = take (length p - 3) p
          | otherwise = p
  where hasSuffix sfx s = drop (length s - length sfx) s == sfx

-- | Find format by file extension (handles .gz: strip suffix, match inner ext).
findFormat :: String -> Maybe Format
findFormat path = let p = map toLower (stripGz path)
                  in find (\fmt -> any (`isSuffixOf'` p) (fmtExts fmt)) formats
  where isSuffixOf' sfx s = drop (length s - length sfx) s == sfx

-- | Is file a recognized data format?
isDataFile :: String -> Bool
isDataFile p = case findFormat p of Just _ -> True; Nothing -> False

-- | Is file a .txt (or .txt.gz)?
isTxtFile :: String -> Bool
isTxtFile p = isSuffixOf' ".txt" (map toLower (stripGz p))
  where isSuffixOf' sfx s = drop (length s - length sfx) s == sfx

-- | Resolve absolute path via realpath.
absPath :: FilePath -> IO FilePath
absPath path = do
  (ec, out, _) <- readProcessWithExitCode "realpath" [path] ""
  pure $ if ec == ExitSuccess then trimEnd out else path
  where trimEnd = reverse . dropWhile (`elem` (" \t\n\r" :: String)) . reverse

-- | Spawn interactive process (bat/less/zcat).
spawn :: FilePath -> [String] -> IO ()
spawn cmd args = do
  (_, _, _, ph) <- createProcess (proc cmd args)
    { std_in = Inherit, std_out = Inherit, std_err = Inherit }
  _ <- waitForProcess ph
  pure ()

-- | View file with bat (if available) or less. .gz files piped through zcat.
-- NOTE: Term.shutdown/init calls should be done by the caller (App layer)
-- since the Haskell port uses brick which manages terminal state.
viewFile :: FilePath -> IO ()
viewFile path = do
  let gz = hasSuffix ".gz" path
      esc = replace' "'" "'\\''" path
  hasBat <- cmdExists "bat"
  if gz then spawn "sh" ["-c", "zcat '" ++ esc ++ "' | " ++
           (if hasBat then "bat --paging=always" else "less")]
  else if hasBat then spawn "bat" ["--paging=always", path]
  else spawn "less" [path]
  where
    hasSuffix sfx s = drop (length s - length sfx) s == sfx
    replace' _ _ [] = []
    replace' from to s@(c:cs)
      | take (length from) s == from = to ++ replace' from to (drop (length from) s)
      | otherwise = c : replace' from to cs
    cmdExists cmd = do
      (ec, _, _) <- readProcessWithExitCode "which" [cmd] ""
      pure (ec == ExitSuccess)

-- | Load a DuckDB extension (INSTALL + LOAD). No-op if name is empty.
loadDuckExt :: DB.Conn -> String -> IO ()
loadDuckExt _ "" = pure ()
loadDuckExt conn ext = do
  _ <- try (DB.query conn (T.pack ("INSTALL " ++ ext))) :: IO (Either SomeException DB.Result)
  _ <- try (DB.query conn (T.pack ("LOAD " ++ ext)))    :: IO (Either SomeException DB.Result)
  pure ()

-- | Build the SELECT SQL for reading a file with optional reader function.
fileReadSql :: FilePath -> String -> Text
fileReadSql path reader =
  let esc = T.replace "'" "''" (T.pack path)
  in if null reader
     then "SELECT * FROM '" <> esc <> "' LIMIT 1000"
     else "SELECT * FROM " <> T.pack reader <> "('" <> esc <> "') LIMIT 1000"

-- | Open a file with a specific reader. Returns Nothing on error or empty result.
fromFileWith :: FilePath -> String -> String -> IO (Maybe TblOps)
fromFileWith path reader ext = do
  r <- try go :: IO (Either SomeException TblOps)
  case r of
    Left _  -> pure Nothing
    Right ops | _tblNRows ops <= 0 -> pure Nothing
    Right ops -> pure (Just ops)
  where
    go = do
      conn <- DB.connect ":memory:"
      loadDuckExt conn ext
      res <- DB.query conn (fileReadSql path reader)
      DB.mkDbOps res

-- | Try to ingest as CSV via DuckDB read_csv (handles .gz). Nothing = not valid CSV.
tryReadCsv :: FilePath -> IO (Maybe View)
tryReadCsv path = do
  ap <- absPath path
  r <- try (fromFileWith ap "read_csv" "") :: IO (Either SomeException (Maybe TblOps))
  case r of
    Left e -> do
      logWrite "tryReadCsv" (path ++ ": " ++ displayException e)
      pure Nothing
    Right Nothing -> pure Nothing
    Right (Just ops) -> pure (fromTbl ops (T.pack path) 0 V.empty 0)

-- | ATTACH database file and list its tables as a folder view.
attachFile :: FilePath -> Format -> IO (Maybe View)
attachFile ap fmt = do
  r <- try go :: IO (Either SomeException (Maybe View))
  case r of
    Left e -> do
      logWrite "attachFile" (ap ++ ": " ++ displayException e)
      pure Nothing
    Right v -> pure v
  where
    go = do
      conn <- DB.connect ":memory:"
      loadDuckExt conn (fmtDuckdbExt fmt)
      let typClause = if null (fmtAttachType fmt) then "" else "TYPE " ++ fmtAttachType fmt ++ ", "
          escAp = T.unpack (T.replace "'" "''" (T.pack ap))
      _ <- try (DB.query conn "DETACH DATABASE IF EXISTS extdb") :: IO (Either SomeException DB.Result)
      _ <- DB.query conn (T.pack ("ATTACH '" ++ escAp ++ "' AS extdb (" ++ typClause ++ "READ_ONLY)"))
      -- list tables from attached db
      res <- DB.query conn "SELECT table_name AS name FROM information_schema.tables WHERE table_catalog='extdb'"
      ops <- DB.mkDbOps res
      if _tblNRows ops <= 0 then pure Nothing
      else do
        let disp = last (splitPath ap)
        case fromTbl ops (T.pack ap) 0 (V.singleton "name") 0 of
          Nothing -> pure Nothing
          Just v  -> pure $ Just v { _vNav = (_vNav v) { _nsVkind = VFld (T.pack ap) 1 }
                                   , _vDisp = T.pack disp }
    splitPath = filter (not . null) . foldr step [[]]
      where step '/' acc = [] : acc; step c (x:xs) = (c:x) : xs; step _ [] = []

-- | Open any supported data file as a View (attach for DB, reader for data files).
openFile :: FilePath -> IO (Maybe View)
openFile path = do
  ap <- absPath path
  case findFormat path of
    Just fmt | fmtAttach fmt -> attachFile ap fmt
    Just fmt -> do
      mops <- fromFileWith ap (fmtReader fmt) (fmtDuckdbExt fmt)
      pure (mops >>= \ops -> fromTbl ops (T.pack path) 0 V.empty 0)
    Nothing -> do
      mops <- fromFileWith ap "" ""
      pure (mops >>= \ops -> fromTbl ops (T.pack path) 0 V.empty 0)
