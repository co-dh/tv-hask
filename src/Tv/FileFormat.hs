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
import Control.Monad (void)
import Data.Char (toLower)
import Data.List (find, isSuffixOf)
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
stripGz p | ".gz" `isSuffixOf` p = take (length p - 3) p
          | otherwise = p

-- | Find format by file extension (handles .gz: strip suffix, match inner ext).
findFormat :: String -> Maybe Format
findFormat path = let p = map toLower (stripGz path)
                  in find (\fmt -> any (`isSuffixOf` p) (fmtExts fmt)) formats

-- | Is file a recognized data format?
isDataFile :: String -> Bool
isDataFile p = case findFormat p of Just _ -> True; Nothing -> False

-- | Is file a .txt (or .txt.gz)?
isTxtFile :: String -> Bool
isTxtFile p = ".txt" `isSuffixOf` map toLower (stripGz p)

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
  let gz = ".gz" `isSuffixOf` path
      esc = T.unpack (T.replace "'" "'\\''" (T.pack path))
  hasBat <- cmdExists "bat"
  if gz then spawn "sh" ["-c", "zcat '" ++ esc ++ "' | " ++
           (if hasBat then "bat --paging=always" else "less")]
  else if hasBat then spawn "bat" ["--paging=always", path]
  else spawn "less" [path]
  where
    cmdExists cmd = do
      (ec, _, _) <- readProcessWithExitCode "which" [cmd] ""
      pure (ec == ExitSuccess)

-- | Run a DuckDB query and swallow any exception.
duckTry :: DB.Conn -> Text -> IO ()
duckTry conn q = void (try (DB.query conn q) :: IO (Either SomeException DB.Result))

-- | Load a DuckDB extension. Tries LOAD first (uses cached extension);
-- only runs INSTALL if LOAD fails. Avoids network retry storms when the
-- extension is already present but extensions.duckdb.org is unreachable.
loadDuckExt :: DB.Conn -> String -> IO ()
loadDuckExt _ "" = pure ()
loadDuckExt conn ext = do
  r <- try (DB.query conn (T.pack ("LOAD " ++ ext))) :: IO (Either SomeException DB.Result)
  case r of
    Right _ -> pure ()
    Left _  -> duckTry conn (T.pack ("INSTALL " ++ ext)) *> duckTry conn (T.pack ("LOAD " ++ ext))

-- | Build the SELECT SQL for reading a file with optional reader function.
fileReadSql :: FilePath -> String -> Text
fileReadSql path reader =
  let esc = T.replace "'" "''" (T.pack path)
  in if null reader
     then "SELECT * FROM '" <> esc <> "' LIMIT 1000"
     else "SELECT * FROM " <> T.pack reader <> "('" <> esc <> "') LIMIT 1000"

-- | Open a file with a specific reader. Returns Nothing on error or empty result.
fromFileWith :: FilePath -> String -> String -> IO (Maybe TblOps)
fromFileWith path reader ext =
  either (const Nothing) keepNonEmpty <$> (try go :: IO (Either SomeException TblOps))
  where
    keepNonEmpty ops = if _tblNRows ops > 0 then Just ops else Nothing
    go = do
      conn <- DB.connect ":memory:"
      loadDuckExt conn ext
      DB.query conn (fileReadSql path reader) >>= DB.mkDbOps

-- | Try to ingest as CSV via DuckDB read_csv (handles .gz). Nothing = not valid CSV.
tryReadCsv :: FilePath -> IO (Maybe View)
tryReadCsv path = do
  ap <- absPath path
  r <- try (fromFileWith ap "read_csv" "") :: IO (Either SomeException (Maybe TblOps))
  case r of
    Left e -> Nothing <$ logWrite "tryReadCsv" (path ++ ": " ++ displayException e)
    Right m -> pure (m >>= \ops -> fromTbl ops (T.pack path) 0 V.empty 0)

-- | ATTACH database file and list its tables as a folder view.
attachFile :: FilePath -> Format -> IO (Maybe View)
attachFile ap fmt = (try go :: IO (Either SomeException (Maybe View))) >>= either logFail pure
  where
    logFail e = Nothing <$ logWrite "attachFile" (ap ++ ": " ++ displayException e)
    go = do
      conn <- DB.connect ":memory:"
      loadDuckExt conn (fmtDuckdbExt fmt)
      let typClause = if null (fmtAttachType fmt) then "" else "TYPE " ++ fmtAttachType fmt ++ ", "
          escAp = T.unpack (T.replace "'" "''" (T.pack ap))
      duckTry conn "DETACH DATABASE IF EXISTS extdb"
      _ <- DB.query conn (T.pack ("ATTACH '" ++ escAp ++ "' AS extdb (" ++ typClause ++ "READ_ONLY)"))
      res <- DB.query conn "SELECT table_name AS name FROM information_schema.tables WHERE table_catalog='extdb'"
      ops <- DB.mkDbOps res
      let disp = T.pack (last (filter (not . null) (splitOn '/' ap)))
          mkFld v = v { _vNav = (_vNav v) { _nsVkind = VFld (T.pack ap) 1 }, _vDisp = disp }
      pure $ if _tblNRows ops <= 0 then Nothing
             else mkFld <$> fromTbl ops (T.pack ap) 0 (V.singleton "name") 0
    splitOn c = foldr step [[]]
      where step x acc | x == c    = [] : acc
                       | otherwise = case acc of (h:t) -> (x:h):t; [] -> [[x]]

-- | Open any supported data file as a View (attach for DB, reader for data files).
openFile :: FilePath -> IO (Maybe View)
openFile path = do
  ap <- absPath path
  let toView ops = fromTbl ops (T.pack path) 0 V.empty 0
      readWith reader ext = (>>= toView) <$> fromFileWith ap reader ext
  case findFormat path of
    Just fmt | fmtAttach fmt -> attachFile ap fmt
    Just fmt                 -> readWith (fmtReader fmt) (fmtDuckdbExt fmt)
    Nothing                  -> readWith "" ""
