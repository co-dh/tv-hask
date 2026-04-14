{-# LANGUAGE ScopedTypeVariables #-}
-- | Tv.Loader — file/DB/folder open pipeline.
--
-- Resolves a path to a 'View': directories → folder TblOps via
-- 'Tv.Folder'; files → DuckDB read_* / ATTACH via
-- 'Tv.FileFormat' when applicable, else a PRQL chain through
-- 'Tv.Data.DuckDB'; database files → attached table picker via
-- 'enterDbTable'. Gzip + plain-text fallbacks included.
--
-- 'openPath' is an 'Eff' handler helper that pushes the new view
-- onto the stack; 'openInitialView' is the CLI entry that just
-- returns a 'View' without touching state.
module Tv.Loader
  ( openPath
  , openInitialView
  , loadFile
  , loadFolder
  , enterDbTable
  , viewFileFallback
  , mkTextViewOps
  , fileQuery
  , isGz
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import Control.Exception (try, SomeException, displayException)
import Control.Monad (when)
import Data.Char (toLower)
import Data.List (isSuffixOf)
import System.Directory
  ( doesDirectoryExist, canonicalizePath, getTemporaryDirectory )
import System.FilePath (takeExtension, (</>))
import System.Process (readProcessWithExitCode)
import System.Exit (ExitCode(..))
import Optics.Core ((^.), (%), (&), (.~))

import Tv.Types
import Tv.View (View, fromTbl, vNav, vsPush)
import qualified Tv.Data.DuckDB as DB
import qualified Tv.Data.Prql as Prql
import qualified Tv.Data.Text as DataText
import qualified Tv.FileFormat as FF
import qualified Tv.Folder as Folder
import Tv.Eff (Eff, AppEff, runEff, tryEitherE, liftIO, (%=), (.=))
import Tv.Handler (refresh, setMsg)
import Tv.Render (AppState, asStack)

-- | Pick the DuckDB SQL to materialize a file. Unknown extensions fall
-- back to @SELECT * FROM '...'@ (DuckDB auto-detect).
fileQuery :: FilePath -> Text
fileQuery p =
  let esc = T.replace "'" "''" (T.pack p)
      ext = map toLower (takeExtension p)
      ext2 = case ext of
        ".gz" -> map toLower (takeExtension (take (length p - 3) p))
        _     -> ext
      reader = case ext2 of
        ".parquet" -> "read_parquet"
        ".csv"     -> "read_csv_auto"
        ".tsv"     -> "read_csv_auto"
        ".json"    -> "read_json_auto"
        ".ndjson"  -> "read_json_auto"
        ".jsonl"   -> "read_json_auto"
        _          -> ""
  in if null reader
       then "SELECT * FROM '" <> esc <> "' LIMIT 1000"
       else "SELECT * FROM " <> T.pack reader <> "('" <> esc <> "') LIMIT 1000"

-- | Load a regular file as a TblOps via DuckDB. Handles:
-- 1. .txt files → parse through Tv.Data.Text space-separated parser
-- 2. Recognized formats (parquet, json, xlsx, etc.) → FileFormat.openFile
-- 3. Unknown → PRQL backtick path (DuckDB auto-detect)
loadFile :: FilePath -> IO (Either String TblOps)
loadFile p
  | isGz p = loadGzFile p
  | FF.isTxtFile p = loadTextFile p
  | otherwise = do
      r <- try $ do
        conn <- DB.connect ":memory:"
        let q = Prql.Query { Prql.base = "from `" <> T.pack p <> "`", Prql.ops = V.empty }
        total <- DB.queryCount conn q
        mOps <- DB.requery conn q (max total 1)
        case mOps of
          Just ops -> pure ops
          Nothing -> do
            res <- DB.query conn (fileQuery p)
            DB.mkDbOps res
      pure $ case r of
        Left (e :: SomeException) -> Left (displayException e)
        Right ops -> Right ops

-- | Check if file is .gz
isGz :: FilePath -> Bool
isGz p = ".gz" `isSuffixOf` map toLower p

-- | Load .gz file: decompress with zcat, then try CSV → text → viewFile fallback.
-- For .csv.gz, DuckDB handles natively via loadFile. For .txt.gz, decompress first.
loadGzFile :: FilePath -> IO (Either String TblOps)
loadGzFile p = do
  r <- try $ do
    conn <- DB.connect ":memory:"
    let q = Prql.Query { Prql.base = "from `" <> T.pack p <> "`", Prql.ops = V.empty }
    total <- DB.queryCount conn q
    mOps <- DB.requery conn q (max total 1)
    case mOps of
      Just ops -> pure ops
      Nothing -> fail "DuckDB can't read gz"
  case r of
    Right ops -> pure (Right ops)
    Left (_ :: SomeException) -> do
      r2 <- try $ do
        (ec, out, _) <- readProcessWithExitCode "zcat" [p] ""
        when (ec /= ExitSuccess) $ fail "zcat failed"
        let content = T.pack out
        tmpDir <- getTemporaryDirectory
        let tmpPath = tmpDir </> "tv_gz_tmp.csv"
        TIO.writeFile tmpPath content
        conn <- DB.connect ":memory:"
        let q = Prql.Query { Prql.base = "from `" <> T.pack tmpPath <> "`", Prql.ops = V.empty }
        total <- DB.queryCount conn q
        if total > 0 then do
          mOps <- DB.requery conn q total
          case mOps of
            Just ops -> pure ops
            Nothing -> fail "gz csv failed"
        else case DataText.fromText content of
          Right tsv -> do
            let tmpPath2 = tmpDir </> "tv_gz_text.tsv"
            TIO.writeFile tmpPath2 tsv
            let q2 = Prql.Query { Prql.base = "from `" <> T.pack tmpPath2 <> "`", Prql.ops = V.empty }
            total2 <- DB.queryCount conn q2
            mOps2 <- DB.requery conn q2 (max total2 1)
            case mOps2 of
              Just ops -> pure ops
              Nothing -> fail "gz text failed"
          Left _ -> fail "not CSV or text"
      pure $ case r2 of
        Left (e :: SomeException) -> Left (displayException e)
        Right ops -> Right ops

-- | Create a read-only TblOps that just shows text lines (viewFile fallback).
-- Single column "text" with one row per line.
mkTextViewOps :: Vector (Vector Text) -> TblOps
mkTextViewOps rows =
  let nr = V.length rows; nc = 1
      cols = V.singleton "text"
      ops = TblOps
        { _tblNRows = nr, _tblColNames = cols, _tblTotalRows = nr
        , _tblQueryOps = V.empty
        , _tblFilter = \_ -> pure Nothing, _tblDistinct = \_ -> pure V.empty
        , _tblFindRow = \_ _ _ _ -> pure Nothing
        , _tblRender = \_ -> pure V.empty, _tblGetCols = \_ _ _ -> pure V.empty
        , _tblColType = \_ -> CTStr
        , _tblBuildFilter = \_ _ _ _ -> T.empty, _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr = \r c -> if r >= 0 && r < nr && c >= 0 && c < nc
                                then pure ((rows V.! r) V.! c) else pure T.empty
        , _tblFetchMore = pure Nothing
        , _tblHideCols = \_ -> pure ops, _tblSortBy = \_ _ -> pure ops
        }
  in ops

-- | Load .txt file: parse space-separated text, then load as CSV via DuckDB.
loadTextFile :: FilePath -> IO (Either String TblOps)
loadTextFile p = do
  r <- try $ do
    content <- TIO.readFile p
    case DataText.fromText content of
      Left e -> fail e
      Right tsv -> do
        conn <- DB.connect ":memory:"
        tmpDir <- getTemporaryDirectory
        let tmpPath = tmpDir </> "tv_text_tmp.tsv"
        TIO.writeFile tmpPath tsv
        let q = Prql.Query { Prql.base = "from `" <> T.pack tmpPath <> "`"
                            , Prql.ops = V.empty }
        total <- DB.queryCount conn q
        mOps <- DB.requery conn q (max total 1)
        case mOps of
          Just ops -> pure ops
          Nothing -> fail "text parse: DuckDB load failed"
  pure $ case r of
    Left (e :: SomeException) -> Left (displayException e)
    Right ops -> Right ops

-- | Load a directory as a folder TblOps. The view path is canonicalized
-- so folderEnter can join with entry names unambiguously.
loadFolder :: FilePath -> IO (TblOps, FilePath)
loadFolder p = do
  ap <- canonicalizePath p
  ops <- runEff (Folder.listFolder ap)
  pure (ops, ap)

-- | Open any path: directory → folder view, otherwise → file view. On
-- success the new view is pushed on the stack; on failure a status
-- message is set. Used both for in-app navigation (folder enter) and
-- the initial CLI arg fallback.
openPath :: FilePath -> Eff AppEff Bool
openPath p = do
  isDir <- liftIO (doesDirectoryExist p)
  if isDir
    then do
      (ops, absPath) <- liftIO (loadFolder p)
      let disp = T.pack absPath
          vk = VFld disp 1
      case (\v -> v & vNav % nsVkind .~ vk) <$> fromTbl ops disp 0 V.empty 0 of
        Nothing -> setMsg ("empty folder: " <> T.pack p)
        Just v  -> asStack %= vsPush v >> refresh
    else do
      let needsFF = case FF.findFormat p of
            Just fmt -> FF.fmtAttach fmt || not (null (FF.fmtDuckdbExt fmt))
            Nothing  -> False
      if needsFF
        then do
          mv <- tryEitherE (FF.openFile p)
          case mv of
            Right (Just v) -> asStack %= vsPush v >> refresh
            _ -> setMsg ("open failed: " <> T.pack p)
        else do
          r <- liftIO (loadFile p)
          case r of
            Left e   -> setMsg ("open failed: " <> T.pack e)
            Right ops -> case fromTbl ops (T.pack p) 0 V.empty 0 of
              Nothing -> setMsg ("empty: " <> T.pack p)
              Just v  -> asStack %= vsPush v >> refresh

-- | Enter a table within an attached database file.
enterDbTable :: FilePath -> Text -> Eff AppEff Bool
enterDbTable dbPath tblName = do
  r <- tryEitherE (liftIO go)
  case r of
    Right (Just v) -> asStack %= vsPush v >> refresh
    Right Nothing  -> setMsg ("empty table: " <> tblName)
    Left e         -> setMsg ("open table failed: " <> T.pack (displayException e))
  where
    go = do
      conn <- DB.connect ":memory:"
      let ext = map toLower (takeExtension dbPath)
          escPath = T.unpack (T.replace "'" "''" (T.pack dbPath))
          isSqlite = ext `elem` [".sqlite", ".sqlite3"]
      when isSqlite $ do
        _ <- try (DB.query conn "INSTALL sqlite") :: IO (Either SomeException DB.Result)
        _ <- try (DB.query conn "LOAD sqlite") :: IO (Either SomeException DB.Result)
        pure ()
      _ <- try (DB.query conn "DETACH DATABASE IF EXISTS extdb") :: IO (Either SomeException DB.Result)
      let typClause = if isSqlite then "TYPE SQLITE, " else ""
      _ <- DB.query conn (T.pack ("ATTACH '" ++ escPath ++ "' AS extdb (" ++ typClause ++ "READ_ONLY)"))
      let escTbl = T.replace "\"" "\"\"" tblName
          tmpName = "tc_tbl_" <> T.filter (/= '"') tblName
      _ <- DB.query conn ("CREATE TEMP TABLE \"" <> tmpName <> "\" AS SELECT * FROM extdb.\"" <> escTbl <> "\"")
      let q = Prql.Query { Prql.base = "from " <> tmpName, Prql.ops = V.empty }
      total <- DB.queryCount conn q
      mOps <- DB.requery conn q (max total 1)
      case mOps of
        Nothing -> pure Nothing
        Just ops ->
          let path = T.pack dbPath <> " " <> tblName
              grp = V.singleton "name"
              grp' = V.filter (`V.elem` (ops ^. tblColNames)) grp
          in pure (fromTbl ops path 0 grp' 0)

-- | Open a path as a View (no state mutation). Used at startup to seed
-- the initial stack before the Brick loop starts.
openInitialView :: FilePath -> IO View
openInitialView path = do
  isDir <- doesDirectoryExist path
  if isDir then do
    (ops, absPath) <- loadFolder path
    case fromTbl ops (T.pack absPath) 0 V.empty 0 of
      Nothing -> fail ("empty folder: " ++ path)
      Just v  -> pure (v & vNav % nsVkind .~ VFld (T.pack absPath) 1)
  else do
    let needsFF = case FF.findFormat path of
          Just fmt -> FF.fmtAttach fmt || not (null (FF.fmtDuckdbExt fmt))
          Nothing  -> False
    if needsFF
      then do
        mv <- runEff (FF.openFile path)
        case mv of
          Just v -> pure v
          Nothing -> fail ("open failed: " ++ path)
      else do
        r <- loadFile path
        case r of
          Left _ -> viewFileFallback path
          Right ops
            | (ops ^. tblNRows) > 0 -> case fromTbl ops (T.pack path) 0 V.empty 0 of
                Nothing -> fail ("empty: " ++ path)
                Just v  -> pure v
            | otherwise -> viewFileFallback path

-- | viewFile fallback: show decompressed/raw text as a text table.
-- Marks the view with VViewFile kind so renderText can omit table chrome.
viewFileFallback :: FilePath -> IO View
viewFileFallback path = do
  content <- if isGz path
    then do (_, out, _) <- readProcessWithExitCode "zcat" [path] ""
            pure (T.pack out)
    else TIO.readFile path
  let ls = filter (not . T.null) (T.lines content)
  if null ls then fail ("empty: " ++ path)
  else do
    let rows = V.fromList (map V.singleton ls)
    case fromTbl (mkTextViewOps rows) (T.pack path) 0 V.empty 0 of
      Nothing -> fail ("empty: " ++ path)
      Just v  -> pure (v & vNav % nsVkind .~ VViewFile)
