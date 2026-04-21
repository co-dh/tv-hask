{-# LANGUAGE OverloadedStrings #-}
-- | FTP (ftp://) backend: native ftp-client fetches `LIST`, Haskell parses
-- it into TSV, DuckDB reads the TSV. URLs are not URL-encoded because the
-- FTP protocol takes raw paths via CWD (no URL parser in the wire path).
module Tv.Source.Ftp (ftp) where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import System.Directory (createDirectoryIfMissing)
import System.Exit (ExitCode (..))

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Ftp as Ftp
import qualified Tv.Ftp.Client as FtpC
import qualified Tv.Remote as Remote
import qualified Tv.Render as Render
import qualified Tv.Tmp as Tmp
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "ftp://"

ftpList :: Bool -> Text -> IO (Maybe AdbcTable)
ftpList _ path_ = do
  let p = if T.isSuffixOf "/" path_ then path_ else path_ <> "/"
  (ec, out, err) <- FtpC.listFtp p
  case ec of
    ExitFailure _ -> do
      Render.errorPopup ("List failed: " <> T.strip err)
      pure Nothing
    ExitSuccess
      | T.null (T.strip out) -> pure Nothing
      | otherwise -> do
          tsv <- writeTsv (Ftp.parseLs out)
          tbl <- tmpName "src"
          _ <- Conn.query ( "CREATE TEMP TABLE " <> tbl
                         <> " AS SELECT * FROM read_csv('" <> T.pack tsv
                         <> "', header=true, delim='\t')" )
          Tmp.rmFile tsv
          Core.addParentRow (Remote.parent path_ 3) tbl
          fromTmp tbl
  where
    writeTsv content = do
      tmpFile <- Tmp.tmpPath "src-list.json"
      TIO.writeFile tmpFile content
      pure tmpFile

ftpOpen :: Bool -> Text -> IO OpenResult
ftpOpen _ path_
  | T.isSuffixOf "/" path_ = pure (OpenAsDir path_)
  | otherwise = do
      Render.statusMsg ("Downloading " <> path_ <> " ...")
      tmpDir <- Tmp.tmpPath "src"
      createDirectoryIfMissing True tmpDir
      let dest = tmpDir <> "/" <> T.unpack (Core.fromPath path_)
      _ <- FtpC.downloadFtp path_ dest
      pure $ OpenAsFile dest

ftp :: Source
ftp = Source
  { pfx    = pfx_
  , parent = \p -> Remote.parent p 3
  , grpCol = Nothing
  , list   = ftpList
  , open   = ftpOpen
  }
