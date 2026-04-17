{-# LANGUAGE OverloadedStrings #-}
-- | FTP (ftp://) backend: curl fetches `ls -l`, Haskell parses it into TSV,
-- DuckDB reads the TSV. URLs are encoded per-segment because curl is
-- path-sensitive (spaces, unicode).
module Tv.Source.Ftp (ftp) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Directory (createDirectoryIfMissing)
import System.Exit (ExitCode (..))

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Ftp as Ftp
import qualified Tv.Log as Log
import qualified Tv.Remote as Remote
import qualified Tv.Render as Render
import qualified Tv.Tmp as Tmp
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "ftp://"

cmdTmpl :: Text
cmdTmpl = "curl -sf {path}"

dlTmpl :: Text
dlTmpl = "curl -sfL -o '{tmp}/{name}' {path}"

-- | Build vars with URL-encoded path; creates tmpdir so downloads can land.
ftpVars :: Text -> IO (V.Vector (Text, Text), Text)
ftpVars path_ = do
  Core.checkShell path_ "path"
  tmpDir <- Tmp.tmpPath "src"
  createDirectoryIfMissing True tmpDir
  _ <- Log.run "src" "mkdir" ["-p", tmpDir]
  let tmpT    = T.pack tmpDir
      cmdPath = Ftp.encodeUrl pfx_ path_
  pure (Core.mkVars pfx_ cmdPath tmpT (Core.fromPath path_) "", tmpT)

ftpList :: Bool -> Text -> IO (Maybe AdbcTable)
ftpList _ path_ = do
  let p = if T.isSuffixOf "/" path_ then path_ else path_ <> "/"
  (vars, _) <- ftpVars p
  let cmd = Core.expand cmdTmpl vars
  (ec, out, err) <- Core.runCmd "list" cmd
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
      (vars, tmpDir) <- ftpVars path_
      let cmd = Core.expand dlTmpl vars
      _ <- Core.runCmd "download" cmd
      pure $ OpenAsFile $ T.unpack $ tmpDir <> "/" <> Core.fromPath path_

ftp :: Source
ftp = Source
  { pfx    = pfx_
  , parent = \p -> Remote.parent p 3
  , grpCol = Nothing
  , list   = ftpList
  , open   = ftpOpen
  }
