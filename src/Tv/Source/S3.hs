{-# LANGUAGE OverloadedStrings #-}
-- | S3 (s3://) backend: `aws s3api` JSON listing, `aws s3 cp` downloads.
module Tv.Source.S3 (s3) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import System.Directory (createDirectoryIfMissing)

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Log as Log
import qualified Tv.Remote as Remote
import qualified Tv.Render as Render
import qualified Tv.Tmp as Tmp
import Tv.Source.Core (Source (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "s3://"

-- S3 rejects unsigned requests by default; `--no-sign-request` enables
-- anonymous access to public buckets. We gate it behind a CLI flag.
extraFlag :: Bool -> Text
extraFlag ns = if ns then "--no-sign-request" else ""

-- Explicit JSON columns: missing keys become NULL, so unnest(NULL) yields
-- 0 rows rather than erroring on mixed-response buckets.
listSqlTmpl :: Text
listSqlTmpl =
  "WITH j AS MATERIALIZED (SELECT * FROM read_json('{src}', columns="
  <> "{\"Contents\": 'STRUCT(\"Key\" VARCHAR, \"Size\" BIGINT, \"LastModified\" VARCHAR)[]', "
  <> "\"CommonPrefixes\": 'STRUCT(\"Prefix\" VARCHAR)[]'})) "
  <> "SELECT split_part(c.\"Key\", '/', -1) as name, c.\"Size\" as size, "
  <> "c.\"LastModified\" as date, 'file' as type "
  <> "FROM j, unnest(j.Contents) as t(c) WHERE c.\"Key\" IS NOT NULL "
  <> "UNION ALL "
  <> "SELECT split_part(c.\"Prefix\", '/', -2) as name, 0 as size, '' as date, 'dir' as type "
  <> "FROM j, unnest(j.CommonPrefixes) as t(c) WHERE c.\"Prefix\" IS NOT NULL"

cmdTmpl :: Text
cmdTmpl = "aws s3api list-objects-v2 --bucket {1} --delimiter / --prefix {2+}/ {extra} --output json"

dlTmpl :: Text
dlTmpl = "aws s3 cp {extra} {path} {tmp}/{name}"

-- | Build cmd vars: path sanity + ensure tmpdir exists.
s3Vars :: Bool -> Text -> IO (V.Vector (Text, Text), Text)
s3Vars noSign path_ = do
  Core.checkShell path_ "path"
  tmpDir <- Tmp.tmpPath "src"
  createDirectoryIfMissing True tmpDir
  _ <- Log.run "src" "mkdir" ["-p", tmpDir]
  let tmpT = T.pack tmpDir
  pure (Core.mkVars pfx_ path_ tmpT (Core.fromPath path_) (extraFlag noSign), tmpT)

s3List :: Bool -> Text -> IO (Maybe AdbcTable)
s3List noSign path_ = do
  let p = if T.isSuffixOf "/" path_ then path_ else path_ <> "/"
  (vars, _) <- s3Vars noSign p
  let cmd = Core.expand cmdTmpl vars
  mFile <- Core.writeCmdOut "list" cmd
  case mFile of
    Nothing      -> pure Nothing
    Just tmpFile -> do
      tbl <- tmpName "src"
      let sql = Core.expand listSqlTmpl (V.singleton ("src", T.pack tmpFile))
      _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
      Tmp.rmFile tmpFile
      Core.unnestStruct tbl
      Core.addParentRow (Remote.parent path_ 3) tbl
      fromTmp tbl

s3Dl :: Bool -> Text -> IO Text
s3Dl noSign path_ = do
  Render.statusMsg ("Downloading " <> path_ <> " ...")
  (vars, tmpDir) <- s3Vars noSign path_
  let cmd = Core.expand dlTmpl vars
  _ <- Core.runCmd "download" cmd
  pure (tmpDir <> "/" <> Core.fromPath path_)

s3 :: Source
s3 = Source
  { pfx       = pfx_
  , list      = s3List
  , enter     = Nothing
  , enterUrl  = Nothing
  , download  = s3Dl
  , resolve   = s3Dl                 -- S3 requires local download (DuckDB can't read s3:// directly here)
  , setup     = pure ()
  , parent    = \p -> Remote.parent p 3
  , grpCol    = ""
  , attach    = False
  , dirSuffix = True
  }
