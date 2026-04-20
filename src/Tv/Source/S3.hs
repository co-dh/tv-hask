{-# LANGUAGE OverloadedStrings #-}
-- | S3 (s3://) backend. Native HTTPS via Tv.S3.Client (no `aws` CLI).
-- Anonymous LIST/GET for public buckets when `+n` is passed; SigV4 from
-- AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars otherwise.
module Tv.Source.S3 (s3) where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Directory (createDirectoryIfMissing)
import System.Exit (ExitCode (..))

import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable, fromTmp, tmpName)
import qualified Tv.Remote as Remote
import qualified Tv.Render as Render
import qualified Tv.S3.Client as S3C
import qualified Tv.Tmp as Tmp
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core

pfx_ :: Text
pfx_ = "s3://"

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

-- | Split "s3://bucket/a/b/c" → (bucket, "a/b/c"). Trailing '/' is kept
-- on the key part if the caller passed one in.
splitS3 :: Text -> (Text, Text)
splitS3 path_ =
  let rest = T.drop (T.length pfx_) path_
  in case T.breakOn "/" rest of
       (b, "")  -> (b, "")
       (b, ks)  -> (b, T.drop 1 ks)

s3List :: Bool -> Text -> IO (Maybe AdbcTable)
s3List noSign path_ = do
  Core.checkShell path_ "path"
  let p = if T.isSuffixOf "/" path_ then path_ else path_ <> "/"
      (bucket, key) = splitS3 p
  (ec, out, err) <- S3C.listS3 noSign bucket key
  case ec of
    ExitFailure _ -> do
      Render.errorPopup ("S3 list failed: " <> T.strip err)
      pure Nothing
    ExitSuccess
      | T.null (T.strip out) -> pure Nothing
      | otherwise -> do
          tmpFile <- Tmp.tmpPath "src-list.json"
          TIO.writeFile tmpFile out
          tbl <- tmpName "src"
          let sql = Core.expand listSqlTmpl (V.singleton ("src", T.pack tmpFile))
          _ <- Conn.query ("CREATE TEMP TABLE " <> tbl <> " AS " <> sql)
          Tmp.rmFile tmpFile
          Core.unnestStruct tbl
          Core.addParentRow (Remote.parent path_ 3) tbl
          fromTmp tbl

-- | Trailing '/' = directory (caller appended it for dir rows); otherwise
-- a file that needs local download before FileFormat can read it.
s3Open :: Bool -> Text -> IO OpenResult
s3Open noSign path_
  | T.isSuffixOf "/" path_ = pure (OpenAsDir path_)
  | otherwise = do
      Core.checkShell path_ "path"
      Render.statusMsg ("Downloading " <> path_ <> " ...")
      tmpDir <- Tmp.tmpPath "src"
      createDirectoryIfMissing True tmpDir
      let (bucket, key) = splitS3 path_
          dest = tmpDir <> "/" <> T.unpack (Core.fromPath path_)
      _ <- S3C.downloadS3 noSign bucket key dest
      pure (OpenAsFile dest)

s3 :: Source
s3 = Source
  { pfx    = pfx_
  , parent = \p -> Remote.parent p 3
  , grpCol = Nothing
  , list   = s3List
  , open   = s3Open
  }
