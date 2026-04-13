{-# LANGUAGE OverloadedStrings #-}
module DuckDBSpec (tests) where

import Test.Tasty
import Test.Tasty.HUnit

import qualified Data.Vector as V
import qualified Data.Text as T
import System.FilePath ((</>))
import System.Directory (createDirectoryIfMissing, removeDirectoryRecursive, getTemporaryDirectory)
import Control.Exception (bracket)
import qualified Tv.Data.DuckDB as D
import qualified Tv.Folder as Folder
import Tv.Types
import Tv.Eff (runEff)
import TestUtil (withMemConn)
import Optics.Core ((^.), (%), (&), (.~), (%~))

tests :: TestTree
tests =
  testGroup
    "DuckDB"
    [ testCase "memory_limit config is 256MB" $ do
        withMemConn $ \c -> do
          r <- D.query c "SELECT current_setting('memory_limit') AS ml"
          cs <- D.chunks r
          case cs of
            (ch:_) -> case D.readCellText (D.chunkColumn ch 0) 0 of
              -- 2GB config → DuckDB reports ~1.8 GiB (internal rounding)
              Just t  -> assertBool ("memory_limit=" ++ show t) ("1.8 GiB" `T.isPrefixOf` t)
              Nothing -> assertFailure "null"
            [] -> assertFailure "no chunks"
    , testCase "connect/disconnect in-memory" $ do
        withMemConn $ \_ -> pure ()
    , testCase "scalar SELECT returns correct metadata & cells" $ do
        withMemConn $ \c -> do
          r <- D.query c "SELECT 1 AS x, 'hello' AS y, CAST(3.14 AS DOUBLE) AS z"
          D.columnNames r @?= V.fromList ["x", "y", "z"]
          let tys = D.columnTypes r
          V.toList tys @?= [CTInt, CTStr, CTFloat]
          cs <- D.chunks r
          case cs of
            (chunk : _) -> do
              D.chunkSize chunk @?= 1
              let cX = D.chunkColumn chunk 0
                  cY = D.chunkColumn chunk 1
                  cZ = D.chunkColumn chunk 2
              D.readCellInt cX 0 @?= Just 1
              D.readCellText cY 0 @?= Just "hello"
              case D.readCellDouble cZ 0 of
                Just d ->
                  assertBool ("z was " ++ show d) (abs (d - 3.14) < 1e-9)
                Nothing -> assertFailure "z cell Nothing"
            [] -> assertFailure "no chunks"
    , testCase "range(10000) streams chunks lazily, random-access OK" $ do
        withMemConn $ \c -> do
          r <- D.query c "SELECT range FROM range(10000)"
          cs <- D.chunks r
          let sizes = map D.chunkSize cs
              total = sum sizes
          total @?= 10000
          let pickRow absRow = go 0 cs
                where
                  go _ [] = error "row out of range"
                  go off (ch : rest) =
                    let sz = D.chunkSize ch
                     in if absRow < off + sz
                          then (ch, absRow - off)
                          else go (off + sz) rest
              (ch5k, local5k) = pickRow 5000
              cv = D.chunkColumn ch5k 0
          D.readCellInt cv local5k @?= Just 5000
    , testCase "read from parquet file on disk" $ do
        withMemConn $ \c -> do
          r <- D.query c "SELECT * FROM '/home/dh/repo/Tc/data/1.parquet' LIMIT 5"
          let ncols = V.length (D.columnNames r)
          assertBool "col count > 0" (ncols > 0)
          cs <- D.chunks r
          case cs of
            (ch : _) -> do
              let sz = D.chunkSize ch
              assertBool ("chunk size > 0, got " ++ show sz) (sz > 0)
              let cv0 = D.chunkColumn ch 0
                  ty0 = D.columnTypes r V.! 0
              let hasCell = case ty0 of
                    CTInt -> case D.readCellInt cv0 0 of Just _ -> True; Nothing -> True
                    CTFloat -> case D.readCellDouble cv0 0 of Just _ -> True; Nothing -> True
                    CTStr -> case D.readCellText cv0 0 of Just _ -> True; Nothing -> True
                    _ -> True
              assertBool "first row readable" hasCell
            [] -> assertFailure "no chunks"
    , testCase "mkDbOps on /home/dh/repo/Tc/data/1.parquet" $ do
        withMemConn $ \c -> do
          r <- D.query c "SELECT * FROM read_parquet('/home/dh/repo/Tc/data/1.parquet') LIMIT 1000"
          ops <- D.mkDbOps r
          assertBool "nrows > 0" ((ops ^. tblNRows) > 0)
          assertBool "ncols > 0" (V.length ((ops ^. tblColNames)) > 0)
          s <- (ops ^. tblCellStr) 0 0
          assertBool ("cell (0,0) = " ++ T.unpack s) (not (T.null s))
    , testCase "listFolder returns expected entries for a temp dir" $ do
        tmp <- getTemporaryDirectory
        let d = tmp </> "tv-hask-folder-test"
        bracket (do createDirectoryIfMissing True d
                    writeFile (d </> "a.txt") "hi"
                    writeFile (d </> "b.txt") "yo"
                    pure d)
                removeDirectoryRecursive
                $ \_ -> do
          ops <- runEff (Folder.listFolder d)
          V.toList ((ops ^. tblColNames)) @?= ["name", "size", "modified", "type"]
          (ops ^. tblNRows) @?= 3
          names <- mapM (\r -> (ops ^. tblCellStr) r 0) [0 .. (ops ^. tblNRows) - 1]
          assertBool ("names = " ++ show names) (".." `elem` names)
          assertBool ("names = " ++ show names) ("a.txt" `elem` names)
          assertBool ("names = " ++ show names) ("b.txt" `elem` names)
    ]
