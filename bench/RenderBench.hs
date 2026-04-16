module Main where

import Control.Monad (replicateM_)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import System.CPUTime
import Text.Printf (printf)

import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Term as Term
import Tv.Types (ColType(..))

frames :: Int
frames = 500

time :: String -> IO a -> IO a
time label act = do
  t0 <- getCPUTime
  r  <- act
  t1 <- getCPUTime
  let ms = fromIntegral (t1 - t0) / 1e9 :: Double
  printf "%s: %.1f ms (%d frames, %.3f ms/frame)\n" label ms frames (ms / fromIntegral frames)
  pure r

main :: IO ()
main = do
  err <- Adbc.init
  if not (T.null err) then putStrLn ("init failed: " ++ T.unpack err) else do
    _ <- Adbc.query "CREATE TABLE bench AS SELECT i AS id, 'name_' || i AS name, random()::DOUBLE AS val, DATE '2020-01-01' + CAST(i % 365 AS INTEGER) AS dt, i % 100 AS cat, 'description_' || i AS desc_, i * 1.5 AS price, i % 2 = 0 AS active, 'city_' || (i % 50) AS city, i * 100 AS amt, 'tag_' || (i % 200) AS tag, random()::DOUBLE AS score, 'note_' || i AS notes, i % 1000 AS bucket, DATE '2021-06-15' + CAST(i % 730 AS INTEGER) AS updated, 'group_' || (i % 30) AS grp, i + 1000000 AS big_id, random()::DOUBLE AS pct, 'type_' || (i % 10) AS kind, i % 5 AS rank FROM range(100000) t(i)"
    qr <- Adbc.query "SELECT * FROM bench"
    let nc = V.length (Adbc.qrColNames qr)
        types = Adbc.qrColTypes qr

    _ <- Term.init
    putStrLn "Benchmarking fetchRows + renderTable (pure Haskell)"
    putStrLn $ "Dataset: 100K rows × " ++ show nc ++ " cols, visible: 50 rows × 20 cols"
    putStrLn ""

    let nVisible = 47
        colIdxs = V.enumFromN 0 (fromIntegral nc)
        emptyWidths = V.replicate nc 0
        styles = V.replicate 18 0
        emptySparklines = V.replicate nc T.empty
        emptyHeat = V.empty

    time "fetchRows (50 rows × 20 cols)" $ replicateM_ frames $ do
      _ <- Adbc.fetchRows qr 0 nVisible 3
      pure ()

    texts <- Adbc.fetchRows qr 0 nVisible 3
    time "renderTable (Haskell, from text grid)" $ replicateM_ frames $ do
      _ <- Term.renderTable texts (Adbc.qrColNames qr) V.empty types
             emptyWidths colIdxs 100000 0 0 0
             (fromIntegral nVisible) 10 5 0
             V.empty V.empty V.empty styles 3 0 0
             emptySparklines emptyHeat
      pure ()

    time "fetchRows + renderTable combined" $ replicateM_ frames $ do
      t <- Adbc.fetchRows qr 0 nVisible 3
      _ <- Term.renderTable t (Adbc.qrColNames qr) V.empty types
             emptyWidths colIdxs 100000 0 0 0
             (fromIntegral nVisible) 10 5 0
             V.empty V.empty V.empty styles 3 0 0
             emptySparklines emptyHeat
      pure ()

    Adbc.shutdown
