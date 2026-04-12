-- | Text parsing: space-separated input (ps aux, ls -l, etc.) -> TSV string.
-- DuckDB handles type detection via read_csv_auto on the resulting TSV.
module Tv.Data.Text (fromText, fromStdin) where

import Data.List (foldl')
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Map.Strict as Map

-- | Find mode (most common value) in list.
mode :: [Int] -> Int
mode xs = fst $ Map.foldlWithKey' (\(bk, bc) k v -> if v > bc then (k, v) else (bk, bc))
                                   (0, 0) counts
  where counts = foldl' (\m x -> Map.insertWith (+) x 1 m) Map.empty xs

-- | Count word starts (non-space after space, plus position 0 if non-space).
countWordStarts :: Text -> Int
countWordStarts t
  | T.null t = 0
  | otherwise = (if T.head t /= ' ' then 1 else 0) + go False (T.unpack t)
  where
    go _ [] = 0; go _ [_] = 0
    go _ (c:cs@(n:_)) = (if c == ' ' && n /= ' ' then 1 else 0) + go False cs

-- | Split line into n fields (last field gets remainder with spaces).
splitN :: Text -> Int -> [Text]
splitN _ 0 = []
splitN s n = go (T.stripStart s) (n - 1)
  where
    go rest 0 = [T.strip rest]
    go rest k = case T.breakOn " " rest of
      (_, r) | T.null rest -> T.empty : go T.empty (k - 1)
      (fld, r) -> fld : go (T.stripStart (T.dropWhile (== ' ') r)) (k - 1)

-- | Find column start positions from header (2+ consecutive spaces = separator).
findColStarts :: Text -> [Int]
findColStarts hdr = 0 : go 0 0 (T.unpack hdr)
  where
    go _ _ [] = []
    go i sc (c:cs)
      | c == ' '  = go (i+1) (sc+1) cs
      | sc >= 2   = i : go (i+1) 0 cs
      | otherwise  = go (i+1) 0 cs

-- | Split line by column start positions (last col extends to end).
splitByStarts :: Text -> [Int] -> [Text]
splitByStarts s starts = zipWith extract starts (drop 1 starts ++ [T.length s])
  where extract st en = T.strip (T.take (en - st) (T.drop st s))

-- | Parse space-separated text to TSV (like ps aux, ls -l, systemctl output).
-- Fixed-width if header has 2+ space gaps AND gives more cols than mode-based.
fromText :: Text -> Either String Text
fromText content =
  let lns = filter (not . T.null) (T.splitOn "\n" content)
  in case lns of
    [] -> Left "empty input"
    (hdr : rest) ->
      let starts = findColStarts hdr
          allLines = hdr : rest
          modeNc = mode (map countWordStarts allLines)
      -- use fixed-width only if it gives >= mode columns (handles mixed spacing)
      in if length starts >= modeNc && length starts > 1
         then let names = splitByStarts hdr starts
                  header = T.intercalate "\t" names
                  rows = map (\l -> T.intercalate "\t" (splitByStarts l starts)) rest
              in Right (header <> "\n" <> T.intercalate "\n" rows)
         else if modeNc == 0 then Left "no columns"
         else let names = splitN hdr modeNc
                  header = T.intercalate "\t" names
                  rows = map (\l -> T.intercalate "\t" (splitN l modeNc)) rest
              in Right (header <> "\n" <> T.intercalate "\n" rows)

-- | Load from stdin (reads all input, returns TSV).
fromStdin :: IO (Either String Text)
fromStdin = fromText <$> TIO.getContents
