{-
  Text parsing: space-separated input (ps aux, ls -l, etc.) -> TSV string
  DuckDB handles type detection via read_csv_auto.
-}
module Tv.Data.Text
  ( mode
  , wordStarts
  , splitN
  , colStarts
  , splitCols
  , fromText
  , fromStdin
  ) where

import Data.List (foldl', maximumBy)
import Data.Ord (comparing)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.HashMap.Strict as HM

-- | Find mode (most common value) in array
mode :: Vector Int -> Int
mode xs
  | V.null xs = 0
  | otherwise =
      let m = V.foldl' (\acc x -> HM.insertWith (+) x 1 acc) HM.empty xs
      in fst (maximumBy (comparing snd) (HM.toList m))

-- | Count word starts (non-space after space, plus position 0 if non-space)
wordStarts :: Text -> Int
wordStarts = length . T.words

-- | Split line into n fields (last field gets remainder with spaces)
splitN :: Text -> Int -> Vector Text
splitN s n
  | n == 0 = V.empty
  | otherwise =
      let ws = T.words s
          (first, rest) = splitAt (n - 1) ws
          pad = replicate (max 0 (n - 1 - length first)) ""
      in V.fromList (first ++ pad ++ [T.unwords rest])

-- | Find column start positions from header (2+ consecutive spaces = separator)
colStarts :: Text -> Vector Int
colStarts hdr =
  let chars = V.fromList (T.unpack hdr)
      n = V.length chars
      getD i = if i >= 0 && i < n then chars V.! i else ' '
      step (starts, spaceCount) i =
        if getD i == ' '
          then (starts, spaceCount + 1)
          else
            let starts' = if spaceCount >= 2 then starts ++ [i] else starts
            in (starts', 0)
      (result, _) = foldl' step ([0], 0 :: Int) [0 .. n - 1]
  in V.fromList result

-- | Split line by column start positions (last col extends to end)
splitCols :: Text -> Vector Int -> Vector Text
splitCols s starts =
  let sz = V.length starts
      getD i = if i >= 0 && i < sz then starts V.! i else 0
      slen = T.length s
      mkField i =
        let st = getD i
            en = if i + 1 < sz then getD (i + 1) else slen
        in T.strip (T.take (en - st) (T.drop st s))
  in V.generate sz mkField

-- | Parse space-separated text to TSV string (like ps aux, ls -l, systemctl output)
-- Fixed-width if header has 2+ space gaps AND gives more cols than mode-based
fromText :: Text -> Either Text Text
fromText content =
  let lines_ = filter (not . T.null) (T.splitOn "\n" content)
  in case lines_ of
       [] -> Left "empty input"
       (hdr : rest) ->
         let starts = colStarts hdr
             allLines = V.fromList (hdr : rest)
             modeNc = mode (V.map wordStarts allLines)
         in
           -- use fixed-width only if it gives >= mode columns (handles mixed spacing)
           if V.length starts >= modeNc && V.length starts > 1
             then
               let names = splitCols hdr starts
                   header = T.intercalate "\t" (V.toList names)
                   rows = map (\line -> T.intercalate "\t" (V.toList (splitCols line starts))) rest
               in Right (header <> "\n" <> T.intercalate "\n" rows)
             else
               -- else use mode of word counts (handles "total 836" outliers)
               let nc = modeNc
               in if nc == 0 then Left "no columns"
                  else
                    let names = splitN hdr nc
                        header = T.intercalate "\t" (V.toList names)
                        rows = map (\line -> T.intercalate "\t" (V.toList (splitN line nc))) rest
                    in Right (header <> "\n" <> T.intercalate "\n" rows)

-- | Load from stdin (reads all input, returns TSV)
fromStdin :: IO (Either Text Text)
fromStdin = do
  content <- TIO.getContents
  pure $ fromText content
