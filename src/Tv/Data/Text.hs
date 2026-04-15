{-
  Text parsing: space-separated input (ps aux, ls -l, etc.) -> TSV string
  DuckDB handles type detection via read_csv_auto.
-}
module Tv.Data.Text where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.HashMap.Strict as HM

-- | Find mode (most common value) in array
mode :: Vector Int -> Int
mode xs =
  let m :: HM.HashMap Int Int
      m = V.foldl' (\acc x -> HM.insertWith (+) x 1 acc) HM.empty xs
      step (b, cnt) (k, v) = if v > cnt then (k, v) else (b, cnt)
      (best, _) = foldl step (0 :: Int, 0 :: Int) (HM.toList m)
  in best

-- | Count word starts (non-space after space, plus position 0 if non-space)
countWordStarts :: Text -> Int
countWordStarts s =
  let chars = V.fromList (T.unpack s)
      n = V.length chars
      getD i = if i >= 0 && i < n then chars V.! i else ' '
  in if n == 0 then 0
     else
       let c0 = if getD 0 /= ' ' then 1 else 0
           step cnt i = if getD i /= ' ' && getD (i - 1) == ' ' then cnt + 1 else cnt
       in foldl step c0 [1 .. n - 1]

-- | Split line into n fields (last field gets remainder with spaces)
splitN :: Text -> Int -> Vector Text
splitN s n
  | n == 0 = V.empty
  | otherwise =
      let go :: Int -> Text -> [Text] -> (Text, [Text])
          go 0 rest acc = (rest, reverse acc)
          go k rest acc =
            case filter (not . T.null) (T.splitOn " " rest) of
              [] -> go (k - 1) rest ("" : acc)
              (fld : tl) ->
                let rest' = T.stripStart (T.intercalate " " tl)
                in go (k - 1) rest' (fld : acc)
          (finalRest, fields) = go (n - 1) (T.stripStart s) []
      in V.fromList (fields ++ [T.strip finalRest])

-- | Find column start positions from header (2+ consecutive spaces = separator)
findColStarts :: Text -> Vector Int
findColStarts hdr =
  let chars = V.fromList (T.unpack hdr)
      n = V.length chars
      getD i = if i >= 0 && i < n then chars V.! i else ' '
      step (starts, spaceCount) i =
        if getD i == ' '
          then (starts, spaceCount + 1)
          else
            let starts' = if spaceCount >= 2 then starts ++ [i] else starts
            in (starts', 0)
      (result, _) = foldl step ([0], 0 :: Int) [0 .. n - 1]
  in V.fromList result

-- | Split line by column start positions (last col extends to end)
splitByStarts :: Text -> Vector Int -> Vector Text
splitByStarts s starts =
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
         let starts = findColStarts hdr
             allLines = V.fromList (hdr : rest)
             modeNc = mode (V.map countWordStarts allLines)
         in
           -- use fixed-width only if it gives >= mode columns (handles mixed spacing)
           if V.length starts >= modeNc && V.length starts > 1
             then
               let names = splitByStarts hdr starts
                   header = T.intercalate "\t" (V.toList names)
                   rows = map (\line -> T.intercalate "\t" (V.toList (splitByStarts line starts))) rest
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
  pure (fromText content)
