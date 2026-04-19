{-
  FTP: parse the LIST command's `ls -l` output into folder-view TSV.
  URL encoding helpers were dropped along with the curl shellout
  (a751fcc → 7d2826c).
-}
module Tv.Ftp
  ( parseLs
  ) where

import Data.List (foldl')
import Data.Text (Text)
import qualified Data.Text as T

import Tv.Types (headD, getD)

-- | Parse FTP LIST (ls -l) output into TSV (name\tsize\tdate\ttype).
-- Format: perms links user group size month day time name...
--         p[0]  p[1]  p[2] p[3]  p[4] p[5]  p[6] p[7] p[8:]
parseLs :: Text -> Text
parseLs raw =
  let header = "name\tsize\tdate\ttype"
      ls = T.splitOn "\n" raw
      rows = foldl' step [header] ls
  in T.intercalate "\n" (reverse rows)
  where
    step acc line =
      let parts = T.words line
      in if length parts < 9
           then acc
           else
             let name = headD "" $ T.splitOn " -> " $ T.unwords $ drop 8 parts
                 size = getD parts 4 "0"
                 date = T.unwords $ take 3 $ drop 5 parts
                 typ  = if T.isPrefixOf "d" $ getD parts 0 "" then "dir" else "file"
             in (name <> "\t" <> size <> "\t" <> date <> "\t" <> typ) : acc
