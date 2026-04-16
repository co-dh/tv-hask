{-
  FTP: parse curl's ls -l output into folder-view TSV.
  Names stored raw (readable); URL-encoding applied at curl command time.
-}
module Tv.Ftp
  ( urlEncode
  , encodeUrl
  , parseLs
  ) where

import Data.Char (isAlphaNum)
import Data.List (foldl')
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString as BS
import Data.Word (Word8)
import Text.Printf (printf)

import qualified Tv.Util as Util

-- | URL-encode a string: encode all bytes not in [A-Za-z0-9._~/-]
urlEncode :: Text -> Text
urlEncode s = T.concat (map encChar (T.unpack s))
  where
    encChar c
      | isAlphaNum c || c == '.' || c == '_' || c == '~' || c == '/' || c == '-' =
          T.singleton c
      | otherwise =
          let bytes = BS.unpack (TE.encodeUtf8 (T.singleton c))
          in T.concat (map encByte bytes)
    encByte :: Word8 -> Text
    encByte b = T.pack (printf "%%%02X" b)

-- | URL-encode an FTP URL: encode only path segments, not protocol/host.
-- e.g. "ftp://host/a b/c d/" -> "ftp://host/a%20b/c%20d/"
encodeUrl :: Text -> Text -> Text
encodeUrl pfx url =
  let rest = T.drop (T.length pfx) url
      segs = T.splitOn "/" rest
  in case segs of
       (host : tl) -> pfx <> host <> "/" <> T.intercalate "/" (map urlEncode tl)
       []          -> url

-- | Parse FTP ls -l output into TSV (name\tsize\tdate\ttype).
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
             let name = Util.headD "" (T.splitOn " -> " (T.unwords (drop 8 parts)))
                 size = Util.getD parts 4 "0"
                 date = T.unwords (take 3 (drop 5 parts))
                 typ  = if T.isPrefixOf "d" (Util.getD parts 0 "") then "dir" else "file"
             in (name <> "\t" <> size <> "\t" <> date <> "\t" <> typ) : acc
