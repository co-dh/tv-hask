{-# LANGUAGE OverloadedStrings #-}
-- | URI path operations shared by remote backends (FTP, S3, HuggingFace, …).
module Tv.Remote
  ( joinRemote
  , stripSlash
  , parent
  , dispName
  ) where

import Tv.Prelude
import qualified Data.Text as T

-- | Join URI prefix with child name
joinRemote :: Text -> Text -> Text
joinRemote pfx name =
  if T.isSuffixOf "/" pfx then pfx <> name else pfx <> "/" <> name

-- | Strip trailing slash from path
stripSlash :: Text -> Text
stripSlash p =
  if T.isSuffixOf "/" p then T.dropEnd 1 p else p

-- | Get parent URI: drop last path component. Returns none at root (<= minParts components).
parent :: Text -> Int -> Maybe Text
parent pth minParts =
  let parts = T.splitOn "/" (stripSlash pth)
  in if length parts <= minParts
       then Nothing
       else Just (T.intercalate "/" (init parts) <> "/")

-- | Display name: last non-empty path component (preserves protocol-only paths)
dispName :: Text -> Text
dispName pth =
  let parts = filter (not . T.null) (T.splitOn "/" (stripSlash pth))
  in if length parts <= 1 then pth else last parts
