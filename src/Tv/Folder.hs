{-# LANGUAGE ScopedTypeVariables #-}
-- | Folder: directory browser. Builds a read-only TblOps from a local
-- directory listing with columns name/size/modified/type. Used both as
-- the default view (no CLI args) and on folder.enter for subdirectories.
--
-- Mirrors Tc.Folder.listDir in shape (same column names) so navigation
-- tests written against the mock folder layout stay portable.
module Tv.Folder (listFolder, listFolderDepth) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Format (formatTime, defaultTimeLocale)
import qualified Data.Vector as V
import System.Directory
  ( doesDirectoryExist, getModificationTime, getFileSize, listDirectory
  , pathIsSymbolicLink )
import System.FilePath ((</>))
import Control.Exception (catch, SomeException)

import Tv.Types

-- | Row in the folder table.
data Entry = Entry
  { eName :: !Text
  , eSize :: !Text
  , eMod  :: !Text
  , eType :: !Text
  }

-- | Stat one entry. Catches IO errors (broken symlinks, permissions).
statEntry :: FilePath -> FilePath -> IO Entry
statEntry base name = do
  let p = base </> name
  isLnk <- pathIsSymbolicLink p `catch` (\(_ :: SomeException) -> pure False)
  isDir <- doesDirectoryExist p `catch` (\(_ :: SomeException) -> pure False)
  let kind | isLnk = "symlink"
           | isDir = "dir"
           | otherwise = "file"
  sz <- if isDir then pure 0
        else getFileSize p `catch` (\(_ :: SomeException) -> pure 0)
  mt <- (Just <$> getModificationTime p)
          `catch` (\(_ :: SomeException) -> pure Nothing)
  let mtTxt = case mt of
        Nothing -> ""
        Just t  -> T.pack (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" t)
  pure (Entry (T.pack name) (T.pack (show sz)) mtTxt kind)

-- | Recursively list children of @base@ up to @depth@ levels deep.
-- Entries at depth > 1 get a name equal to their path relative to
-- @base@ (e.g. "sub/file.txt") so the folder table shows a flat
-- view of the subtree. Mirrors Tc.Folder.listDir with find -maxdepth.
collect :: FilePath -> FilePath -> Int -> IO [Entry]
collect base rel remaining = do
  let dir = if null rel then base else base </> rel
  names <- listDirectory dir `catch` (\(_ :: SomeException) -> pure [])
  let withRel n = if null rel then n else rel </> n
  entries <- mapM (\n -> do
                    e <- statEntry base (withRel n)
                    pure e) names
  if remaining <= 1
    then pure entries
    else do
      -- Descend into subdirectories (real directories, not symlinks).
      subs <- concat <$> mapM (descend base rel remaining) names
      pure (entries ++ subs)

descend :: FilePath -> FilePath -> Int -> FilePath -> IO [Entry]
descend base rel remaining n = do
  let relN = if null rel then n else rel </> n
      p = base </> relN
  isLnk <- pathIsSymbolicLink p `catch` (\(_ :: SomeException) -> pure False)
  isDir <- doesDirectoryExist p `catch` (\(_ :: SomeException) -> pure False)
  if isDir && not isLnk
    then collect base relN (remaining - 1)
    else pure []

-- | Build a read-only TblOps for a directory at depth 1.
listFolder :: FilePath -> IO TblOps
listFolder p = listFolderDepth p 1

-- | Build a read-only TblOps for a directory, recursing @depth@ levels.
-- Columns: name, size, modified, type. Includes a ".." parent entry
-- first (matching Tc).
listFolderDepth :: FilePath -> Int -> IO TblOps
listFolderDepth path depth = do
  entries <- collect path "" (max 1 depth)
  let parent = Entry ".." "0" "" "dir"
      rows = V.fromList (parent : entries)
      rowCell e i = case i of
        0 -> eName e; 1 -> eSize e; 2 -> eMod e; 3 -> eType e; _ -> T.empty
      nr = V.length rows
      cols = V.fromList ["name", "size", "modified", "type"]
      nc = V.length cols
  let ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = cols
        , _tblTotalRows   = nr
        , _tblFilter      = \_ -> pure Nothing
        , _tblDistinct    = \_ -> pure V.empty
        , _tblFindRow     = \_ _ _ _ -> pure Nothing
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = \_ -> CTStr
        , _tblBuildFilter = \_ _ _ _ -> T.empty
        , _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr     = \r c ->
            if r < 0 || r >= nr || c < 0 || c >= nc
              then pure T.empty
              else pure (rowCell (rows V.! r) c)
        , _tblFetchMore   = pure Nothing
        , _tblHideCols    = \_ -> pure ops
        , _tblSortBy      = \_ _ -> pure ops
        }
  pure ops
