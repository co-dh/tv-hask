{-# LANGUAGE ScopedTypeVariables #-}
-- | Folder: directory browser. Builds a read-only TblOps from a local
-- directory listing with columns name/size/modified/type. Used both as
-- the default view (no CLI args) and on folder.enter for subdirectories.
--
-- Mirrors Tc.Folder.listDir in shape (same column names) so navigation
-- tests written against the mock folder layout stay portable.
module Tv.Folder
  ( listFolder
  , listFolderDepth
  , folderDelH
  , folderDepthAdjH
  , folderDepthIncH
  , folderDepthDecH
  ) where

import Control.Exception (displayException, catch, SomeException)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Format (formatTime, defaultTimeLocale)
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.List (sortBy)
import Data.Ord (comparing, Down(..))
import System.Directory
  ( createDirectoryIfMissing, doesDirectoryExist, doesPathExist
  , getHomeDirectory, getModificationTime, getFileSize, listDirectory
  , pathIsSymbolicLink, renamePath )
import System.FilePath ((</>))

import Tv.Types
import Tv.Render (headNav, headTbl)
import Tv.Eff (Eff, IOE, (:>), use, (.=), liftIO, tryEitherE)
import Tv.Handler (Handler, refreshGrid, setMsg)
import Optics.Core ((^.), (%))

-- | Move the current row's file to @~/.local/share/Trash/files/@
-- (XDG trash). Collisions get @.1@, @.2@, … suffixes.
folderDelH :: Handler
folderDelH _ = do
  ns <- use headNav
  case ns ^. nsVkind of
    VFld base _ -> do
      let r = ns ^. nsRow % naCur
      name <- liftIO ((ns ^. nsTbl % tblCellStr) r 0)
      if T.null name || name == ".."
        then setMsg "del: nothing to trash"
        else do
          home <- liftIO getHomeDirectory
          let trashDir = home </> ".local/share/Trash/files"
              srcPath = T.unpack base </> T.unpack name
          liftIO (createDirectoryIfMissing True trashDir)
          dest <- liftIO (uniqueTrashPath trashDir (T.unpack name) (0 :: Int))
          r1 <- tryEitherE (liftIO (renamePath srcPath dest))
          case r1 of
            Left e -> setMsg ("del failed: " <> T.pack (displayException e))
            Right _ -> do
              ops' <- listFolderDepth (T.unpack base) 1
              headTbl .= ops'
              refreshGrid
              setMsg ("trashed " <> name)
    _ -> setMsg "del: not in a folder view"
  where
    uniqueTrashPath dir base n = do
      let cand = if n == 0 then dir </> base else dir </> (base <> "." <> show n)
      ex <- doesPathExist cand
      if ex then uniqueTrashPath dir base (n + 1) else pure cand

-- | Bump the folder recursion depth by @d@ (clamped 1..5), rebuild the
-- folder TblOps, and update the view kind.
folderDepthAdjH :: Int -> Handler
folderDepthAdjH d _ = do
  vk <- use (headNav % nsVkind)
  case vk of
    VFld base depth -> do
      let d' = max 1 (min 5 (depth + d))
      ops' <- listFolderDepth (T.unpack base) d'
      headTbl .= ops'
      headNav % nsVkind .= VFld base d'
      refreshGrid
      setMsg ("depth=" <> T.pack (show d'))
    _ -> setMsg "depth: not in a folder view"

folderDepthIncH, folderDepthDecH :: Handler
folderDepthIncH = folderDepthAdjH 1
folderDepthDecH = folderDepthAdjH (-1)

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
listFolder :: IOE :> es => FilePath -> Eff es TblOps
listFolder p = listFolderDepth p 1

-- | Build a read-only TblOps for a directory, recursing @depth@ levels.
-- Columns: name, size, modified, type. Includes a ".." parent entry
-- first (matching Tc).
listFolderDepth :: IOE :> es => FilePath -> Int -> Eff es TblOps
listFolderDepth path depth = liftIO $ do
  entries <- collect path "" (max 1 depth)
  let parent = Entry ".." "0" "" "dir"
      rows = V.fromList (parent : entries)
      cols = V.fromList ["name", "size", "modified", "type"]
  pure (mkFolderOps rows cols)

-- | Build folder TblOps from row data (shared between listFolderDepth and sort).
mkFolderOps :: Vector Entry -> Vector Text -> TblOps
mkFolderOps rows cols =
  let nr = V.length rows
      nc = V.length cols
      rowCell e i = case i of
        0 -> eName e; 1 -> eSize e; 2 -> eMod e; 3 -> eType e; _ -> T.empty
      ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = cols
        , _tblTotalRows   = nr
        , _tblQueryOps    = V.empty
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
        , _tblSortBy      = \idxs asc -> do
            let sortCol = if V.null idxs then 0 else V.head idxs
                cmpFn = if asc then comparing id else comparing Down
                sorted = V.cons (V.head rows) $
                  V.fromList $ sortBy (\a b -> cmpFn (rowCell a sortCol) (rowCell b sortCol))
                                      (V.toList (V.tail rows))
            pure (mkFolderOps sorted cols)
        }
  in ops
