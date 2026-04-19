{-
  Folder: directory browser with configurable find depth
  Commands: .fld .dup (push), .fld .inc/.dec (depth), .fld .ent (enter)

  Literal port of Tc/Tc/Folder.lean — same function set, same order, same
  comments. Refactor only after parity.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Folder
  ( listDir
  , curPath
  , curType
  , mkView
  , push
  , goParent
  , enter
  , trashCmd
  , selPaths
  , drawDialog
  , waitYN
  , confirmDel
  , trashFiles
  , del
  , setDepth
  , commands
  ) where

import Control.Exception (SomeException, try)
import Data.Char (ord)
import Data.List (isPrefixOf)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock.POSIX (POSIXTime, posixSecondsToUTCTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word32)
import System.Directory (canonicalizePath, doesDirectoryExist, findExecutable, listDirectory)
import System.Exit (ExitCode(..))
import System.FilePath ((</>))
import qualified System.Posix.Files as Posix

import Optics.Core ((&), (.~))

import Tv.App.Types (AppState(..), HandlerFn, tryStk, viewUp)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import Tv.Data.DuckDB.Table (AdbcTable)
import qualified Tv.Data.DuckDB.Table as Table
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.FileFormat as FileFormat
import qualified Tv.Source as Source
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import Tv.Types
  ( Cmd(..)
  , ViewKind(..)
  , getD
  )
import qualified Tv.Log as Log
import qualified Tv.Remote as Remote
import Tv.View (View, ViewStack)
import qualified Tv.View as View
import qualified Tv.Nav as Nav

-- | Per-entry tuple from the directory walk: (type, size, mtime, fullPath).
type DirEntry = (Char, Int, POSIXTime, FilePath)

-- | List directory contents up to `depth` levels deep. Returns the same
-- tab-separated format the previous `find -printf` shellout produced:
-- header line + ".." parent row + one row per descendant.
-- Row format: name<TAB>size<TAB>modified ("YYYY-MM-DD HH:MM:SS")<TAB>type.
listDir :: Text -> Int -> IO Text
listDir path_ depth = do
  let p = if T.null path_ then "." else T.unpack path_
  entries <- walkChildren p depth
  let hdr         = "name\tsize\tmodified\ttype"
      parentEntry = "..\t0\t\tdir"
      body        = map (mkRow p) entries
  pure (hdr <> "\n" <> parentEntry
         <> (if null body then "" else "\n" <> T.intercalate "\n" body))

-- | Format a single entry to the tab-separated row.
mkRow :: FilePath -> DirEntry -> Text
mkRow base (ty, sz, mt, fp) =
  T.intercalate "\t" [relPath base fp, T.pack (show sz), fmtTime mt, typName ty]
  where
    typName 'f' = "file"
    typName 'd' = "dir"
    typName 'l' = "symlink"
    typName _   = ""
    fmtTime t = T.pack $ formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S"
                        (posixSecondsToUTCTime t)

-- | Strip the leading `base` so the displayed path is relative to it,
-- matching the previous behavior of stripping the find argument prefix.
relPath :: FilePath -> FilePath -> Text
relPath base full
  | base `isPrefixOf` full =
      let r = drop (length base) full
      in T.pack (if "/" `isPrefixOf` r then drop 1 r else r)
  | "./" `isPrefixOf` full = T.pack (drop 2 full)
  | otherwise              = T.pack full

-- | Recursively list children of `dir` up to `depth` levels.
-- Mirrors `find -maxdepth depth` (the root itself is excluded; only
-- descendants are returned, matching the previous `drop 1 lines_`).
walkChildren :: FilePath -> Int -> IO [DirEntry]
walkChildren dir depth
  | depth <= 0 = pure []
  | otherwise = do
      r <- try (listDirectory dir) :: IO (Either SomeException [FilePath])
      case r of
        Left _    -> pure []
        Right xs  -> concat <$> mapM (visit dir depth) xs

visit :: FilePath -> Int -> FilePath -> IO [DirEntry]
visit dir depth name = do
  let full = dir </> name
  r <- try (Posix.getSymbolicLinkStatus full) :: IO (Either SomeException Posix.FileStatus)
  case r of
    Left _  -> pure []
    Right s -> do
      let me = (typeChar s, fromIntegral (Posix.fileSize s),
                Posix.modificationTimeHiRes s, full)
      kids <- if Posix.isDirectory s && depth > 1
                then walkChildren full (depth - 1)
                else pure []
      pure (me : kids)

typeChar :: Posix.FileStatus -> Char
typeChar s
  | Posix.isSymbolicLink s = 'l'
  | Posix.isDirectory s    = 'd'
  | Posix.isRegularFile s  = 'f'
  | otherwise              = '?'

-- | Find path column index (tries "path", "name", "id" in order)
pathIdx :: Vector Text -> Maybe Int
pathIdx names =
  case Nav.idxOf names "path" of
    Just i  -> Just i
    Nothing -> case Nav.idxOf names "name" of
      Just i  -> Just i
      Nothing -> Nav.idxOf names "id"

-- | Get single cell value as string from current row
cellStr :: View AdbcTable -> Int -> IO Text
cellStr v colIdx = do
  let rowCur = Nav.cur (Nav.row (View.nav v))
  cols <- Ops.getCols (Nav.tbl (View.nav v)) (V.singleton colIdx) rowCur (rowCur + 1)
  pure $ fromMaybe "" $ cols V.!? 0 >>= (V.!? 0)

-- | Get path column value from current row
curPath :: View AdbcTable -> IO (Maybe Text)
curPath v = case View.vkind v of
  VkFld _ _ ->
    maybe (pure Nothing) (fmap Just . cellStr v) $
      pathIdx (Nav.colNames (View.nav v))
  _ -> pure Nothing

-- | Get type column value from current row
-- Normalizes: 'f'/"file" -> 'f', 'd'/"dir" -> 'd', ' ' (HF/S3 file) -> 'f'
curType :: View AdbcTable -> IO (Maybe Char)
curType v = case View.vkind v of
  VkFld _ _ ->
    case Nav.idxOf (Nav.colNames (View.nav v)) "type" of
      Nothing      -> pure (Just 'f')
      Just typeCol -> do
        s <- cellStr v typeCol
        case T.unpack s of
          (' ':_) -> pure (Just 'f')  -- HF/S3 space = file
          (c:_)   -> pure (Just c)
          []      -> pure Nothing
  _ -> pure Nothing

-- | Build folder view from AdbcTable
fldView :: AdbcTable -> Text -> Int -> Text -> Vector Text -> Maybe (View AdbcTable)
fldView adbc path_ depth disp grp =
  fmap (\v -> v & #vkind .~ VkFld path_ depth & #disp .~ disp)
       $ View.fromTbl adbc path_ 0 grp 0

-- | Create folder view — config-driven listing, local fallback
mkView :: Bool -> Text -> Int -> IO (Maybe (View AdbcTable))
mkView noSign_ path_ depth = case Source.findSource path_ of
  Just src -> do
    m <- Source.runList noSign_ src path_
    case m of
      Nothing -> pure Nothing
      Just adbc -> do
        let grp = maybe V.empty V.singleton (Source.grpCol src)
        pure $ fldView adbc path_ depth (Remote.dispName path_) grp
  Nothing -> do
      -- canonicalizePath throws on missing paths; fall back to the input on error
      rp <- try (canonicalizePath (T.unpack path_)) :: IO (Either SomeException FilePath)
      let absPath = case rp of
            Right p -> T.pack p
            Left _  -> path_
          disp = case reverse (T.splitOn "/" absPath) of
                   (x:_) -> x
                   []    -> absPath
      content <- listDir path_ depth
      m <- Table.fromTsv content
      case m of
        Nothing -> pure Nothing
        Just adbc -> pure (fldView adbc absPath depth disp V.empty)

-- | Push new folder view onto stack
push :: Bool -> ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
push noSign_ s = do
  mp <- curPath (View.cur s)
  let path_ = fromMaybe (case View.vkind (View.cur s) of
                           VkFld p _ -> p
                           _         -> ".") mp
  fmap (View.push s) <$> mkView noSign_ path_ 1

-- | Join parent path with entry name (works for local, S3, HF)
joinPath :: Text -> Text -> Text
joinPath parent_ entry
  | parent_ == "." = "./" <> entry
  | otherwise      = Remote.joinRemote parent_ entry

-- | Try to create a view; push or setCur, fallback to original stack
tryView :: Bool -> ViewStack AdbcTable -> Text -> Int -> Bool -> IO (Maybe (ViewStack AdbcTable))
tryView noSign_ s path_ depth push_ =
  Just . maybe s (\v -> if push_ then View.push s v else View.setCur s v)
    <$> mkView noSign_ path_ depth

-- | Get current folder depth
curDepth :: ViewStack AdbcTable -> Int
curDepth s = case View.vkind (View.cur s) of
  VkFld _ d -> d
  _         -> 1

-- | Go to parent directory (backspace key) — works for all folder backends
goParent :: Bool -> ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
goParent noSign_ s = do
  let dir_ = View.curDir (View.cur s)
  case Source.findSource dir_ of
    Just src -> case Source.configParent src dir_ of
      Just par -> tryView noSign_ s par 1 False
      Nothing  -> pure (Just s)
    Nothing -> case View.pop s of
      Just s' -> pure (Just s')
      Nothing -> tryView noSign_ s (dir_ <> "/..") (curDepth s) False

-- | Open a local file: data → FileFormat reader, else hexdump/text viewer.
-- `fullPath` is the path used for display; `viewPath` is what the reader/viewer
-- actually opens (may be a local download of a URI).
openLocal :: Bool -> ViewStack AdbcTable -> Text -> Text -> FilePath
          -> IO (Maybe (ViewStack AdbcTable))
openLocal tm s fullPath p viewPath =
  if FileFormat.isData p
    then do
      m <- FileFormat.openFile (T.pack viewPath)
      case m of
        Just v  -> pure (Just (View.push s v))
        Nothing -> do FileFormat.viewFile tm fullPath; pure (Just s)
    else do
      done <-
        if T.isSuffixOf ".gz" p
          then do
            mv <- FileFormat.readCsv (T.pack viewPath)
            pure $ fmap (View.push s) mv
          else pure Nothing
      case done of
        Just s' -> pure (Just s')
        Nothing -> do FileFormat.viewFile tm (T.pack viewPath); pure (Just s)

-- | FileFormat-attach enter: curDir_ is a .duckdb file, rows are tables.
enterAttach :: ViewStack AdbcTable -> Text -> IO (Maybe (ViewStack AdbcTable))
enterAttach s curDir_ = do
  mTbl <- curPath (View.cur s)
  case mTbl of
    Nothing -> pure (Just s)
    Just tableName -> do
      mAK <- Table.fromTable tableName
      case mAK of
        Nothing -> pure (Just s)
        Just (adbc, keys) ->
          case View.fromTbl adbc (curDir_ <> ":" <> tableName) 0 keys 0 of
            Nothing -> pure (Just s)
            Just v  -> pure (Just (View.push s v))

-- | Dispatch a source's `open` result.
handleOpen :: Bool -> Bool -> ViewStack AdbcTable -> Text -> Text -> Source.OpenResult
           -> IO (Maybe (ViewStack AdbcTable))
handleOpen tm noSign_ s fullPath p res = case res of
  Source.OpenAsTable adbc ->
    case View.fromTbl adbc fullPath 0 V.empty 0 of
      Just v  -> pure (Just (View.push s v))
      Nothing -> pure (Just s)
  Source.OpenAsFile localPath -> openLocal tm s fullPath p localPath
  Source.OpenAsDir  uri       -> do
    m <- mkView noSign_ uri 1
    pure $ maybe (Just s) (Just . View.push s) m
  Source.OpenNothing -> pure (Just s)

-- | Enter directory or view file based on current row
enter :: Bool -> Bool -> ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
enter tm noSign_ s = do
  let curDir_ = View.curDir (View.cur s)
      src = Source.findSource curDir_
  -- FileFormat-attach: the current view is a .duckdb file, rows are tables.
  -- Source-level attach (pg://) is now handled via `open → OpenAsTable`.
  if maybe False FileFormat.attach (FileFormat.find curDir_)
    then enterAttach s curDir_
    else do
      mTyp <- curType (View.cur s)
      mPth <- curPath (View.cur s)
      case (mTyp, mPth) of
        (Just 'd', Just p) ->
          if p == ".." || T.isSuffixOf "/.." p
            then goParent noSign_ s
            else
              -- append '/' so source.open knows this is a directory
              let fullPath = case src of
                    Just _  -> joinPath curDir_ (p <> "/")
                    Nothing -> joinPath curDir_ p
              in case src of
                   Just c -> do
                     r <- Source.runOpen noSign_ c fullPath
                     handleOpen tm noSign_ s fullPath p r
                   Nothing -> tryView noSign_ s fullPath (curDepth s) True
        (Just 'f', Just p) -> do
          let fullPath = joinPath curDir_ p
          case src of
            Just c  -> do r <- Source.runOpen noSign_ c fullPath
                          handleOpen tm noSign_ s fullPath p r
            Nothing -> openLocal tm s fullPath p (T.unpack fullPath)
        (Just 's', Just p) ->
          case src of
            Just _  -> pure (Just s)
            Nothing -> do
              let fullPath = joinPath curDir_ p
              isDir <- doesDirectoryExist (T.unpack fullPath)
              if isDir
                then tryView noSign_ s fullPath (curDepth s) True
                else openLocal tm s fullPath p (T.unpack fullPath)
        _ -> pure Nothing

-- | Get trash command (trash-put or gio trash)
trashCmd :: IO (Maybe (String, [String]))
trashCmd = do
  mTp <- findExecutable "trash-put"
  case mTp of
    Just _  -> pure (Just ("trash-put", []))
    Nothing -> do
      mGio <- findExecutable "gio"
      case mGio of
        Just _  -> pure (Just ("gio", ["trash"]))
        Nothing -> pure Nothing

-- | Get full paths of selected rows, or current row if none selected
selPaths :: View AdbcTable -> IO (Vector Text)
selPaths v = case View.vkind v of
  VkFld curDir_ _ ->
    case pathIdx (Nav.colNames (View.nav v)) of
      Nothing -> pure V.empty
      Just pathCol -> do
        cols <- Ops.getCols (Nav.tbl (View.nav v)) (V.singleton pathCol)
                  0 (View.nRows v)
        let c = fromMaybe V.empty (cols V.!? 0)
            rows =
              if V.null (Nav.sels (Nav.row (View.nav v)))
                then V.singleton (Nav.cur (Nav.row (View.nav v)))
                else Nav.sels (Nav.row (View.nav v))
        pure $ V.map (\r -> joinPath curDir_ (fromMaybe "" (c V.!? r))) rows
  _ -> pure V.empty

-- | Draw centered dialog box
drawDialog :: Text -> Vector Text -> Text -> IO ()
drawDialog title lines_ footer = do
  w <- Term.width
  h <- Term.height
  let maxLen = V.foldl' (\m l -> max m (T.length l))
                 (max (T.length title) (T.length footer)) lines_
      boxW = maxLen + 4
      boxH = V.length lines_ + 4
      x0 = (fromIntegral w - boxW) `div` 2
      y0 = (fromIntegral h - boxH) `div` 2
  s <- Theme.getStyles
  let fg = Theme.styleFg s Theme.sBar
      bg = Theme.styleBg s Theme.sBar
      w32 :: Int -> Word32
      w32 = fromIntegral
  Term.print (w32 x0) (w32 y0) fg bg
    ("┌" <> T.replicate (boxW - 2) "─" <> "┐")
  let tpad = (boxW - 2 - T.length title) `div` 2
  Term.print (w32 x0) (w32 (y0 + 1)) fg bg
    ("│" <> T.replicate tpad " " <> title
      <> T.replicate (boxW - 2 - tpad - T.length title) " " <> "│")
  Term.print (w32 x0) (w32 (y0 + 2)) fg bg
    ("│" <> T.replicate (boxW - 2) " " <> "│")
  V.iforM_ lines_ $ \i l -> do
    let ln = T.take (boxW - 4) l
    Term.print (w32 x0) (w32 (y0 + 3 + i)) fg bg
      ("│ " <> ln <> T.replicate (boxW - 3 - T.length ln) " " <> "│")
  let fpad = (boxW - 2 - T.length footer) `div` 2
  Term.print (w32 x0) (w32 (y0 + 3 + V.length lines_)) fg bg
    ("│" <> T.replicate fpad " " <> footer
      <> T.replicate (boxW - 2 - fpad - T.length footer) " " <> "│")
  Term.print (w32 x0) (w32 (y0 + 4 + V.length lines_)) fg bg
    ("└" <> T.replicate (boxW - 2) "─" <> "┘")
  Term.present

-- | Wait for y/n keypress
waitYN :: IO Bool
waitYN = do
  ev <- Term.pollEvent
  if Term.typ ev /= Term.eventKey
    then waitYN
    else
      let ch = Term.ch ev
          k  = Term.keyCode ev
          yC = fromIntegral (ord 'y')
          yU = fromIntegral (ord 'Y')
          nC = fromIntegral (ord 'n')
          nU = fromIntegral (ord 'N')
      in if ch == yC || ch == yU then pure True
         else if ch == nC || ch == nU || k == Term.keyEsc then pure False
         else waitYN

-- | Confirm deletion with popup dialog (auto-decline in test mode)
confirmDel :: Bool -> Vector Text -> IO Bool
confirmDel tm paths = do
  if tm then pure False
  else do
    let title = "Delete " <> T.pack (show (V.length paths)) <> " file(s)?"
        shown = V.take 6 paths
        lines0 =
          if V.length paths > 6
            then V.snoc shown ("... +" <> T.pack (show (V.length paths - 6)) <> " more")
            else shown
    cmd <- trashCmd
    let cmdInfo = case cmd of
          Just ("trash-put", _) -> "via trash-put (undo: trash-restore)"
          Just ("gio", _)       -> "via gio trash (undo: gio trash --restore)"
          _                     -> "no trash command found"
    let lines_ = V.snoc lines0 "" V.++ V.fromList [cmdInfo, "[Y]es  [N]o"]
    drawDialog title lines_ ""
    waitYN

-- | Trash files, returns true if all succeeded
trashFiles :: Vector Text -> IO Bool
trashFiles paths = do
  m <- trashCmd
  case m of
    Nothing -> pure False
    Just (cmd, baseArgs) -> do
      let step ok p = do
            (ec, _, _) <- Log.run "trash" cmd (baseArgs ++ [T.unpack p])
            pure (ok && ec == ExitSuccess)
      V.foldM' step True paths

-- | Refresh folder view at path/depth, preserving cursor row
refreshView :: Bool -> ViewStack AdbcTable -> Text -> Int
            -> IO (Maybe (ViewStack AdbcTable))
refreshView noSign_ s path_ depth = do
  m <- mkView noSign_ path_ depth
  case m of
    Nothing -> pure (Just s)
    Just v ->
      let oldRow = Nav.cur (Nav.row (View.nav (View.cur s)))
          row_ = min oldRow (if View.nRows v > 0 then View.nRows v - 1 else 0)
          mv' = View.fromTbl (Nav.tbl (View.nav v)) path_ 0 V.empty row_
      in pure $ fmap
        (\x -> View.setCur s x
                 { View.vkind = VkFld path_ depth
                 , View.disp = View.disp (View.cur s)
                 })
        mv'

del :: Bool -> Bool -> ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
del tm noSign_ s = case View.vkind (View.cur s) of
  VkFld path_ depth -> case Source.findSource path_ of
    Just _  -> pure (Just s)
    Nothing -> do
      paths <- selPaths (View.cur s)
      if V.null paths
        then pure Nothing
        else do
          ok <- confirmDel tm paths
          if not ok
            then pure (Just s)
            else do
              _ <- trashFiles paths
              refreshView noSign_ s path_ depth
  _ -> pure Nothing

setDepth :: Bool -> ViewStack AdbcTable -> Int -> IO (Maybe (ViewStack AdbcTable))
setDepth noSign_ s delta = case View.vkind (View.cur s) of
  VkFld path_ depth -> case Source.findSource path_ of
    Just _  -> pure (Just s)
    Nothing -> do
      let newDepth = max 1 (depth + delta)
      if newDepth == depth
        then pure (Just s)
        else refreshView noSign_ s path_ newDepth
  _ -> pure Nothing

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdFolderPush     "r" "D"     "Browse folder"               True  "")
        (\a ci _ -> tryStk a ci (push (noSign a) (stk a)))
  , hdl (mkEntry CmdFolderEnter    "r" "<ret>" "Open file or enter directory" True "fld")
        (\a ci _ -> tryStk a ci (enter (testMode a) (noSign a) (stk a)))
  , hdl (mkEntry CmdFolderParent   ""  "<bs>"  "Go to parent directory"       True "fld")
        (\a ci _ -> tryStk a ci (goParent (noSign a) (stk a)))
  , hdl (mkEntry CmdFolderDel      "r" ""      "Move to trash"               True  "")
        (\a ci _ -> case View.vkind (View.cur (stk a)) of
            VkFld _ _ -> tryStk a ci (del (testMode a) (noSign a) (stk a))
            _         -> viewUp a ci)
  , hdl (mkEntry CmdFolderDepthDec ""  ""      "Decrease folder depth"       True  "")
        (\a ci _ -> tryStk a ci (setDepth (noSign a) (stk a) (-1)))
  , hdl (mkEntry CmdFolderDepthInc ""  ""      "Increase folder depth"       True  "")
        (\a ci _ -> tryStk a ci (setDepth (noSign a) (stk a) 1))
  ]

