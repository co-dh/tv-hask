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
  , dispatch
  , commands
  ) where

import Data.Char (ord)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word32)
import System.Exit (ExitCode(..))
import System.Process (readProcessWithExitCode)

import Optics.Core ((&), (.~))

import Tv.App.Types (AppState(..), HandlerFn, tryStk, viewUp)
import Tv.CmdConfig (CmdInfo(..), Entry, mkEntry, hdl)
import Tv.Data.ADBC.Table (AdbcTable)
import qualified Tv.Data.ADBC.Table as Table
import qualified Tv.Data.ADBC.Ops as Ops
import qualified Tv.FileFormat as FileFormat
import qualified Tv.SourceConfig as SourceConfig
import Tv.SourceConfig (Config)
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import Tv.Types
  ( Cmd(..)
  , ViewKind(..)
  )
import qualified Tv.Util as Log
import qualified Tv.Util as Remote
import Tv.View (View, ViewStack)
import qualified Tv.View as View
import qualified Tv.Nav as Nav

-- | Strip base path prefix to get relative entry name
stripBase :: Text -> Text -> Text
stripBase base path_ =
  if T.isPrefixOf base path_
    then
      let rest = T.drop (T.length base) path_
      in if T.isPrefixOf "/" rest then T.drop 1 rest else rest
    else if T.isPrefixOf "./" path_ then T.drop 2 path_
    else path_

-- | Format date: "2024-01-05+12:34:56.123" -> "2024-01-05 12:34:56"
fmtDate :: Text -> Text
fmtDate s =
  let replaced = T.replace "+" " " s
      head_ = case T.splitOn "." replaced of
                (x:_) -> x
                []    -> ""
  in T.take 19 head_

-- | Format type: f->file, d->dir, l->symlink
fmtType :: Text -> Text
fmtType s = case s of
  "f" -> "file"
  "d" -> "dir"
  "l" -> "symlink"
  x   -> x

-- | List directory with find command, returns tab-separated output
listDir :: Text -> Int -> IO Text
listDir path_ depth = do
  let p = if T.null path_ then "." else path_
  (_, out, _) <- Log.run "find" "find"
    [ "-H", T.unpack p, "-maxdepth", show depth, "-printf", "%y\t%s\t%T+\t%p\n" ]
  let lines_ = filter (not . T.null) (T.splitOn "\n" (T.pack out))
      hdr = "name\tsize\tmodified\ttype"
      parentEntry = "..\t0\t\tdir"
      body = map mkRow (drop 1 lines_)
      mkRow line =
        let parts = T.splitOn "\t" line
            getD i = Log.getD parts i ""
            getLastD = case reverse parts of
                         (x:_) -> x
                         []    -> ""
        in if length parts >= 4
             then
               let typ = fmtType (getD 0)
                   sz  = getD 1
                   dt  = fmtDate (getD 2)
                   pp  = stripBase p getLastD
               in T.intercalate "\t" [pp, sz, dt, typ]
             else line
  pure (hdr <> "\n" <> parentEntry
         <> (if null body then "" else "\n" <> T.intercalate "\n" body))

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
  case cols V.!? 0 of
    Just c  -> pure (fromMaybe "" (c V.!? 0))
    Nothing -> pure ""

-- | Get path column value from current row
curPath :: View AdbcTable -> IO (Maybe Text)
curPath v = case View.vkind v of
  VkFld _ _ ->
    case pathIdx (Nav.colNames (View.nav v)) of
      Just col -> Just <$> cellStr v col
      Nothing  -> pure Nothing
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
       (View.fromTbl adbc path_ 0 grp 0)

-- | Create folder view — config-driven listing, local fallback
mkView :: Bool -> Text -> Int -> IO (Maybe (View AdbcTable))
mkView noSign_ path_ depth = do
  msrc <- SourceConfig.findSource path_
  case msrc of
    Just cfg -> do
      m <- SourceConfig.runList noSign_ cfg path_
      case m of
        Nothing -> pure Nothing
        Just adbc -> do
          let grp = if T.null (SourceConfig.grp cfg) then V.empty
                    else V.singleton (SourceConfig.grp cfg)
          pure (fldView adbc path_ depth (Remote.dispName path_) grp)
    Nothing -> do
      (ec, rpOut, _) <- readProcessWithExitCode "realpath" [T.unpack path_] ""
      let absPath = if ec == ExitSuccess then T.strip (T.pack rpOut) else path_
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
  let path_ = case mp of
        Just p  -> p
        Nothing -> case View.vkind (View.cur s) of
          VkFld p _ -> p
          _         -> "."
  m <- mkView noSign_ path_ 1
  case m of
    Just v  -> pure (Just (View.push s v))
    Nothing -> pure Nothing

-- | Join parent path with entry name (works for local, S3, HF)
joinPath :: Text -> Text -> Text
joinPath parent_ entry =
  if parent_ == "." then "./" <> entry
  else Remote.joinRemote parent_ entry

-- | Try to create a view; push or setCur, fallback to original stack
tryView :: Bool -> ViewStack AdbcTable -> Text -> Int -> Bool -> IO (Maybe (ViewStack AdbcTable))
tryView noSign_ s path_ depth push_ = do
  m <- mkView noSign_ path_ depth
  case m of
    Just v  -> pure (Just (if push_ then View.push s v else View.setCur s v))
    Nothing -> pure (Just s)

-- | Get current folder depth
curDepth :: ViewStack AdbcTable -> Int
curDepth s = case View.vkind (View.cur s) of
  VkFld _ d -> d
  _         -> 1

-- | Go to parent directory (backspace key) — works for all folder backends
goParent :: Bool -> ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
goParent noSign_ s = do
  let dir_ = View.curDir (View.cur s)
  msrc <- SourceConfig.findSource dir_
  case msrc of
    Just c -> case SourceConfig.configParent c dir_ of
      Just par -> tryView noSign_ s par 1 False
      Nothing  -> pure (Just s)
    Nothing -> case View.pop s of
      Just s' -> pure (Just s')
      Nothing -> tryView noSign_ s (dir_ <> "/..") (curDepth s) False

-- | Try to open a file as data, fall back to viewer
openFile :: Bool -> Bool -> ViewStack AdbcTable -> Text -> Text -> Maybe Config
         -> IO (Maybe (ViewStack AdbcTable))
openFile tm noSign_ s curDir_ p cfg = do
  let fullPath = joinPath curDir_ p
  if FileFormat.isData p
    then do
      openPath <- case cfg of
        Just c  -> SourceConfig.configResolve noSign_ c fullPath
        Nothing -> pure fullPath
      m <- FileFormat.openFile openPath
      case m of
        Just v  -> pure (Just (View.push s v))
        Nothing -> do
          case cfg of
            Nothing -> FileFormat.viewFile tm fullPath
            _       -> pure ()
          pure (Just s)
    else do
      viewPath <- case cfg of
        Just c  -> SourceConfig.runDl noSign_ c fullPath
        Nothing -> pure fullPath
      done <-
        if T.isSuffixOf ".gz" p
          then do
            mv <- FileFormat.readCsv viewPath
            case mv of
              Just v  -> pure (Just (View.push s v))
              Nothing -> pure Nothing
          else pure Nothing
      case done of
        Just s' -> pure (Just s')
        Nothing -> do
          FileFormat.viewFile tm viewPath
          pure (Just s)

-- | Enter directory or view file based on current row
enter :: Bool -> Bool -> ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
enter tm noSign_ s = do
  let curDir_ = View.curDir (View.cur s)
  cfg <- SourceConfig.findSource curDir_
  -- Attach-based: FileFormat or config-driven (e.g. pg://)
  let isAttach =
        maybe False FileFormat.attach (FileFormat.find curDir_)
        || maybe False SourceConfig.attach cfg
  if isAttach
    then do
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
    else do
      mTyp <- curType (View.cur s)
      mPth <- curPath (View.cur s)
      case (mTyp, mPth) of
        (Just 'd', Just p) ->
          if p == ".." || T.isSuffixOf "/.." p
            then goParent noSign_ s
            else
              let fullPath = case cfg of
                    Just c  -> joinPath curDir_
                                 (if SourceConfig.dirSuffix c then p <> "/" else p)
                    Nothing -> joinPath curDir_ p
              in tryView noSign_ s fullPath (curDepth s) True
        (Just 'f', Just p) -> do
          -- Config-driven enter: script cmd -> JSON, or enterUrl redirect
          case cfg of
            Just c | not (T.null (SourceConfig.script c)) -> do
              mAdbc <- SourceConfig.runEnter c p
              case mAdbc of
                Just adbc ->
                  case View.fromTbl adbc (SourceConfig.pfx c <> p) 0 V.empty 0 of
                    Just v  -> pure (Just (View.push s v))
                    Nothing -> pure (Just s)
                Nothing -> pure (Just s)
            Just c | not (T.null (SourceConfig.enterUrl c)) -> do
              let url = SourceConfig.expand (SourceConfig.enterUrl c)
                          (V.singleton ("name", p))
              tryView noSign_ s url (curDepth s) True
            _ -> openFile tm noSign_ s curDir_ p cfg
        (Just 's', Just p) ->
          case cfg of
            Just _  -> pure (Just s)
            Nothing -> do
              let fullPath = joinPath curDir_ p
              (ec, _, _) <- readProcessWithExitCode "test" ["-d", T.unpack fullPath] ""
              if ec == ExitSuccess
                then tryView noSign_ s fullPath (curDepth s) True
                else openFile tm noSign_ s curDir_ p Nothing
        _ -> pure Nothing

-- | Get trash command (trash-put or gio trash)
trashCmd :: IO (Maybe (String, [String]))
trashCmd = do
  (ec1, _, _) <- readProcessWithExitCode "which" ["trash-put"] ""
  if ec1 == ExitSuccess
    then pure (Just ("trash-put", []))
    else do
      (ec2, _, _) <- readProcessWithExitCode "which" ["gio"] ""
      if ec2 == ExitSuccess
        then pure (Just ("gio", ["trash"]))
        else pure Nothing

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
        pure (V.map (\r -> joinPath curDir_ (fromMaybe "" (c V.!? r))) rows)
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
  VkFld path_ depth -> do
    msrc <- SourceConfig.findSource path_
    case msrc of
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
  VkFld path_ depth -> do
    msrc <- SourceConfig.findSource path_
    case msrc of
      Just _  -> pure (Just s)
      Nothing -> do
        let newDepth = max 1 (depth + delta)
        if newDepth == depth
          then pure (Just s)
          else refreshView noSign_ s path_ newDepth
  _ -> pure Nothing

-- | Dispatch folder handler to IO action. Returns Nothing if handler not recognized.
dispatch :: Bool -> Bool -> ViewStack AdbcTable -> Cmd
         -> Maybe (IO (Maybe (ViewStack AdbcTable)))
dispatch tm noSign_ s h =
  let isFld = case View.vkind (View.cur s) of { VkFld _ _ -> True; _ -> False }
  in case h of
    CmdFolderPush     -> Just (push noSign_ s)
    CmdFolderDepthInc -> Just (setDepth noSign_ s 1)
    CmdFolderDepthDec -> Just (setDepth noSign_ s (-1))
    CmdFolderDel      -> if isFld then Just (del tm noSign_ s) else Nothing
    CmdFolderParent   -> if isFld then Just (goParent noSign_ s) else Nothing
    CmdFolderEnter    -> if isFld then Just (enter tm noSign_ s) else Nothing
    _                 -> Nothing

folderH :: HandlerFn
folderH = \a ci _ ->
  case dispatch (testMode a) (noSign a) (stk a) (ciCmd ci) of
    Just f  -> tryStk a ci f
    Nothing -> viewUp a ci

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdFolderPush     "r" "D"     "Browse folder"              True  "")    folderH
  , hdl (mkEntry CmdFolderEnter    "r" "<ret>" "Open file or enter directory" True "fld") folderH
  , hdl (mkEntry CmdFolderParent   ""  "<bs>"  "Go to parent directory"      True "fld") folderH
  , hdl (mkEntry CmdFolderDel      "r" ""      "Move to trash"              True  "")    folderH
  , hdl (mkEntry CmdFolderDepthDec ""  ""      "Decrease folder depth"      True  "")    folderH
  , hdl (mkEntry CmdFolderDepthInc ""  ""      "Increase folder depth"      True  "")    folderH
  ]

