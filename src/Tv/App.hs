-- | App: brick application wiring — appDraw, appHandleEvent, appAttrMap.
-- Handler dispatch uses a Map Cmd Handler combinator pattern.
module Tv.App where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.Vector as V
import Control.Monad (forM)
import Control.Monad.IO.Class (liftIO)
import qualified Brick
import qualified Brick.Main as BM
import qualified Brick.AttrMap as A
import qualified Graphics.Vty as Vty
import qualified Graphics.Vty.Platform.Unix as VtyUnix
import System.Environment (getArgs)
import System.Directory
  ( doesDirectoryExist, getCurrentDirectory, canonicalizePath
  , getHomeDirectory, createDirectoryIfMissing, doesPathExist, renamePath )
import System.FilePath (takeFileName, takeExtension, (</>))
import System.IO (hPutStrLn, stderr)
import Control.Exception (try, SomeException, displayException)
import Data.Char (toLower)
import Tv.Types
import Tv.View
import qualified Tv.Nav as Nav
import Tv.Render (AppState(..), Name(..), drawApp, appAttrMap)
import Tv.Key (evToKey)
import Tv.CmdConfig (keyLookup, CmdInfo(..))
import qualified Tv.CmdConfig as CC
import Tv.Theme (initTheme, toAttrMap, ThemeState(..))
import qualified Tv.Data.DuckDB as DB
import qualified Tv.Folder as Folder
import qualified Tv.Meta as Meta
import qualified Tv.Transpose as Transpose
import qualified Tv.Derive as Derive
import qualified Tv.Split as Split
import qualified Tv.Diff as Diff
import qualified Tv.Join as Join
import qualified Tv.Session as Session
import qualified Tv.Export as Export
import qualified Tv.Fzf as Fzf
import Data.Word (Word8)

-- ============================================================================
-- Handler combinators
-- ============================================================================

-- | Pure handler: updates state, IO allowed for cell refreshing. Nothing = halt.
type Handler = AppState -> IO (Maybe AppState)

-- | Apply a NavState transform to the head view.
withNav :: (NavState -> Maybe NavState) -> Handler
withNav f st =
  let vs = asStack st
      hd = _vsHd vs
      ns = _vNav hd
  in case f ns of
       Nothing -> pure (Just st)
       Just ns' -> Just <$> refreshGrid st { asStack = vs { _vsHd = hd { _vNav = ns' } } }

-- | Quit handler — returning Nothing signals halt.
quitH :: Handler
quitH _ = pure Nothing

-- | Handler map. Single source of truth for Cmd → behaviour.
-- Extended incrementally as features land. TODOs at the bottom are commands
-- whose source modules exist but need extra UI plumbing (prompts, fzf,
-- search bar) that hasn't landed yet.
handlerMap :: Map Cmd Handler
handlerMap = Map.fromList $
  [ -- in-place navigation (Nav.execNav returns a new NavState)
    (RowInc,    withNav (Nav.execNav RowInc  pageRows))
  , (RowDec,    withNav (Nav.execNav RowDec  pageRows))
  , (RowPgdn,   withNav (Nav.execNav RowPgdn pageRows))
  , (RowPgup,   withNav (Nav.execNav RowPgup pageRows))
  , (RowTop,    withNav (Nav.execNav RowTop  pageRows))
  , (RowBot,    withNav (Nav.execNav RowBot  pageRows))
  , (ColInc,    withNav (Nav.execNav ColInc  pageRows))
  , (ColDec,    withNav (Nav.execNav ColDec  pageRows))
  , (ColFirst,  withNav (Nav.execNav ColFirst pageRows))
  , (ColLast,   withNav (Nav.execNav ColLast  pageRows))
  , (RowSel,    withNav (Nav.execNav RowSel  pageRows))
  , (ColGrp,    withNav (Nav.execNav ColGrp  pageRows))
  , (ColHide,   withNav (Nav.execNav ColHide pageRows))
  , (ColShiftL, withNav (Nav.execNav ColShiftL pageRows))
  , (ColShiftR, withNav (Nav.execNav ColShiftR pageRows))
    -- sort (ops-producing)
  , (SortAsc,   sortH True)
  , (SortDesc,  sortH False)
  , (ColExclude, excludeH)
    -- heat mode + precision
  , (Heat0,  heatH 0)
  , (Heat1,  heatH 1)
  , (Heat2,  heatH 2)
  , (Heat3,  heatH 3)
  , (PrecDec,  precH (subtract 1))
  , (PrecInc,  precH (+ 1))
  , (PrecZero, precH (const 0))
  , (PrecMax,  precH (const 17))
    -- stack / quit / folder
  , (TblQuit, quitH)
  , (FolderEnter, folderEnterH)
  , (FolderParent, folderParentH)
  , (FolderPush, folderPushH)
  , (FolderDel, folderDelH)
  , (FolderDepthInc, folderDepthIncH)
  , (FolderDepthDec, folderDepthDecH)
  , (StkPop, stkPopH)
  , (StkSwap, stkSwapH)
  , (StkDup,  stkDupH)
    -- meta / xpose / derive / split / diff / join / export / session
  , (MetaPush, metaPushH)
  , (TblXpose, xposeH)
  , (ColDerive, deriveH)
  , (ColSplit, splitH)
  , (TblDiff, diffH)
  , (TblJoin, joinH)
  , (TblExport, exportH)
  , (SessSave, sessSaveH)
  , (SessLoad, sessLoadH)
  , (InfoTog, infoTogH)
  -- NB: TblMenu is intentionally NOT here. It must run under Brick.suspendAndResume
  -- (see handleEvent) so fzf can grab /dev/tty while the brick UI is paused.
  -- TODO FreqOpen, FreqFilter: need freq view wiring (stub Freq module pending)
  -- TODO RowSearch, RowSearchNext, RowSearchPrev: need search-bar UI
  -- TODO MetaSetKey, MetaSelNull, MetaSelSingle: need meta dispatch plumbing
  ]
  where pageRows = 20  -- TODO: derive from terminal size at draw time

-- | Sort the current table by the current column. Builds a fresh TblOps
-- via _tblSortBy and replaces the head view in place.
sortH :: Bool -> Handler
sortH asc st = do
  let ns  = _vNav $ _vsHd $ asStack st
      ops = _nsTbl ns
      ci  = curColIdx ns
  ops' <- _tblSortBy ops (V.singleton ci) asc
  let hd' = (_vsHd (asStack st)) { _vNav = ns { _nsTbl = ops' } }
  Just <$> refreshGrid st { asStack = (asStack st) { _vsHd = hd' } }

-- | Drop the current column from the view. Delegates to _tblHideCols.
excludeH :: Handler
excludeH st = do
  let ns  = _vNav $ _vsHd $ asStack st
      ops = _nsTbl ns
      ci  = curColIdx ns
  ops' <- _tblHideCols ops (V.singleton ci)
  case fromTbl ops' (_vPath (curView st)) 0 V.empty 0 of
    Nothing -> pure (Just st { asMsg = "exclude: empty result" })
    Just v  -> Just <$> refreshGrid st { asStack = vsPush v (asStack st) }

-- | Set the heatmap mode on the head view's NavState.
heatH :: Word8 -> Handler
heatH m st =
  let ns = _vNav $ _vsHd $ asStack st
      hd = _vsHd (asStack st)
      hd' = hd { _vNav = ns { _nsHeatMode = m } }
  in Just <$> refreshGrid st { asStack = (asStack st) { _vsHd = hd' } }

-- | Adjust decimal precision on the head view's NavState, clamped 0..17.
precH :: (Int -> Int) -> Handler
precH f st =
  let ns = _vNav $ _vsHd $ asStack st
      p' = max 0 (min 17 (f (_nsPrecAdj ns)))
      hd = _vsHd (asStack st)
      hd' = hd { _vNav = ns { _nsPrecAdj = p' } }
  in Just <$> refreshGrid st { asStack = (asStack st) { _vsHd = hd' } }

-- | Open the space-bar command menu via fzf. Called from handleEvent under
-- Brick.suspendAndResume so fzf can grab /dev/tty.  The picker returns a
-- handler name string like "row.inc"; we convert it to a Cmd, run its
-- handler, and return the updated AppState.
runCmdMenu :: AppState -> IO AppState
runCmdMenu st = do
  let vctx = vkCtxStr $ _nsVkind $ _vNav $ _vsHd $ asStack st
  mh <- Fzf.cmdMode vctx
  case mh of
    Nothing -> pure st
    Just h -> case cmdFromStr h of
      Nothing -> pure st { asMsg = "unknown cmd: " <> h }
      Just c -> do
        r <- handleCmd c st
        pure (maybe st id r)

-- | Build a fresh View from a newly-produced TblOps and push onto stack.
-- Used by MetaPush / TblXpose / derive / split / diff / join handlers. The
-- new view inherits a VTbl (or caller-supplied) kind and a default cursor.
pushOps :: AppState -> Text -> ViewKind -> TblOps -> IO (Maybe AppState)
pushOps st path vk ops = case fromTbl ops path 0 V.empty 0 of
  Nothing -> pure (Just st { asMsg = "empty result" })
  Just v ->
    let v' = v { _vNav = (_vNav v) { _nsVkind = vk } }
    in Just <$> refreshGrid st { asStack = vsPush v' (asStack st) }

-- | Current TblOps of the top view.
curOps :: AppState -> TblOps
curOps st = _nsTbl $ _vNav $ _vsHd $ asStack st

-- | Current top view (for path/disp).
curView :: AppState -> View
curView = _vsHd . asStack

-- | Swap top two views in the stack.
stkSwapH :: Handler
stkSwapH st = Just <$> refreshGrid st { asStack = vsSwap (asStack st) }

-- | Duplicate the top view on the stack.
stkDupH :: Handler
stkDupH st = Just <$> refreshGrid st { asStack = vsDup (asStack st) }

-- | Push a column-metadata view computed from the current table.
metaPushH :: Handler
metaPushH st = do
  ops' <- Meta.mkMetaOps (curOps st)
  pushOps st (_vPath (curView st) <> " [meta]") VColMeta ops'

-- | Push a transposed-table view.
xposeH :: Handler
xposeH st = do
  ops' <- Transpose.mkTransposedOps (curOps st)
  pushOps st (_vPath (curView st) <> " [T]") VTbl ops'

-- | Derive a new column from @asCmd == "name = expr"@. No-op + message on
-- parse failure; fail-soft on DuckDB errors (addDerived returns original).
deriveH :: Handler
deriveH st = case Derive.parseDerive (asCmd st) of
  Nothing -> pure (Just st { asMsg = "derive: usage 'name = expr'", asCmd = "" })
  Just (n, e) -> do
    ops' <- Derive.addDerived (curOps st) n e
    s' <- pushOps st (_vPath (curView st) <> " +" <> n) VTbl ops'
    pure $ fmap (\x -> x { asCmd = "" }) s'

-- | Split the current column by regex taken from @asCmd@.
splitH :: Handler
splitH st
  | T.null (asCmd st) = pure (Just st { asMsg = "split: usage ':<regex>'" })
  | otherwise = do
      let ns = _vNav (curView st)
          ci = curColIdx ns
      ops' <- Split.splitColumn (curOps st) ci (asCmd st)
      s' <- pushOps st (_vPath (curView st) <> " [split]") VTbl ops'
      pure $ fmap (\x -> x { asCmd = "" }) s'

-- | Diff top two views on the stack. Requires at least two views.
diffH :: Handler
diffH st = case _vsTl (asStack st) of
  [] -> pure (Just st { asMsg = "diff: need 2 views on stack" })
  (v2:_) -> do
    let l = _nsTbl (_vNav (_vsHd (asStack st)))
        r = _nsTbl (_vNav v2)
    ops' <- Diff.diffTables l r
    pushOps st "[diff]" VTbl ops'

-- | Inner-join top two views on the grouped columns of the top view.
-- Mirrors Lean Join.runWith's default (empty key → inner join on nav.grp).
joinH :: Handler
joinH st = case _vsTl (asStack st) of
  [] -> pure (Just st { asMsg = "join: need 2 views on stack" })
  (v2:_) -> do
    let hdNs = _vNav (_vsHd (asStack st))
        l = _nsTbl hdNs
        r = _nsTbl (_vNav v2)
        keys = _nsGrp hdNs
    ops' <- Join.joinInner l r keys
    pushOps st "[join]" VTbl ops'

-- | Export the current table. @asCmd@ is the filename; format from extension.
exportH :: Handler
exportH st
  | T.null (asCmd st) = pure (Just st { asMsg = "export: usage ':<path.csv|json|ndjson>'" })
  | otherwise = do
      let path = T.unpack (asCmd st)
          ext = T.toLower $ T.pack $ drop 1 $ takeExtension path
      case Export.exportFmtFromText ext of
        Nothing -> pure (Just st { asMsg = "export: unknown fmt ." <> ext, asCmd = "" })
        Just fmt -> do
          r <- try (Export.exportTable (curOps st) fmt path)
          let msg = case r of
                Left (e :: SomeException) -> "export failed: " <> T.pack (displayException e)
                Right _ -> "exported to " <> T.pack path
          pure (Just st { asMsg = msg, asCmd = "" })

-- | Save the current stack as a named session (name from @asCmd@).
sessSaveH :: Handler
sessSaveH st = do
  mp <- Session.saveSession (asCmd st) (asStack st)
  let msg = case mp of
        Just p  -> "saved session " <> T.pack p
        Nothing -> "save failed"
  pure (Just st { asMsg = msg, asCmd = "" })

-- | Load a session by name. Haskell cannot rehydrate TblOps without
-- re-running the source query, so for now we just report the result
-- and leave the live stack alone.
-- TODO: rehydrate via openPath once SourceConfig replay lands.
sessLoadH :: Handler
sessLoadH st = do
  ms <- Session.loadSession (asCmd st)
  let msg = case ms of
        Just _  -> "loaded session (rehydrate TODO)"
        Nothing -> "load failed"
  pure (Just st { asMsg = msg, asCmd = "" })

-- | Toggle info overlay. AppState has no info flag yet (adding one would
-- touch Tv.Render which is off-limits), so this is a best-effort marker in
-- asMsg until the overlay lands. TODO: real toggle once AppState grows it.
infoTogH :: Handler
infoTogH st = pure (Just st { asMsg = "info: TODO (overlay not wired)" })

-- | Pop the top view; quit if it was the last.
stkPopH :: Handler
stkPopH st = case vsPop (asStack st) of
  Nothing  -> pure Nothing
  Just vs' -> Just <$> refreshGrid st { asStack = vs' }

-- | Folder enter: resolve the current row's path cell, load the target as
-- a new view and push it. Directories → listFolder, files → DuckDB read_*.
folderEnterH :: Handler
folderEnterH st = do
  let hd = _vsHd (asStack st)
      ns = _vNav hd
  case _nsVkind ns of
    VFld base _ -> do
      -- row, col 0 always holds the name in the folder TblOps we build.
      let r = _naCur (_nsRow ns)
      name <- _tblCellStr (_nsTbl ns) r 0
      if T.null name then pure (Just st)
      else openPath st (T.unpack base </> T.unpack name)
    _ -> pure (Just st)

-- | Parent: replace current folder view with one rooted at parent dir.
folderParentH :: Handler
folderParentH st = do
  let hd = _vsHd (asStack st)
      ns = _vNav hd
  case _nsVkind ns of
    VFld base _ -> openPath st (T.unpack base </> "..")
    _ -> pure (Just st)

-- | Rebuild asGrid for the visible window. Walks the TblOps _tblCellStr
-- IO function for each cell. Called after any handler that could change
-- the viewport or the underlying table — the single refresh path keeps
-- drawApp pure and lets it do zero IO.
refreshGrid :: AppState -> IO AppState
refreshGrid st = do
  let ns = _vNav $ _vsHd $ asStack st
      tbl = _nsTbl ns
      disp = _nsDispIdxs ns
      nr = _tblNRows tbl
      nc = V.length disp
      -- clamp viewport to current table bounds
      r0 = clamp 0 (max 1 nr) (asVisRow0 st)
      c0 = clamp 0 (max 1 nc) (asVisCol0 st)
      h = min (asVisH st) (max 0 (nr - r0))
      w = min (asVisW st) (max 0 (nc - c0))
  grid <- V.generateM h $ \ri ->
            V.generateM w $ \ci ->
              _tblCellStr tbl (r0 + ri) (disp V.! (c0 + ci))
  pure st { asGrid = grid, asVisRow0 = r0, asVisCol0 = c0 }

-- | Dispatch a Cmd via the handler map. Returns Nothing if the handler
-- asked to quit. Factored out so tests can exercise the dispatch path
-- without pumping a Brick EventM.
handleCmd :: Cmd -> AppState -> IO (Maybe AppState)
handleCmd cmd st = case Map.lookup cmd handlerMap of
  Nothing -> pure (Just st)
  Just h  -> h st

-- | Dispatch a canonical key string through keyLookup + handleCmd.
-- Mirrors handleEvent's logic without needing a Brick EventM, so tests
-- can drive the full key → command → state-update path in pure IO.
handleKey :: Text -> AppState -> IO (Maybe AppState)
handleKey key st = do
  let ctx = vkCtxStr $ _nsVkind $ _vNav $ _vsHd $ asStack st
  mci <- keyLookup key ctx
  case mci of
    Nothing -> pure (Just st)
    Just ci -> handleCmd (ciCmd ci) st

-- ============================================================================
-- Brick app definition
-- ============================================================================

app :: Brick.App AppState () Name
app = Brick.App
  { Brick.appDraw = drawApp
  , Brick.appChooseCursor = Brick.neverShowCursor
  , Brick.appHandleEvent = handleEvent
  , Brick.appStartEvent = pure ()
  , Brick.appAttrMap = \st -> appAttrMap (asStyles st)
  }

handleEvent :: Brick.BrickEvent Name () -> Brick.EventM Name AppState ()
handleEvent (Brick.VtyEvent (Vty.EvKey Vty.KEsc [])) = BM.halt
handleEvent (Brick.VtyEvent (Vty.EvResize w h)) = do
  st <- Brick.get
  -- 4 reserved lines: header + footer + tab + status. Floor at 1 visible row.
  let visH = max 1 (h - 4)
  st' <- liftIO $ refreshGrid st { asVisH = visH, asVisW = max 1 w }
  Brick.put st'
handleEvent (Brick.VtyEvent ev@(Vty.EvKey _ _)) = do
  st <- Brick.get
  let key = evToKey ev
      ctx = vkCtxStr $ _nsVkind $ _vNav $ _vsHd $ asStack st
  mci <- liftIO $ keyLookup key ctx
  case mci of
    Nothing -> pure ()
    Just ci
      -- TblMenu needs fzf, which takes over /dev/tty. Brick owns the
      -- terminal; suspendAndResume releases it for the duration of the
      -- IO action and re-grabs it afterwards.
      | ciCmd ci == TblMenu -> BM.suspendAndResume (runCmdMenu st)
      | otherwise -> do
          r <- liftIO $ handleCmd (ciCmd ci) st
          case r of
            Nothing  -> BM.halt
            Just st' -> Brick.put st'
handleEvent _ = pure ()

-- ============================================================================
-- Path → View loading
-- ============================================================================

-- | Pick the DuckDB SQL to materialize a file. Unknown extensions fall
-- back to @SELECT * FROM '...'@ (DuckDB auto-detect).
fileQuery :: FilePath -> Text
fileQuery p =
  let esc = T.replace "'" "''" (T.pack p)
      ext = map toLower (takeExtension p)
      ext2 = case ext of
        ".gz" -> map toLower (takeExtension (take (length p - 3) p))
        _     -> ext
      reader = case ext2 of
        ".parquet" -> "read_parquet"
        ".csv"     -> "read_csv_auto"
        ".tsv"     -> "read_csv_auto"
        ".json"    -> "read_json_auto"
        ".ndjson"  -> "read_json_auto"
        ".jsonl"   -> "read_json_auto"
        _          -> ""
  in if null reader
       then "SELECT * FROM '" <> esc <> "' LIMIT 1000"
       else "SELECT * FROM " <> T.pack reader <> "('" <> esc <> "') LIMIT 1000"

-- | Load a regular file as a TblOps via DuckDB. Returns Left on any error.
loadFile :: FilePath -> IO (Either String TblOps)
loadFile p = do
  r <- try $ do
    conn <- DB.connect ":memory:"
    res <- DB.query conn (fileQuery p)
    DB.mkDbOps res
  pure $ case r of
    Left (e :: SomeException) -> Left (displayException e)
    Right ops -> Right ops

-- | Load a directory as a folder TblOps. The view path is canonicalized
-- so folderEnter can join with entry names unambiguously.
loadFolder :: FilePath -> IO (TblOps, FilePath)
loadFolder p = do
  ap <- canonicalizePath p
  ops <- Folder.listFolder ap
  pure (ops, ap)

-- | Open any path: directory → folder view, otherwise → file view. Used
-- both for the initial CLI arg and for in-app navigation. On success the
-- new view is pushed on the stack; on failure a status message is set.
openPath :: AppState -> FilePath -> IO (Maybe AppState)
openPath st p = do
  isDir <- doesDirectoryExist p
  if isDir
    then do
      (ops, absPath) <- loadFolder p
      let disp = T.pack absPath
          vk = VFld disp 1
          v' = case fromTbl ops disp 0 V.empty 0 of
                 Nothing -> Nothing
                 Just v  -> Just v { _vNav = (_vNav v) { _nsVkind = vk } }
      case v' of
        Nothing -> pure (Just st { asMsg = "empty folder: " <> T.pack p })
        Just v  -> Just <$> refreshGrid st { asStack = vsPush v (asStack st) }
    else do
      r <- loadFile p
      case r of
        Left e   -> pure (Just st { asMsg = "open failed: " <> T.pack e })
        Right ops -> case fromTbl ops (T.pack p) 0 V.empty 0 of
          Nothing -> pure (Just st { asMsg = "empty: " <> T.pack p })
          Just v  -> Just <$> refreshGrid st { asStack = vsPush v (asStack st) }

-- | Build an empty initial AppState. The stack head is a placeholder view
-- we replace via openPath before running the brick loop.
initialState :: View -> IO AppState
initialState v = initialStateSized v 200 80

-- | Like 'initialState' but seeds asVisW/asVisH from real terminal bounds
-- so the first frame is laid out at the correct size.
initialStateSized :: View -> Int -> Int -> IO AppState
initialStateSized v tw th = do
  theme <- initTheme
  refreshGrid AppState
    { asStack = ViewStack v []
    , asThemeIdx = tsThemeIdx theme
    , asTestKeys = []
    , asMsg = ""
    , asErr = ""
    , asCmd = ""
    , asGrid = V.empty
    , asVisRow0 = 0
    , asVisCol0 = 0
    , asVisH = max 1 (th - 4)
    , asVisW = max 1 tw
    , asStyles = tsStyles theme
    }

-- | Default keybinding + menu table. Ported from Tc/App/Common.lean's
-- `commands` array. Populates Tv.CmdConfig caches at startup so the
-- Brick handler dispatch (handleEvent → keyLookup → handleCmd) can
-- resolve keys like "j"/"q"/"<ret>" at runtime. Without this the app
-- ignores every key and can't even be quit.
defaultEntries :: [CC.Entry]
defaultEntries =
  [ e RowInc       "r"  "j"         ""                                     False ""
  , e RowDec       "r"  "k"         ""                                     False ""
  , e RowPgdn      "r"  "<pgdn>"    ""                                     False ""
  , e RowPgup      "r"  "<pgup>"    ""                                     False ""
  , e RowPgdn      "r"  "<C-d>"     ""                                     False ""
  , e RowPgup      "r"  "<C-u>"     ""                                     False ""
  , e RowTop       "r"  "<home>"    ""                                     False ""
  , e RowBot       "r"  "<end>"     ""                                     False ""
  , e RowSel       "r"  "T"         "Select/deselect current row"          False ""
  , e RowSearch    "ca" "/"         "Search for value in current column"   True  ""
  , e RowFilter    "a"  "\\"        "Filter rows by PRQL expression"       True  ""
  , e RowSearchNext "rc" "n"        "Jump to next search match"            False ""
  , e RowSearchPrev "rc" "N"        "Jump to previous search match"        False ""
  , e ColInc       "c"  "l"         ""                                     False ""
  , e ColDec       "c"  "h"         ""                                     False ""
  , e ColFirst     "c"  ""          ""                                     False ""
  , e ColLast      "c"  ""          ""                                     False ""
  , e ColGrp       "c"  "!"         "Toggle group on current column"       False ""
  , e ColHide      "c"  "H"         "Hide/unhide current column"           False ""
  , e ColExclude   "c"  "x"         "Delete column(s) from query"          True  ""
  , e ColShiftL    "c"  "<S-left>"  "Shift key column left"                False ""
  , e ColShiftR    "c"  "<S-right>" "Shift key column right"               False ""
  , e SortAsc      "c"  "["         "Sort ascending"                       True  ""
  , e SortDesc     "c"  "]"         "Sort descending"                      True  ""
  , e ColSplit     "ca" ":"         "Split column by delimiter"            False ""
  , e ColDerive    "a"  "="         "Derive new column (name = expr)"      False ""
  , e ColSearch    "a"  "g"         "Jump to column by name"               True  ""
  , e PlotArea     "cg" ""          "Area (g=x numeric, c=y numeric)"      False ""
  , e PlotLine     "cg" ""          "Line (g=x numeric, c=y numeric)"      False ""
  , e PlotScatter  "cg" ""          "Scatter (g=x numeric, c=y numeric)"   False ""
  , e PlotBar      "cg" ""          "Bar (g=x categorical, c=y numeric)"   False ""
  , e PlotBox      "cg" ""          "Boxplot (g=x categorical, c=y numeric)" False ""
  , e PlotStep     "cg" ""          "Step (g=x numeric, c=y numeric)"      False ""
  , e PlotHist     "c"  ""          "Histogram (c=numeric column)"         False ""
  , e PlotDensity  "c"  ""          "Density (c=numeric column)"           False ""
  , e PlotViolin   "cg" ""          "Violin (g=x categorical, c=y numeric)" False ""
  , e TblMenu      ""   " "         "Open command menu"                    False ""
  , e StkSwap      "S"  "S"         "Swap top two views"                   False ""
  , e StkPop       ""   "q"         "Close current view"                   True  ""
  , e StkDup       ""   ""          "Duplicate current view"               False ""
  , e TblQuit      ""   ""          ""                                     False ""
  , e TblXpose     ""   "X"         "Transpose table (rows <-> columns)"   False ""
  , e TblDiff      "S"  "d"         "Diff top two views"                   False ""
  , e InfoTog      ""   "I"         "Toggle info overlay"                  False ""
  , e PrecDec      ""   ""          "Decrease decimal precision"           False ""
  , e PrecInc      ""   ""          "Increase decimal precision"           False ""
  , e PrecZero     ""   ""          "Set precision to 0 decimals"          False ""
  , e PrecMax      ""   ""          "Set precision to max (17)"            False ""
  , e CellUp       ""   "{"         "Scroll cell preview up"               False ""
  , e CellDn       ""   "}"         "Scroll cell preview down"             False ""
  , e Heat0        ""   ""          "Heatmap: off"                         False ""
  , e Heat1        ""   ""          "Heatmap: numeric columns"             False ""
  , e Heat2        ""   ""          "Heatmap: categorical columns"         False ""
  , e Heat3        ""   ""          "Heatmap: all columns"                 False ""
  , e MetaPush     ""   "M"         "Open column metadata view"            True  ""
  , e MetaSetKey   "s"  "<ret>"     "Set selected rows as key columns"     True  "colMeta"
  , e MetaSelNull  ""   "0"         "Select columns with null values"      True  ""
  , e MetaSelSingle ""  "1"         "Select columns with single value"     True  ""
  , e FreqOpen     "cg" "F"         "Open frequency view"                  True  ""
  , e FreqFilter   "r"  "<ret>"     "Filter parent table by current row"   True  "freqV"
  , e FolderPush   "r"  "D"         "Browse folder"                        True  ""
  , e FolderEnter  "r"  "<ret>"     "Open file or enter directory"         True  "fld"
  , e FolderParent ""   "<bs>"      "Go to parent directory"               True  "fld"
  , e FolderDel    "r"  ""          "Move to trash"                        True  ""
  , e FolderDepthDec "" ""          "Decrease folder depth"                True  ""
  , e FolderDepthInc "" ""          "Increase folder depth"                True  ""
  , e TblExport    "a"  "e"         "Export table (csv/parquet/json/ndjson)" False ""
  , e SessSave     "a"  "W"         "Save session"                         False ""
  , e SessLoad     "a"  ""          "Load session"                         False ""
  , e TblJoin      "Sa" "J"         "Join tables"                          False ""
  , e ThemeOpen    ""   ""          "Pick color theme"                     False ""
  , e ThemePreview ""   ""          ""                                     False ""
  ]
  where e c ctx k lbl rs vctx = CC.Entry c ctx k lbl rs vctx

-- | Entry point: parse args, load data, run brick app.
runApp :: IO ()
runApp = do
  CC.initCmds defaultEntries
  args <- getArgs
  path <- case args of
    []    -> getCurrentDirectory
    (p:_) -> pure p
  isDir <- doesDirectoryExist path
  initialView <- if isDir
    then do
      (ops, absPath) <- loadFolder path
      case fromTbl ops (T.pack absPath) 0 V.empty 0 of
        Nothing -> fail ("empty folder: " ++ path)
        Just v  -> pure v { _vNav = (_vNav v) { _nsVkind = VFld (T.pack absPath) 1 } }
    else do
      r <- loadFile path
      case r of
        Left e -> fail e
        Right ops -> case fromTbl ops (T.pack path) 0 V.empty 0 of
          Nothing -> fail ("empty: " ++ path)
          Just v  -> pure v
  let buildVty = VtyUnix.mkVty Vty.defaultConfig
  initVty <- buildVty
  -- Query real terminal bounds before starting brick so the first frame
  -- is sized correctly. EvResize keeps it in sync afterwards.
  (tw, th) <- Vty.displayBounds (Vty.outputIface initVty)
  st0 <- initialStateSized initialView tw th
  _ <- BM.customMain initVty buildVty Nothing app st0
  pure ()
