-- | App: brick application wiring — appDraw, appHandleEvent, appAttrMap.
-- Handler dispatch uses a Map Cmd Handler combinator pattern.
module Tv.App where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.Vector as V
import Control.Monad (forM, when)
import qualified Brick
import qualified Brick.Main as BM
import qualified Brick.AttrMap as A
import qualified Graphics.Vty as Vty
import qualified Graphics.Vty.Platform.Unix as VtyUnix
import System.Environment (getArgs)
import System.Directory
  ( doesDirectoryExist, getCurrentDirectory, canonicalizePath
  , getHomeDirectory, createDirectoryIfMissing, doesPathExist, renamePath
  , getTemporaryDirectory )
import System.FilePath (takeFileName, takeExtension, takeDirectory, (</>))
import System.IO (hPutStrLn, stderr, hFlush, stdout)
import System.Process (readProcessWithExitCode)
import System.Exit (ExitCode(..))
import Control.Exception (try, SomeException, displayException)
import Control.Monad.IO.Class (liftIO)
import Data.Char (toLower)
import Control.Applicative ((<|>))
import Data.List (find, isSuffixOf, sort)
import qualified Data.Text.IO as TIO
import Tv.Types
import Tv.View
import qualified Tv.Nav as Nav
import Tv.Render
import qualified Tv.Render as R
import Optics.Core ((^.), (%), (&), (.~), (%~))
import qualified Tv.Key as Key
import Tv.Key (evToKey)
import Tv.CmdConfig (keyLookup, CmdInfo(..))
import qualified Tv.CmdConfig as CC
import Tv.Eff (Eff, AppEff, runEff, runAppEff, tryE, tryEitherE)
import Tv.Theme (initTheme, toAttrMap, ThemeState(..), themeName, themes, applyTheme, tsStyles, tsThemeIdx)
import qualified Tv.Theme
import qualified Tv.Data.DuckDB as DB
import qualified Tv.Data.Prql as Prql
import qualified Tv.Data.Text as DataText
import qualified Tv.FileFormat as FF
import qualified Tv.Folder as Folder
import qualified Tv.Util as Util
import qualified Tv.Freq as Freq
import qualified Tv.Meta as Meta
import qualified Tv.Transpose as Transpose
import qualified Tv.Derive as Derive
import qualified Tv.Split as Split
import qualified Tv.Diff as Diff
import qualified Tv.Join as Join
import qualified Tv.Session as Session
import qualified Tv.Export as Export
import qualified Tv.Fzf as Fzf
import qualified Tv.Plot as Plot
import qualified Tv.UI.Info as Info
import qualified Tv.Replay as Replay
import qualified Tv.StatusAgg as StatusAgg
import qualified Tv.Sparkline as Sparkline
import Data.IORef
import Data.Word (Word8)
import qualified Tv.AppF as AppF

-- ============================================================================
-- Handler combinators
-- ============================================================================

-- | Handler: (state, arg) → Eff (Nothing = halt). Every handler runs in
-- the 'AppEff' stack so it can compose analytics calls directly and catch
-- exceptions via 'tryE' without an @IO@ trampoline.
type Handler = AppState -> Text -> Eff AppEff (Maybe AppState)

-- | Apply a NavState transform to the head view.
withNav :: (NavState -> Maybe NavState) -> Handler
withNav f st _ = case f (st ^. headNav) of
  Nothing  -> pure (Just st)
  Just ns' -> Just <$> refreshGrid (st & headNav .~ ns')

-- | Quit handler — returning Nothing signals halt.
quitH :: Handler
quitH _ _ = pure Nothing

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
    -- plot (all 9 kinds: area/line/scatter/bar/box/step/hist/density/violin)
  , (PlotArea,    plotH PKArea)
  , (PlotLine,    plotH PKLine)
  , (PlotScatter, plotH PKScatter)
  , (PlotBar,     plotH PKBar)
  , (PlotBox,     plotH PKBox)
  , (PlotStep,    plotH PKStep)
  , (PlotHist,    plotH PKHist)
  , (PlotDensity, plotH PKDensity)
  , (PlotViolin,  plotH PKViolin)
    -- TODO RowSearch, RowFilter, ColSearch: handlers pending search-bar UI
    -- freq
  , (FreqOpen,   freqOpenH)
  , (FreqFilter, freqFilterH)
  -- NB: TblMenu is intentionally NOT here. It must run under Brick.suspendAndResume
  -- (see handleEvent) so fzf can grab /dev/tty while the brick UI is paused.
  , (RowSearch, rowSearchH)
  , (RowFilter, rowFilterH)
  , (ColSearch, colSearchH)
  , (RowSearchNext, rowSearchNextH)
  , (RowSearchPrev, rowSearchPrevH)
  , (MetaSetKey,   metaSetKeyH)
  , (MetaSelNull,  metaSelNullH)
  , (MetaSelSingle, metaSelSingleH)
  ]
  where pageRows = 20  -- TODO: derive from terminal size at draw time

-- | Sort the current table by the current column. Builds a fresh TblOps
-- via tblSortBy and replaces the head view in place.
sortH :: Bool -> Handler
sortH asc st _ = do
  let ns  = st ^. headNav
      ops = ns ^. nsTbl
      ci  = curColIdx ns
  ops' <- liftIO ((ops ^. tblSortBy) (V.singleton ci) asc)
  Just <$> refreshGrid (st & headTbl .~ ops')

-- | Drop column(s) from the query: current column + any hidden columns.
-- Lean View.update ColExclude produces Effect.exclude (hidden ++ [current]).
excludeH :: Handler
excludeH st _ = do
  let ns   = st ^. headNav
      ops  = ns ^. nsTbl
      ci   = curColIdx ns
      names = ops ^. tblColNames
      hiddenIdxs = V.mapMaybe (`V.elemIndex` names) (ns ^. nsHidden)
      allIdxs = if V.elem ci hiddenIdxs then hiddenIdxs
                else V.snoc hiddenIdxs ci
  ops' <- liftIO ((ops ^. tblHideCols) allIdxs)
  case fromTbl ops' (curView st ^. vPath) 0 V.empty 0 of
    Nothing -> pure (Just (st & asMsg .~ "exclude: empty result"))
    Just v  -> Just <$> refreshGrid (st & asStack %~ vsPush v)

-- | Set the heatmap mode on the head view's NavState.
heatH :: Word8 -> Handler
heatH m st _ = Just <$> refreshGrid (st & headNav % nsHeatMode .~ m)

-- | Adjust decimal precision on the head view's NavState, clamped 0..17.
precH :: (Int -> Int) -> Handler
precH f st _ =
  let p' = max 0 (min 17 (f (st ^. headNav % nsPrecAdj)))
  in Just <$> refreshGrid (st & headNav % nsPrecAdj .~ p')

-- | Open the space-bar command menu via fzf. Called from handleEvent under
-- Brick.suspendAndResume so fzf can grab /dev/tty.  The picker returns a
-- handler name string like "row.inc"; we convert it to a Cmd, run its
-- handler, and return the updated AppState. Returns 'IO' because
-- 'BM.suspendAndResume' takes an IO action — internally we cross back
-- into 'Eff' via 'handleCmd'.
runCmdMenu :: AppState -> IO AppState
runCmdMenu st = do
  let vctx = vkCtxStr (st ^. headNav % nsVkind)
  mh <- runEff (Fzf.cmdMode vctx)
  case mh of
    Nothing -> pure st
    Just h -> case cmdFromStr h of
      Nothing -> pure (st & asMsg .~ "unknown cmd: " <> h)
      Just c -> do
        r <- handleCmd c "" st
        pure (maybe st id r)

-- | Build a fresh View from a newly-produced TblOps and push onto stack.
pushOps :: AppState -> Text -> ViewKind -> TblOps -> Eff AppEff (Maybe AppState)
pushOps st path vk ops = case fromTbl ops path 0 V.empty 0 of
  Nothing -> pure (Just (st & asMsg .~ "empty result"))
  Just v  -> Just <$> refreshGrid (st & asStack %~ vsPush (v & vNav % nsVkind .~ vk))

-- | Current TblOps of the top view.
curOps :: AppState -> TblOps
curOps st = st ^. headTbl

-- | Current top view (for path/disp).
curView :: AppState -> View
curView st = st ^. headView

-- | Swap top two views in the stack.
stkSwapH :: Handler
stkSwapH st _ = Just <$> refreshGrid (st & asStack %~ vsSwap)

-- | Duplicate the top view on the stack.
stkDupH :: Handler
stkDupH st _ = Just <$> refreshGrid (st & asStack %~ vsDup)

-- | Push a column-metadata view computed from the current table.
metaPushH :: Handler
metaPushH st _ = do
  ops' <- Meta.mkMetaOps (curOps st)
  pushOps st (curView st ^. vPath <> " [meta]") VColMeta ops'

-- | Open a frequency view on (nav.grp ++ current col). The grouping
-- columns are resolved to indices against the parent table's column
-- names and passed to Freq.mkFreqOps. The new view is pushed with a
-- VFreq vkind carrying the group column names and total distinct-group
-- count (nRows of the freq table).
freqOpenH :: Handler
freqOpenH st _ = do
  let ns      = ((curView st) ^. vNav)
      ops     = (ns ^. nsTbl)
      names   = (ops ^. tblColNames)
      grp     = (ns ^. nsGrp)
      curName = curColName ns
      -- Group cols = nav.grp + current col (if not already in grp).
      -- Matches Tc.Freq.update:
      --   if n.grp.contains curName then n.grp else n.grp.push curName
      colNames =
        if V.elem curName grp then grp else V.snoc grp curName
      colIdxs = V.mapMaybe (`V.elemIndex` names) colNames
  if V.null colIdxs
    then pure (Just (st & asMsg .~ "freq: no columns selected"))
    else do
      ops' <- Freq.mkFreqOps ops colIdxs
      let total = ops' ^. tblNRows
          vk    = VFreq colNames total
          path  = curView st ^. vPath <> " [freq " <> T.intercalate "," (V.toList colNames) <> "]"
      case fromTbl ops' path 0 colNames 0 of
        Nothing -> pure (Just (st & asMsg .~ "freq: empty result"))
        Just v  -> Just <$> refreshGrid (st & asStack %~ vsPush (v & vNav % nsVkind .~ vk))

-- | Filter the parent table by the current freq-view row. Reads the
-- group-key values of the focused row, builds a WHERE clause via
-- Freq.filterExpr, and calls the parent table's _tblFilter. Mirrors
-- Tc.App.Common.runViewEffect's .freqFilter arm: the filter is applied
-- to the PARENT table (stack tail), not the freq table itself. The
-- filtered result is pushed on top of the parent view.
freqFilterH :: Handler
freqFilterH st _ = case st ^. headNav % nsVkind of
  VFreq cols _ -> case st ^. asStack % vsTl of
    [] -> pure (Just (st & asMsg .~ "freq.filter: no parent view"))
    (parent:_) -> do
      let freqOps   = curOps st
          parentOps = parent ^. vNav % nsTbl
          row       = st ^. headNav % nsRow % naCur
      expr <- Freq.filterExpr freqOps cols row
      mOps <- liftIO ((parentOps ^. tblFilter) expr)
      case mOps of
        Nothing -> pure (Just (st & asMsg .~ "freq.filter: filter failed"))
        Just ops' ->
          case fromTbl ops' (parent ^. vPath) 0 (parent ^. vNav % nsGrp) 0 of
            Nothing -> pure (Just (st & asMsg .~ "freq.filter: empty result"))
            Just v  -> Just <$> refreshGrid (st & asStack %~ vsPush v)
  _ -> pure (Just (st & asMsg .~ "freq.filter: not a freq view"))

-- | Push a transposed-table view.
xposeH :: Handler
xposeH st _ = do
  ops' <- Transpose.mkTransposedOps (curOps st)
  pushOps st (((curView st) ^. vPath) <> " [T]") VTbl ops'

-- | Derive a new column from @asCmd == "name = expr"@. No-op + message on
-- parse failure; fail-soft on DuckDB errors (addDerived returns original).
deriveH :: Handler
deriveH st arg = case Derive.parseDerive arg of
  Nothing -> pure (Just (st & asMsg .~ "derive: usage 'name = expr'"))
  Just (n, e) -> do
    ops' <- Derive.addDerived (curOps st) n e
    pushOps st (((curView st) ^. vPath) <> " =" <> n) VTbl ops'

-- | Split the current column by regex taken from @asCmd@.
splitH :: Handler
splitH st arg
  | T.null arg = splitH st "-"  -- test mode: fzf picks first suggestion = "-"
  | otherwise = do
      let ns = ((curView st) ^. vNav)
          ci = curColIdx ns
          colName = curColName ns
          origNc = V.length (((curOps st) ^. tblColNames))
      ops' <- Split.splitColumn (curOps st) ci arg
      -- Position cursor at the first split column (right after original columns).
      let startCol = origNc
          path = ((curView st) ^. vPath) <> " :" <> colName
      case fromTbl ops' path startCol V.empty 0 of
        Nothing -> pure (Just (st & asMsg .~ "split: empty result"))
        Just v  -> Just <$> refreshGrid (st & asStack %~ vsPush (v & vNav % nsVkind .~ VTbl))

-- | Diff top two views on the stack. Requires at least two views.
diffH :: Handler
diffH st _ = case st ^. asStack % vsTl of
  [] -> pure (Just (st & asMsg .~ "diff: need 2 views on stack"))
  (v2:_) -> do
    let l = st ^. headTbl
        r = v2 ^. vNav % nsTbl
    ops' <- Diff.diffTables l r
    pushOps st "[diff]" VTbl ops'

-- | Join top two views via fzf-selected operation.  Lean Join.run:
-- if both views share non-empty grp columns → all join types available;
-- otherwise only union/diff.  In test mode fzfIdx picks index 0.
joinH :: Handler
joinH st _ = case st ^. asStack % vsTl of
  [] -> pure (Just (st & asMsg .~ "join: need 2 views on stack"))
  (v2:_) -> do
    let curNs = st ^. headNav
        parNs = v2 ^. vNav
        l     = parNs ^. nsTbl    -- parent = left (Lean convention)
        r     = curNs ^. nsTbl    -- current = right
        leftGrp = (parNs ^. nsGrp)
        joinOk = not (V.null leftGrp) && leftGrp == (curNs ^. nsGrp)
        allOps = [Join.JInner, Join.JLeft, Join.JRight, Join.JUnion, Join.JDiff]
        availOps = if joinOk then allOps else [Join.JUnion, Join.JDiff]
        labels  = map joinLabel availOps
    mIdx <- Fzf.fzfIdx ["--prompt=join> "] labels
    case mIdx of
      Nothing -> pure (Just st)
      Just idx -> do
        let op = availOps !! min idx (length availOps - 1)
        ops' <- Join.joinWith op l r leftGrp
        pushOps st "[join]" VTbl ops'
  where
    joinLabel Join.JInner = "join inner"
    joinLabel Join.JLeft  = "join left"
    joinLabel Join.JRight = "join right"
    joinLabel Join.JUnion = "append (union)"
    joinLabel Join.JDiff  = "remove (diff)"

-- | Export the current table. Arg can be a format name (csv/parquet/json/ndjson)
-- or a full path. Format name → auto-generates path under ~/.cache/tv/.
-- Matches Lean export: arg is the fzf-selected format, path is generated.
exportH :: Handler
exportH st arg
  | T.null arg = exportH st "csv"  -- test mode: fzf auto-selects first item = csv
  | otherwise = do
      let mFmt = Export.exportFmtFromText (T.toLower arg)
      case mFmt of
        Just fmt -> do
          dir <- Util.logDir
          let rawBase = takeWhile (/= '.') (takeFileName (T.unpack (((curView st) ^. vPath))))
              base = if null rawBase then "export" else rawBase
              ext = T.unpack (Export.exportFmtExt fmt)
              path = dir </> ("tv_export_" ++ base ++ "." ++ ext)
          r <- tryEitherE (Export.exportTable (curOps st) fmt path)
          let msg = case r of
                Left e  -> "export failed: " <> T.pack (displayException e)
                Right _ -> "exported to " <> T.pack path
          pure (Just (st & asMsg .~ msg))
        Nothing -> do
          let path = T.unpack arg
              ext = T.toLower $ T.pack $ drop 1 $ takeExtension path
          case Export.exportFmtFromText ext of
            Nothing -> pure (Just (st & asMsg .~ "export: unknown fmt " <> arg))
            Just fmt -> do
              r <- tryEitherE (Export.exportTable (curOps st) fmt path)
              let msg = case r of
                    Left e  -> "export failed: " <> T.pack (displayException e)
                    Right _ -> "exported to " <> T.pack path
              pure (Just (st & asMsg .~ msg))

-- | Save the current stack as a named session (name from @asCmd@).
sessSaveH :: Handler
sessSaveH st arg = do
  mp <- Session.saveSession arg ((st ^. asStack))
  let msg = case mp of
        Just p  -> "saved session " <> T.pack p
        Nothing -> "save failed"
  pure (Just (st & asMsg .~ msg))

-- | Load a session by name. Haskell cannot rehydrate TblOps without
-- re-running the source query, so for now we just report the result
-- and leave the live stack alone.
-- TODO: rehydrate via openPath once SourceConfig replay lands.
sessLoadH :: Handler
sessLoadH st arg = do
  ms <- Session.loadSession arg
  let msg = case ms of
        Just _  -> "loaded session (rehydrate TODO)"
        Nothing -> "load failed"
  pure (Just (st & asMsg .~ msg))

-- | Toggle info overlay (asInfoVis). When the flag is on, 'drawApp'
-- draws a one-line panel just above the status bar showing the current
-- column's name, type, and (1-indexed) position. Stateless toggle —
-- everything lives in 'AppState'.
infoTogH :: Handler
infoTogH st _ = pure (Just (st & asInfoVis .~ not ((st ^. asInfoVis))))

-- | Push a fresh folder view at the current directory. Unlike
-- folderEnterH (which follows the row under the cursor into either
-- a file or a subdirectory), folderPushH always opens a *new* folder
-- view at the same path — useful for forking the browser into another
-- stack slot. Matches Tc.Folder.dispatch FolderPush.
folderPushH :: Handler
folderPushH st _ = do
  case st ^. headNav % nsVkind of
    VFld base depth -> do
      ops <- Folder.listFolderDepth (T.unpack base) depth
      case fromTbl ops base 0 V.empty 0 of
        Nothing -> pure (Just (st & asMsg .~ "folder.push: empty"))
        Just v  -> Just <$> refreshGrid (st & asStack %~ vsPush (v & vNav % nsVkind .~ VFld base depth))
    _ -> do
      -- From a file view: push a folder view of the file's parent directory.
      -- Lean Folder.dispatch FolderPush opens curDir (parent of file path).
      let path = curView st ^. vPath
          dir  = T.pack (takeDirectory (T.unpack path))
      absDir <- liftIO (canonicalizePath (T.unpack dir))
      openPath st absDir

-- | Move the current row's file to @~/.local/share/Trash/files/@
-- (XDG trash).  Collisions get @.1@, @.2@, … suffixes. Does not write
-- the accompanying .trashinfo metadata (matches Tc's best-effort).
folderDelH :: Handler
folderDelH st _ = do
  let ns = st ^. headNav
  case ns ^. nsVkind of
    VFld base _ -> do
      let r = ns ^. nsRow % naCur
      name <- liftIO ((ns ^. nsTbl % tblCellStr) r 0)
      if T.null name || name == ".." then pure (Just (st & asMsg .~ "del: nothing to trash"))
      else do
        home <- liftIO getHomeDirectory
        let trashDir = home </> ".local/share/Trash/files"
            srcPath = T.unpack base </> T.unpack name
        liftIO (createDirectoryIfMissing True trashDir)
        dest <- liftIO (uniqueTrashPath trashDir (T.unpack name) 0)
        r1 <- tryEitherE (liftIO (renamePath srcPath dest))
        case r1 of
          Left e ->
            pure (Just (st & asMsg .~ "del failed: " <> T.pack (displayException e)))
          Right _ -> do
            ops' <- Folder.listFolderDepth (T.unpack base) 1
            s' <- refreshGrid (st & headTbl .~ ops')
            pure (Just (s' & asMsg .~ "trashed " <> name))
    _ -> pure (Just (st & asMsg .~ "del: not in a folder view"))
  where
    uniqueTrashPath dir base n = do
      let cand = if n == 0 then dir </> base else dir </> (base <> "." <> show n)
      ex <- doesPathExist cand
      if ex then uniqueTrashPath dir base (n + 1) else pure cand

-- | Bump the folder recursion depth by @d@ (clamped 1..5), rebuild the
-- folder TblOps, and update the view kind. Used by Inc/Dec handlers.
folderDepthAdjH :: Int -> Handler
folderDepthAdjH d st _ = case st ^. headNav % nsVkind of
  VFld base depth -> do
    let d' = max 1 (min 5 (depth + d))
    ops' <- Folder.listFolderDepth (T.unpack base) d'
    s' <- refreshGrid (st & headTbl .~ ops' & headNav % nsVkind .~ VFld base d')
    pure (Just (s' & asMsg .~ "depth=" <> T.pack (show d')))
  _ -> pure (Just (st & asMsg .~ "depth: not in a folder view"))

folderDepthIncH, folderDepthDecH :: Handler
folderDepthIncH = folderDepthAdjH 1
folderDepthDecH = folderDepthAdjH (-1)

-- | Search the current column for the buffer in @asCmd@ and jump the row
-- cursor to the first row whose value contains it.  Uses tblFindRow
-- which the DuckDB ops implement as a SELECT rowid WHERE col LIKE …;
-- fails-soft with a status message if not found.
rowSearchH :: Handler
rowSearchH st arg
  | T.null arg = pure (Just (st & asMsg .~ "search: empty query"))
  | otherwise = do
      let ns  = st ^. headNav
          ops = ns ^. nsTbl
          ci  = curColIdx ns
          startRow = ns ^. nsRow % naCur + 1  -- skip current row, matching Lean
      mr <- liftIO ((ops ^. tblFindRow) ci arg startRow True)
      case mr of
        Just r -> Just <$> refreshGrid (st & headNav % nsRow % naCur .~ r
                                           & headNav % nsSearch     .~ arg
                                           & asMsg                  .~ "")
        Nothing -> pure (Just (st & asMsg .~ "not found: " <> arg))

-- | n / N: jump to the next / previous search match. Reuses the
-- previously-stored query in @nsSearch@ and calls tblFindRow with
-- an advanced starting row.
rowSearchStepH :: Int -> Handler
rowSearchStepH step st _ = do
  let ns = st ^. headNav
      q  = ns ^. nsSearch
  if T.null q then pure (Just (st & asMsg .~ "no prior search"))
  else do
    let ops = ns ^. nsTbl
        ci  = curColIdx ns
        startRow = ns ^. nsRow % naCur + step
    mr <- liftIO ((ops ^. tblFindRow) ci q startRow (step > 0))
    case mr of
      Just r -> do
        s' <- refreshGrid (st & headNav % nsRow % naCur .~ r)
        pure (Just (s' & asMsg .~ ""))
      Nothing -> pure (Just (st & asMsg .~ "no more matches: " <> q))

rowSearchNextH, rowSearchPrevH :: Handler
rowSearchNextH = rowSearchStepH 1
rowSearchPrevH = rowSearchStepH (-1)

-- | Filter: when arg is empty, pick first distinct value via fzf (Lean
-- Filter.lean:rowFilter). When arg is non-empty, apply it as a raw PRQL
-- filter expression (Lean filterWith path).
rowFilterH :: Handler
rowFilterH st arg
  | T.null arg = do
      let ops = curOps st
          ns  = st ^. headNav
          colIdx  = curColIdx ns
          colName = curColName ns
          isNum   = isNumeric ((ops ^. tblColType) colIdx)
      vals <- liftIO ((ops ^. tblDistinct) colIdx)
      let sorted = V.fromList (sort (V.toList vals))
          input  = T.intercalate "\n" (V.toList sorted)
      mResult <- Fzf.fzf ["--print-query", "--prompt=filter > "] input
      case mResult of
        Nothing -> pure (Just st)
        Just result -> do
          let expr = (ops ^. tblBuildFilter) colName sorted result isNum
          applyFilter st ops colName expr
  | otherwise = do
      let ops = curOps st
          colName = curColName (st ^. headNav)
      applyFilter st ops colName arg
  where
    applyFilter st' ops' cn expr
      | T.null expr = pure (Just st')
      | otherwise = do
          mOps <- liftIO ((ops' ^. tblFilter) expr)
          case mOps of
            Nothing -> pure (Just (st' & asMsg .~ "filter failed: " <> expr))
            Just ops'' ->
              case fromTbl ops'' (curView st' ^. vPath <> " \\" <> cn) 0 V.empty 0 of
                Nothing -> pure (Just (st' & asMsg .~ "filter: empty result"))
                Just v  -> Just <$> refreshGrid (st' & asStack %~ vsPush v)

-- | Goto column by name prefix/substring. First case-insensitive prefix
-- match wins; falls back to substring match so users can type partial
-- names. Moves the nsCol cursor, no new view.
colSearchH :: Handler
colSearchH st arg
  | T.null arg = pure (Just (st & asMsg .~ "goto: empty name"))
  | otherwise = do
      let ns = st ^. headNav
          names = ns ^. nsTbl % tblColNames
          disp  = ns ^. nsDispIdxs
          q = T.toLower arg
          at i = T.toLower (names V.! (disp V.! i))
          idxRange = [0 .. V.length disp - 1]
      case find ((q `T.isPrefixOf`) . at) idxRange <|> find ((q `T.isInfixOf`) . at) idxRange of
        Just i  -> moveTo i
        Nothing -> pure (Just (st & asMsg .~ "no column matches: " <> arg))
  where
    moveTo i = Just <$> refreshGrid (st & headNav % nsCol % naCur .~ i & asMsg .~ "")

-- | MetaSetKey: in a colMeta view, take the row selections (which are
-- column indices in the parent) and set them as the parent's group
-- columns, then pop back to the parent.
metaSetKeyH :: Handler
metaSetKeyH st _ = case st ^. headNav % nsVkind of
  VColMeta -> case st ^. asStack % vsTl of
    [] -> pure (Just (st & asMsg .~ "meta.key: no parent view"))
    (parent:_) -> do
      let metaNs = st ^. headNav
          sels = metaNs ^. nsRow % naSels
          parentNames = parent ^. vNav % nsTbl % tblColNames
          keyNames = V.mapMaybe (\i -> parentNames V.!? i) sels
          parent' = parent & vNav % nsGrp .~ keyNames
                           & vNav % nsDispIdxs .~ dispOrder keyNames parentNames
      Just <$> refreshGrid (st & asStack % vsHd .~ parent'
                              & asStack % vsTl %~ drop 1)
  _ -> pure (Just (st & asMsg .~ "meta.key: not a meta view"))

-- | Meta "select rows where null_pct == 100". Lean Meta.selNull filters
-- on null_pct == 100 (fully null columns).
metaSelNullH :: Handler
metaSelNullH = metaSelByColValue "null_pct" (== 100) "no null columns"

-- | Meta "select rows where dist == 1". Lean Meta.selSingle filters on
-- dist == 1 (single distinct value).
metaSelSingleH :: Handler
metaSelSingleH = metaSelByColValue "dist" (== 1) "no single-value columns"

metaSelByColValue :: Text -> (Int -> Bool) -> Text -> Handler
metaSelByColValue colName predFn emptyMsg st _ = case st ^. headNav % nsVkind of
  VColMeta -> do
    let ns = st ^. headNav
        ops = ns ^. nsTbl
        names = ops ^. tblColNames
    case V.elemIndex colName names of
      Nothing -> pure (Just (st & asMsg .~ "meta.sel: column '" <> colName <> "' missing"))
      Just colIdx -> do
        let nr = ops ^. tblNRows
        matches <- liftIO (V.filterM (\r -> do
            cell <- (ops ^. tblCellStr) r colIdx
            let v = case reads (T.unpack cell) of [(n,"")] -> n; _ -> (0 :: Int)
            pure (predFn v)) (V.enumFromN 0 nr))
        if V.null matches then pure (Just (st & asMsg .~ emptyMsg))
        else do
          s' <- refreshGrid (st & headNav % nsRow % naSels .~ matches)
          pure (Just (s' & asMsg .~ tshow (V.length matches) <> " columns selected"))
  _ -> pure (Just (st & asMsg .~ "meta.sel: not a meta view"))
  where tshow = T.pack . show

-- | ThemeOpen: fzf picker over available themes. Must run under
-- BM.suspendAndResume in handleEvent — but our fzf helper works when
-- called via runCmdMenu-style wrapper too.  We keep this as a pure IO
-- helper; wiring to handleEvent happens via runThemePicker below.
runThemePicker :: AppState -> IO AppState
runThemePicker st = do
  let names = V.toList (V.imap (\i _ -> T.pack (show i) <> ": " <> Tv.Theme.themeName i)
                               (Tv.Theme.themes :: Vector (Text, Text)))
  mSel <- runEff (Fzf.fzf ["--prompt=theme "] (T.intercalate "\n" names))
  case mSel of
    Nothing -> pure st
    Just sel -> case reads (T.unpack (T.takeWhile (/= ':') sel)) of
      [(i, _)] -> do
        theme' <- runEff (Tv.Theme.applyTheme (Tv.Theme.ThemeState ((st ^. asStyles)) ((st ^. asThemeIdx))) i)
        pure (st & asStyles .~ Tv.Theme.tsStyles theme' & asThemeIdx .~ Tv.Theme.tsThemeIdx theme')
      _ -> pure st

-- | Run a plot command: resolve cursor + group columns from the current
-- 'NavState', hand off to 'Plot.runPlot', and surface the resulting PNG
-- path (or a generic failure message) via @asMsg@. Unlike the Lean port
-- we don't enter an interactive downsample loop — one plot, one PNG.
plotH :: PlotKind -> Handler
plotH kind st _ = do
  let ns    = st ^. headNav
      tbl   = ns ^. nsTbl
      curC  = curColIdx ns
      names = tbl ^. tblColNames
      grpIdx = V.mapMaybe (`V.elemIndex` names) (ns ^. nsGrp)
  r <- Plot.runPlot kind tbl curC grpIdx
  let msg = case r of
        Just p  -> "plot: saved to " <> T.pack p
        Nothing -> "plot: failed (check column types / R install)"
  pure (Just (st & asMsg .~ msg))

-- | Pop the top view; quit if it was the last.
stkPopH :: Handler
stkPopH st _ = case vsPop (st ^. asStack) of
  Nothing  -> pure Nothing
  Just vs' -> Just <$> refreshGrid (st & asStack .~ vs')

-- | Folder enter: resolve the current row's path cell, load the target as
-- a new view and push it. Directories → listFolder, files → DuckDB read_*.
-- Database folder views (.duckdb/.sqlite) → query the named table.
folderEnterH :: Handler
folderEnterH st _ = do
  let ns = st ^. headNav
  case ns ^. nsVkind of
    VFld base _ -> do
      let r = ns ^. nsRow % naCur
      name <- liftIO ((ns ^. nsTbl % tblCellStr) r 0)
      if T.null name then pure (Just st)
      else if isDbFile (T.unpack base) && name /= ".."
        then enterDbTable st (T.unpack base) name
        else openPath st (T.unpack base </> T.unpack name)
    _ -> pure (Just st)
  where
    isDbFile p = let ext = map toLower (takeExtension p)
                 in ext `elem` [".duckdb", ".db", ".sqlite", ".sqlite3"]

-- | Enter a table within an attached database file.
enterDbTable :: AppState -> FilePath -> Text -> Eff AppEff (Maybe AppState)
enterDbTable st dbPath tblName = do
  r <- tryEitherE (liftIO go)
  case r of
    Right (Just v) -> Just <$> refreshGrid (st & asStack %~ vsPush v)
    Right Nothing -> pure (Just (st & asMsg .~ "empty table: " <> tblName))
    Left e -> pure (Just (st & asMsg .~ "open table failed: " <> T.pack (displayException e)))
  where
    go = do
      conn <- DB.connect ":memory:"
      let ext = map toLower (takeExtension dbPath)
          escPath = T.unpack (T.replace "'" "''" (T.pack dbPath))
          isSqlite = ext `elem` [".sqlite", ".sqlite3"]
      when isSqlite $ do
        _ <- try (DB.query conn "INSTALL sqlite") :: IO (Either SomeException DB.Result)
        _ <- try (DB.query conn "LOAD sqlite") :: IO (Either SomeException DB.Result)
        pure ()
      _ <- try (DB.query conn "DETACH DATABASE IF EXISTS extdb") :: IO (Either SomeException DB.Result)
      let typClause = if isSqlite then "TYPE SQLITE, " else ""
      _ <- DB.query conn (T.pack ("ATTACH '" ++ escPath ++ "' AS extdb (" ++ typClause ++ "READ_ONLY)"))
      -- Create temp table from attached table so PRQL sort/filter chain works
      let escTbl = T.replace "\"" "\"\"" tblName
          tmpName = "tc_tbl_" <> T.filter (/= '"') tblName
      _ <- DB.query conn ("CREATE TEMP TABLE \"" <> tmpName <> "\" AS SELECT * FROM extdb.\"" <> escTbl <> "\"")
      let q = Prql.Query { Prql.base = "from " <> tmpName, Prql.ops = V.empty }
      total <- DB.queryCount conn q
      mOps <- DB.requery conn q (max total 1)
      case mOps of
        Nothing -> pure Nothing
        Just ops ->
          -- Set grp to primary key if available (name column for table listing)
          let path = T.pack dbPath <> " " <> tblName
              grp = V.singleton "name"  -- Lean sets grp=1 for DuckDB tables
              grp' = V.filter (`V.elem` (ops ^. tblColNames)) grp
          in pure (fromTbl ops path 0 grp' 0)

-- | Parent: replace current folder view with one rooted at parent dir.
-- Replaces in-place (doesn't push) so backspace chain works correctly.
folderParentH :: Handler
folderParentH st _ = case st ^. headNav % nsVkind of
  VFld base _ -> do
    parentPath <- liftIO (canonicalizePath (T.unpack base </> ".."))
    (ops, absP) <- liftIO (loadFolder parentPath)
    let disp = T.pack absP
        vk = VFld disp 1
    case fromTbl ops disp 0 V.empty 0 of
      Nothing -> pure (Just (st & asMsg .~ "parent: empty"))
      Just v  -> Just <$> refreshGrid (st & asStack % vsHd .~ (v & vNav % nsVkind .~ vk))
  _ -> pure (Just st)

-- | Rebuild asGrid for the visible window. Walks the TblOps tblCellStr
-- IO function for each cell. Called after any handler that could change
-- the viewport or the underlying table — the single refresh path keeps
-- drawApp pure and lets it do zero IO.
refreshGrid :: AppState -> Eff AppEff AppState
refreshGrid st = do
  let ns   = st ^. headNav
      tbl  = ns ^. nsTbl
      disp = ns ^. nsDispIdxs
      nr   = tbl ^. tblNRows
      nc   = V.length disp
      curR = ns ^. nsRow % naCur
      curC = ns ^. nsCol % naCur
      visH = max 1 (st ^. asVisH)
      visW = max 1 (st ^. asVisW)
      adjOff cur off page = clamp 0 (max 1 (cur + 1)) (max (cur + 1 - page) (min off cur))
      r0 = adjOff curR (st ^. asVisRow0) visH
      c0 = adjOff curC (st ^. asVisCol0) visW
      h = min visH (max 0 (nr - r0))
      w = min visW (max 0 (nc - c0))
  grid <- liftIO $ V.generateM h $ \ri ->
            V.generateM w $ \ci ->
              (tbl ^. tblCellStr) (r0 + ri) (disp V.! (c0 + ci))
  pure (st & asGrid .~ grid & asVisRow0 .~ r0 & asVisCol0 .~ c0)

-- | Dispatch: (state, cmdinfo, arg) → Eff Action. Returns Nothing to quit,
-- Just to continue. Looks up 'handlerMap', falls back to a no-op.
dispatch :: AppState -> CmdInfo -> Text -> Eff AppEff (Maybe AppState)
dispatch st ci arg = case Map.lookup (ciCmd ci) handlerMap of
  Nothing -> pure (Just st)
  Just h  -> h st arg

-- | IO-facing wrapper around 'dispatch'. Runs the handler under
-- 'runAppEff' and collapses exceptions to an @asErr@ message. This is
-- the boundary where Brick and tests cross into the effect world —
-- everything downstream stays in 'Eff AppEff'.
runDispatchIO :: AppState -> Eff AppEff (Maybe AppState) -> IO (Maybe AppState)
runDispatchIO st act = do
  (r, _) <- runAppEff st act
  case r of
    Right m -> pure m
    Left e  -> pure (Just (st & asErr .~ T.pack (displayException e)))

-- | Convenience: dispatch by Cmd + arg. IO-facing for tests and
-- 'suspendAndResume' callbacks.
handleCmd :: Cmd -> Text -> AppState -> IO (Maybe AppState)
handleCmd cmd arg st = runDispatchIO st $ do
  ci <- CC.cmdLookup cmd
  dispatch st ci arg

-- | Dispatch a single key through 'keyLookup' + 'dispatch'. Non-arg
-- commands get empty arg; arg commands also get empty arg (use
-- 'loopProg' + 'testInterp' for full arg-passing in -c test mode).
-- Kept for unit tests that drive one key.
handleKey :: Text -> AppState -> IO (Maybe AppState)
handleKey key st = runDispatchIO st $ do
  let ctx = vkCtxStr (st ^. headNav % nsVkind)
  mci <- keyLookup key ctx
  case mci of
    Nothing -> pure (Just st)
    Just ci -> dispatch st ci ""

-- ============================================================================
-- Brick app definition
-- ============================================================================

app :: Brick.App AppState () Name
app = Brick.App
  { Brick.appDraw = drawApp
  , Brick.appChooseCursor = Brick.neverShowCursor
  , Brick.appHandleEvent = handleEvent
  , Brick.appStartEvent = pure ()
  , Brick.appAttrMap = \st -> appAttrMap ((st ^. asStyles))
  }

handleEvent :: Brick.BrickEvent Name () -> Brick.EventM Name AppState ()
handleEvent (Brick.VtyEvent (Vty.EvResize w h)) = do
  st <- Brick.get
  -- 4 reserved lines: header + footer + tab + status. Floor at 1 visible row.
  let visH = max 1 (h - 4)
      st0  = st & asVisH .~ visH & asVisW .~ max 1 w
  st' <- liftIO $ do
    (r, _) <- runAppEff st0 (refreshGrid st0)
    pure (either (const st0) id r)
  Brick.put st'
handleEvent (Brick.VtyEvent ev@(Vty.EvKey k mods)) = do
  st <- Brick.get
  case (st ^. asPendingCmd) of
    -- Prompt mode: route keys to the input buffer instead of dispatching.
    Just c -> case (k, mods) of
      (Vty.KEsc, _) -> Brick.put (st & asPendingCmd .~ Nothing & asCmd .~ "")
      (Vty.KEnter, _) -> do
        -- Commit: dispatch with the prompt buffer as arg.
        r <- liftIO $ handleCmd c (st ^. asCmd) (st & asPendingCmd .~ Nothing & asCmd .~ "")
        case r of
          Nothing -> BM.halt
          Just st' -> Brick.put (st' & asPendingCmd .~ Nothing & asCmd .~ "")
      (Vty.KBS, _) -> Brick.put (st & asCmd .~ if T.null ((st ^. asCmd)) then "" else T.init ((st ^. asCmd)))
      (Vty.KChar ch, _) | ch /= '\t' -> Brick.put (st & asCmd .~ T.snoc ((st ^. asCmd)) ch)
      _ -> pure ()
    -- Normal mode: global Esc quits; everything else goes through keyLookup.
    Nothing -> case (k, mods) of
      (Vty.KEsc, []) -> BM.halt
      _ -> do
        let key = evToKey ev
            ctx = vkCtxStr (st ^. headNav % nsVkind)
        mci <- liftIO $ runEff (keyLookup key ctx)
        case mci of
          Nothing -> pure ()
          Just ci
            | ciCmd ci == TblMenu -> BM.suspendAndResume (runCmdMenu st)
            | ciCmd ci == ThemeOpen -> BM.suspendAndResume (runThemePicker st)
            | otherwise -> do
                isArg <- liftIO $ runEff (CC.isArgCmd (ciCmd ci))
                if isArg
                  then Brick.put (st & asPendingCmd .~ Just (ciCmd ci) & asCmd .~ "" & asMsg .~ "")
                  else do
                    r <- liftIO $ runDispatchIO st (dispatch st ci "")
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

-- | Load a regular file as a TblOps via DuckDB. Handles:
-- 1. .txt files → parse through Tv.Data.Text space-separated parser
-- 2. Recognized formats (parquet, json, xlsx, etc.) → FileFormat.openFile
-- 3. Unknown → PRQL backtick path (DuckDB auto-detect)
loadFile :: FilePath -> IO (Either String TblOps)
loadFile p
  | isGz p = loadGzFile p         -- .gz files: decompress first
  | FF.isTxtFile p = loadTextFile p  -- .txt files: space-separated parser
  | otherwise = do
      r <- try $ do
        conn <- DB.connect ":memory:"
        let q = Prql.Query { Prql.base = "from `" <> T.pack p <> "`", Prql.ops = V.empty }
        total <- DB.queryCount conn q
        mOps <- DB.requery conn q (max total 1)
        case mOps of
          Just ops -> pure ops
          Nothing -> do
            res <- DB.query conn (fileQuery p)
            DB.mkDbOps res
      pure $ case r of
        Left (e :: SomeException) -> Left (displayException e)
        Right ops -> Right ops

-- | Check if file is .gz
isGz :: FilePath -> Bool
isGz p = ".gz" `isSuffixOf` map toLower p

-- | Load .gz file: decompress with zcat, then try CSV → text → viewFile fallback.
-- For .csv.gz, DuckDB handles natively via loadFile. For .txt.gz, decompress first.
loadGzFile :: FilePath -> IO (Either String TblOps)
loadGzFile p = do
  -- First try DuckDB native gz support (works for csv.gz, parquet.gz, etc.)
  r <- try $ do
    conn <- DB.connect ":memory:"
    let q = Prql.Query { Prql.base = "from `" <> T.pack p <> "`", Prql.ops = V.empty }
    total <- DB.queryCount conn q
    mOps <- DB.requery conn q (max total 1)
    case mOps of
      Just ops -> pure ops
      Nothing -> fail "DuckDB can't read gz"
  case r of
    Right ops -> pure (Right ops)
    Left (_ :: SomeException) -> do
      -- Decompress and try as CSV, then text
      r2 <- try $ do
        (ec, out, _) <- readProcessWithExitCode "zcat" [p] ""
        when (ec /= ExitSuccess) $ fail "zcat failed"
        let content = T.pack out
        -- Try as CSV via temp file
        tmpDir <- getTemporaryDirectory
        let tmpPath = tmpDir </> "tv_gz_tmp.csv"
        TIO.writeFile tmpPath content
        conn <- DB.connect ":memory:"
        let q = Prql.Query { Prql.base = "from `" <> T.pack tmpPath <> "`", Prql.ops = V.empty }
        total <- DB.queryCount conn q
        if total > 0 then do
          mOps <- DB.requery conn q total
          case mOps of
            Just ops -> pure ops
            Nothing -> fail "gz csv failed"
        else case DataText.fromText content of
          Right tsv -> do
            let tmpPath2 = tmpDir </> "tv_gz_text.tsv"
            TIO.writeFile tmpPath2 tsv
            let q2 = Prql.Query { Prql.base = "from `" <> T.pack tmpPath2 <> "`", Prql.ops = V.empty }
            total2 <- DB.queryCount conn q2
            mOps2 <- DB.requery conn q2 (max total2 1)
            case mOps2 of
              Just ops -> pure ops
              Nothing -> fail "gz text failed"
          Left _ -> fail "not CSV or text"
      pure $ case r2 of
        Left (e :: SomeException) -> Left (displayException e)
        Right ops -> Right ops

-- | Create a read-only TblOps that just shows text lines (viewFile fallback).
-- Single column "text" with one row per line.
mkTextViewOps :: Vector (Vector Text) -> TblOps
mkTextViewOps rows =
  let nr = V.length rows; nc = 1
      cols = V.singleton "text"
      ops = TblOps
        { _tblNRows = nr, _tblColNames = cols, _tblTotalRows = nr
        , _tblQueryOps = V.empty
        , _tblFilter = \_ -> pure Nothing, _tblDistinct = \_ -> pure V.empty
        , _tblFindRow = \_ _ _ _ -> pure Nothing
        , _tblRender = \_ -> pure V.empty, _tblGetCols = \_ _ _ -> pure V.empty
        , _tblColType = \_ -> CTStr
        , _tblBuildFilter = \_ _ _ _ -> T.empty, _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr = \r c -> if r >= 0 && r < nr && c >= 0 && c < nc
                                then pure ((rows V.! r) V.! c) else pure T.empty
        , _tblFetchMore = pure Nothing
        , _tblHideCols = \_ -> pure ops, _tblSortBy = \_ _ -> pure ops
        }
  in ops

-- | Load .txt file: parse space-separated text, then load as CSV via DuckDB.
loadTextFile :: FilePath -> IO (Either String TblOps)
loadTextFile p = do
  r <- try $ do
    content <- TIO.readFile p
    case DataText.fromText content of
      Left e -> fail e
      Right tsv -> do
        conn <- DB.connect ":memory:"
        -- Write TSV to a temp directory, not next to the source file
        tmpDir <- getTemporaryDirectory
        let tmpPath = tmpDir </> "tv_text_tmp.tsv"
        TIO.writeFile tmpPath tsv
        let q = Prql.Query { Prql.base = "from `" <> T.pack tmpPath <> "`"
                            , Prql.ops = V.empty }
        total <- DB.queryCount conn q
        mOps <- DB.requery conn q (max total 1)
        case mOps of
          Just ops -> pure ops
          Nothing -> fail "text parse: DuckDB load failed"
  pure $ case r of
    Left (e :: SomeException) -> Left (displayException e)
    Right ops -> Right ops

-- | Load a directory as a folder TblOps. The view path is canonicalized
-- so folderEnter can join with entry names unambiguously.
loadFolder :: FilePath -> IO (TblOps, FilePath)
loadFolder p = do
  ap <- canonicalizePath p
  ops <- runEff (Folder.listFolder ap)
  pure (ops, ap)

-- | Open any path: directory → folder view, otherwise → file view. Used
-- both for the initial CLI arg and for in-app navigation. On success the
-- new view is pushed on the stack; on failure a status message is set.
openPath :: AppState -> FilePath -> Eff AppEff (Maybe AppState)
openPath st p = do
  isDir <- liftIO (doesDirectoryExist p)
  if isDir
    then do
      (ops, absPath) <- liftIO (loadFolder p)
      let disp = T.pack absPath
          vk = VFld disp 1
          v' = (\v -> v & vNav % nsVkind .~ vk) <$> fromTbl ops disp 0 V.empty 0
      case v' of
        Nothing -> pure (Just (st & asMsg .~ "empty folder: " <> T.pack p))
        Just v  -> Just <$> refreshGrid (st & asStack %~ vsPush v)
    else do
      -- Use FileFormat for ATTACH/extension formats (.duckdb, .sqlite, .xlsx, etc.)
      -- Use loadFile (PRQL chain) for regular data files (.csv, .parquet, .json, etc.)
      let needsFF = case FF.findFormat p of
            Just fmt -> FF.fmtAttach fmt || not (null (FF.fmtDuckdbExt fmt))
            Nothing  -> False
      if needsFF
        then do
          mv <- tryE (FF.openFile p)
          case mv of
            Just (Just v) -> Just <$> refreshGrid (st & asStack .~ vsPush v ((st ^. asStack)))
            _ -> pure (Just (st & asMsg .~ "open failed: " <> T.pack p))
        else do
          r <- liftIO (loadFile p)
          case r of
            Left e   -> pure (Just (st & asMsg .~ "open failed: " <> T.pack e))
            Right ops -> case fromTbl ops (T.pack p) 0 V.empty 0 of
              Nothing -> pure (Just (st & asMsg .~ "empty: " <> T.pack p))
              Just v  -> Just <$> refreshGrid (st & asStack .~ vsPush v ((st ^. asStack)))

-- | Build an empty initial AppState. The stack head is a placeholder view
-- we replace via openPath before running the brick loop.
initialState :: View -> IO AppState
initialState v = initialStateSized v 200 80

-- | Like 'initialState' but seeds asVisW/asVisH from real terminal bounds
-- so the first frame is laid out at the correct size.
initialStateSized :: View -> Int -> Int -> IO AppState
initialStateSized v tw th = do
  theme <- runEff initTheme
  let st0 = AppState
        { _asStack = ViewStack v []
        , _asThemeIdx = tsThemeIdx theme
        , _asTestKeys = []
        , _asMsg = ""
        , _asErr = ""
        , _asCmd = ""
        , _asPendingCmd = Nothing
        , _asGrid = V.empty
        , _asVisRow0 = 0
        , _asVisCol0 = 0
        , _asVisH = max 1 (th - 4)
        , _asVisW = max 1 tw
        , _asStyles = tsStyles theme
        , _asInfoVis = False
        }
  (r, _) <- runAppEff st0 (refreshGrid st0)
  pure (either (const st0) id r)

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

-- | CLI args: (path, Maybe keys, Maybe sessionName).
parseCliArgs :: [String] -> IO (FilePath, Maybe [Text], Maybe String)
parseCliArgs args = case args of
  ["-c", ks]           -> do p <- getCurrentDirectory; pure (p, Just (Key.tokenizeKeys (T.pack ks)), Nothing)
  [p, "-c", ks]        -> pure (p, Just (Key.tokenizeKeys (T.pack ks)), Nothing)
  ["-s", s]            -> pure ("", Nothing, Just s)
  ["-s", s, "-c", ks]  -> pure ("", Just (Key.tokenizeKeys (T.pack ks)), Just s)
  [p]                  -> pure (p, Nothing, Nothing)
  []                   -> do p <- getCurrentDirectory; pure (p, Nothing, Nothing)
  _                    -> do p <- getCurrentDirectory; pure (p, Nothing, Nothing)

-- | Open a path as a View.
openInitialView :: FilePath -> IO View
openInitialView path = do
  isDir <- doesDirectoryExist path
  if isDir then do
    (ops, absPath) <- loadFolder path
    case fromTbl ops (T.pack absPath) 0 V.empty 0 of
      Nothing -> fail ("empty folder: " ++ path)
      Just v  -> pure (v & vNav % nsVkind .~ VFld (T.pack absPath) 1)
  else do
    let needsFF = case FF.findFormat path of
          Just fmt -> FF.fmtAttach fmt || not (null (FF.fmtDuckdbExt fmt))
          Nothing  -> False
    if needsFF
      then do
        mv <- runEff (FF.openFile path)
        case mv of
          Just v -> pure v
          Nothing -> fail ("open failed: " ++ path)
      else do
        r <- loadFile path
        case r of
          Left _ -> viewFileFallback path
          Right ops
            | (ops ^. tblNRows) > 0 -> case fromTbl ops (T.pack path) 0 V.empty 0 of
                Nothing -> fail ("empty: " ++ path)
                Just v  -> pure v
            | otherwise -> viewFileFallback path

-- | viewFile fallback: show decompressed/raw text as a text table.
-- Marks the view with VViewFile kind so renderText can omit table chrome.
viewFileFallback :: FilePath -> IO View
viewFileFallback path = do
  content <- if isGz path
    then do (_, out, _) <- readProcessWithExitCode "zcat" [path] ""
            pure (T.pack out)
    else TIO.readFile path
  let ls = filter (not . T.null) (T.lines content)
  if null ls then fail ("empty: " ++ path)
  else do
    let rows = V.fromList (map V.singleton ls)
    case fromTbl (mkTextViewOps rows) (T.pack path) 0 V.empty 0 of
      Nothing -> fail ("empty: " ++ path)
      Just v  -> pure (v & vNav % nsVkind .~ VViewFile)

-- | Render the AppState as plain text lines (header, data, footer, tab, status).
-- Mirrors the Lean Term.bufferStr used by -c test mode.
-- Layout: header, sparkline (if any), data rows, footer, tab, status.
-- Columns separated by │ (like the Brick UI).
renderText :: AppState -> IO Text
renderText st = case st ^. headNav % nsVkind of
  VViewFile -> do
    let tbl = st ^. headTbl
        nr  = tbl ^. tblNRows
    ls <- mapM (\r -> (tbl ^. tblCellStr) r 0) [0..nr-1]
    pure (T.unlines ls)
  _ -> renderTextTable st

-- | Render table view as text (the normal path).
renderTextTable :: AppState -> IO Text
renderTextTable st = do
  let ns   = st ^. headNav
      tbl  = ns ^. nsTbl
      disp = ns ^. nsDispIdxs
      widths = R.visColWidths st
      visNames = R.visColNames st
      nVis = V.length visNames
      grpNames = (ns ^. nsGrp)
      nKeys = V.length grpNames
      isNum ci = let p = (st ^. asVisCol0) + ci
                     idx = if p < V.length disp then disp V.! p else 0
                 in isNumeric ((tbl ^. tblColType) idx)
      isGrp ci = (st ^. asVisCol0) + ci < nKeys
      -- Column separator: ║ after last key column, │ otherwise
      sep ci | ci + 1 >= nVis = "│"
             | isGrp ci && not (isGrp (ci + 1)) = "║"
             | otherwise = "│"
      fmtRow row = T.concat [ " " <> R.padCell (max 0 (widths V.! ci - 2)) (isNum ci)
                                       (if ci < V.length row then row V.! ci else "")
                               <> " " <> sep ci
                             | ci <- [0 .. nVis - 1] ]
      hdrLine = T.concat [ " " <> T.justifyLeft (max 0 (widths V.! ci - 2)) ' ' (visNames V.! ci)
                            <> " " <> sep ci
                          | ci <- [0 .. nVis - 1] ]
      grid = (st ^. asGrid)
      dataLines = map fmtRow (V.toList grid)
      infoLines = if (st ^. asInfoVis)
        then map (\(k,d) -> T.justifyRight 5 ' ' k <> " " <> d) (Info.viewHints ((ns ^. nsVkind)))
        else []
      -- Replay ops on tab line (Lean renderTabLine appends ops right-aligned)
      qops = (tbl ^. tblQueryOps)
      replayOps = if V.null qops then "" else " " <> Replay.renderOps qops
      tab  = R.tabText st
      tabLine = tab <> replayOps
  -- Sparklines: one string per column, nBars = column inner width
  sparkLines <- runEff (Sparkline.compute tbl 10)
  let sparkRow = if V.any (not . T.null) sparkLines
        then [T.concat [ " " <> T.justifyLeft (max 0 (widths V.! ci - 2)) ' '
                           (if ci < V.length sparkLines then sparkLines V.! ci else "")
                         <> " " <> sep ci | ci <- [0 .. nVis - 1] ]]
        else []
  -- Status aggregation: Σ/μ/# for current column
  agg <- runEff (StatusAgg.compute tbl (curColIdx ns))
  let aggSuffix = if T.null agg then "" else " " <> agg
      stat = R.statusText st <> aggSuffix
  pure $ T.unlines (hdrLine : sparkRow ++ dataLines ++ infoLines ++ [hdrLine, tabLine, stat])

-- | Main event loop expressed as a free monad program over AppM.
-- Matches Lean's loopProg (Common.lean:426-456): render → poll → keyLookup
-- → isArgCmd check → readArg if needed → dispatch with arg → loop.
loopProg :: AppState -> AppF.AppM AppState AppState
loopProg a = do
  a' <- AppF.doRender a
  mKey <- AppF.poll
  case mKey of
    Nothing -> pure a'
    Just key
      | T.null key -> loopProg a'
      | otherwise -> do
          let ctx = vkCtxStr (a' ^. headNav % nsVkind)
          mci <- liftIO (runEff (keyLookup key ctx))
          case mci of
            Nothing -> loopProg a'
            Just ci -> do
              isArg <- liftIO (runEff (CC.isArgCmd (ciCmd ci)))
              arg <- if isArg then AppF.readArg' else pure ""
              r <- liftIO (runDispatchIO a' (dispatch a' ci arg))
              case r of
                Nothing -> pure a'
                Just a'' -> loopProg a''

-- | Test interpreter (Lean Common.lean:476-511). Keystroke queue in IORef;
-- nextKey pops front, empty → print buffer + return Nothing to exit loopProg.
-- readArg scans forward to <ret>, returns tokens before it as the arg.
testInterp :: IORef [Text] -> AppF.Interp AppState
testInterp ref = AppF.Interp
  { AppF.iRender = \st -> do
      (r, _) <- runAppEff st (refreshGrid st)
      pure (either (const st) id r)
  , AppF.iNextKey = do
      ks <- readIORef ref
      case ks of
        [] -> pure Nothing
        _  -> do
          let (key, ks') = Key.nextKeyFromQueue ks
          writeIORef ref ks'
          pure (Just key)
  , AppF.iReadArg = do
      ks <- readIORef ref
      case break (== "<ret>") ks of
        (before, _ret:after) -> do
          writeIORef ref after
          pure (T.concat before)
        (_, []) -> pure ""  -- no <ret>, same as prod cancel
  }

-- | -c test mode: run loopProg under testInterp, print rendered output.
runTestMode :: AppState -> [Text] -> IO ()
runTestMode st0 keys = do
  runEff (Fzf.setTestMode True)
  ref <- newIORef keys
  st <- AppF.run (testInterp ref) (loopProg st0)
  txt <- renderText st
  TIO.putStr txt
  hFlush stdout

runApp :: IO ()
runApp = do
  runEff (CC.initCmds defaultEntries)
  args <- getArgs
  (path, mKeys, mSess) <- parseCliArgs args
  -- Session mode: load session JSON and rehydrate views
  v <- case mSess of
    Just sessName -> do
      ms <- runEff (Session.loadSession (T.pack sessName))
      case ms of
        Nothing -> do
          hPutStrLn stderr ("Session not found: " ++ sessName)
          -- Return a dummy folder view of CWD
          p <- getCurrentDirectory
          openInitialView p
        Just saved -> do
          -- Rehydrate: the first saved view has a path; re-open it with saved ops
          let views = Session.ssViews saved
          case views of
            (sv:_) -> do
              let p = T.unpack (Session.svPath sv)
              openInitialView p
            [] -> do
              p <- getCurrentDirectory
              openInitialView p
    Nothing -> do
      let p = if null path then "." else path
      openInitialView p
  case mKeys of
    Just ks -> do
      st0 <- initialStateSized v 80 24
      runTestMode st0 ks
    Nothing -> do
      let buildVty = VtyUnix.mkVty Vty.defaultConfig
      initVty <- buildVty
      (tw, th) <- Vty.displayBounds (Vty.outputIface initVty)
      st0 <- initialStateSized v tw th
      _ <- BM.customMain initVty buildVty Nothing app st0
      pure ()
