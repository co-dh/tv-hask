{-# LANGUAGE ScopedTypeVariables #-}
-- | App: brick application wiring, handler dispatch table, CLI entry.
module Tv.App where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Brick
import qualified Brick.Main as BM
import qualified Graphics.Vty as Vty
import qualified Graphics.Vty.Platform.Unix as VtyUnix
import System.Environment (getArgs)
import System.Directory (canonicalizePath, getCurrentDirectory)
import System.FilePath (takeDirectory, takeExtension, (</>))
import Data.Char (toLower)
import System.IO (hPutStrLn, stderr, hFlush, stdout)
import Control.Exception (SomeException, displayException)
import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Control.Applicative ((<|>))
import Data.List (find, sort)
import Data.Maybe (fromMaybe)
import qualified Data.Text.IO as TIO
import Tv.Types
import Tv.View
import qualified Tv.Nav as Nav
import Tv.Render
import qualified Tv.Render as R
import Optics.Core ((^.), (%), (&), (.~))
import qualified Tv.Key as Key
import Tv.Key (evToKey)
import Tv.CmdConfig (keyLookup, CmdInfo(..))
import qualified Tv.CmdConfig as CC
import Tv.Eff
  ( Eff, AppEff, runEff, runAppEff, tryEitherE
  , use, (.=), (%=)
  )
import Tv.Handler (Handler, cont, refresh, setMsg, withNav, pushOps, pushOpsAt, curOps, refreshGrid)
import Tv.Loader (openPath, openInitialView, loadFolder, enterDbTable)
import Tv.Theme (initTheme, ThemeState(..), themeName, themes, applyTheme, tsStyles, tsThemeIdx)
import qualified Tv.Theme
import qualified Tv.Folder as Folder
import Tv.Freq (freqOpenH, freqFilterH)
import Tv.Meta (metaPushH, metaSetKeyH, metaSelNullH, metaSelSingleH)
import Tv.Transpose (xposeH)
import Tv.Derive (deriveH)
import Tv.Split (splitH)
import Tv.Diff (diffH)
import Tv.Join (joinH)
import Tv.Session (sessSaveH, sessLoadH)
import Tv.Export (exportH)
import qualified Tv.Session as Session
import qualified Tv.Fzf as Fzf
import Tv.Plot (plotH)
import qualified Tv.UI.Info as Info
import Tv.UI.Info (infoTogH)
import qualified Tv.Replay as Replay
import qualified Tv.StatusAgg as StatusAgg
import qualified Tv.Sparkline as Sparkline
import Data.IORef
import Data.Word (Word8)
import qualified Tv.AppF as AppF

-- | Quit handler — returning False signals halt.
quitH :: Handler
quitH _ = pure False

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
  , (FolderDel, Folder.folderDelH)
  , (FolderDepthInc, Folder.folderDepthIncH)
  , (FolderDepthDec, Folder.folderDepthDecH)
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
sortH asc _ = do
  ns <- use headNav
  let ops = ns ^. nsTbl
      ci  = curColIdx ns
  ops' <- liftIO ((ops ^. tblSortBy) (V.singleton ci) asc)
  headTbl .= ops'
  refresh

-- | Drop column(s) from the query: current column + any hidden columns.
-- Lean View.update ColExclude produces Effect.exclude (hidden ++ [current]).
excludeH :: Handler
excludeH _ = do
  ns <- use headNav
  let ops   = ns ^. nsTbl
      ci    = curColIdx ns
      names = ops ^. tblColNames
      hiddenIdxs = V.mapMaybe (`V.elemIndex` names) (ns ^. nsHidden)
      allIdxs = if V.elem ci hiddenIdxs then hiddenIdxs
                else V.snoc hiddenIdxs ci
  ops' <- liftIO ((ops ^. tblHideCols) allIdxs)
  path <- use (headView % vPath)
  pushOpsAt path 0 V.empty "exclude: empty result" VTbl ops'

-- | Set the heatmap mode on the head view's NavState.
heatH :: Word8 -> Handler
heatH m _ = headNav % nsHeatMode .= m >> refresh

-- | Adjust decimal precision on the head view's NavState, clamped 0..17.
precH :: (Int -> Int) -> Handler
precH f _ = do
  p <- use (headNav % nsPrecAdj)
  headNav % nsPrecAdj .= max 0 (min 17 (f p))
  refresh

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
        pure (fromMaybe st r)

-- | Swap top two views in the stack.
stkSwapH :: Handler
stkSwapH _ = asStack %= vsSwap >> refresh

-- | Duplicate the top view on the stack.
stkDupH :: Handler
stkDupH _ = asStack %= vsDup >> refresh

-- | Push a fresh folder view at the current directory (or the
-- directory of the current file if we're in a non-folder view).
folderPushH :: Handler
folderPushH _ = do
  vk <- use (headNav % nsVkind)
  case vk of
    VFld base depth -> do
      ops <- Folder.listFolderDepth (T.unpack base) depth
      pushOps base (VFld base depth) ops
    _ -> do
      path <- use (headView % vPath)
      absDir <- liftIO (canonicalizePath (takeDirectory (T.unpack path)))
      openPath absDir

-- | Search the current column for the buffer in @asCmd@ and jump the row
-- cursor to the first row whose value contains it.  Uses tblFindRow
-- which the DuckDB ops implement as a SELECT rowid WHERE col LIKE …;
-- fails-soft with a status message if not found.
-- | Row search: mirrors Tc/Tc/Filter.lean:42 rowSearch. When arg is
-- empty (normal `/` key) we fzf-pick one of the current column's
-- distinct values; when arg is non-empty (direct handleCmd from tests)
-- we treat it as a literal value. Then jump to the first matching row
-- and store (col, value) in nsSearch so n/N can step through matches.
rowSearchH :: Handler
rowSearchH arg = do
  ns <- use headNav
  let ops = ns ^. nsTbl
      ci  = curColIdx ns
      cn  = curColName ns
  query <-
    if not (T.null arg) then pure (Just arg)
    else do
      vals <- liftIO ((ops ^. tblDistinct) ci)
      let sorted = sort (V.toList vals)
          input  = T.intercalate "\n" sorted
          prompt = "--prompt=/" <> cn <> ": "
      Fzf.fzf [prompt] input
  case query of
    Nothing -> cont
    Just q
      | T.null q  -> cont
      | otherwise -> do
          let startRow = ns ^. nsRow % naCur + 1
          mr <- liftIO ((ops ^. tblFindRow) ci q startRow True)
          case mr of
            Just r  -> do
              headNav % nsRow % naCur .= r
              headNav % nsSearch      .= q
              asMsg                   .= ""
              refresh
            Nothing -> setMsg ("not found: " <> q)

-- | n / N: jump to the next / previous search match. Reuses the
-- previously-stored query in @nsSearch@ and calls tblFindRow with
-- an advanced starting row.
rowSearchStepH :: Int -> Handler
rowSearchStepH step _ = do
  ns <- use headNav
  let q = ns ^. nsSearch
  if T.null q then setMsg "no prior search"
  else do
    let ops = ns ^. nsTbl
        ci  = curColIdx ns
        startRow = ns ^. nsRow % naCur + step
    mr <- liftIO ((ops ^. tblFindRow) ci q startRow (step > 0))
    case mr of
      Just r -> do
        headNav % nsRow % naCur .= r
        asMsg                   .= ""
        refresh
      Nothing -> setMsg ("no more matches: " <> q)

rowSearchNextH, rowSearchPrevH :: Handler
rowSearchNextH = rowSearchStepH 1
rowSearchPrevH = rowSearchStepH (-1)

-- | Filter: when arg is empty, pick first distinct value via fzf (Lean
-- Filter.lean:rowFilter). When arg is non-empty, apply it as a raw PRQL
-- filter expression (Lean filterWith path).
rowFilterH :: Handler
rowFilterH arg
  | T.null arg = do
      ops <- curOps
      ns  <- use headNav
      let colIdx  = curColIdx ns
          colName = curColName ns
          isNum   = isNumeric ((ops ^. tblColType) colIdx)
      vals <- liftIO ((ops ^. tblDistinct) colIdx)
      let sorted = V.fromList (sort (V.toList vals))
          input  = T.intercalate "\n" (V.toList sorted)
      mResult <- Fzf.fzf ["--print-query", "--prompt=filter > "] input
      case mResult of
        Nothing -> cont
        Just result -> do
          let expr = (ops ^. tblBuildFilter) colName sorted result isNum
          applyFilter ops colName expr
  | otherwise = do
      ops <- curOps
      ns  <- use headNav
      applyFilter ops (curColName ns) arg
  where
    applyFilter ops' cn expr
      | T.null expr = cont
      | otherwise = do
          mOps <- liftIO ((ops' ^. tblFilter) expr)
          case mOps of
            Nothing -> setMsg ("filter failed: " <> expr)
            Just ops'' -> do
              path <- use (headView % vPath)
              case fromTbl ops'' (path <> " \\" <> cn) 0 V.empty 0 of
                Nothing -> setMsg "filter: empty result"
                Just v  -> asStack %= vsPush v >> refresh

-- | Goto column by name prefix/substring. First case-insensitive prefix
-- match wins; falls back to substring match so users can type partial
-- names. Moves the nsCol cursor, no new view.
colSearchH :: Handler
colSearchH arg
  | T.null arg = setMsg "goto: empty name"
  | otherwise = do
      ns <- use headNav
      let names = ns ^. nsTbl % tblColNames
          disp  = ns ^. nsDispIdxs
          q = T.toLower arg
          at i = T.toLower (names V.! (disp V.! i))
          idxRange = [0 .. V.length disp - 1]
      case find ((q `T.isPrefixOf`) . at) idxRange <|> find ((q `T.isInfixOf`) . at) idxRange of
        Just i  -> do
          headNav % nsCol % naCur .= i
          asMsg .= ""
          refresh
        Nothing -> setMsg ("no column matches: " <> arg)

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

-- | Pop the top view; quit if it was the last.
stkPopH :: Handler
stkPopH _ = do
  stk <- use asStack
  case vsPop stk of
    Nothing  -> pure False
    Just vs' -> asStack .= vs' >> refresh

-- | Folder enter: resolve the current row's path cell, load the target as
-- a new view and push it. Directories → listFolder, files → DuckDB read_*.
-- Database folder views (.duckdb/.sqlite) → query the named table.
folderEnterH :: Handler
folderEnterH _ = do
  ns <- use headNav
  case ns ^. nsVkind of
    VFld base _ -> do
      let r = ns ^. nsRow % naCur
      name <- liftIO ((ns ^. nsTbl % tblCellStr) r 0)
      if T.null name then cont
      else if isDbFile (T.unpack base) && name /= ".."
        then enterDbTable (T.unpack base) name
        else openPath (T.unpack base </> T.unpack name)
    _ -> cont
  where
    isDbFile p =
      let ext = map toLower (takeExtension p)
      in ext `elem` [".duckdb", ".db", ".sqlite", ".sqlite3"]

-- | Parent: replace current folder view with one rooted at parent dir.
-- Replaces in-place (doesn't push) so backspace chain works correctly.
folderParentH :: Handler
folderParentH _ = do
  vk <- use (headNav % nsVkind)
  case vk of
    VFld base _ -> do
      parentPath <- liftIO (canonicalizePath (T.unpack base </> ".."))
      (ops, absP) <- liftIO (loadFolder parentPath)
      let disp = T.pack absP
          vk' = VFld disp 1
      case fromTbl ops disp 0 V.empty 0 of
        Nothing -> setMsg "parent: empty"
        Just v  -> asStack % vsHd .= (v & vNav % nsVkind .~ vk') >> refresh
    _ -> cont

-- | Dispatch @cmdinfo@ + @arg@ to the handler registered in 'handlerMap'.
-- Returns False to quit, True to continue.
dispatch :: CmdInfo -> Text -> Eff AppEff Bool
dispatch ci arg = case Map.lookup (ciCmd ci) handlerMap of
  Nothing -> cont
  Just h  -> h arg

-- | IO-facing wrapper around an Eff action. Runs under 'runAppEff' and
-- collapses exceptions to an @asErr@ message on the pre-dispatch state
-- so a partially-mutated state from a crashing handler is discarded.
runDispatchIO :: AppState -> Eff AppEff Bool -> IO (Maybe AppState)
runDispatchIO st act = do
  (r, st') <- runAppEff st act
  case r of
    Right True  -> pure (Just st')
    Right False -> pure Nothing
    Left e      -> pure (Just (st & asErr .~ T.pack (displayException e)))

-- | Convenience: dispatch by Cmd + arg. IO-facing for tests and
-- 'suspendAndResume' callbacks.
handleCmd :: Cmd -> Text -> AppState -> IO (Maybe AppState)
handleCmd cmd arg st = runDispatchIO st $ do
  ci <- CC.cmdLookup cmd
  dispatch ci arg

-- | Dispatch a single key through 'keyLookup' + 'dispatch'. Non-arg
-- commands get empty arg; arg commands also get empty arg (use
-- 'loopProg' + 'testInterp' for full arg-passing in -c test mode).
-- Kept for unit tests that drive one key.
handleKey :: Text -> AppState -> IO (Maybe AppState)
handleKey key st = runDispatchIO st $ do
  let ctx = vkCtxStr (st ^. headNav % nsVkind)
  mci <- keyLookup key ctx
  case mci of
    Nothing -> cont
    Just ci -> dispatch ci ""

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
    (_, s') <- runAppEff st0 refreshGrid
    pure s'
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
                    r <- liftIO $ runDispatchIO st (dispatch ci "")
                    case r of
                      Nothing  -> BM.halt
                      Just st' -> Brick.put st'
handleEvent _ = pure ()

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
        , _asVisColN = 0
        , _asVisH = max 1 (th - 4)
        , _asVisW = max 1 tw
        , _asStyles = tsStyles theme
        , _asInfoVis = False
        }
  (_, st1) <- runAppEff st0 refreshGrid
  pure st1

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
      hdrCell ci =
        let inner = max 0 (widths V.! ci - 2)
        in T.take inner (T.justifyLeft inner ' ' (visNames V.! ci))
      hdrLine = T.concat [ " " <> hdrCell ci <> " " <> sep ci | ci <- [0 .. nVis - 1] ]
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

-- | Main event loop: render → poll → keyLookup → isArgCmd check →
-- readArg if needed → dispatch with arg → loop. Runs entirely inside
-- 'Eff AppEff'; the three operations that differ between prod and test
-- (render, nextKey, readArg) are supplied via an 'AppF.Interp' record.
loopProg :: AppF.Interp -> Eff AppEff ()
loopProg i = go
  where
    go = do
      AppF.iRender i
      mKey <- AppF.iNextKey i
      case mKey of
        Nothing -> pure ()
        Just key
          | T.null key -> go
          | otherwise -> do
              ctx <- vkCtxStr <$> use (headNav % nsVkind)
              mci <- keyLookup key ctx
              case mci of
                Nothing -> go
                Just ci -> do
                  isArg <- CC.isArgCmd (ciCmd ci)
                  arg <- if isArg then AppF.iReadArg i else pure ""
                  continue <- dispatch ci arg
                  when continue go

-- | Test interpreter. Keystroke queue in IORef; nextKey pops front,
-- empty → Nothing to exit. readArg scans forward to <ret>, returns
-- tokens before it as the arg.
testInterp :: IORef [Text] -> AppF.Interp
testInterp ref = AppF.Interp
  { AppF.iRender = refreshGrid
  , AppF.iNextKey = liftIO $ do
      ks <- readIORef ref
      case ks of
        [] -> pure Nothing
        _  -> do
          let (key, ks') = Key.nextKeyFromQueue ks
          writeIORef ref ks'
          pure (Just key)
  , AppF.iReadArg = liftIO $ do
      ks <- readIORef ref
      case break (== "<ret>") ks of
        (before, _ret:after) -> do
          writeIORef ref after
          pure (T.concat before)
        (_, []) -> pure ""
  }

-- | -c test mode: run loopProg under testInterp, print rendered output.
runTestMode :: AppState -> [Text] -> IO ()
runTestMode st0 keys = do
  runEff (Fzf.setTestMode True)
  ref <- newIORef keys
  (_, st) <- runAppEff st0 (loopProg (testInterp ref))
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
