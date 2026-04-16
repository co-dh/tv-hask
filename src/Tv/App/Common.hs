-- App/Common: app state, dispatch, main loop
--
-- Literal port of Tc/Tc/App/Common.lean — same AppState fields, same handler
-- combinators, same commands array in the same order, same loopProg control
-- flow. The free monad lives in Tv.AppF; Lean's `abbrev HandlerFn` becomes a
-- plain Haskell type alias here, and the single `handlerMap` IORef is shared
-- across all commands.
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.App.Common where

import qualified Control.Concurrent
import Control.Exception (SomeException, try)
import Control.Monad (when)
import Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8, Word32)
import System.IO.Unsafe (unsafePerformIO)

import qualified Tv.AppF as AppM
import Tv.AppF (AppM, Interp(..))
import qualified Tv.CmdConfig as CmdConfig
import Tv.CmdConfig (Entry(..), CmdInfo(..))
import qualified Tv.Derive as Derive
import qualified Tv.Diff as Diff
import qualified Tv.Export as Export
import qualified Tv.Filter as Filter
import qualified Tv.Folder as Folder
import qualified Tv.Fzf as Fzf
import qualified Tv.Join as Join
import qualified Tv.Key as Key
import Optics.Core (Lens', (%), (&), (.~), (^.), over, set)
import qualified Tv.Meta as Meta
import qualified Tv.Nav as Nav
import qualified Tv.Plot as Plot
import qualified Tv.Render as Render
import Tv.Render (ViewState, renderTabLine, errorPopup)
import qualified Tv.Replay as Replay
import qualified Tv.Runner as Freq
import qualified Tv.Session as Session
import qualified Tv.Sparkline as Sparkline
import qualified Tv.Split as Split
import qualified Tv.StatusAgg as StatusAgg
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import qualified Tv.Transpose as Transpose
import Tv.Types
  ( Cmd(..)
  , Effect(..)
  , ViewKind(..)
  , cmdPlotKindQ
  , effectIsNone
  , viewKindCtxStr
  )
import qualified Tv.Types as TblOps
import qualified Tv.UI.Info as UIInfo
import qualified Tv.UI.Preview as UIPreview
import qualified Tv.Util as Log
import qualified Tv.Util as Socket
import qualified Tv.Data.ADBC.Ops as Ops
import qualified Tv.Data.ADBC.Table as AdbcTable
import Tv.Data.ADBC.Table (AdbcTable)
import Tv.View (View(..), ViewStack(..))
import qualified Tv.View as View
import Optics.TH (makeFieldLabelsNoPrefix)

-- | App state: view stack + render state + theme + info + preview scroll
data AppState = AppState
  { stk         :: ViewStack AdbcTable
  , vs          :: ViewState
  , theme       :: Theme.State
  , info        :: UIInfo.State
  , prevScroll  :: Int
  , heatMode    :: Word8                    -- 0=off, 1=numeric, 2=categorical, 3=both
  , sparklines  :: Vector Text              -- per-column sparkline cache (empty = recompute)
  , statusCache :: (Text, Text, Text)       -- (path, col, desc) — avoids per-frame DB query
  , aggCache    :: StatusAgg.Cache
  }
makeFieldLabelsNoPrefix ''AppState

-- | Shared by precSet / precAdj — the only lens composition in this module
-- that's used in more than one place, so it earns a name.
curPrecL :: Lens' AppState Int
curPrecL = #stk % #hd % #prec

-- | Dispatch result: quit, unhandled, or new state
data Action
  = ActQuit
  | ActUnhandled
  | ActOk AppState

-- | Handler function type: (state, cmdInfo, arg) → IO Action
-- arg is empty for non-arg commands, contains user input for arg commands.
type HandlerFn = AppState -> CmdInfo -> Text -> IO Action

-- | Unified handler map: Cmd → HandlerFn.
-- One HashMap for all commands (pure, IO, arg). Lookup falls back to viewUp.
handlerMap :: IORef (HashMap Cmd HandlerFn)
handlerMap = unsafePerformIO (newIORef HashMap.empty)
{-# NOINLINE handlerMap #-}

-- namespace AppState

-- | Reset view state caches (after data changes)
resetVS :: AppState -> AppState
resetVS a = a & #vs .~ Render.viewStateDefault & #sparklines .~ V.empty

-- | Update stk, reset vs if ci.resetsVS
withStk :: AppState -> CmdInfo -> ViewStack AdbcTable -> AppState
withStk a ci s' =
  a & #stk .~ s'
    & #vs .~ (if ciResetsVS ci then Render.viewStateDefault else a ^. #vs)

-- | Log error, show popup, return unchanged state
errAction :: AppState -> SomeException -> IO Action
errAction a e = do
  let msg = T.pack (show e)
  Log.errorLog msg
  errorPopup msg
  pure (ActOk a)

-- | Try/catch wrapper for stack-level IO (resets vs+sparklines on success)
tryStk
  :: AppState -> CmdInfo
  -> IO (Maybe (ViewStack AdbcTable))
  -> IO Action
tryStk a ci f = do
  r <- try f :: IO (Either SomeException (Maybe (ViewStack AdbcTable)))
  case r of
    Right (Just s') -> pure (ActOk (resetVS (withStk a ci s')))
    Right Nothing   -> pure (ActOk a)
    Left  e         -> errAction a e

-- | Execute a residual Effect from View.update/Freq.update inline
runViewEffect
  :: AppState -> CmdInfo -> View AdbcTable -> Effect -> IO Action
runViewEffect a ci v' e = do
  let s  = stk a
      a' = withStk a ci (View.setCur s v')
  case e of
    EffectNone -> pure (ActOk a')
    EffectQuit -> pure ActQuit
    EffectFetchMore -> do
      r <- try (TblOps.fetchMore (View.tbl s))
             :: IO (Either SomeException (Maybe AdbcTable))
      case r of
        Right (Just tbl') ->
          case View.rebuild v' tbl' 0 V.empty (Nav.cur (Nav.row (View.nav v'))) of
            Just rv -> pure (ActOk (resetVS (a' { stk = View.setCur s rv })))
            Nothing -> pure (ActOk a')
        Right Nothing  -> pure (ActOk a')
        Left  err      -> errAction a' err
    EffectSort colIdx sels grp asc -> tryStk a ci $ do
      tbl' <- TblOps.modifyTableSort (View.tbl s) colIdx sels grp asc
      let mrv = View.rebuild v' tbl' colIdx V.empty (Nav.cur (Nav.row (View.nav v')))
      pure (fmap (View.setCur s) mrv)
    EffectExclude cols -> tryStk a ci $ do
      tbl' <- AdbcTable.excludeCols (View.tbl s) cols
      let grp'    = V.filter (not . (`V.elem` cols)) (Nav.grp (View.nav v'))
          hidden' = V.filter (not . (`V.elem` cols)) (Nav.hidden (View.nav v'))
          mrv = View.rebuild v' tbl' 0 grp' (Nav.cur (Nav.row (View.nav v')))
      pure $ fmap (\rv -> View.setCur s (rv & #nav % #hidden .~ hidden')) mrv
    EffectFreq colNames -> tryStk a ci $ do
      mft <- AdbcTable.freqTable (View.tbl s) colNames
      case mft of
        Nothing -> pure Nothing
        Just (adbc, totalGroups) ->
          case View.fromTbl adbc (View.path (View.cur s)) 0 colNames 0 of
            Just fv ->
              pure (Just (View.push s (fv
                { View.vkind = VkFreqV colNames totalGroups
                , View.disp  = "freq " <> TblOps.joinWith colNames ","
                })))
            Nothing -> pure Nothing
    EffectFreqFilter cols row -> tryStk a ci $ do
      case (View.vkind (View.cur s), View.pop s) of
        (VkFreqV _ _, Just s') -> do
          expr <- Freq.filterExprIO (View.tbl s) cols row
          mf <- TblOps.filter_ (View.tbl s') expr
          case mf of
            Just tbl' ->
              case View.rebuild (View.cur s') tbl' 0 V.empty 0 of
                Just rv -> pure (Just (View.push s' rv))
                Nothing -> pure Nothing
            Nothing -> pure Nothing
        _ -> pure Nothing

-- | View.update + runViewEffect fallback for nav/sort/exclude/freq handlers
viewUp :: AppState -> CmdInfo -> IO Action
viewUp a ci = do
  -- freq handlers: try Freq.update first, then View.update
  freqFirst <-
    if ciCmd ci == CmdFreqOpen || ciCmd ci == CmdFreqFilter
      then case Freq.update (stk a) (ciCmd ci) of
             Just (s', e) ->
               Just <$> runViewEffect (withStk a ci s') ci (View.cur s') e
             Nothing -> pure Nothing
      else pure Nothing
  case freqFirst of
    Just r  -> pure r
    Nothing ->
      case View.update (View.cur (stk a)) (ciCmd ci) 20 of
        Just (v', e) -> runViewEffect a ci v' e
        Nothing      -> pure ActUnhandled

-- | Pure-only dispatch for preview polling (fzf cmd mode). No IO effects, no HashMap.
-- Only called from runMenu poll loop — perf not critical.
pureDispatch :: AppState -> CmdInfo -> Maybe AppState
pureDispatch a ci
  | ciCmd ci == CmdStkDup || ciCmd ci == CmdStkPop || ciCmd ci == CmdStkSwap =
      case View.updateStack (stk a) (ciCmd ci) of
        Just (s', _) -> Just (withStk a ci s')
        Nothing      -> Nothing
  | otherwise =
      -- Nav-only: try View.update, keep only .none effects
      case View.update (View.cur (stk a)) (ciCmd ci) 20 of
        Just (v', e)
          | effectIsNone e -> Just (withStk a ci (View.setCur (stk a) v'))
        _ -> Nothing

-- | Full dispatch: unified HashMap lookup with viewUp fallback.
-- Returns ActQuit to exit, ActUnhandled if command not recognized, ActOk for everything else.
dispatch :: AppState -> CmdInfo -> Text -> IO Action
dispatch a ci arg = do
  m <- readIORef handlerMap
  case HashMap.lookup (ciCmd ci) m of
    Just f  -> f a ci arg
    Nothing -> viewUp a ci  -- default: View.update + runViewEffect

-- | fzf command menu with live socket polling for preview
runMenu :: AppState -> IO Action
runMenu a = do
  ref <- newIORef a
  let poll :: IO ()
      poll = do
        mcmd <- Socket.pollCmd
        case mcmd of
          Just cmdStr -> do
            Log.write "sock" ("poll cmd=" <> cmdStr)
            mci <- CmdConfig.handlerLookup cmdStr
            case mci of
              Just ci -> do
                a0 <- readIORef ref
                case pureDispatch a0 ci of
                  Just a' -> do
                    (vs', v') <- View.doRender (View.cur (stk a'))
                                   (vs a') (Theme.styles (theme a'))
                                   (heatMode a') (sparklines a')
                    writeIORef ref (a' { stk = View.setCur (stk a') v', vs = vs' })
                    Term.present
                  Nothing -> pure ()
              Nothing -> pure ()
          Nothing -> pure ()
  handler <- Fzf.cmdMode (View.vkind (View.cur (stk a))) poll
  a' <- readIORef ref
  _ <- Socket.pollCmd  -- drain stale preview command from fzf focus
  case handler of
    Just h -> do
      mci <- CmdConfig.handlerLookup h
      case mci of
        Just ci -> dispatch a' ci ""
        Nothing -> pure (ActOk a')
    Nothing -> pure (ActOk a')

-- | Run a stack-level IO action with shared error handling and state reset
runStackIO :: AppState -> IO (ViewStack AdbcTable) -> IO Action
runStackIO a f = do
  r <- try f :: IO (Either SomeException (ViewStack AdbcTable))
  case r of
    Right s' -> pure (ActOk (resetVS (a & #stk .~ s')))
    Left  e  -> errAction a e

-- end namespace AppState

-- | Handler combinators — build HandlerFn from domain functions
-- set prec to absolute value
precSet :: Int -> HandlerFn
precSet v = \a _ _ -> pure (ActOk (set curPrecL v a))

-- adjust prec by delta, clamped to [0,17]
precAdj :: Int -> HandlerFn
precAdj delta = \a _ _ ->
  pure (ActOk (over curPrecL (\p -> min 17 (max 0 (p + delta))) a))

-- domain dispatch with tryStk + viewUp fallback
domainH
  :: (ViewStack AdbcTable -> Cmd -> Maybe (IO (Maybe (ViewStack AdbcTable))))
  -> HandlerFn
domainH d = \a ci _ ->
  case d (stk a) (ciCmd ci) of
    Just f  -> tryStk a ci f
    Nothing -> viewUp a ci

-- domain dispatch returning non-optional (Filter style)
domainH'
  :: (ViewStack AdbcTable -> Cmd -> Maybe (IO (ViewStack AdbcTable)))
  -> HandlerFn
domainH' d = \a ci _ ->
  case d (stk a) (ciCmd ci) of
    Just f  -> tryStk a ci (Just <$> f)
    Nothing -> viewUp a ci

-- stack op
stkH :: HandlerFn
stkH = \a ci _ ->
  pure (case View.updateStack (stk a) (ciCmd ci) of
    Just (_, EffectQuit) -> ActQuit
    Just (s', _)         -> ActOk (withStk a ci s')
    Nothing              -> ActUnhandled)

-- plot: Cmd → PlotKind → run
plotH :: HandlerFn
plotH = \a ci _ ->
  case cmdPlotKindQ (ciCmd ci) of
    Just k  -> tryStk a ci (Plot.run (stk a) k)
    Nothing -> pure ActUnhandled

-- arg command: fzf version when empty, direct when arg given
argH
  :: (ViewStack AdbcTable -> IO (ViewStack AdbcTable))
  -> (ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable))
  -> HandlerFn
argH fzf direct = \a _ arg ->
  runStackIO a (if T.null arg then fzf (stk a) else direct (stk a) arg)

-- | viewUp as HandlerFn (for freq/nav fallthrough)
vuH :: HandlerFn
vuH = \a ci _ -> viewUp a ci

-- | Entry constructor shorthand — empty defaults for optional fields.
mkEntry :: Cmd -> Text -> Text -> Text -> Bool -> Text -> Entry
mkEntry c ctx key label resets vctx = Entry
  { entryCmd      = c
  , entryCtx      = ctx
  , entryKey      = key
  , entryLabel    = label
  , entryResetsVS = resets
  , entryViewCtx  = vctx
  }

-- | Single source of truth: command metadata + handler function.
-- Nothing fn = nav/sort handler (falls through to viewUp in dispatch).
-- nav/sort handlers: no explicit fn, falls through to viewUp
navE :: Entry -> (Entry, Maybe HandlerFn)
navE e = (e, Nothing)

-- handlers with explicit fn
cmd :: Entry -> HandlerFn -> (Entry, Maybe HandlerFn)
cmd e f = (e, Just f)

commands :: Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ -- row navigation                              ctx: r=row c=col g=groups s=sels a=arg S=stack
    navE (mkEntry CmdRowInc   "r"  "j"        ""                                  False  "")
  , navE (mkEntry CmdRowDec   "r"  "k"        ""                                  False  "")
  , navE (mkEntry CmdRowPgdn  "r"  "<pgdn>"   ""                                  False  "")
  , navE (mkEntry CmdRowPgup  "r"  "<pgup>"   ""                                  False  "")
  , navE (mkEntry CmdRowPgdn  "r"  "<C-d>"    ""                                  False  "")
  , navE (mkEntry CmdRowPgup  "r"  "<C-u>"    ""                                  False  "")
  , navE (mkEntry CmdRowTop   "r"  "<home>"   ""                                  False  "")
  , navE (mkEntry CmdRowBot   "r"  "<end>"    ""                                  False  "")
  , navE (mkEntry CmdRowSel   "r"  "T"        "Select/deselect current row"       False  "")
    -- row search/filter
  , cmd (mkEntry CmdRowSearch "ca" "/"        "Search for value in current column" True  "")
        (\a _ arg -> do
          if not (T.null arg)
            then runStackIO a (Filter.searchWith (stk a) arg)
            else do
              ref <- newIORef a
              let preview :: ViewStack AdbcTable -> IO ()
                  preview stk' = do
                    a' <- readIORef ref
                    (vs', v') <- View.doRender (View.cur stk')
                                   (vs a') (Theme.styles (theme a'))
                                   (heatMode a') (sparklines a')
                    writeIORef ref (a' { stk = View.setCur stk' v', vs = vs' })
                    renderTabLine (View.tabNames stk') 0 (Replay.opsStr (View.cur stk'))
                    when (UIInfo.vis (info a')) $ do
                      h <- Term.height; w <- Term.width
                      UIInfo.render (fromIntegral h) (fromIntegral w) (View.vkind (View.cur stk'))
                    Term.present
              stk' <- Filter.rowSearchLive (stk a) preview
              a' <- readIORef ref
              pure (ActOk (resetVS (a' { stk = stk' }))))
  , cmd (mkEntry CmdRowFilter "a"  "\\"       "Filter rows by PRQL expression"    True  "")
        (argH Filter.rowFilter Filter.filterWith)
  , cmd (mkEntry CmdRowSearchNext "rc" "n"    "Jump to next search match"         False "")
        (domainH' Filter.dispatch)
  , cmd (mkEntry CmdRowSearchPrev "rc" "N"    "Jump to previous search match"     False "")
        (domainH' Filter.dispatch)
    -- col navigation
  , navE (mkEntry CmdColInc     "c"  "l"         ""                                False "")
  , navE (mkEntry CmdColDec     "c"  "h"         ""                                False "")
  , navE (mkEntry CmdColFirst   "c"  ""          ""                                False "")
  , navE (mkEntry CmdColLast    "c"  ""          ""                                False "")
  , navE (mkEntry CmdColGrp     "c"  "!"         "Toggle group on current column"  False "")
  , navE (mkEntry CmdColHide    "c"  "H"         "Hide/unhide current column"      False "")
  , navE (mkEntry CmdColExclude "c"  "x"         "Delete column(s) from query"     True  "")
  , navE (mkEntry CmdColShiftL  "c"  "<S-left>"  "Shift key column left"           False "")
  , navE (mkEntry CmdColShiftR  "c"  "<S-right>" "Shift key column right"          False "")
    -- col sort
  , navE (mkEntry CmdSortAsc    "c"  "["         "Sort ascending"                  True  "")
  , navE (mkEntry CmdSortDesc   "c"  "]"         "Sort descending"                 True  "")
    -- col arg commands
  , cmd (mkEntry CmdColSplit   "ca" ":"         "Split column by delimiter"       False "")
        (argH Split.run Split.runWith)
  , cmd (mkEntry CmdColDerive  "a"  "="         "Derive new column (name = expr)" False "")
        (argH Derive.run Derive.runWith)
  , cmd (mkEntry CmdColSearch  "a"  "g"         "Jump to column by name"          True  "")
        (argH Filter.colSearch Filter.colJumpWith)
    -- col plot
  , cmd (mkEntry CmdPlotArea    "cg" ""  "Area (g=x numeric, c=y numeric)"         False "") plotH
  , cmd (mkEntry CmdPlotLine    "cg" ""  "Line (g=x numeric, c=y numeric)"         False "") plotH
  , cmd (mkEntry CmdPlotScatter "cg" ""  "Scatter (g=x numeric, c=y numeric)"      False "") plotH
  , cmd (mkEntry CmdPlotBar     "cg" ""  "Bar (g=x categorical, c=y numeric)"      False "") plotH
  , cmd (mkEntry CmdPlotBox     "cg" ""  "Boxplot (g=x categorical, c=y numeric)"  False "") plotH
  , cmd (mkEntry CmdPlotStep    "cg" ""  "Step (g=x numeric, c=y numeric)"         False "") plotH
  , cmd (mkEntry CmdPlotHist    "c"  ""  "Histogram (c=numeric column)"            False "") plotH
  , cmd (mkEntry CmdPlotDensity "c"  ""  "Density (c=numeric column)"              False "") plotH
  , cmd (mkEntry CmdPlotViolin  "cg" ""  "Violin (g=x categorical, c=y numeric)"   False "") plotH
    -- stk: view stack operations
  , cmd (mkEntry CmdTblMenu    ""   " "  "Open command menu"                       False "")
        (\a _ _ -> runMenu a)
  , cmd (mkEntry CmdStkSwap    "S"  "S"  "Swap top two views"                      False "") stkH
  , cmd (mkEntry CmdStkPop     ""   "q"  "Close current view"                      True  "") stkH
  , cmd (mkEntry CmdStkDup     ""   ""   "Duplicate current view"                  False "") stkH
  , cmd (mkEntry CmdTblQuit    ""   ""   ""                                        False "")
        (\_ _ _ -> pure ActQuit)
  , cmd (mkEntry CmdTblXpose   ""   "X"  "Transpose table (rows <-> columns)"      False "")
        (\a ci _ -> tryStk a ci (Transpose.push (stk a)))
  , cmd (mkEntry CmdTblDiff    "S"  "d"  "Diff top two views"                      False "")
        (\a ci _ ->
          if V.null (View.sameHide (View.cur (stk a)))
            then tryStk a ci (Diff.run (stk a))
            else pure (ActOk (resetVS
                    (a & #stk .~ View.setCur (stk a) (Diff.showSame (View.cur (stk a)))))))
    -- info: precision, heatmap, scroll
  , cmd (mkEntry CmdInfoTog    ""   "I"  "Toggle info overlay"                     False "")
        (\a ci _ -> pure (case UIInfo.update (info a) (ciCmd ci) of
                             Just i' -> ActOk (a & #info .~ i')
                             Nothing -> ActUnhandled))
  , cmd (mkEntry CmdPrecDec    ""   ""   "Decrease decimal precision"              False "") (precAdj (-1))
  , cmd (mkEntry CmdPrecInc    ""   ""   "Increase decimal precision"              False "") (precAdj 1)
  , cmd (mkEntry CmdPrecZero   ""   ""   "Set precision to 0 decimals"             False "") (precSet 0)
  , cmd (mkEntry CmdPrecMax    ""   ""   "Set precision to max (17)"               False "") (precSet 17)
  , cmd (mkEntry CmdCellUp     ""   "{"  "Scroll cell preview up"                  False "")
        (\a _ _ -> pure (ActOk (a & #prevScroll .~ (a ^. #prevScroll - min (a ^. #prevScroll) 5))))
  , cmd (mkEntry CmdCellDn     ""   "}"  "Scroll cell preview down"                False "")
        (\a _ _ -> pure (ActOk (a & #prevScroll .~ (a ^. #prevScroll + 5))))
  , cmd (mkEntry CmdHeat0      ""   ""   "Heatmap: off"                            False "")
        (\a _ _ -> pure (ActOk (a & #heatMode .~ 0)))
  , cmd (mkEntry CmdHeat1      ""   ""   "Heatmap: numeric columns"                False "")
        (\a _ _ -> pure (ActOk (a & #heatMode .~ 1)))
  , cmd (mkEntry CmdHeat2      ""   ""   "Heatmap: categorical columns"            False "")
        (\a _ _ -> pure (ActOk (a & #heatMode .~ 2)))
  , cmd (mkEntry CmdHeat3      ""   ""   "Heatmap: all columns"                    False "")
        (\a _ _ -> pure (ActOk (a & #heatMode .~ 3)))
    -- metaV: column metadata view
  , cmd (mkEntry CmdMetaPush      ""  "M"     "Open column metadata view"            True  "")
        (domainH Meta.dispatch)
  , cmd (mkEntry CmdMetaSetKey    "s" "<ret>" "Set selected rows as key columns"     True  "colMeta")
        (domainH Meta.dispatch)
  , cmd (mkEntry CmdMetaSelNull   ""  "0"     "Select columns with null values"      True  "")
        (domainH Meta.dispatch)
  , cmd (mkEntry CmdMetaSelSingle ""  "1"     "Select columns with single value"     True  "")
        (domainH Meta.dispatch)
    -- freq: frequency table
  , cmd (mkEntry CmdFreqOpen   "cg" "F"     "Open frequency view"                   True  "") vuH
  , cmd (mkEntry CmdFreqFilter "r"  "<ret>" "Filter parent table by current row"    True  "freqV") vuH
    -- fld: folder/file browser
  , cmd (mkEntry CmdFolderPush     "r" "D"     "Browse folder"                       True  "")
        (domainH Folder.dispatch)
  , cmd (mkEntry CmdFolderEnter    "r" "<ret>" "Open file or enter directory"        True  "fld")
        (domainH Folder.dispatch)
  , cmd (mkEntry CmdFolderParent   ""  "<bs>"  "Go to parent directory"              True  "fld")
        (domainH Folder.dispatch)
  , cmd (mkEntry CmdFolderDel      "r" ""      "Move to trash"                       True  "")
        (domainH Folder.dispatch)
  , cmd (mkEntry CmdFolderDepthDec ""  ""      "Decrease folder depth"               True  "")
        (domainH Folder.dispatch)
  , cmd (mkEntry CmdFolderDepthInc ""  ""      "Increase folder depth"               True  "")
        (domainH Folder.dispatch)
    -- arg-only commands
  , cmd (mkEntry CmdTblExport "a"  "e"  "Export table (csv/parquet/json/ndjson)"    False "")
        (\a _ arg -> runStackIO a
          (if T.null arg
             then do
               mf <- Export.pickFmt
               case mf of
                 Just f  -> Export.run (stk a) f
                 Nothing -> pure (stk a)
             else Export.runWith (stk a) arg))
  , cmd (mkEntry CmdSessSave "a"  "W"  "Save session"                               False "")
        (\a _ arg -> runStackIO a (do Session.saveWith (stk a) arg; pure (stk a)))
  , cmd (mkEntry CmdSessLoad "a"  ""   "Load session"                               False "")
        (\a _ arg -> runStackIO a (do
          ms <- Session.loadWith arg
          case ms of
            Just stk' -> pure stk'
            Nothing   -> pure (stk a)))
  , cmd (mkEntry CmdTblJoin  "Sa" "J"  "Join tables"                                False "")
        (\a _ arg -> runStackIO a (do
          ms <- Join.runWith (stk a) arg
          case ms of
            Just s' -> pure s'
            Nothing -> pure (stk a)))
    -- theme: fzf picker with live preview via socket
  , cmd (mkEntry CmdThemeOpen ""  ""   "Pick color theme"                           False "")
        (\a _ _ -> do
          ref <- newIORef a
          let render_ :: Vector Word32 -> IO ()
              render_ styles_ = do
                writeIORef Theme.stylesRef styles_
                a' <- readIORef ref
                (vs', v') <- View.doRender (View.cur (stk a')) (vs a') styles_
                               (heatMode a') (sparklines a')
                writeIORef ref (a' { stk = View.setCur (stk a') v', vs = vs' })
                renderTabLine (View.tabNames (stk a')) 0 (Replay.opsStr (View.cur (stk a')))
                when (UIInfo.vis (info a')) $ do
                  h <- Term.height; w <- Term.width
                  UIInfo.render (fromIntegral h) (fromIntegral w)
                    (View.vkind (View.cur (stk a')))
                Term.present
          mt <- Theme.run (theme a) render_
          case mt of
            Just t  -> do
              a' <- readIORef ref
              pure (ActOk (resetVS (a' { theme = t })))
            Nothing -> do
              writeIORef Theme.stylesRef (Theme.styles (theme a))
              a' <- readIORef ref
              pure (ActOk (resetVS (a' { theme = theme a }))))
  , cmd (mkEntry CmdThemePreview "" "" "" False "")
        (\a _ _ -> pure (ActOk a))
  ]

initHandlers :: IO ()
initHandlers = do
  CmdConfig.init (V.map fst commands)
  let m = V.foldl'
            (\acc (e, mfn) -> case mfn of
                Just f  -> HashMap.insert (entryCmd e) f acc
                Nothing -> acc)
            HashMap.empty
            commands
  writeIORef handlerMap m

-- | Alias for ViewKind.ctxStr (used in dispatch)
viewCtxStr :: ViewKind -> Text
viewCtxStr = viewKindCtxStr

-- | Dispatch a handler name string from socket (handler name, optionally with arg after space)
dispatchHandler :: AppState -> Text -> IO AppState
dispatchHandler a cmdStr = do
  Log.write "sock" ("cmd=" <> cmdStr)
  -- Handler names may include arg after space: "row.filter Bid > 100"
  let (h, arg) = case T.splitOn " " cmdStr of
        [single]     -> (single, "")
        (x : rest) -> (x, T.intercalate " " rest)
        []           -> (cmdStr, "")
  mci <- CmdConfig.handlerLookup h
  case mci of
    Nothing -> pure a
    Just ci -> do
      act <- dispatch a ci arg
      case act of
        ActOk a' -> pure (a' { prevScroll = 0 })
        _        -> pure a

-- Status line + per-frame caches; common to both interpreters.
renderBase :: AppState -> IO AppState
renderBase a0 = do
  -- Lazy sparkline computation: recompute when cache is empty
  a <- if V.null (sparklines a0)
         then do
           sp <- Sparkline.compute (View.tbl (stk a0)) 20
           pure (a0 & #sparklines .~ sp)
         else pure a0
  (vs', v') <- View.doRender (View.cur (stk a)) (vs a) (Theme.styles (theme a))
                 (heatMode a) (sparklines a)
  let a' = a & #stk .~ View.setCur (stk a) v' & #vs .~ vs'
  renderTabLine (View.tabNames (stk a')) 0 (Replay.opsStr (View.cur (stk a')))
  -- Column description on status line from DuckDB column comments (cached by path+col)
  let colName = Nav.curColName (View.nav (View.cur (stk a')))
      (cachedPath, cachedCol, _) = statusCache a'
  a'' <- if cachedPath == View.path (View.cur (stk a')) && cachedCol == colName
           then pure a'
           else do
             desc <- Ops.columnComment (View.path (View.cur (stk a'))) colName
             pure (a' & #statusCache .~
                          (View.path (View.cur (stk a')), colName, desc))
  let (_, _, desc) = statusCache a''
  when (not (T.null desc)) $ do
    ht <- Term.height
    w  <- Term.width
    let label0 = colName <> ": " <> desc
        maxLen = fromIntegral w * 2 `div` 3
        label  = if T.length label0 > maxLen
                   then T.take maxLen label0 <> "…"
                   else label0
    Term.print 0 (ht - 1)
      (Theme.styleFg (Theme.styles (theme a'')) Theme.sStatus)
      (Theme.styleBg (Theme.styles (theme a'')) Theme.sStatus)
      label
  agg' <- StatusAgg.update (aggCache a'') (View.tbl (stk a''))
            (View.path (View.cur (stk a'')))
            (Nav.curColIdx (View.nav (View.cur (stk a''))))
  let a''' = a'' { aggCache = agg' }
  when (UIInfo.vis (info a''')) $ do
    h <- Term.height; w <- Term.width
    UIInfo.render (fromIntegral h) (fromIntegral w) (View.vkind (View.cur (stk a''')))
  pure a'''

-- Full frame render. `showPreview` toggles the truncated-cell overlay; prod
-- shows it, test skips it (tests don't exercise the preview box).
renderFrame :: Bool -> AppState -> IO AppState
renderFrame showPreview a0 = do
  a <- renderBase a0
  when showPreview $ do
    h <- Term.height
    w <- Term.width
    let nav_ = View.nav (View.cur (stk a))
    cellText <- TblOps.cellStr (Nav.tbl nav_) (Nav.cur (Nav.row nav_)) (Nav.curColIdx nav_)
    let colW = min (maybe 10 id (View.widths (View.cur (stk a)) V.!? Nav.cur (Nav.col nav_))) 50
    when (T.length cellText + 2 > colW) $
      UIPreview.render (fromIntegral h) (fromIntegral w) cellText (prevScroll a)
  Term.present
  pure a

-- Loop program expressed over AppM. Free of any test/prod conditionals —
-- the interpreter decides render flavor, key source, and arg collection.
loopProg :: AppState -> AppM AppState AppState
loopProg a0 = do
  a <- AppM.doRender a0
  mkey <- AppM.poll
  case mkey of
    Nothing  -> pure a
    Just key -> do
      -- Socket commands from external tools (handler names)
      a' <- do
        mcmd <- liftIO_ Socket.pollCmd
        case mcmd of
          Just cmdStr -> liftIO_ (dispatchHandler a cmdStr)
          Nothing     -> pure a
      -- <wait>: sleep so external socat → socket → pollCmd can land mid-run.
      -- Only used by socket-dispatch tests that race an out-of-process sender
      -- against the -c keystroke stream; normal synchronous tests don't need it.
      if key == "<wait>"
        then do liftIO_ (threadDelayMs 50); loopProg a'
        else if T.null key
          then loopProg a'
          else do
            let vkStr = viewCtxStr (View.vkind (View.cur (stk a')))
            mci <- liftIO_ (CmdConfig.keyLookup key vkStr)
            case mci of
              Just ci -> do
                isArg <- liftIO_ (CmdConfig.isArgCmd (ciCmd ci))
                arg <- if isArg then AppM.readArg' else pure ""
                act <- liftIO_ (dispatch a' ci arg)
                case act of
                  ActQuit      -> pure a'
                  ActUnhandled -> loopProg a'
                  ActOk a''0   -> do
                    -- Reset cell-preview scroll offset after every command except the
                    -- two that explicitly scroll the preview ({, }): moving to a new
                    -- cell should start the overlay at the top, but actively scrolling
                    -- within it must not reset mid-gesture. Handled here rather than
                    -- in dispatch so the generic dispatcher stays agnostic of UI state.
                    let a'' = if ciCmd ci == CmdCellUp || ciCmd ci == CmdCellDn
                                then a''0
                                else a''0 { prevScroll = 0 }
                    loopProg a''
              Nothing -> loopProg a'
  where
    liftIO_ :: IO a -> AppM AppState a
    liftIO_ io = AppM.LiftIO io AppM.Pure

-- | Sleep helper (avoids pulling Control.Concurrent at top of file)
threadDelayMs :: Int -> IO ()
threadDelayMs ms = do
  -- IO.sleep in Lean uses milliseconds; Haskell threadDelay takes microseconds
  Control.Concurrent.threadDelay (ms * 1000)

-- Production interpreter: real Term.pollEvent; arg commands open fzf from inside their handlers.
prodInterp :: Interp AppState
prodInterp = Interp
  { render  = renderFrame True
  , nextKey = do e <- Term.pollEvent; pure (Just (Key.evToKey e))
  , readArg = pure ""
  }

-- Test interpreter. Used by the `-c "keys"` test harness: tests spawn `tv`,
-- feed it a canned keystroke string, then read the rendered terminal buffer
-- from stdout to assert on what the UI would have shown.
--
-- The interpreter lives in an `IORef (Vector Text)` — a mutable reference
-- holding the unconsumed suffix of the keystroke queue. Each call to
-- `nextKey`/`readArg` reads the current queue, decides what to return, and
-- writes the remaining tokens back. This is the only piece of test state;
-- rendering is real (writes to the termbox fake-buffer) and command handlers
-- run in real IO (real DuckDB queries, real file ops, etc.). The test/prod
-- difference is *just* where keystrokes come from and what happens when the
-- queue runs out — everything else is identical to production.
testInterp :: IORef (Vector Text) -> Interp AppState
testInterp ref = Interp
  { -- Tests don't exercise the truncated-cell preview overlay → skip it to
    -- keep the asserted buffer predictable across terminal sizes.
    render  = renderFrame False
    -- Poll for the next keystroke. In production this blocks on the terminal;
    -- here we pop from the queue instead. Empty queue means "the test is
    -- finished": print the current termbox buffer so the parent `-c` process
    -- can capture it from stdout, then return `Nothing` which tells `loopProg`
    -- to exit cleanly (see the `Nothing -> pure a` branch above).
  , nextKey = do
      ks <- readIORef ref
      if V.null ks
        then do
          buf <- Term.bufferStr
          Prelude.putStr (T.unpack buf)
          pure Nothing
        else do
          (key, ks') <- Key.nextKey ks
          writeIORef ref ks'
          pure (Just key)
    -- Arg commands (`/`, `\`, `=`, etc.) prompt the user for a string. In
    -- production the handler opens an fzf popup; in tests we splice the arg
    -- directly into the keystroke stream terminated by a `<ret>` token, e.g.
    -- `"/foo<ret>"` means "press `/`, then type `foo`, then Enter". Here we
    -- scan forward to the next `<ret>`, concatenate the tokens before it into
    -- the arg string, and drop everything up to and including the `<ret>` so
    -- the outer loop picks up after the Enter. If there is no `<ret>` in the
    -- queue we return "" (same as prod when the user cancels fzf) so the
    -- handler takes its "empty arg" branch.
  , readArg = do
      ks <- readIORef ref
      if V.any (== "<ret>") ks
        then do
          let idx = maybe (V.length ks) id (V.findIndex (== "<ret>") ks)
          writeIORef ref (V.drop (idx + 1) ks)
          pure (T.concat (V.toList (V.take idx ks)))
        else pure ""
  }

-- Main loop entry point: picks the interpreter and runs the free-monad program.
mainLoop :: AppState -> Bool -> Vector Text -> IO AppState
mainLoop a test ks =
  if test
    then do
      ref <- newIORef ks
      AppM.run (testInterp ref) (loopProg a)
    else
      AppM.run prodInterp (loopProg a)

