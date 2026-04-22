-- App/Common: command collection, dispatch, render, main loop.
-- Types and combinators live in App.Types; domain commands in their feature modules.
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.App.Common where

import Tv.Prelude
import qualified Control.Concurrent
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified Tv.AppF as AppM
import Tv.AppF (AppM, Interp(..))
import Tv.App.Types
import Tv.App.FeatureCommands (featureCommands)
import qualified Tv.Clip as Clip
import qualified Tv.CmdConfig as CmdConfig
import Tv.CmdConfig (Entry(..), CmdInfo(..), mkEntry, hdl)
import qualified Tv.Filter as Filter
import qualified Tv.Folder as Folder
import qualified Tv.Freq as Freq
import qualified Tv.Fzf as Fzf
import qualified Tv.Key as Key
import qualified Tv.Nav as Nav
import qualified Tv.Render as Render
import Tv.Render (tabLine)
import qualified Tv.Sparkline as Sparkline
import qualified Tv.Ops as Ops
import qualified Tv.StatusAgg as StatusAgg
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import Tv.Types (Cmd(..), ColCache(..), ViewKind(..), toString, noEffect)
import qualified Tv.UI.Info as UIInfo
import qualified Tv.UI.Preview as UIPreview
import qualified Tv.Log as Log
import qualified Tv.Socket as Socket
import qualified Tv.Data.DuckDB.Ops as Ops
import Tv.Data.DuckDB.Table (AdbcTable)
import Tv.View (ViewStack(..))
import qualified Tv.View as View

-- Dispatch --

pureDispatch :: AppState -> CmdInfo -> Maybe AppState
pureDispatch a ci
  | ci ^. #ciCmd == CmdStkDup || ci ^. #ciCmd == CmdStkPop || ci ^. #ciCmd == CmdStkSwap =
      fmap (\(s', _) -> withStk a ci s') (View.updateStack (a ^. #stk) (ci ^. #ciCmd))
  | otherwise =
      case View.update (View.cur (a ^. #stk)) (ci ^. #ciCmd) 20 of
        Just (v', e)
          | noEffect e -> Just (withStk a ci (View.setCur (a ^. #stk) v'))
        _ -> Nothing

dispatch :: AppState -> CmdInfo -> Text -> IO Action
dispatch a ci arg =
  case HashMap.lookup (ci ^. #ciCmd) (a ^. #handlers) of
    Just f  -> f a ci arg
    Nothing -> viewUp a ci

runMenu :: AppState -> IO Action
runMenu a = do
  ref <- newIORef a
  let poll :: IO ()
      poll = do
        mcmd <- Socket.pollCmd
        case mcmd of
          Just cmdStr -> do
            Log.write "sock" ("poll cmd=" <> cmdStr)
            case CmdConfig.handlerLookup (a ^. #cmdCache) cmdStr of
              Just ci -> do
                a0 <- readIORef ref
                case pureDispatch a0 ci of
                  Just a' -> do
                    (vs', v') <- View.doRender (View.cur (a' ^. #stk))
                                   (a' ^. #vs) ((a' ^. #theme) ^. #styles)
                                   (a' ^. #heatMode) (a' ^. #sparklines)
                    writeIORef ref (a' & #stk .~ View.setCur (a' ^. #stk) v' & #vs .~ vs')
                    Term.present
                  Nothing -> pure ()
              Nothing -> pure ()
          Nothing -> pure ()
  handler <- Fzf.cmdMode (a ^. #testMode) (a ^. #cmdCache) (View.cur (a ^. #stk) ^. #vkind) poll
  a' <- readIORef ref
  _ <- Socket.pollCmd  -- drain stale command from fzf focus
  case handler of
    Just h -> do
      case CmdConfig.handlerLookup (a' ^. #cmdCache) h of
        Just ci -> dispatch a' ci ""
        Nothing -> pure (ActOk a')
    Nothing -> pure (ActOk a')

-- | Truncate clipboard status message to fit on the status line without
-- wrapping; show head + ellipsis if the cell's too long.
clipMsg :: Text -> Text
clipMsg t
  | T.length t <= 40 = t
  | otherwise       = T.take 37 t <> "..."

-- Command table: feature-module commands + local wiring --

commands :: Vector (Entry, Maybe HandlerFn)
commands = featureCommands <> localCmds

-- | Commit a rendered frame (used by live-preview closures after a state mutation).
--   Reads current AppState from ref, re-renders with the supplied styles, writes
--   updated state back, and paints the tab line + info overlay + present.
renderSnap :: IORef AppState -> ViewStack AdbcTable -> V.Vector Word32 -> IO ()
renderSnap ref stk' styles_ = do
  a <- readIORef ref
  (vs', v') <- View.doRender (View.cur stk') (a ^. #vs) styles_
                 (a ^. #heatMode) (a ^. #sparklines)
  writeIORef ref (a & #stk .~ View.setCur stk' v' & #vs .~ vs')
  tabLine (View.tabNames stk') 0 (View.opsStr (View.cur stk'))
  when (a ^. #info) $ do
    h <- Term.height; w <- Term.width
    UIInfo.render (fromIntegral h) (fromIntegral w) (View.cur stk' ^. #vkind)
  Term.present

freqH :: HandlerFn
freqH = \a ci _ -> case Freq.update (a ^. #stk) (ci ^. #ciCmd) of
  Just (s', e) -> runViewEffect (withStk a ci s') ci (View.cur s') e
  Nothing      -> viewUp a ci

localCmds :: Vector (Entry, Maybe HandlerFn)
localCmds = V.fromList
  [ hdl (mkEntry CmdRowSearch "ca" "/"  "Search for value in current column" True "")
        (\a _ arg -> do
          if not (T.null arg)
            then stackIO a (Filter.searchWith (a ^. #stk) arg)
            else do
              ref <- newIORef a
              let preview :: ViewStack AdbcTable -> IO ()
                  preview stk' = do
                    a' <- readIORef ref
                    renderSnap ref stk' ((a' ^. #theme) ^. #styles)
              stk' <- Filter.rowSearchLive (a ^. #testMode) (a ^. #stk) preview
              a' <- readIORef ref
              pure $ ActOk $ resetVS (a' & #stk .~ stk'))
  , hdl (mkEntry CmdTblMenu   ""  " "  "Open command menu"                  False "") (\a _ _ -> runMenu a)
  , hdl (mkEntry CmdStkSwap   "S" "S"  "Swap top two views"                 False "") stkH
  , hdl (mkEntry CmdStkPop    ""  "q"  "Close current view"                 True  "") stkH
  , hdl (mkEntry CmdStkDup    ""  ""   "Duplicate current view"             False "") stkH
  , hdl (mkEntry CmdTblQuit   ""  ""   ""                                   False "") (\_ _ _ -> pure ActQuit)
  , hdl (mkEntry CmdInfoTog   ""  "I"  "Toggle info overlay"                False "")
        (\a ci _ -> pure $ case UIInfo.update (a ^. #info) (ci ^. #ciCmd) of
                             Just i' -> ActOk (a & #info .~ i')
                             Nothing -> ActUnhandled)
  , hdl (mkEntry CmdPrecDec   ""  ""   "Decrease decimal precision"         False "") (precAdj (-1))
  , hdl (mkEntry CmdPrecInc   ""  ""   "Increase decimal precision"         False "") (precAdj 1)
  , hdl (mkEntry CmdPrecZero  ""  ""   "Set precision to 0 decimals"        False "") (precSet 0)
  , hdl (mkEntry CmdPrecMax   ""  ""   "Set precision to max (17)"          False "") (precSet 17)
  , hdl (mkEntry CmdCellUp    ""  "{"  "Scroll cell preview up"             False "")
        (\a _ _ -> pure $ ActOk $ a & #prevScroll %~ (\p -> p - min p 5))
  , hdl (mkEntry CmdCellDn    ""  "}"  "Scroll cell preview down"           False "")
        (\a _ _ -> pure $ ActOk $ a & #prevScroll %~ (+ 5))
  , hdl (mkEntry CmdCellYank  "rc" "y"  "Copy current cell to clipboard (OSC 52)" False "")
        (\a _ _ -> do
          let nav_ = View.cur (a ^. #stk) ^. #nav
          txt <- Ops.cellStr (nav_ ^. #tbl) (nav_ ^. #row % #cur) (Nav.colIdx nav_)
          Clip.copy txt
          Render.statusMsg ("copied: " <> clipMsg txt)
          pure (ActOk a))
  , hdl (mkEntry CmdHeat0     ""  ""   "Heatmap: off"                       False "") (heatSet 0)
  , hdl (mkEntry CmdHeat1     ""  ""   "Heatmap: numeric columns"           False "") (heatSet 1)
  , hdl (mkEntry CmdHeat2     ""  ""   "Heatmap: categorical columns"       False "") (heatSet 2)
  , hdl (mkEntry CmdHeat3     ""  ""   "Heatmap: all columns"               False "") (heatSet 3)
  , hdl (mkEntry CmdFreqOpen   "cg" "F"     "Open frequency view"                True  "")    freqH
  , hdl (mkEntry CmdFreqFilter "r"  "<ret>" "Filter parent table by current row" True "freqV") freqH
  , hdl (mkEntry CmdThemeOpen ""  ""   "Pick color theme"                    False "")
        (\a _ _ -> do
          ref <- newIORef a
          let render_ :: Vector Word32 -> IO ()
              render_ styles_ = do
                writeIORef Theme.stylesRef styles_
                a' <- readIORef ref
                renderSnap ref (a' ^. #stk) styles_
          mt <- Theme.run (a ^. #testMode) (a ^. #theme) render_
          case mt of
            Just t  -> do
              a' <- readIORef ref
              pure $ ActOk $ resetVS (a' & #theme .~ t)
            Nothing -> do
              writeIORef Theme.stylesRef ((a ^. #theme) ^. #styles)
              a' <- readIORef ref
              pure $ ActOk $ resetVS (a' & #theme .~ (a ^. #theme)))
  , hdl (mkEntry CmdThemePreview "" "" "" False "") (\a _ _ -> pure (ActOk a))
  ]

initHandlers :: (CmdConfig.CmdCache, HashMap.HashMap Cmd HandlerFn)
initHandlers =
  let cc = CmdConfig.buildCache (V.map fst commands)
      m  = V.foldl' (\acc (e, mfn) -> case mfn of
                Just f  -> HashMap.insert (e ^. #cmd) f acc
                Nothing -> acc)
            HashMap.empty commands
  in (cc, m)

-- Misc --

ctxStr :: ViewKind -> Text
ctxStr = toString

dispatchHandler :: AppState -> Text -> IO AppState
dispatchHandler a cmdStr = do
  Log.write "sock" ("cmd=" <> cmdStr)
  let (h, arg) = case T.words cmdStr of
        []         -> (cmdStr, "")
        [single]   -> (single, "")
        (x : rest) -> (x, T.unwords rest)
  case CmdConfig.handlerLookup (a ^. #cmdCache) h of
    Nothing -> do
      Log.write "cmd" ("unknown command: " <> h)
      pure a
    Just ci -> do
      act <- dispatch a ci arg
      case act of
        ActOk a' -> pure (a' & #prevScroll .~ 0)
        _        -> pure a

-- Render --

renderBase :: AppState -> IO AppState
renderBase a0 = do
  a <- if V.null (a0 ^. #sparklines)
         then do
           sp <- Sparkline.compute (View.tbl (a0 ^. #stk)) 20
           pure (a0 & #sparklines .~ sp)
         else pure a0
  (vs', v') <- View.doRender (View.cur (a ^. #stk)) (a ^. #vs) ((a ^. #theme) ^. #styles)
                 (a ^. #heatMode) (a ^. #sparklines)
  let a' = a & #stk .~ View.setCur (a ^. #stk) v' & #vs .~ vs'
  tabLine (View.tabNames (a' ^. #stk)) 0 (View.opsStr (View.cur (a' ^. #stk)))
  let colName = Nav.colName (View.cur (a' ^. #stk) ^. #nav)
      ColCache{cachedPath, cachedCol} = a' ^. #statusCache
  a'' <- if cachedPath == View.cur (a' ^. #stk) ^. #path && cachedCol == colName
           then pure a'
           else do
             desc <- Ops.columnComment (View.cur (a' ^. #stk) ^. #path) colName
             pure (a' & #statusCache .~
                          ColCache (View.cur (a' ^. #stk) ^. #path) colName desc)
  let ColCache{cachedVal = desc} = a'' ^. #statusCache
  unless (T.null desc) $ do
    ht <- Term.height
    w  <- Term.width
    let label0 = colName <> ": " <> desc
        maxLen = fromIntegral w * 2 `div` 3
        label  = if T.length label0 > maxLen
                   then T.take maxLen label0 <> "…"
                   else label0
    Term.print 0 (ht - 1)
      (Theme.styleFg ((a'' ^. #theme) ^. #styles) Theme.sStatus)
      (Theme.styleBg ((a'' ^. #theme) ^. #styles) Theme.sStatus)
      label
  agg' <- StatusAgg.update (a'' ^. #aggCache) (View.tbl (a'' ^. #stk))
            (View.cur (a'' ^. #stk) ^. #path)
            (Nav.colIdx (View.cur (a'' ^. #stk) ^. #nav))
  let a''' = a'' & #aggCache .~ agg'
  when (a''' ^. #info) $ do
    h <- Term.height; w <- Term.width
    UIInfo.render (fromIntegral h) (fromIntegral w) (View.cur (a''' ^. #stk) ^. #vkind)
  pure a'''

renderFrame :: Bool -> AppState -> IO AppState
renderFrame showPreview a0 = do
  a <- renderBase a0
  when showPreview $ do
    h <- Term.height
    w <- Term.width
    let nav_ = View.cur (a ^. #stk) ^. #nav
    cellText <- Ops.cellStr (nav_ ^. #tbl) (nav_ ^. #row % #cur) (Nav.colIdx nav_)
    let colW = min (fromMaybe 10 ((View.cur (a ^. #stk) ^. #widths) V.!? (nav_ ^. #col % #cur))) 50
    when (T.length cellText + 2 > colW) $
      UIPreview.render (fromIntegral h) (fromIntegral w) cellText (a ^. #prevScroll)
  Term.present
  pure a

-- Main loop --

loopProg :: AppState -> AppM AppState AppState
loopProg a0 = do
  a <- AppM.doRender a0
  mkey <- AppM.poll
  case mkey of
    Nothing  -> pure a
    Just key -> do
      a' <- do
        mcmd <- liftIO_ Socket.pollCmd
        case mcmd of
          Just cmdStr -> liftIO_ (dispatchHandler a cmdStr)
          Nothing     -> pure a
      if key == "<wait>"
        then do liftIO_ (delayMs 50); loopProg a'
        else if T.null key
          then loopProg a'
          else do
            let vkStr = ctxStr (View.cur (a' ^. #stk) ^. #vkind)
            case CmdConfig.keyLookup (a' ^. #cmdCache) key vkStr of
              Just ci -> do
                let isArg = CmdConfig.isArgCmd (a' ^. #cmdCache) (ci ^. #ciCmd)
                arg <- if isArg then AppM.readArg' else pure ""
                act <- liftIO_ (dispatch a' ci arg)
                case act of
                  ActQuit      -> pure a'
                  ActUnhandled -> loopProg a'
                  ActOk a''0   -> do
                    let a'' = if ci ^. #ciCmd == CmdCellUp || ci ^. #ciCmd == CmdCellDn
                                then a''0
                                else a''0 & #prevScroll .~ 0
                    loopProg a''
              Nothing -> loopProg a'
  where
    liftIO_ :: IO a -> AppM AppState a
    liftIO_ io = AppM.LiftIO io AppM.Pure

delayMs :: Int -> IO ()
delayMs ms = Control.Concurrent.threadDelay (ms * 1000)

prodInterp :: Interp AppState
prodInterp = Interp
  { render  = renderFrame True
  , nextKey = do e <- Term.pollEvent; pure $ Just $ Key.toKey e
  , readArg = pure ""
  }

testInterp :: IORef (Vector Text) -> Interp AppState
testInterp ref = Interp
  { render  = renderFrame False
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
  , readArg = do
      ks <- readIORef ref
      if V.any (== "<ret>") ks
        then do
          let idx = fromMaybe (V.length ks) (V.findIndex (== "<ret>") ks)
          writeIORef ref (V.drop (idx + 1) ks)
          pure (T.concat (V.toList (V.take idx ks)))
        else pure ""
  }

mainLoop :: AppState -> Bool -> Vector Text -> IO AppState
mainLoop a test ks =
  if test
    then do
      ref <- newIORef ks
      AppM.run (testInterp ref) (loopProg a)
    else
      AppM.run prodInterp (loopProg a)
