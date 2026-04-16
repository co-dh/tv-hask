{-
  Filter: fzf-based column/row filtering and search.
  dispatch returns IO action directly; no intermediate Effect.

  Literal port of Tc/Tc/Filter.lean. Namespaces Tc.ViewStack and Tc.Filter
  both land in this module.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Filter
  ( -- Tc.ViewStack namespace
    colSearch
  , rowSearch
  , rowSearchLive
  , searchDir
  , rowFilter
  , jumpCol
  , filterWith
  , searchWith
    -- Tc.Filter namespace
  , commands
  , dispatch
  ) where

import Data.IORef (IORef, newIORef, readIORef, writeIORef, modifyIORef)
import qualified Data.HashMap.Strict as HM
import Data.HashMap.Strict (HashMap)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.List as L
import Data.Maybe (fromMaybe)
import Text.Read (readMaybe)

import Optics.Core ((%), (&), (.~), (^.), over)
import Tv.App.Types (AppState(..), HandlerFn, domainH', stackIO)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import Tv.Nav (NavState, rowCur, colCur, finClamp)
import qualified Tv.Nav as Nav
import Tv.Types (Cmd(..), ColType, isNumeric, typeStr, filterPrql, filterPrompt)
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)
import Tv.View (View(..), ViewStack, cur, setCur, push, tbl)
import qualified Tv.View as View
import qualified Tv.Fzf as Fzf
import qualified Tv.Util as Util

-- | Move row cursor to target index (pure helper)
moveRowTo
  :: ViewStack AdbcTable -> Int -> Maybe (Int, Text) -> ViewStack AdbcTable
moveRowTo s rowIdx search_ =
  let v      = cur s
      n      = v ^. #nav
      nRows_ = Table.nRows (n ^. #tbl)
      delta  = rowIdx - n ^. #row % #cur
      nav'   = over rowCur (\f -> finClamp nRows_ f delta) n
  in setCur s (v & #nav .~ nav'
                 & #search .~ maybe (v ^. #search) Just search_)

-- | Move col cursor to target index (pure helper)
moveTo :: ViewStack AdbcTable -> Int -> ViewStack AdbcTable
moveTo s colIdx =
  let v      = cur s
      n      = v ^. #nav
      nCols_ = V.length (Table.colNames (n ^. #tbl))
      delta  = colIdx - n ^. #col % #cur
      nav'   = over colCur (\f -> finClamp nCols_ f delta) n
  in setCur s (v & #nav .~ nav')

-- | col search: fzf jump to column by name (IO version for backward compat)
colSearch :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
colSearch tm s = do
  mi <- Fzf.fzfIdx tm (V.fromList ["--prompt=Column: "]) (Nav.dispNames (nav (cur s)))
  case mi of
    Nothing  -> pure s
    Just idx -> pure (moveTo s idx)

-- | Shared: resolve current column, fetch sorted distinct values
withDistinct
  :: ViewStack AdbcTable
  -> (Int -> Text -> Vector Text -> IO (ViewStack AdbcTable))
  -> IO (ViewStack AdbcTable)
withDistinct s f = do
  let v       = cur s
      curCol  = Nav.colIdx (v ^. #nav)
      curName = Nav.colName (v ^. #nav)
  vals <- Table.distinct (v ^. #nav % #tbl) curCol
  let sorted = V.fromList (L.sort (V.toList vals))
  f curCol curName sorted

-- | row search (/): find value in current column, jump to matching row (IO)
rowSearch :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
rowSearch tm s = withDistinct s $ \curCol curName vals -> do
  mr <- Fzf.fzf tm (V.fromList ["--prompt=/" <> curName <> ": "])
                (T.intercalate "\n" (V.toList vals))
  case mr of
    Nothing     -> pure s
    Just result -> do
      let start = cur s ^. #nav % #row % #cur + 1
      mri <- Table.findRow (tbl s) curCol result start True
      case mri of
        Nothing     -> pure s
        Just rowIdx -> pure (moveRowTo s rowIdx (Just (curCol, result)))

-- | findRow with cache: fzf fires focus on every arrow key, so without caching
-- each fires a SQL query causing visible lag.
cachedFindRow
  :: IORef (HashMap Int (Maybe Int))
  -> AdbcTable -> Int -> Int -> Text -> IO (Maybe Int)
cachedFindRow cache tbl_ col idx val = do
  m <- readIORef cache
  case HM.lookup idx m of
    Just hit -> pure hit
    Nothing  -> do
      r <- Table.findRow tbl_ col val 0 True
      modifyIORef cache (HM.insert idx r)
      pure r

-- | Build poll callback for live search preview.
-- On each fzf focus change, finds the matching row (cached) and re-renders.
searchPoll
  :: AdbcTable -> Int -> Vector Text
  -> IORef (ViewStack AdbcTable) -> (ViewStack AdbcTable -> IO ())
  -> IO (IORef (HashMap Int (Maybe Int)), IO ())
searchPoll tbl_ curCol vals sRef preview = do
  lastIdx <- newIORef (Nothing :: Maybe Int)
  cache   <- newIORef (HM.empty :: HashMap Int (Maybe Int))
  let poll :: IO ()
      poll = do
        mc <- Util.pollCmd
        case mc of
          Nothing -> pure ()
          Just cmdStr -> do
            let parts = T.splitOn " " cmdStr
            if Util.headD "" parts /= "search.preview"
              then pure ()
              else do
                let p1 = case drop 1 parts of { (x:_) -> x; _ -> "" }
                case readMaybe (T.unpack p1) :: Maybe Int of
                  Nothing  -> pure ()
                  Just idx -> do
                    li <- readIORef lastIdx
                    if idx >= V.length vals || li == Just idx
                      then pure ()
                      else do
                        writeIORef lastIdx (Just idx)
                        let val = fromMaybe "" (vals V.!? idx)
                        mri <- cachedFindRow cache tbl_ curCol idx val
                        case mri of
                          Nothing     -> pure ()
                          Just rowIdx -> do
                            s0 <- readIORef sRef
                            let s' = moveRowTo s0 rowIdx (Just (curCol, val))
                            writeIORef sRef s'
                            preview s'
  pure (cache, poll)

-- | Row search with live preview: cursor moves as user browses fzf results.
-- fzf focus -> shell script -> socat -> socket -> poll -> findRow -> re-render.
rowSearchLive
  :: Bool -> ViewStack AdbcTable -> (ViewStack AdbcTable -> IO ()) -> IO (ViewStack AdbcTable)
rowSearchLive tm s preview = withDistinct s $ \curCol curName vals -> do
  if tm
    then do
      let result = fromMaybe "" (vals V.!? 0)
      if T.null result
        then pure s
        else do
          mri <- Table.findRow (tbl s) curCol result 0 True
          case mri of
            Nothing     -> pure s
            Just rowIdx -> pure (moveRowTo s rowIdx (Just (curCol, result)))
    else do
      -- setup: socat script for fzf execute-silent, indexed items for shell-safe args
      sockPath <- Util.getPath
      script   <- Util.tmpPath "search-preview.sh"
      TIO.writeFile script
        (T.pack ("#!/bin/sh\necho \"search.preview $1\" | socat - UNIX-CONNECT:" ++ sockPath))
      let items = V.imap (\i v -> T.pack (show i) <> "\t" <> v) vals
      sRef <- newIORef s
      (cache, poll) <- searchPoll (tbl s) curCol vals sRef preview
      -- run fzf with live preview polling
      let opts = V.fromList
            [ "--prompt=/" <> curName <> ": "
            , "--with-nth=2..", "--delimiter=\t"
            , T.pack ("--bind=focus:execute-silent(sh " ++ script ++ " {1})")
            ]
      out <- Fzf.fzfCore False opts (T.intercalate "\n" (V.toList items)) poll
      if T.null out
        then readIORef sRef  -- cancelled: keep preview position
        else do
          -- apply final selection (reuse cache from preview)
          let parts = T.splitOn "\t" out
              hd = case parts of { (x:_) -> x; _ -> "" }
          case readMaybe (T.unpack hd) :: Maybe Int of
            Nothing  -> pure s
            Just idx -> do
              let result = fromMaybe "" (vals V.!? idx)
              mri <- cachedFindRow cache (tbl s) curCol idx result
              case mri of
                Nothing     -> pure s
                Just rowIdx -> pure (moveRowTo s rowIdx (Just (curCol, result)))

-- | search in direction: fwd=true (n), bwd=false (N)
searchDir :: ViewStack AdbcTable -> Bool -> IO (ViewStack AdbcTable)
searchDir s fwd = do
  let v = cur s
  case v ^. #search of
    Nothing         -> pure s
    Just (col, val) -> do
      let rowCur = v ^. #nav % #row % #cur
          start  = if fwd then rowCur + 1 else rowCur
      mri <- Table.findRow (v ^. #nav % #tbl) col val start fwd
      case mri of
        Nothing     -> pure s
        Just rowIdx -> pure (moveRowTo s rowIdx Nothing)

-- | row filter (\): filter rows by expression, push filtered view (IO)
rowFilter :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
rowFilter tm s = withDistinct s $ \_curCol curName vals -> do
  let typ    = Ops.colType (tbl s) _curCol
      typStr = typeStr typ
      header = filterPrompt curName typStr
  mr <- Fzf.fzf tm
          (V.fromList ["--print-query", "--header=" <> header, "--prompt=filter > "])
          (T.intercalate "\n" (V.toList vals))
  case mr of
    Nothing     -> pure s
    Just result -> do
      let expr = filterPrql curName vals result (isNumeric typ)
      if T.null expr
        then pure s
        else do
          mt <- Table.filter (tbl s) expr
          case mt of
            Nothing   -> pure s
            Just tbl' ->
              -- Preserve cursor column across filter (matches Lean's
              -- `rebuild tbl' (row := 0)` default). Resetting col=0 would
              -- snap the cursor back to the first column.
              let v      = cur s
                  curCol = Nav.colIdx (v ^. #nav)
                  grp'   = v ^. #nav % #grp
              in case View.rebuild v tbl' curCol grp' 0 of
                   Nothing -> pure s
                   Just v' -> pure (push s (v' & #disp .~ ("\\" <> curName)))

-- | Jump to column by name directly (no fzf). Called by socket/dispatch.
jumpCol :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
jumpCol s name = do
  if T.null name
    then pure s
    else case V.findIndex (== name) (Nav.dispNames (cur s ^. #nav)) of
      Just idx -> pure (moveTo s idx)
      Nothing  -> pure s

-- | Filter by expression directly (no fzf). Called by socket/dispatch.
filterWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
filterWith s expr = do
  if T.null expr
    then pure s
    else do
      mt <- Table.filter (tbl s) expr
      case mt of
        Nothing   -> pure s
        Just tbl' ->
          -- Match Lean's `rebuild tbl' (row := 0)` — col defaults to the
          -- old cursor column, grp to the old grp. Resetting col to 0
          -- would jump the cursor home, which the filter demo explicitly
          -- asserts should NOT happen (cursor stays on the filtered col).
          let v      = cur s
              curCol = Nav.colIdx (v ^. #nav)
              grp'   = v ^. #nav % #grp
          in case View.rebuild v tbl' curCol grp' 0 of
               Nothing -> pure s
               Just v' -> pure (push s (v' & #disp .~ ("\\" <> expr)))

-- | Search for value directly (no fzf). Called by socket/dispatch.
searchWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
searchWith s val = do
  if T.null val
    then pure s
    else do
      let v      = cur s
          curCol = Nav.colIdx (v ^. #nav)
          start  = v ^. #nav % #row % #cur + 1
      mri <- Table.findRow (tbl s) curCol val start True
      case mri of
        Nothing     -> pure s
        Just rowIdx -> pure (moveRowTo s rowIdx (Just (curCol, val)))

-- | Dispatch filter handler to IO action. Returns none if handler not recognized.
dispatch
  :: Bool -> ViewStack AdbcTable
  -> Cmd
  -> Maybe (IO (ViewStack AdbcTable))
dispatch tm s h = case h of
  CmdColSearch     -> Just (colSearch tm s)
  CmdRowFilter     -> Just (rowFilter tm s)
  CmdRowSearchNext -> Just (searchDir s True)
  CmdRowSearchPrev -> Just (searchDir s False)
  _                -> Nothing

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdRowFilter     "a"  "\\" "Filter rows by PRQL expression"    True  "")
        (\a _ arg -> stackIO a (if T.null arg then rowFilter (testMode a) (stk a) else filterWith (stk a) arg))
  , hdl (mkEntry CmdRowSearchNext "rc" "n"  "Jump to next search match"         False "") (domainH' (dispatch False))
  , hdl (mkEntry CmdRowSearchPrev "rc" "N"  "Jump to previous search match"     False "") (domainH' (dispatch False))
  , hdl (mkEntry CmdColSearch     "a"  "g"  "Jump to column by name"            True  "")
        (\a _ arg -> stackIO a (if T.null arg then colSearch (testMode a) (stk a) else jumpCol (stk a) arg))
  ]
