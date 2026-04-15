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
  , colJumpWith
  , filterWith
  , searchWith
    -- Tc.Filter namespace
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
import Text.Read (readMaybe)

import Optics.Core ((%), (&), (.~), (^.), over)
import Tv.Nav (NavState, rowCurL, colCurL, finClamp)
import qualified Tv.Nav as Nav
import Tv.Types (Cmd(..), TblOps, ColType, colTypeIsNumeric)
import qualified Tv.Types as TblOps
import Tv.View (View(..), ViewStack, cur, setCur, push, tbl)
import qualified Tv.View as View
import qualified Tv.StrEnum as StrEnum
import qualified Tv.Fzf as Fzf
import qualified Tv.Util as Util
import Tv.Data.ADBC.Table (AdbcTable)
import Tv.Data.ADBC.Ops ()  -- TblOps instance for AdbcTable

-- | Move row cursor to target index (pure helper)
moveRowTo
  :: TblOps t
  => ViewStack t -> Int -> Maybe (Int, Text) -> ViewStack t
moveRowTo s rowIdx search_ =
  let v      = cur s
      n      = v ^. #nav
      nRows_ = TblOps.nRows (n ^. #tbl)
      delta  = rowIdx - n ^. #row % #cur
      nav'   = over rowCurL (\f -> finClamp nRows_ f delta) n
  in setCur s (v & #nav .~ nav'
                 & #search .~ maybe (v ^. #search) Just search_)

-- | Move col cursor to target index (pure helper)
moveColTo :: TblOps t => ViewStack t -> Int -> ViewStack t
moveColTo s colIdx =
  let v      = cur s
      n      = v ^. #nav
      nCols_ = V.length (TblOps.colNames (n ^. #tbl))
      delta  = colIdx - n ^. #col % #cur
      nav'   = over colCurL (\f -> finClamp nCols_ f delta) n
  in setCur s (v & #nav .~ nav')

-- | col search: fzf jump to column by name (IO version for backward compat)
colSearch :: TblOps t => ViewStack t -> IO (ViewStack t)
colSearch s = do
  mi <- Fzf.fzfIdx (V.fromList ["--prompt=Column: "]) (Nav.dispColNames (nav (cur s)))
  case mi of
    Nothing  -> pure s
    Just idx -> pure (moveColTo s idx)

-- | Shared: resolve current column, fetch sorted distinct values
withDistinct
  :: TblOps t
  => ViewStack t
  -> (Int -> Text -> Vector Text -> IO (ViewStack t))
  -> IO (ViewStack t)
withDistinct s f = do
  let v       = cur s
      curCol  = Nav.curColIdx (v ^. #nav)
      curName = Nav.curColName (v ^. #nav)
  vals <- TblOps.distinct (v ^. #nav % #tbl) curCol
  let sorted = V.fromList (L.sort (V.toList vals))
  f curCol curName sorted

-- | row search (/): find value in current column, jump to matching row (IO)
rowSearch :: TblOps t => ViewStack t -> IO (ViewStack t)
rowSearch s = withDistinct s $ \curCol curName vals -> do
  mr <- Fzf.fzf (V.fromList ["--prompt=/" <> curName <> ": "])
                (T.intercalate "\n" (V.toList vals))
  case mr of
    Nothing     -> pure s
    Just result -> do
      let start = cur s ^. #nav % #row % #cur + 1
      mri <- TblOps.findRow (tbl s) curCol result start True
      case mri of
        Nothing     -> pure s
        Just rowIdx -> pure (moveRowTo s rowIdx (Just (curCol, result)))

-- | findRow with cache: fzf fires focus on every arrow key, so without caching
-- each fires a SQL query causing visible lag.
cachedFindRow
  :: TblOps t
  => IORef (HashMap Int (Maybe Int))
  -> t -> Int -> Int -> Text -> IO (Maybe Int)
cachedFindRow cache tbl_ col idx val = do
  m <- readIORef cache
  case HM.lookup idx m of
    Just hit -> pure hit
    Nothing  -> do
      r <- TblOps.findRow tbl_ col val 0 True
      modifyIORef cache (HM.insert idx r)
      pure r

-- | Build poll callback for live search preview.
-- On each fzf focus change, finds the matching row (cached) and re-renders.
searchPoll
  :: TblOps t
  => t -> Int -> Vector Text
  -> IORef (ViewStack t) -> (ViewStack t -> IO ())
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
                headD d []    = d
                headD _ (x:_) = x
            if headD "" parts /= "search.preview"
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
                        let val = maybe "" id (vals V.!? idx)
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
  :: TblOps t
  => ViewStack t -> (ViewStack t -> IO ()) -> IO (ViewStack t)
rowSearchLive s preview = withDistinct s $ \curCol curName vals -> do
  tm <- Fzf.getTestMode
  if tm
    then do
      let result = maybe "" id (vals V.!? 0)
      if T.null result
        then pure s
        else do
          mri <- TblOps.findRow (tbl s) curCol result 0 True
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
      out <- Fzf.fzfCore opts (T.intercalate "\n" (V.toList items)) poll
      if T.null out
        then readIORef sRef  -- cancelled: keep preview position
        else do
          -- apply final selection (reuse cache from preview)
          let parts = T.splitOn "\t" out
              hd = case parts of { (x:_) -> x; _ -> "" }
          case readMaybe (T.unpack hd) :: Maybe Int of
            Nothing  -> pure s
            Just idx -> do
              let result = maybe "" id (vals V.!? idx)
              mri <- cachedFindRow cache (tbl s) curCol idx result
              case mri of
                Nothing     -> pure s
                Just rowIdx -> pure (moveRowTo s rowIdx (Just (curCol, result)))

-- | search in direction: fwd=true (n), bwd=false (N)
searchDir :: TblOps t => ViewStack t -> Bool -> IO (ViewStack t)
searchDir s fwd = do
  let v = cur s
  case v ^. #search of
    Nothing         -> pure s
    Just (col, val) -> do
      let rowCur = v ^. #nav % #row % #cur
          start  = if fwd then rowCur + 1 else rowCur
      mri <- TblOps.findRow (v ^. #nav % #tbl) col val start fwd
      case mri of
        Nothing     -> pure s
        Just rowIdx -> pure (moveRowTo s rowIdx Nothing)

-- | row filter (\): filter rows by expression, push filtered view (IO)
-- Uses TblOps.buildFilter for backend-specific syntax (PRQL vs q)
rowFilter :: TblOps t => ViewStack t -> IO (ViewStack t)
rowFilter s = withDistinct s $ \_curCol curName vals -> do
  let typ    = TblOps.colType (tbl s) _curCol
      typStr = StrEnum.toString typ
      header = TblOps.filterPrompt (tbl s) curName typStr
  mr <- Fzf.fzf
          (V.fromList ["--print-query", "--header=" <> header, "--prompt=filter > "])
          (T.intercalate "\n" (V.toList vals))
  case mr of
    Nothing     -> pure s
    Just result -> do
      let expr = TblOps.buildFilter (tbl s) curName vals result (colTypeIsNumeric typ)
      if T.null expr
        then pure s
        else do
          mt <- TblOps.filter_ (tbl s) expr
          case mt of
            Nothing   -> pure s
            Just tbl' ->
              -- Preserve cursor column across filter (matches Lean's
              -- `rebuild tbl' (row := 0)` default). Resetting col=0 would
              -- snap the cursor back to the first column.
              let v      = cur s
                  curCol = Nav.curColIdx (v ^. #nav)
                  grp'   = v ^. #nav % #grp
              in case View.rebuild v tbl' curCol grp' 0 of
                   Nothing -> pure s
                   Just v' -> pure (push s (v' & #disp .~ ("\\" <> curName)))

-- | Jump to column by name directly (no fzf). Called by socket/dispatch.
colJumpWith :: TblOps t => ViewStack t -> Text -> IO (ViewStack t)
colJumpWith s name = do
  if T.null name
    then pure s
    else case V.findIndex (== name) (Nav.dispColNames (cur s ^. #nav)) of
      Just idx -> pure (moveColTo s idx)
      Nothing  -> pure s

-- | Filter by expression directly (no fzf). Called by socket/dispatch.
filterWith :: TblOps t => ViewStack t -> Text -> IO (ViewStack t)
filterWith s expr = do
  if T.null expr
    then pure s
    else do
      mt <- TblOps.filter_ (tbl s) expr
      case mt of
        Nothing   -> pure s
        Just tbl' ->
          -- Match Lean's `rebuild tbl' (row := 0)` — col defaults to the
          -- old cursor column, grp to the old grp. Resetting col to 0
          -- would jump the cursor home, which the filter demo explicitly
          -- asserts should NOT happen (cursor stays on the filtered col).
          let v      = cur s
              curCol = Nav.curColIdx (v ^. #nav)
              grp'   = v ^. #nav % #grp
          in case View.rebuild v tbl' curCol grp' 0 of
               Nothing -> pure s
               Just v' -> pure (push s (v' & #disp .~ ("\\" <> expr)))

-- | Search for value directly (no fzf). Called by socket/dispatch.
searchWith :: TblOps t => ViewStack t -> Text -> IO (ViewStack t)
searchWith s val = do
  if T.null val
    then pure s
    else do
      let v      = cur s
          curCol = Nav.curColIdx (v ^. #nav)
          start  = v ^. #nav % #row % #cur + 1
      mri <- TblOps.findRow (tbl s) curCol val start True
      case mri of
        Nothing     -> pure s
        Just rowIdx -> pure (moveRowTo s rowIdx (Just (curCol, val)))

-- | Dispatch filter handler to IO action. Returns none if handler not recognized.
dispatch
  :: ViewStack AdbcTable
  -> Cmd
  -> Maybe (IO (ViewStack AdbcTable))
dispatch s h = case h of
  CmdColSearch     -> Just (colSearch s)
  CmdRowFilter     -> Just (rowFilter s)
  CmdRowSearchNext -> Just (searchDir s True)
  CmdRowSearchPrev -> Just (searchDir s False)
  _                -> Nothing
