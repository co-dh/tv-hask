{-
  Filter: fzf-based column/row filtering and search.
  dispatch returns IO action directly; no intermediate Effect.

  Literal port of Tc/Tc/Filter.lean. Namespaces Tc.ViewStack and Tc.Filter
  both land in this module.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Filter where

import Tv.Prelude
import Control.Applicative ((<|>))
import Control.Monad (guard)
import Control.Monad.Trans.Maybe (MaybeT(..), runMaybeT, hoistMaybe)
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.List as L
import Text.Read (readMaybe)

import Tv.App.Types (HandlerFn, onStk, stackIO)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import Tv.Nav (rowCur, colCur, finClamp)
import qualified Tv.Nav as Nav
import Tv.Types (Cmd(..), isNumeric, toString, filterPrql, filterPrompt, exprError)
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Table as Table
import Tv.Data.DuckDB.Table (AdbcTable)
import Tv.View (ViewStack, cur, setCur, push, tbl)
import qualified Tv.View as View
import qualified Tv.Fzf as Fzf
import qualified Tv.Render as Render

-- | Run a MaybeT IO pipeline; on Nothing (cancel/no-match), keep the
-- original stack. The Filter family all have this shape:
-- "chain several IO-Maybe stages; if any stage bails, leave s alone."
orKeep :: ViewStack AdbcTable -> MaybeT IO (ViewStack AdbcTable) -> IO (ViewStack AdbcTable)
orKeep s = fmap (fromMaybe s) . runMaybeT

-- | Move row cursor to target index (pure helper)
moveRowTo
  :: ViewStack AdbcTable -> Int -> Maybe (Int, Text) -> ViewStack AdbcTable
moveRowTo s rowIdx search_ =
  let v      = cur s
      n      = v ^. #nav
      nRows_ = (n ^. #tbl % #nRows)
      delta  = rowIdx - n ^. #row % #cur
      nav'   = over rowCur (\f -> finClamp nRows_ f delta) n
  in setCur s (v & #nav .~ nav'
                 & #search .~ (search_ <|> (v ^. #search)))

-- | Move col cursor to target index (pure helper)
moveTo :: ViewStack AdbcTable -> Int -> ViewStack AdbcTable
moveTo s colIdx =
  let v      = cur s
      n      = v ^. #nav
      nCols_ = V.length (n ^. #tbl % #colNames)
      delta  = colIdx - n ^. #col % #cur
      nav'   = over colCur (\f -> finClamp nCols_ f delta) n
  in setCur s (v & #nav .~ nav')

-- | col search: fzf jump to column by name (IO version for backward compat)
colSearch :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
colSearch tm s = orKeep s $ do
  idx <- MaybeT $ Fzf.fzfIdx tm (V.fromList ["--prompt=Column: "])
                    (Nav.dispNames (cur s ^. #nav))
  pure $ moveTo s idx

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
rowSearch tm s = withDistinct s $ \curCol curName vals -> orKeep s $ do
  result <- MaybeT $ Fzf.fzf tm (V.fromList ["--prompt=/" <> curName <> ": "])
              (T.intercalate "\n" (V.toList vals))
  let start = cur s ^. #nav % #row % #cur + 1
  rowIdx <- MaybeT $ Table.findRow (tbl s) curCol result start True
  pure $ moveRowTo s rowIdx (Just (curCol, result))

-- | findRow with cache: the picker fires onFocus on every arrow key,
-- so without caching each fires a SQL query causing visible lag.
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

-- | Build the picker's onFocus callback. Given an item index into the
-- distinct-values vector, resolve it to a row (cached), move the cursor,
-- and re-render. Debounced by @lastIdx@ so duplicate focus events are no-ops.
searchFocus
  :: AdbcTable -> Int -> Vector Text
  -> IORef (ViewStack AdbcTable) -> (ViewStack AdbcTable -> IO ())
  -> IO (IORef (HashMap Int (Maybe Int)), Int -> Text -> IO ())
searchFocus tbl_ curCol vals sRef preview = do
  lastIdx <- newIORef (Nothing :: Maybe Int)
  cache   <- newIORef (HM.empty :: HashMap Int (Maybe Int))
  -- Picker hands us (indexIntoInput, rawLine). The input is "i\tvalue", so
  -- we pull i back out of the raw line for the cache key. A duplicate focus
  -- event (same i as last) is a no-op to avoid redundant re-renders.
  let onFocus _rawIdx line = case T.splitOn "\t" line of
        (hTxt : _) -> maybe (pure ()) (\idx -> applyFocus idx lastIdx cache)
                            (readMaybe (T.unpack hTxt))
        _ -> pure ()
      applyFocus idx lastR cacheR = do
        li <- readIORef lastR
        if idx >= V.length vals || li == Just idx
          then pure ()
          else do
            writeIORef lastR (Just idx)
            let val = fromMaybe "" (vals V.!? idx)
            mri <- cachedFindRow cacheR tbl_ curCol idx val
            case mri of
              Nothing     -> pure ()
              Just rowIdx -> do
                s0 <- readIORef sRef
                let s' = moveRowTo s0 rowIdx (Just (curCol, val))
                writeIORef sRef s'
                preview s'
  pure (cache, onFocus)

-- | Row search with live preview: cursor moves as the user browses the
-- picker results. Picker onFocus → findRow → re-render.
rowSearchLive
  :: Bool -> ViewStack AdbcTable -> (ViewStack AdbcTable -> IO ()) -> IO (ViewStack AdbcTable)
rowSearchLive tm s preview = withDistinct s $ \curCol curName vals ->
  if tm
    then let result = fromMaybe "" (vals V.!? 0)
         in if T.null result then pure s
            else applyRow s curCol result (Table.findRow (tbl s) curCol result 0 True)
    else runLive s preview curCol curName vals

-- | Apply a row-search match: run `findM`, move cursor on hit, keep `s`
-- on miss. Shared by test-mode (uncached) and live-mode finalize (cached).
applyRow
  :: ViewStack AdbcTable -> Int -> Text -> IO (Maybe Int)
  -> IO (ViewStack AdbcTable)
applyRow s curCol result findM = do
  maybe s (\rowIdx -> moveRowTo s rowIdx (Just (curCol, result))) <$> findM

-- | Live-preview path: the in-process picker fires onFocus for every
-- highlight change, which we wire straight into row movement — no socat,
-- no preview script.
runLive
  :: ViewStack AdbcTable -> (ViewStack AdbcTable -> IO ())
  -> Int -> Text -> Vector Text
  -> IO (ViewStack AdbcTable)
runLive s preview curCol curName vals = do
  let items = V.imap (\i v -> T.pack (show i) <> "\t" <> v) vals
  sRef <- newIORef s
  (cache, onFocus) <- searchFocus (tbl s) curCol vals sRef preview
  let opts = V.fromList
        [ "--prompt=/" <> curName <> ": "
        , "--with-nth=2..", "--delimiter=\t"
        ]
  out <- Fzf.fzfCoreLive False opts (T.intercalate "\n" (V.toList items))
           (pure ()) onFocus
  if T.null out
    then readIORef sRef
    else maybe (pure s)
               (\idx ->
                  let result = fromMaybe "" (vals V.!? idx)
                  in applyRow s curCol result (cachedFindRow cache (tbl s) curCol idx result))
               (readMaybe (T.unpack (pickIdx out)))
  where
    pickIdx out = case T.splitOn "\t" out of { (x:_) -> x; _ -> "" }

-- | search in direction: fwd=true (n), bwd=false (N)
searchDir :: ViewStack AdbcTable -> Bool -> IO (ViewStack AdbcTable)
searchDir s fwd = orKeep s $ do
  let v = cur s
  (col, val) <- hoistMaybe (v ^. #search)
  let rc    = v ^. #nav % #row % #cur
      start = if fwd then rc + 1 else rc
  rowIdx <- MaybeT $ Table.findRow (v ^. #nav % #tbl) col val start fwd
  pure $ moveRowTo s rowIdx Nothing

-- | row filter (\): filter rows by expression, push filtered view (IO).
-- If the user's expression trips 'exprError' (lone @=@, etc.) the filter
-- is aborted with a popup message instead of silently failing in PRQL.
rowFilter :: Bool -> ViewStack AdbcTable -> IO (ViewStack AdbcTable)
rowFilter tm s = withDistinct s $ \_curCol curName vals -> do
  let typ    = Ops.colType (tbl s) _curCol
      header = filterPrompt curName (toString typ)
  mResult <- Fzf.fzf tm
              (V.fromList ["--print-query", "--header=" <> header, "--prompt=filter > "])
              (T.intercalate "\n" (V.toList vals))
  case mResult of
    Nothing -> pure s
    Just result ->
      let expr = filterPrql curName vals result (isNumeric typ)
      in case exprError expr of
        Just msg -> Render.errorPopup ("filter: " <> msg) >> pure s
        Nothing  -> orKeep s $ do
          guard $ not $ T.null expr
          tbl' <- MaybeT $ Table.filter (tbl s) expr
          let v      = cur s
              curCol = Nav.colIdx (v ^. #nav)
              grp'   = v ^. #nav % #grp
          v' <- hoistMaybe $ View.rebuild v tbl' curCol grp' 0
          pure $ push s (v' & #disp .~ ("\\" <> curName))

-- | Jump to column by name directly (no fzf). Called by socket/dispatch.
jumpCol :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
jumpCol s name = orKeep s $ do
  guard $ not $ T.null name
  idx <- hoistMaybe $ V.findIndex (== name) (Nav.dispNames (cur s ^. #nav))
  pure $ moveTo s idx

-- | Filter by expression directly (no fzf). Called by socket/dispatch.
-- Lints the expression for common SQL-vs-PRQL mistakes (e.g. lone @=@)
-- and surfaces a popup instead of silently bouncing the query through
-- PRQL and getting back an opaque compile error.
filterWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
filterWith s expr = case exprError expr of
  Just msg -> Render.errorPopup ("filter: " <> msg) >> pure s
  Nothing  -> orKeep s $ do
    guard $ not $ T.null expr
    tbl' <- MaybeT $ Table.filter (tbl s) expr
    -- Match Lean's `rebuild tbl' (row := 0)` — col defaults to the old cursor
    -- column, grp to the old grp. Resetting col to 0 would jump the cursor
    -- home, which the filter demo explicitly asserts should NOT happen.
    let v      = cur s
        curCol = Nav.colIdx (v ^. #nav)
        grp'   = v ^. #nav % #grp
    v' <- hoistMaybe $ View.rebuild v tbl' curCol grp' 0
    pure $ push s (v' & #disp .~ ("\\" <> expr))

-- | Search for value directly (no fzf). Called by socket/dispatch.
searchWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
searchWith s val = orKeep s $ do
  guard $ not $ T.null val
  let v      = cur s
      curCol = Nav.colIdx (v ^. #nav)
      start  = v ^. #nav % #row % #cur + 1
  rowIdx <- MaybeT $ Table.findRow (tbl s) curCol val start True
  pure $ moveRowTo s rowIdx (Just (curCol, val))

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdRowFilter     "a"  "\\" "Filter rows by PRQL expression"    True  "")
        (\a _ arg -> stackIO a (if T.null arg then rowFilter (a ^. #testMode) (a ^. #stk) else filterWith (a ^. #stk) arg))
  , hdl (mkEntry CmdRowSearchNext "rc" "n"  "Jump to next search match"         False "") (onStk (fmap Just . (`searchDir` True)))
  , hdl (mkEntry CmdRowSearchPrev "rc" "N"  "Jump to previous search match"     False "") (onStk (fmap Just . (`searchDir` False)))
  , hdl (mkEntry CmdColSearch     "a"  "g"  "Jump to column by name"            True  "")
        (\a _ arg -> stackIO a (if T.null arg then colSearch (a ^. #testMode) (a ^. #stk) else jumpCol (a ^. #stk) arg))
  ]
