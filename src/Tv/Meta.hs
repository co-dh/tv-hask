{-
  Meta view: column statistics via DuckDB temp table tc_meta_N.
  Selection and key operations use composable PRQL (via ADBC/Meta).
  Each meta view gets a unique temp table so concurrent views don't collide.

  Literal port of Tc/Tc/Meta.lean — same function names, same order, same
  comments. No invented abstractions.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Meta
  ( push
  , selNull
  , selSingle
  , setKey
  , dispatch
  , commands
  ) where

import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Vector (Vector)

import Optics.Core ((%), (&), (.~), (^.))
import Tv.App.Types (HandlerFn, domainH)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import qualified Tv.Nav as Nav
import Tv.Types (Cmd (..), ViewKind (..))
import qualified Tv.Types as TblOps
import Tv.View (ViewStack)
import qualified Tv.View as View

import qualified Tv.Data.ADBC.Ops as Ops
import qualified Tv.Data.ADBC.Prql as Prql
import qualified Tv.Data.ADBC.Table as Table
import Tv.Data.ADBC.Table (AdbcTable)

-- | Extract meta table name from the current view's AdbcTable query base.
--   e.g. "from tc_meta_3" -> "tc_meta_3"
tblName :: ViewStack AdbcTable -> Text
tblName s = T.strip (T.drop 5 (Prql.base (Table.query (View.tbl s))))  -- drop "from "

-- | Push column metadata view onto stack
push :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
push s = do
  mAdbc <- Ops.queryMeta (View.tbl s)
  case mAdbc of
    Nothing -> pure Nothing
    Just adbc0 -> do
      -- Enrich meta with DuckDB column comments (e.g. osquery views with COMMENT ON COLUMN)
      let metaBase = T.strip (T.drop 5 (Prql.base (Table.query adbc0)))
      enriched <- Ops.enrichComments metaBase (View.cur s ^. #path)
      adbc <-
        if enriched
          then fromMaybe adbc0 <$> Table.requery (Table.query adbc0) (Table.totalRows adbc0)
          else pure adbc0
      let mV = View.fromTbl adbc (View.cur s ^. #path) 0 mempty 0
      pure $ fmap
        (\v -> View.push s (v & #vkind .~ VkColMeta & #disp .~ "meta"))
        mV

-- | Select meta rows matching PRQL filter
selBy :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
selBy s flt =
  if View.cur s ^. #vkind /= VkColMeta then pure s
  else do
    rows <- Ops.metaIdxs (tblName s) flt
    pure (View.setCur s (View.cur s & #nav % #row % #sels .~ rows))

selNull :: ViewStack AdbcTable -> IO (ViewStack AdbcTable)
selNull s = selBy s "null_pct == 100"

selSingle :: ViewStack AdbcTable -> IO (ViewStack AdbcTable)
selSingle s = selBy s "dist == 1"

-- | Set key cols from meta view selections, pop to parent, select cols
setKey :: ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))
setKey s =
  if View.cur s ^. #vkind /= VkColMeta then pure (Just s)
  else if not (View.hasParent s) then pure (Just s)
  else do
    colNames <- Ops.metaNames (tblName s) (View.cur s ^. #nav % #row % #sels)
    case View.pop s of
      Just s' -> do
        let di = Nav.dispOrder colNames (TblOps.colNames (View.tbl s'))
            v  = View.cur s'
                 & #nav % #grp      .~ colNames
                 & #nav % #dispIdxs .~ di
                 & #nav % #col % #sels .~ colNames
        pure (Just (View.setCur s' v))
      Nothing -> pure (Just s)

-- | Dispatch meta handler to IO action. Returns Nothing if handler not recognized.
dispatch
  :: ViewStack AdbcTable -> Cmd
  -> Maybe (IO (Maybe (ViewStack AdbcTable)))
dispatch s h = case h of
  CmdMetaPush      -> Just (push s)
  CmdMetaSelNull   -> Just (Just <$> selNull s)
  CmdMetaSelSingle -> Just (Just <$> selSingle s)
  CmdMetaSetKey ->
    if View.cur s ^. #vkind == VkColMeta then Just (setKey s) else Nothing
  _ -> Nothing

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdMetaPush      ""  "M"     "Open column metadata view"       True  "")        (domainH dispatch)
  , hdl (mkEntry CmdMetaSetKey    "s" "<ret>" "Set selected rows as key columns" True  "colMeta") (domainH dispatch)
  , hdl (mkEntry CmdMetaSelNull   ""  "0"     "Select columns with null values"  True  "")        (domainH dispatch)
  , hdl (mkEntry CmdMetaSelSingle ""  "1"     "Select columns with single value" True  "")        (domainH dispatch)
  ]
