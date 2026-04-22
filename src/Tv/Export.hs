{-
  Export: save current view to file (csv/parquet/json/ndjson).
  Uses DuckDB COPY for efficient streaming export.

  Literal port of Tc/Tc/Export.lean. Same function names and order, same
  comments, no invented abstractions.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Export where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.App.Types (HandlerFn, stackIO)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Table (AdbcTable, stripSemi)
import qualified Tv.Fzf as Fzf
import qualified Tv.Render as Render
import Tv.Types (Cmd(..), ExportFmt(..), StrEnum(toString, ofStringQ), escSql)
import qualified Tv.Util as Log
import Tv.View (ViewStack)
import qualified Tv.View as View


-- | DuckDB COPY option clause for an export format
copyOpt :: ExportFmt -> Text
copyOpt ExportCsv     = "(FORMAT CSV, HEADER true)"
copyOpt ExportJson    = "(FORMAT JSON)"
copyOpt ExportParquet = "(FORMAT PARQUET)"
copyOpt ExportNdjson  = "(FORMAT JSON, ARRAY false)"

-- ============================================================================
-- Tc.Export namespace
-- ============================================================================

-- | Prompt user for export format via fzf
pickFmt :: Bool -> IO (Maybe ExportFmt)
pickFmt tm = do
  m <- Fzf.fzf tm (V.fromList ["--prompt=export: "]) "csv\nparquet\njson\nndjson"
  pure (maybe Nothing (\raw -> ofStringQ (T.strip raw)) m)

-- | Export current view to file via DuckDB COPY
exportView :: AdbcTable -> Text -> ExportFmt -> IO ()
exportView t path fmt = do
  mSql <- Prql.compile (Prql.queryRender (t ^. #query))
  case mSql of
    Nothing  -> ioError (userError "PRQL compile failed")
    Just sql -> do
      let copySql = "COPY (" <> stripSemi sql <> ") TO '" <> escSql path
                 <> "' " <> copyOpt fmt
      Log.write "export" copySql
      _ <- Conn.query copySql
      pure ()

-- | Run export effect: build path from view name, export via DuckDB COPY
run :: ViewStack AdbcTable -> ExportFmt -> IO (ViewStack AdbcTable)
run s fmt = do
  let name0 = T.replace " " "_" (T.replace "/" "_" (View.tabName (View.cur s)))
      parts = T.splitOn "." name0
      stem  = case parts of
                (x : _) | not (T.null x) -> x
                _                        -> name0
  d <- Log.dir
  let path = T.pack d <> "/tv_export_" <> stem <> "." <> toString fmt
  exportView (View.tbl s) path fmt
  Render.statusMsg ("exported " <> path)
  pure s

-- | Export by format string directly (no fzf). Called by socket/dispatch.
runWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
runWith s fmtStr = maybe (pure s) (\fmt -> run s fmt) (ofStringQ fmtStr)

exportH :: HandlerFn
exportH = \a _ arg -> stackIO a
  (if T.null arg
     then do
       mf <- pickFmt (a ^. #testMode)
       maybe (pure (a ^. #stk)) (\f -> run (a ^. #stk) f) mf
     else runWith (a ^. #stk) arg)

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdTblExport "a" "e" "Export table (csv/parquet/json/ndjson)" False "") exportH
  ]
