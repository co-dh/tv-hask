{-
  Core types: Cell, Column, Table, PureKey
  Table stores columns by name (HashMap) for direct name-based access
-}
{-# LANGUAGE DeriveGeneric #-}
module Tv.Types where

import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8, Word32)
import Tv.StrEnum (StrEnum(..))
import qualified Tv.StrEnum as StrEnum

-- | Join array elements with separator (avoids .toList |> sep.intercalate)
joinWith :: Vector Text -> Text -> Text
joinWith a sep = T.intercalate sep (V.toList a)

-- | Column type — parsed once at FFI boundary, used everywhere else
data ColType
  = ColTypeInt
  | ColTypeFloat
  | ColTypeDecimal
  | ColTypeStr
  | ColTypeDate
  | ColTypeTime
  | ColTypeTimestamp
  | ColTypeBool
  | ColTypeOther
  deriving (Eq, Show)

instance StrEnum ColType where
  toString ColTypeInt       = "int"
  toString ColTypeFloat     = "float"
  toString ColTypeDecimal   = "decimal"
  toString ColTypeStr       = "str"
  toString ColTypeDate      = "date"
  toString ColTypeTime      = "time"
  toString ColTypeTimestamp = "timestamp"
  toString ColTypeBool      = "bool"
  toString ColTypeOther     = "other"
  all = V.fromList
    [ ColTypeInt, ColTypeFloat, ColTypeDecimal, ColTypeStr
    , ColTypeDate, ColTypeTime, ColTypeTimestamp, ColTypeBool, ColTypeOther ]
  ofStringQ "int"       = Just ColTypeInt
  ofStringQ "float"     = Just ColTypeFloat
  ofStringQ "decimal"   = Just ColTypeDecimal
  ofStringQ "str"       = Just ColTypeStr
  ofStringQ "date"      = Just ColTypeDate
  ofStringQ "time"      = Just ColTypeTime
  ofStringQ "timestamp" = Just ColTypeTimestamp
  ofStringQ "bool"      = Just ColTypeBool
  ofStringQ "other"     = Just ColTypeOther
  ofStringQ _           = Nothing

-- ofString with fallback (FFI boundary returns unknown types)
colTypeOfString :: Text -> ColType
colTypeOfString s = maybe ColTypeOther id (StrEnum.ofStringQ s)

colTypeIsNumeric :: ColType -> Bool
colTypeIsNumeric ColTypeInt     = True
colTypeIsNumeric ColTypeFloat   = True
colTypeIsNumeric ColTypeDecimal = True
colTypeIsNumeric _              = False

colTypeIsTime :: ColType -> Bool
colTypeIsTime ColTypeTime      = True
colTypeIsTime ColTypeTimestamp = True
colTypeIsTime ColTypeDate      = True
colTypeIsTime _                = False

-- | Toggle element in array (add if absent, remove if present)
toggle :: Eq a => Vector a -> a -> Vector a
toggle arr x =
  if V.elem x arr then V.filter (/= x) arr else V.snoc arr x

-- | Cell value (sum type)
-- Uses Int64 to guarantee scalar representation (no MPZ boxing)
data Cell
  = CellNull
  | CellInt Int64
  | CellFloat Double
  | CellStr Text
  | CellBool Bool

-- | Column: uniform typed storage (one type per column)
-- More efficient than Array Cell (no per-cell tag overhead)
-- ints: no null support; floats: NaN = null; strs: empty = null
data Column
  = ColumnInts   (Vector Int64)
  | ColumnFloats (Vector Double)
  | ColumnStrs   (Vector Text)

columnMapArr
  :: Column
  -> (Vector Int64 -> Vector Int64)
  -> (Vector Double -> Vector Double)
  -> (Vector Text -> Vector Text)
  -> Column
columnMapArr col fi ff fs = case col of
  ColumnInts   d -> ColumnInts   (fi d)
  ColumnFloats d -> ColumnFloats (ff d)
  ColumnStrs   d -> ColumnStrs   (fs d)

-- | Get cell at row index
columnGet :: Column -> Int -> Cell
columnGet col i = case col of
  ColumnInts data_ -> CellInt (maybe 0 id (data_ V.!? i))
  ColumnFloats data_ ->
    let f = maybe 0 id (data_ V.!? i)
    in if isNaN f then CellNull else CellFloat f
  ColumnStrs data_ ->
    let s = maybe "" id (data_ V.!? i)
    in if T.null s then CellNull else CellStr s

-- | Row count
columnSize :: Column -> Int
columnSize col = case col of
  ColumnInts d   -> V.length d
  ColumnFloats d -> V.length d
  ColumnStrs d   -> V.length d

columnGather :: Column -> Vector Int -> Column
columnGather col idxs =
  columnMapArr col
    (\d -> V.map (\i -> maybe 0 id (d V.!? i)) idxs)
    (\d -> V.map (\i -> maybe 0 id (d V.!? i)) idxs)
    (\d -> V.map (\i -> maybe "" id (d V.!? i)) idxs)

columnTake :: Column -> Int -> Column
columnTake col n =
  columnMapArr col (V.take n) (V.take n) (V.take n)

-- | Raw string value (for PRQL filters)
cellToRaw :: Cell -> Text
cellToRaw CellNull      = ""
cellToRaw (CellInt n)   = T.pack (show n)
cellToRaw (CellFloat f) = T.pack (show f)
cellToRaw (CellStr s)   = s
cellToRaw (CellBool b)  = if b then "true" else "false"

-- | Format cell value as PRQL literal
cellToPrql :: Cell -> Text
cellToPrql CellNull      = "null"
cellToPrql (CellInt n)   = T.pack (show n)
cellToPrql (CellFloat f) = T.pack (show f)
cellToPrql (CellStr s)   = "'" <> T.replace "'" "''" s <> "'"
cellToPrql (CellBool b)  = if b then "true" else "false"

-- | Escape single quotes for SQL string literals
escSql :: Text -> Text
escSql s = T.replace "'" "''" s

-- | Compute pct and bar from count data (for freq → fromArrays).
freqPctBar :: Vector Int64 -> (Vector Double, Vector Text)
freqPctBar cntData =
  let total = V.sum cntData
      pct = V.map
              (\c -> if total > 0
                       then fromIntegral c * 100 / fromIntegral total
                       else 0)
              cntData
      bar = V.map
              (\p -> T.replicate (max 0 (truncate (p / 5.0) :: Int)) "#")
              pct
  in (pct, bar)

{-! ## Core Typeclasses -}

-- Render context: all parameters for table rendering (avoids NavState dependency)
data RenderCtx = RenderCtx
  { inWidths   :: Vector Int
  , dispIdxs   :: Vector Int
  , nGrp       :: Int
  , r0         :: Int
  , r1         :: Int
  , curRow     :: Int
  , curCol     :: Int
  , moveDir    :: Int
  , selColIdxs :: Vector Int
  , rowSels    :: Vector Int
  , hiddenIdxs :: Vector Int
  , styles     :: Vector Word32
  , prec       :: Int
  , widthAdj   :: Int
  , heatMode   :: Word8  -- 0=off, 1=numeric, 2=categorical, 3=both
  , sparklines :: Vector Text
  }

-- | Build PRQL filter expression from fzf result (default for TblOps.buildFilter)
-- With --print-query: line 0 = query, lines 1+ = selections
buildFilterPrql :: Text -> Vector Text -> Text -> Bool -> Text
buildFilterPrql col vals result numeric =
  let lines_ = V.fromList (filter (not . T.null) (T.splitOn "\n" result))
      input = maybe "" id (lines_ V.!? 0)
      fromHints = V.filter (`V.elem` vals) (V.slice 1 (max 0 (V.length lines_ - 1)) lines_)
      selected =
        if V.elem input vals && not (V.elem input fromHints)
          then V.cons input fromHints
          else fromHints
      q v = if numeric then v else "'" <> v <> "'"
  in if V.length selected == 1
       then col <> " == " <> q (maybe "" id (selected V.!? 0))
     else if V.length selected > 1
       then "(" <> joinWith (V.map (\v -> col <> " == " <> q v) selected) " || " <> ")"
     else if not (T.null input)
       then input
       else ""

-- compat shim — prefer ColType.isNumeric on typed values
isNumericType :: Text -> Bool
isNumericType t = colTypeIsNumeric (colTypeOfString t)

{- | TblOps: unified read-only table interface.
    Provides row/column access, metadata queries, filtering, and rendering. -}
class TblOps a where
  nRows     :: a -> Int                                            -- row count in view
  colNames  :: a -> Vector Text                                    -- column names
  totalRows :: a -> Int                                            -- actual rows (ADBC)
  totalRows = nRows
  filter_   :: a -> Text -> IO (Maybe a)                           -- filter by expr
  distinct  :: a -> Int -> IO (Vector Text)                        -- distinct values
  findRow   :: a -> Int -> Text -> Int -> Bool -> IO (Maybe Int)   -- find row
  render    :: a -> RenderCtx -> IO (Vector Int)
  -- extract columns [r0, r1) by index (for plot/export)
  getCols   :: a -> Vector Int -> Int -> Int -> IO (Vector Column)
  getCols _ _ _ _ = pure V.empty
  -- column type
  colType   :: a -> Int -> ColType
  colType _ _ = ColTypeOther
  -- build filter expression from fzf result (default: PRQL syntax)
  buildFilter :: a -> Text -> Vector Text -> Text -> Bool -> Text
  buildFilter _ = buildFilterPrql
  -- filter header hint (shown above fzf input, default: PRQL examples)
  filterPrompt :: a -> Text -> Text -> Text
  filterPrompt _ col typ =
    let eg = if isNumericType typ
               then "e.g. " <> col <> " > 5,  " <> col <> " >= 10 && " <> col <> " < 100"
               else "e.g. " <> col <> " == 'USD',  " <> col <> " ~= 'pattern'"
    in "PRQL filter on " <> col <> " (" <> typ <> "):  " <> eg
  -- export plot data to tmpdir/plot.dat via DB (returns category list, or none for fallback)
  -- args: tbl xName yName catName? xIsTime step truncLen
  plotExport :: a -> Text -> Text -> Maybe Text -> Bool -> Int -> Int -> IO (Maybe (Vector Text))
  plotExport _ _ _ _ _ _ _ = pure Nothing
  -- get cell value as string (for preview)
  cellStr   :: a -> Int -> Int -> IO Text
  cellStr _ _ _ = pure ""
  -- fetch more rows (scroll-to-bottom): returns table with more rows, or none
  fetchMore :: a -> IO (Maybe a)
  fetchMore _ = pure Nothing
  -- loading (file or URL)
  fromFile  :: Text -> IO (Maybe a)
  fromFile _ = pure Nothing
  fromUrl   :: Text -> IO (Maybe a)
  fromUrl _ = pure Nothing

{- | ModifyTable: mutable table operations (extends TblOps).
    Column hiding and sorting; row deletion is done via filter. -}
class TblOps a => ModifyTable a where
  hideCols :: Vector Int -> a -> IO a           -- hide columns
  sortBy   :: Vector Int -> Bool -> a -> IO a   -- sort by columns

-- Hide columns at cursor + selections, return new table and filtered group
modifyTableHide
  :: ModifyTable a
  => a -> Int -> Vector Int -> Vector Text -> IO (a, Vector Text)
modifyTableHide tbl cursor sels grp = do
  let idxs = if V.elem cursor sels then sels else V.snoc sels cursor
      names = colNames tbl
      hideNames = V.map (\i -> maybe "" id (names V.!? i)) idxs
  newTbl <- hideCols idxs tbl
  pure (newTbl, V.filter (not . (`V.elem` hideNames)) grp)

-- Sort table by selected columns + cursor column, excluding group (key) columns
modifyTableSort
  :: ModifyTable a
  => a -> Int -> Vector Int -> Vector Int -> Bool -> IO a
modifyTableSort tbl cursor selIdxs grpIdxs asc =
  let cols = V.fromList
             . eraseDups
             . V.toList
             . V.filter (not . (`V.elem` grpIdxs))
             $ selIdxs V.++ V.singleton cursor
  in if V.null cols then pure tbl else sortBy cols asc tbl
  where
    eraseDups []     = []
    eraseDups (x:xs) = x : eraseDups (filter (/= x) xs)

-- | Keep columns not in hide set (shared by hideCols impls)
keepCols :: Int -> Vector Int -> Vector Text -> Vector Text
keepCols nCols hideIdxs names =
  V.map (\i -> maybe "" id (names V.!? i))
    (V.filter (not . (`V.elem` hideIdxs)) (V.enumFromN 0 nCols))

-- | Convert columns to tab-separated text (shared by Table toText impls)
colsToText :: Vector Text -> Vector Column -> Int -> Text
colsToText names cols nr =
  let header = joinWith names "\t"
      rowLines = V.generate nr $ \r ->
        let row = V.map (\col -> cellToRaw (columnGet col r)) cols
        in joinWith row "\t"
  in joinWith (V.cons header rowLines) "\n"

-- | Aggregate function
data Agg
  = AggCount
  | AggSum
  | AggAvg
  | AggMin
  | AggMax
  | AggStddev
  | AggDist
  deriving (Eq, Show)

instance StrEnum Agg where
  toString AggCount  = "count"
  toString AggSum    = "sum"
  toString AggAvg    = "avg"
  toString AggMin    = "min"
  toString AggMax    = "max"
  toString AggStddev = "stddev"
  toString AggDist   = "dist"
  all = V.fromList [AggCount, AggSum, AggAvg, AggMin, AggMax, AggStddev, AggDist]
  ofStringQ "count"  = Just AggCount
  ofStringQ "sum"    = Just AggSum
  ofStringQ "avg"    = Just AggAvg
  ofStringQ "min"    = Just AggMin
  ofStringQ "max"    = Just AggMax
  ofStringQ "stddev" = Just AggStddev
  ofStringQ "dist"   = Just AggDist
  ofStringQ _        = Nothing

aggShort :: Agg -> Text
aggShort a = StrEnum.toString a

aggFromStrQ :: Text -> Maybe Agg
aggFromStrQ = StrEnum.ofStringQ

-- | Table operation (single pipeline stage)
data Op
  = OpFilter Text
  | OpSort (Vector (Text, Bool))
  | OpSel (Vector Text)
  | OpExclude (Vector Text)
  | OpDerive (Vector (Text, Text))
  | OpGroup (Vector Text) (Vector (Agg, Text, Text))
  | OpTake Int
  deriving (Eq)

-- | View kind: how to render/interact (used by key mapping for context-sensitive verbs)
data ViewKind
  = VkTbl                                           -- table view
  | VkFreqV (Vector Text) Int                       -- frequency view with total distinct groups
  | VkColMeta                                       -- column metadata
  | VkFld Text Int                                  -- folder browser: path + find depth
  deriving (Eq, Show)

-- | Context string for config lookup (shared by Fzf and App dispatch)
viewKindCtxStr :: ViewKind -> Text
viewKindCtxStr (VkFreqV _ _) = "freqV"
viewKindCtxStr VkColMeta     = "colMeta"
viewKindCtxStr (VkFld _ _)   = "fld"
viewKindCtxStr VkTbl         = "tbl"

-- | Plot types and export formats
data PlotKind
  = PlotLine
  | PlotBar
  | PlotScatter
  | PlotHist
  | PlotBox
  | PlotArea
  | PlotDensity
  | PlotStep
  | PlotViolin
  deriving (Eq, Show)

instance StrEnum PlotKind where
  toString PlotLine    = "line"
  toString PlotBar     = "bar"
  toString PlotScatter = "scatter"
  toString PlotHist    = "hist"
  toString PlotBox     = "box"
  toString PlotArea    = "area"
  toString PlotDensity = "density"
  toString PlotStep    = "step"
  toString PlotViolin  = "violin"
  all = V.fromList
    [ PlotLine, PlotBar, PlotScatter, PlotHist, PlotBox
    , PlotArea, PlotDensity, PlotStep, PlotViolin ]
  ofStringQ "line"    = Just PlotLine
  ofStringQ "bar"     = Just PlotBar
  ofStringQ "scatter" = Just PlotScatter
  ofStringQ "hist"    = Just PlotHist
  ofStringQ "box"     = Just PlotBox
  ofStringQ "area"    = Just PlotArea
  ofStringQ "density" = Just PlotDensity
  ofStringQ "step"    = Just PlotStep
  ofStringQ "violin"  = Just PlotViolin
  ofStringQ _         = Nothing

data ExportFmt
  = ExportCsv
  | ExportParquet
  | ExportJson
  | ExportNdjson
  deriving (Eq, Show)

instance StrEnum ExportFmt where
  toString ExportCsv     = "csv"
  toString ExportParquet = "parquet"
  toString ExportJson    = "json"
  toString ExportNdjson  = "ndjson"
  all = V.fromList [ExportCsv, ExportParquet, ExportJson, ExportNdjson]
  ofStringQ "csv"     = Just ExportCsv
  ofStringQ "parquet" = Just ExportParquet
  ofStringQ "json"    = Just ExportJson
  ofStringQ "ndjson"  = Just ExportNdjson
  ofStringQ _         = Nothing

-- | Residual effects from pure code that can't do IO (View.update, ViewStack.update, Freq.update).
data Effect
  = EffectNone
  | EffectQuit
  | EffectFetchMore
  | EffectSort Int (Vector Int) (Vector Int) Bool
  | EffectExclude (Vector Text)
  | EffectFreq (Vector Text)
  | EffectFreqFilter (Vector Text) Int
  deriving (Eq, Show)

effectIsNone :: Effect -> Bool
effectIsNone EffectNone = True
effectIsNone _          = False

-- | Macro cmd_enum: Lean generates inductive + toStr + ToString + all + strMap + ofString?
-- Haskell port lists commands explicitly.
data Cmd
  = CmdRowInc
  | CmdRowDec
  | CmdRowPgdn
  | CmdRowPgup
  | CmdRowTop
  | CmdRowBot
  | CmdRowSel
  | CmdRowSearch
  | CmdRowFilter
  | CmdRowSearchNext
  | CmdRowSearchPrev
  | CmdColInc
  | CmdColDec
  | CmdColFirst
  | CmdColLast
  | CmdColGrp
  | CmdColHide
  | CmdColExclude
  | CmdColShiftL
  | CmdColShiftR
  | CmdSortAsc
  | CmdSortDesc
  | CmdColSplit
  | CmdColDerive
  | CmdColSearch
  | CmdPlotArea
  | CmdPlotLine
  | CmdPlotScatter
  | CmdPlotBar
  | CmdPlotBox
  | CmdPlotStep
  | CmdPlotHist
  | CmdPlotDensity
  | CmdPlotViolin
  | CmdTblMenu
  | CmdStkSwap
  | CmdStkPop
  | CmdStkDup
  | CmdTblQuit
  | CmdTblXpose
  | CmdTblDiff
  | CmdInfoTog
  | CmdPrecDec
  | CmdPrecInc
  | CmdPrecZero
  | CmdPrecMax
  | CmdCellUp
  | CmdCellDn
  | CmdHeat0
  | CmdHeat1
  | CmdHeat2
  | CmdHeat3
  | CmdMetaPush
  | CmdMetaSetKey
  | CmdMetaSelNull
  | CmdMetaSelSingle
  | CmdFreqOpen
  | CmdFreqFilter
  | CmdFolderPush
  | CmdFolderEnter
  | CmdFolderParent
  | CmdFolderDel
  | CmdFolderDepthDec
  | CmdFolderDepthInc
  | CmdTblExport
  | CmdSessSave
  | CmdSessLoad
  | CmdTblJoin
  | CmdThemeOpen
  | CmdThemePreview
  deriving (Eq, Show)

instance StrEnum Cmd where
  toString CmdRowInc         = "row.inc"
  toString CmdRowDec         = "row.dec"
  toString CmdRowPgdn        = "row.pgdn"
  toString CmdRowPgup        = "row.pgup"
  toString CmdRowTop         = "row.top"
  toString CmdRowBot         = "row.bot"
  toString CmdRowSel         = "row.sel"
  toString CmdRowSearch      = "row.search"
  toString CmdRowFilter      = "row.filter"
  toString CmdRowSearchNext  = "row.searchNext"
  toString CmdRowSearchPrev  = "row.searchPrev"
  toString CmdColInc         = "col.inc"
  toString CmdColDec         = "col.dec"
  toString CmdColFirst       = "col.first"
  toString CmdColLast        = "col.last"
  toString CmdColGrp         = "col.grp"
  toString CmdColHide        = "col.hide"
  toString CmdColExclude     = "col.exclude"
  toString CmdColShiftL      = "col.shiftL"
  toString CmdColShiftR      = "col.shiftR"
  toString CmdSortAsc        = "sort.asc"
  toString CmdSortDesc       = "sort.desc"
  toString CmdColSplit       = "col.split"
  toString CmdColDerive      = "col.derive"
  toString CmdColSearch      = "col.search"
  toString CmdPlotArea       = "plot.area"
  toString CmdPlotLine       = "plot.line"
  toString CmdPlotScatter    = "plot.scatter"
  toString CmdPlotBar        = "plot.bar"
  toString CmdPlotBox        = "plot.box"
  toString CmdPlotStep       = "plot.step"
  toString CmdPlotHist       = "plot.hist"
  toString CmdPlotDensity    = "plot.density"
  toString CmdPlotViolin     = "plot.violin"
  toString CmdTblMenu        = "tbl.menu"
  toString CmdStkSwap        = "stk.swap"
  toString CmdStkPop         = "stk.pop"
  toString CmdStkDup         = "stk.dup"
  toString CmdTblQuit        = "tbl.quit"
  toString CmdTblXpose       = "tbl.xpose"
  toString CmdTblDiff        = "tbl.diff"
  toString CmdInfoTog        = "info.tog"
  toString CmdPrecDec        = "prec.dec"
  toString CmdPrecInc        = "prec.inc"
  toString CmdPrecZero       = "prec.zero"
  toString CmdPrecMax        = "prec.max"
  toString CmdCellUp         = "cell.up"
  toString CmdCellDn         = "cell.dn"
  toString CmdHeat0          = "heat.0"
  toString CmdHeat1          = "heat.1"
  toString CmdHeat2          = "heat.2"
  toString CmdHeat3          = "heat.3"
  toString CmdMetaPush       = "meta.push"
  toString CmdMetaSetKey     = "meta.setKey"
  toString CmdMetaSelNull    = "meta.selNull"
  toString CmdMetaSelSingle  = "meta.selSingle"
  toString CmdFreqOpen       = "freq.open"
  toString CmdFreqFilter     = "freq.filter"
  toString CmdFolderPush     = "folder.push"
  toString CmdFolderEnter    = "folder.enter"
  toString CmdFolderParent   = "folder.parent"
  toString CmdFolderDel      = "folder.del"
  toString CmdFolderDepthDec = "folder.depthDec"
  toString CmdFolderDepthInc = "folder.depthInc"
  toString CmdTblExport      = "tbl.export"
  toString CmdSessSave       = "sess.save"
  toString CmdSessLoad       = "sess.load"
  toString CmdTblJoin        = "tbl.join"
  toString CmdThemeOpen      = "theme.open"
  toString CmdThemePreview   = "theme.preview"
  all = V.fromList
    [ CmdRowInc, CmdRowDec, CmdRowPgdn, CmdRowPgup, CmdRowTop, CmdRowBot
    , CmdRowSel, CmdRowSearch, CmdRowFilter, CmdRowSearchNext, CmdRowSearchPrev
    , CmdColInc, CmdColDec, CmdColFirst, CmdColLast, CmdColGrp, CmdColHide
    , CmdColExclude, CmdColShiftL, CmdColShiftR, CmdSortAsc, CmdSortDesc
    , CmdColSplit, CmdColDerive, CmdColSearch
    , CmdPlotArea, CmdPlotLine, CmdPlotScatter, CmdPlotBar, CmdPlotBox
    , CmdPlotStep, CmdPlotHist, CmdPlotDensity, CmdPlotViolin
    , CmdTblMenu, CmdStkSwap, CmdStkPop, CmdStkDup, CmdTblQuit, CmdTblXpose
    , CmdTblDiff, CmdInfoTog
    , CmdPrecDec, CmdPrecInc, CmdPrecZero, CmdPrecMax
    , CmdCellUp, CmdCellDn, CmdHeat0, CmdHeat1, CmdHeat2, CmdHeat3
    , CmdMetaPush, CmdMetaSetKey, CmdMetaSelNull, CmdMetaSelSingle
    , CmdFreqOpen, CmdFreqFilter
    , CmdFolderPush, CmdFolderEnter, CmdFolderParent, CmdFolderDel
    , CmdFolderDepthDec, CmdFolderDepthInc
    , CmdTblExport, CmdSessSave, CmdSessLoad, CmdTblJoin
    , CmdThemeOpen, CmdThemePreview
    ]
  ofStringQ s = V.find (\c -> StrEnum.toString c == s) (StrEnum.all :: Vector Cmd)

cmdPlotKindQ :: Cmd -> Maybe PlotKind
cmdPlotKindQ CmdPlotArea    = Just PlotArea
cmdPlotKindQ CmdPlotLine    = Just PlotLine
cmdPlotKindQ CmdPlotScatter = Just PlotScatter
cmdPlotKindQ CmdPlotBar     = Just PlotBar
cmdPlotKindQ CmdPlotBox     = Just PlotBox
cmdPlotKindQ CmdPlotStep    = Just PlotStep
cmdPlotKindQ CmdPlotHist    = Just PlotHist
cmdPlotKindQ CmdPlotDensity = Just PlotDensity
cmdPlotKindQ CmdPlotViolin  = Just PlotViolin
cmdPlotKindQ _              = Nothing
