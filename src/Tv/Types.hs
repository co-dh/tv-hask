{-# LANGUAGE DeriveAnyClass #-}
-- | Core types: Cell, Column, ColType, Cmd, Op, Effect, TblOps (record-of-functions),
-- RenderCtx, NavAxis, NavState. Uses lenses for all record types.
module Tv.Types where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Int (Int64)
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Hashable (Hashable)
import Data.Word (Word8, Word32, Word64)
import Foreign.ForeignPtr (ForeignPtr)
import Foreign.Ptr (Ptr)
import GHC.Generics (Generic)
import Optics.TH (makeLenses)

-- ============================================================================
-- ColType
-- ============================================================================

data ColType = CTInt | CTFloat | CTDecimal | CTStr | CTDate | CTTime | CTTimestamp | CTBool | CTOther
  deriving (Eq, Ord, Show, Enum, Bounded)

colTypeFromText :: Text -> ColType
colTypeFromText = \case
  "int" -> CTInt; "float" -> CTFloat; "decimal" -> CTDecimal; "str" -> CTStr
  "date" -> CTDate; "time" -> CTTime; "timestamp" -> CTTimestamp; "bool" -> CTBool
  _ -> CTOther

isNumeric :: ColType -> Bool
isNumeric CTInt = True; isNumeric CTFloat = True; isNumeric CTDecimal = True; isNumeric _ = False

isTime :: ColType -> Bool
isTime CTTime = True; isTime CTTimestamp = True; isTime CTDate = True; isTime _ = False

-- ============================================================================
-- Cell & Column
-- ============================================================================

data Cell = CNull | CInt !Int64 | CFloat !Double | CStr !Text | CBool !Bool
  deriving (Eq, Show)

cellToRaw :: Cell -> Text
cellToRaw CNull = ""; cellToRaw (CInt n) = T.pack (show n); cellToRaw (CFloat f) = T.pack (show f)
cellToRaw (CStr s) = s; cellToRaw (CBool b) = if b then "true" else "false"

cellToPrql :: Cell -> Text
cellToPrql CNull = "null"; cellToPrql (CInt n) = T.pack (show n); cellToPrql (CFloat f) = T.pack (show f)
cellToPrql (CStr s) = "'" <> T.replace "'" "''" s <> "'"; cellToPrql (CBool b) = if b then "true" else "false"

-- | Opaque handle to one DuckDB @duckdb_data_chunk@. Defined here (rather
-- than in Tv.Data.DuckDB) to break an import cycle: 'Column' stores a list
-- of these and lives in Tv.Types, but Tv.Data.DuckDB also imports from
-- Tv.Types. See Tv.Data.DuckDB for the functions that construct/consume
-- data chunks and perform zero-copy reads.
--
-- Each chunk carries a ForeignPtr with a @duckdb_destroy_data_chunk@
-- finalizer plus a record of the per-column raw/logical type metadata
-- needed to peek into its buffers.
data DataChunk = DataChunk
  { _dcFp       :: !(ForeignPtr ())          -- underlying duckdb_data_chunk
  , _dcRawTypes :: !(Vector Int)             -- DuckDBType (unwrapped as Int) per column
  , _dcColTypes :: !(Vector ColType)         -- Haskell-side type classification
  }

instance Show DataChunk where show _ = "<DataChunk>"

-- | Zero-copy column handle. Holds the list of 'DataChunk's that back a
-- result column, plus the column's index within each chunk and its type.
-- Readers (see Tv.Data.DuckDB) compute @(chunkIdx, rowIdx)@ and peek into
-- the vector data pointer directly — no materialization.
data Column = Column
  { _colChunks :: ![DataChunk]
  , _colIndex  :: !Int
  , _colType   :: !ColType
  , _colName   :: !Text
  }

instance Show Column where show c = "<Column " <> T.unpack (_colName c) <> ">"

-- Row-level readers and size helpers live in Tv.Data.DuckDB because they
-- need to call into libduckdb (duckdb_data_chunk_get_size / vector_get_data).
-- This keeps Tv.Types pure of C FFI.

-- ============================================================================
-- Cmd (all commands — Haskell ADT replaces Lean cmd_enum macro)
-- ============================================================================

data Cmd
  = RowInc | RowDec | RowPgdn | RowPgup | RowTop | RowBot | RowSel
  | RowSearch | RowFilter | RowSearchNext | RowSearchPrev
  | ColInc | ColDec | ColFirst | ColLast | ColGrp | ColHide | ColExclude
  | ColShiftL | ColShiftR | ColSplit | ColDerive | ColSearch
  | SortAsc | SortDesc
  | PlotArea | PlotLine | PlotScatter | PlotBar | PlotBox | PlotStep | PlotHist | PlotDensity | PlotViolin
  | TblMenu | StkSwap | StkPop | StkDup | TblQuit | TblXpose | TblDiff
  | InfoTog | PrecDec | PrecInc | PrecZero | PrecMax
  | CellUp | CellDn
  | Heat0 | Heat1 | Heat2 | Heat3
  | MetaPush | MetaSetKey | MetaSelNull | MetaSelSingle
  | FreqOpen | FreqFilter
  | FolderPush | FolderEnter | FolderParent | FolderDel | FolderDepthDec | FolderDepthInc
  | TblExport | SessSave | SessLoad | TblJoin | ThemeOpen | ThemePreview
  deriving (Eq, Ord, Show, Enum, Bounded, Generic, Hashable)

cmdStr :: Cmd -> Text
cmdStr = \case
  RowInc -> "row.inc"; RowDec -> "row.dec"; RowPgdn -> "row.pgdn"; RowPgup -> "row.pgup"
  RowTop -> "row.top"; RowBot -> "row.bot"; RowSel -> "row.sel"
  RowSearch -> "row.search"; RowFilter -> "row.filter"
  RowSearchNext -> "row.searchNext"; RowSearchPrev -> "row.searchPrev"
  ColInc -> "col.inc"; ColDec -> "col.dec"; ColFirst -> "col.first"; ColLast -> "col.last"
  ColGrp -> "col.grp"; ColHide -> "col.hide"; ColExclude -> "col.exclude"
  ColShiftL -> "col.shiftL"; ColShiftR -> "col.shiftR"
  ColSplit -> "col.split"; ColDerive -> "col.derive"; ColSearch -> "col.search"
  SortAsc -> "sort.asc"; SortDesc -> "sort.desc"
  PlotArea -> "plot.area"; PlotLine -> "plot.line"; PlotScatter -> "plot.scatter"
  PlotBar -> "plot.bar"; PlotBox -> "plot.box"; PlotStep -> "plot.step"
  PlotHist -> "plot.hist"; PlotDensity -> "plot.density"; PlotViolin -> "plot.violin"
  TblMenu -> "tbl.menu"; StkSwap -> "stk.swap"; StkPop -> "stk.pop"; StkDup -> "stk.dup"
  TblQuit -> "tbl.quit"; TblXpose -> "tbl.xpose"; TblDiff -> "tbl.diff"
  InfoTog -> "info.tog"; PrecDec -> "prec.dec"; PrecInc -> "prec.inc"
  PrecZero -> "prec.zero"; PrecMax -> "prec.max"
  CellUp -> "cell.up"; CellDn -> "cell.dn"
  Heat0 -> "heat.0"; Heat1 -> "heat.1"; Heat2 -> "heat.2"; Heat3 -> "heat.3"
  MetaPush -> "meta.push"; MetaSetKey -> "meta.setKey"
  MetaSelNull -> "meta.selNull"; MetaSelSingle -> "meta.selSingle"
  FreqOpen -> "freq.open"; FreqFilter -> "freq.filter"
  FolderPush -> "folder.push"; FolderEnter -> "folder.enter"
  FolderParent -> "folder.parent"; FolderDel -> "folder.del"
  FolderDepthDec -> "folder.depthDec"; FolderDepthInc -> "folder.depthInc"
  TblExport -> "tbl.export"; SessSave -> "sess.save"; SessLoad -> "sess.load"
  TblJoin -> "tbl.join"; ThemeOpen -> "theme.open"; ThemePreview -> "theme.preview"

cmdFromStr :: Text -> Maybe Cmd
cmdFromStr t = Map.lookup t _cmdStrMap
  where _cmdStrMap = Map.fromList [(cmdStr c, c) | c <- [minBound..maxBound]]

-- ============================================================================
-- PlotKind, ExportFmt, Agg
-- ============================================================================

data PlotKind = PKLine | PKBar | PKScatter | PKHist | PKBox | PKArea | PKDensity | PKStep | PKViolin
  deriving (Eq, Ord, Show, Enum, Bounded)

cmdPlotKind :: Cmd -> Maybe PlotKind
cmdPlotKind PlotArea = Just PKArea; cmdPlotKind PlotLine = Just PKLine
cmdPlotKind PlotScatter = Just PKScatter; cmdPlotKind PlotBar = Just PKBar
cmdPlotKind PlotBox = Just PKBox; cmdPlotKind PlotStep = Just PKStep
cmdPlotKind PlotHist = Just PKHist; cmdPlotKind PlotDensity = Just PKDensity
cmdPlotKind PlotViolin = Just PKViolin; cmdPlotKind _ = Nothing

data ExportFmt = EFCsv | EFParquet | EFJson | EFNdjson
  deriving (Eq, Ord, Show, Enum, Bounded)

data Agg = ACount | ASum | AAvg | AMin | AMax | AStddev | ADist
  deriving (Eq, Ord, Show, Enum, Bounded)

aggStr :: Agg -> Text
aggStr ACount = "count"; aggStr ASum = "sum"; aggStr AAvg = "avg"
aggStr AMin = "min"; aggStr AMax = "max"; aggStr AStddev = "stddev"; aggStr ADist = "dist"

-- ============================================================================
-- Op, ViewKind, Effect
-- ============================================================================

data Op
  = OpFilter !Text
  | OpSort !(Vector (Text, Bool))  -- (colName, ascending)
  | OpSel !(Vector Text)
  | OpExclude !(Vector Text)
  | OpDerive !(Vector (Text, Text))  -- (name, expr)
  | OpGroup !(Vector Text) !(Vector (Agg, Text, Text))  -- keys, (agg, col, alias)
  | OpTake !Int
  deriving (Eq, Show)

data ViewKind
  = VTbl
  | VFreq !(Vector Text) !Int  -- cols, total distinct
  | VColMeta
  | VFld !Text !Int  -- path, find depth
  | VViewFile          -- viewFile fallback (raw text, no table chrome)
  deriving (Eq, Show)

vkCtxStr :: ViewKind -> Text
vkCtxStr VTbl = "tbl"; vkCtxStr (VFreq _ _) = "freqV"; vkCtxStr VColMeta = "colMeta"
vkCtxStr (VFld _ _) = "fld"; vkCtxStr VViewFile = "tbl"

data Effect
  = ENone | EQuit | EFetchMore
  | ESort !Int !(Vector Int) !(Vector Int) !Bool  -- colIdx, sels, grp, asc
  | EExclude !(Vector Text)
  | EFreq !(Vector Text)
  | EFreqFilter !(Vector Text) !Int  -- cols, row
  deriving (Eq, Show)

-- ============================================================================
-- RenderCtx
-- ============================================================================

data RenderCtx = RenderCtx
  { _rcWidths     :: !(Vector Int)
  , _rcDispIdxs   :: !(Vector Int)
  , _rcNGrp       :: !Int
  , _rcR0         :: !Int
  , _rcR1         :: !Int
  , _rcCurRow     :: !Int
  , _rcCurCol     :: !Int
  , _rcMoveDir    :: !Int
  , _rcSelColIdxs :: !(Vector Int)
  , _rcRowSels    :: !(Vector Int)
  , _rcHiddenIdxs :: !(Vector Int)
  , _rcStyles     :: !(Vector Word32)
  , _rcPrec       :: !Int
  , _rcWidthAdj   :: !Int
  , _rcHeatMode   :: !Word8  -- 0=off, 1=numeric, 2=categorical, 3=both
  , _rcSparklines :: !(Vector Text)
  } deriving (Show)

makeLenses ''RenderCtx

-- ============================================================================
-- TblOps: record-of-functions (replaces Lean typeclass)
-- ============================================================================

-- | Table interface as a record of functions. Constructors (mkMemOps, mkDbOps, etc.)
-- fill in the fields. Mockable, composable, no typeclass dispatch overhead.
data TblOps = TblOps
  { _tblNRows       :: !Int
  , _tblColNames    :: !(Vector Text)
  , _tblTotalRows   :: !Int
  , _tblQueryOps    :: !(Vector Op)  -- PRQL ops chain (for replay/tab display)
  , _tblFilter      :: Text -> IO (Maybe TblOps)
  , _tblDistinct    :: Int -> IO (Vector Text)
  , _tblFindRow     :: Int -> Text -> Int -> Bool -> IO (Maybe Int)
  , _tblRender      :: RenderCtx -> IO (Vector Int)
  , _tblGetCols     :: Vector Int -> Int -> Int -> IO (Vector Column)
  , _tblColType     :: Int -> ColType
  , _tblBuildFilter :: Text -> Vector Text -> Text -> Bool -> Text
  , _tblFilterPrompt :: Text -> Text -> Text
  , _tblPlotExport  :: Text -> Text -> Maybe Text -> Bool -> Int -> Int -> IO (Maybe (Vector Text))
  , _tblCellStr     :: Int -> Int -> IO Text
  , _tblFetchMore   :: IO (Maybe TblOps)
  , _tblHideCols    :: Vector Int -> IO TblOps
  , _tblSortBy      :: Vector Int -> Bool -> IO TblOps
  }

makeLenses ''TblOps

-- Show instance for TblOps (can't derive, has function fields). Must be
-- declared before NavState because NavState's deriving Show looks it up.
instance Show TblOps where show _ = "<TblOps>"

-- ============================================================================
-- NavAxis, NavState
-- ============================================================================

data NavAxis = NavAxis
  { _naCur  :: !Int
  , _naSels :: !(Vector Int)
  } deriving (Show)

makeLenses ''NavAxis

mkAxis :: NavAxis
mkAxis = NavAxis 0 V.empty

data NavState = NavState
  { _nsTbl      :: !TblOps
  , _nsRow      :: !NavAxis
  , _nsCol      :: !NavAxis
  , _nsGrp      :: !(Vector Text)   -- grouped column names
  , _nsHidden   :: !(Vector Text)   -- hidden column names
  , _nsDispIdxs :: !(Vector Int)    -- cached display order
  , _nsVkind    :: !ViewKind
  , _nsSearch   :: !Text            -- current search string
  , _nsPrecAdj  :: !Int
  , _nsWidthAdj :: !Int
  , _nsHeatMode :: !Word8
  } deriving (Show)

makeLenses ''NavState

-- ============================================================================
-- Navigation helpers
-- ============================================================================

clamp :: Int -> Int -> Int -> Int
clamp lo hi v = max lo (min v (hi - 1))

-- | Move cursor by delta, clamped to [0, n)
axisMove :: Int -> Int -> NavAxis -> NavAxis
axisMove n d ax = ax { _naCur = clamp 0 n (_naCur ax + d) }

-- | Toggle element in vector (add if absent, remove if present)
vToggle :: Int -> Vector Int -> Vector Int
vToggle x xs = if V.elem x xs then V.filter (/= x) xs else V.snoc xs x

-- | Display order: group columns first, then rest
dispOrder :: Vector Text -> Vector Text -> Vector Int
dispOrder grp names =
  let n = V.length names
      isGrp i = V.elem (names V.! i) grp
      grpIdxs = V.filter isGrp (V.enumFromN 0 n)
      -- sort group indices by their position in grp vector
      grpSorted = sortByGrpPos grp names grpIdxs
      rest = V.filter (not . isGrp) (V.enumFromN 0 n)
  in grpSorted V.++ rest
  where
    sortByGrpPos g ns idxs = V.fromList $
      let pairs = [(i, V.elemIndex (ns V.! i) g) | i <- V.toList idxs]
      in map fst $ sortOn snd pairs
    sortOn f = foldr (insertBy (compare `on` f)) []
    insertBy cmp x [] = [x]
    insertBy cmp x (y:ys) = case cmp x y of GT -> y : insertBy cmp x ys; _ -> x : y : ys
    on f g a b = f (g a) (g b)

-- | Current column index in data order (via dispIdxs)
curColIdx :: NavState -> Int
curColIdx ns = _nsDispIdxs ns V.! _naCur (_nsCol ns)

-- | Current column name
curColName :: NavState -> Text
curColName ns = _tblColNames (_nsTbl ns) V.! curColIdx ns

-- | Number of group columns
nGrp :: NavState -> Int
nGrp = V.length . _nsGrp

-- | Escape single quotes for SQL
escSql :: Text -> Text
escSql = T.replace "'" "''"
