{-
  Core types: ColType, RenderCtx, Cmd, Effect, etc.
-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.Types
  ( -- * StrEnum
    StrEnum(..)
    -- * Utility
  , joinWith
  , toggle
  , headD
  , getD
    -- * ColCache
  , ColCache(..)
    -- * Column types
  , ColType(..)
  , ofString
  , isNumeric
  , isTime
    -- * PRQL / SQL helpers
  , toPrql
  , escSql
  , pctBar
    -- * Render context
  , RenderCtx(..)
    -- * Filter
  , filterPrql
  , numType
    -- * Table helpers
  , filterPrompt
  , keepCols
  , colText
    -- * Agg
  , Agg(..)
    -- * Op
  , Op(..)
    -- * ViewKind
  , ViewKind(..)
    -- * PlotKind / ExportFmt
  , PlotKind(..)
  , ExportFmt(..)
    -- * Effect
  , Effect(..)
  , noEffect
    -- * Cmd
  , Cmd(..)
  , plotKind
  ) where

import qualified Data.Aeson as A
import qualified Data.Aeson.Types as A
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8, Word32)
import Optics.TH (makeFieldLabelsNoPrefix)
import Data.Hashable (Hashable(..))
import Data.Maybe (fromMaybe, listToMaybe)
-- | Enum-ish types that have a conventional string name. Defaults let
-- types with payloads (e.g. 'ViewKind') provide only 'toString'; plain
-- enums override 'all' and 'ofStringQ' for roundtrip.
class StrEnum a where
  toString  :: a -> Text
  all       :: Vector a
  all = V.empty
  ofStringQ :: Text -> Maybe a
  ofStringQ _ = Nothing

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
    , ColTypeDate, ColTypeTime, ColTypeTimestamp, ColTypeBool, ColTypeOther
    ]
  ofStringQ "int"       = Just ColTypeInt
  ofStringQ "float"     = Just ColTypeFloat
  ofStringQ "decimal"   = Just ColTypeDecimal
  ofStringQ "str"       = Just ColTypeStr
  ofStringQ "date"      = Just ColTypeDate
  ofStringQ "time"      = Just ColTypeTime
  ofStringQ "timestamp" = Just ColTypeTimestamp
  ofStringQ "bool"      = Just ColTypeBool
  ofStringQ _           = Nothing

-- | Total parse: falls back to 'ColTypeOther' on any unknown string.
-- Used at FFI boundaries where input is user/DB-supplied and must not fail.
ofString :: Text -> ColType
ofString = fromMaybe ColTypeOther . ofStringQ

isNumeric :: ColType -> Bool
isNumeric ColTypeInt     = True
isNumeric ColTypeFloat   = True
isNumeric ColTypeDecimal = True
isNumeric _              = False

isTime :: ColType -> Bool
isTime ColTypeTime      = True
isTime ColTypeTimestamp = True
isTime ColTypeDate      = True
isTime _                = False

-- | Toggle element in array (add if absent, remove if present)
toggle :: Eq a => Vector a -> a -> Vector a
toggle arr x =
  if V.elem x arr then V.filter (/= x) arr else V.snoc arr x

-- | Safe head with default
headD :: a -> [a] -> a
headD d []    = d
headD _ (x:_) = x

-- | Safe list index with default
getD :: [a] -> Int -> a -> a
getD xs i d = fromMaybe d $ listToMaybe $ drop i xs

-- | Path + per-column cache. Key kind @k@ varies (Text column name vs Int
--   column index) depending on caller; value @v@ is whatever was computed
--   against that (path, column) pair.
data ColCache k v = ColCache
  { cachedPath :: !Text
  , cachedCol  :: !k
  , cachedVal  :: !v
  } deriving (Eq, Show)

-- | Format raw text as PRQL literal based on column type.
-- Raw text means: ints as "1234567" (no commas), floats as "1.234", strings as-is.
toPrql :: ColType -> Text -> Text
toPrql _ t | T.null t = "null"
toPrql typ t
  | isNumeric typ = t  -- bare numeric literal
  | typ == ColTypeBool   = t  -- true/false are bare
  | otherwise = "'" <> T.replace "'" "''" t <> "'"

-- | Escape single quotes for SQL string literals
escSql :: Text -> Text
escSql s = T.replace "'" "''" s

-- | Compute pct and bar from count data (for freq → fromArrays).
pctBar :: Vector Int64 -> (Vector Double, Vector Text)
pctBar cntData =
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
  , selIdxs :: Vector Int
  , rowSels    :: Vector Int
  , hiddenIdxs :: Vector Int
  , styles     :: Vector Word32
  , prec       :: Int
  , widthAdj   :: Int
  , heatMode   :: Word8  -- 0=off, 1=numeric, 2=categorical, 3=both
  , sparklines :: Vector Text
  }
makeFieldLabelsNoPrefix ''RenderCtx

-- | Build PRQL filter expression from fzf result
-- With --print-query: line 0 = query, lines 1+ = selections
filterPrql :: Text -> Vector Text -> Text -> Bool -> Text
filterPrql col vals result numeric =
  let lines_ = V.fromList (filter (not . T.null) (T.splitOn "\n" result))
      input = fromMaybe "" (lines_ V.!? 0)
      fromHints = V.filter (`V.elem` vals) (V.slice 1 (max 0 (V.length lines_ - 1)) lines_)
      selected =
        if V.elem input vals && not (V.elem input fromHints)
          then V.cons input fromHints
          else fromHints
      q v = if numeric then v else "'" <> v <> "'"
  in if V.length selected == 1
       then col <> " == " <> q (fromMaybe "" (selected V.!? 0))
     else if V.length selected > 1
       then "(" <> joinWith (V.map (\v -> col <> " == " <> q v) selected) " || " <> ")"
     else if not (T.null input)
       then input
       else ""

-- compat shim — prefer ColType.isNumeric on typed values
numType :: Text -> Bool
numType t = isNumeric (ofString t)

-- | Filter prompt hint (PRQL examples for the given column/type)
filterPrompt :: Text -> Text -> Text
filterPrompt col typ =
  let eg = if numType typ
             then "e.g. " <> col <> " > 5,  " <> col <> " >= 10 && " <> col <> " < 100"
             else "e.g. " <> col <> " == 'USD',  " <> col <> " ~= 'pattern'"
  in "PRQL filter on " <> col <> " (" <> typ <> "):  " <> eg

-- | Keep columns not in hide set (shared by hideCols impls)
keepCols :: Int -> Vector Int -> Vector Text -> Vector Text
keepCols nCols hideIdxs names =
  V.map (\i -> fromMaybe "" $ names V.!? i)
    (V.filter (not . (`V.elem` hideIdxs)) (V.enumFromN 0 nCols))

-- | Convert columns to tab-separated text (shared by Table toText impls)
colText :: Vector Text -> Vector (Vector Text) -> Int -> Text
colText names cols nr =
  let header = joinWith names "\t"
      rowLines = V.generate nr $ \r ->
        let row = V.map (\col -> fromMaybe "" (col V.!? r)) cols
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

-- | JSON decoder for Op (kept here so Session.hs stays orphan-free under -Worphans).
instance A.FromJSON Op where
  parseJSON = A.withObject "Op" $ \o -> do
    t <- o A..: "type"
    case (t :: Text) of
      "filter"  -> OpFilter <$> o A..: "expr"
      "sort"    -> OpSort <$> (o A..: "cols" >>= parseSortCols)
      "sel"     -> OpSel <$> o A..: "cols"
      "exclude" -> OpExclude <$> o A..: "cols"
      "derive"  -> OpDerive <$> (o A..: "bindings" >>= parseBindings)
      "group"   -> do
        keys    <- o A..: "keys"
        rawAggs <- o A..: "aggs" :: A.Parser (Vector (Vector A.Value))
        aggs    <- V.mapM parseAggTriple rawAggs
        pure $ OpGroup keys aggs
      "take"    -> OpTake <$> o A..: "n"
      other     -> fail $ "unknown op type: " ++ T.unpack other
    where
      parseAgg :: A.Value -> A.Parser Agg
      parseAgg v = do
        s <- A.parseJSON v
        case ofStringQ s of
          Just a  -> pure a
          Nothing -> fail "unknown agg"
      parseSortCols :: Vector A.Value -> A.Parser (Vector (Text, Bool))
      parseSortCols = V.mapM $ \v -> case v of
        A.Array a | V.length a >= 2 ->
          (,) <$> A.parseJSON (a V.! 0) <*> A.parseJSON (a V.! 1)
        _ -> fail "sort pair expected"
      parseBindings :: Vector A.Value -> A.Parser (Vector (Text, Text))
      parseBindings = V.mapM $ \v -> case v of
        A.Array a | V.length a >= 2 ->
          (,) <$> A.parseJSON (a V.! 0) <*> A.parseJSON (a V.! 1)
        _ -> fail "binding pair expected"
      parseAggTriple :: Vector A.Value -> A.Parser (Agg, Text, Text)
      parseAggTriple a
        | V.length a >= 3 = do
            fn   <- parseAgg (a V.! 0)
            name <- A.parseJSON (a V.! 1)
            col  <- A.parseJSON (a V.! 2)
            pure (fn, name, col)
        | otherwise = fail "agg triple expected"

-- | View kind: how to render/interact (used by key mapping for context-sensitive verbs)
data ViewKind
  = VkTbl                                           -- table view
  | VkFreqV (Vector Text) Int                       -- frequency view with total distinct groups
  | VkColMeta                                       -- column metadata
  | VkFld Text Int                                  -- folder browser: path + find depth
  deriving (Eq, Show)

-- | JSON decoder for ViewKind (kept here so Session.hs stays orphan-free under -Worphans).
instance A.FromJSON ViewKind where
  parseJSON = A.withObject "ViewKind" $ \o -> do
    kind <- o A..: "kind" :: A.Parser Text
    case kind of
      "freqV"   -> VkFreqV <$> o A..: "cols" <*> o A..: "total"
      "colMeta" -> pure VkColMeta
      "fld"     -> VkFld <$> o A..: "path" <*> o A..: "depth"
      _         -> pure VkTbl

-- | Context string for config lookup (shared by Fzf and App dispatch).
-- ViewKind carries payloads (cols/depth), so 'ofStringQ' and 'all' aren't
-- meaningful here — only 'toString' is used, by 'CmdConfig.keyLookup'.
instance StrEnum ViewKind where
  toString (VkFreqV _ _) = "freqV"
  toString VkColMeta     = "colMeta"
  toString (VkFld _ _)   = "fld"
  toString VkTbl         = "tbl"

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

noEffect :: Effect -> Bool
noEffect EffectNone = True
noEffect _          = False

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
  ofStringQ s = V.find (\c -> toString c == s) (Tv.Types.all :: Vector Cmd)

-- Hash via canonical string form (StrEnum). Defined here (not CmdConfig) to
-- avoid an orphan instance.
instance Hashable Cmd where
  hashWithSalt s c = hashWithSalt s (toString c :: Text)

plotKind :: Cmd -> Maybe PlotKind
plotKind CmdPlotArea    = Just PlotArea
plotKind CmdPlotLine    = Just PlotLine
plotKind CmdPlotScatter = Just PlotScatter
plotKind CmdPlotBar     = Just PlotBar
plotKind CmdPlotBox     = Just PlotBox
plotKind CmdPlotStep    = Just PlotStep
plotKind CmdPlotHist    = Just PlotHist
plotKind CmdPlotDensity = Just PlotDensity
plotKind CmdPlotViolin  = Just PlotViolin
plotKind _              = Nothing

