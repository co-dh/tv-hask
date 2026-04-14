{-# LANGUAGE TemplateHaskell #-}
-- | Render: brick widget drawing for table, status bar, header.
-- Mirrors the layout of Tc/c/render.c:
--   y=0               header row (column names, bold+underline, type char at end)
--   y=1 .. h-4        data rows
--   y=h-3             footer (header repeated so names stay visible)
--   y=h-2             tab line
--   y=h-1             status line (colName left, stats right)
-- Columns separated by │ (U+2502); key columns are pinned at the left
-- and separated from the rest by ║ (U+2551).
module Tv.Render where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Read as TR
import qualified Data.IntSet as IS
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Bits (xor, (.&.))
import Data.Word (Word8, Word32)
import qualified Brick
import qualified Brick.AttrMap as A
import qualified Brick.Widgets.Core as C
import qualified Graphics.Vty as Vty
import Optics.Core (Lens', (%), (^.), (&), (.~), (%~))
import Optics.TH (makeLenses)
import Tv.Types
import Tv.View

-- | Brick resource names.
data Name = TableViewport | StatusBar | HeaderRow | CellGrid
  deriving (Eq, Ord, Show)

-- | Human label for a command when it's used as a prompt heading.
-- Falls back to cmdStr so unknown commands still render.
cmdLabel :: Cmd -> Text
cmdLabel = \case
  RowSearch  -> "search"
  RowFilter  -> "filter"
  ColSplit   -> "split regex"
  ColDerive  -> "derive (name = expr)"
  ColSearch  -> "goto col"
  TblExport  -> "export path"
  SessSave   -> "save session"
  SessLoad   -> "load session"
  TblJoin    -> "join keys"
  c          -> cmdStr c

-- | Application state for brick. asGrid cell text for the visible
-- window; it is rebuilt after each handler that moves the viewport or
-- mutates the underlying table (see Tv.App.refreshGrid).
data AppState = AppState
  { _asStack    :: !ViewStack
  , _asThemeIdx :: !Int
  , _asTestKeys :: ![Text]               -- remaining -c keys (empty = interactive)
  , _asMsg      :: !Text                 -- status message
  , _asErr      :: !Text                 -- error popup text
  , _asCmd      :: !Text                 -- prompt input buffer (when _asPendingCmd is Just)
                                         -- or a stashed command arg for handlers
  , _asPendingCmd :: !(Maybe Cmd)        -- Just c = prompt mode, pending dispatch of c
  , _asGrid     :: !(Vector (Vector Text))  -- [row][col] pre-fetched cells, visible window
  , _asVisRow0  :: !Int                  -- first visible row (viewport origin row)
  , _asVisCol0  :: !Int                  -- first visible col (in dispIdxs order)
  , _asVisColN  :: !Int                  -- number of visible columns starting at _asVisCol0
  , _asVisH     :: !Int                  -- terminal row capacity (lines)
  , _asVisW     :: !Int                  -- terminal column capacity (characters)
  , _asStyles   :: !(Vector (Maybe Vty.Color, Maybe Vty.Color))  -- theme styles keyed by index; Nothing = terminal default
  , _asInfoVis  :: !Bool                 -- info overlay on current column (name/type/index)
  } deriving (Show)

makeLenses ''AppState

-- | Composed path lenses for the most-reached parts of AppState. Every
-- handler that moves the cursor or mutates the head view reaches one of
-- these; preferring them avoids repeating the asStack % vsHd % ... prefix.
headView :: Lens' AppState View
headView = asStack % vsHd

headNav :: Lens' AppState NavState
headNav = headView % vNav

headTbl :: Lens' AppState TblOps
headTbl = headNav % nsTbl

-- ============================================================================
-- Attribute names (must match Tv.Theme.styleNames)
-- ============================================================================

attrCursor, attrSelRow, attrSelColCurRow, attrSelCol :: A.AttrName
attrCurRow, attrCurCol, attrDefault, attrHeader, attrGroup :: A.AttrName
attrStatus, attrStatusDim, attrBar, attrBarDim, attrError, attrErrorDim, attrHint :: A.AttrName
attrCursor        = A.attrName "cursor"
attrSelRow        = A.attrName "selRow"
attrSelColCurRow  = A.attrName "selColCurRow"
attrSelCol        = A.attrName "selCol"
attrCurRow        = A.attrName "curRow"
attrCurCol        = A.attrName "curCol"
attrDefault       = A.attrName "default"
attrHeader        = A.attrName "header"
attrGroup         = A.attrName "group"
attrStatus        = A.attrName "status"
attrStatusDim     = A.attrName "statusDim"
attrBar           = A.attrName "bar"
attrBarDim        = A.attrName "barDim"
attrError         = A.attrName "error"
attrErrorDim      = A.attrName "errorDim"
attrHint          = A.attrName "hint"

attrSep :: A.AttrName
attrSep = A.attrName "sep"

-- | Build a Brick AttrMap from the theme styles vector. Styles are indexed
-- in the order of Tv.Theme.styleNames so attrName "cursor" == styles !! 0,
-- etc. Nothing in a (fg, bg) pair means "keep terminal default" — we do
-- NOT call withForeColor/withBackColor for those. A trailing "sep" entry
-- gives column separators a dim gray foreground over the default bg.
appAttrMap :: Vector (Maybe Vty.Color, Maybe Vty.Color) -> A.AttrMap
appAttrMap styles =
  let names =
        [ "cursor", "selRow", "selColCurRow", "selCol"
        , "curRow", "curCol", "default", "header", "group"
        , "status", "statusDim", "bar", "barDim", "error", "errorDim", "hint" ]
      applyPair (mfg, mbg) =
        let a0 = Vty.defAttr
            a1 = maybe a0 (Vty.withForeColor a0) mfg
            a2 = maybe a1 (Vty.withBackColor a1) mbg
        in a2
      mkAttr i = maybe Vty.defAttr applyPair (styles V.!? i)
      pairs = [(A.attrName n, mkAttr i) | (i, n) <- zip [0 :: Int ..] names]
      -- separators: dim gray fg (ANSI 8), terminal default bg
      sepAttr = Vty.defAttr `Vty.withForeColor` Vty.ISOColor 8
  in A.attrMap Vty.defAttr ((attrSep, sepAttr) : pairs)

-- ============================================================================
-- Pure text helpers (exported for tests)
-- ============================================================================

-- Width caps: mirror Tc/c/render.c MIN_HDR_WIDTH=3, MAX_DISP_WIDTH=50.
maxColW, minColW :: Int
maxColW = 50
minColW = 3

-- | Type char appended at the last column slot in header/footer, mirroring
-- Tc/c/render.c type_char_col: # int, % float, ? bool, @ date/time, space str.
typeChar :: ColType -> Char
typeChar CTInt       = '#'
typeChar CTFloat     = '%'
typeChar CTDecimal   = '%'
typeChar CTBool      = '?'
typeChar CTDate      = '@'
typeChar CTTime      = '@'
typeChar CTTimestamp = '@'
typeChar _           = ' '

-- | Column names of the visible window (data-order via dispIdxs, sliced).
visColNames :: AppState -> Vector Text
visColNames st =
  let ns = st ^. headNav
      names = ns ^. nsTbl % tblColNames
      disp = ns ^. nsDispIdxs
      n = V.length disp
      c0 = min n (st ^. asVisCol0)
      cn = max 0 (min (st ^. asVisColN) (n - c0))
      slice = V.slice c0 cn disp
  in V.map (names V.!) slice

-- | Per-visible-column display width: max(header, cell lengths) + 2
-- (leading space + trailing space/type char), clamped to [minColW,maxColW].
visColWidths :: AppState -> Vector Int
visColWidths st =
  let names = visColNames st
      grid = st ^. asGrid
      one ci =
        let cellLen r = if ci < V.length r then T.length (r V.! ci) else 0
            dw = V.foldl' (\a r -> max a (cellLen r)) (T.length (names V.! ci)) grid
            w  = max minColW dw + 2  -- +2 for leading + trailing slot
        in min maxColW w
  in V.generate (V.length names) one

-- | Pad/clip a cell to fit a column width. Numeric → right-align, else
-- left-align. Caller adds leading/trailing space and separator.
padCell :: Int -> Bool -> Text -> Text
padCell w numeric t =
  let clipped = if T.length t > w then T.take w t else t
  in if numeric then T.justifyRight w ' ' clipped else T.justifyLeft w ' ' clipped

-- | Header text: visible column names space-joined. Used by tests.
headerText :: AppState -> Text
headerText st = T.intercalate " " (V.toList (visColNames st))

-- | Status bar text: left half = view path/tab; right half = stats.
-- | Status line matching Lean Render.lean:74 format:
-- "colName  cI/N grp=G sel=S r{row}/{total}"
statusText :: AppState -> Text
statusText st =
  let ns = st ^. headNav
      r = ns ^. nsRow % naCur
      tbl = ns ^. nsTbl
      total = tbl ^. tblTotalRows
      nc = V.length (tbl ^. tblColNames)
      ci = curColIdx ns
      colName = curColName ns
      grpN = V.length (ns ^. nsGrp)
      selN = V.length (ns ^. nsRow % naSels)
      right = "c" <> tshow ci <> "/" <> tshow nc
              <> " grp=" <> tshow grpN <> " sel=" <> tshow selN
              <> " r" <> tshow r <> "/" <> tshow total
      msg = st ^. asMsg
      cmd = st ^. asCmd
  in colName <> "  " <> right
     <> (if T.null msg then "" else " " <> msg)
     <> (if T.null cmd then "" else " :" <> cmd)
  where tshow = T.pack . show

-- | Tab line: source path or folder path.
-- | Tab line matching Lean renderTabLine: [current] for single view.
tabText :: AppState -> Text
tabText st = "[" <> st ^. headView % vPath <> "]"

-- ============================================================================
-- Heat mode: viridis-inspired background gradient (ports Tc/c/heat.c)
-- ============================================================================

-- | Per-column heat classification after scanning visible cells.
data HeatCol = HeatNum !Double !Double  -- min, max (mn < mx)
             | HeatStr                   -- categorical (FNV hash)
             | HeatNone                  -- no heat for this column

-- | Viridis-inspired 17-stop ramp of xterm-256 color indices (Tc/c/heat.c:ramp[]).
viridisRamp :: V.Vector Word8
viridisRamp = V.fromList [53, 54, 55, 61, 25, 31, 30, 36, 42, 41, 77, 113, 149, 148, 184, 190, 226]

-- | Nearest-neighbor interpolation into the viridis ramp. t ∈ [0,1].
rampColor :: Double -> Vty.Color
rampColor t =
  let n = V.length viridisRamp
      pos = max 0 (min (fromIntegral (n - 1)) (t * fromIntegral (n - 1)))
      idx = round pos :: Int
      xt = fromIntegral (viridisRamp V.! min (n - 1) idx)
  in Vty.Color240 (xt - 16)  -- Color240 maps index 0 → xterm 16

-- | FNV-1a hash of text to [0,1] (Tc/c/heat.c:str_hash01).
fnvHash01 :: Text -> Double
fnvHash01 s = fromIntegral (h Data.Bits..&. 0xFFFF) / 65535.0
  where
    h = T.foldl' step 2166136261 s :: Word32
    step !acc c = (acc `xor` fromIntegral (fromEnum c)) * 16777619

-- | Scan visible cells to classify each column for heat coloring.
heatScan :: Word8 -> AppState -> Vector HeatCol
heatScan mode st = V.generate nVis scanCol
  where
    ns = st ^. headNav
    tbl = ns ^. nsTbl
    disp = ns ^. nsDispIdxs
    grid = st ^. asGrid
    nVis = V.length (visColNames st)
    origIdx ci = let p = st ^. asVisCol0 + ci
                 in if p < V.length disp then disp V.! p else 0
    colType i = (tbl ^. tblColType) i
    scanCol ci
      | isNumeric (colType (origIdx ci)) && (mode Data.Bits..&. 1 /= 0) = scanNum ci
      | not (isNumeric (colType (origIdx ci))) && (mode Data.Bits..&. 2 /= 0) = scanStr ci
      | otherwise = HeatNone
    scanNum ci =
      let vals = V.mapMaybe (\row -> parseNum (cellAt row ci)) grid
      in if V.null vals then HeatNone
         else let mn = V.minimum vals; mx = V.maximum vals
              in if mx > mn then HeatNum mn mx else HeatNone
    scanStr ci =
      -- Only apply categorical heat if column has at least 2 distinct values
      let vals = V.map (\row -> cellAt row ci) grid
          first = if V.null vals then "" else V.head vals
      in if V.any (/= first) vals then HeatStr else HeatNone
    cellAt row ci = if ci < V.length row then row V.! ci else ""
    parseNum t = case TR.double t of Right (d, _) -> Just d; _ -> Nothing

-- | Compute heat background for a cell. Returns Nothing if heat doesn't
-- apply (mode off, cursor cell, etc.)
heatCellBg :: HeatCol -> Text -> Vty.Color
heatCellBg (HeatNum mn mx) cell = case TR.double cell of
  Right (v, _) -> rampColor ((v - mn) / (mx - mn))
  _ -> rampColor 0  -- non-numeric cell in numeric column → min color
heatCellBg HeatStr cell = rampColor (fnvHash01 cell)
heatCellBg HeatNone _ = Vty.black  -- shouldn't be called

-- | Foreground for heat-colored cells: black (xterm 16 = Color240 0).
heatFg :: Vty.Color
heatFg = Vty.Color240 0

-- ============================================================================
-- drawApp
-- ============================================================================

-- | Render the whole app. Layout matches Tc/c/render.c:
--   row 0: header (column names, bold+underline)
--   rows 1..h-4: data rows
--   row h-3: footer (header repeated)
--   row h-2: tab line
--   row h-1: status line (left: colName+msg, right: stats)
drawApp :: AppState -> [Brick.Widget Name]
drawApp st =
  [C.vBox ([headerW, gridW, footerW, tabW] ++ infoW ++ [statusW])]
  where
    ns = st ^. headNav
    tbl = ns ^. nsTbl
    disp = ns ^. nsDispIdxs
    curCol = ns ^. nsCol % naCur
    curRow = ns ^. nsRow % naCur
    rowSelSet = IS.fromList $ V.toList $ ns ^. nsRow % naSels
    colSelSet = IS.fromList $ V.toList $ ns ^. nsCol % naSels
    grpNames = ns ^. nsGrp
    nKeys = V.length grpNames
    names = visColNames st
    widths = visColWidths st
    nVis = V.length names
    hMode = ns ^. nsHeatMode
    hcols = if hMode /= 0 then heatScan hMode st else V.empty

    colType i = (tbl ^. tblColType) i
    -- Translate visible column index -> absolute dispIdxs position.
    absDispPos ci = st ^. asVisCol0 + ci
    -- Data-order column index for a visible slot.
    origIdx ci =
      let p = absDispPos ci
      in if p < V.length disp then disp V.! p else 0
    isNumCol ci = isNumeric (colType (origIdx ci))
    isGrpCol ci = absDispPos ci < nKeys
    isCurCol ci = origIdx ci == curCol
    isSelCol ci = IS.member (origIdx ci) colSelSet

    -- Separator widget: ║ immediately after the last key column, │ otherwise.
    -- Rightmost column omits its trailing separator.
    sep ci
      | ci + 1 >= nVis = C.str ""
      | isGrpCol ci && not (isGrpCol (ci + 1)) = C.withAttr attrGroup (C.str "║")
      | otherwise = C.withAttr attrSep (C.str "│")

    -- Header cell: " <name padded> <typeChar>" with bold+underline style.
    -- width >= 3 since minColW=3 and we +2 for the two slots.
    headerCell ci name =
      let w = widths V.! ci
          inner = max 0 (w - 2)
          tc = T.singleton (typeChar (colType (origIdx ci)))
          nameTxt = T.take inner (T.justifyLeft inner ' ' name)
          txt = " " <> nameTxt <> tc
          a | isCurCol ci = attrCursor
            | isSelCol ci = attrSelCol
            | isGrpCol ci = attrGroup
            | otherwise   = attrHeader
      in C.withAttr a (C.txt txt)

    headerRow = C.hBox $ concat
      [ [headerCell ci (names V.! ci), sep ci] | ci <- [0 .. nVis - 1] ]
    headerW = headerRow
    footerW = headerRow

    -- Data cell: " <value padded>[space]" within width w. Right-align for
    -- numerics. Background is chosen by (row, col) selection/cursor state.
    -- Heat mode overrides background for non-cursor/non-selection cells
    -- (Tc/c/heat.c: suppresses for STYLE_CURSOR/SEL_ROW/SEL_CUR).
    dataCell absRow ci cell =
      let w = widths V.! ci
          inner = max 0 (w - 2)
          padded = padCell inner (isNumCol ci) cell
          txt = " " <> padded <> " "
          isSelR = IS.member absRow rowSelSet
          isCurR = absRow == curRow
          isSelC = isSelCol ci
          isCurC = isCurCol ci
          -- Style index determines whether heat applies (mirrors C enum)
          suppressHeat = (isCurR && isCurC) || isSelR || (isSelC && isCurR)
          hc = if ci < V.length hcols then hcols V.! ci else HeatNone
          useHeat = hMode /= 0 && not suppressHeat && not (T.null cell)
                  && case hc of HeatNone -> False; _ -> True
      in if useHeat
         then let bg = heatCellBg hc cell
                  attr = Vty.defAttr `Vty.withForeColor` heatFg `Vty.withBackColor` bg
              in C.raw (Vty.text attr (TL.fromStrict txt))
         else let a | isCurR && isCurC           = attrCursor
                    | isSelR                     = attrSelRow
                    | isSelC && isCurR           = attrSelColCurRow
                    | isSelC                     = attrSelCol
                    | isCurR                     = attrCurRow
                    | isCurC                     = attrCurCol
                    | isGrpCol ci                = attrGroup
                    | otherwise                  = attrDefault
              in C.withAttr a (C.txt txt)

    mkRow rIdx row =
      let absRow = st ^. asVisRow0 + rIdx
      in C.hBox $ concat
           [ [dataCell absRow ci (if ci < V.length row then row V.! ci else ""), sep ci]
           | ci <- [0 .. nVis - 1] ]

    gridW = C.vBox $ V.toList $ V.imap mkRow (st ^. asGrid)

    -- Tab line at h-2. bar attribute gives it its own colour.
    tabW = C.withAttr attrBar (C.txt (tabText st))

    -- Optional info overlay drawn just above the status line. Enabled by
    -- the InfoTog command. Shows current column's name/type/index so the
    -- user can inspect metadata without opening the full colMeta view.
    infoW
      | st ^. asInfoVis =
          let colNames = tbl ^. tblColNames
              nCols = V.length colNames
              nm = if curCol < nCols then colNames V.! curCol else ""
              ty = if curCol < nCols then colTypeName' (colType curCol) else ""
              line = "info: " <> nm <> " : " <> ty
                     <> "  [col " <> tshow (curCol + 1) <> "/" <> tshow nCols <> "]"
          in [C.withAttr attrHint (C.txt line)]
      | otherwise = []
    colTypeName' = \case
      CTInt -> "int"; CTFloat -> "float"; CTDecimal -> "decimal"
      CTStr -> "str"; CTDate -> "date"; CTTime -> "time"
      CTTimestamp -> "timestamp"; CTBool -> "bool"; CTOther -> "other"

    -- Status line at h-1: colName on left, stats on right, padded in between.
    statsRight =
      let c = ns ^. nsCol % naCur
          colNames = tbl ^. tblColNames
          nCols = V.length colNames
          r = ns ^. nsRow % naCur
          n = tbl ^. tblNRows
          nSel = IS.size rowSelSet
      in "c" <> tshow c <> "/" <> tshow nCols
         <> " grp=" <> tshow nKeys
         <> " sel=" <> tshow nSel
         <> " r" <> tshow r <> "/" <> tshow n
    colName =
      let names' = tbl ^. tblColNames
      in if curCol < V.length names' then names' V.! curCol else ""
    msg = st ^. asMsg
    cmd = st ^. asCmd
    msgPart = if T.null msg then "" else " " <> msg
    cmdPart = if T.null cmd then "" else " :" <> cmd
    leftTxt = colName <> msgPart <> cmdPart
    w = st ^. asVisW
    gap = max 1 (w - T.length leftTxt - T.length statsRight)
    -- When asPendingCmd Just the whole status line becomes an input
    -- prompt: "{label}: {buffer}▌" with a block cursor so the user can
    -- see what they're typing. Otherwise the normal colName/stats line.
    promptLine label =
      let body = label <> ": " <> cmd <> "\x258C"
          pad  = max 0 (w - T.length body)
      in body <> T.replicate pad " "
    statusW = case st ^. asPendingCmd of
      Just c  -> C.withAttr attrStatus (C.txt (promptLine (cmdLabel c)))
      Nothing -> C.withAttr attrStatus
               $ C.txt (leftTxt <> T.replicate gap " " <> statsRight)

    tshow :: Int -> Text
    tshow = T.pack . show
