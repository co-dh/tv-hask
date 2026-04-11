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
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Brick
import qualified Brick.AttrMap as A
import qualified Brick.Widgets.Core as C
import qualified Graphics.Vty as Vty
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

-- | Application state for brick. asGrid caches cell text for the visible
-- window; it is rebuilt after each handler that moves the viewport or
-- mutates the underlying table (see Tv.App.refreshGrid).
data AppState = AppState
  { asStack    :: !ViewStack
  , asThemeIdx :: !Int
  , asTestKeys :: ![Text]               -- remaining -c keys (empty = interactive)
  , asMsg      :: !Text                 -- status message
  , asErr      :: !Text                 -- error popup text
  , asCmd      :: !Text                 -- prompt input buffer (when asPendingCmd is Just)
                                        -- or a stashed command arg for handlers
  , asPendingCmd :: !(Maybe Cmd)        -- Just c = prompt mode, pending dispatch of c
  , asGrid     :: !(Vector (Vector Text))  -- [row][col] pre-fetched cells, visible window
  , asVisRow0  :: !Int                  -- first visible row (viewport origin row)
  , asVisCol0  :: !Int                  -- first visible col
  , asVisH     :: !Int                  -- visible row count
  , asVisW     :: !Int                  -- visible col count
  , asStyles   :: !(Vector (Maybe Vty.Color, Maybe Vty.Color))  -- theme styles keyed by index; Nothing = terminal default
  , asInfoVis  :: !Bool                 -- info overlay on current column (name/type/index)
  } deriving (Show)

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
  let ns = _vNav $ _vsHd $ asStack st
      names = _tblColNames (_nsTbl ns)
      disp = _nsDispIdxs ns
      slice = V.slice (min (V.length disp) (asVisCol0 st))
                      (min (asVisW st) (max 0 (V.length disp - asVisCol0 st)))
                      disp
  in V.map (names V.!) slice

-- | Per-visible-column display width: max(header, cell lengths) + 2
-- (leading space + trailing space/type char), clamped to [minColW,maxColW].
visColWidths :: AppState -> Vector Int
visColWidths st =
  let names = visColNames st
      grid = asGrid st
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
statusText :: AppState -> Text
statusText st =
  let ns = _vNav $ _vsHd $ asStack st
      r = _naCur (_nsRow ns)
      n = _tblNRows (_nsTbl ns)
      c = _naCur (_nsCol ns)
      base = "[row " <> tshow (r + 1) <> "/" <> tshow n <> " | col " <> tshow (c + 1) <> "]"
      cmd = asCmd st
      msg = asMsg st
  in base
     <> (if T.null msg then "" else " " <> msg)
     <> (if T.null cmd then "" else " :" <> cmd)
  where tshow = T.pack . show

-- | Tab line: source path or folder path.
tabText :: AppState -> Text
tabText st =
  let v = _vsHd (asStack st)
      ns = _vNav v
  in case _nsVkind ns of
       VFld p _ -> p
       _        -> _vPath v

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
    ns = _vNav $ _vsHd $ asStack st
    tbl = _nsTbl ns
    disp = _nsDispIdxs ns
    curCol = _naCur (_nsCol ns)
    curRow = _naCur (_nsRow ns)
    rowSels = _naSels (_nsRow ns)
    colSels = _naSels (_nsCol ns)
    grpNames = _nsGrp ns
    nKeys = V.length grpNames
    names = visColNames st
    widths = visColWidths st
    nVis = V.length names

    -- Translate visible column index -> absolute dispIdxs position.
    absDispPos ci = asVisCol0 st + ci
    -- Data-order column index for a visible slot.
    origIdx ci =
      let p = absDispPos ci
      in if p < V.length disp then disp V.! p else 0
    isNumCol ci = isNumeric (_tblColType tbl (origIdx ci))
    isGrpCol ci = absDispPos ci < nKeys
    isCurCol ci = origIdx ci == curCol
    isSelCol ci = V.elem (origIdx ci) colSels

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
          tc = T.singleton (typeChar (_tblColType tbl (origIdx ci)))
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
    dataCell absRow ci cell =
      let w = widths V.! ci
          inner = max 0 (w - 2)
          padded = padCell inner (isNumCol ci) cell
          txt = " " <> padded <> " "
          isSelR = V.elem absRow rowSels
          isCurR = absRow == curRow
          isSelC = isSelCol ci
          isCurC = isCurCol ci
          a | isCurR && isCurC           = attrCursor
            | isSelR                     = attrSelRow
            | isSelC && isCurR           = attrSelColCurRow
            | isSelC                     = attrSelCol
            | isCurR                     = attrCurRow
            | isCurC                     = attrCurCol
            | isGrpCol ci                = attrGroup
            | otherwise                  = attrDefault
      in C.withAttr a (C.txt txt)

    mkRow rIdx row =
      let absRow = asVisRow0 st + rIdx
      in C.hBox $ concat
           [ [dataCell absRow ci (if ci < V.length row then row V.! ci else ""), sep ci]
           | ci <- [0 .. nVis - 1] ]

    gridW = C.vBox $ V.toList $ V.imap mkRow (asGrid st)

    -- Tab line at h-2. bar attribute gives it its own colour.
    tabW = C.withAttr attrBar (C.txt (tabText st))

    -- Optional info overlay drawn just above the status line. Enabled by
    -- the InfoTog command. Shows current column's name/type/index so the
    -- user can inspect metadata without opening the full colMeta view.
    infoW
      | asInfoVis st =
          let nCols = V.length (_tblColNames tbl)
              nm = if curCol < nCols then _tblColNames tbl V.! curCol else ""
              ty = if curCol < nCols then colTypeName' (_tblColType tbl curCol) else ""
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
      let c = _naCur (_nsCol ns)
          nCols = V.length (_tblColNames tbl)
          r = _naCur (_nsRow ns)
          n = _tblNRows (_nsTbl ns)
          nSel = V.length rowSels
      in "c" <> tshow c <> "/" <> tshow nCols
         <> " grp=" <> tshow nKeys
         <> " sel=" <> tshow nSel
         <> " r" <> tshow r <> "/" <> tshow n
    colName =
      if curCol < V.length (_tblColNames tbl)
        then _tblColNames tbl V.! curCol
        else ""
    msgPart = if T.null (asMsg st) then "" else " " <> asMsg st
    cmdPart = if T.null (asCmd st) then "" else " :" <> asCmd st
    leftTxt = colName <> msgPart <> cmdPart
    w = asVisW st
    gap = max 1 (w - T.length leftTxt - T.length statsRight)
    -- When asPendingCmd is Just the whole status line becomes an input
    -- prompt: "{label}: {buffer}▌" with a block cursor so the user can
    -- see what they're typing. Otherwise the normal colName/stats line.
    promptLine label =
      let body = label <> ": " <> asCmd st <> "\x258C"
          pad  = max 0 (w - T.length body)
      in body <> T.replicate pad " "
    statusW = case asPendingCmd st of
      Just c  -> C.withAttr attrStatus (C.txt (promptLine (cmdLabel c)))
      Nothing -> C.withAttr attrStatus
               $ C.txt (leftTxt <> T.replicate gap " " <> statsRight)

    tshow :: Int -> Text
    tshow = T.pack . show
