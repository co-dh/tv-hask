{-
  Render logic for typeclass-based navigation
  ViewState holds scroll offsets and cached widths

  Literal port of Tc/Tc/Render.lean. Function names, argument order, and
  arithmetic mirror the Lean reference line-for-line. Any drift here breaks
  demo-cast parity.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
module Tv.Render where

import Prelude hiding (init, print)
import Tv.Prelude
import Data.Bits ((.|.))
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.Nav (NavState(..), adjOff)
import qualified Tv.Nav as Nav
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import qualified Tv.Data.DuckDB.Conn as Conn
import Tv.Data.DuckDB.Table (AdbcTable)
import Tv.Types
  ( ColType
  , RenderCtx(..)
  , ViewKind(..)
  )

-- ViewState: scroll offsets (widths moved to View for type safety)
data ViewState = ViewState
  { rowOff  :: Int   -- first visible row
  , lastCol :: Int   -- last cursor column (for tooltip direction)
  }

-- Default ViewState
defVS :: ViewState
defVS = ViewState { rowOff = 0, lastCol = 0 }

-- Reserved lines: 1 header + 1 footer + 1 tab + 1 status (+ 1 sparkline when active)
reservedLines :: Bool -> Int
reservedLines sparkOn = if sparkOn then 5 else 4

-- Max visible rows (no terminal should exceed this)
visRows :: Int
visRows = 200

-- Styles: loaded from Theme, or use default
-- 9 states: cursor, selRow, selColCurRow, selCol, curRow, curCol, default, header, group

-- | Fetch rows from AdbcTable and render via renderCols
renderView :: AdbcTable -> RenderCtx -> IO (Vector Int)
renderView t ctx@RenderCtx{r0, r1, prec, commasPerCol, heatMode} = do
  texts <- Conn.fetchRows (t ^. #qr) r0 r1 prec commasPerCol
  heatDs <- if heatMode == 0
    then pure V.empty
    else Conn.fetchHeatDoubles (t ^. #qr) r0 r1
  renderCols texts (t ^. #colNames) (t ^. #colFmts) (t ^. #colTypes)
    (t ^. #nRows) ctx r0 (r1 - r0) heatDs

-- | Shared render helper: adjusts cursor/selections for window, calls renderer
renderCols
  :: Vector (Vector Text)  -- text grid (column-major)
  -> Vector Text           -- column names
  -> Vector Char           -- fmts
  -> Vector ColType        -- colTypes
  -> Int -> RenderCtx -> Int -> Int
  -> Vector (Vector Double)  -- heatDoubles
  -> IO (Vector Int)
renderCols texts names fmts colTypes totalRows RenderCtx{..} r0_ nVisible heatDs = do
  let adjCur = curRow - r0_
      adjSel = V.mapMaybe
        (\r -> if r >= r0_ && r < r0_ + nVisible then Just $ r - r0_ else Nothing)
        rowSels
  ws <- Term.renderTable texts names fmts colTypes
    (V.map fromIntegral inWidths)
    (V.map fromIntegral dispIdxs)
    (fromIntegral totalRows)
    (fromIntegral nGrp)
    0
    0
    (fromIntegral nVisible)
    (fromIntegral adjCur)
    (fromIntegral curCol)
    (fromIntegral moveDir :: Int64)
    (V.map fromIntegral selIdxs)
    (V.map fromIntegral adjSel)
    (V.map fromIntegral hiddenIdxs)
    styles
    (fromIntegral prec :: Int64)
    (fromIntegral widthAdj :: Int64)
    heatMode
    sparklines
    heatDs
  pure $ V.map fromIntegral ws

-- | Render table to terminal, returns (ViewState, widths)
-- Calls renderView with NavState fields unpacked
render
  :: NavState AdbcTable -> ViewState -> Vector Int
  -> Vector Word32 -> Int -> Bool -> Vector Text -> Int
  -> ViewKind -> Word8 -> Vector Text -> Vector Int
  -> IO (ViewState, Vector Int)
render nav ViewState{rowOff, lastCol} inWidths_ styles_ prec_ commas_ commaFlip_ widthAdj_ vkind heatMode_ sparklines_ extraHidden = do
  Term.clear
  h <- Term.height
  w <- Term.width
  let sparkOn = V.any (not . T.null) sparklines_
  let nVis = min visRows (fromIntegral h - reservedLines sparkOn)
  let rowOff_ = adjOff (nav ^. #row % #cur) rowOff nVis
  let curColIdx_ = Nav.colIdx nav
  let moveDir_ =
        if curColIdx_ > lastCol then 1
        else if curColIdx_ < lastCol then -1
        else 0
  let nRows_ = nav ^. #tbl % #nRows
  -- Comma display per column: global `commas_` flipped for names in commaFlip_.
  let commasPC = V.map (\n -> if V.elem n commaFlip_ then not commas_ else commas_)
                       (nav ^. #tblNames)
  let ctx = RenderCtx
        { inWidths   = inWidths_
        , dispIdxs   = nav ^. #dispIdxs
        , nGrp       = V.length (nav ^. #grp)
        , r0         = rowOff_
        , r1         = min nRows_ (rowOff_ + nVis)
        , curRow     = nav ^. #row % #cur
        , curCol     = curColIdx_
        , moveDir    = moveDir_
        , selIdxs = Nav.selIdxs nav
        , rowSels    = nav ^. #row % #sels
        , hiddenIdxs = Nav.hiddenIdxs nav V.++ extraHidden
        , styles     = styles_
        , prec       = prec_
        , commasPerCol = commasPC
        , widthAdj   = widthAdj_
        , heatMode   = heatMode_
        , sparklines = sparklines_
        }
  -- C returns base widths (no widthAdj), store as-is
  widths <- renderView (nav ^. #tbl) ctx
  statusLine nav vkind styles_ prec_ widthAdj_ curColIdx_ w h
  pure (ViewState { rowOff = rowOff_, lastCol = curColIdx_ }, widths)

-- | Bottom status line: colName on the left, cursor/grp/sel/prec/width/row stats on the right.
-- freqV uses its own total (distinct groups); other views use the table's totalRows.
statusLine
  :: NavState AdbcTable -> ViewKind -> Vector Word32 -> Int -> Int
  -> Int -> Word32 -> Word32 -> IO ()
statusLine nav vkind styles_ prec_ widthAdj_ curColIdx_ w h = do
  let total = case vkind of
        VkFreqV _ t -> t
        _           -> nav ^. #tbl % #totalRows
      colName = Nav.colName nav
      nCols_ = V.length (Nav.colNames nav)
      adj =
        (if prec_ /= 3 then " p" <> T.pack (show prec_) else "")
        <> (if widthAdj_ /= 0 then " w" <> T.pack (show widthAdj_) else "")
      right =
        "c" <> T.pack (show curColIdx_) <> "/" <> T.pack (show nCols_)
        <> " grp=" <> T.pack (show (V.length (nav ^. #grp)))
        <> " sel=" <> T.pack (show (V.length (nav ^. #row % #sels)))
        <> adj
        <> " r" <> T.pack (show (nav ^. #row % #cur))
        <> "/" <> T.pack (show total)
      pad = fromIntegral w - T.length colName - T.length right
  Term.print 0 (h - 1)
    (Theme.styleFg styles_ Theme.sStatus)
    (Theme.styleBg styles_ Theme.sStatus)
    (colName <> T.replicate (max 1 pad) " " <> right)

-- | Render tab line: parent2 │ parent1 │ [current]  replay_ops (stack top on right)
tabLine :: Vector Text -> Int -> Text -> IO ()
tabLine tabs curIdx replay = do
  s <- Theme.getStyles
  let -- Bold the tab-line foreground so the file header reads as a header,
      -- not as another data row. Same _TB_BOLD bit drawHeader uses.
      fg  = Theme.styleFg s Theme.sBar    .|. 0x01000000
      bg  = Theme.styleBg s Theme.sBar
      dfg = Theme.styleFg s Theme.sBarDim .|. 0x01000000
      dbg = Theme.styleBg s Theme.sBarDim
  h <- Term.height
  w <- Term.width
  let line =
        T.intercalate " │ "
          (reverse
            (V.toList
              (V.imap (\i t -> if i == curIdx then "[" <> t <> "]" else t) tabs)))
  Term.print 0 (h - 2) fg bg line
  let gap = fromIntegral w - T.length line
  if not (T.null replay) && gap > T.length replay + 2
    then do
      let rpad = gap - T.length replay - 1
      Term.print (fromIntegral (T.length line)) (h - 2) fg bg (T.replicate rpad " ")
      Term.print (w - fromIntegral (T.length replay) - 1) (h - 2) dfg dbg replay
      Term.print (w - 1) (h - 2) fg bg " "
    else
      when (T.length line < fromIntegral w) $
        Term.print (fromIntegral (T.length line)) (h - 2) fg bg
          (T.replicate (fromIntegral w - T.length line) " ")

-- | Wait for 'q' key press
waitForQ :: IO ()
waitForQ = do
  Term.Event{Term.typ, Term.ch} <- Term.pollEvent
  unless (typ == Term.eventKey
       && ch == fromIntegral (fromEnum 'q')) waitForQ

-- | Render error popup centered on screen, returns on 'q' press.
-- Callers like Tv.Source.runList may invoke this before Term.init —
-- waitForQ would CPU-spin since tb_peek_event returns immediately pre-init.
errorPopup :: Text -> IO ()
errorPopup msg = do
  on <- Term.inited
  when on $ do
    s <- Theme.getStyles
    let fg  = Theme.styleFg s Theme.sError
        bg  = Theme.styleBg s Theme.sError
        dfg = Theme.styleFg s Theme.sErrorDim
        dbg = Theme.styleBg s Theme.sErrorDim
    h <- Term.height
    w <- Term.width
    let help = "press q to dismiss" :: Text
    let boxW = max (T.length msg) (T.length help) + 4
    let x0 = (fromIntegral w - boxW) `div` 2 :: Int
    let y0 = fromIntegral h `div` 2 - 1 :: Int
    let pad str =
          " " <> str <> T.replicate (boxW - T.length str - 2) " " <> " "
    Term.print (fromIntegral x0) (fromIntegral y0) fg bg
      (pad (T.replicate (boxW - 2) " "))
    Term.print (fromIntegral x0) (fromIntegral (y0 + 1)) fg bg (pad msg)
    Term.print (fromIntegral x0) (fromIntegral (y0 + 2)) dfg dbg (pad help)
    Term.present
    waitForQ

-- | Show a brief status message on the bottom line (non-blocking)
-- No-op if terminal not initialized (height=0)
statusMsg :: Text -> IO ()
statusMsg msg = do
  on <- Term.inited
  when on $ do
    h <- Term.height
    unless (h == 0) $ do
      s <- Theme.getStyles
      w <- Term.width
      let padLen =
            if fromIntegral w > T.length msg
              then fromIntegral w - T.length msg
              else 0
      Term.print 0 (h - 1)
        (Theme.styleFg s Theme.sStatus)
        (Theme.styleBg s Theme.sStatus)
        (msg <> T.replicate padLen " ")
      Term.present
