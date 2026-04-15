{-
  Render logic for typeclass-based navigation
  ViewState holds scroll offsets and cached widths

  Literal port of Tc/Tc/Render.lean. Function names, argument order, and
  arithmetic mirror the Lean reference line-for-line. Any drift here breaks
  demo-cast parity.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.Render
  ( ViewState(..)
  , viewStateDefault
  , reservedLines
  , maxVisRows
  , defaultRowPg
  , renderCols
  , render
  , renderTabLine
  , waitForQ
  , errorPopup
  , statusMsg
  ) where

import Prelude hiding (init, print)
import Control.Monad (unless, when)
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8, Word32, Word64)

import Optics.TH (makeFieldLabelsNoPrefix)

import Tv.Nav (NavState(..), adjOff)
import qualified Tv.Nav as Nav
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import Tv.Types
  ( Column
  , RenderCtx(..)
  , TblOps
  , ViewKind(..)
  )
import qualified Tv.Types as TblOps

-- ViewState: scroll offsets (widths moved to View for type safety)
data ViewState = ViewState
  { rowOff  :: Int   -- first visible row
  , lastCol :: Int   -- last cursor column (for tooltip direction)
  }
makeFieldLabelsNoPrefix ''ViewState

-- Default ViewState
viewStateDefault :: ViewState
viewStateDefault = ViewState { rowOff = 0, lastCol = 0 }

-- Reserved lines: 1 header + 1 footer + 1 tab + 1 status (+ 1 sparkline when active)
reservedLines :: Bool -> Int
reservedLines sparkOn = if sparkOn then 5 else 4

-- Max visible rows (no terminal should exceed this)
maxVisRows :: Int
maxVisRows = 200

-- Default row page size (fallback when terminal height unknown)
defaultRowPg :: Int
defaultRowPg = 20

-- Styles: loaded from Theme, or use default
-- 9 states: cursor, selRow, selColCurRow, selCol, curRow, curCol, default, header, group

-- RenderTable removed: render method now in Table class (Types.hs)

-- | Shared render helper: adjusts cursor/selections for window, calls C FFI
renderCols
  :: Vector Column -> Vector Text -> Vector Char
  -> Int -> RenderCtx -> Int -> Int -> IO (Vector Int)
renderCols cols names fmts totalRows RenderCtx{..} r0_ nVisible = do
  let adjCur = curRow - r0_
      adjSel = V.mapMaybe
        (\r -> if r >= r0_ && r < r0_ + nVisible then Just (r - r0_) else Nothing)
        rowSels
  ws <- Term.renderTable cols names fmts
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
    (V.map fromIntegral selColIdxs)
    (V.map fromIntegral adjSel)
    (V.map fromIntegral hiddenIdxs)
    styles
    (fromIntegral prec :: Int64)
    (fromIntegral widthAdj :: Int64)
    heatMode
    sparklines
  pure (V.map fromIntegral ws)

-- | Render table to terminal, returns (ViewState, widths)
-- Calls TblOps.render with NavState fields unpacked
render
  :: TblOps t
  => NavState t -> ViewState -> Vector Int
  -> Vector Word32 -> Int -> Int
  -> ViewKind -> Word8 -> Vector Text -> Vector Int
  -> IO (ViewState, Vector Int)
render nav view inWidths_ styles_ prec_ widthAdj_ vkind heatMode_ sparklines_ extraHidden = do
  Term.clear
  h <- Term.height
  w <- Term.width
  let sparkOn = V.any (not . T.null) sparklines_
  let visRows = min maxVisRows (fromIntegral h - reservedLines sparkOn)
  let rowOff_ = adjOff (Nav.cur (Nav.row nav)) (rowOff view) visRows
  let curColIdx_ = Nav.curColIdx nav
  let moveDir_ =
        if curColIdx_ > lastCol view then 1
        else if curColIdx_ < lastCol view then -1
        else 0
  let nRows_ = TblOps.nRows (tbl nav)
  let nCols_ = V.length (Nav.colNames nav)
  let ctx = RenderCtx
        { inWidths   = inWidths_
        , dispIdxs   = Nav.dispIdxs nav
        , nGrp       = V.length (grp nav)
        , r0         = rowOff_
        , r1         = min nRows_ (rowOff_ + visRows)
        , curRow     = Nav.cur (Nav.row nav)
        , curCol     = curColIdx_
        , moveDir    = moveDir_
        , selColIdxs = Nav.selColIdxs nav
        , rowSels    = Nav.sels (Nav.row nav)
        , hiddenIdxs = Nav.hiddenIdxs nav V.++ extraHidden
        , styles     = styles_
        , prec       = prec_
        , widthAdj   = widthAdj_
        , heatMode   = heatMode_
        , sparklines = sparklines_
        }
  -- C returns base widths (no widthAdj), store as-is
  widths <- TblOps.render (tbl nav) ctx
  -- status line: colName left, stats right
  -- freqV shows total distinct groups, others show table totalRows
  let total = case vkind of
        VkFreqV _ t -> t
        _           -> TblOps.totalRows (tbl nav)
  let colName = Nav.curColName nav
  let adj =
        (if prec_ /= 3 then " p" <> T.pack (show prec_) else "")
        <> (if widthAdj_ /= 0 then " w" <> T.pack (show widthAdj_) else "")
  let right =
        "c" <> T.pack (show curColIdx_) <> "/" <> T.pack (show nCols_)
        <> " grp=" <> T.pack (show (V.length (grp nav)))
        <> " sel=" <> T.pack (show (V.length (Nav.sels (Nav.row nav))))
        <> adj
        <> " r" <> T.pack (show (Nav.cur (Nav.row nav)))
        <> "/" <> T.pack (show total)
  let pad = fromIntegral w - T.length colName - T.length right
  Term.print 0 (h - 1)
    (Theme.styleFg styles_ Theme.sStatus)
    (Theme.styleBg styles_ Theme.sStatus)
    (colName <> T.replicate (max 1 pad) " " <> right)
  pure (ViewState { rowOff = rowOff_, lastCol = curColIdx_ }, widths)

-- | Render tab line: parent2 │ parent1 │ [current]  replay_ops (stack top on right)
renderTabLine :: Vector Text -> Int -> Text -> IO ()
renderTabLine tabs curIdx replay = do
  s <- Theme.getStyles
  let fg  = Theme.styleFg s Theme.sBar
      bg  = Theme.styleBg s Theme.sBar
      dfg = Theme.styleFg s Theme.sBarDim
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
  ev <- Term.pollEvent
  if Term.eventType ev == Term.eventKey
       && Term.eventCh ev == fromIntegral (fromEnum 'q')
    then pure ()
    else waitForQ

-- | Render error popup centered on screen, returns on 'q' press.
-- Callers like SourceConfig.runList may invoke this before Term.init —
-- waitForQ would CPU-spin since tb_peek_event returns immediately pre-init.
errorPopup :: Text -> IO ()
errorPopup msg = do
  on <- Term.inited
  unless (not on) $ do
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
  unless (not on) $ do
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
