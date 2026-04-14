{-# LANGUAGE OverloadedStrings #-}
-- | Tests for Tv.Render / Tv.App handler dispatch.
-- Uses a hand-rolled mock TblOps — the real DuckDB backend is out of scope
-- here. Stub IO fields error if called; handler dispatch must avoid them.
module RenderSpec (tests) where

import Test.Tasty
import Test.Tasty.HUnit
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Tv.Types
import Tv.View
import Tv.Render
import Tv.App (handleCmd, handleKey)
import qualified Tv.CmdConfig as CC
import Tv.Eff (runEff)
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- ============================================================================
-- Mock TblOps: 5 rows x 3 cols, cell (r,c) = "r,c".
-- ============================================================================

mockOps :: TblOps
mockOps = TblOps
  { _tblNRows     = 5
  , _tblColNames  = V.fromList ["c0", "c1", "c2"]
  , _tblTotalRows = 5
  , _tblQueryOps  = V.empty
  , _tblFilter    = \_ -> stub "filter"
  , _tblDistinct  = \_ -> stub "distinct"
  , _tblFindRow   = \_ _ _ _ -> stub "findRow"
  , _tblRender    = \_ -> stub "render"
  , _tblGetCols   = \_ _ _ -> stub "getCols"
  , _tblColType   = const CTStr
  , _tblBuildFilter = \_ _ _ _ -> ""
  , _tblFilterPrompt = \_ _ -> ""
  , _tblPlotExport = \_ _ _ _ _ _ -> stub "plotExport"
  , _tblCellStr   = \r c -> pure (T.pack (show r <> "," <> show c))
  , _tblFetchMore = stub "fetchMore"
  , _tblHideCols  = \_ -> stub "hideCols"
  , _tblSortBy    = \_ _ -> stub "sortBy"
  }
  where stub s = error ("mockOps: " <> s <> " called")

mockState :: IO AppState
mockState = do
  let Just v = fromTbl mockOps "mock" 0 V.empty 0
      stack = ViewStack v []
      st0 = AppState
        { _asStack   = stack
        , _asThemeIdx = 0
        , _asTestKeys = []
        , _asMsg = ""
        , _asErr = ""
        , _asCmd = ""
        , _asPendingCmd = Nothing
        , _asGrid = V.empty
        , _asVisRow0 = 0
        , _asVisCol0 = 0
        , _asVisColN = 3
        , _asVisH = 5
        , _asVisW = 3
        , _asStyles = V.empty
        , _asInfoVis = False
        }
  -- prefetch the grid via the same path the App uses
  grid <- V.generateM 5 $ \r ->
          V.generateM 3 $ \c -> (mockOps ^. tblCellStr) r c
  pure (st0 & asGrid .~ grid)

-- Minimal command entry set so keyLookup resolves j/k/h/l/q.
initTestCmds :: IO ()
initTestCmds = runEff $ CC.initCmds
  [ CC.Entry RowInc  "r" "j" "row++"  False ""
  , CC.Entry RowDec  "r" "k" "row--"  False ""
  , CC.Entry ColInc  "c" "l" "col++"  False ""
  , CC.Entry ColDec  "c" "h" "col--"  False ""
  , CC.Entry TblQuit ""  "q" "quit"   False ""
  ]

-- ============================================================================
-- Tests
-- ============================================================================

tests :: TestTree
tests = testGroup "Render"
  [ testCase "drawApp returns a single top-level widget" $ do
      st <- mockState
      length (drawApp st) @?= 1

  , testCase "headerText contains all visible column names" $ do
      st <- mockState
      let hdr = headerText st
      assertBool ("missing c0 in " <> T.unpack hdr) ("c0" `T.isInfixOf` hdr)
      assertBool ("missing c1 in " <> T.unpack hdr) ("c1" `T.isInfixOf` hdr)
      assertBool ("missing c2 in " <> T.unpack hdr) ("c2" `T.isInfixOf` hdr)

  , testCase "asGrid holds 0,0 cell after prefetch" $ do
      st <- mockState
      ((st ^. asGrid) V.! 0 V.! 0) @?= "0,0"

  , testCase "statusText shows row 1/5 | col 1" $ do
      st <- mockState
      let s = statusText st
      assertBool ("status = " <> T.unpack s) ("row 1/5" `T.isInfixOf` s)
      assertBool ("status = " <> T.unpack s) ("col 1"   `T.isInfixOf` s)

  , testCase "handleKey 'j' advances nsRow from 0 to 1" $ do
      initTestCmds
      st <- mockState
      r <- handleKey "j" st
      case r of
        Nothing -> assertFailure "handleKey j: expected state, got halt"
        Just st' ->
          let cur = _naCur $ _nsRow $ _vNav $ _vsHd $ (st' ^. asStack)
          in cur @?= 1

  , testCase "handleKey 'l' advances nsCol from 0 to 1" $ do
      initTestCmds
      st <- mockState
      r <- handleKey "l" st
      case r of
        Nothing -> assertFailure "handleKey l: expected state, got halt"
        Just st' ->
          let cur = _naCur $ _nsCol $ _vNav $ _vsHd $ (st' ^. asStack)
          in cur @?= 1

  , testCase "handleKey 'q' halts (returns Nothing)" $ do
      initTestCmds
      st <- mockState
      r <- handleKey "q" st
      case r of
        Nothing -> pure ()
        Just _  -> assertFailure "handleKey q: expected halt, got state"
  ]
