{-# LANGUAGE OverloadedStrings #-}
-- | Pure-core tests ported from Tc/test/TestPure.lean. Every test here must
-- match the Lean spec: same inputs, same expected outputs.
module PureSpec (tests) where

import Test.Tasty
import Test.Tasty.HUnit

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Tv.Types
import Tv.View
import Tv.Key (evToKey, tokenizeKeys, KMod(..), mkArrowEv, mkCharEv, mkEnterEv, mkBsEv)
import Tv.Theme (parseColorIx)
import qualified Tv.Fzf as Fzf
import qualified Tv.Plot as Plot

-- ----------------------------------------------------------------------------
-- Mock TblOps for 5 rows x 3 cols (matches Lean's MockTable 5 3).
-- ----------------------------------------------------------------------------

mockTbl :: TblOps
mockTbl = TblOps
  { _tblNRows      = 5
  , _tblColNames   = V.fromList ["c0", "c1", "c2"]
  , _tblTotalRows  = 5
  , _tblQueryOps   = V.empty
  , _tblFilter     = \_ -> pure Nothing
  , _tblDistinct   = \_ -> pure V.empty
  , _tblFindRow    = \_ _ _ _ -> pure Nothing
  , _tblRender     = \_ -> pure V.empty
  , _tblGetCols    = \_ _ _ -> pure V.empty
  , _tblColType    = \_ -> CTOther
  , _tblBuildFilter = \_ _ _ _ -> ""
  , _tblFilterPrompt = \_ _ -> ""
  , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
  , _tblCellStr     = \_ _ -> pure ""
  , _tblFetchMore   = pure Nothing
  , _tblHideCols    = \_ -> pure mockTbl
  , _tblSortBy      = \_ _ -> pure mockTbl
  }

mockNav :: NavState
mockNav = NavState
  { _nsTbl = mockTbl, _nsRow = mkAxis, _nsCol = mkAxis
  , _nsGrp = V.empty, _nsHidden = V.empty
  , _nsDispIdxs = V.fromList [0, 1, 2]
  , _nsVkind = VTbl, _nsSearch = "", _nsPrecAdj = 0, _nsWidthAdj = 0, _nsHeatMode = 0 }

testView :: View
testView = mkView mockNav "data/test.csv"

testStack :: ViewStack
testStack = ViewStack testView []

-- ----------------------------------------------------------------------------

tests :: TestTree
tests = testGroup "Pure"
  [ keyMapTests
  , tokenizeKeysTests
  , parseColorTests
  , viewUpdateTests
  , viewStackUpdateTests
  , tabNameTests
  , fzfParseFlatSelTests
  , plotTests
  ]

-- ----------------------------------------------------------------------------
-- evToKey

keyMapTests :: TestTree
keyMapTests = testGroup "evToKey"
  [ tc "arrow down"  (mkArrowEv 'j' []) "j"
  , tc "arrow up"    (mkArrowEv 'k' []) "k"
  , tc "arrow right" (mkArrowEv 'l' []) "l"
  , tc "arrow left"  (mkArrowEv 'h' []) "h"
  , tc "shift+left"  (mkArrowEv 'h' [KShift]) "<S-left>"
  , tc "shift+right" (mkArrowEv 'l' [KShift]) "<S-right>"
  , tc "ctrl+up"     (mkArrowEv 'k' [KCtrl]) "<C-up>"
  , tc "alt+down"    (mkArrowEv 'j' [KAlt]) "<A-down>"
  , tc "enter (ctrl mod ignored)" (mkEnterEv [KCtrl]) "<ret>"
  , tc "enter no mod" (mkEnterEv []) "<ret>"
  , tc "bs (ctrl mod)" (mkBsEv [KCtrl]) "<bs>"
  , tc "bs no mod"   (mkBsEv []) "<bs>"
  , tc "ctrl+d"      (mkCharEv 'd' [KCtrl]) "<C-d>"
  , tc "ctrl+u"      (mkCharEv 'u' [KCtrl]) "<C-u>"
  , tc "alt+x"       (mkCharEv 'x' [KAlt]) "<A-x>"
  ]
  where tc n ev expected = testCase n $ evToKey ev @?= expected

-- ----------------------------------------------------------------------------
-- tokenizeKeys

tokenizeKeysTests :: TestTree
tokenizeKeysTests = testGroup "tokenizeKeys"
  [ tc "abc" ["a", "b", "c"]
  , tc "jjj" ["j", "j", "j"]
  , tc "<ret>" ["<ret>"]
  , tc "<C-d>" ["<C-d>"]
  , tc "<C-u>" ["<C-u>"]
  , tc "<esc>" ["<esc>"]
  , tc "<S-left>" ["<S-left>"]
  , tc "<S-right>" ["<S-right>"]
  , tc "jjj<ret>" ["j", "j", "j", "<ret>"]
  , tc "<C-d><C-u>" ["<C-d>", "<C-u>"]
  , tc "!l!<S-left>" ["!", "l", "!", "<S-left>"]
  , tc "\\" ["\\"]
  , tc "<wait><wait>" ["<wait>", "<wait>"]
  , tc "<down><up>" ["j", "k"]
  , tc "<right><left>" ["l", "h"]
  , tc "" []
  , tc "<" ["<"]
  , tc "a<b" ["a", "<", "b"]
  , tc "<>" ["<", ">"]
  ]
  where tc s ex = testCase (T.unpack s) $ tokenizeKeys s @?= ex

-- ----------------------------------------------------------------------------
-- Term.parseColor → parseColorIx

parseColorTests :: TestTree
parseColorTests = testGroup "parseColor"
  [ tc "default" 0
  , tc "black" 16
  , tc "red" 1
  , tc "white" 7
  , tc "brCyan" 14
  , tc "brWhite" 15
  , tc "rgb000" 16
  , tc "rgb555" 231
  , tc "rgb520" 208
  , tc "rgb234" 110
  , tc "gray0" 232
  , tc "gray23" 255
  , tc "gray4" 236
  , tc "rgb600" 0
  , tc "gray24" 0
  , tc "nosuchcolor" 0
  ]
  where tc n ex = testCase (T.unpack n) $ parseColorIx n @?= ex

-- ----------------------------------------------------------------------------
-- View.update

viewUpdateTests :: TestTree
viewUpdateTests = testGroup "View.update"
  [ testCase "sort.asc → ESort 0 _ _ True" $
      fmap snd (updateView testView SortAsc 1)
        @?= Just (ESort 0 V.empty V.empty True)
  , testCase "sort.desc → ESort 0 _ _ False" $
      fmap snd (updateView testView SortDesc 1)
        @?= Just (ESort 0 V.empty V.empty False)
  , testCase "row.inc moves to row 1" $
      fmap (_naCur . _nsRow . _vNav . fst) (updateView testView RowInc 1)
        @?= Just 1
  , testCase "row.inc effect ENone" $
      fmap snd (updateView testView RowInc 1)
        @?= Just ENone
  , testCase "row.dec at 0 stays" $
      fmap (_naCur . _nsRow . _vNav . fst) (updateView testView RowDec 1)
        @?= Just 0
  ]

-- ----------------------------------------------------------------------------
-- ViewStack.update

viewStackUpdateTests :: TestTree
viewStackUpdateTests = testGroup "ViewStack.update"
  [ testCase "stk.swap on singleton keeps same hd path" $
      fmap (_vPath . _vsHd . fst) (updateViewStack testStack StkSwap)
        @?= Just "data/test.csv"
  , testCase "stk.swap effect ENone" $
      fmap snd (updateViewStack testStack StkSwap) @?= Just ENone
  , testCase "stk.dup tl length = 1" $
      fmap (length . _vsTl . fst) (updateViewStack testStack StkDup)
        @?= Just 1
  , testCase "stk.dup effect ENone" $
      fmap snd (updateViewStack testStack StkDup) @?= Just ENone
  , testCase "stk.pop on last view → quit" $
      fmap snd (updateViewStack testStack StkPop) @?= Just EQuit
  , testCase "stk.pop with parent pops" $
      let two = vsDup testStack
      in fmap (length . _vsTl . fst) (updateViewStack two StkPop)
           @?= Just 0
  , testCase "stk.pop with parent → ENone" $
      let two = vsDup testStack
      in fmap snd (updateViewStack two StkPop) @?= Just ENone
  , testCase "row.inc at stack level → Nothing" $
      assertBool "isNothing" (maybe True (const False) (updateViewStack testStack RowInc))
  ]

-- ----------------------------------------------------------------------------
-- View.tabName

tabNameTests :: TestTree
tabNameTests = testGroup "View.tabName"
  [ testCase "table view shows filename" $
      tabName (mkView mockNav "data/sample.parquet") @?= "sample.parquet"
  , testCase "folder view shows absolute path" $
      let v = (mkView mockNav "/home/user/Tc")
                { _vNav = mockNav { _nsVkind = VFld "/home/user/Tc" 1 } }
      in tabName v @?= "/home/user/Tc"
  , testCase "meta disp" $
      let v = testView { _vDisp = "meta"
                       , _vNav = mockNav { _nsVkind = VColMeta } }
      in tabName v @?= "meta"
  , testCase "freq disp" $
      let v = testView { _vDisp = "freq"
                       , _vNav = mockNav { _nsVkind = VFreq (V.singleton "c0") 5 } }
      in tabName v @?= "freq"
  ]

-- ----------------------------------------------------------------------------
-- Fzf.parseFlatSel

fzfParseFlatSelTests :: TestTree
fzfParseFlatSelTests = testGroup "Fzf.parseFlatSel"
  [ testCase "plot.area" $
      Fzf.parseFlatSel "plot.area    | cg |   | Plot: area chart"
        @?= Just "plot.area"
  , testCase "sort.asc" $
      Fzf.parseFlatSel "sort.asc     | c  | [ | Sort ascending"
        @?= Just "sort.asc"
  , testCase "empty" $
      Fzf.parseFlatSel "" @?= Nothing
  ]

-- ----------------------------------------------------------------------------
-- Plot

plotTests :: TestTree
plotTests = testGroup "Plot"
  [ testCase "100 does not exceed maxPoints" $
      assertBool "100 > maxPoints" (not (100 > Plot.maxPoints))
  , testCase "3000 exceeds maxPoints" $
      assertBool "3000 > maxPoints" (3000 > Plot.maxPoints)
  , testCase "rScript density includes ggtitle" $
      assertBool "has ggtitle" $
        "ggtitle('density of Close')" `T.isInfixOf`
          Plot.rScript "d.dat" "p.png" PKDensity "" "Close" False "" False ""
                       Plot.POther "density of Close"
  , testCase "rScript density includes hjust" $
      assertBool "has hjust" $
        "hjust = 0.5" `T.isInfixOf`
          Plot.rScript "d.dat" "p.png" PKDensity "" "Close" False "" False ""
                       Plot.POther "density of Close"
  , testCase "rScript line includes multi-col title" $
      assertBool "has multi-col ggtitle" $
        "ggtitle('line: Price vs Date by Ticker')" `T.isInfixOf`
          Plot.rScript "d.dat" "p.png" PKLine "Date" "Price" True "Ticker" False ""
                       Plot.POther "line: Price vs Date by Ticker"
  , testCase "rScript no title omits ggtitle" $
      assertBool "no ggtitle" $ not $
        "ggtitle" `T.isInfixOf`
          Plot.rScript "d.dat" "p.png" PKLine "Date" "Price" False "" False ""
                       Plot.POther ""
  ]
