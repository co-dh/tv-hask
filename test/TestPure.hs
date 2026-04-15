-- |
--   Pure core tests — literal port of Tc/test/TestPure.lean.
--   Theorems and #guard checks both become HUnit testCases here.
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
module TestPure (tests) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=), assertBool)

import qualified Tv.Fzf as Fzf
import qualified Tv.Key as Key
import qualified Tv.Nav as Nav
import qualified Tv.Plot as Plot
import qualified Tv.Term as Term
import Tv.Term (Event(..))
import Tv.Types
  ( Cmd(..)
  , ColType(..)
  , Effect(..)
  , PlotKind(..)
  , TblOps(..)
  , ViewKind(..)
  )
import qualified Tv.View as View
import Tv.View (View(..), ViewStack(..))

-- ## Mock Table
--
-- Lean: `structure MockTable (nRows nCols : Nat) where names : Array String`
-- Ported literally: Haskell has no dependent Nat params, so we store the sizes
-- as ordinary fields and the TblOps instance returns them directly.
data MockTable = MockTable
  { mockRows  :: Int
  , mockNames :: Vector Text
  }

instance TblOps MockTable where
  nRows     t     = mockRows t
  colNames  t     = mockNames t
  filter_   _ _   = pure Nothing
  distinct  _ _   = pure V.empty
  findRow   _ _ _ _ _ = pure Nothing
  render    _ _   = pure V.empty

mock53 :: MockTable
mock53 = MockTable { mockRows = 5, mockNames = V.fromList ["c0", "c1", "c2"] }

testNav :: Nav.NavState MockTable
testNav = Nav.new mock53

testView :: View MockTable
testView = View.new testNav "data/test.csv"

testStack :: ViewStack MockTable
testStack = ViewStack { hd = testView, tl = [] }

-- Build a raw Event record matching the Lean `⟨type, mod, key, ch, w, h⟩` shape.
mkEv :: Event
mkEv = Event
  { eventType    = Term.eventKey
  , eventMod     = 0
  , eventKeyCode = 0
  , eventCh      = 0
  , eventW       = 0
  , eventH       = 0
  }

-- Helper: substring occurs in string (Lean `has` helper).
has :: Text -> Text -> Bool
has s needle = length (T.splitOn needle s) > 1

-- ## evToKey Tests
keyMapTests :: TestTree
keyMapTests = testGroup "evToKey"
  [ testCase "arrow down -> j" $
      Key.evToKey (mkEv { eventKeyCode = Term.keyArrowDown }) @?= "j"
  , testCase "arrow up -> k" $
      Key.evToKey (mkEv { eventKeyCode = Term.keyArrowUp }) @?= "k"
  , testCase "arrow right -> l" $
      Key.evToKey (mkEv { eventKeyCode = Term.keyArrowRight }) @?= "l"
  , testCase "arrow left -> h" $
      Key.evToKey (mkEv { eventKeyCode = Term.keyArrowLeft }) @?= "h"
  , testCase "shift+left -> <S-left>" $
      Key.evToKey (mkEv { eventMod = Term.modShift, eventKeyCode = Term.keyArrowLeft }) @?= "<S-left>"
  , testCase "shift+right -> <S-right>" $
      Key.evToKey (mkEv { eventMod = Term.modShift, eventKeyCode = Term.keyArrowRight }) @?= "<S-right>"
  , testCase "ctrl+up -> <C-up>" $
      Key.evToKey (mkEv { eventMod = Term.modCtrl, eventKeyCode = Term.keyArrowUp }) @?= "<C-up>"
  , testCase "alt+down -> <A-down>" $
      Key.evToKey (mkEv { eventMod = Term.modAlt, eventKeyCode = Term.keyArrowDown }) @?= "<A-down>"
  , testCase "ctrl+enter -> <ret>" $
      Key.evToKey (mkEv { eventMod = Term.modCtrl, eventKeyCode = Term.keyEnter }) @?= "<ret>"
  , testCase "enter no-mod -> <ret>" $
      Key.evToKey (mkEv { eventKeyCode = Term.keyEnter }) @?= "<ret>"
  , testCase "ctrl+backspace -> <bs>" $
      Key.evToKey (mkEv { eventMod = Term.modCtrl, eventKeyCode = Term.keyBackspace }) @?= "<bs>"
  , testCase "backspace2 -> <bs>" $
      Key.evToKey (mkEv { eventKeyCode = Term.keyBackspace2 }) @?= "<bs>"
  , testCase "ctrl code 4 -> <C-d>" $
      Key.evToKey (mkEv { eventMod = 2, eventKeyCode = 4 }) @?= "<C-d>"
  , testCase "ctrl code 21 -> <C-u>" $
      Key.evToKey (mkEv { eventMod = 2, eventKeyCode = 21 }) @?= "<C-u>"
  , testCase "alt+x printable -> <A-x>" $
      Key.evToKey (mkEv { eventMod = Term.modAlt, eventCh = 120 }) @?= "<A-x>"
  ]

-- ## View.update Tests
viewUpdateTests :: TestTree
viewUpdateTests = testGroup "View.update"
  [ testCase "sort.asc -> Effect.sort asc=True" $
      fmap snd (View.update testView CmdSortAsc 1)
        @?= Just (EffectSort 0 V.empty V.empty True)
  , testCase "sort.desc -> Effect.sort asc=False" $
      fmap snd (View.update testView CmdSortDesc 1)
        @?= Just (EffectSort 0 V.empty V.empty False)
  , testCase "row.inc moves cursor to 1" $
      fmap (Nav.cur . Nav.row . View.nav . fst) (View.update testView CmdRowInc 1)
        @?= Just 1
  , testCase "row.inc returns Effect.none" $
      fmap snd (View.update testView CmdRowInc 1) @?= Just EffectNone
  , testCase "row.dec at 0 stays at 0" $
      fmap (Nav.cur . Nav.row . View.nav . fst) (View.update testView CmdRowDec 1)
        @?= Just 0
  ]

-- ## Menu Alignment Tests
menuAlignTests :: TestTree
menuAlignTests = testGroup "parseFlatSel"
  [ testCase "plot.area aligned row" $
      Fzf.parseFlatSel "plot.area    | cg |   | Plot: area chart" @?= Just "plot.area"
  , testCase "sort.asc aligned row" $
      Fzf.parseFlatSel "sort.asc     | c  | [ | Sort ascending" @?= Just "sort.asc"
  , testCase "empty string -> none" $
      Fzf.parseFlatSel "" @?= Nothing
  ]

-- ## Plot downsampling interval bar visibility
plotDownsampleTests :: TestTree
plotDownsampleTests = testGroup "Plot downsampling bar"
  [ testCase "100 rows does not trigger downsampling" $
      assertBool "100 > maxPoints must be false" (not (100 > Plot.maxPoints))
  , testCase "3000 rows triggers downsampling" $
      assertBool "3000 > maxPoints must be true" (3000 > Plot.maxPoints)
  ]

-- ## Plot rScript Tests
plotRScriptTests :: TestTree
plotRScriptTests = testGroup "Plot.rScript"
  [ testCase "density includes ggtitle when title is set" $
      assertBool "ggtitle('density of Close') missing"
        (has (Plot.rScript "d.dat" "p.png" PlotDensity "" "Close" False "" False "" ColTypeOther "density of Close")
             "ggtitle('density of Close')")
  , testCase "density includes hjust = 0.5 centering" $
      assertBool "hjust = 0.5 missing"
        (has (Plot.rScript "d.dat" "p.png" PlotDensity "" "Close" False "" False "" ColTypeOther "density of Close")
             "hjust = 0.5")
  , testCase "line multi-col title" $
      assertBool "ggtitle('line: Price vs Date by Ticker') missing"
        (has (Plot.rScript "d.dat" "p.png" PlotLine "Date" "Price" True "Ticker" False "" ColTypeOther "line: Price vs Date by Ticker")
             "ggtitle('line: Price vs Date by Ticker')")
  , testCase "omits ggtitle when title empty" $
      assertBool "ggtitle should be absent"
        (not (has (Plot.rScript "d.dat" "p.png" PlotLine "Date" "Price" False "" False "" ColTypeOther "")
                  "ggtitle"))
  ]

-- ## ViewStack.update Tests
viewStackUpdateTests :: TestTree
viewStackUpdateTests = testGroup "ViewStack.update"
  [ testCase "S (stk.swap) identity on single-element stack" $
      fmap (View.path . View.hd . fst) (View.updateStack testStack CmdStkSwap)
        @?= Just "data/test.csv"
  , testCase "S returns Effect.none" $
      fmap snd (View.updateStack testStack CmdStkSwap) @?= Just EffectNone
  , testCase "stk.dup pushes a copy (tl length = 1)" $
      fmap (length . View.tl . fst) (View.updateStack testStack CmdStkDup) @?= Just 1
  , testCase "stk.dup returns Effect.none" $
      fmap snd (View.updateStack testStack CmdStkDup) @?= Just EffectNone
  , testCase "q on empty stack -> quit" $
      fmap snd (View.updateStack testStack CmdStkPop) @?= Just EffectQuit
  , testCase "q with parent pops (tl length = 0)" $
      let twoStack = View.dup testStack
      in fmap (length . View.tl . fst) (View.updateStack twoStack CmdStkPop) @?= Just 0
  , testCase "q with parent returns Effect.none" $
      let twoStack = View.dup testStack
      in fmap snd (View.updateStack twoStack CmdStkPop) @?= Just EffectNone
  , testCase "unhandled cmd returns none" $
      assertBool "row.inc on stack should be Nothing"
        (case View.updateStack testStack CmdRowInc of { Nothing -> True; Just _ -> False })
  ]

-- ## View.tabName Tests
tabNameTests :: TestTree
tabNameTests = testGroup "View.tabName"
  [ testCase "table view: shows filename from path" $
      let tblView = View.new testNav "data/sample.parquet"
      in View.tabName tblView @?= "sample.parquet"
  , testCase "folder view: shows absolute path" $
      let tblView = View.new testNav "data/sample.parquet"
          fldView = tblView { View.vkind = VkFld "/home/user/Tc" 1
                            , View.path  = "/home/user/Tc"
                            }
      in View.tabName fldView @?= "/home/user/Tc"
  , testCase "colMeta view: custom disp wins" $
      let metaView = testView { View.vkind = VkColMeta, View.disp = "meta" }
      in View.tabName metaView @?= "meta"
  , testCase "freq view: custom disp wins" $
      let freqView = testView { View.vkind = VkFreqV (V.singleton "c0") 5
                              , View.disp  = "freq"
                              }
      in View.tabName freqView @?= "freq"
  ]

-- ## tokenizeKeys Tests
tokenizeKeysTests :: TestTree
tokenizeKeysTests = testGroup "tokenizeKeys"
  [ testCase "abc -> [a,b,c]" $
      Key.tokenizeKeys "abc" @?= V.fromList ["a", "b", "c"]
  , testCase "jjj -> [j,j,j]" $
      Key.tokenizeKeys "jjj" @?= V.fromList ["j", "j", "j"]
  , testCase "<ret>" $
      Key.tokenizeKeys "<ret>" @?= V.fromList ["<ret>"]
  , testCase "<C-d>" $
      Key.tokenizeKeys "<C-d>" @?= V.fromList ["<C-d>"]
  , testCase "<C-u>" $
      Key.tokenizeKeys "<C-u>" @?= V.fromList ["<C-u>"]
  , testCase "<esc>" $
      Key.tokenizeKeys "<esc>" @?= V.fromList ["<esc>"]
  , testCase "<S-left>" $
      Key.tokenizeKeys "<S-left>" @?= V.fromList ["<S-left>"]
  , testCase "<S-right>" $
      Key.tokenizeKeys "<S-right>" @?= V.fromList ["<S-right>"]
  , testCase "jjj<ret>" $
      Key.tokenizeKeys "jjj<ret>" @?= V.fromList ["j", "j", "j", "<ret>"]
  , testCase "<C-d><C-u>" $
      Key.tokenizeKeys "<C-d><C-u>" @?= V.fromList ["<C-d>", "<C-u>"]
  , testCase "!l!<S-left>" $
      Key.tokenizeKeys "!l!<S-left>" @?= V.fromList ["!", "l", "!", "<S-left>"]
  , testCase "backslash is a plain char" $
      Key.tokenizeKeys "\\" @?= V.fromList ["\\"]
  , testCase "<wait><wait>" $
      Key.tokenizeKeys "<wait><wait>" @?= V.fromList ["<wait>", "<wait>"]
  , testCase "<down><up> -> j k" $
      Key.tokenizeKeys "<down><up>" @?= V.fromList ["j", "k"]
  , testCase "<right><left> -> l h" $
      Key.tokenizeKeys "<right><left>" @?= V.fromList ["l", "h"]
  , testCase "empty string" $
      Key.tokenizeKeys "" @?= V.empty
  , testCase "lone <" $
      Key.tokenizeKeys "<" @?= V.fromList ["<"]
  , testCase "a<b unclosed bracket" $
      Key.tokenizeKeys "a<b" @?= V.fromList ["a", "<", "b"]
  , testCase "<> empty bracket" $
      Key.tokenizeKeys "<>" @?= V.fromList ["<", ">"]
  ]

-- ## Term.parseColor Tests
parseColorTests :: TestTree
parseColorTests = testGroup "Term.parseColor"
  [ testCase "default -> 0" $ Term.parseColor "default" @?= 0
  , testCase "black -> 16 (cube)" $ Term.parseColor "black" @?= 16
  , testCase "red -> 1"   $ Term.parseColor "red" @?= 1
  , testCase "white -> 7" $ Term.parseColor "white" @?= 7
  , testCase "brCyan -> 14"  $ Term.parseColor "brCyan" @?= 14
  , testCase "brWhite -> 15" $ Term.parseColor "brWhite" @?= 15
  , testCase "rgb000 -> 16"  $ Term.parseColor "rgb000" @?= 16
  , testCase "rgb555 -> 231" $ Term.parseColor "rgb555" @?= 231
  , testCase "rgb520 -> 208" $ Term.parseColor "rgb520" @?= 208
  , testCase "rgb234 -> 110" $ Term.parseColor "rgb234" @?= 110
  , testCase "gray0 -> 232"  $ Term.parseColor "gray0" @?= 232
  , testCase "gray23 -> 255" $ Term.parseColor "gray23" @?= 255
  , testCase "gray4 -> 236"  $ Term.parseColor "gray4" @?= 236
  , testCase "rgb600 out of range -> 0" $ Term.parseColor "rgb600" @?= 0
  , testCase "gray24 out of range -> 0" $ Term.parseColor "gray24" @?= 0
  , testCase "nosuchcolor -> 0"         $ Term.parseColor "nosuchcolor" @?= 0
  ]

tests :: TestTree
tests = testGroup "TestPure"
  [ keyMapTests
  , viewUpdateTests
  , menuAlignTests
  , plotDownsampleTests
  , plotRScriptTests
  , viewStackUpdateTests
  , tabNameTests
  , tokenizeKeysTests
  , parseColorTests
  ]
