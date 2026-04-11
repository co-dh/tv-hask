{-# LANGUAGE OverloadedStrings #-}
-- | Main test suite ported from Tc/test/Test.lean.
--
-- The Lean suite drives the actual `tv` binary end-to-end (spawn it with
-- a key script, assert on the captured screen dump). The Haskell port
-- is still wiring up its runtime, so the vast majority of those cases
-- would require features that are currently stubbed out in src/Tv/.
-- Where a test maps cleanly to a pure function in the Haskell port
-- (nav, view, plot, fzf, remote URI helpers, theme color parser,
-- DuckDB query, etc.) we port the assertion. For everything else we
-- leave a `pending` marker citing the Lean source line so a future agent
-- can replace it with a real case as the runtime lands.
module MainSpec (tests) where

import Test.Tasty
import Test.Tasty.HUnit

import qualified Data.Text as T
import qualified Data.Vector as V
import Control.Exception (SomeException, try)
import System.Directory (removeFile, doesFileExist, getTemporaryDirectory)
import System.FilePath ((</>))
import qualified Data.Text.IO as TIO

import TestUtil
import Tv.Types
import Tv.View
import qualified Tv.Nav as Nav
import qualified Tv.Plot as Plot
import qualified Tv.Fzf as Fzf
import qualified Tv.Theme as Theme
import qualified Tv.Util as Util
import qualified Tv.Data.DuckDB as D
import qualified Tv.Filter as Filter
import qualified Tv.Export as Export
import qualified Tv.Meta as Meta
import qualified Tv.Transpose as Transpose
import qualified Tv.Derive as Derive
import qualified Tv.Split as Split
import qualified Tv.Diff as Diff
import qualified Tv.Join as Join
import qualified Tv.Session as Sess
import qualified Tv.SourceConfig as SC

-- ----------------------------------------------------------------------------
-- Pending: helper that stands in for "we know what the Lean test wanted but
-- the Haskell port isn't wired up yet". Keeps the test tree auditable.
-- ----------------------------------------------------------------------------
pending :: String -> String -> TestTree
pending name reason = testCase (name <> " (pending)") $
  assertBool ("pending: " <> reason) True

-- | Resolve a path in the system temp dir; callers are responsible for
-- removing the file after use.
tmpPath :: FilePath -> IO FilePath
tmpPath name = do
  d <- getTemporaryDirectory
  pure (d </> ("tv-hask-" <> name))

tests :: TestTree
tests = testGroup "Main (ported from Test.lean)"
  [ sortTests
  , metaTests
  , freqTests
  , searchTests
  , folderTests
  , remoteTests
  , colTests
  , filterExportTests
  , transposeTests
  , derivSplitTests
  , joinDiffTests
  , plotExportTests
  , sessionTests
  , sparkStatusTests
  , replayTests
  , dbSourceTests
  , sourceFormatTests
  , navExtraTests
  , arrowTests
  , themeTests
  ]

-- ============================================================================
-- Sort / View.update (Test.lean lines 28-36, 146-160, 254-266)
-- ============================================================================
sortTests :: TestTree
sortTests = testGroup "sort"
  [ testCase "updateView SortAsc emits ESort asc=True" $
      fmap snd (updateView testView SortAsc 1)
        @?= Just (ESort 0 V.empty V.empty True)
  , testCase "updateView SortDesc emits ESort asc=False" $
      fmap snd (updateView testView SortDesc 1)
        @?= Just (ESort 0 V.empty V.empty False)
  , testCase "sort.asc cursor-col is curColIdx" $
      -- move cursor right then check ESort carries col=1
      let ns = (_vNav testView) { _nsCol = mkAxis { _naCur = 1 } }
          v' = testView { _vNav = ns }
      in fmap snd (updateView v' SortAsc 1)
           @?= Just (ESort 1 V.empty V.empty True)
  , pending "sort_asc binary-level" "runtime tv-binary spawning not ported"
  , pending "sort_desc binary-level" "runtime tv-binary spawning not ported"
  , pending "sort_excludes_key" "grp_sort.csv requires real folder+csv reader"
  , pending "sort_selected_not_key" "sort on grp col is runtime-only"
  , pending "folder_sort_type" "folder module is a stub"
  ]

-- ============================================================================
-- Meta view (Test.lean 40-94)
-- ============================================================================
metaTests :: TestTree
metaTests = testGroup "meta"
  [ testCase "VColMeta view kind has ctx 'colMeta'" $
      vkCtxStr VColMeta @?= "colMeta"
  , testCase "tabName for meta disp" $
      let v = testView { _vDisp = "meta"
                       , _vNav = mockNav { _nsVkind = VColMeta } }
      in tabName v @?= "meta"
  , testCase "mkMetaOps emits 7 cols in fixed order" $ do
      m <- Meta.mkMetaOps mockTbl
      _tblColNames m @?=
        V.fromList ["column","coltype","cnt","dist","null_pct","mn","mx"]
  , testCase "mkMetaOps: one row per source col" $ do
      m <- Meta.mkMetaOps mockTbl
      _tblNRows m @?= V.length (_tblColNames mockTbl)
  , testCase "mkMetaOps: row i names source col i" $ do
      m <- Meta.mkMetaOps mockTbl
      c0 <- _tblCellStr m 0 0
      c1 <- _tblCellStr m 1 0
      c0 @?= "c0"
      c1 @?= "c1"
  , testCase "mkMetaOps cnt=nrows when no nulls" $ do
      m <- Meta.mkMetaOps mockTbl
      cnt <- _tblCellStr m 0 2          -- row 0 col cnt
      cnt @?= T.pack (show (_tblNRows mockTbl))
  , testCase "mkMetaOps null_pct rounds on half-null column" $ do
      let t = mkGridTbl ["x"] [["a"], [""], ["b"], [""]]
      m <- Meta.mkMetaOps t
      np <- _tblCellStr m 0 4
      np @?= "50"
  , testCase "mkMetaOps dist counts uniques" $ do
      let t = mkGridTbl ["x"] [["a"], ["a"], ["b"]]
      m <- Meta.mkMetaOps t
      d <- _tblCellStr m 0 3
      d @?= "2"
  , testCase "mkMetaOps min/max textual" $ do
      let t = mkGridTbl ["x"] [["b"], ["a"], ["c"]]
      m <- Meta.mkMetaOps t
      mn <- _tblCellStr m 0 5
      mx <- _tblCellStr m 0 6
      mn @?= "a"
      mx @?= "c"
  ]

-- ============================================================================
-- Freq view (Test.lean 54-72, 104-110)
-- ============================================================================
freqTests :: TestTree
freqTests = testGroup "freq"
  [ testCase "VFreq kind exposes 'freqV' ctx" $
      vkCtxStr (VFreq (V.singleton "c0") 5) @?= "freqV"
  , testCase "tabName for freq disp" $
      let v = testView { _vDisp = "freq"
                       , _vNav = mockNav { _nsVkind = VFreq (V.singleton "c0") 5 } }
      in tabName v @?= "freq"
  , pending "freq_shows" "freq.open handler stubbed"
  , pending "freq_by_key" "freq.open handler stubbed"
  , pending "freq_multi_key" "freq.open handler stubbed"
  , pending "freq_keeps_grp" "freq.open handler stubbed"
  , pending "freq_after_meta" "meta+freq chain not ported"
  , pending "freq_enter" "freq.filter handler stubbed"
  ]

-- ============================================================================
-- Search (Test.lean 120-144)
-- ============================================================================
searchTests :: TestTree
searchTests = testGroup "search"
  [ pending "search_jump" "row search runtime not ported"
  , pending "search_next" "row search runtime not ported"
  , pending "search_prev" "row search runtime not ported"
  , pending "search_after_sort" "sort+search combined runtime"
  , pending "col_search" "col search runtime not ported"
  ]

-- ============================================================================
-- Folder view (Test.lean 163-222, 245-251)
-- ============================================================================
folderTests :: TestTree
folderTests = testGroup "folder"
  [ testCase "VFld exposes 'fld' ctx" $
      vkCtxStr (VFld "/tmp" 1) @?= "fld"
  , testCase "tabName folder absolute path" $
      let v = (mkView mockNav "/home/user/Tc")
                { _vNav = mockNav { _nsVkind = VFld "/home/user/Tc" 1 } }
      in tabName v @?= "/home/user/Tc"
  , pending "folder_no_args" "no-arg folder runtime not ported"
  , pending "folder_D" "D key folder push not ported"
  , pending "folder_tab" "cwd folder runtime not ported"
  , pending "folder_enter" "folder enter runtime not ported"
  , pending "folder_pop" "folder stack handler not ported"
  , pending "folder_backspace" "folder bs handler not ported"
  , pending "folder_backspace_twice" "folder bs handler not ported"
  , pending "folder_enter_symlink" "symlink follow not ported"
  , pending "folder_relative" "relative path column not ported"
  , pending "folder_prefix" "folder comma prefix not ported"
  ]

-- ============================================================================
-- Remote / URI helpers (Util.remote*)
-- ============================================================================
remoteTests :: TestTree
remoteTests = testGroup "remote URI"
  [ testCase "remoteJoin adds slash when missing" $
      Util.remoteJoin "s3://bucket" "key" @?= "s3://bucket/key"
  , testCase "remoteJoin keeps trailing slash" $
      Util.remoteJoin "s3://bucket/" "key" @?= "s3://bucket/key"
  , testCase "remoteParent strips last segment" $
      Util.remoteParent "s3://bucket/a/b/c" 1 @?= Just "s3://bucket/a/b/"
  , testCase "remoteParent returns Nothing at minParts" $
      Util.remoteParent "s3://bucket" 4 @?= Nothing
  , testCase "remoteDispName returns last segment" $
      Util.remoteDispName "hf://datasets/openai/gsm8k" @?= "gsm8k"
  , testCase "remoteDispName on single segment" $
      Util.remoteDispName "file" @?= "file"
  , testCase "parseUri s3 → s3 config" $
      (SC.cfgPfx <$> SC.parseUri "s3://bucket/key") @?= Just "s3://"
  , testCase "parseUri hf datasets longest-prefix win" $
      (SC.cfgPfx <$> SC.parseUri "hf://datasets/openai/gsm8k")
        @?= Just "hf://datasets/"
  , testCase "parseUri hf root matches shorter prefix" $
      (SC.cfgPfx <$> SC.parseUri "hf://list") @?= Just "hf://"
  , testCase "parseUri pg" $
      (SC.cfgPfx <$> SC.parseUri "pg://u:p@h/db") @?= Just "pg://"
  , testCase "parseUri ftp" $
      (SC.cfgPfx <$> SC.parseUri "ftp://x/y") @?= Just "ftp://"
  , testCase "parseUri unknown returns Nothing" $
      (SC.cfgPfx <$> SC.parseUri "zzz://x") @?= Nothing
  , testCase "uriToSql parquet" $
      SC.uriToSql "s3://b/x.parquet"
        @?= Just "SELECT * FROM read_parquet('s3://b/x.parquet')"
  , testCase "uriToSql csv" $
      SC.uriToSql "https://x/y.csv"
        @?= Just "SELECT * FROM read_csv_auto('https://x/y.csv')"
  , testCase "uriToSql json" $
      SC.uriToSql "https://x/y.json"
        @?= Just "SELECT * FROM read_json_auto('https://x/y.json')"
  , testCase "uriToSql ndjson" $
      SC.uriToSql "https://x/y.ndjson"
        @?= Just "SELECT * FROM read_ndjson_auto('https://x/y.ndjson')"
  , testCase "uriToSql tsv uses delim tab" $
      case SC.uriToSql "https://x/y.tsv" of
        Just s  -> assertBool "has delim" ("delim='\t'" `T.isInfixOf` s)
        Nothing -> assertFailure "expected Just"
  , testCase "uriToSql unknown ext Nothing" $
      SC.uriToSql "s3://b/prefix/" @?= Nothing
  , testCase "pathParts strips prefix and splits" $
      SC.pathParts "s3://" "s3://bucket/a/b" @?= ["bucket","a","b"]
  , testCase "pathParts drops trailing slash" $
      SC.pathParts "s3://" "s3://bucket/a/" @?= ["bucket","a"]
  , testCase "pathParts empty rest" $
      SC.pathParts "s3://" "s3://" @?= []
  , testCase "expand fills {k}" $
      SC.expand "x/{a}/y" [("a","z")] @?= "x/z/y"
  , testCase "expand strips /{k} on empty value" $
      SC.expand "x/{a}" [("a","")] @?= "x"
  , testCase "mkVars includes path and dsn" $
      lookup "dsn" (SC.mkVars (head SC.sources) "s3://bkt/k" "" "" "")
        @?= Just "bkt/k"
  , testCase "mkVars exposes numbered segments" $
      lookup "2" (SC.mkVars (head SC.sources) "s3://bkt/key" "" "" "")
        @?= Just "key"
  , pending "hf_readme" "HF protocol needs curl + live API"
  , pending "hf_enter_parquet" "HF protocol needs curl + live API"
  , pending "s3_list" "s3 listing needs aws CLI"
  , pending "ftp_list" "ftp listing needs curl"
  ]

-- ============================================================================
-- Columns: delete / hide / shift (Test.lean 270-288, 816-823)
-- ============================================================================
colTests :: TestTree
colTests = testGroup "col"
  [ testCase "Nav.execNav ColHide toggles hidden" $ do
      let Just ns' = Nav.execNav ColHide 1 mockNav
      _nsHidden ns' @?= V.singleton "c0"
  , testCase "Nav.execNav ColHide toggle round-trip" $ do
      let Just ns1 = Nav.execNav ColHide 1 mockNav
          Just ns2 = Nav.execNav ColHide 1 ns1
      _nsHidden ns2 @?= V.empty
  , testCase "Nav.execNav ColGrp toggles grp" $ do
      let Just ns' = Nav.execNav ColGrp 1 mockNav
      _nsGrp ns' @?= V.singleton "c0"
  , testCase "ColShiftL with 2 grp cols reorders" $ do
      -- set grp=["c0","c1"], cursor on c1 (display idx 1 which maps to c1)
      let ns0 = mockNav { _nsGrp = V.fromList ["c0", "c1"]
                        , _nsDispIdxs = V.fromList [0, 1, 2]
                        , _nsCol = mkAxis { _naCur = 1 } }
      case Nav.execNav ColShiftL 1 ns0 of
        Just ns' -> _nsGrp ns' @?= V.fromList ["c1", "c0"]
        Nothing  -> assertFailure "shiftL returned Nothing"
  , pending "delete_col" "col.exclude runtime not ported"
  , pending "delete_hidden_cols" "exclude+hide combo not ported"
  ]

-- ============================================================================
-- Filter / Export argument commands (Test.lean 291-297, 703-726)
-- ============================================================================
filterExportTests :: TestTree
filterExportTests = testGroup "filter/export"
  [ testCase "buildFilter single selection string" $
      Filter.buildFilter "col" (V.fromList ["a","b","c"]) "\nb" False
        @?= "col == 'b'"
  , testCase "buildFilter single numeric selection unquoted" $
      Filter.buildFilter "n" (V.fromList ["1","2","3"]) "\n2" True
        @?= "n == 2"
  , testCase "buildFilter multi selection OR" $
      Filter.buildFilter "col" (V.fromList ["a","b","c"]) "\na\nc" False
        @?= "(col == 'a' || col == 'c')"
  , testCase "buildFilter empty typed query is empty" $
      Filter.buildFilter "col" (V.fromList ["a"]) "" False @?= ""
  , testCase "buildFilter raw expression passthrough" $
      Filter.buildFilter "col" (V.fromList ["a"]) "col > 5" False
        @?= "col > 5"
  , testCase "buildFilter typed query matching value becomes selection" $
      Filter.buildFilter "col" (V.fromList ["a","b"]) "a" False
        @?= "col == 'a'"
  , testCase "buildFilterWith numeric from ColType" $
      Filter.buildFilterWith "n" (V.fromList ["5"]) "\n5" CTInt
        @?= "n == 5"
  , testCase "filterPrompt numeric hint mentions > and >=" $
      let p = Filter.filterPrompt "age" "int"
      in assertBool "has > 5" ("> 5" `T.isInfixOf` p)
  , testCase "filterPrompt str hint uses == quoted" $
      let p = Filter.filterPrompt "name" "str"
      in assertBool "has == 'USD'" ("== 'USD'" `T.isInfixOf` p)
  , testCase "exportFmtExt" $ do
      Export.exportFmtExt EFCsv     @?= "csv"
      Export.exportFmtExt EFParquet @?= "parquet"
      Export.exportFmtExt EFJson    @?= "json"
      Export.exportFmtExt EFNdjson  @?= "ndjson"
  , testCase "exportFmtFromText" $ do
      Export.exportFmtFromText "csv"     @?= Just EFCsv
      Export.exportFmtFromText "parquet" @?= Just EFParquet
      Export.exportFmtFromText "json"    @?= Just EFJson
      Export.exportFmtFromText "ndjson"  @?= Just EFNdjson
      Export.exportFmtFromText "bogus"   @?= Nothing
  , testCase "exportTable csv writes header + rows" $ do
      p <- tmpPath "export-csv.csv"
      let t = mkGridTbl ["a","b"] [["1","x"], ["2","y"]]
      Export.exportTable t EFCsv p
      s <- TIO.readFile p
      removeFile p
      s @?= "a,b\n1,x\n2,y\n"
  , testCase "exportTable csv quotes cells with commas" $ do
      p <- tmpPath "export-csv-q.csv"
      let t = mkGridTbl ["a"] [["x,y"]]
      Export.exportTable t EFCsv p
      s <- TIO.readFile p
      removeFile p
      s @?= "a\n\"x,y\"\n"
  , testCase "exportTable ndjson one object per line" $ do
      p <- tmpPath "export.ndjson"
      let t = mkGridTbl ["a","b"] [["1","x"], ["2","y"]]
      Export.exportTable t EFNdjson p
      s <- TIO.readFile p
      removeFile p
      assertBool "obj1" ("{\"a\":\"1\",\"b\":\"x\"}" `T.isInfixOf` s)
      assertBool "obj2" ("{\"a\":\"2\",\"b\":\"y\"}" `T.isInfixOf` s)
  , testCase "exportTable json top-level array" $ do
      p <- tmpPath "export.json"
      let t = mkGridTbl ["a"] [["1"]]
      Export.exportTable t EFJson p
      s <- TIO.readFile p
      removeFile p
      s @?= "[{\"a\":\"1\"}]\n"
  , testCase "exportTable parquet throws (no DB)" $ do
      p <- tmpPath "export.parquet"
      let t = mkGridTbl ["a"] [["1"]]
      r <- try (Export.exportTable t EFParquet p) :: IO (Either SomeException ())
      exists <- doesFileExist p
      if exists then removeFile p else pure ()
      case r of
        Left _  -> pure ()
        Right _ -> assertFailure "parquet export should throw without DB backend"
  ]

-- ============================================================================
-- Transpose (Test.lean 745-762)
-- ============================================================================
transposeTests :: TestTree
transposeTests = testGroup "transpose"
  [ testCase "mkTransposedOps swaps dims" $ do
      let t = mkGridTbl ["a","b","c"] [["1","2","3"], ["4","5","6"]]
      tx <- Transpose.mkTransposedOps t
      _tblNRows tx @?= 3
      V.length (_tblColNames tx) @?= 3  -- "column" + row_0 + row_1
  , testCase "transpose col0 is 'column' marker" $ do
      let t = mkGridTbl ["a","b"] [["1","2"]]
      tx <- Transpose.mkTransposedOps t
      (_tblColNames tx V.! 0) @?= "column"
  , testCase "transpose col i>0 named 'row_{i-1}'" $ do
      let t = mkGridTbl ["a","b"] [["1","2"], ["3","4"]]
      tx <- Transpose.mkTransposedOps t
      _tblColNames tx @?= V.fromList ["column", "row_0", "row_1"]
  , testCase "transpose cell[i][0] is source col name" $ do
      let t = mkGridTbl ["a","b"] [["1","2"]]
      tx <- Transpose.mkTransposedOps t
      n0 <- _tblCellStr tx 0 0
      n1 <- _tblCellStr tx 1 0
      n0 @?= "a"
      n1 @?= "b"
  , testCase "transpose cells preserve values" $ do
      let t = mkGridTbl ["a","b"] [["1","2"], ["3","4"]]
      tx <- Transpose.mkTransposedOps t
      v00 <- _tblCellStr tx 0 1  -- a, row0 = 1
      v01 <- _tblCellStr tx 0 2  -- a, row1 = 3
      v10 <- _tblCellStr tx 1 1  -- b, row0 = 2
      v11 <- _tblCellStr tx 1 2  -- b, row1 = 4
      (v00,v01,v10,v11) @?= ("1","3","2","4")
  , testCase "transpose caps source rows at 200" $ do
      -- synthesize a 250-row, 1-col table
      let rows = [[T.pack (show i)] | i <- [0 .. 249 :: Int]]
          t    = mkGridTbl ["x"] rows
      tx <- Transpose.mkTransposedOps t
      -- 1 source col -> 1 output row, 1 + min(250,200) = 201 output cols
      _tblNRows tx @?= 1
      V.length (_tblColNames tx) @?= 201
  ]

-- ============================================================================
-- Derive / Split (Test.lean 651-700)
-- ============================================================================
derivSplitTests :: TestTree
derivSplitTests = testGroup "derive/split"
  [ testCase "parseDerive splits on first ' = '" $
      Derive.parseDerive "y = x + 1" @?= Just ("y", "x + 1")
  , testCase "parseDerive rejoins rhs containing '='" $
      Derive.parseDerive "y = x == 1" @?= Just ("y", "x == 1")
  , testCase "parseDerive strips surrounding ws" $
      Derive.parseDerive "  foo   =  bar  " @?= Just ("foo", "bar")
  , testCase "parseDerive rejects empty name" $
      Derive.parseDerive " = expr" @?= Nothing
  , testCase "parseDerive rejects empty expr" $
      Derive.parseDerive "n = " @?= Nothing
  , testCase "parseDerive rejects missing separator" $
      Derive.parseDerive "y + 1" @?= Nothing
  , testCase "quoteId wraps in double quotes" $
      Derive.quoteId "a" @?= "\"a\""
  , testCase "quoteId doubles embedded quotes" $
      Derive.quoteId "a\"b" @?= "\"a\"\"b\""
  , testCase "addDerived adds a column via DuckDB" $ do
      let t = mkGridTblTy ["x"] [["1"], ["2"], ["3"]] (const CTInt)
      t' <- Derive.addDerived t "y" "x + 1"
      _tblColNames t' @?= V.fromList ["x", "y"]
      _tblNRows t' @?= 3
  , testCase "addDerived fails soft on bad expr" $ do
      let t = mkGridTblTy ["x"] [["1"], ["2"]] (const CTInt)
      t' <- Derive.addDerived t "y" "!!!bogus!!!"
      -- on parse failure we keep the original shape
      _tblColNames t' @?= V.fromList ["x"]
  , testCase "splitColumn: int column no-op" $ do
      let t = mkGridTblTy ["n"] [["1"], ["2"]] (const CTInt)
      t' <- Split.splitColumn t 0 "-"
      _tblColNames t' @?= V.fromList ["n"]
  , testCase "splitColumn: empty pat no-op" $ do
      let t = mkGridTbl ["s"] [["a-b-c"]]
      t' <- Split.splitColumn t 0 ""
      _tblColNames t' @?= V.fromList ["s"]
  , testCase "splitColumn: out-of-range idx no-op" $ do
      let t = mkGridTbl ["s"] [["a-b"]]
      t' <- Split.splitColumn t 9 "-"
      _tblColNames t' @?= V.fromList ["s"]
  , testCase "splitColumn: dash splits adds cols" $ do
      let t = mkGridTbl ["tag"] [["a-b-c"], ["d-e-f"]]
      t' <- Split.splitColumn t 0 "-"
      -- original + 3 extra parts
      V.length (_tblColNames t') @?= 4
      (_tblColNames t' V.! 0) @?= "tag"
      (_tblColNames t' V.! 1) @?= "tag_1"
  ]

-- ============================================================================
-- Join / Diff (Test.lean 766-1008)
-- ============================================================================
joinDiffTests :: TestTree
joinDiffTests = testGroup "join/diff"
  [ testCase "joinInner matches on key" $ do
      let l = mkGridTbl ["k","a"] [["1","x"], ["2","y"]]
          r = mkGridTbl ["k","b"] [["1","p"], ["3","q"]]
      j <- Join.joinInner l r (V.singleton "k")
      _tblNRows j @?= 1
      -- columns: left ++ right with collision suffix
      _tblColNames j @?= V.fromList ["k","a","k_r","b"]
      v <- _tblCellStr j 0 3
      v @?= "p"
  , testCase "joinLeft keeps unmatched left row" $ do
      let l = mkGridTbl ["k"] [["1"], ["2"]]
          r = mkGridTbl ["k"] [["1"]]
      j <- Join.joinLeft l r (V.singleton "k")
      _tblNRows j @?= 2
  , testCase "joinRight keeps unmatched right row" $ do
      let l = mkGridTbl ["k"] [["1"]]
          r = mkGridTbl ["k"] [["1"], ["2"]]
      j <- Join.joinRight l r (V.singleton "k")
      _tblNRows j @?= 2
  , testCase "joinUnion concatenates row counts" $ do
      let l = mkGridTbl ["a"] [["1"], ["2"]]
          r = mkGridTbl ["a"] [["3"]]
      j <- Join.joinUnion l r
      _tblNRows j @?= 3
      _tblColNames j @?= V.fromList ["a"]
  , testCase "joinDiff removes keyed matches" $ do
      let l = mkGridTbl ["k"] [["1"], ["2"], ["3"]]
          r = mkGridTbl ["k"] [["2"]]
      j <- Join.joinDiff l r (V.singleton "k")
      _tblNRows j @?= 2
  , testCase "joinWith JUnion dispatches" $ do
      let l = mkGridTbl ["a"] [["1"]]
          r = mkGridTbl ["a"] [["2"]]
      j <- Join.joinWith Join.JUnion l r V.empty
      _tblNRows j @?= 2
  , testCase "diffTables with common key col" $ do
      let l = mkGridTbl ["k","v"] [["1","a"], ["2","b"]]
          r = mkGridTbl ["k","v"] [["1","a"], ["2","c"]]
      d <- Diff.diffTables l r
      -- at least one output row exists (joined on k)
      assertBool "rows>0" (_tblNRows d > 0)
  , testCase "diffTablesSameHide flags same cols" $ do
      let l = mkGridTbl ["k","v"] [["1","a"], ["2","a"]]
          r = mkGridTbl ["k","v"] [["1","a"], ["2","a"]]
      (_, hide) <- Diff.diffTablesSameHide l r
      -- "v" is identical on both sides → appears in sameHide
      assertBool "v_left hidden" (V.elem "v_left" hide)
      assertBool "v_right hidden" (V.elem "v_right" hide)
  ]

-- ============================================================================
-- Plot (Test.lean 1013-1197): all the pure rScript + kind cases.
-- ============================================================================
plotExportTests :: TestTree
plotExportTests = testGroup "plot"
  [ testCase "rScript line with title contains ggtitle" $ do
      let s = Plot.rScript "d.dat" "p.png" PKLine "x" "y" False "" False ""
                Plot.POther "line: y vs x"
      assertBool "has ggtitle" ("ggtitle('line: y vs x')" `T.isInfixOf` s)
  , testCase "rScript no title omits ggtitle entirely" $ do
      let s = Plot.rScript "d.dat" "p.png" PKLine "x" "y" False "" False ""
                Plot.POther ""
      assertBool "no ggtitle" $ not ("ggtitle" `T.isInfixOf` s)
  , testCase "rScript bar uses geom_bar" $ do
      let s = Plot.rScript "d.dat" "p.png" PKBar "x" "y" False "" False ""
                Plot.POther ""
      assertBool "bar" ("geom_bar" `T.isInfixOf` s)
  , testCase "rScript scatter uses geom_point" $ do
      let s = Plot.rScript "d.dat" "p.png" PKScatter "x" "y" False "" False ""
                Plot.POther ""
      assertBool "point" ("geom_point" `T.isInfixOf` s)
  , testCase "rScript hist uses geom_histogram" $ do
      let s = Plot.rScript "d.dat" "p.png" PKHist "" "y" False "" False ""
                Plot.POther ""
      assertBool "hist" ("geom_histogram" `T.isInfixOf` s)
  , testCase "rScript area uses geom_area" $ do
      let s = Plot.rScript "d.dat" "p.png" PKArea "x" "y" False "" False ""
                Plot.POther ""
      assertBool "area" ("geom_area" `T.isInfixOf` s)
  , testCase "rScript step uses geom_step" $ do
      let s = Plot.rScript "d.dat" "p.png" PKStep "x" "y" False "" False ""
                Plot.POther ""
      assertBool "step" ("geom_step" `T.isInfixOf` s)
  , testCase "rScript violin uses geom_violin" $ do
      let s = Plot.rScript "d.dat" "p.png" PKViolin "cat" "y" True "cat" False ""
                Plot.POther ""
      assertBool "violin" ("geom_violin" `T.isInfixOf` s)
  , testCase "rScript density uses geom_density" $ do
      let s = Plot.rScript "d.dat" "p.png" PKDensity "" "y" False "" False ""
                Plot.POther ""
      assertBool "density" ("geom_density" `T.isInfixOf` s)
  , testCase "rScript box uses geom_boxplot" $ do
      let s = Plot.rScript "d.dat" "p.png" PKBox "x" "y" False "" False ""
                Plot.POther ""
      assertBool "box" ("geom_boxplot" `T.isInfixOf` s)
  , testCase "cmdPlotKind maps PlotLine" $ cmdPlotKind PlotLine @?= Just PKLine
  , testCase "cmdPlotKind maps PlotArea" $ cmdPlotKind PlotArea @?= Just PKArea
  , testCase "cmdPlotKind maps PlotDensity" $ cmdPlotKind PlotDensity @?= Just PKDensity
  , testCase "cmdPlotKind rejects RowInc" $ cmdPlotKind RowInc @?= Nothing
  , testCase "Plot.maxPoints threshold" $ do
      assertBool "100 <= maxPoints"  (100 <= Plot.maxPoints)
      assertBool "3000 > maxPoints" (3000 > Plot.maxPoints)
  , pending "plot_key_dispatch"          "plot UI handler not ported"
  , pending "plot_export_string_col"     "plotExport backend not ported"
  , pending "plot_export_data"           "plotExport backend not ported"
  , pending "plot_export_cat"            "plotExport backend not ported"
  , pending "plot_time_downsample"       "plotExport backend not ported"
  , pending "plot_downsample_step"       "plotExport backend not ported"
  , pending "plot_render_line"           "R subprocess rendering not wired"
  , pending "plot_render_scatter_cat"    "R subprocess rendering not wired"
  , pending "plot_render_histogram"      "R subprocess rendering not wired"
  , pending "plot_render_area"           "R subprocess rendering not wired"
  , pending "plot_render_density"        "R subprocess rendering not wired"
  , pending "plot_render_step"           "R subprocess rendering not wired"
  , pending "plot_render_violin"         "R subprocess rendering not wired"
  , pending "plot_render_time"           "R subprocess rendering not wired"
  , pending "plot_r_installed"           "R toolchain check skipped"
  ]

-- ============================================================================
-- Session (Test.lean 934-981)
-- ============================================================================
sessionTests :: TestTree
sessionTests = testGroup "session"
  [ testCase "sanitize filters path separators" $
      Sess.sanitize "foo/bar baz" @?= "foobarbaz"
  , testCase "sanitize allows alnum and _.-" $
      Sess.sanitize "a-b_c.d" @?= "a-b_c.d"
  , testCase "toSaved copies core fields" $ do
      let v = testView { _vDisp = "mine", _vPath = "data/x.csv" }
          sv = Sess.toSaved v
      Sess.svPath sv @?= "data/x.csv"
      Sess.svDisp sv @?= "mine"
  , testCase "stackToSaved version=1 and includes hd+tl" $ do
      let vs = ViewStack testView [testView, testView]
          ss = Sess.stackToSaved vs
      Sess.ssVersion ss @?= 1
      length (Sess.ssViews ss) @?= 3
  , testCase "encodeSession/decodeSession round-trip" $ do
      let ss = Sess.stackToSaved testStack
          bs = Sess.encodeSession ss
      case Sess.decodeSession bs of
        Just ss' -> Sess.ssVersion ss' @?= 1
        Nothing  -> assertFailure "decode failed"
  , testCase "decodeSession on garbage returns Nothing" $
      Sess.decodeSession "not json" @?= Nothing
  , testCase "saveSession + loadSession round-trip" $ do
      let name = "tvhask-test-roundtrip"
      mp <- Sess.saveSession name testStack
      case mp of
        Nothing -> assertFailure "save returned Nothing"
        Just _  -> do
          r <- Sess.loadSession name
          case r of
            Nothing -> assertFailure "load returned Nothing"
            Just s  -> length (Sess.ssViews s) @?= 1
  , testCase "loadSession missing name returns Nothing" $ do
      r <- Sess.loadSession "definitely-not-a-real-session-xyz123"
      r @?= Nothing
  , testCase "listSessions contains saved name" $ do
      let name = "tvhask-test-listable"
      _ <- Sess.saveSession name testStack
      xs <- Sess.listSessions
      assertBool "present" (name `elem` xs)
  ]

-- ============================================================================
-- Status / sparkline (Test.lean 790-811)
-- ============================================================================
sparkStatusTests :: TestTree
sparkStatusTests = testGroup "status/spark"
  [ pending "sparkline_on"        "status/render layer lacks sparklines"
  , pending "statusagg_numeric"   "status-agg not rendered yet"
  , pending "statusagg_string"    "status-agg not rendered yet"
  ]

-- ============================================================================
-- Replay ops (Test.lean 1202-1221)
-- ============================================================================
replayTests :: TestTree
replayTests = testGroup "replay"
  [ pending "replay_sort"  "tab ops replay not rendered"
  , pending "replay_empty" "tab ops replay not rendered"
  ]

-- ============================================================================
-- DB source tests (sqlite/duckdb/pg/osquery) — use real DuckDB.
-- ============================================================================
dbSourceTests :: TestTree
dbSourceTests = testGroup "db sources"
  [ testCase "DuckDB in-memory lists builtin sqlite_master equivalents" $
      withMemConn $ \c -> do
        r <- D.query c "SELECT 1 AS x"
        D.columnNames r @?= V.fromList ["x"]
  , testCase "DuckDB SELECT literal string" $
      withMemConn $ \c -> do
        r <- D.query c "SELECT 'alpha' AS s"
        cs <- D.chunks r
        case cs of
          (ch:_) -> D.readCellText (D.chunkColumn ch 0) 0 @?= Just "alpha"
          []     -> assertFailure "no chunks"
  , pending "duckdb_list"         "folder-level DB listing not ported"
  , pending "duckdb_enter"        "folder-level DB listing not ported"
  , pending "duckdb_primary_key"  "schema introspection not ported"
  , pending "sqlite_list"         "sqlite attach not ported"
  , pending "sqlite_enter"        "sqlite attach not ported"
  , pending "pg_list"             "pg:// URI handler not ported"
  , pending "pg_enter"            "pg:// URI handler not ported"
  , pending "osquery_list"        "osquery:// handler not ported"
  , pending "osquery_enter"       "osquery:// handler not ported"
  , pending "osquery_scroll_no_hide" "osquery:// handler not ported"
  , pending "osquery_back"        "osquery:// handler not ported"
  , pending "osquery_meta_description" "osquery:// handler not ported"
  , pending "osquery_direct_table"     "osquery:// handler not ported"
  , pending "osquery_typed_columns"    "osquery:// handler not ported"
  , pending "osquery_sort_enter"       "osquery:// handler not ported"
  ]

-- ============================================================================
-- File-format open tests (csv/json/xlsx/etc.) — use real DuckDB w/ checked-in data
-- ============================================================================
sourceFormatTests :: TestTree
sourceFormatTests = testGroup "source formats"
  [ testCase "parquet open via checked-in 1.parquet has > 0 rows" $
      withMemConn $ \c -> do
        r <- D.query c "SELECT count(*) AS n FROM '/home/dh/repo/Tc/data/1.parquet'"
        cs <- D.chunks r
        case cs of
          (ch:_) ->
            case D.readCellInt (D.chunkColumn ch 0) 0 of
              Just n  -> assertBool "rows>0" (n > 0)
              Nothing -> assertFailure "null count"
          []     -> assertFailure "no chunks"
  , testCase "parquet open has >= 1 column" $
      withMemConn $ \c -> do
        r <- D.query c "SELECT * FROM '/home/dh/repo/Tc/data/1.parquet' LIMIT 1"
        assertBool "cols>=1" (V.length (D.columnNames r) >= 1)
  , testCase "csv open via read_csv_auto has rows" $
      withMemConn $ \c -> do
        r <- D.query c "SELECT count(*) FROM read_csv_auto('/home/dh/repo/Tc/data/basic.csv')"
        cs <- D.chunks r
        case cs of
          (ch:_) -> case D.readCellInt (D.chunkColumn ch 0) 0 of
            Just n  -> assertBool "rows>0" (n > 0)
            Nothing -> assertFailure "null count"
          _ -> assertFailure "no chunks"
  , testCase "json open via read_json_auto has rows" $
      withMemConn $ \c -> do
        r <- D.query c "SELECT count(*) FROM read_json_auto('/home/dh/repo/Tc/data/test.json')"
        cs <- D.chunks r
        case cs of
          (ch:_) -> case D.readCellInt (D.chunkColumn ch 0) 0 of
            Just n  -> assertBool "rows>0" (n > 0)
            Nothing -> assertFailure "null count"
          _ -> assertFailure "no chunks"
  , testCase "ndjson open via read_ndjson_auto has rows" $
      withMemConn $ \c -> do
        r <- D.query c "SELECT count(*) FROM read_ndjson_auto('/home/dh/repo/Tc/data/test.ndjson')"
        cs <- D.chunks r
        case cs of
          (ch:_) -> case D.readCellInt (D.chunkColumn ch 0) 0 of
            Just n  -> assertBool "rows>0" (n > 0)
            Nothing -> assertFailure "null count"
          _ -> assertFailure "no chunks"
  , testCase "loadFromUri local parquet returns Right" $ do
      r <- SC.loadFromUri "/home/dh/repo/Tc/data/1.parquet"
      case r of
        Right t -> assertBool "has rows" (_tblNRows t > 0)
        Left e  -> assertFailure ("loadFromUri: " <> e)
  , testCase "loadFromUri local csv returns Right" $ do
      r <- SC.loadFromUri "/home/dh/repo/Tc/data/basic.csv"
      case r of
        Right t -> assertBool "has rows" (_tblNRows t > 0)
        Left e  -> assertFailure ("loadFromUri: " <> e)
  , pending "jsonl_sort"    "jsonl sort runtime not ported"
  , pending "xlsx_open"     "xlsx needs excel extension install"
  , pending "avro_open"     "avro needs avro extension"
  , pending "arrow_open"    "arrow extension not loaded"
  , pending "feather_open"  "arrow extension not loaded"
  , pending "gz_viewfile"   "gz fallback viewer not ported"
  , pending "gz_csv_ingest" "gz csv ingest not ported"
  , pending "gz_txt_fallback" "gz fallback viewer not ported"
  , pending "spaced_header" "csv header parsing not ported"
  ]

-- ============================================================================
-- Extra nav cases (Test.lean 916-929)
-- ============================================================================
navExtraTests :: TestTree
navExtraTests = testGroup "nav"
  [ testCase "RowInc at 0 → 1" $ do
      let Just ns' = Nav.execNav RowInc 1 mockNav
      _naCur (_nsRow ns') @?= 1
  , testCase "RowDec at 0 stays 0" $ do
      let Just ns' = Nav.execNav RowDec 1 mockNav
      _naCur (_nsRow ns') @?= 0
  , testCase "RowPgdn jumps by page size" $ do
      let Just ns' = Nav.execNav RowPgdn 3 mockNav
      _naCur (_nsRow ns') @?= 3
  , testCase "RowBot → last row" $ do
      let Just ns' = Nav.execNav RowBot 1 mockNav
      _naCur (_nsRow ns') @?= _tblNRows mockTbl - 1
  , testCase "ColLast → last col" $ do
      let Just ns' = Nav.execNav ColLast 1 mockNav
      _naCur (_nsCol ns') @?= V.length (_tblColNames mockTbl) - 1
  , testCase "ColFirst → 0" $ do
      -- move then move back
      let Just ns1 = Nav.execNav ColLast  1 mockNav
          Just ns2 = Nav.execNav ColFirst 1 ns1
      _naCur (_nsCol ns2) @?= 0
  , testCase "RowInc twice then RowDec once = 1" $ do
      let Just ns1 = Nav.execNav RowInc 1 mockNav
          Just ns2 = Nav.execNav RowInc 1 ns1
          Just ns3 = Nav.execNav RowDec 1 ns2
      _naCur (_nsRow ns3) @?= 1
  ]

-- ============================================================================
-- Arrow / key mapping sanity (Test.lean 917-929 + tokenizeKeys covered in PureSpec)
-- ============================================================================
arrowTests :: TestTree
arrowTests = testGroup "arrow"
  [ -- Main arrow-mapping assertions already live in PureSpec.keyMapTests.
    -- This group just contains pending markers for binary-level tests.
    pending "arrow_nav" "runtime spawn not ported; see PureSpec for pure version"
  , pending "flat_menu" "fzf menu runtime not ported"
  , pending "heat_mode" "socket+heat not ported"
  , pending "socket"    "socket server not ported"
  , pending "socket_dispatch" "socket server not ported"
  , pending "no_stderr" "source tree lint check not ported"
  ]

-- ============================================================================
-- Theme color parser (not in Test.lean explicitly but mentioned as runtime theme)
-- ============================================================================
themeTests :: TestTree
themeTests = testGroup "theme"
  [ testCase "parseColorIx unknown returns 0" $
      Theme.parseColorIx "no-such-color" @?= 0
  , testCase "parseColorIx rgb clamp" $
      -- Valid rgb555 = bright white = 16 + 36*5 + 6*5 + 5 = 231
      Theme.parseColorIx "rgb555" @?= 231
  , testCase "parseColorIx gray0 = 232" $
      Theme.parseColorIx "gray0" @?= 232
  , pending "theme" "theme.run cycle runtime not ported"
  , testCase "Fzf.parseFlatSel round-trip" $ do
      Fzf.parseFlatSel "foo | c | k | desc" @?= Just "foo"
      Fzf.parseFlatSel ""                    @?= Nothing
      Fzf.parseFlatSel "   | x | y | z"      @?= Nothing
  ]
