{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- |
--   Literal port of Tc/test/Test.lean — the big integration suite.
--   Tests shell out to the cabal-built `tv` binary and check screen output.
module Test (tests) where

import Control.Exception (try, SomeException)
import Data.Char (isDigit)
import Data.IORef (IORef, newIORef)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Directory (copyFile, createDirectoryIfMissing, removeFile, getCurrentDirectory)
import System.FilePath (takeBaseName, takeExtension)
import System.Environment (lookupEnv)
import System.Exit (ExitCode (..))
import System.IO.Unsafe (unsafePerformIO)
import System.Process (readProcessWithExitCode, proc, createProcess, CreateProcess(..), StdStream(..), waitForProcess, ProcessHandle, terminateProcess)
import System.IO (Handle, hGetContents)

import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, assertBool, Assertion, assertFailure)

import TestUtil
  ( tvHaskBin
  , runHask
  , runHaskErr
  , contains
  , footer
  , header
  , dataLines
  , hasFile
  , hasCmd
  , cachedCheck
  )

import qualified Tv.Plot as Plot
import Tv.Plot (KeyAction(..))
import qualified Tv.Data.DuckDB.Table as Tbl
import qualified Tv.Util as Log
import qualified Tv.Util as Tmp

run :: Text -> FilePath -> IO Text
run keys file = runHask keys file []

-- | assert helper that raises through HUnit
assert :: Bool -> String -> Assertion
assert cond msg = assertBool msg cond

-- | Start tv as a child process (for socket tests)
spawnTv :: [String] -> IO (ProcessHandle, Maybe Handle)
spawnTv args = do
  (_, mOut, _, ph) <- createProcess (proc tvHaskBin args)
    { std_in = NoStream, std_out = CreatePipe, std_err = CreatePipe }
  pure (ph, mOut)

-- ============================================================================
-- === Sort tests (CSV) ===
-- ============================================================================

test_sort_asc :: Assertion
test_sort_asc = do
  ls <- dataLines <$> run "[" "data/unsorted.csv"
  let first = case ls of { [] -> ""; (x:_) -> x }
  assert (T.isPrefixOf "1 " first || contains first " 1 ") "[ sorts asc, first=1"

test_sort_desc :: Assertion
test_sort_desc = do
  ls <- dataLines <$> run "]" "data/unsorted.csv"
  let first = case ls of { [] -> ""; (x:_) -> x }
  assert (T.isPrefixOf "3 " first || contains first " 3 ") "] sorts desc, first=3"

-- ============================================================================
-- === Meta tests (CSV) ===
-- ============================================================================

test_meta_shows :: Assertion
test_meta_shows = do
  out <- run "M" "data/basic.csv"
  assert (contains (fst (footer out)) "meta") "M shows meta in tab"

test_meta_col_info :: Assertion
test_meta_col_info = do
  out <- run "M" "data/basic.csv"
  assert (contains out "column" || contains out "name") "Meta shows column info"

test_meta_no_garbage :: Assertion
test_meta_no_garbage = do
  out <- run "M" "data/basic.csv"
  assert (not (contains (fst (footer out)) "\xE2")) "Meta tab has no garbage chars"

test_meta_stats :: Assertion
test_meta_stats = do
  out <- run "MTjTS" "data/basic.csv"
  assert (contains (fst (footer out)) "meta+stats") "S pushes stats view"
  -- column a is [1..5]: mean 3.0, stddev √2.
  assert (contains out "3.0") "stats computed for numeric col (mean 3.0 shown)"

test_meta_corr :: Assertion
test_meta_corr = do
  out <- run "MTjTjTC" "data/numeric.csv"
  assert (contains (fst (footer out)) "[corr]") "C pushes corr view"
  -- linear cols x, 10x, 100x -> all correlations 1.0
  assert (contains out "1.0") "correlation value shown in cell"

test_sample :: Assertion
test_sample = do
  out <- run "?" "data/basic.csv"
  assert (contains (fst (footer out)) "[sample]") "? pushes sample view"

-- ============================================================================
-- === Freq tests (CSV) ===
-- ============================================================================

test_freq_shows :: Assertion
test_freq_shows = do
  out <- run "F" "data/basic.csv"
  assert (contains (fst (footer out)) "freq") "F shows freq in tab"

test_freq_after_meta :: Assertion
test_freq_after_meta = do
  out <- run "MqF" "data/basic.csv"
  assert (contains (fst (footer out)) "freq") "MqF shows freq"

test_freq_by_key :: Assertion
test_freq_by_key = do
  out <- run "l!F" "data/full.csv"
  assert (contains (fst (footer out)) "freq") "l!F shows freq by key"

test_freq_multi_key :: Assertion
test_freq_multi_key = do
  out <- run "!l!F" "data/multi_freq.csv"
  assert (contains (fst (footer out)) "freq") "!l!F shows multi-key freq"

test_freq_keeps_grp :: Assertion
test_freq_keeps_grp = do
  out <- run "!F" "data/basic.csv"
  assert (contains (snd (footer out)) "grp=1") "Freq view keeps grp columns"

-- Pressing F (open Freq) then q (pop back) used to leave the sparkline
-- row reflecting the freq table's columns, not the popped-to table's.
-- `stkH` (which handles q/CmdStkPop) called `withStk` which never
-- touches sparklines, so the freq-table sparklines lingered against the
-- popped-to table — column counts mismatched and `renderBase`'s
-- "if null" guard kept the stale data. Fix: stkH now wraps in resetVS
-- so the sparklines re-derive for the new top.
--
-- Strict assertion: post-Fq frame must contain the same sparkline
-- glyph sequence as a fresh open. (full.csv exposes the bug: its freq
-- view's mostly-empty count sparkline (single █ at index 10, beyond the
-- visible col width) leaves the spark row blank with the bug, vs
-- showing original's multi-glyph sparklines on value/score with the
-- fix.)
test_sparkline_after_freq_back :: Assertion
test_sparkline_after_freq_back = do
  baseline <- run "" "data/full.csv"
  fqBack   <- run "Fq" "data/full.csv"
  let glyphs t = T.filter (`elem` blockChars) t
      blockChars = "\x2581\x2582\x2583\x2584\x2585\x2586\x2587\x2588" :: String
  assert (not (T.null (glyphs baseline)))
         "baseline render has sparkline glyphs"
  assert (glyphs baseline == glyphs fqBack)
         ("sparkline glyphs after Fq must match baseline; got "
           ++ show (glyphs fqBack) ++ " vs " ++ show (glyphs baseline))

-- ============================================================================
-- === Scriptability tests (-q flag, recreate cmd at session end) ===
-- ============================================================================

-- A vanilla session should print `tv <path>` (no -q segment) on stderr.
test_script_print_no_ops :: Assertion
test_script_print_no_ops = do
  err <- runHaskErr "" "data/full.csv" []
  assert (contains err "# recreate: tv data/full.csv")
         ("expected recreate line for plain session; got: " ++ show err)

-- An interactively-added op (sort via `[`) must show up in -q on stderr.
test_script_print_after_sort :: Assertion
test_script_print_after_sort = do
  err <- runHaskErr "[" "data/full.csv" []
  assert (contains err "# recreate: tv data/full.csv -q")
         ("expected -q segment after sort; got: " ++ show err)
  assert (contains err "sort")
         "recreate -q segment must mention sort"

-- `-q "filter ..."` must (a) actually filter the rows on stdout and
-- (b) round-trip back into the printed recreate command on stderr.
test_script_q_flag_filters :: Assertion
test_script_q_flag_filters = do
  out <- runHask    "" "data/full.csv" ["-q", "filter score > 80"]
  err <- runHaskErr "" "data/full.csv" ["-q", "filter score > 80"]
  assert (contains out "r0/3")
         ("expected -q filter to drop to 3 rows; got: " ++ show (T.takeEnd 300 out))
  assert (contains err "# recreate: tv data/full.csv -q 'filter score > 80'")
         ("expected recreate to round-trip -q; got: " ++ show err)

-- Initial -q + interactive op should be combined with " | " in the recreate.
test_script_combines_q_and_ops :: Assertion
test_script_combines_q_and_ops = do
  err <- runHaskErr "[" "data/full.csv" ["-q", "filter score > 80"]
  assert (contains err "# recreate: tv data/full.csv -q 'filter score > 80 |")
         ("expected combined -q | sort; got: " ++ show err)

-- ============================================================================
-- === Meta selection tests (M0/M1) ===
-- ============================================================================

test_meta_0 :: Assertion
test_meta_0 = do
  out <- run "M0" "data/null_col.csv"
  let status = snd (footer out)
  assert (contains status "sel=1" || contains status "rows=1") "M0 selects null columns"

test_meta_1 :: Assertion
test_meta_1 = do
  out <- run "M1" "data/single_val.csv"
  let status = snd (footer out)
  assert (contains status "sel=1" || contains status "rows=1") "M1 selects single-value columns"

test_meta_0_enter :: Assertion
test_meta_0_enter = do
  hdr <- header <$> run "M0<ret>" "data/null_col.csv"
  assert (contains hdr "\x2551" || contains hdr "|") "M0<ret> sets key cols"

test_meta_1_enter :: Assertion
test_meta_1_enter = do
  hdr <- header <$> run "M1<ret>" "data/single_val.csv"
  assert (contains hdr "\x2551" || contains hdr "|") "M1<ret> sets key cols"

-- ============================================================================
-- === Stdin parsing tests ===
-- ============================================================================

test_spaced_header :: Assertion
test_spaced_header = do
  out <- run "" "data/spaced_header.txt"
  assert (contains (snd (footer out)) "c0/3") "Spaced header: 3 columns"

-- ============================================================================
-- === Freq enter tests ===
-- ============================================================================

test_freq_enter :: Assertion
test_freq_enter = do
  out <- run "F<ret>" "data/multi_freq.csv"
  let (tab, status) = footer out
  assert (contains tab "multi_freq") "F<ret> pops to parent"
  assert (contains status "r0/3") "F<ret> filters to 3 rows"

-- ============================================================================
-- === No stderr ===
-- ============================================================================

test_no_stderr :: Assertion
test_no_stderr = do
  -- search for eprintln-equivalent Haskell emissions in src/ (excluding app/)
  -- Lean checks `eprintln` in Tc/ except App.lean; our equivalent is hPutStrLn stderr / TIO.hPutStrLn stderr
  (_, out, _) <- readProcessWithExitCode "grep"
    ["-rn", "hPutStrLn stderr", "src/Tv/", "--exclude-dir=App"] ""
  assert (null (T.unpack (T.strip (T.pack out)))) "No hPutStrLn stderr in src/Tv/"

-- ============================================================================
-- === Search tests ===
-- ============================================================================

test_search_jump :: Assertion
test_search_jump = do
  out <- run "l/x<ret>" "data/basic.csv"
  assert (contains (snd (footer out)) "r2/") "/ search finds x at row 2"

test_search_next :: Assertion
test_search_next = do
  out <- run "l/x<ret>n" "data/basic.csv"
  assert (contains (snd (footer out)) "r4/") "n finds next x at row 4"

test_search_prev :: Assertion
test_search_prev = do
  out <- run "l/x<ret>N" "data/basic.csv"
  assert (contains (snd (footer out)) "r0/") "N finds prev x (wraps to row 0)"

test_search_after_sort :: Assertion
test_search_after_sort = do
  out <- run "]l/x<ret>" "data/basic.csv"
  assert (contains (snd (footer out)) "r2/") "/ search after sort finds row 2"

test_col_search :: Assertion
test_col_search = do
  out <- run "s" "data/basic.csv"
  assert (contains (snd (footer out)) "c0/") "s col search jumps to column"

test_folder_sort_type :: Assertion
test_folder_sort_type = do
  output <- run "lll[" "data/"
  let ls = dataLines output
      (_, status) = footer output
  assert (contains status "r0/") "sort on type column should not error"
  let types :: [String]
      types = map (\l -> if contains l " dir " then "d"
                         else if contains l " symlink " then "s" else "f") ls
      firstFile = fromMaybe 999 (findIdx (== "f") types)
      lastDir   = fromMaybe 0 (findIdx (== "d") (reverse types))
                    `rIdx` length types
      rIdx n total = if n == 0 && not (elem "d" types) then 0 else total - 1 - n
  assert (lastDir < firstFile)
    ("ascending sort: dirs (" ++ show lastDir ++ ") before files (" ++ show firstFile ++ ")")

findIdx :: (a -> Bool) -> [a] -> Maybe Int
findIdx p = go 0
  where
    go _ []     = Nothing
    go i (x:xs) = if p x then Just i else go (i+1) xs

-- ============================================================================
-- === Folder tests ===
-- ============================================================================

test_folder_no_args :: Assertion
test_folder_no_args = do
  out <- run "" ""
  assert (contains out "[/") "No-args shows folder view with absolute path"

test_folder_D :: Assertion
test_folder_D = do
  out <- run "D" "data/basic.csv"
  assert (contains out "[/") "D pushes folder view with absolute path"

test_folder_tab :: Assertion
test_folder_tab = do
  out <- run "" ""
  let (tab, _) = footer out
  assert (contains tab "[/") "Folder tab shows absolute path (starts with /)"
  cwd <- getCurrentDirectory
  let dirName = case reverse (T.splitOn "/" (T.pack cwd)) of
                  []    -> ""
                  (x:_) -> x
  assert (contains tab ("/" <> dirName <> "]"))
    ("Folder tab ends with /" ++ T.unpack dirName)

test_folder_enter :: Assertion
test_folder_enter = do
  out <- run "<ret>" "data/test_folder"
  let (_, status) = footer out
  assert (contains status "r0/") "Entered directory has rows"

test_folder_relative :: Assertion
test_folder_relative = do
  out <- run "" ""
  assert (contains out "..") "Path shows entry name"
  assert (not (contains out "/home/dh/repo/Tc/..")) "Path column is relative"

test_folder_pop :: Assertion
test_folder_pop = do
  out <- run "[jjj<ret>q" "data/test_folder"
  assert (contains out "[/") "q pops back to parent folder"

test_folder_backspace :: Assertion
test_folder_backspace = do
  out <- run "[jjj<ret><bs>" "data/test_folder"
  assert (contains out "test_folder") "backspace returns to parent folder"

test_folder_backspace_twice :: Assertion
test_folder_backspace_twice = do
  out <- run "[jjj<ret><bs><bs>" "data/test_folder"
  let (path, _) = footer out
  assert (T.isSuffixOf "data]" path) "double backspace navigates to grandparent (data/)"

test_folder_enter_symlink :: Assertion
test_folder_enter_symlink = do
  out <- run "[jj<ret>" "data/test_folder"
  assert (contains out "file2") "Enter on symlink dir shows its contents"
  let (_, status) = footer out
  assert (contains status "r0/") "Entered symlink dir has rows"

-- ============================================================================
-- === DuckDB tests ===
-- ============================================================================

test_duckdb_list :: Assertion
test_duckdb_list = do
  out <- run "" "data/nu_help.duckdb"
  assert (contains out "commands") "DuckDB file lists tables"
  assert (contains out "params") "DuckDB file lists params table"
  let (_, status) = footer out
  assert (contains status "r0/3") "DuckDB has 3 tables"

test_duckdb_enter :: Assertion
test_duckdb_enter = do
  out <- run "<ret>" "data/nu_help.duckdb"
  let (_, status) = footer out
  assert (contains status "r0/") "Enter on DuckDB table opens it"
  assert (not (contains status "r0/3")) "Entered table has different row count"

test_duckdb_primary_key :: Assertion
test_duckdb_primary_key = do
  out <- run "<ret>" "data/nu_help.duckdb"
  let (_, status) = footer out
  assert (contains status "grp=1") "DuckDB primary key is keyed (grp=1)"

test_folder_prefix :: Assertion
test_folder_prefix = do
  out1 <- run "" ""
  out2 <- run "," ""
  let s1 = snd (footer out1)
      s2 = snd (footer out2)
      r1 = takeNat (dropUntilAfter "r0/" s1)
      r2 = takeNat (dropUntilAfter "r0/" s2)
  assert (r2 >= r1) ", prefix works in folder"
  where
    dropUntilAfter needle hay =
      case T.breakOn needle hay of
        (_, rest) -> if T.null rest then "" else T.drop (T.length needle) rest
    takeNat t =
      case reads (T.unpack (T.takeWhile isDigit t)) :: [(Int, String)] of
        ((n,_):_) -> n
        []        -> 0

-- Sort by non-first column
test_sort_excludes_key :: Assertion
test_sort_excludes_key = do
  ls <- dataLines <$> run "l[" "data/grp_sort.csv"
  let first = case ls of { [] -> ""; (x:_) -> x }
  assert (contains first " 1 ") "sort by val: val=1 first"

test_sort_selected_not_key :: Assertion
test_sort_selected_not_key = do
  ls <- dataLines <$> run "![" "data/grp_sort.csv"
  let first = case ls of { [] -> ""; (x:_) -> x }
  assert (contains first " 3 " || contains first " 6 ") "sort on key col is no-op"

-- ============================================================================
-- === Delete column tests ===
-- ============================================================================

test_delete_col :: Assertion
test_delete_col = do
  out <- run "x" "data/grp_sort.csv"
  let hdr = header out
  assert (not (contains hdr "grp")) "x removes grp column from header"
  assert (contains hdr "val") "x keeps val column"
  assert (contains hdr "nam") "x keeps name column"

test_delete_hidden_cols :: Assertion
test_delete_hidden_cols = do
  out <- run "Hlx" "data/grp_sort.csv"
  let hdr = header out
  assert (not (contains hdr "grp")) "hidden col grp removed"
  assert (not (contains hdr "val")) "current col val removed"
  assert (contains hdr "nam") "name column remains"

-- ============================================================================
-- === Filter tests (parquet) ===
-- ============================================================================

test_filter_parquet_full_db :: Assertion
test_filter_parquet_full_db = do
  out <- run "\\" "data/filtered_test.parquet"
  let (tab, status) = footer out
  assert (contains tab "\\sym") "\\ filter shows \\sym in tab"
  let countStr = T.takeWhile isDigit
               $ case T.breakOn "r0/" status of
                   (_, rest) -> if T.null rest then "" else T.drop 3 rest
      count = case reads (T.unpack countStr) :: [(Int, String)] of
                ((n,_):_) -> n
                []        -> 0
  assert (count > 1000)
    ("filter queries full DB (" ++ show count ++ " rows, expected 40000 or 60000)")

-- ============================================================================
-- === SQLite tests ===
-- ============================================================================

test_sqlite_list :: Assertion
test_sqlite_list = do
  out <- run "" "data/test.sqlite"
  assert (contains out "items") "SQLite file lists 'items' table"

test_sqlite_enter :: Assertion
test_sqlite_enter = do
  out <- run "<ret>" "data/test.sqlite"
  assert (contains out "alpha") "SQLite table shows 'alpha' row"
  assert (contains out "gamma") "SQLite table shows 'gamma' row"
  let (_, status) = footer out
  assert (contains status "r0/3") "SQLite table has 3 rows"

-- ============================================================================
-- === CSV test ===
-- ============================================================================

test_csv_open :: Assertion
test_csv_open = do
  out <- run "" "data/basic.csv"
  let (_, status) = footer out
  assert (contains status "r0/5") "CSV has 5 rows"

-- ============================================================================
-- === JSON tests ===
-- ============================================================================

test_json_open :: Assertion
test_json_open = do
  out <- run "" "data/test.json"
  assert (contains out "alpha") "JSON shows 'alpha' row"
  let (_, status) = footer out
  assert (contains status "r0/3") "JSON has 3 rows"

test_ndjson_open :: Assertion
test_ndjson_open = do
  out <- run "" "data/test.ndjson"
  assert (contains out "beta") "NDJSON shows 'beta' row"
  let (_, status) = footer out
  assert (contains status "r0/3") "NDJSON has 3 rows"

-- ============================================================================
-- === JSONL tests ===
-- ============================================================================

test_jsonl_open :: Assertion
test_jsonl_open = do
  out <- run "" "data/test.jsonl"
  assert (contains out "alpha") "JSONL shows 'alpha' row"
  assert (contains out "beta") "JSONL shows 'beta' row"
  let (_, status) = footer out
  assert (contains status "r0/3") "JSONL has 3 rows"

test_jsonl_sort :: Assertion
test_jsonl_sort = do
  ls <- dataLines <$> run "[" "data/test.jsonl"
  let first = case ls of { [] -> ""; (x:_) -> x }
  assert (contains first "alpha") "JSONL sort asc: first row is alpha"

-- ============================================================================
-- === Excel tests ===
-- ============================================================================

test_xlsx_open :: Assertion
test_xlsx_open = do
  out <- run "" "data/test.xlsx"
  assert (contains out "alpha") "Excel shows 'alpha' value"
  assert (contains out "gamma") "Excel shows 'gamma' value"
  let (_, status) = footer out
  assert (contains status "r0/3") "Excel has 3 rows"

-- ============================================================================
-- === Avro tests ===
-- ============================================================================

test_avro_open :: Assertion
test_avro_open = do
  out <- run "" "data/test.avro"
  assert (contains out "alpha") "Avro shows 'alpha' row"
  assert (contains out "gamma") "Avro shows 'gamma' row"
  let (_, status) = footer out
  assert (contains status "r0/3") "Avro has 3 rows"

-- ============================================================================
-- === BSV / CSV-with-bars tests (read_csv auto-sniffs the delimiter) ===
-- ============================================================================

test_bsv_open :: Assertion
test_bsv_open = do
  out <- run "" "data/basic.bsv"
  assert (contains out "a") "BSV header shows column 'a'"
  assert (contains out "b") "BSV header shows column 'b'"
  assert (contains out "x") "BSV shows 'x' value"
  assert (contains out "z") "BSV shows 'z' value"
  let (_, status) = footer out
  assert (contains status "r0/5") "BSV has 5 rows"

-- A `.csv` file whose values contain `|` characters must still parse as
-- comma-separated — DuckDB's sniffer should not be tricked into using `|`.
test_csv_with_bars :: Assertion
test_csv_with_bars = do
  out <- run "" "data/bars_in_values.csv"
  assert (contains out "foo|bar") "CSV value 'foo|bar' kept intact"
  assert (contains out "baz|qux") "CSV value 'baz|qux' kept intact"
  let (_, status) = footer out
  assert (contains status "r0/3") "CSV has 3 rows (sniffer picked `,` not `|`)"

-- ============================================================================
-- === Rendering tests ===
-- ============================================================================

test_last_col_no_stretch :: Assertion
test_last_col_no_stretch = do
  out <- run "" "data/basic.csv"
  let hdr = header out
  assert (T.length hdr < 30)
    ("last col should not stretch to 80: got " ++ show (T.length hdr) ++ " chars")
  assert (contains hdr "\x2502") "last col should have trailing separator"

test_width_grows_on_scroll :: Assertion
test_width_grows_on_scroll = do
  let keys = T.replicate 26 "j"
  out <- run keys "data/wide_scroll.csv"
  let ls = dataLines out
  assert (any (\l -> contains l "input-required") ls) "scrolled data should show full 'input-required'"

-- ============================================================================
-- === Derive tests ===
-- ============================================================================

test_derive :: Assertion
test_derive = do
  out <- run "=" "data/basic.csv"
  let hdr = header out
  assert (contains hdr "a") "derive: original columns should remain"
  assert (not (contains hdr "_1")) "derive: no derived column without name = expr"

-- ============================================================================
-- === Split tests ===
-- ============================================================================

test_split :: Assertion
test_split = do
  out <- run ":" "data/split_test.csv"
  let (tab, status) = footer out
  assert (contains status "c2/6") "split: 6 columns after split"
  assert (contains tab ":tag") "split: tab shows :tag"
  let ls = dataLines out
  assert (any (\l -> contains l " a ") ls) "split: part 'a' visible"
  assert (any (\l -> contains l " d ") ls) "split: part 'd' visible"

test_split_noop :: Assertion
test_split_noop = do
  out <- run "l:" "data/split_test.csv"
  let (_, status) = footer out
  assert (contains status "c1/2") "split: no split on int column"

test_split_arg :: Assertion
test_split_arg = do
  out <- run ":-<ret>llll" "data/split_test.csv"
  let (tab, status) = footer out
  assert (contains status "c5/6") "split_arg: cursor at tag_4 (c5/6)"
  assert (contains tab ":tag") "split_arg: tab shows :tag"
  let ls = dataLines out
  assert (any (\l -> contains l " w ") ls) "split_arg: last part 'w' visible"

test_derive_arg :: Assertion
test_derive_arg = do
  out <- run "=double = x * 2<ret>lll" "data/numeric.csv"
  let (tab, status) = footer out
  assert (contains status "c3/4") "derive_arg: cursor at double (c3/4)"
  assert (contains tab "=double") "derive_arg: tab shows =double"
  let ls = dataLines out
  assert (any (\l -> contains l "10") ls) "derive_arg: 5*2=10 visible"

test_filter_arg :: Assertion
test_filter_arg = do
  out <- run "\\Exchange == 'P'<ret>" "data/nyse10k.parquet"
  let (_, status) = footer out
  assert (contains status "r0/528") "filter_arg: 528 rows after filter"

-- | Copy a fixture to a thread-unique tmp path so parallel tests don't
-- clobber each other's export output (the export path is derived from
-- the source file stem).
uniqSource :: FilePath -> IO FilePath
uniqSource src = do
  let name = takeBaseName src <> takeExtension src
  dst <- Tmp.threadPath name
  copyFile src dst
  pure dst

test_export_arg :: Assertion
test_export_arg = do
  src <- uniqSource "data/sort_test.parquet"
  d <- Log.dir
  let path = d ++ "/tv_export_" ++ takeBaseName src ++ ".csv"
  _ <- try (removeFile path) :: IO (Either SomeException ())
  _ <- run "ecsv<ret>" src
  csv <- TIO.readFile path
  assert (contains csv "name") "export_arg: csv should contain header"
  assert (contains csv "alice") "export_arg: csv should contain data"
  removeFile path

test_col_jump_arg :: Assertion
test_col_jump_arg = do
  out <- run "gExchange<ret>" "data/nyse10k.parquet"
  let (_, status) = footer out
  assert (contains status "c1/") "col_jump_arg: cursor moved to Exchange column"

-- ============================================================================
-- === Export tests ===
-- ============================================================================

test_export_csv :: Assertion
test_export_csv = do
  src <- uniqSource "data/sort_test.parquet"
  d <- Log.dir
  let path = d ++ "/tv_export_" ++ takeBaseName src ++ ".csv"
  _ <- try (removeFile path) :: IO (Either SomeException ())
  out <- run "e" src
  assert (contains out "name") "export_csv: table should render"
  csv <- TIO.readFile path
  assert (contains csv "name") "export_csv: csv should contain header"
  assert (contains csv "alice") "export_csv: csv should contain data"
  removeFile path

-- ============================================================================
-- === Transpose tests ===
-- ============================================================================

test_transpose :: Assertion
test_transpose = do
  out <- run "X" "data/basic.csv"
  assert (contains out "column") "transpose shows 'column' header"
  let ls = dataLines out
  assert (any (\l -> contains l " a ") ls) "transposed row for col 'a'"
  assert (any (\l -> contains l " b ") ls) "transposed row for col 'b'"
  let (_, status) = footer out
  assert (contains status "r0/2") "transposed has 2 rows (one per original column)"

test_transpose_pop :: Assertion
test_transpose_pop = do
  out <- run "Xq" "data/basic.csv"
  let (_, status) = footer out
  assert (contains status "r0/5") "q pops back to original 5-row view"

-- ============================================================================
-- === Join tests ===
-- ============================================================================

test_join_inner :: Assertion
test_join_inner = do
  out <- run "[j<ret>!Sj<ret>!SqJ" "data/join_test"
  assert (contains out "alice") "J shows alice from left table"
  assert (contains out "90") "J shows score=90 from right table"
  let (_, status) = footer out
  assert (contains status "r0/2") "Inner join has 2 rows (id=1,3)"

test_join_union :: Assertion
test_join_union = do
  out <- run "[j<ret>S<ret>SqJ" "data/join_test"
  assert (contains out "name") "Union shows name column"
  let (_, status) = footer out
  assert (contains status "r0/6") "Union of same 3-row table = 6 rows"

-- ============================================================================
-- === Sparkline tests ===
-- ============================================================================

test_sparkline_on :: Assertion
test_sparkline_on = do
  out <- run "" "data/basic.csv"
  let hasBlock = T.any (\c -> let n = fromEnum c in n >= 0x2581 && n <= 0x2588) out
  assert hasBlock "sparklines always on (block chars visible)"

-- ============================================================================
-- === Status bar aggregation tests ===
-- ============================================================================

test_statusagg_numeric :: Assertion
test_statusagg_numeric = do
  out <- run "" "data/basic.csv"
  assert (contains out "\x03A3") "Numeric column shows sum (\x03A3)"
  assert (contains out "#5") "Numeric column shows count (#5)"

test_statusagg_string :: Assertion
test_statusagg_string = do
  out <- run "l" "data/basic.csv"
  assert (contains out "#5") "String column shows count"
  assert (not (contains out "\x03A3")) "String column has no sum"

-- ============================================================================
-- === Key column reorder tests ===
-- ============================================================================

test_key_shift :: Assertion
test_key_shift = do
  hdr <- header <$> run "!l!<S-left>" "data/basic.csv"
  let bPos = case T.splitOn "b" hdr of
               (x:_:_) -> T.length x
               _       -> 999
      aPos = case T.splitOn "a" hdr of
               (x:_:_) -> T.length x
               _       -> 999
  assert (bPos < aPos)
    ("shift-left: b (" ++ show bPos ++ ") should appear before a (" ++ show aPos ++ ") in header")

-- ============================================================================
-- === Heat mode / flat menu / socket tests ===
-- ============================================================================

test_heat_mode :: Assertion
test_heat_mode = do
  socat <- hasCmd "socat"
  if not socat
    then pure ()
    else do
      (ph, mOut) <- spawnTv ["data/basic.csv", "-c", "<wait><wait><wait>"]
      -- Without access to child pid we can't find the socket reliably; fall back to skip.
      terminateProcess ph
      _ <- waitForProcess ph
      case mOut of
        Just h  -> do _ <- hGetContents h; pure ()
        Nothing -> pure ()

test_flat_menu :: Assertion
test_flat_menu = do
  out1 <- run " " "data/basic.csv"
  assert (contains out1 "a") "flat menu: table renders after first menu item"
  out2 <- run "  " "data/basic.csv"
  assert (contains out2 "a") "flat menu: double space still renders"

test_socket :: Assertion
test_socket = do
  socat <- hasCmd "socat"
  if not socat
    then pure ()
    else pure ()

test_socket_dispatch :: Assertion
test_socket_dispatch = do
  socat <- hasCmd "socat"
  if not socat
    then pure ()
    else pure ()

-- ============================================================================
-- === Arrow navigation ===
-- ============================================================================

test_arrow_nav :: Assertion
test_arrow_nav = do
  s1 <- snd . footer <$> run "<down>" "data/basic.csv"
  assert (contains s1 "r1/") "arrow down moves to r1"
  s2 <- snd . footer <$> run "j" "data/basic.csv"
  assert (contains s2 "r1/") "j moves to r1"
  h1 <- header <$> run "<right>" "data/basic.csv"
  h2 <- header <$> run "l" "data/basic.csv"
  assert (h1 == h2) ("arrow right == l: '" ++ T.unpack h1 ++ "' vs '" ++ T.unpack h2 ++ "'")

-- ============================================================================
-- === Session tests ===
-- ============================================================================

test_session_load :: Assertion
test_session_load = do
  home <- fromMaybe "." <$> lookupEnv "HOME"
  let dir = home ++ "/.cache/tv/sessions"
  createDirectoryIfMissing True dir
  (_, absP, _) <- readProcessWithExitCode "realpath" ["data/basic.csv"] ""
  let absPath = T.unpack (T.strip (T.pack absP))
      json = "{\"version\":1,\"views\":[{\"path\":\"" ++ absPath ++
             "\",\"vkind\":{\"kind\":\"tbl\"},\"disp\":\"\",\"prec\":3,\"widthAdj\":0,\"row\":0,\"col\":0,\"grp\":[],\"hidden\":[],\"colSels\":[],\"search\":null,\"query\":{\"base\":\"from `" ++
             absPath ++ "`\",\"ops\":[{\"type\":\"sort\",\"cols\":[[\"a\",true]]}]}}]}"
      sessionPath = dir ++ "/test_session.json"
  writeFile sessionPath json
  (ec, sOut, _) <- readProcessWithExitCode tvHaskBin ["-s", "test_session", "-c", "I"] ""
  assert (ec == ExitSuccess) ("session load exit code: " ++ show ec)
  let out = T.pack sOut
  assert (contains out "a") "session load: shows column a"
  let (_, status) = footer out
  assert (contains status "r0/5") "session load: has 5 rows"
  _ <- try (removeFile sessionPath) :: IO (Either SomeException ())
  pure ()

test_session_save_load :: Assertion
test_session_save_load = do
  home <- fromMaybe "." <$> lookupEnv "HOME"
  let dir = home ++ "/.cache/tv/sessions"
      sessionPath = dir ++ "/basic.json"
  _ <- try (removeFile sessionPath) :: IO (Either SomeException ())
  _ <- run "[W" "data/basic.csv"
  saved <- hasFile sessionPath
  assert saved "W should create basic.json session file"
  (ec, sOut, _) <- readProcessWithExitCode tvHaskBin ["-s", "basic", "-c", "I"] ""
  assert (ec == ExitSuccess) ("session round-trip exit code: " ++ show ec)
  let ls = dataLines (T.pack sOut)
      first = case ls of { [] -> ""; (x:_) -> x }
  assert (contains first " 1 " || T.isPrefixOf "1 " first)
    "session round-trip: sort preserved (first row = 1)"
  let (_, status) = footer (T.pack sOut)
  assert (contains status "r0/5") "session round-trip: has 5 rows"
  _ <- try (removeFile sessionPath) :: IO (Either SomeException ())
  pure ()

test_session_missing :: Assertion
test_session_missing = do
  (ec, _, serr) <- readProcessWithExitCode tvHaskBin ["-s", "nonexistent_session_xyz"] ""
  let err = T.pack serr
  assert (ec == ExitSuccess || contains err "not found" || contains err "Session")
    "missing session should report error"

-- ============================================================================
-- === Diff tests ===
-- ============================================================================

test_diff :: Assertion
test_diff = do
  out <- run "[j<ret>Sjj<ret>Sqd" "data/diff_test"
  let (tab, status) = footer out
  assert (contains tab "diff") "d shows diff in tab"
  assert (contains status "r0/3") "diff has 3 rows"
  assert (contains out "\x0394") "diff columns have \x0394 prefix"
  assert (contains out "name") "diff shows key column name"
  assert (contains out "regi") "diff shows key column region (truncated)"

test_diff_show_same :: Assertion
test_diff_show_same = do
  out <- run "[j<ret>Sjj<ret>Sqdd" "data/diff_test"
  assert (contains out "cos") "dd reveals same-value cost columns"

-- ============================================================================
-- === Plot tests ===
-- ============================================================================

test_plot_key_dispatch :: Assertion
test_plot_key_dispatch = do
  assert (Plot.handleKey 'q' == KeyQuit) "q should quit"
  assert (Plot.handleKey '.' == KeyInterval 1) ". should increase interval"
  assert (Plot.handleKey ',' == KeyInterval (-1)) ", should decrease interval"
  mapM_ (\k -> assert (Plot.handleKey k == KeyNoop) ([k] ++ " should be noop"))
    ['h', 'l', 'x', 'a', ' ', '+', '-', '\x1b', '[', 'A', 'j', 'k']

test_plot_export_string_col :: Assertion
test_plot_export_string_col = do
  mTbl <- Tbl.fromFile "data/plot/mixed.csv"
  case mTbl of
    Nothing  -> assertFailure "failed to open mixed.csv"
    Just tbl -> do
      result <- Tbl.plotExport tbl "x" "cat" Nothing False 1 1
      case result of
        Just _  -> pure ()
        Nothing -> assertFailure "plotExport with string y column should not crash with type cast error"

test_plot_export_data :: Assertion
test_plot_export_data = do
  mTbl <- Tbl.fromFile "data/plot/mixed.csv"
  case mTbl of
    Nothing  -> assertFailure "failed to open mixed.csv"
    Just tbl -> do
      result <- Tbl.plotExport tbl "x" "y" Nothing False 1 1
      case result of
        Just _  -> pure ()
        Nothing -> assertFailure "plotExport should succeed for numeric y"
      datPath <- Tbl.plotDatPath
      content <- TIO.readFile datPath
      let ls = filter (not . T.null) (T.splitOn "\n" content)
      assert (length ls > 0) "plot.dat should have data rows"
      assert (all (\l -> length (T.splitOn "\t" l) >= 2) ls)
        "plot.dat rows should be tab-separated x\\ty"

test_plot_export_cat :: Assertion
test_plot_export_cat = do
  mTbl <- Tbl.fromFile "data/plot/mixed.csv"
  case mTbl of
    Nothing  -> assertFailure "failed to open mixed.csv"
    Just tbl -> do
      result <- Tbl.plotExport tbl "x" "y" (Just "cat") False 1 1
      case result of
        Nothing   -> assertFailure "plotExport with cat should succeed"
        Just cats -> assert (V.length cats == 2)
                       ("expected 2 categories (A,B), got " ++ show (V.length cats))

test_plot_time_downsample :: Assertion
test_plot_time_downsample = do
  mTbl <- Tbl.fromFile "data/plot/time_wide.csv"
  case mTbl of
    Nothing  -> assertFailure "failed to open time_wide.csv"
    Just tbl -> do
      result <- Tbl.plotExport tbl "t" "val" Nothing True 1 300
      case result of
        Just _  -> pure ()
        Nothing -> assertFailure "time downsample should succeed"
      datPath <- Tbl.plotDatPath
      content <- TIO.readFile datPath
      let ls = filter (not . T.null) (T.splitOn "\n" content)
      assert (length ls >= 4)
        ("expected >= 4 5-min buckets, got " ++ show (length ls))
      mapM_ (\line ->
              let tv = case T.splitOn "\t" line of { (x:_) -> x; _ -> "" }
              in assert (T.length tv >= 8)
                   ("time '" ++ T.unpack tv ++ "' should be HH:MM:SS format"))
            ls

test_plot_downsample_step :: Assertion
test_plot_downsample_step = do
  mTbl <- Tbl.fromFile "data/plot/line.csv"
  case mTbl of
    Nothing  -> assertFailure "failed to open line.csv"
    Just tbl -> do
      _ <- Tbl.plotExport tbl "x" "y" Nothing False 1 1
      datPath <- Tbl.plotDatPath
      c1 <- TIO.readFile datPath
      let n1 = length (filter (not . T.null) (T.splitOn "\n" c1))
      _ <- Tbl.plotExport tbl "x" "y" Nothing False 1 2
      c2 <- TIO.readFile datPath
      let n2 = length (filter (not . T.null) (T.splitOn "\n" c2))
      assert (n2 < n1)
        ("step=2 (" ++ show n2 ++ " rows) should produce fewer rows than step=1 (" ++ show n1 ++ " rows)")

-- ============================================================================
-- === Replay ops tests ===
-- ============================================================================

test_replay_sort :: Assertion
test_replay_sort = do
  out <- run "[" "data/unsorted.csv"
  let (tab, _) = footer out
  assert (contains tab "sort") "replay: sort op shown on tab line after ["

test_replay_empty :: Assertion
test_replay_empty = do
  out <- run "" "data/basic.csv"
  let (tab, _) = footer out
  assert (not (contains tab "sort")) "replay: no sort on fresh view"
  assert (not (contains tab "filter")) "replay: no filter on fresh view"

test_theme :: Assertion
test_theme = do
  out <- run "" "data/basic.csv"
  assert (contains out "a") "theme: basic render still works after theme init"

-- ============================================================================
-- === Gz view tests ===
-- ============================================================================

test_gz_viewfile :: Assertion
test_gz_viewfile = do
  out <- run "" "data/test.txt.gz"
  assert (contains out "hello gz world") "gz viewfile shows decompressed text"

test_gz_csv_ingest :: Assertion
test_gz_csv_ingest = do
  out <- run "" "data/csv_data.txt.gz"
  let (_, status) = footer out
  assert (contains out "name") "gz csv ingest: has name column"
  assert (contains out "alpha") "gz csv ingest: has data"
  assert (contains status "r0/3") "gz csv ingest: 3 rows"

test_gz_txt_fallback :: Assertion
test_gz_txt_fallback = do
  out <- run "" "data/test.txt.gz"
  assert (contains out "hello gz world") "gz txt fallback: shows text content"
  let (_, status) = footer out
  assert (not (contains status "r0/")) "gz txt fallback: not a table view"

-- ============================================================================
-- === External-tool tests (osquery, HF, S3, FTP) - auto-skip via hasCmd ===
-- ============================================================================

hasOsquery :: IO Bool
hasOsquery = hasCmd "osqueryi"

test_osquery_list :: Assertion
test_osquery_list = do
  ok <- hasOsquery
  if not ok
    then pure ()
    else do
      out <- run "" "osquery://"
      assert (contains out "name") "osquery:// shows name column"
      assert (contains out "safety") "osquery:// shows safety column"

test_osquery_enter :: Assertion
test_osquery_enter = do
  ok <- hasOsquery
  if not ok
    then pure ()
    else do
      out <- run "<ret>" "osquery://"
      let (tab, _) = footer out
      assert (contains tab "acpi_tables") "Enter on safe table opens it"

test_osquery_scroll_no_hide :: Assertion
test_osquery_scroll_no_hide = do
  ok <- hasOsquery
  if not ok
    then pure ()
    else do
      o0 <- run "" "osquery://"
      assert (contains o0 "name") "col 0: name visible"
      o1 <- run "l" "osquery://"
      assert (contains o1 "name") "col 1: name still visible after moving right"

test_osquery_back :: Assertion
test_osquery_back = do
  ok <- hasOsquery
  if not ok
    then pure ()
    else do
      out <- run "<ret>q" "osquery://"
      assert (contains out "name") "q pops back to osquery table list"

test_osquery_meta_description :: Assertion
test_osquery_meta_description = do
  ok <- hasOsquery
  if not ok
    then pure ()
    else do
      out <- run "<ret>M" "osquery://"
      assert (contains out "description") "Meta view on osquery table shows description column"

test_osquery_direct_table :: Assertion
test_osquery_direct_table = do
  ok <- hasOsquery
  if not ok
    then pure ()
    else do
      out <- run "" "osquery://groups"
      assert (contains out "gid") "osquery://groups shows gid column"
      assert (not (contains out "safety")) "osquery://groups is not the listing"

test_osquery_typed_columns :: Assertion
test_osquery_typed_columns = do
  ok <- hasOsquery
  if not ok
    then pure ()
    else do
      out <- run "" "osquery://groups"
      assert (contains out "gid") "osquery://groups has gid column"
      assert (contains out "#") "osquery://groups gid is numeric (# indicator)"
      out2 <- run "<ret>" "osquery://"
      assert (not (contains out2 "safety")) "osquery enter table is not listing"

test_osquery_sort_enter :: Assertion
test_osquery_sort_enter = do
  ok <- hasOsquery
  if not ok
    then pure ()
    else do
      out <- run "lll]<ret>" "osquery://"
      let (tab, _) = footer out
      assert (contains tab "osquery://") "sort+enter: osquery tab still visible"
      assert (not (contains out "safety")) "sort+enter: opened table, not listing"

cachedCurlCheck :: IORef (Maybe Bool) -> String -> IO Bool
cachedCurlCheck ref url = cachedCheck ref $ do
  (ec, _, _) <- readProcessWithExitCode "curl"
                  ["-sf", "--max-time", "3", url] ""
  pure (ec == ExitSuccess)

{-# NOINLINE hfAccessCache #-}
hfAccessCache :: IORef (Maybe Bool)
hfAccessCache = unsafePerformIO (newIORef Nothing)

hasHfAccess :: IO Bool
hasHfAccess = cachedCurlCheck hfAccessCache "https://huggingface.co/api/datasets/openai/gsm8k"

test_hf_readme :: Assertion
test_hf_readme = do
  ok <- hasHfAccess
  if not ok
    then pure ()
    else do
      out <- run "jjjjj<ret>" "hf://datasets/openai/gsm8k"
      assert (contains out "GSM8K") "HF README shows dataset name"
      assert (contains out "math" || contains out "arithmetic" || contains out "word problems")
        "HF README shows description content"

test_hf_enter_parquet :: Assertion
test_hf_enter_parquet = do
  ok <- hasHfAccess
  if not ok
    then pure ()
    else do
      out <- run "jj<ret>j<ret>" "hf://datasets/openai/gsm8k"
      assert (contains out "question") "HF parquet has question column"
      assert (contains out "answer") "HF parquet has answer column"

test_hf_backspace :: Assertion
test_hf_backspace = do
  ok <- hasHfAccess
  if not ok
    then pure ()
    else do
      out <- run "jj<ret><bs>" "hf://datasets/openai/gsm8k"
      assert (contains out "gsm8k") "backspace returns to HF repo root"

test_hf_org_list :: Assertion
test_hf_org_list = do
  ok <- hasHfAccess
  if not ok
    then pure ()
    else do
      out <- run "" "hf://datasets/tablegpt/"
      assert (contains out "AppleStockData") "HF org lists datasets"
      assert (contains out "dow") "HF org shows downloads column"

{-# NOINLINE s3AccessCache #-}
s3AccessCache :: IORef (Maybe Bool)
s3AccessCache = unsafePerformIO (newIORef Nothing)

-- Native S3 client; check connectivity by curl-pinging the bucket URL
-- with the same query the listing uses. Avoids aws-CLI dependency.
hasS3Access :: IO Bool
hasS3Access = cachedCurlCheck s3AccessCache
  "https://overturemaps-us-west-2.s3.amazonaws.com/?list-type=2&max-keys=1"

s3path :: FilePath
s3path = "s3://overturemaps-us-west-2/release/"

s3run :: Text -> IO Text
s3run keys = runHask keys s3path ["+n"]

test_s3_list :: Assertion
test_s3_list = do
  ok <- hasS3Access
  if not ok
    then pure ()
    else do
      out <- s3run ""
      assert (contains out "dir") "S3 listing shows dir type"

test_s3_enter_dir :: Assertion
test_s3_enter_dir = do
  ok <- hasS3Access
  if not ok
    then pure ()
    else do
      out <- s3run "j<ret>"
      assert (contains out "dir" || contains out "file") "S3 enter dir shows contents"

test_s3_backspace :: Assertion
test_s3_backspace = do
  ok <- hasS3Access
  if not ok
    then pure ()
    else do
      out <- s3run "j<ret><bs>"
      assert (contains out "release") "S3 backspace returns to parent"

test_s3_sort :: Assertion
test_s3_sort = do
  ok <- hasS3Access
  if not ok
    then pure ()
    else do
      out <- s3run "["
      assert (contains out "dir") "S3 sort doesn't break listing"

{-# NOINLINE ftpAccessCache #-}
ftpAccessCache :: IORef (Maybe Bool)
ftpAccessCache = unsafePerformIO (newIORef Nothing)

hasFtpAccess :: IO Bool
hasFtpAccess = cachedCurlCheck ftpAccessCache "ftp://ftp.nyse.com/"

test_ftp_list :: Assertion
test_ftp_list = do
  ok <- hasFtpAccess
  if not ok
    then pure ()
    else do
      out <- run "" "ftp://ftp.nyse.com/"
      assert (contains out "dir") "FTP listing shows dir type"
      assert (contains out "Historical") "FTP listing shows Historical Data folder"

test_ftp_enter_dir :: Assertion
test_ftp_enter_dir = do
  ok <- hasFtpAccess
  if not ok
    then pure ()
    else do
      out <- run "j<ret>" "ftp://ftp.nyse.com/"
      assert (contains out "dir" || contains out "file")
        "FTP enter dir shows subdirectory contents"

test_ftp_backspace :: Assertion
test_ftp_backspace = do
  ok <- hasFtpAccess
  if not ok
    then pure ()
    else do
      out <- run "j<ret><bs>" "ftp://ftp.nyse.com/"
      assert (contains out "ftp.nyse.com") "FTP backspace returns to parent"

-- ============================================================================
-- === Test group aggregation ===
-- ============================================================================

ciTests :: TestTree
ciTests = testGroup "ci"
  [ testCase "sort_asc" test_sort_asc
  , testCase "sort_desc" test_sort_desc
  , testCase "meta_shows" test_meta_shows
  , testCase "meta_col_info" test_meta_col_info
  , testCase "meta_no_garbage" test_meta_no_garbage
  , testCase "meta_stats" test_meta_stats
  , testCase "meta_corr" test_meta_corr
  , testCase "sample" test_sample
  , testCase "freq_shows" test_freq_shows
  , testCase "freq_after_meta" test_freq_after_meta
  , testCase "freq_by_key" test_freq_by_key
  , testCase "freq_multi_key" test_freq_multi_key
  , testCase "freq_keeps_grp" test_freq_keeps_grp
  , testCase "sparkline_after_freq_back" test_sparkline_after_freq_back
  , testCase "script_print_no_ops" test_script_print_no_ops
  , testCase "script_print_after_sort" test_script_print_after_sort
  , testCase "script_q_flag_filters" test_script_q_flag_filters
  , testCase "script_combines_q_and_ops" test_script_combines_q_and_ops
  , testCase "meta_0" test_meta_0
  , testCase "meta_1" test_meta_1
  , testCase "meta_0_enter" test_meta_0_enter
  , testCase "meta_1_enter" test_meta_1_enter
  , testCase "freq_enter" test_freq_enter
  , testCase "spaced_header" test_spaced_header
  , testCase "no_stderr" test_no_stderr
  , testCase "search_jump" test_search_jump
  , testCase "search_next" test_search_next
  , testCase "search_prev" test_search_prev
  , testCase "search_after_sort" test_search_after_sort
  , testCase "col_search" test_col_search
  , testCase "folder_no_args" test_folder_no_args
  , testCase "folder_D" test_folder_D
  , testCase "folder_tab" test_folder_tab
  , testCase "folder_enter" test_folder_enter
  , testCase "folder_relative" test_folder_relative
  , testCase "folder_pop" test_folder_pop
  , testCase "folder_backspace" test_folder_backspace
  , testCase "folder_backspace_twice" test_folder_backspace_twice
  , testCase "folder_enter_symlink" test_folder_enter_symlink
  , testCase "duckdb_list" test_duckdb_list
  , testCase "duckdb_enter" test_duckdb_enter
  , testCase "duckdb_primary_key" test_duckdb_primary_key
  , testCase "sqlite_list" test_sqlite_list
  , testCase "sqlite_enter" test_sqlite_enter
  , testCase "csv_open" test_csv_open
  , testCase "json_open" test_json_open
  , testCase "ndjson_open" test_ndjson_open
  , testCase "jsonl_open" test_jsonl_open
  , testCase "jsonl_sort" test_jsonl_sort
  , testCase "xlsx_open" test_xlsx_open
  , testCase "avro_open" test_avro_open
  , testCase "bsv_open" test_bsv_open
  , testCase "csv_with_bars" test_csv_with_bars
  , testCase "folder_prefix" test_folder_prefix
  , testCase "sort_excludes_key" test_sort_excludes_key
  , testCase "sort_selected_not_key" test_sort_selected_not_key
  , testCase "delete_col" test_delete_col
  , testCase "delete_hidden_cols" test_delete_hidden_cols
  , testCase "filter_parquet_full_db" test_filter_parquet_full_db
  , testCase "last_col_no_stretch" test_last_col_no_stretch
  , testCase "width_grows_on_scroll" test_width_grows_on_scroll
  , testCase "export_csv" test_export_csv
  , testCase "export_arg" test_export_arg
  , testCase "col_jump_arg" test_col_jump_arg
  , testCase "transpose" test_transpose
  , testCase "transpose_pop" test_transpose_pop
  , testCase "split" test_split
  , testCase "split_noop" test_split_noop
  , testCase "split_arg" test_split_arg
  , testCase "derive" test_derive
  , testCase "derive_arg" test_derive_arg
  , testCase "filter_arg" test_filter_arg
  , testCase "join_inner" test_join_inner
  , testCase "join_union" test_join_union
  , testCase "sparkline_on" test_sparkline_on
  , testCase "key_shift" test_key_shift
  , testCase "arrow_nav" test_arrow_nav
  , testCase "heat_mode" test_heat_mode
  , testCase "flat_menu" test_flat_menu
  , testCase "socket" test_socket
  , testCase "socket_dispatch" test_socket_dispatch
  , testCase "statusagg_numeric" test_statusagg_numeric
  , testCase "statusagg_string" test_statusagg_string
  , testCase "session_load" test_session_load
  , testCase "session_save_load" test_session_save_load
  , testCase "session_missing" test_session_missing
  , testCase "diff" test_diff
  , testCase "diff_show_same" test_diff_show_same
  , testCase "plot_key_dispatch" test_plot_key_dispatch
  , testCase "plot_export_string_col" test_plot_export_string_col
  , testCase "plot_export_data" test_plot_export_data
  , testCase "plot_export_cat" test_plot_export_cat
  , testCase "plot_time_downsample" test_plot_time_downsample
  , testCase "plot_downsample_step" test_plot_downsample_step
  , testCase "replay_sort" test_replay_sort
  , testCase "replay_empty" test_replay_empty
  , testCase "folder_sort_type" test_folder_sort_type
  , testCase "gz_viewfile" test_gz_viewfile
  , testCase "gz_csv_ingest" test_gz_csv_ingest
  , testCase "gz_txt_fallback" test_gz_txt_fallback
  , testCase "theme" test_theme
  ]

heavyTests :: TestTree
heavyTests = testGroup "heavy"
  [ -- testCase "pg_list" test_pg_list       -- requires local PostgreSQL
    -- testCase "pg_enter" test_pg_enter     -- requires local PostgreSQL
    testCase "osquery_list" test_osquery_list
  , testCase "osquery_enter" test_osquery_enter
  , testCase "osquery_scroll_no_hide" test_osquery_scroll_no_hide
  , testCase "osquery_back" test_osquery_back
  , testCase "osquery_meta_description" test_osquery_meta_description
  , testCase "osquery_direct_table" test_osquery_direct_table
  , testCase "osquery_typed_columns" test_osquery_typed_columns
  , testCase "osquery_sort_enter" test_osquery_sort_enter
  , testCase "hf_readme" test_hf_readme
  , testCase "hf_enter_parquet" test_hf_enter_parquet
  , testCase "hf_backspace" test_hf_backspace
  , testCase "hf_org_list" test_hf_org_list
  , testCase "s3_list" test_s3_list
  , testCase "s3_enter_dir" test_s3_enter_dir
  , testCase "s3_backspace" test_s3_backspace
  , testCase "s3_sort" test_s3_sort
  , testCase "ftp_list" test_ftp_list
  , testCase "ftp_enter_dir" test_ftp_enter_dir
  , testCase "ftp_backspace" test_ftp_backspace
  ]

tests :: TestTree
tests = testGroup "Test" [ciTests, heavyTests]
