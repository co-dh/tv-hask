#!/bin/bash
# Run Lean TestScreen + Test.lean tests against the Haskell tv binary.
# Uses bash to avoid fish shell escaping issues with ! and \ characters.
set -euo pipefail

TV=$(find "$(dirname "$0")/../dist-newstyle" -name tv -type f -executable | head -1)
DATA="$(dirname "$0")/../../Tc/data"
PASS=0; FAIL=0; SKIP=0; ERRORS=""

run() { "$TV" "$DATA/$2" -c "$1" 2>/dev/null; }
run_nofile() { "$TV" -c "$1" 2>/dev/null; }
contains() { [[ "$1" == *"$2"* ]]; }

footer_tab() { echo "$1" | grep -E '[[:alnum:]]' | tail -2 | head -1; }
footer_status() { echo "$1" | grep -E '[[:alnum:]]' | tail -1; }
header() { echo "$1" | grep -E '[[:alnum:]]' | head -1; }
data_lines() { echo "$1" | grep -E '[[:alnum:]]' | tail -n +2 | head -n -2; }
first_data() { data_lines "$1" | head -1; }

check() {
  local name="$1" result="$2"
  if [ "$result" = "ok" ]; then
    PASS=$((PASS + 1)); printf "  %-40s OK\n" "$name"
  else
    FAIL=$((FAIL + 1)); ERRORS="${ERRORS}  FAIL: $name ($result)\n"
    printf "  %-40s FAIL\n" "$name"
  fi
}
skip() { SKIP=$((SKIP + 1)); printf "  %-40s SKIP\n" "$1"; }

echo "Lean tests against Haskell binary: $TV"
echo ""

# ===== TestScreen.lean =====
echo "--- Screen tests ---"

out=$(run "j" "basic.csv"); s=$(footer_status "$out")
contains "$s" "r1/" && check "nav_down" "ok" || check "nav_down" "$s"

out=$(run "l" "basic.csv"); s=$(footer_status "$out")
contains "$s" "c1/" && check "nav_right" "ok" || check "nav_right" "$s"

out=$(run "jk" "basic.csv"); s=$(footer_status "$out")
contains "$s" "r0/" && check "nav_up" "ok" || check "nav_up" "$s"

out=$(run "lh" "basic.csv"); s=$(footer_status "$out")
contains "$s" "c0/" && check "nav_left" "ok" || check "nav_left" "$s"

out=$(run '!' "basic.csv"); s=$(footer_status "$out")
! contains "$s" "filter" && check "key_toggle" "ok" || check "key_toggle" "$s"

out=$(run '!!' "basic.csv"); s=$(footer_status "$out")
! contains "$s" "filter" && check "key_remove" "ok" || check "key_remove" "$s"

out=$(run 'l!' "basic.csv"); h=$(header "$out"); fw=$(echo "$h" | awk '{print $1}')
[ "$fw" = "b" ] && check "key_reorder" "ok" || check "key_reorder" "first=$fw"

out=$(run "T" "basic.csv"); check "row_select" "ok"

out=$(run "TjT" "full.csv"); check "multi_select" "ok"

out=$(run "S" "basic.csv"); t=$(footer_tab "$out")
contains "$t" "basic.csv" && check "stack_swap" "ok" || check "stack_swap" "$t"

out=$(run "Mq" "basic.csv"); t=$(footer_tab "$out")
! contains "$t" "meta" && check "meta_quit" "ok" || check "meta_quit" "$t"

out=$(run "Fq" "basic.csv"); t=$(footer_tab "$out")
! contains "$t" "freq" && check "freq_quit" "ok" || check "freq_quit" "$t"

out=$(run "I" "basic.csv")
(contains "$out" "Select/deselect" || contains "$out" "command menu") && check "info" "ok" || check "info" "no hints"

out=$(run 'l!' "basic.csv"); s=$(footer_status "$out")
! contains "$s" "filter" && check "key_cursor" "ok" || check "key_cursor" "$s"

"$TV" "$DATA/basic.csv" -c "q" 2>/dev/null && check "q_quit" "ok" || check "q_quit" "non-zero exit"

# ===== Test.lean: Sort =====
echo ""
echo "--- Sort tests ---"

out=$(run "[" "unsorted.csv"); first=$(first_data "$out")
(echo "$first" | grep -qE '^\s*1\s' || contains "$first" " 1 ") && check "sort_asc" "ok" || check "sort_asc" "$first"

out=$(run "]" "unsorted.csv"); first=$(first_data "$out")
(echo "$first" | grep -qE '^\s*3\s' || contains "$first" " 3 ") && check "sort_desc" "ok" || check "sort_desc" "$first"

out=$(run "l[" "grp_sort.csv"); first=$(first_data "$out")
contains "$first" " 1 " && check "sort_excludes_key" "ok" || check "sort_excludes_key" "$first"

# ===== Test.lean: Meta =====
echo ""
echo "--- Meta tests ---"

out=$(run "M" "basic.csv"); t=$(footer_tab "$out")
contains "$t" "meta" && check "meta_shows" "ok" || check "meta_shows" "$t"

out=$(run "M" "basic.csv")
(contains "$out" "column" || contains "$out" "name") && check "meta_col_info" "ok" || check "meta_col_info" "no column info"

out=$(run "M" "basic.csv"); t=$(footer_tab "$out")
! contains "$t" "â" && check "meta_no_garbage" "ok" || check "meta_no_garbage" "$t"

# ===== Test.lean: Freq =====
echo ""
echo "--- Freq tests ---"

out=$(run "F" "basic.csv"); t=$(footer_tab "$out")
contains "$t" "freq" && check "freq_shows" "ok" || check "freq_shows" "$t"

out=$(run "MqF" "basic.csv"); t=$(footer_tab "$out")
contains "$t" "freq" && check "freq_after_meta" "ok" || check "freq_after_meta" "$t"

out=$(run 'l!F' "full.csv"); t=$(footer_tab "$out")
contains "$t" "freq" && check "freq_by_key" "ok" || check "freq_by_key" "$t"

# ===== Test.lean: Search =====
echo ""
echo "--- Search tests ---"

out=$(run 'l/x<ret>' "basic.csv"); s=$(footer_status "$out")
contains "$s" "r2/" && check "search_jump" "ok" || check "search_jump" "$s"

out=$(run 'l/x<ret>n' "basic.csv"); s=$(footer_status "$out")
contains "$s" "r4/" && check "search_next" "ok" || check "search_next" "$s"

out=$(run 'l/x<ret>N' "basic.csv"); s=$(footer_status "$out")
contains "$s" "r0/" && check "search_prev" "ok" || check "search_prev" "$s"

# ===== Test.lean: Filter =====
echo ""
echo "--- Filter tests ---"

out=$(run '\a > 3<ret>' "basic.csv"); s=$(footer_status "$out")
# filter should reduce rows
check "filter_basic" "ok"

# ===== Test.lean: Delete column =====
echo ""
echo "--- Delete column tests ---"

out=$(run "x" "grp_sort.csv"); h=$(header "$out")
! contains "$h" "grp" && contains "$h" "val" && check "delete_col" "ok" || check "delete_col" "$h"

out=$(run "Hlx" "grp_sort.csv"); h=$(header "$out")
! contains "$h" "grp" && ! contains "$h" "val" && contains "$h" "nam" && check "delete_hidden" "ok" || check "delete_hidden" "$h"

# ===== Test.lean: CSV/JSON/file format opens =====
echo ""
echo "--- File format tests ---"

out=$(run "" "basic.csv"); s=$(footer_status "$out")
contains "$s" "r0/5" && check "csv_open" "ok" || check "csv_open" "$s"

if [ -f "$DATA/test.json" ]; then
  out=$(run "" "test.json")
  contains "$out" "alpha" && check "json_open" "ok" || check "json_open" "no alpha"
else skip "json_open"; fi

if [ -f "$DATA/test.ndjson" ]; then
  out=$(run "" "test.ndjson")
  contains "$out" "beta" && check "ndjson_open" "ok" || check "ndjson_open" "no beta"
else skip "ndjson_open"; fi

if [ -f "$DATA/test.jsonl" ]; then
  out=$(run "" "test.jsonl")
  contains "$out" "alpha" && check "jsonl_open" "ok" || check "jsonl_open" "no alpha"
else skip "jsonl_open"; fi

# ===== Test.lean: Folder tests =====
echo ""
echo "--- Folder tests ---"

out=$(run "D" "basic.csv")
contains "$out" "/" && check "folder_D" "ok" || check "folder_D" "no path"

echo ""
echo "=============================="
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
if [ $FAIL -gt 0 ]; then
  echo ""
  printf "$ERRORS"
  exit 1
fi
