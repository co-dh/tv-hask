#!/bin/bash
# Run each Lean CI test individually against the Haskell tv binary.
# Invokes .lake/build/bin/test once per test — so one failure does not
# halt the rest of the run. Compare with `test --ci` which stops on first fail.
set -uo pipefail

TC=/home/dh/repo/Tc
TEST=$TC/.lake/build/bin/test
cd "$TC"

# Pull the ciTests name list by parsing Test.lean. We take the identifiers
# that appear as ("name", test_name) in any ciTests entry.
LEAN=$TC/test/Test.lean
NAMES=$(awk '
  /^def ciTests/ {on=1; next}
  on && /^]/ {exit}
  on && /^[[:space:]]*\(/ {
    while (match($0, /"[a-z_0-9]+"[[:space:]]*,[[:space:]]*test_/)) {
      s=substr($0, RSTART, RLENGTH); gsub(/^"/,"",s); gsub(/".*/,"",s);
      print s; $0=substr($0, RSTART+RLENGTH)
    }
  }' "$LEAN")

# Infra-dependent tests we know cannot pass locally.
# avro_open: duckdb v1.5.1 has no avro community extension published yet.
SKIP="avro_open"

PASS=0; FAIL=0; SKIPPED=0; FAILS=""
for name in $NAMES; do
  if [[ " $SKIP " == *" $name "* ]]; then
    SKIPPED=$((SKIPPED+1)); printf "  %-36s SKIP\n" "$name"; continue
  fi
  out=$("$TEST" "$name" 2>&1)
  if echo "$out" | grep -q "All tests passed"; then
    PASS=$((PASS+1)); printf "  %-36s OK\n" "$name"
  else
    FAIL=$((FAIL+1))
    msg=$(echo "$out" | grep -E "uncaught exception" | head -1 | sed 's/uncaught exception: //')
    FAILS+="  FAIL $name: $msg\n"
    printf "  %-36s FAIL: %s\n" "$name" "$msg"
  fi
done

echo ""
echo "=============================="
echo "Results: $PASS passed, $FAIL failed, $SKIPPED skipped"
if [ $FAIL -gt 0 ]; then echo ""; printf "$FAILS"; exit 1; fi
