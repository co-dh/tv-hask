#!/usr/bin/env python3
"""Content diff between two asciinema .cast files.
Strips timestamps, concatenates all "o" frame bodies, reports first divergence.
"""
import json, sys

def load(path):
    with open(path) as f:
        lines = f.readlines()
    return lines[0], "".join(json.loads(l)[2] for l in lines[1:]), len(lines) - 1

hA, a, nA = load(sys.argv[1])
hB, b, nB = load(sys.argv[2])
print(f"haskell: {len(a)} bytes, {nA} frames")
print(f"lean:    {len(b)} bytes, {nB} frames")
print(f"header H: {hA.strip()}")
print(f"header L: {hB.strip()}")
print()
if a == b:
    print("CONTENT EQUAL")
    sys.exit(0)
ncommon = min(len(a), len(b))
first = None
for i in range(ncommon):
    if a[i] != b[i]:
        first = i
        break
if first is None:
    print(f"prefix equal, lengths differ (H={len(a)}, L={len(b)})")
    tail = (a if len(a) > len(b) else b)[ncommon:ncommon+200]
    print(f"tail of longer: {tail!r}")
    sys.exit(1)
lo = max(0, first - 80)
hi = min(len(a), first + 80)
print(f"first divergence at byte {first}")
print(f"  H: {a[lo:hi]!r}")
hi2 = min(len(b), first + 80)
print(f"  L: {b[lo:hi2]!r}")
sys.exit(1)
