import json, sys
def fr(p):
    with open(p) as f:
        return [json.loads(l)[2] for l in f.readlines()[1:]]
h = fr(sys.argv[1]); l = fr(sys.argv[2])
print(f"H={len(h)} L={len(l)}")
a = "".join(h); b = "".join(l)
lim = min(len(a), len(b))
diff = None
for i in range(lim):
    if a[i] != b[i]:
        diff = i
        break
if diff is None:
    print(f"common prefix full, diff in tail only (lengths {len(a)} {len(b)})")
    sys.exit(0)
lo = max(0, diff - 60); hi = min(len(a), diff + 60)
print(f"diverge at byte {diff}")
print(f"  H: {a[lo:hi]!r}")
print(f"  L: {b[lo:min(len(b), diff+60)]!r}")

tot = 0
for i, f in enumerate(h):
    if tot + len(f) > diff - 100:
        print(f"H frame {i}: offset {tot}, len {len(f)}")
        print(f"  first: {f[:80]!r}")
        print(f"  last:  {f[-40:]!r}")
    if tot > diff + 200:
        break
    tot += len(f)
tot = 0
for i, f in enumerate(l):
    if tot + len(f) > diff - 100:
        print(f"L frame {i}: offset {tot}, len {len(f)}")
        print(f"  first: {f[:80]!r}")
        print(f"  last:  {f[-40:]!r}")
    if tot > diff + 200:
        break
    tot += len(f)
