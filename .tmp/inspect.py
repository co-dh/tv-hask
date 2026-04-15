import json, sys
with open(sys.argv[1]) as f:
    lines = f.readlines()
print(f"Header: {lines[0].strip()}")
for i, l in enumerate(lines[1:]):
    t, kind, s = json.loads(l)
    print(f"Frame {i}: t={t}s, {len(s)} bytes")
    print(f"  first: {s[:100]!r}")
    print(f"  last : {s[-100:]!r}")
