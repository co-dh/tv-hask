import json, sys
with open(sys.argv[1]) as f:
    ls = f.readlines()
frames = [json.loads(l)[2] for l in ls[1:]]
for i, f in enumerate(frames):
    clrs = f.count("\x1b[49X")
    if clrs > 0 or "\x1b7" in f:
        print(f"frame {i}: clears={clrs}, len={len(f)}, head={f[:80]!r}")
