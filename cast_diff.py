#!/usr/bin/env python3
"""Compare two .cast files frame by frame."""
import json, sys

def extract_frames(path):
    frames = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('{'):
                continue
            try:
                ts, typ, data = json.loads(line)
                if typ == 'o':
                    frames.append(data)
            except:
                pass
    return frames

hask = extract_frames(sys.argv[1])
lean = extract_frames(sys.argv[2])

print(f"Frames: hask={len(hask)} lean={len(lean)}")

diffs = 0
for i in range(min(len(hask), len(lean))):
    if hask[i] != lean[i]:
        diffs += 1
        if diffs <= 5:
            h, l = hask[i], lean[i]
            for j in range(min(len(h), len(l))):
                if h[j] != l[j]:
                    ctx = 30
                    print(f"Frame {i}, byte {j}:")
                    print(f"  hask: {repr(h[max(0,j-ctx):j+ctx])}")
                    print(f"  lean: {repr(l[max(0,j-ctx):j+ctx])}")
                    break
            else:
                print(f"Frame {i}: length diff hask={len(h)} lean={len(l)}")

if diffs == 0:
    print("ALL FRAMES IDENTICAL")
else:
    print(f"DIFFS: {diffs} frames differ out of {min(len(hask), len(lean))}")
