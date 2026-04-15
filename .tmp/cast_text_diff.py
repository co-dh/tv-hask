#!/usr/bin/env python3
"""Compare two .cast files by their on-screen text content only.
Replays all frames through a minimal VT100 grid, strips SGR/cursor escapes,
and prints the resulting 80x24 screen for each. Then diffs the screens.
"""
import json, re, sys

def replay(path):
    W, H = 80, 24
    grid = [[" "] * W for _ in range(H)]
    cx = cy = 0
    with open(path) as f:
        lines = f.readlines()
    text = "".join(json.loads(l)[2] for l in lines[1:])
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c == "\x1b":
            # escape sequence
            if i + 1 < n and text[i+1] == "[":
                # CSI
                m = re.match(r"\x1b\[([0-9;?]*)([A-Za-z])", text[i:])
                if m:
                    params, cmd = m.group(1), m.group(2)
                    if cmd == "H":
                        parts = params.split(";") if params else []
                        cy = (int(parts[0]) - 1) if len(parts) > 0 and parts[0] else 0
                        cx = (int(parts[1]) - 1) if len(parts) > 1 and parts[1] else 0
                    elif cmd == "J" and params == "2":
                        grid = [[" "] * W for _ in range(H)]
                    # ignore SGR, DSR, window ops, etc
                    i += len(m.group(0))
                    continue
                else:
                    i += 1
                    continue
            elif i + 1 < n and text[i+1] in "()":
                # charset select
                i += 3
                continue
            elif i + 1 < n and text[i+1] in "78=":
                i += 2
                continue
            elif i + 1 < n and text[i+1] == "]":
                # OSC — consume until BEL or ST
                j = text.find("\x07", i)
                if j == -1:
                    j = text.find("\x1b\\", i)
                if j == -1:
                    i += 2
                else:
                    i = j + 1
                continue
            else:
                i += 1
                continue
        elif c == "\r":
            cx = 0
        elif c == "\n":
            cy = min(cy + 1, H - 1)
        elif c == "\b":
            cx = max(cx - 1, 0)
        elif c == "\x07":
            pass
        else:
            if 0 <= cy < H and 0 <= cx < W:
                grid[cy][cx] = c
            cx += 1
            if cx >= W:
                cx = 0
                cy = min(cy + 1, H - 1)
        i += 1
    return ["".join(row).rstrip() for row in grid]

def show(label, rows):
    print(f"=== {label} ===")
    for i, r in enumerate(rows):
        print(f"{i:2d}|{r}")
    print()

gridA = replay(sys.argv[1])
gridB = replay(sys.argv[2])
show(sys.argv[1], gridA)
show(sys.argv[2], gridB)

same = True
for i, (a, b) in enumerate(zip(gridA, gridB)):
    if a != b:
        same = False
        print(f"ROW {i:2d} DIFF:")
        print(f"  H: {a!r}")
        print(f"  L: {b!r}")

if same:
    print("SCREENS IDENTICAL")
