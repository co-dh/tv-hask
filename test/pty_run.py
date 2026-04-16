#!/usr/bin/env python3
"""Run tv in a PTY, send scripted keystrokes, dump the final screen.

Usage:
  test/pty_run.py <file> <keys>
    keys: literal chars sent one-at-a-time via pty (\r for enter, \x7f for bs)

Returns the final rendered screen on stdout. Mirrors gen_demo.py's
recording engine but without title overlays / cast files / agg.
"""
import os, pty, select, signal, struct, sys, time, fcntl, termios

TV = os.environ.get(
    "TV",
    "/home/dh/repo/tv-hask/dist-newstyle/build/x86_64-linux/ghc-9.6.6/tv-hask-0.1.0.0/x/tv/build/tv/tv",
)
W, H = 120, 30


def run(file_arg, keys, hold=0.5):
    env = {**os.environ, "TERM": "xterm-256color", "TMPDIR": "/tmp"}
    env.pop("TMUX", None)
    args = [TV, file_arg]
    pid, fd = pty.fork()
    if pid == 0:
        os.execvpe(TV, args, env)

    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", H, W, 0, 0))
    os.kill(pid, signal.SIGWINCH)

    buf = bytearray()
    def drain(timeout=0.2):
        while True:
            r, _, _ = select.select([fd], [], [], timeout)
            if not r:
                return
            try:
                chunk = os.read(fd, 65536)
            except OSError:
                return
            if not chunk:
                return
            buf.extend(chunk)
            timeout = 0.02

    drain(2.0)
    time.sleep(0.5)
    drain(0.5)

    # Group bytes into logical keystrokes. An ESC followed by `[` or `O`
    # must reach the reader as an atomic chunk, otherwise the app's ESC
    # timeout expires between bytes and the sequence is misread as a lone
    # Esc key. Anything else is sent one byte at a time as before.
    i = 0
    n = len(keys)
    while i < n:
        if keys[i] == '\x1b' and i + 1 < n and keys[i + 1] in ('[', 'O'):
            j = i + 2
            # CSI parameter bytes (0x30..0x3f) then intermediate (0x20..0x2f)
            # then a final byte (0x40..0x7e). SS3 is the same letter set.
            while j < n and 0x30 <= ord(keys[j]) <= 0x3f:
                j += 1
            while j < n and 0x20 <= ord(keys[j]) <= 0x2f:
                j += 1
            if j < n:
                j += 1  # consume final byte
            chunk = keys[i:j]
            os.write(fd, chunk.encode())
            i = j
        else:
            os.write(fd, keys[i].encode())
            i += 1
        time.sleep(0.06)
        drain(0.1)

    drain(hold)

    try:
        os.kill(pid, signal.SIGTERM)
        os.waitpid(pid, 0)
    except (ProcessLookupError, ChildProcessError):
        pass
    os.close(fd)
    return bytes(buf)


def render(raw):
    """Apply ANSI cursor moves + erase-in-line to a fixed grid; return text."""
    grid = [[" "] * W for _ in range(H)]
    row, col = 0, 0
    i = 0
    n = len(raw)
    saved = (0, 0)
    while i < n:
        b = raw[i]
        if b == 0x1B and i + 1 < n:
            nxt = raw[i + 1]
            if nxt in (ord("("), ord(")"), ord("*"), ord("+")):
                i += 3; continue
            if nxt == ord("["):
                j = i + 2
                while j < n and not (0x40 <= raw[j] <= 0x7E):
                    j += 1
                if j >= n:
                    break
                params = raw[i + 2 : j].decode("latin1")
                cmd = chr(raw[j])
                nums = [int(x) if x else 0 for x in params.split(";")] if params and not params.startswith("?") else []
                if cmd == "H" or cmd == "f":
                    r = (nums[0] if len(nums) > 0 else 1) - 1
                    c = (nums[1] if len(nums) > 1 else 1) - 1
                    row, col = max(0, min(H - 1, r)), max(0, min(W - 1, c))
                elif cmd == "A":
                    row = max(0, row - (nums[0] or 1))
                elif cmd == "B":
                    row = min(H - 1, row + (nums[0] or 1))
                elif cmd == "C":
                    col = min(W - 1, col + (nums[0] or 1))
                elif cmd == "D":
                    col = max(0, col - (nums[0] or 1))
                elif cmd == "G":
                    col = max(0, min(W - 1, (nums[0] or 1) - 1))
                elif cmd == "J":
                    n_arg = nums[0] if nums else 0
                    if n_arg == 2:
                        grid = [[" "] * W for _ in range(H)]
                elif cmd == "K":
                    n_arg = nums[0] if nums else 0
                    if n_arg == 0:
                        for c in range(col, W): grid[row][c] = " "
                    elif n_arg == 1:
                        for c in range(0, col + 1): grid[row][c] = " "
                    else:
                        for c in range(W): grid[row][c] = " "
                elif cmd == "X":
                    nn = nums[0] or 1
                    for c in range(col, min(W, col + nn)): grid[row][c] = " "
                i = j + 1
                continue
            if nxt == ord("7"):
                saved = (row, col); i += 2; continue
            if nxt == ord("8"):
                row, col = saved; i += 2; continue
            if nxt == ord("]"):
                while i < n and raw[i] != 0x07:
                    i += 1
                i += 1
                continue
            i += 2
            continue
        if b == 0x0D:
            col = 0; i += 1; continue
        if b == 0x0A:
            row = min(H - 1, row + 1); i += 1; continue
        if b == 0x08:
            col = max(0, col - 1); i += 1; continue
        if b < 0x20:
            i += 1; continue
        try:
            decoded = bytes([b]).decode("utf-8")
        except UnicodeDecodeError:
            j = i + 1
            while j < n and (raw[j] & 0xC0) == 0x80:
                j += 1
            try:
                decoded = raw[i:j].decode("utf-8")
            except UnicodeDecodeError:
                decoded = "?"
            if 0 <= row < H and 0 <= col < W:
                grid[row][col] = decoded
            col = min(W - 1, col + 1)
            i = j
            continue
        if 0 <= row < H and 0 <= col < W:
            grid[row][col] = decoded
        col = min(W - 1, col + 1)
        i += 1
    return "\n".join("".join(r).rstrip() for r in grid)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__); sys.exit(1)
    file_arg, keys = sys.argv[1], sys.argv[2]
    # Only translate explicit escapes; leave other chars (including !) literal.
    keys = (keys.replace("\\r", "\r").replace("\\n", "\n")
                .replace("\\t", "\t").replace("\\b", "\x7f")
                .replace("\\e", "\x1b"))
    raw = run(file_arg, keys)
    print(render(raw))
