#!/usr/bin/env python3
import os, sys
os.chdir("/home/dh/repo/tv-hask")

with open("doc/gen_demo.py") as f:
    src = f.read()

# Log ALL three interesting points: emit, drain (entry+exit), step loop body
patches = [
    (
        '        def emit(text):\n'
        '            t = time.monotonic() - t0\n'
        '            cast_f.write(json.dumps([round(t, 3), "o", text]) + "\\n")',
        '        def emit(text):\n'
        '            t = time.monotonic() - t0\n'
        '            open("/tmp/emit.log","a").write("EMIT t=%.2f len=%d\\n" % (t, len(text)))\n'
        '            cast_f.write(json.dumps([round(t, 3), "o", text]) + "\\n")\n'
        '            cast_f.flush()'
    ),
    (
        '            if buf:\n'
        '                t = time.monotonic() - t0\n'
        '                text = buf.decode("utf-8", errors="replace")',
        '            open("/tmp/emit.log","a").write("DRAIN_EXIT buf=%d child_dead=%s\\n" % (len(buf), child_dead))\n'
        '            if buf:\n'
        '                t = time.monotonic() - t0\n'
        '                text = buf.decode("utf-8", errors="replace")'
    ),
    (
        '        try:\n'
        '            for desc, keys_shown, keys, pause in steps:',
        '        try:\n'
        '            open("/tmp/emit.log","a").write("STEPLOOP_ENTER child_dead=%s\\n" % child_dead)\n'
        '            for desc, keys_shown, keys, pause in steps:\n'
        '                open("/tmp/emit.log","a").write("STEP_ITER child_dead=%s\\n" % child_dead)'
    ),
    (
        '            for desc, keys_shown, keys, pause in steps:\n'
        '                open("/tmp/emit.log","a").write("STEP_ITER child_dead=%s\\n" % child_dead)\n'
        '                if child_dead:',
        '            for desc, keys_shown, keys, pause in steps:\n'
        '                open("/tmp/emit.log","a").write("STEP_ITER child_dead=%s\\n" % child_dead)\n'
        '                if child_dead:\n'
        '                    open("/tmp/emit.log","a").write("STEP_BREAK\\n")'
    ),
    (
        '                drain(0.5)\n'
        '                title, nlines = title_escape(desc, keys_shown)\n'
        '                if title:',
        '                drain(0.5)\n'
        '                open("/tmp/emit.log","a").write("AFTER_DRAIN05 child_dead=%s\\n" % child_dead)\n'
        '                title, nlines = title_escape(desc, keys_shown)\n'
        '                open("/tmp/emit.log","a").write("TITLE len=%d\\n" % len(title))\n'
        '                if title:'
    ),
    (
        '        except OSError:\n'
        '            pass',
        '        except OSError as _e:\n'
        '            open("/tmp/emit.log","a").write("OSERROR %s\\n" % _e)\n'
        '            pass\n'
        '        except Exception as _e:\n'
        '            open("/tmp/emit.log","a").write("EXC %s %s\\n" % (type(_e).__name__, _e))'
    ),
]

src2 = src
for old, new in patches:
    if old not in src2:
        print(f"PATCH MISSING: {old[:80]}", file=sys.stderr)
        sys.exit(1)
    src2 = src2.replace(old, new)

with open("/tmp/emit.log", "w"): pass
sys.argv = ["patched", "sparkline"]
try:
    exec(compile(src2, "patched", "exec"), {"__name__": "__main__"})
except SystemExit:
    pass
