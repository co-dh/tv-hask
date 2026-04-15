import os, sys
os.chdir("/home/dh/repo/tv-hask")
with open("doc/gen_demo.py") as f:
    src = f.read()
OLD = (
    '        def emit(text):\n'
    '            t = time.monotonic() - t0\n'
    '            cast_f.write(json.dumps([round(t, 3), "o", text]) + "\\n")'
)
NEW = (
    '        def emit(text):\n'
    '            t = time.monotonic() - t0\n'
    '            sys.stderr.write("EMIT t=%.2f len=%d first40=%r\\n" % (t, len(text), text[:40]))\n'
    '            sys.stderr.flush()\n'
    '            cast_f.write(json.dumps([round(t, 3), "o", text]) + "\\n")\n'
    '            cast_f.flush()'
)
if OLD not in src:
    print("patch target not found", file=sys.stderr)
    sys.exit(1)
patched = src.replace(OLD, NEW)
sys.argv = ["gen_demo_patched", "sparkline"]
exec(compile(patched, "gen_demo_patched", "exec"), {"__name__": "__main__"})
