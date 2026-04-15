#!/usr/bin/env python3
# Subprocess the patched gen_demo so stderr routing works cleanly
import os, subprocess, sys
os.chdir("/home/dh/repo/tv-hask")

with open("doc/gen_demo.py") as f:
    src = f.read()

OLD_EMIT = (
    '        def emit(text):\n'
    '            t = time.monotonic() - t0\n'
    '            cast_f.write(json.dumps([round(t, 3), "o", text]) + "\\n")'
)
NEW_EMIT = (
    '        def emit(text):\n'
    '            t = time.monotonic() - t0\n'
    '            open("/tmp/emit.log","a").write("EMIT t=%.2f len=%d first=%r\\n" % (t, len(text), text[:60]))\n'
    '            cast_f.write(json.dumps([round(t, 3), "o", text]) + "\\n")'
)
OLD_DRAIN = 'def drain(timeout=0.1):'
NEW_DRAIN = '''def drain(timeout=0.1):
            open("/tmp/emit.log","a").write("DRAIN entry timeout=%s child_dead=%s\\n" % (timeout, child_dead))
            import time as _t
            _stime = _t.monotonic()
'''
OLD_STEP_LOOP_TOP = 'try:\n            for desc, keys_shown, keys, pause in steps:'
NEW_STEP_LOOP_TOP = '''try:
            open("/tmp/emit.log","a").write("STEPLOOP child_dead=%s\\n" % child_dead)
            for desc, keys_shown, keys, pause in steps:'''

src2 = src
src2 = src2.replace(OLD_EMIT, NEW_EMIT)
# src2 = src2.replace(OLD_DRAIN, NEW_DRAIN)
src2 = src2.replace(OLD_STEP_LOOP_TOP, NEW_STEP_LOOP_TOP)

# add log just after warmup
src2 = src2.replace(
    "        drain(2.0)\n        time.sleep(1.0)",
    '''        open("/tmp/emit.log","a").write("WARMUP start\\n")
        drain(2.0)
        open("/tmp/emit.log","a").write("WARMUP drain done child_dead=%s\\n" % child_dead)
        time.sleep(1.0)
        open("/tmp/emit.log","a").write("WARMUP sleep done\\n")''',
)

with open("/tmp/emit.log", "w") as f:
    pass

sys.argv = ["patched", "sparkline"]
exec(compile(src2, "patched", "exec"), {"__name__": "__main__"})
