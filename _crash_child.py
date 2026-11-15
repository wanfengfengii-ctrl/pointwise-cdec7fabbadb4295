"""Real process-termination probe for the restart tests.

Usage: python3 _crash_child.py DB PATH MODE

MODE is ``before_commit`` (exit inside the first per-record transaction after
the insert/update but before COMMIT), ``after_commit`` (exit right after a
committed record) or ``migration_before_commit`` (exit inside the migration's
``before_commit`` hook, before the format switch is committed).
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingest import Ingestor           # noqa: E402
from migration import upgrade         # noqa: E402
from samples import world             # noqa: E402


def main():
    dbpath, mode = sys.argv[1], sys.argv[2]
    conn = sqlite3.connect(dbpath)
    if mode == 'migration_before_commit':
        def stop(phase):
            os._exit(23)
        upgrade(conn, hook=stop)
        os._exit(0)
    ing = Ingestor(conn, world())
    if mode == 'before_commit':
        def before(phase, offset):
            if phase == 'before_commit':
                os._exit(21)
        ing.run('resume', hook=before)
    elif mode == 'after_commit':
        def after(phase, offset):
            if phase == 'after_commit' and offset == 48:
                os._exit(22)
        ing.run('resume', hook=after)
    else:
        raise SystemExit('unknown mode')
    os._exit(0)


if __name__ == '__main__':
    main()
