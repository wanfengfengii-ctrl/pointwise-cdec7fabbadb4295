"""Complete legacy database builder, including two ingested records."""
import json
import sqlite3
import struct
import sys
from pathlib import Path
from ingest import connect
from vendor.service import World

RECORDS = [(100, 12), (101, -4), (102, 0), (103, 19), (104, 7)]
REPLACEMENT = [(200, 90), (201, 91), (202, 92), (203, 93), (204, 94)]
URL = 'mock://obs/station%20A.bin?sig=old'

def encoded(rows):
    return b''.join(struct.pack('>qq', *r) for r in rows)

def world(*, events=None, renew_fail_at=(), max_read=7, old_expires=100):
    return World({('obs', 'station A.bin', 'E1'): encoded(RECORDS)},
                 {('obs', 'station A.bin'): 'E1'}, {'old': old_expires},
                 events, renew_fail_at, max_read)

def seed(db):
    c = connect(db)
    c.executescript(Path(__file__).with_name('schema_v1.sql').read_text())
    with c:
        c.executemany('INSERT INTO tasks VALUES(?,?,?,?)',
                      [('resume', URL, 'E1', 32), ('empty', URL, 'E1', 0)])
        c.executemany('INSERT INTO warehouse VALUES(?,?,?,?)',
                      [('resume', i, t, v) for i, (t, v) in enumerate(RECORDS[:2])])
    return c

def from_script(path):
    spec = json.loads(Path(path).read_text())
    return world(events=spec.get('events'), renew_fail_at=spec.get('renew_fail_at', []),
                 max_read=spec.get('max_read', 7), old_expires=spec.get('old_expires', 100))

if __name__ == '__main__':
    seed(sys.argv[1]).close()
