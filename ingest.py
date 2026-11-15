"""Existing fixed-record telemetry ingestion and durable checkpoint."""
import sqlite3
import struct
from storage import Storage

RECORD = struct.Struct('>qq')

def connect(db):
    if isinstance(db, sqlite3.Connection):
        return db
    conn = sqlite3.connect(db)
    conn.execute('PRAGMA foreign_keys=ON')
    return conn

def decode(raw):
    return RECORD.unpack(raw)

class Ingestor:
    def __init__(self, db, world):
        self.db = connect(db)
        self.storage = Storage(world)

    def rows(self, task_id):
        return self.db.execute(
            'SELECT ordinal,observed,value FROM warehouse WHERE task_id=? ORDER BY ordinal',
            (task_id,)).fetchall()

    def run(self, task_id, *, hook=None):
        task = self.db.execute(
            'SELECT signed_url,etag,committed FROM tasks WHERE task_id=?',
            (task_id,)).fetchone()
        if task is None:
            raise KeyError(task_id)
        url, etag, committed = task
        pending = bytearray()
        for chunk in self.storage.chunks(url, etag, committed):
            pending.extend(chunk)
            while len(pending) >= RECORD.size:
                raw = bytes(pending[:RECORD.size])
                observed, value = decode(raw)
                following = committed + RECORD.size
                with self.db:
                    self.db.execute('INSERT INTO warehouse VALUES(?,?,?,?)',
                                    (task_id, committed // RECORD.size, observed, value))
                    self.db.execute('UPDATE tasks SET committed=? WHERE task_id=?',
                                    (following, task_id))
                    if hook:
                        hook('before_commit', following)
                committed = following
                del pending[:RECORD.size]
                if hook:
                    hook('after_commit', committed)
        if pending:
            raise ValueError('truncated record')
        return committed
