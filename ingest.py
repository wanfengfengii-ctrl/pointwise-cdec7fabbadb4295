"""Fixed-record telemetry ingestion and durable checkpoint.

The ingestor works against whichever storage generation the database has
switched to.  The switch is the ``format2`` marker row: the legacy tables are
never removed, but once upgraded they are audit-only and all reads/commits go
through ``tasks2``/``warehouse2`` plus the SDK2 storage boundary.
"""
import sqlite3
import struct
from storage import Storage, Storage2

RECORD = struct.Struct('>qq')


def connect(db):
    if isinstance(db, sqlite3.Connection):
        return db
    conn = sqlite3.connect(db)
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


def decode(raw):
    return RECORD.unpack(raw)


def is_upgraded(db):
    """True when the durable v2 switch marker is present and well-formed."""
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='format2'").fetchone()
    if row is None:
        return False
    versions = db.execute('SELECT version FROM format2').fetchall()
    if versions != [(2,)]:
        raise RuntimeError('malformed format2 switch marker: %r' % (versions,))
    return True


class Ingestor:
    def __init__(self, db, world):
        self.db = connect(db)
        self.world = world
        # Both boundaries exist up front so an Ingestor constructed *before*
        # the migration transparently keeps working afterwards.
        self.storage = Storage(world)
        self.storage2 = Storage2(world)

    def _upgraded_task(self, task_id):
        return self.db.execute(
            'SELECT container,object_key,etag,committed FROM tasks2 WHERE task_id=?',
            (task_id,)).fetchone()

    def rows(self, task_id):
        """Committed rows in ordinal order; unknown tasks raise ``KeyError``."""
        upgraded = is_upgraded(self.db)
        tasks = 'tasks2' if upgraded else 'tasks'
        if self.db.execute('SELECT 1 FROM %s WHERE task_id=?' % tasks,
                           (task_id,)).fetchone() is None:
            raise KeyError(task_id)
        table = 'warehouse2' if upgraded else 'warehouse'
        return self.db.execute(
            'SELECT ordinal,observed,value FROM %s WHERE task_id=? ORDER BY ordinal' % table,
            (task_id,)).fetchall()

    def _source(self, task_id):
        """Return (storage2?, args, committed) for the database's current format."""
        if is_upgraded(self.db):
            task = self._upgraded_task(task_id)
            if task is None:
                raise KeyError(task_id)
            container, key, etag, committed = task
            return True, (container, key, etag, committed), committed
        task = self.db.execute(
            'SELECT signed_url,etag,committed FROM tasks WHERE task_id=?',
            (task_id,)).fetchone()
        if task is None:
            raise KeyError(task_id)
        signed_url, etag, committed = task
        return False, (signed_url, etag, committed), committed

    def run(self, task_id, *, hook=None):
        upgraded, args, committed = self._source(task_id)
        storage = self.storage2 if upgraded else self.storage
        pending = bytearray()  # only the unconsumed <16-byte fragment
        for chunk in storage.chunks(*args):
            pending.extend(chunk)
            while len(pending) >= RECORD.size:
                raw = bytes(pending[:RECORD.size])
                observed, value = decode(raw)
                following = committed + RECORD.size
                with self.db:
                    if upgraded:
                        self.db.execute('INSERT INTO warehouse2 VALUES(?,?,?,?)',
                                        (task_id, committed // RECORD.size, observed, value))
                        self.db.execute('UPDATE tasks2 SET committed=? WHERE task_id=?',
                                        (following, task_id))
                    else:
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
