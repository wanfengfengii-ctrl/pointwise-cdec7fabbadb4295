"""One-shot legacy (format 1) to format 2 migration.

Runs under a single maintenance worker.  Every statement, including the
``format2`` switch marker, executes inside one explicit SQLite transaction that
is committed only after ``hook('before_commit')`` returns.  A callback error or
a real process exit therefore leaves the legacy database untouched; a retry
rolls back and replays cleanly.

The legacy ``meta``/``tasks``/``warehouse`` tables are never dropped or edited:
after the switch they are read-only audit data.  Their signed URLs are only
decoded here, once, into the independent ``container``/``object_key`` target
identity.
"""
import sqlite3
from pathlib import Path

from vendor.service import locator

SCHEMA_V2 = Path(__file__).with_name('schema_v2.sql')

LEGACY_TABLES = ('meta', 'tasks', 'warehouse')
TARGET_TABLES = ('tasks2', 'warehouse2')


def _table_exists(db, name):
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _ddl():
    """schema_v2.sql split into individual statements.

    ``executescript`` would issue an implicit COMMIT before running the script,
    which would break the migration's single-atomic-transaction boundary.
    """
    lines = []
    for line in SCHEMA_V2.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        lines.append(line)
    statements = [' '.join(part.split()) for part in '\n'.join(lines).split(';')]
    return [stmt for stmt in statements if stmt]


def _validate_legacy(db):
    for name in LEGACY_TABLES:
        if not _table_exists(db, name):
            raise RuntimeError('not a legacy database: missing %s' % name)
    meta = db.execute('SELECT version FROM meta').fetchall()
    if meta != [(1,)]:
        raise RuntimeError('unsupported legacy meta: %r' % (meta,))

    tasks = db.execute('SELECT task_id,signed_url,etag,committed FROM tasks').fetchall()
    locations = {}
    for task_id, signed_url, etag, committed in tasks:
        if not isinstance(etag, str) or not etag:
            raise RuntimeError('task %r has no ETag' % (task_id,))
        if committed < 0 or committed % 16:
            raise RuntimeError('task %r has invalid committed position' % (task_id,))
        # Decoding here does not contact the service; the service is only
        # reached via SDK2 after the switch.  The stored ETag, never the path's
        # current version, remains the object identity.
        locations[task_id] = locator(signed_url)
    return tasks, locations


def _assert_copy_consistent(conn, tasks):
    """The copied v2 state must be exactly the legacy state."""
    for task_id, signed_url, etag, committed in tasks:
        row = conn.execute('SELECT etag,committed FROM tasks2 WHERE task_id=?',
                           (task_id,)).fetchone()
        if row != (etag, committed):
            raise RuntimeError('task %r copy mismatch' % (task_id,))
    legacy = conn.execute(
        'SELECT task_id,ordinal,observed,value FROM warehouse ORDER BY task_id,ordinal').fetchall()
    copied = conn.execute(
        'SELECT task_id,ordinal,observed,value FROM warehouse2 ORDER BY task_id,ordinal').fetchall()
    if legacy != copied:
        raise RuntimeError('warehouse copy mismatch')


def upgrade(db, *, hook=None):
    """Migrate a legacy database to format 2.  Returns ``None``.

    ``db`` is a path or an existing SQLite connection.
    """
    if isinstance(db, sqlite3.Connection):
        conn = db
        owned = False
        if conn.in_transaction:
            # Never silently commit or partially join an existing transaction:
            # the switch must be one atomic unit with the hook boundary.
            raise RuntimeError('connection already has an open transaction')
    else:
        conn = sqlite3.connect(db, isolation_level=None)
        conn.execute('PRAGMA foreign_keys=ON')
        owned = True
    try:
        # ``BEGIN IMMEDIATE`` makes the whole migration one write transaction,
        # regardless of the connection's configured isolation level.
        conn.execute('BEGIN IMMEDIATE')
        try:
            if _table_exists(conn, 'format2'):
                version = conn.execute('SELECT version FROM format2').fetchall()
                if version != [(2,)]:
                    raise RuntimeError('unexpected format2 contents: %r' % (version,))
                # Already upgraded: no data movement, no side effects.
                conn.execute('ROLLBACK')
                return None

            stray = [name for name in TARGET_TABLES if _table_exists(conn, name)]
            if stray:
                raise RuntimeError('target tables exist without format2: %r' % (stray,))
            tasks, locations = _validate_legacy(conn)

            for stmt in _ddl():
                conn.execute(stmt)
            for task_id, signed_url, etag, committed in tasks:
                container, key, _sig = locations[task_id]
                conn.execute('INSERT INTO tasks2 VALUES(?,?,?,?,?)',
                             (task_id, container, key, etag, committed))
            conn.execute(
                'INSERT INTO warehouse2(task_id,ordinal,observed,value) '
                'SELECT task_id,ordinal,observed,value FROM warehouse')
            conn.execute('INSERT INTO format2 VALUES(2)')
            _assert_copy_consistent(conn, tasks)

            if hook:
                hook('before_commit')   # may raise: rollback, legacy unchanged
            conn.execute('COMMIT')
        except BaseException:
            conn.execute('ROLLBACK')
            raise
    finally:
        if owned:
            conn.close()
    return None
