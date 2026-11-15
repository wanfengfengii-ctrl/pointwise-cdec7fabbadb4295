"""Upgrade, SDK2 lifecycle, failure and process-restart verification.

Run with ``python3 -I test_upgrade.py``.  The legacy regressions stay in
``regression.py`` and are executed separately by the runtime manifest.
"""
import json
import os
import sqlite3
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from ingest import Ingestor, is_upgraded          # noqa: E402
from migration import upgrade                     # noqa: E402
from samples import (RECORDS, REPLACEMENT,          # noqa: E402
                     encoded, from_script, seed, world)
from storage import Storage2                       # noqa: E402
from vendor.service import (RefreshFailed, VersionGone, World)  # noqa: E402

EXPECTED = [(i, t, v) for i, (t, v) in enumerate(RECORDS)]


def capture_credentials(w):
    """Record issued credentials through the public ``issue`` interface only."""
    issued = []
    original = w.issue

    def issue():
        credential = original()   # raises RefreshFailed, nothing recorded
        issued.append(credential)
        return credential

    w.issue = issue
    return issued


def expiries(issued):
    return {credential.token: credential.expires_at for credential in issued}


def legacy_snapshot(db):
    return (db.execute('SELECT * FROM meta ORDER BY rowid').fetchall(),
            db.execute('SELECT * FROM tasks ORDER BY task_id').fetchall(),
            db.execute('SELECT * FROM warehouse ORDER BY task_id,ordinal').fetchall())


def upgraded(db):
    db = sqlite3.connect(db) if not isinstance(db, sqlite3.Connection) else db
    return is_upgraded(db)


class MigrationTests(unittest.TestCase):
    def test_real_upgrade_preserves_identity_rows_and_audit(self):
        db = seed(sqlite3.connect(':memory:'))
        before = legacy_snapshot(db)
        self.assertIsNone(upgrade(db))
        self.assertTrue(is_upgraded(db))
        self.assertEqual(db.execute('SELECT * FROM format2').fetchall(), [(2,)])
        self.assertEqual(db.execute('SELECT * FROM tasks2 ORDER BY task_id').fetchall(), [
            ('empty', 'obs', 'station A.bin', 'E1', 0),
            ('resume', 'obs', 'station A.bin', 'E1', 32)])
        self.assertEqual(db.execute(
            'SELECT task_id,ordinal,observed,value FROM warehouse2 '
            'ORDER BY task_id,ordinal').fetchall(),
            [('resume', 0, 100, 12), ('resume', 1, 101, -4)])
        # Legacy tables are byte-for-byte audit data, not deleted or rewritten.
        self.assertEqual(legacy_snapshot(db), before)

    def test_upgrade_is_idempotent(self):
        db = seed(sqlite3.connect(':memory:'))
        upgrade(db)
        before = (legacy_snapshot(db), db.execute('SELECT * FROM tasks2').fetchall(),
                  db.execute('SELECT * FROM warehouse2').fetchall())
        self.assertIsNone(upgrade(db))
        self.assertEqual(
            (legacy_snapshot(db), db.execute('SELECT * FROM tasks2').fetchall(),
             db.execute('SELECT * FROM warehouse2').fetchall()), before)

    def test_hook_failure_rolls_back_everything(self):
        db = seed(sqlite3.connect(':memory:'))
        before = legacy_snapshot(db)
        class Stop(Exception):
            pass
        def boom(phase):
            raise Stop()
        with self.assertRaises(Stop):
            upgrade(db, hook=boom)
        self.assertFalse(is_upgraded(db))
        self.assertEqual(legacy_snapshot(db), before)
        self.assertEqual(db.execute(
            "SELECT name FROM sqlite_master WHERE name IN ('format2','tasks2','warehouse2')"
        ).fetchall(), [])
        # The old database still works through the legacy chain after failure.
        self.assertEqual(Ingestor(db, world()).run('resume'), 80)
        # Retrying the migration afterwards succeeds cleanly.
        self.assertIsNone(upgrade(db))
        self.assertTrue(is_upgraded(db))
        self.assertEqual(len(Ingestor(db, world()).rows('resume')), 5)

    def test_rejects_connection_with_open_transaction(self):
        db = seed(sqlite3.connect(':memory:'))
        db.execute('BEGIN IMMEDIATE')
        with self.assertRaises(RuntimeError):
            upgrade(db)
        db.execute('ROLLBACK')
        self.assertFalse(is_upgraded(db))

    def test_upgrade_by_path_and_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'old.sqlite')
            subprocess.run([sys.executable, str(HERE / 'samples.py'), path], check=True,
                           cwd=str(HERE))
            upgrade(path)
            conn = sqlite3.connect(path)
            self.assertTrue(is_upgraded(conn))
            upgrade(conn)
            self.assertEqual(Ingestor(conn, world()).run('resume'), 80)
            conn.close()


class SDK2LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.db = seed(sqlite3.connect(':memory:'))
        upgrade(self.db)

    def tearDown(self):
        self.db.close()

    def test_rotates_before_expiry_and_reads_original_etag(self):
        w = world(old_expires=100)
        issued = capture_credentials(w)
        ing = Ingestor(self.db, w)
        self.assertEqual(ing.run('resume'), 80)
        self.assertEqual(ing.rows('resume'), EXPECTED)
        self.assertGreaterEqual(len(issued), 2)  # credentials really rotate
        self.assertFalse([t for t in w.trace if t.get('sdk') == 1])
        opens = [t for t in w.trace if t['op'] == 'open']
        self.assertTrue(all(t['etag'] == 'E1' for t in opens))
        reads = [t for t in w.trace if t['op'] == 'read']
        # Each read happens strictly before its credential's expiry, and no
        # stream is opened with an expired credential.
        expiry = expiries(issued)
        for r in reads:
            self.assertLess(r['at'], expiry[r['token']])
        for t in opens:
            self.assertLess(t['at'], expiry[t['token']])
        # Position only advances by successful bytes: the final read ends at 80.
        self.assertEqual(max(r['offset'] for r in reads), 80)

    def test_refresh_failure_still_reads_with_unexpired_old_credential(self):
        w = world(renew_fail_at=[2, 3], old_expires=100)
        issued = capture_credentials(w)
        self.assertEqual(Ingestor(self.db, w).run('resume'), 80)
        # A failed issuance at t=2 must not have aborted the run nor forced an
        # expired credential: reads at t=2 keep using the old, still-valid one.
        self.assertTrue(any(t['op'] == 'issue' and t['at'] == 2 for t in w.trace))
        reads_at_2 = [t for t in w.trace if t['op'] == 'read' and t['at'] == 2]
        self.assertTrue(reads_at_2)
        expiry = expiries(issued)
        for r in reads_at_2:
            self.assertLess(2, expiry[r['token']])
        # Later successful issuance is actually used, not merely cached.
        tokens_after = {t['token'] for t in w.trace
                        if t['op'] in ('open', 'read') and t['at'] >= 3}
        self.assertTrue(tokens_after - {reads_at_2[0]['token']})

    def test_expired_without_renewal_never_reads(self):
        # Renewal fails from t=2 on; the credential issued at t=0 expires at 4.
        w = world(renew_fail_at=list(range(2, 50)), old_expires=100)
        issued = capture_credentials(w)
        ing = Ingestor(self.db, w)
        with self.assertRaises(RefreshFailed):
            ing.run('resume')
        expiry = expiries(issued)
        expired = [t for t in w.trace
                   if t['op'] in ('open', 'read') and t['at'] >= expiry[t['token']]]
        self.assertEqual(expired, [])
        # Whatever was committed is a clean 16-byte prefix; rows agree.
        committed, = self.db.execute(
            "SELECT committed FROM tasks2 WHERE task_id='resume'").fetchone()
        self.assertEqual(committed % 16, 0)
        self.assertEqual(len(ing.rows('resume')), committed // 16)

    def test_rows_match_independently_decoded_raw_bytes(self):
        # Cross-check the delivered rows against the frozen original byte
        # stream, decoded here without going through ingest or storage.
        w = from_script(HERE / 'scenarios.json')
        ing = Ingestor(self.db, w)
        self.assertEqual(ing.run('resume'), 80)
        raw = w.objects[('obs', 'station A.bin', 'E1')]
        independently = [struct.unpack('>qq', raw[i:i + 16])
                         for i in range(0, len(raw), 16)]
        self.assertEqual(ing.rows('resume'), [(i, t, v)
                                              for i, (t, v) in enumerate(independently)])
        self.assertEqual(len(raw), 80)

    def test_interrupted_read_never_delivers_its_buffer(self):
        # The very first read faults after dirtying 3 bytes, after a same-call
        # version replacement of the path.  Rows must match the original E1.
        w = from_script(HERE / 'scenarios.json')
        ing = Ingestor(self.db, w)
        self.assertEqual(ing.run('resume'), 80)
        self.assertEqual(ing.rows('resume'), EXPECTED)
        self.assertEqual(ing.rows('resume')[-1], (4, 104, 7))

    def test_persistent_etag_survives_path_replacement_between_runs(self):
        # Commit only through offset 48, then the same path is replaced in a
        # later read call.  The retry must keep reading the original E1.
        class Stop(Exception):
            pass
        def stop(phase, following):
            if phase == 'before_commit' and following == 48:
                raise Stop()
        e2 = encoded(REPLACEMENT).hex()
        w = world(events={1: [{'op': 'replace', 'etag': 'E2', 'hex': e2}]})
        ing = Ingestor(self.db, w)
        with self.assertRaises(Stop):
            ing.run('resume', hook=stop)
        self.assertEqual(self.db.execute(
            "SELECT committed FROM tasks2 WHERE task_id='resume'").fetchone(), (32,))
        self.assertEqual(w.current[('obs', 'station A.bin')], 'E2')
        self.assertEqual(ing.run('resume'), 80)
        # Rows are the original E1 records, not the same-path replacement.
        self.assertEqual(ing.rows('resume'), EXPECTED)

    def test_version_gone_keeps_checkpoint_and_prefix(self):
        e2 = encoded(REPLACEMENT).hex()
        w = world(events={1: [{'op': 'replace', 'etag': 'E2', 'hex': e2},
                              {'op': 'drop', 'etag': 'E1'}]})
        ing = Ingestor(self.db, w)
        with self.assertRaises(VersionGone):
            ing.run('resume')
        self.assertEqual(self.db.execute(
            "SELECT committed FROM tasks2 WHERE task_id='resume'").fetchone(), (32,))
        self.assertEqual(ing.rows('resume'), EXPECTED[:2])
        # The replacement E2 exists under the same path but must not be bound.
        self.assertIn(('obs', 'station A.bin', 'E2'), w.objects)
        # No later read was silently re-pointed at the current version.
        self.assertTrue(all(t['etag'] == 'E1' for t in w.trace
                            if t['op'] in ('open', 'read')))

    def test_legacy_tables_not_consulted_after_upgrade(self):
        # Deleting business rows from the audit tables must not affect results.
        self.db.execute('DELETE FROM warehouse')
        self.db.execute("UPDATE tasks SET signed_url='broken://x'")
        ing = Ingestor(self.db, world())
        self.assertEqual(ing.run('resume'), 80)
        self.assertEqual(ing.rows('resume'), EXPECTED)

    def test_empty_task_is_independent(self):
        ing = Ingestor(self.db, world())
        self.assertEqual(ing.run('empty'), 80)
        self.assertEqual(len(ing.rows('empty')), 5)
        self.assertEqual(ing.rows('resume'), EXPECTED[:2])

    def test_unknown_task_raises_keyerror_after_upgrade(self):
        ing = Ingestor(self.db, world())
        with self.assertRaises(KeyError):
            ing.run('missing')
        with self.assertRaises(KeyError):
            ing.rows('missing')

    def test_truncated_tail_keeps_complete_record_prefix(self):
        body = encoded(RECORDS) + b'\x01\x02\x03\x04\x05'   # 85 bytes
        w = World({('obs', 'station A.bin', 'E1'): body},
                  {('obs', 'station A.bin'): 'E1'}, {'old': 100}, None, (), 7)
        ing = Ingestor(self.db, w)
        with self.assertRaises(ValueError):
            ing.run('resume')
        self.assertEqual(self.db.execute(
            "SELECT committed FROM tasks2 WHERE task_id='resume'").fetchone(), (80,))
        self.assertEqual(ing.rows('resume'), EXPECTED)      # all complete records
        with self.assertRaises(ValueError):                # still truncated
            ing.run('resume')
        self.assertEqual(len(ing.rows('resume')), 5)        # no duplicate rows

    def test_payload_buffer_has_fixed_bound(self):
        # A large object is streamed with the same fixed request size; rows are
        # complete and the buffer is independent of object size.
        rows = [(1000 + i, i) for i in range(2000)]  # 32 KiB
        w = World({('obs', 'big.bin', 'B1'): encoded(rows)},
                  {('obs', 'big.bin'): 'B1'}, {'old': 10 ** 9}, None, (), 4096)
        db = sqlite3.connect(':memory:')
        db.executescript((HERE / 'schema_v1.sql').read_text())
        with db:
            db.execute('INSERT INTO tasks VALUES(?,?,?,?)',
                       ('big', 'mock://obs/big.bin?sig=old', 'B1', 0))
        upgrade(db)
        self.assertEqual(Ingestor(db, w).run('big'), len(rows) * 16)
        self.assertEqual(db.execute('SELECT count(*) FROM warehouse2').fetchone(), (2000,))
        self.assertEqual(len(Ingestor(db, w).rows('big')), 2000)
        mounts = [t['count'] for t in w.trace if t['op'] == 'read']
        self.assertTrue(mounts)
        self.assertLessEqual(max(mounts), Storage2.BLOCK)


class ProcessRestartTests(unittest.TestCase):
    """Real os._exit in a child process: the only credible crash evidence."""

    def _spawn(self, tmp, script, *args):
        return subprocess.run([sys.executable, '-I', str(script), *args],
                              cwd=str(tmp), capture_output=True, text=True)

    def test_child_exit_before_commit_rolls_back_row_and_position(self):
        with tempfile.TemporaryDirectory() as tmp:
            dbpath = os.path.join(tmp, 'old.sqlite')
            subprocess.run([sys.executable, str(HERE / 'samples.py'), dbpath],
                           check=True, cwd=str(HERE))
            upgrade(dbpath)
            child = HERE / '_crash_child.py'
            r = self._spawn(tmp, child, dbpath, 'before_commit')
            self.assertNotEqual(r.returncode, 0)
            conn = sqlite3.connect(dbpath)
            self.assertEqual(conn.execute(
                "SELECT committed FROM tasks2 WHERE task_id='resume'").fetchone(), (32,))
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM warehouse2").fetchone(), (2,))
            # Reopen continues exactly from the durable prefix.
            ing = Ingestor(conn, world())
            self.assertEqual(ing.run('resume'), 80)
            self.assertEqual(ing.rows('resume'), EXPECTED)
            self.assertEqual(ing.run('resume'), 80)
            self.assertEqual(len(ing.rows('resume')), 5)
            conn.close()

    def test_child_exit_after_commit_keeps_row_and_resumes(self):
        with tempfile.TemporaryDirectory() as tmp:
            dbpath = os.path.join(tmp, 'old.sqlite')
            subprocess.run([sys.executable, str(HERE / 'samples.py'), dbpath],
                           check=True, cwd=str(HERE))
            upgrade(dbpath)
            child = HERE / '_crash_child.py'
            r = self._spawn(tmp, child, dbpath, 'after_commit')
            self.assertNotEqual(r.returncode, 0)
            conn = sqlite3.connect(dbpath)
            # The committed record and position survived the abrupt exit.
            self.assertEqual(conn.execute(
                "SELECT committed FROM tasks2 WHERE task_id='resume'").fetchone(), (48,))
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM warehouse2").fetchone(), (3,))
            ing = Ingestor(conn, world())
            self.assertEqual(ing.run('resume'), 80)
            rows = ing.rows('resume')
            self.assertEqual(len(rows), 5)
            self.assertEqual(len(set(r[0] for r in rows)), 5)  # no duplicates
            self.assertEqual(rows, EXPECTED)
            conn.close()

    def test_child_exit_during_migration_leaves_old_db_usable(self):
        with tempfile.TemporaryDirectory() as tmp:
            dbpath = os.path.join(tmp, 'old.sqlite')
            subprocess.run([sys.executable, str(HERE / 'samples.py'), dbpath],
                           check=True, cwd=str(HERE))
            child = HERE / '_crash_child.py'
            r = self._spawn(tmp, child, dbpath, 'migration_before_commit')
            self.assertNotEqual(r.returncode, 0)
            conn = sqlite3.connect(dbpath)
            self.assertFalse(is_upgraded(conn))
            self.assertEqual(conn.execute('SELECT count(*) FROM tasks').fetchone(), (2,))
            # Old chain still works; a retry migrates cleanly.
            self.assertEqual(Ingestor(conn, world()).run('resume'), 80)
            upgrade(conn)
            self.assertTrue(is_upgraded(conn))
            self.assertEqual(Ingestor(conn, world()).run('resume'), 80)
            conn.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
