"""Frozen normal regressions. Uses a real SQLite connection without disk setup."""
import sqlite3
import unittest
from ingest import Ingestor
from samples import seed, world, RECORDS
from vendor.service import Expired, ReadInterrupted, VersionGone, RefreshFailed, locator
from vendor import sdk2

class LegacyRegression(unittest.TestCase):
    def setUp(self):
        self.db = seed(sqlite3.connect(':memory:'))
        self.w = world()
        self.ing = Ingestor(self.db, self.w)

    def tearDown(self):
        self.db.close()

    def test_resume_and_replay(self):
        self.assertEqual(self.ing.rows('resume'), [(i, *r) for i, r in enumerate(RECORDS[:2])])
        self.assertEqual(self.ing.run('resume'), 80)
        self.assertEqual(self.ing.rows('resume'), [(i, *r) for i, r in enumerate(RECORDS)])
        self.assertEqual(self.ing.run('resume'), 80)
        self.assertEqual(len(self.ing.rows('resume')), 5)

    def test_empty_task(self):
        self.assertEqual(self.ing.run('empty'), 80)
        self.assertEqual(self.ing.rows('empty'), [(i, *r) for i, r in enumerate(RECORDS)])

    def test_atomic_row_and_checkpoint(self):
        class Stop(Exception):
            pass
        def stop(phase, offset):
            if phase == 'before_commit':
                raise Stop()
        with self.assertRaises(Stop):
            self.ing.run('resume', hook=stop)
        self.assertEqual(self.db.execute("SELECT committed FROM tasks WHERE task_id='resume'").fetchone(), (32,))
        self.assertEqual(len(self.ing.rows('resume')), 2)
        self.assertEqual(self.ing.run('resume'), 80)

class DependencyRegression(unittest.TestCase):
    def test_locator(self):
        self.assertEqual(locator('mock://obs/a%2Fb%20c?sig=t'), ('obs', 'a/b c', 't'))

    def test_new_stream_binds_credential_and_short_reads(self):
        w = world()
        first = w.issue()
        s = sdk2.Client(w).open('obs', 'station A.bin', first, if_match='E1', offset=32)
        b = bytearray(16)
        self.assertEqual(s.readinto(b), 7)
        self.assertEqual(bytes(b[:7]), __import__('samples').encoded(RECORDS)[32:39])
        w.now = 4
        w.issue()
        with self.assertRaises(Expired):
            s.readinto(b)

    def test_failed_read_changes_buffer(self):
        w = world(events={1: [{'op': 'fault', 'after': 3}]})
        s = sdk2.Client(w).open('obs', 'station A.bin', w.issue(), if_match='E1', offset=32)
        b = bytearray(b'x' * 16)
        with self.assertRaises(ReadInterrupted):
            s.readinto(b)
        self.assertEqual(b[:3], __import__('samples').encoded(RECORDS)[32:35])
        self.assertEqual(b[3:], b'x' * 13)
        self.assertEqual(s.pos, 35)

    def test_version_condition_and_refresh_failure(self):
        w = world(renew_fail_at=[0])
        with self.assertRaises(RefreshFailed):
            w.issue()
        w.now = 1
        c = w.issue()
        with self.assertRaises(VersionGone):
            sdk2.Client(w).open('obs', 'station A.bin', c, if_match='missing', offset=0)

if __name__ == '__main__':
    unittest.main()
