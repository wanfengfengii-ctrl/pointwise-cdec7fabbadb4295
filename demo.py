"""Demonstrate the real upgrade and the SDK2 rotation/resume path.

Builds a genuine legacy SQLite file with ``samples.seed``, shows its legacy
prefix, migrates it, then resumes the same task through SDK2 under
``scenarios.json`` (same-call version replacement, one 3-byte interrupted read,
a failed issuance at t=2 and the old signature expiring at t=3).

Run: python3 demo.py [database-path]
"""
import json
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingest import Ingestor, is_upgraded      # noqa: E402
from migration import upgrade                 # noqa: E402
from samples import from_script, seed, RECORDS, URL  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def legacy_view(db):
    return {
        'meta': db.execute('SELECT version FROM meta').fetchall(),
        'tasks': db.execute(
            'SELECT task_id,signed_url,etag,committed FROM tasks ORDER BY task_id').fetchall(),
        'warehouse': db.execute(
            'SELECT task_id,ordinal,observed,value FROM warehouse ORDER BY task_id,ordinal'
        ).fetchall(),
    }


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        tempfile.mkdtemp(prefix='telemetry-demo-'), 'old.sqlite')
    seed(path).close()

    db = sqlite3.connect(path)
    world = from_script(os.path.join(HERE, 'scenarios.json'))
    # Observe issued credentials through the public ``issue`` interface only.
    issued = {}
    original_issue = world.issue

    def issue():
        credential = original_issue()
        issued[credential.token] = credential
        return credential

    world.issue = issue
    ing = Ingestor(db, world)          # constructed *before* the upgrade
    report = {
        'database': path,
        'legacy_url': URL,
        'legacy_tables': legacy_view(db),
        'upgraded_before_run': is_upgraded(db),
        'legacy_prefix': ing.rows('resume'),
    }

    hook_phases = []
    upgrade(db, hook=hook_phases.append)
    report['upgrade'] = {
        'hook_phases': hook_phases,
        'upgraded_after_run': is_upgraded(db),
        'format2': db.execute('SELECT version FROM format2').fetchall(),
        'tasks2': [list(row) for row in db.execute(
            'SELECT task_id,container,object_key,etag,committed FROM tasks2 ORDER BY task_id')],
        # The legacy tables survive byte-for-byte as audit data.
        'audit_unchanged': legacy_view(db) == report['legacy_tables'],
    }

    trace_before_run = len(world.trace)
    report['committed'] = ing.run('resume')
    report['rows'] = [list(r) for r in ing.rows('resume')]
    report['expected_rows'] = [[i, t, v] for i, (t, v) in enumerate(RECORDS)]
    report['committed_after'] = db.execute(
        "SELECT committed FROM tasks2 WHERE task_id='resume'").fetchone()[0]
    report['rerun_committed'] = ing.run('resume')
    report['row_count_after_rerun'] = len(ing.rows('resume'))

    trace = world.trace[trace_before_run:]
    report['sdk2_trace'] = {
        'credentials_issued_at': [t['at'] for t in trace if t['op'] == 'issue'],
        'streams': [{'token': t['token'], 'etag': t['etag'], 'offset': t['offset']}
                    for t in trace if t['op'] == 'open'],
        'read_offsets': [t['offset'] for t in trace if t['op'] == 'read'],
        'etags_read': sorted({t['etag'] for t in trace if t['op'] == 'read'}),
        'reads_with_expired_credential': [
            t for t in trace if t['op'] in ('open', 'read')
            and t['at'] >= issued[t['token']].expires_at],
        'sdk1_calls': [t for t in trace if t.get('sdk') == 1],
    }
    report['source_versions'] = {
        'E1_bytes': len(world.objects[('obs', 'station A.bin', 'E1')]),
        'E2_present': ('obs', 'station A.bin', 'E2') in world.objects,
        'current_path_points_at': world.current[('obs', 'station A.bin')],
    }

    checks = {
        'upgrade_committed': report['upgrade']['upgraded_after_run'],
        'audit_preserved': report['upgrade']['audit_unchanged'],
        'resumed_from_32': report['committed'] == 80,
        'rows_match_original_E1': report['rows'] == report['expected_rows'],
        'checkpoint_persisted': report['committed_after'] == 80,
        'rerun_idempotent': report['rerun_committed'] == 80
                            and report['row_count_after_rerun'] == 5,
        'only_sdk2_used': not report['sdk2_trace']['sdk1_calls'],
        'always_original_etag': report['sdk2_trace']['etags_read'] == ['E1'],
        'rotated_credentials': len(report['sdk2_trace']['streams']) >= 2,
        'never_read_expired': not report['sdk2_trace']['reads_with_expired_credential'],
        'same_path_replacement_ignored': report['source_versions']['E2_present'],
    }
    report['checks'] = checks
    print(json.dumps(report, ensure_ascii=False, indent=2))
    db.close()
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        print('FAILED CHECKS: %s' % ', '.join(failed), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
