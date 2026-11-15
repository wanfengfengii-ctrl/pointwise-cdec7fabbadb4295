"""Public verification entry point.

Runs the frozen legacy regression suite (``regression.py``) unchanged and the
upgrade/lifecycle/restart suite (``test_upgrade.py``).  Both are executed as
real subprocesses so a failure in either is reported with its own output.

    python3 verify.py            # run both suites
    python3 verify.py legacy     # legacy regressions only
    python3 verify.py upgrade    # upgrade + SDK2 + restart checks only
"""
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUITES = {
    'legacy': ['regression.py'],
    'upgrade': ['-I', 'test_upgrade.py'],
}


def run(name, args):
    proc = subprocess.run([sys.executable, *args], cwd=str(HERE),
                          capture_output=True, text=True)
    status = 'PASS' if proc.returncode == 0 else 'FAIL'
    print('=== %s: %s (%s) ===' % (name, status, ' '.join(args)))
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    return proc.returncode == 0


def main(argv):
    selected = argv or list(SUITES)
    unknown = [name for name in selected if name not in SUITES]
    if unknown:
        print('unknown suite(s): %s' % ', '.join(unknown), file=sys.stderr)
        print('choose from: %s' % ', '.join(SUITES), file=sys.stderr)
        return 2
    results = {name: run(name, SUITES[name]) for name in selected}
    failed = [name for name, ok in results.items() if not ok]
    print('\nsummary: %s' % ', '.join(
        '%s=%s' % (name, 'ok' if ok else 'failed') for name, ok in results.items()))
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
