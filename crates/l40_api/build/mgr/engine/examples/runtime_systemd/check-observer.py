# Exercise the production observer against the real PID1 in the disposable harness.
import os
import signal
import subprocess
import sys

expected = dict(arg.split('=', 1) for arg in sys.argv[1:])
process = subprocess.Popen(
    ['timeout', '3s', '/bin/sh', '/fixtures/observe.sh', *expected],
    stdout=subprocess.PIPE, text=True, start_new_session=True,
)
try:
    lines = []
    for line in process.stdout:
        if line == '.\n':
            break
        lines.append(line)
    else:
        raise AssertionError('observer did not complete a batch')
    units = {}
    for block in ''.join(lines).split('\n\n'):
        values = dict(line.split('=', 1) for line in block.splitlines() if '=' in line)
        if 'Id' in values:
            units[values['Id']] = values
    for unit, state in expected.items():
        actual = units[unit]
        assert actual['LoadState'] == 'loaded', actual
        assert actual['ActiveState'] + '/' + actual['SubState'] == state, actual
        assert int(actual['InactiveExitTimestampMonotonic']) > 0, actual
finally:
    os.killpg(process.pid, signal.SIGKILL)
    process.wait()
