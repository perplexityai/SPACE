import json
import os
import sys

with open('/fixtures/expected.json') as source:
    expected = json.load(source)
assert sys.argv[1:] == expected['argv']
assert os.environ['LITERAL'] == expected['env']
assert os.getuid() != 0
print('runtime-probe-output', flush=True)
sys.exit(0 if os.path.exists('/run/runtime-ready') else 1)
