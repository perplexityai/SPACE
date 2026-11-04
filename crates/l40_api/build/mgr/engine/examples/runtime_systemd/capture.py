#!/usr/bin/python3
import json
import os
import signal
import sys

with open('/run/runtime-observed/app.json', 'w') as output:
    json.dump({'argv': sys.argv[1:], 'env': os.environ['LITERAL'],
               'user': os.getuid(), 'cwd': os.getcwd(),
               'setting': os.environ['RUNTIME_TEST']}, output)
print('runtime-app-output', flush=True)
while True:
    signal.pause()
