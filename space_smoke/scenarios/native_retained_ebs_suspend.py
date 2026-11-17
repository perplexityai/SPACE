"""Opt-in retained EBS disk lifecycle proof through the native API."""

from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import quote

from space_smoke.native_concurrency import command_output
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario


SERVER = r"""
import hashlib, json, os, secrets, socket
from pathlib import Path

nonce = secrets.token_hex(32)
Path("/tmp/retained-root-proof").write_text("root-proof")
Path("/data/retained-data-proof").write_text("data-proof")
held = open("/data/retained-open-proof", "w+")
os.unlink("/data/retained-open-proof")
held.write("open")
held.flush()
listener = socket.socket(socket.AF_UNIX)
listener.bind("/tmp/retained-proof.sock")
listener.listen(1)
counter = 0
while True:
    client, _ = listener.accept()
    client.recv(1)
    counter += 1
    held.seek(0, 2)
    held.write(":" + str(counter))
    held.flush()
    os.fsync(held.fileno())
    held.seek(0)
    result = {
        "nonce": nonce, "counter": counter, "open_fd": held.read(),
        "root_hash": hashlib.sha256(Path("/tmp/retained-root-proof").read_bytes()).hexdigest(),
        "data_hash": hashlib.sha256(Path("/data/retained-data-proof").read_bytes()).hexdigest(),
    }
    client.sendall(json.dumps(result).encode())
    client.close()
"""
LAUNCH = (
    "import subprocess,sys,time; from pathlib import Path; "
    f"p=subprocess.Popen([sys.executable,'-c',{SERVER!r}],"
    "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,"
    "start_new_session=True); "
    "deadline=time.monotonic()+15\n"
    "while not Path('/tmp/retained-proof.sock').exists():\n"
    " assert p.poll() is None, 'proof process exited'\n"
    " assert time.monotonic()<deadline, 'proof process not ready'\n"
    " time.sleep(.05)\n"
)
QUERY = (
    "import socket,sys; s=socket.socket(socket.AF_UNIX); s.settimeout(10); "
    "s.connect('/tmp/retained-proof.sock'); s.sendall(b'?'); "
    "sys.stdout.buffer.write(s.recv(8192))"
)
READ_ONLY = r"""
import errno, os, subprocess
from pathlib import Path
assert not Path("/data/retained-data-proof").exists(), "another sandbox's data leaked"
result = subprocess.run(["mount", "-o", "remount,rw", "/data"], capture_output=True)
assert result.returncode != 0, "guest remounted a host-read-only drive writable"
for path in ["/data/forbidden-write", "/dev/vdb"]:
    try:
        fd = os.open(path, os.O_WRONLY | (os.O_CREAT if path.startswith("/data/") else 0))
    except OSError as error:
        assert error.errno in [errno.EROFS, errno.EACCES, errno.EPERM], error
    else:
        os.close(fd)
        raise AssertionError("read-only drive opened writable: " + path)
print("read-only-and-isolated")
"""


@dataclass(frozen=True)
class Observation:
    nonce: str
    counter: int
    open_fd: str
    root_hash: str
    data_hash: str


def _run(test: NativeHardeningFixture, sandbox: str, program: str) -> bytes:
    accepted = test.command(sandbox, ["sudo", "-n", "python3", "-c", program]).require(201)
    events = test.events(accepted)
    test.context.verify(events.status == 200, "guest command attachment failed")
    output, errors = command_output(events.events, limit=32768)
    test.context.verify(not errors, "guest proof command failed", stderr=errors.decode(errors="replace"))
    return output


def _observe(test: NativeHardeningFixture, sandbox: str) -> Observation:
    return Observation(**json.loads(_run(test, sandbox, QUERY)))


def _lifecycle(test: NativeHardeningFixture, sandbox: str, operation: str, target: str) -> None:
    accepted = test.request(
        "POST", f"/v1/sandboxes/{quote(sandbox, safe='')}:{operation}",
        idempotency_key=test.key(operation),
    ).require(202)
    test.wait_operation(str(accepted["operation_id"]), timeout=600)
    test.wait_sandbox(sandbox, target, timeout=600)


@scenario(
    "native.retained_ebs_suspend_preserves_open_fd",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    tier="manual",
    manual_reason="requires an admitted organization, qualified workers and a two-disk EBS budget",
    timeout_minutes=45,
)
def retained_ebs_suspend_preserves_open_fd(test: NativeHardeningFixture) -> None:
    """Two restores preserve RAM, root/data hashes and an unlinked open FD.

    This portable contract does not force worker movement and is not evidence
    of cross-node portability. The deployment's A→B→A qualification must also
    verify actual node identities, unchanged AWS volume ID and final deletion.
    """
    with test.context.phase("create a writable retained disk and an in-memory proof process"):
        sandbox = test.create_ready({"volume_mounts": [{
            "type": "ebs", "size_gib": 128, "permissions": "read_write", "mount": "/data",
        }]}, timeout=600)
        _run(test, sandbox, LAUNCH)
        original = _observe(test, sandbox)
        test.context.verify(original.counter == 1 and original.open_fd == "open:1",
                            "initial proof process state is incorrect")

    for cycle in (1, 2):
        with test.context.phase(f"suspend and restore retained disk cycle {cycle}"):
            _lifecycle(test, sandbox, "suspend", "suspended")
            _lifecycle(test, sandbox, "restore", "running")
            observed = _observe(test, sandbox)
            test.context.verify(
                observed.nonce == original.nonce
                and observed.counter == cycle + 1
                and observed.open_fd == "open:" + ":".join(map(str, range(1, cycle + 2)))
                and observed.root_hash == original.root_hash
                and observed.data_hash == original.data_hash,
                "RAM, open file descriptor or filesystem state changed across restore",
            )

    with test.context.phase("a second read-only disk cannot expose or modify the first disk"):
        readonly = test.create_ready({"volume_mounts": [{
            "type": "ebs", "size_gib": 128, "permissions": "read_only", "mount": "/data",
        }]}, timeout=600)
        test.context.verify(
            _run(test, readonly, READ_ONLY).strip() == b"read-only-and-isolated",
            "host-enforced read-only or sandbox isolation proof failed",
        )
