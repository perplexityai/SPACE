"""Explicit stopped-disk cold restart, distinct from memory restore."""

from __future__ import annotations

import json
import time
from typing import cast
from urllib.parse import quote

from space_smoke.native_concurrency import required_string
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.scenarios.native_retained_ebs_suspend import _lifecycle, _run


SEED = r"""
import os
from pathlib import Path
for name in ("/data/cold-start-proof", "/var/tmp/cold-start-root-proof"):
    with open(name, "xb") as output:
        output.write(os.urandom(4096))
        output.flush()
        os.fsync(output.fileno())
"""

OBSERVE = r"""
import hashlib, json, os, subprocess
from pathlib import Path
mounts = [line.split() for line in Path("/proc/self/mountinfo").read_text().splitlines()]
entries = [entry for entry in mounts if entry[4] == "/data"]
assert len(entries) == 1, "required data mount missing or ambiguous"
entry = entries[0]
assert entry[entry.index("-") + 1:entry.index("-") + 3] == ["ext4", "/dev/vdb"], entry
assert os.stat("/data").st_dev != os.stat("/").st_dev
print(json.dumps({
    "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
    "filesystem_uuid": subprocess.check_output(
        ["blkid", "-s", "UUID", "-o", "value", "/dev/vdb"], text=True,
    ).strip(),
    "data_hash": hashlib.sha256(Path("/data/cold-start-proof").read_bytes()).hexdigest(),
    "root_hash": hashlib.sha256(Path("/var/tmp/cold-start-root-proof").read_bytes()).hexdigest(),
}))
"""


@scenario(
    "native.retained_volume_cold_start",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    tier="manual",
    manual_reason="requires explicit retained cold-start host qualification and one owned EBS disk",
    timeout_minutes=45,
)
def retained_volume_cold_start(test: NativeHardeningFixture) -> None:
    """Cold boots retain the same data filesystem, not the old guest kernel.

    The ordinary stopped-rootfs policy must retain this sandbox's root marker.
    This does not qualify relocation of stopped rootfs bytes or checkpoint
    discard, and requires independent provider identity and cleanup checks.
    """
    with test.context.phase("create a data disk and durable root/data markers"):
        sandbox = test.create_ready({
            "volume_mounts": [{
                "type": "ebs", "size_gib": 128,
                "permissions": "read_write", "mount": "/data",
            }],
        }, timeout=600)
        sandbox_path = f"/v1/sandboxes/{quote(sandbox, safe='')}"
        owner = test.request("GET", sandbox_path).require(200)
        mounts = cast(list[dict[str, object]], owner["volume_mounts"])
        test.context.verify(len(mounts) == 1, "required volume binding missing")
        volume = required_string(mounts[0], "volume_id")
        volume_path = f"/v1/volumes/{quote(volume, safe='')}"
        _run(test, sandbox, SEED)
        original = json.loads(_run(test, sandbox, OBSERVE))
        test.context.record("before_cold_start", original)
        previous_boot = original["boot_id"]

    for cycle in range(2):
        with test.context.phase(f"stop and observe disk detach before cold boot {cycle}"):
            _lifecycle(test, sandbox, "stop", "stopped")
            deadline = time.monotonic() + 180
            while True:
                resource = test.request("GET", volume_path).require(200)
                if resource["state"] == "available":
                    break
                test.context.verify(time.monotonic() < deadline, "stopped disk did not detach")
                time.sleep(0.5)

        with test.context.phase(f"cold boot {cycle} preserves data and filesystem identity"):
            _lifecycle(test, sandbox, "start", "started")
            observed = json.loads(_run(test, sandbox, OBSERVE))
            test.context.record(f"after_cold_start_{cycle}", observed)
            test.context.verify(
                observed["boot_id"] != previous_boot, "start reused the old guest kernel",
            )
            for field in ("filesystem_uuid", "data_hash", "root_hash"):
                test.context.verify(
                    observed[field] == original[field], f"cold start changed {field}",
                )
            previous_boot = observed["boot_id"]
            owner = test.request("GET", sandbox_path).require(200)
            mounts = cast(list[dict[str, object]], owner["volume_mounts"])
            test.context.verify(
                len(mounts) == 1 and mounts[0]["volume_id"] == volume,
                "cold start replaced its required logical disk",
            )
