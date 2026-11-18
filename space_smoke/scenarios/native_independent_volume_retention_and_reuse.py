"""Opt-in standalone-volume lifecycle through the native API."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from typing import cast
from urllib.parse import quote

from space_smoke.native_concurrency import required_string
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.scenarios.native_retained_ebs_suspend import (
    LAUNCH, _lifecycle, _observe, _run,
)


REUSED_READ_ONLY = r"""
import errno, hashlib, json, os, subprocess
from pathlib import Path
entries = [line.split() for line in Path("/proc/self/mountinfo").read_text().splitlines()]
entry, = [entry for entry in entries if entry[4] == "/retained"]
assert entry[entry.index("-") + 1:entry.index("-") + 3] == ["ext4", "/dev/vdb"], entry
assert os.stat("/retained").st_dev != os.stat("/").st_dev
assert Path("/sys/block/vdb/ro").read_text().strip() == "1"
assert not Path("/tmp/retained-proof.sock").exists(), "old sandbox rootfs reused"
assert subprocess.run(
    ["mount", "-o", "remount,rw", "/retained"], capture_output=True,
).returncode != 0, "read-only disk remounted writable"
for path in ["/retained/forbidden-write", "/dev/vdb"]:
    try:
        fd = os.open(path, os.O_WRONLY | (os.O_CREAT if path.startswith("/retained/") else 0))
        try:
            os.write(fd, b"forbidden")
        finally:
            os.close(fd)
    except OSError as error:
        assert error.errno in [errno.EROFS, errno.EACCES, errno.EPERM], error
    else:
        raise AssertionError("reused read-only disk accepted a write: " + path)
print(json.dumps({
    "data_hash": hashlib.sha256(Path("/retained/retained-data-proof").read_bytes()).hexdigest(),
    "filesystem_uuid": subprocess.check_output(
        ["blkid", "-s", "UUID", "-o", "value", "/dev/vdb"], text=True,
    ).strip(),
}))
"""


def _wait_unreserved(test: NativeHardeningFixture, path: str) -> dict[str, object]:
    deadline = time.monotonic() + 180
    while True:
        resource = test.request("GET", path).require(200)
        if resource["state"] == "available" and resource["sandbox_id"] is None:
            test.context.verify(
                resource["retained"] is True and resource["delete_requested"] is False,
                "standalone volume was not retained after sandbox deletion",
            )
            return resource
        test.context.verify(time.monotonic() < deadline, "volume reservation was not released")
        time.sleep(0.5)


def _delete_sandbox(test: NativeHardeningFixture, sandbox: str) -> None:
    accepted = test.request(
        "DELETE", f"/v1/sandboxes/{quote(sandbox, safe='')}",
        idempotency_key=test.key("delete-owner"),
    ).require(202)
    test.wait_operation(required_string(accepted, "operation_id"), timeout=600)
    test.wait_sandbox(sandbox, "deleted", timeout=600)


@scenario(
    "native.independent_volume_retention_and_reuse",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    tier="manual",
    manual_reason="requires independent-volume admission and one owned 128-GiB EBS disk",
    timeout_minutes=45,
)
def independent_volume_retention_and_reuse(test: NativeHardeningFixture) -> None:
    """One standalone disk survives restore and two sequential sandbox owners.

    Reuse changes the mount path and enforces read-only outside the guest.
    This does not force same-node or cross-node placement, exercise resize,
    or replace independent provider identity and final deletion evidence.
    """
    with test.context.phase("create and inspect one independent volume with replay"):
        key = test.key("independent-create")
        body: dict[str, object] = {"type": "ebs", "size_gib": 128}
        accepted = test.request(
            "POST", "/v1/volumes", body=body, idempotency_key=key,
        ).require(202)
        volume = required_string(accepted, "target_id")
        operation = required_string(accepted, "id")
        path = f"/v1/volumes/{quote(volume, safe='')}"
        replay = test.request(
            "POST", "/v1/volumes", body=body, idempotency_key=key,
        ).require(202)
        test.context.verify(
            replay["id"] == operation and replay["target_id"] == volume,
            "volume create replay changed resource or operation identity",
        )
        test.wait_operation(operation, timeout=600)
        _wait_unreserved(test, path)
        cursor = ""
        for _ in range(100):
            page = test.request("GET", "/v1/volumes" + cursor).require(200)
            rows = cast(list[dict[str, object]], page["volumes"])
            if any(row["volume_id"] == volume for row in rows):
                break
            after = page["next_after"]
            test.context.verify(bool(after), "created volume is absent from its owner's list")
            cursor = "?after=" + quote(str(after), safe="")
        else:
            raise AssertionError("volume listing exceeded its bounded page budget")

    with test.context.phase("mount by reference and start the original open-file process"):
        sandbox = test.create_ready({"volume_mounts": [{
            "volume_id": volume, "mount": "/data", "permissions": "read_write",
        }]}, timeout=600)
        _run(test, sandbox, LAUNCH)
        original = _observe(test, sandbox)
        test.context.record("independent_volume_process_before", asdict(original))
        filesystem_uuid = _run(
            test, sandbox,
            "import subprocess; print(subprocess.check_output("
            "['blkid','-s','UUID','-o','value','/dev/vdb'],text=True).strip())",
        ).decode().strip()
        test.context.verify(bool(filesystem_uuid), "mounted filesystem identity is missing")

    with test.context.phase("suspend keeps the logical reservation despite physical detach"):
        _lifecycle(test, sandbox, "suspend", "suspended")
        deadline = time.monotonic() + 180
        while True:
            resource = test.request("GET", path).require(200)
            test.context.verify(resource["sandbox_id"] == sandbox, "suspend freed the reservation")
            if resource["state"] == "available":
                break
            test.context.verify(time.monotonic() < deadline, "suspended disk did not detach")
            time.sleep(0.5)
        test.request("DELETE", path, idempotency_key=test.key("reserved-delete")).require(409)
        _lifecycle(test, sandbox, "restore", "running")
        observed = _observe(test, sandbox)
        test.context.record("independent_volume_process_after", asdict(observed))
        test.context.verify(
            observed.nonce == original.nonce
            and observed.counter == original.counter + 1
            and observed.open_fd == original.open_fd + ":" + str(observed.counter)
            and observed.root_hash == original.root_hash
            and observed.data_hash == original.data_hash,
            "independent-volume restore lost RAM, the original descriptor or filesystem data",
        )

    with test.context.phase("delete the first sandbox without deleting its standalone disk"):
        _delete_sandbox(test, sandbox)
        retained = _wait_unreserved(test, path)
        test.context.record("independent_volume_after_first_owner", retained)

    with test.context.phase("reuse the same filesystem at a new read-only mount"):
        second = test.create_ready({"volume_mounts": [{
            "volume_id": volume, "mount": "/retained", "permissions": "read_only",
        }]}, timeout=600)
        resource = test.request("GET", path).require(200)
        test.context.record("independent_volume_second_owner", resource)
        test.context.verify(
            resource["volume_id"] == volume and resource["sandbox_id"] == second
            and resource["mount"] == "/retained" and resource["permissions"] == "read_only",
            "volume inspection did not reflect its new reservation and mount policy",
        )
        observed_disk = json.loads(_run(test, second, REUSED_READ_ONLY))
        test.context.record("independent_volume_reuse", observed_disk)
        test.context.verify(
            observed_disk["filesystem_uuid"] == filesystem_uuid
            and observed_disk["data_hash"] == original.data_hash,
            "sandbox reuse replaced or reformatted the retained filesystem",
        )
        _delete_sandbox(test, second)
        _wait_unreserved(test, path)

    with test.context.phase("explicit deletion completes and replays its original operation"):
        key = test.key("independent-delete")
        accepted = test.request("DELETE", path, idempotency_key=key).require(202)
        operation = required_string(accepted, "id")
        test.wait_operation(operation, timeout=600)
        replay = test.request("DELETE", path, idempotency_key=key).require(202)
        tombstone = test.request("GET", path).require(200)
        test.context.verify(
            replay["id"] == operation
            and tombstone["state"] == "deleted"
            and tombstone["delete_requested"] is True,
            "independent volume deletion did not retain its terminal identity",
        )
