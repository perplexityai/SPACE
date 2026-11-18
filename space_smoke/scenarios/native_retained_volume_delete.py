"""Opt-in volume-addressed deletion through the native API."""

from __future__ import annotations

import time
from typing import cast
from urllib.parse import quote

from space_smoke.native_concurrency import required_string
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario


@scenario(
    "native.retained_volume_delete_blocks_restart",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    tier="manual",
    manual_reason="requires explicit volume-management admission and an owned EBS disk budget",
    timeout_minutes=45,
)
def retained_volume_delete_blocks_restart(test: NativeHardeningFixture) -> None:
    """Observe deletion completion and reject root-only fallback on restart.

    Provider deletion is the public operation's contract. Independent cloud
    inventory verification belongs to the caller's deployed qualification.
    """
    with test.context.phase("inspect a sandbox-owned disk and reject active deletion"):
        sandbox = test.create_ready({
            "volume_mounts": [{
                "type": "ebs", "size_gib": 128,
                "permissions": "read_write", "mount": "/data",
            }],
        }, timeout=600)
        sandbox_path = f"/v1/sandboxes/{quote(sandbox, safe='')}"
        owner = test.request("GET", sandbox_path).require(200)
        mounts = cast(list[dict[str, object]], owner["volume_mounts"])
        test.context.verify(len(mounts) == 1, "required volume binding is absent")
        volume = required_string(mounts[0], "volume_id")
        path = f"/v1/volumes/{quote(volume, safe='')}"
        resource = test.request("GET", path).require(200)
        test.context.verify(
            resource["sandbox_id"] == sandbox and resource["mount"] == "/data",
            "volume inspection returned the wrong owner or mount",
        )
        test.request("DELETE", path, idempotency_key=test.key("active-delete")).require(409)

    with test.context.phase("stop, observe detach and delete by logical volume ID"):
        stopped = test.request(
            "POST", sandbox_path + ":stop", idempotency_key=test.key("stop"),
        ).require(202)
        test.wait_operation(required_string(stopped, "operation_id"), timeout=600)
        test.wait_sandbox(sandbox, "stopped", timeout=600)
        deadline = time.monotonic() + 180
        while True:
            resource = test.request("GET", path).require(200)
            if resource["state"] == "available":
                break
            test.context.verify(time.monotonic() < deadline, "stopped volume did not detach")
            time.sleep(0.5)
        key = test.key("delete-volume")
        accepted = test.request("DELETE", path, idempotency_key=key).require(202)
        operation = required_string(accepted, "id")
        test.wait_operation(operation, timeout=600)
        replay = test.request("DELETE", path, idempotency_key=key).require(202)
        test.context.verify(
            replay["id"] == operation, "completed volume deletion did not replay",
        )
        tombstone = test.request("GET", path).require(200)
        test.context.verify(
            tombstone["state"] == "deleted" and tombstone["delete_requested"] is True,
            "volume deletion completed without a terminal required-volume tombstone",
        )

    with test.context.phase("a deleted required disk cannot become a root-only sandbox"):
        wake = test.request(
            "POST", sandbox_path + ":wake", idempotency_key=test.key("wake-deleted"),
        )
        if wake.status == 202:
            terminal = test.wait_operation(
                required_string(wake.body, "operation_id"), timeout=600, require_success=False,
            )
            test.context.verify(terminal["state"] == "failed", "missing required disk allowed wake")
        else:
            test.context.verify(wake.status in {409, 412}, "unexpected missing-disk wake response")
        owner = test.request("GET", sandbox_path).require(200)
        test.context.verify(
            owner["state"] in {"stopped", "failed"},
            "deleted volume allowed activation instead of remaining stopped or failed",
        )
