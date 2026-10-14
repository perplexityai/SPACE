"""Receipt-owned pool configuration for isolated Kind create-path proofs."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from space_smoke.capabilities import (
    ColdCreateSelection,
    KindColdCreateConfig,
    KindWarmPoolConfig,
    WarmPoolSelection,
)
from space_smoke.runtime import SmokeConfigurationError


class PoolAdmin(Protocol):
    def __call__(
        self, method: str, resource: str, body: dict[str, object] | None = None
    ) -> Any: ...


def _seal(path: Path, value: object) -> None:
    """Never overwrite intent or ownership, including after a failed write."""
    with path.open("x") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o444)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class KindColdCreateSetup:
    def __init__(
        self,
        admin: PoolAdmin,
        receipts: Path,
        cluster_uid: str,
        assert_lease: Callable[[], None],
    ) -> None:
        self.admin = admin
        self.receipts = receipts
        self.cluster_uid = cluster_uid
        self.assert_lease = assert_lease

    def prepare(
        self, template: str, config: KindColdCreateConfig
    ) -> ColdCreateSelection:
        self.assert_lease()
        releases = self.admin("GET", "template-releases")
        matches = []
        for row in releases:
            name, digest = row["template_name"], row["content_hash"]
            selectors = {f"{name}@{digest}", *(f"{name}:{tag}" for tag in row["tags"])}
            if "latest" in row["tags"]:
                selectors.add(name)
            if template in selectors and row["runtime"] == "fc":
                matches.append(row)
        if len(matches) != 1:
            raise SmokeConfigurationError(
                "cold setup requires one authoritative release for the selector"
            )
        release = matches[0]
        name, digest = release["template_name"], release["content_hash"]
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise SmokeConfigurationError(
                "cold setup requires a full immutable content digest"
            )
        pin = f"{name}@{digest}"
        identity = {
            "cluster_uid": self.cluster_uid,
            "template_release_id": release["id"],
            "template_name": name,
            "content_hash": digest,
            "vcpu": config.vcpu,
            "memory_mib": config.memory_mib,
        }
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        directory = self.receipts / key
        directory.mkdir(parents=True, exist_ok=True)
        intent_path, owned_path = directory / "intent.json", directory / "owned.json"
        request = {
            "template_name": name,
            "content_hash": digest,
            "vcpu": config.vcpu,
            "memory_mib": config.memory_mib,
            # The API files the config under its own sandbox namespace
            # (k8s.sandboxNamespace) whatever this says; named to match it.
            "namespace": "sandboxes",
            "target_ready_slots": 0,
            "refill_per_tick": 1,
            "max_warming_slots": 1,
            "enabled": False,
            "encryptable": False,
        }

        def inventory() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            self.assert_lease()
            policies = [
                row
                for row in self.admin("GET", "warm-pool-policies")
                if row["template_name"] == name and row["runtime"] == "fc"
            ]
            rows = [
                row
                for row in self.admin("GET", "warm-pools")
                if row["template_release_id"] == release["id"] and row.get("encryptable") is not True
            ]
            _seal(
                directory / f"inventory-{uuid.uuid4().hex}.json",
                {"identity": identity, "configs": rows, "policies": policies},
            )
            if any(row.get("enabled") is not False for row in policies):
                raise SmokeConfigurationError(
                    "enabled or ambiguous cold-template policy"
                )
            return rows, policies

        def require_owned(rows: list[dict[str, Any]], owned: dict[str, Any]) -> None:
            if owned["identity"] != identity or len(rows) != 1:
                raise SmokeConfigurationError("unknown or changed cold pool ownership")
            row = rows[0]
            # API config responses are the authority for claim eligibility.
            # No slot count is used to infer coldness.
            expected = {
                **request,
                "template_release_id": release["id"],
                "managed_by": "manual",
            }
            if any(
                type(row.get(k)) is not type(v) or row[k] != v
                for k, v in expected.items()
            ):
                raise SmokeConfigurationError(
                    "cold pool config is not the disabled owned config"
                )
            if (
                row.get("id") != owned["config_id"]
                or row.get("created_at") != owned["created_at"]
            ):
                raise SmokeConfigurationError("cold pool config identity changed")
            if any(
                row.get(k) != 0
                for k in ("warming_slots", "ready_slots", "claimed_slots")
            ):
                raise SmokeConfigurationError(
                    "unexpected capacity on inert fixture config"
                )

        rows, policies = inventory()
        if owned_path.exists():
            owned = json.loads(owned_path.read_text())
            intent = json.loads(intent_path.read_text())
            if (
                owned["intent_sha256"]
                != hashlib.sha256(intent_path.read_bytes()).hexdigest()
                or intent["identity"] != identity
            ):
                raise SmokeConfigurationError(
                    "cold pool ownership receipt does not bind intent"
                )
            require_owned(rows, owned)
        else:
            if intent_path.exists() or rows:
                raise SmokeConfigurationError(
                    "unknown pool config or unresolved cold setup intent"
                )
            # At this point there is no config to adopt, disable, or drain.
            _seal(
                intent_path,
                {
                    "identity": identity,
                    "request": request,
                    "requested_selector": template,
                    "selected_release": release,
                    "before_configs": rows,
                    "before_policies": policies,
                },
            )
            self.assert_lease()
            try:
                created = self.admin("PUT", "warm-pools", request)
            except (SmokeConfigurationError, OSError, ValueError):
                # A matching GET cannot prove this caller performed the PUT.
                # Preserve the readback, but never retry or mint ownership.
                try:
                    observed = self.admin("GET", "warm-pools")
                except (SmokeConfigurationError, OSError, ValueError):
                    observed = "readback unavailable"
                _seal(directory / "uncertain-put.json", {"observed": observed})
                raise SmokeConfigurationError(
                    "cold config PUT uncertain; retained intent requires review"
                ) from None
            _seal(directory / "put-response.json", created)
            owned = {
                "identity": identity,
                "config_id": created["id"],
                "created_at": created["created_at"],
                "intent_sha256": hashlib.sha256(intent_path.read_bytes()).hexdigest(),
                "put_response": created,
            }
            require_owned([created], owned)
            _seal(owned_path, owned)

        def verify() -> None:
            rows, policies = inventory()
            require_owned(rows, owned)
            _seal(
                directory / f"verified-{uuid.uuid4().hex}.json",
                {"identity": identity, "configs": rows, "policies": policies},
            )

        verify()
        return ColdCreateSelection(pin, owned["config_id"], str(owned_path), verify)

    def prepare_warm(
        self, template: str, config: KindWarmPoolConfig
    ) -> WarmPoolSelection:
        cold = self.prepare(
            template,
            KindColdCreateConfig(vcpu=config.vcpu, memory_mib=config.memory_mib),
        )

        def owned_row() -> dict[str, Any]:
            self.assert_lease()
            rows = [
                row
                for row in self.admin("GET", "warm-pools")
                if row["id"] == cold.config_id
            ]
            if len(rows) != 1:
                raise SmokeConfigurationError(
                    "owned warm-pool config disappeared or became ambiguous"
                )
            return rows[0]

        row = owned_row()
        common_request = {
            "template_name": row["template_name"],
            "content_hash": row["content_hash"],
            "vcpu": row["vcpu"],
            "memory_mib": row["memory_mib"],
            "namespace": row["namespace"],
            "refill_per_tick": 1,
            "max_warming_slots": 1,
            "encryptable": row["encryptable"],
            **({"disk_mib": row["disk_mib"]} if row.get("disk_mib") else {}),
        }
        enabled_request = {
            **common_request,
            "target_ready_slots": 1,
            "enabled": True,
        }
        disabled_request = {
            **common_request,
            "target_ready_slots": 0,
            "enabled": False,
        }

        def restore() -> None:
            self.assert_lease()
            restored = self.admin("PUT", "warm-pools", disabled_request)
            if restored.get("id") != cold.config_id:
                raise SmokeConfigurationError(
                    "warm-pool restore changed the owned config identity"
                )
            deadline = time.monotonic() + 90
            while True:
                current = owned_row()
                remaining_slots = sum(
                    current.get(key, 0)
                    for key in ("warming_slots", "ready_slots", "claimed_slots")
                )
                if (
                    current.get("enabled") is False
                    and current.get("target_ready_slots") == 0
                    and remaining_slots == 0
                ):
                    return
                if time.monotonic() >= deadline:
                    raise SmokeConfigurationError(
                        "disabled warm-pool config retained capacity after cleanup"
                    )
                time.sleep(1)

        def verify() -> None:
            current = owned_row()
            if (
                current.get("enabled") is not True
                or current.get("target_ready_slots") != 1
                or current.get("ready_slots", 0) < 1
            ):
                raise SmokeConfigurationError(
                    "owned warm-pool config no longer has ready capacity"
                )

        try:
            self.assert_lease()
            activated = self.admin("PUT", "warm-pools", enabled_request)
            if activated.get("id") != cold.config_id:
                raise SmokeConfigurationError(
                    "warm-pool activation changed the owned config identity"
                )
            deadline = time.monotonic() + 300
            while True:
                current = owned_row()
                if current.get("ready_slots", 0) >= 1:
                    verify()
                    break
                if time.monotonic() >= deadline:
                    raise SmokeConfigurationError(
                        "owned warm-pool config did not produce a ready slot"
                    )
                time.sleep(2)
        except Exception as error:
            try:
                restore()
            except Exception as cleanup_error:
                error.add_note(f"warm-pool config restore failed: {cleanup_error}")
            raise

        return WarmPoolSelection(
            cold.template,
            cold.config_id,
            cold.ownership_receipt,
            verify,
            restore,
        )
