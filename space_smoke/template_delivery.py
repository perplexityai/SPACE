"""Typed service-level template delivery evidence on an explicitly configured bench.

The caller owns host inventory, experiment configuration and deployment. This
fixture only invokes its named, task-scoped operations; stdout is a versioned
JSON result, while service logs are supplemental evidence.
"""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from space_smoke.runtime import ScenarioContext, SmokeConfigurationError

@dataclass(frozen=True)
class DeliveryBench:
    config_path: Path

    @classmethod
    def from_environment(cls, values: dict[str, str]) -> DeliveryBench:
        value = values.get("SPACE_SMOKE_TEMPLATE_DELIVERY_CONFIG")
        if not value:
            raise SmokeConfigurationError("template delivery requires an explicit bench configuration")
        path = Path(value)
        if not path.is_absolute():
            raise SmokeConfigurationError("template delivery configuration must be absolute")
        data = json.loads(path.read_text())
        if data.get("schema") != 1 or not data.get("task_prefix", "").endswith("-"):
            raise SmokeConfigurationError("invalid template delivery bench configuration")
        return cls(path)

class TemplateDeliveryFixture:
    fixture_name: ClassVar[str] = "template-delivery"
    timeout_minutes: ClassVar[int] = 45

    def __init__(self, context: ScenarioContext, bench: DeliveryBench) -> None:
        self.context = context
        self.config = json.loads(bench.config_path.read_text())
        self.evidence: list[dict[str, object]] = []
        context.record("template_delivery_operations", self.evidence)

    def operation(self, name: str, **arguments: object) -> dict[str, object]:
        allowed = {"preflight", "publish", "pull", "delete_build", "set_mode", "compare", "immutable", "shared_extents", "boot_clone"}
        if name not in allowed:
            raise ValueError("unsupported template delivery operation")
        command = self.config["driver"]
        if not isinstance(command, list) or not all(isinstance(part, str) for part in command):
            raise SmokeConfigurationError("bench driver must be a structured argv")
        request = {"schema": 1, "operation": name, "arguments": arguments}
        result = subprocess.run(command, input=json.dumps(request), text=True, capture_output=True, timeout=900)
        if result.returncode:
            raise RuntimeError(f"template delivery {name} failed: {result.stderr[-4000:]}")
        reply = json.loads(result.stdout)
        if reply.get("schema") != 1:
            raise RuntimeError("invalid template delivery response")
        self.evidence.append({"operation": name, **reply})
        return reply

    def prove_native_gate(self) -> None:
        context = self.context
        target = self.config["task_prefix"] + "native-gate"
        with context.phase("verify approved Admin API access"):
            authenticated = self.operation("preflight")
            context.verify(authenticated.get("admin_read_authenticated") is True, "Admin API access check failed")
        with context.phase("publish two real builder artifacts"):
            older = self.operation("publish", template=target, revision=1)
            newer = self.operation("publish", template=target, revision=2, base_digest=older["digest"])
            context.verify(older["digest"] != newer["digest"], "builder releases must be distinct")
        with context.phase("flat service pull and exact-byte comparison"):
            self.operation("set_mode", mode="flat", chunk_bytes=4194304)
            self.operation("pull", template=target, digest=newer["digest"])
            exact = self.operation("compare", template=target, digest=newer["digest"], label="flat")
            context.verify(exact["equal"] is True, "flat bytes differ from builder original")
            immutable = self.operation("immutable", template=target, digest=newer["digest"])
            context.verify(immutable["write_rejected"] and immutable["truncate_rejected"], "flat cache accepted mutation")
            self.operation("delete_build", template=target, digest=newer["digest"])
        with context.phase("chunk service pulls and cache reuse"):
            self.operation("set_mode", mode="chunks", chunk_bytes=4194304)
            self.operation("pull", template=target, digest=older["digest"])
            exact = self.operation("compare", template=target, digest=older["digest"], label="older")
            context.verify(exact["equal"] is True, "older cached bytes differ from builder original")
            self.operation("pull", template=target, digest=newer["digest"])
            exact = self.operation("compare", template=target, digest=newer["digest"], label="chunks")
            context.verify(exact["equal"] is True, "chunk bytes differ from builder original")
            sharing = self.operation("shared_extents", template=target, older=older["digest"], newer=newer["digest"])
            context.verify(sharing["shared_physical_bytes"] > 0, "completed images share no cached physical extents")
            immutable = self.operation("immutable", template=target, digest=newer["digest"])
            context.verify(immutable["write_rejected"] and immutable["truncate_rejected"], "chunk cache accepted mutation")
        with context.phase("boot a writable clone and preserve cached parent"):
            boot = self.operation("boot_clone", template=target, digest=newer["digest"])
            context.verify(boot["ready"] and boot["guest_marker_verified"] and boot["parent_unchanged"], "sandbox boot or parent isolation failed")
        context.record("gate_artifacts_retained_for_matrix", {"template": target, "older": older["digest"], "newer": newer["digest"]})
