"""Unit contracts for `space_smoke/environment_placement_gate.py`.

A passing Kind run exercises the happy path where the activation pod carries
the runtime-upgrade checkpoint and the target environment annotation. These
tests pin the negative predicates so a pod missing the checkpoint, missing the
operation ID, targeting the wrong environment, or targeting the wrong state
fails `expect_pod_runtime_upgrade_target`.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable, Mapping
from typing import Any

from space_smoke.capabilities import ConsoleApiResponse, SandboxPodRender
from space_smoke.environment_placement_gate import (
    CHECKPOINT_ANNOTATION,
    RUNTIME_UPGRADE_CHECKPOINT,
    RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
    RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION,
    RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION,
    EnvironmentPlacementGate,
    activation_upgrade_left_behind,
    gate_left_behind,
)
from space_smoke.runtime import InvariantViolation, ResourceScope

SANDBOX_ID = "019a0000-0000-7000-8000-000000000001"
TARGET_REF = "sha256:" + ("b" * 64)
OTHER_REF = "sha256:" + ("a" * 64)


class _Fixture:
    def __init__(self) -> None:
        self.records: dict[str, Any] = {}

    def verify(self, condition: bool, message: str, **details: Any) -> None:
        if not condition:
            raise InvariantViolation(f"{message}: {details}" if details else message)

    def record(self, key: str, value: Any) -> None:
        self.records[key] = value


class _Control:
    def __init__(self, render: SandboxPodRender | None) -> None:
        self.render = render

    def acquire_public_api_mutation(self) -> Callable[[], None]:
        return lambda: None

    def acquire_runtime_environment_policy_mutation(self) -> Callable[[], None]:
        return lambda: None

    def environment_placement_gate_env(self) -> dict[str, str | None]:
        return {}

    def set_environment_placement_gate(
        self,
        values: Mapping[str, str | None],
        *,
        api_first: bool,
        record: Mapping[str, str | None] | None = None,
    ) -> None:
        del values, api_first, record

    def clear_environment_placement_gate_record(self) -> None:
        return None

    def request_admin_api(
        self,
        method: str,
        path: str,
        *,
        json_body: Any | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ConsoleApiResponse:
        del method, path, json_body, headers
        raise AssertionError("not called")

    def clear_runtime_environment_policy_record(self) -> None:
        return None

    def sandbox_pod_render(self, sandbox_id: str) -> SandboxPodRender | None:
        assert sandbox_id == SANDBOX_ID
        return self.render

    def node_labels(self, node_name: str) -> dict[str, str]:
        del node_name
        return {}


def _pod(annotations: Mapping[str, str]) -> SandboxPodRender:
    return SandboxPodRender(
        pod_name="space-sbx-test",
        node_selector={},
        phase="Running",
        node_name="space-local-worker",
        pool_role=None,
        annotations=dict(annotations),
    )


class EnvironmentPlacementGateTests(unittest.TestCase):
    def test_standing_env_predicates_distinguish_clean_and_left_behind_values(self) -> None:
        for active_gate in ("on", "shadow", " ON ", ""):
            self.assertTrue(gate_left_behind(active_gate), repr(active_gate))
        for clean_gate in (None, "off", " OFF "):
            self.assertFalse(gate_left_behind(clean_gate), repr(clean_gate))

        for active_upgrade in ("1", "true", "yes", "on", "enabled", " TRUE "):
            self.assertTrue(activation_upgrade_left_behind(active_upgrade), active_upgrade)
        for inactive_upgrade in (None, "", "off", "false", "0", "no", "disabled"):
            self.assertFalse(activation_upgrade_left_behind(inactive_upgrade), repr(inactive_upgrade))

    def test_expect_pod_runtime_upgrade_target_accepts_complete_contract(self) -> None:
        fixture = _Fixture()
        annotations = {
            CHECKPOINT_ANNOTATION: RUNTIME_UPGRADE_CHECKPOINT,
            RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION: "019a0000-0000-7000-8000-000000000099",
            RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION: TARGET_REF,
            RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION: "started",
        }
        gate = EnvironmentPlacementGate(fixture, _Control(_pod(annotations)), ResourceScope())
        gate.expect_pod_runtime_upgrade_target(SANDBOX_ID, TARGET_REF)
        self.assertEqual(
            fixture.records[f"pod_runtime_upgrade_{SANDBOX_ID}"],
            {
                "checkpoint": "runtime-upgrade",
                "operation_id": "019a0000-0000-7000-8000-000000000099",
                "target_environment": TARGET_REF,
                "target_state": "started",
            },
        )

    def test_expect_pod_runtime_upgrade_target_rejects_incomplete_or_mismatched_annotations(self) -> None:
        valid = {
            CHECKPOINT_ANNOTATION: RUNTIME_UPGRADE_CHECKPOINT,
            RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION: "019a0000-0000-7000-8000-000000000099",
            RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION: TARGET_REF,
            RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION: "started",
        }
        bad_cases = [
            {**valid, CHECKPOINT_ANNOTATION: "cold-disk"},
            {k: v for k, v in valid.items() if k != RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION},
            {**valid, RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION: OTHER_REF},
            {**valid, RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION: "stopped"},
        ]
        for annotations in bad_cases:
            with self.subTest(annotations=annotations):
                fixture = _Fixture()
                gate = EnvironmentPlacementGate(fixture, _Control(_pod(annotations)), ResourceScope())
                with self.assertRaises(InvariantViolation):
                    gate.expect_pod_runtime_upgrade_target(SANDBOX_ID, TARGET_REF)

    def test_expect_pod_runtime_upgrade_target_rejects_missing_pod(self) -> None:
        fixture = _Fixture()
        gate = EnvironmentPlacementGate(fixture, _Control(None), ResourceScope())
        with self.assertRaises(InvariantViolation):
            gate.expect_pod_runtime_upgrade_target(SANDBOX_ID, TARGET_REF)


if __name__ == "__main__":
    unittest.main()
