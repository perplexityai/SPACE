"""Verdicts behind the encrypted restore -> re-suspend Kind scenario."""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.capabilities import (
    EncryptedSnapshotObservation,
    SandboxPodPlacement,
)
from space_smoke.fixtures import EncryptedResuspendAssertions
from space_smoke.runtime import (
    InvariantViolation,
)

SANDBOX_ID = "01a0cf42-ed75-719b-8439-7a8651e38fc3"


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SANDBOX_ID)
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, key: str, value: object) -> None:
        self.records[key] = value


class _Control:
    def __init__(self, observation: EncryptedSnapshotObservation) -> None:
        self.observation = observation

    def sandbox_pod_placement(self, sandbox_id: str) -> SandboxPodPlacement:
        return SandboxPodPlacement(sandbox_id, "pod-a", "uid-a", "worker-a", "1")

    def restore_portable(self, sandbox_id: str) -> bool:
        return True

    def encrypted_snapshot_observation(
        self, sandbox_id: str, node_name: str
    ) -> EncryptedSnapshotObservation:
        return self.observation


def _observation(
    *,
    luks_uuid: str = "12345678-1234-1234-1234-123456789abc",
    allocated: int = 32 * 1024**2,
) -> EncryptedSnapshotObservation:
    return EncryptedSnapshotObservation(
        node_name="worker-a",
        luks_uuid=luks_uuid,
        logical_bytes=16 * 1024**3,
        allocated_bytes=allocated,
    )


def _assertions(
    observation: EncryptedSnapshotObservation,
) -> tuple[_Fixture, EncryptedResuspendAssertions]:
    fixture = _Fixture()
    return fixture, EncryptedResuspendAssertions(
        cast(Any, fixture), cast(Any, _Control(observation))
    )


class EncryptedSnapshotVerdicts(unittest.TestCase):
    def test_sparse_diff_allocation_passes_and_full_sized_allocation_fails(self) -> None:
        _, assertions = _assertions(_observation())
        self.assertEqual(assertions.sealed_snapshot("worker-a"), _observation())

        _, assertions = _assertions(
            _observation(allocated=EncryptedResuspendAssertions.MAX_DIFF_ALLOCATION_BYTES)
        )
        with self.assertRaisesRegex(InvariantViolation, "sparse Diff/CoW"):
            assertions.sealed_snapshot("worker-a")

    def test_reformatting_the_luks_base_fails(self) -> None:
        _, assertions = _assertions(_observation())
        with self.assertRaisesRegex(InvariantViolation, "reformatted"):
            assertions.expect_base_reused(
                _observation(),
                _observation(luks_uuid="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            )

if __name__ == "__main__":
    unittest.main()
