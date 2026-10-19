"""Contracts behind the deferred-capture scenario the bench cannot exercise.

The live run proves a deferred suspend restores cross-node. These pin the
retained-source assertion failing closed: a send file beside the retained
bundle, and a missing bundle.
"""

from __future__ import annotations

import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import RETAINED_SOURCE_BUNDLE, ROOTFS_SEND_FILE
from space_smoke.fixtures import SuspendCodecAssertions
from space_smoke.runtime import InvariantViolation, ResourceScope

SANDBOX_ID = "01a0ac88-c99e-735a-b8fa-f13c948332fa"


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SANDBOX_ID)
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, key: str, value: object) -> None:
        self.records[key] = value

    def phase(self, _name: str):
        return nullcontext()


class _Control:
    def __init__(self) -> None:
        self.entries: tuple[str, ...] = ()

    def sandbox_canonical_snapshot_entries(self, node_name: str, sandbox_id: str):
        return self.entries


def _assertions(control: _Control) -> tuple[_Fixture, SuspendCodecAssertions]:
    fixture = _Fixture()
    return fixture, SuspendCodecAssertions(cast(Any, fixture), cast(Any, control), ResourceScope())


class DeferredCaptureAssertions(unittest.TestCase):
    def test_send_file_beside_the_bundle_fails_closed(self) -> None:
        control = _Control()
        control.entries = ("memory", "vmstate", "snapshot.json", RETAINED_SOURCE_BUNDLE, ROOTFS_SEND_FILE)
        _, assertions = _assertions(control)
        with self.assertRaises(InvariantViolation):
            assertions.expect_retained_source_without_send_file("worker-a")

    def test_missing_bundle_fails_closed(self) -> None:
        control = _Control()
        control.entries = ("memory", "vmstate", "snapshot.json")
        _, assertions = _assertions(control)
        with self.assertRaises(InvariantViolation):
            assertions.expect_retained_source_without_send_file("worker-a")

    def test_bundle_without_send_file_passes(self) -> None:
        control = _Control()
        control.entries = ("memory", "vmstate", "snapshot.json", RETAINED_SOURCE_BUNDLE)
        _, assertions = _assertions(control)
        self.assertIn(RETAINED_SOURCE_BUNDLE, assertions.expect_retained_source_without_send_file("worker-a"))


if __name__ == "__main__":
    unittest.main()
