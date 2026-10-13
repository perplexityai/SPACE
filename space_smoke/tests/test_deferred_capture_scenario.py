"""Contracts behind the deferred-capture scenario the bench cannot exercise.

The live run proves a deferred suspend restores cross-node. These pin the
log reader (the encode line is matched by message and sandbox id, with the
boolean field in either JSON form) and the two assertions failing closed: a
materialized capture, and a send file beside the retained bundle.
"""

from __future__ import annotations

import json
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import (
    BTRFS_SEND_ENCODED_MESSAGE,
    RETAINED_SOURCE_BUNDLE,
    ROOTFS_SEND_FILE,
    _btrfs_send_captures_from_logs,
)
from space_smoke.capabilities import BtrfsSendCapture, CompletedTransfer
from space_smoke.fixtures import SuspendCodecAssertions
from space_smoke.runtime import InvariantViolation, ResourceScope

SANDBOX_ID = "01a0ac88-c99e-735a-b8fa-f13c948332fa"


def _encode_line(sandbox_id: str, *, deferred: Any, stream_bytes: Any) -> str:
    return json.dumps(
        {
            "message": BTRFS_SEND_ENCODED_MESSAGE,
            "fields": {
                "message": BTRFS_SEND_ENCODED_MESSAGE,
                "sandbox_id": sandbox_id,
                "stream_bytes": stream_bytes,
                "deferred": deferred,
            },
        }
    )


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
        self.captures: list[BtrfsSendCapture] = []
        self.entries: tuple[str, ...] = ()
        self.transfers: list[CompletedTransfer] = []

    def btrfs_send_captures(self, sandbox_id: str, node_name: str, *, since_time: str):
        return list(self.captures)

    def sandbox_canonical_snapshot_entries(self, node_name: str, sandbox_id: str):
        return self.entries

    def completed_transfers(self, sandbox_id: str, node_name: str, *, since_time: str):
        return list(self.transfers)


def _assertions(control: _Control) -> tuple[_Fixture, SuspendCodecAssertions]:
    fixture = _Fixture()
    return fixture, SuspendCodecAssertions(cast(Any, fixture), cast(Any, control), ResourceScope())


class EncodeLineReader(unittest.TestCase):
    def test_matches_message_and_sandbox_and_reads_both_boolean_forms(self) -> None:
        logs = "\n".join(
            [
                _encode_line(SANDBOX_ID, deferred=True, stream_bytes=0),
                _encode_line("01a0ac88-0000-7000-8000-000000000000", deferred=True, stream_bytes=0),
                _encode_line(SANDBOX_ID, deferred="false", stream_bytes="112764663"),
                "not json",
            ]
        )
        captures = _btrfs_send_captures_from_logs(logs, SANDBOX_ID, "worker-a")
        self.assertEqual(
            captures,
            [
                BtrfsSendCapture("worker-a", SANDBOX_ID, 0, True),
                BtrfsSendCapture("worker-a", SANDBOX_ID, 112764663, False),
            ],
        )


class DeferredCaptureAssertions(unittest.TestCase):
    def test_a_materialized_capture_fails_closed(self) -> None:
        control = _Control()
        control.captures = [BtrfsSendCapture("worker-a", SANDBOX_ID, 4096, False)]
        _, assertions = _assertions(control)
        with self.assertRaises(InvariantViolation):
            assertions.expect_deferred_capture("worker-a", since_time="t", visibility_timeout=0)

    def test_no_capture_fails_closed(self) -> None:
        _, assertions = _assertions(_Control())
        with self.assertRaises(InvariantViolation):
            assertions.expect_deferred_capture("worker-a", since_time="t", visibility_timeout=0)

    def test_a_deferred_capture_is_recorded(self) -> None:
        control = _Control()
        control.captures = [BtrfsSendCapture("worker-a", SANDBOX_ID, 0, True)]
        fixture, assertions = _assertions(control)
        capture = assertions.expect_deferred_capture("worker-a", since_time="t", visibility_timeout=0)
        self.assertTrue(capture.deferred)
        # The scenario's result is JSON; evidence must serialize.
        self.assertEqual(
            json.loads(json.dumps(fixture.records["deferred_capture"])),
            {"node_name": "worker-a", "sandbox_id": SANDBOX_ID, "stream_bytes": 0, "deferred": True},
        )

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
