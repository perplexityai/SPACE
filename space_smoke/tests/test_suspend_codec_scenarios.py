"""Contracts of the btrfs-send codec scenarios the bench cannot exercise.

The live runs prove the codec's failure matrix; nothing here restates that.
These pin what a passing run would not catch: the tracing-line reader
picking up a `trigger` recorded on an enclosing span (the pull's tag lives
there, not on the event), the received-UUID mirror of the Rust derivation's
input shape, the shell-word and SQL-literal guards on values that reach an
on-node shell or a `psql` statement, and the on-node probes failing closed.
"""

from __future__ import annotations

import json
import unittest
from typing import Any, cast
from unittest import mock

from space_smoke.adapters import (
    TEMPLATE_RECEIVED_UUID_NAMESPACE,
    KindConfig,
    KindHarness,
    _object_name,
    _sql_timestamp,
    _storage_uri,
    _tracing_fields,
    template_received_uuid,
)
from space_smoke.fixtures import SuspendCodecAssertions
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope


class TracingFieldMerge(unittest.TestCase):
    def test_span_fields_are_visible_and_event_fields_win(self) -> None:
        line = json.dumps(
            {
                "message": "vol-mgr pull plan resolved from manifest",
                "spans": [{"name": "pull", "template": "seed", "trigger": "on_demand"}],
                "fields": {"message": "vol-mgr pull plan resolved from manifest", "size": 1},
            }
        )
        fields = _tracing_fields(line)
        assert fields is not None
        self.assertEqual(fields["trigger"], "on_demand")
        self.assertEqual(fields["template"], "seed")
        self.assertEqual(fields["size"], 1)
        self.assertNotIn("name", fields)
        self.assertIsNone(_tracing_fields("not json"))


class ReceivedUuidMirror(unittest.TestCase):
    def test_matches_the_vector_the_rust_side_pins(self) -> None:
        # `crates/libs/btrfs/src/tests.rs` pins this pair; a
        # drift in namespace, separator or hashing on the Python side shows
        # up here, and nowhere else.
        self.assertEqual(
            template_received_uuid(
                "seed", "sha256:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1"
            ),
            "7c824a00-83ad-518d-a954-053fab95bc5c",
        )
        self.assertEqual(str(TEMPLATE_RECEIVED_UUID_NAMESPACE), "1f3a9c44-5e6b-47d8-b9c1-2a0f8e517d36")


class LiteralGuards(unittest.TestCase):
    def test_timestamp_literal_fails_closed(self) -> None:
        self.assertEqual(_sql_timestamp("2026-09-18T20:50:41.032097Z"), "2026-09-18T20:50:41.032097Z")
        for bad in ("2026-09-18 20:50:41", "now()", "2026-09-18T20:50:41Z'; --"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                _sql_timestamp(bad)

    def test_storage_uri_and_object_name_fail_closed(self) -> None:
        good = "s3://space-templates/seed/sha256:abc?region=us-east-1"
        self.assertEqual(_storage_uri(good), good)
        for bad in ("", "s3://x/'; DROP TABLE t; --", "s3://bucket/key with space", "s3://a\nb"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                _storage_uri(bad)
        self.assertEqual(_object_name("rootfs.btrfs-send"), "rootfs.btrfs-send")
        for bad in ("../rootfs", "a/b", "", ".hidden"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                _object_name(bad)


class _ExecKubectl:
    """Scripted execs; the operator-shell pod lookup each one starts with is answered."""

    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        argv = tuple(command)
        self.calls.append(argv)
        if "get" in argv and "pods" in argv:
            return CommandResult(argv, 0, "space-node-debug-x", "")
        return self.results.pop(0)


class TemplateRemovalRefusal(unittest.TestCase):
    def test_in_use_refusal_yields_ids_and_a_nameless_one_raises(self) -> None:
        argv = ("kubectl",)
        sid = "01a0b647-9b69-70d6-9de4-4e40e9dcc3cd"
        refusal = (
            "Error: code: 'The system is not in a state required for the operation's "
            f"execution', message: \"cannot delete template seed: 1 sandbox(es) were created "
            f"from 1 build(s): <any>: 1 sandbox(es) ({sid})\""
        )
        kubectl = _ExecKubectl(
            [
                CommandResult(argv, 1, "", refusal),
                CommandResult(argv, 0, "template 'seed' deleted\n", ""),
                CommandResult(argv, 1, "", "cannot delete template seed: 2 sandbox(es) were created from 1 build(s)"),
                CommandResult(argv, 1, "", "some other failure"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        self.assertEqual(harness.evict_template_from_worker("p", "seed"), [sid])
        self.assertEqual(harness.evict_template_from_worker("p", "seed"), [])
        with self.assertRaisesRegex(RuntimeError, "named none"):
            harness.evict_template_from_worker("p", "seed")
        with self.assertRaisesRegex(RuntimeError, "some other failure"):
            harness.evict_template_from_worker("p", "seed")


class TemplateRemovalAlreadyAbsent(unittest.TestCase):
    def test_not_found_means_already_removed(self) -> None:
        # The error the bench returned when one btrfs-send scenario removed the
        # seed template from a worker and the next one tried again before
        # prefetch re-pulled it.
        argv = ("kubectl",)
        absent = (
            "Error: code: 'Some requested entity was not found', "
            'message: "template not found: seed"'
        )
        kubectl = _ExecKubectl(
            [
                CommandResult(argv, 1, "", absent),
                CommandResult(argv, 1, "", "some other failure"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        self.assertEqual(harness.evict_template_from_worker("p", "seed"), [])
        with self.assertRaisesRegex(RuntimeError, "some other failure"):
            harness.evict_template_from_worker("p", "seed")


class InstanceProbesFailClosed(unittest.TestCase):
    def test_entries_filter_and_exec_failure(self) -> None:
        argv = ("kubectl",)
        kubectl = _ExecKubectl(
            [
                CommandResult(argv, 0, "instance\ninstance.ro\nsnapshots\n", ""),
                CommandResult(argv, 0, "", ""),
                CommandResult(argv, 1, "", "error: unable to upgrade connection"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        sid = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
        self.assertEqual(harness.sandbox_instance_entries("p", sid), ("instance", "instance.ro"))
        self.assertEqual(harness.sandbox_instance_entries("p", sid), ())
        with self.assertRaises(RuntimeError):
            harness.sandbox_instance_entries("p", sid)

    def test_subvolume_probe_distinguishes_missing_from_plain(self) -> None:
        argv = ("kubectl",)
        kubectl = _ExecKubectl(
            [
                CommandResult(argv, 0, "yes\n", ""),
                CommandResult(argv, 0, "no\n", ""),
                CommandResult(argv, 4, "", ""),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        sid = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
        self.assertTrue(harness.sandbox_instance_is_subvolume("p", sid))
        self.assertFalse(harness.sandbox_instance_is_subvolume("p", sid))
        with self.assertRaises(RuntimeError):
            harness.sandbox_instance_is_subvolume("p", sid)


if __name__ == "__main__":
    unittest.main()


class _EvictionFixture:
    def __init__(self) -> None:
        self.records: dict[str, object] = {}
        self.waited: list[float] = []

    def sandbox_template_name(self) -> str:
        return "seed"

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, key: str, value: object) -> None:
        self.records[key] = value

    def wait(self, seconds: float) -> None:
        self.waited.append(seconds)


class _RacingPrefetchControl:
    """A worker whose prefetch lands the digest directory after the first deletes."""

    def __init__(self, landings_after_delete: int) -> None:
        self.landings_after_delete = landings_after_delete
        self.deletes = 0

    def evict_template_from_worker(self, node_name: str, template: str) -> list[str]:
        self.deletes += 1
        return []

    def template_digest_dirs(self, node_name: str, template: str) -> tuple[str, ...]:
        return ("sha256:abc",) if self.deletes <= self.landings_after_delete else ()


class TemplateEvictionSettles(unittest.TestCase):
    def _evict(self, control: _RacingPrefetchControl) -> _EvictionFixture:
        fixture = _EvictionFixture()
        assertions = SuspendCodecAssertions(
            cast(Any, fixture), cast(Any, control), ResourceScope()
        )
        assertions.evict_template("worker-b", "seed")
        return fixture

    def test_a_prefetch_pull_landing_after_the_delete_is_removed_again(self) -> None:
        control = _RacingPrefetchControl(landings_after_delete=1)
        fixture = self._evict(control)
        self.assertEqual(control.deletes, 2)
        self.assertEqual(fixture.records["template_eviction_rounds_worker-b"], 2)

    def test_a_template_that_never_settles_fails_closed(self) -> None:
        control = _RacingPrefetchControl(landings_after_delete=10**6)
        with mock.patch("space_smoke.fixtures.TEMPLATE_EVICTION_SETTLE_SECONDS", 0.0):
            with self.assertRaisesRegex(InvariantViolation, "still has digest directories"):
                self._evict(control)
        self.assertEqual(control.deletes, 1)
