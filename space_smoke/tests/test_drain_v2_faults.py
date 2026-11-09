"""Contracts of the v2 drain fault scenarios' harness the bench cannot exercise.

A passing bench shows a fault applied and lifted. It cannot show that the
node's own report is read in full, that a drain-restore env value off the
list is refused before any write, or that the unblock removes exactly the
rules the block added and nothing else.
"""

from __future__ import annotations

import json
import unittest

from space_smoke.adapters import (
    NODE_TRAFFIC_NODE_API,
    NODE_TRAFFIC_OBJECT_STORE,
    KindConfig,
    KindHarness,
    parse_drain_node_report,
)
from space_smoke.drain_restore import (
    DRAIN_RESTORE_HOLD_ENV,
    DRAIN_RESTORE_SLOTS_ENV,
    validate_drain_restore_hold_overrides,
)
from space_smoke.runtime import CommandResult

NODE = "space-local-worker"
OBJECT_STORE_PODS = json.dumps({"items": [{"status": {"podIP": "10.244.1.7"}}]})


class _Executor:
    """Answers kubectl's object-store pod listing, and `iptables -C` from
    the rules it holds; records every command."""

    def __init__(self, present: set[tuple[str, ...]] | None = None) -> None:
        self.present = set(present or ())
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        command = tuple(command)
        self.calls.append(command)
        if command[0] == "kubectl":
            return CommandResult(command, 0, OBJECT_STORE_PODS, "")
        verb, rule = command[4], command[5:]
        if verb == "-C":
            return CommandResult(command, 0 if rule in self.present else 1, "", "")
        if verb == "-I":
            self.present.add(rule)
        if verb == "-D":
            self.present.discard(rule)
        return CommandResult(command, 0, "", "")


def _harness(executor: _Executor) -> KindHarness:
    return KindHarness(executor, KindConfig(), environment={})


class NodeDrainReport(unittest.TestCase):
    def test_every_line_of_the_nodes_answer_is_read(self) -> None:
        report = parse_drain_node_report(
            "sealed_epoch=7\n"
            "done=false\n"
            "evacuated=a started 01a\n"
            "evacuated=b stopped 01b\n"
            "pending=c upload\n"
            "blocked=d upload failed 3 times: reset\n"
            "blocked=- orphan inventory didn't finish within 20s\n"
        )

        self.assertEqual(
            report,
            {
                "sealed_epoch": 7,
                "done": False,
                "evacuated": [
                    {"sandbox_id": "a", "prior": "started", "artifact_id": "01a"},
                    {"sandbox_id": "b", "prior": "stopped", "artifact_id": "01b"},
                ],
                "pending": [{"sandbox_id": "c", "action": "upload"}],
                "blocked": [
                    {"sandbox_id": "d", "reason": "upload failed 3 times: reset"},
                    {"sandbox_id": None, "reason": "orphan inventory didn't finish within 20s"},
                ],
            },
        )

    def test_a_done_unsealed_answer_reads_as_such(self) -> None:
        report = parse_drain_node_report("sealed_epoch=none\ndone=true\n")

        self.assertEqual((report["sealed_epoch"], report["done"]), (None, True))

    def test_an_answer_without_its_epoch_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            parse_drain_node_report("done=true\n")


class DrainRestoreEnv(unittest.TestCase):
    def test_the_hold_and_the_slots_take_their_own_bounds(self) -> None:
        validate_drain_restore_hold_overrides(
            {DRAIN_RESTORE_HOLD_ENV: "120", DRAIN_RESTORE_SLOTS_ENV: "16"}
        )
        for overrides in (
            {DRAIN_RESTORE_HOLD_ENV: "121"},
            {DRAIN_RESTORE_SLOTS_ENV: "17"},
            {DRAIN_RESTORE_SLOTS_ENV: "0"},
            {DRAIN_RESTORE_SLOTS_ENV: "ten"},
            {"SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY": "4"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                validate_drain_restore_hold_overrides(overrides)


class NodeTrafficFaults(unittest.TestCase):
    def test_the_object_store_block_resets_the_nodes_pods_and_host_alike(self) -> None:
        executor = _Executor()

        _harness(executor).block_node_traffic(NODE, NODE_TRAFFIC_OBJECT_STORE)

        self.assertEqual(
            sorted(rule[0] for rule in executor.present), ["FORWARD", "OUTPUT"]
        )
        for rule in executor.present:
            self.assertEqual(rule[1:3], ("-d", "10.244.1.7"))
            self.assertIn("space-smoke-object-store", rule)
            self.assertEqual(rule[-3:], ("REJECT", "--reject-with", "tcp-reset"))
        self.assertTrue(
            all(call[:3] == ("docker", "exec", NODE) for call in executor.calls if call[0] == "docker")
        )

    def test_a_block_already_in_place_is_not_added_again(self) -> None:
        executor = _Executor()
        harness = _harness(executor)
        harness.block_node_traffic(NODE, NODE_TRAFFIC_NODE_API)
        harness.block_node_traffic(NODE, NODE_TRAFFIC_NODE_API)

        inserts = [call for call in executor.calls if call[3:5] == ("iptables", "-I")]
        self.assertEqual(len(inserts), 1)
        self.assertEqual(
            inserts[0][5:],
            ("INPUT", "-p", "tcp", "--dport", "9800", "-m", "comment", "--comment",
             "space-smoke-node-api", "-j", "DROP"),
        )

    def test_the_unblock_removes_only_its_own_rules(self) -> None:
        other = ("INPUT", "-p", "tcp", "--dport", "22", "-j", "DROP")
        executor = _Executor({other})
        harness = _harness(executor)
        harness.block_node_traffic(NODE, NODE_TRAFFIC_NODE_API)
        harness.block_node_traffic(NODE, NODE_TRAFFIC_OBJECT_STORE)

        harness.unblock_node_traffic(NODE, NODE_TRAFFIC_NODE_API)

        self.assertIn(other, executor.present)
        self.assertEqual(
            sorted(rule[0] for rule in executor.present), ["FORWARD", "INPUT", "OUTPUT"]
        )
        self.assertFalse(any("space-smoke-node-api" in rule for rule in executor.present))

    def test_an_unknown_path_is_refused_before_any_command(self) -> None:
        executor = _Executor()

        with self.assertRaises(ValueError):
            _harness(executor).block_node_traffic(NODE, "postgres")
        self.assertEqual(executor.calls, [])


if __name__ == "__main__":
    unittest.main()
