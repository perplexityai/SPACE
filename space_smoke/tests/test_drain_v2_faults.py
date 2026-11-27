"""Contracts of the v2 drain scenarios' harness the bench cannot exercise.

A passing bench shows a fault applied and lifted and a drain judged. It
cannot show that a controller env value off the list is refused before any
write, that the unblock removes exactly the rules the block added, or that
the harness reads the drain's admin view, the stall, the Rollback refusal
and the gate's answers the way the contract block in `space_smoke.drain_v2`
says, refusing what it rules out.
"""

from __future__ import annotations

import io
import json
import time
import unittest
from typing import Any
from unittest import mock
from urllib.error import HTTPError

from space_smoke import drain_v2
from space_smoke.adapters import (
    NODE_TRAFFIC_NODE_API,
    NODE_TRAFFIC_OBJECT_STORE,
    KindConfig,
    KindHarness,
    _call_answer,
    _preview_busy_retry_after,
)
from space_smoke.capabilities import CallAnswer, DrainV2Status, NodeCliOutcome, SandboxResidency
from space_smoke.drain_v2 import (
    CALL_PASSED_STATUS,
    DRAIN_V2_ENABLED_ENV,
    DRAIN_V2_MAX_EVACUATING_ENV,
    EVACUATED_KIND_FIELD,
    GATE_CONFLICT_TEXT,
    GATE_REFUSAL_STATUS,
    GATE_RETRY_TEXT,
    STOP_REFUSAL_TEXT,
    STOPPED_REFUSAL_CODE,
    DrainV2Assertions,
    artifact_kind,
    stop_refused_by_the_drain,
    timestamp_seconds,
    hook_seconds,
    require_drain_v2,
    validate_drain_v2_api_overrides,
    validate_drain_v2_overrides,
)
from space_smoke.runtime import CommandResult, InconclusiveSmoke, InvariantViolation, ResourceScope

NODE = "space-local-worker"
NODE_ID = "00000000-0000-4000-8000-000000000001"
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


class _Fixture:
    """What `DrainV2Assertions` reads of its fixture: verdicts raise, waits
    don't."""

    def __init__(self) -> None:
        self.evidence: dict[str, Any] = {}

    def verify(self, condition: bool, message: str, **evidence: Any) -> None:
        self.evidence.update(evidence)
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: Any) -> None:
        self.evidence[name] = value

    def wait(self, _seconds: float, **_options: Any) -> None:
        pass


class _Control:
    """The reads the judging helpers make, from fixed answers."""

    def __init__(
        self,
        residencies: dict[str, list[SandboxResidency]] | None = None,
        *,
        view: dict[str, Any] | None = None,
        status: dict[str, Any] | None = None,
        last_error: str | None = None,
        rollback: NodeCliOutcome | None = None,
        node_states: dict[str, list[str]] | None = None,
        switches: dict[str, str | None] | None = None,
        hooks: dict[str, str | None] | None = None,
    ) -> None:
        self.node_states = node_states or {}
        self.switches = {"api": "true", "controller": "true"} if switches is None else switches
        self.hooks = hooks or {}
        self.gate_reads = 0
        self.residencies = residencies or {}
        self.view = view
        self.status = status
        self.error = last_error
        self.rollback = rollback

    def sandbox_residency(self, sandbox_id: str) -> SandboxResidency:
        answers = self.residencies[sandbox_id]
        return answers.pop(0) if len(answers) > 1 else answers[0]

    def admin_node_drain(self, _node_id: str) -> dict[str, Any] | None:
        return self.view

    def drain_v2_status(self, _node_id: str) -> DrainV2Status | None:
        return DrainV2Status(engine="v2", status=self.status)

    def admin_nodes(self) -> list[Any]:
        return [type("Node", (), {"id": NODE_ID, "drain_last_error": self.error})()]

    def node_rollback(self, _node: str, _sandbox_id: str, _snapshot: str) -> NodeCliOutcome:
        assert self.rollback is not None
        return self.rollback

    def drain_v2_switches(self) -> dict[str, str | None]:
        self.gate_reads += 1
        return self.switches

    def drain_v2_test_hooks(self) -> dict[str, str | None]:
        return self.hooks

    def node_sandbox_state(self, _node: str, sandbox_id: str) -> str:
        answers = self.node_states.get(sandbox_id, ["started"])
        return answers.pop(0) if len(answers) > 1 else answers[0]


class _Sandbox:
    def __init__(self, sandbox_id: str, statuses: list[int]) -> None:
        self.sandbox_id = sandbox_id
        self.statuses = statuses
        self.commands = 0

    def command_answer(self, _command: str) -> CallAnswer:
        self.commands += 1
        return CallAnswer(status=self.statuses.pop(0), retry_after=None, body={})


def _assertions(control: _Control | None = None) -> tuple[DrainV2Assertions, _Fixture]:
    fixture = _Fixture()
    drain = DrainV2Assertions(fixture, control or _Control(), lambda **_: None, None)  # type: ignore[arg-type]
    drain._node_name, drain._node_id, drain._generation = NODE, NODE_ID, 7
    return drain, fixture


class DrainV2Env(unittest.TestCase):
    def test_the_switch_the_restore_knobs_and_the_cap_are_off_the_controller_list(self) -> None:
        validate_drain_v2_overrides(drain_v2.AWS_TERMINATOR_ENV)
        validate_drain_v2_overrides(drain_v2.LEGACY_LOOPS_ENV)
        validate_drain_v2_overrides({drain_v2.STALL_BOUND_HOOK: "30"})
        for overrides in (
            {DRAIN_V2_ENABLED_ENV: "true"},
            {drain_v2.STALL_BOUND_HOOK: "5"},
            {drain_v2.STALL_BOUND_HOOK: "1000"},
            {"SPACE_DRAIN_RESTORE_TEST_HOLD_SECS": "30"},
            {"SPACE_PUBLIC_OPERATION_DISPATCHER_DRAIN_RESTORE_SLOTS": "4"},
            {DRAIN_V2_MAX_EVACUATING_ENV: "1"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                validate_drain_v2_overrides(overrides)

    def test_the_cap_is_the_only_public_api_key_and_takes_a_count(self) -> None:
        validate_drain_v2_api_overrides({DRAIN_V2_MAX_EVACUATING_ENV: "1"})
        for overrides in (
            {DRAIN_V2_MAX_EVACUATING_ENV: "0"},
            {DRAIN_V2_MAX_EVACUATING_ENV: "many"},
            {DRAIN_V2_ENABLED_ENV: "true"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                validate_drain_v2_api_overrides(overrides)

    def test_a_hold_out_of_range_is_refused_before_any_request(self) -> None:
        executor = _Executor()
        for hold in (-1, 86401):
            with self.subTest(hold=hold), self.assertRaises(ValueError):
                _harness(executor).drain_node_v2(NODE_ID, hold_seconds=hold)
        self.assertEqual(executor.calls, [])

    def test_a_rollback_to_an_odd_snapshot_name_runs_nothing(self) -> None:
        executor = _Executor()
        for name in ("", "a; rm -rf /", "-delete-all"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                _harness(executor).node_rollback(NODE, NODE_ID, name)
        self.assertEqual(executor.calls, [])


class Gate(unittest.TestCase):
    def setUp(self) -> None:
        drain_v2._GATE = None
        self.addCleanup(setattr, drain_v2, "_GATE", None)

    def test_a_batch_checks_the_switches_once_and_reads_the_bounds(self) -> None:
        control = _Control(hooks={drain_v2.STALL_BOUND_HOOK: "120", drain_v2.DISPATCH_BACKOFF_BASE_HOOK: "2"})
        first = require_drain_v2(_Fixture(), control)  # type: ignore[arg-type]
        again = require_drain_v2(_Fixture(), control)  # type: ignore[arg-type]
        self.assertIs(first, again)
        self.assertEqual(control.gate_reads, 1)
        self.assertEqual((first.stall_bound_seconds, first.dispatch_backoff_base_seconds), (120, 2))

    def test_unset_zero_or_odd_hooks_keep_the_release_bounds(self) -> None:
        for value in (None, "0", "-5", "soon"):
            with self.subTest(value=value):
                self.assertEqual(hook_seconds({drain_v2.STALL_BOUND_HOOK: value}, drain_v2.STALL_BOUND_HOOK, 900), 900)
        drain_v2._GATE = None
        gate = require_drain_v2(_Fixture(), _Control(hooks={drain_v2.DISPATCH_BACKOFF_BASE_HOOK: "600"}))  # type: ignore[arg-type]
        self.assertEqual((gate.stall_bound_seconds, gate.dispatch_backoff_base_seconds), (900, 120))

    def test_a_bench_with_either_switch_off_fails_the_gate(self) -> None:
        for switches in ({"api": "true", "controller": None}, {"api": "false", "controller": "true"}, {}):
            drain_v2._GATE = None
            with self.subTest(switches=switches), self.assertRaises(InvariantViolation):
                require_drain_v2(_Fixture(), _Control(switches=switches))  # type: ignore[arg-type]
            self.assertIsNone(drain_v2._GATE)

    def test_only_a_drain_that_needs_ec2_scopes_the_controller(self) -> None:
        control = _Control()
        control.acquire_controller_mutation = mock.Mock()  # type: ignore[attr-defined]
        drain, fixture = _assertions(control)
        with drain.v2_controller():
            pass
        control.acquire_controller_mutation.assert_not_called()  # type: ignore[attr-defined]
        self.assertIn("drain_v2_gate", fixture.evidence)
        with self.assertRaises(ValueError), drain.v2_controller(legacy_loops=True):
            pass


class ReportReads(unittest.TestCase):
    def test_a_report_timestamp_is_seconds_or_rfc3339(self) -> None:
        self.assertEqual(timestamp_seconds(1_700_000_000), 1_700_000_000.0)
        self.assertEqual(timestamp_seconds("2023-11-14T22:13:20Z"), 1_700_000_000.0)
        for odd in (None, True, "soon", {"seconds": 1}):
            with self.subTest(value=odd):
                self.assertIsNone(timestamp_seconds(odd))

    def test_an_artifact_kind_reads_in_any_of_the_proto_spellings(self) -> None:
        for value in ("full", "FULL", "ARTIFACT_KIND_FULL"):
            self.assertEqual(artifact_kind(value), drain_v2.EVACUATED_FULL)
        self.assertEqual(artifact_kind("ARTIFACT_KIND_DISK"), "disk")

    def test_evacuated_is_a_snapshot_of_each_resident_never_a_disk(self) -> None:
        started, paused = "00000000-0000-4000-8000-00000000000a", "00000000-0000-4000-8000-00000000000b"
        drain, _ = _assertions()
        report = {
            "evacuated_total": [started, paused],
            "evacuated": [
                {"sandbox_id": started, EVACUATED_KIND_FIELD: "FULL"},
                {"sandbox_id": paused, EVACUATED_KIND_FIELD: "ARTIFACT_KIND_FULL"},
            ],
        }
        drain.expect_evacuated(report, [f"sbx_{started}", paused])
        report["evacuated"][1][EVACUATED_KIND_FIELD] = "DISK"
        with self.assertRaises(InvariantViolation):
            drain.expect_evacuated(report, [paused], timeout=0)

    def test_a_released_one_counts_through_evacuated_total_alone(self) -> None:
        sandbox = "00000000-0000-4000-8000-00000000000c"
        drain, _ = _assertions()
        drain.expect_evacuated({"evacuated_total": [sandbox], "evacuated": []}, [sandbox])
        with self.assertRaises(InvariantViolation):
            drain.expect_evacuated(
                {"evacuated": [{"sandbox_id": sandbox, EVACUATED_KIND_FIELD: "full"}]}, [sandbox], timeout=0
            )

    def test_a_resident_counted_by_its_release_is_waited_for(self) -> None:
        reported, evicted = "00000000-0000-4000-8000-00000000000d", "00000000-0000-4000-8000-00000000000e"
        first = {"evacuated_total": [reported], "evacuated": [{"sandbox_id": reported, EVACUATED_KIND_FIELD: "full"}]}
        drain, fixture = _assertions(_Control(view={"evacuated_total": [reported, evicted], "evacuated": []}))
        drain.expect_evacuated(first, [reported, evicted], timeout=60)
        self.assertEqual(fixture.evidence["evacuated_total"], sorted([reported, evicted]))

    def test_a_blocked_entry_may_not_name_a_sandbox(self) -> None:
        drain, _ = _assertions()
        drain.expect_nothing_blocked_per_sandbox({"blocked": [{"reason": "disk"}]})
        drain.expect_nothing_blocked_per_sandbox({"blocked": [{"sandbox_id": "", "reason": "disk"}]})
        with self.assertRaises(InvariantViolation):
            drain.expect_nothing_blocked_per_sandbox(
                {"blocked": [{"sandbox_id": "abc", "reason": "quarantined"}]}
            )

    def test_reported_is_a_stored_report_under_this_generation(self) -> None:
        drain, _ = _assertions()
        self.assertTrue(drain.reported({"generation": 7, "reported_at": "2023-11-14T22:13:20Z"}))
        for view in ({"generation": 6, "reported_at": "2023-11-14T22:13:20Z"}, {"generation": 7, "reported_at": None}, {}):
            with self.subTest(view=view):
                self.assertFalse(drain.reported(view))

    def test_finished_is_nothing_pending_and_nothing_blocked(self) -> None:
        self.assertTrue(DrainV2Assertions.finished({"pending": [], "blocked": [], "evacuated": [{"sandbox_id": "a"}]}))
        self.assertTrue(DrainV2Assertions.finished({}))
        self.assertFalse(DrainV2Assertions.finished({"pending": [{"sandbox_id": "a", "wait": "hold"}]}))
        self.assertFalse(DrainV2Assertions.finished({"blocked": [{"reason": "disk"}]}))

    def test_a_held_sandbox_waits_hold_and_runs(self) -> None:
        sandbox = "00000000-0000-4000-8000-000000000010"
        view = {
            "generation": 7,
            "reported_at": "2023-11-14T22:13:20Z",
            "pending": [{"sandbox_id": sandbox, "wait": "hold", "running": True, "pool_member": False}],
        }
        drain, _ = _assertions(_Control({sandbox: [SandboxResidency("started", NODE_ID)]}, view=view))
        drain.expect_held(sandbox, timeout=0)
        for entry in ({"wait": "moving", "running": True}, {"wait": "hold", "running": False}):
            view["pending"] = [{"sandbox_id": sandbox, **entry}]
            with self.subTest(entry=entry), self.assertRaises(InvariantViolation):
                drain.expect_held(sandbox, timeout=0)

    def test_the_views_deadline_must_be_the_stored_one(self) -> None:
        drain, _ = _assertions()
        drain._deadline = 1_700_000_000
        self.assertEqual(drain.expect_view_deadline({"deadline": "2023-11-14T22:13:20Z"}), 1_700_000_000.0)
        with self.assertRaises(InvariantViolation):
            drain.expect_view_deadline({"deadline": "2023-11-14T22:23:20Z"})
        with self.assertRaises(InvariantViolation):
            drain.expect_view_deadline({})

    def test_an_answer_keeps_its_retry_after_and_its_body(self) -> None:
        answer = _call_answer(409, {"Retry-After": "30"}, b'{"code": "x"}')
        self.assertEqual((answer.status, answer.retry_after, answer.body), (409, "30", {"code": "x"}))
        self.assertEqual(_call_answer(502, None, b"<html>").body, {"raw": "<html>"})


class GateAnswers(unittest.TestCase):
    @staticmethod
    def _answer(*, refusals=(), progress=(), state="succeeded", code=None) -> dict[str, Any]:
        return {
            "refusals": list(refusals),
            "progress": list(progress),
            "operation_id": "op",
            "operation": {"state": state, "terminal_error_code": code},
        }

    def test_a_retry_then_success_passes(self) -> None:
        drain, _ = _assertions()
        drain.expect_retried_never_conflict(
            self._answer(progress=[f"sandbox cannot wake yet: ...; {GATE_RETRY_TEXT}"])
        )
        drain.expect_retried_never_conflict(
            self._answer(refusals=[{"status": GATE_REFUSAL_STATUS, "retry_after": "10", "body": {}}])
        )

    def test_a_409_a_bare_503_or_no_retry_fails(self) -> None:
        drain, _ = _assertions()
        for answer in (
            self._answer(progress=[f"sandbox cannot wake; {GATE_CONFLICT_TEXT}"]),
            self._answer(state="failed", code="conflict", progress=[GATE_RETRY_TEXT]),
            self._answer(refusals=[{"status": GATE_REFUSAL_STATUS, "retry_after": None, "body": {}}]),
            self._answer(refusals=[{"status": 409, "retry_after": None, "body": {}}]),
            self._answer(),
        ):
            with self.subTest(answer=answer), self.assertRaises(InvariantViolation):
                drain.expect_retried_never_conflict(answer)

    def test_only_a_budget_spent_on_the_gates_retry_is_asked_again(self) -> None:
        exhausted = {
            "state": "failed",
            "terminal_error": {"reason": drain_v2.RETRIES_EXHAUSTED_REASON},
            "progress_message": f"retry attempts exhausted after 16 attempt(s): ...; {GATE_RETRY_TEXT}",
        }
        self.assertTrue(drain_v2.retries_exhausted_on_the_gate(exhausted))
        for other in (
            {**exhausted, "state": "succeeded"},
            {**exhausted, "terminal_error": {"reason": "conflict"}},
            {**exhausted, "progress_message": f"sandbox cannot wake; {GATE_CONFLICT_TEXT}"},
            {**exhausted, "terminal_error": None},
        ):
            with self.subTest(operation=other):
                self.assertFalse(drain_v2.retries_exhausted_on_the_gate(other))

    def test_a_wait_is_told_to_retry_and_not_ended(self) -> None:
        drain, _ = _assertions()
        drain.expect_told_to_retry(self._answer(state="queued", progress=[GATE_RETRY_TEXT]))
        with self.assertRaises(InvariantViolation):
            drain.expect_told_to_retry(self._answer(state="succeeded", progress=[GATE_RETRY_TEXT]))

    def test_calls_around_the_deadline_pass_until_the_node_suspends(self) -> None:
        sandbox = "00000000-0000-4000-8000-00000000000d"
        # The node's own state: its row stays `started` with the upload cut.
        states = ["started", "started", "started", "suspending"]
        drain, fixture = _assertions(_Control(node_states={sandbox: list(states)}))
        # Inside the lead: the calls start at once, before the deadline, and
        # the node suspends at the deadline, before any call lands past it:
        # no call there judged. The last clock reading is when the node is
        # first read not running.
        drain._deadline = int(time.time()) + 3
        caller = _Sandbox(sandbox, [CALL_PASSED_STATUS, CALL_PASSED_STATUS])
        with mock.patch.object(type(drain), "seconds_to_deadline", side_effect=[3, 2, 1, 0]):
            seen = drain.expect_calls_pass_until_suspended(caller)  # type: ignore[arg-type]
        self.assertEqual(
            seen,
            {
                "before_deadline": [CALL_PASSED_STATUS],
                "past_deadline": [],
                "overlapping_suspend": [{"status": CALL_PASSED_STATUS, "retry_after": None, "body": {}, "seconds_to_deadline": 1}],
            },
        )
        self.assertEqual(caller.commands, 2)
        with self.assertRaises(InconclusiveSmoke):
            drain.expect_sampled_past_deadline(seen)
        # The same, but the node is first read not running before the
        # deadline: it took the VM down early, whatever the calls answered.
        drain, fixture = _assertions(_Control(node_states={sandbox: list(states)}))
        drain._deadline = int(time.time()) + 3
        with (
            mock.patch.object(type(drain), "seconds_to_deadline", side_effect=[3, 2, 1, 0.5]),
            self.assertRaisesRegex(InvariantViolation, "before its deadline"),
        ):
            drain.expect_calls_pass_until_suspended(
                _Sandbox(sandbox, [CALL_PASSED_STATUS, CALL_PASSED_STATUS])  # type: ignore[arg-type]
            )
        # One call before the deadline and one past it, both meeting the VM.
        drain, fixture = _assertions(_Control(node_states={sandbox: ["started"] * 4 + ["suspending"]}))
        drain._deadline = int(time.time()) + 3
        with mock.patch.object(type(drain), "seconds_to_deadline", side_effect=[3, 2, -1, -2]):
            seen = drain.expect_calls_pass_until_suspended(
                _Sandbox(sandbox, [CALL_PASSED_STATUS, CALL_PASSED_STATUS])  # type: ignore[arg-type]
            )
        self.assertEqual(
            (seen["before_deadline"], seen["past_deadline"]), ([CALL_PASSED_STATUS], [CALL_PASSED_STATUS])
        )
        drain.expect_sampled_past_deadline(seen)
        for refused in (409, GATE_REFUSAL_STATUS):
            drain, fixture = _assertions(_Control(node_states={sandbox: list(states)}))
            drain._deadline = int(time.time()) + 3
            with (
                self.subTest(refused=refused),
                mock.patch.object(type(drain), "seconds_to_deadline", side_effect=[3, 2, 1, 0]),
                self.assertRaisesRegex(InvariantViolation, "did not pass"),
            ):
                drain.expect_calls_pass_until_suspended(_Sandbox(sandbox, [refused, 201]))  # type: ignore[arg-type]
            self.assertIn(f"calls_around_deadline_{sandbox}", fixture.evidence)

    def test_before_the_deadline_a_call_must_meet_the_vm_running(self) -> None:
        sandbox = "00000000-0000-4000-8000-00000000000e"
        drain, _ = _assertions(_Control(node_states={sandbox: ["suspending"]}))
        drain._deadline = int(time.time()) + 3
        caller = _Sandbox(sandbox, [CALL_PASSED_STATUS])
        # First read not running at the deadline, so no call met the VM.
        with (
            mock.patch.object(type(drain), "seconds_to_deadline", side_effect=[3, 0]),
            self.assertRaisesRegex(InvariantViolation, "no call met the VM running"),
        ):
            drain.expect_calls_pass_until_suspended(caller)  # type: ignore[arg-type]


class StoppedSandboxes(unittest.TestCase):
    STOPPED = "00000000-0000-4000-8000-000000000020"

    @staticmethod
    def _refused(*, operation_state: str = "failed", reason: str = drain_v2.RETRIES_EXHAUSTED_REASON,
                 progress: tuple[str, ...] = ()) -> dict[str, Any]:
        return {
            "verb": "stop",
            "refusals": [],
            "progress": list(progress),
            "operation_id": "op",
            "operation": {"state": operation_state, "terminal_error": {"reason": reason}},
        }

    def test_a_stop_refused_up_front_or_retried_until_its_budget_ran_out_is_refused(self) -> None:
        message = (
            "sandbox cannot stop yet: its node is under a v2 node drain, which doesn't move stopped "
            f"sandboxes; {STOP_REFUSAL_TEXT}, or delete the sandbox"
        )
        self.assertTrue(stop_refused_by_the_drain(self._refused(progress=(message, f"retry attempts exhausted: {message}"))))
        self.assertTrue(
            stop_refused_by_the_drain(
                {
                    "operation_id": None,
                    "refusals": [{"status": GATE_REFUSAL_STATUS, "retry_after": "10", "body": {"error": message}}],
                }
            )
        )

    def test_a_stop_that_ran_failed_otherwise_or_still_runs_is_not_refused(self) -> None:
        message = f"sandbox cannot stop yet: ...; {STOP_REFUSAL_TEXT}"
        for answer in (
            self._refused(operation_state="succeeded", progress=(message,)),
            self._refused(reason="conflict", progress=(message,)),
            self._refused(progress=(f"sandbox cannot wake yet: ...; {GATE_RETRY_TEXT}",)),
            self._refused(operation_state="queued", reason="", progress=(message,)),
            {"operation_id": "op", "operation": None, "progress": [message]},
            {"operation_id": None, "refusals": [{"status": GATE_REFUSAL_STATUS, "retry_after": None, "body": {"error": message}}]},
            {"operation_id": None, "refusals": [{"status": GATE_REFUSAL_STATUS, "retry_after": "10", "body": {}}]},
            {"operation_id": None, "refusals": [{"status": 409, "retry_after": "10", "body": {"error": message}}]},
            {"operation_id": None, "refusals": []},
        ):
            with self.subTest(answer=answer):
                self.assertFalse(stop_refused_by_the_drain(answer))

    def test_an_unknown_stop_verb_asks_nothing(self) -> None:
        drain, _ = _assertions()
        for verb in ("delete", "force-delete", "suspend"):
            with self.subTest(verb=verb), self.assertRaises(ValueError):
                drain.request_stop(_Sandbox(self.STOPPED, []), verb)  # type: ignore[arg-type]

    def test_an_admin_action_off_the_list_is_refused_before_any_request(self) -> None:
        executor = _Executor()
        for action in ("delete", "start", "stop; rm"):
            with self.subTest(action=action), self.assertRaises(ValueError):
                _harness(executor).admin_sandbox_action_answer(self.STOPPED, action)
        self.assertEqual(executor.calls, [])

    def test_a_stopped_sandbox_is_pending_external_and_not_running(self) -> None:
        pending = {"sandbox_id": self.STOPPED, "wait": "external", "running": False}
        self.assertTrue(DrainV2Assertions.stopped_pending({"pending": [pending]}, f"sbx_{self.STOPPED}"))
        for entry in (
            {**pending, "wait": "hold"},
            {**pending, "wait": "moving"},
            {**pending, "running": True},
            {**pending, "sandbox_id": "00000000-0000-4000-8000-000000000021"},
        ):
            with self.subTest(entry=entry):
                self.assertFalse(DrainV2Assertions.stopped_pending({"pending": [entry]}, self.STOPPED))
        self.assertFalse(DrainV2Assertions.stopped_pending({}, self.STOPPED))

    def _refusing(
        self, answer: CallAnswer, *, engine: str | None = None, phase: str | None = None
    ) -> tuple[DrainV2Assertions, _Fixture]:
        control = _Control()
        control.drain_node_answer = lambda _id, _body: answer  # type: ignore[attr-defined]
        control.drain_v2_status = lambda _id: DrainV2Status(engine=engine, status=None)  # type: ignore[method-assign]
        control.admin_nodes = lambda: [  # type: ignore[method-assign]
            type("Node", (), {"id": NODE_ID, "drain_phase": phase, "drain_last_error": None})()
        ]
        control.kube_node_unschedulable = lambda _node: False  # type: ignore[attr-defined]
        drain, fixture = _assertions(control)
        drain._undrain = mock.Mock()  # an armed undrain, so no scope is needed
        return drain, fixture

    def test_a_drain_of_a_worker_holding_stopped_sandboxes_is_refused_naming_them(self) -> None:
        refusal = CallAnswer(
            status=409,
            retry_after=None,
            body={"code": STOPPED_REFUSAL_CODE, "stopped": 1, "stopped_sandbox_ids": [f"sbx_{self.STOPPED}"]},
        )
        drain, _ = self._refusing(refusal)
        drain.expect_drain_refused_for_stopped([self.STOPPED], hold=600)
        other = "00000000-0000-4000-8000-000000000022"
        for answer, written in (
            (CallAnswer(status=202, retry_after=None, body={}), {}),
            (CallAnswer(status=409, retry_after=None, body={**refusal.body, "code": "drain_v2_evacuation_cap"}), {}),
            (CallAnswer(status=409, retry_after=None, body={**refusal.body, "stopped": 2}), {}),
            (CallAnswer(status=409, retry_after=None, body={**refusal.body, "stopped_sandbox_ids": [other]}), {}),
            (refusal, {"engine": "v2"}),
            (refusal, {"phase": "draining"}),
        ):
            with self.subTest(answer=answer, written=written), self.assertRaises(InvariantViolation):
                drain, _ = self._refusing(answer, **written)
                drain.expect_drain_refused_for_stopped([self.STOPPED], hold=600)

    def test_residents_are_pinned_started_or_paused_only(self) -> None:
        drain, _ = _assertions()
        with self.assertRaises(ValueError):
            drain.pin_residents({"started": 1, "stopped": 1})


class NoSecondExecution(unittest.TestCase):
    SANDBOX = "00000000-0000-4000-8000-000000000011"

    def _drain(self, *, other_state: str | None, pod_node: str | None) -> DrainV2Assertions:
        control = _Control(node_states={self.SANDBOX: [other_state or "suspended"]})
        control.sandbox_pods = lambda _id: (  # type: ignore[attr-defined]
            [] if pod_node is None else [type("Pod", (), {"pod_name": "pod", "node_name": pod_node})()]
        )
        drain, _ = _assertions(control)
        drain._other = "space-local-worker2"
        return drain

    def test_nothing_off_the_drained_worker_passes(self) -> None:
        for pod_node in (None, NODE):
            self._drain(other_state=None, pod_node=pod_node).expect_no_second_execution(self.SANDBOX, watch_seconds=0)

    def test_a_vm_or_a_pod_off_the_drained_worker_fails(self) -> None:
        for other_state, pod_node in (("started", None), (None, "space-local-worker2")):
            with self.subTest(other=other_state, pod=pod_node), self.assertRaises(InvariantViolation):
                self._drain(other_state=other_state, pod_node=pod_node).expect_no_second_execution(
                    self.SANDBOX, watch_seconds=0
                )


class Deleted(unittest.TestCase):
    SANDBOX = "00000000-0000-4000-8000-000000000012"

    def _drain(self, *answers: tuple[str, str | None] | None) -> tuple[DrainV2Assertions, _Fixture]:
        reads = list(answers)
        control = _Control()
        control.sandbox_placements = lambda ids: (  # type: ignore[attr-defined]
            {} if (answer := reads.pop(0) if len(reads) > 1 else reads[0]) is None else {ids[0]: answer}
        )
        return _assertions(control)

    def test_a_deleted_or_removed_row_is_the_delete_landed(self) -> None:
        for answers, seen in (
            ((("started", NODE_ID), ("deleted", None)), "deleted"),
            ((("started", NODE_ID), None), "row gone"),
        ):
            with self.subTest(seen=seen):
                drain, fixture = self._drain(*answers)
                drain.expect_deleted(f"sbx_{self.SANDBOX}", timeout=60)
                self.assertEqual(fixture.evidence[f"deleted_{self.SANDBOX}"], seen)

    def test_a_row_that_never_leaves_deleting_fails(self) -> None:
        drain, _ = self._drain(("started", NODE_ID))
        with self.assertRaises(InvariantViolation):
            drain.expect_deleted(self.SANDBOX, timeout=0)


class StillLetGo(unittest.TestCase):
    def _drain(self, *, engine: str | None, phase: str | None, tombstoned: bool) -> DrainV2Assertions:
        control = _Control()
        control.drain_v2_status = lambda _id: DrainV2Status(engine=engine, status=None)  # type: ignore[method-assign]
        control.admin_nodes = lambda: [  # type: ignore[method-assign]
            type("Node", (), {"id": NODE_ID, "drain_phase": phase, "drain_last_error": None})()
        ]
        control.node_tombstoned = lambda _id: tombstoned  # type: ignore[attr-defined]
        drain, _ = _assertions(control)
        return drain

    def test_cleared_columns_pass(self) -> None:
        self._drain(engine=None, phase=None, tombstoned=False).expect_still_let_go()

    def test_columns_a_phase_or_a_tombstone_fail(self) -> None:
        for engine, phase, tombstoned in (("v2", None, False), (None, "draining", False), (None, None, True)):
            with self.subTest(engine=engine, phase=phase, tombstoned=tombstoned), self.assertRaises(InvariantViolation):
                self._drain(engine=engine, phase=phase, tombstoned=tombstoned).expect_still_let_go()


class NodeRowAfterLetGo(unittest.TestCase):
    SNAPSHOT = "00000000-0000-4000-8000-000000000021"

    def _drain(self, *, exists: list[bool], tombstoned: bool = False) -> tuple[DrainV2Assertions, _Fixture]:
        control = _Control()
        control.node_row_exists = lambda _id: exists.pop(0) if len(exists) > 1 else exists[0]  # type: ignore[attr-defined]
        control.creating_snapshots_from_node = lambda _id: [self.SNAPSHOT]  # type: ignore[attr-defined]
        control.node_tombstoned = lambda _id: tombstoned  # type: ignore[attr-defined]
        return _assertions(control)

    def test_a_deleted_row_is_recorded(self) -> None:
        drain, fixture = self._drain(exists=[True, False])
        self.assertTrue(drain.record_node_row_after_let_go(timeout=60))
        self.assertTrue(fixture.evidence["node_row_after_let_go"]["deleted"])

    def test_a_kept_row_is_recorded_with_its_snapshots_not_failed(self) -> None:
        drain, fixture = self._drain(exists=[True])
        self.assertFalse(drain.record_node_row_after_let_go(timeout=0))
        self.assertEqual(
            fixture.evidence["node_row_after_let_go"],
            {"deleted": False, "creating_snapshots_from_node": [self.SNAPSHOT]},
        )

    def test_a_tombstone_still_fails(self) -> None:
        drain, _ = self._drain(exists=[False], tombstoned=True)
        with self.assertRaises(InvariantViolation):
            drain.record_node_row_after_let_go(timeout=0)


class InFlightSuspends(unittest.TestCase):
    SANDBOX = "00000000-0000-4000-8000-00000000000f"

    def test_a_running_suspend_is_waited_for_until_it_lands(self) -> None:
        control = _Control(node_states={self.SANDBOX: ["suspending", "suspending", "suspended"]})
        drain, fixture = _assertions(control)
        self.assertEqual(drain.wait_for_in_flight_suspends([self.SANDBOX], timeout=60), {self.SANDBOX: "suspended"})
        self.assertTrue(fixture.evidence["in_flight_suspends_ended"]["ended"])

    def test_a_suspend_still_running_on_the_node_is_not_ended(self) -> None:
        drain, fixture = _assertions(_Control(node_states={self.SANDBOX: ["suspending"]}))
        drain.wait_for_in_flight_suspends([self.SANDBOX], timeout=0)
        self.assertFalse(fixture.evidence["in_flight_suspends_ended"]["ended"])


class StallRule(unittest.TestCase):
    BOUND = 180

    def setUp(self) -> None:
        drain_v2._GATE = drain_v2.DrainV2Gate(stall_bound_seconds=self.BOUND, dispatch_backoff_base_seconds=2)
        self.addCleanup(setattr, drain_v2, "_GATE", None)

    @staticmethod
    def _stalled(*, grew_at: float, deadline: float) -> tuple[DrainV2Assertions, _Fixture]:
        stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(grew_at))
        control = _Control(
            view={drain_v2.VIEW_GREW_AT_FIELD: stamp},
            last_error=f"{drain_v2.STALL_ERROR_PREFIX}: no sandbox evacuated for a while",
        )
        drain, fixture = _assertions(control)
        drain._deadline = int(deadline)
        return drain, fixture

    def test_a_stall_the_bound_past_the_stamp_passes(self) -> None:
        now = time.time()
        drain, fixture = self._stalled(grew_at=now - self.BOUND - 2, deadline=now - self.BOUND - 2)
        drain.wait_for_stall(timeout=1)
        self.assertEqual(fixture.evidence["stalled"]["stall_bound_seconds"], self.BOUND)

    def test_an_early_stall_or_one_before_the_deadline_fails(self) -> None:
        now = time.time()
        for grew_at, deadline in ((now - 60, now - 60), (now - self.BOUND - 2, now + 60)):
            drain, _ = self._stalled(grew_at=grew_at, deadline=deadline)
            with self.subTest(grew_at=grew_at, deadline=deadline), self.assertRaises(InvariantViolation):
                drain.wait_for_stall(timeout=1)


class RollbackRefusal(unittest.TestCase):
    SANDBOX = "00000000-0000-4000-8000-00000000000e"

    def _refused(self, stderr: str) -> None:
        control = _Control(
            {self.SANDBOX: [SandboxResidency("started", NODE_ID)]},
            rollback=NodeCliOutcome(exit_code=1, stdout="", stderr=stderr),
        )
        drain, _ = _assertions(control)
        drain.expect_rollback_refused(_Sandbox(self.SANDBOX, []), "snap")  # type: ignore[arg-type]

    def test_the_sealed_refusal_up_front_passes(self) -> None:
        self._refused(
            f"ERROR: code: '{drain_v2.ROLLBACK_REFUSAL_CODE}', message: "
            f'"{drain_v2.ROLLBACK_SEALED_MARKER} (generation 7); refusing the restore activation of sandbox x"'
        )

    def test_a_late_refusal_or_another_code_fails(self) -> None:
        for stderr in (
            f"ERROR: code: '{drain_v2.ROLLBACK_REFUSAL_CODE}', message: \"the rollback of sandbox x "
            f'to snap was applied; {drain_v2.ROLLBACK_LATE_TEXT} because the node was sealed during the rollback: start it"',
            "ERROR: code: 'The service is currently unavailable', message: \"activation seal unreadable\"",
        ):
            with self.subTest(stderr=stderr), self.assertRaises(InvariantViolation):
                self._refused(stderr)


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


class PreviewContentionTests(unittest.TestCase):
    """A node busy with another drain call answers a preview `ABORTED`, which
    the admin API reports as a 503 `node_busy` with `Retry-After`: the
    harness retries it as contention and takes any other refusal as the
    answer."""

    @staticmethod
    def _refusal(status: int, reason: str, retry_after: str | None = "5") -> HTTPError:
        headers = {} if retry_after is None else {"Retry-After": retry_after}
        body = json.dumps({"error": "a message the harness never reads", "reason": reason})
        return HTTPError("http://127.0.0.1/preview", status, "refused", headers, io.BytesIO(body.encode()))

    @staticmethod
    def _answer(body: dict[str, Any]) -> mock.MagicMock:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(body).encode()
        return response

    def _preview(self, *answers: Any) -> tuple[dict[str, Any], list[float]]:
        harness = KindHarness(_Executor(), KindConfig(), environment={})
        slept: list[float] = []
        with (
            mock.patch.object(KindHarness, "_require_isolated_kind"),
            mock.patch.object(KindHarness, "_admin_token", return_value="token"),
            mock.patch("space_smoke.adapters._URL_OPEN", side_effect=list(answers)),
            mock.patch("space_smoke.adapters.time.sleep", side_effect=slept.append),
        ):
            return harness.drain_v2_preview(NODE_ID), slept

    def test_a_busy_node_is_asked_again_after_its_retry_after(self) -> None:
        busy = [self._refusal(drain_v2.PREVIEW_BUSY_STATUS, drain_v2.PREVIEW_BUSY_REASON) for _ in range(2)]
        report = {"node_id": NODE_ID, "evacuated": [], "pending": [], "blocked": []}
        body, slept = self._preview(*busy, self._answer(report))

        self.assertEqual(body, report)
        self.assertEqual(slept, [5.0, 5.0])

    def test_any_other_refusal_is_the_answer(self) -> None:
        for status, reason in [
            (503, "node_unreachable"),
            (409, drain_v2.PREVIEW_BUSY_REASON),
            (drain_v2.PREVIEW_REFUSED_STATUS, drain_v2.PREVIEW_ORPHANED_SEAL_REASON),
            (drain_v2.PREVIEW_REFUSED_STATUS, drain_v2.PREVIEW_IN_PROGRESS_REASON),
        ]:
            with self.subTest(status=status, reason=reason):
                with self.assertRaisesRegex(RuntimeError, f"HTTP {status}"):
                    self._preview(self._refusal(status, reason))

    def test_contention_is_classified_by_reason_never_by_message(self) -> None:
        busy = json.dumps({"error": "anything", "reason": drain_v2.PREVIEW_BUSY_REASON})
        worded = json.dumps({"error": "another drain call is in progress on this node", "reason": "node_preview_failed"})

        self.assertEqual(_preview_busy_retry_after(503, {"Retry-After": "5"}, busy), 5.0)
        self.assertEqual(_preview_busy_retry_after(503, {}, busy), 1.0)
        self.assertIsNone(_preview_busy_retry_after(503, {"Retry-After": "5"}, worded))
        self.assertIsNone(_preview_busy_retry_after(503, {}, "not json"))


if __name__ == "__main__":
    unittest.main()


class _MutatingControl(_Control):
    """A control whose mutations are recorded, for the compensations a
    scenario arms around them."""

    def __init__(self, *, terminate_status: int = 200, scale_fails: bool = False, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.terminate_status = terminate_status
        self.scale_fails = scale_fails
        self.calls: list[tuple[Any, ...]] = []

    def kind_node_labels(self, _node: str) -> dict[str, str]:
        return {"space.pplx.net/pool": "sandboxes"}

    def terminate_node(self, node_id: str) -> int:
        self.calls.append(("terminate_node", node_id))
        return self.terminate_status

    def stop_kind_worker(self, node: str, *, keep_node: bool = False) -> None:
        self.calls.append(("stop_kind_worker", node, keep_node))

    def start_kind_worker(self, node: str, _labels: Any, **options: Any) -> None:
        self.calls.append(("start_kind_worker", node, options.get("provider_id")))

    def acquire_controller_mutation(self) -> Any:
        return lambda: None

    def controller_replicas(self) -> int:
        return 2

    def scale_controller(self, replicas: int, *, timeout: float) -> int:
        self.calls.append(("scale_controller", replicas))
        if replicas == 0 and self.scale_fails:
            raise RuntimeError("the scale-down wrote, then timed out")
        return 2

    def drain_node_answer(self, _node_id: str, _body: Any) -> CallAnswer:
        return CallAnswer(status=202, retry_after=None, body={})

    def uncordon_kube_node(self, node: str) -> None:
        self.calls.append(("uncordon_kube_node", node))


def _scoped(control: _MutatingControl) -> tuple[DrainV2Assertions, ResourceScope]:
    scope = ResourceScope()
    drain = DrainV2Assertions(_Fixture(), control, lambda **_: None, scope)  # type: ignore[arg-type]
    drain._node_name, drain._node_id, drain._generation = NODE, NODE_ID, 7
    return drain, scope


class Compensations(unittest.TestCase):
    def test_a_refused_terminate_arms_no_replacement_start(self) -> None:
        control = _MutatingControl(terminate_status=409)
        drain, scope = _scoped(control)
        drain._instance = "i-0123456789abcdef0"
        with self.assertRaises(InvariantViolation):
            drain.terminate()
        self.assertEqual(scope.close(), ())
        self.assertFalse(any(call[0] in ("stop_kind_worker", "start_kind_worker") for call in control.calls))

    def test_the_replacement_start_is_armed_only_once_the_worker_is_stopped(self) -> None:
        control = _MutatingControl()
        drain, scope = _scoped(control)
        labels = control.kind_node_labels(NODE)
        restart = drain._stop_then_arm_replacement(labels, "aws:///zone/i-0fedcba9876543210")
        self.assertEqual(control.calls, [("stop_kind_worker", NODE, False)])
        restart.release()
        self.assertEqual(control.calls[-1], ("start_kind_worker", NODE, "aws:///zone/i-0fedcba9876543210"))
        self.assertEqual(scope.close(), ())

    def test_a_scale_down_that_fails_still_scales_the_controller_back(self) -> None:
        control = _MutatingControl(scale_fails=True)
        drain, scope = _scoped(control)
        with self.assertRaises(RuntimeError), drain.controller_down():
            pass
        self.assertEqual(scope.close(), ())
        self.assertEqual(control.calls, [("scale_controller", 0), ("scale_controller", 2)])

    def test_an_accepted_drain_answer_leaves_an_undrain_armed(self) -> None:
        control = _MutatingControl()
        drain, scope = _scoped(control)
        self.assertEqual(drain.drain_answer({"engine": "v2", "hold_seconds": 0}).status, 202)
        self.assertIsNotNone(drain._undrain)
        names = [compensation.name for compensation in scope._compensations]
        self.assertIn(f"undrain {NODE}", names)
        self.assertIn(f"uncordon {NODE}", names)


class CleanFleet(unittest.TestCase):
    def test_a_node_whose_undrain_hasnt_finished_fails_the_clean_fleet_check(self) -> None:
        cancelling = type("Node", (), {
            "id": NODE_ID, "k8s_node_name": NODE, "drain_phase": None, "drain_engine": "v2",
            "drain_last_error": None,
        })()

        class Fleet(_MutatingControl):
            def admin_nodes(self) -> list[Any]:
                return [cancelling]

        drain, _ = _scoped(Fleet())
        with self.assertRaisesRegex(InvariantViolation, "undrain hasn't finished"):
            drain.pin_to_resident_worker()
