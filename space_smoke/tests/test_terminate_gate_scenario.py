"""Contracts of the terminate-gate scenario the bench cannot exercise.

A passing bench run shows one 409. It cannot show that the *other* 409 this
route can produce — the illegal drain-phase transition, whose body carries
`error` / `current_drain_phase` and no counts — is refused as evidence, which
is the whole difference between proving the gate and proving that something
said no. Nor that a refusal counting a resident sandbox is rejected (the
resident half would have produced it on any revision, including the buggy
one), that the seeding refuses when no attributable node exists rather than
improvising, that neither write happens without the mutation Lease, and that
the repair order puts the row's deletion before the node's undrain.
"""

from __future__ import annotations

import io
import json
import unittest
from email.message import Message
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock
from urllib.error import HTTPError

from space_smoke.adapters import (
    DRAIN_GATE_NODE_ANNOTATION,
    DRAIN_GATE_SANDBOX_ANNOTATION,
    KindConfig,
    KindHarness,
)
from space_smoke.capabilities import DrainGateBlocker, TerminateAttempt
from space_smoke.drain_gate import CONFLICT_BODY_KEYS, DrainGateAssertions
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    SmokeConfigurationError,
)

NODE_ID = "018f2b41-7c22-7bb2-9f2a-0c9a1d5f4e11"
SANDBOX_ID = "018f2b41-7c22-7bb2-9f2a-0c9a1d5f4e22"
OCCUPANCY_409 = {
    "sandboxes_active": 0,
    "sandboxes_drain_blockers": 1,
    "sandboxes_local_snapshots": 1,
}
# What the same route answers when the node is not `drained`: same status,
# different body, and nothing to do with occupancy.
TRANSITION_409 = {
    "error": "drain-phase transition not allowed",
    "current_drain_phase": None,
    "allowed_from": ["drained"],
}


class _Fixture:
    def __init__(self) -> None:
        self.records: dict[str, object] = {}

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)


class _Control:
    def __init__(self, attempt: TerminateAttempt, *, seed_fails: bool = False) -> None:
        self.attempt = attempt
        self.seed_fails = seed_fails
        self.calls: list[str] = []

    def acquire_drain_gate_mutation(self):
        self.calls.append("acquire")
        return lambda: self.calls.append("release-lease")

    def seed_drain_gate_blocker(self) -> DrainGateBlocker:
        self.calls.append("seed")
        if self.seed_fails:
            raise RuntimeError("the drain-gate blocker row was not seeded")
        return DrainGateBlocker(node_id=NODE_ID, sandbox_id=SANDBOX_ID)

    def clear_drain_gate_residue(self) -> None:
        self.calls.append("clear-residue")

    def attempt_terminate(self, node_id: str) -> TerminateAttempt:
        self.calls.append(f"terminate:{node_id}")
        return self.attempt


def _assertions(body: dict[str, object], status: int = 409, *, seed_fails: bool = False):
    fixture = _Fixture()
    control = _Control(TerminateAttempt(status, body), seed_fails=seed_fails)
    scope = ResourceScope()
    return (
        fixture,
        control,
        scope,
        DrainGateAssertions(cast(Any, fixture), cast(Any, control), scope),
    )


class OnlyTheOccupancyRefusalCounts(unittest.TestCase):
    def test_the_occupancy_refusal_passes_and_is_recorded(self) -> None:
        fixture, control, _scope, gate = _assertions(dict(OCCUPANCY_409))
        gate.seed_blocker()
        gate.expect_terminate_refused()
        self.assertEqual(fixture.records["terminate_status"], 409)
        self.assertEqual(fixture.records["drain_gate_node_id"], NODE_ID)
        self.assertEqual(control.calls[-1], f"terminate:{NODE_ID}")

    def test_the_transition_refusal_is_not_evidence(self) -> None:
        _fixture, _control, _scope, gate = _assertions(dict(TRANSITION_409))
        gate.seed_blocker()
        with self.assertRaisesRegex(InvariantViolation, "occupancy counts"):
            gate.expect_terminate_refused()

    def test_a_refusal_from_the_resident_half_is_not_evidence(self) -> None:
        body = dict(OCCUPANCY_409, sandboxes_active=1)
        _fixture, _control, _scope, gate = _assertions(body)
        gate.seed_blocker()
        with self.assertRaisesRegex(InvariantViolation, "resident sandbox"):
            gate.expect_terminate_refused()

    def test_an_accepted_terminate_fails_the_scenario(self) -> None:
        _fixture, _control, _scope, gate = _assertions({"status": "terminating"}, 200)
        gate.seed_blocker()
        with self.assertRaisesRegex(InvariantViolation, "still\n?\\s*holds"):
            gate.expect_terminate_refused()

    def test_dropping_the_rollout_driver_alias_fails(self) -> None:
        # A copy of `space-rollout-mng.py` from before the rename reads only
        # this key; losing it nulls that copy's stall report rather than
        # failing it.
        body = {
            key: value
            for key, value in OCCUPANCY_409.items()
            if key != "sandboxes_local_snapshots"
        }
        _fixture, _control, _scope, gate = _assertions(body)
        gate.seed_blocker()
        with self.assertRaisesRegex(InvariantViolation, "occupancy counts"):
            gate.expect_terminate_refused()

    def test_the_alias_is_part_of_the_asserted_contract(self) -> None:
        self.assertIn("sandboxes_local_snapshots", CONFLICT_BODY_KEYS)


class TheBenchIsHandedBackClean(unittest.TestCase):
    def test_release_undoes_both_writes_and_the_lease_once(self) -> None:
        _fixture, control, scope, gate = _assertions(dict(OCCUPANCY_409))
        gate.seed_blocker()
        gate.release_blocker()
        self.assertEqual(
            control.calls,
            ["acquire", "seed", "clear-residue", "release-lease"],
        )
        # Scope close must not repeat a compensation the phase already ran.
        self.assertEqual(scope.close(), ())
        self.assertEqual(
            control.calls,
            ["acquire", "seed", "clear-residue", "release-lease"],
        )

    def test_a_scenario_that_dies_after_seeding_still_undoes_both(self) -> None:
        _fixture, control, scope, gate = _assertions(dict(TRANSITION_409))
        gate.seed_blocker()
        with self.assertRaises(InvariantViolation):
            gate.expect_terminate_refused()
        self.assertEqual(scope.close(), ())
        self.assertEqual(
            control.calls,
            [
                "acquire",
                "seed",
                f"terminate:{NODE_ID}",
                "clear-residue",
                "release-lease",
            ],
        )

    def test_seeding_that_dies_between_its_writes_still_undoes_them(self) -> None:
        # The node moves to `drained` before the row is inserted, and the two
        # are separate statements. A compensation registered on the return
        # value would not exist here, and the Lease release that follows makes
        # the residue permanent: takeover repair only fires on an *expired*
        # Lease, and a released one never expires.
        _fixture, control, scope, gate = _assertions(
            dict(OCCUPANCY_409), seed_fails=True
        )
        with self.assertRaises(RuntimeError):
            gate.seed_blocker()
        self.assertEqual(scope.close(), ())
        self.assertEqual(
            control.calls, ["acquire", "seed", "clear-residue", "release-lease"]
        )
        self.assertLess(
            control.calls.index("clear-residue"),
            control.calls.index("release-lease"),
            "the residue must be cleared while the Lease is still held",
        )


def _lease_json(node: str | None, sandbox: str | None) -> str:
    annotations = {}
    if node is not None:
        annotations[DRAIN_GATE_NODE_ANNOTATION] = node
    if sandbox is not None:
        annotations[DRAIN_GATE_SANDBOX_ANNOTATION] = sandbox
    return json.dumps(
        {
            "metadata": {
                "name": "l",
                "uid": "u",
                "resourceVersion": "1",
                "annotations": annotations,
            },
            "spec": {},
        }
    )


class _RecordingKubectl:
    """A kubectl whose `exec` answers are the scripted psql results."""

    def __init__(self, *, psql_results: list[str], lease_json: str = "") -> None:
        # `_psql` sends the statement on stdin, not in argv, so the fake has
        # to keep the two together or an assertion about SQL reads a flag.
        self.calls: list[tuple[tuple[str, ...], str | None]] = []
        self.psql_results = list(psql_results)
        self.lease_json = lease_json

    def run(self, command, *, input_text=None, **_options) -> CommandResult:
        argv = tuple(command)
        self.calls.append((argv, input_text))
        verb = argv[5]
        if verb == "exec":
            return CommandResult(argv, 0, self.psql_results.pop(0), "")
        if verb == "get":
            return CommandResult(argv, 0, self.lease_json, "")
        return CommandResult(argv, 0, "", "")

    def verbs(self) -> list[str]:
        return [argv[5] for argv, _ in self.calls]

    def psql_statements(self) -> list[str]:
        return [text or "" for argv, text in self.calls if argv[5] == "exec"]

    def annotations(self) -> list[str]:
        return [argv[8] for argv, _ in self.calls if argv[5] == "annotate"]


def _harness(kubectl: _RecordingKubectl) -> KindHarness:
    return KindHarness(cast(Any, kubectl), KindConfig())


class SeedingRefusesRatherThanImprovising(unittest.TestCase):
    def test_no_write_happens_without_the_lease(self) -> None:
        kubectl = _RecordingKubectl(psql_results=[])
        harness = _harness(kubectl)
        with self.assertRaisesRegex(SmokeConfigurationError, "live mutation Lease"):
            harness.seed_drain_gate_blocker()
        with self.assertRaisesRegex(SmokeConfigurationError, "live mutation Lease"):
            harness.attempt_terminate(NODE_ID)
        self.assertEqual(kubectl.psql_statements(), [])

    def test_no_attributable_node_is_a_configuration_error(self) -> None:
        kubectl = _RecordingKubectl(psql_results=[""])
        harness = _harness(kubectl)
        with mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None):
            harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
            with self.assertRaisesRegex(SmokeConfigurationError, "attributable"):
                harness.seed_drain_gate_blocker()
        # Only the selection ran; nothing was drained and nothing was seeded.
        self.assertEqual(len(kubectl.psql_statements()), 1)

    def test_the_seeded_row_is_pinned_by_target_node_id_alone(self) -> None:
        kubectl = _RecordingKubectl(
            psql_results=[
                NODE_ID,
                SANDBOX_ID,
                f"{SANDBOX_ID}\nINSERT 0 1",
                f"{NODE_ID}\nUPDATE 1",
            ]
        )
        harness = _harness(kubectl)
        with mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None):
            harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
            blocker = harness.seed_drain_gate_blocker()
        self.assertEqual(blocker, DrainGateBlocker(NODE_ID, SANDBOX_ID))
        insert = kubectl.psql_statements()[2]
        self.assertIn("'suspending'", insert)
        self.assertIn("target_node_id", insert)
        self.assertNotIn("active_node_id", insert)
        self.assertEqual(
            kubectl.annotations(),
            [
                f"{DRAIN_GATE_SANDBOX_ANNOTATION}={SANDBOX_ID}",
                f"{DRAIN_GATE_NODE_ANNOTATION}={NODE_ID}",
            ],
        )
        def call_index(predicate) -> int:
            return next(
                index for index, call in enumerate(kubectl.calls) if predicate(call)
            )

        def sql_at(fragment: str) -> int:
            return call_index(
                lambda call: call[0][5] == "exec" and fragment in (call[1] or "")
            )

        def annotation_at(key: str) -> int:
            return call_index(
                lambda call: call[0][5] == "annotate" and call[0][8].startswith(key)
            )

        # Each write is recorded on the Lease *before* it happens — including
        # the row, whose id is minted first for exactly that reason. A death
        # between a write and its annotation must leave the next holder
        # over-cleaning; the reverse leaves a `suspending` row pinned to a
        # bench node with nothing able to name it.
        self.assertLess(
            annotation_at(DRAIN_GATE_NODE_ANNOTATION),
            sql_at("UPDATE nodes SET drain_phase = 'drained'"),
        )
        self.assertLess(
            annotation_at(DRAIN_GATE_SANDBOX_ANNOTATION),
            sql_at("INSERT INTO sandboxes"),
        )
        # And the node is never `drained` without its blocker: on the kind
        # bench `drained_node_terminator` deletes the kube Node of a drained
        # node the gate no longer holds, so the row must exist first.
        self.assertLess(
            sql_at("INSERT INTO sandboxes"),
            sql_at("UPDATE nodes SET drain_phase = 'drained'"),
        )

    def test_the_row_is_inserted_under_the_id_the_lease_records(self) -> None:
        # A mismatch means the Lease names a row that does not exist while an
        # unnamed one does. Fail rather than return the wrong identity.
        kubectl = _RecordingKubectl(
            psql_results=[
                NODE_ID,
                f"{NODE_ID}\nUPDATE 1",
                SANDBOX_ID,
                f"{NODE_ID}\nINSERT 0 1",
            ]
        )
        harness = _harness(kubectl)
        with mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None):
            harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
            with self.assertRaisesRegex(SmokeConfigurationError, "was not seeded"):
                harness.seed_drain_gate_blocker()

    def test_the_node_must_still_be_undrained_when_the_write_lands(self) -> None:
        # The selection and the update are two statements; a node that picked
        # up a drain phase in between must abort the scenario, not silently
        # reuse someone else's drain.
        kubectl = _RecordingKubectl(
            psql_results=[
                NODE_ID,
                SANDBOX_ID,
                f"{SANDBOX_ID}\nINSERT 0 1",
                "UPDATE 0",
            ]
        )
        harness = _harness(kubectl)
        with mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None):
            harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
            with self.assertRaisesRegex(SmokeConfigurationError, "between the read"):
                harness.seed_drain_gate_blocker()
        # The drain was someone else's, so its record is dropped before the
        # failure: the cleanup then deletes our row and leaves their node be.
        self.assertEqual(
            kubectl.annotations(),
            [
                f"{DRAIN_GATE_SANDBOX_ANNOTATION}={SANDBOX_ID}",
                f"{DRAIN_GATE_NODE_ANNOTATION}={NODE_ID}",
                f"{DRAIN_GATE_NODE_ANNOTATION}-",
            ],
        )

    def test_the_undo_undrains_the_node_before_deleting_the_row(self) -> None:
        # Read off the Lease, not off an argument, so the undo is armable
        # before anything has been written.
        kubectl = _RecordingKubectl(
            psql_results=["reset", ""], lease_json=_lease_json(NODE_ID, SANDBOX_ID)
        )
        harness = _harness(kubectl)
        with mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None):
            harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
            harness.clear_drain_gate_residue()
        statements = kubectl.psql_statements()
        # Undrained first: a `drained` node whose blocker has gone is the
        # shape the kind bench's terminator deletes.
        self.assertIn("UPDATE nodes SET drain_phase = NULL", statements[0])
        self.assertIn("DELETE FROM sandboxes", statements[1])
        # Each annotation is cleared only after its own write commits.
        self.assertEqual(
            kubectl.annotations(),
            [f"{DRAIN_GATE_NODE_ANNOTATION}-", f"{DRAIN_GATE_SANDBOX_ANNOTATION}-"],
        )

    def test_the_undo_is_a_no_op_when_nothing_was_written(self) -> None:
        # Armed before seeding, it runs on every path — including one where
        # the Lease was taken and the first write never happened.
        kubectl = _RecordingKubectl(psql_results=[], lease_json=_lease_json(None, None))
        harness = _harness(kubectl)
        with mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None):
            harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
            harness.clear_drain_gate_residue()
        self.assertEqual(kubectl.psql_statements(), [])

    def test_a_node_recorded_without_a_row_is_still_undrained(self) -> None:
        # The exact partial state the ordering bug produced: drained, no row.
        kubectl = _RecordingKubectl(
            psql_results=["reset"], lease_json=_lease_json(NODE_ID, None)
        )
        harness = _harness(kubectl)
        with mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None):
            harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
            harness.clear_drain_gate_residue()
        statements = kubectl.psql_statements()
        self.assertEqual(len(statements), 1)
        self.assertIn("UPDATE nodes SET drain_phase = NULL", statements[0])


class TheUndoKeepsABlockerTheNodeStillNeeds(unittest.TestCase):
    """The blocker row is deleted only once the node can't be acted on for
    lacking it. A node `:terminate` accepted is `terminating`, not `drained`;
    deleting its row would leave the terminator's gate nothing to hold it on,
    and clearing the Lease record would leave a later run nothing to repair."""

    def _undo(self, found: str) -> tuple[_RecordingKubectl, BaseException | None]:
        kubectl = _RecordingKubectl(
            psql_results=[found, ""], lease_json=_lease_json(NODE_ID, SANDBOX_ID)
        )
        harness = _harness(kubectl)
        raised: BaseException | None = None
        with mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None):
            harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
            try:
                harness.clear_drain_gate_residue()
            except RuntimeError as exc:
                raised = exc
        return kubectl, raised

    def test_a_terminating_node_keeps_its_row_and_the_record(self) -> None:
        kubectl, raised = self._undo("terminating")
        self.assertIsNotNone(raised)
        self.assertIn("terminating", str(raised))
        statements = kubectl.psql_statements()
        self.assertEqual(len(statements), 1, "no DELETE after an unmatched reset")
        self.assertEqual(kubectl.annotations(), [], "the Lease record is kept")

    def test_a_node_another_drain_holds_keeps_its_row(self) -> None:
        kubectl, raised = self._undo("draining")
        self.assertIsNotNone(raised)
        self.assertEqual(len(kubectl.psql_statements()), 1)

    def test_a_node_already_back_to_no_phase_lets_the_row_go(self) -> None:
        # A run that died after the reset committed but before its record was
        # cleared: the next holder finds no phase and finishes the undo.
        for found in ("none", "gone"):
            with self.subTest(found=found):
                kubectl, raised = self._undo(found)
                self.assertIsNone(raised)
                statements = kubectl.psql_statements()
                self.assertEqual(len(statements), 2)
                self.assertIn("DELETE FROM sandboxes", statements[1])


def _http_error(status: int, body: bytes) -> HTTPError:
    return HTTPError(
        "http://127.0.0.1:8080/admin/api/v1/nodes/x:terminate",
        status,
        "conflict",
        Message(),
        io.BytesIO(body),
    )


class TheRefusalIsDataNotAnException(unittest.TestCase):
    def _owned_harness(self) -> KindHarness:
        harness = _harness(_RecordingKubectl(psql_results=[]))
        harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
        harness.environment["SPACE_API_TOKEN"] = "token"
        return harness

    def test_a_409_is_returned_with_its_body(self) -> None:
        harness = self._owned_harness()
        with (
            mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None),
            mock.patch(
                "space_smoke.adapters.urlopen",
                side_effect=_http_error(409, b'{"sandboxes_drain_blockers":1}'),
            ),
        ):
            attempt = harness.attempt_terminate(NODE_ID)
        self.assertEqual(attempt.status_code, 409)
        self.assertEqual(attempt.body, {"sandboxes_drain_blockers": 1})

    def test_a_non_json_body_is_a_configuration_error_not_an_empty_body(self) -> None:
        harness = self._owned_harness()
        with (
            mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None),
            mock.patch(
                "space_smoke.adapters.urlopen",
                side_effect=_http_error(502, b"<html>bad gateway</html>"),
            ),
            self.assertRaisesRegex(SmokeConfigurationError, "not JSON"),
        ):
            harness.attempt_terminate(NODE_ID)

    def test_the_route_is_refused_outside_the_local_kind_api(self) -> None:
        kubectl = _RecordingKubectl(psql_results=[])
        harness = KindHarness(
            cast(Any, kubectl),
            KindConfig(cluster="prod", api_url="https://space.example.com"),
        )
        harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
        with self.assertRaisesRegex(SmokeConfigurationError, "isolated local Kind API"):
            harness.attempt_terminate(NODE_ID)
        with self.assertRaisesRegex(SmokeConfigurationError, "isolated local Kind API"):
            harness.seed_drain_gate_blocker()

    def test_a_named_bench_runs_only_when_pinned_to_its_live_cluster(self) -> None:
        # A per-workspace bench's context is `kind-space-oss-<hash>`, not
        # `kind-space-local`. The scenario runs there exactly as the rest of the
        # harness's admin fixtures do: pinned to the cluster's kube-system UID,
        # and only while the live cluster still carries it.
        uid = "00000000-0000-4000-8000-000000000001"

        def bench(pinned: str | None, live: str) -> KindHarness:
            harness = KindHarness(
                cast(Any, _RecordingKubectl(psql_results=[], lease_json=live)),
                KindConfig(cluster="space-oss-0123456789"),
            )
            if pinned is None:
                harness.environment.pop("SPACE_SMOKE_KIND_CLUSTER_UID", None)
            else:
                harness.environment["SPACE_SMOKE_KIND_CLUSTER_UID"] = pinned
            return harness

        for action in ("seed_drain_gate_blocker", "attempt_terminate"):
            args = () if action == "seed_drain_gate_blocker" else (NODE_ID,)
            with self.assertRaisesRegex(SmokeConfigurationError, "isolated local Kind API"):
                getattr(bench(None, uid), action)(*args)
            with self.assertRaisesRegex(SmokeConfigurationError, "changed Kind cluster identity"):
                getattr(bench(uid, "a-recreated-cluster"), action)(*args)
            # Pinned and matching: past the isolation check, refused only by
            # the next guard, because this harness holds no Lease.
            with self.assertRaisesRegex(SmokeConfigurationError, "live mutation Lease"):
                getattr(bench(uid, uid), action)(*args)

    def test_a_missing_token_is_named_rather_than_sent_empty(self) -> None:
        harness = _harness(_RecordingKubectl(psql_results=[]))
        harness._drain_gate_lease_ownership = cast(Any, SimpleNamespace(name="l"))
        harness.environment.pop("SPACE_API_TOKEN", None)
        harness.environment.pop("SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE", None)
        with (
            mock.patch.object(KindHarness, "_assert_lease_owned", return_value=None),
            self.assertRaisesRegex(SmokeConfigurationError, "SPACE_API_TOKEN"),
        ):
            harness.attempt_terminate(NODE_ID)


if __name__ == "__main__":
    unittest.main()


class PsqlOutputIsParsedTheWayPsqlWritesIt(unittest.TestCase):
    """The shape the fakes above must keep matching, pinned against real output.

    A live run is what exposed this: every fixture fed one clean line, real
    psql appends a command tag under `-tA`, and the seeding refused its own
    successful INSERT. The cases below are transcribed from
    `psql -U spaceuser -d spacedb -tA` on the kind Postgres.
    """

    def test_a_returned_row_is_read_without_its_command_tag(self) -> None:
        self.assertEqual(
            KindHarness._returned_value(f"{SANDBOX_ID}\nINSERT 0 1\n"), SANDBOX_ID
        )
        self.assertEqual(
            KindHarness._returned_value(f"{NODE_ID}\nUPDATE 1\n"), NODE_ID
        )

    def test_a_statement_that_matched_nothing_reads_as_no_rows(self) -> None:
        # The tag alone. Returning it as a value would turn "another holder
        # already drained this node" into a successful read of the string
        # "UPDATE 0", and the scenario would seed against a node it does not
        # own.
        self.assertEqual(KindHarness._returned_value("UPDATE 0\n"), "")
        self.assertEqual(KindHarness._returned_value("INSERT 0 0\n"), "")
        self.assertEqual(KindHarness._returned_value(""), "")

    def test_a_plain_select_has_no_tag_to_strip(self) -> None:
        # `-t` does suppress the footer for SELECT, so the node lookup reads
        # its row directly; this pins that the tag stripping cannot eat it.
        self.assertEqual(KindHarness._returned_value(f"{NODE_ID}\n"), NODE_ID)
