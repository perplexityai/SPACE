"""Contracts of the no-free-slot scenario a bench run cannot exercise.

A bench that is correctly set up shows one thing: a full cluster refusing a
cold create with the slot-shortage 503 and admitting one again once a slot
frees. It cannot show what happens when the bench is set up *wrongly* — nodes
that advertise hundreds of slots, nodes that disagree with each other, a
cluster that is full only in total — nor what happens when the refusal is the
right status for the wrong reason. Those are the cases that would make the
scenario pass while proving nothing, and they are pinned here.
"""

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from space_smoke.capabilities import LimiterProbeOutcome
from space_smoke.no_free_slot import NoFreeSlotAssertions, SLOT_CHECK_CAPABILITY
from space_smoke.rate_limiters import CAPABILITIES_ENV, CAPACITY_LIMIT_MESSAGE
from space_smoke.runtime import InvariantViolation, ResourceScope

EMPTY_CLUSTER_MESSAGE = (
    f"{CAPACITY_LIMIT_MESSAGE} no eligible sandbox activation nodes are available. "
    "Not specific to your client — retry with backoff."
)
NO_FREE_SLOT_FULL_MESSAGE = (
    f"{CAPACITY_LIMIT_MESSAGE} no eligible sandbox node has a free sandbox slot. "
    "Not specific to your client — retry with backoff."
)


def _outcome(status: int, message: str = "", retry_after: str | None = None) -> LimiterProbeOutcome:
    return LimiterProbeOutcome(status, retry_after, message, None, 0.0)


class _Fixture:
    lifecycle_transition_timeout = 0
    lifecycle_poll_interval = 0

    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id="sbx_x")
        self.records: dict[str, object] = {}

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, seconds: float, *, reason: str | None = None) -> None:
        return None

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)


class _SlotControl:
    def __init__(self, slots: dict[str, dict[str, int]]) -> None:
        # `terminating` defaults so each test states only what it is about.
        self.slots = {
            name: {"terminating": 0, **facts} for name, facts in slots.items()
        }

    def sandbox_node_slots(self) -> dict[str, dict[str, int]]:
        return {name: dict(facts) for name, facts in self.slots.items()}


class _Probe:
    def __init__(self, *outcomes: LimiterProbeOutcome) -> None:
        self._outcomes = list(outcomes)

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        return self._outcomes.pop(0) if self._outcomes else _outcome(0, "no scripted outcome")

    def kill_matching(self, metadata) -> None:
        return None


def _assertions(
    slots: dict[str, dict[str, int]] | None = None,
    *outcomes: LimiterProbeOutcome,
) -> NoFreeSlotAssertions:
    return NoFreeSlotAssertions(
        cast(Any, _Fixture()),
        cast(Any, _SlotControl(slots or {})),
        cast(Any, _Probe(*outcomes)),
        ResourceScope(),
    )


class SlotBudgetPrecondition(unittest.TestCase):
    """The fill is only affordable on a bench that was told to be small."""

    def test_a_bench_with_no_sandbox_pool_node_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "placement-pool label"):
            _assertions({}).expect_fillable_slot_budget()

    def test_a_node_advertising_no_slots_fails(self) -> None:
        # Nothing can be placed there, so the cluster never becomes full and
        # the refusal this scenario is about would never come.
        slots = {"a": {"capacity": 2, "used": 0}, "b": {"capacity": 0, "used": 0}}
        with self.assertRaisesRegex(InvariantViolation, "no network slots at all"):
            _assertions(slots).expect_fillable_slot_budget()

    def test_nodes_that_disagree_fail(self) -> None:
        slots = {"a": {"capacity": 2, "used": 0}, "b": {"capacity": 3, "used": 0}}
        with self.assertRaisesRegex(InvariantViolation, "different slot counts"):
            _assertions(slots).expect_fillable_slot_budget()

    def test_the_release_default_fails_with_the_chart_hint(self) -> None:
        # The case that matters most: a bench nobody configured advertises the
        # node-manager default, and filling it would be thousands of boots.
        slots = {"a": {"capacity": 300, "used": 0}, "b": {"capacity": 300, "used": 0}}
        with self.assertRaisesRegex(InvariantViolation, "networkPoolMaxSlots"):
            _assertions(slots).expect_fillable_slot_budget()

    def test_a_small_uniform_budget_is_accepted_and_totalled(self) -> None:
        slots = {"a": {"capacity": 2, "used": 0}, "b": {"capacity": 2, "used": 0}}
        assertions = _assertions(slots)
        self.assertEqual(assertions.expect_fillable_slot_budget(), 4)
        recorded = cast(Any, assertions)._fixture.records["slot_budget"]
        self.assertEqual(recorded, {"per_node": 2, "nodes": 2, "total": 4})


class FillLeavesNoNodeWithRoom(unittest.TestCase):
    """Full in total is not full enough."""

    def test_a_cluster_full_only_in_total_fails(self) -> None:
        # One node took every slot while another sat empty: the totals match
        # the budget, so a total-only count would call this full, and the
        # check would still have somewhere to place a create.
        slots = {"a": {"capacity": 2, "used": 4}, "b": {"capacity": 2, "used": 0}}
        assertions = _assertions(slots)
        cast(Any, assertions)._budget = 4
        with self.assertRaisesRegex(InvariantViolation, "full\nonly in total|only in total"):
            assertions.fill_slots()

    def test_a_dirty_bench_fails_before_it_creates_anything(self) -> None:
        slots = {"a": {"capacity": 2, "used": 3}, "b": {"capacity": 2, "used": 2}}
        assertions = _assertions(slots)
        cast(Any, assertions)._budget = 4
        with self.assertRaisesRegex(InvariantViolation, "not clean when the scenario started"):
            assertions.fill_slots()

    def test_a_fill_with_no_budget_established_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "no target"):
            _assertions({"a": {"capacity": 2, "used": 0}}).fill_slots()


class SlotRefusalVerdict(unittest.TestCase):
    """The 503 has to be the slot check's, not merely a 503."""

    def test_an_admitted_create_is_the_defect(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "was admitted while every eligible"):
            _assertions({}, _outcome(201)).expect_no_free_slot()

    def test_a_429_is_not_the_slot_check(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "not the 503"):
            _assertions({}, _outcome(429, "Server rate limit: shedding", "3")).expect_no_free_slot()

    def test_the_empty_cluster_503_does_not_satisfy_it(self) -> None:
        # The discriminator. The same build raises this from its empty-cluster
        # guard under the same prefix, and a bench whose nodes had gone
        # ineligible would answer exactly this while proving nothing about
        # slots.
        with self.assertRaisesRegex(InvariantViolation, "does not name a free sandbox slot"):
            _assertions({}, _outcome(503, EMPTY_CLUSTER_MESSAGE, "5")).expect_no_free_slot()

    def test_a_slot_503_without_retry_after_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "no Retry-After"):
            _assertions({}, _outcome(503, NO_FREE_SLOT_FULL_MESSAGE)).expect_no_free_slot()

    def test_the_slot_shortage_503_passes_and_records_its_evidence(self) -> None:
        assertions = _assertions({}, _outcome(503, NO_FREE_SLOT_FULL_MESSAGE, "5"))
        assertions.expect_no_free_slot()
        records = cast(Any, assertions)._fixture.records
        self.assertEqual(records["no_free_slot_status"], 503)
        self.assertEqual(records["no_free_slot_retry_after"], "5")


class ReconnectAndRecovery(unittest.TestCase):
    """The two verdicts that keep a broken cluster from passing."""

    def test_a_refused_reconnect_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "adds no placement"):
            _assertions({}, _outcome(503, NO_FREE_SLOT_FULL_MESSAGE, "5")).expect_reconnect_admitted(
                "sbx-1"
            )

    def test_recovery_without_a_fill_fails_rather_than_asserting_blind(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "no slot to free"):
            _assertions({"a": {"capacity": 2, "used": 1}}).expect_recovery()

    def test_a_create_still_refused_after_a_slot_frees_fails(self) -> None:
        # Without this verdict a build that refused every create for any
        # reason at all would satisfy every verdict above it.
        slots = {"a": {"capacity": 2, "used": 1}}
        assertions = _assertions(slots, _outcome(503, NO_FREE_SLOT_FULL_MESSAGE, "5"))
        killed: list[str] = []
        cast(Any, assertions)._ballast = [
            SimpleNamespace(sandbox_id="fill-1", kill=lambda: killed.append("fill-1"))
        ]
        with self.assertRaisesRegex(InvariantViolation, "still refused after a network slot"):
            assertions.expect_recovery()
        self.assertEqual(killed, ["fill-1"])


class RecoveryWaitsForThePodToActuallyGo(unittest.TestCase):
    """A terminating pod still holds its slot, and the wait has to know it.

    Found on the first bench run of this scenario: the killed sandbox's pod was
    still terminating, a count that excluded terminating pods reported the slot
    free, and the recovery create got past the slot check only to be refused by
    kube-scheduler, which was still charging the departing pod's slot request.
    That reads as the check failing to recover when nothing is wrong with it.
    """

    def test_a_terminating_pod_is_not_room_to_place(self) -> None:
        # Every slot accounted for, but one pod is on its way out. The node has
        # no room until it is gone, so the wait must not break here. With a
        # zero deadline the wait gives up at once, which surfaces as the
        # give-up verdict rather than as a recovery attempt.
        slots = {"a": {"capacity": 2, "used": 1, "terminating": 1}}
        assertions = _assertions(slots, _outcome(201))
        cast(Any, assertions)._ballast = [
            SimpleNamespace(sandbox_id="fill-1", kill=lambda: None)
        ]
        # `timeout=0` so the bounded wait gives up at once: the fake fixture's
        # `wait` is a no-op, so the method's own 600s default would spin.
        with self.assertRaisesRegex(InvariantViolation, "no node reported a free network slot"):
            assertions.expect_recovery(timeout=0)

    def test_a_pod_that_has_gone_is_room_to_place(self) -> None:
        slots = {"a": {"capacity": 2, "used": 1, "terminating": 0}}
        assertions = _assertions(slots, _outcome(201))
        cast(Any, assertions)._ballast = [
            SimpleNamespace(sandbox_id="fill-1", kill=lambda: None)
        ]
        assertions.expect_recovery()
        self.assertEqual(
            cast(Any, assertions)._fixture.records["recovery_statuses"], {"201": 1}
        )


class SlotCheckCapability(unittest.TestCase):
    """Declared, never probed — and recorded either way."""

    def test_an_undeclared_bench_reports_absent(self) -> None:
        assertions = _assertions({})
        with mock.patch.dict(os.environ, {CAPABILITIES_ENV: ""}, clear=False):
            self.assertFalse(assertions.capability_declared())
        self.assertIs(
            cast(Any, assertions)._fixture.records["capability_slot_check_declared"], False
        )

    def test_a_declared_bench_runs(self) -> None:
        assertions = _assertions({})
        with mock.patch.dict(os.environ, {CAPABILITIES_ENV: SLOT_CHECK_CAPABILITY}, clear=False):
            self.assertTrue(assertions.capability_declared())


if __name__ == "__main__":
    unittest.main()
