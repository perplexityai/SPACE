"""Unit tests for scripts/space-rollout-mng.py.

Run from the repo root:

    python -m pytest scripts/tests/test_rollout_mng.py -q

The script lives at `scripts/space-rollout-mng.py` (kebab case for the CLI
entry point), so we import it via importlib rather than a normal module path.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import pathlib
import sys
from typing import Any

import pytest

SCRIPT_PATH = (
    pathlib.Path(__file__).resolve().parent.parent / "space-rollout-mng.py"
)
_spec = importlib.util.spec_from_file_location("space_rollout_mng", SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
rollout = importlib.util.module_from_spec(_spec)
sys.modules["space_rollout_mng"] = rollout
_spec.loader.exec_module(rollout)


# Sentinel for "no override" on a fake response body, so a test can pass an
# explicit `None` body and mean it.
_KEEP = object()


class _StopProbe(Exception):
    """Aborts a real `AdminApi` call once it has produced its URL.

    Lets a URL be captured without modelling any response, so the assertion
    cannot accidentally depend on the fake's body shape.
    """


def make_admin_node(
    *,
    node_id: str = "00000000-0000-0000-0000-000000000001",
    k8s_node_name: str | None = "ip-10-0-0-1.us-east-1.compute.internal",
    drain_phase: str | None = None,
    drain_started_at: dt.datetime | None = None,
    state: str | None = "ready",
    sandboxes_active: int = 0,
    sandboxes_local_snapshots: int = 0,
    terminate_decrement_capacity: bool = True,
) -> Any:
    return rollout.AdminNode(
        id=node_id,
        k8s_node_name=k8s_node_name,
        address=None,
        drain_phase=drain_phase,
        drain_started_at=drain_started_at,
        state=state,
        sandboxes_active=sandboxes_active,
        sandboxes_local_snapshots=sandboxes_local_snapshots,
        terminate_decrement_capacity=terminate_decrement_capacity,
    )


def make_node_status(
    *,
    instance_id: str = "i-0123456789abcdef0",
    asg_name: str = "eks-source-asg",
    private_dns: str | None = "ip-10-0-0-1.us-east-1.compute.internal",
    lifecycle_state: str = "InService",
    admin: Any | None = None,
    k8s_ready: bool = True,
) -> Any:
    return rollout.NodeStatus(
        instance_id=instance_id,
        asg_name=asg_name,
        private_dns=private_dns,
        lifecycle_state=lifecycle_state,
        admin=admin,
        k8s_ready=k8s_ready,
    )


class TestParseAdminNode:
    def test_full_row(self) -> None:
        row = {
            "id": "11111111-1111-1111-1111-111111111111",
            "k8s_node_name": "ip-10-0-0-1.us-east-1.compute.internal",
            "address": "10.0.0.1",
            "drain_phase": "draining",
            "drain_started_at": "2026-05-15T12:00:00Z",
            "state": "ready",
            "sandboxes_active": 3,
            "sandboxes_local_snapshots": 1,
            "terminate_decrement_capacity": True,
        }
        node = rollout.parse_admin_node(row)
        assert node.id == row["id"]
        assert node.k8s_node_name == row["k8s_node_name"]
        assert node.drain_phase == "draining"
        assert node.drain_started_at is not None
        assert node.drain_started_at.tzinfo is not None
        assert node.sandboxes_active == 3
        assert node.sandboxes_local_snapshots == 1
        assert node.terminate_decrement_capacity is True

    def test_missing_optional_fields(self) -> None:
        row = {"id": "22222222-2222-2222-2222-222222222222"}
        node = rollout.parse_admin_node(row)
        assert node.drain_phase is None
        assert node.drain_started_at is None
        assert node.sandboxes_active == 0
        # Default-false matches the admin-API column default — protects
        # against terminating a node whose decrement intent we can't confirm.
        assert node.terminate_decrement_capacity is False

    def test_bad_timestamp_is_none(self) -> None:
        node = rollout.parse_admin_node(
            {"id": "33333333-3333-3333-3333-333333333333", "drain_started_at": "not-a-time"}
        )
        assert node.drain_started_at is None


class TestRolloutPhase:
    def test_no_admin_means_gone(self) -> None:
        ns = make_node_status(admin=None)
        assert ns.rollout_phase == rollout.DrainPhase.GONE

    def test_no_phase_means_none(self) -> None:
        ns = make_node_status(admin=make_admin_node(drain_phase=None))
        assert ns.rollout_phase == rollout.DrainPhase.NONE

    @pytest.mark.parametrize(
        "phase",
        [rollout.DrainPhase.DRAINING, rollout.DrainPhase.DRAINED, rollout.DrainPhase.TERMINATING],
    )
    def test_passthrough(self, phase: str) -> None:
        ns = make_node_status(admin=make_admin_node(drain_phase=phase))
        assert ns.rollout_phase == phase


class TestCorrelateNodes:
    def test_joins_on_private_dns(self) -> None:
        asg = [
            rollout.AsgInstance(
                instance_id="i-1",
                asg_name="asg",
                lifecycle_state="InService",
                health_status="Healthy",
            )
        ]
        ec2 = {
            "i-1": rollout.Ec2Instance(
                instance_id="i-1",
                private_dns="ip-10-0-0-1.us-east-1.compute.internal",
                state="running",
            )
        }
        admin = [make_admin_node(k8s_node_name="ip-10-0-0-1.us-east-1.compute.internal")]
        ready = {"ip-10-0-0-1.us-east-1.compute.internal"}
        out = rollout.correlate_nodes(asg, ec2, admin, ready)
        assert len(out) == 1
        assert out[0].admin is not None
        assert out[0].k8s_ready is True

    def test_no_ec2_means_no_admin_join(self) -> None:
        asg = [
            rollout.AsgInstance(
                instance_id="i-2",
                asg_name="asg",
                lifecycle_state="InService",
                health_status="Healthy",
            )
        ]
        out = rollout.correlate_nodes(asg, {}, [], set())
        assert len(out) == 1
        assert out[0].admin is None
        assert out[0].k8s_ready is False
        assert out[0].rollout_phase == rollout.DrainPhase.GONE


class TestPickDrainCandidates:
    def _node(self, idx: int, phase: str | None) -> Any:
        admin = make_admin_node(
            node_id=f"00000000-0000-0000-0000-{idx:012d}",
            drain_phase=phase,
        )
        return make_node_status(instance_id=f"i-{idx}", admin=admin)

    @staticmethod
    def _az_node(idx: int, az: str) -> Any:
        """A NodeStatus pinned to an AZ, for the AZ-balance picker tests."""
        admin = make_admin_node(node_id=f"00000000-0000-0000-0000-{idx:012d}")
        return rollout.NodeStatus(
            instance_id=f"i-{idx:03d}",
            asg_name="eks-source-asg",
            private_dns=f"ip-10-0-0-{idx}.us-east-1.compute.internal",
            lifecycle_state="InService",
            admin=admin,
            k8s_ready=True,
            availability_zone=az,
        )

    def test_no_drains_in_progress_picks_up_to_cap(self) -> None:
        source = [self._node(i, None) for i in range(5)]
        picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=2)
        assert len(picked) == 2
        assert {p.instance_id for p in picked} == {"i-0", "i-1"}

    def test_respects_existing_draining(self) -> None:
        source = [
            self._node(0, rollout.DrainPhase.DRAINING),
            self._node(1, None),
            self._node(2, None),
        ]
        picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=2)
        assert len(picked) == 1
        assert picked[0].instance_id == "i-1"

    def test_drained_counts_against_cap_until_terminated(self) -> None:
        """drained/terminating source nodes still occupy a target slot until
        the ASG decrement lands, so they count toward the in-flight cap.
        Starting a new drain wave while a prior wave is drained-but-not-yet-
        terminated would put migrating sandboxes into a target with no
        free landing capacity."""

        source = [
            self._node(0, rollout.DrainPhase.DRAINED),
            self._node(1, rollout.DrainPhase.TERMINATING),
            self._node(2, None),
            self._node(3, None),
        ]
        # 2 nodes in flight (drained + terminating); cap of 2 means no slots.
        picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=2)
        assert picked == []
        # cap of 3 leaves 1 slot for a new wave.
        picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=3)
        assert {p.instance_id for p in picked} == {"i-2"}

    def test_cap_full_returns_empty(self) -> None:
        source = [
            self._node(0, rollout.DrainPhase.DRAINING),
            self._node(1, rollout.DrainPhase.DRAINING),
            self._node(2, None),
        ]
        picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=2)
        assert picked == []

    def test_skips_nodes_with_no_admin_row(self) -> None:
        """A source instance not yet registered with SPACE cannot be drained.

        It will be skipped this tick and picked up once registration catches
        up — keeps the picker idempotent against transient races.
        """

        source = [make_node_status(instance_id="i-0", admin=None), self._node(1, None)]
        picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=2)
        assert [p.instance_id for p in picked] == ["i-1"]

    def test_stuck_node_does_not_consume_drain_slot(self) -> None:
        """A node stuck in draining past --stuck-threshold must not deadlock
        the wave at concurrency caps of 1."""

        stuck = self._node(0, rollout.DrainPhase.DRAINING)
        fresh = self._node(1, None)
        source = [stuck, fresh]
        stuck_ids = frozenset({stuck.admin.id})
        picked = rollout.pick_drain_candidates(
            source, max_concurrent_node_drains=1, stuck_node_ids=stuck_ids
        )
        assert [p.instance_id for p in picked] == ["i-1"]

    def test_recovered_stuck_node_counts_toward_concurrency_cap(self) -> None:
        """The stuck-node exclusion is scoped to phase=DRAINING. Once a
        previously-stuck node progresses to drained/terminating, its
        evacuated sandboxes are running on target nodes and still occupying
        target capacity, so it must count against the cap again — otherwise
        a sticky `stuck_logged` set would let the picker overrun `H` once
        the stuck node recovered.
        """

        recovered = self._node(0, rollout.DrainPhase.DRAINED)
        fresh = self._node(1, None)
        source = [recovered, fresh]
        # stuck_logged is sticky — `recovered.admin.id` is still in the set
        # even though the node has progressed past DRAINING.
        stuck_ids = frozenset({recovered.admin.id})
        picked = rollout.pick_drain_candidates(
            source, max_concurrent_node_drains=1, stuck_node_ids=stuck_ids
        )
        # `recovered` consumes the slot; `fresh` is gated.
        assert picked == []

    def test_recovered_stuck_terminating_counts_toward_concurrency_cap(
        self,
    ) -> None:
        """Same as above but with the recovered node in `terminating` —
        the ASG decrement hasn't landed yet, so target capacity is still
        held."""

        recovered = self._node(0, rollout.DrainPhase.TERMINATING)
        fresh = self._node(1, None)
        source = [recovered, fresh]
        stuck_ids = frozenset({recovered.admin.id})
        picked = rollout.pick_drain_candidates(
            source, max_concurrent_node_drains=1, stuck_node_ids=stuck_ids
        )
        assert picked == []

    def test_already_drained_node_counts_toward_concurrency_cap(self) -> None:
        """Until the admin API reflects a freshly-issued drain, the node we
        POSTed `:drain` to still reads as phase=none. It must consume an
        in-flight slot anyway — otherwise the picker would overshoot the cap."""

        just_drained = self._node(0, None)  # snapshot lags; we POSTed already
        fresh = self._node(1, None)
        picked = rollout.pick_drain_candidates(
            [just_drained, fresh],
            max_concurrent_node_drains=1,
            already_drained_ids=frozenset({just_drained.admin.id}),
        )
        assert picked == []

    def test_already_drained_filter_blocks_redrain(self) -> None:
        """A node we've already POSTed :drain to must not be picked again
        even when the snapshot still shows phase=None — the admin endpoint
        returns 409 on repeat, not idempotent success."""

        fresh = self._node(0, None)
        racing = self._node(1, None)  # snapshot says none, but we drained it
        picked = rollout.pick_drain_candidates(
            [fresh, racing],
            max_concurrent_node_drains=2,
            already_drained_ids=frozenset({racing.admin.id}),
        )
        assert [p.instance_id for p in picked] == ["i-0"]

    def test_deterministic_pick_order(self) -> None:
        """Picks are reproducible regardless of input order — ASG paginator
        response order is not stable, so we sort by instance_id."""

        source_a = [self._node(2, None), self._node(0, None), self._node(1, None)]
        source_b = [self._node(1, None), self._node(2, None), self._node(0, None)]
        picked_a = rollout.pick_drain_candidates(source_a, max_concurrent_node_drains=2)
        picked_b = rollout.pick_drain_candidates(source_b, max_concurrent_node_drains=2)
        assert [p.instance_id for p in picked_a] == ["i-0", "i-1"]
        assert [p.instance_id for p in picked_a] == [p.instance_id for p in picked_b]

    def test_az_balanced_drain_keeps_source_asg_flat(self) -> None:
        """A 12-instance ASG balanced 4-4-4 across three AZs, drained 6 nodes
        deep at concurrency 2 across 3 waves, must end at 2-2-2 — not 0-2-4
        or any other lopsided shape. A purely instance_id-sorted picker
        would drain a contiguous run from one AZ first, which is exactly
        what triggers ASG AZRebalance to launch replacements in the
        depleted AZ and breaks the rollout's monotonic shrink invariant.
        """

        from collections import Counter

        azs = ["us-east-1a", "us-east-1b", "us-east-1c"]
        # Cluster instance IDs by AZ (i-000..i-003 = 1a, i-004..i-007 = 1b,
        # ...) rather than round-robin, so the IDs are *correlated* with AZ.
        # An AZ-blind picker that sorts by instance_id would then drain a
        # contiguous run from 1a first (ending 0-2-4, not 2-2-2) and fail this
        # test — which is the regression we're pinning.
        source = [self._az_node(idx, azs[idx // 4]) for idx in range(12)]
        # Sanity: starts 4-4-4.
        assert Counter(n.availability_zone for n in source) == {az: 4 for az in azs}

        for _ in range(3):
            picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=2)
            assert len(picked) == 2
            # Simulate the terminate landing: the picked instances exit the
            # ASG before the next wave's picker call. This is the
            # steady-state at concurrency=2: a wave fully completes before
            # the next one fires.
            picked_ids = {p.instance_id for p in picked}
            source = [n for n in source if n.instance_id not in picked_ids]

        final = Counter(n.availability_zone for n in source)
        assert final == {az: 2 for az in azs}, f"AZ balance drifted; got {final!r}"

    def test_az_spread_never_exceeds_one_across_full_roll_at_k1(self) -> None:
        """At K=1 (one drain in flight at a time) the picker keeps the live
        InService set within spread-1 for the *entire* roll. With only one
        in-flight drain, a node leaves the ASG before the next is picked, and
        always picking from the largest remaining AZ keeps the per-AZ counts
        within 1 at every step — which is the property that keeps the source
        ASG out of AZRebalance's reach (it fires when an AZ is over-represented
        by more than 1), so at K=1 the manual AZRebalance suspension is
        genuinely optional. At K>1 the picker is only approximately balanced
        (see test_uneven_counts_pick_same_az_twice_in_one_wave), which is why
        the runbook keeps the suspend as a recommended step.
        """

        from collections import Counter

        azs = ["us-east-1a", "us-east-1b", "us-east-1c"]
        # IDs clustered by AZ (i-000..i-005 = 1a, etc.), not round-robin, so an
        # AZ-blind instance_id sort would drain all of 1a first and blow the
        # spread past 1 immediately — the regression this pins.
        source = [self._az_node(idx, azs[idx // 6]) for idx in range(18)]

        def spread(nodes: list[Any]) -> int:
            counts = Counter(n.availability_zone for n in nodes)
            # Include AZs that have drained to zero so a fully-emptied AZ
            # still counts as 0 against the others.
            per_az = [counts.get(az, 0) for az in azs]
            return max(per_az) - min(per_az)

        assert spread(source) == 0
        while source:
            picked = rollout.pick_drain_candidates(
                source, max_concurrent_node_drains=1
            )
            assert len(picked) == 1, "K=1 must pick exactly one node per tick"
            # One in-flight drain at a time: the node leaves the ASG before the
            # next pick.
            picked_ids = {p.instance_id for p in picked}
            source = [n for n in source if n.instance_id not in picked_ids]
            assert spread(source) <= 1, (
                "per-AZ spread exceeded 1 mid-roll; got "
                f"{Counter(n.availability_zone for n in source)!r}"
            )

    def test_uneven_counts_pick_same_az_twice_in_one_wave(self) -> None:
        """A wave can draw two nodes from the same AZ even when the ASG starts
        balanced-within-1, so K>1 cannot guarantee the live set stays within
        spread-1. With 5-5-4 and K=3 the greedy picker selects 1a, 1b, then 1a
        again (1a and 1b tie at 5, lex picks 1a; after 1a drops to 4 and 1b is
        taken, 1a ties at 4 with 1c... actually 1a=4,1b=4,1c=4 on the third
        round, lex picks 1a). If both 1a picks terminate before the 1b pick,
        the live set transiently skews to 3-5-4 (spread 2) and AZRebalance can
        fire — and at the K=10 throughput rate the per-wave same-AZ draw (and
        thus the skew) can be larger. This is why the runbook keeps the manual
        AZRebalance suspend as a recommended best-practice step for K>1 rolls,
        rather than relying on the picker alone.
        """

        from collections import Counter

        layout = {"us-east-1a": 5, "us-east-1b": 5, "us-east-1c": 4}
        source: list[Any] = []
        idx = 0
        for az, count in layout.items():
            for _ in range(count):
                source.append(self._az_node(idx, az))
                idx += 1

        picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=3)
        az_counts = Counter(p.availability_zone for p in picked)
        assert len(picked) == 3
        # The key limitation: one AZ contributes two nodes to a single wave.
        assert max(az_counts.values()) == 2, (
            f"expected a wave to draw two from one AZ; got {dict(az_counts)!r}"
        )

    def test_az_picker_converges_from_imbalanced_start(self) -> None:
        """An already-imbalanced source ASG (5-3-1) must drain its
        over-represented AZ first so the remaining set moves toward balance,
        never away from it. The greedy 'largest bucket' rule should pull
        every early pick from the 5-node AZ until it stops being the largest.
        """

        from collections import Counter

        layout = {"us-east-1a": 5, "us-east-1b": 3, "us-east-1c": 1}
        # Assign the *lowest* instance IDs to the *smallest* AZ (1c first, then
        # 1b, then 1a), so instance_id order is anti-correlated with AZ size.
        # An AZ-blind picker that sorts by instance_id would drain 1c/1b first
        # (making the imbalance worse) and fail the assertion below; the
        # AZ-aware picker drains the largest AZ (1a) first instead.
        source: list[Any] = []
        idx = 0
        for az in sorted(layout, key=layout.get):
            for _ in range(layout[az]):
                source.append(self._az_node(idx, az))
                idx += 1

        # First wave of 3 must come entirely from the 5-node AZ: it's the
        # largest bucket and stays the greedy winner for all three rounds
        # (5->4->3, with 1b at 3 only tying on the third pick and losing the
        # lex tie-break to "us-east-1a").
        picked = rollout.pick_drain_candidates(source, max_concurrent_node_drains=3)
        assert [p.availability_zone for p in picked] == ["us-east-1a"] * 3
        picked_ids = {p.instance_id for p in picked}
        source = [n for n in source if n.instance_id not in picked_ids]
        remaining = Counter(n.availability_zone for n in source)
        assert remaining == {"us-east-1a": 2, "us-east-1b": 3, "us-east-1c": 1}
        # Max-min spread shrank from 4 (5-1) to 2 (3-1): moved toward balance.
        assert (max(remaining.values()) - min(remaining.values())) == 2


class TestTerminateRefusal:
    """`AdminApi.terminate` turns a 409 into a return value, not an exception.

    The endpoint is the authoritative occupancy check, so "this node is blocked"
    is an expected per-node answer rather than a wave-level failure. Raising
    would abort the rest of the terminate wave through the caller's
    broad-except — one blocked node stalling every other node's terminate.
    """

    class _Resp:
        def __init__(self, status_code: int, body: Any = None) -> None:
            self.status_code = status_code
            self._body = body

        def json(self) -> Any:
            if self._body is None:
                raise ValueError("no body")
            return self._body

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise AssertionError(
                    f"raise_for_status must not fire for {self.status_code}"
                )

    def _api(self, resp: Any) -> Any:
        api = rollout.AdminApi(base_url="http://x", token="t", dry_run=False)
        api.session = type("S", (), {"post": lambda _self, *a, **k: resp})()
        return api

    def test_409_returns_the_body_instead_of_raising(self) -> None:
        body = {"sandboxes_active": 2, "sandboxes_local_snapshots": 0}
        refusal = self._api(self._Resp(409, body)).terminate("n-1")
        assert refusal == body

    def test_409_without_a_json_body_still_reports_a_refusal(self) -> None:
        """A refusal must be distinguishable from success even if the body is
        unparseable — returning `None` there would read as 'terminated'."""

        assert self._api(self._Resp(409)).terminate("n-1") == {}

    def test_the_three_409_bodies_are_distinguishable(self) -> None:
        """The endpoint has *three* 409 bodies, and the caller classifies on which
        key is present rather than which is absent.

        A node can leave `drained` between the tick's listing poll and this POST
        — the cluster's own `drained_node_terminator` flips it, or an operator
        undrains — and that body carries no occupancy keys. Reporting it as an
        active-sandbox block would point an operator at the wrong runbook entry
        for a node with no sandbox problem, so the caller keys on the body shape.
        """

        # 1. The transition check, which runs *before* occupancy is read.
        transition = {
            "error": "drain-phase transition not allowed",
            "current_drain_phase": "terminating",
            "allowed_from": ["drained"],
        }
        refusal = self._api(self._Resp(409, transition)).terminate("n-1")
        assert refusal == transition
        assert "current_drain_phase" in refusal

        # 2. The occupancy guard.
        occupancy = {
            "error": "node still holds sandboxes",
            "sandboxes_active": 2,
            "sandboxes_local_snapshots": 0,
        }
        refusal = self._api(self._Resp(409, occupancy)).terminate("n-1")
        assert "sandboxes_active" in refusal
        assert "current_drain_phase" not in refusal

        # 3. `db_error` mapping a Postgres serialization failure or deadlock
        #    (40001 / 40P01) to a 409. Neither key — which is why an
        #    absence test on `sandboxes_active` alone would file this under
        #    phase drift and point an operator at the wrong runbook entry.
        retryable = {
            "error": "operation conflicted with a concurrent admin write; retry the request"
        }
        refusal = self._api(self._Resp(409, retryable)).terminate("n-1")
        assert "sandboxes_active" not in refusal
        assert "current_drain_phase" not in refusal

    def test_success_returns_none(self) -> None:
        assert self._api(self._Resp(204)).terminate("n-1") is None

    def test_other_errors_still_raise(self) -> None:
        class Boom:
            status_code = 500

            def raise_for_status(self) -> None:
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            self._api(Boom()).terminate("n-1")


class TestPickTerminateCandidates:
    def _node(self, idx: int, phase: str | None) -> Any:
        admin = make_admin_node(
            node_id=f"00000000-0000-0000-0000-{idx:012d}",
            drain_phase=phase,
        )
        return make_node_status(instance_id=f"i-{idx}", admin=admin)

    def test_picks_drained_nodes(self) -> None:
        """The cluster controller's `drained_node_terminator` is disabled by
        default; the orchestrator must drive `drained -> terminating` itself
        or the rollout hangs after evacuation."""

        source = [
            self._node(0, rollout.DrainPhase.DRAINING),
            self._node(1, rollout.DrainPhase.DRAINED),
            self._node(2, rollout.DrainPhase.DRAINED),
            self._node(3, rollout.DrainPhase.TERMINATING),
            self._node(4, None),
        ]
        picked = rollout.pick_terminate_candidates(source)
        assert [p.instance_id for p in picked] == ["i-1", "i-2"]

    def test_excludes_already_terminated(self) -> None:
        a = self._node(0, rollout.DrainPhase.DRAINED)
        b = self._node(1, rollout.DrainPhase.DRAINED)
        picked = rollout.pick_terminate_candidates(
            [a, b],
            already_terminated_ids=frozenset({a.admin.id}),
        )
        assert [p.instance_id for p in picked] == ["i-1"]

    def test_deterministic_order(self) -> None:
        source_a = [self._node(2, rollout.DrainPhase.DRAINED), self._node(0, rollout.DrainPhase.DRAINED), self._node(1, rollout.DrainPhase.DRAINED)]
        picked = rollout.pick_terminate_candidates(source_a)
        assert [p.instance_id for p in picked] == ["i-0", "i-1", "i-2"]

    def test_does_not_gate_on_the_listings_active_count(self) -> None:
        """A drained node with `sandboxes_active > 0` in the *listing* must still
        be offered to the endpoint.

        The listing's `active` count is deliberately not census-aware while the
        `:terminate` endpoint's is, so filtering on it here would exclude nodes
        the endpoint would terminate — a worker whose census disproves thousands
        of phantom `active_node_id` rows would sit in `drained` forever without
        this script ever asking. The endpoint is authoritative; this picker only
        screens on the count that agrees with it (`local_snapshots`)."""

        phantom_admin = make_admin_node(
            node_id="a-phantom",
            drain_phase=rollout.DrainPhase.DRAINED,
            sandboxes_active=5392,
        )
        phantom = make_node_status(instance_id="i-0", admin=phantom_admin)
        ok = self._node(1, rollout.DrainPhase.DRAINED)

        picked = rollout.pick_terminate_candidates([phantom, ok])
        assert [p.instance_id for p in picked] == ["i-0", "i-1"], (
            "the listing's active count must not gate the terminate wave"
        )

    def test_excludes_local_snapshot_blocked(self) -> None:
        """Admin :terminate 409s when sandboxes_local_snapshots > 0. Without
        this filter the blocked node would sit at the head of the
        deterministic sort and 409 every tick, aborting the terminate wave
        via the outer broad-except."""

        blocked_admin = make_admin_node(
            node_id="a-blocked",
            drain_phase=rollout.DrainPhase.DRAINED,
        )
        blocked_admin = make_admin_node(
            node_id="a-blocked",
            drain_phase=rollout.DrainPhase.DRAINED,
            sandboxes_local_snapshots=3,
        )
        blocked = make_node_status(instance_id="i-0", admin=blocked_admin)
        ok = self._node(1, rollout.DrainPhase.DRAINED)
        picked = rollout.pick_terminate_candidates([blocked, ok])
        assert [p.instance_id for p in picked] == ["i-1"]

        blocker_list = rollout.pick_terminate_blocked_by_snapshots([blocked, ok])
        assert [n.instance_id for n in blocker_list] == ["i-0"]

    def test_excludes_non_decrement_drains(self) -> None:
        """Drained nodes whose drain was started without
        `replace_after_terminate=false` (so terminate_decrement_capacity is
        False) must not be terminated by the orchestrator — the source ASG
        would auto-replace and break the monotonic-shrink invariant."""

        replace_admin = make_admin_node(
            node_id="a-replace",
            drain_phase=rollout.DrainPhase.DRAINED,
            terminate_decrement_capacity=False,
        )
        replace_node = make_node_status(instance_id="i-0", admin=replace_admin)
        ok = self._node(1, rollout.DrainPhase.DRAINED)
        picked = rollout.pick_terminate_candidates([replace_node, ok])
        assert [p.instance_id for p in picked] == ["i-1"]

        blockers = rollout.pick_terminate_blocked_by_replace_semantics(
            [replace_node, ok]
        )
        assert [n.instance_id for n in blockers] == ["i-0"]


class TestSourceCounters:
    def test_source_remaining_live_counts_alive_states_only(self) -> None:
        nodes = [
            make_node_status(instance_id="i-1", lifecycle_state="InService"),
            make_node_status(instance_id="i-2", lifecycle_state="Pending"),
            make_node_status(instance_id="i-3", lifecycle_state="Terminating"),
            make_node_status(instance_id="i-4", lifecycle_state="Terminated"),
        ]
        assert rollout.source_remaining_live(nodes) == 2

    def test_source_active_excludes_drained_and_terminating(self) -> None:
        nodes = [
            make_node_status(
                instance_id="i-0",
                admin=make_admin_node(node_id="a-0", drain_phase=None),
            ),
            make_node_status(
                instance_id="i-1",
                admin=make_admin_node(node_id="a-1", drain_phase=rollout.DrainPhase.DRAINING),
            ),
            make_node_status(
                instance_id="i-2",
                admin=make_admin_node(node_id="a-2", drain_phase=rollout.DrainPhase.DRAINED),
            ),
            make_node_status(
                instance_id="i-3",
                admin=make_admin_node(node_id="a-3", drain_phase=rollout.DrainPhase.TERMINATING),
            ),
        ]
        assert rollout.source_active_to_migrate(nodes) == 2

    def test_source_active_excludes_pending_states(self) -> None:
        """Pending source nodes haven't joined K8s yet and don't host sandboxes,
        so they should not pull target capacity higher."""

        nodes = [
            make_node_status(
                instance_id="i-0",
                lifecycle_state="Pending",
                admin=make_admin_node(node_id="a-0", drain_phase=None),
            ),
            make_node_status(
                instance_id="i-1",
                lifecycle_state="Pending:Wait",
                admin=make_admin_node(node_id="a-1", drain_phase=None),
            ),
            make_node_status(
                instance_id="i-2",
                lifecycle_state="InService",
                admin=make_admin_node(node_id="a-2", drain_phase=None),
            ),
        ]
        assert rollout.source_active_to_migrate(nodes) == 1


class TestTargetReadyCount:
    def _t(
        self, *, k8s_ready: bool, drain_phase: str | None, has_admin: bool = True
    ) -> Any:
        admin = (
            make_admin_node(node_id=f"a-{id(self)}-{drain_phase}", drain_phase=drain_phase)
            if has_admin
            else None
        )
        return make_node_status(admin=admin, k8s_ready=k8s_ready)

    def test_counts_ready_undrained(self) -> None:
        nodes = [self._t(k8s_ready=True, drain_phase=None)]
        assert rollout.target_ready_count(nodes) == 1

    def test_excludes_draining_target(self) -> None:
        """A target node mid-drain is cordoned; the scheduler won't place
        migrated pods on it, so it can't count as landing capacity."""

        nodes = [
            self._t(k8s_ready=True, drain_phase=None),
            self._t(k8s_ready=True, drain_phase=rollout.DrainPhase.DRAINING),
            self._t(k8s_ready=True, drain_phase=rollout.DrainPhase.DRAINED),
        ]
        assert rollout.target_ready_count(nodes) == 1

    def test_excludes_not_ready(self) -> None:
        nodes = [self._t(k8s_ready=False, drain_phase=None)]
        assert rollout.target_ready_count(nodes) == 0

    def test_excludes_unregistered(self) -> None:
        nodes = [self._t(k8s_ready=True, drain_phase=None, has_admin=False)]
        assert rollout.target_ready_count(nodes) == 0


class TestComputeTargetDesired:
    def _mng(self, desired: int, max_size: int) -> Any:
        return rollout.MngState(
            name="target",
            cluster="example-eks-cluster",
            desired_size=desired,
            min_size=0,
            max_size=max_size,
            status="ACTIVE",
            asg_names=("eks-target-asg",),
        )

    def test_tick_zero_target_equals_headroom(self) -> None:
        """No terminates yet → target = H. Total = N + H invariant is
        satisfied: source.desired = N (unchanged), target = H."""

        assert (
            rollout.compute_target_desired(
                self._mng(desired=0, max_size=50),
                initial_source_desired=10,
                source_current_desired=10,
                headroom=2,
            )
            == 2
        )

    def test_target_grows_with_terminated_count(self) -> None:
        """After K terminates, target = headroom + K. Source has dropped
        from N to N-K; sum stays at N + H."""

        assert (
            rollout.compute_target_desired(
                self._mng(desired=2, max_size=50),
                initial_source_desired=10,
                source_current_desired=7,
                headroom=2,
            )
            == 5  # headroom 2 + terminated_count 3
        )

    def test_never_drops_below_current(self) -> None:
        """Monotonic non-decrease — if operator pre-warmed target higher
        than (headroom + terminated), keep the higher value."""

        assert (
            rollout.compute_target_desired(
                self._mng(desired=15, max_size=50),
                initial_source_desired=10,
                source_current_desired=5,
                headroom=2,
            )
            == 15
        )

    def test_end_state_target_equals_n_plus_h(self) -> None:
        """When source.desired = 0, target = N + H."""

        assert (
            rollout.compute_target_desired(
                self._mng(desired=10, max_size=50),
                initial_source_desired=10,
                source_current_desired=0,
                headroom=2,
            )
            == 12
        )

    def test_clamps_negative_terminated(self) -> None:
        """If source.desired > initial (operator manually scaled source up
        mid-rollout), clamp terminated at 0 — target = headroom."""

        assert (
            rollout.compute_target_desired(
                self._mng(desired=0, max_size=50),
                initial_source_desired=10,
                source_current_desired=12,
                headroom=2,
            )
            == 2
        )


class TestEmitPhaseTransitions:
    def test_emits_only_on_change(self, capsys: pytest.CaptureFixture[str]) -> None:
        admin = make_admin_node(node_id="a-1", drain_phase=rollout.DrainPhase.DRAINING)
        ns = make_node_status(instance_id="i-1", admin=admin)
        seen: dict[str, str] = {}

        rollout.emit_phase_transitions([ns], seen, mng="source", cohort="v3")
        rollout.emit_phase_transitions([ns], seen, mng="source", cohort="v3")
        out = capsys.readouterr().out.splitlines()
        assert len(out) == 1
        event = json.loads(out[0])
        assert event["event"] == "draining"
        assert event["from_phase"] is None
        assert event["to_phase"] == rollout.DrainPhase.DRAINING

    def test_terminated_on_disappearance(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        admin = make_admin_node(node_id="a-1", drain_phase=rollout.DrainPhase.TERMINATING)
        first = make_node_status(instance_id="i-1", admin=admin)
        gone = make_node_status(instance_id="i-1", admin=None)
        seen: dict[str, str] = {}

        rollout.emit_phase_transitions([first], seen, mng="source", cohort="v3")
        capsys.readouterr()
        # Second call: admin row gone, but emit_phase_transitions skips nodes
        # with admin=None (we can't correlate by admin.id once the row is
        # deleted). The 'terminated' event is fired by the row going away on
        # the API side; we rely on the prior 'terminating' event as the
        # operator-visible end-state.
        rollout.emit_phase_transitions([gone], seen, mng="source", cohort="v3")
        out = capsys.readouterr().out
        assert out == ""


class TestDetectStuck:
    def test_logs_once_past_threshold(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        old_start = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=1)
        admin = make_admin_node(
            node_id="a-1",
            drain_phase=rollout.DrainPhase.DRAINING,
            drain_started_at=old_start,
            sandboxes_active=4,
        )
        ns = make_node_status(instance_id="i-1", admin=admin)
        logged: set[str] = set()

        rollout.detect_stuck([ns], threshold_sec=60, logged=logged, cohort="v3")
        rollout.detect_stuck([ns], threshold_sec=60, logged=logged, cohort="v3")
        out = capsys.readouterr().out.splitlines()
        assert len(out) == 1
        event = json.loads(out[0])
        assert event["event"] == "node_stuck"
        assert event["sandboxes_active"] == 4

    def test_not_logged_when_under_threshold(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        recent = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(seconds=10)
        admin = make_admin_node(
            node_id="a-1",
            drain_phase=rollout.DrainPhase.DRAINING,
            drain_started_at=recent,
        )
        ns = make_node_status(instance_id="i-1", admin=admin)
        rollout.detect_stuck([ns], threshold_sec=300, logged=set(), cohort="v3")
        assert capsys.readouterr().out == ""

    def test_ignores_non_draining(self, capsys: pytest.CaptureFixture[str]) -> None:
        old_start = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(hours=1)
        admin = make_admin_node(
            node_id="a-1",
            drain_phase=rollout.DrainPhase.DRAINED,
            drain_started_at=old_start,
        )
        ns = make_node_status(instance_id="i-1", admin=admin)
        rollout.detect_stuck([ns], threshold_sec=60, logged=set(), cohort="v3")
        assert capsys.readouterr().out == ""


class TestParseRfc3339:
    def test_zulu_to_utc(self) -> None:
        parsed = rollout.parse_rfc3339("2026-05-15T12:00:00Z")
        assert parsed is not None
        assert parsed.tzinfo is not None
        assert parsed == dt.datetime(2026, 5, 15, 12, 0, 0, tzinfo=dt.timezone.utc)

    def test_offset_preserved(self) -> None:
        parsed = rollout.parse_rfc3339("2026-05-15T12:00:00+02:00")
        assert parsed is not None
        assert parsed.utcoffset() == dt.timedelta(hours=2)

    @pytest.mark.parametrize("value", [None, "", "not-a-time"])
    def test_invalid_returns_none(self, value: str | None) -> None:
        assert rollout.parse_rfc3339(value) is None


class TestReconcileMngXxx:
    def _mng(self, *, min_size: int, desired: int, max_size: int) -> Any:
        return rollout.MngState(
            name="workers",
            cluster="example-eks-cluster",
            desired_size=desired,
            min_size=min_size,
            max_size=max_size,
            status="ACTIVE",
            asg_names=("eks-workers-asg",),
        )

    def test_noop_when_already_xxx(self, capsys: pytest.CaptureFixture[str]) -> None:
        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.reconcile_mng_xxx(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            "source",
            self._mng(min_size=5, desired=5, max_size=5),
            dry_run=False,
        )
        assert calls == []
        assert capsys.readouterr().out == ""

    def test_collapses_to_desired_when_drift(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Source MNG after a wave fully terminates: ASG decremented `desired`
        to 4, `min` was pre-lowered to 3, `max` still 5; no in-flight
        drains remain → reconcile pulls everything to 4/4/4."""

        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.reconcile_mng_xxx(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            "source",
            self._mng(min_size=3, desired=4, max_size=5),
            dry_run=False,
        )
        assert len(calls) == 1
        assert calls[0]["scalingConfig"] == {
            "minSize": 4,
            "desiredSize": 4,
            "maxSize": 4,
        }
        out = capsys.readouterr().out
        assert "mng_reconciled_xxx" in out
        event = json.loads(out.splitlines()[0])
        assert event["from_min"] == 3
        assert event["from_desired"] == 4
        assert event["from_max"] == 5
        assert event["to_min"] == 4
        assert event["to_desired"] == 4
        assert event["to_max"] == 4
        assert event["pending_decrements"] == 0
        assert event["role"] == "source"

    def test_holds_min_below_desired_when_decrements_pending(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Mid-wave: source has 2 nodes whose `:terminate` call is in flight
        but the ASG decrement hasn't landed yet. Reconcile must keep
        `min = desired - 2` so each pending terminate-with-decrement is
        still valid; collapsing to strict x/x/x would block them."""

        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.reconcile_mng_xxx(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            "source",
            self._mng(min_size=3, desired=5, max_size=5),
            dry_run=False,
            pending_decrements=2,
        )
        # min already at desired - pending (3 == 5 - 2) and max == desired,
        # so this is a no-op.
        assert calls == []
        assert capsys.readouterr().out == ""

    def test_lowers_min_when_pending_grows_beyond_current_min(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """If `pending_decrements` exceeds `desired - min`, the reconcile
        lowers `min` further so all pending terminates can decrement."""

        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.reconcile_mng_xxx(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            "source",
            self._mng(min_size=5, desired=5, max_size=5),
            dry_run=False,
            pending_decrements=2,
        )
        assert len(calls) == 1
        assert calls[0]["scalingConfig"] == {
            "minSize": 3,
            "desiredSize": 5,
            "maxSize": 5,
        }

    def test_dry_run_does_not_mutate(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.reconcile_mng_xxx(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            "target",
            self._mng(min_size=0, desired=2, max_size=5),
            dry_run=True,
        )
        assert calls == []
        out = capsys.readouterr().out.splitlines()
        # Two events: the reconciled-xxx event, then the dry-run set-scaling
        events = [json.loads(line)["event"] for line in out]
        assert "mng_reconciled_xxx" in events
        assert "dry_run_set_scaling" in events

    def test_skips_when_desired_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        """At end of source roll, desired=0 but max stays at the pre-wave
        value. EKS rejects maxSize<1 on update, so reconcile must skip
        rather than try to collapse to 0/0/0. Cleanup PR handles the
        final state (delete or hold-at-zero with max>=1)."""

        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.reconcile_mng_xxx(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            "source",
            self._mng(min_size=0, desired=0, max_size=5),
            dry_run=False,
        )
        assert calls == []
        assert capsys.readouterr().out == ""


class TestPrepareSourceForWave:
    def _mng(self, *, min_size: int, desired: int, max_size: int) -> Any:
        return rollout.MngState(
            name="workers-old",
            cluster="example-eks-cluster",
            desired_size=desired,
            min_size=min_size,
            max_size=max_size,
            status="ACTIVE",
            asg_names=("eks-workers-asg",),
        )

    def test_noop_for_empty_wave(self) -> None:
        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.prepare_source_for_wave(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            self._mng(min_size=5, desired=5, max_size=5),
            wave_size=0,
            pending_decrements=0,
            dry_run=False,
        )
        assert calls == []

    def test_lowers_min_to_allow_terminates(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Source at 5/5/5, wave of 2 → min lowered to 3 so ASG can decrement
        twice."""

        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.prepare_source_for_wave(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            self._mng(min_size=5, desired=5, max_size=5),
            wave_size=2,
            pending_decrements=0,
            dry_run=False,
        )
        assert len(calls) == 1
        assert calls[0]["scalingConfig"] == {
            "minSize": 3,
            "desiredSize": 5,
            "maxSize": 5,
        }
        event = json.loads(capsys.readouterr().out.splitlines()[0])
        assert event["event"] == "source_prepared_for_wave"
        assert event["from_min"] == 5
        assert event["to_min"] == 3
        assert event["wave_size"] == 2
        assert event["pending_decrements"] == 0

    def test_accounts_for_existing_pending_decrements(self) -> None:
        """Source at 3/5/5 with 2 already-pending decrements (post-reconcile
        state). New wave of 1 → min lowered further to 5 - 2 - 1 = 2."""

        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.prepare_source_for_wave(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            self._mng(min_size=3, desired=5, max_size=5),
            wave_size=1,
            pending_decrements=2,
            dry_run=False,
        )
        assert len(calls) == 1
        assert calls[0]["scalingConfig"]["minSize"] == 2

    def test_clamps_at_zero(self) -> None:
        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.prepare_source_for_wave(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            self._mng(min_size=2, desired=2, max_size=2),
            wave_size=5,
            pending_decrements=0,
            dry_run=False,
        )
        assert calls[0]["scalingConfig"]["minSize"] == 0

    def test_noop_when_min_already_low_enough(self) -> None:
        """If `min` is already <= `desired - pending - wave_size`, no API
        call needed (the reconcile already accounted for prior in-flight
        and this wave is small enough not to push min lower)."""

        calls: list[dict[str, Any]] = []

        class FakeEks:
            def update_nodegroup_config(self, **kwargs: Any) -> None:
                calls.append(kwargs)

        rollout.prepare_source_for_wave(
            FakeEks(),
            "example-eks-cluster",
            "v3",
            self._mng(min_size=0, desired=5, max_size=5),
            wave_size=2,
            pending_decrements=0,
            dry_run=False,
        )
        assert calls == []


class TestArgParsing:
    def test_headroom_defaults_to_max_concurrent(self) -> None:
        """`H = k` is the minimum that satisfies the `1 <= k <= H`
        cluster-capacity invariant — each wave's `k` drains have
        exactly `k` free target slots, no pipeline buffer. Operators
        who want extra throughput buffer pass --headroom-count
        explicitly."""

        cfg = rollout.parse_args(
            [
                "--eks-cluster-name", "example-eks-cluster",
                "--k8s-context", "example-eks-cluster",
                "--source-mng", "workers-old",
                "--target-mng", "workers-new",
                "--cohort", "v3",
                "--space-api", "https://example.invalid",
                "--aws-region", "us-east-1",
                "--max-concurrent-node-drains", "3",
                "--initial-source-desired-size", "10",
                "--rollout-kind", "lt",
            ]
        )
        assert cfg.headroom_count == 3
        assert cfg.token_env == "SPACE_API_TOKEN"

    def test_explicit_headroom_wins(self) -> None:
        cfg = rollout.parse_args(
            [
                "--eks-cluster-name", "example-eks-cluster",
                "--k8s-context", "example-eks-cluster",
                "--source-mng", "workers-old",
                "--target-mng", "workers-new",
                "--cohort", "v3",
                "--space-api", "https://example.invalid",
                "--aws-region", "us-east-1",
                "--max-concurrent-node-drains", "2",
                "--headroom-count", "3",
                "--initial-source-desired-size", "10",
                "--rollout-kind", "lt",
            ]
        )
        assert cfg.headroom_count == 3
        assert cfg.max_concurrent_node_drains == 2

    def test_requires_initial_source_desired_size(self) -> None:
        """`--initial-source-desired-size` is mandatory on every invocation.
        Auto-deriving N from current source.desired is unsafe in the
        terminate-then-interrupt window where source has already shrunk."""

        with pytest.raises(SystemExit):
            rollout.parse_args(
                [
                    "--eks-cluster-name", "example-eks-cluster",
                    "--k8s-context", "example-eks-cluster",
                    "--source-mng", "workers-old",
                    "--target-mng", "workers-new",
                    "--cohort", "v3",
                    "--space-api", "https://example.invalid",
                    "--aws-region", "us-east-1",
                ]
            )

    def test_rejects_headroom_below_max_concurrent(self) -> None:
        """`k > H` would leave drain waves with no free target slots; the
        cluster-capacity invariant requires `1 <= k <= H`.

        `--initial-source-desired-size` is supplied so the argparse
        missing-required check doesn't short-circuit before this
        invariant runs.
        """

        with pytest.raises(SystemExit):
            rollout.parse_args(
                [
                    "--eks-cluster-name", "example-eks-cluster",
                    "--k8s-context", "example-eks-cluster",
                    "--source-mng", "workers-old",
                    "--target-mng", "workers-new",
                    "--cohort", "v3",
                    "--space-api", "https://example.invalid",
                    "--aws-region", "us-east-1",
                    "--max-concurrent-node-drains", "3",
                    "--headroom-count", "2",
                    "--initial-source-desired-size", "10",
                ]
            )

    def test_rejects_identical_source_and_target_mng(self) -> None:
        """Identical MNGs would let target Ready be satisfied by the very
        nodes being drained, and `replace_after_terminate=false` would
        shrink the only MNG the operator has."""

        with pytest.raises(SystemExit):
            rollout.parse_args(
                [
                    "--eks-cluster-name", "example-eks-cluster",
                    "--k8s-context", "example-eks-cluster",
                    "--source-mng", "workers",
                    "--target-mng", "workers",
                    "--cohort", "v3",
                    "--space-api", "https://example.invalid",
                    "--aws-region", "us-east-1",
                    "--initial-source-desired-size", "10",
                ]
            )

    def test_rejects_zero_concurrency(self) -> None:
        with pytest.raises(SystemExit):
            rollout.parse_args(
                [
                    "--eks-cluster-name", "c",
                    "--k8s-context", "c",
                    "--source-mng", "s",
                    "--target-mng", "t",
                    "--cohort", "v3",
                    "--space-api", "https://example.invalid",
                    "--aws-region", "us-east-1",
                    "--max-concurrent-node-drains", "0",
                    "--initial-source-desired-size", "10",
                ]
            )

    def test_requires_k8s_context(self) -> None:
        """`--k8s-context` is mandatory — defaulting to the current kube
        context risks acting on the wrong cluster."""

        with pytest.raises(SystemExit):
            rollout.parse_args(
                [
                    "--eks-cluster-name", "example-eks-cluster",
                    "--source-mng", "workers-old",
                    "--target-mng", "workers-new",
                    "--cohort", "v3",
                    "--space-api", "https://example.invalid",
                    "--aws-region", "us-east-1",
                ]
            )


# --- Per-target-node automatic untaint --------------------------------------


def _kubectl_node_blob(items: list[dict]) -> dict:
    return {"items": items}


def _node_item(
    name: str,
    *,
    ready: bool = True,
    taints: list[dict] | None = None,
) -> dict:
    return {
        "metadata": {"name": name},
        "status": {
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True" if ready else "False",
                },
            ],
        },
        "spec": {"taints": list(taints or [])},
    }


def _cohort_taint() -> dict:
    return {
        "key": rollout.DEFAULT_COHORT_TAINT_KEY,
        "value": "1",
        "effect": "NoSchedule",
    }


class _FakeProc:
    def __init__(self, *, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TestKubectlTargetNodesByTaint:
    def test_partitions_nodes_into_three_sets(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        blob = _kubectl_node_blob([
            _node_item("ready-tainted", ready=True, taints=[_cohort_taint()]),
            _node_item("ready-untainted", ready=True, taints=[]),
            _node_item("notready-tainted", ready=False, taints=[_cohort_taint()]),
            _node_item("notready-untainted", ready=False, taints=[]),
            _node_item("ready-other-taint", ready=True, taints=[
                {"key": "other.key/foo", "value": "bar", "effect": "NoSchedule"},
            ]),
        ])

        captured: dict[str, Any] = {}

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            captured["cmd"] = cmd
            return _FakeProc(stdout=json.dumps(blob))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        ready_tainted, ready_untainted, not_ready_untainted = (
            rollout.kubectl_target_nodes_by_taint("ctx", "workers-1", rollout.DEFAULT_COHORT_TAINT_KEY)
        )

        assert ready_tainted == {"ready-tainted"}
        # `ready-other-taint` has a non-cohort taint, treated as
        # untainted-from-the-cohort's-perspective.
        assert ready_untainted == {"ready-untainted", "ready-other-taint"}
        # NotReady untainted goes into its own bucket so the resume seed
        # can include it even though the per-tick untaint skips it.
        assert not_ready_untainted == {"notready-untainted"}
        # NotReady+tainted is dropped entirely: the per-tick loop only
        # operates on Ready+tainted, and the resume seed only cares about
        # untainted nodes.
        # Confirms the kubectl call scoped to the target MNG via label selector.
        assert "-l" in captured["cmd"]
        idx = captured["cmd"].index("-l")
        assert captured["cmd"][idx + 1] == "eks.amazonaws.com/nodegroup=workers-1"
        assert "--context" in captured["cmd"]
        assert captured["cmd"][captured["cmd"].index("--context") + 1] == "ctx"

    def test_empty_when_no_nodes_match(self, monkeypatch: pytest.MonkeyPatch) -> None:
        blob = _kubectl_node_blob([])

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(stdout=json.dumps(blob))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        assert rollout.kubectl_target_nodes_by_taint("ctx", "workers-1", rollout.DEFAULT_COHORT_TAINT_KEY) == (
            set(),
            set(),
            set(),
        )

    def test_missing_taints_field_is_treated_as_untainted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Kubernetes omits `spec.taints` entirely when there are none.
        blob = {
            "items": [
                {
                    "metadata": {"name": "n1"},
                    "status": {"conditions": [
                        {"type": "Ready", "status": "True"},
                    ]},
                    "spec": {},
                }
            ]
        }

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(stdout=json.dumps(blob))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        ready_tainted, ready_untainted, not_ready_untainted = (
            rollout.kubectl_target_nodes_by_taint("ctx", "workers-1", rollout.DEFAULT_COHORT_TAINT_KEY)
        )
        assert ready_tainted == set()
        assert ready_untainted == {"n1"}
        assert not_ready_untainted == set()

    def test_not_ready_untainted_separated_from_ready(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Resume seed must include NotReady untainted nodes — otherwise a
        node that was untainted by a prior tick but is currently NotReady
        is missed, the budget under-counts, and the next tick over-untaints
        once the node returns to Ready."""

        blob = _kubectl_node_blob([
            _node_item("ready-untainted", ready=True, taints=[]),
            _node_item("notready-untainted", ready=False, taints=[]),
        ])

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(stdout=json.dumps(blob))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        ready_tainted, ready_untainted, not_ready_untainted = (
            rollout.kubectl_target_nodes_by_taint("ctx", "workers-1", rollout.DEFAULT_COHORT_TAINT_KEY)
        )
        # Resume seed = ready_untainted | not_ready_untainted, two nodes.
        assert ready_tainted == set()
        assert ready_untainted == {"ready-untainted"}
        assert not_ready_untainted == {"notready-untainted"}


class TestKubectlRemoveCohortTaint:
    def test_success_invokes_kubectl_with_trailing_dash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            captured["cmd"] = cmd
            return _FakeProc(returncode=0)

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        rollout.kubectl_remove_cohort_taint("ctx", "ip-10-0-0-1", rollout.DEFAULT_COHORT_TAINT_KEY)

        assert captured["cmd"][:4] == ["kubectl", "taint", "nodes", "ip-10-0-0-1"]
        assert f"{rollout.DEFAULT_COHORT_TAINT_KEY}-" in captured["cmd"]
        assert captured["cmd"][captured["cmd"].index("--context") + 1] == "ctx"

    def test_custom_taint_key_threaded_through_to_kubectl(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The TF module exposes `cohort_taint_key` as an input; the script
        must honor a non-default key end-to-end or the untaint loop will
        target the wrong taint and leave the target pool fenced."""

        captured: dict[str, Any] = {}

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            captured["cmd"] = cmd
            return _FakeProc(returncode=0)

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        rollout.kubectl_remove_cohort_taint(
            "ctx", "ip-10-0-0-1", "custom.example.com/cohort"
        )

        assert "custom.example.com/cohort-" in captured["cmd"]
        assert f"{rollout.DEFAULT_COHORT_TAINT_KEY}-" not in captured["cmd"]

    def test_swallows_not_found_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`kubectl taint <node> <key>-` errors with 'not found' when the
        taint is already absent. That's our intended post-condition, so we
        treat it as success rather than blowing up the rollout loop."""

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(
                returncode=1,
                stderr='error: taint "space.pplx.net/rollout-cohort" not found',
            )

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        # Should not raise.
        rollout.kubectl_remove_cohort_taint("ctx", "ip-10-0-0-1", rollout.DEFAULT_COHORT_TAINT_KEY)

    def test_raises_on_unexpected_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import subprocess as _subprocess

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(returncode=1, stderr="Unauthorized")

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        with pytest.raises(_subprocess.CalledProcessError):
            rollout.kubectl_remove_cohort_taint("ctx", "ip-10-0-0-1", rollout.DEFAULT_COHORT_TAINT_KEY)

    def test_raises_on_node_not_found(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If the Node object has been GC'd between listing and untaint,
        kubectl returns `Error from server (NotFound): nodes "<name>" not
        found`. That contains "not found" but NOT "taint", so it must NOT
        be swallowed — the script depends on the untaint actually removing
        the taint (or the node being present) to keep the untaint
        bookkeeping honest."""

        import subprocess as _subprocess

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(
                returncode=1,
                stderr='Error from server (NotFound): nodes "ip-10-0-0-1" not found',
            )

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        with pytest.raises(_subprocess.CalledProcessError):
            rollout.kubectl_remove_cohort_taint("ctx", "ip-10-0-0-1", rollout.DEFAULT_COHORT_TAINT_KEY)


class TestUntaintBudgetCap:
    """Regression coverage for the per-tick untaint budget.

    The script's main loop takes the minimum of two independent caps:

      (A) headroom-preserving:    tainted_ready_count - headroom
      (B) terminate-driven:       source_terminated_total - already_untainted

    Both must hold simultaneously. (A) closes the mid-wave-terminate and
    target-provisioning-lag windows where (B) alone would over-untaint;
    (B) closes the oversized-prewarm window (notably mirror rollback,
    where the rollback target is scaled up to `ROLLBACK_N >> H` tainted
    Ready nodes before any source terminate) where (A) alone would
    over-untaint.
    """

    @staticmethod
    def _budget(
        tainted_ready_count: int,
        headroom: int,
        source_terminated_total: int = 0,
        already_untainted: int = 0,
    ) -> int:
        # Mirror the inline expression in `run_loop`.
        headroom_cap = tainted_ready_count - headroom
        terminate_cap = source_terminated_total - already_untainted
        return max(0, min(headroom_cap, terminate_cap))

    def test_no_budget_when_tainted_equals_headroom(self) -> None:
        # Steady prewarm or any state where `tainted_ready == H`: no
        # untaints allowed — that would dip below the H reservation.
        assert (
            self._budget(tainted_ready_count=2, headroom=2, source_terminated_total=0)
            == 0
        )

    def test_one_extra_tainted_releases_one_untaint(self) -> None:
        # After the first terminate landed AND the replacement target
        # node reached Ready: tainted = H + 1, T = 1, U = 0.
        # Both caps say 1.
        assert (
            self._budget(
                tainted_ready_count=3,
                headroom=2,
                source_terminated_total=1,
                already_untainted=0,
            )
            == 1
        )

    def test_provisioning_lag_blocks_untaint(self) -> None:
        # One source terminated, replacement target node still
        # provisioning so tainted_ready is still at H. Headroom cap = 0
        # tightens the budget below the terminate cap of 1.
        H = 4
        assert (
            self._budget(
                tainted_ready_count=H,
                headroom=H,
                source_terminated_total=1,
                already_untainted=0,
            )
            == 0
        )
        # Once the replacement reaches Ready, headroom cap = 1 and the
        # terminate cap = 1 agree → 1 untaint allowed.
        assert (
            self._budget(
                tainted_ready_count=H + 1,
                headroom=H,
                source_terminated_total=1,
                already_untainted=0,
            )
            == 1
        )

    def test_mid_wave_concurrent_drain_keeps_headroom(self) -> None:
        # k=4, H=4: one drain in a 4-wide wave finishes early.
        # `source_terminated_total = 1`, three drains still in flight.
        # Once the replacement target reaches Ready, tainted = H + 1.
        # Headroom cap = 1, terminate cap = 1 → 1 untaint, leaving H
        # tainted slots fenced for the 3 in-flight drains.
        H = 4
        assert (
            self._budget(
                tainted_ready_count=H + 1,
                headroom=H,
                source_terminated_total=1,
                already_untainted=0,
            )
            == 1
        )

    def test_oversized_prewarm_blocked_by_terminate_cap(self) -> None:
        # Mirror rollback: operator pre-scaled the rollback target up
        # to ROLLBACK_N (e.g. 105) tainted Ready nodes before invoking
        # the script. T = 0, U = 0, tainted = ROLLBACK_N >> H.
        # Headroom cap alone would say ROLLBACK_N - H = 101 — wrong.
        # Terminate cap = 0 dominates and the budget collapses to 0.
        ROLLBACK_N = 105
        H = 4
        assert (
            self._budget(
                tainted_ready_count=ROLLBACK_N,
                headroom=H,
                source_terminated_total=0,
                already_untainted=0,
            )
            == 0
        )

    def test_oversized_prewarm_after_first_terminate_releases_one(self) -> None:
        # Same rollback scenario, after the first source terminate
        # lands. T = 1, U = 0. Tainted is now ROLLBACK_N + 1 (ratchet
        # grew target by 1). Headroom cap = ROLLBACK_N + 1 - H = 102,
        # terminate cap = 1 → 1 untaint. Each subsequent terminate
        # releases exactly one more untaint, keeping the script in
        # lockstep with source drains.
        ROLLBACK_N = 105
        H = 4
        assert (
            self._budget(
                tainted_ready_count=ROLLBACK_N + 1,
                headroom=H,
                source_terminated_total=1,
                already_untainted=0,
            )
            == 1
        )

    def test_final_state_caps_at_h(self) -> None:
        # End of rollout: target Ready = N + H, of which N have already
        # been untainted by prior ticks (so tainted_ready = H). Budget
        # = 0; the remaining H Ready+tainted nodes are dropped by the
        # EVACUATION_COMPLETE final-headroom-untaint block and then by
        # the post-rollout Terraform MNG-level untaint PR.
        N = 10
        H = 2
        assert (
            self._budget(
                tainted_ready_count=H,
                headroom=H,
                source_terminated_total=N,
                already_untainted=N,
            )
            == 0
        )


class TestValidateInitialTaintState:
    """Fail-fast cohort-taint sanity check covered as a pure function so we
    don't need to spin up the full `run_loop` integration harness.

    The two real failure modes the check catches: wrong
    `--cohort-taint-key` (kubectl matches no tainted nodes, seed adds
    everything to the untainted set) and mirror rollback where the
    rollback target was scaled up before its cohort taint was re-applied.
    """

    def test_fresh_rollout_all_tainted_passes(self) -> None:
        # Target prewarm with H = 3 Ready tainted nodes; nothing
        # untainted yet; no source terminates done. Healthy fresh start.
        assert (
            rollout.validate_initial_taint_state(
                target_mng_name="workers-1",
                cohort_taint_key="space.pplx.net/rollout-cohort",
                ready_untainted=0,
                not_ready_untainted=0,
                source_terminated_at_start=0,
            )
            is None
        )

    def test_fresh_rollout_wrong_taint_key_fails(self) -> None:
        # Operator typo'd `--cohort-taint-key`. kubectl reads target
        # nodes as all untainted (per the wrong key). Without this
        # guard the rollout proceeds with no fencing and the cleanup
        # sweep is a no-op against the real key.
        err = rollout.validate_initial_taint_state(
            target_mng_name="workers-1",
            cohort_taint_key="space.pplx.net/wrong-key",
            ready_untainted=3,
            not_ready_untainted=0,
            source_terminated_at_start=0,
        )
        assert err is not None
        assert "wrong-key" in err
        assert "Refusing to proceed" in err

    def test_mirror_rollback_unfenced_target_fails(self) -> None:
        # Mirror rollback where the operator forgot to re-apply the
        # cohort taint to the rollback target before scaling it up.
        # All `ROLLBACK_N` rollback target nodes are genuinely
        # untainted; no source terminate has landed in the rollback
        # direction yet.
        err = rollout.validate_initial_taint_state(
            target_mng_name="workers-cgnat",
            cohort_taint_key="space.pplx.net/rollout-cohort",
            ready_untainted=105,
            not_ready_untainted=0,
            source_terminated_at_start=0,
        )
        assert err is not None
        assert "105 nodes without the cohort taint" in err
        assert "Mirror rollback" in err

    def test_resume_matching_terminate_count_passes(self) -> None:
        # Mid-rollout resume: 4 source nodes have terminated, 4 target
        # nodes were correctly untainted by prior ticks. Resume is
        # healthy.
        assert (
            rollout.validate_initial_taint_state(
                target_mng_name="workers-1",
                cohort_taint_key="space.pplx.net/rollout-cohort",
                ready_untainted=4,
                not_ready_untainted=0,
                source_terminated_at_start=4,
            )
            is None
        )

    def test_resume_exceeding_terminate_count_fails(self) -> None:
        # Operator passed a wrong taint key on resume: the seed reads
        # ALL target nodes as untainted, including the H still-fenced
        # nodes. Untainted count >> source_terminated.
        err = rollout.validate_initial_taint_state(
            target_mng_name="workers-1",
            cohort_taint_key="space.pplx.net/wrong-key",
            ready_untainted=10,
            not_ready_untainted=0,
            source_terminated_at_start=4,
        )
        assert err is not None
        assert "wrong-key" in err

    def test_not_ready_untainted_counts_toward_seed(self) -> None:
        # NotReady+untainted nodes are part of the seed too — they
        # could become Ready before the next tick and would over-grant
        # the untaint budget. The validator must catch them on a fresh
        # start.
        err = rollout.validate_initial_taint_state(
            target_mng_name="workers-1",
            cohort_taint_key="space.pplx.net/rollout-cohort",
            ready_untainted=0,
            not_ready_untainted=3,
            source_terminated_at_start=0,
        )
        assert err is not None
        assert "NotReady: 3" in err


def _pod_with_tolerations(name: str, tolerations: list[dict]) -> dict:
    return {
        "metadata": {"name": name, "namespace": "default"},
        "spec": {"tolerations": tolerations},
        "status": {"phase": "Pending"},
    }


class TestKubectlPendingEvacuationPodCount:
    """Counts Pending sandbox pods carrying the rollout-cohort toleration.

    The filter must narrow by `(key, value)` so concurrent rollouts (cohorts
    A and B in flight simultaneously) don't double-count each other's
    evacuation pods. Pods with `Exists`-form tolerations (no value) still
    count toward every cohort by definition — the cluster-controller's
    `drain_evacuation_tolerations` default is `Exists`, so this is the
    correct behavior under the single-rollout-at-a-time assumption.
    """

    def test_counts_pods_with_exists_form_cohort_toleration(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        blob = {
            "items": [
                _pod_with_tolerations(
                    "evac-1",
                    [
                        {
                            "key": rollout.DEFAULT_COHORT_TAINT_KEY,
                            "operator": "Exists",
                            "effect": "NoSchedule",
                        }
                    ],
                ),
                _pod_with_tolerations(
                    "evac-2",
                    [
                        {
                            "key": rollout.DEFAULT_COHORT_TAINT_KEY,
                            "operator": "Exists",
                            "effect": "NoSchedule",
                        }
                    ],
                ),
            ]
        }

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(stdout=json.dumps(blob))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        assert (
            rollout.kubectl_pending_evacuation_pod_count(
                "ctx", rollout.DEFAULT_COHORT_TAINT_KEY, "1"
            )
            == 2
        )

    def test_excludes_user_pods_without_cohort_toleration(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        blob = {
            "items": [
                _pod_with_tolerations(
                    "user-create",
                    [
                        {
                            "key": "node.kubernetes.io/not-ready",
                            "operator": "Exists",
                            "effect": "NoExecute",
                        }
                    ],
                ),
                _pod_with_tolerations("user-restore-no-tol", []),
            ]
        }

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(stdout=json.dumps(blob))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        assert (
            rollout.kubectl_pending_evacuation_pod_count(
                "ctx", rollout.DEFAULT_COHORT_TAINT_KEY, "1"
            )
            == 0
        )

    def test_value_form_toleration_for_other_cohort_is_excluded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test for silent over-counting when two rollouts run
        concurrently and the
        operator narrowed the cluster-controller's toleration to a specific
        cohort value (instead of the default `Exists` form), each script
        must only count pods that target its own cohort. Otherwise both
        cohort A's and cohort B's dashboard panels show the same union
        and the `> 0 for 5 min` alert can fire on the wrong rollout.
        """

        blob = {
            "items": [
                _pod_with_tolerations(
                    "cohort-a-evac",
                    [
                        {
                            "key": rollout.DEFAULT_COHORT_TAINT_KEY,
                            "operator": "Equal",
                            "value": "1",
                            "effect": "NoSchedule",
                        }
                    ],
                ),
                _pod_with_tolerations(
                    "cohort-b-evac",
                    [
                        {
                            "key": rollout.DEFAULT_COHORT_TAINT_KEY,
                            "operator": "Equal",
                            "value": "2",
                            "effect": "NoSchedule",
                        }
                    ],
                ),
            ]
        }

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(stdout=json.dumps(blob))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        # This script is running with cohort=1. It must count only the
        # cohort-a-evac pod, not cohort-b-evac.
        assert (
            rollout.kubectl_pending_evacuation_pod_count(
                "ctx", rollout.DEFAULT_COHORT_TAINT_KEY, "1"
            )
            == 1
        )

    def test_unrelated_taint_key_is_excluded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        blob = {
            "items": [
                _pod_with_tolerations(
                    "unrelated",
                    [
                        {
                            "key": "node-role.kubernetes.io/control-plane",
                            "operator": "Exists",
                            "effect": "NoSchedule",
                        }
                    ],
                ),
            ]
        }

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            return _FakeProc(stdout=json.dumps(blob))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        assert (
            rollout.kubectl_pending_evacuation_pod_count(
                "ctx", rollout.DEFAULT_COHORT_TAINT_KEY, "1"
            )
            == 0
        )

    def test_cluster_wide_field_selector_used(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pending pods have no `nodeName`; the query must scope by label
        + field-selector cluster-wide, not by node."""

        captured: dict[str, Any] = {}

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            captured["cmd"] = cmd
            return _FakeProc(stdout=json.dumps({"items": []}))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        rollout.kubectl_pending_evacuation_pod_count(
            "ctx", rollout.DEFAULT_COHORT_TAINT_KEY, "1"
        )
        assert "-A" in captured["cmd"]
        assert "-l" in captured["cmd"]
        idx = captured["cmd"].index("-l")
        assert captured["cmd"][idx + 1] == rollout.SANDBOX_POD_LABEL_SELECTOR
        assert "--field-selector" in captured["cmd"]
        idx = captured["cmd"].index("--field-selector")
        assert captured["cmd"][idx + 1] == "status.phase=Pending"

    def test_empty_context_omits_context_flag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: when the script runs in-cluster (no kubeconfig
        file), `context` is the empty string and the kubectl command
        must omit `--context` entirely. Passing `--context ""` makes
        kubectl error with `context "" does not exist`, surfaces as a
        `tick_error`, and the `evacuation_pods_pending` gauge never
        reaches Datadog — which is exactly the silent-fail mode the
        in-cluster Job path is meant to fix.
        """

        captured: dict[str, Any] = {}

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            captured["cmd"] = cmd
            return _FakeProc(stdout=json.dumps({"items": []}))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        rollout.kubectl_pending_evacuation_pod_count(
            "", rollout.DEFAULT_COHORT_TAINT_KEY, "1"
        )
        assert "--context" not in captured["cmd"]

    def test_nonempty_context_threads_through(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The local port-forward path passes a concrete context (e.g.
        `example-eks-cluster`); kubectl must receive `--context <name>`."""

        captured: dict[str, Any] = {}

        def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
            captured["cmd"] = cmd
            return _FakeProc(stdout=json.dumps({"items": []}))

        monkeypatch.setattr(rollout.subprocess, "run", fake_run)
        rollout.kubectl_pending_evacuation_pod_count(
            "example-eks-cluster", rollout.DEFAULT_COHORT_TAINT_KEY, "1"
        )
        assert "--context" in captured["cmd"]
        idx = captured["cmd"].index("--context")
        assert captured["cmd"][idx + 1] == "example-eks-cluster"


class TestAdminApiDrainBody:
    """The `:drain` POST body the rollout sends.

    The admin endpoint defaults to a **gentle** drain: it cordons the node and
    waits for resident sandboxes to leave on their own. A blue/green MNG
    rollout replaces the node, so a wave that inherited that default would
    cordon its incumbents and then wait on whichever long-lived sandbox happens
    to be running — the ASG never shrinks, and nothing looks broken. These
    tests pin both fields of the body so that regression can't land silently.
    """

    @staticmethod
    def _api_with_capture(
        drain_phase: str | None = None,
        get_status: int = 200,
        get_body: Any = _KEEP,
        post_status: int = 200,
        post_body: Any = None,
    ) -> tuple[Any, dict[str, Any]]:
        """An `AdminApi` whose session captures the POST.

        `drain_phase` is what the pre-POST re-read reports. The default `None`
        is an undrained node, which is the only state the rollout may drain.
        `get_status=404` models a vanished admin row; any other non-200 status
        raises from `raise_for_status`, the way `requests` does — the guard has
        three distinguishable outcomes on the read, not two, and collapsing the
        error case into the 404 case would let a fail-open on a 500 pass.
        `get_body` replaces the whole response body. `post_status` /
        `post_body` do the same for the `:drain` POST, so the
        commit-then-substrate-patch 502 can be modelled — the response that
        means the drain *did* start.
        """
        captured: dict[str, Any] = {}

        class FakeSession:
            headers: dict[str, str] = {}

            def update(self, *_args: Any, **_kwargs: Any) -> None:
                pass

            def get(
                self, url: str, params: Any = None, timeout: Any = None
            ) -> Any:
                captured["get_url"] = url
                captured["get_params"] = params

                class Resp:
                    status_code = get_status

                    @staticmethod
                    def raise_for_status() -> None:
                        if get_status >= 400:
                            raise RuntimeError(f"HTTP {get_status}")

                    @staticmethod
                    def json() -> Any:
                        if get_body is not _KEEP:
                            return get_body
                        return {
                            "id": url.rsplit("/", 1)[-1],
                            "drain_phase": drain_phase,
                            "sandboxes_active": 0,
                            "sandboxes_local_snapshots": 0,
                            "terminate_decrement_capacity": False,
                        }

                return Resp()

            def post(self, url: str, json: Any = None, timeout: Any = None) -> Any:
                captured["url"] = url
                captured["json"] = json

                class Resp:
                    status_code = post_status

                    @staticmethod
                    def raise_for_status() -> None:
                        if post_status >= 400:
                            raise RuntimeError(f"HTTP {post_status}")

                    @staticmethod
                    def json() -> Any:
                        return post_body

                return Resp()

        api = rollout.AdminApi("https://space.example.com", "token", dry_run=False)
        api.session = FakeSession()
        return api, captured

    def test_drain_requests_forced_mode(self) -> None:
        api, captured = self._api_with_capture()
        api.drain("11111111-1111-1111-1111-111111111111", replace_after_terminate=False)
        assert captured["json"]["mode"] == "forced", (
            "the rollout must ask for a forced drain; the endpoint's default is "
            "gentle, which would never evacuate the incumbent's sandboxes"
        )

    def test_drain_carries_replace_after_terminate(self) -> None:
        api, captured = self._api_with_capture()
        api.drain("22222222-2222-2222-2222-222222222222", replace_after_terminate=False)
        assert captured["json"]["replace_after_terminate"] is False
        assert captured["url"].endswith(
            "/admin/api/v1/nodes/22222222-2222-2222-2222-222222222222:drain"
        )

    def test_dry_run_does_not_post(self) -> None:
        api, captured = self._api_with_capture()
        api.dry_run = True
        assert api.drain(
            "33333333-3333-3333-3333-333333333333", replace_after_terminate=True
        )
        assert "url" not in captured, "dry run must not POST"

    def test_drain_refuses_a_node_that_is_already_draining(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A drain visible on the pre-POST read is skipped without a POST.

        `pick_drain_candidates` filters on the tick's snapshot, so a drain an
        operator started inside the snapshot window still reads as
        `DrainPhase.NONE`. The endpoint would refuse the POST anyway
        (`require_undrained`), so this read isn't what keeps the drain safe;
        it skips the node without a write attempt and reports what it found.
        """
        for phase in ["draining", "drained", "terminating"]:
            api, captured = self._api_with_capture(drain_phase=phase)
            issued = api.drain(
                "44444444-4444-4444-4444-444444444444",
                replace_after_terminate=False,
            )
            assert not issued, f"{phase}: drain must not be issued"
            assert "url" not in captured, (
                f"{phase}: a drain the read can see is skipped without a POST"
            )
            events = [
                json.loads(line)
                for line in capsys.readouterr().out.splitlines()
                if line.strip()
            ]
            assert [e["event"] for e in events] == ["drain_skipped_already_draining"], (
                f"{phase}: the skip is the only trace the caller leaves"
            )
            assert events[0]["drain_phase"] == phase

    def test_drain_requires_an_undrained_node(self) -> None:
        """Every POST is start-only, so the endpoint can never escalate a drain
        this process did not start."""
        api, captured = self._api_with_capture()
        api.drain("66666666-6666-6666-6666-666666666666", replace_after_terminate=False)
        assert captured["json"]["require_undrained"] is True

    def test_a_drain_that_lands_after_the_read_is_skipped_not_failed(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The read sees an undrained node, somebody's drain commits before the
        POST, and the endpoint refuses it with a 409. That is the same outcome
        as a drain the read saw, not a tick error."""
        api, captured = self._api_with_capture(
            drain_phase=None,
            post_status=409,
            post_body={
                "error": "node is already in a drain phase and the request set require_undrained",
                "current_drain_phase": "draining",
            },
        )
        issued = api.drain(
            "77777777-7777-7777-7777-777777777777", replace_after_terminate=False
        )
        assert not issued
        assert "url" in captured, "the read saw no drain, so the POST went out"
        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip()
        ]
        assert [e["event"] for e in events] == ["drain_skipped_already_draining"]
        assert events[0]["drain_phase"] == "draining"
        assert events[0]["refused_by"] == "endpoint"

    def test_the_pre_post_read_skips_occupancy_and_accepts_null_counts(
        self,
    ) -> None:
        """The re-read asks for the drain columns only, and the response that
        produces carries `null` occupancy counts, which must not stop the
        drain."""
        api, captured = self._api_with_capture(
            get_body={
                "id": "88888888-8888-8888-8888-888888888888",
                "drain_phase": None,
                "sandboxes_active": None,
                "sandboxes_local_snapshots": None,
                "terminate_decrement_capacity": False,
            }
        )
        assert api.drain(
            "88888888-8888-8888-8888-888888888888", replace_after_terminate=False
        )
        assert captured["get_params"] == {"include_occupancy": "false"}
        assert "url" in captured

    def test_a_contention_409_is_an_error_not_a_skip(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The admin API also answers 409 for a retryable serialization or
        deadlock abort. That started no drain and names no phase, so it must
        raise rather than read as somebody else's drain."""
        api, _ = self._api_with_capture(
            drain_phase=None,
            post_status=409,
            post_body={
                "error": "operation conflicted with a concurrent admin write; retry the request"
            },
        )
        with pytest.raises(RuntimeError, match="HTTP 409"):
            api.drain(
                "99999999-9999-9999-9999-999999999999", replace_after_terminate=False
            )
        assert "drain_skipped_already_draining" not in capsys.readouterr().out

    def test_drain_proceeds_when_the_node_is_still_undrained(self) -> None:
        """The guard must not block the case the rollout exists to drive."""
        api, captured = self._api_with_capture(drain_phase=None)
        assert api.drain(
            "55555555-5555-5555-5555-555555555555", replace_after_terminate=False
        )
        assert captured["json"]["mode"] == "forced"


class TestAdminApiDrainGuardFailsClosed:
    """The pre-POST phase guard must refuse when it cannot read the phase.

    A 404 on the read is indistinguishable from "the GET never reached a node
    route" — a wrong path prefix, an ingress change, a renamed route. Under that
    reading a guard that treats an unreadable phase as "safe to proceed" is
    inert for *every* candidate, forever, while still appearing to be there: the
    POSTs go out exactly as they did before the guard existed, and the wave is
    back to escalating drains it did not start. So the unreadable case has to
    refuse, and it needs its own test, because it is the one branch whose
    failure looks like success.
    """

    def test_an_unreadable_phase_refuses_the_drain(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        api, captured = TestAdminApiDrainBody._api_with_capture(get_status=404)
        issued = api.drain(
            "66666666-6666-6666-6666-666666666666", replace_after_terminate=False
        )
        assert not issued, "an unreadable phase must not be treated as undrained"
        assert "url" not in captured, (
            "no POST may be sent — reaching the endpoint is what escalates a "
            "drain the rollout did not start"
        )
        # The event is the only trace that a wave declined a node: the caller
        # just `continue`s. Without it a wave that skips every candidate — the
        # shape a wrong path prefix produces — looks identical to one with
        # nothing to do.
        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip()
        ]
        assert [e["event"] for e in events] == ["drain_skipped_phase_unreadable"]
        assert events[0]["node_id"] == "66666666-6666-6666-6666-666666666666"

    def test_a_body_without_the_phase_field_refuses_the_drain(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A 200 whose body has no `drain_phase` key is unreadable, not undrained.

        `AdminNodeResponse.drain_phase` carries no `skip_serializing_if`, so the
        real endpoint always emits the key — `null` when the node is undrained.
        Absence means the body came from somewhere else: a proxy error page
        rendered as JSON, a redirect that landed off the node route, a future
        response shape. `dict.get` maps every one of those to `None`, which is
        precisely the value that tells this guard the node is safe to drain — so
        the fail-open is reached by a *successful* request, which the 404 test
        above cannot catch.
        """
        api, captured = TestAdminApiDrainBody._api_with_capture(
            get_body={"id": "88888888-8888-8888-8888-888888888888", "state": "ready"}
        )
        assert not api.drain(
            "88888888-8888-8888-8888-888888888888", replace_after_terminate=False
        )
        assert "url" not in captured, "no POST may be sent"
        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip()
        ]
        assert [e["event"] for e in events] == ["drain_skipped_phase_unreadable"]
        assert "drain_phase" in events[0]["reason"], (
            "the reason must name the missing field, since this refusal and the "
            "404 one share an event"
        )

    def test_a_read_that_errors_is_not_a_silent_skip(self) -> None:
        """A 5xx on the read must raise, not refuse quietly.

        The three read outcomes are distinguishable and want different handling:
        200 decides on the phase, 404 refuses this node, and an error status is a
        problem with the control plane rather than with this node. Collapsing the
        third into the second would turn a control-plane outage into a rollout
        that skips every candidate and exits zero. Raising lets the wave report
        it as `tick_error` and keep the run's shape honest.
        """
        api, captured = TestAdminApiDrainBody._api_with_capture(get_status=500)
        with pytest.raises(RuntimeError):
            api.drain(
                "99999999-9999-9999-9999-999999999999", replace_after_terminate=False
            )
        assert "url" not in captured, "and still no POST"

    def test_a_readable_undrained_phase_still_proceeds(self) -> None:
        """The fail-closed branch must not swallow the case the wave exists for."""
        api, captured = TestAdminApiDrainBody._api_with_capture(get_status=200)
        assert api.drain(
            "77777777-7777-7777-7777-777777777777", replace_after_terminate=False
        )
        assert captured["json"]["mode"] == "forced"


class TestDrainCommitOrdering:
    """A 502 from the admin handler means the drain *started*.

    `apply_drain_action` commits the Postgres transition and only then patches
    the substrate, so that a failed patch is recoverable instead of leaving the
    scheduler and the database disagreeing. The 502 it answers with therefore
    reports a drain that is already `draining` in the row, with the reconciler
    re-asserting cordon every tick.

    Reading that as a failure kept the run correct — `pick_drain_candidates`
    recounts `in_progress` from the next snapshot, which sees the node as
    draining — while making the run's own record of itself wrong: no
    `drain_started`, a `tick_error` for a request that succeeded, and
    `drain_wave_made_no_progress` on a wave that had made exactly the progress
    asked of it.
    """

    HANDLER_502 = {
        "error": "kube PATCH failed: node not found",
        "db_state": {"id": "n", "drain_phase": "draining"},
        "reconciler_will_retry": True,
    }

    def test_a_handler_502_after_commit_counts_as_started(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        api, captured = TestAdminApiDrainBody._api_with_capture(
            post_status=502, post_body=self.HANDLER_502
        )
        assert api.drain(
            "cccccccc-cccc-cccc-cccc-cccccccccccc", replace_after_terminate=False
        ), "the drain committed, so the wave must book it"
        assert captured["json"]["mode"] == "forced", "and the POST still went out"
        event = json.loads(capsys.readouterr().out.strip())
        assert event["event"] == "drain_committed_substrate_patch_failed"
        assert event["reconciler_will_retry"] is True, (
            "the operator needs to know a repair is coming, since this path "
            "reports success on a response that says something failed"
        )
        assert "kube PATCH failed" in event["error"]

    def test_a_gateway_502_does_not_count_as_started(self) -> None:
        """The other 502, which means the opposite, and must still raise.

        A load balancer, ingress or mesh in front of the api-server answers 502
        with no `db_state`, and for that one the transition never committed.
        Booking it would drop the node from candidacy for the rest of the run —
        the rollout would skip a node it never drained — so the default is to
        raise and only positive evidence of a commit opts out.
        """
        for body in (
            None,
            "<html>502 Bad Gateway</html>",
            {"error": "upstream connect error"},
            {"error": "x", "db_state": None},
            # The handler's shape, but reporting a row that is *not* draining:
            # no commit to infer, so no booking.
            {"error": "x", "db_state": {"id": "n", "drain_phase": None}},
        ):
            api, _ = TestAdminApiDrainBody._api_with_capture(
                post_status=502, post_body=body
            )
            with pytest.raises(RuntimeError):
                api.drain(
                    "dddddddd-dddd-dddd-dddd-dddddddddddd",
                    replace_after_terminate=False,
                )

    def test_a_plain_200_still_needs_no_event(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The ordinary path must not start narrating a failure that did not happen."""
        api, _ = TestAdminApiDrainBody._api_with_capture()
        assert api.drain(
            "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee", replace_after_terminate=False
        )
        assert capsys.readouterr().out.strip() == ""


class TestResolvedDrainMode:
    """The refusal event must name the destructive case, not print `null` for it.

    `AdminNodeResponse.drain_mode` is `skip_serializing_if = "Option::is_none"`,
    so a node draining with `drain_mode IS NULL` omits the key and the raw value
    is `None`. NULL resolves to **gentle** in the control plane, and an inherited
    gentle drain is exactly the one whose escalation destroys sandboxes. So the
    single most important refusal to recognize in the JSONL was the one that
    printed as unreported, while a routine explicit `"gentle"` printed normally.
    """

    def test_null_and_unrecognized_both_resolve_to_gentle(self) -> None:
        """The non-evacuating mode is what a missing or unparseable value means.

        Mirrors `NodeDrainMode::from_column_str`. `"gently"` stands in for a
        value a newer service could write during a rollout: the column carries
        no `CHECK`, so it is reachable rather than hypothetical.
        """
        assert rollout.resolved_drain_mode(None) == "gentle"
        assert rollout.resolved_drain_mode("gently") == "gentle"
        assert rollout.resolved_drain_mode("") == "gentle"
        # And the two it must pass through unchanged, so the resolution cannot
        # be satisfied by a function that answers "gentle" to everything.
        assert rollout.resolved_drain_mode("gentle") == "gentle"
        assert rollout.resolved_drain_mode("forced") == "forced"

    def test_a_null_mode_is_reported_as_gentle_on_the_refusal(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """End to end: a NULL column reaches the event as a named mode.

        The body omits `drain_mode` entirely, which is what the API sends for a
        NULL column — not `"drain_mode": null`. Both fields are asserted: the
        raw one stays `None` so "nobody set a mode" is still distinguishable
        from "someone chose gentle", and the resolved one names the risk.
        """
        api, captured = TestAdminApiDrainBody._api_with_capture(
            get_body={
                "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "drain_phase": "draining",
                "sandboxes_active": 0,
                "sandboxes_local_snapshots": 0,
                "terminate_decrement_capacity": False,
            }
        )
        assert not api.drain(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", replace_after_terminate=False
        )
        assert "url" not in captured, "no POST may reach the escalation edge"
        event = json.loads(capsys.readouterr().out.strip())
        assert event["event"] == "drain_skipped_already_draining"
        assert event["drain_mode"] is None, (
            "the raw column stays unset, so an operator can still tell "
            "'nobody set a mode' from 'someone chose gentle'"
        )
        assert event["drain_mode_resolved"] == "gentle", (
            "and the resolution names it as the destructive case"
        )

    def test_an_explicit_forced_mode_is_reported_as_itself(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """The routine case must stay distinguishable from the one above.

        Re-reading our own forced drain after a restart is not worth paging on.
        If both resolved to the same string the field would carry nothing.
        """
        api, _ = TestAdminApiDrainBody._api_with_capture(
            get_body={
                "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "drain_phase": "draining",
                "drain_mode": "forced",
                "sandboxes_active": 0,
                "sandboxes_local_snapshots": 0,
                "terminate_decrement_capacity": False,
            }
        )
        assert not api.drain(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", replace_after_terminate=False
        )
        event = json.loads(capsys.readouterr().out.strip())
        assert event["drain_mode"] == "forced"
        assert event["drain_mode_resolved"] == "forced"


class TestAdminApiPrefix:
    """Every admin URL this script builds must resolve against the real router.

    A wrong prefix is the one defect class here that a green suite cannot rule
    out on its own, because the symptom is indistinguishable from a healthy tick
    with nothing to do: `list_nodes` raises, the tick handler reports
    `tick_error`, and the run retries forever having drained nothing.

    The assertions below spell `/admin/api/v1` out **as a literal** rather than
    reading `rollout.ADMIN_API_PREFIX`. Reading the constant would make every one
    of them pass for whatever the constant happened to say, which is no test of
    the value at all — the point is to pin it against the router's mount.
    """

    LITERAL = "/admin/api/v1"

    def test_the_prefix_matches_the_router_mount(self) -> None:
        """Pinned against `shared_host_router`'s `.nest()` in admin/mod.rs.

        The dedicated admin host reaches the same tree at `/v1`, and the ingress
        `/admin` rule is a bare `pathType: Prefix` with no rewrite-target, so
        this is the spelling that resolves on every deployment — the same
        reasoning `space-admin-cli`'s `ADMIN_API_PREFIX` records.
        """
        assert rollout.ADMIN_API_PREFIX == self.LITERAL

    def test_every_call_site_uses_it(self) -> None:
        """All four routes, and asserted as a set so a duplicate cannot stand in.

        The stale prefix survived because only `drain` had its URL asserted.
        Three of the four call sites had no coverage at all, and one of those
        three is `get_node` — the pre-POST guard, whose whole job is to refuse a
        drain it cannot verify.

        Two constraints make this test cover what it claims, and both are easy
        to violate in a way that still passes:

        * The GET must answer a real 200. `drain()` calls `get_node` before it
          posts, so a session that aborts the GET propagates out of `drain()`
          and the `:drain` POST is never built — leaving the drain route
          unobserved while the test looks fine.
        * The assertion must be over the *set* of `(method, suffix)` pairs, not
          a count. `get_node`'s URL appears twice — once directly, once from
          inside `drain` — so a length check is satisfied by the duplicate and
          stays green when a route is missing.
        """
        seen: list[tuple[str, str]] = []
        base = "https://space.example.com"

        class RecordingSession:
            headers: dict[str, str] = {}

            def update(self, *_a: Any, **_k: Any) -> None:
                pass

            def get(self, url: str, params: Any = None, timeout: Any = None) -> Any:
                seen.append(("GET", url))

                class Resp:
                    status_code = 200

                    @staticmethod
                    def raise_for_status() -> None:
                        pass

                    @staticmethod
                    def json() -> Any:
                        # A list for the collection route, a row for the detail
                        # route, so both parse. `drain_phase` must be absent-as-
                        # NULL rather than missing, or `get_node` refuses and
                        # `drain` returns before posting.
                        if url.endswith("/nodes"):
                            return []
                        return {
                            "id": url.rsplit("/", 1)[-1],
                            "drain_phase": None,
                            "sandboxes_active": 0,
                            "sandboxes_local_snapshots": 0,
                            "terminate_decrement_capacity": False,
                        }

                return Resp()

            def post(self, url: str, json: Any = None, timeout: Any = None) -> Any:
                seen.append(("POST", url))

                class Resp:
                    status_code = 200

                    @staticmethod
                    def raise_for_status() -> None:
                        pass

                    @staticmethod
                    def json() -> Any:
                        return {}

                return Resp()

        api = rollout.AdminApi(base, "t", dry_run=False)
        api.session = RecordingSession()  # type: ignore[assignment]

        node = "44444444-4444-4444-4444-444444444444"
        api.list_nodes()
        api.get_node(node)
        api.drain(node, replace_after_terminate=False)
        api.terminate(node)

        prefix = f"{base}{self.LITERAL}"
        assert set(seen) >= {
            ("GET", f"{prefix}/nodes"),
            ("GET", f"{prefix}/nodes/{node}"),
            ("POST", f"{prefix}/nodes/{node}:drain"),
            ("POST", f"{prefix}/nodes/{node}:terminate"),
        }, f"every route must resolve against the admin mount; saw {sorted(set(seen))}"


class TestIssueDrainWave:
    """The two decisions the drain wave makes about `AdminApi.drain`'s result.

    Both are invisible from `drain`'s own tests — they live at the call site —
    and both are silent when wrong. Extracted from the tick loop so they can be
    asserted at all.
    """

    @staticmethod
    def _node(node_id: str) -> Any:
        return make_node_status(
            instance_id=f"i-{node_id[:4]}",
            admin=make_admin_node(node_id=node_id),
        )

    class _Api:
        """Answers `drain` from a scripted list of outcomes."""

        def __init__(self, outcomes: list[Any]) -> None:
            self.outcomes = list(outcomes)
            self.calls: list[str] = []

        def drain(self, node_id: str, replace_after_terminate: bool) -> bool:
            self.calls.append(node_id)
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    def test_a_refused_node_is_not_booked_as_drained(self) -> None:
        """A refusal must not enter `drain_issued`.

        `drain_issued` removes a node from candidacy permanently *and* counts it
        against `--max-concurrent-node-drains`. Booking a refusal would lose the
        wave one slot per refusal until it could no longer drain anything —
        silently, since the refusal itself is expected.
        """
        api = self._Api([False, True])
        drain_issued: set[str] = set()
        last_phase: dict[str, Any] = {}
        issued = rollout.issue_drain_wave(
            api,
            [self._node("aaaa1111"), self._node("bbbb2222")],
            cohort="c",
            drain_issued=drain_issued,
            last_phase=last_phase,
        )
        assert issued == 1
        assert drain_issued == {"bbbb2222"}, (
            "only the node that was actually drained may be booked"
        )
        assert "aaaa1111" not in last_phase

    def test_a_failing_node_does_not_starve_the_ones_after_it(self) -> None:
        """A raise must be contained per node.

        `pick_drain_candidates` orders deterministically, so an uncontained
        raise on the first candidate would starve every later one — and the
        terminate phase in the same block — on every tick, turning a slow wave
        into a stalled one.
        """
        api = self._Api([RuntimeError("boom"), True])
        drain_issued: set[str] = set()
        issued = rollout.issue_drain_wave(
            api,
            [self._node("cccc3333"), self._node("dddd4444")],
            cohort="c",
            drain_issued=drain_issued,
            last_phase={},
        )
        assert api.calls == ["cccc3333", "dddd4444"], (
            "the second candidate must still be attempted"
        )
        assert issued == 1
        assert drain_issued == {"dddd4444"}

    def test_a_wave_that_declines_every_candidate_says_so(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A wave that had candidates and issued none must not be silent.

        `drain_skipped_*` says why per node, but nothing says the wave as a whole
        stopped moving. And a refusal is not an error, so the tick completes
        normally and the loop keeps ticking with no failure status — a rollout
        wedged behind a foreign drain on its only slot is indistinguishable, from
        the outside, from one patiently waiting on something that is progressing.
        """
        api = self._Api([False, RuntimeError("boom")])
        assert (
            rollout.issue_drain_wave(
                api,
                [self._node("eeee5555"), self._node("ffff6666")],
                cohort="c",
                drain_issued=set(),
                last_phase={},
            )
            == 0
        )
        events = [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip()
        ]
        assert [e["event"] for e in events] == [
            "tick_error",
            "drain_wave_made_no_progress",
        ], "the raise is reported, then the wave-level stall"
        assert events[-1]["candidates"] == 2, (
            "the candidate count is what separates a stalled wave from an "
            "empty one, so it has to be on the event"
        )

    def test_a_wave_with_no_candidates_is_not_called_stalled(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """An empty wave issues zero too, and must not report a stall.

        This is the steady state of a *finishing* rollout: once the last node is
        drained, `pick_drain_candidates` returns nothing on every subsequent
        tick. Reporting no-progress there would fire the event continuously at
        exactly the moment the rollout is succeeding — and it would mean the
        opposite of what it says, which is worse than not emitting it.
        """
        api = self._Api([])
        assert (
            rollout.issue_drain_wave(
                api,
                [],
                cohort="c",
                drain_issued=set(),
                last_phase={},
            )
            == 0
        )
        assert capsys.readouterr().out.strip() == "", (
            "an empty wave emits nothing at all"
        )
