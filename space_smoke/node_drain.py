"""Both node-drain modes, proven on a real cluster.

`smoke-kind-node-drain-gentle.sh` drained the worker hosting a live sandbox
with a bodiless `:drain`, held it there through ~20 reconciler ticks, and
asserted that nothing moved: the node is cordoned and parked in `draining`
with `drain_mode = gentle`, the resident sandbox keeps its lifecycle, pod, pod
UID, node pin and user-API access, no suspend snapshot appears, and the node
reaches `drained` only once the resident suspends of its own accord.
`smoke-kind-node-drain.sh` drained it with `mode: forced` and asserted the
opposite: the drain reconciler suspends the resident and restores it on the
other worker. This module is both suites in typed form.

Because the kind chart runs the drained-node terminator with a 1 s tick and
the `kube-delete` backend, the node is guarded with the
`space.pplx.net/drain-keep` operator override before either drain and
asserted to stay `drained`. That gives the override its runtime coverage and
leaves no tombstone behind, where the forced shell suite let the node be
deleted and restarted kubelet to get it back. The terminator's own contracts
are crate tests (`drained_node_terminator/tests.rs`,
`node_drain/kube_delete/kube_delete_tests.rs`). The control surface is
[`NodeDrainControl`]; the compensations (undrain first, then drop the
override, then uncordon) are armed before the drain is dispatched.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    AdminNodeView,
    NodeDrainControl,
    SandboxPodPlacement,
    SandboxResidency,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import CommandSandboxFixture

DRAIN_KEEP_VALUE = "space-smoke-node-drain"


class NodeDrainAssertions:
    """Drain the sandbox's worker in either mode and judge what happened to the resident."""

    def __init__(
        self, fixture: CommandSandboxFixture, control: NodeDrainControl, scope: ResourceScope
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._release_lease: Compensation | None = None
        self._undrain: Compensation | None = None
        self._uncordon: Compensation | None = None
        self._node: AdminNodeView | None = None
        self._placement_before: SandboxPodPlacement | None = None
        self._residency_before: SandboxResidency | None = None

    @property
    def sandbox_id(self) -> str:
        """The fixture sandbox's bare UUID, the form the pod label and the rows carry.

        The native route answers `sbx_<uuid>`; the ECW route already speaks the
        bare UUID, so the strip is a no-op there.
        """

        return self._fixture.sandbox.sandbox_id.removeprefix("sbx_")

    @property
    def drained_node(self) -> AdminNodeView:
        """The SPACE node `resident_node()` recorded: the one the drain targets."""

        if self._node is None:
            raise RuntimeError("resident_node() must run first")
        return self._node

    # --- preconditions --------------------------------------------------------

    def require_clean_fleet(self, *, min_workers: int = 2) -> list[AdminNodeView]:
        """No node mid-drain, and enough workers for an evacuation to land somewhere.

        A node left `draining` by an aborted run would make every verdict
        ambiguous, so it is a precondition rather than something to repair.
        With one worker an evacuation could not place: a forced drain would
        fail for want of a target, and a gentle drain's "the sandbox never
        moved" would pass for the wrong reason.
        """

        nodes = self._control.admin_nodes()
        draining = [n for n in nodes if n.drain_phase is not None]
        self._fixture.verify(
            not draining,
            "a node is already mid-drain; refusing to judge a drain on a dirty fleet",
            draining=[(n.k8s_node_name, n.drain_phase, n.drain_mode) for n in draining],
        )
        workers = [n for n in nodes if n.k8s_node_name]
        self._fixture.verify(
            len(workers) >= min_workers,
            f"a drain needs at least {min_workers} workers so an evacuation has somewhere to land",
            workers=[n.k8s_node_name for n in workers],
        )
        return nodes

    def resident_node(self) -> AdminNodeView:
        """The SPACE node hosting the fixture sandbox's pod, with the pod identity recorded."""

        sandbox_id = self.sandbox_id
        placement = self._control.sandbox_pod_placement(sandbox_id)
        self._fixture.verify(
            placement is not None,
            "the sandbox has no single bound execution pod to drain around",
            sandbox_id=sandbox_id,
        )
        assert placement is not None
        node = next(
            (n for n in self._control.admin_nodes() if n.k8s_node_name == placement.node_name),
            None,
        )
        self._fixture.verify(
            node is not None,
            "the pod's kube node has no SPACE node row",
            kube_node=placement.node_name,
        )
        assert node is not None
        residency = self._control.sandbox_residency(sandbox_id)
        self._fixture.verify(
            residency.lifecycle == "started" and residency.active_node_id == node.id,
            "the sandbox is not a started resident of the node its pod is bound to",
            lifecycle=residency.lifecycle,
            active_node_id=residency.active_node_id,
            node_id=node.id,
        )
        self._node = node
        self._placement_before = placement
        self._residency_before = residency
        self._fixture.record("resident_node", node.k8s_node_name)
        self._fixture.record("resident_pod_uid", placement.pod_uid)
        return node

    def target_node(self, node: AdminNodeView) -> None:
        """Drain `node` without recording a started resident on it.

        For a worker whose resident is a `stopped` sandbox with no pod, which
        `resident_node()` can't find through the pod and refuses as not started.
        """

        self._node = node
        self._fixture.record("drain_target_node", node.k8s_node_name)

    # --- the drain ------------------------------------------------------------

    def drain_gently(self, *, cordon_timeout: float = 30) -> None:
        """Guard the node, drain it with no body, and see gentle mode plus a cordon."""

        self._guard_and_drain(
            self._control.drain_node_default_mode,
            expected_mode="gentle",
            mode_failure="a bodiless :drain must park the node in draining with the gentle default",
            cordon_timeout=cordon_timeout,
        )

    def drain_forced(self, *, cordon_timeout: float = 30) -> None:
        """Guard the node, drain it with `mode: forced`, and see forced mode plus a cordon."""

        self._guard_and_drain(
            self._control.drain_node_forced,
            expected_mode="forced",
            mode_failure="a :drain with mode forced must park the node in draining in forced mode",
            cordon_timeout=cordon_timeout,
        )

    def _guard_and_drain(
        self,
        dispatch: Callable[[str], None],
        *,
        expected_mode: str,
        mode_failure: str,
        cordon_timeout: float,
    ) -> None:
        """Arm the compensations and the guard, dispatch the drain, and wait for its cordon.

        Compensations are armed in the order they must unwind: undrain and, only
        once it has returned, the override (one compensation, so a failed undrain
        leaves the guard on a node that is still `drained`), then a
        belt-and-braces uncordon.
        """

        node = self._node
        if node is None:
            raise RuntimeError("resident_node() must run before the drain")
        release = self._control.acquire_worker_cordon_mutation()
        self._release_lease = self._scope.defer("release the worker cordon lease", release)
        self._uncordon = self._scope.defer(
            "uncordon the drained worker",
            lambda: self._control.uncordon_kube_node(node.k8s_node_name),
            critical=True,
        )
        self._control.set_drain_keep(node.k8s_node_name, DRAIN_KEEP_VALUE)
        self._undrain = self._scope.defer(
            "undrain the worker, then drop the drain-keep override",
            lambda: self._undrain_then_drop_override(node),
            critical=True,
        )
        dispatch(node.id)
        after = self._admin_node(node.id)
        self._fixture.verify(
            after.drain_mode == expected_mode and after.drain_phase == "draining",
            mode_failure,
            drain_mode=after.drain_mode,
            drain_phase=after.drain_phase,
        )
        deadline = time.monotonic() + cordon_timeout
        while not self._control.kube_node_unschedulable(node.k8s_node_name):
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the {expected_mode} drain did not cordon the worker",
                node=node.k8s_node_name,
            )
            self._fixture.wait(1)
        self._fixture.record("drain_mode", after.drain_mode)

    def expect_resident_untouched(self, *, hold_seconds: float = 20) -> None:
        """Hold ~20 reconciler ticks; the node stays draining and nothing about the sandbox moves."""

        node, before, residency = self._node, self._placement_before, self._residency_before
        if node is None or before is None or residency is None:
            raise RuntimeError("drain_gently() must run before expect_resident_untouched()")
        self._fixture.wait(hold_seconds, reason="for reconciler ticks under the gentle drain")
        now = self._admin_node(node.id)
        placement = self._control.sandbox_pod_placement(self.sandbox_id)
        after = self._control.sandbox_residency(self.sandbox_id)
        snapshots = self._control.ready_full_snapshots(self.sandbox_id)
        self._fixture.verify(
            now.drain_phase == "draining"
            and after.lifecycle == "started"
            and after.active_node_id == node.id
            and placement is not None
            and placement.node_name == before.node_name
            and placement.pod_uid == before.pod_uid
            and not snapshots,
            "the gentle drain touched its resident: lifecycle, pod, pod UID, node pin or a snapshot changed",
            drain_phase=now.drain_phase,
            lifecycle=after.lifecycle,
            active_node_id=after.active_node_id,
            pod_before=(before.pod_name, before.pod_uid, before.node_name),
            pod_after=None
            if placement is None
            else (placement.pod_name, placement.pod_uid, placement.node_name),
            ready_snapshots=[s.snapshot_id for s in snapshots],
        )
        # The gate: a forced drain answers 409 on the user API for the whole
        # window; a gentle drain keeps serving. A command round trip is that
        # gate through the public data plane.
        result = self._fixture.run_command_when_ready("true")
        self._fixture.verify(
            result.exit_code == 0,
            "the user API stopped serving the resident during a gentle drain",
            command_stderr=result.stderr,
        )

    def expect_evacuated(self, *, timeout: float = 180) -> SandboxPodPlacement:
        """The forced drain moves the resident: `started` again, pinned to and running on another worker.

        Both halves are required. The row alone could name another node while
        the pod still ran on the drained one, and the pod alone could be a
        replacement the control plane never adopted.
        """

        node, before = self._node, self._placement_before
        if node is None or before is None:
            raise RuntimeError("drain_forced() must run before expect_evacuated()")
        sandbox_id = self.sandbox_id
        deadline = time.monotonic() + timeout
        while True:
            residency = self._control.sandbox_residency(sandbox_id)
            placement = self._control.sandbox_pod_placement(sandbox_id)
            if (
                residency.lifecycle == "started"
                and residency.active_node_id not in (None, node.id)
                and placement is not None
                and placement.node_name != node.k8s_node_name
            ):
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the forced drain did not evacuate its resident to another worker",
                lifecycle=residency.lifecycle,
                active_node_id=residency.active_node_id,
                drained_node_id=node.id,
                pod_before=(before.pod_name, before.pod_uid, before.node_name),
                pod_now=None
                if placement is None
                else (placement.pod_name, placement.pod_uid, placement.node_name),
            )
            self._fixture.wait(2)
        assert placement is not None
        self._fixture.record("evacuated_to", placement.node_name)
        return placement

    def expect_drained_and_held(self, *, timeout: float = 120, hold_seconds: float = 5) -> None:
        """Once the resident has left, the node reaches drained on its own and stays there."""

        node = self._node
        if node is None:
            raise RuntimeError("the drain must run before expect_drained_and_held()")
        deadline = time.monotonic() + timeout
        while True:
            now = self._admin_node(node.id)
            if now.drain_phase == "drained":
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the node never reached drained after its resident left",
                drain_phase=now.drain_phase,
            )
            self._fixture.wait(1)
        # The kind terminator ticks every second with the kube-delete backend;
        # if the override were not honoured the node would already be gone.
        self._fixture.wait(hold_seconds, reason="for terminator ticks under the drain-keep override")
        held = self._admin_node(node.id)
        self._fixture.verify(
            held.drain_phase == "drained" and self._control.kube_node_exists(node.k8s_node_name),
            "the drain-keep override did not hold the drained node",
            drain_phase=held.drain_phase,
            kube_node_present=self._control.kube_node_exists(node.k8s_node_name),
        )

    def undrain(self, *, timeout: float = 30) -> None:
        """Undrain, drop the override, uncordon; the node is schedulable again with no drain phase."""

        node = self._node
        if node is None:
            return
        for slot in ("_undrain", "_uncordon", "_release_lease"):
            compensation = getattr(self, slot)
            if compensation is not None:
                setattr(self, slot, None)
                compensation.release()
        deadline = time.monotonic() + timeout
        while True:
            now = self._admin_node(node.id)
            if now.drain_phase is None and not self._control.kube_node_unschedulable(node.k8s_node_name):
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the node did not return to schedulable with no drain phase after :undrain",
                drain_phase=now.drain_phase,
                unschedulable=self._control.kube_node_unschedulable(node.k8s_node_name),
            )
            self._fixture.wait(1)

    def _undrain_then_drop_override(self, node: AdminNodeView) -> None:
        # The override outlives a failed undrain on purpose. `ResourceScope.close`
        # keeps unwinding after a critical compensation raises, so as two
        # compensations a failed `:undrain` would still clear the guard on a node
        # that is still `drained`, and the one-second kind terminator could
        # delete the worker in that gap. Clearing only after the undrain returns
        # leaves the node guarded, drained and reported as a critical cleanup
        # failure for an operator instead.
        self._control.undrain_node(node.id)
        self._control.clear_drain_keep(node.k8s_node_name)

    def _admin_node(self, node_id: str) -> AdminNodeView:
        node = next((n for n in self._control.admin_nodes() if n.id == node_id), None)
        if node is None:
            raise RuntimeError(f"SPACE node {node_id} vanished from the admin listing")
        return node
