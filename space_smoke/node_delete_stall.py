"""A Delete whose node-local delete stalls releases its dispatcher slot.

A Delete teardown waits at most 10 s on the node's blocking sandbox-manager
`DeleteSandbox`, then asks the node, bounded at 2 s, whether it admitted the
delete (`SandboxState` reads `deleting`, or no row). If it did, the teardown
continues and deletes the pod, and the node's cleanup executor finishes the
rest. If it did not, or the node does not answer, the teardown defers: the
sandbox is kept and the operation is requeued, so each attempt holds a
dispatcher slot for about 12 s rather than the node RPC's 150 s.

The stall is the cgroup v2 freezer on one node service's container. A frozen
sandbox-manager answers nothing, so its delete is never admitted; a frozen
node-manager leaves sandbox-manager serving while the cleanup it admitted
hangs on node-manager. The thaw is armed before each freeze, and each stall
stays under the services' liveness window (three failed 20 s probes) so the
kubelet never restarts the frozen container.

A slot is held from the operation's `claimed` event to the `requeued` or
terminal event that ends that attempt; both are public operation events.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from space_smoke.capabilities import NodeDeleteStallControl, OperationEvent

if TYPE_CHECKING:
    from space_smoke.capabilities import CommandSandbox
    from space_smoke.fixtures import KindEcw
    from space_smoke.runtime import Compensation, ResourceScope

SANDBOX_MANAGER = "sandbox-manager"
NODE_MANAGER = "node-manager"
# The teardown's node delete wait (10 s) plus its admission probe (2 s).
NODE_DELETE_BUDGET_SECONDS = 12.0
# Claim, event write and pod delete on top of the budget, on a loaded runner.
SLOT_HOLD_LIMIT_SECONDS = 20.0
# Longer than the budget, so a Delete that answered in time is told apart.
HANG_PROOF_SECONDS = 15.0
ENDING_EVENTS = ("requeued", "completed", "failed", "canceled", "expired")


def slot_holds(events: list[OperationEvent]) -> list[tuple[float, str]]:
    """Each attempt's slot hold: seconds from a `claimed` event to the event that ends it."""

    holds: list[tuple[float, str]] = []
    claimed: float | None = None
    for event in events:
        if event.event_type == "claimed":
            claimed = event.created_at
        elif event.event_type in ENDING_EVENTS and claimed is not None:
            holds.append((round(event.created_at - claimed, 1), event.event_type))
            claimed = None
    return holds


class NodeDeleteStallAssertions:
    """Stall one node service under a Delete and read the operation and the sandbox."""

    def __init__(
        self, fixture: KindEcw, control: NodeDeleteStallControl, scope: ResourceScope
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._thaw: Compensation | None = None
        self._frozen_at: float | None = None
        self._frozen: tuple[str, str, int] | None = None

    def node_of(self, sandbox_id: str) -> str:
        placement = self._control.sandbox_pod_placement(sandbox_id)
        self._fixture.verify(
            placement is not None,
            "the sandbox has no bound pod to read its worker from",
            sandbox_id=sandbox_id,
        )
        assert placement is not None
        return placement.node_name

    def freeze(self, node_name: str, component: str) -> None:
        """Freeze the service; its thaw is armed first."""

        def thaw() -> None:
            self._control.thaw_node_service(node_name, component)

        self._frozen = (node_name, component, self._control.node_service_restarts(node_name, component))
        self._thaw = self._scope.defer(f"thaw {component} on {node_name}", thaw, critical=True)
        self._control.freeze_node_service(node_name, component)
        self._frozen_at = time.monotonic()

    def thaw(self) -> float:
        """Thaw the frozen service; returns how long it was frozen.

        The kubelet did not restart it meanwhile, so the stall was the
        frozen process throughout rather than a replacement that answers.
        """

        assert self._thaw is not None and self._frozen_at is not None and self._frozen is not None
        thaw, self._thaw = self._thaw, None
        thaw.release()
        frozen_for = round(time.monotonic() - self._frozen_at, 1)
        node_name, component, restarts = self._frozen
        self._fixture.record(f"{component}_frozen_seconds", frozen_for)
        after = self._control.node_service_restarts(node_name, component)
        self._fixture.verify(
            after == restarts,
            f"the kubelet restarted the frozen {component}",
            node=node_name,
            restarts_before=restarts,
            restarts_after=after,
        )
        return frozen_for

    def expect_service_hangs(self, node_name: str, component: str) -> None:
        """The frozen service's own health socket does not answer past the teardown's budget."""

        started = time.monotonic()
        answered = self._control.node_service_health_answers(
            node_name, component, rpc_timeout=HANG_PROOF_SECONDS
        )
        waited = round(time.monotonic() - started, 1)
        self._fixture.record(f"{component}_health_unanswered_seconds", waited)
        self._fixture.verify(
            not answered and waited >= HANG_PROOF_SECONDS,
            f"the frozen {component} answered within {HANG_PROOF_SECONDS:g}s",
            node=node_name,
            answered=answered,
            waited_seconds=waited,
        )

    def expect_service_answers(self, node_name: str, component: str) -> None:
        answered = self._control.node_service_health_answers(node_name, component, rpc_timeout=5)
        self._fixture.verify(answered, f"{component} on {node_name} does not answer", node=node_name)

    def delete(self, sandbox_id: str) -> str:
        operation_id = self._control.delete_without_waiting(sandbox_id)
        self._fixture.record(f"delete_operation_{sandbox_id}", operation_id)
        return operation_id

    def holds(self, operation_id: str) -> list[tuple[float, str]]:
        return slot_holds(self._control.operation_events(operation_id))

    def expect_requeued_while_kept(
        self, operation_id: str, sandbox: CommandSandbox, *, attempts: int, timeout: float
    ) -> list[tuple[float, str]]:
        """At least `attempts` attempts each end requeued within the slot limit,
        while the public API still returns the sandbox and the operation is not done."""

        deadline = time.monotonic() + timeout
        while True:
            holds = self.holds(operation_id)
            state = self._control.operation_state(operation_id)
            info = sandbox.try_get_info()
            self._fixture.verify(
                state in ("queued", "running"),
                "the delete operation ended while the node could not admit it",
                operation_state=state,
                holds=holds,
            )
            self._fixture.verify(
                info is not None,
                "the public API stopped returning the sandbox before its node admitted the delete",
                sandbox_id=sandbox.sandbox_id,
                holds=holds,
            )
            self._fixture.verify(
                all(kind == "requeued" and held <= SLOT_HOLD_LIMIT_SECONDS for held, kind in holds),
                f"an attempt held its dispatcher slot past {SLOT_HOLD_LIMIT_SECONDS:g}s"
                " or ended other than requeued",
                holds=holds,
            )
            if len(holds) >= attempts:
                self._fixture.record("not_admitted_slot_holds", holds)
                self._fixture.record("not_admitted_sandbox_state", str(getattr(info, "state", None)))
                return holds
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the delete was not requeued {attempts} times",
                holds=holds,
            )
            self._fixture.wait(1)

    def expect_succeeded(self, operation_id: str, *, timeout: float) -> list[tuple[float, str]]:
        """The operation succeeds within `timeout`; every attempt stayed within the slot limit."""

        started = time.monotonic()
        deadline = started + timeout
        while True:
            state = self._control.operation_state(operation_id)
            if state == "succeeded":
                holds = self.holds(operation_id)
                self._fixture.verify(
                    all(held <= SLOT_HOLD_LIMIT_SECONDS for held, _ in holds),
                    f"an attempt held its dispatcher slot past {SLOT_HOLD_LIMIT_SECONDS:g}s",
                    holds=holds,
                )
                self._fixture.record(f"succeeded_after_seconds_{operation_id}", round(time.monotonic() - started, 1))
                return holds
            self._fixture.verify(
                state in ("queued", "running") and time.monotonic() < deadline,
                "the delete operation did not succeed",
                operation_state=state,
                holds=self.holds(operation_id),
            )
            self._fixture.wait(1)

    def expect_gone(self, sandbox: CommandSandbox, *, timeout: float = 60) -> None:
        deadline = time.monotonic() + timeout
        while True:
            info = sandbox.try_get_info()
            if info is None:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the public API still returns the deleted sandbox",
                sandbox_id=sandbox.sandbox_id,
                state=str(getattr(info, "state", None)),
            )
            self._fixture.wait(2)

    def node_row(self, node_name: str, sandbox_id: str) -> str | None:
        return self._control.node_sandbox_state(node_name, sandbox_id)

    def expect_node_row_gone(self, node_name: str, sandbox_id: str, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            row = self.node_row(node_name, sandbox_id)
            if row is None:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the node's cleanup did not finish after node-manager was thawed",
                node=node_name,
                row_state=row,
            )
            self._fixture.wait(2)
