"""Node memory admission refusals: the create fail-fast and the slot clamp.

The refusal comes from the real admission check. node-manager decides a new
sandbox against the node's `/proc/meminfo`, and a scenario raises its
`--memory-admission-reserve-mib` far above any node's memory, so every new
allocation on that node is refused the way a full node refuses it. Kind
nodes share the host kernel and read the same `/proc/meminfo`, so the reserve
is what makes one node full: the fail-fast scenario raises it on every
worker, and the clamp scenario raises it on one worker only, through a
command wrapper that adds the flag when `KUBE_NODE_NAME` matches.

Four knobs, each restored by compensation as the scope unwinds:

- node-manager's reserve, a command flag (node-manager has no env for it),
  captured before it is written and put back verbatim.
- node-ctl's `SPACE_NETWORK_SLOTS_MEMORY_CLAMP`, captured and put back.
- the API's `SPACE_CREATE_FAIL_FAST_ON_NODE_REJECTION`, captured and put back.
- a worker cordon, so a create can only be placed on one named worker.

The first three are each written under their own mutation Lease, and the
restore is verified.

Every observable is public state: the create response and how long it took,
the sandbox namespace's pods (sampled while the create runs, so a pod's
binding is seen even if the API deletes it before answering), and each
worker Node's allocatable `space.pplx.net/network-slots`.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from space_smoke.adapters import MEMORY_ADMISSION_RESERVE_FLAG
from space_smoke.capabilities import EcwCreateAttempt, NodeMemoryAdmissionControl
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# Far above any node's memory: node-manager refuses every new sandbox.
REFUSING_RESERVE_MIB = 1 << 20
# node-ctl refreshes the advertised capacity every 5 s once the clamp is on.
# The kubelet publishes it on its next device-plugin update.
CAPACITY_SETTLE_SECONDS = 90
POLL_SECONDS = 3
# How often a create's new pods are sampled while it runs.
POD_SAMPLE_SECONDS = 1

Capacity = dict[str, dict[str, int | None]]


@dataclass(frozen=True)
class TimedCreate:
    attempt: EcwCreateAttempt
    elapsed_seconds: float
    # Sandbox-namespace pods that appeared during the create and still exist, not deleting.
    leftover_pods: tuple[str, ...]
    # Every pod that appeared during the create, with the node it was bound to ("" if none).
    new_pod_nodes: dict[str, str]


class NodeMemoryAdmissionAssertions:
    def __init__(self, fixture: KindEcw, control: NodeMemoryAdmissionControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._held: list[Compensation] = []
        self._restores: list[tuple[str, Callable[[], None], Callable[[], bool], Compensation]] = []
        self._node_manager_command: tuple[str, ...] | None = None

    # --- scoped knobs -----------------------------------------------------------

    @contextmanager
    def scoped_knobs(self) -> Iterator[None]:
        """Take the three Leases, capture the standing values and put them back as the body unwinds.

        The scope's compensations stay armed as the backstop. A failed
        body's error is the one that propagates; a restore failure is
        attached to it as a note.
        """

        self._take_knobs()
        try:
            yield
        except BaseException as body_error:
            try:
                self.restore_standing()
            except Exception as restore_error:  # noqa: BLE001 - attached to the body's error below
                self._fixture.record("memory_admission_restore_failure", repr(restore_error)[:1_000])
                body_error.add_note(
                    f"restoring the standing memory admission knobs also failed: {restore_error!r}; "
                    "the scope's restore compensations stay armed"
                )
            raise
        self.restore_standing()

    def _take_knobs(self) -> None:
        for name, acquire in (
            ("node-manager", self._control.acquire_node_manager_mutation),
            ("node-ctl", self._control.acquire_node_ctl_mutation),
            ("public API", self._control.acquire_public_api_mutation),
        ):
            self._held.append(self._scope.defer(f"release the {name} mutation lease", acquire(), critical=True))

        command = self._control.node_manager_command()
        clamp = self._control.network_slots_memory_clamp_env()
        fail_fast = self._control.create_fail_fast_env()
        self._node_manager_command = command
        self._fixture.record("node_manager_command_before", list(command))
        self._fixture.record("memory_clamp_env_before", clamp)
        self._fixture.record("create_fail_fast_env_before", fail_fast)
        # No chart sets these, so a value here is one an earlier run left behind.
        # The reserve flag counts anywhere in the command, including inside a
        # leaked per-node `sh -c` wrapper.
        leaked_reserve = any(MEMORY_ADMISSION_RESERVE_FLAG in arg for arg in command)
        self._fixture.verify(
            clamp is None and fail_fast is None and not leaked_reserve,
            "the memory clamp, create fail-fast env, or node-manager reserve flag is already set; "
            "an earlier run likely left it behind",
            clamp=clamp,
            fail_fast=fail_fast,
            node_manager_command=list(command),
        )
        # Armed before any write, so a write that half-applies is still put back.
        self._arm(
            "node-manager memory admission reserve",
            lambda: self._control.set_node_manager_memory_admission_reserve(command, None),
            lambda: self._control.node_manager_command() == command,
        )
        self._arm(
            "node-ctl memory clamp",
            lambda: self._control.set_network_slots_memory_clamp_env(clamp),
            lambda: self._control.network_slots_memory_clamp_env() == clamp,
        )
        self._arm(
            "API create fail-fast",
            lambda: self._control.set_create_fail_fast_env(fail_fast),
            lambda: self._control.create_fail_fast_env() == fail_fast,
        )

    def _arm(self, name: str, restore: Callable[[], None], restored: Callable[[], bool]) -> None:
        compensation = self._scope.defer(f"restore the standing {name}", restore, critical=True)
        self._restores.append((name, restore, restored, compensation))

    def restore_standing(self) -> None:
        """Put every knob back, check each took, then release the Leases."""

        while self._restores:
            name, restore, restored, compensation = self._restores[-1]
            restore()
            self._fixture.verify(restored(), f"the standing {name} did not come back")
            compensation.disarm()
            self._restores.pop()
        while self._held:
            self._held.pop().release()

    # --- the knobs ----------------------------------------------------------------

    def refuse_new_sandboxes(self, nodes: Sequence[str] | None = None) -> None:
        """Roll node-manager with a reserve no node can meet, on `nodes` only when given."""

        if self._node_manager_command is None:
            raise RuntimeError("scoped_knobs first")
        self._control.set_node_manager_memory_admission_reserve(
            self._node_manager_command, REFUSING_RESERVE_MIB, nodes=nodes
        )

    @contextmanager
    def cordoned(self, node: str) -> Iterator[None]:
        """Keep the scheduler off `node` for the body; uncordoned as it unwinds."""

        release = self._scope.defer(
            "release the worker cordon lease", self._control.acquire_worker_cordon_mutation(), critical=True
        )
        uncordon = self._scope.defer(
            f"uncordon {node}", lambda: self._control.uncordon_kube_node(node), critical=True
        )
        try:
            self._control.cordon_kube_node(node)
            yield
        finally:
            self._control.uncordon_kube_node(node)
            uncordon.disarm()
            release.release()

    def admit_new_sandboxes(self) -> None:
        if self._node_manager_command is None:
            raise RuntimeError("scoped_knobs first")
        self._control.set_node_manager_memory_admission_reserve(self._node_manager_command, None)

    def set_fail_fast(self, enabled: bool) -> None:
        expected = "true" if enabled else None
        self._control.set_create_fail_fast_env(expected)
        actual = self._control.create_fail_fast_env()
        self._fixture.verify(actual == expected, "the API create fail-fast env did not take", actual=actual)

    def set_clamp(self, enabled: bool) -> None:
        expected = "true" if enabled else None
        self._control.set_network_slots_memory_clamp_env(expected)
        actual = self._control.network_slots_memory_clamp_env()
        self._fixture.verify(actual == expected, "the node-ctl memory clamp env did not take", actual=actual)

    # --- observations -------------------------------------------------------------

    def data_plane_ready_budget_seconds(self) -> int:
        """The create's data-plane ready budget the API runs with (the binary default when unset)."""

        key = "SPACE_CREATE_DATA_PLANE_READY_TIMEOUT_SECS"
        value = self._control.public_api_env((key,))[key]
        return int(value) if value is not None else 120

    def workers(self) -> list[str]:
        workers = self._control.sandbox_worker_nodes()
        self._fixture.verify(bool(workers), "the cluster has no sandbox worker")
        return workers

    def timed_create(self, label: str) -> TimedCreate:
        """One raw create, timed, with the sandbox-namespace pods it left behind.

        A 503 is definitive here: the API answers it for a refused or
        unplaceable pod only after deleting the sandbox, which `leftover_pods`
        and cleanup's reconciliation (accepting no match) both check.
        """

        before = {pod.pod_name for pod in self._control.all_sandbox_pods()}
        # Pods are sampled while the create runs too, so a pod the API binds
        # and deletes before it answers still shows where it was bound.
        nodes: dict[str, str] = {}
        done = threading.Event()

        def sample() -> None:
            for pod in self._control.all_sandbox_pods():
                if pod.pod_name not in before and (pod.node_name or pod.pod_name not in nodes):
                    nodes[pod.pod_name] = pod.node_name

        sample_errors: list[BaseException] = []

        def sampler() -> None:
            try:
                while not done.wait(POD_SAMPLE_SECONDS):
                    sample()
            except Exception as error:  # noqa: BLE001 - reported after join
                sample_errors.append(error)

        thread = threading.Thread(target=sampler, name="create-pod-sampler", daemon=True)
        started = time.monotonic()
        thread.start()
        try:
            attempt = self._fixture.sandboxes.attempt_create(definitive_statuses=(503,))
        finally:
            # Before join, so a sampler mid-listing doesn't add to the create's time.
            elapsed = time.monotonic() - started
            done.set()
            thread.join()
        self._fixture.verify(
            not sample_errors,
            "sampling the create's pods failed, so where its pod was bound is unknown",
            error=repr(sample_errors[0])[:600] if sample_errors else None,
        )
        new = [pod for pod in self._control.all_sandbox_pods() if pod.pod_name not in before]
        sample()
        timed = TimedCreate(
            attempt=attempt,
            elapsed_seconds=round(elapsed, 1),
            leftover_pods=tuple(pod.pod_name for pod in new if not pod.deleting),
            new_pod_nodes=dict(sorted(nodes.items())),
        )
        self._fixture.record(
            f"create_{label}",
            {
                "status": attempt.status_code,
                "elapsed_seconds": timed.elapsed_seconds,
                "sandbox_id": attempt.sandbox_id,
                "body": attempt.body[:600],
                "leftover_pods": list(timed.leftover_pods),
                "new_pod_nodes": timed.new_pod_nodes,
            },
        )
        return timed

    def capacity(self) -> Capacity:
        return {
            node: {
                "advertised": self._control.advertised_network_slots(node),
                "bound": self._control.bound_network_slot_requests(node),
            }
            for node in self.workers()
        }

    def expect_capacity(self, label: str, settled: Callable[[Capacity], bool], message: str) -> Capacity:
        deadline = time.monotonic() + CAPACITY_SETTLE_SECONDS
        seen = self.capacity()
        while not settled(seen) and time.monotonic() < deadline:
            self._fixture.wait(POLL_SECONDS)
            seen = self.capacity()
        self._fixture.record(f"capacity_{label}", seen)
        self._fixture.verify(settled(seen), message, capacity=seen)
        return seen

