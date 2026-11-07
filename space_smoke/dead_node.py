"""The dead-node verb's dry run, proven against a Kind worker that really died.

The cluster-controller loop `dead_node`
(crates/l40_api/cluster/controller/server/src/dead_node/) judges each node
whose heartbeat is stale and reports, per outcome, what reconciling the
sandbox rows it holds would do, without writing anything but its report. It
ships off and reads its config once at startup, so the scenario scopes a
`dry-run` config to itself: the controller Deployment's `SPACE_DEAD_NODE_*`
env is captured, overridden (mode, and the test-hook tick, absence window,
upload settle window and instance authority), and put back in a scenario
phase, under the controller-mutation Lease the retention scenarios share.

The node fault is one Kind worker that runs no stack pod: its container is
stopped and its Kubernetes Node deleted, so its heartbeat goes stale, the
Node is gone from the reflector and from a direct GET, and Kind's fake EC2
answers DescribeInstances for its name with no instance, since it derives
every answer from the Node. The worker is started again in the scenario's
last phase, and the start is armed as a compensation before the stop.

The observables are state: the admin route `GET /dead-node-dry-runs`, and the
sandbox rows, read before and after the dry run. The controller's log line
per node is evidence only.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    AdminNodeView,
    DeadNodeControl,
    DeadNodeDryRun,
    SandboxRowState,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

MODE_ENV = "SPACE_DEAD_NODE_MODE"
TICK_ENV = "SPACE_DEAD_NODE_TEST_TICK_SECS"
ABSENCE_WINDOW_ENV = "SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS"
UPLOAD_SETTLE_ENV = "SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS"
INSTANCE_AUTHORITY_ENV = "SPACE_DEAD_NODE_TEST_INSTANCE_AUTHORITY"

# The only controller env this scenario may touch, and the closed vocabulary
# its overrides come from.
DEAD_NODE_ENV_KEYS: tuple[str, ...] = (
    MODE_ENV,
    TICK_ENV,
    ABSENCE_WINDOW_ENV,
    UPLOAD_SETTLE_ENV,
    INSTANCE_AUTHORITY_ENV,
)
_MODE_VALUES = frozenset({"off", "dry-run"})
_AUTHORITY_VALUES = frozenset({"ec2"})
_SECONDS_VALUE = re.compile(r"[1-9][0-9]{0,3}")

# Sandbox actions the harness may ask the native API for.
NATIVE_ACTIONS = frozenset({"suspend", "pause", "stop"})

# What the verb reports for a row of each state the scenario creates; the
# rule itself is `dead_node::outcome::row_outcome`.
STARTED = "started"
PAUSED = "paused"
STOPPED = "stopped"
SUSPENDED_REMOTE = "suspended with a remote snapshot"
SUSPENDING_LOCAL = "suspending with its upload blocked"
EXPECTED_OUTCOME: Mapping[str, str] = {
    STARTED: "lost",
    PAUSED: "lost",
    # A plain stop records no stop artifact, and none of these sandboxes has
    # a snapshot.
    STOPPED: "lost",
    SUSPENDED_REMOTE: "stays_suspended",
    # Kind's suspend is durable: with uploads blocked the row stays
    # `suspending`, its only copy on the node. Once the dead node's upload has
    # been quiet past the settle window it no longer makes the row wait.
    SUSPENDING_LOCAL: "lost",
}
# Earlier runs leave `deleted` tombstones pinned to a worker; they are
# reported apart from the outcomes this scenario creates.
TOMBSTONE_OUTCOME = "references_cleared"


def validate_dead_node_keys(values: Mapping[str, object]) -> None:
    unknown = sorted(set(values) - set(DEAD_NODE_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing dead-node env keys {unknown}")


def validate_dead_node_overrides(overrides: Mapping[str, str]) -> None:
    validate_dead_node_keys(overrides)
    for key, value in overrides.items():
        if key == MODE_ENV:
            allowed = value in _MODE_VALUES
        elif key == INSTANCE_AUTHORITY_ENV:
            allowed = value in _AUTHORITY_VALUES
        else:
            allowed = _SECONDS_VALUE.fullmatch(value) is not None
        if not allowed:
            raise ValueError(f"refusing {key}={value!r}")


def _evidence(entry: DeadNodeDryRun) -> dict[str, object]:
    """The entry as JSON-safe evidence."""

    return {**entry.__dict__, "observed_at": entry.observed_at.isoformat()}


@dataclass(frozen=True)
class CreatedSandbox:
    sandbox_id: str
    state: str


class DeadNodeAssertions:
    """Kill one worker under sandboxes in known states and judge the dry run."""

    def __init__(self, fixture: KindEcw, control: DeadNodeControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._node: AdminNodeView | None = None
        self._sandboxes: list[CreatedSandbox] = []
        self._store_back: Compensation | None = None
        self._restart: Compensation | None = None
        self._rows_before: list[SandboxRowState] | None = None
        # The database clock just before the latest config was applied; only
        # an entry written after it judges that config.
        self._configured_at: datetime | None = None

    @property
    def node(self) -> AdminNodeView:
        if self._node is None:
            raise RuntimeError("choose_worker() must run first")
        return self._node

    # --- preconditions --------------------------------------------------------

    def choose_worker(self) -> AdminNodeView:
        """A worker no stack pod runs on, on a fleet with no node mid-drain and
        another worker to keep the stack running."""

        nodes = self._control.admin_nodes()
        draining = [n for n in nodes if n.drain_phase is not None]
        self._fixture.verify(
            not draining,
            "a node is already mid-drain; the verb skips it and the fleet is not clean",
            draining=[(n.k8s_node_name, n.drain_phase) for n in draining],
        )
        pool = set(self._control.sandbox_worker_nodes())
        workers = [n for n in nodes if n.k8s_node_name in pool]
        candidates = [n for n in workers if not self._control.node_stack_pods(n.k8s_node_name)]
        self._fixture.verify(
            len(workers) >= 2 and bool(candidates),
            "the scenario needs two sandbox workers, one of them running no stack pod, to stop one",
            workers={n.k8s_node_name: self._control.node_stack_pods(n.k8s_node_name) for n in workers},
        )
        self._node = candidates[0]
        self._fixture.record("dead_worker", self._node.k8s_node_name)
        self._fixture.record("dead_node_id", self._node.id)
        return self._node

    @contextmanager
    def only_worker_schedulable(self) -> Iterator[None]:
        """Cordon every other worker while the body creates sandboxes, so they
        all land on the chosen one."""

        node = self.node
        others = [
            name for name in self._control.sandbox_worker_nodes() if name != node.k8s_node_name
        ]
        release = self._control.acquire_worker_cordon_mutation()
        lease = self._scope.defer("release the worker cordon lease", release)
        uncordons = []
        try:
            for other in others:
                uncordons.append(
                    self._scope.defer(
                        f"uncordon {other}",
                        lambda name=other: self._control.uncordon_kube_node(name),
                        critical=True,
                    )
                )
                self._control.cordon_kube_node(other)
            yield
        finally:
            for uncordon in uncordons:
                uncordon.release()
            lease.release()

    def adopt(self, sandbox_id: str, state: str) -> None:
        """Track a sandbox the body created, after checking it runs on the worker."""

        placement = self._control.sandbox_pod_placement(sandbox_id)
        self._fixture.verify(
            placement is not None and placement.node_name == self.node.k8s_node_name,
            "a sandbox created with every other worker cordoned is not on the chosen worker",
            sandbox_id=sandbox_id,
            placed_on=None if placement is None else placement.node_name,
            worker=self.node.k8s_node_name,
        )
        self._sandboxes.append(CreatedSandbox(sandbox_id, state))

    def adopt_fixture_sandbox_if_resident(self) -> None:
        """The fixture's own sandbox was created before the cordon; if it runs
        on the chosen worker it is one more `started` row there."""

        sandbox_id = self._fixture.sandbox.sandbox_id
        placement = self._control.sandbox_pod_placement(sandbox_id)
        if placement is not None and placement.node_name == self.node.k8s_node_name:
            self._sandboxes.append(CreatedSandbox(sandbox_id, STARTED))
            self._fixture.record("fixture_sandbox_on_dead_worker", sandbox_id)

    # --- putting the sandboxes into their states ------------------------------

    def _await_lifecycle(self, sandbox_id: str, lifecycle: str, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            residency = self._control.sandbox_residency(sandbox_id)
            if residency.lifecycle == lifecycle:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the sandbox never reached {lifecycle}",
                sandbox_id=sandbox_id,
                lifecycle=residency.lifecycle,
            )
            self._fixture.wait(2)

    def act(self, sandbox_id: str, action: str, lifecycle: str, *, timeout: float = 300) -> None:
        """Ask the native API for `action` and wait for the row to reach `lifecycle`."""

        if action not in NATIVE_ACTIONS:
            raise ValueError(f"refusing native action {action!r}")
        operation = self._control.native_sandbox_action(sandbox_id, action)
        self._fixture.record(f"{action}_operation_{sandbox_id}", operation)
        self._await_lifecycle(sandbox_id, lifecycle, timeout=timeout)

    def suspend_remote(self, sandbox_id: str, *, timeout: float = 300) -> None:
        """Suspend, and wait until the API's portability predicate holds."""

        self.act(sandbox_id, "suspend", "suspended", timeout=timeout)
        deadline = time.monotonic() + timeout
        while not self._control.restore_portable(sandbox_id):
            self._fixture.verify(
                time.monotonic() < deadline,
                "the suspended sandbox never had a durable snapshot",
                sandbox_id=sandbox_id,
            )
            self._fixture.wait(3)

    def block_uploads(self, *, timeout: float = 120) -> None:
        """Scale the object store to zero; the scale-back is armed before the write."""

        previous = self._control.object_store_replicas()

        def back() -> None:
            self._control.scale_object_store(max(previous, 1))
            self._control.wait_object_store_rollout(timeout=180)

        self._store_back = self._scope.defer("scale the object store back up", back, critical=True)
        self._control.scale_object_store(0)
        self._control.wait_object_store_gone(timeout=timeout)

    def suspend_local_only(self, sandbox_id: str, *, timeout: float = 180) -> None:
        """Suspend with uploads blocked: `suspending`, its upload in `creating`.

        Kind's suspend waits for the durable copy, so the row stays
        `suspending` while the node holds the only copy. A `suspended` row
        without a durable copy is the crate tests' to cover.
        """

        if self._store_back is None:
            raise RuntimeError("block_uploads() must run first")
        self.act(sandbox_id, "suspend", "suspending", timeout=timeout)
        deadline = time.monotonic() + timeout
        while "creating" not in self._control.snapshot_states(sandbox_id):
            self._fixture.verify(
                time.monotonic() < deadline,
                "no in-flight upload appeared for the suspend with uploads blocked",
                sandbox_id=sandbox_id,
                snapshot_states=self._control.snapshot_states(sandbox_id),
            )
            self._fixture.wait(3)

    # --- the fault ------------------------------------------------------------

    def kill_worker(self) -> None:
        """Stop the worker and delete its Node, its restart armed first with
        the Node's labels read before anything changes; then let uploads
        through again, which the dead worker can no longer use."""

        node = self.node
        others = [s.sandbox_id for s in self._sandboxes]
        foreign = self._control.other_live_rows_on_node(node.id, others)
        self._fixture.verify(
            not foreign,
            "the worker holds live sandboxes this scenario did not create; their outcomes "
            "would be counted with this scenario's",
            foreign=foreign,
        )
        labels = self._control.kind_node_labels(node.k8s_node_name)
        self._restart = self._scope.defer(
            f"start Kind worker {node.k8s_node_name} again",
            lambda: self._control.start_kind_worker(node.k8s_node_name, labels, timeout=300),
            critical=True,
        )
        self._control.stop_kind_worker(node.k8s_node_name)
        if self._store_back is not None:
            self._store_back.release()
        self._rows_before = self._control.sandbox_row_states(others)
        self._fixture.record("rows_before", [row.__dict__ for row in self._rows_before])

    @contextmanager
    def scoped_dry_run(self) -> Iterator[Callable[[Mapping[str, str]], None]]:
        """Capture the standing dead-node env and put it back when the body ends.

        Yields the setter, so the body can switch configs inside the scope.
        Each call first reads the database clock, so `expect_dry_run` accepts
        only an entry written under that config.
        """

        release = self._control.acquire_controller_mutation()
        lease = self._scope.defer("release the controller mutation lease", release)
        standing = self._control.dead_node_env()
        self._fixture.record("standing_dead_node_env", standing)
        restore = self._scope.defer(
            "put the standing dead-node env back",
            lambda: self._control.restore_dead_node_env(standing),
            critical=True,
        )

        def configure(overrides: Mapping[str, str]) -> None:
            self._configured_at = self._control.database_now()
            self._control.set_dead_node_env(overrides)

        try:
            yield configure
        finally:
            with self._fixture.phase("put the standing dead-node env back"):
                restore.release()
                lease.release()

    # --- verdicts -------------------------------------------------------------

    def expect_dry_run(self, *, reason: str, timeout: float = 600) -> DeadNodeDryRun:
        """The route reports the worker lost for `reason` in an entry written
        after the latest config was applied, and every sandbox the scenario
        created under the outcome its state predicts, with counts for exactly
        those rows."""

        node = self.node
        configured_at = self._configured_at
        if configured_at is None:
            raise RuntimeError("configure a dry run in scoped_dry_run() first")
        deadline = time.monotonic() + timeout
        while True:
            entry = next(
                (e for e in self._control.dead_node_dry_runs() if e.node_id == node.id), None
            )
            if (
                entry is not None
                and entry.observed_at > configured_at
                and entry.verdict == "lost"
                and entry.reason == reason
            ):
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the dry run never judged the stopped worker lost under the latest config",
                expected_reason=reason,
                configured_at=configured_at.isoformat(),
                entry=None if entry is None else _evidence(entry),
            )
            self._fixture.wait(5)

        expected_counts: dict[str, int] = {}
        for sandbox in self._sandboxes:
            outcome = EXPECTED_OUTCOME[sandbox.state]
            expected_counts[outcome] = expected_counts.get(outcome, 0) + 1
        counted = {k: v for k, v in entry.counts.items() if k != TOMBSTONE_OUTCOME}
        listed = {
            sandbox.sandbox_id: next(
                (o for o, ids in entry.samples.items() if sandbox.sandbox_id in ids), None
            )
            for sandbox in self._sandboxes
        }
        expected_listed = {s.sandbox_id: EXPECTED_OUTCOME[s.state] for s in self._sandboxes}
        self._fixture.verify(
            not entry.rows_truncated and counted == expected_counts and listed == expected_listed,
            "the dry run's outcomes are not the ones the scenario's sandbox states predict",
            expected_counts=expected_counts,
            counts=dict(entry.counts),
            expected_outcomes=expected_listed,
            listed_outcomes=listed,
            states={s.sandbox_id: s.state for s in self._sandboxes},
            rows_truncated=entry.rows_truncated,
        )
        self._fixture.record(f"dry_run_{reason}", _evidence(entry))
        return entry

    def expect_rows_unchanged(self) -> None:
        """No sandbox row the scenario created changed after the worker died."""

        before = self._rows_before
        if before is None:
            raise RuntimeError("kill_worker() must run first")
        after = self._control.sandbox_row_states([row.sandbox_id for row in before])
        self._fixture.verify(
            after == before,
            "a sandbox row changed while the verb ran in dry-run",
            before=[row.__dict__ for row in before],
            after=[row.__dict__ for row in after],
        )

    def restart_worker(self, *, delete_timeout: float = 1200) -> None:
        """Start the worker again, and delete the sandbox whose suspend it held.

        A delete conflicts while the suspend is in flight, and the suspend
        resolves only once the returning node finishes or gives up its upload,
        which can take longer than the scenario cleanup's wait. So the delete
        is asked for here, again each minute, until the row is gone.
        """

        if self._restart is None:
            raise RuntimeError("kill_worker() must run first")
        self._restart.release()
        for sandbox in self._sandboxes:
            if sandbox.state != SUSPENDING_LOCAL:
                continue
            deadline = time.monotonic() + delete_timeout
            asked = 0.0
            while True:
                rows = self._control.sandbox_row_states([sandbox.sandbox_id])
                if not rows or rows[0].lifecycle == "deleted":
                    break
                self._fixture.verify(
                    time.monotonic() < deadline,
                    "the sandbox whose suspend the dead node held could not be deleted",
                    sandbox_id=sandbox.sandbox_id,
                    lifecycle=rows[0].lifecycle,
                )
                if time.monotonic() - asked >= 60:
                    self._control.native_delete_sandbox(sandbox.sandbox_id)
                    asked = time.monotonic()
                self._fixture.wait(5)


def created_states() -> Sequence[str]:
    """The states the scenario creates, in creation order."""

    return (STARTED, PAUSED, STOPPED, SUSPENDED_REMOTE, SUSPENDING_LOCAL)
