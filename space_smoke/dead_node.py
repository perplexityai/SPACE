"""The dead-node verb, proven against a Kind worker that really died.

The cluster-controller loop `dead_node`
(crates/l40_api/cluster/controller/server/src/dead_node/) judges the exact
EC2 instances each node's sandbox rows ran on and the node itself, reports
per outcome what reconciling the rows does, and in `live` does it. It ships
off and reads its config once at startup, so a scenario scopes its config to
itself: the controller Deployment's `SPACE_DEAD_NODE_*` env is captured,
overridden (mode, and the test-hook tick, absence window, upload settle
window and instance authority), and put back in a scenario phase, under the
controller-mutation Lease the retention scenarios share.

Instances: Kind registers its workers with a `kind://` providerID, so the
scenario re-registers the chosen worker and a reference worker under fake
AWS providerIDs (`aws:///kind-a/i-<17 hex>`). Their instances are recorded
in `node_k8s`, sandbox executions placed on them are stamped with them, and
Kind's fake EC2 answers for them by ID. The providerIDs stay after the
scenario: nothing on Kind reads them but the instance recording.

The node fault is the chosen worker, which runs no stack pod: its container
is stopped, its Kubernetes Node deleted and its instance marked terminated in
the fake EC2, so its heartbeat goes stale, the Node is gone from the
reflector and from a direct GET, and EC2 lists its instance `terminated`.
The worker is started again in the scenario's last phase, under a new
providerID, as a same-name replacement would be; the start is armed as a
compensation before the stop.

The observables are state: the admin node listing's `dead_node_status`, the node
row and the sandbox rows. The controller's log line per node is evidence
only.
"""

from __future__ import annotations

import re
import secrets
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    AdminNodeView,
    DeadNodeControl,
    DeadNodeStatus,
    SandboxRowState,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

MODE_ENV = "SPACE_DEAD_NODE_MODE"
TICK_ENV = "SPACE_DEAD_NODE_TEST_TICK_SECS"
ABSENCE_WINDOW_ENV = "SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS"
UPLOAD_SETTLE_ENV = "SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS"
# The node-drain terminator: `aws` gives the verb Kind's fake EC2 to judge by,
# and the v2 drain its terminate.
TERMINATOR_KIND_ENV = "SPACE_NODE_DRAIN_TERMINATOR_KIND"
# The drain's v2, which the controller runs only with the verb `live` and the
# `aws` terminator.
DRAIN_V2_ENABLED_ENV = "SPACE_DRAIN_V2_ENABLED"

# The only controller env this scenario may touch, and the closed vocabulary
# its overrides come from.
DEAD_NODE_ENV_KEYS: tuple[str, ...] = (
    MODE_ENV,
    TICK_ENV,
    ABSENCE_WINDOW_ENV,
    UPLOAD_SETTLE_ENV,
    TERMINATOR_KIND_ENV,
    DRAIN_V2_ENABLED_ENV,
)
_MODE_VALUES = frozenset({"off", "dry-run", "live"})
_TERMINATOR_VALUES = frozenset({"aws", "kube-delete"})
_FLAG_VALUES = frozenset({"true", "false"})
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
# What a row selects to without an instance authority: nothing.
UNSELECTED = "unselected"


@dataclass(frozen=True)
class LiveRow:
    """The row a live reconcile leaves: its lifecycle, and whether it keeps
    the dead node as its last target (a row written off does; a restorable
    one is unplaced from it)."""

    lifecycle: str
    keeps_target: bool


# What `live` leaves each created row as; the writes are
# `pplx_space_sandbox_store::dead_node` and `::lost`.
LIVE_ROW: Mapping[str, LiveRow] = {
    STARTED: LiveRow("failed", keeps_target=True),
    PAUSED: LiveRow("failed", keeps_target=True),
    STOPPED: LiveRow("failed", keeps_target=True),
    SUSPENDED_REMOTE: LiveRow("suspended", keeps_target=False),
    SUSPENDING_LOCAL: LiveRow("failed", keeps_target=True),
}

_FAKE_INSTANCE = re.compile(r"i-[0-9a-f]{17}")
_FAKE_PROVIDER = re.compile(r"aws:///kind-a/(i-[0-9a-f]{17})")


def validate_fake_instance_id(instance_id: str) -> None:
    if _FAKE_INSTANCE.fullmatch(instance_id) is None:
        raise ValueError(f"refusing instance id {instance_id!r}")


def validate_fake_provider_id(provider_id: str) -> None:
    if _FAKE_PROVIDER.fullmatch(provider_id) is None:
        raise ValueError(f"refusing providerID {provider_id!r}")


def fake_provider_id() -> tuple[str, str]:
    """A new fake AWS providerID and the instance it names."""

    instance_id = "i-" + secrets.token_hex(9)[:17]
    return f"aws:///kind-a/{instance_id}", instance_id


def await_recorded_instance(
    fixture: KindEcw, control: DeadNodeControl, node_name: str, node_id: str, instance_id: str,
    *, timeout: float,
) -> None:
    """Wait until the node's row records `instance_id`."""

    deadline = time.monotonic() + timeout
    while control.node_recorded_instance(node_id) != instance_id:
        fixture.verify(
            time.monotonic() < deadline,
            "node registration never recorded the worker's fake instance",
            worker=node_name,
            expected=instance_id,
            recorded=control.node_recorded_instance(node_id),
        )
        fixture.wait(3)


def register_fake_instance(
    fixture: KindEcw, control: DeadNodeControl, node_name: str, node_id: str, *, timeout: float = 300
) -> str:
    """Re-register the worker under a new fake AWS providerID and wait until
    its row records the instance; returns it. Re-registering deletes the
    pods on the worker."""

    provider_id, instance_id = fake_provider_id()
    labels = control.kind_node_labels(node_name)
    control.set_kind_worker_provider_id(node_name, provider_id, labels, timeout=timeout)
    await_recorded_instance(fixture, control, node_name, node_id, instance_id, timeout=timeout)
    return instance_id


@contextmanager
def scoped_dead_node_env(
    fixture: KindEcw, control: DeadNodeControl, scope: ResourceScope
) -> Iterator[Callable[[Mapping[str, str]], None]]:
    """Hold the controller-mutation Lease, capture the standing dead-node env,
    and put it back when the body ends; yields the setter, so the body can
    switch configs inside the scope."""

    release = control.acquire_controller_mutation()
    lease = scope.defer("release the controller mutation lease", release)
    standing = control.dead_node_env()
    fixture.record("standing_dead_node_env", standing)
    restore = scope.defer(
        "put the standing dead-node env back",
        lambda: control.restore_dead_node_env(standing),
        critical=True,
    )
    try:
        yield control.set_dead_node_env
    finally:
        with fixture.phase("put the standing dead-node env back"):
            restore.release()
            lease.release()


def validate_dead_node_keys(values: Mapping[str, object]) -> None:
    unknown = sorted(set(values) - set(DEAD_NODE_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing dead-node env keys {unknown}")


def validate_dead_node_overrides(overrides: Mapping[str, str]) -> None:
    validate_dead_node_keys(overrides)
    for key, value in overrides.items():
        if key == MODE_ENV:
            allowed = value in _MODE_VALUES
        elif key == DRAIN_V2_ENABLED_ENV:
            allowed = value in _FLAG_VALUES
        elif key == TERMINATOR_KIND_ENV:
            allowed = value in _TERMINATOR_VALUES
        else:
            allowed = _SECONDS_VALUE.fullmatch(value) is not None
        if not allowed:
            raise ValueError(f"refusing {key}={value!r}")


def _evidence(entry: DeadNodeStatus) -> dict[str, object]:
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
        self._reference: AdminNodeView | None = None
        # The instance each worker's fake providerID names, by Kube name.
        self._instances: dict[str, str] = {}
        self._labels: dict[str, str] | None = None
        self._sandboxes: list[CreatedSandbox] = []
        self._replacement_sandboxes: list[str] = []
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
        another such worker to serve as the EC2 reference."""

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
            len(candidates) >= 2,
            "the scenario needs two sandbox workers running no stack pod: one to stop, one to "
            "re-register as the EC2 reference",
            workers={n.k8s_node_name: self._control.node_stack_pods(n.k8s_node_name) for n in workers},
        )
        # Re-registering a worker deletes the pods on it; prefer to stop the
        # one the fixture's own sandbox doesn't run on.
        fixture = self._control.sandbox_pod_placement(self._fixture.sandbox.sandbox_id)
        hosting = None if fixture is None else fixture.node_name
        candidates.sort(key=lambda n: n.k8s_node_name == hosting)
        self._node, self._reference = candidates[0], candidates[1]
        self._fixture.record("dead_worker", self._node.k8s_node_name)
        self._fixture.record("dead_node_id", self._node.id)
        self._fixture.record("reference_worker", self._reference.k8s_node_name)
        return self._node

    def _register_fake_instance(self, node: AdminNodeView) -> str:
        instance_id = register_fake_instance(self._fixture, self._control, node.k8s_node_name, node.id)
        self._instances[node.k8s_node_name] = instance_id
        return instance_id

    def give_fake_instances(self) -> None:
        """Re-register the chosen worker and the reference under fake AWS
        providerIDs, and wait until their instances are recorded."""

        if self._reference is None:
            raise RuntimeError("choose_worker() must run first")
        for node in (self.node, self._reference):
            self._fixture.record(
                f"instance_{node.k8s_node_name}", self._register_fake_instance(node)
            )

    @property
    def instance(self) -> str:
        instance = self._instances.get(self.node.k8s_node_name)
        if instance is None:
            raise RuntimeError("give_fake_instances() must run first")
        return instance

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

        self._expect_placed_and_stamped(sandbox_id)
        self._sandboxes.append(CreatedSandbox(sandbox_id, state))

    def _expect_placed_and_stamped(self, sandbox_id: str) -> None:
        """The sandbox runs on the chosen worker, and its execution is stamped
        with the worker's current instance."""

        placement = self._control.sandbox_pod_placement(sandbox_id)
        self._fixture.verify(
            placement is not None and placement.node_name == self.node.k8s_node_name,
            "a sandbox created with every other worker cordoned is not on the chosen worker",
            sandbox_id=sandbox_id,
            placed_on=None if placement is None else placement.node_name,
            worker=self.node.k8s_node_name,
        )
        stamp = self._control.latest_execution_instance(sandbox_id)
        self._fixture.verify(
            stamp == self.instance,
            "the sandbox's execution is not stamped with its worker's instance",
            sandbox_id=sandbox_id,
            stamp=stamp,
            instance=self.instance,
        )

    def adopt_fixture_sandbox_if_resident(self) -> None:
        """The fixture's own sandbox was created before the cordon; if it runs
        on the chosen worker it is one more `started` row there. The
        re-registration deleted its pod with the Node, so it is a candidate
        only if it was placed again since."""

        sandbox_id = self._fixture.sandbox.sandbox_id
        placement = self._control.sandbox_pod_placement(sandbox_id)
        if (
            placement is not None
            and placement.node_name == self.node.k8s_node_name
            and self._control.latest_execution_instance(sandbox_id) == self.instance
        ):
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
        """Stop the worker, delete its Node and mark its instance terminated in
        the fake EC2, its restart under a new providerID armed first with the
        Node's labels read before anything changes; then let uploads through
        again, which the dead worker can no longer use."""

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
        self._labels = labels
        provider_id, replacement = fake_provider_id()
        self._fixture.record("replacement_instance", replacement)

        def restart() -> None:
            self._control.start_kind_worker(
                node.k8s_node_name, labels, timeout=300, provider_id=provider_id
            )
            self._instances[node.k8s_node_name] = replacement

        self._restart = self._scope.defer(
            f"start Kind worker {node.k8s_node_name} again as a replacement",
            restart,
            critical=True,
        )
        self._control.stop_kind_worker(node.k8s_node_name)
        self._control.fake_ec2_terminate(self.instance)
        if self._store_back is not None:
            self._store_back.release()
        self._rows_before = self._control.sandbox_row_states(others)
        self._fixture.record("rows_before", [row.__dict__ for row in self._rows_before])

    @contextmanager
    def scoped_config(self) -> Iterator[Callable[[Mapping[str, str]], None]]:
        """The standing dead-node env, put back when the body ends
        (`scoped_dead_node_env`).

        Each config the body sets first reads the database clock, so
        `expect_status` accepts only an entry written under that config.
        """

        with scoped_dead_node_env(self._fixture, self._control, self._scope) as set_env:

            def configure(overrides: Mapping[str, str]) -> None:
                self._configured_at = self._control.database_now()
                set_env(overrides)

            yield configure

    # --- verdicts -------------------------------------------------------------

    def expect_status(
        self,
        *,
        mode: str,
        verdict: str,
        reason: str,
        outcome_of: Callable[[str], str],
        timeout: float = 600,
    ) -> DeadNodeStatus:
        """The route reports the worker with `verdict` for `reason` in an entry
        `mode` wrote after the latest config was applied, its instance judged,
        and every sandbox the scenario created under `outcome_of(state)`, with
        counts for exactly those rows."""

        node = self.node
        configured_at = self._configured_at
        if configured_at is None:
            raise RuntimeError("configure the verb in scoped_config() first")
        deadline = time.monotonic() + timeout
        while True:
            entry = next(
                (e for e in self._control.dead_node_statuses() if e.node_id == node.id), None
            )
            if (
                entry is not None
                and entry.observed_at > configured_at
                and entry.mode == mode
                and entry.verdict == verdict
                and entry.reason == reason
            ):
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the verb never judged the stopped worker as expected under the latest config",
                expected=(mode, verdict, reason),
                configured_at=configured_at.isoformat(),
                entry=None if entry is None else _evidence(entry),
            )
            self._fixture.wait(5)

        expected_counts: dict[str, int] = {}
        for sandbox in self._sandboxes:
            outcome = outcome_of(sandbox.state)
            expected_counts[outcome] = expected_counts.get(outcome, 0) + 1
        counted = {k: v for k, v in entry.counts.items() if k != TOMBSTONE_OUTCOME}
        listed = {
            sandbox.sandbox_id: next(
                (o for o, ids in entry.samples.items() if sandbox.sandbox_id in ids), None
            )
            for sandbox in self._sandboxes
        }
        expected_listed = {s.sandbox_id: outcome_of(s.state) for s in self._sandboxes}
        self._fixture.verify(
            not entry.rows_truncated
            and counted == expected_counts
            and listed == expected_listed
            and self.instance in entry.instances,
            "the verb's outcomes are not the ones the scenario's sandbox states predict",
            expected_counts=expected_counts,
            counts=dict(entry.counts),
            expected_outcomes=expected_listed,
            listed_outcomes=listed,
            instances=dict(entry.instances),
            instance=self.instance,
            states={s.sandbox_id: s.state for s in self._sandboxes},
            rows_truncated=entry.rows_truncated,
        )
        self._fixture.record(f"status_{mode}_{reason}", _evidence(entry))
        return entry

    def expect_live_reconciled(self, *, timeout: float = 600) -> None:
        """Live mode leaves every created row as `LIVE_ROW` says, unplaced from
        the dead worker, and then deletes the worker's node row."""

        node = self.node
        ids = [s.sandbox_id for s in self._sandboxes]
        states = {s.sandbox_id: s.state for s in self._sandboxes}
        deadline = time.monotonic() + timeout
        while True:
            rows = {row.sandbox_id: row for row in self._control.sandbox_row_states(ids)}
            mismatched = {
                sandbox_id: None if row is None else row.__dict__
                for sandbox_id in ids
                if not _reconciled(rows.get(sandbox_id), LIVE_ROW[states[sandbox_id]], node.id)
                for row in [rows.get(sandbox_id)]
            }
            node_gone = not self._control.node_row_exists(node.id)
            if not mismatched and node_gone:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "live mode never left the dead worker's rows in their rules' states and "
                "deleted its node row",
                mismatched=mismatched,
                states=states,
                node_row_exists=not node_gone,
                statuses=[
                    _evidence(e) for e in self._control.dead_node_statuses() if e.node_id == node.id
                ],
            )
            self._fixture.wait(5)
        self._fixture.record(
            "rows_after_live",
            [row.__dict__ for row in self._control.sandbox_row_states(ids)],
        )

    def replace_worker(self, *, timeout: float = 600) -> str:
        """Start the worker again under a new providerID, a same-name
        replacement, and wait until its node row records the new instance."""

        if self._restart is None:
            raise RuntimeError("kill_worker() must run first")
        self._restart.release()
        await_recorded_instance(
            self._fixture, self._control, self.node.k8s_node_name, self.node.id, self.instance,
            timeout=timeout,
        )
        return self.instance

    def adopt_replacement_sandbox(self, sandbox_id: str) -> None:
        """Track a sandbox created on the replacement, stamped with its instance."""

        self._expect_placed_and_stamped(sandbox_id)
        self._replacement_sandboxes.append(sandbox_id)

    def expect_replacement_untouched(self, *, ticks: int, tick_seconds: int) -> None:
        """Over `ticks` live ticks the replacement's sandboxes keep every
        column, and its node row stays and is never judged lost."""

        node = self.node
        before = self._control.sandbox_row_states(self._replacement_sandboxes)
        self._fixture.verify(
            len(before) == len(self._replacement_sandboxes)
            and all(row.lifecycle == "started" for row in before),
            "a sandbox created on the replacement is not started",
            rows=[row.__dict__ for row in before],
        )
        for _ in range(ticks):
            self._fixture.wait(tick_seconds, reason="let the live verb tick over the replacement")
            entry = next(
                (e for e in self._control.dead_node_statuses() if e.node_id == node.id), None
            )
            # The dead worker's row, and its status with it, were deleted;
            # any `lost` entry now is about the replacement.
            self._fixture.verify(
                self._control.node_row_exists(node.id)
                and (entry is None or entry.verdict != "lost"),
                "the live verb judged the replacement lost or deleted its node row",
                entry=None if entry is None else _evidence(entry),
            )
        after = self._control.sandbox_row_states(self._replacement_sandboxes)
        self._fixture.verify(
            after == before,
            "a sandbox on the replacement changed while the verb ran live",
            before=[row.__dict__ for row in before],
            after=[row.__dict__ for row in after],
        )

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
        """Start the worker again as a replacement, and delete the sandbox
        whose suspend it held.

        A delete conflicts while the suspend is in flight, and the suspend
        resolves only once the returning node finishes or gives up its upload,
        which can take longer than the scenario cleanup's wait. So the delete
        is asked for here, again each minute, until the row is gone.
        """

        if self._restart is None:
            raise RuntimeError("kill_worker() must run first")
        self._restart.release()
        self.delete_suspending_local(delete_timeout=delete_timeout)

    def delete_suspending_local(self, *, delete_timeout: float = 1200) -> None:
        """Delete the sandbox whose suspend the dead worker held, asking again
        each minute while the delete conflicts."""

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


def _reconciled(row: SandboxRowState | None, expected: LiveRow, node_id: str) -> bool:
    return (
        row is not None
        and row.lifecycle == expected.lifecycle
        and row.placement == "unplaced"
        and row.active_node_id is None
        and row.target_node_id == (node_id if expected.keeps_target else None)
    )
