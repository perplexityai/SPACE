"""The node drain's v2 engine, proven on a real cluster.

A v2 drain seals the worker and lets its sandbox-manager make every resident
durable elsewhere (`DrainNode`); the cluster controller records what the node
reports, restores each suspended VM to its prior lifecycle through a drain
restore operation, and leaves a stopped sandbox stopped and unplaced, its
disk pinned, for a user start to boot anywhere. The scenarios here pin
residents to one worker by cordoning the other, uncordon it, and drain the
first with `engine: v2`. Every run arms, before the drain, the undrain, the
uncordon and an unseal of the worker.

Every run scopes the controller's drain v2 flag and the `aws` terminator to
itself (`v2_controller`), under the controller-mutation Lease, with the
drain restore hold (`SPACE_DRAIN_RESTORE_TEST_HOLD_SECS`) or the gone-node
loops a let-go node is handed to when the run asks for them.

A v2 drain needs the node's EC2 instance recorded (the drain terminates that
instance, and asks EC2 about it once the node stops answering), so every run
gives the drained worker a fake AWS providerID first, before any resident
lands on it. A terminate then runs as production's does: the controller
fences the instance, and the `aws` terminator asks Kind's fake EC2 to
terminate it. The fake only marks it, `shutting-down` for the delay a run
sets (`fake_ec2_delay`) and `terminated` after; once the controller records
the dispatch, the harness plays the cloud's part, stopping the worker's
container and deleting its Node. Once EC2 says the instance is terminated
and the Node is gone, the drain deletes the node's row, with no tombstone.
The worker comes back under the same name with a new providerID, a
same-name replacement, started from the stopped container with its sealed
sandbox-manager database, which the run unseals; that start is armed before
the terminate. A worker that dies mid-drain is let go once EC2 says its
instance is terminated, and the gone-node loops take its rows as any
crashed node's.
"""

from __future__ import annotations

import re
import secrets
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from space_smoke.capabilities import (
    DrainV2Control,
    DurableLifecycleSandbox,
    KindWorkerControl,
    ResidentLifecycleSandbox,
    SnapshotRecord,
    StoppableLifecycleSandbox,
)
from space_smoke.drain_restore import DRAIN_RESTORE_HOLD_ENV, DRAIN_RESTORE_SLOTS_ENV
from space_smoke.runtime import Compensation, ResourceScope

# The node-drain terminator: `aws` gives the v2 drain its terminate, and
# EC2's answer for the instance, through Kind's fake EC2.
TERMINATOR_KIND_ENV = "SPACE_NODE_DRAIN_TERMINATOR_KIND"
# Whether `:drain` may start a v2 drain.
DRAIN_V2_ENABLED_ENV = "SPACE_DRAIN_V2_ENABLED"
# How many v2 drains may evacuate their nodes at once.
DRAIN_V2_MAX_EVACUATING_ENV = "SPACE_DRAIN_V2_MAX_EVACUATING_NODES"
# The gone-node loops a node the drain lets go is handed to, off by default:
# `lost_started_sandboxes`, `orphaned_nodes`, and `orphaned_sandboxes` with
# its dry run off.
LOST_STARTED_ENABLED_ENV = "SPACE_LOST_STARTED_SANDBOXES_ENABLED"
ORPHANED_NODES_ENABLED_ENV = "SPACE_ORPHANED_NODES_ENABLED"
ORPHANED_SANDBOXES_ENABLED_ENV = "SPACE_ORPHANED_SANDBOX_RECONCILER_ENABLED"
ORPHANED_SANDBOXES_DRY_RUN_ENV = "SPACE_API_RECONCILER_DRY_RUN"

# The only controller env a drain v2 scenario may touch, and the closed
# vocabulary its overrides come from.
DRAIN_V2_ENV_KEYS: tuple[str, ...] = (
    TERMINATOR_KIND_ENV,
    DRAIN_V2_ENABLED_ENV,
    DRAIN_V2_MAX_EVACUATING_ENV,
    LOST_STARTED_ENABLED_ENV,
    ORPHANED_NODES_ENABLED_ENV,
    ORPHANED_SANDBOXES_ENABLED_ENV,
    ORPHANED_SANDBOXES_DRY_RUN_ENV,
)
# The gone-node loops on, writing: what a scenario that hands a dead node to
# them scopes to itself.
LEGACY_LOOPS_ENV = {
    LOST_STARTED_ENABLED_ENV: "true",
    ORPHANED_NODES_ENABLED_ENV: "true",
    ORPHANED_SANDBOXES_ENABLED_ENV: "true",
    ORPHANED_SANDBOXES_DRY_RUN_ENV: "false",
}
_TERMINATOR_VALUES = frozenset({"aws", "kube-delete"})
_FLAG_VALUES = frozenset({"true", "false"})
_COUNT_VALUE = re.compile(r"[1-9][0-9]{0,2}")

# The controller config every v2 run scopes to itself.
V2_CONTROLLER_ENV = {
    DRAIN_V2_ENABLED_ENV: "true",
    TERMINATOR_KIND_ENV: "aws",
}
_FAKE_PROVIDER_PREFIX = "aws:///kind-a/"
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
    return f"{_FAKE_PROVIDER_PREFIX}{instance_id}", instance_id


def await_recorded_instance(
    fixture: ScenarioFixture, control: KindWorkerControl, node_name: str, node_id: str, instance_id: str,
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
    fixture: ScenarioFixture, control: KindWorkerControl, node_name: str, node_id: str,
    *, timeout: float = 300,
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
def scoped_drain_v2_env(
    fixture: ScenarioFixture, control: KindWorkerControl, scope: ResourceScope
) -> Iterator[Callable[[Mapping[str, str]], None]]:
    """Hold the controller-mutation Lease, capture the standing drain v2 env,
    and put it back when the body ends; yields the setter, so the body can
    switch configs inside the scope."""

    release = control.acquire_controller_mutation()
    lease = scope.defer("release the controller mutation lease", release)
    standing = control.drain_v2_env()
    fixture.record("standing_drain_v2_env", standing)
    restore = scope.defer(
        "put the standing drain v2 env back",
        lambda: control.restore_drain_v2_env(standing),
        critical=True,
    )
    try:
        yield control.set_drain_v2_env
    finally:
        with fixture.phase("put the standing drain v2 env back"):
            restore.release()
            lease.release()


def validate_drain_v2_keys(values: Mapping[str, object]) -> None:
    unknown = sorted(set(values) - set(DRAIN_V2_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing drain v2 env keys {unknown}")


def validate_drain_v2_overrides(overrides: Mapping[str, str]) -> None:
    validate_drain_v2_keys(overrides)
    for key, value in overrides.items():
        if key == TERMINATOR_KIND_ENV:
            allowed = value in _TERMINATOR_VALUES
        elif key == DRAIN_V2_MAX_EVACUATING_ENV:
            allowed = _COUNT_VALUE.fullmatch(value) is not None
        else:
            allowed = value in _FLAG_VALUES
        if not allowed:
            raise ValueError(f"refusing {key}={value!r}")


# How long after the fake EC2's delay a scenario waits to see that nothing
# deleted a replacement's row: a few of the v2 loop's dispatch backoffs.
DELETE_SETTLE_SECONDS = 90
# The sandbox-manager row states in which a node may hold the sandbox's VM.
_VM_STATES = frozenset({"started", "paused", "pausing", "resuming", "starting", "suspending"})
# Guest memory a resident holds in tmpfs, random so its snapshot can't shrink it.
BALLAST = "/dev/shm/space-smoke-v2-ballast"
BALLAST_DIGEST = f"md5sum {BALLAST} | cut -d' ' -f1"

if TYPE_CHECKING:
    from space_smoke.fixtures import CommandSandboxFixture, ScenarioFixture


class DrainV2Assertions:
    """Drain the fixture sandbox's worker with the v2 engine and judge what follows."""

    def __init__(
        self,
        fixture: CommandSandboxFixture,
        control: DrainV2Control,
        create_sandbox: Callable[..., DurableLifecycleSandbox],
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._create_sandbox = create_sandbox
        self._scope = scope
        self._node_name: str | None = None
        self._node_id: str | None = None
        self._other: str | None = None
        self._uncordon_other: Compensation | None = None
        self._undrain: Compensation | None = None
        self._epoch: int | None = None
        self._instance: str | None = None
        self._replacement: str | None = None
        # Each resident a report this run read named evacuated, with its prior.
        self._reported_priors: dict[str, Any] = {}

    @staticmethod
    def bare_id(sandbox_id: str) -> str:
        return sandbox_id.removeprefix("sbx_")

    @property
    def node_name(self) -> str:
        if self._node_name is None:
            raise RuntimeError("pin_to_resident_worker() must run first")
        return self._node_name

    @property
    def node_id(self) -> str:
        if self._node_id is None:
            raise RuntimeError("pin_to_resident_worker() must run first")
        return self._node_id

    @property
    def epoch(self) -> int:
        if self._epoch is None:
            raise RuntimeError("drain() must run first")
        return self._epoch

    # --- placement ------------------------------------------------------------

    def pin_to_resident_worker(self, *, away_from_fixture: bool = False) -> str:
        """Hold the worker-cordon Lease, record the fixture's worker and cordon
        the other one; with `away_from_fixture`, the other way round, so the
        drained worker hosts no fixture pod."""

        for node in self._control.admin_nodes():
            self._fixture.verify(
                node.drain_phase is None,
                "a node is mid-drain; the v2 drain scenarios need a clean fleet",
                node=node.k8s_node_name,
                drain_phase=node.drain_phase,
            )
        placement = self._control.sandbox_pod_placement(self.bare_id(self._fixture.sandbox.sandbox_id))
        self._fixture.verify(placement is not None, "the fixture sandbox has no bound execution pod")
        assert placement is not None
        workers = self._control.sandbox_pool_nodes()
        others = [node for node in workers if node != placement.node_name]
        self._fixture.verify(
            placement.node_name in workers and len(others) == 1,
            "the scenario needs exactly two sandbox workers, one hosting the fixture sandbox",
            workers=workers,
            resident_node=placement.node_name,
        )
        release = self._control.acquire_worker_cordon_mutation()
        self._scope.defer("release the worker cordon lease", release)
        other = others[0]
        if away_from_fixture:
            other, pinned = placement.node_name, others[0]
        else:
            pinned = placement.node_name
        self._other = other
        self._uncordon_other = self._scope.defer(
            f"uncordon {other}", lambda: self._control.uncordon_kube_node(other), critical=True
        )
        self._control.cordon_kube_node(other)
        node = next(
            (n for n in self._control.admin_nodes() if n.k8s_node_name == pinned), None
        )
        self._fixture.verify(node is not None, "the resident worker has no SPACE node row")
        assert node is not None
        self._node_name = pinned
        self._node_id = node.id
        self._fixture.record("drained_worker", pinned)
        return pinned

    def give_fake_instance(self, *, timeout: float = 300) -> str:
        """The pinned worker's fake AWS instance: re-register it under a fake
        AWS providerID unless it has one, and wait until its node row records
        the instance. Run before any resident lands on it: re-registering
        deletes the pods on it."""

        node_name, node_id = self.node_name, self.node_id
        provider = self._control.kind_node_provider_id(node_name)
        if provider.startswith(_FAKE_PROVIDER_PREFIX):
            instance = provider.removeprefix(_FAKE_PROVIDER_PREFIX)
            await_recorded_instance(
                self._fixture, self._control, node_name, node_id, instance, timeout=timeout
            )
        else:
            instance = register_fake_instance(
                self._fixture, self._control, node_name, node_id, timeout=timeout
            )
        self._instance = instance
        self._fixture.record("drained_instance", instance)
        return instance

    @contextmanager
    def v2_controller(
        self,
        *,
        hold_seconds: int | None = None,
        restore_slots: int | None = None,
        max_evacuating_nodes: int | None = None,
        legacy_loops: bool = False,
    ) -> Iterator[None]:
        """Run the body with the controller's v2 loop on: the drain v2 flag
        and the `aws` terminator, against Kind's fake EC2. With
        `hold_seconds`, each claimed drain restore holds that long before its
        node work; with `restore_slots`, that many drain restores run at once
        instead of the dispatcher's default; with `max_evacuating_nodes`, at
        most that many v2 drains evacuate at once; with `legacy_loops`, the
        gone-node loops a let-go node is handed to run and write. The
        standing config goes back when the body ends."""

        env = dict(V2_CONTROLLER_ENV)
        if max_evacuating_nodes is not None:
            env[DRAIN_V2_MAX_EVACUATING_ENV] = str(max_evacuating_nodes)
        if legacy_loops:
            env.update(LEGACY_LOOPS_ENV)
        with scoped_drain_v2_env(self._fixture, self._control, self._scope) as set_env:
            set_env(env)
            overrides: dict[str, str] = {}
            if hold_seconds is not None:
                overrides[DRAIN_RESTORE_HOLD_ENV] = str(hold_seconds)
            if restore_slots is not None:
                overrides[DRAIN_RESTORE_SLOTS_ENV] = str(restore_slots)
            if not overrides:
                yield
                return
            standing = self._control.drain_restore_hold_env()
            self._fixture.record("standing_drain_restore_env", standing)
            restore = self._scope.defer(
                "put the standing drain-restore env back",
                lambda: self._control.restore_drain_restore_hold_env(standing),
                critical=True,
            )
            self._control.set_drain_restore_hold_env(overrides)
            pods = [
                {key: pod.get(key) for key in overrides}
                for pod in self._control.running_controller_drain_restore_hold_envs()
            ]
            self._fixture.verify(
                bool(pods) and all(pod == overrides for pod in pods),
                "the rolled cluster-controller does not carry the drain-restore env",
                expected=overrides,
                running_pods=pods,
            )
            try:
                yield
            finally:
                with self._fixture.phase("put the standing drain-restore env back"):
                    restore.release()

    def create_resident(self, *, idle_timeout: str | None = None) -> DurableLifecycleSandbox:
        """A run-owned sandbox, running on the pinned worker; with
        `idle_timeout`, one the node suspends once it has been idle that long."""

        sandbox = self._create_sandbox(idle_timeout=idle_timeout)
        placement = self._control.sandbox_pod_placement(self.bare_id(sandbox.sandbox_id))
        self._fixture.verify(
            placement is not None and placement.node_name == self.node_name,
            "a resident did not land on the pinned worker",
            sandbox_id=sandbox.sandbox_id,
            node=None if placement is None else placement.node_name,
        )
        return sandbox

    def pin_residents(
        self, counts: dict[str, int], *, mib: int = 64
    ) -> tuple[dict[str, DurableLifecycleSandbox], dict[str, str], dict[str, str]]:
        """Pin residents to the worker the fixture is not on, under a fake
        instance: `counts` of each lifecycle (`started`, `paused`, `stopped`),
        each VM holding `mib` of random memory first. The fixture sandbox is
        suspended and the other worker released. Returns the sandboxes, each
        one's lifecycle and each one's memory digest, by sandbox id."""

        self.pin_to_resident_worker(away_from_fixture=True)
        self.give_fake_instance()
        sandboxes: dict[str, DurableLifecycleSandbox] = {}
        lifecycles: dict[str, str] = {}
        for lifecycle, count in counts.items():
            for _ in range(count):
                sandbox = self.create_resident()
                sandboxes[sandbox.sandbox_id] = sandbox
                lifecycles[sandbox.sandbox_id] = lifecycle
        digests = {
            sandbox_id: self.fill_memory(sandbox, mib=mib) for sandbox_id, sandbox in sandboxes.items()
        }
        for sandbox_id, sandbox in sandboxes.items():
            if lifecycles[sandbox_id] == "paused":
                cast(ResidentLifecycleSandbox, sandbox).pause()
            elif lifecycles[sandbox_id] == "stopped":
                cast(StoppableLifecycleSandbox, sandbox).stop()
        self._fixture.record(
            "residents",
            {self.bare_id(sandbox_id): lifecycle for sandbox_id, lifecycle in lifecycles.items()},
        )
        cast(DurableLifecycleSandbox, self._fixture.sandbox).suspend()
        self.release_other_worker()
        self.expect_lifecycles(lifecycles)
        return sandboxes, lifecycles, digests

    def expect_started_memory(
        self,
        sandboxes: dict[str, DurableLifecycleSandbox],
        lifecycles: dict[str, str],
        digests: dict[str, str],
    ) -> None:
        """Each started resident's memory is what it held before the drain."""

        for sandbox_id, sandbox in sandboxes.items():
            if lifecycles[sandbox_id] == "started":
                self.expect_memory(sandbox, digests[sandbox_id])

    def run_on(self, sandbox: DurableLifecycleSandbox, command: str, *, timeout: float = 180) -> Any:
        """Run `command` in a run-owned sandbox, retrying while its data plane comes up."""

        deadline = time.monotonic() + timeout
        while True:
            try:
                result = sandbox.run(command, timeout=60)
            except Exception as error:  # noqa: BLE001 - retried until the deadline
                self._fixture.verify(
                    time.monotonic() < deadline,
                    "a run-owned sandbox never answered a command",
                    sandbox_id=sandbox.sandbox_id,
                    error=repr(error)[:500],
                )
                self._fixture.wait(3)
                continue
            return result

    @property
    def other_node_name(self) -> str:
        if self._other is None:
            raise RuntimeError("pin_to_resident_worker() must run first")
        return self._other

    def fill_memory(self, sandbox: DurableLifecycleSandbox, *, mib: int) -> str:
        """Fill `mib` of guest memory with random tmpfs content; its digest."""

        filled = self.run_on(
            sandbox,
            f"dd if=/dev/urandom of={BALLAST} bs=1M count={int(mib)} 2>/dev/null && {BALLAST_DIGEST}",
            timeout=300,
        )
        digest = filled.stdout.strip()
        self._fixture.verify(
            filled.exit_code == 0 and len(digest) == 32,
            "failed to fill a resident's memory",
            sandbox_id=sandbox.sandbox_id,
            stdout=filled.stdout,
        )
        return digest

    def expect_memory(self, sandbox: DurableLifecycleSandbox, digest: str) -> None:
        """The resident's memory holds what `fill_memory` put there."""

        observed = self.run_on(sandbox, BALLAST_DIGEST)
        self._fixture.verify(
            observed.exit_code == 0 and observed.stdout.strip() == digest,
            "a resident's memory did not survive the v2 drain",
            sandbox_id=sandbox.sandbox_id,
            expected=digest,
            observed=observed.stdout,
        )

    def block(self, path: str, node_name: str | None = None) -> Compensation:
        """Cut a node-local path (`object-store` or `node-api`) on the drained
        worker, or `node_name`; the unblock is armed first and returned."""

        node = node_name or self.node_name
        unblock = self._scope.defer(
            f"unblock {path} on {node}",
            lambda: self._control.unblock_node_traffic(node, path),
            critical=True,
        )
        self._control.block_node_traffic(node, path)
        self._fixture.record(f"blocked_{path}_{node}", time.time())
        return unblock

    def restart_controller(self) -> None:
        """Kill the cluster-controller and wait for its replacement."""

        killed = self._control.restart_controller(timeout=300)
        self._fixture.record("restarted_controller_pods", killed)

    @contextmanager
    def controller_down(self) -> Iterator[None]:
        """Run the body with the cluster-controller scaled to zero; it is scaled back after."""

        previous = self._control.scale_controller(0, timeout=180)

        def scale_back() -> None:
            self._control.scale_controller(previous, timeout=300)

        back = self._scope.defer("scale the cluster-controller back", scale_back, critical=True)
        try:
            yield
        finally:
            with self._fixture.phase("scale the cluster-controller back"):
                back.release()

    def restart_sandbox_manager(self) -> str:
        """Restart the drained worker's sandbox-manager; the replacement pod."""

        pod = self._control.restart_node_service(self.node_name, "sandbox-manager", timeout=300)
        self._fixture.record("restarted_sandbox_manager", pod)
        return pod

    def pending_actions(self, report: dict[str, Any]) -> dict[str, str]:
        """The report's pending work, by bare sandbox id."""

        return {
            str(entry.get("sandbox_id")): str(entry.get("action"))
            for entry in report.get("pending", [])
            if isinstance(entry, dict)
        }

    def expect_all_back(self, residents: dict[str, str], *, timeout: float = 900) -> dict[str, Any]:
        """Each resident (sandbox id -> lifecycle) ends in its lifecycle with
        one VM, on the other worker unless stopped, landed by exactly one
        drain restore unless stopped; the restores are recorded."""

        restores: dict[str, Any] = {}
        for sandbox_id, lifecycle in residents.items():
            if lifecycle == "stopped":
                self.expect_stopped_unplaced(sandbox_id)
            else:
                self.expect_back_elsewhere(sandbox_id, lifecycle, timeout=timeout)
                restores[self.bare_id(sandbox_id)] = self.drain_restores(sandbox_id)
                self.expect_landed_once(sandbox_id)
            self.expect_single_vm(sandbox_id, lifecycle)
        self._fixture.record("restores", restores)
        return restores

    def companion(self) -> DrainV2Assertions:
        """Assertions for the other worker, under this run's worker-cordon
        Lease, so a run can drain both. Its residents land on it only while
        this one's worker is cordoned."""

        if self._other is None:
            raise RuntimeError("pin_to_resident_worker() must run first")
        node = next(
            (n for n in self._control.admin_nodes() if n.k8s_node_name == self._other), None
        )
        self._fixture.verify(node is not None, "the other worker has no SPACE node row")
        assert node is not None
        other = DrainV2Assertions(self._fixture, self._control, self._create_sandbox, self._scope)
        other._node_name = self._other
        other._node_id = node.id
        other._other = self.node_name
        return other

    def cordon(self) -> None:
        """Cordon this worker, uncordoned again when the run ends."""

        node_name = self.node_name
        self._scope.defer(
            f"uncordon {node_name}", lambda: self._control.uncordon_kube_node(node_name), critical=True
        )
        self._control.cordon_kube_node(node_name)

    def uncordon(self) -> None:
        self._control.uncordon_kube_node(self.node_name)

    def status(self) -> dict[str, Any]:
        """The node's `drain_status`."""

        columns = self._control.drain_v2_status(self.node_id)
        return {} if columns is None or columns.status is None else columns.status

    def release_other_worker(self) -> None:
        """Uncordon the other worker, so the restores have somewhere to land."""

        if self._uncordon_other is not None:
            compensation, self._uncordon_other = self._uncordon_other, None
            compensation.release()

    # --- the drain ------------------------------------------------------------

    def drain(self) -> int:
        """Drain the worker with `engine: v2`; the row records the engine and its epoch."""

        node_name, node_id = self.node_name, self.node_id
        self._scope.defer(
            f"uncordon {node_name}", lambda: self._control.uncordon_kube_node(node_name), critical=True
        )
        self._undrain = self._scope.defer(
            f"undrain {node_name}", lambda: self._undrain_if_draining(node_id), critical=True
        )
        self._control.drain_node_v2(node_id)
        status = self._control.drain_v2_status(node_id)
        epoch = None if status is None or status.status is None else status.status.get("epoch")
        self._fixture.verify(
            status is not None and status.engine == "v2" and isinstance(epoch, int),
            "a v2 :drain must record the engine and the epoch with the phase",
            status=None if status is None else (status.engine, status.status),
        )
        assert isinstance(epoch, int)
        self._epoch = epoch
        # Armed after the epoch is known: a worker left sealed (a terminated
        # node comes back sealed) is opened again whatever the run reached.
        self._scope.defer(
            f"unseal {node_name}",
            lambda: self._control.unseal_activations(node_name, epoch),
            critical=True,
        )
        self._fixture.record("drain_epoch", epoch)
        return epoch

    def _undrain_if_draining(self, node_id: str) -> None:
        node = next((n for n in self._control.admin_nodes() if n.id == node_id), None)
        if node is not None and node.drain_phase is not None:
            self._control.undrain_node(node_id)

    def last_error(self) -> str | None:
        """The node's `drain_last_error`, as the admin listing shows it."""

        node = next((n for n in self._control.admin_nodes() if n.id == self.node_id), None)
        return None if node is None else node.drain_last_error

    def wait_for_last_error(self, text: str, *, timeout: float) -> str:
        """Poll the node's `drain_last_error` until it holds `text`."""

        deadline = time.monotonic() + timeout
        while True:
            error = self.last_error()
            if error is not None and text in error:
                return error
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the node's drain_last_error never held {text!r}",
                drain_last_error=error,
            )
            self._fixture.wait(3)

    def residency(self, sandbox_id: str) -> tuple[str, str | None]:
        """The sandbox row's lifecycle and active node."""

        residency = self._control.sandbox_residency(self.bare_id(sandbox_id))
        return residency.lifecycle, residency.active_node_id

    def ready_snapshots(self, sandbox_id: str) -> list[SnapshotRecord]:
        """The sandbox's ready full snapshots, oldest first."""

        return self._control.ready_full_snapshots(self.bare_id(sandbox_id))

    def expect_back_on_drained(self, sandbox_id: str, *, timeout: float = 300) -> None:
        """The sandbox is `started` on the pinned worker."""

        deadline = time.monotonic() + timeout
        while True:
            lifecycle, node = self.residency(sandbox_id)
            if lifecycle == "started" and node == self.node_id:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the sandbox never came back started on the pinned worker",
                sandbox_id=sandbox_id,
                lifecycle=lifecycle,
                active_node_id=node,
            )
            self._fixture.wait(3)

    def expect_lifecycle(self, sandbox_id: str, lifecycle: str, *, timeout: float = 600) -> None:
        """The sandbox reaches `lifecycle` on some node."""

        deadline = time.monotonic() + timeout
        while True:
            current, node = self.residency(sandbox_id)
            if current == lifecycle and node is not None:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the sandbox never reached {lifecycle}",
                sandbox_id=sandbox_id,
                lifecycle=current,
                active_node_id=node,
            )
            self._fixture.wait(3)

    def expect_lifecycle_back(self, sandbox_id: str, lifecycle: str, *, timeout: float) -> None:
        """The sandbox row goes back to `lifecycle`, wherever it is placed."""

        deadline = time.monotonic() + timeout
        while True:
            current, node = self.residency(sandbox_id)
            if current == lifecycle:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the sandbox never went back to {lifecycle}",
                sandbox_id=sandbox_id,
                lifecycle=current,
                active_node_id=node,
                restores=self.drain_restores(sandbox_id),
            )
            self._fixture.wait(3)

    def drain_phase(self) -> str | None:
        node = next((n for n in self._control.admin_nodes() if n.id == self.node_id), None)
        return None if node is None else node.drain_phase

    def terminate_status(self) -> int:
        """`POST :terminate`; the HTTP status it answered."""

        return self._control.terminate_node(self.node_id)

    def report(self) -> dict[str, Any]:
        status = self._control.drain_v2_status(self.node_id)
        report = None if status is None or status.status is None else status.status.get("report")
        if not isinstance(report, dict):
            return {}
        # A report is the node as one call found it: a resident whose row
        # left the node before the drain ended is not in the last one.
        for entry in report.get("evacuated", []):
            if isinstance(entry, dict) and isinstance(entry.get("sandbox_id"), str):
                self._reported_priors[entry["sandbox_id"]] = entry.get("prior")
        return report

    def wait_for_report(
        self, accept: Callable[[dict[str, Any]], bool], failure: str, *, timeout: float
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            report = self.report()
            if report and accept(report):
                return report
            self._fixture.verify(time.monotonic() < deadline, failure, report=report)
            self._fixture.wait(2)

    def expect_drained(
        self, *, timeout: float = 900, first_reported: dict[str, float] | None = None
    ) -> dict[str, Any]:
        """The node reports `done` under the drain's epoch and the row reaches `drained`.

        With `first_reported`, each sandbox the report names evacuated is
        noted there with the wall-clock time this poll first saw it."""

        epoch = self.epoch
        deadline = time.monotonic() + timeout
        while True:
            node = next((n for n in self._control.admin_nodes() if n.id == self.node_id), None)
            report = self.report()
            if first_reported is not None:
                for entry in report.get("evacuated", []):
                    if isinstance(entry, dict) and isinstance(entry.get("sandbox_id"), str):
                        first_reported.setdefault(entry["sandbox_id"], time.time())
            if node is not None and node.drain_phase == "drained":
                self._fixture.verify(
                    report.get("done") is True and report.get("sealed_epoch") == epoch,
                    "the node reached drained without a done report under its epoch",
                    report=report,
                )
                self._fixture.record("drained_report", report)
                return report
            self._fixture.verify(
                time.monotonic() < deadline,
                "the v2 drain never reached drained",
                drain_phase=None if node is None else node.drain_phase,
                report=report,
            )
            self._fixture.wait(3)

    def expect_evacuated(self, report: dict[str, Any], expected: dict[str, str]) -> None:
        """The report, or one this run read before it, names each resident
        with the lifecycle it had."""

        priors = dict(self._reported_priors)
        priors.update(
            {
                entry.get("sandbox_id"): entry.get("prior")
                for entry in report.get("evacuated", [])
                if isinstance(entry, dict)
            }
        )
        wanted = {self.bare_id(sandbox_id): prior for sandbox_id, prior in expected.items()}
        # One whose row left the node between two of this run's reads was
        # reported only to the controller: its drain restore is the
        # controller's record of it, and the restore's landing in the prior
        # lifecycle is checked on its own.
        unseen = {
            sandbox_id: self.drain_restores(sandbox_id)
            for sandbox_id, prior in wanted.items()
            if sandbox_id not in priors and prior != "stopped"
        }
        self._fixture.record("evacuated_unseen_by_the_run", unseen)
        self._fixture.verify(
            all(
                priors.get(sandbox_id) == prior or bool(unseen.get(sandbox_id))
                for sandbox_id, prior in wanted.items()
            ),
            "the node did not report each resident evacuated with its prior lifecycle",
            reported=priors,
            expected=wanted,
            unseen_restores=unseen,
        )

    def expect_back_elsewhere(self, sandbox_id: str, lifecycle: str, *, timeout: float = 600) -> None:
        """The sandbox is `lifecycle` again, no longer on the drained worker."""

        bare = self.bare_id(sandbox_id)
        deadline = time.monotonic() + timeout
        while True:
            residency = self._control.sandbox_residency(bare)
            placement = self._control.sandbox_pod_placement(bare)
            elsewhere = residency.active_node_id not in (None, self.node_id) and (
                placement is None or placement.node_name != self.node_name
            )
            if residency.lifecycle == lifecycle and elsewhere:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the sandbox never came back {lifecycle} off the drained worker",
                sandbox_id=bare,
                lifecycle=residency.lifecycle,
                active_node_id=residency.active_node_id,
                drained_node_id=self.node_id,
                pod_node=None if placement is None else placement.node_name,
            )
            self._fixture.wait(3)

    def expect_no_vm_off_the_drained_worker(self, sandbox_id: str) -> None:
        """The other worker's sandbox-manager holds no VM of the sandbox: it
        has not left the drained worker."""

        bare = self.bare_id(sandbox_id)
        other = self._control.node_sandbox_state(self.other_node_name, bare)
        self._fixture.verify(
            other not in _VM_STATES,
            "a sandbox still on the drained worker holds a VM on the other one",
            sandbox_id=bare,
            other_worker_state=other,
        )

    def settled_lifecycles(
        self, lifecycles: dict[str, str], *, timeout: float
    ) -> dict[str, tuple[str, str | None]]:
        """Wait until no drain restore of the sandboxes is queued or running
        and each row's lifecycle has held for three polls; each one's
        lifecycle and active node, by bare id."""

        deadline = time.monotonic() + timeout
        previous: dict[str, tuple[str, str | None]] | None = None
        steady = 0
        while True:
            current = {self.bare_id(sandbox_id): self.residency(sandbox_id) for sandbox_id in lifecycles}
            busy = any(
                state in ("queued", "running")
                for sandbox_id in lifecycles
                for state, _ in self.drain_restores(sandbox_id)
            )
            settled = not busy and all(
                lifecycle in ("started", "paused", "stopped", "suspended")
                for lifecycle, _ in current.values()
            )
            steady = steady + 1 if settled and current == previous else 0
            if steady >= 3:
                return current
            self._fixture.verify(
                time.monotonic() < deadline,
                "the residents never settled",
                residency=current,
            )
            previous = current
            self._fixture.wait(3)

    def expect_single_vm(self, sandbox_id: str, lifecycle: str) -> None:
        """Exactly one worker's sandbox-manager holds the sandbox's VM, in
        `lifecycle`; for a lifecycle without a VM, neither holds one."""

        bare = self.bare_id(sandbox_id)
        states = {
            node: self._control.node_sandbox_state(node, bare)
            for node in (self.node_name, self.other_node_name)
        }
        holding = [state for state in states.values() if state in _VM_STATES]
        wanted = [lifecycle] if lifecycle in _VM_STATES else []
        self._fixture.verify(
            holding == wanted,
            "the sandbox does not hold exactly one VM in its lifecycle",
            sandbox_id=bare,
            lifecycle=lifecycle,
            states=states,
        )

    def expect_lifecycles(self, expected: dict[str, str]) -> None:
        """Each sandbox is in its lifecycle on the pinned worker now."""

        observed = {
            self.bare_id(sandbox_id): (
                residency.lifecycle,
                residency.active_node_id == self.node_id,
            )
            for sandbox_id in expected
            for residency in (self._control.sandbox_residency(self.bare_id(sandbox_id)),)
        }
        wanted = {
            self.bare_id(sandbox_id): (lifecycle, lifecycle != "stopped")
            for sandbox_id, lifecycle in expected.items()
        }
        self._fixture.verify(
            all(
                observed[sandbox_id][0] == lifecycle
                and (observed[sandbox_id][1] or not placed)
                for sandbox_id, (lifecycle, placed) in wanted.items()
            ),
            "a resident is not in its lifecycle on the pinned worker",
            observed=observed,
            expected=wanted,
        )

    def expect_durable(self, sandbox_id: str, lifecycle: str, *, timeout: float) -> None:
        """The drained node itself reports the sandbox evacuated with
        `lifecycle`: its snapshot or disk is durable. Asked through the node's
        own CLI, so no controller learns of it."""

        bare = self.bare_id(sandbox_id)
        deadline = time.monotonic() + timeout
        while True:
            report = self._control.node_drain_report(self.node_name, self.epoch)
            priors = {entry["sandbox_id"]: entry["prior"] for entry in report["evacuated"]}
            if priors.get(bare) == lifecycle:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the node never reported the sandbox evacuated",
                sandbox_id=bare,
                lifecycle=lifecycle,
                report=report,
            )
            self._fixture.wait(3)

    def wait_for_restores(
        self,
        sandbox_ids: list[str],
        accept: Callable[[list[tuple[str, int]]], bool],
        failure: str,
        *,
        timeout: float,
    ) -> dict[str, list[tuple[str, int]]]:
        """Poll the sandboxes' drain restores until `accept` holds for any one of them."""

        deadline = time.monotonic() + timeout
        while True:
            restores = {self.bare_id(sandbox_id): self.drain_restores(sandbox_id) for sandbox_id in sandbox_ids}
            if any(accept(each) for each in restores.values()):
                self._fixture.record("restores_seen", restores)
                return restores
            self._fixture.verify(time.monotonic() < deadline, failure, restores=restores)
            self._fixture.wait(2)

    def expect_landed_once(self, sandbox_id: str) -> None:
        """Exactly one drain restore of the sandbox succeeded, and none is left running."""

        restores = self.drain_restores(sandbox_id)
        self._fixture.record(f"drain_restores_{self.bare_id(sandbox_id)}", restores)
        self._fixture.verify(
            [state for state, _ in restores].count("succeeded") == 1
            and all(state in ("succeeded", "canceled", "failed") for state, _ in restores),
            "the sandbox was not landed by exactly one drain restore, with none left running",
            sandbox_id=sandbox_id,
            restores=restores,
        )

    def placements(self, sandbox_ids: list[str]) -> dict[str, tuple[str, str | None]]:
        """Each sandbox's lifecycle and active node id, by bare id."""

        return self._control.sandbox_placements(sandbox_ids)

    def drain_restores(self, sandbox_id: str) -> list[tuple[str, int]]:
        """Each drain restore operation of the sandbox, oldest first: its state and attempts."""

        return self._control.drain_restore_operations(self.bare_id(sandbox_id))

    def expect_restored_once(self, sandbox_id: str) -> None:
        """One drain restore brought the sandbox back, on its first attempt."""

        restores = self.drain_restores(sandbox_id)
        self._fixture.record(f"drain_restores_{self.bare_id(sandbox_id)}", restores)
        self._fixture.verify(
            restores == [("succeeded", 1)],
            "the sandbox was not restored by one drain restore, on its first attempt",
            sandbox_id=sandbox_id,
            restores=restores,
        )

    def expect_stopped_unplaced(self, sandbox_id: str) -> None:
        """The stopped sandbox is still stopped, unplaced, with no drain restore."""

        bare = self.bare_id(sandbox_id)
        residency = self._control.stopped_residency(bare)
        restores = self.drain_restores(sandbox_id)
        self._fixture.record(
            "stopped_after_drain",
            {"residency": residency.__dict__, "drain_restores": restores},
        )
        self._fixture.verify(
            residency.lifecycle == "stopped"
            and residency.placement == "unplaced"
            and residency.active_node_id is None
            and residency.evacuation_restore_target is None
            and not restores,
            "the drained stopped sandbox is not stopped and unplaced, untouched by a drain restore",
            sandbox_id=bare,
            residency=residency.__dict__,
            restores=restores,
        )

    @contextmanager
    def fake_ec2_delay(self, seconds: int) -> Iterator[None]:
        """Run the body with each instance Kind's fake EC2 marks terminated
        reading `shutting-down` for `seconds` first; 0 goes back after."""

        reset = self._scope.defer(
            "put the fake EC2's termination delay back",
            lambda: self._control.set_fake_ec2_terminate_delay(0),
            critical=True,
        )
        self._control.set_fake_ec2_terminate_delay(seconds)
        try:
            yield
        finally:
            reset.release()

    def wait_for_dispatch(self, *, timeout: float = 600) -> dict[str, Any]:
        """Wait until the controller records that Auto Scaling accepted the
        termination of the fenced instance (`dispatched_at`), under the
        drain's epoch and on the instance the run gave the worker; the
        stored status."""

        node_id = self.node_id
        deadline = time.monotonic() + timeout
        while True:
            columns = self._control.drain_v2_status(node_id)
            record = None if columns is None else columns.status
            if record is not None and record.get("dispatched_at") is not None:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the controller never recorded the dispatch of the termination",
                drain_status=record,
                last_error=self.last_error(),
            )
            self._fixture.wait(2)
        self._fixture.record("dispatched_status", record)
        self._fixture.verify(
            record.get("epoch") == self.epoch
            and record.get("terminate_instance") == self._instance,
            "the termination was dispatched under another epoch or for another instance",
            record=record,
            instance=self._instance,
        )
        return record

    def _terminate_and_dispatch(self) -> tuple[Compensation, str, str]:
        """`:terminate` the drained worker and wait for the dispatch; the
        armed replacement start, its providerID and its instance."""

        node_name, node_id = self.node_name, self.node_id
        if self._instance is None:
            raise RuntimeError("give_fake_instance() must run first")
        labels = self._control.kind_node_labels(node_name)
        provider_id, replacement = fake_provider_id()
        self._fixture.record("replacement_instance", replacement)
        restart = self._scope.defer(
            f"start Kind worker {node_name} again as a same-name replacement",
            lambda: self._control.start_kind_worker(
                node_name, labels, timeout=300, provider_id=provider_id
            ),
            critical=True,
        )
        status = self._control.terminate_node(node_id)
        self._fixture.verify(status in (200, 202), ":terminate of a drained v2 node was refused", status=status)
        self.wait_for_dispatch()
        return restart, provider_id, replacement

    def _expect_row_deleted_without_tombstone(self, *, timeout: float) -> None:
        node_id = self.node_id
        deadline = time.monotonic() + timeout
        while self._control.node_row_exists(node_id):
            self._fixture.verify(
                time.monotonic() < deadline,
                "the terminated node's row was never deleted",
                drain_status=self._control.drain_v2_status(node_id),
                last_error=self.last_error(),
            )
            self._fixture.wait(3)
        self._fixture.verify(
            not self._control.node_tombstoned(node_id),
            "the terminated v2 node was tombstoned",
        )

    def expect_node_row_deleted_without_tombstone(self, *, timeout: float) -> None:
        """Wait until the node's row is gone, deleted by whoever owns it
        then, and verify nothing tombstoned it."""

        self._expect_row_deleted_without_tombstone(timeout=timeout)

    def _come_back(self, restart: Compensation, replacement: str, *, timeout: float) -> int | None:
        """Start the worker again as a same-name replacement, wait until its
        row records the new instance, and unseal it; the epoch it is still
        sealed under afterwards."""

        restart.release()
        self._instance = replacement
        await_recorded_instance(
            self._fixture, self._control, self.node_name, self.node_id, replacement, timeout=timeout
        )
        return self._control.unseal_activations(self.node_name, self.epoch)

    def terminate(self, *, timeout: float = 600) -> None:
        """`:terminate` reaches the worker's instance through the `aws`
        terminator and Kind's fake EC2. Once the controller records the
        dispatch, the harness plays the cloud's part, stopping the worker and
        deleting its Node; once EC2 says the instance is terminated and a GET
        finds the Node gone, the drain deletes the row, with no tombstone.
        The worker comes back as a same-name replacement, unsealed.

        Runs inside `v2_controller` after `give_fake_instance`.
        """

        restart, _, replacement = self._terminate_and_dispatch()
        self._control.stop_kind_worker(self.node_name)
        self._expect_row_deleted_without_tombstone(timeout=timeout)
        self._fixture.record(
            "after_terminate",
            {
                "node_row_deleted_without_tombstone": True,
                "replacement_instance": replacement,
                "sealed_epoch_after_unseal": self._come_back(restart, replacement, timeout=timeout),
            },
        )

    def terminate_replaced_while_shutting_down(self, *, timeout: float = 600) -> None:
        """`:terminate`, then, while the fenced instance is still
        `shutting-down` (run inside `fake_ec2_delay`), the worker comes back
        under the same name with a new instance. Its registration takes the
        row and clears the drain, fence included: no tombstone, nothing
        deleted, and the replacement keeps the row once the old instance is
        terminated.

        Runs inside `v2_controller` after `give_fake_instance`.
        """

        node_id = self.node_id
        restart, _, replacement = self._terminate_and_dispatch()
        self._control.stop_kind_worker(self.node_name)
        sealed = self._come_back(restart, replacement, timeout=timeout)
        deadline = time.monotonic() + timeout
        while (columns := self._control.drain_v2_status(node_id)) is not None and (
            columns.engine is not None
        ):
            self._fixture.verify(
                time.monotonic() < deadline,
                "the replacement's registration never took the fenced row",
                drain_status=columns.status,
            )
            self._fixture.wait(2)
        self._fixture.verify(
            self._control.node_row_exists(node_id)
            and self._control.node_recorded_instance(node_id) == replacement
            and not self._control.node_tombstoned(node_id),
            "the replacement doesn't hold the row under its own instance",
            recorded=self._control.node_recorded_instance(node_id),
            tombstoned=self._control.node_tombstoned(node_id),
        )
        # Past the fake's delay the old instance reads terminated; the row,
        # the replacement's now, stays.
        self._fixture.wait(DELETE_SETTLE_SECONDS)
        self._fixture.verify(
            self._control.node_row_exists(node_id) and self.drain_phase() is None,
            "the replacement's row was deleted or drained after the old instance terminated",
            drain_status=self._control.drain_v2_status(node_id),
        )
        self._fixture.record(
            "after_replacement",
            {"replacement_instance": replacement, "sealed_epoch_after_unseal": sealed},
        )

    def terminate_keeping_the_node(self, *, timeout: float = 600) -> None:
        """`:terminate`; the worker stops but its Node stays, as behind a
        cloud controller that lags. Once EC2 says the fenced instance is
        terminated the row still waits, saying why; deleting the Node with a
        uid precondition, as the runbook says, lets the drain delete the row
        with no tombstone. The worker comes back as a same-name replacement.

        Runs inside `v2_controller` after `give_fake_instance`.
        """

        node_name, node_id = self.node_name, self.node_id
        restart, _, replacement = self._terminate_and_dispatch()
        uid = self._control.kube_node_uid(node_name)
        self._fixture.verify(uid is not None, "the worker's Node is gone before the stop")
        assert uid is not None
        self._control.stop_kind_worker(node_name, keep_node=True)
        self.wait_for_last_error("waiting for its Kubernetes Node", timeout=timeout)
        self._fixture.verify(
            self._control.node_row_exists(node_id),
            "the row was deleted while its Node lingered",
        )
        self._control.delete_kube_node(node_name, uid=uid)
        self._expect_row_deleted_without_tombstone(timeout=timeout)
        self._fixture.record(
            "after_lingering_node",
            {
                "replacement_instance": replacement,
                "sealed_epoch_after_unseal": self._come_back(restart, replacement, timeout=timeout),
            },
        )

    def die(self) -> Compensation:
        """The drained worker dies mid-drain: Kind's fake EC2 marks its
        instance terminated (after the delay `fake_ec2_delay` set, if any)
        and the harness stops its container and deletes its Node. Returns
        the armed start of the worker as a same-name replacement.

        Runs inside `v2_controller` after `give_fake_instance`.
        """

        node_name = self.node_name
        if self._instance is None:
            raise RuntimeError("give_fake_instance() must run first")
        labels = self._control.kind_node_labels(node_name)
        provider_id, replacement = fake_provider_id()
        self._replacement = replacement
        restart = self._scope.defer(
            f"start Kind worker {node_name} again as a same-name replacement",
            lambda: self._control.start_kind_worker(
                node_name, labels, timeout=300, provider_id=provider_id
            ),
            critical=True,
        )
        self._control.fake_ec2_terminate(self._instance)
        self._control.stop_kind_worker(node_name)
        self._fixture.record("died_at", time.time())
        return restart

    def expect_let_go(self, *, timeout: float = 900) -> str:
        """Wait until the drain lets the dead node go: its phase and v2
        columns cleared, the row kept, and the trace naming its instance in
        `drain_last_error`; the trace."""

        node_id = self.node_id
        trace = f"v2 drain let go: instance {self._instance} terminated outside the drain"
        deadline = time.monotonic() + timeout
        while True:
            columns = self._control.drain_v2_status(node_id)
            if columns is not None and columns.engine is None and self.last_error() == trace:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the drain never let the dead node go",
                drain_status=None if columns is None else columns.status,
                last_error=self.last_error(),
            )
            self._fixture.wait(3)
        self._fixture.verify(
            self.drain_phase() is None and not self._control.node_tombstoned(node_id),
            "the let-go left a phase or a tombstone",
        )
        self._fixture.record("let_go", {"trace": trace, "at": time.time()})
        return trace

    def come_back_after_death(self, restart: Compensation, *, timeout: float = 600) -> None:
        """Start the dead worker again as a same-name replacement and unseal it."""

        if self._replacement is None:
            raise RuntimeError("die() must run first")
        self._fixture.record(
            "sealed_epoch_after_unseal",
            self._come_back(restart, self._replacement, timeout=timeout),
        )

    def undrain(self) -> None:
        """`:undrain` the node, without waiting for the cancel."""

        if self._undrain is not None:
            compensation, self._undrain = self._undrain, None
            compensation.release()

    def undrain_and_expect_cancelled(self, *, timeout: float = 600) -> None:
        """`:undrain` returns the node to no phase, and the controller cancels
        the epoch on it, once the node has handed back what the drain
        evacuated."""

        self.undrain()
        self.expect_cancelled(timeout=timeout)

    def expect_cancelled(self, *, timeout: float = 600) -> None:
        """The controller clears the undrained node's v2 columns."""

        deadline = time.monotonic() + timeout
        while True:
            status = self._control.drain_v2_status(self.node_id)
            if status is not None and status.engine is None and status.status is None:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the undrained node's epoch was never cancelled",
                status=None if status is None else (status.engine, status.status),
            )
            self._fixture.wait(2)
        # The controller clears the columns only once its cancel answered the
        # node unsealed with nothing left to hand back; the scenario then
        # proves it by activating again.

    def expect_stale_epoch_refused(self) -> None:
        """A drain under the cancelled epoch is refused before it seals anything:
        epochs only move forward."""

        try:
            self._control.seal_activations(self.node_name, self.epoch)
        except Exception as error:  # noqa: BLE001 - the refusal is the expected outcome
            self._fixture.record("stale_epoch_refusal", repr(error)[:300])
        else:
            self._control.unseal_activations(self.node_name, self.epoch)
            self._fixture.verify(False, "a drain under the cancelled epoch was accepted")
