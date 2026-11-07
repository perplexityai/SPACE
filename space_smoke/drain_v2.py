"""The node drain's v2 engine, proven on a real cluster.

A v2 drain seals the worker and lets its sandbox-manager make every resident
durable elsewhere (`DrainNode`); the cluster controller records what the node
reports and restores each sandbox to its prior lifecycle through a drain
restore operation. The scenarios here pin residents to one worker by cordoning
the other, uncordon it, and drain the first with `engine: v2`. Every run arms,
before the drain, the undrain, the uncordon and an unseal of the worker.

A terminate runs as production's does: the run gives the worker a fake AWS
providerID first, so the controller's `aws` terminator, which the run selects
together with the dead-node verb in `live` (a scoped controller config, as
`space_smoke.dead_node` scopes its own), asks Kind's fake EC2 to terminate the
instance behind it. The fake only marks it; once the controller records the
termination, the harness plays the cloud's part, stopping the worker's
container and deleting its Node. The verb then reconciles the node by the
drain's record and deletes its row, with no tombstone. The worker comes back
under the same name with a new providerID, a same-name replacement, started
from the stopped container with its sealed sandbox-manager database, which
the run unseals; that start is armed before the terminate.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from space_smoke.capabilities import DrainV2Control, DurableLifecycleSandbox
from space_smoke.dead_node import MODE_ENV, TERMINATOR_KIND_ENV, TICK_ENV, fake_provider_id
from space_smoke.runtime import Compensation, ResourceScope

# The dead-node verb's tick while a run terminates a worker, shortened by its
# test hook from five minutes.
VERB_TICK_SECONDS = 5
_FAKE_PROVIDER_PREFIX = "aws:///kind-a/"

if TYPE_CHECKING:
    from space_smoke.fixtures import CommandSandboxFixture


class DrainV2Assertions:
    """Drain the fixture sandbox's worker with the v2 engine and judge what follows."""

    def __init__(
        self,
        fixture: CommandSandboxFixture,
        control: DrainV2Control,
        create_sandbox: Callable[[], DurableLifecycleSandbox],
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
        else:
            provider, instance = fake_provider_id()
            labels = self._control.kind_node_labels(node_name)
            self._control.set_kind_worker_provider_id(node_name, provider, labels, timeout=timeout)
        self._await_recorded_instance(instance, timeout=timeout)
        self._instance = instance
        self._fixture.record("drained_instance", instance)
        return instance

    def _await_recorded_instance(self, instance: str, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while self._control.node_recorded_instance(self.node_id) != instance:
            self._fixture.verify(
                time.monotonic() < deadline,
                "node registration never recorded the worker's fake instance",
                worker=self.node_name,
                expected=instance,
                recorded=self._control.node_recorded_instance(self.node_id),
            )
            self._fixture.wait(3)

    @contextmanager
    def terminating_like_production(self) -> Iterator[None]:
        """Run the body with the controller's terminator `aws`, against Kind's
        fake EC2, and the dead-node verb `live` on a short tick; put the
        standing config back when it ends."""

        release = self._control.acquire_controller_mutation()
        lease = self._scope.defer("release the controller mutation lease", release)
        standing = self._control.dead_node_env()
        self._fixture.record("standing_controller_env", standing)
        restore = self._scope.defer(
            "put the standing terminator and dead-node env back",
            lambda: self._control.restore_dead_node_env(standing),
            critical=True,
        )
        self._control.set_dead_node_env(
            {
                MODE_ENV: "live",
                TICK_ENV: str(VERB_TICK_SECONDS),
                TERMINATOR_KIND_ENV: "aws",
            }
        )
        try:
            yield
        finally:
            with self._fixture.phase("put the standing terminator and dead-node env back"):
                restore.release()
                lease.release()

    def create_resident(self) -> DurableLifecycleSandbox:
        """A run-owned sandbox, running on the pinned worker."""

        sandbox = self._create_sandbox()
        placement = self._control.sandbox_pod_placement(self.bare_id(sandbox.sandbox_id))
        self._fixture.verify(
            placement is not None and placement.node_name == self.node_name,
            "a resident did not land on the pinned worker",
            sandbox_id=sandbox.sandbox_id,
            node=None if placement is None else placement.node_name,
        )
        return sandbox

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

    def report(self) -> dict[str, Any]:
        status = self._control.drain_v2_status(self.node_id)
        report = None if status is None or status.status is None else status.status.get("report")
        return report if isinstance(report, dict) else {}

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

    def expect_drained(self, *, timeout: float = 900) -> dict[str, Any]:
        """The node reports `done` under the drain's epoch and the row reaches `drained`."""

        epoch = self.epoch
        deadline = time.monotonic() + timeout
        while True:
            node = next((n for n in self._control.admin_nodes() if n.id == self.node_id), None)
            report = self.report()
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
        """The report names each resident with the lifecycle it had."""

        priors = {
            entry.get("sandbox_id"): entry.get("prior")
            for entry in report.get("evacuated", [])
            if isinstance(entry, dict)
        }
        wanted = {self.bare_id(sandbox_id): prior for sandbox_id, prior in expected.items()}
        self._fixture.verify(
            all(priors.get(sandbox_id) == prior for sandbox_id, prior in wanted.items()),
            "the node did not report each resident evacuated with its prior lifecycle",
            reported=priors,
            expected=wanted,
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

    def terminate(self, *, timeout: float = 600) -> None:
        """`:terminate` reaches the worker's instance through the `aws`
        terminator and Kind's fake EC2; the harness then stops the worker, the
        dead-node verb deletes its row with no tombstone, and the worker comes
        back as a same-name replacement, unsealed.

        Runs inside `terminating_like_production` after `give_fake_instance`.
        """

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

        # The controller terminated the instance through the fake and recorded it.
        deadline = time.monotonic() + timeout
        while True:
            columns = self._control.drain_v2_status(node_id)
            record = None if columns is None or columns.status is None else columns.status
            terminated = None if record is None else record.get("terminated")
            if isinstance(terminated, dict):
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the controller never recorded the terminate",
                drain_status=record,
            )
            self._fixture.wait(2)
        self._fixture.record("terminated_record", terminated)
        self._fixture.verify(
            terminated.get("instance_id") == self._instance
            and terminated.get("epoch") == self.epoch
            and isinstance(record, dict)
            and record.get("terminate") == {"epoch": self.epoch},
            "the termination was not recorded for the worker's instance under the drain's epoch",
            record=record,
            instance=self._instance,
        )

        # The cloud's part: the instance stops, and its Node goes.
        self._control.stop_kind_worker(node_name)

        # The dead-node verb, by the drain's record: the row goes, with no tombstone.
        while self._control.drain_v2_status(node_id) is not None:
            self._fixture.verify(
                time.monotonic() < deadline,
                "the dead-node verb never deleted the terminated node's row",
                status=next(
                    (
                        (entry.verdict, entry.reason, dict(entry.counts))
                        for entry in self._control.dead_node_statuses()
                        if entry.node_id == node_id
                    ),
                    None,
                ),
            )
            self._fixture.wait(3)
        self._fixture.verify(
            not self._control.node_tombstoned(node_id),
            "the terminated v2 node was tombstoned",
        )

        # A same-name replacement: the worker under a new instance, its
        # sandbox-manager still sealed under the drain's epoch.
        restart.release()
        self._instance = replacement
        self._await_recorded_instance(replacement, timeout=timeout)
        self._fixture.record(
            "after_terminate",
            {
                "node_row_deleted_without_tombstone": True,
                "replacement_instance": replacement,
                "sealed_epoch_after_unseal": self._control.unseal_activations(
                    node_name, self.epoch
                ),
            },
        )

    def undrain_and_expect_cancelled(self, *, timeout: float = 120) -> None:
        """`:undrain` returns the node to no phase, and the controller cancels the epoch on it."""

        if self._undrain is not None:
            compensation, self._undrain = self._undrain, None
            compensation.release()
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
        # node unsealed; the scenario then proves it by activating again.

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
