"""A worker's sandbox-manager activation seal, proven on a real cluster.

A sealed sandbox-manager refuses every new VM activation (a cold boot, a
snapshot restore, the resume of a paused VM it holds in memory) and keeps
teardown and durability work running. Its crate tests pin the refusal at the
admission point and the in-flight accounting with a fake node-manager; this
module is the seam on a real worker: the public API's create, wake and resume
fail on a sealed sandbox-manager without it admitting an activation, the
resume with the seal's marker in its answer, while a suspend and a delete
still complete, and the same calls work again once it is unsealed.

The seal has no control-plane route yet, so it is set and read through the
worker's `space-sbxmgr` in the operator shell ([`ActivationSealControl`]).
The other sandbox worker is cordoned under the shared worker-cordon Lease for
the whole scenario, so every create and every portable restore the scenario
makes is scheduled onto the sealed worker.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from space_smoke.capabilities import (
    ActivationSealControl,
    ActivationSealReport,
    DurableLifecycleSandbox,
    InFlightActivationView,
    ResidentLifecycleSandbox,
)
from space_smoke.runtime import (
    Compensation,
    InconclusiveSmoke,
    InvariantViolation,
    QuarantinedSmoke,
    ResourceScope,
    ScenarioTimedOut,
    SmokeConfigurationError,
)

if TYPE_CHECKING:
    from space_smoke.fixtures import CommandSandboxFixture

# sandbox-manager's refusal marker: `ACTIVATIONS_SEALED_MARKER` in
# `crates/l20_domain/rpc/contracts/src/lib.rs`.
ACTIVATIONS_SEALED_MARKER = "node activations are sealed"


def parse_activation_seal_report(output: str) -> ActivationSealReport:
    """Parse a `space-sbxmgr` seal command's `key=value` lines; refuses anything else."""

    values: dict[str, str] = {}
    in_flight: list[InFlightActivationView] = []
    for raw in output.splitlines():
        line = raw.strip()
        if not line:
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise ValueError(f"unrecognized activation-seal line: {line[:200]!r}")
        if key == "in_flight":
            fields = value.split()
            if len(fields) != 3:
                raise ValueError(f"unrecognized in-flight line: {line[:200]!r}")
            in_flight.append(InFlightActivationView(fields[0], fields[1], int(fields[2])))
            continue
        if key in values:
            raise ValueError(f"activation-seal output repeats {key!r}")
        values[key] = value
    missing = {"sealed_epoch", "sealed_since", "activations_admitted"} - values.keys()
    if missing:
        raise ValueError(f"activation-seal output lacks {sorted(missing)}: {output[:400]!r}")
    epoch = None if values["sealed_epoch"] == "none" else int(values["sealed_epoch"])
    quiesced_text = values.get("quiesced")
    if quiesced_text not in {None, "true", "false"}:
        raise ValueError(f"unrecognized quiesced value: {quiesced_text!r}")
    return ActivationSealReport(
        sealed_epoch=epoch,
        sealed_since=None if values["sealed_since"] == "none" else values["sealed_since"],
        activations_admitted=int(values["activations_admitted"]),
        in_flight=tuple(in_flight),
        quiesced=None if quiesced_text is None else quiesced_text == "true",
    )


class ActivationSealAssertions:
    """Seal the fixture sandbox's worker and judge what its sandbox-manager admits."""

    def __init__(
        self,
        fixture: CommandSandboxFixture,
        control: ActivationSealControl,
        create_sandbox: Callable[[], DurableLifecycleSandbox],
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._create_sandbox = create_sandbox
        self._scope = scope
        self._node: str | None = None
        self._epoch: int | None = None
        self._sealed: ActivationSealReport | None = None
        self._unseal: Compensation | None = None

    @staticmethod
    def bare_id(sandbox_id: str) -> str:
        """The bare UUID the pod label and the rows carry; the native route answers `sbx_<uuid>`."""

        return sandbox_id.removeprefix("sbx_")

    @property
    def node(self) -> str:
        if self._node is None:
            raise RuntimeError("pin_to_resident_worker() must run first")
        return self._node

    # --- placement ------------------------------------------------------------

    def pin_to_resident_worker(self) -> str:
        """Record the fixture sandbox's worker and cordon the other sandbox worker.

        Exactly two sandbox workers: the worker-cordon Lease records one
        cordoned node, which is what lets a later run repair a cordon this run
        never undid.
        """

        sandbox_id = self.bare_id(self._fixture.sandbox.sandbox_id)
        placement = self._control.sandbox_pod_placement(sandbox_id)
        self._fixture.verify(
            placement is not None,
            "the fixture sandbox has no single bound execution pod",
            sandbox_id=sandbox_id,
        )
        assert placement is not None
        workers = self._control.sandbox_pool_nodes()
        others = [node for node in workers if node != placement.node_name]
        self._fixture.verify(
            placement.node_name in workers and len(others) == 1,
            "the scenario needs exactly two sandbox workers, one hosting the fixture sandbox",
            workers=workers,
            resident_node=placement.node_name,
        )
        other = others[0]
        release = self._control.acquire_worker_cordon_mutation()
        self._scope.defer("release the worker cordon lease", release)
        self._scope.defer(
            f"uncordon {other}",
            lambda: self._control.uncordon_kube_node(other),
            critical=True,
        )
        self._control.cordon_kube_node(other)
        self._node = placement.node_name
        self._fixture.record("sealed_worker", placement.node_name)
        self._fixture.record("cordoned_worker", other)
        return placement.node_name

    def create_on_worker(self) -> DurableLifecycleSandbox:
        """Create a run-owned sandbox and require it to run on the pinned worker."""

        sandbox = self._create_sandbox()
        self.expect_on_worker(sandbox.sandbox_id)
        return sandbox

    def expect_on_worker(self, sandbox_id: str) -> None:
        placement = self._control.sandbox_pod_placement(self.bare_id(sandbox_id))
        self._fixture.verify(
            placement is not None and placement.node_name == self.node,
            "the sandbox does not run on the sealed worker the other worker's cordon pins it to",
            sandbox_id=sandbox_id,
            node=None if placement is None else placement.node_name,
            sealed_worker=self.node,
        )

    # --- the seal -------------------------------------------------------------

    def seal(self, *, wait_seconds: int = 60) -> ActivationSealReport:
        """Seal the worker under a run-unique epoch; nothing may be in flight once it answers.

        The unseal is armed before the seal is sent, so a run that fails
        anywhere after this point leaves the worker unsealed; the seal is also
        recorded on the worker-cordon Lease first, so the Lease's next holder
        unseals it after a run killed before its cleanup.
        """

        node = self.node
        epoch = time.time_ns() // 1_000_000
        self._epoch = epoch

        def unseal() -> None:
            self._control.unseal_activations(node, epoch)

        self._unseal = self._scope.defer(
            f"unseal activations on {node}", unseal, critical=True
        )
        report = self._control.seal_activations(node, epoch, wait_seconds=wait_seconds)
        self._fixture.verify(
            report.sealed_epoch == epoch
            and report.sealed_since is not None
            and report.quiesced is True
            and not report.in_flight,
            "the worker did not answer sealed under the run's epoch with nothing in flight",
            epoch=epoch,
            sealed_epoch=report.sealed_epoch,
            sealed_since=report.sealed_since,
            quiesced=report.quiesced,
            in_flight=[(a.kind, a.sandbox_id, a.age_ms) for a in report.in_flight],
        )
        self._sealed = report
        self._fixture.record("seal_epoch", epoch)
        self._fixture.record("sealed_since", report.sealed_since)
        return report

    def expect_refused(
        self, action: str, call: Callable[[], object], *, answer_carries_marker: bool = True
    ) -> str:
        """`call` fails, the worker admits no activation for it, and, where the
        public answer can carry it, that answer names the seal's marker.

        A create or a restore on the Kubernetes substrate starts its VM inside
        a pod's sandbox creation, so the refusal goes back to the kubelet,
        which keeps retrying, and the operation ends without it
        (`pod_never_ready`). For those calls the verdict is the seal's own
        read: still sealed under the run's epoch, with the admitted count it
        had when sealed.
        """

        try:
            call()
        except (
            ScenarioTimedOut,
            SmokeConfigurationError,
            InvariantViolation,
            InconclusiveSmoke,
            QuarantinedSmoke,
        ):
            # The harness's own verdicts, such as a create placed off the
            # sealed worker, are not the refusal.
            raise
        except Exception as error:  # noqa: BLE001 - the public answer is the verdict
            text = f"{type(error).__name__}: {error}"
        else:
            self._fixture.verify(False, f"{action} succeeded on a sealed worker", node=self.node)
            raise AssertionError("unreachable")
        self._fixture.record(f"refused_{action.replace(' ', '_')}", text[:500])
        if answer_carries_marker:
            self._fixture.verify(
                ACTIVATIONS_SEALED_MARKER in text,
                f"{action} on a sealed worker failed without the seal's refusal marker",
                marker=ACTIVATIONS_SEALED_MARKER,
                error=text[:2_000],
            )
        self.expect_still_sealed()
        return text

    def expect_lifecycle(self, sandbox_id: str, lifecycle: str, *, timeout: float = 120) -> None:
        """The sandbox's control-plane lifecycle settles on `lifecycle`."""

        bare = self.bare_id(sandbox_id)
        deadline = time.monotonic() + timeout
        while True:
            residency = self._control.sandbox_residency(bare)
            if residency.lifecycle == lifecycle:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the sandbox did not settle {lifecycle}",
                sandbox_id=sandbox_id,
                lifecycle=residency.lifecycle,
                active_node_id=residency.active_node_id,
            )
            self._fixture.wait(1)

    def expect_still_sealed(self) -> None:
        """The same seal is still in place and admitted nothing since it was set."""

        sealed = self._sealed
        if sealed is None:
            raise RuntimeError("seal() must run first")
        now = self._control.activation_seal_state(self.node)
        self._fixture.verify(
            now.sealed_epoch == sealed.sealed_epoch
            and now.sealed_since == sealed.sealed_since
            and now.activations_admitted == sealed.activations_admitted,
            "the seal was not continuous, or the worker admitted an activation under it",
            sealed_epoch=(sealed.sealed_epoch, now.sealed_epoch),
            sealed_since=(sealed.sealed_since, now.sealed_since),
            activations_admitted=(sealed.activations_admitted, now.activations_admitted),
        )

    def unseal(self) -> None:
        """Unseal under the run's epoch; the worker then reads unsealed."""

        compensation, self._unseal = self._unseal, None
        if compensation is not None:
            compensation.release()
        now = self._control.activation_seal_state(self.node)
        self._fixture.verify(
            now.sealed_epoch is None and now.sealed_since is None,
            "the worker still reads sealed after the unseal",
            sealed_epoch=now.sealed_epoch,
            sealed_since=now.sealed_since,
        )

    # --- lifecycle operations ---------------------------------------------------

    @staticmethod
    def resident(sandbox: DurableLifecycleSandbox) -> ResidentLifecycleSandbox:
        """The native sandbox's resident pause/wake surface."""

        return cast(ResidentLifecycleSandbox, sandbox)

    def fixture_resident(self) -> ResidentLifecycleSandbox:
        """The fixture sandbox's own resident pause/wake surface, unwrapped.

        The fixture's `wake_from_resident_pause` turns a failure into a
        verdict; a refusal check needs the raw public answer.
        """

        return cast(ResidentLifecycleSandbox, self._fixture.sandbox)
