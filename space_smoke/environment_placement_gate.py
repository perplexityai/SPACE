"""Turn the environment placement gate on for one Kind scenario, then put it back.

`SPACE_ENVIRONMENT_PLACEMENT_GATE` is read where a substrate is built: the API
(`space-api`, and `space-admin-api` when the bench runs one) and the cluster
controller. With it `on`, a sandbox pod bound to an environment requires
node-ctl's `space.pplx.net/env-<alg>-<digest>=usable` label on its node, so a
bound create with no such node is unschedulable and refused, and with it in
shadow or off the same create places as it always did
(docs/execution-environments.md). A Kind chart sets neither the gate nor
resume-time runtime upgrades, so the bench runs it off.

Under the public API mutation Lease, held for the scenario's whole budget,
this reads both keys on each of those Deployments, refuses while resume-time
upgrades are on (the substrate refuses `on` beside them at startup, so a
rollout would never become ready) or while a gate literal an earlier run left
behind is set to anything but `off`, arms the restore, records the standing
literals on the Lease, writes `on` and awaits the rollouts. A run killed with
the gate on leaves that record, and the Lease's next holder (any Kind
scenario's preflight repair) puts the literals back: at once on the same host
and user, whose dead holder it can prove, and once the four-hour Lease
expires anywhere else. The restore writes the
API's literal before the controller's, though the rollouts run together, and
checks the recorded literals took before it clears the record.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Protocol

from space_smoke.capabilities import SandboxPodRender
from space_smoke.execution_environment import (
    ENVIRONMENT_LABEL_PREFIX,
    ENVIRONMENT_LABEL_VALUE,
    Judge,
    environment_label_key,
)
from space_smoke.runtime import Compensation, ResourceScope, SmokeConfigurationError

GATE_ENV = "SPACE_ENVIRONMENT_PLACEMENT_GATE"
RUNTIME_UPGRADE_ENV = "SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED"
GATE_ON = "on"
# What the gate reads as off when it's set (an unset gate is off too).
GATE_OFF = "off"
# The pod annotations the gate renders (crates/l40_api/sandbox/pod/src/environment_labels.rs).
ENVIRONMENT_ANNOTATION = "space.pplx.net/environment"
ENVIRONMENT_REQUIRED_ANNOTATION = "space.pplx.net/environment-required"
# The Deployments whose substrate reads the gate, as the env keys name them.
GATE_DEPLOYMENTS = ("api", "admin-api", "controller")
ROLLOUT_TIMEOUT_SECONDS = 300.0
# Four hours: longer than the scenario's 230-minute budget, its cleanup
# included, so the Lease can't expire while the gate is on.
LEASE_DURATION_SECONDS = 4 * 3600


def upgrade_flag_on(value: str | None) -> bool:
    """Whether a `RUNTIME_UPGRADE_ENV` literal turns resume-time upgrades on (`env_flag`'s vocabulary)."""

    return (value or "").strip().lower() in {"1", "true", "yes", "on", "enabled"}


def gate_left_behind(value: str | None) -> bool:
    """Whether a standing gate literal is one a Kind chart doesn't render and isn't off."""

    return value is not None and value.strip().lower() != GATE_OFF


def required_environment_labels(render: SandboxPodRender) -> frozenset[str]:
    """The environment label keys every required node-affinity term requires as usable.

    Terms are ORed, so a key binds placement only if each term carries it.
    """

    per_term = [
        {
            str(expression.get("key"))
            for expression in term
            if str(expression.get("key", "")).startswith(ENVIRONMENT_LABEL_PREFIX)
            and expression.get("operator") == "In"
            and list(expression.get("values") or ()) == [ENVIRONMENT_LABEL_VALUE]
        }
        for term in render.required_node_terms
    ]
    if not per_term:
        return frozenset()
    return frozenset(set.intersection(*per_term))


class EnvironmentPlacementGateControl(Protocol):
    """Kind-only operations behind [`EnvironmentPlacementGate`]."""

    def acquire_public_api_mutation(self, *, duration_seconds: int) -> Callable[[], None]:
        """Take the public API mutation Lease for `duration_seconds`; returns its release."""
        ...

    def environment_placement_env(self) -> dict[str, str | None]:
        """`<deployment>:<key>` literals for `GATE_ENV` and `RUNTIME_UPGRADE_ENV`, None where unset.

        `<deployment>` is one of `GATE_DEPLOYMENTS` the bench runs.
        """
        ...

    def set_environment_placement_gate(
        self,
        values: Mapping[str, str | None],
        *,
        api_first: bool,
        record: Mapping[str, str | None] | None = None,
    ) -> None:
        """Write `<deployment>:GATE_ENV` literals (None unsets) and await every rollout.

        Renews the held Lease first, and records `record` on it in the same
        patch when given.
        """
        ...

    def clear_environment_placement_gate_record(self) -> None:
        """Drop the standing literals recorded on the held Lease."""
        ...

    def wait_public_api_ready(self, *, timeout: float) -> None: ...

    def sandbox_pod_render(self, sandbox_id: str) -> SandboxPodRender | None: ...


class GateNotRestored(RuntimeError):
    """A Deployment doesn't carry its recorded gate literal after the restore."""


class EnvironmentPlacementGate:
    """The gate turned on for one scenario, and its recorded literals put back."""

    def __init__(self, fixture: Judge, control: EnvironmentPlacementGateControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._restore: Compensation | None = None
        self._release_lease: Compensation | None = None

    def turn_on(self) -> None:
        """Turn the gate `on` on every Deployment that reads it; the restore is armed first."""

        if self._restore is not None:
            raise RuntimeError("the environment placement gate is already on")
        release = self._control.acquire_public_api_mutation(duration_seconds=LEASE_DURATION_SECONDS)
        self._release_lease = self._scope.defer(
            "release the public API mutation lease", release, critical=True
        )
        env = self._control.environment_placement_env()
        self._fixture.record("environment_placement_env_before", dict(env))
        upgrades = {key: value for key, value in env.items() if key.endswith(f":{RUNTIME_UPGRADE_ENV}") and upgrade_flag_on(value)}
        if upgrades:
            raise SmokeConfigurationError(
                f"resume-time runtime upgrades are on ({upgrades}); the substrate refuses "
                f"{GATE_ENV}={GATE_ON} beside them, so the gate can't be turned on"
            )
        standing = {key: value for key, value in env.items() if key.endswith(f":{GATE_ENV}")}
        if not standing:
            raise SmokeConfigurationError("the bench runs no Deployment that reads the environment placement gate")
        left = {key: value for key, value in standing.items() if gate_left_behind(value)}
        if left:
            # Restoring a literal no chart sets would keep it for good.
            raise SmokeConfigurationError(
                f"{left} already sets the environment placement gate; no Kind chart sets it, so an "
                f"earlier run left it behind; unset {GATE_ENV} and rerun"
            )

        def restore() -> None:
            self._control.set_environment_placement_gate(standing, api_first=True)
            self._control.wait_public_api_ready(timeout=ROLLOUT_TIMEOUT_SECONDS)
            after = {
                key: value
                for key, value in self._control.environment_placement_env().items()
                if key in standing
            }
            if after != standing:
                raise GateNotRestored(f"the gate reads {after}, not its standing literals {standing}")
            # Only a restore that reads back clears the record and frees the
            # Lease; otherwise the record stays for the next holder's repair.
            self._control.clear_environment_placement_gate_record()
            release()

        # Armed before the write: one that half-applies is still put back.
        self._restore = self._scope.defer(
            "restore the environment placement gate", restore, critical=True
        )
        assert self._release_lease is not None
        self._release_lease.disarm()
        self._release_lease = None
        self._control.set_environment_placement_gate(
            {key: GATE_ON for key in standing}, api_first=False, record=standing
        )
        self._control.wait_public_api_ready(timeout=ROLLOUT_TIMEOUT_SECONDS)
        after = self._control.environment_placement_env()
        self._fixture.record("environment_placement_env_on", dict(after))
        self._fixture.verify(
            all(after.get(key) == GATE_ON for key in standing),
            "every Deployment that reads the gate must run it on",
            env=dict(after),
        )

    def expect_pod_requires(self, sandbox_id: str, reference: str) -> SandboxPodRender:
        """The sandbox's pod records `reference` and requires its usable label."""

        render = self._control.sandbox_pod_render(sandbox_id)
        self._fixture.verify(render is not None, "the bound sandbox must have a pod", sandbox_id=sandbox_id)
        assert render is not None
        label = environment_label_key(reference)
        required = required_environment_labels(render)
        self._fixture.record(
            f"pod_environment_{sandbox_id}",
            {
                "annotation": render.annotations.get(ENVIRONMENT_ANNOTATION),
                "required_annotation": render.annotations.get(ENVIRONMENT_REQUIRED_ANNOTATION),
                "required_labels": sorted(required),
                "node": render.node_name,
            },
        )
        self._fixture.verify(
            render.annotations.get(ENVIRONMENT_ANNOTATION) == reference
            and render.annotations.get(ENVIRONMENT_REQUIRED_ANNOTATION) == "true"
            and required == {label},
            "with the gate on, a bound pod must record its environment and require exactly its usable label",
            expected_label=label,
            required_labels=sorted(required),
            # Only the gate's annotations: others carry the pod's env.
            annotations={
                key: render.annotations.get(key)
                for key in (ENVIRONMENT_ANNOTATION, ENVIRONMENT_REQUIRED_ANNOTATION)
            },
        )
        return render

    def restore(self) -> None:
        """Put the recorded literals back and release the Lease, as a scenario phase.

        A restore that doesn't read back fails the run and keeps the Lease.
        """

        if self._restore is not None:
            restore, self._restore = self._restore, None
            try:
                restore.release()
            except GateNotRestored as error:
                self._fixture.verify(False, str(error))
        if self._release_lease is not None:
            release, self._release_lease = self._release_lease, None
            release.release()
