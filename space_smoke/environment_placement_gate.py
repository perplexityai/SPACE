"""Turn the environment placement gate on for one Kind scenario, then put it back.

`SPACE_ENVIRONMENT_PLACEMENT_GATE` is read where a substrate is built: the API
(`space-api`, and `space-admin-api` when the bench runs one) and the cluster
controller. With it `on`, a sandbox pod bound to an environment requires
node-ctl's `space.pplx.net/env-<alg>-<digest>=usable` label on its node, so a
bound create with no such node is unschedulable and refused, and with it in
shadow or off the same create places as it always did
(docs/execution-environments.md). A Kind chart doesn't set the gate, so the
bench runs it off.

Under the public API mutation Lease, held for the scenario's whole budget,
this reads the gate on each of those Deployments, refuses while a gate literal
an earlier run left behind is set to anything but `off`, arms the restore, records the standing
literals on the Lease, writes `on` and awaits the rollouts. A run killed with
the gate on leaves that record, and the Lease's next holder (any Kind
scenario's preflight repair) puts the literals back: at once on the same host
and user, whose dead holder it can prove, and once the four-hour Lease
expires anywhere else. The restore writes the
API's literal before the controller's, though the rollouts run together, and
checks the recorded literals took before it clears the record.
"""

from __future__ import annotations

import time
import urllib.parse
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from space_smoke.capabilities import ConsoleApiResponse, SandboxPodRender
from space_smoke.execution_environment import (
    ENVIRONMENT_LABEL_PREFIX,
    ENVIRONMENT_LABEL_VALUE,
    Judge,
    environment_label_key,
)
from space_smoke.runtime import Compensation, InconclusiveSmoke, ResourceScope, SmokeConfigurationError

GATE_ENV = "SPACE_ENVIRONMENT_PLACEMENT_GATE"
GATE_ON = "on"
# What the gate reads as off when it's set (an unset gate is off too).
GATE_OFF = "off"
RUNTIME_UPGRADE_AT_ACTIVATION_ENV = "SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED"
RUNTIME_UPGRADE_AT_ACTIVATION_KEY = f"api:{RUNTIME_UPGRADE_AT_ACTIVATION_ENV}"
RUNTIME_UPGRADE_AT_ACTIVATION_ON = "true"
# The pod annotations the gate and activation-time runtime upgrade render
# (`crates/l40_api/sandbox/pod/src/runtime_upgrade.rs`).
CHECKPOINT_ANNOTATION = "space.pplx.net/checkpoint"
RUNTIME_UPGRADE_CHECKPOINT = "runtime-upgrade"
ENVIRONMENT_ANNOTATION = "space.pplx.net/environment"
ENVIRONMENT_REQUIRED_ANNOTATION = "space.pplx.net/environment-required"
RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION = "space.pplx.net/runtime-upgrade-operation-id"
RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION = "space.pplx.net/runtime-upgrade-target-environment"
RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION = "space.pplx.net/runtime-upgrade-target-state"
POLICY_PATH = "/admin/api/v1/runtime-environments"
# Above `POLICY_CACHE_TTL` (5 s) in `crates/l40_api/api/src/controllers/substrate/kubernetes/store/runtime_upgrade.rs`.
POLICY_CACHE_SETTLE_SECONDS = 6.0
# The Deployments whose substrate reads the gate, as the env keys name them.
GATE_DEPLOYMENTS = ("api", "admin-api", "controller")
ROLLOUT_TIMEOUT_SECONDS = 300.0
# Four hours: longer than the scenario's 230-minute budget, its cleanup
# included, so the Lease can't expire while the gate is on.
LEASE_DURATION_SECONDS = 4 * 3600


def gate_left_behind(value: str | None) -> bool:
    """Whether a standing gate literal is one a Kind chart doesn't render and isn't off."""

    return value is not None and value.strip().lower() != GATE_OFF


def activation_upgrade_left_behind(value: str | None) -> bool:
    """Whether `SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED` is already enabled on the API."""

    return value is not None and value.strip().lower() in {"1", "true", "yes", "on", "enabled"}


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

    def environment_placement_env(self, *, include_activation_upgrade: bool = False) -> dict[str, str | None]:
        """`<deployment>:GATE_ENV` literals (plus `RUNTIME_UPGRADE_AT_ACTIVATION_KEY` when requested), None where unset.

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

    def restore_portable(self, sandbox_id: str) -> bool: ...

    def record_activation_upgrade_policy_source(self, source_ref: str) -> None: ...

    def clear_activation_upgrade_policy_source(self) -> None: ...

    def console_api(
        self,
        method: str,
        path: str,
        body: Mapping[str, object] | None = None,
        *,
        idempotency_key: str | None = None,
        if_match: int | None = None,
    ) -> ConsoleApiResponse: ...


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

    def turn_on(self, *, runtime_upgrade_at_activation: bool = False) -> None:
        """Turn the gate `on` on every Deployment that reads it (and optionally `SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED=true` on the API); the restore is armed first."""

        if self._restore is not None:
            raise RuntimeError("the environment placement gate is already on")
        release = self._control.acquire_public_api_mutation(duration_seconds=LEASE_DURATION_SECONDS)
        self._release_lease = self._scope.defer(
            "release the public API mutation lease", release, critical=True
        )
        env = self._control.environment_placement_env(
            include_activation_upgrade=runtime_upgrade_at_activation
        )
        self._fixture.record("environment_placement_env_before", dict(env))
        standing = {key: value for key, value in env.items() if key.endswith(f":{GATE_ENV}")}
        if not standing:
            raise SmokeConfigurationError("the bench runs no Deployment that reads the environment placement gate")
        left = {key: value for key, value in standing.items() if gate_left_behind(value)}
        if runtime_upgrade_at_activation:
            activation_standing = env.get(RUNTIME_UPGRADE_AT_ACTIVATION_KEY)
            if activation_upgrade_left_behind(activation_standing):
                left[RUNTIME_UPGRADE_AT_ACTIVATION_KEY] = activation_standing
            standing[RUNTIME_UPGRADE_AT_ACTIVATION_KEY] = activation_standing
        if left:
            # Restoring a literal no chart sets would keep it for good.
            raise SmokeConfigurationError(
                f"{left} already sets the environment placement gate or activation runtime upgrade; "
                f"no Kind chart sets them, so an earlier run left them behind; unset {GATE_ENV} / "
                f"{RUNTIME_UPGRADE_AT_ACTIVATION_ENV} and rerun"
            )

        def restore() -> None:
            self._control.set_environment_placement_gate(standing, api_first=True)
            self._control.wait_public_api_ready(timeout=ROLLOUT_TIMEOUT_SECONDS)
            after = {
                key: value
                for key, value in self._control.environment_placement_env(
                    include_activation_upgrade=runtime_upgrade_at_activation
                ).items()
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
        desired = {
            key: (
                RUNTIME_UPGRADE_AT_ACTIVATION_ON
                if key == RUNTIME_UPGRADE_AT_ACTIVATION_KEY
                else GATE_ON
            )
            for key in standing
        }
        self._control.set_environment_placement_gate(desired, api_first=False, record=standing)
        self._control.wait_public_api_ready(timeout=ROLLOUT_TIMEOUT_SECONDS)
        after = self._control.environment_placement_env(
            include_activation_upgrade=runtime_upgrade_at_activation
        )
        self._fixture.record("environment_placement_env_on", dict(after))
        self._fixture.verify(
            all(after.get(key) == expected for key, expected in desired.items()),
            "every Deployment that reads the gate must run the requested settings",
            env=dict(after),
        )

    def wait_until_restore_portable(self, sandbox_id: str, *, timeout: float = 300.0) -> None:
        """Block until `sandbox_id` has a ready remote snapshot in object storage."""

        deadline = time.monotonic() + timeout
        while not self._control.restore_portable(sandbox_id):
            self._fixture.verify(
                time.monotonic() <= deadline,
                "suspend snapshot did not become portable from object storage",
                sandbox_id=sandbox_id,
                portability_timeout_seconds=timeout,
            )
            time.sleep(2.0)

    def _policy(self) -> dict[str, Any]:
        response = self._control.console_api("GET", POLICY_PATH)
        if response.status != 200 or not isinstance(response.body, dict):
            raise SmokeConfigurationError(
                f"reading the runtime environment policy answered {response.status}"
            )
        return response.body

    def _put_default(self, environment_ref: str, *, reason: str) -> None:
        policy = self._policy()
        response = self._control.console_api(
            "PUT",
            f"{POLICY_PATH}/default",
            {"environment_ref": environment_ref, "reason": reason, "strand": True},
            if_match=int(policy["generation"]),
        )
        self._fixture.record(
            f"activation_upgrade_set_default_{environment_ref}",
            {"status": response.status, "body": response.body},
        )
        if response.status != 200:
            raise SmokeConfigurationError(
                f"setting default runtime environment to {environment_ref} answered {response.status}: {response.body}"
            )

    def _put_state(self, environment_ref: str, *, state: str, reason: str) -> None:
        policy = self._policy()
        encoded = urllib.parse.quote(environment_ref, safe="")
        body: dict[str, object] = {"state": state, "reason": reason}
        if state == "blocked":
            body["strand"] = True
        response = self._control.console_api(
            "PUT",
            f"{POLICY_PATH}/{encoded}/state",
            body,
            if_match=int(policy["generation"]),
        )
        self._fixture.record(
            f"activation_upgrade_set_state_{environment_ref}_{state}",
            {"status": response.status, "body": response.body},
        )
        if response.status != 200:
            raise SmokeConfigurationError(
                f"setting runtime environment {environment_ref} to {state} answered {response.status}: {response.body}"
            )

    def upgrade_default_and_block_source(
        self, *, source_ref: str, target_ref: str
    ) -> Compensation:
        """Move the cluster default to `target_ref` and block `source_ref`, arming `source_ref`'s restore first."""

        standing_policy = self._policy()
        standing_default = standing_policy.get("default_environment_ref")
        if standing_default is not None and str(standing_default) != source_ref:
            raise InconclusiveSmoke(
                f"the bench's default environment is {standing_default}, not {source_ref}"
            )

        def restore_policy() -> None:
            self._put_state(
                source_ref,
                state="allowed",
                reason="space_smoke: restore E0 policy after activation upgrade check",
            )
            self._put_default(
                source_ref,
                reason="space_smoke: restore default runtime environment after activation upgrade check",
            )
            self._control.clear_activation_upgrade_policy_source()
            time.sleep(POLICY_CACHE_SETTLE_SECONDS)

        compensation = self._scope.defer(
            "restore runtime environment policy after activation upgrade check",
            restore_policy,
            critical=True,
        )
        self._control.record_activation_upgrade_policy_source(source_ref)
        self._put_default(
            target_ref,
            reason="space_smoke: move default to target environment for activation upgrade",
        )
        self._put_state(
            source_ref,
            state="blocked",
            reason="space_smoke: block source environment to trigger activation upgrade",
        )
        time.sleep(POLICY_CACHE_SETTLE_SECONDS)
        return compensation

    def expect_pod_runtime_upgrade_target(self, sandbox_id: str, reference: str) -> None:
        """The sandbox's pod carries the runtime-upgrade checkpoint and targets `reference`."""

        render = self._control.sandbox_pod_render(sandbox_id)
        self._fixture.verify(
            render is not None,
            "the upgraded sandbox must have a pod",
            sandbox_id=sandbox_id,
        )
        assert render is not None
        checkpoint = render.annotations.get(CHECKPOINT_ANNOTATION)
        operation_id = render.annotations.get(RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION)
        target_environment = render.annotations.get(RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION)
        target_state = render.annotations.get(RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION)
        observed = {
            "checkpoint": checkpoint,
            "operation_id": operation_id,
            "target_environment": target_environment,
            "target_state": target_state,
        }
        self._fixture.record(f"pod_runtime_upgrade_{sandbox_id}", observed)
        self._fixture.verify(
            checkpoint == RUNTIME_UPGRADE_CHECKPOINT
            and bool(operation_id)
            and target_environment == reference
            and target_state == "started",
            "the activation pod must carry the runtime-upgrade checkpoint and target the new default environment",
            expected_target=reference,
            runtime_upgrade_annotations=observed,
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
