"""Explicitly owned Kind faults for native recovery scenarios.

Faults temporarily change captured replica counts; restarts delete captured pods.
An autoscaler on a target Deployment is pinned to that Deployment's current
replica count for the faults' duration, so a replica count the scenario waits
for cannot move under it, and its captured bounds are restored afterwards.
The invalid-create probe may shorten three operation/readiness budgets under a
separate exact-restore guard. Images and database sizing stay unchanged.
The existing cluster-visible controller mutation Lease serializes cooperating
smokes; the operator must dedicate the acknowledged cluster to this suite.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from urllib.parse import urlsplit

from space_smoke.native_hardening import NativeHardeningFixture, NativeTransportError

from space_smoke.adapters import (
    KindConfig,
    KindHarness,
    NATIVE_OPERATION_AGE_ENV as M1_AGE_ENV,
    NATIVE_OPERATION_READY_ENVS as READINESS_BUDGET_ENVS,
)
from space_smoke.runtime import ScenarioContext, SmokeConfigurationError, SubprocessExecutor

DEPLOYMENTS = ("space-api", "space-cluster-controller", "space-data-plane")
STORAGE_DEPLOYMENT = "space-object-store"
RECOVERY_ENVIRONMENT = (
    "SPACE_SMOKE_NATIVE_RECOVERY",
    "SPACE_SMOKE_NATIVE_RECOVERY_CONTEXT",
    "SPACE_SMOKE_NATIVE_RECOVERY_CLUSTER_UID",
)
ROLLOUT_TIMEOUT = 90.0
# The cluster-controller's leader-election kill switch: on unless one of these
# values turns it off (truthy_default in
# crates/libs/cluster/controller/server/src/env_config.rs).
LEADER_ELECTION_ENV = "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_ENABLED"
FALSE_VALUES = frozenset({"0", "false", "False", "FALSE"})


@dataclass(frozen=True)
class DeploymentCapture:
    uid: str
    replicas: int
    selector: str
    spec_digest: str


@dataclass(frozen=True)
class AutoscalerCapture:
    uid: str
    target: str
    min_replicas: int
    max_replicas: int


def _digest(spec: Mapping[str, object]) -> str:
    # Replicas are the only configuration field this control changes.
    value = {key: value for key, value in spec.items() if key != "replicas"}
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class KindNativeHardeningFixture(NativeHardeningFixture):
    """Native API fixture with privileged, Kind-only recovery controls."""

    fixture_name = "kind-native-hardening"
    # A storage-held suspend can need the bounded 600 s upload retry drain.
    timeout_minutes = 25

    @property
    def recovery(self) -> NativeRecoveryControl:
        return NativeRecoveryControl(
            self.context,
            api_url=self.config.api_url,
            data_plane_url=self.config.data_plane_url,
        )


class NativeRecoveryControl:
    """Bounded faults restricted to an explicitly acknowledged local Kind cluster."""

    def __init__(self, context: ScenarioContext, *, api_url: str, data_plane_url: str):
        self.context = context
        self.environment = dict(os.environ)
        target = self.environment.get("SPACE_SMOKE_NATIVE_RECOVERY_CONTEXT", "")
        if (
            self.environment.get("SPACE_SMOKE_BACKEND") != "kind"
            or self.environment.get("SPACE_SMOKE_NATIVE_RECOVERY") != "1"
            or re.fullmatch(r"kind-[a-z0-9][a-z0-9-]*", target) is None
        ):
            raise SmokeConfigurationError(
                "native recovery requires the kind backend, "
                "SPACE_SMOKE_NATIVE_RECOVERY=1, and an explicit Kind context"
            )
        for url in (api_url, data_plane_url):
            parsed = urlsplit(url)
            if parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
                raise SmokeConfigurationError("native recovery requires loopback HTTP endpoints")
        self.harness = KindHarness(
            SubprocessExecutor(),
            KindConfig(cluster=target.removeprefix("kind-"), api_url=api_url, data_plane_url=data_plane_url),
            environment=self.environment,
        )
        self._captures: dict[str, DeploymentCapture] = {}
        self._autoscalers: dict[str, AutoscalerCapture] = {}
        self._release = None

    def _json(self, *args: str) -> dict:
        result = self.harness.kubectl(*args, timeout=15)
        if result.returncode:
            # Kubernetes output may contain env credentials. Keep only the action.
            raise SmokeConfigurationError(f"native recovery Kubernetes action failed: {' '.join(args[:3])}")
        value = json.loads(result.stdout)
        if not isinstance(value, dict):
            raise SmokeConfigurationError("native recovery received a non-object Kubernetes response")
        return value

    def _identity(self) -> str:
        uid = self._json("get", "namespace", "kube-system", "-o", "json")["metadata"]["uid"]
        expected = self.environment.get("SPACE_SMOKE_NATIVE_RECOVERY_CLUSTER_UID")
        if not expected or uid != expected:
            raise SmokeConfigurationError(
                "native recovery requires the current kube-system UID in "
                "SPACE_SMOKE_NATIVE_RECOVERY_CLUSTER_UID"
            )
        nodes = self.harness.executor.run(
            ["kind", "get", "nodes", "--name", self.harness.config.cluster], timeout=15
        )
        observed = {
            item["metadata"]["name"]
            for item in self._json("get", "nodes", "-o", "json").get("items", [])
        }
        if nodes.returncode or not observed or observed != set(nodes.stdout.split()):
            raise SmokeConfigurationError("acknowledged context does not match the local Kind node inventory")
        return uid

    def _fence(self) -> None:
        self._identity()
        if self._release is None:
            raise SmokeConfigurationError("native recovery requires its live mutation Lease")
        self.harness._assert_retention_lease_owned()

    def _deployment(self, name: str) -> dict:
        if name not in (*DEPLOYMENTS, STORAGE_DEPLOYMENT):
            raise ValueError("deployment is outside the native recovery allowlist")
        value = self._json("get", "deployment", name, "-o", "json")
        capture = self._captures.get(name)
        if capture and value["metadata"]["uid"] != capture.uid:
            raise SmokeConfigurationError(f"refusing replaced deployment {name}")
        return value

    def _pods(self, name: str) -> list[dict]:
        capture = self._captures[name]
        sets = self._json("get", "replicasets", "-l", capture.selector, "-o", "json").get("items", [])
        owned = {
            item["metadata"]["uid"]
            for item in sets
            if any(
                owner.get("uid") == capture.uid and owner.get("controller") is True
                for owner in item["metadata"].get("ownerReferences", [])
            )
        }
        pods = self._json("get", "pods", "-l", capture.selector, "-o", "json").get("items", [])
        for pod in pods:
            if not any(
                owner.get("uid") in owned and owner.get("controller") is True
                for owner in pod["metadata"].get("ownerReferences", [])
            ):
                raise SmokeConfigurationError(f"selector for {name} includes an unowned pod")
        return pods

    def _wait(self, name: str, count: int, *, replaced: set[str] | None = None) -> None:
        deadline = time.monotonic() + ROLLOUT_TIMEOUT
        while time.monotonic() < deadline:
            deployment = self._deployment(name)
            status = deployment.get("status", {})
            pods = self._pods(name)
            current = [pod for pod in pods if not pod["metadata"].get("deletionTimestamp")]
            ready = all(
                pod.get("status", {}).get("phase") == "Running"
                and any(
                    c.get("type") == "Ready" and c.get("status") == "True"
                    for c in pod.get("status", {}).get("conditions", [])
                )
                for pod in current
            )
            old_gone = not replaced or not replaced.intersection(p["metadata"]["uid"] for p in pods)
            if (
                deployment["spec"].get("replicas", 1) == count
                and status.get("observedGeneration", 0) >= deployment["metadata"]["generation"]
                and status.get("readyReplicas", 0) == count
                and len(pods) == len(current) == count
                and ready and old_gone
            ):
                return
            time.sleep(0.5)
        raise TimeoutError(f"{name} did not reach {count} ready replacement pods")

    def _patch_autoscaler(self, name: str, *, uid: str, minimum: int, maximum: int) -> None:
        autoscaler = self._json("get", "hpa", name, "-o", "json")
        if autoscaler["metadata"]["uid"] != uid:
            raise SmokeConfigurationError(f"refusing replaced autoscaler {name}")
        patch = [
            {"op": "test", "path": "/metadata/resourceVersion", "value": autoscaler["metadata"]["resourceVersion"]},
            {"op": "replace", "path": "/spec/minReplicas", "value": minimum},
            {"op": "replace", "path": "/spec/maxReplicas", "value": maximum},
        ]
        result = self.harness.kubectl(
            "patch", "hpa", name, "--type=json", "--patch", json.dumps(patch), timeout=15
        )
        if result.returncode:
            raise SmokeConfigurationError(f"bounds update failed for autoscaler {name}; concurrent changes are not overwritten")

    def _pin_autoscalers(self, targets: tuple[str, ...]) -> None:
        """Hold each target's autoscaler at the Deployment's current replica count."""
        for item in self._json("get", "hpa", "-o", "json").get("items", []):
            target = item.get("spec", {}).get("scaleTargetRef", {})
            if target.get("kind") != "Deployment" or target.get("name") not in targets:
                continue
            name, spec = item["metadata"]["name"], item["spec"]
            capture = AutoscalerCapture(
                item["metadata"]["uid"], target["name"], int(spec.get("minReplicas", 1)), int(spec["maxReplicas"]),
            )
            self._fence()
            replicas = int(self._deployment(capture.target)["spec"].get("replicas", 1))
            # Captured before the write: a pin that lands but times out is still undone.
            self._autoscalers[name] = capture
            self._patch_autoscaler(name, uid=capture.uid, minimum=replicas, maximum=replicas)
        self.context.record("native_recovery_pinned_autoscalers", {
            name: {"target": value.target, "min": value.min_replicas, "max": value.max_replicas}
            for name, value in self._autoscalers.items()
        })

    def _scale(self, name: str, replicas: int) -> None:
        self._fence()
        deployment = self._deployment(name)
        patch = [
            {"op": "test", "path": "/metadata/resourceVersion", "value": deployment["metadata"]["resourceVersion"]},
            {"op": "replace", "path": "/spec/replicas", "value": replicas},
        ]
        result = self.harness.kubectl(
            "patch", "deployment", name, "--type=json", "--patch", json.dumps(patch), timeout=15
        )
        if result.returncode:
            raise SmokeConfigurationError(f"replica update failed for {name}; concurrent changes are not overwritten")
        self._wait(name, replicas)

    def stop_controller(self) -> None:
        self._scale("space-cluster-controller", 0)

    def restore_controller(self) -> None:
        name = "space-cluster-controller"
        self._scale(name, self._captures[name].replicas)

    def stop_object_storage(self) -> None:
        if STORAGE_DEPLOYMENT not in self._captures:
            raise SmokeConfigurationError("object storage fault was not captured")
        self._scale(STORAGE_DEPLOYMENT, 0)

    def restore_object_storage(self) -> None:
        self._scale(STORAGE_DEPLOYMENT, self._captures[STORAGE_DEPLOYMENT].replicas)

    def require_dispatcher_capacity(self) -> None:
        """Fairness needs an idle execution slot on the same dispatcher.

        Only the cluster-controller that holds the leader Lease runs the
        dispatcher, so any replica count has one dispatcher while election is
        on; with it switched off every replica would run one.
        """
        name = "space-cluster-controller"
        env = self.harness._deployment_env_literals(
            name, ("SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY",
                   "SPACE_PUBLIC_OPERATION_DISPATCHER_TICK_SECS",
                   LEADER_ELECTION_ENV)
        )
        election = env[LEADER_ELECTION_ENV]
        if self._captures[name].replicas != 1 and election in FALSE_VALUES:
            raise SmokeConfigurationError(
                "dispatcher fairness requires one controller, or leader election to choose one"
            )
        concurrency = env["SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY"]
        tick = env["SPACE_PUBLIC_OPERATION_DISPATCHER_TICK_SECS"]
        if (concurrency is not None and (not concurrency.isdigit() or int(concurrency) < 2)
                or tick is not None and (not tick.isdigit() or not 0 < int(tick) <= 5)):
            raise SmokeConfigurationError(
                "dispatcher fairness requires at least two execution slots and a tick of at most 5 s"
            )

    def _patch_budget_env(self, expected: list[dict], desired: list[dict], *, remove: bool = False) -> None:
        self._fence()
        name = "space-cluster-controller"
        self.harness.set_native_operation_probe_budgets(
            expected, desired, expected_uid=self._captures[name].uid, remove=remove
        )
        self._wait(name, self._captures[name].replicas)

    def _verify_budget_env(self, values: Mapping[str, str | None]) -> None:
        for pod in self._pods("space-cluster-controller"):
            containers = pod["spec"]["containers"]
            if len(containers) != 1:
                raise SmokeConfigurationError("controller budget verification requires one container")
            for key, value in values.items():
                entries = [entry for entry in containers[0].get("env", []) if entry.get("name") == key]
                matches = not entries if value is None else (
                    len(entries) == 1 and "valueFrom" not in entries[0]
                    and entries[0].get("value", "") == value
                )
                if not matches:
                    raise SmokeConfigurationError(f"running controller pod did not restore budget {key}")

    @contextmanager
    def operation_age_limit(self, *, seconds: int = 5, readiness_seconds: int | None = None) -> Iterator[None]:
        """Shorten only probe deadlines and restore their exact deployment entries."""
        if not 1 <= seconds <= 30 or readiness_seconds is not None and not 1 <= readiness_seconds <= 30:
            raise ValueError("probe operation budgets must be between 1 and 30 seconds")
        self._fence()
        name = "space-cluster-controller"
        values = {M1_AGE_ENV: str(seconds)}
        if readiness_seconds is not None:
            values.update({key: str(readiness_seconds) for key in READINESS_BUDGET_ENVS})
        standing = self.harness._deployment_env_literals(name, tuple(values))
        deployment = self._deployment(name)
        containers = deployment["spec"]["template"]["spec"]["containers"]
        if len(containers) != 1:
            raise SmokeConfigurationError("controller budget override requires one container")
        original = containers[0].get("env", [])
        had_env = "env" in containers[0]
        changed = [dict(entry) for entry in original]
        for key, value in values.items():
            matches = [entry for entry in changed if entry.get("name") == key]
            if len(matches) > 1:
                raise SmokeConfigurationError("controller budget environment contains duplicate entries")
            if matches:
                matches[0]["value"] = value
            else:
                changed.append({"name": key, "value": value})
        applied = False

        def restore_budgets() -> None:
            nonlocal applied
            if not applied:
                return
            current = self._deployment(name)["spec"]["template"]["spec"]["containers"][0].get("env", [])
            if current == original:
                # The attempted mutation may have failed before committing.
                self._wait(name, self._captures[name].replicas)
            else:
                self._patch_budget_env(changed, original, remove=not had_env)
            self._verify_budget_env(standing)
            applied = False
            self.context.record("native_probe_budgets_restored", True)

        fallback = self.context.scope.defer(
            "restore native operation probe budgets", restore_budgets, critical=True
        )
        primary = None
        try:
            # Arm before patch dispatch: a lost response can still commit.
            applied = True
            self._patch_budget_env(original, changed)
            self._verify_budget_env(values)
            self.context.record("native_probe_budgets", values)
            yield
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                restore_budgets()
                fallback.disarm()
            except Exception as error:
                self.context.record("native_probe_budget_restore_failure", str(error))
                if primary is None:
                    raise
                primary.add_note(f"Operation budget restoration also failed: {error}")

    def restart(self, name: str) -> None:
        if name not in {"space-api", "space-data-plane"}:
            raise ValueError("only API and data-plane restarts are supported")
        self._fence()
        pods = self._pods(name)
        before = {pod["metadata"]["uid"] for pod in pods}
        if not before:
            raise SmokeConfigurationError(f"cannot restart absent {name} pods")
        for pod in pods:
            self._fence()
            result = self.harness.kubectl(
                "delete", "pod", pod["metadata"]["name"], "--wait=false", timeout=15
            )
            if result.returncode:
                raise SmokeConfigurationError(f"failed to restart a captured {name} pod")
        self._wait(name, self._captures[name].replicas, replaced=before)
        self.context.record(f"{name}_restarted_pods", len(before))

    def restore(self) -> None:
        if self._release is None:
            return
        failures = []
        # Restore the controller first so accepted API operations can drain.
        for name in (STORAGE_DEPLOYMENT, "space-cluster-controller", "space-api", "space-data-plane"):
            capture = self._captures.get(name)
            if capture is None:
                continue
            try:
                self._fence()
                deployment = self._deployment(name)
                if deployment["spec"].get("replicas", 1) != capture.replicas:
                    self._scale(name, capture.replicas)
                else:
                    self._wait(name, capture.replicas)
                if _digest(self._deployment(name)["spec"]) != capture.spec_digest:
                    raise SmokeConfigurationError(f"{name} configuration changed during recovery")
            except Exception as error:
                failures.append(f"{name}: {type(error).__name__}: {error}")
        # Unpinned only after every Deployment is back at its captured count.
        for name, autoscaler in list(self._autoscalers.items()):
            try:
                self._fence()
                self._patch_autoscaler(
                    name, uid=autoscaler.uid,
                    minimum=autoscaler.min_replicas, maximum=autoscaler.max_replicas,
                )
                del self._autoscalers[name]
            except Exception as error:
                failures.append(f"autoscaler {name}: {type(error).__name__}: {error}")
        if failures:
            self.context.record("native_recovery_restore_failures", failures)
            raise RuntimeError("; ".join(failures))
        self._release()
        self._release = None
        self.context.record("native_recovery_restored", True)

    @contextmanager
    def owned_faults(self, *, object_storage: bool = False) -> Iterator[NativeRecoveryControl]:
        targets = (*DEPLOYMENTS, STORAGE_DEPLOYMENT) if object_storage else DEPLOYMENTS
        uid = self._identity()
        self._release = self.harness.acquire_retention_sweeper_mutation()
        fallback = self.context.scope.defer("restore native recovery deployments", self.restore, critical=True)
        primary = None
        try:
            self._pin_autoscalers(targets)
            for name in targets:
                deployment = self._deployment(name)
                spec = deployment["spec"]
                selector = spec["selector"]
                labels = selector.get("matchLabels", {})
                if selector.get("matchExpressions") or not labels or spec.get("replicas", 1) < 1:
                    raise SmokeConfigurationError(f"{name} requires a simple selector and positive replicas")
                self._captures[name] = DeploymentCapture(
                    deployment["metadata"]["uid"], spec.get("replicas", 1),
                    ",".join(f"{key}={value}" for key, value in sorted(labels.items())), _digest(spec),
                )
                self._wait(name, self._captures[name].replicas)
            self.context.record("native_recovery_target", {
                "context": self.harness.config.context, "cluster_uid": uid,
                "replicas": {name: value.replicas for name, value in self._captures.items()},
            })
            yield self
        except BaseException as error:
            primary = error
            raise
        finally:
            with self.context.phase("restore and verify native recovery deployments"):
                try:
                    self.restore()
                    fallback.disarm()
                except Exception as error:
                    if primary is None:
                        raise
                    primary.add_note(f"Recovery restoration also failed: {error}")


def expect_response(fixture: NativeHardeningFixture, response, status: int) -> dict:
    fixture.context.verify(
        response.status == status, f"native recovery expected HTTP {status}",
        response_status=response.status,
    )
    return response.body


def attach_after_recovery(fixture: NativeHardeningFixture, command: dict, *, last_event_id: str | None = None, max_events: int | None = None):
    # Recovery observes each transient result explicitly; command POST is never
    # replayed here, so a retry cannot silently restart guest execution.
    attempts = []
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            result = fixture.events(
                command, timeout=60, last_event_id=last_event_id, max_events=max_events
            )
        except NativeTransportError as error:
            attempts.append(type(error).__name__)
            time.sleep(0.5)
            continue
        attempts.append(result.status)
        if result.status == 200:
            fixture.context.record("recovery_attach_attempts", attempts)
            return result
        if result.status not in {409, 502, 503, 504}:
            break
        time.sleep(0.5)
    fixture.context.record("recovery_attach_attempts", attempts)
    raise RuntimeError("native recovery attach did not become available")


def stdout_bytes(events) -> bytes:
    return b"".join(
        base64.b64decode(event.data["data_base64"], validate=True)
        for event in events.events if event.data.get("type") == "stdout"
    )


def expect_success(fixture: NativeHardeningFixture, events) -> None:
    fixture.context.verify(
        any(event.data.get("type") == "process_exited" and event.data.get("exit_code") == 0 for event in events.events)
        and not any(event.data.get("type") == "error" for event in events.events),
        "recovered command must exit successfully without a stream error",
        recovered_event_types=[event.data.get("type") for event in events.events],
    )
