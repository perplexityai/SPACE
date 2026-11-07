"""A stopped sandbox's runtime upgrade boots in its pod and leaves it stopped; a start's boots it.

On Kubernetes a sandbox's microVM launches in its pod's shim, so a runtime
upgrade does too: the pod carries the `space.pplx.net/checkpoint:
runtime-upgrade` mode and its `space.pplx.net/runtime-upgrade-*` annotations,
and its shim calls sandbox-manager's `UpgradeRuntime`. The control plane
renders that pod for two callers this scenario drives:

- `SubstrateAdapter::upgrade_stopped_runtime`, which nothing in SPACE calls
  yet. The api's test-only trigger, `POST /debug/sandbox/{id}/upgrade-stopped-
  runtime` on the admin surface, mounted only while
  `SPACE_ADMIN_RUNTIME_UPGRADE_TEST_HOOK_ENABLED` is on, reaches it.
- a start of a stopped sandbox while `SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED`
  is on, on the API and the cluster controller (either may drive a start).

The scenario scopes both settings to itself under the public-API and
controller mutation Leases, and puts the standing literals back. It seeds the
execution environment catalog (`runtime_environments`) with E0 and
Firecracker 1.17.0 / Linux 6.1.155 (E1), sets the cluster
policy through the admin API, and restores the policy and catalog rows it
captured, through named SQL operations, since the policy store has no verb
that clears a default. Nodes also write the catalog: the cluster controller
rebuilds it from their certified upgrade paths every few minutes, so an edge
the scenario seeds that no node certifies lasts only until the next rebuild.

A sandbox can move to E1 only on a worker that installed and certifies E1,
which a worker shows by enforcing it as its default once the policy names it
(`GET /runtime-environments/nodes`). A bench whose workers don't is an
infrastructure error, never a pass.

Observables: the admin trigger's response, the pods the apiserver delivers
(watched from before each arm), the node's publication of the sandbox
(`sandbox_observed_states`: status and execution environment), the sandbox's
disk snapshot rows (polled: the trigger answers once the backup is enqueued),
the sandbox's lifecycle as the control plane's `sandboxes` row records it
(read through SQL, once after each trigger answers), and a file written
before the stop read back after the start.
"""

from __future__ import annotations

import dataclasses
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypeVar

from space_smoke.capabilities import (
    PublishedExecution,
    RuntimeEnvironmentSnapshot,
    RuntimeUpgradeControl,
    SandboxPodWatch,
)
from space_smoke.runtime import Compensation, InconclusiveSmoke, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import CommandSandboxFixture

# `docs/execution-environments.md`, "Selection and lifecycle".
E0 = "sha256:3168c96b5719ce62543ae53e6641a4eb80416a9f01e5585f6f76c3c70111fa0b"
E1 = "sha256:6e97c03b2523af6cc7b8e67bcde3fd89cf756c1958b10c24472aeadd68a32e0a"
# A binding the bench's nodes don't certify an edge to from E0, so the
# controller's edge rebuild never adds one: a default of E2 strands an E0
# sandbox however the rebuild's timing falls.
E2 = "sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4"
# The catalog rows a node build would publish for the bindings.
CATALOG: tuple[tuple[str, str, str, str], ...] = (
    (E0, "1.15.1", "6.1.155", "9.0.0"),
    (E1, "1.17.0", "6.1.155", "12.0.0"),
)
STRANDED_ROW: tuple[str, str, str, str] = (E2, "1.17.0", "6.18.44", "12.0.0")

ACTIVATION_ENV = "SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED"
HOOK_ENV = "SPACE_ADMIN_RUNTIME_UPGRADE_TEST_HOOK_ENABLED"
# The only env this scenario may touch on each Deployment.
API_ENV_KEYS: tuple[str, ...] = (ACTIVATION_ENV, HOOK_ENV)
CONTROLLER_ENV_KEYS: tuple[str, ...] = (ACTIVATION_ENV,)

# `crates/libs/common/core/src/k8s/runtime_upgrade.rs`.
CHECKPOINT_ANNOTATION = "space.pplx.net/checkpoint"
RUNTIME_UPGRADE_CHECKPOINT = "runtime-upgrade"
OPERATION_ANNOTATION = "space.pplx.net/runtime-upgrade-operation-id"
TARGET_ENVIRONMENT_ANNOTATION = "space.pplx.net/runtime-upgrade-target-environment"
TARGET_STATE_ANNOTATION = "space.pplx.net/runtime-upgrade-target-state"

T = TypeVar("T")


def poll(
    read: Callable[[], T], done: Callable[[T], bool], *, timeout: float, interval: float
) -> T:
    """Read until `done` or the deadline passes; the last reading either way."""

    deadline = time.monotonic() + timeout
    observed = read()
    while not done(observed) and time.monotonic() < deadline:
        time.sleep(interval)
        observed = read()
    return observed


def bare(sandbox_id: str) -> str:
    """The sandbox's UUID: the native surface names it `sbx_<uuid>`, the controls take the UUID."""

    return sandbox_id.removeprefix("sbx_")


def validate_runtime_upgrade_env(
    api: Mapping[str, Any], controller: Mapping[str, Any]
) -> None:
    """Refuse any key off the two closed lists."""

    for keys, values, owner in (
        (API_ENV_KEYS, api, "API"),
        (CONTROLLER_ENV_KEYS, controller, "cluster-controller"),
    ):
        unknown = sorted(set(values) - set(keys))
        if unknown:
            raise ValueError(
                f"refusing non-runtime-upgrade {owner} env keys: {unknown}"
            )


def annotations(document: Mapping[str, Any] | None) -> Mapping[str, str]:
    metadata = (document or {}).get("metadata") or {}
    return metadata.get("annotations") or {}


def runtime_upgrade_pod_problems(
    documents: list[Mapping[str, Any]], *, target_state: str, operation_id: str | None
) -> list[str]:
    """Why the pods an arm created don't all boot one upgrade to E1 with `target_state`."""

    if not documents:
        return ["no pod was created"]
    problems = []
    operations = set()
    for document in documents:
        name = ((document.get("metadata") or {}).get("name")) or "a pod"
        found = annotations(document)
        if found.get(CHECKPOINT_ANNOTATION) != RUNTIME_UPGRADE_CHECKPOINT:
            problems.append(f"{name}: checkpoint {found.get(CHECKPOINT_ANNOTATION)!r}")
        if found.get(TARGET_ENVIRONMENT_ANNOTATION) != E1:
            problems.append(
                f"{name}: target {found.get(TARGET_ENVIRONMENT_ANNOTATION)!r}"
            )
        if found.get(TARGET_STATE_ANNOTATION) != target_state:
            problems.append(
                f"{name}: target state {found.get(TARGET_STATE_ANNOTATION)!r}"
            )
        operations.add(found.get(OPERATION_ANNOTATION))
    if len(operations) != 1:
        problems.append(
            f"the pods name {len(operations)} operations: {sorted(map(str, operations))}"
        )
    elif operation_id is not None and operations != {operation_id}:
        problems.append(f"the pods name operation {operations}, not {operation_id}")
    return problems


def worker_enforces(nodes: list[Mapping[str, Any]], environment: str) -> list[str]:
    """The workers that don't enforce `environment` as their default at the current policy."""

    lagging = []
    for node in nodes:
        if node.get("role") != "worker":
            continue
        enforced = node.get("enforced") or {}
        if (
            node.get("conformance") != "conforms"
            or node.get("applied_current") is not True
            or enforced.get("default_environment_reference") != environment
        ):
            lagging.append(str(node.get("name") or node.get("node_id")))
    return lagging


class RuntimeUpgradeAssertions:
    """Scope the settings, catalog and policy to the scenario; judge each arm."""

    def __init__(
        self,
        fixture: CommandSandboxFixture,
        control: RuntimeUpgradeControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._releases: list[Compensation] = []
        self._restore_env: Compensation | None = None
        self._restore_catalog: Compensation | None = None
        self._standing_env: (
            tuple[dict[str, str | None], dict[str, str | None]] | None
        ) = None
        self._snapshot: RuntimeEnvironmentSnapshot | None = None
        self._watch: SandboxPodWatch | None = None
        self._watched_uids: frozenset[str] = frozenset()

    # --- the scoped settings, catalog and policy --------------------------------

    @contextmanager
    def scoped(self) -> Iterator[None]:
        """Put the standing env, catalog and policy back as the body unwinds, success or failure."""

        try:
            yield
        except BaseException as body_error:
            try:
                self.restore_standing()
            except Exception as restore_error:  # noqa: BLE001 - attached to the body's error
                body_error.add_note(
                    f"restoring the standing runtime-upgrade settings also failed: {restore_error!r}; "
                    "the scope's compensations stay armed"
                )
            raise
        self.restore_standing()

    def enable(self) -> None:
        """Both settings on, on the API and controller Deployments and their running pods."""

        for acquire, label in (
            (self._control.acquire_public_api_mutation, "public API"),
            (self._control.acquire_controller_mutation, "controller"),
        ):
            release = acquire()
            self._releases.append(
                self._scope.defer(
                    f"release the {label} mutation lease", release, critical=True
                )
            )
        standing = self._control.runtime_upgrade_env()
        self._standing_env = standing
        self._fixture.record(
            "runtime_upgrade_env_before",
            {"api": standing[0], "controller": standing[1]},
        )
        self._restore_env = self._scope.defer(
            "restore the standing runtime-upgrade env",
            lambda: self._control.restore_runtime_upgrade_env(*standing),
            critical=True,
        )
        self._control.set_runtime_upgrade_env(
            {key: "true" for key in API_ENV_KEYS},
            {key: "true" for key in CONTROLLER_ENV_KEYS},
        )
        live = self._control.runtime_upgrade_env()
        self._fixture.verify(
            all(value == "true" for value in live[0].values())
            and all(value == "true" for value in live[1].values()),
            "the rolled API and controller do not carry the runtime-upgrade settings",
            api=live[0],
            controller=live[1],
        )

    def seed_policy(self, *, edge: bool, e0_state: str, default: str = E1) -> None:
        """Seed the catalog (E0's upgrade edge to E1 only with `edge`), `default`, E0 in `e0_state`.

        A default of E2 seeds E2's row too, with no edge to it from E0."""

        if self._snapshot is None:
            snapshot = self._control.runtime_environment_snapshot()
            self._snapshot = snapshot
            self._restore_catalog = self._scope.defer(
                "restore the standing runtime environment catalog and policy",
                lambda: self._control.restore_runtime_environments(snapshot),
                critical=True,
            )
        rows = (*CATALOG, STRANDED_ROW) if default == E2 else CATALOG
        self._control.seed_runtime_environments(
            rows, upgrade_edges={E0: [E1] if edge else []}
        )
        self._control.set_runtime_environment_policy(
            default=default, states={E0: e0_state}
        )

    def restore_standing(self) -> None:
        if self._restore_catalog is not None:
            self._restore_catalog.release()
            self._restore_catalog = None
        if self._restore_env is not None and self._standing_env is not None:
            self._restore_env.release()
            after = self._control.runtime_upgrade_env()
            self._fixture.verify(
                after == self._standing_env,
                "the runtime-upgrade env did not come back to its standing values",
                standing=self._standing_env,
                after=after,
            )
            self._restore_env = None
        while self._releases:
            self._releases.pop().release()

    # --- preconditions ----------------------------------------------------------

    def require_workers_enforce_target(self, *, timeout: float = 300) -> None:
        """Every worker certifies E1 and enforces it as its default; otherwise the bench can't run this."""

        lagging = poll(
            lambda: worker_enforces(self._control.runtime_policy_nodes(), E1),
            lambda lagging: not lagging,
            timeout=timeout,
            interval=5,
        )
        if lagging:
            # Not the change under test: the bench lacks Firecracker 1.17.0 / Linux
            # 6.1.155, or hasn't applied the policy.
            raise RuntimeError(
                f"workers {lagging} don't enforce {E1} as their default; install and approve "
                "Firecracker 1.17.0 / Linux 6.1.155 on every Kind worker"
            )

    # --- each arm --------------------------------------------------------------

    def published(self, sandbox_id: str) -> PublishedExecution:
        return self._control.published_execution(bare(sandbox_id))

    def expect_published(
        self,
        sandbox_id: str,
        *,
        status: str,
        environment: str,
        unbound_ok: bool = False,
        timeout: float = 120,
    ) -> None:
        """With `unbound_ok`, a publication with no binding passes too: a
        sandbox made from an unbound template (the Kind seed) runs E0 as
        legacy unbound work, and the node publishes no environment for it."""

        accepted = {environment, None} if unbound_ok else {environment}
        observed = poll(
            lambda: self.published(sandbox_id),
            lambda observed: (
                observed.status == status and observed.environment in accepted
            ),
            timeout=timeout,
            interval=2,
        )
        if not (observed.status == status and observed.environment in accepted):
            self._fixture.verify(
                False,
                f"the node did not publish {sandbox_id} {status} under {environment}",
                observed=dataclasses.asdict(observed),
            )

    def watch(self, sandbox_id: str) -> None:
        self._watch = self._control.watch_sandbox_pods(bare(sandbox_id))
        self._scope.defer(f"stop watching the pods of {sandbox_id}", self._watch.stop)
        self._watched_uids = self._watch.listed_uids()

    def created_pods(self) -> list[Mapping[str, Any]]:
        """The latest version of every pod created since [`Self::watch`]."""

        if self._watch is None:
            raise InconclusiveSmoke("no pod watch is running")
        # A watch that lost events or ended early would hide pods: its history
        # isn't evidence that no other pod was created.
        watch_failure = self._watch.failure()
        self._fixture.verify(
            watch_failure is None,
            "the sandbox pod watch ended before its pods could be judged",
            watch_failure=watch_failure,
        )
        latest: dict[str, Mapping[str, Any]] = {}
        for _view, document in self._watch.versions():
            uid = str(((document.get("metadata") or {}).get("uid")) or "")
            if uid and uid not in self._watched_uids:
                latest[uid] = document
        return list(latest.values())

    def upgrade_stopped(self, sandbox_id: str) -> tuple[int, Mapping[str, Any]]:
        status, body = self._control.upgrade_stopped_runtime(bare(sandbox_id))
        self._fixture.record(
            f"upgrade_stopped_runtime_{sandbox_id}", {"status": status, "body": body}
        )
        return status, body

    def lifecycle(self, sandbox_id: str) -> str:
        """The lifecycle the control plane's `sandboxes` row records, read through SQL."""

        return self._control.stopped_residency(bare(sandbox_id)).lifecycle

    def disk_snapshot_ids(self, sandbox_id: str) -> list[str]:
        return self._control.disk_snapshot_ids(bare(sandbox_id))

    def wait_new_disk_snapshot(
        self, sandbox_id: str, before: set[str], *, timeout: float = 180
    ) -> set[str]:
        """The disk snapshot rows that appeared since `before`, polled until one does or the deadline passes."""

        return poll(
            lambda: set(self.disk_snapshot_ids(sandbox_id)) - before,
            bool,
            timeout=timeout,
            interval=2,
        )

    def wait_no_live_pod(self, sandbox_id: str, *, timeout: float = 120) -> None:
        pods = poll(
            lambda: self._control.sandbox_pods(bare(sandbox_id)),
            lambda pods: not pods,
            timeout=timeout,
            interval=2,
        )
        if pods:
            self._fixture.verify(
                False,
                f"the upgrade's pod for {sandbox_id} outlived it",
                pods=[pod.pod_name for pod in pods],
            )
