"""Reusable contracts between smoke scenarios and environment adapters."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, BinaryIO, Generic, Protocol, TypeVar


@dataclass(frozen=True)
class KindColdCreateConfig:
    vcpu: int
    memory_mib: int


@dataclass(frozen=True)
class KindWarmPoolConfig:
    vcpu: int
    memory_mib: int


@dataclass(frozen=True)
class ColdCreateSelection:
    template: str
    config_id: str
    ownership_receipt: str
    verify: Callable[[], None]


@dataclass(frozen=True)
class WarmPoolSelection:
    template: str
    config_id: str
    ownership_receipt: str
    verify: Callable[[], None]
    restore: Callable[[], None]


@dataclass(frozen=True)
class ColdCreatePod:
    sandbox_id: str
    pod_name: str
    pod_uid: str
    is_cold: bool
    rejected_markers: tuple[str, ...]


class ColdCreateObserver(Protocol):
    def cold_create_pod(self, sandbox_id: str) -> ColdCreatePod: ...


@dataclass(frozen=True)
class WarmPoolCandidate:
    sandbox_id: str
    pod_name: str
    pod_uid: str
    config_id: str
    slot_id: str
    node_name: str
    state: str


class WarmPoolObserver(Protocol):
    def warm_pool_candidate(self, config_id: str) -> WarmPoolCandidate: ...

    def warm_pool_claim_pod(self, sandbox_id: str) -> WarmPoolCandidate: ...


class EcwSandboxUnavailable(RuntimeError):
    """The public data plane cannot currently reach a known sandbox."""


class EcwDataPlaneNotFound(EcwSandboxUnavailable):
    """The data plane returned 404 for a sandbox-scoped request."""


class EcwPermissionDenied(RuntimeError):
    """The guest rejected an operation for the selected sandbox identity."""


class EcwCreateNotDelivered(RuntimeError):
    """A sandbox create failed before HTTP delivery and is safe to retry."""


class EcwSandboxDeleteRetryable(RuntimeError):
    """A sandbox delete should be retried after a transient API response."""


class EcwSandboxBusy(EcwSandboxDeleteRetryable):
    """A sandbox delete conflicts with an in-flight lifecycle transition."""


class EcwSandboxState(StrEnum):
    RUNNING = "running"
    PAUSED = "paused"


class EcwFilesystemEventType(StrEnum):
    CHMOD = "chmod"
    CREATE = "create"
    REMOVE = "remove"
    RENAME = "rename"
    WRITE = "write"


class EcwTimeoutAction(StrEnum):
    KILL = "kill"
    PAUSE = "pause"


@dataclass(frozen=True)
class EcwLifecycle:
    on_timeout: EcwTimeoutAction
    auto_resume: bool = False

    def __post_init__(self) -> None:
        if self.auto_resume and self.on_timeout is not EcwTimeoutAction.PAUSE:
            raise ValueError("auto-resume requires on-timeout pause")


@dataclass(frozen=True)
class NetworkConfig:
    """ECW network options that materially change a sandbox invariant."""

    allow_public_traffic: bool | None
    allow_out: tuple[str, ...] = ()
    deny_out: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if any(not destination for destination in (*self.allow_out, *self.deny_out)):
            raise ValueError("network destinations must not be empty")


@dataclass(frozen=True)
class EcwCreateAttempt:
    """Observable response contract for a sandbox create request."""

    status_code: int
    body: str
    sandbox_id: str | None


@dataclass(frozen=True)
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class SandboxInfo:
    sandbox_id: str


SandboxInfoT = TypeVar("SandboxInfoT", bound=SandboxInfo, covariant=True)


@dataclass(frozen=True)
class SandboxPages(Generic[SandboxInfoT]):
    pages: tuple[tuple[SandboxInfoT, ...], ...]
    has_more: bool


# Compatibility name for the ECW-only scenarios that predate the shared
# command seam.
EcwCommandResult = CommandResult


class CommandSandbox(Protocol):
    """The protocol-neutral command surface used by the shared smoke."""

    @property
    def sandbox_id(self) -> str: ...

    def get_info(self) -> SandboxInfo: ...

    def try_get_info(self) -> SandboxInfo | None: ...

    def run(self, command: str, *, timeout: int = 60) -> CommandResult: ...


class DurableLifecycleSandbox(CommandSandbox, Protocol):
    """Command sandbox that can leave and later regain resident capacity."""

    def suspend(self) -> None: ...

    def wake(self) -> None: ...


class ResidentLifecycleSandbox(CommandSandbox, Protocol):
    """Command sandbox that can pause while retaining resident capacity."""

    def pause(self) -> None: ...

    def wake(self) -> None: ...


class StoppableLifecycleSandbox(CommandSandbox, Protocol):
    """Command sandbox that can stop, keeping its rootfs on the node, and restart from it.

    `stop` / `start` carry no snapshot-persistence promise (see the lifecycle
    vocabulary in AGENTS.md): the rootfs stays where the sandbox last ran and a
    restart re-materializes the execution there.
    """

    def stop(self) -> None: ...

    def start(self) -> None: ...


class ProcessHandle(Protocol):
    @property
    def process_id(self) -> str: ...

    @property
    def pid(self) -> int: ...


@dataclass(frozen=True)
class ProcessInfo:
    process_id: str
    pid: int
    command: tuple[str, ...]


class ProcessSandbox(CommandSandbox, Protocol):
    """Protocol-neutral background process control used by the shared smoke."""

    def start_background(self, command: str, *, timeout: int = 60) -> ProcessHandle: ...

    def list_processes(self) -> tuple[ProcessInfo, ...]: ...

    def get_process(self, process_id: str) -> ProcessInfo | None: ...

    def signal_process(self, process_id: str, signal: int) -> bool: ...

    def kill_process(self, process_id: str) -> bool: ...


class CommandSandboxFactory(Protocol):
    """A configured owner of scenario-scoped command sandboxes."""

    def create(self, metadata: Mapping[str, str] | None = None) -> CommandSandbox: ...

    def delete(self, sandbox: CommandSandbox) -> None: ...

    def list_pages(
        self,
        *,
        limit: int,
        max_pages: int,
        stop_after_sandbox_ids: Collection[str] | None = None,
    ) -> SandboxPages[SandboxInfo]: ...


@dataclass(frozen=True)
class EcwSandboxInfo(SandboxInfo):
    template_id: str | None
    state: EcwSandboxState | None = None


class EcwSandboxPages(SandboxPages[EcwSandboxInfo]):
    """ECW pages retain rich sandbox information at the shared page seam."""


class ScenarioSandboxes(Protocol):
    @property
    def items(self) -> tuple[CommandSandbox, ...]: ...

    def list_pages(
        self, *, limit: int, max_pages: int
    ) -> SandboxPages[SandboxInfo]: ...

    def delete(self, sandbox: CommandSandbox) -> int: ...


@dataclass(frozen=True)
class EcwFileEntry:
    name: str


@dataclass(frozen=True)
class EcwFilesystemEvent:
    name: str
    type: EcwFilesystemEventType


@dataclass(frozen=True)
class EcwPublicPort:
    """Public request route derived from an SDK sandbox port."""

    base_url: str
    headers: Mapping[str, str]


class EcwProcess(ProcessHandle, Protocol):
    @property
    def pid(self) -> int: ...

    def disconnect(self) -> None: ...

    def wait(self) -> EcwCommandResult: ...

    def kill(self) -> bool: ...


class EcwWatchHandle(Protocol):
    def get_new_events(self) -> tuple[EcwFilesystemEvent, ...]: ...

    def stop(self) -> None: ...


class EcwFilesystem(Protocol):
    def write(
        self,
        path: str,
        data: str | bytes | BinaryIO,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None: ...

    def read(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> str: ...

    def read_bytes(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> bytes: ...

    def make_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None: ...

    def exists(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> bool: ...

    def list(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> tuple[EcwFileEntry, ...]: ...

    def rename(
        self,
        old_path: str,
        new_path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None: ...

    def remove(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None: ...

    def watch_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        recursive: bool = False,
        request_timeout: float = 60,
    ) -> EcwWatchHandle: ...


class EcwProtocolCompatibility(Protocol):
    """Public ECW protocol probes the high-level SDK cannot express."""

    def read_with_basic_auth(
        self,
        path: str,
        *,
        user: str,
        request_timeout: float = 10,
    ) -> str:
        """Read without a query selector while sending Basic Auth identity."""
        ...

    def probe_watch_stream(
        self,
        path: str,
        *,
        user: str | None = None,
        recursive: bool = True,
        request_timeout: float = 10,
    ) -> None:
        """Open and immediately close the legacy streaming WatchDir route."""
        ...


@dataclass(frozen=True)
class RestoreCallerOutcome:
    """One concurrent caller's result.

    `status` is the HTTP status (0 = transport failure); `finished_at` is a
    monotonic timestamp taken as the response arrived, so an outcome can be
    ordered against other events the fixture timestamps the same way.
    """

    status: int
    elapsed_seconds: float
    finished_at: float
    detail: str = ""


class RestoreRacer(Protocol):
    """Drive the public ECW `connect` route for a sandbox without the SDK's own retries.

    One call is one `POST /sandboxes/{id}/connect`; the caller's outcome is
    the invariant, so it is returned rather than raised.
    """

    def race_connect(
        self, *, timeout: float, gate: Callable[[], None] | None = None
    ) -> RestoreCallerOutcome:
        """One `connect`; `gate` (a barrier wait) is called right before the request leaves."""
        ...


@dataclass(frozen=True)
class LimiterProbeOutcome:
    """One raw ECW request judged by the rate-limiter scenario.

    `status` 0 is a transport failure; `retry_after` is the raw header value
    (the verdict checks it is delta-seconds); `message` is the response body
    (or the error body), which for a 429 starts with the rejecting limiter's
    prefix; `sandbox_id` is set when a create was admitted.
    """

    status: int
    retry_after: str | None
    message: str
    sandbox_id: str | None
    finished_at: float


class RateLimitProbe(Protocol):
    """Raw ECW control-plane requests for one client id, plus their cleanup.

    The limiter scenario judges statuses and headers the SDK hides, so it
    goes to the route directly; every create it admits carries the metadata
    `kill_matching` later deletes by.
    """

    def race_ecw_request(
        self,
        method: str,
        path: str,
        body: Mapping[str, object] | None,
        *,
        client_id: str,
        timeout: float,
        gate: Callable[[], None] | None = None,
    ) -> LimiterProbeOutcome: ...

    def kill_matching(self, metadata: Mapping[str, str]) -> None: ...

    def kill_sandbox(self, sandbox_id: str) -> None:
        """ECW kill of one bare sandbox uuid, not-found counting as done."""
        ...


class RateLimiterControl(Protocol):
    """Privileged kind-only operations for the rate-limiter scenario.

    The public API Deployment's limiter env is captured before it is written
    and restored to the captured literals (never blanket-unset); a `valueFrom`
    entry is refused because a literal could not put the reference back. The
    Lease serializes cooperating smokes on the same Deployment. Redis is the
    kind `space-redis` Deployment; taking it to zero is the fail-open fault.
    """

    def acquire_public_api_mutation(self) -> Callable[[], None]: ...

    def public_api_env(self, keys: Sequence[str]) -> dict[str, str | None]: ...

    def set_public_api_env(self, overrides: Mapping[str, str]) -> None: ...

    def restore_public_api_env(self, standing: Mapping[str, str | None]) -> None: ...

    def wait_public_api_ready(self, *, timeout: float) -> None: ...

    def redis_replicas(self) -> int:
        """The Redis Deployment's current `.spec.replicas`, read without writing."""
        ...

    def scale_redis(self, replicas: int) -> int: ...

    def wait_redis_unreachable(self, *, timeout: float) -> None: ...

    def wait_redis_rollout(self, *, timeout: float) -> None: ...


@dataclass(frozen=True)
class AdminNodeGroupView:
    """One node group as `GET /admin/api/v1/node-groups` lists it."""

    id: str
    name: str
    org_id: str | None
    is_default: bool
    nodes_count: int


class NodeGroupControl(Protocol):
    """Kind-only operations for the node-group placement scenario.

    The gate (`SPACE_SANDBOX_NODE_GROUP_PLACEMENT`, api + cluster-controller)
    and the labeler (`SPACE_NODE_GROUP_LABELER_ENABLED`, cluster-controller)
    are literal Deployment env captured before they are written and restored
    to the captured values; the admin node-group routes are the product
    surface; the worker boundary labels are snapshotted and restored because
    the labeler writes the whole fleet.
    """

    def acquire_public_api_mutation(self) -> Callable[[], None]: ...

    def node_group_env(self) -> dict[str, str | None]: ...

    def set_node_group_env(self, *, gate: bool, labeler: bool) -> None: ...

    def restore_node_group_env(self, standing: Mapping[str, str | None]) -> None: ...

    def wait_public_api_ready(self, *, timeout: float) -> None: ...

    def node_groups(self) -> list[AdminNodeGroupView]: ...

    def try_create_node_group(self, *, name: str, org_id: str | None) -> tuple[int, str]:
        """`POST /admin/api/v1/node-groups`; returns (status, body) so a refusal is judged, not raised."""
        ...

    def worker_boundary_labels(self) -> dict[str, str | None]: ...

    def restore_worker_boundary_labels(self, snapshot: Mapping[str, str | None]) -> None: ...

    def sandbox_pod_node_group_terms(self, sandbox_id: str) -> list[str]:
        """Every value of the boundary key across the pod's required nodeSelectorTerms."""
        ...

    def sandbox_pod_phase_and_node(self, sandbox_id: str) -> tuple[str | None, str | None]: ...

    def admin_org_id(self) -> str:
        """The kind admin's org id, for admin routes that scope a resource to an org."""
        ...


@dataclass(frozen=True)
class PendingBackupRow:
    """One `node_local_backup_pending` row for a sandbox: the artifact a node reported unbacked."""

    artifact_id: str
    artifact_kind: str


@dataclass(frozen=True)
class CensusPublication:
    """One `node state snapshot published` line from space-internal-api, Full censuses only."""

    node_id: str
    outcome: str
    reported: int


class PendingBackupControl(Protocol):
    """Kind-only operations for the pending-backup census scenario.

    The object store fault is the kind `space-object-store` Deployment scaled to
    zero with its scale-back armed first; the observables are the cluster's
    `node_local_backup_pending` queue, space-internal-api's census publication
    log, the sandbox's snapshot rows and the backed-artifact predicate.
    """

    def object_store_replicas(self) -> int: ...

    def scale_object_store(self, replicas: int) -> int: ...

    def wait_object_store_gone(self, *, timeout: float) -> None: ...

    def wait_object_store_rollout(self, *, timeout: float) -> None: ...

    def log_since_time(self) -> str: ...

    def suspend_without_waiting(self, sandbox_id: str) -> str:
        """`POST /v1/sandboxes/{id}:suspend` as the kind admin; returns the operation id, does not wait.

        The ECW `pause` is synchronous on durability (the API polls the node
        for up to 300 s before answering), so with uploads blocked it can only
        fail; the native suspend is an asynchronous operation, which is what
        lets the snapshot be observed in flight.
        """
        ...

    def operation_state(self, operation_id: str) -> str: ...

    def snapshot_states(self, sandbox_id: str) -> list[str]: ...

    def pending_backup_queue(self, sandbox_id: str) -> list[PendingBackupRow]: ...

    def census_publications(self, *, since_time: str) -> list[CensusPublication]: ...

    def artifact_backed(self, sandbox_id: str, local_artifact_id: str) -> bool: ...


@dataclass(frozen=True)
class RecoveryAttempt:
    """One `RetryScheduled` no-progress record of the bounded quarantine recovery loop."""

    sandbox_id: str
    attempt: int
    at: float
    """Epoch seconds of the log line."""


class QuarantineRecoveryControl(Protocol):
    """Kind-only operations for the quarantine-recovery pacing scenario.

    The fault is a planted `snapshots/.suspend-publish-*` staging directory under
    a sandbox cache directory with no metadata row, on the worker running the
    fixture sandbox; the observables are that worker's sandbox-manager log,
    which lives and dies with its pod (hence the uid pin).
    """

    def sandbox_manager_for_sandbox(self, sandbox_id: str) -> tuple[str, str, str]:
        """The sandbox's worker node, its sandbox-manager pod and that pod's uid."""
        ...

    def pod_uid(self, pod_name: str) -> str | None: ...

    def plant_orphan_staging(self, node_name: str, sandbox_id: str) -> None: ...

    def remove_orphan_sandbox(self, node_name: str, sandbox_id: str) -> None:
        """`space-sbxmgr delete` the induced row, then remove its directory."""
        ...

    def orphan_absent(self, node_name: str, sandbox_id: str) -> bool: ...

    def orphan_quarantined(self, pod_name: str, sandbox_id: str) -> bool:
        """The log names the sandbox with reason `orphaned_sandbox_row_missing`."""
        ...

    def recovery_attempts(self, pod_name: str, sandbox_id: str) -> list[RecoveryAttempt]:
        """`RetryScheduled` no-progress records for the sandbox, oldest first.

        Raises when a `RetryScheduled` record carries no `attempt_count`: the
        oracle no longer matches the log site and cannot judge the build.
        """
        ...


class OrphanedRestorePodGcControl(Protocol):
    """Kind-only operations for the orphaned-restore-pod GC scenario.

    The reaper ships default-off and dry-run and reads its config once at
    startup, so the scenario overrides the cluster-controller Deployment's
    `SPACE_ORPHANED_RESTORE_POD_GC_*` env (closed key list, closed value
    vocabulary) under the controller-mutation Lease the retention scenarios
    share, and puts the captured standing values back. The orphans are
    clones of a real restore pod renamed to a retired generation and pinned
    to a nodeSelector no node carries, so they stay Pending and never boot a
    VM; each is created with a per-plant label its deletion selects on. The
    observables are the controller's structured log lines.
    """

    def acquire_controller_mutation(self) -> Callable[[], None]:
        """Take the cluster-visible controller-deployment Lease; returns its release."""
        ...

    def orphaned_restore_pod_gc_env(self) -> dict[str, str | None]:
        """The literal value of every GC env key on the controller Deployment, None if unset."""
        ...

    def set_orphaned_restore_pod_gc_env(self, overrides: Mapping[str, str]) -> None:
        """Apply smoke overrides and await the controller rollout; refuses keys or values off the lists."""
        ...

    def restore_orphaned_restore_pod_gc_env(self, standing: Mapping[str, str | None]) -> None:
        """Put the captured literals back (None unsets) and await the controller rollout."""
        ...

    def controller_log_records(self, *, since_time: str) -> list[dict[str, Any]]:
        """GC-related structured records from every live controller pod since `since_time`."""
        ...

    def restore_pod_document(self, pod_name: str) -> dict[str, Any] | None:
        """The apiserver's JSON for one sandbox pod, None when it does not exist."""
        ...

    def plant_orphaned_restore_pod(
        self,
        template: Mapping[str, Any],
        *,
        name: str,
        sandbox_id: str,
        generation: int,
        token: str,
    ) -> str:
        """Create an unschedulable clone of `template` as `name`; returns its uid."""
        ...

    def remove_planted_restore_pods(self, token: str) -> None:
        """Delete the pods carrying this plant token's label; absent is success."""
        ...

    def sandbox_executions(self, sandbox_id: str) -> list[ExecutionRow]: ...

    def sandbox_pods(self, sandbox_id: str) -> list[SandboxPodView]: ...

    def sandbox_residency(self, sandbox_id: str) -> SandboxResidency: ...


@dataclass(frozen=True)
class TemplateRecipe:
    """A Dockerfile-plus-SDK template definition, rendered by the SDK adapter."""

    dockerfile: str
    files: Mapping[str, str]
    """Context files beside the Dockerfile, by relative name."""
    envs: Mapping[str, str]
    run_cmds: tuple[tuple[str, str], ...]
    """`(command, user)` pairs appended after the Dockerfile."""
    start_cmd: str
    ready_timeout_seconds: int


@dataclass(frozen=True)
class TemplateBuildResult:
    template_id: str
    build_id: str
    alias: str
    elapsed_seconds: float
    log_tail: tuple[str, ...]


class TemplateBuilder(Protocol):
    """The public SDK surface a build scenario needs: build a template, create from it."""

    def build_template(
        self,
        recipe: TemplateRecipe,
        *,
        alias: str,
        cpu_count: int,
        memory_mb: int,
        tags: Sequence[str],
    ) -> TemplateBuildResult:
        """`Template.build` (blocking; the SDK polls build status) with the run's credentials."""
        ...

    def create(
        self,
        *,
        template: str,
        timeout: int,
        metadata: Mapping[str, str],
    ) -> EcwSandbox: ...


@dataclass(frozen=True)
class GuestMetadata:
    """What the published template says about its guest versus what its rootfs holds."""

    spaced_version: str
    compatibility_family: str
    bundle_version: str


class TemplateBuildControl(Protocol):
    """Kind-only operations for the template-build scenario.

    The two builders' node-apis sit behind one Service; pinning it to one
    builder's node-api pod by a routing label is what makes the handoff
    observable. A builder is named by its node: build-manager's log there says
    whether it accepted the build, and artefact reads run in that node's
    operator shell.
    """

    def builder_nodes(self) -> list[str]:
        """Builder nodes with a Ready node-api, stable order."""
        ...

    def route_builder_service(self, route: str, node_name: str, *, timeout: float = 30) -> None:
        """Label the builder's node-api pod and point the Service selector at it; wait until it is the only endpoint."""
        ...

    def clear_builder_service_route(self) -> None: ...

    def builder_logged_alias(self, node_name: str, alias: str) -> bool: ...

    def prepared_rootfs_handoff(self, node_name: str, *, window_seconds: int = 60) -> str | None:
        """None once the builder committed a prepared rootfs for its build; else, after
        `window_seconds` without one, the failure text."""
        ...

    def published_guest_metadata(self, node_name: str, alias: str) -> GuestMetadata: ...

    def containerd_residue(self, node_name: str, image: str) -> list[str]:
        """Source-image records, temporary image records and temporary paths left behind; empty when clean."""
        ...

    def delete_template_as_admin(self, alias: str) -> str | None:
        """`DELETE /admin/api/v1/templates/{alias}`; the operation id, or None when already gone."""
        ...

    def operation_state(self, operation_id: str) -> str: ...

@dataclass(frozen=True)
class NodeSnapshot:
    """One node as the eligible-node filter sees it: labels, readiness, cordon."""

    name: str
    labels: Mapping[str, str]
    ready: bool
    unschedulable: bool


@dataclass(frozen=True)
class SandboxPodRender:
    """What a sandbox's newest non-terminating pod rendered and where it landed."""

    pod_name: str
    node_selector: Mapping[str, str]
    phase: str | None
    node_name: str | None
    pool_role: str | None


class NodeSelectorExtrasControl(Protocol):
    """Kind-only operations for the nodeSelector-extras scenario.

    The renderers' env (`api:<KEY>` / `controller:<KEY>` / `internal-api:<KEY>` literals) is captured
    before it is written and restored to the captured values; the node snapshot
    is the apiserver's, read once per question so the answers agree.
    """

    def acquire_public_api_mutation(self) -> Callable[[], None]: ...

    def renderer_env(self) -> dict[str, str | None]:
        """Renderer-prefixed literals for the selector keys and the admission switch."""
        ...

    def set_renderer_env(self, values: Mapping[str, str | None]) -> None: ...

    def restore_renderer_env(self, standing: Mapping[str, str | None]) -> None: ...

    def wait_public_api_ready(self, *, timeout: float) -> None: ...

    def nodes_snapshot(self) -> list[NodeSnapshot]: ...

    def nodes_carrying_label(self, key: str) -> list[str]: ...

    def node_labels(self, node_name: str) -> dict[str, str]: ...

    def sandbox_pod_render(self, sandbox_id: str) -> SandboxPodRender | None:
        """The newest non-terminating pod's render; `None` does not mean the pod is gone."""
        ...

    def sandbox_pods(self, sandbox_id: str) -> list[SandboxPodView]:
        """The sandbox's execution pods by label, including terminating ones."""
        ...

    def sandbox_pod_count(self) -> int:
        """Non-pool, non-terminating sandbox pods in the namespace."""
        ...

    def native_create_sandbox(self, *, template: str, metadata: Mapping[str, str]) -> str:
        """`POST /v1/sandboxes` as the kind admin; waits for the operation; returns the bare sandbox uuid."""
        ...


@dataclass(frozen=True)
class NodeCtlIdentity:
    """The node-ctl pod on a worker, its uid and the ServiceAccount its token names."""

    pod_name: str
    pod_uid: str
    node_name: str
    service_account: str


@dataclass(frozen=True)
class FencePolicy:
    name: str
    validation_actions: list[str]
    type_check_warnings: int


@dataclass(frozen=True)
class WhoAmI:
    username: str
    node_claim: str | None


@dataclass(frozen=True)
class SaWriteOutcome:
    """A kubectl write made with the ServiceAccount kubeconfig: exit code and combined output."""

    returncode: int
    output: str


class NodeFenceControl(Protocol):
    """Kind-only operations for the node-ctl admission-fence scenario.

    Writes as the ServiceAccount go through a kubeconfig whose only user is the
    projected token — with the admin client certificate still ambient, TLS auth
    would win and every refusal would be a false negative. Admin repairs use
    the harness's own context.
    """

    @property
    def namespace(self) -> str: ...

    def acquire_worker_cordon_mutation(self) -> Callable[[], None]: ...

    def node_ctl_identity_for_sandbox(self, sandbox_id: str) -> NodeCtlIdentity: ...

    def kube_node_names(self) -> list[str]: ...

    def fence_policy_for(self, service_account: str) -> FencePolicy | None: ...

    def pod_file_readable(self, pod_name: str, path: str) -> bool: ...

    def pod_file(self, pod_name: str, path: str) -> str: ...

    def bound_token(self, identity: NodeCtlIdentity) -> str:
        """A `TokenRequest` bound to the pod; the documented fallback when the mount is absent."""
        ...

    def kubeconfig_with_token(self, token: str) -> str:
        """Path of a minified kubeconfig whose only user is the token; removed at scope end by the caller's harness."""
        ...

    def whoami(self, kubeconfig: str) -> WhoAmI: ...

    def can_i_patch_nodes(self, kubeconfig: str) -> bool: ...

    def node_json(self, node_name: str) -> dict[str, Any]: ...

    def node_label(self, node_name: str, key: str) -> str | None: ...

    def node_capacity(self, node_name: str, resource: str) -> str | None:
        """`status.capacity[resource]`, None when absent or unreadable."""
        ...

    def patch_node_as(
        self, kubeconfig: str, node_name: str, patch: Mapping[str, object], *, subresource: str | None = None
    ) -> SaWriteOutcome: ...

    def apply_as(
        self, kubeconfig: str, manifest: str, *, field_manager: str, subresource: str | None = None
    ) -> SaWriteOutcome:
        """`kubectl apply --server-side --force-conflicts` as the ServiceAccount."""
        ...

    def admin_label_node(self, node_name: str, key: str, value: str | None) -> None: ...

    def admin_annotate_node(self, node_name: str, key: str, value: str | None) -> None: ...

    def admin_patch_node(self, node_name: str, patch: Mapping[str, object], *, subresource: str | None = None) -> None: ...

    def admin_remove_capacity(self, node_name: str, resource: str) -> None: ...

    def revert_node_from_snapshot(self, node_name: str, snapshot: Mapping[str, object]) -> list[str]:
        """Restore every field a probe can touch, keyed on markers; returns the repairs it had to make."""
        ...

    def node_ctl_log_tail(self, pod_name: str, lines: int) -> list[str]: ...


class ForkableSandbox(Protocol):
    """The public ECW fork route, which the pinned SDK does not expose.

    A fork captures a *disk* named snapshot of the source (encoded to a
    parent-relative btrfs stream in a plain directory), uploads it, and
    creates the child from the durable copy. The source artifact is left
    behind unpinned — the artifact class the on-node snapshot reaper exists
    to reclaim.
    """

    def fork(self, *, timeout: int = 600) -> str:
        """Fork the sandbox once; returns the child sandbox id. Synchronous."""
        ...

    def delete_forked(self, child_id: str) -> None:
        """Delete one fork child through the public route; idempotent on 404."""
        ...


class EcwSandbox(ProcessSandbox, Protocol):
    @property
    def sandbox_id(self) -> str: ...

    @property
    def files(self) -> EcwFilesystem: ...

    @property
    def compatibility(self) -> EcwProtocolCompatibility: ...

    def run(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
        envs: Mapping[str, str] | None = None,
    ) -> EcwCommandResult: ...

    def start_background(self, command: str, *, timeout: int = 60) -> EcwProcess: ...

    def connect_process(self, pid: int, *, timeout: int = 60) -> EcwProcess: ...

    def public_port(self, port: int) -> EcwPublicPort: ...

    def get_info(self) -> EcwSandboxInfo: ...

    def try_get_info(self) -> EcwSandboxInfo | None: ...

    def set_timeout(self, timeout: int) -> None: ...

    def refresh(self) -> None: ...

    def pause(self) -> None: ...

    def connect(self, *, timeout: int = 300) -> EcwSandbox: ...

    def kill(self) -> None: ...


class EcwSandboxes(Protocol):
    def create(
        self,
        *,
        template: str,
        timeout: int,
        metadata: Mapping[str, str],
        envs: Mapping[str, str] | None = None,
        lifecycle: EcwLifecycle | None = None,
        allow_internet_access: bool | None = None,
        network: NetworkConfig | None = None,
    ) -> EcwSandbox: ...

    def attempt_create(
        self,
        *,
        template: str,
        timeout: int,
        metadata: Mapping[str, str],
        allow_internet_access: bool | None = None,
        network: NetworkConfig | None = None,
    ) -> EcwCreateAttempt: ...

    def kill_matching(
        self,
        metadata: Mapping[str, str],
        *,
        attempts: int = 10,
        interval_seconds: float = 1.0,
    ) -> None: ...

    def list_pages(
        self,
        metadata: Mapping[str, str],
        *,
        limit: int,
        max_pages: int,
    ) -> EcwSandboxPages: ...


class EcwScenarioSandboxes(Protocol):
    @property
    def items(self) -> tuple[EcwSandbox, ...]: ...

    def create(self, metadata: Mapping[str, str] | None = None) -> EcwSandbox: ...

    def attempt_create(
        self,
        *,
        allow_internet_access: bool | None = None,
        network: NetworkConfig | None = None,
    ) -> EcwCreateAttempt: ...

    def delete(self, sandbox: CommandSandbox) -> int: ...

    def list_pages(self, *, limit: int, max_pages: int) -> EcwSandboxPages: ...


class KindEnvironment(Protocol):
    def require_encrypted_volume_support(self) -> None: ...

    def acquire_cold_create_setup(self) -> Callable[[], None]: ...

    def prepare_cold_create(
        self, template: str, config: KindColdCreateConfig
    ) -> ColdCreateSelection: ...

    def bootstrap(self) -> None: ...

    def admin_request(
        self, method: str, resource: str, body: dict[str, object] | None = None,
        *, expected: tuple[int, ...] = (200, 201, 202, 204),
    ) -> Any: ...

    def seed_api_key(
        self, api_key: str, *,
        org_id: str = "00000000-0000-0000-0000-000000000001",
        user_id: str = "00000000-0000-0000-0000-000000000010",
    ) -> None: ...

    def delete_api_key(self, api_key: str) -> None: ...

    def reclaim_local_sandbox_everywhere(self, sandbox_id: str) -> None: ...


@dataclass(frozen=True)
class RetentionSweeperConfig:
    """The deployed retention sweeper's knobs, read from the cluster."""

    enabled: bool
    dry_run: bool
    keep_n: int
    tick_seconds: int
    min_age_seconds: int


@dataclass(frozen=True)
class SnapshotRecord:
    """One durable snapshot row, ordered context left to the caller."""

    snapshot_id: str
    artifact_id: str
    artifact_kind: str
    storage_uri: str
    storage_location_count: int


class ClusterRetentionControl(Protocol):
    """Privileged control-plane operations for retention scenarios.

    Deliberately named operations rather than a raw SQL surface: every
    privileged thing a scenario may do is enumerated here, so the white-box
    surface stays auditable and cannot drift into ad-hoc queries. Only the
    kind backend provides an implementation; requesting a fixture that
    carries this on a deployed backend is a configuration error.
    """

    def retention_sweeper_config(self) -> RetentionSweeperConfig: ...

    def acquire_retention_sweeper_mutation(self) -> Callable[[], None]:
        """Acquire exclusive ownership of shared controller sweeper settings.

        Returns a release callback. A competing smoke fails closed instead of
        changing the same deployment concurrently.
        """
        ...

    def retention_sweeper_env(self) -> dict[str, str | None]:
        """The deployment's standing values for the sweeper env allowlist.

        Absent variables map to None so a later restore removes them again.
        """
        ...

    def set_retention_sweeper_env(self, overrides: Mapping[str, str | None]) -> None:
        """Apply allowlisted sweeper env on the controller and await rollout.

        Keys outside the allowlist and values outside the closed
        true/false/seconds vocabulary are refused before kubectl runs; None
        removes the variable, returning it to the chart-rendered spec.
        """
        ...

    def restore_retention_sweeper_env(self, standing: Mapping[str, str | None]) -> None:
        """Restore values returned by ``retention_sweeper_env`` exactly."""
        ...

    def ready_full_snapshots(self, sandbox_id: str) -> list[SnapshotRecord]:
        """Oldest-first ready `full` snapshots for the sandbox."""
        ...

    def pin_current_artifact(self, sandbox_id: str, record: SnapshotRecord) -> bool:
        """Point the suspended sandbox's restore marker at `record`.

        Returns False when no suspended row matched (the sandbox moved).
        """
        ...

    def snapshot_exists(self, snapshot_id: str) -> bool: ...

    def snapshot_storage_exists(
        self, record: SnapshotRecord, *, timeout: float
    ) -> bool:
        """Whether the selected object-store prefix contains a key."""
        ...

    def delete_snapshot_storage(self, record: SnapshotRecord) -> None:
        """Fault injection: delete the selected object-store prefix.

        This simulates a claimer crash after durable bytes are gone but before
        catalog finalization.
        """
        ...

    def mark_snapshot_deleting(self, snapshot_id: str) -> bool:
        """Fault injection: durably claim the row exactly as `claim_prune`
        commits it (`ready` -> `deleting`), simulating a claimer crash.

        Returns False when no `ready` row matched (already claimed or gone).
        """
        ...

    def deleting_snapshots(self, sandbox_id: str) -> list[str]:
        """Snapshot ids parked in `state = 'deleting'` for the sandbox."""
        ...


@dataclass(frozen=True)
class SandboxPodPlacement:
    """Where a sandbox's execution pod is bound right now.

    `execution_generation` is the pod's `space.pplx.net/execution-generation`
    annotation — the same value sbx-manager stamps on that execution's
    `runtime_activated` event, so the two can be joined without a log-derived
    baseline.
    """

    sandbox_id: str
    pod_name: str
    pod_uid: str
    node_name: str
    execution_generation: str


@dataclass(frozen=True)
class RestoreActivation:
    """One `runtime_activated` sandbox event, read from sandbox-manager's log.

    `restore_provenance` is sbx-manager's own classification of where the
    restored bytes came from (`local_only`, `remote_matches_local`,
    `remote_only`, ...). `remote_*` with `restore_storage_uri_present` means the
    restore pulled the snapshot from object storage; `local_only` means it
    reused the source node's retained suspend artifact.
    """

    sandbox_id: str
    node_name: str
    boot_type: str
    restore_provenance: str
    restore_storage_uri_present: bool
    execution_generation: str


class NodeFaultControl(Protocol):
    """Kind-only scheduling faults against kube nodes.

    Named operations, not raw kubectl: each fault a scenario may inject is
    enumerated here so the white-box surface stays auditable. The kind backend
    is the only implementation; a deployed backend refuses fixtures that carry
    this.
    """

    def acquire_worker_cordon_mutation(self) -> Callable[[], None]:
        """Take exclusive ownership of worker-cordon changes on this cluster.

        Two concurrent smokes each cordoning a different worker on a small
        bench would starve restore placement; a competing smoke fails closed.
        Taking over an *expired* lease first uncordons whatever node its
        previous holder recorded, so a smoke killed mid-cordon cannot leave a
        worker unschedulable for the next run. Returns a release callback.
        """
        ...

    def cordon_kube_node(self, node_name: str) -> None:
        """Mark the kube node unschedulable. Requires the cordon mutation.

        Records the node on the mutation Lease before patching it so a later
        takeover can undo a cordon whose owner never got to.
        """
        ...

    def uncordon_kube_node(self, node_name: str) -> None:
        """Mark the kube node schedulable again and clear the Lease record.

        Idempotent: compensation may run this for a cordon that never reached
        the apiserver.
        """
        ...


@dataclass(frozen=True)
class AdminNodeView:
    """One SPACE node as `GET /admin/api/v1/nodes` lists it."""

    id: str
    k8s_node_name: str
    drain_phase: str | None
    drain_mode: str | None
    sandboxes_active: int | None


@dataclass(frozen=True)
class SandboxResidency:
    """`sandboxes.lifecycle` and `active_node_id`, the columns the drain reconciler reads."""

    lifecycle: str
    active_node_id: str | None


class NodeDrainControl(NodeFaultControl, Protocol):
    """Kind-only operations for the node-drain scenarios.

    The admin drain routes are the product surface under test; the kube
    reads and the drain-keep override are the harness's own. The worker
    cordon Lease from [`NodeFaultControl`] serializes drain smokes with the
    cross-node restore's cordon on a small bench.
    """

    def admin_nodes(self) -> list[AdminNodeView]: ...

    def drain_node_default_mode(self, node_id: str) -> None:
        """`POST /admin/api/v1/nodes/{id}:drain` with NO body; the absent mode is the assertion."""
        ...

    def drain_node_forced(self, node_id: str) -> None:
        """`POST /admin/api/v1/nodes/{id}:drain` with `{"mode": "forced"}`: evacuate the residents."""
        ...

    def undrain_node(self, node_id: str) -> None: ...

    def set_drain_keep(self, node_name: str, value: str) -> None:
        """Set the `space.pplx.net/drain-keep` operator override on the kube node."""
        ...

    def clear_drain_keep(self, node_name: str) -> None: ...

    def kube_node_unschedulable(self, node_name: str) -> bool: ...

    def kube_node_exists(self, node_name: str) -> bool: ...

    def sandbox_pod_placement(self, sandbox_id: str) -> SandboxPodPlacement | None: ...

    def sandbox_residency(self, sandbox_id: str) -> SandboxResidency: ...

    def ready_full_snapshots(self, sandbox_id: str) -> list[SnapshotRecord]: ...


class RestoreStateRead(Protocol):
    """Control-plane reads about where and whether a sandbox can restore."""

    def sandbox_pod_placement(self, sandbox_id: str) -> SandboxPodPlacement | None:
        """The sandbox's single bound execution pod, or None if absent/ambiguous."""
        ...

    def restore_portable(self, sandbox_id: str) -> bool:
        """Whether a user restore would select a durable remote snapshot.

        The same predicate the API's suspend-restore planner applies: the
        sandbox is suspended (or restoring), a `ready` full snapshot exists with
        a storage location, and it is newer than the last placement change.
        While this is false the planner has only a source node, and the pod is
        pre-bound with `spec.nodeName` — which bypasses kube-scheduler and so
        ignores any cordon.
        """
        ...


@dataclass(frozen=True)
class EncryptedSnapshotObservation:
    """One sealed encrypted memory volume observed on its source worker."""

    node_name: str
    luks_uuid: str
    logical_bytes: int
    allocated_bytes: int


class EncryptedResuspendControl(RestoreStateRead, Protocol):
    """Kind-only reads for the encrypted restore -> re-suspend regression."""

    def encrypted_snapshot_observation(
        self, sandbox_id: str, node_name: str
    ) -> EncryptedSnapshotObservation:
        """Read LUKS identity and sparse allocation from the sealed backing file."""
        ...

    def clear_completed_sandbox_pods(self, sandbox_id: str) -> tuple[str, ...]:
        """Force-clear only deleting pods whose containers all exited successfully."""
        ...


class SandboxEventRead(Protocol):
    """Read sbx-manager's structured sandbox events from its pods' logs."""

    def restore_activations(
        self,
        sandbox_id: str,
        *,
        node_names: Collection[str] | None = None,
        since_time: str | None = None,
    ) -> list[RestoreActivation]:
        """`runtime_activated` events for the sandbox.

        `node_names` restricts the read to specific workers' sandbox-managers;
        the default reads every worker. `since_time` is an RFC 3339 lower bound
        passed to the log read so a long-lived bench does not scan history.
        """
        ...


class CrossNodeRestoreControl(
    NodeFaultControl, RestoreStateRead, SandboxEventRead, Protocol
):
    """Everything the cross-node durable-restore scenario is allowed to do."""


@dataclass(frozen=True)
class ExecutionRow:
    """One cluster `sandbox_k8s_executions` row: the API's record of a pod intent."""

    generation: int
    state: str
    active: bool
    pod_name: str


@dataclass(frozen=True)
class SandboxPodView:
    """One sandbox execution pod as the apiserver shows it."""

    pod_name: str
    phase: str
    node_name: str
    execution_generation: str
    checkpoint: str
    restore_source_node: str
    restore_storage_uri: str
    deleting: bool


class ConcurrentRestoreControl(Protocol):
    """Privileged operations for the concurrent-restore contention scenario.

    The compose hammer forced its races through a stub apiserver (a pod POST
    delay, a forced first-POST failure, a POST counter). On a real cluster:
    the forced failure has a named, compensated, kind-only substitute (a
    namespace ResourceQuota pinned at the current pod count, so pod POSTs
    are rejected at admission); the counters are the API's own
    `sandbox_k8s_executions` rows plus the pods by label; and the same quota
    holds a restore open for the teardown fence, since callers keep
    re-claiming while POSTs are refused. Cordoning every sandbox worker is
    NOT a stand-in for the POST delay: the API's placement admission then
    answers 429 before any POST, so nothing is in flight.

    Only what the contention assertions call is declared here; placement,
    portability and event reads come from the cross-node control the
    scenario obtains separately.
    """

    def scale_public_api(self, replicas: int) -> int:
        """Set the kind public API Deployment's replica count; returns the previous count."""
        ...

    def wait_public_api_rollout(self, *, timeout: float) -> None:
        """Block until the public API Deployment's rollout is complete."""
        ...

    def pin_namespace_pod_count(self) -> int:
        """Create a ResourceQuota capping `pods` at the namespace's current usage.

        Returns the cap. Every pod POST in the namespace is then rejected at
        admission with 403 until the cap is lifted. Blast radius: the quota
        is namespaced to `space` and pins `pods` only, but every SPACE
        component lives in that namespace, so any pod that restarts during
        the seconds-long window is refused until the lift. The compensation
        (`lift_namespace_pod_cap`, `--ignore-not-found`) is armed before the
        apply.
        """
        ...

    def lift_namespace_pod_cap(self) -> None:
        """Delete the pod-count ResourceQuota; idempotent."""
        ...

    def sandbox_executions(self, sandbox_id: str) -> list[ExecutionRow]:
        """The sandbox's execution rows, oldest generation first."""
        ...

    def sandbox_pods(self, sandbox_id: str) -> list[SandboxPodView]:
        """The sandbox's execution pods by label, including terminating ones."""
        ...

    def delete_sandbox_pod(self, pod_name: str) -> None:
        """Delete one sandbox execution pod out from under the API (kind-only fault)."""
        ...


@dataclass(frozen=True)
class TemplateCacheGcConfig:
    """template-manager's LRU eviction knobs as rendered on the worker DaemonSet.

    Read once at daemon start (`GcConfig::from_env`); `enabled` is exactly
    "a positive budget is set". Interval and grace fall back to the binary's
    defaults (300 s and 1800 s) when the DaemonSet does not render them.
    """

    enabled: bool
    budget_bytes: int
    interval_seconds: int
    grace_seconds: int
    headroom_fraction: float
    # Prefetch's latest-N pins that many release digests per warmed template;
    # every pinned byte counts toward the budget but can never be reclaimed,
    # so the budget only means what the scenario thinks if this is small.
    prefetch_latest_n: int


@dataclass(frozen=True)
class TemplateGcTick:
    """One `template GC tick complete` log line from a worker's template-manager."""

    node_name: str
    candidates: int
    deleted: int
    bytes_before: int
    bytes_freed: int


class TemplateCacheControl(RestoreStateRead, Protocol):
    """Privileged on-node template-cache operations for the eviction scenario.

    Everything here acts on one worker node through named operations: the
    commands run in that node's operator shell, the knobs and log lines come
    from its template-manager pod. There is deliberately no SQLite surface: the
    worker image ships no `sqlite3`, and the point is to prove the loop through
    the same interfaces an operator has — the `space-tmplmgr` CLI and the
    daemon's own structured log line.
    """

    def template_cache_gc_config(self, node_name: str) -> TemplateCacheGcConfig:
        """The eviction knobs of the template-manager pod on that node.

        Read from the running pod, not the DaemonSet spec: the knobs are read
        once at daemon start, and mid-rollout the spec describes a pod that
        does not exist yet on this node.
        """
        ...

    def register_unpinned_template(
        self, node_name: str, template: str, size_bytes: int
    ) -> None:
        """Plant a throwaway build of `size_bytes` real bytes and register it.

        Its name is not in `SPACE_TEMPLATE_PREFETCH_TAGS`, so no pin protects
        it; nothing references it, so the active-sandbox set does not either.
        It is therefore exactly what the loop should reclaim once the node is
        over budget and the grace period has passed.
        """
        ...

    def delete_template(self, node_name: str, template: str) -> None:
        """Remove the throwaway build and its on-disk residue; idempotent."""
        ...

    def protected_template_builds(
        self, node_name: str, sandbox_id: str, template_selector: str
    ) -> tuple[str, ...]:
        """Full active-sandbox and selected-release digests, read before planting."""
        ...

    def template_build_state(
        self, node_name: str, template: str, content_hash: str
    ) -> str | None:
        """State of the `template` build with `content_hash`; None once it no longer resolves.

        `space-tmplmgr get` resolves the exact hash from the node's cache; a
        bare name would resolve through the cluster, which has no release for
        a build planted only on the node. Two side effects make it unfit for
        anything but a final probe
        of a build that is *supposed* to be gone: a successful resolution
        advances the build's LRU clock (`db::touch_build`), restarting its
        grace period; and a local miss falls through to an on-demand pull
        from object storage, so a template with a cluster release — the seed —
        answers `ready` whether or not its bytes were on the node. Never poll
        this while waiting for an eviction, and never use it to prove a
        template survived one.
        """
        ...

    def template_digest_dirs(self, node_name: str, template: str) -> tuple[str, ...]:
        """Names of the `sha256:*` digest directories under the template, from disk.

        Fails closed: an exec that could not run raises rather than reading as
        "no directories".
        """
        ...

    def template_gc_ticks(
        self, node_name: str, *, since_time: str
    ) -> list[TemplateGcTick]:
        """Completed eviction ticks logged by that worker since `since_time`."""
        ...


@dataclass(frozen=True)
class TemplateRecoveryVerdict:
    """One digest directory's fate in a template-manager startup recovery pass.

    `outcome` is one of `confirmed` (row present, declared files intact, no
    hashing), `registered` (no row; bootable artifact hashed to the dir name
    and a row inserted), `rejected` (manifest, size or hash disagreement;
    directory removed, no row) or `pruned` (row whose bytes were gone).
    `reason` is the daemon's own rejection text for `rejected`, else empty.
    """

    template: str
    content_hash: str
    outcome: str
    reason: str = ""


@dataclass(frozen=True)
class StartupRecoveryReport:
    """What one worker's template-manager logged for its startup recovery pass."""

    node_name: str
    pod_name: str
    finished: bool
    verdicts: tuple[TemplateRecoveryVerdict, ...]

    def for_build(self, template: str, content_hash: str) -> tuple[TemplateRecoveryVerdict, ...]:
        return tuple(
            v for v in self.verdicts if v.template == template and v.content_hash == content_hash
        )


class StartupRecoveryControl(RestoreStateRead, Protocol):
    """Privileged operations for template-manager's startup recovery decision table.

    Everything on-node runs through the same named operations as the eviction
    scenario (no SQLite: rows are dropped through `space-tmplmgr
    delete-template` after the digest directory is stashed aside). The one
    new privilege is restarting a worker's template-manager pod, which is what
    makes recovery run at all; it is kind-only by fixture type and by the
    adapter's own isolation guard, and the scenario points it at the
    sandbox-pool worker that is *not* running its sandbox.
    """

    def other_sandbox_worker_node(self, excluding_node: str) -> str:
        """A sandbox-pool kube node other than `excluding_node`, or a configuration error."""
        ...

    def ready_node_service_pod(self, node_name: str, component: str) -> str | None:
        """The single Ready pod of that worker node service on `node_name`, or None right now."""
        ...

    def register_unpinned_template(
        self, node_name: str, template: str, size_bytes: int
    ) -> None:
        """Plant a throwaway build of `size_bytes` real bytes and register it.

        Returns only once the daemon's background compression job has
        rewritten the manifest and re-upserted the row, so later mutations
        of the plant are not undone underneath the scenario.
        """
        ...

    def delete_template(self, node_name: str, template: str) -> None:
        """Remove the throwaway build, its on-disk residue and any stash; idempotent."""
        ...

    def template_build_state(
        self, node_name: str, template: str, content_hash: str
    ) -> str | None:
        """State of the `template` build with `content_hash`; None once it no longer resolves."""
        ...

    def template_digest_dirs(self, node_name: str, template: str) -> tuple[str, ...]:
        """Names of the `sha256:*` digest directories under the template, from disk."""
        ...

    def remove_template_digest_dir(self, node_name: str, template: str, digest: str) -> None:
        """Delete the digest directory, leaving the catalog row behind (stale row)."""
        ...

    def orphan_template_digest_dir(self, node_name: str, template: str, digest: str) -> None:
        """Drop the catalog row while keeping the digest directory's bytes intact.

        Stashes the directory beside the cache, runs the daemon's own
        `delete-template`, then puts the directory back under a fresh name
        directory — the on-disk state a crash between the file rename and
        the DB commit leaves behind.
        """
        ...

    def corrupt_template_rootfs(self, node_name: str, template: str, digest: str) -> None:
        """Overwrite the head of the bootable artifact in place; size unchanged, hash not."""
        ...

    def truncate_template_rootfs(
        self, node_name: str, template: str, digest: str, *, bytes_removed: int
    ) -> None:
        """Shorten the bootable artifact so its on-disk size disagrees with the manifest."""
        ...

    def restart_node_service(self, node_name: str, component: str, *, timeout: float) -> str:
        """Delete that node service's pod and wait for its Ready replacement on the same node.

        Returns the replacement's name. Only that service restarts; the node's
        sandboxes keep running under its host runtime-supervisor.
        """
        ...

    def startup_recovery_report(self, node_name: str) -> StartupRecoveryReport:
        """template-manager's startup recovery verdicts from its pod's log on that node.

        The pod's own log, a restarted container's previous attempt first, so
        the first recovery pass is read even if a later attempt ran another.
        """
        ...


@dataclass(frozen=True)
class BackupQueued:
    """One `artifact_backup_queued` sandbox event: the suspend's sealed facts.

    `rootfs_diff_format` is the codec the suspend actually used, as recorded
    in the sidecar sbx-manager published (`btrfs-send-v1`, or `full`).
    """

    node_name: str
    snapshot_id: str
    rootfs_diff_format: str
    object_uri: str


@dataclass(frozen=True)
class BtrfsSendCapture:
    """One btrfs-send capture, from sandbox-manager's encode line.

    `deferred` is true when the capture retained its source for a streamed
    upload instead of writing `rootfs.btrfs-send`; `stream_bytes` is then 0.
    """

    node_name: str
    sandbox_id: str
    stream_bytes: int
    deferred: bool


@dataclass(frozen=True)
class CompletedTransfer:
    """One volume-manager transfer job that completed, from its log line.

    `kind` is the job kind: `push_btrfs_send` streams a rootfs from a retained
    btrfs source, while a materialized send file uploads under another kind
    with the same `local_path`.
    """

    node_name: str
    kind: str
    local_path: str
    bytes: int


@dataclass(frozen=True)
class SnapshotRowFacts:
    """The immutable facts of one cluster `snapshots` row, in row order."""

    snapshot_id: str
    state: str
    timestamp: str
    size_bytes: int | None
    storage_uri: str


@dataclass(frozen=True)
class RestoreFailure:
    """One `snapshot_restore_failed` sandbox event from a worker's sbx-manager.

    `reason` is the stage that failed (`ensure_template`, `pull_rootfs`, ...),
    `error_kind` the bounded name sbx-manager gives the failure
    (`parent_template_lookup`, `btrfs_receive`, ...), `error` the daemon's own
    message for humans, `restore_codec` the codec it was restoring with. Match
    on `reason` and `error_kind`, never on `error`: its wording is free to change.
    """

    node_name: str
    operation: str
    reason: str
    error_kind: str
    error: str
    restore_codec: str


@dataclass(frozen=True)
class ReleaseStorageRow:
    """One `template_release_storage_locations` row, whole, held for a compensation.

    Every column is carried so the row goes back exactly as it was: same
    `id` (the encrypted-fields sidecar keys on it), same `org_id`, same
    `created_at` (the API orders locations by it).
    """

    row_id: str
    release_id: str
    org_id: str
    storage_uri: str
    created_at: str


@dataclass(frozen=True)
class TemplatePullLine:
    """One template-manager log line inside a pull span: its trigger and message."""

    trigger: str
    message: str


class SuspendCodecControl(CrossNodeRestoreControl, Protocol):
    """Privileged operations for the btrfs-send codec failure matrix.

    Builds on the cross-node control (cordon, placement, portability,
    activation events) with the reads and faults those scenarios need:
    the codec fact from sbx-manager's own event, the parent template's
    presence and subvolume identity on a worker, the daemon's failure lines,
    and two kind-only faults — withholding a template release's storage
    rows and corrupting one object of a durable snapshot. Every fault is a
    named operation with its compensation owned by the fixture.
    """

    def other_sandbox_worker_node(self, excluding_node: str) -> str:
        """A sandbox-pool kube node other than `excluding_node`, or a configuration error."""
        ...

    def btrfs_send_captures(
        self, sandbox_id: str, node_name: str, *, since_time: str
    ) -> list[BtrfsSendCapture]:
        """sandbox-manager's btrfs-send encode lines for the sandbox on `node_name` since `since_time`."""
        ...

    def sandbox_canonical_snapshot_entries(self, node_name: str, sandbox_id: str) -> tuple[str, ...]:
        """Entries of the sandbox's canonical suspend snapshot directory on `node_name`."""
        ...

    def completed_transfers(
        self, sandbox_id: str, node_name: str, *, since_time: str
    ) -> list[CompletedTransfer]:
        """volume-manager's completed transfer jobs under the sandbox's cache directory on `node_name` since `since_time`."""
        ...

    def backup_queued_events(self, sandbox_id: str, *, since_time: str) -> list[BackupQueued]:
        """`artifact_backup_queued` events for the sandbox since `since_time`."""
        ...

    def suspend_snapshot_template_hash(self, sandbox_id: str) -> str:
        """The parent build's digest recorded on the sandbox's newest ready full snapshot row."""
        ...

    def template_digest_dirs(self, node_name: str, template: str) -> tuple[str, ...]:
        """Names of the `sha256:*` digest directories under the template, from disk."""
        ...

    def evict_template_from_worker(self, node_name: str, template: str) -> list[str]:
        """Remove a template's builds from one worker through `delete-template`.

        A cache-miss forcing device: the next restore of a btrfs-send
        snapshot on that worker must materialize the parent again. Returns
        the ids of local sandboxes still referencing the template when the
        daemon refuses for that reason (empty on success); anything else
        raises. The refusal previews at most three builds and five ids
        each, so a caller reclaiming and retrying clears at most that many
        per round.
        """
        ...

    def reclaim_cluster_deleted_local_sandbox(self, node_name: str, sandbox_id: str) -> None:
        """Remove one worker's local row for a sandbox the cluster no longer has.

        Refuses (configuration error) when the cluster row still exists or a
        warm-pool slot reserves the id (a pool clone has a local row before
        any `sandboxes` row): only provably dead local state — the retained suspend bytes or the
        `restoring` row of a sandbox already deleted — may be reclaimed early.
        The cluster-deleted eviction would do the same for `suspended` rows
        after its quiescence window; this is that, now, through the
        daemon's own `space-sbxmgr delete`.
        """
        ...

    def reclaim_local_sandbox_everywhere(self, sandbox_id: str) -> None:
        """Remove a just-deleted sandbox's local rows from every sandbox-pool worker.

        The owner's own cleanup, so no row it left can outlive it: the
        retained suspend bytes on its source and the `restoring` row a failed
        cross-node restore keeps on the target, both of which the
        cluster-deleted eviction reclaims after its quiescence window. Waits a
        bounded time for the cluster row and then every unfinished pod of
        the sandbox to go (a restore pod's retries re-create the row until it
        does; a `Succeeded` or `Failed` pod is never retried and is ignored).
        Then, on its own budget, deletes with `--blocking` on every worker,
        retrying only the refusals that settle on their own: a quarantine
        recovery's "retry cleanup" deferral and node-manager's claim on a
        sandbox another host mutation or reset still holds. Any other refusal,
        including a row whose suspend publication can never land, is
        collected and raised once every worker has been attempted.
        """
        ...

    def template_pulls(
        self, node_name: str, template: str, *, since_time: str
    ) -> list[TemplatePullLine]:
        """template-manager lines inside pull spans for the template since `since_time`."""
        ...

    def template_subvolume_received_uuid(
        self, node_name: str, template: str, digest: str
    ) -> str | None:
        """The digest dir's btrfs `Received UUID`; None when it is not a subvolume."""
        ...

    def sandbox_instance_entries(self, node_name: str, sandbox_id: str) -> tuple[str, ...]:
        """`instance*` entries under the sandbox's cache dir on that worker; fails closed."""
        ...

    def sandbox_instance_is_subvolume(self, node_name: str, sandbox_id: str) -> bool:
        """Whether the sandbox's `instance` on that worker is a btrfs subvolume."""
        ...

    def restore_failures(self, sandbox_id: str, *, since_time: str) -> list[RestoreFailure]:
        """`snapshot_restore_failed` events for the sandbox since `since_time`."""
        ...

    def ready_full_snapshots(self, sandbox_id: str) -> list[SnapshotRecord]:
        """The sandbox's `ready` full snapshot rows with their storage locations."""
        ...

    def snapshot_row_facts(self, sandbox_id: str) -> list[SnapshotRowFacts]:
        """Every full `snapshots` row of the sandbox, oldest first."""
        ...

    def withhold_template_release_storage(self, template: str) -> list[ReleaseStorageRow]:
        """Delete the template's release storage rows; returns them for restoration."""
        ...

    def restore_template_release_storage(self, rows: list[ReleaseStorageRow]) -> None:
        """Put withheld release storage rows back; idempotent."""
        ...

    def corrupt_snapshot_object(
        self, record: SnapshotRecord, *, object_name: str, offset: int
    ) -> None:
        """Flip one byte of one object under the snapshot's storage prefix."""
        ...


@dataclass(frozen=True)
class ColdEvictionConfig:
    """sandbox-manager's cold-sandbox eviction knobs as rendered on a worker pod.

    Mirrors `EvictionConfig::from_env`: `enabled` is the literal `1`/`true`;
    `engage_fraction == 0` is the documented always-on gate; the minimum idle
    is honored below the 25 h recommended floor (the daemon warns, nothing
    clamps).
    """

    enabled: bool
    engage_fraction: float
    target_fraction: float
    min_idle_seconds: int
    interval_seconds: int


@dataclass(frozen=True)
class LocalEviction:
    """One `local_eviction_completed` sandbox event from a worker's sbx-manager."""

    sandbox_id: str
    node_name: str
    reason: str
    snapshot_id: str


class ColdEvictionControl(RestoreStateRead, SandboxEventRead, Protocol):
    """Privileged reads for the cold-sandbox eviction scenario.

    Nothing here mutates anything: the loop's knobs are chart values read
    once at daemon start (see `sandboxEviction` in the kind sandbox-manager
    chart), and the scenario's only forcing device is time. The reads are the
    knobs on the very pod that will do the evicting, the on-disk cache
    directory, and sbx-manager's own eviction event.
    """

    def sandbox_pods(self, sandbox_id: str) -> list[SandboxPodView]:
        """Execution pods, including terminal predecessors retained by Kubernetes."""
        ...

    def cold_eviction_config(self, node_name: str) -> ColdEvictionConfig:
        """The eviction knobs of the sandbox-manager pod on the sandbox's worker."""
        ...

    def sandbox_cache_dir_present(self, node_name: str, sandbox_id: str) -> bool:
        """Whether `/space/cache/firecracker/sandboxes/<id>` exists on that node.

        Fails closed: an exec that could not run raises rather than reading
        as "gone".
        """
        ...

    def local_evictions(
        self,
        sandbox_id: str,
        *,
        node_names: Collection[str] | None = None,
        since_time: str | None = None,
    ) -> list[LocalEviction]:
        """`local_eviction_completed` events for the sandbox, from sandbox-manager logs."""
        ...

    def eviction_loop_log(self, node_name: str, *, since_time: str) -> list[str]:
        """sbx-manager's own eviction tick/skip lines on that worker, for diagnosis.

        A candidate the loop could not prove is backed off for 30 minutes —
        longer than the scenario's wait — so a timeout without these lines
        reads as "did not evict" with no cause.
        """
        ...


@dataclass(frozen=True)
class SnapshotReaperConfig:
    """sandbox-manager's snapshot artifact reaper knobs as rendered on a worker pod.

    Mirrors `GcConfig::from_env` in `snapshot_gc.rs`: `enabled` is the literal
    `1`/`true`; `engage_fraction == 0` is the documented always-on gate.
    """

    enabled: bool
    engage_fraction: float
    interval_seconds: int
    grace_seconds: int
    idle_seconds: int


@dataclass(frozen=True)
class ReapedArtifact:
    """One `snapshot reaper: deleted cache artifact via node-mgr` line."""

    node_name: str
    local_artifact_id: str
    staging_path: str


class SnapshotReaperControl(RestoreStateRead, Protocol):
    """Privileged reads for the snapshot reaper scenario. Nothing here mutates.

    The knobs are chart values read once at daemon start; the forcing device
    is a public fork plus time. The reads are the knobs on the worker the
    source sandbox lives on, its on-disk snapshots directory, the reaper's own
    per-candidate log line, and the cluster's record that the reaped artifact
    had a ready remote copy.
    """

    def snapshot_reaper_config(self, node_name: str) -> SnapshotReaperConfig: ...

    def sandbox_snapshot_dirs(self, node_name: str, sandbox_id: str) -> tuple[str, ...]:
        """Entry names under `<cache>/<sandbox>/snapshots/` on that worker.

        Fails closed: an exec that could not run raises rather than reading
        as "none".
        """
        ...

    def reaped_artifacts(
        self, node_name: str, sandbox_id: str, *, since_time: str
    ) -> list[ReapedArtifact]:
        """The reaper's per-candidate deletion lines for artifacts under this
        sandbox's snapshots directory on that worker, since `since_time`.

        The adapter owns the staging-path convention and the canonical id
        form; orphan-sweep lines (no artifact id) are excluded.
        """
        ...

    def artifact_backed(self, sandbox_id: str, local_artifact_id: str) -> bool:
        """The cluster holds a `ready` snapshot row for exactly this local artifact
        with a storage location — the reaper's "backup completed" precondition,
        read from the control plane after the fact."""
        ...

    def reaper_loop_log(self, node_name: str, *, since_time: str) -> list[str]:
        """The reaper's tick/skip lines on that worker, for a timeout's evidence."""
        ...


class TelemetryBackendControl(Protocol):
    """Kind-only reads of the local backends `otel` mode installs.

    Prometheus (`space-metrics`), Tempo (`space-traces`) and Loki
    (`space-logs`) are read through their own HTTP query APIs, on the localhost
    ports both bring-up paths forward to them.
    """

    def telemetry_query(self, backend: str, path: str, params: Mapping[str, str]) -> Any:
        """The decoded JSON body of `GET <path>?<params>` on `prometheus`, `tempo` or `loki`."""
        ...
