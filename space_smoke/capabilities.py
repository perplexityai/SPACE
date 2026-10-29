"""Reusable contracts between smoke scenarios and environment adapters."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
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
    # The native API can report a recently exited process, with its exit.
    state: str = "running"
    exit_status: str | None = None
    signal: int | None = None


class ProcessSandbox(CommandSandbox, Protocol):
    """Protocol-neutral background process control used by the shared smoke."""

    @property
    def reports_exited_processes(self) -> bool:
        """Whether the smoke requires `get_process` to report a recently exited process."""
        ...

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
    end_at: datetime | None = None
    """The public `end_at`: the persisted timeout policy as an expiry from the
    sandbox's start. It is not the node's live deadline."""


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
    `node_local_backup_pending` queue, the sandbox's snapshot rows, the
    backed-artifact predicate and the suspend operation's state.
    space-internal-api's census publication log is failure evidence only.
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

    def sandbox_pod_placement(self, sandbox_id: str) -> SandboxPodPlacement | None: ...

    def publish_node_ctl_facet(self, node_name: str, facet: str) -> str | None:
        """Have that node's node-ctl publish `facet` now instead of on its next poll.

        None once node-ctl has the request; otherwise why a transient failure
        kept it from node-ctl, for the caller to record and retry.
        """
        ...


class TeardownRestartControl(PendingBackupControl, Protocol):
    """Kind-only operations for restarting sandbox-manager under an in-flight teardown suspend.

    Uploads are held with the pending-backup scenario's object store fault, so
    the suspend's teardown stays `suspending` on its node while the restart
    runs. The observable is the node's own sandbox row, read through the
    product's `space-sbxmgr list`.
    """

    def restart_node_service(self, node_name: str, component: str, *, timeout: float) -> str:
        """Delete that node service's pod and wait for its Ready replacement on the same node."""
        ...

    def node_sandbox_state(self, node_name: str, sandbox_id: str) -> str | None:
        """The sandbox's row state in that node's sandbox-manager, or None without a row."""
        ...


@dataclass(frozen=True)
class ReconcileQueueRef:
    """One `node_reconcile_queue` row: its node, facet and whether it is a dead letter."""

    node_id: str
    facet: str
    dead: bool


class ReconcileQueueGcControl(Protocol):
    """Kind-only operations for the reconcile-queue GC scenario.

    The GC prunes only rows a day old, a retention no public path can
    produce, so the scenario seeds synthetic `managed_by='k8s'` node rows
    with backdated queue refs (the stack's Postgres, as the retention
    fixtures write it) and reads the queue back. Every seeded `node_k8s` row
    carries the scenario's marker as its `k8s_node_uid`, which no real Node
    has, and the deletion selects on it. The controller Deployment's env is
    read, never changed; the fake EC2 endpoint's log is failure evidence
    only.
    """

    def reconcile_queue_gc_env(self) -> dict[str, str | None]:
        """The literal GC switch and EC2 endpoint on the controller Deployment, None if unset."""
        ...

    def unregistered_ready_node(self) -> str:
        """A Ready Kubernetes Node no SPACE node row names; the scenario's live node."""
        ...

    def node_exists(self, name: str) -> bool: ...

    def seed_reconcile_queue_gc_nodes(
        self,
        *,
        live: tuple[str, str],
        gone: tuple[str, str],
        marker: str,
    ) -> None:
        """Insert two (node id, Kubernetes name) rows, non-ready, with their backdated refs.

        Each gets a 25 h-old live `node-status` ref; the live node also gets a
        25 h-old and a 1 h-old dead letter. One transaction.
        """
        ...

    def reconcile_queue_refs(self, node_ids: Sequence[str]) -> list[ReconcileQueueRef]: ...

    def delete_seeded_reconcile_queue_nodes(self, node_ids: Sequence[str], *, marker: str) -> int:
        """Delete the marked node rows (their refs cascade); returns how many remain."""
        ...

    def fake_ec2_log_tail(self) -> str:
        """The fake EC2 endpoint's recent requests. Failure evidence only."""
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
    observables are the pods themselves, the sandbox's execution rows and
    the controller Deployment's env; the controller's structured log lines
    are failure evidence only.
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
        """GC-related structured records from every live controller pod since `since_time`.

        Failure evidence only: no assertion reads it.
        """
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
class NodeTemplatePresence:
    """The `space.pplx.net/template-build-*` metadata on one Node.

    node-ctl keeps one label per ready local template build and a same-key
    annotation carrying the build's full content hash.
    """

    labels: frozenset[str]
    annotations: Mapping[str, str]


@dataclass(frozen=True)
class NodeCtlFacetRun:
    """One node-ctl inbound facet's latest run, from `space-nodectl facet status --json`.

    Times are unix seconds, 0 before the first run. node-ctl runs one work
    item at a time, so `running` false with `last_completed_at` no earlier
    than `last_started_at` means the latest started run has finished, and
    `last_success_at` equal to `last_completed_at` that it succeeded.
    """

    last_started_at: int
    last_completed_at: int
    last_success_at: int
    running: bool
    last_error: str


class UnansweredTemplateEvictionControl(Protocol):
    """Kind-only operations for node-ctl's unanswered-template cleanup scenario.

    A worker's node-ctl waits an hour before deleting a local template the
    cluster released, and reads that wait once at start, so the scenario
    overrides one env key on the worker node-ctl DaemonSet
    (`SPACE_NODECTL_UNANSWERED_TEMPLATE_EVICT_AFTER_SECS`, to a bounded number
    of seconds) under its own mutation Lease and puts the captured literal
    back. The observables are what an operator reads on the
    worker: `space-tmplmgr list`, the template's digest directories, and the
    Node's template-build labels and annotations. node-ctl's log is failure
    evidence only. Instead of waiting for node-ctl's poll, the scenario runs
    the template desired-state facet on demand and reads the facet's status
    to know the run finished.
    """

    def acquire_node_ctl_mutation(self) -> Callable[[], None]:
        """Take the cluster-visible worker node-ctl DaemonSet Lease; returns its release."""
        ...

    def unanswered_template_evict_env(self) -> str | None:
        """The key's literal on the worker node-ctl DaemonSet, None if unset."""
        ...

    def set_unanswered_template_evict_after(self, seconds: int) -> None:
        """Set the wait and await the rollout; refuses a value outside 10..600 s."""
        ...

    def restore_unanswered_template_evict_env(self, standing: str | None) -> None:
        """Put the captured literal back (None unsets) and await the rollout."""
        ...

    def running_unanswered_template_evict_env(self, node_name: str) -> str | None:
        """The key's literal on the node-ctl pod running on that worker, None if unset."""
        ...

    def other_sandbox_worker_node(self, excluding_node: str) -> str:
        """A sandbox-pool kube node other than `excluding_node`, or a configuration error."""
        ...

    def local_template_names(self, node_name: str) -> frozenset[str]:
        """The template names `space-tmplmgr list` reports on that worker."""
        ...

    def template_digest_dirs(self, node_name: str, template: str) -> tuple[str, ...]:
        """Names of the `sha256:*` digest directories under the template, from disk."""
        ...

    def node_template_presence(self, node_name: str) -> NodeTemplatePresence: ...

    def reconcile_node_ctl_facet(self, node_name: str, facet: str) -> None:
        """Have that node's node-ctl run inbound `facet` at full scope now (`space-nodectl reconcile`).

        node-ctl's `RefreshFacetState` with no element: the run a poll makes
        for that facet, followed by an outbound sync. It only enqueues the
        run; read `node_ctl_facet_run` to see it finish. Raises if node-ctl
        did not take the request.
        """
        ...

    def node_ctl_facet_run(self, node_name: str, facet: str) -> NodeCtlFacetRun:
        """That node's node-ctl status for inbound `facet`'s latest run."""
        ...

    def node_ctl_log(self, node_name: str, *, since_time: str) -> str:
        """That worker's node-ctl log since `since_time`. Failure evidence only."""
        ...


@dataclass(frozen=True)
class TemplateRecipe:
    """A Dockerfile-plus-SDK template definition, rendered by the SDK adapter."""

    dockerfile: str
    files: Mapping[str, str]
    """Context files beside the Dockerfile, by relative name."""
    envs: Mapping[str, str]
    run_cmds: tuple[tuple[str, str], ...]
    """`(command, user)` pairs appended after the Dockerfile."""
    start_cmd: str | None
    """None builds without a start command (and so without a ready wait)."""
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

    def build_status(self, template_id: str, build_id: str) -> str:
        """The public build status the SDK polls: `waiting`, `building`, `ready` or `error`."""
        ...

    def create(
        self,
        *,
        template: str,
        timeout: int,
        metadata: Mapping[str, str],
    ) -> EcwSandbox: ...


@dataclass(frozen=True)
class TemplateBuildJob:
    """The API's record of an accepted build: `template_build_jobs` for the alias."""

    daemon_job_id: str
    builder_node_id: str | None
    builder_node: str | None
    """The accepting builder's Kubernetes node, through `node_k8s`; `None` when unpinned."""
    build_id: str = ""
    """The public build id the SDK polls the status of."""


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
    observable. A builder is named by its node: the API's build job row names
    the builder that accepted the build, and artefact reads run in that node's
    operator shell.
    """

    def builder_nodes(self) -> list[str]:
        """Builder nodes with a Ready node-api, stable order."""
        ...

    def route_builder_service(self, route: str, node_name: str, *, timeout: float = 30) -> None:
        """Label the builder's node-api pod and point the Service selector at it; wait until it is the only endpoint."""
        ...

    def clear_builder_service_route(self) -> None: ...

    def template_build_job(self, alias: str) -> TemplateBuildJob | None:
        """The newest accepted build job for the alias; `None` until the API records one."""
        ...

    def prepared_rootfs_handoff(self, node_name: str, *, window_seconds: int = 60) -> str | None:
        """None once the builder committed a prepared rootfs for its build; else, after
        `window_seconds` without one, the failure text."""
        ...

    def build_sandbox_started(self, node_name: str) -> bool:
        """Whether the builder still runs the build's own sandbox: the `builder@local`
        sandbox holding the committed prepared rootfs, listed as not yet run or running.

        A probe that could not list the sandboxes raises instead."""
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
        """Non-pool, non-terminating pods in the sandbox namespace."""
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

    def publish_node_ctl_facet(self, node_name: str, facet: str) -> str | None:
        """Have that node's node-ctl publish `facet` now instead of on its next poll.

        None once node-ctl has the request; otherwise why a transient failure
        kept it from node-ctl, for the caller to record and retry.
        """
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

    def fork_response(
        self, *, timeout: int, idempotency_key: str | None = None
    ) -> Mapping[str, Any]:
        """The public fork result, including child data-plane credentials."""
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
    def attach_fork(self, response: Mapping[str, Any]) -> EcwSandbox:
        """Construct a client from a fork response without waking the child."""
        ...

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


@dataclass(frozen=True)
class EcwForkResult:
    """A tracked child and the complete public reply used to construct its client."""

    sandbox: EcwSandbox
    response: Mapping[str, Any] = field(repr=False)


class EcwScenarioSandboxes(Protocol):
    @property
    def items(self) -> tuple[EcwSandbox, ...]: ...

    def create(self, metadata: Mapping[str, str] | None = None) -> EcwSandbox: ...

    def fork(
        self,
        source: EcwSandbox,
        *,
        idempotency_key: str | None = None,
        timeout: int | None = None,
    ) -> EcwForkResult: ...

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

    def bootstrap(self) -> bool: ...

    def telemetry_mode(self) -> str: ...

    def rust_build(self) -> str: ...

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

    def running_retention_sweeper_config(self) -> RetentionSweeperConfig | None:
        """The knobs the running controller pods started with; `None` unless they all agree."""
        ...

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
    annotation — the generation of the `sandbox_k8s_executions` row the pod
    runs, so the two can be joined.
    """

    sandbox_id: str
    pod_name: str
    pod_uid: str
    node_name: str
    execution_generation: str


@dataclass(frozen=True)
class RestoreActivation:
    """One `runtime_activated` sandbox event: a documented contract
    (`crates/libs/events/README.md`).

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

    def sandbox_pods(self, sandbox_id: str) -> list[SandboxPodView]:
        """The sandbox's execution pods by label, including terminating ones."""
        ...


class RestorePodRead(Protocol):
    """The landed execution of a restore, read from the apiserver and the cluster rows."""

    def restore_pod_document(self, pod_name: str) -> dict[str, Any] | None:
        """The apiserver's JSON for one sandbox pod, None when it does not exist."""
        ...

    def sandbox_executions(self, sandbox_id: str) -> list[ExecutionRow]:
        """The sandbox's `sandbox_k8s_executions` rows."""
        ...

    def sandbox_manager_log(
        self,
        sandbox_id: str,
        *,
        node_names: Collection[str] | None = None,
        since_time: str,
    ) -> list[str]:
        """The worker sandbox-managers' recent lines naming the sandbox.

        Failure evidence only: no assertion reads it. `node_names` restricts
        the read to specific workers; the default reads every worker. Storage
        URIs are redacted.
        """
        ...

    def restore_activations(
        self,
        sandbox_id: str,
        *,
        node_names: Collection[str] | None = None,
        since_time: str | None = None,
    ) -> list[RestoreActivation]:
        """`runtime_activated` events for the sandbox, from worker sandbox-manager logs.

        `since_time` is an RFC 3339 lower bound passed to the log read.
        """
        ...


class CrossNodeRestoreControl(
    NodeFaultControl, RestoreStateRead, RestorePodRead, Protocol
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
        """Set the kind public API Deployment's replica count; returns the previous count.

        An autoscaler on the Deployment is pinned to `replicas` (both bounds)
        so it cannot reconcile the count away; its first captured bounds are
        restored by `restore_public_api_autoscaler`.
        """
        ...

    def restore_public_api_autoscaler(self) -> None:
        """Put back the autoscaler bounds `scale_public_api` pinned; a no-op when none was."""
        ...

    def wait_public_api_rollout(self, *, timeout: float) -> None:
        """Block until the public API Deployment's rollout is complete."""
        ...

    def wait_public_api_pods_drained(self, *, timeout: float) -> None:
        """Block until only the desired public API pods exist and none is Terminating."""
        ...

    def wait_public_api_ready(self, *, timeout: float) -> None:
        """Block until the public API answers `/readyz` on the smoke's API URL."""
        ...

    def pin_namespace_pod_count(self) -> int:
        """Create a ResourceQuota capping `pods` at the namespace's current usage.

        Returns the cap. Every pod POST in the namespace is then rejected at
        admission with 403 until the cap is lifted. Blast radius: the quota
        is namespaced to the sandbox namespace and pins `pods` only, so any
        sandbox or warm-pool pod created during the seconds-long window is
        refused until the lift; the control plane in `space` is untouched.
        The compensation (`lift_namespace_pod_cap`, `--ignore-not-found`) is
        armed before the apply.
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


class TemplateCacheControl(RestoreStateRead, Protocol):
    """Privileged on-node template-cache operations for the eviction scenario.

    Everything here acts on one worker node through named operations: the
    commands run in that node's operator shell, the knobs and log lines come
    from its template-manager pod. There is deliberately no SQLite surface: the
    worker image ships no `sqlite3`, and the point is to prove the loop through
    the same interfaces an operator has — the `space-tmplmgr` CLI and the
    digest directories on disk. The daemon's log is failure evidence only.
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

    def template_gc_log(self, node_name: str, *, since_time: str) -> list[str]:
        """template-manager's own GC lines on that worker, for a timeout's evidence."""
        ...


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
        """State of the `template` build with `content_hash`; None once it no longer resolves.

        A row whose digest directory is gone still answers its own state:
        with no manifest left there is nothing to check the rootfs against,
        and the answer comes from the row alone, without a pull, so it is
        safe to poll on a build whose bytes were removed.
        """
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

    def startup_recovery_log(self, node_name: str) -> list[str]:
        """template-manager's startup-recovery lines from its pod's log on that node.

        Failure evidence only: no assertion reads it. A restarted container's
        previous attempt comes first.
        """
        ...


@dataclass(frozen=True)
class TransferJobView:
    """One volume-manager transfer job, as `space-volmgr job` reports it.

    `kind` is the job kind: `push_btrfs_send` streams a rootfs from a retained
    btrfs source, while a materialized send file uploads under another kind
    with the same `local_path`. `error` is the persisted message of the last
    failed attempt, cleared when the job completes; `attempts` counts the
    executions the job started, retries included, and is never reset.
    """

    job_id: str
    kind: str
    status: str
    local_path: str
    error: str = ""
    attempts: int = 0


@dataclass(frozen=True)
class RestoreFailure:
    """One `snapshot_restore_failed` sandbox event: a documented contract
    (`crates/libs/events/README.md`).

    `reason` is the stage that failed (`ensure_template`, `pull_rootfs`, ...),
    `error_kind` the bounded name sbx-manager gives the failure
    (`parent_template_lookup`, `btrfs_receive`, ...), `error` the daemon's own
    message for humans (storage URIs redacted), `restore_codec` the codec it
    was restoring with. Match on `reason` and `error_kind`, never on `error`:
    its wording is free to change.
    """

    node_name: str
    operation: str
    reason: str
    error_kind: str
    error: str
    restore_codec: str


@dataclass(frozen=True)
class SnapshotRowFacts:
    """The immutable facts of one cluster `snapshots` row, in row order."""

    snapshot_id: str
    state: str
    timestamp: str
    size_bytes: int | None
    storage_uri: str


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


class SuspendCodecControl(CrossNodeRestoreControl, Protocol):
    """Privileged operations for the btrfs-send codec failure matrix.

    Builds on the cross-node control (cordon, placement, portability, the
    landed restore pod) with the reads and faults those scenarios need: the
    snapshot rows and the objects under a snapshot's storage prefix (the
    codec fact), volume-manager's transfer jobs, sbx-manager's
    `snapshot_restore_failed` events, template-manager's pull metric, the
    parent template's presence and subvolume identity on a worker, and three
    kind-only faults — withholding a template release's storage rows,
    corrupting one object of a durable snapshot, and stopping node-manager
    while its btrfs send blocks on a full pipe. Every fault is a named
    operation with its compensation owned by the fixture.
    """

    def other_sandbox_worker_node(self, excluding_node: str) -> str:
        """A sandbox-pool kube node other than `excluding_node`, or a configuration error."""
        ...

    def sandbox_canonical_snapshot_entries(self, node_name: str, sandbox_id: str) -> tuple[str, ...]:
        """Entries of the sandbox's canonical suspend snapshot directory on `node_name`."""
        ...

    def metric_export_interval_seconds(self, node_name: str, component: str) -> float:
        """The OTLP metric export interval `component`'s pod on `node_name` runs with."""
        ...

    def transfer_jobs(self, node_name: str, sandbox_id: str) -> list[TransferJobView]:
        """The sandbox's volume-manager transfer jobs on `node_name`.

        `space-volmgr jobs --subject sandbox:<id>` in the node's operator
        shell, then `space-volmgr job <id>` for each, so every field comes
        from the daemon's own job record.
        """
        ...

    def transfer_job_log(self, node_name: str, *, since_time: str) -> list[str]:
        """volume-manager's transfer-job lines on `node_name` since `since_time`.

        Failure evidence only: no assertion reads it. Storage URIs are redacted.
        """
        ...

    def interrupt_btrfs_send(
        self, node_name: str, *, token: str, pulses: int, window_seconds: int
    ) -> list[str]:
        """Kind-only fault: stop and continue node-manager while its send blocks.

        Blocks for up to `window_seconds`. Each time a btrfs send is in flight
        on the worker, at most `pulses` times, volume-manager is stopped so
        the send pipe fills, then node-manager is stopped and continued, then
        volume-manager is continued. Ends early once
        `end_btrfs_send_interrupt` is called with the same `token`. Returns
        the interrupter's output lines (`pulse <n>` per pulse, then
        `pulses <n>`).
        """
        ...

    def end_btrfs_send_interrupt(self, node_name: str, *, token: str) -> None:
        """Stop the interrupter and continue node-manager and volume-manager; idempotent."""
        ...

    def restore_failures(self, sandbox_id: str, *, since_time: str) -> list[RestoreFailure]:
        """`snapshot_restore_failed` events for the sandbox since `since_time`."""
        ...

    def telemetry_mode(self) -> str:
        """The Kind telemetry mode (`datadog`, `otel` or `off`) the bench runs."""
        ...

    def telemetry_query(self, backend: str, path: str, params: Mapping[str, str]) -> Any:
        """The decoded JSON body of `GET <path>?<params>` on a local telemetry backend."""
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

    def ready_full_snapshots(self, sandbox_id: str) -> list[SnapshotRecord]:
        """The sandbox's `ready` full snapshot rows with their storage locations."""
        ...

    def snapshot_row_facts(self, sandbox_id: str) -> list[SnapshotRowFacts]:
        """Every full `snapshots` row of the sandbox, oldest first."""
        ...

    def snapshot_object_names(self, snapshot: SnapshotRowFacts) -> tuple[str, ...]:
        """Names of the objects directly under the snapshot's storage prefix."""
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


class ColdEvictionControl(RestoreStateRead, RestorePodRead, Protocol):
    """Privileged reads for the cold-sandbox eviction scenario.

    Nothing here mutates anything: the loop's knobs are chart values read
    once at daemon start (see `sandboxEviction` in the kind sandbox-manager
    chart), and the scenario's only forcing device is time. The reads are the
    knobs on the very pod that will do the evicting, the on-disk cache
    directory, and the landed pod and execution rows of the wake.
    """

    def sandbox_pods(self, sandbox_id: str) -> list[SandboxPodView]:
        """Execution pods, including terminal predecessors retained by Kubernetes."""
        ...

    def cold_eviction_config(self, node_name: str) -> ColdEvictionConfig:
        """The eviction knobs of the sandbox-manager pod on the sandbox's worker."""
        ...

    def cluster_deleted_quiescence_seconds(self, node_name: str) -> int:
        """How long that worker's sandbox-manager refuses to evict a cluster-deleted
        row after the row last moved (`SPACE_SBX_CLUSTER_DELETED_QUIESCENCE_SECS`)."""
        ...

    def sandbox_cache_dir_present(self, node_name: str, sandbox_id: str) -> bool:
        """Whether `/space/cache/firecracker/sandboxes/<id>` exists on that node.

        Fails closed: an exec that could not run raises rather than reading
        as "gone".
        """
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


class SnapshotReaperControl(RestoreStateRead, Protocol):
    """Privileged reads for the snapshot reaper scenario. Nothing here mutates.

    The knobs are chart values read once at daemon start; the forcing device
    is a public fork plus time. The reads are the knobs on the worker the
    source sandbox lives on, its on-disk snapshots directory, and the
    cluster's record of which of its local artifacts have a ready remote copy.
    """

    def snapshot_reaper_config(self, node_name: str) -> SnapshotReaperConfig: ...

    def sandbox_snapshot_dirs(self, node_name: str, sandbox_id: str) -> tuple[str, ...]:
        """Entry names under `<cache>/<sandbox>/snapshots/` on that worker.

        Fails closed: an exec that could not run raises rather than reading
        as "none".
        """
        ...

    def backed_local_artifacts(self, sandbox_id: str) -> tuple[str, ...]:
        """Local artifact ids of the sandbox's `ready` snapshot rows with a live storage location."""
        ...

    def artifact_backed(self, sandbox_id: str, local_artifact_id: str) -> bool:
        """The cluster holds a `ready` snapshot row for exactly this local artifact
        with a storage location — the reaper's "backup completed" precondition."""
        ...

    def reaper_loop_log(self, node_name: str, *, since_time: str) -> list[str]:
        """The reaper's tick/skip lines on that worker, for a timeout's evidence."""
        ...


class TelemetryBackendControl(Protocol):
    """Kind-only reads of the local backends `otel` mode installs.

    Prometheus (`space-metrics`), Tempo (`space-traces`) and Loki
    (`space-logs`) are read through their own HTTP query APIs, on the localhost
    ports the Tiltfile forwards to them.
    """

    def telemetry_query(self, backend: str, path: str, params: Mapping[str, str]) -> Any:
        """The decoded JSON body of `GET <path>?<params>` on `prometheus`, `tempo` or `loki`."""
        ...

    def telemetry_mode(self) -> str:
        """The Kind telemetry mode (`datadog`, `otel` or `off`) the bench runs.

        Read from the space-api Deployment's metrics sink and trace exporter,
        never from this runner's environment, which need not have brought the
        bench up. Exporters that match no mode, or that are not literals, raise
        `SmokeConfigurationError` naming what was found.
        """
        ...


@dataclass(frozen=True)
class TcpTarget:
    """One address a network-isolation probe opens a TCP connection to."""

    name: str
    host: str
    port: int


@dataclass(frozen=True)
class TcpProbe:
    """Whether a TCP connect from one network namespace completed.

    `connected` is a completed handshake; anything else (a refusal, a reset,
    a policy drop timing out) is not, and `detail` says which the probe saw.
    """

    target: TcpTarget
    connected: bool
    detail: str


@dataclass(frozen=True)
class PodNetwork:
    """A pod's name, bound node and pod IP."""

    pod_name: str
    node_name: str
    pod_ip: str


class SandboxNetworkIsolationControl(Protocol):
    """Kind-only probes of the sandbox namespace's NetworkPolicies.

    Every probe runs in the node's operator shell (`space-node-debug`), either
    in the shell's own network namespace, which sits in the control-plane
    namespace with no policy, or in another pod's network namespace entered by
    its netns handle (`nsenter --net`). Nothing runs inside a sandbox VM, so
    the per-sandbox net-gateway's own rules are not on the path.
    """

    def sandbox_pod_network(self, sandbox_id: str) -> PodNetwork | None:
        """The sandbox's bound, running execution pod in the sandbox namespace."""
        ...

    def control_plane_targets(self) -> list[TcpTarget]:
        """Postgres, the Kubernetes API, internal-api and the object store, by Service ClusterIP."""
        ...

    def cluster_dns_target(self) -> TcpTarget:
        """The cluster DNS Service on TCP 53, which the allow policy admits."""
        ...

    def probe_from_node_shell(
        self, node_name: str, targets: Sequence[TcpTarget], *, timeout: float
    ) -> list[TcpProbe]:
        """TCP connects from the operator shell's own network namespace on that node."""
        ...

    def probe_from_pod_netns(
        self, pod: PodNetwork, targets: Sequence[TcpTarget], *, timeout: float
    ) -> list[TcpProbe]:
        """TCP connects from the pod's network namespace, outside any VM it runs."""
        ...

    def start_unselected_pod(self, node_name: str, token: str) -> PodNetwork:
        """A pause pod in the sandbox namespace that no allow rule selects, bound to the node."""
        ...

    def remove_unselected_pods(self, token: str) -> None:
        """Delete the pods `start_unselected_pod` created under this token; idempotent."""
        ...


@dataclass(frozen=True)
class ControllerLease:
    """The cluster-controller's `coordination.k8s.io` Lease, as the apiserver holds it.

    Times are epoch seconds. A holder that dies without releasing keeps its
    name here until a live replica takes the Lease over after
    `duration_seconds`, so a holder is only current while `renew_time` is.
    """

    holder: str
    renew_time: float
    acquire_time: float | None
    duration_seconds: int
    transitions: int


@dataclass(frozen=True)
class ControllerPod:
    """One cluster-controller pod and its controller container's status.

    `started_at` is when the current container started; `last_exit_code` and
    `last_finished_at` describe the previous container, the one kubelet last
    restarted. Times are epoch seconds.
    """

    name: str
    uid: str
    node: str
    ready: bool
    deleting: bool
    restart_count: int
    started_at: float | None
    last_exit_code: int | None
    last_finished_at: float | None


@dataclass(frozen=True)
class NodeContainer:
    """A controller container as its node's container runtime records it.

    Read through the CRI on the node itself, so it answers while the
    apiserver does not. Times are epoch seconds.
    """

    container_id: str
    attempt: int
    running: bool
    started_at: float | None
    finished_at: float | None
    exit_code: int | None


class ControllerLeaseWatch(Protocol):
    """A running watch of the leadership Lease."""

    def latest(self) -> ControllerLease | None:
        """The delivered Lease with the newest `renewTime`, or None before the first."""
        ...

    def stop(self) -> None:
        """End the watch; idempotent."""
        ...


class NodeExitWatch(Protocol):
    """A running watch of one node's container exits, read from its containerd."""

    def exits(self) -> dict[str, tuple[int, float]]:
        """Container id -> (exit code, exit time) for every exit seen so far."""
        ...

    def diagnostics(self) -> str:
        """Whether the watch still runs, and the end of its stderr, for evidence."""
        ...

    def stop(self) -> None:
        """End the watch; idempotent."""
        ...


class LeaderElectionControl(TelemetryBackendControl, Protocol):
    """Kind-only operations for the cluster-controller leader-election scenario.

    The faults are a third contender Pod that runs the controller's own
    pod template under a ServiceAccount with no `leases` grant, and the
    control-plane apiserver's static-pod manifest moved out of kubelet's
    manifest directory. Everything asserted is read from Kubernetes objects,
    the node's container runtime, the readiness marker file the controller's
    probe reads, or Prometheus.
    """

    def acquire_retention_sweeper_mutation(self) -> Callable[[], None]:
        """Hold the Kind mutation Lease that serializes cooperating smokes."""
        ...

    def controller_replicas(self) -> int:
        """The cluster-controller Deployment's desired replica count."""
        ...

    def controller_leader_election_enabled(self) -> bool:
        """The Deployment's leader-election kill switch is on."""
        ...

    def controller_liveness_budget_env(self, *, refused_contender: bool = False) -> str | None:
        """The liveness-budget test hook's literal on the healthy Deployment or the contender's.

        None when unset; a `valueFrom` entry is refused.
        """
        ...

    def set_controller_liveness_budget_env(self, seconds: str) -> None:
        """Set the hook to whole seconds under the caller's mutation Lease and await the rollout."""
        ...

    def restore_controller_liveness_budget_env(
        self, standing: str | None, *, wait: bool = True
    ) -> None:
        """Put a captured literal back verbatim, removing the key for None.

        Awaits the rollout unless `wait` is False, which cleanup uses so the
        write fits its budget.
        """
        ...

    def controller_lease(self) -> ControllerLease | None:
        """The leadership Lease, or None when it does not exist yet."""
        ...

    def watch_controller_lease(self) -> ControllerLeaseWatch:
        """Start watching the leadership Lease; every change it delivers is kept."""
        ...

    def controller_pods(self, *, refused_contender: bool = False) -> list[ControllerPod]:
        """The healthy Deployment's pods, or the refused contender's."""
        ...

    def place_refused_contender(self) -> None:
        """Place one more pod of the controller's pod template without `leases` RBAC.

        Returns once its gate container runs, holding the controller container
        back until `release_refused_contender`. Refuses when the substitute
        ServiceAccount can read Leases, since every lease call then succeeds
        and nothing would be refused.
        """
        ...

    def release_refused_contender(self) -> None:
        """Open the placed contender's gate through its node's CRI, so its controller starts."""
        ...

    def remove_refused_contender(self, *, wait: bool = True) -> None:
        """Delete the contender, and any earlier Deployment of it; idempotent.

        Waits for its pods to be gone unless `wait` is False.
        """
        ...

    def watch_node_container_exits(self, node_name: str) -> NodeExitWatch:
        """Start recording the node's container exits as containerd reports them."""
        ...

    def control_plane_node(self) -> str:
        """The Kind node container that runs the apiserver's static pod."""
        ...

    def apiserver_answers(self) -> bool:
        """`/readyz` answers through the kube context within a few seconds."""
        ...

    def park_apiserver(self, node_name: str) -> None:
        """Move the apiserver's static-pod manifest out of kubelet's directory."""
        ...

    def restore_apiserver(self, node_name: str, *, timeout: float = 30) -> None:
        """Put a parked apiserver manifest back; a no-op when none is parked.

        The node exec is bounded by `timeout`.
        """
        ...

    def node_controller_containers(self, node_name: str, pod_name: str) -> list[NodeContainer]:
        """Every controller container the node's runtime still holds for the pod."""
        ...

    def readiness_marker_present(self, node_name: str, container_id: str) -> bool | None:
        """Whether the container holds the controller's readiness marker file.

        Read by one exec inside the container that answers explicitly. None
        when that exec fails or answers anything else, which says nothing about
        the marker.
        """
        ...

    def node_container_log_tail(self, node_name: str, container_id: str, *, lines: int) -> str:
        """The container's last log lines, read on the node: failure evidence only."""
        ...


@dataclass(frozen=True)
class StorageObject:
    """One object in the Kind object store, as the store's root keys see it."""

    bucket: str
    key: str
    etag: str
    size: int

    @property
    def arn(self) -> str:
        return f"arn:aws:s3:::{self.bucket}/{self.key}"


@dataclass(frozen=True)
class StorageSession:
    """Credentials credential-manager's storage exchange issued."""

    access_key_id: str
    secret_access_key: str
    session_token: str
    expires_at_unix: int


@dataclass(frozen=True)
class StorageExchangeOutcome:
    """An ExchangeToken answer: a session, or the gRPC refusal."""

    session: StorageSession | None
    status: str
    detail: str


@dataclass(frozen=True)
class StorageRequestResult:
    """One S3 request made with an exchanged session."""

    operation: str
    target: str
    succeeded: bool
    refused: bool
    detail: str


class StorageExchangeControl(Protocol):
    """Kind-only access to credential-manager's storage exchange and the store.

    The exchange goes through a sandbox worker's or builder's
    credential-manager agent, on the volume socket and as the volume-manager
    account, exactly as volume-manager asks for a transfer's credentials; the
    agent forwards it to the authority, which vends a session through STS. S3 requests then use
    that session against the object store.
    """

    def storage_credentials_mode(self) -> str:
        """The mode the cluster runs: `exchange` or `static`."""
        ...

    def sandbox_worker_nodes(self) -> list[str]:
        """Nodes running the worker volume-manager DaemonSet, sorted."""
        ...

    def builder_nodes(self) -> list[str]:
        """Builder nodes with a Ready node-api, sorted."""
        ...

    def ready_full_snapshots(self, sandbox_id: str) -> list[SnapshotRecord]: ...

    def snapshot_objects(self, record: SnapshotRecord) -> list[StorageObject]:
        """The objects under a snapshot's storage prefix, read with root keys."""
        ...

    def bucket_objects(self, bucket: str, *, limit: int, prefix: str = "") -> list[StorageObject]:
        """Up to `limit` objects of a bucket under `prefix`, read with root keys."""
        ...

    def head_object(self, bucket: str, key: str) -> StorageObject | None:
        """An object's ETag and size read with root keys; None if absent."""
        ...

    def exchange_storage_credentials(
        self, node_name: str, *, subject: str, resource: str, scope: str
    ) -> StorageExchangeOutcome:
        """ExchangeToken through the node's agent volume socket."""
        ...

    def storage_request(
        self, session: StorageSession, operation: str, bucket: str, key: str, *, body: bytes | None = None
    ) -> StorageRequestResult:
        """One S3 request (head, get, list, put) signed with the session."""
        ...


@dataclass(frozen=True)
class TerminateAttempt:
    """What admin `:terminate` answered, verbatim.

    A refusal is the expected outcome, so the status is data rather than an
    exception — and the body is carried whole because which 409 this is can
    only be told apart by the keys it contains.
    """

    status_code: int
    body: Mapping[str, object]


@dataclass(frozen=True)
class DrainGateBlocker:
    """The seeded row that must make `:terminate` refuse, and its node."""

    node_id: str
    sandbox_id: str


class DrainGateControl(Protocol):
    """Privileged operations behind the admin `:terminate` occupancy guard.

    Kind-only, and every write is fenced on a cluster-visible mutation Lease:
    the scenario moves a real `nodes` row to `drained` and seeds a real
    `sandboxes` row, so two concurrent smokes must not interleave. The Lease
    records both so an expired holder's residue is repaired on takeover.
    """

    def acquire_drain_gate_mutation(self) -> Callable[[], None]:
        """Take the drain-gate mutation Lease; the returned callable releases it."""
        ...

    def seed_drain_gate_blocker(self) -> DrainGateBlocker:
        """Pin one `suspending` sandbox to an idle node and move it to `drained`.

        The node is registered, carries no drain phase, and holds no sandbox
        by `active_node_id`; the seeded row sets `target_node_id` only. That
        pairing is what makes a refusal attributable: the resident half of the
        guard cannot see this row, so only the drain-gate half can produce it.
        Raises rather than improvising when no such node exists.
        """
        ...

    def clear_drain_gate_residue(self) -> None:
        """Undo whatever this holder's Lease records, in repair order.

        Takes no argument on purpose. The Lease is the record of what was
        written, updated before each write, so this is callable the moment
        the Lease is held — before any seeding — and a caller that dies
        between two of the seeding's writes still has its compensation
        armed. Idempotent: nothing recorded is nothing to undo.
        """
        ...

    def attempt_terminate(self, node_id: str) -> TerminateAttempt:
        """POST `:terminate` for that node as an administrator."""
        ...
