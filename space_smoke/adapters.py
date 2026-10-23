"""Concrete SDK and kubectl adapters shared by smoke scenarios."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import importlib
import ipaddress
import json
import os
import re
import secrets
import shlex
import subprocess
import sys
import tempfile
import threading
import socket
import sys
import time
import uuid
from collections import deque
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from http.client import HTTPException
from pathlib import Path
from typing import Any, BinaryIO, NoReturn
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlsplit
from urllib.request import Request, urlopen

from space_smoke.capabilities import (
    ControllerLease,
    ControllerPod,
    NodeContainer,
    LimiterProbeOutcome,
    PodNetwork,
    TcpProbe,
    TcpTarget,
    AdminNodeGroupView,
    CensusPublication,
    FencePolicy,
    NodeSnapshot,
    NodeCtlIdentity,
    PendingBackupRow,
    SaWriteOutcome,
    WhoAmI,
    GuestMetadata,
    TemplateBuildJob,
    TemplateBuildResult,
    TemplateRecipe,
    SandboxPodRender,
    AdminNodeView,
    SandboxResidency,
    EncryptedSnapshotObservation,
    ExecutionRow,
    RestoreCallerOutcome,
    SandboxPodView,
    ColdCreatePod,
    ColdCreateSelection,
    EcwCommandResult,
    EcwCreateAttempt,
    EcwCreateNotDelivered,
    EcwDataPlaneNotFound,
    EcwFileEntry,
    EcwFilesystemEvent,
    EcwFilesystemEventType,
    EcwLifecycle,
    EcwPermissionDenied,
    EcwProcess,
    EcwPublicPort,
    EcwSandbox,
    EcwSandboxBusy,
    EcwSandboxDeleteRetryable,
    EcwSandboxInfo,
    EcwSandboxPages,
    EcwSandboxState,
    EcwSandboxUnavailable,
    EcwWatchHandle,
    KindColdCreateConfig,
    KindWarmPoolConfig,
    NetworkConfig,
    ProcessInfo,
    RetentionSweeperConfig,
    ColdEvictionConfig,
    SnapshotReaperConfig,
    SandboxPodPlacement,
    SnapshotRecord,
    StorageExchangeOutcome,
    StorageObject,
    StorageRequestResult,
    StorageSession,
    ReconcileQueueRef,
    ReleaseStorageRow,
    RestoreActivation,
    RestoreFailure,
    TransferJobView,
    SnapshotRowFacts,
    TemplateCacheGcConfig,
    WarmPoolCandidate,
    WarmPoolSelection,
)
from space_smoke.credmgr_socket import exchange_token, relay_argv
from space_smoke.reconcile_queue_gc import (
    GC_ENV_KEYS as RECONCILE_QUEUE_GC_ENV_KEYS,
    validate_marker,
    validate_node_name,
)
from space_smoke.orphaned_restore_pod_gc import (
    GC_ENV_KEYS,
    GC_LOG_NEEDLE,
    PLANTED_ORPHAN_LABEL,
    build_planted_orphan,
    validate_gc_keys,
    validate_gc_overrides,
)
from space_smoke.runtime import (
    background,
    CommandExecutor,
    CommandResult,
    REPO_ROOT,
    SmokeConfigurationError,
    run_concurrently,
)

KIND_NAMESPACE = "space"
# The API creates sandbox pods in their own namespace, apart from the control
# plane (`k8s.sandboxNamespace` in the Kind charts), where the
# space-sandbox-network-policy chart's NetworkPolicies confine them.
# Everything else the harness reads or mutates stays in KIND_NAMESPACE.
KIND_SANDBOX_NAMESPACE = "sandboxes"
KIND_BOOTSTRAP_POST_API_TIMEOUT_SECONDS = 2400
# Longer than the 30-minute scenario budget plus a full controller rollout.
# A crashed owner eventually expires, while a live scenario cannot be taken
# over before its body-level restore and release have had time to finish.
RETENTION_MUTATION_LEASE_DURATION_SECONDS = 3600


def _serialize_network_config(
    network: NetworkConfig,
) -> dict[str, object]:
    """Return the shared SDK/raw-create network body."""

    network_options: dict[str, object] = {}
    if network.allow_public_traffic is not None:
        network_options["allow_public_traffic"] = network.allow_public_traffic
    if network.allow_out:
        network_options["allow_out"] = list(network.allow_out)
    if network.deny_out:
        network_options["deny_out"] = list(network.deny_out)
    return network_options


def _require_ok(result: CommandResult, action: str) -> str:
    if result.returncode != 0:
        raise RuntimeError(
            f"{action} failed ({result.returncode}): {result.detail() or '<no output>'}"
        )
    return result.stdout


def _env_literals(
    entries: Any, keys: Sequence[str], *, owner: str, value_from_refusal: str
) -> dict[str, str | None]:
    """The literal each key holds in a container's `env` entries, None if unset.

    A `valueFrom` entry for a key is refused, naming `owner`, with the
    caller's reason.
    """

    standing: dict[str, str | None] = {key: None for key in keys}
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("name") not in standing:
            continue
        if "valueFrom" in entry:
            raise SmokeConfigurationError(
                f"{entry['name']} on {owner} is a valueFrom reference{value_from_refusal}"
            )
        value = entry.get("value")
        standing[entry["name"]] = value if isinstance(value, str) else ""
    return standing


def _truthy_default(value: str | None, default: bool) -> bool:
    """Match cluster-controller's accepted boolean environment vocabulary."""

    if value in {"1", "true", "True", "TRUE"}:
        return True
    if value in {"0", "false", "False", "FALSE"}:
        return False
    return default


def _kube_timestamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_kube_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise SmokeConfigurationError(
            "existing retention controller mutation Lease has no safe timestamp"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise SmokeConfigurationError(
            "existing retention controller mutation Lease has an invalid timestamp"
        ) from error
    if parsed.tzinfo is None:
        raise SmokeConfigurationError(
            "existing retention controller mutation Lease has a naive timestamp"
        )
    return parsed.astimezone(UTC)


def _lease_document(output: str) -> dict[str, object]:
    try:
        document = json.loads(output)
    except json.JSONDecodeError as error:
        raise SmokeConfigurationError(
            "retention controller mutation Lease returned invalid JSON"
        ) from error
    if not isinstance(document, dict):
        raise SmokeConfigurationError(
            "retention controller mutation Lease returned a non-object"
        )
    return document


def _lease_annotations(document: Mapping[str, object]) -> Mapping[str, object]:
    metadata = document.get("metadata")
    annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
    return annotations if isinstance(annotations, dict) else {}


def _require_limiter_env_keys(values: Mapping[str, object]) -> None:
    from space_smoke.rate_limiters import LIMITER_ENV_KEYS

    unknown = sorted(set(values) - set(LIMITER_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing non-limiter public API env keys: {unknown}")


def _recorded_limiter_env(recorded: str) -> dict[str, str | None]:
    """The standing limiter env a Lease recorded, refused unless every entry is restorable."""

    try:
        standing = json.loads(recorded)
    except json.JSONDecodeError as error:
        raise SmokeConfigurationError("the recorded limiter env is not JSON") from error
    if not isinstance(standing, dict) or not all(
        value is None or (isinstance(value, str) and (not value or _LIMITER_ENV_VALUE.fullmatch(value)))
        for value in standing.values()
    ):
        raise SmokeConfigurationError("the recorded limiter env holds a value a restore cannot write")
    _require_limiter_env_keys(standing)
    return standing


def _holder_process_gone(holder: object) -> bool:
    """True only for a holder that was a process of this user on this host and has exited.

    A smoke killed mid-fault leaves its Lease live for the full duration; a
    successor that can prove the holder dead need not wait that out. Any doubt
    (another host or user, a malformed identity, a live or reused pid) is
    treated as alive.
    """

    if not isinstance(holder, str):
        return False
    fields = dict(part.partition("=")[::2] for part in holder.split(";"))
    pid = fields.get("pid", "")
    if (
        fields.get("uid") != str(os.getuid())
        or fields.get("host") != socket.gethostname()
        or not pid.isdigit()
    ):
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return False


def _json_pointer_token(key: str) -> str:
    return key.replace("~", "~0").replace("/", "~1")


def _json_pointer_get(document: Mapping[str, Any], pointer: str) -> object | None:
    """The value at a JSON pointer of plain keys, None if any step is absent."""

    value: object = document
    for token in pointer.split("/")[1:]:
        if not isinstance(value, dict) or token not in value:
            return None
        value = value[token]
    return value


@dataclass(frozen=True)
class _RolloutPace:
    """A Deployment's identity and the pace fields it stood at before a fast rollout (None: unset)."""

    uid: str
    standing: Mapping[str, object | None]


_FAST_ROLLOUT_FIELDS = frozenset({"/spec/minReadySeconds", "/spec/strategy/rollingUpdate/maxSurge"})


def _fast_pace(deployment: Mapping[str, Any]) -> dict[str, object]:
    """The pace fields a fast rollout writes on this Deployment, and their values."""

    fast: dict[str, object] = {"/spec/minReadySeconds": 0}
    if (deployment["spec"].get("strategy") or {}).get("type") == "RollingUpdate":
        fast["/spec/strategy/rollingUpdate/maxSurge"] = FAST_ROLLOUT_MAX_SURGE
    return fast


def _holds_fast_value(deployment: Mapping[str, Any], pointer: str, fast: Mapping[str, object]) -> bool:
    if pointer not in fast:
        return False
    live = _json_pointer_get(deployment, pointer)
    # The apiserver omits a zero `minReadySeconds`.
    return live == fast[pointer] or (live is None and fast[pointer] == 0)


def _fast_rollout_record(text: str, owner: str) -> dict[str, object | None]:
    try:
        record = json.loads(text)
    except json.JSONDecodeError:
        record = None
    if not isinstance(record, dict) or not set(record) <= _FAST_ROLLOUT_FIELDS or not all(
        value is None
        or (isinstance(value, int) and not isinstance(value, bool) and value >= 0)
        or (isinstance(value, str) and re.fullmatch(r"\d+%?", value))
        for value in record.values()
    ):
        raise SmokeConfigurationError(
            f"{owner} carries an unreadable {FAST_ROLLOUT_STANDING_ANNOTATION} record: {text!r}"
        )
    return record


def _lease_parts(
    document: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    metadata = document.get("metadata")
    spec = document.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        raise SmokeConfigurationError(
            "retention controller mutation Lease is missing metadata or spec"
        )
    if not isinstance(metadata.get("uid"), str) or not isinstance(
        metadata.get("resourceVersion"), str
    ):
        raise SmokeConfigurationError(
            "retention controller mutation Lease is missing its ownership fence"
        )
    return metadata, spec


def _sql_uuid(value: str) -> str:
    """Refuse anything that is not a bare UUID before it reaches SQL."""

    import uuid as _uuid

    return str(_uuid.UUID(value))


def _sql_identifier_ish(value: str) -> str:
    """Refuse artifact kinds outside the closed lowercase vocabulary."""

    if not value or not all(ch.isalnum() or ch == "_" for ch in value):
        raise ValueError(f"unexpected artifact kind: {value!r}")
    return value


def _sql_uuid_list(values: Sequence[str]) -> str:
    if not values:
        raise ValueError("refusing an empty id list")
    return ", ".join(f"'{_sql_uuid(value)}'" for value in values)


# Two non-ready k8s nodes, each with a 25 h-old live ref; the live one also
# holds a 25 h-old dead letter and a 1 h-old one (`space_smoke.reconcile_queue_gc`).
RECONCILE_QUEUE_GC_SEED_SQL = """\
BEGIN;
INSERT INTO nodes (id, role, managed_by, state)
VALUES ('{live_id}', 'worker', 'k8s', 'registered'),
       ('{gone_id}', 'worker', 'k8s', 'registered');
INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
VALUES ('{live_id}', '{live_name}', '{marker}'),
       ('{gone_id}', '{gone_name}', '{marker}');
INSERT INTO node_reconcile_queue (node_id, facet, element, requested_at, updated_at, dead_at, dead_reason)
VALUES ('{live_id}', 'node-status', '', NOW() - INTERVAL '25 hours', NOW() - INTERVAL '25 hours', NULL, ''),
       ('{gone_id}', 'node-status', '', NOW() - INTERVAL '25 hours', NOW() - INTERVAL '25 hours', NULL, ''),
       ('{live_id}', 'sandbox-inventory', '', NOW() - INTERVAL '25 hours', NOW() - INTERVAL '25 hours',
        NOW() - INTERVAL '25 hours', '{marker}'),
       ('{live_id}', 'template-inventory', '', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour',
        NOW() - INTERVAL '1 hour', '{marker}');
COMMIT;
"""


# Oldest-first so [0] is the pin target the retention scenario manufactures.
READY_FULL_SNAPSHOTS_SQL = """\
SELECT s.id,
       COALESCE(s.local_artifact_id::text, ''),
       COALESCE(s.artifact_kind, ''),
       COALESCE(location.storage_connection_string, ''),
       location.storage_location_count
  FROM snapshots s
  LEFT JOIN LATERAL (
       SELECT CASE WHEN COUNT(*) = 1
                   THEN MIN(storage_connection_string)
              END AS storage_connection_string,
              COUNT(*) AS storage_location_count
         FROM snapshot_storage_locations
        WHERE snapshot_id = s.id
  ) location ON TRUE
 WHERE s.sandbox_id = '{sandbox_id}'
   AND s.state = 'ready' AND s.type = 'full'
 ORDER BY s.timestamp ASC, s.id ASC;
"""

# Matches only a still-suspended row: the manufactured stale pin must never
# land on a sandbox that woke mid-scenario.
PIN_CURRENT_ARTIFACT_SQL = """\
UPDATE sandboxes
   SET current_local_artifact_id = '{artifact_id}',
       current_local_artifact_kind = '{artifact_kind}',
       current_local_artifact_origin = 'suspend'
 WHERE id = '{sandbox_id}' AND lifecycle = 'suspended'
 RETURNING id;
"""

SNAPSHOT_EXISTS_SQL = "SELECT 1 FROM snapshots WHERE id = '{snapshot_id}';"

# Fault injection for the claim-before-delete protocol: durably claim a row
# exactly the way `claim_prune` commits it, then vanish — the crashed-claimer
# state the sweeper must rediscover and finish forward. Guarded on
# state='ready' so a doubly-applied injection reads as "no row updated"
# rather than silently re-marking.
MARK_SNAPSHOT_DELETING_SQL = """\
UPDATE snapshots
   SET state = 'deleting'
 WHERE id = '{snapshot_id}' AND state = 'ready'
 RETURNING id;
"""

DELETING_SNAPSHOTS_SQL = """\
SELECT id FROM snapshots
 WHERE sandbox_id = '{sandbox_id}' AND state = 'deleting';
"""

KIND_CONTROLLER_SELECTOR = "app.kubernetes.io/name=space-cluster-controller"
# The cluster-controller's leadership Lease, container and readiness marker
# (crates/libs/cluster/controller/server/src/leader_election/mod.rs and the
# space-cluster-controller-local chart).
CONTROLLER_LEASE_NAME = "cluster-controller"
CONTROLLER_CONTAINER = "cluster-controller"
CONTROLLER_READINESS_MARKER = "/tmp/space-cluster-controller-ready"
# The leader-election kill switch: on unless one of these values turns it off
# (truthy_default in crates/libs/cluster/controller/server/src/env_config.rs).
LEADER_ELECTION_ENV = "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_ENABLED"
LEADER_ELECTION_FALSE_VALUES = frozenset({"0", "false", "False", "FALSE"})
# The liveness-budget test hook the brownout scenario scopes to its own run
# (LEASE_FAILURE_GRACE_TEST_ENV in leader_election/mod.rs), the one
# controller env key the leader-election operations write. Inert unless the
# `test-hooks` feature is compiled in.
LIVENESS_BUDGET_ENV = "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_FAILURE_GRACE_SECS"
# The bound on each request the brownout scenario's cleanup makes; its time
# plan is in space_smoke/leader_election.py. The mutation Lease's release uses
# it for every caller: a release request that outlasts it has stalled.
CLEANUP_REQUEST_SECONDS = 8.0
_LIVENESS_BUDGET_VALUE = re.compile(r"[1-9][0-9]{0,3}")
# A third contender: the controller's own pod template under the API's
# ServiceAccount, which carries the substrate's pod and node RBAC but no
# `leases` grant, so its substrate builds and every lease call is a 403. It
# gets its own `app.kubernetes.io/name`, so no selector of the healthy
# Deployment's, nor KIND_CONTROLLER_SELECTOR, overlaps it. It is a bare Pod
# bound to a healthy replica's node, and an init container holds it until the
# scenario opens a gate file through the node's CRI. So it can be placed
# before an apiserver outage and start right after one: once the apiserver is
# back, kubelets and the controller-manager take a minute or more to watch
# new pods again, while a CRI exec needs neither.
REFUSED_CONTENDER_NAME = "space-cluster-controller-refused"
REFUSED_CONTENDER_SELECTOR = f"app.kubernetes.io/name={REFUSED_CONTENDER_NAME}"
REFUSED_CONTENDER_SERVICE_ACCOUNT = "space-api"
REFUSED_CONTENDER_GATE_CONTAINER = "space-smoke-gate"
REFUSED_CONTENDER_GATE_DIR = "/space-smoke-gate"
# Outside kubelet's static-pod directory, which it reads every file of.
APISERVER_MANIFEST = "/etc/kubernetes/manifests/kube-apiserver.yaml"
APISERVER_PARKED_MANIFEST = "/etc/kubernetes/kube-apiserver.yaml.space-smoke-parked"
# Node services run one DaemonSet per service per role, as production's do
# (infra/space/kind/lib/node-services.sh). Every pod carries its node role as
# `app.kubernetes.io/name` (space-worker or space-builder) and its service as
# `app.kubernetes.io/component`, and its container is named after the service
# (infra/space/kind/charts/space-node-service). Logs and environment are read
# from the service that owns them.
KIND_WORKER_ROLE = "space-worker"
KIND_BUILDER_ROLE = "space-builder"
NODE_SANDBOX_MANAGER = "sandbox-manager"
NODE_MANAGER = "node-manager"
# The OTLP metric export interval and the SDKs' default for it
# (docs/observability.md).
OTEL_METRIC_EXPORT_INTERVAL_ENV = "OTEL_METRIC_EXPORT_INTERVAL"
OTEL_METRIC_EXPORT_DEFAULT_SECONDS = 10.0
NODE_TEMPLATE_MANAGER = "template-manager"
NODE_VOLUME_MANAGER = "volume-manager"
NODE_CTL = "node-ctl"
NODE_API = "node-api"
# The node services that read the object store, by DaemonSet and the
# container that does: each holds the store's static keys in
# `SPACE_KIND_STORAGE_CREDENTIALS=static` only, and exchanges its credentials
# through credential-manager otherwise.
STORAGE_CREDENTIAL_DAEMONSETS = (
    ("space-volume-manager", NODE_VOLUME_MANAGER, "worker volume-manager"),
    ("space-builder-volume-manager", NODE_VOLUME_MANAGER, "builder volume-manager"),
    ("space-builder-build-manager", "build-manager", "build-manager"),
)
OBJECT_STORE_STATIC_KEYS = (
    "SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID",
    "SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY",
)
# The Kind-only operator shell on every sandbox worker and builder
# (infra/space/kind/charts/space-node-debug): the stand-in for the node host's
# root shell, where the on-node CLIs, btrfs, dmsetup, cryptsetup and ctr run
# against the node's /space and containerd. No service container is given a
# tool, an identity or a privilege for the harness's sake.
KIND_NODE_SHELL_SELECTOR = "app.kubernetes.io/name=space-node-debug"
KIND_NODE_SHELL_CONTAINER = "shell"
# Two builder node-apis sit behind one Service on kind.
KIND_BUILDER_SERVICE = "space-builder-node-api"
BUILDER_ROUTING_LABEL = "space.pplx.net/routing-smoke"
OCI_BUILD_CACHE_DIR = "/space/cache/firecracker/builds/oci"
CONTAINERD_BUILDS_NAMESPACE = "space-builds"
# The worker-cordon Lease records which node its holder cordoned, so the next
# holder can uncordon it if that holder never did. Nothing in SPACE itself ever
# uncordons.
CORDONED_NODE_ANNOTATION = "space.pplx.net/smoke-cordoned-node"
# The public-API mutation Lease records what the rate-limiter scenario is about
# to change before it changes it: the limiter env the public API Deployment
# held (JSON, null for unset) and the Redis replica count it scales down from.
# A record is cleared only once its restore succeeded. The next holder of the
# Lease puts back whatever is still recorded, and every Kind scenario run first
# makes sure nothing is (`repair_orphaned_limiter_faults`), so a run killed or
# failing mid-fault does not leave later scenarios on smoke-sized caps or
# without the limiters' store.
LIMITER_ENV_ANNOTATION = "space.pplx.net/smoke-limiter-env"
REDIS_REPLICAS_ANNOTATION = "space.pplx.net/smoke-redis-replicas"
# A rollout a scenario starts only to apply or restore its env
# (`KindHarness._fast_rollout`) runs with every replacement surged at once and
# no `minReadySeconds`, and the Deployment carries the `minReadySeconds` and
# `maxSurge` it stood at (JSON, null for unset) until they are back. A run
# killed in between leaves the record, and every Kind scenario run first puts
# the recorded values back (`repair_orphaned_rollout_pace`).
FAST_ROLLOUT_STANDING_ANNOTATION = "space.pplx.net/smoke-fast-rollout-standing"
FAST_ROLLOUT_MAX_SURGE = "100%"
DRAIN_KEEP_ANNOTATION = "space.pplx.net/drain-keep"
NODE_GROUP_BOUNDARY_LABEL = "node-restriction.kubernetes.io/space-node-group"
NODE_GROUP_GATE_ENV = "SPACE_SANDBOX_NODE_GROUP_PLACEMENT"
NODE_GROUP_LABELER_ENV = "SPACE_NODE_GROUP_LABELER_ENABLED"
SELECTOR_KEY_ENV = "SPACE_SANDBOX_NODE_SELECTOR_KEY"
SELECTOR_VALUE_ENV = "SPACE_SANDBOX_NODE_SELECTOR_VALUE"
SELECTOR_LABELS_ENV = "SPACE_SANDBOX_NODE_SELECTOR_LABELS"
ADMISSION_ENABLED_ENV = "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED"
# Non-pool sandbox pods: `build_pool_pod` stamps `pool-role`, a user create does not.
KIND_SANDBOX_POD_SELECTOR = "app.kubernetes.io/name=space-sandbox,!space.pplx.net/pool-role"
CONTROL_PLANE_LABEL = "node-role.kubernetes.io/control-plane"
KIND_WORKER_NODE_SELECTOR = f"!{CONTROL_PLANE_LABEL}"
SANDBOX_RESIDENCY_SQL = """\
SELECT lifecycle, coalesce(active_node_id::text, '') FROM sandboxes WHERE id = '{sandbox_id}'::uuid;
"""
POD_CAP_QUOTA_NAME = "space-smoke-pod-cap"
# The sandbox-isolation smoke's pod that no allow rule selects: the sandbox
# pods' own image (k8s.sandboxPodImage), already on every sandbox node, run by
# the node's default runtime rather than the SPACE shim.
UNSELECTED_POD_LABEL = "space.pplx.net/smoke-unselected"
UNSELECTED_POD_IMAGE = "registry.k8s.io/pause:3.10"
UNSELECTED_POD_READY_SECONDS = 120
PUBLIC_API_DEPLOYMENT_SELECTOR = "app.kubernetes.io/name=space-api"
KIND_REDIS_DEPLOYMENT = "space-redis"
KIND_OBJECT_STORE_DEPLOYMENT = "space-object-store"
KIND_OBJECT_STORE_POD_SELECTOR = "app.kubernetes.io/name=space-object-store"
KIND_INTERNAL_API_SELECTOR = "app.kubernetes.io/name=space-internal-api"
PENDING_BACKUP_QUEUE_SQL = """\
SELECT artifact_id::text, artifact_kind FROM node_local_backup_pending
 WHERE sandbox_id = '{sandbox_id}'::uuid ORDER BY artifact_id;
"""
# The E2B route records the build under its template id, the lowercased alias,
# once a builder has accepted it (`record_accepted_build`).
TEMPLATE_BUILD_JOB_SQL = """\
SELECT coalesce(j.daemon_job_id, ''), coalesce(j.builder_node_id::text, ''),
       coalesce(nk.k8s_node_name, ''), j.build_id
  FROM template_build_jobs j LEFT JOIN node_k8s nk ON nk.node_id = j.builder_node_id
 WHERE j.template_id = '{template_id}' AND j.daemon_job_id IS NOT NULL
 ORDER BY j.created_at DESC LIMIT 1;
"""
SNAPSHOT_STATES_SQL = """\
SELECT state FROM snapshots WHERE sandbox_id = '{sandbox_id}'::uuid AND type = 'full' ORDER BY "timestamp";
"""
KIND_REDIS_POD_SELECTOR = "app.kubernetes.io/name=space-redis"
SANDBOX_ID_LABEL = "space.pplx.net/sandbox-id"
CHECKPOINT_ANNOTATION = "space.pplx.net/checkpoint"
RESTORE_SOURCE_NODE_ANNOTATION = "space.pplx.net/suspend-restore-source-node"
RESTORE_STORAGE_URI_ANNOTATION = "space.pplx.net/suspend-restore-storage-uri"
SANDBOX_EXECUTIONS_SQL = """\
SELECT generation, state, active, pod_name
  FROM sandbox_k8s_executions
 WHERE sandbox_id = '{sandbox_id}'
 ORDER BY generation ASC;
"""
EXECUTION_GENERATION_ANNOTATION = "space.pplx.net/execution-generation"
# template-manager's on-node cache layout and unix identities, as the node
# layer and the service's identity create them. The CLI the operator shell
# runs is root (an allowed operator peer), but the DAEMON runs as the tmplmgr
# user and is what stages, reads and removes the planted files — so the plant
# must be owned like a real build or register cannot touch it.
TEMPLATE_CACHE_ROOT = "/space/cache/firecracker/templates"
TEMPLATE_CACHE_OWNER = "space-tmplmgr"
TEMPLATE_CACHE_GROUP = "space-cache-rw"
KIND_SANDBOX_NODE_SELECTOR = "space.pplx.net/node-pool=sandbox"
# The query API of each local observability backend, on the localhost port
# the Tiltfile forwards to it in `otel` mode.
KIND_TELEMETRY_BACKENDS = {
    "prometheus": "http://127.0.0.1:19090",
    "tempo": "http://127.0.0.1:13200",
    "loki": "http://127.0.0.1:13101",
}
# Each Kind telemetry mode by the (SPACE_METRICS_SINK, SPACE_TRACE_EXPORTER) it
# gives space-api; MODES in infra/space/kind/scripts/telemetry_config.py sets them.
KIND_TELEMETRY_MODES = {
    ("datadog", "datadog"): "datadog",
    ("otlp", "otlp"): "otel",
    ("off", "off"): "off",
}
# A local row is dead only when neither the cluster row nor a warm-pool slot
# claims the id (a pool clone has a local row before any `sandboxes` row).
SANDBOX_LIVE_SQL = """\
SELECT 1 FROM sandboxes WHERE id = '{sandbox_id}'
UNION ALL
SELECT 1 FROM kubernetes_pool_slots WHERE reserved_sandbox_id = '{sandbox_id}'
LIMIT 1;
"""
# Separate budgets for the reclaim's two phases. Waiting for the cluster row
# and the pods can use most of the first on the path the reclaim exists for
# (the kubelet keeps retrying a failed restore pod), and must not starve the
# deletes. Both fit the 120 s cleanup window a 30-minute scenario gets.
LOCAL_SANDBOX_RECLAIM_WAIT_SECONDS = 60
LOCAL_SANDBOX_RECLAIM_DELETE_SECONDS = 45
# Each poll is itself a kubectl round trip; the sleeps only space them out.
LOCAL_SANDBOX_RECLAIM_POLL_SECONDS = 0.25
LOCAL_SANDBOX_RECLAIM_POD_POLL_SECONDS = 0.5
# Three delete refusals get their own handling. Two settle on their own and are
# retried within the delete budget: sbx-manager's quarantine recovery
# completing (`CleanupAdmissionError` in
# crates/libs/sbx/mgr/server/src/service/cleanup.rs), and node-manager's
# sandbox mutation claim held by another host mutation or reset
# (`claim_sandbox_mutation` in crates/libs/node/mgr/server/src/service.rs),
# which the delete's durable cleanup passes through. A pending suspend
# publication cannot land once the cluster row is gone, so it is reported as
# such. Every other refusal, `RetainedFilesystemOnlyCopy` included, is reported
# with the daemon's own message.
_LOCAL_DELETE_RETRY_MARKERS = (
    "retry cleanup",
    "sandbox host mutation or reset is in progress",
)
_LOCAL_DELETE_STUCK_SUSPEND_MARKER = "suspend completion has not reached the cluster"
_TERMINAL_POD_PHASES = frozenset({"Succeeded", "Failed"})
SUSPEND_SNAPSHOT_TEMPLATE_HASH_SQL = """\
SELECT content_hash
  FROM snapshots
 WHERE sandbox_id = '{sandbox_id}' AND type = 'full' AND state = 'ready'
 ORDER BY "timestamp" DESC, id DESC
 LIMIT 1;
"""
_TEMPLATE_IN_USE_MARKER = "sandbox(es) were created from"
BTRFS_SEND_V1 = "btrfs-send-v1"
RETAINED_SOURCE_BUNDLE = "rootfs.btrfs-source"
ROOTFS_SEND_FILE = "rootfs.btrfs-send"
# node-manager's relay refuses a send stream whose framing breaks, as a kernel
# send restarted mid-pipe does, before forwarding it; volume-manager's failed
# attempt carries the status message
# (crates/libs/node/mgr/server/src/service/btrfs_send_transport.rs).
BTRFS_SEND_REJECTED_ERROR = "btrfs send stream rejected"
# node-manager stages each send lease here; a lease's `.send-*` wire clone
# exists only while its kernel send runs.
BTRFS_SEND_STAGING_ROOT = "/space/cache/.btrfs-send-staging"
# Runs in the node's operator shell. While a send is in flight, at most
# `max` times: stop volume-manager, so the send fills the pipe and blocks;
# stop and continue node-manager, which makes the blocked `pipe_write` return
# -ERESTARTSYS and the kernel restart the send ioctl; continue
# volume-manager. Both daemons are continued however the script ends.
_BTRFS_SEND_INTERRUPTER = r"""
set -u
stop={stop}; max={pulses}; deadline=$(( $(date +%s) + {window} )); n=0
pid_of() {{ for d in /proc/[0-9]*; do [ "$(cat "$d/comm" 2>/dev/null)" = "$1" ] && echo "${{d#/proc/}}"; done; }}
nm=$(pid_of space-nodemgr-s); vol=$(pid_of space-volmgr-sv)
case "$nm $vol" in
  *[!0-9\ ]* | " "* | *" ")
    echo "expected one node-manager and one volume-manager, found [$nm] [$vol]" >&2
    exit 3 ;;
esac
is() {{ [ "$(cat "/proc/$1/comm" 2>/dev/null)" = "$2" ]; }}
resume() {{ kill -CONT "$nm" "$vol" 2>/dev/null; rm -f "$stop"; echo "pulses $n"; }}
trap resume EXIT
trap 'exit 0' HUP INT TERM
while [ ! -e "$stop" ] && [ "$(date +%s)" -lt "$deadline" ]; do
  if ! is "$nm" space-nodemgr-s || ! is "$vol" space-volmgr-sv; then
    echo "node-manager or volume-manager restarted" >&2
    exit 4
  fi
  if [ "$n" -lt "$max" ] && [ -n "$(find {staging} -maxdepth 3 -type d -name '.send-*' -print -quit 2>/dev/null)" ]; then
    kill -STOP "$vol"; sleep 0.3
    kill -STOP "$nm"; sleep 0.1
    kill -CONT "$nm"; kill -CONT "$vol"
    n=$((n + 1)); echo "pulse $n"
  else
    sleep 0.01
  fi
done
"""
_BTRFS_SEND_INTERRUPT_END = r"""
stop={stop}
touch "$stop"
for _ in $(seq 50); do [ -e "$stop" ] || break; sleep 0.1; done
rm -f "$stop"
for d in /proc/[0-9]*; do
  case "$(cat "$d/comm" 2>/dev/null)" in
    space-nodemgr-s | space-volmgr-sv) kill -CONT "${{d#/proc/}}" ;;
  esac
done
true
"""
# The rootfs object each suspend codec publishes under the snapshot's storage
# prefix (`SnapshotArtifact` in crates/libs/common/core/src/types/object_store.rs).
SUSPEND_ROOTFS_OBJECTS = {BTRFS_SEND_V1: ROOTFS_SEND_FILE, "full": "rootfs"}
# The AWS CLI's refusal when nothing listens on the endpoint.
_OBJECT_STORE_UNREACHABLE = "Could not connect to the endpoint URL"
RESTORE_FAILED_EVENT = "snapshot_restore_failed"
# `space-volmgr jobs` prints a comfy-table: `│ <id> ┆ <status> ┆ ...`.
_TRANSFER_JOB_ROW = re.compile(r"│\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s*┆")
# A storage URI's query can carry endpoint credentials; log evidence keeps the
# scheme and drops the rest.
_STORAGE_URI = re.compile(r"s3://[^\s\"',}\]]*")
# `SPACE_TEMPLATE_RECEIVED_UUID_NS` (crates/libs/btrfs/src/lib.rs):
# a template installed from `(name, content_hash)` is stamped with
# `UUIDv5(NS, "name@content_hash")` so a btrfs-send stream produced against
# it on one node can be received against an independently installed copy
# on another. Pinned forever on the Rust side; mirrored here to check it.
TEMPLATE_RECEIVED_UUID_NAMESPACE = uuid.UUID("1f3a9c44-5e6b-47d8-b9c1-2a0f8e517d36")
SNAPSHOT_ROW_FACTS_SQL = """\
SELECT s.id,
       s.state,
       to_char(s."timestamp" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
       COALESCE(s.size_bytes::text, ''),
       COALESCE((SELECT MIN(storage_connection_string)
                   FROM snapshot_storage_locations
                  WHERE snapshot_id = s.id), '')
  FROM snapshots s
 WHERE s.sandbox_id = '{sandbox_id}'
   AND s.type = 'full'
 ORDER BY s."timestamp" ASC, s.id ASC;
"""
RELEASE_STORAGE_SELECT_SQL = """\
SELECT ssl.id,
       ssl.template_release_id,
       COALESCE(ssl.org_id::text, ''),
       ssl.storage_connection_string,
       to_char(ssl.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
  FROM template_release_storage_locations ssl
  JOIN template_releases r ON r.id = ssl.template_release_id
 WHERE r.template_name = '{template}';
"""
# The encrypted-template sidecar cascades on delete and cannot be put back;
# a template with one is outside what this fault may touch.
RELEASE_STORAGE_SIDECAR_COUNT_SQL = """\
SELECT COUNT(*)
  FROM template_release_storage_location_encrypted_fields
 WHERE template_release_storage_location_id IN ({ids});
"""
RELEASE_STORAGE_DELETE_SQL = """\
DELETE FROM template_release_storage_locations
 WHERE template_release_id IN (
       SELECT id FROM template_releases WHERE template_name = '{template}');
"""
# Put the row back whole: same id (the sidecar keys on it), org_id and
# created_at (the API orders locations by it).
RELEASE_STORAGE_INSERT_SQL = """\
INSERT INTO template_release_storage_locations
       (id, template_release_id, org_id, storage_connection_string, created_at)
VALUES ('{row_id}', '{release_id}', {org_id}, '{uri}', '{created_at}')
ON CONFLICT (template_release_id, storage_connection_string) DO UPDATE
    SET org_id = COALESCE(template_release_storage_locations.org_id, EXCLUDED.org_id);
"""
# sandbox-manager's per-sandbox cache root (`SANDBOX_CACHE_DIR` in
# common-core's linux paths); cold eviction removes `<root>/<sandbox uuid>`.
SANDBOX_CACHE_ROOT = "/space/cache/firecracker/sandboxes"
_COLD_EVICTION_DEFAULT_ENGAGE = 0.75
_COLD_EVICTION_DEFAULT_TARGET = 0.70
_COLD_EVICTION_DEFAULT_MIN_IDLE_SECONDS = 25 * 60 * 60
_COLD_EVICTION_DEFAULT_INTERVAL_SECONDS = 600
_REAPER_DEFAULT_ENGAGE = 0.50
_REAPER_DEFAULT_INTERVAL_SECONDS = 300
_REAPER_DEFAULT_GRACE_SECONDS = 1800
_REAPER_DEFAULT_IDLE_SECONDS = 86_400

# The reaper's own precondition, read from the control plane: a `ready`
# snapshot row for exactly this local artifact that has a live (not orphaned)
# storage location — the same predicate the fork store waits on.
BACKED_LOCAL_ARTIFACTS_SQL = """\
SELECT DISTINCT s.local_artifact_id
  FROM snapshots s
  JOIN snapshot_storage_locations l ON l.snapshot_id = s.id
 WHERE s.sandbox_id = '{sandbox_id}'::uuid
   AND s.local_artifact_id IS NOT NULL
   AND s.state = 'ready'
   AND l.orphaned_at IS NULL
 ORDER BY 1;
"""
ARTIFACT_BACKED_SQL = """\
SELECT EXISTS (
  SELECT 1
    FROM snapshots s
    JOIN snapshot_storage_locations l ON l.snapshot_id = s.id
   WHERE s.sandbox_id = '{sandbox_id}'::uuid
     AND s.local_artifact_id = '{local_artifact_id}'::uuid
     AND s.state = 'ready'
     AND l.orphaned_at IS NULL
);
"""
# How a NotFound from template-manager reaches us through the CLI: the CLI
# propagates the tonic `Status`, whose Display is `code: '<description>'`
# followed by the daemon's message. Match both the gRPC code text and the two
# daemon messages explicitly rather than a loose "not found" that would also
# swallow unrelated failures.
_TEMPLATE_NOT_FOUND_MARKERS = (
    "code: 'some requested entity was not found'",
    "no builds found for",
    "build not found:",
    "template not found",
)
_TEMPLATE_GC_DEFAULT_INTERVAL_SECONDS = 300
_TEMPLATE_GC_DEFAULT_GRACE_SECONDS = 1800
_TEMPLATE_GC_DEFAULT_HEADROOM = 0.9
_TEMPLATE_GC_DEFAULT_BUDGET_BYTES = 100 * 1024 * 1024 * 1024
_TEMPLATE_PREFETCH_DEFAULT_LATEST_N = 5

# The API's suspend-restore selector for a user restore (see
# `selected_suspend_restore_storage_uri` in
# crates/libs/api/.../store/restore_source.rs and the legacy suite's
# `wait_for_restore_affinity_portable`): a durable remote
# snapshot is selectable only when a `ready` full snapshot with a storage
# location is newer than the last placement change. Until then the planner
# has only a source node and pre-binds the pod with `spec.nodeName`, which
# bypasses kube-scheduler — and therefore any cordon.
RESTORE_PORTABLE_SQL = """\
SELECT EXISTS (
  SELECT 1
    FROM sandboxes s
   WHERE s.id = '{sandbox_id}'::uuid
     AND s.lifecycle IN ('suspended', 'restoring')
     AND EXISTS (
       SELECT 1
         FROM snapshots sn
         JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
        WHERE sn.sandbox_id = s.id
          AND sn.type = 'full'
          AND sn.state = 'ready'
          AND (s.placement_changed_at IS NULL OR sn.timestamp >= s.placement_changed_at)
     )
);
"""

# The only deployment env a retention scenario may touch, and the closed
# vocabulary its values come from. `set_retention_sweeper_env` refuses
# anything else before kubectl runs.
RETENTION_SWEEPER_ENV_KEYS: tuple[str, ...] = (
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED",
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN",
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS",
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS",
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N",
)
_RETENTION_BOOLEAN_ENV_KEYS = {
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED",
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN",
}
_RETENTION_BOOLEAN_ENV_VALUES = {
    "0",
    "1",
    "false",
    "False",
    "FALSE",
    "true",
    "True",
    "TRUE",
}
_RETENTION_ENV_VALUE = re.compile(r"[a-z0-9]{1,16}")
# Limiter values: booleans, small numbers, a redis:// URL, a client id.
_LIMITER_ENV_VALUE = re.compile(r"[A-Za-z0-9._:/@-]{1,64}")
_UNSIGNED_INTEGER = re.compile(r"\+?[0-9]+")


def _env_flag_default(value: str | None, default: bool) -> bool:
    """Match `env_flag`, the wider boolean vocabulary some controller knobs use."""

    normalized = (value or "").strip().lower()
    if value is not None and normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if value is not None and normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _literal_retention_env(entries: list[Any], source: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            continue
        name = entry["name"]
        value = entry.get("value")
        if name in RETENTION_SWEEPER_ENV_KEYS and (
            "valueFrom" in entry or not isinstance(value, str) or not value
        ):
            raise SmokeConfigurationError(
                f"cannot safely read nonliteral or empty {source} env {name}"
            )
        if isinstance(value, str):
            env[name] = value
    return env


def _retention_sweeper_config(env: Mapping[str, str]) -> RetentionSweeperConfig:
    """Parse as `ReconcilerConfig::from_environment` does, knob by knob."""

    min_age = env.get("SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS")
    return RetentionSweeperConfig(
        enabled=_truthy_default(env.get("SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED"), False),
        # A test hook read through `env_flag`; unset is live.
        dry_run=_env_flag_default(env.get("SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN"), False),
        keep_n=_unsigned_default(
            env.get("SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N"),
            3,
            maximum=(1 << 32) - 1,
            allow_zero=False,
        ),
        tick_seconds=_unsigned_default(
            env.get("SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS"),
            300,
            maximum=(1 << 64) - 1,
            allow_zero=False,
        ),
        # `snapshot_sweeper_grace_seconds` trims; zero disables the grace.
        min_age_seconds=_unsigned_default(
            min_age.strip() if min_age is not None else None,
            3600,
            maximum=(1 << 64) - 1,
            allow_zero=True,
        ),
    )


def _unsigned_default(
    raw: str | None,
    default: int,
    *,
    maximum: int,
    allow_zero: bool,
) -> int:
    if raw is None or _UNSIGNED_INTEGER.fullmatch(raw) is None:
        return default
    value = int(raw)
    if value > maximum or (not allow_zero and value == 0):
        return default
    return value


@dataclass(frozen=True)
class KindConfig:
    cluster: str = "space-local"
    api_url: str = "http://127.0.0.1:8080"
    data_plane_url: str = "http://127.0.0.1:8090"
    skip_bootstrap: bool = False
    bootstrap_timeout: int = 1800
    # Set in the scenarios the suite runner launches after its own bootstrap.
    suite_bootstrapped: bool = False

    @property
    def context(self) -> str:
        return f"kind-{self.cluster}"


NATIVE_OPERATION_AGE_ENV = "SPACE_PUBLIC_OPERATION_DISPATCHER_M1_MAX_AGE_SECS"
NATIVE_OPERATION_READY_ENVS = (
    "SPACE_CREATE_DATA_PLANE_READY_TIMEOUT_SECS",
    "SPACE_CREATE_COMMAND_READY_TIMEOUT_SECS",
)


class KindHarness:
    def __init__(
        self,
        executor: CommandExecutor,
        config: KindConfig,
        *,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        self.executor = executor
        self.config = config
        self.environment = dict(environment or os.environ)
        self._retention_lease_ownership: _LeaseOwnership | None = None
        # The controller Deployment and container the liveness-budget override
        # was written to, so its restore needs no lookup.
        self._liveness_budget_target: tuple[str, str] | None = None
        self._cordon_lease_ownership: _LeaseOwnership | None = None
        self._public_api_lease_ownership: _LeaseOwnership | None = None
        # The public API autoscaler's bounds before `scale_public_api` pinned
        # them: (name, uid, minReplicas, maxReplicas).
        self._public_api_autoscaler_bounds: tuple[str, str, int, int] | None = None

    def kubectl(
        self,
        *args: str,
        input_text: str | None = None,
        timeout: float | None = None,
        namespace: str = KIND_NAMESPACE,
    ) -> CommandResult:
        if namespace not in (KIND_NAMESPACE, KIND_SANDBOX_NAMESPACE):
            raise ValueError(f"refusing kubectl in namespace {namespace!r}")
        return self.executor.run(
            [
                "kubectl",
                "--context",
                self.config.context,
                "-n",
                namespace,
                *args,
            ],
            input_text=input_text,
            timeout=timeout,
        )

    def acquire_cold_create_setup(self) -> Callable[[], None]:
        # Share the existing Kind mutation Lease conservatively with retention
        # controls. It serializes cooperating smokes, not external administrators.
        return self.acquire_retention_sweeper_mutation()

    def admin_request(
        self,
        method: str,
        resource: str,
        body: dict[str, object] | None = None,
        *,
        expected: tuple[int, ...] = (200, 201, 202, 204),
    ) -> Any:
        self._require_isolated_kind("admin smoke operations")
        token_file = self.environment.get("SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE")
        if not token_file:
            raise SmokeConfigurationError("Kind admin operations require a token file")
        token = Path(token_file).read_text().strip()
        if not token:
            raise SmokeConfigurationError("Kind admin token is empty")
        request = Request(
            self.config.api_url.rstrip("/") + "/admin/api/v1/" + resource,
            method=method,
            headers={
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
                "Idempotency-Key": str(uuid.uuid4()),
            },
            data=json.dumps(body).encode() if body is not None else None,
        )
        try:
            try:
                response = urlopen(request, timeout=30)
            except HTTPError as error:
                response = error
            with response:
                if response.status not in expected:
                    raise SmokeConfigurationError(
                        f"Kind admin {method} {resource}: HTTP {response.status}"
                    )
                raw = response.read()
                return json.loads(raw) if raw else None
        except (OSError, ValueError, HTTPException):
            # Authentication and transport exceptions may contain secrets.
            raise SmokeConfigurationError(
                f"Kind admin {method} {resource} failed"
            ) from None

    def _owned_pool_setup(self) -> Any:
        from space_smoke.kind_cold_create import KindColdCreateSetup

        def assert_lease() -> None:
            if self._retention_lease_ownership is None:
                raise SmokeConfigurationError(
                    "cold setup requires the Kind mutation Lease"
                )
            self._assert_retention_lease_owned()

        assert_lease()
        if self.config.context != "kind-space-local" or urlsplit(
            self.config.api_url
        ).hostname not in {"localhost", "127.0.0.1"}:
            raise SmokeConfigurationError(
                "cold setup requires the isolated local Kind API"
            )
        token_file = self.environment.get("SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE")
        receipts = self.environment.get("SPACE_SMOKE_KIND_COLD_RECEIPTS")
        if not token_file or not receipts:
            raise SmokeConfigurationError(
                "cold setup requires an admin token file and durable receipt directory"
            )
        namespace = json.loads(
            _require_ok(
                self.kubectl("get", "namespace", "kube-system", "-o", "json"),
                "identify Kind cluster",
            )
        )

        return KindColdCreateSetup(
            self.admin_request, Path(receipts), namespace["metadata"]["uid"], assert_lease
        )

    def prepare_cold_create(
        self, template: str, config: KindColdCreateConfig
    ) -> ColdCreateSelection:
        return self._owned_pool_setup().prepare(template, config)

    def prepare_warm_pool(
        self, template: str, config: KindWarmPoolConfig
    ) -> WarmPoolSelection:
        return self._owned_pool_setup().prepare_warm(template, config)

    def cold_create_pod(self, sandbox_id: str) -> ColdCreatePod:
        """Observe on-demand pod provenance without changing pool admission."""
        prefix = "space.pplx.net/"
        result = self.kubectl(
            "get",
            "pods",
            "-l",
            f"{prefix}sandbox-id={sandbox_id}",
            "-o",
            "json",
            timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        _require_ok(result, "observe cold-create pod")
        pods = json.loads(result.stdout).get("items", [])
        if len(pods) != 1:
            return ColdCreatePod(sandbox_id, "", "", False, ("ambiguous-pod",))
        pod = pods[0]
        metadata = pod.get("metadata", {})
        labels = metadata.get("labels", {})
        annotations = metadata.get("annotations", {})
        # The pooled renderer adds these markers; adoption retains the pod.
        # Restore renderers add source markers even when generation is one.
        excluded = (
            "pool-slot-id",
            "pool-config-id",
            "pool-role",
            "checkpoint",
            "suspend-restore-storage-uri",
            "suspend-restore-source-node",
            "fork-disk-restore",
        )
        rejected = tuple(
            key
            for key in excluded
            if prefix + key in labels or prefix + key in annotations
        )
        identity_matches = all(
            values.get(prefix + "sandbox-id") == sandbox_id
            and values.get(prefix + "execution-generation") == "1"
            for values in (labels, annotations)
        )
        name, uid = metadata.get("name", ""), metadata.get("uid", "")
        cold = bool(
            identity_matches
            and name
            and uid
            and not rejected
            and not metadata.get("deletionTimestamp")
            and pod.get("spec", {}).get("runtimeClassName") == "space"
        )
        return ColdCreatePod(sandbox_id, name, uid, cold, rejected)

    def _warm_pool_pod(self, selector: str, value: str) -> WarmPoolCandidate:
        prefix = "space.pplx.net/"
        result = self.kubectl(
            "get",
            "pods",
            "-l",
            f"{prefix}{selector}={value},{prefix}pool-role=warm",
            "-o",
            "json",
            timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        _require_ok(result, "observe warm-pool pod")
        pods = [
            pod
            for pod in json.loads(result.stdout).get("items", [])
            if not pod.get("metadata", {}).get("deletionTimestamp")
        ]
        if len(pods) != 1:
            raise SmokeConfigurationError(
                "warm-pool proof requires exactly one matching live pod"
            )
        pod = pods[0]
        metadata = pod.get("metadata", {})
        labels = metadata.get("labels", {})
        annotations = metadata.get("annotations", {})
        sandbox_id = labels.get(prefix + "sandbox-id", "")
        config_id = labels.get(prefix + "pool-config-id", "")
        slot_id = annotations.get(prefix + "pool-slot-id", "")
        pod_name = metadata.get("name", "")
        pod_uid = metadata.get("uid", "")
        node_name = pod.get("spec", {}).get("nodeName", "")
        if not all(
            (sandbox_id, config_id, slot_id, pod_name, pod_uid, node_name)
        ):
            raise SmokeConfigurationError("warm-pool pod identity is incomplete")
        doctor = self.kubectl(
            "exec",
            self._node_shell_pod(node_name),
            "-c",
            KIND_NODE_SHELL_CONTAINER,
            "--",
            "/space/bin/space-sbxmgr",
            "doctor",
            sandbox_id,
            timeout=30,
        )
        output = _require_ok(doctor, "observe warm-pool sandbox state")
        match = re.search(r"^Doctor: .* \(([^)]+)\)$", output, re.MULTILINE)
        if match is None:
            raise SmokeConfigurationError(
                "warm-pool sandbox state output was not recognized"
            )
        return WarmPoolCandidate(
            sandbox_id,
            pod_name,
            pod_uid,
            config_id,
            slot_id,
            node_name,
            match.group(1),
        )

    def warm_pool_candidate(self, config_id: str) -> WarmPoolCandidate:
        return self._warm_pool_pod("pool-config-id", config_id)

    def warm_pool_claim_pod(self, sandbox_id: str) -> WarmPoolCandidate:
        return self._warm_pool_pod("sandbox-id", sandbox_id)

    def bootstrap(self) -> bool:
        """Bootstrap the stack; whether its image provenance check ran and passed.

        A `SKIP_BOOTSTRAP=1` bootstrap skips that check while Tilt is still
        updating, and the per-scenario recheck never runs it.
        """

        script = REPO_ROOT / "scripts/tests/lib/bootstrap-kind-stack.sh"
        environment = dict(self.environment)
        environment.update(
            {
                "REPO_ROOT": str(REPO_ROOT),
                "API_URL": self.config.api_url,
                "DATA_PLANE_URL": self.config.data_plane_url,
                "KIND_CLUSTER": self.config.cluster,
                "SKIP_BOOTSTRAP": "1" if self.config.skip_bootstrap else "0",
                "BOOTSTRAP_TIMEOUT": str(self.config.bootstrap_timeout),
            }
        )
        entry = "bootstrap_kind_stack"
        if self.config.skip_bootstrap and self.config.suite_bootstrapped:
            # The library's own split of what can change between scenarios.
            entry = "kind_stack_recheck_for_scenario"
        with tempfile.TemporaryDirectory(prefix="space-smoke-bootstrap-") as scratch:
            passed = Path(scratch) / "provenance-passed"
            environment["KIND_STACK_PROVENANCE_PASSED_FILE"] = str(passed)
            result = self.executor.run(
                ["bash", "-c", f"source {shlex.quote(str(script))}; {entry}"],
                env=environment,
                # The shell helper has bounded waits after its configurable API
                # deadline. Keep an outer subprocess deadline around all of them.
                timeout=(
                    self.config.bootstrap_timeout + KIND_BOOTSTRAP_POST_API_TIMEOUT_SECONDS
                ),
            )
            _require_ok(result, "kind stack bootstrap")
            return passed.exists()

    @staticmethod
    def api_key_hash(api_key: str) -> str:
        return hashlib.sha256(api_key.encode()).hexdigest()

    def _psql(self, sql: str, *, action: str) -> str:
        """Run one statement on the stack's Postgres; tuples-only output."""

        result = self.kubectl(
            "exec",
            "-i",
            "space-postgres-0",
            "--",
            # Loopback TLS, as kind_stack_psql connects: the server refuses
            # unencrypted TCP.
            "env",
            "PGHOST=127.0.0.1",
            "PGSSLMODE=require",
            "psql",
            "-U",
            "spaceuser",
            "-d",
            "spacedb",
            "-v",
            "ON_ERROR_STOP=1",
            "-tA",
            input_text=sql,
        )
        _require_ok(result, action)
        return result.stdout.strip()

    # Named privileged operations for retention scenarios; the SQL templates
    # are module constants so tests can pin their shape without a cluster.
    def _controller_env(self) -> dict[str, str]:
        result = self.kubectl(
            "get",
            "deploy",
            "-l",
            KIND_CONTROLLER_SELECTOR,
            "-o",
            "jsonpath={.items[0].spec.template.spec.containers[0].env}",
        )
        _require_ok(result, "read cluster-controller deployment env")
        return _literal_retention_env(json.loads(result.stdout or "[]"), "deployment")

    def _running_controller_envs(self) -> list[dict[str, str]]:
        """The literal env of every running, not-terminating controller pod."""

        result = self.kubectl(
            "get", "pods", "-l", KIND_CONTROLLER_SELECTOR, "-o", "json", timeout=15
        )
        _require_ok(result, "read cluster-controller pods")
        envs = []
        for pod in json.loads(result.stdout or "{}").get("items", []):
            metadata = pod.get("metadata") or {}
            if (pod.get("status") or {}).get("phase") != "Running" or metadata.get(
                "deletionTimestamp"
            ):
                continue
            containers = (pod.get("spec") or {}).get("containers") or [{}]
            envs.append(_literal_retention_env(containers[0].get("env") or [], "pod"))
        return envs

    def _controller_deployment(self) -> str:
        result = self.kubectl(
            "get",
            "deploy",
            "-l",
            KIND_CONTROLLER_SELECTOR,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        )
        name = _require_ok(result, "resolve cluster-controller deployment").strip()
        if not name:
            raise RuntimeError(
                "no deployment matched the cluster-controller kind selector"
            )
        return name

    def _internal_api_deployment(self) -> str:
        result = self.kubectl(
            "get", "deploy", "-l", KIND_INTERNAL_API_SELECTOR,
            "-o", "jsonpath={.items[0].metadata.name}",
        )
        name = _require_ok(result, "resolve internal-api deployment").strip()
        if not name:
            raise RuntimeError("no deployment matched the internal-api kind selector")
        return name

    def retention_sweeper_config(self) -> RetentionSweeperConfig:
        return _retention_sweeper_config(self._controller_env())

    def running_retention_sweeper_config(self) -> RetentionSweeperConfig | None:
        configs = {_retention_sweeper_config(env) for env in self._running_controller_envs()}
        return configs.pop() if len(configs) == 1 else None

    # ----- Cluster-visible mutation Leases --------------------------------------
    #
    # One Lease per (kind context, guarded resource). Retention guards the
    # cluster-controller deployment; the worker-cordon Lease guards node
    # schedulability. Cold-create shares retention's. The mechanics are shared
    # here so a new guarded resource is ~5 lines, not a copy of ~90.

    def _acquire_named_mutation_lease(
        self,
        name: str,
        subject: str,
        *,
        repair: Callable[[Mapping[str, object], _LeaseOwnership], None] | None = None,
    ) -> _LeaseOwnership:
        """Create or take over `name`; refuse while another live holder exists.

        `repair` runs with the previous document and the new ownership on every
        acquisition of an existing Lease, released or expired, and only AFTER
        the resourceVersion-guarded patch has succeeded. A holder whose cleanup
        failed releases with its records still on the Lease, so a released
        Lease is no proof that nothing is left to undo. Two contenders can both
        read the same Lease; running repairs before the patch would let the
        loser undo a fault the winner has since started. The callback must
        fence its own writes on `ownership`. A Lease with a repair may also be
        taken over from a holder `_holder_process_gone` proves dead, since the
        repair undoes what that holder left. If the repair fails, the Lease is
        released with its records for the next holder to retry.
        """

        holder = (
            f"uid={os.getuid()};pid={os.getpid()};host={socket.gethostname()};"
            f"token={secrets.token_hex(16)}"
        )
        acquired_at = datetime.now(UTC)
        spec = {
            "holderIdentity": holder,
            "leaseDurationSeconds": RETENTION_MUTATION_LEASE_DURATION_SECONDS,
            "acquireTime": _kube_timestamp(acquired_at),
            "renewTime": _kube_timestamp(acquired_at),
        }
        observed = self.kubectl("get", "lease", name, "-o", "json")
        if observed.returncode != 0:
            if "notfound" not in observed.detail().lower():
                raise SmokeConfigurationError(
                    f"could not inspect the {subject} Lease: "
                    f"{observed.detail() or '<no output>'}"
                )
            document = {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {"name": name, "namespace": KIND_NAMESPACE},
                "spec": spec,
            }
            created = self.kubectl(
                "create", "-f", "-", "-o", "json", input_text=json.dumps(document)
            )
            if created.returncode != 0:
                detail = created.detail()
                if "alreadyexists" in detail.lower():
                    raise SmokeConfigurationError(
                        f"another smoke holds the {subject} Lease on this kind cluster"
                    )
                raise SmokeConfigurationError(
                    f"could not create the {subject} Lease: {detail or '<no output>'}"
                )
            lease = _lease_document(created.stdout)
        else:
            lease = _lease_document(observed.stdout)
            metadata, current_spec = _lease_parts(lease)
            current_holder = current_spec.get("holderIdentity")
            if current_holder:
                duration = current_spec.get("leaseDurationSeconds")
                renewed = current_spec.get("renewTime") or current_spec.get(
                    "acquireTime"
                )
                if not isinstance(duration, int) or duration <= 0:
                    raise SmokeConfigurationError(
                        f"existing {subject} Lease has an unsafe duration"
                    )
                if acquired_at < _parse_kube_timestamp(renewed) + timedelta(
                    seconds=duration
                ) and not (repair is not None and _holder_process_gone(current_holder)):
                    raise SmokeConfigurationError(
                        f"another smoke holds the {subject} Lease on this kind "
                        f"cluster (owner {current_holder})"
                    )
            previous = lease
            lease = self._replace_lease_spec(name, metadata, spec, subject=subject)

        metadata, _ = _lease_parts(lease)
        ownership = _LeaseOwnership(
            name=name, uid=metadata["uid"], holder=holder, subject=subject
        )
        if observed.returncode == 0 and repair is not None:
            try:
                repair(previous, ownership)
            except BaseException:
                with contextlib.suppress(Exception):
                    self._release_lease(ownership)
                raise
        return ownership

    def _release_lease(self, ownership: _LeaseOwnership) -> None:
        # Bounded: it runs in scenario cleanup, after compensations that may
        # have spent most of its shared budget.
        current = self.kubectl(
            "get", "lease", ownership.name, "-o", "json", timeout=CLEANUP_REQUEST_SECONDS
        )
        if current.returncode != 0:
            raise RuntimeError(
                f"could not inspect the {ownership.subject} Lease during release: "
                f"{current.detail() or '<no output>'}"
            )
        metadata, spec = _lease_parts(_lease_document(current.stdout))
        if (
            metadata["uid"] != ownership.uid
            or spec.get("holderIdentity") != ownership.holder
        ):
            raise RuntimeError(
                f"refusing to release {ownership.subject} Lease owned by another caller"
            )
        released = dict(spec)
        released["holderIdentity"] = ""
        self._replace_lease_spec(
            ownership.name, metadata, released, subject=ownership.subject,
            timeout=CLEANUP_REQUEST_SECONDS,
        )

    def _assert_lease_owned(
        self, ownership: _LeaseOwnership, action: str, *, timeout: float | None = None
    ) -> tuple[dict[str, object], dict[str, object]]:
        current = self.kubectl("get", "lease", ownership.name, "-o", "json", timeout=timeout)
        if current.returncode != 0:
            raise SmokeConfigurationError(
                f"{ownership.subject} Lease cannot be verified before {action}: "
                f"{current.detail() or '<no output>'}"
            )
        metadata, spec = _lease_parts(_lease_document(current.stdout))
        duration = spec.get("leaseDurationSeconds")
        renewed = spec.get("renewTime") or spec.get("acquireTime")
        if (
            metadata["uid"] != ownership.uid
            or spec.get("holderIdentity") != ownership.holder
            or not isinstance(duration, int)
            or duration <= 0
            or datetime.now(UTC)
            >= _parse_kube_timestamp(renewed) + timedelta(seconds=duration)
        ):
            # Keep "live mutation Lease" contiguous: callers and their tests
            # match on that phrase regardless of which resource is guarded.
            raise SmokeConfigurationError(
                f"refusing {action} without the caller's live mutation Lease "
                f"({ownership.subject})"
            )
        return metadata, spec

    def _renew_lease(
        self,
        ownership: _LeaseOwnership,
        action: str,
        annotations: Mapping[str, str | None] | None = None,
    ) -> None:
        """Renew our live Lease and set or clear `annotations` in one guarded patch.

        Callers renew right before each write a successor could own. A runner
        stopped past its Lease's expiry may resume after a successor took over
        and repaired its records; the patch tests uid, resourceVersion and
        holder, so that runner fails here instead of undoing the successor's
        fault or erasing its records, and a renewed Lease cannot expire under
        the write that follows.
        """

        metadata, _ = self._assert_lease_owned(ownership, action)
        existing = metadata.get("annotations")
        existing = existing if isinstance(existing, dict) else None
        patch: list[dict[str, object]] = [
            {"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": metadata["resourceVersion"],
            },
            {"op": "test", "path": "/spec/holderIdentity", "value": ownership.holder},
            {
                "op": "add",
                "path": "/spec/renewTime",
                "value": _kube_timestamp(datetime.now(UTC)),
            },
        ]
        for key, value in (annotations or {}).items():
            path = f"/metadata/annotations/{_json_pointer_token(key)}"
            if value is None:
                if existing is not None and key in existing:
                    patch.append({"op": "remove", "path": path})
                continue
            if existing is None:
                patch.append({"op": "add", "path": "/metadata/annotations", "value": {}})
                existing = {}
            patch.append({"op": "add", "path": path, "value": value})
        result = self.kubectl(
            "patch", "lease", ownership.name, "--type=json", "--patch", json.dumps(patch)
        )
        if result.returncode != 0:
            raise SmokeConfigurationError(
                f"{ownership.subject} Lease changed under {action}; refusing to write: "
                f"{result.detail() or '<no output>'}"
            )

    def _annotate_lease(self, name: str, key: str, value: str | None) -> None:
        assignment = f"{key}-" if value is None else f"{key}={value}"
        _require_ok(
            self.kubectl("annotate", "lease", name, assignment, "--overwrite"),
            f"annotate Lease {name}",
        )

    def _replace_lease_spec(
        self,
        name: str,
        metadata: Mapping[str, object],
        spec: Mapping[str, object],
        *,
        subject: str,
        timeout: float | None = None,
    ) -> dict[str, object]:
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": metadata["resourceVersion"],
            },
            {"op": "replace", "path": "/spec", "value": dict(spec)},
        ]
        result = self.kubectl(
            "patch",
            "lease",
            name,
            "--type=json",
            "--patch",
            json.dumps(patch),
            "-o",
            "json",
            timeout=timeout,
        )
        if result.returncode != 0:
            raise SmokeConfigurationError(
                f"{subject} Lease changed concurrently; refusing unsafe ownership: "
                f"{result.detail() or '<no output>'}"
            )
        return _lease_document(result.stdout)

    # ----- Retention: thin wrappers over the generic Lease -----------------------

    def acquire_retention_sweeper_mutation(self) -> Callable[[], None]:
        """Hold one cluster-visible Lease per kind context until release."""

        context_hash = hashlib.sha256(self.config.context.encode()).hexdigest()[:16]
        ownership = self._acquire_named_mutation_lease(
            f"space-smoke-retention-{context_hash}", "retention controller mutation"
        )
        self._retention_lease_ownership = ownership

        def release() -> None:
            self._release_lease(ownership)
            if self._retention_lease_ownership is ownership:
                self._retention_lease_ownership = None

        return release

    def _assert_retention_lease_owned(self) -> None:
        ownership = self._retention_lease_ownership
        if ownership is None:
            return
        self._assert_lease_owned(ownership, "a retention controller deployment write")

    def retention_sweeper_env(self) -> dict[str, str | None]:
        env = self._controller_env()
        return {key: env.get(key) for key in RETENTION_SWEEPER_ENV_KEYS}

    def set_retention_sweeper_env(self, overrides: Mapping[str, str | None]) -> None:
        unknown = sorted(set(overrides) - set(RETENTION_SWEEPER_ENV_KEYS))
        if unknown:
            raise ValueError(f"refusing non-sweeper env keys: {unknown}")
        for key, value in overrides.items():
            if value is None:
                continue
            valid_value = (
                value in _RETENTION_BOOLEAN_ENV_VALUES
                if key in _RETENTION_BOOLEAN_ENV_KEYS
                else _RETENTION_ENV_VALUE.fullmatch(value) is not None
            )
            if not valid_value:
                raise ValueError(f"refusing sweeper env value {value!r} for {key}")
        self._apply_retention_sweeper_env(overrides)

    def restore_retention_sweeper_env(self, standing: Mapping[str, str | None]) -> None:
        """Restore the literal values captured from the deployment.

        The controller treats unrecognized boolean strings as defaults, so
        restoration must preserve an existing value such as ``yes`` rather
        than applying the stricter vocabulary used for smoke overrides.
        """

        unknown = sorted(set(standing) - set(RETENTION_SWEEPER_ENV_KEYS))
        if unknown:
            raise ValueError(f"refusing non-sweeper env keys: {unknown}")
        self._apply_retention_sweeper_env(standing)

    def _apply_retention_sweeper_env(self, values: Mapping[str, str | None]) -> None:
        assignments = [
            f"{key}-" if values[key] is None else f"{key}={values[key]}"
            for key in RETENTION_SWEEPER_ENV_KEYS
            if key in values
        ]
        if not assignments:
            return
        self._assert_retention_lease_owned()
        deployment = self._controller_deployment()
        # At the charts' pace, not `_fast_rollout`'s: the sweeper runs on the
        # Lease holder, and surging every replica retires the old holder only
        # as the rollout ends, so the new knobs took hold later (on the kind
        # bench the Lease reached a new pod ~9 s after the write, against
        # ~6.5 s) even though this wait, which does not drain, returned sooner.
        _require_ok(
            self.kubectl("set", "env", f"deploy/{deployment}", *assignments),
            "set retention sweeper env",
        )
        _require_ok(
            self.kubectl(
                "rollout",
                "status",
                f"deploy/{deployment}",
                "--timeout=180s",
                timeout=200,
            ),
            "await cluster-controller rollout",
        )

    def ready_full_snapshots(self, sandbox_id: str) -> list[SnapshotRecord]:
        rows = self._psql(
            READY_FULL_SNAPSHOTS_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="list ready full snapshots",
        )
        records = []
        for line in rows.splitlines():
            snapshot_id, artifact_id, artifact_kind, storage_uri, location_count = (
                line.split("|", 4)
            )
            records.append(
                SnapshotRecord(
                    snapshot_id=snapshot_id,
                    artifact_id=artifact_id,
                    artifact_kind=artifact_kind,
                    storage_uri=storage_uri,
                    storage_location_count=int(location_count),
                )
            )
        return records

    def pin_current_artifact(self, sandbox_id: str, record: SnapshotRecord) -> bool:
        updated = self._psql(
            PIN_CURRENT_ARTIFACT_SQL.format(
                sandbox_id=_sql_uuid(sandbox_id),
                artifact_id=_sql_uuid(record.artifact_id),
                artifact_kind=_sql_identifier_ish(record.artifact_kind),
            ),
            action="pin current artifact marker",
        )
        return bool(updated)

    def snapshot_exists(self, snapshot_id: str) -> bool:
        return bool(
            self._psql(
                SNAPSHOT_EXISTS_SQL.format(snapshot_id=_sql_uuid(snapshot_id)),
                action="probe snapshot row",
            )
        )

    def snapshot_storage_exists(
        self, record: SnapshotRecord, *, timeout: float
    ) -> bool:
        """Whether the selected storage prefix still contains an object."""

        bucket, prefix, endpoint, environment = self._snapshot_storage_target(record)
        result = self.executor.run(
            [
                "aws",
                "--no-cli-pager",
                "--endpoint-url",
                endpoint,
                "s3api",
                "list-objects-v2",
                "--bucket",
                bucket,
                "--prefix",
                prefix,
                "--max-keys",
                "1",
                "--output",
                "json",
            ],
            env=environment,
            timeout=timeout,
        )
        output = _require_ok(result, "probe snapshot storage prefix")
        try:
            return int(json.loads(output).get("KeyCount", 0)) > 0
        except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "object-store prefix probe returned an invalid listing: "
                f"snapshot_id={record.snapshot_id}"
            ) from error

    def delete_snapshot_storage(self, record: SnapshotRecord) -> None:
        """Delete one scenario snapshot prefix to inject a post-reap crash."""

        bucket, prefix, endpoint, environment = self._snapshot_storage_target(record)
        validate_loopback_environment(
            endpoint,
            self.environment.get("SPACE_SMOKE_ALLOW_REMOTE_S3_DELETE", "0") == "1",
            scenario_id="local.retention_claim_finish_forward",
            endpoint_name="S3_ENDPOINT",
            remote_override_name="SPACE_SMOKE_ALLOW_REMOTE_S3_DELETE",
        )
        result = self.executor.run(
            [
                "aws",
                "--no-cli-pager",
                "--endpoint-url",
                endpoint,
                "s3",
                "rm",
                f"s3://{bucket}/{prefix}",
                "--recursive",
            ],
            env=environment,
            timeout=30,
        )
        _require_ok(result, "inject a crash after snapshot byte deletion")

    def _snapshot_storage_target(
        self, record: SnapshotRecord | SnapshotRowFacts
    ) -> tuple[str, str, str, dict[str, str | None]]:
        parsed = urlsplit(record.storage_uri)
        raw_prefix = parsed.path.lstrip("/").rstrip("/")
        if parsed.scheme != "s3" or not parsed.netloc or not raw_prefix:
            raise SmokeConfigurationError(
                "snapshot storage URI is not a scoped s3 prefix: "
                f"snapshot_id={record.snapshot_id}"
            )
        prefix = f"{raw_prefix}/"
        region = parse_qs(parsed.query).get("region", ["us-east-1"])[0]
        endpoint = self.environment.get("S3_ENDPOINT", "http://127.0.0.1:8333")
        environment = {
            "AWS_SESSION_TOKEN": None,
            "AWS_SECURITY_TOKEN": None,
            "AWS_PROFILE": None,
            "AWS_DEFAULT_PROFILE": None,
            "AWS_WEB_IDENTITY_TOKEN_FILE": None,
            "AWS_ROLE_ARN": None,
            "AWS_ROLE_SESSION_NAME": None,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_ACCESS_KEY_ID": self.environment.get(
                "SPACE_SMOKE_S3_ACCESS_KEY_ID", "localadmin"
            ),
            "AWS_SECRET_ACCESS_KEY": self.environment.get(
                "SPACE_SMOKE_S3_SECRET_ACCESS_KEY", "localadmin-dev-secret"
            ),
            "AWS_REGION": region,
            "AWS_DEFAULT_REGION": region,
            "AWS_EC2_METADATA_DISABLED": "true",
        }
        return parsed.netloc, prefix, endpoint, environment

    def mark_snapshot_deleting(self, snapshot_id: str) -> bool:
        updated = self._psql(
            MARK_SNAPSHOT_DELETING_SQL.format(snapshot_id=_sql_uuid(snapshot_id)),
            action="inject a crash-orphaned prune claim",
        )
        return bool(updated)

    def deleting_snapshots(self, sandbox_id: str) -> list[str]:
        rows = self._psql(
            DELETING_SNAPSHOTS_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="list parked prune claims",
        )
        return [line for line in rows.splitlines() if line]

    def seed_api_key(
        self,
        api_key: str,
        *,
        org_id: str = "00000000-0000-0000-0000-000000000001",
        user_id: str = "00000000-0000-0000-0000-000000000010",
    ) -> None:
        key_hash = self.api_key_hash(api_key)
        sql = f"""\
INSERT INTO api_keys (org_id, name, key_hash, created_by)
VALUES ('{_sql_uuid(org_id)}', 'space-smoke', '{key_hash}', '{_sql_uuid(user_id)}')
ON CONFLICT (key_hash) DO NOTHING;
"""
        self._psql(sql, action="seed run-scoped ECW API key")

    def delete_api_key(self, api_key: str) -> None:
        key_hash = self.api_key_hash(api_key)
        self._psql(
            f"DELETE FROM api_keys WHERE key_hash = '{key_hash}';\n",
            action="delete run-scoped ECW API key",
        )

    # ----- NodeFaultControl / RestoreStateRead / RestorePodRead -----------------
    #
    # The cross-node durable-restore scenario forces a restore off the node
    # that holds the suspended bytes. Kubernetes offers no per-sandbox "restore
    # elsewhere" hint — cordon and drain are the only placement levers — so on
    # a disposable kind cluster the scenario cordons the source worker for the
    # restore window. Nothing here is reachable from the deployed backend.

    def acquire_worker_cordon_mutation(self) -> Callable[[], None]:
        context_hash = hashlib.sha256(self.config.context.encode()).hexdigest()[:16]
        name = f"space-smoke-cordon-{context_hash}"

        def repair_orphaned_cordon(
            previous: Mapping[str, object], ownership: _LeaseOwnership
        ) -> None:
            # A previous holder that died, or whose uncordon failed, left the
            # node it cordoned recorded; nothing in SPACE ever uncordons, so
            # undo it. This runs only after we own the Lease, and re-checks
            # that ownership right before the write, so a contender that lost
            # the takeover can never uncordon the winner's fault.
            metadata = previous.get("metadata")
            annotations = (
                metadata.get("annotations") if isinstance(metadata, dict) else None
            )
            recorded = (
                annotations.get(CORDONED_NODE_ANNOTATION)
                if isinstance(annotations, dict)
                else None
            )
            if isinstance(recorded, str) and recorded:
                self._assert_lease_owned(ownership, "an orphaned-cordon repair")
                _require_ok(
                    self.kubectl("uncordon", _kube_node_name(recorded), timeout=30),
                    f"uncordon kube node {recorded} orphaned by an expired smoke Lease",
                )
                self._annotate_lease(name, CORDONED_NODE_ANNOTATION, None)

        ownership = self._acquire_named_mutation_lease(
            name, "worker cordon mutation", repair=repair_orphaned_cordon
        )
        self._cordon_lease_ownership = ownership

        def release() -> None:
            self._release_lease(ownership)
            if self._cordon_lease_ownership is ownership:
                self._cordon_lease_ownership = None

        return release

    def cordon_kube_node(self, node_name: str) -> None:
        ownership = self._cordon_lease_ownership
        if ownership is None:
            raise SmokeConfigurationError(
                "refusing a worker cordon without the caller's live mutation Lease"
            )
        node = _kube_node_name(node_name)
        self._assert_lease_owned(ownership, f"cordoning {node}")
        # Record first: if we die between here and the patch, the next holder
        # over-cleans (uncordon is idempotent) rather than leaving a node stuck.
        self._annotate_lease(ownership.name, CORDONED_NODE_ANNOTATION, node)
        _require_ok(self.kubectl("cordon", node, timeout=30), f"cordon kube node {node}")

    def uncordon_kube_node(self, node_name: str) -> None:
        node = _kube_node_name(node_name)
        _require_ok(
            self.kubectl("uncordon", node, timeout=30), f"uncordon kube node {node}"
        )
        ownership = self._cordon_lease_ownership
        if ownership is not None:
            self._annotate_lease(ownership.name, CORDONED_NODE_ANNOTATION, None)

    def sandbox_pod_placement(self, sandbox_id: str) -> SandboxPodPlacement | None:
        """Read the sandbox pod's bound kube node and execution generation."""

        result = self.kubectl(
            "get",
            "pods",
            "-l",
            f"space.pplx.net/sandbox-id={_sql_uuid(sandbox_id)}",
            "-o",
            "json",
            timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        _require_ok(result, "observe sandbox pod placement")
        pods = [
            pod
            for pod in json.loads(result.stdout or "{}").get("items", [])
            if pod.get("spec", {}).get("nodeName")
        ]
        if len(pods) != 1:
            return None
        pod = pods[0]
        metadata = pod.get("metadata", {})
        annotations = metadata.get("annotations", {}) or {}
        return SandboxPodPlacement(
            sandbox_id=sandbox_id,
            pod_name=str(metadata.get("name", "")),
            pod_uid=str(metadata.get("uid", "")),
            node_name=str(pod["spec"]["nodeName"]),
            execution_generation=str(
                annotations.get(EXECUTION_GENERATION_ANNOTATION, "")
            ),
        )

    def restore_portable(self, sandbox_id: str) -> bool:
        """The API's own suspend-restore selection predicate, read directly."""

        value = self._psql(
            RESTORE_PORTABLE_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="check restore portability",
        ).strip()
        if value not in ("t", "f"):
            raise SmokeConfigurationError(
                f"unexpected restore portability query result: {value!r}"
            )
        return value == "t"

    def _worker_sandbox_lines(
        self,
        canonical_sandbox_id: str,
        *,
        node_names: Collection[str] | None,
        since_time: str | None,
    ) -> list[tuple[str, str]]:
        """(node, line) for every worker sandbox-manager log line naming the sandbox.

        `since_time` bounds the read so a long-lived bench does not scan history.
        """

        listing = self.kubectl(
            "get", "pods", "-l", _node_service_selector(NODE_SANDBOX_MANAGER),
            "-o", "json", timeout=15,
        )
        _require_ok(listing, "list worker sandbox-manager pods")
        wanted = set(node_names) if node_names is not None else None
        lines: list[tuple[str, str]] = []
        for pod in json.loads(listing.stdout or "{}").get("items", []):
            pod_name = pod.get("metadata", {}).get("name", "")
            node_name = pod.get("spec", {}).get("nodeName", "")
            if not pod_name or (wanted is not None and node_name not in wanted):
                continue
            args = ["logs", pod_name, "-c", NODE_SANDBOX_MANAGER]
            args += ["--since-time", since_time] if since_time else ["--tail=50000"]
            logs = self.kubectl(*args, timeout=60)
            _require_ok(logs, f"read sandbox-manager logs on {node_name or pod_name}")
            lines += [
                (node_name, line)
                for line in logs.stdout.splitlines()
                if canonical_sandbox_id in line
            ]
        return lines

    def _sandbox_events(
        self,
        canonical_sandbox_id: str,
        event_type: str,
        *,
        node_names: Collection[str] | None,
        since_time: str | None,
    ) -> list[tuple[str, dict[str, Any]]]:
        """(node, event) for every structured sandbox event of one type.

        Sandbox events are a documented contract (`crates/libs/events/README.md`);
        callers poll for one and judge its fields, never its absence or count.
        """

        needle = f'"{event_type}"'
        events: list[tuple[str, dict[str, Any]]] = []
        for node_name, line in self._worker_sandbox_lines(
            canonical_sandbox_id, node_names=node_names, since_time=since_time
        ):
            if needle not in line:
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if (
                isinstance(event, dict)
                and event.get("event_type") == event_type
                and event.get("sandbox_id") == canonical_sandbox_id
            ):
                events.append((node_name, event))
        return events

    def sandbox_manager_log(
        self,
        sandbox_id: str,
        *,
        node_names: Collection[str] | None = None,
        since_time: str,
    ) -> list[str]:
        """The worker sandbox-managers' lines naming the sandbox, for failure evidence."""

        lines = self._worker_sandbox_lines(
            _sql_uuid(sandbox_id), node_names=node_names, since_time=since_time
        )
        return [f"{node}: {_redact_storage_uris(line)[:400]}" for node, line in lines][-40:]

    def restore_activations(
        self,
        sandbox_id: str,
        *,
        node_names: Collection[str] | None = None,
        since_time: str | None = None,
    ) -> list[RestoreActivation]:
        """Parse `runtime_activated` sandbox events from worker sandbox-manager logs."""

        canonical = _sql_uuid(sandbox_id)
        return [
            RestoreActivation(
                sandbox_id=canonical,
                node_name=node_name,
                boot_type=str(event.get("boot_type", "")),
                restore_provenance=str(event.get("restore_provenance", "")),
                restore_storage_uri_present=bool(
                    event.get("restore_storage_uri_present", False)
                ),
                execution_generation=str(event.get("execution_generation", "")),
            )
            for node_name, event in self._sandbox_events(
                canonical, "runtime_activated", node_names=node_names, since_time=since_time
            )
        ]

    def restore_failures(self, sandbox_id: str, *, since_time: str) -> list[RestoreFailure]:
        canonical = _sql_uuid(sandbox_id)
        return [
            RestoreFailure(
                node_name=node,
                operation=str(event.get("operation", "")),
                reason=str(event.get("reason", "")),
                error_kind=str(event.get("error_kind", "")),
                error=_redact_storage_uris(str(event.get("error", ""))),
                restore_codec=str(event.get("restore_codec", "")),
            )
            for node, event in self._sandbox_events(
                canonical, RESTORE_FAILED_EVENT, node_names=None, since_time=since_time
            )
        ]

    # ----- Template cache eviction: named on-node operations ------------------
    # Each of these acts on one worker node: the operator shell runs the
    # commands, and the owning service's pod supplies the logs and the knobs.
    # The eviction knobs are read once at daemon start, so nothing here
    # mutates them: the scenario reads what the DaemonSet rendered and plants a
    # build the loop must reclaim.

    def _node_service_pod(
        self, node_name: str, component: str, *, role: str = KIND_WORKER_ROLE
    ) -> str:
        """The one pod of a node service on a node."""

        result = self.kubectl(
            "get",
            "pods",
            "-l",
            _node_service_selector(component, role=role),
            "--field-selector",
            f"spec.nodeName={_kube_node_name(node_name)}",
            "-o",
            "jsonpath={.items[*].metadata.name}",
            timeout=15,
        )
        names = _require_ok(result, f"find the {component} pod on {node_name}").split()
        if len(names) != 1:
            raise SmokeConfigurationError(
                f"expected one {component} pod on {node_name}, found {names}"
            )
        return names[0]

    def ready_node_service_pod(
        self, node_name: str, component: str, *, role: str = KIND_WORKER_ROLE
    ) -> str | None:
        """The node service's pod on a node, if exactly one is Ready."""

        result = self.kubectl(
            "get",
            "pods",
            "-l",
            _node_service_selector(component, role=role),
            "--field-selector",
            f"spec.nodeName={_kube_node_name(node_name)}",
            "-o",
            "json",
            timeout=15,
        )
        ready = _ready_pod_names(
            _require_ok(result, f"list {component} pods on {node_name}"), excluding=""
        )
        return ready[0] if len(ready) == 1 else None

    def _node_shell_pod(self, node_name: str) -> str:
        result = self.kubectl(
            "get",
            "pods",
            "-l",
            KIND_NODE_SHELL_SELECTOR,
            "--field-selector",
            f"spec.nodeName={_kube_node_name(node_name)}",
            "-o",
            "jsonpath={.items[*].metadata.name}",
            timeout=15,
        )
        names = _require_ok(result, f"find the operator shell on {node_name}").split()
        if len(names) != 1:
            raise SmokeConfigurationError(
                f"expected one space-node-debug pod on {node_name}, found {names}"
            )
        return names[0]

    def _node_service_logs(
        self,
        node_name: str,
        component: str,
        *,
        role: str = KIND_WORKER_ROLE,
        since_time: str | None = None,
        tail: int | None = None,
    ) -> str:
        pod = self._node_service_pod(node_name, component, role=role)
        args = ["logs", pod, "-c", component]
        if since_time:
            args += ["--since-time", since_time]
        if tail is not None:
            args.append(f"--tail={int(tail)}")
        logs = self.kubectl(*args, timeout=60)
        return _require_ok(logs, f"read {component} logs on {node_name}")

    def _node_service_env(
        self, node_name: str, component: str
    ) -> dict[str, str | None]:
        """Literal env of a node service's container on one running pod.

        The pod, not the DaemonSet spec: daemons read their knobs once at
        start, and mid-rollout the spec describes a pod that does not exist
        yet on this node.
        """

        pod = self._node_service_pod(node_name, component)
        result = self.kubectl("get", "pod", pod, "-o", "json", timeout=15)
        _require_ok(result, f"read {component} pod {pod}")
        containers = json.loads(result.stdout or "{}").get("spec", {}).get(
            "containers", []
        )
        return {
            entry["name"]: entry.get("value")
            for container in containers
            if container.get("name") == component
            for entry in container.get("env", [])
            if isinstance(entry, dict) and isinstance(entry.get("name"), str)
        }

    def metric_export_interval_seconds(self, node_name: str, component: str) -> float:
        """The OTLP metric export interval a node service's pod on `node_name` runs with.

        `OTEL_METRIC_EXPORT_INTERVAL` in milliseconds; unset, the SDKs' 10 s.
        """

        raw = self._node_service_env(node_name, component).get(OTEL_METRIC_EXPORT_INTERVAL_ENV)
        if raw is None or not raw.strip():
            return OTEL_METRIC_EXPORT_DEFAULT_SECONDS
        try:
            millis = int(raw.strip())
        except ValueError:
            raise SmokeConfigurationError(
                f"{component} on {node_name} sets {OTEL_METRIC_EXPORT_INTERVAL_ENV}={raw!r}, not milliseconds"
            ) from None
        if millis <= 0:
            raise SmokeConfigurationError(
                f"{component} on {node_name} sets {OTEL_METRIC_EXPORT_INTERVAL_ENV}={raw!r}, not positive"
            )
        return millis / 1000

    def template_cache_gc_config(self, node_name: str) -> TemplateCacheGcConfig:
        return _template_cache_gc_config_from_env(
            self._node_service_env(node_name, NODE_TEMPLATE_MANAGER)
        )

    def cold_eviction_config(self, node_name: str) -> ColdEvictionConfig:
        return _cold_eviction_config_from_env(
            self._node_service_env(node_name, NODE_SANDBOX_MANAGER)
        )

    def cluster_deleted_quiescence_seconds(self, node_name: str) -> int:
        # `cluster_deleted_eviction_quiescence`: an unset, unparsable or zero
        # value keeps the 600 s default.
        raw = str(
            self._node_service_env(node_name, NODE_SANDBOX_MANAGER).get(
                "SPACE_SBX_CLUSTER_DELETED_QUIESCENCE_SECS"
            )
            or ""
        )
        return int(raw) if raw.isdigit() and int(raw) > 0 else 600

    def sandbox_cache_dir_present(self, node_name: str, sandbox_id: str) -> bool:
        path = f"{SANDBOX_CACHE_ROOT}/{_sql_uuid(sandbox_id)}"
        # `test -e` exits 1 for "absent" and 0 for "present"; anything else is
        # the exec itself failing and must not read as "gone".
        result = self._node_shell_exec(
            node_name, f"test -e {path}; echo $?", timeout=30
        )
        if result.returncode != 0 or result.stdout.strip() not in ("0", "1"):
            raise RuntimeError(
                f"could not probe {path} on {node_name} ({result.returncode}): "
                f"{result.detail() or '<no output>'}"
            )
        return result.stdout.strip() == "0"

    def snapshot_reaper_config(self, node_name: str) -> SnapshotReaperConfig:
        return _snapshot_reaper_config_from_env(
            self._node_service_env(node_name, NODE_SANDBOX_MANAGER)
        )

    def sandbox_canonical_snapshot_entries(self, node_name: str, sandbox_id: str) -> tuple[str, ...]:
        """Entries of the sandbox's canonical suspend snapshot directory, dotfiles included."""

        path = f"{SANDBOX_CACHE_ROOT}/{_sql_uuid(sandbox_id)}/snapshot"
        result = self._node_shell_exec(
            node_name,
            f"if [ ! -e {path} ]; then exit 0; fi; ls -1A {path}",
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"could not list {path} on {node_name} ({result.returncode}): "
                f"{result.detail() or '<no output>'}"
            )
        return tuple(line.strip() for line in result.stdout.splitlines() if line.strip())

    def transfer_jobs(self, node_name: str, sandbox_id: str) -> list[TransferJobView]:
        subject = shlex.quote(f"sandbox:{_sql_uuid(sandbox_id)}")
        listing = _require_ok(
            self._node_shell_exec(node_name, f"space-volmgr jobs --subject {subject}", timeout=30),
            f"list volume-manager transfer jobs on {node_name}",
        )
        # A row's first cell is the job id; the header and borders hold none.
        job_ids = [
            match.group(1)
            for match in (_TRANSFER_JOB_ROW.match(line.strip()) for line in listing.splitlines())
            if match
        ]
        jobs: list[TransferJobView] = []
        for job_id in job_ids:
            shown = _require_ok(
                self._node_shell_exec(node_name, f"space-volmgr job {shlex.quote(job_id)}", timeout=30),
                f"show volume-manager transfer job {job_id} on {node_name}",
            )
            fields = {
                key.strip(): value.strip()
                for key, _, value in (line.partition(":") for line in shown.splitlines())
            }
            jobs.append(
                TransferJobView(
                    job_id=fields.get("id", job_id),
                    kind=fields.get("kind", ""),
                    status=fields.get("status", ""),
                    local_path=fields.get("local_path", ""),
                    error=fields.get("error", ""),
                    attempts=int(fields.get("attempts") or 0),
                )
            )
        return jobs

    def transfer_job_log(self, node_name: str, *, since_time: str) -> list[str]:
        logs = self._node_service_logs(node_name, NODE_VOLUME_MANAGER, since_time=since_time)
        return [
            _redact_storage_uris(line)[:600] for line in logs.splitlines() if "transfer job" in line
        ][-40:]

    def interrupt_btrfs_send(
        self, node_name: str, *, token: str, pulses: int, window_seconds: int
    ) -> list[str]:
        if pulses < 1 or not 0 < window_seconds <= 3600:
            raise ValueError(f"refusing {pulses} pulses over {window_seconds} s")
        script = _BTRFS_SEND_INTERRUPTER.format(
            stop=shlex.quote(_btrfs_send_interrupt_stop_file(token)),
            pulses=int(pulses),
            window=int(window_seconds),
            staging=shlex.quote(BTRFS_SEND_STAGING_ROOT),
        )
        result = self._node_shell_exec(node_name, script, timeout=window_seconds + 60)
        output = _require_ok(result, f"interrupt btrfs sends on {node_name}")
        return [line.strip() for line in output.splitlines() if line.strip()]

    def end_btrfs_send_interrupt(self, node_name: str, *, token: str) -> None:
        script = _BTRFS_SEND_INTERRUPT_END.format(
            stop=shlex.quote(_btrfs_send_interrupt_stop_file(token))
        )
        _require_ok(
            self._node_shell_exec(node_name, script, timeout=60),
            f"end the btrfs-send interrupter on {node_name}",
        )

    def sandbox_snapshot_dirs(self, node_name: str, sandbox_id: str) -> tuple[str, ...]:
        path = f"{SANDBOX_CACHE_ROOT}/{_sql_uuid(sandbox_id)}/snapshots"
        # A snapshots dir that does not exist is a legitimate "none". Any
        # other `ls` failure (it exits 2 for unreadable paths too) or an exec
        # failure must propagate rather than read as "reaped".
        result = self._node_shell_exec(
            node_name,
            f"if [ ! -e {path} ]; then exit 0; fi; ls -1 {path}",
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"could not list {path} on {node_name} ({result.returncode}): "
                f"{result.detail() or '<no output>'}"
            )
        return tuple(line.strip() for line in result.stdout.splitlines() if line.strip())

    def artifact_backed(self, sandbox_id: str, local_artifact_id: str) -> bool:
        value = self._psql(
            ARTIFACT_BACKED_SQL.format(
                sandbox_id=_sql_uuid(sandbox_id),
                local_artifact_id=_sql_uuid(local_artifact_id),
            ),
            action="check the reaped artifact's remote backup",
        ).strip()
        if value not in ("t", "f"):
            raise SmokeConfigurationError(f"unexpected artifact backup query result: {value!r}")
        return value == "t"

    def backed_local_artifacts(self, sandbox_id: str) -> tuple[str, ...]:
        rows = self._psql(
            BACKED_LOCAL_ARTIFACTS_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="list the sandbox's backed local artifacts",
        )
        return tuple(line.strip() for line in rows.splitlines() if line.strip())

    def reaper_loop_log(self, node_name: str, *, since_time: str) -> list[str]:
        logs = self._node_service_logs(
            node_name, NODE_SANDBOX_MANAGER, since_time=since_time
        )
        return [
            _redact_storage_uris(line)[:400] for line in logs.splitlines() if "snapshot reaper" in line
        ][-40:]

    def eviction_loop_log(self, node_name: str, *, since_time: str) -> list[str]:
        logs = self._node_service_logs(
            node_name, NODE_SANDBOX_MANAGER, since_time=since_time
        )
        return [
            _redact_storage_uris(line)[:400]
            for line in logs.splitlines()
            if "sandbox eviction" in line and "tick complete" not in line
        ][-40:]

    def template_gc_log(self, node_name: str, *, since_time: str) -> list[str]:
        logs = self._node_service_logs(
            node_name, NODE_TEMPLATE_MANAGER, since_time=since_time
        )
        return [
            _redact_storage_uris(line)[:400] for line in logs.splitlines() if "template GC" in line
        ][-40:]

    def require_encrypted_volume_support(self) -> None:
        """Reject missing host targets before creating an owned BYOK identity."""
        self._require_isolated_kind("encrypted volume preflight")
        for node in self.sandbox_pool_nodes():
            result = self.kubectl(
                "exec", self._node_shell_pod(node), "-c", KIND_NODE_SHELL_CONTAINER,
                "--", "dmsetup", "targets", timeout=15,
            )
            targets = {
                line.split()[0] for line in
                _require_ok(result, "inspect encrypted volume targets").splitlines()
                if line.split()
            }
            missing = {"crypt", "snapshot"} - targets
            if missing:
                raise SmokeConfigurationError(
                    f"Kind worker {node} lacks device-mapper targets "
                    + ", ".join(sorted(missing))
                    + "; load the host's dm_crypt and dm_snapshot modules before BYOK smoke"
                    " (`source scripts/lib/space-smoke-prereqs.sh && space_smoke_prepare_encrypted_targets`"
                    " on a host that has them; Devbox nodes do not load them)"
                )

    def encrypted_snapshot_observation(
        self, sandbox_id: str, node_name: str
    ) -> EncryptedSnapshotObservation:
        """Read one sealed BYOK snapshot volume without opening or mutating it."""

        self._require_isolated_kind("encrypted snapshot observation")
        canonical = _sql_uuid(sandbox_id)
        pod = self._node_shell_pod(node_name)
        path = f"/space/cache/firecracker/sandboxes/{canonical}/instance/snapshot.luks"
        luks_uuid = _require_ok(
            self.kubectl(
                "exec",
                pod,
                "-c",
                KIND_NODE_SHELL_CONTAINER,
                "--",
                "cryptsetup",
                "luksUUID",
                path,
                timeout=30,
            ),
            "read encrypted snapshot LUKS identity",
        ).strip()
        try:
            normalized_uuid = str(uuid.UUID(luks_uuid))
        except ValueError as error:
            raise SmokeConfigurationError(
                f"encrypted snapshot returned an invalid LUKS UUID: {luks_uuid!r}"
            ) from error

        encoded_size = _require_ok(
            self.kubectl(
                "exec",
                pod,
                "-c",
                KIND_NODE_SHELL_CONTAINER,
                "--",
                "stat",
                "-c",
                "%s %b",
                path,
                timeout=30,
            ),
            "read encrypted snapshot allocation",
        ).strip()
        fields = encoded_size.split()
        if len(fields) != 2:
            raise SmokeConfigurationError(
                f"encrypted snapshot stat returned an invalid shape: {encoded_size!r}"
            )
        try:
            logical_bytes, allocated_blocks = (int(field) for field in fields)
        except ValueError as error:
            raise SmokeConfigurationError(
                f"encrypted snapshot stat returned non-integer fields: {encoded_size!r}"
            ) from error
        if logical_bytes <= 0 or allocated_blocks <= 0:
            raise SmokeConfigurationError(
                f"encrypted snapshot stat returned unusable sizes: {encoded_size!r}"
            )
        return EncryptedSnapshotObservation(
            node_name=node_name,
            luks_uuid=normalized_uuid,
            logical_bytes=logical_bytes,
            allocated_bytes=allocated_blocks * 512,
        )

    def clear_completed_sandbox_pods(self, sandbox_id: str) -> tuple[str, ...]:
        """Clear Kind pods stuck after a successful node-side suspend teardown."""

        self._require_isolated_kind("completed sandbox pod cleanup")
        canonical = _sql_uuid(sandbox_id)
        deadline = time.monotonic() + 10
        while True:
            result = self.kubectl(
                "get",
                "pods",
                "-l",
                f"{SANDBOX_ID_LABEL}={canonical}",
                "-o",
                "json",
                timeout=15,
                namespace=KIND_SANDBOX_NAMESPACE,
            )
            payload = json.loads(_require_ok(result, "list completed sandbox pods"))
            items = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(items, list):
                raise SmokeConfigurationError(
                    "sandbox pod listing returned an invalid shape"
                )

            unsafe = [pod for pod in items if _completed_deleting_pod_name(pod) is None]
            if not unsafe:
                break
            if time.monotonic() >= deadline:
                raise SmokeConfigurationError(
                    "refusing to force-clear sandbox pods that did not become deleting "
                    "with every container terminated successfully"
                )
            time.sleep(0.25)

        cleared: list[str] = []
        for pod in items:
            name = _completed_deleting_pod_name(pod)
            assert name is not None
            _require_ok(
                self.kubectl(
                    "delete",
                    "pod",
                    _kube_pod_name(name),
                    "--grace-period=0",
                    "--force",
                    "--wait=false",
                    timeout=15,
                    namespace=KIND_SANDBOX_NAMESPACE,
                ),
                f"force-clear completed sandbox pod {name}",
            )
            cleared.append(name)
        return tuple(cleared)

    def _require_isolated_kind(self, action: str) -> None:
        if urlsplit(self.config.api_url).hostname not in {"localhost", "127.0.0.1"}:
            raise SmokeConfigurationError(f"{action} require the isolated local Kind API")
        if self.config.context == "kind-space-local":
            return
        # Named development benches must be explicitly pinned to their current
        # cluster identity before node-local mutations or admin fixture setup.
        expected_uid = self.environment.get("SPACE_SMOKE_KIND_CLUSTER_UID")
        if not expected_uid or not re.fullmatch(
            r"kind-(?:space-local-[a-z0-9]+(?:-[a-z0-9]+)*|space-oss-[0-9a-f]{10})",
            self.config.context,
        ):
            raise SmokeConfigurationError(f"{action} require the isolated local Kind API")
        result = self.kubectl(
            "get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}", timeout=15,
        )
        _require_ok(result, "verify isolated Kind cluster identity")
        if result.stdout.strip() != expected_uid:
            raise SmokeConfigurationError(f"{action} refused a changed Kind cluster identity")

    def _node_shell_exec(
        self, node_name: str, script: str, *, timeout: float = 120
    ) -> CommandResult:
        """Run a script in the node's operator shell, as root on the node."""

        self._require_isolated_kind("on-node operations")
        return self.kubectl(
            "exec",
            self._node_shell_pod(node_name),
            "-c",
            KIND_NODE_SHELL_CONTAINER,
            "--",
            "bash",
            "-c",
            script,
            timeout=timeout,
        )

    def publish_node_ctl_facet(self, node_name: str, facet: str) -> str | None:
        """Have node-ctl on `node_name` publish an outbound facet now (`space-nodectl facet publish`).

        Marks the whole facet dirty through node-ctl's own `MarkFacetDirty`, so
        its next outbound sync is the same publication its poll would make:
        `sandbox-inventory` a Full census, `template-inventory` with the node's
        Kubernetes object reconciles.

        Returns None once node-ctl has the request, or why it does not: a
        failed exec or RPC (node-ctl restarting, a kubectl blip) is the
        caller's to record and retry, since node-ctl's poll still publishes the
        facet. A bench that is not an isolated Kind stack, or a node without
        exactly one operator shell, still raises `SmokeConfigurationError`.
        """

        try:
            result = self._node_shell_exec(
                node_name, f"space-nodectl facet publish {shlex.quote(facet)}", timeout=30
            )
        except SmokeConfigurationError:
            raise
        except RuntimeError as error:
            return f"publish node-ctl facet {facet} on {node_name}: {error}"
        if result.returncode != 0:
            return (
                f"publish node-ctl facet {facet} on {node_name} failed "
                f"({result.returncode}): {result.detail() or '<no output>'}"
            )
        return None

    def register_unpinned_template(
        self, node_name: str, template: str, size_bytes: int
    ) -> None:
        name = _smoke_template_name(template)
        if size_bytes <= 0 or size_bytes > 8 * 1024**3:
            raise ValueError(f"refusing template size {size_bytes}")
        directory = f"{TEMPLATE_CACHE_ROOT}/{name}"
        rootfs = shlex.quote(f"{directory}/rootfs.ext4")
        # The flat layout `register` scans: `<name>/rootfs.ext4`. The daemon
        # copies it into a digest directory (plus a `rootfs.ext4.zst` it
        # writes for publishing), hashes it, and removes the flat source
        # itself. `register` ignores its `--tag`; nothing pins the result
        # because pins come only from `SPACE_TEMPLATE_PREFETCH_TAGS`, which
        # names seed and code-runtime.
        script = " && ".join(
            [
                "set -eu",
                f"install -d -m 2770 -o {TEMPLATE_CACHE_OWNER} -g {TEMPLATE_CACHE_GROUP} "
                f"{shlex.quote(directory)}",
                f"head -c {size_bytes} /dev/urandom > {rootfs}",
                f"chown {TEMPLATE_CACHE_OWNER}:{TEMPLATE_CACHE_GROUP} {rootfs}",
                f"chmod 0660 {rootfs}",
                f"space-tmplmgr register --template {name}",
                # `register` returns before the daemon's background job has
                # written `rootfs.ext4.zst`, re-stat'd the rootfs, rewritten
                # `manifest.json` and re-upserted the build row
                # (`spawn_template_compressed_rootfs_generation`). Anything a
                # scenario does to the plant before that lands — drop the row,
                # truncate the rootfs — would be undone by it. The manifest
                # listing the compressed sibling is the last file write; the
                # row upsert follows within the same task, so give it a beat.
                f"d=$(ls -d {shlex.quote(directory)}/*:* | head -n1)",
                "for _ in $(seq 1 360); do "
                'if grep -q rootfs.ext4.zst "$d/manifest.json" 2>/dev/null; then sleep 2; exit 0; fi; '
                "sleep 0.5; done",
                "echo 'register did not settle: manifest.json never listed rootfs.ext4.zst' >&2",
                "exit 1",
            ]
        )
        _require_ok(
            self._node_shell_exec(node_name, script, timeout=900),
            f"register throwaway template {name} on {node_name}",
        )

    def delete_template(self, node_name: str, template: str) -> None:
        name = _smoke_template_name(template)
        directory = shlex.quote(f"{TEMPLATE_CACHE_ROOT}/{name}")
        stash = shlex.quote(_template_stash_dir(name))
        # `delete-template` is the daemon's own removal (rows, files and the
        # `template_settings` row eviction leaves behind); once eviction has
        # removed the name dir it answers NotFound (`template not found`, via
        # the tonic code text), which is fine. Either way the name directory
        # the plant created is removed so no residue outlives the scenario.
        script = (
            f"out=$(space-tmplmgr delete-template --template {name} 2>&1); rc=$?; "
            'if [ "$rc" -ne 0 ] && ! printf %s "$out" | grep -qiF '
            '-e "some requested entity was not found" -e "template not found"; then '
            'printf %s "$out" >&2; exit "$rc"; fi; '
            f"rm -rf {directory} {stash}"
        )
        _require_ok(
            self._node_shell_exec(node_name, script, timeout=120),
            f"delete throwaway template {name} on {node_name}",
        )

    def protected_template_builds(
        self, node_name: str, sandbox_id: str, template_selector: str
    ) -> tuple[str, ...]:
        """The active sandbox build and selected release, resolved before planting."""
        name_and_tag, separator, digest = template_selector.partition("@")
        name, tagged, tag = name_and_tag.partition(":")
        name = _template_name(name)
        if separator:
            selector_arg = f"--content-hash {shlex.quote(_template_digest(digest))}"
        else:
            tag = tag if tagged else "latest"
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", tag):
                raise ValueError(f"refusing template tag {tag!r}")
            selector_arg = f"--tag {shlex.quote(tag)}"
        pods = self.kubectl(
            "get", "pods", "-l",
            f"space.pplx.net/sandbox-id={_sql_uuid(sandbox_id)}", "-o", "json",
            timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        document = json.loads(_require_ok(pods, "read active sandbox template build"))
        bound = [p for p in document.get("items", []) if p.get("spec", {}).get("nodeName")]
        if len(bound) != 1:
            raise RuntimeError("expected one bound sandbox pod for template protection")
        annotations = bound[0].get("metadata", {}).get("annotations", {}) or {}
        if annotations.get("space.pplx.net/template") != name:
            raise RuntimeError("sandbox pod template does not match the selected template")
        active = _template_digest(str(annotations.get("space.pplx.net/content-hash", "")))
        resolved = self._node_shell_exec(
            node_name, f"space-tmplmgr get --template {name} {selector_arg}", timeout=60
        )
        output = _require_ok(resolved, "resolve the selected protected template before planting")
        hashes = re.findall(r"^content_hash:\s*((?:sha256|blake3):[0-9a-f]{64})\s*$", output, re.MULTILINE)
        if len(hashes) != 1:
            raise RuntimeError("template resolve did not return one full content hash")
        return tuple(sorted({active, hashes[0]}))

    def template_build_state(
        self, node_name: str, template: str, content_hash: str
    ) -> str | None:
        # By exact content hash. A bare name always resolves through the
        # cluster (a tenant must authorize even a cache hit), which has no
        # release for a build registered only on this node; an exact hash
        # with no caller organization resolves from this node's cache. Either
        # way a successful resolution touches the build's LRU clock.
        name = _template_name(template)
        result = self._node_shell_exec(
            node_name,
            f"space-tmplmgr get --template {name} --content-hash {shlex.quote(content_hash)}",
            timeout=60,
        )
        if result.returncode != 0:
            detail = result.detail().lower()
            if any(marker in detail for marker in _TEMPLATE_NOT_FOUND_MARKERS):
                return None
            raise RuntimeError(
                f"space-tmplmgr get {name} failed ({result.returncode}): "
                f"{result.detail() or '<no output>'}"
            )
        for line in result.stdout.splitlines():
            key, _, value = line.partition(":")
            if key.strip() == "state":
                return value.strip() or None
        raise RuntimeError(f"space-tmplmgr get {name} printed no state line")

    def template_digest_dirs(self, node_name: str, template: str) -> tuple[str, ...]:
        name = _template_name(template)
        # `ls -d` exits 2 with no match, which is a legitimate "none"; anything
        # else (no shell, pod gone, exec refused) must not read as "gone".
        result = self._node_shell_exec(
            node_name,
            f"out=$(ls -d {TEMPLATE_CACHE_ROOT}/{name}/*:* 2>/dev/null); "
            'rc=$?; if [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; then exit "$rc"; fi; '
            'printf "%s\n" "$out"',
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"could not list digest directories for {name} on {node_name} "
                f"({result.returncode}): {result.detail() or '<no output>'}"
            )
        return tuple(
            line.rsplit("/", 1)[-1] for line in result.stdout.splitlines() if line.strip()
        )

    # --- btrfs-send codec failure matrix ---------------------------------

    def suspend_snapshot_template_hash(self, sandbox_id: str) -> str:
        digest = self._psql(
            SUSPEND_SNAPSHOT_TEMPLATE_HASH_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="read the suspend snapshot's parent template digest",
        ).strip()
        if not digest:
            raise RuntimeError(f"sandbox {sandbox_id} has no ready full snapshot row")
        return _template_digest(digest)

    def evict_template_from_worker(self, node_name: str, template: str) -> list[str]:
        name = _template_name(template)
        # The daemon's own removal: rows, files (subvolume-aware) and
        # settings. Prefetch will re-pull a pinned template on its next tick,
        # which is why callers act within the same breath. The daemon refuses
        # while any local sandbox row was created from the template — that
        # includes a `restoring` row a failed cross-node restore retained.
        result = self._node_shell_exec(
            node_name, f"space-tmplmgr delete-template --template {name}", timeout=300
        )
        if result.returncode != 0 and _TEMPLATE_IN_USE_MARKER in result.detail():
            ids = _UUID_RE.findall(result.detail())
            if not ids:
                raise RuntimeError(
                    f"template-manager refused to remove {name} on {node_name} for in-use "
                    f"sandboxes but named none: {result.detail()[:400]}"
                )
            return ids
        if result.returncode != 0 and any(
            marker in result.detail().lower() for marker in _TEMPLATE_NOT_FOUND_MARKERS
        ):
            # Already absent: an earlier scenario removed it from this worker
            # and prefetch has not re-pulled it yet. The caller wants it gone,
            # and proves that from the worker's filesystem afterwards.
            return []
        _require_ok(result, f"remove template {name} from {node_name}")
        return []

    def reclaim_cluster_deleted_local_sandbox(self, node_name: str, sandbox_id: str) -> None:
        canonical = _sql_uuid(sandbox_id)
        if self._psql(
            SANDBOX_LIVE_SQL.format(sandbox_id=canonical), action="probe sandbox liveness"
        ).strip():
            raise SmokeConfigurationError(
                f"sandbox {canonical} still exists on the cluster or is a reserved pool "
                f"slot; its local row on {node_name} is live state, not residue"
            )
        # An unknown id is a no-op exit 0 ("No sandboxes deleted."), so a
        # row that vanished between the refusal and here is harmless.
        _require_ok(
            self._node_shell_exec(node_name, f"space-sbxmgr delete {canonical}", timeout=180),
            f"reclaim cluster-deleted local sandbox {canonical} on {node_name}",
        )

    def reclaim_local_sandbox_everywhere(
        self,
        sandbox_id: str,
        *,
        wait_timeout: float = LOCAL_SANDBOX_RECLAIM_WAIT_SECONDS,
        delete_timeout: float = LOCAL_SANDBOX_RECLAIM_DELETE_SECONDS,
    ) -> None:
        canonical = _sql_uuid(sandbox_id)
        deadline = time.monotonic() + wait_timeout
        while self._psql(
            SANDBOX_LIVE_SQL.format(sandbox_id=canonical), action="probe sandbox liveness"
        ).strip():
            if time.monotonic() >= deadline:
                raise SmokeConfigurationError(
                    f"sandbox {canonical} still exists on the cluster {wait_timeout:g}s after "
                    "its delete; its local rows are live state, not residue"
                )
            time.sleep(LOCAL_SANDBOX_RECLAIM_POLL_SECONDS)
        # While a pod of the sandbox has not finished, the kubelet keeps
        # retrying its sandbox creation, and each retry of a restore pod
        # re-materializes a local row from the pod's own annotations. Rows are
        # only final once no such pod remains. A pod that reached `Succeeded`
        # or `Failed` is never retried, so it cannot re-create a row; such a
        # pod left behind with no cluster rows is a separate leak this cleanup
        # does not own, and waiting on it would only fail the scenario.
        while pods := [
            p for p in self.sandbox_pods(canonical) if p.phase not in _TERMINAL_POD_PHASES
        ]:
            if time.monotonic() >= deadline:
                raise SmokeConfigurationError(
                    f"sandbox {canonical} still has unfinished pods {wait_timeout:g}s after "
                    "its delete: "
                    + ", ".join(f"{p.pod_name} ({p.phase}, node {p.node_name})" for p in pods)
                )
            time.sleep(LOCAL_SANDBOX_RECLAIM_POD_POLL_SECONDS)
        # Every worker is attempted, all at once, before anything is raised:
        # one worker's refusal must not leave another worker's rows behind for
        # the next scenario.
        deadline = time.monotonic() + delete_timeout
        nodes = self.sandbox_pool_nodes()
        outcomes = run_concurrently(
            [
                partial(self._reclaim_local_sandbox_on, node, canonical, deadline)
                for node in nodes
            ]
        )
        failures = [
            f"on {node}: {type(outcome).__name__}: {outcome}"
            if isinstance(outcome, BaseException)
            else outcome
            for node, outcome in zip(nodes, outcomes)
            if outcome
        ]
        if failures:
            raise RuntimeError(f"reclaim local sandbox {canonical}: " + "; ".join(failures))

    def _reclaim_local_sandbox_on(self, node: str, canonical: str, deadline: float) -> str | None:
        while True:
            remaining = deadline - time.monotonic()
            # A scenario may have just restarted this worker's sandbox-manager.
            if self.ready_node_service_pod(node, NODE_SANDBOX_MANAGER) is None:
                if remaining <= 0:
                    return f"no ready sandbox-manager pod on {node}"
                time.sleep(min(2.0, remaining))
                continue
            # An unknown id is a no-op exit 0, as above; `--blocking` returns
            # once the row is gone rather than once it is admitted. The exec is
            # bounded by what is left of the delete budget.
            result = self._node_shell_exec(
                node, f"space-sbxmgr delete --blocking {canonical}", timeout=max(5.0, remaining)
            )
            if result.returncode == 0:
                return None
            detail = result.detail()
            if _LOCAL_DELETE_STUCK_SUSPEND_MARKER in detail:
                return (
                    f"sbx-manager on {node} refuses to delete its row because the row's "
                    "suspend publication is pending, and with the cluster row gone it can "
                    f"never land: {detail[:400]}"
                )
            if (
                any(marker in detail for marker in _LOCAL_DELETE_RETRY_MARKERS)
                and time.monotonic() < deadline
            ):
                time.sleep(2)
                continue
            return f"the delete on {node} failed: {detail[:400]}"

    def sandbox_pool_nodes(self) -> list[str]:
        result = self.kubectl(
            "get",
            "nodes",
            "-l",
            KIND_SANDBOX_NODE_SELECTOR,
            "-o",
            "jsonpath={.items[*].metadata.name}",
            timeout=15,
        )
        nodes = sorted(_require_ok(result, "list sandbox-pool nodes").split())
        if not nodes:
            raise SmokeConfigurationError("the bench has no sandbox-pool nodes")
        return nodes

    def template_subvolume_received_uuid(
        self, node_name: str, template: str, digest: str
    ) -> str | None:
        path = shlex.quote(
            f"{TEMPLATE_CACHE_ROOT}/{_template_name(template)}/{_template_digest(digest)}"
        )
        # 0: subvolume (output parsed); 3: a plain directory; 4: absent.
        result = self._node_shell_exec(
            node_name,
            f"if [ ! -e {path} ]; then exit 4; fi; "
            f"if btrfs subvolume show {path} 2>/dev/null; then exit 0; fi; exit 3",
            timeout=60,
        )
        if result.returncode == 3:
            return None
        if result.returncode == 4:
            raise RuntimeError(f"no digest dir {digest} for {template} on {node_name}")
        stdout = _require_ok(result, f"inspect template subvolume on {node_name}")
        for line in stdout.splitlines():
            key, _, value = line.partition(":")
            if key.strip() == "Received UUID":
                value = value.strip()
                return "" if value == "-" else value
        raise RuntimeError("btrfs subvolume show printed no Received UUID line")

    def sandbox_instance_entries(self, node_name: str, sandbox_id: str) -> tuple[str, ...]:
        root = shlex.quote(f"{SANDBOX_CACHE_ROOT}/{_sql_uuid(sandbox_id)}")
        result = self._node_shell_exec(
            node_name, f"if [ ! -e {root} ]; then exit 0; fi; ls -1 {root}", timeout=30
        )
        stdout = _require_ok(result, f"list sandbox cache dir on {node_name}")
        return tuple(
            e.strip() for e in stdout.splitlines() if e.strip().startswith("instance")
        )

    def sandbox_instance_is_subvolume(self, node_name: str, sandbox_id: str) -> bool:
        path = shlex.quote(f"{SANDBOX_CACHE_ROOT}/{_sql_uuid(sandbox_id)}/instance")
        result = self._node_shell_exec(
            node_name,
            f"if [ ! -e {path} ]; then exit 4; fi; "
            f"btrfs subvolume show {path} >/dev/null 2>&1 && echo yes || echo no",
            timeout=60,
        )
        if result.returncode == 4:
            raise RuntimeError(f"sandbox {sandbox_id} has no instance on {node_name}")
        return _require_ok(result, f"inspect instance on {node_name}").strip() == "yes"

    def snapshot_object_names(self, snapshot: SnapshotRowFacts) -> tuple[str, ...]:
        bucket, prefix, endpoint, environment = self._snapshot_storage_target(snapshot)
        # The localhost forward to the object store exits on the first request
        # after its pod was replaced (a scenario that scaled the store down)
        # and its supervisor reconnects within seconds; only that refusal is
        # retried.
        deadline = time.monotonic() + 30
        while True:
            result = self.executor.run(
                [
                    "aws", "--no-cli-pager", "--endpoint-url", endpoint,
                    "s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix,
                    "--delimiter", "/", "--output", "json",
                ],
                env=environment,
                timeout=30,
            )
            if (
                result.returncode == 0
                or _OBJECT_STORE_UNREACHABLE not in result.detail()
                or time.monotonic() >= deadline
            ):
                break
            time.sleep(2)
        output = _require_ok(result, "list the snapshot's objects")
        try:
            contents = (json.loads(output or "{}") or {}).get("Contents") or []
            return tuple(sorted(str(item["Key"])[len(prefix):] for item in contents))
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise RuntimeError(
                f"object-store listing returned an invalid document: snapshot_id={snapshot.snapshot_id}"
            ) from error

    def snapshot_row_facts(self, sandbox_id: str) -> list[SnapshotRowFacts]:
        rows = self._psql(
            SNAPSHOT_ROW_FACTS_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="list snapshot rows",
        )
        facts: list[SnapshotRowFacts] = []
        for line in rows.splitlines():
            if not line.strip():
                continue
            snapshot_id, state, timestamp, size_bytes, storage_uri = line.split("|", 4)
            facts.append(
                SnapshotRowFacts(
                    snapshot_id=snapshot_id,
                    state=state,
                    timestamp=timestamp,
                    size_bytes=int(size_bytes) if size_bytes.strip().isdigit() else None,
                    storage_uri=storage_uri,
                )
            )
        return facts

    def withhold_template_release_storage(self, template: str) -> list[ReleaseStorageRow]:
        self._require_isolated_kind("cluster release-storage mutation")
        name = _template_name(template)
        listing = self._psql(
            RELEASE_STORAGE_SELECT_SQL.format(template=name),
            action=f"list release storage rows for {name}",
        )
        rows: list[ReleaseStorageRow] = []
        for line in listing.splitlines():
            if not line.strip():
                continue
            row_id, release_id, org_id, uri, created_at = line.split("|", 4)
            rows.append(
                ReleaseStorageRow(
                    row_id=_sql_uuid(row_id),
                    release_id=_sql_uuid(release_id),
                    org_id=_sql_uuid(org_id) if org_id.strip() else "",
                    storage_uri=_storage_uri(uri),
                    created_at=_sql_timestamp(created_at),
                )
            )
        if not rows:
            raise SmokeConfigurationError(
                f"template {name} has no release storage rows on this cluster; "
                "an on-demand pull could never have succeeded"
            )
        sidecars = self._psql(
            RELEASE_STORAGE_SIDECAR_COUNT_SQL.format(
                ids=", ".join(f"'{r.row_id}'" for r in rows)
            ),
            action="count encrypted-template sidecar rows",
        ).strip()
        if sidecars != "0":
            raise SmokeConfigurationError(
                f"template {name} has {sidecars} encrypted storage sidecar row(s), which a "
                "delete would cascade away and nothing could restore; this fault is for "
                "plain templates only"
            )
        self._psql(
            RELEASE_STORAGE_DELETE_SQL.format(template=name),
            action=f"withhold release storage rows for {name}",
        )
        return rows

    def restore_template_release_storage(self, rows: list[ReleaseStorageRow]) -> None:
        for row in rows:
            self._psql(
                RELEASE_STORAGE_INSERT_SQL.format(
                    row_id=_sql_uuid(row.row_id),
                    release_id=_sql_uuid(row.release_id),
                    org_id=f"'{_sql_uuid(row.org_id)}'" if row.org_id else "NULL",
                    uri=_storage_uri(row.storage_uri),
                    created_at=_sql_timestamp(row.created_at),
                ),
                action="restore a release storage row",
            )

    def corrupt_snapshot_object(
        self, record: SnapshotRecord, *, object_name: str, offset: int
    ) -> None:
        bucket, prefix, endpoint, environment = self._snapshot_storage_target(record)
        validate_loopback_environment(
            endpoint,
            self.environment.get("SPACE_SMOKE_ALLOW_REMOTE_S3_DELETE", "0") == "1",
            scenario_id="local.btrfs_send_restore_rejects_corrupted_stream",
            endpoint_name="S3_ENDPOINT",
            remote_override_name="SPACE_SMOKE_ALLOW_REMOTE_S3_DELETE",
        )
        name = _object_name(object_name)
        if offset < 0:
            raise ValueError("offset must be non-negative")
        key = f"s3://{bucket}/{prefix}{name}"
        with tempfile.TemporaryDirectory(prefix="space-smoke-corrupt-") as tmp:
            local = os.path.join(tmp, name)
            _require_ok(
                self.executor.run(
                    ["aws", "--no-cli-pager", "--endpoint-url", endpoint, "s3", "cp", key, local],
                    env=environment,
                    timeout=120,
                ),
                f"download {name} for corruption",
            )
            size = os.path.getsize(local)
            if offset >= size:
                raise RuntimeError(f"{name} is {size} bytes; cannot flip byte {offset}")
            with open(local, "r+b") as handle:
                handle.seek(offset)
                original = handle.read(1)
                handle.seek(offset)
                handle.write(bytes([original[0] ^ 0xFF]))
            _require_ok(
                self.executor.run(
                    ["aws", "--no-cli-pager", "--endpoint-url", endpoint, "s3", "cp", local, key],
                    env=environment,
                    timeout=120,
                ),
                f"upload corrupted {name}",
            )

    # --- concurrent restore contention -------------------------------------

    def _public_api_deployment(self) -> str:
        result = self.kubectl(
            "get", "deploy", "-l", PUBLIC_API_DEPLOYMENT_SELECTOR,
            "-o", "jsonpath={.items[*].metadata.name}", timeout=15,
        )
        names = _require_ok(result, "find the public API Deployment").split()
        if len(names) != 1:
            raise SmokeConfigurationError(
                f"expected one public API Deployment ({PUBLIC_API_DEPLOYMENT_SELECTOR}), found {names}"
            )
        return names[0]

    def scale_public_api(self, replicas: int) -> int:
        self._require_isolated_kind("public API scaling")
        if replicas < 1 or replicas > 5:
            raise ValueError(f"refusing to scale the public API to {replicas} replicas")
        name = self._public_api_deployment()
        previous = _require_ok(
            self.kubectl("get", "deploy", name, "-o", "jsonpath={.spec.replicas}", timeout=15),
            f"read replicas of {name}",
        ).strip()
        autoscaler = self._public_api_autoscaler(name)
        if autoscaler is not None:
            # An autoscaler owns the replica count and reconciles a manual
            # scale away once its scale-down window passes, so the count is
            # set through its bounds; `restore_public_api_autoscaler` puts the
            # captured bounds back.
            metadata, spec = autoscaler["metadata"], autoscaler["spec"]
            if self._public_api_autoscaler_bounds is None:
                self._public_api_autoscaler_bounds = (
                    str(metadata["name"]), str(metadata["uid"]),
                    int(spec.get("minReplicas", 1)), int(spec["maxReplicas"]),
                )
            self._patch_public_api_autoscaler(replicas, replicas)
        _require_ok(
            self.kubectl("scale", f"deploy/{name}", f"--replicas={replicas}", timeout=30),
            f"scale {name} to {replicas}",
        )
        return int(previous) if previous.isdigit() else 1

    def restore_public_api_autoscaler(self) -> None:
        if self._public_api_autoscaler_bounds is None:
            return
        _, _, minimum, maximum = self._public_api_autoscaler_bounds
        self._patch_public_api_autoscaler(minimum, maximum)
        self._public_api_autoscaler_bounds = None

    def _public_api_autoscaler(self, deployment: str) -> dict[str, Any] | None:
        listing = json.loads(
            _require_ok(self.kubectl("get", "hpa", "-o", "json", timeout=15), "list autoscalers")
            or "{}"
        )
        matches = [
            item
            for item in listing.get("items", [])
            if (item.get("spec") or {}).get("scaleTargetRef", {}).get("kind") == "Deployment"
            and item["spec"]["scaleTargetRef"].get("name") == deployment
        ]
        if len(matches) > 1:
            raise SmokeConfigurationError(f"more than one autoscaler targets {deployment}")
        return matches[0] if matches else None

    def _patch_public_api_autoscaler(self, minimum: int, maximum: int) -> None:
        assert self._public_api_autoscaler_bounds is not None
        name, uid, _, _ = self._public_api_autoscaler_bounds
        current = json.loads(
            _require_ok(self.kubectl("get", "hpa", name, "-o", "json", timeout=15), f"read {name}")
            or "{}"
        )
        if current.get("metadata", {}).get("uid") != uid:
            raise SmokeConfigurationError(f"refusing replaced autoscaler {name}")
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": uid},
            {"op": "replace", "path": "/spec/minReplicas", "value": minimum},
            {"op": "replace", "path": "/spec/maxReplicas", "value": maximum},
        ]
        _require_ok(
            self.kubectl("patch", "hpa", name, "--type=json", "--patch", json.dumps(patch), timeout=15),
            f"set {name} bounds to {minimum}..{maximum}",
        )

    def wait_public_api_rollout(self, *, timeout: float) -> None:
        name = self._public_api_deployment()
        _require_ok(
            self.kubectl(
                "rollout", "status", f"deploy/{name}", f"--timeout={int(timeout)}s",
                timeout=timeout + 30,
            ),
            f"await rollout of {name}",
        )
        self._wait_deployment_pods_drained(name, timeout=timeout)

    def wait_public_api_pods_drained(self, *, timeout: float) -> None:
        """Block until only the Deployment's desired API pods exist, none Terminating."""

        self._wait_deployment_pods_drained(self._public_api_deployment(), timeout=timeout)

    def _wait_deployment_pods_drained(self, name: str, *, timeout: float) -> None:
        """Block until only the Deployment's desired pods exist, none Terminating.

        `kubectl rollout status` completes a rolling update, or a scale-down,
        once the available count matches the spec, while the replaced pods are
        still shutting down. A client that holds a pooled connection to one of
        them (the load-balanced entry hands each connection to one replica)
        has it reset when that pod exits, so a caller about to reuse its
        client waits for the old pods to be gone, which closes those
        connections first.
        """

        deadline = time.monotonic() + timeout
        while True:
            # Re-read each poll: an autoscaler may move the desired count.
            deployment = json.loads(
                _require_ok(
                    self.kubectl("get", "deploy", name, "-o", "json", timeout=15),
                    f"read {name}",
                )
            )
            desired = int(deployment["spec"].get("replicas", 1))
            match_labels = deployment["spec"]["selector"].get("matchLabels") or {}
            if not match_labels:
                raise SmokeConfigurationError(f"{name} has no matchLabels selector to list its pods by")
            selector = ",".join(f"{key}={value}" for key, value in sorted(match_labels.items()))
            pods = json.loads(
                _require_ok(
                    self.kubectl("get", "pods", "-l", selector, "-o", "json", timeout=15),
                    f"list {name} pods",
                )
            ).get("items", [])
            terminating = [
                pod["metadata"]["name"] for pod in pods if pod["metadata"].get("deletionTimestamp")
            ]
            if not terminating and len(pods) == desired:
                return
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"{name} pods did not drain to {desired} within {timeout:g}s "
                    f"(present={len(pods)}, terminating={terminating})"
                )
            time.sleep(1)

    # --- node groups ----------------------------------------------------------

    def set_native_operation_probe_budgets(self, expected: list[dict], desired: list[dict], *, expected_uid: str, remove: bool = False) -> None:
        """Change only the three native invalid-create probe budget literals."""
        if (
            self._retention_lease_ownership is None
            or not self.config.context.startswith("kind-")
            or urlsplit(self.config.api_url).hostname not in {"localhost", "127.0.0.1", "::1"}
        ):
            raise SmokeConfigurationError("native probe budgets require owned local Kind control")
        self._assert_retention_lease_owned()
        name = "space-cluster-controller"
        observed = self.kubectl("get", "deployment", name, "-o", "json", timeout=15)
        if observed.returncode:
            raise SmokeConfigurationError("could not inspect native probe controller")
        deployment = json.loads(observed.stdout)
        if deployment["metadata"]["uid"] != expected_uid:
            raise SmokeConfigurationError("refusing replaced native probe controller")
        containers = deployment["spec"]["template"]["spec"]["containers"]
        if len(containers) != 1 or containers[0].get("env", []) != expected:
            raise SmokeConfigurationError("controller environment changed concurrently; refusing overwrite")
        path = "/spec/template/spec/containers/0/env"
        patch = [
            {"op": "test", "path": "/metadata/resourceVersion", "value": deployment["metadata"]["resourceVersion"]},
        ]
        allowed = {NATIVE_OPERATION_AGE_ENV, *NATIVE_OPERATION_READY_ENVS}
        if remove:
            if desired or any(entry.get("name") not in allowed for entry in expected):
                raise SmokeConfigurationError("refusing to remove unrelated controller environment")
            patch.append({"op": "remove", "path": path})
        else:
            if "env" not in containers[0]:
                patch.append({"op": "add", "path": path, "value": []})
            for index in range(min(len(expected), len(desired))):
                if expected[index] == desired[index]:
                    continue
                if (expected[index].get("name") not in allowed
                        or desired[index].get("name") != expected[index].get("name")):
                    raise SmokeConfigurationError("refusing unrelated controller environment changes")
                patch.append({"op": "replace", "path": f"{path}/{index}", "value": desired[index]})
            for index in range(len(expected) - 1, len(desired) - 1, -1):
                if expected[index].get("name") not in allowed:
                    raise SmokeConfigurationError("refusing to remove unrelated controller environment")
                patch.append({"op": "remove", "path": f"{path}/{index}"})
            for entry in desired[len(expected):]:
                if entry.get("name") not in allowed:
                    raise SmokeConfigurationError("refusing to add unrelated controller environment")
                patch.append({"op": "add", "path": f"{path}/-", "value": entry})
        result = self.kubectl(
            "patch", "deployment", name, "--type=json", "--patch", json.dumps(patch), timeout=15
        )
        if result.returncode:
            raise SmokeConfigurationError("controller budget patch failed; concurrent changes were not overwritten")


    def _deployment_env_literals(self, name: str, keys: Sequence[str]) -> dict[str, str | None]:
        result = self.kubectl(
            "get", "deploy", name, "-o",
            "jsonpath={.spec.template.spec.containers[0].env}", timeout=15,
        )
        _require_ok(result, f"read env of {name}")
        return _env_literals(
            json.loads(result.stdout or "[]"), keys, owner=name,
            value_from_refusal="; refusing to override what a literal restore could not put back",
        )

    def _set_deployment_env(self, name: str, values: Mapping[str, str | None], *, timeout: float) -> None:
        if self._set_deployment_env_only(name, values):
            self._await_deployment_rollout(name, timeout=timeout)

    def _set_deployment_env_only(self, name: str, values: Mapping[str, str | None]) -> bool:
        """`kubectl set env` without waiting; True when anything was set."""

        assignments = [f"{k}-" if v is None else f"{k}={v}" for k, v in values.items()]
        if not assignments:
            return False
        container = self._public_api_container(name)
        _require_ok(
            self.kubectl("set", "env", f"deploy/{name}", "-c", container, *assignments, timeout=30),
            f"set env on {name}",
        )
        return True

    def _await_deployment_rollout(self, name: str, *, timeout: float) -> None:
        _require_ok(
            self.kubectl("rollout", "status", f"deploy/{name}", f"--timeout={int(timeout)}s", timeout=timeout + 30),
            f"await rollout of {name}",
        )
        self._wait_deployment_pods_drained(name, timeout=timeout)

    @contextlib.contextmanager
    def _fast_rollout(self, name: str, ownership: _LeaseOwnership | None = None) -> Iterator[None]:
        """Roll `name` fast while the body runs, then put its standing pace back, even if the body fails.

        Only for a rollout that is a scenario's setup or restore, never its
        subject: a scenario about rollouts, drains, restarts or disruption
        keeps the chart's pace. Every replacement surges at once and counts as
        available once Ready, so a two-replica Deployment rolls in one wave
        without `minReadySeconds`, not in two waves with it. `maxUnavailable`
        is untouched, so no old replica stops before its replacement is Ready.
        Neither field is in the pod template, so neither patch rolls a pod.
        Callers write the env and await the rollout inside the body. A
        leader-elected Deployment hands its Lease over only once every old
        replica has gone, after the rollout rather than during it, so a caller
        waiting on the new leader gains little.

        Only under the public-API mutation Lease, the caller's or `ownership`
        (a repair running inside the acquisition), which is the guard
        `repair_orphaned_rollout_pace` takes before it restores a pace a run
        left behind.
        """

        ownership = ownership or self._held_public_api_lease(f"a fast rollout of {name}")
        self._assert_lease_owned(ownership, f"a fast rollout of {name}")

        # Armed before the first patch is sent: a patch whose reply is lost,
        # or that a deadline interrupts, may still have committed, and the
        # restore is a no-op on a Deployment that carries no record.
        armed: list[_RolloutPace] = []
        try:
            self._speed_up_rollout(name, armed.append)
            yield
        except BaseException as error:
            if armed:
                try:
                    self._restore_rollout_pace(name, armed[-1])
                except Exception as restore_error:  # noqa: BLE001 - attached to the body's error below
                    note = (
                        f"restoring the rollout pace of {name} also failed: {restore_error!r}; "
                        "the next scenario's preflight repair restores it from the Deployment's record"
                    )
                    error.add_note(note)
                    # A verdict reports only the body's error, not its notes.
                    print(f"PACE RESTORE FAILED: {note}", file=sys.stderr, flush=True)
            raise
        if armed:
            self._restore_rollout_pace(name, armed[-1])

    def _speed_up_rollout(self, name: str, arm: Callable[[_RolloutPace], None]) -> None:
        def fast_patch(deployment: Mapping[str, Any]) -> list[dict[str, object]]:
            metadata = deployment["metadata"]
            fast = _fast_pace(deployment)
            recorded = (metadata.get("annotations") or {}).get(FAST_ROLLOUT_STANDING_ANNOTATION)
            # An earlier run that died mid-rollout left its fast values live,
            # and what it recorded is what the Deployment stood at. A record
            # beside values something else has since written is stale.
            previous = _fast_rollout_record(recorded, name) if recorded is not None else {}
            if not all(_holds_fast_value(deployment, pointer, fast) for pointer in previous):
                previous = {}
            standing = {
                pointer: previous[pointer] if pointer in previous else _json_pointer_get(deployment, pointer)
                for pointer in fast
            }
            arm(_RolloutPace(uid=str(metadata["uid"]), standing=standing))
            patch: list[dict[str, object]] = []
            if not metadata.get("annotations"):
                patch.append({"op": "add", "path": "/metadata/annotations", "value": {}})
            patch.append({
                "op": "add",
                "path": f"/metadata/annotations/{_json_pointer_token(FAST_ROLLOUT_STANDING_ANNOTATION)}",
                "value": json.dumps(standing, sort_keys=True),
            })
            patch.extend({"op": "add", "path": pointer, "value": value} for pointer, value in fast.items())
            return patch

        self._patch_deployment_guarded(name, fast_patch, f"speed up the rollout of {name}")

    def _restore_rollout_pace(self, name: str, pace: _RolloutPace | None) -> None:
        """Put back the pace `pace` recorded, or with None the one the Deployment's record names.

        Only while every recorded field still holds its fast value, the rule
        `_speed_up_rollout` trusts a record by: otherwise something else (a
        chart re-apply) has written the pace since, and only the record goes.
        """

        def restore_patch(deployment: Mapping[str, Any]) -> list[dict[str, object]]:
            metadata = deployment["metadata"]
            if pace is not None and metadata["uid"] != pace.uid:
                raise SmokeConfigurationError(f"refusing to restore the rollout pace of a replaced {name}")
            recorded = (metadata.get("annotations") or {}).get(FAST_ROLLOUT_STANDING_ANNOTATION)
            if recorded is None:
                # Never sped up, or a cooperating run's restore already ran.
                return []
            standing = pace.standing if pace is not None else _fast_rollout_record(recorded, name)
            fast = _fast_pace(deployment)
            patch: list[dict[str, object]] = []
            still_fast = all(_holds_fast_value(deployment, pointer, fast) for pointer in standing)
            for pointer, value in standing.items() if still_fast else ():
                if value is not None:
                    patch.append({"op": "add", "path": pointer, "value": value})
                elif _json_pointer_get(deployment, pointer) is not None:
                    patch.append({"op": "remove", "path": pointer})
            patch.append({
                "op": "remove",
                "path": f"/metadata/annotations/{_json_pointer_token(FAST_ROLLOUT_STANDING_ANNOTATION)}",
            })
            return patch

        self._patch_deployment_guarded(name, restore_patch, f"restore the rollout pace of {name}")

    def _patch_deployment_guarded(
        self,
        name: str,
        build: Callable[[Mapping[str, Any]], list[dict[str, object]]],
        action: str,
    ) -> None:
        """JSON-patch the Deployment as last read; re-read and retry when a concurrent write lands first.

        The Deployment controller writes status throughout a rollout, so a
        patch guarded by the resourceVersion it read can lose that race.
        """

        detail = ""
        for _ in range(5):
            deployment = json.loads(
                _require_ok(self.kubectl("get", "deploy", name, "-o", "json", timeout=15), f"read {name}")
            )
            patch = build(deployment)
            if not patch:
                return
            metadata = deployment["metadata"]
            result = self.kubectl(
                "patch", "deploy", name, "--type=json", "--patch",
                json.dumps([
                    {"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
                    {"op": "test", "path": "/metadata/resourceVersion", "value": metadata["resourceVersion"]},
                    *patch,
                ]),
                timeout=15,
            )
            if result.returncode == 0:
                return
            detail = result.detail()
            time.sleep(0.5)
        raise SmokeConfigurationError(f"could not {action}: {detail or '<no output>'}")

    def node_group_env(self) -> dict[str, str | None]:
        """`api:<key>` / `controller:<key>` literals for the gate and the labeler."""

        api = self._deployment_env_literals(self._public_api_deployment(), (NODE_GROUP_GATE_ENV,))
        controller = self._deployment_env_literals(
            self._controller_deployment(), (NODE_GROUP_GATE_ENV, NODE_GROUP_LABELER_ENV)
        )
        return {
            **{f"api:{k}": v for k, v in api.items()},
            **{f"controller:{k}": v for k, v in controller.items()},
        }

    def _apply_node_group_env(
        self, values: Mapping[str, str | None], *, controller_first: bool
    ) -> None:
        allowed = {
            f"api:{NODE_GROUP_GATE_ENV}",
            f"controller:{NODE_GROUP_GATE_ENV}",
            f"controller:{NODE_GROUP_LABELER_ENV}",
        }
        for value in values.values():
            if value is not None and value not in {"", "true", "false", "1", "0"}:
                raise ValueError(f"refusing node-group env value {value!r}")
        self._apply_renderer_env(values, allowed=allowed, controller_first=controller_first, what="node-group")

    def _apply_renderer_env(
        self,
        values: Mapping[str, str | None],
        *,
        allowed: Collection[str],
        controller_first: bool,
        what: str,
    ) -> None:
        """Write allowlisted renderer env literals and await every affected rollout."""

        self._require_isolated_kind(f"{what} env override")
        unknown = sorted(set(values) - set(allowed))
        if unknown:
            raise ValueError(f"refusing non-{what} env keys: {unknown}")
        api = {k.split(":", 1)[1]: v for k, v in values.items() if k.startswith("api:")}
        controller = {k.split(":", 1)[1]: v for k, v in values.items() if k.startswith("controller:")}
        # Enable: controller first, so the labeler is seeding the default
        # group and labelling workers while the API rolls with the gate on.
        # Restore: API first, so the gate is off before the labeler stops and
        # a stray group cannot affect a create in between. The *writes* keep
        # that order; the rollouts are then awaited together, since neither
        # depends on the other having finished (the scenario's create waits for
        # the default group and the labels anyway). Each runs at the fast pace
        # and ends once the replaced pods are gone; the slowest, the API's,
        # is about 15 s on kind, against about 38 s at the chart's pace.
        order = [("controller", controller), ("api", api)] if controller_first else [("api", api), ("controller", controller)]
        order.append(("internal-api", {k.split(":", 1)[1]: v for k, v in values.items() if k.startswith("internal-api:")}))
        deployments = {
            "api": self._public_api_deployment,
            "controller": self._controller_deployment,
            "internal-api": self._internal_api_deployment,
        }
        rolling: list[str] = []
        with contextlib.ExitStack() as paces:
            for target, assignments in order:
                if not assignments:
                    continue
                name = deployments[target]()
                paces.enter_context(self._fast_rollout(name))
                if self._set_deployment_env_only(name, assignments):
                    rolling.append(name)
            waits = [
                background(f"rollout of {name}", lambda name=name: self._await_deployment_rollout(name, timeout=300))
                for name in rolling
            ]
            for wait in waits:
                wait.join(timeout=330)

    def set_node_group_env(self, *, gate: bool, labeler: bool) -> None:
        self._apply_node_group_env(
            {
                f"controller:{NODE_GROUP_LABELER_ENV}": "true" if labeler else "false",
                f"controller:{NODE_GROUP_GATE_ENV}": "true" if gate else "false",
                f"api:{NODE_GROUP_GATE_ENV}": "true" if gate else "false",
            },
            controller_first=True,
        )

    def restore_node_group_env(self, standing: Mapping[str, str | None]) -> None:
        self._apply_node_group_env(standing, controller_first=False)

    # --- node-ctl admission fence -------------------------------------------

    @property
    def namespace(self) -> str:
        return KIND_NAMESPACE

    def node_ctl_identity_for_sandbox(self, sandbox_id: str) -> NodeCtlIdentity:
        placement = self.sandbox_pod_placement(sandbox_id)
        if placement is None:
            raise SmokeConfigurationError(f"sandbox {sandbox_id} has no execution pod; cannot pick its worker")
        pod = self._node_service_pod(placement.node_name, NODE_CTL)
        result = self.kubectl("get", "pod", _kube_pod_name(pod), "-o", "json", timeout=15)
        document = json.loads(_require_ok(result, f"read node-ctl pod {pod}") or "{}")
        metadata, spec = document.get("metadata") or {}, document.get("spec") or {}
        uid, sa = str(metadata.get("uid", "")), str(spec.get("serviceAccountName", ""))
        if not uid or not sa:
            raise SmokeConfigurationError(f"node-ctl pod {pod} has no uid or serviceAccountName")
        return NodeCtlIdentity(pod, uid, _kube_node_name(placement.node_name), sa)

    def kube_node_names(self) -> list[str]:
        result = self.kubectl("get", "nodes", "-o", "jsonpath={.items[*].metadata.name}", timeout=15)
        return sorted(_require_ok(result, "list nodes").split())

    def fence_policy_for(self, service_account: str) -> FencePolicy | None:
        user = f"system:serviceaccount:{KIND_NAMESPACE}:{service_account}"
        policies = json.loads(
            _require_ok(self.kubectl("get", "validatingadmissionpolicies", "-o", "json", timeout=15), "list admission policies") or "{}"
        )
        match = None
        for item in policies.get("items") or []:
            conditions = (item.get("spec") or {}).get("matchConditions") or []
            if any(user in str(c.get("expression", "")) for c in conditions):
                match = item
                break
        if match is None:
            return None
        name = str((match.get("metadata") or {}).get("name", ""))
        warnings = len((((match.get("status") or {}).get("typeChecking") or {}).get("expressionWarnings")) or [])
        bindings = json.loads(
            _require_ok(self.kubectl("get", "validatingadmissionpolicybindings", "-o", "json", timeout=15), "list policy bindings") or "{}"
        )
        actions: list[str] = []
        for item in bindings.get("items") or []:
            spec = item.get("spec") or {}
            if spec.get("policyName") == name:
                actions = [str(a) for a in spec.get("validationActions") or []]
        return FencePolicy(name, actions, warnings)

    def _node_ctl_exec(self, pod_name: str, script: str) -> CommandResult:
        # Inside node-ctl's own container: the token under test is the one
        # kubelet projects there, and only there.
        self._require_isolated_kind("node-ctl credential reads")
        return self.kubectl(
            "exec", _kube_pod_name(pod_name), "-c", NODE_CTL, "--", "sh", "-c", script,
            timeout=30,
        )

    def pod_file_readable(self, pod_name: str, path: str) -> bool:
        return self._node_ctl_exec(pod_name, f"test -r {shlex.quote(_pod_path(path))}").returncode == 0

    def pod_file(self, pod_name: str, path: str) -> str:
        result = self._node_ctl_exec(pod_name, f"cat {shlex.quote(_pod_path(path))}")
        return _require_ok(result, f"read {path} from {pod_name}")

    def bound_token(self, identity: NodeCtlIdentity) -> str:
        result = self.kubectl(
            "create", "token", identity.service_account, "--bound-object-kind", "Pod",
            "--bound-object-name", _kube_pod_name(identity.pod_name), "--bound-object-uid", identity.pod_uid,
            "--duration=1800s", timeout=30,
        )
        return _require_ok(result, "mint a pod-bound token").strip()

    def kubeconfig_with_token(self, token: str) -> str:
        self._require_isolated_kind("ServiceAccount kubeconfig")
        raw = _require_ok(self.kubectl("config", "view", "--raw", "--minify", "-o", "json", timeout=15), "read the kubeconfig")
        document = json.loads(raw)
        users = document.get("users") or []
        if not users:
            raise SmokeConfigurationError("the minified kubeconfig has no user entry to replace")
        users[0]["user"] = {"token": token}
        handle = tempfile.NamedTemporaryFile("w", prefix="space-smoke-sa-", suffix=".kubeconfig", delete=False)
        with handle:
            json.dump(document, handle)
        os.chmod(handle.name, 0o600)
        return handle.name

    def _as_sa(self, kubeconfig: str, *args: str, input_text: str | None = None, timeout: float = 60) -> CommandResult:
        return self.executor.run(
            ["kubectl", "--kubeconfig", kubeconfig, "--context", self.config.context, f"--request-timeout={int(timeout)}s", *args],
            input_text=input_text,
            timeout=timeout + 15,
        )

    def whoami(self, kubeconfig: str) -> WhoAmI:
        result = self._as_sa(kubeconfig, "auth", "whoami", "-o", "json")
        payload = json.loads(_require_ok(result, "auth whoami as the ServiceAccount") or "{}")
        info = (payload.get("status") or {}).get("userInfo") or {}
        claims = (info.get("extra") or {}).get("authentication.kubernetes.io/node-name") or []
        return WhoAmI(str(info.get("username", "")), str(claims[0]) if claims else None)

    def can_i_patch_nodes(self, kubeconfig: str) -> bool:
        result = self._as_sa(kubeconfig, "auth", "can-i", "patch", "nodes")
        return result.stdout.strip() == "yes"

    def node_json(self, node_name: str) -> dict[str, Any]:
        result = self.kubectl("get", "node", _kube_node_name(node_name), "-o", "json", timeout=15)
        return json.loads(_require_ok(result, f"read node {node_name}") or "{}")

    def node_label(self, node_name: str, key: str) -> str | None:
        value = ((self.node_json(node_name).get("metadata") or {}).get("labels") or {}).get(_label_key(key))
        return str(value) if value is not None else None

    def node_capacity(self, node_name: str, resource: str) -> str | None:
        result = self.kubectl("get", "node", _kube_node_name(node_name), "-o", "json", timeout=15)
        if result.returncode != 0:
            return None
        value = (((json.loads(result.stdout or "{}").get("status") or {}).get("capacity")) or {}).get(resource)
        return str(value) if value else None

    def patch_node_as(
        self, kubeconfig: str, node_name: str, patch: Mapping[str, object], *, subresource: str | None = None
    ) -> SaWriteOutcome:
        args = ["patch", "node", _kube_node_name(node_name), "--type=merge", "-p", json.dumps(dict(patch))]
        if subresource:
            args.insert(3, f"--subresource={_subresource(subresource)}")
        result = self._as_sa(kubeconfig, *args)
        return SaWriteOutcome(result.returncode, (result.stdout + result.stderr).strip())

    def apply_as(
        self, kubeconfig: str, manifest: str, *, field_manager: str, subresource: str | None = None
    ) -> SaWriteOutcome:
        args = _server_side_apply_args(field_manager, subresource)
        result = self._as_sa(kubeconfig, *args, input_text=manifest)
        return SaWriteOutcome(result.returncode, (result.stdout + result.stderr).strip())

    def admin_label_node(self, node_name: str, key: str, value: str | None) -> None:
        self._require_isolated_kind("admin node label")
        assignment = f"{_label_key(key)}-" if value is None else f"{_label_key(key)}={_label_value(value)}"
        result = self.kubectl("label", "node", _kube_node_name(node_name), assignment, "--overwrite", timeout=30)
        if result.returncode != 0 and not (value is None and "not found" in result.stderr):
            raise RuntimeError(f"label node {node_name} {assignment}: {result.detail()}")

    def admin_annotate_node(self, node_name: str, key: str, value: str | None) -> None:
        self._require_isolated_kind("admin node annotate")
        assignment = f"{_label_key(key)}-" if value is None else f"{_label_key(key)}={value}"
        result = self.kubectl("annotate", "node", _kube_node_name(node_name), assignment, "--overwrite", timeout=30)
        if result.returncode != 0 and not (value is None and "not found" in result.stderr):
            raise RuntimeError(f"annotate node {node_name} {assignment}: {result.detail()}")

    def admin_apply_node(self, node_name: str, manifest: str, *, field_manager: str) -> None:
        self._require_isolated_kind("admin node apply")
        _require_ok(
            self.kubectl(*_server_side_apply_args(field_manager), input_text=manifest, timeout=30),
            f"apply node {node_name} as {field_manager}",
        )

    def admin_patch_node(self, node_name: str, patch: Mapping[str, object], *, subresource: str | None = None) -> None:
        self._require_isolated_kind("admin node patch")
        args = ["patch", "node", _kube_node_name(node_name), "--type=merge", "-p", json.dumps(dict(patch))]
        if subresource:
            args.insert(3, f"--subresource={_subresource(subresource)}")
        _require_ok(self.kubectl(*args, timeout=30), f"patch node {node_name}")

    def admin_remove_capacity(self, node_name: str, resource: str) -> None:
        self._require_isolated_kind("admin capacity strip")
        path = "/status/capacity/" + resource.replace("~", "~0").replace("/", "~1")
        _require_ok(
            self.kubectl(
                "patch", "node", _kube_node_name(node_name), "--subresource=status", "--type=json",
                "-p", json.dumps([{"op": "remove", "path": path}]), timeout=30,
            ),
            f"strip {resource} from {node_name}",
        )

    def revert_node_from_snapshot(self, node_name: str, snapshot: Mapping[str, object]) -> list[str]:
        """The shell suite's `revert_one_node`, keyed on markers so a REPAIR means a probe got through."""

        from space_smoke import node_fence as nf

        self._require_isolated_kind("node probe revert")
        node = _kube_node_name(node_name)
        live = self.node_json(node)
        repairs: list[str] = []
        snap_spec, live_spec = snapshot.get("spec") or {}, live.get("spec") or {}
        want = {"taints": snap_spec.get("taints") or [], "unschedulable": bool(snap_spec.get("unschedulable", False))}  # type: ignore[union-attr]
        have = {"taints": live_spec.get("taints") or [], "unschedulable": bool(live_spec.get("unschedulable", False))}  # type: ignore[union-attr]
        if want != have:
            repairs.append("restoring .spec taints/unschedulable")
            self.admin_patch_node(node, {"spec": want})
        snap_labels = (snapshot.get("metadata") or {}).get("labels") or {}  # type: ignore[union-attr]
        for key in (nf.BOUNDARY_LABEL, nf.CRED_AUTHORITY_POOL_LABEL, nf.OWNED_LABEL, nf.BLOCKED_LABEL):
            self.admin_label_node(node, key, snap_labels.get(key))
        live_meta = live.get("metadata") or {}
        # Keyed on the probe value, not on drift, and applied under node-ctl's
        # field manager: a `kubectl label` would take the key away from that
        # manager, and node-ctl's unknown-CPU apply could then no longer
        # remove it.
        if (live_meta.get("labels") or {}).get(nf.HOST_CPU_LABEL) == nf.PROBE_HOST_CPU:  # type: ignore[union-attr]
            repairs.append("restoring the host-cpu label")
            self.admin_apply_node(
                node,
                nf.host_cpu_manifest(node, snap_labels.get(nf.HOST_CPU_LABEL)),
                field_manager=nf.HOST_CPU_FIELD_MANAGER,
            )
        for key in sorted(set(live_meta.get("labels") or {}) | set(live_meta.get("annotations") or {})):
            if nf.PROBE_KEY_MARKER in key:
                repairs.append(f"removing probe key {key}")
                self.admin_label_node(node, key, None)
                self.admin_annotate_node(node, key, None)
        finalizers = list(live_meta.get("finalizers") or [])
        if nf.PROBE_FINALIZER in finalizers:
            repairs.append("removing probe finalizer")
            keep = [f for f in finalizers if f != nf.PROBE_FINALIZER]
            _require_ok(
                self.kubectl(
                    "patch", "node", node, "--type=json", "-p",
                    json.dumps([{"op": "replace", "path": "/metadata/finalizers", "value": keep}]), timeout=30,
                ),
                f"remove the probe finalizer from {node}",
            )
        live_status = live.get("status") or {}
        if any(c.get("reason") == nf.PROBE_CONDITION_REASON for c in (live_status.get("conditions") or [])):  # type: ignore[union-attr]
            repairs.append("restoring status.conditions")
            self.admin_patch_node(node, {"status": {"conditions": (snapshot.get("status") or {}).get("conditions") or []}}, subresource="status")  # type: ignore[union-attr]
        for map_name in ("capacity", "allocatable"):
            want_value = ((snapshot.get("status") or {}).get(map_name) or {}).get(nf.FOREIGN_CAPACITY_RESOURCE)  # type: ignore[union-attr]
            live_value = (live_status.get(map_name) or {}).get(nf.FOREIGN_CAPACITY_RESOURCE)  # type: ignore[union-attr]
            if live_value == want_value:
                continue
            if want_value:
                repairs.append(f"restoring status.{map_name}[{nf.FOREIGN_CAPACITY_RESOURCE}]")
                self.admin_patch_node(node, {"status": {map_name: {nf.FOREIGN_CAPACITY_RESOURCE: want_value}}}, subresource="status")
            else:
                repairs.append(f"removing forged status.{map_name}[{nf.FOREIGN_CAPACITY_RESOURCE}]")
                path = f"/status/{map_name}/" + nf.FOREIGN_CAPACITY_RESOURCE.replace("~", "~0").replace("/", "~1")
                self.kubectl("patch", "node", node, "--subresource=status", "--type=json", "-p", json.dumps([{"op": "remove", "path": path}]), timeout=30)
        return repairs

    def node_ctl_log_tail(self, pod_name: str, lines: int) -> list[str]:
        result = self.kubectl("logs", _kube_pod_name(pod_name), "-c", NODE_CTL, f"--tail={int(lines)}", timeout=30)
        if result.returncode != 0:
            return []
        return [line for line in result.stdout.splitlines() if any(w in line.lower() for w in ("denied", "forbidden", "ssd", "network", "capacity"))]

    # --- nodeSelector extras ------------------------------------------------

    _RENDERER_ENV_KEYS: tuple[str, ...] = (
        f"api:{SELECTOR_KEY_ENV}",
        f"api:{SELECTOR_VALUE_ENV}",
        f"api:{SELECTOR_LABELS_ENV}",
        f"api:{ADMISSION_ENABLED_ENV}",
        f"controller:{SELECTOR_LABELS_ENV}",
        f"internal-api:{SELECTOR_LABELS_ENV}",
    )

    def renderer_env(self) -> dict[str, str | None]:
        api = self._deployment_env_literals(
            self._public_api_deployment(), (SELECTOR_LABELS_ENV, SELECTOR_KEY_ENV, SELECTOR_VALUE_ENV, ADMISSION_ENABLED_ENV)
        )
        controller = self._deployment_env_literals(self._controller_deployment(), (SELECTOR_LABELS_ENV,))
        internal_api = self._deployment_env_literals(self._internal_api_deployment(), (SELECTOR_LABELS_ENV,))
        return {
            **{f"api:{k}": v for k, v in api.items()},
            **{f"controller:{k}": v for k, v in controller.items()},
            **{f"internal-api:{k}": v for k, v in internal_api.items()},
        }

    def set_renderer_env(self, values: Mapping[str, str | None]) -> None:
        for key, value in values.items():
            if value is None:
                continue
            if key.endswith(ADMISSION_ENABLED_ENV) and value not in {"", "true", "false", "1", "0"}:
                raise ValueError(f"refusing admission env value {value!r}")
            if key.endswith(SELECTOR_LABELS_ENV) and not _SELECTOR_EXTRAS_VALUE.fullmatch(value):
                raise ValueError(f"refusing selector extras value {value!r}")
        self._apply_renderer_env(
            values,
            allowed=[k for k in self._RENDERER_ENV_KEYS if not k.endswith((SELECTOR_KEY_ENV, SELECTOR_VALUE_ENV))],
            controller_first=True,
            what="selector-extras",
        )

    def restore_renderer_env(self, standing: Mapping[str, str | None]) -> None:
        self._apply_renderer_env(
            standing,
            allowed=[k for k in self._RENDERER_ENV_KEYS if not k.endswith((SELECTOR_KEY_ENV, SELECTOR_VALUE_ENV))],
            controller_first=False,
            what="selector-extras",
        )

    def nodes_snapshot(self) -> list[NodeSnapshot]:
        result = self.kubectl("get", "nodes", "-o", "json", timeout=15)
        _require_ok(result, "list the cluster's nodes")
        return _parse_nodes(result.stdout)

    def nodes_carrying_label(self, key: str) -> list[str]:
        result = self.kubectl("get", "nodes", "-l", _label_key(key), "-o", "name", timeout=15)
        _require_ok(result, f"list nodes carrying {key}")
        return sorted(_kube_node_name(line.split("/", 1)[-1]) for line in result.stdout.split() if line)

    def node_labels(self, node_name: str) -> dict[str, str]:
        result = self.kubectl("get", "node", _kube_node_name(node_name), "-o", "json", timeout=15)
        _require_ok(result, f"read labels of {node_name}")
        labels = (json.loads(result.stdout or "{}").get("metadata") or {}).get("labels") or {}
        return {str(k): str(v) for k, v in labels.items()}

    def sandbox_pod_render(self, sandbox_id: str) -> SandboxPodRender | None:
        result = self.kubectl(
            "get", "pods", "-l", f"space.pplx.net/sandbox-id={_sql_uuid(sandbox_id)}", "-o", "json", timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        _require_ok(result, "read the sandbox pod")
        return _newest_pod_render(result.stdout)

    def sandbox_pod_count(self) -> int:
        result = self.kubectl(
            "get", "pods", "-l", KIND_SANDBOX_POD_SELECTOR, "-o", "json", timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        _require_ok(result, "count sandbox pods")
        items = json.loads(result.stdout or "{}").get("items", [])
        return sum(1 for item in items if not (item.get("metadata") or {}).get("deletionTimestamp"))

    def native_create_sandbox(self, *, template: str, metadata: Mapping[str, str]) -> str:
        self._require_isolated_kind("native create")
        body = json.dumps(
            {"template": template, "runtime": "fc", "env": {}, "metadata": dict(metadata), "resources": None}
        ).encode()
        request = Request(
            self.config.api_url.rstrip("/") + "/v1/sandboxes",
            method="POST",
            headers={
                "Authorization": "Bearer " + self._admin_token(),
                "Content-Type": "application/json",
                "Idempotency-Key": f"space-smoke-create-{secrets.token_hex(8)}",
            },
            data=body,
        )
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.load(response)
        except HTTPError as error:
            raise RuntimeError(f"native create returned HTTP {error.code}") from None
        except (URLError, OSError, ValueError):
            raise RuntimeError("native create failed") from None
        operation_id = payload.get("operation_id") if isinstance(payload, dict) else None
        raw_id = payload.get("sandbox_id") if isinstance(payload, dict) else None
        if not isinstance(operation_id, str) or not operation_id or not isinstance(raw_id, str) or not raw_id:
            raise RuntimeError("native create returned no operation_id or sandbox_id")
        deadline = time.monotonic() + 180
        while True:
            state = self.operation_state(operation_id)
            if state == "succeeded":
                break
            if state in ("failed", "canceled", "expired") or time.monotonic() >= deadline:
                raise RuntimeError(f"native create operation {operation_id} ended {state or 'pending'}")
            time.sleep(1)
        # The native route answers `sbx_<uuid>`; the pod label and the ECW route
        # speak the bare uuid (`ids::sandbox_label_or_id` strips the prefix).
        return _sql_uuid(raw_id.removeprefix("sbx_"))

    def node_groups(self) -> list[AdminNodeGroupView]:
        listing = self._admin("GET", "node-groups", expect=(200,))
        items = listing.get("data") if isinstance(listing, dict) else listing
        if not isinstance(items, list):
            raise RuntimeError("admin node-groups listing was not an array")
        groups = []
        for item in items:
            if not isinstance(item, dict):
                continue
            org = item.get("org_id")
            count = item.get("nodes_count")
            groups.append(
                AdminNodeGroupView(
                    id=str(item.get("id", "")),
                    name=str(item.get("name", "")),
                    org_id=str(org) if isinstance(org, str) and org else None,
                    is_default=bool(item.get("is_default")),
                    nodes_count=count if isinstance(count, int) else 0,
                )
            )
        return groups

    def try_create_node_group(self, *, name: str, org_id: str | None) -> tuple[int, str]:
        self._require_isolated_kind("admin node-group creation")
        if not re.fullmatch(r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?", name):
            raise ValueError(f"refusing node group name {name!r}")
        body: dict[str, object] = {"name": name, "template_policy": "default"}
        if org_id is not None:
            body["org_id"] = _sql_uuid(org_id)
        request = Request(
            self.config.api_url.rstrip("/") + "/admin/api/v1/node-groups",
            method="POST",
            headers={"Authorization": "Bearer " + self._admin_token(), "Content-Type": "application/json"},
            data=json.dumps(body).encode(),
        )
        try:
            with urlopen(request, timeout=20) as response:
                return int(response.status), response.read().decode(errors="replace")[:2_000]
        except HTTPError as error:
            return error.code, error.read(2_000).decode(errors="replace")
        except (URLError, OSError):
            raise RuntimeError("admin POST node-groups failed") from None

    def worker_boundary_labels(self) -> dict[str, str | None]:
        result = self.kubectl(
            "get", "nodes", "-l", KIND_WORKER_NODE_SELECTOR, "-o", "json", timeout=15,
        )
        _require_ok(result, "list worker node labels")
        labels: dict[str, str | None] = {}
        for item in json.loads(result.stdout or "{}").get("items", []):
            metadata = item.get("metadata", {})
            name = _kube_node_name(str(metadata.get("name", "")))
            value = (metadata.get("labels") or {}).get(NODE_GROUP_BOUNDARY_LABEL)
            labels[name] = value if isinstance(value, str) else None
        return labels

    def restore_worker_boundary_labels(self, snapshot: Mapping[str, str | None]) -> None:
        self._require_isolated_kind("worker label restore")
        for node_name, value in snapshot.items():
            node = _kube_node_name(node_name)
            assignment = f"{NODE_GROUP_BOUNDARY_LABEL}-" if value is None else f"{NODE_GROUP_BOUNDARY_LABEL}={value}"
            _require_ok(
                self.kubectl("label", "node", node, assignment, "--overwrite", timeout=30),
                f"restore the boundary label on {node}",
            )

    def _sandbox_pod_json(self, sandbox_id: str) -> dict[str, Any] | None:
        result = self.kubectl(
            "get", "pods", "-l", f"space.pplx.net/sandbox-id={_sql_uuid(sandbox_id)}", "-o", "json", timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        _require_ok(result, "read the sandbox pod")
        items = json.loads(result.stdout or "{}").get("items", [])
        return items[0] if items else None

    def sandbox_pod_node_group_terms(self, sandbox_id: str) -> list[str]:
        pod = self._sandbox_pod_json(sandbox_id)
        if pod is None:
            return []
        required = (
            ((pod.get("spec") or {}).get("affinity") or {}).get("nodeAffinity") or {}
        ).get("requiredDuringSchedulingIgnoredDuringExecution") or {}
        values: set[str] = set()
        for term in required.get("nodeSelectorTerms") or []:
            for expression in term.get("matchExpressions") or []:
                if expression.get("key") == NODE_GROUP_BOUNDARY_LABEL:
                    values.update(str(v) for v in expression.get("values") or [])
        return sorted(values)

    def sandbox_pod_phase_and_node(self, sandbox_id: str) -> tuple[str | None, str | None]:
        pod = self._sandbox_pod_json(sandbox_id)
        if pod is None:
            return None, None
        return (pod.get("status") or {}).get("phase"), (pod.get("spec") or {}).get("nodeName")

    def admin_org_id(self) -> str:
        """The managing org of the kind admin, read from the admin's own listing route."""

        listing = self._admin("GET", "node-groups", expect=(200,))
        # The node-groups listing carries no org; the users row does, and the
        # session response does not. Read it through psql like the retention
        # reads do, scoped to the seeded admin.
        value = self._psql(
            "SELECT org_id FROM users WHERE email = 'root@local' LIMIT 1;", action="read the admin org"
        ).strip()
        del listing
        if not value:
            raise SmokeConfigurationError("the kind admin has no org_id")
        return value

    # --- pending-backup census ------------------------------------------------

    def object_store_replicas(self) -> int:
        previous = _require_ok(
            self.kubectl(
                "get", "deploy", KIND_OBJECT_STORE_DEPLOYMENT, "-o", "jsonpath={.spec.replicas}", timeout=15
            ),
            "read object-store replicas",
        ).strip()
        return int(previous) if previous.isdigit() else 1

    def scale_object_store(self, replicas: int) -> int:
        self._require_isolated_kind("object-store scaling")
        if replicas < 0 or replicas > 1:
            raise ValueError(f"refusing to scale the object store to {replicas} replicas")
        previous = self.object_store_replicas()
        _require_ok(
            self.kubectl(
                "scale", f"deploy/{KIND_OBJECT_STORE_DEPLOYMENT}", f"--replicas={replicas}", timeout=30
            ),
            f"scale the object store to {replicas}",
        )
        return previous

    def wait_object_store_gone(self, *, timeout: float) -> None:
        result = self.kubectl(
            "wait", "--for=delete", "pod", "-l", KIND_OBJECT_STORE_POD_SELECTOR,
            f"--timeout={int(timeout)}s", timeout=timeout + 15,
        )
        # A selector that matches no pod means the gateway pod is already gone.
        # Current kubectl exits 0 for it; older releases fail with this message.
        if result.returncode != 0 and "no matching resources found" in result.detail():
            return
        _require_ok(result, "await the object-store pod deletion")

    def wait_object_store_rollout(self, *, timeout: float) -> None:
        _require_ok(
            self.kubectl(
                "rollout", "status", f"deploy/{KIND_OBJECT_STORE_DEPLOYMENT}",
                f"--timeout={int(timeout)}s", timeout=timeout + 30,
            ),
            "await the object-store rollout",
        )

    def log_since_time(self) -> str:
        return (datetime.now(UTC) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def suspend_without_waiting(self, sandbox_id: str) -> str:
        self._require_isolated_kind("native suspend")
        public_id = f"sbx_{_sql_uuid(sandbox_id)}"
        request = Request(
            self.config.api_url.rstrip("/") + f"/v1/sandboxes/{quote(public_id, safe='')}:suspend",
            method="POST",
            headers={
                "Authorization": "Bearer " + self._admin_token(),
                "Content-Type": "application/json",
                "Idempotency-Key": f"space-smoke-suspend-{secrets.token_hex(8)}",
            },
            data=b"{}",
        )
        try:
            with urlopen(request, timeout=20) as response:
                payload = json.load(response)
        except HTTPError as error:
            raise RuntimeError(f"native :suspend returned HTTP {error.code}") from None
        except (URLError, OSError, ValueError):
            raise RuntimeError("native :suspend failed") from None
        operation_id = payload.get("operation_id") if isinstance(payload, dict) else None
        if not isinstance(operation_id, str) or not operation_id:
            raise RuntimeError("native :suspend returned no operation_id")
        return operation_id

    def operation_state(self, operation_id: str) -> str:
        payload = self._admin_v1("GET", f"operations/{quote(operation_id, safe='')}")
        state = payload.get("state") if isinstance(payload, dict) else None
        return str(state) if isinstance(state, str) else ""

    def _admin_v1(self, method: str, resource: str) -> Any:
        """A native `/v1` read as the kind admin (operations, sandboxes)."""

        request = Request(
            self.config.api_url.rstrip("/") + "/v1/" + resource,
            method=method,
            headers={"Authorization": "Bearer " + self._admin_token()},
        )
        try:
            with urlopen(request, timeout=20) as response:
                return json.load(response)
        except HTTPError as error:
            raise RuntimeError(f"native {method} /v1/{resource} returned HTTP {error.code}") from None
        except (URLError, OSError, ValueError):
            raise RuntimeError(f"native {method} /v1/{resource} failed") from None

    def snapshot_states(self, sandbox_id: str) -> list[str]:
        out = self._psql(
            SNAPSHOT_STATES_SQL.format(sandbox_id=_sql_uuid(sandbox_id)), action="read snapshot states"
        )
        return [line.strip() for line in out.splitlines() if line.strip()]

    def pending_backup_queue(self, sandbox_id: str) -> list[PendingBackupRow]:
        out = self._psql(
            PENDING_BACKUP_QUEUE_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="read the pending-backup queue",
        )
        rows = []
        for line in out.splitlines():
            artifact, _, kind = line.strip().partition("|")
            if artifact:
                rows.append(PendingBackupRow(artifact_id=artifact, artifact_kind=kind.strip()))
        return rows

    def census_publications(self, *, since_time: str) -> list[CensusPublication]:
        """Full-census publications space-internal-api logged since `since_time`."""

        result = self.kubectl(
            "logs", "-l", KIND_INTERNAL_API_SELECTOR, "--since-time", since_time,
            "--tail=-1", "--prefix=false", timeout=60,
        )
        _require_ok(result, "read space-internal-api logs")
        publications = []
        for line in result.stdout.splitlines():
            if "node state snapshot published" not in line:
                continue
            fields = _tracing_fields(line)
            if fields is None or not _truthy_default(str(fields.get("sandbox_inventory_complete", "")), False):
                continue
            reported = fields.get("pending_local_backups_reported")
            publications.append(
                CensusPublication(
                    node_id=str(fields.get("node_id", "")),
                    outcome=str(fields.get("local_backup_queue_outcome", "")),
                    reported=int(reported) if isinstance(reported, int) or str(reported).isdigit() else 0,
                )
            )
        return publications

    # --- template build (builders) ------------------------------------------

    def builder_nodes(self) -> list[str]:
        result = self.kubectl(
            "get", "pods", "-l", _node_service_selector(NODE_API, role=KIND_BUILDER_ROLE),
            "-o", "json", timeout=15,
        )
        document = json.loads(_require_ok(result, "list builder node-api pods") or "{}")
        ready = set(_ready_pod_names(result.stdout, excluding=""))
        return sorted(
            str(item.get("spec", {}).get("nodeName", ""))
            for item in document.get("items", [])
            if item.get("metadata", {}).get("name") in ready
        )

    def route_builder_service(self, route: str, node_name: str, *, timeout: float = 30) -> None:
        self._require_isolated_kind("builder Service routing")
        route = _routing_value(route)
        pod = self._node_service_pod(node_name, NODE_API, role=KIND_BUILDER_ROLE)
        _require_ok(
            self.kubectl(
                "label", "pod", pod, f"{BUILDER_ROUTING_LABEL}={route}", "--overwrite", timeout=30
            ),
            f"label {pod} for the {route} route",
        )
        patch = json.dumps({"spec": {"selector": {BUILDER_ROUTING_LABEL: route}}})
        _require_ok(
            self.kubectl(
                "patch", "service", KIND_BUILDER_SERVICE, "--type", "merge", "-p", patch, timeout=30
            ),
            f"route {KIND_BUILDER_SERVICE} to {route}",
        )
        deadline = time.monotonic() + timeout
        while True:
            endpoints = self._builder_service_endpoints()
            if endpoints == [pod]:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"{KIND_BUILDER_SERVICE} did not route exclusively to {pod}: endpoints {endpoints}"
                )
            time.sleep(0.2)

    def clear_builder_service_route(self) -> None:
        self._require_isolated_kind("builder Service routing")
        patch = json.dumps({"spec": {"selector": {BUILDER_ROUTING_LABEL: None}}})
        _require_ok(
            self.kubectl(
                "patch", "service", KIND_BUILDER_SERVICE, "--type", "merge", "-p", patch, timeout=30
            ),
            f"clear the {KIND_BUILDER_SERVICE} route",
        )
        _require_ok(
            self.kubectl(
                "label", "pods", "-l", _node_service_selector(NODE_API, role=KIND_BUILDER_ROLE),
                f"{BUILDER_ROUTING_LABEL}-", timeout=30,
            ),
            "remove the routing label from the builders",
        )

    def _builder_service_endpoints(self) -> list[str]:
        result = self.kubectl("get", "endpoints", KIND_BUILDER_SERVICE, "-o", "json", timeout=15)
        if result.returncode != 0:
            return []
        return _endpoint_target_pods(result.stdout)

    def template_build_job(self, alias: str) -> TemplateBuildJob | None:
        row = self._psql(
            TEMPLATE_BUILD_JOB_SQL.format(template_id=_template_alias(alias)),
            action=f"read the build job of {alias}",
        )
        if not row:
            return None
        job_id, node_id, node, build_id = (row.split("|") + ["", "", ""])[:4]
        return TemplateBuildJob(
            daemon_job_id=job_id.strip(),
            builder_node_id=node_id.strip() or None,
            builder_node=node.strip() or None,
            build_id=build_id.strip(),
        )

    def prepared_rootfs_handoff(self, node_name: str, *, window_seconds: int = 60) -> str | None:
        if window_seconds <= 0:
            raise ValueError(f"refusing handoff window {window_seconds!r}")
        script = _PREPARED_ROOTFS_HANDOFF_SCRIPT.replace("__WINDOW_SECONDS__", str(int(window_seconds)))
        result = self._node_shell_exec(node_name, script, timeout=window_seconds + 60)
        return None if result.returncode == 0 else (result.detail() or f"exit {result.returncode}")

    def build_sandbox_started(self, node_name: str) -> bool:
        result = self._node_shell_exec(node_name, _BUILD_SANDBOX_STARTED_SCRIPT, timeout=60)
        if result.returncode not in (0, 3):
            raise RuntimeError(
                f"could not list the build sandbox on {node_name} ({result.returncode}): "
                f"{result.detail() or '<no output>'}"
            )
        return result.returncode == 0

    def published_guest_metadata(self, node_name: str, alias: str) -> GuestMetadata:
        template_dir = shlex.quote(f"{TEMPLATE_CACHE_ROOT}/{_template_alias(alias)}")
        result = self._node_shell_exec(
            node_name, _GUEST_METADATA_SCRIPT.replace("__TEMPLATE_DIR__", template_dir), timeout=120
        )
        _require_ok(result, f"read the published guest metadata of {alias} on {node_name}")
        lines = (result.stdout.splitlines() + ["", "", ""])[:3]
        return GuestMetadata(
            bundle_version=lines[0].strip(),
            spaced_version=lines[1].strip(),
            compatibility_family=lines[2].strip(),
        )

    def containerd_residue(self, node_name: str, image: str) -> list[str]:
        result = self._node_shell_exec(
            node_name, _CONTAINERD_RESIDUE_SCRIPT.replace("__IMAGE__", shlex.quote(image)), timeout=120
        )
        # A failed listing is an inspection failure, not a clean result.
        _require_ok(result, f"inspect containerd build residue on {node_name}")
        return [line for line in result.stdout.splitlines() if line.strip()]

    def delete_template_as_admin(self, alias: str) -> str | None:
        self._require_isolated_kind("admin template delete")
        request = Request(
            self.config.api_url.rstrip("/")
            + "/admin/api/v1/templates/"
            + quote(_template_alias(alias), safe=""),
            method="DELETE",
            headers={
                "Authorization": "Bearer " + self._admin_token(),
                "Idempotency-Key": f"space-smoke-template-delete-{secrets.token_hex(8)}",
            },
        )
        try:
            with urlopen(request, timeout=20) as response:
                payload = json.load(response)
        except HTTPError as error:
            if error.code == 404:
                return None
            raise RuntimeError(f"admin DELETE templates/{alias} returned HTTP {error.code}") from None
        except (URLError, OSError, ValueError):
            raise RuntimeError(f"admin DELETE templates/{alias} failed") from None
        operation_id = payload.get("operation_id") if isinstance(payload, dict) else None
        if not isinstance(operation_id, str) or not operation_id:
            raise RuntimeError(f"admin DELETE templates/{alias} returned no operation_id")
        return operation_id

    # --- rate limiters -----------------------------------------------------

    def _public_api_lease_name(self) -> str:
        context_hash = hashlib.sha256(self.config.context.encode()).hexdigest()[:16]
        return f"space-smoke-public-api-{context_hash}"

    def acquire_public_api_mutation(self) -> Callable[[], None]:
        ownership = self._acquire_named_mutation_lease(
            self._public_api_lease_name(),
            "public API deployment mutation",
            repair=self._repair_recorded_limiter_faults,
        )
        self._public_api_lease_ownership = ownership

        def release() -> None:
            self._release_lease(ownership)
            if self._public_api_lease_ownership is ownership:
                self._public_api_lease_ownership = None

        return release

    def _repair_recorded_limiter_faults(
        self, previous: Mapping[str, object], ownership: _LeaseOwnership
    ) -> None:
        # A previous holder recorded what it changed before changing it, and
        # clears a record only after the restore succeeded. Every write here
        # renews the Lease first, so a contender that lost the acquisition
        # never undoes the winner's fault.
        annotations = _lease_annotations(previous)
        recorded_replicas = annotations.get(REDIS_REPLICAS_ANNOTATION)
        if isinstance(recorded_replicas, str) and recorded_replicas:
            if not recorded_replicas.isdigit():
                raise SmokeConfigurationError("the recorded Redis replica count is not a count")
            self._renew_lease(ownership, "an orphaned Redis scale-down repair")
            self._scale_redis_deployment(max(int(recorded_replicas), 1))
            self.wait_redis_rollout(timeout=120)
            self._renew_lease(
                ownership, "clearing the Redis record", {REDIS_REPLICAS_ANNOTATION: None}
            )
        recorded_env = annotations.get(LIMITER_ENV_ANNOTATION)
        if isinstance(recorded_env, str) and recorded_env:
            standing = _recorded_limiter_env(recorded_env)
            self._renew_lease(ownership, "an orphaned limiter env repair")
            self._apply_public_api_env(
                standing, "restore the limiter env a previous smoke Lease holder left", ownership
            )
            self._renew_lease(
                ownership, "clearing the limiter env record", {LIMITER_ENV_ANNOTATION: None}
            )

    def repair_orphaned_limiter_faults(self) -> bool:
        """Undo a limiter fault an earlier run left recorded, or refuse to proceed.

        Every Kind scenario runs this first: a run killed while Redis was down
        or the caps were shrunk leaves its records on the public-API mutation
        Lease, and only the three scenarios that take that Lease would repair
        them. One Lease read when nothing is recorded. A recorded fault is
        repaired by acquiring the Lease, which is possible once it is released,
        expired or held by a holder proven dead; a live holder is a smoke
        mid-fault, and the caller must not run against it. True if a repair ran.
        """

        if self._kind_context_missing():
            return False
        observed = self.kubectl("get", "lease", self._public_api_lease_name(), "-o", "json", timeout=15)
        if observed.returncode != 0:
            if "notfound" in observed.detail().lower():
                return False
            raise SmokeConfigurationError(
                "could not inspect the public API mutation Lease for an orphaned limiter fault: "
                f"{observed.detail() or '<no output>'}"
            )
        document = _lease_document(observed.stdout)
        annotations = _lease_annotations(document)
        if not any(annotations.get(key) for key in (LIMITER_ENV_ANNOTATION, REDIS_REPLICAS_ANNOTATION)):
            return False
        try:
            release = self.acquire_public_api_mutation()
        except SmokeConfigurationError as error:
            _, spec = _lease_parts(document)
            raise SmokeConfigurationError(
                "refusing to run while the public API mutation Lease records a limiter fault "
                f"(holder {spec.get('holderIdentity') or '<none>'}): {error}"
            ) from error
        release()
        return True

    def repair_orphaned_rollout_pace(self) -> bool:
        """Put back a rollout pace an earlier run left recorded on a Deployment, or refuse to proceed.

        Every Kind scenario runs this first, beside the limiter repair: a run
        killed, or out of cleanup time, between `_fast_rollout`'s speed-up and
        its restore leaves a Deployment surging every replica without
        `minReadySeconds`, and a later scenario whose subject is that pace
        (an API restart, the leader-election brownout) would run without it.
        One Deployment listing when nothing is recorded. Fast rollouts run
        only under the public-API mutation Lease, so the repair acquires it:
        a live holder may be mid-rollout, and the caller must not run against
        it. True if a repair ran.
        """

        if self._kind_context_missing():
            return False
        listing = json.loads(
            _require_ok(
                self.kubectl("get", "deploy", "-o", "json", timeout=15),
                "list Deployments for a fast-rollout pace an earlier run left",
            )
            or "{}"
        )
        recorded = sorted(
            item["metadata"]["name"]
            for item in listing.get("items", [])
            if FAST_ROLLOUT_STANDING_ANNOTATION in (item["metadata"].get("annotations") or {})
        )
        if not recorded:
            return False
        self._require_isolated_kind("orphaned rollout-pace repair")
        try:
            release = self.acquire_public_api_mutation()
        except SmokeConfigurationError as error:
            raise SmokeConfigurationError(
                f"refusing to run while another smoke may still own the fast-rollout pace "
                f"recorded on {', '.join(recorded)}: {error}"
            ) from error
        try:
            ownership = self._held_public_api_lease("an orphaned rollout-pace repair")
            for name in recorded:
                self._assert_lease_owned(ownership, f"restoring the rollout pace of {name}")
                self._restore_rollout_pace(name, None)
        finally:
            release()
        return True

    def _kind_context_missing(self) -> bool:
        """No such kubectl context yet: the run bootstraps a fresh cluster, which has no faults."""

        contexts = self.executor.run(["kubectl", "config", "get-contexts", "-o", "name"], timeout=15)
        return contexts.returncode == 0 and self.config.context not in contexts.stdout.split()

    def _held_public_api_lease(self, action: str) -> _LeaseOwnership:
        ownership = self._public_api_lease_ownership
        if ownership is None:
            raise SmokeConfigurationError(
                f"refusing {action} without the caller's live mutation Lease"
            )
        return ownership

    def _lease_annotation(self, ownership: _LeaseOwnership, key: str) -> str | None:
        current = self.kubectl("get", "lease", ownership.name, "-o", "json")
        _require_ok(current, f"read the {ownership.subject} Lease")
        value = _lease_annotations(_lease_document(current.stdout)).get(key)
        return value if isinstance(value, str) and value else None

    def _public_api_container(self, name: str) -> str:
        container = _require_ok(
            self.kubectl(
                "get", "deploy", name, "-o",
                "jsonpath={.spec.template.spec.containers[0].name}", timeout=15,
            ),
            f"read the container name of {name}",
        ).strip()
        if not container:
            raise SmokeConfigurationError(f"{name} has no first container")
        return container

    def public_api_env(self, keys: Sequence[str]) -> dict[str, str | None]:
        """The literal value each key holds on the public API Deployment, None if unset.

        A `valueFrom` entry is refused: `set env NAME=literal` would replace the
        reference and the restore could not put it back.
        """

        return self._deployment_env_literals(self._public_api_deployment(), keys)

    def _apply_public_api_env(
        self, values: Mapping[str, str | None], action: str, ownership: _LeaseOwnership | None = None
    ) -> None:
        _require_limiter_env_keys(values)
        self._require_isolated_kind("public API env override")
        assignments = [
            f"{key}-" if value is None else f"{key}={value}" for key, value in values.items()
        ]
        if not assignments:
            return
        name = self._public_api_deployment()
        container = self._public_api_container(name)
        with self._fast_rollout(name, ownership):
            _require_ok(
                self.kubectl("set", "env", f"deploy/{name}", "-c", container, *assignments, timeout=30),
                action,
            )
            self.wait_public_api_rollout(timeout=240)

    def set_public_api_env(self, overrides: Mapping[str, str]) -> None:
        from space_smoke.rate_limiters import LIMITER_ENV_KEYS

        for key, value in overrides.items():
            if not _LIMITER_ENV_VALUE.fullmatch(value):
                raise ValueError(f"refusing limiter env value {value!r} for {key}")
        _require_limiter_env_keys(overrides)
        action = "a public API limiter override"
        ownership = self._held_public_api_lease(action)
        # Record first, and only the first time: a run that dies between here
        # and the write leaves a record of values still standing, which the
        # next holder restores to no effect, and a second override in the same
        # run must not record the first one's values as standing.
        record: dict[str, str | None] = {}
        if self._lease_annotation(ownership, LIMITER_ENV_ANNOTATION) is None:
            standing = self.public_api_env(LIMITER_ENV_KEYS)
            record[LIMITER_ENV_ANNOTATION] = json.dumps(standing, sort_keys=True)
        self._renew_lease(ownership, action, record)
        self._apply_public_api_env(overrides, "set the public API limiter env")

    def restore_public_api_env(self, standing: Mapping[str, str | None]) -> None:
        """Put the standing env back, then clear its record; both only while the Lease is ours.

        A runner that lost its Lease refuses: the successor that took it over
        has already restored the record, and may have a fault of its own up.
        A failed restore keeps the record for the next holder.
        """

        action = "restoring the public API limiter env"
        ownership = self._held_public_api_lease(action)
        self._renew_lease(ownership, action)
        self._apply_public_api_env(standing, "restore the public API limiter env")
        self._renew_lease(
            ownership, "clearing the limiter env record", {LIMITER_ENV_ANNOTATION: None}
        )

    def wait_public_api_ready(self, *, timeout: float) -> None:
        """`/readyz`, not `/healthz`: readiness gates the Service on the reflector's initial LIST."""

        deadline = time.monotonic() + timeout
        url = f"{self.config.api_url.rstrip('/')}/readyz"
        while True:
            try:
                with urlopen(Request(url), timeout=5) as response:
                    if response.status == 200:
                        return
            except (HTTPError, URLError, OSError):
                pass
            if time.monotonic() > deadline:
                raise RuntimeError(f"the public API did not report /readyz=200 within {timeout:g}s")
            time.sleep(1)

    def redis_replicas(self) -> int:
        previous = _require_ok(
            self.kubectl(
                "get", "deploy", KIND_REDIS_DEPLOYMENT, "-o", "jsonpath={.spec.replicas}", timeout=15
            ),
            "read Redis replicas",
        ).strip()
        return int(previous) if previous.isdigit() else 1

    def scale_redis(self, replicas: int) -> int:
        """Scale the limiters' store, only while the public-API Lease is ours.

        Every Kind API, admin API and cluster-controller replica runs its
        limiters against this store, so scaling it to zero is recorded on the
        Lease first, where the next holder scales it back if this run does
        not. A runner that lost its Lease refuses either way, so it cannot
        undo or erase a successor's fault.
        """

        self._require_isolated_kind("Redis scaling")
        if replicas < 0 or replicas > 1:
            raise ValueError(f"refusing to scale Redis to {replicas} replicas")
        action = f"scaling Redis to {replicas}"
        ownership = self._held_public_api_lease(action)
        previous = self.redis_replicas()
        if replicas == 0:
            record: dict[str, str | None] = {}
            if self._lease_annotation(ownership, REDIS_REPLICAS_ANNOTATION) is None:
                record[REDIS_REPLICAS_ANNOTATION] = str(previous)
            self._renew_lease(ownership, action, record)
            self._scale_redis_deployment(0)
        else:
            self._renew_lease(ownership, action)
            self._scale_redis_deployment(replicas)
            self._renew_lease(
                ownership, "clearing the Redis record", {REDIS_REPLICAS_ANNOTATION: None}
            )
        return previous

    def _scale_redis_deployment(self, replicas: int) -> None:
        _require_ok(
            self.kubectl(
                "scale", f"deploy/{KIND_REDIS_DEPLOYMENT}", f"--replicas={replicas}", timeout=30
            ),
            f"scale Redis to {replicas}",
        )

    def wait_redis_unreachable(self, *, timeout: float) -> None:
        """No Redis pod object left, then endpoints empty.

        The API removes a deleted pod's object only once the kubelet reports
        its containers stopped: as soon as Redis exits on SIGTERM, at the
        latest when the grace period ends. Endpoints empty as soon as it is
        NotReady, while it may still serve. With the object gone no
        Redis process is left to answer, so a pooled connection can only error
        or hit the limiters' 500 ms store timeout, and both fail open.
        """

        deadline = time.monotonic() + timeout
        while True:
            pods = _require_ok(
                self.kubectl("get", "pods", "-l", KIND_REDIS_POD_SELECTOR, "-o", "name", timeout=15),
                "list Redis pods",
            ).split()
            if not pods:
                break
            if time.monotonic() > deadline:
                raise RuntimeError(f"Redis pods {pods} still exist {timeout:g}s after the scale to zero")
            time.sleep(0.5)
        deadline = time.monotonic() + 30
        while True:
            result = self.kubectl(
                "get", "endpoints", KIND_REDIS_DEPLOYMENT, "-o",
                "jsonpath={.subsets[*].addresses[*].ip}", timeout=15,
            )
            if result.returncode == 0 and not result.stdout.strip():
                break
            if time.monotonic() > deadline:
                raise RuntimeError("Redis endpoints did not drain after the pod left")
            time.sleep(1)

    def wait_redis_rollout(self, *, timeout: float) -> None:
        _require_ok(
            self.kubectl(
                "rollout", "status", f"deploy/{KIND_REDIS_DEPLOYMENT}",
                f"--timeout={int(timeout)}s", timeout=timeout + 30,
            ),
            "await the Redis rollout",
        )

    # --- node drain ----------------------------------------------------------

    def _admin_token(self) -> str:
        """The kind admin bearer: the CI token file, else the seeded root@local login."""

        token_file = self.environment.get("SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE")
        if token_file:
            token = Path(token_file).read_text().strip()
            if token:
                return token
        self._require_isolated_kind("admin login")
        request = Request(
            self.config.api_url.rstrip("/") + "/auth/v1/sessions",
            data=json.dumps(
                {
                    "email": "root@local",
                    "password": self.environment.get("SPACE_CONSOLE_PASSWORD") or "password",
                }
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=15) as response:
                token = json.load(response).get("token")
        except (HTTPError, URLError, OSError, ValueError):
            raise SmokeConfigurationError("kind admin login failed") from None
        if not isinstance(token, str) or not token:
            raise SmokeConfigurationError("kind admin login returned no token")
        return token

    def _admin(
        self, method: str, resource: str, *, expect: Collection[int], body: Any = None
    ) -> Any:
        self._require_isolated_kind("admin node operations")
        headers = {"Authorization": "Bearer " + self._admin_token()}
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = Request(
            self.config.api_url.rstrip("/") + "/admin/api/v1/" + resource,
            method=method,
            headers=headers,
            data=None if body is None else json.dumps(body).encode(),
        )
        try:
            with urlopen(request, timeout=20) as response:
                if response.status not in expect:
                    raise RuntimeError(f"admin {method} {resource} returned HTTP {response.status}")
                raw = response.read()
        except HTTPError as error:
            raise RuntimeError(f"admin {method} {resource} returned HTTP {error.code}") from None
        except (URLError, OSError):
            raise RuntimeError(f"admin {method} {resource} failed") from None
        return json.loads(raw) if raw else None

    def admin_nodes(self) -> list[AdminNodeView]:
        listing = self._admin("GET", "nodes", expect=(200,))
        if not isinstance(listing, list):
            raise RuntimeError("admin nodes listing was not an array")
        nodes = []
        for item in listing:
            if not isinstance(item, dict):
                continue
            active = item.get("sandboxes_active")
            phase = item.get("drain_phase")
            mode = item.get("drain_mode")
            nodes.append(
                AdminNodeView(
                    id=str(item.get("id", "")),
                    k8s_node_name=str(item.get("k8s_node_name") or ""),
                    drain_phase=phase if isinstance(phase, str) else None,
                    drain_mode=mode if isinstance(mode, str) else None,
                    sandboxes_active=active if isinstance(active, int) else None,
                )
            )
        return nodes

    def drain_node_default_mode(self, node_id: str) -> None:
        self._admin("POST", f"nodes/{quote(_sql_uuid(node_id), safe='')}:drain", expect=(200, 202))

    def drain_node_forced(self, node_id: str) -> None:
        self._admin(
            "POST",
            f"nodes/{quote(_sql_uuid(node_id), safe='')}:drain",
            expect=(200, 202),
            body={"mode": "forced"},
        )

    def undrain_node(self, node_id: str) -> None:
        self._admin("POST", f"nodes/{quote(_sql_uuid(node_id), safe='')}:undrain", expect=(200, 202))

    def set_drain_keep(self, node_name: str, value: str) -> None:
        self._require_isolated_kind("drain-keep override")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,63}", value):
            raise ValueError(f"refusing drain-keep value {value!r}")
        node = _kube_node_name(node_name)
        _require_ok(
            self.kubectl(
                "annotate", "node", node, f"{DRAIN_KEEP_ANNOTATION}={value}", "--overwrite", timeout=30
            ),
            f"set the drain-keep override on {node}",
        )

    def clear_drain_keep(self, node_name: str) -> None:
        node = _kube_node_name(node_name)
        _require_ok(
            self.kubectl("annotate", "node", node, f"{DRAIN_KEEP_ANNOTATION}-", "--overwrite", timeout=30),
            f"clear the drain-keep override on {node}",
        )

    def kube_node_unschedulable(self, node_name: str) -> bool:
        node = _kube_node_name(node_name)
        value = _require_ok(
            self.kubectl("get", "node", node, "-o", "jsonpath={.spec.unschedulable}", timeout=15),
            f"read unschedulable of {node}",
        ).strip()
        return value == "true"

    def kube_node_exists(self, node_name: str) -> bool:
        node = _kube_node_name(node_name)
        return self.kubectl("get", "node", node, "-o", "name", timeout=15).returncode == 0

    def sandbox_residency(self, sandbox_id: str) -> SandboxResidency:
        row = self._psql(
            SANDBOX_RESIDENCY_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="read sandbox residency",
        )
        if not row:
            raise RuntimeError(f"sandbox {sandbox_id} has no sandboxes row")
        lifecycle, _, node = row.partition("|")
        return SandboxResidency(lifecycle=lifecycle.strip(), active_node_id=node.strip() or None)

    def pin_namespace_pod_count(self) -> int:
        self._require_isolated_kind("namespace pod-count quota")
        # `kubectl get pods` over-counts: the quota charges only non-terminal
        # pods, and a naive count includes terminal ones (Completed seed-publish
        # and migrate Jobs, when sandboxes shared the control-plane namespace —
        # the first attempt pinned 16 against a real usage of 14 and admitted
        # the POST — and finished sandbox pods here). Create the quota with a wide
        # cap, let the controller report what it actually charges, then pin the
        # cap to exactly that number.
        wide = (
            "apiVersion: v1\nkind: ResourceQuota\n"
            f"metadata:\n  name: {POD_CAP_QUOTA_NAME}\n  namespace: {KIND_SANDBOX_NAMESPACE}\n"
            'spec:\n  hard:\n    pods: "10000"\n'
        )
        _require_ok(
            self.kubectl(
                "apply", "-f", "-", input_text=wide, timeout=30, namespace=KIND_SANDBOX_NAMESPACE
            ),
            "apply the pod-count quota",
        )
        deadline = time.monotonic() + 60
        while True:
            used = self.kubectl(
                "get", "resourcequota", POD_CAP_QUOTA_NAME,
                "-o", "jsonpath={.status.used.pods}", timeout=15,
                namespace=KIND_SANDBOX_NAMESPACE,
            )
            value = used.stdout.strip() if used.returncode == 0 else ""
            if value.isdigit():
                break
            if time.monotonic() > deadline:
                raise RuntimeError("the pod-count quota never reported usage")
            time.sleep(1)
        count = int(value)
        pinned = wide.replace('pods: "10000"', f'pods: "{count}"')
        _require_ok(
            self.kubectl(
                "apply", "-f", "-", input_text=pinned, timeout=30, namespace=KIND_SANDBOX_NAMESPACE
            ),
            "pin the pod-count quota",
        )
        # Admission reads the quota's status; wait until the pinned hard value
        # is reflected there so the next POST is refused, not admitted.
        deadline = time.monotonic() + 60
        while True:
            hard = self.kubectl(
                "get", "resourcequota", POD_CAP_QUOTA_NAME,
                "-o", "jsonpath={.status.hard.pods}", timeout=15,
                namespace=KIND_SANDBOX_NAMESPACE,
            )
            if hard.returncode == 0 and hard.stdout.strip() == str(count):
                return count
            if time.monotonic() > deadline:
                raise RuntimeError("the pinned pod-count quota never reached status.hard")
            time.sleep(1)

    def lift_namespace_pod_cap(self) -> None:
        result = self.kubectl(
            "delete", "resourcequota", POD_CAP_QUOTA_NAME, "--ignore-not-found", timeout=30,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        _require_ok(result, "delete the pod-count quota")

    def sandbox_executions(self, sandbox_id: str) -> list[ExecutionRow]:
        rows = self._psql(
            SANDBOX_EXECUTIONS_SQL.format(sandbox_id=_sql_uuid(sandbox_id)),
            action="list sandbox execution rows",
        )
        executions: list[ExecutionRow] = []
        for line in rows.splitlines():
            if not line.strip():
                continue
            generation, state, active, pod_name = line.split("|", 3)
            executions.append(
                ExecutionRow(
                    generation=int(generation),
                    state=state,
                    active=active.strip() == "t",
                    pod_name=pod_name,
                )
            )
        return executions

    def sandbox_pods(self, sandbox_id: str) -> list[SandboxPodView]:
        result = self.kubectl(
            "get", "pods", "-l", f"{SANDBOX_ID_LABEL}={_sql_uuid(sandbox_id)}",
            "-o", "json", timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        _require_ok(result, "list sandbox pods")
        pods: list[SandboxPodView] = []
        for pod in json.loads(result.stdout or "{}").get("items", []):
            metadata = pod.get("metadata", {}) or {}
            annotations = metadata.get("annotations", {}) or {}
            pods.append(
                SandboxPodView(
                    pod_name=str(metadata.get("name", "")),
                    phase=str((pod.get("status") or {}).get("phase", "")),
                    node_name=str((pod.get("spec") or {}).get("nodeName", "") or ""),
                    execution_generation=str(annotations.get(EXECUTION_GENERATION_ANNOTATION, "")),
                    checkpoint=str(annotations.get(CHECKPOINT_ANNOTATION, "")),
                    restore_source_node=str(annotations.get(RESTORE_SOURCE_NODE_ANNOTATION, "")),
                    restore_storage_uri=str(annotations.get(RESTORE_STORAGE_URI_ANNOTATION, "")),
                    deleting=bool(metadata.get("deletionTimestamp")),
                )
            )
        return sorted(pods, key=lambda p: p.pod_name)

    def delete_sandbox_pod(self, pod_name: str) -> None:
        self._require_isolated_kind("sandbox pod deletion")
        pod = _kube_pod_name(pod_name)
        if not pod.startswith("space-sbx-"):
            raise ValueError(f"refusing to delete a pod that is not a sandbox execution pod: {pod}")
        _require_ok(
            self.kubectl(
                "delete", "pod", pod, "--wait=false", timeout=60, namespace=KIND_SANDBOX_NAMESPACE
            ),
            f"delete sandbox pod {pod}",
        )

    # --- reconcile-queue GC -------------------------------------------------
    #
    # Named reads and the one seed; the rows' shape and the verdicts live in
    # `space_smoke.reconcile_queue_gc`.

    def reconcile_queue_gc_env(self) -> dict[str, str | None]:
        return self._deployment_env_literals(self._controller_deployment(), RECONCILE_QUEUE_GC_ENV_KEYS)

    def unregistered_ready_node(self) -> str:
        registered = set(
            self._psql(
                "SELECT DISTINCT k8s_node_name FROM node_k8s;\n",
                action="list the node names SPACE rows carry",
            ).split()
        )
        candidates = sorted(
            (CONTROL_PLANE_LABEL not in node.labels, node.name)
            for node in self.nodes_snapshot()
            if node.ready and node.name not in registered
        )
        if not candidates:
            raise SmokeConfigurationError("no Ready Kubernetes Node is free of SPACE node rows")
        return validate_node_name(candidates[0][1])

    def node_exists(self, name: str) -> bool:
        result = self.kubectl(
            "get", "node", validate_node_name(name), "--ignore-not-found", "-o", "name", timeout=15
        )
        _require_ok(result, f"look up node {name}")
        return bool(result.stdout.strip())

    def seed_reconcile_queue_gc_nodes(
        self,
        *,
        live: tuple[str, str],
        gone: tuple[str, str],
        marker: str,
    ) -> None:
        self._require_isolated_kind("reconcile-queue GC seeding")
        self._psql(
            RECONCILE_QUEUE_GC_SEED_SQL.format(
                live_id=_sql_uuid(live[0]),
                live_name=validate_node_name(live[1]),
                gone_id=_sql_uuid(gone[0]),
                gone_name=validate_node_name(gone[1]),
                marker=validate_marker(marker),
            ),
            action="seed the reconcile-queue GC's synthetic nodes",
        )

    def reconcile_queue_refs(self, node_ids: Sequence[str]) -> list[ReconcileQueueRef]:
        out = self._psql(
            "SELECT node_id, facet, dead_at IS NOT NULL FROM node_reconcile_queue "
            f"WHERE node_id IN ({_sql_uuid_list(node_ids)}) ORDER BY 1, 2;\n",
            action="read the seeded nodes' reconcile queue",
        )
        refs = []
        for line in out.splitlines():
            node_id, _, rest = line.strip().partition("|")
            facet, _, dead = rest.partition("|")
            if node_id:
                refs.append(ReconcileQueueRef(node_id=node_id, facet=facet, dead=dead == "t"))
        return refs

    def delete_seeded_reconcile_queue_nodes(self, node_ids: Sequence[str], *, marker: str) -> int:
        ids = _sql_uuid_list(node_ids)
        out = self._psql(
            "DELETE FROM nodes n USING node_k8s k WHERE k.node_id = n.id "
            f"AND n.id IN ({ids}) AND k.k8s_node_uid = '{validate_marker(marker)}';\n"
            f"SELECT COUNT(*) FROM nodes WHERE id IN ({ids});\n",
            action="delete the reconcile-queue GC's synthetic nodes",
        )
        return int(out.splitlines()[-1])

    def fake_ec2_log_tail(self) -> str:
        result = self.kubectl(
            "logs", "deployment/space-kind-fake-ec2", "--tail=40", "--timestamps", timeout=30
        )
        return result.stdout if result.returncode == 0 else result.stderr

    # --- orphaned-restore-pod GC ---------------------------------------------
    #
    # Thin kubectl wrappers; the key/value lists, the clone shape and the
    # verdicts live in `space_smoke.orphaned_restore_pod_gc`.

    def acquire_controller_mutation(self) -> Callable[[], None]:
        # The retention Lease already serializes cooperating smokes' writes to
        # the cluster-controller Deployment; this is the same resource.
        return self.acquire_retention_sweeper_mutation()

    def orphaned_restore_pod_gc_env(self) -> dict[str, str | None]:
        return self._deployment_env_literals(self._controller_deployment(), GC_ENV_KEYS)

    def set_orphaned_restore_pod_gc_env(self, overrides: Mapping[str, str]) -> None:
        validate_gc_overrides(overrides)
        self._apply_orphaned_restore_pod_gc_env(overrides)

    def restore_orphaned_restore_pod_gc_env(self, standing: Mapping[str, str | None]) -> None:
        # The captured literals go back verbatim: the controller falls back to
        # a default on an unparseable value, and restoring must not tighten it.
        validate_gc_keys(standing)
        self._apply_orphaned_restore_pod_gc_env(standing)

    def _apply_orphaned_restore_pod_gc_env(self, values: Mapping[str, str | None]) -> None:
        self._require_isolated_kind("orphaned-restore-pod GC env override")
        ownership = self._retention_lease_ownership
        if ownership is None:
            raise SmokeConfigurationError(
                "refusing an orphaned-restore-pod GC controller deployment write without the "
                "caller's live mutation Lease"
            )
        self._assert_lease_owned(ownership, "an orphaned-restore-pod GC controller deployment write")
        # At the charts' pace, not `_fast_rollout`'s, as for the retention
        # sweeper: the GC runs on the Lease holder, and the fast rollout's
        # earlier return was spent waiting for the new holder's first reaps
        # (bench: 125-127 s at this pace, 123-126 s at the fast one).
        self._set_deployment_env(self._controller_deployment(), values, timeout=180)

    def controller_log_records(self, *, since_time: str) -> list[dict[str, Any]]:
        listing = self.kubectl(
            "get", "pods", "-l", KIND_CONTROLLER_SELECTOR, "-o", "json", timeout=15
        )
        _require_ok(listing, "list cluster-controller pods")
        records: list[dict[str, Any]] = []
        for pod in json.loads(listing.stdout or "{}").get("items", []):
            metadata = pod.get("metadata") or {}
            containers = (pod.get("spec") or {}).get("containers") or []
            if (
                metadata.get("deletionTimestamp")
                or (pod.get("status") or {}).get("phase") != "Running"
                or not containers
            ):
                continue
            name = _kube_pod_name(str(metadata.get("name", "")))
            logs = self.kubectl(
                "logs", name, "-c", str(containers[0].get("name", "")),
                "--since-time", since_time, timeout=60,
            )
            _require_ok(logs, f"read cluster-controller logs on {name}")
            for line in logs.stdout.splitlines():
                if GC_LOG_NEEDLE not in line:
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                if isinstance(record, dict):
                    records.append(record)
        return records

    def restore_pod_document(self, pod_name: str) -> dict[str, Any] | None:
        pod = _kube_pod_name(pod_name)
        result = self.kubectl(
            "get", "pod", pod, "-o", "json", timeout=15, namespace=KIND_SANDBOX_NAMESPACE
        )
        if result.returncode != 0 and "notfound" in result.detail().lower().replace(" ", ""):
            return None
        document = json.loads(_require_ok(result, f"read pod {pod}") or "{}")
        return document if isinstance(document, dict) else None

    def plant_orphaned_restore_pod(
        self,
        template: Mapping[str, Any],
        *,
        name: str,
        sandbox_id: str,
        generation: int,
        token: str,
    ) -> str:
        self._require_isolated_kind("planting an orphaned restore pod")
        manifest = build_planted_orphan(
            template,
            name=_kube_pod_name(name),
            sandbox_id=sandbox_id,
            generation=generation,
            token=token,
        )
        created = self.kubectl(
            "create", "-f", "-", "-o", "json", input_text=json.dumps(manifest), timeout=30,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        uid = (json.loads(_require_ok(created, f"create the planted orphan {name}") or "{}")
               .get("metadata", {}).get("uid"))
        if not isinstance(uid, str) or not uid:
            raise RuntimeError(f"the planted orphan {name} came back without a uid")
        return uid

    def remove_planted_restore_pods(self, token: str) -> None:
        # Select on the per-plant label only, never a name: whatever else the
        # namespace holds, only this plant can match.
        if not re.fullmatch(r"[a-f0-9]{8}", token):
            raise ValueError(f"refusing plant token {token!r}")
        _require_ok(
            self.kubectl(
                "delete", "pods", "-l", f"{PLANTED_ORPHAN_LABEL}={token}",
                "--ignore-not-found", "--wait=false", timeout=30,
                namespace=KIND_SANDBOX_NAMESPACE,
            ),
            f"remove planted orphan pods {token}",
        )

    # --- template-manager startup recovery -------------------------------

    def other_sandbox_worker_node(self, excluding_node: str) -> str:
        nodes = [n for n in self.sandbox_pool_nodes() if n != _kube_node_name(excluding_node)]
        if not nodes:
            raise SmokeConfigurationError(
                f"no sandbox-pool node other than {excluding_node}; the startup "
                "recovery scenario needs a second worker to restart"
            )
        return nodes[0]

    def _template_digest_path(self, template: str, digest: str) -> str:
        return f"{TEMPLATE_CACHE_ROOT}/{_smoke_template_name(template)}/{_template_digest(digest)}"

    def remove_template_digest_dir(self, node_name: str, template: str, digest: str) -> None:
        path = shlex.quote(self._template_digest_path(template, digest))
        _require_ok(
            self._node_shell_exec(node_name, f"rm -rf {path}", timeout=60),
            f"remove digest dir {digest} of {template} on {node_name}",
        )

    def orphan_template_digest_dir(self, node_name: str, template: str, digest: str) -> None:
        name = _smoke_template_name(template)
        digest = _template_digest(digest)
        name_dir = f"{TEMPLATE_CACHE_ROOT}/{name}"
        stash = _template_stash_dir(name)
        # The stash sits beside the name directories under a dot-prefixed name,
        # which recovery's directory walk skips; `delete_template` removes it
        # too, so a failure mid-way leaves nothing the scope does not own.
        script = " && ".join(
            [
                "set -eu",
                f"test -d {shlex.quote(f'{name_dir}/{digest}')}",
                f"rm -rf {shlex.quote(stash)}",
                f"mkdir -p {shlex.quote(stash)}",
                f"mv {shlex.quote(f'{name_dir}/{digest}')} {shlex.quote(stash)}/",
                f"space-tmplmgr delete-template --template {name}",
                f"rm -rf {shlex.quote(name_dir)}",
                f"install -d -m 2770 -o {TEMPLATE_CACHE_OWNER} -g {TEMPLATE_CACHE_GROUP} "
                f"{shlex.quote(name_dir)}",
                f"mv {shlex.quote(f'{stash}/{digest}')} {shlex.quote(name_dir)}/",
                f"rmdir {shlex.quote(stash)}",
            ]
        )
        _require_ok(
            self._node_shell_exec(node_name, script, timeout=120),
            f"orphan digest dir {digest} of {template} on {node_name}",
        )

    def corrupt_template_rootfs(self, node_name: str, template: str, digest: str) -> None:
        rootfs = shlex.quote(f"{self._template_digest_path(template, digest)}/rootfs.ext4")
        script = " && ".join(
            [
                "set -eu",
                f"test -f {rootfs}",
                f"dd if=/dev/zero of={rootfs} bs=4096 count=1 conv=notrunc status=none",
                "sync",
            ]
        )
        _require_ok(
            self._node_shell_exec(node_name, script, timeout=60),
            f"corrupt rootfs of {template}/{digest} on {node_name}",
        )

    def truncate_template_rootfs(
        self, node_name: str, template: str, digest: str, *, bytes_removed: int
    ) -> None:
        if bytes_removed <= 0 or bytes_removed > 1024**2:
            raise ValueError(f"refusing to truncate by {bytes_removed} bytes")
        rootfs = shlex.quote(f"{self._template_digest_path(template, digest)}/rootfs.ext4")
        script = " && ".join(
            [
                "set -eu",
                f"cur=$(stat -c %s {rootfs})",
                f'test "$cur" -gt {bytes_removed}',
                f'truncate -s "$((cur - {bytes_removed}))" {rootfs}',
                "sync",
            ]
        )
        _require_ok(
            self._node_shell_exec(node_name, script, timeout=60),
            f"truncate rootfs of {template}/{digest} on {node_name}",
        )

    def restart_node_service(self, node_name: str, component: str, *, timeout: float) -> str:
        """Delete one node service's pod and wait for its Ready replacement.

        Only that service restarts: the node's sandboxes keep running under the
        host runtime-supervisor, and its other services keep serving.
        """

        self._require_isolated_kind("node-service restarts")
        pod = self._node_service_pod(node_name, component)
        _require_ok(
            self.kubectl("delete", "pod", pod, "--wait=true", timeout=timeout),
            f"delete {component} pod {pod}",
        )
        deadline = time.monotonic() + timeout
        while True:
            result = self.kubectl(
                "get",
                "pods",
                "-l",
                _node_service_selector(component),
                "--field-selector",
                f"spec.nodeName={_kube_node_name(node_name)}",
                "-o",
                "json",
                timeout=15,
            )
            ready = _ready_pod_names(
                _require_ok(result, f"list {component} pods on {node_name}"), excluding=pod
            )
            if len(ready) == 1:
                return ready[0]
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"no Ready replacement {component} pod on {node_name} within "
                    f"{timeout:.0f}s after deleting {pod} (ready: {ready})"
                )
            time.sleep(5)

    def startup_recovery_log(self, node_name: str) -> list[str]:
        pod = self._node_service_pod(node_name, NODE_TEMPLATE_MANAGER)
        # Oldest attempt first: a failed startup restarts the container, and
        # its interrupted pass is then only in the previous attempt's log.
        previous = self.kubectl(
            "logs", pod, "-c", NODE_TEMPLATE_MANAGER, "--previous", timeout=60
        )
        current = self.kubectl("logs", pod, "-c", NODE_TEMPLATE_MANAGER, timeout=60)
        log = _require_ok(current, f"read template-manager log on {node_name}")
        if previous.returncode == 0:
            log = previous.stdout + log
        return [
            _redact_storage_uris(line)[:400]
            for line in log.splitlines()
            if "startup recovery" in line or "smoke-rec-" in line
        ][-80:]

    def telemetry_query(self, backend: str, path: str, params: Mapping[str, str]) -> Any:
        base = KIND_TELEMETRY_BACKENDS[backend]
        url = base + path
        if params:
            url += "?" + urlencode(params)
        try:
            with urlopen(Request(url), timeout=30) as response:
                return json.load(response)
        except HTTPError as error:
            raise RuntimeError(f"query {backend} {path} at {base} returned HTTP {error.code}") from None
        except (URLError, OSError, ValueError) as error:
            # The URL a forward serves, not the long query string.
            reason = error.reason if isinstance(error, URLError) else error
            raise RuntimeError(f"query {backend} {path} at {base} failed: {reason}") from None

    def telemetry_mode(self) -> str:
        name = self._public_api_deployment()
        exporters = self._read_deployment_env(name, ("SPACE_METRICS_SINK", "SPACE_TRACE_EXPORTER"))
        metrics, traces = exporters["SPACE_METRICS_SINK"], exporters["SPACE_TRACE_EXPORTER"]
        mode = KIND_TELEMETRY_MODES.get((metrics, traces))
        if mode is None:
            raise SmokeConfigurationError(
                f"{name} runs no Kind telemetry mode: SPACE_METRICS_SINK={metrics or 'unset'}, "
                f"SPACE_TRACE_EXPORTER={traces or 'unset'}"
            )
        return mode

    def rust_build(self) -> str:
        # Read from the images every running SPACE Rust container runs, checked
        # against the build each pod was rendered for; the bring-up uses the
        # same reader.
        result = self.executor.run(
            [sys.executable, str(REPO_ROOT / "infra/space/kind/scripts/deployed-rust-build.py"),
             "--cluster", self.config.cluster],
            timeout=180,
        )
        if result.returncode != 0:
            raise SmokeConfigurationError(f"the bench runs no single Rust build: {result.detail()}")
        return result.stdout.strip()

    # --- cluster-controller leader election ------------------------------------
    #
    # The two faults behind `local.leader_election_brownout_keeps_one_leader`:
    # a refused third contender, and the apiserver's static pod taken away. The
    # node reads go through `docker exec` and the node's CRI rather than
    # kubectl, because they are the only reads that answer while the apiserver
    # is down.

    def controller_replicas(self) -> int:
        result = self.kubectl(
            "get", "deploy", self._controller_deployment(), "-o", "jsonpath={.spec.replicas}",
            timeout=15,
        )
        return int(_require_ok(result, "read cluster-controller replicas").strip() or "1")

    def controller_leader_election_enabled(self) -> bool:
        value = self._read_deployment_env(
            self._controller_deployment(), (LEADER_ELECTION_ENV,)
        )[LEADER_ELECTION_ENV]
        return value is None or value not in LEADER_ELECTION_FALSE_VALUES

    def controller_liveness_budget_env(self, *, refused_contender: bool = False) -> str | None:
        if not refused_contender:
            return self._deployment_env_literals(
                self._controller_deployment(), (LIVENESS_BUDGET_ENV,)
            )[LIVENESS_BUDGET_ENV]
        result = self.kubectl(
            "get", "pod", REFUSED_CONTENDER_NAME, "-o", "jsonpath={.spec.containers[0].env}",
            timeout=15,
        )
        _require_ok(result, "read the refused contender's env")
        return _env_literals(
            json.loads(result.stdout or "[]"), (LIVENESS_BUDGET_ENV,), owner=REFUSED_CONTENDER_NAME,
            value_from_refusal="",
        )[LIVENESS_BUDGET_ENV]

    def set_controller_liveness_budget_env(self, seconds: str) -> None:
        if _LIVENESS_BUDGET_VALUE.fullmatch(seconds) is None:
            raise ValueError(f"refusing liveness-budget value {seconds!r}")
        # Resolved here, in setup, so the restore in cleanup looks nothing up.
        deployment = self._controller_deployment()
        self._liveness_budget_target = (deployment, self._public_api_container(deployment))
        self._apply_controller_liveness_budget_env(seconds)

    def restore_controller_liveness_budget_env(
        self, standing: str | None, *, wait: bool = True
    ) -> None:
        # The captured literal goes back verbatim: the controller keeps its
        # default on an unparseable value, and restoring must not change that.
        self._apply_controller_liveness_budget_env(standing, wait=wait)

    def _apply_controller_liveness_budget_env(self, value: str | None, *, wait: bool = True) -> None:
        """Write the key under the caller's Lease; with `wait`, await the rollout.

        Every request is bounded by CLEANUP_REQUEST_SECONDS. Without `wait`,
        as cleanup calls it, the ownership read and the write are tried twice,
        so one request that stalls right after an outage does not leave the
        override in place: at most 4 x CLEANUP_REQUEST_SECONDS in all.
        """

        self._require_isolated_kind("leader-election liveness-budget override")
        ownership = self._retention_lease_ownership
        target = self._liveness_budget_target
        if ownership is None or target is None:
            raise SmokeConfigurationError(
                "refusing a liveness-budget controller deployment write without the caller's "
                "live mutation Lease"
            )
        deployment, container = target
        assignment = f"{LIVENESS_BUDGET_ENV}-" if value is None else f"{LIVENESS_BUDGET_ENV}={value}"
        attempts = 1 if wait else 2
        for attempt in range(1, attempts + 1):
            try:
                self._assert_lease_owned(
                    ownership, "a liveness-budget controller deployment write",
                    timeout=CLEANUP_REQUEST_SECONDS,
                )
                _require_ok(
                    self.kubectl(
                        "set", "env", f"deploy/{deployment}", "-c", container, assignment,
                        timeout=CLEANUP_REQUEST_SECONDS,
                    ),
                    f"set env on {deployment}",
                )
                break
            except (RuntimeError, SmokeConfigurationError):
                if attempt == attempts:
                    raise
        if wait:
            self._await_deployment_rollout(deployment, timeout=180)

    def controller_lease(self) -> ControllerLease | None:
        result = self.kubectl(
            "get", "lease", CONTROLLER_LEASE_NAME, "-o", "json", timeout=15
        )
        if result.returncode != 0 and "notfound" in result.detail().lower().replace(" ", ""):
            return None
        return _controller_lease(
            json.loads(_require_ok(result, "read the cluster-controller Lease") or "{}")
        )

    def watch_node_container_exits(self, node_name: str) -> CtrExitWatch:
        # Ending `docker exec` leaves the process it started running in the
        # node, so the watch lives only as long as its stdin: closing it, or
        # this process dying, ends `cat` and with it the watch. The node's own
        # `timeout` backs that up after the longest a scenario may run.
        return CtrExitWatch(
            [
                "docker", "exec", "-i", self._kind_node_container(node_name), "sh", "-c",
                "timeout 3600 ctr -n k8s.io events 'topic==\"/tasks/exit\"' & watch=$!; "
                "cat >/dev/null; kill \"$watch\" 2>/dev/null; wait",
            ]
        )

    def watch_controller_lease(self) -> KubectlLeaseWatch:
        return KubectlLeaseWatch(
            [
                "kubectl", "--context", self.config.context, "-n", KIND_NAMESPACE,
                "get", "lease", CONTROLLER_LEASE_NAME, "--watch", "-o", "json",
            ]
        )

    def controller_pods(self, *, refused_contender: bool = False) -> list[ControllerPod]:
        selector = REFUSED_CONTENDER_SELECTOR if refused_contender else KIND_CONTROLLER_SELECTOR
        result = self.kubectl("get", "pods", "-l", selector, "-o", "json", timeout=15)
        pods = []
        for item in json.loads(_require_ok(result, "list cluster-controller pods") or "{}").get(
            "items", []
        ):
            metadata, status = item.get("metadata") or {}, item.get("status") or {}
            container = next(
                (
                    entry
                    for entry in status.get("containerStatuses") or []
                    if entry.get("name") == CONTROLLER_CONTAINER
                ),
                {},
            )
            terminated = (container.get("lastState") or {}).get("terminated") or {}
            pods.append(
                ControllerPod(
                    name=str(metadata.get("name", "")),
                    uid=str(metadata.get("uid", "")),
                    node=str((item.get("spec") or {}).get("nodeName") or ""),
                    ready=any(
                        condition.get("type") == "Ready" and condition.get("status") == "True"
                        for condition in status.get("conditions") or []
                    ),
                    deleting=bool(metadata.get("deletionTimestamp")),
                    restart_count=int(container.get("restartCount") or 0),
                    started_at=_kube_epoch(
                        ((container.get("state") or {}).get("running") or {}).get("startedAt")
                    ),
                    last_exit_code=terminated.get("exitCode"),
                    last_finished_at=_kube_epoch(terminated.get("finishedAt")),
                )
            )
        return sorted(pods, key=lambda pod: pod.name)

    def place_refused_contender(self) -> None:
        self._require_isolated_kind("placing a refused leader-election contender")
        account = f"system:serviceaccount:{KIND_NAMESPACE}:{REFUSED_CONTENDER_SERVICE_ACCOUNT}"
        for verb in ("get", "update"):
            allowed = self.kubectl(
                "auth", "can-i", verb, "leases.coordination.k8s.io", "--as", account, timeout=15
            )
            if allowed.stdout.strip() != "no":
                raise SmokeConfigurationError(
                    f"{REFUSED_CONTENDER_SERVICE_ACCOUNT} may {verb} Leases "
                    f"({allowed.stdout.strip() or allowed.detail()}); its lease calls would not be refused"
                )
        result = self.kubectl(
            "get", "deploy", self._controller_deployment(), "-o", "json", timeout=15
        )
        deployment = json.loads(_require_ok(result, "read the cluster-controller Deployment"))
        # Any node a healthy replica was placed on satisfies the template's
        # placement rules.
        nodes = sorted({pod.node for pod in self.controller_pods() if pod.node})
        if not nodes:
            raise RuntimeError("no cluster-controller pod is bound to a node to place the contender on")
        # A contender a killed run left behind would start the budget window
        # from its own, older container.
        self.remove_refused_contender()
        _require_ok(
            self.kubectl(
                "create", "-f", "-",
                input_text=json.dumps(refused_contender_pod(deployment, node=nodes[0])),
                timeout=30,
            ),
            "create the refused leader-election contender",
        )
        deadline = time.monotonic() + 180
        while not self._refused_contender_gate(nodes[0]):
            if time.monotonic() > deadline:
                raise RuntimeError("the refused contender's gate container never started")
            time.sleep(2)

    def release_refused_contender(self) -> None:
        # Read right after the apiserver answers again, while kubectl may still
        # be reconnecting.
        deadline = time.monotonic() + 60
        while True:
            result = self.kubectl(
                "get", "pod", REFUSED_CONTENDER_NAME, "-o", "jsonpath={.spec.nodeName}", timeout=15
            )
            if result.returncode == 0 or time.monotonic() > deadline:
                break
            time.sleep(2)
        node = _require_ok(result, "find the refused contender's node").strip()
        gate = self._refused_contender_gate(node)
        if gate is None:
            raise RuntimeError("the refused contender has no running gate container to open")
        _require_ok(
            self._docker_exec(node, "crictl", "exec", gate, "touch", f"{REFUSED_CONTENDER_GATE_DIR}/open"),
            "open the refused contender's gate",
        )

    def _refused_contender_gate(self, node_name: str) -> str | None:
        """The contender's running gate container on its node, read through the CRI."""

        listing = self._docker_exec(
            node_name, "crictl", "ps", "-q", "--state", "running",
            "--label", f"io.kubernetes.pod.namespace={KIND_NAMESPACE}",
            "--label", f"io.kubernetes.pod.name={REFUSED_CONTENDER_NAME}",
            "--label", f"io.kubernetes.container.name={REFUSED_CONTENDER_GATE_CONTAINER}",
        )
        ids = _require_ok(listing, f"list the refused contender's gate on {node_name}").split()
        return ids[0] if ids else None

    def remove_refused_contender(self, *, wait: bool = True) -> None:
        # The Deployment is the shape earlier versions of the scenario ran the
        # contender as; one a killed run left behind would keep a pod under the
        # contender's selector. Not `--wait`: right after an apiserver outage
        # the node's kubelet can take a minute or more to see the deletion,
        # which the poll below outlasts.
        _require_ok(
            self.kubectl(
                "delete", f"pod/{REFUSED_CONTENDER_NAME}", f"deployment/{REFUSED_CONTENDER_NAME}",
                "--ignore-not-found", "--wait=false",
                timeout=60 if wait else CLEANUP_REQUEST_SECONDS,
            ),
            "delete the refused leader-election contender",
        )
        if not wait:
            return
        deadline = time.monotonic() + 120
        while self.controller_pods(refused_contender=True):
            if time.monotonic() > deadline:
                raise RuntimeError("the refused contender's pod outlived its deletion")
            time.sleep(2)

    def control_plane_node(self) -> str:
        result = self.kubectl(
            "get", "nodes", "-l", "node-role.kubernetes.io/control-plane", "-o",
            "jsonpath={.items[*].metadata.name}", timeout=15,
        )
        names = _require_ok(result, "find the Kind control-plane node").split()
        if len(names) != 1:
            raise SmokeConfigurationError(
                f"expected one Kind control-plane node, found {names or 'none'}"
            )
        return self._kind_node_container(names[0])

    def apiserver_answers(self) -> bool:
        result = self.executor.run(
            ["kubectl", "--context", self.config.context, "get", "--raw", "/readyz",
             "--request-timeout=3s"],
            timeout=10,
        )
        return result.returncode == 0 and result.stdout.strip() == "ok"

    def park_apiserver(self, node_name: str) -> None:
        self._require_isolated_kind("taking the Kind apiserver down")
        _require_ok(
            self._docker_exec(
                node_name, "sh", "-c",
                f"test ! -e {APISERVER_PARKED_MANIFEST} && mv {APISERVER_MANIFEST} {APISERVER_PARKED_MANIFEST}",
            ),
            "park the apiserver manifest",
        )

    def restore_apiserver(self, node_name: str, *, timeout: float = 30) -> None:
        _require_ok(
            self._docker_exec(
                node_name, "sh", "-c",
                f"if test -e {APISERVER_PARKED_MANIFEST}; then "
                f"mv {APISERVER_PARKED_MANIFEST} {APISERVER_MANIFEST}; fi",
                timeout=timeout,
            ),
            "restore the apiserver manifest",
        )

    def node_controller_containers(self, node_name: str, pod_name: str) -> list[NodeContainer]:
        listing = self._docker_exec(
            node_name, "crictl", "ps", "-a", "-q",
            "--label", f"io.kubernetes.pod.namespace={KIND_NAMESPACE}",
            "--label", f"io.kubernetes.pod.name={_kube_pod_name(pod_name)}",
            "--label", f"io.kubernetes.container.name={CONTROLLER_CONTAINER}",
        )
        containers = []
        for container_id in _require_ok(listing, f"list controller containers on {node_name}").split():
            inspected = self._docker_exec(node_name, "crictl", "inspect", "-o", "json", container_id)
            if inspected.returncode != 0:
                # Removed by kubelet's container GC between the two reads.
                continue
            status = json.loads(inspected.stdout).get("status") or {}
            running = status.get("state") == "CONTAINER_RUNNING"
            containers.append(
                NodeContainer(
                    container_id=container_id,
                    attempt=int((status.get("metadata") or {}).get("attempt") or 0),
                    running=running,
                    started_at=_cri_epoch(status.get("startedAt")),
                    finished_at=None if running else _cri_epoch(status.get("finishedAt")),
                    exit_code=None if running else status.get("exitCode"),
                )
            )
        return sorted(containers, key=lambda container: container.attempt)

    def readiness_marker_present(self, node_name: str, container_id: str) -> bool | None:
        # One exec that names its answer. An exec that fails, including one
        # into a container that has exited meanwhile, says nothing about the
        # marker; one that answers ran inside the running container.
        result = self._docker_exec(
            node_name, "crictl", "exec", container_id, "sh", "-c",
            f"if test -f {CONTROLLER_READINESS_MARKER}; then echo present; else echo absent; fi",
        )
        if result.returncode != 0:
            return None
        return {"present": True, "absent": False}.get(result.stdout.strip())

    def node_container_log_tail(self, node_name: str, container_id: str, *, lines: int) -> str:
        result = self._docker_exec(node_name, "crictl", "logs", "--tail", str(lines), container_id)
        return (result.stdout + result.stderr)[-20_000:]

    def _kind_node_container(self, node_name: str) -> str:
        if not re.fullmatch(
            rf"{re.escape(self.config.cluster)}-(control-plane|worker[0-9]*)", node_name
        ):
            raise ValueError(f"refusing node {node_name!r} outside Kind cluster {self.config.cluster}")
        return node_name

    def _docker_exec(self, node_name: str, *command: str, timeout: float = 30) -> CommandResult:
        return self.executor.run(
            ["docker", "exec", self._kind_node_container(node_name), *command], timeout=timeout
        )

    # --- storage exchange ----------------------------------------------------
    #
    # credential-manager's storage exchange, asked on a sandbox worker's agent
    # volume socket as the volume-manager account (credmgr_socket.py), and S3
    # requests against the store with the session it returns or with the
    # store's root keys. Reads only, except one refused small write.

    def storage_credentials_mode(self) -> str:
        result = self.kubectl(
            "get", "deployments", "-l", "app.kubernetes.io/name=space-credmgr-authority",
            "-o", "jsonpath={.items[*].metadata.labels.space\\.pplx\\.net/kind-storage-credentials}",
            timeout=15,
        )
        modes = set(_require_ok(result, "read the storage credentials mode").split())
        # An authority rendered before the modes existed ran static keys.
        mode = modes.pop() if len(modes) == 1 else "static" if not modes else ""
        if mode not in ("exchange", "static"):
            raise SmokeConfigurationError(f"the authority runs no known storage credentials mode: {sorted(modes)}")
        # The label records how the authority's chart was rendered. Every node
        # service that transfers objects must agree, in its DaemonSet's pod
        # template and in every pod it runs: a failed apply, a stale Tilt
        # resource or a rollout still under way can leave either on the other
        # side of it.
        pods = json.loads(_require_ok(self.kubectl("get", "pods", "-o", "json", timeout=30), "list pods") or "{}")
        for daemonset, container, service in STORAGE_CREDENTIAL_DAEMONSETS:
            result = self.kubectl("get", "daemonset", daemonset, "-o", "json", timeout=15)
            template = json.loads(_require_ok(result, f"read the {service} DaemonSet") or "{}")
            specs = [("its pod template", template.get("spec", {}).get("template", {}).get("spec", {}))] + [
                (f"pod {item['metadata']['name']}", item.get("spec", {}))
                for item in pods.get("items", [])
                if any(
                    owner.get("kind") == "DaemonSet" and owner.get("name") == daemonset
                    for owner in item.get("metadata", {}).get("ownerReferences", [])
                )
            ]
            for where, spec in specs:
                problem = self._storage_keys_disagreement(mode, spec, container)
                if problem:
                    raise SmokeConfigurationError(
                        f"the authority runs storage credentials mode {mode}, but the {service} "
                        f"({daemonset}, {where}) {problem}"
                    )
        return mode

    def _storage_keys_disagreement(self, mode: str, spec: dict[str, Any], container: str) -> str:
        """How one pod spec contradicts `mode`, or "" when it agrees.

        In `exchange`, no container, init and ephemeral ones included, may
        hold a static key name, by `env` or through an `envFrom` Secret or
        ConfigMap, which is resolved to its keys; one that cannot be read is
        refused. In `static`, the consuming container must hold both keys.
        """

        sources: dict[str, set[str] | None] = {}

        def static_keys(item: dict[str, Any]) -> tuple[set[str], list[str]]:
            """The static key names a container gets, and its envFrom sources that cannot be read."""
            found = {str(entry.get("name", "")) for entry in item.get("env") or []}
            unreadable: list[str] = []
            for source in item.get("envFrom") or []:
                if source.get("secretRef"):
                    ref = f"secret/{source['secretRef'].get('name', '')}"
                elif source.get("configMapRef"):
                    ref = f"configmap/{source['configMapRef'].get('name', '')}"
                else:
                    continue
                if ref not in sources:
                    result = self.kubectl("get", ref, "--ignore-not-found", "-o", "json", timeout=15)
                    document = json.loads(_require_ok(result, f"read {ref}") or "null")
                    sources[ref] = (
                        None if document is None
                        else set(document.get("data") or {}) | set(document.get("binaryData") or {})
                    )
                keys = sources[ref]
                if keys is None:
                    unreadable.append(ref)
                else:
                    found |= {str(source.get("prefix") or "") + key for key in keys}
            return {name for name in found if name.startswith("SPACE_OBJECT_STORE_STATIC_")}, unreadable

        containers = [
            item
            for field in ("initContainers", "containers", "ephemeralContainers")
            for item in spec.get(field) or []
        ]
        if mode == "exchange":
            for item in containers:
                keys, unreadable = static_keys(item)
                if keys:
                    return f"has container {item.get('name')} holding static keys ({', '.join(sorted(keys))})"
                if unreadable:
                    return f"has container {item.get('name')} taking env from {unreadable[0]}, which cannot be read"
            return ""
        consumer = next((item for item in containers if item.get("name") == container), None)
        if consumer is None:
            return f"has no {container} container"
        missing = sorted(set(OBJECT_STORE_STATIC_KEYS) - static_keys(consumer)[0])
        return f"has container {container} lacking static keys ({', '.join(missing)})" if missing else ""

    def sandbox_worker_nodes(self) -> list[str]:
        result = self.kubectl(
            "get", "pods", "-l", "app.kubernetes.io/name=space-worker,app.kubernetes.io/component=volume-manager",
            "-o", "jsonpath={.items[*].spec.nodeName}", timeout=15,
        )
        return sorted(set(_require_ok(result, "list worker volume-manager pods").split()))

    def _storage_environment(
        self, access_key_id: str, secret_access_key: str, session_token: str | None
    ) -> dict[str, str | None]:
        return {
            "AWS_SESSION_TOKEN": session_token,
            "AWS_SECURITY_TOKEN": None,
            "AWS_PROFILE": None,
            "AWS_DEFAULT_PROFILE": None,
            "AWS_WEB_IDENTITY_TOKEN_FILE": None,
            "AWS_ROLE_ARN": None,
            "AWS_ROLE_SESSION_NAME": None,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_ACCESS_KEY_ID": access_key_id,
            "AWS_SECRET_ACCESS_KEY": secret_access_key,
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_EC2_METADATA_DISABLED": "true",
            # A refusal is the answer under test, never a transient to retry.
            "AWS_MAX_ATTEMPTS": "1",
        }

    def _root_storage_environment(self) -> dict[str, str | None]:
        return self._storage_environment(
            self.environment.get("SPACE_SMOKE_S3_ACCESS_KEY_ID", "localadmin"),
            self.environment.get("SPACE_SMOKE_S3_SECRET_ACCESS_KEY", "localadmin-dev-secret"),
            None,
        )

    def _s3api(self, environment: Mapping[str, str | None], *args: str, timeout: float = 30) -> CommandResult:
        return self.executor.run(
            [
                "aws", "--no-cli-pager", "--endpoint-url",
                self.environment.get("S3_ENDPOINT", "http://127.0.0.1:8333"), "s3api", *args,
            ],
            env=environment,
            timeout=timeout,
        )

    def bucket_objects(self, bucket: str, *, limit: int, prefix: str = "") -> list[StorageObject]:
        args = ["list-objects-v2", "--bucket", bucket, "--max-items", str(limit), "--output", "json"]
        if prefix:
            args += ["--prefix", prefix]
        output = _require_ok(self._s3api(self._root_storage_environment(), *args), f"list {bucket}")
        contents = (json.loads(output) if output.strip() else {}).get("Contents", [])
        return [
            StorageObject(bucket=bucket, key=item["Key"], etag=item.get("ETag", ""), size=int(item.get("Size", 0)))
            for item in contents
        ]

    def snapshot_objects(self, record: SnapshotRecord) -> list[StorageObject]:
        bucket, prefix, _, _ = self._snapshot_storage_target(record)
        return self.bucket_objects(bucket, limit=100, prefix=prefix)

    def head_object(self, bucket: str, key: str) -> StorageObject | None:
        result = self._s3api(self._root_storage_environment(), "head-object", "--bucket", bucket, "--key", key)
        if result.returncode != 0 and ("404" in result.stderr or "Not Found" in result.stderr):
            return None
        head = json.loads(_require_ok(result, f"head {bucket}/{key}"))
        return StorageObject(bucket=bucket, key=key, etag=head.get("ETag", ""), size=int(head.get("ContentLength", 0)))

    def exchange_storage_credentials(
        self, node_name: str, *, subject: str, resource: str, scope: str
    ) -> StorageExchangeOutcome:
        self._require_isolated_kind("on-node operations")
        argv = relay_argv(
            ["kubectl", "--context", self.config.context, "-n", KIND_NAMESPACE],
            self._node_shell_pod(node_name),
            KIND_NODE_SHELL_CONTAINER,
            "/space/run/volume-credentials/credmgr.sock",
            # volume-manager's account; the socket's group admits it
            # (crates/libs/credential/mgr/server/src/run/volume_socket.rs).
            user="space-volmgr",
            groups=("space-volcredclients",),
        )
        credentials, status, detail = exchange_token(argv, resource=resource, scope=scope, subject=subject)
        session = (
            StorageSession(
                access_key_id=credentials.access_key_id,
                secret_access_key=credentials.secret_access_key,
                session_token=credentials.session_token,
                expires_at_unix=credentials.expires_at_unix,
            )
            if credentials is not None
            else None
        )
        return StorageExchangeOutcome(session=session, status=status, detail=detail[:500])

    def storage_request(
        self, session: StorageSession, operation: str, bucket: str, key: str, *, body: bytes | None = None
    ) -> StorageRequestResult:
        environment = self._storage_environment(
            session.access_key_id, session.secret_access_key, session.session_token
        )
        with tempfile.TemporaryDirectory(prefix="space-smoke-s3-") as directory:
            if operation == "head":
                args = ["head-object", "--bucket", bucket, "--key", key]
            elif operation == "get":
                args = ["get-object", "--bucket", bucket, "--key", key, os.path.join(directory, "object")]
            elif operation == "list":
                args = ["list-objects-v2", "--bucket", bucket, "--prefix", key, "--max-items", "1"]
            elif operation == "put":
                if body is None:
                    raise ValueError("a put needs a body")
                path = os.path.join(directory, "body")
                Path(path).write_bytes(body)
                args = ["put-object", "--bucket", bucket, "--key", key, "--body", path]
            else:
                raise ValueError(f"unknown storage operation {operation!r}")
            result = self._s3api(environment, *args, timeout=60)
        detail = result.detail()[-500:]
        # The aws CLI names the S3 status in its error: 403 Forbidden for a
        # body-less refusal, AccessDenied when the store sent an error body.
        refused = result.returncode != 0 and any(
            marker in detail for marker in ("(403)", "AccessDenied", "Forbidden")
        )
        return StorageRequestResult(
            operation=operation,
            target=f"{bucket}/{key}",
            succeeded=result.returncode == 0,
            refused=refused,
            detail=detail,
        )

    # --- sandbox network isolation -------------------------------------------
    #
    # Probes of the sandbox namespace's NetworkPolicies, run in the node's
    # operator shell: in its own network namespace (the control-plane
    # namespace, under no policy), or in a pod's, entered by the netns handle
    # whose eth0 carries the pod IP. A sandbox pod has no process of its own to
    # name with a pid; the handle is what the node's runtime created for it.

    def sandbox_pod_network(self, sandbox_id: str) -> PodNetwork | None:
        result = self.kubectl(
            "get", "pods", "-l", f"{SANDBOX_ID_LABEL}={_sql_uuid(sandbox_id)}", "-o", "json",
            timeout=15, namespace=KIND_SANDBOX_NAMESPACE,
        )
        running = [
            pod
            for pod in json.loads(_require_ok(result, "read the sandbox pod") or "{}").get("items", [])
            if not (pod.get("metadata") or {}).get("deletionTimestamp")
            and (pod.get("status") or {}).get("phase") == "Running"
            and (pod.get("spec") or {}).get("nodeName")
            and (pod.get("status") or {}).get("podIP")
        ]
        if len(running) != 1:
            return None
        pod = running[0]
        return PodNetwork(
            pod_name=str(pod["metadata"]["name"]),
            node_name=str(pod["spec"]["nodeName"]),
            pod_ip=str(pod["status"]["podIP"]),
        )

    def control_plane_targets(self) -> list[TcpTarget]:
        return [
            self._service_target("postgres", "postgres", 5432),
            self._raw_service_target("kubernetes-api", "default", "kubernetes", 443),
            self._service_target("internal-api", "space-internal-api", 9800),
            self._service_target("object-store", "object-store", 8333),
        ]

    def cluster_dns_target(self) -> TcpTarget:
        return self._raw_service_target("cluster-dns", "kube-system", "kube-dns", 53)

    def _service_target(self, name: str, service: str, port: int) -> TcpTarget:
        result = self.kubectl("get", "service", service, "-o", "json", timeout=15)
        return _service_tcp_target(
            name, json.loads(_require_ok(result, f"read Service {service}") or "{}"), port
        )

    def _raw_service_target(self, name: str, namespace: str, service: str, port: int) -> TcpTarget:
        # Outside the two namespaces the harness addresses; `--raw` takes the
        # path verbatim and ignores `-n`.
        path = f"/api/v1/namespaces/{namespace}/services/{service}"
        result = self.kubectl("get", "--raw", path, timeout=15)
        return _service_tcp_target(
            name, json.loads(_require_ok(result, f"read Service {namespace}/{service}") or "{}"), port
        )

    def probe_from_node_shell(
        self, node_name: str, targets: Sequence[TcpTarget], *, timeout: float
    ) -> list[TcpProbe]:
        script = _tcp_probe_script(targets, timeout=timeout, pod_ip=None)
        result = self._node_shell_exec(node_name, script, timeout=60 + timeout * len(targets))
        return _parse_tcp_probes(
            _require_ok(result, f"probe from the operator shell on {node_name}"), targets
        )

    def probe_from_pod_netns(
        self, pod: PodNetwork, targets: Sequence[TcpTarget], *, timeout: float
    ) -> list[TcpProbe]:
        script = _tcp_probe_script(targets, timeout=timeout, pod_ip=pod.pod_ip)
        result = self._node_shell_exec(pod.node_name, script, timeout=60 + timeout * len(targets))
        if result.returncode == 3 and "NETNS none" in result.stdout:
            raise SmokeConfigurationError(
                f"no network namespace on {pod.node_name} carries {pod.pod_name}'s IP {pod.pod_ip}"
            )
        output = _require_ok(result, f"probe from the network namespace of {pod.pod_name}")
        return _parse_tcp_probes(output, targets)

    def start_unselected_pod(self, node_name: str, token: str) -> PodNetwork:
        self._require_isolated_kind("a pod in the sandbox namespace")
        if not re.fullmatch(r"[a-f0-9]{8}", token):
            raise ValueError(f"refusing unselected-pod token {token!r}")
        name = f"space-smoke-unselected-{token}"
        manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "labels": {UNSELECTED_POD_LABEL: token}},
            "spec": {
                # Bound directly: the probe needs it on the sandbox's node,
                # and the scheduler has no say in what this proves.
                "nodeName": _kube_node_name(node_name),
                "automountServiceAccountToken": False,
                "enableServiceLinks": False,
                "terminationGracePeriodSeconds": 0,
                "tolerations": [{"operator": "Exists"}],
                "containers": [
                    {"name": "pause", "image": UNSELECTED_POD_IMAGE, "imagePullPolicy": "IfNotPresent"}
                ],
            },
        }
        _require_ok(
            self.kubectl(
                "create", "-f", "-", input_text=json.dumps(manifest), timeout=30,
                namespace=KIND_SANDBOX_NAMESPACE,
            ),
            f"create {name}",
        )
        deadline = time.monotonic() + UNSELECTED_POD_READY_SECONDS
        while True:
            result = self.kubectl(
                "get", "pod", name, "-o", "json", timeout=15, namespace=KIND_SANDBOX_NAMESPACE
            )
            pod = json.loads(_require_ok(result, f"read {name}") or "{}")
            status = pod.get("status") or {}
            if status.get("phase") == "Running" and status.get("podIP"):
                return PodNetwork(pod_name=name, node_name=node_name, pod_ip=str(status["podIP"]))
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"{name} was not Running with a pod IP within {UNSELECTED_POD_READY_SECONDS}s: "
                    f"phase {status.get('phase')!r}"
                )
            time.sleep(2)

    def remove_unselected_pods(self, token: str) -> None:
        if not re.fullmatch(r"[a-f0-9]{8}", token):
            raise ValueError(f"refusing unselected-pod token {token!r}")
        _require_ok(
            self.kubectl(
                "delete", "pods", "-l", f"{UNSELECTED_POD_LABEL}={token}",
                "--ignore-not-found", "--wait=false", timeout=30,
                namespace=KIND_SANDBOX_NAMESPACE,
            ),
            f"remove the unselected pod {token}",
        )

    def _read_deployment_env(self, name: str, keys: Sequence[str]) -> dict[str, str | None]:
        """The literal each key holds on a Deployment's first container, None if unset.

        Read-only. A value Kubernetes resolves at runtime, from a `valueFrom`
        reference or an `envFrom` source, is refused rather than read as unset.
        """

        result = self.kubectl(
            "get", "deploy", name, "-o", "jsonpath={.spec.template.spec.containers[0]}", timeout=15,
        )
        container = json.loads(_require_ok(result, f"read the container of {name}") or "{}")
        values = _env_literals(
            container.get("env") or [], keys, owner=name,
            value_from_refusal=", not a literal to read",
        )
        unset = [key for key, value in values.items() if value is None]
        if unset and container.get("envFrom"):
            raise SmokeConfigurationError(
                f"{', '.join(unset)} on {name} may come from its envFrom sources, which are not read"
            )
        return values


def _node_service_selector(component: str, *, role: str = KIND_WORKER_ROLE) -> str:
    """Label selector for one node service's pods of one role."""

    return f"app.kubernetes.io/name={role},app.kubernetes.io/component={component}"


def _ready_pod_names(pod_list_json: str, *, excluding: str) -> list[str]:
    """Names of the Ready pods in a `kubectl get pods -o json` document."""

    try:
        document = json.loads(pod_list_json)
    except ValueError:
        return []
    names: list[str] = []
    for item in document.get("items", []) if isinstance(document, dict) else []:
        name = str(item.get("metadata", {}).get("name", ""))
        if not name or name == excluding:
            continue
        conditions = item.get("status", {}).get("conditions", []) or []
        if any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions):
            names.append(name)
    return names


def _storage_uri(value: str) -> str:
    """A storage connection string as it may be spliced into a SQL literal."""

    if not value or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:/?=&._+%~-]{0,1023}", value):
        raise ValueError(f"refusing storage uri {value!r}")
    return value


def _sql_timestamp(value: str) -> str:
    """An ISO-8601 UTC timestamp as it may be spliced into a SQL literal."""

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z", value):
        raise ValueError(f"refusing timestamp {value!r}")
    return value


def _object_name(value: str) -> str:
    """One object name under a snapshot prefix, never a path."""

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
        raise ValueError(f"refusing object name {value!r}")
    return value


def template_received_uuid(template: str, content_hash: str) -> str:
    """Mirror `deterministic_template_received_uuid`: UUIDv5(NS, "name@hash")."""

    return str(uuid.uuid5(TEMPLATE_RECEIVED_UUID_NAMESPACE, f"{template}@{content_hash}"))


def _race_request(
    request: Request, *, timeout: float, gate: Callable[[], None] | None
) -> RestoreCallerOutcome:
    """One raced HTTP request classified as (status, elapsed, finished_at, detail).

    HTTP errors keep their status and body; a transport failure is status 0.
    The gate, when given, is waited on right before the request leaves so
    every caller in a race departs together.
    """

    if gate is not None:
        gate()
    started = time.monotonic()
    try:
        with urlopen(request, timeout=timeout) as response:
            status = int(response.status)
            response.read()
    except HTTPError as error:
        detail = error.read(2_000).decode(errors="replace").strip()
        finished = time.monotonic()
        return RestoreCallerOutcome(error.code, finished - started, finished, detail[:300])
    except (URLError, OSError) as error:
        reason = getattr(error, "reason", error)
        finished = time.monotonic()
        return RestoreCallerOutcome(0, finished - started, finished, str(reason)[:300])
    finished = time.monotonic()
    return RestoreCallerOutcome(status, finished - started, finished)


def _template_stash_dir(name: str) -> str:
    return f"{TEMPLATE_CACHE_ROOT}/.smoke-stash-{name}"


def _template_cache_gc_config_from_env(
    env: Mapping[str, str | None],
) -> TemplateCacheGcConfig:
    """Mirror `GcConfig::from_env`: unset uses the default budget; 0 or a non-byte-count disables."""

    def positive(key: str, default: int) -> int:
        raw = env.get(key)
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError:
            return default
        return value if value > 0 else default

    raw_grace = str(env.get("SPACE_TEMPLATE_GC_GRACE_SECS") or "")
    raw_budget = env.get("SPACE_TEMPLATE_CACHE_BUDGET_BYTES")
    if raw_budget is None:
        budget = _TEMPLATE_GC_DEFAULT_BUDGET_BYTES
    else:
        try:
            parsed = int(str(raw_budget).strip())
        except ValueError:
            parsed = -1
        # The binary parses a u64: a negative or non-numeric value is invalid and
        # disables the loop, like an explicit zero.
        budget = 0 if parsed < 0 else parsed
    # The headroom fraction is a fixed constant in the binary.
    headroom = _TEMPLATE_GC_DEFAULT_HEADROOM
    # Prefetch latest-N: unset → default 5; the binary treats a non-numeric
    # value as the default too, and 0 disables the pass.
    raw_latest = str(env.get("SPACE_TEMPLATE_PREFETCH_LATEST_N") or "")
    latest_n = int(raw_latest) if raw_latest.isdigit() else _TEMPLATE_PREFETCH_DEFAULT_LATEST_N
    return TemplateCacheGcConfig(
        enabled=budget > 0,
        budget_bytes=budget,
        interval_seconds=positive(
            "SPACE_TEMPLATE_GC_INTERVAL_SECS", _TEMPLATE_GC_DEFAULT_INTERVAL_SECONDS
        ),
        # Grace accepts zero (the binary parses it without a positivity
        # filter); only an unset or unparsable value falls back.
        grace_seconds=(
            int(raw_grace) if raw_grace.isdigit() else _TEMPLATE_GC_DEFAULT_GRACE_SECONDS
        ),
        headroom_fraction=headroom,
        prefetch_latest_n=latest_n,
    )


def _cold_eviction_config_from_env(env: Mapping[str, str | None]) -> ColdEvictionConfig:
    """Mirror `EvictionConfig::from_env` for the knobs the scenario gates on."""

    def fraction(key: str, default: float) -> float:
        try:
            value = float(str(env.get(key)))
        except ValueError:
            return default
        if value != value or value in (float("inf"), float("-inf")):
            return default
        return min(1.0, max(0.0, value))

    def positive(key: str, default: int) -> int:
        raw = str(env.get(key) or "")
        return int(raw) if raw.isdigit() and int(raw) > 0 else default

    raw_idle = str(env.get("SPACE_SBX_EVICT_MIN_IDLE_SECS") or "")
    return ColdEvictionConfig(
        enabled=str(env.get("SPACE_SBX_EVICT_ENABLED") or "").strip().lower() in ("1", "true"),
        engage_fraction=fraction("SPACE_SBX_EVICT_ENGAGE_FRACTION", _COLD_EVICTION_DEFAULT_ENGAGE),
        target_fraction=fraction("SPACE_SBX_EVICT_TARGET_FRACTION", _COLD_EVICTION_DEFAULT_TARGET),
        # The daemon honors any parseable idle, zero included; only an unset or
        # unparsable value falls back to the 25 h default.
        min_idle_seconds=(
            int(raw_idle) if raw_idle.isdigit() else _COLD_EVICTION_DEFAULT_MIN_IDLE_SECONDS
        ),
        interval_seconds=positive(
            "SPACE_SBX_EVICT_INTERVAL_SECS", _COLD_EVICTION_DEFAULT_INTERVAL_SECONDS
        ),
    )


def _snapshot_reaper_config_from_env(env: Mapping[str, str | None]) -> SnapshotReaperConfig:
    """Mirror `snapshot_gc::GcConfig::from_env` for the knobs the scenario gates on."""

    def fraction(key: str, default: float) -> float:
        try:
            value = float(str(env.get(key)))
        except ValueError:
            return default
        if value != value or value in (float("inf"), float("-inf")):
            return default
        return min(1.0, max(0.0, value))

    def seconds(key: str, default: int) -> int:
        raw = str(env.get(key) or "")
        return int(raw) if raw.isdigit() else default

    return SnapshotReaperConfig(
        enabled=str(env.get("SPACE_SBX_GC_ENABLED") or "").strip().lower() in ("1", "true"),
        engage_fraction=fraction("SPACE_SBX_GC_ENGAGE_FRACTION", _REAPER_DEFAULT_ENGAGE),
        interval_seconds=seconds("SPACE_SBX_GC_INTERVAL_SECS", _REAPER_DEFAULT_INTERVAL_SECONDS),
        grace_seconds=seconds("SPACE_SBX_GC_GRACE_SECS", _REAPER_DEFAULT_GRACE_SECONDS),
        idle_seconds=seconds("SPACE_SBX_GC_IDLE_SECS", _REAPER_DEFAULT_IDLE_SECONDS),
    )




def _redact_storage_uris(text: str) -> str:
    """`text` with every storage URI replaced by its scheme, for attached evidence."""

    return _STORAGE_URI.sub("s3://<redacted>", text)


def _btrfs_send_interrupt_stop_file(token: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{8,64}", token):
        raise ValueError(f"refusing interrupter token {token!r}")
    return f"/tmp/space-smoke-send-interrupt-{token}"


def _tracing_fields(line: str) -> dict[str, Any] | None:
    """The event fields of one JSON tracing line, whichever layout it uses."""

    try:
        event = json.loads(line)
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    merged: dict[str, Any] = {}
    # Span fields first (outermost to innermost), then the event's own; a
    # `trigger` recorded on the enclosing `pull` span is what tags every
    # line template-manager writes inside it.
    for span in event.get("spans") or []:
        if isinstance(span, dict):
            merged.update({k: v for k, v in span.items() if k != "name"})
    span = event.get("span")
    if isinstance(span, dict):
        merged.update({k: v for k, v in span.items() if k != "name"})
    merged.update(event)
    fields = event.get("fields")
    if isinstance(fields, dict):
        merged.update(fields)
    return merged


def _routing_value(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,30}", value):
        raise ValueError(f"refusing routing label value {value!r}")
    return value


def _template_alias(value: str) -> str:
    """Only the scenario's own throwaway aliases reach a shell or a URL."""

    if not re.fullmatch(r"space-smoke-(oci|parent)-[0-9a-f]{8}", value):
        raise ValueError(f"refusing template alias {value!r}")
    return value


def _endpoint_target_pods(endpoints_json: str) -> list[str]:
    """Pod names behind a Service from a `kubectl get endpoints -o json` document."""

    try:
        document = json.loads(endpoints_json)
    except ValueError:
        return []
    names: list[str] = []
    for subset in (document.get("subsets") or []) if isinstance(document, dict) else []:
        for address in subset.get("addresses") or []:
            target = address.get("targetRef") or {}
            name = str(target.get("name", ""))
            if name:
                names.append(name)
    return sorted(names)


# The shell suite's handoff check: the accepting builder committed a prepared
# rootfs (marker present, in-progress marker gone, a btrfs subvolume, the
# rootfs sized to the marker's MiB) for a sandbox row owned by builder@local,
# within __WINDOW_SECONDS__.
_PREPARED_ROOTFS_HANDOFF_SCRIPT = r"""set -eu
deadline=$(( $(date +%s) + __WINDOW_SECONDS__ ))
while [ "$(date +%s)" -le "$deadline" ]; do
  sandboxes="$(/space/bin/space-sbxmgr list)"
  for marker in /space/cache/firecracker/sandboxes/*/instance/.space-prepared-rootfs-ready; do
    [ -f "$marker" ] || continue
    instance_dir="${marker%/.space-prepared-rootfs-ready}"
    sandbox_dir="${instance_dir%/instance}"
    sandbox_id="${sandbox_dir##*/}"
    short_id="$(printf '%s' "$sandbox_id" | cut -c 29-36)"
    if printf '%s\n' "$sandboxes" | grep -F "$short_id" | grep -Fq 'builder@local'; then
      test ! -e "$instance_dir/.space-prepared-rootfs-in-progress"
      btrfs subvolume show "$instance_dir" >/dev/null
      expected_bytes=$(( $(cat "$marker") * 1024 * 1024 ))
      actual_bytes="$(stat -c %s "$instance_dir/rootfs.ext4")"
      test "$actual_bytes" -eq "$expected_bytes"
      exit 0
    fi
  done
  sleep 0.2
done
echo "committed prepared rootfs was not observed on the accepting builder" >&2
exit 1
"""

# Exits 0 while the builder@local sandbox holding a committed prepared rootfs is
# listed before or in its run (the build's RUN steps run in it), and 3 when
# there is none, so a failed listing (any other exit, under `set -e`) is never
# read as "the build finished". The list's cells are separated by `┆`.
_BUILD_SANDBOX_STARTED_SCRIPT = r"""set -eu
sandboxes="$(/space/bin/space-sbxmgr list)"
for marker in /space/cache/firecracker/sandboxes/*/instance/.space-prepared-rootfs-ready; do
  [ -f "$marker" ] || continue
  sandbox_dir="${marker%/instance/.space-prepared-rootfs-ready}"
  short_id="$(printf '%s' "${sandbox_dir##*/}" | cut -c 29-36)"
  if printf '%s\n' "$sandboxes" | grep -F "$short_id" | grep -F 'builder@local' \
      | grep -Eq '┆ *(creating|created|starting|started) *┆'; then
    exit 0
  fi
done
exit 3
"""

# Prints three lines: the bundle version inside the rootfs, then metadata.json's
# spaced_version and spaced_compatibility_family. The family oracle is Python's.
_GUEST_METADATA_SCRIPT = r"""set -eu
template_dir=__TEMPLATE_DIR__
metadata_path="$(find "$template_dir" -mindepth 2 -maxdepth 2 -type f -name metadata.json -print -quit)"
test -n "$metadata_path"
rootfs_path="${metadata_path%/metadata.json}/rootfs.ext4"
# Some debugfs versions return inline storage capacity rather than inode size.
# Accept only zero padding beyond the logical file, never strip payload bytes.
export LC_ALL=C
inode="$(debugfs -R 'stat /etc/spaced/bundle-version' "$rootfs_path" 2>/dev/null)"
size="$(printf '%s\n' "$inode" | awk '/^User:/ {for (i=1; i<NF; i++) if ($i == "Size:") print $(i+1)}')"
case "$size" in ''|*[!0-9]*) echo 'missing bundle-version inode size' >&2; exit 1 ;; esac
test "$size" -gt 0 && test "$size" -le 4096
scratch="$(mktemp)"
trap 'rm -f "$scratch"' EXIT HUP INT TERM
debugfs -R 'cat /etc/spaced/bundle-version' "$rootfs_path" > "$scratch" 2>/dev/null
test "$(wc -c < "$scratch")" -ge "$size"
test "$(tail -c +$((size + 1)) "$scratch" | tr -d '\000' | wc -c)" -eq 0
head -c "$size" "$scratch" | tr -d '\r\n'; echo
sed -n 's/.*"spaced_version": "\([^"]*\)".*/\1/p' "$metadata_path"
sed -n 's/.*"spaced_compatibility_family": "\([^"]*\)".*/\1/p' "$metadata_path"
"""

# Prints one line per leftover; nothing when clean. The `ctr` listing itself
# must succeed: an unavailable or failing containerd would otherwise read as an
# empty list and let the residue check pass without inspecting anything, so its
# failure exits non-zero with ctr's stderr and the caller raises. Only the greps
# (which exit 1 on no match) are tolerated.
_CONTAINERD_RESIDUE_SCRIPT = r"""set -u
image=__IMAGE__
# containerd records the pull by repository and digest, without the tag: drop
# the reference's digest, then any tag on its last path component.
repo="${image%@*}"
case "${repo##*/}" in *:*) repo="${repo%:*}" ;; esac
if ! images="$(ctr --namespace space-builds images list --quiet)"; then
  echo "ctr images list failed in the space-builds namespace" >&2
  exit 3
fi
printf '%s\n' "$images" | grep -F "$repo@sha256:" | sed 's/^/source-image-record: /' || true
printf '%s\n' "$images" | grep '^space\.local/oci-builds:' | sed 's/^/temp-image-record: /' || true
cache_dir=/space/cache/firecracker/builds/oci
if [ -d "$cache_dir" ]; then
  find "$cache_dir" -mindepth 1 -maxdepth 1 \( -name 'space-oci-rootfs-*' -o -name 'space-oci-assembly-*' \) -print | sed 's/^/temp-path: /'
fi
exit 0
"""
_SELECTOR_EXTRAS_VALUE = re.compile(r"[A-Za-z0-9._/-]+=[A-Za-z0-9._-]*(,[A-Za-z0-9._/-]+=[A-Za-z0-9._-]*)*|")


def _pod_path(value: str) -> str:
    if not re.fullmatch(r"/[A-Za-z0-9._/-]{1,200}", value) or ".." in value:
        raise ValueError(f"refusing pod path {value!r}")
    return value


def _label_value(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]{0,63}", value):
        raise ValueError(f"refusing label value {value!r}")
    return value


def _subresource(value: str) -> str:
    if value not in {"status"}:
        raise ValueError(f"refusing subresource {value!r}")
    return value


def _server_side_apply_args(field_manager: str, subresource: str | None = None) -> list[str]:
    args = ["apply", "--server-side", "--force-conflicts", f"--field-manager={_field_manager(field_manager)}", "-f", "-"]
    if subresource:
        args.insert(3, f"--subresource={_subresource(subresource)}")
    return args


def _field_manager(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,62}", value):
        raise ValueError(f"refusing field manager {value!r}")
    return value


def _label_key(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,252}", value):
        raise ValueError(f"refusing label key {value!r}")
    return value


def _parse_nodes(node_list_json: str) -> list[NodeSnapshot]:
    try:
        document = json.loads(node_list_json)
    except ValueError:
        return []
    nodes: list[NodeSnapshot] = []
    for item in (document.get("items") or []) if isinstance(document, dict) else []:
        metadata = item.get("metadata") or {}
        conditions = (item.get("status") or {}).get("conditions") or []
        nodes.append(
            NodeSnapshot(
                name=str(metadata.get("name", "")),
                labels={str(k): str(v) for k, v in (metadata.get("labels") or {}).items()},
                ready=any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions),
                unschedulable=bool((item.get("spec") or {}).get("unschedulable", False)),
            )
        )
    return nodes


def _newest_pod_render(pod_list_json: str) -> SandboxPodRender | None:
    """The newest non-terminating pod: a generation bump can leave two for one sandbox."""

    try:
        document = json.loads(pod_list_json)
    except ValueError:
        return None
    live = [
        item
        for item in ((document.get("items") or []) if isinstance(document, dict) else [])
        if not (item.get("metadata") or {}).get("deletionTimestamp")
    ]
    if not live:
        return None
    newest = max(live, key=lambda item: str((item.get("metadata") or {}).get("creationTimestamp", "")))
    metadata = newest.get("metadata") or {}
    spec = newest.get("spec") or {}
    return SandboxPodRender(
        pod_name=str(metadata.get("name", "")),
        node_selector={str(k): str(v) for k, v in (spec.get("nodeSelector") or {}).items()},
        phase=(newest.get("status") or {}).get("phase"),
        node_name=spec.get("nodeName") or None,
        pool_role=(metadata.get("labels") or {}).get("space.pplx.net/pool-role"),
    )


def _smoke_template_name(value: str) -> str:
    """Only the scenarios' own throwaway names reach the on-node shell."""

    if not re.fullmatch(
        r"smoke-lru-[0-9a-f]{8}|smoke-rec-[0-9a-f]{8}-(happy|stale|orphan|tamper|drift)",
        value,
    ):
        raise ValueError(f"refusing template name {value!r}")
    return value


def _template_digest(value: str) -> str:
    """A digest directory name as a single safe shell word."""

    if not re.fullmatch(r"(?:sha256|blake3):[0-9a-f]{64}", value):
        raise ValueError(f"refusing template digest {value!r}")
    return value


def _template_name(value: str) -> str:
    """A template name or tag as a single safe shell word."""

    if not value or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", value):
        raise ValueError(f"refusing template name {value!r}")
    return value


_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def _ecw_sandbox_id(value: str) -> str:
    """A public ECW sandbox id as one safe path segment (not a kube node name)."""

    if not value or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value):
        raise ValueError(f"refusing ECW sandbox id {value!r}")
    return value


def _completed_deleting_pod_name(pod: object) -> str | None:
    metadata = pod.get("metadata", {}) if isinstance(pod, dict) else {}
    status = pod.get("status", {}) if isinstance(pod, dict) else {}
    name = metadata.get("name")
    statuses = status.get("containerStatuses")
    completed = (
        isinstance(name, str)
        and bool(metadata.get("deletionTimestamp"))
        and isinstance(statuses, list)
        and bool(statuses)
        and all(
            isinstance(container, dict)
            and isinstance(container.get("state"), dict)
            and isinstance(container["state"].get("terminated"), dict)
            and container["state"]["terminated"].get("exitCode") == 0
            for container in statuses
        )
    )
    return name if completed else None


_PROBE_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")


def _service_tcp_target(name: str, service: Mapping[str, Any], port: int) -> TcpTarget:
    """A Service's ClusterIP on one of its declared ports."""

    spec = service.get("spec") or {}
    cluster_ip = str(spec.get("clusterIP") or "")
    try:
        ipaddress.IPv4Address(cluster_ip)
    except ValueError as error:
        raise SmokeConfigurationError(f"{name} has no IPv4 ClusterIP: {cluster_ip!r}") from error
    if not any(
        entry.get("port") == port and entry.get("protocol", "TCP") == "TCP"
        for entry in spec.get("ports") or ()
    ):
        raise SmokeConfigurationError(f"{name}'s Service declares no TCP port {port}")
    return TcpTarget(name=name, host=cluster_ip, port=port)


def _tcp_probe_script(targets: Sequence[TcpTarget], *, timeout: float, pod_ip: str | None) -> str:
    """Bash for the operator shell: one TCP connect per target, one result line each.

    With `pod_ip`, the connects run in the network namespace whose interface
    carries that address, found among the node's netns handles; without it,
    in the shell's own. The connects run concurrently, each under its own
    timeout, so a refusal costs one timeout however many targets there are.
    Each result is one short line, written whole.
    """

    if not targets:
        raise ValueError("a probe needs at least one target")
    if not 0 < timeout <= 30:
        raise ValueError(f"refusing probe timeout {timeout!r}")
    names = [target.name for target in targets]
    if len(set(names)) != len(names):
        raise ValueError(f"duplicate probe targets: {names}")
    lines = ["set -u", 'enter=""']
    if pod_ip is not None:
        address = str(ipaddress.IPv4Address(pod_ip))
        lines += [
            'handle=""',
            "for candidate in /var/run/netns/*; do",
            '  [ -e "$candidate" ] || continue',
            f'  if nsenter --net="$candidate" ip -4 -o addr show 2>/dev/null | grep -qF " inet {address}/"; then',
            '    handle="$candidate"; break',
            "  fi",
            "done",
            'if [ -z "$handle" ]; then echo "NETNS none"; exit 3; fi',
            'echo "NETNS $handle"',
            'enter="nsenter --net=$handle"',
        ]
    seconds = f"{timeout:g}"
    for target in targets:
        if not _PROBE_NAME.fullmatch(target.name) or not 0 < target.port < 65536:
            raise ValueError(f"refusing probe target {target!r}")
        host = str(ipaddress.IPv4Address(target.host))
        lines += [
            "(",
            f"if $enter timeout {seconds} bash -c 'exec 3<>\"/dev/tcp/$0/$1\"' {host} {target.port} 2>/dev/null; then",
            f"  echo 'PROBE {target.name} connected'",
            "else",
            "  rc=$?",
            f"  if [ \"$rc\" = 124 ]; then echo 'PROBE {target.name} timed-out'; else echo \"PROBE {target.name} refused rc=$rc\"; fi",
            "fi",
            ") &",
        ]
    lines.append("wait")
    return "\n".join(lines) + "\n"


def _parse_tcp_probes(output: str, targets: Sequence[TcpTarget]) -> list[TcpProbe]:
    results: dict[str, str] = {}
    for line in output.splitlines():
        if line.startswith("PROBE "):
            _, name, detail = line.split(" ", 2)
            results[name] = detail
    missing = [target.name for target in targets if target.name not in results]
    if missing:
        raise RuntimeError(f"the probe reported nothing for {missing}: {output.strip()!r}")
    return [
        TcpProbe(target=target, connected=results[target.name] == "connected", detail=results[target.name])
        for target in targets
    ]



def _controller_lease(document: Mapping[str, Any]) -> ControllerLease:
    spec = document.get("spec") or {}
    renewed = _kube_epoch(spec.get("renewTime"))
    if renewed is None or not isinstance(spec.get("leaseDurationSeconds"), int):
        raise RuntimeError("the cluster-controller Lease has no renewTime or duration")
    return ControllerLease(
        holder=str(spec.get("holderIdentity") or ""),
        renew_time=renewed,
        acquire_time=_kube_epoch(spec.get("acquireTime")),
        duration_seconds=spec["leaseDurationSeconds"],
        transitions=int(spec.get("leaseTransitions") or 0),
    )


class KubectlLeaseWatch:
    """`kubectl get lease --watch`, keeping the newest renewal it delivered.

    The stream is a sequence of JSON documents, one per change; a reader
    thread decodes them as they arrive. It ends by itself when the apiserver
    goes away, and `stop` ends it otherwise.
    """

    def __init__(self, command: Sequence[str]) -> None:
        self._process = subprocess.Popen(
            list(command), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        self._latest: ControllerLease | None = None
        self._lock = threading.Lock()
        self._reader = threading.Thread(target=self._read, name="lease-watch", daemon=True)
        self._reader.start()

    def _read(self) -> None:
        assert self._process.stdout is not None
        decoder = json.JSONDecoder()
        buffer = ""
        for chunk in iter(lambda: self._process.stdout.read1(65536), b""):  # type: ignore[union-attr]
            buffer += chunk.decode(errors="replace")
            while True:
                buffer = buffer.lstrip()
                try:
                    document, end = decoder.raw_decode(buffer)
                except ValueError:
                    break
                buffer = buffer[end:]
                try:
                    lease = _controller_lease(document)
                except (RuntimeError, ValueError, AttributeError):
                    continue
                with self._lock:
                    if self._latest is None or lease.renew_time >= self._latest.renew_time:
                        self._latest = lease

    def latest(self) -> ControllerLease | None:
        with self._lock:
            return self._latest

    def stop(self) -> None:
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        self._reader.join(timeout=5)


class CtrExitWatch:
    """containerd's task-exit events on one node, keyed by container id.

    Read from the node's own containerd, so it answers while the apiserver
    does not, and it records an exit as it happens: kubelet can remove an
    exited container as soon as it starts the next one, before any listing
    shows it exited, and then reports no last termination for it either.
    """

    # The stderr kept for evidence, from its end.
    STDERR_LIMIT = 4_000

    def __init__(self, command: Sequence[str]) -> None:
        self._process = subprocess.Popen(
            list(command), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self._exits: dict[str, tuple[int, float]] = {}
        self._stderr = ""
        self._lock = threading.Lock()
        self._reader = threading.Thread(target=self._read, name="node-exit-watch", daemon=True)
        self._reader.start()
        self._stderr_reader = threading.Thread(
            target=self._read_stderr, name="node-exit-watch-stderr", daemon=True
        )
        self._stderr_reader.start()

    def _read(self) -> None:
        assert self._process.stdout is not None
        for line in iter(self._process.stdout.readline, b""):
            parsed = task_exit(line.decode(errors="replace"))
            if parsed is not None:
                container_id, exit_code, exited_at = parsed
                with self._lock:
                    self._exits[container_id] = (exit_code, exited_at)

    def _read_stderr(self) -> None:
        assert self._process.stderr is not None
        for line in iter(self._process.stderr.readline, b""):
            with self._lock:
                self._stderr = (self._stderr + line.decode(errors="replace"))[-self.STDERR_LIMIT:]

    def exits(self) -> dict[str, tuple[int, float]]:
        with self._lock:
            return dict(self._exits)

    def diagnostics(self) -> str:
        code = self._process.poll()
        with self._lock:
            stderr = self._stderr
        state = "running" if code is None else f"exited {code}"
        return f"{state}; stderr: {stderr.strip() or '<none>'}"

    def stop(self) -> None:
        # Closing stdin ends the watch in the node; terminate only if it lingers.
        if self._process.stdin is not None and not self._process.stdin.closed:
            try:
                self._process.stdin.close()
            except OSError:
                pass
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        self._reader.join(timeout=5)
        self._stderr_reader.join(timeout=5)


def task_exit(line: str) -> tuple[str, int, float] | None:
    """(container id, exit code, exit time) from one `ctr events` line for a container's own task.

    The line ends in the event's JSON. An exec into the container exits with
    its own `id`, so only an event whose `id` is its `container_id` counts.
    The JSON leaves out a zero exit status.
    """

    start = line.find("{")
    if start < 0:
        return None
    try:
        event = json.loads(line[start:])
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    container_id = event.get("container_id")
    exited_at = event.get("exited_at")
    if not container_id or event.get("id") != container_id or not isinstance(exited_at, dict):
        return None
    try:
        return (
            str(container_id),
            int(event.get("exit_status", 0)),
            int(exited_at.get("seconds", 0)) + int(exited_at.get("nanos", 0)) / 1e9,
        )
    except (TypeError, ValueError):
        return None


def refused_contender_pod(deployment: Mapping[str, Any], *, node: str) -> dict[str, Any]:
    """The live controller Deployment's pod template as one refused pod under another identity.

    Derived rather than written out, so the contender runs the image and env
    of the replicas it is compared against. Only the name, the selector label
    and the ServiceAccount change; the pod is bound to `node`, and an init
    container on the controller's image waits for a gate file in a volume of
    its own before the unchanged controller container starts.
    """

    template = json.loads(json.dumps((deployment.get("spec") or {}).get("template") or {}))
    labels = {
        **((template.get("metadata") or {}).get("labels") or {}),
        "app.kubernetes.io/name": REFUSED_CONTENDER_NAME,
    }
    spec = template.get("spec") or {}
    spec["serviceAccountName"] = REFUSED_CONTENDER_SERVICE_ACCOUNT
    spec.pop("serviceAccount", None)
    spec["nodeName"] = node
    controller = (spec.get("containers") or [{}])[0]
    spec["initContainers"] = [
        *(spec.get("initContainers") or []),
        {
            "name": REFUSED_CONTENDER_GATE_CONTAINER,
            "image": controller.get("image"),
            "imagePullPolicy": controller.get("imagePullPolicy", "IfNotPresent"),
            # As PID 1 the shell ignores SIGTERM unless it traps it, so a
            # closed gate would hold a deletion for the whole grace period.
            "command": [
                "sh", "-c",
                f"trap 'exit 0' TERM; until test -e {REFUSED_CONTENDER_GATE_DIR}/open; do sleep 0.2; done",
            ],
            "resources": controller.get("resources") or {},
            "volumeMounts": [{"name": REFUSED_CONTENDER_GATE_CONTAINER, "mountPath": REFUSED_CONTENDER_GATE_DIR}],
        },
    ]
    spec["volumes"] = [
        *(spec.get("volumes") or []),
        {"name": REFUSED_CONTENDER_GATE_CONTAINER, "emptyDir": {}},
    ]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": REFUSED_CONTENDER_NAME,
            "namespace": KIND_NAMESPACE,
            "labels": labels,
            "annotations": (template.get("metadata") or {}).get("annotations") or {},
        },
        "spec": spec,
    }


_RFC3339 = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})")


def _kube_epoch(value: object) -> float | None:
    """Epoch seconds of a Kubernetes or CRI RFC 3339 time, None when absent.

    The CRI reports nanoseconds, which `datetime` cannot hold; the fraction is
    cut to microseconds.
    """

    if not isinstance(value, str) or not value:
        return None
    match = _RFC3339.fullmatch(value)
    if match is None:
        raise ValueError(f"unparseable timestamp {value!r}")
    whole, fraction, zone = match.groups()
    parsed = datetime.fromisoformat(whole + ("+00:00" if zone == "Z" else zone))
    return parsed.timestamp() + (float("0." + fraction[:6]) if fraction else 0.0)


def _cri_epoch(value: object) -> float | None:
    """`_kube_epoch`, with the CRI's zero time (an unset field) read as None."""

    epoch = _kube_epoch(value)
    return epoch if epoch is not None and epoch > 0 else None

def _kube_pod_name(value: str) -> str:
    if not value or not re.fullmatch(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?", value):
        raise ValueError(f"refusing kube pod name {value!r}")
    return value


@dataclass(frozen=True)
class _LeaseOwnership:
    name: str
    uid: object
    holder: str
    subject: str


def _kube_node_name(value: str) -> str:
    """Refuse anything that is not a DNS-1123 node name before it reaches kubectl."""

    if not value or not re.fullmatch(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?", value):
        raise ValueError(f"refusing kube node name {value!r}")
    return value

@dataclass(frozen=True)
class EcwConfig:
    api_key: str
    domain: str = "local.e2b.test"
    api_url: str | None = None
    api_host: str | None = None
    data_plane_url: str | None = None
    data_plane_domain: str | None = None

    def sdk_options(self) -> dict[str, Any]:
        options: dict[str, Any] = {
            "api_key": self.api_key,
            "domain": self.domain,
        }
        if self.api_url is not None:
            options["api_url"] = self.api_url.rstrip("/")
        if self.api_host is not None:
            options["headers"] = {"Host": self.api_host}
        return options

    def api_endpoint(self, path: str) -> str:
        base_url = self.api_url or f"https://api.{self.domain}"
        return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

    def api_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "space-smoke/0.1",
            "X-API-Key": self.api_key,
        }
        if self.api_host is not None:
            headers["Host"] = self.api_host
        return headers


class _SdkSandbox:
    def __init__(self, sandbox: Any, config: EcwConfig) -> None:
        self._sandbox = sandbox
        self._config = config
        self.sandbox_id = str(sandbox.sandbox_id)
        self._files = _SdkFilesystem(self)
        self._compatibility = _SdkProtocolCompatibility(self)
        self._local_sdk: Any | None = None
        self._sdk_selector_headers: dict[str, str] = {}
        if config.data_plane_url is not None:
            headers = dict(sandbox.connection_config.sandbox_headers)
            normalized = {name.lower(): value for name, value in headers.items()}
            expected = {
                "e2b-sandbox-id": self.sandbox_id,
                "e2b-sandbox-port": "49983",
            }
            for name, value in expected.items():
                if normalized.get(name) != value:
                    raise RuntimeError(
                        f"E2B SDK sandbox headers omitted or changed {name}={value!r}"
                    )
            self._sdk_selector_headers = {
                name: value
                for name, value in headers.items()
                if name.lower() in expected
            }

    @property
    def files(self) -> _SdkFilesystem:
        return self._files

    @property
    def compatibility(self) -> _SdkProtocolCompatibility:
        return self._compatibility

    def run(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
        envs: Mapping[str, str] | None = None,
    ) -> EcwCommandResult:
        options: dict[str, object] = {"timeout": timeout}
        if user is not None:
            options["user"] = user
        if envs is not None:
            options["envs"] = dict(envs)
        return _sdk_command_result(
            lambda: self._data_plane_sdk().commands.run(command, **options)
        )

    def start_background(self, command: str, *, timeout: int = 60) -> EcwProcess:
        handle = self._data_plane_sdk().commands.run(
            command,
            background=True,
            timeout=timeout,
        )
        return _SdkProcess(handle)

    def list_processes(self) -> tuple[ProcessInfo, ...]:
        processes = self._data_plane_sdk().commands.list()
        return tuple(
            ProcessInfo(
                process_id=str(process.pid),
                pid=int(process.pid),
                command=(str(process.cmd), *(str(arg) for arg in process.args)),
            )
            for process in processes
        )

    def get_process(self, process_id: str) -> ProcessInfo | None:
        return next(
            (
                process
                for process in self.list_processes()
                if process.process_id == process_id
            ),
            None,
        )

    def signal_process(self, process_id: str, signal: int) -> bool:
        self._compatibility._compatibility_request(
            "/process.Process/SendSignal",
            method="POST",
            body=json.dumps(
                {"process": {"pid": int(process_id)}, "signal": signal},
                separators=(",", ":"),
            ).encode(),
            user=None,
            request_timeout=60,
        )
        return True

    def kill_process(self, process_id: str) -> bool:
        return bool(self._data_plane_sdk().commands.kill(int(process_id)))

    def connect_process(self, pid: int, *, timeout: int = 60) -> EcwProcess:
        try:
            handle = self._data_plane_sdk().commands.connect(pid, timeout=timeout)
        except Exception as error:  # noqa: BLE001 - normalize SDK exception types
            _raise_normalized_data_plane_error(error)
        return _SdkProcess(handle)

    def public_port(self, port: int) -> EcwPublicPort:
        """Resolve one sandbox port through the SDK's public host contract."""

        if not 1 <= port <= 65535:
            raise ValueError("sandbox port must be between 1 and 65535")
        headers: dict[str, str] = {}
        traffic_access_token = getattr(self._sandbox, "traffic_access_token", None)
        if traffic_access_token:
            headers["e2b-traffic-access-token"] = str(traffic_access_token)
        if self._config.data_plane_url is not None:
            # The SDK debug host is localhost:<guest-port>, but Kind exposes
            # every guest port through one local data-plane port-forward. Use
            # the same sandbox-subdomain host shape the deployed SDK returns.
            domain = self._config.data_plane_domain or self._config.domain
            host = f"{port}-{self.sandbox_id}.{domain}"
            headers.update(
                {
                    "Host": host,
                    "e2b-sandbox-id": self.sandbox_id,
                    "e2b-sandbox-port": str(port),
                }
            )
            base_url = self._config.data_plane_url.rstrip("/")
        else:
            host = str(self._sandbox.get_host(port))
            base_url = f"https://{host}"
        return EcwPublicPort(base_url=base_url, headers=headers)

    def kill(self) -> None:
        _kill_with_sdk(self._sandbox.kill)

    def race_connect(
        self, *, timeout: float, gate: Callable[[], None] | None = None
    ) -> RestoreCallerOutcome:
        # No body and no JSON content type: the handler takes an optional JSON
        # body (`Option<Json<..>>`) whose only field, `timeout`, would also
        # rewrite the auto-lifecycle policy. The hammer sent none; so do we.
        headers = {k: v for k, v in self._config.api_headers().items() if k != "Content-Type"}
        request = Request(
            self._config.api_endpoint(f"/sandboxes/{quote(self.sandbox_id, safe='')}/connect"),
            data=None,
            headers=headers,
            method="POST",
        )
        return _race_request(request, timeout=timeout, gate=gate)

    def fork_response(
        self, *, timeout: int, idempotency_key: str | None = None
    ) -> Mapping[str, Any]:
        # The server accepts exactly one child per request today.
        key = idempotency_key or f"space-smoke-fork-{secrets.token_hex(16)}"
        deadline = time.monotonic() + timeout + 60
        for attempt in range(3):
            try:
                results = _ecw_control_plane_json(
                    self._config,
                    "POST",
                    f"/sandboxes/{quote(self.sandbox_id, safe='')}/fork",
                    body={"count": 1, "timeout": timeout},
                    idempotency_key=key,
                    expected_status=201,
                    timeout=max(0.001, deadline - time.monotonic()),
                )
                break
            except EcwControlPlaneHttpError as error:
                # Admission contention asks the caller to retry this key.
                # A stuck admission must still fail within a bounded budget.
                if error.status != 503 or attempt == 2 or time.monotonic() + 1 >= deadline:
                    raise
                time.sleep(1)
        if not isinstance(results, list) or len(results) != 1:
            raise RuntimeError(f"fork returned an unexpected body: {results!r}")
        (result,) = results
        if not isinstance(result, dict) or "error" in result or "sandboxID" not in result:
            raise RuntimeError(f"fork result is not a child sandbox: {result!r}")
        return {**result, "sandboxID": _ecw_sandbox_id(str(result["sandboxID"]))}

    def delete_forked(self, child_id: str) -> None:
        # Same lifecycle classification as `_kill_with_sdk`, so the fixture's
        # cleanup retry policy applies to fork children exactly as it does to
        # the run-scoped sandbox: 404 is already-gone, 409 is busy, the
        # transient statuses and transport failures are retryable, anything
        # else is a real failure.
        try:
            _ecw_control_plane_json(
                self._config,
                "DELETE",
                f"/sandboxes/{quote(_ecw_sandbox_id(child_id), safe='')}",
                body=None,
                idempotency_key=None,
                expected_status=None,
                timeout=60,
            )
        except EcwControlPlaneHttpError as error:
            if error.status == 404:
                return
            if error.status == 409:
                raise EcwSandboxBusy(str(error)) from error
            if error.status in _ECW_TRANSIENT_STATUSES:
                raise EcwSandboxDeleteRetryable(str(error)) from error
            raise
        except EcwControlPlaneTransportError as error:
            raise EcwSandboxDeleteRetryable(str(error)) from error

    def get_info(self) -> EcwSandboxInfo:
        return _sdk_sandbox_info(self._sandbox.get_info())

    def try_get_info(self) -> EcwSandboxInfo | None:
        e2b = importlib.import_module("e2b")
        try:
            return self.get_info()
        except e2b.SandboxNotFoundException:
            return None

    def set_timeout(self, timeout: int) -> None:
        # The pinned SDK drops request_timeout on this route. Use a fresh
        # connection with a socket timeout; never replay an ambiguous reset,
        # which could restart the idle clock after the first request applied.
        _ecw_control_plane_json(
            self._config,
            "POST",
            f"/sandboxes/{quote(self.sandbox_id, safe='')}/timeout",
            body={"timeout": timeout},
            idempotency_key=None,
            expected_status=None,
            timeout=_ECW_SET_TIMEOUT_REQUEST_SECONDS,
        )

    def refresh(self) -> None:
        _ecw_control_plane_json(
            self._config,
            "POST",
            f"/sandboxes/{quote(self.sandbox_id, safe='')}/refreshes",
            body={},
            idempotency_key=None,
            expected_status=None,
            timeout=30,
        )

    def pause(self) -> None:
        self._sandbox.pause()

    def connect(self, *, timeout: int = 300) -> EcwSandbox:
        sandbox_type = importlib.import_module("e2b").Sandbox
        self._sandbox = sandbox_type.connect(
            self.sandbox_id,
            timeout=timeout,
            **self._config.sdk_options(),
        )
        self._local_sdk = None
        return self

    def _data_plane_sdk(self) -> Any:
        """Return the real SDK sandbox client routed through Kind's local URL."""

        if self._config.data_plane_url is None:
            return self._sandbox
        if self._local_sdk is not None:
            return self._local_sdk

        sandbox_type = importlib.import_module("e2b").Sandbox
        selector_headers = dict(self._sdk_selector_headers)
        selector_headers.update(
            {
                name: value
                for name, value in self._sandbox.connection_config.sandbox_headers.items()
                if name.lower() != "host"
            }
        )
        domain = self._config.data_plane_domain or self._config.domain
        connection_config_type = importlib.import_module(
            "e2b.connection_config"
        ).ConnectionConfig
        connection_config = connection_config_type(
            api_key=self._config.api_key,
            api_url=self._config.api_url,
            domain=self._config.domain,
            sandbox_url=self._config.data_plane_url,
            headers={"Host": f"49983-{self.sandbox_id}.{domain}"},
            extra_sandbox_headers=selector_headers,
        )
        self._local_sdk = sandbox_type(
            sandbox_id=self.sandbox_id,
            sandbox_domain=self._sandbox.sandbox_domain,
            envd_version=self._sandbox._envd_version,
            envd_access_token=self._sandbox._envd_access_token,
            traffic_access_token=self._sandbox.traffic_access_token,
            connection_config=connection_config,
        )
        return self._local_sdk


class _SdkFilesystem:
    """SDK-shaped filesystem calls for Kind's split local data-plane URL."""

    def __init__(self, sandbox: _SdkSandbox) -> None:
        self._sandbox = sandbox

    def write(
        self,
        path: str,
        data: str | bytes | BinaryIO,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        options: dict[str, object] = {"request_timeout": request_timeout}
        if user is not None:
            options["user"] = user
        _sdk_data_plane_call(
            lambda: self._sandbox._data_plane_sdk().files.write(
                path,
                data,
                **options,
            )
        )

    def read(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> str:
        options: dict[str, object] = {"request_timeout": request_timeout}
        if user is not None:
            options["user"] = user
        result = _sdk_data_plane_call(
            lambda: self._sandbox._data_plane_sdk().files.read(
                path,
                **options,
            )
        )
        return str(result)

    def read_bytes(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> bytes:
        options: dict[str, object] = {
            "format": "bytes",
            "request_timeout": request_timeout,
        }
        if user is not None:
            options["user"] = user
        result = _sdk_data_plane_call(
            lambda: self._sandbox._data_plane_sdk().files.read(
                path,
                **options,
            )
        )
        return bytes(result)

    def make_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        options: dict[str, object] = {"request_timeout": request_timeout}
        if user is not None:
            options["user"] = user
        _sdk_data_plane_call(
            lambda: self._sandbox._data_plane_sdk().files.make_dir(
                path,
                **options,
            )
        )

    def exists(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> bool:
        options: dict[str, object] = {"request_timeout": request_timeout}
        if user is not None:
            options["user"] = user
        return bool(
            _sdk_data_plane_call(
                lambda: self._sandbox._data_plane_sdk().files.exists(
                    path,
                    **options,
                )
            )
        )

    def list(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> tuple[EcwFileEntry, ...]:
        options: dict[str, object] = {"request_timeout": request_timeout}
        if user is not None:
            options["user"] = user
        entries = _sdk_data_plane_call(
            lambda: self._sandbox._data_plane_sdk().files.list(
                path,
                **options,
            )
        )
        return tuple(EcwFileEntry(str(entry.name)) for entry in entries)

    def rename(
        self,
        old_path: str,
        new_path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        options: dict[str, object] = {"request_timeout": request_timeout}
        if user is not None:
            options["user"] = user
        _sdk_data_plane_call(
            lambda: self._sandbox._data_plane_sdk().files.rename(
                old_path,
                new_path,
                **options,
            )
        )

    def remove(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        options: dict[str, object] = {"request_timeout": request_timeout}
        if user is not None:
            options["user"] = user
        _sdk_data_plane_call(
            lambda: self._sandbox._data_plane_sdk().files.remove(
                path,
                **options,
            )
        )

    def watch_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        recursive: bool = False,
        request_timeout: float = 60,
    ) -> EcwWatchHandle:
        options: dict[str, object] = {
            "recursive": recursive,
            "request_timeout": request_timeout,
        }
        if user is not None:
            options["user"] = user
        handle = _sdk_data_plane_call(
            lambda: self._sandbox._data_plane_sdk().files.watch_dir(
                path,
                **options,
            )
        )
        return _SdkWatchHandle(handle)


class _SdkProtocolCompatibility:
    """Public protocol probes unavailable through the pinned SDK surface."""

    def __init__(self, sandbox: _SdkSandbox) -> None:
        self._sandbox = sandbox

    def read_with_basic_auth(
        self,
        path: str,
        *,
        user: str,
        request_timeout: float = 10,
    ) -> str:
        body = self._compatibility_request(
            f"/files?{urlencode({'path': path})}",
            user=user,
            request_timeout=request_timeout,
        )
        return body.decode(errors="replace")

    def probe_watch_stream(
        self,
        path: str,
        *,
        user: str | None = None,
        recursive: bool = True,
        request_timeout: float = 10,
    ) -> None:
        encoded_path = path.encode()
        if len(encoded_path) >= 128:
            raise ValueError("watch path must fit in one protobuf length byte")
        body = b"\x0a" + bytes([len(encoded_path)]) + encoded_path
        if recursive:
            body += b"\x10\x01"
        self._compatibility_request(
            "/filesystem.Filesystem/WatchDir",
            method="POST",
            body=body,
            user=user,
            content_type="application/connect+proto",
            read_limit=5,
            request_timeout=request_timeout,
        )

    def _compatibility_request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: bytes | None = None,
        user: str | None,
        content_type: str = "application/json",
        read_limit: int | None = None,
        request_timeout: float,
    ) -> bytes:
        sdk = self._sandbox._data_plane_sdk()
        headers = dict(sdk.connection_config.sandbox_headers)
        headers["Content-Type"] = content_type
        if user is not None:
            selector = base64.b64encode(f"{user}:".encode()).decode()
            headers["Authorization"] = f"Basic {selector}"
        request = Request(
            f"{sdk.envd_api_url.rstrip('/')}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=request_timeout) as response:
                return (
                    response.read() if read_limit is None else response.read(read_limit)
                )
        except HTTPError as error:
            detail = error.read(4_000).decode(errors="replace")
            if error.code in {403, 502} and "permission" in detail.casefold():
                raise EcwPermissionDenied(detail or f"HTTP {error.code}") from error
            raise RuntimeError(
                f"ECW {method} {path} returned HTTP {error.code}: {detail}"
            ) from error
        except URLError as error:
            raise RuntimeError(f"ECW {method} {path} failed: {error.reason}") from error


class _SdkWatchHandle:
    def __init__(self, handle: Any) -> None:
        self._handle = handle

    def get_new_events(self) -> tuple[EcwFilesystemEvent, ...]:
        events = _sdk_data_plane_call(self._handle.get_new_events)
        return tuple(
            EcwFilesystemEvent(
                name=str(event.name),
                type=EcwFilesystemEventType(str(event.type.value)),
            )
            for event in events
        )

    def stop(self) -> None:
        _sdk_data_plane_call(self._handle.stop)


class _SdkProcess:
    def __init__(self, handle: Any) -> None:
        self._handle = handle

    @property
    def pid(self) -> int:
        return int(self._handle.pid)

    @property
    def process_id(self) -> str:
        return str(self.pid)

    def disconnect(self) -> None:
        self._handle.disconnect()

    def wait(self) -> EcwCommandResult:
        return _sdk_command_result(self._handle.wait)

    def kill(self) -> bool:
        return bool(self._handle.kill())


def _is_temporary_dns_connect_error(error: BaseException) -> bool:
    cause: BaseException | None = error
    while cause is not None:
        if isinstance(cause, socket.gaierror):
            return cause.errno == socket.EAI_AGAIN
        cause = cause.__cause__ or cause.__context__
    return False


class EcwSdk:
    def __init__(self, config: EcwConfig) -> None:
        self.config = config

    def attach_fork(self, response: Mapping[str, Any]) -> EcwSandbox:
        # SDK connect can wake a stopped child and mask a broken fork startup.
        # Use only the identity and credentials the fork itself returned.
        sandbox_type = importlib.import_module("e2b").Sandbox
        connection_config_type = importlib.import_module("e2b.connection_config").ConnectionConfig
        version_type = importlib.import_module("packaging.version").Version
        sandbox_headers = {
            "E2b-Sandbox-Id": response["sandboxID"],
            "E2b-Sandbox-Port": str(connection_config_type.envd_port),
        }
        if response.get("envdAccessToken") is not None:
            sandbox_headers["X-Access-Token"] = response["envdAccessToken"]
        sandbox = sandbox_type(
            sandbox_id=response["sandboxID"],
            sandbox_domain=response.get("domain"),
            envd_version=version_type(response["envdVersion"]),
            envd_access_token=response.get("envdAccessToken"),
            traffic_access_token=response.get("trafficAccessToken"),
            connection_config=connection_config_type(
                extra_sandbox_headers=sandbox_headers, **self.config.sdk_options()
            ),
        )
        return _SdkSandbox(sandbox, self.config)

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
    ) -> EcwSandbox:
        sandbox_type = importlib.import_module("e2b").Sandbox
        create_options: dict[str, Any] = {
            "template": template,
            "timeout": timeout,
            "metadata": dict(metadata),
            **self.config.sdk_options(),
        }
        if envs is not None:
            create_options["envs"] = dict(envs)
        if lifecycle is not None:
            create_options["lifecycle"] = {
                "on_timeout": lifecycle.on_timeout.value,
                "auto_resume": lifecycle.auto_resume,
            }
        if allow_internet_access is not None:
            create_options["allow_internet_access"] = allow_internet_access
        if network is not None:
            create_options["network"] = _serialize_network_config(network)
        try:
            sandbox = sandbox_type.create(
                **create_options,
            )
        except Exception as error:
            httpx = importlib.import_module("httpx")
            # EAI_AGAIN is raised while resolving the API host, before httpx can
            # deliver the POST. Other connect failures retain ambiguous outcome
            # handling because a second create could duplicate a committed request.
            if isinstance(
                error, httpx.ConnectError
            ) and _is_temporary_dns_connect_error(error):
                raise EcwCreateNotDelivered(str(error.__cause__)) from error
            raise
        return _SdkSandbox(sandbox, self.config)

    def build_template(
        self,
        recipe: TemplateRecipe,
        *,
        alias: str,
        cpu_count: int,
        memory_mb: int,
        tags: Sequence[str],
        log_tail: int = 40,
    ) -> TemplateBuildResult:
        """`Template.build` through the SDK, blocking until the build is ready or failed."""

        e2b = importlib.import_module("e2b")
        tail: deque[str] = deque(maxlen=log_tail)

        def on_log(entry: Any) -> None:
            tail.append(
                f"{getattr(entry, 'timestamp', '')} {getattr(entry, 'level', '')} "
                f"{getattr(entry, 'message', entry)}".strip()
            )

        with tempfile.TemporaryDirectory(prefix="space-smoke-template-") as tmp:
            root = Path(tmp)
            for name, text in recipe.files.items():
                if "/" in name or name.startswith("."):
                    raise ValueError(f"refusing context file name {name!r}")
                (root / name).write_text(text)
            dockerfile = root / "Dockerfile"
            dockerfile.write_text(recipe.dockerfile)
            template = e2b.Template(file_context_path=root).from_dockerfile(str(dockerfile))
            if recipe.envs:
                template = template.set_envs(dict(recipe.envs))
            for command, user in recipe.run_cmds:
                template = template.run_cmd(command, user=user)
            final = (
                template
                if recipe.start_cmd is None
                else template.set_start_cmd(
                    recipe.start_cmd, e2b.wait_for_timeout(recipe.ready_timeout_seconds)
                )
            )
            started = time.monotonic()
            info = e2b.Template.build(
                final,
                alias=alias,
                tags=list(tags),
                cpu_count=cpu_count,
                memory_mb=memory_mb,
                on_build_logs=on_log,
                **self._build_sdk_options(),
            )
        return TemplateBuildResult(
            template_id=str(getattr(info, "template_id", "") or ""),
            build_id=str(getattr(info, "build_id", "") or ""),
            alias=alias,
            elapsed_seconds=round(time.monotonic() - started, 1),
            log_tail=tuple(tail),
        )

    def build_status(self, template_id: str, build_id: str) -> str:
        info = _ecw_control_plane_json(
            self.config,
            "GET",
            f"/templates/{quote(template_id, safe='')}/builds/{quote(build_id, safe='')}/status",
            body=None,
            idempotency_key=None,
            expected_status=200,
            timeout=30,
        )
        status = info.get("status") if isinstance(info, dict) else None
        if not isinstance(status, str):
            raise RuntimeError(f"the build status response carried no status: {info!r}")
        return status

    def _build_sdk_options(self) -> dict[str, Any]:
        """SDK options for `Template.build`: the Host header is the API URL's own authority.

        The API derives the file-upload URL it hands back from the request's
        Host header (`request_origin` in the e2b template-build handlers), and
        the SDK uploads to that absolute URL with a fresh client that has to
        resolve the name. A Host with no DNS record — `api.local.e2b.test`, the
        kind default for every other ECW call — fails the upload with `Name or
        service not known` (bench 2026-09-23). The loopback authority is
        allowlisted by the kind chart for exactly this, and is what the shell
        suite derived. Off kind, the authority equals the configured host.
        """

        options = self.config.sdk_options()
        if self.config.api_url is not None:
            authority = urlsplit(self.config.api_url).netloc
            if authority:
                options["headers"] = {**options.get("headers", {}), "Host": authority}
        return options

    def attempt_create(
        self,
        *,
        template: str,
        timeout: int,
        metadata: Mapping[str, str],
        allow_internet_access: bool | None = None,
        network: NetworkConfig | None = None,
    ) -> EcwCreateAttempt:
        """Observe the raw create status when that status is the invariant."""

        body: dict[str, object] = {
            "templateID": template,
            "timeout": timeout,
            "metadata": dict(metadata),
        }
        if allow_internet_access is not None:
            body["allowInternetAccess"] = allow_internet_access
        headers = self.config.api_headers()
        if network is not None:
            body["network"] = _serialize_network_config(network)

        request = Request(
            self.config.api_endpoint("/sandboxes"),
            data=json.dumps(body).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=90) as response:
                status = response.status
                response_body = response.read().decode(errors="replace")
        except HTTPError as error:
            status = error.code
            response_body = error.read(4_000).decode(errors="replace")
        except URLError as error:
            raise RuntimeError(f"ECW sandbox create failed: {error.reason}") from error

        sandbox_id: str | None = None
        if 200 <= status < 300:
            try:
                payload = json.loads(response_body)
            except json.JSONDecodeError as error:
                raise RuntimeError(
                    "ECW sandbox create returned a non-JSON success body"
                ) from error
            raw_id = payload.get("sandboxID")
            if not isinstance(raw_id, str) or not raw_id:
                raise RuntimeError("ECW sandbox create success omitted sandboxID")
            sandbox_id = raw_id
        return EcwCreateAttempt(status, response_body, sandbox_id)

    def race_ecw_request(
        self,
        method: str,
        path: str,
        body: Mapping[str, object] | None,
        *,
        client_id: str,
        timeout: float,
        gate: Callable[[], None] | None = None,
    ) -> LimiterProbeOutcome:
        """One raw request for `client_id`, classified with its Retry-After and body."""

        headers = dict(self.config.api_headers())
        headers["x-client-id"] = client_id
        data = None
        if body is None:
            headers.pop("Content-Type", None)
        else:
            data = json.dumps(dict(body)).encode()
        request = Request(self.config.api_endpoint(path), data=data, headers=headers, method=method)
        if gate is not None:
            gate()
        try:
            with urlopen(request, timeout=timeout) as response:
                status = int(response.status)
                retry_after = response.headers.get("Retry-After")
                text = response.read().decode(errors="replace")
        except HTTPError as error:
            status = error.code
            retry_after = error.headers.get("Retry-After") if error.headers else None
            text = error.read(4_000).decode(errors="replace")
        except (URLError, OSError) as error:
            reason = getattr(error, "reason", error)
            return LimiterProbeOutcome(0, None, str(reason)[:300], None, time.monotonic())
        # Only a parsed string `message` field is kept. A successful create body
        # carries `envdAccessToken`, so raw text must never reach the evidence;
        # an error body that is not JSON (a proxy page, a bare string) is kept
        # as text only when the status says it is an error.
        message = ""
        sandbox_id: str | None = None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = None
        if not isinstance(payload, dict) and status >= 400:
            message = text.strip()
        if isinstance(payload, dict):
            if isinstance(payload.get("message"), str):
                message = payload["message"]
            raw_id = payload.get("sandboxID")
            if 200 <= status < 300 and isinstance(raw_id, str) and raw_id:
                sandbox_id = raw_id
        return LimiterProbeOutcome(status, retry_after, message[:300], sandbox_id, time.monotonic())

    def kill_sandbox(self, sandbox_id: str) -> None:
        e2b = importlib.import_module("e2b")
        _kill_with_sdk(e2b.Sandbox.kill, _sql_uuid(sandbox_id), **self.config.sdk_options())

    def kill_matching(
        self,
        metadata: Mapping[str, str],
        *,
        attempts: int = 10,
        interval_seconds: float = 1.0,
    ) -> None:
        if attempts <= 0:
            raise ValueError("reconciliation attempts must be positive")
        e2b = importlib.import_module("e2b")
        for attempt in range(attempts):
            matched = False
            paginator = e2b.Sandbox.list(
                query=e2b.SandboxQuery(metadata=dict(metadata)),
                limit=100,
                **self.config.sdk_options(),
            )
            while paginator.has_next:
                for sandbox in paginator.next_items():
                    matched = True
                    _kill_with_sdk(
                        e2b.Sandbox.kill,
                        sandbox.sandbox_id,
                        **self.config.sdk_options(),
                    )
            if matched:
                return
            if attempt + 1 < attempts:
                time.sleep(interval_seconds)
        raise RuntimeError(
            "sandbox create may have committed, but no sandbox became visible "
            "during run-metadata reconciliation"
        )

    def list_pages(
        self,
        metadata: Mapping[str, str],
        *,
        limit: int,
        max_pages: int,
    ) -> EcwSandboxPages:
        if limit <= 0:
            raise ValueError("sandbox page size must be positive")
        if max_pages <= 0:
            raise ValueError("sandbox page cap must be positive")
        e2b = importlib.import_module("e2b")
        paginator = e2b.Sandbox.list(
            query=e2b.SandboxQuery(metadata=dict(metadata)),
            limit=limit,
            **self.config.sdk_options(),
        )
        pages: list[tuple[EcwSandboxInfo, ...]] = []
        while paginator.has_next and len(pages) < max_pages:
            pages.append(
                tuple(_sdk_sandbox_info(item) for item in paginator.next_items())
            )
        return EcwSandboxPages(tuple(pages), bool(paginator.has_next))


def _kill_with_sdk(
    kill: Callable[..., object],
    *args: object,
    **kwargs: Any,
) -> None:
    """Treat SDK not-found as success and normalize transient delete failures."""

    e2b = importlib.import_module("e2b")
    try:
        deleted = kill(*args, **kwargs)
        # The pinned SDK maps an ECW DELETE 404 to False.
        if deleted is False:
            return
    except Exception as error:
        status = _sdk_http_status(error)
        if isinstance(error, e2b.SandboxException) and status == 409:
            raise EcwSandboxBusy(str(error)) from error
        if isinstance(error, e2b.SandboxException) and status in {
            408,
            429,
            502,
            503,
            504,
        }:
            raise EcwSandboxDeleteRetryable(str(error)) from error
        raise


class EcwControlPlaneHttpError(RuntimeError):
    """A public ECW route answered with a status the caller did not expect."""

    def __init__(self, method: str, path: str, status: int, detail: str = "") -> None:
        suffix = f": {detail}" if detail else ""
        super().__init__(f"ECW {method} {path} returned HTTP {status}{suffix}")
        self.status = status


class EcwControlPlaneTransportError(RuntimeError):
    """A public ECW route could not be reached at all."""


# Statuses the SDK delete path already treats as "try again", mirrored here so
# routes the pinned SDK does not expose classify identically.
_ECW_TRANSIENT_STATUSES = frozenset({408, 429, 502, 503, 504})

_ECW_SET_TIMEOUT_REQUEST_SECONDS = 4.0


def _ecw_control_plane_json(
    config: EcwConfig,
    method: str,
    path: str,
    *,
    body: Mapping[str, object] | None,
    idempotency_key: str | None,
    expected_status: int | None,
    timeout: float,
) -> Any:
    """Call a public ECW route the pinned SDK does not expose; return its JSON.

    Non-2xx (or not `expected_status`) raises `EcwControlPlaneHttpError` with
    the status attached; a failure to reach the server at all raises
    `EcwControlPlaneTransportError`. Callers that need lifecycle semantics
    (retryable, busy, already-gone) classify from those.
    """

    headers = config.api_headers()
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    data = json.dumps(dict(body)).encode() if body is not None else None
    request = Request(config.api_endpoint(path), data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.status
            # 204 (refresh, delete) carries no body; don't read or parse one.
            raw = response.read() if status != 204 else b""
    except HTTPError as error:
        detail = error.read(4_000).decode(errors="replace").strip()
        raise EcwControlPlaneHttpError(method, path, error.code, detail) from error
    except (URLError, OSError) as error:
        reason = getattr(error, "reason", error)
        raise EcwControlPlaneTransportError(f"ECW {method} {path} failed: {reason}") from error
    if expected_status is not None and status != expected_status:
        raise EcwControlPlaneHttpError(method, path, status, f"expected {expected_status}")
    if expected_status is None and not 200 <= status < 300:
        raise EcwControlPlaneHttpError(method, path, status)
    return json.loads(raw) if raw else None


def _sdk_sandbox_info(info: Any) -> EcwSandboxInfo:
    raw_state = getattr(info, "state", None)
    state = EcwSandboxState(str(raw_state)) if raw_state is not None else None
    template_id = getattr(info, "template_id", None)
    end_at = getattr(info, "end_at", None)
    return EcwSandboxInfo(
        sandbox_id=str(info.sandbox_id),
        template_id=str(template_id) if template_id is not None else None,
        state=state,
        end_at=end_at if isinstance(end_at, datetime) else None,
    )


def _sdk_http_status(error: Exception) -> int | None:
    prefix, separator, _detail = str(error).partition(":")
    if not separator:
        return None
    try:
        return int(prefix)
    except ValueError:
        return None


def _sdk_data_plane_call(operation: Callable[[], Any]) -> Any:
    try:
        return operation()
    except Exception as error:  # noqa: BLE001 - normalize SDK exception types
        _raise_normalized_data_plane_error(error)


def _sdk_command_result(operation: Callable[[], Any]) -> EcwCommandResult:
    e2b = importlib.import_module("e2b")
    try:
        result = operation()
    except Exception as error:  # noqa: BLE001 - normalize SDK exception types
        if isinstance(error, e2b.CommandExitException):
            command_error: Any = error
            return EcwCommandResult(
                int(command_error.exit_code),
                str(command_error.stdout),
                str(command_error.stderr),
            )
        _raise_normalized_data_plane_error(error)
    return EcwCommandResult(
        int(result.exit_code),
        str(result.stdout),
        str(result.stderr),
    )


def _raise_normalized_data_plane_error(error: Exception) -> NoReturn:
    e2b = importlib.import_module("e2b")
    status = _sdk_http_status(error)
    detail = str(error).casefold()
    # The pinned SDK maps HTTP 502 through format_sandbox_timeout_exception(),
    # which drops the status but preserves the backend's permission message.
    # Keep this narrow to the SDK's timeout type so an arbitrary server error
    # containing the same words cannot satisfy an identity invariant.
    normalized_502_permission_denial = isinstance(error, e2b.TimeoutException) and (
        "permission denied" in detail
    )
    if isinstance(error, e2b.SandboxException) and (
        "code.permission_denied" in detail
        or (status in {403, 502} and "permission denied" in detail)
        or normalized_502_permission_denial
    ):
        raise EcwPermissionDenied(str(error)) from error
    if isinstance(error, e2b.NotFoundException) or (
        isinstance(error, e2b.SandboxException) and status == 404
    ):
        raise EcwDataPlaneNotFound(str(error)) from error
    unavailable_status = status in {408, 429, 502, 503, 504} or (
        status == 409 and "sandbox is not running" in str(error).casefold()
    )
    if isinstance(error, (e2b.TimeoutException, e2b.RateLimitException)) or (
        isinstance(error, e2b.SandboxException) and unavailable_status
    ):
        raise EcwSandboxUnavailable(str(error)) from error
    raise error


def validate_loopback_environment(
    endpoint_url: str,
    allow_remote: bool,
    *,
    scenario_id: str,
    endpoint_name: str = "API_URL",
    remote_override_name: str = "SPACE_SMOKE_ALLOW_REMOTE_API",
) -> None:
    try:
        parsed = urlsplit(endpoint_url)
        port = parsed.port
    except ValueError as error:
        raise SmokeConfigurationError(
            f"{endpoint_name}={endpoint_url} is not a valid endpoint URL"
        ) from error
    if (
        parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost"}
        and port is not None
        and parsed.username is None
        and parsed.password is None
    ):
        return
    if not allow_remote:
        raise SmokeConfigurationError(
            f"{endpoint_name}={endpoint_url} is not loopback; "
            f"{scenario_id} is loopback-only "
            f"(set {remote_override_name}=1 to override)"
        )
