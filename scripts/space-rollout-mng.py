#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "boto3==1.43.13",
#   "requests==2.34.2",
# ]
# ///
"""Two-MNG node rollout orchestration.

Operator-driven loop that drains a source EKS managed node group (MNG) onto a
target MNG. The cluster-controller's drain reconciler does the per-sandbox
migration. This script does the per-node sequencing:

  * Maintains the x/x/x invariant (min == desired == max) on both MNGs at
    every observation point between waves. Source shrinks `x/x/x` →
    `(x-1)/(x-1)/(x-1)` per drained node; target grows symmetrically.
    Implementation: top-of-loop reconcile to x/x/x, per-wave drop of source
    `min` just before issuing terminates, then re-reconcile on the next
    iteration.
  * Issues `POST /admin/api/v1/nodes/{id}:drain
    {"replace_after_terminate": false, "mode": "forced",
    "require_undrained": true}` against the SPACE admin API for up to
    `--max-concurrent-node-drains` source nodes at a time.
    `mode` is explicit because the API default is `gentle`, which waits for
    residents to leave on their own and would stall a wave on any long-lived
    sandbox. ASG terminate-with-decrement decrements source
    `desired`; the next loop iteration pulls `min` and `max` to match.
  * Polls AWS + the SPACE admin API for drain progress and emits structured
    JSON-line telemetry on stdout.
  * Idempotent: re-running mid-rollout reads current state from AWS and the
    admin API and continues from where it left off.

Inputs:
    --eks-cluster-name, --source-mng, --target-mng, --cohort,
    --max-concurrent-node-drains, --headroom-count, --space-api, --aws-region,
    --k8s-context, --stuck-threshold, --poll-interval, --dry-run.

Authentication:
    AWS via the standard env / SSO chain (use the project `aws-sso-*` shell
    aliases). SPACE admin bearer token read from `SPACE_API_TOKEN` (override
    with `--token-env`).
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import enum
import json
import os
import subprocess
import sys
import time
from typing import Any, Iterable

import boto3
import botocore.exceptions
import requests

DEFAULT_POLL_INTERVAL_SEC = 15
DEFAULT_STUCK_THRESHOLD_SEC = 30 * 60
DEFAULT_TOKEN_ENV = "SPACE_API_TOKEN"

class DrainPhase(enum.StrEnum):
    """Per-node rollout phase, derived from the admin API row.

    `NONE` and `GONE` are synthetic states the orchestrator computes —
    `NONE` means "admin row present but `drain_phase IS NULL`", `GONE`
    means "no admin row" (correlation by `k8s_node_name` returned None).
    The remaining members mirror the admin-API `drain_phase` strings
    exactly so `n.admin.drain_phase == DrainPhase.DRAINING` works without
    extra coercion.
    """

    NONE = "none"
    DRAINING = "draining"
    DRAINED = "drained"
    TERMINATING = "terminating"
    GONE = "gone"


class Event(enum.StrEnum):
    """JSON-line telemetry vocabulary emitted on stdout.

    Centralizing the catalog so the surface a downstream consumer (Datadog,
    log-aggregator pipelines, the smoke-test runner) needs to parse lives
    in one place.
    """

    ROLLOUT_STARTED = "rollout_started"
    TICK = "tick"
    EVACUATION_POD_PENDING = "evacuation_pod_pending"
    TARGET_RATCHET = "target_ratchet"
    TARGET_RATCHET_DEFERRED = "target_ratchet_deferred"
    TERMINATE_DEFERRED_SOURCE_UPDATING = "terminate_deferred_source_updating"
    WAITING_TARGET_READY = "waiting_target_ready"
    WAITING_SOURCE_DESIRED_ZERO = "waiting_source_desired_zero"
    EVACUATION_COMPLETE = "evacuation_complete"
    DRAIN_STARTED = "drain_started"
    DRAIN_SKIPPED_ALREADY_DRAINING = "drain_skipped_already_draining"
    DRAIN_SKIPPED_PHASE_UNREADABLE = "drain_skipped_phase_unreadable"
    DRAIN_COMMITTED_SUBSTRATE_PATCH_FAILED = "drain_committed_substrate_patch_failed"
    DRAIN_WAVE_MADE_NO_PROGRESS = "drain_wave_made_no_progress"
    TERMINATE_STARTED = "terminate_started"
    TERMINATE_BLOCKED_ACTIVE_SANDBOXES = "terminate_blocked_active_sandboxes"
    TERMINATE_BLOCKED_PHASE_DRIFT = "terminate_blocked_phase_drift"
    TERMINATE_REFUSED_UNKNOWN = "terminate_refused_unknown"
    TERMINATE_BLOCKED_LOCAL_SNAPSHOTS = "terminate_blocked_local_snapshots"
    # The `:terminate` guard refused on its drain-gate half: rows the node-drain
    # reconciler would also hold the node for (`suspending`, non-evacuation
    # `restoring`, a non-durable `suspended`, an upload in flight from it).
    # Distinct from the listing's local-snapshot count, which has no
    # `suspending` or non-evacuation `restoring` arm.
    TERMINATE_BLOCKED_DRAIN_GATE = "terminate_blocked_drain_gate"
    TERMINATE_BLOCKED_REPLACE_SEMANTICS = "terminate_blocked_replace_semantics"
    NODE_STUCK = "node_stuck"
    DRAINING = "draining"
    DRAINED = "drained"
    TERMINATING = "terminating"
    INTERRUPTED = "interrupted"
    TICK_ERROR = "tick_error"
    SOURCE_PREPARED_FOR_WAVE = "source_prepared_for_wave"
    MNG_RECONCILED_XXX = "mng_reconciled_xxx"
    WAITING_FINAL_TARGET_ACTIVE = "waiting_final_target_active"
    FATAL_AWS_CREDENTIALS = "fatal_aws_credentials"
    FATAL_AWS_CREDENTIALS_EXPIRED = "fatal_aws_credentials_expired"
    TARGET_NODE_UNTAINTED = "target_node_untainted"
    DRY_RUN_SET_SCALING = "dry_run_set_scaling"
    DRY_RUN_DRAIN = "dry_run_drain"
    DRY_RUN_TERMINATE = "dry_run_terminate"
    DRY_RUN_UNTAINT = "dry_run_untaint"


PHASES_IN_FLIGHT = (DrainPhase.DRAINING, DrainPhase.DRAINED, DrainPhase.TERMINATING)

# ASG LifecycleStates where the instance is mid-termination: ASG has
# accepted the terminate-with-decrement call but the eventual
# `desiredSize` decrement may not yet be visible in `describe_nodegroup`.
# A source node in this state with `admin = None` (admin row already
# deleted by the reconciler) still has a pending source-desired
# decrement and must be counted in `source_pending_decrements`.
ASG_TERMINATING_STATES = frozenset(
    {
        "Terminating",
        "Terminating:Wait",
        "Terminating:Proceed",
    }
)

# ASG LifecycleStates that consume MNG capacity. Used for the
# "is the source MNG drained yet" exit condition.
ASG_LIVE_STATES = frozenset(
    {
        "Pending",
        "Pending:Wait",
        "Pending:Proceed",
        "InService",
        "Standby",
    }
)

# ASG LifecycleStates where the instance has joined the cluster and may be
# running sandboxes. Used for target ratchet sizing: Pending* nodes have not
# joined yet, so they don't yet consume target capacity for migration.
ASG_REGISTERED_STATES = frozenset({"InService", "Standby"})

KUBECTL_TIMEOUT_SEC = 30

# Default cohort taint key the rollout-mng module declares on the target MNG
# and that evacuation pods tolerate. Operators who
# override `cohort_taint_key` on the TF module (or who run the script against
# a cluster with a different convention) must pass `--cohort-taint-key` with
# the matching value so the script's untaint loop targets the right key.
DEFAULT_COHORT_TAINT_KEY = "space.pplx.net/rollout-cohort"

# Closed set of rollout dimensions per RFC §Observability. Carried on
# `rollout_started` / `tick` / `evacuation_pod_pending` events as the
# `rollout_kind` tag for the three RFC-specified gauges
# (`node_drain.rollout_nodes_remaining`, `node_drain.rollout_capacity_peak`,
# `node_drain.evacuation_pods_pending`), which the Datadog log pipeline
# derives from the events. The set matches the RFC verbatim.
ROLLOUT_KINDS = ("lt", "instance_type", "eks_version", "capacity_type", "az", "other")

# Label every sandbox pod (user-create, restore, evacuation, pooled) carries
# via `build_sandbox_pod` (`crates/libs/common/core/src/k8s.rs`). Used to
# scope the cluster-wide Pending-pod query for the
# `node_drain.evacuation_pods_pending` derived gauge.
SANDBOX_POD_LABEL_SELECTOR = "app.kubernetes.io/name=space-sandbox"

# botocore error codes that signal the AWS session is permanently broken from
# the loop's perspective: refreshing them requires an out-of-band SSO login
# the script can't perform autonomously. Anything else is treated as a
# transient blip and retried via the broad-except.
AWS_CREDENTIAL_FAILURE_CODES = frozenset(
    {
        "ExpiredToken",
        "ExpiredTokenException",
        "RequestExpired",
        "InvalidClientTokenId",
        "UnrecognizedClientException",
        "TokenRefreshRequired",
    }
)


# --- Telemetry --------------------------------------------------------------


def emit_event(event: Event, **fields: Any) -> None:
    """Print one JSON-line telemetry event to stdout."""

    payload = {
        "ts": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    json.dump(payload, sys.stdout, default=str, separators=(",", ":"))
    sys.stdout.write("\n")
    sys.stdout.flush()


# --- Data shapes ------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class MngState:
    name: str
    cluster: str
    desired_size: int
    min_size: int
    max_size: int
    status: str
    asg_names: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class AsgInstance:
    instance_id: str
    asg_name: str
    lifecycle_state: str
    health_status: str
    # Empty string when the ASG response omits AvailabilityZone (shouldn't
    # happen in practice, but keeps the picker robust if it ever does — all
    # such instances land in a single bucket and degrade to instance_id sort).
    availability_zone: str = ""


@dataclasses.dataclass(frozen=True)
class Ec2Instance:
    instance_id: str
    private_dns: str | None
    state: str


# Every admin route lives under this prefix, spelled once so the four call sites
# cannot drift apart from each other or from the router.
#
# `/admin/api/v1` is the spelling that resolves everywhere: the shared-host
# api-server mounts it, the dedicated admin host reaches the same tree at `/v1`,
# and the ingress `/admin` rule is a bare `pathType: Prefix` with no
# rewrite-target, so the path arrives at the backend intact. `space-admin-cli`
# picked it for the same reason (`admin_client.rs`, `ADMIN_API_PREFIX`).
#
# Getting it wrong is expensive to notice, which is why it is a named constant
# rather than four literals. `list_nodes` is the first admin call in a tick, so
# a prefix the router does not mount raises before any of `run_loop`'s exit
# paths is reachable; the tick handler emits `tick_error`, sleeps
# `--poll-interval-sec`, and loops. With no deadline and no max-tick count the
# process retries forever, draining nothing, and never exits with any status.
ADMIN_API_PREFIX = "/admin/api/v1"


def resolved_drain_mode(raw: str | None) -> str:
    """Resolve a raw `drain_mode` the way the control plane does.

    Mirrors `NodeDrainMode::from_column_str`: NULL and any unrecognized value
    read as `gentle`, so a mode nobody set — or one this build cannot parse —
    never selects the evacuating branch. Mirrored rather than referenced because
    this process has no access to that code, and kept to a single function so
    there is one place to correct if the closed set ever grows.

    Used only for reporting. Nothing here decides anything on a mode; the
    endpoint owns that, and duplicating a safety rule for a decision would be
    the wrong trade.
    """
    return raw if raw in ("gentle", "forced") else "gentle"


@dataclasses.dataclass(frozen=True)
class NodeDrainRead:
    """The drain columns of one node, as the pre-POST re-read in
    `AdminApi.drain` sees them. No occupancy counts: that read asks the
    endpoint not to compute them."""

    drain_phase: str | None
    drain_mode: str | None
    terminate_decrement_capacity: bool


class MalformedAdminResponse(Exception):
    """A 2xx admin response whose body is not the shape we asked for.

    Distinct from a transport error and from a 404: the request *succeeded* and
    something answered, but the answer is missing a field the real endpoint
    always sends. The dangerous reading is a body from somewhere other than the
    node route — a proxy error page rendered as JSON, a redirect landing
    elsewhere, a future response shape — because `dict.get` turns every one of
    those into a plausible-looking default.
    """


@dataclasses.dataclass(frozen=True)
class AdminNode:
    id: str
    k8s_node_name: str | None
    address: str | None
    drain_phase: str | None
    drain_started_at: dt.datetime | None
    state: str | None
    sandboxes_active: int
    sandboxes_local_snapshots: int
    # `True` if the eventual ASG terminate will pass
    # `ShouldDecrementDesiredCapacity=true`, i.e. shrink the source MNG
    # monotonically. `False` (the API default for drains not started with
    # `replace_after_terminate=false`) means the ASG would auto-replace the
    # terminated node, breaking the rollout invariant.
    terminate_decrement_capacity: bool
    # The mode of the drain in flight, as the admin API reports it. Carried so a
    # refusal can name *which* kind of drain it declined to escalate: an
    # inherited `gentle` one is the destructive case, a `forced` one is our own
    # after a restart. Last, with a default, so the many fixtures that build this
    # struct without it keep working — nothing but the skip event reads it.
    drain_mode: str | None = None


@dataclasses.dataclass(frozen=True)
class NodeStatus:
    """A single MNG instance correlated with its admin row and K8s readiness."""

    instance_id: str
    asg_name: str
    private_dns: str | None
    lifecycle_state: str
    admin: AdminNode | None
    k8s_ready: bool
    # AZ from the ASG describe response. Empty string when unavailable; the
    # picker treats that as its own bucket so behavior degrades gracefully.
    availability_zone: str = ""

    @property
    def rollout_phase(self) -> DrainPhase:
        if self.admin is None:
            return DrainPhase.GONE
        if self.admin.drain_phase is None:
            return DrainPhase.NONE
        try:
            return DrainPhase(self.admin.drain_phase)
        except ValueError:
            # Defensive: an unknown phase string from a newer admin API
            # falls back to NONE so the orchestrator doesn't crash.
            return DrainPhase.NONE


# --- AWS plumbing -----------------------------------------------------------


@dataclasses.dataclass
class AwsClients:
    eks: Any
    autoscaling: Any
    ec2: Any


def make_aws_clients(region: str) -> AwsClients:
    session = boto3.session.Session(region_name=region)
    return AwsClients(
        eks=session.client("eks"),
        autoscaling=session.client("autoscaling"),
        ec2=session.client("ec2"),
    )


def describe_mng(eks: Any, cluster: str, name: str) -> MngState:
    resp = eks.describe_nodegroup(clusterName=cluster, nodegroupName=name)
    ng = resp["nodegroup"]
    scaling = ng.get("scalingConfig", {})
    asgs = tuple(a["name"] for a in ng.get("resources", {}).get("autoScalingGroups", []))
    return MngState(
        name=name,
        cluster=cluster,
        desired_size=int(scaling.get("desiredSize", 0)),
        min_size=int(scaling.get("minSize", 0)),
        max_size=int(scaling.get("maxSize", 0)),
        status=ng.get("status", "UNKNOWN"),
        asg_names=asgs,
    )


def list_asg_instances(asg_client: Any, asg_names: Iterable[str]) -> list[AsgInstance]:
    names = list(asg_names)
    if not names:
        return []
    out: list[AsgInstance] = []
    paginator = asg_client.get_paginator("describe_auto_scaling_groups")
    for page in paginator.paginate(AutoScalingGroupNames=names):
        for group in page.get("AutoScalingGroups", []):
            for inst in group.get("Instances", []):
                out.append(
                    AsgInstance(
                        instance_id=inst["InstanceId"],
                        asg_name=group["AutoScalingGroupName"],
                        lifecycle_state=inst.get("LifecycleState", ""),
                        health_status=inst.get("HealthStatus", ""),
                        availability_zone=inst.get("AvailabilityZone", ""),
                    )
                )
    return out


def describe_ec2_instances(ec2: Any, instance_ids: Iterable[str]) -> dict[str, Ec2Instance]:
    ids = list(instance_ids)
    out: dict[str, Ec2Instance] = {}
    if not ids:
        return out
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(InstanceIds=ids):
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                iid = inst["InstanceId"]
                out[iid] = Ec2Instance(
                    instance_id=iid,
                    private_dns=inst.get("PrivateDnsName") or None,
                    state=inst.get("State", {}).get("Name", ""),
                )
    return out


def set_mng_scaling(
    eks: Any,
    cluster: str,
    name: str,
    *,
    min_size: int,
    desired_size: int,
    max_size: int,
    dry_run: bool,
) -> None:
    """Set all three `scalingConfig` fields on an MNG in one EKS API call.

    The rollout maintains an "x/x/x" invariant on both MNGs at steady-state
    observation points (min == desired == max). EKS Auto Scaling rejects
    `desired < min` and refuses to decrement below `min` on
    `TerminateInstanceInAutoScalingGroup(ShouldDecrementDesiredCapacity=true)`,
    so the script issues all three together rather than ratcheting
    `desired` alone and inheriting whatever `min`/`max` Terraform set at
    creation.
    """

    if dry_run:
        emit_event(
            Event.DRY_RUN_SET_SCALING,
            cluster=cluster,
            nodegroup=name,
            min_size=min_size,
            desired_size=desired_size,
            max_size=max_size,
        )
        return
    eks.update_nodegroup_config(
        clusterName=cluster,
        nodegroupName=name,
        scalingConfig={
            "minSize": min_size,
            "desiredSize": desired_size,
            "maxSize": max_size,
        },
    )


# --- SPACE admin API --------------------------------------------------------


# How often a still-blocked `drained` node is re-reported. The terminate wave
# asks again every `TERMINATE_BLOCKED_RETRY_SEC`, and `detect_stuck` cannot see it
# (that pass only looks at `DRAINING`), so a one-shot log line means a node wedged
# early in a multi-hour rollout goes unnoticed for the rest of it. A report is due
# once this much has passed since the last one, and lands on the first retry after
# that, so the gap is at least this and short of it plus one retry gap (longer if
# ticks are skipped). A refusal that moves to the other half of the guard is reported
# at once. Frequent enough to notice, sparse enough not to bury the event stream.
BLOCKED_REPORT_INTERVAL_SEC = 900.0

# How long a node the `:terminate` guard refused waits before it's POSTed again.
# The refusal is the guard's own answer, so re-asking every poll interval only
# repeats it; asking again after 15 minutes would hold a wave that long after the
# blocker clears. The node is asked again on the first poll at least this long after
# the refusal, which keeps the delay small against a multi-hour rollout while taking
# the refused node out of every tick's POSTs.
TERMINATE_BLOCKED_RETRY_SEC = 120.0


class AdminApi:
    def __init__(self, base_url: str, token: str, dry_run: bool, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
        )
        self.timeout = timeout
        self.dry_run = dry_run

    def list_nodes(self) -> list[AdminNode]:
        resp = self.session.get(
            f"{self.base_url}{ADMIN_API_PREFIX}/nodes", timeout=self.timeout
        )
        resp.raise_for_status()
        rows = resp.json()
        return [parse_admin_node(r) for r in rows]

    def get_node(self, node_id: str) -> NodeDrainRead | None:
        """Read one node's drain state. `None` when the admin row is gone (404).

        Asks for `include_occupancy=false`: the caller needs the drain columns
        and nothing else, and the occupancy and drainability counts cost a
        statement per candidate whose failure or timeout would otherwise skip
        a drain the endpoint would have accepted. The counts come back `null`
        on that response, which is why this returns a `NodeDrainRead` rather
        than an `AdminNode`.
        """
        resp = self.session.get(
            f"{self.base_url}{ADMIN_API_PREFIX}/nodes/{node_id}",
            params={"include_occupancy": "false"},
            timeout=self.timeout,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        row = resp.json()
        if not isinstance(row, dict) or "drain_phase" not in row:
            # `AdminNodeResponse.drain_phase` is an `Option<String>` with no
            # `skip_serializing_if`, so this endpoint always emits the key —
            # `"drain_phase": null` for an undrained node. Absence therefore
            # means the body did not come from this route, and `row.get` would
            # read it as NULL: the exact value that tells the caller's guard the
            # node is undrained and safe to drain. Refuse instead.
            raise MalformedAdminResponse(
                f"admin node response for {node_id} has no drain_phase field"
            )
        return NodeDrainRead(
            drain_phase=row.get("drain_phase"),
            drain_mode=row.get("drain_mode"),
            terminate_decrement_capacity=bool(
                row.get("terminate_decrement_capacity", False)
            ),
        )

    def drain(self, node_id: str, replace_after_terminate: bool) -> bool:
        url = f"{self.base_url}{ADMIN_API_PREFIX}/nodes/{node_id}:drain"
        # `mode` is explicit because the API default is `gentle` — cordon and
        # wait for residents to leave on their own. A blue/green MNG rollout
        # replaces the node, so waiting on a long-lived sandbox would stall the
        # wave indefinitely; the rollout accepts the per-node evacuation
        # disruption in exchange for bounded progress.
        #
        # `require_undrained` makes the POST start-only: the endpoint refuses
        # it with a 409 if the node already has any drain phase, checked under
        # the same row lock as the write. That is what keeps this request from
        # ever escalating a drain somebody else started.
        body = {
            "replace_after_terminate": replace_after_terminate,
            "mode": "forced",
            "require_undrained": True,
        }

        # Re-read the phase immediately before posting, and skip a node whose
        # drain already exists.
        #
        # `pick_drain_candidates` filters on the tick's `list_nodes()` snapshot,
        # so a drain someone else started inside the snapshot window still looks
        # like `DrainPhase.NONE` here. The admin `:drain` endpoint accepts a
        # forced request against a gentle-draining node and escalates it in
        # place, so posting on that stale read could escalate a human's gentle
        # drain and arm the stale-transient sweep, with no operator in the loop.
        #
        # Safety doesn't rest on this read: `require_undrained` in the body
        # makes the endpoint refuse the POST under its row lock, which also
        # covers a drain that lands between this read and the POST. The read
        # is here so the common case is skipped without a write attempt, and so
        # the skip event can report the drain it found.
        try:
            current = self.get_node(node_id)
        except MalformedAdminResponse as err:
            emit_event(
                Event.DRAIN_SKIPPED_PHASE_UNREADABLE,
                node_id=node_id,
                reason=str(err),
            )
            return False
        if current is None:
            # Fail closed. A 404 here means either the admin row vanished — in
            # which case the POST would 404 too — or the GET did not reach a
            # node route at all, e.g. a wrong path prefix. The second is the
            # dangerous reading: it would make this guard silently inert for
            # every candidate, forever, with the POSTs proceeding exactly as
            # they did before it existed. A guard that cannot read the state it
            # guards has to refuse, not wave the request through.
            emit_event(
                Event.DRAIN_SKIPPED_PHASE_UNREADABLE,
                node_id=node_id,
                reason="admin row not found",
            )
            return False
        if current.drain_phase is not None:
            emit_event(
                Event.DRAIN_SKIPPED_ALREADY_DRAINING,
                node_id=node_id,
                drain_phase=current.drain_phase,
                # Both the raw column and its resolution, because for this
                # field they carry different information and the operator
                # needs both. `AdminNodeResponse.drain_mode` is
                # `skip_serializing_if = "Option::is_none"`, so a node
                # draining with `drain_mode IS NULL` omits the key entirely
                # and the raw value here is `None` — and NULL resolves to
                # *gentle*, which is the destructive case this refusal exists
                # to name. Reporting only the raw value would print `null` for
                # exactly the drain worth paging on, while an explicit
                # `"gentle"` printed normally.
                drain_mode=current.drain_mode,
                drain_mode_resolved=resolved_drain_mode(current.drain_mode),
                terminate_decrement_capacity=current.terminate_decrement_capacity,
            )
            return False

        if self.dry_run:
            emit_event(
                Event.DRY_RUN_DRAIN,
                node_id=node_id,
                replace_after_terminate=replace_after_terminate,
            )
            return True
        resp = self.session.post(url, json=body, timeout=self.timeout)
        if (
            resp.status_code == 409
            and self._response_field(resp, "current_drain_phase") is not None
        ):
            # The node gained a drain phase between the read above and this
            # POST, and `require_undrained` made the endpoint refuse it. Only a
            # 409 naming the phase means that: the admin API also answers 409
            # for a retryable serialization or deadlock abort, which started no
            # drain and falls through to `raise_for_status` below.
            emit_event(
                Event.DRAIN_SKIPPED_ALREADY_DRAINING,
                node_id=node_id,
                drain_phase=self._response_field(resp, "current_drain_phase"),
                refused_by="endpoint",
            )
            return False
        if resp.status_code == 502 and self._drain_committed_despite_502(resp):
            # The endpoint commits the Postgres transition and *then* patches
            # the substrate, deliberately, so that a failed patch is
            # recoverable rather than leaving the scheduler and the DB
            # disagreeing. A 502 from this handler therefore means the drain
            # started — the row is `draining`, which is what `db_state` above
            # is checked for.
            #
            # Whether the *cordon* is then repaired is a separate and
            # conditional question, and the endpoint answers it in the same
            # body: `reconciler_will_retry` is true only for a
            # `managed_by = 'k8s'` node in `draining` or `terminating`. It holds
            # for every candidate here, but not because they came out of
            # `describe-auto-scaling-groups`: `nodes.managed_by` defaults to
            # `'space'`, and exactly two paths write `'k8s'` — the Kubernetes
            # substrate's inventory upsert and the node daemon's registration
            # bootstrap. Both are upserts whose `ON CONFLICT DO UPDATE` arm does
            # set `managed_by`, so what keeps an existing `'space'` row from
            # being adopted is the `WHERE nodes.managed_by = 'k8s'` predicate on
            # each of those arms — not the shape of the statement. The daemon
            # path surfaces the refusal as a `failed_precondition`. It holds
            # because these instances join the kube cluster and are registered
            # by one of those two, whichever runs first. `AdminNode` carries no
            # `managed_by`, so this process cannot check it — which is why the
            # value is carried into the event rather than branched on.
            #
            # Treating it as a failure was wrong in a way that only showed up
            # in telemetry: the node would not be booked into `drain_issued`,
            # no `drain_started` would be emitted, a `tick_error` would be
            # reported for a request that had in fact succeeded, and a wave of
            # one such node would report `drain_wave_made_no_progress` while
            # having made exactly the progress it was asked for. The
            # concurrency cap self-corrects (`pick_drain_candidates` recounts
            # `in_progress` from the next tick's snapshot, which now sees the
            # node as `draining`), so the run stayed correct while its own
            # record of what it did was wrong.
            emit_event(
                Event.DRAIN_COMMITTED_SUBSTRATE_PATCH_FAILED,
                node_id=node_id,
                error=self._response_field(resp, "error"),
                reconciler_will_retry=self._response_field(
                    resp, "reconciler_will_retry"
                ),
            )
            return True
        resp.raise_for_status()
        return True

    @staticmethod
    def _response_field(resp: Any, key: str) -> Any:
        """One field out of a JSON body, or `None` if the body is not JSON."""
        try:
            payload = resp.json()
        except Exception:  # noqa: BLE001 - a non-JSON body is the case handled
            return None
        return payload.get(key) if isinstance(payload, dict) else None

    def _drain_committed_despite_502(self, resp: Any) -> bool:
        """Did this 502 come from the admin handler *after* its commit?

        Keyed on the body rather than the status, because the two 502s that
        reach this client mean opposite things. The handler's own carries
        `db_state` — the re-read row — and a non-NULL `drain_phase` in it is
        proof the transition committed. A 502 from anything in front of the
        api-server (load balancer, ingress, service mesh) carries no such body,
        and for that one the drain did **not** start.

        Getting this backwards in the permissive direction is the worse error:
        booking a node whose drain never committed removes it from candidacy
        for the rest of the run, so the rollout would skip a node it never
        drained. So the default is to raise, and only positive evidence of a
        committed transition opts out.
        """
        db_state = self._response_field(resp, "db_state")
        if not isinstance(db_state, dict):
            return False
        return db_state.get("drain_phase") is not None

    def terminate(self, node_id: str) -> dict[str, Any] | None:
        """POST `:terminate`. Returns `None` on success, or the 409 body when the
        endpoint refuses because the node still holds sandboxes.

        The refusal is a return value rather than an exception because it is an
        expected per-node outcome, not a wave-level failure: the endpoint is the
        authoritative occupancy check (it is census-aware, and the admin listing
        this script polls deliberately is not), so the only correct way to learn
        that a node is blocked is to ask it. Raising would abort the rest of the
        terminate wave through the caller's broad-except. Every other status still
        raises."""

        url = f"{self.base_url}{ADMIN_API_PREFIX}/nodes/{node_id}:terminate"
        if self.dry_run:
            emit_event(Event.DRY_RUN_TERMINATE, node_id=node_id)
            return None
        resp = self.session.post(url, timeout=self.timeout)
        if resp.status_code == 409:
            try:
                return resp.json()
            except ValueError:
                return {}
        resp.raise_for_status()
        return None


def parse_admin_node(row: dict[str, Any]) -> AdminNode:
    return AdminNode(
        id=row["id"],
        k8s_node_name=row.get("k8s_node_name"),
        address=row.get("address"),
        drain_phase=row.get("drain_phase"),
        drain_mode=row.get("drain_mode"),
        drain_started_at=parse_rfc3339(row.get("drain_started_at")),
        state=row.get("state"),
        sandboxes_active=int(row.get("sandboxes_active", 0)),
        sandboxes_local_snapshots=int(row.get("sandboxes_local_snapshots", 0)),
        terminate_decrement_capacity=bool(row.get("terminate_decrement_capacity", False)),
    )


def parse_rfc3339(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    # `datetime.fromisoformat` accepts the trailing `Z` suffix natively on
    # Python 3.11+ (declared in this script's PEP 723 metadata), so no manual
    # offset substitution is needed.
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


# --- K8s readiness ----------------------------------------------------------


def _kubectl_context_args(context: str) -> list[str]:
    """Return `["--context", context]` if a context is set, else an empty list.

    Empty context means "use kubectl's default config", which is how the script
    runs inside a Kubernetes Job: there is no kubeconfig file, only the
    in-cluster ServiceAccount + KUBERNETES_SERVICE_HOST env vars that kubectl
    picks up automatically. Passing `--context ""` (or `--context in-cluster`)
    would fail with `error: context "..." does not exist`.
    """

    return ["--context", context] if context else []


def kubectl_ready_node_names(context: str) -> set[str]:
    """Return the set of K8s node names with Ready=True.

    Shells out to kubectl rather than pulling in the K8s python client. The
    operator already needs kubectl access for the rollout.
    """

    cmd = ["kubectl", "get", "nodes", "-o", "json"] + _kubectl_context_args(context)
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        timeout=KUBECTL_TIMEOUT_SEC,
    )
    blob = json.loads(proc.stdout)
    ready: set[str] = set()
    for item in blob.get("items", []):
        name = item.get("metadata", {}).get("name")
        if not name:
            continue
        for cond in item.get("status", {}).get("conditions", []):
            if cond.get("type") == "Ready" and cond.get("status") == "True":
                ready.add(name)
                break
    return ready


def kubectl_target_nodes_by_taint(
    context: str, target_mng: str, taint_key: str
) -> tuple[set[str], set[str], set[str]]:
    """Return `(ready_tainted, ready_untainted, not_ready_untainted)` target
    nodes from one kubectl call.

    `taint_key` is the cohort taint key declared by the `rollout-mng`
    Terraform module on the target MNG — operators override the TF default
    by passing `--cohort-taint-key` to the script.

    Three filters serve three callers:

    * `ready_tainted` — candidate pool for the per-tick untaint loop. Only
      Ready nodes are eligible (the kubectl taint command on a NotReady
      node would race kubelet recovery, and we'd rather wait).
    * `ready_untainted` — resume signal: a prior tick already untainted
      this node, which only happens after a source termination. Used as
      the "rollout has made progress past prewarm" hard-fail check.
    * `not_ready_untainted` — included in the resume seed for
      `untainted_target_nodes` so the budget accounting counts these
      against future untaints. Without this, a NotReady-during-restart
      untainted node would be missed by the seed, the budget would
      over-grant on the next tick, and the H reserved tainted slots could
      shrink below the headroom invariant once the node returns to Ready.
    """

    cmd = [
        "kubectl", "get", "nodes",
        "-l", f"eks.amazonaws.com/nodegroup={target_mng}",
        "-o", "json",
    ] + _kubectl_context_args(context)
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        timeout=KUBECTL_TIMEOUT_SEC,
    )
    blob = json.loads(proc.stdout)
    ready_tainted: set[str] = set()
    ready_untainted: set[str] = set()
    not_ready_untainted: set[str] = set()
    for item in blob.get("items", []):
        name = item.get("metadata", {}).get("name")
        if not name:
            continue
        is_ready = any(
            cond.get("type") == "Ready" and cond.get("status") == "True"
            for cond in item.get("status", {}).get("conditions", [])
        )
        taints = item.get("spec", {}).get("taints") or []
        has_cohort = any(t.get("key") == taint_key for t in taints)
        if has_cohort:
            if is_ready:
                ready_tainted.add(name)
            # NotReady+tainted: skip. The per-tick untaint loop wants
            # Ready+tainted only; the seed cares about untainted only.
        else:
            if is_ready:
                ready_untainted.add(name)
            else:
                not_ready_untainted.add(name)
    return ready_tainted, ready_untainted, not_ready_untainted


def kubectl_remove_cohort_taint(
    context: str, node_name: str, taint_key: str
) -> None:
    """Remove the cohort taint from a single target node.

    `kubectl taint nodes <node> <key>-` is idempotent — running it against
    an already-untainted node exits non-zero with "taint ... not found",
    which we swallow because the post-condition (taint absent) is the same
    either way. The check requires BOTH "taint" and "not found" so that
    other "not found" errors — most importantly `Error from server
    (NotFound): nodes "<name>" not found` when the Node object itself has
    been GC'd between listing and untaint — are surfaced rather than
    silently treated as success.
    """

    cmd = [
        "kubectl", "taint", "nodes", node_name,
        f"{taint_key}-",
    ] + _kubectl_context_args(context)
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=KUBECTL_TIMEOUT_SEC,
    )
    stderr_lower = proc.stderr.lower()
    if proc.returncode != 0 and not (
        "taint" in stderr_lower and "not found" in stderr_lower
    ):
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr,
        )


def kubectl_pending_evacuation_pod_count(
    context: str, cohort_taint_key: str, cohort_value: str
) -> int:
    """Count Pending sandbox pods carrying a rollout-cohort toleration that
    applies to this rollout's cohort.

    Source for the RFC-specified `node_drain.evacuation_pods_pending`
    gauge. The Linear issue suggests filtering
    `kubectl get pods -l app.kubernetes.io/name=space-sandbox
    --field-selector status.phase=Pending` to the target MNG, but a Pending
    pod has no `nodeName` yet — that's why it's Pending — so per-node
    filtering is impossible at the kube-apiserver. Instead we look at the
    pod spec: only evacuation pods carry a rollout-cohort toleration
    (cluster-controller's `drain_evacuation_tolerations`, RFC §Steering
    Mechanism). User-create / restore / pooled pods never get one.

    A toleration matches this rollout when its `key` is `cohort_taint_key`
    **and** either (a) the toleration's `value` is empty / absent (the
    `Exists`-form default the cluster-controller ships, which by definition
    applies to every cohort), or (b) the `value` equals this rollout's
    `cohort_value` (the `Equal`-form an operator might narrow to). The
    value-narrowing arm is the per-cohort discriminator that prevents
    silent over-counting when two rollouts run concurrently — e.g. cohorts
    A=1 and B=2 in flight at the same time would otherwise produce the
    same union count tagged with two different `cohort_to` values.

    Caveat under the default `Exists` form: when both cohorts' evacuation
    pods carry `operator: Exists` tolerations, neither pod carries a
    cohort discriminator in its spec — both scripts will see the same
    union count. This is acceptable because concurrent two-MNG rollouts
    are unsupported: source-MNG accounting assumes a single in-flight rollout.
    If concurrent rollouts
    become a requirement, narrow the cluster-controller's Helm value
    from `operator: Exists` to per-cohort `operator: Equal, value: <n>`
    so this filter can discriminate.

    Cluster-wide query (`-A`) because evacuation pods land in whatever
    namespace the user sandbox runs in, not just one.
    """

    cmd = [
        "kubectl", "get", "pods",
        "-A",
        "-l", SANDBOX_POD_LABEL_SELECTOR,
        "--field-selector", "status.phase=Pending",
        "-o", "json",
    ] + _kubectl_context_args(context)
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
        timeout=KUBECTL_TIMEOUT_SEC,
    )
    blob = json.loads(proc.stdout)
    count = 0
    for item in blob.get("items", []):
        tolerations = item.get("spec", {}).get("tolerations") or []
        for t in tolerations:
            if t.get("key") != cohort_taint_key:
                continue
            tol_value = t.get("value")
            # Exists-form: `value` is absent or empty. Equal-form: `value`
            # equals this rollout's cohort. Anything else (Equal-form for
            # a different cohort) is excluded.
            if not tol_value or tol_value == cohort_value:
                count += 1
                break
    return count


# --- Correlation + classification -------------------------------------------


def correlate_nodes(
    asg_instances: list[AsgInstance],
    ec2_by_id: dict[str, Ec2Instance],
    admin_nodes: list[AdminNode],
    ready_node_names: set[str],
) -> list[NodeStatus]:
    """Build per-instance NodeStatus by joining ASG, EC2, and admin views.

    The admin API does not expose nodegroup membership. We anchor on the ASG
    member list (the AWS source of truth for MNG membership) and look up each
    instance's admin row by `k8s_node_name == private_dns`.
    """

    admin_by_k8s: dict[str, AdminNode] = {}
    for a in admin_nodes:
        if a.k8s_node_name:
            admin_by_k8s[a.k8s_node_name] = a
    out: list[NodeStatus] = []
    for inst in asg_instances:
        ec2 = ec2_by_id.get(inst.instance_id)
        dns = ec2.private_dns if ec2 else None
        admin = admin_by_k8s.get(dns) if dns else None
        out.append(
            NodeStatus(
                instance_id=inst.instance_id,
                asg_name=inst.asg_name,
                private_dns=dns,
                lifecycle_state=inst.lifecycle_state,
                admin=admin,
                k8s_ready=bool(dns and dns in ready_node_names),
                availability_zone=inst.availability_zone,
            )
        )
    return out


def pick_drain_candidates(
    source: list[NodeStatus],
    max_concurrent_node_drains: int,
    stuck_node_ids: frozenset[str] = frozenset(),
    already_drained_ids: frozenset[str] = frozenset(),
) -> list[NodeStatus]:
    """Return source nodes to issue `:drain` against on this tick.

    Honors the cluster-capacity invariant: at most
    `max_concurrent_node_drains` source nodes should be in flight at once
    (`drain_phase IN {draining, drained, terminating}`). Drained and
    terminating nodes still occupy target capacity until the ASG decrement
    lands (because their migrated sandboxes are running on target nodes,
    and source `desired` only drops on terminate completion); counting
    them toward the cap keeps `1 <= k <= H` honored across the
    drain → drained → terminate → ASG decrement → target grow cycle.

    Stuck nodes (already flagged via the `--stuck-threshold` guard) do not
    consume a drain slot — without this exclusion a single stuck node would
    deadlock the rest of the wave at concurrency caps of 1.

    `already_drained_ids` lists node IDs the orchestrator has POSTed `:drain`
    to in this process, so a stale `list_nodes()` read between drain commit
    and the next tick does not re-POST.

    The admin `:drain` endpoint accepts a forced request against a
    gentle-draining node and escalates it in place, so a POST reaching an
    inherited drain would be destructive: evacuation plus the stale-transient
    sweep. This orchestrator never sends one. Every POST carries
    `require_undrained: true`, which the endpoint checks under its row lock,
    so a node that has any drain phase by the time the POST is served is a 409
    and nothing is written.

    `pick_drain_candidates` only ever offers `DrainPhase.NONE` nodes, so a node
    already draining is excluded on any tick that sees it as such. Two windows
    get past that, and both end at the endpoint's refusal:

    * **Our own stale read.** A `list_nodes()` snapshot still showing `NONE` for
      a node whose `:drain` has already committed. In-process
      `already_drained_ids` closes it, and a restart empties that set — so after
      one, the first tick *offers* a node this process drained moments earlier.
      The phase re-read in `drain()` sees `draining` and skips it with
      `drain_skipped_already_draining`.
    * **Somebody else's drain, committed inside our snapshot window.** An
      operator running `space-admin-cli drain-node <node>` by hand during a wave. The
      phase re-read in `drain()` skips it if the drain is visible by then;
      if it commits between that read and the POST, the endpoint refuses the
      POST and `drain()` emits the same event with `refused_by: endpoint`.

    This *filter* closes neither, because the set it tracks is only what this
    process drained.

    Picks are distributed proportionally across AZs so the source ASG's
    per-AZ count shrinks at roughly the same rate. A purely `instance_id`-
    sorted picker is AZ-blind and tends to drain a contiguous run from one
    AZ, which provokes ASG's AZRebalance process to launch replacement
    instances in the under-populated AZ (the source MNG no longer shrinks
    monotonically). The greedy "pick from the largest remaining bucket"
    rule keeps the candidate set as flat as possible across AZs; ties
    between buckets break by AZ name (lex), and within a bucket by
    `instance_id`, so pick order is reproducible across reruns (boto3
    paginator response order is not).
    """

    # Count any source node whose drain wave has started but whose ASG
    # decrement hasn't completed yet. That covers three propagation
    # windows (same set as `source_pending_decrements` in `run_loop`):
    #   - DRAINING / DRAINED / TERMINATING phases (PHASES_IN_FLIGHT)
    #   - NONE phase with a pending POST :drain (admin-API read lag
    #     between commit and next tick)
    #   - GONE phase (admin row deleted) but ASG instance still in
    #     `Terminating*` lifecycle (ASG decrement hasn't landed yet
    #     in `describe_nodegroup.desiredSize`)
    # Each in-flight wave is holding one of the `H` target slots; without
    # counting all three, the picker would start a new wave before the
    # prior wave's target slot was freed by the next-tick
    # `compute_target_desired` grow.
    # The stuck-node exclusion is scoped to `DRAINING` only. A node flagged
    # stuck while in `draining` is excluded from the in-progress count so the
    # rest of the wave isn't deadlocked at `k=1`. But if that node later
    # progresses to `drained` or `terminating`, its evacuated sandboxes are
    # still occupying target capacity — counting it normally from that point
    # on keeps `1 <= k <= H` honored. Without this scope, a recovered stuck
    # node would stay excluded permanently (its ID is sticky in
    # `stuck_logged`) and the next tick could start an extra drain wave,
    # overrunning `H`.
    in_progress = sum(
        1
        for n in source
        if (
            n.rollout_phase in PHASES_IN_FLIGHT
            or (
                n.rollout_phase == DrainPhase.NONE
                and n.admin is not None
                and n.admin.id in already_drained_ids
            )
            or (
                n.rollout_phase == DrainPhase.GONE
                and n.lifecycle_state in ASG_TERMINATING_STATES
            )
        )
        and not (
            n.admin is not None
            and n.admin.id in stuck_node_ids
            and n.rollout_phase == DrainPhase.DRAINING
        )
    )
    slots = max(0, max_concurrent_node_drains - in_progress)
    if slots == 0:
        return []
    candidates = [
        n
        for n in source
        if n.rollout_phase == DrainPhase.NONE
        and n.admin is not None
        and n.admin.id not in already_drained_ids
    ]
    return _distribute_picks_across_azs(candidates, slots)


def _distribute_picks_across_azs(
    candidates: list[NodeStatus], slots: int
) -> list[NodeStatus]:
    """Greedy AZ-balanced pick: each round, take one from the largest bucket.

    Buckets candidates by `availability_zone`; pre-sorts each bucket by
    `instance_id` so picks within a bucket are reproducible. On each of
    `slots` rounds, picks the bucket with the most remaining candidates
    (ties broken by AZ name, lex), and pops its smallest instance_id.
    This keeps the candidate set as flat as possible across AZs as the
    rollout drains. With perfectly balanced input (e.g. 4-4-4 across 3 AZs)
    and `slots * waves == 6`, end-state is 2-2-2 — matching the source ASG
    balance AZRebalance would otherwise try to enforce.
    """

    by_az: dict[str, list[NodeStatus]] = {}
    for n in candidates:
        by_az.setdefault(n.availability_zone, []).append(n)
    # Sort each bucket so the smallest instance_id is at the *end*: pop()
    # then returns it in O(1).
    for nodes in by_az.values():
        nodes.sort(key=lambda n: n.instance_id, reverse=True)

    picks: list[NodeStatus] = []
    for _ in range(slots):
        non_empty = [(az, nodes) for az, nodes in by_az.items() if nodes]
        if not non_empty:
            break
        # Largest bucket wins; tie-break by AZ name (lex). Sorting by
        # (-len, az) gives the right order in one pass.
        non_empty.sort(key=lambda item: (-len(item[1]), item[0]))
        picks.append(non_empty[0][1].pop())
    return picks


def pick_terminate_candidates(
    source: list[NodeStatus],
    already_terminated_ids: frozenset[str] = frozenset(),
) -> list[NodeStatus]:
    """Source nodes in `drained` that need a `:terminate` POST.

    `SPACE_DRAINED_NODE_TERMINATOR_ENABLED` defaults to `false` **in code**, so
    this script drives the transition rather than assuming something else will.
    Note the EKS chart sets it `true`, so on a deployed cluster the
    `drained_node_terminator` is also advancing `drained -> terminating` on its
    own 60s tick — which is why the terminate loop has to tolerate a node that
    left `drained` between this tick's poll and its POST. Sorted by `instance_id`
    for deterministic order across reruns.

    **Neither listing count is filtered on here**; the endpoint decides, and
    `run_terminate_pass` handles its refusal as a paced skip. The listing's
    `sandboxes_local_snapshots` disagrees with the guard both ways. It misses
    a `suspending` or non-evacuation `restoring` row pinned to the node, which
    the guard refuses on. And its in-flight arm still uses the legacy freshness
    gate (`legacy_upload` in `admin/node_store/single_node_occupancy.rs`, and
    the note in `admin/node_store.rs::batched_occupancy_rows`), so it counts a
    `creating` row naming a non-current artifact, on a source node that isn't
    the sandbox's target, that the guard doesn't count. Screening on it would
    hold a node with only that row out of the wave on every tick without ever
    asking.

    The same goes for `sandboxes_active`, which the endpoint also refuses on.
    The listing's `active` count is intentionally *not* census-aware while the
    endpoint's is (see
    `admin/node_store.rs::occupancy_for_nodes` for why the multi-node scan can't
    afford the discount). Gating on the listing's number would therefore exclude
    nodes the endpoint would happily terminate — a worker whose own census
    disproves thousands of phantom `active_node_id` rows would sit in `drained`
    forever without this script ever asking. So the endpoint decides: `terminate`
    returns the 409 body instead of raising, and the caller reports that node and
    moves on.

    Nodes whose drain was started without `replace_after_terminate=false`
    (i.e. `terminate_decrement_capacity == False`) are also excluded: the
    cluster-controller would call ASG terminate without
    `ShouldDecrementDesiredCapacity`, so the source MNG would auto-replace
    the node and the rollout's monotonic shrink would never complete. This
    catches pre-existing drained rows from manual `:drain` calls and any
    rollout-era drain that ran on a build before the
    `replace_after_terminate` flag landed.
    """

    return sorted(
        (
            n
            for n in source
            if n.rollout_phase == DrainPhase.DRAINED
            and n.admin is not None
            and n.admin.id not in already_terminated_ids
            and n.admin.terminate_decrement_capacity
        ),
        key=lambda n: n.instance_id,
    )


@dataclasses.dataclass
class TerminateState:
    """What the terminate wave remembers between ticks."""

    # Nodes whose `:terminate` POST was accepted; never re-POSTed.
    issued: set[str] = dataclasses.field(default_factory=set)
    # Node id -> monotonic time before which a refused node isn't re-POSTed.
    retry_after: dict[str, float] = dataclasses.field(default_factory=dict)
    # Node id -> the event its refusal was last reported under, and when.
    reported: dict[str, tuple[Event, float]] = dataclasses.field(default_factory=dict)


def run_terminate_pass(
    api: Any,
    candidates: list[NodeStatus],
    *,
    cohort: str,
    state: TerminateState,
    now: float,
) -> None:
    """POST `:terminate` for each candidate, and act on what the guard answers.

    The endpoint is the authoritative occupancy check: it decides on the same
    drain-gate predicate the node-drain reconciler holds a node on, which the
    listing's counts don't reproduce. So a refusal is only knowable by asking,
    and the wave keeps going past a refused node.

    An occupancy refusal is a paced skip. The node isn't POSTed again for
    `TERMINATE_BLOCKED_RETRY_SEC`, and it's reported under the event that names
    which half of the guard refused: again once `BLOCKED_REPORT_INTERVAL_SEC`
    has passed, or at once when the refusal moves to the other half. It isn't
    added to `issued`, so it's asked again once the retry interval passes, and it
    self-clears when the blocker does.

    Three different 409s come back from this endpoint, so the refusal is
    classified on which key is *present* rather than on which is absent:

    * `sandboxes_active` -> the occupancy guard refused;
    * `current_drain_phase` -> the node left `drained` between this tick's
      listing poll and this POST (the cluster's own `drained_node_terminator`
      flips it on a 60s tick, or an operator undrained), and the transition check
      fires before occupancy is even read;
    * neither -> `db_error` mapped a Postgres serialization failure or deadlock
      (40001 / 40P01) to a 409 whose body is just `{"error": ...}`. Transient and
      retryable, and nothing to do with sandboxes.
    """

    for ns in candidates:
        assert ns.admin is not None
        node_id = ns.admin.id
        if state.retry_after.get(node_id, float("-inf")) > now:
            continue
        refusal = api.terminate(node_id)
        if refusal is None:
            state.issued.add(node_id)
            state.retry_after.pop(node_id, None)
            emit_event(
                Event.TERMINATE_STARTED,
                cohort=cohort,
                node_id=node_id,
                k8s_node_name=ns.admin.k8s_node_name,
                instance_id=ns.instance_id,
            )
            continue
        if "current_drain_phase" in refusal:
            emit_event(
                Event.TERMINATE_BLOCKED_PHASE_DRIFT,
                cohort=cohort,
                node_id=node_id,
                k8s_node_name=ns.admin.k8s_node_name,
                instance_id=ns.instance_id,
                current_drain_phase=refusal.get("current_drain_phase"),
            )
            continue
        if "sandboxes_active" not in refusal:
            emit_event(
                Event.TERMINATE_REFUSED_UNKNOWN,
                cohort=cohort,
                node_id=node_id,
                k8s_node_name=ns.admin.k8s_node_name,
                instance_id=ns.instance_id,
                error=refusal.get("error"),
            )
            continue
        state.retry_after[node_id] = now + TERMINATE_BLOCKED_RETRY_SEC
        # The guard's own count. An endpoint from before the field existed sent
        # only the alias, counted by its older guard: non-durable `suspended`
        # rows and uploads in flight from the node, without the `suspending`
        # and non-evacuation `restoring` arms. It's the closest count that
        # endpoint has.
        drain_blockers = refusal.get(
            "sandboxes_drain_blockers", refusal.get("sandboxes_local_snapshots")
        )
        if refusal.get("sandboxes_active"):
            event = Event.TERMINATE_BLOCKED_ACTIVE_SANDBOXES
            fields = {
                "sandboxes_active": refusal.get("sandboxes_active"),
                "sandboxes_drain_blockers": drain_blockers,
            }
        else:
            event = Event.TERMINATE_BLOCKED_DRAIN_GATE
            fields = {"sandboxes_drain_blockers": drain_blockers}
        # Re-reported on a cadence rather than once: `detect_stuck` can't see a
        # `drained` node (that pass skips anything not in `DRAINING`), so one
        # line at the start of a multi-hour rollout, then silence, is how a
        # wedged node goes unnoticed. A refusal that moves to the other half of
        # the guard is reported at once, so the latest line always names the
        # half now refusing.
        last = state.reported.get(node_id)
        if (
            last is not None
            and last[0] == event
            and now - last[1] < BLOCKED_REPORT_INTERVAL_SEC
        ):
            continue
        state.reported[node_id] = (event, now)
        emit_event(
            event,
            cohort=cohort,
            node_id=node_id,
            k8s_node_name=ns.admin.k8s_node_name,
            instance_id=ns.instance_id,
            **fields,
        )


def pick_terminate_blocked_by_replace_semantics(
    source: list[NodeStatus],
) -> list[NodeStatus]:
    """Drained source nodes whose admin row has
    `terminate_decrement_capacity == False`.

    Terminating them would call ASG without decrement, the source MNG
    would auto-replace, and the rollout's monotonic shrink invariant
    would break. Surface so the operator can re-drain them with
    `replace_after_terminate=false` via the admin CLI (or directly).

    Whatever the listing's local-snapshot count says: these nodes are never
    POSTed, so no refusal names their blocker, and the listing's count can
    name one the `:terminate` guard doesn't hold the node for.
    """

    return [
        n
        for n in source
        if n.rollout_phase == DrainPhase.DRAINED
        and n.admin is not None
        and not n.admin.terminate_decrement_capacity
    ]


def pick_terminate_blocked_by_snapshots(source: list[NodeStatus]) -> list[NodeStatus]:
    """Drained source nodes the listing shows holding local-disk snapshots.

    A hint naming the likely blocker, so the operator reaches for the
    local-snapshot remedies. It doesn't hold the node back: the listing's count
    doesn't affect whether the node is a terminate candidate, and the guard's
    refusal is what's paced and re-reported (`run_terminate_pass`). A node with
    replace-after-terminate semantics isn't a candidate either way, and
    `pick_terminate_blocked_by_replace_semantics` names it for that.
    """

    return [
        n
        for n in source
        if n.rollout_phase == DrainPhase.DRAINED
        and n.admin is not None
        and n.admin.sandboxes_local_snapshots > 0
    ]


def report_local_snapshot_hints(
    source: list[NodeStatus],
    *,
    cohort: str,
    logged: set[str],
    terminated: set[str],
) -> None:
    """Name local-disk snapshots as the likely blocker, once per node.

    The cadence belongs to the guard's refusal, which `run_terminate_pass`
    re-reports; this line only points the operator at the local-snapshot
    remedies (durable S3 suspend or manual cleanup). A node already terminated
    isn't blocked, so it isn't named, including one the listing still shows in
    `drained` on the tick its POST was accepted.
    """

    for ns in pick_terminate_blocked_by_snapshots(source):
        assert ns.admin is not None
        if ns.admin.id in logged or ns.admin.id in terminated:
            continue
        logged.add(ns.admin.id)
        emit_event(
            Event.TERMINATE_BLOCKED_LOCAL_SNAPSHOTS,
            cohort=cohort,
            node_id=ns.admin.id,
            k8s_node_name=ns.admin.k8s_node_name,
            instance_id=ns.instance_id,
            sandboxes_local_snapshots=ns.admin.sandboxes_local_snapshots,
        )


def source_remaining_live(source: list[NodeStatus]) -> int:
    """Count source instances still consuming MNG capacity (any live ASG state)."""

    return sum(1 for n in source if n.lifecycle_state in ASG_LIVE_STATES)


def source_active_to_migrate(source: list[NodeStatus]) -> int:
    """Count source instances whose sandboxes have not yet migrated.

    `none` (never drained) and `draining` (migration in progress) consume
    target capacity; `drained`/`terminating`/`gone` do not. Pending* ASG
    states don't count yet either — those nodes haven't joined and aren't
    running sandboxes, so growing the target for them would over-shoot.
    """

    return sum(
        1
        for n in source
        if n.lifecycle_state in ASG_REGISTERED_STATES
        and n.rollout_phase in (DrainPhase.NONE, DrainPhase.DRAINING)
    )


def compute_target_desired(
    target: MngState,
    initial_source_desired: int,
    source_current_desired: int,
    headroom: int,
) -> int:
    """Monotonically non-decreasing target desiredSize.

    Implements the RFC's "total cluster capacity stays at N + H
    throughout" invariant: target grows in lockstep with source's
    shrinkage. With `N = initial_source_desired` and `H = headroom`,

        target_desired = headroom + terminated_count
                       = headroom + (N - source_current_desired)

    so `source_current_desired + target_desired = N + H` is constant.
    At tick 0 (no terminates yet) target = H. After each source
    terminate-with-decrement, target grows by 1. End state: source = 0,
    target = N + H.

    Returns `max(target.desired_size, want)` so the field is
    monotonically non-decreasing — operators who pre-warmed manually
    aren't shrunk back. Negative deltas (e.g. an operator manually
    scaled source up mid-rollout) are clamped at 0.
    """

    terminated = max(0, initial_source_desired - source_current_desired)
    want = headroom + terminated
    return max(target.desired_size, want)


def target_ready_count(target: list[NodeStatus]) -> int:
    """Count target nodes usable as a landing pad for migrated sandboxes.

    A target node must be K8s Ready, registered with SPACE, AND not itself
    draining. A target node carrying any `drain_phase` is cordoned and the
    scheduler will not place migrated pods on it; counting it would
    over-report usable capacity and let the readiness gate open prematurely.
    """

    return sum(
        1
        for n in target
        if n.k8s_ready
        and n.admin is not None
        and n.admin.drain_phase is None
    )


# --- Main loop --------------------------------------------------------------


@dataclasses.dataclass
class Config:
    cluster: str
    source_mng: str
    target_mng: str
    cohort: str
    max_concurrent_node_drains: int
    headroom_count: int
    space_api: str
    aws_region: str
    kubectl_context: str
    stuck_threshold_sec: int
    poll_interval_sec: int
    dry_run: bool
    token_env: str
    # Source MNG's pre-rollout desiredSize, supplied via
    # `--initial-source-desired-size`. Anchors the "total cluster capacity
    # stays at N + H throughout" invariant: target grows by 1 each time
    # source decrements by 1, so
    # `source + target = initial_source_desired_size + headroom_count`
    # is constant at every observation point. Required on every invocation
    # because the script cannot safely auto-derive N from current MNG state:
    # the terminate-then-interrupt window leaves
    # `source.desired = N - terminated` indistinguishable from a fresh
    # `0/H/H` prewarm with a smaller N.
    initial_source_desired_size: int
    # Kubernetes taint key declared by the `rollout-mng` Terraform module on
    # the target MNG. Defaults to the SPACE convention; must match the TF
    # module's `cohort_taint_key` input or the script's untaint loop will
    # find no matching taint and the target pool stays fenced from user
    # workloads.
    cohort_taint_key: str
    # Which dimension of the MNG spec is changing. Carried on telemetry
    # events as the `rollout_kind` tag for the RFC-specified derived gauges
    # (`node_drain.rollout_nodes_remaining`, `node_drain.rollout_capacity_peak`,
    # `node_drain.evacuation_pods_pending`). Closed set per RFC §Observability;
    # see `ROLLOUT_KINDS` for the enum.
    rollout_kind: str
    # When True, ignore `--token-env` and prompt for email + password to
    # mint a fresh session token via `/auth/v1/sessions`. Removes the
    # operator step of remembering to `read -s` / `read "VAR?…"` in the
    # right shell dialect before launching the script.
    prompt_login: bool


def reconcile_mng_xxx(
    eks: Any,
    cluster: str,
    cohort: str,
    role: str,
    mng: MngState,
    dry_run: bool,
    pending_decrements: int = 0,
) -> None:
    """Pull an MNG back toward `min == desired == max` in one EKS call.

    The rollout's steady-state invariant is x/x/x on both MNGs **at
    observation points where no drains are in flight**. When there are
    `K` pending source decrements (drained-not-yet-terminated and
    terminating nodes whose ASG `should_decrement_desired_capacity`
    call has not yet completed), source `min` must stay at `desired -
    K` so each pending terminate is allowed to decrement `desired`.
    Once all pending decrements land, ASG drops `desired` by `K` and
    the next call to this function (with `pending_decrements = 0`)
    collapses to x/x/x at the new (lower) size.

    `pending_decrements` defaults to `0`, which is the right value for
    the target MNG (only grows) and for source MNGs at observation
    points between waves.

    Skipped when `desired == 0` (end of source roll). AWS EKS rejects
    `maxSize < 1` on `update-nodegroup-config`, so the strict x/x/x =
    0/0/0 is not reachable. The post-rollout cleanup PR persists the
    end state in tfvars (`min_size = 0, desired_size = 0`, `max_size`
    left at the pre-rollout envelope); the source MNG stays as the
    rollback artifact for one cycle, then is deleted by the next
    rollout's cleanup PR per the steady-state cleanup policy.
    """

    if mng.desired_size == 0:
        return
    target_min = max(0, mng.desired_size - pending_decrements)
    if mng.min_size == target_min and mng.desired_size == mng.max_size:
        return
    emit_event(
        Event.MNG_RECONCILED_XXX,
        cohort=cohort,
        role=role,
        nodegroup=mng.name,
        from_min=mng.min_size,
        from_desired=mng.desired_size,
        from_max=mng.max_size,
        to_min=target_min,
        to_desired=mng.desired_size,
        to_max=mng.desired_size,
        pending_decrements=pending_decrements,
    )
    set_mng_scaling(
        eks,
        cluster,
        mng.name,
        min_size=target_min,
        desired_size=mng.desired_size,
        max_size=mng.desired_size,
        dry_run=dry_run,
    )


def prepare_source_for_wave(
    eks: Any,
    cluster: str,
    cohort: str,
    source_mng: MngState,
    wave_size: int,
    pending_decrements: int,
    dry_run: bool,
) -> None:
    """Lower source `minSize` to allow this wave's terminate calls.

    AWS Auto Scaling refuses to decrement `desiredSize` below `minSize`
    on `TerminateInstanceInAutoScalingGroup(ShouldDecrementDesiredCapacity
    =true)`. After `reconcile_mng_xxx` has accounted for the already
    pending decrements, source `min == desired - pending_decrements`.
    The new wave adds `wave_size` more pending decrements, so this
    function lowers `min` to `max(0, desired - pending_decrements -
    wave_size)`. The next loop's reconcile pulls `min` back up once
    decrements have landed.
    """

    if wave_size <= 0:
        return
    target_min = max(0, source_mng.desired_size - pending_decrements - wave_size)
    if source_mng.min_size <= target_min:
        return
    emit_event(
        Event.SOURCE_PREPARED_FOR_WAVE,
        cohort=cohort,
        source_mng=source_mng.name,
        wave_size=wave_size,
        pending_decrements=pending_decrements,
        from_min=source_mng.min_size,
        to_min=target_min,
        source_desired=source_mng.desired_size,
        source_max=source_mng.max_size,
    )
    set_mng_scaling(
        eks,
        cluster,
        source_mng.name,
        min_size=target_min,
        desired_size=source_mng.desired_size,
        max_size=source_mng.max_size,
        dry_run=dry_run,
    )


def validate_initial_taint_state(
    *,
    target_mng_name: str,
    cohort_taint_key: str,
    ready_untainted: int,
    not_ready_untainted: int,
    source_terminated_at_start: int,
) -> str | None:
    """Fail-fast cohort-taint sanity check, called once at startup.

    Two failure modes this is designed to catch:

      1. **Wrong `--cohort-taint-key`** (typo, or stale value if the TF
         module overrode `cohort_taint_key`). kubectl matches no nodes
         with the configured key, the seed adds every Ready target
         node to `untainted_target_nodes`, and the rollout proceeds
         with no real fencing. The post-roll cleanup sweep then issues
         `kubectl taint <node> <wrong-key>-` which is a no-op against
         the actual taint — the script exits "successfully" but the
         real cohort taint stays in place, fencing the new worker
         pool from user workloads indefinitely.
      2. **Mirror rollback with the rollback target not re-tainted.**
         Re-apply the cohort taint to the rollback target before scale-up.
         If skipped, all rollback target nodes are
         genuinely untainted and the rollout drains the new source
         with zero fenced headroom, racing user pods for evacuation
         slots.

    Detection: the count of seed-untainted target nodes (Ready or
    NotReady) must not exceed how many source nodes have already
    terminated. On a fresh rollout (`source_terminated_at_start == 0`)
    any untainted target node is a violation. On a resume, the
    untainted count should be roughly equal to the source-terminated
    count — anything materially higher means the seed is being fed by
    a wrong-key kubectl read.

    Returns the error message string when the check fails, else None.
    """

    untainted_seed_count = ready_untainted + not_ready_untainted
    if untainted_seed_count <= source_terminated_at_start:
        return None
    return (
        f"error: target MNG `{target_mng_name}` has {untainted_seed_count} "
        f"nodes without the cohort taint `{cohort_taint_key}` "
        f"(Ready: {ready_untainted}, NotReady: {not_ready_untainted}), "
        f"but only {source_terminated_at_start} source node(s) have "
        "terminated. The rollout fencing invariant requires at least "
        f"{untainted_seed_count - source_terminated_at_start} more "
        "target nodes to carry the cohort taint than are currently seen "
        "by kubectl. Likely causes:\n"
        "  (1) --cohort-taint-key does not match the TF module's "
        "`cohort_taint_key` input. Pass the value the rollout PR "
        "actually set.\n"
        "  (2) Mirror rollback: the rollback target MNG was scaled up "
        "before its cohort taint was re-applied. Re-apply the cohort taint "
        "before scaling up the rollback target.\n"
        "Refusing to proceed; running without fencing would let "
        "user-create / user-restore traffic race evacuating sandboxes "
        "for target capacity."
    )


def run_loop(cfg: Config, aws: AwsClients, api: AdminApi) -> int:
    last_phase: dict[str, str] = {}
    stuck_logged: set[str] = set()
    drain_issued: set[str] = set()
    untainted_target_nodes: set[str] = set()
    terminate_state = TerminateState()
    snapshot_blocked_logged: set[str] = set()
    replace_blocked_logged: set[str] = set()

    # Anchor the "total = N + H" invariant: N (source's pre-rollout
    # desiredSize) comes from `--initial-source-desired-size` — required
    # on every invocation, no auto-derive. The script cannot safely
    # infer N from current cluster state: the terminate-then-interrupt
    # window leaves `source.desired = N - terminated` indistinguishable
    # from a fresh `0/H/H` prewarm with a smaller N, so any auto-derive
    # would silently under-provision target by the number of completed
    # terminates. Capture N from the source MNG's pre-rollout
    # `desired_size` once, before running the script. The flag is
    # validated below: it must be >= the current source.desired (source
    # only shrinks).
    initial_source = describe_mng(aws.eks, cfg.cluster, cfg.source_mng)
    initial_target = describe_mng(aws.eks, cfg.cluster, cfg.target_mng)

    # Read kubectl's current view of target taints for two startup-time
    # uses: the cohort-taint validator (immediately below) and the
    # `seen_tainted_target` initial value (further below). The in-memory
    # `untainted_target_nodes` bookkeeping is NOT seeded from this read:
    # the per-tick loop rebuilds it from kubectl at the top of every
    # tick (see the rebuild call inside `run_loop`'s while True). The
    # rebuild is load-bearing because kubectl is the source of truth —
    # an external `kubectl taint <node> <key>-` or a partial-success
    # taint mutation (API server accepts but the client times out
    # before adding to the set) would otherwise leave the in-memory set
    # stale and the terminate-driven budget cap would silently over-
    # grant on a later tick, dropping below the `H` reserved tainted
    # Ready nodes.
    (
        _initial_ready_tainted,
        initial_ready_untainted_target,
        initial_not_ready_untainted_target,
    ) = kubectl_target_nodes_by_taint(
        cfg.kubectl_context, cfg.target_mng, cfg.cohort_taint_key
    )

    # Validate operator-supplied N. The only invariant the script can
    # check unambiguously is `N >= source.desired_size` — N is the
    # source's pre-rollout `desired_size`, and source only shrinks
    # (terminate-with-decrement) over the course of the rollout.
    # An N below source.desired implies the operator passed a wrong
    # value (typo, confused with current size); accepting it would
    # silently under-grow target and risk drain waves with no
    # landing slots.
    if cfg.initial_source_desired_size < initial_source.desired_size:
        print(
            f"error: --initial-source-desired-size "
            f"({cfg.initial_source_desired_size}) is below the source MNG's "
            f"current desired_size ({initial_source.desired_size}). N is the "
            "source's *pre-rollout* desired_size and the source only shrinks "
            "during the rollout, so this value cannot be lower than the "
            "current source desired. Pass the pre-rollout value (likely "
            f"{initial_source.desired_size + max(0, initial_target.desired_size - cfg.headroom_count)} "
            "based on current state).",
            file=sys.stderr,
        )
        return 2

    # Validate the cohort taint is actually on the target before any
    # drain starts. See `validate_initial_taint_state` for the failure
    # modes this catches. Startup is the first chance to catch it, but
    # the check is repeated per-tick (below, inside the loop) until we
    # observe at least one tainted Ready target node — that observation
    # is the definitive signal that `--cohort-taint-key` matches the
    # MNG-declared taint. The startup call alone is insufficient when
    # the operator launches the script before target nodes have
    # registered (kubectl returns empty sets and the check vacuously
    # passes).
    err = validate_initial_taint_state(
        target_mng_name=cfg.target_mng,
        cohort_taint_key=cfg.cohort_taint_key,
        ready_untainted=len(initial_ready_untainted_target),
        not_ready_untainted=len(initial_not_ready_untainted_target),
        source_terminated_at_start=(
            cfg.initial_source_desired_size - initial_source.desired_size
        ),
    )
    if err is not None:
        print(err, file=sys.stderr)
        return 2
    seen_tainted_target = len(_initial_ready_tainted) > 0

    emit_event(
        Event.ROLLOUT_STARTED,
        cluster=cfg.cluster,
        source_mng=cfg.source_mng,
        target_mng=cfg.target_mng,
        cohort=cfg.cohort,
        cohort_from=cfg.source_mng,
        cohort_to=cfg.cohort,
        rollout_kind=cfg.rollout_kind,
        max_concurrent_node_drains=cfg.max_concurrent_node_drains,
        headroom_count=cfg.headroom_count,
        initial_source_desired_size=cfg.initial_source_desired_size,
        dry_run=cfg.dry_run,
    )

    while True:
        try:
            source_mng = describe_mng(aws.eks, cfg.cluster, cfg.source_mng)
            target_mng = describe_mng(aws.eks, cfg.cluster, cfg.target_mng)
            source_asg_inst = list_asg_instances(aws.autoscaling, source_mng.asg_names)
            target_asg_inst = list_asg_instances(aws.autoscaling, target_mng.asg_names)
            all_ids = [i.instance_id for i in source_asg_inst + target_asg_inst]
            ec2_by_id = describe_ec2_instances(aws.ec2, all_ids)
            admin_nodes = api.list_nodes()
            ready = kubectl_ready_node_names(cfg.kubectl_context)

            source = correlate_nodes(source_asg_inst, ec2_by_id, admin_nodes, ready)
            target = correlate_nodes(target_asg_inst, ec2_by_id, admin_nodes, ready)

            emit_phase_transitions(source, last_phase, mng="source", cohort=cfg.cohort)
            detect_stuck(source, cfg.stuck_threshold_sec, stuck_logged, cohort=cfg.cohort)

            # x/x/x reconcile: pull both MNGs back toward `min == desired ==
            # max`. Source `min` is held at `desired - pending_decrements`
            # so each pending terminate-with-decrement remains valid; once
            # all in-flight drains finish and ASG decrements `desired`, the
            # next iteration's reconcile (with `pending = 0`) collapses
            # everything to the new (lower) size.
            # Count source nodes whose terminate-with-decrement has been
            # initiated but whose source `desiredSize` decrement hasn't
            # yet been observed. Three cases:
            #
            # (1) Admin row present + phase in PHASES_IN_FLIGHT
            #     (DRAINING / DRAINED / TERMINATING). The standard
            #     wave-in-flight case.
            # (2) Admin row present + phase NONE + already in
            #     `drain_issued` set. Admin-API read lag — we POSTed
            #     `:drain` but `list_nodes()` hasn't reflected the
            #     commit yet.
            # (3) Admin row deleted (phase GONE) + ASG lifecycle still
            #     `Terminating*`. After `:terminate` the drain
            #     reconciler deletes the admin row immediately, but
            #     AWS can still report the ASG instance as
            #     `Terminating` before `describe_nodegroup` reflects
            #     the decrement. Without counting this case the
            #     reconcile would briefly see `pending=0` and raise
            #     source `min` back to the stale `desired`, racing
            #     ASG's still-in-flight decrement.
            source_pending_decrements = sum(
                1
                for n in source
                if n.rollout_phase in PHASES_IN_FLIGHT
                or (
                    n.rollout_phase == DrainPhase.NONE
                    and n.admin is not None
                    and n.admin.id in drain_issued
                )
                or (
                    n.rollout_phase == DrainPhase.GONE
                    and n.lifecycle_state in ASG_TERMINATING_STATES
                )
            )
            reconcile_mng_xxx(
                aws.eks,
                cfg.cluster,
                cfg.cohort,
                "source",
                source_mng,
                cfg.dry_run,
                pending_decrements=source_pending_decrements,
            )
            reconcile_mng_xxx(
                aws.eks, cfg.cluster, cfg.cohort, "target", target_mng, cfg.dry_run
            )
            # Re-fetch after the reconcile so subsequent decisions see the
            # collapsed state, not the pre-reconcile snapshot.
            source_mng = describe_mng(aws.eks, cfg.cluster, cfg.source_mng)
            target_mng = describe_mng(aws.eks, cfg.cluster, cfg.target_mng)

            src_remaining = source_remaining_live(source)
            tgt_ready = target_ready_count(target)

            # `cluster` / `cohort_from` / `cohort_to` / `rollout_kind` tag
            # every tick so the Datadog log pipeline can derive
            # `node_drain.rollout_nodes_remaining{cluster,cohort_from,cohort_to,rollout_kind}`
            # from `source_live` and
            # `node_drain.rollout_capacity_peak` from `total_capacity`
            # Keep cluster identity in the tag set so
            # simultaneous rollouts with equal cohort names in different
            # clusters do not collapse into one metric series.
            # `total_capacity` is precomputed here rather than summed inside
            # the log-based metric because `datadog_logs_metric` cannot read
            # more than one numeric path.
            emit_event(
                Event.TICK,
                cluster=cfg.cluster,
                cohort=cfg.cohort,
                cohort_from=cfg.source_mng,
                cohort_to=cfg.cohort,
                rollout_kind=cfg.rollout_kind,
                source_desired=source_mng.desired_size,
                source_live=src_remaining,
                target_desired=target_mng.desired_size,
                target_ready=tgt_ready,
                total_capacity=source_mng.desired_size + target_mng.desired_size,
                source_draining=sum(1 for n in source if n.rollout_phase == DrainPhase.DRAINING),
                source_drained=sum(1 for n in source if n.rollout_phase == DrainPhase.DRAINED),
                source_terminating=sum(
                    1 for n in source if n.rollout_phase == DrainPhase.TERMINATING
                ),
            )

            # Pending sandbox-pod count for the RFC-specified
            # `node_drain.evacuation_pods_pending{cohort_to}` derived gauge.
            # Emitted every tick (zero included) so the Datadog gauge has a
            # continuous value and the `> 0 for 5 min` alert evaluates on the
            # metric directly, without a `default_zero()` workaround.
            # kubectl failures here must not abort the rollout: catch + log
            # via a tick_error-shaped emission and continue.
            try:
                evacuation_pods_pending = kubectl_pending_evacuation_pod_count(
                    cfg.kubectl_context, cfg.cohort_taint_key, cfg.cohort
                )
                emit_event(
                    Event.EVACUATION_POD_PENDING,
                    cluster=cfg.cluster,
                    cohort=cfg.cohort,
                    cohort_from=cfg.source_mng,
                    cohort_to=cfg.cohort,
                    rollout_kind=cfg.rollout_kind,
                    count=evacuation_pods_pending,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError) as e:
                # Carry the same tag set as the success path so persistent
                # kubectl failures during a rollout can be filtered by
                # cluster/cohort/rollout_kind in Datadog — otherwise a flat
                # `evacuation_pods_pending` gauge can't be correlated with
                # its underlying failure cause without a manual log scan.
                emit_event(
                    Event.TICK_ERROR,
                    cluster=cfg.cluster,
                    cohort=cfg.cohort,
                    cohort_from=cfg.source_mng,
                    cohort_to=cfg.cohort,
                    rollout_kind=cfg.rollout_kind,
                    where="evacuation_pod_pending_query",
                    error=str(e),
                )

            # Per-target-node untaint. Each terminated source node releases
            # exactly one target slot for user traffic: the target node that
            # absorbed its sandboxes has done its migration job and can drop
            # the cohort taint. Cap the running untaint count at the running
            # source-terminated count so the `H` headroom slots stay
            # cohort-tainted across the rollout — user-create / user-restore
            # pods would otherwise crowd into freshly-launched target nodes
            # before the next wave's evacuation pods get a chance to land
            # there, violating the `k <= H` capacity invariant. The MNG-level
            # taint covering the final `H` Ready+tainted nodes is dropped in
            # the post-rollout Terraform cleanup PR.
            # Untaint budget: minimum of two independent constraints.
            #
            #  (A) **Headroom-preserving cap** — `tainted_ready - H`.
            #      Never dip below H tainted Ready target nodes. Closes
            #      two transient over-untaint windows:
            #        * Mid-wave terminate with `k > 1`: one drain in a
            #          wave finishes before the others. The old
            #          terminate-driven formula would untaint one of the
            #          H fenced slots while `k-1` evacuations are still
            #          landing pods there, racing user pods for the
            #          same slot.
            #        * Target provisioning lag: target `desired_size`
            #          grows on terminate, but the new EC2 instance
            #          takes ~60–120s to reach Ready. During that
            #          window `tainted_ready` is still at `H + T - 1`
            #          while the terminate-driven formula would already
            #          permit an untaint.
            #
            #  (B) **Terminate-driven cap** — `source_terminated_total -
            #      len(untainted_target_nodes)`. One untaint per landed
            #      source terminate. Closes the *oversized-target*
            #      window that (A) alone misses: in a mirror rollback,
            #      the operator scales the rollback target up to its
            #      pre-rollback size before invoking the script, so
            #      tick 0 sees `tainted_ready = ROLLBACK_N >> H` with
            #      zero source terminates done. (A) alone would
            #      immediately untaint `ROLLBACK_N - H` nodes before any
            #      source drain landed, violating the
            #      `target_desired = H + (N_initial - source.desired)`
            #      capacity invariant the rollback wave is supposed to
            #      enforce. (B) prevents that — `source_terminated_total
            #      = 0 → budget = 0`.
            #
            # Taking the **min** of (A) and (B) (clipped to 0) makes the
            # budget honor both invariants simultaneously. At steady
            # observation points in a forward rollout, (A) and (B) are
            # numerically equal; in the transient and rollback cases,
            # one of them tightens the cap to the correct value.
            (
                tainted_ready,
                tick_ready_untainted,
                tick_not_ready_untainted,
            ) = kubectl_target_nodes_by_taint(
                cfg.kubectl_context, cfg.target_mng, cfg.cohort_taint_key
            )
            # Re-run the cohort-taint sanity check per tick until we've
            # confirmed at least one tainted Ready target node exists.
            # The startup snapshot is empty if the operator launched
            # the script before target nodes registered, so a wrong
            # `--cohort-taint-key` or a mirror rollback without a
            # re-applied taint wouldn't surface until target nodes
            # appeared. Once we see one tainted Ready node the key is
            # proven correct and we stop re-checking — past that point,
            # `untainted > source_terminated` is the legitimate steady
            # state and would false-positive.
            if not seen_tainted_target:
                if len(tainted_ready) > 0:
                    seen_tainted_target = True
                else:
                    source_terminated_now = (
                        cfg.initial_source_desired_size - source_mng.desired_size
                    )
                    err = validate_initial_taint_state(
                        target_mng_name=cfg.target_mng,
                        cohort_taint_key=cfg.cohort_taint_key,
                        ready_untainted=len(tick_ready_untainted),
                        not_ready_untainted=len(tick_not_ready_untainted),
                        source_terminated_at_start=source_terminated_now,
                    )
                    if err is not None:
                        print(err, file=sys.stderr)
                        return 2
            # Rebuild `untainted_target_nodes` from kubectl every tick.
            # kubectl is the source of truth; the in-memory set would
            # drift if an external `kubectl taint <node> <key>-` runs
            # outside our happy path, or if our own `kubectl taint`
            # mutation lands on the API server but the client request
            # times out before reaching the `add()` below — leaving a
            # node untainted on the cluster but absent from our set.
            # A stale (under-counting) set would let the terminate cap
            # `T - len(set)` silently over-grant on a later tick,
            # dropping below the H reserved tainted Ready nodes. The
            # within-tick `add()` after each successful
            # `kubectl_remove_cohort_taint` keeps the set consistent
            # for the rest of this tick.
            untainted_target_nodes = (
                set(tick_ready_untainted) | set(tick_not_ready_untainted)
            )
            # Intersect with admin-registered Ready target nodes. K8s
            # `Ready` flips when the kubelet first heartbeats, but the
            # SPACE node-local DaemonSets (`space-node-api`,
            # `space-sandbox-manager`, …) take additional seconds to
            # start, register, and create the admin row this script
            # treats as the "node is SPACE-manageable" signal. During
            # that bootstrap window a target node would be tainted +
            # K8s-Ready but unable to host SPACE sandboxes — untainting
            # it now exposes it to user-create/restore scheduling
            # against capacity the control plane can't actually manage.
            # `target_ready_count` already uses this stricter notion;
            # the untaint loop must too. Same correlation rule for both
            # the budget calc and the candidate list, so the H slots
            # the budget formula reserves are the same H slots
            # evacuation pods can actually land on.
            admin_registered_target_names = {
                n.private_dns for n in target
                if n.k8s_ready
                and n.admin is not None
                and n.admin.drain_phase is None
                and n.private_dns is not None
            }
            tainted_ready &= admin_registered_target_names
            source_terminated_total = (
                cfg.initial_source_desired_size - source_mng.desired_size
            )
            headroom_cap = len(tainted_ready) - cfg.headroom_count
            terminate_cap = source_terminated_total - len(untainted_target_nodes)
            untaint_budget = max(0, min(headroom_cap, terminate_cap))
            if untaint_budget > 0:
                # Prefer occupied target nodes for untainting. An
                # already-filled target node (sandboxes migrated into
                # it by completed drains) is using its target capacity
                # for evacuation; untainting it lets user pods land on
                # the same node without contending for *new* fenced
                # headroom. Untainting an empty target node instead
                # opens up headroom that the next drain wave was
                # counting on — the `H` tainted invariant would still
                # be numerically intact, but the actual free
                # evacuation capacity would shrink as user pods bin-
                # packed onto the just-released empty node.
                #
                # Fill is `sandboxes_active + sandboxes_local_snapshots`
                # from the admin row. Sort fill descending, name
                # ascending on ties so the pick stays deterministic.
                # Nodes without an admin row are treated as fill=0 and
                # picked last — they shouldn't end up here at all
                # because `tainted_ready` is already intersected with
                # `admin_registered_target_names`, but the guard is
                # cheap and defends against future refactors.
                target_by_name = {
                    n.private_dns: n for n in target if n.private_dns
                }

                def _fill_score(node_name: str) -> int:
                    ns = target_by_name.get(node_name)
                    if ns is None or ns.admin is None:
                        return 0
                    return (
                        ns.admin.sandboxes_active
                        + ns.admin.sandboxes_local_snapshots
                    )

                candidates = sorted(
                    tainted_ready - untainted_target_nodes,
                    key=lambda n: (-_fill_score(n), n),
                )
                for node_name in candidates[:untaint_budget]:
                    if cfg.dry_run:
                        emit_event(
                            Event.DRY_RUN_UNTAINT,
                            cohort=cfg.cohort,
                            node=node_name,
                            taint_key=cfg.cohort_taint_key,
                        )
                    else:
                        kubectl_remove_cohort_taint(
                            cfg.kubectl_context, node_name, cfg.cohort_taint_key
                        )
                        emit_event(
                            Event.TARGET_NODE_UNTAINTED,
                            cohort=cfg.cohort,
                            node=node_name,
                            taint_key=cfg.cohort_taint_key,
                        )
                    untainted_target_nodes.add(node_name)

            # Completion needs both: no live source instances AND source MNG
            # `desiredSize == 0`. Otherwise ASG can launch fresh source nodes
            # after we exit — `replace_after_terminate=false` decrements
            # desired per-termination, but a transient capacity/launch
            # failure could leave `live=0` while desired > 0, which EKS will
            # then refill behind the rollout.
            if src_remaining == 0 and source_mng.desired_size == 0:
                # Final ratchet: the last source terminate landed in the
                # previous tick, so target.desired is still at
                # `H + (N - 1)`. Grow it to `H + N` before declaring done,
                # otherwise the rollout exits one node short of the
                # documented `target = N + H` end-state.
                final_target_desired = (
                    cfg.headroom_count + cfg.initial_source_desired_size
                )
                if target_mng.desired_size < final_target_desired:
                    # Final ratchet still needs to be issued (or re-issued
                    # if previously deferred).
                    if target_mng.status != "ACTIVE":
                        # Target MNG mid-update from a prior grow. EKS
                        # rejects overlapping `update_nodegroup_config`
                        # calls; defer to the next tick.
                        emit_event(
                            Event.TARGET_RATCHET_DEFERRED,
                            cohort=cfg.cohort,
                            target_mng=cfg.target_mng,
                            target_status=target_mng.status,
                            from_desired=target_mng.desired_size,
                            to_desired=final_target_desired,
                            final=True,
                        )
                        time.sleep(cfg.poll_interval_sec)
                        continue
                    emit_event(
                        Event.TARGET_RATCHET,
                        cohort=cfg.cohort,
                        target_mng=cfg.target_mng,
                        from_desired=target_mng.desired_size,
                        to_desired=final_target_desired,
                        source_terminated=cfg.initial_source_desired_size,
                        headroom=cfg.headroom_count,
                        final=True,
                    )
                    set_mng_scaling(
                        aws.eks,
                        cfg.cluster,
                        cfg.target_mng,
                        min_size=final_target_desired,
                        desired_size=final_target_desired,
                        max_size=final_target_desired,
                        dry_run=cfg.dry_run,
                    )
                    # `update_nodegroup_config` is async — EKS goes to
                    # UPDATING while the ASG launches the final H nodes.
                    # Don't declare completion yet; the next tick will
                    # observe `target.desired == final_target_desired`
                    # and fall through to the ACTIVE-wait below.
                    time.sleep(cfg.poll_interval_sec)
                    continue
                if (
                    target_mng.status != "ACTIVE"
                    or tgt_ready < final_target_desired
                ):
                    # Final ratchet has been issued and `target.desired`
                    # matches, but either the EKS scaling update is still
                    # in flight (status UPDATING) or the freshly-added
                    # target nodes have not yet reached K8s `Ready` (EKS
                    # ACTIVE means ASG launched, not that node-local
                    # SPACE services are running). Per-wave growth gates
                    # on `tgt_ready >= want_desired`; the final wave gets
                    # the same treatment so operators aren't told the
                    # rollout is done while the last `H` target nodes
                    # are still booting.
                    emit_event(
                        Event.WAITING_FINAL_TARGET_ACTIVE,
                        cohort=cfg.cohort,
                        target_mng=cfg.target_mng,
                        target_status=target_mng.status,
                        target_desired=target_mng.desired_size,
                        target_ready=tgt_ready,
                        needed=final_target_desired,
                    )
                    time.sleep(cfg.poll_interval_sec)
                    continue
                # Final headroom untaint. The per-tick budget caps untaints
                # at `source_terminated_total` (= N at rollout end), leaving
                # the last `H` Ready target nodes — the ratcheted-in
                # headroom from the final grow — still carrying the cohort
                # taint. Drop the taint on every remaining Ready+tainted
                # target node before signaling completion so the operator
                # ends with a uniformly untainted pool. The MNG-level
                # Terraform untaint (cleanup Step 2) is still required to
                # cover future scale-out / replacement nodes.
                final_tainted_ready, _, _ = kubectl_target_nodes_by_taint(
                    cfg.kubectl_context, cfg.target_mng, cfg.cohort_taint_key
                )
                for node_name in sorted(final_tainted_ready - untainted_target_nodes):
                    if cfg.dry_run:
                        emit_event(
                            Event.DRY_RUN_UNTAINT,
                            cohort=cfg.cohort,
                            node=node_name,
                            taint_key=cfg.cohort_taint_key,
                            final_headroom=True,
                        )
                    else:
                        kubectl_remove_cohort_taint(
                            cfg.kubectl_context, node_name, cfg.cohort_taint_key
                        )
                        emit_event(
                            Event.TARGET_NODE_UNTAINTED,
                            cohort=cfg.cohort,
                            node=node_name,
                            taint_key=cfg.cohort_taint_key,
                            final_headroom=True,
                        )
                    untainted_target_nodes.add(node_name)

                emit_event(
                    Event.EVACUATION_COMPLETE,
                    cohort=cfg.cohort,
                    source_mng=cfg.source_mng,
                    target_mng=cfg.target_mng,
                )
                return 0
            if src_remaining == 0 and source_mng.desired_size > 0:
                # No live nodes but desired hasn't caught up — log so the
                # operator can investigate ASG state.
                emit_event(
                    Event.WAITING_SOURCE_DESIRED_ZERO,
                    cohort=cfg.cohort,
                    source_mng=cfg.source_mng,
                    source_desired=source_mng.desired_size,
                )

            want_desired = compute_target_desired(
                target_mng,
                cfg.initial_source_desired_size,
                source_mng.desired_size,
                cfg.headroom_count,
            )
            if want_desired > target_mng.desired_size:
                # Skip the ratchet if the target nodegroup is in the middle
                # of a prior scaling update — EKS rejects overlapping
                # `update_nodegroup_config` calls with `ResourceInUseException`,
                # and the next tick will see the new size once the prior
                # update completes.
                if target_mng.status != "ACTIVE":
                    emit_event(
                        Event.TARGET_RATCHET_DEFERRED,
                        cohort=cfg.cohort,
                        target_mng=cfg.target_mng,
                        target_status=target_mng.status,
                        from_desired=target_mng.desired_size,
                        to_desired=want_desired,
                    )
                else:
                    terminated_count = max(
                        0,
                        cfg.initial_source_desired_size - source_mng.desired_size,
                    )
                    emit_event(
                        Event.TARGET_RATCHET,
                        cohort=cfg.cohort,
                        target_mng=cfg.target_mng,
                        from_desired=target_mng.desired_size,
                        to_desired=want_desired,
                        source_terminated=terminated_count,
                        headroom=cfg.headroom_count,
                    )
                    # x/x/x grow: bump min and max along with desired so the
                    # target MNG stays at min == desired == max at every
                    # observation point between waves.
                    set_mng_scaling(
                        aws.eks,
                        cfg.cluster,
                        cfg.target_mng,
                        min_size=want_desired,
                        desired_size=want_desired,
                        max_size=want_desired,
                        dry_run=cfg.dry_run,
                    )

            # Only issue new drains once the target has caught up to the
            # size the rollout wants for this wave. Use `want_desired`
            # (the value just sent to EKS this tick, if a grow fired)
            # rather than `target_mng.desired_size` (the pre-grow
            # snapshot); otherwise tick 0 — where `target.desired_size
            # == 0` before the first grow lands — would short-circuit
            # the gate and start draining before any target node exists.
            # `want_desired = max(target.desired_size, …)` so it is
            # always >= the in-memory pre-grow value, which preserves
            # the gate's behavior on ticks where no grow was needed.
            needed_target = want_desired
            if tgt_ready < needed_target:
                emit_event(
                    Event.WAITING_TARGET_READY,
                    cohort=cfg.cohort,
                    target_ready=tgt_ready,
                    needed=needed_target,
                )
            else:
                to_drain = pick_drain_candidates(
                    source,
                    cfg.max_concurrent_node_drains,
                    stuck_node_ids=frozenset(stuck_logged),
                    already_drained_ids=frozenset(drain_issued),
                )
                # x/x/x shrink prep: lower source `min` so ASG's
                # terminate-with-decrement is allowed to run on every node
                # this wave will drain. The reconcile at the top of the next
                # iteration pulls `min` back up to the new (lower)
                # `desired` once terminates land.
                prepare_source_for_wave(
                    aws.eks,
                    cfg.cluster,
                    cfg.cohort,
                    source_mng,
                    wave_size=len(to_drain),
                    pending_decrements=source_pending_decrements,
                    dry_run=cfg.dry_run,
                )
                issue_drain_wave(
                    api,
                    to_drain,
                    cohort=cfg.cohort,
                    drain_issued=drain_issued,
                    last_phase=last_phase,
                )

            # Drive `drained -> terminating` from the orchestrator regardless
            # of whether the cluster's `drained_node_terminator` reconciler
            # is enabled. The default Helm value is
            # `SPACE_DRAINED_NODE_TERMINATOR_ENABLED=false`; without us
            # POSTing `:terminate` here, source nodes would pile up in
            # `drained` and the rollout would hang. Independent of the drain
            # concurrency cap (terminate is cheap and only flips DB state
            # plus an async ASG call).
            #
            # Gate on source MNG `status == UPDATING` (and only UPDATING)
            # before issuing terminates. `prepare_source_for_wave` runs an
            # async EKS `update_nodegroup_config` to lower source `minSize`;
            # if that update hasn't propagated, ASG's
            # `TerminateInstanceInAutoScalingGroup(decrement=true)` can
            # still see the old min and reject the call when desired would
            # drop below it. Fast-draining empty source nodes can transition
            # `draining → drained` quickly enough to hit this race; deferring
            # the terminate to the next tick lets EKS finish propagation.
            #
            # Scoped to UPDATING only — other non-ACTIVE statuses
            # (`DEGRADED`, `CREATE_FAILED`, `UPDATE_FAILED`, etc.) are not
            # races that waiting fixes. A `DEGRADED` source MNG (e.g. from
            # a stuck-bootstrap node) shouldn't block legitimate terminates;
            # the operator needs to see the rollout progress or surface a
            # `terminate_failures` counter, not have the loop silently sleep
            # forever.
            if source_mng.status == "UPDATING":
                pending_terminate = pick_terminate_candidates(
                    source, already_terminated_ids=frozenset(terminate_state.issued)
                )
                if pending_terminate:
                    emit_event(
                        Event.TERMINATE_DEFERRED_SOURCE_UPDATING,
                        cohort=cfg.cohort,
                        source_mng=cfg.source_mng,
                        source_status=source_mng.status,
                        pending_terminates=len(pending_terminate),
                    )
                time.sleep(cfg.poll_interval_sec)
                continue
            run_terminate_pass(
                api,
                pick_terminate_candidates(
                    source, already_terminated_ids=frozenset(terminate_state.issued)
                ),
                cohort=cfg.cohort,
                state=terminate_state,
                now=time.monotonic(),
            )

            report_local_snapshot_hints(
                source,
                cohort=cfg.cohort,
                logged=snapshot_blocked_logged,
                terminated=terminate_state.issued,
            )

            # Surface drained nodes whose drain was started without
            # `replace_after_terminate=false`. Terminating them would let
            # the source ASG auto-replace, breaking the rollout's
            # monotonic-shrink invariant. The operator should re-drain
            # them with the correct flag (admin CLI / direct API).
            for ns in pick_terminate_blocked_by_replace_semantics(source):
                assert ns.admin is not None
                if ns.admin.id in replace_blocked_logged:
                    continue
                replace_blocked_logged.add(ns.admin.id)
                emit_event(
                    Event.TERMINATE_BLOCKED_REPLACE_SEMANTICS,
                    cohort=cfg.cohort,
                    node_id=ns.admin.id,
                    k8s_node_name=ns.admin.k8s_node_name,
                    instance_id=ns.instance_id,
                )

            time.sleep(cfg.poll_interval_sec)
        except KeyboardInterrupt:
            emit_event(Event.INTERRUPTED, cohort=cfg.cohort)
            return 130
        except botocore.exceptions.NoCredentialsError as exc:
            # AWS credentials are absent (env unset, SSO cache cleared).
            # The loop cannot recover without operator action — exit so an
            # unattended rollout doesn't quietly retry-spin until source
            # nodes get stuck.
            emit_event(
                Event.FATAL_AWS_CREDENTIALS,
                cohort=cfg.cohort,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return 4
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in AWS_CREDENTIAL_FAILURE_CODES:
                # SSO session expired mid-rollout. Refreshing requires an
                # interactive `aws-sso ...` login the script can't drive.
                # Bail loudly so the operator re-auths and re-runs (the loop
                # is idempotent on resume).
                emit_event(
                    Event.FATAL_AWS_CREDENTIALS_EXPIRED,
                    cohort=cfg.cohort,
                    error_code=code,
                    error=str(exc),
                )
                return 4
            emit_event(
                Event.TICK_ERROR,
                cohort=cfg.cohort,
                error_type=type(exc).__name__,
                error_code=code,
                error=str(exc),
            )
            time.sleep(cfg.poll_interval_sec)
        except Exception as exc:
            # Transient AWS / admin-API / kubectl errors should not kill an
            # unattended rollout. Surface the error, sleep, and retry on the
            # next tick — the loop is idempotent against re-reading current
            # state. Re-raise `KeyboardInterrupt` and `SystemExit` (handled by
            # the bare `except Exception` not catching `BaseException`).
            emit_event(
                Event.TICK_ERROR,
                cohort=cfg.cohort,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            time.sleep(cfg.poll_interval_sec)


def emit_phase_transitions(
    nodes: list[NodeStatus],
    last_phase: dict[str, str],
    mng: str,
    cohort: str,
) -> None:
    """Emit one event per node whose drain_phase changed since the last tick."""

    for n in nodes:
        if n.admin is None:
            continue
        cur = n.rollout_phase
        prev = last_phase.get(n.admin.id)
        if prev == cur:
            continue
        last_phase[n.admin.id] = cur
        event = PHASE_TO_EVENT.get(cur)
        if event is None:
            continue
        emit_event(
            event,
            cohort=cohort,
            mng=mng,
            node_id=n.admin.id,
            k8s_node_name=n.admin.k8s_node_name,
            instance_id=n.instance_id,
            from_phase=prev,
            to_phase=cur,
        )


PHASE_TO_EVENT: dict[DrainPhase, Event] = {
    DrainPhase.DRAINING: Event.DRAINING,
    DrainPhase.DRAINED: Event.DRAINED,
    DrainPhase.TERMINATING: Event.TERMINATING,
}


def issue_drain_wave(
    api: AdminApi,
    to_drain: list[NodeStatus],
    *,
    cohort: str,
    drain_issued: set[str],
    # `dict[str, str]`, matching the declaration at the call site and
    # `emit_phase_transitions`, which shares this dict. `DrainPhase` is a
    # `StrEnum`, so writing a member in is still a `str`; annotating the
    # parameter as `dict[str, DrainPhase]` made mypy reject the call outright,
    # because `dict` is invariant in its value type.
    last_phase: dict[str, str],
) -> int:
    """POST `:drain` for each candidate. Returns how many were actually issued.

    Extracted from the tick so the two decisions here are testable, because both
    are invisible from `AdminApi.drain`'s own tests and both are silent when
    wrong:

    * **A refusal must not be booked.** `drain` returns `False` when it declines
      — somebody else's drain landed inside our snapshot window, or the phase
      could not be read. Adding such a node to `drain_issued` would remove it
      from candidacy permanently *and* count it against
      `--max-concurrent-node-drains` forever, so a wave would quietly lose a
      slot per refusal until it could no longer drain anything.
    * **A failure must be contained per node.** `drain` reads before it posts,
      so it has two ways to raise instead of one, and `pick_drain_candidates`
      orders deterministically — an uncontained raise on the first candidate
      would starve every later one *and* skip the terminate phase in the same
      block, every tick, turning a slow wave into a stalled one.

    A wave that had candidates and issued none of them emits
    `Event.DRAIN_WAVE_MADE_NO_PROGRESS`. Reported here rather than by the caller
    because both numbers the decision needs are local: the count of candidates
    and the count issued. Deciding it from the return value alone conflated an
    all-refused wave with an empty one, since both give zero.
    """
    issued_count = 0
    for ns in to_drain:
        assert ns.admin is not None
        try:
            issued = api.drain(ns.admin.id, replace_after_terminate=False)
        except Exception as err:  # noqa: BLE001 - per-node containment
            emit_event(
                Event.TICK_ERROR,
                cohort=cohort,
                node_id=ns.admin.id,
                error=str(err),
                phase="drain",
            )
            continue
        if not issued:
            continue
        drain_issued.add(ns.admin.id)
        issued_count += 1
        emit_event(
            Event.DRAIN_STARTED,
            cohort=cohort,
            node_id=ns.admin.id,
            k8s_node_name=ns.admin.k8s_node_name,
            instance_id=ns.instance_id,
        )
        last_phase[ns.admin.id] = DrainPhase.DRAINING
    if to_drain and issued_count == 0:
        # Every candidate was refused or errored. Reported because the
        # alternative is silence: a wave that declines all of them — the shape a
        # foreign drain on the only slot, or an unreadable phase on every read,
        # produces — otherwise looks exactly like a wave with nothing to do,
        # right up to the zero exit code. `drain_skipped_*` says why per node;
        # this says the wave as a whole is not moving.
        #
        # `to_drain and` is load-bearing: `to_drain` is empty on every tick after
        # the last node is drained, which is the steady state of a finishing
        # rollout. Without the conjunct this would fire on each of those ticks
        # and mean the opposite of what it says.
        emit_event(
            Event.DRAIN_WAVE_MADE_NO_PROGRESS,
            cohort=cohort,
            candidates=len(to_drain),
        )
    return issued_count


def detect_stuck(
    nodes: list[NodeStatus],
    threshold_sec: int,
    logged: set[str],
    cohort: str,
) -> None:
    now = dt.datetime.now(tz=dt.timezone.utc)
    for n in nodes:
        if n.admin is None or n.rollout_phase != DrainPhase.DRAINING:
            continue
        started = n.admin.drain_started_at
        if started is None:
            continue
        elapsed = (now - started).total_seconds()
        if elapsed < threshold_sec or n.admin.id in logged:
            continue
        logged.add(n.admin.id)
        emit_event(
            Event.NODE_STUCK,
            cohort=cohort,
            node_id=n.admin.id,
            k8s_node_name=n.admin.k8s_node_name,
            instance_id=n.instance_id,
            elapsed_sec=int(elapsed),
            sandboxes_active=n.admin.sandboxes_active,
        )


# --- CLI --------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> Config:
    p = argparse.ArgumentParser(
        description="Two-MNG node rollout orchestration for SPACE EKS clusters.",
    )
    p.add_argument(
        "--eks-cluster-name",
        required=True,
        help="EKS cluster name (used in AWS EKS / ASG / EC2 API calls)",
    )
    p.add_argument("--source-mng", required=True, help="Incumbent managed node group")
    p.add_argument("--target-mng", required=True, help="New managed node group")
    p.add_argument(
        "--cohort",
        required=True,
        help="Cohort label for telemetry (e.g. the target LT version)",
    )
    p.add_argument(
        "--max-concurrent-node-drains",
        type=int,
        default=1,
        help="Max source nodes in 'draining' at once (default: 1)",
    )
    p.add_argument(
        "--headroom-count",
        type=int,
        default=None,
        help="Extra target capacity beyond source-active (default: max-concurrent-node-drains)",
    )
    p.add_argument(
        "--space-api",
        required=True,
        help="SPACE admin API base URL (e.g. https://api.example.com)",
    )
    p.add_argument("--aws-region", required=True)
    p.add_argument(
        "--k8s-context",
        default="",
        help=(
            "kubectl context for Ready checks. Required when running outside "
            "a Kubernetes pod — no default to the current context, to avoid "
            "acting on the wrong cluster. Pass an empty value (or omit the "
            "flag) only when running in-cluster as a Job, where the "
            "ServiceAccount-mounted config already targets this cluster and "
            "kubectl uses it automatically with no `--context` flag."
        ),
    )
    p.add_argument(
        "--stuck-threshold",
        type=int,
        default=DEFAULT_STUCK_THRESHOLD_SEC,
        help=f"Seconds before flagging a node as stuck in draining (default: {DEFAULT_STUCK_THRESHOLD_SEC})",
    )
    p.add_argument(
        "--poll-interval",
        type=int,
        default=DEFAULT_POLL_INTERVAL_SEC,
        help=f"Loop interval in seconds (default: {DEFAULT_POLL_INTERVAL_SEC})",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Read-only: log planned mutations instead of issuing them",
    )
    p.add_argument(
        "--token-env",
        default=DEFAULT_TOKEN_ENV,
        help=f"Env var holding the SPACE admin bearer token (default: {DEFAULT_TOKEN_ENV})",
    )
    p.add_argument(
        "--initial-source-desired-size",
        type=int,
        required=True,
        help=(
            "Source MNG's pre-rollout `N` used in the "
            "`target_desired = headroom + (N - source.desired)` formula. "
            "Required (no auto-derive): if a previous run terminated a "
            "source node and crashed before the next tick's target "
            "ratchet / per-node untaint, the cluster state is "
            "indistinguishable from a fresh `0/H/H` prewarm except that "
            "`source.desired` has already shrunk. Auto-deriving N from "
            "current source.desired in that window would under-grow "
            "target by the number of completed terminates. Capture N "
            "from the source MNG's pre-rollout desired_size before "
            "starting:\n"
            "  N=$(aws eks describe-nodegroup --cluster-name <c> "
            "--nodegroup-name <source-mng> "
            "--query 'nodegroup.scalingConfig.desiredSize' --output text)"
        ),
    )
    p.add_argument(
        "--cohort-taint-key",
        default=DEFAULT_COHORT_TAINT_KEY,
        help=(
            "Kubernetes taint key declared by the `rollout-mng` Terraform "
            f"module on the target MNG. Default: `{DEFAULT_COHORT_TAINT_KEY}` "
            "(matches the TF module's default). If the rollout PR overrode "
            "`cohort_taint_key` on the module, pass the same value here so "
            "the script's untaint loop can find the taint to remove."
        ),
    )
    p.add_argument(
        "--rollout-kind",
        required=True,
        choices=ROLLOUT_KINDS,
        help=(
            "Which dimension of the MNG spec is changing. Carried on "
            "telemetry events (`rollout_started`, `tick`, "
            "`evacuation_pod_pending`) as the `rollout_kind` tag for the "
            "RFC-specified derived gauges. Pick `other` for AZ-only/IAM/"
            "degraded-host rollouts that don't fit the other slots."
        ),
    )
    p.add_argument(
        "--prompt-login",
        action="store_true",
        help=(
            "Prompt for SPACE admin email + password and mint a fresh "
            "session token via `/auth/v1/sessions`. Overrides --token-env. "
            "Use when you haven't already exported `SPACE_API_TOKEN` "
            "in the current shell (or when the previous token expired)."
        ),
    )
    args = p.parse_args(argv)

    headroom = (
        args.headroom_count
        if args.headroom_count is not None
        else args.max_concurrent_node_drains
    )
    if args.max_concurrent_node_drains < 1:
        p.error("--max-concurrent-node-drains must be >= 1")
    if headroom < args.max_concurrent_node_drains:
        # `k > H` would leave each drain wave with fewer free target slots
        # than migrating sandboxes; the script would block waiting for
        # target capacity that the wave itself is supposed to fill. The
        # cluster-capacity invariant requires `1 <= k <= H`.
        p.error(
            f"--headroom-count ({headroom}) must be >= --max-concurrent-node-drains "
            f"({args.max_concurrent_node_drains}); cluster-capacity invariant requires 1 <= k <= H"
        )
    if args.source_mng == args.target_mng:
        # If both names point at the same MNG the source ASG and target ASG
        # are identical: target Ready is satisfied by the very nodes being
        # drained, and `replace_after_terminate=false` shrinks the only MNG
        # the operator has. Fail fast before any AWS or admin-API call.
        p.error("--source-mng and --target-mng must refer to different node groups")

    return Config(
        cluster=args.eks_cluster_name,
        source_mng=args.source_mng,
        target_mng=args.target_mng,
        cohort=args.cohort,
        max_concurrent_node_drains=args.max_concurrent_node_drains,
        headroom_count=headroom,
        space_api=args.space_api,
        aws_region=args.aws_region,
        kubectl_context=args.k8s_context,
        stuck_threshold_sec=args.stuck_threshold,
        poll_interval_sec=args.poll_interval,
        dry_run=args.dry_run,
        token_env=args.token_env,
        initial_source_desired_size=args.initial_source_desired_size,
        cohort_taint_key=args.cohort_taint_key,
        rollout_kind=args.rollout_kind,
        prompt_login=args.prompt_login,
    )


def prompt_admin_login(api_url: str) -> str:
    """Interactively mint a SPACE admin session token via `/auth/v1/sessions`.

    Reads email from stdin (or /dev/tty if stdin is piped — e.g. when the
    operator pipes stdout to `tee`), reads the password via getpass so it
    is not echoed. Returns the raw bearer token. Bypasses the bash-vs-zsh
    `read -rsp` / `read "VAR?…"` syntax difference operators used to hit
    when minting tokens via the shell.
    """
    import getpass
    print("SPACE admin login (POST /auth/v1/sessions)", file=sys.stderr)
    if sys.stdin.isatty():
        sys.stderr.write("Email: ")
        sys.stderr.flush()
        email = sys.stdin.readline().rstrip("\n")
    else:
        with open("/dev/tty", "r") as tty_in:
            sys.stderr.write("Email: ")
            sys.stderr.flush()
            email = tty_in.readline().rstrip("\n")
    password = getpass.getpass("Password: ")
    resp = requests.post(
        f"{api_url.rstrip('/')}/auth/v1/sessions",
        json={"email": email, "password": password},
        timeout=20,
    )
    if resp.status_code == 401:
        print("login failed: invalid email or password", file=sys.stderr)
        sys.exit(2)
    resp.raise_for_status()
    body = resp.json()
    token = body.get("token")
    if not token:
        print(f"login response missing token: {body}", file=sys.stderr)
        sys.exit(2)
    return token


IN_CLUSTER_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"


def main(argv: list[str] | None = None) -> int:
    cfg = parse_args(argv)
    if cfg.prompt_login:
        token = prompt_admin_login(cfg.space_api)
    else:
        token = os.environ.get(cfg.token_env, "")
        if not token:
            print(
                f"error: admin bearer token not set in ${cfg.token_env}. "
                "Either export it, or pass --prompt-login to mint one "
                "interactively.",
                file=sys.stderr,
            )
            return 2
    if not cfg.kubectl_context and not os.path.exists(IN_CLUSTER_SA_TOKEN_PATH):
        # Empty --k8s-context is only safe inside a Kubernetes pod, where
        # kubectl uses the ServiceAccount-mounted in-cluster config. Outside a
        # pod, an empty value would fall through to the operator's current
        # kubeconfig context — exactly the foot-gun the explicit-context
        # default was added to prevent. Fail fast.
        print(
            "error: --k8s-context is required when running outside a Kubernetes pod "
            "(no ServiceAccount token at "
            f"{IN_CLUSTER_SA_TOKEN_PATH})",
            file=sys.stderr,
        )
        return 2
    aws = make_aws_clients(cfg.aws_region)
    api = AdminApi(cfg.space_api, token, dry_run=cfg.dry_run)
    return run_loop(cfg, aws, api)


if __name__ == "__main__":
    sys.exit(main())
