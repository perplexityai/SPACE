"""The node drain's v2 engine, proven on a real cluster.

A v2 drain seals the worker under its generation, and the worker's own
sandbox-manager makes every resident durable (`DrainNode`): a paused one is
suspended at once, and a running one keeps serving until the drain's
deadline. The deadline is the control plane's (`drain_started_at` plus the
hold, `deadline_at`): each controller call says whether it has passed
(`evacuate`), and from the first call that says so the node suspends its
running sandboxes too. Each running or paused sandbox ends `suspended` and
unplaced, and nothing brings it back: the next user call does, through its
wake policy, wherever the scheduler places it. The controller writes no
sandbox row while it drains. The node's report is rebuilt from its
sandboxes each call, and the drain is finished exactly when it lists
nothing pending and nothing blocked; once `drained`, the controller stops
calling the node and adds every durably suspended row still homed on it to
`evacuated_total`, which keeps every sandbox it recorded as evacuated. The
terminate transaction releases the suspended ones.

A v2 drain never moves a stopped sandbox, and never terminates a node that
holds one. `:drain` refuses to start one on a node whose preview lists a
sandbox Postgres holds stopped; while a drain lasts, its cancel pending
included, a stop, a kill or a rollback without restart of a sandbox on the
node is refused with the gate's retryable 503; and a stopped sandbox that
appears anyway, on Kubernetes a stop that raced the API's check, is
reported pending, waiting on nothing the drain drives, so the drain never
reaches `drained` until it is deleted. (A VM that exits on its own gets a
forced cleanup with `keep_filesystem: false`, which leaves no stopped
sandbox.) So no scenario
drains a stopped resident.

The scenarios pin residents to one worker by cordoning the other,
uncordon it, and drain the first with `engine: v2` and a hold (`drain`, 0 by
default: due now). Every run arms, before the drain, the undrain and the
uncordon of the worker; only the control plane may call a node's
`DrainNode`, and the undrain's cancel unseals the node, after which the
controller uncordons it. An undrain hands nothing back: a resident already
suspended stays suspended until a user call wakes it.

Kind renders `SPACE_DRAIN_V2_ENABLED` on for the API and the controller
through its own values (`values-kind.yaml`), and the controller's
`SPACE_DRAIN_V2_TEST_*` bounds with it, so a run changes no Deployment to
drain: each batch of runs checks the switches once (`require_drain_v2`). A
run whose drain needs EC2's answers, a terminate or a let-go, scopes the
`aws` terminator to itself (`v2_controller(aws=True)`), under the
controller-mutation Lease, with the gone-node loops a let-go node is handed
to when the run asks for them; Kind's other drains run `kube-delete`. A
run that waits a stall out scopes a shorter stall bound to itself the same
way (`v2_controller(stall_bound=...)`). The
evacuation cap is the API's, read where `:drain` is served, so a run that
needs it scopes it to the public API Deployment (`evacuation_cap`), under
the public-API mutation Lease.

A v2 drain needs the node's EC2 instance recorded (the drain terminates that
instance, and asks EC2 about it once the node stops answering), so every run
gives the drained worker a fake AWS providerID first, before any resident
lands on it. A terminate then runs as production's does: the controller
fences the instance, and the `aws` terminator asks Kind's fake EC2 to
terminate it. The fake only marks it, `shutting-down` for the delay a run
sets (`fake_ec2_delay`) and `terminated` after; once the controller records
the dispatch, the harness plays the cloud's part, stopping the worker's
container and deleting its Node. Once EC2 says the instance is terminated
and the Node is gone, the drain deletes the node's row, with no tombstone.
The worker comes back under the same name with a new providerID, a
same-name replacement, started from the stopped container with an empty
sandbox-manager database, as a new machine starts; that start is armed
before the terminate. A worker that dies mid-drain is let go once EC2 says its
instance is terminated, and the gone-node loops take its rows as any
crashed node's.

The API shows no node's seal. A run reads the seal from what it does show:
the controller stored a report under the drain's generation (`reported_at`),
an activation answered 503, and, once a drain is cancelled or its worker
replaced, a preview that succeeds, which a sealed node refuses.

Every name, code and text the scenarios read of the drain contract is in
one block below ("The drain contract, as built"). A pending entry's
`reason` is display text: no scenario reads it.
"""

from __future__ import annotations

import datetime
import re
import secrets
import shlex
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, cast

from space_smoke.capabilities import (
    CallAnswer,
    CommandSandbox,
    DrainV2Control,
    DrainV2Status,
    DurableLifecycleSandbox,
    KindWorkerControl,
    RawCallSandbox,
    ResidentLifecycleSandbox,
    SandboxRowState,
    SnapshotRecord,
)
from space_smoke.runtime import (
    Compensation,
    InconclusiveSmoke,
    ResourceScope,
    primary_failure,
    run_concurrently,
)

T = TypeVar("T")

# The node-drain terminator: `aws` gives the v2 drain its terminate, and
# EC2's answer for the instance, through Kind's fake EC2.
TERMINATOR_KIND_ENV = "SPACE_NODE_DRAIN_TERMINATOR_KIND"
# Whether `:drain` may start a v2 drain: read by the API, and checked by the
# controller against the node drain it runs in. Kind renders it on for both
# through its values; no run changes it.
DRAIN_V2_ENABLED_ENV = "SPACE_DRAIN_V2_ENABLED"
# How many v2 drains may evacuate their nodes at once.
DRAIN_V2_MAX_EVACUATING_ENV = "SPACE_DRAIN_V2_MAX_EVACUATING_NODES"
# The gone-node loops a node the drain lets go is handed to, off by default:
# `lost_started_sandboxes`, `orphaned_nodes`, and `orphaned_sandboxes` with
# its dry run off.
LOST_STARTED_ENABLED_ENV = "SPACE_LOST_STARTED_SANDBOXES_ENABLED"
ORPHANED_NODES_ENABLED_ENV = "SPACE_ORPHANED_NODES_ENABLED"
ORPHANED_SANDBOXES_ENABLED_ENV = "SPACE_ORPHANED_SANDBOX_RECONCILER_ENABLED"
ORPHANED_SANDBOXES_DRY_RUN_ENV = "SPACE_API_RECONCILER_DRY_RUN"

# The only public API env a drain v2 scenario may touch: the API counts the
# evacuating drains when it answers `:drain`.
DRAIN_V2_API_ENV_KEYS: tuple[str, ...] = (DRAIN_V2_MAX_EVACUATING_ENV,)
# The gone-node loops on, writing: what a scenario that hands a dead node to
# them scopes to itself.
LEGACY_LOOPS_ENV = {
    LOST_STARTED_ENABLED_ENV: "true",
    ORPHANED_NODES_ENABLED_ENV: "true",
    ORPHANED_SANDBOXES_ENABLED_ENV: "true",
    ORPHANED_SANDBOXES_DRY_RUN_ENV: "false",
}
_TERMINATOR_VALUES = frozenset({"aws", "kube-delete"})
_FLAG_VALUES = frozenset({"true", "false"})
_COUNT_VALUE = re.compile(r"[1-9][0-9]{0,2}")
# A scoped stall bound, in whole seconds: at least 10.
_STALL_BOUND_VALUE = re.compile(r"[1-9][0-9]{1,2}")

# The controller config a run whose drain needs EC2's answers scopes to
# itself: the `aws` terminator, which alone gives the v2 loop EC2 and Auto
# Scaling (Kind's fake EC2). Under Kind's standing `kube-delete` the loop
# asks EC2 nothing, so it neither terminates nor lets a node go.
AWS_TERMINATOR_ENV = {TERMINATOR_KIND_ENV: "aws"}
# The controller's v2 bounds, each a test hook in whole seconds that Kind
# renders through its values (`testHookEnv`); unset, the release value.
STALL_BOUND_HOOK = "SPACE_DRAIN_V2_TEST_STALL_BOUND_SECONDS"
STILL_SUSPENDED_AFTER_HOOK = "SPACE_DRAIN_V2_TEST_STILL_SUSPENDED_AFTER_SECONDS"
TERMINATE_BOUND_HOOK = "SPACE_DRAIN_V2_TEST_TERMINATE_BOUND_SECONDS"
DISPATCH_BACKOFF_BASE_HOOK = "SPACE_DRAIN_V2_TEST_DISPATCH_BACKOFF_BASE_SECONDS"
DRAIN_V2_HOOK_KEYS: tuple[str, ...] = (
    STALL_BOUND_HOOK,
    STILL_SUSPENDED_AFTER_HOOK,
    TERMINATE_BOUND_HOOK,
    DISPATCH_BACKOFF_BASE_HOOK,
)
RELEASE_STALL_BOUND_SECONDS = 15 * 60
# The only controller env a drain v2 scenario may touch, and the closed
# vocabulary its overrides come from.
DRAIN_V2_ENV_KEYS: tuple[str, ...] = (
    TERMINATOR_KIND_ENV,
    LOST_STARTED_ENABLED_ENV,
    ORPHANED_NODES_ENABLED_ENV,
    ORPHANED_SANDBOXES_ENABLED_ENV,
    ORPHANED_SANDBOXES_DRY_RUN_ENV,
    # A run that waits a stall out shortens it for itself (`v2_controller`).
    STALL_BOUND_HOOK,
)
RELEASE_DISPATCH_BACKOFF_BASE_SECONDS = 10
# The controller's cap on a fenced node's EC2 check backoff.
DISPATCH_BACKOFF_CAP_SECONDS = 120
_FAKE_PROVIDER_PREFIX = "aws:///kind-a/"
_FAKE_INSTANCE = re.compile(r"i-[0-9a-f]{17}")
_FAKE_PROVIDER = re.compile(r"aws:///kind-a/(i-[0-9a-f]{17})")


# --- How long each wait may take ------------------------------------------
#
# A wait's timeout is the fixed timers it waits out, about four times the
# real work it waits for as measured on a local Kind bench, and a margin: a step that hangs fails within a minute or
# two of when it should have ended, not ten minutes later. None is shorter
# than the longest wait it can contain, such as one `DrainNode` call's own
# budget after a controller tick, or the upload worker's longest retry
# backoff. Windows a check watches to see that something does *not* happen
# are not these: each scenario keeps its own.

# The controller's node-drain tick (`node_drain::DEFAULT_TICK`).
TICK_SECONDS = 5
# One `DrainNode` call's own budget on the node (`DRAIN_CALL_TIMEOUT`).
CALL_BUDGET_SECONDS = 45
# How often a run reads the admin view while it waits for a report.
REPORT_POLL_SECONDS = 2
# The next report the controller stores: a tick, a call within its budget,
# a poll and 10 s.
REPORT_SECONDS = TICK_SECONDS + CALL_BUDGET_SECONDS + REPORT_POLL_SECONDS + 10
# A drain of up to three small residents to `drained`, due now: the next
# report, and their suspends and uploads (8 s for three measured, so 10 s),
# four times over, with a 30 s margin. A run that drains more sizes its own
# from this rule.
DRAINED_SECONDS = REPORT_SECONDS + 4 * 10 + 30
# A resident's state once the drain is `drained`, or once the node says it
# is suspended: polls of rows already written (under 5 s measured).
SETTLED_SECONDS = 60
# A wake to `started` elsewhere, a restore from a durable snapshot (2-10 s
# measured), four times over, with a 30 s margin.
WAKE_SECONDS = 4 * 10 + 30
# The node's own suspend past the deadline: the next report and the suspend
# and upload (about 10 s measured), four times over, with a 30 s margin.
NODE_SUSPEND_SECONDS = REPORT_SECONDS + 4 * 10 + 30
# An upload the node retried while its path was cut, once the path is back:
# the upload worker's longest retry backoff (300 s,
# `crates/l10_foundation/common/jobs/src/worker.rs`), an attempt, a report
# and a minute's margin.
UPLOAD_RETRY_SECONDS = 300 + 30 + REPORT_SECONDS + 60
# The undrain's cancel and uncordon: the next report and the cancel call
# (3-5 s measured), with a 20 s margin.
CANCEL_SECONDS = REPORT_SECONDS + 20
# A worker re-registered under a new providerID, its node services Ready and
# its row recording the instance (35-40 s measured), four times over.
REGISTER_SECONDS = 4 * 40
# A Kind worker started again as a same-name replacement (77 s measured),
# four times over.
COME_BACK_SECONDS = 4 * 80
# A gated activation, retried by the client until the drain made the
# sandbox durable and the wake restored it (13-49 s measured), four times
# over, with a minute's margin.
ACTIVATION_SECONDS = 4 * 50 + 60
# A stop or kill refused for the dispatcher's whole retry budget: 16 attempts
# after the gate's Retry-After (`public_operation_driver.rs`), about 150 s
# measured, with a two-minute margin.
REFUSED_STOP_SECONDS = 150 + 120
# How long a run waits, as it pins its workers, for sandboxes an earlier
# run deleted to leave them: a delete lands in seconds; two minutes.
LEFTOVER_SECONDS = 120
# A command on a sandbox whose data plane is coming up, or a memory fill of
# up to 256 MiB (seconds measured), with a minute's margin.
COMMAND_SECONDS = 90
# The node-row states of a running sandbox a drain holds: `started`, or a
# checkpoint it passes through on the way back to `started` (sandbox-manager's
# state machine has no checkpoint-to-suspend edge).
HELD_NODE_STATES = frozenset({"started", "checkpointing"})
# One probe of a running sandbox around its deadline: the node's own state,
# a command, and the node's state again. About 0.4 s measured on Kind
# (26-30 probes in the 12 s before a deadline); 4 s allows for a bench ten
# times slower.
PROBE_SECONDS = 4
# How long before the deadline the probes start. The first one's call is
# asked up to a probe after they start, so at least `2 * PROBE_SECONDS`
# before the deadline: room for two calls to meet the VM held there even at
# 4 s a probe.
CALLS_LEAD_SECONDS = 3 * PROBE_SECONDS
# How far past one of the controller's ticks `bring_deadline_to(...,
# after_a_tick=True)` puts a deadline: the stored deadline may land a second
# either side of the one asked for (`expect_stored_deadline`), and a tick's
# stamp is its whole second, so 2 s keeps the deadline after that tick and
# the next one, which evacuates, 2-5 s past the deadline.
AFTER_A_TICK_SECONDS = 2


# How many independent per-resident calls (creates, fills, wakes, deletes) a
# run makes at once: enough to overlap their round trips, few enough that a
# worker's admission and the shared upload lane don't refuse or queue them.
PARALLEL_WIDTH = 4


def in_parallel(calls: Sequence[Callable[[], T]], *, width: int = PARALLEL_WIDTH) -> list[T]:
    """Run independent calls `width` at a time; their results in order. Once
    a batch ends with a failure, the first verdict it raised (else the first
    error) is raised, with the evidence that call recorded, and no later
    batch starts. An interrupted batch ends its calls before the interrupt
    goes on to cleanup (`run_concurrently`), and a resident a create hands
    out is owned by the run's cleanup from the moment the API accepts it."""

    results: list[T] = []
    for start in range(0, len(calls), width):
        outcomes = run_concurrently(calls[start : start + width])
        failure = primary_failure(outcomes)
        if failure is not None:
            raise failure
        results.extend(cast(list[T], outcomes))
    return results


def validate_fake_instance_id(instance_id: str) -> None:
    if _FAKE_INSTANCE.fullmatch(instance_id) is None:
        raise ValueError(f"refusing instance id {instance_id!r}")


def validate_fake_provider_id(provider_id: str) -> None:
    if _FAKE_PROVIDER.fullmatch(provider_id) is None:
        raise ValueError(f"refusing providerID {provider_id!r}")


def fake_provider_id() -> tuple[str, str]:
    """A new fake AWS providerID and the instance it names."""

    instance_id = "i-" + secrets.token_hex(9)[:17]
    return f"{_FAKE_PROVIDER_PREFIX}{instance_id}", instance_id


def await_recorded_instance(
    fixture: ScenarioFixture, control: KindWorkerControl, node_name: str, node_id: str, instance_id: str,
    *, timeout: float,
) -> None:
    """Wait until the node's row records `instance_id`."""

    deadline = time.monotonic() + timeout
    while control.node_recorded_instance(node_id) != instance_id:
        fixture.verify(
            time.monotonic() < deadline,
            "node registration never recorded the worker's fake instance",
            worker=node_name,
            expected=instance_id,
            recorded=control.node_recorded_instance(node_id),
        )
        fixture.wait(3)


def register_fake_instance(
    fixture: ScenarioFixture, control: KindWorkerControl, node_name: str, node_id: str,
    *, timeout: float = REGISTER_SECONDS,
) -> str:
    """Re-register the worker under a new fake AWS providerID and wait until
    its row records the instance; returns it. Re-registering deletes the
    pods on the worker."""

    provider_id, instance_id = fake_provider_id()
    labels = control.kind_node_labels(node_name)
    control.set_kind_worker_provider_id(node_name, provider_id, labels, timeout=timeout)
    await_recorded_instance(fixture, control, node_name, node_id, instance_id, timeout=timeout)
    return instance_id


@dataclass(frozen=True)
class DrainV2Gate:
    """What a batch of drain v2 runs found standing: the switch on both the
    API that answers `:drain` and the controller, and the controller's v2
    bounds."""

    stall_bound_seconds: int
    dispatch_backoff_base_seconds: int


# The gate a batch passed, once: every run in one process shares it.
_GATE: DrainV2Gate | None = None


def hook_seconds(values: Mapping[str, str | None], key: str, default: int) -> int:
    """A test hook's whole seconds as the controller reads it: unset, zero or
    unreadable keeps `default`."""

    raw = values.get(key)
    try:
        seconds = int(raw) if raw is not None else 0
    except ValueError:
        seconds = 0
    return seconds if seconds > 0 else default


def require_drain_v2(fixture: ScenarioFixture, control: DrainV2Control) -> DrainV2Gate:
    """Check, once per batch, that the bench's values switched drain v2 on
    for the API and the controller, and read the controller's v2 bounds."""

    global _GATE
    if _GATE is not None:
        return _GATE
    switches = control.drain_v2_switches()
    fixture.record("drain_v2_switches", switches)
    fixture.verify(
        all(value == "true" for value in switches.values()) and bool(switches),
        f"drain v2 is off on this bench: {DRAIN_V2_ENABLED_ENV} must be \"true\" on the API and "
        "the controller, which only Kind's values-kind.yaml sets",
        switches=switches,
    )
    hooks = control.drain_v2_test_hooks()
    fixture.record("drain_v2_test_hooks", hooks)
    _GATE = DrainV2Gate(
        stall_bound_seconds=hook_seconds(hooks, STALL_BOUND_HOOK, RELEASE_STALL_BOUND_SECONDS),
        dispatch_backoff_base_seconds=min(
            hook_seconds(
                hooks, DISPATCH_BACKOFF_BASE_HOOK, RELEASE_DISPATCH_BACKOFF_BASE_SECONDS
            ),
            DISPATCH_BACKOFF_CAP_SECONDS,
        ),
    )
    return _GATE


@contextmanager
def scoped_drain_v2_env(
    fixture: ScenarioFixture, control: KindWorkerControl, scope: ResourceScope
) -> Iterator[Callable[[Mapping[str, str]], None]]:
    """Hold the controller-mutation Lease, capture the standing drain v2 env,
    and put it back when the body ends; yields the setter, so the body can
    switch configs inside the scope."""

    release = control.acquire_controller_mutation()
    lease = scope.defer("release the controller mutation lease", release)
    standing = control.drain_v2_env()
    fixture.record("standing_drain_v2_env", standing)
    restore = scope.defer(
        "put the standing drain v2 env back",
        lambda: control.restore_drain_v2_env(standing),
        critical=True,
    )
    try:
        yield control.set_drain_v2_env
    finally:
        with fixture.phase("put the standing drain v2 env back"):
            restore.release()
            lease.release()


def drain_status_evidence(status: DrainV2Status | None) -> dict[str, Any] | None:
    """A node's drain v2 columns as evidence carries them."""

    return None if status is None else {"engine": status.engine, "status": status.status}


def validate_drain_v2_keys(values: Mapping[str, object]) -> None:
    unknown = sorted(set(values) - set(DRAIN_V2_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing drain v2 env keys {unknown}")


def validate_drain_v2_overrides(overrides: Mapping[str, str]) -> None:
    validate_drain_v2_keys(overrides)
    for key, value in overrides.items():
        if key == TERMINATOR_KIND_ENV:
            allowed = value in _TERMINATOR_VALUES
        elif key == STALL_BOUND_HOOK:
            allowed = _STALL_BOUND_VALUE.fullmatch(value) is not None
        else:
            allowed = value in _FLAG_VALUES
        if not allowed:
            raise ValueError(f"refusing {key}={value!r}")


def validate_drain_v2_api_keys(values: Mapping[str, object]) -> None:
    unknown = sorted(set(values) - set(DRAIN_V2_API_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing drain v2 public API env keys {unknown}")


def validate_drain_v2_api_overrides(overrides: Mapping[str, str]) -> None:
    validate_drain_v2_api_keys(overrides)
    for key, value in overrides.items():
        if _COUNT_VALUE.fullmatch(value) is None:
            raise ValueError(f"refusing {key}={value!r}")


# --- The drain contract, as built ------------------------------------------
#
# Every name, code, status and text these scenarios read of the drain
# contract is here, so a change to the contract changes them in one place.

# `:drain`'s body for a v2 drain: `{"engine": "v2", "hold_seconds": N}`, N
# whole seconds counted from `drain_started_at`, 0 for due now.
DRAIN_HOLD_FIELD = "hold_seconds"
# A repeat `:drain` that would make the deadline later.
LENGTHENED_DEADLINE_STATUS = 409
# `mode: forced` beside `engine: v2`.
MODE_WITH_V2_STATUS = 400
# A `:drain` at `SPACE_DRAIN_V2_MAX_EVACUATING_NODES`: a 409 whose JSON body
# carries this stable code under this key, with this `Retry-After`.
CAP_REFUSAL_STATUS = 409
CAP_REFUSAL_CODE_FIELD = "code"
CAP_REFUSAL_CODE = "drain_v2_evacuation_cap"
CAP_RETRY_AFTER = "60"
# A v1 `:drain` (no engine, or `engine: v1`) while drain v2 is on: v1 and v2
# drains never run together. A 409 with this stable code under the cap's key.
V1_REFUSAL_STATUS = 409
V1_REFUSAL_CODE = "drain_v1_refused_while_v2_enabled"
# A v2 `:drain` that would start a drain on a node whose own preview lists a
# sandbox Postgres holds `stopped`, which a v2 drain doesn't move: a 409 with
# this stable code under the cap's key, the count under `stopped` and their
# ids under `stopped_sandbox_ids`; nothing is written. A repeat of a drain in
# flight doesn't preview, and a preview that can't be read lets the drain
# start.
STOPPED_REFUSAL_STATUS = 409
STOPPED_REFUSAL_CODE = "drain_v2_stopped_sandboxes"
STOPPED_REFUSAL_COUNT_FIELD = "stopped"
STOPPED_REFUSAL_IDS_FIELD = "stopped_sandbox_ids"
# `nodes.drain_status` keys (Unix seconds by the database's clock where a
# time): the drain's `generation` (`drain_started_at` in microseconds), its
# deadline (`deadline_at`, `drain_started_at` plus the hold), every sandbox
# the controller recorded as evacuated (`evacuated_total`, append-only), the
# fenced instance and the dispatch of its termination, and the last report
# stored for the generation (`report`, with `reported_at` and the stall's
# stamp `evacuated_grew_at`).
STATUS_GENERATION_FIELD = "generation"
STATUS_DEADLINE_FIELD = "deadline_at"
STATUS_EVACUATED_TOTAL_FIELD = "evacuated_total"
STATUS_FENCE_FIELD = "terminate_instance"
STATUS_DISPATCHED_FIELD = "dispatched_at"
# The admin node response's read-only view of a v2 drain (GET
# /admin/api/v1/nodes/{id}, `AdminNodeDrain`): `generation`, `deadline`
# (RFC 3339), `cancelling`, `fenced`, `reported_at` and `evacuated_grew_at`
# (RFC 3339, set once the controller stored a report under the generation),
# `evacuated_total`, and the last report's `evacuated` (each `sandbox_id`,
# `artifact_kind`, `was_running`, `recorded`), `pending` (each `sandbox_id`,
# `wait`, `running`, `pool_member`, `reason`, `since`) and `blocked` (each a
# node-wide `reason`). Absent for a node with no v2 drain. The drain is
# finished exactly when `pending` and `blocked` are both empty.
DRAIN_VIEW_FIELD = "drain"
VIEW_GENERATION_FIELD = "generation"
VIEW_DEADLINE_FIELD = "deadline"
VIEW_REPORTED_FIELD = "reported_at"
VIEW_GREW_AT_FIELD = "evacuated_grew_at"
VIEW_EVACUATED_TOTAL_FIELD = "evacuated_total"
VIEW_CANCELLING_FIELD = "cancelling"
# An evacuated entry's artifact kind: every resident a v2 drain evacuates
# is a suspended snapshot (`full`); proto spellings (`ARTIFACT_KIND_FULL`)
# read too.
EVACUATED_KIND_FIELD = "artifact_kind"
EVACUATED_FULL = "full"
# A pending entry's coarse wait: `hold` (kept running until the drain
# evacuates), `moving` (the drain is making it durable: a suspend, a
# publication, an upload, or waiting for a window slot or disk room for one)
# or `external` (work the drain doesn't drive: an activation or a user's
# snapshot in flight, a transition that isn't moving, and every stopped
# sandbox, which the drain never moves, not running); `unspecified` for one
# this build doesn't know. `running`: a started VM at the node's planning
# read. `since`: when its row entered its state, by the node's clock, for
# display only.
PENDING_WAIT_FIELD = "wait"
PENDING_RUNNING_FIELD = "running"
PENDING_SINCE_FIELD = "since"
WAIT_HOLD = "hold"
WAIT_MOVING = "moving"
WAIT_EXTERNAL = "external"
# The stall: past the stored deadline, a drain that isn't finished and
# whose evacuated set hasn't grown for the controller's stall bound (15
# minutes; Kind's values shorten it, `require_drain_v2`). `drain_last_error`
# then starts with this text; the stamp it is measured from is
# `evacuated_grew_at`, never before the deadline. Before the deadline a
# drain is never stalled.
STALL_ERROR_PREFIX = "the drain is stalled"
# The sealed node's refusal of an activation: a retryable 503 with
# `Retry-After`, never a 409. A lifecycle call refused before it is accepted
# answers it directly; one the dispatcher retries says so in its
# operation's `progress_message`.
GATE_REFUSAL_STATUS = 503
GATE_RETRY_TEXT = "retry shortly"
# An operation the dispatcher retried on that refusal until its retry budget
# ran out fails with this `terminal_error.reason`; its client asks again.
RETRIES_EXHAUSTED_REASON = "retry_attempts_exhausted"
# With drain v2 on, a stop or kill of a sandbox whose node, active or target,
# is under a v2 drain, in any phase, its cancel pending included: the user's
# `:stop` and `:kill` and an operator's `:force-stop` and `:force-kill`. Each
# is refused with the gate's 503 and `Retry-After`, said in the operation's
# `progress_message` with this text while the dispatcher retries it, until
# its retry budget runs out; once the node is undrained the call goes
# through. A delete is not refused.
STOP_VERBS: tuple[str, ...] = ("stop", "kill", "force-stop", "force-kill")
ADMIN_ACTION_PREFIX = "force-"
STOP_REFUSAL_TEXT = "retry once the node is undrained"
# What a gate's 409 says, in an operation's progress or terminal error code,
# which a sealed node's activation must never meet.
GATE_CONFLICT_TEXT = "migrating it off"
GATE_CONFLICT_CODE = "conflict"
# A command a user sends a running sandbox on a draining node passes, before
# the deadline and after it, until the node suspends the sandbox.
CALL_PASSED_STATUS = 201
# Rollback on a sealed node (E10), as `space-sbxmgr` prints tonic's status:
# refused up front with `FailedPrecondition`, whose description it prints,
# carrying the sealed marker every sealed refusal carries. A late refusal,
# the seal landing after that check, means the rollback was applied: the
# same code without the marker, saying the sandbox is stopped, for the
# client to start it.
ROLLBACK_REFUSAL_CODE = "The system is not in a state required for the operation's execution"
ROLLBACK_SEALED_MARKER = "node activations are sealed"
ROLLBACK_LATE_TEXT = "the sandbox is stopped"
# The preview (GET .../drain-v2-preview) answers the node's report: the
# entries under the stored report's keys. A node with a v2 drain recorded, a
# cancel pending included, is refused 409 with `drain_v2_in_progress`; a
# node sealed with no drain recorded 409 with `orphaned_seal`. A node busy
# with another drain call answers `ABORTED`, which the admin API reports as
# a 503 with `node_busy` and a `Retry-After`: retryable contention that
# changed nothing, so the harness asks again, within
# PREVIEW_BUSY_RETRY_SECONDS.
PREVIEW_REFUSED_STATUS = 409
PREVIEW_IN_PROGRESS_REASON = "drain_v2_in_progress"
PREVIEW_ORPHANED_SEAL_REASON = "orphaned_seal"
PREVIEW_BUSY_STATUS = 503
PREVIEW_BUSY_REASON = "node_busy"
PREVIEW_BUSY_RETRY_SECONDS = 60
# Once the drain is drained, a suspended resident's row is unplaced: it
# names no active node (`expect_suspended_unplaced`).

# A wake policy under which the next call to a suspended sandbox wakes it
# (`wake.on: ["access"]`); a Native sandbox created without one is not
# woken by a call.
ACCESS_WAKE_POLICY: dict[str, object] = {
    "wake": {"on": ["access"]},
    "condition_sets": {"access_idle": {"kind": "inactivity", "activity_source": "access"}},
    "stages": [{"condition_set": "access_idle", "at": "21600s", "action": "suspend"}],
}


# How long after the fake EC2's delay a scenario waits to see that nothing
# deleted a replacement's row: the v2 loop's first four dispatch backoffs
# (1 + 2 + 4 + 8 times the base), within these bounds.
DELETE_SETTLE_MIN_SECONDS = 30
DELETE_SETTLE_MAX_SECONDS = 90
# The sandbox-manager row states in which a node may hold the sandbox's VM.
_VM_STATES = frozenset({"started", "paused", "pausing", "resuming", "starting", "suspending"})
_TERMINAL_OPERATION_STATES = frozenset({"succeeded", "failed", "canceled", "expired"})
# The node rows of a suspend still running: its teardown, or the cleanup that
# owns it.
_SUSPENDING_STATES = frozenset({"suspending", "cleanup_required"})
# Guest memory a resident holds in tmpfs, random so its snapshot can't shrink it.
BALLAST = "/dev/shm/space-smoke-v2-ballast"
BALLAST_DIGEST = f"md5sum {BALLAST} | cut -d' ' -f1"

if TYPE_CHECKING:
    from space_smoke.fixtures import CommandSandboxFixture, ScenarioFixture


def timestamp_seconds(value: object) -> float | None:
    """A report timestamp in Unix seconds: a number as it is, an RFC 3339
    string parsed; None for anything else."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value).timestamp()
        except ValueError:
            return None
    return None


def artifact_kind(value: object) -> str:
    """An evacuated entry's artifact kind, normalized: lowercase, without the proto prefix."""

    return str(value).lower().removeprefix("artifact_kind_")


def retries_exhausted_on_the_gate(operation: Mapping[str, Any]) -> bool:
    """The operation failed only because the dispatcher's retry budget ran
    out on the gate's retryable refusal: its terminal error says the retries
    were exhausted, and its last progress is the gate's retry."""

    error = operation.get("terminal_error")
    return (
        operation.get("state") == "failed"
        and isinstance(error, Mapping)
        and error.get("reason") == RETRIES_EXHAUSTED_REASON
        and GATE_RETRY_TEXT in str(operation.get("progress_message") or "")
    )


def stop_refused_by_the_drain(answer: Mapping[str, Any]) -> bool:
    """A stop or kill (`DrainV2Assertions.request_stop`) the v2 drain
    refused: answered with the gate's 503, a `Retry-After` and the drain's
    refusal in its `error` before anything was accepted, or accepted and
    retried on the drain's refusal until the dispatcher's budget ran out.
    Never one that succeeded, failed for anything else, or is still running,
    nor a 503 the drain didn't send, such as a busy API's."""

    refusals = list(answer.get("refusals") or [])
    if answer.get("operation_id") is None:
        return bool(refusals) and all(
            isinstance(refusal, Mapping)
            and refusal.get("status") == GATE_REFUSAL_STATUS
            and bool(refusal.get("retry_after"))
            and isinstance(refusal.get("body"), Mapping)
            and STOP_REFUSAL_TEXT in str(refusal["body"].get("error") or "")
            for refusal in refusals
        )
    operation = answer.get("operation")
    if not isinstance(operation, Mapping):
        return False
    error = operation.get("terminal_error")
    return (
        operation.get("state") == "failed"
        and isinstance(error, Mapping)
        and error.get("reason") == RETRIES_EXHAUSTED_REASON
        and any(STOP_REFUSAL_TEXT in str(message) for message in answer.get("progress") or [])
    )


def refused_as_suspended(answer: CallAnswer) -> bool:
    """A command the API refused because its sandbox is suspended and its
    policy wakes on no call: the native `409` `conflict` whose details say
    `sandbox_not_running`, `suspended`, and name the `:wake` to make."""

    details = {
        detail.get("key"): detail.get("value")
        for detail in answer.body.get("details") or []
        if isinstance(detail, Mapping)
    }
    return (
        answer.status == 409
        and answer.body.get("code") == "conflict"
        and details.get("reason") == "sandbox_not_running"
        and details.get("state") == "suspended"
        and str(details.get("wake") or "").endswith(":wake")
    )


class DrainV2Assertions:
    """Drain the fixture sandbox's worker with the v2 engine and judge what follows."""

    def __init__(
        self,
        fixture: CommandSandboxFixture,
        control: DrainV2Control,
        create_sandbox: Callable[..., DurableLifecycleSandbox],
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
        self._generation: int | None = None
        self._hold: int | None = None
        self._deadline: int | None = None
        self._instance: str | None = None
        self._replacement: str | None = None
        # Whether this run's own cordon of this worker stands, so the
        # undrain's uncordon is not waited for while it does.
        self._cordoned_by_run = False
        # Whether this run holds the controller-mutation Lease.
        self._controller_lease = False
        # Each resident a report this run read named evacuated, with its kind.
        self._reported_kinds: dict[str, str] = {}
        # The stall bound `v2_controller` scoped to this run, if any.
        self._stall_bound: int | None = None

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
    def generation(self) -> int:
        if self._generation is None:
            raise RuntimeError("drain() must run first")
        return self._generation

    @property
    def gate(self) -> DrainV2Gate:
        """The batch's drain v2 gate (`require_drain_v2`)."""

        return require_drain_v2(self._fixture, self._control)

    @property
    def stall_bound_seconds(self) -> int:
        """The controller's stall bound for this run: the one `v2_controller`
        scoped to it, else the batch's."""

        return self._stall_bound if self._stall_bound is not None else self.gate.stall_bound_seconds

    @property
    def deadline(self) -> int:
        """The drain's stored deadline, in Unix seconds."""

        if self._deadline is None:
            raise RuntimeError("drain() must run first")
        return self._deadline

    def seconds_to_deadline(self) -> float:
        return self.deadline - time.time()

    # --- placement ------------------------------------------------------------

    def pin_to_resident_worker(self, *, away_from_fixture: bool = False) -> str:
        """Hold the worker-cordon Lease, record the fixture's worker and cordon
        the other one; with `away_from_fixture`, the other way round, so the
        drained worker hosts no fixture pod."""

        for node in self._control.admin_nodes():
            self._fixture.verify(
                node.drain_phase is None and node.drain_engine is None,
                "a node is mid-drain or its undrain hasn't finished; the v2 drain scenarios need a "
                "clean fleet",
                node=node.k8s_node_name,
                drain_phase=node.drain_phase,
                drain_engine=node.drain_engine,
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
        self._expect_no_leftovers(workers, self.bare_id(self._fixture.sandbox.sandbox_id))
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

    def _expect_no_leftovers(self, workers: Sequence[str], fixture_id: str) -> None:
        """Neither worker holds a sandbox a drain would wait on but the
        fixture's: each one's preview lists nothing else pending. A sandbox
        left there would be counted in this run's reports and fail it for
        something it didn't do; so, once the deletes still landing have had
        `LEFTOVER_SECONDS`, the run ends inconclusive with what it saw in
        `leftover_sandboxes`, as it does when a preview can't be read for as
        long. Each leftover is classed by its control-plane row: one still
        live is the bench's (an earlier run's, such as one whose cleanup
        failed, which stays that run's failure); one the control plane has
        deleted or is deleting, which the node still holds, may be the
        product's own orphan, and the verdict says so."""

        nodes = {n.k8s_node_name: n.id for n in self._control.admin_nodes() if n.k8s_node_name in workers}
        deadline = time.monotonic() + LEFTOVER_SECONDS
        while True:
            leftovers: dict[str, list[str]] = {}
            unread: dict[str, str] = {}
            for name, node_id in nodes.items():
                try:
                    pending = self._control.drain_v2_preview(node_id).get("pending") or []
                except Exception as error:  # noqa: BLE001 - a worker still coming up is read again
                    unread[name] = repr(error)[:300]
                    continue
                left = [
                    str(entry.get("sandbox_id"))
                    for entry in pending
                    if isinstance(entry, Mapping) and str(entry.get("sandbox_id")) != fixture_id
                ]
                if left:
                    leftovers[name] = left
            if not leftovers and not unread:
                return
            if time.monotonic() >= deadline:
                break
            self._fixture.wait(5)
        left_ids = [sandbox_id for ids in leftovers.values() for sandbox_id in ids]
        rows = self._control.sandbox_placements(left_ids) if left_ids else {}
        live = sorted(i for i in left_ids if i in rows and rows[i][0] not in ("deleting", "deleted"))
        deleted = sorted(i for i in left_ids if i not in live)
        self._fixture.record(
            "leftover_sandboxes",
            {
                "by_worker": leftovers,
                "unread_previews": unread,
                "rows": {sandbox_id: list(placement) for sandbox_id, placement in rows.items()},
                "live": live,
                "deleted_but_held": deleted,
                "waited_seconds": LEFTOVER_SECONDS,
            },
        )
        found = []
        if live:
            found.append(
                f"{len(live)} live sandbox(es) an earlier run left, such as one whose cleanup "
                "failed: bench hygiene, delete them or bring the bench up again"
            )
        if deleted:
            found.append(
                f"{len(deleted)} sandbox(es) the control plane deleted or is deleting that a "
                "worker still holds: possibly a product orphan, report it before cleaning the bench"
            )
        if unread:
            found.append(
                f"no drain preview could be read from {sorted(unread)}, so the bench's state is unknown"
            )
        raise InconclusiveSmoke(
            f"the bench is not known to be clean after {LEFTOVER_SECONDS} s: "
            + "; ".join(found)
            + " (leftover_sandboxes); this run's drain would count them, so it did not start"
        )

    def give_fake_instance(self, *, timeout: float = REGISTER_SECONDS) -> str:
        """The pinned worker's fake AWS instance: re-register it under a fake
        AWS providerID unless it has one, and wait until its node row records
        the instance. Run before any resident lands on it: re-registering
        deletes the pods on it."""

        node_name, node_id = self.node_name, self.node_id
        provider = self._control.kind_node_provider_id(node_name)
        if provider.startswith(_FAKE_PROVIDER_PREFIX):
            instance = provider.removeprefix(_FAKE_PROVIDER_PREFIX)
            await_recorded_instance(
                self._fixture, self._control, node_name, node_id, instance, timeout=timeout
            )
        else:
            instance = register_fake_instance(
                self._fixture, self._control, node_name, node_id, timeout=timeout
            )
        self._instance = instance
        self._fixture.record("drained_instance", instance)
        return instance

    @contextmanager
    def v2_controller(
        self, *, aws: bool = False, legacy_loops: bool = False, stall_bound: int | None = None
    ) -> Iterator[None]:
        """Run the body on a bench whose values switched drain v2 on, checked
        once per batch. With `aws`, the controller's `aws` terminator, against
        Kind's fake EC2, for a drain that terminates its node or lets it go;
        with `legacy_loops` too, the gone-node loops a let-go node is handed
        to run and write. With `stall_bound`, the stall bound for this run, in
        whole seconds, where Kind's values render the bound's test hook (a
        release build reads none, and keeps its own); `stall_bound_seconds`
        says which holds. Only those change the controller, under its
        mutation Lease; the standing config goes back when the body ends, and
        is checked to be back."""

        self._fixture.record("drain_v2_gate", self.gate.__dict__)
        if legacy_loops and not aws:
            raise ValueError("the gone-node loops take a let-go node: they need aws=True")
        env: dict[str, str] = {}
        if aws:
            env.update(AWS_TERMINATOR_ENV)
        if legacy_loops:
            env.update(LEGACY_LOOPS_ENV)
        if stall_bound is not None and self.gate.stall_bound_seconds != RELEASE_STALL_BOUND_SECONDS:
            env[STALL_BOUND_HOOK] = str(stall_bound)
            self._stall_bound = stall_bound
        self._fixture.record("stall_bound_seconds", self.stall_bound_seconds)
        if not env:
            yield
            return
        if self._controller_lease:
            raise RuntimeError("the run already holds the controller-mutation lease")
        with scoped_drain_v2_env(self._fixture, self._control, self._scope) as set_env:
            self._controller_lease = True
            try:
                set_env(env)
                yield
            finally:
                self._controller_lease = False

    def _hold_controller_lease(self) -> None:
        """Hold the controller-mutation Lease for the rest of the run, unless
        it does already: a restart or scale of the controller needs it."""

        if self._controller_lease:
            return
        release = self._control.acquire_controller_mutation()
        self._scope.defer("release the controller mutation lease", release)
        self._controller_lease = True

    @contextmanager
    def evacuation_cap(self, nodes: int) -> Iterator[None]:
        """Run the body with at most `nodes` v2 drains evacuating at once:
        `SPACE_DRAIN_V2_MAX_EVACUATING_NODES` on the public API Deployment,
        which serves the admin `:drain` and counts the slots when it answers.
        Under the public-API mutation Lease; the standing value goes back
        when the body ends, and is checked to be back."""

        release = self._control.acquire_public_api_mutation()
        lease = self._scope.defer("release the public API mutation lease", release)
        standing = self._control.drain_v2_api_env()
        self._fixture.record("standing_drain_v2_api_env", standing)
        restore = self._scope.defer(
            "put the standing evacuation cap back",
            lambda: self._control.restore_drain_v2_api_env(standing),
            critical=True,
        )
        try:
            self._control.set_drain_v2_api_env({DRAIN_V2_MAX_EVACUATING_ENV: str(nodes)})
            yield
        finally:
            with self._fixture.phase("put the standing evacuation cap back"):
                restore.release()
                lease.release()

    def create_resident(
        self,
        *,
        idle_timeout: str | None = None,
        auto_lifecycle: Mapping[str, object] | None = None,
    ) -> DurableLifecycleSandbox:
        """A run-owned sandbox, running on the pinned worker; with
        `idle_timeout`, one the node suspends once it has been idle that long;
        with `auto_lifecycle`, one under that policy (`ACCESS_WAKE_POLICY`
        wakes it on its next call)."""

        sandbox = self._create_sandbox(idle_timeout=idle_timeout, auto_lifecycle=auto_lifecycle)
        placement = self._control.sandbox_pod_placement(self.bare_id(sandbox.sandbox_id))
        self._fixture.verify(
            placement is not None and placement.node_name == self.node_name,
            "a resident did not land on the pinned worker",
            sandbox_id=sandbox.sandbox_id,
            node=None if placement is None else placement.node_name,
        )
        return sandbox

    def create_residents(
        self,
        count: int,
        *,
        idle_timeout: str | None = None,
        auto_lifecycle: Mapping[str, object] | None = None,
    ) -> list[DurableLifecycleSandbox]:
        """`count` residents (`create_resident`), created a few at a time."""

        return in_parallel(
            [
                lambda: self.create_resident(idle_timeout=idle_timeout, auto_lifecycle=auto_lifecycle)
                for _ in range(count)
            ]
        )

    def fill_memories(
        self, sandboxes: Sequence[DurableLifecycleSandbox], *, mib: int
    ) -> dict[str, str]:
        """`fill_memory` of each sandbox, a few at a time; each digest by sandbox id."""

        digests = in_parallel(
            [lambda sandbox=sandbox: self.fill_memory(sandbox, mib=mib) for sandbox in sandboxes]
        )
        return {sandbox.sandbox_id: digest for sandbox, digest in zip(sandboxes, digests)}

    def pause_all(self, sandboxes: Iterable[DurableLifecycleSandbox]) -> None:
        """Pause each sandbox, a few at a time."""

        in_parallel(
            [lambda sandbox=sandbox: cast(ResidentLifecycleSandbox, sandbox).pause() for sandbox in sandboxes]
        )

    def delete_all(self, sandboxes: Iterable[CommandSandbox]) -> None:
        """Delete each run-owned sandbox, a few at a time."""

        in_parallel([lambda sandbox=sandbox: self._fixture.delete_sandbox(sandbox) for sandbox in sandboxes])

    def pin_residents(
        self, counts: dict[str, int], *, mib: int = 64
    ) -> tuple[dict[str, DurableLifecycleSandbox], dict[str, str], dict[str, str]]:
        """Pin residents to the worker the fixture is not on, under a fake
        instance: `counts` of each lifecycle (`started`, `paused`), each VM
        holding `mib` of random memory first. A v2 drain refuses a worker
        holding a stopped sandbox, so there is no stopped resident. The
        fixture sandbox is suspended and the other worker released. Returns
        the sandboxes, each one's lifecycle and each one's memory digest, by
        sandbox id."""

        unknown = sorted(set(counts) - {"started", "paused"})
        if unknown:
            raise ValueError(f"refusing resident lifecycles {unknown}: a v2 drain pins started and paused ones")
        self.pin_to_resident_worker(away_from_fixture=True)
        self.give_fake_instance()
        created = self.create_residents(sum(counts.values()))
        sandboxes = {sandbox.sandbox_id: sandbox for sandbox in created}
        wanted = [lifecycle for lifecycle, count in counts.items() for _ in range(count)]
        lifecycles = {sandbox.sandbox_id: lifecycle for sandbox, lifecycle in zip(created, wanted)}
        digests = self.fill_memories(created, mib=mib)
        self.pause_all(
            sandbox for sandbox_id, sandbox in sandboxes.items() if lifecycles[sandbox_id] == "paused"
        )
        self._fixture.record(
            "residents",
            {self.bare_id(sandbox_id): lifecycle for sandbox_id, lifecycle in lifecycles.items()},
        )
        cast(DurableLifecycleSandbox, self._fixture.sandbox).suspend()
        self.release_other_worker()
        self.expect_lifecycles(lifecycles)
        return sandboxes, lifecycles, digests

    def run_on(self, sandbox: DurableLifecycleSandbox, command: str, *, timeout: float = COMMAND_SECONDS) -> Any:
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

    @property
    def other_node_name(self) -> str:
        if self._other is None:
            raise RuntimeError("pin_to_resident_worker() must run first")
        return self._other

    def write_file(self, sandbox: DurableLifecycleSandbox, path: str, content: str | None = None) -> None:
        """Write `content`, the sandbox id by default, to `path` in the sandbox."""

        text = sandbox.sandbox_id if content is None else content
        written = self.run_on(sandbox, f"printf %s {shlex.quote(text)} > {shlex.quote(path)}")
        self._fixture.verify(
            written.exit_code == 0,
            "failed to write a resident's file",
            sandbox_id=sandbox.sandbox_id,
        )

    def expect_file(
        self, sandbox: DurableLifecycleSandbox, path: str, content: str | None = None
    ) -> None:
        """`path` in the sandbox holds what `write_file` put there."""

        text = sandbox.sandbox_id if content is None else content
        observed = self.run_on(sandbox, f"cat {shlex.quote(path)}")
        self._fixture.verify(
            observed.exit_code == 0 and observed.stdout == text,
            "a resident's file did not survive the v2 drain",
            sandbox_id=sandbox.sandbox_id,
            expected=text,
            stdout=observed.stdout,
        )

    def fill_memory(self, sandbox: DurableLifecycleSandbox, *, mib: int) -> str:
        """Fill `mib` of guest memory with random tmpfs content; its digest."""

        filled = self.run_on(
            sandbox,
            f"dd if=/dev/urandom of={BALLAST} bs=1M count={int(mib)} 2>/dev/null && {BALLAST_DIGEST}",
            timeout=COMMAND_SECONDS,
        )
        digest = filled.stdout.strip()
        self._fixture.verify(
            filled.exit_code == 0 and len(digest) == 32,
            "failed to fill a resident's memory",
            sandbox_id=sandbox.sandbox_id,
            stdout=filled.stdout,
        )
        return digest

    def expect_memory(self, sandbox: DurableLifecycleSandbox, digest: str) -> None:
        """The resident's memory holds what `fill_memory` put there."""

        observed = self.run_on(sandbox, BALLAST_DIGEST)
        self._fixture.verify(
            observed.exit_code == 0 and observed.stdout.strip() == digest,
            "a resident's memory did not survive the v2 drain",
            sandbox_id=sandbox.sandbox_id,
            expected=digest,
            observed=observed.stdout,
        )

    # --- faults -----------------------------------------------------------------

    def block(self, path: str, node_name: str | None = None) -> Compensation:
        """Cut a node-local path (`object-store` or `node-api`) on the drained
        worker, or `node_name`; the unblock is armed first and returned."""

        node = node_name or self.node_name
        unblock = self._scope.defer(
            f"unblock {path} on {node}",
            lambda: self._control.unblock_node_traffic(node, path),
            critical=True,
        )
        self._control.block_node_traffic(node, path)
        self._fixture.record(f"blocked_{path}_{node}", time.time())
        return unblock

    def restart_controller(self) -> None:
        """Kill the cluster-controller and wait for its replacement."""

        self._hold_controller_lease()
        killed = self._control.restart_controller(timeout=300)
        self._fixture.record("restarted_controller_pods", killed)

    @contextmanager
    def controller_down(self) -> Iterator[None]:
        """Run the body with the cluster-controller scaled to zero; it is scaled back after."""

        self._hold_controller_lease()
        # Armed before the scale-down, so a scale-down that fails after its
        # write still scales the controller back.
        previous = self._control.controller_replicas()

        def scale_back() -> None:
            self._control.scale_controller(previous, timeout=300)

        back = self._scope.defer("scale the cluster-controller back", scale_back, critical=True)
        self._control.scale_controller(0, timeout=180)
        try:
            yield
        finally:
            with self._fixture.phase("scale the cluster-controller back"):
                back.release()

    def restart_sandbox_manager(self) -> str:
        """Restart the drained worker's sandbox-manager; the replacement pod."""

        pod = self._control.restart_node_service(self.node_name, "sandbox-manager", timeout=300)
        self._fixture.record("restarted_sandbox_manager", pod)
        return pod

    # --- where the drain leaves each resident -----------------------------------

    def expect_all_evacuated(self, residents: Iterable[str], *, timeout: float = SETTLED_SECONDS) -> None:
        """Each resident (sandbox ids), `started` or `paused` before the
        drain, ends where the drain leaves it (`expect_suspended_unplaced`)."""

        ids = list(residents)
        deadline = time.monotonic() + timeout
        while True:
            left = {
                self.bare_id(sandbox_id): residency
                for sandbox_id in ids
                if (residency := self._unless_suspended_unplaced(sandbox_id)) is not None
            }
            if not left:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "a resident was never left suspended and unplaced with a durable snapshot",
                **({"sandbox_id": next(iter(left))} if len(ids) == 1 else {}),
                residents_left=left,
            )
            self._fixture.wait(3)
        for sandbox_id in ids:
            self.expect_single_vm(sandbox_id, "suspended")

    def _unless_suspended_unplaced(self, sandbox_id: str) -> dict[str, Any] | None:
        """None once the sandbox is `suspended` and unplaced with a durable
        snapshot; else what it is."""

        residency = self._control.sandbox_residency(self.bare_id(sandbox_id))
        snapshots = self.ready_snapshots(sandbox_id)
        durable = bool(snapshots) and snapshots[-1].storage_location_count > 0
        if residency.lifecycle == "suspended" and residency.active_node_id is None and durable:
            return None
        return {
            "lifecycle": residency.lifecycle,
            "active_node_id": residency.active_node_id,
            "ready_snapshots": len(snapshots),
            "durable": durable,
        }

    def expect_suspended_unplaced(self, sandbox_id: str, *, timeout: float = SETTLED_SECONDS) -> None:
        """The drain left the sandbox `suspended` and unplaced, its snapshot
        durable, with no VM on either worker: nothing restores it, and its
        next user call wakes it wherever the scheduler places it."""

        self.expect_all_evacuated([sandbox_id], timeout=timeout)

    def expect_back_at_home(self, sandbox_id: str, lifecycle: str, *, timeout: float = WAKE_SECONDS) -> None:
        """The sandbox is `lifecycle` again on the worker the drain was on."""

        bare = self.bare_id(sandbox_id)
        deadline = time.monotonic() + timeout
        while True:
            residency = self._control.sandbox_residency(bare)
            if residency.lifecycle == lifecycle and residency.active_node_id == self.node_id:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the sandbox never came back {lifecycle} on its own worker",
                sandbox_id=bare,
                lifecycle=residency.lifecycle,
                active_node_id=residency.active_node_id,
                node_id=self.node_id,
            )
            self._fixture.wait(3)

    def wake_elsewhere(self, sandbox: DurableLifecycleSandbox, *, timeout: float = WAKE_SECONDS) -> None:
        """Wake a sandbox the drain left `suspended`: it is restored `started`
        off the drained worker."""

        cast(ResidentLifecycleSandbox, sandbox).wake()
        self.expect_back_elsewhere(sandbox.sandbox_id, "started", timeout=timeout)

    def wake_all_elsewhere(
        self,
        sandboxes: dict[str, DurableLifecycleSandbox],
        digests: dict[str, str] | None = None,
    ) -> None:
        """Wake each resident the drain left suspended: it lands `started`
        off the drained worker, with its memory intact when `digests` holds
        it."""

        def wake(sandbox_id: str, sandbox: DurableLifecycleSandbox) -> None:
            self.wake_elsewhere(sandbox)
            if digests is not None:
                self.expect_memory(sandbox, digests[sandbox_id])

        in_parallel(
            [
                lambda sandbox_id=sandbox_id, sandbox=sandbox: wake(sandbox_id, sandbox)
                for sandbox_id, sandbox in sandboxes.items()
            ]
        )

    def expect_call_refused_while_suspended(self, sandbox: DurableLifecycleSandbox) -> CallAnswer:
        """A command to a suspended sandbox whose policy has no implicit wake
        is refused by the API (`refused_as_suspended`), and it stays
        suspended: its client resumes it. Returns the refusal. A request that
        fails at the transport raises: it says nothing about the API."""

        answer = cast(RawCallSandbox, sandbox).command_answer("true")
        lifecycle, node = self.residency(sandbox.sandbox_id)
        self._fixture.record(
            f"call_while_suspended_{self.bare_id(sandbox.sandbox_id)}",
            {"status": answer.status, "body": answer.body},
        )
        self._fixture.verify(
            refused_as_suspended(answer),
            "a call to a suspended sandbox with no implicit wake was not refused as suspended",
            status=answer.status,
            body=answer.body,
        )
        self._fixture.verify(
            lifecycle == "suspended",
            "a refused call to a suspended sandbox with no implicit wake woke it",
            lifecycle=lifecycle,
            active_node_id=node,
        )
        return answer

    def companion(self) -> DrainV2Assertions:
        """Assertions for the other worker, under this run's worker-cordon
        Lease, so a run can drain both. Its residents land on it only while
        this one's worker is cordoned."""

        if self._other is None:
            raise RuntimeError("pin_to_resident_worker() must run first")
        node = next(
            (n for n in self._control.admin_nodes() if n.k8s_node_name == self._other), None
        )
        self._fixture.verify(node is not None, "the other worker has no SPACE node row")
        assert node is not None
        other = DrainV2Assertions(self._fixture, self._control, self._create_sandbox, self._scope)
        other._node_name = self._other
        other._node_id = node.id
        other._other = self.node_name
        return other

    def cordon(self) -> None:
        """Cordon this worker, uncordoned again when the run ends."""

        node_name = self.node_name
        self._scope.defer(
            f"uncordon {node_name}", lambda: self._control.uncordon_kube_node(node_name), critical=True
        )
        self._control.cordon_kube_node(node_name)
        self._cordoned_by_run = True

    def uncordon(self) -> None:
        self._control.uncordon_kube_node(self.node_name)
        self._cordoned_by_run = False

    def unschedulable(self) -> bool:
        """Whether the worker's Kubernetes Node is cordoned."""

        return self._control.kube_node_unschedulable(self.node_name)

    def status(self) -> dict[str, Any]:
        """The node's `drain_status`."""

        columns = self._control.drain_v2_status(self.node_id)
        return {} if columns is None or columns.status is None else columns.status

    def release_other_worker(self) -> None:
        """Uncordon the other worker, so wakes have somewhere to land."""

        if self._uncordon_other is not None:
            compensation, self._uncordon_other = self._uncordon_other, None
            compensation.release()

    def cordon_other_worker(self) -> None:
        """Cordon the other worker again, under the worker-cordon Lease the
        pin holds, so that a wake can land only on the drained worker; its
        uncordon is armed, and `release_other_worker` runs it."""

        other = self._other
        if other is None:
            raise RuntimeError("pin_to_resident_worker() must run first")
        if self._uncordon_other is None:
            self._uncordon_other = self._scope.defer(
                f"uncordon {other}", lambda: self._control.uncordon_kube_node(other), critical=True
            )
        self._control.cordon_kube_node(other)

    # --- the preview ----------------------------------------------------------

    def preview(self) -> dict[str, Any]:
        """The pinned worker's v2 drain preview, through the admin API."""

        return self._control.drain_v2_preview(self.node_id)

    def plant_quarantined_sandbox(self, *, timeout: float = 300) -> tuple[str, Compensation]:
        """A sandbox the pinned worker quarantines: a cache directory named
        for a sandbox no node or row knows, which sandbox-manager's
        inventory makes a quarantined row of on its next pass. Waits for
        that row. Its removal, row and directory, is armed before the
        directory is made. Returns the sandbox id and the removal."""

        node_name, sandbox_id = self.node_name, str(uuid.uuid4())
        removal = self._scope.defer(
            f"remove the planted cache directory on {node_name}",
            lambda: self._control.remove_orphan_cache_dir(node_name, sandbox_id),
            critical=True,
        )
        path = self._control.plant_orphan_cache_dir(node_name, sandbox_id)
        self._fixture.record("planted_cache_dir", path)
        planted = time.monotonic()
        while (state := self._control.node_sandbox_state(node_name, sandbox_id)) != "quarantined":
            self._fixture.verify(
                time.monotonic() - planted < timeout,
                "the worker never quarantined the planted sandbox directory",
                path=path,
                state=state,
            )
            self._fixture.wait(5)
        self._fixture.record("seconds_to_quarantine", round(time.monotonic() - planted))
        return sandbox_id, removal

    def disk_snapshot_ids(self, sandbox_id: str) -> list[str]:
        """The sandbox's disk snapshot ids."""

        return self._control.disk_snapshot_ids(self.bare_id(sandbox_id))

    def expect_node_lifecycles(self, expected: dict[str, str]) -> None:
        """The pinned worker's sandbox-manager holds each sandbox's row in its
        lifecycle now."""

        observed = {
            self.bare_id(sandbox_id): self._control.node_sandbox_state(
                self.node_name, self.bare_id(sandbox_id)
            )
            for sandbox_id in expected
        }
        wanted = {self.bare_id(sandbox_id): lifecycle for sandbox_id, lifecycle in expected.items()}
        self._fixture.verify(
            observed == wanted,
            "a resident's row on the pinned worker is not in its lifecycle",
            observed=observed,
            expected=wanted,
        )

    # --- the drain ------------------------------------------------------------

    def drain(self, *, hold: int = 0) -> int:
        """Drain the worker with `engine: v2` and a hold of `hold` seconds (0:
        due now). The row records the engine, the generation and the
        deadline, `drain_started_at` plus the hold; returns the generation."""

        node_id = self.node_id
        self._arm_undrain()
        self._control.drain_node_v2(node_id, hold_seconds=hold)
        status = self._control.drain_v2_status(node_id)
        recorded = {} if status is None or status.status is None else status.status
        generation = recorded.get(STATUS_GENERATION_FIELD)
        self._fixture.verify(
            status is not None
            and status.engine == "v2"
            and isinstance(generation, int)
            and not isinstance(generation, bool),
            "a v2 :drain must record the engine and the generation with the phase",
            drain_status=drain_status_evidence(status),
        )
        assert isinstance(generation, int)
        self._generation = generation
        self._hold = hold
        self._deadline = self.expect_stored_deadline(hold)
        self._fixture.record("drain_generation", generation)
        return generation

    def _arm_undrain(self) -> None:
        """Arm the undrain and the uncordon of the worker, before any `:drain`
        that may start a drain, unless they are armed already."""

        if self._undrain is not None:
            return
        node_name, node_id = self.node_name, self.node_id
        self._scope.defer(
            f"uncordon {node_name}",
            lambda: self._uncordon_once_cancelled(node_name, node_id),
            critical=True,
        )
        self._undrain = self._scope.defer(
            f"undrain {node_name}", lambda: self._undrain_if_draining(node_id), critical=True
        )

    def _drain_cleared(self, node_id: str) -> bool:
        """The controller cleared the node's v2 engine and status, which it
        does only once the node answered the cancel and is unsealed; or the
        node's row is gone, so no seal is left under it."""

        status = self._control.drain_v2_status(node_id)
        return status is None or (status.engine is None and status.status is None)

    def _uncordon_once_cancelled(self, node_name: str, node_id: str) -> None:
        """Cleanup's uncordon of the drained worker, after the undrain: only
        once the drain is cancelled on the node (`_drain_cleared`), waited
        for as long as a cancel may take (`CANCEL_SECONDS`). A worker whose
        node never answered may still be sealed, so it is left cordoned, and
        the raise makes cleanup report it, so nothing the next scenario
        creates lands on a sealed worker."""

        deadline = time.monotonic() + CANCEL_SECONDS
        while not self._drain_cleared(node_id):
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"left {node_name} cordoned: its drain's cancel was not confirmed within "
                    f"{CANCEL_SECONDS} s (the v2 engine or status is still set), so the worker may "
                    "still be sealed; it is uncordoned once the node answers the controller's cancel"
                )
            self._fixture.wait(2)
        self._control.uncordon_kube_node(node_name)

    def expect_drain_refused_for_stopped(self, stopped: Sequence[str], *, hold: int) -> CallAnswer:
        """A v2 `:drain` with a hold of `hold` seconds of the worker, with no
        drain of it in flight, while it holds the `stopped` sandboxes is
        refused up front: the stopped refusal's 409, counting and naming
        exactly them, with nothing written (no phase, no v2 columns, no
        cordon). The undrain is armed first, should one be accepted."""

        self._arm_undrain()
        answer = self.drain_answer({"engine": "v2", DRAIN_HOLD_FIELD: hold})
        named = answer.body.get(STOPPED_REFUSAL_IDS_FIELD)
        ids = {self.bare_id(str(sandbox_id)) for sandbox_id in named} if isinstance(named, list) else None
        self._fixture.record("stopped_refusal", {"status": answer.status, "body": answer.body})
        self._fixture.verify(
            answer.status == STOPPED_REFUSAL_STATUS
            and answer.body.get(CAP_REFUSAL_CODE_FIELD) == STOPPED_REFUSAL_CODE
            and answer.body.get(STOPPED_REFUSAL_COUNT_FIELD) == len(stopped)
            and ids == {self.bare_id(sandbox_id) for sandbox_id in stopped},
            "a v2 :drain of a worker holding stopped sandboxes was not the stopped refusal's 409 naming them",
            status=answer.status,
            body=answer.body,
            stopped=[self.bare_id(sandbox_id) for sandbox_id in stopped],
        )
        columns = self._control.drain_v2_status(self.node_id)
        phase = self.drain_phase()
        unschedulable = self.unschedulable()
        self._fixture.verify(
            phase is None
            and columns is not None
            and columns.engine is None
            and columns.status is None
            and not unschedulable,
            "a v2 :drain refused for its stopped sandboxes left a drain, or a cordon, on its worker",
            drain_phase=phase,
            drain_status=drain_status_evidence(columns),
            unschedulable=unschedulable,
        )
        return answer

    def bring_deadline_to(self, lead: int, *, after_a_tick: bool = False) -> int:
        """A repeat `:drain` with a shorter hold, counted from
        `drain_started_at`, moves the stored deadline to `lead` seconds from
        now; returns it. Nothing tells the node: `DrainNode` carries no
        deadline, only whether to evacuate, which the controller decides each
        tick from the stored one. So the next tick past the new deadline
        evacuates, whichever controller process runs it.

        With `after_a_tick`, the deadline instead lands up to a tick later,
        `AFTER_A_TICK_SECONDS` past one of the controller's ticks: their
        period is fixed, and the last report's `reported_at` is one tick's
        own database clock. The next tick then comes 2-5 s past the deadline,
        a window in which the node still holds a running sandbox. A tick that
        ran long since shifts the phase, which only narrows that window; and
        with no report to read, the deadline is `lead` from now."""

        before = self.deadline
        target = time.time() + lead
        reported = timestamp_seconds(self.report().get(VIEW_REPORTED_FIELD)) if after_a_tick else None
        if reported is not None:
            ticks = -(-(target - AFTER_A_TICK_SECONDS - reported) // TICK_SECONDS)
            target = reported + ticks * TICK_SECONDS + AFTER_A_TICK_SECONDS
            self._fixture.record("deadline_after_tick", {"tick_reported_at": reported, "target": target})
        answer = self.repeat_drain(round(target - self.started_at()))
        self._fixture.verify(
            answer.status in (200, 202) and self.deadline < before,
            "a repeat :drain with a shorter hold did not move the deadline earlier",
            status=answer.status,
            body=answer.body,
            deadline=self.deadline,
            before=before,
        )
        self._fixture.record("brought_in_deadline", {"from": before, "to": self.deadline})
        return self.deadline

    def expect_stored_deadline(self, hold: int) -> int:
        """The row's deadline is `drain_started_at` plus `hold`; returns it."""

        started = self._control.drain_started_at(self.node_id)
        stored = self.status().get(STATUS_DEADLINE_FIELD)
        self._fixture.record("stored_deadline", {"drain_started_at": started, "deadline": stored, "hold": hold})
        self._fixture.verify(
            started is not None
            and isinstance(stored, int)
            and abs(stored - (started + hold)) <= 1,
            "the stored deadline is not drain_started_at plus the hold",
            drain_started_at=started,
            deadline=stored,
            hold=hold,
        )
        assert isinstance(stored, int)
        return stored

    def started_at(self) -> int:
        """The drain's `drain_started_at`, in Unix seconds."""

        started = self._control.drain_started_at(self.node_id)
        if started is None:
            raise RuntimeError("the node has no drain_started_at; drain() must run first")
        return started

    def drain_answer(self, body: Mapping[str, object]) -> CallAnswer:
        """`POST :drain` with `body`, as the admin API answers it. The undrain
        is armed first, should the call start a drain."""

        self._arm_undrain()
        answer = self._control.drain_node_answer(self.node_id, body)
        self._fixture.record(
            f"drain_answer_{len(body)}_{answer.status}",
            {"body": dict(body), "status": answer.status, "retry_after": answer.retry_after},
        )
        return answer

    def repeat_drain(self, hold: int) -> CallAnswer:
        """A repeat `:drain` of the drain in flight with a hold of `hold`
        seconds, counted from `drain_started_at`. One that makes the
        deadline earlier moves it; the answer is returned either way."""

        answer = self.drain_answer({"engine": "v2", DRAIN_HOLD_FIELD: hold})
        if answer.status in (200, 202):
            self._deadline = self.expect_stored_deadline(hold)
            self._hold = hold
        return answer

    def _undrain_if_draining(self, node_id: str) -> None:
        node = next((n for n in self._control.admin_nodes() if n.id == node_id), None)
        if node is not None and node.drain_phase is not None:
            self._control.undrain_node(node_id)

    def last_error(self) -> str | None:
        """The node's `drain_last_error`, as the admin listing shows it."""

        node = next((n for n in self._control.admin_nodes() if n.id == self.node_id), None)
        return None if node is None else node.drain_last_error

    def wait_for_last_error(self, text: str, *, timeout: float) -> str:
        """Poll the node's `drain_last_error` until it holds `text`."""

        deadline = time.monotonic() + timeout
        while True:
            error = self.last_error()
            if error is not None and text in error:
                return error
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the node's drain_last_error never held {text!r}",
                drain_last_error=error,
            )
            self._fixture.wait(3)

    def expect_not_stalled(self) -> None:
        """`drain_last_error` doesn't call the drain stalled."""

        error = self.last_error() or ""
        self._fixture.verify(
            not error.startswith(STALL_ERROR_PREFIX),
            "the drain was marked stalled",
            drain_last_error=error,
        )

    def wait_for_stall(self, *, timeout: float) -> dict[str, Any]:
        """Wait until the drain is marked stalled: `drain_last_error` starts
        with the stall's text. Then it checks the rule: never before the
        stored deadline, nor before the stall bound past the stamp it is
        measured from, the view's `evacuated_grew_at`. Returns the error, the
        stamp and the bound."""

        bound = self.stall_bound_seconds
        deadline = time.monotonic() + timeout
        while True:
            error = self.last_error() or ""
            grew_at = timestamp_seconds(self.report().get(VIEW_GREW_AT_FIELD))
            if error.startswith(STALL_ERROR_PREFIX):
                evidence = {
                    "drain_last_error": error,
                    "evacuated_grew_at": grew_at,
                    "stall_bound_seconds": bound,
                    "stored_deadline": self.deadline,
                    "now": time.time(),
                }
                self._fixture.record("stalled", evidence)
                # The stamp and the clock are the database's; a second of
                # rounding either way.
                self._fixture.verify(
                    grew_at is not None
                    and grew_at >= self.deadline - 1
                    and time.time() >= grew_at + bound - 5,
                    "the drain was marked stalled against the rule: before the deadline, "
                    "or before the stall bound without an evacuation past it",
                    **evidence,
                )
                return evidence
            self._fixture.verify(
                time.monotonic() < deadline,
                "the drain was never marked stalled",
                drain_last_error=error,
                evacuated_grew_at=grew_at,
                stored_deadline=self.deadline,
            )
            self._fixture.wait(5)

    def row_states(self, sandboxes: Sequence[CommandSandbox]) -> dict[str, SandboxRowState]:
        """Each sandbox's row lifecycle and placement, by bare id."""

        rows = self._control.sandbox_row_states([self.bare_id(s.sandbox_id) for s in sandboxes])
        return {row.sandbox_id: row for row in rows}

    def residency(self, sandbox_id: str) -> tuple[str, str | None]:
        """The sandbox row's lifecycle and active node."""

        residency = self._control.sandbox_residency(self.bare_id(sandbox_id))
        return residency.lifecycle, residency.active_node_id

    def ready_snapshots(self, sandbox_id: str) -> list[SnapshotRecord]:
        """The sandbox's ready full snapshots, oldest first."""

        return self._control.ready_full_snapshots(self.bare_id(sandbox_id))

    def expect_lifecycle(self, sandbox_id: str, lifecycle: str, *, timeout: float = WAKE_SECONDS) -> None:
        """The sandbox reaches `lifecycle` on some node."""

        deadline = time.monotonic() + timeout
        while True:
            current, node = self.residency(sandbox_id)
            if current == lifecycle and node is not None:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the sandbox never reached {lifecycle}",
                sandbox_id=sandbox_id,
                lifecycle=current,
                active_node_id=node,
            )
            self._fixture.wait(3)

    def expect_lifecycle_back(self, sandbox_id: str, lifecycle: str, *, timeout: float) -> None:
        """The sandbox row goes back to `lifecycle`, wherever it is placed."""

        deadline = time.monotonic() + timeout
        while True:
            current, node = self.residency(sandbox_id)
            if current == lifecycle:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the sandbox never went back to {lifecycle}",
                sandbox_id=sandbox_id,
                lifecycle=current,
                active_node_id=node,
            )
            self._fixture.wait(3)

    def expect_deleted(self, sandbox_id: str, *, timeout: float) -> None:
        """The sandbox's delete lands: its row reads `deleted`, or is gone.
        Once the node reports the sandbox gone, the delete's finalize removes
        a row with no durable artifact outright, and the tombstone sweep
        removes a `deleted` one later, so either is the delete done."""

        bare = self.bare_id(sandbox_id)
        deadline = time.monotonic() + timeout
        while True:
            placement = self._control.sandbox_placements([bare]).get(bare)
            if placement is None or placement[0] == "deleted":
                self._fixture.record(f"deleted_{bare}", "row gone" if placement is None else "deleted")
                return
            lifecycle, node = placement
            self._fixture.verify(
                time.monotonic() < deadline,
                "the sandbox's delete never landed",
                sandbox_id=bare,
                lifecycle=lifecycle,
                active_node_id=node,
            )
            self._fixture.wait(3)

    def drain_phase(self) -> str | None:
        node = next((n for n in self._control.admin_nodes() if n.id == self.node_id), None)
        return None if node is None else node.drain_phase

    def drain_mode(self) -> str | None:
        node = next((n for n in self._control.admin_nodes() if n.id == self.node_id), None)
        return None if node is None else node.drain_mode

    def terminate_status(self) -> int:
        """`POST :terminate`; the HTTP status it answered."""

        return self._control.terminate_node(self.node_id)

    # --- the node's report ------------------------------------------------------

    def stored_report(self) -> dict[str, Any]:
        """The node's last report as the controller stored it
        (`drain_status.report`), for what the admin view leaves out: the
        evacuations it has yet to record (`unrecorded`)."""

        status = self._control.drain_v2_status(self.node_id)
        report = None if status is None or status.status is None else status.status.get("report")
        return report if isinstance(report, dict) else {}

    def report(self) -> dict[str, Any]:
        """The admin node response's view of the drain (`drain`), {} when
        the node has none."""

        report = self._control.admin_node_drain(self.node_id) or {}
        # A report is the node as one call found it, rebuilt from its rows: a
        # resident woken elsewhere before the drain ended is not in the last one.
        for entry in report.get("evacuated", []):
            if isinstance(entry, dict) and isinstance(entry.get("sandbox_id"), str):
                self._reported_kinds[entry["sandbox_id"]] = artifact_kind(
                    entry.get(EVACUATED_KIND_FIELD)
                )
        return report

    def wait_for_report(
        self, accept: Callable[[dict[str, Any]], bool], failure: str, *, timeout: float
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            report = self.report()
            if report and accept(report):
                return report
            self._fixture.verify(time.monotonic() < deadline, failure, report=report)
            self._fixture.wait(REPORT_POLL_SECONDS)

    def reported(self, report: dict[str, Any]) -> bool:
        """The view holds a report the controller stored under this run's
        generation: the node answered a `DrainNode` call of this drain, so it
        is sealed under it. The API shows no seal of its own."""

        return (
            report.get(VIEW_GENERATION_FIELD) == self.generation
            and report.get(VIEW_REPORTED_FIELD) is not None
        )

    @staticmethod
    def finished(report: dict[str, Any]) -> bool:
        """The report lists nothing pending and nothing blocked."""

        return not report.get("pending") and not report.get("blocked")

    def pending_waits(self, report: dict[str, Any]) -> dict[str, str]:
        """What each pending sandbox waits on (`wait`), by bare sandbox id."""

        return {
            str(entry.get("sandbox_id")): str(entry.get(PENDING_WAIT_FIELD))
            for entry in report.get("pending", [])
            if isinstance(entry, dict)
        }

    def evacuated_residents(self, report: dict[str, Any], residents: Iterable[str]) -> set[str]:
        """The residents (sandbox ids, either form) the report lists
        evacuated, by bare id. Only these: the worker may still hold a
        copy of an earlier run's sandbox, deleted but not yet evicted, and
        report it evacuated too."""

        wanted = {self.bare_id(sandbox_id) for sandbox_id in residents}
        return {
            str(entry.get("sandbox_id"))
            for entry in report.get("evacuated", [])
            if isinstance(entry, dict) and str(entry.get("sandbox_id")) in wanted
        }

    def evacuated_total(self, report: dict[str, Any]) -> set[str]:
        """Every sandbox the controller recorded as evacuated in this drain
        (`evacuated_total`), the drained ticks' count of durable homed rows
        included, by bare id."""

        total = report.get(VIEW_EVACUATED_TOTAL_FIELD)
        return {str(sandbox_id) for sandbox_id in total} if isinstance(total, list) else set()

    def pending_entries(self, report: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """The report's pending entries, by bare sandbox id."""

        return {
            str(entry.get("sandbox_id")): entry
            for entry in report.get("pending", [])
            if isinstance(entry, dict)
        }

    def expect_nothing_blocked_per_sandbox(self, report: dict[str, Any] | None = None) -> None:
        """A `blocked` entry is node-wide: none names a sandbox. Reads
        `report`, a preview or a stored report, or the stored report; the
        admin view's entries carry only the reason."""

        if report is None:
            report = self.stored_report()
        named = [
            entry
            for entry in report.get("blocked", [])
            if not isinstance(entry, dict) or entry.get("sandbox_id") not in (None, "")
        ]
        self._fixture.verify(
            not named,
            "the report named a sandbox blocked; a sandbox that can't move yet is pending",
            blocked=named,
        )

    def view_deadline(self, report: dict[str, Any]) -> float | None:
        """The drain's deadline as the admin view shows it, in Unix seconds."""

        return timestamp_seconds(report.get(VIEW_DEADLINE_FIELD))

    def expect_view_deadline(self, report: dict[str, Any]) -> float:
        """The admin view shows the stored deadline; returns it. The node
        keeps none of its own: the controller decides from the stored one
        when its calls ask the node to evacuate."""

        shown = self.view_deadline(report)
        self._fixture.verify(
            shown is not None and abs(shown - self.deadline) <= 1,
            "the admin view's deadline is not the stored one",
            shown=report.get(VIEW_DEADLINE_FIELD),
            stored=self.deadline,
        )
        assert shown is not None
        return shown

    def wait_for_reported(self, *, timeout: float = 2 * REPORT_SECONDS) -> dict[str, Any]:
        """The view once the controller stored the node's report under the
        drain's generation, showing the stored deadline."""

        report = self.wait_for_report(
            self.reported,
            "the controller never stored a report of the node under the drain's generation",
            timeout=timeout,
        )
        self.expect_view_deadline(report)
        return report

    def expect_drained(
        self, *, timeout: float = DRAINED_SECONDS, first_reported: dict[str, float] | None = None
    ) -> dict[str, Any]:
        """The node's report under the drain's generation lists nothing
        pending or blocked, and the row reaches `drained`.

        With `first_reported`, each sandbox the report names evacuated is
        noted there with the wall-clock time this poll first saw it."""

        deadline = time.monotonic() + timeout
        while True:
            node = next((n for n in self._control.admin_nodes() if n.id == self.node_id), None)
            report = self.report()
            if first_reported is not None:
                for entry in report.get("evacuated", []):
                    if isinstance(entry, dict) and isinstance(entry.get("sandbox_id"), str):
                        first_reported.setdefault(entry["sandbox_id"], time.time())
            if node is not None and node.drain_phase == "drained":
                self._fixture.verify(
                    self.reported(report) and self.finished(report),
                    "the node reached drained without a finished report under its generation",
                    report=report,
                )
                self.expect_nothing_blocked_per_sandbox()
                self._fixture.record("drained_report", report)
                return report
            self._fixture.verify(
                time.monotonic() < deadline,
                "the v2 drain never reached drained",
                drain_phase=None if node is None else node.drain_phase,
                report=report,
            )
            self._fixture.wait(3)

    def expect_evacuated(
        self, report: dict[str, Any], expected: Iterable[str], *, timeout: float = 60
    ) -> None:
        """The report, or one this run read before it, names each resident
        (sandbox ids, either form) evacuated as a suspended snapshot, the
        only artifact a v2 drain makes. A resident the node evicted before
        any report named it is counted by the controller's ticks after
        `drained`, which add every durably suspended row still homed on the
        node, so this reads the drain again for up to `timeout` until
        `evacuated_total` holds every resident."""

        wanted = {self.bare_id(sandbox_id): EVACUATED_FULL for sandbox_id in expected}
        deadline = time.monotonic() + timeout
        while True:
            kinds = dict(self._reported_kinds)
            kinds.update(
                {
                    str(entry.get("sandbox_id")): artifact_kind(entry.get(EVACUATED_KIND_FIELD))
                    for entry in report.get("evacuated", [])
                    if isinstance(entry, dict)
                }
            )
            total = self.evacuated_total(report)
            covered = all(
                sandbox_id in total and kinds.get(sandbox_id, kind) == kind
                for sandbox_id, kind in wanted.items()
            )
            if covered or time.monotonic() >= deadline:
                break
            self._fixture.wait(3)
            report = self.report()
        # A released or woken sandbox leaves the node's report, but stays in
        # `evacuated_total`; its artifact is judged where a report this run
        # read named it.
        self._fixture.record("evacuated_total", sorted(total))
        self._fixture.verify(
            covered,
            "the controller did not record each resident evacuated, or a report named another artifact",
            reported=kinds,
            evacuated_total=sorted(total),
            expected=wanted,
        )

    # --- the deadline -----------------------------------------------------------

    def expect_held(self, sandbox_id: str, *, timeout: float = 2 * REPORT_SECONDS) -> dict[str, Any]:
        """Before the deadline, the report holds the running sandbox, and it is
        still started on the drained worker."""

        bare = self.bare_id(sandbox_id)

        def held(report: dict[str, Any]) -> bool:
            entry = self.pending_entries(report).get(bare, {})
            return (
                self.reported(report)
                and entry.get(PENDING_WAIT_FIELD) == WAIT_HOLD
                and entry.get(PENDING_RUNNING_FIELD) is True
            )

        report = self.wait_for_report(
            held, "the drain never reported the running sandbox held", timeout=timeout
        )
        lifecycle, node = self.residency(sandbox_id)
        self._fixture.verify(
            lifecycle == "started" and node == self.node_id,
            "a held sandbox must keep running on the drained worker",
            sandbox_id=bare,
            lifecycle=lifecycle,
            active_node_id=node,
        )
        self._fixture.record(f"held_report_{bare}", report)
        return report

    def expect_started_until_deadline(
        self, sandbox_ids: Sequence[str], *, margin: float = 10
    ) -> None:
        """Each sandbox stays started on the drained worker until `margin`
        seconds before the deadline; with a negative `margin`, until that
        long past it. Read from both rows: the control-plane row, and the
        node's own, which a suspend changes at once even while its upload is
        cut and the control-plane row still reads `started`. The node's row
        may read `checkpointing`, which a running VM passes through and
        returns from to `started`, never to a suspend. Only a read that
        finished while the deadline was still more than `margin` away is
        judged, and each sandbox needs at least one: a window too short to
        read in ends the run inconclusive rather than passing."""

        bare = {sandbox_id: self.bare_id(sandbox_id) for sandbox_id in sandbox_ids}
        judged: dict[str, int] = dict.fromkeys(bare.values(), 0)
        late: dict[str, int] = dict.fromkeys(bare.values(), 0)
        while self.seconds_to_deadline() > margin:
            residencies: dict[str, tuple[str, str | None, str | None]] = {}
            for sandbox_id, short in bare.items():
                read = (*self.residency(sandbox_id), self.node_state(sandbox_id))
                if self.seconds_to_deadline() > margin:
                    residencies[short] = read
                    judged[short] += 1
                else:
                    late[short] += 1
            self._fixture.verify(
                all(
                    lifecycle == "started" and node == self.node_id and node_state in HELD_NODE_STATES
                    for lifecycle, node, node_state in residencies.values()
                ),
                "a held sandbox left started before the run stopped watching",
                residency=residencies,
                seconds_to_deadline=round(self.seconds_to_deadline()),
            )
            if self.seconds_to_deadline() > margin:
                self._fixture.wait(min(5, max(1, self.seconds_to_deadline() - margin)))
        self._fixture.record("held_reads", {"judged": judged, "past_the_window": late, "margin": margin})
        if not all(judged.values()):
            raise InconclusiveSmoke(
                "the watch of the held sandboxes judged no read of one of them before the "
                "deadline came within its margin, so it proved nothing (held_reads); rerun"
            )

    def wait_past_deadline(self) -> None:
        """Return once the deadline has passed."""

        while (remaining := self.seconds_to_deadline()) > -1:
            self._fixture.wait(max(1, remaining + 1))

    def wait_until_deadline(self) -> None:
        """Return once the deadline is reached."""

        while (remaining := self.seconds_to_deadline()) > 0:
            self._fixture.wait(max(1, remaining))

    def node_state(self, sandbox_id: str) -> str | None:
        """The sandbox's row state in the drained worker's sandbox-manager."""

        return self._control.node_sandbox_state(self.node_name, self.bare_id(sandbox_id))

    def node_holds_vm_running(self, sandbox_id: str) -> bool:
        """Whether the drained worker's sandbox-manager still holds the
        sandbox `started`. The node's own state, not the row's: a suspend the
        node made reaches the row only once its snapshot is durable, so with
        its upload cut the row still reads `started` after the node took the
        VM down."""

        return self.node_state(sandbox_id) == "started"

    def expect_left_started(self, sandbox_id: str, *, timeout: float = NODE_SUSPEND_SECONDS) -> str:
        """Past the deadline, the node takes the running sandbox down: its row
        on the drained worker leaves `started` for a suspend, durable or not;
        the state it left for. The control-plane row reads `suspended` only
        once the snapshot is durable (`expect_suspended_by_the_node`)."""

        bare = self.bare_id(sandbox_id)
        deadline = time.monotonic() + timeout
        while (state := self._control.node_sandbox_state(self.node_name, bare)) == "started":
            self._fixture.verify(
                time.monotonic() < deadline,
                "the node never suspended the running sandbox past the deadline",
                sandbox_id=bare,
                seconds_past_deadline=round(-self.seconds_to_deadline()),
            )
            self._fixture.wait(2)
        # A cleanup owns the row only over a teardown, and the only one a
        # sealed node starts on a running sandbox is the drain's suspend.
        self._fixture.verify(
            state in ("suspending", "suspended", "cleanup_required"),
            "the running sandbox left started past the deadline for something but a suspend",
            sandbox_id=bare,
            node_state=state,
        )
        return str(state)

    def expect_suspended_by_the_node(self, sandbox_id: str, *, timeout: float = NODE_SUSPEND_SECONDS) -> None:
        """Past the deadline, the node suspends the running sandbox itself:
        its row leaves started for `suspended`, with a durable snapshot."""

        bare = self.bare_id(sandbox_id)
        deadline = time.monotonic() + timeout
        while True:
            lifecycle, node = self.residency(sandbox_id)
            snapshots = self.ready_snapshots(sandbox_id)
            if lifecycle == "suspended" and snapshots and snapshots[-1].storage_location_count > 0:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the node never suspended the running sandbox past the deadline",
                sandbox_id=bare,
                lifecycle=lifecycle,
                active_node_id=node,
                seconds_past_deadline=round(-self.seconds_to_deadline()),
            )
            self._fixture.wait(3)
        self._fixture.record(
            f"suspended_past_deadline_{bare}", round(-self.seconds_to_deadline())
        )

    # --- the gate -----------------------------------------------------------------

    def request_activation(
        self, sandbox: DurableLifecycleSandbox, verb: str = "wake", *, timeout: float = ACTIVATION_SECONDS
    ) -> dict[str, Any]:
        """Ask for `verb` and follow it until it ends or `timeout` passes:
        each refusal of the request itself, retried after its `Retry-After`
        as a client does, and each progress message of the operation once
        accepted. `finish_activation` follows it further."""

        subject = cast(RawCallSandbox, sandbox)
        deadline = time.monotonic() + timeout
        answer: dict[str, Any] = {
            "verb": verb, "refusals": [], "progress": [], "operation_id": None, "operation": None,
        }
        while True:
            reply = subject.lifecycle_answer(verb)
            if reply.status == 202 and isinstance(reply.body.get("operation_id"), str):
                answer["operation_id"] = str(reply.body["operation_id"])
                break
            answer["refusals"].append(
                {"status": reply.status, "retry_after": reply.retry_after, "body": reply.body}
            )
            if reply.status != GATE_REFUSAL_STATUS or time.monotonic() >= deadline:
                return answer
            try:
                delay = float(reply.retry_after or 5)
            except ValueError:
                delay = 5.0
            self._fixture.wait(min(max(delay, 1.0), 15.0))
        return self.finish_activation(sandbox, answer, timeout=max(deadline - time.monotonic(), 1))

    def finish_activation(
        self, sandbox: DurableLifecycleSandbox, answer: dict[str, Any], *, timeout: float
    ) -> dict[str, Any]:
        """Follow the operation `request_activation` started until it ends or
        `timeout` passes; `answer`, updated."""

        operation_id = answer.get("operation_id")
        if not isinstance(operation_id, str):
            # Every request so far was refused: ask again, as a client does.
            again = self.request_activation(sandbox, str(answer["verb"]), timeout=timeout)
            again["refusals"] = answer["refusals"] + again["refusals"]
            return again
        subject = cast(RawCallSandbox, sandbox)
        progress: list[str] = answer["progress"]
        deadline = time.monotonic() + timeout
        while True:
            operation = subject.operation(operation_id)
            answer["operation"] = operation
            message = str(operation.get("progress_message") or "")[:300]
            if message and (not progress or progress[-1] != message):
                progress.append(message)
            if operation.get("state") in _TERMINAL_OPERATION_STATES or time.monotonic() >= deadline:
                break
            self._fixture.wait(2)
        if not retries_exhausted_on_the_gate(operation) or time.monotonic() >= deadline:
            return answer
        # The dispatcher retried the gate's 503 until its budget ran out
        # while the drain was still making the sandbox durable: told to
        # retry all along, so the client asks again.
        answer.setdefault("exhausted", []).append(operation_id)
        again = self.request_activation(
            sandbox, str(answer["verb"]), timeout=max(deadline - time.monotonic(), 1)
        )
        again["refusals"] = answer["refusals"] + again["refusals"]
        again["progress"] = progress + again["progress"]
        again["exhausted"] = answer["exhausted"] + again.get("exhausted", [])
        return again

    def expect_told_to_retry(self, answer: dict[str, Any]) -> None:
        """An activation still waiting on the sealed node: told to retry, with
        the drain's 503, never its 409, and not ended."""

        refusals, progress = answer["refusals"], answer["progress"]
        operation = answer["operation"] or {}
        self._fixture.record("gate_answer_waiting", answer)
        self._fixture.verify(
            (bool(refusals) or any(GATE_RETRY_TEXT in message for message in progress))
            and all(
                refusal["status"] == GATE_REFUSAL_STATUS and refusal["retry_after"]
                for refusal in refusals
            )
            and not any(GATE_CONFLICT_TEXT in message for message in progress)
            and operation.get("state") not in _TERMINAL_OPERATION_STATES,
            "an activation the sealed node can't take yet was not told to retry with a 503",
            refusals=refusals,
            progress=progress,
            operation=operation,
        )

    def expect_retried_never_conflict(
        self, answer: dict[str, Any], *, retry_seen: bool = True
    ) -> None:
        """What `request_activation` saw: never the gate's 409, each refusal
        a 503 with `Retry-After`, and the activation succeeded in the end;
        with `retry_seen`, the gate told it to retry at least once."""

        refusals = answer["refusals"]
        progress = answer["progress"]
        operation = answer["operation"] or {}
        retried = any(GATE_RETRY_TEXT in message for message in progress) or bool(refusals)
        self._fixture.record("gate_answer", answer)
        self._fixture.verify(
            all(
                refusal["status"] == GATE_REFUSAL_STATUS and refusal["retry_after"]
                for refusal in refusals
            )
            and not any(GATE_CONFLICT_TEXT in message for message in progress)
            and operation.get("terminal_error_code") != GATE_CONFLICT_CODE,
            "a sealed node's activation met a 409, or a 503 without Retry-After",
            refusals=refusals,
            progress=progress,
            operation=operation,
        )
        self._fixture.verify(
            retried or not retry_seen,
            "the sealed node's gate never told the activation to retry",
            progress=progress,
        )
        self._fixture.verify(
            operation.get("state") == "succeeded",
            "the activation did not succeed once the sandbox was durable",
            operation=operation,
        )

    def expect_calls_pass_until_suspended(
        self, sandbox: DurableLifecycleSandbox, *, lead: float = CALLS_LEAD_SECONDS
    ) -> dict[str, list[int]]:
        """From `lead` seconds before the deadline until the node suspends the
        sandbox, one probe after another: each call that met its VM running
        on the drained worker before and after it passes, before the deadline
        and past it, as a drain refuses no call to a running sandbox. At
        least one lands before the deadline, when the node must still hold
        the VM. Past the deadline the node suspends at the controller's
        first evacuating call, up to a tick after it, so a run may meet the
        VM running there or not: `expect_sampled_past_deadline` judges that.
        The statuses before and past the deadline."""

        subject = cast(RawCallSandbox, sandbox)
        bare = self.bare_id(sandbox.sandbox_id)
        while (remaining := self.seconds_to_deadline()) > lead:
            self._fixture.wait(max(1, remaining - lead))
        before: list[int] = []
        past: list[int] = []
        # The lead, then the controller's first evacuating tick and its call,
        # which starts the node's suspend, with a 30 s margin.
        deadline = time.monotonic() + lead + TICK_SECONDS + CALL_BUDGET_SECONDS + 30
        # No pause between probes: each takes seconds, and past the deadline
        # the window before the suspend is about a tick long.
        while self.node_holds_vm_running(sandbox.sandbox_id):
            asked = self.seconds_to_deadline()
            answer = subject.command_answer("true")
            if not self.node_holds_vm_running(sandbox.sandbox_id):
                break
            (before if asked > 0 else past).append(answer.status)
            self._fixture.verify(
                time.monotonic() < deadline,
                "the node never suspended the running sandbox past the deadline",
                sandbox_id=bare,
                before_deadline=before[-10:],
                past_deadline=past[-10:],
            )
        seen = {"before_deadline": before, "past_deadline": past}
        self._fixture.record(f"calls_around_deadline_{bare}", seen)
        self._fixture.verify(
            bool(before),
            "no call met the VM running in the seconds before the deadline",
            sandbox_id=bare,
        )
        self._fixture.verify(
            all(status == CALL_PASSED_STATUS for status in before + past),
            "a call to a running sandbox around the deadline did not pass",
            **seen,
        )
        return seen

    def expect_sampled_past_deadline(self, seen: Mapping[str, list[int]]) -> None:
        """At least one call met the VM running past the deadline
        (`expect_calls_pass_until_suspended`), so the run judged a call
        there. A run whose calls all landed before it, or after the node's
        suspend began, judged none: it ends inconclusive, not passed. A
        scenario calls this last, so its other checks still decide a
        failure."""

        if not seen.get("past_deadline"):
            raise InconclusiveSmoke(
                "no call met the running sandbox past the deadline before the node "
                "suspended it, so no call there was judged (calls_around_deadline_*); rerun"
            )

    # --- stops and stopped sandboxes ---------------------------------------------

    def request_stop(
        self, sandbox: DurableLifecycleSandbox, verb: str, *, timeout: float = REFUSED_STOP_SECONDS
    ) -> dict[str, Any]:
        """Ask once for `verb` (`STOP_VERBS`: the user's `stop` or `kill`
        through the public API, an operator's `force-stop` or `force-kill`
        through the admin API) and follow its operation, if one was
        accepted, until it ends or `timeout` passes; what it got, as
        `request_activation` keeps it. Never asked again: a stop the drain
        refuses ends refused, and none is left running behind the run."""

        if verb not in STOP_VERBS:
            raise ValueError(f"unsupported stop verb {verb!r}")
        bare = self.bare_id(sandbox.sandbox_id)
        read: Callable[[str], Mapping[str, Any]]
        if verb.startswith(ADMIN_ACTION_PREFIX):
            reply = self._control.admin_sandbox_action_answer(bare, verb.removeprefix(ADMIN_ACTION_PREFIX))
            read = self._control.admin_operation
        else:
            subject = cast(RawCallSandbox, sandbox)
            reply = subject.lifecycle_answer(verb)
            read = subject.operation
        answer: dict[str, Any] = {
            "verb": verb, "refusals": [], "progress": [], "operation_id": None, "operation": None,
        }
        operation_id = reply.body.get("operation_id")
        if reply.status not in (200, 202) or not isinstance(operation_id, str):
            answer["refusals"].append(
                {"status": reply.status, "retry_after": reply.retry_after, "body": reply.body}
            )
            return answer
        answer["operation_id"] = operation_id
        progress: list[str] = answer["progress"]
        deadline = time.monotonic() + timeout
        while True:
            operation = dict(read(operation_id))
            answer["operation"] = operation
            message = str(operation.get("progress_message") or "")[:300]
            if message and (not progress or progress[-1] != message):
                progress.append(message)
            if operation.get("state") in _TERMINAL_OPERATION_STATES or time.monotonic() >= deadline:
                return answer
            self._fixture.wait(5)

    def expect_stop_refused(
        self, sandbox: DurableLifecycleSandbox, verb: str, *, timeout: float = REFUSED_STOP_SECONDS
    ) -> dict[str, Any]:
        """A `verb` of a running sandbox on the v2-drained worker is refused
        with the drain's retryable 503 (`stop_refused_by_the_drain`), and the
        sandbox still runs there: started on the worker in the catalog and
        in the worker's sandbox-manager, and answering a command. Returns
        what the stop got."""

        bare = self.bare_id(sandbox.sandbox_id)
        answer = self.request_stop(sandbox, verb, timeout=timeout)
        self._fixture.record(f"{verb}_on_drained_{bare}", answer)
        self._fixture.verify(
            stop_refused_by_the_drain(answer),
            f"a {verb} of a sandbox on a v2-drained worker was not refused with the drain's retryable 503",
            verb=verb,
            refusals=answer["refusals"],
            progress=answer["progress"],
            operation=answer["operation"],
        )
        lifecycle, node = self.residency(sandbox.sandbox_id)
        state = self.node_state(sandbox.sandbox_id)
        self._fixture.verify(
            lifecycle == "started" and node == self.node_id and state == "started",
            f"a refused {verb} left the sandbox other than running on the worker",
            verb=verb,
            lifecycle=lifecycle,
            active_node_id=node,
            node_state=state,
        )
        served = self.run_on(sandbox, "true", timeout=60)
        self._fixture.verify(
            served.exit_code == 0,
            f"the sandbox stopped answering after a refused {verb}",
            verb=verb,
            exit_code=served.exit_code,
        )
        return answer

    @staticmethod
    def stopped_pending(report: Mapping[str, Any], sandbox_id: str) -> bool:
        """The report, a drain's or a preview's, lists the stopped sandbox as
        a v2 drain does every stopped one: pending, waiting on nothing the
        drain drives (`external`), never a hold, and not running."""

        bare = sandbox_id.removeprefix("sbx_")
        entry = next(
            (
                entry
                for entry in report.get("pending") or []
                if isinstance(entry, Mapping) and str(entry.get("sandbox_id")) == bare
            ),
            None,
        )
        return (
            entry is not None
            and entry.get(PENDING_WAIT_FIELD) == WAIT_EXTERNAL
            and entry.get(PENDING_RUNNING_FIELD) is False
        )

    # --- Rollback (E10) -------------------------------------------------------------

    def node_disk_snapshot(self, sandbox: DurableLifecycleSandbox) -> str:
        """A disk snapshot of the sandbox, taken by the drained worker's
        sandbox-manager; its name."""

        name = self._control.node_disk_snapshot(self.node_name, self.bare_id(sandbox.sandbox_id))
        self._fixture.record(f"disk_snapshot_{self.bare_id(sandbox.sandbox_id)}", name)
        return name

    def expect_rollback_refused(self, sandbox: DurableLifecycleSandbox, snapshot: str) -> None:
        """A Rollback of the running sandbox on the sealed worker is refused up
        front, `FailedPrecondition` with the sealed marker, and the sandbox is
        still started there. A late refusal, the same code saying the
        sandbox is stopped and without the marker, means the rollback was
        applied: on a worker sealed before the call, that is a failure."""

        bare = self.bare_id(sandbox.sandbox_id)
        outcome = self._control.node_rollback(self.node_name, bare, snapshot)
        lifecycle, node = self.residency(sandbox.sandbox_id)
        state = self._control.node_sandbox_state(self.node_name, bare)
        evidence = {
            "exit_code": outcome.exit_code,
            "stderr": outcome.stderr[:500],
            "lifecycle": lifecycle,
            "active_node_id": node,
            "node_state": state,
        }
        self._fixture.record(f"rollback_on_sealed_{bare}", evidence)
        self._fixture.verify(
            not (
                ROLLBACK_LATE_TEXT in outcome.stderr
                and ROLLBACK_SEALED_MARKER not in outcome.stderr
            ),
            "a Rollback on a worker sealed before it was applied, then refused late",
            **evidence,
        )
        self._fixture.verify(
            outcome.exit_code != 0
            and ROLLBACK_REFUSAL_CODE in outcome.stderr
            and ROLLBACK_SEALED_MARKER in outcome.stderr,
            "a Rollback on a sealed worker was not refused up front with the sealed marker",
            **evidence,
        )
        self._fixture.verify(
            lifecycle == "started" and node == self.node_id and state == "started",
            "the refused Rollback left the sandbox other than running on the worker",
            **evidence,
        )

    # --- settling -------------------------------------------------------------------

    def expect_back_elsewhere(self, sandbox_id: str, lifecycle: str, *, timeout: float = WAKE_SECONDS) -> None:
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

    def expect_no_vm_off_the_drained_worker(self, sandbox_id: str) -> None:
        """The other worker's sandbox-manager holds no VM of the sandbox: it
        has not left the drained worker."""

        bare = self.bare_id(sandbox_id)
        other = self._control.node_sandbox_state(self.other_node_name, bare)
        self._fixture.verify(
            other not in _VM_STATES,
            "a sandbox still on the drained worker holds a VM on the other one",
            sandbox_id=bare,
            other_worker_state=other,
        )

    def expect_no_second_execution(self, sandbox_id: str, *, watch_seconds: float = 30) -> None:
        """For `watch_seconds`, the sandbox runs nowhere but the drained worker:
        the other worker's sandbox-manager holds no VM of it, and none of its
        execution pods is bound off the drained worker, the original pod still
        bound there or not. A drain's suspend that isn't durable yet leaves the
        row `started`, and nothing may boot the sandbox elsewhere meanwhile.
        The two reads are independent: a second pod can bind before its VM
        starts, and a VM can start whatever its pod reads."""

        bare = self.bare_id(sandbox_id)
        until = time.monotonic() + watch_seconds
        readings = 0
        while True:
            other = self._control.node_sandbox_state(self.other_node_name, bare)
            pods = self._control.sandbox_pods(bare)
            off_worker = [
                {"pod": pod.pod_name, "node": pod.node_name}
                for pod in pods
                if pod.node_name and pod.node_name != self.node_name
            ]
            self._fixture.verify(
                other not in _VM_STATES and not off_worker,
                "a second execution of the sandbox exists off the drained worker",
                sandbox_id=bare,
                other_worker_state=other,
                pods_off_the_drained_worker=off_worker,
                pod_nodes=[pod.node_name for pod in pods],
            )
            readings += 1
            if time.monotonic() >= until:
                break
            self._fixture.wait(5)
        self._fixture.record(f"no_second_execution_readings_{bare}", readings)

    def node_states(self, sandbox_ids: Sequence[str]) -> dict[str, str | None]:
        """Each sandbox's row state in the drained worker's sandbox-manager,
        by bare id."""

        return {
            self.bare_id(sandbox_id): self._control.node_sandbox_state(
                self.node_name, self.bare_id(sandbox_id)
            )
            for sandbox_id in sandbox_ids
        }

    def wait_for_in_flight_suspends(
        self, sandbox_ids: Sequence[str], *, timeout: float = 180
    ) -> dict[str, str | None]:
        """Wait until no suspend of `sandbox_ids` (bare) is still running on
        the drained worker: none of their rows there reads `suspending`, or a
        cleanup of its teardown (`cleanup_required`). A
        suspend that passed its fence before the cancel may finish after it;
        past `timeout` this returns what each reads, for the caller to judge.
        `started` is a resting lifecycle, so `settled_lifecycles` alone can't
        tell a suspend that never ran from one still running."""

        deadline = time.monotonic() + timeout
        while True:
            states = self.node_states(sandbox_ids)
            ended = all(state not in _SUSPENDING_STATES for state in states.values())
            if ended or time.monotonic() >= deadline:
                self._fixture.record("in_flight_suspends_ended", {"ended": ended, "node_states": states})
                return states
            self._fixture.wait(3)

    def settled_lifecycles(
        self, lifecycles: Mapping[str, str], *, timeout: float
    ) -> dict[str, tuple[str, str | None]]:
        """Wait until each row's lifecycle is a resting one and has held for
        three polls; each one's lifecycle and active node, by bare id."""

        deadline = time.monotonic() + timeout
        previous: dict[str, tuple[str, str | None]] | None = None
        steady = 0
        while True:
            current = {self.bare_id(sandbox_id): self.residency(sandbox_id) for sandbox_id in lifecycles}
            settled = all(
                lifecycle in ("started", "paused", "stopped", "suspended")
                for lifecycle, _ in current.values()
            )
            steady = steady + 1 if settled and current == previous else 0
            if steady >= 3:
                return current
            self._fixture.verify(
                time.monotonic() < deadline,
                "the residents never settled",
                residency=current,
            )
            previous = current
            self._fixture.wait(3)

    def expect_single_vm(self, sandbox_id: str, lifecycle: str) -> None:
        """Exactly one worker's sandbox-manager holds the sandbox's VM, in
        `lifecycle`; for a lifecycle without a VM, neither holds one."""

        bare = self.bare_id(sandbox_id)
        states = {
            node: self._control.node_sandbox_state(node, bare)
            for node in (self.node_name, self.other_node_name)
        }
        holding = [state for state in states.values() if state in _VM_STATES]
        wanted = [lifecycle] if lifecycle in _VM_STATES else []
        self._fixture.verify(
            holding == wanted,
            "the sandbox does not hold exactly one VM in its lifecycle",
            sandbox_id=bare,
            lifecycle=lifecycle,
            states=states,
        )

    def expect_lifecycles(self, expected: dict[str, str]) -> None:
        """Each sandbox is in its lifecycle on the pinned worker now."""

        observed = {
            self.bare_id(sandbox_id): (
                residency.lifecycle,
                residency.active_node_id == self.node_id,
            )
            for sandbox_id in expected
            for residency in (self._control.sandbox_residency(self.bare_id(sandbox_id)),)
        }
        wanted = {
            self.bare_id(sandbox_id): (lifecycle, lifecycle != "stopped")
            for sandbox_id, lifecycle in expected.items()
        }
        self._fixture.verify(
            all(
                observed[sandbox_id][0] == lifecycle
                and (observed[sandbox_id][1] or not placed)
                for sandbox_id, (lifecycle, placed) in wanted.items()
            ),
            "a resident is not in its lifecycle on the pinned worker",
            observed=observed,
            expected=wanted,
        )

    def expect_durable(self, sandbox_id: str, *, timeout: float) -> None:
        """The drained node made the sandbox durable, with no controller to
        see it: its own sandbox-manager holds it `suspended` with a ready,
        stored snapshot in the catalog. Read from the node's state and the
        catalog, since only the control plane may call the node's
        `DrainNode`."""

        bare = self.bare_id(sandbox_id)
        deadline = time.monotonic() + timeout
        while True:
            state = self._control.node_sandbox_state(self.node_name, bare)
            durable = any(
                snapshot.storage_location_count > 0 for snapshot in self.ready_snapshots(sandbox_id)
            )
            if state == "suspended" and durable:
                self._fixture.record(f"durable_{bare}", state)
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the node never made the sandbox durable",
                sandbox_id=bare,
                node_state=state,
            )
            self._fixture.wait(3)

    def placements(self, sandbox_ids: list[str]) -> dict[str, tuple[str, str | None]]:
        """Each sandbox's lifecycle and active node id, by bare id."""

        return self._control.sandbox_placements(sandbox_ids)

    @contextmanager
    def fake_ec2_delay(self, seconds: int) -> Iterator[None]:
        """Run the body with each instance Kind's fake EC2 marks terminated
        reading `shutting-down` for `seconds` first; 0 goes back after."""

        reset = self._scope.defer(
            "put the fake EC2's termination delay back",
            lambda: self._control.set_fake_ec2_terminate_delay(0),
            critical=True,
        )
        self._control.set_fake_ec2_terminate_delay(seconds)
        try:
            yield
        finally:
            reset.release()

    def wait_for_dispatch(self, *, timeout: float = 180) -> dict[str, Any]:
        """Wait until the controller records that Auto Scaling accepted the
        termination of the fenced instance (`dispatched_at`), under the
        drain's generation and on the instance the run gave the worker; the
        stored status."""

        node_id = self.node_id
        deadline = time.monotonic() + timeout
        while True:
            columns = self._control.drain_v2_status(node_id)
            record = None if columns is None else columns.status
            if record is not None and record.get(STATUS_DISPATCHED_FIELD) is not None:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the controller never recorded the dispatch of the termination",
                drain_status=drain_status_evidence(columns),
                last_error=self.last_error(),
            )
            self._fixture.wait(2)
        self._fixture.record("dispatched_status", record)
        self._fixture.verify(
            record.get(STATUS_GENERATION_FIELD) == self.generation
            and record.get(STATUS_FENCE_FIELD) == self._instance,
            "the termination was dispatched under another generation or for another instance",
            record=record,
            instance=self._instance,
        )
        return record

    def _terminate_and_dispatch(self) -> tuple[dict[str, str], str, str]:
        """`:terminate` the drained worker and wait for the dispatch; the
        worker's labels, and the replacement's providerID and instance, which
        `_stop_then_arm_replacement` starts it with once it is stopped."""

        node_name, node_id = self.node_name, self.node_id
        if self._instance is None:
            raise RuntimeError("give_fake_instance() must run first")
        labels = self._control.kind_node_labels(node_name)
        provider_id, replacement = fake_provider_id()
        self._fixture.record("replacement_instance", replacement)
        status = self._control.terminate_node(node_id)
        self._fixture.verify(status in (200, 202), ":terminate of a drained v2 node was refused", status=status)
        self.wait_for_dispatch()
        return labels, provider_id, replacement

    def _stop_then_arm_replacement(
        self,
        labels: Mapping[str, str],
        provider_id: str | None,
        *,
        keep_node: bool = False,
        label: str = "again as a same-name replacement",
    ) -> Compensation:
        """Stop the worker's machine, then arm its start, under `provider_id`
        when given: armed only once the stop returned, so a cleanup never
        starts a worker that still runs, which would rewrite its kubelet
        flags and remove its sandbox-manager database under it."""

        node_name = self.node_name
        self._control.stop_kind_worker(node_name, keep_node=keep_node)
        start_labels = dict(labels)

        def start() -> None:
            if provider_id is None:
                self._control.start_kind_worker(node_name, start_labels, timeout=300)
            else:
                self._control.start_kind_worker(
                    node_name, start_labels, timeout=300, provider_id=provider_id
                )

        return self._scope.defer(f"start Kind worker {node_name} {label}", start, critical=True)

    def _expect_row_deleted_without_tombstone(self, *, timeout: float) -> None:
        node_id = self.node_id
        deadline = time.monotonic() + timeout
        while self._control.node_row_exists(node_id):
            self._fixture.verify(
                time.monotonic() < deadline,
                "the terminated node's row was never deleted",
                drain_status=drain_status_evidence(self._control.drain_v2_status(node_id)),
                last_error=self.last_error(),
            )
            self._fixture.wait(3)
        self._fixture.verify(
            not self._control.node_tombstoned(node_id),
            "the terminated v2 node was tombstoned",
        )

    def record_node_row_after_let_go(self, *, timeout: float) -> bool:
        """Wait up to `timeout` for the gone-node loops to delete the let-go
        node's row, and record whether they did and which `creating`
        snapshots still name the node as their source, without judging it:
        deleting the row is the crashed-node handling's, not the drain's. A
        tombstone is the drain's, so one still fails. Whether the row went."""

        node_id = self.node_id
        deadline = time.monotonic() + timeout
        while (exists := self._control.node_row_exists(node_id)) and time.monotonic() < deadline:
            self._fixture.wait(3)
        self._fixture.record(
            "node_row_after_let_go",
            {
                "deleted": not exists,
                "creating_snapshots_from_node": self._control.creating_snapshots_from_node(node_id),
            },
        )
        self._fixture.verify(
            not self._control.node_tombstoned(node_id),
            "the terminated v2 node was tombstoned",
        )
        return not exists

    def expect_node_row_deleted_without_tombstone(self, *, timeout: float) -> None:
        """Wait until the node's row is gone, deleted by whoever owns it
        then, and verify nothing tombstoned it."""

        self._expect_row_deleted_without_tombstone(timeout=timeout)

    def expect_preview_succeeds(self, failure: str, *, timeout: float = 120) -> dict[str, Any]:
        """A drain preview of the worker succeeds within `timeout`, which only
        an unsealed node with no v2 drain recorded answers: a sealed one is
        refused (`orphaned_seal`), and so is one whose drain or cancel is
        still recorded (`drain_v2_in_progress`). Retried while the node's
        services may still be starting; the report."""

        deadline = time.monotonic() + timeout
        while True:
            try:
                report = self.preview()
                break
            except Exception as error:  # noqa: BLE001 - its node services may still be starting
                self._fixture.verify(time.monotonic() < deadline, failure, error=repr(error)[:300])
                self._fixture.wait(5)
        self._fixture.record(
            "unsealed_preview",
            {key: len(report.get(key) or []) for key in ("evacuated", "pending", "blocked")},
        )
        return report

    def _come_back(self, restart: Compensation, replacement: str, *, timeout: float) -> None:
        """Start the worker again as a same-name replacement, a new machine
        with an empty sandbox-manager database, wait until its row records the
        new instance, and verify through the admin preview that it answers
        unsealed."""

        started = time.monotonic()
        restart.release()
        steps = {"worker_registered": round(time.monotonic() - started, 1)}
        self._instance = replacement
        await_recorded_instance(
            self._fixture, self._control, self.node_name, self.node_id, replacement, timeout=timeout
        )
        steps["instance_recorded"] = round(time.monotonic() - started, 1)
        self.expect_preview_succeeds(
            "the same-name replacement never answered a drain preview: it came back sealed, "
            "or its drain is still recorded",
            timeout=timeout,
        )
        steps["preview_answered"] = round(time.monotonic() - started, 1)
        self._fixture.record("come_back_seconds", steps)

    def terminate(self, *, timeout: float = 600) -> None:
        """`:terminate` reaches the worker's instance through the `aws`
        terminator and Kind's fake EC2. Once the controller records the
        dispatch, the harness plays the cloud's part, stopping the worker and
        deleting its Node; once EC2 says the instance is terminated and a GET
        finds the Node gone, the drain deletes the row, with no tombstone.
        The worker comes back as a same-name replacement, unsealed.

        Runs inside `v2_controller` after `give_fake_instance`.
        """

        started = time.monotonic()
        labels, provider_id, replacement = self._terminate_and_dispatch()
        steps = {"dispatched": round(time.monotonic() - started, 1)}
        restart = self._stop_then_arm_replacement(labels, provider_id)
        steps["worker_stopped"] = round(time.monotonic() - started, 1)
        self._expect_row_deleted_without_tombstone(timeout=timeout)
        steps["row_deleted"] = round(time.monotonic() - started, 1)
        self._fixture.record("terminate_seconds", steps)
        self._come_back(restart, replacement, timeout=timeout)
        self._fixture.record(
            "after_terminate",
            {"node_row_deleted_without_tombstone": True, "replacement_instance": replacement},
        )

    def terminate_replaced_while_shutting_down(self, *, timeout: float = 600) -> None:
        """`:terminate`, holding the fenced instance `shutting-down` (run
        inside `fake_ec2_delay` with a delay longer than the run), and the
        worker comes back under the same name with a new instance. Its
        registration takes the same row, its `created_at` unchanged, and
        clears the drain, fence included, while the old instance still reads
        `shutting-down`: no tombstone, nothing deleted. Only then is the old
        instance released; once the fake lists it `terminated`, the
        controller's loops run past it and the replacement still holds the
        row, undrained, under its own instance.

        Runs inside `v2_controller` after `give_fake_instance`.
        """

        node_id = self.node_id
        old = self._instance
        if old is None:
            raise RuntimeError("give_fake_instance() must run first")
        created_at = self._control.node_created_at(node_id)
        labels, provider_id, replacement = self._terminate_and_dispatch()
        restart = self._stop_then_arm_replacement(labels, provider_id)
        self._come_back(restart, replacement, timeout=timeout)
        deadline = time.monotonic() + timeout
        while (columns := self._control.drain_v2_status(node_id)) is not None and (
            columns.engine is not None
        ):
            self._fixture.verify(
                time.monotonic() < deadline,
                "the replacement's registration never took the fenced row",
                drain_status=drain_status_evidence(columns),
            )
            self._fixture.wait(2)
        old_state = self._control.fake_ec2_instance_state(old)
        held = {
            "old_instance_state": old_state,
            "created_at": self._control.node_created_at(node_id),
            "created_at_before": created_at,
            "recorded": self._control.node_recorded_instance(node_id),
            "tombstoned": self._control.node_tombstoned(node_id),
        }
        self._fixture.record("fence_cleared_while_shutting_down", held)
        self._fixture.verify(
            old_state == "shutting-down",
            "the old instance left shutting-down before the replacement cleared the fence",
            **held,
        )
        self._fixture.verify(
            created_at is not None
            and held["created_at"] == created_at
            and held["recorded"] == replacement
            and not held["tombstoned"],
            "the replacement doesn't hold the same row under its own instance",
            **held,
        )
        self._control.release_fake_ec2_instance(old)
        deadline = time.monotonic() + timeout
        while (old_state := self._control.fake_ec2_instance_state(old)) != "terminated":
            self._fixture.verify(
                time.monotonic() < deadline,
                "the released old instance never read terminated",
                old_instance_state=old_state,
            )
            self._fixture.wait(2)
        # The controller's loops, past the old instance's `terminated`: the
        # row, the replacement's now, stays.
        self._fixture.wait(
            min(
                max(15 * self.gate.dispatch_backoff_base_seconds, DELETE_SETTLE_MIN_SECONDS),
                DELETE_SETTLE_MAX_SECONDS,
            )
        )
        columns = self._control.drain_v2_status(node_id)
        after = {
            "created_at": self._control.node_created_at(node_id),
            "recorded": self._control.node_recorded_instance(node_id),
            "drain_phase": self.drain_phase(),
            "drain_status": drain_status_evidence(columns),
            "tombstoned": self._control.node_tombstoned(node_id),
        }
        self._fixture.verify(
            after["created_at"] == created_at
            and after["recorded"] == replacement
            and after["drain_phase"] is None
            and (columns is None or columns.status is None)
            and not after["tombstoned"],
            "the replacement's row was deleted, replaced or drained after the old instance terminated",
            **after,
        )
        self._fixture.record(
            "after_replacement",
            {"replacement_instance": replacement, **after},
        )

    def terminate_keeping_the_node(self, *, timeout: float = 600) -> None:
        """`:terminate`; the worker stops but its Node stays, as behind a
        cloud controller that lags. Once EC2 says the fenced instance is
        terminated the row still waits, saying why; deleting the Node with a
        uid precondition, as the runbook says, lets the drain delete the row
        with no tombstone. The worker comes back as a same-name replacement.

        Runs inside `v2_controller` after `give_fake_instance`.
        """

        node_name, node_id = self.node_name, self.node_id
        labels, provider_id, replacement = self._terminate_and_dispatch()
        uid = self._control.kube_node_uid(node_name)
        self._fixture.verify(uid is not None, "the worker's Node is gone before the stop")
        assert uid is not None
        restart = self._stop_then_arm_replacement(labels, provider_id, keep_node=True)
        self.wait_for_last_error("waiting for its Kubernetes Node", timeout=timeout)
        self._fixture.verify(
            self._control.node_row_exists(node_id),
            "the row was deleted while its Node lingered",
        )
        self._control.delete_kube_node(node_name, uid=uid)
        self._expect_row_deleted_without_tombstone(timeout=timeout)
        self._come_back(restart, replacement, timeout=timeout)
        self._fixture.record("after_lingering_node", {"replacement_instance": replacement})

    def give_stale_instance(self, *, timeout: float = REGISTER_SECONDS) -> str:
        """The pinned worker under Kind's own providerID, which names no EC2
        instance, with a fake instance written into its node row: the stale
        instance an older registration writer leaves across a Node UID
        change, which registration keeps while the Node names none.
        Re-registering deletes the pods on the worker. Returns the instance;
        clearing it is armed."""

        node_name, node_id = self.node_name, self.node_id
        if self._control.kind_node_provider_id(node_name).startswith(_FAKE_PROVIDER_PREFIX):
            labels = self._control.kind_node_labels(node_name)
            self._control.restore_kind_worker_provider_id(node_name, labels, timeout=timeout)
        _, stale = fake_provider_id()
        self._scope.defer(
            f"clear the stale instance recorded for {node_name}",
            lambda: self._control.set_node_recorded_instance(node_id, None),
            critical=True,
        )
        # Registration of the re-created Node records its instance, none, on
        # its first sync; the stale value is written until it holds across a
        # few syncs.
        deadline = time.monotonic() + timeout
        held = 0
        while held < 3:
            if self._control.node_recorded_instance(node_id) == stale:
                held += 1
            else:
                held = 0
                self._fixture.verify(
                    time.monotonic() < deadline,
                    "registration never kept the stale instance",
                    worker=node_name,
                )
                self._control.set_node_recorded_instance(node_id, stale)
            self._fixture.wait(10)
        self._instance = stale
        self._fixture.record("stale_instance", stale)
        return stale

    def stop_keeping_node(self) -> Compensation:
        """The pinned worker's machine stops answering while its Kubernetes
        Node stays: the harness stops its container and keeps the Node.
        Returns the armed start of the same machine."""

        labels = self._control.kind_node_labels(self.node_name)
        restart = self._stop_then_arm_replacement(labels, None, keep_node=True, label="again")
        self._fixture.record("stopped_at", time.time())
        return restart

    def fake_ec2_terminate(self, instance_id: str) -> None:
        """Kind's fake EC2 lists `instance_id` terminated from now on."""

        self._control.fake_ec2_terminate(instance_id)

    def die(self) -> Compensation:
        """The drained worker dies mid-drain: Kind's fake EC2 marks its
        instance terminated (after the delay `fake_ec2_delay` set, if any)
        and the harness stops its container and deletes its Node. Returns
        the armed start of the worker as a same-name replacement.

        Runs inside `v2_controller` after `give_fake_instance`.
        """

        if self._instance is None:
            raise RuntimeError("give_fake_instance() must run first")
        labels = self._control.kind_node_labels(self.node_name)
        provider_id, replacement = fake_provider_id()
        self._replacement = replacement
        self._control.fake_ec2_terminate(self._instance)
        restart = self._stop_then_arm_replacement(labels, provider_id)
        self._fixture.record("died_at", time.time())
        return restart

    def expect_let_go(self, *, timeout: float = 600) -> str:
        """Wait until the drain lets the dead node go: its phase and v2
        columns cleared, the row kept, and the trace naming its instance in
        `drain_last_error`; the trace."""

        node_id = self.node_id
        trace = f"v2 drain let go: instance {self._instance} terminated outside the drain"
        deadline = time.monotonic() + timeout
        while True:
            columns = self._control.drain_v2_status(node_id)
            if columns is not None and columns.engine is None and self.last_error() == trace:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the drain never let the dead node go",
                drain_status=drain_status_evidence(columns),
                last_error=self.last_error(),
            )
            self._fixture.wait(3)
        self._fixture.verify(
            self.drain_phase() is None and not self._control.node_tombstoned(node_id),
            "the let-go left a phase or a tombstone",
        )
        self._fixture.record("let_go", {"trace": trace, "at": time.time()})
        return trace

    def expect_still_let_go(self) -> None:
        """After the let-go, the dead node's row still has no drain phase, no
        v2 columns and no tombstone: nothing put the drain back."""

        columns = self._control.drain_v2_status(self.node_id)
        phase = self.drain_phase()
        self._fixture.verify(
            columns is not None
            and columns.engine is None
            and phase is None
            and not self._control.node_tombstoned(self.node_id),
            "the drain's columns came back after the let-go",
            drain_status=drain_status_evidence(columns),
            drain_phase=phase,
        )

    def come_back_after_death(self, restart: Compensation, *, timeout: float = COME_BACK_SECONDS) -> None:
        """Start the dead worker again as a same-name replacement and open it again."""

        if self._replacement is None:
            raise RuntimeError("die() must run first")
        self._come_back(restart, self._replacement, timeout=timeout)

    # --- the undrain ------------------------------------------------------------

    def undrain(self) -> None:
        """`:undrain` the node, without waiting for the cancel."""

        if self._undrain is not None:
            compensation, self._undrain = self._undrain, None
            compensation.release()

    def undrain_and_expect_cancelled(self, *, timeout: float = CANCEL_SECONDS) -> None:
        """`:undrain` returns the node to no phase; the controller cancels the
        drain on the node, and uncordons it once it answers."""

        self.undrain()
        self.expect_cancelled(timeout=timeout)

    def expect_cancelled(self, *, timeout: float = CANCEL_SECONDS) -> None:
        """The controller clears the undrained node's v2 columns and, unless
        this run cordons the worker itself, uncordons it."""

        deadline = time.monotonic() + timeout
        while True:
            status = self._control.drain_v2_status(self.node_id)
            if status is not None and status.engine is None and status.status is None:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the undrained node's drain was never cancelled",
                drain_status=drain_status_evidence(status),
            )
            self._fixture.wait(2)
        if self._cordoned_by_run:
            return
        while self.unschedulable():
            self._fixture.verify(
                time.monotonic() < deadline,
                "the undrained node was never uncordoned",
            )
            self._fixture.wait(2)
        # The controller uncordons and clears the columns only once the node
        # answered its cancel; `expect_uncordoned_unsealed` proves the seal
        # gone with a preview.

    def expect_kept_cordoned(self, *, watch_seconds: float) -> None:
        """For `watch_seconds` after an undrain the node can't answer, the
        worker stays cordoned and its cancel stays pending: the v2 columns
        stay with no phase, and the view says it is cancelling."""

        until = time.monotonic() + watch_seconds
        readings = 0
        while time.monotonic() < until:
            columns = self._control.drain_v2_status(self.node_id)
            cordoned = self.unschedulable()
            cancelling = self.report().get(VIEW_CANCELLING_FIELD)
            self._fixture.verify(
                cordoned and columns is not None and columns.engine == "v2" and cancelling is True,
                "an undrain the node never answered uncordoned it or cleared its drain",
                unschedulable=cordoned,
                drain_status=drain_status_evidence(columns),
                drain_phase=self.drain_phase(),
                cancelling=cancelling,
            )
            readings += 1
            self._fixture.wait(5)
        self._fixture.record("cordoned_while_unanswered_readings", readings)

    def expect_uncordoned_unsealed(self, *, timeout: float = 300) -> None:
        """Once the node answers again, the cancel is answered and the worker
        uncordoned; a preview of the node then succeeds, which a sealed node
        refuses: the cancel unsealed it."""

        self.expect_cancelled(timeout=timeout)
        self.expect_preview_succeeds(
            "a preview of the uncordoned worker failed: its node is still sealed, or its "
            "cancel still recorded"
        )
