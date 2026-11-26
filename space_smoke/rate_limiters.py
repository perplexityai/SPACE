"""The two Redis-backed API rate limiters, proven end to end on a real cluster.

The scenario shrinks the cluster activation token bucket and switches
per-client concurrency to enforce on the public API, then drives real HTTP
traffic until each answers its own 429 (`Server rate limit:` and `Per-client
rate limit:`), scales Redis to zero to show both fail open, and brings it back
to show they enforce again.

Both faults (the Deployment env override and the Redis replica count) are
recorded under one cluster Lease before they change and cleared only once
restored, so a killed run is repaired by the next Kind scenario
(`KindHarness.repair_orphaned_limiter_faults`). The control surface is
[`RateLimiterControl`]; raw requests go through [`RateLimitProbe`] so a 429's
`Retry-After` survives.
"""

from __future__ import annotations

import math
import re
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, nullcontext
from functools import partial
from threading import Barrier, BrokenBarrierError, Event, Lock, Thread
from typing import TYPE_CHECKING, Any

from space_smoke.adapters import (
    ACTIVATION_CLIENT_LABEL,
    AdminReapIncomplete,
    CLIENT_LABEL_VALUE,
    LIMITER_ENV_VALUE,
    LIMITER_ENV_VALUE_MAX,
    REDIS_RECORD_CLEAR_SECONDS,
)
from space_smoke.capabilities import (
    EcwSandbox,
    EcwSandboxDeleteRetryable,
    EcwSandboxReconcileUnmatched,
    EcwSandboxState,
    LimiterProbeOutcome,
    RateLimiterControl,
    RateLimitProbe,
)
from space_smoke.runtime import (
    Compensation,
    ResourceScope,
    ScenarioTimedOut,
    SmokeConfigurationError,
    run_concurrently,
)

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SERVER_LIMIT_MESSAGE = "Server rate limit:"
PER_CLIENT_LIMIT_MESSAGE = "Per-client rate limit:"
# The live cap's refusal: a 429 with a header-only `Retry-After`, built by
# `LiveCapRefusal::new`.
LIVE_CAP_MESSAGE = "Client live sandbox cap reached:"
# The activation limiter's capacity refusals, both under this prefix: the
# slot check's 503 and the empty-cluster guard's 429.
CAPACITY_LIMIT_MESSAGE = "Server capacity limit:"
# The slot check's own refusal (`check_network_slot_headroom`), told apart
# from the empty-cluster guard's by the rest of its message.
NO_FREE_SLOT_MESSAGE = "Server capacity limit: no eligible sandbox node has a free sandbox slot"

# The limiter env a smoke may write on the public API Deployment, and nothing
# else. `enable` refuses anything outside it before kubectl runs. The store's
# URL is not among them: the chart renders it, and the fail-open half takes
# the store itself away instead. The inline switches and permit count are
# the API's own, and the inline hold its test hook, which the
# `native_recovery.inline_*` scenarios set for their run
# (`native_inline.inline_execution`); they share the Lease's record and
# repair.
LIMITER_ENV_KEYS: tuple[str, ...] = (
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS",
    "SPACE_API_CLIENT_CONCURRENCY_MODE",
    "SPACE_API_CLIENT_CONCURRENCY_MAX_PER_CLIENT",
    "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS",
    "SPACE_NATIVE_INLINE_OPERATIONS_ENABLED",
    "SPACE_NATIVE_INLINE_KINDS",
    "SPACE_NATIVE_INLINE_MAX_CONCURRENT",
    "SPACE_NATIVE_INLINE_TEST_HOLD_SECS",
    "SPACE_NATIVE_INLINE_TEST_RELEASE_FILE",
    # The attribution, live-cap and slot-check switches. Listed here so the
    # same capture-and-restore and the same Lease record cover them: a build
    # that does not know a key ignores it, so writing one costs nothing on a
    # cluster without the capability, while leaving it off the list would mean
    # a second uncaptured write with no orphan repair behind it.
    "SPACE_ACTIVATION_CLIENT_ATTRIBUTION_ENABLED",
    "SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT",
    "SPACE_API_CLIENT_LIVE_CAP_UNKNOWN",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED",
)
# The isolation rounds need more allowlisted ids, a noisy and a control id
# for each of the two rounds. Only an id in
# `KNOWN_CLIENTS` gets its own bucket and the per-client cap under test; every
# other id falls into the shared `unspecified` bucket, which has its own
# separate cap. Both arms have to be allowlisted because the question the
# round asks is about two clients the limiter is *meant* to keep apart: an
# unlisted control client would be measuring the allowlisted-versus-
# `unspecified` pair instead, which `limiter_unknown_ids_share_one_throttled_
# bucket` already covers, and which would pass here for a reason that has
# nothing to do with the per-client cap.
#
# Both carry the run's own suffix, and both need it. The control arm is sized
# to the cap, so it has no headroom to spare, and a leftover member (a release
# `ZREM` that failed, an earlier aborted run of this phase inside the
# limiter's 180s max hold, or a second bench sharing this Redis) can take the
# slot one of its requests needed — which the round would report as broken
# isolation against a limiter that is working. "Can", not "will": the Lua
# admits on `ZCARD < cap` at the instant each request arrives and releases by
# `ZREM`, so how many of a wave get in is a race, not a fixed number (see the
# note in `expect_control_isolated`). The noisy arm is exposed the mirror way:
# enough leftover members and it draws nothing but rejections, so it never
# gets the admission of its own that `noisy admitted` requires.
# `redis_limiter.rs` documents the same hazard for the keys its own tests do
# not namespace. A per-run id carries no predecessor's members,
# short of a collision between two concurrent runs' 20-bit suffixes.
#
# And each round gets its own pair, because the per-client guard releases its
# slot in a task spawned as the handler returns: a slot from the wake flood
# can still be held when the reconnect round arrives, and the control arm,
# sized to the cap, has none to spare. The prefixes are short so all five ids
# fit the 64-character `KNOWN_CLIENTS` limit.
ISOLATION_NOISY_ID_PREFIX = "smkn"
ISOLATION_CONTROL_ID_PREFIX = "smkc"
REDIS_URL_ENV = "SPACE_REDIS_URL"
_RESERVED_CLIENT_IDS: frozenset[str] = frozenset({"unknown", "native", "overflow"})
_PROBE_CLEANUP_ATTEMPTS = 10
_PROBE_CLEANUP_RETRY_SECONDS = 1.0
# Admitted probes the sweep deletes at once. A full run admits up to eighteen
# and each ECW DELETE waits up to 10 s for its operation, so nine at a time is
# two rounds, about 20 s at that bound, inside the 25 s sweep cap.
_SWEEP_DELETE_WORKERS = 9


def is_limiter_rejection(outcome: LimiterProbeOutcome, message: str) -> bool:
    """A 429 whose body starts with the named limiter's message."""

    return outcome.status == 429 and outcome.message.startswith(message)


def has_retry_after_seconds(outcome: LimiterProbeOutcome) -> bool:
    """`Retry-After` present as RFC 9110 delta-seconds (digits only)."""

    return outcome.retry_after is not None and outcome.retry_after.strip().isdigit()


# Every refusal this scenario can tell apart, with the status it arrives on.
# A refusal missing from here records as a bare status, which in the 429 case
# is indistinguishable from a placement 429 and in the 503 case from any other
# unavailability -- so a phase whose verdict rests on *which* limiter answered
# would record evidence that cannot settle it. Keyed by status as well as
# message because the capacity prefix arrives on both: a 503 for no free slot,
# and a 429 for a cluster with no eligible activation node
# (`recovery_charge.rs` maps `EmptyCluster` to `rate_limited`).
LIMITER_MESSAGES: tuple[tuple[int, str], ...] = (
    (429, SERVER_LIMIT_MESSAGE),
    (429, PER_CLIENT_LIMIT_MESSAGE),
    (429, LIVE_CAP_MESSAGE),
    (429, CAPACITY_LIMIT_MESSAGE),
    (503, CAPACITY_LIMIT_MESSAGE),
)


def status_histogram(outcomes: list[LimiterProbeOutcome]) -> dict[str, int]:
    histogram: dict[str, int] = {}
    for outcome in outcomes:
        key = str(outcome.status)
        for status, message in LIMITER_MESSAGES:
            if outcome.status == status and outcome.message.startswith(message):
                key = f"{status} {message.rstrip(':')}"
        histogram[key] = histogram.get(key, 0) + 1
    return histogram


# The activation limiter's release defaults (`cluster_admission/mod.rs`): on,
# with a burst and refill of 60. Per-client concurrency defaults to `observe`,
# which never refuses, and the live cap is off while its keys are unset or 0.
RELEASE_ACTIVATION_BURST = 60.0
RELEASE_ACTIVATION_REFILL_RPS = 60.0
_ENV_FALSY = frozenset({"0", "false", "no", "off", "disabled"})
_ENV_TRUTHY = frozenset({"1", "true", "yes", "on", "enabled"})
_LIVE_CAP_KEYS = ("SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT", "SPACE_API_CLIENT_LIVE_CAP_UNKNOWN")
_U64_MAX = 2**64 - 1


def _rust_f64(raw: str | None) -> float | None:
    """`raw.trim().parse::<f64>().ok()`; Python's float also takes `1_0`, Rust's doesn't."""

    text = (raw or "").strip()
    if "_" in text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def limiter_tightenings(env: Mapping[str, str | None]) -> list[str]:
    """The env entries that make a limiter stricter than its release default.

    Read the way the API reads them, so a value the binary ignores (a typo, a
    non-positive rate) counts as the default it falls back to: the
    `pplx_space_env_vars` flag vocabulary for the enable and slot-check
    switches, a finite burst of at least 1 and a finite, strictly positive refill,
    `Mode::from_str` for the mode, and a nonzero `u64` for each live cap. The
    burst is a test hook a build without `test-hooks` ignores; this counts it
    either way, which can only skip a verdict, never red one. Only the env is
    read; a feature-flag provider's assignments aren't visible here.
    """

    tightened: list[str] = []
    enabled = (env.get("SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED") or "").strip().lower()
    if enabled not in _ENV_FALSY:
        burst_key = "SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST"
        burst = _rust_f64(env.get(burst_key))
        if burst is not None and math.isfinite(burst) and 1.0 <= burst < RELEASE_ACTIVATION_BURST:
            tightened.append(f"{burst_key}={env.get(burst_key)}")
        refill_key = "SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS"
        refill = _rust_f64(env.get(refill_key))
        if (
            refill is not None
            and math.isfinite(refill)
            and 0.0 < refill < RELEASE_ACTIVATION_REFILL_RPS
        ):
            tightened.append(f"{refill_key}={env.get(refill_key)}")
    # Off by default, and checked outside the bucket's switch: the API runs the
    # slot check on a cold create even with the bucket disabled.
    slot_key = "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED"
    if (env.get(slot_key) or "").strip().lower() in _ENV_TRUTHY:
        tightened.append(f"{slot_key}={env.get(slot_key)}")
    mode = env.get("SPACE_API_CLIENT_CONCURRENCY_MODE")
    if (mode or "").strip().lower() == "enforce":
        tightened.append(f"SPACE_API_CLIENT_CONCURRENCY_MODE={mode}")
    for key in _LIVE_CAP_KEYS:
        raw = (env.get(key) or "").strip()
        if re.fullmatch(r"\+?[0-9]+", raw) and 0 < int(raw) <= _U64_MAX:
            tightened.append(f"{key}={env.get(key)}")
    return tightened


def _create_uncertain(outcome: LimiterProbeOutcome) -> bool:
    """A create whose answer can't say whether it left a sandbox behind.

    No answer at all, a 5xx (for example a gateway error or a post-start fetch
    failure), or a success that carried no id: any of them may have committed,
    so the sweep has to reconcile by metadata. None counts as admission
    evidence, which takes an id.
    """

    return not outcome.sandbox_id and (
        outcome.status == 0
        or outcome.status >= 500
        or 200 <= outcome.status < 300
    )


# Scenario-phase restoration runs inside the scenario's own execution budget
# and gets a full rollout window. The deferred `ResourceScope` compensations
# are backstops inside `min(120 s, 10 %)` of the scenario timeout: both their
# rollout waits and their per-compensation wall-clock alarms (`25 s + 45 s +
# 25 s = 95 s <= 120 s`, with the probe sweep's) are capped so slow or
# conflicting `kubectl` calls still leave room to release the mutation Lease
# and delete the staged wake fleet.
RESTORE_ROLLOUT_TIMEOUT_SECONDS = 240.0
REDIS_RESTORE_TIMEOUT_SECONDS = 120.0
CLEANUP_ROLLOUT_TIMEOUT_SECONDS = 30.0
CLEANUP_REDIS_ROLLOUT_TIMEOUT_SECONDS = 15.0
CLEANUP_ENV_BUDGET_SECONDS = 45.0
CLEANUP_REDIS_BUDGET_SECONDS = 25.0
# The probe sweep's backstop cap: what it hasn't deleted by then goes to the
# late admin reap, so the staged fleet's deletes after it still get time.
CLEANUP_SWEEP_BUDGET_SECONDS = 25.0
_READYZ_FLOOR_SECONDS = 5.0
_INFLIGHT_CREATE_SETTLE_SECONDS = 20.0


# Stored lifecycles the ECW connect route wakes even when ECW renders the
# sandbox running (`should_wake_on_connect`): an in-flight restore, and the
# at-rest states that render paused (`ecw_sandbox_state`).
_RECONNECT_WAKE_LIFECYCLES = frozenset({"restoring", "paused", "suspended"})


class _SweepStopped(Exception):
    """A sweep worker's DELETE not sent because the sweep was cut off first."""


class _InflightCreate:
    """One in-flight create probe tracked across main-thread timeouts and teardown."""

    def __init__(self) -> None:
        self.done = Event()
        self.swept_past = False
        self.settled = False


class RateLimiterAssertions:
    """Drive the kind public API's limiters and judge their answers.

    Caps are low on purpose: every admitted create or restore boots a real
    Firecracker sandbox, and a full run admits eight to eighteen creates. The
    teardown kills every admitted id and reconciles any unanswered create by
    run metadata, even when a phase fails mid-burst.
    """

    def __init__(
        self,
        fixture: KindEcw,
        control: RateLimiterControl,
        probe: RateLimitProbe,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._probe = probe
        self._scope = scope
        self._restore_env: Compensation | None = None
        self._release_lease: Compensation | None = None
        self._redis_back: Compensation | None = None
        self._rollout_timeout = RESTORE_ROLLOUT_TIMEOUT_SECONDS
        self._redis_rollout_timeout = REDIS_RESTORE_TIMEOUT_SECONDS
        self._redis_standing_replicas = 1
        self._env_phase_restore_failed = False
        self._redis_phase_restore_failed = False
        self._client_id = "space-smoke"
        self._template = "seed"
        self._per_client_cap = 3
        self._cluster_burst = 0
        self._live_cap = 0
        self._standing: Mapping[str, str | None] | None = None
        # Whether the public API container has `envFrom` sources, which the
        # literal read doesn't see.
        self._standing_env_from = False
        self._probe_lock = Lock()
        self._admitted: list[str] = []
        self._inflight_creates: set[_InflightCreate] = set()
        self._inflight_settle_timeout = _INFLIGHT_CREATE_SETTLE_SECONDS
        self._refill_rps = 0.0
        self._last_spend_at: float | None = None
        # Creates whose answer didn't tell whether a sandbox committed (no
        # answer, 5xx, 2xx without an id, or an interrupted call): the sweep
        # reconciles by metadata for them too.
        self._uncertain_creates = 0
        self._sweep_delete_workers = _SWEEP_DELETE_WORKERS
        run_token = uuid.uuid4().hex
        self._metadata = {
            "space_smoke_scenario": "local.rate_limiters_enforce_and_fail_open",
            "space_smoke_run": run_token,
            "space_smoke_role": "rate-limiter-probe",
        }
        # Kept short on purpose: all five ids travel as one comma-separated
        # `KNOWN_CLIENTS` value through a guard that caps it at 64 characters.
        suffix = run_token[:5]
        self._noisy_id = f"{ISOLATION_NOISY_ID_PREFIX}1-{suffix}"
        self._control_id = f"{ISOLATION_CONTROL_ID_PREFIX}1-{suffix}"
        # The reconnect round's own pair; see the note on the prefixes.
        self._round_noisy_id = f"{ISOLATION_NOISY_ID_PREFIX}2-{suffix}"
        self._round_control_id = f"{ISOLATION_CONTROL_ID_PREFIX}2-{suffix}"
        # The ids each round ran under, keyed by its label, for its verdict.
        self._arm_ids: dict[str, tuple[str, str]] = {}
        # Only the role: the sandbox collection stamps its own
        # `space_smoke_scenario` and `space_smoke_run` on every create and
        # *raises* if a caller supplies either (`_create_metadata` in
        # `services.py`), so handing it this class's `_metadata` wholesale
        # would fail the phase at its first create. The run identity it stamps
        # is its own uuid, not the one above, which is also what keeps the
        # staged fleet out of `sweep_admitted`'s metadata reconciliation: that
        # filters by `self._metadata`, the ECW metadata filter is JSONB
        # containment over every supplied pair, and the two runs' tokens
        # differ. The fleet's
        # cleanup is the one the collection arms before its create attempt,
        # and nothing else should be deleting out from under it.
        self._wake_metadata = {"space_smoke_role": "rate-limiter-wake-target"}
        self._wake_targets: list[str] = []
        self._wake_fleet: list[EcwSandbox] = []
        # Kept directly below the cluster restorations (Redis, env, Lease) by
        # `_redefer_lease_above_sandboxes`, so LIFO teardown runs it after them
        # and before the staged fleet's deletes, which register later. Its own
        # cap keeps those deletes eligible: a sweep that overruns it is cut
        # off, and its timeout handler hands the rest to the late admin reap.
        self._sweep = self._scope.defer(
            "kill every sandbox the limiter probes admitted",
            self.sweep_admitted,
            critical=True,
            timeout_seconds=CLEANUP_SWEEP_BUDGET_SECONDS,
        )

    # --- environment shape ------------------------------------------------

    def _phase(self, name: str) -> AbstractContextManager[Any]:
        phase = getattr(self._fixture, "phase", None)
        return phase(name) if phase is not None else nullcontext()

    @contextmanager
    def scoped_limiters(self) -> Iterator[None]:
        """Restore Redis, the public API limiter env, and the Lease in scenario phases on exit.

        The `ResourceScope` compensations stay armed as bounded backstops:
        `ResourceScope.close()` gets only `min(120 s, 10 %)` for every
        compensation combined and cannot absorb a full Deployment rollout.
        Normal exits and body exceptions unwind here under the scenario's
        execution budget; `ScenarioTimedOut` and non-`Exception` interrupts
        skip straight to those bounded backstops because the execution budget
        is already spent.
        """

        try:
            yield
        except BaseException as body_error:
            if isinstance(body_error, Exception) and not isinstance(
                body_error, ScenarioTimedOut
            ):
                self._unwind_remaining(body_error)
            raise
        self._unwind_remaining(None)

    def _run_unwind_step(
        self,
        phase_name: str,
        record_key: str,
        step: Callable[[], None],
        first_error: Exception | None,
    ) -> tuple[Exception | None, bool]:
        try:
            with self._phase(phase_name):
                step()
        except Exception as step_error:  # noqa: BLE001 - attached to first_error or re-raised
            timed_out = isinstance(step_error, ScenarioTimedOut)
            if first_error is None:
                return step_error, timed_out
            self._fixture.record(record_key, repr(step_error)[:1_000])
            first_error.add_note(
                f"{phase_name} also failed while unwinding: {step_error!r}; "
                "the bounded cleanup compensation stays armed"
            )
            return first_error, timed_out
        return first_error, False

    def _unwind_remaining(self, primary_error: Exception | None) -> None:
        first_error = primary_error
        timed_out = False
        if self._redis_back is not None and not self._redis_phase_restore_failed:
            first_error, timed_out = self._run_unwind_step(
                "bring Redis back up",
                "redis_restore_failure",
                self.bring_redis_back,
                first_error,
            )
        if (
            not timed_out
            and self._restore_env is not None
            and not self._env_phase_restore_failed
        ):
            first_error, timed_out = self._run_unwind_step(
                "restore the public API limiter env",
                "limiter_env_restore_failure",
                lambda: self.restore_limiters(release_lease=False),
                first_error,
            )
        if (
            not timed_out
            and self._redis_back is None
            and self._restore_env is None
            and self._release_lease is not None
        ):
            first_error, _ = self._run_unwind_step(
                "release the public API mutation lease",
                "lease_release_failure",
                self.release_mutation_lease,
                first_error,
            )
        if primary_error is None and first_error is not None:
            raise first_error

    def acquire_mutation_lease(self) -> None:
        """Take the public API mutation Lease, unless this run already holds it.

        The scenario takes it before staging the wake fleet: those cold creates
        spend activation tokens, and a cooperating run that has the bucket
        shrunk and drained would refuse them.
        """

        if self._release_lease is None:
            release = self._control.acquire_public_api_mutation()
            self._release_lease = self._scope.defer(
                "release the public API mutation lease", release, critical=True
            )

    def _redefer_lease_above_sandboxes(self) -> None:
        """Keep Redis restore, env restore, Lease release, then the probe sweep
        ahead of the staged fleet's deletes in LIFO teardown."""

        ordered = [
            comp
            for comp in (self._sweep, self._release_lease, self._restore_env, self._redis_back)
            if comp is not None and comp.armed
        ]
        if ordered:
            self._scope.redefer(*ordered)

    def enable_limiters(
        self,
        *,
        template: str,
        client_id: str = "space-smoke",
        per_client_cap: int = 3,
        cluster_burst: int = 2,
        cluster_refill_rps: str = "0.1",
        client_attribution: bool = True,
        rollout_timeout: float = 240,
    ) -> None:
        """Shrink both limiters' caps and put per-client concurrency in enforce mode.

        The standing values are captured first and restored at scope end (or
        eagerly by `restore_limiters`), so a chart or operator value comes
        back rather than being deleted. The Lease serializes cooperating
        smokes on the same Deployment and records the standing values for a
        successor if this run dies.
        """

        if not (1 <= per_client_cap <= 8 and 1 <= cluster_burst <= 8):
            raise ValueError("refusing limiter caps outside 1..8: each admitted create boots a VM")
        # Both `client_id` syntax and the combined allowlist length are checked
        # before the Lease is acquired or any restore is deferred: `_LIMITER_ENV_VALUE`
        # admits `,`, `:`, `/`, and `@` for other keys, whereas `ClientId::from_header`
        # folds any non-label or reserved id (`unknown`, `native`, `overflow`) to
        # `unknown`, and `KnownClients` ignores invalid or reserved entries.
        if not CLIENT_LABEL_VALUE.fullmatch(client_id) or client_id.lower() in _RESERVED_CLIENT_IDS:
            raise ValueError(
                f"refusing client_id {client_id!r}: must be a non-empty, unreserved "
                "Kubernetes label value"
            )
        allowlist = ",".join(
            (
                client_id,
                self._noisy_id,
                self._control_id,
                self._round_noisy_id,
                self._round_control_id,
            )
        )
        if len(allowlist) > LIMITER_ENV_VALUE_MAX:
            raise ValueError(
                f"refusing a client id of {len(client_id)} characters: with the isolation "
                f"rounds' four ids the allowlist reaches {len(allowlist)}, over the "
                f"{LIMITER_ENV_VALUE_MAX}-character env value limit"
            )
        refill_rps = _rust_f64(cluster_refill_rps)
        if (
            not LIMITER_ENV_VALUE.fullmatch(cluster_refill_rps)
            or refill_rps is None
            or not math.isfinite(refill_rps)
            or refill_rps <= 0.0
        ):
            raise ValueError(
                f"refusing cluster_refill_rps {cluster_refill_rps!r}: must be a finite positive float"
            )
        if self._restore_env is not None:
            raise SmokeConfigurationError(
                "enable_limiters called while limiter overrides are already active"
            )
        self._client_id = client_id
        self._template = template
        self._per_client_cap = per_client_cap
        self._cluster_burst = cluster_burst
        self._refill_rps = refill_rps
        self._rollout_timeout = rollout_timeout
        self.acquire_mutation_lease()
        self._redefer_lease_above_sandboxes()
        store = self._control.public_api_env((REDIS_URL_ENV,))[REDIS_URL_ENV]
        self._fixture.verify(
            bool(store),
            f"the public API has no {REDIS_URL_ENV}; both limiters would fail open, not enforce",
        )
        self._fixture.record("limiter_store", store)
        standing = self._control.public_api_env(LIMITER_ENV_KEYS)
        self._fixture.record("limiter_env_before", dict(standing))
        if set(standing) != set(LIMITER_ENV_KEYS) or not all(
            value is None or (isinstance(value, str) and (not value or LIMITER_ENV_VALUE.fullmatch(value)))
            for value in standing.values()
        ):
            raise SmokeConfigurationError(
                "the standing public API limiter env holds a value a restore cannot write"
            )
        self._standing = standing
        self._standing_env_from = self._control.public_api_has_env_from()
        self._env_phase_restore_failed = False
        fallback_timeout = min(rollout_timeout, CLEANUP_ROLLOUT_TIMEOUT_SECONDS)

        def restore() -> None:
            self._restore_standing_env(
                standing, rollout_timeout=fallback_timeout, shared_deadline=True
            )

        self._restore_env = self._scope.defer(
            "restore the public API limiter env",
            restore,
            critical=True,
            timeout_seconds=CLEANUP_ENV_BUDGET_SECONDS,
        )
        # Only the public API's copy of the activation caps shrinks. The
        # cluster-controller's `NativePublicOperationExecutor` consumes the
        # same bucket at the chart's defaults, and the token-bucket script
        # refills by each caller's own burst and rate, so a native activation
        # during the drain would top the bucket up. None runs here: this
        # scenario sends only ECW requests, which the API admits itself, and
        # the suite runs one scenario at a time. Overriding the controller
        # too would add a second Deployment rollout and a second record to
        # restore for a race the suite cannot produce.
        # Written in the same rollout as the caps, not a second one: each
        # `set_public_api_env` is a Deployment rollout the scenario then waits
        # out, and a build without the capability ignores the key. Off is a
        # deliberate value here, not an omission -- it is how the attribution
        # phase's mechanism gets disabled for the run that has to show the
        # phase failing without it. API-only, unlike the configuration docs'
        # advice to set it on the controller too: every create and wake here
        # goes through the API, which labels the pods it creates, and the
        # API's own pod read repairs a record that lags the label
        # (`pod_ops/read.rs`), so the controller's copy at its default writes
        # nothing these phases read.
        # The live caps are pinned off (0 is no cap) until `enable_live_cap`:
        # attribution is what lets a standing cap count anything, so leaving
        # them to the standing env would let a bench's own cap refuse the
        # drain's creates before they reach the cluster bucket. The slot check
        # is pinned off for the same reason: a bench that turns it on would
        # answer a cold create 503 once slots fill, which none of the verdicts
        # here expect.
        overrides = {
            "SPACE_ACTIVATION_CLIENT_ATTRIBUTION_ENABLED": str(client_attribution).lower(),
            "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED": "false",
            **dict.fromkeys(_LIVE_CAP_KEYS, "0"),
        }
        self._control.set_public_api_env(
            overrides
            | {
                "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED": "true",
                "SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST": str(cluster_burst),
                "SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS": cluster_refill_rps,
                "SPACE_API_CLIENT_CONCURRENCY_MODE": "enforce",
                "SPACE_API_CLIENT_CONCURRENCY_MAX_PER_CLIENT": str(per_client_cap),
                "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS": allowlist,
            }
        )
        self._control.wait_public_api_ready(timeout=rollout_timeout)
        self._fixture.record(
            "limiter_caps",
            {
                "client_attribution": client_attribution,
                "per_client": per_client_cap,
                "cluster_burst": cluster_burst,
                "cluster_refill_rps": cluster_refill_rps,
                "client_id": client_id,
            },
        )

    @property
    def control_client_id(self) -> str:
        """The control arm's per-run client id, for phases that drive it directly."""

        return self._control_id

    def require_attribution_schema(self) -> None:
        """Fail loudly if this build cannot record an activating client.

        The phase runs unconditionally: the writer is on `main`, so a bench
        that cannot record one is an old bench, and that is worth a clear
        message rather than a silent skip. The column is checked because it
        is a *precondition* -- necessary, not sufficient, since migrations
        land ahead of the code that writes them -- so its absence names the
        problem exactly, while its presence proves nothing and the verdicts
        below do the proving.
        """

        column = self._control.activation_attribution_available()
        self._fixture.record("client_attribution_column", column)
        self._fixture.verify(
            column,
            "this build has no `sandboxes.activating_client_id` column, so it cannot record "
            "an activating client at all and the attribution phase has nothing to judge",
        )

    def expect_client_attributed(self, sandbox_id: str, *, expected: str | None = None) -> None:
        """Every pod of this sandbox, and its row, names the activating client.

        `expected` defaults to the client the probes create under. The label
        and row are written as the activation proceeds, so both are polled; an
        expired wait is recorded as evidence rather than made its own verdict,
        so the verdicts can still say which of no pod, an unlabelled pod, a
        wrong label or a wrong row went wrong. `a pod to judge` comes first
        because the rest quantify over the pod list and an empty one passes
        them vacuously. Label and row are each compared with the activating
        client, not with each other, so two surfaces agreeing on the wrong id
        still fail.
        """

        want = expected if expected is not None else self._client_id
        deadline = time.monotonic() + self._fixture.lifecycle_transition_timeout
        expired = False
        labels: list[str] = []
        row: str | None = None
        while True:
            labels = self._control.sandbox_pod_client_ids(sandbox_id)
            row = self._control.sandbox_activating_client(sandbox_id)
            if labels and all(labels) and row:
                break
            if time.monotonic() >= deadline:
                expired = True
                break
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record(
            "client_attribution",
            {
                "sandbox_id": sandbox_id,
                "pod_labels": list(labels),
                "row": row,
                "expected": want,
                # Whether the wait ran out rather than settling. The verdicts
                # below are the specific ones -- no pod, never labelled,
                # labelled wrong, recorded wrong -- and they stay specific
                # whichever way the loop ended; this is what lets a reader
                # tell "never written" from "not written within the wait".
                "wait_expired": expired,
            },
        )
        self._fixture.verify(
            bool(labels),
            "the sandbox has no live sandbox pod (none that isn't terminal, deleting or "
            "stopped), so there is nothing for the attribution verdicts to judge",
            sandbox_id=sandbox_id,
        )
        self._fixture.verify(
            all(labels),
            f"{sum(1 for label in labels if not label)} of {len(labels)} pods carry no "
            f"{ACTIVATION_CLIENT_LABEL} at all, so the activating client was never written",
            pod_labels=list(labels),
        )
        self._fixture.verify(
            all(label == want for label in labels),
            f"a pod names a client other than {want!r} as the activator",
            pod_labels=list(labels),
        )
        self._fixture.verify(
            row is not None,
            "the sandbox has no row, so there's no recorded activating client to judge",
            sandbox_id=sandbox_id,
        )
        self._fixture.verify(
            row != "",
            "the sandbox row has no activating_client_id, so the record was never written",
            sandbox_id=sandbox_id,
        )
        self._fixture.verify(
            row == want,
            f"the sandbox row records {row!r} as the activating client, not {want!r}",
            row=row,
        )

    def _retry_delete(self, delete: Callable[[], None], *, stop: Event | None = None) -> None:
        for attempt in range(1, _PROBE_CLEANUP_ATTEMPTS + 1):
            # Checked before every attempt, a retry's included: once the sweep
            # is cut off, the handoff owns the id and sends its DELETE.
            if stop is not None and stop.is_set():
                raise _SweepStopped
            try:
                delete()
                return
            except EcwSandboxDeleteRetryable:
                if attempt == _PROBE_CLEANUP_ATTEMPTS:
                    raise
                self._fixture.wait(_PROBE_CLEANUP_RETRY_SECONDS)

    def _kill_pending_admitted(
        self,
        pending: Sequence[str],
        attempted: set[str],
        deleted: set[str],
        first_error: Exception | None,
    ) -> Exception | None:
        # `attempted` keeps the second pass from repeating an id; only
        # `deleted` keeps one out of the timeout handoff, so a DELETE that
        # failed is still retried if the deadline then cuts the sweep off.
        #
        # An ECW DELETE waits for its delete operation, up to 10 s, and a run
        # admits up to eighteen probes, so they go a few at a time: one by one
        # they would outlast the sweep's cap on a passing run. The main thread
        # waits on an `Event`, which the cleanup alarm can interrupt; workers
        # it leaves behind finish on their own, and the handoff re-sends what
        # they hadn't deleted, which a delete answers as done.
        ids = list(dict.fromkeys(pending))
        if not ids:
            return first_error
        lock = Lock()
        queue = list(ids)
        errors: dict[str, BaseException] = {}
        finished = Event()
        # Set once the sweep is cut off: the handoff owns what's left then.
        stop = Event()
        left = [len(ids)]

        def work() -> None:
            while True:
                with lock:
                    if not queue or stop.is_set():
                        return
                    sandbox_id = queue.pop(0)
                outcome: BaseException | None = None
                try:
                    self._retry_delete(
                        partial(self._probe.kill_sandbox, sandbox_id), stop=stop
                    )
                except BaseException as error:  # noqa: BLE001 - reported by the waiting thread
                    outcome = error
                with lock:
                    if outcome is None:
                        deleted.add(sandbox_id)
                    elif not isinstance(outcome, _SweepStopped):
                        errors[sandbox_id] = outcome
                    # Neither a cut-off DELETE nor one stopped before sending
                    # counts as attempted: the handoff re-sends both.
                    if not isinstance(outcome, (ScenarioTimedOut, _SweepStopped)):
                        attempted.add(sandbox_id)
                    left[0] -= 1
                    if isinstance(outcome, ScenarioTimedOut):
                        stop.set()
                    if not left[0] or stop.is_set():
                        finished.set()

        # Starting the workers is inside the `try`: an alarm between two starts
        # must stop the ones already running as surely as one during the wait.
        try:
            for _ in range(min(self._sweep_delete_workers, len(ids))):
                Thread(target=work, name="space-smoke-rate-limiter-sweep", daemon=True).start()
            while not finished.wait(timeout=0.5):
                pass
        finally:
            stop.set()
        with lock:
            outcomes = dict(errors)
        for error in outcomes.values():
            if isinstance(error, ScenarioTimedOut):
                raise error
        for sandbox_id in ids:
            error = outcomes.get(sandbox_id)
            if isinstance(error, Exception):
                first_error = first_error or error
            elif error is not None:
                raise error
        return first_error

    def _mark_inflight_swept_past(self) -> int:
        with self._probe_lock:
            for inflight in self._inflight_creates:
                inflight.swept_past = True
            return len(self._inflight_creates)

    def _handoff_late_arrivals(self, deleted: set[str], *, reconciled: bool) -> None:
        # Whatever the interrupted sweep hadn't finished, not only what arrived
        # during it: a DELETE that failed or that the deadline cut off leaves
        # its id, and every id queued behind it, undeleted, and an uncertain
        # create recorded before the sweep started still needs the lookup it
        # never reached.
        with self._probe_lock:
            for inflight in self._inflight_creates:
                inflight.swept_past = True
            late_ids = [
                sid for sid in dict.fromkeys(self._admitted) if sid not in deleted
            ]
            late_uncertain = not reconciled and self._uncertain_creates > 0
        if not late_ids and not late_uncertain:
            return

        def reap_late_batch() -> None:
            self._reap_late(late_ids, uncertain=late_uncertain)

        Thread(
            target=reap_late_batch,
            name="space-smoke-rate-limiter-late-reap",
            daemon=True,
        ).start()

    def _settle_inflight_creates(self) -> Exception | None:
        with self._probe_lock:
            if not self._inflight_creates:
                return None
        deadline = time.monotonic() + self._inflight_settle_timeout
        while True:
            with self._probe_lock:
                inflight = next(iter(self._inflight_creates), None)
            if inflight is None:
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not inflight.done.wait(timeout=remaining):
                stranded = self._mark_inflight_swept_past()
                if not stranded:
                    return None
                return RuntimeError(
                    f"{stranded} interrupted create probe(s) remained in flight "
                    f"after {self._inflight_settle_timeout:g}s cleanup settle window"
                )

    def sweep_admitted(self) -> None:
        """Kill every sandbox a probe admitted, and reconcile uncertain creates.

        Admitted creates have known ids, so `kill_sandbox` deletes each one
        directly — including sandboxes whose 120s idle timeout already reaped
        them before teardown, where `kill_sandbox` treats 404 as done whereas
        `kill_matching` would find zero live matches and fail. Interrupted
        create workers still in flight settle before reconciliation so a late
        commit can't land after cleanup reported success (and if the cleanup
        deadline itself interrupts the sweep, still-in-flight workers, every id
        it hadn't deleted and any unreconciled uncertain create are reaped
        asynchronously, through the bench's admin API, since teardown may have
        deleted the run's ECW key by then).
        Any create whose outcome didn't confirm or rule out a commit (no
        answer, 5xx, 2xx without an id, or an interrupted call) is then
        reconciled by run metadata.
        """

        attempted: set[str] = set()
        deleted: set[str] = set()
        reconciled = False
        with self._probe_lock:
            initial_pending = list(dict.fromkeys(self._admitted))
        try:
            first_error = self._kill_pending_admitted(initial_pending, attempted, deleted, None)
            settle_error = self._settle_inflight_creates()
            first_error = first_error or settle_error
            with self._probe_lock:
                late_pending = [
                    sid for sid in dict.fromkeys(self._admitted) if sid not in attempted
                ]
            first_error = self._kill_pending_admitted(
                late_pending, attempted, deleted, first_error
            )
            with self._probe_lock:
                uncertain = self._uncertain_creates
            if uncertain:
                try:
                    self._retry_delete(
                        partial(
                            self._probe.kill_matching, self._metadata, scan_whole_window=True
                        )
                    )
                    reconciled = True
                except ScenarioTimedOut:
                    raise
                except EcwSandboxReconcileUnmatched as error:
                    reconciled = True
                    self._fixture.record(
                        "unanswered_creates_unmatched",
                        {"count": uncertain, "error": str(error)},
                    )
                except Exception as error:  # noqa: BLE001 - preserve earlier delete failure if any
                    # Not reconciled: a scan that didn't finish leaves the
                    # lookup to the handoff below.
                    first_error = first_error or error
        except BaseException:
            self._handoff_late_arrivals(deleted, reconciled=reconciled)
            raise
        if first_error is not None:
            # A DELETE that kept failing, or a metadata scan that didn't
            # finish, leaves work the admin reap can still do: hand it off as
            # a timeout does, then report the failure.
            self._handoff_late_arrivals(deleted, reconciled=reconciled)
            raise first_error

    def _note(self, outcome: LimiterProbeOutcome) -> LimiterProbeOutcome:
        if outcome.sandbox_id:
            self._admitted.append(outcome.sandbox_id)
        return outcome

    def _begin_create(self, inflight: _InflightCreate, cancelled: Event | None) -> None:
        with self._probe_lock:
            if cancelled is not None and cancelled.is_set():
                inflight.settled = True
                inflight.done.set()
                raise BrokenBarrierError
            self._inflight_creates.add(inflight)

    def _reap_late_create(self, sandbox_id: str | None, *, uncertain: bool) -> None:
        # By the time a late reap runs, teardown may already have deleted the
        # run's ECW key the probe authenticates with, so it goes through the
        # bench's admin API instead: by id, or by the run's metadata for a
        # create whose answer named none. A metadata reap scans the whole
        # bounded window rather than stopping at its first match: several
        # creates of unknown outcome share the run's metadata, and one becoming
        # visible says nothing about the others, which may commit later.
        #
        # The admin route answers 202 once a delete is queued, so a queued
        # delete isn't the end: every id stays pending until its delete is
        # confirmed done, and is polled (a failed delete re-sent) within the
        # bounded window, the ones a metadata scan queued on its last round
        # included. An id still pending when the window ends is left: a
        # detached reap has no caller to report it to.
        self._reap_late([sandbox_id] if sandbox_id else [], uncertain=uncertain)

    def _reap_late(self, sandbox_ids: Iterable[str], *, uncertain: bool) -> None:
        """Reap these ids, and with `uncertain` the run's metadata, in shared
        rounds: every id's first DELETE goes out in the first round, so one
        whose delete stays pending holds no other id, nor the metadata scan,
        behind its window, and a runner that exits mid-window has sent them all."""

        # Insertion-ordered, so ids go out in the order they were handed off.
        pending: dict[str, None] = dict.fromkeys(sandbox_ids)
        if not pending and not uncertain:
            return

        def poll(ids: Iterable[str]) -> None:
            for pending_id in list(ids):
                try:
                    if self._control.admin_force_delete_sandbox(pending_id):
                        pending.pop(pending_id, None)
                except Exception:  # noqa: BLE001 - a detached reap has no caller to raise to
                    pass

        rounds = 2 * _PROBE_CLEANUP_ATTEMPTS if uncertain else _PROBE_CLEANUP_ATTEMPTS
        for attempt in range(1, rounds + 1):
            scanning = uncertain and attempt <= _PROBE_CLEANUP_ATTEMPTS
            if scanning:
                try:
                    queued = list(self._control.admin_force_delete_matching(self._metadata))
                except AdminReapIncomplete as error:
                    # What it queued before raising is still polled.
                    queued = list(error.pending)
                except Exception:  # noqa: BLE001 - a detached reap has no caller to raise to
                    queued = []
                # Ids queued earlier that this scan no longer lists are polled
                # by id; the ones it listed it has just polled itself.
                poll([sid for sid in pending if sid not in queued])
                for sid in queued:
                    pending.setdefault(sid, None)
            else:
                poll(pending)
            # The scan window always runs out: later creates may commit yet.
            if not pending and not (scanning and attempt < _PROBE_CLEANUP_ATTEMPTS):
                return
            if attempt < rounds:
                self._fixture.wait(_PROBE_CLEANUP_RETRY_SECONDS)

    def _finish_create_outcome(
        self, inflight: _InflightCreate, outcome: LimiterProbeOutcome
    ) -> None:
        uncertain = _create_uncertain(outcome)
        with self._probe_lock:
            if inflight.settled:
                return
            swept_past = inflight.swept_past
            if not swept_past:
                self._note(outcome)
                if uncertain:
                    self._uncertain_creates += 1
            self._inflight_creates.discard(inflight)
            inflight.done.set()
            inflight.settled = True
        if swept_past:
            self._reap_late_create(outcome.sandbox_id, uncertain=uncertain)

    def _finish_create_error(
        self, inflight: _InflightCreate, *, aborted_before_dispatch: bool
    ) -> None:
        with self._probe_lock:
            if inflight.settled:
                return
            swept_past = inflight.swept_past and not aborted_before_dispatch
            if not aborted_before_dispatch and not swept_past:
                self._uncertain_creates += 1
            self._inflight_creates.discard(inflight)
            inflight.done.set()
            inflight.settled = True
        if swept_past:
            self._reap_late_create(None, uncertain=True)

    def _tracked_create_request(
        self,
        method: str,
        path: str,
        body: Mapping[str, object] | None,
        *,
        client_id: str,
        timeout: float,
        gate: Callable[[], None] | None = None,
        cancelled: Event | None = None,
    ) -> LimiterProbeOutcome:
        inflight = _InflightCreate()
        outcome: LimiterProbeOutcome | None = None
        dispatched = False

        def tracked_gate() -> None:
            nonlocal dispatched
            assert gate is not None
            gate()
            if cancelled is not None and cancelled.is_set():
                raise BrokenBarrierError
            dispatched = True

        try:
            self._begin_create(inflight, cancelled)
            if gate is None:
                dispatched = True
            outcome = self._probe.race_ecw_request(
                method,
                path,
                body,
                client_id=client_id,
                timeout=timeout,
                gate=None if gate is None else tracked_gate,
            )
            self._finish_create_outcome(inflight, outcome)
            return outcome
        except BaseException:
            if outcome is not None:
                self._finish_create_outcome(inflight, outcome)
            else:
                self._finish_create_error(inflight, aborted_before_dispatch=not dispatched)
            raise

    def _restore_standing_env(
        self,
        standing: Mapping[str, str | None],
        *,
        rollout_timeout: float,
        shared_deadline: bool = False,
    ) -> None:
        deadline = time.monotonic() + rollout_timeout
        if shared_deadline:
            self._control.restore_public_api_env(
                standing, rollout_timeout=rollout_timeout, shared_deadline=True
            )
        else:
            self._control.restore_public_api_env(standing, rollout_timeout=rollout_timeout)
        ready_timeout = min(
            rollout_timeout, max(_READYZ_FLOOR_SECONDS, deadline - time.monotonic())
        )
        self._control.wait_public_api_ready(timeout=ready_timeout)

    def restore_limiters(
        self, *, release_lease: bool = True, rollout_timeout: float | None = None
    ) -> None:
        """Put the standing env back, and by default drop the mutation Lease.

        `release_lease=False` keeps the Lease while a caller goes on to judge
        the restored configuration: the Lease is what stops a cooperating
        smoke from writing this Deployment, so dropping it first would let
        another run change the env underneath those requests and leave this
        one reporting on a configuration it did not establish.
        """

        if self._restore_env is not None:
            if self._standing is None:
                raise RuntimeError("restore_limiters called without a captured standing env")
            effective_timeout = (
                self._rollout_timeout if rollout_timeout is None else rollout_timeout
            )
            try:
                self._restore_standing_env(self._standing, rollout_timeout=effective_timeout)
            except BaseException:
                self._env_phase_restore_failed = True
                raise
            self._env_phase_restore_failed = False
            self._restore_env.disarm()
            self._restore_env = None
        if release_lease and self._release_lease is not None:
            self._release_lease.release()
            self._release_lease = None

    def release_mutation_lease(self) -> None:
        """Drop the Lease on its own, after a `restore_limiters` that kept it."""

        self.restore_limiters()

    # --- the probes ---------------------------------------------------------

    def _create_body(self) -> dict[str, object]:
        return {"templateID": self._template, "timeout": 120, "metadata": dict(self._metadata)}

    def create_once(
        self, *, client_id: str | None = None, timeout: float = 90
    ) -> LimiterProbeOutcome:
        try:
            return self._tracked_create_request(
                "POST",
                "/sandboxes",
                self._create_body(),
                client_id=client_id or self._client_id,
                timeout=timeout,
            )
        finally:
            self._last_spend_at = time.monotonic()

    def preflight(self) -> LimiterProbeOutcome:
        """One create must be admitted or rate-limited: auth, host and template are wired."""

        outcome = self.create_once()
        self._fixture.verify(
            outcome.status in (201, 429),
            "the preflight ECW create was neither admitted nor rate-limited",
            status=outcome.status,
            response_message=outcome.message,
        )
        return outcome

    def create_until_admitted(self, *, attempts: int = 5) -> LimiterProbeOutcome:
        """Create until one is admitted, waiting out each `Server rate limit:` 429.

        The rollout that switches attribution on doesn't refill the activation
        bucket, so a create right after it can draw that 429. Any other answer
        ends the loop, so a create that fails some other way stays visible.
        """

        outcome = self.create_once()
        for _ in range(attempts - 1):
            if not is_limiter_rejection(outcome, SERVER_LIMIT_MESSAGE):
                break
            retry_after = outcome.retry_after
            delay = int(retry_after) if retry_after and retry_after.strip().isdigit() else 1
            self._fixture.wait(max(delay, 1))
            outcome = self.create_once()
        self._fixture.verify(
            outcome.status == 201 and bool(outcome.sandbox_id),
            f"no create was admitted in {attempts} attempts",
            status=outcome.status,
            response_message=outcome.message,
        )
        return outcome

    def drain_cluster_bucket(self, *, max_probes: int = 40) -> LimiterProbeOutcome:
        """Sequential creates until the activation bucket refuses with the server message."""

        seen: list[int] = []
        for _ in range(max_probes):
            outcome = self.create_once()
            seen.append(outcome.status)
            if is_limiter_rejection(outcome, SERVER_LIMIT_MESSAGE):
                self._fixture.record("cluster_drain_probes", len(seen))
                self._fixture.verify(
                    has_retry_after_seconds(outcome),
                    "the server rate-limit 429 carried no Retry-After delta-seconds",
                    retry_after=outcome.retry_after,
                )
                self._fixture.record("cluster_retry_after", outcome.retry_after)
                return outcome
        self._fixture.verify(
            False,
            f"never observed a 429 '{SERVER_LIMIT_MESSAGE}' after {max_probes} creates",
            statuses=seen,
        )
        raise AssertionError("unreachable")

    def burst(
        self,
        count: int,
        method: str,
        path: str,
        body: Mapping[str, object] | None,
        *,
        client_id: str | None = None,
        timeout: float = 90,
        creates: bool = False,
    ) -> list[LimiterProbeOutcome]:
        """`count` gated concurrent requests for one client id; every outcome returned.

        With `creates`, each worker registers an in-flight create handle so
        `sweep_admitted` waits for interrupted workers before reconciling and
        late stragglers reap their own sandboxes if cleanup times out first. A
        worker raise is re-raised once every worker has finished; a main-thread
        interrupt (`ScenarioTimedOut`) escapes immediately.
        """

        # Bounded, and aborted if a worker raises before the gate or a thread
        # fails to start, so workers already at the gate raise
        # `BrokenBarrierError` instead of waiting on a party that never
        # arrives. Daemon workers via `run_concurrently` let a scenario
        # deadline escape immediately instead of blocking in pool shutdown.
        tripped = Event()
        cancelled = Event()
        barrier = Barrier(count, action=tripped.set, timeout=timeout)
        effective_client_id = client_id or self._client_id

        def gate() -> None:
            # `Barrier.wait` returns the caller's arrival index; the probe's
            # gate is a plain hook.
            barrier.wait()

        def call() -> LimiterProbeOutcome:
            try:
                # Only creates are tracked for `sweep_admitted`: a connect
                # echoes the id of a sandbox it didn't create, the fixture's
                # own included, and the sweep would delete it out from under
                # its owner.
                if creates:
                    return self._tracked_create_request(
                        method,
                        path,
                        body,
                        client_id=effective_client_id,
                        timeout=timeout,
                        gate=gate,
                        cancelled=cancelled,
                    )
                return self._probe.race_ecw_request(
                    method,
                    path,
                    body,
                    client_id=effective_client_id,
                    timeout=timeout,
                    gate=gate,
                )
            except BaseException:
                if not tripped.is_set():
                    barrier.abort()
                raise

        try:
            raw = run_concurrently([call for _ in range(count)])
        except BaseException:
            with self._probe_lock:
                cancelled.set()
                if creates:
                    self._uncertain_creates += count
            if not tripped.is_set():
                barrier.abort()
            self._last_spend_at = time.monotonic()
            raise
        outcomes: list[LimiterProbeOutcome] = []
        causes: list[BaseException] = []
        collateral: list[BrokenBarrierError] = []
        for item in raw:
            if isinstance(item, BrokenBarrierError):
                collateral.append(item)
            elif isinstance(item, BaseException):
                causes.append(item)
            else:
                outcomes.append(item)
        errors: list[BaseException] = [*causes, *collateral]
        self._last_spend_at = time.monotonic()
        if errors:
            raise errors[0]
        self._fixture.verify(
            len(outcomes) == count,
            "not every concurrent caller reported an outcome",
            reported=len(outcomes),
            callers=count,
        )
        return outcomes

    def burst_creates(
        self, count: int = 20, *, client_id: str | None = None
    ) -> list[LimiterProbeOutcome]:
        return self.burst(
            count, "POST", "/sandboxes", self._create_body(), client_id=client_id, creates=True
        )

    def expect_create_capped(self) -> None:
        """With Redis up, a create from the capped client takes the live-cap 429.

        The live-cap phase only refuses wakes. Without this, the outage
        burst's 201s couldn't tell a cap failing open from a create path the
        cap never checks.
        """

        outcome = self.create_once(client_id=self._noisy_id)
        self._fixture.record("live_cap_create_status", outcome.status)
        self._fixture.verify(
            is_limiter_rejection(outcome, LIVE_CAP_MESSAGE),
            "a create from the client at its live cap wasn't refused by the live cap, so "
            "the outage burst's admissions wouldn't show the cap failing open",
            status=outcome.status,
            response_message=outcome.message[:200],
        )

    def burst_creates_at_live_cap(self, count: int) -> list[LimiterProbeOutcome]:
        """Creates from the client the live-cap phase left holding its cap.

        The live cap fails open on an unusable Redis like the other limiters,
        but only a client already at its cap can show it: a client below the
        cap is admitted either way. So the burst goes out as the capped
        client, and its live sandboxes are re-read from the substrate first,
        with Redis already down, so the precondition holds in the outage.
        """

        held = self._control.live_client_sandboxes(self._noisy_id)
        self._fixture.verify(
            self._live_cap > 0 and len(held) >= self._live_cap,
            "the capped client isn't at its live cap, so a fail-open create from it "
            "wouldn't show the live cap failing open",
            cap=self._live_cap,
            held=held,
        )
        return self.burst_creates(count, client_id=self._noisy_id)

    def await_activation_token(self) -> None:
        """Wait out one refill interval since the last request that could spend a token.

        The drain and the create burst can leave the activation bucket empty,
        and a wake charges a `Restore` token. Every probe stamps the time it
        finished, connects included, because a reconnect whose keepalive finds
        the runtime unreachable escalates to a wake. The resuspend and the live-cap
        rollout usually outlast the refill, so this rarely waits; it's there
        for the run where they don't. The live cap is reserved before the
        token is spent, so only the first wake of the live-cap phase needs one,
        but without it that wake draws the cluster 429 and the phase reads a
        working cap as one that admitted nothing.
        """

        if self._last_spend_at is None or self._refill_rps <= 0:
            return
        remaining = self._last_spend_at + 1.0 / self._refill_rps - time.monotonic()
        self._fixture.record("activation_refill_wait_seconds", round(max(remaining, 0.0), 1))
        if remaining > 0:
            self._fixture.wait(remaining)

    def _release_together(
        self,
        plan: Sequence[tuple[str, str, Sequence[tuple[str, Mapping[str, object]]]]],
        *,
        timeout: float,
        label: str,
        what: str,
    ) -> dict[str, list[LimiterProbeOutcome]]:
        """Fire several separately-bucketed arms from one barrier.

        `plan` is one entry per arm: its name (how its evidence is keyed), the
        client id its requests go out under, and one `(path, body)` per request
        it should make. Returns the outcomes keyed by arm name.

        Shared by every phase that needs two clients in flight at once, so the
        exception triage below exists once rather than per phase.
        """

        total = sum(len(specs) for _, _, specs in plan)
        # The workers do their own setup before reaching the gate
        # (`race_ecw_request` builds config, headers and body first), so one of
        # them raising there would leave the rest waiting on a party that never
        # arrives, with the pool sized exactly to the barrier. `released`
        # aborts the barrier on the way out of such a raise, so the rest fail
        # at once instead of waiting out the timeout. The timeout stays as the
        # backstop for a worker that neither arrives nor raises. It is the
        # request timeout too, so a round where every worker reaches the gate
        # and then waits out its request can take up to twice `timeout`.
        # `tripped` is set by the barrier's action, which runs once every
        # party has arrived and before any of them is released.
        tripped = Event()
        barrier = Barrier(total, action=tripped.set, timeout=timeout)

        def gate() -> None:
            barrier.wait()

        def released(client: str, path: str, body: Mapping[str, object]) -> LimiterProbeOutcome:
            try:
                return self._probe.race_ecw_request(
                    "POST", path, body, client_id=client, timeout=timeout, gate=gate
                )
            except BaseException:
                # A raise before the gate strands the others, so abort. Once
                # the barrier has tripped, an abort could still break a party
                # that's on its way out of `wait`, and turn a request that was
                # released into a `BrokenBarrierError`, so leave it alone.
                if not tripped.is_set():
                    barrier.abort()
                raise

        calls: list[tuple[str, Callable[[], LimiterProbeOutcome]]] = [
            (arm, partial(released, client, path, body))
            for arm, client, specs in plan
            for path, body in specs
        ]
        # `run_concurrently` joins daemon threads directly, so a scenario
        # deadline interrupts the main thread at once rather than waiting out
        # in-flight HTTP timeouts inside `ThreadPoolExecutor.__exit__`.
        try:
            raw = run_concurrently([call for _, call in calls])
        except BaseException:
            if not tripped.is_set():
                barrier.abort()
            self._last_spend_at = time.monotonic()
            raise
        # A worker that raises before it reaches the gate breaks the
        # barrier for every other one, so most of what comes back is
        # `BrokenBarrierError` — a consequence, not the cause. Collect
        # every outcome first and raise the cause if there was one, rather
        # than letting the first worker's bystander error be what the
        # round reports.
        arms: dict[str, list[LimiterProbeOutcome]] = {arm: [] for arm, _, _ in plan}
        collateral: BrokenBarrierError | None = None
        cause: BaseException | None = None
        for (arm, _), item in zip(calls, raw, strict=True):
            if isinstance(item, BrokenBarrierError):
                collateral = collateral or item
            elif isinstance(item, BaseException):
                cause = cause or item
            else:
                # Deliberately not `_note` here: that list is what the
                # teardown kills, and a reconnect or a wake creates
                # nothing. A successful `/connect` echoes `sandboxID`,
                # so recording it would put a sandbox this phase did
                # not create into `sweep_admitted`'s kill list and
                # delete it out from under the fixture or staged fleet.
                arms[arm].append(item)
        self._last_spend_at = time.monotonic()
        if cause is not None or collateral is not None:
            # Record what did come back before raising: a round that died
            # mid-flight is exactly the one whose partial histograms are worth
            # reading, and the verdicts never get to run. That is the `cause`
            # path, where a worker raised after its peers had already answered.
            # On a *barrier timeout* there is nothing to record and the empty
            # histograms are themselves the signal: breaking the barrier raises
            # `BrokenBarrierError` in every waiting party, so no worker ever
            # reaches its request and the arms come back empty. Keyed off
            # `label` like the verdicts are, so a round's evidence lands under
            # one family of names whichever way it ended.
            for arm in arms:
                self._fixture.record(f"{label}_{arm}_statuses", status_histogram(arms[arm]))
        if cause is not None:
            raise cause
        if collateral is not None:
            # No worker raised, so the barrier timed out: some request never
            # reached the gate within the round's own timeout. Bare, that is a
            # `BrokenBarrierError` from the threading module with nothing
            # naming the round.
            raise AssertionError(
                f"the {what}'s {total} requests did not all reach the barrier within "
                f"{timeout:.3g}s, so they were never released together"
            ) from collateral
        return arms

    def stage_suspended_fleet(self, count: int) -> list[str]:
        """Create `count` sandboxes, suspend each, and return their ids.

        Call this before `enable_limiters`, or these cold creates spend the
        tokens the wake flood needs. Each member must reach a stored
        `suspended`, not just an ECW `paused`: a still-`suspending` row can
        already render `paused`, and `activation_class` charges it no token, as
        for a running one, so the flood would silently become a reconnect round
        and pass. The fleet stays small because each member is a real VM boot.
        """

        if not 1 <= count <= 4:
            raise ValueError(
                f"refusing a suspended fleet of {count}: each member is a real VM boot, and "
                "four distinct restore targets are already enough for the flood"
            )
        if self._restore_env is not None or self._redis_back is not None:
            raise SmokeConfigurationError(
                "stage_suspended_fleet must run before enable_limiters or take_redis_down"
            )
        staged: list[str] = []
        try:
            for _ in range(count):
                sandbox = self._fixture.sandboxes.create(metadata=self._wake_metadata)
                sandbox.pause()
                self._await_suspended(
                    sandbox,
                    "a staged wake target never reported itself suspended, so the flood would "
                    "have been a reconnect storm against a running sandbox and would have "
                    "passed without exercising the restore path at all",
                )
                # The handle, not just the id: the live-cap phase re-suspends this
                # same fleet, and `pause` is on the handle. Re-staging from ids
                # would mean new sandboxes and new boots for a phase that needs
                # neither.
                self._wake_fleet.append(sandbox)
                staged.append(sandbox.sandbox_id)
        finally:
            self._redefer_lease_above_sandboxes()
        self._wake_targets = staged
        self._fixture.record("wake_fleet", list(staged))
        return staged

    def _await_suspended(self, sandbox: EcwSandbox, failure: str) -> None:
        """Block until the public API reports this sandbox `paused` and its row
        is stored `suspended`.

        The fixture's own `wait_until_paused` only reads the scenario's
        sandbox, so the poll is written out here for a collection member. Its
        timeout and interval are the harness's own lifecycle values rather
        than new constants, so a bench whose transitions are slow is already
        accounted for. `failure` is the verdict message when it never does.
        """

        deadline = time.monotonic() + self._fixture.lifecycle_transition_timeout
        while True:
            info = sandbox.try_get_info()
            lifecycle = (
                self._control.sandbox_lifecycle(sandbox.sandbox_id)
                if info is not None and info.state is EcwSandboxState.PAUSED
                else None
            )
            if lifecycle == "suspended":
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                failure,
                sandbox_id=sandbox.sandbox_id,
                observed_state=(info.state if info is not None else "deleted"),
                stored_lifecycle=lifecycle,
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)

    def wake_flood(
        self,
        suspended: Sequence[str],
        running: str,
        *,
        noisy: int = 20,
        control: int = 3,
        timeout: float = 90,
        label: str = "wake_flood",
    ) -> tuple[list[LimiterProbeOutcome], list[LimiterProbeOutcome]]:
        """Flood one client's wakes while another client only reconnects.

        Like `isolation_round`, but the noisy targets are suspended, so each
        admitted request holds its per-client slot for a whole restore
        (`routes/e2b/lifecycle.rs`): the saturation shape the limiter exists
        for.

        The control arm must stay connect-only against a running sandbox.
        That's the only request here that takes no activation token, so the
        per-client gate is the only shared resource in its way; a control arm
        that created or woke would draw the cluster limiter's 429s and report
        on the wrong limiter. That's why `running` must be outside `suspended`.
        """

        if not suspended:
            raise ValueError(
                "refusing a wake flood with no suspended targets: every request would be a "
                "reconnect to a running sandbox and nothing would take the restore path"
            )
        if running in set(suspended):
            raise ValueError(
                f"refusing {running} as the control target: it is one of the suspended wake "
                "targets, so the control arm would wake too and queue behind the cluster "
                "activation bucket instead of testing the per-client gate"
            )
        if not 1 <= control <= self._per_client_cap:
            raise ValueError(
                f"refusing a control arm of {control} against a per-client cap of "
                f"{self._per_client_cap}: it is asserted to succeed, so it must fit inside "
                "its own cap, and an empty arm would assert nothing at all"
            )
        if noisy <= self._per_client_cap:
            raise ValueError(
                f"refusing a noisy arm of {noisy} against a per-client cap of "
                f"{self._per_client_cap}: it has to be able to exceed the cap"
            )
        body: Mapping[str, object] = {"timeout": 30}
        # Round-robin rather than all-at-one-target: once the first wake moves
        # a sandbox off `Suspended`, `activation_class` stops charging the
        # restore class for requests against it, so with one target every
        # admitted wake after the first would be as cheap as a reconnect and
        # the arm would stop being a wake flood after its first member.
        noisy_specs = [
            (f"/sandboxes/{suspended[index % len(suspended)]}/connect", body)
            for index in range(noisy)
        ]
        control_specs = [(f"/sandboxes/{running}/connect", body)] * control
        self._arm_ids[label] = (self._noisy_id, self._control_id)
        arms = self._release_together(
            [
                ("noisy", self._noisy_id, noisy_specs),
                ("control", self._control_id, control_specs),
            ],
            timeout=timeout,
            label=label,
            what="wake flood",
        )
        # Recorded here so the histograms survive whichever verdict ends the
        # phase. The caller judges isolation with `expect_control_isolated`
        # first and only then `expect_flood_restored`, so a shared bucket that
        # refused the noisy arm reads as sharing, not as a flood that woke
        # nothing.
        for arm, outcomes in arms.items():
            self._fixture.record(f"{label}_{arm}_statuses", status_histogram(outcomes))
        return arms["noisy"], arms["control"]

    def expect_flood_restored(
        self,
        suspended: Sequence[str],
        noisy: Sequence[LimiterProbeOutcome] = (),
        *,
        label: str,
    ) -> None:
        """At least one flooded target must have left `paused`.

        Without this the phase cannot tell a wake flood from the reconnect
        round that runs beside it. Every verdict in `expect_control_isolated`
        is about statuses, and a `/connect` that answered 200 without taking
        the restore path produces exactly the same statuses as one that took
        it -- so the phase would have passed on the cheap reconnects it exists
        to be different from, and its own comment about admitted requests
        loading a snapshot would have gone untested.

        The weaker "at least one" rather than "all": an admitted wake can
        still be refused downstream by the cluster activation bucket, which
        charges a `Restore` token per wake, so requiring every target to wake
        would red on a bench whose bucket ran dry. One is what separates the
        two phases; more is a matter of how many tokens were going spare.
        """

        # A flood with no admitted wake can't have woken anything, and saying
        # "reconnect storm" would blame the wake path for what a limiter did.
        # Judged first, so it doesn't sit out the whole lifecycle wait.
        if noisy:
            self._fixture.verify(
                any(200 <= o.status < 300 for o in noisy),
                f"{label}: no noisy wake was admitted, so nothing could have woken",
                noisy_statuses=status_histogram(list(noisy)),
                noisy_cluster_rejections=sum(
                    1 for o in noisy if is_limiter_rejection(o, SERVER_LIMIT_MESSAGE)
                ),
            )
        targets = {sandbox_id for sandbox_id in suspended}
        staged = [s for s in self._wake_fleet if s.sandbox_id in targets]
        self._fixture.verify(
            bool(staged),
            f"{label}: none of the flooded ids is a staged fleet member, so nothing here can "
            "say whether the flood took the restore path",
            flooded=list(suspended),
        )
        deadline = time.monotonic() + self._fixture.lifecycle_transition_timeout
        woke: list[str] = []
        states: dict[str, EcwSandboxState | str] = {}
        pod_phases: dict[str, list[str]] = {}
        lifecycles: dict[str, str | None] = {}
        while True:
            states = {}
            pod_phases = {}
            lifecycles = {}
            for sandbox in staged:
                sid = sandbox.sandbox_id
                info = sandbox.try_get_info()
                state: EcwSandboxState | str = (
                    info.state if info is not None and info.state is not None else "deleted"
                )
                states[sid] = state
                if isinstance(state, EcwSandboxState) and state is not EcwSandboxState.PAUSED:
                    pod_phases[sid] = self._control.sandbox_live_pod_phases(sid)
                    lifecycles[sid] = self._control.sandbox_lifecycle(sid)
            # A member that is *gone* is not a member that woke. `try_get_info`
            # answers `None` once the sandbox is reaped, and a staged sandbox
            # can be reaped by its own timeout while the flood is being
            # refused -- so counting "deleted" as "left paused" would pass this
            # verdict on a round that restored nothing, which is the one thing
            # it exists to catch.
            # ECW renders `running` for every lifecycle but paused and
            # suspended, and the restore pod itself enters `Running` before the
            # row flips out of `restoring`. Requiring a live Running pod *and*
            # a stored `started` row matches `resuspend_fleet` and keeps the
            # poll running until a restore actually settles.
            woke = [
                sid
                for sid in pod_phases
                if "Running" in pod_phases[sid] and lifecycles[sid] == "started"
            ]
            if woke or time.monotonic() >= deadline:
                break
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record(f"{label}_target_states", {k: str(v) for k, v in states.items()})
        self._fixture.verify(
            bool(woke),
            f"{label}: none of the {len(staged)} flooded targets reached started with a Running "
            "pod, so the flood was answered without completing a restore and this was a reconnect "
            "storm, not a wake flood",
            target_states={k: str(v) for k, v in states.items()},
            target_pod_phases=pod_phases,
            target_lifecycles=lifecycles,
        )

    def isolation_round(
        self,
        sandbox_id: str,
        *,
        noisy: int = 20,
        control: int = 3,
        timeout: float = 90,
        label: str = "isolation",
    ) -> tuple[list[LimiterProbeOutcome], list[LimiterProbeOutcome]]:
        """Reconnect as two separately-bucketed clients at the same instant.

        Both arms wait on one barrier so a limiter keying every client to one
        bucket can't pass by having the control requests go out after the noisy
        ones drained. Release isn't arrival, and probes stamp only completion,
        so the round is evidence rather than proof (see
        `expect_control_isolated`).

        `control` is bounded by the per-client cap because the arm is asserted
        to succeed; a wider arm would rely on slot churn and read a lost race
        as an isolation failure. The scenario sizes it at the cap, so the
        per-run bucket id that keeps a predecessor's members out is load
        bearing.
        """

        if not 1 <= control <= self._per_client_cap:
            raise ValueError(
                f"refusing a control arm of {control} against a per-client cap of "
                f"{self._per_client_cap}: it is asserted to succeed, so it must fit inside "
                "its own cap, and an empty arm would assert nothing at all"
            )
        if noisy <= self._per_client_cap:
            raise ValueError(
                f"refusing a noisy arm of {noisy} against a per-client cap of "
                f"{self._per_client_cap}: it has to be able to exceed the cap"
            )
        body: Mapping[str, object] = {"timeout": 30}
        path = f"/sandboxes/{sandbox_id}/connect"
        self._arm_ids[label] = (self._round_noisy_id, self._round_control_id)
        arms = self._release_together(
            [
                ("noisy", self._round_noisy_id, [(path, body)] * noisy),
                ("control", self._round_control_id, [(path, body)] * control),
            ],
            timeout=timeout,
            label=label,
            what="isolation round",
        )
        return arms["noisy"], arms["control"]

    def resuspend_fleet(self) -> list[str]:
        """Suspend the staged fleet again, after a phase woke it.

        The live-cap phase needs suspended targets and the wake flood leaves
        them running, so the same sandboxes are put back rather than replaced:
        a suspend costs no boot, a create does.
        """

        self._fixture.verify(
            bool(self._wake_fleet),
            "no fleet has been staged, so there is nothing to re-suspend and the live-cap "
            "phase would have no activation to refuse",
        )
        for sandbox in self._wake_fleet:
            # The wake flood only has to restore one member, so another can
            # still be suspended, and suspending it again finds no pod. A
            # member that's gone is left to `_await_suspended`'s verdict.
            # A member ECW calls running can still be restoring, behind a
            # flood request that timed out, and a suspend claim only takes a
            # `started` or `paused` row (`claim_suspend_execution`), so one
            # mid-restore would raise rather than judge. So wait for its live
            # Running pod and a row the claim accepts.
            deadline = time.monotonic() + self._fixture.lifecycle_transition_timeout
            settled = False
            while True:
                info = sandbox.try_get_info()
                settled = (
                    info is None
                    or info.state is EcwSandboxState.PAUSED
                    or (
                        "Running" in self._control.sandbox_live_pod_phases(sandbox.sandbox_id)
                        and self._control.sandbox_lifecycle(sandbox.sandbox_id)
                        in ("started", "paused")
                    )
                )
                if settled or time.monotonic() >= deadline:
                    break
                self._fixture.wait(self._fixture.lifecycle_poll_interval)
            self._fixture.verify(
                settled,
                "a fleet member was still restoring after the lifecycle timeout, so it "
                "couldn't be suspended again for the live-cap phase",
                sandbox_id=sandbox.sandbox_id,
            )
            if info is not None and info.state is not EcwSandboxState.PAUSED:
                sandbox.pause()
            self._await_suspended(
                sandbox,
                "a fleet member didn't return to paused, so the live-cap phase has no "
                "suspended target to wake",
            )
        # And wait for the capped client to hold no live sandbox. The cap
        # counts a client's *sandboxes* with at least one pod that is neither
        # terminal nor deleting, so what has to drain here is that set --
        # otherwise the phase's first wake is refused and it reds on `the cap
        # admitted`, reporting a working cap as a broken one.
        deadline = time.monotonic() + self._fixture.lifecycle_transition_timeout
        while True:
            held = self._control.live_client_sandboxes(self._noisy_id)
            if not held:
                break
            self._fixture.verify(
                time.monotonic() < deadline,
                "the re-suspended fleet still has pods labelled for the capped client, so "
                "its live cap is already spent before the phase begins",
                held=held,
            )
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record("wake_fleet_resuspended", list(self._wake_targets))
        return list(self._wake_targets)

    def enable_live_cap(self, cap: int, *, rollout_timeout: float = 240) -> None:
        """Put a live-sandbox cap of `cap` on every allowlisted client key.

        A separate rollout from `enable_limiters`, deliberately: in force
        during the wake flood it would refuse that flood's admitted wakes with
        *its* 429 instead of leaving them to the concurrency gate, and the
        flood's verdicts would be reading this cap's work. The restore is
        already armed -- these keys are in `LIMITER_ENV_KEYS`, so
        `enable_limiters` captured their standing values before anything wrote
        them, and the Lease carries the same record.

        The `unknown` key is pinned to no cap. Both arms are allowlisted, so
        neither lands there, and leaving it unset would let a standing value
        decide what the shared bucket does while this phase ran.
        """

        if not 1 <= cap <= 4:
            raise ValueError(
                f"refusing a live cap of {cap}: the phase wakes one staged sandbox per "
                "admission, and each is a real VM"
            )
        self._fixture.verify(
            self._restore_env is not None,
            "the live cap was enabled before the limiter env was captured, so nothing would "
            "put the standing value back",
        )
        self._live_cap = cap
        self._control.set_public_api_env(
            {
                "SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT": str(cap),
                "SPACE_API_CLIENT_LIVE_CAP_UNKNOWN": "0",
            }
        )
        self._control.wait_public_api_ready(timeout=rollout_timeout)
        self._fixture.record("live_cap", cap)

    def connect_sequentially(
        self,
        targets: Sequence[str],
        *,
        client_id: str | None = None,
        timeout: float = 90,
    ) -> list[LimiterProbeOutcome]:
        """Reconnect to each target in turn as one client, in order.

        A suspended target makes the request a wake; a running one makes it a
        plain reconnect. Both are the same call, and the live-cap phase needs
        both: the capped client wakes, and the control client reconnects to
        something already running.

        Sequential on purpose, unlike the flood: the live cap is a statement
        about how many a client may *hold*, so the question is what the n-th
        request is told once the first n-1 are held, and a concurrent fan-out
        would make the admitted count a race instead of an answer.
        """

        body: Mapping[str, object] = {"timeout": 30}
        outcomes: list[LimiterProbeOutcome] = []
        for sandbox_id in targets:
            outcomes.append(
                self._probe.race_ecw_request(
                    "POST",
                    f"/sandboxes/{sandbox_id}/connect",
                    body,
                    client_id=client_id or self._noisy_id,
                    timeout=timeout,
                )
            )
            self._last_spend_at = time.monotonic()
        return outcomes

    def expect_live_cap_plateau(
        self,
        outcomes: list[LimiterProbeOutcome],
        control: list[LimiterProbeOutcome],
        *,
        targets: Sequence[str] | None = None,
        capped_reconnects: Sequence[LimiterProbeOutcome] = (),
        client_id: str | None = None,
        label: str = "live_cap",
    ) -> None:
        """A client's live sandboxes stop at its cap; another client is untouched.

        `outcomes` are the capped client's wakes in request order, and
        `targets`, when given, the sandbox each one woke, index for index.

        The verdicts run most-specific first: the cap admitted something (a
        cap of zero admits nothing and isn't a cap), refused something, with
        `Retry-After` on every refusal, admitted no more than the cap, and
        refused only once the client held its cap. The substrate count then
        has to sit between the admissions and the cap: `<= cap` alone passes
        with zero sandboxes, an admission that placed nothing. With `targets`,
        every admitted target has to be among them. Last, the capped client's
        own reconnects to a running sandbox and every control reconnect
        succeed: a reconnect to a started sandbox isn't an activation
        (`activation_class` answers `None`), so the cap must never answer it.
        """

        cap = self._live_cap
        admitted = [o for o in outcomes if 200 <= o.status < 300]
        refused = [o for o in outcomes if is_limiter_rejection(o, LIVE_CAP_MESSAGE)]
        # The arm's own client, not a hard-coded one: `connect_sequentially`
        # takes a `client_id`, and counting a different client's pods would
        # make the pod verdict pass at `0 <= cap` -- the exact shape it exists
        # to catch.
        capped = client_id or self._noisy_id
        # Polled up to the admitted count rather than read once: a restore
        # hands its sandbox to a new pod, and a read between the old pod
        # going and the new one qualifying would undercount a correct cap.
        deadline = time.monotonic() + self._fixture.lifecycle_transition_timeout
        while True:
            live = self._control.live_client_sandboxes(capped)
            if len(live) >= len(admitted) or time.monotonic() >= deadline:
                break
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record(f"{label}_statuses", status_histogram(outcomes))
        self._fixture.record(f"{label}_control_statuses", status_histogram(control))
        self._fixture.record(f"{label}_live_sandboxes", len(live))
        self._fixture.record(
            f"{label}_capped_reconnect_statuses", status_histogram(list(capped_reconnects))
        )
        self._fixture.verify(
            bool(outcomes) and bool(control),
            f"{label}: an arm was empty, so the round asked the cap nothing",
            woken=len(outcomes),
            control=len(control),
        )
        self._fixture.verify(
            bool(admitted),
            f"{label}: not one of the {len(outcomes)} wakes was admitted, so nothing here "
            f"shows a cap of {cap} admitting up to it",
            statuses=status_histogram(outcomes),
            # The drain phase empties the cluster activation bucket by
            # construction and a wake charges a `Restore` token, so an arm
            # this phase finds entirely refused may have been turned away by
            # the *activation* limiter rather than the cap. Counted here for
            # the same reason `expect_control_isolated` counts it: without it
            # a red reads as a broken cap when the bucket was the cause.
            cluster_rejections=sum(
                1 for o in outcomes if is_limiter_rejection(o, SERVER_LIMIT_MESSAGE)
            ),
        )
        self._fixture.verify(
            bool(refused),
            f"{label}: no wake drew a 429 '{LIVE_CAP_MESSAGE}' after {len(admitted)} were "
            f"admitted against a cap of {cap}, so the cap never fired",
            statuses=status_histogram(outcomes),
        )
        self._fixture.verify(
            all(has_retry_after_seconds(o) for o in refused),
            f"{label}: a live-cap 429 carried no Retry-After delta-seconds",
            retry_after=[o.retry_after for o in refused],
        )
        self._fixture.verify(
            len(admitted) <= cap,
            f"{label}: {len(admitted)} wakes were admitted against a cap of {cap}",
            statuses=status_histogram(outcomes),
        )
        early = [
            index
            for index, o in enumerate(outcomes)
            if is_limiter_rejection(o, LIVE_CAP_MESSAGE)
            and sum(1 for p in outcomes[:index] if 200 <= p.status < 300) < cap
        ]
        self._fixture.verify(
            not early,
            f"{label}: the cap refused a wake before the client held {cap} admitted "
            "sandboxes",
            refused_at=early,
            statuses=[o.status for o in outcomes],
        )
        self._fixture.verify(
            len(admitted) <= len(live) <= cap,
            f"{label}: the client holds {len(live)} live sandboxes against "
            f"{len(admitted)} admitted wakes and a cap of {cap}",
            live_sandboxes=live,
            admitted=len(admitted),
        )
        if targets is not None:
            self._fixture.verify(
                len(targets) == len(outcomes),
                f"{label}: {len(targets)} targets for {len(outcomes)} wakes",
            )
            admitted_targets = sorted(
                {t for t, o in zip(targets, outcomes) if 200 <= o.status < 300}
            )
            missing = [t for t in admitted_targets if t not in live]
            self._fixture.verify(
                not missing,
                f"{label}: an admitted wake's sandbox isn't among the client's live sandboxes",
                missing=missing,
                live_sandboxes=live,
            )
        self._fixture.verify(
            all(200 <= o.status < 300 for o in capped_reconnects),
            f"{label}: the capped client's reconnect to an already-running sandbox was "
            "refused, though a reconnect is no activation. A keepalive that found the "
            "runtime unreachable escalates the connect to a wake (`connect_touch_recovery`), "
            "which the cap may refuse, so check the target's state before reading this as "
            "the cap charging a reconnect",
            statuses=status_histogram(list(capped_reconnects)),
        )
        self._fixture.verify(
            all(200 <= o.status < 300 for o in control),
            f"{label}: the control client's reconnect to an already-running sandbox was "
            "refused while another client sat at its live cap",
            control_statuses=status_histogram(control),
        )

    def expect_control_isolated(
        self,
        noisy: list[LimiterProbeOutcome],
        control: list[LimiterProbeOutcome],
        *,
        label: str = "isolation",
    ) -> None:
        """One client at its cap must not spend another's reconnect budget.

        Both arms' histograms are recorded first, so a red round keeps its
        evidence. The verdicts run most-specific first: the noisy arm drew
        per-client rejections (else the control ran beside an idle limiter),
        every control request was answered, none was rejected by the
        per-client limiter, the noisy arm got at least one admission (a
        shared bucket whose slots all went to the control arm would otherwise
        pass), and every control reconnect returned 2xx.

        The round doesn't prove the two ids are on separate buckets: slots
        churn while a wave is in flight (`client_concurrency.lua` releases by
        `ZREM`), so a shared bucket can pass every verdict given lucky
        timings. The timing-free proof is the unit test
        `limiter_at_cap_does_not_spend_another_allowlisted_clients_budget`.
        """

        noisy_id, control_id = self._arm_ids.get(label, (self._noisy_id, self._control_id))

        noisy_rejections = [o for o in noisy if is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)]
        noisy_admitted = [o for o in noisy if 200 <= o.status < 300]
        noisy_unanswered = [o for o in noisy if o.status == 0]
        unanswered = [o for o in control if o.status == 0]
        control_rejections = [
            o for o in control if is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)
        ]
        succeeded = [o for o in control if 200 <= o.status < 300]
        self._fixture.record(f"{label}_noisy_statuses", status_histogram(noisy))
        self._fixture.record(f"{label}_control_statuses", status_histogram(control))
        # Before anything reads the arms. An empty control arm satisfies three
        # of the verdicts below by vacuity — nothing was unanswered, nothing
        # was rejected, and every one of zero requests succeeded — so the
        # round would pass having asked the limiter nothing at all. The span
        # arithmetic would also raise `min() arg is an empty sequence` rather
        # than a verdict. `isolation_round` cannot produce this, but this
        # method is callable on its own.
        self._fixture.verify(
            bool(noisy) and bool(control),
            f"{label}: an arm was empty, so the round asked the limiter nothing",
            noisy=len(noisy),
            control=len(control),
        )
        # Recorded, not asserted. A healthy round can finish every noisy
        # request before the first control one — the rejections come back in
        # milliseconds while an admitted reconnect does real work — so
        # disjoint completion ranges do not imply the arms were serialized,
        # and requiring them to intersect would red a correctly-isolating
        # limiter. The spans go into the evidence so a reader can see a run's
        # shape without the round ruling on it.
        noisy_span = (min(o.finished_at for o in noisy), max(o.finished_at for o in noisy))
        control_span = (
            min(o.finished_at for o in control),
            max(o.finished_at for o in control),
        )
        self._fixture.record(f"{label}_noisy_span_s", round(noisy_span[1] - noisy_span[0], 3))
        self._fixture.record(
            f"{label}_control_span_s", round(control_span[1] - control_span[0], 3)
        )
        self._fixture.verify(
            bool(noisy_rejections),
            f"{label}: the {len(noisy)}-deep storm as '{noisy_id}' drew no 429 "
            f"'{PER_CLIENT_LIMIT_MESSAGE}', so nothing was over cap while the control arm ran",
            noisy_statuses=status_histogram(noisy),
        )
        self._fixture.verify(
            not unanswered,
            f"{label}: {len(unanswered)} of {len(control)} control reconnects got no status at all",
            control_statuses=status_histogram(control),
        )
        self._fixture.verify(
            not control_rejections,
            f"{label}: '{control_id}' took {len(control_rejections)} 429 "
            f"'{PER_CLIENT_LIMIT_MESSAGE}' while '{noisy_id}' was over cap, so "
            "the two clients are not on separate buckets",
            control_statuses=status_histogram(control),
        )
        self._fixture.verify(
            bool(noisy_admitted),
            f"{label}: not one of '{noisy_id}''s {len(noisy)} requests was admitted "
            f"({len(noisy) - len(noisy_unanswered)} answered non-2xx, "
            f"{len(noisy_unanswered)} unanswered), so nothing it did distinguishes one shared bucket from "
            "two. Check `noisy_cluster_rejections` before reading this as a per-client "
            "result: a noisy arm the *cluster* limiter turned away says nothing about "
            "per-client buckets either way",
            noisy_statuses=status_histogram(noisy),
            noisy_unanswered=len(noisy_unanswered),
            noisy_cluster_rejections=sum(
                1 for o in noisy if is_limiter_rejection(o, SERVER_LIMIT_MESSAGE)
            ),
        )
        self._fixture.verify(
            len(succeeded) == len(control),
            f"{label}: only {len(succeeded)} of {len(control)} control reconnects succeeded; a "
            "control client has to keep reconnecting successfully while the noisy one is throttled",
            control_statuses=status_histogram(control),
            # A control request the *cluster* limiter turned away says nothing
            # about isolation: the per-client gate stayed quiet and the round
            # simply ran without an activation token. Counted separately so a
            # red round is not read as a per-client failure.
            control_cluster_rejections=sum(
                1 for o in control if is_limiter_rejection(o, SERVER_LIMIT_MESSAGE)
            ),
            sample_messages=sorted({o.message[:120] for o in control if not 200 <= o.status < 300})[
                :4
            ],
        )

    def burst_connects(self, sandbox_id: str, count: int = 20) -> list[LimiterProbeOutcome]:
        # The connect body is a SandboxTimeoutRequest with a REQUIRED timeout:
        # without it axum answers 422 before the handler reserves the
        # per-client slot, so the limiter would never be reached.
        return self.burst(count, "POST", f"/sandboxes/{sandbox_id}/connect", {"timeout": 30})

    def expect_per_client_limited(self, outcomes: list[LimiterProbeOutcome], *, label: str) -> None:
        """At least one 429 with the per-client message, and it carries Retry-After."""

        rejections = [o for o in outcomes if is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)]
        histogram = status_histogram(outcomes)
        self._fixture.record(f"{label}_statuses", histogram)
        self._fixture.verify(
            bool(rejections),
            f"{label}: no 429 '{PER_CLIENT_LIMIT_MESSAGE}' over {len(outcomes)} concurrent calls",
            statuses=histogram,
            sample_messages=sorted({o.message[:120] for o in outcomes})[:4],
        )
        self._fixture.verify(
            all(has_retry_after_seconds(o) for o in rejections),
            f"{label}: a per-client 429 carried no Retry-After delta-seconds",
            retry_after_values=[o.retry_after for o in rejections],
        )

    # --- the fault ----------------------------------------------------------

    def _restore_redis(self, replicas: int, *, rollout_timeout: float) -> None:
        self._control.scale_redis(replicas)
        self._control.wait_redis_rollout(timeout=rollout_timeout)

    def take_redis_down(self, *, timeout: float = REDIS_RESTORE_TIMEOUT_SECONDS) -> None:
        """Scale Redis to zero and wait until the store is really unreachable.

        Endpoints drain the moment the pod is NotReady, but during its grace
        window the pod still serves and the API's pooled connection keeps
        succeeding, so the limiter would still enforce. The wait covers pod
        deletion and empty endpoints.
        """

        if self._redis_back is not None:
            raise SmokeConfigurationError(
                "take_redis_down called while Redis is already scaled down"
            )
        self.acquire_mutation_lease()
        self._redefer_lease_above_sandboxes()
        # Read, arm the restoration, then write: a scale that times out after
        # the write must already have its scale-back deferred.
        previous = self._control.redis_replicas()
        self._fixture.record("redis_replicas_before", previous)
        self._redis_rollout_timeout = timeout
        self._redis_standing_replicas = max(previous, 1)
        self._redis_phase_restore_failed = False
        fallback_timeout = min(timeout, CLEANUP_REDIS_ROLLOUT_TIMEOUT_SECONDS)

        def back() -> None:
            # The scale's own reads and Lease writes come out of the same step
            # budget, so the rollout wait and its record clear get what they
            # leave, as one hard bound: a slow scale then fails as a bounded
            # wait rather than as the step's alarm cutting it off.
            step_deadline = time.monotonic() + CLEANUP_REDIS_BUDGET_SECONDS
            self._control.scale_redis(self._redis_standing_replicas)
            remaining = step_deadline - time.monotonic()
            if remaining <= REDIS_RECORD_CLEAR_SECONDS:
                raise RuntimeError(
                    "Redis was scaled back up, but the scale used the cleanup budget's "
                    "rollout share; its Lease record stays for the next holder's repair"
                )
            self._control.wait_redis_rollout(
                timeout=min(fallback_timeout + REDIS_RECORD_CLEAR_SECONDS, remaining),
                shared_deadline=True,
            )

        self._redis_back = self._scope.defer(
            "scale Redis back up",
            back,
            critical=True,
            timeout_seconds=CLEANUP_REDIS_BUDGET_SECONDS,
        )
        self._control.scale_redis(0)
        self._control.wait_redis_unreachable(timeout=timeout)

    def bring_redis_back(self, *, timeout: float | None = None) -> None:
        if self._redis_back is not None:
            effective_timeout = (
                self._redis_rollout_timeout if timeout is None else timeout
            )
            try:
                self._restore_redis(
                    self._redis_standing_replicas, rollout_timeout=effective_timeout
                )
            except BaseException:
                self._redis_phase_restore_failed = True
                raise
            self._redis_phase_restore_failed = False
            self._redis_back.disarm()
            self._redis_back = None

    def expect_enforcing_again(
        self, sandbox_id: str, *, callers: int = 20, budget: float = 60
    ) -> None:
        """Once the store is back, a connect burst draws a per-client 429 again.

        Connects rather than creates: the slot is reserved before the sandbox
        is fetched, so the verdict costs no VM boots. The API's pooled
        connections died with the old pod; the pool replaces them on use, so a
        first burst may still fail open. Bursts repeat until one is limited or
        the budget runs out, and the number it took is recorded.
        """

        deadline = self.now() + budget
        bursts = 0
        while True:
            bursts += 1
            outcomes = self.burst_connects(sandbox_id, callers)
            limited = any(is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE) for o in outcomes)
            if limited or self.now() >= deadline:
                break
            time.sleep(2)
        self._fixture.record("recovery_bursts", bursts)
        self.expect_per_client_limited(outcomes, label="per_client_after_recovery")

    def expect_fail_open(self, outcomes: list[LimiterProbeOutcome]) -> None:
        """With the store down every create must still admit a sandbox."""

        histogram = status_histogram(outcomes)
        self._fixture.record("fail_open_statuses", histogram)
        self._fixture.verify(
            bool(outcomes) and all(o.status == 201 and o.sandbox_id for o in outcomes),
            "creates did not all admit sandboxes with Redis unreachable; every limiter, "
            "the live cap still at its test value included, must fail open",
            statuses=histogram,
            sample_messages=sorted({o.message[:120] for o in outcomes})[:4],
        )

    def expect_running(self, sandbox: EcwSandbox, *, label: str) -> None:
        """The reconnect target must still be running, or a reconnect is a wake.

        The fixture sandbox pauses at its own deadline, and a reconnect only
        ever pushes that out by its own timeout, so a long run can outlive it.
        A reconnect to a paused sandbox is then a wake the live cap or the
        cluster bucket may refuse, which would read as a reconnect refused.
        """

        info = sandbox.try_get_info()
        state = info.state if info is not None else None
        self._fixture.verify(
            state is EcwSandboxState.RUNNING,
            f"{label}: the reconnect target isn't running, so a reconnect to it would be a "
            "wake, and a refusal would say nothing about reconnects",
            sandbox_id=sandbox.sandbox_id,
            state=str(state),
        )
        # ECW renders an in-flight restore as running, but the connect route
        # reads the stored lifecycle too (`should_wake_on_connect`): a row left
        # `restoring`, or one at rest as `paused` or `suspended`, makes the
        # reconnect a wake even while ECW says running.
        lifecycle = self._control.sandbox_lifecycle(sandbox.sandbox_id)
        self._fixture.verify(
            (lifecycle or "").strip().lower() not in _RECONNECT_WAKE_LIFECYCLES,
            f"{label}: the reconnect target's row reads {lifecycle!r}, so the connect route "
            "would wake it, and a refusal would say nothing about reconnects",
            sandbox_id=sandbox.sandbox_id,
            lifecycle=lifecycle,
        )

    def judges_release_defaults(self) -> bool:
        """Whether the restored env is one `expect_restored_env_admits_the_burst` can judge.

        `restore_limiters` puts back the env this scenario captured, which is
        the release defaults on a stock bench but whatever an operator set on
        any other. A standing per-client cap of one would refuse part of the
        burst correctly, so the verdict only runs when nothing in that env is
        stricter than the defaults; otherwise the phase records what it found
        and makes no claim.
        """

        if self._standing is None:
            raise RuntimeError("the standing limiter env is read by enable_limiters, which hasn't run")
        tightened = limiter_tightenings(self._standing)
        self._fixture.record("default_admission_tightened_env", tightened)
        # A key unset in the literals may still come from an `envFrom`
        # source, which comes back once the restore drops the overrides, so
        # an unset key there isn't known to be at its release default.
        unset = sorted(key for key, value in self._standing.items() if value is None)
        if unset and self._standing_env_from:
            self._fixture.record("default_admission_unread_env_from", unset)
            return False
        return not tightened

    def expect_restored_env_admits_the_burst(self, outcomes: list[LimiterProbeOutcome]) -> None:
        """With the standing env restored, a burst the shrunk caps refuse draws no limiter refusal.

        This proves the restore took the scenario's caps away and the restored
        env admits what they refused: more creates are admitted than the
        largest count cap the run applied. Four creates exceed the shrunk
        activation burst and per-client cap of three and the live cap of one,
        and fit the release-default activation burst of 60. It doesn't tell per-client observe from
        enforce: four creates are under every release-default per-client cap
        (8 for an unlisted client, 32 for an allowlisted one), so the same
        answers come back in either mode. Call it only after
        `restore_limiters` and when `judges_release_defaults` holds.

        Only limiter rejections are judged, by message. A 429 from placement
        admission, or any other status, says nothing about the caps and is not
        this phase's business -- the same rule `expect_fail_open` follows.

        The burst only reaches the caps the probe's client still meets, so the
        limiter env is first read back from the Deployment and must equal the
        captured standing values: a listed-client live cap left set, with the
        allowlist that made the probe a listed client removed, is invisible to
        the burst and shows here.
        """

        if self._standing is None:
            raise RuntimeError("the standing limiter env is read by enable_limiters, which hasn't run")
        restored = self._control.public_api_env(LIMITER_ENV_KEYS)
        self._fixture.record("restored_limiter_env", dict(restored))
        leftover = sorted(
            key for key in LIMITER_ENV_KEYS if restored.get(key) != self._standing.get(key)
        )
        self._fixture.verify(
            not leftover,
            f"the public API's limiter env differs from its captured standing values after "
            f"the restore, in {leftover}, so the restore didn't take the scenario's caps away",
            restored={key: restored.get(key) for key in leftover},
            standing={key: self._standing.get(key) for key in leftover},
        )

        # Every limiter refusal, the live cap included: a cap the restore left
        # behind would answer with any of them, so a refusal it does not
        # recognise is one it would pass over in silence. The slot check's 503 counts too:
        # it is off at the release defaults (a standing env that turns it on
        # skips this phase), so a no-free-slot refusal here is that default
        # regressing. The empty-cluster guard's refusal isn't a cap: it answers
        # for a cluster with no eligible node, which the admitted check below
        # already reds on.
        rejections = [
            o
            for o in outcomes
            if is_limiter_rejection(o, SERVER_LIMIT_MESSAGE)
            or is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)
            or is_limiter_rejection(o, LIVE_CAP_MESSAGE)
            or (o.status == 503 and o.message.startswith(NO_FREE_SLOT_MESSAGE))
        ]
        histogram = status_histogram(outcomes)
        self._fixture.record("default_config_statuses", histogram)
        # Before the rejection check: at the defaults, "nothing was refused by
        # a limiter" is also true of a flood where every create failed for some
        # other reason, and by this point the earlier phases have left the
        # cluster holding a good many sandboxes. An assertion of absence needs a
        # possible presence.
        # A 201 with a sandbox id, as `expect_fail_open` requires: a 201 whose
        # body named no sandbox shows nothing was admitted. More of them than
        # the largest count cap this run applied (the shrunk activation burst,
        # the per-client cap the concurrent burst would meet, the live cap):
        # fewer would fit under the caps the restore was meant to take away.
        shrunk = max(self._cluster_burst, self._per_client_cap, self._live_cap)
        if len(outcomes) <= shrunk:
            raise SmokeConfigurationError(
                f"a burst of {len(outcomes)} can't exceed the shrunk caps' {shrunk}"
            )
        admitted = sum(1 for o in outcomes if o.status == 201 and o.sandbox_id)
        self._fixture.verify(
            admitted > shrunk,
            f"{admitted} of {len(outcomes)} creates were admitted with the standing env "
            f"restored, no more than the shrunk caps' {shrunk}, so this doesn't show the "
            "restore took them away",
            statuses=histogram,
            shrunk_caps=shrunk,
        )
        # A request with no answer can't show whether a limiter refused it.
        unanswered = sum(1 for o in outcomes if o.status == 0)
        self._fixture.verify(
            not unanswered,
            f"{unanswered} of {len(outcomes)} creates got no answer with the standing env restored, "
            "so the phase can't tell whether a limiter refused them",
            statuses=histogram,
        )
        self._fixture.verify(
            not rejections,
            f"{len(rejections)} of {len(outcomes)} requests were refused by a limiter with the "
            "standing env restored; the restore must take the scenario's caps away",
            statuses=histogram,
            limiter_rejections=[o.message[:120] for o in rejections],
        )

    def admitted_ids(self, outcomes: list[LimiterProbeOutcome]) -> list[str]:
        return [o.sandbox_id for o in outcomes if o.sandbox_id]

    @staticmethod
    def now() -> float:
        return time.monotonic()
