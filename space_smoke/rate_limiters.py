"""The two Redis-backed API rate limiters, proven end to end on a real cluster.

Kind runs both limiters against its Redis by default, as deployments do: the
charts render `SPACE_REDIS_URL` on the API, admin API and cluster-controller,
with the activation limiter at the binary defaults and per-client concurrency
in `observe`. This scenario shrinks the caps and switches per-client
concurrency to enforce on the public API, then drives real HTTP traffic at
them: sequential creates until the cluster activation token bucket answers
429 `Server rate limit:`, concurrent creates and connects for one
`X-Client-ID` until the per-client concurrency limiter answers 429
`Per-client rate limit:`, Redis scaled to zero to show both limiters admit
when their store is unreachable, and Redis back to show they enforce again.

The Deployment env override is a named, captured-and-restored, kind-only
operation behind one cluster Lease, which records the standing env and the
Redis replica count before either changes and clears each record only once
its restore succeeded. If this run is killed or a restore fails, the next
Kind scenario puts back what is recorded before it starts (see
`KindHarness.repair_orphaned_limiter_faults`). The Redis fault has its
compensation armed before dispatch; every sandbox the probes admit carries
run metadata the teardown kills by. The control surface is [`RateLimiterControl`]; the raw
requests go through [`RateLimitProbe`] so a 429's `Retry-After` survives.
"""

from __future__ import annotations

import math
import re
import time
import uuid
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, BrokenBarrierError, Event
from typing import TYPE_CHECKING

from space_smoke.adapters import ACTIVATION_CLIENT_LABEL, LIMITER_ENV_VALUE_MAX
from space_smoke.capabilities import (
    EcwSandbox,
    EcwSandboxState,
    LimiterProbeOutcome,
    RateLimiterControl,
    RateLimitProbe,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SERVER_LIMIT_MESSAGE = "Server rate limit:"
PER_CLIENT_LIMIT_MESSAGE = "Per-client rate limit:"
# The live cap's refusal: a 429 with a header-only `Retry-After`, built by
# `LiveCapRefusal::new`.
LIVE_CAP_MESSAGE = "Client live sandbox cap reached:"
# The activation limiter's 503s: the slot check's and the empty-cluster
# guard's, both under this prefix.
CAPACITY_LIMIT_MESSAGE = "Server capacity limit:"

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

    No answer at all, or a success that carried no id: either may have
    committed, so the sweep has to reconcile by metadata. Neither counts as
    admission evidence, which takes an id.
    """

    return outcome.status == 0 or (200 <= outcome.status < 300 and not outcome.sandbox_id)

class RateLimiterAssertions:
    """Drive the kind public API's limiters and judge their answers.

    Caps are low on purpose: every admitted create boots a real Firecracker
    sandbox. At the scenario's burst of 3 a full run admits eight to eighteen:
    the fixture's own, the suspended fleet, the preflight, the drain's
    admissions, what the per-client cap lets through on the create burst, the
    fail-open burst, and the release-default burst when it runs. The spread is
    the drain, the create burst (which admit nothing once the activation bucket
    is empty) and the release-default burst, which is skipped when the
    standing env tightens a limiter. On top of those creates come the
    attribution retry's create when the preflight drew the 429, and the
    restores the wake flood and the live-cap phase admit, each a real VM boot
    too. The teardown kills all of them by run metadata even when a phase
    fails mid-burst.
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
        self._client_id = "space-smoke"
        self._template = "seed"
        self._per_client_cap = 3
        self._live_cap = 0
        self._standing: Mapping[str, str | None] | None = None
        # Whether the public API container has `envFrom` sources, which the
        # literal read doesn't see.
        self._standing_env_from = False
        self._admitted: list[str] = []
        self._refill_rps = 0.0
        self._last_spend_at: float | None = None
        # Creates that got no answer (status 0): the API may still have
        # committed them, so the sweep reconciles by metadata for them too.
        self._uncertain_creates = 0
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
        # staged fleet out of `sweep_admitted`'s reach: that kills by
        # `self._metadata`, the ECW metadata filter is JSONB containment over
        # every supplied pair, and the two runs' tokens differ. The fleet's
        # cleanup is the one the collection arms before its create attempt,
        # and nothing else should be deleting out from under it.
        self._wake_metadata = {"space_smoke_role": "rate-limiter-wake-target"}
        self._wake_targets: list[str] = []
        self._wake_fleet: list[EcwSandbox] = []

    # --- environment shape ------------------------------------------------

    def acquire_mutation_lease(self) -> None:
        """Take the public API mutation Lease, unless this run already holds it.

        The scenario takes it before staging the wake fleet: those cold creates
        spend activation tokens, and a cooperating run that has the bucket
        shrunk and drained would refuse them.
        """

        if self._release_lease is None:
            release = self._control.acquire_public_api_mutation()
            self._release_lease = self._scope.defer(
                "release the public API mutation lease", release
            )

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
        # All five ids go out as one comma-separated value, and the env guard
        # refuses anything longer. The ceiling is imported rather than
        # restated: a bound that drifted from the regex would let this pass a
        # value `set_public_api_env` then rejects after the env is captured and
        # its restore deferred. The length is checked here, before any of that,
        # so a too-long id fails on the limit it broke rather than partway
        # through the setup. Length only: the env guard's character class is
        # still the one that rejects a malformed `client_id`.
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
        self._client_id = client_id
        self._template = template
        self._per_client_cap = per_client_cap
        self._refill_rps = float(cluster_refill_rps)
        self.acquire_mutation_lease()
        store = self._control.public_api_env((REDIS_URL_ENV,))[REDIS_URL_ENV]
        self._fixture.verify(
            bool(store),
            f"the public API has no {REDIS_URL_ENV}; both limiters would fail open, not enforce",
        )
        self._fixture.record("limiter_store", store)
        standing = self._control.public_api_env(LIMITER_ENV_KEYS)
        self._fixture.record("limiter_env_before", dict(standing))
        self._standing = standing
        self._standing_env_from = self._control.public_api_has_env_from()

        def restore() -> None:
            self._control.restore_public_api_env(standing)
            self._control.wait_public_api_ready(timeout=rollout_timeout)

        self._restore_env = self._scope.defer(
            "restore the public API limiter env", restore, critical=True
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
        self._scope.defer(
            "kill every sandbox the limiter probes admitted", self.sweep_admitted, critical=True
        )
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

        `expected` defaults to the client id the probes create under, which is
        the client whose request drove the activation.

        The label and the row are written as the activation proceeds, not
        before the create answers, so both are polled to a bound rather than
        read once. The wait expiring is *not* its own verdict, deliberately:
        the verdicts below say which of four things went wrong -- no pod at
        all, a pod never labelled, a pod naming another client, a row
        recording another client -- and a timeout verdict in front of them
        would replace all four with "time ran out". After the full wait those
        four are accurate statements, and `wait_expired` goes into the
        evidence so a reader can still tell "never written" from "not written
        within the wait".

        The verdicts run most-specific first:

        `a pod to judge` the sandbox has at least one pod. Every verdict below
        quantifies over the pod list, so an empty list satisfies all of them
        by vacuity -- the exact shape a create that never placed would leave.

        `labelled at all` no pod carries an empty label. Named apart from the
        match verdict so an unlabelled pod reads as the attribution never
        having been written, rather than as having been written wrong.

        `labelled with the activating client` every pod's label is `expected`.

        `recorded on the row` the sandbox row's `activating_client_id` is
        `expected`. The label and the row are each compared against the
        activating client rather than against each other, which is strictly
        stronger: two surfaces that agreed on the *wrong* id would pass an
        agreement check and fail these.
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

    def sweep_admitted(self) -> None:
        """Kill every sandbox a probe admitted, by run metadata.

        `kill_matching` treats zero matches as a create that never became
        visible and raises; here zero admits is a legitimate outcome (every
        probe rejected), so the sweep runs only once something was admitted.
        A create that got no answer may still have committed, so with only
        those the sweep still reconciles, and finding nothing is the expected
        outcome of a create that never landed.
        """

        if self._admitted:
            self._probe.kill_matching(self._metadata)
        elif self._uncertain_creates:
            try:
                self._probe.kill_matching(self._metadata)
            except RuntimeError as error:
                self._fixture.record(
                    "unanswered_creates_unmatched",
                    {"count": self._uncertain_creates, "error": str(error)},
                )

    def _note(self, outcome: LimiterProbeOutcome) -> LimiterProbeOutcome:
        if outcome.sandbox_id:
            self._admitted.append(outcome.sandbox_id)
        return outcome

    def restore_limiters(self, *, release_lease: bool = True) -> None:
        """Put the standing env back, and by default drop the mutation Lease.

        `release_lease=False` keeps the Lease while a caller goes on to judge
        the restored configuration: the Lease is what stops a cooperating
        smoke from writing this Deployment, so dropping it first would let
        another run change the env underneath those requests and leave this
        one reporting on a configuration it did not establish.
        """

        slots = ("_restore_env", "_release_lease") if release_lease else ("_restore_env",)
        for slot in slots:
            compensation = getattr(self, slot)
            if compensation is not None:
                setattr(self, slot, None)
                compensation.release()

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
            outcome = self._note(
                self._probe.race_ecw_request(
                    "POST",
                    "/sandboxes",
                    self._create_body(),
                    client_id=client_id or self._client_id,
                    timeout=timeout,
                )
            )
        except BaseException:
            # A create that raised (a truncated 201 body, say) may still have
            # committed, so the sweep has to reconcile by metadata.
            self._uncertain_creates += 1
            self._last_spend_at = time.monotonic()
            raise
        self._last_spend_at = time.monotonic()
        if _create_uncertain(outcome):
            self._uncertain_creates += 1
        return outcome

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

        With `creates`, admitted ids are noted for the sweep and every create
        that got no answer, succeeded without an id, or raised, counts as
        uncertain. A raise is
        re-raised only once every worker has finished, so the others' outcomes
        are still accounted for.
        """

        # Bounded, and aborted if a submit fails, so workers already at the
        # gate raise `BrokenBarrierError` instead of waiting on a party that
        # never arrives.
        barrier = Barrier(count, timeout=timeout)

        def gate() -> None:
            # `Barrier.wait` returns the caller's arrival index; the probe's
            # gate is a plain hook.
            barrier.wait()

        with ThreadPoolExecutor(max_workers=count) as pool:
            try:
                futures = [
                    pool.submit(
                        self._probe.race_ecw_request,
                        method,
                        path,
                        body,
                        client_id=client_id or self._client_id,
                        timeout=timeout,
                        gate=gate,
                    )
                    for _ in range(count)
                ]
            except BaseException:
                barrier.abort()
                raise
            outcomes: list[LimiterProbeOutcome] = []
            errors: list[BaseException] = []
            for future in futures:
                try:
                    outcomes.append(future.result())
                except BaseException as error:  # noqa: BLE001 - re-raised below
                    errors.append(error)
        self._last_spend_at = time.monotonic()
        if creates:
            # Only creates are noted for the sweep: a connect echoes the id of
            # a sandbox it didn't create, the fixture's own included, and the
            # sweep would then look for probe metadata that sandbox lacks.
            outcomes = [self._note(o) for o in outcomes]
            self._uncertain_creates += len(errors) + sum(1 for o in outcomes if _create_uncertain(o))
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

        with ThreadPoolExecutor(max_workers=total) as pool:
            futures = {
                arm: [pool.submit(released, client, path, body) for path, body in specs]
                for arm, client, specs in plan
            }
            # A worker that raises before it reaches the gate breaks the
            # barrier for every other one, so most of what comes back is
            # `BrokenBarrierError` — a consequence, not the cause. Collect
            # every outcome first and raise the cause if there was one, rather
            # than letting the first future's bystander error be what the
            # round reports.
            arms: dict[str, list[LimiterProbeOutcome]] = {}
            collateral: BrokenBarrierError | None = None
            cause: BaseException | None = None
            for arm, submitted in futures.items():
                answered: list[LimiterProbeOutcome] = []
                for future in submitted:
                    error = future.exception()
                    if error is None:
                        # Deliberately not `_note` here: that list is what the
                        # teardown kills by, and a reconnect or a wake creates
                        # nothing. A successful `/connect` echoes `sandboxID`,
                        # so recording it would put a sandbox this phase did
                        # not create into the kill list — and then
                        # `sweep_admitted` would call `kill_matching` on run
                        # metadata that sandbox does not carry, failing the
                        # teardown of a run whose every create was
                        # legitimately rejected.
                        answered.append(future.result())
                    elif isinstance(error, BrokenBarrierError):
                        collateral = collateral or error
                    else:
                        cause = cause or error
                arms[arm] = answered
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

        Call this **before** `enable_limiters`. Every create here is an
        ordinary cold activation, and at the shrunk cluster burst it would
        spend the very tokens the wake flood then needs. At the standing caps
        they draw on the same bucket, but the shrink caps its tokens at the new
        burst, so the flood starts from a full one.

        Each sandbox is confirmed `paused` on the ECW surface before the next
        is staged, because the whole point of the fleet is that a request
        against it takes the restore path. ECW pause is SPACE suspend
        (`e2b_native_pause_action` answers `SandboxAction::Suspend`), and a
        sandbox still `suspending` would answer a wake as a running one does:
        `activation_class` returns `None` for every lifecycle but `Suspended`,
        `Created` and `Stopped`, so the phase would quietly degrade into the
        reconnect round that already runs below, and pass.

        Polling the ECW state alone can't rule that out: `rendered_lifecycle`
        lets the stored lifecycle decide only for the at-rest and terminal
        states, so a `suspending` row follows its route node's observation,
        and that can already read `paused` while finalization is still
        running. So the wait also reads the row and holds out for a stored
        `suspended`.

        The fleet is deliberately small: each member is a real VM boot, and
        the flood only needs enough distinct suspended targets that the wakes
        the per-client gate admits are each doing real restore work rather
        than piling onto one in-flight restore.
        """

        if not 1 <= count <= 4:
            raise ValueError(
                f"refusing a suspended fleet of {count}: each member is a real VM boot, and "
                "four distinct restore targets are already enough for the flood"
            )
        staged: list[str] = []
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

        The same shape as `isolation_round`, with one difference that is the
        whole reason the phase exists: the noisy arm's targets are suspended,
        so each request the per-client gate admits loads a snapshot. The slot
        is `try_acquire`d at the top of the connect handler and held for the
        whole handler (`routes/e2b/lifecycle.rs`), so an admitted wake holds
        its slot for the length of a restore rather than the milliseconds a
        reconnect to a running sandbox takes. That is the saturation shape the
        per-client limiter was built for, and the reconnect round below cannot
        produce it: there, slots churn so fast that a control request finding a
        free slot says little.

        **The control arm is connect-only, against a sandbox that is already
        running.** It must stay that way. The property under test is
        per-client isolation, and the only shared resource that may stand
        between the control client and its answer is the per-client gate. A
        control arm that *created* would queue behind the shrunk cluster
        activation bucket instead, so its 429s would be the cluster limiter's
        and the round would be reporting on the wrong limiter — a red that
        looks like broken isolation and is not. A control arm that *woke*
        would do the same by the restore class. Reconnecting to a running
        sandbox is the one request on this surface that takes no activation
        token at all: `activation_class` answers `None` for every lifecycle
        but `Created`, `Stopped` and `Suspended`.

        `running` is therefore required to be outside `suspended`, and that
        check is load bearing rather than defensive: passing a staged member
        here would turn the control arm into a second wake flood and quietly
        move the round onto the cluster limiter.
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
        while True:
            states = {}
            for sandbox in staged:
                info = sandbox.try_get_info()
                states[sandbox.sandbox_id] = info.state if info is not None else "deleted"
            # A member that is *gone* is not a member that woke. `try_get_info`
            # answers `None` once the sandbox is reaped, and a staged sandbox
            # can be reaped by its own timeout while the flood is being
            # refused -- so counting "deleted" as "left paused" would pass this
            # verdict on a round that restored nothing, which is the one thing
            # it exists to catch.
            # ECW renders `running` for every lifecycle but paused and
            # suspended, so a member stuck restoring would pass a state check
            # alone. A live Running pod is what a finished restore leaves; a
            # prior generation's terminal or deleting pod doesn't count.
            woke = [
                sid
                for sid, state in states.items()
                if isinstance(state, EcwSandboxState)
                and state is not EcwSandboxState.PAUSED
                and "Running" in self._control.sandbox_live_pod_phases(sid)
            ]
            if woke or time.monotonic() >= deadline:
                break
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record(f"{label}_target_states", {k: str(v) for k, v in states.items()})
        self._fixture.verify(
            bool(woke),
            f"{label}: no flooded target left paused with a Running pod, so the {len(suspended)} requests "
            "were answered without restoring anything and this was a reconnect storm, not a "
            "wake flood",
            target_states={k: str(v) for k, v in states.items()},
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

        Both arms wait on one barrier, so the noisy fan-out is not merely
        launched first: every request is released together. That matters
        because the round's verdict is about what one client's saturation does
        to another, and if the control requests only went out after the noisy
        ones drained, a limiter that keyed every client to one bucket would
        still answer them cleanly and the round would pass on the defect it
        exists to catch.

        Releasing together is not the same as arriving together, though, and
        the barrier can only do the first. Nothing here establishes that the
        arms were in flight together: that would take each request's start
        time, and the probes stamp only when one finished. It is part of why
        this round is evidence rather than proof — see the note in
        `expect_control_isolated`.

        `control` is bounded by the per-client cap, and the scenario sizes it
        *at* the cap rather than under it — which is deliberate (a full arm
        succeeding is the stronger evidence) but leaves no slack, so the
        per-run bucket id that keeps a predecessor's members out is load
        bearing, not tidiness. The arm is asserted to
        succeed, and sizing it to its own bucket is what keeps that assertion
        about isolation rather than about its own load. A wider arm might
        still pass — slots churn, so a wave can admit more than `cap` — but it
        would be relying on that race, and when it lost the round would read
        as an isolation failure. The bound is the conservative choice, not a
        statement about what a wider arm must do.
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

    def take_redis_down(self, *, timeout: float = 120) -> None:
        """Scale Redis to zero and wait until the store is really unreachable.

        Endpoints drain the moment the pod is NotReady, but during its grace
        window the pod still serves and the API's pooled connection keeps
        succeeding, so the limiter would still enforce. The wait covers pod
        deletion and empty endpoints.
        """

        # Read, arm the restoration, then write: a scale that times out after
        # the write must already have its scale-back deferred.
        previous = self._control.redis_replicas()
        self._fixture.record("redis_replicas_before", previous)

        def back() -> None:
            self._control.scale_redis(max(previous, 1))
            self._control.wait_redis_rollout(timeout=timeout)

        self._redis_back = self._scope.defer("scale Redis back up", back, critical=True)
        self._control.scale_redis(0)
        self._control.wait_redis_unreachable(timeout=timeout)

    def bring_redis_back(self) -> None:
        if self._redis_back is not None:
            release = self._redis_back
            self._redis_back = None
            release.release()

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

    def judges_release_defaults(self) -> bool:
        """Whether the restored env is one `expect_admitted_by_default` can judge.

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

    def expect_admitted_by_default(self, outcomes: list[LimiterProbeOutcome]) -> None:
        """At the release defaults, the fail-open burst draws no limiter rejection.

        The other phases prove the limiters reject once they are configured to.
        This proves the converse, which is what the enablement work actually
        risks: that landing the caps starts refusing traffic the release
        defaults admitted. Those defaults aren't "no limiter": the activation
        bucket is on at a burst and refill of 60, which a four-create burst
        can't exhaust, and per-client concurrency observes without refusing.
        Call it only after `restore_limiters` and when
        `judges_release_defaults` holds.

        Only limiter rejections are judged, by message. A 429 from placement
        admission, or any other status, says nothing about the caps and is not
        this phase's business -- the same rule `expect_fail_open` follows.
        """

        # Every 429 limiter refusal, the live cap included: the phase's whole
        # subject is that landing these caps must not start refusing traffic a
        # cluster without them admitted, so a refusal it does not recognise is
        # one it would pass over in silence. The capacity 503s aren't caps:
        # the slot check is off at the release defaults (a standing env that
        # turns it on skips this phase), and the empty-cluster guard answers
        # for a cluster with no eligible node, which the admitted check below
        # already reds on.
        rejections = [
            o
            for o in outcomes
            if is_limiter_rejection(o, SERVER_LIMIT_MESSAGE)
            or is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)
            or is_limiter_rejection(o, LIVE_CAP_MESSAGE)
        ]
        histogram = status_histogram(outcomes)
        self._fixture.record("default_config_statuses", histogram)
        # Before the rejection check: at the defaults, "nothing was refused by
        # a limiter" is also true of a flood where every create failed for some
        # other reason, and by this point the earlier phases have left the
        # cluster holding a good many sandboxes. An assertion of absence needs a
        # possible presence.
        # A 201 with a sandbox id, as `expect_fail_open` requires: a 201 whose
        # body named no sandbox shows nothing was admitted.
        self._fixture.verify(
            any(o.status == 201 and o.sandbox_id for o in outcomes),
            f"not one of {len(outcomes)} creates was admitted at the release defaults, so "
            "this phase recorded no limiter rejection by admitting nothing at all",
            statuses=histogram,
        )
        # A request with no answer can't show whether a limiter refused it.
        unanswered = sum(1 for o in outcomes if o.status == 0)
        self._fixture.verify(
            not unanswered,
            f"{unanswered} of {len(outcomes)} creates got no answer at the release defaults, "
            "so the phase can't tell whether a limiter refused them",
            statuses=histogram,
        )
        self._fixture.verify(
            not rejections,
            f"{len(rejections)} of {len(outcomes)} requests were refused by a limiter at the "
            "release defaults; the default configuration must admit what it admitted before "
            "the caps existed",
            statuses=histogram,
            limiter_rejections=[o.message[:120] for o in rejections],
        )

    def admitted_ids(self, outcomes: list[LimiterProbeOutcome]) -> list[str]:
        return [o.sandbox_id for o in outcomes if o.sandbox_id]

    @staticmethod
    def now() -> float:
        return time.monotonic()
