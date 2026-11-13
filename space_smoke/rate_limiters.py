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

import os
import time
import uuid
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, BrokenBarrierError
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
# The slot check's refusal is a 503, and the same build raises a second 503
# under the same prefix from its empty-cluster guard -- a different function
# in the same module, reached from the same admission call. Only the tail
# tells the two apart, so the tail is what the phase matches on: a bench whose
# nodes had gone ineligible would otherwise satisfy a prefix-only assertion
# while proving nothing about slots.
CAPACITY_LIMIT_MESSAGE = "Server capacity limit:"
NO_FREE_SLOT_MESSAGE = "no eligible sandbox node has a free sandbox slot"
# Which of the stacked capabilities this bench's API is asserted to carry, as
# a comma-separated list (`live_cap`, `slot_check`).
#
# An operator assertion, not a build probe, and deliberately so: neither
# capability ships a migration, a chart key, a route or an eagerly-loaded
# script, so nothing the harness can read distinguishes a build that lacks the
# code from one whose code is broken. What makes the assertion safe is its
# asymmetry. Claim a capability the build lacks and the phase runs and *fails*
# -- the refusal never comes. Leave it unclaimed and the phase records itself
# absent, loudly, and the run cannot read as coverage. Neither mistake passes
# quietly, which is the only property a declaration of this kind has to have.
CAPABILITIES_ENV = "SPACE_SMOKE_RATE_LIMITER_CAPABILITIES"

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
    # The three stacked per-client capabilities' switches. Listed here so the
    # same capture-and-restore and the same Lease record cover them: a build
    # that does not know a key ignores it, so writing one costs nothing on a
    # cluster without the capability, while leaving it off the list would mean
    # a second uncaptured write with no orphan repair behind it.
    "SPACE_ACTIVATION_CLIENT_ATTRIBUTION_ENABLED",
    "SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT",
    "SPACE_API_CLIENT_LIVE_CAP_UNKNOWN",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED",
)
# The isolation round needs two more allowlisted ids. Only an id in
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
# short of a collision between two concurrent runs' 32-bit suffixes.
ISOLATION_NOISY_ID_PREFIX = "smoke-noisy"
ISOLATION_CONTROL_ID_PREFIX = "smoke-ctl"
REDIS_URL_ENV = "SPACE_REDIS_URL"


def declared_capabilities() -> frozenset[str]:
    """The capabilities this bench run asserts its API carries.

    Shared with the no-free-slot scenario, which gates the same way for the
    same reason: see `CAPABILITIES_ENV`.
    """

    return frozenset(
        token.strip() for token in os.environ.get(CAPABILITIES_ENV, "").split(",") if token.strip()
    )


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
# message because the capacity refusal is a 503, not a 429.
LIMITER_MESSAGES: tuple[tuple[int, str], ...] = (
    (429, SERVER_LIMIT_MESSAGE),
    (429, PER_CLIENT_LIMIT_MESSAGE),
    (429, LIVE_CAP_MESSAGE),
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


class RateLimiterAssertions:
    """Drive the kind public API's limiters and judge their answers.

    Caps are low on purpose: every admitted create boots a real Firecracker
    sandbox. With the defaults below a full run admits about ten, all of
    which the teardown kills by run metadata even when a phase fails
    mid-burst.
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
        self._admitted: list[str] = []
        run_token = uuid.uuid4().hex
        self._metadata = {
            "space_smoke_scenario": "local.rate_limiters_enforce_and_fail_open",
            "space_smoke_run": run_token,
            "space_smoke_role": "rate-limiter-probe",
        }
        # Kept short on purpose: all three ids travel as one comma-separated
        # `KNOWN_CLIENTS` value through a guard that caps it at 64 characters.
        self._noisy_id = f"{ISOLATION_NOISY_ID_PREFIX}-{run_token[:8]}"
        self._control_id = f"{ISOLATION_CONTROL_ID_PREFIX}-{run_token[:8]}"
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
        # All three ids go out as one comma-separated value, and the env guard
        # refuses anything longer. The ceiling is imported rather than
        # restated: a bound that drifted from the regex would let this pass a
        # value `set_public_api_env` then rejects *after* the Lease, stranding
        # exactly what this check exists to protect. The length is checked here
        # rather than left to that guard because this runs before the mutation
        # Lease is taken, and the same value rejected a few lines later would
        # strand an acquired Lease and a deferred restore on a limit nothing
        # had mentioned. Length only: the env guard's character class is still
        # the one that rejects a malformed `client_id`, and it does so after
        # the Lease.
        allowlist = ",".join((client_id, self._noisy_id, self._control_id))
        if len(allowlist) > LIMITER_ENV_VALUE_MAX:
            raise ValueError(
                f"refusing a client id of {len(client_id)} characters: with the isolation "
                f"round's two ids the allowlist reaches {len(allowlist)}, over the "
                f"{LIMITER_ENV_VALUE_MAX}-character env value limit"
            )
        self._client_id = client_id
        self._template = template
        self._per_client_cap = per_client_cap
        release = self._control.acquire_public_api_mutation()
        self._release_lease = self._scope.defer("release the public API mutation lease", release)
        store = self._control.public_api_env((REDIS_URL_ENV,))[REDIS_URL_ENV]
        self._fixture.verify(
            bool(store),
            f"the public API has no {REDIS_URL_ENV}; both limiters would fail open, not enforce",
        )
        self._fixture.record("limiter_store", store)
        standing = self._control.public_api_env(LIMITER_ENV_KEYS)
        self._fixture.record("limiter_env_before", dict(standing))

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
        # phase failing without it.
        overrides = {
            "SPACE_ACTIVATION_CLIENT_ATTRIBUTION_ENABLED": str(client_attribution).lower()
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

    def capability_declared(self, name: str) -> bool:
        """Whether this bench run asserts its API carries `name`.

        Read from `CAPABILITIES_ENV`; see its note for why these two
        capabilities are declared rather than probed. Recorded either way, so
        a run's evidence names what it claimed as well as what it found.
        """

        declared = declared_capabilities()
        self._fixture.record(f"capability_{name}_declared", name in declared)
        return name in declared

    def attribution_available(self) -> bool:
        """Whether this bench's API records the activating client.

        **The column is not the probe.** It looked like the one sound
        fingerprint among the three stacked capabilities -- it ships as a
        migration, so a build either has it or does not -- and that reasoning
        is wrong in this repository specifically, because migrations ship
        *ahead* of the code that reads them, in their own change. The pinned
        tree this was first run against carried
        `20261030070237_sandbox_activating_client_id` and none of the code
        that writes either the label or the row. A probe that read the column
        alone would have called the capability present, run the phase, and
        failed it -- reporting the writer as broken on a build that simply
        does not have one yet.

        So this is a declaration like the live cap's, with the column kept as
        a cross-check rather than as the signal. Declaring the capability on a
        build whose schema cannot hold it is a contradiction the bench should
        hear about immediately, which is why that case is a verdict rather
        than a quiet skip.
        """

        if not self.capability_declared("client_attribution"):
            return False
        column = self._control.activation_attribution_available()
        self._fixture.record("client_attribution_column", column)
        self._fixture.verify(
            column,
            "this bench declares the activating-client capability, but the schema has no "
            "`sandboxes.activating_client_id` column for it to be recorded in, so the "
            "declaration and the database disagree",
        )
        return True

    def expect_client_attributed(self, sandbox_id: str, *, expected: str | None = None) -> None:
        """Every pod of this sandbox, and its row, names the activating client.

        `expected` defaults to the client id the probes create under, which is
        the client whose request drove the activation.

        The label and the row are written as the activation proceeds, not
        before the create answers, so both are polled to a bound rather than
        read once. The wait failing is a verdict, not a return: a poll that
        gave up quietly would leave the verdicts below judging an unlabelled
        pod as though the API had been given no chance to label it.

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
        labels: list[str] = []
        row = ""
        while True:
            labels = self._control.sandbox_pod_client_ids(sandbox_id)
            row = self._control.sandbox_activating_client(sandbox_id)
            if labels and all(labels) and row:
                break
            if time.monotonic() >= deadline:
                break
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record(
            "client_attribution",
            {"sandbox_id": sandbox_id, "pod_labels": list(labels), "row": row, "expected": want},
        )
        self._fixture.verify(
            bool(labels),
            f"the sandbox has no pod carrying {ACTIVATION_CLIENT_LABEL}, so there is nothing "
            "for the attribution verdicts to judge",
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
            row == want,
            f"the sandbox row records {row!r} as the activating client, not {want!r}",
            row=row,
        )

    def sweep_admitted(self) -> None:
        """Kill every sandbox a probe admitted, by run metadata.

        `kill_matching` treats zero matches as a create that never became
        visible and raises; here zero admits is a legitimate outcome (every
        probe rejected), so the sweep runs only once something was admitted.
        """

        if self._admitted:
            self._probe.kill_matching(self._metadata)

    def _note(self, outcome: LimiterProbeOutcome) -> LimiterProbeOutcome:
        if outcome.sandbox_id:
            self._admitted.append(outcome.sandbox_id)
        return outcome

    def restore_limiters(self) -> None:
        for slot in ("_restore_env", "_release_lease"):
            compensation = getattr(self, slot)
            if compensation is not None:
                setattr(self, slot, None)
                compensation.release()

    # --- the probes ---------------------------------------------------------

    def _create_body(self) -> dict[str, object]:
        return {"templateID": self._template, "timeout": 120, "metadata": dict(self._metadata)}

    def create_once(self, *, timeout: float = 90) -> LimiterProbeOutcome:
        return self._note(
            self._probe.race_ecw_request(
                "POST", "/sandboxes", self._create_body(), client_id=self._client_id, timeout=timeout
            )
        )

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
    ) -> list[LimiterProbeOutcome]:
        """`count` gated concurrent requests for one client id; every outcome returned."""

        barrier = Barrier(count)
        with ThreadPoolExecutor(max_workers=count) as pool:
            futures = [
                pool.submit(
                    self._probe.race_ecw_request,
                    method,
                    path,
                    body,
                    client_id=client_id or self._client_id,
                    timeout=timeout,
                    gate=barrier.wait,
                )
                for _ in range(count)
            ]
            outcomes = [self._note(future.result()) for future in futures]
        self._fixture.verify(
            len(outcomes) == count,
            "not every concurrent caller reported an outcome",
            reported=len(outcomes),
            callers=count,
        )
        return outcomes

    def burst_creates(self, count: int = 20) -> list[LimiterProbeOutcome]:
        return self.burst(count, "POST", "/sandboxes", self._create_body())

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
        barrier = Barrier(total, timeout=timeout)

        def released(client: str, path: str, body: Mapping[str, object]) -> LimiterProbeOutcome:
            try:
                return self._probe.race_ecw_request(
                    "POST", path, body, client_id=client, timeout=timeout, gate=barrier.wait
                )
            except BaseException:
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
                        # legitimately rejected. (`burst_connects` still routes
                        # through `_note`; harmless there only because the
                        # creates that precede it already populate the list.)
                        answered.append(future.result())
                    elif isinstance(error, BrokenBarrierError):
                        collateral = collateral or error
                    else:
                        cause = cause or error
                arms[arm] = answered
        if cause is not None or collateral is not None:
            # Record what did come back before raising, on either path: a round
            # that died mid-flight is exactly the one whose partial histograms
            # are worth reading, and the verdicts never get to run. A barrier
            # timeout especially — every worker that reached the gate still
            # produced a real outcome. Keyed off `label` like the verdicts are,
            # so a round's evidence lands under one family of names whichever
            # way it ended.
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
        spend the very tokens the wake flood then needs; run at the standing
        caps it spends none of them.

        Each sandbox is confirmed `paused` on the ECW surface before the next
        is staged, because the whole point of the fleet is that a request
        against it takes the restore path. ECW pause is SPACE suspend
        (`e2b_native_pause_action` answers `SandboxAction::Suspend`), and a
        sandbox still `suspending` would answer a wake as a running one does:
        `activation_class` returns `None` for every lifecycle but `Suspended`,
        `Created` and `Stopped`, so the phase would quietly degrade into the
        reconnect round that already runs below, and pass.

        Polling the ECW state is enough to rule that out, and only because of
        how narrowly it renders: `ecw_sandbox_state` answers `paused` for
        `paused` and `suspended` alone and `running` for everything else,
        `suspending` included. A surface that collapsed the transient into
        `paused` would make this wait prove nothing.

        The fleet is deliberately small: each member is a real VM boot, and
        the flood only needs enough distinct suspended targets that the wakes
        the per-client gate admits are each doing real restore work rather
        than piling onto one in-flight restore.
        """

        if not 1 <= count <= 4:
            raise ValueError(
                f"refusing a suspended fleet of {count}: each member is a real VM boot, and "
                "one is already enough for the flood to take the restore path"
            )
        staged: list[str] = []
        for _ in range(count):
            sandbox = self._fixture.sandboxes.create(metadata=self._wake_metadata)
            sandbox.pause()
            self._await_suspended(sandbox)
            # The handle, not just the id: the live-cap phase re-suspends this
            # same fleet, and `pause` is on the handle. Re-staging from ids
            # would mean new sandboxes and new boots for a phase that needs
            # neither.
            self._wake_fleet.append(sandbox)
            staged.append(sandbox.sandbox_id)
        self._wake_targets = staged
        self._fixture.record("wake_fleet", list(staged))
        return staged

    def _await_suspended(self, sandbox: EcwSandbox) -> None:
        """Block until the public API reports this sandbox `paused`.

        The fixture's own `wait_until_paused` only reads the scenario's
        sandbox, so the poll is written out here for a collection member. Its
        timeout and interval are the harness's own lifecycle values rather
        than new constants, so a bench whose transitions are slow is already
        accounted for.
        """

        deadline = time.monotonic() + self._fixture.lifecycle_transition_timeout
        while True:
            info = sandbox.try_get_info()
            if info is not None and info.state is EcwSandboxState.PAUSED:
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "a staged wake target never reported itself suspended, so the flood would "
                "have been a reconnect storm against a running sandbox and would have "
                "passed without exercising the restore path at all",
                sandbox_id=sandbox.sandbox_id,
                observed_state=(info.state if info is not None else "deleted"),
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
        arms = self._release_together(
            [
                ("noisy", self._noisy_id, noisy_specs),
                ("control", self._control_id, control_specs),
            ],
            timeout=timeout,
            label=label,
            what="wake flood",
        )
        self._expect_a_target_woke(suspended, label=label)
        return arms["noisy"], arms["control"]

    def _expect_a_target_woke(self, suspended: Sequence[str], *, label: str) -> None:
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
            woke = [sid for sid, state in states.items() if state is not EcwSandboxState.PAUSED]
            if woke or time.monotonic() >= deadline:
                break
            self._fixture.wait(self._fixture.lifecycle_poll_interval)
        self._fixture.record(f"{label}_target_states", {k: str(v) for k, v in states.items()})
        self._fixture.verify(
            bool(woke),
            f"{label}: every flooded target is still paused, so the {len(suspended)} requests "
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
        arms = self._release_together(
            [
                ("noisy", self._noisy_id, [(path, body)] * noisy),
                ("control", self._control_id, [(path, body)] * control),
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
            sandbox.pause()
            self._await_suspended(sandbox)
        # And wait for the capped client to hold nothing. A pod still
        # terminating counts towards the live cap exactly as a running one
        # does -- the cap counts from a reflector of the same label selector
        # this read uses -- so a phase that started here would have its first
        # wake refused and would red on `the cap admitted`, reporting a
        # working cap as a broken one.
        deadline = time.monotonic() + self._fixture.lifecycle_transition_timeout
        while True:
            held = self._control.live_client_sandbox_pods(self._noisy_id)
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
        return outcomes

    def expect_live_cap_plateau(
        self,
        outcomes: list[LimiterProbeOutcome],
        control: list[LimiterProbeOutcome],
        *,
        label: str = "live_cap",
    ) -> None:
        """A client's live sandboxes stop at its cap; another client is untouched.

        Verdicts, most-specific first:

        `the cap admitted` at least one wake got through. Without it a cap of
        zero, or a build refusing everything, would satisfy every verdict
        below -- a cap that admits nothing is not a cap.

        `the cap refused` at least one wake took the live-cap 429. Named
        apart from the count verdict so a build that never refuses reads as
        the cap not firing, rather than as the cap being the wrong size.

        `every refusal is retryable` each live-cap 429 carries
        `Retry-After`. The cap lifts when the client frees a sandbox, so a
        refusal without a hint leaves a caller nothing to pace against.

        `admissions stop at the cap` no more wakes were admitted than the cap
        allows. This is the plateau, read from the answers.

        `the pods stop at the cap too` the client's labelled pods are at most
        the cap, read from the substrate the cap itself counts. The answers
        and the pods are checked separately because an admission that never
        produced a pod would satisfy the count verdict while leaving the cap
        guarding nothing.

        `the other client is untouched` every control reconnect succeeded. A
        reconnect to an already-running sandbox is not an activation at all
        (`activation_class` answers `None` for a started lifecycle), so the
        cap must never be what answers it.
        """

        cap = self._live_cap
        admitted = [o for o in outcomes if 200 <= o.status < 300]
        refused = [o for o in outcomes if is_limiter_rejection(o, LIVE_CAP_MESSAGE)]
        live_pods = self._control.live_client_sandbox_pods(self._noisy_id)
        self._fixture.record(f"{label}_statuses", status_histogram(outcomes))
        self._fixture.record(f"{label}_control_statuses", status_histogram(control))
        self._fixture.record(f"{label}_live_pods", len(live_pods))
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
        self._fixture.verify(
            len(live_pods) <= cap,
            f"{label}: the client holds {len(live_pods)} labelled sandbox pods against a cap "
            f"of {cap}, so the answers plateaued but the cluster did not",
            live_pods=live_pods,
        )
        self._fixture.verify(
            all(200 <= o.status < 300 for o in control),
            f"{label}: a reconnect by '{self._control_id}' to an already-running sandbox was "
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

        Both arms' status histograms are recorded before any verdict is
        checked, so a red round's evidence survives the raise. The verdicts
        themselves short-circuit — `verify` raises on the first false one --
        so they run in the order that makes that first failure the most
        specific true statement about what broke:

        `noisy saturated` the storm really drew per-client rejections. Without
        it the control arm ran beside an idle limiter and proves nothing.

        `control answered` every control request came back with a status. A
        transport failure is neither an admission nor a rejection, and
        counting it as "not rejected" would pass the round for each request
        the bench dropped.

        `control isolated` no control request was rejected by the per-client
        limiter. Named separately from the success verdict so a 429 reads as
        broken isolation rather than an unusable target.

        `noisy admitted` the storm also got through at least once. Saturation
        alone leaves a way for a shared bucket to read as two: if the control
        requests win every slot the bucket has, the noisy arm takes nothing but
        rejections and the control arm succeeds, which is the shared-bucket
        reading of exactly the evidence separate buckets produce. Requiring one
        noisy admission makes that reading much less likely without ruling it
        out — see the note on what this round does and does not prove.

        It is checked *after* `control isolated` because a bucket both arms
        share rejects both arms, and this verdict would otherwise fail first
        and report the noisy arm as starved rather than the two clients as
        sharing.

        What the round does **not** establish is that the two ids are on
        separate buckets. The limiter admits whenever the bucket holds fewer
        than `cap` members *at that instant* and releases by `ZREM` when a
        guard drops (`client_concurrency.lua`, `redis_limiter.rs`), so slots
        churn while a wave is in flight and the admissions one bucket grants
        across a wave are not bounded by `cap`. A single shared bucket can
        therefore satisfy every verdict here, given arrival timings that let
        each control request find a free slot. This round is real-cluster
        evidence that isolation holds, and it fails loudly on the shapes a
        broken limiter most often produces; the timing-free proof that two
        allowlisted ids key different buckets is the unit test
        `limiter_at_cap_does_not_spend_another_allowlisted_clients_budget`,
        which holds one client's guard across the other's acquire so no
        ordering is involved — though that test skips itself when no Redis is
        reachable, so on such a runner it reports green having proven nothing.

        `control succeeded` every control reconnect returned 2xx. This is the
        verdict the others exist to make meaningful: a round where the control
        arm answers 404, 500, or a cluster-level 429 shows only that the
        per-client gate stayed quiet, not that a second client kept working
        while the first was throttled.
        """

        noisy_rejections = [o for o in noisy if is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)]
        noisy_admitted = [o for o in noisy if 200 <= o.status < 300]
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
            f"{label}: the {len(noisy)}-deep storm as '{self._noisy_id}' drew no 429 "
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
            f"{label}: '{self._control_id}' took {len(control_rejections)} 429 "
            f"'{PER_CLIENT_LIMIT_MESSAGE}' while '{self._noisy_id}' was over cap, so "
            "the two clients are not on separate buckets",
            control_statuses=status_histogram(control),
        )
        self._fixture.verify(
            bool(noisy_admitted),
            f"{label}: '{self._noisy_id}' was rejected on every one of its "
            f"{len(noisy)} requests, so nothing it did distinguishes one shared bucket from "
            "two. Check `noisy_cluster_rejections` before reading this as a per-client "
            "result: a noisy arm the *cluster* limiter turned away says nothing about "
            "per-client buckets either way",
            noisy_statuses=status_histogram(noisy),
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
            "creates did not all admit sandboxes with Redis unreachable; both limiters must fail open",
            statuses=histogram,
            sample_messages=sorted({o.message[:120] for o in outcomes})[:4],
        )

    def expect_admitted_by_default(self, outcomes: list[LimiterProbeOutcome]) -> None:
        """With no caps configured, the same flood draws no limiter rejection.

        The other phases prove the limiters reject once they are configured to.
        This proves the converse, which is what the enablement work actually
        risks: that landing the caps starts refusing traffic a cluster with no
        caps set would have admitted. It runs after `restore_limiters` has put
        the standing env back, so the configuration under test is whatever the
        chart renders rather than anything this scenario set.

        Only limiter rejections are judged, by message. A 429 from placement
        admission, or any other status, says nothing about the caps and is not
        this phase's business -- the same rule `expect_fail_open` follows.
        """

        rejections = [
            o
            for o in outcomes
            if is_limiter_rejection(o, SERVER_LIMIT_MESSAGE)
            or is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)
        ]
        histogram = status_histogram(outcomes)
        self._fixture.record("default_config_statuses", histogram)
        self._fixture.verify(
            not rejections,
            f"{len(rejections)} of {len(outcomes)} requests were refused by a limiter with no "
            "caps configured; the default configuration must admit what it admitted before "
            "the caps existed",
            statuses=histogram,
            limiter_rejections=[o.message[:120] for o in rejections],
        )

    def admitted_ids(self, outcomes: list[LimiterProbeOutcome]) -> list[str]:
        return [o.sandbox_id for o in outcomes if o.sandbox_id]

    @staticmethod
    def now() -> float:
        return time.monotonic()
