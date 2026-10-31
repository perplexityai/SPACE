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

import time
import uuid
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, BrokenBarrierError
from typing import TYPE_CHECKING

from space_smoke.adapters import LIMITER_ENV_VALUE_MAX
from space_smoke.capabilities import (
    LimiterProbeOutcome,
    RateLimitProbe,
    RateLimiterControl,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SERVER_LIMIT_MESSAGE = "Server rate limit:"
PER_CLIENT_LIMIT_MESSAGE = "Per-client rate limit:"

# The env this scenario may write on the public API Deployment, and nothing
# else. `enable` refuses anything outside it before kubectl runs. The store's
# URL is not among them: the chart renders it, and the fail-open half takes
# the store itself away instead.
LIMITER_ENV_KEYS: tuple[str, ...] = (
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS",
    "SPACE_API_CLIENT_CONCURRENCY_MODE",
    "SPACE_API_CLIENT_CONCURRENCY_MAX_PER_CLIENT",
    "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS",
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
# gets the admission of its own that `noisy admitted` requires. `redis_limiter.rs` documents the same hazard for the keys its own
# tests do not namespace. A per-run id carries no predecessor's members,
# short of a collision between two concurrent runs' 32-bit suffixes.
ISOLATION_NOISY_ID_PREFIX = "smoke-noisy"
ISOLATION_CONTROL_ID_PREFIX = "smoke-ctl"
REDIS_URL_ENV = "SPACE_REDIS_URL"


def is_limiter_rejection(outcome: LimiterProbeOutcome, message: str) -> bool:
    """A 429 whose body starts with the named limiter's message."""

    return outcome.status == 429 and outcome.message.startswith(message)


def has_retry_after_seconds(outcome: LimiterProbeOutcome) -> bool:
    """`Retry-After` present as RFC 9110 delta-seconds (digits only)."""

    return outcome.retry_after is not None and outcome.retry_after.strip().isdigit()


def status_histogram(outcomes: list[LimiterProbeOutcome]) -> dict[str, int]:
    histogram: dict[str, int] = {}
    for outcome in outcomes:
        key = str(outcome.status)
        if outcome.status == 429:
            for message in (SERVER_LIMIT_MESSAGE, PER_CLIENT_LIMIT_MESSAGE):
                if outcome.message.startswith(message):
                    key = f"429 {message.rstrip(':')}"
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

    # --- environment shape ------------------------------------------------

    def enable_limiters(
        self,
        *,
        template: str,
        client_id: str = "space-smoke",
        per_client_cap: int = 3,
        cluster_burst: int = 2,
        cluster_refill_rps: str = "0.1",
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
        self._control.set_public_api_env(
            {
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
                "per_client": per_client_cap,
                "cluster_burst": cluster_burst,
                "cluster_refill_rps": cluster_refill_rps,
                "client_id": client_id,
            },
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

        `control` stays inside the per-client cap. The arm is asserted to
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
        # The workers do their own setup before reaching the gate
        # (`race_ecw_request` builds config, headers and body first), so one of
        # them raising there would leave the rest waiting on a party that never
        # arrives, with the pool sized exactly to the barrier. `released`
        # aborts the barrier on the way out of such a raise, so the rest fail
        # at once instead of waiting out the timeout. The timeout stays as the
        # backstop for a worker that neither arrives nor raises. It is the
        # request timeout too, so a round where every worker reaches the gate
        # and then waits out its request can take up to twice `timeout`.
        barrier = Barrier(noisy + control, timeout=timeout)

        def released(client: str) -> LimiterProbeOutcome:
            try:
                return self._probe.race_ecw_request(
                    "POST", path, body, client_id=client, timeout=timeout, gate=barrier.wait
                )
            except BaseException:
                barrier.abort()
                raise

        plan = [(self._noisy_id, noisy), (self._control_id, control)]
        with ThreadPoolExecutor(max_workers=noisy + control) as pool:
            futures = {
                client: [pool.submit(released, client) for _ in range(count)]
                for client, count in plan
            }
            # A worker that raises before it reaches the gate breaks the
            # barrier for the other 22, so most of what comes back is
            # `BrokenBarrierError` — a consequence, not the cause. Collect
            # every outcome first and raise the cause if there was one, rather
            # than letting the first future's bystander error be what the
            # round reports.
            arms: dict[str, list[LimiterProbeOutcome]] = {}
            collateral: BrokenBarrierError | None = None
            cause: BaseException | None = None
            for client, submitted in futures.items():
                answered: list[LimiterProbeOutcome] = []
                for future in submitted:
                    error = future.exception()
                    if error is None:
                        # Deliberately not `_note` here: that list is what
                        # the teardown kills by, and a reconnect creates
                        # nothing. (`burst_connects` still routes through
                        # `_note`; harmless there only because the creates
                        # that precede it already populate the list.)
                        # A successful `/connect` echoes `sandboxID`, so
                        # recording it would put the *fixture's* sandbox in the
                        # kill list — and then `sweep_admitted` would call
                        # `kill_matching` on run metadata that sandbox does not
                        # carry, failing a teardown on a run whose every create
                        # was legitimately rejected.
                        answered.append(future.result())
                    elif isinstance(error, BrokenBarrierError):
                        collateral = collateral or error
                    else:
                        cause = cause or error
                arms[client] = answered
        if cause is not None or collateral is not None:
            # Record what did come back before raising, on either path: a round
            # that died mid-flight is exactly the one whose partial histograms
            # are worth reading, and `expect_control_isolated` never gets to
            # run. A barrier timeout especially -- every worker that reached
            # the gate still produced a real outcome. Keyed off `label` like
            # the verdicts are, so a round's evidence lands under one family of
            # names whichever way it ended.
            for arm, client in (("noisy", self._noisy_id), ("control", self._control_id)):
                self._fixture.record(
                    f"{label}_{arm}_statuses", status_histogram(arms.get(client, []))
                )
        if cause is not None:
            raise cause
        if collateral is not None:
            # No worker raised, so the barrier timed out: some request never
            # reached the gate within the round's own timeout. Bare, that is a
            # `BrokenBarrierError` from the threading module with nothing
            # naming the round.
            raise AssertionError(
                f"the isolation round's {noisy + control} requests did not all reach the "
                f"barrier within {timeout:.3g}s, so they were never released together"
            ) from collateral
        return arms[self._noisy_id], arms[self._control_id]

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
            f"{len(noisy)} requests, so its own cap admitted nothing and the control arm's "
            "success is as consistent with one shared bucket as with two",
            noisy_statuses=status_histogram(noisy),
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
        """With the store down neither limiter may reject; other statuses are not judged."""

        limiter_429s = [
            o
            for o in outcomes
            if is_limiter_rejection(o, SERVER_LIMIT_MESSAGE)
            or is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)
        ]
        histogram = status_histogram(outcomes)
        self._fixture.record("fail_open_statuses", histogram)
        self._fixture.verify(
            not limiter_429s,
            "a limiter rejected with Redis unreachable; both must fail open",
            statuses=histogram,
            limiter_rejections=[o.message[:120] for o in limiter_429s],
        )

    def admitted_ids(self, outcomes: list[LimiterProbeOutcome]) -> list[str]:
        return [o.sandbox_id for o in outcomes if o.sandbox_id]

    @staticmethod
    def now() -> float:
        return time.monotonic()
