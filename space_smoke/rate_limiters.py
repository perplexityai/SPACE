"""The two Redis-backed API rate limiters, proven end to end on a real cluster.

`smoke-kind-rate-limiters.sh` pointed the kind `space-api` at Redis, enabled
both limiters with low caps in enforce mode, and drove real HTTP traffic at
them: sequential creates until the cluster activation token bucket answered
429 `Server rate limit:`, concurrent creates and connects for one
`X-Client-ID` until the per-client concurrency limiter answered 429
`Per-client rate limit:`, then Redis scaled to zero to show both limiters
admit when their store is unreachable. This module is that suite in typed
form: the Deployment env override is a named, captured-and-restored, kind-only
operation behind one cluster Lease; the Redis fault has its compensation
armed before dispatch; every sandbox the probes admit carries run metadata
the teardown kills by. The control surface is [`RateLimiterControl`]; the raw
requests go through [`RateLimitProbe`] so a 429's `Retry-After` survives.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import TYPE_CHECKING

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
# else. `enable` refuses anything outside it before kubectl runs.
LIMITER_ENV_KEYS: tuple[str, ...] = (
    "SPACE_REDIS_URL",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS",
    "SPACE_API_CLIENT_CONCURRENCY_MODE",
    "SPACE_API_CLIENT_CONCURRENCY_MAX_PER_CLIENT",
    "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS",
)
# The isolation round needs two more allowlisted ids. Only an id in
# `KNOWN_CLIENTS` gets its own bucket and the per-client cap; anything else
# shares one deliberately tighter `unspecified` bucket, so a control client
# left off the list would contend with the noisy storm through that shared
# bucket and the round would measure contention instead of isolation.
ISOLATION_NOISY_CLIENT_ID = "space-smoke-noisy"
ISOLATION_CONTROL_CLIENT_ID = "space-smoke-control"
KIND_REDIS_URL = "redis://space-redis:6379"


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
        self._metadata = {
            "space_smoke_scenario": "local.rate_limiters_enforce_and_fail_open",
            "space_smoke_run": uuid.uuid4().hex,
            "space_smoke_role": "rate-limiter-probe",
        }

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
        """Point the API at Redis and turn both limiters on in enforce mode.

        The standing values are captured first and restored at scope end (or
        eagerly by `restore_limiters`), so a chart or operator value comes
        back rather than being deleted. The Lease serializes cooperating
        smokes on the same Deployment.
        """

        if not (1 <= per_client_cap <= 8 and 1 <= cluster_burst <= 8):
            raise ValueError("refusing limiter caps outside 1..8: each admitted create boots a VM")
        self._client_id = client_id
        self._template = template
        self._per_client_cap = per_client_cap
        release = self._control.acquire_public_api_mutation()
        self._release_lease = self._scope.defer("release the public API mutation lease", release)
        standing = self._control.public_api_env(LIMITER_ENV_KEYS)
        self._fixture.record("limiter_env_before", dict(standing))

        def restore() -> None:
            self._control.restore_public_api_env(standing)
            self._control.wait_public_api_ready(timeout=rollout_timeout)

        self._restore_env = self._scope.defer(
            "restore the public API limiter env", restore, critical=True
        )
        self._control.set_public_api_env(
            {
                "SPACE_REDIS_URL": KIND_REDIS_URL,
                "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED": "true",
                "SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST": str(cluster_burst),
                "SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS": cluster_refill_rps,
                "SPACE_API_CLIENT_CONCURRENCY_MODE": "enforce",
                "SPACE_API_CLIENT_CONCURRENCY_MAX_PER_CLIENT": str(per_client_cap),
                "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS": ",".join(
                    (client_id, ISOLATION_NOISY_CLIENT_ID, ISOLATION_CONTROL_CLIENT_ID)
                ),
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

    def establish_connect_target(self, existing: str | None = None) -> str:
        """A sandbox the isolation round can actually reconnect to.

        Called before the activation bucket is drained, because that is the
        only window in which a create can still be admitted: the drain leaves
        the bucket empty on purpose, and a reconnect that has to wake a
        sandbox goes through the same admission. A target established here is
        already running, so the round's reconnects touch the per-client gate
        and nothing else.
        """

        if existing:
            return existing
        outcome = self.create_once()
        self._fixture.verify(
            outcome.status == 201 and bool(outcome.sandbox_id),
            "could not establish a reconnect target for the isolation round",
            status=outcome.status,
            response_message=outcome.message,
        )
        assert outcome.sandbox_id is not None
        return outcome.sandbox_id

    def isolation_round(
        self,
        sandbox_id: str,
        *,
        noisy: int = 20,
        control: int = 3,
        timeout: float = 90,
    ) -> tuple[list[LimiterProbeOutcome], list[LimiterProbeOutcome]]:
        """Reconnect as two separately-bucketed clients at the same instant.

        Both arms wait on one barrier, so the noisy fan-out is not merely
        launched first — every request is released together and the arms are
        genuinely concurrent. That matters because the round's verdict is
        about what one client's saturation does to another: if the control
        requests only went out after the noisy ones drained, a limiter that
        keyed every client to one bucket would still answer them cleanly and
        the round would pass on the defect it exists to catch.

        `control` stays inside the per-client cap. The control arm is asserted
        to succeed, which can only hold while its own fan-out fits in its own
        bucket; a wider arm would be rejected by its own load and read as an
        isolation failure.
        """

        if not 1 <= control <= self._per_client_cap:
            raise ValueError(
                f"refusing a control arm of {control} against a per-client cap of "
                f"{self._per_client_cap}: it is asserted to succeed, which only holds "
                "inside its own cap"
            )
        if noisy <= self._per_client_cap:
            raise ValueError(
                f"refusing a noisy arm of {noisy} against a per-client cap of "
                f"{self._per_client_cap}: it has to be able to exceed the cap"
            )
        body: Mapping[str, object] = {"timeout": 30}
        path = f"/sandboxes/{sandbox_id}/connect"
        barrier = Barrier(noisy + control)
        plan = [(ISOLATION_NOISY_CLIENT_ID, noisy), (ISOLATION_CONTROL_CLIENT_ID, control)]
        with ThreadPoolExecutor(max_workers=noisy + control) as pool:
            futures = {
                client: [
                    pool.submit(
                        self._probe.race_ecw_request,
                        "POST",
                        path,
                        body,
                        client_id=client,
                        timeout=timeout,
                        gate=barrier.wait,
                    )
                    for _ in range(count)
                ]
                for client, count in plan
            }
            arms = {
                client: [self._note(future.result()) for future in submitted]
                for client, submitted in futures.items()
            }
        return arms[ISOLATION_NOISY_CLIENT_ID], arms[ISOLATION_CONTROL_CLIENT_ID]

    def expect_control_isolated(
        self,
        noisy: list[LimiterProbeOutcome],
        control: list[LimiterProbeOutcome],
        *,
        label: str = "isolation",
    ) -> None:
        """One client at its cap must not spend another's reconnect budget.

        Four verdicts, each recorded before the first failure is raised, so a
        red round names the property that broke rather than the first symptom:

        `noisy saturated` the storm really drew per-client rejections. Without
        it the control arm ran beside an idle limiter and proves nothing.

        `control answered` every control request came back with a status. A
        transport failure is neither an admission nor a rejection, and
        counting it as "not rejected" would pass the round for each request
        the bench dropped.

        `control isolated` no control request was rejected by the per-client
        limiter. Named separately from the success verdict so a 429 reads as
        broken isolation rather than an unusable target.

        `control succeeded` every control reconnect returned 2xx. This is the
        verdict the others exist to make meaningful: a round where the control
        arm answers 404, 500, or a cluster-level 429 shows only that the
        per-client gate stayed quiet, not that a second client kept working
        while the first was throttled.
        """

        noisy_rejections = [o for o in noisy if is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)]
        unanswered = [o for o in control if o.status == 0]
        control_rejections = [
            o for o in control if is_limiter_rejection(o, PER_CLIENT_LIMIT_MESSAGE)
        ]
        succeeded = [o for o in control if 200 <= o.status < 300]
        self._fixture.record(f"{label}_noisy_statuses", status_histogram(noisy))
        self._fixture.record(f"{label}_control_statuses", status_histogram(control))
        self._fixture.verify(
            bool(noisy_rejections),
            f"{label}: the {len(noisy)}-deep storm as '{ISOLATION_NOISY_CLIENT_ID}' drew no 429 "
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
            f"{label}: '{ISOLATION_CONTROL_CLIENT_ID}' took {len(control_rejections)} 429 "
            f"'{PER_CLIENT_LIMIT_MESSAGE}' while '{ISOLATION_NOISY_CLIENT_ID}' was over cap, so "
            "the two clients are not on separate buckets",
            control_statuses=status_histogram(control),
        )
        self._fixture.verify(
            len(succeeded) == len(control),
            f"{label}: only {len(succeeded)} of {len(control)} control reconnects succeeded; a "
            "control client has to keep reconnecting successfully while the noisy one is throttled",
            control_statuses=status_histogram(control),
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
        deletion, empty endpoints and a settle for the stale connection.
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
