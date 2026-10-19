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
        """Shrink both limiters' caps and put per-client concurrency in enforce mode.

        The standing values are captured first and restored at scope end (or
        eagerly by `restore_limiters`), so a chart or operator value comes
        back rather than being deleted. The Lease serializes cooperating
        smokes on the same Deployment and records the standing values for a
        successor if this run dies.
        """

        if not (1 <= per_client_cap <= 8 and 1 <= cluster_burst <= 8):
            raise ValueError("refusing limiter caps outside 1..8: each admitted create boots a VM")
        self._client_id = client_id
        self._template = template
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
                "SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS": client_id,
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
