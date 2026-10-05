"""The two Redis-backed API rate limiters, proven end to end on a real cluster.

The scenario points the kind `space-api` at Redis, enables both limiters with
low caps in enforce mode, and drives real HTTP traffic at them: sequential
creates until the cluster activation token bucket answers 429 `Server rate
limit:`, concurrent creates and connects for one `X-Client-ID` until the
per-client concurrency limiter answers 429 `Per-client rate limit:`, then Redis
scaled to zero to show both limiters admit when their store is unreachable.
The Redis store is opt-in in the bring-up, so the scenario creates it when the
bench has none. The Deployment env override is a named, captured-and-restored,
kind-only operation behind one cluster Lease; the Redis fault has its
compensation armed before dispatch; every sandbox the probes admit carries run
metadata the teardown kills by. The control surface is [`RateLimiterControl`]; the raw
requests go through [`RateLimitProbe`] so a 429's `Retry-After` survives.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    HalfPresentRedisStore,
    LimiterProbeOutcome,
    LimiterRunRecord,
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
        self._remove_store: Compensation | None = None
        self._remove_store_callback: Callable[[], None] | None = None
        self._store_removal_attempted = False
        self._store_removed = False
        self._env_dirty = False
        self._clear_record: Compensation | None = None
        self._owns_store = False
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
        redis_rollout_timeout: float = 600,
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
        release = self._control.acquire_public_api_mutation(purpose="rate-limiter")
        self._release_lease = self._scope.defer("release the public API mutation lease", release)
        # What this run changes goes into a record on the public API Deployment
        # before the first change, and is cleared after the last undo. A run
        # killed in between leaves it, and it is the next run's way back: its
        # `env` becomes the baseline (the live env would be the killed run's
        # overrides), and a store it created is removed rather than taken for
        # the bench's.
        live = self._control.public_api_env(LIMITER_ENV_KEYS)
        left = self._control.limiter_run_record()
        # A key added to LIMITER_ENV_KEYS since the killed run has no recorded
        # value, so the live one stands for it.
        recorded = {} if left is None else left.env
        standing = {key: recorded.get(key, live[key]) for key in LIMITER_ENV_KEYS}
        self._fixture.record("limiter_env_before", dict(standing))
        if left is not None:
            self._env_dirty = True
            self._fixture.record(
                "left_by_killed_run", {"env": dict(live), "redis_created": left.redis_created}
            )
        self._owns_store = left is not None and left.redis_created
        if left is None or dict(left.env) != standing:
            # Also over a killed run's record when the merge above changed its
            # env (a key added since), so that a second kill can't lose it.
            self._control.write_limiter_run_record(
                LimiterRunRecord(standing, redis_created=self._owns_store)
            )
        self._clear_record = self._scope.defer(
            "clear the limiter run record", self._clear_run_record, critical=True
        )

        def restore() -> None:
            if self._env_dirty:
                self._control.restore_public_api_env(standing)
                self._control.wait_public_api_ready(timeout=rollout_timeout)
                self._env_dirty = False

        self._restore_env = self._scope.defer(
            "restore the public API limiter env", restore, critical=True
        )
        # The bring-up leaves the store out unless asked (`--with-redis`), so
        # provide it before SPACE_REDIS_URL names it. The removal is attempted
        # before the lease is released, so another run can't find the store
        # and then lose it; a removal that fails leaves the record, and the
        # next run removes the store.
        try:
            present = self._control.redis_store_present()
        except HalfPresentRedisStore:
            if not self._owns_store:
                raise
            # The half is this scenario's: a killed run's create or removal
            # stopped between the two objects. Finish removing it.
            self._control.remove_redis_store()
            present = False
        # Adopt only what a killed run's record says it created. A store this
        # run is about to create is claimed below but never adopted: one that
        # appears between the probe above and the create is someone else's.
        adopt = self._owns_store
        if not present and not self._owns_store:
            # Claimed before the create, so a run killed during it leaves a
            # record the next run acts on; an adopt that then finds no store
            # just creates one.
            self._owns_store = True
            self._control.write_limiter_run_record(LimiterRunRecord(standing, redis_created=True))
        try:
            remove_store = self._control.provide_redis(adopt=adopt)
        except Exception as error:
            if not present:
                self._take_back_claim_if_foreign(standing, error)
            raise
        if remove_store is None and self._owns_store and not adopt:
            # The store appeared in that window: it is not this run's.
            self._owns_store = False
            self._control.write_limiter_run_record(LimiterRunRecord(standing, redis_created=False))
        self._fixture.record("redis_owned_by_run", remove_store is not None)
        if remove_store is not None:

            def remove() -> None:
                # Set before the delete: a delete that fails partway may already
                # have taken the Deployment, and scaling it back would recreate
                # nothing but still restore a half the run meant to delete.
                self._store_removal_attempted = True
                remove_store()
                self._store_removed = True

            self._remove_store_callback = remove
            self._arm_store_removal_first()
        if self._control.redis_replicas() == 0:
            # A run killed mid fail-open phase leaves the store at zero, whether
            # it was the bench's or the one this run just adopted.
            self._control.scale_redis(1)
        self._control.wait_redis_rollout(timeout=redis_rollout_timeout)
        self._env_dirty = True
        self._control.set_public_api_env(
            {
                "SPACE_REDIS_URL": KIND_REDIS_URL,
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
        self._arm_store_removal_first()
        self._fixture.record(
            "limiter_caps",
            {
                "per_client": per_client_cap,
                "cluster_burst": cluster_burst,
                "cluster_refill_rps": cluster_refill_rps,
                "client_id": client_id,
            },
        )

    def _arm_store_removal_first(self) -> None:
        """Move a run-created store's removal to the front of scope cleanup.

        Called after each later compensation is armed. Cleanup gets
        min(120 s, 10 %) of the scenario budget, and the store goes first so
        the sweep and the env rollout can't run it out: a store the budget
        missed is not lost (the run record keeps it, and the next run removes
        it), but it outlives this run. It also makes the Redis scale-back moot.
        The API only fails open on the store meanwhile, and the Lease is still
        held.
        """

        if self._remove_store_callback is None or self._store_removal_attempted:
            return
        # Arm the new registration before disarming the old one, so no instant
        # passes with the removal unarmed.
        previous = self._remove_store
        self._remove_store = self._scope.defer(
            "remove the Redis store this run created", self._remove_store_callback, critical=True
        )
        if previous is not None:
            previous.disarm()

    def _take_back_claim_if_foreign(self, standing: dict[str, str | None], error: Exception) -> None:
        """After a failed create, give up the claim only for a store that raced in.

        A complete store now present is most likely someone else's (the create
        refused it), and keeping the claim would have the next run delete it.
        It can also be this run's own, when the create succeeded server-side
        and the client failed; giving up the claim then only leaves a store
        that the next default bring-up removes. Nothing or a half keeps the
        claim, for the next run to heal.
        """

        try:
            raced = self._control.redis_store_present()
        except Exception:  # noqa: BLE001 - a half or an unreadable store keeps the claim
            return
        if not raced:
            return
        self._owns_store = False
        try:
            self._control.write_limiter_run_record(LimiterRunRecord(standing, redis_created=False))
        except Exception as rollback:  # noqa: BLE001 - reported on the original error
            error.add_note(f"taking back the Redis store claim failed too: {rollback}")

    def _clear_run_record(self) -> None:
        """Clear the run record, but only once everything it describes is undone.

        Otherwise it stays for the next run: an env that was not restored or a
        store that was not removed is exactly what the record lets it undo.
        """

        if self._env_dirty or (self._owns_store and not self._store_removed):
            return
        self._control.clear_limiter_run_record()

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
        for slot in ("_restore_env", "_remove_store", "_clear_record", "_release_lease"):
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
            if self._store_removal_attempted or self._owns_store:
                # This run's own store is removed at teardown; scaling it back
                # would only wait out a rollout for a store about to go.
                return
            self._control.scale_redis(max(previous, 1))
            self._control.wait_redis_rollout(timeout=timeout)

        self._redis_back = self._scope.defer("scale Redis back up", back, critical=True)
        self._arm_store_removal_first()
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
