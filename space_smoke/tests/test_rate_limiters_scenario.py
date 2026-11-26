"""Contracts of the rate-limiter scenario the bench cannot exercise.

A passing bench run shows both limiters rejecting and then admitting. It
cannot show that a 429 from *another* source (placement admission, a
malformed Retry-After) is refused as limiter evidence, that the env override
refuses keys outside its closed list or a `valueFrom` entry it could not
restore, or that a Retry-After without delta-seconds fails the verdict.
Those are pinned here.
"""

from __future__ import annotations

import threading
import unittest
from collections.abc import Sequence
from threading import BrokenBarrierError
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from space_smoke.adapters import (
    LIMITER_ENV_VALUE_MAX,
    KindConfig,
    KindHarness,
)
from space_smoke.capabilities import (
    EcwSandboxDeleteRetryable,
    EcwSandboxReconcileUnmatched,
    EcwSandboxState,
    LimiterProbeOutcome,
)
from space_smoke.rate_limiters import (
    CLEANUP_ROLLOUT_TIMEOUT_SECONDS,
    LIMITER_ENV_KEYS,
    LIVE_CAP_MESSAGE,
    PER_CLIENT_LIMIT_MESSAGE,
    SERVER_LIMIT_MESSAGE,
    RateLimiterAssertions,
    has_retry_after_seconds,
    is_limiter_rejection,
    limiter_tightenings,
    status_histogram,
)
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    ScenarioTimedOut,
    SmokeConfigurationError,
)


def _outcome(status: int, message: str = "", retry_after: str | None = None) -> LimiterProbeOutcome:
    return LimiterProbeOutcome(status, retry_after, message, None, 0.0)


def _created(sandbox_id: str = "sbx-1") -> LimiterProbeOutcome:
    return LimiterProbeOutcome(201, None, "", sandbox_id, 0.0)


class LimiterRejectionClassification(unittest.TestCase):
    def test_a_429_from_another_source_is_not_limiter_evidence(self) -> None:
        placement = _outcome(429, "no eligible node has capacity", "5")
        self.assertFalse(is_limiter_rejection(placement, SERVER_LIMIT_MESSAGE))
        self.assertFalse(is_limiter_rejection(placement, PER_CLIENT_LIMIT_MESSAGE))
        self.assertEqual(status_histogram([placement]), {"429": 1})

    def test_limiter_messages_are_told_apart_in_the_histogram(self) -> None:
        outcomes = [
            _outcome(429, f"{SERVER_LIMIT_MESSAGE} shedding", "7"),
            _outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} too many", "1"),
            _outcome(201),
        ]
        self.assertEqual(
            status_histogram(outcomes),
            {"429 Server rate limit": 1, "429 Per-client rate limit": 1, "201": 1},
        )

    def test_every_recognized_refusal_gets_its_own_histogram_key(self) -> None:
        # A refusal the histogram does not know records as a bare status: a
        # live-cap 429 would read like a placement 429, and a capacity 503 like
        # any other unavailability. The phases whose verdicts turn on *which*
        # limiter answered record that histogram as their evidence, so a
        # missing entry costs the evidence rather than the verdict -- which is
        # worse, because the run still passes.
        # Written out rather than read from `LIMITER_MESSAGES`, so dropping an
        # entry there fails here instead of shrinking the input with it.
        refusals = [
            _outcome(429, "Server rate limit: activation bucket empty", "1"),
            _outcome(429, "Per-client rate limit: too many in flight", "1"),
            _outcome(429, "Client live sandbox cap reached: at its cap", "1"),
            _outcome(503, "Server capacity limit: no free slot", "1"),
            _outcome(429, "Server capacity limit: no eligible activation nodes", "1"),
        ]
        histogram = status_histogram(
            refusals + [_outcome(429, "no eligible node has capacity", "5"), _outcome(503, "down")]
        )
        self.assertEqual(
            histogram,
            {
                "429 Server rate limit": 1,
                "429 Per-client rate limit": 1,
                "429 Client live sandbox cap reached": 1,
                "503 Server capacity limit": 1,
                "429 Server capacity limit": 1,
                "429": 1,
                "503": 1,
            },
        )

    def test_retry_after_must_be_delta_seconds(self) -> None:
        self.assertTrue(has_retry_after_seconds(_outcome(429, "", "7")))
        self.assertTrue(has_retry_after_seconds(_outcome(429, "", " 12 ")))
        self.assertFalse(has_retry_after_seconds(_outcome(429, "", None)))
        self.assertFalse(has_retry_after_seconds(_outcome(429, "", "")))
        self.assertFalse(has_retry_after_seconds(_outcome(429, "", "Wed, 21 Oct 2026 07:28:00 GMT")))


class _Fixture:
    # Zero timeout so a bounded poll reads once and then judges that read:
    # these tests are about the verdicts, not about the waiting.
    lifecycle_transition_timeout = 0
    lifecycle_poll_interval = 0

    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id="sbx_x")
        self.records: dict[str, object] = {}

    def wait(self, seconds: float, *, reason: str | None = None) -> None:
        return None

    def sandbox_template_name(self) -> str:
        return "seed"


    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)


class _Probe:
    def __init__(
        self,
        outcome: LimiterProbeOutcome,
        *,
        kill_error: Exception | None = None,
        kill_sandbox_error: Exception | None = None,
    ) -> None:
        self.outcome = outcome
        self.kills = 0
        self.whole_window_scans: list[bool] = []
        self.killed_ids: list[str] = []
        self._kill_error = kill_error
        self._kill_sandbox_error = kill_sandbox_error

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        return self.outcome

    def kill_sandbox(self, sandbox_id: str) -> None:
        self.killed_ids.append(sandbox_id)
        if self._kill_sandbox_error is not None:
            raise self._kill_sandbox_error

    def kill_matching(
        self, metadata, *, scan_whole_window: bool = False, deadline: float | None = None
    ) -> None:
        del deadline
        self.kills += 1
        self.whole_window_scans.append(scan_whole_window)
        raise self._kill_error or EcwSandboxReconcileUnmatched(
            "no sandbox became visible during run-metadata reconciliation"
        )


class _RedisControl:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def acquire_public_api_mutation(self) -> Any:
        return lambda: None

    def redis_replicas(self) -> int:
        self.calls.append("read")
        return 1

    def scale_redis(self, replicas: int) -> int:
        self.calls.append(f"scale:{replicas}")
        if replicas == 0:
            raise TimeoutError("kubectl scale timed out after the write")
        return 0

    def wait_redis_unreachable(self, *, timeout: float) -> None:
        self.calls.append("unreachable")

    def wait_redis_rollout(self, *, timeout: float, shared_deadline: bool = False) -> None:
        self.calls.append("rollout")


class _AdminReaper:
    """Records the bench admin's late reaps."""

    def __init__(self) -> None:
        self.deleted: list[str] = []
        self.matched: list[dict[str, str]] = []

    def admin_force_delete_sandbox(self, sandbox_id: str) -> bool:
        self.deleted.append(sandbox_id)
        return True

    def admin_force_delete_matching(self, metadata: dict[str, str]) -> list[str]:
        self.matched.append(dict(metadata))
        return []


class PerClientVerdict(unittest.TestCase):
    def _assertions(
        self, probe: _Probe | None = None, control: object = None
    ) -> RateLimiterAssertions:
        return RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, control), cast(Any, probe), ResourceScope()
        )

    def test_an_all_rejected_wave_does_not_make_the_sweep_fail(self) -> None:
        # kill_matching raises on zero matches; zero admits is a valid outcome.
        probe = _Probe(_outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} x", "1"))
        assertions = self._assertions(probe)
        assertions.create_once()
        assertions.sweep_admitted()
        self.assertEqual(probe.killed_ids, [])
        self.assertEqual(probe.kills, 0)

    def test_an_admitted_create_is_swept(self) -> None:
        # Admitted ids are killed directly (404-tolerant for 120s-expired probes,
        # deduplicated), and an unanswered create in the same run still triggers
        # metadata reconciliation afterwards.
        probe = _Probe(LimiterProbeOutcome(201, None, "", "sbx_a", 0.0))
        assertions = self._assertions(probe)
        assertions.create_once()
        assertions.create_once()
        assertions.sweep_admitted()
        self.assertEqual(probe.killed_ids, ["sbx_a"])
        self.assertEqual(probe.kills, 0)

        probe.outcome = _outcome(0, "timed out")
        assertions.create_once()
        assertions.sweep_admitted()
        self.assertEqual(probe.killed_ids, ["sbx_a", "sbx_a"])
        self.assertEqual(probe.kills, 1)

        # A post-side-effect 5xx (e.g. "Sandbox started but DB fetch failed")
        # carries no id either, so `sweep_admitted` reconciles by metadata too.
        five_xx = _Probe(_outcome(500, "Sandbox started but DB fetch failed"))
        uncertain_500 = self._assertions(five_xx)
        uncertain_500.create_once()
        uncertain_500.sweep_admitted()
        self.assertEqual(five_xx.kills, 1)

        failing = _Probe(
            LimiterProbeOutcome(201, None, "", "sbx_b", 0.0),
            kill_sandbox_error=EcwSandboxDeleteRetryable("DELETE returned 503"),
        )
        doomed = self._assertions(failing)
        doomed.create_once()
        with self.assertRaises(EcwSandboxDeleteRetryable):
            doomed.sweep_admitted()
        self.assertEqual(len(failing.killed_ids), 10)

        # `ScenarioTimedOut` escapes immediately rather than continuing to
        # delete remaining ids or reconcile past the one-shot cleanup timer,
        # and hands off everything it didn't finish: the id whose DELETE was
        # cut off, the id queued behind it, and the uncertain create recorded
        # before the sweep began, whose count never grew during it.
        timed_out_probe = _Probe(
            LimiterProbeOutcome(201, None, "", "sbx_c", 0.0),
            kill_sandbox_error=ScenarioTimedOut("cleanup deadline exhausted"),
        )
        admin = _AdminReaper()
        timed_out_sweep = self._assertions(timed_out_probe, admin)
        # One worker, so the sweep's order is the subject, not its concurrency.
        cast(Any, timed_out_sweep)._sweep_delete_workers = 1
        timed_out_sweep.create_once()
        timed_out_probe.outcome = LimiterProbeOutcome(201, None, "", "sbx_d", 0.0)
        timed_out_sweep.create_once()
        timed_out_probe.outcome = _outcome(0, "timed out")
        timed_out_sweep.create_once()
        handoffs: list[threading.Thread] = []
        real_thread = threading.Thread

        def held_thread(*args: Any, **kwargs: Any) -> threading.Thread:
            thread = real_thread(*args, **kwargs)
            if kwargs.get("name") == "space-smoke-rate-limiter-late-reap":
                handoffs.append(thread)
            return thread

        with (
            mock.patch("space_smoke.rate_limiters.Thread", side_effect=held_thread),
            self.assertRaises(ScenarioTimedOut),
        ):
            timed_out_sweep.sweep_admitted()
        self.assertEqual(len(handoffs), 1)
        handoffs[0].join(timeout=30)
        self.assertFalse(handoffs[0].is_alive())
        # The sweep's own one attempt through the probe; the handoff's retry,
        # the id behind it and the metadata lookup through the bench admin,
        # since teardown may delete the run's ECW key before they run.
        self.assertEqual(timed_out_probe.killed_ids, ["sbx_c"])
        self.assertEqual(timed_out_probe.kills, 0)
        self.assertEqual(admin.deleted, ["sbx_c", "sbx_d"])
        # The metadata reap scans its whole ten-attempt window.
        self.assertEqual(admin.matched, [cast(Any, timed_out_sweep)._metadata] * 10)

    def test_admitted_probes_are_deleted_a_few_at_a_time(self) -> None:
        # An ECW DELETE waits for its operation, up to 10s, and a run admits
        # up to eighteen probes, so one by one they would outlast the sweep's
        # cap on a passing run. Six DELETEs that each wait for all six to be
        # in flight complete only if the sweep sends them together.
        ids = [f"sbx_{n}" for n in range(6)]
        together = threading.Barrier(len(ids), timeout=10)
        killed: list[str] = []

        class _Overlapping:
            outcome = LimiterProbeOutcome(201, None, "", ids[0], 0.0)

            def race_ecw_request(self, *_a: object, **_kw: object) -> LimiterProbeOutcome:
                return self.outcome

            def kill_sandbox(self, sandbox_id: str) -> None:
                together.wait()
                killed.append(sandbox_id)

            def kill_matching(self, *_a: object, **_kw: object) -> None:
                raise AssertionError("no create was uncertain")

        probe = _Overlapping()
        assertions = self._assertions(cast(Any, probe))
        for sandbox_id in ids:
            probe.outcome = LimiterProbeOutcome(201, None, "", sandbox_id, 0.0)
            assertions.create_once()
        assertions.sweep_admitted()
        self.assertCountEqual(killed, ids)

    def _sweep_threads(
        self,
    ) -> tuple[list[threading.Thread], list[threading.Thread], threading.Event, Any]:
        """Patch the sweep's `Thread` to keep its workers and its handoff, and
        to say when the handoff has been made."""

        workers: list[threading.Thread] = []
        handoffs: list[threading.Thread] = []
        handed_off = threading.Event()
        real_thread = threading.Thread

        def held_thread(*args: Any, **kwargs: Any) -> threading.Thread:
            thread = real_thread(*args, **kwargs)
            if kwargs.get("name") == "space-smoke-rate-limiter-late-reap":
                handoffs.append(thread)
                handed_off.set()
            elif kwargs.get("name") == "space-smoke-rate-limiter-sweep":
                workers.append(thread)
            return thread

        return workers, handoffs, handed_off, held_thread

    def test_a_worker_sends_no_retry_after_the_sweep_is_handed_off(self) -> None:
        # The cleanup alarm cuts the sweep off while a worker's DELETE is in
        # flight; the handoff then owns the id. That DELETE answering
        # retryable must not lead the worker to send another one.
        import signal

        from space_smoke.runtime import _scenario_deadline

        workers, handoffs, handed_off, held_thread = self._sweep_threads()
        main_thread = threading.get_ident()
        deletes: list[str] = []

        class _RetryableAfterCutoff:
            outcome = LimiterProbeOutcome(201, None, "", "sbx_r", 0.0)

            def race_ecw_request(self, *_a: object, **_kw: object) -> LimiterProbeOutcome:
                return self.outcome

            def kill_sandbox(self, sandbox_id: str) -> None:
                deletes.append(sandbox_id)
                if len(deletes) == 1:
                    signal.pthread_kill(main_thread, signal.SIGALRM)
                    assert handed_off.wait(timeout=30)
                    raise EcwSandboxDeleteRetryable("DELETE returned 503")

            def kill_matching(self, *_a: object, **_kw: object) -> None:
                raise AssertionError("no create was uncertain")

        admin = _AdminReaper()
        assertions = self._assertions(cast(Any, _RetryableAfterCutoff()), admin)
        assertions.create_once()
        with (
            mock.patch("space_smoke.rate_limiters.Thread", side_effect=held_thread),
            mock.patch("space_smoke.runtime.signal.setitimer"),
            self.assertRaises(ScenarioTimedOut),
        ):
            with _scenario_deadline(30.0, label="cleanup"):
                assertions.sweep_admitted()
        for thread in workers + handoffs:
            thread.join(timeout=30)
        # One DELETE from the worker, sent before the cutoff; the retry went
        # to the handoff, through the bench admin.
        self.assertEqual(deletes, ["sbx_r"])
        self.assertEqual(admin.deleted, ["sbx_r"])

    def test_an_alarm_while_workers_start_stops_the_ones_already_running(self) -> None:
        # The alarm lands between two worker starts. The worker already
        # running finishes its DELETE but takes no queued id after the handoff.
        workers, handoffs, handed_off, held_thread = self._sweep_threads()
        deletes: list[str] = []
        first_in_flight = threading.Event()

        class _Slow:
            outcome = LimiterProbeOutcome(201, None, "", "sbx_a", 0.0)

            def race_ecw_request(self, *_a: object, **_kw: object) -> LimiterProbeOutcome:
                return self.outcome

            def kill_sandbox(self, sandbox_id: str) -> None:
                deletes.append(sandbox_id)
                first_in_flight.set()
                assert handed_off.wait(timeout=30)

            def kill_matching(self, *_a: object, **_kw: object) -> None:
                raise AssertionError("no create was uncertain")

        probe = _Slow()
        admin = _AdminReaper()
        assertions = self._assertions(cast(Any, probe), admin)
        for sandbox_id in ("sbx_a", "sbx_b", "sbx_c"):
            probe.outcome = LimiterProbeOutcome(201, None, "", sandbox_id, 0.0)
            assertions.create_once()
        cast(Any, assertions)._sweep_delete_workers = 2
        started = [0]

        def alarm_on_second_start(*args: Any, **kwargs: Any) -> threading.Thread:
            if kwargs.get("name") == "space-smoke-rate-limiter-sweep":
                started[0] += 1
                if started[0] == 2:
                    assert first_in_flight.wait(timeout=30)
                    raise ScenarioTimedOut("cleanup deadline exhausted starting a worker")
            return cast(threading.Thread, held_thread(*args, **kwargs))

        with (
            mock.patch("space_smoke.rate_limiters.Thread", side_effect=alarm_on_second_start),
            self.assertRaises(ScenarioTimedOut),
        ):
            assertions.sweep_admitted()
        for thread in workers + handoffs:
            thread.join(timeout=30)
        self.assertEqual(deletes, ["sbx_a"])
        self.assertIn("sbx_b", admin.deleted)
        self.assertIn("sbx_c", admin.deleted)

    def test_a_sweep_that_fails_without_a_timeout_still_hands_off(self) -> None:
        # Every ECW DELETE of an admitted probe answers 503 until its retries
        # run out, and the metadata scan for an unanswered create fails: the
        # sweep reports the failure, but the admin reap still gets both the
        # undeleted id and the lookup the scan didn't finish.
        workers, handoffs, _, held_thread = self._sweep_threads()

        class _Refusing:
            outcome = LimiterProbeOutcome(201, None, "", "sbx_kept", 0.0)

            def race_ecw_request(self, *_a: object, **_kw: object) -> LimiterProbeOutcome:
                return self.outcome

            def kill_sandbox(self, _sandbox_id: str) -> None:
                raise EcwSandboxDeleteRetryable("DELETE returned 503")

            def kill_matching(self, *_a: object, **_kw: object) -> None:
                raise RuntimeError("sandbox list answered HTTP 503")

        probe = _Refusing()
        admin = _AdminReaper()
        assertions = self._assertions(cast(Any, probe), admin)
        assertions.create_once()
        probe.outcome = _outcome(0, "timed out")
        assertions.create_once()
        with (
            mock.patch("space_smoke.rate_limiters.Thread", side_effect=held_thread),
            self.assertRaises(EcwSandboxDeleteRetryable),
        ):
            assertions.sweep_admitted()
        self.assertEqual(len(handoffs), 1)
        for thread in workers + handoffs:
            thread.join(timeout=30)
        self.assertEqual(admin.deleted, ["sbx_kept"])
        self.assertEqual(admin.matched, [cast(Any, assertions)._metadata] * 10)

    def test_a_reconnect_target_the_connect_route_would_wake_is_refused(self) -> None:
        # ECW renders an in-flight restore as running, but the connect route
        # wakes a row left `restoring` and one at rest as `paused` or
        # `suspended`, so the guard reads the row as well as the ECW state.
        target = SimpleNamespace(
            sandbox_id="sbx_t",
            try_get_info=lambda: SimpleNamespace(state=EcwSandboxState.RUNNING),
        )
        for lifecycle in ("restoring", "paused", "suspended", "Restoring "):
            with self.subTest(lifecycle=lifecycle):
                control = SimpleNamespace(sandbox_lifecycle=lambda _sid, value=lifecycle: value)
                assertions = self._assertions(None, control)
                with self.assertRaisesRegex(InvariantViolation, "would wake it"):
                    assertions.expect_running(cast(Any, target), label="live_cap")
        for lifecycle in ("started", None):
            with self.subTest(lifecycle=lifecycle):
                control = SimpleNamespace(sandbox_lifecycle=lambda _sid, value=lifecycle: value)
                self._assertions(None, control).expect_running(cast(Any, target), label="live_cap")

    def test_a_delete_that_failed_before_the_deadline_is_still_handed_off(self) -> None:
        # A exhausts its DELETE retries, then B's DELETE hits the cleanup
        # deadline. A was attempted but never deleted, so the handoff retries
        # it along with B rather than leaking it.
        attempts: dict[str, int] = {"sbx_a": 0, "sbx_b": 0}

        class _FlakyDeletes:
            outcome = LimiterProbeOutcome(201, None, "", "sbx_a", 0.0)

            def race_ecw_request(self, *_a: object, **_kw: object) -> LimiterProbeOutcome:
                return self.outcome

            def kill_sandbox(self, sandbox_id: str) -> None:
                attempts[sandbox_id] += 1
                if sandbox_id == "sbx_a":
                    raise EcwSandboxDeleteRetryable("DELETE returned 503")
                raise ScenarioTimedOut("cleanup deadline exhausted during DELETE")

            def kill_matching(self, _metadata: dict[str, str], **_kw: object) -> None:
                raise AssertionError("no create was uncertain")

        probe = _FlakyDeletes()
        admin = _AdminReaper()
        assertions = self._assertions(cast(Any, probe), admin)
        # One worker, so the sweep's order is the subject, not its concurrency.
        cast(Any, assertions)._sweep_delete_workers = 1
        assertions.create_once()
        probe.outcome = LimiterProbeOutcome(201, None, "", "sbx_b", 0.0)
        assertions.create_once()
        handoffs: list[threading.Thread] = []
        at_handoff: list[dict[str, int]] = []
        real_thread = threading.Thread

        def held_thread(*args: Any, **kwargs: Any) -> threading.Thread:
            thread = real_thread(*args, **kwargs)
            if kwargs.get("name") == "space-smoke-rate-limiter-late-reap":
                at_handoff.append(dict(attempts))
                handoffs.append(thread)
            return thread

        with (
            mock.patch("space_smoke.rate_limiters.Thread", side_effect=held_thread),
            self.assertRaises(ScenarioTimedOut),
        ):
            assertions.sweep_admitted()
        # The sweep's own attempts: all ten of A's retries, then B once.
        self.assertEqual(at_handoff, [{"sbx_a": 10, "sbx_b": 1}])
        self.assertEqual(len(handoffs), 1)
        handoffs[0].join(timeout=30)
        self.assertFalse(handoffs[0].is_alive())
        # The handoff retried both, through the bench admin.
        self.assertEqual(attempts, {"sbx_a": 10, "sbx_b": 1})
        self.assertEqual(admin.deleted, ["sbx_a", "sbx_b"])
        self.assertEqual(admin.matched, [])

    def test_an_unanswered_create_is_reconciled_and_finding_nothing_is_fine(self) -> None:
        # The API may have committed a create whose answer was lost, so the
        # sweep looks; `_Probe.kill_matching` finds nothing and raises.
        probe = _Probe(_outcome(0, "timed out"))
        assertions = self._assertions(probe)
        assertions.create_once()
        assertions.sweep_admitted()
        self.assertEqual(probe.kills, 1)
        # Several unanswered creates share the run's metadata, so the sweep
        # scans the whole window instead of stopping at the first match.
        self.assertEqual(probe.whole_window_scans, [True])
        records = cast(Any, assertions)._fixture.records
        self.assertEqual(records["unanswered_creates_unmatched"]["count"], 1)

    def test_an_unanswered_create_whose_kill_fails_propagates_the_delete_error(self) -> None:
        # Finding a sandbox whose delete fails is an orphaned sandbox, not an
        # unmatched reconciliation; `EcwSandboxDeleteRetryable` is a
        # `RuntimeError` too, so only `EcwSandboxReconcileUnmatched` is benign.
        probe = _Probe(
            _outcome(0, "timed out"),
            kill_error=EcwSandboxDeleteRetryable("DELETE returned 503"),
        )
        assertions = self._assertions(probe)
        assertions.create_once()
        with self.assertRaises(EcwSandboxDeleteRetryable):
            assertions.sweep_admitted()
        self.assertNotIn("unanswered_creates_unmatched", cast(Any, assertions)._fixture.records)

    def test_redis_restoration_is_armed_before_the_scale_to_zero(self) -> None:
        control = _RedisControl()
        scope = ResourceScope()
        assertions = RateLimiterAssertions(cast(Any, _Fixture()), cast(Any, control), cast(Any, None), scope)
        with self.assertRaises(TimeoutError):
            assertions.take_redis_down()
        self.assertEqual(control.calls, ["read", "scale:0"])
        scope.close()
        self.assertEqual(control.calls, ["read", "scale:0", "scale:1", "rollout"])

    def test_a_preflight_that_is_neither_admitted_nor_limited_fails_with_evidence(self) -> None:
        # The bench found this path raising TypeError instead of a verdict: the
        # evidence keyword collided with `verify`'s own `message` parameter.
        assertions = self._assertions(_Probe(_outcome(401, "unauthorized")))
        with self.assertRaisesRegex(InvariantViolation, "neither admitted nor rate-limited"):
            assertions.preflight()

    def test_a_per_client_429_without_retry_after_fails(self) -> None:
        outcomes = [_outcome(201), _outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} x", None)]
        with self.assertRaisesRegex(InvariantViolation, "Retry-After"):
            self._assertions().expect_per_client_limited(outcomes, label="t")

    def test_only_server_429s_do_not_satisfy_the_per_client_verdict(self) -> None:
        outcomes = [_outcome(429, f"{SERVER_LIMIT_MESSAGE} x", "3")] * 3
        with self.assertRaisesRegex(InvariantViolation, "no 429"):
            self._assertions().expect_per_client_limited(outcomes, label="t")

    def test_fail_open_requires_successful_admission_for_every_caller(self) -> None:
        assertions = self._assertions()
        admitted = LimiterProbeOutcome(201, None, "", "sbx_a", 0.0)
        assertions.expect_fail_open([admitted] * 4)
        for rejected in (
            [],
            [_outcome(500)] * 4,
            [admitted, _outcome(429, "placement", "2")],
            [_outcome(429, f"{SERVER_LIMIT_MESSAGE} x", "2")],
            [_outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} x", "2")],
            [_outcome(0)],
            [_outcome(201)],
        ):
            with (
                self.subTest(outcomes=rejected),
                self.assertRaisesRegex(InvariantViolation, "fail open"),
            ):
                assertions.expect_fail_open(rejected)

    def test_a_flood_that_admitted_nothing_is_not_evidence_of_no_caps(self) -> None:
        # By this phase the earlier ones have left the cluster holding a good
        # many sandboxes, so "no limiter refused anything" is also true of a
        # flood where every create failed for capacity. An assertion of
        # absence needs a possible presence.
        assertions = self._restored_assertions()
        with self.assertRaisesRegex(InvariantViolation, "0 of 4 creates were admitted"):
            assertions.expect_restored_env_admits_the_burst(
                [
                    _outcome(503, "Server capacity limit: no free slot"),
                    _outcome(0, "timed out"),
                    _outcome(500),
                    _outcome(500),
                ]
            )

    def _restored_assertions(
        self, deployment: dict[str, str | None] | None = None
    ) -> RateLimiterAssertions:
        """Assertions after a restore: the captured standing env is the chart's,
        and the Deployment reads back `deployment` (by default, the same)."""

        standing = _stock_env()
        readback = dict(standing if deployment is None else deployment)
        control = SimpleNamespace(public_api_env=lambda keys: {key: readback.get(key) for key in keys})
        assertions = self._assertions(None, control)
        cast(Any, assertions)._standing = standing
        return assertions

    def test_a_cap_the_restore_left_in_the_deployment_fails_the_restored_env(self) -> None:
        # The burst only meets the caps its client still reaches: with the
        # allowlist gone, a listed-client live cap left at 1 never answers it.
        # The read-back of the Deployment's limiter env is what sees it.
        four = [_created(f"sbx-{n}") for n in range(4)]
        leftover = _stock_env(SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT="1")
        assertions = self._restored_assertions(leftover)
        with self.assertRaisesRegex(
            InvariantViolation, r"differs from its captured standing values.*SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT"
        ):
            assertions.expect_restored_env_admits_the_burst(four)
        self._restored_assertions().expect_restored_env_admits_the_burst(four)

    def test_the_restored_env_must_admit_a_burst_its_shrunk_caps_would_refuse(self) -> None:
        # The converse of the other verdicts: those prove the limiters reject
        # once configured, this proves the restore took the caps away again.
        # Every limiter message counts, since a cap left behind answers with
        # one; a placement 429 or a 500 does not, because neither says
        # anything about the caps.
        assertions = self._restored_assertions()
        # More admissions than the largest count cap the run applied (three
        # here: the per-client cap), so the shrunk caps can't be what admitted them.
        four = [_created(f"sbx-{n}") for n in range(4)]
        assertions.expect_restored_env_admits_the_burst(
            [*four, _outcome(429, "no eligible node has capacity", "5"), _outcome(500)]
        )
        # One admission and three unrelated failures fit under the shrunk caps
        # too, so they don't show the restore took them away.
        with self.assertRaisesRegex(InvariantViolation, "1 of 4 creates were admitted"):
            assertions.expect_restored_env_admits_the_burst(
                [_created(), _outcome(500), _outcome(500), _outcome(500)]
            )
        # Exactly as many as the shrunk caps allow isn't enough either.
        with self.assertRaisesRegex(InvariantViolation, "3 of 4 creates were admitted"):
            assertions.expect_restored_env_admits_the_burst([*four[:3], _outcome(500)])
        # A burst no larger than the caps could never show it.
        with self.assertRaisesRegex(SmokeConfigurationError, "can't exceed the shrunk caps' 3"):
            assertions.expect_restored_env_admits_the_burst(four[:3])
        # A request with no answer could have been a limiter refusal.
        with self.assertRaisesRegex(InvariantViolation, "got no answer"):
            assertions.expect_restored_env_admits_the_burst([*four, _outcome(0, "timed out")])
        # A 201 whose body named no sandbox isn't an admission.
        with self.assertRaisesRegex(InvariantViolation, "0 of 4 creates were admitted"):
            assertions.expect_restored_env_admits_the_burst([_outcome(201)] * 4)
        # Every 429 limiter refusal, the live cap included: a cap the restore
        # left behind refuses through one of them.
        for refused in (SERVER_LIMIT_MESSAGE, PER_CLIENT_LIMIT_MESSAGE, LIVE_CAP_MESSAGE):
            with self.subTest(message=refused):
                with self.assertRaisesRegex(InvariantViolation, "with the standing env restored"):
                    assertions.expect_restored_env_admits_the_burst(
                        [*four, _outcome(429, f"{refused} x", "2")]
                    )

    def test_a_slot_check_refusal_at_the_release_defaults_is_the_default_regressing(self) -> None:
        # The slot check is off by default, and a standing env that turns it
        # on skips this phase, so its 503 here means the default changed. One
        # free slot then admits some creates and refuses the rest, which the
        # admitted check alone would pass. The bodies are the API's own text
        # (`cluster_admission::check_network_slot_headroom` and the
        # empty-cluster guard), so a reworded message reds here too. The bursts
        # carry four admissions, more than the shrunk caps' three, so the
        # refusal is what the verdict turns on.
        no_free_slot = (
            "Server capacity limit: no eligible sandbox node has a free sandbox slot. "
            "Not specific to your client — retry with backoff."
        )
        empty_cluster = (
            "Server capacity limit: no eligible sandbox activation nodes are available. "
            "Not specific to your client — retry with backoff."
        )
        assertions = self._restored_assertions()
        four = [_created(f"sbx-{n}") for n in range(4)]
        with self.assertRaisesRegex(InvariantViolation, "3 of 7 requests were refused"):
            assertions.expect_restored_env_admits_the_burst(
                [*four, *(_outcome(503, no_free_slot, "2") for _ in range(3))]
            )
        # Placement refusals that aren't a cap still pass beside the admissions.
        assertions.expect_restored_env_admits_the_burst(
            [
                *four,
                _outcome(429, empty_cluster, "2"),
                _outcome(503, "upstream unavailable"),
            ]
        )


def _stock_env(**overrides: str | None) -> dict[str, str | None]:
    # What the kind chart renders: the two env masters present and the rest
    # unset (`space-api-local` deployment template and values).
    env: dict[str, str | None] = dict.fromkeys(LIMITER_ENV_KEYS)
    env["SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED"] = ""
    env["SPACE_API_CLIENT_CONCURRENCY_MODE"] = "observe"
    env.update(overrides)
    return env


class ReleaseDefaultGate(unittest.TestCase):
    """The default-admission verdict runs only on an env it can judge.

    A stock bench always renders the release defaults, so the case this gate
    exists for -- a standing env that tightens a limiter, and would refuse
    part of the burst correctly -- never reaches a bench run.
    """

    def test_the_stock_env_tightens_nothing(self) -> None:
        self.assertEqual(limiter_tightenings(_stock_env()), [])

    def test_each_stricter_setting_is_named(self) -> None:
        for key, value in (
            ("SPACE_API_CLIENT_CONCURRENCY_MODE", " Enforce "),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST", "3"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS", "0.1"),
            ("SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT", "1"),
            ("SPACE_API_CLIENT_LIVE_CAP_UNKNOWN", "+4"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED", " Yes "),
        ):
            with self.subTest(key=key):
                self.assertEqual(
                    limiter_tightenings(_stock_env(**{key: value})), [f"{key}={value}"]
                )

    def test_values_the_api_ignores_read_as_the_default(self) -> None:
        # Each falls back to the release default in the API's own parsing, so
        # none makes a limiter stricter.
        for key, value in (
            ("SPACE_API_CLIENT_CONCURRENCY_MODE", "enforcing"),
            ("SPACE_API_CLIENT_CONCURRENCY_MODE", "off"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST", "0.5"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST", "1_0"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST", "nan"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST", "120"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS", "0"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS", "-1"),
            ("SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT", "0"),
            ("SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT", "-1"),
            ("SPACE_API_CLIENT_LIVE_CAP_UNKNOWN", str(2**64)),
            ("SPACE_API_CLIENT_LIVE_CAP_UNKNOWN", "one"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED", "off"),
            ("SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED", "maybe"),
        ):
            with self.subTest(key=key, value=value):
                self.assertEqual(limiter_tightenings(_stock_env(**{key: value})), [])

    def test_a_disabled_activation_limiter_cannot_be_tightened(self) -> None:
        env = _stock_env(
            SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED="Off",
            SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST="1",
        )
        self.assertEqual(limiter_tightenings(env), [])
        # The slot check isn't behind that switch: the API runs it either way.
        env["SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED"] = "true"
        self.assertEqual(
            limiter_tightenings(env), ["SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED=true"]
        )
        del env["SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED"]
        # An unrecognized value keeps the default, which is on.
        env["SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED"] = "maybe"
        self.assertEqual(limiter_tightenings(env), ["SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST=1"])

    def test_the_gate_judges_the_captured_env_and_records_why(self) -> None:
        fixture = _Fixture()
        assertions = RateLimiterAssertions(
            cast(Any, fixture), cast(Any, None), cast(Any, None), ResourceScope()
        )
        with self.assertRaisesRegex(RuntimeError, "enable_limiters"):
            assertions.judges_release_defaults()
        assertions._standing = _stock_env()
        self.assertTrue(assertions.judges_release_defaults())
        assertions._standing = _stock_env(SPACE_API_CLIENT_CONCURRENCY_MODE="enforce")
        self.assertFalse(assertions.judges_release_defaults())
        self.assertEqual(
            fixture.records["default_admission_tightened_env"],
            ["SPACE_API_CLIENT_CONCURRENCY_MODE=enforce"],
        )
        # An unset key may come back from `envFrom` after the restore.
        assertions._standing = _stock_env()
        assertions._standing_env_from = True
        self.assertFalse(assertions.judges_release_defaults())
        self.assertIn(
            "SPACE_API_CLIENT_LIVE_CAP_PER_CLIENT",
            cast(list[str], fixture.records["default_admission_unread_env_from"]),
        )


class ControlIsolationVerdict(unittest.TestCase):
    """The isolation verdict only passes on control reconnects that worked.

    A bench run cannot show this: on a healthy cluster the control arm
    succeeds, so the cases that matter — a control arm that answered only
    404, 500, or a cluster-level 429 — never occur there. Each is a response
    set the per-client gate stays quiet through, which is exactly why
    "no per-client 429" was never evidence that a second client kept working.
    """

    def _assertions(self) -> RateLimiterAssertions:
        return RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, None), ResourceScope()
        )

    @staticmethod
    def _saturated_noisy() -> list[LimiterProbeOutcome]:
        # 200, not 201: these stand in for reconnects, and `/connect` answers
        # 200. A fixture status the path cannot produce is a fixture that has
        # drifted from what it claims to represent.
        rejected = _outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} too many", "1")
        return [rejected] * 17 + [_outcome(200)] * 3

    def test_a_successful_control_arm_passes_and_records_both_arms(self) -> None:
        # The recorded evidence is the assertion: without it this passes with
        # `expect_control_isolated` replaced by `pass`.
        assertions = self._assertions()
        control = [_outcome(200)] * 3
        assertions.expect_control_isolated(self._saturated_noisy(), control)
        records = cast(Any, assertions)._fixture.records
        self.assertEqual(records["isolation_control_statuses"], {"200": 3})
        self.assertEqual(
            records["isolation_noisy_statuses"], {"200": 3, "429 Per-client rate limit": 17}
        )

    def test_a_control_arm_that_never_succeeded_fails_whatever_it_answered(self) -> None:
        # Every response set the per-client gate stays quiet through: not
        # found, a server error, and the *cluster* limiter's own 429. Each
        # passes all four per-client verdicts, so only the success verdict
        # bites — which is the whole reason it exists.
        for answer in (
            _outcome(404, "sandbox not found"),
            _outcome(500, "internal error"),
            _outcome(429, f"{SERVER_LIMIT_MESSAGE} shedding", "6"),
        ):
            with self.subTest(status=answer.status, message=answer.message):
                with self.assertRaisesRegex(InvariantViolation, "control reconnects succeeded"):
                    self._assertions().expect_control_isolated(
                        self._saturated_noisy(), [answer] * 3
                    )

    def test_a_partly_successful_control_arm_fails(self) -> None:
        control = [_outcome(200), _outcome(404, "gone"), _outcome(200)]
        with self.assertRaisesRegex(InvariantViolation, "only 2 of 3"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), control)

    def test_a_noisy_arm_that_only_ever_lost_fails(self) -> None:
        # A shape a shared bucket wears when every other verdict holds: the
        # control requests take all of the bucket's slots, so the noisy arm
        # sees nothing but per-client 429s while the control arm succeeds.
        # Requiring one noisy admission rejects that reading. It does not make
        # the round a proof — slots churn while a wave is in flight, so one
        # bucket can still satisfy every verdict on favourable timings (see
        # `expect_control_isolated`); it removes the shape a broken limiter
        # most often produces.
        noisy = [_outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} too many", "1")] * 20
        control = [_outcome(200)] * 3
        with self.assertRaisesRegex(InvariantViolation, "distinguishes one shared bucket"):
            self._assertions().expect_control_isolated(noisy, control)

    def test_a_bucket_both_arms_share_names_isolation_not_starvation(self) -> None:
        # A shared bucket rejects both arms, which satisfies neither the
        # isolation verdict nor the noisy-admitted one. The round has to fail
        # on the first of those: "not on separate buckets" is the true
        # statement, and "admitted nothing" would describe the noisy arm as
        # starved and say nothing about the two clients sharing.
        rejected = _outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} too many", "1")
        with self.assertRaisesRegex(InvariantViolation, "not on separate buckets"):
            self._assertions().expect_control_isolated([rejected] * 20, [rejected] * 3)

    def test_an_empty_arm_fails_rather_than_passing_by_vacuity(self) -> None:
        # An empty *control* arm satisfies three verdicts for the wrong
        # reason: nothing unanswered, nothing rejected, and all zero of its
        # requests succeeded. An empty *noisy* arm would instead reach the
        # saturation verdict, which is a true-but-unhelpful reading of the
        # same mistake. Both mean the round asked the limiter nothing, so the
        # guard refuses them up front and neither reaches a verdict at all.
        with self.assertRaisesRegex(InvariantViolation, "an arm was empty"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), [])
        with self.assertRaisesRegex(InvariantViolation, "an arm was empty"):
            self._assertions().expect_control_isolated([], [_outcome(200)] * 3)

    def test_an_idle_limiter_fails_even_with_a_clean_control_arm(self) -> None:
        # Nothing metered: the control arm's success says nothing about
        # isolation because no client was ever over cap.
        noisy = [_outcome(200)] * 20
        control = [_outcome(200)] * 3
        with self.assertRaisesRegex(InvariantViolation, "drew no 429"):
            self._assertions().expect_control_isolated(noisy, control)

    def test_an_unanswered_control_request_is_not_a_clean_one(self) -> None:
        control = [_outcome(200), _outcome(0, "connection refused"), _outcome(200)]
        with self.assertRaisesRegex(InvariantViolation, "no status at all"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), control)


class _LiveCapControl:
    def __init__(self, live: int, *, expected_client: str) -> None:
        self._live = live
        self._expected_client = expected_client

    def live_client_sandboxes(self, client_id: str) -> list[str]:
        if client_id != self._expected_client:
            return []
        return [f"sbx-{index}" for index in range(self._live)]


class LiveCapPlateauVerdict(unittest.TestCase):
    """The plateau verdicts. A healthy bench produces none of the failures.

    On a working build the cap admits up to itself and refuses past it, so a
    cap that admits nothing, one that never refuses, one that overshoots, and
    a refusal with no `Retry-After` never occur there.
    """

    def _assertions(
        self, *, cap: int = 1, live: int = 1, expected_client: str | None = None
    ) -> RateLimiterAssertions:
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()),
            cast(Any, None),
            cast(Any, None),
            ResourceScope(),
        )
        cast(Any, assertions)._control = _LiveCapControl(
            live, expected_client=expected_client or assertions._noisy_id
        )
        cast(Any, assertions)._live_cap = cap
        return assertions

    @staticmethod
    def _refusal() -> LimiterProbeOutcome:
        return _outcome(429, f"{LIVE_CAP_MESSAGE} client key is at its cap", "3")

    def test_a_cap_that_admitted_nothing_is_not_a_cap(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "admitting up to it"):
            self._assertions().expect_live_cap_plateau(
                [self._refusal(), self._refusal()], [_outcome(200)]
            )

    def test_a_cap_that_never_refused_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "never fired"):
            self._assertions().expect_live_cap_plateau([_outcome(200)], [_outcome(200)])

    def test_a_live_cap_429_without_retry_after_fails(self) -> None:
        bare = _outcome(429, f"{LIVE_CAP_MESSAGE} client key is at its cap")
        with self.assertRaisesRegex(InvariantViolation, "no Retry-After"):
            self._assertions().expect_live_cap_plateau([_outcome(200), bare], [_outcome(200)])

    def test_more_admissions_than_the_cap_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "against a cap of 1"):
            self._assertions().expect_live_cap_plateau(
                [_outcome(200), _outcome(200), self._refusal()], [_outcome(200)]
            )

    def test_more_pods_than_the_cap_fails(self) -> None:
        # The API refused past the cap, but the sandboxes it already admitted
        # outnumber it, so the cap guarded the answers and not the cluster.
        with self.assertRaisesRegex(InvariantViolation, "live sandboxes against"):
            self._assertions(live=3).expect_live_cap_plateau(
                [_outcome(200), self._refusal()], [_outcome(200)]
            )

    def test_an_admission_that_produced_no_pod_fails(self) -> None:
        # The direction `<= cap` alone cannot see: one wake answered 200 and
        # placed nothing, so every answer-side verdict passes while the cap
        # guards an empty cluster. `0 <= 1` would have called this a plateau.
        with self.assertRaisesRegex(InvariantViolation, "live sandboxes against"):
            self._assertions(live=0).expect_live_cap_plateau(
                [_outcome(200), self._refusal()], [_outcome(200)]
            )

    def test_a_refused_control_reconnect_fails(self) -> None:
        for refusal in (self._refusal(), _outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} x", "2")):
            with self.subTest(message=refusal.message):
                with self.assertRaisesRegex(InvariantViolation, "already-running sandbox"):
                    self._assertions().expect_live_cap_plateau(
                        [_outcome(200), self._refusal()], [_outcome(200), refusal]
                    )

    def test_a_refusal_before_the_client_held_anything_fails(self) -> None:
        # The totals match a plateau, one admitted and one refused, but the
        # cap answered the first wake while the client held nothing.
        with self.assertRaisesRegex(InvariantViolation, "before the client held 1"):
            self._assertions().expect_live_cap_plateau(
                [self._refusal(), _outcome(200)], [_outcome(200)]
            )

    def test_an_admitted_target_missing_from_the_live_sandboxes_fails(self) -> None:
        # One live sandbox, `sbx-0`, but the admitted wake was `sbx-1`: the count
        # came from some other sandbox.
        with self.assertRaisesRegex(InvariantViolation, "isn't among the client's live"):
            self._assertions().expect_live_cap_plateau(
                [_outcome(200), self._refusal()], [_outcome(200)], targets=["sbx-1", "sbx-0"]
            )

    def test_the_admitted_target_among_the_live_sandboxes_passes(self) -> None:
        self._assertions(expected_client="capped-arm").expect_live_cap_plateau(
            [_outcome(200), self._refusal()],
            [_outcome(200)],
            targets=["sbx-0", "sbx-1"],
            client_id="capped-arm",
        )

    def test_targets_that_do_not_pair_with_the_wakes_fail(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "1 targets for 2 wakes"):
            self._assertions().expect_live_cap_plateau(
                [_outcome(200), self._refusal()], [_outcome(200)], targets=["sbx-0"]
            )

    def test_a_refused_reconnect_by_the_capped_client_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "capped client's reconnect"):
            self._assertions().expect_live_cap_plateau(
                [_outcome(200), self._refusal()],
                [_outcome(200)],
                capped_reconnects=[self._refusal()],
            )

    def test_an_empty_arm_fails_rather_than_passing_by_vacuity(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "asked the cap nothing"):
            self._assertions().expect_live_cap_plateau([], [_outcome(200)])

    def test_a_plateau_passes_and_records_both_the_answers_and_the_pods(self) -> None:
        assertions = self._assertions()
        assertions.expect_live_cap_plateau([_outcome(200), self._refusal()], [_outcome(200)])
        records = cast(Any, assertions)._fixture.records
        self.assertEqual(records["live_cap_statuses"], {"200": 1, "429 Client live sandbox cap reached": 1})
        self.assertEqual(records["live_cap_live_sandboxes"], 1)


class _AttributionControl:
    def __init__(self, labels: list[str], row: str | None, *, column: bool = True) -> None:
        self._labels = labels
        self._row = row
        self._column = column

    def activation_attribution_available(self) -> bool:
        return self._column

    def sandbox_pod_client_ids(self, sandbox_id: str) -> list[str]:
        return list(self._labels)

    def sandbox_activating_client(self, sandbox_id: str) -> str | None:
        return self._row


class ClientAttributionVerdict(unittest.TestCase):
    """The attribution verdicts.

    A bench cannot produce the cases that matter: on a working build every pod
    is labelled and the row agrees, so an unlabelled pod, a pod naming another
    client, and a row that disagrees never occur there.
    """

    def _assertions(self, labels: list[str], row: str | None) -> RateLimiterAssertions:
        return RateLimiterAssertions(
            cast(Any, _Fixture()),
            cast(Any, _AttributionControl(labels, row)),
            cast(Any, None),
            ResourceScope(),
        )

    def test_a_sandbox_with_no_pod_fails_rather_than_passing_by_vacuity(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "nothing"):
            self._assertions([], "space-smoke").expect_client_attributed("sbx-1")

    def test_an_unlabelled_pod_reads_as_never_written_not_as_written_wrong(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "never written"):
            self._assertions(["space-smoke", ""], "space-smoke").expect_client_attributed("sbx-1")

    def test_a_pod_naming_another_client_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "other than"):
            self._assertions(["someone-else"], "space-smoke").expect_client_attributed("sbx-1")

    def test_a_missing_row_reads_as_missing_not_as_an_unset_column(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "has no row"):
            self._assertions(["space-smoke"], None).expect_client_attributed("sbx-1")

    def test_an_unset_column_reads_as_never_written_not_as_written_wrong(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "never written"):
            self._assertions(["space-smoke"], "").expect_client_attributed("sbx-1")

    def test_a_row_that_disagrees_with_the_pods_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "not 'space-smoke'"):
            self._assertions(["space-smoke"], "someone-else").expect_client_attributed("sbx-1")

    def test_a_fully_attributed_sandbox_passes_and_records_both_surfaces(self) -> None:
        assertions = self._assertions(["space-smoke", "space-smoke"], "space-smoke")
        assertions.expect_client_attributed("sbx-1")
        recorded = cast(Any, assertions)._fixture.records["client_attribution"]
        self.assertEqual(recorded["pod_labels"], ["space-smoke", "space-smoke"])
        self.assertEqual(recorded["row"], "space-smoke")


class WakeFloodTargets(unittest.TestCase):
    """The control arm has to stay on the one request that takes no token.

    A bench run cannot show this either: pointing the control arm at a
    suspended sandbox still produces a round, and on a healthy cluster it may
    well pass. What it would no longer be is a statement about the per-client
    limiter -- the control requests would be queueing behind the shrunk
    cluster activation bucket -- so the guard is what keeps the phase's
    verdict about the thing it names.
    """

    def _assertions(self) -> RateLimiterAssertions:
        return RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, None), ResourceScope()
        )

    def test_a_control_target_that_is_itself_suspended_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "cluster activation bucket"):
            self._assertions().wake_flood(["sbx-a", "sbx-b"], "sbx-b")

    def test_a_flood_with_nothing_suspended_is_refused(self) -> None:
        # Otherwise the phase degrades into the reconnect round that already
        # runs beside it, and passes while exercising no restore at all.
        with self.assertRaisesRegex(ValueError, "no suspended targets"):
            self._assertions().wake_flood([], "sbx-running")

    def test_a_distinct_running_target_is_accepted_and_spread_over_the_fleet(self) -> None:
        # The positive case, so the two refusals above are not the only thing
        # the guard can do. The probe raises as soon as a request is built, so
        # the round dies past the guards with the paths it would have sent.
        assertions = self._assertions()
        probe = _RecordingProbe()
        cast(Any, assertions)._probe = probe
        with self.assertRaises(_ProbeStop):
            assertions.wake_flood(["sbx-a", "sbx-b"], "sbx-running", noisy=4, control=2)
        self.assertEqual(
            sorted(probe.paths),
            sorted(
                [
                    "/sandboxes/sbx-a/connect",
                    "/sandboxes/sbx-b/connect",
                    "/sandboxes/sbx-a/connect",
                    "/sandboxes/sbx-b/connect",
                    "/sandboxes/sbx-running/connect",
                    "/sandboxes/sbx-running/connect",
                ]
            ),
        )


class _FleetMember:
    def __init__(self, sandbox_id: str, state: object) -> None:
        self.sandbox_id = sandbox_id
        self._state = state

    def try_get_info(self):
        return None if self._state is None else SimpleNamespace(state=self._state)


class WakeFloodRestoredSomething(unittest.TestCase):
    """The flood has to prove it took the restore path.

    A `/connect` that answered 200 without restoring produces exactly the
    statuses one that restored produces, so every status verdict in
    `expect_control_isolated` passes either way. Statuses alone therefore
    cannot separate this phase from the reconnect round beside it; only the
    targets' own state can.
    """

    def _assertions(
        self,
        members: list[_FleetMember],
        pod_phases: dict[str, list[str]] | None = None,
        lifecycles: dict[str, str] | None = None,
    ) -> RateLimiterAssertions:
        pod_phases = pod_phases or {}
        lifecycles = lifecycles or {}
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, None), ResourceScope()
        )
        cast(Any, assertions)._wake_fleet = members
        cast(Any, assertions)._control = SimpleNamespace(
            sandbox_live_pod_phases=lambda sid: list(pod_phases.get(sid, ["Running"])),
            sandbox_lifecycle=lambda sid: lifecycles.get(sid, "started"),
        )
        return assertions

    def test_a_member_ecw_calls_running_without_a_running_pod_has_not_woken(self) -> None:
        # ECW renders `running` for a member still restoring, and the restore
        # pod enters `Running` before the row leaves `restoring`; either state
        # without a settled `started` row + `Running` pod must fail.
        members = [_FleetMember("a", EcwSandboxState.RUNNING)]
        with self.assertRaisesRegex(InvariantViolation, "reconnect storm, not a"):
            cast(Any, self._assertions(members, {"a": ["Pending"]})).expect_flood_restored(
                ["a"], label="wake_flood"
            )
        with self.assertRaisesRegex(InvariantViolation, "reconnect storm, not a"):
            cast(
                Any, self._assertions(members, {"a": ["Running"]}, {"a": "restoring"})
            ).expect_flood_restored(["a"], label="wake_flood")

    def test_a_flood_the_cluster_bucket_refused_says_so(self) -> None:
        members = [_FleetMember("a", EcwSandboxState.PAUSED)]
        refused = [_outcome(429, "Server rate limit: activation budget exhausted")] * 3
        with self.assertRaisesRegex(InvariantViolation, "no noisy wake was admitted"):
            cast(Any, self._assertions(members)).expect_flood_restored(
                ["a"], refused, label="wake_flood"
            )

    def test_a_flood_that_restored_nothing_fails(self) -> None:
        members = [_FleetMember("a", EcwSandboxState.PAUSED), _FleetMember("b", EcwSandboxState.PAUSED)]
        with self.assertRaisesRegex(InvariantViolation, "reconnect storm, not a"):
            cast(Any, self._assertions(members)).expect_flood_restored(["a", "b"], label="wake_flood")

    def test_one_target_leaving_paused_is_enough(self) -> None:
        members = [_FleetMember("a", EcwSandboxState.PAUSED), _FleetMember("b", EcwSandboxState.RUNNING)]
        assertions = self._assertions(members)
        cast(Any, assertions).expect_flood_restored(["a", "b"], label="wake_flood")
        self.assertEqual(
            cast(Any, assertions)._fixture.records["wake_flood_target_states"]["b"],
            str(EcwSandboxState.RUNNING),
        )

    def test_a_reaped_target_does_not_count_as_woken(self) -> None:
        # `try_get_info` answers None once a sandbox is gone, and a staged
        # member can be reaped by its own timeout while the flood is being
        # refused. Counting "deleted" as "left paused" would pass the verdict
        # on a round that restored nothing -- the one case it exists to catch.
        members = [_FleetMember("a", EcwSandboxState.PAUSED), _FleetMember("b", None)]
        with self.assertRaisesRegex(InvariantViolation, "reconnect storm, not a"):
            cast(Any, self._assertions(members)).expect_flood_restored(["a", "b"], label="wake_flood")

    def test_flooding_ids_that_are_not_staged_fails_rather_than_passing_blind(self) -> None:
        members = [_FleetMember("a", EcwSandboxState.RUNNING)]
        with self.assertRaisesRegex(InvariantViolation, "staged fleet member"):
            cast(Any, self._assertions(members)).expect_flood_restored(["z"], label="wake_flood")


class _PausableMember(_FleetMember):
    def __init__(self, sandbox_id: str, state: object) -> None:
        super().__init__(sandbox_id, state)
        self.pauses = 0

    def pause(self) -> None:
        self.pauses += 1
        self._state = EcwSandboxState.PAUSED


class ResuspendFleet(unittest.TestCase):
    def test_only_members_the_flood_woke_are_suspended_again(self) -> None:
        # The flood only has to restore one member. Suspending one it left
        # suspended has no execution to stop, and the API refuses it.
        woke = _PausableMember("a", EcwSandboxState.RUNNING)
        stayed = _PausableMember("b", EcwSandboxState.PAUSED)
        members = {"a": woke, "b": stayed}
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()),
            cast(
                Any,
                SimpleNamespace(
                    live_client_sandboxes=lambda _client: [],
                    sandbox_live_pod_phases=lambda _sid: ["Running"],
                    sandbox_lifecycle=lambda sid: (
                        "suspended" if members[sid]._state is EcwSandboxState.PAUSED else "started"
                    ),
                ),
            ),
            cast(Any, None),
            ResourceScope(),
        )
        cast(Any, assertions)._wake_fleet = [woke, stayed]
        assertions.resuspend_fleet()
        self.assertEqual((woke.pauses, stayed.pauses), (1, 0))



class _WaitingFixture(_Fixture):
    def __init__(self) -> None:
        super().__init__()
        self.waits: list[float] = []

    def wait(self, seconds: float, *, reason: str | None = None) -> None:
        self.waits.append(seconds)


class ActivationTokenWait(unittest.TestCase):
    def _assertions(self, fixture: _WaitingFixture, refill: float) -> RateLimiterAssertions:
        assertions = RateLimiterAssertions(
            cast(Any, fixture), cast(Any, None), cast(Any, None), ResourceScope()
        )
        cast(Any, assertions)._refill_rps = refill
        return assertions

    def test_waits_out_one_refill_interval_since_the_last_create(self) -> None:
        fixture = _WaitingFixture()
        assertions = self._assertions(fixture, 0.1)
        cast(Any, assertions)._last_spend_at = 100.0
        with mock.patch("space_smoke.rate_limiters.time.monotonic", return_value=103.0):
            assertions.await_activation_token()
        self.assertEqual(fixture.waits, [7.0])
        self.assertEqual(fixture.records["activation_refill_wait_seconds"], 7.0)

    def test_no_wait_once_the_interval_has_passed(self) -> None:
        fixture = _WaitingFixture()
        assertions = self._assertions(fixture, 0.1)
        cast(Any, assertions)._last_spend_at = 100.0
        with mock.patch("space_smoke.rate_limiters.time.monotonic", return_value=111.0):
            assertions.await_activation_token()
        self.assertEqual(fixture.waits, [])
        self.assertEqual(fixture.records["activation_refill_wait_seconds"], 0.0)


class _LeaseControl:
    def __init__(self) -> None:
        self.acquired = 0
        self.released = 0

    def acquire_public_api_mutation(self):
        self.acquired += 1

        def release() -> None:
            self.released += 1

        return release


class MutationLease(unittest.TestCase):
    def test_the_lease_is_taken_once_and_released_once(self) -> None:
        # The scenario takes it before staging and `enable_limiters` asks
        # again; a second acquire would contend with this run's own Lease.
        control = _LeaseControl()
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, control), cast(Any, None), ResourceScope()
        )
        for bad_id in ("", "unknown", "NATIVE", "overflow", "a,b", "a/b", "-bad"):
            with self.subTest(bad_id=bad_id), self.assertRaisesRegex(ValueError, "refusing"):
                assertions.enable_limiters(template="base", client_id=bad_id)
        self.assertEqual(control.acquired, 0)
        assertions.acquire_mutation_lease()
        assertions.acquire_mutation_lease()
        self.assertEqual(control.acquired, 1)
        assertions.release_mutation_lease()
        self.assertEqual(control.released, 1)

        # In LIFO teardown, standing-env restoration and Lease release run
        # ahead of sandbox deletes so retryable 503s cannot starve them.
        events: list[str] = []
        scope = ResourceScope()
        teardown_control = SimpleNamespace(
            acquire_public_api_mutation=lambda: (lambda: events.append("release_lease")),
            public_api_env=lambda keys: dict.fromkeys(keys, "redis://x")
            if "SPACE_REDIS_URL" in keys
            else dict.fromkeys(keys, None),
            public_api_has_env_from=lambda: False,
            restore_public_api_env=lambda _s, *, rollout_timeout=240, shared_deadline=False: events.append(
                f"restore_env:{int(rollout_timeout)}"
            ),
            set_public_api_env=lambda _v: None,
            wait_public_api_ready=lambda **_kw: None,
        )
        failing_probe = _Probe(
            LimiterProbeOutcome(201, None, "", "sbx_b", 0.0),
            kill_sandbox_error=EcwSandboxDeleteRetryable("DELETE returned 503"),
        )
        def _fail_kill(_sid: str) -> None:
            events.append("kill_sandbox")
            raise EcwSandboxDeleteRetryable("DELETE returned 503")

        failing_probe.kill_sandbox = mock.Mock(side_effect=_fail_kill)  # type: ignore[method-assign]
        run = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, teardown_control), cast(Any, failing_probe), scope
        )
        run.acquire_mutation_lease()
        scope.defer("delete staged fleet sandbox", lambda: events.append("delete_fleet"))
        run.enable_limiters(template="base")
        run.create_once()
        failures = scope.close()
        self.assertEqual(len(failures), 1)
        self.assertIn("kill every sandbox", failures[0].name)
        # Then the probe sweep, ahead of the staged fleet's delete, so a slow
        # fleet delete can't skip the sweep (and its handoff) altogether; the
        # fleet delete still runs after the sweep failed.
        self.assertEqual(
            events[:2],
            [f"restore_env:{int(CLEANUP_ROLLOUT_TIMEOUT_SECONDS)}", "release_lease"],
        )
        self.assertEqual(events[2:12], ["kill_sandbox"] * 10)
        self.assertEqual(events[12:], ["delete_fleet"])


class _ScriptedProbe:
    def __init__(self, outcomes: list[LimiterProbeOutcome]) -> None:
        self.outcomes = list(outcomes)

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        return self.outcomes.pop(0)


class CreateUntilAdmitted(unittest.TestCase):
    def _assertions(self, fixture: _WaitingFixture, outcomes) -> RateLimiterAssertions:
        assertions = RateLimiterAssertions(
            cast(Any, fixture), cast(Any, None), cast(Any, None), ResourceScope()
        )
        cast(Any, assertions)._probe = _ScriptedProbe(outcomes)
        return assertions

    def test_waits_out_the_activation_429_and_returns_the_admitted_create(self) -> None:
        fixture = _WaitingFixture()
        throttled = _outcome(429, f"{SERVER_LIMIT_MESSAGE} bucket empty", "3")
        admitted = _created("sbx-9")
        got = self._assertions(fixture, [throttled, admitted]).create_until_admitted()
        self.assertEqual(got.sandbox_id, "sbx-9")
        self.assertEqual(fixture.waits, [3])

    def test_another_failure_is_not_retried(self) -> None:
        fixture = _WaitingFixture()
        assertions = self._assertions(fixture, [_outcome(500, "boom"), _created()])
        with self.assertRaisesRegex(InvariantViolation, "no create was admitted"):
            assertions.create_until_admitted()
        self.assertEqual(fixture.waits, [])

    def test_gives_up_after_its_attempts(self) -> None:
        fixture = _WaitingFixture()
        throttled = _outcome(429, f"{SERVER_LIMIT_MESSAGE} bucket empty", "1")
        assertions = self._assertions(fixture, [throttled] * 3)
        with self.assertRaisesRegex(InvariantViolation, "in 3 attempts"):
            assertions.create_until_admitted(attempts=3)
        self.assertEqual(fixture.waits, [1, 1])


class _ProbeStop(Exception):
    """Raised by `_RecordingProbe` so a round dies before any barrier wait."""


class _RecordingProbe:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self.clients: set[str] = set()
        self._lock = threading.Lock()

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        with self._lock:
            self.paths.append(path)
            self.clients.add(client_id)
        raise _ProbeStop(path)


class RoundClientIds(unittest.TestCase):
    def test_the_reconnect_round_runs_under_ids_the_wake_flood_never_used(self) -> None:
        # A slot the wake flood still holds, released by a spawned task after
        # its handler returned, mustn't be one the reconnect round's
        # control arm, sized to the cap, needs.
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, None), ResourceScope()
        )
        cast(Any, assertions)._per_client_cap = 3
        flood, reconnect = _RecordingProbe(), _RecordingProbe()
        cast(Any, assertions)._probe = flood
        with self.assertRaises(_ProbeStop):
            assertions.wake_flood(["sbx-a"], "sbx-running", noisy=4, control=2)
        cast(Any, assertions)._probe = reconnect
        with self.assertRaises(_ProbeStop):
            assertions.isolation_round("sbx-running", noisy=4, control=2)
        self.assertEqual(len(flood.clients), 2)
        self.assertEqual(len(reconnect.clients), 2)
        self.assertFalse(flood.clients & reconnect.clients)


class _RecordingBarrier:
    """A `threading.Barrier` stand-in that records when it was aborted.

    `abort_calls` holds, per abort, whether the barrier had already tripped.
    An abort after the trip is the one that can break a party still leaving
    `wait`, so the tests judge the contract from that record rather than by
    trying to win the race.
    """

    instances: list[_RecordingBarrier] = []

    def __init__(self, parties, action=None, timeout=None) -> None:
        self._parties = parties
        self._action = action
        self._timeout = timeout
        self._arrived = 0
        self._tripped = False
        self._broken = False
        self._cond = threading.Condition()
        self.first_waiting = threading.Event()
        self.abort_calls: list[bool] = []
        self.timed_out = False
        _RecordingBarrier.instances.append(self)

    def wait(self) -> int:
        with self._cond:
            if self._broken:
                raise BrokenBarrierError
            self._arrived += 1
            if self._arrived == self._parties:
                if self._action is not None:
                    self._action()
                self._tripped = True
                self._cond.notify_all()
                return 0
            self.first_waiting.set()
            if not self._cond.wait_for(lambda: self._tripped or self._broken, self._timeout):
                self.timed_out = True
            if not self._tripped:
                self._broken = True
                raise BrokenBarrierError
            return 0

    def abort(self) -> None:
        with self._cond:
            self.abort_calls.append(self._tripped)
            self._broken = True
            self._cond.notify_all()


class _GateProbe:
    """Every request passes the gate except `/early`, which raises before it;
    `/boom` raises after it."""

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        if path == "/early":
            raise ValueError("before the gate")
        gate()
        if path == "/boom":
            raise ValueError("after the gate")
        return _outcome(200)


class ReleaseTogetherAbort(unittest.TestCase):
    def _round(self, first: str) -> _RecordingBarrier:
        _RecordingBarrier.instances = []
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, None), ResourceScope()
        )
        cast(Any, assertions)._probe = _GateProbe()
        with mock.patch("space_smoke.rate_limiters.Barrier", _RecordingBarrier):
            with self.assertRaises(ValueError):
                cast(Any, assertions)._release_together(
                    [("a", "client-a", [(first, {})] + [("/ok", {})] * 3)],
                    timeout=5,
                    label="round",
                    what="round",
                )
        (barrier,) = _RecordingBarrier.instances
        return barrier

    def test_a_raise_after_the_gate_never_aborts_the_tripped_barrier(self) -> None:
        self.assertEqual(self._round("/boom").abort_calls, [])

    def test_a_raise_before_the_gate_aborts_so_the_peers_are_not_stranded(self) -> None:
        # The peers it breaks abort on their way out too; none of it is after
        # a trip, because the barrier never tripped.
        barrier = self._round("/early")
        self.assertTrue(barrier.abort_calls)
        self.assertNotIn(True, barrier.abort_calls)
        # Released by the abort, not by sitting out the timeout.
        self.assertFalse(barrier.timed_out)

        # Same if `Thread.start` fails partway through after an earlier worker
        # is already parked at the barrier.
        _RecordingBarrier.instances = []
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, None), ResourceScope()
        )
        cast(Any, assertions)._probe = _GateProbe()
        real_start = threading.Thread.start
        started_threads: list[threading.Thread] = []
        calls = 0

        def start_once_then_fail(thread: threading.Thread) -> None:
            nonlocal calls
            calls += 1
            if calls > 1:
                self.assertTrue(_RecordingBarrier.instances[0].first_waiting.wait(timeout=5.0))
                raise RuntimeError("thread start failed mid-launch")
            started_threads.append(thread)
            real_start(thread)

        with (
            mock.patch("space_smoke.rate_limiters.Barrier", _RecordingBarrier),
            mock.patch.object(threading.Thread, "start", start_once_then_fail),
            self.assertRaises(RuntimeError),
        ):
            cast(Any, assertions)._release_together(
                [("a", "client-a", [("/ok", {}), ("/ok", {})])],
                timeout=5,
                label="round",
                what="round",
            )
        self.assertEqual(len(started_threads), 1)
        started_threads[0].join(timeout=5.0)
        self.assertFalse(started_threads[0].is_alive())
        (barrier,) = _RecordingBarrier.instances
        self.assertTrue(barrier.abort_calls)
        self.assertNotIn(True, barrier.abort_calls)
        self.assertFalse(barrier.timed_out)


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


class LiveClientSandboxCount(unittest.TestCase):
    """The read has to count what the cap counts: sandboxes, not pods.

    The cap's index maps a client-id label to *the sandboxes that have at
    least one qualifying pod* -- a sandbox or restore pod that is neither
    terminal nor being deleted. Counting pod names instead gets it wrong in
    both directions, and a bench cannot show either: a wake legitimately has
    a restore pod and a sandbox pod at once, so one capped sandbox reads as
    two and reds a working cap; and a leftover terminating pod, which the cap
    deliberately ignores, satisfies the "something was actually placed" bound
    that exists to catch an admission which placed nothing.
    """

    def test_two_pods_of_one_sandbox_count_once_and_dead_pods_not_at_all(self) -> None:
        kubectl = _ScriptedKubectl([CommandResult((), 0, '{"items": [{"metadata": {"name": "sbx-pod", "labels": {"space.pplx.net/sandbox-id": "0198a8a6-0000-7000-8000-000000000001"}}, "status": {"phase": "Running"}}, {"metadata": {"name": "restore-pod", "labels": {"space.pplx.net/sandbox-id": "0198a8a6-0000-7000-8000-000000000001"}}, "status": {"phase": "Running"}}, {"metadata": {"name": "leftover", "deletionTimestamp": "2026-11-16T00:00:00Z", "labels": {"space.pplx.net/sandbox-id": "0198a8a6-0000-7000-8000-000000000002"}}, "status": {"phase": "Running"}}, {"metadata": {"name": "done", "labels": {"space.pplx.net/sandbox-id": "0198a8a6-0000-7000-8000-000000000003"}}, "status": {"phase": "Succeeded"}}, {"metadata": {"name": "materialize", "annotations": {"space.pplx.net/checkpoint": "stopped-materialization"}, "labels": {"space.pplx.net/sandbox-id": "0198a8a6-0000-7000-8000-000000000004"}}, "status": {"phase": "Running"}}, {"metadata": {"name": "annotated", "annotations": {"space.pplx.net/sandbox-id": "0198a8a6-0000-7000-8000-000000000005"}}, "status": {"phase": "Running"}}, {"metadata": {"name": "empty-annotation", "annotations": {"space.pplx.net/sandbox-id": ""}, "labels": {"space.pplx.net/sandbox-id": "0198a8a6-0000-7000-8000-000000000006"}}, "status": {"phase": "Running"}}, {"metadata": {"name": "not-a-uuid", "labels": {"space.pplx.net/sandbox-id": "sbx-7"}}, "status": {"phase": "Running"}}]}', "")])
        harness = KindHarness(kubectl, KindConfig())
        # sbx-1 has both a sandbox pod and a restore pod; sbx-2's only pod is
        # terminating; sbx-3's has gone terminal; sbx-4's is a stopped
        # sandbox's materialization pod; sbx-5's id is only on the annotation;
        # sbx-6's annotation is present but empty, which the cap reads instead
        # of the label and drops; and sbx-7 isn't a UUID.
        live = harness.live_client_sandboxes("space-smoke")
        # Pinned so a query for the wrong client or without the managed-by
        # selector fails here, since the scripted LIST answers any selector.
        # Compared as a set of terms: their order doesn't change what it selects.
        self.assertEqual(
            _selector_terms(kubectl.calls[0]),
            {"space.pplx.net/managed-by=space-api", "space.pplx.net/client-id=space-smoke"},
        )
        self.assertEqual(live, ["0198a8a6-0000-7000-8000-000000000001", "0198a8a6-0000-7000-8000-000000000005"])
        for bad_selector in ("", "a,b=c", "a=b", "-bad"):
            with self.subTest(bad_selector=bad_selector), self.assertRaisesRegex(ValueError, "refusing"):
                harness.live_client_sandboxes(bad_selector)


def _selector_terms(command: Sequence[str]) -> set[str]:
    """The label selector's terms in a kubectl LIST, `-l` or `--selector`."""

    args = list(command)
    for flag in ("-l", "--selector"):
        if flag in args:
            return set(args[args.index(flag) + 1].split(","))
    for arg in args:
        if arg.startswith("--selector="):
            return set(arg.removeprefix("--selector=").split(","))
    raise AssertionError(f"no label selector in {args}")


class SandboxPodClientIds(unittest.TestCase):
    def test_an_unlabelled_live_pod_reads_empty_and_dead_pods_are_skipped(self) -> None:
        # An unlabelled live pod is the defect and must surface as "", while a
        # prior execution's terminating or terminal pod predates the
        # activation being judged and must not red it.
        kubectl = _ScriptedKubectl([CommandResult((), 0, '{"items": [{"metadata": {"name": "live", "labels": {"space.pplx.net/client-id": "space-smoke"}}, "status": {"phase": "Running"}}, {"metadata": {"name": "unlabelled"}, "status": {"phase": "Pending"}}, {"metadata": {"name": "old", "deletionTimestamp": "2026-11-16T00:00:00Z"}, "status": {"phase": "Running"}}, {"metadata": {"name": "done"}, "status": {"phase": "Succeeded"}}]}', "")])
        harness = KindHarness(kubectl, KindConfig())
        self.assertEqual(harness.sandbox_pod_client_ids("0198a8a6-0000-7000-8000-000000000001"), ["space-smoke", ""])
        self.assertEqual(
            _selector_terms(kubectl.calls[0]),
            {
                "space.pplx.net/managed-by=space-api",
                "space.pplx.net/sandbox-id=0198a8a6-0000-7000-8000-000000000001",
            },
        )

    def test_live_phases_skip_a_prior_generation_s_dead_and_deleting_pods(self) -> None:
        # kubectl sorts by name, so a lingering g1 comes ahead of the restored
        # g2; reading the first pod would judge the wrong generation.
        kubectl = _ScriptedKubectl([CommandResult((), 0, '{"items": [{"metadata": {"name": "space-sbx-a-g1"}, "status": {"phase": "Succeeded"}}, {"metadata": {"name": "space-sbx-a-g2", "deletionTimestamp": "2026-11-16T00:00:00Z"}, "status": {"phase": "Running"}}, {"metadata": {"name": "space-sbx-a-g3"}, "status": {"phase": "Pending"}}]}', "")])
        harness = KindHarness(kubectl, KindConfig())
        self.assertEqual(harness.sandbox_live_pod_phases("0198a8a6-0000-7000-8000-000000000001"), ["Pending"])


class PublicApiEnvOverride(unittest.TestCase):
    def test_accepts_a_comma_separated_allowlist_and_still_refuses_the_rest(self) -> None:
        # The class already admits the comma that `KNOWN_CLIENTS` needs — it is
        # there for `SPACE_NATIVE_INLINE_KINDS` too — so this pins that it
        # stays admitted and that nothing else rides in beside it. Without the
        # comma the kind bench refuses `a,b,c` outright, which is what the
        # allowlist's own grammar requires it to accept.
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        for refused in (
            "space smoke",
            "space-smoke;rm -rf /",
            "$(whoami)",
            "`id`",
            "a\nb",
            "'quoted'",
            "x" * (LIMITER_ENV_VALUE_MAX + 1),
            "",
        ):
            with self.subTest(value=refused), self.assertRaisesRegex(ValueError, "refusing"):
                harness.set_public_api_env({"SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS": refused})
        # The accepted shape clears both gates the value could fail: the value
        # class, then the closed key list. `set_public_api_env` checks those in
        # that order and only then demands the caller's mutation Lease, which
        # this bare harness does not hold — so being turned away for the Lease,
        # and not for the value, is what proves the comma got through.
        with self.assertRaisesRegex(SmokeConfigurationError, "mutation Lease"):
            harness.set_public_api_env(
                {"SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS": "space-smoke,space-smoke-noisy"}
            )

    def test_refuses_keys_outside_the_limiter_list(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        with self.assertRaisesRegex(ValueError, "refusing"):
            harness.set_public_api_env({"SPACE_INTERNAL_TOKEN": "x"})

    def test_refuses_a_value_from_reference_it_could_not_restore(self) -> None:
        argv = ("kubectl",)
        env_json = '[{"name":"SPACE_REDIS_URL","valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}]'
        harness = KindHarness(
            _ScriptedKubectl([CommandResult(argv, 0, "space-api\n", ""), CommandResult(argv, 0, env_json, "")]),
            KindConfig(),
        )
        with self.assertRaisesRegex(Exception, "valueFrom|nonliteral"):
            harness.public_api_env(("SPACE_REDIS_URL",))


if __name__ == "__main__":
    unittest.main()
