"""Unit tests for rate-limiter scenario unwind and bounded cleanup budgets."""

from __future__ import annotations

import json
import signal
import threading
import unittest
from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from space_smoke.adapters import (
    FAST_ROLLOUT_MAX_SURGE,
    FAST_ROLLOUT_STANDING_ANNOTATION,
    LIMITER_ENV_ANNOTATION,
    REDIS_RECORD_CLEAR_SECONDS,
    REDIS_REPLICAS_ANNOTATION,
    AdminReapIncomplete,
    KindConfig,
    KindHarness,
    _kube_timestamp,
    _LeaseOwnership,
)
from space_smoke.capabilities import ConsoleApiResponse, LimiterProbeOutcome
from space_smoke.rate_limiters import (
    CLEANUP_ENV_BUDGET_SECONDS,
    CLEANUP_REDIS_BUDGET_SECONDS,
    CLEANUP_REDIS_ROLLOUT_TIMEOUT_SECONDS,
    CLEANUP_ROLLOUT_TIMEOUT_SECONDS,
    CLEANUP_SWEEP_BUDGET_SECONDS,
    LIMITER_ENV_KEYS,
    RateLimiterAssertions,
)
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    ScenarioTimedOut,
    SmokeConfigurationError,
    _scenario_deadline,
)


class _Fixture:
    lifecycle_transition_timeout = 0
    lifecycle_poll_interval = 0

    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id="sbx_x")
        self.records: dict[str, object] = {}
        self.phases: list[str] = []

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        self.phases.append(name)
        yield

    def wait(self, seconds: float, *, reason: str | None = None) -> None:
        return None

    def sandbox_template_name(self) -> str:
        return "seed"

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def verify(self, condition: bool, message: str, **evidence: object) -> None:
        if not condition:
            raise InvariantViolation(f"{message} {evidence}" if evidence else message)


class _Probe:
    def __init__(self, sandbox_id: str = "sbx_probe") -> None:
        self._sandbox_id = sandbox_id
        self.killed_ids: list[str] = []

    def race_ecw_request(self, *_args: object, **_kwargs: object) -> LimiterProbeOutcome:
        return LimiterProbeOutcome(201, None, "", self._sandbox_id, 0.0)

    def kill_sandbox(self, sandbox_id: str) -> None:
        self.killed_ids.append(sandbox_id)


def _control(events: list[str], **overrides: object) -> SimpleNamespace:
    def _scale_redis(replicas: int) -> int:
        events.append(f"scale_redis:{replicas}")
        return 1

    defaults: dict[str, object] = {
        "acquire_public_api_mutation": lambda: (lambda: events.append("release_lease")),
        "public_api_env": lambda keys: dict.fromkeys(keys, "redis://x")
        if "SPACE_REDIS_URL" in keys
        else dict.fromkeys(keys, None),
        "public_api_has_env_from": lambda: False,
        "redis_replicas": lambda: 1,
        "restore_public_api_env": lambda _s, *, rollout_timeout=240, shared_deadline=False: events.append(
            f"restore_env:{int(rollout_timeout)}"
        ),
        "scale_redis": _scale_redis,
        "set_public_api_env": lambda _v: None,
        "wait_public_api_ready": lambda **_kw: None,
        "wait_redis_rollout": lambda **_kw: None,
        "wait_redis_unreachable": lambda **_kw: None,
    }
    return SimpleNamespace(**(defaults | overrides))


class _AdminReaper:
    """The bench admin's late reap: what a detached reap deletes once the
    run's ECW key may be gone. `fail_first` makes each listed id's, or the
    metadata match's, first attempt answer an error; `reaped` events fire
    once the id is recorded."""

    def __init__(
        self,
        *,
        fail_first: Collection[str] = (),
        reaped: Mapping[str, threading.Event] | None = None,
    ) -> None:
        self.deleted: list[str] = []
        self.matched: list[dict[str, str]] = []
        self.attempts: dict[str, int] = {}
        self._fail_first = set(fail_first)
        self._reaped = dict(reaped or {})

    def _attempt(self, key: str) -> None:
        self.attempts[key] = self.attempts.get(key, 0) + 1
        if key in self._fail_first and self.attempts[key] == 1:
            raise RuntimeError("admin force-delete answered HTTP 503")

    def admin_force_delete_sandbox(self, sandbox_id: str) -> bool:
        self._attempt(sandbox_id)
        self.deleted.append(sandbox_id)
        if sandbox_id in self._reaped:
            self._reaped[sandbox_id].set()
        return True

    def admin_force_delete_matching(self, metadata: Mapping[str, str]) -> list[str]:
        self._attempt("<metadata>")
        self.matched.append(dict(metadata))
        return []


class ScopedLimiterCleanup(unittest.TestCase):
    def test_scoped_limiters_unwinds_in_phases_at_full_budget_before_cleanup(self) -> None:
        events: list[str] = []
        fixture = _Fixture()
        scope = ResourceScope()
        clock = [100.0]

        def restore_env(
            _standing: object, *, rollout_timeout: float = 240, shared_deadline: bool = False
        ) -> None:
            self.assertFalse(shared_deadline)
            events.append(f"restore_env:{rollout_timeout:g}")
            clock[0] += 15.0

        control = _control(
            events,
            restore_public_api_env=restore_env,
            wait_public_api_ready=lambda *, timeout: events.append(f"ready:{timeout:g}"),
            wait_redis_rollout=lambda *, timeout, shared_deadline=False: events.append(
                f"redis_rollout:{timeout:g}"
            ),
        )
        probe = _Probe()
        assertions = RateLimiterAssertions(
            cast(Any, fixture), cast(Any, control), cast(Any, probe), scope
        )
        scope.defer("delete staged fleet sandbox", lambda: events.append("delete_fleet"))
        with (
            mock.patch("space_smoke.rate_limiters.time.monotonic", side_effect=lambda: clock[0]),
            self.assertRaisesRegex(InvariantViolation, "mid-scenario failure"),
        ):
            with assertions.scoped_limiters():
                assertions.acquire_mutation_lease()
                assertions.enable_limiters(template="base", rollout_timeout=180)
                assertions.create_once()
                assertions.take_redis_down(timeout=90)
                raise InvariantViolation("mid-scenario failure")
        self.assertEqual(
            fixture.phases,
            [
                "bring Redis back up",
                "restore the public API limiter env",
                "release the public API mutation lease",
            ],
        )
        self.assertEqual(
            events,
            [
                "ready:180",
                "scale_redis:0",
                "scale_redis:1",
                "redis_rollout:90",
                "restore_env:180",
                "ready:165",
                "release_lease",
            ],
        )
        events.clear()
        self.assertEqual(scope.close(timeout_seconds=120), ())
        self.assertEqual(events, ["delete_fleet"])
        self.assertEqual(probe.killed_ids, ["sbx_probe"])

    def test_stalled_fallback_restore_leaves_cleanup_budget_for_lease_and_fleet(self) -> None:
        # When `ScenarioTimedOut` interrupts the body, `scoped_limiters` leaves
        # restoration to `ResourceScope.close()`. Even if both restore steps
        # hang past their rollout waits (for example on slow `kubectl` metadata
        # calls), their per-compensation `ITIMER_REAL` caps interrupt them so
        # Lease release, staged fleet deletion, and the probe sweep still run.
        events: list[str] = []
        armed_caps: list[float] = []
        clock = [1000.0]
        alarm_deadline: list[float | None] = [None]
        fixture = _Fixture()
        scope = ResourceScope()

        def fake_getitimer(_which: int) -> tuple[float, float]:
            if alarm_deadline[0] is None:
                return (0.0, 0.0)
            return (max(0.0, alarm_deadline[0] - clock[0]), 0.0)

        def fake_setitimer(
            _which: int, seconds: float, _interval: float = 0.0
        ) -> tuple[float, float]:
            previous = fake_getitimer(_which)[0]
            alarm_deadline[0] = (clock[0] + seconds) if seconds > 0 else None
            return (previous, 0.0)

        def hung_restore(
            _standing: object, *, rollout_timeout: float = 240, shared_deadline: bool = False
        ) -> None:
            self.assertTrue(shared_deadline)
            events.append(f"restore_env:{rollout_timeout:g}")
            armed_caps.append(fake_getitimer(signal.ITIMER_REAL)[0])
            clock[0] += CLEANUP_ENV_BUDGET_SECONDS
            signal.raise_signal(signal.SIGALRM)

        def hung_redis_rollout(*, timeout: float, shared_deadline: bool = False) -> None:
            self.assertTrue(shared_deadline)
            events.append(f"redis_rollout:{timeout:g}")
            armed_caps.append(fake_getitimer(signal.ITIMER_REAL)[0])
            clock[0] += CLEANUP_REDIS_BUDGET_SECONDS
            signal.raise_signal(signal.SIGALRM)

        control = _control(
            events,
            restore_public_api_env=hung_restore,
            wait_redis_rollout=hung_redis_rollout,
        )
        probe = _Probe()
        assertions = RateLimiterAssertions(
            cast(Any, fixture), cast(Any, control), cast(Any, probe), scope
        )
        self.assertLess(CLEANUP_REDIS_BUDGET_SECONDS + CLEANUP_ENV_BUDGET_SECONDS, 120.0)
        with (
            mock.patch("space_smoke.runtime.time.monotonic", side_effect=lambda: clock[0]),
            mock.patch("space_smoke.rate_limiters.time.monotonic", side_effect=lambda: clock[0]),
            mock.patch("space_smoke.runtime.signal.getitimer", side_effect=fake_getitimer),
            mock.patch("space_smoke.runtime.signal.setitimer", side_effect=fake_setitimer),
        ):
            with self.assertRaises(ScenarioTimedOut):
                with assertions.scoped_limiters():
                    assertions.acquire_mutation_lease()
                    scope.defer("delete staged fleet sandbox", lambda: events.append("delete_fleet"))
                    assertions.enable_limiters(template="base", rollout_timeout=240)
                    with self.assertRaises(SmokeConfigurationError):
                        assertions.stage_suspended_fleet(1)
                    assertions.create_once()
                    assertions.take_redis_down(timeout=120)
                    events.clear()
                    raise ScenarioTimedOut("scenario execution exceeded budget")
            self.assertEqual((fixture.phases, events), ([], []))
            failures = scope.close(timeout_seconds=120)
        self.assertEqual(
            armed_caps,
            [CLEANUP_REDIS_BUDGET_SECONDS, CLEANUP_ENV_BUDGET_SECONDS],
        )
        self.assertEqual(
            [failure.name for failure in failures],
            ["scale Redis back up", "restore the public API limiter env"],
        )
        self.assertTrue(all("ScenarioTimedOut" in failure.detail for failure in failures))
        self.assertEqual(
            events,
            [
                "scale_redis:1",
                f"redis_rollout:{CLEANUP_REDIS_ROLLOUT_TIMEOUT_SECONDS + REDIS_RECORD_CLEAR_SECONDS:g}",
                "restore_env:30",
                "release_lease",
                "delete_fleet",
            ],
        )
        self.assertEqual(probe.killed_ids, ["sbx_probe"])

    def test_the_redis_backstop_bounds_its_rollout_wait_by_what_the_scale_left(self) -> None:
        # The scale's own reads and Lease writes spend the step's budget, so
        # the rollout wait gets what's left of it, less the record clear's
        # share, as a hard bound: a slow scale makes the wait shorter instead
        # of letting the step's alarm cut it off mid-rollout.
        clock = [1000.0]
        alarm_deadline: list[float | None] = [None]

        def fake_getitimer(_which: int) -> tuple[float, float]:
            if alarm_deadline[0] is None:
                return (0.0, 0.0)
            return (max(0.0, alarm_deadline[0] - clock[0]), 0.0)

        def fake_setitimer(
            _which: int, seconds: float, _interval: float = 0.0
        ) -> tuple[float, float]:
            previous = fake_getitimer(_which)[0]
            alarm_deadline[0] = (clock[0] + seconds) if seconds > 0 else None
            return (previous, 0.0)

        def close_after_slow_scale(scale_seconds: float) -> tuple[list[str], list[Any]]:
            events: list[str] = []
            scope = ResourceScope()

            def slow_scale(replicas: int) -> int:
                events.append(f"scale_redis:{replicas}")
                if replicas:
                    clock[0] += scale_seconds
                return 1

            def rollout(*, timeout: float, shared_deadline: bool = False) -> None:
                events.append(f"redis_rollout:{timeout:g}:{int(shared_deadline)}")

            control = _control(events, scale_redis=slow_scale, wait_redis_rollout=rollout)
            assertions = RateLimiterAssertions(
                cast(Any, _Fixture()), cast(Any, control), cast(Any, _Probe()), scope
            )
            assertions.acquire_mutation_lease()
            assertions.take_redis_down(timeout=120)
            events.clear()
            failures = scope.close(timeout_seconds=120)
            return events, [f for f in failures if f.name == "scale Redis back up"]

        with (
            mock.patch("space_smoke.runtime.time.monotonic", side_effect=lambda: clock[0]),
            mock.patch("space_smoke.rate_limiters.time.monotonic", side_effect=lambda: clock[0]),
            mock.patch("space_smoke.runtime.signal.getitimer", side_effect=fake_getitimer),
            mock.patch("space_smoke.runtime.signal.setitimer", side_effect=fake_setitimer),
        ):
            # Loose end: a fast scale leaves the whole fallback rollout window
            # plus the record clear's floor (15s + 2s).
            events, failures = close_after_slow_scale(0.5)
            self.assertEqual(failures, [])
            self.assertIn("redis_rollout:17:1", events)
            # 25s budget - 9s scale = 16s for the rollout and its clear, of
            # which only the 2s floor is held back from the rollout.
            events, failures = close_after_slow_scale(9.0)
            self.assertEqual(failures, [])
            self.assertIn("redis_rollout:16:1", events)
            # Tight end: 25s - 22.5s = 2.5s, just above the floor, still gets
            # a wait the adapter splits into 0.5s of rollout and 2s of clear.
            events, failures = close_after_slow_scale(22.5)
            self.assertEqual(failures, [])
            self.assertIn("redis_rollout:2.5:1", events)
            # A scale that left no more than the clear's floor starts no wait,
            # and says the record stays for the repair.
            events, failures = close_after_slow_scale(23.0)
            self.assertEqual(len(failures), 1)
            self.assertIn("record stays", failures[0].detail)
            self.assertTrue(failures[0].critical)
            self.assertFalse(any(e.startswith("redis_rollout") for e in events))

    def test_a_stuck_sweep_is_capped_hands_off_its_ids_and_leaves_the_fleet_delete_time(self) -> None:
        # The sweep runs after the cluster restorations and before the staged
        # fleet's deletes, which register later: a slow fleet delete can no
        # longer use up the budget and skip the sweep and its handoff. Its own
        # cap keeps the fleet deletes eligible in turn: a stuck DELETE is cut
        # off there, its id goes to the admin reap, and the fleet delete runs.
        events: list[str] = []
        armed: list[float] = []
        clock = [1000.0]
        alarm_deadline: list[float | None] = [None]

        def fake_getitimer(_which: int) -> tuple[float, float]:
            if alarm_deadline[0] is None:
                return (0.0, 0.0)
            return (max(0.0, alarm_deadline[0] - clock[0]), 0.0)

        def fake_setitimer(_which: int, seconds: float, _interval: float = 0.0) -> tuple[float, float]:
            previous = fake_getitimer(_which)[0]
            alarm_deadline[0] = (clock[0] + seconds) if seconds > 0 else None
            return (previous, 0.0)

        unstick = threading.Event()

        class _StuckProbe(_Probe):
            def kill_sandbox(self, sandbox_id: str) -> None:
                events.append(f"sweep:{sandbox_id}")
                armed.append(fake_getitimer(signal.ITIMER_REAL)[0])
                clock[0] += CLEANUP_SWEEP_BUDGET_SECONDS
                signal.raise_signal(signal.SIGALRM)
                # A stuck DELETE doesn't return: the cap's alarm cuts the
                # sweep off while this worker is still waiting on it.
                unstick.wait(timeout=30)
                raise TimeoutError("DELETE never answered")

        admin = _AdminReaper()
        control = _control(
            events,
            admin_force_delete_sandbox=admin.admin_force_delete_sandbox,
            admin_force_delete_matching=admin.admin_force_delete_matching,
        )
        scope = ResourceScope()
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, control), cast(Any, _StuckProbe()), scope
        )
        handoffs: list[threading.Thread] = []
        real_thread = threading.Thread

        def held_thread(*args: Any, **kwargs: Any) -> threading.Thread:
            thread = real_thread(*args, **kwargs)
            if kwargs.get("name") == "space-smoke-rate-limiter-late-reap":
                handoffs.append(thread)
            return thread

        with (
            mock.patch("space_smoke.runtime.time.monotonic", side_effect=lambda: clock[0]),
            mock.patch("space_smoke.rate_limiters.time.monotonic", side_effect=lambda: clock[0]),
            mock.patch("space_smoke.runtime.signal.getitimer", side_effect=fake_getitimer),
            mock.patch("space_smoke.runtime.signal.setitimer", side_effect=fake_setitimer),
            mock.patch("space_smoke.rate_limiters.Thread", side_effect=held_thread),
        ):
            assertions.acquire_mutation_lease()
            assertions.create_once()
            # The fleet registers its delete after the sweep, then the run
            # promotes the restorations and the sweep above it, as staging does.
            scope.defer("delete staged fleet sandbox", lambda: events.append("delete_fleet"))
            assertions._redefer_lease_above_sandboxes()
            try:
                failures = scope.close(timeout_seconds=120)
            finally:
                unstick.set()
        self.assertEqual(events, ["release_lease", "sweep:sbx_probe", "delete_fleet"])
        self.assertEqual(armed, [CLEANUP_SWEEP_BUDGET_SECONDS])
        self.assertEqual([f.name for f in failures], ["kill every sandbox the limiter probes admitted"])
        self.assertIn("ScenarioTimedOut", failures[0].detail)
        self.assertEqual(len(handoffs), 1)
        handoffs[0].join(timeout=30)
        self.assertEqual(admin.deleted, ["sbx_probe"])

    def test_a_failed_unwind_keeps_the_lease_held_for_the_cleanup_backstop(self) -> None:
        events: list[str] = []
        fixture = _Fixture()
        scope = ResourceScope()
        attempts = 0

        def flaky_restore(
            _standing: object, *, rollout_timeout: float = 240, shared_deadline: bool = False
        ) -> None:
            nonlocal attempts
            attempts += 1
            events.append(f"restore_env:{int(rollout_timeout)}:{int(shared_deadline)}")
            if attempts == 1:
                raise RuntimeError("first rollout stalled")

        assertions = RateLimiterAssertions(
            cast(Any, fixture),
            cast(Any, _control(events, restore_public_api_env=flaky_restore)),
            cast(Any, None),
            scope,
        )
        with self.assertRaisesRegex(InvariantViolation, "wake flood failed") as raised:
            with assertions.scoped_limiters():
                assertions.enable_limiters(template="base", rollout_timeout=180)
                raise InvariantViolation("wake flood failed")
        self.assertIn("limiter_env_restore_failure", fixture.records)
        self.assertTrue(
            any("bounded cleanup compensation stays armed" in note for note in raised.exception.__notes__)
        )
        # Lease stays held so the `ResourceScope` fallback can still write the Deployment.
        self.assertEqual(events, ["restore_env:180:0"])
        self.assertEqual(scope.close(timeout_seconds=120), ())
        self.assertEqual(
            events,
            [
                "restore_env:180:0",
                f"restore_env:{int(CLEANUP_ROLLOUT_TIMEOUT_SECONDS)}:1",
                "release_lease",
            ],
        )

        # And if `restore_limiters` itself fails inside the `with` block,
        # `scoped_limiters` does not repeat a second full-budget rollout before
        # handing off to the 30s cleanup backstop; `/readyz` also keeps a floor
        # when the rollout consumes the full window.
        events.clear()
        ready_budgets: list[float] = []
        scope2 = ResourceScope()
        attempts = 0
        clock = [200.0]

        def slow_second_restore(
            _standing: object, *, rollout_timeout: float = 240, shared_deadline: bool = False
        ) -> None:
            flaky_restore(_standing, rollout_timeout=rollout_timeout, shared_deadline=shared_deadline)
            clock[0] += 31.0

        assertions2 = RateLimiterAssertions(
            cast(Any, _Fixture()),
            cast(
                Any,
                _control(
                    events,
                    restore_public_api_env=slow_second_restore,
                    wait_public_api_ready=lambda *, timeout: ready_budgets.append(timeout),
                ),
            ),
            cast(Any, None),
            scope2,
        )
        with mock.patch("space_smoke.rate_limiters.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertRaisesRegex(RuntimeError, "first rollout stalled"):
                with assertions2.scoped_limiters():
                    assertions2.enable_limiters(template="base", rollout_timeout=180)
                    events.clear()
                    ready_budgets.clear()
                    assertions2.restore_limiters(release_lease=False)
            self.assertEqual(events, ["restore_env:180:0"])
            self.assertEqual(scope2.close(), ())
        self.assertEqual(
            events,
            ["restore_env:180:0", f"restore_env:{int(CLEANUP_ROLLOUT_TIMEOUT_SECONDS)}:1", "release_lease"],
        )
        self.assertEqual(ready_budgets, [5.0])

        # Refusing an un-restorable standing value happens before arming the
        # restore compensation, so unwind never writes to an untouched Deployment.
        bad_events: list[str] = []
        lease_acquires = [0]
        bad_scope = ResourceScope()

        def _spy_acquire() -> Any:
            lease_acquires[0] += 1
            return lambda: bad_events.append("release_lease")

        def _bad_public_api_env(keys: tuple[str, ...]) -> dict[str, str | None]:
            if "SPACE_REDIS_URL" in keys:
                return {"SPACE_REDIS_URL": "redis://x"}
            return {key: (" 1 " if key == LIMITER_ENV_KEYS[0] else None) for key in keys}

        # Every Deployment write is recorded: a rejected standing env must be
        # refused before anything is written, not just before the error.
        env_writes: list[object] = []
        bad_control = _control(
            bad_events,
            acquire_public_api_mutation=_spy_acquire,
            public_api_env=_bad_public_api_env,
            set_public_api_env=env_writes.append,
        )
        bad_assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, bad_control), cast(Any, None), bad_scope
        )
        for bad_rps in ("", " 1 ", "nan", "inf", "-inf", "0", "-0.5", "1_0", "1;2"):
            with self.subTest(bad_rps=bad_rps), self.assertRaisesRegex(ValueError, "cluster_refill_rps"):
                bad_assertions.enable_limiters(template="base", cluster_refill_rps=bad_rps)
        self.assertEqual(lease_acquires[0], 0)
        self.assertEqual(bad_events, [])
        with self.assertRaisesRegex(SmokeConfigurationError, "restore cannot write"):
            with bad_assertions.scoped_limiters():
                bad_assertions.enable_limiters(template="base")
        self.assertEqual(lease_acquires[0], 1)
        self.assertEqual(bad_events, ["release_lease"])
        self.assertEqual(env_writes, [])
        self.assertEqual(bad_scope.close(), ())

        incomplete_events: list[str] = []

        def _incomplete_public_api_env(keys: tuple[str, ...]) -> dict[str, str | None]:
            if "SPACE_REDIS_URL" in keys:
                return {"SPACE_REDIS_URL": "redis://x"}
            return {keys[0]: None}

        incomplete_assertions = RateLimiterAssertions(
            cast(Any, _Fixture()),
            cast(
                Any,
                _control(
                    incomplete_events,
                    public_api_env=_incomplete_public_api_env,
                    set_public_api_env=env_writes.append,
                ),
            ),
            cast(Any, None),
            ResourceScope(),
        )
        with self.assertRaisesRegex(SmokeConfigurationError, "restore cannot write"):
            with incomplete_assertions.scoped_limiters():
                incomplete_assertions.enable_limiters(template="base")
        self.assertEqual(incomplete_events, ["release_lease"])
        self.assertEqual(env_writes, [])

        # `ResourceScope.redefer` promotes multiple armed compensations in one
        # update without disarming them, and `close()`'s identity guard runs a
        # duplicated failing compensation at most once before continuing.
        redefer_log: list[str] = []
        redefer_scope = ResourceScope()
        redefer_scope.defer("sandbox", lambda: redefer_log.append("sandbox"))
        lease_comp = redefer_scope.defer("lease", lambda: redefer_log.append("lease"))
        env_comp = redefer_scope.defer("env", lambda: redefer_log.append("env"))
        redis_comp = redefer_scope.defer("redis", lambda: redefer_log.append("redis"))
        redefer_scope.defer("fleet", lambda: redefer_log.append("fleet"))
        with self.assertRaisesRegex(ValueError, "not registered"):
            redefer_scope.redefer(ResourceScope().defer("foreign", lambda: None))
        redefer_scope.redefer(lease_comp, env_comp, redis_comp)
        self.assertTrue(all(c.armed for c in (lease_comp, env_comp, redis_comp)))
        self.assertEqual(redefer_scope.close(), ())
        self.assertEqual(redefer_log, ["redis", "env", "lease", "fleet", "sandbox"])

        dup_log: list[str] = []
        dup_scope = ResourceScope()
        dup_scope.defer("next", lambda: dup_log.append("next"))

        def _fail_dup() -> None:
            dup_log.append("failing")
            raise RuntimeError("boom")

        dup_comp = dup_scope.defer("failing", _fail_dup)
        dup_scope._compensations.insert(0, dup_comp)
        dup_failures = dup_scope.close()
        self.assertEqual(dup_log, ["failing", "next"])
        self.assertEqual([f.name for f in dup_failures], ["failing"])

    def test_scenario_deadline_mid_unwind_preserves_primary_verdict_and_stops_eager_unwind(self) -> None:
        events: list[str] = []
        fixture = _Fixture()
        scope = ResourceScope()

        def timeout_during_redis_rollout(*, timeout: float, shared_deadline: bool = False) -> None:
            events.append(f"redis_rollout:{int(timeout)}")
            if timeout > 30:
                raise ScenarioTimedOut("execution deadline fired during unwind")

        control = _control(events, wait_redis_rollout=timeout_during_redis_rollout)
        assertions = RateLimiterAssertions(
            cast(Any, fixture), cast(Any, control), cast(Any, None), scope
        )
        with self.assertRaisesRegex(InvariantViolation, "fail-open probe failed") as raised:
            with assertions.scoped_limiters():
                assertions.enable_limiters(template="base", rollout_timeout=180)
                with self.assertRaises(SmokeConfigurationError):
                    assertions.enable_limiters(template="base", rollout_timeout=180)
                assertions.take_redis_down(timeout=90)
                with self.assertRaises(SmokeConfigurationError):
                    assertions.take_redis_down(timeout=90)
                events.clear()
                raise InvariantViolation("fail-open probe failed")
        # `ScenarioTimedOut` preserves the primary `InvariantViolation` verdict
        # and stops the eager unwind before env restore or Lease release.
        self.assertTrue(any("ScenarioTimedOut" in note for note in raised.exception.__notes__))
        self.assertEqual(events, ["scale_redis:1", "redis_rollout:90"])
        self.assertEqual(scope.close(timeout_seconds=120), ())
        self.assertEqual(
            events,
            [
                "scale_redis:1",
                "redis_rollout:90",
                "scale_redis:1",
                f"redis_rollout:{CLEANUP_REDIS_ROLLOUT_TIMEOUT_SECONDS + REDIS_RECORD_CLEAR_SECONDS:g}",
                f"restore_env:{int(CLEANUP_ROLLOUT_TIMEOUT_SECONDS)}",
                "release_lease",
            ],
        )

        # `ScenarioTimedOut` during concurrent HTTP fan-out (`_release_together`
        # and `burst`) escapes immediately without joining stalled workers;
        # `sweep_admitted` waits for interrupted create workers before
        # reconciling, and any straggler that outlives the settle window fails
        # cleanup loudly and reaps its own sandbox on late arrival.
        main_thread_id = threading.get_ident()
        reconciled: list[dict[str, str]] = []
        killed_ids: list[str] = []
        release_connects = threading.Event()
        connect_returned = threading.Event()
        release_creates = threading.Event()
        create_returned = threading.Event()
        release_straggler = threading.Event()
        straggler_reaped = threading.Event()
        connect_ready = threading.Barrier(
            2, action=lambda: signal.pthread_kill(main_thread_id, signal.SIGALRM)
        )
        creates_ready = threading.Barrier(
            3, action=lambda: signal.pthread_kill(main_thread_id, signal.SIGALRM)
        )
        straggler_ready = threading.Barrier(
            1, action=lambda: signal.pthread_kill(main_thread_id, signal.SIGALRM)
        )
        phase_mode = ["early"]
        slow_lock = threading.Lock()
        slow_seq = [0]

        class _SlowProbe:
            def race_ecw_request(
                self, _method: str, path: str, *_a: object, gate: Any = None, **_kw: object
            ) -> LimiterProbeOutcome:
                if gate is not None:
                    gate()
                if path in ("/x", "/y"):
                    connect_ready.wait(timeout=30.0)
                    release_connects.wait(timeout=30.0)
                    connect_returned.set()
                    return LimiterProbeOutcome(200, None, "", None, 0.0)
                if phase_mode[0] == "early":
                    return LimiterProbeOutcome(201, None, "", "sbx_early", 0.0)
                if phase_mode[0] == "burst":
                    creates_ready.wait(timeout=30.0)
                    release_creates.wait(timeout=30.0)
                    with slow_lock:
                        slow_seq[0] += 1
                        slow_id = f"sbx_slow_{slow_seq[0]}"
                    create_returned.set()
                    return LimiterProbeOutcome(201, None, "", slow_id, 0.0)
                straggler_ready.wait(timeout=30.0)
                release_straggler.wait(timeout=30.0)
                return LimiterProbeOutcome(201, None, "", "sbx_straggler", 0.0)

            def kill_sandbox(self, sid: str) -> None:
                killed_ids.append(sid)
                if sid == "sbx_early":
                    release_creates.set()

            def kill_matching(self, metadata: dict[str, str], **_kw: object) -> None:
                reconciled.append(dict(metadata))

        # Late reaps go to the bench admin, not the probe: by then teardown
        # may have deleted the run's ECW key the probe authenticates with.
        admin = _AdminReaper(
            fail_first={"sbx_straggler", "<metadata>"},
            reaped={"sbx_straggler": straggler_reaped},
        )
        fanout_assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, admin), cast(Any, _SlowProbe()), ResourceScope()
        )
        # Barrier actions deliver `SIGALRM` deterministically once all workers
        # reach the stall point; patching `setitimer` avoids racing an OS timer
        # against `SIG_DFL` restoration if thread startup pauses under CI load.
        try:
            with mock.patch("space_smoke.runtime.signal.setitimer"):
                with self.assertRaises(ScenarioTimedOut):
                    with _scenario_deadline(30.0, label="execution"):
                        fanout_assertions._release_together(
                            [("noisy", "n", [("/x", {})]), ("control", "c", [("/y", {})])],
                            timeout=30,
                            label="round",
                            what="round",
                        )
                self.assertFalse(connect_returned.is_set())

                fanout_assertions.create_once()
                phase_mode[0] = "burst"
                with self.assertRaises(ScenarioTimedOut):
                    with _scenario_deadline(30.0, label="execution"):
                        fanout_assertions.burst_creates(3)
                self.assertFalse(create_returned.is_set())
                fanout_assertions.sweep_admitted()
                self.assertTrue(create_returned.is_set())
                self.assertEqual(killed_ids[0], "sbx_early")
                self.assertCountEqual(
                    killed_ids[1:], ["sbx_slow_1", "sbx_slow_2", "sbx_slow_3"]
                )
                self.assertEqual(len(reconciled), 1)

                phase_mode[0] = "straggler"
                fanout_assertions._inflight_settle_timeout = 0.0
                with self.assertRaises(ScenarioTimedOut):
                    with _scenario_deadline(30.0, label="execution"):
                        fanout_assertions.burst_creates(1)
                with self.assertRaisesRegex(RuntimeError, "remained in flight"):
                    fanout_assertions.sweep_admitted()
                self.assertNotIn("sbx_straggler", killed_ids)
                release_straggler.set()
                self.assertTrue(straggler_reaped.wait(timeout=30.0))
                # Retried past its first failure, and never through the probe.
                self.assertEqual(admin.attempts["sbx_straggler"], 2)
                self.assertEqual(admin.deleted, ["sbx_straggler"])
                self.assertNotIn("sbx_straggler", killed_ids)

                # The metadata reap scans every attempt of its window, its
                # first failure included, rather than stopping at a match.
                reconciled_before = len(reconciled)
                fanout_assertions._reap_late_create(None, uncertain=True)
                self.assertEqual(admin.attempts["<metadata>"], 10)
                self.assertEqual(admin.matched, [fanout_assertions._metadata] * 9)
                self.assertEqual(len(reconciled), reconciled_before)

                # If an interrupted worker finishes while Pass 1's DELETE is in
                # progress and that DELETE then hits `ScenarioTimedOut`, the
                # sweep hands off the newly arrived ID so it isn't leaked.
                during_delete_ready = threading.Barrier(
                    1, action=lambda: signal.pthread_kill(main_thread_id, signal.SIGALRM)
                )
                release_during_delete = threading.Event()
                during_delete_recorded = threading.Event()
                during_delete_reaped = threading.Event()
                during_delete_mode = ["initial"]

                class _TimedOutDeleteProbe:
                    def race_ecw_request(
                        self, *_a: object, gate: Any = None, **_kw: object
                    ) -> LimiterProbeOutcome:
                        if gate is not None:
                            gate()
                        if during_delete_mode[0] == "initial":
                            return LimiterProbeOutcome(201, None, "", "sbx_initial", 0.0)
                        during_delete_ready.wait(timeout=30.0)
                        release_during_delete.wait(timeout=30.0)
                        return LimiterProbeOutcome(201, None, "", "sbx_during_delete", 0.0)

                    def kill_sandbox(self, sid: str) -> None:
                        if sid == "sbx_initial":
                            release_during_delete.set()
                            assert during_delete_recorded.wait(timeout=30.0)
                            raise ScenarioTimedOut("cleanup deadline exhausted during DELETE")
                        raise AssertionError(f"{sid} reached the probe after the sweep moved on")

                    def kill_matching(self, _metadata: dict[str, str], **_kw: object) -> None:
                        pass

                during_admin = _AdminReaper(reaped={"sbx_during_delete": during_delete_reaped})
                timed_assertions = RateLimiterAssertions(
                    cast(Any, _Fixture()),
                    cast(Any, during_admin),
                    cast(Any, _TimedOutDeleteProbe()),
                    ResourceScope(),
                )
                orig_finish = timed_assertions._finish_create_outcome

                def _notify_finish(inflight: Any, outcome: LimiterProbeOutcome) -> None:
                    orig_finish(inflight, outcome)
                    if outcome.sandbox_id == "sbx_during_delete":
                        during_delete_recorded.set()

                timed_assertions._finish_create_outcome = _notify_finish  # type: ignore[method-assign]
                try:
                    timed_assertions.create_once()
                    during_delete_mode[0] = "late"
                    with self.assertRaises(ScenarioTimedOut):
                        with _scenario_deadline(30.0, label="execution"):
                            timed_assertions.burst_creates(1)
                    with self.assertRaises(ScenarioTimedOut):
                        timed_assertions.sweep_admitted()
                    self.assertTrue(during_delete_reaped.wait(timeout=30.0))
                    self.assertIn("sbx_initial", during_admin.deleted)
                finally:
                    release_during_delete.set()

                # Likewise if Pass 2's own DELETE is interrupted by `ScenarioTimedOut`,
                # the late-arriving ID isn't marked killed early and is handed off.
                pass2_ready = threading.Barrier(
                    1, action=lambda: signal.pthread_kill(main_thread_id, signal.SIGALRM)
                )
                release_pass2 = threading.Event()
                pass2_reaped = threading.Event()
                pass2_attempts = [0]

                class _Pass2TimedOutProbe:
                    def race_ecw_request(
                        self, *_a: object, gate: Any = None, **_kw: object
                    ) -> LimiterProbeOutcome:
                        if gate is not None:
                            gate()
                        pass2_ready.wait(timeout=30.0)
                        release_pass2.wait(timeout=30.0)
                        return LimiterProbeOutcome(201, None, "", "sbx_pass2_late", 0.0)

                    def kill_sandbox(self, sid: str) -> None:
                        if sid == "sbx_pass2_late":
                            pass2_attempts[0] += 1
                            raise ScenarioTimedOut("cleanup deadline exhausted during Pass 2")

                    def kill_matching(self, _metadata: dict[str, str], **_kw: object) -> None:
                        pass

                pass2_admin = _AdminReaper(reaped={"sbx_pass2_late": pass2_reaped})
                pass2_assertions = RateLimiterAssertions(
                    cast(Any, _Fixture()),
                    cast(Any, pass2_admin),
                    cast(Any, _Pass2TimedOutProbe()),
                    ResourceScope(),
                )
                try:
                    with self.assertRaises(ScenarioTimedOut):
                        with _scenario_deadline(30.0, label="execution"):
                            pass2_assertions.burst_creates(1)
                    release_pass2.set()
                    with self.assertRaises(ScenarioTimedOut):
                        pass2_assertions.sweep_admitted()
                    self.assertTrue(pass2_reaped.wait(timeout=30.0))
                    self.assertEqual(pass2_attempts[0], 1)
                finally:
                    release_pass2.set()
        finally:
            release_connects.set()
            release_creates.set()
            release_straggler.set()


class _RecordingExecutor:
    def __init__(self, *results: CommandResult) -> None:
        self._results = list(results)
        self.calls: list[tuple[tuple[str, ...], float | None]] = []

    def run(self, command: list[str], *, timeout: float | None = None, **_kw: object) -> CommandResult:
        self.calls.append((tuple(command), timeout))
        return self._results.pop(0)


def _lease_json(
    *,
    holder: str = "me",
    recorded_env: str | None = json.dumps({LIMITER_ENV_KEYS[0]: None}),
    recorded_redis: str | None = None,
) -> str:
    annotations: dict[str, str] = {}
    if recorded_env is not None:
        annotations[LIMITER_ENV_ANNOTATION] = recorded_env
    if recorded_redis is not None:
        annotations[REDIS_REPLICAS_ANNOTATION] = recorded_redis
    return json.dumps(
        {
            "metadata": {
                "name": "space-smoke-public-api-x",
                "uid": "lease-uid",
                "resourceVersion": "7",
                "annotations": annotations,
            },
            "spec": {
                "holderIdentity": holder,
                "leaseDurationSeconds": 600,
                "renewTime": _kube_timestamp(datetime.now(UTC)),
            },
        }
    )


def _deployment_json(*, fast: bool = False) -> str:
    annotations = (
        {
            FAST_ROLLOUT_STANDING_ANNOTATION: json.dumps(
                {"/spec/minReadySeconds": 5, "/spec/strategy/rollingUpdate/maxSurge": "25%"},
                sort_keys=True,
            )
        }
        if fast
        else {}
    )
    return json.dumps(
        {
            "metadata": {
                "name": "space-api",
                "uid": "deploy-uid",
                "resourceVersion": "11",
                "annotations": annotations,
            },
            "spec": {
                "replicas": 2,
                "minReadySeconds": 0 if fast else 5,
                "selector": {"matchLabels": {"app": "space-api"}},
                "strategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxSurge": FAST_ROLLOUT_MAX_SURGE if fast else "25%"},
                },
            },
            "status": {"replicas": 2},
        }
    )


class KindAdminBearerStaysOnLoopback(unittest.TestCase):
    """Admin calls carry the bench's admin bearer, so a redirect or a proxy
    setting must not carry it anywhere but the loopback API."""

    def test_a_redirect_surfaces_as_its_status_and_no_request_reaches_its_target(self) -> None:
        import http.server
        import os
        import tempfile

        received: list[dict[str, str]] = []

        class _Recorder(http.server.BaseHTTPRequestHandler):
            def _record(self) -> None:
                received.append(dict(self.headers.items()))
                self.send_response(200)
                self.end_headers()

            do_GET = do_POST = _record

            def log_message(self, *_a: object) -> None:
                return None

        recorder = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
        target = f"http://127.0.0.1:{recorder.server_address[1]}"

        class _Redirector(http.server.BaseHTTPRequestHandler):
            def _redirect(self) -> None:
                self.send_response(302)
                self.send_header("Location", target + self.path)
                self.end_headers()

            do_GET = do_POST = _redirect

            def log_message(self, *_a: object) -> None:
                return None

        redirector = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Redirector)
        servers = [recorder, redirector]
        for server in servers:
            threading.Thread(target=server.serve_forever, daemon=True).start()
        with tempfile.NamedTemporaryFile("w", suffix=".token") as token_file:
            token_file.write("admin-bearer")
            token_file.flush()
            environment = {"SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE": token_file.name}
            harness = KindHarness(
                cast(Any, _RecordingExecutor()),
                KindConfig(api_url=f"http://127.0.0.1:{redirector.server_address[1]}"),
                environment=environment,
            )
            try:
                # A proxy setting that doesn't bypass loopback must not route them either.
                with mock.patch.dict(os.environ, {"http_proxy": target, "no_proxy": ""}):
                    answer = harness.console_api("GET", "/admin/api/v1/sandboxes?cursor=")
                    self.assertEqual(answer.status, 302)
                    with self.assertRaisesRegex(SmokeConfigurationError, "HTTP 302"):
                        harness.admin_request("GET", "sandboxes")
                    with self.assertRaisesRegex(SmokeConfigurationError, "force-delete answered HTTP 302"):
                        harness.admin_force_delete_sandbox("11111111-1111-1111-1111-111111111111")
            finally:
                for server in servers:
                    server.shutdown()
                    server.server_close()
        self.assertEqual(received, [])


class NewerLimiterEnvRecord(unittest.TestCase):
    def test_a_record_naming_unknown_keys_says_exactly_how_to_recover(self) -> None:
        # A newer runner's record can name limiter keys this checkout doesn't
        # know; the repair refuses it as before, and the error carries the
        # exact recovery for this Lease and context.
        harness = KindHarness(cast(Any, _RecordingExecutor()), KindConfig())
        ownership = _LeaseOwnership(
            name="space-smoke-public-api-x", uid="lease-uid", holder="me", subject="public API mutation"
        )
        record = json.dumps({LIMITER_ENV_KEYS[0]: None, "SPACE_SOME_FUTURE_LIMITER": "1"})
        with self.assertRaises(ValueError) as raised:
            harness._repair_recorded_limiter_faults(
                {"metadata": {"annotations": {LIMITER_ENV_ANNOTATION: record}}}, ownership
            )
        message = str(raised.exception)
        self.assertIn("SPACE_SOME_FUTURE_LIMITER", message)
        self.assertIn(
            f"kubectl annotate lease space-smoke-public-api-x {LIMITER_ENV_ANNOTATION}- "
            f"-n space --context {KindConfig().context}",
            message,
        )


class KindLateProbeReap(unittest.TestCase):
    """The admin-authenticated reap a detached late cleanup uses."""

    def _harness(self, answers: list[ConsoleApiResponse]) -> tuple[KindHarness, list[tuple[str, str, str | None]]]:
        harness = KindHarness(cast(Any, _RecordingExecutor()), KindConfig())
        calls: list[tuple[str, str, str | None]] = []

        def console_api(
            method: str, path: str, body: object = None, *, idempotency_key: str | None = None, **_kw: object
        ) -> ConsoleApiResponse:
            calls.append((method, path, idempotency_key))
            return answers.pop(0)

        harness.console_api = console_api  # type: ignore[method-assign]
        return harness, calls

    def test_matching_pages_the_admin_list_and_deletes_only_live_rows_holding_every_pair(self) -> None:
        run = {"space_smoke_run": "abc", "space_smoke_role": "rate-limiter-probe"}
        first = "11111111-1111-1111-1111-111111111111"
        second = "22222222-2222-2222-2222-222222222222"
        page_one = {
            "sandboxes": [
                {"id": first, "status": "running", "metadata": {**run, "extra": "kept"}},
                # Another run, a partial match, and an already-deleted row.
                {"id": "33333333-3333-3333-3333-333333333333", "status": "running",
                 "metadata": {**run, "space_smoke_run": "other"}},
                {"id": "44444444-4444-4444-4444-444444444444", "status": "running",
                 "metadata": {"space_smoke_run": "abc"}},
                {"id": "55555555-5555-5555-5555-555555555555", "status": "deleted", "metadata": run},
            ],
            "next_cursor": "page-2",
        }
        page_two = {"sandboxes": [{"id": second, "status": "paused", "metadata": run}], "next_cursor": None}
        harness, calls = self._harness([
            ConsoleApiResponse(200, page_one),
            ConsoleApiResponse(200, page_two),
            ConsoleApiResponse(202, {}),
            ConsoleApiResponse(404, {}),
        ])
        # The first delete was queued (pending); the second sandbox was gone.
        self.assertEqual(harness.admin_force_delete_matching(run), [first])
        self.assertEqual([c[0] for c in calls], ["GET", "GET", "POST", "POST"])
        self.assertIn("cursor=&", calls[0][1])
        self.assertIn("cursor=page-2", calls[1][1])
        self.assertEqual(
            [c[1] for c in calls[2:]],
            [f"/admin/api/v1/sandboxes/{first}:force-delete", f"/admin/api/v1/sandboxes/{second}:force-delete"],
        )
        # Each delete carries its own idempotency key.
        self.assertTrue(all(c[2] for c in calls[2:]))
        self.assertNotEqual(calls[2][2], calls[3][2])

    def test_one_failing_delete_does_not_keep_the_other_matches_alive(self) -> None:
        run = {"space_smoke_run": "abc"}
        failing = "11111111-1111-1111-1111-111111111111"
        other = "22222222-2222-2222-2222-222222222222"
        page = {
            "sandboxes": [
                {"id": failing, "status": "running", "metadata": run},
                {"id": other, "status": "running", "metadata": run},
            ],
            "next_cursor": None,
        }
        harness, calls = self._harness([
            ConsoleApiResponse(200, page),
            ConsoleApiResponse(503, {}),
            ConsoleApiResponse(202, {}),
        ])
        with self.assertRaisesRegex(SmokeConfigurationError, "force-delete answered HTTP 503") as raised:
            harness.admin_force_delete_matching(run)
        # The failed one (outcome unknown) and the queued one both stay pending.
        self.assertEqual(cast(AdminReapIncomplete, raised.exception).pending, [failing, other])
        # The second match was attempted after the first one's failure.
        self.assertEqual(
            [c[1] for c in calls[1:]],
            [f"/admin/api/v1/sandboxes/{failing}:force-delete", f"/admin/api/v1/sandboxes/{other}:force-delete"],
        )

    def test_a_whole_window_scan_reaps_a_create_that_commits_after_the_first_match(self) -> None:
        # Two creates of unknown outcome under one run's metadata: A is visible
        # on the first scan, B only on the third. Stopping at the first match
        # leaves B; scanning the whole window reaps both.
        from space_smoke.adapters import EcwConfig, EcwSdk

        def scans() -> list[list[str]]:
            return [["sbx_a"], [], ["sbx_b"], [], []]

        def run(scan_whole_window: bool) -> list[str]:
            pending = scans()
            killed: list[str] = []

            class _Paginator:
                def __init__(self, ids: list[str]) -> None:
                    self._ids = ids
                    self.has_next = True

                def next_items(self) -> list[SimpleNamespace]:
                    self.has_next = False
                    return [SimpleNamespace(sandbox_id=i) for i in self._ids]

            fake_e2b = SimpleNamespace(
                Sandbox=SimpleNamespace(
                    list=lambda **_kw: _Paginator(pending.pop(0)),
                    kill=None,
                ),
                SandboxQuery=lambda **kw: kw,
            )
            # The `import_module` patch goes last: it is process-wide, and
            # `mock.patch` resolves the other targets through it.
            with (
                mock.patch(
                    "space_smoke.adapters._kill_with_sdk",
                    side_effect=lambda _kill, sandbox_id, **_kw: killed.append(sandbox_id),
                ),
                mock.patch("space_smoke.adapters.time.sleep"),
                mock.patch("space_smoke.adapters.importlib.import_module", return_value=fake_e2b),
            ):
                EcwSdk(EcwConfig(api_key="key")).kill_matching(
                    {"space_smoke_run": "abc"},
                    attempts=5,
                    interval_seconds=0,
                    scan_whole_window=scan_whole_window,
                )
            return killed

        self.assertEqual(run(False), ["sbx_a"])
        self.assertEqual(run(True), ["sbx_a", "sbx_b"])

    def test_matches_from_pages_read_before_a_failing_page_are_still_deleted(self) -> None:
        run = {"space_smoke_run": "abc"}
        found = "11111111-1111-1111-1111-111111111111"
        harness, calls = self._harness([
            ConsoleApiResponse(
                200,
                {"sandboxes": [{"id": found, "status": "running", "metadata": run}], "next_cursor": "p2"},
            ),
            ConsoleApiResponse(503, {}),
            ConsoleApiResponse(202, {}),
        ])
        with self.assertRaisesRegex(
            SmokeConfigurationError, r"scan incomplete after 1 match\(es\): admin sandbox list answered HTTP 503"
        ) as raised:
            harness.admin_force_delete_matching(run)
        self.assertEqual(calls[2][:2], ("POST", f"/admin/api/v1/sandboxes/{found}:force-delete"))
        # The delete it queued before raising is carried for the reap to poll.
        self.assertIsInstance(raised.exception, AdminReapIncomplete)
        self.assertEqual(cast(AdminReapIncomplete, raised.exception).pending, [found])

    def test_a_whole_window_scan_keeps_killing_past_a_failing_delete(self) -> None:
        # A and B match every scan. A's DELETE fails: B is still killed, A
        # stays eligible for the next scan, and only a failure no later DELETE
        # cleared is raised once the window ends.
        from space_smoke.adapters import EcwConfig, EcwSdk

        def run(a_fails: set[int]) -> tuple[list[tuple[str, int]], BaseException | None]:
            calls: list[tuple[str, int]] = []
            scan = [0]

            class _Paginator:
                has_next = True

                def next_items(self) -> list[SimpleNamespace]:
                    self.has_next = False
                    scan[0] += 1
                    return [SimpleNamespace(sandbox_id="sbx_a"), SimpleNamespace(sandbox_id="sbx_b")]

            def kill(_kill: object, sandbox_id: str, **_kw: object) -> None:
                calls.append((sandbox_id, scan[0]))
                if sandbox_id == "sbx_a" and scan[0] in a_fails:
                    raise RuntimeError("DELETE answered HTTP 503")

            fake_e2b = SimpleNamespace(
                Sandbox=SimpleNamespace(list=lambda **_kw: _Paginator(), kill=None),
                SandboxQuery=lambda **kw: kw,
            )
            raised: BaseException | None = None
            with (
                mock.patch("space_smoke.adapters._kill_with_sdk", side_effect=kill),
                mock.patch("space_smoke.adapters.time.sleep"),
                mock.patch("space_smoke.adapters.importlib.import_module", return_value=fake_e2b),
            ):
                try:
                    EcwSdk(EcwConfig(api_key="key")).kill_matching(
                        {"space_smoke_run": "abc"},
                        attempts=3,
                        interval_seconds=0,
                        scan_whole_window=True,
                    )
                except RuntimeError as error:
                    raised = error
            return calls, raised

        calls, raised = run({1, 2, 3})
        self.assertIn(("sbx_b", 1), calls)
        self.assertEqual([c for c in calls if c[0] == "sbx_a"], [("sbx_a", 1), ("sbx_a", 2), ("sbx_a", 3)])
        self.assertIsNotNone(raised)
        self.assertIn("503", str(raised))
        # Failing once, then deleted on a later scan: nothing is left to report.
        calls, raised = run({1})
        self.assertIsNone(raised)

    def test_a_rescan_sends_no_second_force_delete_while_the_first_is_pending(self) -> None:
        # The admin route answers 202 once a delete is queued and queues another
        # for each new key. One history per outcome, each reachable: a delete
        # that runs and succeeds sends one POST and then answers done; a running
        # delete that fails is replaced by a POST under a fresh key.
        sid = "11111111-1111-1111-1111-111111111111"
        harness, calls = self._harness([
            ConsoleApiResponse(202, {"operation_id": "op_1", "sandbox_id": f"sbx_{sid}"}),
            ConsoleApiResponse(200, {"id": "op_1", "state": "running"}),
            ConsoleApiResponse(200, {"id": "op_1", "state": "succeeded"}),
            ConsoleApiResponse(200, {"id": "op_1", "state": "succeeded"}),
        ])
        self.assertEqual(
            [harness.admin_force_delete_sandbox(sid) for _ in range(4)], [False, False, True, True]
        )
        self.assertEqual(
            [(method, path) for method, path, _ in calls],
            [("POST", f"/admin/api/v1/sandboxes/{sid}:force-delete")]
            + [("GET", "/admin/api/v1/operations/op_1")] * 3,
        )

        harness, calls = self._harness([
            ConsoleApiResponse(202, {"operation_id": "op_1", "sandbox_id": f"sbx_{sid}"}),
            ConsoleApiResponse(200, {"id": "op_1", "state": "running"}),
            ConsoleApiResponse(200, {"id": "op_1", "state": "failed"}),
            ConsoleApiResponse(202, {"operation_id": "op_2", "sandbox_id": f"sbx_{sid}"}),
            ConsoleApiResponse(200, {"id": "op_2", "state": "succeeded"}),
        ])
        self.assertEqual(
            [harness.admin_force_delete_sandbox(sid) for _ in range(4)], [False, False, False, True]
        )
        posts = [c for c in calls if c[0] == "POST"]
        self.assertEqual(len(posts), 2)
        self.assertNotEqual(posts[0][2], posts[1][2])
        self.assertEqual(calls[-1][:2], ("GET", "/admin/api/v1/operations/op_2"))

    def test_one_reserved_key_covers_a_lost_reply_and_every_caller(self) -> None:
        # The key is reserved before the first POST: a retry after a lost reply,
        # or a second caller that finds no operation stored yet, sends the same
        # key, which the route answers with the delete already queued under it.
        sid = "11111111-1111-1111-1111-111111111111"
        harness = KindHarness(cast(Any, _RecordingExecutor()), KindConfig())
        keys: list[str | None] = []
        answers: list[object] = [
            SmokeConfigurationError("console API POST failed"),
            ConsoleApiResponse(202, {"operation_id": "op_1", "sandbox_id": f"sbx_{sid}"}),
        ]

        def console_api(
            method: str, path: str, body: object = None, *, idempotency_key: str | None = None, **_kw: object
        ) -> ConsoleApiResponse:
            keys.append(idempotency_key)
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return cast(ConsoleApiResponse, answer)

        harness.console_api = console_api  # type: ignore[method-assign]
        with self.assertRaises(SmokeConfigurationError):
            harness.admin_force_delete_sandbox(sid)
        self.assertFalse(harness.admin_force_delete_sandbox(sid))
        self.assertEqual(len(keys), 2)
        self.assertIsNotNone(keys[0])
        self.assertEqual(keys[0], keys[1])

    def test_a_late_reap_polls_queued_deletes_until_they_are_done(self) -> None:
        # A 202 only queues the delete: the reap by id keeps polling until it's
        # confirmed done, and a delete a metadata scan queued on its last round
        # is polled after the scan window too.
        polls: list[str] = []

        class _Queued:
            def __init__(self, done_after: int) -> None:
                self._done_after = done_after
                self.scans = 0

            def admin_force_delete_sandbox(self, sandbox_id: str) -> bool:
                polls.append(sandbox_id)
                return polls.count(sandbox_id) >= self._done_after

            def admin_force_delete_matching(self, _metadata: Mapping[str, str]) -> list[str]:
                self.scans += 1
                return ["sbx_last"] if self.scans == 10 else []

        by_id = _Queued(done_after=3)
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, by_id), cast(Any, _Probe()), ResourceScope()
        )
        assertions._reap_late_create("sbx_kept", uncertain=False)
        self.assertEqual(polls, ["sbx_kept"] * 3)

        polls.clear()
        by_metadata = _Queued(done_after=2)
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, by_metadata), cast(Any, _Probe()), ResourceScope()
        )
        assertions._reap_late_create(None, uncertain=True)
        self.assertEqual(by_metadata.scans, 10)
        # Queued on the tenth scan, then polled by id until done.
        self.assertEqual(polls, ["sbx_last"] * 2)

    def test_a_failed_delete_whose_sandbox_leaves_the_list_is_retried_by_id(self) -> None:
        # A's DELETE commits but its reply is lost, so the next scans don't list
        # A. It's retried by id, which counts a gone sandbox as done, instead of
        # staying a failure no later DELETE could clear.
        from space_smoke.adapters import EcwConfig, EcwSdk

        scans = [["sbx_a", "sbx_b"], ["sbx_b"], []]
        calls: list[str] = []

        class _Paginator:
            def __init__(self, ids: list[str]) -> None:
                self._ids = ids
                self.has_next = True

            def next_items(self) -> list[SimpleNamespace]:
                self.has_next = False
                return [SimpleNamespace(sandbox_id=i) for i in self._ids]

        def kill(_kill: object, sandbox_id: str, **_kw: object) -> None:
            calls.append(sandbox_id)
            if sandbox_id == "sbx_a" and calls.count("sbx_a") == 1:
                raise RuntimeError("connection reset after the DELETE was sent")

        fake_e2b = SimpleNamespace(
            Sandbox=SimpleNamespace(list=lambda **_kw: _Paginator(scans.pop(0)), kill=None),
            SandboxQuery=lambda **kw: kw,
        )
        with (
            mock.patch("space_smoke.adapters._kill_with_sdk", side_effect=kill),
            mock.patch("space_smoke.adapters.time.sleep"),
            mock.patch("space_smoke.adapters.importlib.import_module", return_value=fake_e2b),
        ):
            EcwSdk(EcwConfig(api_key="key")).kill_matching(
                {"space_smoke_run": "abc"}, attempts=3, interval_seconds=0, scan_whole_window=True
            )
        # Scan 1 lists both; scan 2 lists only B, and A is retried by id there.
        self.assertEqual(calls, ["sbx_a", "sbx_b", "sbx_b", "sbx_a"])

    def test_a_scan_that_raises_still_hands_its_queued_deletes_to_the_poll(self) -> None:
        # The last metadata scan queues a delete, then a later page fails: the
        # id rides on the error and is polled after the window until done.
        polls: list[str] = []

        class _LastScanFails:
            scans = 0

            def admin_force_delete_sandbox(self, sandbox_id: str) -> bool:
                polls.append(sandbox_id)
                return polls.count(sandbox_id) >= 2

            def admin_force_delete_matching(self, _metadata: Mapping[str, str]) -> list[str]:
                self.scans += 1
                if self.scans == 10:
                    raise AdminReapIncomplete("run-metadata scan incomplete after 1 match(es)", ["sbx_q"])
                return []

        control = _LastScanFails()
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, control), cast(Any, _Probe()), ResourceScope()
        )
        assertions._reap_late_create(None, uncertain=True)
        self.assertEqual(control.scans, 10)
        self.assertEqual(polls, ["sbx_q"] * 2)

    def test_handed_off_ids_all_get_their_first_delete_before_any_retry(self) -> None:
        # One id's delete stays pending for the whole window. The others' first
        # DELETEs still go out in the first round, not after its window, so a
        # runner that exits mid-window has already sent them.
        calls: list[str] = []

        class _FirstStalls:
            def admin_force_delete_sandbox(self, sandbox_id: str) -> bool:
                calls.append(sandbox_id)
                return sandbox_id != "sbx_stalled"

            def admin_force_delete_matching(self, _metadata: Mapping[str, str]) -> list[str]:
                calls.append("<scan>")
                return []

        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, _FirstStalls()), cast(Any, _Probe()), ResourceScope()
        )
        assertions._reap_late(["sbx_stalled", "sbx_b", "sbx_c"], uncertain=True)
        self.assertEqual(calls[:4], ["<scan>", "sbx_stalled", "sbx_b", "sbx_c"])
        # After the first round only the stalled id is polled, beside the scans.
        self.assertEqual(set(calls[4:]), {"<scan>", "sbx_stalled"})

        # Through the handoff itself: three admitted ids it hadn't deleted go
        # to one reap, not one window each.
        calls.clear()
        handed = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, _FirstStalls()), cast(Any, _Probe()), ResourceScope()
        )
        cast(Any, handed)._admitted.extend(["sbx_stalled", "sbx_b", "sbx_c"])
        reaps: list[threading.Thread] = []
        real_thread = threading.Thread

        def held(*args: Any, **kwargs: Any) -> threading.Thread:
            thread = real_thread(*args, **kwargs)
            reaps.append(thread)
            return thread

        with mock.patch("space_smoke.rate_limiters.Thread", side_effect=held):
            handed._handoff_late_arrivals(set(), reconciled=True)
        for thread in reaps:
            thread.join(timeout=30)
        self.assertEqual(calls[:3], ["sbx_stalled", "sbx_b", "sbx_c"])
        self.assertEqual(set(calls[3:]), {"sbx_stalled"})

    def test_a_refused_delete_or_list_raises_and_the_list_is_bounded(self) -> None:
        harness, _ = self._harness([ConsoleApiResponse(503, {})])
        with self.assertRaisesRegex(SmokeConfigurationError, "force-delete answered HTTP 503"):
            harness.admin_force_delete_sandbox("sbx_11111111-1111-1111-1111-111111111111")
        harness, _ = self._harness([ConsoleApiResponse(401, {})])
        with self.assertRaisesRegex(SmokeConfigurationError, "list answered HTTP 401"):
            harness.admin_force_delete_matching({"space_smoke_run": "abc"})
        endless = [ConsoleApiResponse(200, {"sandboxes": [], "next_cursor": "more"}) for _ in range(3)]
        harness, _ = self._harness(endless)
        with self.assertRaisesRegex(SmokeConfigurationError, "ran past 3 pages"):
            harness.admin_force_delete_matching({"space_smoke_run": "abc"}, max_pages=3)


class KindLimiterCleanupBounds(unittest.TestCase):
    def test_restore_bounds_every_kubectl_call_and_keeps_record_when_rollout_stalls(self) -> None:
        ownership = _LeaseOwnership(
            name="space-smoke-public-api-x",
            uid="lease-uid",
            holder="me",
            subject="public API mutation",
        )
        executor = _RecordingExecutor(
            # `_renew_lease`: get lease, patch lease
            CommandResult(("kubectl",), 0, _lease_json(), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
            # `_apply_public_api_env`: find deployment, find container
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            # `_fast_rollout` enter: `_assert_lease_owned`, read deploy, patch fast pace
            CommandResult(("kubectl",), 0, _lease_json(), ""),
            CommandResult(("kubectl",), 0, _deployment_json(fast=False), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
            # `kubectl set env`
            CommandResult(("kubectl",), 0, "deployment.apps/space-api env updated", ""),
            # `wait_public_api_rollout`: `kubectl rollout status` times out
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 124, "", "timed out waiting for rollout"),
            # `_fast_rollout` exit: read deploy, patch standing pace back
            CommandResult(("kubectl",), 0, _deployment_json(fast=True), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
        )
        harness = KindHarness(cast(Any, executor), KindConfig())
        harness._public_api_lease_ownership = ownership

        with self.assertRaisesRegex(RuntimeError, "await rollout of space-api"):
            harness.restore_public_api_env({LIMITER_ENV_KEYS[0]: None}, rollout_timeout=30)

        # Every subprocess call carries a finite timeout, and the rollout status
        # check uses the 30s cleanup budget rather than the 240s happy-path wait.
        self.assertTrue(all(bound is not None and bound <= 37.5 for _, bound in executor.calls))
        rollout_call = next(cmd for cmd, _ in executor.calls if "rollout" in cmd)
        self.assertIn("--timeout=30s", rollout_call)
        # Because the rollout stalled, the final `_renew_lease` clearing
        # `LIMITER_ENV_ANNOTATION` never ran, keeping the orphan-repair record.
        lease_patches = [cmd[-1] for cmd, _ in executor.calls if cmd[5:7] == ("patch", "lease")]
        self.assertEqual(len(lease_patches), 1)
        self.assertNotIn("remove", lease_patches[0])

        # The strict path the cleanup backstop takes: `restore_public_api_env`
        # must carry `shared_deadline` through `_apply_public_api_env` into the
        # rollout wait, so a Deployment lookup that spends 12s of the 30s leaves
        # the rollout process exactly the 18s remainder, with no grace.
        clock = [500.0]
        strict = _RecordingExecutor(
            CommandResult(("kubectl",), 0, _lease_json(), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 0, _lease_json(), ""),
            CommandResult(("kubectl",), 0, _deployment_json(fast=False), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
            CommandResult(("kubectl",), 0, "deployment.apps/space-api env updated", ""),
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 124, "", "timed out waiting for rollout"),
            CommandResult(("kubectl",), 0, _deployment_json(fast=True), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
        )

        class _SlowLookup:
            def run(self, command: list[str], *, timeout: float | None = None, **kw: object) -> CommandResult:
                result = strict.run(command, timeout=timeout, **kw)
                if "-l" in command and "deploy" in command:
                    clock[0] += 12.0
                return result

        strict_harness = KindHarness(cast(Any, _SlowLookup()), KindConfig())
        strict_harness._public_api_lease_ownership = ownership
        with (
            mock.patch("space_smoke.adapters.time.monotonic", side_effect=lambda: clock[0]),
            self.assertRaisesRegex(RuntimeError, "await rollout of space-api"),
        ):
            strict_harness.restore_public_api_env(
                {LIMITER_ENV_KEYS[0]: None}, rollout_timeout=30, shared_deadline=True
            )
        rollout_cmd, rollout_bound = next((c, b) for c, b in strict.calls if "rollout" in c)
        self.assertIn("--timeout=18s", rollout_cmd)
        assert rollout_bound is not None
        self.assertAlmostEqual(rollout_bound, 18.0)
        strict_patches = [cmd[-1] for cmd, _ in strict.calls if cmd[5:7] == ("patch", "lease")]
        self.assertEqual(len(strict_patches), 1)
        self.assertNotIn("remove", strict_patches[0])

    def test_rollout_waits_stay_inside_a_shared_deadline_and_never_pass_zero_to_kubectl(self) -> None:
        clock = [100.0]
        lookup_step = [0.05]
        rollout_step = [0.10]
        base_executor = _RecordingExecutor(
            # 1) Default `shared_deadline=False`: lookup + rollout (235s) still leaves full 240s drain
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 0, "rolled out", ""),
            # 2) `shared_deadline=True` with subsecond remaining budget: the
            # flag rounds `0.15s` up to `1s`, the process bound is `0.15s`
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 0, "rolled out", ""),
            # 3) `wait_redis_rollout` subsecond budget, separate (`1s` + `5s` grace)
            # and then shared (`0.2s` exactly)
            CommandResult(("kubectl",), 0, "rolled out", ""),
            CommandResult(("kubectl",), 0, "rolled out", ""),
            # 4) `shared_deadline=True` where Deployment lookup consumes the full `0.2s` budget
            CommandResult(("kubectl",), 0, "space-api\n", ""),
        )

        class _AdvancingExecutor:
            def run(self, command: list[str], *, timeout: float | None = None, **kw: object) -> CommandResult:
                if "get" in command and "deploy" in command:
                    clock[0] += lookup_step[0]
                elif "rollout" in command:
                    clock[0] += rollout_step[0]
                return base_executor.run(command, timeout=timeout, **kw)

        harness = KindHarness(cast(Any, _AdvancingExecutor()), KindConfig())
        drained_calls: list[tuple[float, float | None]] = []
        with (
            mock.patch("space_smoke.adapters.time.monotonic", side_effect=lambda: clock[0]),
            mock.patch.object(
                harness,
                "_wait_deployment_pods_drained",
                side_effect=lambda _name, *, timeout, deadline=None: drained_calls.append(
                    (timeout, deadline)
                ),
            ),
        ):
            lookup_step[0] = 2.0
            rollout_step[0] = 235.0
            harness.wait_public_api_rollout(timeout=240)

            lookup_step[0] = 0.05
            rollout_step[0] = 0.10
            shared_start = clock[0]
            harness.wait_public_api_rollout(timeout=0.2, shared_deadline=True)
            harness.wait_redis_rollout(timeout=0.2)
            harness.wait_redis_rollout(timeout=0.2, shared_deadline=True)
            with self.assertRaisesRegex(RuntimeError, "Redis rollout budget"):
                harness.wait_redis_rollout(timeout=0.0, shared_deadline=True)

            lookup_step[0] = 0.20
            with self.assertRaisesRegex(RuntimeError, "exhausted its 0.2s budget"):
                harness.wait_public_api_rollout(timeout=0.2, shared_deadline=True)

        # Default callers retain their separate drain window and kubectl's
        # grace past its own `--timeout`; opt-in `shared_deadline=True` passes
        # the absolute deadline into the drain, bounds every process by what's
        # left of it (rounding only kubectl's whole-second flag up to `1s`),
        # and starts no rollout wait once Deployment lookup exhausts the budget.
        self.assertEqual(drained_calls[0], (240, None))
        self.assertEqual(drained_calls[1][0], 0.2)
        assert drained_calls[1][1] is not None
        self.assertAlmostEqual(drained_calls[1][1], shared_start + 0.2)
        rollout_calls = [(cmd, bound) for cmd, bound in base_executor.calls if "rollout" in cmd]
        self.assertEqual(len(rollout_calls), 4)
        self.assertIn("--timeout=240s", rollout_calls[0][0])
        for cmd, _ in rollout_calls[1:]:
            self.assertIn("--timeout=1s", cmd)
        shared_lookup = next(
            bound for cmd, bound in base_executor.calls[2:] if "-l" in cmd
        )
        assert shared_lookup is not None
        self.assertAlmostEqual(shared_lookup, 0.2)
        assert rollout_calls[1][1] is not None
        self.assertAlmostEqual(rollout_calls[1][1], 0.15)
        self.assertEqual(rollout_calls[2][1], 6.0)
        self.assertEqual(rollout_calls[3][1], 0.2)

        # Real `_wait_deployment_pods_drained` under `shared_deadline=True`:
        # starts no read when rollout used the whole budget, and refuses to
        # return success once a pod read crosses the shared deadline.
        drain_clock = [500.0]
        pods_json = json.dumps(
            {
                "items": [
                    {"metadata": {"name": "space-api-1"}},
                    {"metadata": {"name": "space-api-2"}},
                ]
            }
        )
        drain_base = _RecordingExecutor(
            # Case A: lookup (0.5s) + rollout (1.5s) exhausts 2.0s -> drain starts 0 reads
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 0, "rolled out", ""),
            # Case B: lookup (0.2s) + rollout (1.0s) + get deploy (0.4s) + get pods (0.5s) = 2.1s > 2.0s
            CommandResult(("kubectl",), 0, "space-api\n", ""),
            CommandResult(("kubectl",), 0, "rolled out", ""),
            CommandResult(("kubectl",), 0, _deployment_json(fast=False), ""),
            CommandResult(("kubectl",), 0, pods_json, ""),
        )
        steps = iter([0.5, 1.5, 0.2, 1.0, 0.4, 0.5])

        class _StepExecutor:
            def run(self, command: list[str], *, timeout: float | None = None, **kw: object) -> CommandResult:
                res = drain_base.run(command, timeout=timeout, **kw)
                drain_clock[0] += next(steps)
                return res

        drain_harness = KindHarness(cast(Any, _StepExecutor()), KindConfig())
        with mock.patch("space_smoke.adapters.time.monotonic", side_effect=lambda: drain_clock[0]):
            with self.assertRaisesRegex(RuntimeError, "pods did not drain within 2s"):
                drain_harness.wait_public_api_rollout(timeout=2.0, shared_deadline=True)
            self.assertEqual(len(drain_base.calls), 2)

            with self.assertRaisesRegex(RuntimeError, "pods did not drain to 2 within 2s"):
                drain_harness.wait_public_api_rollout(timeout=2.0, shared_deadline=True)
            self.assertEqual(len(drain_base.calls), 6)
            # Drain reads are bounded by what's left (`0.8s`, then `0.4s`),
            # not `remaining + 5s` and not floored at a second.
            self.assertAlmostEqual(cast(float, drain_base.calls[4][1]), 0.8)
            self.assertAlmostEqual(cast(float, drain_base.calls[5][1]), 0.4)

    def test_a_strict_redis_wait_bounds_its_record_clear_by_the_same_deadline(self) -> None:
        # Under `shared_deadline=True` the rollout gets the budget less the
        # record clear's floor, and the Lease read and patch that clear the
        # record get whatever the rollout left. A clear that runs out fails
        # before its patch and keeps the record for the orphan repair. The fake
        # kubectl times a rollout out when it outlasts its process bound.
        ownership = _LeaseOwnership(
            name="space-smoke-public-api-x",
            uid="lease-uid",
            holder="me",
            subject="public API mutation",
        )
        clock = [100.0]

        def clear_after(
            rollout_seconds: float, lease_read_seconds: float
        ) -> tuple[KindHarness, _RecordingExecutor]:
            recording = _RecordingExecutor(
                CommandResult(("kubectl",), 0, "rolled out", ""),
                CommandResult(("kubectl",), 0, _lease_json(recorded_env=None, recorded_redis="1"), ""),
                CommandResult(("kubectl",), 0, "patched", ""),
            )

            class _Advancing:
                def run(self, command: list[str], *, timeout: float | None = None, **kw: object) -> CommandResult:
                    result = recording.run(command, timeout=timeout, **kw)
                    if "rollout" in command:
                        assert timeout is not None
                        clock[0] += min(rollout_seconds, timeout)
                        if rollout_seconds > timeout:
                            return CommandResult(("kubectl",), 124, "", "timed out")
                    elif "get" in command:
                        clock[0] += lease_read_seconds
                    return result

            harness = KindHarness(cast(Any, _Advancing()), KindConfig())
            harness._public_api_lease_ownership = ownership
            harness._redis_fault_clear_ready = True
            return harness, recording

        def assert_bounds(recording: _RecordingExecutor, expected: tuple[float, ...]) -> None:
            bounds = [bound for _, bound in recording.calls]
            self.assertEqual(len(bounds), len(expected))
            for bound, want in zip(bounds, expected, strict=True):
                assert bound is not None
                self.assertAlmostEqual(bound, want)

        self.assertEqual(REDIS_RECORD_CLEAR_SECONDS, 2.0)
        with mock.patch("space_smoke.adapters.time.monotonic", side_effect=lambda: clock[0]):
            # Loose end: 20s left, a 17s rollout and a fast clear both fit. The
            # rollout's bound is 18s; the clear gets the 3s it left, the read's
            # 0.05s leaving the patch 2.95s.
            harness, recording = clear_after(17.0, 0.05)
            harness.wait_redis_rollout(timeout=20, shared_deadline=True)
            assert_bounds(recording, (18.0, 3.0, 2.95))
            self.assertIn("remove", recording.calls[2][0][-1])
            self.assertFalse(harness._redis_fault_clear_ready)

            # Tight end: 2.5s left gives the rollout 0.5s (kubectl's flag still
            # says 1s) and the clear its 2s floor plus what the rollout left.
            harness, recording = clear_after(0.4, 0.05)
            harness.wait_redis_rollout(timeout=2.5, shared_deadline=True)
            self.assertIn("--timeout=1s", recording.calls[0][0])
            assert_bounds(recording, (0.5, 2.1, 2.05))
            self.assertFalse(harness._redis_fault_clear_ready)

            # A rollout that outlasts its bound fails without touching the record.
            harness, recording = clear_after(19.0, 0.05)
            with self.assertRaisesRegex(RuntimeError, "await the Redis rollout"):
                harness.wait_redis_rollout(timeout=20, shared_deadline=True)
            self.assertEqual(len(recording.calls), 1)
            self.assertTrue(harness._redis_fault_clear_ready)

            # A read that takes the rest of the budget starts no patch.
            harness, recording = clear_after(14.0, 7.0)
            with self.assertRaisesRegex(RuntimeError, "ran out of its budget"):
                harness.wait_redis_rollout(timeout=20, shared_deadline=True)
            self.assertEqual(len(recording.calls), 2)
            self.assertTrue(harness._redis_fault_clear_ready)

            # A budget no larger than the clear's floor starts no rollout wait.
            harness, recording = clear_after(0.0, 0.0)
            with self.assertRaisesRegex(RuntimeError, "record stays for the repair"):
                harness.wait_redis_rollout(timeout=REDIS_RECORD_CLEAR_SECONDS, shared_deadline=True)
            self.assertEqual(recording.calls, [])

    def test_redis_record_clears_only_after_rollout_succeeds_and_timeout_skips_pace_restore(self) -> None:
        ownership = _LeaseOwnership(
            name="space-smoke-public-api-x",
            uid="lease-uid",
            holder="me",
            subject="public API mutation",
        )
        executor = _RecordingExecutor(
            # `wait_redis_rollout` before `scale_redis(1)` must NOT clear `REDIS_REPLICAS_ANNOTATION`
            CommandResult(("kubectl",), 0, "rolled out at zero", ""),
            # `scale_redis(1)`: read replicas, `_renew_lease` (get+patch), `kubectl scale`
            CommandResult(("kubectl",), 0, "0\n", ""),
            CommandResult(("kubectl",), 0, _lease_json(recorded_env=None, recorded_redis="1"), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
            CommandResult(("kubectl",), 0, "scaled", ""),
            # First `wait_redis_rollout`: times out -> must NOT clear `REDIS_REPLICAS_ANNOTATION`
            CommandResult(("kubectl",), 124, "", "timed out"),
            # Second `wait_redis_rollout`: succeeds -> `_renew_lease` clears `REDIS_REPLICAS_ANNOTATION`
            CommandResult(("kubectl",), 0, "rolled out", ""),
            CommandResult(("kubectl",), 0, _lease_json(recorded_env=None, recorded_redis="1"), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
            # `_fast_rollout` interrupted by `ScenarioTimedOut`: enter only, no exit pace patch
            CommandResult(("kubectl",), 0, _lease_json(), ""),
            CommandResult(("kubectl",), 0, _deployment_json(fast=False), ""),
            CommandResult(("kubectl",), 0, "patched", ""),
        )
        harness = KindHarness(cast(Any, executor), KindConfig())
        harness._public_api_lease_ownership = ownership

        harness.wait_redis_rollout(timeout=15)
        self.assertEqual(
            [cmd for cmd, _ in executor.calls if cmd[5:7] == ("patch", "lease")], []
        )

        harness.scale_redis(1)
        with self.assertRaisesRegex(RuntimeError, "await the Redis rollout"):
            harness.wait_redis_rollout(timeout=15)
        lease_patches = [cmd[-1] for cmd, _ in executor.calls if cmd[5:7] == ("patch", "lease")]
        self.assertEqual(len(lease_patches), 1)
        self.assertNotIn("remove", lease_patches[0])

        harness.wait_redis_rollout(timeout=15)
        lease_patches = [cmd[-1] for cmd, _ in executor.calls if cmd[5:7] == ("patch", "lease")]
        self.assertEqual(len(lease_patches), 2)
        self.assertIn("remove", lease_patches[1])

        with self.assertRaises(ScenarioTimedOut) as timed_out:
            with harness._fast_rollout("space-api", ownership):
                raise ScenarioTimedOut("cleanup timed out after 45s")
        self.assertEqual(executor._results, [])
        self.assertTrue(any("skipping rollout pace restore" in n for n in timed_out.exception.__notes__))


if __name__ == "__main__":
    unittest.main()
