"""Contracts of the rate-limiter scenario the bench cannot exercise.

A passing bench run shows both limiters rejecting and then admitting. It
cannot show that a 429 from *another* source (placement admission, a
malformed Retry-After) is refused as limiter evidence, that the env override
refuses keys outside its closed list or a `valueFrom` entry it could not
restore, or that a Retry-After without delta-seconds fails the verdict.
Those are pinned here.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import KindConfig, KindHarness
from space_smoke.capabilities import LimiterProbeOutcome
from space_smoke.rate_limiters import (
    PER_CLIENT_LIMIT_MESSAGE,
    SERVER_LIMIT_MESSAGE,
    RateLimiterAssertions,
    has_retry_after_seconds,
    is_limiter_rejection,
    status_histogram,
)
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope


def _outcome(status: int, message: str = "", retry_after: str | None = None) -> LimiterProbeOutcome:
    return LimiterProbeOutcome(status, retry_after, message, None, 0.0)


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

    def test_retry_after_must_be_delta_seconds(self) -> None:
        self.assertTrue(has_retry_after_seconds(_outcome(429, "", "7")))
        self.assertTrue(has_retry_after_seconds(_outcome(429, "", " 12 ")))
        self.assertFalse(has_retry_after_seconds(_outcome(429, "", None)))
        self.assertFalse(has_retry_after_seconds(_outcome(429, "", "")))
        self.assertFalse(has_retry_after_seconds(_outcome(429, "", "Wed, 21 Oct 2026 07:28:00 GMT")))


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id="sbx_x")
        self.records: dict[str, object] = {}

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)


class _Probe:
    def __init__(self, outcome: LimiterProbeOutcome) -> None:
        self.outcome = outcome
        self.kills = 0

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        return self.outcome

    def kill_matching(self, metadata) -> None:
        self.kills += 1
        raise RuntimeError("no sandbox became visible during run-metadata reconciliation")


class _RedisControl:
    def __init__(self) -> None:
        self.calls: list[str] = []

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

    def wait_redis_rollout(self, *, timeout: float) -> None:
        self.calls.append("rollout")


class PerClientVerdict(unittest.TestCase):
    def _assertions(self, probe: _Probe | None = None) -> RateLimiterAssertions:
        return RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, probe), ResourceScope()
        )

    def test_an_all_rejected_wave_does_not_make_the_sweep_fail(self) -> None:
        # kill_matching raises on zero matches; zero admits is a valid outcome.
        probe = _Probe(_outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} x", "1"))
        assertions = self._assertions(probe)
        assertions.create_once()
        assertions.sweep_admitted()
        self.assertEqual(probe.kills, 0)

    def test_an_admitted_create_is_swept(self) -> None:
        probe = _Probe(LimiterProbeOutcome(201, None, "", "sbx_a", 0.0))
        assertions = self._assertions(probe)
        assertions.create_once()
        with self.assertRaises(RuntimeError):
            assertions.sweep_admitted()
        self.assertEqual(probe.kills, 1)

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


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


class PublicApiEnvOverride(unittest.TestCase):
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
