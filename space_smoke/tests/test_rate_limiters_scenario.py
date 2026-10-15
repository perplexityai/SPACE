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
    ISOLATION_CONTROL_CLIENT_ID,
    ISOLATION_NOISY_CLIENT_ID,
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

    def test_fail_open_ignores_non_limiter_statuses_but_not_limiter_429s(self) -> None:
        assertions = self._assertions()
        assertions.expect_fail_open([_outcome(201), _outcome(429, "placement", "2"), _outcome(500)])
        with self.assertRaisesRegex(InvariantViolation, "fail open"):
            assertions.expect_fail_open([_outcome(429, f"{SERVER_LIMIT_MESSAGE} x", "2")])


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
        return [_outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} too many", "1")] * 17 + [_outcome(201)] * 3

    def test_a_successful_control_arm_passes(self) -> None:
        control = [LimiterProbeOutcome(200, None, "", None, 0.0)] * 3
        self._assertions().expect_control_isolated(self._saturated_noisy(), control)

    def test_an_all_404_control_arm_fails(self) -> None:
        control = [_outcome(404, "sandbox not found")] * 3
        with self.assertRaisesRegex(InvariantViolation, "control reconnects succeeded"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), control)

    def test_an_all_500_control_arm_fails(self) -> None:
        control = [_outcome(500, "internal error")] * 3
        with self.assertRaisesRegex(InvariantViolation, "control reconnects succeeded"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), control)

    def test_an_all_cluster_429_control_arm_fails(self) -> None:
        # The cluster limiter's own message: the per-client gate stayed quiet,
        # so every per-client check passes and only the success verdict bites.
        control = [_outcome(429, f"{SERVER_LIMIT_MESSAGE} shedding", "6")] * 3
        with self.assertRaisesRegex(InvariantViolation, "control reconnects succeeded"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), control)

    def test_a_partly_successful_control_arm_fails(self) -> None:
        control = [LimiterProbeOutcome(200, None, "", None, 0.0), _outcome(404, "gone"), _outcome(200)]
        with self.assertRaisesRegex(InvariantViolation, "only 2 of 3"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), control)

    def test_a_shared_bucket_fails_as_broken_isolation_not_as_failure(self) -> None:
        # Both clients on one bucket: the control arm is rejected by the noisy
        # storm. That has to name isolation, not an unusable target.
        control = [_outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} too many", "1")] * 3
        with self.assertRaisesRegex(InvariantViolation, "not on separate buckets"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), control)

    def test_an_idle_limiter_fails_even_with_a_clean_control_arm(self) -> None:
        # Nothing metered: the control arm's success says nothing about
        # isolation because no client was ever over cap.
        noisy = [LimiterProbeOutcome(200, None, "", None, 0.0)] * 20
        control = [LimiterProbeOutcome(200, None, "", None, 0.0)] * 3
        with self.assertRaisesRegex(InvariantViolation, "drew no 429"):
            self._assertions().expect_control_isolated(noisy, control)

    def test_an_unanswered_control_request_is_not_a_clean_one(self) -> None:
        control = [LimiterProbeOutcome(200, None, "", None, 0.0), _outcome(0, "connection refused"), _outcome(200)]
        with self.assertRaisesRegex(InvariantViolation, "no status at all"):
            self._assertions().expect_control_isolated(self._saturated_noisy(), control)

    def test_the_two_isolation_client_ids_are_distinct_and_not_the_scenario_id(self) -> None:
        # One id is one bucket: any collision would have the round loading and
        # measuring the same bucket from both arms.
        self.assertNotEqual(ISOLATION_NOISY_CLIENT_ID, ISOLATION_CONTROL_CLIENT_ID)
        self.assertNotIn("space-smoke", (ISOLATION_NOISY_CLIENT_ID, ISOLATION_CONTROL_CLIENT_ID))


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


class PublicApiEnvOverride(unittest.TestCase):
    def test_accepts_a_comma_separated_allowlist_and_still_refuses_the_rest(self) -> None:
        # The kind bench refused `a,b,c` for KNOWN_CLIENTS before the comma was
        # in the class, which is the one separator that env var's own grammar
        # requires. Widening it must not let anything else through.
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        for refused in (
            "space smoke",
            "space-smoke;rm -rf /",
            "$(whoami)",
            "`id`",
            "a\nb",
            "'quoted'",
            "x" * 65,
            "",
        ):
            with self.subTest(value=refused), self.assertRaisesRegex(ValueError, "refusing"):
                harness.set_public_api_env({"SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS": refused})
        # The accepted shape reaches the key check instead of the value check,
        # which this closed list of keys then passes; kubectl is never reached
        # because the scripted runner has no results queued.
        with self.assertRaises(IndexError):
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
