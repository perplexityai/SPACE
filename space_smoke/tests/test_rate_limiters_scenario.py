"""Contracts of the rate-limiter scenario the bench cannot exercise.

A passing bench run shows both limiters rejecting and then admitting. It
cannot show that a 429 from *another* source (placement admission, a
malformed Retry-After) is refused as limiter evidence, that the env override
refuses keys outside its closed list or a `valueFrom` entry it could not
restore, or that a Retry-After without delta-seconds fails the verdict.
Those are pinned here.
"""

from __future__ import annotations

import os
import threading
import unittest
from unittest import mock
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import (
    ACTIVATION_CLIENT_JSONPATH,
    LIMITER_ENV_VALUE_MAX,
    KindConfig,
    KindHarness,
)
from space_smoke.capabilities import EcwSandboxState, LimiterProbeOutcome
from space_smoke.rate_limiters import (
    CAPABILITIES_ENV,
    LIMITER_MESSAGES,
    LIVE_CAP_MESSAGE,
    PER_CLIENT_LIMIT_MESSAGE,
    SERVER_LIMIT_MESSAGE,
    RateLimiterAssertions,
    has_retry_after_seconds,
    is_limiter_rejection,
    status_histogram,
)
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    SmokeConfigurationError,
)


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

    def test_every_recognized_refusal_gets_its_own_histogram_key(self) -> None:
        # A refusal the histogram does not know records as a bare status: a
        # live-cap 429 would read like a placement 429, and a capacity 503 like
        # any other unavailability. The phases whose verdicts turn on *which*
        # limiter answered record that histogram as their evidence, so a
        # missing entry costs the evidence rather than the verdict -- which is
        # worse, because the run still passes.
        outcomes = [_outcome(status, f"{message} detail", "1") for status, message in LIMITER_MESSAGES]
        histogram = status_histogram(outcomes)
        self.assertEqual(len(histogram), len(LIMITER_MESSAGES))
        self.assertNotIn("429", histogram)
        self.assertNotIn("503", histogram)

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

    def test_the_default_configuration_must_admit_what_it_admitted_before(self) -> None:
        # The converse of the other verdicts: those prove the limiters reject
        # once configured, this proves an unconfigured cluster still admits.
        # Both limiter messages count, since either cap shipping on by
        # accident is the regression; a placement 429 or a 500 does not,
        # because neither says anything about the caps.
        assertions = self._assertions()
        assertions.expect_admitted_by_default(
            [_outcome(201), _outcome(429, "no eligible node has capacity", "5"), _outcome(500)]
        )
        for refused in (SERVER_LIMIT_MESSAGE, PER_CLIENT_LIMIT_MESSAGE):
            with self.subTest(message=refused):
                with self.assertRaisesRegex(InvariantViolation, "no caps configured"):
                    assertions.expect_admitted_by_default(
                        [_outcome(201), _outcome(429, f"{refused} x", "2")]
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
    def __init__(self, live_pods: int) -> None:
        self._live_pods = live_pods

    def live_client_sandbox_pods(self, client_id: str) -> list[str]:
        return [f"pod-{index}" for index in range(self._live_pods)]


class LiveCapPlateauVerdict(unittest.TestCase):
    """The plateau verdicts. A healthy bench produces none of the failures.

    On a working build the cap admits up to itself and refuses past it, so a
    cap that admits nothing, one that never refuses, one that overshoots, and
    a refusal with no `Retry-After` never occur there.
    """

    def _assertions(self, *, cap: int = 1, live_pods: int = 1) -> RateLimiterAssertions:
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()),
            cast(Any, _LiveCapControl(live_pods)),
            cast(Any, None),
            ResourceScope(),
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

    def test_answers_that_plateaued_while_the_cluster_did_not_fails(self) -> None:
        # The case the pod count exists for: the API refused past the cap, but
        # the sandboxes it already admitted outnumber it, so the cap guarded
        # the answers and not the cluster.
        with self.assertRaisesRegex(InvariantViolation, "the cluster did not"):
            self._assertions(live_pods=3).expect_live_cap_plateau(
                [_outcome(200), self._refusal()], [_outcome(200)]
            )

    def test_a_refused_control_reconnect_fails(self) -> None:
        for refusal in (self._refusal(), _outcome(429, f"{PER_CLIENT_LIMIT_MESSAGE} x", "2")):
            with self.subTest(message=refusal.message):
                with self.assertRaisesRegex(InvariantViolation, "already-running sandbox"):
                    self._assertions().expect_live_cap_plateau(
                        [_outcome(200), self._refusal()], [_outcome(200), refusal]
                    )

    def test_an_empty_arm_fails_rather_than_passing_by_vacuity(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "asked the cap nothing"):
            self._assertions().expect_live_cap_plateau([], [_outcome(200)])

    def test_a_plateau_passes_and_records_both_the_answers_and_the_pods(self) -> None:
        assertions = self._assertions()
        assertions.expect_live_cap_plateau([_outcome(200), self._refusal()], [_outcome(200)])
        records = cast(Any, assertions)._fixture.records
        self.assertEqual(records["live_cap_statuses"], {"200": 1, "429 Client live sandbox cap reached": 1})
        self.assertEqual(records["live_cap_live_pods"], 1)


class _AttributionControl:
    def __init__(self, labels: list[str], row: str, *, column: bool = True) -> None:
        self._labels = labels
        self._row = row
        self._column = column

    def activation_attribution_available(self) -> bool:
        return self._column

    def sandbox_pod_client_ids(self, sandbox_id: str) -> list[str]:
        return list(self._labels)

    def sandbox_activating_client(self, sandbox_id: str) -> str:
        return self._row


class AttributionCapabilityProbe(unittest.TestCase):
    """The column is a cross-check, never the probe.

    Migrations ship ahead of the code that reads them in this repository, so
    the pinned tree the scenario first ran against carried the
    `activating_client_id` column and none of the code that writes it. A probe
    that read the column alone would have called the capability present, run
    the phase, and failed it -- blaming a writer that did not exist yet. That
    is why this is a declaration, and why a declaration the schema contradicts
    is a verdict rather than a quiet skip.
    """

    def _assertions(self, *, column: bool) -> RateLimiterAssertions:
        return RateLimiterAssertions(
            cast(Any, _Fixture()),
            cast(Any, _AttributionControl(["x"], "x", column=column)),
            cast(Any, None),
            ResourceScope(),
        )

    def test_an_undeclared_bench_skips_however_the_schema_looks(self) -> None:
        assertions = self._assertions(column=True)
        with mock.patch.dict(os.environ, {CAPABILITIES_ENV: ""}, clear=False):
            self.assertFalse(assertions.attribution_available())

    def test_a_declared_bench_with_the_column_runs(self) -> None:
        assertions = self._assertions(column=True)
        with mock.patch.dict(
            os.environ, {CAPABILITIES_ENV: "client_attribution"}, clear=False
        ):
            self.assertTrue(assertions.attribution_available())

    def test_a_declaration_the_schema_contradicts_is_a_verdict(self) -> None:
        assertions = self._assertions(column=False)
        with mock.patch.dict(
            os.environ, {CAPABILITIES_ENV: "client_attribution"}, clear=False
        ):
            with self.assertRaisesRegex(InvariantViolation, "disagree"):
                assertions.attribution_available()


class ClientAttributionVerdict(unittest.TestCase):
    """The attribution verdicts, and the escaping the label read depends on.

    A bench cannot produce the cases that matter: on a working build every pod
    is labelled and the row agrees, so an unlabelled pod, a pod naming another
    client, and a row that disagrees never occur there.
    """

    def _assertions(self, labels: list[str], row: str) -> RateLimiterAssertions:
        return RateLimiterAssertions(
            cast(Any, _Fixture()),
            cast(Any, _AttributionControl(labels, row)),
            cast(Any, None),
            ResourceScope(),
        )

    def test_the_label_read_escapes_the_dotted_key(self) -> None:
        # kubectl answers a malformed jsonpath with empty output rather than an
        # error, so a broken expression here would make every verdict below
        # quantify over an empty list. The dots of the label key have to be
        # escaped inside the selector, and the braces have to survive into the
        # argument.
        self.assertEqual(
            ACTIVATION_CLIENT_JSONPATH,
            "jsonpath={range .items[*]}"
            "{.metadata.labels['space\\.pplx\\.net/client-id']}"
            '{"\\n"}'
            "{end}",
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

    def test_a_row_that_disagrees_with_the_pods_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "not 'space-smoke'"):
            self._assertions(["space-smoke"], "someone-else").expect_client_attributed("sbx-1")

    def test_a_fully_attributed_sandbox_passes_and_records_both_surfaces(self) -> None:
        assertions = self._assertions(["space-smoke", "space-smoke"], "space-smoke")
        assertions.expect_client_attributed("sbx-1")
        recorded = cast(Any, assertions)._fixture.records["client_attribution"]
        self.assertEqual(recorded["pod_labels"], ["space-smoke", "space-smoke"])
        self.assertEqual(recorded["row"], "space-smoke")


class SuspendedFleetMetadata(unittest.TestCase):
    """The staged fleet must not try to restamp the collection's run identity.

    Only a bench can show this one failing, and it fails loudly there: the
    sandbox collection stamps `space_smoke_scenario`, `space_smoke_run` and
    `space_smoke_instance` itself and *raises* `SmokeConfigurationError` when
    a caller supplies any of them (`_create_metadata` in `services.py`), so a
    fleet metadata carrying this class's own run identity would abort the
    phase at its first create. Pinned here because nothing in a local run
    reaches that code.
    """

    # Mirrored from `services.py`; it builds the set inline rather than
    # exporting a constant, so a key added there has to be added here too.
    COLLECTION_STAMPED = frozenset(
        {"space_smoke_scenario", "space_smoke_run", "space_smoke_instance"}
    )

    def test_the_fleet_supplies_no_key_the_collection_stamps_itself(self) -> None:
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, None), ResourceScope()
        )
        staged = cast(Any, assertions)._wake_metadata
        self.assertTrue(staged, "the fleet needs a marker of its own")
        self.assertFalse(self.COLLECTION_STAMPED & set(staged))


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
    `expect_control_isolated` passes either way. On the first bench run this
    phase passed with three admissions and no cluster rejections, which was
    not enough to say the admitted requests had woken anything -- the question
    that separates this phase from the reconnect round beside it.
    """

    def _assertions(self, members: list[_FleetMember]) -> RateLimiterAssertions:
        assertions = RateLimiterAssertions(
            cast(Any, _Fixture()), cast(Any, None), cast(Any, None), ResourceScope()
        )
        cast(Any, assertions)._wake_fleet = members
        return assertions

    def test_a_flood_that_restored_nothing_fails(self) -> None:
        members = [_FleetMember("a", EcwSandboxState.PAUSED), _FleetMember("b", EcwSandboxState.PAUSED)]
        with self.assertRaisesRegex(InvariantViolation, "reconnect storm, not a"):
            cast(Any, self._assertions(members))._expect_a_target_woke(["a", "b"], label="wake_flood")

    def test_one_target_leaving_paused_is_enough(self) -> None:
        members = [_FleetMember("a", EcwSandboxState.PAUSED), _FleetMember("b", EcwSandboxState.RUNNING)]
        assertions = self._assertions(members)
        cast(Any, assertions)._expect_a_target_woke(["a", "b"], label="wake_flood")
        self.assertEqual(
            cast(Any, assertions)._fixture.records["wake_flood_target_states"]["b"],
            str(EcwSandboxState.RUNNING),
        )

    def test_flooding_ids_that_are_not_staged_fails_rather_than_passing_blind(self) -> None:
        members = [_FleetMember("a", EcwSandboxState.RUNNING)]
        with self.assertRaisesRegex(InvariantViolation, "staged fleet member"):
            cast(Any, self._assertions(members))._expect_a_target_woke(["z"], label="wake_flood")


class _ProbeStop(Exception):
    """Raised by `_RecordingProbe` so a round dies before any barrier wait."""


class _RecordingProbe:
    def __init__(self) -> None:
        self.paths: list[str] = []
        self._lock = threading.Lock()

    def race_ecw_request(self, method, path, body, *, client_id, timeout, gate=None):
        with self._lock:
            self.paths.append(path)
        raise _ProbeStop(path)


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


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
