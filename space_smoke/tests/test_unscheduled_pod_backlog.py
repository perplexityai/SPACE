"""Contracts of the unscheduled-pod backlog scenario the bench cannot exercise.

A passing Kind run shows the circuit breaker shedding cold creates once
`kube-scheduler` is paused and recovering once it resumes. It cannot reliably
force one `space-api` replica's watch stream to lag behind `kubectl get pods`,
delay a lagging readiness probe's pod creation, interrupt
`ThreadPoolExecutor.map` mid-burst with a scenario deadline, exhaust a rollout
readiness wait during teardown, pause a subset of nodes, or point orphan Lease
repair at a non-isolated cluster. Those paths are pinned here.
"""

from __future__ import annotations

import json
import threading
import time
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

import space_smoke.unscheduled_pod_backlog as backlog_module
from space_smoke.adapters import (
    PAUSED_SCHEDULER_ANNOTATION,
    KindConfig,
    KindHarness,
    _LeaseOwnership,
)
from space_smoke.capabilities import LimiterProbeOutcome
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    SmokeConfigurationError,
)
from space_smoke.scenarios.local_unscheduled_pod_backlog_sheds_cold_creates import (
    unscheduled_pod_backlog_sheds_cold_creates as backlog_scenario,
)
from space_smoke.unscheduled_pod_backlog import (
    CLEANUP_JOIN_BUDGET_SECONDS,
    CLEANUP_RESTORE_READY_BUDGET_SECONDS,
    SCENARIO_ID,
    UNSCHEDULED_BACKLOG_MESSAGE,
    UnscheduledPodBacklogAssertions,
)


def _shed_outcome(retry_after: str = "2") -> LimiterProbeOutcome:
    return LimiterProbeOutcome(
        503,
        retry_after,
        f"{UNSCHEDULED_BACKLOG_MESSAGE} Please retry shortly.",
        None,
        0.01,
    )


class _Fixture:
    def __init__(self) -> None:
        self.records: dict[str, object] = {}

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, seconds: float) -> None:
        time.sleep(min(seconds, 0.01))

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)


class _LaggingControl:
    def __init__(self, scheduler_resumed: threading.Event) -> None:
        self._scheduler_resumed = scheduler_resumed
        self.pending: list[str] = []
        self.env: dict[str, str | None] = {}
        self.events: list[str] = []
        self.opened_forwards: list[str] = []
        self.closed_forwards: list[str] = []
        self._forward_seq = 0
        self.fail_restore_readback = False
        self.hang_restore_ready = False

    def control_plane_nodes(self) -> list[str]:
        return ["space-local-control-plane"]

    def public_api_pod_names(self) -> list[str]:
        return ["space-api-0", "space-api-1"]

    def open_public_api_pod_forward(self, pod_name: str):
        self._forward_seq += 1
        tag = f"{pod_name}#{self._forward_seq}"
        self.opened_forwards.append(tag)
        closed = False

        def close() -> None:
            nonlocal closed
            if not closed:
                closed = True
                self.closed_forwards.append(tag)

        port = 41000 if pod_name == "space-api-0" else 41001
        return f"http://127.0.0.1:{port}", close

    def acquire_public_api_mutation(self):
        self.events.append("acquire_lease")

        def release() -> None:
            self.events.append("release_lease")

        return release

    def public_api_env(self, keys):
        self.events.append("read_env")
        if self.fail_restore_readback:
            return {key: "stale-contender" for key in keys}
        return {key: self.env.get(key) for key in keys}

    def set_public_api_env(self, overrides) -> None:
        self.env.update(overrides)

    def restore_public_api_env(self, standing) -> None:
        self.events.append("restore_env")
        for key, value in standing.items():
            self.env[key] = value

    def wait_public_api_ready(self, *, timeout: float) -> None:
        if self.hang_restore_ready and "restore_env" in self.events:
            time.sleep(timeout)
            raise TimeoutError(f"readyz did not pass within {timeout:g}s")

    def pause_kube_scheduler(self, _nodes) -> None:
        return None

    def resume_kube_scheduler(self, _nodes) -> None:
        self.events.append("resume_scheduler")
        self.pending.clear()
        self._scheduler_resumed.set()

    def unscheduled_sandbox_pod_names(self) -> list[str]:
        return list(self.pending)

    def sandbox_pods(self, _sandbox_id: str):
        return []


class _LaggingProbe:
    """`space-api-0` sheds immediately; `space-api-1` lags once, then sheds."""

    def __init__(
        self,
        control: _LaggingControl,
        scheduler_resumed: threading.Event,
    ) -> None:
        self._control = control
        self._scheduler_resumed = scheduler_resumed
        self._lock = threading.Lock()
        self._lagging_replica_hits = 0
        self.killed: list[str] = []
        self.list_calls = 0

    def race_ecw_request(
        self,
        _method: str,
        _path: str,
        body: dict[str, Any],
        *,
        client_id: str,
        timeout: float,
        api_url: str | None = None,
    ) -> LimiterProbeOutcome:
        del timeout
        probe_tag = body["metadata"]["space_smoke_probe"]
        if probe_tag == "first":
            with self._lock:
                self._control.pending.append("sandbox-first")
            self._scheduler_resumed.wait(timeout=5)
            return LimiterProbeOutcome(201, None, "", "sbx_first", 0.1)
        if probe_tag.startswith("ready-"):
            if api_url == "http://127.0.0.1:41001":
                with self._lock:
                    self._lagging_replica_hits += 1
                    hit = self._lagging_replica_hits
                    if hit == 1:
                        self._control.pending.append("sandbox-lagging-ready")
                if hit == 1:
                    self._scheduler_resumed.wait(timeout=5)
                    return LimiterProbeOutcome(201, None, "", "sbx_lagging_ready", 0.1)
            return _shed_outcome()
        if probe_tag.startswith("shed-"):
            return _shed_outcome()
        if probe_tag.startswith("recovered-"):
            return LimiterProbeOutcome(201, None, "", "sbx_recovered", 0.1)
        raise AssertionError(f"unexpected probe tag {probe_tag} ({client_id})")

    def kill_sandbox(self, sandbox_id: str) -> None:
        self._control.events.append(f"kill:{sandbox_id}")
        self.killed.append(sandbox_id)

    def list_pages(self, _metadata, *, limit: int, max_pages: int):
        del limit, max_pages
        self.list_calls += 1
        return SimpleNamespace(pages=[])


class _InterruptedBurstProbe:
    """Commits one sandbox during the burst, then raises a deadline interrupt."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._committed: list[tuple[str, dict[str, str]]] = [
            (
                "sbx_other_run",
                {
                    "space_smoke_scenario": SCENARIO_ID,
                    "space_smoke_role": "unscheduled-backlog-probe",
                    "space_smoke_run": "other-run",
                },
            )
        ]
        self.killed: list[str] = []
        self.listed_metadata: list[dict[str, str]] = []

    def race_ecw_request(
        self,
        _method: str,
        _path: str,
        body: dict[str, Any],
        *,
        client_id: str,
        timeout: float,
        api_url: str | None = None,
    ) -> LimiterProbeOutcome:
        del timeout, api_url
        if client_id == "space-smoke-shed-0":
            with self._lock:
                self._committed.append(
                    ("sbx_committed_before_interrupt", dict(body["metadata"]))
                )
            return LimiterProbeOutcome(201, None, "", "sbx_committed_before_interrupt", 0.01)
        raise TimeoutError("scenario deadline expired mid-burst")

    def list_pages(self, metadata: dict[str, str], *, limit: int, max_pages: int):
        del limit, max_pages
        self.listed_metadata.append(dict(metadata))
        with self._lock:
            matched = [
                SimpleNamespace(sandbox_id=sandbox_id)
                for sandbox_id, stored in self._committed
                if all(stored.get(key) == value for key, value in metadata.items())
            ]
        return SimpleNamespace(pages=[matched] if matched else [])

    def kill_sandbox(self, sandbox_id: str) -> None:
        self.killed.append(sandbox_id)


def _isolated_lease_harness(
    runner,
    initial_paused: str | None = None,
) -> tuple[KindHarness, dict[str, str | None], list[dict[str, str | None]]]:
    harness = KindHarness(
        cast(Any, SimpleNamespace(run=runner)),
        KindConfig(cluster="space-local", api_url="http://127.0.0.1:8080"),
    )
    ownership = _LeaseOwnership("space-smoke-public-api", "uid-1", "holder-1", "public API")
    harness._public_api_lease_ownership = ownership
    state: dict[str, str | None] = {PAUSED_SCHEDULER_ANNOTATION: initial_paused}
    writes: list[dict[str, str | None]] = []

    def fake_lease_annotation(_own: _LeaseOwnership, key: str) -> str | None:
        return state.get(key)

    def fake_renew_lease(
        _own: _LeaseOwnership,
        _action: str,
        annotations: dict[str, str | None] | None = None,
    ) -> None:
        if annotations:
            writes.append(dict(annotations))
            for key, value in annotations.items():
                state[key] = value

    harness._lease_annotation = fake_lease_annotation  # type: ignore[method-assign]
    harness._renew_lease = fake_renew_lease  # type: ignore[method-assign]
    return harness, state, writes


class UnscheduledPodBacklogContracts(unittest.TestCase):
    def test_readiness_wait_probes_every_replica_absorbs_lagging_pod_and_verifies_restore_under_lease(
        self,
    ) -> None:
        resumed = threading.Event()
        control = _LaggingControl(resumed)
        probe = _LaggingProbe(control, resumed)
        fixture = _Fixture()
        scope = ResourceScope()
        assertions = UnscheduledPodBacklogAssertions(
            cast(Any, fixture),
            cast(Any, control),
            cast(Any, probe),
            scope,
        )
        assertions.enable_circuit_breaker(max_unscheduled_pods=1)

        first_pod = assertions.pause_scheduler_and_admit_first_pending_pod(template="base")
        self.assertEqual(first_pod, "sandbox-first")
        self.assertEqual(
            fixture.records["breaker_readiness"],
            {
                "attempts": 3,
                "ready_pods": ["space-api-0", "space-api-1"],
                "inflight_probes": 1,
            },
        )
        # `space-api-1#2` stays open while its lagging create waits on pod bind;
        # `space-api-0#1` and `space-api-1#3` answered 503 and closed right away.
        self.assertEqual(
            control.opened_forwards,
            ["space-api-0#1", "space-api-1#2", "space-api-1#3"],
        )
        self.assertEqual(
            control.closed_forwards,
            ["space-api-0#1", "space-api-1#3"],
        )

        outcomes = assertions.expect_follow_up_cold_creates_shed_without_pods(
            template="base", burst_size=3
        )
        self.assertEqual([item.status for item in outcomes], [503, 503, 503])

        assertions.resume_scheduler_and_expect_recovery(template="base")
        self.assertIn("space-api-1#2", control.closed_forwards)
        self.assertEqual(
            probe.killed,
            ["sbx_first", "sbx_lagging_ready", "sbx_recovered"],
        )

        # Leave one owed ID to verify `restore()` sweeps, restores env, verifies
        # standing env while holding the Lease, and releases the Lease last.
        assertions._admitted.append("sbx_leftover")
        assertions.restore()
        self.assertEqual(probe.list_calls, 0)
        self.assertFalse(assertions._has_ambiguous)
        self.assertEqual(assertions._ambiguous_inflight, 0)
        self.assertEqual(
            control.events[-4:],
            ["kill:sbx_leftover", "restore_env", "read_env", "release_lease"],
        )
        scope.close()

    def test_restore_keeps_lease_armed_when_readback_verification_fails(self) -> None:
        resumed = threading.Event()
        control = _LaggingControl(resumed)
        probe = _LaggingProbe(control, resumed)
        scope = ResourceScope()
        assertions = UnscheduledPodBacklogAssertions(
            cast(Any, _Fixture()),
            cast(Any, control),
            cast(Any, probe),
            scope,
        )
        assertions.enable_circuit_breaker(max_unscheduled_pods=1)
        control.fail_restore_readback = True

        with self.assertRaisesRegex(InvariantViolation, "standing admission env after restore"):
            assertions.restore()

        self.assertNotIn("release_lease", control.events)
        failures = scope.close()
        self.assertEqual(failures, ())
        self.assertEqual(control.events[-1], "release_lease")

    def test_burst_waits_for_delayed_readiness_probe_pod_before_capturing_baseline(
        self,
    ) -> None:
        resumed = threading.Event()
        control = _LaggingControl(resumed)
        control.pending = ["sandbox-first"]
        probe = _LaggingProbe(control, resumed)
        assertions = UnscheduledPodBacklogAssertions(
            cast(Any, _Fixture()),
            cast(Any, control),
            cast(Any, probe),
            ResourceScope(),
        )
        release_probe = threading.Event()
        box: list[LimiterProbeOutcome | None] = [None]

        def delayed_pod_probe() -> None:
            time.sleep(0.03)
            control.pending.append("sandbox-delayed-ready")
            release_probe.wait(timeout=5)
            box[0] = LimiterProbeOutcome(201, None, "", "sbx_delayed_ready", 0.1)

        thread = threading.Thread(target=delayed_pod_probe, daemon=True)
        assertions._ambiguous_inflight = 1
        assertions._inflight_probes.append((thread, box, lambda: None))
        thread.start()
        try:
            outcomes = assertions.expect_follow_up_cold_creates_shed_without_pods(
                template="base", burst_size=3
            )
            self.assertEqual([item.status for item in outcomes], [503, 503, 503])
            self.assertEqual(
                control.pending,
                ["sandbox-first", "sandbox-delayed-ready"],
            )
        finally:
            release_probe.set()
            thread.join(timeout=2)

    def test_teardown_retires_sandboxes_and_releases_lease_when_restore_ready_times_out(
        self,
    ) -> None:
        self.assertIsNotNone(backlog_scenario.cleanup_timeout_seconds)
        assert backlog_scenario.cleanup_timeout_seconds is not None
        self.assertGreaterEqual(
            backlog_scenario.cleanup_timeout_seconds,
            CLEANUP_JOIN_BUDGET_SECONDS + 240.0 + CLEANUP_RESTORE_READY_BUDGET_SECONDS + 60.0,
        )

        resumed = threading.Event()
        control = _LaggingControl(resumed)
        control.hang_restore_ready = True
        probe = _LaggingProbe(control, resumed)
        scope = ResourceScope()
        assertions = UnscheduledPodBacklogAssertions(
            cast(Any, _Fixture()),
            cast(Any, control),
            cast(Any, probe),
            scope,
        )
        assertions.enable_circuit_breaker(max_unscheduled_pods=1)
        assertions._admitted.append("sbx_admitted_before_failure")

        with mock.patch.object(
            backlog_module, "CLEANUP_RESTORE_READY_BUDGET_SECONDS", 0.3
        ):
            failures = scope.close(timeout_seconds=3)

        self.assertEqual([failure.name for failure in failures], ["restore the public API admission env"])
        self.assertEqual(
            [failure.name for failure in failures if "deadline exhausted" in failure.detail],
            [],
        )
        self.assertEqual(probe.killed, ["sbx_admitted_before_failure"])
        self.assertLess(
            control.events.index("kill:sbx_admitted_before_failure"),
            control.events.index("restore_env"),
        )
        self.assertEqual(control.events[-1], "release_lease")

    def test_interrupted_burst_marks_run_ambiguous_and_filters_by_run_metadata(self) -> None:
        resumed = threading.Event()
        control = _LaggingControl(resumed)
        control.pending = ["sandbox-first"]
        probe = _InterruptedBurstProbe()
        assertions = UnscheduledPodBacklogAssertions(
            cast(Any, _Fixture()),
            cast(Any, control),
            cast(Any, probe),
            ResourceScope(),
        )
        self.assertEqual(assertions._ambiguous_inflight, 0)
        self.assertEqual(
            probe.list_pages({"space_smoke_run": "non-existent"}, limit=10, max_pages=1).pages,
            [],
        )

        with self.assertRaisesRegex(TimeoutError, "deadline expired mid-burst"):
            assertions.expect_follow_up_cold_creates_shed_without_pods(
                template="base", burst_size=2
            )

        self.assertGreater(assertions._ambiguous_inflight, 0)
        assertions.sweep_admitted()
        self.assertEqual(probe.listed_metadata[-1], assertions._metadata)
        self.assertEqual(probe.killed, ["sbx_committed_before_interrupt"])

    def test_pause_and_resume_scheduler_maintain_paused_node_set_on_lease(self) -> None:
        commands: list[tuple[str, ...]] = []

        def run(cmd, **_kwargs) -> CommandResult:
            commands.append(tuple(cmd))
            return CommandResult(tuple(cmd), 0, "", "")

        harness, state, _writes = _isolated_lease_harness(run)
        harness.pause_kube_scheduler(["space-local-control-plane"])
        self.assertEqual(
            json.loads(state[PAUSED_SCHEDULER_ANNOTATION] or "[]"),
            ["space-local-control-plane"],
        )

        harness.pause_kube_scheduler(["space-local-worker2"])
        self.assertEqual(
            json.loads(state[PAUSED_SCHEDULER_ANNOTATION] or "[]"),
            ["space-local-control-plane", "space-local-worker2"],
        )

        harness.resume_kube_scheduler(["space-local-control-plane"])
        self.assertEqual(
            json.loads(state[PAUSED_SCHEDULER_ANNOTATION] or "[]"),
            ["space-local-worker2"],
        )

        harness.resume_kube_scheduler(["space-local-worker2"])
        self.assertIsNone(state[PAUSED_SCHEDULER_ANNOTATION])
        self.assertTrue(any("-STOP" in cmd for cmd in commands))
        self.assertTrue(any("-CONT" in cmd for cmd in commands))

    def test_resume_scheduler_status_one_retains_annotation_unless_container_running_and_absent(
        self,
    ) -> None:
        container_state = "true true"
        presence_stdout = "absent"

        def run(cmd, **_kwargs) -> CommandResult:
            argv = tuple(cmd)
            if argv[:2] == ("docker", "inspect"):
                return CommandResult(argv, 0, f"{container_state}\n", "")
            if "-CONT" in argv:
                return CommandResult(argv, 1, "", "container is paused")
            if "pgrep -x kube-scheduler" in " ".join(argv):
                return CommandResult(argv, 0, presence_stdout, "")
            raise AssertionError(f"unexpected command {argv}")

        harness, state, _writes = _isolated_lease_harness(
            run,
            initial_paused='["space-local-control-plane"]',
        )
        with self.assertRaisesRegex(RuntimeError, "container state is 'true true'"):
            harness.resume_kube_scheduler(["space-local-control-plane"])
        self.assertEqual(
            state[PAUSED_SCHEDULER_ANNOTATION],
            '["space-local-control-plane"]',
        )

        container_state = "true false"
        harness.resume_kube_scheduler(["space-local-control-plane"])
        self.assertIsNone(state[PAUSED_SCHEDULER_ANNOTATION])

    def test_orphan_scheduler_repair_refuses_non_isolated_api_target(self) -> None:
        harness = KindHarness(
            cast(Any, SimpleNamespace(run=lambda *a, **k: CommandResult(("kubectl",), 0, "", ""))),
            KindConfig(cluster="remote", api_url="https://space.example.invalid"),
        )
        previous_lease = {
            "metadata": {
                "annotations": {
                    PAUSED_SCHEDULER_ANNOTATION: '["space-local-control-plane"]',
                }
            }
        }
        with self.assertRaisesRegex(SmokeConfigurationError, "isolated local Kind API"):
            harness._repair_recorded_limiter_faults(
                previous_lease, cast(Any, SimpleNamespace())
            )


if __name__ == "__main__":
    unittest.main()
