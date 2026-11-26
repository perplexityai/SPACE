"""Unscheduled-pod backlog circuit breaker proven end to end on a real cluster.

`slot_check_enabled` subtracts only pods that already carry `spec.nodeName`, and
`wait_ready` short-circuits only after `kube-scheduler` writes
`PodScheduled=False, reason=Unschedulable`. When `kube-scheduler` falls behind
and hasn't evaluated newly created pods yet, `slot_check_enabled` continues
reporting free worker slots while each admitted cold create leaves another pod
`Pending` (`spec.nodeName` unset) and polls `kube-apiserver` until its create
budget expires.

With `SPACE_CLUSTER_ACTIVATION_ADMISSION_MAX_UNSCHEDULED_PODS=1`:
1. While `kube-scheduler` is paused, the first cold create passes admission
   (`0 < 1`) and materializes one unscheduled pod in `sandboxes`.
2. Concurrent follow-up cold creates are refused before creating additional pods
   with `503 Service Unavailable`, `Server capacity limit: too many sandbox pods
   are waiting to be scheduled.`, and an RFC 9110 integer `Retry-After` in
   `1..=5`.
3. Commands on an already-running sandbox keep succeeding while cold creates are
   shed.
4. Resuming `kube-scheduler` binds the waiting pod, finishes the first create,
   clears the unscheduled count in the API pod reflector, and admits fresh cold
   creates again.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
import secrets
import threading
import time
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    LimiterProbeOutcome,
    RateLimitProbe,
    UnscheduledPodBacklogControl,
)
from space_smoke.rate_limiters import has_retry_after_seconds
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SCENARIO_ID = "local.unscheduled_pod_backlog_sheds_cold_creates"
UNSCHEDULED_BACKLOG_MESSAGE = (
    "Server capacity limit: too many sandbox pods are waiting to be scheduled."
)
ADMISSION_ENV_KEYS: tuple[str, ...] = (
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_MAX_UNSCHEDULED_PODS",
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_UNSCHEDULED_POD_MAX_STALE_SECS",
)
# Cleanup compensations share the scenario's `cleanup_timeout_seconds` (420s):
# scheduler resume + bounded create join (<= 30s) + sandbox sweep run ahead of
# the `space-api` env rollout (<= 240s) and bounded readiness wait (<= 60s),
# leaving headroom for the final Lease release even if readiness times out.
CLEANUP_JOIN_BUDGET_SECONDS = 30.0
CLEANUP_RESTORE_READY_BUDGET_SECONDS = 60.0


class UnscheduledPodBacklogAssertions:
    """Drive the unscheduled-pod backlog circuit breaker on Kind and judge its outcomes."""

    def __init__(
        self,
        fixture: KindEcw,
        control: UnscheduledPodBacklogControl,
        probe: RateLimitProbe,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._probe = probe
        self._scope = scope
        self._standing: dict[str, str | None] | None = None
        self._control_planes: list[str] = []
        self._release_lease: Compensation | None = None
        self._restore_env: Compensation | None = None
        self._sweep_admitted: Compensation | None = None
        self._join_first: Compensation | None = None
        self._resume_scheduler: Compensation | None = None
        self._first_thread: threading.Thread | None = None
        self._first_outcome: LimiterProbeOutcome | None = None
        self._inflight_probes: list[
            tuple[
                threading.Thread,
                list[LimiterProbeOutcome | None],
                Callable[[], None],
            ]
        ] = []
        self._probe_admitted: list[str] = []
        self._admitted: list[str] = []
        self._ambiguous_inflight: int = 0
        self._has_ambiguous: bool = False
        self._metadata = {
            "space_smoke_scenario": SCENARIO_ID,
            "space_smoke_role": "unscheduled-backlog-probe",
            "space_smoke_run": secrets.token_hex(8),
        }

    def _record_completed_outcome(
        self,
        outcome: LimiterProbeOutcome | None,
        *,
        probe_admitted: bool = False,
    ) -> None:
        if (
            outcome is None
            or outcome.status == 0
            or (200 <= outcome.status < 300 and not outcome.sandbox_id)
        ):
            self._has_ambiguous = True
        elif outcome.sandbox_id:
            if outcome.sandbox_id not in self._admitted:
                self._admitted.append(outcome.sandbox_id)
            if probe_admitted and outcome.sandbox_id not in self._probe_admitted:
                self._probe_admitted.append(outcome.sandbox_id)
        self._ambiguous_inflight = max(0, self._ambiguous_inflight - 1)

    def enable_circuit_breaker(
        self,
        *,
        max_unscheduled_pods: int = 1,
        rollout_timeout: float = 240,
    ) -> None:
        """Enable both `slot_check_enabled` and `max_unscheduled_pods` on `space-api`."""

        if max_unscheduled_pods < 1 or max_unscheduled_pods > 4:
            raise ValueError("refusing max_unscheduled_pods outside 1..4 on Kind")
        control_planes = self._control.control_plane_nodes()
        self._fixture.verify(
            len(control_planes) >= 1,
            "Kind stack has no control-plane nodes hosting kube-scheduler",
            control_planes=control_planes,
        )
        self._control_planes = control_planes
        release = self._control.acquire_public_api_mutation()
        self._release_lease = self._scope.defer(
            "release the public API mutation lease", release, critical=True
        )
        # Checked after acquiring the Lease so any orphaned paused
        # `kube-scheduler` repaired during Lease acquisition can bind leftover
        # pods before this scenario starts.
        self._wait_no_unscheduled_pods(timeout=30)
        standing = self._control.public_api_env(ADMISSION_ENV_KEYS)
        self._standing = dict(standing)
        self._fixture.record("standing_admission_env", dict(standing))

        def restore_teardown() -> None:
            assert self._standing is not None
            self._control.restore_public_api_env(self._standing)
            self._control.wait_public_api_ready(
                timeout=min(rollout_timeout, CLEANUP_RESTORE_READY_BUDGET_SECONDS)
            )
            self._fixture.record("restored_env_by_teardown", dict(self._standing))

        self._restore_env = self._scope.defer(
            "restore the public API admission env", restore_teardown, critical=True
        )
        # Registered after `_restore_env` so LIFO failure teardown sweeps
        # run-owned sandboxes before rolling `space-api` pods and while still
        # holding the public API mutation Lease.
        self._sweep_admitted = self._scope.defer(
            "kill every sandbox this run admitted", self.sweep_admitted, critical=True
        )
        overrides = {
            "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED": "true",
            "SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED": "true",
            "SPACE_CLUSTER_ACTIVATION_ADMISSION_MAX_UNSCHEDULED_PODS": str(
                max_unscheduled_pods
            ),
        }
        self._control.set_public_api_env(overrides)
        self._control.wait_public_api_ready(timeout=rollout_timeout)
        applied = self._control.public_api_env(tuple(overrides))
        self._fixture.verify(
            applied == overrides,
            "space-api does not carry the admission env just written",
            applied=applied,
            wanted=overrides,
        )
        self._fixture.record("configured_admission_env", overrides)

    def pause_scheduler_and_admit_first_pending_pod(
        self,
        *,
        template: str,
        pending_timeout: float = 30,
    ) -> str:
        """Pause `kube-scheduler`, dispatch one cold create, and wait until its pod sits `Pending`."""

        if not self._control_planes:
            raise RuntimeError("enable_circuit_breaker() must run first")

        # Wait for any pod created during the `space-api` rollout (such as a
        # warm-pool refill pod) to bind before pausing `kube-scheduler`.
        self._wait_no_unscheduled_pods(timeout=30)

        first_metadata = {**self._metadata, "space_smoke_probe": "first"}

        # Registered before `resume_scheduler` so LIFO teardown resumes
        # `kube-scheduler` first, letting the in-flight create bind and finish
        # quickly before `join_first` and `sweep_admitted` run. Arm
        # `resume_scheduler` before sending `SIGSTOP` so a partial failure or
        # transport timeout inside `pause_kube_scheduler` still resumes the
        # scheduler before the API rollout restore runs.
        self._join_first = self._scope.defer(
            "join in-flight cold creates",
            lambda: self._collect_inflight_creates(
                join_timeout=CLEANUP_JOIN_BUDGET_SECONDS
            ),
            critical=True,
        )
        self._resume_scheduler = self._scope.defer(
            "resume kube-scheduler on control-plane nodes",
            lambda: self._control.resume_kube_scheduler(self._control_planes),
            critical=True,
        )
        self._control.pause_kube_scheduler(self._control_planes)
        self._fixture.record("paused_scheduler_nodes", list(self._control_planes))

        def run_first() -> None:
            self._first_outcome = self._probe.race_ecw_request(
                "POST",
                "/sandboxes",
                {
                    "templateID": template,
                    "timeout": 120,
                    "metadata": first_metadata,
                },
                client_id="space-smoke-first",
                timeout=120,
            )

        self._ambiguous_inflight += 1
        thread = threading.Thread(target=run_first, name="first-cold-create", daemon=True)
        self._first_thread = thread
        thread.start()

        deadline = time.monotonic() + pending_timeout
        pending: list[str] = []
        while True:
            pending = self._control.unscheduled_sandbox_pod_names()
            if len(pending) == 1:
                break
            self._fixture.verify(
                time.monotonic() < deadline and thread.is_alive(),
                "the first cold create did not materialize an unscheduled Pending pod while kube-scheduler was paused",
                pending_pods=pending,
                first_thread_alive=thread.is_alive(),
                first_outcome=None
                if self._first_outcome is None
                else {
                    "status": self._first_outcome.status,
                    "message": self._first_outcome.message[:200],
                },
            )
            self._fixture.wait(0.25)

        self._wait_circuit_breaker_tripped(template=template)
        self._fixture.record("first_pending_pod", pending[0])
        return pending[0]

    def _collect_inflight_creates(self, *, join_timeout: float) -> None:
        deadline = time.monotonic() + join_timeout
        thread = self._first_thread
        if thread is not None:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
            self._first_thread = None
            if thread.is_alive():
                self._has_ambiguous = True
                self._ambiguous_inflight = max(0, self._ambiguous_inflight - 1)
            else:
                self._record_completed_outcome(self._first_outcome)

        inflight_probes = list(self._inflight_probes)
        self._inflight_probes.clear()
        for probe_thread, probe_box, close_forward in inflight_probes:
            probe_thread.join(timeout=max(0.0, deadline - time.monotonic()))
            close_forward()
            if probe_thread.is_alive():
                self._has_ambiguous = True
                self._ambiguous_inflight = max(0, self._ambiguous_inflight - 1)
            else:
                self._record_completed_outcome(probe_box[0], probe_admitted=True)

    def _reap_finished_readiness_probes(self) -> None:
        still_alive: list[
            tuple[
                threading.Thread,
                list[LimiterProbeOutcome | None],
                Callable[[], None],
            ]
        ] = []
        for probe_thread, probe_box, close_forward in self._inflight_probes:
            if probe_thread.is_alive():
                still_alive.append((probe_thread, probe_box, close_forward))
            else:
                close_forward()
                self._record_completed_outcome(probe_box[0], probe_admitted=True)
        self._inflight_probes = still_alive

    def _settle_inflight_readiness_probes(self, *, deadline: float) -> None:
        """Wait until every admitted readiness probe has either finished or created its pod."""

        while True:
            self._reap_finished_readiness_probes()
            if not self._inflight_probes:
                return
            pending = self._control.unscheduled_sandbox_pod_names()
            expected_pending = 1 + len(self._inflight_probes)
            if len(pending) == expected_pending:
                return
            self._fixture.verify(
                len(pending) < expected_pending and time.monotonic() < deadline,
                "admitted readiness probes did not settle to one unscheduled pod per in-flight probe",
                pending_pods=pending,
                expected_pending=expected_pending,
                inflight_probes=len(self._inflight_probes),
            )
            self._fixture.wait(0.05)

    def _wait_circuit_breaker_tripped(
        self,
        *,
        template: str,
        probe_wait: float = 5.0,
        readiness_timeout: float = 15,
    ) -> None:
        """Observe a backlog 503 directly from every ready `space-api` pod before the burst."""

        deadline = time.monotonic() + readiness_timeout
        attempt = 0
        ready_pods: set[str] = set()
        last_summary: dict[str, object] | None = None
        while True:
            current_pods = self._control.public_api_pod_names()
            self._fixture.verify(
                bool(current_pods),
                "space-api has no ready pods while waiting for unscheduled-pod breaker readiness",
            )
            ready_pods.intersection_update(current_pods)
            unready = [pod_name for pod_name in current_pods if pod_name not in ready_pods]
            if not unready:
                break

            for pod_name in unready:
                attempt += 1
                try:
                    api_url, close_forward = self._control.open_public_api_pod_forward(pod_name)
                except RuntimeError as exc:
                    if pod_name not in self._control.public_api_pod_names():
                        last_summary = {"pod": pod_name, "vanished": str(exc)[:200]}
                        continue
                    raise
                self._scope.defer(f"close readiness port-forward to {pod_name}", close_forward)
                known_pending = set(self._control.unscheduled_sandbox_pod_names())
                probe_metadata = {
                    **self._metadata,
                    "space_smoke_probe": f"ready-{attempt}",
                }
                box: list[LimiterProbeOutcome | None] = [None]

                def run_probe(
                    metadata: dict[str, str] = probe_metadata,
                    client_id: str = f"space-smoke-ready-{attempt}",
                    target_url: str = api_url,
                    target: list[LimiterProbeOutcome | None] = box,
                ) -> None:
                    target[0] = self._probe.race_ecw_request(
                        "POST",
                        "/sandboxes",
                        {
                            "templateID": template,
                            "timeout": 120,
                            "metadata": metadata,
                        },
                        client_id=client_id,
                        timeout=120,
                        api_url=target_url,
                    )

                self._ambiguous_inflight += 1
                probe_thread = threading.Thread(
                    target=run_probe,
                    name=f"breaker-readiness-{attempt}",
                    daemon=True,
                )
                probe_thread.start()
                probe_deadline = time.monotonic() + probe_wait
                while probe_thread.is_alive():
                    if any(
                        name not in known_pending
                        for name in self._control.unscheduled_sandbox_pod_names()
                    ):
                        break
                    remaining = probe_deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    probe_thread.join(timeout=min(0.05, remaining))

                if probe_thread.is_alive():
                    # This replica had not received the pending-pod watch event
                    # yet and admitted the probe into pod-readiness wait; keep
                    # its port-forward open until `_collect_inflight_creates`
                    # joins and retires the sandbox after `kube-scheduler` resumes.
                    self._inflight_probes.append((probe_thread, box, close_forward))
                    last_summary = {
                        "pod": pod_name,
                        "inflight_pod_wait": True,
                        "attempt": attempt,
                    }
                else:
                    close_forward()
                    outcome = box[0]
                    self._record_completed_outcome(outcome, probe_admitted=True)
                    if (
                        outcome is not None
                        and outcome.status == 503
                        and outcome.message.startswith(UNSCHEDULED_BACKLOG_MESSAGE)
                        and has_retry_after_seconds(outcome)
                        and 1 <= int((outcome.retry_after or "0").strip()) <= 5
                    ):
                        ready_pods.add(pod_name)
                        continue
                    last_summary = (
                        {"pod": pod_name, "outcome": None}
                        if outcome is None
                        else {
                            "pod": pod_name,
                            "status": outcome.status,
                            "retry_after": outcome.retry_after,
                            "message": outcome.message[:200],
                        }
                    )

            refreshed_pods = self._control.public_api_pod_names()
            if refreshed_pods and all(pod_name in ready_pods for pod_name in refreshed_pods):
                break

            self._fixture.verify(
                time.monotonic() < deadline,
                "not every space-api replica started shedding cold creates after an unscheduled pod appeared",
                attempts=attempt,
                ready_pods=sorted(ready_pods),
                current_pods=refreshed_pods,
                inflight_probes=len(self._inflight_probes),
                last_outcome=last_summary,
            )
            self._fixture.wait(0.25)

        self._settle_inflight_readiness_probes(deadline=deadline)
        self._fixture.record(
            "breaker_readiness",
            {
                "attempts": attempt,
                "ready_pods": sorted(ready_pods),
                "inflight_probes": len(self._inflight_probes),
            },
        )

    def expect_follow_up_cold_creates_shed_without_pods(
        self,
        *,
        template: str,
        burst_size: int = 3,
        request_timeout: float = 8,
    ) -> list[LimiterProbeOutcome]:
        """With 1 unscheduled pod waiting, follow-up cold creates are shed with 503 before creating pods."""

        if burst_size < 1 or burst_size > 8:
            raise ValueError("refusing follow-up burst_size outside 1..8 on Kind")
        self._settle_inflight_readiness_probes(deadline=time.monotonic() + 5.0)
        before_pending = self._control.unscheduled_sandbox_pod_names()
        expected_pending = 1 + len(self._inflight_probes)
        self._fixture.verify(
            len(before_pending) == expected_pending,
            "unexpected unscheduled pod count before dispatching follow-up cold creates",
            before_pending=before_pending,
            expected_pending=expected_pending,
        )

        probe_metadatas = [
            {**self._metadata, "space_smoke_probe": f"shed-{index}"}
            for index in range(burst_size)
        ]

        def send_one(index: int) -> LimiterProbeOutcome:
            return self._probe.race_ecw_request(
                "POST",
                "/sandboxes",
                {
                    "templateID": template,
                    "timeout": 120,
                    "metadata": probe_metadatas[index],
                },
                client_id=f"space-smoke-shed-{index}",
                timeout=request_timeout,
            )

        # A scenario deadline can interrupt `pool.map` after a create commits;
        # count the burst as in-flight before dispatch so `sweep_admitted` lists
        # run-owned sandboxes by metadata if `pool.map` does not complete.
        self._ambiguous_inflight += burst_size
        with ThreadPoolExecutor(max_workers=burst_size) as pool:
            outcomes = list(pool.map(send_one, range(burst_size)))

        for outcome in outcomes:
            self._record_completed_outcome(outcome)

        after_pending = self._control.unscheduled_sandbox_pod_names()
        summary_outcomes = [
            {
                "status": item.status,
                "retry_after": item.retry_after,
                "message": item.message[:200],
            }
            for item in outcomes
        ]
        self._fixture.verify(
            len(after_pending) <= len(before_pending)
            and set(after_pending).issubset(set(before_pending)),
            "follow-up cold creates materialized additional unscheduled pods instead of tripping the circuit breaker",
            pending_before=before_pending,
            pending_after=after_pending,
            outcomes=summary_outcomes,
        )
        self._fixture.verify(
            all(
                item.status == 503
                and item.message.startswith(UNSCHEDULED_BACKLOG_MESSAGE)
                for item in outcomes
            ),
            "not every follow-up cold create was refused with the unscheduled-pod backlog 503",
            outcomes=summary_outcomes,
        )
        self._fixture.verify(
            all(
                has_retry_after_seconds(item)
                and 1 <= int((item.retry_after or "0").strip()) <= 5
                for item in outcomes
            ),
            "every unscheduled-pod backlog 503 must carry an integer Retry-After in 1..=5s",
            outcomes=summary_outcomes,
        )
        self._fixture.record(
            "shed_cold_creates",
            {
                "outcomes": summary_outcomes,
                "pending_pods": after_pending,
            },
        )
        return outcomes

    def expect_running_sandbox_unaffected(self) -> None:
        """An already-running sandbox keeps executing commands while cold creates are shed."""

        marker = "active-sandbox-unaffected"
        result = self._fixture.run_command(f"printf '{marker}'")
        self._fixture.verify(
            result.exit_code == 0 and result.stdout == marker,
            "command execution on an existing running sandbox failed while cold creates were shed",
            exit_code=result.exit_code,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    def resume_scheduler_and_expect_recovery(
        self,
        *,
        template: str,
        recovery_timeout: float = 90,
        pod_cleanup_timeout: float = 120,
    ) -> None:
        """Resume `kube-scheduler`; verify the waiting create finishes and a fresh cold create succeeds."""

        if self._resume_scheduler is not None:
            resume = self._resume_scheduler
            self._resume_scheduler = None
            resume.release()
        if self._join_first is not None:
            self._join_first.disarm()
            self._join_first = None
        self._collect_inflight_creates(join_timeout=130)

        first = self._first_outcome
        self._fixture.verify(
            first is not None
            and first.status in (200, 201)
            and bool(first.sandbox_id),
            "the first cold create did not finish cleanly after kube-scheduler resumed",
            first_status=None if first is None else first.status,
            first_sandbox_id=None if first is None else first.sandbox_id,
            first_message=None if first is None else first.message[:200],
        )
        assert first is not None and first.sandbox_id

        deadline = time.monotonic() + recovery_timeout
        while pending := self._control.unscheduled_sandbox_pod_names():
            self._fixture.verify(
                time.monotonic() < deadline,
                "unscheduled sandbox pods remained after kube-scheduler resumed",
                pending_pods=pending,
            )
            self._fixture.wait(0.5)

        # `kubectl get pods` sees `spec.nodeName` slightly before every
        # `space-api` replica's pod reflector watch delivers the bind event, so
        # retry a lagging `UNSCHEDULED_BACKLOG_MESSAGE` 503 within `deadline`.
        attempt = 0
        while True:
            attempt += 1
            recovered_metadata = {
                **self._metadata,
                "space_smoke_probe": f"recovered-{attempt}",
            }
            self._ambiguous_inflight += 1
            recovered = self._probe.race_ecw_request(
                "POST",
                "/sandboxes",
                {
                    "templateID": template,
                    "timeout": 120,
                    "metadata": recovered_metadata,
                },
                client_id="space-smoke-recovered",
                timeout=120,
            )
            self._record_completed_outcome(recovered)
            if (
                recovered.status == 503
                and recovered.message.startswith(UNSCHEDULED_BACKLOG_MESSAGE)
                and time.monotonic() < deadline
            ):
                self._fixture.wait(0.5)
                continue
            break

        self._fixture.verify(
            recovered.status in (200, 201) and bool(recovered.sandbox_id),
            "cold creates did not recover after the unscheduled-pod backlog cleared",
            status=recovered.status,
            sandbox_id=recovered.sandbox_id,
            attempts=attempt,
            response_message=recovered.message[:200],
        )
        assert recovered.sandbox_id
        self._fixture.record(
            "recovered_creates",
            {
                "first_sandbox_id": first.sandbox_id,
                "probe_sandbox_ids": list(self._probe_admitted),
                "recovered_sandbox_id": recovered.sandbox_id,
                "recovery_attempts": attempt,
            },
        )

        admitted_to_retire = list(
            dict.fromkeys([first.sandbox_id, *self._probe_admitted, recovered.sandbox_id])
        )
        self._probe_admitted.clear()
        for sandbox_id in admitted_to_retire:
            self._probe.kill_sandbox(sandbox_id)
            self._wait_pod_gone(sandbox_id, timeout=pod_cleanup_timeout)
            if sandbox_id in self._admitted:
                self._admitted.remove(sandbox_id)

    def _wait_no_unscheduled_pods(self, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while pending := self._control.unscheduled_sandbox_pod_names():
            self._fixture.verify(
                time.monotonic() < deadline,
                "sandbox namespace still holds unscheduled pods before kube-scheduler is paused",
                pending_pods=pending,
            )
            self._fixture.wait(0.25)

    def _wait_pod_gone(self, sandbox_id: str, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while pods := self._control.sandbox_pods(sandbox_id):
            self._fixture.verify(
                time.monotonic() < deadline,
                "admitted sandbox pod still exists after teardown kill",
                sandbox_id=sandbox_id,
                pods=[f"{pod.pod_name} ({pod.phase})" for pod in pods],
            )
            self._fixture.wait(1)

    def sweep_admitted(self) -> None:
        """Kill any sandbox still owed to this run by ID or run metadata."""

        targets = list(dict.fromkeys(self._admitted))
        errors: list[Exception] = []
        if self._has_ambiguous or self._ambiguous_inflight > 0:
            try:
                pages = self._probe.list_pages(self._metadata, limit=100, max_pages=10)
                for page in pages.pages:
                    for item in page:
                        if item.sandbox_id not in targets:
                            targets.append(item.sandbox_id)
            except Exception as exc:  # noqa: BLE001 - continue killing known IDs
                errors.append(exc)
        for sandbox_id in targets:
            try:
                self._probe.kill_sandbox(sandbox_id)
            except Exception as exc:  # noqa: BLE001 - finish every target before raising
                errors.append(exc)
        if errors:
            raise errors[0]

    def restore(self) -> None:
        """Sweep owed sandboxes, restore `space-api` admission env, verify it, and release the Lease."""

        for slot in ("_sweep_admitted", "_restore_env"):
            compensation = getattr(self, slot)
            if compensation is not None:
                setattr(self, slot, None)
                compensation.release()
        if self._standing is not None:
            applied = self._control.public_api_env(ADMISSION_ENV_KEYS)
            wanted = dict(self._standing)
            self._fixture.verify(
                applied == wanted,
                "space-api does not carry its standing admission env after restore",
                applied=applied,
                wanted=wanted,
            )
            self._fixture.record("restored_admission_env", wanted)
        if self._release_lease is not None:
            release = self._release_lease
            self._release_lease = None
            release.release()
