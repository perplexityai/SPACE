"""State-only observations of an idle suspend crossing a real manager handoff."""

from __future__ import annotations

import shlex
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

from space_smoke.capabilities import SandboxPodPlacement

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw


@dataclass(frozen=True)
class HandoffObservation:
    lifecycle: str
    next_event: str | None
    last_error: str | None
    runtime_pid: int | None
    runtime_alive: bool
    runtime_start_ticks: str | None
    pod_phase: str | None
    transfers: tuple[dict[str, object], ...]
    carrier_pid: int | None = None
    carrier_alive: bool = False
    carrier_start_ticks: str | None = None
    uploads: tuple[dict[str, object], ...] = ()
    transfer_query_error: str | None = None


class AutosuspendHandoffControl(Protocol):
    def locate_handoff(self, sandbox_id: str) -> SandboxPodPlacement: ...
    def prepare_handoff(self, placement: SandboxPodPlacement) -> None: ...
    def begin_handoff(
        self, placement: SandboxPodPlacement, *, hold_successor: bool = False
    ) -> dict[str, object]: ...
    def release_handoff(self) -> None: ...
    def release_handoff_storage(self) -> None: ...
    def finish_handoff(self, placement: SandboxPodPlacement) -> None: ...
    def observe_handoff(self, placement: SandboxPodPlacement) -> HandoffObservation: ...
    def collect_handoff_logs(self, placement: SandboxPodPlacement) -> str: ...
    def cleanup_handoff(self, placement: SandboxPodPlacement) -> None: ...
    def restart_handoff_owner(
        self, placement: SandboxPodPlacement
    ) -> dict[str, object]: ...


@dataclass
class AutosuspendHandoffAssertions:
    fixture: KindEcw
    control: AutosuspendHandoffControl

    def run(self, *, outage_seconds: int = 70, restart_pending: bool = False) -> None:
        f = self.fixture
        placement = self.control.locate_handoff(f.sandbox.sandbox_id)
        f.record("handoff_placement", asdict(placement))
        observations: list[dict[str, object]] = []

        def observe() -> HandoffObservation:
            state = self.control.observe_handoff(placement)
            observations.append({"at": datetime.now(UTC).isoformat(), **asdict(state)})
            f.record("handoff_observations", observations)
            return state

        # Register compensation before the first fault. The fault only affects
        # the dedicated Kind stack, never the deployed backend.
        f._context.scope.defer(
            "remove handoff probe files",
            lambda: self.control.cleanup_handoff(placement),
        )
        f._context.scope.defer(
            "restore object storage after volume handoff",
            self.control.release_handoff,
            critical=True,
        )
        self.control.prepare_handoff(placement)
        marker = f"volume-handoff-{f.sandbox.sandbox_id}"
        marker_path = "/tmp/space-smoke-volume-handoff.txt"
        written = f.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(marker_path)}"
        )
        f.verify(written.exit_code == 0, "cannot write the guest marker")
        before = observe()
        f.verify(before.runtime_alive, "the reproduction requires a live real VM")
        f.verify(before.carrier_alive, "the reproduction requires a live shim carrier")
        f.record("baseline_runtime", asdict(before))

        handoff = self.control.begin_handoff(placement, hold_successor=restart_pending)
        f.record("volume_manager_handoff", handoff)
        started = time.monotonic()
        # A real-time observation window is deliberate here: this opt-in
        # deployment scenario must span the actual access-idle decision.
        restarted = False
        storage_released = False
        while time.monotonic() - started < outage_seconds:
            current = observe()
            if (
                outage_seconds > 70
                and not storage_released
                and time.monotonic() - started >= 65
            ):
                self.control.release_handoff_storage()
                storage_released = True
            pending = any(
                isinstance(job.get("auto_suspend"), dict)
                and int(job["auto_suspend"].get("failures", 0)) > 0
                for job in current.uploads
            )
            if restart_pending and pending and not restarted:
                f.verify(
                    current.next_event is not None,
                    "transient exhaustion lost its deadline",
                )
                f.verify(
                    current.runtime_alive, "the pending retry must not tear down the VM"
                )
                f.record("pending_retry_before_restart", asdict(current))
                f.record(
                    "sandbox_manager_restart",
                    self.control.restart_handoff_owner(placement),
                )
                restarted = True
            f.wait(2)
        if restart_pending:
            f.verify(
                restarted, "the long outage did not leave durable retry work to restart"
            )
        self.control.release_handoff()
        self.control.finish_handoff(placement)
        f.record("dependency_recovered_at", datetime.now(UTC).isoformat())

        deadline = time.monotonic() + 100
        last = observe()
        while time.monotonic() < deadline:
            if last.lifecycle.endswith("_SUSPENDED") and not last.runtime_alive:
                break
            f.wait(2)
            last = observe()
        f.record("handoff_service_logs", self.control.collect_handoff_logs(placement))
        refused = [
            job
            for observation in observations
            for job in observation["uploads"]
            if isinstance(job.get("auto_suspend"), dict)
            and job["auto_suspend"].get("refused_at") is not None
        ]
        f.verify(
            bool(refused), "no persisted real transfer-admission refusal was observed"
        )
        f.verify(
            last.lifecycle.endswith("_SUSPENDED") and not last.runtime_alive,
            "idle suspend did not recover after the real volume-manager handoff",
            original_runtime_pid=before.runtime_pid,
            final_state=asdict(last),
        )
        deadline = time.monotonic() + 60
        while True:
            final = observe()
            if final.pod_phase in (None, "Succeeded") and not final.carrier_alive:
                break
            f.verify(
                time.monotonic() < deadline, "the shim carrier did not exit normally"
            )
            f.wait(2)
        identities = {
            (job["job_id"], job["snapshot_id"], job["local_artifact_id"])
            for observation in observations
            for job in observation["uploads"]
            if job.get("auto_suspend") is not None
        }
        f.verify(
            len(identities) == 1,
            "recovery allocated a replacement capture or backup attempt",
            identities=sorted(identities),
        )
        destinations: dict[str, set[str]] = {}
        for observation in observations:
            for transfer in observation["transfers"]:
                destinations.setdefault(str(transfer["s3Uri"]), set()).add(
                    str(transfer["jobId"])
                )
        f.verify(
            all(len(ids) == 1 for ids in destinations.values()),
            "accepted immutable transfers were duplicated",
            transfers={uri: sorted(ids) for uri, ids in destinations.items()},
        )
        deadline = time.monotonic() + 180
        while not any(job.get("status") == "completed" for job in final.uploads):
            f.verify(
                time.monotonic() < deadline, "the retained backup did not become ready"
            )
            f.wait(2)
            final = observe()
        f.record("retained_backup", asdict(final))
        f.reconnect_by_id(timeout=300)
        f.expect_file(marker_path, marker)
