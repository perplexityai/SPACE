"""Memory admission stops at the sandbox parent cgroup's `memory.max`.

Every sandbox cgroup sits under `/sys/fs/cgroup/space`, whose `memory.max`
is below host memory. The kernel OOM-kills sandboxes in that cgroup when it
reaches the limit, whatever the host still reports available. node-manager's
admission therefore runs its host rule a second time against that limit:

    parent used + running sandboxes' growth charge + new demand + reserve
        <= parent memory.max

where "used" leaves out reclaimable file cache. This module sets each Ready
worker's limit a known distance above or below that sum, with host memory
far above it, and judges the public create. A create admitted a little
above the sum and refused a little below it proves the parent limit, not host
memory, decided, and that running sandboxes are charged no more than today's
growth (at most `reserve_mib` each, the `commit_percent = 0` default). Where a
sandbox's limit exceeds its use by more than `reserve_mib` plus the slack,
charging its full limit would refuse both; the recorded full-charge sum shows
whether this run's template separates the two. Hiding the parent's
`memory.stat` from node-manager alone, with the limit still below the sum,
must leave only the host check, which admits.

The sandbox a refused create would have booted still fits under the lowered
limit, so a refusal is admission, never the kernel failing a boot.
"""

from __future__ import annotations

import secrets
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    CgroupMemory,
    ParentMemoryAdmissionControl,
    RateLimitProbe,
    SandboxParentMemory,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

MIB = 1024 * 1024
# node-manager's default `memory_admission.reserve_mib`.
RESERVE_MIB = 1024
# How far each worker's limit sits from the admission sum. Parent use moves
# by a few MiB between the read and node-manager's; a sandbox charged its full
# limit adds thousands.
SLACK_MIB = 256
# The fixture sandbox's create path waits for readiness; a refused create
# fails within the API's create deadline.
CREATE_TIMEOUT_SECONDS = 200
# Probes must outlive the scenario's 1500 s budget: an occupying probe that idled
# out mid-run would lower the admission sum and let a below-sum create through.
PROBE_TIMEOUT_SECONDS = 1800


def _mib_ceil(value: int) -> int:
    return -(-value // MIB)


def _used(memory: CgroupMemory) -> int:
    return max(memory.current - memory.reclaimable_file, 0)


def growth_charge_mib(sandbox: CgroupMemory, *, commit_percent: int) -> int:
    """What admission charges a running sandbox for growth."""

    if sandbox.maximum is None:
        return 0
    committed = sandbox.maximum * commit_percent // 100
    growth = max(RESERVE_MIB * MIB, committed - _used(sandbox))
    return _mib_ceil(min(sandbox.maximum - _used(sandbox), growth))


def admission_sum_mib(memory: SandboxParentMemory, demand_mib: int, *, commit_percent: int) -> int:
    growth = sum(growth_charge_mib(s, commit_percent=commit_percent) for s in memory.sandboxes)
    return _mib_ceil(_used(memory.parent)) + growth + demand_mib + RESERVE_MIB


class ParentMemoryAdmissionAssertions:
    """Move each worker's parent limit around the admission sum and create publicly."""

    def __init__(
        self,
        fixture: KindEcw,
        control: ParentMemoryAdmissionControl,
        probe: RateLimitProbe,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._probe = probe
        self._scope = scope
        self._release_lease: Compensation | None = None
        self._restore: Compensation | None = None
        self._sweep: Compensation | None = None
        self._workers: list[str] = []
        self._standing: dict[str, str] = {}
        self._demand_mib = 0
        self._started_at = ""
        self._metadata = {
            "space_smoke_scenario": "local.parent_memory_admission",
            "space_smoke_run": secrets.token_hex(8),
        }

    def capture(self) -> None:
        """Take the Lease, record every Ready worker's standing limit, arm the restore."""

        release = self._control.acquire_parent_memory_mutation()
        self._release_lease = self._scope.defer("release the parent-memory mutation lease", release)
        self._workers = self._control.parent_memory_workers()
        self._fixture.verify(bool(self._workers), "no Ready sandbox worker to run on")
        standing = self._standing
        for node in self._workers:
            memory = self._control.read_sandbox_parent_memory(node)
            standing[node] = "max" if memory.parent.maximum is None else str(memory.parent.maximum)
        self._fixture.record("standing_parent_memory_max", standing)
        self._control.record_standing_parent_memory_max(standing)
        self._sweep = self._scope.defer(
            "kill the run's probe sandboxes", lambda: self._probe.kill_matching(self._metadata)
        )
        self._restore = self._scope.defer(
            "restore the workers' parent memory.max and memory.stat",
            self._control.restore_parent_memory,
            critical=True,
        )
        self._started_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def occupy_every_worker(self, *, attempts_per_worker: int = 3) -> None:
        """Run a sandbox on every worker so each one's growth charge is non-zero."""

        _, node = self._control.sandbox_pod_phase_and_node(self._fixture.sandbox.sandbox_id)
        occupied = {node} if node else set()
        for _ in range(attempts_per_worker * len(self._workers)):
            if occupied >= set(self._workers):
                break
            sandbox_id = self._create(expect_admitted=True, reason="occupy a worker")
            occupied.add(self._wait_running(sandbox_id))
        demand = [
            s.maximum
            for n in self._workers
            for s in self._control.read_sandbox_parent_memory(n).sandboxes
            if s.maximum is not None
        ]
        self._fixture.verify(
            occupied >= set(self._workers) and bool(demand),
            "could not run a sandbox on every Ready worker",
            occupied=sorted(occupied),
            workers=self._workers,
        )
        # Every probe uses the fixture's template, so its cgroup limit is the
        # new sandbox's demand.
        self._demand_mib = _mib_ceil(max(demand))
        self._fixture.record("sandbox_cgroup_memory_mib", self._demand_mib)

    def set_limits(self, offset_mib: int, label: str) -> None:
        """Set each worker's limit to today's admission sum plus `offset_mib`."""

        limits = {}
        for node in self._workers:
            memory = self._control.read_sandbox_parent_memory(node)
            today = admission_sum_mib(memory, self._demand_mib, commit_percent=0)
            full = admission_sum_mib(memory, self._demand_mib, commit_percent=100)
            limit = today + offset_mib
            # The limit must leave room for the sandbox to boot, or a refusal
            # could be the kernel's rather than admission's; and host memory
            # must fit even a full charge, so only the parent limit can refuse.
            self._fixture.verify(
                _mib_ceil(_used(memory.parent)) + self._demand_mib < limit
                and memory.host_available // MIB > full,
                f"{node}'s memory cannot place the {label} limit between boot room and a full charge",
                node=node,
                limit_mib=limit,
                today_sum_mib=today,
                full_charge_sum_mib=full,
                parent_used_mib=_mib_ceil(_used(memory.parent)),
                host_available_mib=memory.host_available // MIB,
            )
            self._control.write_parent_memory_max(node, str(limit * MIB))
            limits[node] = {
                "limit_mib": limit,
                "today_sum_mib": today,
                "full_charge_sum_mib": full,
                "host_available_mib": memory.host_available // MIB,
                "running_sandboxes": len(memory.sandboxes),
            }
        self._fixture.record(f"limits_{label}", limits)

    def expect_admitted(self, label: str) -> None:
        """The create is admitted; its sandbox is then killed and its cgroup gone."""

        before = {node: self._sandbox_cgroups(node) for node in self._workers}
        sandbox_id = self._create(expect_admitted=True, reason=label)
        node = self._wait_running(sandbox_id)
        self._fixture.record(f"{label}_node", node)
        added = self._sandbox_cgroups(node) - before[node]
        self._probe.kill_sandbox(sandbox_id)
        # The next limit is computed from the remaining cgroups.
        deadline = time.monotonic() + 120
        while added & self._sandbox_cgroups(node):
            self._fixture.verify(
                time.monotonic() < deadline,
                "a killed probe's sandbox cgroup stayed",
                node=node,
                cgroups=sorted(added),
            )
            self._fixture.wait(2)

    def expect_refused(self, label: str) -> None:
        """The create is refused, and the pod's sandbox failure names the parent cgroup."""

        since = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        # The refused create returns no sandbox id, so its pod is the one
        # that did not exist before it; the Lease keeps other creates out.
        existing = self._control.sandbox_pod_names()
        self._create(expect_admitted=False, reason=label)
        # Kubelet records the sandbox failure asynchronously.
        deadline = time.monotonic() + 60
        while True:
            failures = [
                message
                for pod, message in self._control.pod_sandbox_failures_since(since)
                if pod not in existing
            ]
            if any("sandbox parent cgroup" in m for m in failures) or time.monotonic() >= deadline:
                break
            self._fixture.wait(2)
        self._fixture.record(f"{label}_pod_sandbox_failures", [m[:600] for m in failures[-5:]])
        self._fixture.verify(
            any("sandbox parent cgroup" in message for message in failures)
            and not any("above total memory" in message for message in failures),
            f"{label}: no pod sandbox failure names the parent cgroup limit alone",
            failures=[m[:600] for m in failures[-5:]],
        )

    def _sandbox_cgroups(self, node: str) -> set[str]:
        return {s.name for s in self._control.read_sandbox_parent_memory(node).sandboxes}

    def hide_parent_stat(self) -> None:
        for node in self._workers:
            self._control.hide_parent_memory_stat(node)

    def restore(self) -> None:
        """Put every limit back, verify it, sweep the probes, and attach node-manager's logs."""

        if self._restore is not None:
            restore, self._restore = self._restore, None
            restore.release()
        standing = self._standing
        for node in self._workers:
            memory = self._control.read_sandbox_parent_memory(node)
            value = "max" if memory.parent.maximum is None else str(memory.parent.maximum)
            self._fixture.verify(
                value == standing[node],
                f"{node}'s parent memory.max was not restored",
                restored=value,
                standing=standing[node],
            )
            # A surviving bind would leave node-manager on the host-only
            # fallback for every later scenario.
            self._fixture.verify(
                self._control.parent_memory_stat_visible(node),
                f"node-manager still cannot read {node}'s parent memory.stat",
                node=node,
            )
        # Every probe may already be gone; the deferred sweep still covers a failed run.
        self._probe.kill_matching(self._metadata, absent_ok=True)
        if self._sweep is not None:
            self._sweep.disarm()
        self._fixture.record(
            "node_manager_parent_cgroup_warnings",
            {
                node: [
                    line[:400]
                    for line in self._control.node_manager_logs_since(
                        node, self._started_at
                    ).splitlines()
                    if "sandbox parent cgroup" in line
                ][-5:]
                for node in self._workers
            },
        )
        if self._release_lease is not None:
            release, self._release_lease = self._release_lease, None
            release.release()

    def _create(self, *, expect_admitted: bool, reason: str) -> str:
        started = time.monotonic()
        outcome = self._probe.race_ecw_request(
            "POST",
            "/sandboxes",
            {
                "templateID": self._fixture.sandbox_template_name(),
                "timeout": PROBE_TIMEOUT_SECONDS,
                "metadata": dict(self._metadata),
            },
            client_id="space-smoke",
            timeout=CREATE_TIMEOUT_SECONDS,
        )
        evidence = {
            "status": outcome.status,
            "message": outcome.message,
            "sandbox_id": outcome.sandbox_id,
            "seconds": round(time.monotonic() - started, 1),
        }
        self._fixture.record(f"create_{reason}", evidence)
        if expect_admitted:
            self._fixture.verify(
                outcome.status == 201 and outcome.sandbox_id is not None,
                f"{reason}: the create was refused",
                outcome=evidence,
            )
            assert outcome.sandbox_id is not None
            return outcome.sandbox_id
        self._fixture.verify(
            outcome.status != 201 and outcome.status != 0,
            f"{reason}: the create was not refused",
            outcome=evidence,
        )
        return ""

    def _wait_running(self, sandbox_id: str, timeout: float = 180) -> str:
        deadline = time.monotonic() + timeout
        while True:
            phase, node = self._control.sandbox_pod_phase_and_node(sandbox_id)
            if phase == "Running" and node:
                return node
            self._fixture.verify(
                time.monotonic() < deadline,
                "an admitted sandbox's pod never ran",
                sandbox_id=sandbox_id,
                phase=phase,
                node=node,
            )
            self._fixture.wait(2)
