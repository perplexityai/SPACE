"""Bounded quarantine recovery, proven against the real node-local loop.

`smoke-kind-quarantine-recovery.sh` existed for one behaviour no unit test can
show end to end: a quarantined sandbox with no central recovery plan is retried
on an exponential backoff rather than re-selected on every 60 s reconciler tick
for ever. This module is its typed form. The quarantine is induced the way the
real failure produces it — a sandbox cache directory whose name parses as a
sandbox id, holding a `snapshots/.suspend-publish-*` staging directory and no
metadata row, which the inventory reconciler classifies as unsafe evidence and
materialises as a quarantined row with reason `orphaned_sandbox_row_missing`.
Nothing writes a database; the only injected state is a directory.

Two orphans, not one: the reconciler handles both in one pass, so it costs no
extra wall clock and shows a cohort larger than one is claimed and paced without
one episode starving another. Every episode legitimately makes no progress
(there is no plan), so each pass charges one attempt and schedules the next
with backoff; the suite reads the `RetryScheduled` no-progress records out of
the sandbox-manager log and judges the cadence with `judge_cadence`.

The log lives and dies with the worker's sandbox-manager pod, so that pod is
pinned by uid and a replacement mid-run is a refusal to judge, not a verdict. Cleanup retires each
induced row through the production on-node path (`space-sbxmgr delete`) before
removing the directory; leaving either would quarantine-poison the next smoke.

Not ported, recorded in the coverage map with the shell suite's reasoning: a
sandbox that actually recovers (no writer in the tree produces the bookkeeping
state that makes a failed restore automatically recoverable) and backoff
durability across a service restart (the log the cadence is read from dies with
the sandbox-manager pod).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from space_smoke.capabilities import QuarantineRecoveryControl, RecoveryAttempt
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

RECONCILER_TICK_SECONDS = 60.0
QUARANTINE_REASON = "orphaned_sandbox_row_missing"


@dataclass(frozen=True)
class CadenceVerdict:
    numbers: tuple[int, ...]
    gaps_seconds: tuple[float, ...]
    failure: str | None


def judge_cadence(attempts: Sequence[RecoveryAttempt], *, want: int) -> CadenceVerdict:
    """The shell suite's oracle, unchanged in substance.

    Attempts are only taken on a reconciler tick, so a gap is the backoff rounded
    up to the next tick; backoff(1) is 48–60 s and backoff(2) 96–120 s, and a
    first pass that burns its plan-lookup timeout can push both gaps to 120 s, so
    a delta between adjacent gaps is the wrong oracle. Compare against a floor:
    doubling puts backoff(2) at 96 s at the least, so the last gap of a
    three-attempt run clears 1.5 ticks however the jitter falls, while the flat
    once-per-tick cadence this exists to catch sits at 60 s and fails it.
    """

    ordered = sorted(attempts, key=lambda a: a.attempt)
    numbers = tuple(a.attempt for a in ordered)
    gaps = tuple(
        round(ordered[i + 1].at - ordered[i].at, 1) for i in range(len(ordered) - 1)
    )
    if len(numbers) < want:
        return CadenceVerdict(
            numbers,
            gaps,
            f"observed {len(numbers)} recovery attempts, wanted {want}; a build without "
            "bounded polling records no attempts at all",
        )
    if numbers != tuple(range(1, len(numbers) + 1)):
        return CadenceVerdict(numbers, gaps, "attempt numbers are not a contiguous 1..N sequence")
    floor = 1.5 * RECONCILER_TICK_SECONDS
    if len(gaps) >= 2 and gaps[-1] < floor:
        return CadenceVerdict(
            numbers,
            gaps,
            f"the last retry interval did not clear the backoff floor ({gaps[-1]:.0f}s < {floor:.0f}s); "
            "bounded polling must space successive attempts further apart, not retry at a fixed rate",
        )
    if gaps and max(gaps) < 90:
        return CadenceVerdict(
            numbers,
            gaps,
            f"every retry landed within one reconciler tick (max gap {max(gaps):.0f}s); "
            "that is the unbounded cadence this scenario exists to catch",
        )
    return CadenceVerdict(numbers, gaps, None)


class QuarantineRecoveryAssertions:
    """Induce orphaned suspend publications on the fixture's worker and judge the retry cadence."""

    def __init__(
        self, fixture: KindEcw, control: QuarantineRecoveryControl, scope: ResourceScope
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._node: str | None = None
        self._pod: str | None = None
        self._pod_uid: str | None = None
        self._orphans: list[str] = []
        self._removals: list[Compensation] = []
        self._attempts: dict[str, list[RecoveryAttempt]] = {}

    # --- induce -----------------------------------------------------------------

    def induce(self, *, count: int = 2) -> list[str]:
        """Plant `count` interrupted suspend publications on the worker running this sandbox.

        The removal is armed before each plant, so a plant that half-succeeds is
        still retired at scope end.
        """

        node, pod, uid = self._control.sandbox_manager_for_sandbox(
            self._fixture.sandbox.sandbox_id
        )
        self._node, self._pod, self._pod_uid = node, pod, uid
        self._fixture.record("worker_node", node)
        self._fixture.record("sandbox_manager_pod", pod)
        for _ in range(count):
            orphan = str(uuid.uuid4())
            self._removals.append(
                self._scope.defer(
                    f"retire the induced orphan {orphan[:8]}",
                    lambda orphan=orphan: self._remove(orphan),
                    critical=True,
                )
            )
            self._control.plant_orphan_staging(node, orphan)
            self._orphans.append(orphan)
        self._fixture.record("induced_orphans", list(self._orphans))
        self._assert_same_pod()
        return list(self._orphans)

    # --- observe ----------------------------------------------------------------

    def expect_quarantined(self, *, timeout: float = 300) -> None:
        """Every induced orphan is quarantined with `orphaned_sandbox_row_missing` (tick is 60 s)."""

        pod = self._require_pod()
        deadline = time.monotonic() + timeout
        while True:
            self._assert_same_pod()
            seen = [o for o in self._orphans if self._control.orphan_quarantined(pod, o)]
            if len(seen) == len(self._orphans):
                self._fixture.record("quarantine_reason", QUARANTINE_REASON)
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "not every induced orphan was quarantined within the budget; the orphan sweep "
                "did not classify the injected staging residue as unsafe evidence",
                quarantined=len(seen),
                induced=len(self._orphans),
            )
            self._fixture.wait(10)

    def expect_attempts(self, *, want: int = 3, timeout: float = 900) -> None:
        """Every episode records at least `want` `RetryScheduled` attempts on its own.

        A collection timeout is its own outcome: zero everywhere is what a build
        without bounded polling looks like, a partial count means the stack ran
        slower than the budget — neither is a cadence verdict.
        """

        pod = self._require_pod()
        deadline = time.monotonic() + timeout
        while True:
            self._assert_same_pod()
            for orphan in self._orphans:
                self._attempts[orphan] = self._control.recovery_attempts(pod, orphan)
            counts = {o[:8]: len(self._attempts[o]) for o in self._orphans}
            if all(n >= want for n in counts.values()):
                self._fixture.record("attempts_observed", counts)
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the attempt budget expired before every episode recorded the wanted attempts "
                "(a collection timeout, not a cadence verdict)",
                attempts_observed=counts,
                wanted=want,
            )
            self._fixture.wait(20)

    def expect_backoff(self, *, want: int = 3) -> None:
        """Each episode's attempts are contiguous and spaced by a growing backoff."""

        cadence: dict[str, dict[str, object]] = {}
        for orphan in self._orphans:
            verdict = judge_cadence(self._attempts.get(orphan, []), want=want)
            cadence[orphan[:8]] = {"attempts": verdict.numbers, "gaps_seconds": verdict.gaps_seconds}
            self._fixture.verify(
                verdict.failure is None,
                f"episode {orphan[:8]}: {verdict.failure or ''}",
                attempts=verdict.numbers,
                gaps_seconds=verdict.gaps_seconds,
            )
        self._fixture.record("cadence", cadence)

    # --- teardown ---------------------------------------------------------------

    def retire(self) -> None:
        """Retire every induced row and directory now; the scope no longer needs to."""

        removals, self._removals = self._removals, []
        for compensation in removals:
            compensation.release()
        residue = [o for o in self._orphans if self._node and not self._control.orphan_absent(self._node, o)]
        self._fixture.verify(
            not residue,
            "an induced sandbox directory survived its retirement",
            residue=residue,
        )

    def _remove(self, orphan: str) -> None:
        self._require_pod()
        assert self._node is not None
        self._control.remove_orphan_sandbox(self._node, orphan)

    def _require_pod(self) -> str:
        if self._pod is None:
            raise RuntimeError("induce() must run first")
        return self._pod

    def _assert_same_pod(self) -> None:
        pod = self._require_pod()
        current = self._control.pod_uid(pod)
        self._fixture.verify(
            current == self._pod_uid,
            "the sandbox-manager pod was replaced mid-run; the log the cadence is read from "
            "lives in that pod, so this run cannot be judged — re-run once it has settled",
            pod=pod,
            uid_before=self._pod_uid,
            uid_now=current,
        )
