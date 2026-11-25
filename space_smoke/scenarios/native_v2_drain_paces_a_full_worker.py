"""A v2 drain of a full worker moves every resident, a window at a time, without blocking or stalling."""

import time
from typing import Any

import space_smoke as smoke
from space_smoke.drain_v2 import STALL_ERROR_PREFIX, in_parallel

RESIDENTS = 30
# More than the window: the window starts paused VMs first, so only with more
# paused residents than its slots does one wait for a slot in it.
PAUSED = 10
# Residents the node suspends itself once idle: their idle suspends run
# through the auto-lifecycle scheduler beside the drain's own.
IDLE = 8
IDLE_TIMEOUT = "90s"
# Guest memory each resident fills, so its snapshot carries real bytes.
FILL_MIB = 256
SAMPLE_SECONDS = 3
# Thirty residents' suspends and uploads, a window at a time, to `drained`:
# about 80 s measured from the drain, four times over, with a minute's
# margin.
DRAINED_SECONDS = 4 * 80 + 60
# One pass over thirty residents' rows and snapshots takes about a minute
# of reads; two passes and a margin.
EVACUATED_SECONDS = 180


# The window, the disk check and the upload polling are planner rows
# (`service/drain_node/tests.rs`). This scenario is the load on real workers:
# a worker holding thirty residents with memory in use, some paused and some
# idle-suspending on their own, drained with engine v2, due now. It samples
# the node's report and every resident's lifecycle while the drain runs, so a
# run records how many residents were moving, held or waiting on outside
# work at each sample, and it verifies that nothing was blocked and the
# drain never stalled, and that a wake of a resident still waiting for a
# slot in the window is told to retry, never refused with a 409. The
# report's coarse wait doesn't say which moves are running, so the window's
# bound is the planner's test.
@smoke.scenario(
    "native.v2_drain_paces_a_full_worker",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Thirty creates, thirty suspends and uploads a window at a time, and
    # thirty wakes, on a slow KVM bench.
    timeout_minutes=150,
    tier="manual",
    manual_reason="a thirty-sandbox drain measured end to end, too slow for the per-merge set",
)
def v2_drain_paces_a_full_worker(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain`, due now, of a worker holding thirty residents with
    memory in use, ten paused and eight idle-suspending, reports nothing
    blocked, is never marked stalled, and reaches `drained`. A wake of a
    paused resident still waiting for a slot in the window gets the drain's
    retryable 503, never a 409, and lands on the other worker once the drain
    has made it durable. Every resident is left suspended and unplaced, and a
    wake lands each on the other worker with its memory intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("fill one worker with thirty residents"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            residents = drain.create_residents(RESIDENTS - IDLE)
            idle = drain.create_residents(IDLE, idle_timeout=IDLE_TIMEOUT)
            digests = drain.fill_memories(residents + idle, mib=FILL_MIB)
            drain.pause_all(residents[:PAUSED])
            test.sandbox.suspend()
            drain.release_other_worker()
            # Activity resets the idle clock, so the idle residents suspend
            # themselves while the drain runs.
            in_parallel([lambda sandbox=sandbox: drain.run_on(sandbox, "true") for sandbox in idle])

        everyone = residents + idle
        ids = [drain.bare_id(sandbox.sandbox_id) for sandbox in everyone]
        by_id = {drain.bare_id(sandbox.sandbox_id): sandbox for sandbox in everyone}
        paused = {drain.bare_id(sandbox.sandbox_id) for sandbox in residents[:PAUSED]}
        samples: list[dict[str, Any]] = []

        def sample(started: float) -> dict[str, Any]:
            report = drain.report()
            waits: dict[str, int] = {}
            for wait in drain.pending_waits(report).values():
                waits[wait] = waits.get(wait, 0) + 1
            entry = {
                "t": round(time.monotonic() - started, 1),
                "pending": waits,
                "blocked": len(report.get("blocked", [])),
                "evacuated": len(report.get("evacuated", [])),
                "stalled": drain.last_error(),
                "lifecycles": drain.placements(ids),
            }
            samples.append(entry)
            return entry

        # A paused resident the window took is no longer `paused` on the
        # worker; one still waiting for a slot is.
        waiting: list[str] = []

        def a_paused_one_waits(report: dict[str, Any]) -> bool:
            if not drain.reported(report):
                return False
            pending = set(drain.pending_waits(report)) & paused
            states = drain.node_states([by_id[sandbox_id].sandbox_id for sandbox_id in sorted(pending)])
            waiting[:] = sorted(sandbox_id for sandbox_id, state in states.items() if state == "paused")
            return bool(waiting)

        with test.phase("drain; a wake of a paused resident waiting for the window is told to retry"):
            started = time.monotonic()
            drain.drain()
            drain.wait_for_report(
                a_paused_one_waits,
                "no paused resident ever waited for a slot in the window",
                timeout=180,
            )
            queued = waiting[-1]
            test.record("woken_while_queued", queued)
            answer = drain.request_activation(by_id[queued])
            drain.expect_retried_never_conflict(answer)
            drain.expect_back_elsewhere(by_id[queued].sandbox_id, "started")

        with test.phase("sample the drain until drained"):
            deadline = started + DRAINED_SECONDS
            while drain.drain_phase() != "drained":
                sample(started)
                test.verify(time.monotonic() < deadline, "the v2 drain never reached drained")
                test.wait(SAMPLE_SECONDS)
            report = drain.expect_drained()
            test.record("drained_seconds", round(time.monotonic() - started, 1))

        timeline = [
            {key: value for key, value in entry.items() if key != "lifecycles"} for entry in samples
        ]
        test.record("timeline", timeline)
        peak = max((s["pending"].get("moving", 0) for s in samples), default=0)
        test.record("peak_moving", peak)
        blocked = max((s["blocked"] for s in samples), default=0)
        test.record("peak_blocked", blocked)
        stalled = [s["stalled"] for s in samples if s["stalled"] and s["stalled"].startswith(STALL_ERROR_PREFIX)]

        with test.phase("nothing was blocked, and the drain never stalled"):
            test.verify(blocked == 0, "the drain reported something blocked", peak_blocked=blocked)
            test.verify(not stalled, "the drain was marked stalled", stalled=stalled[:3])
            drain.expect_not_stalled()

        with test.phase("every other resident is suspended and unplaced; a wake lands each elsewhere"):
            rest = {
                sandbox.sandbox_id: sandbox
                for sandbox in everyone
                if drain.bare_id(sandbox.sandbox_id) != queued
            }
            drain.expect_all_evacuated(rest, timeout=EVACUATED_SECONDS)
            drain.wake_all_elsewhere(rest, digests)
            drain.expect_memory(by_id[queued], digests[by_id[queued].sandbox_id])

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()

        # Thirty deletes outlast the cleanup budget, so the run deletes its
        # residents itself; cleanup deletes only what this leaves.
        with test.phase("delete the residents"):
            drain.delete_all(everyone)
