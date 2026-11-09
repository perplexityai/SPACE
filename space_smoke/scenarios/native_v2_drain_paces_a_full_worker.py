"""A v2 drain of a full worker suspends its residents a window at a time."""

import time
from typing import Any, cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox

RESIDENTS = 30
PAUSED = 6
# Residents the node suspends itself once idle: their idle suspends run
# through the auto-lifecycle scheduler beside the drain's own.
IDLE = 8
IDLE_TIMEOUT = "90s"
# Guest memory each resident fills, so its snapshot carries real bytes.
FILL_MIB = 256
# `SPACE_DRAIN_NODE_WINDOW`'s default.
WINDOW = 8
SAMPLE_SECONDS = 3


# The window, the disk check and the upload polling are planner rows
# (`service/drain_node/tests.rs`). This scenario is the load on real workers:
# a worker holding thirty residents with memory in use, some paused and some
# idle-suspending on their own, drained with engine v2. It samples the node's
# report and every resident's lifecycle while the drain runs, so a run records
# how many suspends ran at once and how long each resident was down, and it
# verifies that no more than the window's suspends ran at once and that
# nothing blocked.
@smoke.scenario(
    "native.v2_drain_paces_a_full_worker",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Thirty creates, thirty suspends and uploads a window at a time, and
    # thirty restores four at a time, on a slow KVM bench.
    timeout_minutes=150,
    ignored="a thirty-sandbox drain measured end to end, too slow for the per-merge set",
)
def v2_drain_paces_a_full_worker(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` of a worker holding thirty residents with memory in
    use, six paused and eight idle-suspending, never runs more than the
    window's suspends at once, blocks nothing, reaches `drained`, and every
    resident comes back in its lifecycle on the other worker."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("fill one worker with thirty residents"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            residents = [drain.create_resident() for _ in range(RESIDENTS - IDLE)]
            idle = [drain.create_resident(idle_timeout=IDLE_TIMEOUT) for _ in range(IDLE)]
            fill = f"head -c {FILL_MIB}M /dev/urandom > /dev/shm/space-smoke-fill && echo ok"
            for sandbox in residents + idle:
                filled = drain.run_on(sandbox, fill)
                test.verify(
                    filled.exit_code == 0,
                    "a resident could not fill its memory",
                    sandbox_id=sandbox.sandbox_id,
                )
            for sandbox in residents[:PAUSED]:
                cast(ResidentLifecycleSandbox, sandbox).pause()
            test.sandbox.suspend()
            drain.release_other_worker()
            # Activity resets the idle clock, so the idle residents suspend
            # themselves while the drain runs.
            for sandbox in idle:
                drain.run_on(sandbox, "true")

        everyone = residents + idle
        ids = [drain.bare_id(sandbox.sandbox_id) for sandbox in everyone]
        paused = {drain.bare_id(sandbox.sandbox_id) for sandbox in residents[:PAUSED]}
        idle_ids = {drain.bare_id(sandbox.sandbox_id) for sandbox in idle}
        samples: list[dict[str, Any]] = []

        def sample(started: float) -> dict[str, Any]:
            report = drain.report()
            actions: dict[str, int] = {}
            for entry in report.get("pending", []):
                action = str(entry.get("action"))
                actions[action] = actions.get(action, 0) + 1
            placements = drain.placements(ids)
            entry = {
                "t": round(time.monotonic() - started, 1),
                "pending": actions,
                "blocked": len(report.get("blocked", [])),
                "evacuated": len(report.get("evacuated", [])),
                "lifecycles": {sandbox_id: placements[sandbox_id] for sandbox_id in ids},
            }
            samples.append(entry)
            return entry

        with test.phase("drain the worker with engine v2 and sample it until drained"):
            started = time.monotonic()
            drain.drain()
            deadline = started + 3600
            while drain.drain_phase() != "drained":
                sample(started)
                test.verify(time.monotonic() < deadline, "the v2 drain never reached drained")
                test.wait(SAMPLE_SECONDS)
            drained_at = round(time.monotonic() - started, 1)
            report = drain.report()
            test.record("drained_seconds", drained_at)
            test.record("drained_report_blocked", report.get("blocked", []))

        with test.phase("every resident comes back on the other worker"):
            deadline = time.monotonic() + 3600
            while True:
                entry = sample(started)
                back = [
                    sandbox_id
                    for sandbox_id, (lifecycle, node) in entry["lifecycles"].items()
                    if node not in (None, drain.node_id)
                    and lifecycle == ("paused" if sandbox_id in paused else "started")
                ]
                # A resident idle long enough suspends itself, on either
                # worker: one the node's idle scheduler reached before the
                # window did has nothing to restore, and one restored can
                # suspend itself again. Either is settled once no drain
                # restore of it waits or runs.
                suspended = [
                    sandbox_id
                    for sandbox_id, (lifecycle, _node) in entry["lifecycles"].items()
                    if lifecycle == "suspended" and sandbox_id not in back
                ]
                settled = [
                    sandbox_id
                    for sandbox_id in suspended
                    if all(
                        state not in ("queued", "running")
                        for state, _attempts in drain.drain_restores(sandbox_id)
                    )
                ] if len(back) + len(suspended) == len(ids) else []
                if len(back) + len(settled) == len(ids):
                    test.record("settled_suspended", sorted(settled))
                    break
                test.verify(
                    time.monotonic() < deadline,
                    "the residents never all came back",
                    back=len(back),
                    lifecycles=entry["lifecycles"],
                )
                test.wait(SAMPLE_SECONDS)
            test.record("restored_seconds", round(time.monotonic() - started, 1))

        timeline = [
            {key: value for key, value in entry.items() if key != "lifecycles"} for entry in samples
        ]
        test.record("timeline", timeline)
        down: dict[str, float] = {}
        for sandbox_id in ids:
            if sandbox_id in idle_ids:
                continue
            serving = "paused" if sandbox_id in paused else "started"
            went = next(
                (s["t"] for s in samples if s["lifecycles"][sandbox_id][0] != serving), None
            )
            back_at = next(
                (
                    s["t"]
                    for s in samples
                    if went is not None
                    and s["t"] > went
                    and s["lifecycles"][sandbox_id][0] == serving
                    and s["lifecycles"][sandbox_id][1] not in (None, drain.node_id)
                ),
                None,
            )
            if went is not None and back_at is not None:
                down[sandbox_id] = round(back_at - went, 1)
        test.record("downtime_seconds", down)
        peak = max((s["pending"].get("suspend", 0) for s in samples), default=0)
        test.record("peak_concurrent_suspends", peak)
        blocked = max((s["blocked"] for s in samples), default=0)
        test.record("peak_blocked", blocked)

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()

        # Thirty deletes outlast the cleanup budget, so the run deletes its
        # residents itself; cleanup deletes only what this leaves.
        with test.phase("delete the residents"):
            for sandbox in everyone:
                test.delete_sandbox(sandbox)

        test.verify(blocked == 0, "the drain reported a resident blocked", peak_blocked=blocked)
        test.verify(
            peak <= WINDOW,
            "the drain ran more suspends at once than its window",
            peak=peak,
            window=WINDOW,
        )
