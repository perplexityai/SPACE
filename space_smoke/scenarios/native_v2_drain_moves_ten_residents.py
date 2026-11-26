"""A v2 drain of ten residents leaves each durable and unplaced; a wake brings each back elsewhere, its memory intact."""

from datetime import UTC, datetime

import space_smoke as smoke
from space_smoke.capabilities import DurableLifecycleSandbox
from space_smoke.drain_v2 import REPORT_SECONDS

# Seven started and three paused: more suspends than one node's drain runs at
# once is `native.v2_drain_paces_a_full_worker`; here the point is each
# resident's memory across the drain and its wake. A v2 drain doesn't move a
# stopped sandbox, so none is a resident.
RESIDENTS = (("started", 7), ("paused", 3))
# Guest memory each VM holds.
BALLAST_MIB = 128
# The drain to `drained`, due now, by the waits' sizing rule
# (`space_smoke/drain_v2.py`): the next report, and the ten suspends and
# uploads (22 s measured) four times over, with a 30 s margin.
TEN_DRAINED_SECONDS = REPORT_SECONDS + 4 * 22 + 30


# `native.v2_drain_evacuates_every_resident` is each lifecycle once. This is
# the drain at width, each VM holding memory. Its evidence records when the
# controller first saw each resident reported evacuated; the rest of each
# resident's timeline is in its snapshot rows.
@smoke.scenario(
    "native.v2_drain_moves_ten_residents",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Ten residents' creates, ten suspends and uploads and ten wakes, on a
    # slow KVM bench.
    timeout_minutes=75,
    tier="manual",
    manual_reason="ten resident VMs on one worker, too heavy for the per-merge set",
)
def v2_drain_moves_ten_residents(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain`, due now, of a worker holding seven started and three
    paused sandboxes, each VM holding random memory, reports each evacuated
    with its snapshot and reaches `drained`. The ten are left suspended and
    unplaced, with no VM on either worker. A wake then lands each on the
    other worker with its memory intact, and `:undrain` cancels the drain."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin ten residents to one worker, each VM holding memory"):
            # A v2 drain needs the node's instance recorded; re-registering
            # under a fake one deletes the pods there, so the fixture's
            # worker is left alone.
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            wanted = [lifecycle for lifecycle, count in RESIDENTS for _ in range(count)]
            residents: list[tuple[str, DurableLifecycleSandbox]] = list(
                zip(wanted, drain.create_residents(len(wanted)), strict=True)
            )
            digests = drain.fill_memories([sandbox for _, sandbox in residents], mib=BALLAST_MIB)
            drain.pause_all(sandbox for lifecycle, sandbox in residents if lifecycle == "paused")
            lifecycles = {sandbox.sandbox_id: lifecycle for lifecycle, sandbox in residents}
            test.record(
                "residents",
                {drain.bare_id(sandbox_id): lifecycle for sandbox_id, lifecycle in lifecycles.items()},
            )
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain the worker with engine v2, due now"):
            drain.expect_lifecycles(lifecycles)
            drain.drain()

        with test.phase("every resident is reported evacuated with its snapshot; the node drains"):
            first_reported: dict[str, float] = {}
            report = drain.expect_drained(timeout=TEN_DRAINED_SECONDS, first_reported=first_reported)
            test.record(
                "first_reported_evacuated",
                {
                    sandbox_id: datetime.fromtimestamp(at, UTC).isoformat()
                    for sandbox_id, at in first_reported.items()
                },
            )
            drain.expect_evacuated(report, lifecycles)

        with test.phase("each resident is left suspended and unplaced"):
            drain.expect_all_evacuated(lifecycles)

        with test.phase("a wake lands each on the other worker, its memory intact"):
            drain.wake_all_elsewhere({sandbox.sandbox_id: sandbox for _, sandbox in residents}, digests)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
