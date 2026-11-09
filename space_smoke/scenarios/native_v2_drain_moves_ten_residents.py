"""A v2 drain of ten residents, more than the drain restore budget, brings each back in its lifecycle."""

from datetime import UTC, datetime
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import (
    DurableLifecycleSandbox,
    ResidentLifecycleSandbox,
    StoppableLifecycleSandbox,
)

# Six started, three paused and one stopped: more suspended residents than
# the dispatcher's four drain restore slots, so restores queue behind each
# other.
RESIDENTS = (("started", 6), ("paused", 3), ("stopped", 1))
# Guest memory each VM holds.
BALLAST_MIB = 128


def drain_ten_residents(test: smoke.CommandSandboxFixture, *, restore_slots: int | None) -> None:
    """The body, shared with runs that change the drain restore budget."""

    drain = test.drain_v2

    with drain.v2_controller(restore_slots=restore_slots):
        with test.phase("pin ten residents to one worker, each VM holding memory"):
            # A v2 drain needs the node's instance recorded; re-registering
            # under a fake one deletes the pods there, so the fixture's
            # worker is left alone.
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            residents: list[tuple[str, DurableLifecycleSandbox]] = [
                (lifecycle, drain.create_resident())
                for lifecycle, count in RESIDENTS
                for _ in range(count)
            ]
            digests = {
                sandbox.sandbox_id: drain.fill_memory(sandbox, mib=BALLAST_MIB)
                for _, sandbox in residents
            }
            for lifecycle, sandbox in residents:
                if lifecycle == "paused":
                    cast(ResidentLifecycleSandbox, sandbox).pause()
                elif lifecycle == "stopped":
                    cast(StoppableLifecycleSandbox, sandbox).stop()
            test.record(
                "residents",
                {drain.bare_id(sandbox.sandbox_id): lifecycle for lifecycle, sandbox in residents},
            )
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain the worker with engine v2"):
            drain.expect_lifecycles(
                {sandbox.sandbox_id: lifecycle for lifecycle, sandbox in residents}
            )
            drain.drain()

        with test.phase("every resident is reported evacuated with its lifecycle; the node drains"):
            first_reported: dict[str, float] = {}
            report = drain.expect_drained(timeout=1800, first_reported=first_reported)
            test.record(
                "first_reported_evacuated",
                {
                    sandbox_id: datetime.fromtimestamp(at, UTC).isoformat()
                    for sandbox_id, at in first_reported.items()
                },
            )
            drain.expect_evacuated(
                report, {sandbox.sandbox_id: lifecycle for lifecycle, sandbox in residents}
            )

        with test.phase("each suspended resident is back, restored once, with one VM elsewhere"):
            for lifecycle, sandbox in residents:
                if lifecycle == "stopped":
                    continue
                drain.expect_back_elsewhere(sandbox.sandbox_id, lifecycle, timeout=1200)
                drain.expect_restored_once(sandbox.sandbox_id)
                drain.expect_single_vm(sandbox.sandbox_id, lifecycle)

        with test.phase("the stopped resident is stopped and unplaced, with no VM"):
            for lifecycle, sandbox in residents:
                if lifecycle == "stopped":
                    drain.expect_stopped_unplaced(sandbox.sandbox_id)
                    drain.expect_single_vm(sandbox.sandbox_id, "stopped")

        with test.phase("each started resident's memory survived"):
            for lifecycle, sandbox in residents:
                if lifecycle != "started":
                    continue
                drain.expect_memory(sandbox, digests[sandbox.sandbox_id])
            test.sandbox.wake()

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()


# `native.v2_drain_evacuates_every_resident` is each lifecycle once. This is
# the drain at width: more suspended residents than the drain restore
# budget (`SPACE_PUBLIC_OPERATION_DISPATCHER_DRAIN_RESTORE_SLOTS`, four by
# default), each VM holding memory, so the restores queue for their slots.
# Its evidence records when the controller first saw each resident reported
# evacuated; the rest of each resident's timeline is in its operation and
# snapshot rows.
@smoke.scenario(
    "native.v2_drain_moves_ten_residents",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Ten residents' creates, nine suspends and uploads, nine restores in
    # three waves, on a slow KVM bench.
    timeout_minutes=75,
    ignored="ten resident VMs on one worker, too heavy for the per-merge set",
)
def v2_drain_moves_ten_residents(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` of a worker holding six started, three paused and one
    stopped sandbox, each VM holding random memory, reports each evacuated
    with its lifecycle and reaches `drained`. One drain restore each, on its
    first attempt, brings the nine suspended back in their lifecycles on the
    other worker, with no VM left on the drained one; the stopped one stays
    stopped and unplaced. Each started resident's memory is intact, and
    `:undrain` cancels the epoch."""

    drain_ten_residents(test, restore_slots=None)
