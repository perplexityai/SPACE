"""A v2 drain with a hold keeps its running sandboxes serving until its deadline, which a repeat can only bring earlier."""

import time
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox
from space_smoke.drain_v2 import (
    DRAIN_HOLD_FIELD,
    LENGTHENED_DEADLINE_STATUS,
    MODE_WITH_V2_STATUS,
)

PATH = "/tmp/space-smoke-v2-held.txt"
# Long enough for the idle sandbox to be seen held and answering under the
# drain (its last command is the file read there), and well under the bench's
# 120 s eviction floor (`minIdleSecs` in space-sandbox-manager-local's
# values.yaml), which counts from that same command: the idle suspend lands
# at about 50 s, so its upload and the next report have more than a minute
# before eviction may drop the node's rows, where an idle policy equal to the
# floor left them one 15 s eviction interval.
IDLE_TIMEOUT = "45s"
IDLE_SECONDS = 45
# The first hold: the drain must not reach it.
LONG_HOLD_SECONDS = 60 * 60
# How far ahead of the repeat the shortened deadline lands: a hold that only
# has to pass, with the admin view showing it within a couple of the
# controller's 5 s ticks and the held sandbox answering once before it.
SHORT_AHEAD_SECONDS = 30
# The paused resident's suspend and upload, from the seal: a report (62 s,
# as below) and the work, under 5 s measured, four times over, with a 30 s
# margin.
DURABLE_SECONDS = 62 + 4 * 5 + 30
# The idle suspend, from its last command: the idle policy, the node's 5 s
# auto-lifecycle sweep, the suspend and upload (under 30 s measured) and a
# 3 s poll, with a 30 s margin.
IDLE_SUSPENDED_SECONDS = IDLE_SECONDS + 5 + 30 + 3 + 30
# The next report after the row reads suspended: a 5 s controller tick, a
# call within its 45 s budget, and a 2 s poll, with a 10 s margin. Inside
# the minute eviction leaves it.
REPORTED_SECONDS = 5 + 45 + 2 + 10


# The hold's parts are crate tests: the planner's hold of a started row
# until a call evacuates, and its suspend from then on
# (`service/drain_node/tests.rs`); the loop's `evacuate` from the stored
# deadline (`node_drain/v2/tests.rs`); the admin route's deadline arithmetic
# and its 409 and 400 (`nodes/drain_handler_tests.rs`); and the user gate
# before the deadline (`drain_guard_postgres_tests.rs`). This scenario is the
# seam on real workers: a held sandbox serving on a sealed worker, an idle
# suspend going ahead there and reported like any other, a repeat that
# brings the deadline earlier, and the node suspending what it held once
# the controller's call past that deadline says to evacuate.
@smoke.scenario(
    "native.v2_drain_holds_running_until_its_deadline",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # A paused resident's suspend and its wake elsewhere, an idle suspend,
    # the shortened hold, a suspend at the deadline and two wakes, on a slow
    # KVM bench.
    timeout_minutes=45,
    tier="manual",
    manual_reason="timing-sensitive on Kind: an idle suspend must be reported before the bench's short eviction floor drops its local rows",
)
def v2_drain_holds_running_until_its_deadline(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` with an hour's hold of a worker holding two running
    sandboxes, one with a short idle policy, and a paused one seals the
    worker, and the admin view shows that deadline. The running
    sandboxes are held there and answer calls; the paused one is made
    durable at once, and a wake lands it on the other worker. The idle one
    suspends on its own on the sealed worker, and the node reports it
    evacuated. A repeat `:drain` with a longer hold is a 409 and one naming a
    `mode` a 400, each leaving the deadline as it was; one with a shorter
    hold, counted from the drain's start, moves the deadline earlier. The
    held sandbox keeps running and answering until that deadline; past it
    the node suspends it, the drain reaches `drained` with its `drain_mode`
    unchanged, and a wake lands each on the other worker with its file."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin two running sandboxes, one with an idle policy, and a paused one to one worker"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            idle = drain.create_resident(idle_timeout=IDLE_TIMEOUT)
            busy = drain.create_resident()
            paused = drain.create_resident()
            for sandbox in (idle, busy, paused):
                drain.write_file(sandbox, PATH)
            cast(ResidentLifecycleSandbox, paused).pause()
            test.sandbox.suspend()
            drain.release_other_worker()
            # Activity restarts the idle clock, so the drain finds it running.
            drain.run_on(idle, "true")

        with test.phase("drain with an hour's hold: the node reports under the drain, showing the deadline"):
            drain.drain(hold=LONG_HOLD_SECONDS)
            drain.wait_for_reported()
            mode = drain.drain_mode()
            test.record("drain_mode_at_start", mode)

        with test.phase("the running sandboxes are held and answer"):
            for sandbox in (idle, busy):
                drain.expect_held(sandbox.sandbox_id)
                drain.expect_file(sandbox, PATH)

        with test.phase("the paused one is made durable at once; a wake lands it on the other worker"):
            drain.expect_durable(paused.sandbox_id, timeout=DURABLE_SECONDS)
            drain.wake_elsewhere(paused)
            drain.expect_file(paused, PATH)

        with test.phase("the idle one suspends on its own on the sealed worker and is reported evacuated"):
            drain.expect_lifecycle_back(idle.sandbox_id, "suspended", timeout=IDLE_SUSPENDED_SECONDS)
            # A report names it evacuated, and the controller keeps every
            # sandbox a report named in `evacuated_total`, which only a report
            # adds to before `drained`: read from either, so a report this
            # poll missed still counts, and one that never came does not.
            drain.wait_for_report(
                lambda report: drain.bare_id(idle.sandbox_id)
                in drain.evacuated_residents(report, [idle.sandbox_id]) | drain.evacuated_total(report),
                "the node never reported the self-suspended sandbox evacuated",
                timeout=REPORTED_SECONDS,
            )
            drain.expect_held(busy.sandbox_id)

        with test.phase("a longer hold is a 409, and a mode a 400; the deadline stays"):
            before = drain.deadline
            longer = drain.repeat_drain(LONG_HOLD_SECONDS * 2)
            with_mode = drain.drain_answer(
                {"engine": "v2", "mode": "forced", DRAIN_HOLD_FIELD: LONG_HOLD_SECONDS}
            )
            test.verify(
                longer.status == LENGTHENED_DEADLINE_STATUS and with_mode.status == MODE_WITH_V2_STATUS,
                "a repeat :drain that lengthens the hold, or names a mode, was not refused",
                longer=longer.status,
                longer_body=longer.body,
                with_mode=with_mode.status,
                with_mode_body=with_mode.body,
            )
            test.verify(
                drain.expect_stored_deadline(LONG_HOLD_SECONDS) == before,
                "a refused repeat :drain moved the deadline",
            )

        with test.phase("a shorter hold moves the deadline earlier"):
            # A hold counts from `drain_started_at`, which a repeat keeps.
            shorter = drain.repeat_drain(
                int(time.time() - drain.started_at()) + SHORT_AHEAD_SECONDS
            )
            test.verify(
                shorter.status in (200, 202) and drain.deadline < before,
                "a repeat :drain with a shorter hold did not move the deadline earlier",
                status=shorter.status,
                body=shorter.body,
                deadline=drain.deadline,
                before=before,
            )
            report = drain.wait_for_report(
                lambda report: (shown := drain.view_deadline(report)) is not None
                and shown < before - SHORT_AHEAD_SECONDS,
                "the admin view never showed the earlier deadline",
                timeout=20,
            )
            drain.expect_view_deadline(report)

        with test.phase("the held sandbox runs and answers until the deadline"):
            drain.expect_file(busy, PATH)
            drain.expect_started_until_deadline([busy.sandbox_id], margin=3)

        with test.phase("past the deadline the node suspends it; the drain drains, its mode unchanged"):
            drain.wait_past_deadline()
            drain.expect_suspended_by_the_node(busy.sandbox_id)
            report = drain.expect_drained()
            # The paused one was woken elsewhere before the drain ended.
            drain.expect_evacuated(report, [busy.sandbox_id, idle.sandbox_id])
            test.verify(
                drain.drain_mode() == mode,
                "the drain's mode changed at its deadline",
                drain_mode=drain.drain_mode(),
                at_start=mode,
            )

        with test.phase("a wake lands each held sandbox on the other worker, its file intact"):
            for sandbox in (busy, idle):
                drain.expect_suspended_unplaced(sandbox.sandbox_id)
                drain.wake_elsewhere(sandbox)
                drain.expect_file(sandbox, PATH)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
