"""A gentle v2 drain leaves a running sandbox running until it goes idle, and turns forced at its deadline."""

import shlex
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox

PATH = "/tmp/space-smoke-v2-gentle-drain.txt"
# Long enough for the held sandbox to be seen running and answering under
# the drain; short enough that it suspends on its own inside the run.
IDLE_TIMEOUT = "120s"
# The first drain must not reach its deadline: it ends gently.
LONG_DEADLINE_SECONDS = 3 * 60 * 60
# The second drain's deadline: enough for its first call to hold the
# running sandbox before it.
SHORT_DEADLINE_SECONDS = 150


# The gentle drain's parts are crate tests: the planner's hold of a started
# row, its self-suspended row uploaded and never reported evacuated, and the
# refusal of a gentle request an older node reads as epoch 0
# (`service/drain_node/tests.rs`); the loop's gentle calls, the echo it
# requires and its turn to forced at the deadline (`node_drain/v2/tests.rs`)
# and the statements behind them (`node_drain/store/v2_sink/tests.rs`); the
# admin route's deadline (`nodes/drain_handler_tests.rs`); and the user gate
# under a sealed gentle node (`drain_guard_postgres_tests.rs`). This scenario
# is the seam on real workers: the node's idle auto-suspend going ahead on a
# sealed worker, the controller recording the moved sandbox and nothing for
# the self-suspended one, the drain gate letting that one's wake through
# once its snapshot is durable while the drain still runs, and the deadline
# turning the drain forced. The forced drain itself is
# `native.v2_drain_evacuates_every_resident`.
@smoke.scenario(
    "native.v2_drain_gentle_waits_for_running",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Two drains on a slow KVM bench: a paused resident's suspend and its
    # wake elsewhere, an idle
    # suspend and its wake elsewhere, a user suspend, the deadline, a held
    # resident's move, two undrains and wakes.
    timeout_minutes=60,
)
def v2_drain_gentle_waits_for_running(test: smoke.CommandSandboxFixture) -> None:
    """A gentle v2 `:drain`, with a deadline hours away, of a worker holding
    two running sandboxes, one with a short idle policy, and a paused one
    seals the worker. The running sandboxes keep running there and answer
    calls; the node reports them held. The paused one is suspended and left
    suspended, with no drain restore, and a wake lands it on the other
    worker. The idle one then suspends on its
    own on the sealed worker: the drain never reports it evacuated and no
    drain restore moves it. Once its snapshot is durable, while its row
    still names the sealed worker and the drain still holds the other, a
    wake brings it up on the other worker. Its user then suspends the held
    one, and the drain reaches `drained` still gentle; a wake brings that
    one up on the other worker too. After `:undrain`, a second gentle drain
    with a deadline minutes away holds a new running sandbox until the
    deadline, turns forced, and moves it, started, to the other worker.
    Each sandbox's file survives."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin two running sandboxes, one with an idle policy, and a paused one to one worker"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            idle = drain.create_resident(idle_timeout=IDLE_TIMEOUT)
            busy = drain.create_resident()
            paused = drain.create_resident()
            for sandbox in (idle, busy, paused):
                written = drain.run_on(
                    sandbox, f"printf %s {shlex.quote(sandbox.sandbox_id)} > {shlex.quote(PATH)}"
                )
                test.verify(written.exit_code == 0, "failed to write a resident's file")
            cast(ResidentLifecycleSandbox, paused).pause()
            test.sandbox.suspend()
            drain.release_other_worker()
            # Activity restarts the idle clock, so the drain finds it running.
            drain.run_on(idle, "true")

        with test.phase("drain the worker gently, with a deadline hours away"):
            drain.drain(gentle_for=LONG_DEADLINE_SECONDS)

        with test.phase("the running sandboxes keep running and answering, held"):
            for sandbox in (idle, busy):
                drain.expect_held(sandbox.sandbox_id)
                answered = drain.run_on(sandbox, f"cat {shlex.quote(PATH)}", timeout=60)
                test.verify(
                    answered.exit_code == 0 and answered.stdout == sandbox.sandbox_id,
                    "a held sandbox stopped answering under the gentle drain",
                    sandbox_id=sandbox.sandbox_id,
                    stdout=answered.stdout,
                )

        with test.phase("the paused sandbox is left suspended, unrestored; a wake lands it on the other worker"):
            drain.expect_suspended_unrestored(paused.sandbox_id)
            drain.wake_elsewhere(paused)

        with test.phase("the idle sandbox suspends on its own on the sealed worker and is not moved"):
            drain.expect_suspended_in_place(idle.sandbox_id)

        with test.phase("once its snapshot is durable, a wake lands it elsewhere while the drain holds the other"):
            drain.expect_durable_in_place(idle.sandbox_id)
            idle.wake()
            drain.expect_back_elsewhere(idle.sandbox_id, "started")
            observed = drain.run_on(idle, f"cat {shlex.quote(PATH)}")
            test.verify(
                observed.exit_code == 0 and observed.stdout == idle.sandbox_id,
                "the self-suspended sandbox's file did not survive its wake elsewhere",
                stdout=observed.stdout,
            )
            phase = drain.drain_phase()
            test.verify(
                phase == "draining",
                "the drain ended before the self-suspended sandbox woke elsewhere",
                drain_phase=phase,
            )
            drain.expect_held(busy.sandbox_id)
            drain.expect_not_moved(idle.sandbox_id)

        with test.phase("the held sandbox's user suspends it; the drain reaches drained, still gentle"):
            busy.suspend()
            report = drain.expect_drained()
            drain.expect_evacuated(report, {paused.sandbox_id: "paused"})
            for sandbox in (idle, busy):
                drain.expect_not_moved(sandbox.sandbox_id)
            status = drain.status()
            mode = drain.drain_mode()
            test.verify(
                mode == "gentle" and "escalated" not in status,
                "a gentle drain that ended before its deadline turned forced",
                drain_mode=mode,
                status=status,
            )

        with test.phase("a wake brings the user-suspended sandbox up on the other worker"):
            busy.wake()
            drain.expect_back_elsewhere(busy.sandbox_id, "started")
            for sandbox in (busy, paused):
                observed = drain.run_on(sandbox, f"cat {shlex.quote(PATH)}")
                test.verify(
                    observed.exit_code == 0 and observed.stdout == sandbox.sandbox_id,
                    "a sandbox's file did not survive the gentle drain",
                    sandbox_id=sandbox.sandbox_id,
                    stdout=observed.stdout,
                )

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()

        with test.phase("pin a new running sandbox to the worker"):
            other = drain.companion()
            other.cordon()
            held = drain.create_resident()
            written = drain.run_on(
                held, f"printf %s {shlex.quote(held.sandbox_id)} > {shlex.quote(PATH)}"
            )
            test.verify(written.exit_code == 0, "failed to write the held sandbox's file")
            other.uncordon()

        with test.phase("drain the worker gently, with a deadline minutes away"):
            drain.drain(gentle_for=SHORT_DEADLINE_SECONDS)
            drain.expect_held(held.sandbox_id)
            status = drain.status()
            mode = drain.drain_mode()
            test.verify(
                mode == "gentle" and "escalated" not in status,
                "a gentle drain holding a sandbox before its deadline is not gentle",
                drain_mode=mode,
                status=status,
            )

        with test.phase("at the deadline the drain turns forced and moves the held sandbox"):
            drain.expect_turned_forced("deadline", timeout=SHORT_DEADLINE_SECONDS + 300)
            drain.expect_back_elsewhere(held.sandbox_id, "started")
            drain.expect_restored_once(held.sandbox_id)
            report = drain.expect_drained()
            drain.expect_evacuated(report, {held.sandbox_id: "started"})
            observed = drain.run_on(held, f"cat {shlex.quote(PATH)}")
            test.verify(
                observed.exit_code == 0 and observed.stdout == held.sandbox_id,
                "the held sandbox's file did not survive the forced turn",
                stdout=observed.stdout,
            )
            test.sandbox.wake()

        with test.phase("undrain: the second epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
