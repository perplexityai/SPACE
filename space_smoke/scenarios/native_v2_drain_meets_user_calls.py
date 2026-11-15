"""User calls before, during and after a v2 drain's restores each get their own answer."""

import time

import space_smoke as smoke

HOLD_SECONDS = 30


# `native.v2_drain_evacuates_every_resident` is the drain with no user in the
# way. Here the drain restore operation's hold (`SPACE_DRAIN_RESTORE_TEST_HOLD_SECS`
# on the controller, scoped with the v2 config) keeps each claimed restore
# open, so a call can meet one: a delete as the drain begins, a wake while its
# restore holds the sandbox (which waits for the restore and finds it
# started), and a suspend and a wake after the restore landed. The claim's
# refusals and the supersession of a waiting restore are crate tests
# (`drain_restore_tests.rs`, `drain_restore_operations/tests`).
@smoke.scenario(
    "native.v2_drain_meets_user_calls",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    test_hooks="SPACE_DRAIN_RESTORE_TEST_HOLD_SECS, which holds each claimed drain restore",
)
def v2_drain_meets_user_calls(test: smoke.CommandSandboxFixture) -> None:
    """Of two residents of a v2-drained worker, one deleted right after the
    drain begins is gone, never restored, and doesn't hold the drain; the
    other, woken while its claimed restore is held, comes back started once
    the restore lands, on one execution, and then suspends and wakes as
    usual. The node reaches `drained`."""

    drain = test.drain_v2

    with drain.v2_controller(hold_seconds=HOLD_SECONDS):
        with test.phase("pin two residents to one worker"):
            # A v2 drain needs the node's instance recorded; re-registering
            # under a fake one deletes the pods there, so the fixture's
            # worker is left alone.
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            kept = drain.create_resident()
            deleted = drain.create_resident()
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain, then delete one resident before it is evacuated"):
            drain.drain()
            test.delete_sandbox(deleted)

        with test.phase("a wake during the held restore waits for it and comes back started"):
            drain.wait_for_report(
                lambda report: any(
                    entry.get("sandbox_id") == drain.bare_id(kept.sandbox_id)
                    for entry in report.get("evacuated", [])
                ),
                "the node never reported the kept resident evacuated",
                timeout=600,
            )
            started = time.monotonic()
            kept.wake()
            test.record("wake_seconds", round(time.monotonic() - started, 1))
            drain.expect_back_elsewhere(kept.sandbox_id, "started")

        with test.phase("the deleted resident is gone and the node drains"):
            report = drain.expect_drained()
            # The node may have made it durable before the delete landed, and so
            # report it evacuated; the controller then enqueues no restore, or one
            # the delete supersedes. None lands it.
            restores = drain.drain_restores(deleted.sandbox_id)
            test.record("deleted_resident_restores", restores)
            test.verify(
                all(state != "succeeded" for state, _attempts in restores),
                "a drain restore brought the deleted resident back",
                restores=restores,
                report=report,
            )

        with test.phase("after: the restored sandbox suspends and wakes"):
            kept.suspend()
            kept.wake()
            observed = drain.run_on(kept, "true")
            test.verify(observed.exit_code == 0, "the restored sandbox did not run after a suspend and wake")

        with test.phase("undrain"):
            drain.undrain_and_expect_cancelled()
