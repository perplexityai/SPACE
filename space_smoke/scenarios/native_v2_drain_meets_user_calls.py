"""User calls before, during and after a v2 drain's restores each get their own answer."""

import time
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import DrainRestoreControl
from space_smoke.drain_restore import DrainRestoreAssertions

HOLD_SECONDS = 30


# `native.v2_drain_evacuates_every_resident` is the drain with no user in the
# way. Here the drain restore operation's hold (`SPACE_DRAIN_RESTORE_TEST_HOLD_SECS`
# on the controller) keeps each claimed restore open, so a call can meet one:
# a delete before the node evacuates its sandbox, a wake while its restore
# holds the sandbox (which joins the restore), and an ECW pause and a wake
# after the restore landed.
@smoke.scenario(
    "native.v2_drain_meets_user_calls",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
)
def v2_drain_meets_user_calls(test: smoke.CommandSandboxFixture) -> None:
    """Of two residents of a v2-drained worker, one deleted right after the
    drain begins is gone, never restored, and doesn't hold the drain; the
    other, woken while its claimed restore is held, comes back started once
    the restore lands, on one execution, and then suspends and wakes as
    usual. The node reaches `drained`."""

    drain = test.drain_v2
    control = test._drain_v2_control  # noqa: SLF001
    assert control is not None
    restores = DrainRestoreAssertions(
        cast(smoke.KindEcw, test), cast(DrainRestoreControl, test._drain_v2_control),  # noqa: SLF001
        test._context.scope,  # noqa: SLF001
    )

    with test.phase("pin two residents to one worker"):
        drain.pin_to_resident_worker()
        kept = drain.create_resident()
        deleted = drain.create_resident()
        test.sandbox.suspend()
        drain.release_other_worker()

    with restores.scoped_hold():
        with test.phase(f"hold each claimed drain restore {HOLD_SECONDS} s"):
            restores.hold_claimed_restores(HOLD_SECONDS)

        with test.phase("drain, then delete one resident before it is evacuated"):
            drain.drain()
            test.delete_sandbox(deleted)

        with test.phase("a wake during the held restore joins it and comes back started"):
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
        restores = control.drain_restore_states(drain.bare_id(deleted.sandbox_id))
        test.record("deleted_resident_restores", restores)
        test.verify(
            "succeeded" not in restores,
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
