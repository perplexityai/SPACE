"""Retention must never destroy the snapshot restore would load."""

import space_smoke as smoke
from space_smoke.fixtures import SMOKE_SWEEPER_KEEP_N

TIMEOUT_MINUTES = 30


@smoke.scenario(
    "local.retention_restore_source_guard",
    timeout_minutes=TIMEOUT_MINUTES,
    # Generous deadline: the default pause-on-timeout keeps even a stalled
    # run recoverable, but must never fire mid-cycle.
    sandbox=smoke.SandboxConfig(timeout=TIMEOUT_MINUTES * 60),
)
def restore_source_guard(test: smoke.KindEcw) -> None:
    """The live retention sweeper prunes unpinned history past keep-N while
    the current-artifact pin — the exact snapshot restore loads for a marked
    suspended sandbox — survives the sweep and the sandbox still resumes."""

    with test.retention.scoped_sweeper_mutation():
        # A slow KVM bench can take longer than the smoke grace to build this
        # history. Hold the loop off until the pin and its negative control
        # exist; otherwise the behavior under test can legitimately prune the
        # oldest row while the fixture is still constructing it. A standing
        # sweeper whose grace outlasts the whole scenario already holds off,
        # with no rollout.
        with test.phase("hold the sweeper off while constructing snapshot history"):
            if not test.retention.standing_sweeper_is_inert(for_seconds=TIMEOUT_MINUTES * 60):
                test.retention.stage_smoke_sweeper(enabled=False, dry_run=True)

        # keep_n + 2 ready fulls: the oldest becomes the manufactured stale pin,
        # the second-oldest stays unpinned past keep-N — the negative control
        # proving the sweep ran live rather than idling.
        cycles = SMOKE_SWEEPER_KEEP_N + 2
        with test.phase(f"commit {cycles} ready full snapshots"):
            for cycle in range(1, cycles + 1):
                test.pause()
                test.retention.wait_for_ready_fulls(cycle)
                if cycle < cycles:
                    test.reconnect_by_id()

        with test.phase("pin the marker to the oldest snapshot"):
            pinned = test.retention.pin_to_oldest()
            control = test.retention.oldest_unpinned(pinned)

        with test.phase("go live after the restore source is pinned"):
            config = test.retention.stage_smoke_sweeper(enabled=True, dry_run=False)

        with test.phase("await the live sweep of the unpinned sibling"):
            budget = config.min_age_seconds + config.tick_seconds * 4 + 60
            test.retention.expect_pruned(control, timeout=budget)

        with test.phase("the pinned restore source survives the sweep"):
            test.retention.expect_present(
                pinned,
                summary="the pinned snapshot was pruned — the restore-source "
                "guard did not hold",
            )
            test.wait(config.tick_seconds * 2, reason="two further sweeper ticks")
            test.retention.expect_present(
                pinned,
                summary="the pinned snapshot was pruned after the control sweep "
                "— the restore-source guard did not hold",
            )

        # A failed body still restores as it unwinds; this phase accounts for
        # the controller rollout on the passing path.
        with test.phase("restore the standing sweeper"):
            test.retention.restore_standing_sweeper()

    with test.phase("resume and execute after the sweep"):
        test.reconnect_by_id()
        # Reconnect only reattaches the SDK handle; prove the restored guest
        # actually executes.
        result = test.run_command(
            "echo restore-source-ok",
            failure_message="the resumed sandbox did not run a command",
        )
        test.verify(
            result.exit_code == 0 and "restore-source-ok" in result.stdout,
            "the resumed sandbox's data plane did not execute after the sweep",
            command_exit_code=result.exit_code,
            command_stdout=result.stdout[:200],
        )
