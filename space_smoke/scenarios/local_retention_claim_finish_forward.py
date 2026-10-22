"""A crash-orphaned prune claim must finish forward."""

import time

import space_smoke as smoke

# Two, so the keep-N check below has an unpinned survivor: the newest ready
# snapshot is the suspended sandbox's restore source and survives by its own
# guard, the second-newest only by the floor.
KEEP_N = 2


@smoke.scenario(
    "local.retention_claim_finish_forward",
    required_tools=("aws",),
    test_hooks="SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN, SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS "
    "and SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS, which it stages",
)
def claim_finish_forward(test: smoke.KindEcw) -> None:
    """A crash-orphaned `deleting` claim finishes forward without violating
    retention or sandbox usability."""

    with test.retention.scoped_sweeper_mutation():
        # Disable the loop while the scenario constructs and injects its crash
        # state; dry-run is also set explicitly so the staged mode is unambiguous.
        with test.phase("disable the sweeper while constructing the fault"):
            config = test.retention.stage_smoke_sweeper(enabled=False, dry_run=True, keep_n=KEEP_N)

        # Once both oldest rows are claimed, no remaining ready row is
        # prune-eligible. The pair covers both crash boundaries in one recovery
        # scan without duplicating the scenario.
        cycles = config.keep_n + 2
        with test.phase(f"commit {cycles} ready full snapshots"):
            for cycle in range(1, cycles + 1):
                test.pause()
                test.retention.wait_for_ready_fulls(cycle)
                if cycle < cycles:
                    test.reconnect_by_id()

        with test.phase("orphan claims at both byte-deletion crash boundaries"):
            recoveries = [
                ("crash after claim", test.retention.orphan_claim_on_oldest()),
                ("crash after byte deletion", test.retention.orphan_claim_on_oldest()),
            ]
            test.retention.delete_backing_objects_for_crash(recoveries[1][1])

        with test.phase("go live: the sweeper finishes the claim forward"):
            config = test.retention.stage_smoke_sweeper(enabled=True, dry_run=False, keep_n=KEEP_N)
            live_since = time.monotonic()
            budget = config.tick_seconds * 4 + 60
            for crash_boundary, orphaned in recoveries:
                with test.phase(f"finish forward from {crash_boundary}"):
                    test.retention.expect_pruned(orphaned, timeout=budget)
                    test.retention.expect_backing_objects_reaped(orphaned, timeout=30)

        with test.phase("nothing stays parked and keep-N remains intact"):
            # Every remaining snapshot predates the live sweeper. Once they are
            # all past its grace and it has ticked twice more, a sweeper that
            # ignored the floor would have claimed the unpinned survivor.
            settle = live_since + config.min_age_seconds + 2 * config.tick_seconds - time.monotonic()
            if settle > 0:
                test.wait(settle, reason="the survivors age past the grace under a live sweeper")
            test.retention.expect_no_parked_claims(
                summary="a claim remained parked in state='deleting' after the "
                "sweep — finish-forward did not converge",
            )
            remaining = test.retention.ready_full_snapshots()
            test.verify(
                len(remaining) >= config.keep_n,
                "the sweep left fewer ready snapshots than the keep-N floor",
                ready_fulls=len(remaining),
                keep_n=config.keep_n,
            )

        # A failed body still restores as it unwinds; this phase accounts for
        # the controller rollout on the passing path.
        with test.phase("restore the standing sweeper"):
            test.retention.restore_standing_sweeper()

    with test.phase("resume and execute after the recovery sweep"):
        test.reconnect_by_id()
        result = test.run_command(
            "echo finish-forward-ok",
            failure_message="the resumed sandbox did not run a command",
        )
        test.verify(
            result.exit_code == 0 and "finish-forward-ok" in result.stdout,
            "the resumed sandbox's data plane did not execute after the recovery sweep",
            command_exit_code=result.exit_code,
            command_stdout=result.stdout[:200],
        )
