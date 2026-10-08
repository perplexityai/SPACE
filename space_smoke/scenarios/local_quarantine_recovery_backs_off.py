"""An induced quarantine with no recovery plan is retried on a growing backoff, not every tick."""

import space_smoke as smoke


# Three attempts exercise the real 60 s base and doubled retry delay. Both
# episodes advance in the same reconciler passes, so proving the two-episode
# contract adds no serial wait. Duration-balanced sharding accounts for this
# production-time proof; it does not need a quarantine or accelerated clocks.
@smoke.scenario(
    "local.quarantine_recovery_backs_off",
    # Up to 300 s for the quarantine, 900 s for three attempts per episode,
    # then the retirement; the fixture sandbox only names the worker. The
    # measured happy path is ~4.3 min including discovery and cleanup
    # (attempts at episode ages 0, 60, 180 s).
    sandbox=smoke.SandboxConfig(timeout=1500),
)
def quarantine_recovery_backs_off(test: smoke.KindEcw) -> None:
    """A sandbox cache directory holding an interrupted `.suspend-publish-*`
    staging and no metadata row is quarantined by the inventory reconciler with
    reason `orphaned_sandbox_row_missing`; the recovery loop then charges one
    attempt per pass and schedules the next with exponential backoff, so three
    successive `RetryScheduled` records per episode are contiguous 1..3 and the
    last interval clears one and a half ticks, for both of two induced episodes."""

    recovery = test.quarantine_recovery

    with test.phase("pin the worker's sandbox-manager pod; induce two interrupted suspend publications"):
        recovery.induce(count=2)

    with test.phase("the reconciler quarantines both as orphaned_sandbox_row_missing"):
        recovery.expect_quarantined()

    with test.phase("collect three bounded recovery attempts per episode"):
        recovery.expect_attempts(want=3)

    with test.phase("the retry cadence backs off for every episode"):
        recovery.expect_backoff(want=3)

    with test.phase("retire the induced rows and directories"):
        recovery.retire()
