"""A node's census reports an artifact it cannot prove backed up, and drops it once it is."""

import space_smoke as smoke


# The populated half of the contract is only observable with the upload
# broken: a natural suspend is unbacked for seconds while the census fires
# once a minute. The object store is scaled to zero for the suspend with its
# scale-back armed first and restored the moment the queue row is seen, so
# the downtime stays inside the API's 300 s durable-suspend budget and the
# transfer's fifth retry never runs against a dead store; the drain budget
# covers a transfer that waited out the whole 455 s ladder. Do not run
# concurrently with another suspend-bearing smoke.
@smoke.scenario(
    "local.pending_backup_census_reports_blocked_upload",
    # Census budget 300 s plus a drain budget of 600 s, plus the object store
    # scale-down and rollout.
    sandbox=smoke.SandboxConfig(timeout=1500),
)
def pending_backup_census_reports_blocked_upload(test: smoke.KindEcw) -> None:
    """With object storage unreachable, a suspend operation leaves a snapshot in
    `creating`, the node's next Full census reports the pinned artifact as
    pending, and space-internal-api mirrors it into the cluster queue with an
    applied non-empty publication; once storage returns, the snapshot becomes
    `ready` with a storage location, the queue row disappears, and the suspend
    operation ends `succeeded`."""

    census = test.pending_backup

    with test.phase("block uploads: scale the object store to zero"):
        census.block_uploads()

    with test.phase("suspend as an operation; the snapshot stays in flight"):
        # Not the ECW pause: that route is synchronous on durability and can
        # only 502 while uploads are blocked (bench, 2026-09-23).
        census.suspend_async()
        census.expect_snapshot_in_flight()

    with test.phase("the census reports the artifact and the queue names it"):
        census.expect_queue_names_this_sandbox()

    with test.phase("restore uploads; the artifact is backed and leaves the queue"):
        census.restore_uploads()
        census.expect_backed_and_dequeued()
        census.expect_suspend_succeeded()
