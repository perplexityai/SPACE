"""The on-node snapshot reaper reclaims a fork's backed, unpinned source snapshot."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-snapshot-reaper.txt"
MARKER = "space-smoke-snapshot-reaper-ok"


# The snapshot artifact reaper deletes backed-up, unpinned local snapshots
# that are idle and past grace, through node-mgr's DeleteCacheArtifact.
#
# The forcing device is a public fork, not a seeded row. A fork captures a
# *disk* named snapshot of the source, encodes it to a parent-relative btrfs
# stream in a plain `d-operation-<op>-artifact` directory (the subvolume
# capture is removed before publish), uploads it, and builds the child from
# the durable copy; the source artifact stays behind unpinned (only a sandbox's
# *current* artifact is pinned) and is never touched again. That is the
# artifact class the reaper exists to reclaim, produced the way production
# produces it. It is also the only *backed* unpinned artifact a bench ever
# holds: checkpoint and import snapshots are never uploaded (so never
# candidates), every suspended or restored sandbox pins its current suspend
# artifact, and a re-suspend deletes the previous rows when it publishes.
#
# Proof, through state only: the source's `snapshots/` directory gains an
# entry after the fork (positive probe, so the later "gone" is evidence) and
# the cluster holds a `ready` snapshot row with a live storage location for
# the source's artifact; the reaper then leaves neither the entry nor a
# `.gc-pending` sibling on disk, while those rows stay `ready` with their
# storage — the backup the reaper requires before it deletes — and the
# source still executes commands. sbx-manager exposes no read that names
# which local artifact backs a snapshot directory, so the scenario does not
# join the reaped directory to one artifact id: the fork leaves the source
# exactly the one backed, unpinned artifact the reaper can take.
#
# Not reached: the negative gates (below-engage, backup incomplete, still
# pinned, not idle), the shared-local-path refusal and the startup orphan
# sweep need constructed node-local rows and are unit-covered
# (`snapshot_gc/tests.rs`, `local_artifacts/tests.rs`). A *btrfs subvolume*
# reap is not reachable on a bench: the only backed unpinned artifact is the
# fork's encoded stream directory, a plain dir.
@smoke.scenario(
    "local.snapshot_reaper_reclaims_backed_fork_snapshot",
    # Fork (capture + upload + child create) plus the rendered idle, grace and
    # a few ticks; the sandbox itself is live throughout. 2400 s covers the
    # largest idle the fixture accepts.
    sandbox=smoke.SandboxConfig(timeout=2400),
    test_hooks="the chart's SPACE_SBX_GC_TARGET_FRACTION, SPACE_SBX_GC_INTERVAL_SECS, "
    "SPACE_SBX_GC_GRACE_SECS and SPACE_SBX_GC_IDLE_SECS; release waits a day's idle",
)
def snapshot_reaper_reclaims_backed_fork_snapshot(test: smoke.KindEcw) -> None:
    """sandbox-manager's snapshot artifact reaper, running with the knobs
    rendered on the worker DaemonSet, deletes a fork's backed, unpinned source
    artifact once it is idle and past grace — removing its directory through
    node-mgr with no residue — while the cluster keeps the durable copy and the
    source keeps executing."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    reaper = test.snapshot_reaper

    with test.phase("write state and locate the worker"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write state before forking the sandbox",
            command_stderr=written.stderr,
        )
        worker = reaper.sandbox_worker()

    with test.phase("that worker's sandbox-manager runs a smoke-sized snapshot reaper"):
        config = reaper.require_smoke_sized_reaper(worker)
        test.record("idle_seconds", config.idle_seconds)

    with test.phase("fork the sandbox; the source keeps an unpinned backed snapshot"):
        reaper.fork_source()
        before = reaper.expect_source_snapshot_present(worker)
        backed = reaper.expect_source_snapshot_backed()

    with test.phase("the reaper deletes the source snapshot once it is cold"):
        reaper.wait_until_reaped(worker, before=before)

    with test.phase("the cluster keeps the reaped artifact's ready remote copy"):
        reaper.expect_durable_copy_kept(backed)

    with test.phase("the source still executes"):
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)
