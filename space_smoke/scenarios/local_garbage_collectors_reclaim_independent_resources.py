"""Independent GC candidates age together without shortening their safety clocks."""

import shlex
from collections.abc import Callable

import space_smoke as smoke


@smoke.scenario(
    "local.garbage_collectors_reclaim_independent_resources",
    sandbox=smoke.SandboxConfig(timeout=2400),
    test_hooks="the chart's snapshot reaper idle/grace/tick and template GC grace/tick; "
    "their existing safety floors remain in force",
)
def garbage_collectors_reclaim_independent_resources(test: smoke.KindEcw) -> None:
    """Evict a cold sandbox, reap a fork snapshot, and reclaim an unpinned template.

    Each candidate has its own positive presence probe before it ages. The
    source of the fork stays live, the template has a separate live pin, and
    the cold sandbox stays suspended until its local presence is gone. No
    daemon configuration changes or concurrent fixture mutations are needed:
    the three existing background loops supply the concurrency.
    """

    # Start the longest eligibility clock first. All three preparations finish
    # before waiting on any loop; the shorter clocks expire within the cold
    # sandbox's minimum idle on the usual Kind profile.
    finish_cold = _prepare_cold_eviction(test)
    with test.phase("create the live source for snapshot reaping"):
        source = test.create_gc_sandbox()
    finish_reaper = _prepare_snapshot_reaper(source)
    with test.phase("create the live template pin for LRU eviction"):
        pin = test.create_gc_sandbox()
    finish_template = _prepare_template_eviction(pin)
    test.record(
        "gc_sandboxes",
        {
            "cold": test.sandbox.sandbox_id,
            "fork_source": source.sandbox.sandbox_id,
            "template_pin": pin.sandbox.sandbox_id,
        },
    )

    # Preserve every original completion oracle, including remote durability,
    # absence of .gc-pending residue, protected template bytes, and guest data.
    finish_reaper()
    finish_template()
    finish_cold()


# Cold-sandbox eviction removes a suspended sandbox's entire local presence
# once it is past minimum idle and its state is provably
# restorable from a ready remote snapshot; the cluster row stays `suspended`
# and the next wake restores from object storage through the same orphan-adopt
# path a cross-node restore uses. The claim worth proving end to end is the
# safety one: eviction never strands a sandbox.
#
# The scenario's only forcing device is time. The knobs are read once at daemon
# start, so it reads what the worker pod rendered — refusing a bench whose loop
# is off, gated on disk pressure a 40 GiB kind cache never reaches, or too slow
# to wait out — and then waits the rendered minimum idle honestly. "Idle" is
# the loop's activity witness: the newest guest-activity clock (this scenario's
# marker write sets `last_command_at`), so the clock starts before the pause,
# and nothing the wait does — cluster Postgres reads, probing the cache
# directory, sleeping — touches it. The retired compose suites aged rows in sbx-manager's private
# SQLite instead; the framework has no such surface by design.
#
# Proof is state: the cache directory gone from the worker's disk while the
# sandbox stays suspended (the cluster-deleted reclaim cannot apply), then a
# wake whose landed pod is the sandbox's one active execution, planned from
# the snapshot's storage URI. No local copy survived to serve it, so state
# written before the suspend must come back through the suspend → upload →
# evict → S3-restore round trip.
#
# What this does not reach, and where it lives instead: the never-evict
# decision table (no ready backup; local bytes newer than the backup; an
# unproven pin) and the disk-gate refusal need constructed node-local rows
# and are covered by `crates/l40_api/sbx/mgr/server/src/sandbox_eviction/tests.rs`.

_COLD_PATH = "/tmp/space-smoke-cold-eviction.txt"
_COLD_MARKER = "space-smoke-cold-eviction-ok"


def _prepare_cold_eviction(test: smoke.KindEcw) -> Callable[[], None]:
    """Prepare an independent GC candidate; return its completion assertions."""

    marker = f"{_COLD_MARKER}-{test.sandbox.sandbox_id}"
    eviction = test.cold_eviction

    with test.phase("write state and record the worker before suspend"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(_COLD_PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write state before suspending the sandbox",
            command_stderr=written.stderr,
        )
        source = eviction.sandbox_placement()
        worker = eviction.worker_node(source)

    with test.phase("that worker's sandbox-manager runs a smoke-sized eviction loop"):
        config = eviction.require_smoke_sized_loop(worker)
        test.record("min_idle_seconds", config.min_idle_seconds)

    with test.phase("suspend durably"):
        # ECW pause is SPACE suspend. The public state settles to paused first;
        # the S3 upload that makes the sandbox provably backed is asynchronous.
        test.pause()

    with test.phase("a ready remote snapshot exists to prove eviction against"):
        eviction.wait_until_restore_portable()
        # Positive probe first, so the later "gone" check is evidence rather
        # than a path that could never match.
        eviction.expect_local_presence_present(worker)

    def finish() -> None:
        with test.phase("the loop evicts the sandbox once it is past the minimum idle"):
            eviction.wait_until_evicted(worker)
            eviction.expect_local_presence_gone(worker)

        with test.phase("the sandbox wakes by restoring from object storage"):
            test.reconnect_by_id()
            eviction.expect_woken_from_object_storage(source=source)

        with test.phase("state survives the suspend → evict → restore round trip"):
            observed = test.run_command_when_ready(f"cat {shlex.quote(_COLD_PATH)}")
            test.expect_command(observed, stdout=marker)

    return finish


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

_REAPER_PATH = "/tmp/space-smoke-snapshot-reaper.txt"
_REAPER_MARKER = "space-smoke-snapshot-reaper-ok"


def _prepare_snapshot_reaper(test: smoke.KindEcw) -> Callable[[], None]:
    """Prepare an independent GC candidate; return its completion assertions."""

    marker = f"{_REAPER_MARKER}-{test.sandbox.sandbox_id}"
    reaper = test.snapshot_reaper

    with test.phase("write state and locate the worker"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(_REAPER_PATH)}"
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

    def finish() -> None:
        with test.phase("the reaper deletes the source snapshot once it is cold"):
            reaper.wait_until_reaped(worker, before=before)

        with test.phase("the cluster keeps the reaped artifact's ready remote copy"):
            reaper.expect_durable_copy_kept(backed)

        with test.phase("the source still executes"):
            observed = test.run_command_when_ready(f"cat {shlex.quote(_REAPER_PATH)}")
            test.expect_command(observed, stdout=marker)

    return finish


# The Kind chart renders a small template-manager cache budget so the bench
# exercises LRU eviction. The knobs are read once at daemon start; this scenario
# never changes them. It reads what the DaemonSet rendered and refuses to run against a
# bench whose loop is off or too slow to prove anything within the budget,
# rather than passing vacuously or silently patching a DaemonSet (a
# template-manager rollout on a shared bench is not this scenario's to make).
#
# The forcing device is a real build, not a fault: a throwaway template of
# enough random bytes to exceed the budget after counting protected rootfs bytes, registered on the worker that runs this
# scenario's sandbox under a name prefetch never pins and with no sandbox
# referencing it. Obsolete cached seed builds can also be valid candidates;
# the selected seed release and live sandbox's exact build must survive.
# Together the protected rootfs allocation and plant exceed the budget.
# Random bytes matter:
# the planner charges allocated blocks, so a sparse file would not count.
# (`register` also writes a `rootfs.ext4.zst` next to the rootfs, and random
# bytes do not compress, so the plant weighs about twice its size on disk —
# which only makes the over-budget condition more certain.)
#
# Proof is through operator-visible surfaces only. The worker image ships no
# `sqlite3`, and the compose harness's habit of asserting on the daemon's
# private DB was one of the things worth retiring. Here: the plant's digest
# directory is gone from disk, `space-tmplmgr get` no longer resolves it (it
# has no cluster release, so a miss cannot re-pull), and the selected release
# and active sandbox digest directories remain — read from disk, because
# resolving the seed through the daemon would re-pull it on a miss and could
# never fail.


def _prepare_template_eviction(test: smoke.KindEcw) -> Callable[[], None]:
    """Prepare an independent GC candidate; return its completion assertions."""

    cache = test.template_cache
    protected = test.sandbox_template_name()

    with test.phase("locate the worker running this sandbox"):
        worker = cache.sandbox_worker()

    with test.phase("that worker's template-manager runs a smoke-sized eviction loop"):
        cache.require_smoke_sized_gc(worker)

    with test.phase("record the protected template's bytes before the plant"):
        seed_digests = cache.protected_template_digests(worker, protected)

    with test.phase("plant an unpinned build that puts the worker over budget"):
        size = cache.over_budget_plant_bytes(worker, protected, seed_digests)
        planted = cache.plant_unpinned_build(worker, size_bytes=size)

    def finish() -> None:
        with test.phase("the loop reclaims the build once it is out of grace"):
            cache.wait_until_evicted(worker, planted)
            cache.expect_reclaimed(worker, planted)

        with test.phase("the protected template and the live sandbox are untouched"):
            cache.expect_protected_template_intact(worker, protected, before=seed_digests)
            observed = test.run_command_when_ready("printf %s space-smoke-lru-alive")
            test.expect_command(observed, stdout="space-smoke-lru-alive")

    return finish
