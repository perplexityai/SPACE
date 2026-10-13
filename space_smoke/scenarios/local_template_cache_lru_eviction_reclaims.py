"""The on-node template LRU loop reclaims an unpinned build over budget."""

import space_smoke as smoke


# The Kind chart renders a small template-manager cache budget so the bench
# exercises LRU eviction. The knobs are read once at daemon start; this scenario
# never changes them. It reads what the DaemonSet rendered and refuses to run against a
# bench whose loop is off or too slow to prove anything within the budget,
# rather than passing vacuously or silently patching a DaemonSet (a
# template-manager rollout on a shared bench is not this scenario's to make).
#
# The forcing device is a real build, not a fault: a throwaway template of
# `budget_bytes` random bytes, registered on the worker that runs this
# scenario's sandbox under a name prefetch never pins and with no sandbox
# referencing it. Obsolete cached seed builds can also be valid candidates;
# the selected seed release and live sandbox's exact build must survive.
# By itself the plant puts the node over the headroom
# target. Random bytes matter:
# the planner charges allocated blocks, so a sparse file would not count.
# (`register` also writes a `rootfs.ext4.zst` next to the rootfs, and random
# bytes do not compress, so the plant weighs about twice its size on disk —
# which only makes the over-budget condition more certain.)
#
# Proof is through operator-visible surfaces only. The worker image ships no
# `sqlite3`, and the compose harness's habit of asserting on the daemon's
# private DB was one of the things worth retiring. Here: the plant's digest
# directory is gone from disk, `space-tmplmgr get` no longer resolves it (it
# has no cluster release, so a miss cannot re-pull), the daemon's own
# `template GC tick complete` line on that worker reports freeing at least
# the plant's bytes, and the selected release and active sandbox digest
# directories remain — read from disk, because resolving the seed through
# the daemon would re-pull it on a miss and could never fail.
@smoke.scenario(
    "local.template_cache_lru_eviction_reclaims",
    # Write and hash the rendered cache budget, then wait out the 60 s grace
    # and a few 15 s ticks; the sandbox itself is idle throughout.
    sandbox=smoke.SandboxConfig(timeout=900),

)
def template_cache_lru_eviction_reclaims(test: smoke.KindEcw) -> None:
    """template-manager's LRU eviction loop, running with the production knobs
    rendered on the worker DaemonSet, reclaims an unpinned unreferenced build
    once the node is over budget and past grace — removing both the build and
    its bytes — while the pinned template and the live sandbox on the same
    worker are untouched."""

    cache = test.template_cache
    protected = test.sandbox_template_name()

    with test.phase("locate the worker running this sandbox"):
        worker = cache.sandbox_worker()

    with test.phase("that worker's template-manager runs a smoke-sized eviction loop"):
        config = cache.require_smoke_sized_gc(worker)

    with test.phase("record the protected template's bytes before the plant"):
        seed_digests = cache.protected_template_digests(worker, protected)

    with test.phase("plant an unpinned build that puts the worker over budget"):
        planted = cache.plant_unpinned_build(worker, size_bytes=config.budget_bytes)

    with test.phase("the loop reclaims the build once it is out of grace"):
        cache.wait_until_evicted(worker, planted)
        cache.expect_reclaimed(worker, planted)

    with test.phase("the protected template and the live sandbox are untouched"):
        cache.expect_protected_template_intact(worker, protected, before=seed_digests)
        observed = test.run_command_when_ready("printf %s space-smoke-lru-alive")
        test.expect_command(observed, stdout="space-smoke-lru-alive")
