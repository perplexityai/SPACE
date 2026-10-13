"""A cold, provably backed suspended sandbox is evicted locally and wakes from S3."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-cold-eviction.txt"
MARKER = "space-smoke-cold-eviction-ok"


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
# and nothing the wait does — cluster Postgres reads, `kubectl logs`, sleeping
# — touches it. The retired compose suites aged rows in sbx-manager's private
# SQLite instead; the framework has no such surface by design.
#
# Proof is through the daemon's own surfaces: its `local_eviction_completed`
# sandbox event on the sandbox's worker, the cache directory gone from that
# node's disk, and the wake's `runtime_activated` event reporting a healthy
# `remote_*` provenance with the storage URI present — a wake served from some
# surviving local copy reads `local_only` and fails. State written before the
# suspend must survive the suspend → upload → evict → S3-restore round trip.
#
# What this does not reach, and where it lives instead: the never-evict
# decision table (no ready backup; local bytes newer than the backup; an
# unproven pin) and the disk-gate refusal need constructed node-local rows
# and are covered by `crates/libs/sbx/mgr/server/src/sandbox_eviction/tests.rs`.
@smoke.scenario(
    "local.cold_sandbox_eviction_wake_round_trip",
    # The rendered minimum idle plus upload, ticks and a remote restore; the
    # sandbox itself is parked for most of it. Worst case ≈ create + 300 s
    # portability + the eviction wait (minimum idle + four ticks + 300 s) +
    # restore; 2400 s covers the largest minimum idle the fixture accepts.
    sandbox=smoke.SandboxConfig(timeout=2400),
)
def cold_sandbox_eviction_wake_round_trip(test: smoke.KindEcw) -> None:
    """sandbox-manager's cold-sandbox eviction loop, running with the knobs
    rendered on the worker DaemonSet, removes a suspended sandbox's local
    presence once it is past the minimum idle and provably backed — and the
    sandbox then wakes by restoring its snapshot from object storage with its
    filesystem intact."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    eviction = test.cold_eviction

    with test.phase("write state and record the worker before suspend"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
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

    with test.phase("the loop evicts the sandbox once it is past the minimum idle"):
        evicted = eviction.wait_until_evicted(worker)
        test.record("eviction_snapshot_id", evicted.snapshot_id)
        eviction.expect_local_presence_gone(worker)

    with test.phase("the sandbox wakes by restoring from object storage"):
        test.reconnect_by_id()
        eviction.expect_woken_from_object_storage(source=source)

    with test.phase("state survives the suspend → evict → restore round trip"):
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)
