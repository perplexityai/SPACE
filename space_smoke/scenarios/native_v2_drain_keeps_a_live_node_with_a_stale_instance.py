"""A v2 drain never lets a live node go on EC2's answer about a stale recorded instance."""

import time

import space_smoke as smoke

# How long the run watches the drain wait once EC2 says the stale instance is
# terminated: several loop ticks.
WATCH_SECONDS = 60


# Each let-go path's check of the machine is a crate test
# (`node_drain/v2/tests.rs`). Here a worker whose Node, under Kind's own
# providerID, names no EC2 instance has a fake instance written into its node
# row, as an older registration writer leaves one across a Node UID change.
# The worker is drained and sealed; its machine then stops answering, its
# Node kept, and Kind's fake EC2 says the stale instance is terminated. The
# drain must not let the node go, since EC2 answered about another machine.
@smoke.scenario(
    "native.v2_drain_keeps_a_live_node_with_a_stale_instance",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
    tier="manual",
    manual_reason="stops a Kind worker while its Node stays, too invasive for the per-merge set",
)
def v2_drain_keeps_a_live_node_with_a_stale_instance(test: smoke.CommandSandboxFixture) -> None:
    """A v2 drain of a worker whose node row records a stale instance, its
    Node naming none, seals the worker. Its machine then stops answering
    with its Node kept, and EC2 answers `terminated` for the recorded
    instance. The drain does not let the node go: it keeps its phase, and
    `drain_last_error` says the Node is live and names no EC2 instance. Once
    the machine answers again, `:undrain` cancels the epoch."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("a worker whose node row records a stale instance"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            stale = drain.give_stale_instance()
            drain.release_other_worker()

        with test.phase("drain with engine v2; the node reports sealed under the epoch"):
            epoch = drain.drain()
            drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch,
                "the node never reported sealed under the drain's epoch",
                timeout=180,
            )

        with test.phase("the machine stops with its Node kept; EC2 says the stale instance is terminated"):
            restart = drain.stop_keeping_node()
            drain.fake_ec2_terminate(stale)

        with test.phase("the drain waits instead of letting the live node go"):
            waiting = drain.wait_for_last_error(
                "names no EC2 instance; not letting it go", timeout=600
            )
            test.record("drain_last_error_waiting", waiting)
            watched_until = time.monotonic() + WATCH_SECONDS
            while time.monotonic() < watched_until:
                phase, error = drain.drain_phase(), drain.last_error() or ""
                test.verify(
                    phase in ("draining", "drained") and "let go" not in error,
                    "the drain let a live node go on EC2's answer about a stale instance",
                    drain_phase=phase,
                    drain_last_error=error,
                )
                test.wait(10)

        with test.phase("the machine back: undrain cancels the epoch"):
            restart.release()
            drain.undrain_and_expect_cancelled()
