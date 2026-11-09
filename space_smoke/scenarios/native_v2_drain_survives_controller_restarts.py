"""A v2 drain survives the cluster-controller restarting at each of its phases."""

import space_smoke as smoke

# Long enough for a controller restart to land while claimed restores hold.
HOLD_SECONDS = 45


# The loop's idempotence is crate tests: a replayed report enqueues one
# restore per snapshot (`node_drain/store/v2_sink/tests.rs`), a stale claim
# is claimed again (`public_operations` tests). This is each seam on real
# workers. Uploads blocked on the drained worker hold the drain after its
# seal with nothing evacuated while the controller restarts; the controller
# is then down while the node makes every resident durable, so no restore is
# enqueued until it is back; and it restarts again while the drain
# restores, held, run.
@smoke.scenario(
    "native.v2_drain_survives_controller_restarts",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=60,
    test_hooks="SPACE_DRAIN_RESTORE_TEST_HOLD_SECS, which holds each claimed drain restore",
    ignored="restarts and scales the cluster-controller, too invasive for the per-merge set",
)
def v2_drain_survives_controller_restarts(test: smoke.CommandSandboxFixture) -> None:
    """A v2 drain of two started, a paused and a stopped resident outlives
    three controller restarts: after the seal, with uploads blocked and
    nothing evacuated; with the controller down while the node makes each
    resident durable, so no restore exists until it is back and enqueues one
    each; and while the held restores run. Every resident ends in its
    lifecycle with one VM, each suspended one landed by exactly one drain
    restore, the node reaches `drained`, and `:undrain` cancels the epoch."""

    drain = test.drain_v2

    with drain.v2_controller(hold_seconds=HOLD_SECONDS):
        with test.phase("pin a started pair, a paused and a stopped resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents(
                {"started": 2, "paused": 1, "stopped": 1}
            )

        with test.phase("uploads blocked: drain, and restart the controller after the seal"):
            uploads = drain.block("object-store")
            epoch = drain.drain()
            sealed = drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and len(drain.pending_actions(report)) == len(lifecycles),
                "the node never reported every resident pending under the drain's epoch",
                timeout=300,
            )
            test.verify(
                not sealed.get("evacuated"),
                "a resident was evacuated with its uploads blocked",
                report=sealed,
            )
            drain.restart_controller()
            after = drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch,
                "the restarted controller lost the drain's epoch",
                timeout=120,
            )
            test.record("report_after_first_restart", after)

        with test.phase("the controller down while the node makes each resident durable"):
            with drain.controller_down():
                uploads.release()
                for sandbox_id, lifecycle in lifecycles.items():
                    drain.expect_durable(sandbox_id, lifecycle, timeout=600)
                waiting = {
                    drain.bare_id(sandbox_id): drain.drain_restores(sandbox_id)
                    for sandbox_id in lifecycles
                }
                test.record("restores_while_controller_down", waiting)
                test.verify(
                    not any(waiting.values()),
                    "a drain restore was enqueued with the controller down",
                    restores=waiting,
                )

        with test.phase("the controller back: one restore each; restart it while they run"):
            drain.wait_for_restores(
                [sandbox_id for sandbox_id, lifecycle in lifecycles.items() if lifecycle != "stopped"],
                lambda restores: any(state == "running" for state, _ in restores),
                "no drain restore was running under the hold",
                timeout=300,
            )
            drain.restart_controller()

        with test.phase("every resident ends in its lifecycle with one VM; the node drains"):
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)
            drain.expect_all_back(lifecycles, timeout=1200)
            drain.expect_started_memory(sandboxes, lifecycles, digests)
            test.sandbox.wake()

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
