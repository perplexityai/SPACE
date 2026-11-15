"""An undrain while a v2 drain's suspends run leaves every resident in its lifecycle."""

import space_smoke as smoke


# `native.v2_undrain_unseals_the_worker` undrains a sealed worker whose
# resident the drain has not touched. Here the undrain lands while the
# drain's suspends of a started and a paused resident run: the cancel lets
# them finish and hands each back, and the controller restores each to the
# lifecycle it had, on any worker, the drained one included, before it
# finishes the cancel. The planner and the loop's cancel are crate tests
# (`service/drain_node/tests.rs`, `node_drain/v2/tests.rs`).
@smoke.scenario(
    "native.v2_undrain_mid_suspend",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
)
def v2_undrain_mid_suspend(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` of a worker holding a started and a paused resident,
    each VM holding memory, is `:undrain`ed while the node reports their
    suspends running. The epoch is cancelled on the node once both are handed
    back, and each resident ends in its lifecycle with exactly one VM, landed
    by one drain restore, the started one's memory intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started and a paused resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents(
                {"started": 1, "paused": 1}, mib=384
            )

        with test.phase("drain, and undrain while the suspends run"):
            epoch = drain.drain()
            suspending = drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and "suspend" in drain.pending_actions(report).values(),
                "the node never reported a drain suspend running",
                timeout=180,
            )
            test.record("report_at_undrain", suspending)
            drain.undrain_and_expect_cancelled()

        with test.phase("each resident ends in its lifecycle with one VM"):
            outcome = drain.settled_lifecycles(lifecycles, timeout=600)
            test.record("lifecycles_after_undrain", outcome)
            test.verify(
                all(outcome[drain.bare_id(sandbox_id)][0] == lifecycle for sandbox_id, lifecycle in lifecycles.items()),
                "an undrain mid-suspend left a resident out of its lifecycle",
                expected={drain.bare_id(sandbox_id): lifecycle for sandbox_id, lifecycle in lifecycles.items()},
                observed=outcome,
                restores={drain.bare_id(sandbox_id): drain.drain_restores(sandbox_id) for sandbox_id in lifecycles},
            )
            for sandbox_id, lifecycle in lifecycles.items():
                drain.expect_single_vm(sandbox_id, lifecycle)
                drain.expect_landed_once(sandbox_id)
            drain.expect_started_memory(sandboxes, lifecycles, digests)
            test.sandbox.wake()
