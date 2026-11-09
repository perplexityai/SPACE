"""A v2 drain whose node stops answering makes no progress and does no harm, and resumes when it answers."""

import space_smoke as smoke

# How long the run watches the drain wait on a node that doesn't answer:
# several loop ticks past the call's own timeout.
UNANSWERED_SECONDS = 150


# An unanswered call raising `node_drain.v2.unanswered` and recording the
# error is a crate test (`node_drain/v2/tests.rs`). Here node-api on the
# drained worker stops answering once the drain has sealed it and started
# its suspends: the node finishes what it started, but nothing it does is
# recorded, so no restore is enqueued and the residents wait, suspended,
# where they are; once node-api answers again the next call reports them
# and the drain finishes.
@smoke.scenario(
    "native.v2_drain_waits_for_an_unreachable_node",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    ignored="cuts a worker's node-api for minutes, too slow for the per-merge set",
)
def v2_drain_waits_for_an_unreachable_node(test: smoke.CommandSandboxFixture) -> None:
    """A v2 drain of a started and a paused resident seals the worker; then
    its node-api stops answering. The loop records the failed `DrainNode`
    call, enqueues no restore and leaves the node `draining`, and each
    resident stays on the worker in its lifecycle or is suspended there by
    the drain, unplaced, with no VM elsewhere. Once node-api answers, the node reports
    both evacuated and reaches `drained`, and each comes back in its
    lifecycle on the other worker, restored once."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started and a paused resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents(
                {"started": 1, "paused": 1}, mib=256
            )

        with test.phase("drain; once sealed, node-api stops answering"):
            epoch = drain.drain()
            drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch,
                "the node never reported sealed under the drain's epoch",
                timeout=180,
            )
            node_api = drain.block("node-api")

        with test.phase("no progress, the failed call recorded, no harm"):
            deadline_error = drain.wait_for_last_error(
                "DrainNode failed", timeout=UNANSWERED_SECONDS
            )
            test.record("drain_last_error", deadline_error)
            test.wait(UNANSWERED_SECONDS, reason="while the node does not answer")
            restores = {
                drain.bare_id(sandbox_id): drain.drain_restores(sandbox_id)
                for sandbox_id in lifecycles
            }
            residency = {
                drain.bare_id(sandbox_id): drain.residency(sandbox_id) for sandbox_id in lifecycles
            }
            test.record("unanswered_state", {"restores": restores, "residency": residency})
            test.verify(
                drain.drain_phase() == "draining"
                and not any(restores.values())
                and all(
                    (lifecycle in ("started", "paused") and node == drain.node_id)
                    or (lifecycle == "suspended" and node is None)
                    for lifecycle, node in residency.values()
                ),
                "an unanswered drain moved, enqueued a restore, or moved a resident off the node",
                drain_phase=drain.drain_phase(),
                restores=restores,
                residency=residency,
            )
            for sandbox_id in lifecycles:
                drain.expect_no_vm_off_the_drained_worker(sandbox_id)

        with test.phase("node-api back: the drain finishes"):
            node_api.release()
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)
            drain.expect_all_back(lifecycles)
            drain.expect_started_memory(sandboxes, lifecycles, digests)
            test.sandbox.wake()

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
