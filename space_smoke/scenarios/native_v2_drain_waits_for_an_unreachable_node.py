"""A v2 drain whose node stops answering keeps evacuating on the node, and finishes when it answers."""

import space_smoke as smoke

# How long the run watches the drain wait on a node that doesn't answer:
# several loop ticks past the call's own timeout.
UNANSWERED_SECONDS = 150


# An unanswered call raising `node_drain.v2.unanswered` and recording the
# error is a crate test (`node_drain/v2/tests.rs`), and the node's driver
# re-planning every 10 s while sealed is the planner's
# (`service/drain_node/tests.rs`). Here node-api on the drained worker stops
# answering once the drain has sealed it: the controller's calls fail, but
# the node's own driver goes on making each resident durable, so the
# residents reach `suspended` there with nothing answered; once node-api
# answers again the next call reports them and the drain finishes.
@smoke.scenario(
    "native.v2_drain_waits_for_an_unreachable_node",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    tier="manual",
    manual_reason="cuts a worker's node-api for minutes, too slow for the per-merge set",
)
def v2_drain_waits_for_an_unreachable_node(test: smoke.CommandSandboxFixture) -> None:
    """A v2 drain, due now, of a started and a paused resident seals the
    worker; then its node-api stops answering. The loop records the failed
    `DrainNode` call and leaves the node `draining`, while the node keeps
    evacuating: each resident is made durable there, `suspended` with a
    stored snapshot and no VM on the other worker. Once node-api answers, the
    node reports both evacuated and reaches `drained`; both are suspended and
    unplaced, and a wake lands each on the other worker with its memory
    intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started and a paused resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents(
                {"started": 1, "paused": 1}, mib=256
            )

        with test.phase("drain; once sealed, node-api stops answering"):
            drain.drain()
            drain.wait_for_reported()
            node_api = drain.block("node-api")

        with test.phase("the call fails and the drain waits, while the node keeps evacuating"):
            error = drain.wait_for_last_error("DrainNode failed", timeout=UNANSWERED_SECONDS)
            test.record("drain_last_error", error)
            for sandbox_id, lifecycle in lifecycles.items():
                drain.expect_durable(sandbox_id, lifecycle, timeout=600)
                drain.expect_no_vm_off_the_drained_worker(sandbox_id)
            test.wait(UNANSWERED_SECONDS, reason="while the node does not answer")
            residency = {
                drain.bare_id(sandbox_id): drain.residency(sandbox_id) for sandbox_id in lifecycles
            }
            test.record("unanswered_state", {"residency": residency})
            test.verify(
                drain.drain_phase() == "draining"
                and all(
                    lifecycle == "suspended" and node in (None, drain.node_id)
                    for lifecycle, node in residency.values()
                ),
                "an unanswered drain left draining, or a resident left the drained worker",
                drain_phase=drain.drain_phase(),
                residency=residency,
            )

        with test.phase("node-api back: the drain finishes"):
            node_api.release()
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)
            drain.expect_all_evacuated(lifecycles)
            drain.wake_all_elsewhere(sandboxes, lifecycles, digests)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
