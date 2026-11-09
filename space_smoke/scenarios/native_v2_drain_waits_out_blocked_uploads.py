"""A v2 drain whose uploads are blocked reports them pending, then blocked, and finishes once they clear."""

import time

import space_smoke as smoke


# The bound is a crate test (`service/drain_node/tests.rs`: an action that
# fails three times under one epoch is `blocked`, and still retried). This
# is the object store unreachable from the drained worker alone: its
# uploads fail until the backup worker gives up on them three times, the
# node reports each resident `blocked` with the failure, the drain holds,
# and once the path is back the same epoch finishes.
@smoke.scenario(
    "native.v2_drain_waits_out_blocked_uploads",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=60,
    ignored="holds a drain until its upload retries are spent, too slow for the per-merge set",
)
def v2_drain_waits_out_blocked_uploads(test: smoke.CommandSandboxFixture) -> None:
    """With the drained worker's path to the object store cut, a v2 drain
    of a started and a stopped resident reports both pending, then both
    `blocked` with the failed upload once their retries are spent, and stays
    `draining` with nothing evacuated. Once the path is back, the node
    reports both evacuated and reaches `drained`; the started one is
    restored once on the other worker and the stopped one left stopped and
    unplaced."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started and a stopped resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents({"started": 1, "stopped": 1})

        with test.phase("uploads blocked: the drain reports both pending"):
            uploads = drain.block("object-store")
            blocked_at = time.monotonic()
            epoch = drain.drain()
            drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and len(drain.pending_actions(report)) == len(lifecycles),
                "the node never reported both residents pending",
                timeout=300,
            )

        with test.phase("past the bound, both are blocked with the failed upload"):
            ids = {drain.bare_id(sandbox_id) for sandbox_id in lifecycles}
            report = drain.wait_for_report(
                lambda report: {
                    entry.get("sandbox_id")
                    for entry in report.get("blocked", [])
                    if "failed 3 times" in str(entry.get("reason"))
                }
                >= ids,
                "the node never reported the blocked uploads blocked",
                timeout=2400,
            )
            test.record("blocked_report", report)
            test.record("seconds_to_blocked", round(time.monotonic() - blocked_at))
            test.verify(
                not report.get("evacuated") and drain.drain_phase() == "draining",
                "a drain with its uploads blocked evacuated something or left draining",
                report=report,
                drain_phase=drain.drain_phase(),
            )

        with test.phase("the path back: the same epoch finishes"):
            uploads.release()
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)
            drain.expect_all_back(lifecycles)
            drain.expect_started_memory(sandboxes, lifecycles, digests)
            test.sandbox.wake()

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
