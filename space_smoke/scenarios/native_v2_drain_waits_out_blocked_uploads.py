"""A v2 drain whose uploads are blocked keeps them pending, never blocked, marks itself stalled, and finishes once they clear."""

import time

import space_smoke as smoke
from space_smoke.drain_v2 import PENDING_SINCE_FIELD, timestamp_seconds


# A failed action staying pending and tried again, and the stall rule, are
# crate tests (`service/drain_node/tests.rs`; `node_drain/v2/tests.rs`: past
# the deadline, a drain whose evacuated set hasn't grown for the stall bound
# is stalled). This is the object store unreachable from the drained worker
# alone: the node reports the resident still pending and no sandbox
# blocked, the controller marks the drain stalled the stall bound past its
# deadline (Kind's values shorten the bound; `require_drain_v2` reads it),
# the drain holds, and once the path is back the same drain finishes. A
# pending entry's reason is display text, so the run doesn't read the
# failure in it. The path is cut with a TCP reset, so each upload attempt
# fails at once rather than at a timeout.
@smoke.scenario(
    "native.v2_drain_waits_out_blocked_uploads",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=40,
    tier="manual",
    manual_reason="holds a drain stalled behind a cut object store, too slow for the per-merge set",
)
def v2_drain_waits_out_blocked_uploads(test: smoke.CommandSandboxFixture) -> None:
    """With the drained worker's path to the object store cut, a v2 drain,
    due now, of a started resident reports it pending, with the time it
    entered its state. The stall bound past its deadline with nothing
    evacuated, its `drain_last_error` says it is stalled; the resident is
    still pending, the report names no sandbox blocked, and the drain stays
    `draining` with nothing evacuated. Once the path is back, the node
    reports it evacuated and reaches `drained`; the resident is left
    suspended and unplaced until a wake lands it on the other worker with
    its memory intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents({"started": 1})
            ids = {drain.bare_id(sandbox_id) for sandbox_id in lifecycles}

        with test.phase("uploads blocked: the drain reports the resident pending"):
            uploads = drain.block("object-store")
            blocked_at = time.monotonic()
            drain.drain()
            first = drain.wait_for_report(
                lambda report: drain.reported(report)
                and set(drain.pending_entries(report)) >= ids
                and all(
                    timestamp_seconds(drain.pending_entries(report)[sandbox_id].get(PENDING_SINCE_FIELD))
                    is not None
                    for sandbox_id in ids
                ),
                "the node never reported the resident pending, with its since",
                timeout=180,
            )
            # `since` is informational: it moves when a row changes state,
            # as the resident's does when its local suspend lands.
            test.record(
                "pending_since",
                {
                    sandbox_id: drain.pending_entries(first)[sandbox_id].get(PENDING_SINCE_FIELD)
                    for sandbox_id in ids
                },
            )

        with test.phase("past the deadline with nothing evacuated for the stall bound: the drain is stalled"):
            remaining = drain.deadline + drain.gate.stall_bound_seconds - time.time()
            drain.wait_for_stall(timeout=max(remaining, 0) + 60)
            report = drain.report()
            test.record("stalled_report", report)
            test.record("seconds_to_stall", round(time.monotonic() - blocked_at))
            drain.expect_nothing_blocked_per_sandbox(report)
            test.verify(
                set(drain.pending_entries(report)) >= ids
                and not drain.evacuated_residents(report, lifecycles)
                and drain.drain_phase() == "draining",
                "a stalled drain stopped reporting its resident pending, evacuated something, or left draining",
                report=report,
                drain_phase=drain.drain_phase(),
            )

        with test.phase("the path back: the same drain finishes"):
            uploads.release()
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)
            drain.expect_all_evacuated(lifecycles)
            drain.wake_all_elsewhere(sandboxes, digests)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
