"""A v2 drain's seal and deadline hold across the drained worker's sandbox-manager restarting on either side of the deadline."""

import space_smoke as smoke
from space_smoke.drain_v2 import PENDING_RUNNING_FIELD, UPLOAD_RETRY_SECONDS, WAIT_MOVING

# The drain's first hold: long enough for a sandbox-manager restart to land
# before the deadline on any bench. Once the restarted node has reported,
# the run brings the deadline in (`LEAD_SECONDS`) rather than wait out the
# rest of a hold sized for the slowest bench.
HOLD_SECONDS = 900
# How long before the deadline the watch of the started residents held ends.
HELD_MARGIN_SECONDS = 5
# How far ahead the deadline lands once brought in: a 25 s watch of the
# restarted node holding the started residents, at least two of its 10 s
# re-plans and four controller ticks, until `HELD_MARGIN_SECONDS` before it.
# The node is never told a deadline (`bring_deadline_to`), so nothing waits
# for it to hear this one.
LEAD_SECONDS = 25 + HELD_MARGIN_SECONDS
# The latest the first restart may end before the deadline.
RESTART_MARGIN_SECONDS = 30


# What a restart keeps is crate tests: the seal row (`activation_seal/tests.rs`),
# the node holding until a call evacuates, since whether the generation is
# evacuating lives in memory only (`service/drain_node/tests.rs`), and an
# upload job outliving the process (`snapshot_upload_jobs` tests). This is
# the seam on a real worker. Uploads are blocked on it from the start, so the
# drain's suspends end local-only. sandbox-manager restarts before the
# deadline, while it holds the running residents, and again after it, while
# their uploads wait; each time the node comes back sealed under the
# generation, answering the controller's next call with its report, the
# controller's calls keep the stored deadline, and the drain finishes once
# uploads are let through.
@smoke.scenario(
    "native.v2_drain_survives_sandbox_manager_restarts",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    tier="manual",
    manual_reason="restarts a worker's sandbox-manager under a drain, too invasive for the per-merge set",
)
def v2_drain_survives_sandbox_manager_restarts(test: smoke.CommandSandboxFixture) -> None:
    """With uploads blocked on the drained worker, a v2 drain with a
    hold of two started and a paused resident seals it. Its sandbox-manager restarts before the deadline: the node reports
    under the same generation again, the deadline is unchanged, and the
    started residents keep running until it, once brought in by a repeat
    `:drain`. Past it the node suspends them,
    and it restarts again while every upload waits: it reports under the
    generation again, and no started resident runs again. Once uploads are let through the node
    reports each resident evacuated and reaches `drained`; every resident is
    suspended and unplaced, and a wake lands each on the other worker with
    its memory intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin two started and a paused resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents({"started": 2, "paused": 1}, mib=256)
            started = [sandbox_id for sandbox_id, lifecycle in lifecycles.items() if lifecycle == "started"]

        with test.phase("uploads blocked: drain with a hold; restart sandbox-manager before the deadline"):
            uploads = drain.block("object-store")
            drain.drain(hold=HOLD_SECONDS)
            first = drain.wait_for_reported()
            persisted = drain.expect_view_deadline(first)
            for sandbox_id in started:
                drain.expect_held(sandbox_id)
            drain.restart_sandbox_manager()
            ahead = drain.seconds_to_deadline()
            test.verify(
                ahead > RESTART_MARGIN_SECONDS,
                "sandbox-manager's restart outlasted the hold",
                seconds_to_deadline=round(ahead),
            )

        with test.phase("the restarted node reports under the drain again and holds until the deadline"):
            after_first = drain.wait_for_report(
                lambda report: drain.reported(report)
                and report.get("reported_at") != first.get("reported_at"),
                "the restarted node never reported again under the drain's generation",
                timeout=180,
            )
            test.record("report_after_first_restart", after_first)
            shown = drain.expect_view_deadline(after_first)
            test.verify(
                shown == persisted,
                "the drain's deadline moved across the restart",
                persisted=persisted,
                shown=shown,
            )
            drain.bring_deadline_to(LEAD_SECONDS)
            persisted = drain.expect_view_deadline(drain.report())
            drain.expect_started_until_deadline(started, margin=HELD_MARGIN_SECONDS)

        with test.phase("past the deadline: restart sandbox-manager again while every upload waits"):
            drain.wait_past_deadline()
            waiting = drain.wait_for_report(
                lambda report: drain.reported(report)
                and len(drain.pending_entries(report)) == len(lifecycles)
                and all(
                    entry.get("wait") == WAIT_MOVING and entry.get(PENDING_RUNNING_FIELD) is False
                    for entry in drain.pending_entries(report).values()
                ),
                "the node never reported every resident moving, none running, its upload waiting",
                timeout=300,
            )
            test.record("report_at_second_restart", waiting)
            drain.restart_sandbox_manager()
            after_second = drain.wait_for_report(
                lambda report: drain.reported(report)
                and report.get("reported_at") != waiting.get("reported_at"),
                "the restarted node never reported again under the drain's generation",
                timeout=180,
            )
            shown = drain.view_deadline(after_second)
            # The node's own rows: with its upload waiting, a suspend the node
            # made hasn't reached the control-plane row, which reads `started`.
            node_states = {
                drain.bare_id(sandbox_id): drain.node_state(sandbox_id) for sandbox_id in started
            }
            test.record("after_second_restart", {"report": after_second, "node_states": node_states})
            test.verify(
                shown == persisted
                and all(state == "suspended" for state in node_states.values()),
                "the node restarted past the deadline lost its seal's deadline, or ran a suspended resident again",
                persisted=persisted,
                shown=shown,
                node_states=node_states,
            )

        with test.phase("uploads let through: every resident is evacuated; the node drains"):
            uploads.release()
            report = drain.expect_drained(timeout=UPLOAD_RETRY_SECONDS)
            drain.expect_evacuated(report, lifecycles)

        with test.phase("every resident is left suspended and unplaced; a wake lands each elsewhere"):
            drain.expect_all_evacuated(lifecycles)
            drain.wake_all_elsewhere(sandboxes, digests)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
