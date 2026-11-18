"""A v2 drain's seal and deadline hold across the drained worker's sandbox-manager restarting on either side of the deadline."""

import space_smoke as smoke
from space_smoke.drain_v2 import PENDING_RUNNING_FIELD, WAIT_MOVING

# The drain's hold: long enough for a sandbox-manager restart to land before
# the deadline on a slow bench.
HOLD_SECONDS = 360
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
    six-minute hold of two started, a paused and a stopped resident seals
    it. Its sandbox-manager restarts before the deadline: the node reports
    under the same generation again, the deadline is unchanged, and the
    started residents keep running until it. Past it the node suspends them,
    and it restarts again while every upload waits: it reports under the
    generation again, and no started resident runs again. Once uploads are let through the node
    reports each resident evacuated and reaches `drained`; every VM resident
    is suspended and unplaced, and a wake lands each on the other worker with
    its memory intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin two started, a paused and a stopped resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents(
                {"started": 2, "paused": 1, "stopped": 1}, mib=256
            )
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
            drain.expect_started_until_deadline(started, margin=5)

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
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)

        with test.phase("every resident ends where the drain leaves it; a wake lands each VM elsewhere"):
            drain.expect_all_evacuated(lifecycles)
            drain.wake_all_elsewhere(sandboxes, lifecycles, digests)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
