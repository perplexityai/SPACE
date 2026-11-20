"""A v2 drain survives the cluster-controller restarting, and its running residents stay held while the controller is down past the deadline."""

import space_smoke as smoke

# The drain's hold: long enough for a controller restart, the blocked
# uploads' retries and the scale to zero to land before it on a slow bench.
HOLD_SECONDS = 900
# The latest the controller may go down before the deadline for the run to
# show the node holding across it.
DOWN_MARGIN_SECONDS = 60
# How long past the deadline the run watches the running residents held with
# the controller down.
HELD_PAST_DEADLINE_SECONDS = 90


# The loop's idempotence and the `evacuate` it sends from the stored
# deadline are crate tests (`node_drain/v2/tests.rs`,
# `node_drain/store/v2_sink/tests.rs`); the node holding a running sandbox
# until a call evacuates, and suspending it from then on, is the planner's
# (`service/drain_node/tests.rs`). This is each seam on real workers.
# Uploads blocked on the drained worker hold the drain after its seal with
# nothing evacuated while the controller restarts, and the restarted
# controller keeps the drain's generation and its stored deadline. The node
# keeps no deadline of its own, so with the controller down across the
# deadline it holds the running residents; once the controller is back, its
# first call evacuates and the node suspends them.
@smoke.scenario(
    "native.v2_drain_survives_controller_restarts",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=60,
    tier="manual",
    manual_reason="restarts and scales the cluster-controller, too invasive for the per-merge set",
)
def v2_drain_survives_controller_restarts(test: smoke.CommandSandboxFixture) -> None:
    """A v2 drain, with a fifteen-minute hold, of two started, a paused and
    a stopped resident outlives a controller restart after the seal, with
    uploads blocked and nothing evacuated: the restarted controller stores the
    node's report under the same generation and the deadline is unchanged. Once uploads are let through
    the paused and stopped residents are made durable. With the controller
    scaled to zero across the deadline, the started residents keep running
    on the worker past it: the node keeps no deadline of its own. Once the
    controller is back the node suspends them and reaches `drained`; every
    VM resident is suspended and unplaced and the stopped one stopped and
    unplaced, a wake lands each VM on the other worker with its memory
    intact, and `:undrain` cancels the drain."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started pair, a paused and a stopped resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents(
                {"started": 2, "paused": 1, "stopped": 1}
            )
            started = [sandbox_id for sandbox_id, lifecycle in lifecycles.items() if lifecycle == "started"]

        with test.phase("uploads blocked: drain with a hold, and restart the controller once the node reported"):
            uploads = drain.block("object-store")
            drain.drain(hold=HOLD_SECONDS)
            first = drain.wait_for_report(
                lambda report: drain.reported(report)
                and len(drain.pending_waits(report)) == len(lifecycles),
                "the node never reported every resident pending under the drain's generation",
                timeout=180,
            )
            before = drain.expect_view_deadline(first)
            test.verify(
                not first.get("evacuated"),
                "a resident was evacuated with its uploads blocked",
                report=first,
            )
            drain.restart_controller()
            reported_before = first.get("reported_at")
            after = drain.wait_for_report(
                lambda report: drain.reported(report) and report.get("reported_at") != reported_before,
                "the restarted controller never stored a new report under the drain's generation",
                timeout=180,
            )
            test.record("report_after_restart", after)
            test.verify(
                drain.expect_view_deadline(after) == before
                and drain.expect_stored_deadline(HOLD_SECONDS) == drain.deadline,
                "the restarted controller moved the drain's deadline",
                before=before,
                after=after.get("deadline"),
            )

        with test.phase("uploads let through: the paused and stopped residents are made durable"):
            uploads.release()
            for sandbox_id, lifecycle in lifecycles.items():
                if lifecycle != "started":
                    drain.expect_durable(sandbox_id, lifecycle, timeout=600)
            for sandbox_id in started:
                drain.expect_held(sandbox_id)

        with test.phase("the controller down across the deadline: the node holds the running ones"):
            with drain.controller_down():
                ahead = drain.seconds_to_deadline()
                test.verify(
                    ahead > DOWN_MARGIN_SECONDS,
                    "the controller went down too close to the deadline to show the node holding across it",
                    seconds_to_deadline=round(ahead),
                )
                drain.expect_started_until_deadline(started, margin=-HELD_PAST_DEADLINE_SECONDS)

        with test.phase("the controller back: the node suspends them and the drain finishes"):
            for sandbox_id in started:
                drain.expect_suspended_by_the_node(sandbox_id)
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)
            drain.expect_all_evacuated(lifecycles)
            drain.wake_all_elsewhere(sandboxes, lifecycles, digests)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
