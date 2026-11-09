"""A v2 drain survives the drained worker's sandbox-manager restarting mid-suspend and mid-upload."""

import space_smoke as smoke


# What a restart keeps is crate tests: the drain's suspend records the
# lifecycle before it suspends (`service/drain_node/tests.rs`), and an upload
# job outlives the process (`snapshot_upload_jobs` tests). This is the seam
# on a real worker. Uploads are blocked on it from the start, so the drain's
# suspends end local-only; sandbox-manager restarts while they run and again
# while their uploads wait, and the drain finishes once uploads are let
# through.
@smoke.scenario(
    "native.v2_drain_survives_sandbox_manager_restarts",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    ignored="restarts a worker's sandbox-manager under a drain, too invasive for the per-merge set",
)
def v2_drain_survives_sandbox_manager_restarts(test: smoke.CommandSandboxFixture) -> None:
    """With uploads blocked on the drained worker, its sandbox-manager
    restarts while the drain's suspends run and again while their uploads
    wait; once uploads are let through the node reports each resident
    evacuated with its lifecycle and reaches `drained`, and each comes back
    in its lifecycle with one VM, landed by one drain restore, its memory
    intact."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin two started, a paused and a stopped resident to one worker"):
            sandboxes, lifecycles, digests = drain.pin_residents(
                {"started": 2, "paused": 1, "stopped": 1}, mib=256
            )

        with test.phase("uploads blocked: drain, and restart sandbox-manager mid-suspend"):
            uploads = drain.block("object-store")
            epoch = drain.drain()
            suspending = drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and "suspend" in drain.pending_actions(report).values(),
                "the node never reported a drain suspend running",
                timeout=180,
            )
            test.record("report_at_first_restart", suspending)
            drain.restart_sandbox_manager()

        with test.phase("restart sandbox-manager again while every upload waits"):
            waiting = drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch
                and set(drain.pending_actions(report).values()) <= {"upload", "publish_disk"}
                and len(drain.pending_actions(report)) == len(lifecycles),
                "the node never reported every resident waiting on its upload",
                timeout=600,
            )
            test.record("report_at_second_restart", waiting)
            drain.restart_sandbox_manager()

        with test.phase("uploads let through: every resident is evacuated; the node drains"):
            uploads.release()
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)

        with test.phase("every resident ends in its lifecycle with one VM"):
            drain.expect_all_back(lifecycles)
            drain.expect_started_memory(sandboxes, lifecycles, digests)
            test.sandbox.wake()

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
