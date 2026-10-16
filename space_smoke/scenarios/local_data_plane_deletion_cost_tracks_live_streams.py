"""The data-plane pod holding a live stream ranks above idle pods for scale-in."""

import space_smoke as smoke

# Long enough for the "10" write (a raise, so one 10 s tick) and its 60 s
# settle budget, and to put the close more than 30 s after that write so the
# lowering back to "0" isn't held back.
HOLD_SECONDS = 90


# Read-only on the cluster: the scenario observes the annotation the cluster
# controller writes and changes nothing itself. ~2 min on the kind bench (one
# settle, one hold that ends when the command exits, one settle back).
@smoke.scenario(
    "local.data_plane_deletion_cost_tracks_live_streams",
    sandbox=smoke.SandboxConfig(timeout=900),
)
def data_plane_deletion_cost_tracks_live_streams(test: smoke.KindEcw) -> None:
    """With no live stream every Ready data-plane pod reads deletion cost 0;
    while one ECW process stream is attached, the pod serving it reads
    `controller.kubernetes.io/pod-deletion-cost: "10"` and the others stay at
    0; once the process exits and the stream closes, the pod returns to 0."""

    cost = test.data_plane_deletion_cost

    with test.phase("no live stream: every Ready data-plane pod reads cost 0"):
        cost.expect_all_idle(label="before_stream")

    with test.phase("hold one ECW process stream open"):
        process = test.sandbox.start_background(f"sleep {HOLD_SECONDS}", timeout=HOLD_SECONDS + 60)
        test.record("held_process_pid", process.pid)

    with test.phase("the pod serving the stream reads cost 10"):
        cost.expect_one_stream()

    with test.phase("the process exits and its stream closes"):
        result = process.wait()
        test.verify(
            result.exit_code == 0,
            "held ECW process did not exit cleanly",
            command_exit_code=result.exit_code,
            command_stderr=result.stderr,
        )

    with test.phase("with the stream closed, the pod returns to cost 0"):
        cost.expect_all_idle(label="after_stream")
