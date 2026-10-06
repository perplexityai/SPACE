"""A stopped sandbox restarts from the rootfs its node kept."""

import shlex

import space_smoke as smoke

PATH = "/home/user/data/stopped.txt"
MARKER = "space-smoke-stopped-restart-ok"

# The shell suite this replaces (`smoke-kind-sandbox-stop-start.sh`) also ran a
# copy of the disk-only stopped-restore selector's SQL through psql and asserted
# it matched nothing without a `disk` pin. That phase never invoked the Rust
# selector, so it could not catch the selector drifting from the copy, and the
# typed framework exposes no ad-hoc SQL by design; the dormancy precondition
# belongs in a Postgres-backed crate test. It is recorded in the coverage map,
# not ported.
@smoke.scenario(
    "native.stopped_restart_preserves_rootfs",
    api_surfaces=("native",),
)
def stopped_restart_preserves_rootfs(test: smoke.CommandSandboxFixture) -> None:
    """Stopping a sandbox parks its rootfs on the node and reports `stopped`;
    starting it again runs from that rootfs, so a file written before the stop
    reads back unchanged after the restart."""

    for cycle in (1, 2):
        marker = f"{MARKER}-{test.sandbox.sandbox_id}-{cycle}"
        with test.phase(f"write the latest rootfs payload for cycle {cycle}"):
            written = test.run_command_when_ready(
                f"mkdir -p {shlex.quote(PATH.rsplit('/', 1)[0])} && "
                f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
            )
            test.verify(
                written.exit_code == 0,
                "failed to write the payload before stop",
                command_stderr=written.stderr,
            )
        with test.phase(f"stop and restart cycle {cycle}"):
            test.stop_on_node()
            failure = test.start_from_node()
            test.verify(
                failure is None,
                "the stopped sandbox did not restart",
                restart_error=failure,
                cycle=cycle,
            )
        with test.phase(f"verify the latest rootfs payload after cycle {cycle}"):
            observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
            test.expect_command(observed, stdout=marker)

    with test.phase("delete stopped sandbox and verify node cleanup"):
        worker = test.local_cleanup_worker()
        test.stop_on_node()
        test.delete_stopped_and_wait_for_local_cleanup(worker)
