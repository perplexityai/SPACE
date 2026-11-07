"""A v2 drain makes every resident durable elsewhere, then the node is terminated."""

import shlex
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox, StoppableLifecycleSandbox

PATH = "/tmp/space-smoke-v2-drain.txt"


# The drain v2's parts are crate tests: sandbox-manager's `DrainNode` planner
# and seal (`service/drain_node/tests.rs`, `activation_seal/tests.rs`), the
# controller's loop against a fake sink (`node_drain/v2/tests.rs`) and its
# statements against Postgres (`node_drain/store/v2_sink/tests.rs`), and the
# drain restore operation (`kubernetes/tests/drain_restore.rs`). This scenario
# is the seam on real workers: a sealed worker's own suspend, upload and disk
# publication, the controller recording each report, and each restore landing
# the sandbox, with its file, in the lifecycle it had, on the other worker.
#
# On Kind, `:terminate` is `kube-delete`: the worker keeps running, but its
# Node is deleted and its row tombstoned. The run puts it back (drops the
# tombstone, restarts its kubelet) and unseals it in cleanup; what it found is
# recorded as `after_terminate`.
@smoke.scenario(
    "native.v2_drain_evacuates_every_resident",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Three residents' suspends or disk publication, their uploads, three
    # restores, the drained edge and the terminate on a slow KVM bench.
    timeout_minutes=45,
)
def v2_drain_evacuates_every_resident(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` of a worker holding a started, a paused and a stopped
    sandbox, beside a suspended one, seals it; the node suspends the VMs and
    publishes the stopped disk itself, reports each evacuated with the
    lifecycle it had, and the controller restores each, started, paused and
    stopped, on the other worker. The node then reports `done` under the
    drain's epoch and reaches `drained`; `:terminate` deletes its row. Each
    sandbox's file is intact, and the suspended one stays suspended."""

    drain = test.drain_v2

    with test.phase("pin a started, a paused and a stopped resident to one worker"):
        drain.pin_to_resident_worker()
        residents = {"started": drain.create_resident(), "paused": drain.create_resident(),
                     "stopped": drain.create_resident()}
        for lifecycle, sandbox in residents.items():
            written = drain.run_on(sandbox, 
                f"printf %s {shlex.quote(sandbox.sandbox_id)} > {shlex.quote(PATH)}"
            )
            test.verify(written.exit_code == 0, f"failed to write the {lifecycle} resident's file")
        cast(ResidentLifecycleSandbox, residents["paused"]).pause()
        cast(StoppableLifecycleSandbox, residents["stopped"]).stop()
        test.sandbox.suspend()
        drain.release_other_worker()

    with test.phase("drain the worker with engine v2"):
        drain.drain()

    with test.phase("the node reports each resident evacuated and reaches drained"):
        report = drain.expect_drained()
        drain.expect_evacuated(
            report, {sandbox.sandbox_id: lifecycle for lifecycle, sandbox in residents.items()}
        )

    with test.phase("each sandbox is back in its lifecycle on the other worker"):
        for lifecycle, sandbox in residents.items():
            drain.expect_back_elsewhere(sandbox.sandbox_id, lifecycle)

    with test.phase(":terminate deletes the drained node's row"):
        drain.terminate()

    with test.phase("each sandbox's file survived"):
        cast(ResidentLifecycleSandbox, residents["paused"]).wake()
        cast(StoppableLifecycleSandbox, residents["stopped"]).start()
        for sandbox in residents.values():
            observed = drain.run_on(sandbox, f"cat {shlex.quote(PATH)}")
            test.verify(
                observed.exit_code == 0 and observed.stdout == sandbox.sandbox_id,
                "a sandbox's file did not survive the v2 drain",
                sandbox_id=sandbox.sandbox_id,
                stdout=observed.stdout,
            )
        test.sandbox.wake()
