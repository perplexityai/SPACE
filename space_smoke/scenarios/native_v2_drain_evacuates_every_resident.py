"""A v2 drain makes every resident durable elsewhere; a stopped one boots on its next start."""

import shlex
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox, StoppableLifecycleSandbox

PATH = "/tmp/space-smoke-v2-drain.txt"


# The drain v2's parts are crate tests: sandbox-manager's `DrainNode` planner
# and seal (`service/drain_node/tests.rs`, `activation_seal/tests.rs`), the
# controller's loop against a fake sink (`node_drain/v2/tests.rs`) and its
# statements against Postgres (`node_drain/store/v2_sink/tests.rs`), the
# drain restore operation (`kubernetes/tests/drain_restore.rs`) and the
# stopped start's materialization (`drain_restore_tests.rs`). This scenario
# is the seam on real workers: a sealed worker's own suspend, upload and disk
# publication, the controller recording each report, each restore landing
# the sandbox, with its file, in the lifecycle it had on the other worker,
# and a stopped sandbox left stopped until a user start boots it there. A
# suspended resident whose upload is still pending and a warm-pool member
# are planner rows only: Kind's uploads land before a drain can see one
# pending, and it runs no warm pool. The terminate that follows a drain is
# `native.v2_drain_terminates_its_worker`.
@smoke.scenario(
    "native.v2_drain_evacuates_every_resident",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Three residents' suspends or disk publication, their uploads, two
    # restores, a stopped start and the drained edge on a slow KVM bench.
    timeout_minutes=45,
)
def v2_drain_evacuates_every_resident(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` of a worker holding a started, a paused and two stopped
    sandboxes seals it; the node suspends the VMs and publishes the stopped disk
    itself, and reports each evacuated with the lifecycle it had. One drain
    restore each, on its first attempt, brings the started and the paused
    back in their lifecycles on the other worker; the stopped ones stay
    stopped and unplaced, with no drain restore. The node reports `done`
    under the drain's epoch and reaches `drained`. A user start boots each
    stopped sandbox on the other worker; a wake of one first is refused and
    leaves it stopped and unplaced. Each sandbox's file is intact, and
    `:undrain` cancels the epoch."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a started, a paused and a stopped resident to one worker"):
            drain.pin_to_resident_worker(away_from_fixture=True)
            residents = {
                "started": drain.create_resident(),
                "paused": drain.create_resident(),
                "stopped": drain.create_resident(),
                "stopped, woken": drain.create_resident(),
            }
            for lifecycle, sandbox in residents.items():
                written = drain.run_on(
                    sandbox, f"printf %s {shlex.quote(sandbox.sandbox_id)} > {shlex.quote(PATH)}"
                )
                test.verify(written.exit_code == 0, f"failed to write the {lifecycle} resident's file")
            cast(ResidentLifecycleSandbox, residents["paused"]).pause()
            cast(StoppableLifecycleSandbox, residents["stopped"]).stop()
            cast(StoppableLifecycleSandbox, residents["stopped, woken"]).stop()
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain the worker with engine v2"):
            drain.drain()

        with test.phase("the node reports each resident evacuated and reaches drained"):
            report = drain.expect_drained()
            drain.expect_evacuated(
                report,
                {
                    sandbox.sandbox_id: lifecycle.split(",")[0]
                    for lifecycle, sandbox in residents.items()
                },
            )

        with test.phase("the started and the paused are back, restored once, on the other worker"):
            for lifecycle in ("started", "paused"):
                drain.expect_back_elsewhere(residents[lifecycle].sandbox_id, lifecycle)
                drain.expect_restored_once(residents[lifecycle].sandbox_id)

        with test.phase("the stopped ones are stopped and unplaced; a wake is refused, a start boots each elsewhere"):
            stopped, woken = residents["stopped"], residents["stopped, woken"]
            drain.expect_stopped_unplaced(stopped.sandbox_id)
            drain.expect_stopped_unplaced(woken.sandbox_id)
            cast(StoppableLifecycleSandbox, stopped).start()
            drain.expect_back_elsewhere(stopped.sandbox_id, "started")
            # A stopped sandbox is started, never woken: a wake, explicit or
            # implicit, is refused before it reaches a node, so none can pin
            # it to the drained one.
            try:
                cast(ResidentLifecycleSandbox, woken).wake()
            except Exception as error:  # noqa: BLE001 - the refusal is the expected outcome
                refusal: str | None = repr(error)[:500]
            else:
                refusal = None
            test.record("released_stopped_wake_refusal", refusal)
            test.verify(
                refusal is not None and "cannot be woken while lifecycle is stopped" in refusal,
                "a wake of a released stopped sandbox was not refused",
                refusal=refusal,
            )
            drain.expect_stopped_unplaced(woken.sandbox_id)
            cast(StoppableLifecycleSandbox, woken).start()
            drain.expect_back_elsewhere(woken.sandbox_id, "started")

        with test.phase("each sandbox's file survived"):
            cast(ResidentLifecycleSandbox, residents["paused"]).wake()
            for sandbox in residents.values():
                observed = drain.run_on(sandbox, f"cat {shlex.quote(PATH)}")
                test.verify(
                    observed.exit_code == 0 and observed.stdout == sandbox.sandbox_id,
                    "a sandbox's file did not survive the v2 drain",
                    sandbox_id=sandbox.sandbox_id,
                    stdout=observed.stdout,
                )
            test.sandbox.wake()

        with test.phase("undrain: the epoch is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
