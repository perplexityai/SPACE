"""A v2 drain leaves every resident durable; a user call brings each back elsewhere."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox, StoppableLifecycleSandbox
from space_smoke.drain_v2 import ACCESS_WAKE_POLICY

PATH = "/tmp/space-smoke-v2-drain.txt"


# The drain v2's parts are crate tests: sandbox-manager's `DrainNode`
# planner, seal and report (`service/drain_node/tests.rs`,
# `activation_seal/tests.rs`), the controller's loop against a fake sink
# (`node_drain/v2/tests.rs`) and its statements against Postgres
# (`node_drain/store/v2_sink/tests.rs`), and the stopped start's handoff
# (`kubernetes/tests/drain_restore.rs`). This scenario is the seam on real workers: a sealed
# worker's own suspends, uploads and disk publication, the report rebuilt
# from its rows, and each resident's next user call: a wake, a call under a
# wake policy that wakes it, a call under one that doesn't, and a start. A
# suspended resident whose upload is still pending and a warm-pool member
# are planner rows only: Kind's uploads land before a drain can see one
# pending, and it runs no warm pool. The terminate that follows a drain is
# `native.v2_drain_terminates_its_worker`.
@smoke.scenario(
    "native.v2_drain_evacuates_every_resident",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Three suspends, two disk publications, their uploads, three wakes, two
    # stopped starts and the drained edge on a slow KVM bench.
    timeout_minutes=45,
)
def v2_drain_evacuates_every_resident(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain`, due now, of a worker holding two started sandboxes (one
    under a policy that wakes it on access), a paused one and two stopped ones
    seals it; the node suspends the VMs and publishes the stopped disks
    itself, reports each evacuated with its artifact, a snapshot or a disk,
    and reaches `drained`. Each VM resident is left suspended and unplaced,
    with no VM on either worker, and stays so until a user call: a call to the
    started one with no implicit wake is refused until a wake lands it on the
    other worker; a call to the one woken on access runs there; a wake lands
    the paused one there. The stopped ones stay stopped on the drained
    worker, their disks pinned; a start hands each off and boots it on the
    other worker, a wake of one first is refused, and one start moves only
    its own sandbox. Each sandbox's file is intact, and `:undrain` cancels
    the drain."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin two started, a paused and two stopped residents to one worker"):
            # A v2 drain needs the node's instance recorded; re-registering
            # under a fake one deletes the pods there, so the fixture's
            # worker is left alone.
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            residents = {
                "started": drain.create_resident(),
                "started, woken on access": drain.create_resident(auto_lifecycle=ACCESS_WAKE_POLICY),
                "paused": drain.create_resident(),
                "stopped": drain.create_resident(),
                "stopped, woken": drain.create_resident(),
            }
            for sandbox in residents.values():
                drain.write_file(sandbox, PATH)
            cast(ResidentLifecycleSandbox, residents["paused"]).pause()
            cast(StoppableLifecycleSandbox, residents["stopped"]).stop()
            cast(StoppableLifecycleSandbox, residents["stopped, woken"]).stop()
            test.sandbox.suspend()
            drain.release_other_worker()
            lifecycles = {
                sandbox.sandbox_id: lifecycle.split(",")[0] for lifecycle, sandbox in residents.items()
            }

        with test.phase("drain the worker with engine v2, due now"):
            drain.drain()

        with test.phase("the node reports each resident evacuated and reaches drained"):
            report = drain.expect_drained()
            drain.expect_evacuated(report, lifecycles)

        with test.phase("each resident is left where the drain leaves it, until a user call"):
            drain.expect_all_evacuated(lifecycles)

        with test.phase("a call without an implicit wake is refused; a wake lands it on the other worker"):
            started = residents["started"]
            drain.expect_call_refused_while_suspended(started)
            drain.wake_elsewhere(started)

        with test.phase("a call under an access wake policy wakes it on the other worker"):
            woken = residents["started, woken on access"]
            drain.expect_file(woken, PATH)
            drain.expect_back_elsewhere(woken.sandbox_id, "started")

        with test.phase("a wake lands the paused one on the other worker"):
            drain.wake_elsewhere(residents["paused"])

        with test.phase("the stopped ones: a wake is refused, a start hands each off and boots it elsewhere"):
            stopped, stopped_woken = residents["stopped"], residents["stopped, woken"]
            cast(StoppableLifecycleSandbox, stopped).start()
            drain.expect_back_elsewhere(stopped.sandbox_id, "started")
            # A stopped sandbox is started, never woken: a wake, explicit or
            # implicit, is refused before it reaches a node, so none can pin
            # it to the drained one.
            try:
                cast(ResidentLifecycleSandbox, stopped_woken).wake()
            except Exception as error:  # noqa: BLE001 - the refusal is the expected outcome
                refusal: str | None = repr(error)[:500]
            else:
                refusal = None
            test.record("stopped_wake_refusal", refusal)
            test.verify(
                refusal is not None and "cannot be woken while lifecycle is stopped" in refusal,
                "a wake of a drained stopped sandbox was not refused",
                refusal=refusal,
            )
            drain.expect_stopped_at_home(stopped_woken.sandbox_id)
            cast(StoppableLifecycleSandbox, stopped_woken).start()
            drain.expect_back_elsewhere(stopped_woken.sandbox_id, "started")

        with test.phase("each sandbox's file survived"):
            for sandbox in residents.values():
                drain.expect_file(sandbox, PATH)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
