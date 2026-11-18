"""A v2 drain leaves every resident durable; a user call brings each back elsewhere."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import ResidentLifecycleSandbox
from space_smoke.drain_v2 import ACCESS_WAKE_POLICY

PATH = "/tmp/space-smoke-v2-drain.txt"


# The drain v2's parts are crate tests: sandbox-manager's `DrainNode`
# planner, seal and report (`service/drain_node/tests.rs`,
# `activation_seal/tests.rs`), the controller's loop against a fake sink
# (`node_drain/v2/tests.rs`) and its statements against Postgres
# (`node_drain/store/v2_sink/tests.rs`). This scenario is the seam on real
# workers: a sealed worker's own suspends and uploads, the report rebuilt
# from its rows, and each resident's next user call: a wake, a call under a
# wake policy that wakes it, and a call under one that doesn't. A suspended
# resident whose upload is still pending and a warm-pool member are planner
# rows only: Kind's uploads land before a drain can see one pending, and it
# runs no warm pool. A v2 drain doesn't move a stopped sandbox:
# `native.v2_drain_refuses_stopped_sandboxes` is that. The terminate that
# follows a drain is `native.v2_drain_terminates_its_worker`.
@smoke.scenario(
    "native.v2_drain_evacuates_every_resident",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    # Three suspends, their uploads, three wakes and the drained edge on a
    # slow KVM bench.
    timeout_minutes=45,
)
def v2_drain_evacuates_every_resident(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain`, due now, of a worker holding two started sandboxes (one
    under a policy that wakes it on access) and a paused one seals it; the
    node suspends them itself, reports each evacuated with its snapshot, and
    reaches `drained`. Each resident is left suspended and unplaced, with no
    VM on either worker, and stays so until a user call: a call to the
    started one with no implicit wake is refused until a wake lands it on the
    other worker; a call to the one woken on access runs there; a wake lands
    the paused one there. Each sandbox's file is intact, and `:undrain`
    cancels the drain."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin two started and a paused resident to one worker"):
            # A v2 drain needs the node's instance recorded; re-registering
            # under a fake one deletes the pods there, so the fixture's
            # worker is left alone.
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            residents = {
                "started": drain.create_resident(),
                "started, woken on access": drain.create_resident(auto_lifecycle=ACCESS_WAKE_POLICY),
                "paused": drain.create_resident(),
            }
            for sandbox in residents.values():
                drain.write_file(sandbox, PATH)
            cast(ResidentLifecycleSandbox, residents["paused"]).pause()
            test.sandbox.suspend()
            drain.release_other_worker()
            resident_ids = [sandbox.sandbox_id for sandbox in residents.values()]

        with test.phase("drain the worker with engine v2, due now"):
            drain.drain()

        with test.phase("the node reports each resident evacuated and reaches drained"):
            report = drain.expect_drained()
            drain.expect_evacuated(report, resident_ids)

        with test.phase("each resident is left suspended and unplaced, until a user call"):
            drain.expect_all_evacuated(resident_ids)

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

        with test.phase("each sandbox's file survived"):
            for sandbox in residents.values():
                drain.expect_file(sandbox, PATH)
            test.sandbox.wake()

        with test.phase("undrain: the drain is cancelled on the node"):
            drain.undrain_and_expect_cancelled()
