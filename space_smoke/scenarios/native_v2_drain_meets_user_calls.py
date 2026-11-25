"""User calls before and after a v2 drain's deadline: calls pass until the node suspends, activations wait for durability."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import RawCallSandbox, ResidentLifecycleSandbox

PATH = "/tmp/space-smoke-v2-calls.txt"
# The drain's hold: the run's calls before the deadline (a delete, a
# Rollback, a pause and its wake elsewhere) fit inside it on a slow bench.
HOLD_SECONDS = 300
# The run cuts the drained worker's uploads this long before the deadline at
# the latest, so the sandbox the node suspends there isn't durable yet.
BLOCK_MARGIN_SECONDS = 30


# `native.v2_drain_evacuates_every_resident` is the drain with no user in the
# way. Here the calls meet a drain with a hold, the gate on either side of its
# deadline: a delete as the drain begins; before the deadline, a running
# sandbox's calls and a Rollback on it, which the sealed node refuses up
# front (E10), and a wake of one its user paused, which the drain makes
# durable first; past the deadline, calls to the running sandbox, which pass
# until the node suspends it, and a wake and a command while the suspend
# can't upload. A node-initiated suspend reaches the row only once it is
# durable, so the row still says `started`: the wake answers "already
# completed" and the command is accepted and waits, an accepted gap, and
# nothing may boot the sandbox elsewhere meanwhile. Each refusal's shape is the drain gate's tests
# (`drain_guard_postgres_tests.rs`, `drain_recovery_gate_tests.rs`) and the
# node's Rollback refusal the seal's (`activation_seal/tests.rs`).
@smoke.scenario(
    "native.v2_drain_meets_user_calls",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=45,
    tier="manual",
    manual_reason="timing-sensitive on Kind: its calls past the deadline race the controller's first evacuating tick",
)
def v2_drain_meets_user_calls(test: smoke.CommandSandboxFixture) -> None:
    """Of three residents of a worker v2-drained with a five-minute hold, one
    deleted as the drain begins is gone and doesn't hold the drain. Before
    the deadline the running one answers calls; a Rollback of it is refused
    on the sealed worker, and it keeps running there with its data intact.
    A wake of the one its user paused is told to retry with a 503, never a
    409, and lands it on the other worker once the drain made it durable.
    Past the deadline, each call to the running one passes until the node
    suspends it. Once the node suspends it, with its upload cut, a wake and
    a command of it are recorded, and no second execution of it runs off
    the worker; once the upload goes through, a wake lands it on the other
    worker, its data intact. The node reaches `drained`."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin three residents to one worker; snapshot the running one's disk"):
            # A v2 drain needs the node's instance recorded; re-registering
            # under a fake one deletes the pods there, so the fixture's
            # worker is left alone.
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            serving = drain.create_resident()
            user_paused = drain.create_resident()
            deleted = drain.create_resident()
            drain.write_file(serving, PATH, "before the snapshot")
            snapshot = drain.node_disk_snapshot(serving)
            drain.write_file(serving, PATH)
            drain.write_file(user_paused, PATH)
            test.sandbox.suspend()
            drain.release_other_worker()

        with test.phase("drain with a hold, then delete one resident"):
            drain.drain(hold=HOLD_SECONDS)
            test.delete_sandbox(deleted)
            drain.wait_for_reported()

        with test.phase("before the deadline: the running one answers; a Rollback of it is refused"):
            drain.expect_held(serving.sandbox_id)
            drain.expect_file(serving, PATH)
            drain.expect_rollback_refused(serving, snapshot)
            drain.expect_file(serving, PATH)

        with test.phase("before the deadline: a wake of a paused one is told to retry, then lands elsewhere"):
            cast(ResidentLifecycleSandbox, user_paused).pause()
            answer = drain.request_activation(user_paused)
            drain.expect_retried_never_conflict(answer)
            drain.expect_back_elsewhere(user_paused.sandbox_id, "started")
            drain.expect_file(user_paused, PATH)

        with test.phase("cut the drained worker's uploads before the deadline"):
            ahead = drain.seconds_to_deadline()
            test.verify(
                ahead > BLOCK_MARGIN_SECONDS,
                "the calls before the deadline outlasted the hold",
                seconds_to_deadline=round(ahead),
            )
            uploads = drain.block("object-store")
            drain.expect_started_until_deadline([serving.sandbox_id], margin=5)

        with test.phase("past the deadline: calls to the running one pass until the node suspends it"):
            drain.expect_calls_pass_until_suspended(serving)

        # The node's suspend reaches the row only once its snapshot is
        # durable: with the upload cut the row still reads `started`, and the
        # node reports the sandbox pending, moving.
        with test.phase("the node suspends it; while its upload is cut nothing runs it elsewhere"):
            drain.expect_left_started(serving.sandbox_id)
            # The row still reads `started`, an accepted gap: what a wake and a
            # command get is recorded, not judged.
            woken = drain.request_activation(serving, timeout=60)
            test.record("wake_while_cut", woken)
            call = cast(RawCallSandbox, serving).command_answer("true")
            test.record(
                "command_while_cut",
                {"status": call.status, "retry_after": call.retry_after, "body": call.body},
            )
            drain.expect_no_second_execution(serving.sandbox_id)

        with test.phase("the upload goes through: a wake lands it elsewhere"):
            uploads.release()
            drain.expect_suspended_by_the_node(serving.sandbox_id)
            drain.wake_elsewhere(serving)
            drain.expect_file(serving, PATH)

        with test.phase("the deleted resident is gone and the node drains"):
            drain.expect_deleted(deleted.sandbox_id, timeout=600)
            # Both others were woken elsewhere before the drain ended, so the
            # last report names neither.
            drain.expect_drained()
            test.sandbox.wake()

        with test.phase("undrain"):
            drain.undrain_and_expect_cancelled()
