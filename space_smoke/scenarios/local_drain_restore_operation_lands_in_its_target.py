"""A drain restore operation lands a suspended sandbox in its target lifecycle."""

import shlex

import space_smoke as smoke

PATH = "/tmp/space-smoke-drain-restore.txt"
MARKER = "space-smoke-drain-restore-ok"
# Long enough for the claim to be observed and the pause, resume and connect
# to be issued inside it; each of those answers in seconds on kind.
HOLD_SECONDS = 25
# Far past the phase that enqueues it, so the dispatcher cannot claim it first.
NOT_DUE_SECONDS = 600


# The claim, the gate, the commit and the supersession rules are pinned by the
# Postgres-backed crate tests (`sandbox/store/src/tests/drain_restore_tests.rs`,
# `api/src/drain_restore_operations/tests/`,
# `api/src/public_operations/drain_restore_supersession/tests.rs`). This is the
# seam between them on a real cluster: the cluster-controller's dispatcher has
# to claim the operation, a real restore has to bring the VM back from the
# durable suspend snapshot, and real user calls through the ECW API have to
# supersede it while it waits and meet it while it runs.
#
# Nothing in production enqueues a drain restore yet, so the scenario queues
# each one through a named Kind-only insert that mirrors the API's own, and
# judges it from the operation row, the sandbox row and its executions. The
# held phases set the cluster-controller's test hold for their duration under
# the controller-mutation Lease and put the standing value back before the
# scenario ends. The delete phase comes last because it deletes the fixture's
# sandbox; the "after" check runs on the restore the phase before it landed.
@smoke.scenario(
    "local.drain_restore_operation_lands_in_its_target",
    # Five durable suspends and their portability waits, five restores and
    # two controller rollouts; idle-pausing the sandbox would supersede a
    # restore the scenario is waiting on.
    sandbox=smoke.SandboxConfig(timeout=1800),
    test_hooks=(
        "SPACE_DRAIN_RESTORE_TEST_HOLD_SECS, which holds each claimed drain restore "
        "`restoring` before its node work so user calls can meet the claim"
    ),
)
def drain_restore_operation_lands_in_its_target(test: smoke.KindEcw) -> None:
    """A `drain_restore_sandbox` operation queued for a suspended sandbox
    lands it in its target: toward `started` it succeeds with the sandbox
    started, and toward `paused` with the sandbox paused, each with its
    episode target cleared, one active execution and the file written before
    the suspend intact. While it waits, an ECW pause of the suspended sandbox
    cancels it and leaves the sandbox suspended, and a connect cancels it and
    starts the sandbox. While its claim holds the row, an ECW pause answers
    503 and a resume 409 with the restore still running, and a connect
    returns once the restore lands; a later ECW pause leaves the finished
    operation `succeeded`. A delete issued during the claim runs behind it:
    the operation ends succeeded or canceled, the sandbox is gone and no
    execution is active."""

    marker = f"{MARKER}-{test.sandbox.sandbox_id}"
    drain = test.drain_restore
    restore = test.cross_node_restore  # portability wait only

    def suspend_durably() -> str:
        """ECW pause (a SPACE suspend), then S once a restore would load it from storage."""

        test.pause()
        restore.wait_until_restore_portable()
        return drain.source_snapshot()

    def expect_marker() -> None:
        observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
        test.expect_command(observed, stdout=marker)

    with test.phase("write state, then suspend durably"):
        written = test.run_command_when_ready(f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}")
        test.verify(written.exit_code == 0, "failed to write state", command_stderr=written.stderr)
        snapshot = suspend_durably()

    with test.phase("lands started: a due drain restore toward started"):
        operation = drain.enqueue("lands_started", target="started", snapshot_id=snapshot)
        drain.expect_landed(operation, target="started")
        test.reconnect_by_id()
        expect_marker()

    with test.phase("lands paused: a due drain restore toward paused"):
        snapshot = suspend_durably()
        operation = drain.enqueue("lands_paused", target="paused", snapshot_id=snapshot)
        drain.expect_landed(operation, target="paused")
        test.reconnect_by_id()
        drain.expect_unchanged(operation, state="succeeded", lifecycle="started")
        expect_marker()

    with test.phase("superseded before the claim: an ECW pause of the suspended sandbox"):
        snapshot = suspend_durably()
        operation = drain.enqueue(
            "superseded_by_pause", target="started", snapshot_id=snapshot, due_in_seconds=NOT_DUE_SECONDS
        )
        answer = drain.ecw_request("pause")
        # A pause of an already suspended sandbox finds no pod to suspend on
        # Kubernetes and answers 503 with or without a drain restore waiting;
        # what this phase checks is that the call still cancels the restore.
        test.verify(
            answer.status in (200, 204, 409)
            or (answer.status == 503 and "not assigned to a pod" in answer.detail),
            "an ECW pause of the suspended sandbox failed instead of answering",
            ecw_status=answer.status,
            ecw_detail=answer.detail,
        )
        drain.expect_canceled(operation, lifecycle="suspended")

    with test.phase("superseded before the claim: a connect wakes the suspended sandbox"):
        operation = drain.enqueue(
            "superseded_by_connect",
            target="started",
            snapshot_id=drain.source_snapshot(),
            due_in_seconds=NOT_DUE_SECONDS,
        )
        test.reconnect_by_id()
        drain.expect_canceled(operation, lifecycle="started", active_executions=1)
        expect_marker()

    with drain.scoped_hold():
        with test.phase(f"hold each claimed drain restore {HOLD_SECONDS} s"):
            drain.hold_claimed_restores(HOLD_SECONDS)

        with test.phase("during the claim: pause and resume refused, a connect joins the restore"):
            snapshot = suspend_durably()
            operation = drain.enqueue("joined_by_connect", target="started", snapshot_id=snapshot)
            drain.wait_until_claimed(operation, target="started")
            drain.expect_refused_during_claim(operation, "pause", status=503)
            drain.expect_refused_during_claim(operation, "resume", status=409)
            drain.expect_connect_joins_claim(operation)
            drain.expect_landed(operation, target="started")
            test.reconnect_by_id()
            expect_marker()

        with test.phase("after: an ECW pause suspends normally and the finished restore stays succeeded"):
            test.pause()
            restore.wait_until_restore_portable()
            drain.expect_unchanged(operation, state="succeeded", lifecycle="suspended")

        with test.phase("delete during the claim: the delete runs behind the restore"):
            operation = drain.enqueue(
                "deleted_during_claim", target="started", snapshot_id=drain.source_snapshot()
            )
            drain.wait_until_claimed(operation, target="started")
            test.delete_sandbox()
            drain.expect_deleted_behind_claim(operation)
            test.wait_until_deleted()
