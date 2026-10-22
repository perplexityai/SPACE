"""ECW timeout-update happy path."""

import time

import space_smoke as smoke
from space_smoke.capabilities import EcwSandboxInfo, EcwSandboxState

MARKER = "space-smoke-timeout-update-ok"
INITIAL_TIMEOUT_SECONDS = 10
# The shortest extension that outlasts the first behaviour check: the original
# deadline waited out by a tick and the margin, with headroom for the check.
EXTENDED_TIMEOUT_SECONDS = 20
SUSPENDED_TIMEOUT_SECONDS = 60
# Connect's extension must be shorter than both policies: otherwise it could
# conceal failure to persist the suspended timeout update.
RECONNECT_TIMEOUT_SECONDS = 1
# sbx-manager's auto-lifecycle scheduler acts on a deadline at its next tick,
# every 5 s (`AUTO_LIFECYCLE_RECONCILE_INTERVAL`, sbx/mgr/server/src/service/
# reconcile.rs), and the suspend it starts reads as running until it lands.
# A behaviour check waits a deadline out by a whole tick plus that margin, so
# a deadline the update failed to move has certainly been acted on.
SCHEDULER_TICK_SECONDS = 5
DEADLINE_MARGIN_SECONDS = 3
# `end_at` is in whole seconds.
END_AT_TOLERANCE_SECONDS = 1


@smoke.scenario(
    "ecw.timeout_update",
    sandbox=smoke.SandboxConfig(timeout=INITIAL_TIMEOUT_SECONDS),
)
def timeout_update(test: smoke.Ecw) -> None:
    """An ECW sandbox remains usable after its original deadline when its timeout
    is extended through the API, and a timeout set while it is suspended is the
    one it runs under once restored.
    """

    # The sandbox's deadline was armed before the fixture handed it over.
    started = time.monotonic()

    with test.phase("extend sandbox timeout"):
        original_end_at = _end_at(test, "before the update")
        test.sandbox.set_timeout(EXTENDED_TIMEOUT_SECONDS)
        _expect_end_at_moved(
            test,
            since=original_end_at,
            by=EXTENDED_TIMEOUT_SECONDS - INITIAL_TIMEOUT_SECONDS,
            when="after extending the running sandbox",
        )

    with test.phase("wait beyond original sandbox deadline"):
        waited = _wait_out(test, started, INITIAL_TIMEOUT_SECONDS)

    with test.phase("verify sandbox remains usable past original deadline"):
        observed = test.sandbox.get_info()
        test.verify(
            observed.state is EcwSandboxState.RUNNING,
            "sandbox was paused at its original deadline despite the extension",
            observed_state=observed.state,
            waited_seconds=waited,
        )
        result = test.sandbox.run(f"printf %s {MARKER}")
        test.verify(
            result.exit_code == 0 and result.stdout == MARKER,
            "sandbox was not usable after its original timeout elapsed",
            initial_timeout_seconds=INITIAL_TIMEOUT_SECONDS,
            extended_timeout_seconds=EXTENDED_TIMEOUT_SECONDS,
            waited_seconds=waited,
            command_exit_code=result.exit_code,
            command_stdout=result.stdout,
            command_stderr=result.stderr,
        )

    with test.phase("update timeout while durably suspended"):
        pre_suspend_end_at = _end_at(test, "before suspending")
        test.suspend_durably()
        test.wait_until_paused()
        test.sandbox.set_timeout(SUSPENDED_TIMEOUT_SECONDS)
        observed = test.sandbox.get_info()
        test.verify(
            observed.state is EcwSandboxState.PAUSED,
            "setting timeout woke the suspended sandbox",
            observed_state=observed.state,
        )

    with test.phase("restore with the stored timeout policy"):
        test.reconnect_by_id(timeout=RECONNECT_TIMEOUT_SECONDS)
        restored = time.monotonic()
        _expect_end_at_moved(
            test,
            since=pre_suspend_end_at,
            by=SUSPENDED_TIMEOUT_SECONDS - EXTENDED_TIMEOUT_SECONDS,
            when="after restoring",
        )
        # Every earlier timeout in this scenario, re-armed at the restore, and
        # connect's own extension end inside this wait; only the update made
        # while suspended outlasts it.
        earlier = max(INITIAL_TIMEOUT_SECONDS, EXTENDED_TIMEOUT_SECONDS, RECONNECT_TIMEOUT_SECONDS)
        waited = _wait_out(test, restored, earlier)
        observed = test.sandbox.try_get_info()
        test.verify(
            observed is not None and observed.state is EcwSandboxState.RUNNING,
            "restored sandbox did not retain its suspended timeout update",
            previous_timeout_seconds=EXTENDED_TIMEOUT_SECONDS,
            requested_timeout_seconds=SUSPENDED_TIMEOUT_SECONDS,
            waited_seconds=waited,
        )
        # Check public state before any guest command that could auto-resume it.
        test.expect_command(test.sandbox.run(f"printf %s {MARKER}"), stdout=MARKER)


def _end_at(test: smoke.Ecw, when: str) -> float:
    info: EcwSandboxInfo = test.sandbox.get_info()
    test.verify(
        info.end_at is not None,
        f"the sandbox information carried no end_at {when}",
        observed_state=info.state,
    )
    assert info.end_at is not None
    return info.end_at.timestamp()


def _expect_end_at_moved(test: smoke.Ecw, *, since: float, by: int, when: str) -> None:
    """The public `end_at` moved out by at least the policy change.

    `end_at` reports the timeout policy the API persisted, as an expiry
    measured from the sandbox's start, so it moves by exactly the change. It
    says nothing about when the node will act; the behaviour checks do.
    """

    moved = round(_end_at(test, when) - since, 1)
    test.record(f"end_at_moved_seconds {when}", moved)
    test.verify(
        moved >= by - END_AT_TOLERANCE_SECONDS,
        f"end_at {when} does not reflect the updated timeout",
        end_at_moved_seconds=moved,
        expected_at_least_seconds=by,
    )


def _wait_out(test: smoke.Ecw, armed: float, timeout_seconds: int) -> float:
    """Wait until a deadline armed by `armed` has had a whole tick to act."""

    seconds = (
        armed + timeout_seconds + SCHEDULER_TICK_SECONDS + DEADLINE_MARGIN_SECONDS
        - time.monotonic()
    )
    if seconds > 0:
        test.wait(seconds)
    return round(max(seconds, 0.0), 1)
