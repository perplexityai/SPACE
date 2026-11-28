"""The restore-pin route and the restore-pin repair both pin an unpinned suspended sandbox to its capture."""

import secrets
import shlex

import space_smoke as smoke
from space_smoke.restore_pin_repair import shifted_token

TIMEOUT_MINUTES = 30
MARKER_PATH = "/tmp/space-smoke-restore-pin.txt"


# A suspend on this build pins its capture; only an older build leaves a
# suspended row unpinned, so the body clears the pin through the one named
# write (`space_smoke.restore_pin_repair`) before each pin under test. The
# repair loop ships off: the body checks it's off before the route's pin and
# stages it on, at a 5 s tick, only for the repair phase, putting the standing
# controller env back before the body ends.
@smoke.scenario(
    "local.restore_pin_route_and_repair_pin_an_unpinned_suspended_sandbox",
    timeout_minutes=TIMEOUT_MINUTES,
    # The default pause-on-timeout must never fire while the body holds the
    # sandbox suspended across two controller rollouts.
    sandbox=smoke.SandboxConfig(timeout=TIMEOUT_MINUTES * 60),
)
def restore_pin_route_and_repair(test: smoke.KindEcw) -> None:
    """After a suspend pins capture P, the admin restore-pin route refuses a
    pin with a stale lifecycle token and a pin of the already-pinned row
    (409). With the pin cleared and the repair loop off, the route pins P.
    With the pin cleared again and the repair loop on, the loop pins P. The
    sandbox then resumes with the data written before the suspend."""

    pins = test.restore_pin
    marker = f"space-smoke-restore-pin-{secrets.token_hex(8)}"

    with test.phase("write a marker and suspend"):
        written = test.run_command_when_ready(
            f"printf %s {shlex.quote(marker)} > {shlex.quote(MARKER_PATH)} && sync"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write the marker file",
            command_stderr=written.stderr[:500],
        )
        test.pause()
        pinned = pins.wait_for_suspend_pin()
        token = pins.read()["lifecycle_changed_at"]
        test.verify(
            isinstance(token, str) and bool(token),
            "the suspended sandbox's restore-pin read carries no lifecycle_changed_at",
        )

    with test.phase("the route refuses a stale token and an already-pinned row"):
        pins.expect_pin_refused(pinned, shifted_token(token, seconds=1), why="stale_token")
        pins.expect_pin_refused(pinned, token, why="already_pinned")
        pins.expect_pinned(pinned)

    with pins.scoped_repair():
        with test.phase("the route pins the unpinned suspended sandbox"):
            pins.expect_repair_off()
            unpinned = pins.clear_pin(pinned)
            test.verify(
                unpinned.get("lifecycle_changed_at") == token,
                "clearing the pin moved the sandbox's lifecycle token",
                before=token,
                after=unpinned.get("lifecycle_changed_at"),
            )
            pins.pin(pinned, token)
            pins.expect_pinned(pinned)

        with test.phase("clear the pin and go live with the restore-pin repair"):
            pins.clear_pin(pinned)
            config = pins.stage_repair()

        with test.phase("the repair pins the unpinned suspended sandbox"):
            pins.wait_for_repair(pinned, config)
            pins.expect_pinned(pinned)

        # A failed body still restores as it unwinds; this phase accounts for
        # the controller rollout on the passing path.
        with test.phase("restore the standing restore-pin repair"):
            pins.restore_standing()

    with test.phase("resume and read the marker back"):
        test.reconnect_by_id()
        result = test.run_command(
            f"cat {shlex.quote(MARKER_PATH)}",
            failure_message="the resumed sandbox did not run a command",
        )
        test.verify(
            result.exit_code == 0 and result.stdout == marker,
            "the resumed sandbox's marker file does not hold what was written before the suspend",
            command_exit_code=result.exit_code,
            command_stdout=result.stdout[:200],
            expected=marker,
        )
