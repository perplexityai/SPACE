"""The smallest useful public-client smoke scenario."""

import space_smoke as smoke

MARKER = "space-smoke-command-ok"


@smoke.scenario("ecw.command_round_trip", api_surfaces=("ecw", "native"))
def command_round_trip(test: smoke.CommandSandboxFixture) -> None:
    """The selected public API returns the sandbox’s own identity, its data plane
    returns exact command output, and deletion makes the sandbox publicly absent.
    """

    with test.phase("read sandbox information"):
        info = test.sandbox.get_info()
        test.verify(
            info.sandbox_id == test.sandbox.sandbox_id,
            "sandbox information returned a different ID",
            expected_sandbox_id=test.sandbox.sandbox_id,
            observed_sandbox_id=info.sandbox_id,
        )

    with test.phase("run marker command"):
        result = test.run_command(f"printf %s {MARKER}")

    test.expect_command(result, stdout=MARKER)

    with test.phase("delete sandbox and verify public absence"):
        test.delete_sandbox()
        test.wait_until_deleted()
