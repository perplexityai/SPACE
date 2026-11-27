"""Repeated ECW pause/connect boundary coverage."""

import os

import space_smoke as smoke
from space_smoke.runtime import SmokeConfigurationError

# The first cycle covers initial restore; the next two exercise repeated use
# of the same sandbox. Longer runs sample more races without adding a new
# lifecycle transition, so keep that stress coverage available via the override.
ITERATIONS = 3
ITERATIONS_ENV = "SPACE_SMOKE_PAUSE_CONNECT_ITERATIONS"
SANDBOX_TIMEOUT_SECONDS = 120
MARKER_PATH = "/tmp/space-smoke-pause-connect-race.txt"


@smoke.scenario(
    "ecw.pause_connect_race",
    sandbox=smoke.SandboxConfig(timeout=SANDBOX_TIMEOUT_SECONDS),
    rerun_environment=(ITERATIONS_ENV,),
)
def pause_connect_race(test: smoke.Ecw) -> None:
    """Consecutive pause and immediate reconnect cycles each restore the
    same sandbox's data plane and preserve that cycle's exact marker.
    """

    try:
        iterations = int(os.environ.get(ITERATIONS_ENV, str(ITERATIONS)))
    except ValueError as error:
        raise SmokeConfigurationError(f"{ITERATIONS_ENV} must be an integer") from error
    if not 2 <= iterations <= 100:
        raise SmokeConfigurationError(f"{ITERATIONS_ENV} must be between 2 and 100")
    test.record("pause_connect_iterations", iterations)
    for iteration in range(1, iterations + 1):
        marker = f"space-smoke-pause-connect:{test.sandbox.sandbox_id}:{iteration}"

        with test.phase(f"cycle {iteration}/{iterations}: write marker"):
            written = test.run_command_when_ready(
                f"printf %s {marker} > {MARKER_PATH}",
                timeout=10,
            )
            test.expect_command(written, stdout="")

        with test.phase(
            f"cycle {iteration}/{iterations}: pause, reconnect, and read marker"
        ):
            test.pause()
            test.reconnect_by_id()
            observed = test.run_command_when_ready(
                f"cat {MARKER_PATH}",
                timeout=10,
            )
            test.expect_command(observed, stdout=marker)
