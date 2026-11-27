"""ECW refresh keepalive invariant."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import EcwSandbox

TIMEOUT_SECONDS = 30
REFRESH_AFTER_SECONDS = 20
VERIFY_AFTER_REFRESH_SECONDS = 17


@smoke.scenario(
    "ecw.refresh_keepalive",
    sandbox=smoke.SandboxConfig(timeout=TIMEOUT_SECONDS),
    sandboxes=2,
)
def refresh_keepalive(test: smoke.Ecw) -> None:
    """Refresh and same-value set_timeout independently reset the idle deadline
    and keep their sandboxes usable beyond their original deadlines.
    """

    refreshed, rearmed = (cast(EcwSandbox, sandbox) for sandbox in test.sandboxes.items)
    test.verify(refreshed.sandbox_id != rearmed.sandbox_id, "deadline resets require independent sandboxes")
    # Fixture creation is sequential. Establish both initial deadlines after
    # setup so a slow second create cannot consume the first reset's margin.
    with test.phase("align initial sandbox idle deadlines"):
        refreshed.set_timeout(TIMEOUT_SECONDS)
        rearmed.set_timeout(TIMEOUT_SECONDS)
    test.prove_idle_deadline_reset(
        reset_after=REFRESH_AFTER_SECONDS,
        verify_after_reset=VERIFY_AFTER_REFRESH_SECONDS,
        reset=refreshed.refresh,
        additional_resets=((rearmed, lambda: rearmed.set_timeout(TIMEOUT_SECONDS)),),
    )
