"""Policy replacement must be enforced before the API reports it applied."""

import space_smoke as smoke


@smoke.scenario(
    "network_policy.live_update",
    sandbox=smoke.network_policy_sandbox(
        internet=False,
        allow=("example.com", "example.org"),
    ),
)
def live_update(test: smoke.Ecw) -> None:
    """Revoke warm and established access, preserve another host, then restore."""
    test.network_policy.verify_live_update()
