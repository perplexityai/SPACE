"""V1 ECW network-policy enforcement across suspend and restore."""

import space_smoke as smoke

ALLOWED = "https://example.com/"
DENIED = "http://1.0.0.1/"
# Domain allows implicitly admit the reserved resolvers, so use another IP.
UNLISTED = "http://1.1.1.2/"


@smoke.scenario(
    "network_policy.v1_rules_survive_restore",
    sandbox=smoke.network_policy_sandbox(
        internet=False,
        allow=("example.com",),
        deny=("1.0.0.1/32",),
    ),
)
def v1_rules_survive_restore(test: smoke.Ecw) -> None:
    """A V1 allow/deny policy enforces default deny and retains the same
    behavior after the sandbox is suspended and restored.
    """

    with test.phase("verify V1 policy after creation"):
        test.network_policy.verify_egress(
            reachable=(ALLOWED,),
            blocked=(DENIED, UNLISTED),
        )

    with test.phase("suspend and restore sandbox"):
        test.pause()
        test.reconnect_by_id()

    with test.phase("verify V1 policy after restore"):
        test.network_policy.verify_egress(reachable=(ALLOWED,), blocked=(DENIED, UNLISTED))
