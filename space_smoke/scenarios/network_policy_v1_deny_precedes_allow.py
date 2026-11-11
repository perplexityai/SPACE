"""V1 overlapping-rule precedence."""

import space_smoke as smoke

OVERLAPPED = "http://1.0.0.1/"
ALLOWED = "https://example.com/"


@smoke.scenario(
    "network_policy.v1_deny_precedes_allow",
    sandbox=smoke.network_policy_sandbox(
        internet=False,
        allow=("1.0.0.1/32", "example.com"),
        deny=("1.0.0.0/24",),
    ),
)
def v1_deny_precedes_allow(test: smoke.Ecw) -> None:
    """V1 blocks an explicitly allowed IP when a broader denied CIDR contains
    it.
    """

    test.network_policy.verify_egress(reachable=(ALLOWED,), blocked=(OVERLAPPED,))
