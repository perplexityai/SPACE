"""V1 exact-IP allow enforcement."""

import space_smoke as smoke

ALLOWED = "https://1.0.0.1/"
# 1.1.1.1 and 8.8.8.8 are gateway-owned DNS destinations, not HTTP controls.
UNLISTED = "http://1.1.1.2/"


@smoke.scenario(
    "network_policy.v1_exact_ip_allow",
    sandbox=smoke.network_policy_sandbox(
        internet=False,
        allow=("1.0.0.1",),
    ),
)
def v1_exact_ip_allow(test: smoke.Ecw) -> None:
    """A V1 exact-IP allow overrides default deny without using CIDR syntax."""

    test.network_policy.verify_egress(
        reachable=(ALLOWED,),
        blocked=(UNLISTED,),
    )
