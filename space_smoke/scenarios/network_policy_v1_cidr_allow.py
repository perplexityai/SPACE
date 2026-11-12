"""V1 CIDR allow enforcement."""

import space_smoke as smoke

INSIDE = "https://1.0.0.1/"
# Outside the allowed CIDR and the gateway's reserved DNS destinations.
OUTSIDE = "http://1.1.1.2/"


@smoke.scenario(
    "network_policy.v1_cidr_allow",
    sandbox=smoke.network_policy_sandbox(
        internet=False,
        allow=("1.0.0.0/24",),
    ),
)
def v1_cidr_allow(test: smoke.Ecw) -> None:
    """A matching V1 CIDR allow overrides default deny."""

    test.network_policy.verify_egress(
        reachable=(INSIDE,),
        blocked=(OUTSIDE,),
    )
