"""ECW compatibility for the top-level internet-access deny switch."""

import space_smoke as smoke

PROBE_URL = "https://example.com/"


@smoke.scenario(
    "ecw.internet_access_denied",
    sandbox=smoke.SandboxConfig(timeout=120, allow_internet_access=False),
)
def internet_access_denied(test: smoke.KindNetworkEcw) -> None:
    """A sandbox created with allow_internet_access=False consistently blocks
    a public HTTPS endpoint.
    """

    # Raw default deny admits no resolver or external route. Read the live
    # gateway state rather than adding an allow that changes this contract.
    policy_ref = test.expect_network_policy_installed()
    test.network_policy.verify_egress(blocked=(PROBE_URL,))
    test.verify(
        test.expect_network_policy_installed() == policy_ref,
        "the installed network policy changed during the denial probe",
    )
