"""Default-off leaves guests unanchored and ordinary HTTPS working."""

from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig
from space_smoke.scenarios.native_split_trust_tls import ANCHOR, PolicySandbox


@smoke.scenario(
    "native.split_trust_disabled",
    ignored="Requires the isolated prototype bench with runtime activation disabled",
    api_surfaces=("native",),
    sandbox=smoke.SandboxConfig(
        timeout=600,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def split_trust_disabled(test: smoke.CommandSandboxFixture) -> None:
    """Kind has no legacy AWS Private CA configured. This proves direct HTTPS
    compatibility, not shared-CA interception or its production rollout."""
    with test.phase("a fresh guest has no sandbox-specific anchor"):
        absent = test.run_command(f"test ! -e {ANCHOR} && printf absent")
        test.expect_command(absent, stdout="absent")
    with test.phase("verify ordinary HTTPS without interception"):
        cast(PolicySandbox, test.sandbox).set_network_policy(
            {
                "version": "v1",
                "egress": {"default_action": "allow"},
                "tls_interception": "off",
            }
        )
        probe = (
            "curl --fail --silent --show-error --noproxy '*' --max-time 20 "
            "https://example.com/ -o /dev/null && printf verified"
        )
        test.wait_until_command_matches(probe, stdout="verified", timeout=90)
        test.record("direct_https_verified", True)
        test.record("shared_ca_interception_validated", False)
        test.record("anchor_installed", False)
