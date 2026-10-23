"""Trust installation on a claimed prewarmed VM."""

import space_smoke as smoke
from space_smoke.scenarios.local_split_trust_lifecycle import _anchor


@smoke.scenario(
    "local.split_trust_warm_pool",
    templates=(),
    sandbox=smoke.SandboxConfig(
        timeout=600,
        kind_warm_pool=smoke.KindWarmPoolConfig(vcpu=2, memory_mib=1024),
    ),
)
def split_trust_warm_pool(test: smoke.KindEcw) -> None:
    """A claimed prewarmed VM contains only the claimed sandbox's anchor."""
    test.expect_warm_pool_claim()
    _anchor(test, test.sandbox)
