"""An ECW create naming a pool's exact shape claims a started pool member."""

import json

import space_smoke as smoke

VCPU = 2
MEMORY_MIB = 1024


@smoke.scenario(
    "ecw.warm_pool_shaped_claim",
    sandbox=smoke.SandboxConfig(
        metadata={
            "space.resources": json.dumps({"vcpu": VCPU, "memory_mib": MEMORY_MIB})
        },
        kind_warm_pool=smoke.KindWarmPoolConfig(vcpu=VCPU, memory_mib=MEMORY_MIB),
    ),
)
def warm_pool_shaped_claim(test: smoke.KindEcw) -> None:
    """A create whose `space.resources` matches the ready slot's shape adopts
    that slot and runs with the requested vCPUs.
    """

    test.expect_warm_pool_claim()

    nproc = test.run_command("nproc")
    test.verify(
        nproc.exit_code == 0 and nproc.stdout.strip() == str(VCPU),
        "shaped warm-pool claim did not run with the requested vCPUs",
        sandbox_id=test.sandbox.sandbox_id,
        command_exit_code=nproc.exit_code,
        nproc=nproc.stdout.strip(),
    )
