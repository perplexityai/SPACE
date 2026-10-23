"""Encrypted backup recovery without exporting any key material."""

import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig
from space_smoke.scenarios.local_split_trust_lifecycle import _anchor


@smoke.scenario(
    "local.split_trust_authority_recovery",
    templates=(),
    sandbox=smoke.SandboxConfig(
        timeout=1200,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def authority_recovery(test: smoke.KindEcw) -> None:
    """Delete the live keyring, prove missing-state startup fails closed,
    recover from KMS/S3, and prove the same sandbox still gets identical trust."""
    with test.phase("capture the sandbox public anchor"):
        before = _anchor(test, test.sandbox)

    with test.phase("lose live seed state and recover both real authority replicas"):
        outcome = test.sandbox_anchor_authority.recover_anchor_keyring()
        test.verify(
            outcome["missing_secret_failed_closed"] == "true",
            "missing keyring was silently replaced instead of failing closed",
        )
        test.verify(
            outcome["restored_authority_replicas"] == "2", "replicas were not restored"
        )
        for name, value in outcome.items():
            test.record(name, value)

    with test.phase("restore a new execution using the recovered authority"):
        test.pause()
        test.reconnect_by_id()
        test.verify(
            _anchor(test, test.sandbox) == before,
            "encrypted recovery changed sandbox trust",
        )
        test.expect_command(test.run_command("printf recovered"), stdout="recovered")
