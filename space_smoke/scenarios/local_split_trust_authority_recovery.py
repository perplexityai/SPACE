"""Authority replacement reproduces trust from KMS and the sandbox's saved pin."""

import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig
from space_smoke.scenarios.local_split_trust_lifecycle import _anchor


@smoke.scenario(
    "local.split_trust_authority_recovery",
    ignored="Requires explicitly enabled split trust and the owned real-KMS Kind fixture",
    sandbox=smoke.SandboxConfig(
        timeout=1200,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def authority_recovery(test: smoke.KindEcw) -> None:
    """Restart both authorities with empty RAM and regenerate the same anchor."""
    with test.phase("capture the sandbox public anchor"):
        before = _anchor(test, test.sandbox)
        initial = test.sandbox_anchor_authority.probe_anchor_access(test.sandbox.sandbox_id)

    with test.phase("replace both real authority replicas and reload hardware seeds"):
        outcome = test.sandbox_anchor_authority.recover_anchor_keyring()
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
            "authority replacement changed sandbox trust",
        )
        test.expect_command(test.run_command("printf recovered"), stdout="recovered")
        proof = test.sandbox_anchor_authority.probe_anchor_access(test.sandbox.sandbox_id)
        test.verify(
            proof["assigned_status"] == "OK"
            and proof["foreign_node_status"] == "OK"
            and proof["public_certificate_sha256"] == proof["foreign_certificate_sha256"]
            and proof["public_certificate_sha256"] == initial["public_certificate_sha256"],
            "restarted authority did not regenerate the same anchor on both nodes",
        )
        test.record("regenerated_anchor_rpc", proof)
