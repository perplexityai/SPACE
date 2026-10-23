"""Real gRPC placement fencing and private-key confinement."""

import hashlib
import ssl
import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig
from space_smoke.scenarios.local_split_trust_lifecycle import _anchor


@smoke.scenario(
    "local.split_trust_placement",
    templates=(),
    sandbox=smoke.SandboxConfig(
        timeout=600,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def anchor_placement(test: smoke.KindEcw) -> None:
    """Only the assigned node and generation may release public sandbox trust."""
    with test.phase("probe the real node credential sockets under service UIDs"):
        pem = _anchor(test, test.sandbox)
        outcome = test.sandbox_anchor_authority.probe_anchor_placement(
            test.sandbox.sandbox_id
        )
        test.verify(
            outcome["assigned_status"] == "OK",
            "assigned node could not release public trust",
        )
        expected = hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()
        test.verify(
            outcome["public_certificate_sha256"] == expected,
            "node and guest disagree on anchor",
        )
        for boundary in (
            "foreign_node_status",
            "forged_generation_status",
            "unprivileged_peer_status",
        ):
            test.verify(
                outcome[boundary] == "PERMISSION_DENIED",
                "anchor boundary was not enforced",
                boundary=boundary,
                status=outcome[boundary],
            )
        for name, value in outcome.items():
            test.record(name, value)
