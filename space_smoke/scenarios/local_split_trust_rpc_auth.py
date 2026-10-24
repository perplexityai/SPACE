"""Real existing-RPC authentication and public-only anchor delivery."""

import hashlib
import ssl
import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig
from space_smoke.scenarios.local_split_trust_lifecycle import _anchor


@smoke.scenario(
    "local.split_trust_rpc_auth",
    ignored="Requires explicitly enabled split trust and the owned real-KMS Kind fixture",
    sandbox=smoke.SandboxConfig(
        timeout=600,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def anchor_rpc_auth(test: smoke.KindEcw) -> None:
    """Authenticated nodes are trusted globally; local callers get no keys."""
    with test.phase("probe the real node credential sockets under service UIDs"):
        pem = _anchor(test, test.sandbox)
        outcome = test.sandbox_anchor_authority.probe_anchor_access(
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
        for boundary in ("foreign_node_status",):
            test.verify(
                outcome[boundary] == "OK",
                "existing authenticated-node boundary unexpectedly refused release",
                boundary=boundary,
                status=outcome[boundary],
            )
        for fingerprint in ("foreign_certificate_sha256",):
            test.verify(
                outcome[fingerprint] == expected,
                "deterministic public anchor changed across authenticated callers",
                fingerprint=fingerprint,
            )
        test.verify(
            outcome["local_signing_profile_status"] == "PERMISSION_DENIED",
            "local caller obtained authority-only signing credentials",
        )
        test.verify(
            outcome["unprivileged_peer_status"] == "PERMISSION_DENIED",
            "unprivileged local caller bypassed the existing node allow-list",
        )
        test.verify(
            outcome["unauthenticated_authority_status"] == "UNAUTHENTICATED",
            "authority probe did not return the expected authentication rejection",
            status=outcome["unauthenticated_authority_status"],
        )
        for name, value in outcome.items():
            test.record(name, value)
