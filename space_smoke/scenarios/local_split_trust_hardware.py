"""Real hardware derivation, regional fallback and stable key rotation."""

import hashlib
import json
import ssl
import time
import uuid

import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig
from space_smoke.scenarios.local_split_trust_lifecycle import _anchor


def public_key_fingerprint(pem: str) -> str:
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    key = x509.load_pem_x509_certificate(pem.encode()).public_key()
    return hashlib.sha256(key.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)).hexdigest()


@smoke.scenario(
    "local.split_trust_hardware",
    ignored="Requires enabled split trust, owned real multi-region KMS keys and the test dependency group",
    sandbox=smoke.SandboxConfig(
        timeout=1500,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def hardware_derivation(test: smoke.KindEcw) -> None:
    """Use real HMAC keys, not Moto; prove failover on a fresh uncached identity."""
    control = test.sandbox_anchor_authority
    with control.hardware() as hardware:
        with test.phase("compare the real installed key with an independent hardware oracle"):
            initial_catalog = json.loads(hardware.catalog()["data"]["epochs.json"])
            test.verify(
                len(initial_catalog["epochs"]) == 1,
                "rotation proof needs a fresh epoch-1 fixture, not an already-rotated catalog",
            )
            original = _anchor(test, test.sandbox)
            original_key = public_key_fingerprint(original)
            original_der_hash = hashlib.sha256(ssl.PEM_cert_to_DER_cert(original)).hexdigest()
            test.verify(
                original_key == hardware.expected_public_key(test.sandbox.sandbox_id),
                "installed anchor does not match the hardware-derived key",
            )
            test.record("original_spki_sha256", original_key)

        with test.phase("measure real HMAC calls and compare related regional keys"):
            benchmark = hardware.benchmark()
            test.verify(
                benchmark["replica_outputs_identical"] is True,
                "related regional hardware keys derived different values",
            )
            test.verify(benchmark["sdk_retries"] == 0, "latency samples included hidden retries")
            test.verify(
                benchmark["runtime_role_is_scoped"] is True
                and all(
                    key["origin"] == "AWS_KMS"
                    and key["key_spec"] == "HMAC_384"
                    and key["key_usage"] == "GENERATE_VERIFY_MAC"
                    and key["multi_region"] is True
                    and key["state"] == "Enabled"
                    for key in benchmark["hardware_keys"]
                ),
                "real key metadata or measurement identity violates the hardware contract",
            )
            test.record("real_kms_latency", benchmark)

        with test.phase("create an uncached sandbox while both primary epoch keys are disabled"):
            with hardware.primary_unavailable():
                failover = test.sandboxes.create()
                failover_anchor = _anchor(test, failover)
                proof = control.probe_anchor_access(failover.sandbox_id)
                test.verify(
                    proof["assigned_status"] == "OK"
                    and proof["foreign_node_status"] == "OK"
                    and proof["public_certificate_sha256"] == proof["foreign_certificate_sha256"],
                    "real credential services could not derive through the regional replica",
                )
                test.record("primary_key_unavailability", {
                    "fresh_sandbox_id": failover.sandbox_id,
                    "spki_sha256": public_key_fingerprint(failover_anchor),
                    "cross_node_public_certificate_sha256": proof["public_certificate_sha256"],
                })
            test.verify(
                public_key_fingerprint(failover_anchor) == hardware.expected_public_key(failover.sandbox_id),
                "regional failover changed the deterministic sandbox key",
            )

        with test.phase("stage a new hardware key without rewriting an existing epoch"):
            epoch = hardware.rotate()
            activation = int(epoch["since_ms"])
            while time.time_ns() // 1_000_000 < activation + 1000:
                hardware.catalog()  # Renew the mutation Lease during the bounded activation wait.
                remaining = (activation + 1000 - time.time_ns() // 1_000_000) / 1000
                test.wait(min(5, max(0, remaining)), reason="wait for the staged hardware epoch")
            test.record("activated_epoch", {"version": epoch["version"], "since_ms": activation})

        with test.phase("rederive the old anchor after cache expiry under the new catalog"):
            proof = control.probe_anchor_access(test.sandbox.sandbox_id)
            test.verify(
                proof["assigned_status"] == "OK"
                and proof["foreign_node_status"] == "OK"
                and proof["public_certificate_sha256"] == proof["foreign_certificate_sha256"]
                and proof["public_certificate_sha256"] == original_der_hash,
                "old identity could not be rederived after rotation",
            )
            test.verify(
                hardware.expected_public_key(test.sandbox.sandbox_id) == original_key,
                "rotation changed an existing sandbox's hardware-derived private/public key",
            )
            test.verify(_anchor(test, test.sandbox) == original, "rotation changed installed trust")
            test.record("old_identity_after_rotation", proof)

        with test.phase("a newly born sandbox uses the new hardware key"):
            new = test.sandboxes.create()
            new_id = uuid.UUID(new.sandbox_id.removeprefix("sbx_"))
            test.verify(
                int.from_bytes(new_id.bytes[:6], "big") >= activation,
                "rotation proof accidentally claimed a pre-activation sandbox identity",
            )
            key = public_key_fingerprint(_anchor(test, new))
            test.verify(
                key == hardware.expected_public_key(new.sandbox_id),
                "new sandbox did not use the selected hardware epoch",
            )
            test.verify(key != original_key, "different sandboxes share a private key")
            test.record("new_identity_after_rotation", {
                "sandbox_id": new.sandbox_id, "epoch": epoch["version"], "spki_sha256": key,
            })
