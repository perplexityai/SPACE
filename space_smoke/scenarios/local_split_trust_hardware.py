"""Real hardware derivation, regional fallback and stable key rotation."""

import hashlib
import ssl
import time
from collections.abc import Mapping

import space_smoke as smoke
from space_smoke.capabilities import CommandSandbox, KindColdCreateConfig
from space_smoke.scenarios.local_split_trust_lifecycle import _anchor
from space_smoke.scenarios.native_split_trust_tls import ANCHOR


def intercepted_https(test: smoke.KindEcw, sandbox: CommandSandbox, *, configure: bool = True) -> None:
    if configure:
        test.sandbox_anchor_authority.set_interception(sandbox.sandbox_id, enabled=True)
    # A different SNI during the authority outage exercises a new locally
    # signed leaf, rather than merely reusing the first cached certificate.
    host = "example.com" if configure else "www.example.com"
    command = (
        "mkdir -p /tmp/split-trust-empty-ca && "
        "curl --fail --silent --show-error --noproxy '*' --max-time 20 "
        f"--cacert {ANCHOR} --capath /tmp/split-trust-empty-ca "
        f"https://{host}/ -o /dev/null && printf verified"
    )
    deadline = time.monotonic() + 90
    while True:
        result = sandbox.run(command)
        if result.exit_code == 0 and result.stdout == "verified" or time.monotonic() >= deadline:
            break
        test.wait(1, reason="wait for the public network policy to converge")
    test.verify(
        result.exit_code == 0 and result.stdout == "verified",
        "the real intercepted TLS chain did not verify under the sandbox-only anchor",
        stderr=result.stderr,
    )


def public_key_fingerprint(pem: str) -> str:
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    key = x509.load_pem_x509_certificate(pem.encode()).public_key()
    return hashlib.sha256(key.public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)).hexdigest()


def reachable_anchor(
    test: smoke.KindEcw, sandbox_id: str, *, expected_der_hash: str,
) -> Mapping[str, str]:
    """Allow node upstream connections to converge after an intentional restart."""
    deadline = time.monotonic() + 30
    attempts: list[dict[str, str]] = []
    while True:
        proof = test.sandbox_anchor_authority.probe_anchor_access(sandbox_id)
        statuses = {proof["assigned_status"], proof["foreign_node_status"]}
        attempts.append({
            "assigned": proof["assigned_status"],
            "foreign": proof["foreign_node_status"],
        })
        test.record(f"authority_reconnection_{sandbox_id}", attempts.copy())
        for status, fingerprint in (
            (proof["assigned_status"], proof["public_certificate_sha256"]),
            (proof["foreign_node_status"], proof["foreign_certificate_sha256"]),
        ):
            if status == "OK":
                test.verify(
                    fingerprint == expected_der_hash,
                    "reachable authority returned a different sandbox certificate",
                    expected_der_sha256=expected_der_hash,
                    actual_proof=proof,
                )
        # Never retry a certificate mismatch or any authentication failure.
        # Only transport unavailability after the deliberately induced outage
        # gets a bounded retry; callers assert the first reachable result.
        if (
            "UNAVAILABLE" not in statuses
            or not statuses.issubset({"OK", "UNAVAILABLE"})
            or time.monotonic() >= deadline
        ):
            return proof
        test.wait(1, reason="wait for node-to-authority transport reconnection")


@smoke.scenario(
    "local.split_trust_hardware",
    ignored="Requires enabled split trust, owned real multi-region KMS keys and the test dependency group",
    sandbox=smoke.SandboxConfig(
        timeout=1500,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def hardware_derivation(test: smoke.KindEcw) -> None:
    """Use real HMAC keys; cold-load seeds, then sign locally for fresh UUIDs."""
    control = test.sandbox_anchor_authority
    with control.hardware() as hardware:
        with test.phase("compare the real installed key with an independent hardware oracle"):
            original_key_id = hardware.active_key()
            test.verify(
                control.probe_anchor_access(test.sandbox.sandbox_id)["anchor_key_id"] == original_key_id,
                "initial sandbox did not persist the active hardware key",
            )
            original = _anchor(test, test.sandbox)
            original_key = public_key_fingerprint(original)
            original_der_hash = hashlib.sha256(ssl.PEM_cert_to_DER_cert(original)).hexdigest()
            test.verify(
                original_key == hardware.expected_public_key(
                    test.sandbox.sandbox_id, key_id=original_key_id,
                ),
                "installed anchor does not match the hardware-derived key",
            )
            test.record("original_spki_sha256", original_key)
            test.record("original_der_sha256", original_der_hash)

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

        with test.phase("cold-load every authority replica while primary KMS keys are disabled"):
            with hardware.primary_unavailable():
                hardware.restart_authority()
                failover = test.sandboxes.create()
                failover_anchor = _anchor(test, failover)
                proof = reachable_anchor(
                    test, failover.sandbox_id,
                    expected_der_hash=hashlib.sha256(
                        ssl.PEM_cert_to_DER_cert(failover_anchor),
                    ).hexdigest(),
                )
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
                public_key_fingerprint(failover_anchor) == hardware.expected_public_key(
                    failover.sandbox_id, key_id=original_key_id,
                ),
                "regional failover changed the deterministic sandbox key",
            )

        with test.phase("warm authorities enroll a new sandbox while every KMS key is disabled"):
            with hardware.unavailable():
                warm = test.sandboxes.create()
                warm_anchor = _anchor(test, warm)
                intercepted_https(test, warm)
                proof = control.probe_anchor_access(warm.sandbox_id)
                test.verify(
                    proof["assigned_status"] == proof["foreign_node_status"] == "OK",
                    "warm authorities needed a hardware call for a fresh UUID",
                )
                test.record("fresh_uuid_without_available_kms", {
                    "sandbox_id": warm.sandbox_id,
                    "spki_sha256": public_key_fingerprint(warm_anchor),
                    "intercepted_tls_verified": True,
                })
                test.verify(
                    hardware.cold_authority_fails_closed(),
                    "cold authority startup did not demonstrably fail closed",
                )
                test.record("cold_authority_without_kms_failed_closed", True)
                # The already-issued gateway CA signs leaves locally; a cold
                # authority outage must not kill this established data plane.
                intercepted_https(test, warm, configure=False)
                test.record("existing_gateway_signed_a_new_leaf_during_authority_outage", True)
            test.verify(
                public_key_fingerprint(warm_anchor) == hardware.expected_public_key(
                    warm.sandbox_id, key_id=original_key_id,
                ),
                "the warm authority used a substitute rather than the hardware-derived seed",
            )
            test.verify(
                reachable_anchor(
                    test, warm.sandbox_id,
                    expected_der_hash=hashlib.sha256(
                        ssl.PEM_cert_to_DER_cert(warm_anchor),
                    ).hexdigest(),
                )["public_certificate_sha256"]
                == hashlib.sha256(ssl.PEM_cert_to_DER_cert(warm_anchor)).hexdigest(),
                "restoring hardware and restarting authorities changed the anchor",
            )

        with test.phase("roll out a replacement active hardware key"):
            replacement_key_id = hardware.rotate()
            test.verify(replacement_key_id != original_key_id, "rotation did not change the active key")
            test.record("active_key_rotation", {
                "original_key": original_key_id, "replacement_key": replacement_key_id,
            })

        with test.phase("restore an old sandbox after authority key rotation"):
            hardware.restart_authority()
            proof = reachable_anchor(
                test, test.sandbox.sandbox_id, expected_der_hash=original_der_hash,
            )
            test.record("old_identity_after_rotation", proof)
            test.verify(
                proof["assigned_status"] == "OK"
                and proof["foreign_node_status"] == "OK"
                and proof["anchor_key_id"] == original_key_id
                and proof["public_certificate_sha256"] == proof["foreign_certificate_sha256"]
                and proof["public_certificate_sha256"] == original_der_hash,
                "old identity could not be rederived after rotation",
                expected_der_sha256=original_der_hash,
                actual_proof=proof,
            )
            test.verify(
                hardware.expected_public_key(
                    test.sandbox.sandbox_id, key_id=original_key_id,
                ) == original_key,
                "rotation changed an existing sandbox's hardware-derived private/public key",
            )
            test.pause()
            test.reconnect_by_id()
            test.verify(_anchor(test, test.sandbox) == original, "rotation or restore changed installed trust")
            intercepted_https(test, test.sandbox)

        with test.phase("a newly born sandbox uses the new hardware key"):
            new = test.sandboxes.create()
            test.verify(
                control.probe_anchor_access(new.sandbox_id)["anchor_key_id"] == replacement_key_id,
                "new sandbox did not persist the replacement key",
            )
            key = public_key_fingerprint(_anchor(test, new))
            test.verify(
                key == hardware.expected_public_key(new.sandbox_id, key_id=replacement_key_id),
                "new sandbox did not use its pinned hardware key",
            )
            test.verify(key != original_key, "different sandboxes share a private key")
            test.record("new_identity_after_rotation", {
                "sandbox_id": new.sandbox_id, "key_id": replacement_key_id, "spki_sha256": key,
            })

        with test.phase("a fork of an old sandbox adopts the current key without changing its parent"):
            child = test.sandboxes.fork(test.sandbox).sandbox
            child_key = public_key_fingerprint(_anchor(test, child))
            proof = control.probe_anchor_access(child.sandbox_id)
            expected_child_key = hardware.expected_public_key(
                child.sandbox_id, key_id=replacement_key_id,
            )
            test.record("fork_key_selection", {
                "sandbox_id": child.sandbox_id, "expected_key_id": replacement_key_id,
                "actual_proof": proof, "spki_sha256": child_key,
                "expected_spki_sha256": expected_child_key,
            })
            test.verify(
                proof["anchor_key_id"] == replacement_key_id
                and child_key == expected_child_key,
                "fork did not select the active hardware key for its own identity",
            )
            test.verify(_anchor(test, test.sandbox) == original, "fork changed parent trust")
            intercepted_https(test, child)
            test.record("fork_after_rotation", {
                "sandbox_id": child.sandbox_id, "key_id": replacement_key_id,
                "spki_sha256": child_key,
            })
