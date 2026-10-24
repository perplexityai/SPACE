"""Public lifecycle proof for UUID-derived sandbox trust anchors."""

import hashlib
import shlex

import space_smoke as smoke
from space_smoke.capabilities import CommandSandbox, KindColdCreateConfig

ANCHOR = "/usr/local/share/ca-certificates/space-sandbox-anchor.crt"


def _anchor(test: smoke.CommandSandboxFixture, sandbox: CommandSandbox) -> str:
    expected_id = sandbox.sandbox_id.removeprefix("sbx_")
    result = sandbox.run(
        "for root in /usr/local/share/ca-certificates/space-mitm-root-*.crt; do "
        "test ! -e \"$root\" && test ! -L \"$root\" || exit 1; done; "
        f"test ! -d /usr/local/share/ca-certificates/space && "
        f'test "$(cat /var/lib/space/sandbox-trust-id)" = {shlex.quote(expected_id)} && '
        f"cat {ANCHOR}"
    )
    test.verify(
        result.exit_code == 0
        and result.stdout.startswith("-----BEGIN CERTIFICATE-----"),
        "guest must contain its own public anchor and no shared SPACE trust roots",
        sandbox_id=sandbox.sandbox_id,
        stderr=result.stderr,
    )
    test.record(
        f"anchor_sha256_{sandbox.sandbox_id}",
        hashlib.sha256(result.stdout.encode()).hexdigest(),
    )
    return result.stdout


@smoke.scenario(
    "local.split_trust_lifecycle",
    ignored="Requires explicitly enabled split trust and the owned real-KMS Kind fixture",
    sandbox=smoke.SandboxConfig(
        timeout=900,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def split_trust_lifecycle(test: smoke.KindEcw) -> None:
    """Cold creation and forks isolate trust; both same-node and cross-node
    durable restores retain the byte-identical anchor."""
    with test.phase("cold create installs sandbox-specific trust before first command"):
        original = _anchor(test, test.sandbox)

    with test.phase("seed inherited shared trust in the production seed layout"):
        seeded = test.run_command(
            "openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes "
            "-keyout /tmp/shared-fixture.key -out /tmp/shared-fixture.crt -days 1 "
            "-subj '/CN=Inherited shared trust fixture' "
            "-addext 'basicConstraints=critical,CA:TRUE' "
            "-addext 'keyUsage=critical,keyCertSign,cRLSign' >/dev/null 2>&1 && "
            "sudo install -m 0644 /tmp/shared-fixture.crt "
            "/usr/local/share/ca-certificates/space-mitm-root-00.crt && "
            "sudo update-ca-certificates >/dev/null 2>&1 && "
            "rm /tmp/shared-fixture.key && "
            "openssl verify -CAfile /etc/ssl/certs/ca-certificates.crt /tmp/shared-fixture.crt >/dev/null && "
            "sha256sum /tmp/shared-fixture.crt"
        )
        test.verify(seeded.exit_code == 0, "shared-root fixture was not actually trusted")

    with test.phase("fork replaces inherited parent trust before child becomes usable"):
        child = test.sandboxes.fork(test.sandbox).sandbox
        child_anchor = _anchor(test, child)
        test.verify(
            original != child_anchor, "fork inherited the parent's trust anchor"
        )
        inherited = child.run("sha256sum /tmp/shared-fixture.crt")
        test.verify(
            inherited.exit_code == 0 and inherited.stdout == seeded.stdout,
            "fork did not inherit the public fixture certificate used by the trust probe",
        )
        removed = child.run(
            "openssl verify -CAfile /etc/ssl/certs/ca-certificates.crt /tmp/shared-fixture.crt"
        )
        test.verify(
            removed.exit_code != 0 and "self-signed certificate" in removed.stderr,
            "fork did not reject the inherited root specifically as untrusted",
        )
        test.record("inherited_shared_root_rejected", True)
        cleanup = test.run_command(
            "sudo rm /usr/local/share/ca-certificates/space-mitm-root-00.crt && "
            "sudo update-ca-certificates --fresh >/dev/null 2>&1"
        )
        test.verify(cleanup.exit_code == 0, "could not remove the parent fixture root")
        for sandbox, other in ((child, original), (test.sandbox, child_anchor)):
            result = sandbox.run(
                f"printf %s {shlex.quote(other)} > /tmp/split-trust-foreign.pem; "
                "openssl verify -CAfile /etc/ssl/certs/ca-certificates.crt /tmp/split-trust-foreign.pem"
            )
            test.verify(result.exit_code != 0, "one sandbox trusted a foreign anchor")

    with test.phase("same-node suspend and restore preserves exact certificate bytes"):
        test.pause()
        test.reconnect_by_id()
        test.verify(
            _anchor(test, test.sandbox) == original, "same-node restore changed anchor"
        )

    restore = test.cross_node_restore
    with test.phase("persist a snapshot for a cross-node restore"):
        source = restore.current_placement()
        test.pause()
        restore.wait_until_restore_portable()

    with restore.cordoned(source.node_name):
        with test.phase("restore from object storage on another node"):
            test.reconnect_by_id()
            landed = restore.current_placement()
            restore.expect_restored_from_object_storage(source=source, landed=landed)
            test.verify(
                _anchor(test, test.sandbox) == original,
                "cross-node restore changed anchor",
            )
