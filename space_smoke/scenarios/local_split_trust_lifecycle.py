"""Public lifecycle proof for UUID-derived sandbox trust anchors."""

import hashlib
import shlex

import space_smoke as smoke
from space_smoke.capabilities import CommandSandbox, KindColdCreateConfig

ANCHOR = "/usr/local/share/ca-certificates/space-sandbox-anchor.crt"


def _anchor(test: smoke.CommandSandboxFixture, sandbox: CommandSandbox) -> str:
    expected_id = sandbox.sandbox_id.removeprefix("sbx_")
    result = sandbox.run(
        f"test ! -d /usr/local/share/ca-certificates/space && "
        f'test "$(cat /var/lib/space/sandbox-trust-id)" = {shlex.quote(expected_id)} && '
        f"cat {ANCHOR}"
    )
    test.verify(
        result.exit_code == 0
        and result.stdout.startswith("-----BEGIN CERTIFICATE-----"),
        "guest must contain its own public anchor and no shared SPACE trust directory",
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
    templates=(),
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

    with test.phase("fork replaces inherited parent trust before child becomes usable"):
        child = test.sandboxes.fork(test.sandbox).sandbox
        child_anchor = _anchor(test, child)
        test.verify(
            original != child_anchor, "fork inherited the parent's trust anchor"
        )
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
