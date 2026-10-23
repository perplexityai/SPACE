"""Real curl/OpenSSL verification of the gateway's sandbox-specific chain."""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
import re
import shlex
from typing import Protocol, cast

import space_smoke as smoke
from space_smoke.capabilities import CommandSandbox

ANCHOR = "/usr/local/share/ca-certificates/space-sandbox-anchor.crt"


class PolicySandbox(CommandSandbox, Protocol):
    def set_network_policy(self, policy: Mapping[str, object]) -> None: ...


@smoke.scenario(
    "native.split_trust_tls",
    templates=(),
    api_surfaces=("native",),
    sandbox=smoke.SandboxConfig(timeout=600),
)
def split_trust_tls(test: smoke.CommandSandboxFixture) -> None:
    """A TLS-intercepted connection trusts its sandbox anchor, rejects a
    foreign root, and keeps working after a durable restore."""
    sandbox = cast(PolicySandbox, test.sandbox)
    with test.phase("enable TLS interception through the public policy API"):
        sandbox.set_network_policy(
            {
                "version": "v1",
                "egress": {"default_action": "allow"},
                "tls_interception": "on",
            }
        )

    probe = (
        "mkdir -p /tmp/split-trust-empty-ca && "
        "curl --fail --silent --show-error --noproxy '*' --max-time 20 "
        f"--cacert {ANCHOR} --capath /tmp/split-trust-empty-ca "
        "https://example.com/ -o /dev/null && printf verified"
    )
    with test.phase("real curl validates the intercepted chain using only this anchor"):
        test.wait_until_command_matches(
            probe,
            stdout="verified",
            timeout=90,
            failure_message="curl did not validate the sandbox-specific gateway chain",
        )
        certificate = test.run_command(
            f"openssl x509 -in {ANCHOR} -noout -fingerprint -sha256"
        )
        test.expect_command(certificate, stdout=certificate.stdout)
        test.record("anchor_fingerprint", certificate.stdout.strip())
        default_trust = test.run_command(
            "curl --fail --silent --show-error --noproxy '*' --max-time 20 "
            "https://example.com/ -o /dev/null"
        )
        test.verify(
            default_trust.exit_code == 0,
            "default guest trust bundle rejected its gateway",
        )

    with test.phase("OpenSSL reports and verifies the actual gateway chain"):
        result = test.run_command(
            "timeout 20 openssl s_client -connect example.com:443 -servername example.com "
            f"-verify_return_error -CAfile {ANCHOR} -no-CApath -no-CAstore -showcerts </dev/null"
        )
        test.verify(
            result.exit_code == 0
            and "space-gateway-intermediate-ca" in result.stdout
            and "SPACE Sandbox Anchor" in result.stdout,
            "actual TLS connection did not use the sandbox anchor gateway chain",
            tls_output=result.stdout,
            tls_errors=result.stderr,
        )
        certificates = re.findall(
            r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----",
            result.stdout,
            re.DOTALL,
        )
        test.verify(
            len(certificates) >= 2, "TLS server did not send its gateway intermediate"
        )
        expiry = test.run_command(
            f"printf %s {shlex.quote(certificates[1])} > /tmp/split-trust-gateway.pem; "
            "openssl x509 -in /tmp/split-trust-gateway.pem -noout -enddate"
        )
        test.verify(
            expiry.exit_code == 0, "could not inspect the real gateway CA expiry"
        )
        not_after = datetime.strptime(
            expiry.stdout.strip().removeprefix("notAfter="),
            "%b %d %H:%M:%S %Y %Z",
        ).replace(tzinfo=UTC)
        test.verify(
            not_after <= datetime.now(UTC) + timedelta(days=365, seconds=60),
            "gateway CA inherited the 100-year anchor expiry",
            not_after=not_after.isoformat(),
        )
        test.record("gateway_ca_not_after", not_after.isoformat())

    with test.phase(
        "real curl rejects the same gateway chain under an unrelated trust root"
    ):
        generated = test.run_command(
            "openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes "
            "-subj /CN=ForeignSandbox -days 1 -keyout /tmp/foreign.key "
            "-out /tmp/foreign.crt >/dev/null 2>&1"
        )
        test.verify(generated.exit_code == 0, "could not prepare foreign trust root")
        rejected = test.run_command(
            "curl --silent --show-error --noproxy '*' --max-time 20 "
            "--cacert /tmp/foreign.crt --capath /tmp/split-trust-empty-ca "
            "https://example.com/ -o /dev/null"
        )
        test.verify(
            rejected.exit_code == 60,
            "foreign-root curl must fail specifically at certificate verification",
            exit_code=rejected.exit_code,
            stderr=rejected.stderr,
        )

    with test.phase("durable restore reissues a gateway CA under the same anchor"):
        test.suspend_durably()
        test.wake_from_durable_suspend()
        test.wait_until_command_matches(probe, stdout="verified", timeout=90)
        restored = test.run_command(
            f"openssl x509 -in {ANCHOR} -noout -fingerprint -sha256"
        )
        test.expect_command(restored, stdout=certificate.stdout)
