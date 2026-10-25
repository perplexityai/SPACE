"""Real curl/OpenSSL verification of the gateway's sandbox-specific chain."""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
import json
import re
import shlex
from typing import Protocol, cast

import space_smoke as smoke
from space_smoke.capabilities import CommandSandbox, KindColdCreateConfig

ANCHOR = "/usr/local/share/ca-certificates/space-sandbox-anchor.crt"
PROCESS_URL = "http://127.0.0.1:18880/"
PROCESS = """
import hashlib, http.server, json, os, secrets, socket, ssl
nonce = secrets.token_hex(24)
pid = os.getpid()
start_ticks = open("/proc/self/stat").read().split()[21]
boot_id = open("/proc/sys/kernel/random/boot_id").read().strip()
sequence = 0
tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
tls_context.load_verify_locations(cafile="/usr/local/share/ca-certificates/space-sandbox-anchor.crt")
tls_anchor = hashlib.sha256(tls_context.get_ca_certs(binary_form=True)[0]).hexdigest()
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global sequence
        if self.path == "/tls":
            with socket.create_connection(("example.com", 443), timeout=15) as connection:
                with tls_context.wrap_socket(connection, server_hostname="example.com") as tls:
                    leaf = hashlib.sha256(tls.getpeercert(binary_form=True)).hexdigest()
            value = dict(nonce=nonce, pid=pid, tls_context_id=id(tls_context),
                         anchor_sha256=tls_anchor, leaf_sha256=leaf, verified=True)
        else:
            sequence += 1
            value = dict(nonce=nonce, pid=pid, start_ticks=start_ticks,
                         boot_id=boot_id, sequence=sequence)
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
http.server.HTTPServer(("127.0.0.1", 18880), Handler).serve_forever()
"""


def process_state(test: smoke.CommandSandboxFixture) -> dict[str, object]:
    test.wait_until_command_matches(
        f"curl --fail --silent --noproxy '*' --max-time 3 {PROCESS_URL} "
        "> /tmp/split-trust-process.json && printf alive",
        stdout="alive", timeout=30,
        failure_message="the original in-memory guest process did not respond",
    )
    result = test.run_command("cat /tmp/split-trust-process.json")
    test.verify(result.exit_code == 0, "could not read process continuity response")
    return json.loads(result.stdout)


def process_tls(test: smoke.CommandSandboxFixture) -> dict[str, object]:
    test.wait_until_command_matches(
        f"curl --fail --silent --noproxy '*' --max-time 20 {PROCESS_URL}tls "
        "> /tmp/split-trust-process-tls.json && printf verified",
        stdout="verified", timeout=90,
        failure_message="the original process's unchanged in-memory TLS context could not verify its gateway",
    )
    result = test.run_command("cat /tmp/split-trust-process-tls.json")
    test.verify(result.exit_code == 0, "could not read the persistent TLS-context proof")
    evidence = json.loads(result.stdout)
    test.verify(evidence["verified"] is True, "the persistent process did not verify a real TLS handshake")
    return evidence


class PolicySandbox(CommandSandbox, Protocol):
    def set_network_policy(self, policy: Mapping[str, object]) -> None: ...


@smoke.scenario(
    "native.split_trust_tls",
    ignored="Requires explicitly enabled split trust and the owned real-KMS Kind fixture",
    api_surfaces=("native",),
    sandbox=smoke.SandboxConfig(
        timeout=1200,
        kind_cold_create=KindColdCreateConfig(vcpu=2, memory_mib=1024),
    ),
)
def split_trust_tls(test: smoke.CommandSandboxFixture) -> None:
    """A TLS-intercepted connection trusts its sandbox anchor, rejects a
    foreign root, and preserves a live process and trust across resident
    pause/resume and durable suspend/restore."""
    sandbox = cast(PolicySandbox, test.sandbox)
    with test.phase("boot a process whose identity and counter exist only in memory"):
        process = test.start_background_process(
            f"exec python3 -u -c {shlex.quote(PROCESS)}", timeout=1200,
        )
        initial_process = process_state(test)
        test.verify(
            initial_process["pid"] == process.pid,
            "the continuity server is not the process started through the public API",
        )
        test.record("process_before", initial_process)
        guest_binary = test.run_command(
            "sudo sh -c 'pid=$(pgrep -xo spaced-svc); test -n \"$pid\" && "
            "sha256sum /proc/$pid/exe' && uname -r"
        )
        test.verify(guest_binary.exit_code == 0, "could not fingerprint the live guest executable")
        test.record("guest_live_elf_and_kernel", guest_binary.stdout.strip())
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
        public_key = test.run_command(
            f"openssl x509 -in {ANCHOR} -pubkey -noout | "
            "openssl pkey -pubin -outform DER | sha256sum"
        )
        test.verify(public_key.exit_code == 0, "could not read anchor public key")
        test.record("anchor_spki_sha256", public_key.stdout.split()[0])
        default_trust = test.run_command(
            "curl --fail --silent --show-error --noproxy '*' --max-time 20 "
            "https://example.com/ -o /dev/null"
        )
        test.verify(
            default_trust.exit_code == 0,
            "default guest trust bundle rejected its gateway",
        )
        initial_tls = process_tls(test)
        test.verify(
            initial_tls["nonce"] == initial_process["nonce"] and initial_tls["pid"] == process.pid,
            "TLS verification ran in a different process",
        )
        test.record("persistent_tls_context_before", initial_tls)

    with test.phase("OpenSSL reports and verifies the actual gateway chain"):
        result = test.run_command(
            "timeout 20 openssl s_client -connect example.com:443 -servername example.com "
            "-verify_hostname example.com "
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

    previous_sequence = int(initial_process["sequence"])
    for transition, stop, wake in (
        ("resident_pause_resume", test.pause_resident, test.wake_from_resident_pause),
        ("durable_suspend_restore", test.suspend_durably, test.wake_from_durable_suspend),
    ):
        with test.phase(f"{transition}: preserve the live process and installed trust"):
            stop()
            wake()
            observed = process_state(test)
            for field in ("nonce", "pid", "start_ticks", "boot_id"):
                test.verify(
                    observed[field] == initial_process[field],
                    "a lifecycle transition restarted the process or guest",
                    transition=transition, field=field,
                )
            test.verify(
                int(observed["sequence"]) > previous_sequence,
                "the restored process did not advance its in-memory counter",
            )
            previous_sequence = int(observed["sequence"])
            restored = test.run_command(
                f"openssl x509 -in {ANCHOR} -noout -fingerprint -sha256"
            )
            test.expect_command(restored, stdout=certificate.stdout)
            restored_key = test.run_command(
                f"openssl x509 -in {ANCHOR} -pubkey -noout | "
                "openssl pkey -pubin -outform DER | sha256sum"
            )
            test.expect_command(restored_key, stdout=public_key.stdout)
            test.wait_until_command_matches(probe, stdout="verified", timeout=90)
            preserved_tls = process_tls(test)
            for field in ("nonce", "pid", "tls_context_id", "anchor_sha256"):
                test.verify(
                    preserved_tls[field] == initial_tls[field],
                    "the process or its in-memory trust context changed",
                    transition=transition, field=field,
                )
            rejected = test.run_command(
                "curl --silent --show-error --noproxy '*' --max-time 20 "
                "--cacert /tmp/foreign.crt --capath /tmp/split-trust-empty-ca "
                "https://example.com/ -o /dev/null"
            )
            test.verify(
                rejected.exit_code == 60,
                "restored TLS unexpectedly accepted the foreign root",
                transition=transition, exit_code=rejected.exit_code,
            )
            test.record(transition, {
                "process": observed,
                "anchor_fingerprint": restored.stdout.strip(),
                "spki_sha256": restored_key.stdout.split()[0],
                "own_anchor_tls_verified": True,
                "persistent_tls_context": preserved_tls,
                "foreign_anchor_tls_exit_code": rejected.exit_code,
            })
