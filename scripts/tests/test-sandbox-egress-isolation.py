#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["e2b==2.21.1", "e2b-code-interpreter==2.6.2"]
# ///
"""Sandbox egress-isolation smoke test for SPACE.

Creates a sandbox via the ECW API and probes whether each of a small set of
in-VPC / cluster / RFC1918 / IMDS destinations is reachable from inside the
guest at the **application layer**. The previous version of this script used
bash's `/dev/tcp/host/port` pseudo-device, which only checked layer-4 connect.
That was structurally inadequate against SPACE's net-gateway TPROXY: the
proxy accepts every guest SYN in the same netns (returning SYN-ACK locally)
and only then decides whether to forward upstream. `/dev/tcp` interpreted
that local accept as a real connection, producing false-positive REGRESSION
results for destinations that net-gateway was correctly denying — and would
have failed to detect a real regression where TPROXY itself was bypassed.

This version sends an application-shaped payload per target (DNS query,
PG SSLRequest, TLS ClientHello, HTTP GET, gRPC h2 preface, SSH read) and
classifies the result by what came back, not by whether connect() succeeded.

Exit codes:
  0   — every probe matched its expectation
  42  — at least one MUST_FAIL probe reached its destination at L7
        (Buildkite soft-fail; pipeline maps 42 → passed_with_failures)
  >0  — hard failure (sandbox creation broken, baseline MUST_PASS broken,
        pre-probe command-stream transport failed, or unexpected error).
        Hard-fails the build.

Set SECURITY_SMOKE_STRICT=1 to upgrade exit 42 to exit 1.
EGRESS_POD_IPS is required: a comma-separated list of representative pod IPv4
addresses from the deployment's pod subnets, with node-api speaking HTTP/2 on
port 9800. These remain MUST_FAIL isolation probes. EGRESS_TARGET is an optional
report label; it does not select network addresses. Missing probe configuration
fails before sandbox creation rather than silently omitting pod isolation.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import shlex
import subprocess
import sys
import textwrap
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Self

from e2b import CommandExitException
from e2b_code_interpreter import Sandbox

DEFAULT_TEMPLATE = "seed"
DEFAULT_AWS_REGION = "us-east-1"
PROBE_TIMEOUT_SECS = 5
DNS_DIAGNOSTIC_HOSTS = ("google.com", "example.com")
CREATE_TIMEOUT_SECS = 180
CREATE_ATTEMPTS = 3
CREATE_BACKOFF_BASE_SECS = 10.0

EXIT_OK = 0
EXIT_REGRESSION = 42
EXIT_HARD_FAIL = 1
COMMAND_STREAM_TRANSPORT_ERROR_MARKERS = (
    "StreamReset",
    "ReadTimeout",
    "The read operation timed out",
    "h2 protocol error",
    "error reading a body from connection",
)


@dataclass(frozen=True)
class Probe:
    name: str
    host: str
    port: int
    # One of: dns_resolve_host, http_get, https_hello, dns_query, pg_ssl,
    # ssh_banner, h2_preface
    kind: str
    expected: str  # "MUST_PASS" | "MUST_FAIL"


@dataclass(frozen=True)
class SmokeContext:
    target: str
    domain: str
    template: str
    sandbox_id: str


class ProbeCommandTransportError(RuntimeError):
    """Raised when the E2B command stream fails before probe output exists."""

    def __init__(self, exc_type: str, detail: str) -> None:
        self.exc_type = exc_type
        self.detail = detail
        super().__init__(f"{exc_type}: {detail}")

    @classmethod
    def from_exception(cls, exc: BaseException) -> Self:
        return cls(exc_type=_exception_type_name(exc), detail=_exception_detail(exc))


# Targets are chosen so that REACHED is unambiguous: a real destination returns
# protocol-shaped bytes; net-gateway's local intercept either RSTs, returns
# nothing, or returns bytes that don't match the expected shape.
PROBES: list[Probe] = [
    # MUST_PASS — at minimum, DNS resolution must work. Internet IP baselines
    # were dropped because on the `seed` template TPROXY currently denies
    # generic outbound HTTP/HTTPS; a separate test should track internet
    # egress posture if that becomes a requirement.
    Probe("dns-resolve google.com via system resolver", "google.com", 0, "dns_resolve_host", "MUST_PASS"),

    # MUST_FAIL — EC2 IMDS. Real IMDS returns `HTTP/1.0 200` followed by an
    # instance id matching `i-[0-9a-f]+`.
    Probe("IMDS 169.254.169.254:80 GET instance-id", "169.254.169.254", 80, "http_get", "MUST_FAIL"),

    # MUST_FAIL — VPC primary 10/8. Real Postgres replies with `S` or `N` to
    # the 8-byte SSLRequest packet.
    Probe("VPC 10.0.0.1:5432 (PG SSLRequest)", "10.0.0.1", 5432, "pg_ssl", "MUST_FAIL"),
    Probe("VPC 10.90.0.1:5432 (RDS subnet shape, PG SSLRequest)", "10.90.0.1", 5432, "pg_ssl", "MUST_FAIL"),

    # MUST_FAIL — Kubernetes Service CIDR. kube-dns replies to a DNS query
    # for example.com.; kube-apiserver completes a TLS handshake and returns
    # a JSON 401 to `GET /version`.
    Probe("K8s Service 172.20.0.10:53 (kube-dns query)", "172.20.0.10", 53, "dns_query", "MUST_FAIL"),
    Probe("K8s Service 172.20.0.1:443 (TLS ClientHello)", "172.20.0.1", 443, "https_hello", "MUST_FAIL"),

    # MUST_FAIL — RFC1918 192.168. Real SSH responds with `SSH-` banner.
    Probe("RFC1918 192.168.1.1:22 (SSH banner)", "192.168.1.1", 22, "ssh_banner", "MUST_FAIL"),

    # MUST_FAIL — SPACE sandbox slot ranges (peer sandboxes on the same node).
    # See `PlatformEgressHardDenyCidr` entries `space-slots-a` (10.11.0.0/16)
    # and `space-slots-b` (10.12.0.0/16) in crates/l20_domain/net/common/src/lib.rs.
    # Reaching any address in these CIDRs from a guest implies cross-sandbox
    # isolation has broken on the node.
    Probe("SPACE slots-a 10.11.0.1:22 (peer sandbox, SSH banner)", "10.11.0.1", 22, "ssh_banner", "MUST_FAIL"),
    Probe("SPACE slots-b 10.12.0.1:22 (peer sandbox, SSH banner)", "10.12.0.1", 22, "ssh_banner", "MUST_FAIL"),
]


def pod_probes() -> list[Probe]:
    # Pod subnets belong to the deployment; never infer them from its hostname.
    hosts = os.environ.get("EGRESS_POD_IPS", "").split(",")
    if not all(host.strip() for host in hosts):
        raise SystemExit("EGRESS_POD_IPS must contain comma-separated pod IPv4 addresses")
    return [
        Probe(f"Pod {host}:9800 (h2 preface)", str(ipaddress.IPv4Address(host)), 9800, "h2_preface", "MUST_FAIL")
        for host in (value.strip() for value in hosts)
    ]


# In-guest probe script. Receives the target list as JSON on stdin and prints
# one JSON line per probe to stdout with fields {idx, outcome, elapsed_ms,
# detail}. The outer script correlates by `idx`.
#
# Outcomes:
#   REACHED               — TCP connected AND response matched protocol shape.
#   INTERCEPTED_OR_DENIED — TCP "connected" but data exchange failed (RST,
#                           empty response, unexpected bytes). The TPROXY
#                           local-accept-then-deny signature.
#   TIMEOUT               — SYN got no response.
#   REFUSED               — SYN got RST (no listener).
#   UNREACHABLE           — ICMP unreachable / kernel routing rejected.
#   RESOLVE_FAIL          — DNS lookup failed (for dns_resolve_host).
#   ERROR                 — other.
GUEST_PROBE_SCRIPT = textwrap.dedent("""\
    import errno
    import json
    import socket
    import ssl
    import sys
    import time


    TIMEOUT = 5.0


    def emit(idx, outcome, elapsed_ms, detail=""):
        print(json.dumps({"idx": idx, "outcome": outcome, "elapsed_ms": elapsed_ms, "detail": detail}), flush=True)


    def classify_oserror(e):
        if e.errno in (errno.ECONNREFUSED,):
            return "REFUSED"
        if e.errno in (errno.ENETUNREACH, errno.EHOSTUNREACH):
            return "UNREACHABLE"
        if e.errno in (errno.ECONNRESET, errno.EPIPE):
            return "INTERCEPTED_OR_DENIED"
        return "ERROR"


    def connect_and_send(host, port, payload, timeout=TIMEOUT, use_tls=False, server_hostname=None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect((host, port))
            if use_tls:
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                s = ctx.wrap_socket(s, server_hostname=server_hostname or host)
            if payload:
                s.sendall(payload)
            s.settimeout(3.0)
            data = b""
            while len(data) < 4096:
                chunk = s.recv(4096 - len(data))
                if not chunk:
                    break
                data += chunk
            return ("OK", data)
        finally:
            try:
                s.close()
            except Exception:
                pass


    def probe_http_get(idx, host, port):
        t0 = time.monotonic()
        try:
            payload = f"GET /latest/meta-data/instance-id HTTP/1.0\\r\\nHost: {host}\\r\\n\\r\\n".encode()
            _, data = connect_and_send(host, port, payload)
            elapsed = int((time.monotonic() - t0) * 1000)
            if data.startswith(b"HTTP/"):
                emit(idx, "REACHED", elapsed, f"http_status_line={data.splitlines()[0].decode('latin-1', 'replace')!r}")
            elif not data:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, "empty_response")
            else:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, f"unexpected={data[:64]!r}")
        except socket.timeout:
            emit(idx, "TIMEOUT", int((time.monotonic() - t0) * 1000))
        except ssl.SSLError as e:
            emit(idx, "INTERCEPTED_OR_DENIED", int((time.monotonic() - t0) * 1000), f"ssl_error={e}")
        except OSError as e:
            emit(idx, classify_oserror(e), int((time.monotonic() - t0) * 1000), f"errno={errno.errorcode.get(e.errno, e.errno)}")


    def probe_https_hello(idx, host, port):
        # A successful TLS handshake is itself proof of reach to a real TLS
        # server (sees a ServerHello, validates certificate format). We do
        # NOT enter a recv loop afterwards because kube-apiserver and most
        # HTTPS servers stay silent post-handshake until they get an HTTP
        # request — a recv would time out at 3s and falsely emit TIMEOUT,
        # masking a real regression where the destination is reachable.
        t0 = time.monotonic()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        try:
            s.connect((host, port))
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            tls = ctx.wrap_socket(s, server_hostname=host)
            try:
                cipher = tls.cipher()
                version = tls.version()
            finally:
                try:
                    tls.unwrap()
                except Exception:
                    pass
            elapsed = int((time.monotonic() - t0) * 1000)
            emit(idx, "REACHED", elapsed, f"tls_handshake_ok version={version} cipher={cipher[0] if cipher else None}")
        except (ssl.SSLError, ssl.SSLEOFError) as e:
            emit(idx, "INTERCEPTED_OR_DENIED", int((time.monotonic() - t0) * 1000), f"ssl_error={e}")
        except socket.timeout:
            emit(idx, "TIMEOUT", int((time.monotonic() - t0) * 1000))
        except OSError as e:
            emit(idx, classify_oserror(e), int((time.monotonic() - t0) * 1000), f"errno={errno.errorcode.get(e.errno, e.errno)}")
        finally:
            try:
                s.close()
            except Exception:
                pass


    def probe_dns_query(idx, host, port):
        # DNS over TCP (RFC 1035 §4.2.2) requires a 2-byte big-endian length
        # prefix. Without it, the server reads the txn id as the length and
        # blocks waiting for bytes that never arrive.
        t0 = time.monotonic()
        try:
            msg = bytes.fromhex("12340100000100000000000007") + b"example" + bytes.fromhex("03") + b"com" + bytes.fromhex("0000010001")
            query = len(msg).to_bytes(2, "big") + msg
            _, data = connect_and_send(host, port, payload=query)
            elapsed = int((time.monotonic() - t0) * 1000)
            # Response is also length-prefixed: 2 bytes length, then the DNS
            # message starting with txn id 0x1234.
            if len(data) >= 4 and data[2:4] == b"\\x12\\x34":
                emit(idx, "REACHED", elapsed, f"dns_response bytes={len(data)}")
            elif not data:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, "empty_response")
            else:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, f"unexpected_bytes={data[:16].hex()}")
        except socket.timeout:
            emit(idx, "TIMEOUT", int((time.monotonic() - t0) * 1000))
        except OSError as e:
            emit(idx, classify_oserror(e), int((time.monotonic() - t0) * 1000), f"errno={errno.errorcode.get(e.errno, e.errno)}")


    def probe_pg_ssl(idx, host, port):
        # PostgreSQL SSLRequest: 8 bytes [00 00 00 08 04 d2 16 2f].
        # Real PG replies with a single byte 'S' (TLS supported) or 'N' (not).
        t0 = time.monotonic()
        try:
            _, data = connect_and_send(host, port, payload=bytes.fromhex("00000008 04d2162f".replace(" ", "")))
            elapsed = int((time.monotonic() - t0) * 1000)
            if data and data[:1] in (b"S", b"N"):
                emit(idx, "REACHED", elapsed, f"pg_ssl_response={data[:1]!r}")
            elif not data:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, "empty_response")
            else:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, f"unexpected_bytes={data[:16].hex()}")
        except socket.timeout:
            emit(idx, "TIMEOUT", int((time.monotonic() - t0) * 1000))
        except OSError as e:
            emit(idx, classify_oserror(e), int((time.monotonic() - t0) * 1000), f"errno={errno.errorcode.get(e.errno, e.errno)}")


    def probe_ssh_banner(idx, host, port):
        # Real SSH server sends 'SSH-x.y-...' banner immediately on connect.
        t0 = time.monotonic()
        try:
            _, data = connect_and_send(host, port, payload=b"")
            elapsed = int((time.monotonic() - t0) * 1000)
            if data.startswith(b"SSH-"):
                emit(idx, "REACHED", elapsed, f"ssh_banner={data[:32]!r}")
            elif not data:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, "empty_response")
            else:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, f"unexpected_bytes={data[:16].hex()}")
        except socket.timeout:
            emit(idx, "TIMEOUT", int((time.monotonic() - t0) * 1000))
        except OSError as e:
            emit(idx, classify_oserror(e), int((time.monotonic() - t0) * 1000), f"errno={errno.errorcode.get(e.errno, e.errno)}")


    def probe_h2_preface(idx, host, port):
        # HTTP/2 connection preface. Real gRPC server replies with SETTINGS
        # frame (frame_type=0x04 at offset 3 of any frame). The exact opening
        # bytes vary, so we just check that *some* bytes come back.
        t0 = time.monotonic()
        try:
            _, data = connect_and_send(host, port, payload=b"PRI * HTTP/2.0\\r\\n\\r\\nSM\\r\\n\\r\\n")
            elapsed = int((time.monotonic() - t0) * 1000)
            # A real h2 server sends a SETTINGS frame in response. Frame format:
            # 3 bytes length, 1 byte type, 1 byte flags, 4 bytes stream_id.
            # type=0x04 is SETTINGS.
            if len(data) >= 9 and data[3] == 0x04:
                emit(idx, "REACHED", elapsed, f"h2_settings_frame bytes={len(data)}")
            elif not data:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, "empty_response")
            else:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, f"unexpected_bytes={data[:16].hex()}")
        except socket.timeout:
            emit(idx, "TIMEOUT", int((time.monotonic() - t0) * 1000))
        except OSError as e:
            emit(idx, classify_oserror(e), int((time.monotonic() - t0) * 1000), f"errno={errno.errorcode.get(e.errno, e.errno)}")


    def probe_dns_resolve_host(idx, host, _port):
        # Resolve via the system resolver. Any non-loopback IPv4 counts as
        # REACHED — net-gateway has a working upstream DNS path.
        t0 = time.monotonic()
        try:
            addr = socket.gethostbyname(host)
            elapsed = int((time.monotonic() - t0) * 1000)
            if addr and not addr.startswith("127.") and addr != "0.0.0.0":
                emit(idx, "REACHED", elapsed, f"resolved={addr}")
            else:
                emit(idx, "INTERCEPTED_OR_DENIED", elapsed, f"loopback_or_zero={addr}")
        except socket.gaierror as e:
            emit(idx, "RESOLVE_FAIL", int((time.monotonic() - t0) * 1000), f"gaierror={e}")
        except Exception as e:
            emit(idx, "ERROR", int((time.monotonic() - t0) * 1000), f"{type(e).__name__}: {e}")


    DISPATCH = {
        "http_get": probe_http_get,
        "https_hello": probe_https_hello,
        "dns_query": probe_dns_query,
        "pg_ssl": probe_pg_ssl,
        "ssh_banner": probe_ssh_banner,
        "h2_preface": probe_h2_preface,
        "dns_resolve_host": probe_dns_resolve_host,
    }


    def main():
        try:
            targets = json.load(sys.stdin)
        except Exception as e:
            print(f"FATAL: cannot read targets json: {e}", file=sys.stderr)
            sys.exit(2)
        for t in targets:
            fn = DISPATCH.get(t["kind"])
            if fn is None:
                emit(t["idx"], "ERROR", 0, f"unknown_kind={t['kind']}")
                continue
            try:
                fn(t["idx"], t["host"], t["port"])
            except Exception as e:
                emit(t["idx"], "ERROR", 0, f"{type(e).__name__}: {e}")


    if __name__ == "__main__":
        main()
""")


GUEST_DNS_DIAGNOSTICS_SCRIPT = textwrap.dedent("""\
    import errno
    import json
    import os
    import socket
    import subprocess
    import sys
    import time


    COMMAND_TIMEOUT = 8
    DNS_TIMEOUT = 3.0
    MAX_OUTPUT_CHARS = 4000


    def truncate(value, limit=MAX_OUTPUT_CHARS):
        if value is None:
            return ""
        if isinstance(value, bytes):
            value = value.decode("utf-8", "replace")
        if len(value) <= limit:
            return value
        return value[:limit] + f"...<truncated {len(value) - limit} chars>"


    def read_file(path):
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                return {"ok": True, "content": truncate(handle.read())}
        except FileNotFoundError:
            return {"ok": False, "error": "not_found"}
        except OSError as error:
            return {"ok": False, "error": f"{type(error).__name__}: {error}"}


    def run_command(args, timeout=COMMAND_TIMEOUT):
        try:
            completed = subprocess.run(
                args,
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=timeout,
            )
            return {
                "ok": completed.returncode == 0,
                "exit_code": completed.returncode,
                "output": truncate(completed.stdout),
            }
        except FileNotFoundError:
            return {"ok": False, "error": "not_found"}
        except subprocess.TimeoutExpired as error:
            return {
                "ok": False,
                "error": f"timeout_after_{timeout}s",
                "output": truncate(error.stdout),
            }
        except OSError as error:
            return {"ok": False, "error": f"{type(error).__name__}: {error}"}


    def parse_nameservers(resolv_conf):
        nameservers = []
        for line in resolv_conf.splitlines():
            line = line.split("#", 1)[0].split(";", 1)[0].strip()
            parts = line.split()
            if len(parts) >= 2 and parts[0] == "nameserver":
                nameservers.append(parts[1])
        return nameservers


    def build_dns_query(host):
        qname = b"".join(
            len(label).to_bytes(1, "big") + label.encode("ascii")
            for label in host.rstrip(".").split(".")
        )
        return b"\\x53\\x44\\x01\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00" + qname + b"\\x00\\x00\\x01\\x00\\x01"


    def parse_dns_response(message):
        if len(message) < 12:
            return {"bytes": len(message), "error": "short_response"}
        flags = int.from_bytes(message[2:4], "big")
        return {
            "bytes": len(message),
            "response_id": message[:2].hex(),
            "rcode": flags & 0xF,
            "answer_count": int.from_bytes(message[6:8], "big"),
        }


    def resolver_address(resolver, protocol):
        socket_type = socket.SOCK_DGRAM if protocol == "udp" else socket.SOCK_STREAM
        return socket.getaddrinfo(resolver, 53, type=socket_type)[0]


    def recv_exact(sock, byte_count):
        data = b""
        while len(data) < byte_count:
            chunk = sock.recv(byte_count - len(data))
            if not chunk:
                raise OSError("short tcp dns response")
            data += chunk
        return data


    def raw_dns(resolver, host, protocol):
        query = build_dns_query(host)
        started = time.monotonic()
        try:
            family, socket_type, proto, _canonname, sockaddr = resolver_address(resolver, protocol)
            with socket.socket(family, socket_type, proto) as sock:
                sock.settimeout(DNS_TIMEOUT)
                if protocol == "udp":
                    sock.sendto(query, sockaddr)
                    response, _peer = sock.recvfrom(4096)
                else:
                    sock.connect(sockaddr)
                    sock.sendall(len(query).to_bytes(2, "big") + query)
                    response = recv_exact(sock, int.from_bytes(recv_exact(sock, 2), "big"))
            return {
                "ok": True,
                "elapsed_ms": int((time.monotonic() - started) * 1000),
                **parse_dns_response(response),
            }
        except socket.timeout:
            error = "timeout"
        except OSError as exc:
            errno_name = errno.errorcode.get(getattr(exc, "errno", None), getattr(exc, "errno", None))
            error = f"{type(exc).__name__}: errno={errno_name} {exc}"
        return {"ok": False, "elapsed_ms": int((time.monotonic() - started) * 1000), "error": error}


    def getaddrinfo_command(host):
        code = (
            "import json, socket, sys\\n"
            "rows = []\\n"
            "for family, socktype, proto, canonname, sockaddr in socket.getaddrinfo(sys.argv[1], None):\\n"
            "    rows.append({"
            "'family': getattr(family, 'name', str(family)), "
            "'socktype': getattr(socktype, 'name', str(socktype)), "
            "'proto': proto, "
            "'canonname': canonname, "
            "'address': sockaddr[0] if sockaddr else None"
            "})\\n"
            "print(json.dumps(rows, separators=(',', ':')))\\n"
        )
        return run_command([sys.executable, "-c", code, host])


    def main():
        hosts = json.load(sys.stdin)
        resolv_conf = read_file("/etc/resolv.conf")
        resolv_content = resolv_conf.get("content", "") if resolv_conf.get("ok") else ""
        nameservers = parse_nameservers(resolv_content)

        diagnostics = {
            "guest": {
                "hostname": socket.gethostname(),
                "uname": " ".join(os.uname()),
            },
            "files": {
                "/etc/resolv.conf": resolv_conf,
                "/etc/nsswitch.conf": read_file("/etc/nsswitch.conf"),
            },
            "nameservers": nameservers,
            "commands": {
                "ip route": run_command(["ip", "route"], timeout=4),
                "ip -o addr show": run_command(["ip", "-o", "addr", "show"], timeout=4),
                "ss -Hlunpt": run_command(["ss", "-Hlunpt"], timeout=4),
            },
            "system_resolver": {},
            "raw_dns": {},
        }

        for host in hosts:
            diagnostics["system_resolver"][host] = {
                "getent hosts": run_command(["getent", "hosts", host]),
                "socket.getaddrinfo": getaddrinfo_command(host),
            }

        for resolver in nameservers:
            diagnostics["raw_dns"][resolver] = {}
            for host in hosts:
                diagnostics["raw_dns"][resolver][host] = {
                    "udp": raw_dns(resolver, host, "udp"),
                    "tcp": raw_dns(resolver, host, "tcp"),
                }

        print(json.dumps(diagnostics, separators=(",", ":")), flush=True)


    if __name__ == "__main__":
        main()
""")


def fetch_api_key_from_secrets_manager() -> str:
    secret_id = os.environ.get("ECW_API_KEY_SECRET_ID")
    if not secret_id:
        raise SystemExit("Set E2B_API_KEY or ECW_API_KEY_SECRET_ID")
    return subprocess.check_output(
        [
            "aws",
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            secret_id,
            "--region",
            os.environ.get("AWS_REGION", DEFAULT_AWS_REGION),
            "--query",
            "SecretString",
            "--output",
            "text",
        ],
        text=True,
    ).strip()


def api_key() -> str:
    value = os.environ.get("E2B_API_KEY")
    if value:
        return value
    try:
        return fetch_api_key_from_secrets_manager()
    except subprocess.CalledProcessError as error:
        raise SystemExit(
            "E2B_API_KEY is not set and fetching the configured ECW key from "
            "AWS Secrets Manager failed"
        ) from error


def api_url(domain: str, path: str) -> str:
    return f"https://api.{domain}{path}"


def request_headers(key: str) -> dict[str, str]:
    return {"X-API-Key": key, "Content-Type": "application/json"}


SAFE_SANDBOX_DETAIL_FIELDS = (
    "sandboxID",
    "templateID",
    "alias",
    "state",
    "startedAt",
    "endAt",
    "domain",
    "envdVersion",
    "cpuCount",
    "memoryMB",
    "diskSizeMB",
    # These placement/version fields are not part of the public OpenAPI schema,
    # but are safe to print if an internal deployment includes them.
    "clusterID",
    "clusterId",
    "nodeID",
    "nodeId",
    "activeNodeID",
    "activeNodeId",
    "targetNodeID",
    "targetNodeId",
    "nodeName",
    "k8sNodeName",
    "availabilityZone",
    "az",
    "version",
    "runtimeVersion",
    "workerVersion",
    "netGatewayVersion",
    "networkPolicyGeneration",
)


def fetch_sandbox_detail_context(domain: str, key: str, sandbox_id: str) -> dict[str, Any]:
    request = urllib.request.Request(
        api_url(domain, f"/sandboxes/{sandbox_id}"),
        headers=request_headers(key),
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            detail = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return {"error": f"GET /sandboxes/{sandbox_id} returned HTTP {error.code}"}
    except (OSError, json.JSONDecodeError) as error:
        return {"error": f"{type(error).__name__}: {error}"}

    if not isinstance(detail, dict):
        return {"error": "sandbox detail response was not a JSON object"}
    return {key: detail[key] for key in SAFE_SANDBOX_DETAIL_FIELDS if key in detail and detail[key] is not None}


def collect_dns_baseline_diagnostics(sbx: Sandbox, hosts: tuple[str, ...]) -> dict[str, Any]:
    hosts_json = json.dumps(list(hosts))
    cmd = f"echo {shlex.quote(hosts_json)} | python3 /tmp/space-egress-dns-diagnostics.py"
    stdout = ""
    stderr = ""
    try:
        # Staging the helper script is best-effort like the rest of this
        # collection: if the write itself fails, return an error dict so the
        # caller still prints the probe table and Buildkite summary instead of
        # this exception hiding the original DNS-baseline failure context.
        sbx.files.write("/tmp/space-egress-dns-diagnostics.py", GUEST_DNS_DIAGNOSTICS_SCRIPT)
        out = sbx.commands.run(cmd, timeout=90)
        stdout = out.stdout or ""
        stderr = out.stderr or ""
    except CommandExitException as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
    except Exception as error:
        return {"error": f"{type(error).__name__}: {error}"}

    for line in stdout.splitlines():
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            if stderr:
                parsed["stderr"] = _truncate(stderr)
            return parsed
    return {
        "error": "guest DNS diagnostics did not emit JSON",
        "stdout": _truncate(stdout),
        "stderr": _truncate(stderr),
    }


def _truncate(value: str, limit: int = 4000) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + f"...<truncated {len(value) - limit} chars>"


def _print_block(title: str, value: object) -> None:
    print(f"{title}:")
    if value in (None, ""):
        print("  <empty>")
        return
    if isinstance(value, (dict, list)):
        rendered = json.dumps(value, indent=2, sort_keys=True)
    else:
        rendered = str(value)
    print(textwrap.indent(rendered.rstrip() or "<empty>", "  "))


def print_dns_baseline_diagnostics(
    *,
    context: SmokeContext,
    sandbox_detail: dict[str, Any],
    dns_diagnostics: dict[str, Any],
) -> None:
    print("DNS BASELINE DIAGNOSTICS")
    _print_block(
        "Smoke context",
        {
            "target": context.target,
            "domain": context.domain,
            "template": context.template,
            "sandbox_id": context.sandbox_id,
            "build_url": os.environ.get("BUILDKITE_BUILD_URL", ""),
            "build_number": os.environ.get("BUILDKITE_BUILD_NUMBER", ""),
            "job_id": os.environ.get("BUILDKITE_JOB_ID", ""),
        },
    )
    _print_block("ECW sandbox detail safe fields", sandbox_detail or "<not available>")
    _print_block("Guest context", dns_diagnostics.get("guest", {}))
    _print_block("/etc/resolv.conf", dns_diagnostics.get("files", {}).get("/etc/resolv.conf", {}))
    _print_block("/etc/nsswitch.conf", dns_diagnostics.get("files", {}).get("/etc/nsswitch.conf", {}))
    _print_block("Parsed nameservers", dns_diagnostics.get("nameservers", []))
    _print_block("System resolver checks", dns_diagnostics.get("system_resolver", {}))
    _print_block("Raw DNS checks to configured resolvers", dns_diagnostics.get("raw_dns", {}))
    _print_block("Local network/socket context", dns_diagnostics.get("commands", {}))
    if error := dns_diagnostics.get("error"):
        _print_block("Diagnostic collection error", error)
    print(
        "Note: node, AZ, worker version, and net-gateway policy hydration are "
        "control-plane/service context. If they are not present in the ECW detail "
        "fields above, correlate this sandbox_id in Datadog."
    )


def classify_results(
    results: list[tuple[Probe, str, int, str]],
) -> tuple[list[Probe], list[Probe]]:
    must_pass_failures: list[Probe] = []
    must_fail_regressions: list[Probe] = []
    for p, outcome, _elapsed_ms, _detail in results:
        reached = outcome == "REACHED"
        if p.expected == "MUST_PASS" and not reached:
            must_pass_failures.append(p)
        if p.expected == "MUST_FAIL" and reached:
            must_fail_regressions.append(p)
    return must_pass_failures, must_fail_regressions


def has_dns_baseline_failure(must_pass_failures: list[Probe]) -> bool:
    return any(p.kind == "dns_resolve_host" for p in must_pass_failures)


def _exception_type_name(exc: BaseException) -> str:
    return f"{exc.__class__.__module__}.{exc.__class__.__name__}"


def _exception_detail(exc: BaseException) -> str:
    return str(exc) or repr(exc)


def _is_probe_command_transport_error(exc: BaseException) -> bool:
    detail = f"{_exception_type_name(exc)}: {_exception_detail(exc)}"
    return any(marker in detail for marker in COMMAND_STREAM_TRANSPORT_ERROR_MARKERS)


def _emit_buildkite_summary_annotation(
    *,
    must_pass_failures: list[Probe],
    must_fail_regressions: list[Probe],
    context: SmokeContext | None = None,
    dns_diagnostics_collected: bool = False,
    command_transport_error: ProbeCommandTransportError | None = None,
) -> None:
    """Surface a single Buildkite annotation summarizing the run.

    Uses the security smoke's distinct `security-smoke-summary` annotation
    context. No-op outside Buildkite.
    """
    if not os.environ.get("BUILDKITE_AGENT_NAME"):
        return
    if command_transport_error:
        style = "error"
    elif must_pass_failures:
        style = "error"
    elif must_fail_regressions:
        style = "warning"
    else:
        style = "success"
    if command_transport_error:
        body = (
            "**SPACE security smoke / egress isolation** — "
            "`pre-probe command-stream transport failure`, `0 probe verdicts`"
            "\n\nThe sandbox was created, but `sbx.commands.run(...)` failed before "
            "the guest probe table emitted any rows. Treat this as ECW/E2B "
            "command-stream/data-plane transport signal, not a DNS baseline or "
            "egress-policy verdict."
            f"\n\nException: `{command_transport_error.exc_type}`"
            f"\n\nDetail: `{command_transport_error.detail}`"
        )
    else:
        body = (
            f"**SPACE security smoke / egress isolation** — "
            f"`{len(PROBES) - len(must_pass_failures) - len(must_fail_regressions)} OK`, "
            f"`{len(must_fail_regressions)} regressions`, "
            f"`{len(must_pass_failures)} hard fails`"
        )
    if context:
        body += (
            f"\n\nContext: target `{context.target}`, template `{context.template}`, "
            f"sandbox `{context.sandbox_id}`, domain `{context.domain}`."
        )
    if must_pass_failures:
        body += "\n\nMUST_PASS baseline failures:\n" + "\n".join(f"- {p.name}" for p in must_pass_failures)
    if dns_diagnostics_collected:
        body += (
            "\n\nDNS baseline diagnostics were collected in the job log "
            "(`/etc/resolv.conf`, `getent`, `socket.getaddrinfo`, and raw UDP/TCP DNS "
            "to configured resolvers). This remains a hard failure."
        )
    if must_fail_regressions:
        body += "\n\nReachable from sandbox guest at L7 (should not be):\n" + "\n".join(
            f"- {p.name}" for p in must_fail_regressions
        )
    try:
        subprocess.run(
            [
                "buildkite-agent",
                "annotate",
                body,
                "--style",
                style,
                "--context",
                "security-smoke-summary",
            ],
            check=False,
            capture_output=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return


def run_probes(sbx: Sandbox) -> list[tuple[Probe, str, int, str]]:
    targets = [
        {"idx": i, "host": p.host, "port": p.port, "kind": p.kind}
        for i, p in enumerate(PROBES)
    ]
    sbx.files.write("/tmp/space-egress-probe.py", GUEST_PROBE_SCRIPT)

    # e2b's commands.run doesn't take a stdin kwarg, so embed the JSON in
    # the command line. Use shlex.quote so any future probe name containing
    # single quotes or backslashes survives shell parsing — otherwise the
    # JSON would be silently corrupted, every probe would emit NO_RESULT,
    # and the test would score as "OK no regressions" while hiding real
    # issues.
    targets_json = json.dumps(targets)
    cmd = f"echo {shlex.quote(targets_json)} | python3 /tmp/space-egress-probe.py"

    stdout = ""
    try:
        out = sbx.commands.run(cmd, timeout=PROBE_TIMEOUT_SECS * len(PROBES) + 30)
        stdout = out.stdout or ""
    except CommandExitException as exc:
        stdout = exc.stdout or ""
        if exc.stderr:
            print(f"[!] guest probe stderr: {exc.stderr}", flush=True)
    except Exception as exc:
        if _is_probe_command_transport_error(exc):
            raise ProbeCommandTransportError.from_exception(exc) from exc
        raise

    by_idx: dict[int, tuple[str, int, str]] = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        by_idx[obj["idx"]] = (obj["outcome"], obj.get("elapsed_ms", -1), obj.get("detail", ""))

    results: list[tuple[Probe, str, int, str]] = []
    for i, p in enumerate(PROBES):
        outcome, elapsed_ms, detail = by_idx.get(i, ("NO_RESULT", -1, "guest probe did not emit a line for this target"))
        results.append((p, outcome, elapsed_ms, detail))
    return results


def _is_retryable_create_error(exc: Exception) -> bool:
    """Whether a Sandbox.create failure is worth retrying.

    ECW surfaces API failures as exceptions whose message starts with the
    HTTP status (e.g. ``503: timed out waiting for sandbox pod to become
    ready``). Client errors are permanent for this run — a bad API key or
    unknown template will not heal on retry — except 408/429, which are
    load signals like the 5xx family. Anything without a leading status
    (connection resets, TLS failures, SDK timeouts) is treated as
    transient.
    """
    match = re.match(r"^\s*(\d{3})\b", str(exc))
    if match is None:
        return True
    status = int(match.group(1))
    return status >= 500 or status in (408, 429)


def create_sandbox_with_retries(template: str, key: str, domain: str) -> Sandbox:
    """Create the smoke sandbox, retrying transient failures with backoff.

    A brief create-capacity blip should not fail the isolation smoke before
    any egress probe executes.
    """
    last_exc: Exception | None = None
    for attempt in range(1, CREATE_ATTEMPTS + 1):
        try:
            return Sandbox.create(
                template=template, api_key=key, domain=domain, timeout=CREATE_TIMEOUT_SECS
            )
        except Exception as exc:
            if not _is_retryable_create_error(exc):
                raise
            last_exc = exc
            if attempt == CREATE_ATTEMPTS:
                break
            delay = CREATE_BACKOFF_BASE_SECS * (2 ** (attempt - 1))
            print(
                f"[!] sandbox create attempt {attempt}/{CREATE_ATTEMPTS} failed: {exc}; "
                f"retrying in {delay:.0f}s",
                flush=True,
            )
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc


def main() -> int:
    domain = os.environ.get("E2B_DOMAIN", "")
    if not domain:
        raise SystemExit("E2B_DOMAIN is required")
    template = os.environ.get("E2B_TEMPLATE", DEFAULT_TEMPLATE)
    strict = os.environ.get("SECURITY_SMOKE_STRICT") in ("1", "true", "True", "TRUE")

    target = os.environ.get("EGRESS_TARGET", "").strip() or domain
    PROBES.extend(pod_probes())
    print(f"[+] target={target} ({len(PROBES)} probes total)", flush=True)

    print(f"[+] connecting to {domain} (template={template})", flush=True)
    key = api_key()

    print("[+] creating sandbox", flush=True)
    sbx = create_sandbox_with_retries(template=template, key=key, domain=domain)
    print(f"[+] sandbox id={sbx.sandbox_id}", flush=True)
    context = SmokeContext(target=target, domain=domain, template=template, sandbox_id=sbx.sandbox_id)

    must_pass_failures: list[Probe] = []
    must_fail_regressions: list[Probe] = []
    dns_diagnostics: dict[str, Any] | None = None
    sandbox_detail: dict[str, Any] = {}
    command_transport_error: ProbeCommandTransportError | None = None
    results: list[tuple[Probe, str, int, str]] = []
    try:
        results = run_probes(sbx)
        must_pass_failures, must_fail_regressions = classify_results(results)
        if has_dns_baseline_failure(must_pass_failures):
            print("[+] collecting DNS baseline diagnostics before sandbox cleanup", flush=True)
            sandbox_detail = fetch_sandbox_detail_context(domain, key, sbx.sandbox_id)
            dns_diagnostics = collect_dns_baseline_diagnostics(sbx, DNS_DIAGNOSTIC_HOSTS)
    except ProbeCommandTransportError as exc:
        command_transport_error = exc
    finally:
        try:
            sbx.kill()
            print(f"[+] killed sandbox {sbx.sandbox_id}", flush=True)
        except Exception as kill_err:
            print(f"[!] sandbox kill failed: {kill_err}", flush=True)

    if command_transport_error:
        print()
        print("HARD FAIL: egress probe command stream failed before probe verdicts were emitted.")
        print(f"Exception: {command_transport_error.exc_type}")
        print(f"Detail: {command_transport_error.detail}")
        print(
            "No probe table was produced, so this run did not exercise DNS baseline, "
            "MUST_PASS connectivity, or MUST_FAIL isolation checks. Treat it as "
            "ECW/E2B command-stream/data-plane transport signal."
        )
        _emit_buildkite_summary_annotation(
            must_pass_failures=[],
            must_fail_regressions=[],
            context=context,
            command_transport_error=command_transport_error,
        )
        return EXIT_HARD_FAIL

    print()
    print(f"{'STATUS':<12} {'EXPECTED':<10} {'OUTCOME':<24} {'ELAPSED':>9}  PROBE  (detail)")
    print(f"{'-' * 120}")
    for p, outcome, elapsed_ms, detail in results:
        reached = outcome == "REACHED"
        if p.expected == "MUST_PASS":
            ok = reached
            tag = "OK" if ok else "HARD_FAIL"
        else:
            ok = not reached
            tag = "OK" if ok else "REGRESSION"
        elapsed = f"{elapsed_ms}ms" if elapsed_ms >= 0 else "-"
        print(f"{tag:<12} {p.expected:<10} {outcome:<24} {elapsed:>9}  {p.name}  ({detail})")
    print()

    if dns_diagnostics is not None:
        print_dns_baseline_diagnostics(
            context=context,
            sandbox_detail=sandbox_detail,
            dns_diagnostics=dns_diagnostics,
        )
        print()

    _emit_buildkite_summary_annotation(
        must_pass_failures=must_pass_failures,
        must_fail_regressions=must_fail_regressions,
        context=context,
        dns_diagnostics_collected=dns_diagnostics is not None,
    )

    if must_pass_failures:
        names = ", ".join(p.name for p in must_pass_failures)
        print(
            f"HARD FAIL: {len(must_pass_failures)} MUST_PASS probe(s) did not reach at L7: {names}.\n"
            "The sandbox's baseline upstream connectivity is broken; investigate the "
            "template's egress policy / net-gateway configuration."
        )
        return EXIT_HARD_FAIL

    if must_fail_regressions:
        names = ", ".join(p.name for p in must_fail_regressions)
        print(
            f"ISOLATION REGRESSION: {len(must_fail_regressions)} address(es) reached from a "
            f"sandbox guest at L7 that should not be: {names}.\n"
            "Net-gateway TPROXY appears to be forwarding traffic it should be denying."
        )
        return EXIT_HARD_FAIL if strict else EXIT_REGRESSION

    print("OK: sandbox guest's baseline upstream works and no must-fail target is reachable at L7.")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
