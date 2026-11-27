"""Raw DNS transport probes used by deployed network-policy smokes."""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from math import ceil
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from space_smoke.fixtures import Ecw


_PROBE_CODE = r"""
import errno, socket, sys

protocol, resolver, qname_text, timeout_seconds = sys.argv[1:]
labels = qname_text.rstrip(".").encode("ascii").split(b".")
qname = b"".join(bytes((len(part),)) + part for part in labels)
query = (b"SP\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00" + qname
         + b"\x00\x00\x01\x00\x01")
sock_type = socket.SOCK_DGRAM if protocol == "udp" else socket.SOCK_STREAM
blocked = {errno.ECONNREFUSED, errno.ECONNRESET, errno.EHOSTUNREACH,
           errno.ENETUNREACH, errno.ETIMEDOUT}

def recv_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise OSError("short TCP DNS response")
        data += chunk
    return data

try:
    family, kind, proto, _name, address = socket.getaddrinfo(
        resolver, 53, type=sock_type
    )[0]
    with socket.socket(family, kind, proto) as sock:
        sock.settimeout(float(timeout_seconds))
        if protocol == "udp":
            sock.sendto(query, address)
            response, _peer = sock.recvfrom(4096)
        else:
            sock.connect(address)
            sock.sendall(len(query).to_bytes(2, "big") + query)
            response = recv_exact(sock, int.from_bytes(recv_exact(sock, 2), "big"))
except socket.timeout:
    print("blocked: timeout")
    raise SystemExit(10)
except OSError as error:
    print(f"blocked: {type(error).__name__}: {error}")
    raise SystemExit(10 if error.errno in blocked else 11)

valid = len(response) >= 12 and response[:2] == b"SP" and response[2] & 0x80
rcode = response[3] & 0x0f if valid else -1
answers = int.from_bytes(response[6:8], "big") if valid else 0
print(f"response: rcode={rcode} answers={answers} bytes={len(response)}")
if rcode == 0 and answers > 0:
    raise SystemExit(0)
if rcode == 5 and answers == 0:
    raise SystemExit(13)
raise SystemExit(12)
""".strip()

_DNS_ACCEPTED = 0
_TRANSPORT_BLOCKED = 10
_DNS_REFUSED = 13
_REPLY_TIMEOUT_SECONDS = 3
_DENIAL_TIMEOUT_SECONDS = 2


def _command(protocol: str, resolver: str, qname: str, timeout_seconds: int) -> str:
    return (
        f"python3 -c {shlex.quote(_PROBE_CODE)} {protocol} "
        f"{shlex.quote(resolver)} {shlex.quote(qname)} {timeout_seconds}"
    )


def verify_dns_transport(
    test: Ecw,
    protocol: str,
    *,
    accepted: Sequence[tuple[str, str]],
    refused: Sequence[tuple[str, str]],
    blocked: Sequence[tuple[str, str]],
    control: Ecw | None = None,
) -> None:
    """Require accepted/refused DNS replies and transport denial as declared."""

    if protocol not in {"tcp", "udp"}:
        raise ValueError("DNS transport must be tcp or udp")
    evidence = test._context.evidence.setdefault("dns_transport_checks", [])
    assert isinstance(evidence, list)

    def probe(
        resolver: str,
        qname: str,
        expectation: str,
        guest: Ecw = test,
        *,
        timeout_seconds: int = _REPLY_TIMEOUT_SECONDS,
    ) -> int:
        result = guest.run_command(
            _command(protocol, resolver, qname, timeout_seconds),
            timeout=10,
            failure_message=f"{protocol.upper()} DNS probe did not complete",
        )
        evidence.append(
            {
                "protocol": protocol,
                "resolver": resolver,
                "qname": qname,
                "expectation": expectation,
                "timeout_seconds": timeout_seconds,
                "exit_code": result.exit_code,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )
        return result.exit_code

    def reply(
        resolver: str,
        qname: str,
        expectation: str,
        guest: Ecw = test,
        *,
        timeout_seconds: int = _REPLY_TIMEOUT_SECONDS,
    ) -> int:
        # UDP loss may hide a real reply. Retry only transport failures, never
        # an unexpected DNS response, and keep denial observations single-shot.
        # A control must reply within the paired denial's bound. Preserve the
        # positive observation's total timeout budget when its attempts shorten.
        attempts = ceil(_REPLY_TIMEOUT_SECONDS * (3 if protocol == "udp" else 1) / timeout_seconds)
        for _ in range(attempts):
            code = probe(resolver, qname, expectation, guest, timeout_seconds=timeout_seconds)
            if code != _TRANSPORT_BLOCKED:
                return code
        return code

    for resolver, qname in accepted:
        test.verify(
            reply(resolver, qname, "accepted") == _DNS_ACCEPTED,
            f"{protocol.upper()} DNS resolver did not accept the allowed QNAME",
            failure_evidence=test.collect_guest_network_snapshot,
            dns_transport_checks=evidence,
        )
    for resolver, qname in refused:
        test.verify(
            reply(resolver, qname, "refused") == _DNS_REFUSED,
            f"{protocol.upper()} DNS resolver did not refuse the policy-denied QNAME",
            failure_evidence=test.collect_guest_network_snapshot,
            dns_transport_checks=evidence,
        )
    # The gateway holds traffic until its create-time policy is installed.
    # Pair a bounded drop with replies from the same resolver on an unrestricted
    # guest; repeated timeouts cannot distinguish a dead resolver from policy.
    for resolver, qname in blocked:
        if control is None:
            raise ValueError("blocked DNS probes require an unrestricted control")
        test.verify(
            reply(
                resolver, qname, "control accepted before", control,
                timeout_seconds=_DENIAL_TIMEOUT_SECONDS,
            ) == _DNS_ACCEPTED,
            "the blocked resolver did not answer the positive control",
            dns_transport_checks=evidence,
        )
        test.verify(
            probe(resolver, qname, "blocked", timeout_seconds=_DENIAL_TIMEOUT_SECONDS) == _TRANSPORT_BLOCKED,
            f"sandbox unexpectedly reached a blocked {protocol.upper()} DNS resolver",
            failure_evidence=test.collect_guest_network_snapshot,
            dns_transport_checks=evidence,
        )
        test.verify(
            reply(
                resolver, qname, "control accepted after", control,
                timeout_seconds=_DENIAL_TIMEOUT_SECONDS,
            ) == _DNS_ACCEPTED,
            "the blocked resolver stopped answering the positive control",
            dns_transport_checks=evidence,
        )
