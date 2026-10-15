"""Guest probe for shared-Fastly-edge domain-policy bypasses."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

from space_smoke.runtime import InconclusiveSmoke

if TYPE_CHECKING:
    from space_smoke.fixtures import Ecw


_PROBE_PATH = "/tmp/space-smoke-fastly-shared-edge.py"
# The guest only observes; `classify` judges on the host. The probe records
# whether each plaintext response carries the edge's `X-Served-By` header:
# responses the gateway generates itself (403 refusals, 502 upstream failures)
# never do, so a header comparison is meaningful only between edge responses.
_PROBE = r"""
import json
import socket
import ssl

ALLOWED = "pypi.org"
CO_TENANT = "fiddle.fastly.dev"
UNRELATED = "example.com"
TIMEOUT = 5
MAX_RESPONSE = 8 * 1024
MAX_BODY_EVIDENCE = 512

def read_http(sock, capture_body):
    response = bytearray()
    while len(response) < MAX_RESPONSE:
        try:
            chunk = sock.recv(min(4096, MAX_RESPONSE - len(response)))
        except socket.timeout:
            break
        if not chunk:
            break
        response.extend(chunk)
        if not capture_body and b"\r\n" in response:
            break
    head, separator, body = bytes(response).partition(b"\r\n\r\n")
    lines = head.split(b"\r\n")
    first_line = lines[0].decode(errors="replace")
    result = {"first_line": first_line}
    if capture_body:
        names = {
            line.split(b":", 1)[0].strip().lower()
            for line in lines[1:]
            if b":" in line
        }
        result["edge_served"] = bool(separator) and b"x-served-by" in names
        result["body"] = (
            body[:MAX_BODY_EVIDENCE].decode(errors="replace") if separator else ""
        )
    return result

def tls_probe(ip, server_name):
    try:
        with socket.create_connection((ip, 443), timeout=TIMEOUT) as raw:
            context = ssl._create_unverified_context()
            with context.wrap_socket(raw, server_hostname=server_name) as secure:
                secure.settimeout(TIMEOUT)
                secure.sendall(
                    f"GET / HTTP/1.1\r\nHost: {server_name}\r\nConnection: close\r\n\r\n".encode()
                )
                result = read_http(secure, False)
                result["connected"] = True
                return result
    except Exception as error:
        return {
            "connected": False,
            "error_type": type(error).__name__,
            "error": str(error)[:300],
        }

def http_probe(ip, host, extra_headers=()):
    try:
        with socket.create_connection((ip, 80), timeout=TIMEOUT) as plain:
            plain.settimeout(TIMEOUT)
            headers = "".join(f"{name}: {value}\r\n" for name, value in extra_headers)
            plain.sendall(
                f"GET /simple/ HTTP/1.1\r\nHost: {host}\r\n{headers}Connection: close\r\n\r\n".encode()
            )
            return read_http(plain, True)
    except Exception as error:
        return {"error_type": type(error).__name__, "error": str(error)[:300]}

ip = socket.gethostbyname(ALLOWED)
observed = {
    "resolved_ip": ip,
    "matching_tls": tls_probe(ip, ALLOWED),
    "co_tenant_tls": tls_probe(ip, CO_TENANT),
    "matching_http": http_probe(ip, ALLOWED),
    "co_tenant_http": http_probe(ip, CO_TENANT),
    "unrelated_http": http_probe(ip, UNRELATED),
    "fastly_host_override": http_probe(
        ip,
        ALLOWED,
        (("Fastly-Host", CO_TENANT), ("Fastly-SSL", "1")),
    ),
}
print(json.dumps(observed, sort_keys=True))
""".strip()


def _response(observed: Mapping[str, object], name: str) -> Mapping[str, object]:
    value = observed.get(name)
    return value if isinstance(value, Mapping) else {}


def _status_line(response: Mapping[str, object]) -> str:
    value = response.get("first_line")
    return value if isinstance(value, str) else ""


def _from_edge(response: Mapping[str, object]) -> bool:
    return _status_line(response).startswith("HTTP/") and response.get("edge_served") is True


def _status_code(response: Mapping[str, object]) -> int | None:
    parts = _status_line(response).split(" ", 2)
    if len(parts) < 2 or not parts[0].startswith("HTTP/") or not parts[1].isdigit():
        return None
    return int(parts[1])


def _server_error(response: Mapping[str, object]) -> bool:
    status = _status_code(response)
    return status is not None and status >= 500


def _gateway_refused(response: Mapping[str, object]) -> bool:
    # The gateway's host-binding refusal: 403, no body, no edge evidence. A
    # 403 the edge served came from beyond the gateway and is not a block.
    return (
        " 403 " in _status_line(response)
        and not response.get("body")
        and not _from_edge(response)
    )


def classify(observed: Mapping[str, object]) -> dict[str, list[str]]:
    """Judge one probe run from its observations.

    `bypass_detected` names checks whose evidence shows traffic escaping the
    allowed hostname. `allowed_failed` names allowed flows the gateway itself
    refused. `inconclusive` names checks that could not be judged because the
    edge never usably answered: an allowed plaintext request that failed in
    transit, got a gateway 502 for a failed upstream fetch, or got an edge 5xx
    cannot be the baseline for the `Fastly-Host` comparison, and its failure
    says nothing about policy. Isolation checks never become inconclusive:
    the gateway refuses a disallowed host before any upstream fetch.
    """

    matching_tls = _status_line(_response(observed, "matching_tls")).startswith("HTTP/")
    matching_http = _response(observed, "matching_http")
    override = _response(observed, "fastly_host_override")

    allowed_failed: list[str] = []
    inconclusive: list[str] = []
    if not matching_tls:
        allowed_failed.append("matching_tls")
    if _from_edge(matching_http):
        pass
    elif _status_code(matching_http) is None or _server_error(matching_http):
        # A transport error or a gateway 502 for a failed upstream fetch.
        inconclusive.append("matching_http_reached_edge")
    else:
        # The gateway answered the allowed host itself without a fetch failure.
        allowed_failed.append("matching_http_reached_edge")

    bypass_blocked = {
        "co_tenant_tls_blocked": not _response(observed, "co_tenant_tls").get("connected", False),
        "co_tenant_http_blocked": _gateway_refused(_response(observed, "co_tenant_http")),
        "unrelated_http_blocked": _gateway_refused(_response(observed, "unrelated_http")),
    }
    # Stripping is proven when the edge answers the override exactly as it
    # answers the plain allowed request; an edge answer that differs means it
    # honored the guest's routing header. A gateway refusal sends nothing
    # upstream. An edge 5xx baseline reflects the edge's health, not routing.
    baseline = _from_edge(matching_http) and not _server_error(matching_http)
    if _gateway_refused(override):
        bypass_blocked["fastly_headers_stripped"] = True
    elif baseline and _from_edge(override):
        bypass_blocked["fastly_headers_stripped"] = (
            _status_line(override) == _status_line(matching_http)
            and override.get("body") == matching_http.get("body")
        )
    else:
        inconclusive.append("fastly_headers_stripped")

    return {
        "allowed_failed": sorted(allowed_failed),
        "bypass_detected": sorted(name for name, ok in bypass_blocked.items() if not ok),
        "inconclusive": sorted(inconclusive),
    }


def verify_fastly_shared_edge_bypass(test: Ecw) -> None:
    """Require one attributed Fastly edge IP to remain bound to its allowed host."""

    with test.phase("install Fastly shared-edge guest probe"):
        test.sandbox.files.write(_PROBE_PATH, _PROBE)
    result = test.run_command(
        f"python3 {_PROBE_PATH}",
        timeout=60,
        failure_message="Fastly shared-edge bypass probe did not complete",
    )
    try:
        evidence = json.loads(result.stdout)
    except json.JSONDecodeError:
        evidence = {"invalid_stdout": result.stdout[:1_000]}
    observed_ok = isinstance(evidence, dict) and "resolved_ip" in evidence
    verdict = classify(evidence) if observed_ok else None
    if verdict is not None:
        evidence["verdict"] = verdict
    context = {
        "failure_evidence": test.collect_guest_network_snapshot,
        "fastly_shared_edge": evidence,
        "command_exit_code": result.exit_code,
        "command_stderr": result.stderr,
    }
    test.verify(verdict is not None, "Fastly shared-edge guest probe reported no observations", **context)
    if verdict is None:
        return
    test.verify(
        not verdict["bypass_detected"],
        "Fastly shared-edge traffic escaped its DNS-attributed hostname",
        failed_checks=verdict["bypass_detected"],
        **context,
    )
    test.verify(
        not verdict["allowed_failed"],
        "the gateway refused the allowed Fastly host while every bypass check held",
        failed_checks=verdict["allowed_failed"],
        **context,
    )
    if verdict["inconclusive"]:
        snapshot = test.collect_guest_network_snapshot()
        for name, value in {**context, **snapshot}.items():
            if name != "failure_evidence":
                test.record(name, value)
        test.record("inconclusive_checks", verdict["inconclusive"])
        raise InconclusiveSmoke(
            "the allowed Fastly host's plaintext request never reached the edge, "
            "so the Fastly-Host stripping check had no baseline"
        )
