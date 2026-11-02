"""Traffic evidence for the ECW policy replacement and observation contract."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from typing import TYPE_CHECKING

from space_smoke.capabilities import NetworkPolicyConflict, NetworkPolicyObservation

if TYPE_CHECKING:
    from space_smoke.fixtures import Ecw


# Keep one real TLS socket and its resolved IPv4 address across the management
# update. A failed fresh DNS lookup alone would not prove warm-flow revocation.
_PROBE = r"""
import http.client
import json
import pathlib
import socket
import ssl
import time

root = pathlib.Path("/tmp/space-policy-update")
root.mkdir(exist_ok=True)
host = "example.com"
context = ssl._create_unverified_context()
ip = socket.gethostbyname(host)

def connect():
    raw = socket.create_connection((ip, 443), timeout=5)
    return context.wrap_socket(raw, server_hostname=host)

def request(sock, name=host):
    sock.sendall(f"GET / HTTP/1.1\r\nHost: {name}\r\nConnection: keep-alive\r\n\r\n".encode())
    response = http.client.HTTPResponse(sock)
    response.begin()
    body = response.read()
    return response.status == 200 and b"Example Domain" in body

sock = connect()
assert request(sock) and request(sock), "initial keep-alive responses did not arrive"
keep = context.wrap_socket(socket.create_connection(("example.org", 443), timeout=5), server_hostname="example.org")
assert request(keep, "example.org") and request(keep, "example.org")
(root / "ready").write_text(json.dumps({"ip": ip, "baseline": True}))
deadline = time.monotonic() + 100
while not (root / "go").exists():
    if time.monotonic() > deadline:
        raise TimeoutError("policy test never released the probe")
    time.sleep(0.1)

def reached(operation):
    try:
        return operation()
    except (OSError, http.client.HTTPException):
        return False

allowed_reached = reached(lambda: request(keep, "example.org"))
keep.close()
old_reached = reached(lambda: request(sock))
sock.close()
def fresh():
    with connect() as new:
        return request(new)
(root / "result").write_text(json.dumps({
    "allowed_established_reached": allowed_reached,
    "established_reached": old_reached,
    "warm_address_reached": reached(fresh),
}))
""".strip()


def _applied(
    test: Ecw, target: str, *, timeout: float = 60
) -> NetworkPolicyObservation:
    deadline = time.monotonic() + timeout
    while True:
        status = test.sandbox.network_policy_status()
        test.verify(
            status.desired_target == target,
            "a concurrent policy replaced the target",
            expected_target=target,
            observation=asdict(status),
        )
        test.verify(
            status.state != "failed",
            "gateway reported enforcement failure",
            observation=asdict(status),
        )
        if status.state == "applied" and status.applied_target == target:
            test.verify(
                bool(status.gateway_instance)
                and status.execution_generation is not None,
                "applied observation omitted its execution identity",
            )
            return status
        test.verify(
            time.monotonic() < deadline,
            "policy never reached live applied state",
            observation=asdict(status),
        )
        time.sleep(0.25)


def verify_live_update(test: Ecw) -> None:
    initial = test.sandbox.network_policy_status()
    test.verify(initial.desired_target is not None, "create omitted desired policy")
    assert initial.desired_target is not None
    _applied(test, initial.desired_target)
    test.network_policy.verify_egress(reachable=("https://example.org/",))
    test.sandbox.files.write("/tmp/space-policy-probe.py", _PROBE)
    probe = test.sandbox.start_background(
        "python3 /tmp/space-policy-probe.py", timeout=120
    )
    try:
        test.wait_until_command_matches(
            "test -f /tmp/space-policy-update/ready",
            stdout="",
            timeout=15,
            failure_message="TLS probe did not establish its initial connection",
        )
        ready = json.loads(test.sandbox.files.read("/tmp/space-policy-update/ready"))
        test.verify(
            ready["baseline"], "revoked destination was not reachable before update"
        )
        target = test.sandbox.replace_network_policy(
            revision=initial.revision,
            key="remove-one-host",
            allow=("example.org",),
        )
        applied = _applied(test, target)
        test.record("policy_applied", asdict(applied))
        test.sandbox.files.write("/tmp/space-policy-update/go", "go")
        result = probe.wait()
        test.verify(
            result.exit_code == 0, "warm-flow probe failed", stderr=result.stderr
        )
        evidence = json.loads(
            test.sandbox.files.read("/tmp/space-policy-update/result")
        )
        test.verify(
            evidence["allowed_established_reached"],
            "policy update closed an allowed established flow",
            **evidence,
        )
        test.verify(
            not evidence["established_reached"]
            and not evidence["warm_address_reached"],
            "revoked traffic continued after applied acknowledgment",
            **evidence,
        )
    finally:
        probe.kill()
    test.network_policy.verify_egress(
        reachable=("https://example.org/",),
        blocked=("https://example.com/",),
    )
    # Replays retain their target, while stale edits and changed payloads fail.
    test.verify(
        test.sandbox.replace_network_policy(
            revision=initial.revision,
            key="remove-one-host",
            allow=("example.org",),
        )
        == target,
        "retry changed the accepted target",
    )
    for key, allow in (("stale-edit", ("example.com",)), ("remove-one-host", ())):
        try:
            test.sandbox.replace_network_policy(
                revision=initial.revision, key=key, allow=allow
            )
        except NetworkPolicyConflict:
            pass
        else:
            test.verify(False, "conflicting update was accepted", key=key)
    # An empty allowlist is explicit deny, never inheritance or public access.
    denied = test.sandbox.replace_network_policy(
        revision=applied.revision, key="deny-all", allow=()
    )
    closed = _applied(test, denied)
    test.network_policy.verify_egress(
        blocked=("https://example.org/", "https://example.com/")
    )
    test.verify(
        test.sandbox.replace_network_policy(
            revision=initial.revision,
            key="remove-one-host",
            allow=("example.org",),
        )
        == target,
        "retry after a later edit changed its receipt",
    )
    _applied(test, denied)  # Replay must not restore its older policy.
    target = test.sandbox.replace_network_policy(
        revision=closed.revision, key="restore-one-host", allow=("example.org",)
    )
    before_suspend = _applied(test, target)
    test.suspend_durably()
    suspended = test.sandbox.network_policy_status()
    test.verify(
        suspended.state != "applied",
        "suspended execution retained an applied acknowledgment",
    )
    test.wake_from_durable_suspend()
    restored = _applied(test, target)
    test.verify(
        (restored.execution_generation, restored.gateway_instance)
        != (before_suspend.execution_generation, before_suspend.gateway_instance),
        "restore reused the previous execution acknowledgment",
    )
    test.network_policy.verify_egress(
        reachable=("https://example.org/",), blocked=("https://example.com/",)
    )
