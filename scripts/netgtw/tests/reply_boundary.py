#!/usr/bin/env python3
"""Exercise production nft rules with direct replies blocked and local ingress preserved.

Requires root, Linux namespaces, ip, nft, and the firewall-reply-fixture example.
No TPROXY listener runs. Guest-initiated and direct-DNAT-style replies must fail
closed; a local gateway-to-guest TCP connection must retain its unfragmented
reply path.
"""

import argparse
import json
import os
import select
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
GATEWAY = "sns-reply-test"
PAYLOAD = "arbitrary guest response, independent of the request"


def run(*args, timeout=10, **kwargs):
    result = subprocess.run(
        args, check=False, text=True, capture_output=True, timeout=timeout, **kwargs
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {args}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return result.stdout


def inside(namespace, *args, **kwargs):
    return run("ip", "netns", "exec", namespace, *args, **kwargs)


def ruleset():
    return json.loads(inside(GATEWAY, "nft", "-j", "list", "ruleset"))


def check_flow(source, destination, address, protocol, allowed):
    server = """
import socket, sys
protocol, address, payload = sys.argv[1:]
s = socket.socket(type=socket.SOCK_STREAM if protocol == 'tcp' else socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((address, 9000))
if protocol == 'tcp': s.listen(1)
s.settimeout(3)
print('ready', flush=True)
try:
    if protocol == 'tcp':
        c, _ = s.accept()
        c.settimeout(3)
        c.recv(128)
        c.sendall(payload.encode())
        c.close()
    else:
        _, peer = s.recvfrom(128)
        s.sendto(payload.encode(), peer)
    print('delivered', flush=True)
except TimeoutError:
    print('blocked', flush=True)
"""
    client = """
import socket, sys
protocol, address = sys.argv[1:]
s = socket.socket(type=socket.SOCK_STREAM if protocol == 'tcp' else socket.SOCK_DGRAM)
s.settimeout(1)
try:
    s.connect((address, 9000))
    s.sendall(b'request')
    reply = s.makefile("rb").read() if protocol == "tcp" else s.recv(1024)
    print(reply.decode())
except (TimeoutError, ConnectionError, OSError):
    print('blocked')
"""
    with subprocess.Popen(
        [
            "ip",
            "netns",
            "exec",
            destination,
            sys.executable,
            "-c",
            server,
            protocol,
            address,
            PAYLOAD,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            assert select.select([process.stdout], [], [], 5)[0], (
                "server did not become ready"
            )
            assert process.stdout.readline().strip() == "ready"
            received = inside(
                source, sys.executable, "-c", client, protocol, address
            ).strip()
            out, err = process.communicate(timeout=5)
            assert process.returncode == 0, err
            assert received == (PAYLOAD if allowed else "blocked"), received
            assert out.strip() == ("delivered" if allowed else "blocked"), out
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    print(
        f"{protocol} {source} -> {destination}: {'reply delivered' if allowed else 'blocked'}"
    )


def check_udp443_recovery(installer, policy):
    # Current policy, opposite stage, and mask-only drift in three idle slots.
    comment = "platform-egress-pre-tproxy-guardrail-quic-udp443"
    packaged = next(
        entry["add"]["rule"]
        for entry in json.loads(f"[{policy}]")
        if entry.get("add", {}).get("rule", {}).get("comment") == comment
    )
    rejects = any("reject" in expr for expr in packaged["expr"])
    expected_mask, stale_mask = 6168, 0
    run("ip", "netns", "add", "host")
    inside("host", "ip", "link", "add", "uplink", "type", "dummy")
    inside("host", "ip", "link", "set", "uplink", "up")
    inside("host", "ip", "route", "add", "default", "dev", "uplink")
    run(installer, "recover", timeout=25)
    inodes = [Path(f"/run/netns/sns-{slot:06d}").stat().st_ino for slot in range(3)]

    def udp443_rule(namespace):
        chain = json.loads(
            inside(
                namespace,
                "nft",
                "-j",
                "list",
                "chain",
                "ip",
                "sandbox",
                "pre_tproxy_guardrails",
            )
        )
        return next(
            entry["rule"]
            for entry in chain["nftables"]
            if entry.get("rule", {}).get("comment") == comment
        )

    for slot in range(3):
        namespace = f"sns-{slot:06d}"
        rule = udp443_rule(namespace)
        for expr in rule["expr"]:
            if "counter" in expr:
                expr["counter"] = {"packets": 7, "bytes": 280}
        if slot == 1:
            if rejects:
                rule["expr"] = [expr for expr in rule["expr"] if "reject" not in expr]
            else:
                rule["expr"].append(
                    {"reject": {"type": "icmp", "expr": "port-unreachable"}}
                )
        if slot in (1, 2):
            inside(namespace, "sysctl", "-qw", f"net.ipv4.icmp_ratemask={stale_mask}")
        inside(
            namespace,
            "nft",
            "-j",
            "-f",
            "-",
            input=json.dumps({"nftables": [{"replace": {"rule": rule}}]}),
        )

    # node-manager owns this SQLite schema; the recovery contract is asserted
    # from the namespaces it builds, not from its private transition rows.
    run(installer, "recover", timeout=25)
    for slot in range(3):
        namespace = f"sns-{slot:06d}"
        exprs = udp443_rule(namespace)["expr"]
        assert any("reject" in expr for expr in exprs) == rejects, exprs
        assert Path(f"/run/netns/{namespace}").stat().st_ino == inodes[slot]
        assert next(
            expr["counter"]["packets"] for expr in exprs if "counter" in expr
        ) == (7 if slot == 0 else 0), exprs
        assert (
            int(inside(namespace, "sysctl", "-n", "net.ipv4.icmp_ratemask"))
            == expected_mask
        )
    action, stale_action = ("reject", "observe") if rejects else ("observe", "reject")
    print(
        f"PASS: startup repairs {stale_action}/{stale_mask} and mask-only drift to "
        f"{action}/{expected_mask} in place; current slot unchanged"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("installer", type=Path)
    parser.add_argument("--parent-netns")
    parser.add_argument("--parent-mntns")
    args = parser.parse_args()
    installer = str(args.installer.resolve(strict=True))
    if os.geteuid() != 0:
        raise SystemExit("run with sudo; all mutations occur in disposable namespaces")
    if args.parent_netns is None:
        os.execvp(
            "timeout",
            [
                "timeout",
                "--kill-after=5s",
                "90s",
                "unshare",
                "--mount",
                "--net",
                "--pid",
                "--fork",
                "--kill-child=TERM",
                "--mount-proc",
                sys.executable,
                str(Path(__file__).resolve()),
                installer,
                "--parent-netns",
                os.readlink("/proc/self/ns/net"),
                "--parent-mntns",
                os.readlink("/proc/self/ns/mnt"),
            ],
        )
    if args.parent_netns == os.readlink("/proc/self/ns/net"):
        raise SystemExit("network namespace was not isolated")
    if not args.parent_mntns or args.parent_mntns == os.readlink("/proc/self/ns/mnt"):
        raise SystemExit("mount namespace was not isolated")
    policy = (
        ROOT / "docker/space/nftables/sandbox-pre-tproxy-guardrails.json"
    ).read_text()
    run("mount", "--make-rprivate", "/")
    for path in ("/run", "/etc"):
        run("mount", "-t", "tmpfs", "tmpfs", path)
    Path("/run/netns").mkdir()
    if not Path("/space").is_dir():
        raise SystemExit("missing /space mountpoint; use the SPACE CI image or devbox")
    run("mount", "-t", "tmpfs", "tmpfs", "/space")
    policy_path = Path("/etc/space/nftables/sandbox-pre-tproxy-guardrails.json")
    policy_path.parent.mkdir(parents=True)
    policy_path.write_text(policy)
    check_udp443_recovery(installer, policy)
    for namespace in (GATEWAY, "guest", "peer"):
        run("ip", "netns", "add", namespace)
        inside(namespace, "ip", "link", "set", "lo", "up")
    for namespace, gateway_link, endpoint_ip, gateway_ip in (
        ("guest", "tap0", "169.254.0.21/30", "169.254.0.22/30"),
        ("peer", "vpeer", "198.18.0.2/24", "198.18.0.1/24"),
    ):
        run(
            "ip",
            "link",
            "add",
            gateway_link,
            "netns",
            GATEWAY,
            "type",
            "veth",
            "peer",
            "name",
            "eth0",
            "netns",
            namespace,
        )
        for ns, link, address in (
            (GATEWAY, gateway_link, gateway_ip),
            (namespace, "eth0", endpoint_ip),
        ):
            inside(ns, "ip", "address", "add", address, "dev", link)
            inside(ns, "ip", "link", "set", link, "up")
            inside(
                ns,
                "sysctl",
                "-qw",
                "net.ipv4.conf.all.rp_filter=0",
                f"net.ipv4.conf.{link}.rp_filter=0",
            )
        inside(
            namespace, "ip", "route", "add", "default", "via", gateway_ip.split("/")[0]
        )
    inside(GATEWAY, "sysctl", "-qw", "net.ipv4.ip_forward=1")
    run(installer, "setup")
    run(installer, "validate")

    def udp443_packets():
        return sum(
            expr["counter"]["packets"]
            for entry in ruleset()["nftables"]
            if entry.get("rule", {}).get("comment")
            == "platform-egress-pre-tproxy-guardrail-quic-udp443"
            for expr in entry["rule"]["expr"]
            if "counter" in expr
        )

    before_packets = udp443_packets()
    with subprocess.Popen(
        [
            "ip",
            "netns",
            "exec",
            "peer",
            sys.executable,
            "-c",
            """
import select, socket, sys
with socket.socket(type=socket.SOCK_DGRAM) as server:
    server.bind(("198.18.0.2", 443))
    print("ready", flush=True)
    readable, _, _ = select.select([server, sys.stdin], [], [], 8)
    assert sys.stdin in readable and server not in readable, "UDP/443 reached upstream"
""",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as upstream:
        assert select.select([upstream.stdout], [], [], 5)[0], (
            "UDP/443 listener not ready"
        )
        assert upstream.stdout.readline().strip() == "ready"
        print(
            inside(
                "guest",
                sys.executable,
                "-c",
                """
import socket, time
refusals = 0
timeouts = 0
with socket.socket(type=socket.SOCK_DGRAM) as isolated:
    isolated.settimeout(0.5)
    isolated.connect(("198.18.0.2", 443))
    start = time.monotonic()
    isolated.send(b"quic-isolated")
    try:
        isolated.recv(128)
    except ConnectionRefusedError:
        print(f"UDP/443 isolated: ECONNREFUSED in {(time.monotonic() - start) * 1000:.2f} ms")
    else:
        raise AssertionError("isolated UDP/443 did not receive an explicit refusal")
for attempt in range(20):
    with socket.socket(type=socket.SOCK_DGRAM) as stream:
        stream.settimeout(0.1)
        stream.connect(("198.18.0.2", 443))
        stream.send(b"quic-burst")
        try:
            stream.recv(128)
        except ConnectionRefusedError:
            refusals += 1
        except TimeoutError:
            timeouts += 1
        else:
            raise AssertionError("UDP/443 was not blocked")
assert refusals + timeouts == 20
print(f"UDP/443 burst: {refusals} refusals, {timeouts} rate-limited replies")
""",
            ).strip()
        )
        upstream.stdin.write("done\n")
        upstream.stdin.flush()
        assert upstream.wait(timeout=5) == 0, upstream.stderr.read()
    assert udp443_packets() - before_packets == 21, (
        "not every UDP/443 packet was blocked"
    )
    audit = str(ROOT / "scripts/netgtw/audit-reply-rules.py")
    print(run(sys.executable, audit, GATEWAY).strip())
    before = ruleset()
    # Seed both legacy broad exceptions and duplicates, preserving live ingress rules.
    reply_matches = [
        {
            "match": {
                "left": {"meta": {"key": "iifname"}},
                "right": "tap0",
                "op": "==",
            }
        },
        {
            "match": {
                "left": {"ct": {"key": "state"}},
                "right": {"set": ["established", "related"]},
                "op": "in",
            }
        },
    ]
    legacy = []
    for table, chain, comment, verdict in (
        ("sandbox", "forward", "ct-inbound-replies", "accept"),
        ("proxy", "prerouting", "skip-tproxy-inbound-replies", "return"),
    ):
        rule = {
            "family": "ip",
            "table": table,
            "chain": chain,
            "comment": comment,
            "expr": reply_matches + [{verdict: None}],
        }
        legacy.extend([{"insert": {"rule": rule}}, {"insert": {"rule": rule}}])
    inside(GATEWAY, "nft", "-j", "-f", "-", input=json.dumps({"nftables": legacy}))
    audit_result = subprocess.run(
        [sys.executable, audit, GATEWAY],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert audit_result.returncode == 1 and "stale" in audit_result.stdout, audit_result
    run(installer, "reapply")
    print(run(sys.executable, audit, GATEWAY).strip())
    after = ruleset()

    def ingress(rules):
        return [
            entry
            for entry in rules["nftables"]
            if entry.get("rule", {}).get("comment", "").startswith("allow-")
        ]

    assert ingress(before) and ingress(before) == ingress(after), (
        "live ingress rules changed"
    )
    for protocol in ("tcp", "udp"):
        check_flow("guest", "peer", "198.18.0.2", protocol, False)
        check_flow("peer", "guest", "169.254.0.21", protocol, False)
    check_flow(GATEWAY, "guest", "169.254.0.21", "tcp", True)
    print(
        "PASS: legacy-rule upgrade, rollout audit, direct reply suppression, and local ingress"
    )


if __name__ == "__main__":
    main()
