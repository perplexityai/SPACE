"""Probe whether guest TCP segments carrying MPTCP options reach the host stack.

Runs inside a SPACE guest as root. It sends two TCP SYNs to the guest's default
gateway: a plain SYN, and the same SYN carrying an MPTCP MP_CAPABLE option
(TCP option 30). The host answers a plain SYN on any port, because the
transparent proxy accepts it or the namespace kernel resets it, so a reply is
the oracle for "this segment was processed". The MPTCP SYN must get no reply:
the sandbox namespace drops every guest segment carrying option 30 before its
TCP stack parses it.

Only the standard library is used so the probe runs on every guest template.
It prints one JSON object and exits 0 whenever both probes ran; the scenario
decides pass or fail from the reported replies.
"""

from __future__ import annotations

import json
import socket
import struct
import sys
import time

PROBE_PORT = 65000
REPLY_TIMEOUT_SECONDS = 2.0
SEGMENTS_PER_CASE = 2
# Kind 30, length 4, subtype MP_CAPABLE with version 1, flag H (HMAC-SHA256).
MP_CAPABLE_SYN = bytes((30, 4, 0x01, 0x01))
CASES = {"plain": b"", "mptcp": MP_CAPABLE_SYN}


def checksum(data: bytes) -> int:
    if len(data) % 2:
        data += b"\0"
    total = sum(struct.unpack(f"!{len(data) // 2}H", data))
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return (~total) & 0xFFFF


def syn_segment(src_ip: str, dst_ip: str, sport: int, options: bytes) -> bytes:
    offset = (20 + len(options)) // 4
    tcp = struct.pack(
        "!HHIIBBHHH", sport, PROBE_PORT, 0x5ACE0001, 0, offset << 4, 0x02, 1024, 0, 0
    )
    tcp += options
    pseudo = socket.inet_aton(src_ip) + socket.inet_aton(dst_ip)
    pseudo += struct.pack("!BBH", 0, socket.IPPROTO_TCP, len(tcp))
    return tcp[:16] + struct.pack("!H", checksum(pseudo + tcp)) + tcp[18:]


def default_gateway() -> str:
    with open("/proc/net/route", encoding="ascii") as route:
        next(route)
        for line in route:
            _, destination, gateway, *_ = line.split()
            if destination == "00000000":
                return socket.inet_ntoa(struct.pack("<I", int(gateway, 16)))
    raise SystemExit("guest has no IPv4 default route")


def local_ip_towards(gateway: str) -> str:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect((gateway, PROBE_PORT))
        return str(probe.getsockname()[0])
    finally:
        probe.close()


def classify_reply(packet: bytes, peer_ip: str, sport: int) -> str | None:
    """Name a TCP reply to our SYN, or None when the packet is unrelated."""

    ihl = (packet[0] & 0x0F) * 4
    if len(packet) < ihl + 14 or socket.inet_ntoa(packet[12:16]) != peer_ip:
        return None
    _, dport, _, _, _, flags = struct.unpack("!HHIIBB", packet[ihl : ihl + 14])
    if dport != sport:
        return None
    if flags & 0x04:
        return "rst"
    if flags & 0x12 == 0x12:
        return "syn-ack"
    return "other"


def probe(gateway: str, segment: bytes, sport: int) -> str:
    """Send `segment` and report the first TCP reply to it, or "none"."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    try:
        sock.settimeout(0.2)
        for _ in range(SEGMENTS_PER_CASE):
            sock.sendto(segment, (gateway, 0))
            time.sleep(0.1)
        deadline = time.monotonic() + REPLY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                received = sock.recv(65535)
            except TimeoutError:
                continue
            verdict = classify_reply(received, gateway, sport)
            if verdict is not None:
                return verdict
        return "none"
    finally:
        sock.close()


def main() -> None:
    gateway = default_gateway()
    our_ip = local_ip_towards(gateway)
    report: dict[str, object] = {"gateway": gateway, "guest_ip": our_ip}
    sport = 41000
    for name, options in CASES.items():
        sport += 1
        report[name] = probe(gateway, syn_segment(our_ip, gateway, sport, options), sport)
    json.dump(report, sys.stdout, sort_keys=True)


if __name__ == "__main__":
    main()
