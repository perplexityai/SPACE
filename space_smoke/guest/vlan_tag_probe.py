"""Probe whether VLAN-tagged guest frames reach the host network stack.

Runs inside a SPACE guest as root. It sends a TCP SYN to the guest's default
gateway three ways: untagged, inside two nested 802.1Q priority tags (VID 0),
and inside an 802.1ad outer tag with an 802.1Q inner tag. The host answers an
untagged SYN on any port, because the transparent proxy accepts it or the
namespace kernel resets it, so a reply is the oracle for "this frame was
processed". A tagged frame must get no reply: the sandbox tap drops VLAN frames
before layer-3 handling, because a guest's routed point-to-point link carries
no VLAN configuration.

Only the standard library is used so the probe runs on every guest template.
It prints one JSON object and exits 0 whenever every probe ran; the scenario
decides pass or fail from the reported replies.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import sys
import time

ETH_P_ALL = 0x0003
ETH_P_IP = 0x0800
ETH_P_8021Q = 0x8100
ETH_P_8021AD = 0x88A8
PACKET_OUTGOING = 4
PROBE_PORT = 65000
REPLY_TIMEOUT_SECONDS = 2.0
FRAMES_PER_CASE = 2

PRIORITY_TAG = (ETH_P_8021Q, 0)
CASES: dict[str, tuple[tuple[int, int], ...]] = {
    "untagged": (),
    "vlan0_nested": (PRIORITY_TAG, PRIORITY_TAG),
    "qinq": ((ETH_P_8021AD, 0), PRIORITY_TAG),
}


def checksum(data: bytes) -> int:
    if len(data) % 2:
        data += b"\0"
    total = sum(struct.unpack(f"!{len(data) // 2}H", data))
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return (~total) & 0xFFFF


def build_frame(
    dst_mac: bytes,
    src_mac: bytes,
    src_ip: str,
    dst_ip: str,
    sport: int,
    dport: int,
    tags: tuple[tuple[int, int], ...],
) -> bytes:
    """An Ethernet frame carrying a TCP SYN, with `tags` as inline VLAN headers."""

    link = dst_mac + src_mac
    for tpid, tci in tags:
        link += struct.pack("!HH", tpid, tci)
    link += struct.pack("!H", ETH_P_IP)

    src = socket.inet_aton(src_ip)
    dst = socket.inet_aton(dst_ip)
    tcp = struct.pack("!HHIIBBHHH", sport, dport, 0x5ACE0001, 0, 5 << 4, 0x02, 1024, 0, 0)
    pseudo = src + dst + struct.pack("!BBH", 0, socket.IPPROTO_TCP, len(tcp))
    tcp = tcp[:16] + struct.pack("!H", checksum(pseudo + tcp)) + tcp[18:]

    total_length = 20 + len(tcp)
    ip = struct.pack(
        "!BBHHHBBH4s4s", 0x45, 0, total_length, 0x5ACE, 0, 64, socket.IPPROTO_TCP, 0, src, dst
    )
    ip = ip[:10] + struct.pack("!H", checksum(ip)) + ip[12:]
    return link + ip + tcp


def classify_reply(frame: bytes, our_ip: str, peer_ip: str, sport: int) -> str | None:
    """Name a TCP reply to our SYN, or None when the frame is unrelated."""

    if len(frame) < 54 or frame[12:14] != struct.pack("!H", ETH_P_IP):
        return None
    ip = frame[14:]
    ihl = (ip[0] & 0x0F) * 4
    if ip[0] >> 4 != 4 or ip[9] != socket.IPPROTO_TCP or len(ip) < ihl + 20:
        return None
    if socket.inet_ntoa(ip[12:16]) != peer_ip or socket.inet_ntoa(ip[16:20]) != our_ip:
        return None
    tcp = ip[ihl:]
    _, dport, _, _, _, flags = struct.unpack("!HHIIBB", tcp[:14])
    if dport != sport:
        return None
    if flags & 0x04:
        return "rst"
    if flags & 0x12 == 0x12:
        return "syn-ack"
    return "other"


def default_route() -> tuple[str, str]:
    """Interface and gateway of the IPv4 default route from /proc/net/route."""

    with open("/proc/net/route", encoding="ascii") as route:
        next(route)
        for line in route:
            iface, destination, gateway, *_ = line.split()
            if destination == "00000000":
                return iface, socket.inet_ntoa(struct.pack("<I", int(gateway, 16)))
    raise SystemExit("guest has no IPv4 default route")


def interface_mac(iface: str) -> bytes:
    with open(f"/sys/class/net/{iface}/address", encoding="ascii") as address:
        return bytes.fromhex(address.read().strip().replace(":", ""))


def local_ip_towards(gateway: str) -> str:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect((gateway, PROBE_PORT))
        return str(probe.getsockname()[0])
    finally:
        probe.close()


def gateway_mac(gateway: str) -> bytes:
    """Resolve the gateway's MAC through the kernel's own ARP cache."""

    for _ in range(10):
        with open("/proc/net/arp", encoding="ascii") as arp:
            next(arp)
            for line in arp:
                ip, _, flags, mac, *_ = line.split()
                if ip == gateway and flags != "0x0" and mac != "00:00:00:00:00:00":
                    return bytes.fromhex(mac.replace(":", ""))
        trigger = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            trigger.sendto(b"arp", (gateway, PROBE_PORT))
        finally:
            trigger.close()
        time.sleep(0.2)
    raise SystemExit(f"could not resolve the MAC of gateway {gateway}")


def probe(
    iface: str,
    frame: bytes,
    our_ip: str,
    peer_ip: str,
    sport: int,
    timeout: float = REPLY_TIMEOUT_SECONDS,
) -> str:
    """Send `frame` and report the first TCP reply to it, or "none"."""

    sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(ETH_P_ALL))
    try:
        sock.bind((iface, 0))
        sock.settimeout(0.2)
        for _ in range(FRAMES_PER_CASE):
            sock.send(frame)
            time.sleep(0.1)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                received, address = sock.recvfrom(65535)
            except TimeoutError:
                continue
            if address[2] == PACKET_OUTGOING:
                continue
            verdict = classify_reply(received, our_ip, peer_ip, sport)
            if verdict is not None:
                return verdict
        return "none"
    finally:
        sock.close()


def main() -> None:
    iface, gateway = default_route()
    our_ip = local_ip_towards(gateway)
    src_mac = interface_mac(iface)
    dst_mac = gateway_mac(gateway)
    report: dict[str, object] = {
        "interface": iface,
        "gateway": gateway,
        "guest_ip": our_ip,
        "euid": os.geteuid(),
    }
    sport = 40000
    for name, tags in CASES.items():
        sport += 1
        frame = build_frame(dst_mac, src_mac, our_ip, gateway, sport, PROBE_PORT, tags)
        report[name] = probe(iface, frame, our_ip, gateway, sport)
    json.dump(report, sys.stdout, sort_keys=True)


if __name__ == "__main__":
    main()
