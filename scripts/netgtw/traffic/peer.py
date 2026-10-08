#!/usr/bin/env python3
"""Local socket peers and persistent probe clients for the traffic harness.

Only the gateway evaluates policy. These peers echo bytes and answer A queries;
their append-only receipt log is an independent oracle for traffic leakage.
"""

import argparse
import asyncio
import ipaddress
import json
import os
import secrets
import socket
import ssl
import struct
import sys
from functools import partial
from pathlib import Path

RESOLVERS = {"1.1.1.1": "cloudflare-dns.com", "8.8.8.8": "dns.google"}


async def forward(reader, writer, upstream_reader, upstream_writer, record=None):
    """Copy opaque bytes until either side disconnects; cancel the other reader."""

    async def copy(source, destination, receipt=None):
        while data := await source.read(65536):
            if receipt:
                receipt(data)
            destination.write(data)
            await destination.drain()

    tasks = [
        asyncio.create_task(copy(reader, upstream_writer, record)),
        asyncio.create_task(copy(upstream_reader, writer)),
    ]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        upstream_writer.close()
        await upstream_writer.wait_closed()


async def relay(path):
    """Host-side Unix socket relay, restricted to the two reserved DNS endpoints."""

    async def connected(reader, writer):
        try:
            address = (await asyncio.wait_for(reader.readline(), 2)).decode().strip()
            if address not in RESOLVERS:
                raise ValueError(f"unsupported resolver: {address!r}")
            upstream = await asyncio.wait_for(asyncio.open_connection(address, 853), 5)
            await forward(reader, writer, *upstream)
        except (OSError, TimeoutError, ValueError) as error:
            print(f"DNS relay failed: {error}", file=sys.stderr, flush=True)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    server = await asyncio.start_unix_server(connected, path, limit=64)
    async with server:
        await server.serve_forever()


def query(name, transaction):
    labels = b"".join(
        bytes([len(label)]) + label.encode("ascii") for label in name.split(".")
    )
    return struct.pack("!6H", transaction, 0x100, 1, 0, 0, 0) + labels + b"\0\0\1\0\1"


def name_end(wire, offset):
    while wire[offset]:
        size = wire[offset]
        if size & 0xC0 == 0xC0:
            return offset + 2
        if size > 63:
            raise ValueError("invalid DNS label")
        offset += size + 1
    return offset + 1


def question(wire):
    offset = 12
    labels = []
    while wire[offset]:
        size = wire[offset]
        if size > 63:
            raise ValueError("fixture expects an uncompressed DNS question")
        labels.append(wire[offset + 1 : offset + 1 + size].decode("ascii"))
        offset += size + 1
    return ".".join(labels).lower(), offset + 5


def dns_result(wire, request):
    transaction, flags, questions, answers, _, _ = struct.unpack("!6H", wire[:12])
    if (
        transaction != struct.unpack("!H", request[:2])[0]
        or not flags & 0x8000
        or questions != 1
    ):
        raise ValueError("DNS response does not match the query")
    if question(wire)[0] != question(request)[0]:
        raise ValueError("DNS response has a different question")
    offset = name_end(wire, 12) + 4
    addresses = []
    for _ in range(answers):
        offset = name_end(wire, offset)
        kind, cls, _, size = struct.unpack("!HHIH", wire[offset : offset + 10])
        offset += 10
        if kind == 1 and cls == 1 and size == 4:
            addresses.append(str(ipaddress.ip_address(wire[offset : offset + size])))
        offset += size
    return {"rcode": flags & 15, "addresses": addresses}


async def serve(args):
    import protocols

    descriptor = os.open(args.events, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
    health_descriptor = os.open(
        args.health_events, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600
    )
    credential = (
        Path(args.credential_file).read_text() if args.credential_file else None
    )

    def record(kind, address, port, *, health=False, **fields):
        event = dict(zone=args.zone, kind=kind, target=address, port=port, **fields)
        os.write(
            health_descriptor if health else descriptor,
            (json.dumps(event) + "\n").encode(),
        )

    def answer(wire, address, port, receipt=record):
        name, end = question(wire)
        receipt("dns", address, port, host=name)
        header = wire[:2] + struct.pack("!5H", 0x8180, 1, 1, 0, 0)
        return (
            header
            + wire[12:end]
            + b"\xc0\x0c"
            + struct.pack("!HHIH", 1, 1, 60, 4)
            + socket.inet_aton("198.18.1.2")
        )

    contexts = {}
    for protocol, advertised in (
        ("h2", ["h2"]),
        ("proxy", ["h2", "http/1.1"]),
        # Put an unsupported protocol first so fallback probes also prove that
        # interception does not advertise guest protocols it cannot bridge.
        ("http/1.1", ["space-unsupported", "http/1.1"]),
        # Native gRPC requires h2 even when an endpoint prefers HTTP/1.1.
        ("grpc", ["http/1.1", "h2"]),
    ):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(args.certificate, args.key)
        context.set_alpn_protocols(advertised)
        contexts[protocol] = context
    grpc_service, grpc_port = (
        await protocols.grpc_server() if args.zone == "upstream" else (None, None)
    )

    async def connected(reader, writer):
        address, port = writer.get_extra_info("sockname")[:2]
        remote = writer.get_extra_info("peername")
        connection = f"{remote[0]}:{remote[1]}"
        # Direct health probes run in the destination's namespace and connect
        # from its own address. Gateway traffic arrives from another namespace.
        # Keep every receipt on a health connection separate, including control
        # frames received after the client reports success.
        record_connection = partial(record, health=remote[0] == address)
        record_connection("connect", address, port, connection=connection)

        def received(data):
            record_connection(
                "data",
                address,
                port,
                data=data.decode("ascii", errors="replace"),
                connection=connection,
            )

        def request_received(host, headers):
            headers = dict(headers)
            credential_matches = []
            if credential is not None:
                credential_matches = sorted(
                    name
                    for name in args.credential_header
                    if name in headers
                    and secrets.compare_digest(headers[name], credential)
                )
                for name in args.credential_header:
                    if name in headers:
                        headers[name] = "<redacted>"
            record_connection(
                "request",
                address,
                port,
                host=host,
                headers=headers,
                credential_matches=credential_matches,
                connection=connection,
            )

        try:
            if port == 853:
                upstream_reader, upstream_writer = await asyncio.open_unix_connection(
                    args.relay_socket
                )
                upstream_writer.write((address + "\n").encode())
                await upstream_writer.drain()
                await forward(
                    reader,
                    writer,
                    upstream_reader,
                    upstream_writer,
                    lambda data: record_connection(
                        "dot-data", address, port, bytes=len(data)
                    ),
                )
                return
            if port in protocols.TLS_PORTS:
                context = contexts[
                    "proxy"
                    if port == 8445
                    else "grpc"
                    if port == 50052
                    else "h2"
                    if port == 8443
                    else "http/1.1"
                ]
                await writer.start_tls(context, ssl_handshake_timeout=2)
                ssl_object = writer.get_extra_info("ssl_object")
                record_connection(
                    "tls-handshake",
                    address,
                    port,
                    alpn=ssl_object.selected_alpn_protocol(),
                    connection=connection,
                )
            if port in (80, 443):
                await protocols.http1_server(reader, writer, received, request_received)
                return
            if port == 8445:

                def connect_received(host):
                    record_connection(
                        "connect-request",
                        address,
                        port,
                        host=host,
                        connection=connection,
                    )

                await protocols.connect_server(
                    reader,
                    writer,
                    received,
                    request_received,
                    connect_received,
                    contexts["http/1.1"],
                )
                return
            if port in (8080, 8443):
                await protocols.http2_server(reader, writer, received, request_received)
                return
            if port in (8081, 8444):

                def upgrade_received(host):
                    record_connection(
                        "websocket-upgrade",
                        address,
                        port,
                        host=host,
                        connection=connection,
                    )

                await protocols.websocket_server(
                    reader, writer, received, upgrade_received
                )
                return
            if port in (50051, 50052):
                upstream = await asyncio.open_connection("127.0.0.1", grpc_port)
                await forward(reader, writer, *upstream, record=received)
                return
            while True:
                if port == 53:
                    size = struct.unpack("!H", await reader.readexactly(2))[0]
                    response = answer(
                        await reader.readexactly(size), address, port, record_connection
                    )
                    writer.write(struct.pack("!H", len(response)) + response)
                else:
                    data = await reader.read(65536)
                    if not data:
                        break
                    received(data)
                    writer.write(data)
                await writer.drain()
        except (OSError, TimeoutError, asyncio.IncompleteReadError):
            pass  # A denied or cancelled client may close during TLS or DNS framing.
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    class Datagram(asyncio.DatagramProtocol):
        def connection_made(self, transport):
            self.transport = transport

        def datagram_received(self, data, remote):
            address, port = self.transport.get_extra_info("sockname")[:2]
            if port == 53:
                data = answer(data, address, port)
            else:
                record(
                    "data",
                    address,
                    port,
                    data=data.decode("ascii", errors="replace"),
                    connection=f"{remote[0]}:{remote[1]}",
                )
            self.transport.sendto(data, remote)

    ports = (
        [9000]
        if args.zone == "guest"
        else [9000, 80, 443, 53, 853, 9443, 8080, 8443, 8081, 8444, 8445, 50051, 50052]
    )
    servers = [await asyncio.start_server(connected, "0.0.0.0", port) for port in ports]
    transports = []
    if args.zone != "guest":
        for address in args.address:
            for port in (443, 8443):
                transports.append(
                    await protocols.quic_server(
                        address, args.certificate, args.key, record, port
                    )
                )
            for port in (53, 9000):
                (
                    transport,
                    _,
                ) = await asyncio.get_running_loop().create_datagram_endpoint(
                    Datagram, local_addr=(address, port)
                )
                transports.append(transport)
    Path(args.ready).touch()
    try:
        await asyncio.Future()
    finally:
        for server in servers:
            server.close()
        for transport in transports:
            transport.close()
        if grpc_service:
            await grpc_service.stop(0)
        os.close(descriptor)
        os.close(health_descriptor)


def receive_exact(stream, size):
    result = bytearray()
    while len(result) < size:
        data = stream.recv(size - len(result))
        if not data:
            raise ConnectionResetError("peer closed before replying")
        result.extend(data)
    return bytes(result)


def probe(spec, sessions):
    kind, target, port = spec["kind"], spec["target"], spec["port"]
    nonce = spec["nonce"].encode()
    session = spec.get("session")
    if session and spec.get("connection") == "existing" and session not in sessions:
        raise ValueError(
            f"existing session {session!r} was never opened or already failed"
        )
    stream = None
    try:
        if kind in (
            "http",
            "tls",
            "http2",
            "h2c",
            "grpc",
            "grpcs",
            "ws",
            "wss",
            "quic",
            "connect",
        ):
            import protocols

            return protocols.probe(spec, sessions)
        dns_over_tls = spec.get("dns_over_tls", False)
        datagram = kind in ("udp", "dns-udp") and not dns_over_tls
        if session and session in sessions:
            stream = sessions[session]
        else:
            stream = socket.socket(
                socket.AF_INET, socket.SOCK_DGRAM if datagram else socket.SOCK_STREAM
            )
            stream.settimeout(spec.get("timeout", 0.7))
            stream.connect((target, 853 if dns_over_tls else port))
            if dns_over_tls:
                context = ssl.create_default_context()
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                stream = context.wrap_socket(stream, server_hostname=RESOLVERS[target])
            elif kind == "tls-echo":
                context = ssl.create_default_context(cafile=spec["certificate"])
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                stream = context.wrap_socket(stream, server_hostname=spec["host"])
            if session:
                sessions[session] = stream
        if kind.startswith("dns-"):
            request = query(spec["host"], secrets.randbelow(65536))
            stream.sendall(
                request if datagram else struct.pack("!H", len(request)) + request
            )
            response = (
                stream.recv(65536)
                if datagram
                else receive_exact(
                    stream, struct.unpack("!H", receive_exact(stream, 2))[0]
                )
            )
            return dict(received=True, **dns_result(response, request))
        if kind == "udp":
            # A sizes array adds multiple datagrams on the same flow; existing
            # cases retain their single nonce echo.
            blocked_udp443 = port == 443 and spec["expect"] == "deny"
            if blocked_udp443:
                stream.settimeout(0.1)
            errors = []
            for index, size in enumerate(spec.get("sizes", [len(nonce)])):
                data = (nonce + bytes(range(256)) * (size // 256 + 1))[:size]
                stream.sendall(data)
                try:
                    response = stream.recv(65536)
                except ConnectionRefusedError:
                    if blocked_udp443:
                        errors.append("ConnectionRefusedError")
                        continue
                    raise
                except TimeoutError:
                    if blocked_udp443:
                        errors.append("TimeoutError")
                        continue
                    return {
                        "received": False,
                        "error": f"UDP datagram {index + 1} ({size} bytes) timed out",
                        "error_type": "TimeoutError",
                        "datagrams_received": index,
                        "timeout_datagram": index + 1,
                    }
                if blocked_udp443:
                    raise ValueError("denied UDP/443 received a reply")
                if response != data:
                    raise ValueError("UDP datagram was corrupted or truncated")
            if errors:
                return {
                    "received": False,
                    "errors": errors,
                    "datagrams_sent": len(spec.get("sizes", [len(nonce)])),
                }
            return {"received": True, "protocol": "udp"}
        data = nonce
        stream.sendall(data)
        response = stream.recv(65536)
        if not response:
            raise ConnectionResetError("peer closed without a reply")
        if not datagram:
            response += receive_exact(stream, max(0, len(data) - len(response)))
        if nonce not in response:
            raise ValueError(f"peer returned the wrong payload: {response!r}")
        return {"received": True}
    except ssl.SSLCertVerificationError:
        raise  # Broken fixture trust must never count as a policy denial.
    except (OSError, TimeoutError) as error:
        if session:
            sessions.pop(session, None)
        if stream:
            stream.close()
        return {
            "received": False,
            "error": str(error),
            "error_type": type(error).__name__,
            "errno": error.errno,
        }
    finally:
        if stream and not session:
            stream.close()


def probe_server():
    sessions = {}
    for line in sys.stdin:
        spec = json.loads(line)
        if spec.get("reset"):
            for stream in sessions.values():
                stream.close()
            sessions.clear()
            result = {"reset": True}
        else:
            result = probe(spec, sessions)
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("probe-server")
    server = commands.add_parser("serve")
    for name in ("zone", "events", "health-events", "certificate", "key", "ready"):
        server.add_argument(f"--{name}", required=True)
    server.add_argument("--address", action="append", default=[])
    server.add_argument("--relay-socket", required=True)
    server.add_argument("--credential-file")
    server.add_argument("--credential-header", action="append", default=[])
    relay_parser = commands.add_parser("relay")
    relay_parser.add_argument("--socket", required=True)
    args = parser.parse_args()
    if args.command == "serve":
        asyncio.run(serve(args))
    elif args.command == "relay":
        asyncio.run(relay(args.socket))
    else:
        probe_server()
