"""Real application protocols for the isolated traffic peers.

These endpoints have no policy knowledge. Clients validate complete replies;
the runner separately checks receipts from the actual destination socket.
"""

import asyncio
import base64
import socket
import ssl
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

import grpc
import h2.config
import h2.connection
import h2.events
import h11
import wsproto
import wsproto.events as ws
from aioquic.asyncio import QuicConnectionProtocol, connect
from aioquic.asyncio.server import QuicServer
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, HandshakeCompleted
from google.protobuf.wrappers_pb2 import BytesValue

TLS_PORTS = {443, 8443, 50052, 8444, 8445, 9443}
QUIC_ALPN = "space-traffic-echo"


def payload(nonce, index, size):
    # Distinct binary payloads reveal truncation, corruption and stream mixups.
    prefix = f"{nonce}/{index}:".encode()
    return (prefix + bytes(range(256)) * (size // 256 + 1))[:size]


def check(condition, message):
    if not condition:
        raise ValueError(message)


def authority(spec):
    host = spec.get("authority", spec["host"])
    default = (
        host
        if spec["kind"] == "ws" or spec["port"] in (80, 443)
        else f"{host}:{spec['port']}"
    )
    return default


async def http1_server(reader, writer, record, request_received):
    connection = h11.Connection(h11.SERVER)
    body = bytearray()
    while True:
        event = connection.next_event()
        if event is h11.NEED_DATA:
            connection.receive_data(await reader.read(65536))
        elif isinstance(event, h11.ConnectionClosed):
            return
        elif isinstance(event, h11.Request):
            headers = {name.decode(): value.decode() for name, value in event.headers}
            request_received(headers.get("host", ""), headers)
            if connection.they_are_waiting_for_100_continue:
                writer.write(
                    connection.send(
                        h11.InformationalResponse(status_code=100, headers=[])
                    )
                )
                await writer.drain()
        elif isinstance(event, h11.Data):
            body.extend(event.data)
        elif isinstance(event, h11.EndOfMessage):
            record(bytes(body))
            writer.write(
                connection.send(
                    h11.Response(
                        status_code=200,
                        headers=[(b"content-length", str(len(body)).encode())],
                    )
                )
            )
            writer.write(connection.send(h11.Data(data=body)))
            writer.write(connection.send(h11.EndOfMessage()))
            await writer.drain()
            if connection.our_state is h11.MUST_CLOSE:
                return
            connection.start_next_cycle()
            body.clear()


async def connect_server(
    reader, writer, record, request_received, connect_received, context
):
    """HTTPS proxy fixture: HTTPS proxy auth, CONNECT, then another TLS session."""
    connection = h11.Connection(h11.SERVER)
    while True:
        event = connection.next_event()
        if event is h11.NEED_DATA:
            connection.receive_data(await reader.read(65536))
        elif isinstance(event, h11.Request):
            headers = dict(event.headers)
            check(event.method == b"CONNECT", "proxy expected CONNECT")
            token = b"Basic " + base64.b64encode(b"traffic:proxy-token")
            authorized = headers.get(b"proxy-authorization") == token
            target = (
                event.target
                in (
                    b"inner.test:443",
                    b"forbidden.test:443",
                    b"inner.127-0-0-1.dev:443",
                    b"forbidden.127-0-0-1.dev:443",
                )
                and headers.get(b"host") == event.target
            )
            status = 200 if authorized and target else 407 if not authorized else 403
            connect_received(event.target.decode())
            headers = [] if status == 200 else [("content-length", "0")]
            if status == 407:
                headers += [
                    ("proxy-authenticate", 'SpaceUnsupported realm="fixture"'),
                    ("proxy-authenticate", 'Basic realm="fixture"'),
                ]
            writer.write(
                connection.send(h11.Response(status_code=status, headers=headers))
            )
            if status != 200:
                writer.write(connection.send(h11.EndOfMessage()))
            await writer.drain()
            if status == 407:
                continue
            if status != 200:
                return
            # asyncio and curl both perform actual TLS-in-TLS here. The inner
            # identity has separate verification from the proxy-facing trust.
            await writer.start_tls(context, ssl_handshake_timeout=2)
            await http1_server(reader, writer, record, request_received)
            return
        elif isinstance(event, h11.EndOfMessage):
            connection.start_next_cycle()
        elif isinstance(event, h11.ConnectionClosed):
            return


async def connect_alpn_probe(spec, body):
    """Reproduce Go's h2 proxy ALPN followed by an HTTP/1.1 CONNECT."""
    outer = ssl.create_default_context(cafile=spec["certificate"])
    outer.set_alpn_protocols(spec["alpn"])
    inner = ssl.create_default_context(cafile=spec["tunnel_certificate"])
    inner.set_alpn_protocols(["http/1.1"])
    host = spec.get("tunnel_host", "inner.test")
    async with asyncio.timeout(spec["timeout"]):
        reader, writer = await asyncio.open_connection(
            spec["target"], spec["port"], ssl=outer, server_hostname=spec["host"]
        )
        try:
            negotiated = writer.get_extra_info("ssl_object").selected_alpn_protocol()
            token = base64.b64encode(b"traffic:proxy-token").decode()
            writer.write(
                (
                    f"CONNECT {host}:443 HTTP/1.1\r\nHost: {host}:443\r\n"
                    f"Proxy-Authorization: Basic {token}\r\n\r\n"
                ).encode()
            )
            await writer.drain()
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
            except asyncio.IncompleteReadError as error:
                raise ConnectionError(
                    "proxy closed before completing CONNECT response"
                ) from error
            check(
                headers.split(b"\r\n", 1)[0].split()[:2] == [b"HTTP/1.1", b"200"],
                "proxy did not accept HTTP/1.1 CONNECT",
            )
            await writer.start_tls(inner, server_hostname=host)
            connection = h11.Connection(h11.CLIENT)
            writer.write(
                connection.send(
                    h11.Request(
                        method="POST",
                        target="/echo",
                        headers=[("host", host), ("content-length", str(len(body)))],
                    )
                )
            )
            writer.write(connection.send(h11.Data(data=body)))
            writer.write(connection.send(h11.EndOfMessage()))
            await writer.drain()
            status, received = None, bytearray()
            while True:
                event = connection.next_event()
                if event is h11.NEED_DATA:
                    connection.receive_data(await reader.read(65536))
                elif isinstance(event, h11.Response):
                    status = event.status_code
                elif isinstance(event, h11.Data):
                    received.extend(event.data)
                    check(
                        len(received) <= len(body), "CONNECT echo exceeded request size"
                    )
                elif isinstance(event, h11.EndOfMessage):
                    break
                elif isinstance(event, h11.ConnectionClosed):
                    raise ValueError("CONNECT peer closed before replying")
            check(
                status == 200 and received == body,
                "CONNECT inner HTTPS reply was corrupted",
            )
            return {
                "received": True,
                "protocol": "https-connect",
                "downstream_alpn": negotiated,
            }
        finally:
            writer.close()
            await writer.wait_closed()


def connect_probe(spec):
    body = payload(spec["nonce"], 0, 96 * 1024)
    if "alpn" in spec:
        return asyncio.run(connect_alpn_probe(spec, body))
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--max-time",
        str(spec["timeout"]),
        "--noproxy",
        "",
        "--proxy",
        f"https://{spec['host']}:{spec['port']}",
        "--resolve",
        f"{spec['host']}:{spec['port']}:{spec['target']}",
        "--proxy-user",
        "traffic:proxy-token",
        "--proxy-anyauth" if spec.get("proxy_anyauth") else "--proxy-basic",
        "--proxy-cacert",
        spec["certificate"],
        "--cacert",
        spec["tunnel_certificate"],
        "--write-out",
        "\n%{http_code}",
        "--data-binary",
        "@-",
        f"https://{spec.get('tunnel_host', 'inner.test')}/echo",
    ]
    if "tunnel_authority" in spec:
        command += ["--header", f"Host: {spec['tunnel_authority']}"]
    if "tunnel_method" in spec:
        # This method override needs HTTP/1 CONNECT framing.
        command += ["--http1.1", "--request", spec["tunnel_method"]]
    with tempfile.TemporaryDirectory(prefix="connect-headers-") as directory:
        headers_path = Path(directory) / "headers"
        result = subprocess.run(
            [*command, "--dump-header", str(headers_path)],
            input=body,
            capture_output=True,
            timeout=spec["timeout"] + 1,
            check=False,
        )
        if spec.get("proxy_anyauth"):
            challenges = [
                line.split(":", 1)[1].strip()
                for line in headers_path.read_text().splitlines()
                if line.lower().startswith("proxy-authenticate:")
            ]
            check(
                challenges
                == ['SpaceUnsupported realm="fixture"', 'Basic realm="fixture"'],
                f"proxy authentication challenges changed: {challenges}",
            )
    if (
        result.returncode == 56
        and b"CONNECT tunnel failed, response 403" in result.stderr
    ):
        return {"received": False, "http_status": 403, "phase": "connect"}
    if result.returncode in (35, 56):
        # Outer policy denial can close TLS before CONNECT. The receipt oracle
        # still requires no peer activity; certificate verification errors (60)
        # remain fixture failures.
        raise ConnectionError(result.stderr.decode())
    check(result.returncode == 0, f"HTTPS CONNECT failed: {result.stderr.decode()}")
    response, status = result.stdout.rsplit(b"\n", 1)
    if status == b"403":
        return {"received": False, "http_status": 403, "phase": "inner_http"}
    check(
        status == b"200" and response == body, "CONNECT inner HTTPS reply was corrupted"
    )
    return {"received": True, "protocol": "https-connect"}


def http1_probe(stream, spec):
    # Reuse the connection across fixed-length and chunked request bodies.
    connection = h11.Connection(h11.CLIENT)
    for index, size in enumerate((128, 96 * 1024)):
        switched = "switch_authority" in spec and index == 1
        nonce = spec["nonce"] + ("-forbidden" if switched else "")
        body = payload(nonce, index, size)
        request_authority = (
            authority(dict(spec, authority=spec["switch_authority"]))
            if switched
            else authority(spec)
        )
        headers = [
            ("host", request_authority),
            *spec.get("preserve_headers", {}).items(),
        ]
        headers.append(
            ("content-length", str(len(body)))
            if index == 0
            else ("transfer-encoding", "chunked")
        )
        if switched:
            # Read a header-level refusal before uploading. Writing after an
            # early TLS close can discard the response in the client's SSL state.
            headers.append(("expect", "100-continue"))
        stream.sendall(
            connection.send(h11.Request(method="POST", target="/echo", headers=headers))
        )

        def upload(body=body):
            for start in range(0, len(body), 4096):
                stream.sendall(
                    connection.send(h11.Data(data=body[start : start + 4096]))
                )
            stream.sendall(connection.send(h11.EndOfMessage()))

        uploaded = not switched
        if uploaded:
            try:
                upload()
            except BrokenPipeError:
                # A first-request refusal can race the small initial upload.
                # Its HTTP response is still required below.
                pass
        status, received = None, bytearray()
        while True:
            event = connection.next_event()
            if event is h11.NEED_DATA:
                connection.receive_data(stream.recv(65536))
            elif isinstance(event, h11.InformationalResponse):
                if event.status_code == 100 and not uploaded:
                    uploaded = True
                    upload()
            elif isinstance(event, h11.Response):
                check(event.http_version == b"1.1", "expected HTTP/1.1 response")
                status = event.status_code
            elif isinstance(event, h11.Data):
                received.extend(event.data)
                check(
                    len(received) <= len(body), "HTTP/1.1 reply exceeded request size"
                )
            elif isinstance(event, h11.EndOfMessage):
                break
            elif isinstance(event, h11.ConnectionClosed):
                raise ValueError("HTTP/1.1 peer closed before replying")
        if status == 403:
            if switched:
                check(
                    spec["switch_expect"] == "deny",
                    "baseline unexpectedly denied authority switch",
                )
                return {
                    "received": True,
                    "protocol": "http/1.1",
                    "authority_switch_denied": True,
                }
            return {"received": False, "http_status": 403}
        if switched:
            check(
                spec["switch_expect"] == "allow",
                "forbidden keep-alive request reached origin",
            )
        check(status == 200, f"expected HTTP/1.1 200, got {status}")
        check(uploaded and received == body, "HTTP/1.1 echo was corrupted or truncated")
        connection.start_next_cycle()
    return {"received": True, "protocol": "http/1.1"}


def h2_connection(client):
    connection = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=client, header_encoding="utf-8")
    )
    connection.initiate_connection()
    return connection


def h2_flush(connection, pending):
    for stream_id, body in list(pending.items()):
        while body:
            size = min(
                len(body),
                connection.local_flow_control_window(stream_id),
                connection.max_outbound_frame_size,
            )
            if not size:
                break
            connection.send_data(
                stream_id, bytes(body[:size]), end_stream=size == len(body)
            )
            del body[:size]
        if not body:
            del pending[stream_id]
    return connection.data_to_send()


async def http2_server(reader, writer, record, request_received):
    connection = h2_connection(False)
    bodies, pending = defaultdict(bytearray), {}
    writer.write(connection.data_to_send())
    await writer.drain()
    while data := await reader.read(65536):
        for event in connection.receive_data(data):
            if isinstance(event, h2.events.RequestReceived):
                headers = dict(event.headers)
                request_received(headers.get(":authority", ""), headers)
            elif isinstance(event, h2.events.DataReceived):
                bodies[event.stream_id].extend(event.data)
                connection.acknowledge_received_data(
                    event.flow_controlled_length, event.stream_id
                )
            elif isinstance(event, h2.events.StreamEnded):
                body = bodies.pop(event.stream_id)
                record(bytes(body))
                connection.send_headers(
                    event.stream_id,
                    [(":status", "200"), ("content-length", str(len(body)))],
                )
                pending[event.stream_id] = body
            elif isinstance(event, h2.events.ConnectionTerminated):
                return
        writer.write(h2_flush(connection, pending))
        await writer.drain()


def http2_probe(stream, spec, connection=None, keep_open=False):
    connection = connection or h2_connection(True)
    first = connection.get_next_available_stream_id()
    switched_stream = first + 2
    # All streams are open before reading replies. Bodies exceed the initial
    # stream AND connection windows, exercising multiplexing and WINDOW_UPDATE.
    bodies = {
        stream_id: payload(
            spec["nonce"]
            + (
                "-forbidden"
                if "switch_authority" in spec and stream_id == switched_stream
                else ""
            ),
            stream_id,
            96 * 1024,
        )
        for stream_id in (first, first + 2, first + 4)
    }
    for stream_id in bodies:
        connection.send_headers(
            stream_id,
            [
                (":method", "POST"),
                (":scheme", "http" if spec["kind"] == "h2c" else "https"),
                (
                    ":authority",
                    authority(dict(spec, authority=spec["switch_authority"]))
                    if "switch_authority" in spec and stream_id == switched_stream
                    else authority(spec),
                ),
                (":path", "/echo"),
                *spec.get("preserve_headers", {}).items(),
            ],
        )
    pending = {stream_id: bytearray(body) for stream_id, body in bodies.items()}
    responses, statuses, ended = defaultdict(bytearray), {}, set()
    stream.sendall(h2_flush(connection, pending))
    while ended != bodies.keys():
        data = stream.recv(65536)
        if not data:
            raise ConnectionResetError("HTTP/2 peer closed before END_STREAM")
        for event in connection.receive_data(data):
            if isinstance(event, h2.events.ResponseReceived):
                statuses[event.stream_id] = dict(event.headers).get(":status")
                if statuses[event.stream_id] == "403":
                    pending.pop(event.stream_id, None)
            elif isinstance(event, h2.events.DataReceived):
                responses[event.stream_id].extend(event.data)
                connection.acknowledge_received_data(
                    event.flow_controlled_length, event.stream_id
                )
            elif isinstance(event, h2.events.StreamEnded):
                ended.add(event.stream_id)
            elif isinstance(
                event, (h2.events.StreamReset, h2.events.ConnectionTerminated)
            ):
                # The event reports a remote protocol failure.
                raise ValueError(f"HTTP/2 stream failed: {event}")  # noqa: TRY004
        stream.sendall(h2_flush(connection, pending))
    if all(statuses.get(sid) == "403" and not responses[sid] for sid in bodies):
        return {"received": False, "http_status": 403}
    check(
        all(
            (statuses.get(sid) == "403" and not responses[sid])
            if "switch_authority" in spec
            and sid == switched_stream
            and spec["switch_expect"] == "deny"
            else (statuses.get(sid) == "200" and responses[sid] == body)
            for sid, body in bodies.items()
        ),
        "HTTP/2 status or multiplexed payload mismatch",
    )
    if not keep_open:
        connection.close_connection()
        stream.sendall(connection.data_to_send())
    return {"received": True, "protocol": "h2"}


async def websocket_server(reader, writer, record, upgrade_received):
    connection = wsproto.WSConnection(wsproto.ConnectionType.SERVER)
    while data := await reader.read(65536):
        connection.receive_data(data)
        for event in connection.events():
            if isinstance(event, ws.Request):
                upgrade_received(event.host)
                reply = ws.AcceptConnection()
            elif isinstance(event, (ws.TextMessage, ws.BytesMessage)):
                record(
                    event.data.encode() if isinstance(event.data, str) else event.data
                )
                reply = event
            elif isinstance(event, (ws.Ping, ws.CloseConnection)):
                reply = event.response()
            else:
                continue
            writer.write(connection.send(reply))
            await writer.drain()
            if isinstance(event, ws.CloseConnection):
                return


def websocket_probe(stream, spec, connection=None, keep_open=False):
    connection = connection or wsproto.WSConnection(wsproto.ConnectionType.CLIENT)
    binary = payload(spec["nonce"], 1, 96 * 1024)

    def send_messages():
        for frame in (
            ws.TextMessage(data=spec["nonce"]),
            ws.BytesMessage(data=binary[:7], message_finished=False),
            ws.BytesMessage(data=binary[7:]),
            ws.Ping(payload=b"traffic-ping"),
        ):
            stream.sendall(connection.send(frame))

    if connection.state == wsproto.ConnectionState.OPEN:
        send_messages()
    else:
        stream.sendall(
            connection.send(ws.Request(host=authority(spec), target="/echo"))
        )
    text, received_binary = "", bytearray()
    messages, pong, closing = 0, False, False
    while True:
        data = stream.recv(65536)
        if not data:
            raise ConnectionResetError(
                "WebSocket peer closed before the close handshake"
            )
        connection.receive_data(data)
        for event in connection.events():
            if isinstance(event, ws.AcceptConnection):
                send_messages()
            elif isinstance(event, ws.TextMessage):
                text += event.data
                messages += event.message_finished
            elif isinstance(event, ws.BytesMessage):
                received_binary.extend(event.data)
                messages += event.message_finished
            elif isinstance(event, ws.Pong):
                check(
                    event.payload == b"traffic-ping", "WebSocket pong payload mismatch"
                )
                pong = True
            elif isinstance(event, ws.RejectConnection):
                if event.status_code in (403, 502):
                    return {"received": False, "http_status": event.status_code}
                raise ValueError(f"WebSocket upgrade rejected: {event.status_code}")
            elif isinstance(event, ws.CloseConnection):
                check(closing and event.code == 1000, "unexpected WebSocket close")
                return {"received": True, "protocol": "websocket"}
        if messages == 2 and pong and not closing:
            check(
                text == spec["nonce"] and received_binary == binary,
                "WebSocket text/binary echo mismatch",
            )
            if keep_open:
                return {"received": True, "protocol": "websocket"}
            stream.sendall(connection.send(ws.CloseConnection(code=1000)))
            closing = True


async def grpc_server():
    async def unary(request, context):
        context.set_trailing_metadata((("echo-bin", b"\x00\xff\x80"),))
        return request

    async def streaming(requests, context):
        context.set_trailing_metadata((("echo-bin", b"\x00\xff\x80"),))
        async for request in requests:
            yield request

    async def collect(requests, context):
        return BytesValue(value=b"".join([request.value async for request in requests]))

    async def expand(request, context):
        for index in range(3):
            yield BytesValue(value=request.value + bytes([index]))

    async def error(request, context):
        context.set_trailing_metadata((("echo-bin", b"\x00\xff\x80"),))
        await context.abort(
            grpc.StatusCode.PERMISSION_DENIED, "fixture trailers-only error"
        )

    async def blocked(request, context):
        # A message proves the RPC reached the origin before cancellation.
        # The runtime cancels this await when RST_STREAM arrives.
        yield BytesValue(value=b"started")
        await asyncio.Future()

    codecs = {
        "request_deserializer": BytesValue.FromString,
        "response_serializer": BytesValue.SerializeToString,
    }
    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                "traffic.Echo",
                {
                    "Unary": grpc.unary_unary_rpc_method_handler(unary, **codecs),
                    "Bidi": grpc.stream_stream_rpc_method_handler(streaming, **codecs),
                    "Collect": grpc.stream_unary_rpc_method_handler(collect, **codecs),
                    "Expand": grpc.unary_stream_rpc_method_handler(expand, **codecs),
                    "Error": grpc.unary_unary_rpc_method_handler(error, **codecs),
                    "Blocked": grpc.unary_stream_rpc_method_handler(blocked, **codecs),
                },
            ),
        )
    )
    # The externally reachable socket is owned by peer.py so every TCP accept
    # is recorded, including failed handshakes before gRPC sees an RPC.
    port = server.add_insecure_port("127.0.0.1:0")
    check(port, "could not bind the gRPC peer")
    await server.start()
    return server, port


def grpc_probe(spec):
    address = f"{spec['target']}:{spec['port']}"
    options = (
        ("grpc.enable_http_proxy", 0),
        ("grpc.ssl_target_name_override", spec["host"]),
        ("grpc.default_authority", authority(spec)),
    )
    channel = (
        grpc.secure_channel(
            address,
            grpc.ssl_channel_credentials(
                root_certificates=Path(spec["certificate"]).read_bytes()
            ),
            options,
        )
        if spec["kind"] == "grpcs"
        else grpc.insecure_channel(address, options)
    )
    codecs = {
        "request_serializer": BytesValue.SerializeToString,
        "response_deserializer": BytesValue.FromString,
    }
    bodies = [
        payload(spec["nonce"], index, size)
        for index, size in enumerate((128, 96 * 1024, 257))
    ]
    try:
        with channel:
            unary = channel.unary_unary("/traffic.Echo/Unary", **codecs)
            check(
                unary(BytesValue(value=bodies[0]), timeout=spec["timeout"]).value
                == bodies[0],
                "gRPC unary echo mismatch",
            )
            streaming = channel.stream_stream("/traffic.Echo/Bidi", **codecs)
            replies = streaming(
                (BytesValue(value=body) for body in bodies), timeout=spec["timeout"]
            )
            check(
                [reply.value for reply in replies] == bodies,
                "gRPC streaming echo mismatch",
            )
            check(
                replies.code() == grpc.StatusCode.OK, "gRPC trailers did not report OK"
            )
            check(
                dict(replies.trailing_metadata())["echo-bin"] == b"\x00\xff\x80",
                "binary trailers changed",
            )
            collect = channel.stream_unary("/traffic.Echo/Collect", **codecs)
            check(
                collect(
                    (BytesValue(value=body) for body in bodies), timeout=spec["timeout"]
                ).value
                == b"".join(bodies),
                "client streaming mismatch",
            )
            expand = channel.unary_stream("/traffic.Echo/Expand", **codecs)
            check(
                [
                    reply.value
                    for reply in expand(
                        BytesValue(value=bodies[0]), timeout=spec["timeout"]
                    )
                ]
                == [bodies[0] + bytes([index]) for index in range(3)],
                "server streaming mismatch",
            )
            try:
                channel.unary_unary("/traffic.Echo/Error", **codecs)(
                    BytesValue(value=bodies[0]), timeout=spec["timeout"]
                )
                raise ValueError("missing gRPC error")
            except grpc.RpcError as error:
                check(
                    error.code() == grpc.StatusCode.PERMISSION_DENIED
                    and dict(error.trailing_metadata())["echo-bin"] == b"\x00\xff\x80",
                    "trailers-only error changed",
                )
            blocked = channel.unary_stream("/traffic.Echo/Blocked", **codecs)
            pending = blocked(BytesValue(value=bodies[0]), timeout=spec["timeout"])
            check(next(pending).value == b"started", "blocked RPC never reached origin")
            check(pending.cancel(), "could not cancel gRPC stream")
            try:
                list(blocked(BytesValue(value=bodies[0]), timeout=0.2))
                raise ValueError("gRPC deadline did not expire")
            except grpc.RpcError as error:
                check(
                    error.code() == grpc.StatusCode.DEADLINE_EXCEEDED,
                    "gRPC deadline status changed",
                )
            check(
                unary(BytesValue(value=bodies[0]), timeout=spec["timeout"]).value
                == bodies[0],
                "cancelled stream damaged another RPC",
            )
        return {"received": True, "protocol": "grpc"}
    except grpc.RpcError as error:
        # Only connection/deadline failures can be policy denials. Application
        # statuses and malformed replies must fail even in a negative case.
        if error.code() not in (
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
        ):
            raise ValueError(f"unexpected gRPC status: {error}") from error
        return {
            "received": False,
            "error": error.details(),
            "grpc_code": error.code().name,
        }


async def quic_server(address, certificate, key, record):
    class ReceiptServer(QuicServer):
        def datagram_received(self, data, remote):
            # Log before parsing: even a leaked Initial which never establishes
            # TLS must fail a deny probe. Decrypted stream bytes are logged below.
            record("datagram", address, 443, bytes=len(data))
            # Preserve the existing raw UDP/443 probes on this same endpoint.
            # QUIC Initial packets have a binary long header, never this prefix.
            if data.startswith((b"traffic-", b"health-")):
                record(
                    "data",
                    address,
                    443,
                    data=data.decode("ascii", errors="replace"),
                    connection=f"{remote[0]}:{remote[1]}",
                )
                self._transport.sendto(data, remote)
                return
            super().datagram_received(data, remote)

    class Echo(QuicConnectionProtocol):
        def quic_event_received(self, event):
            from aioquic.quic.events import StreamDataReceived

            if isinstance(event, StreamDataReceived):
                record(
                    "data",
                    address,
                    443,
                    data=event.data.decode("ascii", errors="replace"),
                    connection=f"{id(self)}:{event.stream_id}",
                )
                self._quic.send_stream_data(
                    event.stream_id, event.data, end_stream=event.end_stream
                )
            else:
                super().quic_event_received(event)

    configuration = QuicConfiguration(is_client=False, alpn_protocols=[QUIC_ALPN])
    configuration.load_cert_chain(certificate, key)
    transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: ReceiptServer(configuration=configuration, create_protocol=Echo),
        local_addr=(address, 443),
    )
    return transport


async def quic_probe(spec):
    class Client(QuicConnectionProtocol):
        handshake = None
        failure = None

        def quic_event_received(self, event):
            if isinstance(event, HandshakeCompleted):
                self.handshake = event
            elif isinstance(event, ConnectionTerminated) and event.error_code:
                self.failure = event
            super().quic_event_received(event)

    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=[QUIC_ALPN],
        server_name=spec["host"],
        idle_timeout=10,
    )
    configuration.load_verify_locations(cafile=spec["certificate"])
    async with connect(
        spec["target"],
        spec["port"],
        configuration=configuration,
        create_protocol=Client,
        wait_connected=False,
    ) as client:
        connected = asyncio.create_task(client.wait_connected())
        phase = "handshake"
        try:
            async with asyncio.timeout(spec["timeout"]):
                client.transmit()
                await asyncio.shield(connected)
                check(
                    client.handshake and client.handshake.alpn_protocol == QUIC_ALPN,
                    "QUIC handshake did not negotiate the echo protocol",
                )
                phase = "stream exchange"

                async def exchange(index):
                    reader, writer = await client.create_stream()
                    body = payload(spec["nonce"], index, 96 * 1024)
                    writer.write(body)
                    writer.write_eof()
                    check(
                        await reader.read() == body, "QUIC stream/FIN payload mismatch"
                    )

                await asyncio.gather(*(exchange(index) for index in range(3)))
                await client.ping()
            return {"received": True, "protocol": "quic"}
        except (ConnectionError, TimeoutError) as error:
            if client.failure:
                raise ValueError(f"QUIC protocol/TLS failure: {client.failure}")
            return {
                "received": False,
                "error": f"QUIC {phase} timed out",
                "error_type": type(error).__name__,
                "timeout_phase": phase,
            }
        finally:
            # wait_connected shields its internal waiter. Keep its consumer
            # alive until close resolves it, including handshake timeouts.
            client.close()
            await client.wait_closed()
            await asyncio.gather(connected, return_exceptions=True)


class HttpSession:
    """A single socket retained across real gateway policy updates."""

    def __init__(self, spec):
        raw = socket.create_connection((spec["target"], spec["port"]), spec["timeout"])
        context = ssl.create_default_context(cafile=spec["certificate"])
        context.set_alpn_protocols(
            spec.get("alpn", ["h2" if spec["kind"] == "http2" else "http/1.1"])
        )
        try:
            self.stream = context.wrap_socket(raw, server_hostname=spec["host"])
        except BaseException:
            raw.close()
            raise
        self.h2 = (
            h2_connection(True)
            if self.stream.selected_alpn_protocol() == "h2"
            else None
        )
        self.ws = (
            wsproto.WSConnection(wsproto.ConnectionType.CLIENT)
            if spec["kind"] == "wss"
            else None
        )

    def close(self):
        self.stream.close()

    def probe(self, spec):
        if spec["expect"] == "deny":
            # The lease polls policy at most once a second. Require actual EOF
            # within two seconds; a timeout cannot pass as revocation evidence.
            self.stream.settimeout(2)
            try:
                for _ in range(16):
                    if not self.stream.recv(65536):
                        return {"received": False, "revoked": True}
            except (ConnectionResetError, ssl.SSLEOFError):
                return {"received": False, "revoked": True}
            except TimeoutError as error:
                raise ValueError(
                    "active connection survived policy revocation"
                ) from error
            raise ValueError("revoked connection kept sending data")
        if self.h2:
            return http2_probe(self.stream, spec, self.h2, keep_open=True)
        if self.ws:
            return websocket_probe(self.stream, spec, self.ws, keep_open=True)
        return http1_probe(self.stream, spec)


def probe(spec, sessions):
    spec = dict(spec, timeout=spec.get("timeout", 2))
    kind = spec["kind"]
    if session := spec.get("session"):
        if session not in sessions:
            sessions[session] = HttpSession(spec)
        return sessions[session].probe(spec)
    if kind == "connect":
        return connect_probe(spec)
    if kind in ("grpc", "grpcs"):
        return grpc_probe(spec)
    if kind == "quic":
        return asyncio.run(quic_probe(spec))
    with socket.create_connection(
        (spec["target"], spec["port"]), spec["timeout"]
    ) as raw:
        stream = raw
        if kind in ("tls", "http2", "wss"):
            context = ssl.create_default_context(cafile=spec["certificate"])
            context.set_alpn_protocols(
                spec.get("alpn", ["h2" if kind == "http2" else "http/1.1"])
            )
            stream = context.wrap_socket(raw, server_hostname=spec["host"])
        with stream:
            if kind == "http2":
                check(
                    stream.selected_alpn_protocol() == "h2",
                    "TLS did not negotiate HTTP/2",
                )
            if kind == "tls" and stream.selected_alpn_protocol() == "h2":
                return http2_probe(stream, spec)
            if kind in ("http", "tls"):
                return http1_probe(stream, spec)
            if kind in ("http2", "h2c"):
                return http2_probe(stream, spec)
            return websocket_probe(stream, spec)
