"""ExchangeToken over a node-local credential-manager socket, from the host.

The node credential agent serves gRPC on Unix sockets inside the Kind node and
authorizes each caller by its peer credentials (SO_PEERCRED). The harness
reaches one the way a node service would: it starts a byte relay in the node's
operator shell under the caller's account (`setpriv`), connected to the
socket, and carries the relay's stdin and stdout over `kubectl exec`. A local
Unix socket in front of that relay gives the host's gRPC client a channel to
the agent. The node image runs perl, which the relay is written in; nothing is
installed on the node.

The two messages are encoded here by field number
(proto/credential/manager/v1/credential_manager.proto) instead of from
generated stubs, which this harness does not build.
"""

from __future__ import annotations

import contextlib
import importlib
import os
import socket
import subprocess
import tempfile
import threading
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import grpc

EXCHANGE_TOKEN_METHOD = "/space.credential.manager.v1.CredentialManager/ExchangeToken"
TOKEN_EXCHANGE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
AWS_CREDENTIALS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:aws-credentials"
SUBJECT_ID_TOKEN_TYPE = "urn:space:token-type:subject-id"
STORAGE_AUDIENCE = "s3.amazonaws.com"

# Copies stdin to the socket and the socket to stdout until either side
# closes. `$ARGV[0]` is the socket path.
RELAY = r"""
use strict; use IO::Socket::UNIX; use IO::Select;
my $s = IO::Socket::UNIX->new(Type => SOCK_STREAM(), Peer => $ARGV[0]) or die "connect $ARGV[0]: $!\n";
binmode STDIN; binmode STDOUT;
my $sel = IO::Select->new(\*STDIN, $s);
while (my @ready = $sel->can_read) {
  for my $from (@ready) {
    my $n = sysread($from, my $buf, 65536);
    exit 0 unless $n;
    my $to = $from == $s ? \*STDOUT : $s;
    for (my $off = 0; $off < $n;) { my $w = syswrite($to, $buf, $n - $off, $off); die "write: $!\n" unless defined $w; $off += $w; }
  }
}
"""


@dataclass(frozen=True)
class ExchangedCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expires_at_unix: int


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _string_field(number: int, value: str) -> bytes:
    data = value.encode()
    return _varint(number << 3 | 2) + _varint(len(data)) + data


def encode_exchange_request(
    *, resource: str, scope: str, subject: str, expires_in: int = 3600
) -> bytes:
    """ExchangeTokenRequest as volume-manager sends it (credentials.rs)."""

    return b"".join(
        [
            _string_field(1, TOKEN_EXCHANGE_GRANT_TYPE),
            _string_field(2, resource),
            _string_field(3, STORAGE_AUDIENCE),
            _string_field(4, scope),
            _string_field(5, AWS_CREDENTIALS_TOKEN_TYPE),
            _string_field(6, subject),
            _string_field(7, SUBJECT_ID_TOKEN_TYPE),
            _varint(10 << 3 | 0) + _varint(expires_in),
        ]
    )


def _fields(data: bytes) -> Iterator[tuple[int, int | bytes]]:
    position = 0

    def varint() -> int:
        nonlocal position
        shift = result = 0
        while True:
            if position >= len(data):
                raise ValueError("truncated varint")
            byte = data[position]
            position += 1
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return result
            shift += 7

    while position < len(data):
        key = varint()
        number, wire = key >> 3, key & 7
        if wire == 0:
            yield number, varint()
        elif wire == 2:
            length = varint()
            if position + length > len(data):
                raise ValueError("truncated field")
            yield number, data[position : position + length]
            position += length
        elif wire == 1:
            position += 8
        elif wire == 5:
            position += 4
        else:
            raise ValueError(f"unsupported wire type {wire}")


def decode_exchange_response(data: bytes) -> ExchangedCredentials:
    """The AwsCredentials payload (field 7) of an ExchangeTokenResponse."""

    for number, value in _fields(data):
        if number == 7 and isinstance(value, bytes):
            strings: dict[int, str] = {}
            expires = 0
            for inner, inner_value in _fields(value):
                if isinstance(inner_value, bytes):
                    strings[inner] = inner_value.decode()
                elif inner == 4:
                    expires = inner_value
            return ExchangedCredentials(
                access_key_id=strings.get(1, ""),
                secret_access_key=strings.get(2, ""),
                session_token=strings.get(3, ""),
                expires_at_unix=expires,
            )
    raise ValueError("ExchangeTokenResponse carries no AWS credentials")


def relay_argv(
    kubectl: Sequence[str], pod: str, container: str, socket_path: str, *, user: str, groups: Sequence[str]
) -> list[str]:
    """`kubectl exec` of the relay, under `user` with `groups` as supplementary groups."""

    resolve = " ".join(
        [
            f'uid="$(id -u {user})"',
            f'gid="$(id -g {user})"',
            'groups="$gid' + "".join(f',$(getent group {group} | cut -d: -f3)' for group in groups) + '"',
        ]
    )
    script = (
        f"set -eu; {resolve}; "
        'exec setpriv --reuid="$uid" --regid="$gid" --groups="$groups" -- perl -e "$1" "$2"'
    )
    return [*kubectl, "exec", "-i", pod, "-c", container, "--", "bash", "-c", script, "relay", RELAY, socket_path]


def _grpc():
    # Imported where it is used: the scenario registry imports this module,
    # and its inventory (quarantine_backlog) runs on the standard library alone.
    return importlib.import_module("grpc")


@contextlib.contextmanager
def relayed_channel(argv: Sequence[str]) -> Iterator[grpc.Channel]:
    """A gRPC channel whose every connection runs `argv` as its byte relay."""

    directory = tempfile.mkdtemp(prefix="space-smoke-credmgr-")
    path = os.path.join(directory, "relay.sock")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(4)
    processes: list[subprocess.Popen[bytes]] = []
    stopping = threading.Event()

    def pump(source, sink, close) -> None:
        try:
            while chunk := source(65536):
                sink(chunk)
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                close()

    def serve() -> None:
        while not stopping.is_set():
            try:
                connection, _ = listener.accept()
            except OSError:
                return
            process = subprocess.Popen(
                list(argv), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0
            )
            processes.append(process)
            assert process.stdin is not None and process.stdout is not None
            threading.Thread(
                target=pump, args=(connection.recv, process.stdin.write, process.stdin.close), daemon=True
            ).start()
            threading.Thread(
                target=pump,
                args=(process.stdout.read, connection.sendall, lambda c=connection: c.shutdown(socket.SHUT_WR)),
                daemon=True,
            ).start()

    threading.Thread(target=serve, daemon=True).start()
    grpc = _grpc()
    # The default :authority for a unix target is the percent-encoded path,
    # which the agent's HTTP/2 server rejects as malformed.
    channel = grpc.insecure_channel(
        f"unix:{path}", options=[("grpc.enable_retries", 0), ("grpc.default_authority", "localhost")]
    )
    try:
        yield channel
    finally:
        channel.close()
        stopping.set()
        listener.close()
        for process in processes:
            with contextlib.suppress(OSError):
                process.kill()
            process.wait(timeout=10)
        with contextlib.suppress(OSError):
            os.unlink(path)
        with contextlib.suppress(OSError):
            os.rmdir(directory)


def exchange_token(
    argv: Sequence[str], *, resource: str, scope: str, subject: str, timeout: float = 60
) -> tuple[ExchangedCredentials | None, str, str]:
    """(credentials, status code name, detail) of one ExchangeToken call."""

    request = encode_exchange_request(resource=resource, scope=scope, subject=subject)
    with relayed_channel(argv) as channel:
        call = channel.unary_unary(
            EXCHANGE_TOKEN_METHOD,
            request_serializer=lambda message: message,
            response_deserializer=lambda message: message,
        )
        try:
            response = call(request, timeout=timeout)
        except _grpc().RpcError as error:
            return None, error.code().name, error.details() or ""
    return decode_exchange_response(response), "OK", ""
