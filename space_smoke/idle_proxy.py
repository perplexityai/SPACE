"""TCP relay that drops connections which stay silent past an idle timeout."""

from __future__ import annotations

import contextlib
import selectors
import socket
import threading
import time
from collections.abc import Callable
from types import TracebackType
from typing import Self

_POLL_SECONDS = 0.1


class IdleTimeoutProxy:
    """Relay local connections to `upstream`, closing any connection that
    carries no bytes in either direction for `idle_timeout` seconds.

    Models an HTTP intermediary's idle timeout at the byte level, so only
    response bytes, not transport-level pings, keep a connection open. TLS
    passes through untouched; clients keep the upstream host for SNI and Host.
    """

    def __init__(self, upstream: tuple[str, int], *, idle_timeout: float) -> None:
        self._upstream = upstream
        self._idle_timeout = idle_timeout
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(_POLL_SECONDS)
        self._stopped = threading.Event()
        self._lock = threading.Lock()
        self._idle_closes = 0
        self._threads: list[threading.Thread] = []

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._listener.getsockname()[:2]
        return host, port

    @property
    def idle_closes(self) -> int:
        """Connections the proxy has closed for exceeding the idle timeout."""

        with self._lock:
            return self._idle_closes

    def __enter__(self) -> Self:
        self._spawn(self._accept)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stopped.set()
        for thread in self._threads:
            thread.join(timeout=5)
        self._listener.close()

    def _spawn(self, target: Callable[..., None], *args: object) -> None:
        thread = threading.Thread(target=target, args=args, daemon=True)
        self._threads.append(thread)
        thread.start()

    def _accept(self) -> None:
        while not self._stopped.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            client.settimeout(None)
            self._spawn(self._relay, client)

    def _relay(self, client: socket.socket) -> None:
        with (
            client,
            contextlib.suppress(OSError),
            socket.create_connection(self._upstream, timeout=10) as upstream,
        ):
            peers = {client: upstream, upstream: client}
            with selectors.DefaultSelector() as selector:
                for peer in peers:
                    selector.register(peer, selectors.EVENT_READ)
                last_activity = time.monotonic()
                while not self._stopped.is_set():
                    ready = selector.select(_POLL_SECONDS)
                    if not ready:
                        if time.monotonic() - last_activity >= self._idle_timeout:
                            with self._lock:
                                self._idle_closes += 1
                            return
                        continue
                    last_activity = time.monotonic()
                    for key, _ in ready:
                        source = key.fileobj
                        assert isinstance(source, socket.socket)
                        data = source.recv(65536)
                        if not data:
                            return
                        peers[source].sendall(data)
