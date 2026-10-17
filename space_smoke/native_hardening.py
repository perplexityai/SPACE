"""Status-preserving native API probes with scenario-owned cleanup.

These probes deliberately do not retry mutations: scenarios must distinguish
first-attempt behavior from successful recovery. Teardown may replay an
ambiguous create with its original key before removing its owned sandbox.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from http.client import HTTPException, HTTPResponse
from typing import ClassVar
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlsplit
from urllib.request import Request

from space_smoke.native import NativeConfig, _safe_origin, _URL_OPEN
from space_smoke.runtime import ScenarioContext

_TERMINAL = frozenset({"canceled", "expired", "failed", "succeeded"})
_MAX_JSON_BYTES = 1024 * 1024
_MAX_STREAM_BYTES = 32 * 1024 * 1024
_MAX_JOURNAL = 4000


class NativeTransportError(RuntimeError):
    """A transport failure with credential-free diagnostic text."""


@dataclass(frozen=True)
class NativeResponse:
    status: int
    body: dict[str, object]
    headers: dict[str, str]
    elapsed_seconds: float
    text: str = ""

    def require(self, status: int) -> dict[str, object]:
        if self.status != status:
            raise AssertionError(
                f"Native response was HTTP {self.status}, expected {status}"
            )
        return self.body


@dataclass(frozen=True)
class NativeEvent:
    id: str | None
    data: dict[str, object]


@dataclass(frozen=True)
class NativeEvents:
    status: int
    events: tuple[NativeEvent, ...]
    elapsed_seconds: float
    body: dict[str, object]
    headers: dict[str, str]


def _object(raw: bytes) -> dict[str, object]:
    try:
        value = json.loads(raw) if raw else {}
    except (ValueError, UnicodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _chunks(stream: HTTPResponse, *, limit: int, deadline: float) -> Iterator[bytes]:
    """Check a wall deadline between socket reads, including unterminated SSE lines."""
    consumed = 0
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError("Native response exceeded its deadline")
        # read1 performs at most one underlying read; read/readline can wait
        # forever for a complete buffer while a peer trickles bytes.
        chunk = stream.read1(min(65536, limit + 1 - consumed))
        if not chunk:
            return
        consumed += len(chunk)
        if consumed > limit:
            raise ValueError("Native response exceeded its byte bound")
        if time.monotonic() >= deadline:
            raise TimeoutError("Native response exceeded its deadline")
        yield chunk


def _event_lines(stream: HTTPResponse, *, deadline: float) -> Iterator[bytes]:
    pending = b""
    for chunk in _chunks(stream, limit=_MAX_STREAM_BYTES, deadline=deadline):
        pending += chunk
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            if len(line) > _MAX_JSON_BYTES:
                raise ValueError("Native SSE line exceeded its byte bound")
            yield line
        if len(pending) > _MAX_JSON_BYTES:
            raise ValueError("Native SSE line exceeded its byte bound")


def _string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"Native response {name} must be a nonempty string")
    return value


class NativeHardeningFixture:
    """Native-only fixture; every scenario explicitly creates its own guests."""

    fixture_name: ClassVar[str] = "native-hardening"
    timeout_minutes: ClassVar[int] = 15

    def __init__(
        self, context: ScenarioContext, config: NativeConfig, scenario_id: str
    ) -> None:
        self.context = context
        self.config = config
        self._lock = threading.RLock()
        self._run_id = uuid.uuid4().hex
        self._scenario_id = scenario_id
        self._creates: dict[str, dict[str, object]] = {}
        self._create_origins: dict[str, str] = {}
        self._uncertain_creates: set[str] = set()
        self._sandboxes: set[str] = set()
        self._operations: set[str] = set()
        self._journal: list[dict[str, object]] = []
        self.context.record("native_requests", self._journal)
        self.context.record("native_run_id", self._run_id)
        self.context.record("native_target", {
            "api_origin": list(_safe_origin(config.api_url)),
            "data_plane_origin": list(_safe_origin(config.data_plane_url)),
            "template": config.template,
        })
        self.context.scope.defer(
            "reconcile and delete native hardening resources",
            self.cleanup,
            critical=True,
        )

    def key(self, label: str) -> str:
        return f"smoke-{self._run_id}-{label}-{uuid.uuid4().hex}"

    def new_key(self, label: str) -> str:
        return self.key(label)

    def _record_request(self, observation: dict[str, object]) -> None:
        # Only request identities and outcome metadata are journaled. Bodies,
        # credentials, command output, URLs with queries, and headers are not.
        with self._lock:
            if len(self._journal) < _MAX_JOURNAL:
                self._journal.append(observation)
            else:
                self.context.evidence["native_requests_omitted"] = (
                    int(self.context.evidence.get("native_requests_omitted", 0)) + 1
                )

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, object] | None = None,
        idempotency_key: str | None = None,
        token: str | None = None,
        api_url: str | None = None,
        timeout: float | None = None,
    ) -> NativeResponse:
        """Return HTTP failures unchanged; transport failures raise safely.

        Raw creates are also tracked, including rejected/ambiguous probes.
        A supplied key is never replaced. Mutations have no automatic retries.
        """
        method = method.upper()
        base = api_url or self.config.api_url
        origin = _safe_origin(base)
        url = urljoin(base.rstrip("/") + "/", path)
        if not path.startswith("/v1/") or _safe_origin(url) != origin:
            raise ValueError("Native probe path must remain within /v1/ on its target")
        budget = self.config.request_timeout_seconds if timeout is None else timeout
        if budget <= 0:
            raise ValueError("Native request timeout must be positive")
        payload = dict(body) if body is not None else None
        create = method == "POST" and path == "/v1/sandboxes"
        if create and token is not None and token != self.config.token:
            raise ValueError("Create probes must use the fixture owner credential")
        if create:
            idempotency_key = idempotency_key or self.key("create")
            with self._lock:
                payload = dict(payload or {})
                metadata = payload.get("metadata", {})
                if isinstance(metadata, dict):
                    metadata = dict(metadata)
                    if any(str(key).startswith("space_smoke_") for key in metadata):
                        raise ValueError("Native probes reserve space_smoke_ metadata")
                    metadata.update({
                        "space_smoke_scenario": self._scenario_id,
                        "space_smoke_run": self._run_id,
                    })
                    payload["metadata"] = metadata
                previous = self._creates.get(idempotency_key)
                # Preserve the first body so cleanup can resolve a lost response,
                # even when a scenario deliberately reuses the key with a conflict.
                if previous is None:
                    self._creates[idempotency_key] = payload
                    self._create_origins[idempotency_key] = base
                self._uncertain_creates.add(idempotency_key)
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.config.token if token is None else token}",
        }
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        data = None
        if payload is not None:
            data = json.dumps(payload, separators=(",", ":")).encode()
            headers["Content-Type"] = "application/json"
        started = time.monotonic()
        observation: dict[str, object] = {
            "method": method,
            "path": urlsplit(path).path,
            "target_origin": list(origin),
        }
        try:
            try:
                response = _URL_OPEN(
                    Request(url, data=data, headers=headers, method=method),
                    timeout=budget,
                )
            except HTTPError as error:
                response = error
            with response:
                status = response.status
                raw = b"".join(_chunks(
                    response, limit=_MAX_JSON_BYTES, deadline=started + budget
                ))
                response_headers = dict(response.headers.items())
            if len(raw) > _MAX_JSON_BYTES:
                raise ValueError("Native JSON response exceeded the size bound")
            result = NativeResponse(
                status, _object(raw), response_headers,
                time.monotonic() - started, raw.decode(errors="replace"),
            )
            observation.update(status=status, elapsed_seconds=result.elapsed_seconds)
            with self._lock:
                operation_id = result.body.get("operation_id")
                sandbox_id = result.body.get("sandbox_id")
                if 200 <= status < 300:
                    if isinstance(operation_id, str):
                        self._operations.add(operation_id)
                        observation["operation_id"] = operation_id
                    if create and isinstance(sandbox_id, str):
                        self._sandboxes.add(sandbox_id)
                        observation["sandbox_id"] = sandbox_id
                    if create and isinstance(operation_id, str) and isinstance(sandbox_id, str):
                        self._uncertain_creates.discard(idempotency_key)
                # Conflicts and server failures can follow a previously committed
                # request. Only definitive rejection of a first request is final.
                elif create and 400 <= status < 500 and status not in {408, 409, 429}:
                    self._uncertain_creates.discard(idempotency_key)
                self.context.record("native_sandbox_ids", sorted(self._sandboxes))
                self.context.record("native_operation_ids", sorted(self._operations))
            error_body = result.body.get("error")
            if isinstance(error_body, dict):
                observation["error_code"] = error_body.get("code")
                observation["retryable"] = error_body.get("retryable")
            return result
        except Exception as error:
            observation.update(
                transport_error=type(error).__name__,
                elapsed_seconds=time.monotonic() - started,
            )
            # Some HTTP transports include request URLs in exception text.
            failure = (
                NativeTransportError
                if isinstance(error, (URLError, OSError, HTTPException))
                else RuntimeError
            )
            raise failure(
                f"Native {method} probe failed ({type(error).__name__})"
            ) from None
        finally:
            self._record_request(observation)

    def create(
        self,
        body: Mapping[str, object] | None = None,
        *,
        idempotency_key: str | None = None,
        api_url: str | None = None,
    ) -> NativeResponse:
        return self.request(
            "POST", "/v1/sandboxes",
            body={"template": self.config.template, **dict(body or {})},
            idempotency_key=idempotency_key,
            api_url=api_url,
        )

    def wait_operation(
        self,
        operation_id: str,
        *,
        timeout: float | None = None,
        require_success: bool = True,
    ) -> dict[str, object]:
        result = self._wait(
            f"/v1/operations/{quote(operation_id, safe='')}",
            _TERMINAL, timeout=timeout,
        )
        if require_success and result.get("state") != "succeeded":
            raise AssertionError(
                f"Native operation {operation_id} ended in {result.get('state')}"
            )
        return result

    def wait_sandbox(
        self, sandbox_id: str, state: str = "running", *, timeout: float | None = None
    ) -> dict[str, object]:
        return self._wait(
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}",
            frozenset({state}), timeout=timeout,
        )

    def _wait(
        self, path: str, states: frozenset[str], *, timeout: float | None
    ) -> dict[str, object]:
        budget = self.config.ready_timeout_seconds if timeout is None else timeout
        deadline = time.monotonic() + budget
        while time.monotonic() < deadline:
            response = self.request(
                "GET", path,
                timeout=min(self.config.request_timeout_seconds, deadline - time.monotonic()),
            )
            if response.status == 404 and states == {"deleted"}:
                return {"state": "deleted"}
            if response.status == 200 and response.body.get("state") in states:
                return response.body
            if response.status not in {200, 404, 429, 502, 503, 504}:
                raise AssertionError(f"Native polling returned HTTP {response.status}")
            time.sleep(min(
                max(self.config.poll_interval_seconds, 0.05),
                max(0, deadline - time.monotonic()),
            ))
        raise TimeoutError("Native resource did not reach the requested state")

    def create_ready(
        self, body: Mapping[str, object] | None = None, *, timeout: float | None = None
    ) -> str:
        accepted = self.create(body).require(202)
        self.wait_operation(_string(accepted.get("operation_id"), "operation_id"), timeout=timeout)
        sandbox_id = _string(accepted.get("sandbox_id"), "sandbox_id")
        self.wait_sandbox(sandbox_id, timeout=timeout)
        return sandbox_id

    def command(
        self,
        sandbox_id: str,
        command: Sequence[str],
        *,
        idempotency_key: str | None = None,
        timeout_seconds: int = 60,
    ) -> NativeResponse:
        return self.request(
            "POST", f"/v1/sandboxes/{quote(sandbox_id, safe='')}/commands",
            body={"command": list(command), "timeout": f"{timeout_seconds}s"},
            idempotency_key=idempotency_key or self.key("command"),
        )

    def events(
        self,
        command: Mapping[str, object],
        *,
        timeout: float = 65,
        last_event_id: str | None = None,
        max_events: int | None = None,
        data_plane_url: str | None = None,
        initial_read_delay_seconds: float = 0,
        event_read_delay_seconds: float = 0,
        on_event: Callable[[NativeEvent], None] | None = None,
    ) -> NativeEvents:
        """Read one bounded attachment, retaining SSE IDs for explicit reconnects.

        `on_event`, when given, sees each event as it arrives, on the reading
        thread, so a caller reading on another thread can act mid-stream.
        """
        if timeout <= 0 or (max_events is not None and max_events <= 0):
            raise ValueError("Native stream bounds must be positive")
        if initial_read_delay_seconds < 0 or event_read_delay_seconds < 0:
            raise ValueError("Native read delays cannot be negative")
        attach = command.get("attach")
        if not isinstance(attach, dict):
            raise TypeError("Native command response omitted attach")
        base = data_plane_url or self.config.data_plane_url
        origin = _safe_origin(base)
        events_url = _string(attach.get("events_url"), "events_url")
        url = urljoin(base.rstrip("/") + "/", events_url)
        if _safe_origin(url) != origin:
            raise ValueError("Native events URL is outside configured data-plane origin")
        headers = {
            "Accept": "text/event-stream",
            "Authorization": f"Bearer {_string(attach.get('token'), 'attach token')}",
        }
        if last_event_id is not None:
            headers["Last-Event-ID"] = last_event_id
        started = time.monotonic()
        deadline = started + timeout
        frames: list[NativeEvent] = []
        observation: dict[str, object] = {
            "method": "GET", "path": urlsplit(url).path,
            "target_origin": list(origin), "stream": True,
        }
        try:
            try:
                stream = _URL_OPEN(
                    Request(url, headers=headers),
                    timeout=min(timeout, self.config.request_timeout_seconds),
                )
            except HTTPError as error:
                stream = error
            with stream:
                status = stream.status
                response_headers = dict(stream.headers.items())
                observation["status"] = status
                if status != 200:
                    raw = b"".join(_chunks(
                        stream, limit=_MAX_JSON_BYTES, deadline=deadline
                    ))
                    return NativeEvents(
                        status, (), time.monotonic() - started, _object(raw), response_headers
                    )
                self._read_delay(initial_read_delay_seconds, deadline)
                lines: list[str] = []
                event_id: str | None = None
                for raw_line in _event_lines(stream, deadline=deadline):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Native event stream exceeded its deadline")
                    line = raw_line.decode().rstrip("\r\n")
                    if line:
                        if line.startswith("data:"):
                            lines.append(line[5:].lstrip())
                        elif line.startswith("id:"):
                            event_id = line[3:].lstrip()
                        continue
                    if not lines:
                        continue
                    frame = json.loads("\n".join(lines))
                    if not isinstance(frame, dict):
                        raise ValueError("Native stream event was not a JSON object")
                    frames.append(NativeEvent(event_id, frame))
                    if on_event is not None:
                        on_event(frames[-1])
                    lines = []
                    if frame.get("type") in {"process_exited", "error"} or (
                        max_events is not None and len(frames) >= max_events
                    ):
                        break
                    self._read_delay(event_read_delay_seconds, deadline)
            observation["event_count"] = len(frames)
            return NativeEvents(status, tuple(frames), time.monotonic() - started, {}, response_headers)
        except Exception as error:
            observation["transport_error"] = type(error).__name__
            failure = (
                NativeTransportError
                if isinstance(error, (URLError, OSError, HTTPException))
                else RuntimeError
            )
            raise failure(
                f"Native stream probe failed ({type(error).__name__})"
            ) from None
        finally:
            observation["elapsed_seconds"] = time.monotonic() - started
            self._record_request(observation)

    @staticmethod
    def _read_delay(delay: float, deadline: float) -> None:
        if delay >= deadline - time.monotonic():
            raise TimeoutError("Native slow-reader delay exceeded stream budget")
        if delay:
            time.sleep(delay)

    def cleanup(self) -> None:
        """Await accepted mutations before deletion; early 404 is not completion."""
        deadline = time.monotonic() + self.config.ready_timeout_seconds
        failures: list[str] = []

        def remaining() -> float:
            budget = deadline - time.monotonic()
            if budget <= 0:
                raise TimeoutError("Native cleanup exceeded its deadline")
            return budget

        # A lost response is resolved only by replaying its identical owned create.
        # A failing replay stays visible as incomplete cleanup, never as success.
        for key in sorted(self._uncertain_creates):
            payload = dict(self._creates[key])
            metadata = payload.get("metadata")
            if isinstance(metadata, dict):
                payload["metadata"] = {
                    name: value for name, value in metadata.items()
                    if not name.startswith("space_smoke_")
                }
            try:
                self.request(
                    "POST", "/v1/sandboxes", body=payload, idempotency_key=key,
                    api_url=self._create_origins[key],
                    timeout=min(self.config.request_timeout_seconds, remaining()),
                )
            except Exception as error:
                failures.append(f"ambiguous create: {type(error).__name__}")
        for operation_id in sorted(self._operations):
            try:
                self.wait_operation(operation_id, timeout=remaining(), require_success=False)
            except Exception as error:
                failures.append(f"{operation_id}: {type(error).__name__}")
        # Do not delete under an unfinished create: it can recreate its sandbox
        # after a transient absence. Leave its identity and fail cleanup instead.
        if failures or self._uncertain_creates:
            self.context.record("native_cleanup_incomplete", {
                "operations": sorted(self._operations),
                "sandboxes": sorted(self._sandboxes),
                "ambiguous_create_count": len(self._uncertain_creates),
                "failures": failures,
            })
            raise RuntimeError("Native cleanup could not establish terminal mutations")
        for sandbox_id in sorted(self._sandboxes):
            try:
                path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}"
                while True:
                    response = self.request(
                        "DELETE", path, idempotency_key=self.key("cleanup"),
                        timeout=min(self.config.request_timeout_seconds, remaining()),
                    )
                    if response.status == 404:
                        break
                    if response.status == 202:
                        operation = self.wait_operation(
                            _string(response.body.get("operation_id"), "operation_id"),
                            timeout=remaining(), require_success=False,
                        )
                        if operation.get("state") == "succeeded":
                            absent = self.request(
                                "GET", path,
                                timeout=min(self.config.request_timeout_seconds, remaining()),
                            )
                            if absent.status == 404:
                                break
                    elif response.status not in {409, 429, 500, 502, 503, 504}:
                        raise RuntimeError(f"Native cleanup DELETE returned {response.status}")
                    time.sleep(min(1, remaining()))
            except Exception as error:
                failures.append(f"{sandbox_id}: {type(error).__name__}")
        self.context.record("native_cleanup", {
            "sandbox_count": len(self._sandboxes),
            "operation_count": len(self._operations),
            "failures": failures,
        })
        if failures:
            raise RuntimeError("Native cleanup failed for owned resources")
