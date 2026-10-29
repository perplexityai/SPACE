"""Minimal raw Native /v1 adapter for shared typed smoke scenarios."""

from __future__ import annotations

import base64
import json
import time
import uuid
from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass, replace
from typing import BinaryIO, cast
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from space_smoke.capabilities import (
    CommandResult,
    CommandSandbox,
    DurableLifecycleSandbox,
    ProcessHandle,
    ProcessInfo,
    SandboxInfo,
    SandboxPages,
)

_TERMINAL_OPERATION_STATES = frozenset({"canceled", "expired", "failed", "succeeded"})
_TERMINAL_CREATE_STATES = frozenset({"deleted", "deleting", "failed"})


class _NativeTerminalState(RuntimeError):
    def __init__(self, subject: str, state: str, payload: Mapping[str, object]):
        super().__init__(f"{subject} ended in {state}: {payload!r}")
        self.payload = payload


def is_suspend_upload_conflict(operation: Mapping[str, object]) -> bool:
    """Whether a delete operation failed only because a suspend's snapshot
    upload is still in flight, which a fresh DELETE retries once it finalizes."""

    detail = operation.get("terminal_error")
    return (
        operation.get("state") == "failed"
        and operation.get("terminal_error_code") == "conflict"
        and isinstance(detail, dict)
        and detail.get("message")
        == (
            "sandbox has a suspend-snapshot upload in flight; "
            "retry the DELETE after the snapshot finalizes"
        )
    )


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


_NO_PROXY = ProxyHandler({})
_URL_OPEN = build_opener(_NO_PROXY, _NoRedirect()).open


@dataclass(frozen=True)
class NativeConfig:
    api_url: str
    data_plane_url: str
    token: str
    template: str
    ready_timeout_seconds: float = 180
    request_timeout_seconds: float = 15
    poll_interval_seconds: float = 1
    # Templates built from this tree retain a finished command's exit.
    reports_exited_processes: bool = True

    def __post_init__(self) -> None:
        if not self.api_url or not self.data_plane_url or not self.token:
            raise ValueError("Native URLs and token must not be empty")
        _safe_origin(self.api_url)
        _safe_origin(self.data_plane_url)
        if self.ready_timeout_seconds <= 0 or self.request_timeout_seconds <= 0:
            raise ValueError("Native timeouts must be positive")
        if self.poll_interval_seconds < 0:
            raise ValueError("Native poll interval must not be negative")


class NativeSandboxFactory:
    """Create, list, command, and delete Native sandboxes with bounded waits."""

    def __init__(self, config: NativeConfig) -> None:
        self._config = config

    @property
    def config(self) -> NativeConfig:
        return self._config

    def as_member(self, email: str, password: str) -> NativeSandboxFactory:
        """Log in as an organization member and act as that member.

        Native creates are owned by the caller's organization, so a session is
        how a scenario creates sandboxes under that organization's policy. A
        failure names only its type: the request carries the password.
        """
        try:
            session = self._json_request(
                "POST",
                "/auth/v1/sessions",
                body={"email": email, "password": password},
                expected_status=200,
                authenticated=False,
            )
            token = _required_string(session, "token")
        except Exception as error:
            raise RuntimeError(
                f"Native session login failed ({type(error).__name__})"
            ) from None
        return NativeSandboxFactory(replace(self._config, token=token))

    def create(
        self, metadata: Mapping[str, str] | None = None
    ) -> DurableLifecycleSandbox:
        response = self._json_request(
            "POST",
            "/v1/sandboxes",
            body={
                "template": self._config.template,
                "metadata": dict(metadata or {}),
            },
            mutation="create",
            expected_status=202,
        )
        sandbox_id = _required_string(response, "sandbox_id")
        sandbox = _NativeSandbox(self, sandbox_id)
        try:
            self._wait_operation(_required_string(response, "operation_id"))
            self._wait_sandbox_running(sandbox_id)
        except Exception as error:
            try:
                self.delete(sandbox)
            except Exception as cleanup_error:
                error.add_note(f"Native create cleanup failed: {cleanup_error}")
            raise
        return sandbox

    def delete(self, sandbox: CommandSandbox) -> None:
        deadline = time.monotonic() + self._config.ready_timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "Native delete did not finish before its cleanup deadline"
                )
            try:
                response = self._json_request(
                    "DELETE",
                    f"/v1/sandboxes/{quote(sandbox.sandbox_id, safe='')}",
                    mutation="delete",
                    expected_status=202,
                    timeout=min(self._config.request_timeout_seconds, remaining),
                )
            except HTTPError as error:
                if error.code == 404:
                    return
                raise
            try:
                self._wait_operation(
                    _required_string(response, "operation_id"), deadline=deadline
                )
                return
            except _NativeTerminalState as error:
                if not is_suspend_upload_conflict(error.payload):
                    raise
                # The failed operation is immutable. Submit a fresh idempotency
                # key after the upload advances, retaining the original deadline.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "Native delete snapshot upload conflict did not clear"
                    ) from error
                time.sleep(min(max(self._config.poll_interval_seconds, 0.1), remaining))

    def register_credential_reference(self, secret_arn: str) -> str:
        """Register an AWS secret by reference through the public API."""

        response = self._json_request(
            "POST",
            "/v1/credential-references",
            body={
                "request_id": uuid.uuid4().hex,
                "provider": "aws_secrets_manager",
                "secret_arn": secret_arn,
            },
            mutation="register-credential-reference",
            expected_status=201,
        )
        return _required_string(response, "credential_reference_id")

    def set_network_policy(
        self, sandbox_id: str, policy: Mapping[str, object]
    ) -> None:
        """Replace a sandbox policy through PATCH and await reconciliation."""

        response = self._json_request(
            "PATCH",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}",
            body={"network_policy": {"mode": "set", "policy": dict(policy)}},
            mutation="set-network-policy",
            expected_status=202,
        )
        self._wait_operation(_required_string(response, "operation_id"))

    def get_network_policy(self, sandbox_id: str) -> dict[str, object]:
        """Read the sandbox's effective desired policy document."""

        return self._json_request(
            "GET",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}/network-policy",
            expected_status=200,
        )

    def retire_credential_reference(self, credential_reference_id: str) -> None:
        """Retire a detached reference through the public API."""

        self._json_request(
            "DELETE",
            "/v1/credential-references/"
            f"{quote(credential_reference_id, safe='')}",
            expected_status=204,
        )

    def list_pages(
        self,
        *,
        limit: int,
        max_pages: int,
        stop_after_sandbox_ids: Collection[str] | None = None,
    ) -> SandboxPages[SandboxInfo]:
        if limit <= 0:
            raise ValueError("sandbox page size must be positive")
        if max_pages <= 0:
            raise ValueError("sandbox page cap must be positive")

        target_ids = (
            frozenset(stop_after_sandbox_ids)
            if stop_after_sandbox_ids is not None
            else None
        )
        pages: list[tuple[SandboxInfo, ...]] = []
        observed_ids: set[str] = set()
        cursor: str | None = None
        while len(pages) < max_pages:
            query = {"limit": str(limit)}
            if cursor is not None:
                query["cursor"] = cursor
            response = self._json_request(
                "GET",
                f"/v1/sandboxes?{urlencode(query)}",
                expected_status=200,
            )
            data = response.get("data")
            if not isinstance(data, list):
                raise TypeError("Native sandbox page field 'data' was not an array")
            page = tuple(
                SandboxInfo(_required_string(_page_item(item, index), "id"))
                for index, item in enumerate(data)
            )
            pages.append(page)
            observed_ids.update(item.sandbox_id for item in page)
            cursor = _optional_string(response, "next_cursor")
            if cursor is None:
                return SandboxPages(tuple(pages), has_more=False)
            if target_ids is not None and target_ids <= observed_ids:
                return SandboxPages(tuple(pages), has_more=True)
        return SandboxPages(tuple(pages), has_more=cursor is not None)

    def run(self, sandbox_id: str, command: str, *, timeout: int) -> CommandResult:
        events_url, token, _ = self._create_command(
            sandbox_id, command, timeout=timeout
        )
        return self._collect_command(events_url, token, timeout=timeout)

    def _create_command(
        self, sandbox_id: str, command: str, *, timeout: int
    ) -> tuple[str, str, str]:
        response = self._json_request(
            "POST",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}/commands",
            body={"command": ["sh", "-c", command], "timeout": f"{timeout}s"},
            mutation="command",
            expected_status=201,
        )
        attach = _required_object(response, "attach")
        events_url = _required_string(attach, "events_url")
        token = _required_string(attach, "token")
        process_id = _required_string(response, "process_id")
        return events_url, token, process_id

    def start_background(
        self, sandbox_id: str, command: str, *, timeout: int
    ) -> ProcessHandle:
        events_url, token, created_process_id = self._create_command(
            sandbox_id, command, timeout=timeout
        )
        url = self._events_url(events_url)
        request = Request(
            url,
            headers={"Accept": "text/event-stream", "Authorization": f"Bearer {token}"},
        )
        with _URL_OPEN(
            request,
            timeout=max(self._config.request_timeout_seconds, timeout + 5),
        ) as response_stream:
            for frame in _sse_frames(cast(BinaryIO, response_stream)):
                if frame.get("type") == "error":
                    raise RuntimeError(f"Native command stream failed: {frame!r}")
                if frame.get("type") == "process_started":
                    # createCommand hands back the process id up front; the
                    # stream must agree with it, or callers that never attach
                    # would address the wrong process.
                    streamed_process_id = _required_string(frame, "process_id")
                    if streamed_process_id != created_process_id:
                        raise RuntimeError(
                            "Native createCommand process_id "
                            f"{created_process_id!r} did not match "
                            f"process_started {streamed_process_id!r}"
                        )
                    return ProcessInfo(
                        process_id=created_process_id,
                        pid=_required_integer(frame, "pid"),
                        command=("sh", "-c", command),
                    )
        raise RuntimeError("Native command stream ended before process_started")

    def list_processes(self, sandbox_id: str) -> tuple[ProcessInfo, ...]:
        response = self._json_request(
            "GET",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}/processes",
            expected_status=200,
        )
        data = response.get("data")
        if not isinstance(data, list):
            raise TypeError("Native process list field 'data' was not an array")
        return tuple(
            _native_process(_process_item(item, index))
            for index, item in enumerate(data)
        )

    def get_process(self, sandbox_id: str, process_id: str) -> ProcessInfo | None:
        try:
            response = self._json_request(
                "GET",
                f"/v1/sandboxes/{quote(sandbox_id, safe='')}/processes/"
                f"{quote(process_id, safe='')}",
                expected_status=200,
            )
        except HTTPError as error:
            if error.code == 404:
                return None
            raise
        return _native_process(response)

    def signal_process(self, sandbox_id: str, process_id: str, signal: int) -> bool:
        response = self._json_request(
            "POST",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}/processes/"
            f"{quote(process_id, safe='')}:signal",
            body={"signal": signal},
            expected_status=200,
        )
        return _required_bool(response, "ok")

    def kill_process(self, sandbox_id: str, process_id: str) -> bool:
        try:
            response = self._json_request(
                "POST",
                f"/v1/sandboxes/{quote(sandbox_id, safe='')}/processes/"
                f"{quote(process_id, safe='')}:kill",
                expected_status=200,
            )
        except HTTPError as error:
            if error.code == 404:
                return False
            raise
        return _required_bool(response, "ok")

    def suspend(self, sandbox_id: str) -> None:
        response = self._json_request(
            "POST",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}:suspend",
            mutation="suspend",
            expected_status=202,
        )
        self._wait_operation(_required_string(response, "operation_id"))
        self._wait_sandbox_state(sandbox_id, "suspended")

    def pause(self, sandbox_id: str) -> None:
        response = self._json_request(
            "POST",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}:pause",
            mutation="pause",
            expected_status=202,
        )
        self._wait_operation(_required_string(response, "operation_id"))
        self._wait_sandbox_state(
            sandbox_id,
            "paused",
            terminal=_TERMINAL_CREATE_STATES | {"suspended"},
        )

    def wake(self, sandbox_id: str) -> None:
        response = self._json_request(
            "POST",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}:wake",
            mutation="wake",
            expected_status=202,
        )
        self._wait_operation(_required_string(response, "operation_id"))
        self._wait_sandbox_running(sandbox_id)

    def stop(self, sandbox_id: str) -> None:
        response = self._json_request(
            "POST",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}:stop",
            mutation="stop",
            expected_status=202,
        )
        self._wait_operation(_required_string(response, "operation_id"))
        self._wait_sandbox_state(sandbox_id, "stopped")

    # A stopped restart that hits the standing kind failure spends up to ~130 s
    # in one Wake readiness budget and then ~16 x 6 s in the controller's retry
    # ladder before the operation terminates (bench, 2026-09-22). The wait must
    # outlast that path or the client times out first and the terminal error is
    # never observed.
    STOPPED_RESTART_TIMEOUT_SECONDS = 360

    def start(self, sandbox_id: str) -> None:
        response = self._json_request(
            "POST",
            f"/v1/sandboxes/{quote(sandbox_id, safe='')}:start",
            mutation="start",
            expected_status=202,
        )
        self._wait_operation(
            _required_string(response, "operation_id"),
            timeout_seconds=self.STOPPED_RESTART_TIMEOUT_SECONDS,
        )
        self._wait_sandbox_running(sandbox_id)

    def _wait_operation(
        self,
        operation_id: str,
        *,
        timeout_seconds: float | None = None,
        deadline: float | None = None,
    ) -> None:
        self._wait_state(
            f"/v1/operations/{quote(operation_id, safe='')}",
            ready="succeeded",
            terminal=_TERMINAL_OPERATION_STATES,
            subject=f"Native operation {operation_id}",
            timeout_seconds=timeout_seconds,
            deadline=deadline,
        )

    def _wait_sandbox_running(self, sandbox_id: str) -> None:
        self._wait_sandbox_state(sandbox_id, "running")
        encoded_id = quote(sandbox_id, safe="")
        self._json_request(
            "GET",
            f"/v1/sandboxes/{encoded_id}/processes?limit=1",
            expected_status=200,
            timeout=self._config.ready_timeout_seconds,
        )

    def _wait_sandbox_state(
        self,
        sandbox_id: str,
        ready: str,
        *,
        terminal: frozenset[str] = _TERMINAL_CREATE_STATES,
    ) -> None:
        encoded_id = quote(sandbox_id, safe="")
        self._wait_state(
            f"/v1/sandboxes/{encoded_id}",
            ready=ready,
            terminal=terminal,
            subject=f"Native sandbox {sandbox_id}",
        )

    def _wait_state(
        self,
        path: str,
        *,
        ready: str,
        terminal: frozenset[str],
        subject: str,
        timeout_seconds: float | None = None,
        deadline: float | None = None,
    ) -> None:
        bounded_request = deadline is not None
        budget = timeout_seconds or self._config.ready_timeout_seconds
        if deadline is None:
            deadline = time.monotonic() + budget
        while True:
            remaining = deadline - time.monotonic()
            if bounded_request and remaining <= 0:
                raise TimeoutError(
                    f"{subject} did not reach {ready} before its cleanup deadline"
                )
            payload = self._json_request(
                "GET",
                path,
                expected_status=200,
                timeout=min(self._config.request_timeout_seconds, remaining)
                if bounded_request
                else None,
            )
            state = _required_string(payload, "state")
            if state == ready:
                return
            if state in terminal:
                if bounded_request:
                    raise _NativeTerminalState(subject, state, payload)
                raise RuntimeError(f"{subject} ended in {state}: {payload!r}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"{subject} did not reach {ready} in {budget:g}s"
                )
            time.sleep(min(self._config.poll_interval_seconds, remaining))

    def _collect_command(
        self,
        events_url: str,
        token: str,
        *,
        timeout: int,
    ) -> CommandResult:
        url = self._events_url(events_url)
        request = Request(
            url,
            headers={
                "Accept": "text/event-stream",
                "Authorization": f"Bearer {token}",
            },
        )
        stdout: list[bytes] = []
        stderr: list[bytes] = []
        with _URL_OPEN(
            request,
            timeout=max(self._config.request_timeout_seconds, timeout + 5),
        ) as response:
            for frame in _sse_frames(cast(BinaryIO, response)):
                frame_type = frame.get("type")
                if frame_type in {"stdout", "stderr"}:
                    output = base64.b64decode(
                        _required_string(frame, "data_base64"), validate=True
                    )
                    (stdout if frame_type == "stdout" else stderr).append(output)
                    continue
                if frame_type == "error":
                    raise RuntimeError(f"Native command stream failed: {frame!r}")
                if frame_type == "process_exited":
                    return CommandResult(
                        _required_integer(frame, "exit_code"),
                        b"".join(stdout).decode(),
                        b"".join(stderr).decode(),
                    )
        raise RuntimeError("Native command stream ended before process_exited")

    def _events_url(self, events_url: str) -> str:
        url = urljoin(f"{self._config.data_plane_url.rstrip('/')}/", events_url)
        if _safe_origin(url) != _safe_origin(self._config.data_plane_url):
            raise ValueError(
                "Native events URL is outside the configured data-plane origin"
            )
        return url

    def _json_request(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, object] | None = None,
        mutation: str | None = None,
        expected_status: int,
        timeout: float | None = None,
        authenticated: bool = True,
    ) -> dict[str, object]:
        headers = {"Accept": "application/json"}
        if authenticated:
            headers["Authorization"] = f"Bearer {self._config.token}"
        if mutation is not None:
            headers["Idempotency-Key"] = f"space-smoke-{mutation}-{uuid.uuid4().hex}"
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, separators=(",", ":")).encode()
        request = Request(
            f"{self._config.api_url.rstrip('/')}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        with _URL_OPEN(
            request, timeout=timeout or self._config.request_timeout_seconds
        ) as response:
            if response.status != expected_status:
                raise RuntimeError(
                    f"Native {method} {path} returned HTTP {response.status}"
                )
            payload = {} if expected_status == 204 else json.load(response)
        if not isinstance(payload, dict):
            raise TypeError(f"Native {method} {path} returned non-object JSON")
        return cast(dict[str, object], payload)


@dataclass(frozen=True)
class _NativeSandbox:
    _factory: NativeSandboxFactory
    sandbox_id: str

    @property
    def reports_exited_processes(self) -> bool:
        return self._factory.config.reports_exited_processes

    def get_info(self) -> SandboxInfo:
        response = self._factory._json_request(
            "GET",
            f"/v1/sandboxes/{quote(self.sandbox_id, safe='')}",
            expected_status=200,
        )
        return SandboxInfo(sandbox_id=_required_string(response, "id"))

    def try_get_info(self) -> SandboxInfo | None:
        try:
            return self.get_info()
        except HTTPError as error:
            if error.code == 404:
                return None
            raise

    def run(self, command: str, *, timeout: int = 60) -> CommandResult:
        return self._factory.run(self.sandbox_id, command, timeout=timeout)

    def register_credential_reference(self, secret_arn: str) -> str:
        return self._factory.register_credential_reference(secret_arn)

    def set_network_policy(self, policy: Mapping[str, object]) -> None:
        self._factory.set_network_policy(self.sandbox_id, policy)

    def get_network_policy(self) -> dict[str, object]:
        return self._factory.get_network_policy(self.sandbox_id)

    def retire_credential_reference(self, credential_reference_id: str) -> None:
        self._factory.retire_credential_reference(credential_reference_id)

    def suspend(self) -> None:
        self._factory.suspend(self.sandbox_id)

    def pause(self) -> None:
        self._factory.pause(self.sandbox_id)

    def wake(self) -> None:
        self._factory.wake(self.sandbox_id)

    def stop(self) -> None:
        self._factory.stop(self.sandbox_id)

    def start(self) -> None:
        self._factory.start(self.sandbox_id)

    def start_background(self, command: str, *, timeout: int = 60) -> ProcessHandle:
        return self._factory.start_background(self.sandbox_id, command, timeout=timeout)

    def list_processes(self) -> tuple[ProcessInfo, ...]:
        return self._factory.list_processes(self.sandbox_id)

    def get_process(self, process_id: str) -> ProcessInfo | None:
        return self._factory.get_process(self.sandbox_id, process_id)

    def signal_process(self, process_id: str, signal: int) -> bool:
        return self._factory.signal_process(self.sandbox_id, process_id, signal)

    def kill_process(self, process_id: str) -> bool:
        return self._factory.kill_process(self.sandbox_id, process_id)


def _safe_origin(url: str) -> tuple[str, str, int]:
    parsed = urlsplit(url)
    if parsed.hostname is None or parsed.username is not None:
        raise ValueError("Native URL must be absolute and credential-free")
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    ):
        raise ValueError("Native URLs must use HTTPS or a loopback HTTP endpoint")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return parsed.scheme, parsed.hostname, port


def _sse_frames(stream: BinaryIO) -> Iterator[dict[str, object]]:
    data_lines: list[str] = []
    for raw_line in stream:
        line = raw_line.decode().rstrip("\r\n")
        if line:
            if line.startswith("data:"):
                data_lines.append(line.removeprefix("data:").lstrip())
            continue
        if data_lines:
            payload = json.loads("\n".join(data_lines))
            if not isinstance(payload, dict):
                raise TypeError("Native SSE data was not a JSON object")
            yield cast(dict[str, object], payload)
            data_lines = []
    if data_lines:
        payload = json.loads("\n".join(data_lines))
        if not isinstance(payload, dict):
            raise TypeError("Native SSE data was not a JSON object")
        yield cast(dict[str, object], payload)


def _required_object(payload: Mapping[str, object], name: str) -> Mapping[str, object]:
    value = payload.get(name)
    if not isinstance(value, dict):
        raise TypeError(f"Native response field {name!r} was not an object")
    return cast(dict[str, object], value)


def _page_item(value: object, index: int) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"Native sandbox page item {index} was not an object")
    return cast(dict[str, object], value)


def _process_item(value: object, index: int) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"Native process list item {index} was not an object")
    return cast(dict[str, object], value)


def _native_process(payload: Mapping[str, object]) -> ProcessInfo:
    command = payload.get("command")
    if not isinstance(command, list) or not all(
        isinstance(arg, str) for arg in command
    ):
        raise TypeError(
            "Native process response field 'command' was not a string array"
        )
    # Optional: older APIs report running processes without these fields.
    state = payload.get("state", "running")
    exit_status = payload.get("exit_status")
    signal = payload.get("signal")
    if not isinstance(state, str) or not state:
        raise TypeError("Native process response field 'state' was not a string")
    if exit_status is not None and not isinstance(exit_status, str):
        raise TypeError("Native process response field 'exit_status' was not a string")
    if signal is not None and (not isinstance(signal, int) or isinstance(signal, bool)):
        raise TypeError("Native process response field 'signal' was not an integer")
    return ProcessInfo(
        process_id=_required_string(payload, "process_id"),
        pid=_required_integer(payload, "pid"),
        command=tuple(cast(list[str], command)),
        state=state,
        exit_status=exit_status,
        signal=signal,
    )


def _required_string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise TypeError(f"Native response field {name!r} was not a string")
    return value


def _optional_string(payload: Mapping[str, object], name: str) -> str | None:
    if name not in payload:
        raise TypeError(f"Native response omitted field {name!r}")
    value = payload[name]
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise TypeError(f"Native response field {name!r} was not a string or null")
    return value


def _required_integer(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int):
        raise TypeError(f"Native response field {name!r} was not an integer")
    return value


def _required_bool(payload: Mapping[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"Native response field {name!r} was not a boolean")
    return value
