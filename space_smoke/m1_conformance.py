"""Exercise an SDK generated from the served M1 document against SPACE.

`scripts/check-m1-sdk-generation.sh` generates the package (`openapi_client`)
from the document the API serves; it is imported from that directory for one
run, never by scenario discovery. Everything a run raises is a contract
violation (`InvariantViolation`) unless it is a transport failure, a transient
HTTP status, a timeout, or a generator that could not run: those stay
infrastructure errors.

The typed scenario `native.m1_generated_sdk_conformance` runs this in the
suite; `python -m space_smoke.m1_conformance` runs it alone against any API
(`scripts/tests/smoke-space-m1-conformance.sh`).
"""

from __future__ import annotations

import base64
import importlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import urllib3

from space_smoke.native import is_suspend_upload_conflict
from space_smoke.runtime import REPO_ROOT, InvariantViolation, ScenarioTimedOut, _sigterm_raises

GENERATION_SCRIPT = REPO_ROOT / "scripts" / "check-m1-sdk-generation.sh"
# check-m1-sdk-generation.sh's exit status when the generator could not run.
GENERATOR_UNAVAILABLE = 69
GENERATOR_LABEL_KEY = "space-smoke.m1-generator"
GENERATOR_TIMEOUT_SECONDS = 900
DEFAULT_RUNTIME = "fc"
# Statuses a retry can clear; 0 is the generated client's TLS or connection
# failure. One is the environment's only when no SPACE error envelope came
# with it (a gateway or ingress answered) or the envelope says it is
# retryable; see transient_infrastructure.
TRANSIENT_HTTP_STATUSES = frozenset({0, 429, 502, 503, 504})
# How long the generation script's process group has to exit after SIGTERM.
_GENERATION_GRACE_SECONDS = 10

EXPECTED_OPERATIONS = frozenset({
    ("GET", "/operations/{operation_id}", "getOperation"),
    ("GET", "/sandboxes", "listSandboxes"),
    ("POST", "/sandboxes", "createSandbox"),
    ("DELETE", "/sandboxes/{sandbox_id}", "deleteSandbox"),
    ("GET", "/sandboxes/{sandbox_id}", "getSandbox"),
    ("POST", "/sandboxes/{sandbox_id}/commands", "createCommand"),
    ("GET", "/sandboxes/{sandbox_id}/processes", "listProcesses"),
    ("GET", "/sandboxes/{sandbox_id}/processes/{process_id}", "getProcess"),
    ("POST", "/sandboxes/{sandbox_id}/processes/{process_id}:kill", "killProcess"),
    ("POST", "/sandboxes/{sandbox_id}/processes/{process_id}:signal", "signalProcess"),
    ("POST", "/sandboxes/{sandbox_id}:pause", "pauseSandbox"),
    ("POST", "/sandboxes/{sandbox_id}:suspend", "suspendSandbox"),
    ("POST", "/sandboxes/{sandbox_id}:wake", "wakeSandbox"),
    ("GET", "/sessions/{session_id}/events", "streamSessionEvents"),
})
HTTP_METHODS = frozenset({"delete", "get", "head", "options", "patch", "post", "put", "trace"})
TERMINAL_OPERATION_STATES = frozenset({"canceled", "expired", "failed", "succeeded"})

# Response vocabularies are extensible strings in the contract (the generated
# client sees `str`, not enums), so this smoke pins the values the server is
# known to emit today. A new value fails here on purpose: it means the public
# docs and this list need updating, not that the server is wrong.
KNOWN_OPERATION_STATES = frozenset({"queued", "running", *TERMINAL_OPERATION_STATES})
KNOWN_SANDBOX_STATES = frozenset({
    "creating", "stopped", "starting", "running", "pausing", "paused", "resuming",
    "suspending", "suspended", "restoring", "migrating", "stopping", "failed",
    "deleting", "deleted",
})
KNOWN_SESSION_STATES = frozenset({
    "pending", "attaching", "open", "reconnecting", "closing", "closed", "revoked",
    "failed",
})
KNOWN_PROCESS_EXIT_STATUSES = frozenset({"exited", "signaled", "timed_out", "unknown"})
KNOWN_PUBLIC_ERROR_CODES = frozenset({
    "invalid_request", "unauthorized", "forbidden", "not_found", "conflict",
    "failed_precondition", "capacity_unavailable", "rate_limited", "quota_exceeded",
    "idempotency_conflict", "operation_failed", "service_unavailable", "internal",
})
MARKER = b"m1-conformance"


def assert_known(field_name: str, value: object, known: Iterable[str]) -> None:
    known = frozenset(known)
    if not isinstance(value, str) or value not in known:
        raise InvariantViolation(
            f"{field_name} emitted {value!r}, not one of the documented values {sorted(known)}"
        )


def check_operations(document: object) -> None:
    """The served document declares exactly the 14 M1 release operations."""

    paths = document.get("paths") if isinstance(document, dict) else None
    if not isinstance(paths, dict):
        raise InvariantViolation("served M1 document has no paths object")
    actual = set()
    for route, path_item in paths.items():
        if not isinstance(path_item, dict):
            raise InvariantViolation(f"served M1 path {route!r} is not an object")
        for method, operation in path_item.items():
            if method not in HTTP_METHODS:
                continue
            if not isinstance(operation, dict):
                raise InvariantViolation(f"served M1 operation {method} {route!r} is not an object")
            operation_id = operation.get("operationId")
            if not isinstance(operation_id, str):
                raise InvariantViolation(
                    f"served M1 operation {method} {route!r} has no string operationId: "
                    f"{operation_id!r}"
                )
            actual.add((method.upper(), route, operation_id))
    if actual != EXPECTED_OPERATIONS:
        raise InvariantViolation(
            "served M1 operation mismatch: "
            f"missing={sorted(EXPECTED_OPERATIONS - actual)}, "
            f"unexpected={sorted(actual - EXPECTED_OPERATIONS)}"
        )


def fetch_document(
    api_url: str, *, timeout: float, open_url: Callable[..., Any] = urlopen
) -> bytes:
    """The served /openapi-m1.json, checked to declare exactly the M1 operations."""

    url = f"{api_url.rstrip('/')}/openapi-m1.json"
    try:
        with open_url(Request(url, headers={"Accept": "application/json"}), timeout=timeout) as response:
            raw = response.read()
    except HTTPError as error:
        body = error.read()
        if transient_infrastructure(error.code, body):
            raise
        raise InvariantViolation(
            f"the API did not serve /openapi-m1.json: HTTP {error.code}: {body[:500]!r}"
        ) from None
    try:
        document = json.loads(raw)
    except ValueError:
        raise InvariantViolation("/openapi-m1.json is not a JSON document") from None
    check_operations(document)
    return raw


def iter_sse_events(stream: Iterable[bytes]) -> Iterator[dict[str, object]]:
    """Parse a text/event-stream body into {event, id, data} dicts.

    Preserves the `id:` field so callers can resume with `Last-Event-ID`;
    comments (heartbeats) are skipped; `data:` is decoded as JSON.
    """
    event_type: str | None = None
    event_id: str | None = None
    data_lines: list[bytes] = []
    for raw_line in stream:
        line = raw_line.rstrip(b"\r\n")
        if not line:
            if data_lines:
                yield {
                    "event": event_type,
                    "id": int(event_id) if event_id is not None else None,
                    "data": json.loads(b"\n".join(data_lines)),
                }
            event_type, event_id, data_lines = None, None, []
            continue
        if line.startswith(b":"):
            continue
        name, _, value = line.partition(b":")
        value = value.lstrip(b" ")
        if name == b"event":
            event_type = value.decode()
        elif name == b"id":
            event_id = value.decode()
        elif name == b"data":
            data_lines.append(value)


def stdout_of(frames: list[dict[str, object]]) -> bytes:
    return b"".join(
        base64.b64decode(cast(str, frame["data_base64"]), validate=True)
        for frame in frames
        if frame.get("type") == "stdout"
    )


class GeneratorUnavailable(RuntimeError):
    """The pinned generator could not run: an environment failure, not a contract one."""


class GenerationTimedOut(TimeoutError):
    """SDK generation outlived its budget; `output` is what it printed until then."""

    def __init__(self, message: str, output: str) -> None:
        super().__init__(message)
        self.output = output


@dataclass(frozen=True)
class Generation:
    returncode: int
    output: str

    def require_sdk(self) -> None:
        if self.returncode == GENERATOR_UNAVAILABLE:
            raise GeneratorUnavailable("the pinned OpenAPI Generator could not run")
        if self.returncode != 0:
            raise InvariantViolation(
                "the pinned OpenAPI Generator did not produce a compilable SDK with the 14 "
                "M1 operations from the served document"
            )


def new_generator_label() -> str:
    return f"{GENERATOR_LABEL_KEY}={uuid.uuid4().hex}"


def generate_sdk(spec: Path, output: Path, *, timeout: float, label: str) -> Generation:
    """Run check-m1-sdk-generation.sh on `spec`, writing the SDK into `output`.

    The script runs in its own process group under `label`. However this
    returns (a timeout, the scenario's deadline, SIGTERM), the group is ended
    and the labelled generator container removed, since the script's own
    cleanup does not run when it is killed.
    """

    environment = dict(
        os.environ,
        SPACE_M1_GENERATED_SDK_DIR=str(output),
        SPACE_M1_GENERATOR_LABEL=label,
        TMPDIR=str(output.parent),
    )
    process = subprocess.Popen(
        ["bash", str(GENERATION_SCRIPT), str(spec)],
        cwd=REPO_ROOT,
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        try:
            stdout, _ = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            _end_process_group(process)
            partial, _ = process.communicate()
            raise GenerationTimedOut(
                f"SDK generation did not finish in {timeout:g} s", partial or ""
            ) from None
    finally:
        _end_process_group(process)
        remove_generator_containers(label, strict=False)
    return Generation(process.returncode, stdout)


def _end_process_group(process: subprocess.Popen[str]) -> None:
    """End every process left in the script's group, leader or not."""

    group = process.pid
    try:
        os.killpg(group, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + _GENERATION_GRACE_SECONDS
    while time.monotonic() < deadline:
        process.poll()  # reap the leader, so only live members keep the group
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    try:
        os.killpg(group, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def remove_generator_containers(label: str, *, strict: bool) -> None:
    """Remove the generator containers carrying `label`; with `strict`, fail if any remain."""

    try:
        listed = subprocess.run(
            ["docker", "ps", "--all", "--quiet", "--filter", f"label={label}"],
            capture_output=True, text=True, timeout=30, check=True,
        )
        containers = listed.stdout.split()
        if containers:
            subprocess.run(
                ["docker", "rm", "--force", *containers],
                capture_output=True, text=True, timeout=60, check=True,
            )
    except (OSError, subprocess.SubprocessError):
        if strict:
            raise


class GeneratedSdkLocationError(RuntimeError):
    """`openapi_client` came from somewhere other than the generated directory."""


@dataclass(frozen=True)
class GeneratedSdk:
    """The classes of one generated `openapi_client` this check uses."""

    ApiClient: Any
    Configuration: Any
    DefaultApi: Any
    ApiException: type[Exception]
    CreateCommandRequest: Any
    CreateSandboxRequest: Any
    ProcessSignalRequest: Any


def _generated_module(name: str) -> bool:
    return name == "openapi_client" or name.startswith("openapi_client.")


@contextmanager
def generated_sdk(directory: Path) -> Iterator[GeneratedSdk]:
    """Import the `openapi_client` generated into `directory` for this block.

    Any `openapi_client` modules and the `sys.path` from before are restored
    afterwards, and the generated modules are unloaded, whether the block
    succeeds or fails.
    """

    saved_modules = {name: module for name, module in sys.modules.items() if _generated_module(name)}
    saved_path = list(sys.path)
    for name in saved_modules:
        del sys.modules[name]
    sys.path.insert(0, str(directory))
    importlib.invalidate_caches()
    try:
        # A generated package without the modules or names below means the
        # served document no longer yields the client this check drives.
        with contract_errors():
            client = importlib.import_module("openapi_client")
            if not Path(client.__file__).resolve().is_relative_to(directory.resolve()):
                raise GeneratedSdkLocationError(
                    f"imported openapi_client from {client.__file__}, not {directory}"
                )
            api = importlib.import_module("openapi_client.api.default_api")
            exceptions = importlib.import_module("openapi_client.exceptions")
            models = {
                name: getattr(importlib.import_module(f"openapi_client.models.{module}"), name)
                for name, module in (
                    ("CreateCommandRequest", "create_command_request"),
                    ("CreateSandboxRequest", "create_sandbox_request"),
                    ("ProcessSignalRequest", "process_signal_request"),
                )
            }
            sdk = GeneratedSdk(
                ApiClient=client.ApiClient,
                Configuration=client.Configuration,
                DefaultApi=api.DefaultApi,
                ApiException=exceptions.ApiException,
                **models,
            )
        yield sdk
    finally:
        for name in [name for name in sys.modules if _generated_module(name)]:
            del sys.modules[name]
        sys.modules.update(saved_modules)
        sys.path[:] = saved_path


def is_infrastructure(error: BaseException, sdk: GeneratedSdk | None = None) -> bool:
    """Whether an error is the environment's, not the contract's."""

    if isinstance(
        error,
        (
            OSError,
            urllib3.exceptions.HTTPError,
            ScenarioTimedOut,
            GeneratorUnavailable,
            GeneratedSdkLocationError,
        ),
    ):
        return True
    return (
        sdk is not None
        and isinstance(error, sdk.ApiException)
        and transient_infrastructure(getattr(error, "status", None), getattr(error, "body", None))
    )


def transient_infrastructure(status: int | None, body: object) -> bool:
    """Whether an HTTP answer is a transient environment failure, not a contract one.

    A transient status (TRANSIENT_HTTP_STATUSES) qualifies when no SPACE error
    envelope came with it, which means a gateway or ingress answered, or when
    the envelope itself says the error is retryable. A SPACE envelope that
    calls a 503 permanent is the server's answer, and the contract's to judge.
    """

    if (status or 0) not in TRANSIENT_HTTP_STATUSES:
        return False
    envelope = _space_envelope(body)
    return envelope is None or envelope.get("retryable") is True


def _space_envelope(body: object) -> dict[str, object] | None:
    if isinstance(body, bytes):
        body = body.decode(errors="replace")
    if not isinstance(body, str):
        return None
    try:
        envelope = json.loads(body)
    except ValueError:
        return None
    if isinstance(envelope, dict) and isinstance(envelope.get("code"), str) and "request_id" in envelope:
        return envelope
    return None


@contextmanager
def contract_errors(sdk: GeneratedSdk | None = None) -> Iterator[None]:
    """Raise anything but an infrastructure error from the block as a contract violation."""

    try:
        yield
    except InvariantViolation:
        raise
    except Exception as error:
        if is_infrastructure(error, sdk):
            raise
        raise InvariantViolation(_describe(error, sdk)) from error


def _describe(error: Exception, sdk: GeneratedSdk | None) -> str:
    if sdk is not None and isinstance(error, sdk.ApiException):
        body = str(getattr(error, "body", None) or "")[:500]
        return f"a generated SDK call answered HTTP {getattr(error, 'status', None)}: {body}"
    # pydantic is the generated client's model layer.
    from pydantic import ValidationError

    if isinstance(error, ValidationError):
        first = error.errors()[0].get("loc") if error.error_count() else None
        return (
            f"the generated SDK could not parse a response: {error.error_count()} "
            f"validation errors, first at {first}"
        )
    return f"the M1 contract was not met: {type(error).__name__}: {str(error)[:500]}"


def error_envelope(error: BaseException) -> dict[str, object]:
    """The public error envelope an error response carried."""

    try:
        envelope = json.loads(getattr(error, "body", None) or "{}")
    except ValueError:
        raise InvariantViolation("an error response is not a JSON document") from None
    if not isinstance(envelope, dict):
        raise InvariantViolation(f"an error response is not a JSON object: {envelope!r}")
    return envelope


@dataclass
class SessionRead:
    """What one SSE connection delivered, for reconnection assertions."""

    last_id: int | None = None
    attached_first: bool = False
    identified: list[tuple[int, dict[str, object]]] = field(default_factory=list)


class M1Conformance:
    """Drive all 14 M1 operations through the generated SDK.

    `run` leaves the sandbox it created deleted and verified absent;
    `cleanup` deletes it after a failure, and raises when it cannot.
    """

    def __init__(
        self,
        sdk: GeneratedSdk,
        *,
        api_url: str,
        data_plane_url: str,
        token: str,
        template: str,
        runtime: str,
        timeout: float,
        request_timeout: float,
        on_sandbox: Callable[[str], None] = lambda _sandbox_id: None,
    ) -> None:
        self.sdk = sdk
        # A generated client whose construction changed breaks the contract.
        with contract_errors(sdk):
            self.client = sdk.ApiClient(
                sdk.Configuration(host=f"{api_url.rstrip('/')}/v1", access_token=token)
            )
            self.api = sdk.DefaultApi(self.client)
        self.data_plane_url = data_plane_url.rstrip("/")
        self.template = template
        self.runtime = runtime
        self.timeout = timeout
        self.request_timeout = request_timeout
        self.on_sandbox = on_sandbox
        self.called: set[str] = set()
        self.sandbox_id: str | None = None
        self.create_key: str | None = None
        # Whether the create's outcome is known: an accepted response, or a
        # definitive rejection. Until then cleanup replays it to find out.
        self.create_settled = False

    @staticmethod
    def key(label: str) -> str:
        return f"m1-conformance-{label}-{uuid.uuid4()}"

    def create_request(self, **fields: object) -> Any:
        return self.sdk.CreateSandboxRequest(
            template=self.template, runtime=self.runtime, **fields
        )

    def wait_operation(
        self, operation_id: str, *, require_success: bool = True
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            operation = self.api.get_operation(operation_id, _request_timeout=self.request_timeout)
            self.called.add("getOperation")
            state = operation.state
            assert_known("operation.state", state, KNOWN_OPERATION_STATES)
            if state in TERMINAL_OPERATION_STATES:
                record = operation.to_dict()
                if require_success and state != "succeeded":
                    raise InvariantViolation(f"operation {operation_id} ended in {state}: {record}")
                return record
            time.sleep(1)
        raise TimeoutError(f"operation {operation_id} did not complete in {self.timeout:g}s")

    def wait_sandbox_state(self, *expected: str) -> None:
        assert self.sandbox_id is not None
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            sandbox = self.api.get_sandbox(self.sandbox_id, _request_timeout=self.request_timeout)
            self.called.add("getSandbox")
            state = sandbox.state
            assert_known("sandbox.state", state, KNOWN_SANDBOX_STATES)
            if state in expected:
                return
            if state in {"deleted", "deleting", "failed"}:
                raise InvariantViolation(f"sandbox entered {state} while waiting for {expected}")
            time.sleep(1)
        raise TimeoutError(f"sandbox did not reach {expected} in {self.timeout:g}s")

    def wait_process(self, process_id: str) -> None:
        assert self.sandbox_id is not None
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            page = self.api.list_processes(
                self.sandbox_id, limit=1000, _request_timeout=self.request_timeout
            )
            self.called.add("listProcesses")
            if any(process.process_id == process_id for process in page.data):
                return
            time.sleep(1)
        raise TimeoutError(f"process {process_id} was not listed in {self.timeout:g}s")

    def mutate_sandbox(self, operation_id: str, method: Callable[..., Any]) -> None:
        assert self.sandbox_id is not None
        response = method(
            self.key(operation_id), self.sandbox_id, _request_timeout=self.request_timeout
        )
        self.called.add(operation_id)
        self.wait_operation(response.operation_id)

    def exercise_command(self, command: Any) -> None:
        assert_known("command.state", command.state, KNOWN_SESSION_STATES)
        if command.attach is None:
            raise InvariantViolation("createCommand did not return data-plane attach credentials")
        if not command.process_id:
            raise InvariantViolation("createCommand did not return a process_id")

        configuration = self.sdk.Configuration(
            host=f"{self.data_plane_url}/v1", access_token=command.attach.token
        )
        frames: list[dict[str, object]] = []
        with self.sdk.ApiClient(configuration) as client:
            api = self.sdk.DefaultApi(client)

            # First connection: read until the process is running and its
            # first output has arrived, then drop the connection on purpose.
            first = self.read_session_events(
                api, command.session_id, None, frames,
                stop=lambda: MARKER in stdout_of(frames)
                and any(frame.get("type") == "process_started" for frame in frames),
            )
            self.called.add("streamSessionEvents")
            if first.last_id is None:
                raise InvariantViolation("first connection delivered no identified events")
            streamed_process_id = next(
                (
                    cast(str, frame["process_id"])
                    for frame in frames
                    if frame.get("type") == "process_started"
                ),
                None,
            )
            # The guest process is spawned by the attach above, so the id can
            # only be resolved now -- but it is resolved from the createCommand
            # response, not from the frame, so a wrong id fails here and in the
            # getProcess/signal/kill calls that exercise_process drives below.
            if streamed_process_id != command.process_id:
                raise InvariantViolation(
                    f"createCommand process_id {command.process_id!r} did not "
                    f"match process_started {streamed_process_id!r}"
                )

            # Reconnect with Last-Event-ID. The documented guarantee: the stream
            # resumes with the output after that event, without a resync while
            # the output is retained, and the process is still controllable.
            resumed: list[dict[str, object]] = []
            second = self.read_session_events(
                api, command.session_id, first.last_id, resumed,
                stop=lambda: any(frame.get("type") == "process_exited" for frame in resumed),
                on_connected=lambda: self.exercise_process(command.process_id),
            )
            if not second.attached_first:
                raise InvariantViolation("reconnected stream did not start with an attached frame")
            # Only output and resync_required carry ids, so a resume that sends
            # neither has none; any it does send must follow Last-Event-ID.
            if second.identified and second.identified[0][0] <= first.last_id:
                raise InvariantViolation(
                    f"resumed stream restarted ids at {second.identified[0][0]}, "
                    f"after {first.last_id}"
                )
            resyncs = [frame for frame in resumed if frame.get("type") == "resync_required"]
            if resyncs:
                raise InvariantViolation(f"lossless Last-Event-ID resume asked to resync: {resyncs}")
            frames.extend(resumed)

        exits = [frame for frame in frames if frame.get("type") == "process_exited"]
        if not exits:
            raise InvariantViolation("session event stream did not report process_exited")
        for frame in exits:
            assert_known("process_exited.status", frame.get("status"), KNOWN_PROCESS_EXIT_STATUSES)
        stdout = stdout_of(frames)
        if stdout.count(MARKER) != 1:
            raise InvariantViolation(
                f"session event streams did not carry the command output once: {stdout!r}"
            )

    def read_session_events(
        self,
        api: Any,
        session_id: str,
        last_event_id: int | None,
        sink: list[dict[str, object]],
        stop: Callable[[], bool],
        on_connected: Callable[[], None] | None = None,
    ) -> SessionRead:
        """Read one SSE connection until `stop()` holds, asserting id monotonicity.

        Every identified frame must carry a greater id than the one before, and
        on a process stream only output and resync_required frames carry one;
        `on_connected` runs once the stream reports the process (started or
        reconnected), so process control can be exercised while attached.
        """
        kwargs: dict[str, object] = {"_request_timeout": (10, self.timeout)}
        if last_event_id is not None:
            kwargs["last_event_id"] = last_event_id
        stream = api.stream_session_events_without_preload_content(session_id, **kwargs)
        result = SessionRead()
        first = True
        try:
            for event in iter_sse_events(stream):
                frame = cast(dict[str, object], event["data"])
                frame_id = cast(int | None, event["id"])
                if first:
                    result.attached_first = frame.get("type") == "attached" and frame_id is None
                    first = False
                if (frame_id is not None) != (
                    frame.get("type") in {"stdout", "stderr", "resync_required"}
                ):
                    raise InvariantViolation(f"unexpected SSE id {frame_id} on {frame}")
                if frame_id is not None:
                    if result.last_id is not None and frame_id <= result.last_id:
                        raise InvariantViolation(
                            f"SSE ids must increase: got {frame_id} after {result.last_id}"
                        )
                    result.last_id = frame_id
                    result.identified.append((frame_id, frame))
                if event["event"] is not None and event["event"] != frame.get("type"):
                    raise InvariantViolation(
                        f"SSE event name {event['event']!r} disagrees with frame {frame}"
                    )
                sink.append(frame)
                if frame.get("type") == "error":
                    raise InvariantViolation(f"session event stream reported an error: {frame}")
                if frame.get("type") == "process_started" and on_connected is not None:
                    on_connected()
                    on_connected = None
                if stop():
                    break
        finally:
            stream.close()
        return result

    def exercise_process(self, process_id: str) -> None:
        assert self.sandbox_id is not None
        self.wait_process(process_id)
        process = self.api.get_process(
            self.sandbox_id, process_id, _request_timeout=self.request_timeout
        )
        self.called.add("getProcess")
        if process.process_id != process_id:
            raise InvariantViolation("getProcess returned a different process")

        signaled = self.api.signal_process(
            self.sandbox_id,
            process_id,
            self.sdk.ProcessSignalRequest(signal=18),
            _request_timeout=self.request_timeout,
        )
        self.called.add("signalProcess")
        if not signaled.ok:
            raise InvariantViolation("signalProcess returned ok=false")
        killed = self.api.kill_process(
            self.sandbox_id, process_id, _request_timeout=self.request_timeout
        )
        self.called.add("killProcess")
        if not killed.ok:
            raise InvariantViolation("killProcess returned ok=false")

    def run(self) -> None:
        with contract_errors(self.sdk):
            self._run()

    def _run(self) -> None:
        self.create_key = self.key("create")
        try:
            create = self.api.create_sandbox(
                self.create_key, self.create_request(), _request_timeout=self.request_timeout
            )
        except self.sdk.ApiException as error:
            status = getattr(error, "status", None) or 0
            # A definitive rejection created nothing; conflicts, rate limits
            # and server errors can follow a create that was committed.
            self.create_settled = 400 <= status < 500 and status not in {408, 409, 429}
            raise
        self.called.add("createSandbox")
        self.sandbox_id = create.sandbox_id
        self.create_settled = True
        self.on_sandbox(self.sandbox_id)
        self.wait_operation(create.operation_id)

        sandbox = self.api.get_sandbox(self.sandbox_id, _request_timeout=self.request_timeout)
        self.called.add("getSandbox")
        page = self.api.list_sandboxes(limit=1000, _request_timeout=self.request_timeout)
        self.called.add("listSandboxes")
        if not any(candidate.id == sandbox.id for candidate in page.data):
            raise InvariantViolation(f"new sandbox {self.sandbox_id} was absent from listSandboxes")

        if sandbox.state != "running":
            self.mutate_sandbox("wakeSandbox", self.api.wake_sandbox)
            self.wait_sandbox_state("running")

        command = self.api.create_command(
            self.key("command"),
            self.sandbox_id,
            self.sdk.CreateCommandRequest(
                command=["sh", "-lc", f"printf {MARKER.decode()}; sleep 300"],
                timeout="10m",
            ),
            _request_timeout=self.request_timeout,
        )
        self.called.add("createCommand")
        self.exercise_command(command)
        self.exercise_negative_paths()

        self.mutate_sandbox("pauseSandbox", self.api.pause_sandbox)
        self.wait_sandbox_state("paused", "suspended")
        self.mutate_sandbox("wakeSandbox", self.api.wake_sandbox)
        self.wait_sandbox_state("running")
        self.mutate_sandbox("suspendSandbox", self.api.suspend_sandbox)
        self.wait_sandbox_state("suspended")
        self.mutate_sandbox("wakeSandbox", self.api.wake_sandbox)
        self.wait_sandbox_state("running")

        self.delete_sandbox(cleanup=False)
        try:
            self.api.get_sandbox(self.sandbox_id, _request_timeout=self.request_timeout)
        except self.sdk.ApiException as error:
            if getattr(error, "status", None) != 404:
                raise
            envelope = error_envelope(error)
            assert_known("error.code", envelope.get("code"), KNOWN_PUBLIC_ERROR_CODES)
            if envelope.get("code") != "not_found":
                raise InvariantViolation(
                    f"deleted sandbox returned code {envelope.get('code')!r}"
                ) from error
        else:
            raise InvariantViolation("deleted sandbox remained readable")
        self.sandbox_id = None

        expected_ids = {operation_id for _, _, operation_id in EXPECTED_OPERATIONS}
        if self.called != expected_ids:
            raise InvariantViolation(
                f"runtime operation mismatch: missing={sorted(expected_ids - self.called)}, "
                f"unexpected={sorted(self.called - expected_ids)}"
            )

    def expect_error(
        self,
        label: str,
        call: Callable[[], object],
        status: int,
        code: str,
        retryable: bool,
    ) -> None:
        """Assert a negative path answers with the documented envelope."""
        try:
            call()
        except self.sdk.ApiException as error:
            actual = getattr(error, "status", None)
            if actual != status:
                if transient_infrastructure(actual, getattr(error, "body", None)):
                    raise
                raise InvariantViolation(f"{label}: expected HTTP {status}, got {actual}") from error
            envelope = error_envelope(error)
            assert_known("error.code", envelope.get("code"), KNOWN_PUBLIC_ERROR_CODES)
            if envelope.get("code") != code or envelope.get("retryable") is not retryable:
                raise InvariantViolation(
                    f"{label}: expected {code} retryable={retryable}, got {envelope}"
                ) from error
            if not envelope.get("request_id"):
                raise InvariantViolation(f"{label}: envelope carries no request_id: {envelope}") from error
            headers = {
                key.lower(): value for key, value in (getattr(error, "headers", None) or {}).items()
            }
            if not retryable and "retry-after" in headers:
                raise InvariantViolation(f"{label}: non-retryable error advertised Retry-After") from error
            return
        raise InvariantViolation(f"{label}: expected HTTP {status} {code}, but the call succeeded")

    def exercise_negative_paths(self) -> None:
        """Errors the contract documents and a client can trigger deterministically."""
        assert self.sandbox_id is not None and self.create_key is not None
        self.expect_error(
            "idempotency key reused with a different body",
            lambda: self.api.create_sandbox(
                self.create_key,
                self.create_request(name="conflict"),
                _request_timeout=self.request_timeout,
            ),
            409,
            "idempotency_conflict",
            retryable=False,
        )
        self.expect_error(
            "unknown sandbox",
            lambda: self.api.get_sandbox(
                "sbx_00000000-0000-7000-8000-000000000000", _request_timeout=self.request_timeout
            ),
            404,
            "not_found",
            retryable=False,
        )
        self.expect_error(
            "malformed sandbox id",
            lambda: self.api.get_sandbox("not-a-sandbox-id", _request_timeout=self.request_timeout),
            400,
            "invalid_request",
            retryable=False,
        )
        unauthenticated = self.sdk.DefaultApi(
            self.sdk.ApiClient(
                self.sdk.Configuration(
                    host=self.client.configuration.host, access_token="invalid-token"
                )
            )
        )
        self.expect_error(
            "invalid credential",
            lambda: unauthenticated.get_sandbox(
                self.sandbox_id, _request_timeout=self.request_timeout
            ),
            401,
            "unauthorized",
            retryable=False,
        )

    def delete_sandbox(self, *, cleanup: bool) -> None:
        """Delete the sandbox and wait for the operation to succeed.

        A delete that fails only because a suspend's snapshot upload is still
        in flight is retried with a fresh key within the deadline, as
        `NativeSandboxFactory.delete` does. The first DELETE is the contract's
        `deleteSandbox` call; cleanup also accepts an absent sandbox and
        retries a busy API.
        """

        assert self.sandbox_id is not None
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                deleted = self.api.delete_sandbox(
                    self.key("delete"), self.sandbox_id, _request_timeout=self.request_timeout
                )
            except self.sdk.ApiException as error:
                status = getattr(error, "status", None)
                if cleanup and status == 404:
                    return
                if (
                    cleanup
                    and status in {409, 429, 500, 502, 503, 504}
                    and time.monotonic() < deadline
                ):
                    time.sleep(1)
                    continue
                raise
            if not cleanup:
                self.called.add("deleteSandbox")
            operation = self.wait_operation(deleted.operation_id, require_success=False)
            if operation.get("state") == "succeeded":
                return
            if not is_suspend_upload_conflict(operation):
                raise InvariantViolation(
                    f"delete operation {deleted.operation_id} ended in "
                    f"{operation.get('state')}: {operation}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError("the suspend-snapshot upload holding off the delete did not finalize")
            time.sleep(1)

    def replay_create(self) -> str | None:
        """Replay an unanswered create to find its sandbox, without the models.

        The raw response is read so a response the models reject, which left
        the create unsettled, does not stop cleanup too.
        """

        assert self.create_key is not None
        response = self.api.create_sandbox_without_preload_content(
            self.create_key, self.create_request(), _request_timeout=self.request_timeout
        )
        try:
            status = response.status
            raw = response.data
        finally:
            response.release_conn()
        if 400 <= status < 500 and status not in {408, 409, 429}:
            return None
        body = json.loads(raw) if 200 <= status < 300 else None
        sandbox_id = body.get("sandbox_id") if isinstance(body, dict) else None
        if not isinstance(sandbox_id, str) or not sandbox_id:
            raise RuntimeError(f"the replayed create answered HTTP {status} without a sandbox_id")
        return sandbox_id

    def cleanup(self) -> None:
        """Delete the sandbox `run` left behind; raise when that cannot be shown."""

        if self.sandbox_id is None and self.create_key is not None and not self.create_settled:
            self.sandbox_id = self.replay_create()
            if self.sandbox_id is not None:
                self.on_sandbox(self.sandbox_id)
        if self.sandbox_id is None:
            return
        self.delete_sandbox(cleanup=True)
        self.sandbox_id = None


def main() -> int:
    """Run the check alone against SPACE_API_URL, outside the typed suite."""

    token = os.environ.get("SPACE_API_TOKEN")
    if not token:
        print("ERROR: SPACE_API_TOKEN is required", file=sys.stderr)
        return 1
    # As in the typed runner: SIGTERM unwinds, so the generator, the temporary
    # directory and the sandbox are cleaned up when the wrapper is stopped.
    with _sigterm_raises():
        _run_alone(token, os.environ.get("SPACE_API_URL", "http://127.0.0.1:8080"))
    print("M1 generated-SDK conformance passed for all 14 release operations")
    return 0


def _run_alone(token: str, api_url: str) -> None:
    with tempfile.TemporaryDirectory(prefix="space-m1-conformance-") as work_name:
        work = Path(work_name)
        spec = work / "openapi-m1.json"
        spec.write_bytes(fetch_document(api_url, timeout=15))
        output = work / "generated"
        output.mkdir()
        label = new_generator_label()
        try:
            generation = generate_sdk(spec, output, timeout=GENERATOR_TIMEOUT_SECONDS, label=label)
        except GenerationTimedOut as error:
            print(error.output, end="", flush=True)
            raise
        print(generation.output, end="", flush=True)
        # generate_sdk already removed the container; this makes a removal
        # that failed visible.
        remove_generator_containers(label, strict=True)
        generation.require_sdk()
        with generated_sdk(output) as sdk:
            conformance = M1Conformance(
                sdk,
                api_url=api_url,
                data_plane_url=os.environ.get("SPACE_DATA_PLANE_URL", api_url),
                token=token,
                template=os.environ.get("SPACE_TEMPLATE", "seed:latest"),
                runtime=os.environ.get("SPACE_RUNTIME", DEFAULT_RUNTIME),
                timeout=float(os.environ.get("SPACE_READY_TIMEOUT_SECS", "180")),
                request_timeout=15,
            )
            try:
                conformance.run()
            except BaseException:
                try:
                    conformance.cleanup()
                except Exception as error:
                    # Cleanup diagnostics must not mask the primary failure.
                    print(
                        f"WARNING: cleanup failed for {conformance.sandbox_id}: {error}",
                        file=sys.stderr,
                    )
                raise
            conformance.cleanup()


if __name__ == "__main__":
    sys.exit(main())
