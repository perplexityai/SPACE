"""Typed node-local fixture using the existing spaced gRPC wire contract."""

from __future__ import annotations

import base64
import json
import os
import selectors
import signal
import socket
import struct
import subprocess
import tempfile
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar

from space_smoke.runtime import ScenarioContext


class SpacedEndpoint(str, Enum):
    GUEST = "guest"
    HOST = "host"

    @property
    def proto_file(self) -> str:
        if self is SpacedEndpoint.HOST:
            return "spaced/host/v1/host.proto"
        return "spaced/bridge/guest/v1/guest.proto"

    @property
    def service_prefix(self) -> str:
        if self is SpacedEndpoint.HOST:
            return "space.spaced.host.v1"
        return "space.spaced.bridge.guest.v1"


class SpacedRpcError(RuntimeError):
    def __init__(self, service: str, method: str, status: dict[str, Any]) -> None:
        self.code = int(status["code"])
        super().__init__(f"{service}/{method}: {status}")


@dataclass(frozen=True)
class SpacedProcessResult:
    process_id: str
    stdout: bytes
    stderr: bytes
    outcome: dict[str, Any]

    @property
    def exit_code(self) -> int | None:
        exited = self.outcome.get("exited")
        return int(exited.get("exitCode", 0)) if isinstance(exited, dict) else None


@dataclass(frozen=True)
class SpacedRuntimeFixture:
    """Uses normal APIs; never allocates a pod or executes a command in one."""

    fixture_name: ClassVar[str] = "spaced-runtime"
    timeout_minutes: ClassVar[int] = 5
    context: ScenarioContext
    socket: str
    endpoint: SpacedEndpoint = SpacedEndpoint.GUEST
    privileged_client: bool = False

    @contextmanager
    def contained_agent(self, program: Path, specification: Path) -> Iterator[int]:
        """Exercise the real single-threaded launcher, not supervisor admission.

        The operator supplies fresh task-owned roots and a private cgroup in
        the specification. This fixture owns the launched process lifetime,
        not catalog qualification, volume attachment or cluster scheduling.
        """
        if os.geteuid() != 0 or not program.is_absolute() or not specification.is_absolute():
            raise ValueError("contained-agent qualification requires root and absolute fixture paths")
        if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
            raise ValueError("contained-agent qualification requires a pidfd-capable Python build")
        if (
            os.environ.get("SPACE_TEST_EXECUTION_PRIVATE_CGROUP_NS") != "1"
            or Path("/proc/self/cgroup").read_text() != "0::/\n"
        ):
            raise ValueError("contained-agent qualification requires a disposable private cgroup namespace")
        if self.endpoint is not SpacedEndpoint.GUEST:
            raise ValueError("contained-agent qualification requires the guest fixture endpoint")
        with specification.open("rb") as source:
            payload = source.read(1024 * 1024 + 1)
        if len(payload) > 1024 * 1024:
            raise ValueError("contained-agent specification exceeds the fixture budget")
        launch = json.loads(payload)
        config = launch["config"]["contained"]
        if (
            launch["kind"] != "Contained"
            or config["runtime"] != "wasmtime"
            or "WasiGuest" not in config["entrypoint"]
            or Path(self.socket)
            != Path(config["registration_directory"]) / "wasi-guest/bridge-guest.sock"
        ):
            raise ValueError("fixture endpoint does not match its contained WASI launch")

        def receive_exact(channel: socket.socket, count: int) -> bytes:
            result = bytearray()
            while len(result) < count:
                chunk = channel.recv(count - len(result))
                if not chunk:
                    raise RuntimeError("contained launcher closed before reporting ownership")
                result.extend(chunk)
            return bytes(result)

        parent, child = socket.socketpair()
        observer: int | None = None
        process: subprocess.Popen[bytes] | None = None
        with tempfile.TemporaryFile() as diagnostic:
            try:
                parent.settimeout(15)
                process = subprocess.Popen(
                    [str(program), "--sidecar-launcher-fd", str(child.fileno())],
                    pass_fds=(child.fileno(),),
                    stdin=subprocess.DEVNULL,
                    stdout=diagnostic,
                    stderr=diagnostic,
                    env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin"},
                )
                child.close()
                parent.sendall(struct.pack("!I", len(payload)) + payload)
                parent.shutdown(socket.SHUT_WR)

                def receive_tag() -> bytes:
                    tag = receive_exact(parent, 1)
                    if tag == b"E":
                        size = struct.unpack("!I", receive_exact(parent, 4))[0]
                        if size > 65536:
                            raise ValueError("contained launcher exceeded its diagnostic budget")
                        raise RuntimeError(receive_exact(parent, size).decode(errors="replace"))
                    return tag

                if receive_tag() != b"\x03":
                    raise RuntimeError("contained launcher omitted the runtime PID")
                pid = struct.unpack("!I", receive_exact(parent, 4))[0]
                if pid <= 1:
                    raise ValueError("contained launcher reported an invalid host PID")
                observer = os.pidfd_open(pid)
                if receive_tag() != b"P" or parent.recv(1):
                    raise RuntimeError("contained launcher did not finish namespace preparation")
                deadline = time.monotonic() + 15
                with selectors.DefaultSelector() as selector:
                    selector.register(observer, selectors.EVENT_READ)
                    while not Path(self.socket).is_socket():
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or selector.select(timeout=min(0.02, remaining)):
                            diagnostic.seek(0)
                            raise RuntimeError(
                                "contained agent did not publish its endpoint: "
                                + diagnostic.read(65536).decode(errors="replace")
                            )
                yield pid
            finally:
                child.close()
                parent.close()
                terminated = True
                if observer is not None:
                    # A pidfd never targets a reused PID. Terminate the namespace
                    # init, which also tears down every remaining WASI process.
                    try:
                        signal.pidfd_send_signal(observer, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    finally:
                        try:
                            with selectors.DefaultSelector() as selector:
                                selector.register(observer, selectors.EVENT_READ)
                                terminated = bool(selector.select(timeout=5))
                        finally:
                            os.close(observer)
                if process is not None:
                    if process.poll() is None:
                        process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                if not terminated:
                    raise RuntimeError("contained namespace did not terminate within its cleanup budget")
                members = (Path(config["cgroup"]) / "cgroup.procs").read_text().strip()
                self.context.verify(
                    not members, "contained cleanup left processes in its owned resource domain",
                    remaining_pids=members,
                )

    def _command(self, service: str, method: str, request: dict[str, Any]) -> tuple[str, ...]:
        return (("sudo", "-n") if self.privileged_client else ()) + (
            "grpcurl", "-plaintext", "-unix", "-emit-defaults", "-format-error",
            "-import-path", str(Path(__file__).resolve().parents[1] / "proto"),
            "-proto", self.endpoint.proto_file,
            "-d", json.dumps(request), f"unix://{self.socket}",
            f"{self.endpoint.service_prefix}.{service}/{method}",
        )

    def start_background(self, request: dict[str, Any]) -> dict[str, Any]:
        """Detach only after Start acknowledges ownership, not after process exit."""
        if not request.get("background"):
            raise ValueError("detached start requires background=True")
        with subprocess.Popen(
            self._command("Process", "Start", request),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ) as client:
            assert client.stdout is not None
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(client.stdout, selectors.EVENT_READ)
                    deadline = time.monotonic() + 30
                    output = bytearray()
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or not selector.select(remaining):
                            raise TimeoutError("background Start did not acknowledge ownership")
                        chunk = os.read(client.stdout.fileno(), 65536)
                        if not chunk:
                            _, error = client.communicate(timeout=5)
                            raise RuntimeError(f"background Start ended: {error.decode()}")
                        output.extend(chunk)
                        if len(output) > 1024 * 1024:
                            raise ValueError("background Start exceeded its acknowledgement budget")
                        try:
                            event, _ = json.JSONDecoder().raw_decode(output.decode().lstrip())
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            continue
                        if not isinstance(event, dict) or "started" not in event:
                            raise ValueError("background Start omitted its started event")
                        return event
            finally:
                # Disconnect the RPC client, not the owned guest process. The
                # subsequent SendSignal/ConnectProcess calls prove it survives.
                if client.poll() is None:
                    client.terminate()
                try:
                    client.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    client.kill()
                    client.communicate(timeout=5)

    def call(
        self, service: str, method: str, request: dict[str, Any], *, timeout: int = 90
    ) -> list[dict[str, Any]]:
        completed = subprocess.run(
            self._command(service, method, request),
            capture_output=True, text=True, timeout=timeout, check=False,
        )
        if completed.returncode:
            try:
                status = json.loads(completed.stderr)
            except json.JSONDecodeError:
                raise RuntimeError(
                    f"{service}/{method}: {completed.stderr.strip()}"
                ) from None
            raise SpacedRpcError(service, method, status)
        decoder = json.JSONDecoder()
        remaining = completed.stdout.strip()
        messages: list[dict[str, Any]] = []
        while remaining:
            value, end = decoder.raw_decode(remaining)
            if not isinstance(value, dict):
                raise ValueError("spaced RPC returned a non-object message")
            messages.append(value)
            remaining = remaining[end:].lstrip()
        return messages

    def python(
        self, source: str, *, key: str | None = None, timeout: int = 30,
        env: dict[str, str] | None = None,
    ) -> SpacedProcessResult:
        return self.execute(["python3", "-c", source], key=key, timeout=timeout, env=env)

    def execute(
        self, command: list[str], *, cwd: str = "/workspace", key: str | None = None,
        timeout: int = 30, env: dict[str, str] | None = None,
    ) -> SpacedProcessResult:
        events = self.call("Process", "Start", {
            "command": command,
            "cwd": cwd,
            "env": env or {},
            "timeout": f"{timeout}s",
            "idempotencyKey": key or f"wasm-smoke-{uuid.uuid4().hex}",
        }, timeout=timeout + 30)
        stdout = bytearray()
        stderr = bytearray()
        identity = ""
        outcome: dict[str, Any] | None = None
        for event in events:
            identity = event["processId"]
            if "stdout" in event:
                stdout.extend(base64.b64decode(event["stdout"]["data"], validate=True))
            if "stderr" in event:
                stderr.extend(base64.b64decode(event["stderr"]["data"], validate=True))
            if "exit" in event:
                outcome = event["exit"]
        if not identity or outcome is None:
            raise ValueError("spaced process stream omitted identity or terminal outcome")
        return SpacedProcessResult(identity, bytes(stdout), bytes(stderr), outcome)

    def read_file(self, path: str) -> bytes:
        chunks = self.call("Filesystem", "Read", {"path": path})
        return b"".join(base64.b64decode(chunk["data"], validate=True) for chunk in chunks)

    def kill_if_running(self, process_id: str) -> None:
        processes = self.call("Process", "ListProcesses", {
            "processId": process_id, "includeExited": True,
        })
        if any(
            process["processId"] == process_id and process.get("exit") is None
            for process in processes
        ):
            self.call("Process", "SendSignal", {"processId": process_id, "signal": 9})
