"""Typed node-local fixture using the existing spaced gRPC wire contract."""

from __future__ import annotations

import base64
import json
import os
import selectors
import subprocess
import time
import uuid
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
