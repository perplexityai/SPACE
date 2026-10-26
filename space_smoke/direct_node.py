"""Explicitly opted-in recovery checks on an otherwise idle standalone node."""

from __future__ import annotations

import json
import re
import shlex
import textwrap
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from space_smoke.runtime import (
    ScenarioContext,
    SmokeConfigurationError,
    SubprocessExecutor,
)


@dataclass(frozen=True)
class DirectNodeConfig:
    host: str
    machine_id: str
    client_env: str
    template: str
    content_hash: str
    environment: str
    crash_owned_runtime: bool = False
    device_profile: str = ""
    device_parent: str = ""

    @classmethod
    def from_environment(cls, values: Mapping[str, str]) -> DirectNodeConfig:
        names = (
            "HOST",
            "MACHINE_ID",
            "CLIENT_ENV",
            "TEMPLATE",
            "CONTENT_HASH",
            "ENVIRONMENT",
        )
        supplied = [values.get(f"SPACE_SMOKE_NODE_{name}", "") for name in names]
        if (
            not all(supplied)
            or values.get("SPACE_SMOKE_NODE_RECOVERY") != "restart-idle-node"
        ):
            raise SmokeConfigurationError(
                "direct-node recovery requires an explicit target and restart-idle-node opt-in"
            )
        host, machine_id, client_env, template, content_hash, environment = supplied
        if host.startswith("-") or not re.fullmatch(r"[A-Za-z0-9_.@:-]+", host):
            raise SmokeConfigurationError("invalid SSH target")
        if not re.fullmatch(r"[a-f0-9]{32}", machine_id) or not client_env.startswith(
            "/"
        ):
            raise SmokeConfigurationError(
                "expected machine ID and absolute client environment path"
            )
        if not re.fullmatch(r"blake3:[a-f0-9]{64}", content_hash):
            raise SmokeConfigurationError(
                "recovery requires a pinned template content hash"
            )
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", environment):
            raise SmokeConfigurationError(
                "recovery requires an exact execution environment"
            )
        fault = values.get("SPACE_SMOKE_NODE_FAULT", "")
        if fault not in ("", "crash-owned-runtime"):
            raise SmokeConfigurationError("unknown direct-node fault request")
        return cls(
            host,
            machine_id,
            client_env,
            template,
            content_hash,
            environment,
            fault == "crash-owned-runtime",
            values.get("SPACE_SMOKE_NODE_DEVICE_PROFILE", ""),
            values.get("SPACE_SMOKE_NODE_DEVICE_PARENT", ""),
        )


@dataclass(frozen=True)
class ExecutionObservation:
    sandbox_id: str
    execution_id: str
    pid: int
    slot: int


class DirectNodeRecoveryFixture:
    fixture_name = "direct-node-recovery"
    timeout_minutes = 15
    services = ("space-runtime-supervisor", "space-nodemgr", "space-sbxmgr")

    def __init__(self, context: ScenarioContext, config: DirectNodeConfig) -> None:
        self.context = context
        self.config = config
        self.executor = SubprocessExecutor()
        self.selector: str | None = None
        self.create_directory: str | None = None
        self.marker = uuid.uuid4().hex

    def command(self, arguments: list[str], *, timeout: float = 60) -> str:
        script = f"set -e; source {shlex.quote(self.config.client_env)}; exec {shlex.join(arguments)}"
        result = self.executor.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                self.config.host,
                "bash",
                "-c",
                shlex.quote(script),
            ],
            timeout=timeout,
        )
        if result.returncode:
            raise RuntimeError(f"node command {arguments[0]} failed: {result.detail()}")
        return result.stdout.strip()

    def prepare(self, *, with_device: bool = False) -> None:
        self.context.verify(
            self.command(["cat", "/etc/machine-id"]) == self.config.machine_id,
            "SSH target has a different machine identity",
        )
        self.context.verify(
            self.command(["space-sbxmgr", "list"]) == "No sandboxes found.",
            "recovery requires a node with no existing sandboxes",
        )
        self.command(["systemctl", "is-active", *self.services])
        self.context.verify(
            self.command(
                [
                    "systemctl",
                    "show",
                    "space-runtime-supervisor",
                    "-p",
                    "KillMode",
                    "--value",
                ]
            )
            == "process",
            "supervisor restart would kill its executions",
        )
        self.context.record("machine_id", self.config.machine_id)
        self.context.scope.defer("remove recovery sandbox", self.cleanup, critical=True)
        self.create_directory = self.command(
            ["mktemp", "-d", "/tmp/space-smoke-create.XXXXXXXXXX"]
        )
        arguments = [
            "space-sbxmgr",
            "create",
            "--template",
            self.config.template,
            "--content-hash",
            self.config.content_hash,
            "--environment",
            self.config.environment,
            "--user",
            "deploy@local",
            "--auto-lifecycle-off",
        ]
        if with_device:
            if not self.config.device_profile or not self.config.device_parent:
                raise SmokeConfigurationError(
                    "GPU run requires an exact enrolled parent and profile"
                )
            self.await_device_ready()
            arguments.extend(
                ["--device-profile", self.config.device_profile, "--device-count", "1"]
            )
        # Retain the response on the node before transmitting it over SSH, so
        # cleanup can recover the run's selector if that connection is lost.
        response = self.create_directory + "/response"
        output = self.command(
            [
                "bash",
                "-c",
                f"{shlex.join(arguments)} > {shlex.quote(response)} && cat {shlex.quote(response)}",
            ]
        )
        match = re.search(r"^Sandbox:\s+([a-f0-9-]+)$", output, re.MULTILINE)
        if match is None:
            raise RuntimeError("create did not return a sandbox selector")
        self.selector = match[1]
        self.context.record("sandbox_selector", self.selector)

    def owned_selector(self) -> str:
        if self.selector is None:
            raise RuntimeError("no owned recovery sandbox")
        return self.selector

    def start(self, *, memory_mib: int | None = None) -> ExecutionObservation:
        arguments = ["space-sbxmgr", "start", self.owned_selector(), "--blocking"]
        if memory_mib is not None:
            arguments.extend(["--memory", str(memory_mib)])
        self.command(arguments, timeout=180)
        return self.observe()

    def observe(self) -> ExecutionObservation:
        output = self.command(["space-nodemgr", "inspect", self.owned_selector()])

        def field(pattern: str) -> str:
            match = re.search(pattern, output, re.MULTILINE)
            if match is None:
                raise RuntimeError(
                    f"missing execution observation: {pattern}\n{output}"
                )
            return match[1]

        observation = ExecutionObservation(
            str(uuid.UUID(field(r"^Sandbox:\s+(\S+)$"))),
            str(uuid.UUID(field(r"^Execution:\s+(\S+)$"))),
            int(field(r"^\s*(?:Firecracker|Runtime) PID:\s+(\d+)\s+alive$")),
            int(field(r"^Slot:\s+(\d+)$")),
        )
        self.selector = observation.sandbox_id
        self.context.record("sandbox_id", observation.sandbox_id)
        return observation

    def guest(self, command: str) -> str:
        return self.command(
            ["space-ssh", "--user", "root", self.owned_selector(), command], timeout=45
        )

    def write_marker(self) -> str:
        self.guest(f"printf '%s' {self.marker} > /root/recovery-marker; sync")
        return self.guest("cat /proc/sys/kernel/random/boot_id")

    def verify_marker(self) -> str:
        self.context.verify(
            self.guest("cat /root/recovery-marker") == self.marker,
            "guest disk marker changed across recovery",
        )
        return self.guest("cat /proc/sys/kernel/random/boot_id")

    def restart_managers(self) -> None:
        # Explicit target opt-in and the idle-node precondition authorize only
        # these service restarts; there is no arbitrary host mutation surface.
        self.command(["sudo", "-n", "systemctl", "restart", *self.services], timeout=90)
        deadline = time.monotonic() + 60
        while True:
            try:
                self.command(["systemctl", "is-active", *self.services])
                self.command(["space-sbxmgr", "list"], timeout=15)
                return
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                self.context.wait(1)

    def stop(self) -> None:
        self.command(
            ["space-sbxmgr", "stop", self.owned_selector(), "--blocking"], timeout=120
        )

    def crash_runtime(self, execution: ExecutionObservation) -> None:
        if not self.config.crash_owned_runtime:
            raise SmokeConfigurationError(
                "VMM crash requires SPACE_SMOKE_NODE_FAULT=crash-owned-runtime"
            )
        self.context.verify(
            self.observe() == execution, "execution changed before the requested crash"
        )
        # Hold a pidfd before inspecting the recorded owner. A recycled PID
        # must never redirect this fault to another process.
        script = textwrap.dedent("""\
            import os
            import signal
            import sys
            from pathlib import Path
            pid, sandbox, execution = sys.argv[1:]
            fd = os.pidfd_open(int(pid))
            try:
                owner = Path('/space/run/runtime-supervisor/executions') / sandbox
                pidfile = Path('/space/run/firecracker/sandboxes') / sandbox / 'fc.pid'
                if owner.read_text() != execution or pidfile.resolve() != Path('/proc') / pid:
                    raise RuntimeError('runtime ownership changed before fault')
                executable = Path('/proc') / pid / 'exe'
                if executable.resolve().name != 'cloud-hypervisor':
                    raise RuntimeError('fault is restricted to the owned CH process')
                signal.pidfd_send_signal(fd, signal.SIGKILL)
            finally:
                os.close(fd)
        """)
        self.command(
            [
                "sudo",
                "-n",
                "python3",
                "-c",
                script,
                str(execution.pid),
                execution.sandbox_id,
                execution.execution_id,
            ]
        )
        self.context.record("crashed_execution", execution.execution_id)

    def await_failed(self) -> None:
        label = self.owned_selector().replace("-", "")[-8:]
        deadline = time.monotonic() + 90
        last_row = ""
        while time.monotonic() < deadline:
            output = self.command(["space-sbxmgr", "list"], timeout=15)
            rows = [line for line in output.splitlines() if label in line]
            self.context.verify(
                len(rows) == 1, "owned sandbox disappeared after VMM crash"
            )
            last_row = rows[0]
            fields = [
                field.strip() for field in re.split(r"[│┆|]", last_row) if field.strip()
            ]
            if len(fields) >= 3 and fields[2] == "failed":
                return
            self.context.wait(1)
        self.context.verify(
            False, "VMM crash did not converge to failed", last_row=last_row
        )

    def await_lease_cleanup(self) -> None:
        label = self.owned_selector().replace("-", "")[-8:]
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            output = self.command(["space-nodemgr", "status"], timeout=15)
            if label not in output:
                self.context.record("node_lease_cleanup", "owned lease absent")
                return
            self.context.wait(1)
        self.context.verify(False, "owned node lease remains after sandbox cleanup")

    def cleanup(self) -> None:
        if self.selector is None:
            if self.create_directory is None:
                return
            output = self.command(["cat", self.create_directory + "/response"])
            match = re.search(r"^Sandbox:\s+([a-f0-9-]+)$", output, re.MULTILINE)
            if match is None:
                raise RuntimeError(
                    "create outcome unknown; retained response directory for inspection"
                )
            self.selector = match[1]
        self.command(
            ["space-sbxmgr", "delete", self.selector, "--blocking"], timeout=90
        )
        remaining = self.command(["space-sbxmgr", "list"])
        self.context.verify(
            self.selector.replace("-", "")[-8:] not in remaining,
            "recovery sandbox remains after delete",
        )
        self.await_lease_cleanup()
        if self.create_directory is not None:
            self.command(["rm", "--", self.create_directory + "/response"])
            self.command(["rmdir", "--", self.create_directory])
        self.context.record("cleanup", "owned sandbox deleted")

    def device_parent(self) -> dict[str, Any]:
        parents = [
            json.loads(line)
            for line in self.command(
                [
                    "sudo",
                    "-n",
                    "bash",
                    "-c",
                    f"source {shlex.quote(self.config.client_env)}; exec space-nodemgr devices list --json",
                ]
            ).splitlines()
            if line.strip()
        ]
        parent = next(
            (p for p in parents if p["parent_id"] == self.config.device_parent), None
        )
        self.context.verify(parent is not None, "configured device parent disappeared")
        if parent is None:
            raise RuntimeError("device parent missing")
        self.context.verify(
            parent["profile"] == self.config.device_profile, "device profile changed"
        )
        if parent.get("usage") is None:
            raise RuntimeError("node-manager does not report device capacity")
        return parent

    def await_device_ready(self) -> None:
        deadline = time.monotonic() + 60
        while True:
            parent = self.device_parent()
            state = parent["state"]
            if (
                state == "DEVICE_PARENT_STATE_READY"
                and parent["usage"]["allocatable"] is True
            ):
                self.context.record("device_pool_ready", parent)
                return
            if (
                state
                not in ("DEVICE_PARENT_STATE_READY", "DEVICE_PARENT_STATE_SANITIZING")
                or time.monotonic() >= deadline
            ):
                raise RuntimeError(f"device pool did not become ready: {parent}")
            self.context.wait(1)

    def verify_device_owner(self, execution: ExecutionObservation) -> None:
        parent = self.device_parent()
        usage = parent["usage"]
        self.context.verify(
            usage["allocatable"] is False, "held device reported available"
        )
        self.context.verify(
            any(
                a["owner_kind"] == "sandbox"
                and a["owner"] == execution.sandbox_id
                and a["execution_id"] == execution.execution_id
                and a["state"] == "attached"
                for a in usage["allocations"]
            ),
            "device owner differs from the running sandbox",
        )
        self.context.record("device_owner", parent)

    def compute_on_gpu(self) -> None:
        source = (Path(__file__).parent / "assets" / "cuda_compute.py").read_text()
        result = json.loads(self.guest("python3 -c " + shlex.quote(source)))
        self.context.verify(
            result
            == {
                "gpu_computation": "passed",
                "checked_elements": 65536,
                "operation": "out[i] = 3*i + 7",
            },
            "CUDA computation mismatch",
        )
        self.context.record("cuda", result)
