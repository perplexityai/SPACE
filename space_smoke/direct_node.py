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

from space_smoke.runtime import (
    ScenarioContext,
    ScenarioTimedOut,
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
    firmware_lifecycle: bool = False

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
            or values.get("SPACE_SMOKE_NODE_RECOVERY") not in ("restart-idle-node", "firmware-lifecycle")
        ):
            raise SmokeConfigurationError(
                "direct-node checks require an explicit target and recovery/lifecycle opt-in"
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
            values.get("SPACE_SMOKE_NODE_RECOVERY") == "firmware-lifecycle",
        )


@dataclass(frozen=True)
class ExecutionObservation:
    sandbox_id: str
    execution_id: str
    pid: int
    slot: int
    start_ticks: int


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
                "-o",
                "ControlMaster=no",
                "-o",
                "ControlPath=none",
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

    def prepare(self, *, firmware: bool = False) -> None:
        if firmware != self.config.firmware_lifecycle:
            raise SmokeConfigurationError("scenario does not match the explicit node operation opt-in")
        self.context.verify(
            self.command(["cat", "/etc/machine-id"]) == self.config.machine_id,
            "SSH target has a different machine identity",
        )
        if not firmware:
            self.context.verify(
                self.command(["space-sbxmgr", "list"]) == "No sandboxes found.",
                "recovery requires a node with no existing sandboxes",
            )
        # Inspect the running service's effective authority, not just an empty
        # sandbox list. Do not print its environment (it contains credentials).
        manager_pids = [
            self.command(["systemctl", "show", service, "-p", "MainPID", "--value"])
            for service in self.services
        ]
        guard = textwrap.dedent("""\
            import sys
            from pathlib import Path
            keys = (b'SPACE_NODE_MANAGED_BY', b'SPACE_INTERNAL_API_ENDPOINT', b'SPACE_CLUSTER_NODE_API_ENDPOINT', b'KUBERNETES_SERVICE_HOST')
            for pid in sys.argv[1:]:
                values = dict(part.split(b'=', 1) for part in Path('/proc', pid, 'environ').read_bytes().split(b'\\0') if b'=' in part)
                if any(values.get(key) for key in keys):
                    raise RuntimeError('recovery fault requires standalone services without cluster authority')
            configs = {Path('/space/config/nodectl.toml')}
            # node-ctl has its own environment and may select another config.
            # Inspect all live instances, including ones outside systemd.
            for process in Path('/proc').iterdir():
                if not process.name.isdigit():
                    continue
                try:
                    executable = (process / 'exe').readlink()
                    if executable.name.removesuffix(' (deleted)') != 'space-nodectl-svc':
                        continue
                    arguments = (process / 'cmdline').read_bytes().split(b'\\0')
                    environment = (process / 'environ').read_bytes().split(b'\\0')
                except FileNotFoundError:
                    continue
                values = dict(part.split(b'=', 1) for part in environment if b'=' in part)
                if any(values.get(key) for key in keys):
                    raise RuntimeError('node-ctl has cluster authority; refusing recovery fault')
                for index, argument in enumerate(arguments):
                    if argument == b'--config':
                        if index + 1 >= len(arguments) or not arguments[index + 1]:
                            raise RuntimeError('cannot determine node-ctl configuration')
                        config = Path(arguments[index + 1].decode())
                    elif argument.startswith(b'--config='):
                        config = Path(argument.split(b'=', 1)[1].decode())
                    else:
                        continue
                    configs.add(config if config.is_absolute() else process / 'cwd' / config)
            for config in configs:
                try:
                    config.stat()
                except FileNotFoundError:
                    continue
                raise RuntimeError('remove cluster enrollment configuration before opting into a standalone fault test')
        """)
        self.command(["sudo", "-n", "python3", "-c", guard, *manager_pids])
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
        # Retain the response on the node before transmitting it over SSH, so
        # cleanup can recover the run's selector if that connection is lost.
        response = self.create_directory + "/response"
        output = self.command(
            [
                "bash",
                "-c",
                f"{shlex.join(arguments)} > {shlex.quote(response)} && cat {shlex.quote(response)}",
            ],
            timeout=180,
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
        if self.config.firmware_lifecycle:
            arguments.extend(["--ready-timeout-secs", "120"])
        self.command(arguments, timeout=240)
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

        pid = int(field(r"^\s*Runtime PID:\s+(\d+)\s+alive$"))
        stat = self.command(["cat", f"/proc/{pid}/stat"])
        start_ticks = int(stat.rsplit(")", 1)[1].split()[19])
        observation = ExecutionObservation(
            str(uuid.UUID(field(r"^Sandbox:\s+(\S+)$"))),
            str(uuid.UUID(field(r"^Execution:\s+(\S+)$"))),
            pid,
            int(field(r"^Slot:\s+(\d+)$")),
            start_ticks,
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
        if self.config.firmware_lifecycle:
            raise SmokeConfigurationError("firmware lifecycle opt-in does not authorize manager restarts")
        # Explicit target opt-in and the idle-node precondition authorize only
        # these service restarts; there is no arbitrary host mutation surface.
        self.command(["sudo", "-n", "systemctl", "restart", *self.services], timeout=90)
        deadline = time.monotonic() + 60
        while True:
            try:
                self.command(["systemctl", "is-active", *self.services])
                self.command(["space-sbxmgr", "list"], timeout=15)
                return
            except (ScenarioTimedOut, SmokeConfigurationError):
                raise
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                self.context.wait(1)

    def stop(self) -> None:
        if self.config.firmware_lifecycle:
            self.command(["space-sbxmgr", "stop", self.owned_selector()])
            self.await_lifecycle_state("stopped")
        else:
            self.command(
                ["space-sbxmgr", "stop", self.owned_selector(), "--blocking"], timeout=120
            )

    def require_crash_opt_in(self) -> None:
        if not self.config.crash_owned_runtime or self.config.firmware_lifecycle:
            raise SmokeConfigurationError(
                "VMM crash requires SPACE_SMOKE_NODE_FAULT=crash-owned-runtime"
            )

    def await_lifecycle_state(self, expected: str | None, *, deadline: float | None = None) -> None:
        """The durable cleanup outlives the blocking RPC's shorter wait budget."""
        label = self.owned_selector().replace("-", "")[-8:]
        if deadline is None:
            deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            output = self.command(["space-sbxmgr", "list"], timeout=min(15, max(0.1, deadline - time.monotonic())))
            rows = [line for line in output.splitlines() if label in line]
            if not rows and expected is None:
                return
            self.context.verify(len(rows) == 1, "owned sandbox disappeared or is ambiguous")
            fields = [field.strip() for field in re.split(r"[│┆|]", rows[0]) if field.strip()]
            if len(fields) >= 3 and fields[2] == expected:
                return
            self.context.wait(min(2, max(0, deadline - time.monotonic())))
        self.context.verify(False, f"owned sandbox did not reach {expected or 'deleted'}")

    def crash_runtime(self, execution: ExecutionObservation) -> None:
        self.require_crash_opt_in()
        self.context.verify(
            self.observe() == execution, "execution changed before the requested crash"
        )
        # Hold a pidfd before inspecting the recorded owner. A recycled PID
        # must never redirect this fault to another process.
        script = textwrap.dedent("""\
            import os
            import signal
            import select
            import sys
            from pathlib import Path
            pid, sandbox, execution, start_ticks = sys.argv[1:]
            fd = os.pidfd_open(int(pid))
            try:
                owner = Path('/space/run/runtime-supervisor/executions') / sandbox
                pidfile = Path('/space/run/firecracker/sandboxes') / sandbox / 'fc.pid'
                if owner.read_text() != execution or pidfile.resolve() != Path('/proc') / pid:
                    raise RuntimeError('runtime ownership changed before fault')
                process = Path('/proc') / pid
                if process.joinpath('stat').read_text().rsplit(')', 1)[1].split()[19] != start_ticks:
                    raise RuntimeError('runtime birth identity changed before fault')
                groups = [line.split(':', 2)[2].split('/') for line in process.joinpath('cgroup').read_text().splitlines()]
                if not any(sandbox in parts or 'sbx-' + sandbox in parts for parts in groups):
                    raise RuntimeError('runtime left its owned cgroup before fault')
                executable = process / 'exe'
                if executable.resolve().name not in ('cloud-hypervisor', 'firecracker'):
                    raise RuntimeError('fault is restricted to the owned supported VMM')
                if select.select([fd], [], [], 0)[0]:
                    raise RuntimeError('pinned runtime exited during ownership checks')
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
                str(execution.start_ticks),
            ]
        )
        self.context.record("crashed_execution", execution.execution_id)

    def await_crash_cleanup(self) -> None:
        label = self.owned_selector().replace("-", "")[-8:]
        # One 60-second discovery tick plus a full cleanup allowance.
        deadline = time.monotonic() + 150
        last_row = ""
        while time.monotonic() < deadline:
            output = self.command(["space-sbxmgr", "list"], timeout=min(15, max(0.1, deadline - time.monotonic())))
            rows = [line for line in output.splitlines() if label in line]
            self.context.verify(
                len(rows) == 1, "owned sandbox disappeared after VMM crash"
            )
            last_row = rows[0]
            fields = [
                field.strip() for field in re.split(r"[│┆|]", last_row) if field.strip()
            ]
            if len(fields) >= 3 and fields[2] == "stopped":
                return
            self.context.wait(1)
        self.context.verify(
            False,
            "VMM crash did not converge to stopped after cleanup",
            last_row=last_row,
        )

    def await_lease_cleanup(self, *, deadline: float | None = None) -> None:
        label = self.owned_selector().replace("-", "")[-8:]
        deadline = deadline if deadline is not None else time.monotonic() + 45
        while time.monotonic() < deadline:
            output = self.command(
                ["space-nodemgr", "status"],
                timeout=min(15, max(0.1, deadline - time.monotonic())),
            )
            if label not in output:
                self.context.record("node_lease_cleanup", "owned lease absent")
                return
            self.context.wait(1)
        self.context.verify(False, "owned node lease remains after sandbox cleanup")

    def cleanup(self) -> None:
        # The runner grants 90 seconds for this fixture. Share one smaller
        # deadline across all cleanup calls, retaining time for its bookkeeping.
        deadline = time.monotonic() + 80

        def remaining_budget() -> float:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("direct-node cleanup budget exhausted")
            return remaining

        if self.selector is None:
            if self.create_directory is None:
                return
            output = self.command(
                ["cat", self.create_directory + "/response"],
                timeout=min(10, remaining_budget()),
            )
            match = re.search(r"^Sandbox:\s+([a-f0-9-]+)$", output, re.MULTILINE)
            if match is None:
                raise RuntimeError(
                    "create outcome unknown; retained response directory for inspection"
                )
            self.selector = match[1]
        if self.config.firmware_lifecycle:
            self.command(["space-sbxmgr", "delete", self.selector], timeout=min(10, remaining_budget()))
            self.await_lifecycle_state(None, deadline=deadline - 25)
        else:
            self.command(
                ["space-sbxmgr", "delete", self.selector, "--blocking"],
                timeout=min(50, remaining_budget()),
            )
        remaining = self.command(
            ["space-sbxmgr", "list"], timeout=min(10, remaining_budget())
        )
        self.context.verify(
            self.selector.replace("-", "")[-8:] not in remaining,
            "recovery sandbox remains after delete",
        )
        self.await_lease_cleanup(deadline=min(time.monotonic() + 45, deadline - 10))
        if self.create_directory is not None:
            self.command(
                ["rm", "--", self.create_directory + "/response"],
                timeout=remaining_budget(),
            )
            self.command(
                ["rmdir", "--", self.create_directory], timeout=remaining_budget()
            )
        self.context.record("cleanup", "owned sandbox deleted")

    def verify_rdp_negotiation(self) -> None:
        """Exercise the operator channel and NLA negotiation, not credential auth."""
        script = textwrap.dedent("""\
            import json, os, select, shlex, subprocess, sys, time
            arguments = ['/space/bin/space-nodemgr', 'guest-channel', sys.argv[1], '--port', '3389']
            command = ['sudo', '-n', 'bash', '-c', 'source '+shlex.quote(sys.argv[2])+'; exec '+shlex.join(arguments)]
            with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as channel:
                try:
                    channel.stdin.write(bytes.fromhex('030000130ee000000000000100080003000000'))
                    channel.stdin.flush()
                    response = bytearray()
                    diagnostics = bytearray()
                    streams = [channel.stdout, channel.stderr]
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        ready = select.select(streams, [], [], max(0, deadline-time.monotonic()))[0]
                        if not ready: break
                        for stream in ready:
                            data = os.read(stream.fileno(), 4096)
                            if not data:
                                streams.remove(stream)
                            elif stream is channel.stderr:
                                diagnostics.extend(data[:max(0, 4096-len(diagnostics))])
                            else:
                                response.extend(data)
                        if channel.stdout not in streams: break
                        if len(response) >= 4 and len(response) >= int.from_bytes(response[2:4], 'big'): break
                    # The server advertises capabilities in byte 12 (flags).
                    # They need not be zero for a valid negotiation response.
                    if len(response) != 19 or response[:2] != bytes([3,0]) or response[11] != 2 or response[13:15] != bytes([8,0]):
                        raise RuntimeError('invalid RDP negotiation response; guest channel: '+diagnostics.decode(errors='replace'))
                    protocol = int.from_bytes(response[15:19], 'little')
                    if protocol not in (2,8): raise RuntimeError('RDP did not select NLA; guest channel: '+diagnostics.decode(errors='replace'))
                    print(json.dumps({'rdp_nla_protocol':protocol}))
                finally:
                    channel.terminate()
                    try: channel.wait(timeout=3)
                    except subprocess.TimeoutExpired: channel.kill(); channel.wait()
        """)
        result = json.loads(self.command(["python3", "-c", script, self.owned_selector(), self.config.client_env]))
        self.context.record("rdp_negotiation", result)
