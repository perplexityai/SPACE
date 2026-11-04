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
    windows_operations_probe: str | None = None

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
            values.get("SPACE_SMOKE_WINDOWS_OPERATIONS_PROBE"),
        )


@dataclass(frozen=True)
class ExecutionObservation:
    sandbox_id: str
    execution_id: str
    pid: int
    slot: int
    start_ticks: int


@dataclass(frozen=True)
class MemoryObservation:
    nonce: str
    pid: int
    counter: int


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

    def prepare_node(self, *, firmware: bool = False) -> None:
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

    def prepare(self, *, firmware: bool = False) -> None:
        self.prepare_node(firmware=firmware)
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

    def verify_agent_template(self) -> None:
        """Check the pinned input so blocking start proves agent readiness."""
        output = self.command([
            "space-tmplmgr", "get", "--template", self.config.template,
            "--content-hash", self.config.content_hash,
        ])
        directory = re.search(r"^template_dir:\s+(\S+)$", output, re.MULTILINE)
        if directory is None:
            raise RuntimeError("template lookup did not return its artifact directory")
        metadata = json.loads(self.command(["sudo", "-n", "cat", directory[1] + "/metadata.json"]))
        contract = metadata.get("guest_image", {})
        self.context.verify(
            contract.get("agent") == "spaced"
            and contract.get("readiness") == {"kind": "agent"}
            and contract.get("disk") == "opaque_raw_disk",
            "native-agent smoke requires an opaque disk with spaced readiness",
        )

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
                # systemd activation precedes node-manager's network-pool and
                # authenticated artifact inventory recovery. Await both before
                # issuing lifecycle calls with their shorter metadata budget.
                self.command(["space-nodemgr", "status"], timeout=15)
                self.command(["space-nodemgr", "environment", "--reference",
                              self.config.environment], timeout=30)
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

    def verify_windows_agent_operations(self) -> None:
        """Call the host API as the operator, only for this fixture's execution."""
        probe = self.config.windows_operations_probe
        if not probe or not probe.startswith("/"):
            raise SmokeConfigurationError("set SPACE_SMOKE_WINDOWS_OPERATIONS_PROBE to the built windows_operations example on the worker")
        before = self.observe()
        self.command(["sudo", "-n", probe, "--sandbox", before.sandbox_id], timeout=150)
        self.context.verify(self.observe() == before, "execution changed during Windows operations")

    def memory_marker(self, *, start: bool = False) -> MemoryObservation:
        """A live guest process updates a nonce and counter across memory lifecycle."""
        windows = self.config.firmware_lifecycle
        path = (
            rf"C:\Users\user\space-lifecycle-{self.marker}.json"
            if windows else f"/root/space-lifecycle-{self.marker}.json"
        )
        host = ["sudo", "-n", "spaced-host", "--sandbox", self.owned_selector()]
        if start:
            if windows:
                script = (
                    "$ErrorActionPreference='Stop'; $nonce=[guid]::NewGuid().ToString(); $counter=0; "
                    "while ($true) { $counter++; "
                    "@{nonce=$nonce;pid=$PID;counter=$counter} | ConvertTo-Json -Compress | "
                    f"Set-Content '{path}.tmp'; Move-Item '{path}.tmp' '{path}' -Force; "
                    "Start-Sleep -Seconds 1 }"
                )
                arguments = ["run", "--background", "--", "powershell.exe", "-NoProfile",
                             "-NonInteractive", "-Command", script]
            else:
                script = (
                    "nonce=$(cat /proc/sys/kernel/random/uuid); counter=0; "
                    "while :; do counter=$((counter + 1)); "
                    "printf '{\"nonce\":\"%s\",\"pid\":%s,\"counter\":%s}' \"$nonce\" \"$$\" \"$counter\" "
                    f"> {path}.tmp; mv {path}.tmp {path}; sleep 1; done"
                )
                arguments = ["run", "--background", "--user", "root", "--", "/bin/sh", "-c", script]
            self.command(host + arguments)
        deadline = time.monotonic() + 15
        last_error: Exception | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("guest memory marker read timed out") from last_error
            try:
                arguments = host + ["read", path] + ([] if windows else ["--user", "root"])
                value = json.loads(self.command(arguments, timeout=min(10, remaining)))
                return MemoryObservation(str(value["nonce"]), int(value["pid"]), int(value["counter"]))
            except ScenarioTimedOut:
                raise
            except (RuntimeError, json.JSONDecodeError) as error:
                last_error = error
                if time.monotonic() >= deadline:
                    raise
                self.context.wait(min(0.5, max(0, deadline - time.monotonic())))

    def verify_memory(self, before: MemoryObservation) -> MemoryObservation:
        baseline = self.memory_marker()
        self.context.verify(baseline.nonce == before.nonce and baseline.pid == before.pid,
                            "guest process or memory nonce changed across memory lifecycle")
        self.context.verify(baseline.counter >= before.counter,
                            "guest memory rewound across lifecycle")
        deadline = time.monotonic() + 15
        while True:
            after = self.memory_marker()
            self.context.verify(after.nonce == before.nonce and after.pid == before.pid,
                                "guest process or memory nonce changed across memory lifecycle")
            if after.counter > baseline.counter:
                return after
            self.context.verify(time.monotonic() < deadline, "restored guest process stopped progressing")
            self.context.wait(0.5)

    def memory_lifecycle(self, action: str) -> ExecutionObservation | None:
        if action not in ("pause", "resume", "suspend", "restore", "wake"):
            raise SmokeConfigurationError("unsupported memory lifecycle action")
        self.command(["space-sbxmgr", action, self.owned_selector(), "--blocking"], timeout=240)
        state = {"pause": "paused", "suspend": "suspended"}.get(action, "started")
        self.await_lifecycle_state(state)
        return None if state == "suspended" else self.observe()

    def automatic_memory_lifecycle(self, action: str) -> None:
        if action not in ("pause", "suspend"):
            raise SmokeConfigurationError("unsupported automatic memory lifecycle action")
        self.command(["space-sbxmgr", "update", self.owned_selector(),
                      "--idle-policy", f"{action}@5s", "--wake-on", "access"])
        self.await_lifecycle_state({"pause": "paused", "suspend": "suspended"}[action])
        # Disarm before wake so verification itself cannot race another idle action.
        self.command(["space-sbxmgr", "update", self.owned_selector(), "--auto-lifecycle-off"])

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

    def cleanup(self, *, deadline: float | None = None) -> None:
        # Standalone callers share their scenario cleanup budget. A single
        # fixture otherwise fits within the runner's default 90-second reserve.
        deadline = min(deadline, time.monotonic() + 180) if deadline is not None else time.monotonic() + 80

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

    def verify_ssh_banner(self) -> None:
        """Prove DHCP and the fenced TCP channel without guest credentials."""
        script = textwrap.dedent("""\
            import json, os, re, select, subprocess, sys, time, tempfile
            deadline = time.monotonic() + 30
            diagnostics = tempfile.TemporaryFile()
            while time.monotonic() < deadline:
                args = ['sudo', '-n', '/space/bin/space-nodemgr', 'guest-channel', sys.argv[1], '--port', '22']
                with subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=diagnostics) as channel:
                    try:
                        banner = bytearray()
                        attempt_deadline = min(deadline, time.monotonic() + 3)
                        while len(banner) < 255 and time.monotonic() < attempt_deadline:
                            ready = select.select([channel.stdout], [], [], max(0, attempt_deadline-time.monotonic()))[0]
                            if not ready:
                                break
                            chunk = os.read(channel.stdout.fileno(), 255-len(banner))
                            if not chunk:
                                break
                            banner.extend(chunk)
                            prefix = b'SSH-2.0-'
                            if not banner.startswith(prefix[:min(len(prefix), len(banner))]):
                                break
                            line, newline, _ = banner.partition(b'\\n')
                            if newline:
                                identification = bytes(line) + b'\\n'
                                if re.fullmatch(rb'SSH-2\\.0-[\\x21-\\x2c\\x2e-\\x7e]+(?: [^\\x00\\r\\n]*)?\\r\\n', identification):
                                    print(json.dumps({'ready': True, 'identification': identification.decode('ascii', errors='replace').rstrip()}))
                                    sys.exit(0)
                                break
                    finally:
                        channel.terminate()
                        try: channel.wait(timeout=1)
                        except subprocess.TimeoutExpired: channel.kill(); channel.wait()
                time.sleep(min(1, max(0, deadline-time.monotonic())))
            diagnostics.seek(0)
            print(json.dumps({'ready': False, 'diagnostics': diagnostics.read(65536).decode(errors='replace')}))
        """)
        result = json.loads(self.command(["python3", "-c", script, self.owned_selector()], timeout=40))
        self.context.record("guest_network", result)
        self.context.verify(result.get("ready") is True, "guest SSH identification was not reachable within 30 seconds")
