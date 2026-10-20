"""Characterization tests for the expanded ECW contract scenarios."""

from __future__ import annotations

import shlex
import types
import unittest
from pathlib import PurePosixPath
from typing import BinaryIO, cast
from unittest import mock

from space_smoke.adapters import EcwConfig, _SdkSandbox
from space_smoke.capabilities import (
    EcwCommandResult,
    EcwDataPlaneNotFound,
    EcwFileEntry,
    EcwPublicPort,
    EcwSandbox,
    EcwSandboxInfo,
    EcwSandboxUnavailable,
    ProcessInfo,
)
from space_smoke.fixtures import Ecw
from space_smoke.runtime import (
    InvariantViolation,
    ResourceScope,
    ScenarioContext,
    ScenarioTimedOut,
    SmokeConfigurationError,
)
from space_smoke.scenarios.ecw_background_process_control import (
    PROCESS_READY_MARKER,
    PROCESS_READY_PREFIX,
    background_process_control,
)
from space_smoke.scenarios.ecw_binary_file_round_trip import (
    CONTENT as BINARY_CONTENT,
    PATH as BINARY_PATH,
    binary_file_round_trip,
)
from space_smoke.scenarios.ecw_filesystem_mutations import (
    ORIGINAL_PATH,
    RENAMED_PATH,
    SIBLING_PATH,
    filesystem_mutations,
)
from space_smoke.scenarios.ecw_filesystem_ownership import (
    ROOT_OWNED_PATH,
    USER_OWNED_PATH,
    WORKSPACE_FILE_PATH,
    WORKSPACE_TREE,
    filesystem_ownership,
)
from space_smoke.scenarios.ecw_immediate_egress import PROBE_URL, immediate_egress
from space_smoke.scenarios.ecw_large_stdout_integrity import (
    PATH as LARGE_STDOUT_PATH,
    PAYLOAD_BYTES,
    large_stdout_integrity,
)
from space_smoke.scenarios.ecw_process_reattach import (
    MARKER as REATTACH_MARKER,
    process_reattach,
)


class _Filesystem:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace
        self.files: dict[str, bytes] = {}
        self.directories: set[str] = set()
        self.owners: dict[str, int] = {}

    def write(
        self,
        path: str,
        data: str | bytes | BinaryIO,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        if isinstance(data, str):
            content = data.encode()
        elif isinstance(data, bytes):
            content = data
        else:
            content = data.read()
        self.trace.append(
            f"files.write:{path}:{len(content)}:{request_timeout}:user={user}"
        )
        self.files[path] = content
        uid = 2000 if user == "user" else 0
        self.owners[path] = uid
        parent = PurePosixPath(path).parent
        while str(parent) not in {".", "/"}:
            self.directories.add(str(parent))
            self.owners.setdefault(str(parent), uid)
            parent = parent.parent

    def read(self, path: str, *, request_timeout: float = 60) -> str:
        self.trace.append(f"files.read:{path}:{request_timeout}")
        return self.files[path].decode()

    def read_bytes(self, path: str, *, request_timeout: float = 60) -> bytes:
        self.trace.append(f"files.read-bytes:{path}:{request_timeout}")
        return self.files[path]

    def make_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        self.trace.append(f"files.mkdir:{path}:{request_timeout}:user={user}")
        self.directories.add(path)
        self.owners[path] = 2000 if user == "user" else 0

    def exists(self, path: str, *, request_timeout: float = 60) -> bool:
        self.trace.append(f"files.exists:{path}:{request_timeout}")
        return path in self.files or path in self.directories

    def list(
        self,
        path: str,
        *,
        request_timeout: float = 60,
    ) -> tuple[EcwFileEntry, ...]:
        self.trace.append(f"files.list:{path}:{request_timeout}")
        names = {
            PurePosixPath(file_path).name
            for file_path in self.files
            if str(PurePosixPath(file_path).parent) == path
        }
        return tuple(EcwFileEntry(name) for name in sorted(names))

    def rename(
        self,
        old_path: str,
        new_path: str,
        *,
        request_timeout: float = 60,
    ) -> None:
        self.trace.append(f"files.rename:{old_path}:{new_path}:{request_timeout}")
        self.files[new_path] = self.files.pop(old_path)
        self.owners[new_path] = self.owners.pop(old_path)

    def remove(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        self.trace.append(f"files.remove:{path}:{request_timeout}:user={user}")
        self.files.pop(path, None)
        self.directories.discard(path)
        self.owners.pop(path, None)
        prefix = f"{path.rstrip('/')}/"
        for child in [item for item in self.files if item.startswith(prefix)]:
            self.files.pop(child)
            self.owners.pop(child, None)
        for child in [item for item in self.directories if item.startswith(prefix)]:
            self.directories.remove(child)
            self.owners.pop(child, None)


class _Process:
    def __init__(
        self,
        trace: list[str],
        pid: int,
        result: EcwCommandResult,
    ) -> None:
        self.trace = trace
        self.pid = pid
        self.result = result
        self.killed = False

    @property
    def process_id(self) -> str:
        return str(self.pid)

    def disconnect(self) -> None:
        self.trace.append(f"process.disconnect:{self.pid}")

    def wait(self) -> EcwCommandResult:
        self.trace.append(f"process.wait:{self.pid}")
        return self.result

    def kill(self) -> bool:
        self.trace.append(f"process.kill:{self.pid}")
        self.killed = True
        return True


class _Sandbox:
    sandbox_id = "sandbox-additional"

    def __init__(self) -> None:
        self.trace: list[str] = []
        self.files = _Filesystem(self.trace)
        self.processes: dict[int, _Process] = {}
        self.next_pid = 100
        self.connect_failures = 0
        self.curl_result = EcwCommandResult(0, "200", "")
        self.curl_error: Exception | None = None
        self.network_snapshot = "resolver and route snapshot"
        self.network_snapshot_error: Exception | None = None
        self.process_ready_failures = 0

    def run(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
    ) -> EcwCommandResult:
        self.trace.append(f"run:{command}:{timeout}:user={user}")
        if command.startswith("curl "):
            if self.curl_error is not None:
                raise self.curl_error
            return self.curl_result
        if command.startswith("set +e"):
            if self.network_snapshot_error is not None:
                raise self.network_snapshot_error
            return EcwCommandResult(0, self.network_snapshot, "")
        if command.startswith("cat "):
            path = command[4:]
            if path.startswith(PROCESS_READY_PREFIX):
                if self.process_ready_failures:
                    self.process_ready_failures -= 1
                    return EcwCommandResult(1, "", f"cat: {path}: No such file")
                return EcwCommandResult(0, PROCESS_READY_MARKER, "")
            return EcwCommandResult(0, self.files.files[path].decode(), "")
        if command == "id -u user":
            return EcwCommandResult(0, "2000\n", "")
        if command.startswith("mkdir -p "):
            path = shlex.split(command)[2]
            self.files.make_dir(path, user=user)
            return EcwCommandResult(0, "", "")
        if command.startswith("stat -c "):
            paths = shlex.split(command.partition("-- ")[2])
            output = "\n".join(f"{self.files.owners[path]} {path}" for path in paths)
            return EcwCommandResult(0, f"{output}\n", "")
        raise AssertionError(f"unexpected command: {command}")

    def start_background(self, command: str, *, timeout: int = 60) -> _Process:
        self.trace.append(f"process.start:{command}:{timeout}")
        self.next_pid += 1
        stdout = REATTACH_MARKER if REATTACH_MARKER in command else ""
        process = _Process(
            self.trace,
            self.next_pid,
            EcwCommandResult(0, stdout, ""),
        )
        self.processes[process.pid] = process
        return process

    def connect_process(self, pid: int, *, timeout: int = 60) -> _Process:
        self.trace.append(f"process.connect:{pid}:{timeout}")
        if self.connect_failures:
            self.connect_failures -= 1
            raise EcwDataPlaneNotFound("process is not visible yet")
        return self.processes[pid]

    def list_processes(self) -> tuple[ProcessInfo, ...]:
        self.trace.append("process.list")
        return tuple(
            ProcessInfo(str(process.pid), process.pid, ("/bin/bash", "-l", "-c"))
            for process in self.processes.values()
            if not process.killed
        )

    def get_process(self, process_id: str) -> ProcessInfo | None:
        self.trace.append(f"process.get:{process_id}")
        return next(
            (
                process
                for process in self.list_processes()
                if process.process_id == process_id
            ),
            None,
        )

    def signal_process(self, process_id: str, signal: int) -> bool:
        self.trace.append(f"process.signal:{process_id}:{signal}")
        return process_id in {str(pid) for pid in self.processes}

    def kill_process(self, process_id: str) -> bool:
        process = self.processes.get(int(process_id))
        if process is None:
            return False
        return process.kill()

    def public_port(self, port: int) -> EcwPublicPort:
        return EcwPublicPort(f"https://{port}-{self.sandbox_id}.example.test", {})

    def get_info(self) -> EcwSandboxInfo:
        return EcwSandboxInfo(self.sandbox_id, "seed")

    def try_get_info(self) -> EcwSandboxInfo | None:
        return self.get_info()

    def set_timeout(self, timeout: int) -> None:
        raise AssertionError(f"unexpected timeout update: {timeout}")

    def refresh(self) -> None:
        raise AssertionError("unexpected sandbox refresh")

    def pause(self) -> None:
        raise AssertionError("unexpected pause")

    def connect(self, *, timeout: int = 300) -> _Sandbox:
        raise AssertionError(f"unexpected sandbox connect: {timeout}")

    def kill(self) -> None:
        self.trace.append("sandbox.kill")


def _fixture(sandbox: _Sandbox) -> Ecw:
    return Ecw(ScenarioContext(ResourceScope(), {}), cast(EcwSandbox, sandbox))


class EcwAdditionalScenarioTests(unittest.TestCase):
    def test_ecw_process_control_preserves_sdk_and_compatibility_routes(self) -> None:
        commands = mock.Mock()
        commands.list.return_value = [
            types.SimpleNamespace(pid=123, cmd="/bin/bash", args=["-l", "-c"])
        ]
        commands.kill.return_value = True
        sandbox = _SdkSandbox(
            types.SimpleNamespace(sandbox_id="sandbox-sdk", commands=commands),
            EcwConfig(api_key="key"),
        )

        with mock.patch.object(
            sandbox.compatibility, "_compatibility_request", return_value=b"{}"
        ) as signal_request:
            processes = sandbox.list_processes()
            process = sandbox.get_process("123")
            signaled = sandbox.signal_process("123", 15)
            killed = sandbox.kill_process("123")

        self.assertEqual(processes, (process,))
        self.assertEqual(processes[0].command, ("/bin/bash", "-l", "-c"))
        self.assertTrue(signaled)
        self.assertTrue(killed)
        signal_request.assert_called_once_with(
            "/process.Process/SendSignal",
            method="POST",
            body=b'{"process":{"pid":123},"signal":15}',
            user=None,
            request_timeout=60,
        )
        commands.kill.assert_called_once_with(123)

    def test_immediate_egress_requires_an_observed_200(self) -> None:
        sandbox = _Sandbox()

        immediate_egress.verify(_fixture(sandbox))

        self.assertEqual(len(sandbox.trace), 1)
        self.assertIn(PROBE_URL, sandbox.trace[0])

    def test_immediate_egress_failure_collects_the_legacy_guest_snapshot(
        self,
    ) -> None:
        sandbox = _Sandbox()
        sandbox.curl_result = EcwCommandResult(7, "000", "connection failed")
        fixture = _fixture(sandbox)

        with self.assertRaises(InvariantViolation):
            immediate_egress.verify(fixture)

        self.assertEqual(fixture._context.evidence["guest_snapshot_status"], "exit=0")
        self.assertEqual(
            fixture._context.evidence["guest_snapshot_output"],
            sandbox.network_snapshot,
        )
        self.assertEqual(len(sandbox.trace), 2)

    def test_immediate_egress_transport_failure_is_an_invariant_with_evidence(
        self,
    ) -> None:
        sandbox = _Sandbox()
        sandbox.curl_error = EcwSandboxUnavailable("sandbox route is not ready")
        fixture = _fixture(sandbox)

        with self.assertRaises(InvariantViolation):
            immediate_egress.verify(fixture)

        self.assertEqual(
            fixture._context.evidence["command_error_type"],
            "EcwSandboxUnavailable",
        )
        self.assertEqual(
            fixture._context.evidence["guest_snapshot_output"],
            sandbox.network_snapshot,
        )

    def test_immediate_egress_does_not_reclassify_a_scenario_timeout(self) -> None:
        sandbox = _Sandbox()
        sandbox.curl_error = ScenarioTimedOut("scenario execution timed out")

        with self.assertRaises(ScenarioTimedOut):
            immediate_egress.verify(_fixture(sandbox))

        self.assertEqual(len(sandbox.trace), 1)

    def test_failed_network_snapshot_does_not_mask_the_egress_failure(self) -> None:
        sandbox = _Sandbox()
        sandbox.curl_result = EcwCommandResult(7, "000", "connection failed")
        sandbox.network_snapshot_error = EcwSandboxUnavailable(
            "diagnostic route unavailable"
        )
        fixture = _fixture(sandbox)

        with self.assertRaisesRegex(
            InvariantViolation,
            "could not reach the HTTPS egress probe",
        ):
            immediate_egress.verify(fixture)

        self.assertEqual(fixture._context.evidence["guest_snapshot_status"], "failed")
        self.assertEqual(
            fixture._context.evidence["guest_snapshot_error_type"],
            "EcwSandboxUnavailable",
        )

    def test_network_snapshot_does_not_swallow_a_scenario_timeout(self) -> None:
        sandbox = _Sandbox()
        sandbox.curl_result = EcwCommandResult(7, "000", "connection failed")
        sandbox.network_snapshot_error = ScenarioTimedOut("scenario execution timed out")

        with self.assertRaises(ScenarioTimedOut):
            immediate_egress.verify(_fixture(sandbox))

        self.assertEqual(len(sandbox.trace), 2)

    def test_unreadable_network_snapshot_keeps_the_egress_failure(self) -> None:
        sandbox = _Sandbox()
        sandbox.curl_result = EcwCommandResult(7, "000", "connection failed")
        sandbox.network_snapshot_error = SmokeConfigurationError(
            "unsafe diagnostic configuration"
        )
        fixture = _fixture(sandbox)

        with self.assertRaises(InvariantViolation):
            immediate_egress.verify(fixture)

        self.assertEqual(
            fixture._context.evidence["failure_evidence_error"],
            "SmokeConfigurationError: unsafe diagnostic configuration",
        )
        self.assertEqual(len(sandbox.trace), 2)

    def test_binary_file_round_trip_preserves_every_byte(self) -> None:
        sandbox = _Sandbox()

        binary_file_round_trip.verify(_fixture(sandbox))

        self.assertEqual(sandbox.files.files[BINARY_PATH], BINARY_CONTENT)
        self.assertEqual(
            sandbox.trace[-1],
            f"files.read-bytes:{BINARY_PATH}:60",
        )

    def test_filesystem_mutations_make_the_rename_visible(self) -> None:
        sandbox = _Sandbox()

        filesystem_mutations.verify(_fixture(sandbox))

        self.assertNotIn(ORIGINAL_PATH, sandbox.files.files)
        self.assertNotIn(RENAMED_PATH, sandbox.files.files)
        self.assertNotIn(SIBLING_PATH, sandbox.files.files)
        self.assertIn(f"files.exists:{ORIGINAL_PATH}:60", sandbox.trace)
        self.assertIn(f"files.exists:{RENAMED_PATH}:60", sandbox.trace)
        self.assertIn(
            "files.remove:/tmp/space-smoke/filesystem-mutations:60:user=None",
            sandbox.trace,
        )

    def test_filesystem_mutations_require_remove_to_change_existence(self) -> None:
        sandbox = _Sandbox()
        remove = sandbox.files.remove

        def ignore_file_remove(
            path: str,
            *,
            user: str | None = None,
            request_timeout: float = 60,
        ) -> None:
            if path == SIBLING_PATH:
                sandbox.trace.append(
                    f"files.remove-ignored:{path}:{request_timeout}:user={user}"
                )
                return
            remove(path, user=user, request_timeout=request_timeout)

        sandbox.files.remove = ignore_file_remove  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            InvariantViolation,
            "returned without removing the file",
        ):
            filesystem_mutations.verify(_fixture(sandbox))

    def test_filesystem_mutations_require_missing_parents_to_be_created(self) -> None:
        sandbox = _Sandbox()
        exists = sandbox.files.exists

        def hide_nested_directory(
            path: str,
            *,
            request_timeout: float = 60,
        ) -> bool:
            if path == str(PurePosixPath(ORIGINAL_PATH).parent):
                return False
            return exists(path, request_timeout=request_timeout)

        sandbox.files.exists = hide_nested_directory  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            InvariantViolation,
            "did not auto-create missing parents",
        ):
            filesystem_mutations.verify(_fixture(sandbox))

    def test_filesystem_ownership_preserves_requested_users(self) -> None:
        sandbox = _Sandbox()

        filesystem_ownership.verify(_fixture(sandbox))

        self.assertEqual(sandbox.files.owners[ROOT_OWNED_PATH], 0)
        self.assertEqual(sandbox.files.owners[USER_OWNED_PATH], 2000)
        self.assertEqual(
            {path: sandbox.files.owners[path] for path in WORKSPACE_TREE},
            {path: 2000 for path in WORKSPACE_TREE},
        )
        self.assertTrue(
            any("user=user" in entry for entry in sandbox.trace),
            sandbox.trace,
        )

    def test_filesystem_ownership_rejects_a_user_write_owned_by_root(self) -> None:
        sandbox = _Sandbox()
        write = sandbox.files.write

        def root_owned_user_write(
            path: str,
            data: str | bytes | BinaryIO,
            *,
            user: str | None = None,
            request_timeout: float = 60,
        ) -> None:
            write(path, data, user=user, request_timeout=request_timeout)
            if user == "user":
                sandbox.files.owners[path] = 0

        sandbox.files.write = root_owned_user_write  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            InvariantViolation,
            "did not retain their requested owners",
        ):
            filesystem_ownership.verify(_fixture(sandbox))

    def test_filesystem_ownership_requires_user_owned_auto_created_parents(
        self,
    ) -> None:
        sandbox = _Sandbox()
        write = sandbox.files.write

        def root_owned_parent(
            path: str,
            data: str | bytes | BinaryIO,
            *,
            user: str | None = None,
            request_timeout: float = 60,
        ) -> None:
            write(path, data, user=user, request_timeout=request_timeout)
            if path == WORKSPACE_FILE_PATH:
                sandbox.files.owners[str(PurePosixPath(path).parent)] = 0

        sandbox.files.write = root_owned_parent  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            InvariantViolation,
            "did not leave the workspace tree owned by the sandbox user",
        ):
            filesystem_ownership.verify(_fixture(sandbox))

    def test_large_stdout_is_verified_at_the_legacy_payload_size(self) -> None:
        sandbox = _Sandbox()

        large_stdout_integrity.verify(_fixture(sandbox))

        self.assertEqual(len(sandbox.files.files[LARGE_STDOUT_PATH]), PAYLOAD_BYTES)
        self.assertIn(
            f"files.read-bytes:{LARGE_STDOUT_PATH}:60",
            sandbox.trace,
        )
        self.assertEqual(
            sandbox.trace[-1],
            f"run:cat {LARGE_STDOUT_PATH}:90:user=None",
        )

    def test_shared_background_process_assertions_run_through_ecw(self) -> None:
        sandbox = _Sandbox()

        background_process_control.verify(_fixture(sandbox))

        process = next(iter(sandbox.processes.values()))
        self.assertTrue(process.killed)
        readiness = sandbox.trace.index(
            f"run:cat {PROCESS_READY_PREFIX}{process.pid}:10:user=None"
        )
        term = sandbox.trace.index(f"process.signal:{process.pid}:15")
        alive = sandbox.trace.index(f"process.get:{process.pid}", term)
        kill = sandbox.trace.index(f"process.kill:{process.pid}")
        self.assertLess(readiness, term)
        self.assertLess(term, alive)
        self.assertLess(alive, kill)
        self.assertEqual(sandbox.trace[-1], "process.list")

    def test_background_process_waits_for_post_trap_readiness(self) -> None:
        sandbox = _Sandbox()
        sandbox.process_ready_failures = 1
        fixture = _fixture(sandbox)

        with mock.patch.object(fixture._context, "wait") as wait:
            background_process_control.verify(fixture)

        process = next(iter(sandbox.processes.values()))
        readiness_command = f"run:cat {PROCESS_READY_PREFIX}{process.pid}"
        readiness_attempts = [
            entry for entry in sandbox.trace if entry.startswith(readiness_command)
        ]
        self.assertEqual(len(readiness_attempts), 2)
        wait.assert_called_once_with(Ecw.lifecycle_poll_interval)
        self.assertLess(
            sandbox.trace.index(readiness_attempts[-1]),
            sandbox.trace.index(f"process.signal:{process.pid}:15"),
        )

    def test_process_is_reattached_by_pid_and_waited_to_completion(self) -> None:
        sandbox = _Sandbox()

        process_reattach.verify(_fixture(sandbox))

        process = next(iter(sandbox.processes.values()))
        self.assertIn("sleep 10", sandbox.trace[0])
        self.assertTrue(sandbox.trace[0].endswith(":20"))
        self.assertEqual(
            sandbox.trace[-3:],
            [
                f"process.disconnect:{process.pid}",
                f"process.connect:{process.pid}:20",
                f"process.wait:{process.pid}",
            ],
        )

    def test_process_reattach_retries_while_the_process_is_becoming_visible(
        self,
    ) -> None:
        sandbox = _Sandbox()
        process = sandbox.start_background("sleep 10", timeout=20)
        sandbox.connect_failures = 1

        with mock.patch("space_smoke.runtime.time.sleep") as sleep:
            connected = _fixture(sandbox).reattach_process(process.pid, timeout=20)

        self.assertIs(connected, process)
        self.assertEqual(
            sandbox.trace[-2:],
            [
                f"process.connect:{process.pid}:20",
                f"process.connect:{process.pid}:20",
            ],
        )
        sleep.assert_called_once_with(Ecw.process_connect_retry_delays[0])

    def test_process_reattach_stops_after_the_legacy_attempt_budget(self) -> None:
        sandbox = _Sandbox()
        process = sandbox.start_background("sleep 10", timeout=20)
        sandbox.connect_failures = 3

        with (
            mock.patch("space_smoke.runtime.time.sleep") as sleep,
            self.assertRaisesRegex(InvariantViolation, "after 3 attempts"),
        ):
            _fixture(sandbox).reattach_process(process.pid, timeout=20)

        connect = f"process.connect:{process.pid}:20"
        self.assertEqual(sandbox.trace.count(connect), 3)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            list(Ecw.process_connect_retry_delays),
        )


if __name__ == "__main__":
    unittest.main()
