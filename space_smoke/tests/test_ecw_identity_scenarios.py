"""Characterization tests for ECW identity scenarios."""

from __future__ import annotations

import shlex
import unittest
from pathlib import PurePosixPath
from typing import BinaryIO, cast

from space_smoke.capabilities import (
    EcwCommandResult,
    EcwFileEntry,
    EcwFilesystemEvent,
    EcwFilesystemEventType,
    EcwPermissionDenied,
    EcwSandbox,
)
from space_smoke.fixtures import Ecw
from space_smoke.model import Verdict
from space_smoke.runtime import ResourceScope, ScenarioContext, execute_scenario
from space_smoke.scenarios.ecw_filesystem_private_boundary import (
    filesystem_private_boundary,
)
from space_smoke.scenarios.ecw_filesystem_read_identity import (
    ROOT_FILE as READ_ROOT_FILE,
)
from space_smoke.scenarios.ecw_filesystem_read_identity import (
    filesystem_read_identity,
)
from space_smoke.scenarios.ecw_process_identity import process_identity
from space_smoke.scenarios.ecw_watcher_identity import (
    ROOT as WATCH_ROOT,
)
from space_smoke.scenarios.ecw_watcher_identity import (
    WATCHED_FILE,
    watcher_identity,
)


class _Watcher:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def get_new_events(self) -> tuple[EcwFilesystemEvent, ...]:
        self.trace.append("watcher.events")
        return (
            EcwFilesystemEvent(
                PurePosixPath(WATCHED_FILE).name,
                EcwFilesystemEventType.WRITE,
            ),
        )

    def stop(self) -> None:
        self.trace.append("watcher.stop")


class _StopDeniedWatcher(_Watcher):
    def stop(self) -> None:
        raise EcwPermissionDenied("RemoveWatcher denied")


class _Filesystem:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace
        self.files: dict[str, bytes] = {}
        self.owners: dict[str, int] = {}
        self.directories: set[str] = set()

    @staticmethod
    def _require_root(path: str, user: str | None) -> None:
        if path.startswith("/root/") and user != "root":
            raise EcwPermissionDenied(f"permission denied for user={user!r}")

    def write(
        self,
        path: str,
        data: str | bytes | BinaryIO,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        self._require_root(path, user)
        if isinstance(data, str):
            content = data.encode()
        elif isinstance(data, bytes):
            content = data
        else:
            content = data.read()
        self.trace.append(f"files.write:{path}:user={user}:{request_timeout}")
        self.files[path] = content
        self.owners[path] = 0 if user == "root" else 2000

    def read(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> str:
        self._require_root(path, user)
        self.trace.append(f"files.read:{path}:user={user}:{request_timeout}")
        return self.files[path].decode()

    def read_bytes(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> bytes:
        self._require_root(path, user)
        return self.files[path]

    def make_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        self._require_root(path, user)
        self.trace.append(f"files.mkdir:{path}:user={user}:{request_timeout}")
        self.directories.add(path)

    def exists(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> bool:
        self._require_root(path, user)
        self.trace.append(f"files.exists:{path}:user={user}:{request_timeout}")
        return path in self.files or path in self.directories

    def list(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> tuple[EcwFileEntry, ...]:
        self._require_root(path, user)
        self.trace.append(f"files.list:{path}:user={user}:{request_timeout}")
        return tuple(
            EcwFileEntry(PurePosixPath(item).name)
            for item in sorted(self.files)
            if str(PurePosixPath(item).parent) == path
        )

    def rename(
        self,
        old_path: str,
        new_path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        self._require_root(old_path, user)
        self.trace.append(
            f"files.rename:{old_path}:{new_path}:user={user}:{request_timeout}"
        )
        self.files[new_path] = self.files.pop(old_path)
        self.owners[new_path] = self.owners.pop(old_path)

    def remove(
        self,
        path: str,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        self._require_root(path, user)
        self.trace.append(f"files.remove:{path}:user={user}:{request_timeout}")
        self.files.pop(path, None)
        self.owners.pop(path, None)

    def watch_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        recursive: bool = False,
        request_timeout: float = 60,
    ) -> _Watcher:
        self._require_root(path, user)
        self.trace.append(
            f"files.watch:{path}:user={user}:recursive={recursive}:{request_timeout}"
        )
        return _Watcher(self.trace)


class _Compatibility:
    def __init__(self, filesystem: _Filesystem) -> None:
        self.filesystem = filesystem

    def read_with_basic_auth(
        self,
        path: str,
        *,
        user: str,
        request_timeout: float = 10,
    ) -> str:
        self.filesystem.trace.append(
            f"protocol.read-basic:{path}:user={user}:{request_timeout}"
        )
        raise EcwPermissionDenied("query identity remained the default user")

    def probe_watch_stream(
        self,
        path: str,
        *,
        user: str | None = None,
        recursive: bool = True,
        request_timeout: float = 10,
    ) -> None:
        self.filesystem._require_root(path, user)
        self.filesystem.trace.append(
            f"protocol.watch-stream:{path}:user={user}:recursive={recursive}:"
            f"{request_timeout}"
        )


class _FilesystemWithStopDenied(_Filesystem):
    def watch_dir(
        self,
        path: str,
        *,
        user: str | None = None,
        recursive: bool = False,
        request_timeout: float = 60,
    ) -> _Watcher:
        return _StopDeniedWatcher(self.trace)


class _Sandbox:
    sandbox_id = "sandbox-identity"

    def __init__(self, *, wrong_default_identity: bool = False) -> None:
        self.trace: list[str] = []
        self.files = _Filesystem(self.trace)
        self.compatibility = _Compatibility(self.files)
        self.wrong_default_identity = wrong_default_identity

    def run(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
    ) -> EcwCommandResult:
        self.trace.append(f"run:{command}:{timeout}:user={user}")
        if command.startswith('printf "%s\\n"'):
            if user == "root" or self.wrong_default_identity:
                return EcwCommandResult(
                    0,
                    "root\n/root\n/root\nroot\nroot\n"
                    "/root:root:root:/bin/bash:/root/.cache:/var/mail/root:/usr/bin",
                    "",
                )
            return EcwCommandResult(
                0,
                "user\n/home/user\n/home/user\nuser\nuser\n"
                "/home/user:user:user:/bin/bash:/home/user/.cache:"
                "/var/mail/user:/usr/bin",
                "",
            )
        if command == "id -u user":
            return EcwCommandResult(0, "2000\n", "")
        if command.startswith("stat -c "):
            paths = shlex.split(command.partition("-- ")[2])
            rows = "\n".join(f"{self.files.owners[path]} {path}" for path in paths)
            return EcwCommandResult(0, f"{rows}\n", "")
        if command.startswith("chmod 700 "):
            return EcwCommandResult(0, "", "")
        raise AssertionError(f"unexpected command: {command}")


def _fixture(sandbox: _Sandbox) -> Ecw:
    return Ecw(
        ScenarioContext(ResourceScope(), {}),
        cast(EcwSandbox, sandbox),
    )


class EcwIdentityScenarioTests(unittest.TestCase):
    def test_process_identity_checks_omitted_and_explicit_root(self) -> None:
        sandbox = _Sandbox()

        process_identity.verify(_fixture(sandbox))

        self.assertEqual(
            len([item for item in sandbox.trace if item.startswith("run:")]), 2
        )
        self.assertIn("user=None", sandbox.trace[0])
        self.assertIn("user=root", sandbox.trace[1])

    def test_process_identity_rejects_privileged_omitted_identity(self) -> None:
        sandbox = _Sandbox(wrong_default_identity=True)

        result = execute_scenario(
            title="Process identity",
            body=lambda context: process_identity.verify(
                Ecw(context, cast(EcwSandbox, sandbox))
            ),
        )

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(
            result.summary,
            "process identity did not match the selected sandbox user",
        )


    def test_filesystem_read_identity_covers_query_and_basic_auth(self) -> None:
        sandbox = _Sandbox()
        fixture = _fixture(sandbox)

        filesystem_read_identity.verify(fixture)

        self.assertEqual(sandbox.files.owners[READ_ROOT_FILE], 0)
        denials = fixture._context.evidence["permission_denials"]
        self.assertIsInstance(denials, list)
        assert isinstance(denials, list)
        self.assertEqual(
            [item["action"] for item in denials],
            [
                "explicit user read of root-private file",
                "Basic root read without a query identity",
            ],
        )

    def test_filesystem_private_boundary_covers_every_operation(self) -> None:
        sandbox = _Sandbox()
        fixture = _fixture(sandbox)

        filesystem_private_boundary.verify(fixture)

        denials = fixture._context.evidence["permission_denials"]
        self.assertIsInstance(denials, list)
        assert isinstance(denials, list)
        self.assertEqual(
            [item["action"] for item in denials],
            [
                f"{label} {operation} under root-private directory"
                for label in ("omitted", "user")
                for operation in ("MakeDir", "Stat", "ListDir", "Move", "Remove")
            ],
        )

    def test_watcher_identity_uses_root_handle_and_rejects_other_users(self) -> None:
        sandbox = _Sandbox()
        fixture = _fixture(sandbox)

        watcher_identity.verify(fixture)

        self.assertIn(
            f"files.watch:{WATCH_ROOT}:user=root:recursive=True:60",
            sandbox.trace,
        )
        self.assertIn("watcher.events", sandbox.trace)
        self.assertIn("watcher.stop", sandbox.trace)
        self.assertIn(
            f"protocol.watch-stream:{WATCH_ROOT}:user=root:recursive=True:10",
            sandbox.trace,
        )
        denials = fixture._context.evidence["permission_denials"]
        self.assertIsInstance(denials, list)
        assert isinstance(denials, list)
        self.assertEqual(
            [item["action"] for item in denials],
            [
                "omitted CreateWatcher under root-private directory",
                "user CreateWatcher under root-private directory",
                "omitted WatchDir under root-private directory",
                "user WatchDir under root-private directory",
            ],
        )

    def test_watcher_cleanup_denial_does_not_count_as_creation_denial(self) -> None:
        sandbox = _Sandbox()
        sandbox.files = _FilesystemWithStopDenied(sandbox.trace)

        result = execute_scenario(
            title="Watcher creation denial",
            body=lambda context: Ecw(
                context,
                cast(EcwSandbox, sandbox),
            ).identity.expect_watcher_denied(
                WATCH_ROOT,
                user="user",
                recursive=True,
            ),
        )

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(
            result.summary,
            "user CreateWatcher under root-private directory unexpectedly succeeded",
        )
        self.assertEqual(
            result.evidence["unexpected_watcher_cleanup_errors"],
            ["RemoveWatcher denied"],
        )


if __name__ == "__main__":
    unittest.main()
