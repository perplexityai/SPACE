"""Contracts of the snapshot-reaper scenario the bench cannot exercise.

The live run proves the reaper reclaims the fork's source artifact. Two things
a passing run would not catch: the snapshots-dir probe reading an exec failure
as "reaped", and the deletion-line scan accepting the orphan sweep's line
(same message, `candidate=tick-sweep`) — which is evidence of a *failed*
node-mgr delete — another sandbox's reap on the same worker, or a malformed
line as this source's reap. Two more the bench never exercises: fork-child
cleanup classifying a transient DELETE response as retryable (and the shared
cleanup policy actually retrying it), and an unreadable snapshots dir not
reading as "reaped".
"""

from __future__ import annotations

import io
import json
import types
import unittest
from email.message import Message
from unittest import mock
from urllib.error import HTTPError, URLError

from space_smoke.adapters import (
    REAPER_DELETED_MESSAGE,
    EcwConfig,
    KindConfig,
    KindHarness,
    _SdkSandbox,
)
from space_smoke.capabilities import EcwSandboxBusy, EcwSandboxDeleteRetryable
from space_smoke.fixtures import SANDBOX_CLEANUP_ATTEMPTS, retry_sandbox_delete
from space_smoke.runtime import CommandResult


class _RecordingKubectl:
    def __init__(self, *, exec_results=None, logs="") -> None:
        self.calls: list[tuple[str, ...]] = []
        self.exec_results = list(exec_results or [])
        self.logs = logs

    def run(self, command, **_options) -> CommandResult:
        argv = tuple(command)
        self.calls.append(argv)
        verb = argv[5]
        if verb == "exec" and self.exec_results:
            return self.exec_results.pop(0)
        if verb == "logs":
            return CommandResult(argv, 0, self.logs, "")
        if verb == "get":
            return CommandResult(argv, 0, "worker-1", "")
        return CommandResult(argv, 0, "", "")


class SnapshotDirProbeFailsClosed(unittest.TestCase):
    def test_entries_none_unreadable_and_exec_failure(self) -> None:
        argv = ("kubectl",)
        kubectl = _RecordingKubectl(
            exec_results=[
                CommandResult(argv, 0, "d-1234\n", ""),
                CommandResult(argv, 0, "\n", ""),
                # `ls` exits 2 for an unreadable dir as well as a missing one;
                # only a truly absent path may read as "no snapshots".
                CommandResult(argv, 2, "", "ls: cannot open directory: Permission denied"),
                CommandResult(argv, 1, "", "error: unable to upgrade connection: pod does not exist"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        sid = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
        self.assertEqual(harness.sandbox_snapshot_dirs("p", sid), ("d-1234",))
        self.assertEqual(harness.sandbox_snapshot_dirs("p", sid), ())
        with self.assertRaisesRegex(RuntimeError, "Permission denied"):
            harness.sandbox_snapshot_dirs("p", sid)
        with self.assertRaises(RuntimeError):
            harness.sandbox_snapshot_dirs("p", sid)
        # The on-node script only lists a path that exists; anything else
        # `ls` says is propagated, not swallowed.
        script = next(call for call in kubectl.calls if call[5] == "exec")[-1]
        self.assertIn("if [ ! -e ", script)
        self.assertNotIn("2>/dev/null", script)


def _http_error(status: int, body: bytes = b"") -> HTTPError:
    return HTTPError("http://api/sandboxes/child", status, "status", Message(), io.BytesIO(body))


class ForkChildDeleteClassifiesLikeTheSdkPath(unittest.TestCase):
    def _sandbox(self) -> _SdkSandbox:
        return _SdkSandbox(
            types.SimpleNamespace(sandbox_id="parent"),
            EcwConfig(api_key="key", api_url="http://127.0.0.1:8080/"),
        )

    def test_not_found_is_already_gone(self) -> None:
        with mock.patch("space_smoke.adapters.urlopen", side_effect=_http_error(404)):
            self.assertIsNone(self._sandbox().delete_forked("child"))

    def test_busy_transient_and_transport_are_retryable(self) -> None:
        cases = [
            (_http_error(409, b'{"message":"lifecycle transition in flight"}'), EcwSandboxBusy),
            (_http_error(408), EcwSandboxDeleteRetryable),
            (_http_error(429), EcwSandboxDeleteRetryable),
            (_http_error(502), EcwSandboxDeleteRetryable),
            (_http_error(503), EcwSandboxDeleteRetryable),
            (_http_error(504), EcwSandboxDeleteRetryable),
            (URLError("connection refused"), EcwSandboxDeleteRetryable),
            (TimeoutError("timed out"), EcwSandboxDeleteRetryable),
        ]
        for failure, expected in cases:
            with self.subTest(failure=failure):
                with (
                    mock.patch("space_smoke.adapters.urlopen", side_effect=failure),
                    self.assertRaises(expected),
                ):
                    self._sandbox().delete_forked("child")

    def test_other_statuses_are_real_failures(self) -> None:
        with (
            mock.patch("space_smoke.adapters.urlopen", side_effect=_http_error(500, b"boom")),
            self.assertRaisesRegex(RuntimeError, "HTTP 500: boom") as caught,
        ):
            self._sandbox().delete_forked("child")
        self.assertNotIsInstance(caught.exception, EcwSandboxDeleteRetryable)


class SharedCleanupRetryPolicy(unittest.TestCase):
    def test_transient_then_success(self) -> None:
        outcomes = [EcwSandboxDeleteRetryable("503"), EcwSandboxBusy("409"), None]

        def delete() -> None:
            outcome = outcomes.pop(0)
            if outcome is not None:
                raise outcome

        with mock.patch("space_smoke.fixtures.time.sleep") as sleep:
            self.assertEqual(retry_sandbox_delete(delete), 3)
        self.assertEqual(sleep.call_count, 2)

    def test_exhausted_retries_raise_the_last_failure(self) -> None:
        calls = 0

        def delete() -> None:
            nonlocal calls
            calls += 1
            raise EcwSandboxDeleteRetryable(f"503 attempt {calls}")

        with (
            mock.patch("space_smoke.fixtures.time.sleep") as sleep,
            self.assertRaisesRegex(EcwSandboxDeleteRetryable, f"attempt {SANDBOX_CLEANUP_ATTEMPTS}$"),
        ):
            retry_sandbox_delete(delete)
        self.assertEqual(calls, SANDBOX_CLEANUP_ATTEMPTS)
        self.assertEqual(sleep.call_count, SANDBOX_CLEANUP_ATTEMPTS - 1)

    def test_non_retryable_failures_propagate_at_once(self) -> None:
        with (
            mock.patch("space_smoke.fixtures.time.sleep") as sleep,
            self.assertRaises(RuntimeError),
        ):
            retry_sandbox_delete(mock.Mock(side_effect=RuntimeError("HTTP 500")))
        sleep.assert_not_called()


class ReapedLineScan(unittest.TestCase):
    def test_selects_this_sandboxes_per_candidate_lines_only(self) -> None:
        sid = "01a0b2d7-ebcb-7770-81e1-1570ccc2b2af"
        other = "11a0b2d7-ebcb-7770-81e1-1570ccc2b2af"
        lines = [
            json.dumps({"message": REAPER_DELETED_MESSAGE, "candidate": "aaaaaaaa-0000-7000-8000-000000000001", "staging_path": f"/space/cache/firecracker/sandboxes/{sid}/snapshots/d-1.gc-pending-x"}),
            json.dumps({"message": "snapshot reaper tick complete", "deleted": 1}),
            # Another sandbox's reap on the same worker (nested tracing layout).
            json.dumps({"fields": {"message": REAPER_DELETED_MESSAGE, "candidate": "bbbbbbbb-0000-7000-8000-000000000002", "staging_path": f"/space/cache/firecracker/sandboxes/{other}/snapshots/d-2.gc-pending-y"}}),
            # The orphan sweep reuses the message; its candidate is a label,
            # not an artifact id, and the line means an earlier delete failed.
            json.dumps({"message": REAPER_DELETED_MESSAGE, "candidate": "tick-sweep", "staging_path": f"/space/cache/firecracker/sandboxes/{sid}/snapshots/d-3.gc-pending-z"}),
            "not json " + REAPER_DELETED_MESSAGE,
        ]
        harness = KindHarness(_RecordingKubectl(logs="\n".join(lines)), KindConfig())
        # The SDK may hand back a non-canonical id form; the join canonicalizes.
        reaped = harness.reaped_artifacts("worker-1", sid.upper(), since_time="2026-01-01T00:00:00Z")
        self.assertEqual(
            [(r.node_name, r.local_artifact_id[:8]) for r in reaped],
            [("worker-1", "aaaaaaaa")],
        )


if __name__ == "__main__":
    unittest.main()
