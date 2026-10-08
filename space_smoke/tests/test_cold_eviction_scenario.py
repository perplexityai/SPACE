"""Contracts of the cold-eviction scenario the bench cannot exercise.

The live run proves the loop evicts and the sandbox wakes from S3; nothing
here restates that. Three things a passing bench run would not catch: the
on-disk probe reading an exec failure as "gone", the event scan counting
another sandbox's or a *failed* eviction as this one's success, and a bench
with the loop off reading as a product failure instead of a configuration
error.
"""

from __future__ import annotations

import json
import unittest

from space_smoke.adapters import (
    KindConfig,
    KindHarness,
    _cold_eviction_config_from_env,
)
from space_smoke.runtime import CommandResult, SmokeConfigurationError



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
        if verb == "get" and "jsonpath={.items[*].metadata.name}" in argv:
            return CommandResult(argv, 0, "space-node-debug-x", "")
        if verb == "get" and "pods" in argv:
            return CommandResult(
                argv,
                0,
                json.dumps(
                    {"items": [{"metadata": {"name": "space-sandbox-manager-x"}, "spec": {"nodeName": "worker-1"}}]}
                ),
                "",
            )
        return CommandResult(argv, 0, "", "")


class CacheDirProbeFailsClosed(unittest.TestCase):
    def test_present_absent_and_exec_failure(self) -> None:
        argv = ("kubectl",)
        kubectl = _RecordingKubectl(
            exec_results=[
                CommandResult(argv, 0, "0\n", ""),
                CommandResult(argv, 0, "1\n", ""),
                CommandResult(argv, 1, "", "error: unable to upgrade connection: pod does not exist"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        sid = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
        self.assertTrue(harness.sandbox_cache_dir_present("p", sid))
        self.assertFalse(harness.sandbox_cache_dir_present("p", sid))
        with self.assertRaises(RuntimeError):
            harness.sandbox_cache_dir_present("p", sid)


class EvictionEventScan(unittest.TestCase):
    def test_selects_only_this_sandboxes_completed_events(self) -> None:
        sid = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
        other = "11a0b0c9-b8a3-7473-9c12-3821a701bc28"
        lines = [
            json.dumps({"event_type": "local_eviction_completed", "sandbox_id": sid, "reason": "latest_backed_idle", "snapshot_id": "snap-1"}),
            json.dumps({"event_type": "local_eviction_failed", "sandbox_id": sid, "reason": "x"}),
            json.dumps({"event_type": "local_eviction_completed", "sandbox_id": other}),
            "not json " + sid + ' "local_eviction_completed"',
        ]
        harness = KindHarness(_RecordingKubectl(logs="\n".join(lines)), KindConfig())
        events = harness.local_evictions(sid, since_time="2026-01-01T00:00:00Z")
        self.assertEqual([(e.node_name, e.reason, e.snapshot_id) for e in events], [("worker-1", "latest_backed_idle", "snap-1")])


class BenchWithTheLoopOffIsAConfigurationError(unittest.TestCase):
    def test_loop_off_is_refused_not_failed(self) -> None:
        from space_smoke.fixtures import ColdEvictionAssertions

        class Control:
            def cold_eviction_config(self, node):
                return _cold_eviction_config_from_env({})

        assertions = ColdEvictionAssertions(None, Control(), None)  # type: ignore[arg-type]
        with self.assertRaises(SmokeConfigurationError):
            assertions.require_smoke_sized_loop("p")

if __name__ == "__main__":
    unittest.main()
