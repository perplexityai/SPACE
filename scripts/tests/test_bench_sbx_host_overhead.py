"""Tests for the host overhead sampler's attribution and sandbox-minute accounting."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parents[1] / "benchmarks"
spec = importlib.util.spec_from_file_location("bench_sbx_host_overhead", BENCH_DIR / "bench-sbx-host-overhead.py")
overhead = importlib.util.module_from_spec(spec)
sys.modules["bench_sbx_host_overhead"] = overhead
spec.loader.exec_module(overhead)


def sample(at: float, cpu: dict[tuple[str, str], float], sandboxes: set[str]) -> overhead.Sample:
    return overhead.Sample(at=at, cpu=cpu, memory={name: 0 for name in overhead.MEMORY_CATEGORIES}, sandboxes=sandboxes)


class WindowTests(unittest.TestCase):
    def test_guest_vcpu_is_excluded_from_host_cpu_per_sandbox_minute(self):
        window = overhead.Window(started=0)
        window.add(
            sample(0, {("guest_vcpu", "a"): 0, ("vmm_non_vcpu", "a"): 0, ("sidecar", "a"): 0, ("node_daemons", "space-volmgr-sv:7"): 0}, {"a"}),
            sample(60, {("guest_vcpu", "a"): 30, ("vmm_non_vcpu", "a"): 1, ("sidecar", "a"): 0.5, ("node_daemons", "space-volmgr-sv:7"): 1.5}, {"a"}),
        )
        row = window.row("total")
        self.assertEqual(row["sandbox_min"], 1.0)
        self.assertEqual(row["host_cpu_s"], 3.0)
        self.assertEqual(row["host_cpu_s_per_sandbox_min"], 3.0)
        self.assertEqual(row["guest_cpu_s_per_sandbox_min"], 30.0)
        self.assertEqual(window.daemons, {"space-volmgr-sv": 1.5})

    def test_entities_seen_at_only_one_end_add_no_cpu_or_sandbox_time(self):
        window = overhead.Window(started=0)
        window.add(
            sample(0, {("vmm_non_vcpu", "gone"): 5}, {"gone"}),
            sample(10, {("vmm_non_vcpu", "new"): 9}, {"new"}),
        )
        row = window.row("total")
        self.assertEqual(row["sandbox_min"], 0.0)
        self.assertEqual(row["host_cpu_s"], 0.0)
        self.assertIsNone(row["host_cpu_s_per_sandbox_min"])


class SampleTests(unittest.TestCase):
    def test_cgroup_tree_splits_vmm_and_sidecar_cgroups(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, usec, memory in (("abc", 2_000_000, 512 << 20), ("sbx-abc", 250_000, 4 << 20)):
                (root / name).mkdir()
                (root / name / "cpu.stat").write_text(f"usage_usec {usec}\nuser_usec 0\n")
                (root / name / "memory.current").write_text(f"{memory}\n")
                (root / name / "cgroup.procs").write_text("")
            # No process name starts with NUL, so the host's daemons stay out of this sample.
            result = overhead.take_sample(root, daemon_prefix="\0")
        self.assertEqual(result.cpu[("vmm_non_vcpu", "abc")], 2.0)
        self.assertEqual(result.cpu[("guest_vcpu", "abc")], 0.0)
        self.assertEqual(result.cpu[("sidecar", "abc")], 0.25)
        self.assertEqual(result.memory["vmm_cgroup"], 512 << 20)
        self.assertEqual(result.memory["sidecar"], 4 << 20)
        # A VMM cgroup without a process is not a running sandbox.
        self.assertEqual(result.sandboxes, set())


if __name__ == "__main__":
    unittest.main()
