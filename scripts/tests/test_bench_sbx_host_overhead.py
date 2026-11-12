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


def sample(at, vmm=None, cpu=None, sandboxes=()):
    return overhead.Sample(
        at=at,
        cpu=cpu or {},
        vmm=vmm or {},
        memory={name: 0 for name in overhead.MEMORY_CATEGORIES},
        sandboxes=set(sandboxes),
    )


def write_stat(path: Path, pid: str, comm: str, utime: int, stime: int, cutime: int = 0, cstime: int = 0) -> None:
    """Write a /proc stat line; times are in clock ticks."""
    path.mkdir(parents=True, exist_ok=True)
    rest = ["S"] + ["0"] * 10 + [str(utime), str(stime), str(cutime), str(cstime)] + ["0"] * 6 + ["256"]
    (path / "stat").write_text(f"{pid} ({comm}) {' '.join(rest)}\n")


class WindowTests(unittest.TestCase):
    def test_guest_vcpu_is_excluded_from_host_cpu_per_sandbox_minute(self):
        window = overhead.Window(started=0)
        window.add(
            sample(0, {"a": (0, {"11": 0})}, {("sidecar", "a"): 0, ("node_daemons", "space-volmgr-sv:7"): 0}, {"a"}),
            sample(60, {"a": (31, {"11": 30})}, {("sidecar", "a"): 0.5, ("node_daemons", "space-volmgr-sv:7"): 1.5}, {"a"}),
        )
        row = window.row("total")
        self.assertEqual(row["sandbox_min"], 1.0)
        self.assertEqual(row["host_cpu_s"], 3.0)
        self.assertEqual(row["host_cpu_s_per_sandbox_min"], 3.0)
        self.assertEqual(row["guest_cpu_s_per_sandbox_min"], 30.0)
        self.assertEqual(window.daemons, {"space-volmgr-sv": 1.5})

    def test_teardown_does_not_turn_lifetime_guest_cpu_into_host(self):
        # 600 s of guest CPU before sampling; the vCPU thread exits while the cgroup lingers.
        window = overhead.Window(started=0)
        running = sample(0, {"a": (601, {"11": 600})}, sandboxes={"a"})
        draining = sample(5, {"a": (603, {})})
        gone = sample(10)
        window.add(running, draining)
        window.add(draining, gone)
        row = window.row("total")
        self.assertEqual(row["host_cpu_s"], 0.0)
        self.assertEqual(row["guest_vcpu_cpu_s"], 0.0)
        self.assertEqual(row["unattributed_cpu_s"], 2.0)


class SampleTests(unittest.TestCase):
    def test_synthetic_proc_splits_vcpu_threads_and_excludes_sandbox_pids_from_daemons(self):
        with tempfile.TemporaryDirectory() as tmp:
            cgroups, proc = Path(tmp) / "space", Path(tmp) / "proc"
            for name, usec, pids in (("abc", 2_000_000, "100\n"), ("sbx-abc", 250_000, "")):
                (cgroups / name).mkdir(parents=True)
                (cgroups / name / "cpu.stat").write_text(f"usage_usec {usec}\nuser_usec 0\n")
                (cgroups / name / "memory.current").write_text("4096\n")
                (cgroups / name / "cgroup.procs").write_text(pids)
            tick = overhead.CLK_TCK
            # Named like a daemon so only sandbox-cgroup membership keeps it out of node_daemons.
            write_stat(proc / "100", "100", "space-fc", tick, 0)
            write_stat(proc / "100" / "task" / "101", "101", "fc_vcpu 0", tick, 0)
            write_stat(proc / "100" / "task" / "102", "102", "fc_api", tick // 2, 0)
            # Reaped helpers (cutime/cstime) count toward the daemon that spawned them.
            write_stat(proc / "200", "200", "space-volmgr-sv", tick, 0, cutime=2 * tick, cstime=tick)
            write_stat(proc / "300", "300", "space-x (y)", tick, 0)
            result = overhead.take_sample(cgroups, "space-", proc_root=proc)
        self.assertEqual(result.vmm, {"abc": (2.0, {"101": 1.0})})
        self.assertEqual(result.sandboxes, {"abc"})
        self.assertEqual(result.cpu[("sidecar", "abc")], 0.25)
        self.assertEqual(result.cpu[("node_daemons", "space-volmgr-sv:200")], 4.0)
        self.assertEqual(result.cpu[("node_daemons", "space-x (y):300")], 1.0)
        self.assertNotIn(("node_daemons", "space-fc:100"), result.cpu)


if __name__ == "__main__":
    unittest.main()
