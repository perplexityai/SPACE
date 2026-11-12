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
        daemon_names={name: name.split("/")[0] for category, name in (cpu or {}) if category == "node_daemons"},
        vmm=vmm or {},
        memory={name: 0 for name in overhead.MEMORY_CATEGORIES},
        sandboxes=set(sandboxes),
    )


def write_stat(path: Path, pid: str, comm: str, utime: int, stime: int, cutime: int = 0, cstime: int = 0, cgroup: str = "/") -> None:
    """Write a /proc stat line (times in clock ticks) and the process's cgroup v2 path."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "cgroup").write_text(f"0::{cgroup}\n")
    rest = ["S"] + ["0"] * 10 + [str(utime), str(stime), str(cutime), str(cstime)] + ["0"] * 6 + ["256"]
    (path / "stat").write_text(f"{pid} ({comm}) {' '.join(rest)}\n")


class WindowTests(unittest.TestCase):
    def test_guest_vcpu_is_excluded_from_host_cpu_per_sandbox_minute(self):
        window = overhead.Window(started=0)
        window.add(
            sample(0, {"a": (0, {"11": 0})}, {("sidecar", "a"): 0, ("node_daemons", "space-volmgr-sv/svc"): 0}, {"a"}),
            sample(60, {"a": (31, {"11": 30})}, {("sidecar", "a"): 0.5, ("node_daemons", "space-volmgr-sv/svc"): 1.5}, {"a"}),
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
            mount, proc = Path(tmp) / "cgroup", Path(tmp) / "proc"
            cgroups = mount / "space"
            for name, usec, pids in (("space/abc", 2_000_000, "100\n"), ("space/sbx-abc", 250_000, ""), ("runtime.service", 1_500_000, "200\n")):
                (mount / name).mkdir(parents=True)
                (mount / name / "cpu.stat").write_text(f"usage_usec {usec}\nuser_usec 0\n")
                (mount / name / "memory.current").write_text("4096\n")
                (mount / name / "cgroup.procs").write_text(pids)
            tick = overhead.CLK_TCK
            # Named like a daemon so only sandbox-cgroup membership keeps it out of node_daemons.
            write_stat(proc / "100", "100", "space-fc", tick, 0)
            write_stat(proc / "100" / "task" / "101", "101", "fc_vcpu 0", tick, 0)
            write_stat(proc / "100" / "task" / "102", "102", "fc_api", tick // 2, 0)
            # This daemon reaped a sidecar (cutime/cstime); its cgroup holds only its own and helper CPU.
            write_stat(proc / "200", "200", "space-runtime-s", tick, 0, cutime=20 * tick, cstime=tick, cgroup="/runtime.service")
            # A daemon in the root cgroup falls back to its own threads.
            write_stat(proc / "300", "300", "space-x (y)", tick, 0, cutime=5 * tick)
            result = overhead.take_sample(cgroups, "space-", proc_root=proc, cgroup_mount=mount)
        self.assertEqual(result.vmm, {"abc": (2.0, {"101": 1.0})})
        self.assertEqual(result.sandboxes, {"abc"})
        self.assertEqual(result.cpu[("sidecar", "abc")], 0.25)
        daemons = {result.daemon_names[name]: cpu for (category, name), cpu in result.cpu.items() if category == "node_daemons"}
        self.assertEqual(daemons, {"space-runtime-s": 1.5, "space-x (y)": 1.0})


if __name__ == "__main__":
    unittest.main()
