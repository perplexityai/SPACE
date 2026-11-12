#!/usr/bin/env python3
"""Attribute one SPACE worker node's CPU and memory to host overhead or guest work.

Run on the worker node while any sandbox workload runs. The sampler reads
/proc and the SPACE cgroup tree every --interval-secs and writes per-window
CPU seconds for each category, plus host CPU-seconds per sandbox-minute.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK")
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
CATEGORIES = ("guest_vcpu", "vmm_non_vcpu", "sidecar", "node_daemons")
HOST_CATEGORIES = ("vmm_non_vcpu", "sidecar", "node_daemons")
# The VMM cgroup charges guest RAM and VMM memory together, so memory has its own categories.
MEMORY_CATEGORIES = ("vmm_cgroup", "sidecar", "node_daemons")


@dataclass
class Sample:
    """Cumulative CPU seconds and current memory at one instant."""

    at: float
    # (category, key) -> cumulative CPU seconds. Keys identify one sandbox or daemon process.
    cpu: dict[tuple[str, str], float] = field(default_factory=dict)
    # category -> resident bytes now.
    memory: dict[str, int] = field(default_factory=dict)
    sandboxes: set[str] = field(default_factory=set)


def read_text(path: Path) -> str | None:
    try:
        return path.read_text()
    except OSError:
        return None


def stat_cpu_and_rss(path: Path) -> tuple[str, float, int] | None:
    """Return (comm, utime+stime seconds, rss bytes) from a /proc stat file."""
    raw = read_text(path)
    if raw is None:
        return None
    comm = raw[raw.find("(") + 1 : raw.rfind(")")]
    fields = raw[raw.rfind(")") + 2 :].split()
    # Fields after comm start at state (field 3): utime is 14, stime 15, rss 24.
    return comm, (int(fields[11]) + int(fields[12])) / CLK_TCK, int(fields[21]) * PAGE_SIZE


def cgroup_usage_secs(directory: Path) -> float | None:
    raw = read_text(directory / "cpu.stat")
    if raw is None:
        return None
    for line in raw.splitlines():
        key, _, value = line.partition(" ")
        if key == "usage_usec":
            return int(value) / 1e6
    return None


def cgroup_memory(directory: Path) -> int:
    raw = read_text(directory / "memory.current")
    return int(raw) if raw and raw.strip().isdigit() else 0


def cgroup_pids(directory: Path) -> list[str]:
    raw = read_text(directory / "cgroup.procs") or ""
    return raw.split()


def vcpu_secs(pid: str) -> float:
    total = 0.0
    try:
        tasks = list(Path(f"/proc/{pid}/task").iterdir())
    except OSError:
        return 0.0
    for task in tasks:
        parsed = stat_cpu_and_rss(task / "stat")
        if parsed and parsed[0].startswith("fc_vcpu"):
            total += parsed[1]
    return total


def take_sample(cgroup_root: Path, daemon_prefix: str) -> Sample:
    sample = Sample(at=time.monotonic(), memory={name: 0 for name in MEMORY_CATEGORIES})
    sandbox_pids: set[str] = set()
    try:
        children = sorted(cgroup_root.iterdir())
    except OSError:
        children = []
    for child in children:
        if not child.is_dir():
            continue
        usage = cgroup_usage_secs(child)
        if usage is None:
            continue
        pids = cgroup_pids(child)
        sandbox_pids.update(pids)
        if child.name.startswith("sbx-"):
            # Per-sandbox sidecars: net-gateway and spaced-host.
            sample.cpu[("sidecar", child.name[4:])] = usage
            sample.memory["sidecar"] += cgroup_memory(child)
            continue
        # VMM cgroup: Firecracker vCPU threads are guest work, every other thread is host.
        vcpu = sum(vcpu_secs(pid) for pid in pids)
        sample.cpu[("guest_vcpu", child.name)] = vcpu
        sample.cpu[("vmm_non_vcpu", child.name)] = max(0.0, usage - vcpu)
        sample.memory["vmm_cgroup"] += cgroup_memory(child)
        if pids:
            sample.sandboxes.add(child.name)
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or proc.name in sandbox_pids:
            continue
        parsed = stat_cpu_and_rss(proc / "stat")
        if parsed is None or not parsed[0].startswith(daemon_prefix):
            continue
        comm, cpu, rss = parsed
        sample.cpu[("node_daemons", f"{comm}:{proc.name}")] = cpu
        sample.memory["node_daemons"] += rss
    return sample


@dataclass
class Window:
    started: float
    seconds: float = 0.0
    sandbox_secs: float = 0.0
    cpu: dict[str, float] = field(default_factory=lambda: {name: 0.0 for name in CATEGORIES})
    daemons: dict[str, float] = field(default_factory=dict)
    memory_peak: dict[str, int] = field(default_factory=lambda: {name: 0 for name in MEMORY_CATEGORIES})
    sandboxes_peak: int = 0

    def add(self, before: Sample, after: Sample) -> None:
        elapsed = after.at - before.at
        self.seconds += elapsed
        # A sandbox counts for an interval only when it ran at both ends of it.
        self.sandbox_secs += len(before.sandboxes & after.sandboxes) * elapsed
        self.sandboxes_peak = max(self.sandboxes_peak, len(after.sandboxes))
        for key, value in after.cpu.items():
            previous = before.cpu.get(key)
            if previous is None:
                continue
            delta = max(0.0, value - previous)
            category, name = key
            self.cpu[category] += delta
            if category == "node_daemons":
                daemon = name.rsplit(":", 1)[0]
                self.daemons[daemon] = self.daemons.get(daemon, 0.0) + delta
        for category, value in after.memory.items():
            self.memory_peak[category] = max(self.memory_peak[category], value)

    def row(self, label: str) -> dict[str, object]:
        host = sum(self.cpu[name] for name in HOST_CATEGORIES)
        sandbox_min = self.sandbox_secs / 60
        return {
            "window": label,
            "seconds": round(self.seconds, 1),
            "sandbox_min": round(sandbox_min, 3),
            "sandboxes_peak": self.sandboxes_peak,
            **{f"{name}_cpu_s": round(self.cpu[name], 3) for name in CATEGORIES},
            "host_cpu_s": round(host, 3),
            "host_cpu_s_per_min": round(host / (self.seconds / 60), 3) if self.seconds else None,
            "host_cpu_s_per_sandbox_min": round(host / sandbox_min, 3) if sandbox_min else None,
            "guest_cpu_s_per_sandbox_min": round(self.cpu["guest_vcpu"] / sandbox_min, 3) if sandbox_min else None,
            **{f"{name}_mem_peak_mib": round(self.memory_peak[name] / 2**20, 1) for name in MEMORY_CATEGORIES},
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--duration-secs", type=float, default=600, help="Total sampling time.")
    parser.add_argument("--window-secs", type=float, default=0, help="Also report consecutive windows of this length; 0 reports only the total.")
    parser.add_argument("--interval-secs", type=float, default=5, help="Time between samples.")
    parser.add_argument("--cgroup-root", type=Path, default=Path("/sys/fs/cgroup/space"), help="Parent of the per-sandbox VMM and sbx-* sidecar cgroups.")
    parser.add_argument("--daemon-prefix", default="space-", help="Process-name prefix of node daemons outside sandbox cgroups.")
    parser.add_argument("--output", type=Path, default=None, help="Write the JSON report here as well as to stdout.")
    args = parser.parse_args()
    if args.interval_secs <= 0 or args.duration_secs < args.interval_secs:
        raise SystemExit("ERROR: need 0 < --interval-secs <= --duration-secs")
    return args


def main() -> int:
    args = parse_args()
    if not args.cgroup_root.is_dir():
        raise SystemExit(f"ERROR: {args.cgroup_root} not found; run on a SPACE worker node")
    total = Window(started=time.monotonic())
    windows: list[Window] = []
    previous = take_sample(args.cgroup_root, args.daemon_prefix)
    deadline = previous.at + args.duration_secs
    while previous.at < deadline:
        time.sleep(max(0.0, min(args.interval_secs, deadline - previous.at)))
        current = take_sample(args.cgroup_root, args.daemon_prefix)
        total.add(previous, current)
        if args.window_secs > 0:
            # Half an interval of slack keeps sleep jitter from splitting a window early.
            if not windows or previous.at - windows[-1].started >= args.window_secs - args.interval_secs / 2:
                windows.append(Window(started=previous.at))
            windows[-1].add(previous, current)
        previous = current
    rows = [window.row(f"w{index}") for index, window in enumerate(windows, start=1)] + [total.row("total")]
    report = {
        "node": os.uname().nodename,
        "interval_secs": args.interval_secs,
        "windows": rows,
        "node_daemons_cpu_s": {name: round(value, 3) for name, value in sorted(total.daemons.items())},
    }
    text = json.dumps(report, indent=2)
    print(text)
    if args.output:
        args.output.write_text(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
