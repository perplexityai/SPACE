#!/usr/bin/env python3
"""Suspend/restore memory benchmark for comparing balloon template variants.

Runs on one SPACE worker node (directly or staged over SSH, like
bench-sbx-concurrent.py). For each variant (a template name tagged with a
label such as ``baseline`` or ``inflation``) it creates sandboxes, assigns each
one a guest workload profile, writes a memory and rootfs correctness pattern,
then runs workload -> suspend -> restore -> verify cycles. Every cycle emits one
JSON line to ``results.jsonl``. ``bench-sbx-balloon-summary.py`` aggregates the
rows and evaluates the Stage 1 gate.

The harness does not configure ballooning itself. Variants differ only by the
template they boot, so it works unchanged when no balloon device exists: the
Firecracker balloon endpoints answer with an error and the balloon fields are
recorded as null.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import random
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Sequence

from sbx_bench_common import (
    SandboxCli,
    UUID_RE,
    add_common_ssh_args,
    checked_prereq,
    choose_sudo_prefix,
    effective_ssh_options,
    ensure_template,
    normalize_target,
    now_ms,
    pull_results,
    run_capture,
    sanitize_error,
    stage_and_run_remote_python,
    timestamp_for_path,
    write_lines,
)

SCHEMA_VERSION = 1
PROFILES = ("build", "alloc-free", "file-heavy", "idle")
MIB = 1024 * 1024
DISK_RE = re.compile(r"^(nvme\d+n\d+|sd[a-z]+|xvd[a-z]+|vd[a-z]+)$")

# Host paths owned by the SPACE node layout (see crates/libs/common/core/src/types/node_fs.rs).
DEFAULT_SANDBOX_CACHE_DIR = "/space/cache/firecracker/sandboxes"
DEFAULT_SANDBOX_RUN_DIR = "/space/run/firecracker/sandboxes"
DEFAULT_JAIL_DIR = "/space/cache/firecracker/jails"

# --- Guest scripts ---------------------------------------------------------
# Each script runs under `sh -c SCRIPT space-bench ARGS...` inside the guest and
# prints key=value lines. They only assume POSIX sh and coreutils/busybox;
# python3 and cc are used when present and reported in the output.

GUEST_PRELUDE = r"""
set -eu
now_ns() { t=$(date +%s%N 2>/dev/null || true); case "$t" in *N*|'') echo "";; *) echo "$t";; esac; }
shm=/dev/shm
if [ ! -d "$shm" ] || [ ! -w "$shm" ]; then shm=/tmp; fi
mem_dir="$shm/space-bench"
disk_dir=/var/tmp/space-bench
# Test hook: lets the harness tests run the guest scripts on a host directory.
if [ -n "${SBX_BENCH_GUEST_ROOT:-}" ]; then mem_dir="$SBX_BENCH_GUEST_ROOT/mem"; disk_dir="$SBX_BENCH_GUEST_ROOT/disk"; fi
"""

GUEST_SETUP = GUEST_PRELUDE + r"""
mem_mb=$1; file_mb=$2; anon_mb=$3; holder=$4
rm -rf "$mem_dir" "$disk_dir"
mkdir -p "$mem_dir" "$disk_dir"
dd if=/dev/urandom of="$mem_dir/pattern" bs=1048576 count="$mem_mb" 2>/dev/null
dd if=/dev/urandom of="$disk_dir/file-pattern" bs=1048576 count="$file_mb" 2>/dev/null
sync
echo "mem_sha=$(sha256sum "$mem_dir/pattern" | cut -d' ' -f1)"
echo "file_sha=$(sha256sum "$disk_dir/file-pattern" | cut -d' ' -f1)"
echo "mem_fstype=$(stat -f -c %T "$mem_dir" 2>/dev/null || echo unknown)"
echo "guest_kernel=$(uname -r)"
if [ "$anon_mb" -gt 0 ] && command -v python3 >/dev/null 2>&1; then
  setsid nohup python3 -c "$holder" "$mem_dir" "$anon_mb" </dev/null >/dev/null 2>&1 &
  i=0
  while [ ! -s "$mem_dir/anon.sha256" ] && [ "$i" -lt 120 ]; do sleep 1; i=$((i+1)); done
  if [ -s "$mem_dir/anon.sha256" ]; then echo "anon_sha=$(cat "$mem_dir/anon.sha256")"; fi
fi
"""

# Long-lived guest process that keeps an anonymous buffer resident across
# suspend/restore and rehashes it on request.
GUEST_ANON_HOLDER = r"""
import hashlib, os, sys, time
d, mb = sys.argv[1], int(sys.argv[2])
buf = bytearray(os.urandom(mb << 20))
def put(name, value):
    tmp = os.path.join(d, name + ".tmp")
    with open(tmp, "w") as f:
        f.write(value)
    os.rename(tmp, os.path.join(d, name))
put("anon.sha256", hashlib.sha256(buf).hexdigest())
while True:
    for name in os.listdir(d):
        if name.startswith("anon.req."):
            token = name[len("anon.req."):]
            os.unlink(os.path.join(d, name))
            put("anon.resp." + token, hashlib.sha256(buf).hexdigest())
    time.sleep(0.2)
"""

GUEST_VERIFY = GUEST_PRELUDE + r"""
token=$1; want_anon=$2
if [ -f "$mem_dir/pattern" ]; then echo "mem_sha=$(sha256sum "$mem_dir/pattern" | cut -d' ' -f1)"; else echo "mem_sha=missing"; fi
if [ -f "$disk_dir/file-pattern" ]; then echo "file_sha=$(sha256sum "$disk_dir/file-pattern" | cut -d' ' -f1)"; else echo "file_sha=missing"; fi
if [ "$want_anon" = 1 ]; then
  rm -f "$mem_dir/anon.resp.$token"
  : > "$mem_dir/anon.req.$token"
  i=0
  while [ ! -s "$mem_dir/anon.resp.$token" ] && [ "$i" -lt 60 ]; do sleep 1; i=$((i+1)); done
  if [ -s "$mem_dir/anon.resp.$token" ]; then echo "anon_sha=$(cat "$mem_dir/anon.resp.$token")"; else echo "anon_sha=missing"; fi
fi
"""

GUEST_WORKLOADS = {
    # Compile generated C sources (or gzip when no compiler), stream a blob
    # through the page cache, then delete everything.
    "build": GUEST_PRELUDE + r"""
units=$1; blob_mb=$2
start=$(now_ns)
d="$disk_dir/build"; rm -rf "$d"; mkdir -p "$d"
i=0
while [ "$i" -lt "$units" ]; do
  printf 'int f%d(int x){int s=0;for(int i=0;i<x;i++)s+=i*%d;return s;}\n' "$i" "$i" > "$d/f$i.c"
  i=$((i+1))
done
tool=gzip
if command -v cc >/dev/null 2>&1; then
  tool=cc
  for f in "$d"/f*.c; do cc -O2 -c "$f" -o "${f%.c}.o"; done
fi
dd if=/dev/urandom of="$d/blob" bs=1048576 count="$blob_mb" 2>/dev/null
gzip -1 -c "$d/blob" > "$d/blob.gz"
cat "$d/blob.gz" > /dev/null
rm -rf "$d"; sync
end=$(now_ns)
echo "build_tool=$tool"
echo "start_ns=$start"; echo "end_ns=$end"
""",
    # Allocate, touch with incompressible data, and free anonymous memory.
    "alloc-free": GUEST_PRELUDE + r"""
alloc_mb=$1
start=$(now_ns)
if command -v python3 >/dev/null 2>&1; then
  method=python-mmap
  python3 - "$alloc_mb" <<'PY'
import mmap, os, sys
size = int(sys.argv[1]) << 20
chunk = os.urandom(64 << 20)
m = mmap.mmap(-1, size)
off = 0
while off < size:
    n = min(len(chunk), size - off)
    m[off:off + n] = chunk[:n]
    off += n
m.close()
PY
else
  method=tmpfs
  dd if=/dev/urandom of="$mem_dir/chunk" bs=1048576 count=64 2>/dev/null
  i=0
  while [ "$i" -lt $((alloc_mb / 64)) ]; do cat "$mem_dir/chunk" >> "$mem_dir/alloc"; i=$((i+1)); done
  rm -f "$mem_dir/alloc" "$mem_dir/chunk"
fi
end=$(now_ns)
echo "alloc_method=$method"; echo "alloc_mb=$alloc_mb"
echo "start_ns=$start"; echo "end_ns=$end"
""",
    # Write and delete files on the rootfs.
    "file-heavy": GUEST_PRELUDE + r"""
total_mb=$1
start=$(now_ns)
d="$disk_dir/files"; rm -rf "$d"; mkdir -p "$d"
i=0
while [ "$i" -lt $((total_mb / 4)) ]; do
  dd if=/dev/urandom of="$d/f$i" bs=1048576 count=4 2>/dev/null
  i=$((i+1))
done
sync
rm -rf "$d"; sync
end=$(now_ns)
echo "files_written=$i"
echo "start_ns=$start"; echo "end_ns=$end"
""",
    "idle": GUEST_PRELUDE + r"""
secs=$1
start=$(now_ns)
sleep "$secs"
end=$(now_ns)
echo "start_ns=$start"; echo "end_ns=$end"
""",
}


# --- Pure helpers (unit tested) -------------------------------------------


def parse_kv(text: str) -> dict[str, str]:
    """Parse ``key=value`` lines; later keys win, other lines are ignored."""
    out: dict[str, str] = {}
    for line in text.splitlines():
        key, sep, value = line.strip().partition("=")
        if sep and re.fullmatch(r"[a-z_][a-z0-9_]*", key):
            out[key] = value
    return out


def assign_profiles(count: int, profiles: Sequence[str], seed: int) -> list[str]:
    """Split ``count`` sandboxes across profiles as evenly as possible.

    Round-robin gives an exact equal split when ``count`` is a multiple of the
    profile count; the seeded shuffle keeps the order from correlating with
    sandbox creation order.
    """
    assigned = [profiles[i % len(profiles)] for i in range(count)]
    random.Random(seed).shuffle(assigned)
    return assigned


def parse_variants(values: Sequence[str], fallback_template: str) -> list[tuple[str, str]]:
    """Parse repeated ``LABEL=TEMPLATE`` values; default to one variant named after the template."""
    if not values:
        return [(fallback_template, fallback_template)]
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for value in values:
        label, sep, template = value.partition("=")
        label, template = label.strip(), template.strip()
        if not sep or not label or not template:
            raise SystemExit(f"ERROR: --variant must be LABEL=TEMPLATE, got {value!r}")
        if label in seen:
            raise SystemExit(f"ERROR: duplicate --variant label {label!r}")
        seen.add(label)
        out.append((label, template))
    return out


def compare_checksums(expected: dict[str, str], observed: dict[str, str]) -> list[str]:
    """Return the names of correctness checks that did not match."""
    failures = []
    for key, name in (("mem_sha", "mem_pattern"), ("file_sha", "file_pattern"), ("anon_sha", "anon_buffer")):
        if key not in expected:
            continue
        if observed.get(key) != expected[key]:
            failures.append(name)
    return failures


def parse_diskstats(text: str) -> int:
    """Total bytes written across whole disks (partitions and virtual devices excluded)."""
    total_sectors = 0
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 10 and DISK_RE.match(parts[2]):
            total_sectors += int(parts[9])
    return total_sectors * 512


def parse_meminfo_kib(text: str, keys: Sequence[str]) -> dict[str, int]:
    out = {}
    for line in text.splitlines():
        name, _, rest = line.partition(":")
        if name in keys and rest.split():
            out[name] = int(rest.split()[0])
    return out


def parse_vm_rss_kib(status_text: str) -> int | None:
    for line in status_text.splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return None


def parse_btrfs_du_exclusive(text: str) -> int | None:
    """Parse ``btrfs filesystem du -s --raw`` output and return the Exclusive column."""
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[0].isdigit() and parts[1].isdigit():
            return int(parts[1])
    return None


def guest_elapsed_ms(kv: dict[str, str]) -> int | None:
    start, end = kv.get("start_ns", ""), kv.get("end_ns", "")
    if start.isdigit() and end.isdigit():
        return (int(end) - int(start)) // 1_000_000
    return None


def http_response_split(text: str) -> tuple[int | None, str]:
    """Split curl output produced with ``-w '\\n%{http_code}'``."""
    body, _, code = text.rstrip("\n").rpartition("\n")
    return (int(code), body) if code.isdigit() else (None, text)


# --- Host probes -----------------------------------------------------------


class HostProbe:
    def __init__(self, prefix: list[str], args: argparse.Namespace) -> None:
        self.prefix = prefix
        self.cache_dir = Path(args.sandbox_cache_dir)
        self.run_dir = Path(args.sandbox_run_dir)
        self.jail_dir = Path(args.jail_dir)
        self.compressed = args.compressed_size and shutil.which("zstd") is not None
        self.curl = shutil.which("curl")
        self.btrfs = shutil.which("btrfs")

    def priv(self, cmd: Sequence[str], timeout: float = 30) -> subprocess.CompletedProcess[str]:
        return run_capture([*self.prefix, *cmd], timeout=timeout)

    def read_text(self, path: Path | str) -> str | None:
        try:
            return Path(path).read_text()
        except PermissionError:
            result = self.priv(["cat", str(path)])
            return result.stdout if result.returncode == 0 else None
        except OSError:
            return None

    def stat_file(self, path: Path) -> tuple[int, int] | None:
        """(apparent bytes, allocated bytes) or None when absent."""
        try:
            st = path.stat()
            return st.st_size, st.st_blocks * 512
        except PermissionError:
            result = self.priv(["stat", "-c", "%s %b %B", str(path)])
            if result.returncode != 0:
                return None
            size, blocks, block_size = (int(v) for v in result.stdout.split()[:3])
            return size, blocks * block_size
        except OSError:
            return None

    def fc_pid(self, sandbox_id: str) -> int | None:
        link = self.run_dir / sandbox_id / "fc.pid"
        try:
            target = os.readlink(link)
        except PermissionError:
            result = self.priv(["readlink", str(link)])
            target = result.stdout.strip() if result.returncode == 0 else ""
        except OSError:
            target = ""
        name = os.path.basename(target)
        return int(name) if name.isdigit() else None

    def fc_rss_kib(self, sandbox_id: str) -> int | None:
        pid = self.fc_pid(sandbox_id)
        if pid is None:
            return None
        text = self.read_text(f"/proc/{pid}/status")
        return parse_vm_rss_kib(text) if text else None

    def firecracker_version(self, sandbox_id: str) -> str | None:
        pid = self.fc_pid(sandbox_id)
        if pid is None:
            return None
        exe = self.priv(["readlink", f"/proc/{pid}/exe"]).stdout.strip()
        if not exe:
            return None
        result = self.priv([exe, "--version"], timeout=10)
        return result.stdout.splitlines()[0].strip() if result.returncode == 0 and result.stdout else None

    def fc_api_get(self, sandbox_id: str, path: str) -> dict[str, Any]:
        """GET a Firecracker API path; never raises. Returns {'status', 'body'|'error'}."""
        if not self.curl:
            return {"status": None, "error": "curl not found"}
        sock = self.jail_dir / "firecracker" / sandbox_id / "root" / "firecracker.sock"
        result = self.priv(
            [self.curl, "-sS", "--max-time", "5", "--unix-socket", str(sock), "-w", "\n%{http_code}", f"http://localhost{path}"],
            timeout=10,
        )
        code, body = http_response_split(result.stdout)
        if result.returncode != 0 or code is None:
            return {"status": code, "error": sanitize_error(result.stdout)}
        try:
            parsed: Any = json.loads(body) if body.strip() else None
        except json.JSONDecodeError:
            parsed = body.strip()
        if code != 200:
            return {"status": code, "error": parsed}
        return {"status": code, "body": parsed}

    def balloon(self, sandbox_id: str, path: str) -> Any:
        response = self.fc_api_get(sandbox_id, path)
        return response.get("body") if response.get("status") == 200 else None

    def compressed_bytes(self, path: Path) -> int | None:
        if not self.compressed:
            return None
        proc = subprocess.Popen([*self.prefix, "zstd", "-3", "-T0", "-q", "-c", str(path)], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        assert proc.stdout is not None
        total = 0
        while True:
            chunk = proc.stdout.read(4 * MIB)
            if not chunk:
                break
            total += len(chunk)
        return total if proc.wait() == 0 else None

    def btrfs_exclusive(self, path: Path) -> int | None:
        if not self.btrfs:
            return None
        result = self.priv([self.btrfs, "filesystem", "du", "-s", "--raw", str(path)], timeout=120)
        return parse_btrfs_du_exclusive(result.stdout) if result.returncode == 0 else None

    def disk_write_bytes(self) -> int | None:
        text = self.read_text("/proc/diskstats")
        return parse_diskstats(text) if text else None

    def btrfs_commits(self) -> int | None:
        """Total btrfs transaction commits across mounted filesystems, if the kernel exposes them."""
        total, seen = 0, False
        for stats in Path("/sys/fs/btrfs").glob("*/commit_stats"):
            text = self.read_text(stats) or ""
            for line in text.splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[0] == "commits" and parts[1].isdigit():
                    total += int(parts[1])
                    seen = True
        return total if seen else None

    def writeback(self) -> dict[str, int]:
        text = self.read_text("/proc/meminfo") or ""
        return parse_meminfo_kib(text, ("Dirty", "Writeback"))

    def snapshot_metrics(self, sandbox_id: str) -> dict[str, Any]:
        snap = self.cache_dir / sandbox_id / "snapshot"
        out: dict[str, Any] = {}
        for key, name in (("snapshot_mem", "memory"), ("snapshot_vmstate", "vmstate"), ("rootfs_snapshot", "rootfs.ext4")):
            st = self.stat_file(snap / name)
            out[f"{key}_bytes"] = st[0] if st else None
            out[f"{key}_disk_bytes"] = st[1] if st else None
        out["snapshot_mem_compressed_bytes"] = (
            self.compressed_bytes(snap / "memory") if out["snapshot_mem_bytes"] is not None else None
        )
        out["rootfs_exclusive_bytes"] = self.btrfs_exclusive(snap / "rootfs.ext4") if out["rootfs_snapshot_bytes"] is not None else None
        return out


# --- Benchmark -------------------------------------------------------------


class BalloonBenchmark:
    def __init__(self, args: argparse.Namespace) -> None:
        if args.sandboxes <= 0 or args.cycles <= 0 or args.concurrency <= 0:
            raise SystemExit("ERROR: --sandboxes, --cycles, and --concurrency must be > 0")
        if args.cycles < 3:
            print(f"WARNING: --cycles {args.cycles} is below the Stage 1 minimum of 3", file=sys.stderr)
        self.profiles = [p.strip() for p in args.profiles.split(",") if p.strip()]
        unknown = [p for p in self.profiles if p not in PROFILES]
        if unknown or not self.profiles:
            raise SystemExit(f"ERROR: unknown --profiles {unknown}; choose from {','.join(PROFILES)}")
        self.variants = parse_variants(args.variant, args.template)
        self.args = args
        self.output_dir = Path(args.output_dir or f"/tmp/space-sbx-balloon-{timestamp_for_path()}")
        self.results_file = self.output_dir / "results.jsonl"
        self.tracked_file = self.output_dir / "tracked-sandboxes.txt"
        self.run_id = f"{socket.gethostname()}-{timestamp_for_path()}"
        self.host = socket.gethostname()
        self.lock = threading.Lock()
        self.fc_version: str | None = None
        self.space_version: str | None = None

        checked_prereq(args.sbxmgr, "space-sbxmgr")
        checked_prereq(args.tmplmgr, "space-tmplmgr")
        self.prefix = choose_sudo_prefix(args.sbxmgr, args.use_sudo)
        for _, template in self.variants:
            ensure_template(self.prefix, args.tmplmgr, template)
        self.probe = HostProbe(self.prefix, args)

    # -- CLI wrappers

    def cli(self, template: str) -> SandboxCli:
        return SandboxCli(
            prefix=self.prefix,
            sbxmgr=self.args.sbxmgr,
            tmplmgr=self.args.tmplmgr,
            nodemgr="/space/bin/space-nodemgr",
            template=template,
            runtime=self.args.runtime,
            boot_mode=self.args.boot_mode,
            user=self.args.user,
        )

    def timed(self, cmd: Sequence[str], timeout: float | None = None) -> tuple[int, int, str]:
        started = now_ms()
        try:
            result = run_capture(cmd, timeout=timeout)
            rc, out = result.returncode, result.stdout
        except subprocess.TimeoutExpired as exc:
            rc, out = 124, f"timeout after {timeout}s: {exc}"
        return rc, now_ms() - started, out

    def guest(self, sandbox_id: str, script: str, *script_args: object, timeout: float | None = None) -> tuple[int, int, str]:
        cmd = [*self.prefix, self.args.sbxmgr, "exec", sandbox_id]
        if self.args.guest_user:
            cmd += ["--user", self.args.guest_user]
        cmd += ["--", "sh", "-c", script, "space-bench", *(str(a) for a in script_args)]
        return self.timed(cmd, timeout=timeout or self.args.guest_timeout_secs)

    def workload_args(self, profile: str, rng: random.Random) -> list[object]:
        a = self.args
        if profile == "build":
            return [a.build_units, a.build_blob_mb]
        if profile == "alloc-free":
            return [rng.choice(range(a.alloc_min_mb, a.alloc_max_mb + 1, 512)) if a.alloc_max_mb > a.alloc_min_mb else a.alloc_min_mb]
        if profile == "file-heavy":
            return [a.file_heavy_mb]
        return [a.idle_secs]

    # -- Output

    def emit(self, row: dict[str, Any]) -> None:
        line = json.dumps(row, sort_keys=True)
        with self.lock:
            with self.results_file.open("a") as f:
                f.write(line + "\n")
        status = "OK" if row.get("correctness_ok") else ("FAIL" if row.get("correctness_ok") is False else "ERR")
        print(
            f"  {row['variant']:12} {row['sandbox_index']:>3} {row['profile']:10} cycle={row['cycle']}"
            f" suspend={row.get('suspend_ms')}ms restore={row.get('restore_ms')}ms"
            f" mem_disk={_mib(row.get('snapshot_mem_disk_bytes'))} {status}"
            + (f" error={row['error']}" if row.get("error") else ""),
            flush=True,
        )

    def track(self, sandbox_id: str) -> None:
        with self.lock:
            write_lines(self.tracked_file, [sandbox_id])

    # -- Per-sandbox pipeline

    def run_sandbox(self, variant: str, template: str, index: int, profile: str) -> None:
        rng = random.Random(f"{self.args.seed}-{variant}-{index}")
        cli = self.cli(template)
        base = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "host": self.host,
            "host_kernel": os.uname().release,
            "variant": variant,
            "template": template,
            "sandbox_index": index,
            "profile": profile,
        }
        rc, _, out = self.timed(cli.create_cmd(autostart=True, snapshots=True), timeout=self.args.op_timeout_secs)
        match = UUID_RE.search(out)
        if rc != 0 or not match:
            self.emit({**base, "cycle": 0, "sandbox_id": None, "correctness_ok": None, "error": f"create failed: {sanitize_error(out)}"})
            return
        sandbox_id = match.group(0)
        self.track(sandbox_id)
        base["sandbox_id"] = sandbox_id
        try:
            self.run_cycles(base, sandbox_id, profile, rng)
        finally:
            if not self.args.no_cleanup:
                cli.delete(sandbox_id)

    def run_cycles(self, base: dict[str, Any], sandbox_id: str, profile: str, rng: random.Random) -> None:
        a = self.args
        rc, _, out = self.guest(sandbox_id, GUEST_SETUP, a.pattern_mem_mb, a.pattern_file_mb, a.anon_check_mb, GUEST_ANON_HOLDER)
        setup = parse_kv(out)
        if rc != 0 or "mem_sha" not in setup or "file_sha" not in setup:
            self.emit({**base, "cycle": 0, "correctness_ok": None, "error": f"pattern setup failed: {sanitize_error(out)}"})
            return
        expected = {k: setup[k] for k in ("mem_sha", "file_sha", "anon_sha") if k in setup}
        base["guest_kernel"] = setup.get("guest_kernel")
        base["mem_pattern_fstype"] = setup.get("mem_fstype")
        base["anon_check"] = "anon_sha" in expected
        if self.fc_version is None:
            self.fc_version = self.probe.firecracker_version(sandbox_id)
        base["firecracker_version"] = self.fc_version
        base["space_version"] = self.space_version
        base["balloon_config"] = self.probe.balloon(sandbox_id, "/balloon")

        for cycle in range(1, a.cycles + 1):
            row: dict[str, Any] = {**base, "cycle": cycle, "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "error": None}
            w_args = self.workload_args(profile, rng)
            rc, ms, out = self.guest(sandbox_id, GUEST_WORKLOADS[profile], *w_args)
            kv = parse_kv(out)
            row.update(workload_ms=ms, workload_rc=rc, workload_guest_ms=guest_elapsed_ms(kv),
                       workload_detail={k: v for k, v in kv.items() if k not in ("start_ns", "end_ns")})
            if rc != 0:
                row["workload_error"] = sanitize_error(out)
            if a.settle_secs:
                time.sleep(a.settle_secs)
            row["fc_rss_before_suspend_kib"] = self.probe.fc_rss_kib(sandbox_id)
            row["balloon_stats_before_suspend"] = self.probe.balloon(sandbox_id, "/balloon/statistics")

            disk0, commits0 = self.probe.disk_write_bytes(), self.probe.btrfs_commits()
            rc, ms, out = self.timed([*self.prefix, a.sbxmgr, "suspend", sandbox_id, "--blocking"], timeout=a.op_timeout_secs)
            disk1 = self.probe.disk_write_bytes()
            row.update(suspend_ms=ms, suspend_rc=rc, host_disk_write_bytes_suspend=_delta(disk0, disk1),
                       host_btrfs_commits_suspend=_delta(commits0, self.probe.btrfs_commits()))
            row["host_writeback_kib_after_suspend"] = self.probe.writeback()
            if rc != 0:
                row.update(correctness_ok=None, error=f"suspend failed: {sanitize_error(out)}")
                self.emit(row)
                return
            row.update(self.probe.snapshot_metrics(sandbox_id))

            restore = [*self.prefix, a.sbxmgr, "restore", sandbox_id, "--blocking"]
            if a.ready_timeout_secs:
                restore += ["--ready-timeout-secs", str(a.ready_timeout_secs)]
            disk0 = self.probe.disk_write_bytes()
            rc, ms, out = self.timed(restore, timeout=a.op_timeout_secs)
            disk1 = self.probe.disk_write_bytes()
            row.update(restore_ms=ms, restore_rc=rc, host_disk_write_bytes_restore=_delta(disk0, disk1))
            if rc != 0:
                row.update(correctness_ok=None, error=f"restore failed: {sanitize_error(out)}")
                self.emit(row)
                return
            row["fc_rss_after_restore_kib"] = self.probe.fc_rss_kib(sandbox_id)

            rc, ms, out = self.guest(sandbox_id, GUEST_VERIFY, cycle, 1 if base["anon_check"] else 0)
            row["verify_ms"] = ms
            if rc != 0:
                row.update(correctness_ok=None, error=f"verify failed to run: {sanitize_error(out)}")
                self.emit(row)
                return
            failures = compare_checksums(expected, parse_kv(out))
            row.update(correctness_ok=not failures, correctness_failures=failures)
            row["fc_rss_after_verify_kib"] = self.probe.fc_rss_kib(sandbox_id)
            self.emit(row)

    # -- Driver

    def run(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results_file.touch()
        self.tracked_file.touch()
        version = run_capture([*self.prefix, self.args.sbxmgr, "--version"])
        self.space_version = version.stdout.strip() if version.returncode == 0 else None
        meta = {
            "run_id": self.run_id,
            "host": self.host,
            "host_kernel": os.uname().release,
            "space_version": self.space_version,
            "variants": [{"variant": v, "template": t} for v, t in self.variants],
            "args": {k: v for k, v in vars(self.args).items() if k not in ("ssh_option",)},
        }
        (self.output_dir / "run.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
        print("=== SPACE balloon suspend/restore benchmark ===")
        print(f"Variants:   {', '.join(f'{v}={t}' for v, t in self.variants)}")
        print(f"Sandboxes:  {self.args.sandboxes} per variant, {self.args.cycles} cycles, concurrency {self.args.concurrency}")
        print(f"Profiles:   {','.join(self.profiles)}")
        print(f"Output:     {self.results_file}\n", flush=True)
        for variant, template in self.variants:
            print(f"--- variant {variant} (template {template})", flush=True)
            profiles = assign_profiles(self.args.sandboxes, self.profiles, self.args.seed)
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.concurrency) as pool:
                futures = [pool.submit(self.run_sandbox, variant, template, i, p) for i, p in enumerate(profiles, start=1)]
                for future in futures:
                    future.result()
        print(f"\nDone. Results: {self.results_file}")
        print(f"Summarize with: python3 {Path(__file__).with_name('bench-sbx-balloon-summary.py')} {self.results_file}")


def _delta(before: int | None, after: int | None) -> int | None:
    return after - before if before is not None and after is not None else None


def _mib(value: Any) -> str:
    return f"{value / MIB:.0f}MiB" if isinstance(value, int) else "-"


# --- CLI -------------------------------------------------------------------

# Options forwarded verbatim to the remote run; SSH/pull options stay local.
LOCAL_ONLY = {"target", "remote_run", "ssh_user", "ssh_option", "pull_results", "pull_raw"}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Balloon suspend/restore benchmark for one SPACE worker node.")
    p.add_argument("target", nargs="?", default="", help="Optional worker host or user@host. If omitted, run on this node.")
    p.add_argument("--remote-run", action="store_true", help=argparse.SUPPRESS)
    add_common_ssh_args(p)
    p.add_argument("--template", default="seed", help="Template for a single unlabeled variant.")
    p.add_argument("--variant", action="append", default=[], metavar="LABEL=TEMPLATE",
                   help="Variant label and template; repeat to run variants one after another.")
    p.add_argument("--runtime", default="fc")
    p.add_argument("--boot-mode", choices=["cold", "base-snapshot"], default="cold")
    p.add_argument("--user", default="", help="Sandbox owner label passed to create.")
    p.add_argument("--guest-user", default="", help="Guest user for exec; default is the guest default.")
    p.add_argument("--sandboxes", type=int, default=8, help="Sandboxes per variant (profiles split evenly).")
    p.add_argument("--cycles", type=int, default=3, help="Workload/suspend/restore cycles per sandbox.")
    p.add_argument("--concurrency", type=int, default=4, help="Sandboxes run in parallel per variant.")
    p.add_argument("--profiles", default=",".join(PROFILES))
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--pattern-mem-mb", type=int, default=256, help="Guest tmpfs correctness pattern size.")
    p.add_argument("--pattern-file-mb", type=int, default=64, help="Guest rootfs correctness pattern size.")
    p.add_argument("--anon-check-mb", type=int, default=256, help="Anonymous buffer held by a guest python3 process; 0 disables.")
    p.add_argument("--build-units", type=int, default=200)
    p.add_argument("--build-blob-mb", type=int, default=512)
    p.add_argument("--alloc-min-mb", type=int, default=2048)
    p.add_argument("--alloc-max-mb", type=int, default=4096)
    p.add_argument("--file-heavy-mb", type=int, default=1024)
    p.add_argument("--idle-secs", type=int, default=30)
    p.add_argument("--settle-secs", type=int, default=5, help="Pause after the workload before suspend.")
    p.add_argument("--no-compressed-size", dest="compressed_size", action="store_false",
                   help="Skip zstd -3 sizing of the snapshot memory file.")
    p.add_argument("--ready-timeout-secs", default="")
    p.add_argument("--op-timeout-secs", type=float, default=600)
    p.add_argument("--guest-timeout-secs", type=float, default=900)
    p.add_argument("--sbxmgr", default="/space/bin/space-sbxmgr")
    p.add_argument("--tmplmgr", default="/space/bin/space-tmplmgr")
    p.add_argument("--sandbox-cache-dir", default=DEFAULT_SANDBOX_CACHE_DIR)
    p.add_argument("--sandbox-run-dir", default=DEFAULT_SANDBOX_RUN_DIR)
    p.add_argument("--jail-dir", default=DEFAULT_JAIL_DIR)
    p.add_argument("--output-dir", default="")
    p.add_argument("--sudo", dest="use_sudo", action="store_const", const="yes", default="auto")
    p.add_argument("--no-sudo", dest="use_sudo", action="store_const", const="no")
    p.add_argument("--no-cleanup", action="store_true")
    return p


def remote_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[str]:
    """Rebuild the non-default options for the staged remote run."""
    out: list[str] = []
    for action in parser._actions:
        dest = action.dest
        if dest in LOCAL_ONLY or dest in ("help", "use_sudo") or not action.option_strings:
            continue
        value = getattr(args, dest)
        if value == action.default:
            continue
        flag = action.option_strings[0]
        if isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
            out.append(flag)
        elif isinstance(action, argparse._AppendAction):
            for item in value:
                out += [flag, str(item)]
        else:
            out += [flag, str(value)]
    if args.use_sudo != "auto":
        out.append("--sudo" if args.use_sudo == "yes" else "--no-sudo")
    return out


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.target and not args.remote_run:
        if not args.output_dir:
            args.output_dir = f"/tmp/space-sbx-balloon-{timestamp_for_path()}"
        target = normalize_target(args.target, args.ssh_user)
        ssh_options = effective_ssh_options(args.ssh_option)
        rc = stage_and_run_remote_python(
            script_path=Path(__file__).resolve(), target=target, ssh_options=ssh_options, remote_args=remote_args(args, parser)
        )
        if args.pull_results:
            pull_results(
                target=target, ssh_options=ssh_options, remote_output_dir=args.output_dir, local_parent=args.pull_results,
                files=["results.jsonl", "run.json", "tracked-sandboxes.txt"], pull_raw=False,
            )
        return rc
    BalloonBenchmark(args).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
