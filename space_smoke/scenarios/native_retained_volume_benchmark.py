"""Paired local-rootfs/data-disk file I/O, not a fleet saturation benchmark."""

from __future__ import annotations

import json

from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.scenarios.native_retained_ebs_suspend import _run


BENCHMARK = r"""
import json, math, mmap, os, random, statistics, tempfile, time
from pathlib import Path

MIB = 1024 * 1024
BLOCK = 4096

def distribution(samples):
    ordered = sorted(samples)
    assert ordered, "benchmark produced no observations"
    return {
        "samples": len(ordered),
        "p50_ms": ordered[math.ceil(len(ordered) * .50) - 1] * 1000,
        "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1] * 1000,
        "p99_ms": ordered[math.ceil(len(ordered) * .99) - 1] * 1000,
    }

def random_io(fd, buffer, size, write, seconds, limit):
    randomizer = random.Random(0)
    samples = []
    started = time.monotonic()
    while len(samples) < limit and time.monotonic() - started < seconds:
        offset = randomizer.randrange(size // BLOCK) * BLOCK
        before = time.monotonic()
        count = os.pwrite(fd, buffer, offset) if write else os.preadv(fd, [buffer], offset)
        assert count == BLOCK, ("short random I/O", count)
        if write:
            os.fdatasync(fd)
        samples.append(time.monotonic() - before)
    elapsed = time.monotonic() - started
    return {
        "iops": len(samples) / elapsed,
        "elapsed_seconds": elapsed,
        "latency": distribution(samples),
    }

def measure(base, size=48*MIB, seconds=5.0, limit=20000, transactions=128):
    assert size > 0 and size % MIB == 0
    assert seconds > 0 and limit > 0 and transactions > 0
    capacity = os.statvfs(base)
    assert capacity.f_bavail * capacity.f_frsize >= size + 8*MIB, "insufficient free space"
    results = {}
    with tempfile.TemporaryDirectory(prefix=".space-io-bench-", dir=base) as directory:
        # No raw-device writes. Private files and O_EXCL preserve existing data.
        fd = os.open(directory + "/direct",
                     os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_DIRECT, 0o600)
        try:
            with mmap.mmap(-1, MIB) as buffer:
                buffer[:] = os.urandom(MIB)
                started = time.monotonic()
                for offset in range(0, size, MIB):
                    assert os.pwrite(fd, buffer, offset) == MIB, "short sequential write"
                os.fsync(fd)
                elapsed = time.monotonic() - started
                results["sequential_write"] = {
                    "mib_per_second": size / MIB / elapsed,
                    "elapsed_seconds": elapsed,
                }
            with mmap.mmap(-1, BLOCK) as buffer:
                results["random_read"] = random_io(fd, buffer, size, False, seconds, limit)
                buffer[:] = os.urandom(BLOCK)
                results["random_overwrite_fdatasync"] = random_io(
                    fd, buffer, size, True, seconds, limit,
                )
        finally:
            os.close(fd)

        payload = os.urandom(BLOCK)
        fd = os.open(directory + "/append", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            samples = []
            for index in range(transactions):
                started = time.monotonic()
                assert os.write(fd, payload) == BLOCK, "short append"
                os.fdatasync(fd)
                samples.append(time.monotonic() - started)
            results["append_fdatasync"] = distribution(samples)
        finally:
            os.close(fd)

        samples = []
        directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            for index in range(transactions):
                started = time.monotonic()
                fd = os.open(directory + "/small-" + str(index),
                             os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    assert os.write(fd, payload) == BLOCK, "short small-file write"
                    os.fsync(fd)
                finally:
                    os.close(fd)
                # Include directory durability, not just the newly created inode.
                os.fsync(directory_fd)
                samples.append(time.monotonic() - started)
        finally:
            os.close(directory_fd)
        results["small_file_create_fsync"] = distribution(samples)
    return results

def main():
    entries = [line.split() for line in Path("/proc/self/mountinfo").read_text().splitlines()]
    data, = [entry for entry in entries if entry[4] == "/data"]
    assert data[data.index("-") + 1:data.index("-") + 3] == ["ext4", "/dev/vdb"], data
    assert os.stat("/data").st_dev != os.stat("/").st_dev, "required data mount missing"
    assert os.stat("/var/tmp").st_dev == os.stat("/").st_dev, "baseline is not the rootfs"
    targets = {"local_rootfs": "/var/tmp", "retained_data": "/data"}
    results = {
        "method": "paired_guest_file_io_v2",
        "working_set_bytes": 48*MIB,
        "random_block_bytes": BLOCK,
        "random_seconds_limit": 5.0,
        "random_samples_limit": 20000,
        "transactions_per_phase": 128,
        "small_file_directory_fsync": True,
        "usable_filesystem_bytes": os.statvfs("/data").f_blocks * os.statvfs("/data").f_frsize,
        "kernel": os.uname().release,
        "mounts": {
            name: max(
                (entry for entry in entries
                 if path == entry[4] or path.startswith(entry[4].rstrip("/") + "/")),
                key=lambda entry: len(entry[4]),
            )
            for name, path in targets.items()
        },
        "runs": [],
    }
    # Alternate which arm runs first; both arms share the same guest and worker.
    for pair in range(4):
        order = tuple(targets) if pair % 2 == 0 else tuple(reversed(targets))
        for name in order:
            results["runs"].append({
                "pair": pair, "target": name, "results": measure(targets[name]),
            })
    summaries = {}
    for name in targets:
        runs = [run["results"] for run in results["runs"] if run["target"] == name]
        summaries[name] = {
            "sequential_write_mib_per_second": statistics.median(
                run["sequential_write"]["mib_per_second"] for run in runs
            ),
        }
        for metric in ("random_read", "random_overwrite_fdatasync",
                       "append_fdatasync", "small_file_create_fsync"):
            observations = [
                run[metric]["latency"] if "latency" in run[metric] else run[metric]
                for run in runs
            ]
            summaries[name][metric] = {
                percentile: statistics.median(item[percentile] for item in observations)
                for percentile in ("p50_ms", "p95_ms", "p99_ms")
            }
    results["median_summary"] = summaries
    print(json.dumps(results))

if __name__ == "__main__":
    main()
"""


@scenario(
    "native.retained_volume_benchmark",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    tier="manual",
    manual_reason="requires an admitted organization and an approved single-disk I/O budget",
    timeout_minutes=20,
)
def retained_volume_benchmark(test: NativeHardeningFixture) -> None:
    """Compare four interleaved pairs using one sandbox and one data disk.

    The baseline is labeled local_rootfs, not NVMe: operator evidence must
    independently establish its backing device. Filesystem/CoW differences
    remain part of this end-to-end comparison, not isolated media performance.
    """
    with test.context.phase("create the single benchmark sandbox"):
        sandbox = test.create_ready({
            "volume_mounts": [{
                "type": "ebs", "size_gib": 128,
                "permissions": "read_write", "mount": "/data",
            }],
        }, timeout=600)
    with test.context.phase("paired rootfs and retained-disk file I/O observations"):
        observed = json.loads(_run(test, sandbox, BENCHMARK, timeout_seconds=600))
        test.context.record("retained_volume_io_benchmark", observed)
        test.context.verify(
            observed["usable_filesystem_bytes"] > 120 * 1024**3,
            "benchmark filesystem has unexpected capacity",
        )
