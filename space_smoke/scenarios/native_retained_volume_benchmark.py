"""Bounded single-guest I/O observations, not a saturation or fleet benchmark."""

from __future__ import annotations

import json

from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.scenarios.native_retained_ebs_suspend import _run


BENCHMARK = r"""
import json, math, mmap, os, random, tempfile, time
from pathlib import Path

assert os.stat("/data").st_dev != os.stat("/").st_dev, "required data mount missing"
entries = [line.split() for line in Path("/proc/self/mountinfo").read_text().splitlines()]
entry, = [entry for entry in entries if entry[4] == "/data"]
assert entry[entry.index("-") + 1:entry.index("-") + 3] == ["ext4", "/dev/vdb"], entry

MIB = 1024 * 1024
SIZE = 128 * MIB
BLOCK = 4096
RANDOM_SECONDS = 5.0
MAX_SAMPLES = 20000

def distribution(samples):
    ordered = sorted(samples)
    return {
        "samples": len(ordered),
        "p50_ms": ordered[math.ceil(len(ordered) * .50) - 1] * 1000,
        "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1] * 1000,
        "p99_ms": ordered[math.ceil(len(ordered) * .99) - 1] * 1000,
    }

def random_io(fd, buffer, write):
    randomizer = random.Random(0)
    samples = []
    started = time.monotonic()
    while len(samples) < MAX_SAMPLES and time.monotonic() - started < RANDOM_SECONDS:
        offset = randomizer.randrange(SIZE // BLOCK) * BLOCK
        before = time.monotonic()
        count = os.pwrite(fd, buffer, offset) if write else os.preadv(fd, [buffer], offset)
        assert count == BLOCK, ("short random I/O", count)
        samples.append(time.monotonic() - before)
    os.fsync(fd)
    elapsed = time.monotonic() - started
    return {
        "iops": len(samples) / elapsed,
        "elapsed_seconds": elapsed,
        "latency": distribution(samples),
    }

capacity = os.statvfs("/data")
results = {
    "method": "single_guest_qd1_direct_io_v1",
    "working_set_bytes": SIZE,
    "random_block_bytes": BLOCK,
    "random_seconds_limit": RANDOM_SECONDS,
    "random_samples_limit": MAX_SAMPLES,
    "usable_filesystem_bytes": capacity.f_blocks * capacity.f_frsize,
    "kernel": os.uname().release,
    "results": {},
}
assert capacity.f_bavail * capacity.f_frsize >= SIZE + 8 * MIB, "insufficient guest free space"

with tempfile.TemporaryDirectory(prefix=".space-io-bench-", dir="/data") as directory:
    # O_EXCL and a private guest directory protect existing caller data.
    # O_DIRECT deliberately fails rather than silently measuring page-cache reads.
    fd = os.open(directory + "/direct", os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_DIRECT, 0o600)
    try:
        with mmap.mmap(-1, MIB) as buffer:
            buffer[:] = os.urandom(MIB)
            started = time.monotonic()
            for offset in range(0, SIZE, MIB):
                assert os.pwrite(fd, buffer, offset) == MIB, "short sequential write"
            os.fsync(fd)
            elapsed = time.monotonic() - started
            results["results"]["sequential_write"] = {
                "mib_per_second": SIZE / MIB / elapsed, "elapsed_seconds": elapsed,
            }
            started = time.monotonic()
            for offset in range(0, SIZE, MIB):
                assert os.preadv(fd, [buffer], offset) == MIB, "short sequential read"
            elapsed = time.monotonic() - started
            results["results"]["sequential_read"] = {
                "mib_per_second": SIZE / MIB / elapsed, "elapsed_seconds": elapsed,
            }
        with mmap.mmap(-1, BLOCK) as buffer:
            results["results"]["random_read"] = random_io(fd, buffer, False)
            buffer[:] = os.urandom(BLOCK)
            results["results"]["random_write"] = random_io(fd, buffer, True)
    finally:
        os.close(fd)

    # Buffered write + fsync is a separate small-transaction observation.
    fd = os.open(directory + "/fsync", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = os.urandom(BLOCK)
        samples = []
        for index in range(128):
            started = time.monotonic()
            assert os.pwrite(fd, payload, index * BLOCK) == BLOCK
            os.fsync(fd)
            samples.append(time.monotonic() - started)
        results["results"]["write_fsync"] = distribution(samples)
    finally:
        os.close(fd)
print(json.dumps(results))
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
    """Measure one 128-MiB guest working set and clean its files afterward.

    Direct I/O bypasses the guest page cache, not provider or host caches.
    This is a short QD1 microbenchmark, not gp3 saturation, steady-state or
    attachment-density evidence. Operator evidence must additionally record
    worker type, provider capacity, IOPS and throughput settings.
    """
    with test.context.phase("create the single benchmark sandbox"):
        sandbox = test.create_ready({
            "volume_mounts": [{
                "type": "ebs", "size_gib": 128,
                "permissions": "read_write", "mount": "/data",
            }],
        }, timeout=600)
    with test.context.phase("bounded direct I/O and fsync observations"):
        observed = json.loads(_run(test, sandbox, BENCHMARK))
        test.context.record("retained_volume_io_benchmark", observed)
        test.context.verify(
            observed["usable_filesystem_bytes"] > 120 * 1024**3,
            "benchmark filesystem has unexpected capacity",
        )
