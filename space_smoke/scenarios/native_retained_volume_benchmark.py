"""Paired, bounded fio workloads on local-rootfs and retained data files."""

from __future__ import annotations

import json

from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.scenarios.native_retained_ebs_suspend import _run


BENCHMARK = r'''
import hashlib, json, os, shutil, subprocess, tempfile
from pathlib import Path

def run_fio(base, size_mib=48, random_seconds=5, random_operations=20000, transactions=128):
    assert 1 <= size_mib <= 48 and 1 <= random_seconds <= 5
    assert 1 <= random_operations <= 20000 and 1 <= transactions <= 128
    assert os.statvfs(base).f_bavail * os.statvfs(base).f_frsize >= (size_mib + 8)*1024**2
    with tempfile.TemporaryDirectory(prefix=".space-fio-", dir=base) as directory:
        # fio treats colons as directory separators; never accept a job-file injection.
        assert not any(character in directory for character in ":\n\r\\")
        jobs = Path(directory) / "workload.fio"
        jobs.write_text(f"""
[global]
ioengine=sync
thread=1
numjobs=1
iodepth=1
directory={directory}
size={size_mib}m
fallocate=none
create_on_open=1
randrepeat=1
refill_buffers=1
percentile_list=50:95:99
exitall_on_error=1

[sequential_write]
filename=data
rw=write
bs=1m
direct=1
end_fsync=1

[random_read]
stonewall=1
filename=data
allow_file_create=0
rw=randread
bs=4k
direct=1
runtime={random_seconds}
io_size={random_operations * 4096}

[random_overwrite_fdatasync]
stonewall=1
filename=data
allow_file_create=0
rw=randwrite
bs=4k
direct=0
fdatasync=1
end_fsync=1
runtime={random_seconds}
io_size={random_operations * 4096}

[append_fdatasync]
stonewall=1
filename=append
rw=write
bs=4k
direct=0
file_append=1
fdatasync=1
end_fsync=1
size={transactions * 4096}
io_size={transactions * 4096}

[small_file_create]
stonewall=1
ioengine=filecreate
rw=write
bs=4k
nrfiles={transactions}
openfiles=1
filesize=4k
size={transactions * 4096}
""")
        result = subprocess.run(
            ["fio", "--eta=never", "--output-format=json",
             "--aux-path=" + directory, str(jobs)],
            cwd=directory, text=True, capture_output=True, timeout=150,
        )
        if result.returncode:
            raise RuntimeError(f"fio failed ({result.returncode}): "
                               f"{result.stderr[-2000:]} {result.stdout[-4000:]}")
        observed = json.loads(result.stdout)
        expected = {"sequential_write", "random_read", "random_overwrite_fdatasync",
                    "append_fdatasync", "small_file_create"}
        assert len(observed["jobs"]) == len(expected)
        assert {job["jobname"] for job in observed["jobs"]} == expected
        assert all(job["error"] == 0 for job in observed["jobs"]), observed
        for job in observed["jobs"]:
            direction = "read" if job["jobname"] == "random_read" else "write"
            assert job[direction]["total_ios"] > 0, job
            if job["jobname"].endswith("_fdatasync"):
                # fio 3.33's sync.total_ios counts DDIR_SYNC, not DDIR_DATASYNC.
                # The shared sync latency sample count includes fdatasync.
                assert job["sync"]["lat_ns"]["N"] > 0, job
        return observed

def main():
    binary = shutil.which("fio")
    assert binary, "qualified guest must include fio; no custom fallback"
    version = subprocess.check_output([binary, "--version"], text=True, timeout=10).strip()
    assert version.startswith("fio-3."), ("expected the fio I/O tester", version)
    entries = [line.split() for line in Path("/proc/self/mountinfo").read_text().splitlines()]
    data, = [entry for entry in entries if entry[4] == "/data"]
    assert data[data.index("-") + 1:data.index("-") + 3] == ["ext4", "/dev/vdb"], data
    assert os.stat("/data").st_dev != os.stat("/").st_dev, "required data mount missing"
    assert os.stat("/var/tmp").st_dev == os.stat("/").st_dev, "baseline is not the rootfs"
    targets = {"local_rootfs": "/var/tmp", "retained_data": "/data"}
    results = {
        "method": "paired_guest_fio_v1",
        "fio_version": version,
        "fio_binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
        "working_set_bytes": 48*1024**2,
        "random_seconds_limit": 5,
        "random_operations_limit": 20000,
        "transactions_per_phase": 128,
        "small_file_create_includes_content_fsync": False,
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
    # Retain fio's own JSON, including separate write and sync latency statistics.
    # Neither summing their percentiles nor calling filecreate durable is valid.
    for pair in range(4):
        order = tuple(targets) if pair % 2 == 0 else tuple(reversed(targets))
        for name in order:
            results["runs"].append({
                "pair": pair, "target": name, "fio": run_fio(targets[name]),
            })
    print(json.dumps(results))

if __name__ == "__main__":
    main()
'''


@scenario(
    "native.retained_volume_benchmark",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    tier="manual",
    manual_reason="requires guest fio, an admitted organization and an approved single-disk I/O budget",
    timeout_minutes=30,
)
def retained_volume_benchmark(test: NativeHardeningFixture) -> None:
    """Compare four interleaved fio pairs using one sandbox and one data disk.

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
    with test.context.phase("paired rootfs and retained-disk fio observations"):
        observed = json.loads(_run(
            test, sandbox, BENCHMARK, timeout_seconds=1200, output_limit=2*1024**2,
        ))
        test.context.record("retained_volume_io_benchmark", observed)
        test.context.verify(
            observed["usable_filesystem_bytes"] > 120 * 1024**3,
            "benchmark filesystem has unexpected capacity",
        )
