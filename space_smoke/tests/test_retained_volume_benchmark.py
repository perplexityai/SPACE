"""Exercise the shipped guest workload on real files, without a deployment."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

from space_smoke.scenarios.native_retained_volume_benchmark import BENCHMARK


@pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("fio") is None,
    reason="requires Linux and the fio I/O tester",
)
def test_guest_workload_reports_real_io_and_cleans_only_its_own_files(tmp_path: Path) -> None:
    namespace: dict[str, Any] = {"__name__": "guest_benchmark_test"}
    exec(compile(BENCHMARK, "<guest-benchmark>", "exec"), namespace)
    marker = tmp_path / "keep"
    marker.write_bytes(b"existing caller data")
    result = namespace["run_fio"](
        str(tmp_path), size_mib=2, random_seconds=1, random_operations=4, transactions=3,
    )

    jobs = {job["jobname"]: job for job in result["jobs"]}
    assert all(job["error"] == 0 for job in jobs.values())
    assert jobs["sequential_write"]["write"]["io_bytes"] == 2 * 1024**2
    assert 1 <= jobs["random_read"]["read"]["total_ios"] <= 4
    assert 1 <= jobs["random_overwrite_fdatasync"]["write"]["total_ios"] <= 4
    assert jobs["random_overwrite_fdatasync"]["sync"]["lat_ns"]["N"] > 0
    assert jobs["append_fdatasync"]["write"]["total_ios"] == 3
    assert jobs["append_fdatasync"]["sync"]["lat_ns"]["N"] > 0
    assert jobs["small_file_create"]["write"]["total_ios"] == 3
    assert list(tmp_path.iterdir()) == [marker]
    assert marker.read_bytes() == b"existing caller data"
