"""Exercise the shipped guest workload on real files, without a deployment."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

from space_smoke.scenarios.native_retained_volume_benchmark import BENCHMARK


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="requires Linux direct I/O")
def test_guest_workload_reports_real_io_and_cleans_only_its_own_files(tmp_path: Path) -> None:
    namespace: dict[str, Any] = {"__name__": "guest_benchmark_test"}
    exec(compile(BENCHMARK, "<guest-benchmark>", "exec"), namespace)
    marker = tmp_path / "keep"
    marker.write_bytes(b"existing caller data")
    result = namespace["measure"](
        str(tmp_path), size=2 * 1024**2, seconds=0.02, limit=4, transactions=3,
    )

    assert result["sequential_write"]["mib_per_second"] > 0
    for metric in ("random_read", "random_overwrite_fdatasync"):
        observation = result[metric]
        assert 1 <= observation["latency"]["samples"] <= 4
        assert observation["iops"] > 0
        assert observation["latency"]["p99_ms"] >= observation["latency"]["p50_ms"] > 0
    for metric in ("append_fdatasync", "small_file_create_fsync"):
        assert result[metric]["samples"] == 3
        assert result[metric]["p99_ms"] >= result[metric]["p50_ms"] > 0
    assert list(tmp_path.iterdir()) == [marker]
    assert marker.read_bytes() == b"existing caller data"
