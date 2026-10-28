#!/usr/bin/env python3
"""Collect one discarded warmup and three measured synthetic publish runs."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("driver", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.driver[:1] == ["--"]:
        args.driver = args.driver[1:]
    if not args.driver or "--output" in args.driver:
        parser.error("provide the driver command without --output after --")
    args.output.mkdir(parents=True, exist_ok=False)
    reports = []
    for run in ("warmup", "run-1", "run-2", "run-3"):
        print(f"Starting {run}", flush=True)
        directory = args.output / run
        subprocess.run(
            [*args.driver, "--output", str(directory)],
            check=True,
        )
        report = json.loads((directory / "report.json").read_text())
        if run != "warmup":
            reports.append(report)
        print(f"Completed {run}", flush=True)
    metrics = (
        "postgres_cpu_ms_per_publish",
        "service_cpu_ms_per_publish",
        "statements_per_publish",
        "exec_ms_per_publish",
        "plan_ms_per_publish",
        "rows_per_publish",
        "shared_blks_hit_per_publish",
        "wal_bytes_per_publish",
        "rpc_p50_ms",
        "rpc_p99_ms",
        "transaction_p50_ms",
        "transaction_p99_ms",
    )
    summary = {}
    for metric in metrics:
        values = [report[metric] for report in reports]
        median = statistics.median(values)
        summary[metric] = {
            "runs": values,
            "median": median,
            "spread_percent": (max(values) - min(values)) / median * 100 if median else 0,
        }
    result = {
        "protocol": "one discarded warmup, three measured runs, fresh fixture databases",
        "workload": {
            key: reports[0][key]
            for key in ("seed", "nodes", "sandboxes_per_node", "simulated_minutes", "time_scale", "publishes")
        },
        "standard_workload": all(
            report["nodes"] == 50
            and report["sandboxes_per_node"] == 160
            and report["simulated_minutes"] == 10
            and report["time_scale"] == 1
            and report["publishes"] == 10_000
            for report in reports
        ),
        "driver_command": args.driver,
        "cpu_affinity": {
            "driver_and_service": sorted(os.sched_getaffinity(0)),
            "postgres": sorted(os.sched_getaffinity(int(os.environ["SPACE_PUBLISH_POSTGRES_PID"]))),
        },
        "metrics": summary,
        "headline_spread_below_5_percent":
            summary["postgres_cpu_ms_per_publish"]["spread_percent"] < 5,
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["headline_spread_below_5_percent"] else 1


if __name__ == "__main__":
    sys.exit(main())
