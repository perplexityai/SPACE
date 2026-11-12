#!/usr/bin/env python3
"""Create-to-usable latency benchmark through the public SPACE API.

Measures how long a sandbox takes to run its first guest command, for a cold
create and for a warm wake after suspend. Runs are sequential so each sample
is one sandbox on an otherwise idle cluster. Every sandbox is deleted after its
run, and the summary reports whether any are still present.

The benchmark only talks to the API, so it can run from any host that reaches
the API and data-plane URLs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from sbx_bench_common import ApiError, SpaceApi, stats, timestamp_for_path

SCHEMA_VERSION = 1
IDEMPOTENCY_PREFIX = "bench-create-to-usable"
METRICS = ("cold_op_ms", "cold_usable_ms", "warm_op_ms", "warm_usable_ms")


def elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def wait_usable(api: SpaceApi, sandbox_id: str, deadline: float, poll_secs: float) -> None:
    """Return once `true` exits 0 in the guest; retry API errors until the deadline."""
    last = ""
    while True:
        try:
            code, out = api.run(sandbox_id, ["true"], 30)
            if code == 0:
                return
            last = f"exit {code}: {out[:200]}"
        except ApiError as error:
            last = str(error)
        if time.monotonic() >= deadline:
            raise ApiError(f"sandbox {sandbox_id} not usable: {last}")
        time.sleep(poll_secs)


def run_one(api: SpaceApi, args: argparse.Namespace, index: int) -> dict[str, Any]:
    row: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "run": index, "ok": False}
    resources = {"vcpu": args.vcpu, "memory_mib": args.memory_mib}
    sandbox_id = None
    try:
        start = time.monotonic()
        sandbox_id, operation_id = api.create(args.template, resources)
        row["sandbox_id"] = sandbox_id
        api.wait_operation(operation_id, args.timeout)
        row["cold_op_ms"] = elapsed_ms(start)
        wait_usable(api, sandbox_id, start + args.timeout, args.poll_secs)
        row["cold_usable_ms"] = elapsed_ms(start)
        if not args.no_warm:
            api.lifecycle(sandbox_id, "suspend", args.timeout)
            api.wait_state(sandbox_id, "suspended", args.timeout)
            start = time.monotonic()
            api.lifecycle(sandbox_id, "wake", args.timeout)
            row["warm_op_ms"] = elapsed_ms(start)
            wait_usable(api, sandbox_id, start + args.timeout, args.poll_secs)
            row["warm_usable_ms"] = elapsed_ms(start)
        row["ok"] = True
    except ApiError as error:
        row["error"] = str(error)[:300]
    finally:
        if sandbox_id:
            try:
                api.delete(sandbox_id, args.timeout)
                row["deleted"] = True
            except ApiError as error:
                row["deleted"] = False
                row["delete_error"] = str(error)[:300]
    return row


def leftovers(api: SpaceApi, rows: list[dict[str, Any]]) -> list[str]:
    """Sandbox IDs from this run that the API still reports as present."""
    present = []
    for row in rows:
        sandbox_id = row.get("sandbox_id")
        if sandbox_id and api.sandbox_state(sandbox_id) not in (None, "deleted"):
            present.append(sandbox_id)
    return present


def summarize(rows: list[dict[str, Any]], remaining: list[str]) -> dict[str, Any]:
    ok = [row for row in rows if row["ok"]]
    summary: dict[str, Any] = {
        metric: stats(row[metric] for row in ok if metric in row) for metric in METRICS
    }
    summary["runs"] = len(rows)
    summary["succeeded"] = len(ok)
    summary["deleted"] = sum(1 for row in rows if row.get("deleted"))
    summary["leftover_sandboxes"] = remaining
    return summary


def print_table(summary: dict[str, Any]) -> None:
    print(f"{'metric':<16} {'n':>3} {'p50':>7} {'p95':>7} {'max':>7}")
    for metric in METRICS:
        s = summary[metric]
        if s["count"]:
            print(f"{metric:<16} {s['count']:>3} {s['p50']:>7} {s['p95']:>7} {s['max']:>7}")
    print(
        f"succeeded {summary['succeeded']}/{summary['runs']}, deleted {summary['deleted']}/{summary['runs']}, "
        f"leftover {len(summary['leftover_sandboxes'])}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api-url", default=os.environ.get("SPACE_API_URL", "http://127.0.0.1:8080"))
    parser.add_argument("--data-plane-url", default=os.environ.get("SPACE_DATA_PLANE_URL", "http://127.0.0.1:8090"))
    parser.add_argument("--token-file", type=Path, help="file holding the API token (default: $SPACE_API_TOKEN)")
    parser.add_argument("--template", default="seed")
    parser.add_argument("--vcpu", type=int, default=2)
    parser.add_argument("--memory-mib", type=int, default=1024)
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--no-warm", action="store_true", help="measure cold create only")
    parser.add_argument("--timeout", type=float, default=300, help="per-operation timeout in seconds")
    parser.add_argument("--poll-secs", type=float, default=0.05, help="interval between usability probes")
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    token = args.token_file.read_text().strip() if args.token_file else os.environ.get("SPACE_API_TOKEN", "")
    if not token:
        raise SystemExit("ERROR: pass --token-file or set SPACE_API_TOKEN")
    api = SpaceApi(args.api_url, args.data_plane_url, token, idempotency_prefix=IDEMPOTENCY_PREFIX)
    output_dir = args.output_dir or Path(f"/tmp/space-sbx-create-to-usable-{timestamp_for_path()}")
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    with (output_dir / "results.jsonl").open("w") as results:
        for index in range(args.runs):
            row = run_one(api, args, index)
            rows.append(row)
            results.write(json.dumps(row) + "\n")
            results.flush()
            print(json.dumps(row), file=sys.stderr, flush=True)

    summary = summarize(rows, leftovers(api, rows))
    run = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items() if k != "token_file"}
    (output_dir / "summary.json").write_text(json.dumps({"args": run, "summary": summary}, indent=2) + "\n")
    print_table(summary)
    print(f"results: {output_dir}")
    clean = summary["succeeded"] == summary["deleted"] == len(rows) and not summary["leftover_sandboxes"]
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
