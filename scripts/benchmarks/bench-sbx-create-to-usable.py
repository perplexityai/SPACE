#!/usr/bin/env python3
"""Create-to-usable latency benchmark through the public SPACE API.

Measures how long a sandbox takes to run its first guest command, for a cold
create and for a warm wake after suspend. Runs are sequential so each sample
is one sandbox on an otherwise idle cluster. A create can adopt a pre-booted
warm-pool slot, so cold samples measure a cold boot only when the template has
no warm pool for the requested shape. Every sandbox is deleted after its
run, and the summary reports whether any are still present.

The benchmark only talks to the API, so it can run from any host that reaches
both the management API (create, lifecycle, delete) and the data plane (the
command event stream used to probe usability).
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from sbx_bench_common import ApiError, SpaceApi, stats

SCHEMA_VERSION = 1
IDEMPOTENCY_PREFIX = "bench-create-to-usable"
METRICS = ("cold_op_ms", "cold_usable_ms", "warm_op_ms", "warm_usable_ms")


def elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def wait_usable(api: SpaceApi, sandbox_id: str, deadline: float, poll_secs: float) -> None:
    """Return once `true` exits 0 in the guest; retry API errors until the deadline."""
    last = "deadline passed before the first probe"
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            code, out = api.run(sandbox_id, ["true"], min(30, remaining))
            if code == 0 and time.monotonic() <= deadline:
                return
            last = "probe finished after the deadline" if code == 0 else f"exit {code}: {out[:200]}"
        except ApiError as error:
            last = str(error)
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_secs)
    raise ApiError(f"sandbox {sandbox_id} not usable: {last}")


def run_one(api: SpaceApi, args: argparse.Namespace, row: dict[str, Any]) -> None:
    """Fill in row in place, so an interrupted run still records its sandbox."""
    resources = {"vcpu": args.vcpu, "memory_mib": args.memory_mib}
    sandbox_id = operation_id = None
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
                # Waits for the create to end and confirms the API no longer finds the sandbox.
                api.delete_after_create(sandbox_id, operation_id, args.timeout)
                row["deleted"] = True
            except ApiError as error:
                row["deleted"] = False
                row["delete_error"] = str(error)[:300]


def leftovers(api: SpaceApi, rows: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    """Sandbox IDs from this run the API still reports, and those it could not check."""
    present: list[str] = []
    unchecked: list[str] = []
    for row in rows:
        sandbox_id = row.get("sandbox_id")
        if not sandbox_id:
            continue
        try:
            if api.sandbox_state(sandbox_id) not in (None, "deleted"):
                present.append(sandbox_id)
        except ApiError as error:
            print(f"cannot check sandbox {sandbox_id}: {error}", file=sys.stderr)
            unchecked.append(sandbox_id)
    return present, unchecked


def summarize(rows: list[dict[str, Any]], remaining: list[str], unchecked: list[str]) -> dict[str, Any]:
    ok = [row for row in rows if row["ok"]]
    summary: dict[str, Any] = {
        metric: stats(row[metric] for row in ok if metric in row) for metric in METRICS
    }
    summary["runs"] = len(rows)
    summary["succeeded"] = len(ok)
    summary["deleted"] = sum(1 for row in rows if row.get("deleted"))
    summary["leftover_sandboxes"] = remaining
    # Sandboxes whose presence is unknown because the final check failed.
    summary["unchecked_sandboxes"] = unchecked
    return summary


def print_table(summary: dict[str, Any]) -> None:
    print(f"{'metric':<16} {'n':>3} {'p50':>7} {'p95':>7} {'max':>7}")
    for metric in METRICS:
        s = summary[metric]
        if s["count"]:
            print(f"{metric:<16} {s['count']:>3} {s['p50']:>7} {s['p95']:>7} {s['max']:>7}")
    print(
        f"succeeded {summary['succeeded']}/{summary['runs']}, deleted {summary['deleted']}/{summary['runs']}, "
        f"leftover {len(summary['leftover_sandboxes'])}, unchecked {len(summary['unchecked_sandboxes'])}"
    )


def interrupt(signum: int, frame: object) -> None:
    """Unwind SIGTERM (e.g. a CI timeout) like Ctrl-C so cleanup and the summary still run."""
    raise KeyboardInterrupt


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
    # Operation polls share --poll-secs so op completion adds at most one interval to each sample.
    try:
        api = SpaceApi(
            args.api_url, args.data_plane_url, token, poll_secs=args.poll_secs, idempotency_prefix=IDEMPOTENCY_PREFIX
        )
    except ValueError as error:
        raise SystemExit(f"ERROR: {error}") from error
    if args.output_dir:
        output_dir = args.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = Path(tempfile.mkdtemp(prefix="space-sbx-create-to-usable-"))

    signal.signal(signal.SIGTERM, interrupt)
    rows: list[dict[str, Any]] = []
    interrupted = False
    with (output_dir / "results.jsonl").open("w") as results:
        for index in range(args.runs):
            row: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "run": index, "ok": False}
            rows.append(row)
            try:
                run_one(api, args, row)
            except KeyboardInterrupt:
                interrupted = True
                row["error"] = "interrupted"
            finally:
                results.write(json.dumps(row) + "\n")
                results.flush()
                print(json.dumps(row), file=sys.stderr, flush=True)
            if interrupted:
                print("interrupted; checking for leftovers", file=sys.stderr)
                break
            if row.get("deleted") is False:
                # Another create could pile up live sandboxes and skew later samples.
                print("delete not confirmed; stopping before the next sample", file=sys.stderr)
                break

    summary = summarize(rows, *leftovers(api, rows))
    summary["interrupted"] = interrupted
    run = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items() if k != "token_file"}
    (output_dir / "summary.json").write_text(json.dumps({"args": run, "summary": summary}, indent=2) + "\n")
    print_table(summary)
    print(f"results: {output_dir}")
    clean = (
        summary["succeeded"] == summary["deleted"] == len(rows)
        and not summary["leftover_sandboxes"]
        and not summary["unchecked_sandboxes"]
    )
    return 130 if interrupted else 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
