#!/usr/bin/env python3
"""Compare synthetic publish exports, explicitly normalizing run-local values."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import difflib
import json
from pathlib import Path
import re
import sys
from typing import Any
from uuid import UUID


TABLES = (
    "nodes", "sandboxes", "sandbox_observed_states", "node_facet_observed_status",
    "sandbox_quarantine_state", "sandbox_quarantine_history",
    "node_status_snapshots", "sandbox_k8s_executions",
    "sandbox_direct_route_identities",
)
ACTIVITY = {"last_access_at", "last_command_at", "last_used_at"}
TIMESTAMP = re.compile(
    r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)$"
)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def timestamp(value: Any) -> int | None:
    match = TIMESTAMP.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        return None
    seconds, fraction, zone = match.groups()
    whole = datetime.fromisoformat(seconds + zone.replace("Z", "+00:00"))
    return int((whole - EPOCH).total_seconds()) * 10**9 + int((fraction or "").ljust(9, "0"))


def node_scope(table: str, row: dict[str, Any]) -> str:
    if table == "nodes":
        return row["id"]
    if row.get("node_id") is not None:
        return row["node_id"]
    sandbox = row["id"] if table == "sandboxes" else row["sandbox_id"]
    # The driver reserves the low 24 bits for sandbox IDs within a node.
    return str(UUID(int=UUID(sandbox).int & ~((1 << 24) - 1)))


def canonicalize(directory: Path) -> dict[str, Any]:
    report = json.loads((directory / "report.json").read_text())
    epoch = report["activity_epoch_seconds"] * 10**9
    tables = {table: json.loads((directory / f"{table}.json").read_text()) for table in TABLES}
    generated: dict[str, str] = {}
    for row in tables["sandbox_k8s_executions"]:
        generated[row["id"]] = f"execution:{row['sandbox_id']}:{row['generation']}"
    for row in tables["sandbox_quarantine_history"]:
        identifier = UUID(row["id"]).int
        seeded = identifier >> 96 == report["seed"] and (identifier >> 64) & 0xffffffff == 0x7000
        if not seeded:
            # Receipt IDs are deterministic fences, never surrogate IDs.
            key = [row[name] for name in (
                "sandbox_id", "quarantine_id", "source", "action", "reason_code", "attempt_count"
            )]
            generated[row["id"]] = "history:" + json.dumps(key)

    times: dict[str, set[int]] = defaultdict(set)

    def collect(value: Any, scope: str, key: str = "") -> None:
        if isinstance(value, dict):
            for name, child in value.items():
                collect(child, scope, name)
        elif isinstance(value, list):
            for child in value:
                collect(child, scope, key)
        elif key not in ACTIVITY and key != "stale_after":
            instant = timestamp(value)
            if instant is not None:
                times[scope].add(instant)

    for table, rows in tables.items():
        for row in rows:
            collect(row, node_scope(table, row))
    ranks = {scope: {instant: rank for rank, instant in enumerate(sorted(values))}
             for scope, values in times.items()}

    def convert(value: Any, scope: str, key: str = "") -> Any:
        if isinstance(value, dict):
            result = {name: convert(child, scope, name) for name, child in value.items()
                      if name != "stale_after"}
            if "stale_after" in value:
                deadline = timestamp(value["stale_after"])
                observed = timestamp(value.get("observed_at"))
                if deadline is None or observed is None:
                    raise ValueError("status deadline must have an observed_at timestamp")
                result["stale_after"] = {"offset_from_observed_ns": deadline - observed}
            return result
        if isinstance(value, list):
            return [convert(child, scope, key) for child in value]
        instant = timestamp(value)
        if instant is not None:
            if key in ACTIVITY:
                # Exact synthetic node-clock offsets, not rounded age buckets.
                return {"activity_offset_ns": instant - epoch}
            return {"server_time_order": ranks[scope][instant]}
        if isinstance(value, str):
            return generated.get(value, value)
        return value

    result: dict[str, Any] = {
        "workload": {key: report[key] for key in (
            "seed", "nodes", "sandboxes_per_node", "simulated_minutes", "time_scale", "publishes"
        )}
    }
    for table, rows in tables.items():
        result[table] = sorted(
            (convert(row, node_scope(table, row)) for row in rows),
            key=lambda row: json.dumps(row, sort_keys=True),
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", type=Path)
    parser.add_argument("head", type=Path)
    parser.add_argument("--output", type=Path, help="optional new directory for canonical evidence")
    args = parser.parse_args()
    base, head = canonicalize(args.base), canonicalize(args.head)
    if args.output:
        args.output.mkdir(parents=True, exist_ok=False)
        for name, value in (("base", base), ("head", head)):
            (args.output / f"{name}.json").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    if base == head:
        print("Equivalent synthetic rows after explicit run-local normalization.")
        return 0
    changes = list(difflib.unified_diff(
        json.dumps(base, indent=2, sort_keys=True).splitlines(),
        json.dumps(head, indent=2, sort_keys=True).splitlines(),
        fromfile="base", tofile="head",
    ))
    print("\n".join(changes[:200]), file=sys.stderr)
    if len(changes) > 200:
        print("Diff truncated; use --output for complete canonical evidence.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
