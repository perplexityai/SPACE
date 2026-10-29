#!/usr/bin/env python3
"""Compare synthetic publish exports, explicitly normalizing run-local values."""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime, timezone
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
    "node_template_inventory", "node_template_components",
    "node_runtime_installations", "node_runtime_guests", "node_runtime_state_formats",
    "node_artifact_components", "sandbox_snapshot_inventory", "node_facet_conditions",
    "node_reconcile_queue", "node_reconcile_triggers", "node_local_backup_pending",
)
ACTIVITY = {"last_access_at", "last_command_at", "last_used_at"}
# Normalize only named database columns. Opaque text and JSON can themselves
# contain timestamp-shaped strings or strings equal to generated identifiers.
SERVER_TIMESTAMPS = {
    "nodes": {"created_at", "drain_started_at"},
    "sandboxes": {
        "created_at", "updated_at", "lifecycle_changed_at", "placement_changed_at",
        "restore_lease_expires_at", "next_restore_attempt_at", "next_auto_event_at",
    },
    # observed_created_at is opaque node-supplied text, not a database clock.
    "sandbox_observed_states": {"observed_at", "updated_at"},
    "node_facet_observed_status": {"observed_at", "updated_at"},
    "sandbox_quarantine_state": {
        "entered_at", "updated_at", "next_action_after", "claim_expires_at",
    },
    "sandbox_quarantine_history": {"occurred_at", "next_action_after"},
    "node_status_snapshots": {
        "observed_at", "updated_at", "environment_refs_received_at",
        "execution_inventory_received_at", "execution_policy_received_at",
        "local_backup_inventory_reported_at", "sandbox_inventory_pruned_at",
    },
    "sandbox_k8s_executions": {
        "created_at", "updated_at", "terminated_at", "pod_materialized_at",
        "activation_attempt_expires_at",
    },
    "sandbox_direct_route_identities": {"updated_at"},
    "node_template_inventory": {"observed_at", "updated_at"},
    "node_template_components": set(),
    "node_runtime_installations": {"observed_at"},
    "node_runtime_guests": set(),
    "node_runtime_state_formats": set(),
    "node_artifact_components": {"observed_at"},
    # Snapshot timestamp and source_node_id are opaque node-supplied strings.
    "sandbox_snapshot_inventory": {"observed_at", "updated_at"},
    "node_facet_conditions": {"last_transition_at", "updated_at"},
    "node_reconcile_queue": {
        "claimed_at", "next_attempt_at", "requested_at", "updated_at", "dead_at",
    },
    "node_reconcile_triggers": {
        "claimed_at", "next_attempt_at", "requested_at", "updated_at", "dead_at",
    },
    "node_local_backup_pending": {"created_at"},
}
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


def episode_aliases(
    captures: list[dict[str, list[dict[str, Any]]]], seed: int
) -> dict[str, str]:
    """Alias only server-created episodes, retaining identity across captures."""
    aliases: dict[str, str] = {}
    owners: dict[str, str] = {}
    counts: dict[str, int] = defaultdict(int)
    for tables in captures:
        history = tables["sandbox_quarantine_history"]
        for row in sorted(history, key=lambda row: (row["sandbox_id"], row["occurred_at"], row["id"])):
            if (
                row["source"] != "node_state_publish"
                or row["action"] != "entered_quarantine"
                or row["reason_code"] != "node_published_quarantined"
            ):
                continue
            episode = row["quarantine_id"]
            identifier = UUID(episode)
            seeded = identifier.int >> 96 == seed and (identifier.int >> 64) & 0xffffffff == 0x7000
            if seeded or identifier.version != 7:
                continue
            sandbox = row["sandbox_id"]
            if episode in aliases:
                if owners[episode] != sandbox:
                    raise ValueError("a generated quarantine episode has multiple sandbox owners")
                continue
            aliases[episode] = f"episode:{sandbox}:{counts[sandbox]}"
            owners[episode] = sandbox
            counts[sandbox] += 1
    return aliases


def canonicalize_tables(
    tables: dict[str, list[dict[str, Any]]], seed: int, activity_epoch_seconds: int,
    episodes: dict[str, str],
) -> dict[str, Any]:
    if set(tables) != set(TABLES):
        raise ValueError("publish-owned table set differs")
    epoch = activity_epoch_seconds * 10**9
    generated: dict[str, str] = {}
    for row in tables["sandbox_k8s_executions"]:
        generated[row["id"]] = f"execution:{row['sandbox_id']}:{row['generation']}"
    for row in tables["sandbox_quarantine_history"]:
        identifier = UUID(row["id"]).int
        seeded = identifier >> 96 == seed and (identifier >> 64) & 0xffffffff == 0x7000
        if not seeded:
            # Receipt IDs are deterministic fences, never surrogate IDs.
            key = [row[name] for name in (
                "sandbox_id", "quarantine_id", "source", "action", "reason_code", "attempt_count"
            )]
            key[1] = episodes.get(key[1], key[1])
            generated[row["id"]] = "history:" + json.dumps(key)

    times: dict[str, set[int]] = defaultdict(set)

    for table, rows in tables.items():
        for row in rows:
            for column in SERVER_TIMESTAMPS[table]:
                instant = timestamp(row.get(column))
                if instant is not None:
                    times[node_scope(table, row)].add(instant)
    ranks = {scope: {instant: rank for rank, instant in enumerate(sorted(values))}
             for scope, values in times.items()}

    def convert(table: str, row: dict[str, Any]) -> dict[str, Any]:
        scope = node_scope(table, row)
        result = dict(row)
        for column, value in row.items():
            if table == "node_status_snapshots" and column == "stale_after":
                deadline = timestamp(value)
                observed = timestamp(row.get("observed_at"))
                if deadline is None or observed is None:
                    raise ValueError("status deadline must have an observed_at timestamp")
                result[column] = {"offset_from_observed_ns": deadline - observed}
            elif table == "sandboxes" and column in ACTIVITY:
                instant = timestamp(value)
                if instant is not None:
                    result[column] = {"activity_offset_ns": instant - epoch}
            elif column in SERVER_TIMESTAMPS[table]:
                instant = timestamp(value)
                if instant is not None:
                    result[column] = {"server_time_order": ranks[scope][instant]}
            elif column == "id" and table in {
                "sandbox_k8s_executions", "sandbox_quarantine_history",
            }:
                result[column] = generated.get(value, value)
            elif column == "quarantine_id" and table in {
                "sandbox_quarantine_state", "sandbox_quarantine_history",
            }:
                result[column] = episodes.get(value, value)
        return result

    result: dict[str, Any] = {}
    for table, rows in tables.items():
        result[table] = sorted(
            (convert(table, row) for row in rows),
            key=lambda row: json.dumps(row, sort_keys=True),
        )
    return result


def canonicalize(directory: Path) -> dict[str, Any]:
    report = json.loads((directory / "report.json").read_text())
    tables = {table: json.loads((directory / f"{table}.json").read_text()) for table in TABLES}
    result = canonicalize_tables(
        tables, report["seed"], report["activity_epoch_seconds"],
        episode_aliases([tables], report["seed"]),
    )
    result["workload"] = {key: report[key] for key in (
        "seed", "nodes", "sandboxes_per_node", "simulated_minutes", "time_scale", "publishes"
    )}
    return result


def canonicalize_scenarios(path: Path) -> dict[str, Any]:
    report = json.loads(path.read_text())
    if report["schema"] != 1 or not report["scenarios"]:
        raise ValueError("unsupported or empty scenario report")
    episodes = episode_aliases(
        [scenario["rows"] for scenario in report["scenarios"]], report["seed"]
    )
    scenarios: dict[str, Any] = {}
    for scenario in report["scenarios"]:
        name = scenario["name"]
        if name in scenarios:
            raise ValueError(f"duplicate scenario: {name}")
        scenarios[name] = canonicalize_tables(
            scenario["rows"], report["seed"], report["activity_epoch_seconds"], episodes
        )
    return {"seed": report["seed"], "scenarios": scenarios}


def differences(
    base: Any, head: Any, path: str = "$",
) -> Iterator[tuple[str, Any, Any]]:
    """Walk JSON structurally instead of aligning repetitive full-table lines."""
    if type(base) is not type(head):
        yield path, base, head
    elif isinstance(base, dict):
        for key in sorted(base.keys() | head.keys()):
            child = f"{path}[{json.dumps(key)}]"
            if key not in base:
                yield child, "<missing>", head[key]
            elif key not in head:
                yield child, base[key], "<missing>"
            else:
                yield from differences(base[key], head[key], child)
    elif isinstance(base, list):
        if len(base) != len(head):
            yield f"{path}.length", len(base), len(head)
        for index, (before, after) in enumerate(zip(base, head)):
            yield from differences(before, after, f"{path}[{index}]")
    elif base != head:
        yield path, base, head


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base", type=Path)
    parser.add_argument("head", type=Path)
    parser.add_argument("--scenarios", action="store_true", help="compare statements.json row captures")
    parser.add_argument("--output", type=Path, help="optional new directory for canonical evidence")
    args = parser.parse_args()
    canonicalizer = canonicalize_scenarios if args.scenarios else canonicalize
    base, head = canonicalizer(args.base), canonicalizer(args.head)
    if args.output:
        args.output.mkdir(parents=True, exist_ok=False)
        for name, value in (("base", base), ("head", head)):
            (args.output / f"{name}.json").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    changes = differences(base, head)
    first = next(changes, None)
    if first is None:
        print("Equivalent synthetic rows after explicit run-local normalization.")
        return 0
    change = first
    for _ in range(20):
        path, before, after = change
        print(f"Difference at {path}", file=sys.stderr)
        print(f"  base: {json.dumps(before, sort_keys=True)[:500]}", file=sys.stderr)
        print(f"  head: {json.dumps(after, sort_keys=True)[:500]}", file=sys.stderr)
        change = next(changes, None)
        if change is None:
            break
    print("Use --output for complete canonical evidence; diagnostics are bounded.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
