#!/usr/bin/env python3
"""Summarize bench-sbx-balloon.py results and evaluate the Stage 1 gate.

Reads one or more ``results.jsonl`` files, prints per-variant medians and p95
for each collected metric, and decides for every non-baseline variant whether
its snapshot memory is small enough to advance:

* its median gate metric is at least ``--min-reduction-pct`` (default 10%)
  *and* ``--min-reduction-mib`` (default 64 MiB) below the baseline median,
* it has zero correctness failures, and
* it has no unverified cycles (a failed suspend, restore, or verify counts
  against the variant because a corrupted guest can fail that way).

A baseline with correctness failures or unverified cycles invalidates the
comparison, so every variant fails in that case. A measured cycle without the
gate metric also fails its variant, so missing sizes never pass silently.

API mode can lose the snapshot files before suspend returns. Those rows carry
sizes sampled during suspend under ``<metric>_sampled`` instead of the measured
field. The gate ignores them unless ``--use-sampled-sizes`` is set, and even
then only uses rows whose sampled memory size stayed at its maximum for at
least ``--min-sample-stable-ms``; the report counts the rows it used.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

MIB = 1024 * 1024

# (field, unit) printed in the per-variant table. Units drive formatting only.
METRICS: tuple[tuple[str, str], ...] = (
    ("snapshot_mem_bytes", "bytes"),
    ("snapshot_mem_disk_bytes", "bytes"),
    ("snapshot_mem_bytes_sampled", "bytes"),
    ("snapshot_mem_disk_bytes_sampled", "bytes"),
    ("snapshot_mem_compressed_bytes", "bytes"),
    ("rootfs_snapshot_disk_bytes", "bytes"),
    ("rootfs_exclusive_bytes", "bytes"),
    ("suspend_ms", "ms"),
    ("restore_ms", "ms"),
    ("workload_ms", "ms"),
    ("fc_rss_before_suspend_kib", "kib"),
    ("fc_rss_after_restore_kib", "kib"),
    ("host_disk_write_bytes_suspend", "bytes"),
    ("host_disk_write_bytes_restore", "bytes"),
)
DEFAULT_GATE_METRIC = "snapshot_mem_disk_bytes"
DEFAULT_MIN_SAMPLE_STABLE_MS = 500


def load_rows(paths: Iterable[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in paths:
        for number, line in enumerate(path.read_text().splitlines(), start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"ERROR: {path}:{number}: invalid JSON: {exc}") from exc
    return rows


def percentile(values: Sequence[float], pct: float) -> float | None:
    """Nearest-rank percentile, matching sbx_bench_common.percentile."""
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(math.ceil(len(ordered) * pct / 100) - 1, len(ordered) - 1))
    return ordered[idx]


def metric_values(rows: Iterable[dict[str, Any]], field: str) -> list[float]:
    return [row[field] for row in rows if isinstance(row.get(field), (int, float)) and not isinstance(row.get(field), bool)]


def group_by_variant(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get("variant")), []).append(row)
    return groups


def is_cycle(row: dict[str, Any]) -> bool:
    """Rows with cycle >= 1 are measured cycles; cycle 0 records setup failures."""
    return isinstance(row.get("cycle"), int) and row["cycle"] >= 1


@dataclasses.dataclass
class Correctness:
    cycles: int
    verified: int
    failures: int
    unverified: int
    setup_errors: int


def correctness(rows: Sequence[dict[str, Any]]) -> Correctness:
    cycles = [r for r in rows if is_cycle(r)]
    return Correctness(
        cycles=len(cycles),
        verified=sum(1 for r in cycles if r.get("correctness_ok") is True),
        failures=sum(1 for r in cycles if r.get("correctness_ok") is False),
        unverified=sum(1 for r in cycles if r.get("correctness_ok") is None),
        setup_errors=sum(1 for r in rows if not is_cycle(r)),
    )


@dataclasses.dataclass
class GateResult:
    variant: str
    passed: bool
    baseline_median: float | None
    variant_median: float | None
    reduction_bytes: float | None
    reduction_pct: float | None
    reasons: list[str]
    sampled_cycles: int = 0
    missing_cycles: int = 0
    baseline_sampled_cycles: int = 0


def gate_values(
    rows: Iterable[dict[str, Any]], metric: str, *, min_sample_stable_ms: float | None
) -> tuple[list[float], int, int]:
    """Gate values for measured cycles, and how many came from sampling or were missing.

    ``min_sample_stable_ms`` of ``None`` disables the sampled fallback.
    """
    values: list[float] = []
    sampled = missing = 0
    for row in rows:
        if not is_cycle(row):
            continue
        measured = metric_values([row], metric)
        if measured:
            values.extend(measured)
            continue
        stable = row.get("snapshot_sample_stable_ms")
        fallback = metric_values([row], f"{metric}_sampled")
        if min_sample_stable_ms is not None and fallback and isinstance(stable, (int, float)) and stable >= min_sample_stable_ms:
            values.extend(fallback)
            sampled += 1
        else:
            missing += 1
    return values, sampled, missing


def evaluate_gate(
    rows: Sequence[dict[str, Any]],
    *,
    baseline: str,
    metric: str = DEFAULT_GATE_METRIC,
    min_reduction_pct: float = 10.0,
    min_reduction_bytes: float = 64 * MIB,
    min_sample_stable_ms: float | None = None,
) -> list[GateResult]:
    groups = group_by_variant(rows)
    if baseline not in groups:
        raise SystemExit(f"ERROR: baseline variant {baseline!r} not found; have {sorted(groups)}")
    base_rows = groups[baseline]
    base_values, base_sampled, base_missing = gate_values(base_rows, metric, min_sample_stable_ms=min_sample_stable_ms)
    base_median = statistics.median(base_values) if base_values else None
    base_ok = correctness(base_rows)

    results = []
    for variant in sorted(v for v in groups if v != baseline):
        v_rows = groups[variant]
        values, sampled, missing = gate_values(v_rows, metric, min_sample_stable_ms=min_sample_stable_ms)
        v_median = statistics.median(values) if values else None
        check = correctness(v_rows)
        reasons: list[str] = []
        if base_ok.failures or base_ok.unverified:
            reasons.append(
                f"baseline invalid: {base_ok.failures} correctness failures, {base_ok.unverified} unverified cycles"
            )
        if check.failures:
            reasons.append(f"{check.failures} correctness failures")
        if check.unverified:
            reasons.append(f"{check.unverified} unverified cycles")
        if check.cycles == 0:
            reasons.append("no measured cycles")
        if base_missing:
            reasons.append(f"baseline invalid: {base_missing} cycles missing {metric}")
        if missing:
            reasons.append(f"{missing} cycles missing {metric}")
        reduction = reduction_pct = None
        if base_median is None or v_median is None:
            reasons.append(f"missing {metric} for {'baseline' if base_median is None else 'variant'}")
        else:
            reduction = base_median - v_median
            reduction_pct = (reduction * 100.0 / base_median) if base_median else None
            if reduction_pct is None or reduction_pct < min_reduction_pct:
                reasons.append(f"reduction {_pct(reduction_pct)} < {min_reduction_pct:g}%")
            if reduction < min_reduction_bytes:
                reasons.append(f"reduction {reduction / MIB:.1f} MiB < {min_reduction_bytes / MIB:g} MiB")
        results.append(
            GateResult(
                variant=variant,
                passed=not reasons,
                baseline_median=base_median,
                variant_median=v_median,
                reduction_bytes=reduction,
                reduction_pct=reduction_pct,
                reasons=reasons,
                sampled_cycles=sampled,
                missing_cycles=missing,
                baseline_sampled_cycles=base_sampled,
            )
        )
    return results


def summarize(rows: Sequence[dict[str, Any]], *, by_profile: bool = False) -> list[dict[str, Any]]:
    """One summary record per (variant[, profile], metric) with count/median/p95."""
    out = []
    keyed: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if not is_cycle(row):
            continue
        profile = str(row.get("profile")) if by_profile else "*"
        keyed.setdefault((str(row.get("variant")), profile), []).append(row)
    for (variant, profile), group in sorted(keyed.items()):
        for field, unit in METRICS:
            values = metric_values(group, field)
            out.append(
                {
                    "variant": variant,
                    "profile": profile,
                    "metric": field,
                    "unit": unit,
                    "count": len(values),
                    "median": statistics.median(values) if values else None,
                    "p95": percentile(values, 95),
                }
            )
    return out


def _pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f}%"


def fmt(value: float | None, unit: str) -> str:
    if value is None:
        return "-"
    if unit == "bytes":
        return f"{value / MIB:.1f}MiB"
    if unit == "kib":
        return f"{value / 1024:.1f}MiB"
    return f"{value:.0f}ms"


def print_report(rows: Sequence[dict[str, Any]], gate: Sequence[GateResult], *, by_profile: bool, metric: str) -> None:
    groups = group_by_variant(rows)
    print("=== Correctness ===")
    print(f"  {'variant':16} {'cycles':>6} {'ok':>6} {'fail':>6} {'unverif':>7} {'setup_err':>9}")
    for variant in sorted(groups):
        c = correctness(groups[variant])
        print(f"  {variant:16} {c.cycles:>6} {c.verified:>6} {c.failures:>6} {c.unverified:>7} {c.setup_errors:>9}")

    print("\n=== Metrics (median / p95) ===")
    print(f"  {'variant':16} {'profile':10} {'metric':32} {'n':>4} {'median':>12} {'p95':>12}")
    for item in summarize(rows, by_profile=by_profile):
        if item["count"] == 0:
            continue
        print(
            f"  {item['variant']:16} {item['profile']:10} {item['metric']:32} {item['count']:>4}"
            f" {fmt(item['median'], item['unit']):>12} {fmt(item['p95'], item['unit']):>12}"
        )

    print(f"\n=== Stage 1 gate on median {metric} ===")
    for result in gate:
        verdict = "PASS" if result.passed else "FAIL"
        print(
            f"  {result.variant:16} {verdict}  baseline={fmt(result.baseline_median, 'bytes')}"
            f" variant={fmt(result.variant_median, 'bytes')}"
            f" reduction={fmt(result.reduction_bytes, 'bytes')} ({_pct(result.reduction_pct)})"
            + (f"  [{'; '.join(result.reasons)}]" if result.reasons else "")
        )
        if result.sampled_cycles or result.baseline_sampled_cycles:
            print(
                f"  {'':16}       uses sizes sampled during suspend: {result.sampled_cycles} variant cycles,"
                f" {result.baseline_sampled_cycles} baseline cycles"
            )


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Summarize bench-sbx-balloon.py JSONL results.")
    p.add_argument("results", nargs="+", type=Path, help="results.jsonl files")
    p.add_argument("--baseline", default="baseline", help="Variant label used as the gate baseline.")
    p.add_argument("--gate-metric", default=DEFAULT_GATE_METRIC, choices=[m for m, u in METRICS if u == "bytes" and not m.endswith("_sampled")])
    p.add_argument("--min-reduction-pct", type=float, default=10.0)
    p.add_argument("--min-reduction-mib", type=float, default=64.0)
    p.add_argument(
        "--use-sampled-sizes",
        action="store_true",
        help="Let the gate use <metric>_sampled on rows whose snapshot files were gone after suspend (API mode).",
    )
    p.add_argument(
        "--min-sample-stable-ms",
        type=float,
        default=DEFAULT_MIN_SAMPLE_STABLE_MS,
        help="With --use-sampled-sizes, how long the sampled memory size must have held at its maximum.",
    )
    p.add_argument("--by-profile", action="store_true", help="Break metrics down by workload profile.")
    p.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of tables.")
    p.add_argument("--fail-on-gate", action="store_true", help="Exit 1 unless every variant passes.")
    args = p.parse_args(argv)

    rows = load_rows(args.results)
    gate = evaluate_gate(
        rows,
        baseline=args.baseline,
        metric=args.gate_metric,
        min_reduction_pct=args.min_reduction_pct,
        min_reduction_bytes=args.min_reduction_mib * MIB,
        min_sample_stable_ms=args.min_sample_stable_ms if args.use_sampled_sizes else None,
    )
    if args.json:
        print(
            json.dumps(
                {
                    "correctness": {v: dataclasses.asdict(correctness(r)) for v, r in sorted(group_by_variant(rows).items())},
                    "metrics": summarize(rows, by_profile=args.by_profile),
                    "gate": [dataclasses.asdict(g) for g in gate],
                    "gate_metric": args.gate_metric,
                },
                indent=2,
            )
        )
    else:
        print_report(rows, gate, by_profile=args.by_profile, metric=args.gate_metric)
    if args.fail_on_gate and not all(g.passed for g in gate):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
