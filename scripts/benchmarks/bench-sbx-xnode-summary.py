#!/usr/bin/env python3
"""Summarize bench-sbx-xnode.py results into the Stage 2 decision.

Only wake-ups whose restore landed off the source worker and downloaded its
snapshot (``classification == cross_node_download``) count toward reconnect
time. The first ``warmup_ms`` of each block is dropped. Failures (timeouts and
errors) are reported as a rate and folded into a failure-inclusive p95, scored
at the wake timeout, so faster successes cannot hide them.

Each variant is judged against the baseline on its own (Metrics Spec,
"Decision rules"), for loaded runs:

- stop_now: any correctness failure in the variant.
- regression_stop: reconnect p99 or bystander command p95 more than 5% worse,
  with the bootstrap interval for the difference excluding zero.
- inconclusive: fewer than the minimum samples or blocks, or the point
  estimate meets the bar but the interval includes zero.
- proceed_pending_cause: p95 improves by at least max(10%, 1 s), the interval
  excludes zero, and every variant block beats its nearest baseline block.
  The spec also requires one cause metric (bytes, codec wait or hold,
  writeback wait, disk pressure) to move the same way; those come from
  Datadog, so this script cannot confirm it.
- stop: anything else.

Quiet runs are report-only. Intervals are bootstrap percentile intervals for
``variant - baseline``; the default level is 98.3% because three variants
share one baseline.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

MIN_SAMPLES = {"loaded": 2000, "quiet": 400}
MIN_BLOCKS = {"loaded": 4, "quiet": 1}
NOT_ELIGIBLE = {"same_node", "outside_pool", "not_snapshot", "cross_node_local_copy", "cross_node_no_remote"}


def percentile(sorted_values: Sequence[float], q: float) -> float | None:
    """Linear-interpolated percentile of already sorted values (numpy's default)."""
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    pos = (len(sorted_values) - 1) * q / 100.0
    lo = math.floor(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    return float(sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo))


def stats(values: Iterable[float]) -> dict[str, Any]:
    data = sorted(values)
    return {"n": len(data), "p50": percentile(data, 50), "p95": percentile(data, 95),
            "p99": percentile(data, 99), "mean": (sum(data) / len(data)) if data else None}


def bootstrap_diff(variant: Sequence[float], baseline: Sequence[float], qs: Sequence[float], *, resamples: int,
                   level: float, rng: random.Random, cap: int | None = None) -> dict[float, tuple[float, float] | None]:
    """Percentile-bootstrap interval of ``pXX(variant) - pXX(baseline)`` for each q."""
    if not variant or not baseline or resamples <= 0:
        return {q: None for q in qs}
    v = list(variant) if cap is None or len(variant) <= cap else rng.sample(list(variant), cap)
    b = list(baseline) if cap is None or len(baseline) <= cap else rng.sample(list(baseline), cap)
    diffs: dict[float, list[float]] = {q: [] for q in qs}
    for _ in range(resamples):
        vs = sorted(rng.choices(v, k=len(v)))
        bs = sorted(rng.choices(b, k=len(b)))
        for q in qs:
            diffs[q].append(percentile(vs, q) - percentile(bs, q))  # type: ignore[operator]
    alpha = (1 - level) / 2
    out: dict[float, tuple[float, float] | None] = {}
    for q, values in diffs.items():
        values.sort()
        out[q] = (percentile(values, alpha * 100), percentile(values, (1 - alpha) * 100))  # type: ignore[assignment]
    return out


def load_rows(paths: Sequence[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in paths:
        for line in path.read_text().splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def collect(rows: Sequence[dict[str, Any]], *, wake_timeout_ms: float) -> dict[str, Any]:
    """Group measured samples by (load, variant)."""
    blocks = {(r["run_id"], r["block"]): r for r in rows if r.get("kind") == "block"}
    placement = {(r["run_id"], r["wake_id"]): r for r in rows if r.get("kind") == "wake_placement"}
    groups: dict[tuple[str, str], dict[str, Any]] = defaultdict(lambda: {
        "latency": [], "failures": 0, "successes": 0, "correctness_failures": 0, "classification": defaultdict(int),
        "block_latency": defaultdict(list), "probe_rtt": [], "probe_failures": 0, "excluded_warmup": 0,
        "verify_ms": [], "skipped_arrivals": 0})

    def window(row: dict[str, Any], t: int | None) -> tuple[dict[str, Any] | None, bool]:
        block = blocks.get((row["run_id"], row["block"]))
        if block is None or t is None:
            return block, False
        return block, block["start_ms"] + block.get("warmup_ms", 0) <= t <= block["end_ms"]

    for row in rows:
        kind = row.get("kind")
        if kind == "wake":
            block, measured = window(row, row.get("wake_start_ms"))
            if block is None:
                continue
            g = groups[(block["load"], row["variant"])]
            if not measured:
                g["excluded_warmup"] += 1
                continue
            label = (placement.get((row["run_id"], row["wake_id"])) or {}).get("classification", "unclassified")
            g["classification"][label] += 1
            if row["outcome"] == "ok":
                if row.get("correctness_ok") is False:
                    g["correctness_failures"] += 1
                if label == "cross_node_download":
                    g["successes"] += 1
                    g["latency"].append(row["reconnect_ms"])
                    g["block_latency"][(row["run_id"], row["block"])].append(row["reconnect_ms"])
                    if row.get("verify_ms") is not None:
                        g["verify_ms"].append(row["verify_ms"])
            elif label not in NOT_ELIGIBLE:
                g["failures"] += 1
        elif kind == "probe":
            block, measured = window(row, row.get("t_ms"))
            if block is None or not measured:
                continue
            g = groups[(block["load"], row["variant"])]
            if row.get("ok"):
                g["probe_rtt"].append(row["rtt_ms"])
            else:
                g["probe_failures"] += 1
        elif kind == "arrival_skipped":
            block, measured = window(row, row.get("t_ms"))
            if block is not None and measured:
                groups[(block["load"], row["variant"])]["skipped_arrivals"] += 1
    for (load, label), g in groups.items():
        g["failure_inclusive"] = g["latency"] + [wake_timeout_ms] * g["failures"]
        g["blocks"] = sorted(g["block_latency"], key=lambda key: blocks[key]["start_ms"])
    return {"groups": groups, "blocks": blocks}


def nearest_baseline_block(block_key: Any, baseline_keys: Sequence[Any], blocks: dict[Any, Any]) -> Any | None:
    if not baseline_keys:
        return None
    start = blocks[block_key]["start_ms"]
    return min(baseline_keys, key=lambda key: abs(blocks[key]["start_ms"] - start))


def decide(load: str, variant: dict[str, Any], baseline: dict[str, Any], *, bar_pct: float, bar_ms: float,
           regression_pct: float, intervals: dict[str, Any], blocks_beat: bool) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if load != "loaded":
        return "report_only", ["quiet runs are report-only"]
    if baseline["correctness_failures"]:
        return "invalid", ["baseline has correctness failures"]
    if variant["correctness_failures"]:
        return "stop_now", [f"{variant['correctness_failures']} correctness failures"]
    v, b = stats(variant["latency"]), stats(baseline["latency"])
    vp, bp = stats(variant["probe_rtt"]), stats(baseline["probe_rtt"])
    ci99, ci_probe = intervals.get("p99"), intervals.get("probe_p95")
    if v["p99"] is not None and b["p99"] and v["p99"] > b["p99"] * (1 + regression_pct) and ci99 and ci99[0] > 0:
        return "regression_stop", [f"reconnect p99 {v['p99']:.0f} ms vs {b['p99']:.0f} ms"]
    if (vp["p95"] is not None and bp["p95"] and vp["p95"] > bp["p95"] * (1 + regression_pct)
            and ci_probe and ci_probe[0] > 0):
        return "regression_stop", [f"bystander p95 {vp['p95']:.0f} ms vs {bp['p95']:.0f} ms"]
    short = []
    for name, group in (("variant", variant), ("baseline", baseline)):
        if len(group["latency"]) < MIN_SAMPLES[load]:
            short.append(f"{name} has {len(group['latency'])} < {MIN_SAMPLES[load]} samples")
        if len(group["blocks"]) < MIN_BLOCKS[load]:
            short.append(f"{name} has {len(group['blocks'])} < {MIN_BLOCKS[load]} blocks")
    if short:
        return "inconclusive", short
    bar = max(b["p95"] * bar_pct, bar_ms)
    improvement = b["p95"] - v["p95"]
    ci95 = intervals.get("p95")
    if improvement < bar:
        return "stop", [f"p95 improved {improvement:.0f} ms; the bar is {bar:.0f} ms"]
    if not ci95 or ci95[1] >= 0:
        return "inconclusive", ["p95 meets the bar but the interval includes zero"]
    if not blocks_beat:
        return "stop", ["not every variant block beats its nearest baseline block"]
    reasons.append("confirm one cause metric moved the same way (Datadog) before calling it a pass")
    return "proceed_pending_cause", reasons


def summarize(rows: Sequence[dict[str, Any]], *, baseline: str, level: float, resamples: int, probe_resamples: int,
              probe_cap: int, seed: int, bar_pct: float, bar_ms: float, regression_pct: float,
              wake_timeout_ms: float) -> dict[str, Any]:
    collected = collect(rows, wake_timeout_ms=wake_timeout_ms)
    groups, blocks = collected["groups"], collected["blocks"]
    rng = random.Random(seed)
    out: dict[str, Any] = {"baseline": baseline, "ci_level": level, "loads": {}}
    for load in sorted({load for load, _ in groups}):
        labels = sorted({label for l, label in groups if l == load}, key=lambda x: (x != baseline, x))
        base = groups.get((load, baseline))
        section: dict[str, Any] = {"variants": {}}
        for label in labels:
            g = groups[(load, label)]
            entry: dict[str, Any] = {
                "reconnect_ms": stats(g["latency"]),
                "failure_inclusive_p95_ms": stats(g["failure_inclusive"])["p95"],
                "failures": g["failures"],
                "failure_rate": (g["failures"] / (g["failures"] + g["successes"])) if (g["failures"] + g["successes"]) else None,
                "correctness_failures": g["correctness_failures"],
                "classification": dict(g["classification"]),
                "excluded_warmup": g["excluded_warmup"],
                "skipped_arrivals": g["skipped_arrivals"],
                "blocks": len(g["blocks"]),
                "block_p95_ms": [stats(g["block_latency"][key])["p95"] for key in g["blocks"]],
                "bystander_rtt_ms": stats(g["probe_rtt"]),
                "bystander_failures": g["probe_failures"],
                "post_restore_verify_ms": stats(g["verify_ms"]),
            }
            if base is not None and label != baseline:
                ci = bootstrap_diff(g["latency"], base["latency"], (95, 99), resamples=resamples, level=level, rng=rng)
                ci_probe = bootstrap_diff(g["probe_rtt"], base["probe_rtt"], (95,), resamples=probe_resamples,
                                          level=level, rng=rng, cap=probe_cap)
                intervals = {"p95": ci[95], "p99": ci[99], "probe_p95": ci_probe[95]}
                beats = []
                for key in g["blocks"]:
                    near = nearest_baseline_block(key, base["blocks"], blocks)
                    mine = stats(g["block_latency"][key])["p95"]
                    theirs = stats(base["block_latency"][near])["p95"] if near is not None else None
                    beats.append(mine is not None and theirs is not None and mine < theirs)
                decision, reasons = decide(load, g, base, bar_pct=bar_pct, bar_ms=bar_ms,
                                           regression_pct=regression_pct, intervals=intervals,
                                           blocks_beat=bool(beats) and all(beats))
                v95, b95 = entry["reconnect_ms"]["p95"], stats(base["latency"])["p95"]
                entry["vs_baseline"] = {
                    "p95_diff_ms": (v95 - b95) if v95 is not None and b95 is not None else None,
                    "p95_change_pct": ((v95 - b95) / b95 * 100) if v95 is not None and b95 else None,
                    "p95_diff_ci_ms": intervals["p95"], "p99_diff_ci_ms": intervals["p99"],
                    "bystander_p95_diff_ci_ms": intervals["probe_p95"], "blocks_beat_nearest_baseline": beats,
                    "decision": decision, "reasons": reasons,
                }
            section["variants"][label] = entry
        out["loads"][load] = section
    return out


def fmt(value: Any, unit: str = "") -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.0f}{unit}" if abs(value) >= 10 else f"{value:.2f}{unit}"
    return f"{value}{unit}"


def render(summary: dict[str, Any]) -> str:
    lines = []
    for load, section in summary["loads"].items():
        lines.append(f"== {load} (baseline: {summary['baseline']}, {summary['ci_level'] * 100:.1f}% intervals) ==")
        lines.append(f"{'variant':<14}{'n':>6}{'p50':>8}{'p95':>8}{'p99':>8}{'fail%':>7}{'p95+fail':>10}"
                     f"{'bystander p95':>15}{'blocks':>8}  decision")
        for label, e in section["variants"].items():
            r = e["reconnect_ms"]
            rate = e["failure_rate"] * 100 if e["failure_rate"] is not None else None
            decision = e.get("vs_baseline", {}).get("decision", "(baseline)")
            lines.append(f"{label:<14}{r['n']:>6}{fmt(r['p50']):>8}{fmt(r['p95']):>8}{fmt(r['p99']):>8}"
                         f"{fmt(rate):>7}{fmt(e['failure_inclusive_p95_ms']):>10}"
                         f"{fmt(e['bystander_rtt_ms']['p95']):>15}{e['blocks']:>8}  {decision}")
        for label, e in section["variants"].items():
            vs = e.get("vs_baseline")
            details = [f"  {label}: placement {e['classification']}"]
            if e["correctness_failures"]:
                details.append(f"    correctness failures: {e['correctness_failures']}")
            if e["skipped_arrivals"]:
                details.append(f"    arrivals skipped (empty reservoir): {e['skipped_arrivals']}")
            if vs:
                ci = vs["p95_diff_ci_ms"]
                details.append(f"    p95 change {fmt(vs['p95_change_pct'], '%')} ({fmt(vs['p95_diff_ms'], ' ms')}), "
                               f"interval {fmt(ci[0]) if ci else '-'}..{fmt(ci[1]) if ci else '-'} ms; "
                               f"blocks beating nearest baseline: {sum(vs['blocks_beat_nearest_baseline'])}"
                               f"/{len(vs['blocks_beat_nearest_baseline'])}")
                for reason in vs["reasons"]:
                    details.append(f"    - {reason}")
            lines += details
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("results", nargs="+", type=Path)
    p.add_argument("--baseline", default="baseline")
    p.add_argument("--ci", type=float, default=0.983, help="Interval level (default 98.3%% for three variants).")
    p.add_argument("--resamples", type=int, default=10_000)
    p.add_argument("--probe-resamples", type=int, default=1_000)
    p.add_argument("--probe-cap", type=int, default=20_000, help="Bystander samples per side used for bootstrapping.")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--bar-pct", type=float, default=10.0, help="Required p95 improvement, percent of baseline.")
    p.add_argument("--bar-ms", type=float, default=1000.0, help="Required p95 improvement, absolute floor.")
    p.add_argument("--regression-pct", type=float, default=5.0)
    p.add_argument("--wake-timeout-ms", type=float, default=120_000, help="Score given to failed wake-ups.")
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)
    summary = summarize(load_rows(args.results), baseline=args.baseline, level=args.ci, resamples=args.resamples,
                        probe_resamples=args.probe_resamples, probe_cap=args.probe_cap, seed=args.seed,
                        bar_pct=args.bar_pct / 100, bar_ms=args.bar_ms, regression_pct=args.regression_pct / 100,
                        wake_timeout_ms=args.wake_timeout_ms)
    sys.stdout.write(json.dumps(summary, indent=2, default=str) + "\n" if args.json else render(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
