#!/usr/bin/env python3
"""Check the local workload ledger against actual stored Prometheus samples.

Run before six-hour retention expires. Report exact observed loss separately
from passing ingestion checks; increase()/rate() are not accounting ledgers.
"""
import argparse
import datetime
import json
import math
from pathlib import Path
import re

from capture import query, save

COUNTER = "space_node_mgr_loop_devices_created_total"
GAUGE = "space_node_mgr_cleanup_operation_in_flight"
TIMING = "space_sandbox_state_convergence_duration_milliseconds"
STATE = "space_sandbox_guest_vcpu_count"
CREATED = "space_sandbox_api_created_total"


def timestamp(record):
    return datetime.datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00")).timestamp()


def delivered(values):
    previous, total, resets = 0.0, 0.0, 0
    for _, raw in values:
        value = float(raw)
        if value < previous:
            resets += 1
            total += value
        else:
            total += value - previous
        previous = value
    return {"delivered": total, "resets": resets, "final_epoch": previous}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    path = parser.parse_args().evidence
    run = json.loads((path / "run.json").read_text())
    start = run["start"]
    match = '{service_version=' + json.dumps(run["version"]) + '}'
    series = json.loads((path / "space-series.json").read_text())["data"]["result"]
    checks, queries, accounting = [], {}, []

    def check(name, passed, observed):
        checks.append({"check": name, "status": "passed" if passed else "failed", "observed": observed})

    def q(name, expression, elapsed):
        result = query(expression, time=start + elapsed)
        queries[name] = {"expression": expression, "elapsed": elapsed, "time": start + elapsed, "response": result}
        if result["status"] != "success":
            raise RuntimeError(result)
        return [float(item["value"][1]) for item in result["data"]["result"]]

    ledger = {}
    for line in (path / "workload.log").read_text().splitlines():
        if "oss workload tick" in line:
            pod = re.search(r"\[pod/([^/]+)/", line).group(1)
            ledger.setdefault(pod, []).append(json.loads(line[line.index("{"):]))
    check("Both producers complete all 144 ticks", len(ledger) == 2 and all([r["tick"] for r in rows] == list(range(144)) for rows in ledger.values()), {p: {"ticks": len(r), "increments": r[-1]["increments"], "active_ticks": r[-1]["active_ticks"]} for p, r in ledger.items()})
    gaps = {pod: max(timestamp(b) - timestamp(a) for a, b in zip(rows, rows[1:])) for pod, rows in ledger.items()}
    check("Emission cadence continues during receiver outages", len(gaps) == 2 and all(gap < 5.5 for gap in gaps.values()), gaps)
    pods = json.loads((path / "final-pods.json").read_text())["items"]
    producers = [p for p in pods if p["metadata"]["name"] in ledger]
    check("Workload processes exit successfully", len(producers) == 2 and all(p["status"]["phase"] == "Succeeded" for p in producers), {p["metadata"]["name"]: p["status"]["phase"] for p in producers})

    for elapsed, expected, name in [(40, 7, "positive"), (100, 0, "zero"), (560, None, "silent/expired"), (710, 11, "eventually recovered")]:
        observed = q("gauge " + name, GAUGE + match, elapsed)
        check("Gauge " + name, observed == ([] if expected is None else [expected, expected]), observed)

    counters = [item for item in series if item["metric"]["__name__"] == COUNTER]
    for item in counters:
        labels = item["metric"]
        actual = delivered(item["values"])
        expected = 396 * (2 if labels["kind"] == "device_node" else 1)
        accounting.append({"producer": labels["instance"], "kind": labels["kind"], "emitted": expected, **actual, "lost": expected - actual["delivered"]})
    check("Counter accounting bounded by known emissions (loss reported)", len(accounting) == 4 and all(0 < row["delivered"] <= row["emitted"] for row in accounting), accounting)
    healthy = q("healthy kernel rate", 'rate(' + COUNTER + match[:-1] + ',kind="kernel_device"}[1m])', 110)
    check("Healthy counter rate: 2 increments / 5 seconds", len(healthy) == 2 and all(math.isclose(v, 0.4, abs_tol=0.025) for v in healthy), healthy)
    for item in counters:
        if item["metric"]["kind"] != "kernel_device":
            continue
        rows = ledger[item["metric"]["k8s_pod_name"]]
        for left, right, label in [(35, 115, "healthy"), (335, 435, "across Prometheus outage")]:
            eligible = [(t, float(v)) for t, v in item["values"] if start + left <= t <= start + right]
            first, last = eligible[0], eligible[-1]
            if label == "healthy":
                expected = sum(r["increment"] for r in rows if first[0] < timestamp(r) <= last[0])
                observed = last[1] - first[1]
            else:
                # Compare the full prefix, accounting for the earlier reset.
                # A ledger event 60us after the start sample rounds into the
                # same Prometheus millisecond; subtracting that boundary naively
                # would falsely count an increment already in the start value.
                expected = sum(r["increment"] for r in rows if timestamp(r) <= last[0])
                observed = delivered([(t, v) for t, v in item["values"] if t <= last[0]])["delivered"]
            check("Exact increments " + label + ": " + item["metric"]["instance"], observed == expected, {"from": first, "to": last, "expected": expected, "observed": observed})
    idle = q("idle kernel rate", 'rate(' + COUNTER + match[:-1] + ',kind="kernel_device"}[30s])', 175)
    check("Idle counter does not invent increments", all(abs(v) < 1e-9 for v in idle), idle)
    q("counter resets", 'resets(' + COUNTER + match + '[15m])', 735)
    for metric, count_per_tick, sum_per_tick, lower in [(TIMING, 4, 15, 7.82), (STATE, 3, 14, 7.91)]:
        counts = q(metric + " count", "histogram_count(" + metric + match + ")", 110)
        sums = q(metric + " sum", "histogram_sum(" + metric + match + ")", 110)
        ticks = q(metric + " ticks", CREATED + match, 110)
        check(metric + " count and sum", len(counts) == 2 and counts == [count_per_tick * t for t in ticks] and sums == [sum_per_tick * t for t in ticks], {"ticks": ticks, "counts": counts, "sums": sums})
        p95 = q(metric + " p95", "histogram_quantile(0.95," + metric + match + ")", 110)
        check(metric + " p95 within known bucket", len(p95) == 2 and all(lower <= value <= 8 for value in p95), p95)
        q(metric + " p50", "histogram_quantile(0.5," + metric + match + ")", 110)
        native = [item for item in series if item["metric"]["__name__"] == metric]
        check(metric + " stored as native histogram", len(native) == 2 and all(item.get("histograms") for item in native), {"series": len(native), "first": [item.get("histograms", [])[:1] for item in native]})
    identities = {item["metric"].get("service_instance_id") for item in series}
    required = {"cluster_id", "deployment_environment_name", "service_version", "service_name", "service_instance_id", "k8s_pod_name", "k8s_node_name", "k8s_cluster_name"}
    check("Distinct producers and identities survive", len(identities) == 2 and all(required <= item["metric"].keys() for item in series), sorted(identities))
    dimensions = {"state": "started", "template": "oss-synthetic", "tls_interception": "off", "kind": "device_node"}
    check("Product dimensions survive", all(any(i["metric"].get(k) == v for i in series) for k, v in dimensions.items()), dimensions)

    internal = json.loads((path / "collector-series.json").read_text())["data"]["result"]
    peaks = {}
    for name in ["otelcol_exporter_queue_size", "otelcol_process_memory_rss", "otelcol_deltatocumulative_streams_tracked"]:
        peaks[name] = max(float(v) for item in internal if item["metric"]["__name__"] == name for _, v in item["values"])
    direct = [p.read_text() for p in path.glob("collector-*.prom")]
    if (path / "collector-direct.jsonl").exists():
        direct.extend(json.loads(line)["metrics"] for line in (path / "collector-direct.jsonl").read_text().splitlines())
    for scrape in direct:
        for line in scrape.splitlines():
            for name in peaks:
                if re.match(re.escape(name) + r"(?:\{| )", line):
                    peaks[name] = max(peaks[name], float(line.rsplit(" ", 1)[1]))
    check("Observed Collector memory, queue and stream bounds", peaks["otelcol_exporter_queue_size"] <= 128 and peaks["otelcol_process_memory_rss"] < 512 * 1024**2 and peaks["otelcol_deltatocumulative_streams_tracked"] < 10000, peaks)
    save(path / "queries.json", queries)
    save(path / "assessment.json", {"checks": checks, "accounting": accounting, "peaks": peaks})
    for row in checks:
        print(row["status"].upper() + ": " + row["check"] + ": " + json.dumps(row["observed"]))
    raise SystemExit(1 if any(row["status"] == "failed" for row in checks) else 0)


if __name__ == "__main__":
    main()
