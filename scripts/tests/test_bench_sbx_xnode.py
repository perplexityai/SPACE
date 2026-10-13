"""Tests for the Stage 2 cross-node balloon benchmark and its summary."""

from __future__ import annotations

import importlib.util
import json
import random
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

BENCH_DIR = Path(__file__).resolve().parents[1] / "benchmarks"
sys.path.insert(0, str(BENCH_DIR))


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, BENCH_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


xnode = _load("bench_sbx_xnode", "bench-sbx-xnode.py")
summary = _load("bench_sbx_xnode_summary", "bench-sbx-xnode-summary.py")
NODES = ["w1", "w2", "w3"]


class HelperTests(unittest.TestCase):
    def test_block_order_alternates_passes(self):
        self.assertEqual(
            xnode.block_order(["baseline", "a", "b"], "baseline", 2),
            ["baseline", "a", "b", "b", "a", "baseline"],
        )
        with self.assertRaises(ValueError):
            xnode.block_order(["a"], "baseline", 1)

    def test_parse_order_rejects_unknown_labels(self):
        self.assertEqual(xnode.parse_order("a, b,a", ["a", "b"]), ["a", "b", "a"])
        with self.assertRaises(ValueError):
            xnode.parse_order("a,c", ["a", "b"])

    def test_poisson_offsets_match_rate_and_seed(self):
        offsets = xnode.poisson_offsets(60, 3600, random.Random(7))
        self.assertEqual(offsets, xnode.poisson_offsets(60, 3600, random.Random(7)))
        self.assertTrue(all(0 <= t < 3600 for t in offsets))
        self.assertEqual(offsets, sorted(offsets))
        self.assertAlmostEqual(len(offsets), 3600, delta=250)

    def test_source_rotates_through_nodes(self):
        self.assertEqual([xnode.source_for(NODES, t, 60) for t in (0, 59, 60, 125, 180)],
                         ["w1", "w1", "w2", "w3", "w1"])

    def test_reservoir_and_expected_samples(self):
        self.assertEqual(xnode.reservoir_target(10, 180, "loaded", 10), 45)
        self.assertEqual(xnode.reservoir_target(10, 180, "quiet", 10), 27)
        order = ["baseline", "a", "a", "baseline"]
        self.assertEqual(xnode.expected_samples(order, "a", rate_per_min=20, block_s=1800, warmup_s=300,
                                                load="loaded", quiet_interval_s=10), 1000)

    def test_canonical_id_and_timestamps(self):
        raw = uuid.uuid4()
        self.assertEqual(xnode.canonical_id(raw.hex), str(raw))
        self.assertEqual(xnode.canonical_id(f"sbx-{raw}"), str(raw))
        self.assertEqual(xnode.parse_timestamp_ms("2026-10-13T00:00:00.123456789Z"), 1_791_849_600_123)
        self.assertEqual(xnode.parse_timestamp_ms(1_791_849_600), 1_791_849_600_000)
        self.assertIsNone(xnode.parse_timestamp_ms("not a time"))

    def test_parse_activation_top_level_and_nested(self):
        sid = str(uuid.uuid4())
        top = json.dumps({"timestamp": "2026-10-13T00:00:01Z", "event_type": "runtime_activated",
                          "sandbox_id": sid, "boot_type": "snapshot", "restore_provenance": "remote_only",
                          "restore_storage_uri_present": True, "execution_generation": 4})
        nested = json.dumps({"timestamp": "2026-10-13T00:00:01Z",
                             "fields": {"event_type": "runtime_activated", "sandbox_id": sid,
                                        "boot_type": "snapshot", "restore_provenance": "local"}})
        a = xnode.parse_activation(top, "w2")
        self.assertEqual((a.sandbox_id, a.node, a.provenance, a.storage_uri_present, a.generation),
                         (sid, "w2", "remote_only", True, "4"))
        self.assertEqual(xnode.parse_activation(nested, "w3").provenance, "local")
        self.assertIsNone(xnode.parse_activation('{"event_type": "sandbox_paused"}', "w1"))
        self.assertIsNone(xnode.parse_activation("runtime_activated but not json", "w1"))

    def test_classify(self):
        def act(node, boot="snapshot", prov="remote_only"):
            return xnode.Activation("s", node, boot, prov, True, "1", 0)

        self.assertEqual(xnode.classify("w1", act("w2"), NODES), "cross_node_download")
        self.assertEqual(xnode.classify("w1", act("w1"), NODES), "same_node")
        self.assertEqual(xnode.classify("w1", act("w9"), NODES), "outside_pool")
        self.assertEqual(xnode.classify("w1", act("w2", prov="remote_matches_local"), NODES),
                         "cross_node_local_copy")
        self.assertEqual(xnode.classify("w1", act("w2", prov="local"), NODES), "cross_node_no_remote")
        self.assertEqual(xnode.classify("w1", act("w2", boot="cold"), NODES), "not_snapshot")
        self.assertEqual(xnode.classify("w1", None, NODES), "unclassified")

    def test_join_takes_earliest_unused_activation_in_window(self):
        acts = [xnode.Activation("s", "w2", "snapshot", "remote_only", True, "2", 1_000),
                xnode.Activation("s", "w3", "snapshot", "remote_only", True, "3", 50_000),
                xnode.Activation("s", "w1", "snapshot", "remote_only", True, "1", -900_000)]
        wakes = [{"wake_id": "a", "sandbox_id": "s", "wake_start_ms": 500},
                 {"wake_id": "b", "sandbox_id": "s", "wake_start_ms": 40_000},
                 {"wake_id": "c", "sandbox_id": "other", "wake_start_ms": 0}]
        joined = xnode.join_activations(wakes, acts)
        self.assertEqual(joined["a"].node, "w2")
        self.assertEqual(joined["b"].node, "w3")
        self.assertIsNone(joined["c"])


class FakeKube:
    def __init__(self, nodes, cordoned=()):
        self.nodes = list(nodes)
        self.cordoned = set(cordoned)
        self.calls = []
        self.lock = threading.Lock()
        self.pods = {}  # canonical id -> Placement
        self.lines = []

    def node_unschedulable(self, node):
        return node in self.cordoned

    def cordon(self, node):
        with self.lock:
            self.calls.append(("cordon", node))
            self.cordoned.add(node)

    def uncordon(self, node):
        with self.lock:
            self.calls.append(("uncordon", node))
            self.cordoned.discard(node)

    def placements(self):
        with self.lock:
            return dict(self.pods)

    def sbxmgr_log_lines(self, nodes, since_ms):
        with self.lock:
            return list(self.lines)


class CordonGuardTests(unittest.TestCase):
    def test_refuses_nodes_outside_pool_and_records_state_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "cordon.state"
            kube = FakeKube(NODES)
            seen = []
            original = kube.cordon
            kube.cordon = lambda node: (seen.append(state.read_text()), original(node))
            guard = xnode.CordonGuard(kube, NODES, state)
            with self.assertRaises(ValueError):
                guard.cordon("other-node")
            guard.cordon("w2")
            self.assertIn("w2", seen[0])
            self.assertEqual(xnode.CordonGuard.repair(kube, state), ["w2"])
            self.assertFalse(state.exists())
            self.assertEqual(kube.cordoned, set())

    def test_only_uncordons_what_it_cordoned(self):
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube(NODES, cordoned={"w3"})
            guard = xnode.CordonGuard(kube, NODES, Path(tmp) / "cordon.state")
            guard.cordon("w1")
            guard.uncordon("w3")
            self.assertEqual(guard.release_all(), [])
            self.assertEqual(kube.cordoned, {"w3"})


class FakeEcw:
    """Simulates ECW plus where restores land, driven by FakeKube's cordons."""

    def __init__(self, kube, *, corrupt=False):
        self.kube = kube
        self.corrupt = corrupt
        self.lock = threading.Lock()
        self.rr = 0
        self.state = {}  # sandbox id -> dict

    def _place(self, sid, node):
        self.kube.pods[xnode.canonical_id(sid)] = xnode.Placement(node, xnode.now_ms())

    def create(self, template, metadata):
        sid = str(uuid.uuid4())
        with self.lock:
            node = self.kube.nodes[self.rr % len(self.kube.nodes)]
            self.rr += 1
            self.state[sid] = {"node": node, "local": {node}}
            self._place(sid, node)
        return SimpleNamespace(sandbox_id=sid)

    def run(self, handle, command, timeout):
        if command == "true":
            return xnode.CommandOutcome(0, "", "")
        if "mem_mb=$1" in command:
            return xnode.CommandOutcome(0, "mem_sha=m\nfile_sha=f\nanon_sha=a\n", "")
        if "token=$1" in command:
            return xnode.CommandOutcome(0, "mem_sha=m\nfile_sha=f\nanon_sha=" + ("x" if self.corrupt else "a"), "")
        return xnode.CommandOutcome(0, "start_ns=1000000\nend_ns=3000000\n", "")

    def pause(self, handle):
        with self.lock:
            self.kube.pods.pop(xnode.canonical_id(handle.sandbox_id), None)

    def connect(self, sid, request_timeout):
        with self.lock:
            st = self.state[sid]
            options = [n for n in self.kube.nodes if n not in self.kube.cordoned]
            node = options[self.rr % len(options)]
            self.rr += 1
            provenance = "remote_matches_local" if node in st["local"] else "remote_only"
            st["node"] = node
            st["local"].add(node)
            self._place(sid, node)
            self.kube.lines.append((node, json.dumps({
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time() * 1000) % 1000:03d}Z",
                "event_type": "runtime_activated", "sandbox_id": sid, "boot_type": "snapshot",
                "restore_provenance": provenance, "restore_storage_uri_present": True})))
        return SimpleNamespace(sandbox_id=sid)

    def kill(self, sid):
        with self.lock:
            self.kube.pods.pop(xnode.canonical_id(sid), None)


def fake_args(tmp, **overrides):
    argv = ["--nodes", ",".join(NODES), "--variant", "baseline=t-base", "--variant", "reporting=t-rep",
            "--blocks-per-variant", "1", "--block-minutes", "0.05", "--warmup-minutes", "0",
            "--rate", "240", "--rotate-seconds", "1", "--population-per-node", "2", "--reservoir-per-node", "4",
            "--bystanders-per-node", "1", "--probe-interval-s", "0.2", "--portable-wait-s", "0",
            "--placement-interval-s", "0.1", "--load-rest-s", "0.05", "--bind-timeout-s", "2",
            "--wake-timeout-s", "5", "--create-concurrency", "8", "--output-dir", tmp]
    for key, value in overrides.items():
        argv += [f"--{key.replace('_', '-')}", str(value)]
    args = xnode.build_parser().parse_args(argv)
    xnode.validate(args)
    return args


class FakeRunTests(unittest.TestCase):
    def run_fake(self, tmp, **overrides):
        kube = FakeKube(NODES)
        args = fake_args(tmp, **overrides)
        bench = xnode.XnodeBenchmark(args, kube=kube, ecw=FakeEcw(kube))
        bench.run()
        rows = [json.loads(line) for line in (Path(tmp) / "results.jsonl").read_text().splitlines()]
        return kube, rows

    def test_loaded_run_classifies_cross_node_restores_and_releases_cordons(self):
        with tempfile.TemporaryDirectory() as tmp:
            kube, rows = self.run_fake(tmp)
            self.assertEqual(kube.cordoned, set())
            self.assertFalse((Path(tmp) / "cordon.state").exists())
            self.assertTrue(all(node in NODES for _, node in kube.calls))
            wakes = [r for r in rows if r["kind"] == "wake"]
            placements = {r["wake_id"]: r for r in rows if r["kind"] == "wake_placement"}
            self.assertGreater(len(wakes), 4)
            self.assertEqual({w["variant"] for w in wakes}, {"baseline", "reporting"})
            for wake in wakes:
                self.assertEqual(wake["outcome"], "ok")
                self.assertTrue(wake["correctness_ok"])
                place = placements[wake["wake_id"]]
                self.assertNotEqual(place["landed_node"], wake["source_node"])
                self.assertIn(place["classification"], {"cross_node_download", "cross_node_local_copy"})
            self.assertTrue(any(r["kind"] == "probe" for r in rows))
            self.assertTrue(any(r["kind"] == "suspend" and r["phase"] == "paired" for r in rows))
            self.assertEqual([r["variant"] for r in rows if r["kind"] == "block"], ["baseline", "reporting"])
            result = summary.summarize(rows, baseline="baseline", level=0.9, resamples=50, probe_resamples=20,
                                       probe_cap=1000, seed=1, bar_pct=0.1, bar_ms=1000, regression_pct=0.05,
                                       wake_timeout_ms=5000)
            self.assertEqual(result["loads"]["loaded"]["variants"]["reporting"]["vs_baseline"]["decision"],
                             "inconclusive")

    def test_quiet_run_is_one_at_a_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, rows = self.run_fake(tmp, load="quiet", quiet_interval_s=0.5, bystanders_per_node=0)
            wakes = [r for r in rows if r["kind"] == "wake"]
            self.assertTrue(wakes)
            self.assertTrue(all(w["inflight_at_dispatch"] == 0 for w in wakes))
            self.assertFalse(any(r["kind"] == "probe" for r in rows))

    def test_preflight_refuses_already_cordoned_pool_node(self):
        with tempfile.TemporaryDirectory() as tmp:
            kube = FakeKube(NODES, cordoned={"w2"})
            bench = xnode.XnodeBenchmark(fake_args(tmp), kube=kube, ecw=FakeEcw(kube))
            with self.assertRaises(SystemExit):
                bench.run()
            self.assertEqual(kube.calls, [])


def synthetic(variant_ms, baseline_ms, *, blocks=4, corrupt=0, probe_v=50, probe_b=50, n=2000):
    rows, rng = [], random.Random(3)
    labels = [("baseline", baseline_ms, probe_b), ("v", variant_ms, probe_v)] * blocks
    t = 0
    for index, (label, center, probe) in enumerate(labels):
        start, end = t, t + 1_000_000
        rows.append({"kind": "block", "run_id": "r", "block": index, "variant": label, "load": "loaded",
                     "start_ms": start, "end_ms": end, "warmup_ms": 100_000})
        per_block = n // blocks
        for i in range(per_block + 10):
            wid = f"{index}-{i}"
            ts = start + 100_000 + i * 100 if i < per_block else start + 10  # last 10 fall in warm-up
            bad = label == "v" and i < corrupt
            rows.append({"kind": "wake", "run_id": "r", "block": index, "variant": label, "wake_id": wid,
                         "sandbox_id": wid, "wake_start_ms": ts, "reconnect_ms": rng.gauss(center, center * 0.1),
                         "outcome": "ok", "correctness_ok": not bad})
            rows.append({"kind": "wake_placement", "run_id": "r", "block": index, "wake_id": wid,
                         "classification": "cross_node_download"})
        for i in range(200):
            rows.append({"kind": "probe", "run_id": "r", "block": index, "variant": label,
                         "t_ms": start + 100_000 + i * 1000, "rtt_ms": rng.gauss(probe, 3), "ok": True})
        t = end
    return rows


def decision(rows):
    out = summary.summarize(rows, baseline="baseline", level=0.983, resamples=200, probe_resamples=100,
                            probe_cap=5000, seed=1, bar_pct=0.1, bar_ms=1000, regression_pct=0.05,
                            wake_timeout_ms=120_000)
    return out["loads"]["loaded"]["variants"]["v"]


class SummaryTests(unittest.TestCase):
    def test_percentile_matches_linear_interpolation(self):
        self.assertEqual(summary.percentile([1, 2, 3, 4], 50), 2.5)
        self.assertEqual(summary.percentile([10], 95), 10.0)
        self.assertIsNone(summary.percentile([], 50))

    def test_proceed_needs_bar_interval_and_blocks(self):
        result = decision(synthetic(variant_ms=10_000, baseline_ms=15_000))
        self.assertEqual(result["vs_baseline"]["decision"], "proceed_pending_cause")
        self.assertEqual(result["excluded_warmup"], 40)
        self.assertEqual(result["reconnect_ms"]["n"], 2000)

    def test_small_improvement_stops(self):
        self.assertEqual(decision(synthetic(variant_ms=14_500, baseline_ms=15_000))["vs_baseline"]["decision"],
                         "stop")

    def test_too_few_samples_is_inconclusive(self):
        self.assertEqual(decision(synthetic(variant_ms=10_000, baseline_ms=15_000, n=400))["vs_baseline"]["decision"],
                         "inconclusive")

    def test_correctness_failure_stops_now(self):
        self.assertEqual(decision(synthetic(variant_ms=10_000, baseline_ms=15_000, corrupt=1))["vs_baseline"]["decision"],
                         "stop_now")

    def test_bystander_regression_stops(self):
        rows = synthetic(variant_ms=10_000, baseline_ms=15_000, probe_v=70, probe_b=50)
        self.assertEqual(decision(rows)["vs_baseline"]["decision"], "regression_stop")

    def test_failures_count_against_the_variant(self):
        rows = synthetic(variant_ms=10_000, baseline_ms=15_000)
        for row in rows:
            if row["kind"] == "wake" and row["variant"] == "v" and row["wake_id"].endswith(("-1", "-2")):
                row["outcome"] = "timeout"
        result = decision(rows)
        self.assertGreater(result["failures"], 0)
        self.assertGreater(result["failure_inclusive_p95_ms"], result["reconnect_ms"]["p95"])

    def test_same_node_restores_are_excluded(self):
        rows = synthetic(variant_ms=10_000, baseline_ms=15_000)
        for row in rows:
            if row["kind"] == "wake_placement" and row["wake_id"].endswith("-5"):
                row["classification"] = "same_node"
        result = decision(rows)
        self.assertLess(result["reconnect_ms"]["n"], 2000)
        self.assertGreater(result["classification"]["same_node"], 0)


if __name__ == "__main__":
    unittest.main()
