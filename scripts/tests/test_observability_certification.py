"""Reject false certification from incomplete or misleading backend evidence."""

import base64
import copy
import importlib.util
import unittest
from pathlib import Path

PATH = Path(__file__).parent / "observability/certification/assertions.py"
spec = importlib.util.spec_from_file_location("certification_assertions", PATH)
checks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checks)


def row(values, **labels):
    return {"metric": labels, "values": values}


class CertificationAssertions(unittest.TestCase):
    def test_counter_requires_real_growth_without_resets(self):
        cases = [
            ([], False),
            ([row([[1, "2"], [11, "2"], [21, "2"]])], False),
            ([row([[1, "2"], [11, "4"], [21, "6"]])], True),
            ([row([[1, "9"], [11, "1"], [21, "3"]])], False),
            ([row([[1, "1"], [2, "2"], [3, "3"]])], False),
            ([row([[1, "1"], [11, "2"], [21, "3"]]), row([[1, "1"]])], False),
        ]
        for rows, expected in cases:
            with self.subTest(rows=rows):
                self.assertEqual(checks.counter_monotonic(rows), expected)
        for points in ([[1, "NaN"], [11, "2"]], [[11, "1"], [1, "2"]]):
            with self.assertRaises(ValueError):
                checks.counter_monotonic([row(points)])

    def test_stale_or_disappearing_gauge_is_not_zero(self):
        cases = [
            ([[2, "1"], [12, "0"]], True),
            ([[2, "1"]], False),
            ([[0, "0"], [2, "1"]], False),
            ([[2, "1"], [9, "0"]], False),
            ([[2, "0"], [12, "0"]], False),
        ]
        for points, expected in cases:
            self.assertEqual(checks.gauge_lifecycle([row(points)], 1, 5, 10), expected)
        self.assertFalse(
            checks.gauge_lifecycle([row([[2, "1"]]), row([[12, "0"]])], 1, 5, 10)
        )

    def test_native_histogram_rejects_classic_buckets_and_empty_series(self):
        self.assertFalse(checks.native_histograms([]))
        self.assertFalse(
            checks.native_histograms([row([[1, "2"]], __name__="latency_bucket")])
        )
        histogram = {
            "histograms": [
                [1, {"count": "4", "sum": "8", "buckets": [[0, "1", "2", "4"]]}]
            ]
        }
        self.assertTrue(checks.native_histograms([histogram]))
        self.assertFalse(checks.native_histograms([histogram, {"histograms": []}]))

    def test_identity_collision_and_wrong_cluster_are_distinct_failures(self):
        a = row(
            [[1, "1"]],
            service_name="api",
            service="api",
            service_instance_id="pod:1",
            service_version="revision",
            k8s_cluster_name="kube-cluster",
        )
        self.assertEqual(
            checks.identities([a], "api", "revision", "kube-cluster"),
            (True, True, True),
        )
        wrong = copy.deepcopy(a)
        wrong["metric"].update(service="peer", k8s_cluster_name="platform-cluster")
        self.assertEqual(
            checks.identities([wrong], "api", "revision", "kube-cluster"),
            (True, False, False),
        )
        self.assertEqual(
            checks.identities([], "api", "revision", "kube-cluster"),
            (False, False, False),
        )
        self.assertFalse(checks.distinct_producers({"api": [a], "worker": [a]}))
        wrong["metric"]["service_instance_id"] = "pod:2"
        self.assertTrue(checks.distinct_producers({"api": [a], "worker": [wrong]}))

    def test_trace_needs_a_client_server_edge_across_services_in_same_revision(self):
        tid = "11" * 16
        parent_id, child_id = "22" * 8, "33" * 8

        def batch(service, span):
            return {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service}},
                        {
                            "key": "service.version",
                            "value": {"stringValue": "revision"},
                        },
                    ]
                },
                "scopeSpans": [{"spans": [span]}],
            }

        def span(sid, parent, kind):
            result = {
                "spanId": base64.b64encode(bytes.fromhex(sid)).decode(),
                "traceId": base64.b64encode(bytes.fromhex(tid)).decode(),
                "kind": kind,
                "startTimeUnixNano": "100",
                "endTimeUnixNano": "200",
            }
            if parent:
                result["parentSpanId"] = base64.b64encode(
                    bytes.fromhex(parent)
                ).decode()
            return result

        client = span(parent_id, None, "SPAN_KIND_CLIENT")
        server = span(child_id, parent_id, "SPAN_KIND_SERVER")
        trace = {"batches": [batch("api", client), batch("worker", server)]}
        seen, edges = checks.trace_evidence(trace, tid, ["api", "worker"], "revision")
        self.assertEqual(seen, {"api", "worker"})
        self.assertEqual(
            [(e["parent_service"], e["child_service"]) for e in edges],
            [("api", "worker")],
        )
        server["parentSpanId"] = "44" * 8
        self.assertEqual(
            checks.trace_evidence(trace, tid, ["api", "worker"], "revision")[1], []
        )
        server["parentSpanId"] = child_id
        with self.assertRaises(ValueError):
            checks.trace_evidence(trace, tid, ["api", "worker"], "revision")
        server["parentSpanId"] = parent_id
        server["traceId"] = "55" * 16
        with self.assertRaises(ValueError):
            checks.trace_evidence(trace, tid, ["api", "worker"], "revision")
        server["traceId"] = tid
        self.assertEqual(
            checks.trace_evidence(trace, tid, ["api", "worker"], "other-version")[1], []
        )

    def test_expected_failure_does_not_hide_success_or_backend_errors(self):
        self.assertEqual(checks.verdict(True, True), "pass")
        self.assertEqual(checks.verdict(False, True), "xfail")
        self.assertEqual(checks.verdict(False, False), "fail")
        with self.assertRaises(ValueError):
            checks.series({"status": "error", "error": "timeout"})
        self.assertEqual(
            checks.series({"status": "success", "data": {"result": []}}), []
        )


if __name__ == "__main__":
    unittest.main()
