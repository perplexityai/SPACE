"""Exercise the parity comparator's ability to reject misleading evidence."""

import sys
import unittest
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))
import trace_parity as parity



def backend_rows(mode, offset=0):
    rows = []
    for trace, case in enumerate(("success", "error"), start=1):
        for index in range(7):
            span_id = offset + trace * 100 + index
            # The last span is the request's saved job: a root of its own trace.
            job = index == 6
            attributes = {
                "operation_name": "http.server",
                "error": int(case == "error" and index == 0),
            }
            if not job:
                attributes["parity.case"] = case
            rows.append(
                {
                    "id": str(span_id),
                    "attributes": {
                        "trace_id": str(offset + trace + (50 if job else 0)),
                        "span_id": str(span_id),
                        "parent_id": "0"
                        if index == 0 or job
                        else str(offset + trace * 100),
                        "service": parity.SERVICE,
                        "env": "local-test",
                        "host": "fixture",
                        "resource_name": "root"
                        if index == 0
                        else "parity"
                        if job
                        else f"child-{index}",
                        "type": "web" if index == 0 else "custom",
                        "start_timestamp": "2026-01-01T00:00:00Z",
                        "end_timestamp": "2026-01-01T00:00:00.001Z",
                        "tags": [f"version:{mode}"],
                        "attributes": attributes,
                    },
                }
            )
    return rows


class TraceParityTest(unittest.TestCase):
    def test_search_uses_span_attribute_version_and_accepts_null_page(self):
        backend = parity.Backend.__new__(parity.Backend)
        requests = []

        def request(path, body):
            requests.append((path, body))
            return {
                "data": backend_rows("otlp"),
                "meta": {"page": None, "status": "done"},
            }

        backend.request = request
        rows = backend.spans("local-test", "otlp", "now-5m")
        self.assertEqual(len(rows), 14)
        self.assertIn(
            "@version:otlp", requests[0][1]["data"]["attributes"]["filter"]["query"]
        )

    def test_retention_cleanup_requires_matching_owned_name_and_query(self):
        backend = parity.Backend.__new__(parity.Backend)
        current = {
            "name": "test-run",
            "filter": {"query": f"service:{parity.SERVICE} env:local-test"},
        }
        deleted = []

        def request(path, *, method=None):
            if method == "DELETE":
                deleted.append(path)
            return {"data": {"attributes": current}}

        backend.request = request
        backend.delete_retention("owned-id", "test-run", "local-test")
        self.assertEqual(len(deleted), 1)
        current["name"] = "someone-else"
        with self.assertRaisesRegex(RuntimeError, "ownership changed"):
            backend.delete_retention("owned-id", "test-run", "local-test")
        current["name"] = "test-run"
        current["filter"]["query"] = "service:production"
        with self.assertRaisesRegex(RuntimeError, "ownership changed"):
            backend.delete_retention("owned-id", "test-run", "local-test")
        self.assertEqual(len(deleted), 1)

    def test_backend_queries_resolve_reserved_fields_without_trusting_app_attributes(
        self,
    ):
        rows = backend_rows("datadog")
        backend = parity.Backend.__new__(parity.Backend)
        backend.request = lambda path, body: {
            "data": [{"attributes": {"by": {"operation_name": "backend.actual"}}}]
        }
        backend.spans = lambda env, mode, start, query: (
            [rows[7]] if query == "status:error" else rows
        )
        enriched, _ = backend.operational_fields(
            rows, "local-test", "datadog", "now-5m"
        )
        self.assertEqual(
            {row["attributes"]["operation_name"] for row in enriched},
            {"backend.actual"},
        )
        self.assertEqual(enriched[7]["attributes"]["status"], "error")
        self.assertNotIn("operation_name", rows[0]["attributes"])
        self.assertEqual(
            parity.canonical_trees(enriched, "datadog", "local-test")[0]["operation"],
            "backend.actual",
        )
        backend.spans = lambda *args: rows[:-1]
        with self.assertRaisesRegex(RuntimeError, "missing backend operation"):
            backend.operational_fields(rows, "local-test", "datadog", "now-5m")

    def test_generated_ids_order_and_sdk_metadata_do_not_change_semantics(self):
        left = backend_rows("datadog")
        right = list(reversed(backend_rows("otlp", 1000)))
        right[0]["attributes"]["attributes"]["telemetry.sdk.name"] = "opentelemetry"
        self.assertEqual(
            parity.canonical_trees(left, "datadog", "local-test"),
            parity.canonical_trees(right, "otlp", "local-test"),
        )

    def test_semantic_changes_remain_visible(self):
        left = parity.canonical_trees(backend_rows("datadog"), "datadog", "local-test")
        changes = (
            (
                "operation",
                lambda s: s["attributes"].update(operation_name="http.server.request"),
            ),
            ("resource", lambda s: s.update(resource_name="different-resource")),
            ("host", lambda s: s.update(host="different-host")),
            ("type", lambda s: s.update(type="db")),
            ("status", lambda s: s["attributes"].update(error=1)),
            ("attribute", lambda s: s["attributes"].update({"db.system": "redis"})),
            ("parent", lambda s: s.update(parent_id="102")),
        )
        for name, change in changes:
            with self.subTest(name=name):
                rows = backend_rows("otlp")
                change(rows[1]["attributes"])
                self.assertTrue(
                    parity.compare(
                        left, parity.canonical_trees(rows, "otlp", "local-test")
                    )
                )

    def test_incomplete_or_corrupt_evidence_cannot_pass(self):
        original = backend_rows("datadog")
        changes = (
            lambda rows: rows.pop(),
            lambda rows: rows.append(deepcopy(rows[0])),
            lambda rows: rows[1]["attributes"].update(parent_id="missing"),
            lambda rows: rows[1]["attributes"].update(span_id="100"),
            lambda rows: rows[0]["attributes"]["attributes"].pop("operation_name"),
            lambda rows: rows[0]["attributes"]["attributes"].update(error=1),
            lambda rows: rows[0]["attributes"].update(tags=[]),
            lambda rows: rows[0]["attributes"].update(service="other"),
            lambda rows: rows[0]["attributes"].update(
                end_timestamp="2025-01-01T00:00:00Z"
            ),
        )
        for index, change in enumerate(changes):
            with self.subTest(index=index):
                rows = deepcopy(original)
                change(rows)
                with self.assertRaises((ValueError, KeyError)):
                    parity.canonical_trees(rows, "datadog", "local-test")


if __name__ == "__main__":
    unittest.main()
