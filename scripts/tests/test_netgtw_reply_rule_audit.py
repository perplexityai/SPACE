"""The rollout audit must reject every old exception, including duplicates."""

import copy
import importlib.util
import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "audit", ROOT / "scripts/netgtw/audit-reply-rules.py"
)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def broad_reply_bypasses():
    # Real nft output exercises the same input normalization as reconciliation.
    fixture = (
        ROOT / "crates/l40_api/node/mgr/server/src/fixtures/live_inbound_reply_chain.json"
    )
    forward = json.loads(fixture.read_text())["nftables"]
    rule = next(entry["rule"] for entry in forward if "rule" in entry)
    proxy = copy.deepcopy(rule)
    proxy.update(
        table="proxy", chain="prerouting", comment="skip-tproxy-inbound-replies"
    )
    proxy["expr"][-1] = {"return": None}
    return {"nftables": [{"rule": rule}, {"rule": proxy}]}


def current_rules():
    # The compliant state has neither broad bypass. Other nft entries are
    # intentionally ignored by this narrow rollout audit.
    return {"nftables": [{"metainfo": {"json_schema_version": 1}}]}


class ReplyRuleAuditTests(unittest.TestCase):
    def test_rejects_every_broad_bypass(self):
        self.assertEqual(audit.audit_ruleset(current_rules()), [])
        for index in range(2):
            with self.subTest(rule=index):
                rule = broad_reply_bypasses()["nftables"][index]
                ruleset = current_rules()
                ruleset["nftables"].extend([rule, copy.deepcopy(rule)])
                problems = audit.audit_ruleset(ruleset)
                self.assertEqual(len(problems), 2)
                self.assertTrue(all("direct reply bypass remains" in problem for problem in problems))
        for malformed in ({}, [], {"nftables": [None]}):
            with self.subTest(malformed=malformed), self.assertRaises(TypeError):
                audit.audit_ruleset(malformed)

    def test_discovery_and_inspection_errors_cannot_report_success(self):
        for results, expected in (
            ([], 2),
            ([{"name": "unrelated"}], 2),
            (PermissionError("cannot list namespaces"), 2),
        ):
            with (
                patch.object(audit.sys, "argv", ["audit"]),
                patch.object(
                    audit,
                    "read_json",
                    side_effect=results if isinstance(results, Exception) else None,
                    return_value=results,
                ),
                redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(audit.main(), expected)
        output = io.StringIO()
        with (
            patch.object(audit.sys, "argv", ["audit", "sns-a", "sns-b"]),
            patch.object(
                audit,
                "read_json",
                side_effect=[PermissionError("unreadable"), current_rules()],
            ),
            redirect_stdout(output),
        ):
            self.assertEqual(audit.main(), 1)
        self.assertIn("sns-a: inspection failed", output.getvalue())
        self.assertIn("sns-b: OK", output.getvalue())


if __name__ == "__main__":
    unittest.main()
