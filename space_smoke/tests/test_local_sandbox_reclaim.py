"""Which local-delete refusals the per-worker reclaim retries.

A node-manager mutation claim can contend with reclaim. Its retryable refusal
must not become an immediate teardown failure.
"""

from __future__ import annotations

import time
import unittest
from unittest import mock

from space_smoke.adapters import KindConfig, KindHarness
from space_smoke.runtime import CommandResult

SID = "00000000-0000-4000-8000-000000000001"
NODE = "space-local-worker2"
POD = "space-sandbox-manager-abcde"
ARGV = ("kubectl",)

# Synthetic response retaining the retryable gRPC status and error text.
NODE_MANAGER_CONTENTION = (
    "2025-01-01T00:00:00.000000Z ERROR grpc.client: gRPC client received non-OK "
    'status rpc_log_side="client" grpc_code=14 grpc_code_name="The service is '
    'currently unavailable" latency_ms=7 failure_origin="server-status"\n'
    "ERROR: code: 'The service is currently unavailable', message: \"durable "
    'sandbox cleanup: sandbox host mutation or reset is in progress; retry"\n'
    "command terminated with exit code 1"
)


def _reclaim(*results: CommandResult, budget: float) -> tuple[str | None, list[str]]:
    harness = KindHarness(mock.Mock(), KindConfig())
    scripts: list[str] = []

    def execute(_node: str, script: str, *, timeout: float) -> CommandResult:
        scripts.append(script)
        return outcomes.pop(0)

    outcomes = list(results)
    with (
        mock.patch.object(harness, "ready_node_service_pod", return_value=POD),
        mock.patch.object(harness, "_node_shell_exec", side_effect=execute),
        mock.patch("space_smoke.adapters.time.sleep"),
    ):
        failure = harness._reclaim_local_sandbox_on(NODE, SID, time.monotonic() + budget)
    return failure, scripts


class LocalSandboxReclaim(unittest.TestCase):
    def test_node_manager_claim_contention_is_retried_until_the_delete_lands(self) -> None:
        failure, scripts = _reclaim(
            CommandResult(ARGV, 1, "", NODE_MANAGER_CONTENTION),
            CommandResult(ARGV, 0, "Deleted 1 sandbox.", ""),
            budget=45,
        )

        self.assertIsNone(failure)
        self.assertEqual(scripts, [f"space-sbxmgr delete --blocking {SID}"] * 2)

    def test_contention_past_the_delete_budget_is_reported(self) -> None:
        failure, scripts = _reclaim(
            CommandResult(ARGV, 1, "", NODE_MANAGER_CONTENTION),
            budget=-1,
        )

        self.assertEqual(len(scripts), 1)
        assert failure is not None
        self.assertIn(f"the delete on {NODE} failed", failure)
        self.assertIn("mutation or reset is in progress", failure)

    def test_a_refusal_that_does_not_settle_is_reported_without_retry(self) -> None:
        failure, scripts = _reclaim(
            CommandResult(ARGV, 1, "", "ERROR: sandbox has a retained filesystem-only copy"),
            budget=45,
        )

        self.assertEqual(len(scripts), 1)
        assert failure is not None
        self.assertIn("retained filesystem-only copy", failure)


if __name__ == "__main__":
    unittest.main()
