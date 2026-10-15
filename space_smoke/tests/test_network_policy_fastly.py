"""Tests for the deployed Fastly shared-edge network-policy smoke."""

from __future__ import annotations

import json
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from typing import cast

import space_smoke as smoke
from space_smoke.capabilities import EcwCommandResult
from space_smoke.fixtures import Ecw
from space_smoke.network_policy_fastly import (
    _EDGE_ERROR_BASELINE,
    _NO_EDGE_BASELINE,
    _OVERRIDE_UNANSWERED,
    _REFUSED_BASELINE,
    classify,
    verify_fastly_shared_edge_bypass,
)
from space_smoke.runtime import InconclusiveSmoke, InvariantViolation
from space_smoke.scenarios import REGISTRY


class _Files:
    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []

    def write(self, path: str, contents: str) -> None:
        self.writes.append((path, contents))


def _edge(first_line: str, body: str) -> dict[str, object]:
    return {"first_line": first_line, "body": body, "edge_served": True}


def _gateway(first_line: str) -> dict[str, object]:
    return {"first_line": first_line, "body": "", "edge_served": False}


EDGE_SSL_REQUIRED = _edge("HTTP/1.1 403 SSL is required", "SSL is required.")
GATEWAY_FORBIDDEN = _gateway("HTTP/1.1 403 Forbidden")
GATEWAY_UPSTREAM_FAILED = _gateway("HTTP/1.1 502 Bad Gateway")
# The co-tenant's own plain-HTTP answer carries no X-Served-By header.
CO_TENANT_REDIRECT = _gateway("HTTP/1.1 301 Moved Permanently")
NO_BASELINE = {
    "fastly_headers_stripped": _NO_EDGE_BASELINE,
    "matching_http_reached_edge": _NO_EDGE_BASELINE,
}


def _observed(**overrides: object) -> dict[str, object]:
    """A probe run in which the edge answers and every isolation check holds."""

    observed: dict[str, object] = {
        "resolved_ip": "192.0.2.10",
        "matching_tls": {"connected": True, "first_line": "HTTP/1.1 200 OK"},
        "co_tenant_tls": {"connected": False, "error_type": "ConnectionResetError"},
        "matching_http": EDGE_SSL_REQUIRED,
        "co_tenant_http": GATEWAY_FORBIDDEN,
        "unrelated_http": GATEWAY_FORBIDDEN,
        "fastly_host_override": EDGE_SSL_REQUIRED,
    }
    observed.update(overrides)
    return observed


NO_VERDICT = "Fastly shared-edge guest probe reported no observations"
ESCAPED = "Fastly shared-edge traffic escaped its DNS-attributed hostname"
ALLOW_FAILED = "the gateway refused the allowed Fastly host while every bypass check held"


class _ProbeTest:
    def __init__(self, stdout: str) -> None:
        self.sandbox = type("Sandbox", (), {"files": _Files()})()
        self.result = EcwCommandResult(0, stdout, "")
        self.verifications: list[tuple[bool, str]] = []
        self.recorded: dict[str, object] = {}

    @contextmanager
    def phase(self, _name: str) -> Iterator[None]:
        yield

    def run_command(self, _command: str, **_kwargs: object) -> EcwCommandResult:
        return self.result

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        self.verifications.append((condition, message))
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.recorded[name] = value

    def collect_guest_network_snapshot(self) -> dict[str, object]:
        return {"guest_snapshot_status": "exit=0"}


class _ScenarioFixture:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.network_policy = self

    def verify_fastly_shared_edge_bypass(self) -> None:
        self.calls.append("verify_fastly_shared_edge_bypass")


class NetworkPolicyFastlyTests(unittest.TestCase):
    def test_classify_judges_against_an_edge_served_baseline(self) -> None:
        cases: list[tuple[str, dict[str, object], dict[str, object]]] = [
            ("held", _observed(), {}),
            (
                # The gateway's 502 for a failed upstream fetch carries no edge
                # evidence, so it is no baseline for the override comparison.
                "allowed fetch failed upstream",
                _observed(matching_http=GATEWAY_UPSTREAM_FAILED),
                {"inconclusive": NO_BASELINE},
            ),
            (
                "allowed request timed out",
                _observed(matching_http={"error_type": "TimeoutError"}),
                {"inconclusive": NO_BASELINE},
            ),
            (
                "edge 5xx baseline",
                _observed(matching_http=_edge("HTTP/1.1 503 Service Unavailable", "busy")),
                {"inconclusive": {"fastly_headers_stripped": _EDGE_ERROR_BASELINE}},
            ),
            (
                "override fetch failed upstream",
                _observed(fastly_host_override=GATEWAY_UPSTREAM_FAILED),
                {"inconclusive": {"fastly_headers_stripped": _OVERRIDE_UNANSWERED}},
            ),
            (
                "override timed out",
                _observed(fastly_host_override={"error_type": "TimeoutError"}),
                {"inconclusive": {"fastly_headers_stripped": _OVERRIDE_UNANSWERED}},
            ),
            (
                "override got an edge 5xx",
                _observed(fastly_host_override=_edge("HTTP/1.1 503 Service Unavailable", "busy")),
                {"inconclusive": {"fastly_headers_stripped": _OVERRIDE_UNANSWERED}},
            ),
            (
                # The co-tenant's answer: only routing on Fastly-Host yields it.
                "edge routed the override to the co-tenant",
                _observed(fastly_host_override=CO_TENANT_REDIRECT),
                {"bypass_detected": ["fastly_headers_stripped"]},
            ),
            (
                "override got a different body",
                _observed(fastly_host_override=_edge("HTTP/1.1 403 SSL is required", "tenant")),
                {"bypass_detected": ["fastly_headers_stripped"]},
            ),
            (
                # Indistinguishable from a co-tenant origin's empty 403, and
                # the gateway strips rather than refuses this request.
                "override got an empty 403 without edge headers",
                _observed(fastly_host_override=GATEWAY_FORBIDDEN),
                {"bypass_detected": ["fastly_headers_stripped"]},
            ),
            (
                "gateway refused the allowed host",
                _observed(matching_http=GATEWAY_FORBIDDEN),
                {
                    "allowed_failed": ["matching_http_reached_edge"],
                    "inconclusive": {"fastly_headers_stripped": _REFUSED_BASELINE},
                },
            ),
            (
                "allowed TLS failed",
                _observed(matching_tls={"connected": False, "error_type": "ConnectionResetError"}),
                {"allowed_failed": ["matching_tls"]},
            ),
            (
                "edge honored the routing header",
                _observed(
                    fastly_host_override=_edge("HTTP/1.1 421 Misdirected Request", "Misdirected request")
                ),
                {"bypass_detected": ["fastly_headers_stripped"]},
            ),
            (
                # An edge-served 403 means the request left the gateway.
                "co-tenant reached the edge",
                _observed(co_tenant_http=_edge("HTTP/1.1 403 Forbidden", "")),
                {"bypass_detected": ["co_tenant_http_blocked"]},
            ),
            (
                # A 502 follows host authorization, so the gateway admitted it.
                "co-tenant admitted, upstream failed",
                _observed(
                    matching_http=GATEWAY_UPSTREAM_FAILED,
                    co_tenant_http=GATEWAY_UPSTREAM_FAILED,
                ),
                {
                    "bypass_detected": ["co_tenant_http_blocked"],
                    "inconclusive": NO_BASELINE,
                },
            ),
            (
                "unrelated host reached the edge",
                _observed(unrelated_http=_edge("HTTP/1.1 200 OK", "<html>")),
                {"bypass_detected": ["unrelated_http_blocked"]},
            ),
            (
                "co-tenant TLS connected",
                _observed(co_tenant_tls={"connected": True, "first_line": "HTTP/1.1 200 OK"}),
                {"bypass_detected": ["co_tenant_tls_blocked"]},
            ),
        ]
        for name, observed, expected in cases:
            with self.subTest(name):
                self.assertEqual(
                    classify(observed),
                    {
                        "allowed_failed": expected.get("allowed_failed", []),
                        "bypass_detected": expected.get("bypass_detected", []),
                        "inconclusive": expected.get("inconclusive", {}),
                    },
                )

    def test_probe_installs_a_compilable_guest_script_and_passes_when_held(self) -> None:
        test = _ProbeTest(json.dumps(_observed()))

        verify_fastly_shared_edge_bypass(cast(Ecw, test))

        writes = test.sandbox.files.writes
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][0], "/tmp/space-smoke-fastly-shared-edge.py")
        compile(writes[0][1], writes[0][0], "exec")
        self.assertEqual(
            test.verifications,
            [(True, NO_VERDICT), (True, ESCAPED), (True, ALLOW_FAILED)],
        )

    def test_inconclusive_runs_name_the_check_that_could_not_be_judged(self) -> None:
        cases = [
            (
                "allowed fetch failed upstream",
                _observed(matching_http=GATEWAY_UPSTREAM_FAILED),
                ["fastly_headers_stripped", "matching_http_reached_edge"],
                _NO_EDGE_BASELINE,
            ),
            (
                "edge 5xx baseline",
                _observed(matching_http=_edge("HTTP/1.1 503 Service Unavailable", "busy")),
                ["fastly_headers_stripped"],
                _EDGE_ERROR_BASELINE,
            ),
            (
                "override fetch failed upstream",
                _observed(fastly_host_override=GATEWAY_UPSTREAM_FAILED),
                ["fastly_headers_stripped"],
                _OVERRIDE_UNANSWERED,
            ),
        ]
        for name, observed, checks, message in cases:
            with self.subTest(name):
                test = _ProbeTest(json.dumps(observed))

                with self.assertRaises(InconclusiveSmoke) as raised:
                    verify_fastly_shared_edge_bypass(cast(Ecw, test))

                self.assertEqual(str(raised.exception), message)
                self.assertEqual(
                    test.verifications,
                    [(True, NO_VERDICT), (True, ESCAPED), (True, ALLOW_FAILED)],
                )
                self.assertEqual(test.recorded["inconclusive_checks"], checks)
                self.assertIn("guest_snapshot_status", test.recorded)

    def test_a_routed_override_without_edge_headers_fails_as_an_escape(self) -> None:
        test = _ProbeTest(json.dumps(_observed(fastly_host_override=CO_TENANT_REDIRECT)))

        with self.assertRaises(InvariantViolation):
            verify_fastly_shared_edge_bypass(cast(Ecw, test))

        self.assertEqual(test.verifications[-1], (False, ESCAPED))

    def test_a_bypass_fails_as_an_escape_even_when_the_baseline_is_missing(self) -> None:
        test = _ProbeTest(
            json.dumps(
                _observed(
                    matching_http=GATEWAY_UPSTREAM_FAILED,
                    co_tenant_http=_edge("HTTP/1.1 200 OK", "tenant"),
                )
            )
        )

        with self.assertRaises(InvariantViolation):
            verify_fastly_shared_edge_bypass(cast(Ecw, test))

        self.assertEqual(test.verifications[-1], (False, ESCAPED))

    def test_unparseable_probe_output_is_reported(self) -> None:
        test = _ProbeTest("Traceback (most recent call last):")

        with self.assertRaises(InvariantViolation):
            verify_fastly_shared_edge_bypass(cast(Ecw, test))

        self.assertEqual(test.verifications, [(False, NO_VERDICT)])

    def test_scenario_allows_only_the_fastly_host_that_seeds_attribution(self) -> None:
        scenario = REGISTRY.get("network_policy.v1_fastly_shared_edge_bypass")
        self.assertIs(scenario.sandbox.allow_internet_access, False)
        self.assertEqual(
            scenario.sandbox.network,
            smoke.NetworkConfig(
                allow_public_traffic=False,
                allow_out=("pypi.org",),
            ),
        )

        fixture = _ScenarioFixture()
        scenario.verify(cast(Ecw, fixture))

        self.assertEqual(fixture.calls, ["verify_fastly_shared_edge_bypass"])


if __name__ == "__main__":
    unittest.main()
