"""Contracts of the sandbox-isolation scenario the bench shows only on success.

A passing bench shows every refusal it expects. It cannot show that the
scenario fails when the policies it guards are gone: deleting the
default-deny must fail the unselected-pod phase (the allow policy alone still
isolates sandbox pods), and deleting both must fail the sandbox-pod phases.
`_Cluster` models the two policies' Kubernetes semantics for the probes the
scenario takes, so those outcomes are pinned here; the live check is the same
scenario run against a cluster with the policy removed.

The adapter half pins the probe script's input checks and parsing, the
Service target reads, and that sandbox-pod reads name the sandbox namespace.
"""

from __future__ import annotations

import json
import subprocess
import unittest
from types import SimpleNamespace
from typing import Any, cast

from space_smoke.adapters import (
    KIND_SANDBOX_NAMESPACE,
    KindConfig,
    KindHarness,
    _parse_tcp_probes,
    _service_tcp_target,
    _tcp_probe_script,
)
from space_smoke.capabilities import PodNetwork, TcpProbe, TcpTarget
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    SmokeConfigurationError,
)
from space_smoke.sandbox_network_isolation import (
    INTERNET_TARGET,
    SandboxNetworkIsolationAssertions,
)

SANDBOX = PodNetwork("space-sbx-a-g1", "worker", "10.244.1.10")
PEER = PodNetwork("space-sbx-b-g1", "worker2", "10.244.2.10")
DNS = TcpTarget("cluster-dns", "10.96.0.10", 53)
TARGETS = [
    TcpTarget("postgres", "10.96.10.1", 5432),
    TcpTarget("kubernetes-api", "10.96.0.1", 443),
    TcpTarget("internal-api", "10.96.10.2", 9800),
    TcpTarget("object-store", "10.96.10.3", 8333),
]


class _Fixture:
    def __init__(self) -> None:
        self.sandboxes = SimpleNamespace(ids=("sbx-a", "sbx-b"))
        self.records: dict[str, object] = {}
        self.https_waits = 0

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait_for_https_access(self, url: str) -> str:
        self.https_waits += 1
        return url

    def expect_https_reachable(self, observed: str) -> None:
        pass


class _Cluster:
    """Kubernetes NetworkPolicy semantics for the scenario's probes.

    `default_deny` selects every pod in the sandbox namespace for both
    directions with no allowances; `allow` selects sandbox pods only and
    admits ingress on 9800 from space-api and cluster-controller, egress to
    the cluster DNS and the internet. A pod no policy selects is open.
    """

    def __init__(self, *, default_deny: bool = True, allow: bool = True, internet: bool = True) -> None:
        self.default_deny = default_deny
        self.allow = allow
        self.internet = internet
        self.calls: list[str] = []
        self.stray: PodNetwork | None = None

    # SandboxNetworkIsolationControl

    def sandbox_pod_network(self, sandbox_id: str) -> PodNetwork | None:
        return {"sbx-a": SANDBOX, "sbx-b": PEER}.get(sandbox_id)

    def control_plane_targets(self) -> list[TcpTarget]:
        return list(TARGETS)

    def cluster_dns_target(self) -> TcpTarget:
        return DNS

    def probe_from_node_shell(self, node_name, targets, *, timeout):
        return [self._probe("shell", target) for target in targets]

    def probe_from_pod_netns(self, pod, targets, *, timeout):
        source = "stray" if pod == self.stray else "sandbox"
        return [self._probe(source, target) for target in targets]

    def start_unselected_pod(self, node_name: str, token: str) -> PodNetwork:
        self.calls.append("start-stray")
        self.stray = PodNetwork(f"space-smoke-unselected-{token}", node_name, "10.244.1.99")
        return self.stray

    def remove_unselected_pods(self, token: str) -> None:
        self.calls.append("remove-stray")

    def _probe(self, source: str, target: TcpTarget) -> TcpProbe:
        if target == INTERNET_TARGET and not self.internet:
            return TcpProbe(target, False, "timed-out")
        sandbox_ips = {SANDBOX.pod_ip, PEER.pod_ip}
        # Ingress: a sandbox pod is isolated by either policy, and neither
        # admits the operator shell or another sandbox.
        if target.host in sandbox_ips and (self.default_deny or self.allow):
            return TcpProbe(target, False, "timed-out")
        if source == "sandbox" and (self.default_deny or self.allow):
            admitted = self.allow and (target == DNS or target == INTERNET_TARGET)
            return TcpProbe(target, admitted, "connected" if admitted else "timed-out")
        if source == "stray" and self.default_deny:
            return TcpProbe(target, False, "timed-out")
        return TcpProbe(target, True, "connected")


def _run(cluster: _Cluster) -> tuple[_Fixture, ResourceScope]:
    fixture = _Fixture()
    scope = ResourceScope()
    isolation = SandboxNetworkIsolationAssertions(cast(Any, fixture), cast(Any, cluster), scope)
    try:
        isolation.locate()
        isolation.expect_targets_answer_outside_the_namespace()
        isolation.expect_sandbox_pod_confined()
        isolation.expect_sandbox_port_closed_to_other_namespaces()
        isolation.expect_unselected_pod_isolated()
        isolation.expect_guest_internet()
    finally:
        scope.close()
    return fixture, scope


class PolicyOutcomes(unittest.TestCase):
    def test_both_policies_in_place_pass_and_record_every_probe(self) -> None:
        fixture, _ = _run(_Cluster())
        for name in (
            "control_namespace_probes",
            "sandbox_namespace_probes",
            "other_namespace_probes",
            "unselected_pod_probes",
        ):
            self.assertIn(name, fixture.records)
        self.assertTrue(fixture.records["host_internet"])
        self.assertEqual(fixture.https_waits, 1)

    def test_without_the_default_deny_the_unselected_pod_phase_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "no allow rule selects connected"):
            _run(_Cluster(default_deny=False))

    def test_without_either_policy_the_sandbox_pod_phase_fails(self) -> None:
        with self.assertRaisesRegex(InvariantViolation, "sandbox pod's network namespace connected"):
            _run(_Cluster(default_deny=False, allow=False))

    def test_without_the_allow_policy_the_namespace_entry_control_fails(self) -> None:
        # Default-deny alone refuses the cluster DNS too, so the scenario
        # cannot tell its refusals from a namespace that reaches nothing.
        with self.assertRaisesRegex(InvariantViolation, "could not open a connection the policy allows"):
            _run(_Cluster(allow=False))

    def test_a_target_dead_everywhere_fails_the_control_not_the_policy(self) -> None:
        cluster = _Cluster()
        real = cluster._probe

        def dead_postgres(source: str, target: TcpTarget) -> TcpProbe:
            if target.name == "postgres":
                return TcpProbe(target, False, "refused rc=1")
            return real(source, target)

        cluster._probe = dead_postgres  # type: ignore[method-assign]
        with self.assertRaisesRegex(InvariantViolation, "did not answer from the control-plane namespace"):
            _run(cluster)

    def test_a_host_without_internet_skips_the_internet_checks_and_says_so(self) -> None:
        fixture, _ = _run(_Cluster(internet=False))
        self.assertFalse(fixture.records["host_internet"])
        self.assertEqual(fixture.https_waits, 0)
        self.assertIn("not judged", str(fixture.records["guest_internet"]))
        probed = [entry["name"] for entry in cast(list, fixture.records["sandbox_namespace_probes"])]
        self.assertNotIn(INTERNET_TARGET.name, probed)

    def test_the_unselected_pod_is_removed_even_when_its_start_fails(self) -> None:
        cluster = _Cluster()

        def failing_start(node_name: str, token: str) -> PodNetwork:
            cluster.calls.append("start-stray")
            raise TimeoutError("create timed out after the write")

        cluster.start_unselected_pod = failing_start  # type: ignore[method-assign]
        with self.assertRaises(TimeoutError):
            _run(cluster)
        self.assertEqual(cluster.calls, ["start-stray", "remove-stray"])

    def test_a_missing_sandbox_pod_fails_before_any_probe(self) -> None:
        cluster = _Cluster()
        cluster.sandbox_pod_network = lambda sandbox_id: None  # type: ignore[method-assign]
        with self.assertRaisesRegex(InvariantViolation, "no running execution pod"):
            _run(cluster)


class ProbeScript(unittest.TestCase):
    def test_the_script_is_valid_bash_with_and_without_a_pod_namespace(self) -> None:
        for pod_ip in (None, SANDBOX.pod_ip):
            script = _tcp_probe_script(TARGETS, timeout=3, pod_ip=pod_ip)
            checked = subprocess.run(["bash", "-n", "-c", script], capture_output=True, text=True)
            self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_the_pod_namespace_is_found_by_its_exact_address(self) -> None:
        script = _tcp_probe_script(TARGETS[:1], timeout=3, pod_ip="10.244.1.1")
        self.assertIn('grep -qF " inet 10.244.1.1/"', script)
        self.assertIn('nsenter --net="$candidate"', script)

    def test_unsafe_inputs_are_refused(self) -> None:
        bad = [
            ([TcpTarget("x;rm", "10.0.0.1", 1)], {}),
            ([TcpTarget("ok", "10.0.0.1; true", 1)], {}),
            ([TcpTarget("ok", "10.0.0.1", 0)], {}),
            ([TcpTarget("a", "10.0.0.1", 1), TcpTarget("a", "10.0.0.2", 1)], {}),
            (TARGETS, {"pod_ip": "10.244.1.1 || true"}),
            (TARGETS, {"timeout": 0}),
            ([], {}),
        ]
        for targets, overrides in bad:
            options: dict[str, Any] = {"timeout": 3, "pod_ip": None, **overrides}
            with self.subTest(targets=targets, overrides=overrides), self.assertRaises(ValueError):
                _tcp_probe_script(targets, **options)

    def test_results_are_parsed_per_target_and_a_missing_one_fails(self) -> None:
        output = "NETNS /var/run/netns/cni-1\nPROBE postgres timed-out\nPROBE kubernetes-api connected\n"
        probes = _parse_tcp_probes(output, TARGETS[:2])
        self.assertEqual([probe.connected for probe in probes], [False, True])
        self.assertEqual(probes[0].detail, "timed-out")
        with self.assertRaisesRegex(RuntimeError, "reported nothing"):
            _parse_tcp_probes(output, TARGETS[:3])


class ServiceTargets(unittest.TestCase):
    def test_a_service_target_needs_an_ipv4_cluster_ip_and_the_declared_port(self) -> None:
        service = {"spec": {"clusterIP": "10.96.0.1", "ports": [{"port": 443, "protocol": "TCP"}]}}
        self.assertEqual(
            _service_tcp_target("kubernetes-api", service, 443),
            TcpTarget("kubernetes-api", "10.96.0.1", 443),
        )
        with self.assertRaisesRegex(SmokeConfigurationError, "no TCP port 5432"):
            _service_tcp_target("kubernetes-api", service, 5432)
        with self.assertRaisesRegex(SmokeConfigurationError, "no IPv4 ClusterIP"):
            _service_tcp_target("headless", {"spec": {"clusterIP": "None"}}, 443)


class _Recorder:
    def __init__(self, stdout: str = "", returncode: int = 0) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.stdout = stdout
        self.returncode = returncode

    def run(self, command, **_options) -> CommandResult:
        self.commands.append(tuple(command))
        return CommandResult(tuple(command), self.returncode, self.stdout, "")


class SandboxNamespaceReads(unittest.TestCase):
    def test_the_sandbox_pod_is_read_in_the_sandbox_namespace(self) -> None:
        pods = {
            "items": [
                {
                    "metadata": {"name": "space-sbx-old-g1", "deletionTimestamp": "now"},
                    "spec": {"nodeName": "worker"},
                    "status": {"phase": "Running", "podIP": "10.244.1.3"},
                },
                {
                    "metadata": {"name": "space-sbx-a-g2"},
                    "spec": {"nodeName": "worker2"},
                    "status": {"phase": "Running", "podIP": "10.244.2.4"},
                },
            ]
        }
        recorder = _Recorder(json.dumps(pods))
        pod = KindHarness(recorder, KindConfig()).sandbox_pod_network(
            "01a0b0c9-b8a3-7473-9c12-3821a701bc28"
        )
        self.assertEqual(pod, PodNetwork("space-sbx-a-g2", "worker2", "10.244.2.4"))
        self.assertEqual(recorder.commands[0][3:5], ("-n", KIND_SANDBOX_NAMESPACE))

    def test_no_namespace_beyond_the_two_fixed_ones_is_addressed(self) -> None:
        with self.assertRaisesRegex(ValueError, "refusing kubectl in namespace"):
            KindHarness(_Recorder(), KindConfig()).kubectl("get", "pods", namespace="default")


if __name__ == "__main__":
    unittest.main()
