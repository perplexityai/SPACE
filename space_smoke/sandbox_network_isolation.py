"""The sandbox namespace's NetworkPolicies, probed from outside the VM.

The API creates sandbox pods in their own namespace, which the
`space-sandbox-network-policy` chart confines: a default-deny for every pod,
and an allow policy giving sandbox pods ingress on TCP 9800 from space-api and
cluster-controller only, egress to the IPv4 internet outside the private,
carrier-grade NAT and link-local ranges, and DNS to the cluster DNS pods.

Inside the VM, the per-sandbox net-gateway applies the tenant's egress policy
and its own guardrails, so a probe from the guest cannot tell whether the
namespace policy holds. These probes run in the node's operator shell instead
and enter a sandbox pod's network namespace by its netns handle. What they
reach is what any process that got into that namespace, outside the VM, would.

Each refusal is judged against a control taken the same way:

- From the operator shell's own namespace (the control plane's, under no
  policy), every control-plane target answers. A refusal from the sandbox pod
  is then the policy, not a dead target.
- From the sandbox pod's namespace, the cluster DNS answers on TCP 53, which
  the allow policy admits. So the namespace was entered, connects complete
  from it, and the refusals are selective.
- Another sandbox's pod IP is shown reachable by its own creation: the API's
  readiness dial on its 9800 is what let it reach running.

The allow policy isolates sandbox pods on its own; the default-deny is what
isolates every other pod in the namespace. A pause pod that no allow rule
selects is started there and must reach nothing, not even the cluster DNS.
Delete the default-deny and that phase fails; delete both policies and every
sandbox-pod phase does.
"""

from __future__ import annotations

import secrets
from collections.abc import Sequence
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    PodNetwork,
    SandboxNetworkIsolationControl,
    TcpProbe,
    TcpTarget,
)
from space_smoke.runtime import ResourceScope, SmokeConfigurationError

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# A policy drop is a SYN that never returns, so a refusal is a timeout; a
# completed connect inside the cluster takes milliseconds.
PROBE_TIMEOUT_SECONDS = 3.0
# Whether this host reaches the internet at all, asked from the control-plane
# namespace: a public anycast resolver's HTTPS port, by address, so no DNS is
# involved.
INTERNET_TARGET = TcpTarget(name="internet", host="1.1.1.1", port=443)
GUEST_INTERNET_URL = "https://example.com/"
SANDBOX_PROXY_PORT = 9800


class SandboxNetworkIsolationAssertions:
    """Probe the sandbox namespace's policies from two sandbox pods and a stray pod."""

    def __init__(
        self,
        fixture: KindEcw,
        control: SandboxNetworkIsolationControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._sandbox: PodNetwork | None = None
        self._peer: PodNetwork | None = None
        self._targets: list[TcpTarget] = []
        self._dns: TcpTarget | None = None
        self._host_internet: bool | None = None

    def locate(self) -> None:
        """Both sandboxes' running pods, and the targets they must not reach."""

        ids = self._fixture.sandboxes.ids
        if len(ids) < 2:
            raise SmokeConfigurationError("the isolation probes need two sandboxes")
        sandbox = self._control.sandbox_pod_network(ids[0])
        peer = self._control.sandbox_pod_network(ids[1])
        self._fixture.verify(
            sandbox is not None and peer is not None,
            "a sandbox has no running execution pod with a pod IP in the sandbox namespace",
            sandbox_ids=list(ids[:2]),
            sandbox_pod=_pod_evidence(sandbox),
            peer_pod=_pod_evidence(peer),
        )
        assert sandbox is not None and peer is not None
        self._sandbox, self._peer = sandbox, peer
        self._targets = self._control.control_plane_targets()
        self._dns = self._control.cluster_dns_target()
        self._fixture.record("sandbox_pod", _pod_evidence(sandbox))
        self._fixture.record("peer_sandbox_pod", _pod_evidence(peer))
        self._fixture.record(
            "control_plane_targets", [_target_evidence(target) for target in self._targets]
        )

    def expect_targets_answer_outside_the_namespace(self) -> None:
        """Control: from the control-plane namespace every target, and the cluster DNS, answers."""

        sandbox, dns = self._located()
        probes = self._control.probe_from_node_shell(
            sandbox.node_name,
            [*self._targets, dns, INTERNET_TARGET],
            timeout=PROBE_TIMEOUT_SECONDS,
        )
        self._fixture.record("control_namespace_probes", _probe_evidence(probes))
        internet = [probe for probe in probes if probe.target == INTERNET_TARGET]
        self._host_internet = bool(internet and internet[0].connected)
        self._fixture.record("host_internet", self._host_internet)
        silent = [
            probe.target.name
            for probe in probes
            if probe.target != INTERNET_TARGET and not probe.connected
        ]
        self._fixture.verify(
            not silent,
            "a target did not answer from the control-plane namespace either, so a refusal "
            "from the sandbox pod would prove nothing about the policy",
            silent_targets=silent,
        )

    def expect_sandbox_pod_confined(self) -> None:
        """From the sandbox pod's own namespace, outside the VM, only what the policy allows connects."""

        sandbox, dns = self._located()
        assert self._peer is not None
        peer = TcpTarget(name="peer-sandbox", host=self._peer.pod_ip, port=SANDBOX_PROXY_PORT)
        allowed = [dns] + ([INTERNET_TARGET] if self._host_internet else [])
        refused = [*self._targets, peer]
        probes = self._control.probe_from_pod_netns(
            sandbox, [*allowed, *refused], timeout=PROBE_TIMEOUT_SECONDS
        )
        self._fixture.record("sandbox_namespace_probes", _probe_evidence(probes))
        self._expect(
            probes,
            allowed=allowed,
            refused=refused,
            what="the sandbox pod's network namespace",
        )

    def expect_sandbox_port_closed_to_other_namespaces(self) -> None:
        """A pod in the control-plane namespace that is neither space-api nor
        cluster-controller cannot open the sandbox pod's 9800."""

        sandbox, _ = self._located()
        target = TcpTarget(name="sandbox-proxy", host=sandbox.pod_ip, port=SANDBOX_PROXY_PORT)
        probes = self._control.probe_from_node_shell(
            sandbox.node_name, [target], timeout=PROBE_TIMEOUT_SECONDS
        )
        self._fixture.record("other_namespace_probes", _probe_evidence(probes))
        self._expect(
            probes,
            allowed=[],
            refused=[target],
            what="a pod in the control-plane namespace",
        )

    def expect_unselected_pod_isolated(self) -> None:
        """The default-deny: a pod in the namespace that no allow rule selects reaches nothing."""

        sandbox, dns = self._located()
        token = secrets.token_hex(4)
        # Armed before the create, so a create that times out after the write
        # still has its pod removed.
        self._scope.defer(
            f"remove the unselected pod {token}",
            lambda: self._control.remove_unselected_pods(token),
            critical=True,
        )
        stray = self._control.start_unselected_pod(sandbox.node_name, token)
        self._fixture.record("unselected_pod", _pod_evidence(stray))
        refused = [dns, *self._targets] + ([INTERNET_TARGET] if self._host_internet else [])
        probes = self._control.probe_from_pod_netns(
            stray, refused, timeout=PROBE_TIMEOUT_SECONDS
        )
        self._fixture.record("unselected_pod_probes", _probe_evidence(probes))
        self._expect(
            probes,
            allowed=[],
            refused=refused,
            what="a pod in the sandbox namespace that no allow rule selects",
        )

    def expect_guest_internet(self) -> None:
        """Inside the VM, the guest still reaches the internet, when this host does."""

        if self._host_internet is None:
            raise SmokeConfigurationError("run the control-namespace probes first")
        if not self._host_internet:
            self._fixture.record(
                "guest_internet",
                "not judged: this host does not reach the internet from the cluster",
            )
            return
        observed = self._fixture.wait_for_https_access(GUEST_INTERNET_URL)
        self._fixture.expect_https_reachable(observed)

    def _expect(
        self,
        probes: Sequence[TcpProbe],
        *,
        allowed: Sequence[TcpTarget],
        refused: Sequence[TcpTarget],
        what: str,
    ) -> None:
        by_target = {probe.target: probe for probe in probes}
        closed = [target.name for target in allowed if not by_target[target].connected]
        opened = [target.name for target in refused if by_target[target].connected]
        self._fixture.verify(
            not closed,
            f"{what} could not open a connection the policy allows, so its refusals "
            "cannot be told from a namespace that reaches nothing",
            allowed_but_refused=closed,
        )
        self._fixture.verify(
            not opened,
            f"{what} connected to a destination the sandbox namespace's NetworkPolicies refuse",
            connected_but_refused_by_policy=opened,
        )

    def _located(self) -> tuple[PodNetwork, TcpTarget]:
        if self._sandbox is None or self._dns is None:
            raise SmokeConfigurationError("locate() must run first")
        return self._sandbox, self._dns


def _pod_evidence(pod: PodNetwork | None) -> dict[str, str] | None:
    if pod is None:
        return None
    return {"pod": pod.pod_name, "node": pod.node_name, "pod_ip": pod.pod_ip}


def _target_evidence(target: TcpTarget) -> dict[str, object]:
    return {"name": target.name, "host": target.host, "port": target.port}


def _probe_evidence(probes: Sequence[TcpProbe]) -> list[dict[str, object]]:
    return [{**_target_evidence(probe.target), "result": probe.detail} for probe in probes]
