#!/usr/bin/env python3
"""Prove a sandbox cannot reach any telemetry receiver, including node host ports.

Creates a disposable two-node Kind cluster, installs space-observability twice
(otlp mode with the node-local Collector, and datadog mode with a dummy key and
an unroutable site), then probes every Collector and Agent port from four pods
on the worker node:

- a pod in the SPACE namespace (must reach its node's receivers),
- a root host-network pod (must reach its node's receivers, as node-manager
  and the worker shim do),
- a non-root host-network pod (as node-api runs; see the host-peers phase),
  and
- a stand-in sandbox pod in the sandbox namespace, labelled like a SPACE
  sandbox (must reach nothing).

A SPACE guest's traffic leaves its microVM through its sandbox pod's network
namespace, so the stand-in pod is the guest's network position. It is not a
Firecracker guest: the per-sandbox egress gateway inside that namespace is an
additional layer this test does not exercise.

Targets are every node address at the node-local OTLP port and the Agent's APM
(TCP) and DogStatsD (UDP) host ports, the gateway Service, and every Collector
and Agent pod address. TCP is a connect probe. UDP has no handshake, so each
datagram carries a unique metric name and the Agent's DogStatsD stats show
whether it arrived.

Phases:

1. receiver-policies: the observability charts' NetworkPolicies alone, with
   default values.
2. with-sandbox-policy: plus the sandbox namespace's own NetworkPolicy, when
   the Kind chart that defines it is present.
3. host-peers: plus every node address in nodeLocal.hostPeers and
   datadog.hostPeers. Some CNIs (kindnet among them) exempt only root host
   processes from NetworkPolicy, so a non-root host-network producer needs
   its node listed; the sandbox must still reach nothing.

Requires kind, kubectl, helm and docker. Never touches an existing cluster or
kubeconfig: everything uses a temporary kubeconfig for a uniquely named cluster,
deleted on exit unless --keep is given.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
CHARTS = ROOT / "infra/space/observability/charts"
UMBRELLA = CHARTS / "space-observability"
SANDBOX_POLICY_CHART = ROOT / "infra/space/kind/charts/space-sandbox-network-policy"

OTLP_NS = "space-observability"
DATADOG_NS = "space-datadog"
SPACE_NS = "space"
SANDBOX_NS = "sandboxes"
NODE_LOCAL_PORT = 4317
APM_PORT = 8126
DOGSTATSD_PORT = 8125
PROBE_IMAGE = "busybox:1.36"

KIND_CONFIG = """\
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
"""


class Cluster:
    def __init__(self, name: str, kubeconfig: Path) -> None:
        self.name = name
        self.kubeconfig = kubeconfig
        self.context = f"kind-{name}"

    def kubectl(self, *args: str, check: bool = True, timeout: int = 300) -> subprocess.CompletedProcess:
        return run(
            ["kubectl", "--kubeconfig", str(self.kubeconfig), "--context", self.context, *args],
            check=check,
            timeout=timeout,
        )

    def helm(self, *args: str) -> subprocess.CompletedProcess:
        return run(["helm", "--kubeconfig", str(self.kubeconfig), "--kube-context", self.context, *args])

    def json(self, *args: str) -> dict:
        return json.loads(self.kubectl(*args, "-o", "json").stdout)


def run(
    command: list[str], check: bool = True, timeout: int = 900, stdin: str | None = None
) -> subprocess.CompletedProcess:
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, input=stdin, check=False)
    if check and result.returncode != 0:
        raise SystemExit(f"command failed ({result.returncode}): {' '.join(command)}\n{result.stderr.strip()}")
    return result


def log(message: str) -> None:
    print(f"[collector-isolation] {message}", flush=True)


@dataclass(frozen=True)
class Target:
    label: str
    host: str
    port: int
    protocol: str  # tcp | udp
    owner_node: str | None  # node whose receiver this is, when node-scoped
    agent_pod: str | None = None  # the Agent pod that counts a UDP datagram


@dataclass(frozen=True)
class Source:
    label: str
    namespace: str
    pod: str
    may_reach: bool | None  # True: must reach its node's receivers; False: nothing; None: per phase


def install(cluster: Cluster) -> None:
    for chart in ("metrics", "logs", "traces", "grafana", "space-observability"):
        run(["helm", "dependency", "build", str(CHARTS / chart)])
    for namespace in (OTLP_NS, DATADOG_NS, SPACE_NS, SANDBOX_NS):
        cluster.kubectl("create", "namespace", namespace)
    common = [
        "--set",
        "global.observability.cluster=isolation-test",
        "--set",
        "global.observability.environment=isolation-test",
        "--wait",
        "--timeout",
        "10m",
    ]
    log("installing otlp mode (gateway, node-local Collector, traces backend)")
    cluster.helm(
        "upgrade",
        "--install",
        "obs",
        str(UMBRELLA),
        "--namespace",
        OTLP_NS,
        "--set",
        "global.observability.mode=otlp",
        "--set",
        f"global.observability.otlp.nodeLocalPort={NODE_LOCAL_PORT}",
        # Only the receivers matter here; one backend keeps the gateway valid.
        "--set",
        "metrics.enabled=false",
        "--set",
        "logs.enabled=false",
        "--set",
        "grafana.enabled=false",
        "--set",
        "traces.storage.useDefaultStorageClass=false",
        "--set",
        "traces.storage.ephemeral=true",
        *common,
    )
    log("installing datadog mode (dummy key, unroutable site: nothing leaves the cluster)")
    cluster.kubectl(
        "-n",
        DATADOG_NS,
        "create",
        "secret",
        "generic",
        "datadog-api-key",
        f"--from-literal=api-key={'0' * 32}",
    )
    cluster.helm(
        "upgrade",
        "--install",
        "dd",
        str(UMBRELLA),
        "--namespace",
        DATADOG_NS,
        "--set",
        "global.observability.mode=datadog",
        "--set",
        "global.observability.datadog.apiKeySecret.name=datadog-api-key",
        "--set",
        "datadog.site=local.invalid",
        "--set",
        "datadog.kubeletTLSVerify=false",
        *common,
    )
    cluster.kubectl("-n", OTLP_NS, "rollout", "status", "daemonset/node-collector", "--timeout=5m")
    cluster.kubectl("-n", DATADOG_NS, "rollout", "status", "daemonset/datadog-agent", "--timeout=5m")


def start_probes(cluster: Cluster, worker: str) -> list[Source]:
    sources = [
        Source("space pod", SPACE_NS, "producer", True),
        Source("host-network pod", SPACE_NS, "host-producer", True),
        Source("non-root host pod", SPACE_NS, "host-nonroot", None),
        Source("sandbox pod", SANDBOX_NS, "guest", False),
    ]
    for source in sources:
        spec: dict = {"nodeName": worker, "terminationGracePeriodSeconds": 0}
        if source.pod.startswith("host-"):
            spec["hostNetwork"] = True
        if source.pod == "host-nonroot":
            spec["securityContext"] = {"runAsUser": 65532, "runAsGroup": 65532}
        labels = "app.kubernetes.io/name=space-sandbox" if source.namespace == SANDBOX_NS else "role=probe"
        cluster.kubectl(
            "-n",
            source.namespace,
            "run",
            source.pod,
            f"--image={PROBE_IMAGE}",
            "--restart=Never",
            f"--labels={labels}",
            f"--overrides={json.dumps({'spec': spec})}",
            "--",
            "sleep",
            "86400",
        )
    for source in sources:
        cluster.kubectl("-n", source.namespace, "wait", "--for=condition=Ready", f"pod/{source.pod}", "--timeout=3m")
    return sources


def targets(cluster: Cluster) -> list[Target]:
    nodes = cluster.json("get", "nodes")["items"]
    found: list[Target] = []
    agents = {
        pod["spec"]["nodeName"]: pod
        for pod in cluster.json("-n", DATADOG_NS, "get", "pods", "-l", "app=datadog-agent")["items"]
    }
    for node in nodes:
        name = node["metadata"]["name"]
        for address in node["status"]["addresses"]:
            if address["type"] != "InternalIP":
                continue
            ip = address["address"]
            found.append(Target(f"{name} node-local OTLP", ip, NODE_LOCAL_PORT, "tcp", name))
            if name in agents:
                agent = agents[name]["metadata"]["name"]
                found.append(Target(f"{name} Agent APM", ip, APM_PORT, "tcp", name, agent))
                found.append(Target(f"{name} Agent DogStatsD", ip, DOGSTATSD_PORT, "udp", name, agent))
    gateway = cluster.json("-n", OTLP_NS, "get", "service", "collector")["spec"]["clusterIP"]
    found.append(Target("gateway Service", gateway, 4317, "tcp", None))
    for selector, port in (("app=collector", 4317), ("app=node-collector", 4317)):
        for pod in cluster.json("-n", OTLP_NS, "get", "pods", "-l", selector)["items"]:
            found.append(Target(f"{pod['metadata']['name']} pod", pod["status"]["podIP"], port, "tcp", None))
    for node, pod in agents.items():
        name = pod["metadata"]["name"]
        found.append(Target(f"{name} pod APM", pod["status"]["podIP"], APM_PORT, "tcp", None, name))
        found.append(Target(f"{name} pod DogStatsD", pod["status"]["podIP"], DOGSTATSD_PORT, "udp", None, name))
    return found


def tcp_open(cluster: Cluster, source: Source, target: Target) -> bool:
    result = cluster.kubectl(
        "-n",
        source.namespace,
        "exec",
        source.pod,
        "--",
        "nc",
        "-z",
        "-w",
        "3",
        target.host,
        str(target.port),
        check=False,
        timeout=30,
    )
    return result.returncode == 0


def udp_send(cluster: Cluster, source: Source, target: Target, metric: str) -> None:
    script = f"for i in 1 2 3; do echo '{metric}:1|c' | nc -u -w1 {target.host} {target.port}; done"
    cluster.kubectl("-n", source.namespace, "exec", source.pod, "--", "sh", "-c", script, check=False, timeout=30)


def dogstatsd_names(cluster: Cluster, agent: str) -> set[str]:
    output = cluster.kubectl("-n", DATADOG_NS, "exec", agent, "--", "agent", "dogstatsd-stats", "--json").stdout
    stats = json.loads(output[output.index("{") :])
    return {entry["name"] for entry in stats.values()}


def expected(phase: str, source: Source, target: Target, worker: str) -> bool | None:
    """True must reach, False must not, None not asserted."""
    if source.may_reach is False:
        return False
    if source.may_reach is None and phase != "host-peers":
        return None
    if target.owner_node == worker:
        return True
    # Controls reaching other nodes or pod addresses depend on the CNI and are
    # not what this test proves.
    return None


def probe(cluster: Cluster, phase: str, sources: list[Source], found: list[Target], worker: str) -> list[str]:
    agents = {t.agent_pod for t in found if t.agent_pod}
    for agent in agents:
        cluster.kubectl("-n", DATADOG_NS, "exec", agent, "--", "agent", "config", "set", "dogstatsd_stats", "true")
    results: list[tuple[Source, Target, bool | str]] = []
    pending_udp: list[tuple[Source, Target, str]] = []
    for source in sources:
        for target in found:
            if target.protocol == "tcp":
                results.append((source, target, tcp_open(cluster, source, target)))
            else:
                metric = f"isolation.{phase}.{source.pod}.{secrets.token_hex(4)}"
                udp_send(cluster, source, target, metric)
                pending_udp.append((source, target, metric))
    time.sleep(3)
    seen: dict[str, set[str]] = {agent: dogstatsd_names(cluster, agent) for agent in agents}
    for source, target, metric in pending_udp:
        results.append((source, target, metric in seen[target.agent_pod]))

    failures: list[str] = []
    print(f"\n== {phase}")
    print(f"{'source':<18} {'target':<46} {'address':<22} {'result':<8} expectation")
    for source, target, reached in results:
        want = expected(phase, source, target, worker)
        verdict = "reached" if reached else "blocked"
        expectation = {True: "must reach", False: "must be blocked", None: "not asserted"}[want]
        status = "ok" if want is None or want == reached else "FAIL"
        address = f"{target.host}:{target.port}/{target.protocol}"
        print(f"{source.label:<18} {target.label:<46} {address:<22} {verdict:<8} {expectation} [{status}]")
        if status == "FAIL":
            failures.append(f"{phase}: {source.label} -> {target.label} {address} {verdict}, {expectation}")
    return failures


def apply_sandbox_policy(cluster: Cluster) -> bool:
    if not SANDBOX_POLICY_CHART.is_dir():
        log(f"{SANDBOX_POLICY_CHART} not found; skipping the sandbox-policy phase")
        return False
    rendered = run(
        [
            "helm",
            "template",
            "space",
            str(SANDBOX_POLICY_CHART),
            "--namespace",
            SPACE_NS,
            "--set",
            f"sandboxNamespace={SANDBOX_NS}",
        ]
    ).stdout
    run(
        ["kubectl", "--kubeconfig", str(cluster.kubeconfig), "--context", cluster.context, "apply", "-f", "-"],
        stdin=rendered,
    )
    return True


def set_host_peers(cluster: Cluster) -> None:
    peers = json.dumps(
        [
            {"ipBlock": {"cidr": f"{address['address']}/32"}}
            for node in cluster.json("get", "nodes")["items"]
            for address in node["status"]["addresses"]
            if address["type"] == "InternalIP"
        ]
    )
    log(f"admitting node addresses as host peers: {peers}")
    for release, namespace, key in (("obs", OTLP_NS, "nodeLocal.hostPeers"), ("dd", DATADOG_NS, "datadog.hostPeers")):
        cluster.helm(
            "upgrade",
            release,
            str(UMBRELLA),
            "--namespace",
            namespace,
            "--reuse-values",
            "--set-json",
            f"{key}={peers}",
            "--wait",
            "--timeout",
            "5m",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cluster-name", default=f"space-obs-isolation-{secrets.token_hex(3)}")
    parser.add_argument("--keep", action="store_true", help="leave the cluster running for inspection")
    args = parser.parse_args()
    for tool in ("kind", "kubectl", "helm", "docker"):
        if not shutil.which(tool):
            raise SystemExit(f"{tool} is required")
    existing = run(["kind", "get", "clusters"]).stdout.split()
    if args.cluster_name in existing:
        raise SystemExit(f"Kind cluster {args.cluster_name} already exists; refusing to reuse it")

    workdir = Path(tempfile.mkdtemp(prefix="collector-isolation-"))
    kubeconfig = workdir / "kubeconfig"
    (workdir / "kind.yaml").write_text(KIND_CONFIG)
    cluster = Cluster(args.cluster_name, kubeconfig)
    log(f"creating Kind cluster {args.cluster_name} (kubeconfig {kubeconfig})")
    run(
        [
            "kind",
            "create",
            "cluster",
            "--name",
            args.cluster_name,
            "--config",
            str(workdir / "kind.yaml"),
            "--kubeconfig",
            str(kubeconfig),
            "--wait",
            "3m",
        ]
    )
    try:
        install(cluster)
        worker = next(
            node["metadata"]["name"]
            for node in cluster.json("get", "nodes")["items"]
            if "node-role.kubernetes.io/control-plane" not in node["metadata"].get("labels", {})
        )
        sources = start_probes(cluster, worker)
        found = targets(cluster)
        failures = probe(cluster, "receiver-policies", sources, found, worker)
        if apply_sandbox_policy(cluster):
            failures += probe(cluster, "with-sandbox-policy", sources, found, worker)
            set_host_peers(cluster)
            failures += probe(cluster, "host-peers", sources, found, worker)
        print()
        if failures:
            for failure in failures:
                print(f"FAIL {failure}")
            return 1
        print(
            "PASS: the sandbox pod reached no Collector or Agent port in any phase; SPACE and host-network pods reached their node's receivers"
        )
        return 0
    finally:
        if args.keep:
            log(f"kept: KUBECONFIG={kubeconfig} kubectl --context {cluster.context} get pods -A")
        else:
            log(f"deleting Kind cluster {args.cluster_name}")
            run(
                ["kind", "delete", "cluster", "--name", args.cluster_name, "--kubeconfig", str(kubeconfig)], check=False
            )
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    os.chdir(ROOT)
    sys.exit(main())
