"""SPACE node services and the space-observability chart agree on the node-local Collector.

One values file drives both charts. In otlp mode every telemetry producer the
space-worker chart renders (pod-network and host-network DaemonSets, the
runtime-supervisor environment file node-manager writes, and the containerd
shim wrapper) must resolve to http://<node IP>:<nodeLocalPort>, the port
space-observability publishes the node-local Collector on, and must be one the
Collector's NetworkPolicy can admit.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SPACE_CHARTS = ROOT / "infra/space/charts"
WORKER = SPACE_CHARTS / "space-worker"
EXAMPLE = SPACE_CHARTS / "examples/values-example.yaml"
OBS_CHARTS = ROOT / "infra/space/observability/charts"
UMBRELLA = OBS_CHARTS / "space-observability"
UMBRELLA_README = UMBRELLA / "README.md"
HOST_ENV = SPACE_CHARTS / "tests/assert_host_env.py"

PORT = 14317
SPACE_NS = "space"
SETTINGS = [
    "--set",
    "global.observability.mode=otlp",
    "--set",
    f"global.observability.otlp.nodeLocalPort={PORT}",
    "--set",
    "global.observability.cluster=wiring-test",
]

pytestmark = pytest.mark.skipif(not WORKER.is_dir(), reason="needs the SPACE node charts")


def helm(*args: str) -> str:
    result = subprocess.run(["helm", *args], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    return result.stdout


@pytest.fixture(scope="module")
def host_env():
    spec = importlib.util.spec_from_file_location("assert_host_env", HOST_ENV)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def worker_docs() -> list[dict]:
    helm("dependency", "build", str(WORKER))
    out = helm("template", "space-worker", str(WORKER), "--namespace", SPACE_NS, "-f", str(EXAMPLE), *SETTINGS)
    return [d for d in yaml.safe_load_all(out) if d]


@pytest.fixture(scope="module")
def observability_docs() -> list[dict]:
    for chart in ("metrics", "logs", "traces", "grafana", "space-observability"):
        helm("dependency", "build", str(OBS_CHARTS / chart))
    out = helm("template", "obs", str(UMBRELLA), "--namespace", "space-observability", "-f", str(EXAMPLE), *SETTINGS)
    return [d for d in yaml.safe_load_all(out) if d]


def producers(docs: list[dict]):
    """(workload, pod spec, container) for every container carrying telemetry settings."""
    for doc in docs:
        if doc["kind"] not in ("DaemonSet", "Deployment", "StatefulSet", "Job"):
            continue
        spec = doc["spec"]["template"]["spec"]
        for container in [*(spec.get("initContainers") or []), *spec["containers"]]:
            if any(e["name"] == "SPACE_METRICS_SINK" for e in container.get("env") or []):
                yield doc, spec, container


def runs_as_root(spec: dict, container: dict) -> bool:
    uid = (container.get("securityContext") or {}).get("runAsUser")
    if uid is None:
        uid = (spec.get("securityContext") or {}).get("runAsUser")
    return uid in (None, 0)


def test_every_node_producer_sends_to_the_node_local_port(worker_docs, host_env) -> None:
    found = list(producers(worker_docs))
    names = {doc["metadata"]["name"] for doc, _, _ in found}
    # Every node service, plus the shim installer, which writes the wrapper.
    assert {
        "space-node-api",
        "space-node-ctl",
        "space-node-manager",
        "space-sandbox-manager",
        "space-template-manager",
        "space-volume-manager",
        "space-runtime-manager",
        "space-shim-installer",
    } <= names
    want = f"http://{host_env.NODE_IP}:{PORT}"
    for doc, _, container in found:
        env = host_env.resolve(container["env"])
        where = f"{doc['metadata']['name']}/{container['name']}"
        assert env["SPACE_METRICS_SINK"] == "otlp", where
        assert env["SPACE_TRACE_EXPORTER"] == "otlp", where
        assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == want, where
        assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == want, where


@pytest.mark.parametrize("writer", ["node-env", "shim-install"])
def test_host_processes_get_the_node_local_port(worker_docs, host_env, writer: str) -> None:
    """Run the rendered writer and read back what a host process would get."""
    (container,) = [c for _, _, c in producers(worker_docs) if c["name"] == writer]
    env = host_env.resolve(container["env"])
    with tempfile.TemporaryDirectory() as scratch:
        for sub in ("config", "run", "shims", "bin", "stage"):
            os.makedirs(os.path.join(scratch, sub))
        Path(scratch, "stage", "containerd-shim-space-v2").write_text("#!/bin/sh\n")
        paths = {
            "/space/config": os.path.join(scratch, "config"),
            "/space/run": os.path.join(scratch, "run"),
            "/host-shim-root": os.path.join(scratch, "shims"),
            "/host-bin": os.path.join(scratch, "bin"),
            "/stage": os.path.join(scratch, "stage"),
        }
        host_env.run(container["command"][-1], env, paths, scratch)
        written = host_env.written_env(writer, scratch)
    want = f"http://{host_env.NODE_IP}:{PORT}"
    assert written["SPACE_METRICS_SINK"] == "otlp"
    assert written["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == want
    assert written["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == want


def test_collector_listens_where_producers_send(observability_docs) -> None:
    (daemonset,) = [
        d for d in observability_docs if d["kind"] == "DaemonSet" and d["metadata"]["name"] == "node-collector"
    ]
    (container,) = daemonset["spec"]["template"]["spec"]["containers"]
    assert [p["hostPort"] for p in container["ports"]] == [PORT]


def test_pod_network_producers_are_in_an_admitted_namespace(worker_docs, observability_docs) -> None:
    (policy,) = [
        d for d in observability_docs if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == "node-collector"
    ]
    (rule,) = policy["spec"]["ingress"]
    admitted = rule["from"][0]["namespaceSelector"]["matchExpressions"][0]["values"]
    for doc, spec, _ in producers(worker_docs):
        if not spec.get("hostNetwork"):
            assert doc["metadata"].get("namespace", SPACE_NS) in admitted, doc["metadata"]["name"]


def test_non_root_host_network_producers_are_documented(worker_docs) -> None:
    """These reach the Collector from the node's address as non-root processes,
    which some CNIs refuse without nodeLocal.hostPeers. The README names them so
    operators know which services the setting is for."""
    non_root = sorted(
        {
            doc["metadata"]["name"]
            for doc, spec, container in producers(worker_docs)
            if spec.get("hostNetwork") and not runs_as_root(spec, container)
        }
    )
    assert non_root, "expected non-root host-network producers"
    readme = UMBRELLA_README.read_text()
    for name in non_root:
        assert f"`{name}`" in readme, f"{name} is not named in {UMBRELLA_README.relative_to(ROOT)}"
