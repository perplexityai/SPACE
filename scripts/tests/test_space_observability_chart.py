"""Render the space-observability umbrella chart in each mode.

No cluster is needed: `helm template` exercises the mode switch, the fail-closed
validation and the composed OTLP stack. Install-time behaviour (Secret lookups,
data arriving in the backends) is verified separately on a disposable cluster.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CHARTS = ROOT / "infra/space/observability/charts"
UMBRELLA = CHARTS / "space-observability"
NAMESPACE = "space-observability"


@pytest.fixture(scope="module", autouse=True)
def vendored_dependencies() -> None:
    """Vendor the library chart into each signal chart, then the umbrella.

    Helm does not resolve nested file:// dependencies, so each signal chart's
    own `common` dependency must be in its charts/ before it is packaged into
    the umbrella's.
    """
    for chart in ("metrics", "logs", "traces", "grafana", "space-observability"):
        result = subprocess.run(
            ["helm", "dependency", "build", str(CHARTS / chart)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.fail(f"helm dependency build {chart} failed:\n{result.stderr}")


def render(*args: str, chart: Path = UMBRELLA, namespace: str = NAMESPACE) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["helm", "template", "so", str(chart), "--namespace", namespace, *args],
        capture_output=True,
        text=True,
    )


def documents(*args: str, **kwargs) -> list[dict]:
    result = render(*args, **kwargs)
    assert result.returncode == 0, result.stderr
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def by_kind(docs: list[dict]) -> dict[tuple[str, str], dict]:
    return {(doc["kind"], doc["metadata"]["name"]): doc for doc in docs}


OTLP = ("--set", "global.observability.mode=otlp", "--set", "global.observability.cluster=test-cluster")
DATADOG = (
    "--set",
    "global.observability.mode=datadog",
    "--set",
    "global.observability.datadog.apiKeySecret.name=datadog-api-key",
)


def test_off_renders_nothing() -> None:
    assert documents() == []
    assert documents("--set", "global.observability.mode=off") == []


def test_unquoted_off_is_read_as_off(tmp_path: Path) -> None:
    values = tmp_path / "values.yaml"
    # YAML 1.1 reads this as boolean false.
    values.write_text("global:\n  observability:\n    mode: off\n")
    assert documents("-f", str(values)) == []


@pytest.mark.parametrize("mode", ["bogus", "Datadog", "otel", "true"])
def test_unknown_mode_fails(mode: str) -> None:
    result = render("--set", f"global.observability.mode={mode}")
    assert result.returncode != 0
    assert "global.observability.mode must be off, datadog or otlp" in result.stderr


def test_protocol_must_be_grpc() -> None:
    result = render(*OTLP, "--set", "global.observability.otlp.protocol=http/protobuf")
    assert result.returncode != 0
    assert "must be grpc" in result.stderr


def test_refuses_a_space_namespace() -> None:
    result = render(namespace="space")
    assert result.returncode != 0
    assert "own namespace" in result.stderr


def test_datadog_requires_an_api_key_secret() -> None:
    result = render("--set", "global.observability.mode=datadog")
    assert result.returncode != 0
    assert "apiKeySecret.name" in result.stderr


def test_datadog_renders_only_the_agent() -> None:
    docs = by_kind(documents(*DATADOG, "--set", "global.observability.cluster=test-cluster"))
    assert set(kind for kind, _ in docs) == {
        "ServiceAccount",
        "ClusterRole",
        "ClusterRoleBinding",
        "DaemonSet",
        "NetworkPolicy",
    }
    container = docs[("DaemonSet", "datadog-agent")]["spec"]["template"]["spec"]["containers"][0]
    ports = {(p["name"], p["protocol"]): p.get("hostPort") for p in container["ports"]}
    assert ports == {("dogstatsd", "UDP"): 8125, ("apm", "TCP"): 8126}
    env = {item["name"]: item for item in container["env"]}
    assert env["DD_API_KEY"]["valueFrom"]["secretKeyRef"] == {"name": "datadog-api-key", "key": "api-key"}
    assert env["DD_LOGS_ENABLED"]["value"] == "true"
    assert env["DD_CONTAINER_INCLUDE_LOGS"]["value"] == "kube_namespace:^space$"
    assert env["DD_CONTAINER_EXCLUDE_LOGS"]["value"] == "kube_namespace:.*"
    assert env["DD_CLUSTER_NAME"]["value"] == "test-cluster"
    policy = docs[("NetworkPolicy", "datadog-agent")]["spec"]
    assert policy["policyTypes"] == ["Ingress"]
    (rule,) = policy["ingress"]
    assert rule["ports"] == [{"protocol": "UDP", "port": 8125}, {"protocol": "TCP", "port": 8126}]
    assert rule["from"][0]["namespaceSelector"]["matchExpressions"][0]["values"] == ["space"]
    # Producers on tainted worker and builder nodes need the Agent there too.
    assert docs[("DaemonSet", "datadog-agent")]["spec"]["template"]["spec"]["tolerations"] == [
        {"operator": "Exists"}
    ]


def test_datadog_ports_follow_the_shared_values() -> None:
    docs = by_kind(
        documents(
            *DATADOG,
            "--set",
            "global.observability.datadog.dogstatsdPort=18125",
            "--set",
            "global.observability.datadog.apmPort=18126",
        )
    )
    container = docs[("DaemonSet", "datadog-agent")]["spec"]["template"]["spec"]["containers"][0]
    assert sorted(p["hostPort"] for p in container["ports"]) == [18125, 18126]


def test_otlp_requires_a_cluster_name() -> None:
    result = render("--set", "global.observability.mode=otlp")
    assert result.returncode != 0
    assert "global.observability.cluster" in result.stderr


def test_otlp_composes_one_gateway_and_every_backend() -> None:
    docs = by_kind(documents(*OTLP))
    deployments = {name for kind, name in docs if kind == "Deployment"}
    # The signal charts' own Collectors are replaced by the single gateway.
    assert deployments == {"collector", "prometheus", "tempo", "loki", "grafana"}
    assert ("DaemonSet", "alloy") in docs
    assert ("DaemonSet", "datadog-agent") not in docs
    gateway = docs[("Deployment", "collector")]
    assert gateway["spec"]["replicas"] == 1
    assert gateway["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/component"] == "gateway"
    config = yaml.safe_load(docs[("ConfigMap", "gateway-config")]["data"]["collector.yaml"])
    assert set(config["service"]["pipelines"]) == {"metrics", "traces", "logs"}
    assert "delta_to_cumulative" in config["service"]["pipelines"]["metrics"]["processors"]


def test_otlp_storage_defaults_to_the_default_storage_class() -> None:
    docs = by_kind(documents(*OTLP))
    claims = {name: doc for (kind, name), doc in docs.items() if kind == "PersistentVolumeClaim"}
    assert set(claims) == {"prometheus-data", "tempo-data", "loki-data"}
    for claim in claims.values():
        assert "storageClassName" not in claim["spec"]


def test_otlp_ephemeral_storage_uses_empty_dir() -> None:
    args = []
    for signal, backend, budget in (("metrics", "prometheus", "3Gi"), ("traces", "tempo", "5Gi"), ("logs", "loki", "3Gi")):
        args += [
            "--set", f"{signal}.storage.ephemeral=true",
            "--set", f"{signal}.storage.useDefaultStorageClass=false",
            "--set", f"{signal}.{backend}.resources.limits.ephemeral-storage={budget}",
        ]
    docs = by_kind(documents(*OTLP, *args))
    assert not [name for kind, name in docs if kind == "PersistentVolumeClaim"]
    for name in ("prometheus", "tempo", "loki"):
        volumes = {v["name"]: v for v in docs[("Deployment", name)]["spec"]["template"]["spec"]["volumes"]}
        assert "emptyDir" in volumes["data"]


def test_otlp_generates_grafana_credentials_and_wires_datasources() -> None:
    docs = by_kind(documents(*OTLP))
    secret = docs[("Secret", "space-observability-grafana-admin")]
    assert set(secret["data"]) == {"admin-user", "admin-password"}
    datasources = yaml.safe_load(docs[("ConfigMap", "grafana-config")]["data"]["datasources.yaml"])
    urls = {source["uid"]: source["url"] for source in datasources["datasources"]}
    assert urls == {
        "space-prometheus": "http://prometheus:9090",
        "space-loki": "http://loki:3100",
        "space-tempo": "http://tempo:3200",
    }


def test_otlp_existing_grafana_secret_is_not_generated() -> None:
    docs = by_kind(documents(*OTLP, "--set", "grafana.auth.existingSecret=my-grafana"))
    assert not [name for kind, name in docs if kind == "Secret"]


def test_otlp_gateway_accepts_only_space_namespaces() -> None:
    docs = by_kind(documents(*OTLP, "--set", "spaceNamespaces={space,space-builder}",
                             "--set", "logs.collection.podTargets[0].path=/var/log/pods/space_*/*/*.log",
                             "--set", "logs.collection.podTargets[0].namespace=space",
                             "--set", "logs.collection.podTargets[1].path=/var/log/pods/space-builder_*/*/*.log",
                             "--set", "logs.collection.podTargets[1].namespace=space-builder"))
    policy = docs[("NetworkPolicy", "gateway-collector")]["spec"]
    receiver = next(rule for rule in policy["ingress"] if rule["ports"][0]["port"] == 4317)
    assert receiver["from"] == [
        {
            "namespaceSelector": {
                "matchExpressions": [
                    {"key": "kubernetes.io/metadata.name", "operator": "In", "values": ["space", "space-builder"]}
                ]
            }
        }
    ]
    assert {name for kind, name in docs if kind == "NetworkPolicy"} == {
        "gateway-collector",
        "metrics-prometheus",
        "traces-tempo",
        "loki",
        "alloy",
        "grafana",
    }


def test_otlp_rejects_uncollected_space_namespace() -> None:
    result = render(*OTLP, "--set", "spaceNamespaces={space,space-builder}")
    assert result.returncode != 0
    assert "logs.collection.podTargets does not collect it" in result.stderr


def test_otlp_rejects_datasource_without_backend() -> None:
    result = render(*OTLP, "--set", "traces.enabled=false")
    assert result.returncode != 0
    assert "grafana.datasources.tempo.enabled requires traces.enabled" in result.stderr


def test_otlp_backend_can_be_dropped() -> None:
    docs = by_kind(documents(*OTLP, "--set", "traces.enabled=false", "--set", "grafana.datasources.tempo.enabled=false"))
    assert ("Deployment", "tempo") not in docs
    config = yaml.safe_load(docs[("ConfigMap", "gateway-config")]["data"]["collector.yaml"])
    assert set(config["service"]["pipelines"]) == {"metrics", "logs"}


@pytest.mark.parametrize("chart", ["metrics", "traces"])
def test_signal_charts_keep_their_collector_standalone(chart: str) -> None:
    docs = by_kind(
        documents("--set", "storage.storageClassName=standard", chart=CHARTS / chart, namespace=f"obs-{chart}")
    )
    assert ("Deployment", "collector") in docs


@pytest.mark.parametrize("mode", ["off", "datadog"])
def test_signal_charts_render_nothing_outside_otlp_mode(mode: str) -> None:
    docs = documents(
        "--set", f"global.observability.mode={mode}", chart=CHARTS / "metrics", namespace="obs-metrics"
    )
    assert docs == []


KUBE_DNS_EGRESS = {
    "to": [
        {
            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
            "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
        }
    ],
    "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}],
}


def dns_rules(docs: list[dict]) -> dict[str, list[dict]]:
    """Each NetworkPolicy with egress, mapped to its DNS (port 53/5353) rules."""
    out = {}
    for doc in docs:
        if doc["kind"] != "NetworkPolicy" or "Egress" not in doc["spec"].get("policyTypes", []):
            continue
        out[doc["metadata"]["name"]] = [
            rule for rule in doc["spec"].get("egress") or []
            if any(port.get("port") in (53, 5353) for port in rule.get("ports") or [])
        ]
    return out


def test_dns_egress_defaults_to_kube_dns() -> None:
    rules = dns_rules(documents(*OTLP))
    assert rules, "no egress NetworkPolicies rendered"
    for name, found in rules.items():
        assert found == [KUBE_DNS_EGRESS], name


def test_dns_egress_peers_and_ports_are_configurable(tmp_path: Path) -> None:
    # OpenShift: the DNS operator's pods, on 5353 behind the Service's 53.
    custom = {
        "to": [
            {
                "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "openshift-dns"}},
                "podSelector": {"matchLabels": {"dns.operator.openshift.io/daemonset-dns": "default"}},
            },
            {"ipBlock": {"cidr": "169.254.20.10/32"}},
        ],
        "ports": [{"protocol": "UDP", "port": 5353}, {"protocol": "TCP", "port": 5353}],
    }
    values = tmp_path / "dns.yaml"
    values.write_text(yaml.safe_dump({"global": {"networkPolicy": {"dns": {"peers": custom["to"], "ports": custom["ports"]}}}}))
    rules = dns_rules(documents(*OTLP, "-f", str(values)))
    assert rules
    for name, found in rules.items():
        assert found == [custom], name
    # A standalone signal chart reads the same global key.
    standalone = dns_rules(documents("-f", str(CHARTS / "traces/profiles/shared.example.yaml"),
                                     "--set", "storage.ephemeral=true",
                                     "--set", "tempo.resources.limits.ephemeral-storage=5Gi", "-f", str(values),
                                     chart=CHARTS / "traces"))
    assert standalone and all(found == [custom] for found in standalone.values())


def test_dns_egress_empty_lists_keep_the_default(tmp_path: Path) -> None:
    # An empty `to` would allow egress anywhere; empty lists fall back instead.
    values = tmp_path / "dns.yaml"
    values.write_text("global: {networkPolicy: {dns: {peers: [], ports: []}}}\n")
    for name, found in dns_rules(documents(*OTLP, "-f", str(values))).items():
        assert found == [KUBE_DNS_EGRESS], name


def test_dns_egress_rejects_a_non_list() -> None:
    result = render(*OTLP, "--set", "global.networkPolicy.dns.peers=kube-dns")
    assert result.returncode != 0
    assert "global.networkPolicy.dns.peers must be a list" in result.stderr
