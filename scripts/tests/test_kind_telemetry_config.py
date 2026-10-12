"""Identity/launch boundaries shared by native and OTLP Kind experiments."""

import importlib.util
import json
import os
import pathlib
import re
import subprocess

import pytest
from kind_chart_render import (
    BUILD_MANAGER_CHART,
    KIND_DIR,
    TELEMETRY_CHARTS,
    bootstrap_guard_environment,
    render,
)

SCRIPT = KIND_DIR / "scripts/telemetry_config.py"
METRICS_ENDPOINT = "http://collector.space-metrics.svc.cluster.local:4317"
TRACES_ENDPOINT = "http://collector.space-traces.svc.cluster.local:4317"
SHIM_OVERRIDE = "http://192.0.2.10:4317"
LOCAL_ENDPOINT = "http://127.0.0.1:4317"
spec = importlib.util.spec_from_file_location("kind_telemetry_config", SCRIPT)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


@pytest.mark.parametrize("mode", ["datadog", "otel", "off"])
def test_chart_and_executed_shim_share_isolated_identity(mode, tmp_path):
    settings = config.configuration(
        {
            "SPACE_KIND_TELEMETRY_MODE": mode,
            "SPACE_KIND_ENV": "local-kind-owner-box",
            "SPACE_KIND_VERSION": "abc123",
            "SPACE_KIND_PHASE": "after-datadog",
            "SPACE_KIND_OWNER": "owner",
        }
    )
    wrapper = tmp_path / "shim"
    wrapper.write_text(config.shim_wrapper(settings, "/usr/bin/env", "old-binary"))
    result = subprocess.run(
        ["bash", str(wrapper)],
        env={"PATH": os.environ["PATH"], "DD_ENV": "prod", "DD_VERSION": "old"},
        check=True,
        capture_output=True,
        text=True,
    )
    launch = dict(row.split("=", 1) for row in result.stdout.splitlines())
    args = [
        item for setting in settings["helm_set"] for item in ("--set-string", setting)
    ]
    for chart in TELEMETRY_CHARTS:
        # Not "docs": oss/refs-to-private.sh reads that word on a path-joining
        # line as the private docs/ tree and fails the OSS export gate.
        manifests = render(chart, "space", *args)
        workload = next(
            doc for doc in manifests if doc["kind"] in ("Deployment", "DaemonSet")
        )
        env = {
            entry["name"]: entry.get("value")
            for entry in workload["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        for key in ("DD_ENV", "DD_VERSION"):
            assert env[key] == launch[key]
        # The workload and the shim agree on the SPACE identity only where the
        # shim uses it. Its DogStatsD version tag carries the shim's build
        # version rather than DD_VERSION, so a SPACE_VERSION in a Datadog-only
        # mode would change the tag; the chart's services have no such
        # difference and keep it in every mode.
        for key in ("SPACE_ENV", "SPACE_VERSION"):
            assert key in env
            if mode == "otel":
                assert env[key] == launch[key]
            else:
                assert key not in launch
        assert env["DD_ENV"].startswith("local-kind-")
        assert env["DD_VERSION"].endswith("-after-datadog")
        resources = dict(
            item.split("=", 1) for item in env["OTEL_RESOURCE_ATTRIBUTES"].split(",")
        )
        assert resources["deployment.environment.name"] == env["DD_ENV"]
        assert resources["service.version"] == env["DD_VERSION"]
        # The mode's whole purpose is selecting one sink; assert it, not just
        # the trace exporter, so inverting MODES cannot leave this suite green.
        assert (
            env["SPACE_METRICS_SINK"]
            == {
                "datadog": "datadog",
                "otel": "otlp",
                "off": "off",
            }[mode]
        )
        if mode == "otel":
            assert env["OTEL_TRACES_SAMPLER"] == "always_on"
            # sandbox-manager takes the node-reachable Collectors instead, for
            # the sidecars it launches on the node.
            if chart.name != "space-sandbox-manager-local":
                assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == METRICS_ENDPOINT
                assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == TRACES_ENDPOINT
            assert env["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] == "grpc"
            assert env["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] == "grpc"
        else:
            # An empty sampler is not a valid name; the charts omit it.
            assert "OTEL_TRACES_SAMPLER" not in env
            assert not any(key.startswith("OTEL_EXPORTER_OTLP_") for key in env)
        assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in env
        assert "OTEL_EXPORTER_OTLP_PROTOCOL" not in env
        # Cluster-named fields carry the cluster, not the environment.
        assert resources["k8s.cluster.name"] == "space-local"
        # The chart and the shim wrapper must agree on the tag set.
        assert env["DD_TAGS"] == launch["DD_TAGS"]
        if chart.name != "space-data-plane-local":
            assert env["SPACE_CLUSTER_ID"] == launch["SPACE_CLUSTER_ID"]
    # Each signal goes to its own Collector: traces for the shim's spans,
    # metrics for its OTLP sink.
    if mode == "off":
        assert not any(key.startswith("OTEL_EXPORTER_OTLP_") for key in launch)
    else:
        assert launch["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == (
            config.collector_endpoint(settings, "traces")
            if mode == "otel"
            else LOCAL_ENDPOINT
        )
    # The shim reads SPACE_METRICS_SINK as well; it must follow the mode, or an
    # otel run leaves the shim on native metrics while the workloads move.
    expected = {"datadog": "datadog", "otel": "otlp", "off": "off"}[mode]
    assert launch["SPACE_TRACE_EXPORTER"] == expected
    assert launch["SPACE_METRICS_SINK"] == expected
    # Shim metrics have no endpoint fallback, unlike traces.
    assert ("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" in launch) == (mode == "otel")
    if mode == "otel":
        assert launch["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == config.collector_endpoint(
            settings, "metrics"
        )
        assert launch["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] != launch[
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
        ]
        assert launch["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] == "grpc"

    launched_resources = dict(
        item.split("=", 1) for item in launch["OTEL_RESOURCE_ATTRIBUTES"].split(",")
    )
    # The resolver recomputes process-local attributes and drops any it
    # inherits here, so the node name has to arrive as its own setting. bash
    # expanded $(hostname) for both, so compare the values.
    assert launch["SPACE_K8S_NODE_NAME"] == launched_resources["k8s.node.name"]
    # Cluster metadata is not identity and survives in every mode, as do the
    # Datadog settings the shim has always read.
    assert "k8s.cluster.name" in launched_resources
    for key in ("DD_ENV", "DD_VERSION", "DD_SERVICE"):
        assert key in launch, f"{mode} must still set {key}"
    # Identity is gated on the mode, in the resource attributes as well as the
    # direct settings: the resolver reads service.version from here into the
    # same slot, and in a Datadog-only mode that would replace the shim's build
    # version in its DogStatsD version tag.
    identity_present = mode == "otel"
    assert ("OTEL_SERVICE_NAME" in launch) == identity_present
    for key in ("service.version", "deployment.environment.name"):
        assert (key in launched_resources) == identity_present, (
            f"{mode}: {key} in {launched_resources}"
        )


def test_build_manager_traces_stay_off_when_the_mode_selects_otlp():
    """Build failures can carry guest output, so build-manager exports no traces."""
    manifests = render(
        BUILD_MANAGER_CHART, "space-builder",
        "--set-string", "telemetry.metricsSink=otlp",
        "--set-string", "telemetry.traceExporter=otlp",
    )
    workload = next(doc for doc in manifests if doc["kind"] == "DaemonSet")
    env = {
        entry["name"]: entry.get("value")
        for entry in workload["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["SPACE_TRACE_EXPORTER"] == "off"
    assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" not in env
    assert env["SPACE_METRICS_SINK"] == "otlp"


@pytest.mark.parametrize("chart", TELEMETRY_CHARTS, ids=lambda chart: chart.name)
def test_chart_otlp_endpoints_can_be_overridden_independently(chart):
    custom_metrics = "http://metrics.example:4317"
    custom_traces = "http://traces.example:4317"
    manifests = render(
        chart, "space",
        "--set-string", "telemetry.metricsSink=otlp",
        "--set-string", "telemetry.traceExporter=otlp",
        "--set-string", f"telemetry.otlpMetricsEndpoint={custom_metrics}",
        "--set-string", f"telemetry.otlpTracesEndpoint={custom_traces}",
    )
    workload = next(doc for doc in manifests if doc["kind"] in ("Deployment", "DaemonSet"))
    env = {
        entry["name"]: entry.get("value")
        for entry in workload["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == custom_metrics
    assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == custom_traces


def test_shim_endpoint_override_replaces_the_collectors(tmp_path):
    env = bootstrap_guard_environment(tmp_path)
    env.update({
        "SPACE_KIND_TELEMETRY_MODE": "otel",
        "SPACE_KIND_SHIM_OTLP_ENDPOINT": SHIM_OVERRIDE,
    })
    wrapper = subprocess.run(
        ["python3", str(SCRIPT), "shim-wrapper", "--binary", "/usr/bin/env"],
        env=env, check=True, capture_output=True, text=True,
    )
    launch = subprocess.run(
        ["bash"], input=wrapper.stdout, env=env,
        check=True, capture_output=True, text=True,
    )
    values = dict(row.split("=", 1) for row in launch.stdout.splitlines())
    assert values["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == SHIM_OVERRIDE
    assert not (tmp_path / "unexpected-operation").exists()


# The shim wrapper included: it is installed before the Collectors exist and
# must not need them.
@pytest.mark.parametrize("command", ["json", "helm-set", "shim-wrapper"])
def test_otel_configuration_does_not_require_a_cluster(command, tmp_path):
    env = bootstrap_guard_environment(tmp_path)
    env["SPACE_KIND_TELEMETRY_MODE"] = "otel"
    result = subprocess.run(
        ["python3", str(SCRIPT), command],
        env=env, check=True, capture_output=True, text=True,
    )
    assert result.stdout
    assert not (tmp_path / "unexpected-operation").exists()


def test_watched_file_overrides_old_launch_environment(tmp_path):
    path = tmp_path / "identity.json"
    path.write_text(
        json.dumps(
            {"SPACE_KIND_ENV": "local-kind-new", "SPACE_KIND_PHASE": "before-main"}
        )
    )
    settings = config.configuration(
        config.options(
            {
                "SPACE_KIND_OBSERVABILITY_CONFIG": str(path),
                "SPACE_KIND_ENV": "old",
                "SPACE_KIND_PHASE": "after-otel",
            }
        )
    )
    assert settings["environment"] == "local-kind-new"
    assert settings["phase"] == "before-main"


def test_default_environment_is_per_devbox():
    """An ordinary bootstrap keeps the documented env:local-<host> identity."""
    settings = config.configuration({})
    assert settings["environment"] == config.default_environment()
    assert settings["environment"].startswith("local-")
    # The Datadog filter documented in datadog-agent-local/values.yaml.
    assert settings["environment"] != "kind-local"


def test_experiment_identity_overrides_the_devbox_default():
    assert config.configuration({"SPACE_KIND_ENV": "exp-a"})["environment"] == "exp-a"
    assert (
        config.configuration({"SPACE_KIND_SHIM_ENV": "legacy-b"})["environment"]
        == "legacy-b"
    )
    assert (
        config.configuration(
            {"SPACE_KIND_ENV": "wins", "SPACE_KIND_SHIM_ENV": "legacy-b"}
        )["environment"]
        == "wins"
    )


def test_default_environment_is_a_valid_telemetry_token():
    config.token(config.default_environment(), "SPACE_KIND_ENV")


@pytest.mark.parametrize(
    "key,value",
    [
        ("SPACE_KIND_ENV", "x,$(touch /tmp/not-allowed)"),
        ("SPACE_KIND_VERSION", "x\nexport DD_ENV=prod"),
        ("SPACE_KIND_TELEMETRY_MODE", "unknown"),
        ("SPACE_KIND_TELEMETRY_MODE", "otel-noncounters"),
        ("SPACE_KIND_TELEMETRY_MODE", "datadog+otlp"),
        ("SPACE_KIND_PHASE", "unbounded-sandbox-id"),
        ("SPACE_KIND_CLUSTER", "production"),
        ("SPACE_KIND_SHIM_OTLP_ENDPOINT", "https://shared.example"),
    ],
)
def test_invalid_configuration_rejected_before_bootstrap_mutates(key, value, tmp_path):
    env = {**bootstrap_guard_environment(tmp_path), key: value}
    result = subprocess.run(
        ["bash", str(KIND_DIR / "scripts/bootstrap.sh"), "--skip-images"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode != 0
    assert re.search(r"Kind (telemetry|Datadog) configuration:", result.stderr)
    # Non-vacuous anywhere: the guard must refuse before rendering any cluster
    # configuration under this HOME.
    assert not list(pathlib.Path(env["HOME"]).rglob("kind-cluster*.yaml"))
    assert not (tmp_path / "unexpected-operation").exists()


def test_configuration_file_rejects_credentials(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"DD_API_KEY": "not-a-real-key"}))
    with pytest.raises(ValueError, match="supported SPACE_KIND"):
        config.options({"SPACE_KIND_OBSERVABILITY_CONFIG": str(path)})


def test_default_bootstrap_ships_no_empty_tags():
    """An unconfigured bootstrap must not tag every pod `kind_owner:`.

    The shim wrapper appends owner and phase only when set, so the charts have
    to match or the two identities diverge exactly where nothing sets them.
    """
    settings = config.configuration({})
    args = [
        item for setting in settings["helm_set"] for item in ("--set-string", setting)
    ]
    for chart in ("space-api-local", "space-sandbox-manager-local"):
        workload = next(
            doc
            for doc in render(KIND_DIR / "charts" / chart, "space", *args)
            if doc["kind"] in ("Deployment", "DaemonSet")
        )
        env = {
            entry["name"]: entry.get("value")
            for entry in workload["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        assert "kind_owner:" not in env["DD_TAGS"], env["DD_TAGS"]
        assert "kind_phase:" not in env["DD_TAGS"], env["DD_TAGS"]
        assert "kind.owner=" not in env["OTEL_RESOURCE_ATTRIBUTES"]
        assert "kind.phase=" not in env["OTEL_RESOURCE_ATTRIBUTES"]


def test_empty_explicit_env_falls_back_to_the_legacy_variable():
    """An exported-but-empty SPACE_KIND_ENV must not skip the legacy name."""
    settings = config.configuration(
        {"SPACE_KIND_ENV": "", "SPACE_KIND_SHIM_ENV": "legacy-b"}
    )
    assert settings["environment"] == "legacy-b"


@pytest.mark.parametrize(
    "endpoint, rejected",
    [
        ("http://127.0.0.1:4317", False),
        ("http://[::1]:4317", False),
        ("http://collector", False),
        ("http://collector:4317/", False),
        ("https://collector:4317", True),
        ("http://:4317", True),
        ("http://collector:", True),
        ("http://collector:99999", True),
        ("http://collector:abc", True),
        ("http://collector:4317:1234", True),
        ("http://user:pw@collector:4317", True),
        ("http://collector:4317/v1/traces", True),
        ("http://collector:4317?", True),
        ("http://collector:4317#", True),
    ],
)
def test_shim_endpoint_override_follows_the_shared_rule(endpoint, rejected) -> None:
    """An override accepted here must not be refused by the shim it configures."""
    environment = {
        "SPACE_KIND_TELEMETRY_MODE": "datadog",
        "SPACE_KIND_SHIM_OTLP_ENDPOINT": endpoint,
    }
    if rejected:
        with pytest.raises(ValueError, match="SPACE_KIND_SHIM_OTLP_ENDPOINT"):
            config.configuration(environment)
    else:
        assert config.configuration(environment)["shim_endpoint"] == endpoint


def test_kind_defaults_to_otel():
    """An unconfigured bootstrap must land telemetry somewhere queryable.

    The previous default was `datadog`, while `SPACE_KIND_DATADOG_MODE`
    defaults to `off` -- so a default cluster emitted DogStatsD to an Agent
    that was never installed and the measurements went nowhere.
    """
    assert config.configuration({})["mode"] == "otel"
