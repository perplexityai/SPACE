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
SHIM_OVERRIDE = "http://192.0.2.10:4317"
LOCAL_ENDPOINT = "http://127.0.0.1:4317"
spec = importlib.util.spec_from_file_location("kind_telemetry_config", SCRIPT)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


@pytest.mark.parametrize("mode", ["datadog", "otel", "off"])
@pytest.mark.parametrize("global_overlay", [None, "null"], ids=["absent", "null"])
def test_chart_and_executed_shim_share_isolated_identity(mode, global_overlay, tmp_path):
    # The node shim and pod workloads have separate launch paths. A profile
    # must not leave either on inherited host identity or a different sink.
    settings = config.configuration({
        "SPACE_KIND_TELEMETRY_MODE": mode,
        "SPACE_KIND_DATADOG_MODE": "fakeintake",
        "SPACE_KIND_ENV": "local-kind-owner-box",
        "SPACE_KIND_VERSION": "abc123",
        "SPACE_KIND_PHASE": "after-datadog",
        "SPACE_KIND_OWNER": "owner",
    })
    wrapper = tmp_path / "shim"
    wrapper.write_text(config.shim_wrapper(settings, "/usr/bin/env", "old-binary"))
    result = subprocess.run(
        ["bash", str(wrapper)],
        env={"PATH": os.environ["PATH"], "DD_ENV": "prod", "DD_VERSION": "old"},
        check=True, capture_output=True, text=True,
    )
    launch = dict(row.split("=", 1) for row in result.stdout.splitlines())
    assert launch["DD_ENV"] != "prod"
    assert launch["DD_VERSION"] != "old"
    expected_sink = {"datadog": "datadog", "otel": "otlp", "off": "off"}[mode]
    assert launch["SPACE_TRACE_EXPORTER"] == expected_sink
    assert launch["SPACE_METRICS_SINK"] == expected_sink
    args = [item for setting in settings["helm_set"] for item in ("--set-string", setting)]
    if global_overlay is not None:
        args += ["--set", f"global.observability={global_overlay}"]
    for chart in TELEMETRY_CHARTS:
        manifests = render(chart, "space", *args)
        workload = next(doc for doc in manifests if doc["kind"] in ("Deployment", "DaemonSet"))
        env = {
            entry["name"]: entry.get("value")
            for entry in workload["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        for key in ("DD_ENV", "DD_VERSION", "DD_TAGS", "SPACE_METRICS_SINK", "SPACE_TRACE_EXPORTER"):
            assert env[key] == launch[key], (chart.name, key)
        # Native shim metrics retain their binary build version; OTLP uses
        # the configured experiment identity across both launch boundaries.
        for key in ("SPACE_ENV", "SPACE_VERSION"):
            if mode == "otel":
                assert env[key] == launch[key], (chart.name, key)
            else:
                assert key not in launch
        resources = dict(item.split("=", 1) for item in env["OTEL_RESOURCE_ATTRIBUTES"].split(","))
        assert resources["deployment.environment.name"] == env["DD_ENV"]
        assert resources["service.version"] == env["DD_VERSION"]


@pytest.mark.parametrize(
    "path,mode",
    [("kind", "datadog"), ("kind", "otel"), ("kind", "off"), ("global", "datadog"), ("global", "otlp")],
)
@pytest.mark.parametrize(
    "trace_exporter",
    [None, '"off"', "off", "false", "null"],
    ids=["default", "quoted-off", "bare-off", "false", "null"],
)
def test_build_manager_traces_and_command_logs_stay_excluded_across_profiles(path, mode, trace_exporter, tmp_path):
    """Build failures can carry guest output, so build-manager exports no traces."""
    if path == "kind":
        settings = config.configuration({"SPACE_KIND_TELEMETRY_MODE": mode})
        args = [item for setting in settings["helm_set"] for item in ("--set-string", setting)]
    else:
        args = [
            "--set-string", f"global.observability.mode={mode}",
            "--set-string", "global.observability.environment=example",
            "--set-string", "global.observability.cluster=cluster-a",
        ]
    if trace_exporter is not None:
        # Use YAML, not Python booleans, to cover the caller-side conversion of
        # bare off to false and Helm's removal of a null default.
        values = tmp_path / "build-manager.yaml"
        values.write_text(f"traceExporter: {trace_exporter}\n")
        args += ["-f", str(values)]
    manifests = render(BUILD_MANAGER_CHART, "space-builder", *args)
    workload = next(doc for doc in manifests if doc["kind"] == "DaemonSet")
    annotations = workload["spec"]["template"]["metadata"]["annotations"]
    assert annotations["ad.datadoghq.com/build-manager.logs_exclude"] == "true"
    assert (annotations.get("ad.datadoghq.com/logs_exclude") == "true") == (path == "kind" and mode == "otel")
    env = {
        entry["name"]: entry.get("value")
        for entry in workload["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["SPACE_TRACE_EXPORTER"] == "off"
    assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" not in env
    if path == "global":
        assert env.get("DD_TRACE_ENABLED") != "true"
        assert "DD_TRACE_AGENT_PORT" not in env
        assert "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL" not in env
        assert "OTEL_TRACES_SAMPLER" not in env


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
    settings = config.configuration(env)
    assert not any(
        value.startswith(("telemetry.otlpMetricsEndpoint=", "telemetry.otlpTracesEndpoint="))
        for value in settings["helm_set"]
    )
    for signal in ("Metrics", "Traces"):
        assert f"telemetry.nodeOtlp{signal}Endpoint={SHIM_OVERRIDE}" in settings["helm_set"]
    for key in ("SPACE_KIND_OTLP_METRICS_ENDPOINT", "SPACE_KIND_OTLP_TRACES_ENDPOINT"):
        with pytest.raises(ValueError, match="cannot be combined with node-only"):
            config.configuration({**env, key: SHIM_OVERRIDE})


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
        ("SPACE_KIND_OTLP_METRICS_ENDPOINT", "http://127.0.0.1:4317"),
        ("SPACE_KIND_OTLP_TRACES_ENDPOINT", "http://[::1]:4317"),
        ("SPACE_KIND_OTLP_METRICS_ENDPOINT", "http://localhost:4317"),
        ("SPACE_KIND_OTLP_TRACES_ENDPOINT", "http://collector,telemetry.traceExporter=off"),
        ("global.observability", "false"),
        ("global.observability.mode", "unknown"),
        ("global.observability.mode", "true"),
        ("global.observability.otlp.protocol", "http/protobuf"),
        ("global.observability.otlp.nodeLocalPort", "0"),
        ("global.observability.otlp.nodeLocalPort", "65536"),
        ("global.observability.otlp.nodeLocalPort", "4317.5"),
        ("global.observability.datadog.apmPort", "8136"),
    ],
)
def test_invalid_configuration_rejected_before_bootstrap_mutates(key, value, tmp_path):
    if key.startswith("global."):
        # Global overlays enter through Helm, not the Kind profile parser.
        # Start from an accepted render so an unrelated chart failure cannot
        # make a rejected-input case pass vacuously.
        mode = "datadog" if ".datadog." in key else "otlp"
        args = [
            "--set", f"global.observability.mode={mode}",
            "--set", "global.observability.environment=example",
            "--set", "global.observability.cluster=cluster-a",
        ]
        chart = KIND_DIR / "charts/space-node-manager-local"
        assert render(chart, "space", *args)
        if key == "global.observability.datadog.apmPort":
            # Refuse partial native routing without blocking modes that do
            # not use this setting at all.
            for unused_mode in ("off", "otlp"):
                assert render(
                    chart, "space", *args, "--set", f"{key}={value}",
                    "--set", f"global.observability.mode={unused_mode}",
                )
        with pytest.raises(subprocess.CalledProcessError) as rejected:
            render(chart, "space", *args, "--set", f"{key}={value}")
        assert key in rejected.value.stderr
        assert not rejected.value.stdout
        return
    env = {
        **bootstrap_guard_environment(tmp_path),
        "SPACE_KIND_TELEMETRY_MODE": "otel",
        key: value,
    }
    result = subprocess.run(
        ["bash", str(KIND_DIR / "scripts/up.sh")],
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
        ("http://collector:4317\t", True),
        ("http://col lector:4317", True),
        ("http://collector,other:4317", True),
        ("http://collector\\:4317", True),
        ("http://{collector}:4317", True),
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


@pytest.mark.parametrize(
    "backend,traces",
    [(None, "off"), ("off", "off"), ("fakeintake", "datadog"), ("saas", "datadog")],
)
def test_native_traces_need_a_deployed_agent(backend, traces):
    """With no Agent, native trace export would only log refused connections."""
    environment = {"SPACE_KIND_TELEMETRY_MODE": "datadog"}
    if backend is not None:
        environment["SPACE_KIND_DATADOG_MODE"] = backend
    settings = config.configuration(environment)
    assert f"telemetry.traceExporter={traces}" in settings["helm_set"]
    # DogStatsD is fire-and-forget, so metrics keep the native sink.
    assert "telemetry.metricsSink=datadog" in settings["helm_set"]
    wrapper = config.shim_wrapper(settings, "/usr/bin/env", "local")
    assert f"export SPACE_TRACE_EXPORTER={traces}\n" in wrapper
    assert "export SPACE_METRICS_SINK=datadog\n" in wrapper
    assert ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" in wrapper) == (traces != "off")


@pytest.mark.parametrize("backend", ["Off", "none", "datadog"])
def test_unknown_datadog_backend_is_rejected_not_read_as_an_agent(backend):
    """A typo must not leave native traces pointed at an Agent that was never deployed."""
    environment = {"SPACE_KIND_TELEMETRY_MODE": "datadog", "SPACE_KIND_DATADOG_MODE": backend}
    with pytest.raises(ValueError, match="SPACE_KIND_DATADOG_MODE"):
        config.configuration(environment)
    assert config.datadog_backend({}) == "off"


@pytest.mark.parametrize(
    "env,mode,installs",
    [
        ({}, "datadog", False),
        ({"SPACE_KIND_TELEMETRY_MODE": "datadog"}, "datadog", False),
        ({"SPACE_KIND_TELEMETRY_MODE": "off"}, "off", False),
        ({"SPACE_KIND_TELEMETRY_MODE": "otel", "SPACE_KIND_OBSERVABILITY": "off"}, "otel", False),
        ({"SPACE_KIND_TELEMETRY_MODE": "otel", "SPACE_KIND_OBSERVABILITY": "auto"}, "otel", True),
        ({"SPACE_KIND_TELEMETRY_MODE": "otel", "SPACE_KIND_OBSERVABILITY_LAYOUT": "ci"}, "otel", True),
        ({"SPACE_KIND_OBSERVABILITY_LAYOUT": "ci"}, "datadog", False),
    ],
)
def test_backends_install_only_when_the_mode_uses_them(env, mode, installs):
    """Receiver installation follows the profile, with explicit reuse-only mode."""
    settings = config.configuration(dict(env))
    assert settings["mode"] == mode
    assert settings["install_observability"] is installs


@pytest.mark.parametrize("option", ["SPACE_KIND_OBSERVABILITY", "SPACE_KIND_OBSERVABILITY_LAYOUT"])
def test_an_unknown_observability_selector_is_rejected(option):
    """A typo must not quietly skip the backends the mode needs."""
    with pytest.raises(ValueError, match=option):
        config.configuration({option: "yes"})
