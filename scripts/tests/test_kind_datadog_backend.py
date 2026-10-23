"""Exercise the backend renderer and rollback boundary without SaaS credentials."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from kind_chart_render import KIND_DIR, bootstrap_guard_environment

sys.path.insert(0, str(KIND_DIR / "scripts"))
spec = importlib.util.spec_from_file_location(
    "kind_datadog_backend", KIND_DIR / "scripts/datadog_backend.py"
)
backend = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backend)


@pytest.fixture(scope="module", autouse=True)
def vendored_datadog_chart():
    """Vendor the upstream subchart so `helm template` can run.

    `charts/` is gitignored, and neither CI nor a fresh clone has run
    `tilt up`, so the dependency is absent exactly where these
    render tests matter most. Skipping instead would make the only guard that
    proves the fail-closed site and dummy-Secret wiring vacuous in CI.
    """
    chart = KIND_DIR / "charts/datadog-agent-local"
    if (chart / "charts").is_dir() and any((chart / "charts").glob("datadog-*.tgz")):
        return
    # `dependency build` alone fails with no repository definition.
    for command in (
        [
            "helm",
            "repo",
            "add",
            "datadog",
            "https://helm.datadoghq.com",
            "--force-update",
        ],
        ["helm", "dependency", "build", str(chart)],
    ):
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            pytest.fail(
                f"could not vendor the Datadog subchart: {' '.join(command)}\n"
                f"{result.stderr.strip()}"
            )


@pytest.fixture
def approved(tmp_path):
    values = {
        "SPACE_KIND_DATADOG_MODE": "saas",
        "SPACE_KIND_DATADOG_UNBOUNDED": "yes",
        "SPACE_KIND_DATADOG_SITE": "datadoghq.com",
        "SPACE_KIND_DATADOG_API_KEY_SECRET": "kind-dev-only-key",
        "SPACE_KIND_ENV": "local-kind-reviewer-devbox",
        "SPACE_KIND_OWNER": "reviewer",
        "SPACE_KIND_VERSION": "a" * 40,
        "SPACE_KIND_PHASE": "after-otel",
        "SPACE_KIND_TELEMETRY_MODE": "otel",
        "SPACE_KIND_SHIM_OTLP_ENDPOINT": "http://127.0.0.1:4317",
        "SPACE_KIND_DATADOG_DURATION_MINUTES": "5",
        "SPACE_KIND_DATADOG_ACKNOWLEDGE_SHARED_BACKEND": "yes",
    }
    path = tmp_path / "experiment.json"
    path.write_text(json.dumps(values))
    return {**values, "SPACE_KIND_OBSERVABILITY_CONFIG": str(path)}


def documents(config):
    return [doc for doc in yaml.safe_load_all(backend.render(config)) if doc]


def pod_specs(docs):
    return [
        doc["spec"]["template"]
        for doc in docs
        if doc["kind"] in ("DaemonSet", "Deployment")
        and doc["metadata"]["name"] != "datadog-fakeintake"
    ]


def test_ambient_key_never_enables_saas():
    assert backend.plan({"DD_API_KEY": "poison"})["backend"] == "off"
    assert backend.render(backend.plan({})) == ""


@pytest.mark.parametrize("mode,emitter", [("fakeintake", "datadog"), ("fakeintake", "otel"), ("saas", "otel")])
def test_real_helm_render_and_running_pod_verification(mode, emitter, approved, monkeypatch):
    env = {**approved, "SPACE_KIND_DATADOG_MODE": mode, "SPACE_KIND_TELEMETRY_MODE": emitter, "DD_API_KEY": "poison"}
    config = backend.plan(env)
    docs = documents(config)
    rendered = json.dumps(docs)
    assert "poison" not in rendered
    secrets = {d["metadata"]["name"] for d in docs if d["kind"] == "Secret"}
    assert "kind-dev-only-key" not in secrets
    assert (backend.FAKE_SECRET in secrets) == (mode == "fakeintake")
    assert ("local.invalid" in rendered) == (mode == "fakeintake")
    assert (backend.FAKE_URL in rendered) == (mode == "fakeintake")
    for pod in pod_specs(docs):
        for container in pod["spec"]["containers"]:
            envs = {e["name"]: e for e in container.get("env", [])}
            assert (
                envs["DD_API_KEY"]["valueFrom"]["secretKeyRef"]["name"]
                == config["api_key_secret"]
            )
            assert envs["DD_SITE"]["value"] == config["site"]
            assert f"version:{config['version']}" in json.loads(
                envs["DD_TAGS"]["value"]
            )
            if container["name"] == "agent":
                assert (
                    envs.get("DD_CONTAINER_INCLUDE_LOGS", {}).get("value", "")
                    == ("kube_namespace:^space$" if config["log_route"] == "datadog" else "")
                )
                assert envs["DD_CONTAINER_EXCLUDE_LOGS"]["value"] == "kube_namespace:.*"
                if mode == "saas":
                    assert envs["DD_HOSTNAME"]["value"].endswith(config["environment"])
    calls = []

    def kube(_config, *args, **_kwargs):
        calls.append(args)
        if args[:2] == ("get", "secret"):
            return "present"
        if args[:2] in (("get", "daemonset"), ("get", "deployment")):
            resource = json.loads(json.dumps(next(doc for doc in docs if doc["metadata"]["name"] == args[2] and doc["kind"].lower() == args[1])))
            resource["metadata"].setdefault("annotations", {})["kubectl.kubernetes.io/last-applied-configuration"] = json.dumps(resource)
            return json.dumps(resource)
        return json.dumps({"items": pod_specs(docs)}) if args[0] == "get" else "ok"

    monkeypatch.setattr(backend, "kube", kube)
    assert backend.apply(config)["node_agents"] == 1
    if mode == "saas":
        assert calls[-1][:2] == ("delete", "deployment,service,secret")
        assert calls[-1][calls[-1].index("-l") + 1] == backend.FAKE_SELECTOR
        with pytest.raises(ValueError, match="requires session"):
            backend.apply({**config, "unbounded": False})
    assert any(call[:2] == ("rollout", "status") for call in calls)
    # Existing template/apply installs need no new owner marker. Missing or
    # conflicting receipts must not authorize replacement of another Agent.
    for mutation in ("receipt", "chart", "cluster"):
        resource = json.loads(kube(config, "get", "daemonset", "datadog-agent"))
        if mutation == "receipt":
            resource["metadata"]["annotations"].clear()
        elif mutation == "chart":
            resource["metadata"]["labels"]["helm.sh/chart"] = "foreign-1.0.0"
        else:
            for container in resource["spec"]["template"]["spec"]["containers"]:
                for entry in container["env"]:
                    if entry["name"] == "DD_TAGS":
                        entry["value"] = '["kind_cluster:another-bench"]'
        monkeypatch.setattr(backend, "kube", lambda *_a, resource=resource, **_kw: json.dumps(resource))
        with pytest.raises(ValueError, match="Refusing unowned"):
            backend.guard_owner(config)
    # Verification inspects running pod specs, not merely desired Helm values.
    pods = pod_specs(docs)
    pods[0]["spec"]["containers"][0]["env"].append(
        {"name": "DD_SITE", "value": "wrong"}
    )
    monkeypatch.setattr(
        backend,
        "kube",
        lambda _c, *a, **_kw: json.dumps({"items": pods}) if a[0] == "get" else "ok",
    )
    with pytest.raises(ValueError, match="site does not match"):
        backend.verify(config)


@pytest.mark.parametrize(
    "key,value",
    [
        ("SPACE_KIND_DATADOG_MODE", "typo"),
        ("SPACE_KIND_DATADOG_SITE", "evil.example"),
        ("SPACE_KIND_DATADOG_API_KEY_SECRET", "datadog-secret"),
        ("SPACE_KIND_DATADOG_API_KEY_SECRET", backend.FAKE_SECRET),
        ("SPACE_KIND_ENV", "experimental"),
        ("SPACE_KIND_OWNER", ""),
        ("SPACE_KIND_VERSION", "latest"),
        ("SPACE_KIND_PHASE", ""),
        ("SPACE_KIND_DATADOG_DURATION_MINUTES", "121"),
        ("SPACE_KIND_DATADOG_ACKNOWLEDGE_SHARED_BACKEND", ""),
        ("SPACE_KIND_OBSERVABILITY_CONFIG", ""),
    ],
)
def test_rejects_incomplete_or_shared_saas_identity(approved, key, value):
    with pytest.raises(ValueError):
        backend.plan({**approved, key: value})


def test_secret_preflight_never_requests_secret_contents(approved, monkeypatch):
    calls = []

    def kube(config, *args, **kwargs):
        calls.append((config, args, kwargs))
        return "present"

    monkeypatch.setattr(backend, "kube", kube)
    backend.preflight(backend.plan(approved))
    assert calls[0][1] == (
        "get",
        "secret",
        "kind-dev-only-key",
        "-o",
        'go-template={{if index .data "api-key"}}present{{end}}',
    )
    monkeypatch.setattr(backend, "kube", lambda *_a, **_kw: "")
    with pytest.raises(ValueError, match="nonempty api-key"):
        backend.preflight(backend.plan(approved))


def test_session_preflight_failure_does_not_mutate_or_roll_back(approved, monkeypatch):
    def reject(_config):
        raise ValueError("missing credential")

    monkeypatch.setattr(backend, "preflight", reject)
    monkeypatch.setattr(backend, "stop", lambda *_a: pytest.fail("unexpected mutation"))
    monkeypatch.setattr(
        backend, "apply", lambda *_a: pytest.fail("unexpected mutation")
    )
    with pytest.raises(ValueError, match="missing credential"):
        backend.session(approved, backend.plan(approved))


@pytest.mark.parametrize("failure", ["apply", "concurrent_write", "ownership"])
def test_stop_persists_local_intent_before_apply_even_if_apply_fails(
    approved, monkeypatch, failure
):
    path = Path(approved["SPACE_KIND_OBSERVABILITY_CONFIG"])
    original = path.read_bytes()
    applied = []

    def kube(config, *args, **kwargs):
        if args[0] == "get":
            assert json.loads(path.read_text())["SPACE_KIND_DATADOG_MODE"] == "saas"
            if failure == "ownership":
                return '{"metadata": {}}'
            return ""
        assert json.loads(path.read_text())["SPACE_KIND_DATADOG_MODE"] == "fakeintake"
        assert config["backend"] == "fakeintake"
        assert config["api_key_secret"] == backend.FAKE_SECRET
        assert "kind-dev-only-key" not in kwargs["input"]
        applied.append(args)
        raise OSError("cluster temporarily unavailable")

    monkeypatch.setattr(backend, "kube", kube)
    if failure == "ownership":
        with pytest.raises(ValueError, match="Refusing unowned"):
            backend.stop(approved)
        assert path.read_bytes() == original
        assert not applied
    elif failure == "concurrent_write":
        original_render = backend.render

        def render(config):
            manifest = original_render(config)
            changed = json.loads(path.read_text())
            changed["SPACE_KIND_OWNER"] = "another-owner"
            path.write_text(json.dumps(changed))
            return manifest

        monkeypatch.setattr(backend, "render", render)
        with pytest.raises(ValueError, match="refusing to overwrite"):
            backend.stop(approved)
        assert json.loads(path.read_text())["SPACE_KIND_OWNER"] == "another-owner"
        assert not applied
    else:
        with pytest.raises(OSError, match="temporarily unavailable"):
            backend.stop(approved)
        assert applied == [("apply", "-f", "-")]
        assert backend.plan(backend.options(approved))["backend"] == "fakeintake"
        assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "ending",
    [
        "timeout",
        "interrupt",
        "rollback_error",
        "interrupt_rollback_error",
        "owner_changed",
    ],
)
def test_session_restores_local_on_timeout_or_interrupt(
    approved, monkeypatch, ending, capsys
):
    monkeypatch.setattr(backend, "preflight", lambda *_a: None)
    monkeypatch.setattr(backend, "render", lambda *_a: "manifest")
    monkeypatch.setattr(backend, "apply", lambda *_a: {"backend": "saas"})
    current = {**approved}
    if ending == "owner_changed":
        current["SPACE_KIND_ENV"] = "local-kind-another-owner"
    monkeypatch.setattr(backend, "options", lambda: current)
    stops = []

    def stop(env):
        stops.append(env)
        if "rollback_error" in ending:
            raise OSError("simulated rollback failure")
        return {"backend": "fakeintake"}

    monkeypatch.setattr(backend, "stop", stop)

    def sleep(seconds):
        assert seconds == 300
        if "interrupt" in ending:
            raise KeyboardInterrupt

    monkeypatch.setattr(backend.time, "sleep", sleep)
    if "interrupt" in ending:
        with pytest.raises(KeyboardInterrupt):
            backend.session(approved, backend.plan(approved))
    elif ending == "rollback_error":
        with pytest.raises(OSError, match="simulated rollback failure"):
            backend.session(approved, backend.plan(approved))
    elif ending == "owner_changed":
        with pytest.raises(ValueError, match="current owner"):
            backend.session(approved, backend.plan(approved))
    else:
        backend.session(approved, backend.plan(approved))
    assert stops == ([] if ending == "owner_changed" else [approved])
    if ending == "interrupt_rollback_error":
        assert "rollback failed: simulated rollback failure" in capsys.readouterr().err


def test_cli_rejects_saas_without_watched_config_before_mutations(tmp_path):
    # Invalid bring-up configurations fail before Docker, Kubernetes or builds.
    env = {
        **bootstrap_guard_environment(tmp_path),
        "SPACE_KIND_DATADOG_MODE": "saas",
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
    assert "watched SPACE_KIND_OBSERVABILITY_CONFIG" in result.stderr
    assert not (tmp_path / "unexpected-operation").exists()
    assert not list(Path(env["HOME"]).rglob("kind-cluster*.yaml"))


def _rendered_pods(mode):
    config = backend.plan(
        {
            "SPACE_KIND_DATADOG_MODE": mode,
            "SPACE_KIND_ENV": "iso-env",
            "SPACE_KIND_OWNER": "iso-owner",
        }
    )
    return config, pod_specs(list(yaml.safe_load_all(backend.render(config))))


@pytest.mark.parametrize(
    "mode,defect,expected",
    [
        ("fakeintake", "no_pods", "Expected running node and Cluster Agent pods"),
        ("fakeintake", "terminating", "still terminating"),
        ("fakeintake", "site", "site does not match"),
        ("fakeintake", "secret", "selected existing Secret"),
        ("fakeintake", "tags", "isolation tags"),
        ("fakeintake", "wrong_endpoint", "missing or nonlocal"),
        ("fakeintake", "missing_logs_endpoint", "missing or nonlocal"),
        ("saas", "leftover_endpoint", "endpoint/TLS overrides"),
        ("saas", "skip_tls", "endpoint/TLS overrides"),
        ("saas", "hostname", "hostname is not isolated"),
    ],
)
def test_each_verify_refusal_is_enforced(monkeypatch, approved, mode, defect, expected):
    config = backend.plan({**approved, "SPACE_KIND_DATADOG_MODE": mode})
    pods = pod_specs(documents(config))
    agent = next(
        c for p in pods for c in p["spec"]["containers"] if c["name"] == "agent"
    )
    env = agent["env"]
    if defect == "no_pods":
        pods.clear()
    elif defect == "terminating":
        pods[0]["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    elif defect == "site":
        env.append({"name": "DD_SITE", "value": "wrong.example"})
    elif defect == "secret":
        env.append(
            {
                "name": "DD_API_KEY",
                "valueFrom": {"secretKeyRef": {"name": "someone-elses-secret"}},
            }
        )
    elif defect == "tags":
        env.append({"name": "DD_TAGS", "value": "[]"})
    elif defect in ("wrong_endpoint", "leftover_endpoint"):
        env.append({"name": "DD_DD_URL", "value": "https://app.datadoghq.com"})
    elif defect == "missing_logs_endpoint":
        env[:] = [e for e in env if e["name"] != "DD_LOGS_CONFIG_LOGS_DD_URL"]
    elif defect == "skip_tls":
        env.append({"name": "DD_SKIP_SSL_VALIDATION", "value": "true"})
    elif defect == "hostname":
        env.append({"name": "DD_HOSTNAME", "value": "shared-host"})

    def kube(_config, *args, **_kwargs):
        return json.dumps({"items": pods}) if args[0] == "get" else "ok"

    monkeypatch.setattr(backend, "kube", kube)
    with pytest.raises(ValueError, match=expected):
        backend.verify(config)


def test_verify_selects_agent_pods_by_name_and_component(monkeypatch):
    """A typo in the selector makes every apply fail; assert the literal."""
    config, pods = _rendered_pods("fakeintake")
    selectors = []

    def kube(_config, *args, **_kwargs):
        if args[0] == "get" and "-l" in args:
            selectors.append(args[args.index("-l") + 1])
        return json.dumps({"items": pods}) if args[0] == "get" else "ok"

    monkeypatch.setattr(backend, "kube", kube)
    backend.verify(config)
    assert any(
        "app.kubernetes.io/name=datadog-agent" in s
        and "component in (agent,cluster-agent)" in s
        for s in selectors
    ), selectors


def test_verify_counts_pods_not_templates(monkeypatch):
    """`node_agents` must count running pods, not one per DaemonSet."""
    config, pods = _rendered_pods("fakeintake")
    agents = [
        p
        for p in pods
        if p["metadata"]["labels"].get("app.kubernetes.io/component") == "agent"
    ]
    cluster = [
        p
        for p in pods
        if p["metadata"]["labels"].get("app.kubernetes.io/component") == "cluster-agent"
    ]
    seeded = agents + [json.loads(json.dumps(agents[0]))] + cluster

    def kube(_config, *args, **_kwargs):
        return json.dumps({"items": seeded}) if args[0] == "get" else "ok"

    monkeypatch.setattr(backend, "kube", kube)
    result = backend.verify(config)
    assert result["node_agents"] == 2
    assert result["cluster_agents"] == 1
