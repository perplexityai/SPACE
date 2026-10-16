"""SPACE's cluster releases and the space-observability chart agree on the node-local Collector.

One values file drives both. In otlp mode, with a non-default nodeLocalPort,
every telemetry producer the `space` (worker) and `space-builder` releases of
infra/space/cluster/chart render must resolve to http://<node IP>:<port>, the
port space-observability publishes the node-local Collector on. That covers
pod-network and host-network containers, the runtime-supervisor's space.env
and the shim settings node-manager's node-env init container writes, and the
wrapper install-shim.sh installs, which reads those settings. Build-manager's
trace exporter stays off, and the host processes keep their own service
identity: the runtime-supervisor's, and the shim's with the version it was
installed with. Pod-network producers must run in a namespace the
Collector admits, and the non-root host-network producers, which some CNIs
refuse without nodeLocal.hostPeers, must be named in the README.
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CLUSTER = ROOT / "infra/space/cluster"
CHART = CLUSTER / "chart"
EXAMPLE = CLUSTER / "values.example.yaml"
BUILDER_VALUES = CHART / "values-builder.yaml"
INSTALL_SHIM = CLUSTER / "install-shim.sh"
OBS_CHARTS = ROOT / "infra/space/observability/charts"
UMBRELLA = OBS_CHARTS / "space-observability"
UMBRELLA_README = UMBRELLA / "README.md"

PORT = 14317
NODE_IP = "10.0.0.5"
ENDPOINT = f"http://{NODE_IP}:{PORT}"
SPACE_NS = "space"
SETTINGS = [
    "--set",
    "global.observability.mode=otlp",
    "--set",
    f"global.observability.otlp.nodeLocalPort={PORT}",
]
# What the kubelet would put in the downward-API fields the charts read.
FIELDS = {
    "status.hostIP": NODE_IP,
    "spec.nodeName": "node-a",
    "metadata.name": "pod-a",
    "metadata.namespace": SPACE_NS,
}
NODE_ENV = "kind-node-env"
CONFIG_DIR = "/space/config"
RUN_DIR = "/space/run"


def helm(*args: str) -> str:
    result = subprocess.run(["helm", *args], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    return result.stdout


def documents(text: str) -> list[dict]:
    return [d for d in yaml.safe_load_all(text) if d]


@pytest.fixture(scope="module")
def releases() -> dict[str, list[dict]]:
    """The two releases install.sh creates, rendered as it renders them."""
    worker = helm(
        "template",
        "space",
        str(CHART),
        "--namespace",
        SPACE_NS,
        "-f",
        str(EXAMPLE),
        *SETTINGS,
    )
    builder = helm(
        "template",
        "space-builder",
        str(CHART),
        "--namespace",
        SPACE_NS,
        "-f",
        str(BUILDER_VALUES),
        "-f",
        str(EXAMPLE),
        *SETTINGS,
    )
    return {"worker": documents(worker), "builder": documents(builder)}


@pytest.fixture(scope="module")
def observability_docs() -> list[dict]:
    for chart in ("metrics", "logs", "traces", "grafana", "space-observability"):
        helm("dependency", "build", str(OBS_CHARTS / chart))
    out = helm(
        "template",
        "obs",
        str(UMBRELLA),
        "--namespace",
        "space-observability",
        "-f",
        str(EXAMPLE),
        *SETTINGS,
    )
    return documents(out)


def producers(docs: list[dict]):
    """(workload, pod spec, container) for every container carrying telemetry settings."""
    for doc in docs:
        if doc["kind"] not in ("DaemonSet", "Deployment", "StatefulSet", "Job"):
            continue
        spec = doc["spec"]["template"]["spec"]
        for container in [*(spec.get("initContainers") or []), *spec["containers"]]:
            if any(e["name"] == "SPACE_METRICS_SINK" for e in container.get("env") or []):
                yield doc, spec, container


def resolve(env_list: list[dict]) -> dict[str, str]:
    """The env the kubelet would give the container: fieldRefs and $(VAR) resolved."""
    env: dict[str, str] = {}
    for entry in env_list:
        if "value" in entry:
            value = re.sub(
                r"\$\(([A-Za-z_][A-Za-z0-9_]*)\)",
                lambda m: env.get(m[1], m[0]),
                str(entry["value"]),
            )
        else:
            field = ((entry.get("valueFrom") or {}).get("fieldRef") or {}).get("fieldPath")
            if field is None:
                continue  # Secret references carry no telemetry settings.
            value = FIELDS[field]
        env[entry["name"]] = value
    return env


def base_name(workload: str) -> str:
    """space-builder-node-api and space-node-api are the same service."""
    return re.sub(r"^space-builder-", "space-", workload)


def runs_as_root(spec: dict, container: dict) -> bool:
    context = container.get("securityContext") or {}
    if context.get("privileged"):
        return True
    uid = context.get("runAsUser", (spec.get("securityContext") or {}).get("runAsUser"))
    return uid == 0


def node_env_container(docs: list[dict]) -> dict:
    (container,) = [c for _, _, c in producers(docs) if c["name"] == NODE_ENV]
    return container


def run_node_env(container: dict, scratch: Path, space_env: str = "") -> dict[str, Path]:
    """Run the rendered node-env script against scratch copies of its host paths,
    `space_env` standing in for a space.env the node's bootstrap already wrote."""
    paths = {CONFIG_DIR: scratch / "config", RUN_DIR: scratch / "run"}
    for path in paths.values():
        path.mkdir()
    if space_env:
        (paths[CONFIG_DIR] / "space.env").write_text(space_env)
    (paths[RUN_DIR] / "runtime-supervisor").mkdir()
    mounts = {m["mountPath"] for m in container.get("volumeMounts") or []}
    assert set(paths) <= mounts, f"{NODE_ENV} no longer mounts {sorted(set(paths) - mounts)}"
    script = container["command"][-1]
    for real, fake in paths.items():
        script = script.replace(real, str(fake))
    assert "/space/" not in script, "node-env writes a host path this test does not redirect"
    env = {"PATH": os.environ["PATH"], **resolve(container["env"])}
    subprocess.run(["bash", "-ec", script], env=env, cwd=scratch, check=True, capture_output=True)
    return {
        "runtime": paths[CONFIG_DIR] / "space.env",
        "shim": paths[CONFIG_DIR] / "shim-telemetry.env",
        "restart": paths[RUN_DIR] / "runtime-supervisor/restart-required",
    }


def env_file(path: Path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


SHIM = "containerd-shim-space-v2"
SHIM_VERSION = "1.2.3-test"
SUPERVISOR = "space-runtime-supervisor"
SHIM_REAL = "/usr/local/lib/space/containerd-shim-space-v2"
SHIM_WRAPPER = "/usr/local/bin/containerd-shim-space-v2"
SHIM_SETTINGS = f"{CONFIG_DIR}/shim-telemetry.env"


def wrapper_env(settings: Path | None, scratch: Path) -> dict[str, str]:
    """The env the installed shim wrapper hands the shim, `settings` standing in for the node's file.

    install-shim.sh installs into a disposable --root; the stand-in shim prints its env.
    """
    root = scratch / "root"
    binary = scratch / "shim-stand-in"
    binary.write_text("#!/bin/sh\nexec env -0\n")
    binary.chmod(0o755)
    subprocess.run(
        [
            "bash",
            str(INSTALL_SHIM),
            "--binary",
            str(binary),
            "--version",
            SHIM_VERSION,
            "--root",
            str(root),
        ],
        capture_output=True,
        check=True,
    )
    wrapper = (root / SHIM_WRAPPER.lstrip("/")).read_text()
    rest = wrapper
    for path in (SHIM_REAL, SHIM_SETTINGS):
        assert path in rest, f"wrapper no longer names {path}"
        rest = rest.replace(path, "")
    assert "/space/" not in rest and "/usr/local/" not in rest, "wrapper names a host path this test does not redirect"
    wrapper = wrapper.replace(SHIM_REAL, str(root / SHIM_REAL.lstrip("/")))
    wrapper = wrapper.replace(SHIM_SETTINGS, str(settings if settings is not None else scratch / "absent.env"))
    raw = subprocess.run(
        ["bash", "-c", wrapper, SHIM],
        env={"PATH": os.environ["PATH"]},
        capture_output=True,
        check=True,
    ).stdout
    return dict(item.decode().split("=", 1) for item in raw.split(b"\0") if item)


def identity(env: dict[str, str], default_service: str) -> dict[str, str]:
    """Service, environment and version a SPACE producer reports from `env`.

    Mirrors crates/libs/observability/src/identity.rs and shim/identity.go: the
    SPACE_* name, then the OTel name, then the resource attribute, then DD_*.
    """
    entries = (entry.partition("=") for entry in env.get("OTEL_RESOURCE_ATTRIBUTES", "").split(","))
    attributes = {key.strip(): value.strip() for key, sep, value in entries if sep}

    def first(*values: str | None) -> str:
        return next((v.strip() for v in values if v and v.strip()), "")

    return {
        "service": first(
            env.get("SPACE_SERVICE_NAME"),
            env.get("OTEL_SERVICE_NAME"),
            attributes.get("service.name"),
            env.get("DD_SERVICE"),
        )
        or default_service,
        "environment": first(
            env.get("SPACE_ENV"),
            attributes.get("deployment.environment.name"),
            env.get("DD_ENV"),
        ),
        "version": first(
            env.get("SPACE_VERSION"),
            attributes.get("service.version"),
            env.get("DD_VERSION"),
        ),
    }


@pytest.mark.parametrize("role", ["worker", "builder"])
def test_every_producer_sends_to_the_node_local_port(releases, role: str) -> None:
    found = list(producers(releases[role]))
    names = {base_name(doc["metadata"]["name"]) for doc, _, _ in found}
    node_services = {
        "space-node-api",
        "space-node-ctl",
        "space-node-manager",
        "space-sandbox-manager",
        "space-template-manager",
        "space-volume-manager",
        "space-runtime-manager",
        "space-credential-manager",
    }
    assert node_services <= names, sorted(node_services - names)
    if role == "worker":
        assert {
            "space-api",
            "space-data-plane",
            "space-cluster-controller",
            "space-internal-api",
        } <= names
    else:
        assert "space-build-manager" in names
        assert "space-api" not in names
    for doc, _, container in found:
        env = resolve(container["env"])
        where = f"{role}: {doc['metadata']['name']}/{container['name']}"
        assert env["SPACE_METRICS_SINK"] == "otlp", where
        assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == ENDPOINT, where
        if base_name(doc["metadata"]["name"]) == "space-build-manager":
            continue  # test_build_manager_keeps_traces_off
        assert env["SPACE_TRACE_EXPORTER"] == "otlp", where
        assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == ENDPOINT, where


def test_build_manager_keeps_traces_off(releases) -> None:
    """Its trace exporter stays off in every mode; metrics follow the mode."""
    (env,) = [
        resolve(c["env"])
        for doc, _, c in producers(releases["builder"])
        if base_name(doc["metadata"]["name"]) == "space-build-manager"
    ]
    assert env["SPACE_TRACE_EXPORTER"] == "off"
    assert "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" not in env
    assert env["SPACE_METRICS_SINK"] == "otlp"
    assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == ENDPOINT


@pytest.mark.parametrize("role", ["worker", "builder"])
def test_host_processes_get_the_node_local_port_and_their_own_identity(releases, role: str) -> None:
    """node-env writes space.env (runtime-supervisor) and shim-telemetry.env; the
    installed shim wrapper hands the shim those settings under the shim's own
    name and the --version it was installed with."""
    container = node_env_container(releases[role])
    node_env = resolve(container["env"])
    environment = identity(node_env, "")["environment"]
    assert environment == "example"  # values.example.yaml global.observability.environment
    with tempfile.TemporaryDirectory() as tmp:
        scratch = Path(tmp)
        # A bootstrap-owned line survives; a stale telemetry setting does not.
        written = run_node_env(container, scratch, "BOOTSTRAP_SETTING=kept\nSPACE_METRICS_SINK=off\n")
        runtime = env_file(written["runtime"])
        assert runtime["BOOTSTRAP_SETTING"] == "kept"
        assert written["restart"].exists(), "a changed space.env must mark the supervisor for restart"
        shim = wrapper_env(written["shim"], scratch)
    for name, env in (("space.env", runtime), ("shim wrapper", shim)):
        assert env["SPACE_METRICS_SINK"] == "otlp", name
        assert env["SPACE_TRACE_EXPORTER"] == "otlp", name
        assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == ENDPOINT, name
        assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == ENDPOINT, name
        assert "NODE_IP" not in env, name
    assert identity(runtime, SUPERVISOR) == {
        "service": SUPERVISOR,
        "environment": environment,
        "version": identity(node_env, "")["version"],  # the supervisor ships in node-manager's image
    }
    assert identity(shim, SHIM) == {
        "service": SHIM,
        "environment": environment,
        "version": SHIM_VERSION,
    }
    # The Datadog client tags its own metrics from DD_* alone.
    assert (shim.get("DD_SERVICE"), shim.get("DD_VERSION")) == (SHIM, SHIM_VERSION)


def test_shim_exports_nothing_without_node_settings() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        env = wrapper_env(None, Path(tmp))
    assert env["SPACE_METRICS_SINK"] == "off"
    assert env["SPACE_TRACE_EXPORTER"] == "off"
    assert not any(name.startswith("OTEL_EXPORTER_OTLP_") for name in env)
    assert identity(env, SHIM)["service"] == SHIM


def test_collector_listens_where_producers_send(observability_docs) -> None:
    (daemonset,) = [
        d for d in observability_docs if d["kind"] == "DaemonSet" and d["metadata"]["name"] == "node-collector"
    ]
    (container,) = daemonset["spec"]["template"]["spec"]["containers"]
    assert [p["hostPort"] for p in container["ports"]] == [PORT]


@pytest.mark.parametrize("role", ["worker", "builder"])
def test_pod_network_producers_are_in_an_admitted_namespace(releases, observability_docs, role: str) -> None:
    (policy,) = [
        d for d in observability_docs if d["kind"] == "NetworkPolicy" and d["metadata"]["name"] == "node-collector"
    ]
    (rule,) = policy["spec"]["ingress"]
    admitted = rule["from"][0]["namespaceSelector"]["matchExpressions"][0]["values"]
    for doc, spec, _ in producers(releases[role]):
        if not spec.get("hostNetwork"):
            assert doc["metadata"].get("namespace", SPACE_NS) in admitted, doc["metadata"]["name"]


def test_non_root_host_network_producers_are_documented(releases) -> None:
    """They reach the Collector from the node's address as non-root processes,
    which some CNIs refuse without nodeLocal.hostPeers; the README names them."""
    non_root = sorted(
        {
            base_name(doc["metadata"]["name"])
            for docs in releases.values()
            for doc, spec, container in producers(docs)
            if spec.get("hostNetwork") and not runs_as_root(spec, container)
        }
    )
    assert non_root, "expected non-root host-network producers"
    readme = UMBRELLA_README.read_text()
    missing = [name for name in non_root if f"`{name}`" not in readme]
    assert not missing, f"{missing} not named in {UMBRELLA_README.relative_to(ROOT)}"
