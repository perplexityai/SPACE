#!/usr/bin/env python3
"""Explicit Kind-only Agent backend selection; never creates SaaS credentials."""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from telemetry_config import configuration, options

ROOT = Path(__file__).resolve().parents[4]
CHART = ROOT / "infra/space/kind/charts/datadog-agent-local"
FAKE_SECRET = "datadog-fakeintake-key"
FAKE_URL = "http://datadog-fakeintake.kube-system.svc.cluster.local"
# Each enabled Agent container gets the same explicit product routing. A
# missing override loses that product to local.invalid, even when others work.
REQUIRED_INTAKE_URLS = {
    "DD_DD_URL",
    "DD_PROCESS_CONFIG_PROCESS_DD_URL",
    "DD_APM_DD_URL",
    "DD_LOGS_CONFIG_LOGS_DD_URL",
    "DD_AGENT_TELEMETRY_LOGS_DD_URL",
    "DD_ORCHESTRATOR_EXPLORER_ORCHESTRATOR_DD_URL",
    "DD_CONTAINER_IMAGE_DD_URL",
    "DD_CONTAINER_LIFECYCLE_DD_URL",
    "DD_DATA_STREAMS_FORWARDER_DD_URL",
}
FAKE_SELECTOR = (
    "app.kubernetes.io/name=datadog-fakeintake,"
    "app.kubernetes.io/instance=datadog-agent,"
    "app.kubernetes.io/component=test-intake,"
    "app.kubernetes.io/part-of=space-local"
)
SITES = {
    "datadoghq.com",
    "datadoghq.eu",
    "us3.datadoghq.com",
    "us5.datadoghq.com",
    "ap1.datadoghq.com",
    "ap2.datadoghq.com",
    "uk1.datadoghq.com",
    "ddog-gov.com",
    "us2.ddog-gov.com",
}


def plan(env):
    result = configuration(env)
    mode = env.get("SPACE_KIND_DATADOG_MODE", "off")
    if mode not in {"off", "fakeintake", "saas"}:
        raise ValueError("SPACE_KIND_DATADOG_MODE must be off, fakeintake, or saas")
    result.update(
        backend=mode,
        context="kind-" + result["kind_cluster"],
        site="local.invalid",
        api_key_secret=FAKE_SECRET,
        duration_minutes=0,
        unbounded=env.get("SPACE_KIND_DATADOG_UNBOUNDED") == "yes",
    )
    if mode != "saas":
        return result
    if not env.get("SPACE_KIND_OBSERVABILITY_CONFIG"):
        raise ValueError(
            "SaaS requires a watched SPACE_KIND_OBSERVABILITY_CONFIG file for rollback"
        )
    site = env.get("SPACE_KIND_DATADOG_SITE", "")
    secret = env.get("SPACE_KIND_DATADOG_API_KEY_SECRET", "")
    if site not in SITES:
        raise ValueError("SaaS requires an explicit supported SPACE_KIND_DATADOG_SITE")
    if not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", secret
    ) or secret in {FAKE_SECRET, "datadog-secret"}:
        raise ValueError(
            "SaaS requires a separate existing dev-only API-key Secret, not either dummy/legacy Secret"
        )
    if not re.fullmatch(
        r"local-kind-[a-z0-9](?:[a-z0-9-]{0,47}[a-z0-9])?", result["environment"]
    ):
        raise ValueError(
            "SaaS SPACE_KIND_ENV must be a unique local-kind-<owner>-<devbox> identity"
        )
    if (
        not result["owner"]
        or not result["phase"]
        or not re.fullmatch(r"[a-f0-9]{40}", result["revision"])
    ):
        raise ValueError(
            "SaaS requires SPACE_KIND_OWNER, SPACE_KIND_PHASE, and a full source SHA in SPACE_KIND_VERSION"
        )
    # A SaaS experiment ships real data to a real organization, so the shim has
    # to go through the node-local Agent that forwards and tags it rather than
    # any other receiver. The Collectors are the default target now, so this
    # has to be pinned explicitly.
    if result["shim_endpoint"] != "http://127.0.0.1:4317":
        raise ValueError(
            "SaaS experiments require the node-local shim receiver; "
            "set SPACE_KIND_SHIM_OTLP_ENDPOINT=http://127.0.0.1:4317"
        )
    if env.get("SPACE_KIND_DATADOG_ACKNOWLEDGE_SHARED_BACKEND") != "yes":
        raise ValueError(
            "Acknowledge approved shared-monitor isolation/costs with SPACE_KIND_DATADOG_ACKNOWLEDGE_SHARED_BACKEND=yes"
        )
    try:
        duration = int(env.get("SPACE_KIND_DATADOG_DURATION_MINUTES", "0"))
    except ValueError:
        duration = 0
    if not 1 <= duration <= 120:
        raise ValueError(
            "SPACE_KIND_DATADOG_DURATION_MINUTES must be 1..120; use session for timed rollback"
        )
    result.update(site=site, api_key_secret=secret, duration_minutes=duration)
    return result


def kube(config, *args, namespace="kube-system", **kwargs):
    return subprocess.run(
        ["kubectl", "--context", config["context"], "-n", namespace, *args],
        check=True,
        text=True,
        capture_output=True,
        timeout=360,
        **kwargs,
    ).stdout


def preflight(config):
    if config["backend"] == "saas":
        # The API key never appears in output, arguments, generated values or Git.
        present = kube(
            config,
            "get",
            "secret",
            config["api_key_secret"],
            "-o",
            'go-template={{if index .data "api-key"}}present{{end}}',
        )
        if present.strip() != "present":
            raise ValueError(
                "The approved SaaS Secret must contain a nonempty api-key entry"
            )


def guard_owner(config):
    """Never replace an Agent deployed outside this selected Kind profile."""
    if config["backend"] == "off":
        # Workload log-exclusion annotations do not reconfigure an Agent.
        return
    for kind, name in (("daemonset", "datadog-agent"), ("deployment", "datadog-agent-cluster-agent")):
        raw = kube(config, "get", kind, name, "--ignore-not-found", "-o", "json")
        if raw.strip():
            resource = json.loads(raw)
            # This helper templates then kubectl-applies; it does not create a
            # Helm release. Recognize its existing chart/release labels and
            # Kind-bound values in both live and last-applied configuration.
            receipt = resource.get("metadata", {}).get("annotations", {}).get(
                "kubectl.kubernetes.io/last-applied-configuration", "{}"
            )
            try:
                applied = json.loads(receipt)
            except (ValueError, TypeError):
                applied = {}
            component = "agent" if kind == "daemonset" else "cluster-agent"
            if not all(owned_agent(item, name, component, config["kind_cluster"])
                       for item in (resource, applied)):
                raise ValueError(
                    f"Refusing unowned {kind}/{name}; expected chart/release identity, "
                    "Kind DD_TAGS and matching kubectl last-applied receipt. "
                    "An owner must review an unproven existing Agent migration"
                )


def owned_agent(resource, name, component, cluster):
    if not isinstance(resource, dict):
        return False
    metadata = resource.get("metadata", {})
    labels = metadata.get("labels", {})
    expected = {
        "app.kubernetes.io/name": "datadog-agent",
        "app.kubernetes.io/instance": f"datadog-agent-{component}",
        "app.kubernetes.io/managed-by": "Helm",
        "app.kubernetes.io/part-of": "kube--system-datadog--agent",
        "app.kubernetes.io/component": component,
    }
    if (resource.get("kind") != ("DaemonSet" if component == "agent" else "Deployment")
            or metadata.get("name") != name or metadata.get("namespace") != "kube-system"
            or not re.fullmatch(r"datadog-3\.[0-9]+\.[0-9]+", labels.get("helm.sh/chart", ""))
            or any(labels.get(key) != value for key, value in expected.items())):
        return False
    containers = resource.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    if not containers or not any(container.get("name") == component for container in containers):
        return False
    for container in containers:
        env = {item["name"]: item for item in container.get("env", [])}
        try:
            tags = json.loads(env.get("DD_TAGS", {}).get("value", "[]"))
        except (ValueError, TypeError):
            return False
        if (not isinstance(tags, list)
                or [tag for tag in tags if str(tag).startswith("kind_cluster:")] != [f"kind_cluster:{cluster}"]
                or not env.get("DD_API_KEY", {}).get("valueFrom", {}).get("secretKeyRef", {}).get("name")):
            return False
    return True


def agent_values(config):
    environment = config["environment"]
    tags = [f"env:{environment}", f"kind_cluster:{config['kind_cluster']}"]
    tags.extend(
        f"kind_{key}:{config[key]}" for key in ("owner", "phase") if config[key]
    )
    if config["version"]:
        tags.append(f"version:{config['version']}")
    datadog = {
        "apiKeyExistingSecret": config["api_key_secret"],
        "site": config["site"],
        # Agent-side cluster identity isolates experiments in the backend;
        # kind_cluster above remains the physical Kubernetes cluster name.
        "clusterName": environment.replace("_", "-").replace(".", "-"),
        "envDict": {"DD_ENV": environment},
        "tags": tags,
        "processAgent": {
            "enabled": False,
            "processCollection": False,
            "containerCollection": False,
            "processDiscovery": False,
        },
        "orchestratorExplorer": {"enabled": False},
        "containerImageCollection": {"enabled": False},
        "containerLifecycle": {"enabled": False},
        "collectEvents": False,
        "containerExcludeLogs": "kube_namespace:.*",
        "containerIncludeLogs": "kube_namespace:^space$" if config["log_route"] == "datadog" else "",
    }
    if config["backend"] == "saas":
        datadog["env"] = [
            {
                "name": "DD_KIND_NODE",
                "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}},
            },
            {"name": "DD_HOSTNAME", "value": f"$(DD_KIND_NODE)-{environment}"},
        ]
    return {
        "fakeintake": {"enabled": config["backend"] == "fakeintake"},
        "datadog": {"datadog": datadog},
    }


def render(config):
    if config["backend"] == "off":
        return ""
    args = [
        "helm",
        "template",
        "datadog-agent",
        str(CHART),
        "--namespace",
        "kube-system",
    ]
    if config["backend"] == "fakeintake":
        args += ["-f", str(CHART / "values-fakeintake.yaml")]
    # Fresh render on every transition: never reuse SaaS or fake endpoint values.
    args += ["-f", "-"]
    return subprocess.run(
        args,
        input=json.dumps(agent_values(config)),
        check=True,
        text=True,
        capture_output=True,
    ).stdout


def verify(config):
    if config["backend"] == "off":
        raise ValueError(
            "off does not uninstall/stop an existing Agent; use stop to reconcile Fake Intake"
        )
    if config["backend"] == "fakeintake":
        kube(
            config,
            "rollout",
            "status",
            "deployment/datadog-fakeintake",
            "--timeout=300s",
        )
    for resource in (
        "daemonset/datadog-agent",
        "deployment/datadog-agent-cluster-agent",
    ):
        kube(config, "rollout", "status", resource, "--timeout=300s")
    pods = json.loads(
        kube(
            config,
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/name=datadog-agent,app.kubernetes.io/component in (agent,cluster-agent)",
            "-o",
            "json",
        )
    )["items"]
    agents = [
        pod
        for pod in pods
        if any(c["name"] == "agent" for c in pod["spec"]["containers"])
    ]
    cluster_agents = [
        pod
        for pod in pods
        if any(c["name"] == "cluster-agent" for c in pod["spec"]["containers"])
    ]
    if not agents or not cluster_agents:
        raise ValueError("Expected running node and Cluster Agent pods")
    for pod in agents + cluster_agents:
        if pod["metadata"].get("deletionTimestamp"):
            raise ValueError(
                "Old Agent pods are still terminating; wait and verify again"
            )
        for container in pod["spec"]["containers"]:
            env = {entry["name"]: entry for entry in container.get("env", [])}
            if env.get("DD_SITE", {}).get("value") != config["site"]:
                raise ValueError("Running Agent site does not match selected backend")
            reference = (
                env.get("DD_API_KEY", {}).get("valueFrom", {}).get("secretKeyRef", {})
            )
            if (
                reference.get("name") != config["api_key_secret"]
                or reference.get("key") != "api-key"
            ):
                raise ValueError(
                    "Running Agent does not use the selected existing Secret"
                )
            tags = json.loads(env.get("DD_TAGS", {}).get("value", "[]"))
            if any(
                tag not in tags
                for tag in agent_values(config)["datadog"]["datadog"]["tags"]
            ):
                raise ValueError(
                    "Running Agent isolation tags do not match the experiment"
                )
            urls = {
                key: entry.get("value", "")
                for key, entry in env.items()
                if key.endswith("_DD_URL")
            }
            if config["backend"] == "fakeintake":
                if set(urls) != REQUIRED_INTAKE_URLS or any(
                    value != FAKE_URL for value in urls.values()
                ):
                    raise ValueError(
                        "Running Fake Intake Agent has a missing or nonlocal intake override"
                    )
            elif urls or env.get("DD_SKIP_SSL_VALIDATION", {}).get("value") == "true":
                raise ValueError("Running SaaS Agent retains endpoint/TLS overrides")
            if (
                config["backend"] == "saas"
                and pod in agents
                and env.get("DD_HOSTNAME", {}).get("value")
                != f"$(DD_KIND_NODE)-{config['environment']}"
            ):
                raise ValueError("Running SaaS node Agent hostname is not isolated")
    return {
        "backend": config["backend"],
        "node_agents": len(agents),
        "cluster_agents": len(cluster_agents),
        "verified": "running pod configuration; not backend ingestion",
    }


def apply(config, action="apply"):
    _warn_unbounded(config, action)
    preflight(config)
    guard_owner(config)
    if config["backend"] == "off":
        raise ValueError("off skips installation; use stop to stop SaaS export")
    manifest = render(config)
    kube(config, "apply", "-f", "-", input=manifest)
    result = verify(config)
    if config["backend"] == "saas":
        # Prune only this harness's local receiver after the new Agents pass
        # verification. A failed transition keeps the rollback receiver intact.
        kube(
            config,
            "delete",
            "deployment,service,secret",
            "-l",
            FAKE_SELECTOR,
            "--ignore-not-found",
            "--wait=true",
            "--timeout=120s",
        )
    return result


def stop(env):
    path = env.get("SPACE_KIND_OBSERVABILITY_CONFIG")
    if not path:
        raise ValueError(
            "stop requires the same watched SPACE_KIND_OBSERVABILITY_CONFIG used to enable SaaS"
        )
    path = Path(path)
    original = path.read_text()
    document = json.loads(original)
    document["SPACE_KIND_DATADOG_MODE"] = "fakeintake"
    # Validate and render the fallback before changing either file or cluster.
    desired_env = {**env, **document}
    config = plan(desired_env)
    manifest = render(config)
    # An unowned Agent must not change the watched intent either. Once this
    # guard passes, persist local intent before apply so a transient API error
    # cannot make a later Tilt reconciliation restart SaaS export.
    guard_owner(config)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(document, stream, indent=2)
            stream.write("\n")
        if path.read_text() != original:
            raise ValueError(
                "Observability config changed concurrently; refusing to overwrite"
            )
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    # Tilt watches the same file, so its next reconciliation also stays local.
    kube(config, "apply", "-f", "-", input=manifest)
    return verify(config)


def session(env, config):
    if config["backend"] != "saas":
        raise ValueError("session requires an explicitly approved SaaS configuration")
    # Reject missing credentials before entering the mutating/rollback scope.
    preflight(config)
    render(config)

    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGTERM, interrupted)
    failed = False
    try:
        print(json.dumps(apply(config, "session")), flush=True)
        print(
            f"SaaS session: rollback in {config['duration_minutes']} minutes; Ctrl-C also rolls back",
            flush=True,
        )
        time.sleep(config["duration_minutes"] * 60)
    except BaseException:
        failed = True
        raise
    finally:
        signal.signal(signal.SIGTERM, previous)
        # Preserve the initiating failure or interrupt. If the session ended
        # normally, a failed rollback must still make the command fail.
        try:
            current = options()
            current_plan = plan(current)
            identity = (
                "context",
                "cluster_id",
                "site",
                "api_key_secret",
                "environment",
                "owner",
            )
            if current_plan["backend"] == "fakeintake":
                print(json.dumps(verify(current_plan)), flush=True)
            elif any(current_plan[key] != config[key] for key in identity):
                # Another experiment now owns this file. Do not overwrite it.
                raise ValueError(
                    "Session configuration changed; run stop explicitly for its "
                    "current owner"
                )
            else:
                print(json.dumps(stop(current)), flush=True)
        except Exception as rollback_error:  # preserve the original failure
            if not failed:
                raise
            print(f"rollback failed: {rollback_error}", file=sys.stderr, flush=True)


def _warn_unbounded(config, action):
    """`duration_minutes` is only honoured by `session`.

    `apply` leaves SaaS export running until someone stops it, so demanding a
    duration there reads like a spending cap it is not.
    """
    if config.get("backend") == "saas" and action != "session":
        if not config.get("unbounded"):
            raise ValueError(
                "SaaS deployment requires session, or explicit "
                "SPACE_KIND_DATADOG_UNBOUNDED=yes for untimed export"
            )
        print(
            "warning: this deployment path allows SaaS export with no time bound; "
            "SPACE_KIND_DATADOG_DURATION_MINUTES applies to `session` only. "
            "Use `session`, or stop explicitly when finished.",
            file=sys.stderr,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("plan", "preflight", "guard-owner", "render", "apply", "verify", "stop", "session"),
    )
    parser.add_argument(
        "--for-deployment",
        action="store_true",
        help="require explicit acknowledgement of untimed SaaS deployment",
    )
    args = parser.parse_args()
    try:
        env = options()
        if args.command == "stop":
            print(json.dumps(stop(env)))
            return
        config = plan(env)
        if args.for_deployment:
            _warn_unbounded(config, args.command)
        if args.command == "guard-owner":
            guard_owner(config)
        elif args.command == "render":
            print(render(config), end="")
        elif args.command == "preflight":
            preflight(config)
            print(json.dumps(config))
        elif args.command == "plan":
            print(json.dumps(config))
        elif args.command == "session":
            session(env, config)
        else:
            print(
                json.dumps(
                    {
                        "apply": lambda: apply(config),
                        "verify": lambda: verify(config),
                        "stop": lambda: stop(env),
                    }[args.command]()
                )
            )
    except KeyboardInterrupt:
        parser.exit(
            130, "Session interrupted; check the rollback verification above.\n"
        )
    except (
        ValueError,
        OSError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
    ) as error:
        # A failed subprocess may hold Kubernetes data in stdout/stderr; never
        # dump it wholesale (in particular when preflighting a credential).
        if isinstance(error, subprocess.CalledProcessError):
            message = f"{error.cmd[0]} failed (exit {error.returncode})"
        elif isinstance(error, subprocess.TimeoutExpired):
            message = f"{error.cmd[0]} timed out"
        else:
            message = str(error)
        parser.exit(2, f"Kind Datadog configuration: {message}\n")


if __name__ == "__main__":
    main()
