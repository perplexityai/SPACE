#!/usr/bin/env python3
"""Shared, non-secret Kind telemetry configuration for Tilt, bootstrap and shims."""

import argparse
import ipaddress
import json
import os
import re
import shlex
import socket
import urllib.parse
import subprocess
from pathlib import Path

OPTIONS = {
    "SPACE_KIND_DATADOG_MODE",
    "SPACE_KIND_DATADOG_UNBOUNDED",
    "SPACE_KIND_DATADOG_SITE",
    "SPACE_KIND_DATADOG_API_KEY_SECRET",
    "SPACE_KIND_DATADOG_DURATION_MINUTES",
    "SPACE_KIND_DATADOG_ACKNOWLEDGE_SHARED_BACKEND",
    "SPACE_KIND_TELEMETRY_MODE",
    "SPACE_KIND_CLUSTER_ID",
    "SPACE_KIND_ENV",
    "SPACE_KIND_VERSION",
    "SPACE_KIND_PHASE",
    "SPACE_KIND_OWNER",
    "SPACE_KIND_SHIM_ENV",
    "SPACE_KIND_SHIM_OTLP_ENDPOINT",
    "SPACE_KIND_OBSERVABILITY",
}
PHASES = {"before-main", "after-datadog", "after-otel"}
MODES = {
    "datadog": ("datadog", "datadog"),
    "otel": ("otlp", "otlp"),
    "journald": ("journald", "datadog"),
}


def otlp_endpoint_rejection(endpoint):
    """Why an endpoint cannot be used, or None when it can.

    The rule the Rust services, the Go shim and the smoke runner apply, so an
    override accepted here is not refused later by the shim it configures.
    Duplicated rather than imported: these are separate entry points with no
    shared module. Keep it in step with `otlp_endpoint_rejection` in
    `scripts/space-cluster-smoke.py`.
    """
    if "?" in endpoint or "#" in endpoint:
        return "must not include a query or fragment"
    try:
        parsed = urllib.parse.urlsplit(endpoint)
    except ValueError:
        return "must be a URL"
    if parsed.scheme != "http":
        return "must use the plaintext http:// scheme"
    if parsed.username is not None or parsed.password is not None:
        return "must not embed credentials"
    if not parsed.hostname:
        return "must include a host"
    host, bracket, rest = parsed.netloc.rpartition("]")
    if bracket:
        port = rest[1:] if rest.startswith(":") else None
    else:
        _, separator, tail = parsed.netloc.rpartition(":")
        port = tail if separator else None
    if port is not None and not (
        port.isascii() and port.isdigit() and 1 <= int(port) <= 65535
    ):
        return "must use a port between 1 and 65535"
    try:
        parsed.port
    except ValueError:
        return "must use a port between 1 and 65535"
    if parsed.path not in ("", "/"):
        return "must not include a path"
    return None


def options(environ=None):
    env = dict(os.environ if environ is None else environ)
    path = env.get("SPACE_KIND_OBSERVABILITY_CONFIG")
    if path:
        document = json.loads(Path(path).read_text())
        if not isinstance(document, dict) or set(document) - OPTIONS:
            raise ValueError(
                "observability config must contain only supported SPACE_KIND options"
            )
        if any(not isinstance(value, str) for value in document.values()):
            raise ValueError("observability config values must be strings")
        # A watched config file is authoritative, including for stop-export;
        # an older environment must not turn SaaS back on after file rollback.
        env.update(document)
    return env


def default_environment():
    """Per-devbox identity, matching bootstrap.sh's pre-selector behaviour.

    The documented Datadog filter is `env:local-*` for all local-dev traffic and
    `env:local-<host>` for one devbox, so an ordinary bootstrap that supplies no
    experiment identity must not collapse every developer onto one shared env.
    """
    host = re.sub(r"[^a-z0-9-]", "-", socket.gethostname().lower()).strip("-")
    return f"local-{host}"[:128] if host else "local-unknown"


def token(value, name, *, empty=False):
    if empty and not value:
        return value
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", value):
        raise ValueError(
            f"{name} must be a lowercase telemetry identifier (max 128 characters)"
        )
    return value


def configuration(env):
    kind_cluster = env.get("SPACE_KIND_CLUSTER", "space-local")
    if not re.fullmatch(r"(?:space-local(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?|space-oss-[0-9a-f]{10})", kind_cluster):
        raise ValueError(
            "SPACE_KIND_CLUSTER must name a space-local or space-oss-<10 hex digits> Kind bench"
        )
    cluster_id = env.get("SPACE_KIND_CLUSTER_ID", "kind-local")
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", cluster_id):
        raise ValueError(
            "SPACE_KIND_CLUSTER_ID must be a lowercase DNS label (max 63 characters)"
        )
    # Kind defaults to otel: the local Collectors come up with the cluster, so
    # metrics and traces land somewhere queryable without a vendor round trip.
    # `datadog` stays available and pairs with SPACE_KIND_DATADOG_MODE=fakeintake
    # to validate DogStatsD output locally.
    mode = env.get("SPACE_KIND_TELEMETRY_MODE", "otel")
    if mode not in MODES:
        raise ValueError("SPACE_KIND_TELEMETRY_MODE must be datadog, otel, or journald")
    environment = token(
        env.get("SPACE_KIND_ENV")
        or env.get("SPACE_KIND_SHIM_ENV")
        or default_environment(),
        "SPACE_KIND_ENV",
    )
    revision = token(
        env.get("SPACE_KIND_VERSION", ""), "SPACE_KIND_VERSION", empty=True
    )
    phase = env.get("SPACE_KIND_PHASE", "")
    if phase and phase not in PHASES:
        raise ValueError("SPACE_KIND_PHASE must name a supported before/after phase")
    owner = token(env.get("SPACE_KIND_OWNER", ""), "SPACE_KIND_OWNER", empty=True)
    endpoint = env.get("SPACE_KIND_SHIM_OTLP_ENDPOINT")
    if endpoint is None and mode != "otel":
        endpoint = "http://127.0.0.1:4317"
    if endpoint is not None:
        rejection = otlp_endpoint_rejection(endpoint)
        if rejection:
            raise ValueError(f"SPACE_KIND_SHIM_OTLP_ENDPOINT {rejection}")
    version = f"{revision}-{phase}" if revision and phase else revision
    metrics, traces = MODES[mode]
    # `otel` needs the local Collectors: install-shim resolves their ClusterIPs
    # and fails without them. `auto` installs them exactly when the mode uses
    # them. `off` is for a caller supplying its own receiver, and for CI, whose
    # Kind jobs test sandbox lifecycle and should not pull six more images
    # through a rate-limited mirror.
    observability = env.get("SPACE_KIND_OBSERVABILITY", "auto")
    if observability not in {"auto", "off"}:
        raise ValueError("SPACE_KIND_OBSERVABILITY must be auto or off")
    return {
        "mode": mode,
        "observability": observability,
        "install_observability": observability == "auto" and mode == "otel",
        "kind_cluster": kind_cluster,
        "cluster_id": cluster_id,
        "environment": environment,
        "revision": revision,
        "phase": phase,
        "owner": owner,
        "version": version,
        "shim_endpoint": endpoint,
        "helm_set": [
            f"telemetry.metricsSink={metrics}",
            f"telemetry.traceExporter={traces}",
            f"telemetry.traceSampler={'always_on' if mode == 'otel' else ''}",
            f"datadogEnvironment={environment}",
            f"telemetry.version={version}",
            f"telemetry.phase={phase}",
            f"telemetry.owner={owner}",
            f"telemetry.cluster={kind_cluster}",
            f"clusterId={cluster_id}",
        ],
    }


# The Go shim reads SPACE_METRICS_SINK too and has no journald sink, erroring
# on that value. Left unset it defaults to auto -> datadog, which would keep the
# shim on native metrics while every workload moved to OTLP: wrong, silently.
SHIM_METRICS_SINKS = {"datadog": "datadog", "otel": "otlp", "journald": "off"}
# Each signal has its own Collector Service; the shim needs both.
SHIM_COLLECTORS = {"traces": "space-traces", "metrics": "space-metrics"}


def collector_endpoint(config, signal):
    """A node-reachable OTLP endpoint for one signal's Collector.

    containerd runs outside pods, so the Kind node has no cluster DNS: resolve
    the Service to its ClusterIP and let kube-proxy route. Only at install time.
    """
    if config["shim_endpoint"] is not None:
        return config["shim_endpoint"]
    namespace = SHIM_COLLECTORS[signal]
    context = f"kind-{config['kind_cluster']}"
    try:
        result = subprocess.run(
            [
                "kubectl", "--context", context,
                "get", "svc", "collector", "-n", namespace,
                "-o", "jsonpath={.spec.clusterIP}", "--request-timeout=10s",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        address = ipaddress.ip_address(result.stdout.strip())
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        raise ValueError(
            f"cannot resolve the {signal} Collector Service {namespace}/collector "
            f"to a ClusterIP in {context}; install it with "
            f"infra/space/kind/{signal}/local.sh up, or set "
            "SPACE_KIND_SHIM_OTLP_ENDPOINT to a node-reachable OTLP/gRPC endpoint"
        ) from error
    host = f"[{address}]" if address.version == 6 else str(address)
    return f"http://{host}:4317"


def shim_endpoint(config):
    return collector_endpoint(config, "traces")


def exports_otlp(config):
    """Whether the shim sends either signal over OTLP in this mode."""
    return "otlp" in (SHIM_METRICS_SINKS[config["mode"]], MODES[config["mode"]][1])


def shim_wrapper(config, binary, fallback_version):
    version = config["version"] or fallback_version
    values = {
        "SPACE_CLUSTER_ID": config["cluster_id"],
        "SPACE_TRACE_EXPORTER": MODES[config["mode"]][1],
        "DD_AGENT_HOST": "127.0.0.1",
        "DD_ENV": config["environment"],
        "DD_SERVICE": "containerd-shim-space-v2",
        "DD_VERSION": version,
        "SPACE_METRICS_SINK": SHIM_METRICS_SINKS[config["mode"]],
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": shim_endpoint(config),
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc",
    }
    if exports_otlp(config):
        # The SPACE and OTel identity settings outrank the DD_* ones, so
        # setting them in a Datadog-only mode would change the tags this shim
        # emits: its DogStatsD version tag carries the shim's build version,
        # and SPACE_VERSION would replace it with the wrapper's. Only a mode
        # that actually exports OTLP needs them.
        values["SPACE_ENV"] = config["environment"]
        values["SPACE_VERSION"] = version
        values["OTEL_SERVICE_NAME"] = "containerd-shim-space-v2"
    if SHIM_METRICS_SINKS[config["mode"]] == "otlp":
        # Shim metrics have no endpoint fallback, unlike traces: without these
        # the shim fails to start rather than guessing a port.
        values["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = collector_endpoint(config, "metrics")
        values["OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"] = "grpc"
    if config["mode"] == "otel":
        values["OTEL_TRACES_SAMPLER"] = "always_on"
    tags = f"kind_cluster:{config['kind_cluster']}"
    # Identity fields, which the resolver ranks above DD_*, and cluster
    # metadata, which it does not read as identity at all. Gating the direct
    # SPACE_* settings alone would not be enough: the resolver reads
    # service.version from here into the same slot, so a Datadog-only mode
    # would still replace the shim's build version in its DogStatsD tag.
    identity = [
        f"deployment.environment.name={config['environment']}",
        f"service.version={version}",
    ]
    metadata = [f"k8s.cluster.name={config['kind_cluster']}"]
    for key in ("owner", "phase"):
        if config[key]:
            tags += f",kind_{key}:{config[key]}"
            metadata.append(f"kind.{key}={config[key]}")
    resource = ",".join((identity if exports_otlp(config) else []) + metadata)
    values["DD_TAGS"] = tags
    lines = ["#!/usr/bin/env bash", "set -euo pipefail"]
    lines.extend(f"export {key}={shlex.quote(value)}" for key, value in values.items())
    # The resolver computes the process-local attributes itself and drops any it
    # inherits through OTEL_RESOURCE_ATTRIBUTES, so one shared value cannot make
    # two processes claim the same identity. The node name therefore has to
    # arrive as its own setting or it is lost; host.name is recomputed from
    # HOSTNAME, which is the node here because the shim runs outside a pod.
    lines.append('export SPACE_K8S_NODE_NAME="$(hostname)"')
    lines.append(
        f'export OTEL_RESOURCE_ATTRIBUTES={shlex.quote(resource)}",host.name=$(hostname),k8s.node.name=$(hostname)"'
    )
    lines.append(f'exec {shlex.quote(binary)} "$@"')
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("json", "helm-set", "shim-wrapper"))
    parser.add_argument(
        "--binary", default="/usr/local/lib/space/containerd-shim-space-v2"
    )
    parser.add_argument("--version", default="local")
    args = parser.parse_args()
    try:
        config = configuration(options())
        if args.command == "json":
            print(json.dumps(config))
        elif args.command == "helm-set":
            print("\n".join(config["helm_set"]))
        else:
            print(shim_wrapper(config, args.binary, args.version), end="")
    except (ValueError, OSError) as error:
        parser.exit(2, f"Kind telemetry configuration: {error}\n")


if __name__ == "__main__":
    main()
