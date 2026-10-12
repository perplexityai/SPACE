#!/usr/bin/env python3
"""Shared, non-secret Kind telemetry configuration for Tilt, bootstrap and shims."""

import argparse
import json
import os
import re
import shlex
import socket
import urllib.parse
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
}
PHASES = {"before-main", "after-datadog", "after-otel"}
# Each mode's (SPACE_METRICS_SINK, SPACE_TRACE_EXPORTER), for the Rust
# services and the Go shim alike.
MODES = {
    "datadog": ("datadog", "datadog"),
    "otel": ("otlp", "otlp"),
    "off": ("off", "off"),
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
        raise ValueError("SPACE_KIND_TELEMETRY_MODE must be otel, datadog, or off")
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
    if endpoint is None and mode == "datadog":
        endpoint = "http://127.0.0.1:4317"
    if endpoint is not None:
        rejection = otlp_endpoint_rejection(endpoint)
        if rejection:
            raise ValueError(f"SPACE_KIND_SHIM_OTLP_ENDPOINT {rejection}")
    version = f"{revision}-{phase}" if revision and phase else revision
    metrics, traces = MODES[mode]
    config = {
        "mode": mode,
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
    # The host runtime-supervisor's node environment (space-node-manager-local)
    # needs the Collectors at addresses the node reaches, as the shim does.
    config["helm_set"] += [
        f"telemetry.nodeOtlpMetricsEndpoint={collector_endpoint(config, 'metrics')}",
        f"telemetry.nodeOtlpTracesEndpoint={collector_endpoint(config, 'traces')}",
    ]
    return config


# Each signal has its own Collector Service. containerd runs the shim, and
# systemd the runtime-supervisor, on the Kind node, which has no cluster DNS, so
# both name each Service by the fixed ClusterIP that chart's profiles/kind.yaml
# pins as collector.clusterIP; change the two together. Because it is fixed
# rather than looked up, the shim wrapper and the supervisor's node environment
# can be written before the Collectors exist and stay right when they are
# reinstalled.
NODE_COLLECTORS = {"metrics": "10.96.0.20", "traces": "10.96.0.21"}


def collector_endpoint(config, signal):
    """A node-reachable OTLP endpoint for one signal's Collector."""
    if config["shim_endpoint"] is not None:
        return config["shim_endpoint"]
    return f"http://{NODE_COLLECTORS[signal]}:4317"


def shim_endpoint(config):
    return collector_endpoint(config, "traces")


def exports_otlp(config):
    """Whether the shim sends either signal over OTLP in this mode."""
    return "otlp" in MODES[config["mode"]]


def shim_wrapper(config, binary, fallback_version):
    version = config["version"] or fallback_version
    metrics, traces = MODES[config["mode"]]
    values = {
        "SPACE_CLUSTER_ID": config["cluster_id"],
        "SPACE_TRACE_EXPORTER": traces,
        "DD_AGENT_HOST": "127.0.0.1",
        "DD_ENV": config["environment"],
        "DD_SERVICE": "containerd-shim-space-v2",
        "DD_VERSION": version,
        # The shim reads SPACE_METRICS_SINK too. Left unset it defaults to
        # auto -> datadog, which would keep the shim on native metrics while
        # every workload moved to OTLP: wrong, silently.
        "SPACE_METRICS_SINK": metrics,
    }
    # `off` exports nothing, so it names no receiver either.
    if traces != "off":
        values["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = shim_endpoint(config)
        values["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] = "grpc"
    if exports_otlp(config):
        # The SPACE and OTel identity settings outrank the DD_* ones, so
        # setting them in a Datadog-only mode would change the tags this shim
        # emits: its DogStatsD version tag carries the shim's build version,
        # and SPACE_VERSION would replace it with the wrapper's. Only a mode
        # that actually exports OTLP needs them.
        values["SPACE_ENV"] = config["environment"]
        values["SPACE_VERSION"] = version
        values["OTEL_SERVICE_NAME"] = "containerd-shim-space-v2"
    if metrics == "otlp":
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
