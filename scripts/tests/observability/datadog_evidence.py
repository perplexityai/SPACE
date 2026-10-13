#!/usr/bin/env python3
"""Record a non-secret Kind comparison window and scoped Datadog MCP query plan."""

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parents[3] / "infra/space/kind/scripts")
)

from datadog_backend import ROOT, kube, plan, verify
from telemetry_config import options

# The control-plane Deployments plus every node-service DaemonSet, from the
# list bootstrap and Tilt deploy (the Kind-only operator shell emits nothing).
WORKLOADS = {
    "space-api",
    "space-internal-api",
    "space-data-plane",
    "space-cluster-controller",
} | {
    name
    for name in subprocess.run(
        ["bash", str(ROOT / "infra/space/kind/lib/node-services.sh"), "daemonsets"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    if name != "space-node-debug"
}
# Every telemetry setting worth recording alongside a capture. A name absent
# from a pod is simply absent from the evidence, so this errs toward listing
# both the shared and per-signal exporter variables: the charts emit the
# per-signal ones, and a deployment may still set the shared ones.
ENV_KEYS = {
    "DD_ENV",
    "DD_VERSION",
    "DD_TAGS",
    "SPACE_SERVICE_NAME",
    "SPACE_ENV",
    "SPACE_VERSION",
    "SPACE_CLUSTER_ID",
    "SPACE_METRICS_SINK",
    "SPACE_TRACE_EXPORTER",
    "SPACE_TRACE_FILTER",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SERVICE_NAME",
    "OTEL_TRACES_SAMPLER",
    "OTEL_METRIC_EXPORT_INTERVAL",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
}


def now():
    return datetime.now(UTC).isoformat()


def write_new(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def git(*args):
    return subprocess.run(
        ["git", "-C", str(ROOT), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def snapshot(config):
    verification = verify(config)
    workloads = json.loads(
        kube(config, "get", "deploy,ds", "-o", "json", namespace="space")
    )["items"]
    workloads = [w for w in workloads if w["metadata"]["name"] in WORKLOADS]
    if {w["metadata"]["name"] for w in workloads} != WORKLOADS:
        raise ValueError("Evidence requires every Kind harness workload")
    for workload in workloads:
        kube(
            config,
            "rollout",
            "status",
            f"{workload['kind']}/{workload['metadata']['name']}",
            "--timeout=300s",
            namespace="space",
        )
    all_pods = json.loads(kube(config, "get", "pods", "-o", "json", namespace="space"))[
        "items"
    ]
    records = []
    for workload in workloads:
        labels = workload["spec"]["selector"]["matchLabels"]
        pods = [
            p
            for p in all_pods
            if all(
                p["metadata"].get("labels", {}).get(k) == v for k, v in labels.items()
            )
        ]
        if not pods:
            raise ValueError(f"No running pods for {workload['metadata']['name']}")
        for pod in pods:
            if pod["metadata"].get("deletionTimestamp"):
                raise ValueError(
                    "Old SPACE pods still terminating; wait before capturing"
                )
            containers = []
            for container in pod["spec"]["containers"]:
                env = {
                    e["name"]: e.get("value", e.get("valueFrom"))
                    for e in container.get("env", [])
                    if e["name"] in ENV_KEYS
                }
                if (
                    env.get("DD_ENV") != config["environment"]
                    or env.get("DD_VERSION") != config["version"]
                ):
                    raise ValueError(
                        f"Stale SPACE identity in {pod['metadata']['name']}/{container['name']}"
                    )
                containers.append(
                    {
                        "name": container["name"],
                        "image": container["image"],
                        "environment": env,
                    }
                )
            records.append(
                {
                    "workload": workload["metadata"]["name"],
                    "pod": pod["metadata"]["name"],
                    "uid": pod["metadata"]["uid"],
                    "node": pod["spec"].get("nodeName"),
                    "containers": containers,
                    "image_ids": [
                        c.get("imageID")
                        for c in pod.get("status", {}).get("containerStatuses", [])
                    ],
                }
            )
    identities = {
        "kube_system_uid": kube(
            config, "get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"
        ),
        "postgres_uid": kube(
            config,
            "get",
            "pod",
            "space-postgres-0",
            "-o",
            "jsonpath={.metadata.uid}",
            namespace="space",
        ),
        "seed_job_uid": kube(
            config,
            "get",
            "job",
            "-l",
            "app.kubernetes.io/name=space-seed-publish",
            "-o",
            "jsonpath={.items[0].metadata.uid}",
            namespace="space",
        ),
    }
    agents = json.loads(
        kube(
            config,
            "get",
            "pods",
            "-l",
            # Real pods are labelled instance=datadog-agent-agent and
            # -cluster-agent; a bare instance match finds nothing. Same
            # selector verify() uses.
            "app.kubernetes.io/name=datadog-agent,"
            "app.kubernetes.io/component in (agent,cluster-agent)",
            "-o",
            "json",
        )
    )["items"]
    if not agents:
        # Provenance is this manifest's whole purpose: an empty list means the
        # selector missed, not that no Agent ran.
        raise ValueError(
            "no Agent pods matched; evidence cannot record which build "
            "produced these payloads"
        )
    return {
        "verification": verification,
        "workloads": records,
        "identities": identities,
        "agent_images": sorted(
            {c["image"] for p in agents for c in p["spec"]["containers"]}
        ),
        "agent_image_ids": sorted(
            {
                c.get("imageID", "")
                for p in agents
                for c in p.get("status", {}).get("containerStatuses", [])
            }
        ),
    }


def queries(manifest, ended_at):
    config = manifest["configuration"]
    metric_scope = f"env:{config['environment']},version:{config['version']}"
    search_scope = metric_scope.replace(",", " ")
    return {
        "status": "query-plan-only; attach raw MCP responses before claiming backend validation",
        "from": manifest["started_at"],
        "to": ended_at,
        "site": config["site"],
        "metric_scope": metric_scope,
        "logs_and_spans_scope": search_scope,
        "metric_queries": {
            "gauge": f"avg:<observed_gauge>{{{metric_scope}}} by {{service}}",
            "distribution": f"p95:<observed_distribution>{{{metric_scope}}} by {{service}}",
            "counter": f"sum:<observed_counter>{{{metric_scope}}}.as_count()",
        },
        "log_query": f"{search_scope} kube_namespace:space",
        "span_query": search_scope,
        "instructions": [
            "Use read-only Datadog MCP metric discovery/query, log search, span search and trace lookup tools.",
            "Replace placeholders with names observed in this run; verify counter types before comparing totals.",
            "Same-name RATE/COUNT experiments require Fake Intake or a separately approved SaaS isolation plan.",
            "Apply both identity filters and this exact UTC window to every query; save queries and full raw responses.",
            "Record aggregation, metric type/unit, tags, ingestion delay, sampling/indexing policy and retention limits.",
            "For a fresh sandbox, save one trace with Rust and Go spans and check trace_id/parent_id connectivity.",
            "Missing results are not proof of absent emission; distinguish export, ingestion, retention and query gaps.",
            "Do not create or modify shared monitors, dashboards, retention filters or metric configurations.",
        ],
    }


def begin(output, config):
    if output.exists():
        raise ValueError("Refusing to overwrite an existing evidence directory")
    if config["backend"] != "saas":
        raise ValueError("Use capture-kind-fakeintake.sh for local payload evidence")
    state = snapshot(config)
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    manifest = {
        "configuration": config,
        "checkout_sha": git("rev-parse", "HEAD"),
        "dirty_paths": git("status", "--short"),
        "started_at": now(),
        "initial_state": state,
        "backend_ingestion_verified": False,
    }
    write_new(output / "manifest.json", manifest)
    return manifest


def finish(output, config):
    if (output / "query-plan.json").exists() or (output / "completion.json").exists():
        raise ValueError("Refusing to overwrite a completed evidence window")
    manifest = json.loads((output / "manifest.json").read_text())
    if config != manifest["configuration"]:
        raise ValueError("Configuration changed during the comparison window")
    ended_at = now()
    state = snapshot(config)
    if state["identities"] != manifest["initial_state"]["identities"]:
        raise ValueError(
            "Cluster, Postgres or seed identity changed during the comparison window"
        )
    write_new(output / "completion.json", {"ended_at": ended_at, "final_state": state})
    result = queries(manifest, ended_at)
    write_new(output / "query-plan.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("begin", "finish"))
    parser.add_argument(
        "output", type=Path, help="new per-phase evidence directory below target/"
    )
    args = parser.parse_args()
    try:
        result = {"begin": begin, "finish": finish}[args.command](
            args.output, plan(options())
        )
        print(json.dumps(result, indent=2))
    except (
        ValueError,
        OSError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
    ) as error:
        message = (
            f"{error.cmd[0]} failed; no backend verification recorded"
            if isinstance(error, subprocess.SubprocessError)
            else str(error)
        )
        parser.exit(2, f"Kind Datadog evidence: {message}\n")


if __name__ == "__main__":
    main()
