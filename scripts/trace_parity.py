#!/usr/bin/env python3
"""Compare SPACE's Rust/Go native and upstream OTel exporters through one Agent.

Backend mode emits a small synthetic workload;
Optional temporary retention is restricted to the unique synthetic run.
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

from trace_fixture import (
    ROOT,
    build_fixture,
    build_go_fixture,
    clean_environment,
    run,
    write_json,
)
SERVICE = "space-trace-parity-rust"
MODES = ("datadog", "otlp")
EXPECTED_SPANS = 14
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
FIELDS = (
    "span.kind",
    "http.method",
    "http.route",
    "http.status_code",
    "rpc.system",
    "rpc.service",
    "rpc.method",
    "rpc.grpc.status_code",
    "db.system",
    "db.statement",
    "db.operation.name",
    "job.kind",
    "sandbox_id",
    "template",
    "task_id",
    "restore_mode",
    "pod_ip",
    "result",
    "error.message",
)


def flatten(value, prefix=""):
    result = {}
    for key, item in value.items():
        name = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            result.update(flatten(item, name))
        else:
            result[name] = item
    return result


def fields(span):
    result = {}
    for tag in span.get("tags", []):
        key, sep, value = tag.partition(":")
        if sep:
            result[key] = value
    result.update(flatten(span.get("custom", {})))
    result.update(flatten(span.get("attributes", {})))
    return result


def canonical_trees(
    rows, mode, environment, *, service=SERVICE, expected_spans=EXPECTED_SPANS
):
    """Compare labeled trees, so changing IDs cannot conceal broken parenting."""
    if len(rows) != expected_spans:
        raise ValueError(f"{mode}: expected {expected_spans} spans, found {len(rows)}")
    spans = [row["attributes"] for row in rows]
    by_id = {(str(s["trace_id"]), str(s["span_id"])): s for s in spans}
    if len(by_id) != len(spans):
        raise ValueError(f"{mode}: duplicate span identities")
    roots = []
    children = {}
    for key, span in by_id.items():
        f = fields(span)
        if span.get("service") != service or span.get("env") != environment:
            raise ValueError(f"{mode}: service/environment identity changed")
        if f.get("version", f.get("service.version")) != mode:
            raise ValueError(f"{mode}: version identity missing or changed")
        start = datetime.fromisoformat(span["start_timestamp"])
        end = datetime.fromisoformat(span["end_timestamp"])
        if end < start:
            raise ValueError(f"{mode}: negative span duration")
        parent_id = str(span["parent_id"])
        if parent_id in ("0", "0000000000000000", ""):
            roots.append(key)
        else:
            parent = (str(span["trace_id"]), parent_id)
            if parent not in by_id:
                raise ValueError(f"{mode}: missing parent for {key}")
            children.setdefault(parent, []).append(key)
    # Each request saves a job that runs as its own root, linked to the
    # request; only the request roots carry the workload case.
    jobs = [key for key in roots if "parity.case" not in fields(by_id[key])]
    roots = [key for key in roots if key not in jobs]
    if len(roots) != 2 or len({key[0] for key in roots}) != 2:
        raise ValueError(f"{mode}: expected two distinct complete traces")
    # Only the Rust workload saves a job.
    expected_jobs = 2 if service == SERVICE else 0
    if len(jobs) != expected_jobs or any(
        by_id[key]["resource_name"] != "parity" for key in jobs
    ):
        raise ValueError(f"{mode}: expected one linked job root per request")
    if {key[0] for key in jobs} & {key[0] for key in roots}:
        raise ValueError(f"{mode}: a linked job joined its request's trace")
    cases = Counter(fields(by_id[key]).get("parity.case") for key in roots)
    if cases != Counter({"success": 1, "error": 1}):
        raise ValueError(f"{mode}: missing success/error workload")

    visited = set()

    def visit(key):
        if key in visited:
            raise ValueError(f"{mode}: cyclic span graph")
        visited.add(key)
        span = by_id[key]
        f = fields(span)
        # The application attribute operation.name is only a mapping input.
        # Comparing it instead of the backend operation could hide a rename.
        operation = span.get("operation_name", f.get("operation_name"))
        if not operation:
            raise ValueError(f"{mode}: backend response has no operation name")
        if "status" in span:
            if span["status"] not in ("ok", "error"):
                raise ValueError(f"{mode}: unknown backend status")
            error = span["status"] == "error"
        else:
            error = (
                str(f.get("error", "0")).lower() in ("1", "true")
                or f.get("status") == "error"
            )
        result = {
            "operation": operation,
            "resource": span["resource_name"],
            "type": span.get("type"),
            "host": span.get("host"),
            "error": error,
            "attributes": {name: str(f[name]) for name in FIELDS if name in f},
            "children": sorted(
                (visit(child) for child in children.get(key, [])),
                key=lambda x: json.dumps(x, sort_keys=True),
            ),
        }
        if key in roots and error != (f["parity.case"] == "error"):
            raise ValueError(f"{mode}: incorrect root error classification")
        return result

    trees = sorted(
        (visit(key) for key in roots + jobs),
        key=lambda x: json.dumps(x, sort_keys=True),
    )
    if len(visited) != len(spans):
        raise ValueError(f"{mode}: spans disconnected from roots")
    return trees


def compare(left, right):
    return "\n".join(
        difflib.unified_diff(
            json.dumps(left, indent=2, sort_keys=True).splitlines(),
            json.dumps(right, indent=2, sort_keys=True).splitlines(),
            fromfile="datadog",
            tofile="otlp",
            lineterm="",
        )
    )


class Backend:
    service = SERVICE
    expected_spans = EXPECTED_SPANS
    operation = "http.server"

    def __init__(
        self,
        site,
        *,
        service=SERVICE,
        expected_spans=EXPECTED_SPANS,
        operation="http.server",
    ):
        self.service = service
        self.expected_spans = expected_spans
        self.operation = operation
        self.base = f"https://api.{site}"
        self.headers = {
            "DD-API-KEY": os.environ["DD_API_KEY"],
            "DD-APPLICATION-KEY": os.environ["DD_APP_KEY"],
            "Content-Type": "application/json",
        }

    def request(self, path, body=None, *, method=None):
        request = urllib.request.Request(
            self.base + path,
            headers=self.headers,
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
        )
        for attempt in range(4):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return None if response.status == 204 else json.load(response)
            except urllib.error.HTTPError as error:
                if (
                    method == "POST"
                    or error.code not in (429, 500, 502, 503, 504)
                    or attempt == 3
                ):
                    raise RuntimeError(
                        f"Datadog API returned HTTP {error.code} for {path.split('?')[0]}"
                    ) from None
                time.sleep(min(2**attempt, 8))
        raise AssertionError("unreachable")

    def create_retention(self, name, environment):
        return self.request(
            "/api/v2/apm/config/retention-filters",
            {
                "data": {
                    "type": "apm_retention_filter",
                    "attributes": {
                        "name": name,
                        "enabled": True,
                        "filter": {
                            "query": f"service:{self.service} env:{environment}"
                        },
                        "filter_type": "spans-sampling-processor",
                        "rate": 1.0,
                        "trace_rate": 1.0,
                    },
                }
            },
            method="POST",
        )["data"]

    def delete_retention(self, identifier, name, environment):
        path = "/api/v2/apm/config/retention-filters/" + urllib.parse.quote(
            identifier, safe=""
        )
        current = self.request(path)["data"]["attributes"]
        if (
            current["name"] != name
            or current["filter"]["query"] != f"service:{self.service} env:{environment}"
        ):
            raise RuntimeError(
                "refusing cleanup: test retention filter ownership changed"
            )
        self.request(path, method="DELETE")

    def spans(self, environment, mode, start, extra_query=""):
        attributes = {
            "filter": {
                "from": start,
                "to": "now",
                "query": f"service:{self.service} env:{environment} @version:{mode} {extra_query}".strip(),
            },
            "page": {"limit": 1000},
            "sort": "timestamp",
        }
        rows = []
        cursors = set()
        while True:
            result = self.request(
                "/api/v2/spans/events/search",
                {
                    "data": {"type": "search_request", "attributes": attributes},
                },
            )
            meta = result.get("meta", {})
            if meta.get("status") == "timeout" or meta.get("warnings"):
                raise RuntimeError("Datadog returned partial span-search results")
            rows.extend(result.get("data", []))
            cursor = (meta.get("page") or {}).get("after")
            if not cursor:
                return rows
            if cursor in cursors or len(rows) > 1000:
                raise RuntimeError("unexpected pagination for bounded parity workload")
            cursors.add(cursor)
            attributes["page"]["cursor"] = cursor

    def operational_fields(self, rows, environment, mode, start):
        # The documented search response does not guarantee operation_name or
        # status fields. Resolve their reserved backend meanings by query,
        # rather than trusting similarly named application attributes.
        result = self.request(
            "/api/v2/spans/analytics/aggregate",
            {
                "data": {
                    "type": "aggregate_request",
                    "attributes": {
                        "filter": {
                            "from": start,
                            "to": "now",
                            "query": f"service:{self.service} env:{environment} @version:{mode}",
                        },
                        "compute": [{"aggregation": "count", "type": "total"}],
                        "group_by": [{"facet": "operation_name", "limit": 100}],
                    },
                },
            },
        )
        meta = result.get("meta", {})
        if (
            meta.get("status") == "timeout"
            or meta.get("warnings")
            or (meta.get("page") or {}).get("after")
        ):
            raise RuntimeError("incomplete operation-name aggregation")
        operations = [
            bucket["attributes"]["by"]["operation_name"]
            for bucket in result.get("data", [])
        ]
        if not operations or len(operations) > self.expected_spans:
            raise RuntimeError("unexpected operation-name inventory")
        enriched = deepcopy(rows)
        indexed = {row["id"]: row["attributes"] for row in enriched}
        assigned = set()
        evidence = {"aggregate": result, "operations": {}}
        for operation in operations:
            members = self.spans(
                environment, mode, start, "operation_name:" + json.dumps(operation)
            )
            evidence["operations"][operation] = members
            for member in members:
                identity = member["id"]
                if identity not in indexed or identity in assigned:
                    raise RuntimeError("operation queries disagree with span inventory")
                assigned.add(identity)
                indexed[identity]["operation_name"] = operation
        if assigned != set(indexed):
            raise RuntimeError("missing backend operation names")
        errors = self.spans(environment, mode, start, "status:error")
        evidence["errors"] = errors
        error_ids = {row["id"] for row in errors}
        if not error_ids <= set(indexed):
            raise RuntimeError("error query disagrees with span inventory")
        for identity, span in indexed.items():
            span["status"] = "error" if identity in error_ids else "ok"
        return enriched, evidence

    def counts(self, environment, mode, start):
        counts = {}
        for metric in ("hits", "errors"):
            query = f"sum:trace.{self.operation}.{metric}{{service:{self.service},env:{environment},version:{mode}}}.as_count()"
            result = self.request(
                "/api/v1/query?"
                + urllib.parse.urlencode(
                    {
                        "from": start,
                        "to": int(time.time()),
                        "query": query,
                    }
                )
            )
            if result.get("status") != "ok":
                raise RuntimeError("APM metric query did not complete")
            points = [
                p[1]
                for series in result.get("series", [])
                for p in series.get("pointlist", [])
                if p[1] is not None
            ]
            counts[metric] = sum(points) if points else None
        return counts


def fixture_environment(
    mode, environment, native_endpoint, otlp_endpoint, *, service=SERVICE
):
    # Exporter/sampling overrides in the caller's shell must not alter one lane,
    # and neither may SPACE identity or the trace filter, which outrank the
    # values each lane pins below.
    env = {
        k: v
        for k, v in clean_environment().items()
        if not k.startswith("DD_")
    }
    env.pop("SPACE_TRACE_FILTER", None)
    env.update(
        {
            "SPACE_TRACE_PARITY_MODE": mode,
            "SPACE_TRACE_EXPORTER": mode,
            "SPACE_METRICS_SINK": "off",
            "DD_SERVICE": service,
            "DD_ENV": environment,
            "DD_VERSION": mode,
            "DD_TAGS": "host.name:space-trace-parity",
            "DD_TRACE_AGENT_URL": native_endpoint,
            "DD_TRACE_SAMPLE_RATE": "1",
            "DD_TRACE_PROPAGATION_STYLE": "tracecontext",
            "DD_TRACE_RATE_LIMIT": "10000",
            "DD_REMOTE_CONFIGURATION_ENABLED": "false",
            "DD_INSTRUMENTATION_TELEMETRY_ENABLED": "false",
            "OTEL_SERVICE_NAME": service,
            "OTEL_RESOURCE_ATTRIBUTES": f"deployment.environment.name={environment},service.version={mode},host.name=space-trace-parity",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": otlp_endpoint,
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc",
            "OTEL_TRACES_SAMPLER": "always_on",
            "RUST_LOG": "info",
        }
    )
    return env


def wait_for_agent(name):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        check = subprocess.run(
            ["docker", "exec", name, "agent", "status", "--json"],
            timeout=15,
            check=False,
            capture_output=True,
            text=True,
        )
        if check.returncode == 0:
            status = json.loads(check.stdout)
            if status.get("otlp", {}).get("otlpCollectorStatus") == "Running":
                try:
                    with urllib.request.urlopen(
                        mapped_port(name, 8126) + "/info", timeout=2
                    ) as response:
                        if response.status == 200:
                            return
                except (urllib.error.URLError, TimeoutError):
                    pass
        state = run(
            ["docker", "inspect", "--format", "{{.State.Running}}", name],
            capture_output=True,
        ).stdout.strip()
        if state != "true":
            raise RuntimeError("local Agent stopped before readiness")
        time.sleep(1)
    raise RuntimeError("local Agent did not become healthy in 90 seconds")


def mapped_port(name, port):
    value = run(
        ["docker", "port", name, f"{port}/tcp"], capture_output=True
    ).stdout.strip()
    return "http://" + value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=("rust", "go"), default="rust")
    parser.add_argument("--go-fixture", type=Path, help="Prebuilt shim test executable")
    parser.add_argument(
        "--retain-test-spans",
        action="store_true",
        help="Create and remove a 100%% retention filter for this run only (requires apm_retention_filter_write)",
    )
    parser.add_argument(
        "--publish-only",
        action="store_true",
        help="Publish both workloads and print queries for MCP; no app key required",
    )
    parser.add_argument(
        "--aws-secret-id",
        help="Load missing keys from an AWS Secrets Manager JSON secret (api-key/app-key)",
    )
    parser.add_argument("--aws-profile", help="AWS profile for --aws-secret-id")
    parser.add_argument(
        "--aws-region", default="us-east-1", help="AWS region for --aws-secret-id"
    )
    parser.add_argument(
        "--agent-image",
        required=True,
        help="Exact Agent tag/digest; use the deployed version when proving compatibility",
    )
    parser.add_argument(
        "--site",
        default=os.environ.get("DD_SITE", "datadoghq.com"),
        choices=sorted(SITES),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Backend visibility deadline in seconds",
    )
    parser.add_argument(
        "--artifacts",
        type=Path,
        help="New directory for raw backend responses and comparison",
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        help="Use an already built observability test executable",
    )
    args = parser.parse_args()
    service = "space-trace-parity-" + args.workload
    expected_spans = {"rust": 14, "go": 4}[args.workload]
    modes = MODES
    operation = "shim.create" if args.workload == "go" else "http.server"
    if args.site not in SITES:
        parser.error("DD_SITE must name a supported Datadog site")
    if not re.search(
        r"(?:@sha256:[0-9a-f]{64}|:\d+\.\d+\.\d+(?:[-.][\w.-]+)?)$", args.agent_image
    ):
        parser.error(
            "--agent-image must use a full version tag or digest, not a floating tag"
        )
    required_keys = (
        ("DD_API_KEY",)
        if args.publish_only and not args.retain_test_spans
        else ("DD_API_KEY", "DD_APP_KEY")
    )
    if args.aws_secret_id and any(not os.environ.get(key) for key in required_keys):
        command = [
            "aws",
            "secretsmanager",
            "get-secret-value",
            "--secret-id",
            args.aws_secret_id,
            "--region",
            args.aws_region,
            "--output",
            "json",
        ]
        if args.aws_profile:
            command.extend(["--profile", args.aws_profile])
        # Capture in memory; never write credential values to output or artifacts.
        try:
            secret = json.loads(
                json.loads(run(command, capture_output=True).stdout)["SecretString"]
            )
        except subprocess.CalledProcessError:
            parser.error(
                "could not load the Datadog test secret; verify the AWS profile/session (credential output was withheld)"
            )
        for target, source in (("DD_API_KEY", "api-key"), ("DD_APP_KEY", "app-key")):
            if not os.environ.get(target) and source in secret:
                os.environ[target] = secret[source]
    missing = [key for key in required_keys if not os.environ.get(key)]
    if missing:
        parser.error(
            "missing "
            + ", ".join(missing)
            + "; load the test account credentials before running"
        )
    if not 30 <= args.timeout <= 1800:
        parser.error("--timeout must be between 30 and 1800 seconds")
    run_id = uuid.uuid4().hex[:12]
    environment = "local-trace-parity-" + run_id
    name = "space-trace-parity-" + run_id
    artifacts = (args.artifacts or Path(tempfile.mkdtemp(prefix=name + "-"))).resolve()
    if args.artifacts:
        artifacts.mkdir(parents=True, exist_ok=False)
    backend = (
        None
        if args.publish_only and not args.retain_test_spans
        else Backend(
            args.site,
            service=service,
            expected_spans=expected_spans,
            operation=operation,
        )
    )
    fixture = None
    go_fixture = None
    if args.workload == "rust":
        fixture = (
            str(args.fixture.resolve()) if args.fixture else build_fixture(artifacts)
        )
    if args.workload == "go":
        go_fixture = (
            str(args.go_fixture.resolve())
            if args.go_fixture
            else build_go_fixture(artifacts)
        )
    agent_env = {
        "DD_API_KEY": os.environ["DD_API_KEY"],
        "DD_SITE": args.site,
        "DD_HOSTNAME": "space-trace-parity",
        "DD_APM_ENABLED": "true",
        "DD_APM_NON_LOCAL_TRAFFIC": "true",
        "DD_BIND_HOST": "0.0.0.0",
        "DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT": "0.0.0.0:4317",
        "DD_OTLP_CONFIG_METRICS_ENABLED": "false",
        "DD_OTLP_CONFIG_LOGS_ENABLED": "false",
        "DD_APM_PROBABILISTIC_SAMPLER_ENABLED": "false",
        "DD_APM_TARGET_TPS": "10000",
        "DD_REMOTE_CONFIGURATION_ENABLED": "false",
        "DD_LOGS_ENABLED": "false",
        "DD_PROCESS_CONFIG_PROCESS_COLLECTION_ENABLED": "false",
    }
    created = False
    retention = None
    report = {
        "agent_image": args.agent_image,
        "environment": environment,
        "service": service,
        "workload": args.workload,
        "run_id": run_id,
        "queries": {
            mode: f"service:{service} env:{environment} @version:{mode}"
            for mode in modes
        },
    }
    write_json(artifacts / "run.json", report)
    print(
        f"Artifacts: {artifacts}\nDatadog query: service:{service} env:{environment}",
        flush=True,
    )
    try:
        command = [
            "docker",
            "run",
            "--detach",
            "--name",
            name,
            "--label",
            f"space.trace-parity={run_id}",
            "--publish",
            "127.0.0.1::8126",
            "--publish",
            "127.0.0.1::4317",
        ]
        for key in agent_env:
            command.extend(["--env", key])
        command.append(args.agent_image)
        run(command, env={**os.environ, **agent_env}, capture_output=True)
        created = True
        report["agent_image_id"] = run(
            ["docker", "inspect", "--format", "{{.Image}}", name], capture_output=True
        ).stdout.strip()
        wait_for_agent(name)
        if args.retain_test_spans:
            retention = backend.create_retention(name, environment)
            report["retention_filter_id"] = retention["id"]
            write_json(artifacts / "retention-filter.json", retention)
            write_json(artifacts / "run.json", report)
            print(
                "Temporary retention created for this run; waiting 30s for propagation.",
                flush=True,
            )
            time.sleep(30)
        native_endpoint, otlp_endpoint = (
            mapped_port(name, 8126),
            mapped_port(name, 4317),
        )
        # APM buckets can be timestamped before the individual spans they count.
        # The unique environment excludes unrelated traffic in this wider window.
        started = int(time.time()) - 120
        start_iso = datetime.fromtimestamp(started, UTC).isoformat()
        report.update(
            started=started,
            start_iso=start_iso,
            native_endpoint=native_endpoint,
            otlp_endpoint=otlp_endpoint,
        )
        write_json(artifacts / "run.json", report)
        for mode in modes:
            env = fixture_environment(
                mode, environment, native_endpoint, otlp_endpoint, service=service
            )
            with (artifacts / f"{mode}-fixture.log").open("w") as output:
                if args.workload == "go":
                    run(
                        [go_fixture, "-test.run=^TestTraceParityFixture$"],
                        env=env,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        timeout=60,
                    )
                else:
                    run(
                        [
                            fixture,
                            "--exact",
                            "trace_parity_fixture::emit",
                            "--nocapture",
                        ],
                        env=env,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        timeout=60,
                    )
        if args.publish_only:
            # Leave one Agent export interval before graceful container shutdown.
            time.sleep(10)
            report["result"] = "published-unverified"
            print(
                "Published both workloads. Use run.json queries in MCP; backend parity has not been verified."
            )
            return 0
        deadline = time.monotonic() + args.timeout
        while True:
            rows, counts = {}, {}
            for mode in modes:
                rows[mode] = backend.spans(environment, mode, start_iso)
                counts[mode] = backend.counts(environment, mode, started)
                write_json(artifacts / f"{mode}-spans.json", rows[mode])
            write_json(artifacts / "apm-counts.json", counts)
            print(
                f"Visible spans: { {mode: len(rows[mode]) for mode in modes} }; APM counts: {counts}",
                flush=True,
            )
            if all(
                len(rows[m]) >= expected_spans
                and counts[m] == {"hits": 2.0, "errors": 1.0}
                for m in modes
            ):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "incomplete backend evidence: inspect retention, ingestion, Agent logs, and APM counts; this is not a parity pass"
                )
            time.sleep(min(10, max(0, deadline - time.monotonic())))
        normalized = {}
        for mode in modes:
            enriched, evidence = backend.operational_fields(
                rows[mode], environment, mode, start_iso
            )
            write_json(artifacts / f"{mode}-operational-fields.json", evidence)
            normalized[mode] = canonical_trees(
                enriched,
                mode,
                environment,
                service=service,
                expected_spans=expected_spans,
            )
            write_json(artifacts / f"{mode}-normalized.json", normalized[mode])
        difference = "\n".join(
            f"{modes[0]} vs {mode}:\n{diff}"
            for mode in modes[1:]
            if (diff := compare(normalized[modes[0]], normalized[mode]))
        )
        (artifacts / "parity.diff").write_text(difference + "\n")
        if difference:
            raise RuntimeError(
                "backend semantic differences found; inspect parity.diff"
            )
        report["result"] = "pass"
        print(
            "PASS: complete span trees, operational fields, and root-operation APM counts agree."
        )
    except (
        RuntimeError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
        subprocess.SubprocessError,
        urllib.error.URLError,
    ) as error:
        report["result"] = "fail"
        report["error"] = str(error)
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    finally:
        write_json(artifacts / "run.json", report)
        try:
            if created:
                with (artifacts / "agent.log").open("w") as output:
                    subprocess.run(
                        ["docker", "logs", name],
                        check=False,
                        stdout=output,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                owner = run(
                    [
                        "docker",
                        "inspect",
                        "--format",
                        '{{index .Config.Labels "space.trace-parity"}}',
                        name,
                    ],
                    capture_output=True,
                ).stdout.strip()
                if owner != run_id:
                    raise RuntimeError(
                        "refusing cleanup: local Agent ownership label changed"
                    )
                run(["docker", "stop", "--time", "10", name], capture_output=True)
                run(["docker", "rm", name], capture_output=True)
        finally:
            if retention:
                backend.delete_retention(retention["id"], name, environment)
                report["retention_filter_removed"] = True
                write_json(artifacts / "run.json", report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
