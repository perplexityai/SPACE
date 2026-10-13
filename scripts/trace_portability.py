#!/usr/bin/env python3
"""Prove Rust/Go OTLP export and real cross-language parenting without Datadog.

Runs local protobuf receivers, with no credentials or DD_* variables. Saves raw
OTLP JSON and validates two endpoints, resources, complete trees and failures.
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import tempfile
import time
from pathlib import Path

from trace_fixture import (
    build_fixture,
    build_go_fixture,
    clean_environment,
    fixture_server,
    write_json,
)


# Every producer must report this pinned host, computed per process from
# HOSTNAME rather than inherited from the shared resource attributes.
FIXTURE_HOST = "space-trace-portability-host"


def fixture_env(endpoint, version):
    env = clean_environment()
    # Node and Kubernetes hints outrank HOSTNAME in the resolver; drop any
    # ambient value so the pinned identity is what the producers see.
    for key in ("SPACE_K8S_NODE_NAME", "KUBE_NODE_NAME", "KUBERNETES_SERVICE_HOST"):
        env.pop(key, None)
    env.update(
        {
            "HOSTNAME": FIXTURE_HOST,
            "SPACE_TRACE_PARITY_MODE": "otlp",
            "SPACE_TRACE_EXPORTER": "otlp",
            "SPACE_METRICS_SINK": "off",
            "RUST_LOG": "info",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": endpoint,
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc",
            "OTEL_SERVICE_NAME": "space-trace-portability",
            "OTEL_RESOURCE_ATTRIBUTES": f"service.name=overridden,service.version={version},deployment.environment.name=local,host.name={FIXTURE_HOST},k8s.cluster.name=fixture-cluster",
            "OTEL_TRACES_SAMPLER": "always_on",
            "OTEL_BSP_SCHEDULE_DELAY": "60000",
            "OTEL_BSP_MAX_QUEUE_SIZE": "64",
            "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "32",
        }
    )
    return env


def emit(binary, language, env, log):
    args = (
        [binary, "--exact", "trace_parity_fixture::emit", "--nocapture"]
        if language == "rust"
        else [binary, "-test.run=^TestTraceParityFixture$"]
    )
    with log.open("w") as stream:
        return subprocess.run(
            args,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            timeout=15,
            check=False,
        )


def attributes(values):
    return {item["key"]: next(iter(item["value"].values())) for item in values}


def spans(path):
    result = []
    for line in path.read_text().splitlines() if path.exists() else []:
        for resource in json.loads(line).get("resourceSpans", []):
            identity = attributes(resource["resource"].get("attributes", []))
            for scope in resource.get("scopeSpans", []):
                for span in scope.get("spans", []):
                    result.append((identity, span))
    return result


def validate(rows, version, expected):
    selected = [(r, s) for r, s in rows if r.get("service.version") == version]
    if len(selected) != expected:
        raise ValueError(
            f"{version}: expected {expected} spans, received {len(selected)}"
        )
    by_id = {}
    for resource, span in selected:
        for key, value in {
            "service.name": "space-trace-portability",
            "deployment.environment.name": "local",
            "host.name": FIXTURE_HOST,
            "k8s.cluster.name": "fixture-cluster",
        }.items():
            if resource.get(key) != value:
                raise ValueError(f"{version}: resource {key}={resource.get(key)!r}")
        if (
            len(base64.b64decode(span["traceId"])) != 16
            or len(base64.b64decode(span["spanId"])) != 8
        ):
            raise ValueError("invalid OTLP IDs")
        key = span["traceId"], span["spanId"]
        if key in by_id:
            raise ValueError("duplicate span")
        by_id[key] = span
    roots = []
    linked = []
    for key, span in by_id.items():
        parent = span.get("parentSpanId")
        if parent and any(base64.b64decode(parent)):
            if (key[0], parent) not in by_id:
                raise ValueError(f"{version}: broken parent chain for {span['name']}")
        elif span.get("links"):
            linked.append(span)
        else:
            roots.append(span)
    if len(roots) != 2 or len({span["traceId"] for span in roots}) != 2:
        raise ValueError(f"{version}: expected two complete traces")
    # Saved work starts its own trace with a link to the request that saved it.
    request_roots = {(span["traceId"], span["spanId"]) for span in roots}
    for span in linked:
        targets = {(link["traceId"], link["spanId"]) for link in span["links"]}
        if len(span["links"]) != 1 or not targets <= request_roots:
            raise ValueError(f"{version}: {span['name']} is not linked to a request root")
        if span["traceId"] in {key[0] for key in request_roots}:
            raise ValueError(f"{version}: linked {span['name']} joined its request's trace")
    if sorted(
        attributes(s.get("attributes", [])).get("parity.case") for s in roots
    ) != ["error", "success"]:
        raise ValueError("missing success/error cases")
    for root in roots:
        failed = attributes(root.get("attributes", []))["parity.case"] == "error"
        if (root.get("status", {}).get("code") == "STATUS_CODE_ERROR") != failed:
            raise ValueError("incorrect root error status")
    # Every chain must terminate at a root. A disconnected cycle cannot pass.
    for span in by_id.values():
        key = span["traceId"], span["spanId"]
        visited = set()
        while key in by_id:
            if key in visited:
                raise ValueError("cyclic trace")
            visited.add(key)
            key = key[0], by_id[key].get("parentSpanId", "")
    if version.startswith("mixed"):
        for root in roots:
            same_trace = {
                s["name"]: s for s in by_id.values() if s["traceId"] == root["traceId"]
            }
            if set(same_trace) != {
                "http.server",
                "grpc.client",
                "shim.create",
                "shim.restore",
            }:
                raise ValueError("mixed chain missing required operations")
            for child, parent in (
                ("grpc.client", "http.server"),
                ("shim.create", "grpc.client"),
                ("shim.restore", "http.server"),
            ):
                if (
                    same_trace[child].get("parentSpanId")
                    != same_trace[parent]["spanId"]
                ):
                    raise ValueError(f"mixed chain: incorrect parent of {child}")


def validate_operation_names(rows, version, resource_rule):
    selected = [(r, s) for r, s in rows if r.get("service.version") == version]
    # These are receiver-side expectations for the naming contract, not a
    # second implementation of the exporter's semantic classifier.
    expected = {
        "root": "Naming.Root",
        "default": "internal",
        "internal": "internal",
        "producer": "producer",
        "consumer": "consumer",
        "client": None,
        "server": None,
        "explicit": "MiXeD.Custom",
        "empty": "internal",
        "messaging-producer": None,
        "messaging-consumer": None,
        "incomplete-messaging": "producer",
        "graphql-internal": None,
        "graphql-producer": None,
        "graphql-consumer": None,
        "empty-graphql": "internal",
        "http-client": None,
        "http-server": None,
        "database": None,
        "rpc": None,
        "faas": None,
        "network": None,
        "explicit-messaging": "MiXeD.Messaging",
    }
    if resource_rule in ("operation", "graphql"):
        expected = {
            key: value if key in ("root", "explicit", "explicit-messaging") else None
            for key, value in expected.items()
        }
    elif resource_rule in ("messaging", "messaging-modern"):
        for key in ("producer", "consumer", "incomplete-messaging"):
            expected[key] = None
    by_name = {s["name"].removeprefix("naming."): s for _, s in selected}
    if len(selected) != len(expected) or set(by_name) != set(expected):
        raise ValueError(f"{version}: naming workload missing or duplicated spans")
    root = by_name["root"]
    resource = selected[0][0]
    for name, operation in expected.items():
        span = by_name[name]
        values = attributes(span.get("attributes", []))
        actual = values.get("operation.name") or None
        if actual != operation:
            raise ValueError(
                f"{version}/{name}: expected operation.name={operation!r}, got {actual!r}"
            )
        if name != "root" and (
            span["traceId"] != root["traceId"]
            or span.get("parentSpanId") != root["spanId"]
        ):
            raise ValueError(f"{version}/{name}: naming changed trace parenting")
        if name not in ("root", "default") and (
            values.get("resource.name") != f"naming.{name}"
        ):
            raise ValueError(f"{version}/{name}: resource name changed")
        if name == "messaging-consumer" and resource_rule != "operation":
            operation = "publish" if resource_rule == "messaging" else "process"
            actual = values.get("messaging.operation", resource.get("messaging.operation"))
            if actual != operation or values.get("messaging.operation.type") != "process":
                raise ValueError(
                    f"{version}/{name}: modern messaging alias missing or source changed"
                )
    for kind in ("internal", "producer", "consumer", "client", "server"):
        if by_name[kind]["kind"] != f"SPAN_KIND_{kind.upper()}":
            raise ValueError(f"{version}/{kind}: span kind changed")
    return len(selected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--rust-fixture", type=Path)
    parser.add_argument("--go-fixture", type=Path)
    args = parser.parse_args()
    artifacts = (
        args.artifacts.resolve()
        if args.artifacts
        else Path(tempfile.mkdtemp(prefix="space-trace-portability-"))
    )
    if args.artifacts:
        artifacts.mkdir(parents=True, exist_ok=False)
    rust = (
        str(args.rust_fixture.resolve())
        if args.rust_fixture
        else build_fixture(artifacts, features="otel")
    )
    go = (
        str(args.go_fixture.resolve())
        if args.go_fixture
        else build_go_fixture(artifacts)
    )
    report = {"result": "running", "checks": [], "credentials": False}
    try:
        with (
            fixture_server(go, "receiver", artifacts, "receiver-a") as (
                first,
                first_output,
                _,
            ),
            fixture_server(go, "receiver", artifacts, "receiver-b") as (
                second,
                second_output,
                _,
            ),
        ):
            for index, (endpoint, output) in enumerate(
                ((first, first_output), (second, second_output))
            ):
                for language, binary, count in (("rust", rust, 14), ("go", go, 6)):
                    version = f"{language}-{index}"
                    env = fixture_env(endpoint, version)
                    if language == "go" and index == 0:
                        env["OTEL_RESOURCE_ATTRIBUTES"] += ",malformed_no_equals"
                        env["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] = "\tGRPC "
                        env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = endpoint + "\n"
                    result = emit(
                        binary,
                        language,
                        env,
                        artifacts / f"{version}.log",
                    )
                    if result.returncode != 0:
                        raise RuntimeError(f"{version} failed; see its log")
                    validate(spans(output), version, count)
                    report["checks"].append(version)
                version = f"mixed-{index}"
                env = fixture_env(endpoint, version)
                with fixture_server(go, "mixed", artifacts, version, env) as (
                    service,
                    _,
                    process,
                ):
                    env["SPACE_TRACE_MIXED_ENDPOINT"] = service
                    result = emit(rust, "rust", env, artifacts / f"{version}-rust.log")
                    if result.returncode != 0:
                        raise RuntimeError(f"{version} failed; see its log")
                    process.wait(timeout=10)
                validate(spans(output), version, 8)
                report["checks"].append(version)
            if len(spans(first_output)) != 28 or len(spans(second_output)) != 28:
                raise ValueError("endpoint redirect leaked or duplicated spans")
            for language, binary in (("rust", rust), ("go", go)):
                env = fixture_env(first, "off")
                env["SPACE_TRACE_EXPORTER"] = "off"
                if (
                    emit(
                        binary, language, env, artifacts / f"{language}-off.log"
                    ).returncode
                    != 0
                ):
                    raise RuntimeError("off workload failed")
            if len(spans(first_output)) != 28:
                raise ValueError("off exporter emitted spans")
            report["checks"].append("off")
            for policy, arg, expected in (
                ("always_on", "", 6),
                ("always_off", "", 0),
                ("parentbased_always_off", "", 0),
                ("traceidratio", "0", 0),
                ("traceidratio", "1", 6),
            ):
                version = f"go-sampling-{policy}-{arg}"
                env = fixture_env(first, version)
                env.update(OTEL_TRACES_SAMPLER=policy, OTEL_TRACES_SAMPLER_ARG=arg)
                env["OTEL_BSP_MAX_EXPORT_BATCH_SIZE"] = "1"
                if emit(go, "go", env, artifacts / f"{version}.log").returncode != 0:
                    raise RuntimeError(f"{version} failed")
                selected = [r for r, _ in spans(first_output) if r.get("service.version") == version]
                if len(selected) != expected:
                    raise ValueError(f"{version}: expected {expected} spans, got {len(selected)}")
                if expected:
                    validate(spans(first_output), version, expected)
                # Validate batch-size configuration at the receiver, through the
                # real shim constructor, rather than constructing a bare SDK.
                for line in first_output.read_text().splitlines():
                    batches = json.loads(line).get("resourceSpans", [])
                    count = sum(
                        len(scope.get("spans", []))
                        for batch in batches
                        if attributes(batch["resource"].get("attributes", [])).get("service.version") == version
                        for scope in batch.get("scopeSpans", [])
                    )
                    if count > 1:
                        raise ValueError(f"{version}: batch limit ignored")
                report["checks"].append({"go_sampler": policy, "arg": arg, "spans": expected})
            version = "go-operation-errors"
            env = fixture_env(first, version)
            env["SPACE_TRACE_PARITY_MODE"] = "operation-errors"
            if emit(go, "go", env, artifacts / f"{version}.log").returncode != 0:
                raise RuntimeError("Go operation failure workload failed")
            selected = [s for r, s in spans(first_output) if r.get("service.version") == version]
            actual = sorted((s["name"], s.get("status", {}).get("code") == "STATUS_CODE_ERROR") for s in selected)
            expected = sorted([
                ("shim.create", True), ("shim.sbxmgr.start", True),
                ("shim.dataplane.prepare", False), ("shim.dataplane.prepare", True),
                ("shim.pod_ip_wait", True),
            ])
            if actual != expected:
                raise ValueError(f"Go operation errors misclassified: {actual}")
            report["checks"].append({"go_operation_errors": len(selected)})
            for rule, extra in (
                ("span", ""),
                ("operation", ",operation.name=Resource.Custom"),
                ("graphql", ",graphql.operation.type=query"),
                ("messaging", ",messaging.system=kafka,messaging.operation=publish"),
                ("messaging-modern", ",messaging.system=kafka,messaging.operation.type=publish"),
            ):
                version = f"names-{rule}"
                env = fixture_env(first, version)
                env["SPACE_TRACE_PARITY_MODE"] = "operation-names"
                env["OTEL_RESOURCE_ATTRIBUTES"] += extra
                result = emit(rust, "rust", env, artifacts / f"{version}.log")
                if result.returncode != 0:
                    raise RuntimeError(f"{version} failed; see its log")
                count = validate_operation_names(spans(first_output), version, rule)
                report["checks"].append({"operation_names": rule, "spans": count})
        with fixture_server(
            go,
            "receiver",
            artifacts,
            "stalled-receiver",
            {**clean_environment(), "SPACE_TRACE_RECEIVER_FAIL": "stall"},
        ) as (stalled, _, _):
            for failure, endpoint in (
                ("outage", "http://127.0.0.1:1"),
                ("stall", stalled),
            ):
                for language, binary, budget in (("rust", rust, 7), ("go", go, 1.5)):
                    env = fixture_env(endpoint, failure)
                    started = time.monotonic()
                    result = emit(
                        binary, language, env, artifacts / f"{language}-{failure}.log"
                    )
                    elapsed = time.monotonic() - started
                    if elapsed > budget:
                        raise RuntimeError(
                            f"{language} outage shutdown took {elapsed:.2f}s"
                        )
                    log = (artifacts / f"{language}-{failure}.log").read_text()
                    if "trace parity workload completed" not in log:
                        raise ValueError(
                            f"{language} workload did not complete during {failure}"
                        )
                    if result.returncode != 0 and not (
                        language == "rust" and "flush and shut down exporter" in log
                    ):
                        raise ValueError(f"{language} failed before exporter shutdown")
                    if not any(
                        marker in log.lower()
                        for marker in ("failed", "error", "timed out", "deadline")
                    ):
                        raise ValueError(
                            f"{language} export failure was not observable"
                        )
                    report["checks"].append(
                        {
                            failure: language,
                            "seconds": elapsed,
                            "exit_code": result.returncode,
                        }
                    )
        report["result"] = "pass"
        print(
            f"PASS: neutral receivers, endpoint redirect, real Rust/Go gRPC and serialized parents, off, bounded outage shutdown. Artifacts: {artifacts}"
        )
        return 0
    except (RuntimeError, ValueError, KeyError, subprocess.SubprocessError) as error:
        report.update(result="fail", error=str(error))
        print(f"FAIL: {error}; artifacts: {artifacts}")
        return 1
    finally:
        write_json(artifacts / "run.json", report)


if __name__ == "__main__":
    raise SystemExit(main())
