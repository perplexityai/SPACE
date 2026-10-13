#!/usr/bin/env python3
"""Run SPACE's existing parity fixture and verify records fetched from Tempo.

Operator integration check, not a new application exporter or a routine CI test.
The source checkout must contain scripts/trace_fixture.py and its Rust fixture.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen


def attributes(items):
    return {item["key"]: item["value"] for item in items}


def fetch(base, path):
    with urlopen(base + path, timeout=10) as response:
        return json.load(response)


def check_trace(trace, trace_id, run):
    spans = []
    for batch in trace["batches"]:
        resource = attributes(batch["resource"]["attributes"])
        for key, expected in {
            "service.name": "space-oss-trace-fixture",
            "service.version": run,
            "deployment.environment.name": "oss-synthetic",
            "k8s.cluster.name": "kind-local",
        }.items():
            assert resource[key] == {"stringValue": expected}, (key, resource)
        spans.extend(span for scope in batch["scopeSpans"] for span in scope["spans"])
    roots = [span for span in spans if not span.get("parentSpanId")]
    assert len(roots) == 1
    root = roots[0]
    # The saved job runs in a trace of its own, linked to its request's root.
    if (root["name"], root["kind"]) == ("job.parity", "SPAN_KIND_INTERNAL"):
        assert len(spans) == 1, len(spans)
        assert base64.b64decode(root["traceId"], validate=True).hex() == trace_id
        links = root.get("links", [])
        assert len(links) == 1, links
        return {
            "trace_id": trace_id,
            "job": True,
            "linked_trace_id": base64.b64decode(links[0]["traceId"], validate=True).hex(),
            "linked_span_id": base64.b64decode(links[0]["spanId"], validate=True).hex(),
        }
    assert len(spans) == 6, len(spans)
    ids = {span["spanId"] for span in spans}
    assert len(ids) == 6
    case = attributes(root["attributes"])["parity.case"]["stringValue"]
    assert case in ("success", "error")
    root_status = "STATUS_CODE_ERROR" if case == "error" else "STATUS_CODE_OK"
    by_role = {(span["name"], span["kind"]): span for span in spans}
    assert len(by_role) == 6
    server = "SPAN_KIND_SERVER"
    client = "SPAN_KIND_CLIENT"
    root_role = ("POST /trace-parity", server)
    grpc_client = ("space.parity.v1.Worker/Execute", client)
    grpc_server = ("space.parity.v1.Worker/Execute", server)
    parents = {
        root_role: None,
        grpc_client: root_role,
        grpc_server: grpc_client,
        ("SELECT 1", client): grpc_server,
        ("db.pool.acquire", client): grpc_server,
        ("GET parity", client): root_role,
    }
    assert set(by_role) == set(parents), set(by_role)
    for role, parent in parents.items():
        span = by_role[role]
        assert base64.b64decode(span["traceId"], validate=True).hex() == trace_id
        assert len(base64.b64decode(span["spanId"], validate=True)) == 8
        assert int(span["endTimeUnixNano"]) >= int(span["startTimeUnixNano"]) > 0
        assert span.get("parentSpanId") == (by_role[parent]["spanId"] if parent else None)
    for role in (root_role, grpc_client, grpc_server):
        assert by_role[role]["status"]["code"] == root_status
    assert sum(span.get("status", {}).get("code") == "STATUS_CODE_ERROR" for span in spans) == (3 if case == "error" else 0)
    assert attributes(root["attributes"])["http.response.status_code"] == {"intValue": "500" if case == "error" else "200"}
    assert attributes(root["attributes"])["sandbox_id"] == {"stringValue": "parity-sandbox"}
    assert attributes(by_role[("SELECT 1", client)]["attributes"])["db.rows_returned"] == {"intValue": "1"}
    assert attributes(by_role[("SELECT 1", client)]["attributes"])["db.rows_affected"] == {"intValue": "0"}
    assert attributes(by_role[grpc_server]["attributes"])["operation.name"] == {"stringValue": "grpc.server"}
    return {"trace_id": trace_id, "root_span_id": base64.b64decode(root["spanId"], validate=True).hex(), "case": case, "spans": len(spans), "errors": 3 if case == "error" else 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--run", required=True, help="Unique synthetic version, letters/digits/dash only")
    parser.add_argument("--endpoint", default="http://127.0.0.1:14317")
    parser.add_argument("--tempo", default="http://127.0.0.1:13200")
    args = parser.parse_args()
    assert re.fullmatch(r"[A-Za-z0-9-]+", args.run), "Unsafe run ID"
    for endpoint in (args.endpoint, args.tempo):
        parsed = urlparse(endpoint)
        assert parsed.scheme == "http" and parsed.hostname == "127.0.0.1", "Loopback tunnels only"
        assert not parsed.username and not parsed.password and parsed.path in ("", "/")
    harness = Path(__file__).resolve().parents[4] / "infra/space/kind/traces"
    subprocess.run(["bash", str(harness / "local.sh"), "status"], check=True)
    source = args.source.resolve()
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=source), "Source checkout must be clean"
    sys.path.insert(0, str(source / "scripts"))
    import trace_fixture

    os.umask(0o077)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary = trace_fixture.build_fixture(output)
    started = int(time.time()) - 120
    env = trace_fixture.clean_environment()
    env.update(
        SPACE_TRACE_EXPORTER="otlp",
        SPACE_METRICS_SINK="journald",
        SPACE_TRACE_PARITY_MODE="basic",
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=args.endpoint,
        OTEL_TRACES_SAMPLER="always_on",
        OTEL_SERVICE_NAME="space-oss-trace-fixture",
        OTEL_RESOURCE_ATTRIBUTES=f"deployment.environment.name=oss-synthetic,service.version={args.run},k8s.cluster.name=kind-local",
        RUST_LOG="info",
    )
    with (output / "fixture.log").open("w") as log:
        subprocess.run([binary, "--exact", "trace_parity_fixture::emit", "--nocapture"], env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=30)
    query = f'{{resource.service.name="space-oss-trace-fixture" && resource.service.version="{args.run}"}}'
    deadline = time.monotonic() + 90
    while True:
        # Tempo 3 excludes the newest 30s even with explicit bounds. A wider
        # window avoids an invalid start > effective end while we poll.
        search = fetch(args.tempo, "/api/search?" + urlencode({"q": query, "start": started, "end": int(time.time()) + 1, "limit": 20}))
        trace_fixture.write_json(output / "search.json", search)
        found = search.get("traces", [])
        if len(found) == 4:
            break
        assert time.monotonic() < deadline, f"Expected 4 traces; got {len(found)}"
        time.sleep(2)
    checked = []
    for match in found:
        trace_id = match["traceID"]
        trace = fetch(args.tempo, "/api/traces/" + trace_id)
        trace_fixture.write_json(output / f"{trace_id}.json", trace)
        checked.append(check_trace(trace, trace_id, args.run))
    requests = [item for item in checked if not item.get("job")]
    jobs = [item for item in checked if item.get("job")]
    assert sorted(item["case"] for item in requests) == ["error", "success"]
    assert sorted((item["linked_trace_id"], item["linked_span_id"]) for item in jobs) == sorted(
        (item["trace_id"], item["root_span_id"]) for item in requests
    ), "each job must link to one request root"
    summary = {"source": str(source), "revision": revision, "fixture": binary, "fixture_sha256": hashlib.file_digest(open(binary, "rb"), "sha256").hexdigest(), "features": "datadog-otel", "run": args.run, "traceql": query, "started": started, "ended": int(time.time()), "results": checked}
    trace_fixture.write_json(output / "summary.json", summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
