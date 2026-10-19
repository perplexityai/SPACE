#!/usr/bin/env python3
"""Check the span parenting a native Datadog capture received from SPACE.

Only a request's entry span, a server or consumer span, takes its parent from
outside its process. Fail on any other span whose parent another process sent,
or is the context an entry span of its process received: SPACE parented it on
remote context, as a handler extracting its caller's context again does. Fail
on a capture with no SPACE trace; report other uncaptured parents.

Report, without failing, each service's split chunks: spans whose parent is in
another chunk from the same process. With partial flush off, the SPACE default,
datadog-opentelemetry 0.5.2 splits a trace so when a second remote-parented
span joins it in one process (`register_local_root_span`), as concurrent calls
and calls back into an open request do. A span parented on context its own
process saved looks the same in the chunks. Fake Intake's `format=json` does
not decode traces, so this decodes the raw payloads.

Usage: datadog_trace_chunks.py TRACES_RAW_JSON SUMMARY_JSON
"""

import base64
import collections
import json
import subprocess
import sys
from pathlib import Path

# The shim's Go tracer reports this service; every Rust service is `space-*`.
SHIM_SERVICE = "containerd-shim-space-v2"
SPACE_SERVICES = ("space-", SHIM_SERVICE)
ENTRY_KINDS = ("server", "consumer")
EXAMPLES = 20


def varint(buf, i):
    shift = result = 0
    while True:
        result |= (buf[i] & 0x7F) << shift
        shift, i = shift + 7, i + 1
        if buf[i - 1] < 0x80:
            return result, i


def fields(buf):
    """Yield (field number, value) for one protobuf message."""
    i = 0
    while i < len(buf):
        key, i = varint(buf, i)
        if key & 7 == 0:
            value, i = varint(buf, i)
        elif key & 7 == 2:
            length, i = varint(buf, i)
            value, i = buf[i : i + length], i + length
        else:
            size = {1: 8, 5: 4}[key & 7]
            value, i = buf[i : i + size], i + size
        yield key >> 3, value


# Field numbers from the Agent's pkg/proto/datadog/trace protobufs.
def span(buf, container):
    s = {"parent_id": 0, "meta": {}}
    names = (None, "service", "operation", "name", "trace", "id", "parent_id")
    for number, raw in fields(buf):
        if number <= 6:
            s[names[number]] = raw.decode() if number <= 3 else raw
        elif number == 10:
            entry = dict(fields(raw))
            s["meta"][entry[1].decode()] = entry.get(2, b"").decode()
    # The Rust services stamp host and PID; a container identifies the rest.
    s["process"] = (s["service"], s["meta"].get("service.instance.id") or container)
    s["label"] = f"{s['service']} {s['operation']} {s['name'][:60]}"
    s["entry"] = s["meta"].get("span.kind") in ENTRY_KINDS
    return s


def chunks(path):
    """Yield the spans of every chunk in a Fake Intake raw capture."""
    for payload in json.loads(Path(path).read_text())["payloads"]:
        if payload.get("encoding") != "zstd":
            raise ValueError(f"unsupported trace encoding {payload.get('encoding')!r}")
        data = base64.b64decode(payload["data"])
        data = subprocess.run(
            ["zstd", "-dc"], input=data, capture_output=True, check=True
        )
        for tracer_payload in (v for n, v in fields(data.stdout) if n == 5):
            tracer = list(fields(tracer_payload))
            container = next((v.decode() for n, v in tracer if n == 1), "")
            for raw in (v for n, v in tracer if n == 6):
                yield [span(s, container) for n, s in fields(raw) if n == 3]


def check(path):
    traces = collections.defaultdict(list)
    for spans in filter(None, chunks(path)):
        traces[spans[0]["trace"]].append(spans)
    remote, inconclusive, splits, space_traces = [], [], [], 0
    for trace in traces.values():
        # Parents may be any captured span; only SPACE's spans are judged.
        spans = [(index, s) for index, c in enumerate(trace) for s in c]
        sent = {s["id"]: (s, index) for index, s in spans}
        spans = [(i, s) for i, s in spans if s["service"].startswith(SPACE_SERVICES)]
        space_traces += bool(spans)
        # Context each process's entry spans received from outside it.
        received = {(s["process"], s["parent_id"]): s for _, s in spans if s["entry"]}
        for index, s in spans:
            if s["entry"] or not s["parent_id"]:
                continue
            parent, parent_chunk = sent.get(s["parent_id"], (None, None))
            entry = received.get((s["process"], s["parent_id"]))
            if parent is None and entry:
                remote.append(f"{s['label']} <- the caller of {entry['label']}")
            elif parent is None:
                inconclusive.append(s["label"])
            elif parent["process"] != s["process"]:
                remote.append(f"{s['label']} <- {parent['label']}")
            elif parent_chunk != index:
                splits.append(s)
    top = collections.Counter
    roots = top(s["label"] for s in splits)
    return {
        "traces": space_traces,
        "remote_parented_spans": dict(top(remote).most_common(EXAMPLES)),
        "inconclusive_parents": dict(top(inconclusive).most_common(EXAMPLES)),
        "split_chunks_by_service": dict(top(s["service"] for s in splits)),
        "split_chunk_roots": dict(roots.most_common(EXAMPLES)),
    }


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__.rsplit("Usage: ", 1)[1].strip())
    summary = check(sys.argv[1])
    Path(sys.argv[2]).write_text(json.dumps(summary, indent=2) + "\n")
    print(
        f"trace parenting: {summary['traces']} SPACE traces; split chunks by service, "
        "from datadog-opentelemetry 0.5.2 register_local_root_span, not failed: "
        f"{summary['split_chunks_by_service']}; parents not captured: "
        f"{summary['inconclusive_parents']}"
    )
    failed = summary["remote_parented_spans"]
    if failed or not summary["traces"]:
        reason = f"non-entry spans parented on remote context {failed}"
        reason = reason if failed else "the capture holds no SPACE trace"
        sys.exit(f"ERROR: Datadog trace parenting: {reason}; see {sys.argv[2]}")


if __name__ == "__main__":
    main()
