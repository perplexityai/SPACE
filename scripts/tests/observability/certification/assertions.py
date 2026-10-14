"""Assertions over stored backend evidence, independent of cluster orchestration."""

import base64
import math
import re


def series(response):
    if response.get("status") != "success":
        raise ValueError("backend returned an error, not an empty successful result")
    return response["data"]["result"]


def samples(row):
    values = [(float(t), float(v)) for t, v in row.get("values", [])]
    if any(not math.isfinite(v) for _, v in values):
        raise ValueError("non-finite metric sample")
    if any(a[0] >= b[0] for a, b in zip(values, values[1:])):
        raise ValueError("metric timestamps must increase")
    return values


def counter_monotonic(rows):
    # Range-vector queries return stored points, not repeated lookback samples.
    if not rows:
        return False
    increased = False
    for row in rows:
        points = samples(row)
        if len(points) < 3 or points[-1][0] - points[0][0] < 20:
            return False
        if any(v < 0 for _, v in points):
            return False
        if any(a[1] > b[1] for a, b in zip(points, points[1:])):
            return False
        increased |= points[-1][1] > points[0][1]
    return increased


def native_histograms(rows):
    return bool(rows) and all(
        any(
            float(h["count"]) > 0 and h.get("buckets")
            for _, h in row.get("histograms", [])
        )
        for row in rows
    )


def gauge_lifecycle(rows, active_start, active_end, deleted_at):
    # An absent/stale series is not a zero. Require a fresh post-delete sample
    # on the same identity that was positive while the sandbox existed.
    return any(
        any(active_start <= t <= active_end and v > 0 for t, v in samples(row))
        and any(t >= deleted_at and v == 0 for t, v in samples(row))
        for row in rows
    )


def identities(rows, service, version, cluster):
    present = bool(rows)
    identity = present and all(
        r["metric"].get("service_name") == service
        and r["metric"].get("service_instance_id")
        and r["metric"].get("service_version") == version
        for r in rows
    )
    emitter = present and all(r["metric"].get("service") == service for r in rows)
    cluster_ok = present and all(
        r["metric"].get("k8s_cluster_name") == cluster for r in rows
    )
    return bool(identity), emitter, cluster_ok


def distinct_producers(by_service):
    owners = {}
    for service, rows in by_service.items():
        if not rows:
            return False
        for row in rows:
            instance = row["metric"].get("service_instance_id")
            if not instance or owners.setdefault(instance, service) != service:
                return False
    return bool(owners)


def wire_id(value, size):
    if re.fullmatch(r"[0-9a-fA-F]{%d}" % (size * 2), value):
        return value.lower()
    raw = base64.b64decode(value, validate=True)
    if len(raw) != size:
        raise ValueError("incorrect trace/span ID length")
    return raw.hex()


def trace_evidence(trace, trace_id, services, version):
    spans = {}
    for batch in trace.get("batches", trace.get("resourceSpans", [])):
        attrs = {
            a["key"]: a["value"].get("stringValue")
            for a in batch["resource"].get("attributes", [])
        }
        for scope in batch.get("scopeSpans", []):
            for span in scope.get("spans", []):
                sid = wire_id(span["spanId"], 8)
                if sid in spans or wire_id(span["traceId"], 16) != trace_id.lower():
                    raise ValueError("duplicate span or mixed trace IDs")
                start, end = (
                    int(span["startTimeUnixNano"]),
                    int(span["endTimeUnixNano"]),
                )
                if end < start or start <= 0:
                    raise ValueError("invalid span interval")
                parent = span.get("parentSpanId")
                spans[sid] = (attrs, wire_id(parent, 8) if parent else None, span)
    seen = {
        a.get("service.name")
        for a, _, _ in spans.values()
        if a.get("service.version") == version
    } & set(services)
    edges = []
    for sid, (attrs, parent, span) in spans.items():
        visited = {sid}
        cursor = parent
        while cursor in spans:
            if cursor in visited:
                raise ValueError("cyclic span parenting")
            visited.add(cursor)
            cursor = spans[cursor][1]
        if parent not in spans:
            continue
        parent_attrs, _, parent_span = spans[parent]
        left, right = parent_attrs.get("service.name"), attrs.get("service.name")
        if (
            left != right
            and {left, right} <= seen
            and attrs.get("service.version")
            == parent_attrs.get("service.version")
            == version
            and parent_span.get("kind") in (3, "SPAN_KIND_CLIENT")
            and span.get("kind") in (2, "SPAN_KIND_SERVER")
        ):
            edges.append(
                {
                    "trace_id": trace_id,
                    "parent_service": left,
                    "child_service": right,
                    "parent_span_id": parent,
                    "span_id": sid,
                }
            )
    return seen, edges


def verdict(passed, expected_failure=False):
    # An expected failure that starts passing is reported as pass, never hidden.
    return "pass" if passed else "xfail" if expected_failure else "fail"
