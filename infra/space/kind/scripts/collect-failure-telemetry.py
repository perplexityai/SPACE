#!/usr/bin/env python3
"""Save the local backends' telemetry for failed Kind smoke scenarios.

The Kind bench's Tempo, Loki and Prometheus die with the CI runner, and with
them the traces and lines that would explain a failure. Given the typed smoke
log, this reads each failed scenario's sandbox ids from its `RESULT` evidence
and writes, below the output directory:

- each sandbox's Tempo searches (`{span.sandbox_id="<uuid>"}`, its failed
  traces first) and the traces they name, fetched by ID;
- each sandbox's Loki lines that mention it;
- a Prometheus snapshot of the `space_*` series over the run, summed by
  service.

With no failed scenario to name a sandbox (a step that failed before or
between scenarios), it saves the window's error traces and the Prometheus
snapshot instead. Every read is bounded in count, bytes and time, a
backend that does not answer is recorded and skipped, and the script exits 0
whatever happens: it gathers evidence and never decides a verdict.

Usage: collect-failure-telemetry.py --out DIR [--log FILE ...] [--since EPOCH]
"""

from __future__ import annotations

import argparse
import calendar
import json
import re
import socket
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from contextlib import suppress
from pathlib import Path

RESULT_PREFIX = "RESULT: "
PASSING_VERDICTS = {"pass", "warn"}
# The summary repeats each failed scenario's sandbox, also for a scenario
# whose RESULT line was lost.
SUMMARY_SANDBOX = re.compile(r"^\s+sandbox_id: (\S+)\s*$")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

KIND_CONTEXT = "kind-space-local"
# The localhost forwards both bring-up paths keep open, and the Service each
# forwards to, for when they are gone.
BACKENDS = {
    "tempo": ("http://127.0.0.1:13200", "space-traces", "svc/tempo", 3200, "/ready"),
    "loki": ("http://127.0.0.1:13101", "space-logs", "svc/loki", 3100, "/ready"),
    "prometheus": ("http://127.0.0.1:19090", "space-metrics", "svc/prometheus", 9090, "/-/ready"),
}

MAX_SANDBOXES = 8
MAX_TRACES_PER_SANDBOX = 25
MAX_ERROR_TRACES = 10
LOKI_LINE_LIMIT = 1000
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BYTES = 48 * 1024 * 1024
PROMETHEUS_POINTS = 120
MAX_METRICS = 400
REQUEST_TIMEOUT_SECONDS = 20.0
# Tempo search withholds the newest 30 s whatever `end` says.
TEMPO_SEARCH_CUTOFF_SECONDS = 30
DEFAULT_WINDOW_SECONDS = 3 * 3600


def canonical_sandbox_id(value: object) -> str | None:
    """The bare UUID, the form every service's `sandbox_id` carries."""
    if not isinstance(value, str):
        return None
    candidate = value.strip().lower().removeprefix("sbx_")
    return candidate if UUID.match(candidate) else None


def failed_sandboxes(lines: list[str]) -> dict[str, list[str]]:
    """Each failed scenario's sandbox ids, in the order the log names them."""
    found: dict[str, list[str]] = {}

    def add(scenario: str, value: object) -> None:
        sandbox_id = canonical_sandbox_id(value)
        if sandbox_id is not None and sandbox_id not in found.setdefault(scenario, []):
            found[scenario].append(sandbox_id)

    scenario = "unknown"
    for line in lines:
        if line.startswith("=== RUN "):
            scenario = line.removeprefix("=== RUN ").strip(" =\n")
        if line.startswith(RESULT_PREFIX):
            try:
                report = json.loads(line.removeprefix(RESULT_PREFIX))
            except json.JSONDecodeError:
                continue
            if not isinstance(report, dict) or report.get("verdict") in PASSING_VERDICTS:
                continue
            name = str(report.get("scenario_id") or scenario)
            found.setdefault(name, [])
            evidence = report.get("evidence")
            if isinstance(evidence, dict):
                add(name, evidence.get("sandbox_id"))
                ids = evidence.get("sandbox_ids")
                for value in ids if isinstance(ids, list) else ():
                    add(name, value)
        match = SUMMARY_SANDBOX.match(line)
        if match:
            add("summary", match.group(1))
    return found


class Collector:
    def __init__(self, out: Path, since: int, until: int, deadline: float) -> None:
        self.out = out
        self.since = since
        self.until = until
        self.deadline = deadline
        self.written = 0
        self.index: dict[str, object] = {
            "window": {"start": since, "end": until},
            "files": [],
            "errors": [],
        }
        self.urls: dict[str, str] = {}
        self.forwards: list[subprocess.Popen[bytes]] = []

    def remaining(self) -> float:
        return self.deadline - time.monotonic()

    def error(self, what: str, detail: object) -> None:
        self.index["errors"].append({"what": what, "detail": str(detail)[:500]})

    def write(self, name: str, body: bytes) -> None:
        if self.written >= MAX_TOTAL_BYTES:
            self.error(name, "output budget exhausted; not written")
            return
        if len(body) > MAX_FILE_BYTES:
            body = body[:MAX_FILE_BYTES]
            self.error(name, f"truncated to {MAX_FILE_BYTES} bytes")
        path = self.out / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        self.written += len(body)
        self.index["files"].append(name)

    def get(self, backend: str, path: str, params: dict[str, str]) -> bytes | None:
        base = self.urls.get(backend)
        if base is None:
            return None
        timeout = min(REQUEST_TIMEOUT_SECONDS, self.remaining())
        if timeout <= 1:
            self.error(f"{backend} {path}", "time budget exhausted")
            return None
        url = f"{base}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return response.read(MAX_FILE_BYTES + 1)
        except (urllib.error.URLError, OSError, ValueError) as error:
            detail = error
            if isinstance(error, urllib.error.HTTPError):
                with suppress(OSError):
                    detail = f"{error}: {error.read(500).decode(errors='replace')}"
            self.error(f"{backend} {path}", detail)
            return None

    def get_json(self, backend: str, path: str, params: dict[str, str]) -> object | None:
        body = self.get(backend, path, params)
        if body is None:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError as error:
            self.error(f"{backend} {path}", error)
            return None

    def connect(self) -> None:
        """Use each localhost forward, or open a forward of our own to the Service."""
        for name, (url, namespace, service, port, ready) in BACKENDS.items():
            if self._ready(url + ready):
                self.urls[name] = url
                continue
            local = _free_port()
            try:
                forward = subprocess.Popen(
                    ["kubectl", "--context", KIND_CONTEXT, "-n", namespace, "port-forward",
                     "--address=127.0.0.1", service, f"{local}:{port}"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except OSError as error:
                self.error(f"{name} forward", error)
                continue
            self.forwards.append(forward)
            url = f"http://127.0.0.1:{local}"
            for _ in range(20):
                if self._ready(url + ready):
                    self.urls[name] = url
                    break
                if forward.poll() is not None or self.remaining() < 5:
                    break
                time.sleep(0.5)
            if name not in self.urls:
                self.error(f"{name} forward", "backend did not answer")
        self.index["backends"] = dict(self.urls)

    def close(self) -> None:
        for forward in self.forwards:
            forward.terminate()
            with suppress(subprocess.TimeoutExpired):
                forward.wait(timeout=5)

    @staticmethod
    def _ready(url: str) -> bool:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                return response.status == 200
        except (urllib.error.URLError, OSError, ValueError):
            return False

    def traces(self, label: str, queries: list[tuple[str, str]], limit: int) -> list[str]:
        """Save each named Tempo search and the traces they name, in query
        order, up to `limit` traces in all; return the trace IDs."""
        trace_ids: list[str] = []
        for name, query in queries:
            if len(trace_ids) >= limit:
                break
            params = {
                "q": query,
                "start": str(self.since - TEMPO_SEARCH_CUTOFF_SECONDS),
                "end": str(self.until + 60),
                "limit": str(limit),
            }
            body = self.get("tempo", "/api/search", params)
            if body is None:
                continue
            self.write(f"{label}/tempo-search-{name}.json", body)
            try:
                found = json.loads(body).get("traces") or []
            except (json.JSONDecodeError, AttributeError):
                continue
            for trace in found:
                trace_id = str(trace.get("traceID") or "")
                if trace_id and trace_id not in trace_ids and len(trace_ids) < limit:
                    trace_ids.append(trace_id)
        for trace_id in trace_ids:
            trace = self.get("tempo", f"/api/v2/traces/{trace_id}", {})
            if trace is not None:
                self.write(f"{label}/trace-{trace_id}.json", trace)
        return trace_ids

    def logs(self, label: str, sandbox_id: str) -> int:
        """Save the Loki lines that mention one sandbox, containers and node journals alike."""
        body = self.get(
            "loki",
            "/loki/api/v1/query_range",
            {
                "query": f'{{log_source=~".+"}} |= "{sandbox_id}"',
                "start": f"{self.since}000000000",
                "end": f"{self.until}000000000",
                "limit": str(LOKI_LINE_LIMIT),
                "direction": "forward",
            },
        )
        if body is None:
            return 0
        self.write(f"{label}/loki-lines.json", body)
        try:
            streams = json.loads(body)["data"]["result"]
        except (json.JSONDecodeError, KeyError, TypeError):
            return 0
        return sum(len(stream.get("values") or ()) for stream in streams)

    def metrics(self) -> None:
        """Save the run's `space_*` series, summed by service, one query per metric.

        Native histograms keep only their count and sum: their buckets would
        multiply the snapshot many times over and add little to it.
        """
        body = self.get_json(
            "prometheus",
            "/api/v1/label/__name__/values",
            {"match[]": '{__name__=~"space_.+"}', "start": str(self.since), "end": str(self.until)},
        )
        names = body.get("data") if isinstance(body, dict) else None
        if not isinstance(names, list):
            return
        step = max(15, (self.until - self.since) // PROMETHEUS_POINTS)
        snapshot: dict[str, object] = {"step": step, "metrics": {}}
        for name in names[:MAX_METRICS]:
            result = self.get_json(
                "prometheus",
                "/api/v1/query_range",
                {
                    "query": f"sum by (service_name) ({name})",
                    "start": str(self.since),
                    "end": str(self.until),
                    "step": str(step),
                },
            )
            data = result.get("data") if isinstance(result, dict) else None
            if not isinstance(data, dict):
                continue
            series = []
            for item in data.get("result") or ():
                entry = {"service_name": item.get("metric", {}).get("service_name", "")}
                if "values" in item:
                    entry["values"] = item["values"]
                if "histograms" in item:
                    entry["histogram_count_sum"] = [
                        [point[0], point[1].get("count"), point[1].get("sum")]
                        for point in item["histograms"]
                    ]
                series.append(entry)
            snapshot["metrics"][name] = series
        if len(names) > MAX_METRICS:
            self.error("prometheus", f"{len(names)} metrics; kept the first {MAX_METRICS}")
        self.write("prometheus/space-series.json", json.dumps(snapshot).encode())


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _kind_cluster_created() -> int | None:
    """When this run's Kind control plane started: the earliest useful window."""
    try:
        created = subprocess.run(
            ["docker", "inspect", "-f", "{{.Created}}", "space-local-control-plane"],
            capture_output=True, text=True, timeout=10, check=True,
        ).stdout.strip()
        stamp = created.split(".")[0].removesuffix("Z")
        return calendar.timegm(time.strptime(stamp, "%Y-%m-%dT%H:%M:%S"))
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def collect(collector: Collector, args: argparse.Namespace) -> None:
    lines: list[str] = []
    for log in args.log:
        try:
            lines += log.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError as error:
            collector.error(f"read {log}", error)
    scenarios = failed_sandboxes(lines)
    collector.index["failed_scenarios"] = scenarios

    try:
        collector.connect()
        sandboxes: list[str] = []
        for ids in scenarios.values():
            sandboxes += [sandbox for sandbox in ids if sandbox not in sandboxes]
        if len(sandboxes) > MAX_SANDBOXES:
            collector.error("sandboxes", f"{len(sandboxes)} found; kept the first {MAX_SANDBOXES}")
        summary = {}
        for sandbox_id in sandboxes[:MAX_SANDBOXES]:
            label = f"sandbox-{sandbox_id}"
            # A busy sandbox has more traces than the bound, so take first
            # those with a span in error status. That is a span its
            # instrumentation marked failed, or one holding an ERROR-level
            # event; a failed detached finalizer or publication marks its
            # own root, since it logs only a WARN.
            selector = f'span.sandbox_id="{sandbox_id}"'
            trace_ids = collector.traces(
                label,
                [("errors", f"{{{selector} && status=error}}"), ("all", f"{{{selector}}}")],
                MAX_TRACES_PER_SANDBOX,
            )
            summary[sandbox_id] = {
                "trace_ids": trace_ids,
                "loki_lines": collector.logs(label, sandbox_id),
            }
        collector.index["sandboxes"] = summary
        if not sandboxes:
            collector.index["error_trace_ids"] = collector.traces(
                "errors",
                [("errors", '{resource.service.name=~"space-.+|containerd-shim-space-v2" && status=error}')],
                MAX_ERROR_TRACES,
            )
        collector.metrics()
    finally:
        collector.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--log", type=Path, action="append", default=[])
    parser.add_argument("--since", type=int, default=0, help="window start, epoch seconds")
    parser.add_argument("--budget", type=float, default=240.0, help="seconds")
    args = parser.parse_args()
    now = int(time.time())
    since = args.since or _kind_cluster_created() or now - DEFAULT_WINDOW_SECONDS
    since = max(since, now - DEFAULT_WINDOW_SECONDS)
    # Built before anything can fail, so index.json, with what failed, is
    # always written.
    collector = Collector(args.out, since, now, time.monotonic() + args.budget)
    try:
        args.out.mkdir(parents=True, exist_ok=True)
        collect(collector, args)
    except Exception as error:  # noqa: BLE001 - evidence collection never fails its caller
        traceback.print_exc()
        collector.error("collection stopped", f"{type(error).__name__}: {error}")
    with suppress(OSError):
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "index.json").write_text(
            json.dumps(collector.index, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print(
        f"failure telemetry: {len(collector.index['files'])} files, "
        f"{collector.written} bytes, {len(collector.index['errors'])} errors in {args.out}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
