"""Telemetry delivery to the local Kind backends, read back from their query APIs.

In `otel` mode the Rust services and the Go shim export metrics and traces over
OTLP to the Collectors, which write them to Prometheus and Tempo, and Alloy
ships every SPACE pod's stdout to Loki. Each leg can break on its own and stay
silent: an exporter that cannot reach its Collector only retries, a Collector
that cannot write drops, and Alloy's targets are a path glob. A passing
lifecycle smoke proves none of it, so this reads each backend back for the
scenario's own sandbox, never for whatever an earlier scenario left behind:

- Tempo holds one trace in which space-api created the sandbox and the shim
  started it, the shim's spans carrying the sandbox's `sandbox_id`;
- Prometheus holds the shim's create latency from a shim process that started
  after the scenario did, on the node that trace names, and a space-api create
  latency sample newer than the scenario's start;
- Loki holds a SPACE pod log line that names the sandbox.

Each check records what it read, at the scenario's start and at its last poll
where it compares the two, so a failure's evidence shows which leg stopped.

Every read polls to a budget of its delivery floor plus margin. Both SDKs export
metrics every 10 s and flush spans every 5 s, and the Collectors batch for 1 s.
Tempo search also withholds the newest 30 s whatever the requested end, so a
trace is findable no sooner than about 36 s after its spans end.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from space_smoke.capabilities import TelemetryBackendControl

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SHIM_SERVICE = "containerd-shim-space-v2"
API_SERVICE = "space-api"
# Two metric export periods plus the Collector's batch, with room for a slow
# runner.
DELIVERY_BUDGET_SECONDS = 60.0
# Tempo's 30 s search cutoff, a span flush and the Collector's batch, with
# room for a slow runner.
TRACE_BUDGET_SECONDS = 90.0
# Tempo's query_end_cutoff: search never covers the newest 30 s.
TEMPO_SEARCH_CUTOFF_SECONDS = 30
POLL_SECONDS = 3.0
# The Kind nodes share this host's clock; this covers second rounding and the
# moment between the scenario's start and its first recorded phase.
WINDOW_SLACK_SECONDS = 5


class TelemetryDeliveryAssertions:
    """Poll Tempo, Prometheus and Loki for the scenario's own sandbox."""

    def __init__(
        self, fixture: KindEcw, control: TelemetryBackendControl, *, since: float
    ) -> None:
        self._fixture = fixture
        self._control = control
        # When the scenario began, before its sandbox was created.
        self._since = int(since) - WINDOW_SLACK_SECONDS
        self._node: str | None = None
        fixture.record("telemetry_window_start", self._since)

    def expect_traces(self) -> None:
        """One trace has a space-api span and a shim span for this sandbox, and Tempo returns it by ID."""

        sandbox_id = self._fixture.sandbox.sandbox_id
        api_spans = f'{{resource.service.name="{API_SERVICE}"}}'
        shim_spans = f'{{resource.service.name="{SHIM_SERVICE}" && span.sandbox_id="{sandbox_id}"}}'

        def search(query: str) -> list[str]:
            # The query names this sandbox, so a wider window cannot match
            # another scenario's trace; Tempo rejects one its 30 s cutoff
            # leaves empty.
            body = self._control.telemetry_query(
                "tempo",
                "/api/search",
                {
                    "q": query,
                    "start": str(self._since - TEMPO_SEARCH_CUTOFF_SECONDS),
                    "end": str(int(time.time()) + 60),
                    "limit": "5",
                },
            )
            return [str(trace["traceID"]) for trace in body.get("traces") or ()]

        found = self._poll(lambda: search(f"{api_spans} && {shim_spans}"), bool, budget=TRACE_BUDGET_SECONDS)
        self._fixture.record("tempo_trace_ids", found)
        self._fixture.verify(
            bool(found),
            f"Tempo found no trace from {API_SERVICE} to the shim for {sandbox_id}",
            # Which side of the join is missing.
            failure_evidence=lambda: {
                "tempo_api_trace_ids": search(api_spans),
                "tempo_shim_trace_ids": search(shim_spans),
            },
        )
        trace_id = found[0]
        body = self._control.telemetry_query("tempo", f"/api/v2/traces/{trace_id}", {})
        services: dict[str, Mapping[str, str]] = {}
        for batch in body.get("trace", {}).get("resourceSpans") or ():
            attributes = {
                attribute["key"]: str(next(iter(attribute["value"].values()), ""))
                for attribute in batch.get("resource", {}).get("attributes", ())
            }
            services[attributes.get("service.name", "")] = attributes
        self._fixture.record("tempo_trace_services", sorted(services))
        self._fixture.verify(
            API_SERVICE in services and SHIM_SERVICE in services,
            f"Tempo returned trace {trace_id} without both {API_SERVICE} and {SHIM_SERVICE} spans",
        )
        self._node = services[SHIM_SERVICE].get("k8s.node.name") or None
        self._fixture.record("sandbox_node", self._node)

    def expect_metrics(self) -> None:
        """A shim started after the scenario reported its create latency, and so did space-api."""

        node = self._node
        self._fixture.verify(node is not None, "the shim's spans named no Kind node")
        shim_selector = f'{{op="create", k8s_node_name="{node}"}}'

        def shim_instances(at: int) -> list[str]:
            return sorted(
                str(sample["metric"].get("service_instance_id", ""))
                for sample in self._prometheus(
                    "group by (service_instance_id) "
                    f"(space_shim_operation_latency_ms_milliseconds{shim_selector})",
                    at,
                )
            )

        earlier = shim_instances(self._since)
        current = self._poll(lambda: shim_instances(int(time.time())), lambda now: bool(set(now) - set(earlier)))
        new = sorted(set(current) - set(earlier))
        self._fixture.record(
            "prometheus_shim_instances", {"before": earlier, "after": current, "new": new}
        )
        self._fixture.verify(
            bool(new),
            f"Prometheus holds no shim create latency on {node} from a shim started after the scenario",
        )

        # The services export delta metrics, and the Collector accumulates a
        # stream only while it is active: one idle past its max_stale restarts
        # from its next delta, so a create count need not move across a
        # scenario that made a create. A delta stream is exported only in a
        # period it was updated, so a create latency sample newer than the
        # scenario's start is this create's, whatever the count says.
        # timestamp() reads sample times only straight off a selector, and
        # drops the metric name, so this names one metric.
        def newest(at: int) -> dict[str, float]:
            return {
                str(sample["metric"].get("service_instance_id", "")): float(sample["value"][1])
                for sample in self._prometheus(
                    "max by (service_instance_id) (timestamp("
                    'space_sandbox_api_latency_milliseconds{op="create", surface="ecw", result="success"}))',
                    at,
                )
            }

        def fresh(samples: dict[str, float]) -> list[str]:
            return sorted(instance for instance, at in samples.items() if at > self._since)

        before = newest(self._since)
        after = self._poll(lambda: newest(int(time.time())), lambda samples: bool(fresh(samples)))
        self._fixture.record(
            "prometheus_api_newest_create_samples",
            {"before": before, "after": after, "fresh": fresh(after)},
        )
        self._fixture.verify(
            bool(fresh(after)),
            f"Prometheus holds no {API_SERVICE} create latency sample from after the scenario began",
        )

    def expect_logs(self) -> None:
        """Loki holds a SPACE pod log line that names the sandbox."""

        sandbox_id = self._fixture.sandbox.sandbox_id

        def streams() -> list[Mapping[str, str]]:
            body = self._control.telemetry_query(
                "loki",
                "/loki/api/v1/query_range",
                {
                    "query": f'{{namespace="space"}} |= "{sandbox_id}"',
                    "start": str(self._since * 10**9),
                    "end": str((int(time.time()) + 60) * 10**9),
                    "limit": "20",
                },
            )
            return [stream["stream"] for stream in body["data"]["result"]]

        def space_lines() -> Mapping[str, object]:
            # Whether Loki is receiving SPACE pod logs at all.
            now = int(time.time())
            body = self._control.telemetry_query(
                "loki",
                "/loki/api/v1/query",
                {
                    "query": f'sum(count_over_time({{namespace="space"}}[{now - self._since}s]))',
                    "time": str(now * 10**9),
                },
            )
            result = body["data"]["result"]
            return {"loki_space_lines_since_start": float(result[0]["value"][1]) if result else 0.0}

        found = self._poll(streams, bool)
        self._fixture.record(
            "loki_services_naming_sandbox",
            sorted({stream.get("service", "") for stream in found}),
        )
        self._fixture.verify(
            bool(found),
            f"Loki holds no SPACE pod log line naming {sandbox_id}",
            failure_evidence=space_lines,
        )

    def _prometheus(self, query: str, at: int) -> list[dict[str, Any]]:
        body = self._control.telemetry_query(
            "prometheus", "/api/v1/query", {"query": query, "time": str(at)}
        )
        return list(body["data"]["result"])

    def _poll(
        self,
        read: Callable[[], Any],
        done: Callable[[Any], bool],
        *,
        budget: float = DELIVERY_BUDGET_SECONDS,
    ) -> Any:
        deadline = time.monotonic() + budget
        while True:
            value = read()
            if done(value) or time.monotonic() >= deadline:
                return value
            self._fixture.wait(POLL_SECONDS)
