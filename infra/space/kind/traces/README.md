# Local traces: Collector → Tempo

**Traces-only** ingestion for verified Docker Kind `space-local`, installed
automatically by default Kind bootstrap and full-stack Tilt in `otel` mode.
The release remains independent from metrics, logs and Grafana. Running this
helper alone changes no application instrumentation, sampling policy, exporter
selection or Datadog Agent settings. There is no tail sampling, span metrics,
logs pipeline or service-graph generator.

## Start and query

Requirements: the existing Kind bench, Docker, Helm and kubectl.

```bash
helm dependency build infra/space/observability/charts/traces
helm lint infra/space/observability/charts/traces \
  -f infra/space/observability/charts/traces/profiles/kind.yaml
bash infra/space/kind/traces/local.sh up
bash infra/space/kind/traces/local.sh status
# Default bootstrap and full-stack Tilt already forward this port in otel mode.
bash infra/space/kind/traces/local.sh ui
# Tempo query API: http://127.0.0.1:13200
```

The helper requires context `kind-space-local` (`kind-$SPACE_KIND_CLUSTER` when
that is set) to match the loopback API port of the Docker Kind control plane
and verifies namespace ownership. It applies the
chart's Kind overlay, which permits disposable storage and skips network
policies. For shared clusters use the portable
[observability charts and deployment runbook](../../observability/README.md),
not this local test harness. Resources stay in `space-traces`. All Services are ClusterIP; tunnels bind loopback. Tempo
has no authentication: other pods can reach it, so this is **local only**, not
tenant isolation. Never expose these ports publicly.

The in-cluster receiver is
`http://collector.space-traces.svc.cluster.local:4317` (plaintext OTLP/gRPC).
The default Kind `otel` mode already points SPACE producers here and needs no
Datadog Agent. Installing this backend independently does not redirect producers
or reconfigure an existing Agent. For a separate canary, if a selected process
already uses OTLP traces, temporarily override only
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`. Native producers additionally need an
explicit, separately reviewed `SPACE_TRACE_EXPORTER=otlp` opt-in. Configure
selected workloads through their owning chart's per-signal telemetry values;
see [Kind workload telemetry](../charts/space-kind-telemetry/README.md). Capture
the standing values before the canary, restore those values afterward, and
verify the running pods. Never unset blindly, change the general OTLP endpoint,
or alter metrics/logs. Coordinate with the local bench owner before rolling
SPACE deployments.

Queries (allow at least 30 seconds after the workload):

```bash
curl -fsSG http://127.0.0.1:13200/api/search \
  --data-urlencode 'q={resource.service.name="space-api"}' \
  --data-urlencode "start=$(date -d '1 hour ago' +%s)" \
  --data-urlencode "end=$(date +%s)" --data-urlencode limit=20
# Replace with an ID returned above; JSON IDs inside spans are base64 encoded.
curl -fsS -H 'Accept: application/json' \
  http://127.0.0.1:13200/api/traces/YOUR_HEX_TRACE_ID
```

Tempo 3's default `query_frontend.query_end_cutoff=30s` deliberately excludes
the newest 30 seconds, **even with explicit end bounds**. Very short windows
can yield an invalid effective range. Use a wider window and bounded polling;
export acknowledgement alone is not evidence that a trace is searchable yet.

Other useful TraceQL examples:

```traceql
{resource.service.name="space-api" && span.http.route="/sandboxes"}
{resource.service.name="space-api" && status=error}
{resource.service.name="space-cluster-api"}
```

The default Kind bring-up also installs the independent `grafana` chart with
the Tempo datasource enabled at `http://tempo.space-traces.svc.cluster.local:3200`.
For a standalone backend, that UI remains optional.
Open `/d/space-traces` on its localhost UI; click a trace ID for the detail
dashboard. Grafana is neither bundled here nor required to validate ingestion.

## Validate

The [trace integration workflow](../../../../scripts/tests/observability/traces/README.md)
owns the Rust fixture invocation, stored-record assertions and restart checks.
Installing this release requires no fixture build or validation scripts.

## Versions, limits and semantics

- Collector Contrib **0.161.0**, Tempo **3.0.3**, both digest-pinned in the
  chart's `values.yaml`. Storage, scheduling, retention and network policy are
  chart values; this harness only supplies the Kind overlay. Tempo runs monolithic `-target=all`, with no Kafka or object
  store. Official [single-binary guidance](https://grafana.com/docs/tempo/latest/set-up-for-tracing/setup-tempo/deploy/locally/linux/),
  [configuration](https://grafana.com/docs/tempo/latest/configuration/),
  [Collector setup](https://grafana.com/docs/tempo/latest/set-up-for-tracing/instrument-send/set-up-collector/otel-collector/)
  and [query API](https://grafana.com/docs/tempo/latest/api_docs/) informed this setup.
- One Collector (100m/128Mi request, 500m/512Mi limit, Go soft target 205Mi),
  memory limiter 256Mi with 64Mi spike allowance, 4MiB receive-message maximum,
  batches of at most 256 spans. Queue capacity is 128 requests/batches, one
  consumer; RPC timeout 3s, retries 1–5s with a 30s retry budget. Queue is memory
  only; process death or an extended outage can lose spans. This is not a byte
  quota or durable delivery guarantee. Observe rejected/failed spans and queue
  occupancy on Collector port 8888.
- One Tempo (250m/512Mi request, 2 CPU/2Gi limit, Go soft target 1536Mi).
  Ingestion limit 2MiB/s, burst 4MiB, 1,000 live traces and 1MiB per trace;
  query duration at most six hours. `/ready` gates startup/readiness.
- A 4Gi PVC holds local blocks and WAL. The default six-hour block retention
  (`tempo.blockRetention`) with a five minute retention sweep is a cleanup policy, not a precise per-span expiry or
  hard disk quota. Kind's local-path storage does not enforce PVC size as a disk
  quota; monitor devbox disk. Loss of the Kind node/cluster loses this local
  storage. This sizing was exercised only by the small local workloads.
- Collector passes trace data without transformations. OTLP names are preserved;
  `operation.name` is a separate attribute, not a replacement for span name.
  Numeric/string distinctions survive (the real API and fixture differ for
  HTTP status type). Datadog APM operation/resource mapping and sampling are
  separate compatibility questions. Partial per-service routing can produce
  traces whose parent/root is stored in the other backend.

See [validation results and remaining gaps](../../../../scripts/tests/observability/traces/validation.md). Shared-cluster
adoption follows the [portable deployment runbook](../../observability/README.md):
authentication, data policy, durable storage, capacity, scoped canaries and
rollback require follow-up review. Do not generalize this helper's context guard.

## Rollback / cleanup

First restore any application endpoint/exporter overrides through their owning
deployment configuration; wait for running pods and confirm normal delivery.
Then stop the localhost tunnels. To remove this disposable stack:

```bash
# Deletes only the owned trace namespace, INCLUDING its stored trace PVC.
bash infra/space/kind/traces/local.sh cleanup --confirm-delete-local-data
```

Deleted trace data is not recoverable without a separately saved export/backup.
Reinstalling the chart recreates an empty backend. Metrics, logs, Grafana and
Datadog are untouched. For configuration-only rollback, inspect `helm history
space-traces -n space-traces --kube-context kind-space-local` and roll
back the selected revision with the same explicit context/namespace; keep PVCs.
