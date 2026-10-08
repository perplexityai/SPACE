# Kind workload telemetry

This library owns the common telemetry defaults and environment template used
by the Kind workload and node-service charts. It deploys no resources and contains no
experiment or validation code. Override `telemetry` in the consuming chart.

Each consumer imports these defaults and includes `space-kind-telemetry.env`.
Its `charts/space-kind-telemetry` source link makes a clean checkout render
without downloading or committing generated archives. Helm packages the linked
source into standalone chart archives. Tilt watches this library explicitly.

`telemetry.cluster` is the Kubernetes cluster name (`space-local` by default).
The application's `clusterId` is a separate platform identity (`kind-local` by
default); the telemetry selector sends the same platform ID to services and
the node-hosted shim. Experiment environment, owner and phase do not rename it.

In `otel` mode, metrics and traces use separate Collector Services over
plaintext OTLP/gRPC. Install them with `infra/space/kind/metrics/local.sh up`
and `infra/space/kind/traces/local.sh up`; no Datadog Agent is required.

| Value | Default | Environment variable |
| --- | --- | --- |
| `telemetry.otlpMetricsEndpoint` | `http://collector.space-metrics.svc.cluster.local:4317` | `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` |
| `telemetry.otlpTracesEndpoint` | `http://collector.space-traces.svc.cluster.local:4317` | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` |

Each signal's endpoint and protocol variables are emitted only when its sink
or exporter is `otlp`. `telemetry.otlpProtocol` defaults to `grpc` and supplies
both per-signal protocol variables. The generic `OTEL_EXPORTER_OTLP_ENDPOINT`
is not emitted. Logs stay on stdout for Alloy to collect; they need no endpoint.
The node-hosted shim resolves the traces Service's ClusterIP separately during
installation because it cannot use pod DNS.
