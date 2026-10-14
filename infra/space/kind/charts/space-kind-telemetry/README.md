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

The default `datadog` mode uses the native exporters without authorizing Agent
installation. In the explicit `otel` profile, metrics and traces use separate
Collector Services over plaintext OTLP/gRPC. Bootstrap and Tilt install required
local receivers in that mode (`infra/space/kind/metrics/local.sh up` and
`infra/space/kind/traces/local.sh up` by hand); no Datadog Agent is required.
The same JSON profile can override `SPACE_KIND_OTLP_METRICS_ENDPOINT` and
`SPACE_KIND_OTLP_TRACES_ENDPOINT` for applications and host processes together.

| Value | Default | Environment variable |
| --- | --- | --- |
| `telemetry.otlpMetricsEndpoint` | `http://collector.space-metrics.svc.cluster.local:4317` | `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` |
| `telemetry.otlpTracesEndpoint` | `http://collector.space-traces.svc.cluster.local:4317` | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` |

Each signal's endpoint and protocol variables are emitted only when its sink or
exporter is `otlp`, so `off` mode renders `off` for both and no endpoints.
`telemetry.otlpProtocol` defaults to `grpc` and supplies both per-signal
protocol variables. `telemetry.otlpMetricsTemporality`, which bring-up sets to
`cumulative` in `otel` mode, becomes
`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` for the Rust services and
the runtime-supervisor, so a rare counter keeps its total however long the
Collector's delta-to-cumulative conversion has been idle; empty keeps their
delta default. The shim stays delta. The generic `OTEL_EXPORTER_OTLP_ENDPOINT`
is not emitted. Logs stay on stdout for Alloy to collect; they need no endpoint. The
node-hosted shim and runtime-supervisor cannot use pod DNS, so they address
each Collector by the fixed ClusterIP that chart's Kind profile pins.
`telemetry.nodeOtlpMetricsEndpoint` and `telemetry.nodeOtlpTracesEndpoint`
carry those addresses to `space-node-manager-local`, which writes the
supervisor's node environment, and to `space-sandbox-manager-local`, which
exports to them in place of the two endpoints above: sandbox-manager forwards
its endpoints to the net-gateway and spaced-host processes it launches on the
node, which cannot resolve cluster DNS either. Bring-up sets both.

`telemetry.excludeDatadogLogs` adds a log-only exclusion to managed workload
Pods in `otel` mode; the builder's command-output exclusion remains in every
profile. Switching to Datadog pauses owned Alloy without deleting Loki data.

## Cluster path: `global.observability`

Kind bring-up never sets `global.observability`. Without it, or with it set to
`null`, the chart renders exactly what the `telemetry` values above describe. A deployment
outside Kind sets it instead, once for every SPACE chart, and then only
`telemetry.version` (or the image tag) and a `telemetry.traceExporter` of
`off` are read from `telemetry`:

```yaml
global:
  observability:
    mode: otlp            # off | datadog | otlp; required. Quote it: YAML reads bare off as false (accepted).
    environment: staging  # SPACE_ENV, DD_ENV, deployment.environment.name; required unless off
    cluster: cluster-a    # k8s.cluster.name and the kube_cluster_name Datadog tag; required unless off
    datadog:
      dogstatsdPort: 8125 # DD_DOGSTATSD_PORT
      apmPort: 8126       # DD_TRACE_AGENT_PORT; datadog mode currently requires 8126
    otlp:
      protocol: grpc      # the only protocol SPACE producers export
      nodeLocalPort: 4317
      traceSampler: ""    # OTEL_TRACES_SAMPLER when set
```

| Mode | Environment |
| --- | --- |
| `off` | `SPACE_METRICS_SINK` and `SPACE_TRACE_EXPORTER` `off`, `DD_TRACE_ENABLED=false`; no Agent address or OTLP endpoint |
| `datadog` | Datadog sinks; `DD_AGENT_HOST` from `status.hostIP`, the configured DogStatsD port and APM port 8126 |
| `otlp` | `NODE_IP` from `status.hostIP`, then both signal endpoints `http://$(NODE_IP):<nodeLocalPort>` over gRPC, with `cumulative` metric temporality for the Rust services |

A chart that sets `telemetry.traceExporter: "off"` keeps traces off in every
mode (`SPACE_TRACE_EXPORTER=off` and no trace endpoint, sampler or APM port;
`DD_TRACE_ENABLED=false` in the `off` and `datadog` modes, where it is
emitted) while its metrics follow the mode. `space-build-manager-local`
does, because build errors can contain guest output. Other `telemetry`
values do not apply on the cluster path.

The build-manager's top-level `traceExporter` accepts both quoted `"off"` and
bare YAML `off`; `null` also retains its safe trace-off default. Only an
explicit empty string requests the shared selector. In `off` mode, omitted
environment and cluster names do not produce empty identity attributes.

`NODE_IP` precedes the endpoints because the kubelet expands `$(NODE_IP)` only
from earlier variables. The `otlp` mode expects an OTLP receiver on every
node at `nodeLocalPort`; this chart does not provide one. An unknown mode, a
protocol other than `grpc`, a port outside 1-65535, a missing environment or
cluster when mode is not `off`, or a name containing anything but letters,
digits and `._:/-` fails the render. The native shim and host-sidecar trace
exporters require APM port 8126, so another `datadog.apmPort` fails in `datadog`
mode rather than routing only some producers to the new port. That override
is unused in `off` and `otlp` modes.

`space-kind-telemetry.envNames` lists the variables `space-kind-telemetry.env`
sets for the selected path and mode, for writers that copy a container's
resolved environment to a host process. `space-kind-telemetry.ownedEnvNames`
lists the union across modes so a mode change also removes stale settings.

On the cluster path, `space-node-manager-local`'s `kind-node-env` writes the
host runtime-supervisor's `/space/config/space.env` from this resolved
environment, including the node-local endpoints. It preserves unrelated
bootstrap lines, replaces only managed variables, keeps the supervisor's own
service identity, and marks it for restart only when its settings change.
The same step writes a root-only `/space/config/shim-telemetry.env` for a
cluster shim wrapper to source. The existing Kind shim installer
(`scripts/install-shim.sh`) still follows `scripts/telemetry_config.py`; it
does not consume this cluster file.

## Cluster install helpers

`templates/_cluster.tpl` holds `space-cluster.dbPassword` for the install path
in `infra/space/cluster`. It takes a managed role password from
`postgres.rolesSecretName` when that is set, and otherwise renders the Kind
development default inline.
