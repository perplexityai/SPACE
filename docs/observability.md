# Observability builds and selectors

[Documentation](../README.md)

SPACE emits metrics and traces either through Datadog's native clients or
through OpenTelemetry over OTLP. Each signal is selected independently, by the
same rules in the Rust services and the Go shim. Logs are not part of this
selection; see [Logs](#logs).

## Selecting a backend

`SPACE_TRACE_EXPORTER` chooses the trace exporter and `SPACE_METRICS_SINK`
chooses the metrics sink. Both trim surrounding whitespace and ignore case. An
unset selector means `auto`; a blank or unrecognized value is a configuration
error rather than a silent default.

`auto` is the same rule for both signals and both languages: **the native
Datadog client when the binary has one, otherwise OTLP when an endpoint says
where to send it, otherwise off.** Adapter availability is a build-time fact,
not a health check, and `auto` never probes a receiver. `auto` also never
fails — see [When a selector fails](#when-a-selector-fails).

Rust traces, by what the binary was built with (`datadog` implies `otel`):

| `SPACE_TRACE_EXPORTER` | `datadog` | `otel` only | neither |
|---|---|---|---|
| unset / `auto` | native Datadog | OTLP with a trace endpoint, else off | off |
| `datadog` | native Datadog | configuration error | configuration error |
| `otlp` | OTLP | OTLP | configuration error |
| `off` | off | off | off |

Rust metrics:

| `SPACE_METRICS_SINK` | `datadog` | `otlp-metrics` only | neither |
|---|---|---|---|
| unset / `auto` | DogStatsD | OTLP with a metrics endpoint, else off | off |
| `datadog` | DogStatsD | configuration error | configuration error |
| `otlp` | OTLP | OTLP | configuration error |
| `journald` | journald | journald | journald |
| `off` | off | off | off |

`auto` no longer falls back to journald on a build without `datadog`. Journald
is still available, but only when it is asked for by name.

Every Go shim build compiles in both native Datadog clients, so the shim has
one column:

| Selector | `SPACE_TRACE_EXPORTER` | `SPACE_METRICS_SINK` |
|---|---|---|
| unset / `auto` | native Datadog | DogStatsD |
| `datadog` | native Datadog | DogStatsD |
| `otlp` | OTLP | OTLP |
| `off` | off | off |
| `journald` | configuration error | configuration error |

The shim therefore cannot reach the OTLP branch of `auto`; it is reachable in
a Rust binary built with `otel` and no `datadog`.

## When a selector fails

**`auto` never stops a process from starting.** It means "whatever this build
can do", so if the destination it picked turns out to be unusable, SPACE logs a
warning saying which setting is wrong and exports nothing for that signal. The
operator never named that destination, so it is not treated as a broken
promise.

**An explicit selector does stop it.** Writing `otlp` or `datadog` names a
destination the process is expected to reach, so a configuration it cannot use
fails telemetry initialization and the process does not start half-observed.
These fail:

- a blank or unrecognized selector value — a typo is never softened into
  "whatever works";
- a selector naming a client the binary does not contain;
- an OTLP endpoint, protocol, or header setting the profile does not accept;
- an invalid `OTEL_METRIC_EXPORT_INTERVAL`.

`SPACE_TRACE_FILTER` is stricter still: an unparsable value fails startup
**even when traces are off**, because the setting is read before any exporter
is selected. A filter nobody can parse is a mistake worth surfacing whether or
not this process would have used it.

The native Datadog path is the exception. An unreachable or unresolvable
DogStatsD address warns and disables metrics rather than failing startup,
because that is how it has always behaved and a sandbox must not depend on the
Agent being up.

This matters most when the two signals share `OTEL_EXPORTER_OTLP_ENDPOINT`.
Configuring an endpoint for metrics also makes `auto` select OTLP for traces
on a build without the native adapter; if the traces side dislikes something,
that is a warning, not a failed startup.

A valid configuration whose receiver is not answering is a runtime condition
either way. The process starts, the exporter retries in the background, and the
failure appears as a log line.

Nothing falls back to a different backend. A rejected OTLP configuration does
not turn into DogStatsD, and an unreachable Collector does not turn into
journald — a degraded signal is off, not redirected.

## Identity

Service name, environment, and version resolve from the first nonblank value
in this order, per field:

| Field | SPACE | OpenTelemetry | Datadog |
|---|---|---|---|
| service | `SPACE_SERVICE_NAME` | `OTEL_SERVICE_NAME`, then `service.name` | `DD_SERVICE` |
| environment | `SPACE_ENV` | `deployment.environment.name` | `DD_ENV` |
| version | `SPACE_VERSION` | `service.version` | `DD_VERSION` |

The OpenTelemetry names other than `OTEL_SERVICE_NAME` are keys inside
`OTEL_RESOURCE_ATTRIBUTES`, whose values are percent-decoded as the SDKs
decode them. A binary's built-in default applies when nothing is set.

**A deployment that sets only `DD_*` is unaffected by this precedence.** Its
metric names, tag keys, tag values, and trace service, environment, and
version are what they have always been, so no Datadog monitor or dashboard
moves. Two details hold that, deliberately:

- **In the Go shim only,** the DogStatsD `version` tag carries the shim's build
  version and does **not** fall back to `DD_VERSION`. That tag has always
  reported the build version, and monitors read the series it produces. The
  shim's OTLP resource does use `DD_VERSION` for `service.version`, because no
  existing series depends on it. Rust services have always used `DD_VERSION`
  for their `version` tag and still do.
- The native tracer receives `DD_ENV` and `DD_VERSION` raw, untrimmed, because
  that is what it has always received.

An explicit `SPACE_*` or OpenTelemetry setting still overrides both.

Rust metric helpers also use a per-measurement `service` tag for a logical
RPC or socket service: the destination of gRPC clients, the served interface
of gRPC rate limiters, and the socket service performing UDS peer-process
authentication checks. The OTLP sink exports that dimension as `service.role`;
resource `service.name` alone identifies the emitting process, because OTLP
points carry no constant `service`, `env` or `version` copy of the resource's
identity. This keeps different peers or interfaces in separate
series even when they share one producer. OTLP queries grouping or filtering
that logical dimension must use `service.role`. Native DogStatsD retains its
existing wire tags, including both `service` values; its queries do not change.

Per-measurement `version` tags similarly export as `version.requested` on
OTLP, while the process version stays in resource `service.version`.
`template.fetch.error` uses this dimension for the requested content hash
(`"<unset>"` when absent), so failures for distinct hashes remain separate.
The hash has unbounded cardinality; it shares the existing 2,000-attribute-set
limit per instrument, with excess combinations going to the SDK overflow
series. Native DogStatsD keeps the original `version` wire tags.

### Mixed identity on the DogStatsD path

Setting a `SPACE_*` or OpenTelemetry identity **and** a `DD_*` one while the
metrics sink is Datadog leaves a limitation this code cannot resolve. The
resolver decides the tags SPACE attaches, but the Datadog client attaches its
own `env`, `service` and `version` from the `DD_*` settings, and suppressing
that would change what a Datadog-only deployment emits. Two identities
therefore land on the same series, and only an operator can decide which to
drop.

The shim reports it once, from the configuration check both of its entry
points run before serving, naming the fields that disagree and never their
values. A padded `DD_ENV=" prod "` beside `SPACE_ENV="prod"` counts as a
disagreement, because the Datadog client injects its value untrimmed. Set one
family, not both. A Datadog-only deployment and an OTLP-only deployment each
work fully and neither sees the warning.

Four attributes are always computed in the process that reports them and are
dropped from an inherited `OTEL_RESOURCE_ATTRIBUTES`, so one shared value
cannot make several processes claim the same identity:

| Attribute | Source |
|---|---|
| `service.instance.id` | host name and process id; for the Go shim, the node name, else the host name, and process id |
| `host.name` | the configured node name, else `HOSTNAME` outside Kubernetes |
| `k8s.node.name` | `SPACE_K8S_NODE_NAME`, then `KUBE_NODE_NAME` |
| `k8s.pod.name` | `SPACE_K8S_POD_NAME` |

A configured node name wins for `host.name` too, in Kubernetes or out of it:
it is the truest thing available about where the process runs. Only when no
node name is configured does `HOSTNAME` apply, and then only outside
Kubernetes, where `HOSTNAME` is the pod name and would invent a host that does
not exist.

A wrapper that puts the node name only inside `OTEL_RESOURCE_ATTRIBUTES` loses
it, since the inherited value is dropped. Pass `SPACE_K8S_NODE_NAME`.

`k8s.cluster.name` is the Kubernetes cluster name supplied through
`OTEL_RESOURCE_ATTRIBUTES`; Rust metrics and traces, the Go shim, and the
Python smoke runner preserve it. The SPACE platform cluster ID is a separate
identity. Rust OTLP metrics carry their constant `cluster_id` as resource
`space.cluster.id` as well as the existing `cluster_id` point attribute. They
never substitute it for an absent or configured Kubernetes name. Existing
queries on `cluster_id` and native DogStatsD wire tags are unchanged. Queries
on `k8s.cluster.name` must use the configured Kubernetes name, not the platform ID.

## Endpoints, protocol, and cadence

Signal-specific settings win over shared ones when nonblank:
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` and `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
over `OTEL_EXPORTER_OTLP_ENDPOINT`, and the `..._PROTOCOL` pair the same way.
Headers are the exception and have no precedence at all; see below.

Every producer — Rust metrics, Rust traces, and the Go shim — applies one
endpoint rule. An endpoint must:

- parse as a URL;
- use the plaintext `http://` scheme;
- name a host;
- embed no credentials;
- use a port between 1 and 65535 if it gives one — an empty `http://host:` is
  rejected, as is a port that is not a number or is out of range;
- carry no query or fragment, not even an empty `?` or `#`;
- carry no path, though a bare `/` is allowed and means the same as none.

The path restriction is worth checking before an upgrade. OpenTelemetry's
`http/protobuf` convention puts the signal in the path, so an address such as
`http://collector:4318/v1/metrics` is a natural thing to have configured. The
gRPC profile reads only the host and port, so that path used to be accepted
and then ignored. It is now rejected at startup with the reason.

| Setting | Metrics | Traces |
|---|---|---|
| endpoint | required, no default | defaults to `http://localhost:4317` |
| protocol | `grpc`, any case, blank means `grpc` | same |
| headers | every header variable must be unset or blank | same |

The trace default only works if a receiver is listening in that process's
network namespace. Neither signal has a TLS or header option: the profile is
plaintext to a local receiver, and that receiver owns outbound credentials.

Headers are the one setting with **no precedence**. A nonblank
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_METRICS_HEADERS` or
`OTEL_EXPORTER_OTLP_TRACES_HEADERS` is rejected on its own; a signal-specific
one does not override the shared one, because the SDK would read both and send
whichever it found over plaintext.

`OTEL_METRIC_EXPORT_INTERVAL` sets the metrics collection cadence in
milliseconds and defaults to 10000 in both languages. A value that is not a
positive whole number of milliseconds fails startup instead of exporting on a
cadence nobody chose. Traces are batched by the OpenTelemetry SDK's own
`OTEL_BSP_*` settings, and `OTEL_TRACES_SAMPLER` applies as usual.

`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` selects the Rust services'
metric temporality: `cumulative`, `delta` or `lowmemory`, in any case, with the
per-instrument meaning the OpenTelemetry specification gives them. Unset or
blank means **delta**, not the specification's cumulative default; see
[Collector requirements](#collector-requirements). Any other value fails
startup the same way a bad cadence does. sandbox-manager forwards the setting
to the host sidecars it launches, and the guest filesystem worker inherits its
launcher's. The Go shim does not read it: its own metrics are always delta.

## Trace filtering

`SPACE_TRACE_FILTER` selects which spans are recorded, in `tracing`
`EnvFilter` syntax, independently of the log verbosity in `RUST_LOG`.

Unset or blank means one `RUST_LOG`-derived filter governs log records and
spans together, evaluated once for the whole subscriber. Setting it opts into
per-layer filtering, where `RUST_LOG` governs log records and
`SPACE_TRACE_FILTER` governs spans — more control, and one filter evaluation
per layer. An unparsable value fails startup, and the message names the
setting without repeating the directives, which can contain field values.

The connection-pool acquire target stays enabled either way, because its
events also feed the pool-acquire metric, and SQLx's slow-acquire warning
reaches logs at any `RUST_LOG` because it is the only per-pod signal that a
pool is saturated.

Enabling that target does not force the span. Whichever filter is the trace
selection is asked before a span is exported — `SPACE_TRACE_FILTER` when it
is set, `RUST_LOG` when it is not — so `sqlx::pool::acquire=off` suppresses
the span while the metric keeps recording, and a span or field directive
elsewhere in the filter does not cancel that. An enclosing span being traced
is not permission either: the rule for the acquire target decides.

Both database exports resolve their parent span through that same filtered
view, so a span the filter kept for logging alone never becomes the parent of
a detached trace.

Two things hold only with `SPACE_TRACE_FILTER` set, because they need a filter
that governs spans alone. The bridge keeps seeing spans that carry a
`db.system` field whatever the filter excludes, so the pool metric knows which
driver an acquire came from; and a rule more verbose than the forced admission
— `sqlx::pool::acquire=trace` — applies.

Unset, neither holds, as neither did before trace selection was separated. The
single filter caps that target at `info`, since the forced admission replaces
a rule for it rather than widening it. And a `db.system` span that `RUST_LOG`
hides leaves an acquire classified as PostgreSQL, so at `RUST_LOG=error` a
SQLite migration pool's wait is counted in the PostgreSQL pool metric. Set
`SPACE_TRACE_FILTER` if that distinction matters to you.

Sidecars and guest workers inherit `SPACE_TRACE_FILTER`, so a child exports the
same span set as its parent.

## Datadog trace chunks

The native Datadog tracer sends the spans a process records for one trace as a
chunk. SPACE leaves partial flush off, so a chunk is sent when the process's
count of the trace's open spans reaches zero, and a span is top-level when its
parent is not in its chunk or belongs to another service. A count that reaches
zero early sends the chunk early: every span of the trace that ends afterward
arrives in a chunk of its own, as a top-level span with its own trace metrics,
and need not carry the trace's sampling decision. OTLP backends assemble spans
by ID and show the same trace intact, so this shows only on the native path.

Spans are parented so that each request is counted once in each process it
reaches:

- A request's server span is the only span parented on the caller's
  extracted context. Handler spans are ordinary children of the server span,
  created in its scope; a handler never extracts the caller's context again.
- Work that outlives the request, or is saved and run later, starts a new root
  with a span link to the context that started it.
- A job its submitter waits for runs as a child of the waiting span, through
  `BackgroundWorker::submit_and_wait`. Any other job is a linked root.

One case remains that SPACE cannot fix. datadog-opentelemetry 0.5.2 counts no
span whose parent is remote when the process already has that trace open
(`TraceRegistry::register_local_root_span`), yet that span's end still lowers
the count. Two requests of one trace that overlap in one process, concurrent
calls or a call back into a process whose request is still open, therefore
still fragment under native Datadog until the tracer counts that span.

A change to span parenting, propagation or the trace exporter is validated by
a Fake Intake capture in Datadog mode
(`SPACE_KIND_TELEMETRY_MODE=datadog SPACE_KIND_DATADOG_MODE=fakeintake`) of a
sandbox lifecycle; see
[the Kind capture](../infra/space/kind/README.md#datadog-optional). The check
is bounded by the traces that window produced. It fails when the capture holds
no SPACE trace, or when a span other than a server or consumer span has its
parent in another process, or shares the remote parent of a request span in
its own process: SPACE parented work on remote context. It reports, without
failing, each service's chunks split by the upstream miscount above. A span
parented on context its own process saved looks the same in the chunks, so
the split report, not the failure, shows it. A capture whose workloads export
traces any other way is not checked.

## Datadog addresses

The native paths keep their existing defaults. DogStatsD goes to
`DD_AGENT_HOST`, defaulting to `127.0.0.1`, on `DD_DOGSTATSD_PORT`,
defaulting to `8125`; the Go shim also accepts a Unix socket path. Native
traces use the Datadog tracer's own defaults.

`scripts/space-cluster-smoke.py` is the exception: it has no default host and
sends no DogStatsD metrics at all unless `DD_AGENT_HOST` is set.

The smoke runner is also **warn-only**. It checks the selected sink's settings
before the suites run and prints what is wrong, but a telemetry problem never
changes the verdict a run reports — the suites decide that, and losing the
run's metrics is not a suite failure. It exports **once**, at the end, rather
than on an interval, so there is no cadence to configure.

### Restricted tag configurations

A Datadog organization can limit which tags it keeps or makes queryable for a
metric, for example with an explicit per-metric tag configuration. A tag that
configuration does not list is still sent but cannot be grouped or filtered
on. That applies to tags added after the configuration was written, including
the `reason` on `template.gc.tick` and `template.gc.tick_duration_ms` errors
and the `error_kind` on `template.build.latency_ms`,
`template.build.step.latency_ms` and `snapshot.restore.failed`. Add them to the
metric's tag configuration before relying on them; series without them are
unaffected.

## Go runtime metrics

With `SPACE_METRICS_SINK=otlp`, the shim also exports Go runtime metrics under
OpenTelemetry's standard `go.*` names, to the same endpoint and resource as its
own metrics but from a separate provider with cumulative temporality.
[Select shim telemetry](installation.md#shim-telemetry-selectors) lists the
instruments.

That set is narrower than Datadog's runtime metrics v2, which also reports GC
cycle counts, GC pause distributions, CPU time by class, and mutex wait time.

`go.schedule.duration` arrives as a cumulative explicit-bucket histogram
rather than following the sink's delta exponential profile; see
[Collector requirements](#collector-requirements). Its `Sum` is the
instrumentation's own lower-bound estimate, each observation counted at its
bucket's lower boundary, so an average derived from it reads low by up to one
bucket width. Quantiles from the bucket counts do not have that bias.

Because the two routes have different owners — the OTLP metrics sink and the
native tracer — mixing the selectors has consequences worth knowing:

- metrics `otlp` with traces `datadog` or `auto` reports runtime data twice,
  since the tracer keeps sending `runtime.go.metrics.*` to the Agent;
- metrics `datadog` with traces `otlp` or `off` reports none of it, since the
  OTLP route is inactive and the tracer that would otherwise report it is not
  running.

## Logs

Logs are not an OTLP signal here. Every producer writes to stdout or stderr
and the collection path is whatever reads a container's output. Rust services
emit structured records carrying the resolved service name and honor
`SPACE_LOG_FORMAT`. A JSON record written inside any span with a valid trace
context, sampled or not, also carries the W3C `trace_id` and `span_id` of the
nearest such span as lowercase hex, under every trace exporter; the
Datadog-reserved `dd.trace_id` and `dd.span_id` are not written. The Go shim writes plain `[space-shim]` lines to stderr
and the smoke script writes plain lines too; neither attaches a service,
environment, or version, so a log pipeline that needs those must add them from
pod metadata.

Rust attribution follows one process-wide `AttributionPolicy`, selected from the
resolved metrics sink and trace exporter before logging is installed. Native
Datadog selects `Legacy`, including automatic selection in builds with both
features even when an OTLP endpoint is configured. If either destination is
native Datadog, shared logs and spans retain Legacy attribution. Otherwise an
OTLP destination selects `Otel`; off/journald-only processes retain Legacy.
Exporter connection failure does not change this choice.

In Legacy mode, explicit JSON event `service` values retain their original
precedence over the emitting process. Shared gRPC diagnostics and connection
spans retain their original `service` field. No `peer.service` or `service.role`
is synthesized. Native Datadog logs, span names/attributes/resource identity,
and DogStatsD output are unchanged, so Datadog queries need no migration.

In OTLP mode, JSON root `service` identifies the resolved emitting process.
Shared gRPC diagnostics and connection spans use `peer.service` for the
destination. An unequal legacy event `service` becomes `service.role`, with an
explicit role taking precedence. Only OTLP queries need these attribution
changes. Other explicit fields retain their existing precedence. OTLP metric
logical-service tags use `service.role` independently of the trace destination;
native DogStatsD tags are untouched.

Before shared initialization, caller-owned logging uses Legacy without caching
that fallback, preserving the historical Datadog-default behavior while allowing
later OTLP initialization. Legacy is a migration bridge for production Datadog
continuity; remove it after native Datadog consumers migrate.

## Sandbox identity

Telemetry names a sandbox by one canonical value, its bare lowercase UUID
(`019d84bd-280d-7331-99c6-b63377f3a3a2`), in the `sandbox_id` span attribute
and JSON log field. A span for work on one sandbox carries it, in every
service that does such work: space-api's request spans for a sandbox route
and the cluster-controller's create operation, node-api's sandbox RPCs,
sandbox-manager and node-manager operations, volume-manager jobs and RPCs for
a sandbox subject, and every RPC a sandbox's net-gateway serves. A Rust
service's JSON log line written inside such a span carries the same field,
unless the event sets its own.

Two other spellings remain where they are part of a contract, and convert by
prefix:

| Where | Spelling | To the canonical value |
| --- | --- | --- |
| Native API ids, public operations' `target_id` | `sbx_<uuid>` | drop `sbx_` |
| Volume-manager `subject_id`, `job.subject_id` | `sandbox:<uuid>` | drop `sandbox:` |

A fork operation's `target_id` is already the source sandbox's bare UUID. The
reverse conversion prefixes `sbx_`; in a shell, `${id#sbx_}` gives the
canonical value.

## Request identity

Every HTTP server span, `http.server`, in space-api and the data plane carries
the request's `request_id` when it has one. A caller-supplied id is the first
non-blank of `x-request-id` and `x-correlation-id`, accepted only as at most
128 bytes of `[A-Za-z0-9._:-]`; anything else is dropped. The native API
returns that id in `x-request-id`, or, when the caller sent none or one that
was dropped, a fresh `req_<uuid>` it records instead. gRPC server spans apply
the same rule to their `request_id`. A JSON log line written inside the
request inherits it, as it does `sandbox_id`.

Each HTTP response also carries a W3C `traceparent` header naming its
`http.server` span, so a client can report the trace ID of any request it
made. The header holds only the version, trace ID, span ID and sampled flag;
it is absent when traces are off, and a proxied sandbox service's own
`traceparent` is passed through unchanged.

## Cargo features

Service and library manifests forward these features to observability:

- `otel`: neutral OpenTelemetry tracing, W3C propagation, and OTLP trace/metric
  exporters. It does not enable a Datadog SDK dependency.
- `datadog`: native Datadog traces and DogStatsD metrics. The native trace SDK
  uses the shared OTel pipeline, so this feature also enables `otel`.
- `datadog-otel`: deprecated alias enabling both, retained for one release.
  Select `otel,datadog` explicitly instead.
- `otlp-metrics`: metrics-only capability on the observability crate, for
  callers that do not need tracing.

For a neutral build, use `--no-default-features --features otel` on a service
that forwards the feature, and explicitly set `SPACE_TRACE_EXPORTER=otlp`
and/or `SPACE_METRICS_SINK=otlp`. Merely compiling `otel` does not opt in to
network export. Other workspace packages must not enable `datadog` in the
same Cargo invocation: Cargo unifies dependency features.

For example, the neutral library can be checked independently:

```sh
cargo check --locked -p pplx-space-observability --no-default-features --features otel
cargo tree --locked -p pplx-space-observability --no-default-features --features otel -e normal,build
```

## Image defaults

Both Rust image builders select `<package>/otel,<package>/datadog` for every
telemetry-enabled package, in their cache-preparation and final build steps:

| Build path | Packages built with telemetry |
|---|---|
| `docker/Dockerfile.rust-build-common`, node stack | Node API, internal API, node controller, node manager, builder, sandbox manager, spaced host, net gateway, template manager, volume manager |
| Same builder, separate runtime build | Runtime supervisor |
| Same builder, API / data plane / cluster controller stages | API, data plane, cluster controller |
| `docker/Dockerfile.rust-build-kind` | The same packages, including the separate runtime supervisor build |

The runtime image Dockerfiles consume those artifacts, and the incremental
Kind builder reads its feature list from `Dockerfile.rust-build-kind`. The
common builder's CLI-only `build-builder` stage has no telemetry features, so
those binaries export nothing. Go shim builds include both native adapters and
use no build tags (`ci/scripts/run-shim-build.sh` and
`.github/workflows/artifact-ci.yml`).

Every shipped telemetry-enabled artifact therefore resolves `auto` to native
Datadog traces and DogStatsD metrics.

CI covers the rest of the matrix: workspace checks run `--all-features`, and
the observability crate is checked on its own with no features, metrics only,
neutral OTel, native Datadog, and the deprecated alias. The neutral dependency
graph is asserted to contain no Datadog SDK.

The Kind chart sets both naming schemes — `DD_ENV` and `DD_VERSION` alongside
`SPACE_ENV`, `SPACE_VERSION`, and `OTEL_RESOURCE_ATTRIBUTES` — so a Kind
cluster reports the same identity whichever backend its selectors choose.

## Supported OTLP topology

OTLP export is an explicit alternative to the native Datadog path. The supported
internal profile is **plaintext OTLP/gRPC to an in-cluster Collector**. The
Collector (or Grafana Alloy) owns HTTPS, backend credentials, and authentication
headers for its outbound connection:

```text
SPACE service ── plaintext OTLP/gRPC, TCP 4317 ──> in-cluster Collector / Alloy
                                                        │
                                                        └── HTTPS + authentication ──> remote backend
```

This uses the same local trust boundary as an in-cluster Datadog Agent. It does
not make an unauthenticated receiver safe to expose outside that boundary.
SPACE-side OTLP TLS and authentication headers are not supported by this
profile. A per-node Collector is not required or introduced here. These settings
select metrics and traces, not a new log-export path.

For a Rust binary built with `otel`, an example configuration is:

```sh
SPACE_METRICS_SINK=otlp
SPACE_TRACE_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.telemetry.svc:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
```

Replace the example Service name with the protected receiver in your cluster.
Leave `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_METRICS_HEADERS`, and
`OTEL_EXPORTER_OTLP_TRACES_HEADERS` unset. Configure remote-backend secrets only
on the Collector/Alloy side; do not put them in the SPACE endpoint URL either.

See [Endpoints, protocol, and cadence](#endpoints-protocol-and-cadence) for the
settings, their precedence, and the endpoint rule every producer applies.

### Collector requirements

SPACE's own metrics — the `space.*` families — export **delta** temporality
by default and **base-2 exponential histograms** (maximum 160 buckets, maximum
scale 8), with a 2000-series attribute cardinality limit per instrument.
Datadog's OTLP ingest accepts both as they arrive.

That profile is the sinks', not the pipeline's. The shim's Go runtime metrics
are **cumulative**, and `go.schedule.duration`, from the runtime
instrumentation's own producer, is an explicit-bucket histogram. A Collector
pipeline therefore sees both shapes on one connection and has to accept both.

A Prometheus-style backend does not. Convert in the Collector:

- delta to cumulative, with the `deltatocumulative` processor;
- exponential histograms to whatever the backend stores — Prometheus native
  histograms keep the resolution, and converting to explicit buckets loses it.

The conversion is lossy for sparse series. `deltatocumulative` keeps each
stream's running total in memory and forgets a stream that has been idle for
its `max_stale`. The next increment then starts a new total from that
increment alone, so a counter that ticks less often than `max_stale` reads
1 → 1 across two events, or drops 2 → 1 like a counter reset, and rate
queries undercount rare events.

A pipeline that stores cumulative data can avoid that state by having the
Rust services export cumulative, with
`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=cumulative`. Each process
then reports every stream's running total on every export, so a counter keeps
its count however long it idles and the Collector passes it through
unconverted. The cost is that a stream, gauges included, keeps being exported
with its last value until the process restarts, instead of falling silent
when nothing records it. The Kind `otel` profile selects cumulative for the
Rust services.

Delta stays the default because Datadog's OTLP intake accepts only delta
sums, and drops the first point of each cumulative series it converts, which
loses exactly the rare events cumulative is meant to keep. The Go shim's own
metrics stay delta in every mode, Kind's included.

### Rejected configurations

Every producer reports the setting it rejected and what that setting has to
be. **No message repeats an endpoint or a header value** — an endpoint can
embed credentials in its userinfo and a header is a credential by definition.
A rejected protocol or selector value is quoted back, because seeing the
misspelling is what makes those diagnosable and neither can hold a secret. A
rejection is a configuration error, not a receiver outage, and it does not
select a different backend.

| Rejected endpoint | Reported as |
|---|---|
| `https://collector:4317`, or a bare `collector:4317` | `must use the plaintext http:// scheme` |
| `http://:4317` | `must include a host` |
| `http://user:pw@collector:4317` | `must not embed credentials` |
| `http://collector:0`, `:65536`, `:abc`, or a bare `http://collector:` | `must use a port between 1 and 65535` |
| `http://collector:4318/v1/metrics` | `must not include a path` |
| `http://collector:4317/?q`, or a bare `?` or `#` | `must not include a query or fragment` |
| one that does not parse | `must be a URL` |

Each of those is prefixed with the settings it applies to, for example
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT must not
include a path`. The remaining rejections name their own setting:

Under `auto`, the same messages appear as warnings and the signal is disabled
instead.

| Rejected | Reported as |
|---|---|
| a protocol other than `grpc` | `OTLP metrics currently require the grpc protocol, got "http/protobuf"`; `OTLP traces require grpc, got "http/protobuf"` |
| nonblank OTLP headers | `OTLP exporter headers are not allowed with the plaintext internal metrics profile`; `OTLP traces do not allow OTEL_EXPORTER_OTLP_TRACES_HEADERS or OTEL_EXPORTER_OTLP_HEADERS on a plaintext endpoint`; in the shim, `OTEL_EXPORTER_OTLP_METRICS_HEADERS is not allowed with the plaintext OTLP metric profile` |
| no metrics endpoint | `missing OTLP endpoint; set OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT` |
| a bad cadence | `OTEL_METRIC_EXPORT_INTERVAL must be a positive whole number of milliseconds` |
| an unknown temporality | `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE must be cumulative, delta, or lowmemory, got "monotonic"` |
| an unknown selector | `invalid SPACE_METRICS_SINK="dogstatsd"; expected one of: auto, datadog, journald, otlp, off`; `invalid SPACE_TRACE_EXPORTER="dd"; expected auto, datadog, otlp, or off` |
| a selector the build cannot serve | `SPACE_METRICS_SINK="datadog" requires the datadog feature`; `trace exporter otlp requires the otel feature` |
| `journald` in the shim | `SPACE_METRICS_SINK=journald is not supported by the Go shim; use datadog, otlp, or off` |
| a bad trace filter | `invalid SPACE_TRACE_FILTER; expected tracing EnvFilter syntax` |

Service-level initialization adds its own prefix, such as
`invalid OTLP metrics configuration:` or `trace initialization failed:`.

The endpoint rule itself lives in one Rust module, `otlp_endpoint`, and in
`validateOTLPEndpoint` in [the shim's OTLP setup](../shim/otlp.go). The
settings around it are read in
[Rust metrics validation](../crates/libs/observability/src/sink/otlp.rs) and
[Rust trace validation](../crates/libs/observability/src/traces.rs), and the
selector errors are declared in
[the observability crate root](../crates/libs/observability/src/lib.rs).

### Operator requirements

- Keep the receiver internal. Apply an ingress **NetworkPolicy limiting TCP
  4317 to the intended SPACE namespaces**, with narrowly selected producers
  where possible. Use a CNI that enforces the policy; do not expose the receiver
  through a public load balancer or an unprotected node port.
- **Sandbox guests must not reach the Collector.** Namespace selection alone
  is not proof of guest isolation: gateway proxying or address translation can
  change the source identity seen by the receiver. Verify the guest boundary
  against the deployed Service and node addresses, including every enabled IP
  family. Treat Datadog Agent UDP 8125 and TCP 8126 as protected telemetry
  endpoints too.
- Give Collector/Alloy outbound access only as needed, use HTTPS with certificate
  verification to the remote backend, and keep authentication secrets in its
  own secret configuration.
- Before rollout, check that allowed SPACE producers can export and guest
  workloads cannot connect. Repeat this check when receiver placement, cluster
  address ranges, or network policy changes. This document specifies deployment
  requirements; it does not install policies or certify a running cluster.

The filesystem identity worker runs **inside the guest**, unlike host-side
`space-net-gateway` and `space-spaced-host`. Forwarding exporter environment
settings to that worker does not create a privileged route through the guest
boundary. Do not open guest access to the Collector just to make its direct
exporter work; guest telemetry needs an independently reviewed collection path.

## Reconnect attribution baseline

Measure ECW reconnects at the API HTTP server span, including both
`POST /sandboxes/{sandboxID}/connect` and
`POST /v2/sandboxes/{sandboxID}/connect`. Its duration includes the API wait for
readiness; a worker restore duration does not. Compare this server baseline
with the caller's latency, error and cancellation counters for the same window.
A cancelled caller may leave server work running. Missing HTTP status is
`unknown`, not evidence of cancellation or success.

The connect handler records the existing `SPACE_API_CLIENT_CLASSES`
classification as `client_class` (`interactive`, `cron`, or `other`). Configure
caller IDs in deployment configuration; raw IDs are not added to this field.
Unmapped callers are `other`; requests rejected by middleware or extractors
before reaching the handler may have no class. Do not silently count either as
interactive traffic.

Two independent signals describe a restore:

| Signal | Evidence and limits |
| --- | --- |
| API `wake_outcome=not_required` | The connect completed without requesting a wake. This is the warm cohort. |
| API `restore_kind`, `restore_kind_basis=placement` | Snapshot source node compared with actual pod binding. `in_place` and `cross_node` describe placement, not proof of local snapshot reuse. |
| Worker `restore.checkpoint.resume`, `remote_snapshot_staged` | Checkpoint resume was reached after source selection/staging. `true` means remote snapshot provenance was selected/staged in this start request, including reuse of staging earlier in that request; `false` means local checkpoint provenance. This does not measure bytes downloaded or prove a successful resume. |

Remote staging can happen on the source node when local artifacts are missing.
Keep snapshot source and placement as separate dimensions. Failures before
checkpoint resume, resident VM resumes, cold starts, missing spans and older
workers remain unknown. Multiple checkpoint resumes under a request remain
ambiguous rather than selecting the last successful attempt.

Attribution requires updated API and worker instrumentation and retained trace
relationships. Either deployment order is compatible, but partial rollouts
have incomplete evidence. Use exact parent relationships or explicit leader
links when correlating a connect with worker activity; a shared sandbox ID and
nearby timestamp are insufficient.

Retained traces are a diagnostic sample. Tail retention and missing spans can
bias cohort shares and percentiles. Use independently counted caller requests
for population latency, error and cancellation rates; restore counters and
reconnect-attempt counters do not necessarily count one event per connect.
