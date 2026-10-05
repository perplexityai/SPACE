# Observability builds and selectors

[Documentation](../README.md)

SPACE emits metrics and traces either through Datadog's native clients or
through OpenTelemetry over OTLP. Each signal is selected independently, by the
same rules in the Rust services and the Go shim. Logs are not part of this
selection; see [Logs](#logs).

## Gateway inspection populations

`gateway.inspection.admission` remains one terminal outcome for each non-reserved-DNS
TCP flow reaching the handler. Its existing `outcome` and `reason` values are
unchanged: `reason` retains peek/certificate detail such as `first_byte_timeout`.
Additive dimensions separate the populations:

| Tag | Values / meaning |
| --- | --- |
| `tls_interception` | `on`, `off`, `unknown`, from the captured admission policy, never a later inventory lookup |
| `stage` | `policy_readiness`, `client_hello`, `destination_policy`, `policy_validation`, `pre_tls`, `handoff` |
| `path` | `unknown` before routing, then `tunnel`, `host_bound_http`, or `tls_inspection` |
| `policy_reason` | `not_evaluated`, `allowed`, `default_deny`, `explicit_deny`, or `platform_guardrail` |

The destination verdict includes DNS-required host binding. A denial with
`reason:first_byte_timeout` can therefore retain `policy_reason:default_deny`;
an independently allowed destination refused during pre-TLS setup retains
`policy_reason:allowed`. `policy_changed` is a cancellation detail, not a new
destination verdict: it retains the original policy's mode and verdict.
`interception_not_ready` proves the installed policy was On; other readiness
failures remain unknown. Cancellation while waiting for policy is unknown,
while cancellation during the peek retains the mode already captured.

Off denials are deliberately still `denied`, not `skipped`. To study enabled
inspection, explicitly filter `tls_interception:on`; exclude
`outcome:skipped` only if the question excludes admitted plaintext. Do not
divide a selected On numerator by the unfiltered admission population.
Admission success means the connection was handed to TLS termination, not that
a handshake or a request completed. Reserved DNS is excluded as before.

`gateway.inspection.session` is a new, separate terminal counter, emitted once
for each admitted inspected connection. It has the same tag keys, fixed
`tls_interception:on`, `path:tls_inspection`, and `policy_reason:allowed`.
`stage:guest_handshake` includes transport setup and guest-handshake failures;
the stage advances to `session_forwarding` only after the guest handshake
actually succeeds. Success is `reason:completed`; failure reasons use the
bounded TLS transport vocabulary. Lease invalidation / gateway withdrawal are
cancellations, and dropping unfinished work emits `cancelled/work_dropped`
at the last reached stage. This is a connection population, not a per-request
or origin-handshake population: lazy origin setup can fail during forwarding,
and a guest may close without ever contacting an origin.

The existing `gateway.tcp.connection` counter is unchanged and still includes
later TLS failures in `rejected/tls_interception`. The existing log
`stage:tls_session` can contain propagated guest-handshake failures; use its
new `processing_stage` field or the session counter rather than treating that
log stage as proof of a completed handshake. No policy references, sandbox IDs,
hostnames, or destination addresses are added to metric tags.

Both counters use the shared metric emitter, so DogStatsD and OTLP carry the
same dimensions. Existing ungrouped totals and `outcome`/`reason` filters retain
their meaning, but the additional tags create new series. Old data and old
binaries lack these dimensions; absent mode must not be interpreted as Off.
For Datadog Metrics without Limits configurations, operators must review the
tag allowlist for `gateway.inspection.admission` and the new
`gateway.inspection.session`, and enable `tls_interception`, `stage`, `path`,
and `policy_reason` before using them in grouping/filtering. DogStatsD uses
the configured namespace prefix; OTLP backend translation may normalize the
metric name. Verify the actual emitted name in the target backend. Index
activation, dashboard migration, rollout, and production queries are not
performed by this code change.

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
the constant `service` point attribute and resource `service.name` identify
the emitting process. This keeps different peers or interfaces in separate
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
| `service.instance.id` | host name and process id |
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

## Go runtime metrics

With `SPACE_METRICS_SINK=otlp`, the shim also exports Go runtime metrics under
OpenTelemetry's standard `go.*` names, through the same provider and resource
as its own metrics. [Select shim telemetry](installation.md#shim-telemetry-selectors)
lists the instruments.

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
`SPACE_LOG_FORMAT`. The Go shim writes plain `[space-shim]` lines to stderr
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
and **base-2 exponential histograms** (maximum 160 buckets, maximum scale 8),
with a 2000-series attribute cardinality limit per instrument. Datadog's OTLP
ingest accepts both as they arrive.

That profile is the sinks', not the pipeline's. `go.schedule.duration` comes
from the runtime instrumentation's own producer, which bypasses the sink's
temporality and aggregation selectors, so it arrives as a **cumulative
explicit-bucket** histogram. A Collector pipeline therefore sees both shapes on
one connection and has to accept both.

A Prometheus-style backend does not. Convert in the Collector, not in SPACE:

- delta to cumulative, with the `deltatocumulative` processor;
- exponential histograms to whatever the backend stores — Prometheus native
  histograms keep the resolution, and converting to explicit buckets loses it.

Exporting cumulative sums from SPACE instead is not an option here: the sinks
are shared with the DogStatsD path, which is delta by construction.

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
