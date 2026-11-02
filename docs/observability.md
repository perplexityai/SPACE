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
| `off` | off | off | off |

`journald` is no longer a metrics sink. Naming it is a configuration error, the
same as any unknown selector.

Every Go shim build compiles in both native Datadog clients, so the shim has
one column:

| Selector | `SPACE_TRACE_EXPORTER` | `SPACE_METRICS_SINK` |
|---|---|---|
| unset / `auto` | native Datadog | DogStatsD |
| `datadog` | native Datadog | DogStatsD |
| `otlp` | OTLP | OTLP |
| `off` | off | off |

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
not turn into DogStatsD — a degraded signal is off, not redirected.

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

## Untraced background traffic

Some calls repeat every few seconds and almost always succeed, and a root trace
for each would outnumber every request's. They are left untraced, whatever the
filter:

- `grpc.health.v1.Health` calls made outside a traced request: on the client,
  with no traced span current; on the server, with no trace context from the
  caller. A health check a request makes, such as a wake path's capability
  probe, keeps both spans.
- GET requests for `/health`, `/healthz`, `/readyz` and `/livez`. Probes report
  their own result. The data plane keeps every request to a sandbox host
  traced whatever its path: a sandbox app's `/health` is the sandbox's own
  traffic.
- A `grpc.client.connect` made while no traced span is current, that is a
  background dial, records a span only when it fails, as a root carrying the
  error. A dial inside a traced request is that request's child, as before.
- A Kubernetes read (`get`, `list`, `watch`) made while no traced span is
  current records a `kubernetes.client` span only when it fails: an error, an
  error status, a response body that fails, or a call dropped before its body
  completes, as by a caller's timeout. A single-object `get`'s 404 is an
  existence check's answer, and a watch its consumer drops has not failed. The
  span opens when the failure is known and carries the call's latency as
  `k8s.duration_ms`. Writes keep their spans, because a write is a change.
- The cluster-controller's leadership lease records `kubernetes.client` spans
  only for failed calls, renewals included; a 409 on its create or replace is a
  lost race, not a failure. A tick that acquires or loses leadership records
  `leader_election.change`.

Other gRPC calls made while no traced span is current, such as periodic
polls, keep their spans so APM stats still count them, but start no sampled
trace. The client span records `untraced_caller` and takes as its parent a
fresh unsampled context that nothing records, so both samplers record it
without sampling it, and the call propagates that unsampled context to its
server, whose spans are recorded unsampled too. The native Datadog sampler
gives such spans the auto-reject priority; under OTLP the wrapped sampler
records every span whose parent was not sampled unsampled, whatever
`OTEL_TRACES_SAMPLER` selects, Kind's `always_on` included. A failed call
still reaches the backend: the Datadog exporter keeps a rejected chunk that
contains an error for the Agent's error sampler (a sampling rule's user reject
would drop it), and under OTLP an unsampled span is exported only when it
failed. Such a span's parent is the unrecorded context, so it heads its trace.
The shim does the same for its calls outside any shim operation, such as
containerd relaying kubelet's stats polls: it sends a fresh unsampled context.
Both mark that context with the `tracestate` member `space=untraced`, which its
descendants carry across services. Work that follows such a call, such as a
job, a reconcile ref or a detached task, does not save it as its origin, since
no request was sampled out, and is sampled as a root of its own. The member
counts only on an unsampled context: a sampled one is saved whatever its
`tracestate` carries.

## gRPC client span status

A `grpc.client` span that ends in a non-OK status is marked ERROR, except for
an answer the call expects: a readiness poll's FAILED_PRECONDITION, a guest
file or process operation's application status, and a code the caller marks
on that one call with `ExpectedGrpcStatus`. A marked call records the code in
`space.grpc.expected_status` whatever the answer. net-gateway marks its policy
pulls PERMISSION_DENIED until sandbox-manager has registered it: a pull sent
before the owner's first control call, within ten seconds of launch, and short
of the last pending attempt. A marked pull's refusal is always retried; the
refusal that ends the wait is still an ERROR span. Platform codes such as
UNAVAILABLE and INTERNAL stay errors even when marked.

## S3 client spans

Every S3 operation SPACE issues through the shared S3 HTTP client, the object
store's and those of snapshot storage and the runtime manager's kernel source,
gets one `s3.client` span named `S3.<Operation>`. It covers the operation's
retries and backoff; `s3.attempts` counts the requests, and each failed attempt
is logged and counted where the client tracks attempts. The span carries
`s3.outcome` (`ok`, `not_found`, `error`, `abandoned`), `http.status_class` of
the last response, and `s3.request.bytes` and `s3.response.bytes` when the
messages declare a length. `not_found` is reserved for a `HeadObject` or
`GetObject` 404, an existence check's answer, and keeps status OK; any other
operation's 404 fails it. A failure sets status ERROR and records why as
`s3.failure_kind`, from a fixed set (`transport`, `connector_timeout`,
`timeout`, `throttled`, `http_5xx`, `access_denied`, `not_found`, `http_4xx`,
`service_error`, `abandoned`, `no_result`, `other`), which is also its status
description; the SDK's and the server's error text, which can quote a key, is
never recorded. No bucket, key, upload id or range is recorded either. A
ranged download's chunk keeps its own `S3.GetObject.range` span, which covers
reading the chunk's body as well, so each chunk shows that span with this
request span inside it.

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
- A public operation saves the `traceparent` of the request that created
  it, and each claimed attempt runs in the cluster controller as a fresh
  `public_operation.execute` root, a consumer span with one link to that
  request. It is not a child of the request: operations that share one
  caller's trace run concurrently in the dispatcher, where the native Datadog
  tracer would split that trace, and an operation retried for as long as its
  admission keeps it waiting would grow the request's trace without bound. A
  request's operations are found by `origin_trace_id` (the request's trace
  id), `operation_id` or `sandbox_id`, or through the link; they are not
  inside the request's trace. The executor's work and its node calls are
  children of the attempt.
- Reconciliation is fan-in: one pass applies refs that many writers queued.
  Each ref saves its latest writer's `traceparent`, except that an unsampled
  writer never replaces a sampled one, and a pass is a root that links to the
  saved contexts of the refs it claimed.
- Each `shim.create` joins its Pod's annotated context; invalid/missing context
  starts a plain root. Kubelet retries intentionally grow the same trace,
  distinguished by `task_id`. Teardown still links to its task's create.

The shim's teardown spans, `shim.kill` and `shim.delete` with their
`shim.sbxmgr.teardown_release` child, wait at most 45 seconds for sbx-manager
to make a generation-fenced execution's suspend durable, remote upload
included. A wait that ends while sbx-manager still answers `PENDING` is the
retry containerd needs, not a failure: the task keeps running, and those spans
end with unset status and `teardown.release=pending`. Every other outcome stays
an error: a refusal, including one that arrives as the deadline passes, an
unknown outcome, or a deadline before any `PENDING`. The
`space.shim.operation.error` counter still counts a pending wait under
`step:prepare_teardown_suspend`, so it counts every retried release. Watch a
prolonged suspend through how long one sandbox's teardown spans keep recording
`teardown.release=pending`, or through pods stuck `Terminating`, rather than
through span errors.

A root that links to the context that caused it records that context's trace
id as `origin_trace_id`, since a link is not searchable, and whether it was
sampled as `origin_sampled`. Its sampling follows that origin: it is kept when
any origin it links to was sampled and dropped when none was, so a kept
request keeps the work it caused. A root with no origin keeps the configured
sampler. Under OTLP the sampler that `OTEL_TRACES_SAMPLER` selects is wrapped
to read the links of a root that records `origin_sampled`. The shim's wrapper
also makes every span with a parent follow it, sampled or recorded unsampled,
so a sampler that is not parent-based, such as Kind's `always_on`, cannot
sample the children of a dropped `shim.create` and send their sampled
`traceparent` on. The native Datadog samplers do not read links, so two
sampling rules on `origin_sampled`, ahead of the ones
`DD_TRACE_SAMPLING_RULES` configures, keep (`1.0`) or drop (`0.0`) such a
root. The Rust services and the shim apply the same rules to their linked
roots. A root linked without `origin_sampled` keeps the configured sampler on
both paths. A root links to at
most eight origins, sampled ones first, so one with any sampled origin always
links it and is kept. Datadog's rate limit (`DD_TRACE_RATE_LIMIT`) still
applies to a kept root, and sampling rules sent
by Remote Configuration replace the whole list, these two included.

A node-ctl `node_ctl.pass` root covers one claim of cluster reconcile refs,
from a trigger or the 60-second timer (`cause`). It exists only for a claim
whose refs saved trace context, which it links to sampled first, then in the
order the claim picked the refs (longest queued first), at most eight,
counting the rest in `links.dropped`; it parents the claim's facet
`node_ctl.reconcile` spans and completion RPC, and records `refs` and
`failed`. A claim of refs that no traced writer queued is background work and
gets no root, unless it fails: whether it fails is known only once it has run,
so a childless `node_ctl.pass` reports the failure afterward. A tick that
claims nothing emits nothing.

The cluster-controller's warm-pool reconciler runs each pass, every five
seconds on the leader, under a `warm_pool.pass` root. No request causes a pass,
so it links to nothing and the configured sampler decides it. That is one
root trace per pass, 720 an hour, which under native Datadog counts against
the Agent's sampling budget with the node's other traces. The root parents the
pass's Postgres, Kubernetes and sandbox-manager calls and every line it logs,
records `configs` and `failed_configs`, and is an error when the pass fails or
any config does not reconcile. The pass logs one summary, `warm pool reconcile
completed` (`warm pool reconcile failed` at WARN when the pass fails), with
`reconciled_configs`, `failed_configs` and each per-config stage's
`<stage>_result`, longest `<stage>_duration_ms` and `<stage>_failed_configs`. The per-stage `warm pool: stage start` and `warm
pool: stage done` lines are DEBUG; a failed stage still logs `warm pool: stage
done` at WARN with its config and error.

Every asynchronous root records the `sandbox_id` of the sandbox it works on:
an operation's target, a job's subject, a detached task's sandbox. A pass
records `sandbox_id` when its refs name one sandbox, and `sandbox_ids`, a
comma-separated list of at most sixteen, whenever they name any, with the
rest counted in `sandbox_ids.dropped`.

Each asynchronous root also names its Datadog operation, so it has its own
APM series: `public_operation.execute`, `job.<kind>`, `node_ctl.pass`,
`warm_pool.pass`, `sbxmgr.detached`, `netgtw.startup` and
`leader_election.change`. An internal span that names
none falls back to the operation `internal`, shared by every such span of the
service.

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
`template.build.step.latency_ms` and `snapshot.restore.failed`, and the
`fault` on `gateway.tcp.connection`. Add them to the
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
Datadog-reserved `dd.trace_id` and `dd.span_id` are not written. A process
outside Kubernetes defaults to plain text, so the host runtime-supervisor's
systemd unit sets `SPACE_LOG_FORMAT=json` and its journal lines are the same
JSON records; the per-sandbox net-gateway and spaced-host it launches share
its journal stream and are launched with JSON too. The Go shim writes plain `[space-shim]` lines to stderr
and the smoke script writes plain lines too; neither attaches a service,
environment, or version, so a log pipeline that needs those must add them from
pod metadata.

Rust attribution follows one process-wide `AttributionPolicy`, selected from the
resolved metrics sink and trace exporter before logging is installed. Native
Datadog selects `Legacy`, including automatic selection in builds with both
features even when an OTLP endpoint is configured. If either destination is
native Datadog, shared logs and spans retain Legacy attribution. Otherwise an
OTLP destination selects `Otel`; processes with both signals off retain Legacy.
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

## Net-gateway TCP connections

`gateway.tcp.connection` counts one point for every TCP connection that reaches
the net-gateway's per-connection handler. Every point carries the same three
tags:

- `status`: `cancelled`, `rejected`, `relayed`, `handled` or `relay_failed`.
- `detail`: the bounded detail within `status`, such as `connect` or
  `transfer` under `relay_failed`.
- `fault`: on `status:relay_failed,detail:transfer`, what failed first: the
  relay leg and operation, an unattributed relay failure, or a reserved-DNS
  exchange; `none` on every other point.

| `fault` | Meaning |
| --- | --- |
| `guest_read`, `guest_write`, `guest_flush`, `guest_shutdown` | The first failing socket operation in a pass-through relay was on the guest leg. |
| `origin_read`, `origin_write`, `origin_flush`, `origin_shutdown` | The first failing socket operation in a pass-through relay was on the destination leg. |
| `relay_io` | A pass-through relay failed without recording a failing leg. Not expected. |
| `reserved_dns` | A reserved-DNS-over-TCP exchange failed: the resolver returned an empty reply, or writing the reply to the guest failed or timed out. No relay ran, so no leg is named. |
| `none` | Every other outcome, including connect-phase failures. |

`fault` records the first failing leg and operation the relay observed, not
which peer caused the failure. A destination that stalls can make the guest
time out and reset, which reads as `guest_read`. Splitting
`gateway.tcp.connection{status:relay_failed,detail:transfer}` by `fault`
separates guest-leg from destination-leg failures. A Datadog organization with
a restricted tag configuration for this metric must list `fault` before it can
be grouped or filtered on; see [Restricted tag configurations](#restricted-tag-configurations).

### Known-egress logs

The net-gateway names failed flows to the exact domains it was launched with
(`--known-egress-domain`). A DNS query matches by its query name and a TCP flow
by its TLS SNI, so only TLS flows whose SNI names a configured domain are logged.
Plaintext and host-bound HTTP flows, and reserved DNS over TCP, carry no SNI:
they are counted only by `gateway.tcp.connection`, not logged here. These
events use the log target
`pplx_space_net_gateway::known_egress` and carry `sandbox_id`,
`original_dst_port`, `destination`, `protocol` and `reason`.

- ERROR `failed to connect to known egress destination`: every failed matched
  DNS query or TCP flow except a pass-through transfer failure. That covers
  rejections and failed connects. It also covers inspected TLS/HTTP failures,
  including those after the upstream connected.
- INFO `known egress destination failed an established connection`: a
  pass-through relay that connected and then failed first on the destination
  leg, or without a recorded leg (`fault:relay_io`).
- DEBUG `guest side failed an established known egress connection`: a
  pass-through relay that connected and then failed first on the guest leg.
  These events are visible only when the gateway's log filter enables DEBUG
  for that target.

The INFO and DEBUG events add `fault`, with the same pass-through values as
the metric tag above. Each of these failures is also counted by
`gateway.tcp.connection{status:relay_failed,detail:transfer}`, with the same
`fault`, so no log level is needed to count transfer failures.

## Firecracker and guest diagnostics

Firecracker and the guest's spaced write nowhere a collector reads, so
sandbox-manager reads what they produce on its node and re-emits it.

Firecracker writes a metrics flush every 60 seconds to a FIFO node-manager
creates in its jail. Firecracker opens it non-blocking, so a flush the reader
has not drained is dropped and counted rather than stalling the VMM. These
node-wide metrics carry no sandbox id:

| Metric | Type | Tags |
| --- | --- | --- |
| `sandbox.firecracker.errors` | counter | `component` (`block`, `net`, `vsock`, `vcpu`, `vmm`, `seccomp`, `signal`), `error` (the Firecracker field) |
| `sandbox.firecracker.block.mean_latency_ms` | timing, one sample per sandbox and flush with block requests | `op` (`read`, `write`) |
| `sandbox.firecracker.operation.duration_ms` | timing, once per VM pause, resume, snapshot load or create a flush reports | `operation` |
| `sandbox.firecracker.metrics.flushes` | counter | `outcome` (`parsed`, `unparsable`, `oversized`) |
| `sandbox.firecracker.telemetry_dropped` | counter, flushes or log lines Firecracker could not write | `stream` (`metrics`, `log`) |
| `sandbox.vm.diagnostics` | counter, forwarded Firecracker ERROR lines and guest WARN and ERROR diagnostics | `source` (`firecracker`, `guest`), `level` (`warn`, `error`), `outcome` (`logged`, `rate_limited`, `dropped_in_guest`) |

The `error` values are listed in `crates/libs/metrics/src/sandbox/vm.rs`.
Firecracker's flow-control counters (`block` `no_avail_buffer`, `net`
`no_rx_avail_buffer` and `no_tx_avail_buffer`) are not among them: they count
times the guest had not yet posted a buffer, which the device retries, not
failures. Nor is `signals` `sigpipe`: Firecracker raises SIGPIPE when it writes
guest vsock data to a host peer that has gone, and `vsock` `tx_write_fails`
counts that write once.

Firecracker's ERROR log lines are re-logged by sandbox-manager as
`firecracker error` with `sandbox_id` and the line in `firecracker_message`, at
most 20 in a burst and then one every 15 seconds per sandbox;
`suppressed_since_last` counts the lines skipped. A line can carry
guest-written values, so it is cut to 512 bytes and its control characters
are replaced. WARN, INFO and DEBUG lines are not forwarded; Firecracker warns
on every snapshot restore. `sandbox.vm.diagnostics` is therefore not emitted
with `source:firecracker` and `level:warn`. The watchdog's failure-time
evidence still includes the tail of Firecracker's log, WARN lines included.

spaced records its own WARN and ERROR callsites (level, target, source file and
line, and a count), never message text or field values, which can carry a
user's command, path or output. The health probe drains them over the bridge's
`Ping` about every 24 seconds, and sandbox-manager logs up to four per drain as
`spaced guest diagnostic` with `sandbox_id` and `guest_level`, `guest_target`,
`guest_file`, `guest_line` and `guest_count`, at WARN whatever the guest's
level. The guest is tenant-controlled: the host decodes at most 16 KiB of a
`Ping` answer, reads at most 32 entries, clamps each count and bounds and
strips each string. What a restored guest had recorded happened in the
sandbox its snapshot came from: the guest discards it on a snapshot-restore
resume notice, and since a restore that ends paused sends none, sandbox-manager
also drops the first drain after every restore. A guest that predates the
field sends none.

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

A lifecycle request's entry span carries it too, not only its children, so a
search by `sandbox_id` finds the request itself: space-api's `http.server`
span for a sandbox route or a create, and the `grpc.server` span of
node-api's and sandbox-manager's lifecycle RPCs (create, start, wake, stop,
delete, suspend, pause, resume, kill, rollback and pool adoption). A create
that names no sandbox records the one it allocated when it returns.

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
request inherits it, as it does `sandbox_id` and a public operation's
`operation_id`. To follow a request into the work it caused, use its trace
ID: the response's `traceparent` names it, log lines carry it as `trace_id`,
and linked roots, an operation's attempts included, record it as
`origin_trace_id`.

Each HTTP response also carries a W3C `traceparent` header naming its
`http.server` span, so a client can report the trace ID of any request it
made. The header holds only the version, trace ID, span ID and sampled flag;
it is absent when traces are off, and a proxied sandbox service's own
`traceparent` is passed through unchanged.

## Sandbox events and traces

A `space sandbox event` line (`event_stream` `space.sandbox`) is written
synchronously where the transition happens, so it carries `trace_id` and
`span_id` exactly when that code runs inside a traced span. Work a traced
request waits for runs in the request's span, so its events carry the
request's trace ID:

- node-manager's sandbox cleanup, which runs in a task the `CleanupSandbox`
  call that started it owns;
- sandbox-manager's lifecycle cleanup admission, which its RPC awaits.

Work that outlives the request runs under a root linked to it, which records
the request's trace ID as `origin_trace_id` and is sampled as the request was;
its events carry the root's trace ID:

- a sandbox-manager durable cleanup attempt is a `job.execute` root
  (`job.sandbox_cleanup`), never a child of its caller, since a caller such as
  Stop can stop waiting while the attempt runs. It links to the context saved
  with the job and to the traced caller waiting for it, if any. The first
  attempt, and one a caller waits for, is sampled as those origins were; a
  later retry still links to them but takes the configured sampler, through
  `link_root_to_origins_without_following`, so a job that keeps failing does
  not keep a trace every retry;
- sandbox-manager's cluster-deleted eviction teardown runs under
  `sbxmgr.detached` (`EvictClusterDeletedSandbox`), linked to the
  `EvictClusterDeletedSandbox` call: node-ctl gives that call a 10-second
  deadline and treats its expiry as an eviction still in progress, so the
  teardown can outlive it. A call from an untraced caller gets no root;
- a network-policy reconcile attempt that a traced request queued runs under
  `sbxmgr.detached` (`NetworkPolicyReconcile`), linked to the latest traced
  request that queued it since the previous attempt, except that an unsampled
  request's context never replaces a sampled one;
- a net-gateway that sandbox-manager launched from a traced request receives
  the launch's context as `TRACEPARENT`, and its startup events and first
  policy pull run under a `netgtw.startup` (`NetGatewayStartup`) root linked
  to it.

No root is created for events with no request behind them: the
network-policy reconciler's periodic sweep (every live sandbox once a minute)
and its retries; startup recovery, the inventory, cache and quarantine
reconcilers, cold-sandbox eviction and the snapshot scheduler; a cleanup job
no traced caller persisted, such as one startup recovery schedules; a
net-gateway the watchdog relaunched, its policy retries and rehydrations, and
its `network_path_stopped` at shutdown, which a signal causes. Their events
carry no trace ID, except those a callee writes while serving such work's
call, which carry the unsampled context of
[untraced background traffic](#untraced-background-traffic); work that
follows that context does not save it as its origin. Due auto-lifecycle
transitions keep their own `AutoLifecycle` root.

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
| an unknown selector | `invalid SPACE_METRICS_SINK="dogstatsd"; expected one of: auto, datadog, otlp, off`; `invalid SPACE_TRACE_EXPORTER="dd"; expected auto, datadog, otlp, or off` |
| a selector the build cannot serve | `SPACE_METRICS_SINK="datadog" requires the datadog feature`; `trace exporter otlp requires the otel feature` |
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

## Rootfs materialization

Template pulls trace `materialize_rootfs` under `pull_in_scope`. Its
`decompress_rootfs` child has `rootfs.setup`, `rootfs.pipeline` (with a
`rootfs.finalize` child for the tail and thread joins), `rootfs.verify` and
`rootfs.publish`. The uncompressed path uses `rootfs.raw_setup` and
`hash_rootfs_file`.

Each concurrent stage gets its own span: `rootfs.read_compressed` (local
artifact reads), `rootfs.hash_compressed` (artifact SHA-256),
`rootfs.zstd_decode`, `rootfs.hash_decoded` (template content hash),
`rootfs.write_sparse` and, on the uncompressed path, `rootfs.hash_raw`. The
`stage` attribute repeats the name without the prefix for queries, and hash
stages set `hash_algorithm`. Each records `busy_ms`, `recv_wait_ms`,
`send_wait_ms`, `wall_ms` and `bytes` when it ends, including on error.
Counters are taken once per pooled chunk. On success only, the decoder also
records `read_ms` (inclusive zstd read time; `busy_ms` subtracts the nested
compressed-input time), and the writer records `data_bytes`, `hole_bytes`
(logical sparse classification) and `file_io_ms` (writes, truncates, seeks
and flushes; the rest of its busy time is zero scanning and buffering). A
failed stage span carries just the common fields.

The stage with high `busy_ms` and low waits is the limiter. Stage wall times
overlap and must not be added, and busy is elapsed time, not CPU time.

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

`sandbox.reconnect.attempts` and `sandbox.reconnect.duration_ms` also carry
`client_id`, and `sandbox.api.client_concurrency.decision` carries it next to
`client_scope`. Its value is the caller's trimmed `X-Client-ID` when the per-client
concurrency limiter lists it (`SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS` or a
runtime quota override key) as a valid, unreserved entry (see
`docs/configuration.md`), else `unknown`, so it names allowlisted callers that
`client_class` folds into `other` without letting a caller mint new series. On
the limiter decision, `client_scope:unspecified` always reports `client_id:unknown`.
A process names at most 64 override-only callers over its lifetime; past that,
a new override-only caller keeps its own limiter bucket but reports
`client_id:overflow`. Which callers fit is first-come per process, so replicas
can disagree once the cap fills. While the limiter is off it resolves against
the static list only. Reconnects through the data-plane entrypoint aren't
attributed and report `unknown`.
Deployments that filter tags by key must keep `client_id` on these metrics.

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

## Sandbox create shape

`create_completed` events carry the resolved `vcpu` and `memory_mib` and a
`shape_source` naming where they came from: `request`, `pool_config` (an
exact-release pool config or claimed warm slot), `policy` (a
`retain_shape_on_miss` warm-pool policy), `build` (the release's build shape),
or `default`. A partial request reports the fallback that filled the gap. The
`space.sandbox.api.create.shape_source` counter is tagged `shape_source`,
`runtime` and `template`. A non-zero `shape_source:default` count means creates
fell back to the platform default shape; those events omit `vcpu` and
`memory_mib`. Direct creates and forks omit `shape_source`.

## Reserved DNS upstream connections

The net-gateway resolves guest queries to the reserved resolver addresses over
DNS-over-TLS through Hickory. Upstreams close idle connections routinely.
Hickory would report a send on a closed connection as `Busy`, treat it as
backpressure, and after a few closed pooled connections fail the lookup with
`NoConnections`, which makes the gateway rebuild the resolver and drop its
cache.

The gateway learns when Hickory's task for a connection exits, so such a send
never reaches Hickory. A request that times out or fails in transport marks its
connection failed too: a connection can stall without closing, and Hickory
would otherwise keep every other pooled connection that shares it until each
timed out in turn. A send on a closed or failed connection moves to the newest
usable connection to the same upstream, or to the one reconnect that every
such send shares. The reconnect runs to completion even if those sends are
cancelled. The connection count shrinks back to one after a burst. Each of
these sends counts once in `gateway.reserved_dns.upstream_closed`, tagged
`resolver` and `outcome`: `reused`, `reconnected`, or `failed` when the
reconnect failed and no connection was usable.

These still warn and count as before:

- `reserved DNS upstream connection failed` for any failed connection,
  including a failed reconnect;
- `reserved DNS upstream request failed` for every error Hickory receives,
  including a failed reconnect and `Busy` from an open connection at its
  request limit;
- `reserved DNS resolver replaced after NoConnections`, now reached by real
  connection failures rather than idle closes;
- the guest-facing `reserved DNS request failed` warning and
  `gateway.reserved_dns.request.duration_ms`, both with `reason` `transport`,
  for lookups that fail with `NoConnections`; Hickory reports exhausted `Busy`
  as `NoConnections` too, so this path does not produce `resolver_busy`.

`gateway.reserved_dns.upstream.request.duration_ms` times each query the
gateway hands to an upstream connection, from the send to its first response
or failure, including any wait for a shared reconnect. It is tagged `resolver`
and `outcome`: `answered`, `timeout` or `no_response` (Hickory's per-attempt
timeout), `busy`, `error`, or `cancelled` when the send was dropped first.
Unlike `gateway.reserved_dns.request.duration_ms`, it excludes cache hits,
local answers and capacity rejections.

`gateway.reserved_dns.upstream_busy.lookup_in_flight` samples only `Busy` that
reaches Hickory, not sends on closed connections. A send that races the
connection's exit can still reach Hickory as `Busy`.
