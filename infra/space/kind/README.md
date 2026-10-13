# Local SPACE on KinD

A multi-node KinD cluster that runs SPACE in the production topology:
api-server on `KubernetesSubstrate` (each sandbox is its own K8s Pod whose
pause container is hijacked by `containerd-shim-space-v2` into a real
Firecracker microVM), the production worker service set on every sandbox
node, a dedicated builder role that runs actual ECW recipes, node registration
through node-ctl → internal-api, templates distributed through the production
publish/pull path, and real S3 snapshot transit via an in-cluster versitygw object store. The
remaining deviations from production are listed in "How it differs from
production" below.

## Default workflow: Tilt

Tilt does not create the cluster. The `Tiltfile` attaches to the existing
`kind-space-local` context (`allow_k8s_contexts`) and refuses any other, so a
fresh host needs the prerequisites and the cluster first. The first-time path
is the scripted bring-up in the root README, which creates the cluster, stages
the Firecracker vendor tree, builds every image, and deploys the stack:

```bash
bash infra/space/kind/scripts/install-prereqs.sh    # kind, kubectl, helm, grpcurl, go
bash infra/space/kind/scripts/install-fc-vendor.sh  # Firecracker, jailer, guest kernel
bash infra/space/kind/scripts/bootstrap.sh          # creates space-local and deploys
tilt up                                             # from the repository root
```

Do not create the cluster by hand: `kind-cluster.yaml` bind-mounts per-node
containerd host directories and the staged vendor tree, which must exist
before `kind create`, and Tilt's bootstrap resources run only after the
cluster is up. `bootstrap.sh` performs that preparation first.

Once the cluster exists, `tilt up` is the everyday loop: it builds any image
that is missing, then watches the tree, rebuilding affected images and rolling
the corresponding workloads without recreating the cluster or its persistent
data. Every Rust binary comes from one artifact image,
`space-rust-build-kind:local` (the union of the production artifact sets),
owned by `scripts/build-rust-artifacts.sh`: on Linux/x86_64 it runs **one
incremental cargo build inside the production `chef-base` builder image** —
bookworm libc and the `rust-toolchain.toml` pin, exactly what CI compiles with —
against a persistent target dir under `~/.space-devbox/kind/`, then assembles
the image from the staged `/artifacts` layout (no Rust compile as a BuildKit
layer, an incremental compile per edit, no build-cache growth). The assembly
refuses any binary whose required GLIBC symbol version exceeds the runtime
images' libc, which is why the compile does not run with the devbox's own
toolchain. Elsewhere it runs `docker/Dockerfile.rust-build-kind` (one cargo-chef
cook and build, as BuildKit stages). `SPACE_KIND_RUST_ARTIFACTS=incremental|docker`
forces either; `SPACE_KIND_RUST_PROFILE=dev-release` uses the incremental-codegen
profile with debug info for the fastest edit loop. Either way a Rust change
compiles once and the affected runtime images then re-copy their binaries from
it. Both paths take their cargo features from the build stage of
`docker/Dockerfile.rust-build-kind`, which also enables
`pplx-space-common-core/test-hooks`: the test-only environment overrides the
Kind charts set (for example the sweeper timings and the `*_TOKEN_PATH` files)
are compiled in only for these images, and release images never enable the
feature. Follow build progress at `http://localhost:10350`.
`tilt down && kind delete cluster --name space-local` tears it down.

If startup reports that the Tilt server on `:10350` could not be verified as
`kind-space-local`, inspect `tilt get cluster default --port 10350 -o yaml`.
A watcher for another cluster must be stopped in its owning session before
starting this stack. An initializing watcher can be retried
once its cluster context is available. Bootstrap leaves a responsive unverified
watcher running rather than terminating it.

A watcher that *does* verify as `kind-space-local` can still be bound to a
cluster that was deleted and recreated underneath it: the context name is the
same, only the apiserver's host port changed, and Tilt loaded the old one at
startup. The smoke bring-up (`scripts/tests/lib/bootstrap-kind-stack.sh`)
restarts such a watcher instead of waiting on it, judging it stale when Tilt
reports a cluster connection error or
when its process predates the current `space-local-control-plane` container.
It likewise restarts a watcher that serves this cluster from a different
checkout, or from one deleted since it started, so the code built and deployed
is always the checkout the bring-up ran from; the cluster and its pods stay up.
The same bring-up ends with a Tilt build error, printing that resource's log,
rather than consuming its timeout — Tilt does not retry a failed build until a
watched file changes or you run `tilt trigger <resource>` — and it returns only
once Tilt has nothing pending and every workload in the namespace has finished
rolling. `teardown.sh` also stops a watcher serving this context that its
pidfile does not name, so a `kind-down` cannot leave the stale watcher behind.

## Template publication authentication

ClusterApi validates projected ServiceAccount tokens with the `space-cluster-api`
audience; static fallback is disabled on that listener. Direct registration by
worker identities is denied. The seed-publish Job has its own operator-only
ServiceAccount. Inventory reports and sandbox creation cannot publish templates,
releases, or tags; this is unconditional and has no compatibility switch.

Each node service runs under its own DaemonSet's ServiceAccount and projects its
own ClusterApi token, and only build-manager's builder account
(`space-builder-build-manager`) carries operator and tenant publisher authority
(`clusterApiAuth` in the internal-api chart). The Kind node-api and build-manager
listeners still accept the shared static token as trusted build authority: a
worker can submit an operator build through that route even though ClusterApi
rejects direct worker publication.

The direct-registration tests prove ClusterApi's publisher checks, not end-to-end
worker isolation. That also requires Kubernetes authentication on the builder
listeners, with each system-build caller explicitly reviewed. Kind's static build
credentials are a local development limitation and must not be used as a secure
deployment model.

## CI smoke partitions

Kind CI builds one exact-commit Rust artifact image and hands it to four
isolated runners. Each runner boots its own Kind stack, proves the baseline,
and runs its assigned typed scenarios serially. Cluster-wide fault injection
must never run concurrently on a shared stack. Every shard must pass.

`scripts/tests/smoke-kind-typed.sh all INDEX/COUNT` partitions the eligible ECW
and native inventories independently, after the normal backend, template, and
quarantine filters. `space_smoke/sharding.py` assigns longest scenarios first
to the least-loaded partition, with stable ID/index tie-breaking, then executes
each partition in ID order. Unsharded runs keep their existing order.

The duration hints are rounded scenario wall times (including fixture setup and
cleanup) from [a successful Kind run](https://github.com/perplexityai/SPACE/actions/runs/36102493553).
To refresh them, measure matching `=== RUN` / `=== END` timestamps separately
for each API surface across all shards of a successful run. They are scheduling
hints only: new or renamed scenarios receive a 30-second estimate and still run;
obsolete hints do not add scenarios. No assertion, timeout, or scenario is removed.
Four shards shorten the serial critical path but duplicate cluster startup and
consume four concurrent runners; queue time remains a separate constraint.

## Bring it up (scripted)

### CI layer cache

Kind CI builds the Rust artifact image once and transfers that exact image to
all four smoke shards. This run-local handoff is independent of cross-run caching;
each shard still verifies its source SHA and image ID before use.

New cross-run BuildKit layers go to `ghcr.io/perplexityai/space-kind-build-cache`,
not GitHub Actions' shared cache quota. The three stable tags are
`kind-rust-chef-base`, `kind-grpc-health-probe`, and `kind-rust-build`.
PR builds import anonymously, without uploading layers or receiving package-write
permission. The main-push caller alone grants `packages: write`, logs in with
its job-scoped `GITHUB_TOKEN`, and exports `mode=max` layers. No personal token or
new secret is used. Smoke jobs retain only `contents: read`.

During migration, CI also imports the existing Actions cache scopes, using the
job's exposed Actions runtime. Neither PRs nor main export to Actions. This
read-only bridge preserves available cache hits while GHCR is unseeded or still
private, without adding to the shared quota. BuildKit can import both sources;
an Actions cache lookup has a 60-second timeout per target. An unavailable or
evicted cache still means a cold build, not a green build without compilation.

Main cache exports are best-effort. The main workflow also keeps the ten newest
untagged versions of this dedicated cache package and preserves all tagged
heads; cleanup failures are visible warnings and must be investigated to keep
storage bounded. No release package or existing Actions cache is deleted.
If neither source has the layers, builds run cold. Cache failure must never hide
a Rust build or smoke failure. Local builds do not use a registry unless explicitly
configured with `SPACE_KIND_BUILD_CACHE_REPOSITORY`.

Rollout:

1. Merge and let a trusted main run seed the package. The organization must allow
   this repository's Actions token to create it, with repository-linked access
   for publication and retention. Check export/cleanup logs for permission errors.
2. A package administrator must make **this OSS-only cache package public** so
   fork PRs can import anonymously. Packages start private; do not expose private
   repository caches. Verify the three tags can be inspected without credentials.
3. Compare cold and warm runs: prove main exports, PRs import without exports,
   and unchanged dependency layers are cache hits. Confirm retained version counts
   and all four smoke results. Do not claim a cache speedup from a cold run.
4. Only after that evidence, remove the Actions import bridge and separately
   retire obsolete Kind Actions cache entries if needed. Other compiler/image
   caches are outside this change; no shared cache is deleted by this rollout.

This removes cache export from the PR build path, not from main, and does not
change smoke scenario coverage, timeout budgets, or runner capacity.

### Scripted steps

The same first-time steps with the validation suites, for CI and scripted fixture setup
without Tilt:

```bash
# 1. user-local prereqs (kind, kubectl, helm, grpcurl, go 1.25)
bash infra/space/kind/scripts/install-prereqs.sh

# 2. Firecracker binary + jailer + vmlinux into the vendor tree
bash infra/space/kind/scripts/install-fc-vendor.sh

# 3. the actual cluster
bash infra/space/kind/scripts/bootstrap.sh

# 4. validate
SKIP_BOOTSTRAP=1 bash scripts/tests/smoke-kind-e2e.sh

# 5. build an ECW template, boot it, and run verification commands
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw \
  uv run --frozen --python 3.11 -m space_smoke run local.ecw_template_build_from_oci_boots
```

No AWS credentials are needed anywhere in the default bring-up: the seed
template is built locally by the production seed builder and published
in-cluster (see "Seed template" below).

`bootstrap.sh` auto-invokes `install-fc-vendor.sh` if the vendor tree is
missing (so step 2 is optional in practice), and is otherwise idempotent: it
creates the kind cluster (if missing) with the SPACE Kind node image on the
sandbox workers and builders (see [The node layer](#the-node-layer)), labels
nodes, advertises the `space.pplx.net/network-slots` extended resource,
installs the local observability backends in the default `otel` mode, installs
the containerd shim on the sandbox workers, applies the `space` and
`sandboxes` Namespaces + `RuntimeClass` and the sandbox namespace's
NetworkPolicies,
builds every image and `kind load`s it (first run is the long step; the
incremental build reuses its target dir and the Docker build is
cargo-chef-cached after that), applies every chart with
`helm template | kubectl apply`, and waits for every node-service DaemonSet
to roll out and every SPACE node to be published and schedulable.

### Metrics (default local backend)

The default Kind bring-up installs Collector → Prometheus to receive SPACE
OTLP metrics; see [the metrics runbook](metrics/README.md) for queries and
independent release management. Synthetic validation workloads are separate
and opt-in. Grafana is also installed by default but remains an independent
release, not a requirement for ingestion or query validation. These Kind
defaults do not change shared deployments or reconfigure an existing
Datadog Agent.

### Logs (default local backend)

The [Alloy → Loki setup](logs/README.md), installed by default, collects SPACE
container and Kind node journal logs without changing application images,
Datadog collection, or metrics/traces settings. It includes local-only UI access,
an existing-process-fixture validation workflow, and scoped cleanup commands.
The ingestion and visualization releases can still be managed independently.

### Querying telemetry

In `otel` mode both bring-up paths forward each backend to localhost, as they
do the UI and the object store (bootstrap unless `--no-port-forward`), and
restart a forward whenever its pod restarts:

| Backend | URL |
| --- | --- |
| Prometheus | <http://localhost:19090> |
| Tempo | <http://localhost:13200> |
| Loki | <http://localhost:13101> |
| Grafana | <http://localhost:13000> |

Each backend's own `local.sh ui` helper uses the same port, for a backend
installed on its own. The typed `local.telemetry_reaches_local_backends`
scenario reads these forwards, so it needs them (`bootstrap.sh
--no-port-forward` turns them off). The suite skips it on a bench running
another mode, but fails if that is not the mode the runner's
`SPACE_KIND_TELEMETRY_MODE` names. Every signal can be read with `curl`:

```bash
now=$(date +%s)

# PromQL. OTLP resource attributes are labels: service_name, k8s_pod_name, ...
curl -sG localhost:19090/api/v1/query \
  --data-urlencode 'query=count by (service_name) ({__name__=~"space_.+"})'
curl -sG localhost:19090/api/v1/query \
  --data-urlencode 'query=histogram_quantile(0.9, sum by (op) (rate(space_sandbox_api_latency_milliseconds[15m])))'

# TraceQL search, then the whole trace by an ID it returned.
curl -sG localhost:13200/api/search \
  --data-urlencode 'q={resource.service.name="space-api" && status=error}' \
  -d start=$((now - 3600)) -d end=$now -d limit=20
curl -s localhost:13200/api/v2/traces/TRACE_ID_FROM_THE_SEARCH

# LogQL. Lines are JSON; `| json` exposes their fields, such as sandbox_id.
curl -sG localhost:13101/loki/api/v1/query_range \
  --data-urlencode 'query={namespace="space"} | json | sandbox_id="SANDBOX_ID"' \
  -d start=$((now - 3600))000000000 -d end=${now}000000000 -d limit=100
```

Tempo search never returns the newest 30 seconds, whatever `end` says
(`query_end_cutoff`), and rejects a window that the cutoff leaves empty, so
poll for a trace you just caused; fetching a trace by ID has no such delay.
For tag values use `/api/v2/search/tag/resource.service.name/values`: the v1
route answers scoped names with an empty list. Span IDs in fetched traces are
base64. A Rust service's log line written inside a traced span carries that
span's `trace_id` and `span_id` in lowercase hex, so
`{namespace="space"} | json | trace_id="TRACE_ID"` finds a trace's lines and the
`trace_id` opens it in Tempo; lines also carry `sandbox_id` where the service
knows it. Grafana shows the same three datasources and the packaged
dashboards, and its Loki datasource links each `trace_id` to Tempo.

#### Following one sandbox

Every service names a sandbox by its bare UUID in the `sandbox_id` span
attribute and log field; the native API's `sbx_` id is the same UUID with a
prefix (see [Sandbox identity](../../../docs/observability.md#sandbox-identity)).
From a create response, or the `sandbox_id` a failed smoke scenario prints:

```bash
sbx=sbx_01a1324d-32be-7235-a41c-1586423506c8   # as the API returned it
id=${sbx#sbx_}
now=$(date +%s)

# 1. Logs: every SPACE pod line for the sandbox, oldest first. A line written
#    inside a sandbox's span carries its sandbox_id even when the message
#    does not name it.
curl -sG localhost:13101/loki/api/v1/query_range \
  --data-urlencode "query={namespace=\"space\"} | json | sandbox_id=\"$id\"" \
  -d start=$((now - 3600))000000000 -d end=${now}000000000 \
  -d direction=forward -d limit=1000 \
  | jq -r '.data.result[] | .stream.service as $s | .values[] | "\($s) \(.[1])"'

# 2. Traces: every trace with a span for the sandbox, in any service. A line
#    from step 1 also names its trace_id, which you can fetch directly.
curl -sG localhost:13200/api/search \
  --data-urlencode "q={span.sandbox_id=\"$id\"}" \
  -d start=$((now - 3600)) -d end=$now -d limit=50 \
  | jq -r '.traces[] | [.traceID, .rootServiceName, .rootTraceName] | @tsv'
curl -s localhost:13200/api/v2/traces/TRACE_ID_FROM_THE_SEARCH

# 3. Metrics carry no sandbox_id (it would be unbounded cardinality): read the
#    node and services the trace names over its time range, e.g. the shim
#    operations on that node (`k8s.node.name` on its spans). Each sandbox has
#    its own shim process, so read its series' last value, not a rate.
curl -sG localhost:19090/api/v1/query \
  --data-urlencode 'query=histogram_count(sum by (op, result) (last_over_time(space_shim_operation_latency_milliseconds{k8s_node_name="NODE"}[15m])))'
```

Each sandbox's net-gateway runs on the node and logs to its journal without a
systemd unit, which the local log collection does not keep; read those lines
with `docker exec NODE journalctl | grep "$id"`. Its spans reach Tempo.

For a native API request, the `x-request-id` response header (`req_...`) is
the `request_id` attribute of its `http.server` span and a field of the log
lines it wrote, and the `traceparent` response header names that span:
its second field is the trace ID to fetch.

### Datadog (optional)

The Kind-only `datadog-agent-local` chart supports two explicit backends:

| `SPACE_KIND_DATADOG_MODE` | Backend | Credential |
| -- | -- | -- |
| `fakeintake` | In-cluster Datadog Fake Intake | Chart-rendered dummy key |
| `saas` | Approved Datadog site | Separate, existing dev-only Kubernetes Secret |
| `off` | Skip Agent reconciliation; leave any existing Agent running | None |

### OTLP telemetry

This is the default. `otel` mode sends metrics and traces to the local
Collectors and needs neither the Datadog Agent nor Datadog credentials, so an
ordinary bootstrap gives you a cluster whose telemetry you can query:

```bash
bash infra/space/kind/scripts/bootstrap.sh
```

Bootstrap and full-stack Tilt install the metrics, traces, logs and visualization
backends, but nothing waits on them: the shim and the Rust services start
without a receiver and retry their exports until one answers, so the backends
can be removed and reinstalled under a running stack. No manual Collector
installation is needed on these default paths. Each helper is
idempotent, so re-running bootstrap does not churn them.

Before installing, `install-observability.sh` loads each backend image onto
every node at the linux/amd64 digest `ci/mirrored-images.txt` lists for the
charts' pin, under the tag the Kind profiles name, so the pods start without a
registry pull. GitHub CI takes these images from the GHCR mirror; elsewhere
they come from Docker Hub. A load that fails leaves the pull to the kubelet.

The `off` and `datadog` modes install no local backends:

```bash
# No metrics or traces at all, and no local backends.
SPACE_KIND_TELEMETRY_MODE=off bash infra/space/kind/scripts/bootstrap.sh

# Native Datadog instead, with local capture rather than the SaaS backend.
SPACE_KIND_TELEMETRY_MODE=datadog SPACE_KIND_DATADOG_MODE=fakeintake \
  bash infra/space/kind/scripts/bootstrap.sh
```

`off` sets `SPACE_METRICS_SINK=off` and `SPACE_TRACE_EXPORTER=off` for every
Rust service and the shim, with no OTLP endpoint or identity overrides, for a
leaner bench. The Kind CI shards pin `otel`, so they run what a devbox runs;
the typed `local.telemetry_reaches_local_backends` scenario, in their normal
inventory, polls Tempo, Prometheus and Loki for its own sandbox's trace, shim
metrics and log lines.

To export to a receiver you run yourself, stay in `otel` mode, set
`SPACE_KIND_SHIM_OTLP_ENDPOINT` for the shim and runtime-supervisor, and
override the workload endpoints below. The local backends are still installed
alongside it.

Rust workloads send metrics to
`http://collector.space-metrics.svc.cluster.local:4317` and traces to
`http://collector.space-traces.svc.cluster.local:4317` over plaintext
OTLP/gRPC. sandbox-manager is the exception: it forwards its endpoints to the
net-gateway and spaced-host processes it launches on the node, outside cluster
DNS, so it uses the shim's node-reachable addresses instead. To use other receivers, override `telemetry.otlpMetricsEndpoint`
and `telemetry.otlpTracesEndpoint` in the workload Helm values. Endpoint and
protocol environment variables are per-signal and emitted only for OTLP
exporters. Logs stay on stdout for Alloy to collect; no log endpoint is needed.

`otel` is the Kind default. Set `SPACE_KIND_TELEMETRY_MODE=datadog` to return
to native Datadog metrics and traces; pair it with
`SPACE_KIND_DATADOG_MODE=fakeintake` for local capture.

The two selectors are independent. `SPACE_KIND_DATADOG_MODE` chooses where the
Agent sends what it collects, and `SPACE_KIND_TELEMETRY_MODE=otel` bypasses the
Agent entirely for metrics and traces — it needs the Collectors installed,
not an Agent. Pair them only when you want a side-by-side comparison.

There is no dual-publish or non-counter mode. The `otel` mode uses `always_on`
diagnostic trace sampling, not the production sampling policy, and exports the
Rust services' metrics cumulative rather than production's delta, so the
metrics Collector has no idle stream to forget; the shim's stay delta
([Collector requirements](../../../docs/observability.md#collector-requirements)).
Logs remain structured JSON on stdout in every mode; there is no OTLP log
exporter.

`SPACE_KIND_DATADOG_MODE` defaults to `off`, even when `DD_API_KEY` is set.
**`off` and stopping Tilt do not stop an existing Agent's export.** Use the
explicit rollback below. For native Datadog metrics/traces/logs capture with no
telemetry egress to Datadog SaaS:

```bash
SPACE_KIND_TELEMETRY_MODE=datadog SPACE_KIND_DATADOG_MODE=fakeintake \
  bash infra/space/kind/scripts/bootstrap.sh --skip-images

# Or start the Tilt-managed stack in the same mode:
SPACE_KIND_TELEMETRY_MODE=datadog SPACE_KIND_DATADOG_MODE=fakeintake tilt up
```

Both paths install Agent `7.81.1`, expose Fake Intake at
`http://localhost:8088`, and set the Agent and SPACE workloads to the same
local environment. Environment-only settings require a Tilt restart; the
optional `SPACE_KIND_OBSERVABILITY_CONFIG` JSON file is watched and authoritative
over the launch environment, including during rollback.

The default Kind telemetry mode sends Rust custom metrics and traces through
the upstream OpenTelemetry SDKs and local Collectors. On an existing Kind
cluster, full-stack Tilt installs the backends automatically:

```bash
tilt up
```

Add `SPACE_KIND_DATADOG_MODE=fakeintake` alongside it to keep an Agent running
for a side-by-side capture; `otel` does not require one.

`datadog` remains an explicit rollback option. The default `otel` mode selects
the exclusive `SPACE_METRICS_SINK=otlp` and `SPACE_TRACE_EXPORTER=otlp` paths with an
`always_on` diagnostic trace sampler, and `off` turns both signals off. In
`otel` mode the shim moves to OTLP as well.
There is no dual-publish or non-counter mode. Counters use the same metric
names: native intake reports RATE and OTLP reports COUNT. Compare event totals
rather than raw values, and keep a same-name type comparison off any backend
shared with other work — two types under one metric name is a conflict the
backend cannot resolve for you.
The metrics stack also enables OTLP in supported filesystem worker processes.
These selectors do not install the application stack: a main-only binary still
needs its owning exporter changes to support OTLP. Logs remain structured JSON
on stdout, collected by whatever is installed to read it — the Agent when one
is running, Alloy in this stack. There is no OTLP log exporter.

The Agent chart is supported only in `kube-system`; a different namespace is
rejected because its host-port deployment and intake URLs belong to this Kind
harness. A direct render must choose `values-fakeintake.yaml` or an existing
SaaS Secret explicitly. The ordinary chart defaults do not deploy a backend.

SPACE bootstrap completes application readiness before installing the optional
Datadog Agent; the default local OTLP backends are installed with the stack.
An Agent failure is reported as a failure without preventing the SPACE releases
from being installed. Comparison runs still require verified Agents and
received payloads.

Fake Intake is a single-replica, in-memory test backend. It captures the real
Agent payloads but does not implement Datadog dashboards or SaaS-side indexing.
Inspect received routes and reset the capture between comparison phases with:

```bash
curl -fsS http://localhost:8088/fakeintake/routestats | jq
curl -fsS http://localhost:8088/fakeintake/flushPayloads >/dev/null
```

For durable, phase-labelled evidence, drain and reset the receiver before one
bounded smoke window, then capture it afterward:

```bash
scripts/tests/capture-kind-fakeintake.sh prepare
# Run one smoke test or one repeated test group.
scripts/tests/capture-kind-fakeintake.sh capture after-otel happy-path
```

Captures are written below `target/kind-telemetry-comparison/` and include raw
metrics, sketches, logs, trace payloads, route counts, workload configuration,
and stable cluster identities. Existing capture directories are never replaced.
The helper records the prepared-window timestamp and the first overlapping
ten-second DogStatsD bucket. Normalized metrics include that bucket (which can
contain up to nine seconds of earlier activity); logs use the exact timestamp.
Raw payloads are retained for interpreting boundary and late-arrival effects.

When the workloads export traces through the native Datadog tracer, capture
then checks the SPACE traces it received. With the evidence already written,
it fails if there are none or if SPACE parented a span other than a request's
entry span on remote context, and it reports the chunks the tracer split.
`space-trace-chunks.json` holds the examples and counts; see
[Datadog trace chunks](../../../docs/observability.md#datadog-trace-chunks).

The three-state smoke specification, pinned inputs, measured results and
guest/worker coverage limits are recorded with the run's evidence rather than
in this repository.

### SaaS experiments

`saas` exports to a real Datadog organization, so it is deliberately harder to
reach than `fakeintake`:

1. Get the export approved by whoever owns the target organization's ingestion
   budget, and agree a window. There is no spending cap in this tooling.
2. Create the Secret out of band and name it; the renderer never accepts a key
   as an argument or writes one into values or captures. Fake Intake uses
   `datadog-fakeintake-key`; SaaS rejects that name and the legacy
   `datadog-secret`.
3. Set an isolated experiment identity (`SPACE_KIND_ENV`, `SPACE_KIND_OWNER`,
   `SPACE_KIND_PHASE`) so the payloads are attributable and separable from
   anything else in that organization.
4. Use `session` for a best-effort timed rollback after
   `SPACE_KIND_DATADOG_DURATION_MINUTES`. The timer runs in that foreground
   process and is not a hard spending cap. Direct `apply`, bootstrap and Tilt
   require the additional `SPACE_KIND_DATADOG_UNBOUNDED=yes` acknowledgement;
   they export until explicitly stopped and do not run that timer.
5. Stop explicitly with the rollback below when finished. Neither `off` nor
   stopping Tilt stops an Agent that is already exporting.

After a successful helper-driven switch to SaaS, the helper deletes only its
labelled Fake Intake Deployment, Service and dummy Secret. Rollback recreates
those resources; a failed switch leaves the receiver available.

Both Tilt and bootstrap use the same preflight and renderer. Neither creates a
real API-key Secret or copies a key into arguments, rendered values or captures.
Fake Intake uses `datadog-fakeintake-key`; SaaS rejects that and the legacy
`datadog-secret` name. Normal SaaS endpoint TLS verification remains enabled;
Kind's kubelet certificate exception is separate.

With the same watched experiment configuration exported in your shell:

```bash
python3 infra/space/kind/scripts/datadog_backend.py stop
python3 infra/space/kind/scripts/datadog_backend.py verify
```

`stop` persists `fakeintake` into the file, applies freshly rendered local-only
values, and waits for Agent rollout before inspecting the running pods. It
preserves the real Secret. A failed rollout is not a successful stop; retry and
verify. Previously ingested SaaS data remains subject to backend retention.

The same mode selects the Go containerd shim's trace exporter and its metrics
sink: `datadog` keeps DogStatsD, `otel` moves the shim to OTLP alongside the
workloads, and `off` disables both. In `datadog` and `off` modes the wrapper
leaves the SPACE and OTel identity settings unset, so the shim's DogStatsD tags
are exactly what they were before this mode existed; `off` sets no OTLP
endpoint either. Because containerd starts
the shim on the Kind node rather than in a Pod, `install-shim.sh` installs a
small production-shaped wrapper around the compiled binary instead of relying
on workload environment inheritance. Kind nodes cannot resolve pod DNS, but
kube-proxy routes a Service's ClusterIP, so in `otel` mode the wrapper sends
each signal to `http://<ClusterIP>:4317` of its Collector. The addresses are
fixed by `collector.clusterIP` in each chart's `profiles/kind.yaml`:
`10.96.0.20` for `space-metrics` and `10.96.0.21` for `space-traces`, in the
first 256 addresses of Kind's `10.96.0.0/16` Service range, which dynamic
allocation avoids. The shim can therefore be installed before the Collectors,
and reinstalling them does not leave the wrapper with a stale address. Set
`SPACE_KIND_SHIM_OTLP_ENDPOINT` to send both signals to another OTLP/gRPC
receiver reachable from the Kind node instead.

The host runtime-supervisor is the other SPACE process outside a pod. Its unit
reads `/space/config/space.env`, which node-manager's Kind-only `kind-node-env`
init container writes from the same selector: the pods' telemetry settings,
with each OTLP endpoint replaced by the fixed address above, or by
`SPACE_KIND_SHIM_OTLP_ENDPOINT` when that is set. Check it with
`docker exec <node> cat /space/config/space.env`.

A Collector installed before its address was pinned keeps the ClusterIP it was
allocated, which Kubernetes will not change in place: `local.sh up` fails with
`spec.clusterIPs[0]: ... may not change once set`. Only that Service is
immutable; the data lives in the release's PersistentVolumeClaim. Delete the
Service and run `up` again, which recreates it at the pinned address and keeps
the stored metrics and traces:

```bash
kubectl --context kind-space-local -n space-metrics delete svc collector
bash infra/space/kind/metrics/local.sh up
```

Do the same for `space-traces`, then re-run `install-shim.sh` (bootstrap and
Tilt both do) so the wrapper names the pinned addresses.

The shim takes `SPACE_METRICS_SINK` from the same telemetry mode as the Rust
workloads: `datadog` keeps DogStatsD, `otel` selects OTLP, and `off` selects
`off`. Left unset the shim would default to `auto`, meaning native Datadog, and
would keep emitting DogStatsD metrics through an otel comparison.
Datadog mode keeps the existing node-local shim settings.

Updating the shim binary or wrapper does not restart containerd unless its
configuration also changed. Existing sandbox shims keep their launch settings;
create fresh test-owned sandboxes when changing phases.

`install-prereqs.sh` falls back to user-local installs (`$HOME/.local/bin`,
`$HOME/.local/go`) when run without root, so it works on sudo-less hosts
for everything it downloads. Its apt step (the suites' prerequisites,
listed in `scripts/lib/space-smoke-prereqs.sh`) has no user-local equivalent:
when one of those is missing and the run can neither sudo nor act as root, the
script fails instead of continuing, because a box that skips them cannot run
the smoke suites. `install-fc-vendor.sh` creates its vendor root
unprivileged when it can, and escalates once in either of two cases — the
root already exists and belongs to root (the shape the devbox platform
pre-provisions), or something above it is unwritable — after which it keeps
writing without escalation. A
root-owned parent it had to *create* is handed over as well; one that already
existed is not, so a pre-existing root-owned `~/.space-devbox` still needs the
manual chown the smoke-test runbook describes.

### Shared experiment identity

Tilt, bootstrap, and the node-hosted shim read the same optional non-secret
JSON file named by `SPACE_KIND_OBSERVABILITY_CONFIG`. Tilt watches this file;
changing it reconciles workload identity and the shim wrapper. File values
override the corresponding environment variables. Never store API keys here.

Replace that file by rename (`mv` a fully written temporary over it) rather
than editing in place: a truncated or half-written file aborts bootstrap and
Tiltfile evaluation, and `bootstrap.sh` already writes its own state this way.

The identity options are `SPACE_KIND_ENV`, `SPACE_KIND_OWNER`,
`SPACE_KIND_VERSION` (the source revision), and `SPACE_KIND_PHASE`
(`before-main`, `after-datadog`, or `after-otel`). When
revision and phase are supplied, the application version is `<revision>-<phase>`
so even native trace/log clients can distinguish phases. Agent-side tags alone
do not override an application's explicit environment/version. Without
`SPACE_KIND_VERSION`, every Rust service and the shim report the checkout's
commit (`git rev-parse --short=12 HEAD`) as their version, read when Tilt or
bootstrap starts, and every image records that commit as its build SHA, which
the startup banner's `git_sha` and the node's host sidecars report.

These values align `DD_ENV`/`DD_VERSION`, neutral log identity, and OTel resource
identity without changing service names. `SPACE_KIND_CLUSTER` names the Kind
cluster and supplies `kind_cluster` / `k8s.cluster.name`. `SPACE_KIND_CLUSTER_ID`
is the separate SPACE/database identity, defaults to `kind-local`, and reaches
both service charts and the shim as `SPACE_CLUSTER_ID`. Keep that value stable
for an existing database; experiment environment/phase changes do not rename it.
Old binaries ignore unsupported neutral variables; native identity still works.
The Agent uses an experiment-specific backend cluster identity; correlate it
with workloads by the shared `kind_cluster` tag. Evidence capture lives in
`scripts/tests/observability/datadog_evidence.py`; chart resources and backend
reconciliation stay under `infra/space/kind`.

Existing long-lived shims retain their launch environment: create a new sandbox
after changing phases. The installer accepts `SPACE_KIND_SHIM_ENV` as a legacy
fallback when `SPACE_KIND_ENV` is absent.

### Host paths (and running as a user other than `dev`)

`kind create` reads plain YAML and expands nothing, so the cluster config is
rendered per host rather than checked in: `kind-cluster.yaml.tmpl` carries
`${SPACE_VENDOR_ROOT}` and `${SPACE_KIND_DATA_ROOT}` placeholders, and
`scripts/lib/kind-host-paths.sh` resolves them, writes the result to
`$SPACE_KIND_CONFIG`, creates the containerd-storage directories, and refuses
to hand `kind` a config whose vendor tree or `/dev/kvm` is missing — Docker
answers a missing bind-mount source by creating an empty directory, so an
unstaged vendor tree used to reach the workers as an empty `/space/vendor`
and fail on the first sandbox with a healthy-looking cluster.

| Knob | Default | What mounts it |
| -- | -- | -- |
| `SPACE_VENDOR_ROOT` | `~/.space-devbox/space/vendor` | `/space/vendor` on the sandbox and builder nodes |
| `SPACE_KIND_DATA_ROOT` | `~/.kind-data` | `/var/lib/containerd` on every node |
| `SPACE_KIND_CONFIG` | `~/.space-devbox/kind/kind-cluster.yaml` | the rendered config itself |

That path is where a completed render is *published*, not the file any given
run hands to `kind create`. Each bring-up renders its own copy first and
creates from that, because the published path is per-user rather than per-run:
two bring-ups on one box would otherwise be able to replace each other's
config in the window between rendering it and reading it back, and the cluster
that won would come up on the other run's host roots with every mount it
declares present. `bootstrap.sh` publishes its copy once it has verified, so
the documented path still names something real and current.

`scripts/lib/kind-host-paths.sh` resolves the vendor path for both staging and
cluster mounts. `SPACE_VENDOR_ROOT` defaults to
`$SPACE_DEVBOX_SPACE_TARGET/vendor` when that target is set, otherwise
`$SPACE_DEVBOX_PERSIST_ROOT/space/vendor`, falling back to
`$HOME/.space-devbox/space/vendor`. `SPACE_KIND_CONFIG` follows only
`SPACE_DEVBOX_PERSIST_ROOT` — it lives beside the other kind state, not under
the `space/` target — so `SPACE_DEVBOX_SPACE_TARGET` moves the vendor mount
without moving the rendered config. An explicit `SPACE_VENDOR_ROOT` outranks
all of them.

If you already have a `space-local` cluster that was created before these
paths were parameterised, it keeps the mounts it was built with — Docker fixes
bind mounts when it creates the node container, and `kind create` is the only
thing that reads the config. Both bring-up paths detect that and stop, naming
each drifted mount and its rendered value; `kind delete cluster --name
space-local` once is the remedy, and a delete leaves the containerd image
caches on disk so the rebuild does not re-pull.

The defaults follow `$HOME`, so none of the three needs setting on a devbox,
and none needs setting under another username either. Three other paths in
this directory are still `/home/dev` by default and are *not* covered by that:
`infra/space/kind/scripts/setup.sh`'s `INSTALL_DIR` and `SPACE_KIND_ENV_FILE`,
and `infra/space/kind/scripts/start-dind.sh`'s `DIND_DATA_ROOT`. Neither
script is `$HOME`-derived, and for the same reason: `setup.sh` is documented
to run under `sudo` (where `$HOME` may be root's) and it is what invokes
`start-dind.sh`, so deriving either from `$HOME` would put a dind data root
or a binary install under root's home. Set those three variables explicitly
if you run either script as another user.

Both bring-up paths — `infra/space/kind/scripts/bootstrap.sh` and the suites'
`scripts/tests/lib/bootstrap-kind-stack.sh` — render before creating. For a
hand-run `kind create`, use
`infra/space/kind/scripts/render-cluster-config.sh`, which prints the path of a
render private to that invocation — pass that to `kind create` rather than
`$SPACE_KIND_CONFIG`, so a second run cannot replace the file between the two
commands. Point `SPACE_VENDOR_ROOT` at a tree you staged elsewhere and
`install-fc-vendor.sh`, the existence check, and the mount all follow it
together.

Once the script returns, the API and data plane answer on the cluster's own
host port mappings (see [Load-balanced entry and replicas](#load-balanced-entry-and-replicas)),
and a tile of background `kubectl port-forward`s exposes the single-replica
rest:

| Service     | URL                       | Through |
|-------------|---------------------------|---------|
| API         | http://localhost:8080     | host port mapping → `space-api-entry` NodePort |
| Data-plane  | http://localhost:8090     | host port mapping → `space-data-plane-entry` NodePort |
| UI          | http://localhost:3000     | port-forward |
| Object store (S3) | http://localhost:8333 | port-forward |
| Fake Intake (when enabled) | http://localhost:8088 | port-forward |

Log in as `root@local` with the initial password (`password` by default). Set `SPACE_CONSOLE_PASSWORD` before running bootstrap or Tilt to choose a custom password. Neither path prints or retains it in a Kubernetes Secret. It is set only while the seeded account is locked; rerunning does not reset an existing password.

### Recovering from an unbundled migration version

On a long-lived task-owned bench, the `space-db-migrate` Job may fail, and
`space-api` never start, because the database contains an applied migration
version that the current binary does not bundle. This can happen when a branch applies a queued migration before its
timestamp changes on merge. No later build can satisfy the orphaned version.

After confirming `kind-space-local` is the intended disposable cluster, reset
the local control-plane database and republish `seed:latest`:

```bash
kubectl delete statefulset space-postgres -n space --context kind-space-local
kubectl delete pvc data-space-postgres-0 -n space --context kind-space-local
kubectl delete job space-seed-publish space-db-migrate -n space --context kind-space-local
tilt trigger space-postgres
tilt trigger space-db-migrate
tilt trigger space-api
tilt trigger space-seed-publish
```

Deleting the publish Job is required because the database reset removes the
template-release tags that resolve `seed:latest`.

### Diagnosing iteration disk pressure

Tilt repeatedly loads large images into each Kind node's containerd store. A
full node may report image extraction or build failures while the host root
filesystem still has space. Inspect and reclaim only unreferenced node images:

```bash
docker exec space-local-worker df -h /
for node in $(kind get nodes --name space-local); do
  docker exec "$node" crictl rmi --prune
done
```

The BuildKit cache is host-global and shared by other worktrees. Do not run
`docker builder prune` without explicit approval after stating that impact.
The Docker data root may live on a separate volume, so check available space
inside a Kind node before starting a large build. Cold Rust builds can consume
substantial space; the incremental artifact build keeps its `target/` under
`$HOME` instead.

## Seed template

The seed is built locally by the **production seed builder**
(`templates/firecracker/seed/Dockerfile` + `rootfs-builder.sh` — the same
code path that builds the seed production publishes to S3), so the guest has
the vsock-listening `spaced-svc` and boots under real Firecracker:

- `infra/space/kind/scripts/build-seed.sh` runs the builder (privileged
  docker run; downloads the Ubuntu minimal cloud squashfs on first run),
  injects the `spaced`/`spaced-svc` binaries from the worker Rust build
  image, bakes the per-host sandbox SSH public key, and authors the
  `manifest.json` next to the artifacts. Idempotent — it skips the build
  when the inputs (guest binaries, builder script, recipe, pubkey) are
  unchanged.
- the `space-seed-publish` Job then plays the role the production builder
  plays for distribution: it uploads `metadata.json` + `manifest.json` +
  `rootfs.ext4` to the object store under `templates/seed/<content_hash>/` and
  registers the template + release + `latest` tag + storage location
  through the real `ClusterApi` RPCs on `space-internal-api`.
- worker template-managers then resolve `seed:latest` via
  `ClusterApi.FindTemplateRelease` and pull the artifacts through
  volume-manager — the production pull path. Nothing is baked into the
  node image or side-staged onto workers.

Tear it down with `bash infra/space/kind/scripts/teardown.sh`.

## Topology

```
kind cluster: space-local (kind-cluster.yaml.tmpl — 6 nodes)
├── control-plane                       (k8s API server, scheduler, etcd)
├── worker  [pool=sandbox, space-enabled=true]
│   ├── space-*.service node layer      (btrfs /space/cache etc., baked into the node image)
│   ├── space-runtime-supervisor.service (systemd; installed by node-manager's init
│   │                                     container; runs every Firecracker VM and
│   │                                     each sandbox's space-netgtw-svc)
│   ├── containerd-shim-space-v2        (installed into containerd by install-shim.sh)
│   ├── one pod per node-service DaemonSet (release `space`):
│   │   ├── space-node-manager        → host supervisor (root, privileged)
│   │   ├── space-runtime-manager     (read-only list)  (space-rtmgr)
│   │   ├── space-volume-manager      → object store    (space-volmgr)
│   │   ├── space-credential-manager                    (space-sbxmgr)
│   │   ├── space-template-manager                      (space-tmplmgr)
│   │   ├── space-sandbox-manager                       (space-sbxmgr)
│   │   ├── space-node-ctl            → internal-api    (space-nodectl)
│   │   └── space-node-api            (hostNetwork, host_ip:9800, component=node-api) (space-nodeapi)
│   ├── space-node-debug              (Kind-only operator shell, root)
│   └── per-sandbox pods in namespace `sandboxes` (pause:3.10 hijacked into Firecracker microVMs)
├── worker2 [pool=sandbox, space-enabled=true]
│   └── identical to worker
├── worker3 [pool=infra,    space.pplx.net/infra=true]
│   ├── space-postgres        (StatefulSet)
│   ├── space-object-store    (versitygw S3 gateway + bucket-init Job)
│   ├── space-api             (Deployment, 2–3 replicas by HPA; SPACE_K8S_CONTROL_PLANE=true)
│   ├── space-admin-api       (Deployment; the same binary on the dedicated admin surface)
│   ├── space-internal-api    (Deployment; ClusterApi — node registration + template releases)
│   ├── space-credmgr-authority (Deployment, 2 replicas; credential-manager authority — credential tables + platform KEK)
│   ├── space-kind-key-broker (Deployment; Kind-only intermediate-key broker fixture behind the platform KEK)
│   ├── space-data-plane      (Deployment, 2–3 replicas by HPA)
│   ├── space-cluster-controller (Deployment, 2 replicas, one leader; materializes per-sandbox pods)
│   ├── metrics-server        (Deployment in kube-system; resource metrics for the HPAs)
│   ├── space-seed-publish    (Job; publishes the locally built seed)
│   └── space-ui              (Deployment)
├── worker4 [pool=builder]
│   ├── the same node layer and space-runtime-supervisor.service
│   ├── the same node-service DaemonSets (release `space-builder`, node.role=builder)
│   │   plus space-builder-build-manager (recipe VM + publish → templates bucket; root)
│   ├── space-node-debug
│   └── Service space-builder-node-api:9800 → the builders' node-api pods
└── worker5 [pool=builder]
    └── identical to worker4
```

Sandbox-scoped traffic (`CreateSandbox`, `Exec`, `SpacedWrite`, …) flows
api-server → per-sandbox pod IP at `:9800`. Node-scoped traffic (`NodeInfo`,
`ListNodeTemplates`, network ops) flows api-server → each node-api pod's
`host_ip:9800`.

## Sandbox namespace and network policy

Sandbox pods run in their own namespace, `sandboxes`, and everything else in
`space`, as in a deployment that confines sandboxes with namespace-scoped
policy. `k8s.sandboxNamespace` in the api, cluster-controller and
internal-api charts names it (`SPACE_SANDBOX_NAMESPACE`), their RBAC for sandbox
pods is granted there, and `SPACE_NODE_API_NAMESPACE` keeps the node-api lookup
in `space`: unset, the substrate would look for node-api pods in the sandbox
namespace. `manifests/namespace.yaml` creates both namespaces; warm-pool
configs are filed under the API's sandbox namespace whatever a request names.

Kind v0.30.0's kindnet enforces NetworkPolicy, and
[`charts/space-sandbox-network-policy`](charts/space-sandbox-network-policy)
confines the namespace with two of them, each rule commented with the code
that needs it:

- `space-sandbox-default-deny` isolates every pod in the namespace, ingress and
  egress.
- `space-sandbox-allow` gives sandbox pods (`app.kubernetes.io/name=space-sandbox`)
  what they carry: ingress on TCP 9800 from the two components that dial a
  sandbox pod IP, `space-api` and `space-cluster-controller`; egress to the IPv4
  internet outside `10.0.0.0/8`, `100.64.0.0/10`, `172.16.0.0/12`,
  `192.168.0.0/16` and `169.254.0.0/16`; and DNS to the cluster DNS pods only.
  There is no IPv6 egress.

The guest needs nothing more. Its traffic leaves the pod masqueraded to the
pod IP, its DNS goes to public resolvers over that internet rule (the gateway
answers the guest's reserved resolvers over DNS-over-TLS), and the shim's
redirect of `<pod IP>:9800` to the node-local node-api is pod-to-own-node
traffic, which NetworkPolicy always allows. internal-api and the data plane
dial node-api at node addresses, never a sandbox pod. The policy sits beneath
the per-sandbox net-gateway's tenant rules and holds for anything that reaches
the pod's network namespace outside the VM.

Because the allow policy selects sandbox pods for both directions, it isolates
them on its own; the default-deny is what isolates any other pod placed in the
namespace. The typed `local.sandbox_network_isolation_holds` scenario checks
both against a live cluster from the `space-node-debug` shell, which enters a
pod's network namespace by its netns handle under `/var/run/netns`.
## Load-balanced entry and replicas

The control plane runs as a deployment's does: several replicas behind
load-balanced entry, scaled on load.

- **Entry.** `localhost:8080` and `:8090` are `extraPortMappings` of the infra
  node (`kind-cluster.yaml.tmpl`, listening on 127.0.0.1) onto the
  `space-api-entry` and `space-data-plane-entry` NodePort Services (30080,
  30090). kube-proxy picks a ready replica for each new connection, so clients
  reach every replica, where a `kubectl port-forward` pins every request to one
  pod; requests on one kept-alive connection stay on its replica. The entry
  Services carry the public HTTP port only; in-cluster callers keep the
  `space-api` and `space-data-plane` ClusterIP Services, and the API's ingress
  activation port stays off the nodes. No restart loop keeps these forwards
  alive any more; the UI, object store and Fake Intake are still forwarded.
- **Autoscaling.** metrics-server (`charts/space-metrics-server`, the upstream
  release manifest with `--kubelet-insecure-tls` for Kind's self-signed
  kubelets, in kube-system) serves the resource metrics API. space-api and
  space-data-plane each have a HorizontalPodAutoscaler between 2 and 3
  replicas, on CPU (80% of the request) and memory (384 MiB per replica). Two
  so every path runs against more than one replica; three so the worst case of
  both tiers at their 1 CPU / 512 MiB limits stays within a 16-core runner's
  infra node. Their Deployments roll one replica at a time rather than
  `Recreate`. The UI chart pins one replica, so it has no autoscaler.
- **Migrations** run once per rollout, as the `space-db-migrate` Job, before
  any API replica starts, the way a deployment's provisioning Job runs them
  ahead of its application releases; the replicas no longer each run them.
  The migrator asks for this: it serializes on a session advisory lock, but a
  competing runner can turn a retried lock race into a failed run
  (`crates/libs/db/migrator/src/migration_retry.rs`). Tilt makes `space-api`
  wait for the Job and bootstrap waits for it before applying the API.
- **cluster-controller** runs two replicas with leader election on: the Lease
  holder runs every reconciler and the other stands by, Ready, to take over.
  `smoke-kind-leader-election-brownout.sh` exercises the handoff.
- **Admin API.** `space-admin-api` runs the same binary with
  `--surface admin`, the dedicated admin surface, in its own Deployment and
  ClusterIP Service. Clients and the harness keep using the shared surface's
  `/admin/api/v1` on `localhost:8080`; reach the dedicated one with
  `kubectl --context kind-space-local -n space port-forward svc/space-admin-api 8081:8080`.

Harness operations that need a fixed replica count take it from the
autoscaler rather than fight it: the concurrent-restore scenario pins the API
autoscaler's bounds to its contention count and restores them, and the native
recovery faults pin every target's autoscaler at its current count until the
Deployments are restored.

A cluster created before the port mappings cannot gain them, since Docker fixes
a node container's ports when it creates it. `check-node-layout.sh` refuses it
with the commands to recreate it (see
[Upgrading from the node-stack layout](#upgrading-from-the-node-stack-layout)).

## How the node registers itself

Every node-service chart passes `SPACE_NODE_ID` as the raw kube node name
(fieldRef `spec.nodeName`) plus its worker or builder `SPACE_NODE_ROLE`;
the services that call ClusterApi also get
`SPACE_INTERNAL_API_ENDPOINT=http://space-internal-api:9800` and their own
projected token. node-ctl's
outbound reconcilers call `ClusterApi.PublishNodeState`, and the internal
API derives the node UUID from `(node name, role)` via `space_k8s_node_id`
and upserts `nodes` / `node_k8s` / `node_status_snapshots` /
`node_template_inventory`. The cluster-controller's `kube_node_sync`
reconciler covers kube-Node inventory. **Nothing on the node touches
Postgres**: credential state belongs to the control-plane credential-manager
authority (below). Node status uses the normal 30-second freshness window. Both bring-up paths end by waiting until
the admin node list (`GET /admin/api/v1/nodes`) reports every sandbox and
builder node reachable, with scheduling status `ready`, and observed within the
five node-ctl ticks the API trusts a census for
(`scripts/wait-node-publication.sh`): ready pods alone are not a registered
node. The wait does not require `stale: false`: node-ctl publishes every 60
seconds, so an idle healthy node reads stale about half the time.

## Node services

Production runs one DaemonSet per node service on nodes whose image provides
the node layer. So does this harness: each service has a chart under
`charts/space-<service>-local`, built on the `space-node-service` library
chart, rendered once per role — release `space` on the sandbox workers and
release `space-builder` (`--set node.role=builder` plus the chart's
`values-builder.yaml`) on the builders, which also run build-manager. The list
lives in `lib/node-services.sh`, which `bootstrap.sh`, Tilt and the smoke
bring-up read. Every pod runs the production worker image
(`docker/Dockerfile.worker`) unchanged, with its service's production launch
path, under the identity `infra/space/identity/node-identities.sh` assigns (the
library reads that file through each chart's `identity/` link). Pods carry
`app.kubernetes.io/name=space-<role>`, `app.kubernetes.io/component=<service>`
(the names the cluster-controller's node-service readiness and the API's
reflector use) and `app.kubernetes.io/part-of=space-node`.

| Service | Identity | Pod shape | Why (public code) |
|---|---|---|---|
| node-manager | root | privileged, hostNetwork, hostPID; `/space/cache` and `/var/run/netns` Bidirectional; `/sys/fs/cgroup`, `/space/bin`, `/space/etc`, `/space/vendor`, `/dev/kvm`; projected token for the credential-manager authority | mounts jail rootfs and late snapshot-memory binds that must reach the host jailer, netns binds the host jailer and sidecar launcher open, loop and device-mapper devices, cgroup limits, root-namespace veth/nft/ip_forward, pidfds on host processes (`crates/libs/node/mgr/server`); init container `node-manager-init.sh` writes the node's systemd unit via `nsenter --target 1` |
| runtime-supervisor | root | host systemd unit, no pod; environment from `/space/config/space.env`, which node-manager's Kind-only `kind-node-env` init container writes | `scripts/k8s/node-manager-init.sh`, `node-manager-runtime-supervisor.sh` |
| runtime-manager | space-rtmgr + space-rtmgrclients | non-root, no capabilities, read-only rootfs; `/space/vendor`, `/space/bin`, `/space/cache` read-only | lists runtimes and kernels |
| volume-manager | space-volmgr + SPACE_VOLUME_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs; `/space/cache` HostToContainer | file transfers only (`crates/libs/vol/mgr/server`); the separate identity `docs/installation.md` describes: node-manager's btrfs send-lease RPCs authorize it and credential-manager serves it on the volume socket |
| credential-manager | space-sbxmgr + SPACE_CREDENTIAL_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs; `/space/run` only; projected token for the authority | the node credential agent (`--role=agent`): sockets only, forwarding to `space-credmgr-authority`; no database, KEK or AWS credential; its peer-process check (`SPACE_UDS_PEER_PROCESS_CHECK`) is off, so no host PID namespace |
| template-manager | space-tmplmgr + SPACE_TEMPLATE_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs, hostNetwork; `/space/cache` HostToContainer | btrfs work delegated to node-manager (`btrfs_via_nodemgr.rs`); host network because its template-release reads are scoped by source address (below) |
| sandbox-manager | space-sbxmgr + SPACE_SANDBOX_MANAGER_GROUPS | non-root, no capabilities, read-only rootfs, hostPID, hostNetwork; `/space/cache` HostToContainer, `/space/etc` read-only | gateway callers are authorized by their host pid, and Firecracker pids go to the node's shim (`service/gateway_identity.rs`, `network_runtime.rs`); host network for its node-bound ClusterApi RPCs |
| node-ctl | space-nodectl + SPACE_NODECTL_GROUPS | non-root, no capabilities, read-only rootfs, hostNetwork; its own ServiceAccount with Node RBAC, fenced by `node-fence.yaml` | the Kubernetes node-local controller; host network for its node-bound ClusterApi RPCs |
| node-api | space-nodeapi + worker/builder groups | non-root, no capabilities, read-only rootfs, hostNetwork, port `grpc` 9800 | the API dials `host_ip:9800` and reads the `grpc` port; on builders it reaches build-manager through the node-local `space-builder-build-manager` Service and sleeps 5 s in preStop (below) |
| build-manager | root | privileged, hostPID, pod network behind the node-local `space-builder-build-manager` Service (`internalTrafficPolicy: Local`); `/var/run/netns` HostToContainer, `/space/cache` without propagation, the node's containerd | loop and bind mounts for OCI assembly, `nsenter --net` into build VMs through netns handles node-manager mounts after it starts, `ctr` against the node's containerd (`crates/libs/build/mgr/src`); host PID to watch the build VM's host Firecracker pid exit (`vm.rs`); no host network: SSH into build VMs runs inside `nsenter --net` |
| space-node-debug (Kind only) | root | privileged; `/space/run`, `/space/cache` HostToContainer, `/space/bin`, `/space/vendor`, `/run/containerd`, `/var/run/netns` HostToContainer read-only | the operator shell; see below |

internal-api binds every node-bound ClusterApi RPC — `PublishNodeState`, the
node desired-state and reconcile calls (node-ctl), quarantine recovery plans and
artifact backups (sandbox-manager) — to the node's registered address, the
Kubernetes Node's InternalIP that cluster-controller's node sync writes, and
refuses a call from any other source address
(`validate_origin_remote_addr` in `crates/libs/internal/api/server/src/lib.rs`).
The check reads neither the credential nor the transport: it applies alike to
the static token and to TokenReview-authenticated projected tokens, and internal
RPC TLS, being server-auth only, does not change it. So node-ctl and
sandbox-manager run in the node's network namespace. template-manager makes no
node-bound call, but under TokenReview authentication its template-release reads
are scoped to the node whose registered address is the caller's source address
(`template_read_scope`, `crates/libs/internal/api/server/src/adapter.rs`), and
nothing maps a pod IP to its node. From the pod network an org's private
templates do not resolve: sandbox creates from them and private-base builds
fail, which a pod-network trial confirmed, so template-manager runs on the host
network too. build-manager's only source-scoped read checks for an existing
upload, and an org-private base is resolved by template-manager, so it runs on
the pod network; node-api reaches it through the node-local
`space-builder-build-manager` Service, named as its TLS leaf.

Services whose `/space/run` directory the node image does not bake (volume-manager,
node-ctl, runtime-manager, and credential-manager's volume-socket directory)
create it in a `prepare-run-dir` init container with only CHOWN and FOWNER; build-manager, which does not migrate its SQLite database
itself, applies its schema in a `migrate-sqlite` init container. Every other
service migrates its own store after taking its service mutex.

**Startup and probes.** Nothing orders the services. Each tolerates a peer that
is not ready yet by failing its own startup or retrying, and the kubelet restarts
that pod alone (sandbox-manager, for example, exits until node-manager answers;
node-api retries sandbox-manager's capability check). Services with an
instance-local health socket (`--health-socket=/run/health/<service>.sock` on a
pod-private emptyDir) are probed on it: liveness answers once the socket is bound,
readiness is SERVING while a successor waits passively for the service mutex and
follows the production socket once active
(`crates/libs/common/svc/src/local_health.rs`). runtime-manager is probed on its
service socket and build-manager on its gRPC health service.

**Rollouts and termination.** Services that hold a node-wide service mutex
(`/space/run/<service>/service.mutex`) roll with `maxSurge: 1, maxUnavailable: 0`:
the successor starts beside the predecessor, reports ready while passive, and
takes over, sockets replaced by rename, when the predecessor drains and releases
the lock. That covers node-manager, credential-manager, template-manager,
sandbox-manager and node-ctl. volume-manager, runtime-manager, node-api and
build-manager delete first: volume-manager because this chart runs the separate
`space-volmgr` identity, and a successor under a different identity can never
take a predecessor's lock, which is also why a cluster whose earlier release
ran the legacy identity must be recreated rather than upgraded in place. runtime-manager holds no mutex, so a successor would
take its socket by rename at once, beside a predecessor its probes can no longer
reach, and its kernel installer empties shared staging on open, which is only
safe when nothing in staging is live. node-api's declared port is a host port the
scheduler will not place twice on a node. build-manager has no mutex and recovers
jobs before binding its listener. Termination grace follows each service's drain: 60 s for
the 25 s drains the shared lifecycle defaults are sized against, 120 s for
volume-manager's 100 s transfer drain plus 10 s server drain, 180 s for
node-api's 150 s shutdown grace, and 7500 s for build-manager's 7200 s build
drain. Each chart's values carry the comment and the citation. Builder node-api
pods also sleep 5 s in preStop: on SIGTERM node-api closes its listener and
answers new calls "draining" at once, while the API reaches builders through the
`space-builder-node-api` Service and does not retry a draining build start, so
the delay lets the pod leave the Service's endpoints first.

**The runtime-supervisor.** node-manager's init container runs the production
`node-manager-init.sh` unchanged: it stages the host binaries under
`/space/bin/releases/<tag>`, switches the `/space/bin` links, stages the guest
kernel, and installs, starts or schedules the restart of
`space-runtime-supervisor.service` on the node through `nsenter --target 1` and
systemctl. A changed supervisor binary or compatibility file touches
`/space/run/runtime-supervisor/restart-required`; while the old node-manager runs,
the restart is deferred, and the successor node-manager performs it once it
holds its mutex (`SPACE_NODEMGR_ACTIVATE_RUNTIME_SUPERVISOR_ON_START=true`,
`node-manager-runtime-supervisor.sh activate`). Production keys the release
directory by its image tag; every Kind build is tagged `local`, so the chart
passes the digest of the staged binaries and the compatibility file as
`SPACE_IMAGE_TAG`. Firecracker jails therefore live in the node's mount
namespace and outlive every service pod. The unit reads
`/space/config/space.env`, which a production node's bootstrap writes; on Kind
the chart's `kind-node-env` init container writes it first (see the shim's
telemetry below) and touches `restart-required` when it changes, so a new
telemetry mode reaches a running supervisor through the same deferred restart.
Inspect it with
`docker exec <node> systemctl status space-runtime-supervisor` and
`docker exec <node> journalctl -u space-runtime-supervisor`.

**The operator shell.** The smoke harness's on-node checks and fault injection —
the node-local CLIs (`space-sbxmgr`, `space-tmplmgr`, `space-rtmgr`,
`space-volmgr`), `btrfs`, `dmsetup`, `cryptsetup`, `ctr`, files under
`/space/cache` and `nsenter --net` into a pod's network namespace — run in the Kind-only `space-node-debug` DaemonSet on each
sandbox worker and builder, a root shell standing in for an operator's shell on
the node host. Logs and environment are read from the pod of the service that
owns them. This keeps every service container to the identity, tools and
privileges its own code needs; the shell holds no ServiceAccount token and no
`component` label, so neither the API nor node-service readiness ever sees it.

Look at one service with
`kubectl --context kind-space-local -n space logs ds/space-<service>` (builder:
`ds/space-builder-<service>`), and at all of a node's pods with
`kubectl --context kind-space-local -n space get pods -o wide -l app.kubernetes.io/part-of=space-node --field-selector spec.nodeName=<node>`.

## Upgrading from the node-stack layout

A cluster created before the per-service DaemonSets runs one `space-node-stack`
pod per node, with runtime-supervisor and every Firecracker VM inside that pod.
Those VMs cannot move to the node's supervisor, and the two layouts must not run
on one node, so such a cluster is recreated once rather than upgraded.
`bootstrap.sh`, the smoke suites' `scripts/tests/lib/bootstrap-kind-stack.sh`
and the `Tiltfile` run `infra/space/kind/scripts/check-node-layout.sh` against
an existing cluster before they apply anything; when it still has resources of
the old `space-node-stack` chart, they list them and stop without changing
anything (the scripts exit 1; Tilt reports a Tiltfile load error). The same
check refuses a cluster whose node containers do not publish every host port
mapping `kind-cluster.yaml.tmpl` declares, the load-balanced entry's among
them, naming each missing one. Recreating
deletes the cluster's sandboxes, snapshots and templates and keeps the
containerd image caches:

```bash
bash infra/space/kind/scripts/teardown.sh
bash infra/space/kind/scripts/bootstrap.sh   # then `tilt up`, for the Tilt loop
```

A cluster already on the per-service layout takes the move to internal RPC TLS
in place: the next bring-up or Tilt reload issues the TLS Secrets and every
listener and caller rolls onto TLS. While they roll, a plaintext caller and a TLS
listener refuse each other, and the services' own restarts carry them until both
sides have moved.

Organizations enrolled for BYOK before the authority used the key broker keep
an `in-memory` master-key row that no authority serves any more, so their
encrypted sandboxes and fields stay unreadable, as they already were after any
restart of the old single authority. Recreate the cluster to start them clean.

The single-worker DirectSubstrate bench (`space-local-3486`, Tilt
`SPACE_LOCAL_MINIMAL=1`) was retired with that layout: its node side was the old
entrypoint's SQL registration of the node, Postgres access from the node that
nothing else needs, and no suite ran on it.
Delete a leftover cluster with `kind delete cluster --name space-local-3486`.

## Service identities

Each node service runs under the Unix account and supplementary groups the
systemd units and EKS charts declare (`infra/space/identity/node-identities.sh`):
sandbox-manager, volume-manager and credential-manager as `space-sbxmgr`,
template-manager as `space-tmplmgr`, node-ctl as `space-nodectl`, node-api as
`space-nodeapi`, the runtime manager as `space-rtmgr`; runtime-supervisor,
node-manager and the builder's build-manager stay root. The per-sandbox
net-gateway is launched by the host runtime-supervisor with that sandbox's
gateway identity. Directory ownership matches packer `07-sysctl.sh` and the
DaemonSet init containers, so **UDS peer authorization (SO_PEERCRED) is live in
this harness** — e.g. warm-pool fills are authorized as `Service(space-tmplmgr)`,
not silently as root.

The runtime manager only lists the node's Firecracker releases and kernels
(`space-rtmgr list`): its DaemonSet starts it with `--list-only`, so it opens
no install root; nothing depends on it yet, and it restarts on its own like
every other service. The baseline Kind smoke checks its JSON listing on both a
worker and a builder, requiring a complete scan with a valid vendor runtime and a
staged cache kernel. These checks do not gate node readiness.

**Credential-manager.** The authority runs on the control plane as the
`space-credmgr-authority` Deployment (`charts/space-credmgr-authority-local`,
named as in the EKS module): `--role=authority --no-uds` on TCP port 9810, with
the control-plane database, the platform KEK and the optional `space-aws-creds`
for its AWS integrations. It authenticates callers by TokenReview of their
projected ServiceAccount tokens (audience `space-credential-manager`), maps each
ServiceAccount to its client service, and never accepts the static internal
token. On every request it also requires the token's pod to be current and
Running in its namespace, with the connection's source address among that
pod's IPs, and the token's node to be registered under that name and UID; its
Role lists and watches the namespace's pods for this. Builder nodes fail the
node check: internal-api registers a node without its UID, which only the
node inventory sync, listing the sandbox-selector nodes, or a sandbox pod
placed on the node records
(`crates/libs/api/src/controllers/substrate/kubernetes/node_sync.rs`), so the
authority refuses builder callers with `node_identity_mismatch`. Kind's
builders do not need it: their build-manager and volume-manager read the
object store's static keys instead of exchanging tokens. Each node's
credential-manager runs `--role=agent`: it holds the node's credential sockets
under its service mutex and forwards every call upstream as the node credential
agent, with no database, KEK or AWS credential. node-manager
calls the authority directly for sandbox volume DEKs, which the authority grants
only to node-manager. The platform KEK takes the production provider path, the
intermediate-key broker: the authority mints each org's intermediate key there
on first use, keeps only the opaque wrapped key in Postgres and unwraps it
through the broker. Kind's broker is the in-repo fixture
(`charts/space-kind-key-broker`, Kind-only image `docker/Dockerfile.kind-key-broker`),
reached over plaintext because the fixture has no TLS listener. It derives keys
from the tenant reference and keeps no state, so keys survive restarts of the
broker and the authority and every replica serves every org, and so Kind's keys
protect nothing. The authority runs two replicas and rolls one at a time. Agents
started before the authority exit and are restarted until it answers.

**Internal RPC TLS.** Kind runs ClusterApi, NodeApi, BuildManager,
IngressActivationApi and the credential-manager authority over authenticated
TLS, as deployed services do
([Internal RPC TLS](../../../docs/internal-rpc-tls.md));
`SPACE_INTERNAL_RPC_INSECURE_DEV` is not set anywhere. Both bring-up paths run
[`scripts/internal-rpc-tls.sh`](scripts/internal-rpc-tls.sh) before any
workload, which keeps the cluster's material in Secrets named as on EKS
(`internal_rpc_tls.tf`, `credmgr_authority_tls.tf`): a per-cluster ECDSA P-256
CA (`space-internal-rpc-ca`, mounted by every caller at `/etc/space-rpc-ca`),
one-year leaves reissued within 60 days of expiry for internal-api
(`space-cluster-api-tls`: `space-internal-api`, `space-cluster-api`), every
node-api (`space-node-api-tls`: `space-node-api`), build-manager
(`space-build-manager-tls`: `space-builder-build-manager`) and the api's
ingress activation listener (`space-ingress-activation-tls`: `space-api`, the
Service internal-api dials; the EKS module issues this Secret's leaf for
`space-ingress-activation` instead), each mounted only by
its listener at `/etc/space-rpc-tls` with the key readable by the listener's own
identity, and the authority's self-signed end-entity certificate
(`space-credmgr-authority-tls`, trusted by its clients through
`space-credmgr-authority-trust`). The leaves carry service names only, so
callers that dial node-api by node IP set `SPACE_NODE_API_TLS_SERVER_NAME=space-node-api`,
while builder node-api dials build-manager by the leaf's own name, through the
node-local `space-builder-build-manager` Service. The CA's key
stays in the Kind-only `space-internal-rpc-ca-signing` Secret, which no workload
mounts. The script restarts a listener whose identity it reissued, and every
workload if the CA itself changed, because neither reloads them while running.
TLS here is server-authenticated: bearer tokens still authorize callers, and
internal-api's node-address check is unchanged. The api's ingress activation
listener admits only internal-api's ServiceAccount, by TokenReview of its
projected `space-cluster-api` token with no static fallback, as
[Ingress activation delegation](../../../docs/internal-rpc-tls.md#ingress-activation-delegation)
asks of the hosting deployment.

## Differences from the EKS deployment modules

| EKS modules                                               | Local kind                                                                    |
|-----------------------------------------------------------|-------------------------------------------------------------------------------|
| One DaemonSet per node service, systemd watchdogs via k8s probes | One DaemonSet per node service and role from the Kind charts (`charts/space-<service>-local`), probed on each service's health socket; runtime-supervisor is the same host systemd unit node-manager's init container installs |
| Node image (packer) provides `/space/cache`, the cgroup subtree, loop devices, device permissions, SPACE accounts and the `/space` skeleton before any pod starts | Sandbox workers and builders boot the SPACE Kind node image, which bakes in the same as systemd units that run before kubelet on every node start (see [The node layer](#the-node-layer)) |
| `/space/cache` is btrfs on instance-store NVMe            | btrfs on a sparse loopback image at `/var/lib/space-cache.img` on the kind node (40 GiB, same code paths, less I/O headroom), mounted on the node's `/space/cache` by `space-cache.service`, so it outlives pod restarts like the NVMe does |
| Postgres on RDS, secrets via ExternalSecrets / AWS Secrets Manager | In-cluster `space-postgres` StatefulSet, static Kubernetes Secrets             |
| S3 for snapshots + template artifacts                     | In-cluster versitygw (real S3 protocol, sigv4, path-style)                    |
| `space-credmgr-authority` Deployment whose platform KEK comes from an external intermediate-key broker, with TLS from its own certificate (`credmgr_authority_tls.tf`; the direct KMS backend is gone, `credmgr_authority_kms.tf`) | two replicas on the same broker path against the in-repo broker fixture (`space-kind-key-broker`), which derives keys instead of protecting them; TLS from a self-signed certificate `internal-rpc-tls.sh` issues per cluster |
| Internal RPC CA and listener leaves issued by Terraform and synced by External Secrets (`internal_rpc_tls.tf`) | the same Secrets issued per cluster by `scripts/internal-rpc-tls.sh`; the ingress activation leaf names `space-api` rather than `space-ingress-activation` |
| volume-manager credentials via credmgr STS/IRSA           | static `SPACE_OBJECT_STORE_STATIC_*` creds (the documented local escape hatch) |
| build-manager gets S3 credentials via credential-manager backed by cloud identity | the same directly injected static object-store credentials; credential-manager's storage exchange endpoint is explicitly disabled |
| `/dev/kvm` access via the AMI's kvm group                 | `space-kind-devices.service` makes `/dev/kvm`, `/dev/vhost-vsock` and `/dev/vhost-net` mode 0666 on the node (kind-only; never replicate on a worker) |
| Loop devices from the AMI's `max_loop=10000` on a real devtmpfs | The node's tmpfs `/dev` starts with 8 nodes; `space-loop-nodes.service` mknods the rest before kubelet, pods copy the node's `/dev`, and node-manager raises its pod's pool further. The cache device has autoclear set, so a node stop releases it (`scripts/lib/space-loop-devices.sh`) |
| vhost-vsock jail nodes provided by the AMI                | the Kind-only `space-kind-vsock-jails.service` mknods them into each jail under `/space/cache/firecracker/jails` as it appears (kind-only) |
| Sandbox SSH key generated by Packer per AMI               | generated per host by `build-seed.sh` under `~/.space-devbox/kind/ssh` |
| ALB ingress in front of api / data-plane / ui             | NodePort entry Services behind the cluster's host port mappings for api and data-plane (kube-proxy balances per connection); `kubectl port-forward` for the single-replica ui |
| The runtime manager installs releases and kernels, which node-manager publishes into the root-owned store `/space/vendor/artifacts` | `/space/vendor` is a read-only host bind, so the runtime manager runs `--list-only`: listing and cache checks work, installs are off |
| Operators run node-local CLIs from the node host's shell   | the same CLIs run in the Kind-only `space-node-debug` root shell DaemonSet     |
| Build sizing, concurrency, and multipart sizes are configurable | 20 GiB default build disk, one concurrent build, and smaller multipart parts fit the 40 GiB local btrfs cache |

`space-seed-publish` remains the bootstrap for `seed:latest`; it is not a
substitute for recipe builds. ECW `Template.build` reaches the builder
node-api, build-manager launches a real build VM, and the result is published
back through the `templates` bucket. The builder sandbox-manager enables
incomplete-route compatibility because build-manager's node-local recipe VMs
have no public control-plane route identity. The worker release keeps that
compatibility disabled.

> **Note for the node-drain e2e test**: draining a sandbox worker terminates
> its k8s Node object (`SPACE_NODE_DRAIN_TERMINATOR_KIND: kube-delete`).
> Recover the cluster with
> `docker exec space-local-worker<N> systemctl restart kubelet`.

## Node layout

The cluster has six nodes with dedicated roles so that draining a sandbox
worker cannot disrupt control-plane components:

| Node                          | Pool label  | Purpose                                                                                 |
|-------------------------------|-------------|-----------------------------------------------------------------------------------------|
| `space-local-control-plane`   | —           | k8s control-plane only                                                                  |
| `space-local-worker`          | `sandbox`   | Runs the worker node-service DaemonSets + receives SPACE sandbox pods                    |
| `space-local-worker2`         | `sandbox`   | Same, second worker                                                                      |
| `space-local-worker3`         | `infra`     | Runs Postgres, object store, space-api, internal-api, data-plane, cluster-controller, UI    |
| `space-local-worker4`         | `builder`   | Runs the builder node-service DaemonSets, including build-manager and builder node-api    |
| `space-local-worker5`         | `builder`   | Same, second builder                                                                       |

Sandbox pods carry `nodeSelector: node.kubernetes.io/space-enabled=true`,
which is only applied to the two sandbox workers. Control-plane components
carry `nodeSelector: space.pplx.net/infra=true`, which targets only the
infra node. The builder release selects only `pool=builder`, and the builder
nodes are deliberately not `space-enabled`, so user sandbox pods cannot land
there.

The two sandbox workers also advertise an extended resource
`space.pplx.net/network-slots: 100` so the kube scheduler can satisfy each
sandbox pod's `space.pplx.net/network-slots: 1` request. Bootstrap advertises
a static pool in place of node-controller's dynamic capacity updates.

## The node layer

A production worker or builder boots from a node image that already holds its
node layer (`infra/space/packer`): the packages, scripts and enabled systemd
units that give it a btrfs `/space/cache`, a cgroup subtree, loop-device
headroom, device permissions, SPACE accounts and the `/space` skeleton before
any SPACE pod starts. Kind does the same with the **SPACE Kind node image**,
[`node/Dockerfile`](node/Dockerfile): `kindest/node:v1.34.0` (the default of
the pinned kind v0.30.0, pinned by digest) plus `btrfs-progs`,
[`node/space-kind-node.sh`](node/space-kind-node.sh) with its libraries, and
these units, enabled in the image:

| Unit | Does | Production counterpart |
|---|---|---|
| `space-loop-nodes.service` | mknods loop-device nodes in the node's `/dev` (64, raised past any index in use); pods copy the node's `/dev` when they start | `max_loop=10000` (07-sysctl.sh) |
| `space-kind-devices.service` | mode 0666 on `/dev/kvm`, `/dev/vhost-vsock`, `/dev/vhost-net` | the `kvm` group; Kind-only, never on a worker |
| `space-cgroup.service` | `/sys/fs/cgroup/space` with `+cpu +memory +io +pids` delegated | `space-cgroup.service` (07-sysctl.sh) |
| `space-layout.service` | the SPACE groups and service accounts; the `/space/run` skeleton and dependency-drain gates; `/space/bin/{firecracker,jailer}` links onto the vendor tree; `/space/etc/sandbox.toml`; `/var/run/netns` | 07-sysctl.sh accounts and directory skeleton, 04-firecracker.sh links |
| `space-cache.service` | formats `/var/lib/space-cache.img` on first boot (sparse, 40 GiB), mounts it on `/space/cache` with loop autoclear and shared propagation, then creates the cache skeleton | 01-partitions.sh / 11-nvme-cache.sh |
| `space-kind-vsock-jails.service` | long-running: mknods `/dev/vhost-vsock` into each Firecracker jail under `/space/cache/firecracker/jails` as it appears (the jailer does not) | the node image provides it; Kind-only |

They are ordered `Before=kubelet` and run on every boot of the node container,
including after `docker restart`, which gives it a fresh `/dev`, mount table
and cgroups; all but the vhost-vsock unit are oneshots. Nothing is installed
when the cluster comes up. The sandbox workers and builders run this image
(`kind-cluster.yaml.tmpl`); the control-plane and infra nodes run its
base unchanged, as production's non-SPACE nodes carry no SPACE node layer, so
every node runs the same Kubernetes version. The cache image sits on the node's
`/var`, which kind keeps on a Docker volume, so the cache survives pod and node
restarts. Defaults are baked like the rest of the layer; a node's
`/etc/space/kind-node.env` can override `SPACE_CACHE_SIZE_MB` and
`SPACE_LOOP_DEVICE_TARGET` for its next boot.

**Distribution.** [`kind-node-image.sh`](scripts/kind-node-image.sh) follows
`ci/scripts/ci-image.sh`. The tag is the SHA-256 of the image's inputs (the
Dockerfile and every file it copies), and the build context holds exactly those
files. `ensure` uses a local image with that tag, else pulls it from
`ghcr.io/perplexityai/space-kind-node`, else builds it (about ten seconds on a
warm Docker cache). Both bring-up paths run it before `kind create`, and the
renderer writes the tag into the cluster config. Main CI publishes each new tag
once; Kind CI jobs pull it and fall back to a build. Builds take the
`kindest/node` base from the third-party image mirror (`ci/mirrored-images.txt`)
when `SPACE_CI_IMAGE_MIRROR` is set, and from the source otherwise. A package
administrator has to make the `space-kind-node` package public for bring-ups
outside CI to pull it; until then they build it, with the same result.

Firecracker, its jails and the per-sandbox sidecars run on the node under
`space-runtime-supervisor.service`, so the jails live in the node's mount
namespace, a slave of the node's shared `/space/cache`. node-manager mounts the
cache `Bidirectional`: the bind mounts it makes into a jail — the instance
rootfs before launch, the snapshot memory file after Firecracker is already
running on restore — propagate to the node and from there into the jail. Every
other pod receives the cache `HostToContainer` (`shared,slave` inside the pod):
it sees the node's cache, its remounts and node-manager's mounts, and the
mounts it makes itself (build-manager's OCI rootfs and assembly mounts) stay in
the pod and never reach the node's mount table. Check a node's view with
`docker exec <node> findmnt -o TARGET,FSTYPE,PROPAGATION`.

Diagnose a node with `docker exec <node> systemctl status 'space-*'` and
`docker exec <node> journalctl -u space-cache`. To start a node's cache clean or
at a new size, delete the node-service DaemonSets of that node's role, run
`systemctl stop space-runtime-supervisor && umount /space/cache && rm
/var/lib/space-cache.img` on the node (adding `SPACE_CACHE_SIZE_MB=<MiB>` to
`/etc/space/kind-node.env` for a new size), then `systemctl restart
space-cache` and re-apply the charts. Recreating the cluster
also starts clean.

**Clusters created before this image, or from an older build of it, must be
recreated.** A kind node keeps the image it was created from, so such a
cluster's SPACE nodes lack the layer or carry a stale one. Every bring-up path
(`bootstrap.sh`, the smoke suites' bring-up, and Tilt's `check-kind-node-image`
resource) runs
[`check-kind-node-image.sh`](scripts/check-kind-node-image.sh), which compares
each SPACE node's image label with the tag this tree builds and, on a mismatch,
stops with the command to run:

```bash
kind delete cluster --name space-local   # keeps the containerd image caches
bash infra/space/kind/scripts/bootstrap.sh   # or `tilt up` via the smoke bring-up
```

Recreating rather than patching nodes in place is the production model: a new
node image reaches a fleet by replacing nodes, not by installing onto running
ones. Keeping an in-place path would have meant a second delivery mechanism
(installing packages and units into live node containers) that has to track
every change to the image and is exercised only on old clusters, which is where
it would drift. Recreation costs a few minutes because the containerd image
caches stay on disk; it does delete the cluster's sandboxes, snapshots and
published templates.

## Smoke test

Main CI builds the Kind Rust artifact image once and hands that exact image to
two isolated runners. Each runner brings up its own Kind/Firecracker stack,
runs the baseline smoke, then runs one half of the eligible ECW and native
inventories. Scenarios remain serial within each shard because some mutate
cluster-wide state. Both shards must pass for `space-ci` to pass.

The CI-only `SPACE_KIND_RUST_ARTIFACTS=prebuilt` mode checks both
`SPACE_KIND_PREBUILT_RUST_SHA` against the checkout and
`SPACE_KIND_PREBUILT_RUST_IMAGE_ID` against the loaded Docker image. A missing
or mismatched artifact fails rather than falling back to compilation. Local
build defaults are unchanged.

To reproduce a shard, use `scripts/tests/smoke-kind-typed.sh all 1/2` (or `2/2`)
with the same admin-token setup as a full `all` run. The direct CLI equivalent
is `python -m space_smoke run --all --shard 1/2` for the selected API surface.
Shards partition sorted eligible scenario IDs after backend, surface, template,
and quarantine filtering; run every shard from the same checkout. Never run
shards concurrently against the same cluster. Without a shard argument, the
full inventory and its existing ordering are unchanged.

Several smokes target this cluster:

```bash
# 1. Portable typed ECW inventory on the real Kind/KVM substrate
SKIP_BOOTSTRAP=1 scripts/tests/smoke-kind-typed.sh ecw

# 2. Minimal e2e: login → create → mkdir → write → read → delete
SKIP_BOOTSTRAP=1 scripts/tests/smoke-kind-e2e.sh

# 3. Full native API surface: + metadata, identity boundary, command SSE,
#    data-plane WS, pause/resume, suspend/restore. Set
#    SPACE_SMOKE_SKIP_INTERACTIVE=1 to skip the interactive WS helper
#    (which was finicky behind kubectl port-forward) and reach the
#    lifecycle stages.
TOKEN=$(SPACE_CONSOLE_PASSWORD="${SPACE_CONSOLE_PASSWORD:-password}" \
  jq -nc '{email:"root@local",password:env.SPACE_CONSOLE_PASSWORD}' | \
  curl -sS -X POST http://localhost:8080/auth/v1/sessions \
  -H 'content-type: application/json' \
  --data-binary @- | jq -r .token)
SPACE_API_URL=http://127.0.0.1:8080 \
  SPACE_DATA_PLANE_URL=http://127.0.0.1:8090 \
  SPACE_API_TOKEN="$TOKEN" SPACE_TEMPLATE=seed:latest SPACE_RUNTIME=fc \
  SPACE_SMOKE_SKIP_INTERACTIVE=1 SPACE_READY_TIMEOUT_MS=300000 \
  scripts/tests/smoke-spaced-rest.sh

# 4. Forced node drain: the resident is evacuated to the second worker through
#    a real suspend snapshot in the object store and its filesystem survives. The
#    admin endpoint's default is `gentle`, which cordons and waits for
#    residents to leave instead; this scenario sends `forced`.
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw \
  uv run --frozen --python 3.11 -m space_smoke run local.forced_drain_evacuates_sandbox

# 5. Public ECW Template.build → ready template → sandbox → command checks
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=ecw \
  uv run --frozen --python 3.11 -m space_smoke run local.ecw_template_build_from_oci_boots

# 6. Explicit AWS Secrets Manager network-credential injection. Before
#    bootstrap, put short-lived sandbox-account AWS_ACCESS_KEY_ID,
#    AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, and
#    SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN in private.env. The selected
#    identity must be able to sts:AssumeRole and sts:TagSession on that role;
#    the role reads the task-owned ARN only when the secret's space-org-id tag
#    equals the smoke organization's ID (docs/configuration.md).
#    The controlled HTTPS endpoint must return only the lowercase SHA-256 of
#    the Authorization header it receives.
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=native \
  SPACE_API_URL=http://127.0.0.1:8080 \
  SPACE_DATA_PLANE_URL=http://127.0.0.1:8090 \
  SPACE_API_TOKEN="$TOKEN" SPACE_TEMPLATE=seed:latest \
  SPACE_SMOKE_SECRET_ARN="$TASK_OWNED_SECRET_ARN" \
  SPACE_SMOKE_ECHO_URL="$CONTROLLED_DIGEST_ECHO_URL" \
  SPACE_SMOKE_SECRET_HEADER_SHA256="$SECRET_HEADER_SHA256" \
  uv run --frozen --python 3.11 -m space_smoke run \
    native.aws_secret_network_credential_injection
```

The AWS scenario is excluded from implicit inventory runs because its secret
and echo service are intentionally caller-owned. It deletes its sandbox,
detaches the policy rule, and retires the SPACE credential reference; the
caller remains responsible for deleting the AWS secret and echo fixture.

The template-build scenario (typed, run by name; the shell suite was retired
2026-09-23) seeds a run-scoped local ECW key, uses the pinned E2B Python SDK
against `http://127.0.0.1:8080` with the allowlisted loopback Host header, and
routes command checks through the public data-plane on `:8090`.
Using loopback for both the URL and Host also makes the SDK's separate upload
client resolve the absolute upload URL without local DNS configuration.
`Template.build` polls the public build-status endpoint until the recipe and
publication phases report ready. The smoke publishes both `latest` (so
`Sandbox.create(alias)` resolves the release) and a `kind-smoke` marker tag,
then requires the first public sandbox launch to succeed. Guest commands use
the public data plane, where the API route token is available.

### Debugging a template build

All commands below pin the kind context explicitly:

```bash
kubectl --context kind-space-local -n space get ds,po,svc -o wide \
  -l app.kubernetes.io/name=space-builder
kubectl --context kind-space-local -n space describe ds/space-builder-build-manager
kubectl --context kind-space-local -n space logs ds/space-builder-build-manager --tail=200
kubectl --context kind-space-local -n space logs ds/space-builder-node-api --tail=200
kubectl --context kind-space-local -n space logs ds/space-builder-template-manager --tail=200

aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://templates/ --recursive
```

`ds/<name>` picks one of the two builders' pods; name a pod to read a specific
builder. build-manager is Ready once its `:9900` gRPC health service serves,
and node-api once its health socket reports ready. A Pending pod usually means
the `pool=builder` node is missing; a running but unready pod should be
diagnosed from its own log and, after a restart, from
`kubectl logs <pod> --previous`. The build VM's commands log through
build-manager, which the Datadog Agent and the Alloy log stack exclude.

**Current pass/fail map** (as of this revision):
- `smoke-kind-e2e.sh` — **PASS** (create → real Firecracker boot of the
  locally built seed → file round-trip → delete).
- `smoke-spaced-rest.sh` — **PASS** through metadata, identity boundary,
  command SSE, file ops, pause/resume (~1s each), and suspend/restore
  (~6s round trip incl. the object-store upload). The trailing immediate
  DELETE can hit a retryable "suspend-snapshot upload in flight" conflict
  when issued within seconds of the lifecycle churn — retry it.

Two harness pitfalls worth knowing about, both fixed here and both
producing the same wedge (snapshot rows stuck in `creating`, restores
never firing) if reintroduced:
- **Mount propagation**: the Firecracker jailer unshares a mount
  namespace, and node-manager bind-mounts the snapshot memory file into
  the jail *after* FC is running. On a prod host the systemd-`rshared`
  root propagates that mount into the jail; from a pod it propagates only
  if the pod's mount is `Bidirectional` and the node's `/space/cache` is
  shared, and otherwise FC opens the 0-byte bind target ("Total sum of
  memory regions exceeds largest possible file offset"). node-manager's
  chart mounts the cache `Bidirectional`; `space-cache.service` makes the
  node's mount shared.
- **Object store layout**: versitygw's POSIX backend stores each bucket as a
  directory and each object as a file under the infra node's
  `/var/lib/space-object-store`, with ETags and user metadata in `user.*`
  xattrs. kind mounts every node's `/var` as an anonymous Docker volume and
  no extraMount covers this path, so the data and its xattrs live on the
  filesystem backing that volume (under Docker's data root), not on the
  node's overlay root. There are no
  pre-allocated volumes or volume slots to exhaust (the SeaweedFS chart it
  replaced needed `-master.volumeSizeLimitMB=1024 -volume.max=48`), but
  CompleteMultipartUpload assembles the parts with `copy_file_range`, which on
  ext4 rewrites the object once.

One product-hardening observation stands: after a genuinely failed restore
attempt, sbxmgr's restore guard only self-heals `Suspended`/`NotFound`
local states, so a sandbox whose local state reached `failed` wedges
(every retry FailedPreconditions, DELETE stays blocked on the in-flight
upload). Only reachable after a real restore failure.

## Real S3 snapshot + template path (object store + volume-manager)

`space-object-store` runs versitygw, an S3-compatible gateway, on the infra node, with
a one-shot Job that creates the `snapshots` and `templates` buckets. The S3
API is port-forwarded to [localhost:8333](http://localhost:8333).

`space-volmgr-svc` performs real S3 transfers to the object store with static creds
from the `space-object-store` Secret — both suspend/restore snapshot artifacts
(`snapshots` bucket) and template pulls (`templates` bucket, populated by the
`space-seed-publish` Job). See `crates/libs/vol/mgr/server/src/run.rs`.

Verify the round-trips with the object-store port-forward:

```bash
aws --endpoint-url http://localhost:8333 s3 ls s3://snapshots/ --recursive
aws --endpoint-url http://localhost:8333 s3 ls s3://templates/ --recursive
```

### Object store transition

The object store was SeaweedFS (`space-seaweedfs` chart, Service
`seaweedfs:8333`, data in `/var/lib/space-seaweedfs` on the infra node). It
is now versitygw (`space-object-store` chart, Service `object-store:8333`,
data in `/var/lib/space-object-store`). The two stores share no data, and the
database of a SeaweedFS-era cluster records every suspend snapshot and
published template at `http://seaweedfs:8333`. Applying the new charts to such
a cluster would leave its restores and template pulls depending on a
deployment nothing manages any more, and pointing the old Service at the new
gateway would not help, because the objects are not in it.

An existing cluster is therefore recreated, not migrated. `bootstrap.sh`, the
smoke suites' `scripts/tests/lib/bootstrap-kind-stack.sh` and the `Tiltfile`
run `infra/space/kind/scripts/check-object-store-transition.sh` against an
existing cluster before they apply anything; when it still has resources from
the `space-seaweedfs` chart, they list those resources and stop without
changing anything: the two scripts exit with status 1, and Tilt reports a
Tiltfile load error. Recreating deletes the cluster's sandboxes,
snapshots and templates, and keeps the containerd image caches:

```bash
bash infra/space/kind/scripts/teardown.sh
bash infra/space/kind/scripts/bootstrap.sh   # then `tilt up`, for the Tilt loop
```

A smoke suite such as `scripts/tests/smoke-kind-e2e.sh` creates the cluster
itself after the teardown. For a non-default cluster, set `SPACE_KIND_CLUSTER`
on both commands.

## Tearing down

```bash
bash infra/space/kind/scripts/teardown.sh           # scripted path: removes everything
bash infra/space/kind/scripts/teardown.sh --keep-cluster   # remove workloads, keep kind cluster
```

Tilt users can still do the previous `tilt down && kind delete cluster --name space-local`.
