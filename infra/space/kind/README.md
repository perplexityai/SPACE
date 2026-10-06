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

The single-worker DirectSubstrate bench is opt-in: create that cluster with
`kind-cluster-3486.yaml` and set `SPACE_KIND_CLUSTER=space-local-3486
SPACE_LOCAL_MINIMAL=1` before `tilt up`. Use it only for tests specifically
targeting DirectSubstrate.

If startup reports that the Tilt server on `:10350` could not be verified as
`kind-space-local`, inspect `tilt get cluster default --port 10350 -o yaml`.
A watcher for the old `space-local-3486` bench must be stopped in its owning
session before starting the full stack. An initializing watcher can be retried
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

Kind does not model the full publisher isolation boundary. The builder node-stack
shares one ServiceAccount and a readable projected token among build-manager,
node-api, template-manager, sandbox-manager, and node-ctl. All those services
therefore carry operator and tenant publisher authority. Separately, the Kind
node-api and build-manager listeners still accept the shared static token as
trusted build authority: a worker can submit an operator build through that
route even though ClusterApi rejects direct worker publication.

The direct-registration tests prove ClusterApi's publisher checks, not end-to-end
worker isolation. That requires a separate build-manager ServiceAccount and
Kubernetes authentication on the builder listeners, with each system-build caller
explicitly reviewed. Kind's shared identity and static build credentials are
local development limitations and must not be used as a secure deployment model.

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
creates the kind cluster (if missing), labels nodes, advertises the
`space.pplx.net/network-slots` extended resource, installs the containerd
shim on the sandbox workers, applies the `space` Namespace + `RuntimeClass`,
builds every image and `kind load`s it (first run is the long step; the
incremental build reuses its target dir and the Docker build is
cargo-chef-cached after that), and applies every chart with
`helm template | kubectl apply`.

### Metrics (isolated local experiment)

For an opt-in Collector → Prometheus stack consuming SPACE OTLP
metrics, see [the metrics runbook](metrics/README.md). It uses its own
namespace and synthetic workload, preserves the Datadog Agent, and does not
change shared deployment defaults. Grafana is an optional independent release;
it is not required for ingestion or query validation.

### Logs (optional)

The isolated [Alloy → Loki setup](logs/README.md) collects SPACE
container and Kind node journal logs without changing application images,
Datadog collection, or metrics/traces settings. It includes local-only UI access,
an existing-process-fixture validation workflow, and scoped cleanup commands.
Metrics and visualization releases are optional and independently deployed.

### Datadog (optional)

The Kind-only `datadog-agent-local` chart supports two explicit backends:

| `SPACE_KIND_DATADOG_MODE` | Backend | Credential |
| -- | -- | -- |
| `fakeintake` | In-cluster Datadog Fake Intake | Chart-rendered dummy key |
| `saas` | Approved Datadog site | Separate, existing dev-only Kubernetes Secret |
| `off` | Skip Agent reconciliation; leave any existing Agent running | None |

### OTLP telemetry

The `otel` mode sends metrics and traces to the Collectors and does not
require the Datadog Agent or Datadog credentials.

**The metrics and traces charts must be installed first.** They ship in
[#85](https://github.com/perplexityai/SPACE/pull/85) and
[#87](https://github.com/perplexityai/SPACE/pull/87); `otel` mode points every
workload at their Collector Services, so without them there is nothing
listening and exports fail. Create the Kind cluster, install the receivers,
then select the mode in bootstrap or Tilt:

```bash
bash infra/space/kind/metrics/local.sh up
bash infra/space/kind/traces/local.sh up
SPACE_KIND_TELEMETRY_MODE=otel bash infra/space/kind/scripts/bootstrap.sh
```

Rust workloads send metrics to
`http://collector.space-metrics.svc.cluster.local:4317` and traces to
`http://collector.space-traces.svc.cluster.local:4317` over plaintext
OTLP/gRPC. To use other receivers, override `telemetry.otlpMetricsEndpoint`
and `telemetry.otlpTracesEndpoint` in the workload Helm values. Endpoint and
protocol environment variables are per-signal and emitted only for OTLP
exporters. Logs stay on stdout for Alloy to collect; no log endpoint is needed.

SPACE uses the native Datadog exporters by default. Set
`SPACE_KIND_TELEMETRY_MODE=otel` for an exclusive OTLP comparison with
compatible application binaries. `datadog` is the default and rollback mode.

One thing the table does not show: `journald` suppresses Rust metrics rather
than writing them to the journal. The sink emits on a tracing target every
formatting layer filters out, and no journald layer is installed, so expect no
Rust metric output. It keeps Datadog traces.

The two selectors are independent. `SPACE_KIND_DATADOG_MODE` chooses where the
Agent sends what it collects, and `SPACE_KIND_TELEMETRY_MODE=otel` bypasses the
Agent entirely for metrics and traces — it needs the Collectors installed,
not an Agent. Pair them only when you want a side-by-side comparison.

There is no dual-publish or non-counter mode. The `otel` mode uses `always_on`
diagnostic trace sampling, not the production sampling policy. Logs remain
structured JSON on stdout in every mode; there is no OTLP log exporter.

The default is always `off`, even when `DD_API_KEY` is set. **`off` and stopping
Tilt do not stop an existing Agent's export.** Use the explicit rollback below.
For a local metrics/traces/logs comparison with no
telemetry egress to Datadog SaaS:

```bash
SPACE_KIND_DATADOG_MODE=fakeintake \
  bash infra/space/kind/scripts/bootstrap.sh --skip-images

# Or start the Tilt-managed stack in the same mode:
SPACE_KIND_DATADOG_MODE=fakeintake tilt up
```

Both paths install Agent `7.81.1`, expose Fake Intake at
`http://localhost:8088`, and set the Agent and SPACE workloads to the same
local environment. Environment-only settings require a Tilt restart; the
optional `SPACE_KIND_OBSERVABILITY_CONFIG` JSON file is watched and authoritative
over the launch environment, including during rollback.

SPACE keeps its native Datadog metrics and trace exporters unless the Kind-only
telemetry mode is changed. To send Rust custom metrics and traces through the
upstream OpenTelemetry SDKs and the Collectors instead — having installed
those charts first:

```bash
SPACE_KIND_TELEMETRY_MODE=otel tilt up
```

Add `SPACE_KIND_DATADOG_MODE=fakeintake` alongside it to keep an Agent running
for a side-by-side capture; `otel` does not require one.

`datadog` is the default and rollback mode. `otel` selects the exclusive
`SPACE_METRICS_SINK=otlp` and `SPACE_TRACE_EXPORTER=otlp` paths with an
`always_on` diagnostic trace sampler. `journald` suppresses Rust metrics and
leaves traces on Datadog. In `otel` mode the shim moves to OTLP as well.
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

SPACE bootstrap completes application readiness before installing optional
telemetry. An Agent failure is reported as a failure without preventing the
SPACE releases from being installed. Comparison runs still require verified
Agents and received payloads.

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
workloads, and `journald` disables shim metrics, since the shim has no
journald sink. In `datadog` and `journald` modes the wrapper leaves the SPACE
and OTel identity settings unset, so the shim's DogStatsD tags are exactly
what they were before this mode existed. Because containerd starts
the shim on the Kind node rather than in a Pod, `install-shim.sh` installs a
small production-shaped wrapper around the compiled binary instead of relying
on workload environment inheritance. In `otel` mode, installation looks up
`collector` in both `space-traces` and `space-metrics` using the selected
Kind context and uses `http://<ClusterIP>:4317` for each signal. Kind nodes cannot
resolve pod DNS, but kube-proxy routes the Service's ClusterIP. Installation fails
if either Service is missing or has no ClusterIP; install
`infra/space/kind/traces/local.sh up` and `infra/space/kind/metrics/local.sh up`
first, or set `SPACE_KIND_SHIM_OTLP_ENDPOINT` to another OTLP/gRPC receiver
reachable from the Kind node. An explicit override bypasses Service discovery
for both signals.

The shim takes `SPACE_METRICS_SINK` from the same telemetry mode as the Rust
workloads: `datadog` keeps DogStatsD, `otel` selects OTLP, and `journald` maps to
`off` because the Go shim has no journald sink and rejects that value. Left
unset the shim would default to `auto`, meaning native Datadog, and would keep
emitting DogStatsD metrics through an otel comparison.
Re-run `install-shim.sh` if the Service is recreated with a different ClusterIP.
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
do not override an application's explicit environment/version.

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

Once the script returns, a tile of background `kubectl port-forward`s exposes:

| Service     | URL                       |
|-------------|---------------------------|
| API         | http://localhost:8080     |
| Data-plane  | http://localhost:8090     |
| UI          | http://localhost:3000     |
| Object store (S3) | http://localhost:8333 |
| Fake Intake (when enabled) | http://localhost:8088 |

Log in as `root@local` with the initial password (`password` by default). Set `SPACE_CONSOLE_PASSWORD` before running bootstrap or Tilt to choose a custom password. Neither path prints or retains it in a Kubernetes Secret. It is set only while the seeded account is locked; rerunning does not reset an existing password.

### Recovering from an unbundled migration version

On a long-lived task-owned bench, `space-api` may refuse to start because the
database contains an applied migration version that the current binary does not
bundle. This can happen when a branch applies a queued migration before its
timestamp changes on merge. No later build can satisfy the orphaned version.

After confirming `kind-space-local` is the intended disposable cluster, reset
the local control-plane database and republish `seed:latest`:

```bash
kubectl delete statefulset space-postgres -n space --context kind-space-local
kubectl delete pvc data-space-postgres-0 -n space --context kind-space-local
kubectl delete job space-seed-publish -n space --context kind-space-local
tilt trigger space-postgres
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
│   ├── containerd-shim-space-v2        (installed into containerd by install-shim.sh)
│   ├── space-node-stack DaemonSet pod (hostNetwork, host_ip:9800, label component=node-api)
│   │   ├── space-runtime-supervisor-svc                 (root)
│   │   ├── space-nodemgr-svc            → Firecracker   (root)
│   │   ├── space-rtmgr-svc              (read-only list) (space-rtmgr)
│   │   ├── space-volmgr-svc             → object store  (space-sbxmgr)
│   │   ├── space-credmgr-svc                            (space-sbxmgr)
│   │   ├── space-tmplmgr-svc                            (space-tmplmgr)
│   │   ├── space-sbxmgr-svc                             (space-sbxmgr)
│   │   ├── space-nodectl-svc            → internal-api  (space-nodectl)
│   │   └── space-node-api-svc  (worker role, :9800)     (space-nodeapi)
│   └── per-sandbox pods (pause:3.10 hijacked into Firecracker microVMs;
│       sbxmgr launches each sandbox's space-netgtw-svc through nodemgr)
├── worker2 [pool=sandbox, space-enabled=true]
│   └── identical to worker, second DaemonSet pod
├── worker3 [pool=infra,    space.pplx.net/infra=true]
│   ├── space-postgres        (StatefulSet)
│   ├── space-object-store    (versitygw S3 gateway + bucket-init Job)
│   ├── space-api             (Deployment; SPACE_K8S_CONTROL_PLANE=true)
│   ├── space-internal-api    (Deployment; ClusterApi — node registration + template releases)
│   ├── space-data-plane      (Deployment)
│   ├── space-cluster-controller (Deployment; materializes per-sandbox pods)
│   ├── space-seed-publish    (Job; publishes the locally built seed)
│   └── space-ui              (Deployment)
├── worker4 [pool=builder]
│   └── space-builder-node-stack DaemonSet pod (hostNetwork, label space-builder)
│       ├── runtime/node/volume/credential/template/sandbox managers
│       ├── space-nodectl-svc            (builder role → internal-api)
│       ├── space-buildmgr-svc            (recipe VM + publish → object-store templates bucket)
│       └── space-node-api-svc            (builder role, :9800)
│           └── Service space-builder-node-api:9800 (stable API dispatch target)
└── worker5 [pool=builder]
    └── identical to worker4, second builder DaemonSet pod
```

Sandbox-scoped traffic (`CreateSandbox`, `Exec`, `SpacedWrite`, …) flows
api-server → per-sandbox pod IP at `:9800`. Node-scoped traffic (`NodeInfo`,
`ListNodeTemplates`, network ops) flows api-server → each DaemonSet pod's
`host_ip:9800`.

## How the node registers itself

The node-stack chart passes `SPACE_NODE_ID` as the raw
kube node name (fieldRef `spec.nodeName`) plus its worker or builder
`SPACE_NODE_ROLE` and `SPACE_INTERNAL_API_ENDPOINT=http://space-internal-api:9800`.
node-ctl's
outbound reconcilers call `ClusterApi.PublishNodeState`, and the internal
API derives the node UUID from `(node name, role)` via `space_k8s_node_id`
and upserts `nodes` / `node_k8s` / `node_status_snapshots` /
`node_template_inventory`. The cluster-controller's `kube_node_sync`
reconciler covers kube-Node inventory. **Nothing writes Postgres from the
node**; node status uses the normal 30-second freshness window.

(The `space-local-3486` DirectSubstrate bench is the one exception: that
substrate schedules off `node_direct` rows and has no registration RPC, so
the chart's `directSubstrate.enabled` opt-in re-enables the legacy SQL
registration in the entrypoint. Never enabled on this stack.)

## Service identities

The node-stack uses `docker/Dockerfile.worker` plus the Kind entrypoint, which
starts services under the Unix accounts and supplementary groups declared in
the systemd units and EKS charts
(`infra/space/identity/node-identities.sh`): sbxmgr/credmgr as
`space-sbxmgr`, tmplmgr as `space-tmplmgr`, node-ctl as `space-nodectl`,
node-api as `space-nodeapi`, the runtime manager as `space-rtmgr`;
runtime-supervisor, nodemgr, volmgr, and the
builder's build-manager stay root. The per-sandbox
net-gateway is launched through nodemgr with that sandbox's runtime identity.
Directory ownership matches
packer `07-sysctl.sh` and the DaemonSet init containers, so **UDS peer authorization
(SO_PEERCRED) is live in this harness** — e.g. warm-pool fills are
authorized as `Service(space-tmplmgr)`, not silently as root.

The runtime manager only lists the node's Firecracker releases and kernels
(`space-rtmgr list`); nothing depends on it yet. The entrypoint restarts it on
its own rather than through the service-set supervisor, so its exit never
restarts the other services. The baseline Kind smoke checks its JSON listing on
both a worker and a builder, requiring a complete scan with a valid vendor
runtime and a staged cache kernel. These checks do not gate node readiness.

Kind explicitly sets `SPACE_INTERNAL_RPC_INSECURE_DEV=true` for its internal
ClusterApi/NodeApi HTTP connections. Deployed services default to authenticated
TLS; see [Internal RPC TLS](../../../docs/internal-rpc-tls.md) for certificate
configuration and the coordinated migration requirements.

## Differences from the EKS deployment modules

| EKS modules                                               | Local kind                                                                    |
|-----------------------------------------------------------|-------------------------------------------------------------------------------|
| One DaemonSet per node service, systemd watchdogs via k8s probes | All node services in one `space-node-stack` pod, supervised by the entrypoint (same binaries, identities, sockets) |
| `/space/cache` is btrfs on instance-store NVMe            | btrfs loopback image, bound from the kind node at `/var/lib/space-cache.img` so it outlives pod restarts like the NVMe does (same code paths, less I/O headroom) |
| Postgres on RDS, secrets via ExternalSecrets / AWS Secrets Manager | In-cluster `space-postgres` StatefulSet, static Kubernetes Secrets             |
| S3 for snapshots + template artifacts                     | In-cluster versitygw (real S3 protocol, sigv4, path-style)                    |
| volume-manager credentials via credmgr STS/IRSA           | static `SPACE_OBJECT_STORE_STATIC_*` creds (the documented local escape hatch) |
| build-manager gets S3 credentials via credential-manager backed by cloud identity | the same directly injected static object-store credentials; credential-manager's storage exchange endpoint is explicitly disabled |
| `/dev/kvm` access via the AMI's kvm group                 | `chmod 0666 /dev/kvm` at pod start (kind-only; never replicate on a worker)   |
| Loop devices from the AMI's `max_loop=10000` on a real devtmpfs | 8 nodes inherited in the pod's tmpfs `/dev`; the entrypoint mknods the rest and sets autoclear on the cache device (`scripts/lib/space-loop-devices.sh`) |
| vhost-vsock jail nodes provided by the AMI                | jail watcher in the entrypoint mknods them as jails appear                    |
| Sandbox SSH key generated by Packer per AMI               | generated per host by `build-seed.sh` under `~/.space-devbox/kind/ssh` |
| ALB ingress in front of api / data-plane / ui             | NodePort + `kubectl port-forward` (started in background by bootstrap.sh)     |
| The runtime manager uses writable `/space/vendor/{firecracker,kernels}` and `/space/vendor/runtime-manager/state` | `/space/vendor` is a read-only host bind: install roots report unprotected and state absent; listing and cache checks still work |
| Builder services use separate DaemonSets | The same service set shares a dedicated builder node-stack pod and a stable `space-builder-node-api` Service |
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
| `space-local-worker`          | `sandbox`   | Runs space-node-stack DaemonSet + receives SPACE sandbox pods                            |
| `space-local-worker2`         | `sandbox`   | Same, second worker                                                                      |
| `space-local-worker3`         | `infra`     | Runs Postgres, object store, space-api, internal-api, data-plane, cluster-controller, UI    |
| `space-local-worker4`         | `builder`   | Runs the builder node-stack, including build-manager and builder node-api                 |
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
#    (which is finicky behind kubectl port-forward) and reach the
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
#    AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN in private.env. The selected
#    identity must have secretsmanager:GetSecretValue on the task-owned ARN.
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
kubectl --context kind-space-local -n space get ds,po,svc -o wide
kubectl --context kind-space-local -n space describe ds/space-builder-node-stack
kubectl --context kind-space-local -n space logs ds/space-builder-node-stack --tail=200

BUILDER_POD=$(kubectl --context kind-space-local -n space get pod \
  -l 'app.kubernetes.io/name=space-builder' -o jsonpath='{.items[0].metadata.name}')
kubectl --context kind-space-local -n space exec "$BUILDER_POD" -- \
  tail -n 200 /space/logs/buildmgr.log
kubectl --context kind-space-local -n space exec "$BUILDER_POD" -- \
  tail -n 200 /space/logs/node-api.log

aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://templates/ --recursive
```

The builder DaemonSet is Ready only when both node-api `:9800` and
build-manager `:9900` pass gRPC health checks. A Pending pod usually means the
`pool=builder` node or `/dev/kvm` mount is missing; a running but unready pod
should be diagnosed from `buildmgr.log`, `tmplmgr.log`, and `node-api.log`.
Service logs survive container retries within the same pod: previous attempts
are kept under `/space/logs/previous-000001/`, `previous-000002/`, and so on.
Pod replacement starts a fresh log volume. PID files remain container-local
under `/run/space-node-stack`; they must not survive a container retry.

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
  root propagates that mount into the jail; in a pod the default-private
  mount tree doesn't, and FC opens the 0-byte bind target ("Total sum of
  memory regions exceeds largest possible file offset"). The entrypoint
  runs `mount --make-rshared /` to restore host semantics.
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

## Real S3 snapshot + template path (object store + node-stack)

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
