# Metrics integration validation

These checks exercise the existing Rust metric helpers and OTLP exporter against
Collector and Prometheus. Install the backend with the
[Kind deployment helper](../../../../infra/space/kind/metrics/README.md).
The fixture chart here is rendered into a standalone Job; it is never a dependency
of the deployable metrics chart or part of its Helm release.

## Run

From the repository root, with Python 3, the Rust toolchain and the verified
Kind bench available:

```bash
cargo build --locked -p pplx-space-metrics --example metrics_workload \
  --features pplx-space-observability/otlp-metrics
docker build -t space-oss-workload:local \
  -f scripts/tests/observability/metrics/workload.Dockerfile target/debug/examples
bash infra/space/kind/metrics/local.sh status
kind load docker-image --name space-local space-oss-workload:local
bash scripts/tests/observability/metrics/run.sh review01 \
  space-oss-workload:local target/oss-review01 --faults
```

Choose a new run and output path each time. `run.sh` verifies the cluster and
namespace through the deployment helper, creates only `workload-RUN`, captures
raw evidence, checks stored records and deletes its Job on exit. It does not
install or upgrade the backend release. `--faults` opts into bounded restarts
and outages of the owned Collector and Prometheus Deployments; capture restores
both to one replica on interruption. Never run two fault scenarios concurrently.

The image runtime must match the build host's glibc. The pinned Debian 13 runtime
matches the reference devbox; otherwise build in a compatible builder or supply
a pinned `WORKLOAD_RUNTIME_IMAGE`. Record source and binary/image digests with
shared evidence. The producer contains only synthetic identities and calls the
same public metric helpers as SPACE services.

To reassess saved evidence before the six-hour retention expires:

```bash
python3 scripts/tests/observability/metrics/assess.py target/oss-review01
```

## Workload and faults

Two independent processes, same service and product labels, distinct
`service.instance.id = HOSTNAME:pid`. Each runs **144 ticks / 720 seconds**.
Every active tick calls loop-device counters (kernel increment below; device
increment twice that), one `sandbox_created(false)`, timing samples `[1,2,4,8]`
ms and the state-like guest-vCPU distribution `[2,4,8]`.

| Seconds | Kernel increment / 5s | Gauge |
| --- | ---: | --- |
| 0–55 | 2 | 7 |
| 60–115 | 2 | 0 (explicit measurement) |
| 120–175 | none (idle) | no measurement |
| 180–355 | 5 | no measurement |
| 360–475 | 1 | no measurement |
| 480–595 | 3 | no measurement |
| 600–715 | 3 | 11 |

Per producer with no loss: kernel/device counters **396 / 792**, creation counter
**132**, timing count/sum **528 / 1980 ms**, vCPU count/sum **396 / 1848**.
The shutdown flush follows the normal ten-second exporter interval; no faster
test-only application interval is introduced.

With `--faults`: restart Collector at ~210s; scale Prometheus to zero at ~350s
and restore at ~395s; stop the Collector OTLP receiver at ~470s and restore at
~505s. Actual events/readiness timestamps, not these targets, define outage
duration. Only task-owned Deployments are touched. Queued HTTP requests have a
30s retry budget; a 45s Prometheus outage exercises retry exhaustion. A later
cumulative sample can preserve totals even when an intermediate export was
dropped. Collector/gRPC unavailability can lose SPACE delta windows permanently.
Report both cases, without interpreting an extrapolated `increase()` as an
exact audit ledger. The emission loop should continue throughout.

## Configuration and semantics

- Images are version **and digest** pinned: Collector Contrib **0.161.0**,
  Prometheus **3.14.0**. See the deployable metrics chart's `values.yaml`.
- Collector's `delta_to_cumulative` is **alpha**. One replica, Recreate strategy,
  **10,000 streams**, **2m stale eviction**. State is memory-only: restart,
  eviction or load-balanced replicas can introduce counter/histogram resets.
  Never horizontally scale this configuration without stream affinity/design.
- Memory limiter first: 256MiB heap threshold, 64MiB spike allowance, 1s checks;
  GOMEMLIMIT 205MiB; 512MiB Pod limit. Maximum receive message 4MiB. Export batch
  maximum 512 points, 1s flush; queue 128 requests, one consumer. RPC timeout 3s,
  retries initially 1s, nominal maximum interval 5s (jitter can exceed it), elapsed
  retry limit 30s. No durable queue.
  These are bounds/configuration, not an overload-capacity guarantee. A rejection
  by the memory/stream limiter can lose deltas: SPACE does not replay them.
- Prometheus explicitly enables `--web.enable-otlp-receiver`, at
  `/api/v1/otlp/v1/metrics`. Native histograms work in this pinned version without
  an experimental flag; Prometheus-side delta conversion is **not** enabled.
  One-minute out-of-order acceptance accommodates bounded delayed exports.
- Prometheus has a **2Gi local-path PVC**, **6h / 512MiB retention**, and 1Gi Pod
  memory limit. Retention size limits blocks, not total WAL/head/disk usage; the
  PVC is the local storage budget. No cloud backup.
- All Services are ClusterIP: no Ingress, NodePort, hostPort, or external
  receiver. UI tunnels bind **127.0.0.1** only.
  This is local-cluster privacy, **not tenant isolation**: other pods can reach
  these Services, and stock Kind networking does not enforce NetworkPolicies.
  Do not send customer metadata or use this unauthenticated profile remotely.

SPACE settings used only on fixture Pods:
`SPACE_METRICS_SINK=otlp`, metrics-specific
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://collector:4317`, protocol `grpc`.
No general OTLP endpoint or trace/log selector is changed. No auth headers.
`DD_SERVICE` supplies the existing `service` point attribute; `DD_ENV` and
`DD_VERSION` supply resource environment/version even in an OTLP-only build.
The fixture was corrected after a short preflight revealed that omitting
`DD_SERVICE` supplied resource `service.name` but no `service` point label.

### Names, labels and units actually observed

The explicit strategy is **UnderscoreEscapingWithSuffixes**. Dots become
underscores; conventional type/unit suffixes appear **only at Prometheus**.
No application names or counter `.count` / `.count_v2` suffixes are introduced.

| Original SPACE name | Prometheus name | Unit / value interpretation |
| --- | --- | --- |
| `space.node_mgr.loop_devices.created` | `space_node_mgr_loop_devices_created_total` | dimensionless counter |
| `space.sandbox.api.created` | `space_sandbox_api_created_total` | dimensionless counter |
| `space.node_mgr.cleanup_operation.in_flight` | `space_node_mgr_cleanup_operation_in_flight` | absolute count; no unit, so no suffix |
| `space.sandbox.state_convergence.duration_ms` | `space_sandbox_state_convergence_duration_milliseconds` | milliseconds; **not converted to seconds** |
| `space.sandbox.guest.vcpu_count` | `space_sandbox_guest_vcpu_count` | dimensionless sample distribution, **not a gauge** |

Resource promotion is an allowlist: `service.name`, `service.instance.id`,
`service.version`, `deployment.environment.name`, `k8s.cluster.name`,
`k8s.pod.name`, `k8s.node.name`. Label dots are escaped. `job` and `instance`
also identify service and producer. `service` and `cluster_id` point attributes
remain; `kind`, `state`, `template`, `tls_interception` remain queryable.
Instrumentation scope appears as `otel_scope_name=pplx-space-observability`.
Do not drop instance identity before delta conversion, or distinct writers can
be merged incorrectly. Do not promote all resource attributes without a
cardinality/privacy review. Name/label escaping can collide with existing
underscore names; review the full catalogue before a broader deployment.

### Queries and expected results

Add `{service_version="YOUR_RUN"}` (plus `kind="kernel_device"` on counters) to
limit each expression. Exact executed queries/results are in `queries.json`.
The optional test-only Grafana contract dashboard includes these checks; the capture and assessment scripts query Prometheus directly.

```promql
space_node_mgr_loop_devices_created_total{kind="kernel_device"}
rate(space_node_mgr_loop_devices_created_total{kind="kernel_device"}[1m])
resets(space_node_mgr_loop_devices_created_total[15m])
space_node_mgr_cleanup_operation_in_flight
histogram_count(space_sandbox_state_convergence_duration_milliseconds)
histogram_sum(space_sandbox_state_convergence_duration_milliseconds)
histogram_quantile(0.95, space_sandbox_state_convergence_duration_milliseconds)
histogram_count(space_sandbox_guest_vcpu_count)
histogram_sum(space_sandbox_guest_vcpu_count)
histogram_quantile(0.95, space_sandbox_guest_vcpu_count)
```

Healthy initial counter rate: **0.4/s per producer**. Idle producers may produce
an absent rate instead of zero. Use rates per producer **before** summing.
Timing mean is **3.75 ms**, observed p50 **3.9143 ms**, p95 **7.9654 ms**;
vCPU mean **14/3**, p50 **3.9784**, p95 **7.9870**. For `[1,2,4,8]`, the 50%
rank lies between populated buckets; this is not the raw-sample median of 3 ms
or a promise of nearest-rank 2 ms. Use the backend's quantile semantics.
Histogram quantiles are bucket interpolations, not retained raw observations.
OTLP exponential buckets/count/sum survive as Prometheus native histograms;
SPACE's maximum scale 8 fits Prometheus's supported -4…8 schema range. The SDK
may lower resolution to honor its 160-bucket limit. **Exact OTLP min/max are
lost** in Prometheus's histogram model. Exemplar preservation is not validated.
Integer counters become float64; exact integer precision above 2^53 is not a
safe backend assumption (large-value ingestion not tested here).

SPACE retires a silent gauge at collection, but the last ingested value remains
visible under Prometheus's **5m lookback**. That is stale last-known data, not a
fresh sample. It eventually disappears; neither Collector nor dashboard fills
the silence with zeros. After recovery it returns as 11. Historical data stays
until retention removes it. Tests must distinguish this from immediate removal.


## Evidence and cleanup

Raw samples, producer logs, manifests, fault timestamps and `assessment.json`
remain under the chosen output path after the Job is removed. A passing run
proves this synthetic ingestion path, not full guest/worker delivery, Datadog
parity, network isolation or production sizing.

If the runner is force-killed, verify ownership with `local.sh status`, restore
`deployment/collector` and `deployment/prometheus` to one replica in
`space-metrics`, then delete only your `job/workload-RUN`. Every kubectl
command must name `--context kind-space-local`. Backend retirement is a separate
operation documented in the deployment helper's README.
