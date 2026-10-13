# Opt-in SPACE observability

These Helm charts support private, single-replica canaries on Kubernetes.
Operators choose the target environment and integrate each release with their
deployment process. Installing a backend does **not** select a SPACE sink or
change existing telemetry routing. Shared deployment requires a separately
approved operator action.

The guarded Kind workflows remain available for
[metrics](../kind/metrics/README.md), [logs](../kind/logs/README.md),
[traces](../kind/traces/README.md) and
[Grafana](../kind/grafana/README.md).
`profiles/kind.yaml` is **only** for verified local Kind: it permits disposable
storage and disables network policies. Do not use that profile elsewhere.

## Releases and ownership

Install each chosen chart in its **own dedicated namespace**. Resource names
are intentionally stable for existing Kind releases and PVCs. One release per
namespace is supported; do not put metrics and traces Collectors together.
Charts do not create namespaces, cluster-wide RBAC, ingress, external load
balancers, or producer workloads in the shared profile.

`charts/common` is a Helm library chart, not an installable release. It
holds the contract these charts share: the profile and shared-cluster guards,
the data claim, placement and container hardening, the DNS egress rule, and the
Collector digest that metrics and traces both pin. Vendor it before any lint,
render or install:

```bash
helm dependency build infra/space/observability/charts/<chart>
```

| Chart | Contents | Private endpoints | Storage / retention defaults |
| --- | --- | --- | --- |
| `charts/metrics` | Collector Contrib → Prometheus | Collector `:4317` OTLP/gRPC; Prometheus `:9090` queries | 2 Gi RWO; 6 h / 512 MB TSDB |
| `charts/logs` | Alloy DaemonSet → Loki | Loki `:3100` push and queries | 2 Gi RWO; 24 h retention |
| `charts/traces` | Collector Contrib → Tempo | Collector `:4317` OTLP/gRPC; Tempo `:3200` queries | 4 Gi RWO; 6 h block retention |
| `charts/grafana` | Grafana, datasources, dashboards | UI `:3000` | None by default; optional 1 Gi RWO |

The visualization release installs no backend and routes no producer: enable
only the datasources you have already installed, and give each its explicit
URL. [Shared-cluster readiness](shared-cluster-readiness.md) lists what a real
canary in an operator-selected environment still requires beyond these charts.

Start with a non-production canary; this is not an HA or production-ready
replacement for Datadog. Pinned image tags and digests, resource requests and
limits, bounded queues, and readiness probes are in each chart. Read its
`values.yaml` and `profiles/shared.example.yaml` before deploying.

## Shared-cluster preflight

1. Select an exact kube context from your deployment inventory and confirm the
   account, cluster and namespace with the cluster owner. Do not infer these
   from the active context or copy a Kind namespace into a shared workload.
2. Reserve dedicated namespaces through the cluster's normal ownership process.
   Set namespace policy/quota and labels there; the charts do not own them.
3. Review node capacity (including reservations, existing DaemonSets, taints and
   disruption headroom). Configure `scheduling.nodeSelector` and `tolerations`
   for a suitable pool. Resource defaults are unchanged from the local stack;
   they are **not** sized for whole-cluster ingestion. Inspect pending pods and
   pressure rather than raising limits without a capacity check.
4. Choose an explicitly named RWO storage class, or a pre-provisioned
   `storage.existingClaim`. Verify its zone/scheduling constraints, permissions,
   expansion/reclaim policy, and sufficient capacity. New shared PVCs have
   Helm's `keep` annotation. An existing claim is never owned by the chart.
5. Verify that the cluster CNI actually enforces NetworkPolicy. The shared
   profile requires policy generation, but rendering a policy does not prove
   enforcement. Test both an allowed and a denied producer before routing data.
6. Copy the chart's shared example to a reviewed per-cluster values file. Set
   storage and explicit producer/query peers. Default external access is empty
   (deny). Namespace + pod selectors in the **same peer** are an AND; separate
   peers are OR. Example, after verifying actual labels:

   ```yaml
   networkPolicy:
     receiverPeers:
       - namespaceSelector:
           matchLabels: {kubernetes.io/metadata.name: approved-canary-namespace}
         podSelector:
           matchLabels: {observability-canary: "true"}
   ```

   Internal component traffic and kube-system CoreDNS (`k8s-app=kube-dns`,
   TCP/UDP 53) are allowed. NodeLocal DNS or different DNS labels require a
   reviewed `networkPolicy.additionalEgress` rule. Producer egress policies also
   need to allow the destination. Do not open the receiver to the entire VPC.
   SPACE node-api uses host networking: peer matching for host-networked pods
   varies by CNI. Begin with an ordinary pod canary; separately prove host-path
   connectivity/isolation before adding node-api or guest/worker producers.
   Readiness and liveness probes originate from the node, not from a pod. Most
   CNIs exempt that traffic, but confirm that pods stay ready after the policies
   are applied rather than assuming it.

NetworkPolicy is **not authentication or encryption**. Metrics currently uses
plaintext OTLP/gRPC and rejects nonempty authentication headers. These receivers
must stay private to a trusted network; production transport/authentication and
multi-tenancy need a separate design. Query peers on a backend HTTP port may
also reach its write API: this is not a read-only security boundary.

## Render, review, then install explicitly

Example for metrics; use an independently reviewed file and namespace for each
additional release. These commands are instructions for an authorized operator,
not evidence that a shared cluster has been tested. Do not store credentials in
values files, rendered manifests, or source control.

```bash
obs_context='<verified-context>'
obs_namespace='<dedicated-metrics-namespace>'
obs_values='/absolute/path/to/reviewed-metrics-values.yaml'
obs_chart=infra/space/observability/charts/metrics
obs_release=space-metrics

kubectl --context "$obs_context" cluster-info
kubectl --context "$obs_context" get namespace "$obs_namespace"
kubectl --context "$obs_context" get storageclass
helm dependency build "$obs_chart"
helm lint "$obs_chart" --namespace "$obs_namespace" -f "$obs_values"
helm template "$obs_release" "$obs_chart" --namespace "$obs_namespace" -f "$obs_values"
helm template "$obs_release" "$obs_chart" --namespace "$obs_namespace" -f "$obs_values" |
  kubectl --context "$obs_context" --namespace "$obs_namespace" apply --dry-run=server -f -

# Only after owner review of the rendered diff and target:
helm upgrade --install "$obs_release" "$obs_chart" --kube-context "$obs_context" \
  --namespace "$obs_namespace" -f "$obs_values" --wait --timeout 5m
kubectl --context "$obs_context" --namespace "$obs_namespace" get pods,pvc,networkpolicy
```

Use a complete reviewed values file on upgrades, not stale `--reuse-values`
from another cluster. No namespace creation or broad cluster bootstrap is
hidden in this workflow. For an existing 0.1 Kind release, the local helper
loads the Kind overlay and can reset defaults while retaining supplied values.

## Metrics canary and verification

Keep **one Collector replica**: `delta_to_cumulative` is stateful and alpha in
the pinned Collector release.
It retains up to 10,000 streams for 2 minutes of inactivity. Restart/eviction
loses accumulated state; the next samples restart cumulative counters and
histograms. There is no persistent queue/WAL for the Collector. Its queue is
128 requests, RPC timeout 3 s and retry window 30 s. Bounded outages can lose
data; monitor rejected/dropped samples, queue use, memory and resets. Keep its
256 MiB memory limiter, 205 MiB Go limit and container limit coherent when
resizing. Do not horizontally scale without stream affinity and a state design.

Prometheus explicitly enables its OTLP receiver. Translation remains
`UnderscoreEscapingWithSuffixes`: dots become underscores and Prometheus adds
unit/type suffixes (for example `space_node_mgr_loop_devices_created_total` and
`space_sandbox_state_convergence_duration_ms_milliseconds`). Application names
do not change. Preserve exponential histograms as native
histograms; do not flatten them. The exact resource promotion, identity,
10 s export cadence, gauge staleness and histogram/query limitations are in
[the metrics integration workflow](../../../scripts/tests/observability/metrics/README.md).
Inspect `config/prometheus.yaml` for the promotion allowlist before comparing
queries with Datadog.

After backend health and policy isolation checks, a **separate, scoped** SPACE
deployment change may set `SPACE_METRICS_SINK=otlp` and the metrics-specific
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://collector.<namespace>.svc.cluster.local:4317`
for an approved canary only. Verify the image includes the required metrics
implementation (including filesystem-worker support for that path). Preserve
all generic OTEL settings and native defaults elsewhere. Sinks are mutually
exclusive: the canary's metrics stop going to native Datadog, although the
Datadog Agent remains installed and logs/traces are unaffected. Do not imply
dual-write coverage. Unset/auto retains the native default.

Query known increments, rates/reset boundaries, positive/zero/retired gauges,
histogram count/sum/quantiles and producer labels. Run traffic, idle/recovery,
then a bounded Collector restart/outage. Observe the actual shared backend;
Kind evidence alone cannot prove CNI, storage, host-path coverage or query
compatibility there. Do not enable broadly until the cluster owner accepts the
loss/reset semantics and the affected dashboards/monitors have been reviewed.

Private query access (loopback only):

```bash
kubectl --context "$obs_context" --namespace "$obs_namespace" \
  port-forward --address 127.0.0.1 service/prometheus 19090:9090
curl --get http://127.0.0.1:19090/api/v1/query --data-urlencode 'query=up'
```

## Logs canary and verification

Logs collection is a **node-level DaemonSet**, so its blast radius differs from
the metrics backend. Alloy runs as UID 0 with read-only host mounts of the CRI
log directory and the host journal; it needs no Kubernetes API credentials or
RBAC. Review this against the cluster's pod security admission and any existing
node agent before installing. Two collectors reading the same files do not
corrupt them, but they do duplicate ingestion cost; Datadog's agent is
unaffected and keeps collecting.

Set these for the target cluster, after verifying each on a real node:

- `clusterLabel`: the cluster name existing queries expect. It is the fallback
  stream label when a record carries no `cluster_id`; the shared profile
  requires it so records are attributable.
- `collection.podTargets`: exact CRI globs. Kubernetes names these directories
  `<namespace>_<pod>_<uid>`, so the default `space_*` glob assumes SPACE runs in
  a namespace named `space`. Confirm the real names before relying on the
  `namespace` label.
- `collection.excludedPodSources`: literal namespace, pod-name prefix, and
  container identities removed from every matched file target before tailing.
  The default excludes `build-manager` in `space-builder-build-manager-*` pods
  in namespace `space`, including rotated log filenames and recreated pod UIDs.
  For a builder release named `build-farm` in `builds`, set
  `{namespace: builds, podNamePrefix: build-farm-build-manager-, container: build-manager}`.
  Lists replace defaults: include every builder release, including old names
  whose files remain on nodes after a rename. Worker containers remain
  eligible.
  Alloy reads no Kubernetes metadata and does not honor Datadog exclusion
  annotations. Audit source eligibility before deployment: shared deployments
  with differently named builder-service pods need their actual pod/container
  identities, not the local Kind example. This is source exclusion, not message
  redaction; other collected sources must also be safe to retain.
  Updating exclusions does not remove records already stored in Loki.
- `collection.journal.path`: Kind keeps a **volatile** journal at
  `/run/log/journal`; persistent hosts use `/var/log/journal`. The shared
  profile rejects the Kind path. The `hostPath` type is `Directory`, so pods
  stay pending on nodes without that directory. Set
  `collection.journal.enabled=false` where the host has no persistent journal.
- `collection.journal.keepRegex`: the keep filter over
  `__journal__systemd_unit;__journal_syslog_identifier`. Widening it collects
  unrelated host units; verify the match before changing it.
- `collection.journal.dropRegex`: optional deny filter over the same
  `unit;identifier` pair, applied after the keep filter and before forwarding.
  Kind builder services are tailed into pod stdout, not host systemd units.
  Before collecting a shared host journal, inventory builder services and
  configure their unit/identifier exclusions (or disable journal collection).
  Pod exclusions do not exclude copies forwarded by another source.
- `alloy.nodeSelector` and `alloy.tolerations`: a DaemonSet reaches tainted
  nodes only with matching tolerations. Nodes it never schedules on are
  silently uncollected, so compare the DaemonSet's desired count with the node
  count instead of assuming full coverage.
- `alloy.statePath`: node-local read cursors. They survive reinstalls and
  prevent full replays. A cursor is **not** a delivery acknowledgement.

Loki runs single-replica with filesystem TSDB v13 and `auth_enabled: false`:
any peer allowed by policy can read and write every tenant's records. It is not
multi-tenant, replicated, or backed up. A PVC request is not a hard disk quota,
and retention does not evict on disk pressure; watch node and volume use.
Ingestion rate, stream count, line size, and query limits are explicit in
`config/loki.yaml`. Raise `loki.retentionPeriod` only with retention and privacy
approval, because logs carry more sensitive content than metric names do.

Delivery is bounded, not guaranteed: a 256 KiB batch, 5 s timeout, five
retries, no durable queue or WAL. Exhausted retries, long outages, rotation,
node loss, line limits, and Loki rejection lose records; Loki rejects samples
older than its retention period, and journal cold start backfills one hour.
`validationEcho` is Kind-only and is rejected by the shared profile.

Verify with real service output rather than a synthetic writer: confirm the
DaemonSet is ready on every intended node, query a known service through
`/loki/api/v1/query_range`, check `/labels` and `/series` for exactly the six
indexed labels, and confirm structured metadata (filename, stream, unit) is not
indexed. Then restart the DaemonSet under load and compare counts, and stop
Loki briefly to observe the retry and cursor behaviour. Collection alone does
not establish which formatter an image contains: record the source and image
IDs for the run. Datadog reserved-field remapping is not exercised by this
stack and still needs its own pipeline canary.

Private query access (loopback only):

```bash
kubectl --context "$obs_context" --namespace "$obs_namespace" \
  port-forward --address 127.0.0.1 service/loki 13101:3100
curl -fsSG http://127.0.0.1:13101/loki/api/v1/query_range \
  --data-urlencode 'query={namespace="space"} | json | __error__=""' \
  --data-urlencode 'since=15m' --data-urlencode 'limit=20'
```

This stack collects existing stdout and journal output. It adds no application
OTel log exporter and no trace or span correlation; those remain out of scope.

## Traces canary and verification

Tempo runs monolithic (`-target=all`) on one replica with local block storage
and no object store, Kafka, or authentication. Any peer allowed by policy can
read every trace. Traces carry request-shaped detail, so review data policy
before pointing a real service at it.

Volume is the main sizing risk. Tempo's defaults here allow 2 MiB/s ingestion,
a 4 MiB burst, 1,000 live traces and 1 MiB per trace; a production service can
exceed those immediately. Raise `storage.size` and the limits in
`config/tempo.yaml` together, and set `tempo.blockRetention` and
`tempo.maxSearchDuration` from measured volume rather than copying the local
6 h defaults. Block retention is a cleanup sweep, not a precise per-span
expiry, and a PVC request is not a hard disk quota.

The Collector's queue is in memory: 128 requests, one consumer, 3 s RPC
timeout, and a 30 s retry budget. Process death or an outage longer than that
budget loses spans. Watch rejected and failed spans and queue occupancy on the
Collector's port 8888; this chart does not scrape them, so point an existing
metrics pipeline at that port if you need the signal. Do not scale the
Collector or Tempo horizontally without a state and routing design.

Tempo 3's `query_frontend.query_end_cutoff` excludes the newest 30 seconds even
with explicit end bounds, so freshly exported traces are briefly unsearchable.
Use a wide window and bounded polling; export acknowledgement is not evidence
that a trace is queryable.

After backend health and policy isolation checks, a **separate, scoped**
deployment change may point an approved canary at
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://collector.<namespace>.svc.cluster.local:4317`.
Native producers additionally need an explicitly reviewed
`SPACE_TRACE_EXPORTER=otlp`. Capture and restore the exact prior settings; do
not change the general OTLP endpoint or the metrics and logs routing along with
it. Partial per-service routing produces broken traces whose parent or root is
stored in the other backend, so move whole trace paths rather than single
services.

The Collector passes spans through without transformation: OTLP names are
preserved and `operation.name` stays a separate attribute. Datadog APM
operation and resource mapping and sampling behaviour are separate
compatibility questions that this stack does not answer.

Private query access (loopback only):

```bash
kubectl --context "$obs_context" --namespace "$obs_namespace" \
  port-forward --address 127.0.0.1 service/tempo 13200:3200
curl -fsSG http://127.0.0.1:13200/api/search \
  --data-urlencode 'q={resource.service.name="space-api"}' \
  --data-urlencode "start=$(date -d '1 hour ago' +%s)" \
  --data-urlencode "end=$(date +%s)" --data-urlencode limit=20
```

## Visualization

Grafana is optional and independent: it owns the UI, datasource provisioning
and dashboards, and nothing else. Install it in its own namespace after the
backends it should read.

The Kind overlay runs Grafana with **anonymous Viewer access and no login
form**, which the shared profile rejects. A shared install requires
`auth.existingSecret` naming an existing Secret in the release namespace with
`admin-user` and `admin-password` keys. Create and rotate that Secret through
the cluster's normal secret process; never place credentials in a values file,
a rendered manifest, or `--set`. Local admin credentials are a starting point,
not an SSO or RBAC design, and the chart adds no Ingress, NodePort, or load
balancer.

Set each datasource's `url` to the namespace where you installed that backend;
the Kind profile supplies local URLs; shared defaults are empty, and the chart fails rather than
provisioning an enabled datasource with an empty URL. Datasource UIDs stay
`space-prometheus`, `space-loki` and `space-tempo` so dashboards
keep working. Only enabled sources' dashboards are mounted, and provisioned
dashboards are read-only. Additional dashboard JSON can be supplied through
operator-owned ConfigMaps listed in `dashboards.extraConfigMaps`; those ConfigMaps
are managed independently of this chart. Synthetic validation dashboards live
under `scripts/tests/observability/grafana/` and are never packaged here.

`persistence.enabled` is false by default, which means an `emptyDir`: accounts,
sessions and annotations are lost on every restart, while provisioned
dashboards and datasources are rebuilt from the chart. Enable persistence with
a reviewed storage class where user state must survive.

Grafana reaches backends in other namespaces, so its egress needs explicit
rules. Supply one `networkPolicy.backendEgress` rule per backend and the peers
allowed to reach the UI in `networkPolicy.uiPeers`; DNS is already allowed.
Each backend's own chart must also list Grafana in its `queryPeers`, or the
connection is denied at the receiving end. Remember that a backend's HTTP query
port usually also serves its write API, so a query peer is not a read-only
boundary.

```yaml
networkPolicy:
  uiPeers:
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: approved-ui-namespace}}
      podSelector: {matchLabels: {app: ingress-nginx}}
  backendEgress:
    - to:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: metrics-namespace}}
          podSelector: {matchLabels: {app: prometheus}}
      ports: [{protocol: TCP, port: 9090}]
```

Private UI access (loopback only):

```bash
kubectl --context "$obs_context" --namespace "$obs_namespace" \
  port-forward --address 127.0.0.1 service/grafana 13000:3000
```

## Rollback and remaining production prerequisites

Roll back the **producer routing first** to its exact previous settings/image,
then verify native delivery and normal application operation. Removing a
backend while producers still point at it causes export loss. Keep Datadog and
unrelated telemetry configuration intact. For a backend configuration rollback,
inspect `helm history` and use `helm rollback` with explicit release, revision,
namespace and `--kube-context` after the owner's approval. Stateful data formats
may not support arbitrary image downgrades.

To retire a shared release, obtain approval for the exact namespace/release,
blast radius and data disposition, then uninstall it with explicit context.
Do not delete the namespace or PVCs as an implicit cleanup step. Shared PVCs
are retained; data/positions need a separate reviewed deletion/retention plan.
Local fixture cleanup remains in the guarded Kind helpers.

Still required for production: capacity/load sizing and quotas, durable storage
and backup/restore, HA/state ownership, stronger authentication/TLS, tenant and
data-access boundaries, retention/privacy approval, enforced network isolation,
host/guest topology validation, outage/loss SLOs, ownership/alerts and a measured
canary rollback. No production-readiness or Datadog-parity claim follows from
successful ingestion.

References: [Kubernetes NetworkPolicy](https://kubernetes.io/docs/concepts/services-networking/network-policies/),
[Collector delta conversion](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.161.0/processor/deltatocumulativeprocessor),
[Prometheus OTLP](https://prometheus.io/docs/guides/opentelemetry/),
[Alloy journal source](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.journal/),
[Loki retention](https://grafana.com/docs/loki/latest/operations/storage/retention/),
[Tempo configuration](https://grafana.com/docs/tempo/latest/configuration/),
[Tempo query API](https://grafana.com/docs/tempo/latest/api_docs/).
