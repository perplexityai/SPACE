# Local logs: Alloy → Loki

Standalone logs ingestion, installed automatically by Kind bootstrap and
full-stack Tilt with the explicit `otel` profile. This chart deploys only Alloy and Loki;
neither metrics nor Grafana is required. It neither changes SPACE
images/environment nor removes or reconfigures Datadog. No application
exporter, trace correlation, metrics pipeline, or production integration is added.

## Start and query

Prerequisites: the existing `space-local` Kind cluster, Docker, kubectl, Helm,
and node directories `/var/log/pods` and `/run/log/journal`. Run from this repo:

```bash
bash infra/space/kind/logs/local.sh up
bash infra/space/kind/logs/local.sh status
# Default bootstrap and full-stack Tilt already forward this port in otel mode.
bash infra/space/kind/logs/local.sh ui
```

The helper verifies that context `kind-space-local` addresses the loopback API
port published by this Docker Kind cluster (`kind-$SPACE_KIND_CLUSTER` on a
bench that sets `SPACE_KIND_CLUSTER`). Its deployments and cleanup affect
only the owned namespace `space-logs`. On first install it waits for Loki
before starting collection. It applies the chart's Kind overlay, which permits
disposable storage, the diagnostic echo, and the volatile `/run/log/journal`.
For shared clusters use the portable
[observability charts and deployment runbook](../../observability/README.md),
not this local test harness. Its Kind guard is intentionally unchanged.

An existing release is owned by its namespace label and Helm identity, not by
whether it matches the latest checkout. `up` can therefore update chart
templates and retry a failed release without deleting Loki. Saved overrides,
including the first install's image pins, are retained; pass the current
`profiles/kind.yaml` explicitly with any custom overrides afterward when you
intend to adopt changed profile defaults.

`pause` and `resume` change only the collector and retain recorded Loki,
configuration, policy, and storage resources. Resume uses a matching retained
collector revision; if it cannot find one, `up` reconciles the current chart.
An unchanged healthy release is a no-op. Pending Helm operation locks still
require operator inspection; this helper never clears them by deleting
release history or local data.

Keep `ui` running for the Loki HTTP API at <http://127.0.0.1:13101>.
The `otel` profile also installs the independent `grafana` chart with
the Loki datasource enabled at `http://loki.space-logs.svc.cluster.local:3100`.
For a standalone backend, visualization remains optional. Open its provisioned
[SPACE logs dashboard](http://127.0.0.1:13000/d/space-main-health-logs).
It works with anonymous Viewer; **Explore is not permitted for this role**.
Select namespace `space` for real services, `space-logs` for fixture
container logs, or `host` and transport `journal` for host collection. Counts
default to one transport to avoid double-counting fixtures emitted to both.
The independent Grafana can visualize metrics, logs and traces without coupling
the ingestion releases. Run only one Grafana tunnel on port 13000. This chart's
`ui` command forwards Loki only, equivalent to:

```bash
kubectl --context kind-space-local -n space-logs port-forward \
  --address 127.0.0.1 svc/loki 13101:3100
```

Grafana has anonymous Viewer access; Loki has no authentication. There is no
Ingress, NodePort, or external load balancer: services are cluster-internal and
port forwards bind only loopback. Other pods in this local cluster can access
them; this is not a multi-tenant security boundary. Do not expose these ports.

Example LogQL (select a time range that includes the workload):

```logql
{namespace="space"} | json | __error__=""
{namespace="space",service="space-node-manager"} | json
{namespace="space"} | json | level="WARN"
{namespace="space"} | json | sandbox_id="YOUR_SYNTHETIC_SANDBOX_ID"
{log_source="journal",namespace="host"}
```

```bash
curl -fsSG http://127.0.0.1:13101/loki/api/v1/query_range \
  --data-urlencode 'query={namespace="space"} | json | __error__=""' \
  --data-urlencode 'since=15m' --data-urlencode 'limit=20'
curl -fsS http://127.0.0.1:13101/loki/api/v1/labels
```

## Collection and semantics

- Alloy runs once per Linux Kind node. Read-only host mounts collect SPACE pod
  CRI files under `/var/log/pods/space_*/*/*.log` and, under the Kind overlay,
  the actual volatile node journal `/run/log/journal`. Fixture pod globs are enabled only by the separate test overlay; it never
  collects its own logs. No Kubernetes API credentials or RBAC are needed.
- Before tailing, `collection.excludedPodSources` removes the builders'
  `build-manager` container, whose stdout includes recipe command output.
  The default matches release `space-builder` in namespace `space`; configure
  literal namespace/pod-name-prefix/container entries for renamed or additional
  releases. Worker logs remain eligible.
  These file-source exclusions are independent of Datadog annotations.
- Each node service runs in its own DaemonSet pod and logs to that pod's
  stdout, a separate Kubernetes container log file per service. The SPACE
  systemd services on Kind sandbox and builder nodes are the node layer the
  SPACE Kind node image bakes in (`space-cache.service` and its siblings) and
  `space-runtime-supervisor.service`, which node-manager's init container
  installs; the per-sandbox sidecars it launches inherit its output. Journal
  collection allows `space-*.service`, and `containerd.service`. The synthetic journal identifier is test-only.
  The sidecars (net-gateway, spaced-host) have no unit: they run in their
  sandbox's `/space/sbx-*` cgroup, by which `collection.journal.sidecars`
  keeps them. Every `local.sh up` lists the nodes labelled
  `space.pplx.net/node-pool=builder` in `sidecars.excludedHosts`, replacing
  any saved list, so builder sandboxes' sidecars stay uncollected, as their
  build-manager is. A release installed before the Kind overlay enabled
  `sidecars` keeps its saved overlay; pass `profiles/kind.yaml` as above.
  Guest journals and the devbox's journal are not mounted or forwarded here.
- The collector removes the CRI envelope, then extracts fields for labels and
  the event timestamp. It does **not** rewrite the original message JSON.
  Journal `MESSAGE` follows the same processing. Non-JSON host output remains
  text. The original `env` field is not renamed to `environment` in the body.
- Indexed labels are exactly `service`, `environment`, `cluster`, `namespace`,
  `log_source` (`container` or `journal`), and `node`. The transport label is
  not called `source`, which would shadow the JSON `source` field of sandbox
  events. JSON `service`, `env`, `cluster_id` supply the first
  three; `cluster` falls back to `clusterLabel`, which this overlay sets to
  `kind-local`. Absent environment stays absent.
  `version`, sandbox/request IDs, and existing nested span fields remain in the
  body. Filename, stdout/stderr stream, and journal unit are structured metadata,
  **not indexed labels**. Loki query responses can show both together; use the
  `/labels` or `/series` API to inspect the index.
- JSON timestamps use RFC3339Nano; absent/invalid timestamps retain transport
  time. Types, field absence, explicit event overrides, escaping, and nested
  spans stay as emitted. There are no new trace/span identifiers.

Pinned versions and immutable manifest digests are in the chart's
`values.yaml`: Alloy v1.19.2 and Loki 3.7.8. Collection paths, the cluster
label, storage, scheduling, and retention are chart values; this harness only
supplies the Kind overlay. Grafana's version and resource limits belong to the
independent visualization chart. Configuration is based on the official
[file](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.file/),
[journal](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.journal/),
[processing](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.process/),
and [writer](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.write/)
contracts.

## Bounds, cleanup, and limitations

- Loki is one replica using filesystem TSDB v13, a 2 GiB PVC, 24-hour retention,
  and a further compactor deletion delay. A local-path PVC request is **not a
  hard disk quota**, and retention does not evict by disk pressure. Monitor
  devbox disk use; this is disposable, unreplicated test storage.
- Loki/Alloy memory limits are 1 GiB/512 MiB per node. No Grafana is deployed
  or managed by this ingestion release.
  Loki limits ingestion, stream count, line size, query concurrency and duration
  explicitly in `config/loki.yaml`.
- Alloy has node-local persistent cursors at `/var/lib/space-logs/alloy`, a
  256 KiB write batch, 5-second request timeout, and five bounded retries. There
  is no durable delivery queue/WAL. A cursor is not a backend acknowledgement;
  exhausted retries, long outages, rotation, node loss, line limits, and Loki
  rejection can lose logs. Loki may deduplicate replays. Journal cold-start
  backfill is limited to one hour; Loki rejects samples older than 24 hours.
  This is not a full historical import or a lossless pipeline.

To remove only logs (this destroys its Loki records/PVC), delete the owned
logs namespace. If you installed Grafana separately, optionally disable its
Loki datasource through that independent release:

```bash
bash infra/space/kind/logs/local.sh cleanup --confirm-delete-local-data
```

This does not restart Grafana or touch metrics/traces ingestion. The logs helper
never deletes or upgrades those releases.
Stop the UI port-forward process too. Shared SPACE logs and Datadog are untouched.
Small Alloy cursor files remain on Kind nodes; retaining them prevents a
reinstall from replaying all files. Deleting/recreating the task-owned cluster
removes them; do not prune shared logs or shared Docker data to remove cursors.

If upgrading the earlier standalone logs experiment, the logs Helm upgrade
removes its duplicate Grafana Deployment/Service and disposable UI data. Enable
the shared datasource first; Loki's PVC and collected records are retained.

## Validate

The [logs integration workflow](../../../../scripts/tests/observability/logs/README.md)
owns fixture builds, temporary collection values, transport assertions and fixture
cleanup. Normal deployment requires no fixture source checkout or Rust build.
