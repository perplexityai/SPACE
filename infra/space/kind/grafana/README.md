# Local visualization: Grafana

Independent visualization for the SPACE Kind metrics, logs and traces backends,
installed automatically by default Kind bootstrap and full-stack Tilt in `otel`
mode. This chart owns Grafana, dashboards and datasource provisioning only.
It does not install a backend or change application routing. The releases remain
runtime-independent: for standalone installations, enable only the datasources
you have.

## Install and open

```bash
helm dependency build infra/space/observability/charts/grafana
helm lint infra/space/observability/charts/grafana -f infra/space/observability/charts/grafana/profiles/kind.yaml
# The Kind profile enables Prometheus, Loki and Tempo.
bash infra/space/kind/grafana/local.sh up
# Default bootstrap and full-stack Tilt already forward this port in otel mode.
bash infra/space/kind/grafana/local.sh ui
```

The helper verifies Docker Kind `space-local` (or `$SPACE_KIND_CLUSTER`), its
published loopback API, the explicit matching `kind-` context and owned
namespace `space-grafana`.
It applies the chart's Kind overlay, which is what permits anonymous access,
disposable state and no network policies. For shared clusters use the portable
[observability charts and deployment runbook](../../observability/README.md),
not this local test harness.
The UI is <http://127.0.0.1:13000>; keep that terminal running. No Ingress,
NodePort, LoadBalancer or external credentials. Anonymous access is **Viewer**;
there is no login, admin account or Explore access. Do not expose the port.
Other local pods can reach the ClusterIP; this is not tenant isolation.

| Dashboard | URL | Backend |
| --- | --- | --- |
| Main Health (default when metrics enabled) | `/d/space-main-health` | Prometheus |
| SPACE logs/events | `/d/space-main-health-logs` | Loki |
| Trace search; click an ID for the detail view | `/d/space-traces` | Tempo |

Stable datasource UIDs are `space-prometheus`, `space-loki`, and
`space-tempo`. Their URLs are explicit values rather than dependencies on
another Helm release. For a logs-only installation:

```bash
bash infra/space/kind/grafana/local.sh up \
  --set datasources.prometheus.enabled=false --set datasources.tempo.enabled=false
```

For traces-only, disable Prometheus and Loki; Tempo is already enabled in the
Kind profile. The shared chart defaults still leave Loki and Tempo disabled.
Only enabled sources' dashboards are mounted. A future authenticated datasource
must use secret-backed provisioning, not plaintext `--set` credentials.

### Migrating the old combined metrics chart

Install this chart and verify its datasource before upgrading `metrics`.
Then use the metrics helper's `up --reuse-values` to remove the old bundled
Grafana. It leaves Collector, Prometheus and the Prometheus PVC intact. Stop
the old port-forward and run this chart's `ui`. Dashboard UIDs and port 13000
are unchanged; URLs shared earlier continue working. Existing user-authored UI
changes were never persistent: dashboards are provisioned and read-only.

### Synthetic validation

The [test-only metrics dashboard](../../../../scripts/tests/observability/grafana/README.md)
is provisioned separately for fixture inspection and removed afterward. It is
absent from normal Kind and shared installations.

### Empty panels

Installing Grafana does **not** route SPACE telemetry to these backends.
Select a time range containing a test and clear stale version filters. The
real API metric selector is promoted `service_name`, not optional `service`.
The synthetic helper workload does not emit real create/reconnect metrics.
After a temporary canary is routed back to a different backend, fresh
metrics/traces stop arriving here.
Metrics retention is six hours in the companion local stack: this is not a
durable test-results archive. Preserve query JSON/reports separately.

See [widget mappings and semantic differences](dashboard-translation.md) and
[the real API journey results](../../../../scripts/tests/observability/grafana/validation.md). Five of six User
Journeys panels were populated; reconnect bad outcomes remain unvalidated.
Unsupported Datadog widgets are identified, not populated with invented data.
Tempo inspection does not implement Datadog `trace.*` APM metrics or service maps.

Trace search uses `{resource.service.name =~ "SERVICE_REGEX"}`. Tempo 3's
default query-end cutoff excludes the newest 30 seconds. Click the table's ID
link to open the provisioned trace-detail dashboard; it needs no Explore role.
Trace IDs, parent edges, attributes and status are available in the detail view.
No log-to-trace correlation is introduced.

## Pin, resources and evidence

Grafana **13.2.2**, immutable digest in `values.yaml`. Requests 100m CPU/256Mi;
limits 500m/1Gi; Go soft memory target 512MiB. Disposable UI state uses a bounded
256Mi `emptyDir`. No PVC is necessary because dashboards/datasources are checked
in. Readiness/liveness use `/api/health`. Upgrade uses Recreate (one replica).
Background analytics, update checks and plugin preinstallation are disabled.

Validated on 2026-09-23 against Kind API `https://127.0.0.1:37403` (cluster UID
`155bf3b0-7530-4a08-8a71-6a0c72fb678d`). Chart source is based on main as
of that date. Helm render/lint and live datasource
checks passed (Prometheus-only, Loki-only, Tempo-only and all-signal rendering).
The existing Datadog cluster-coverage guard passed 37 tests and 15 subtests.
After separation, the unchanged Prometheus retained 18 API create
observations at historical query time `1790189176`. Loki remained independently
available. Historical dashboard and browser results are in the linked reports;
the Tempo companion report records the new trace backend/fixture checks. The
new anonymous-browser trace search → detail journey passed with two successful
datasource requests, zero query/JavaScript errors and all seven fixture spans
visible, including error highlighting and parent/child nesting. Matching the
table override on field name `traceID` (not display caption `Trace ID`) is
necessary to replace the plugin's Explore link with the Viewer-safe dashboard.

## Rollback and cleanup

Restart `ui` after a Grafana rollout replaces its pod. To revert a configuration,
use `helm history space-grafana -n space-grafana --kube-context
kind-space-local`, then `helm rollback` with that explicit context, namespace
and selected revision. This affects only the UI, not backend data or producers.

```bash
# Deletes only this task-owned namespace and disposable UI state.
bash infra/space/kind/grafana/local.sh cleanup --confirm-delete-local-data
```

Reinstall from Git to recover the UI. Backend namespaces and PVCs are untouched.
For experimental/testing deployment prerequisites, see
[shared-cluster readiness](../../observability/shared-cluster-readiness.md).
These local defaults are not approved for shared clusters.
