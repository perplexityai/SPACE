# SPACE Main Health: local Grafana translation

The default Kind Grafana home dashboard covers SPACE's own health signals for
a local cluster: request and create journeys, latency, state transitions, node
disk, and the shim and sidecar services.

It was derived by reading an internal Datadog board for the same service and
keeping the widgets that have a local equivalent. That read changed no Datadog
resource and no production configuration, and no copy of that board is kept in
this repository. Widgets with no local equivalent are summarized in the
dashboard rather than replaced with guessed Prometheus metric names, and
per-service production cards are not duplicated as empty local charts.

## Open and install

From this branch with the existing local backends:

```bash
helm dependency build infra/space/observability/charts/grafana
helm lint infra/space/observability/charts/grafana -f infra/space/observability/charts/grafana/profiles/kind.yaml
bash infra/space/kind/grafana/local.sh up --set datasources.loki.enabled=true
bash infra/space/kind/grafana/local.sh ui
# Home / metrics: http://127.0.0.1:13000/d/space-main-health
# Logs:          http://127.0.0.1:13000/d/space-main-health-logs
```

This independent chart installs visualization only; ingestion releases do not roll.
Keep one UI tunnel running; restart it after the Grafana rollout terminates its
connection. Prometheus stays intact. The synthetic metrics dashboard is provisioned separately
by `scripts/tests/observability/grafana/metrics-dashboard.sh`. The optional
logs dashboard is mounted only when `datasources.loki.enabled=true`; the default
metrics-only installation does not query or require Loki. Both dashboards are
provisioned/read-only and work with anonymous Viewer; no Explore permission,
editor role, admin account or remote datasource is added.

**Installing a dashboard does not route application metrics.** Existing SPACE
processes retain their sink and endpoint. The initial dashboard validation used
only the synthetic fixture's product metrics. A subsequent real API smoke
populated five of the six journey panels: see
[the journey report](../../../../scripts/tests/observability/grafana/validation.md). Its temporary endpoint was
restored afterward; fresh application data is not continuous. Coordinate a separate
local metrics-only endpoint override and representative smoke with the bench
owner before changing that. Do not dual-publish or change shared defaults.
Raw SPACE container logs already flow independently through Alloy to Loki.

## Query contract

- All metric filters are local/dynamic: environment maps to
  `deployment_environment_name`, version to `service_version`, and service /
  cluster ID to `service_name` / `cluster_id`. Use promoted resource
  `service.name`, not the optional `service` metric attribute: real Kind API
  metrics have `service_name="space-api"` without a `service` label. The
  Datadog AWS cluster selector is
  not equivalent to product cluster ID; it is omitted rather than silently
  mapped to an AWS label absent in Kind. No production defaults are copied.
- Metric names use this stack's explicit `UnderscoreEscapingWithSuffixes`
  translation: dots become underscores, counters receive `_total`, latency
  names ending `_ms` receive `_milliseconds`. Values remain **milliseconds**.
  Dimensionless gauge units produce `_ratio` even for absolute counts such as
  `space_sandbox_guest_health_ratio` and `space_warm_pool_slots_ratio`.
  Byte gauges retain the existing `_bytes` suffix. These names are derived
  from the inspected Rust unit/type contract and pinned translator; application
  names and instruments are not changed. Full application-series ingestion has
  **not** been validated in this receiver.
- Counters use per-producer `rate()` / `increase()` **before aggregation** so
  observed resets are handled independently. Counts are extrapolated sliding
  windows (picker: 1m/5m/15m), not exact event ledgers or Datadog's non-overlapping
  `.rollup(sum,60)` buckets. Adjacent count points overlap: do not sum them.
- Native histogram quantiles use
  `histogram_quantile(q, sum by (...) (rate(metric[window])))`; counts use
  `histogram_count(increase(...))`; means use
  `histogram_avg(sum by (...) (rate(...)))`. This aggregates populations before
  computing percentiles, rather than averaging percentiles. These are
  windowed distributions, not lifetime cumulative quantiles. Exact OTLP
  min/max are unavailable in Prometheus; no substitute is labeled exact max.
- The source's displayed formulas, not unused query definitions, determine
  panels: Create Latency displays p90 only. Source table time aggregation is
  deliberately replaced by time-series windows and noted on each panel.
  Datadog `autosmooth`, `weighted`, and fill-zero functions are not replicated.
- Ratios require a positive denominator. Missing numerators remain gaps, not
  invented zero-error evidence. Warm-pool hit/(hit+miss) combines matching
  observations instead of adding absent outcome series. Reconnect bad outcomes
  are split by outcome/entrypoint and divided by the global attempt population,
  matching the source denominator; incompatible label-set addition is avoided.
- Gauges are last-known absolute observations, never zero-filled. Prometheus
  lookback can display a retired gauge for up to five minutes. Disk panels
  retain source **average across reporting producers**, not fleet totals.
- LogQL parses only required JSON fields for event-count panels. Indexed labels
  are unchanged. Sandbox IDs, errors and reasons are query-time dimensions:
  top lists are bounded to ten; no new indexed customer identifiers are added.
  Raw log panels keep original lines and return at most 500 records.
- JSON `source` is extracted as `event_source`, since the indexed `source`
  label already means container/journal transport. Dotted HTTP fields are
  accessed explicitly. Connect panels accept the emitted `url.path` when the
  parent-span route is absent; no trace correlation or Datadog pipeline is
  assumed. Unique-sandbox counts mean at least one error in the window, not
  repeated failures despite the original widget title. Fresh generation >1
  activations are a diagnostic proxy, not proof of workspace loss.
- Logs default to namespace `space`, transport `container`. Select
  `space-logs` for fixture stdout or `host` with `journal` for journal
  collection. Selecting both paths can count a deliberately duplicated fixture
  twice. Environment/cluster/service variables are independent Loki label
  queries, not Prometheus-only variables that hide real logs.

Reference semantics:
[Prometheus native histogram functions](https://prometheus.io/docs/prometheus/latest/querying/functions/),
[OTLP translation](https://prometheus.io/docs/guides/opentelemetry/),
[Loki query parsing](https://grafana.com/docs/loki/latest/query/log_queries/),
[Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).

## Local validation: 2026-09-23

Historical dashboard validation (before the independent chart split):
configuration was added on top of a local logs-branch checkout. The verified
target was Docker Kind `space-local`, context `kind-space-local`, loopback API
`https://127.0.0.1:37403`. Existing image pins
were retained: Grafana 13.2.2, Prometheus 3.14.0, Collector Contrib 0.161.0,
Alloy 1.19.2 and Loki 3.7.8. No application image was rebuilt or redeployed.
Grafana's immutable image digest remains in `values.yaml`.

| Check | Result / evidence |
| --- | --- |
| Helm lint, default/optional-Loki render, server dry-run | Passed against the local API. |
| Existing cluster-coverage pytest guard | 35 passed, 15 subtests passed; dependencies supplied through uv. No new configuration-mirroring tests added. |
| Translated backend queries | 52/52 execute successfully: 43 PromQL targets and nine LogQL targets. Six targets had data in the captured test window; empty application/event targets are not counted as semantic validation. |
| Anonymous browser | Home, SPACE container logs, journal selection and the unchanged synthetic metrics dashboard all rendered with no query failures or JavaScript errors. No Explore/editor permission was added. |
| Actual displayed data | 12 stored SPACE fixture series, Collector ingestion, real SPACE log volume from seven services, warning/error series from three services, and raw structured logs. Journal selection also returned records. |
| Freshness stat | An instant query returns no data after the fixture has stopped; it does not reuse the last non-null value from earlier in the dashboard range. |
| Full application metric / rare-event equivalence | Not validated: source metrics have not been routed to these backends; no new create/reconnect/build/failure workload was run for this UI change. |
| Unrelated workloads | Before/after UIDs/specs unchanged for SPACE, Datadog Agent, Alloy, Loki, Collector and Prometheus. Only Grafana changed. |

The first browser pass exposed an OOM at Grafana's old 512 MiB limit. After
checking all local nodes' allocations, host memory, replica/recreate strategy,
and absence of an HPA/PDB, the local chart now requests 256 MiB and limits
Grafana to **1 GiB**, with a **512 MiB Go soft memory limit**. The final
four-view browser pass had zero container restarts/OOM events; observed cgroup
memory peak was 716,394,496 bytes (about 683 MiB). This is a local observation,
not a production sizing or concurrency guarantee. Other backend limits and
application resources were unchanged.

Raw query results, source dashboard export, browser scripts/screenshots,
before/after snapshots and commands are in the gitignored local directory
`target/grafana-main-health-20260923/`. Browser validation used localhost
forwarding to this Grafana, not an authenticated external devbox proxy session.

To reproduce static checks without changing the project lockfile:

```bash
helm dependency build infra/space/observability/charts/grafana
helm lint infra/space/observability/charts/grafana -f infra/space/observability/charts/grafana/profiles/kind.yaml
helm template space-grafana infra/space/observability/charts/grafana \
  --namespace space-grafana -f infra/space/observability/charts/grafana/profiles/kind.yaml \
  --set datasources.loki.enabled=true
uv run --frozen --with pytest --with python-hcl2 --with pyyaml \
  python -m pytest scripts/tests/test_check_space_datadog_cluster_coverage.py -q
```

For a future rollout, record `helm history` first and revert only the reviewed
Grafana configuration in the owning branch, then run the normal local upgrade.
The historical combined metrics release used revisions 9–12 for this check;
those are **not** revisions of the new independent Grafana release. Use the
current `space-grafana` history, never an old combined-stack revision, for
subsequent UI rollback. Do not delete namespaces, PVCs or backend data to
roll back a UI change. Reconnect the UI tunnel after any Grafana rollout.

## Panel provenance

Panel IDs refer to the checked-in `config/main-health.json` (Metrics) and
`config/main-health-logs.json` (Logs), which are the definitions this chart
installs. Three metric diagnostics and three log collection views are local
positive controls rather than translations.

The panel set was chosen by working through an internal Datadog board for
the same service and keeping the widgets that have a local equivalent. That
board is not reproduced here: its identifiers, group names and widget titles
stay in the internal tooling that owns them. Anything unsupported locally is
summarized in the dashboard itself rather than guessed at, so this file does
not need a per-widget mapping to stay honest.
