# Kind User Journeys dashboard validation: 2026-09-23

Five of six User Journeys panels displayed real SPACE API metrics in Grafana.
The sixth, reconnect bad outcomes, remains **not validated**. This was a local
smoke and visualization check, not a load test or Datadog parity claim.

## Why the panels were empty

- The running API selected `otlp`, but its endpoint was the Datadog Agent,
  not this Collector. Installing dashboards does not redirect applications.
- The dashboard's Service selector used the optional metric label `service`.
  The real API emits promoted resource label `service_name="space-api"`, with
  no `service` label. The metrics dashboard now filters on `service_name`.
- Synthetic contract traffic does not emit the API create/reconnect metrics.
  Rates and percentiles require repeated observations, not just one request.

Only `space-api` received a temporary metrics-specific override:

```text
OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://collector.space-metrics.svc.cluster.local:4317
```

The canary captured the prior setting and restored it afterward.
No sink selector, general OTLP endpoint, trace/log settings, Datadog Agent,
other SPACE service, or shared deployment default was changed. Grafana's
dashboard correction was deployed through the existing guarded Helm wrapper.

## Workload and observed results

Run: **18:42:23–18:46:16 UTC**, with cleanup afterward. Used the public API and
data plane, existing typed scenarios, and existing ECW request/key helpers.
Eight traffic rounds were spaced roughly 23 seconds apart; the application
retained its normal ten-second export interval.

| Check | Result | Observed evidence |
| --- | --- | --- |
| `ecw.command_round_trip` | Passed | Created a real sandbox; command returned the expected marker; deleted its sandbox. |
| `ecw.pause_connect` | Passed | Wrote a marker, durably suspended, reconnected, and read the same marker; cleaned up. |
| Repeated public create/connect/delete | Passed | Eight additional creates returned 201; eight running-sandbox connects returned 200; deletion succeeded. |
| Create rejection control | Passed | Eight missing-template creates returned 404. |
| Creates panel | Passed | Two finite series: successful `seed` creates and failed creates without a template label. Stored histogram counts: **10 success + 8 error**. |
| Create latency panel | Passed | Successful-create p90 approximately **2.10–2.41 seconds**. Raw query values are milliseconds; Grafana auto-scales the display. |
| Create error panel | Passed | Approximately **37.7–48.5%** in the sliding rate window. These are deliberately induced test errors. |
| Reconnect attempts panel | Passed | Finite sliding-window series. Stored cumulative counters: **8 connected + 1 wake_submitted**. |
| Reconnect latency panel | Passed, narrow coverage | All five quantiles rendered; repeated already-running connects measured roughly **10–11.5 ms**. This percentile view does not establish restore latency; see below. |
| Reconnect bad outcome panel | Not validated | No `rejected`, `exhausted`, or `wake_failed` counter samples. The eight post-delete 404 controls appear in HTTP route metrics, not the reconnect outcome counter. |
| Browser rendering with Service=`space-api` | Passed | Final capture: 12 Grafana query requests, zero query errors or browser errors, 344 finite numeric values across returned frames. Screenshots show the populated panels. |
| Cleanup/configuration restoration | Passed | Temporary endpoint removed and running pods verified; API rollout healthy. All SPACE Deployment/DaemonSet pod specs compare identical before and after. Task-owned tunnels and credentials removed; sandbox cleanup succeeded. |

Prometheus held **69 API series across 30 metric families** at the final query.

## Actual queries and labels

Exact stored totals, evaluated at Unix time `1790189176`. The run predates
the OTLP sink dropping the doubled unit, so it queried
`..._latency_ms_milliseconds`; the names below are the current ones.

```promql
sum by (result, status_code) (
  histogram_count(space_sandbox_api_latency_milliseconds{
    service_name="space-api", op="create"
  })
)
# success / 201: 10; error / 404: 8

sum by (outcome) (
  space_sandbox_reconnect_attempts_total{service_name="space-api"}
)
# connected: 8; wake_submitted: 1

histogram_quantile(0.90, sum(rate(
  space_sandbox_api_latency_milliseconds{
    service_name="space-api", op="create", result="success"
  }[5m]
)))
```

Reconnect latency uses native histogram
`space_sandbox_reconnect_duration_milliseconds`. Observed identity labels:
`deployment_environment_name="kind-local"`, `cluster_id="kind-local"`,
`service_name="space-api"`, `service_version="stack-7c051315-after-otel"`,
`instance="space-api-5694fc964f-4mmkt:1"`, plus promoted pod/node identity.
Product labels include `surface`, `op`, `result`, `status_code`, `template_name`
where resolved, and reconnect `entrypoint`, `initial_lifecycle`, `outcome`,
`reason`, `terminal_phase`, and `client_class`.

Sliding `increase()` counts are extrapolated, not an exact request ledger.
They returned fractional estimates (roughly 9.57 successful creates, 8.49
failed creates and 8.49 reconnects at the last sampled evaluation). Raw
cumulative counts above match the independent request ledger. A series with
only one exported observation cannot contribute a rate: the single suspended
reconnect was stored, but its own histogram stream did not contribute to the
rate-based reconnect percentile panel. Repeat suspend/reconnect across export
intervals to validate that latency path separately.

The post-delete 404 is returned while fetching the scoped sandbox, before
`record_reconnect_attempt`. It therefore does **not** prove collection failure
or a zero bad-outcome rate. Testing that sixth panel requires an actual
instrumented rejection/failure scenario; no fault injection or limiter-policy
change was made just to fill it. Other dashboard groups, including node disks,
builds and guest health, were not validated by this API-only run.

## Provenance and local evidence

- Verified context: `kind-space-local`, cluster `space-local`, Kubernetes API
  `https://127.0.0.1:37403`, matching Docker's control-plane port.
- Cluster UID: `155bf3b0-7530-4a08-8a71-6a0c72fb678d`.
- Running image: `space-api:tilt-6c6046efcc6ee907`, runtime image ID
  `sha256:6c6046efcc6ee907b98cf5e3fe219374e50e520405cecdbb9adc8184d804ee73`.
  Its OCI revision and binary Git SHA are `unknown`; the version label is **not**
  proof of its exact source commit. No application image was rebuilt for this run.
- Smoke/code inspection used a clean local checkout of the telemetry branch; the
  dashboard came from a separate local checkout plus local dashboard changes.
- Collector Contrib **0.161.0**, Prometheus **3.14.0**, Grafana **13.2.2**;
  pinned image digests remain in `values.yaml`. Only Grafana rolled for the
  dashboard update; Collector and Prometheus retained their original pods.
- Local evidence: `target/user-journeys-20260923-r2/`: `run.log`,
  `raw-metrics.json`, `totals.json`, `panel-*.json`, `panels-summary.json`,
  `browser-responses.json`, `browser-summary.json`, `user-journeys.png`,
  `dashboard-url.txt`, and before/after pod specs and image identities.
- First attempt retained separately at `target/user-journeys-20260923/`.
  `127.0.0.1:8080` reached a different listener while Tilt listened on IPv6
  localhost; the first ECW create returned routing 404. The successful retry
  used task-owned, explicit Kind API/data-plane tunnels on **19880/19890**.

The exact local rerun wrapper and query/browser checks are retained under
`target/user-journeys-20260923/{run.sh,verify.mjs,browser.mjs}`. The wrapper uses
existing repository helpers, owns its ECW key, traps cleanup/restoration, and
invokes these existing scenarios with explicit local endpoint configuration:

```bash
uv run --frozen --python 3.11 -m space_smoke run ecw.command_round_trip
uv run --frozen --python 3.11 -m space_smoke run ecw.pause_connect
```

Those commands require the run-scoped key and local endpoint environment set
by the wrapper; do not run them against an inherited deployed environment.
Choose a new evidence directory and coordinate with the bench owner before
repeating the API override. The temporary tunnels are cleaned up; the existing
Grafana UI remains available on localhost port 13000.

Open Grafana with the fixed range **18:41:53–18:46:46 UTC on 2026-09-23**, Service
**space-api**, other filters **All**, and window **5m**. The saved URL contains
these selections. Prometheus retains only **six hours** (also size-bounded):
the link will eventually expire as evidence. The saved raw JSON and screenshots
remain available locally afterward. Fresh API metrics again flow to the
original Datadog Agent endpoint; ongoing collection here would be a separate,
explicit local routing choice.
