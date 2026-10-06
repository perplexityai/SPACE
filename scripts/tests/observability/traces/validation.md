# Traces Kind validation: 2026-09-23

Collector → Tempo stored and served SPACE OTLP traces without application
instrumentation changes. This proves the tested local ingestion/query path,
**not production readiness, complete cross-service coverage or Datadog parity**.

## Source and target

- Docker cluster `space-local`, explicit context `kind-space-local`, API
  `https://127.0.0.1:37403` verified against the Docker published control-plane
  port. `kube-system` UID `155bf3b0-7530-4a08-8a71-6a0c72fb678d`.
- Chart based on main as of the run date; isolated namespace `space-traces`.
  The integration/Tilt checkout was left clean.
- Existing Rust fixture built from a clean local integration checkout, feature
  `datadog-otel`.
  Binary SHA-256 `b07d9d2c4e4ff1b8a6fc28816a344fd34aed78d1d59047181e21616136bb9274`.
  This source contains the tracing implementation; it is not an assertion that
  all tracing-stack PRs are already on main.
- Running API image `space-api:tilt-6c6046efcc6ee907`, runtime ID
  `sha256:6c6046efcc6ee907b98cf5e3fe219374e50e520405cecdbb9adc8184d804ee73`.
  Internal API uses `space-node-stack:tilt-69dc04edf9a906a6`. These existing
  images report unknown source revision: `DD_VERSION=stack-7c051315-after-otel`
  is a label, **not provenance proof**. No SPACE image was rebuilt for this run.
- Collector Contrib **0.161.0**, manifest digest
  `fd328de2552466ad78385e1b1289c3f2402b1c45f265b252aab1955b42845ac1`;
  Tempo **3.0.3**, digest
  `0296560ac66f8a3600d7fb3014a52c189d4d9c3549ad6ff441bf2409855d68d5`.
  The running container image IDs match these pins.

## Results

| Check | Result | Stored evidence / limit |
| --- | --- | --- |
| Pinned binary configuration validation | Passed | Collector `validate --config=...`; Tempo `-config.verify=true`; Helm lint/render and local server dry-run. |
| Existing fixture → backend | Passed | Two traces, exactly seven spans each: HTTP server, gRPC client/server, DB query/pool, Redis and serialized-parent job. Raw Tempo records were fetched and asserted, not inferred from export logs. |
| Trace identity and parenting | Passed | Same 128-bit trace ID across each trace; seven unique 64-bit span IDs; exact six parent edges, including gRPC metadata and serialized W3C context. |
| Names, kinds and error status | Passed | Span names remain `POST /trace-parity`, `space.parity.v1.Worker/Execute`, `SELECT 1`, etc. Three ERROR spans in the failure case; none in success. Server/client/internal kinds preserved. |
| Typed fields and resources | Passed | Integer HTTP 200/500 and DB rows 1/0, string synthetic sandbox ID, service/version/environment/cluster identity all preserved. `operation.name=grpc.server` remains a separate string attribute. |
| Existing `ecw.command_round_trip` | Passed | Real public create, guest command marker and critical sandbox cleanup. Repeated successfully in the final run. |
| Existing `ecw.pause_connect` | Passed | Real create, marker write, durable suspend/reconnect and marker read; no cleanup failures. Final restore/connect phase about 7.8s. |
| Real API trace storage | Passed, scoped | Example create trace `7b01ef31f85b3adb3f0f94f18a10393b` has 38 API spans including Kubernetes client calls, DB pool spans and worker client retries. A transient worker error can coexist with an ultimately successful create. |
| Real HTTP error / upstream context | Passed | Missing-template POST returned 404. Trace `8e62a44695afea968d01a189c56a4ba5` retained the supplied trace ID and supplied remote parent, with ERROR status and HTTP status **string** `"404"`. |
| Distinct live services | Passed, scoped | Tempo resource search contains `space-api`, `space-cluster-api` (the internal-API deployment's actual service name), and the isolated fixture service. |
| Collector and Tempo drained restart | Passed | Seven stored success spans remained fetchable after both deployments restarted; PVC UID `41bb2cca-819b-47c3-92ff-549ebe94e7f7` unchanged. A fresh fixture run then stored/verified two new traces and all 14 spans. |
| Queue / rejection snapshot | Passed, narrow | Before restart: 905 accepted/sent spans, queue 0/128, receiver failed/refused 0; Collector RSS about 196MiB. This snapshot is not a sustained-load or bounded-loss test. |
| SPACE restoration | Passed | Both temporary traces-specific endpoints restored with the existing helper; live pods healthy; all SPACE Deployment/DaemonSet pod specifications identical before/after. Metrics/logs and general DD/OTEL settings unchanged. |
| Complete multi-service / Go / guest trace | Not validated | Only API and internal API were routed. Workers, shim, data plane and controller stayed on their original backend. Some internal-API traces therefore have parents/roots absent from Tempo. No trace containing both selected service names was found. |
| Links/events, production sampling, tail sampling, Datadog mapping | Not validated | Fixture checks the fields it emits. No new log correlation or APM aggregate/service-map compatibility claim. |
| In-flight outage, queue exhaustion, disk-full, HA and retention expiry | Not validated | Restart was after drained exports. Memory queues can lose data on failure; local PVC is not durable across cluster destruction. |

Before restart, checked run `oss-tempo-checked-r2-20260923`:

| Case | Trace ID | Spans | ERROR spans |
| --- | --- | --- | --- |
| Success | `99a6f000f161961064bb540445e41fff` | 7 | 0 |
| Error | `f6573974b1a1ec4c5c948c46f8998b63` | 7 | 3 |

After restart, checked run `oss-tempo-recovery-20260923`:

| Case | Trace ID | Spans | ERROR spans |
| --- | --- | --- | --- |
| Success | `d03d624da1bd495b9f840f739a38a612` | 7 | 0 |
| Error | `28c954933c4b162999acaee484ddb7a4` | 7 | 3 |

## Findings and interpretation

The first automated query used a two-second pre-workload window. Tempo 3's
30-second query-end cutoff initially returned no results and eventually an
invalid effective range. The operator check now searches a wider window and
polls up to 90 seconds. No backend rewrite or disabled cutoff was necessary.

The first optional HTTP negative control omitted the credential helper's
required `--config -`; its guard prevented an unauthenticated request. That run
still passed both typed smokes and restored both deployments. The corrected
full rerun passed both smokes plus the correlated negative control and cleanup.

The real API emits HTTP status as a string whereas the fixture uses integers.
The Collector does not coerce either. Query real API status with
`span.http.response.status_code="404"`; query the fixture with `=500`, or use
intrinsic `status=error` when that is the intended question. Ingestion does not
make inconsistent application attribute types uniform.

Per-service canary routing deliberately splits distributed traces across
backends. Internal API also handles background traffic from unredirected
workers. Missing roots in that selected subset are not evidence of collector
loss; proving a complete trace requires coordinated routing of its participating
services and a known trace ID. Do not broadly reroute the bench just to hide it.

## Commands and evidence

Startup, repeatable fixture, query, restart and cleanup commands are in
[README.md](README.md). The real journeys used run-owned credentials and
configuration cleanup, plus explicit Kind port-forwards to API `19880:8080`
and data plane `19890:8090`.
Typed commands (local endpoints despite the runner's `deployed` mode, which
avoids a second bootstrap):

```bash
SPACE_SMOKE_BACKEND=deployed E2B_DOMAIN=local.e2b.test E2B_TEMPLATE=seed \
  E2B_API_URL=http://127.0.0.1:19880 ECW_API_HOST=api.local.e2b.test \
  ECW_DATA_PLANE_BASE_URL=http://127.0.0.1:19890 ECW_DATA_PLANE_DOMAIN=local.e2b.test \
  uv run --frozen --python 3.11 -m space_smoke run ecw.command_round_trip
# Same environment, then: ... -m space_smoke run ecw.pause_connect
```

`E2B_API_KEY` was supplied from the run-owned local helper, not copied into Git
or printed. The negative request used the same helper and synthetic missing
template, with a generated `traceparent: 00-<trace-id>-<parent-id>-01` header.
Both temporary endpoint overrides targeted only
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`; their original absence was restored.

Local evidence remains in the run checkout's
`target/validation-20260923/`: checked fixture
summaries/raw records, compiler output, source/image snapshots, real-smoke
commands/output, before/after specs, negative-control record, Collector counters
and PVC snapshots. This directory is gitignored and is not a permanent shared
artifact store. The six-hour backend retention is likewise not an archive.

Shared-cluster rollout requires a separate review of security, data policy,
storage, resource sizing, delivery failure behavior and explicit service routing.
The Grafana companion PR contains the cross-signal readiness checklist.
