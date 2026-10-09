# Local metrics backend

Install Collector Contrib and Prometheus on the verified `kind-space-local`
bench. The helper checks cluster identity and namespace ownership. Shared
installations use the [observability deployment runbook](../../observability/README.md).

Default Kind bootstrap and full-stack Tilt install this backend automatically
in `otel` mode. Use this helper to manage the release independently.

## Deploy and query

Run from the repository root:

```bash
helm dependency build infra/space/observability/charts/metrics
helm lint infra/space/observability/charts/metrics \
  -f infra/space/observability/charts/metrics/profiles/kind.yaml
bash infra/space/kind/metrics/local.sh up
bash infra/space/kind/metrics/local.sh status
bash infra/space/kind/metrics/local.sh ui
# Prometheus: http://127.0.0.1:19090; Ctrl-C closes the tunnel.
```

The release owns only the backend, configuration, storage and access policies.
Installing it creates no producer Jobs and changes no SPACE sink. Grafana is an
independent release, also installed by default but not required for ingestion.
Kind uses disposable local storage and does not provide enforced network
isolation, authentication or production sizing.

## Validate

The [metrics integration workflow](../../../../scripts/tests/observability/metrics/README.md)
owns the synthetic producer, capture, fault injection and assessment. It creates
and cleans up its own Job without upgrading this release.

## Upgrade and retire

Use the helper's `up` command for upgrades. When moving from the earlier chart
that owned a synthetic workload, archive its evidence first: Helm removes that
old release-owned Job. New validation Jobs have an independent lifecycle.

To retire only this owned local namespace and its disposable TSDB:

```bash
bash infra/space/kind/metrics/local.sh cleanup --confirm-delete-local-data
```

This deletes local data. It leaves saved evidence, the Kind cluster, SPACE,
Datadog and other observability releases intact.
