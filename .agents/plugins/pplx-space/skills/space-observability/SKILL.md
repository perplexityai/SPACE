---
name: space-observability
description: Develop or review SPACE metrics, traces, sandbox events, Datadog dashboards, and monitors.
---

# Develop SPACE observability

## Rust telemetry

- Keep `pplx-space-observability` limited to telemetry transport and plumbing.
  Product metric names and helpers belong in `pplx-space-metrics`.
- Product crates emit through `pplx-space-metrics`, not the low-level
  observability `counter`, `gauge`, `timing_ms`, or `tag` functions.
- Keep the owning module's documentation in `crates/libs/metrics/src/` current
  when adding, renaming, removing, or retagging product metrics.
- External calls that can block or conceal endpoint latency require
  OpenTelemetry-shaped client spans. Reuse shared SQLx, Kubernetes, and gRPC
  wrappers. Wrap each Redis command or pipeline in a `redis.client` span;
  `redis_client_span()` in `crates/l40_api/api/src/redis_otel.rs` is the single
  source of truth for the span name and attribute set — extend it there
  rather than re-specifying attributes, and record status on every exit
  path, including timeouts and fail-open errors.

## Sandbox state events

Logical sandbox transition requests, completions, failures, reconciliation, and
force-cleanup paths emit a `pplx-space-events` event on `space.sandbox` with the
sandbox id, typed event, source, and useful details.

Keep event names domain-level, put implementation identity in `source`, and put
causality in `trigger`. Reserve the stream for state transitions, durable
handoffs, race outcomes, cleanup, network policy outcomes, and immutable restore
identifiers—not high-volume request telemetry. Update event tests and the PR's
event/debugging table when changing the vocabulary.

## Datadog

Before creating, changing, or reviewing a `datadog_monitor`, read
[Datadog monitor guidance](references/datadog-monitors.md). It requires source
and live-ingestion evidence, red-teams temporal semantics, and loads the
deployment policy when supplied.

For dashboards and cluster scope, apply `references/datadog-monitor-policy.md`
when supplied; otherwise use the deployment's dashboard policy and cluster
registry. Derive pickers and scopes from that registry, and run its coverage
checks. Environment tags may span multiple clusters; use a supported cluster
identity tag when they must remain distinct.
