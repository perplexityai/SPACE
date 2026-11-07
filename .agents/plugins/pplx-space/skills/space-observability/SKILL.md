---
name: space-observability
description: Develop or review SPACE metrics, traces, sandbox events, Datadog dashboards, and monitors.
---

# Develop SPACE observability

## Rust telemetry

- Keep `pplx-space-observability` limited to telemetry transport and plumbing.
  Define a product metric family, with its tag vocabulary, in a `metrics`
  module of the crate that records it, built on `pplx-space-metrics-emit`.
  When several crates record a family, define it in the recorder the others
  depend on, or else in a crate they all depend on that is not one of the
  `legacy` crates in `ARCHITECTURE.toml`, such as `pplx-space-grpc-metrics`;
  if there is none, add a `*-metrics` crate in the lowest layer whose rules
  allow its names, as `pplx-space-reconcile-metrics` (l10_foundation) was
  added. A vocabulary whose values a lower crate computes lives in that
  crate, as the auto-lifecycle delete admission reason lives in
  `pplx-space-sbx-mgr-store`. `pplx-space-metrics` is being split up into
  those owners; add no family to it.
- Product code emits through a metrics module's named functions, not the
  low-level `counter`, `gauge`, `timing_ms`, or `tag` functions of
  `pplx-space-observability` or `pplx-space-metrics-emit`.
- Keep the owning metrics module's documentation current when adding,
  renaming, removing, or retagging product metrics.
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
