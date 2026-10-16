# From local validation to a cluster deployment

The [charts in this directory](README.md) can now be installed on a SPACE
cluster, and the runbook beside them is the mechanical procedure. This file is
the remaining **readiness checklist**: what an owner must decide and prove
before a canary in an operator-selected environment. A chart that renders and
installs is not an approved deployment. Operators must explicitly integrate
each release with their deployment process and approve any routing changes.

## Keep four releases independent

| Release | Owns | Does not own |
| --- | --- | --- |
| Metrics | OTLP metrics Collector, Prometheus, metric retention | Grafana, application sink selection |
| Logs | Alloy, Loki, log retention/positions | Application formatter/exporter, trace correlation |
| Traces | OTLP traces Collector, Tempo, trace retention | Sampling policy, span-generated metrics |
| Visualization | Grafana, explicit datasource URLs, dashboards | Backend installation, producer routing |

Promote validated chart configuration through the operator's normal shared Helm
deployment process. The charts' `shared` profile requires explicit storage,
network peers and, for Grafana, real credentials; it does not
supply per-environment values, namespace and release ownership, or deployment
approval. Do not make the local helpers accept arbitrary contexts. The
[`space-observability`](charts/space-observability/README.md) mode switch gives
working defaults for a new cluster; it does not replace this checklist for an
existing shared one. "Any context" must mean a reviewed profile for that
cluster, not silent reliance on kubectl's current context. Context alone is not
identity: record account, cluster ARN/UID, nodes, namespace and release.

## Required before a shared-cluster canary

1. **Owners and security:** nominate backend/data owners; decide SSO/RBAC for
   Grafana and authenticated internal receivers/query APIs. The shared profile
   generates NetworkPolicies and requires a Grafana credentials Secret, but
   rendering a policy does not prove the CNI enforces it, and an admin password
   is not SSO. TLS and secret-manager-backed rotation remain outstanding. Today's SPACE metrics
   client only supports plaintext OTLP/gRPC and rejects nonempty auth headers;
   use an approved private authenticated gateway boundary or resolve that client
   limitation separately. Never put public receivers behind these local defaults.
2. **Data policy:** approve trace/log contents, sensitive-attribute handling,
   retention, deletion, and tenant separation. Cluster/environment/version must
   remain queryable; product/customer/request IDs must not become Loki labels.
   Read actual host/sidecar paths in each cluster: Kind's journal and stdout-tail
   topology is not evidence that EKS nodes expose the same files or permissions.
3. **Storage/reliability:** the shared profile requires a named storage class or
   an existing claim and retains new PVCs, which is not the same as reviewed
   durable storage with a backup/restore policy. Loki/Tempo need an appropriate
   object store and topology for the expected volume; provision scoped IAM using
   existing infrastructure conventions. Grafana must have managed authentication
   and persistence if user edits/accounts are supported. Prometheus retention,
   disk quota, scrape/query load and native histograms need explicit sizing.
4. **Capacity/failure policy:** calculate requests, limits, PVC capacity, rollout
   overlap, selectors, autoscaling and quotas against actual nodes. Metrics
   delta-to-cumulative state is per stream and in memory: one replica remains
   safest initially. Scaling needs producer-consistent routing, not random
   load balancing. Bound cardinality, queues/retries and rejected-data alerts;
   decide what data loss on restart/outage is acceptable. Queue size in requests
   is not a byte budget or durable delivery guarantee.
5. **Producer canary:** deploy the backends with no routing changes first. Then
   opt in only explicitly named SPACE services in the selected environment via
   the owning chart's per-service/per-cluster values. Preserve all unrelated
   DD/OTEL settings.
   Metrics use exclusive `SPACE_METRICS_SINK=otlp` plus a metrics-specific endpoint;
   traces use their own exporter/endpoint and a separately reviewed sampler.
   Unset/auto retains the native default. Keep the canary limited to exclusive
   routing and counter mode. Parallel log collectors need their own checkpoints
   and must not disable Datadog collection.
6. **Acceptance gate:** reproduce known counter increments, histogram units and
   quantiles, gauge zero/retirement, distinct producers, raw typed log fields,
   trace IDs/parent links/error status, and public sandbox journeys. Verify actual
   stored/queryable records and dashboard views. Record exact source/image
   digests, query ranges, rejected/dropped counts, restart/outage loss and the
   original settings. Production sampling, Go/guest delivery and Datadog backend
   mapping require their own evidence, not extrapolation from local ingestion.

## Promotion and rollback

Proceed one signal/service at a time through operator-defined environments,
with a reviewed observation window before each promotion and a separate
production decision. The exclusive routing means moving a producer away from
Datadog also removes that producer's future
Datadog telemetry. Inventory affected dashboards, monitors and alerts before
enabling a canary; do not describe an exporter switch as merely dormant code.

First rollback producer routing to the **captured prior values**, verify running
pods and Datadog delivery, then roll back the backend release if necessary. Keep
data PVCs/buckets until retention and investigation requirements are satisfied.
Use normal reconciliation, not namespace deletion, as shared-cluster rollback.
Production-scale ingestion, HA, retention enforcement, security and monitor
equivalence remain unvalidated by this local MVP.
