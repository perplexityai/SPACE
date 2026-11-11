
# Develop SPACE API and controller services

- Keep stateless API services stateless. Do not add long-running controller,
  reconciler, or stateful background loops to an API process.
- Keep handlers latency-bounded; under ten seconds is the normal target. For
  longer convergence, persist intent and enqueue work instead of holding the
  request open.
- Cluster-singleton background work belongs in
  `crates/l40_api/cluster/controller/server/`. Add a reconciler to
  `run_reconcilers()` and configure it under
  `clusterController.reconcilers.<name>` rather than creating a binary and Helm
  release per loop.
- Per-node loops belong in `crates/l40_api/node/ctl/server/`, which runs as the
  `space-node-ctl` DaemonSet once per node.
- If neither cardinality fits, define a service with explicit deployment,
  lifecycle, ownership, telemetry, and failure handling. The API may make
  bounded request-scoped calls, persist intent, or enqueue work.

Before changing Kubernetes substrate code, read
`crates/l40_api/api/src/controllers/substrate/AGENTS.md` completely. It includes
the rule against naked Kubernetes LIST calls.

Read [async and Tokio](async.md) when the service change adds tasks or loops,
and use `$space-observability` when it changes telemetry.
