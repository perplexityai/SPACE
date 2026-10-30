# `pplx-space-internal-api-server`

Every table named here is in the control-plane Postgres store.

Before changing code that branches on the substrate or names a Direct-only
table (`node_direct`, `sandbox_direct_route_*`), read the
[substrate contract](../../../api/src/controllers/substrate/AGENTS.md#substrate-contract).

## Changes

- New ClusterApi and NodeInfo RPCs rely on the auth layers in `src/run.rs`:
  `NodeApiAuthLayer` (`SPACE_INTERNAL_TOKEN` or TokenReview) and, for
  template publication, `BuildAuthLayer`. Health and reflection are exempt.
  BuildAuthLayer supplies freshly reviewed pod-bound node name/UID for
  grant-bound template publication; match both against live builder inventory.
- A new RPC that acts for a node identifies it as the registered node whose
  address equals the caller's remote IP (except publication above and template
  catalog reads, which use reviewed node name/UID when supplied and retain
  address matching for missing claims), never by a
  request field alone; one that names a node uses `authenticated_origin_node_id`. Existing exception:
  `TouchArtifactBackup` binds no node.
- ClusterApi never creates a node registration. `nodes` and `node_k8s` rows
  come from the cluster controller's Kubernetes node sync, sandbox pod
  placement and Direct admin registration; a request naming an unregistered
  node fails with `NotFound`.
- `PublishNodeState` writes inventory, projections, node status and receipt
  consumption in one transaction (`src/publish.rs`), after resolving the
  publisher to its existing registration. Keep receipts last, so their fencing
  sees the final evidence.
- A new `public.sandboxes` lifecycle write from a node report rechecks, in
  the write or under a `FOR UPDATE` or `FOR NO KEY UPDATE` row lock taken in
  the same transaction, that `active_node_id` is the authenticated publisher
  and that the current lifecycle is one it may leave, as the restored,
  stopped, failed and quarantined projections (`src/published_state.rs`) do.
  Existing exceptions: activation-completion receipts recheck only the
  lifecycle and are fenced by the execution attempt they release
  (generation, attempt, `active`); delete receipts for stopped or suspended
  rows with no `active_node_id` are fenced by the latest execution.
- New restoring-to-target convergence leaves rows with
  `evacuation_restore_target` set to the node-drain redrive.
- New certification columns advance only on the server's own verdict.
  `node_status_snapshots.local_backup_inventory_reported_at` is also
  withdrawn on the node's claim.

## Tests

Follow "Privileged tests and the CI host" in `docs/rust/space-conventions.md`.

- Node-bound RPCs deny a request with no remote address. Give the test request
  a peer address equal to the node's registered one.

Module moves change log targets and `sql!` locators: see "Log targets" in
`docs/rust/space-conventions.md`.
