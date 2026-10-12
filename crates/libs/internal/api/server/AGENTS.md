# `pplx-space-internal-api-server`

Every table named here is in the control-plane Postgres store.

## Changes

- New ClusterApi and NodeInfo RPCs rely on the auth layers in `src/run.rs`:
  `NodeApiAuthLayer` (`SPACE_INTERNAL_TOKEN` or TokenReview) and, for
  template publication, `BuildAuthLayer`. Health and reflection are exempt.
  Neither layer identifies the calling node.
- A new RPC that acts for a node identifies it as the registered node whose
  address equals the caller's remote IP, never by a request field alone; one
  that names a node uses `authenticated_origin_node_id`. Existing exception:
  `TouchArtifactBackup` binds no node.
- Destructive work authorized by the caller's role needs a registration that
  existed before the call.
- `PublishNodeState` writes inventory, projections, node status and receipt
  consumption in one transaction (`src/publish.rs`), after origin
  registration commits separately. Keep receipts last, so their fencing sees
  the final evidence.
- A new `public.sandboxes` lifecycle write from a node report rechecks, in
  the write or under a `FOR UPDATE` row lock taken in the same transaction,
  that `active_node_id` is the authenticated publisher and that the current
  lifecycle is one it may leave, as the restored, stopped and quarantined
  projections (`src/published_state.rs`) do. Existing exceptions: the failed
  projection checks in a CTE and updates by id; delete receipts for stopped
  or suspended rows with no `active_node_id` are fenced by the latest
  execution.
- New restoring-to-started convergence leaves rows with
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
