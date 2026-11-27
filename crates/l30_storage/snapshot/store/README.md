# Snapshot catalog store

`pplx-space-snapshot-store` is the owning PostgreSQL storage layer for the
durable snapshot catalog. It is not an object-storage backend and does not
read or delete snapshot bytes.

## Owned aggregate

The store owns data access for the transactional aggregate formed by:

- `snapshots`;
- `snapshot_storage_locations`;
- `snapshot_storage_location_encrypted_fields`;
- `snapshot_artifact_digests`;
- invariants that span those rows and their owning sandbox.

Each snapshot has zero or one storage-location row. Zero is a valid transient
or already-reaped catalog shape; more than one is invalid. A storage location
identifies the single backend route for that snapshot, not one replica in a
caller-managed set.

Snapshot metadata SQL belongs here as callers are migrated. S3, local disk,
S3-compatible stores, and other byte-storage implementations remain behind
their storage backend interfaces.

## API directive

- Expose named operations with typed arguments, records, outcomes, and errors.
  Do not expose a query builder or caller-selected columns and predicates.
- Ordinary reads and single-statement CRUD use explicit filters and limits and
  accept a generic PostgreSQL executor. Multi-statement aggregate operations
  must run in the caller's open transaction, so their statements commit or
  roll back together. Their parameter accepts a postgres-pool `Tx` or a
  `PgConnection` and refuses a pool. It cannot tell whether a connection has
  a transaction open, so each such operation's documentation says the caller
  must pass its open transaction.
- Add an operation and record fields with their first migrated caller; do not
  build a speculative complete CRUD surface.
- Do not expose an unrestricted `update_snapshot` operation. A workflow's
  guarded state transition is a verb named for the transition, never
  flattened into ordinary store CRUD. Its statement keeps its compare-and-set
  predicate whole, bound to the values the caller decided on, and any
  `NOW()` in its SQL. It runs on the caller's executor or transaction and
  returns its outcome; the caller keeps its locks and decides what each
  outcome means. A store test pins its losing case. A step that runs in the
  transaction whose own guarded statement decided the transition may return
  nothing, as `lost::fail_quiet_full_uploads_from_node` does.
- A zero-row guarded write is a normal race outcome, not a database error.
- A row lock the caller relies on after the statement is a verb that takes
  the caller's transaction and is named for the lock; the lock lasts until
  that transaction ends.

The crate is general in ownership, not generic in mutation. Retention,
restore, publication, and other workflows may use different named operations,
but they share this catalog boundary rather than duplicating snapshot SQL in
controller call sites.

## Retention boundary

Retention is split across three responsibilities:

1. The retention workflow discovers work, owns the authorization transaction,
   and claims eligible metadata with the catalog store's typed operations.
2. The selected storage backend deletes the bytes at the immutable route
   returned by that committed claim.
3. The retention workflow finalizes the claimed catalog metadata.

Discovery is advisory. Only the guarded claim authorizes byte deletion. The
backend must use the route returned by the committed claim rather than query
the catalog again. Claim/prune is a retention operation, not generic snapshot
CRUD.

## Current status

Caller-backed APIs cover aggregate insertion, reads, unguarded delete CRUD,
and the guarded transitions and caller-relied locks of the modules below.
`get` returns the typed `SnapshotRecord`, containing the complete
non-projection `snapshots` row and its optional linked
`SnapshotStorageLocationRecord`. Narrower reads return only what their
question needs; `listing::stored_for_sandbox` leaves text columns unparsed, so
a debug view can show rows that `get` would reject. The duplicated scalar
execution projections stay private to SQL writers; typed `execution_metadata`
is canonical. Orphan storage locations have no owning snapshot and are not
reachable through `get(snapshot_id)`.

The base location row's `storage_connection_string` is exposed by that schema
name, not as a resolved URI. When an encrypted-fields sidecar exists, the base
value is only a non-secret stand-in; this read API neither decrypts nor exposes
sidecar secrets. `insert` accepts one typed `NewSnapshot`, derives the
duplicated scalar execution columns, and writes the snapshot plus its single
location in the caller's transaction. `UNIQUE (snapshot_id)` provides the
concurrency backstop for idempotent retries; reusing an id for a different
snapshot identity or exact URI is rejected.
`disk_artifact::insert_creating_fork_copy` is a narrower insert: it writes only
a `creating` fork-copy snapshot row, with no execution columns and no storage
location, and leaves any row already registered under that id as it is. Delete
removes the storage locations before the
snapshot inside the caller's existing authorization-and-lock transaction. The
store does not decide whether deletion or retry is safe and does not touch
storage-backend bytes.

The `lost` module holds what the lost-sandbox give-ups run in their flip's
transaction: the `FOR UPDATE` locks they take on `creating` full uploads
before the flip, and the guarded write that then fails the gone node's quiet
`creating` full uploads for the sandboxes they flipped. It also holds node
drain's guarded write for a node whose instance is confirmed terminated:
failing every `creating` full upload from it while the node is still
`draining` under the Kubernetes identity the caller observed
(`fail_inflight_uploads_from_gone_node`). Its guard reads `nodes` and
`node_k8s`, which belong to `pplx-space-node-registration-store`.

The `claimed` module holds the reads of snapshots a committed claim parked in
`deleting`. Retention's recovery runs two of them: the highest claimed id
(`scan_epoch`) and one page of claims with their optional storage location
(`list`). Both keep their `OFFSET 0` fences so the cost follows the claims, not
the table, and `list` projects the snapshot's age from the database clock.
`deleting_without_ready_copy` is the check a caller runs under its sandbox row
lock before it pins a local artifact a claim may be deleting.

`snapshot_ids_after` pages snapshot ids in ascending order, in any state. It
is the raw id page retention's discovery reads before it checks eligibility.

Some narrower operations live in modules named for their question: `attempt`
(one attempt's stored state), `checkpoint` (the execution a restore's
checkpoint was produced under), `disk_artifact` (the disk artifacts a fork
reads and copies, and the guarded transitions that register, mark ready, hand
over and retire a fork copy), `environment` (the environment a snapshot was
produced under), `fork_source` (a suspended fork source's current suspend
snapshot and live location), `listing` (every row of a sandbox, unparsed, for
debug views), `public` (the snapshots the public API advertises),
`storage_location` (one live location's connection string) and `upload`
(uploads still in flight, and their guarded transitions: a node's finalize and
heartbeat, and the stuck-upload reaper's give-up).

Others are named for the path that issues them. `orphan_locations` pages
orphan storage locations and deletes one that is still an orphan, for the
storage sweeper.
`quarantine_recovery` holds the recovery plan's two snapshot lookups and the
`FOR SHARE` locks publication recovery takes on the snapshot and its location.
`retention` holds retention's discovery read, the locks and re-checks its
claim and finalize take, and the claim's guarded write: the snapshot
`FOR UPDATE` locks, the usable location lock, the claim's safety re-check,
its `ready -> deleting` update (`claim`), and finalize's keep-N floor
re-check.

Add further typed CRUD incrementally with each migrated caller. The store
holds no composite claim or prune operation: the retention workflow keeps
the claim's and finalize's transactions, the order of their locks and
re-checks, and what each outcome means, and calls `retention::claim` for the
claim's `ready -> deleting` update. Move that orchestration into the store
only after the workflow has a complete protection matrix for restore, fork,
lifecycle, publication, the zero-or-one location invariant, encryption, and
recovery.
