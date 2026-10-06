# Snapshot catalog store

`pplx-space-snapshot-store` is the owning PostgreSQL storage layer for the
durable snapshot catalog. It is not an object-storage backend and does not
read or delete snapshot bytes.

## Owned aggregate

The store owns data access for the transactional aggregate formed by:

- `snapshots`;
- `snapshot_storage_locations`;
- `snapshot_storage_location_encrypted_fields`;
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
  require the caller's transaction so partial mutation is not representable.
- Add an operation and record fields with their first migrated caller; do not
  build a speculative complete CRUD surface.
- Do not expose an unrestricted `update_snapshot` operation. Workflow-owned
  guarded state transitions keep their compare-and-set predicate, database
  clock, and row locks in the caller's transaction; they are not flattened
  into ordinary store CRUD.
- A zero-row guarded write is a normal race outcome, not a database error.

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

Caller-backed APIs cover aggregate insertion, reads, and unguarded delete CRUD.
Every read returns the same typed `SnapshotRecord`, containing the complete
non-projection `snapshots` row and its optional linked
`SnapshotStorageLocationRecord`. The duplicated scalar execution projections
stay private to SQL writers; typed `execution_metadata` is canonical. Orphan
storage locations have no owning snapshot and are not reachable through
`get(snapshot_id)`.

The base location row's `storage_connection_string` is exposed by that schema
name, not as a resolved URI. When an encrypted-fields sidecar exists, the base
value is only a non-secret stand-in; this read API neither decrypts nor exposes
sidecar secrets. Insert accepts one typed `NewSnapshot`, derives the duplicated
scalar execution columns, and writes the snapshot plus its single location in
the caller's transaction. `UNIQUE (snapshot_id)` provides the concurrency
backstop for idempotent retries; reusing an id for a different snapshot identity
or exact URI is rejected. Delete removes the storage locations before the
snapshot inside the caller's existing authorization-and-lock transaction. The
store does not decide whether deletion or retry is safe and does not touch
storage-backend bytes.

Add further typed CRUD incrementally with each migrated caller. Add claim/prune
with its live retention workflow, after the workflow has a complete protection
matrix for restore, fork, lifecycle, publication, the zero-or-one location invariant,
encryption, and recovery.
