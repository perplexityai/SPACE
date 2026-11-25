# `pplx-space-node-mgr-store`

node-manager's node-local SQLite store: `network_slots`, `network_pool_meta`
and `sandbox_executions` in node-manager's own database, not the
control-plane Postgres. Outside tests, node-manager opens, migrates and
queries that database only through this crate. `pplx-space-db-migrate` also
migrates, reverts and resets it, with its own copy of the `db/node-manager`
migrations; a change to how this crate migrates the database needs the same
change there. `layers.storage_access` in `ARCHITECTURE.toml` keeps rusqlite
and `pplx-space-sqlite` out of `pplx-space-node-mgr`'s normal and build
dependencies; dev-dependencies are not checked, so its tests may seed and
inspect rows directly.

## Changes

- Outside the `test-support` feature, no public item may return, expose or
  re-export a rusqlite connection, transaction, statement, row or error
  value. Callers name `Connection`, `Transaction` and `Error`; the rusqlite
  values they wrap stay inside this crate. `Error`'s `Display` and `source` are
  rusqlite's, unchanged.
- `database::open` keeps `pplx_space_sqlite::open_rusqlite` with foreign
  keys enforced, and `database::open_read_only` keeps
  `SQLITE_OPEN_READ_ONLY` with a five-second busy timeout and no PRAGMAs.
  node-manager holds the one connection `open` returns behind its slot
  pool's mutex.
- `Transaction::deferred` is the only transaction node-manager opens. A
  call site that needs another kind gets its own constructor here, named for
  that kind, as `pplx-space-sbx-mgr-store`'s `Transaction::immediate` is.
- Add a verb, handle method or re-export with its first caller in
  node-manager, never speculatively.
