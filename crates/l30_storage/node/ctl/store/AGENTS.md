# `pplx-space-node-ctl-store`

node-ctl's node-local SQLite store: `dirty_facets`, `dirty_node_state`,
`facet_sync_overrides` and `reconciliation_status` in node-ctl's own database,
not the control-plane Postgres. Outside tests, node-ctl opens, migrates and
queries that database only through this crate. The database migrator
(`pplx-space-db-migrate`) also opens it, with its own copy of the
`db/node-ctl` migrations, to apply, revert, reset or report them; a change to
how this crate migrates the database needs the same change there.
`layers.storage_access` in `ARCHITECTURE.toml` keeps rusqlite out of the
normal and build dependencies of `pplx-space-node-ctl-server`;
dev-dependencies are not checked, so its tests may seed and inspect the
database directly.

## Changes

- Outside the `test-support` feature, no public item may return, expose or
  re-export a rusqlite connection, transaction, statement, row or error
  value. Callers name `database::Connection`, `Transaction`,
  `SharedConnection`, `ConnectionGuard` and `database::Error`; the rusqlite
  values they wrap stay `pub(crate)`. `Error`'s `Display` and `source` are
  rusqlite's, unchanged, and an error node-ctl raises itself, such as for a
  poisoned lock, comes from an `Error` constructor (`Error::invalid_query`).
- `database::open` keeps the shared open options: the default busy timeout,
  WAL and `synchronous=NORMAL`. node-ctl shares that one connection through
  `SharedConnection`.
- A transaction node-ctl opens is a `database::Transaction`, built with the
  kind it has always used (`Transaction::deferred`); its boundaries stay with
  node-ctl.
- Add a verb, handle method or constructor with its first caller in node-ctl,
  never speculatively.
