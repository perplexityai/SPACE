# `pplx-space-node-runtime-manager-store`

runtime-manager's node-local SQLite store: `operation_results` and the shared
`jobs` table in `runtime-manager.db`. Outside tests, runtime-manager opens,
migrates and queries that database only through this crate. The database
migrator (`pplx-space-db-migrate`) also migrates and resets it.
`layers.storage_access` in `ARCHITECTURE.toml` keeps rusqlite and
`pplx-space-common-jobs` out of the normal and build dependencies of
`pplx-space-node-runtime-manager`; dev-dependencies are not checked, so its
tests may seed the jobs table directly.

## Changes

- Outside the `test-support` feature, no public item may return, expose or
  re-export a rusqlite connection, transaction, statement, row or error
  value. Callers name `database::Connection`, `SharedConnection`,
  `ConnectionGuard` and `database::Error`; the rusqlite values they wrap stay
  `pub(crate)`.
- runtime-manager reaches the job queue's worker only through
  `jobs::JobWorker` (`src/jobs.rs`), built by
  `SharedConnection::background_worker`. A worker operation runtime-manager
  does not call yet is a new forwarding method there, which returns the
  worker's future directly; never re-export `BackgroundWorker`. Re-export
  only job items that run no SQL, and list each in the `jobs` module doc.
- Add a verb, worker method or re-export with its first caller in
  runtime-manager, never speculatively.
