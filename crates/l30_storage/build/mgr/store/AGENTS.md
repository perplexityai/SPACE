# `pplx-space-build-mgr-store`

build-manager's node-local SQLite store: `build_job_results` and the shared
`jobs` table (`db/common`) in build-manager's own database, not the
control-plane Postgres. Outside tests, build-manager opens and queries that
database only through this crate; it does not migrate it. The database
migrator (`pplx-space-db-migrate`) migrates and resets it with its own copy
of the `db/build-manager` and `db/common` migrations.
`layers.storage_access` in `ARCHITECTURE.toml` keeps rusqlite and
`pplx-space-common-jobs` out of the normal and build dependencies of
`pplx-space-build-mgr-server`; dev-dependencies are not checked, so its tests
may seed the jobs table directly.

## Changes

- Outside the `test-support` feature, no public item may return, expose or
  re-export a rusqlite connection, transaction, statement, row or error
  value; the one exception is the rusqlite errors that `FromSql` and `ToSql`
  impls return by design, such as those on `JobId`. Callers name
  `database::Connection`, `SharedConnection`, `ConnectionGuard` and
  `database::Error`; the rusqlite values they wrap stay `pub(crate)`.
- `database::open` keeps build-manager's open: `rusqlite::Connection::open`,
  a 30 second busy timeout, then one `execute_batch` that sets
  `journal_mode=WAL`, `synchronous=NORMAL` and `foreign_keys=ON`, in that
  order. build-manager shares that one connection through
  `SharedConnection`, with its jobs worker too.
- build-manager reaches the job queue's worker only through
  `jobs::JobWorker` (`src/jobs.rs`), built by
  `SharedConnection::background_worker`. A worker operation build-manager
  does not call yet is a new forwarding method there, which returns the
  worker's value or future directly; never re-export `BackgroundWorker`.
  Re-export from `jobs` only job items that run no SQL and that build-manager
  names, and list each in that module's doc.
- Add a verb, worker method or re-export with its first caller in
  build-manager, never speculatively.
