# `pplx-space-volmgr-store`

volume-manager's node-local SQLite store: `volume_attachments` and the shared
`jobs` table (`db/common`) in volume-manager's own database, not the
control-plane Postgres. In production, only this crate and the common-jobs
worker it builds over its connection open or query that database, apart from
the schema migrator: `pplx-space-db-migrate` (`space-db-migrator
volume-manager`) opens it with its own copy of the `db/volume-manager` and
`db/common` migrations to apply, revert, reset or report them. A change to how
this crate migrates the database needs the same change there.
`layers.storage_access` in `ARCHITECTURE.toml` keeps rusqlite and
`pplx-space-common-jobs` out of `pplx-space-volmgr`'s normal and build
dependencies; dev-dependencies are not checked, so its tests may seed the
jobs table directly.

## Changes

- Outside the `test-support` feature, no public item may return, expose or
  re-export a rusqlite connection, transaction, statement, row or error
  value; the one exception is the rusqlite errors that `FromSql` and `ToSql`
  impls return by design, such as those on `JobId` and the re-exported
  `JobFailureKind` and `JobFailureReason`. Every function that takes the
  store's `Connection` fails with its `Error` or with an error that wraps it;
  `Error`'s `Display` and `source` are rusqlite's, unchanged. Raw rusqlite
  access stays `pub(crate)`.
- `database::open` keeps the bare `rusqlite::Connection::open`: rusqlite's
  default busy timeout and no PRAGMAs. volume-manager shares that one
  connection through `SharedConnection`, with its jobs worker too.
- volume-manager reaches the job queue's worker only through
  `jobs::JobWorker` (`src/jobs.rs`). A worker operation volume-manager does
  not call yet is a new forwarding method there that returns the worker's
  value or future unchanged; never re-export `BackgroundWorker`. A read of job
  rows that must run under the caller's held connection is a function here
  that forwards to the job queue's own (as `count_active_jobs` does) or a
  named census, never SQL at a call site.
- Re-export from `jobs` only job items that run no SQL and that
  volume-manager names, and list each in that module's doc.
- Add what volume-manager calls, when it first calls it; nothing
  speculative.
