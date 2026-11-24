# `pplx-space-template-mgr-store`

template-manager's node-local SQLite store: every template-manager table and
the shared `jobs` table (`db/common`) in template-manager's own database, not
the control-plane Postgres. Outside tests, template-manager opens, migrates
and queries that database only through this crate, and the common-jobs
worker this crate builds runs the job queue's statements on the same
connection. `pplx-space-db-migrate` also migrates and resets the database,
with its own copy of the `db/template-manager` and `db/common` migrations; a
change to how this crate migrates it needs the same change there.
`layers.storage_access` in `ARCHITECTURE.toml` keeps rusqlite out of
`pplx-space-template-mgr`'s normal and build dependencies; dev-dependencies
are not checked, so its tests may seed and inspect rows directly.

## Changes

- Outside the `test-support` feature, no public item may return, expose or
  re-export a rusqlite connection, transaction, statement, row or error
  value; the one exception is the rusqlite errors that `FromSql` and `ToSql`
  impls return by design, such as those on the `JobId` that `JobWorker`'s
  methods return. Callers name `Connection`, `Transaction`,
  `SharedConnection`, `ConnectionGuard` and `Error`, whose `Display` and
  `source` are rusqlite's, unchanged; the rusqlite values they wrap stay
  `pub(crate)`. Where template-manager reports
  a failure of its own as a database error, it builds it with an `Error`
  constructor, so its text stays the same.
- `database::open` keeps `pplx-space-sqlite`'s default rusqlite open: the
  default busy timeout, WAL and `synchronous=NORMAL`. template-manager shares
  that one connection through `SharedConnection`, with its jobs worker too.
- No function outside `database` and the handle types opens a connection or a
  transaction. template-manager opens each transaction with
  `Transaction::immediate` and keeps its boundaries; a function whose
  statements are only correct together takes a `Transaction`.
- template-manager reaches the job queue's worker only through
  `jobs::JobWorker` (`src/jobs.rs`), built by
  `SharedConnection::background_worker`. A worker operation template-manager
  does not call yet is a new forwarding method there that returns the
  worker's value or future unchanged; never re-export `BackgroundWorker`.
  Re-export from `jobs` only job items that run no SQL and that
  template-manager names, and list each in that module's doc.
- Add a verb, handle method, worker method or re-export with its first caller
  in template-manager, never speculatively.
