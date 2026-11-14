# `pplx-space-sbx-mgr-store`

sbx-mgr's node-local SQLite store: `sandboxes` and every other sbx-mgr table,
not the control-plane Postgres. Only this crate opens, migrates or queries that
database. `layers.storage_access` in `ARCHITECTURE.toml` keeps rusqlite and
the shared path-taking helpers in `pplx-space-sqlite` out of the normal and
build dependencies of every sbx-mgr crate but its stores, this one and
`pplx-space-sbx-mgr-migrations`; dev-dependencies are not checked. As an
`l30_storage` crate, the migrations crate may take rusqlite,
`libsqlite3-sys` and `pplx-space-sqlite`: CI's `run_sbxmgr_production_checks`,
which asserts which crates bring rusqlite and `pplx-space-sqlite` into the
server's normal production graph, rejects a normal dependency on either
there, but not a build or dev dependency, nor `libsqlite3-sys`.

## Changes

- Outside the `test-support` feature, no public item may return, expose or
  re-export a rusqlite connection, transaction, statement, row or error
  value; the one exception is the `FromSqlError` that `FromSql` impls return
  by design. A method that hands one out is declared through
  `db_private_outside_tests!` (`src/db/handle.rs`), which makes it public only
  with `test-support`.
- Code outside the store reads through `db::read` (one record or a narrow
  view of it) and `db::scan` (named many-row scans). Extend those before
  adding a public reader; the readers they consolidate are declared through
  `db_private_outside_tests!`, so only tests outside the store still call them.
- Write `sandboxes.state` only through `update_state_from` with an explicit
  edge, or through a named specialized writer. A new edge in
  `src/db/state_write.rs` needs a kind and a doc comment naming its writer;
  a writer that hands a row to cleanup or records its failure is a cleanup
  edge, not a lifecycle edge. Add any new disagreement with
  `can_transition_to`, with a reason, to `ALLOWLIST` in
  `src/db/tests/state_write_table.rs`. Nothing checks the table
  against the writers; the `&'static Edge` argument does that at call sites. A
  refused write must leave every row unchanged.
- `sandboxes.state` holds lifecycle states only; `cleanup_state` (`required`,
  `failed`) records a cleanup that owns the row, over the teardown transition
  its intent names. The triggers allow a cleanup only over `stopping`,
  `deleting` or `suspending`. Decide a lifecycle predicate with the generated
  `unowned_state`, which is `NULL` while a cleanup owns the row, so a cleanup
  never matches it. `state` also holds a cleanup's teardown transition: read
  it to name the stored lifecycle, or in a predicate on a lifecycle no cleanup
  can be over, as the indexed `suspended` scans do. Compare `unowned_state`
  and `cleanup_state` with `IS` or `IS NOT` where a `NULL` must not turn the
  result unknown, as under `NOT`. A write of a lifecycle state clears
  `cleanup_state`, and a cleanup write sets both in one statement; the
  triggers in `db/sandbox-manager/20261105142930_sandbox_cleanup_state.up.sql`
  reject any other shape. The generated `legacy_state` is gone:
  `20261109120000` dropped it, `20261113190000` restored it while the
  rollback target still read it, and `20261114020000` drops it again.
- Only some child tables declare `ON DELETE CASCADE`: register a new sandbox
  child table in `delete_sandbox_child_rows` (`src/db.rs`).
- Fixtures that sbx-mgr's other test suites share live in `db::fixture` and
  `db::test_fixtures`, behind `test-support`; fixtures only this crate's tests
  use stay in the test modules.

Module moves change log targets: see "Log targets" in
`docs/rust/space-conventions.md`.
