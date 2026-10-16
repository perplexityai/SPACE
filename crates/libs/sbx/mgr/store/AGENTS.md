# `pplx-space-sbx-mgr-store`

sbx-mgr's node-local SQLite store: `sandboxes` and every other sbx-mgr table,
not the control-plane Postgres. Only this crate opens, migrates or queries that
database. `dependency_fence.sbxmgr_rusqlite` in `ARCHITECTURE.toml` keeps
rusqlite and the shared path-taking helpers in `pplx-space-sqlite` out of
every other sbx-mgr crate, and CI's `run_sbxmgr_production_checks` asserts
that nothing else in the server's production graph brings them in.

## Changes

- Outside the `test-support` feature, no public item may return, expose or
  re-export a rusqlite connection, transaction, statement, row or error
  value; the one exception is the `FromSqlError` that `FromSql` impls return
  by design. A method that hands one out is declared through
  `db_private_outside_tests!` (`src/db/handle.rs`), which makes it public only
  with `test-support`.
- Write `sandboxes.state` only through `update_state_from` with an explicit
  edge, or through a named specialized writer. A new edge in
  `src/db/state_write.rs` needs a kind and a doc comment naming its writer;
  add any new disagreement with `can_transition_to`, with a reason, to
  `ALLOWLIST` in `src/db/tests/state_write_table.rs`. Nothing checks the table
  against the writers; the `&'static Edge` argument does that at call sites. A
  refused write must leave every row unchanged.
- Foreign keys are off: register a new sandbox child table in
  `delete_sandbox_child_rows` (`src/db.rs`).
- Fixtures that sbx-mgr's other test suites share live in `db::fixture` and
  `db::test_fixtures`, behind `test-support`; fixtures only this crate's tests
  use stay in the test modules.

Module moves change log targets: see "Log targets" in
`docs/rust/space-conventions.md`.
