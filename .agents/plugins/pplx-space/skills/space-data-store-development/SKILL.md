---
name: space-data-store-development
description: Develop, review, or diagnose SPACE PostgreSQL or SQLite storage code, queries, migrations, and database tests.
---

# Develop and inspect SPACE data stores

Read `db/README.md` before adding a migration. For Rust query construction, also
use `$space-rust-development`.

## Storage layers

Data operations go through the owning storage layer. Inline SQL at a call site
means one of two things: that data needs a storage crate/module, or an existing
one needs an API reused or extended. Prefer reuse; add a new API only when the
question or the latency budget genuinely differs. Migrating the inline
statements a PR touches onto the store is in scope for that PR.

Exemplar: `crates/libs/sandbox/store` (`pplx-space-sandbox-store`, the
`sandboxes` row — its crate docs carry the boundary rules). The shape it sets:

- Named verbs with typed arguments and typed row structs; CRUD with filters
  and limits is the default. No query builder and no `SELECT`-shaped
  arguments — callers pick an API, never pass columns, predicates, or sort
  orders.
- Executor-generic free functions (`impl PgExecutor`) so an operation composes
  into the caller's open transaction. A pool-owning store forces callers to
  choose between committing early and bypassing the store.
- `sql!` locators on every statement; small operation-specific `thiserror`
  enums. A guarded write that matches zero rows is an outcome
  (`Option`/`bool`), not an error — "another writer won" is a normal race
  result.
- Verbs and record columns are added with their first migrated caller, never
  speculatively, and `SELECT` lists stay explicit.
- Keep every SQL statement syntactically whole and reviewable at its query
  site. Do not build a statement by factoring out or interpolating partial SQL
  strings, including conditional predicates, joins, assignments, `ORDER BY`,
  or comma-separated column/value lists. If a small fixed set of query shapes
  is required, branch between complete statements and share only bound values
  and result-processing code. Use a typed query builder only when the query
  shape is genuinely open-ended; keep identifiers and operators allowlisted
  and values bound as parameters.
- One exception is a whole boolean predicate from
  `pplx_space_sandbox_portability`, the single definition of whether a
  sandbox's bytes are recoverable elsewhere. Interpolate it rather than
  re-spelling it: a second copy of that rule is how deletion paths and drain
  gates drift apart. Build the statement once (a `LazyLock<String>`), bind every
  value, and keep the rest of the statement whole at its query site.
- The other is the column list of one row type read from one table: a shared
  constant of that table's bare column names, with no expressions, aliases, or
  joined columns, matching one `FromRow` struct field for field. Interpolate it
  only as the entire `SELECT` list of a statement whose only `FROM` item is that
  table and which decodes that struct, each statement built once (a
  `LazyLock<String>`) at its query site with every value bound. It keeps a new
  column to one SQL edit, and no read falls back to `SELECT *`, whose cached
  statements fail with `0A000` once the table gains or loses a column. Nothing
  else is exempt: predicates, joins, `FROM` clauses, assignments, ordering, and
  any other column list stay whole at their query sites.
- A migration that changes a predicate's shape (`SELECT EXISTS(..)` to
  `get(..).is_some_and(..)`, a hand-rolled probe onto a shared fragment) pins
  the equivalence with a Postgres-backed test before it merges.

Not store verbs — leave these where they are:

- **Guarded state transitions**, where the `WHERE` re-checks a precondition
  (a lifecycle CAS, a lease-token or generation guard). The guard is the
  correctness mechanism; flattening one into a plain `update` re-opens the
  drift class where a pooled wake resurrected torn-down sandboxes. Sandbox
  placement transitions specifically belong to `pplx-space-placement`
  (see `crates/libs/placement/machine/src/lib.rs`).
- **`SELECT ... FOR UPDATE`**: a lock acquisition inside the caller's
  transaction. Converting it to a store read silently drops the lock.
- **Predicates evaluated against the database clock** (`NOW()`-relative
  freshness checks). Moving the comparison client-side changes clock
  authority.

When a store change adds a workspace crate or dependency edge, run a lib-only
`cargo check -p <crate>` — `--all-targets` pulls dev-dependencies into feature
unification and can mask a missing cargo feature on the crate itself. Release
artifact digests read the crate graph from the workspace manifests, so no
input list needs updating.

## PostgreSQL migrations

- Squawk is a generic safety lint, not proof that a migration is safe for SPACE
  production traffic.
- `sandboxes` is hot. Do not place a whole-table `UPDATE`, `DELETE`, or
  `INSERT ... SELECT` against it in a SQLx startup migration, and do not combine
  DDL with a data backfill. Use a bounded job or maintenance runbook.
- Ordinary SQLx migrations are transaction-wrapped. An index requiring
  `CREATE INDEX CONCURRENTLY` must be a single-statement non-transactional
  migration with the Squawk transaction override.
- Do not add partial indexes in either up or down migrations. Use a non-partial
  index or redesign the access path.
- Enforce value sets that grow with the code in the application, not in
  `CHECK` constraints. A `CHECK` that lists an enum's wire values duplicates the
  Rust type, and every new variant then needs its own migration that has to
  deploy before the code that writes it. When a change would widen such a
  `CHECK`, drop it instead, but only when all of these hold:
  - Every production write path binds the column through one closed enum
    (`wire_enum!` or `wire_enum_open_schema!`; `open_wire_enum!` types carry an
    `Other(String)` and don't qualify) whose values, apart from the ones this
    change adds, are exactly the set the `CHECK` admits. A wider enum that a
    caller filters doesn't qualify; the `CHECK` is what narrows it.
  - Nothing in production writes a non-NULL value around that enum: no SQL
    literal, `CASE` result, column default, or trigger.
  - The `CHECK` reads only that column. One that relates several columns of a
    row stays: concurrent updates that each change one column can combine into
    a row no single update validated.
  - If the `CHECK` also enforces a rule that doesn't grow, such as
    `cardinality(col) > 0`, keep that part as its own `CHECK`.

  The drop's down migration must refuse while rows hold values outside the
  restored list, because a `NOT VALID` `CHECK` still evaluates every later
  update to them, including `ON DELETE SET NULL` cascades. Follow
  `db/postgres/20261016013700_drop_public_operations_kind_check.down.sql`.
  Keep `CHECK`s for ranges, formats, and other rules that don't grow, along
  with primary keys, `UNIQUE`, foreign keys, and `NOT NULL`.
- Prefer explicit statements and Rust-managed transactions over dense CTE
  chains. Generate application-owned identifiers and labels in application code.
- Use `space_uuid_v7()` for database-owned UUIDs. A UUIDv4 exception requires
  `/* space-allow-uuid-v4: <reason> */` on the same line and must protect a real
  privacy or protocol property.
- New Postgres migration versions use a UTC `YYYYMMDDHHMMSS` prefix newer than
  the base branch's maximum. Legacy sequential numbering ends at `093`.

## Tests

Use the shared database fixtures. Nextest uses one run-scoped Postgres and
isolated clones; direct `cargo test` can use a process-local testcontainer.
Unavailable Postgres or Redis fails the test unless the run opts out with
`SPACE_TEST_DEPS_OPTIONAL=1`. Opted-out skips are diagnostic only, not
acceptable final validation when Docker can be started.
