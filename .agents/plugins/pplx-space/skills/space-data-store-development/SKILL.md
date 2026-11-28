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

Exemplar: `crates/l30_storage/sandbox/store` (`pplx-space-sandbox-store`, the
`sandboxes` row — its crate docs carry the boundary rules). The shape it sets:

- Named verbs with typed arguments and typed row structs; CRUD with filters
  and limits is the default. No query builder and no `SELECT`-shaped
  arguments — callers pick an API, never pass columns, predicates, or sort
  orders.
- Free functions on `pplx-space-postgres-pool`'s handles so an operation
  composes into the caller's open transaction. A pool-owning store forces
  callers to choose between committing early and bypassing the store. A verb
  that runs one statement on whatever the caller passes, the pool or its
  transaction, takes `impl Executor<'_>` and runs it on
  `executor.into_sqlx()`, which consumes the executor. A verb that runs
  several statements takes `impl Executor<'_> + Copy`, which only a pool
  satisfies, when each statement may run on whichever connection the pool
  hands out, as `pplx-space-firewall-rules-store`'s `read_adopted` does.
  When its statements must run in the caller's transaction, for example to
  read one snapshot of a `db.begin_snapshot()` transaction or to see the
  caller's uncommitted writes, it takes the caller's transaction in the form
  the next item gives and runs each statement on `(&mut *tx).into_sqlx()`, as
  that store's `fetch_instructions` does.
- A verb whose lock the caller relies on after the statement, that runs under
  a lock the caller took, or that must commit together with another of the
  caller's statements takes the caller's transaction: `tx: &mut C` where
  `for<'a> &'a mut C: Executor<'a>`, which accepts a `Tx` or a
  `PgConnection` and refuses a pool. The bound cannot prove a transaction is
  open, so the verb's doc says the caller must pass its open transaction. Take
  `&mut Tx` instead where every caller already holds one. A verb that opens
  its own transaction takes `&Db` and calls `db.begin()` (or
  `db.begin_snapshot()`).
- No sqlx type in a store's public signatures, public fields or public type
  aliases; sqlx stays the store's own dependency. A Storage crate depends on
  `pplx-space-postgres-pool` without a `[rules.layers.store_users]` entry
  (archcheck's `[rules.layers] storage_handles`). A JSON column or argument
  crosses the API as its plain Rust type (the typed document or
  `serde_json::Value`): public rows, verb arguments and return types use
  `T`, `Option<T>`, `&T`, `Vec<T>` or `&[T]`, and `sqlx::types::Json`
  appears only inside the store. There it keeps the shape of the SQL value:
  `Json<T>` for one document, `Option<Json<T>>` where the value can be SQL
  NULL, `Json<Option<T>>` where a `NOT NULL` column stores `None` as JSON
  `null`, and `Vec<Json<T>>` for a `JSONB[]`. Decode a document field of a
  `FromRow` row with `#[sqlx(json)]` only when its type is not an `Option`:
  the attribute decodes `Json<field type>`, which fails on SQL NULL. Decode
  anything else as its shape, in a private row or at the `query_scalar` or
  `try_get`, and map `.0` (per element for a `JSONB[]`). Don't use
  `#[sqlx(json(nullable))]` where the column can hold JSON `null`: it
  decodes a stored JSON `null` as `None`, like SQL NULL. A
  `serde_json::Value` or `Option<serde_json::Value>` field or argument may
  instead decode and bind as itself, with no `Json`, the `Option` form only
  where `None` is SQL NULL (it binds `None` as SQL NULL and decodes a stored
  JSON `null` as `Some(Value::Null)`): sqlx's `Value` impls go through
  `Json`, as `SandboxNetworkPolicyInputs` shows. Bind a document as
  `Json(&value)`, including an optional value that a `NOT NULL` column
  stores as JSON `null`; an optional value of a nullable column as
  `value.as_ref().map(Json)` (`Json(&None)` stores JSON `null`, not SQL
  NULL); and a `JSONB[]` as `values.iter().map(Json).collect::<Vec<_>>()`
  (`Json(&values)` binds one document holding an array, as
  `jsonb_to_recordset($1::jsonb)` reads). A `JSONB[]` whose elements can be
  SQL NULL is a `Vec<Option<Json<T>>>`, each element bound as
  `v.as_ref().map(Json)`: `.map(Json)` turns a `None` into a JSON `null`
  element, and `Json<T>` fails to decode a SQL NULL element.
- `sql!` locators on every statement. Verbs return `DbError`, or a small
  operation-specific `thiserror` enum with a variant that wraps `DbError`
  (usually `#[from] DbError`); callers classify a failure with
  `DbError::kind()`, `constraint()` and `is_lock_contention()`, never by
  matching the driver's error. A failure a caller must tell apart that
  `kind()` reports as `Other`, such as an I/O, decode or closed-pool error,
  first gets its own `DbErrorKind` in `pplx-space-postgres-pool`. A guarded
  write that matches zero rows is an outcome (`Option`/`bool`), not an error
  — "another writer won" is a normal race result.
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
- Another is a whole fragment from `pplx_space_sandbox_residency`
  (`occupies_node_predicate`, `resident_lifecycle`,
  `non_resident_lifecycle_exclusion` and its other gates), the single
  definition of whether a sandbox row still occupies its node for node-release
  decisions. Interpolate it as built rather than re-spelling its lifecycle
  sets: a drain gate and the drained-node terminator that disagree park a node
  in `drained`. Build the statement once, bind every value, and keep the rest
  of the statement whole at its query site.
- Another is a whole expression about node environment evidence:
  `pplx_space_node_status_store`'s `environment_evidence` and
  `environment_refusal`, the single definition of whether a node's published
  environments count as evidence, and
  `pplx_space_runtime_environment_catalog_store::node_certifies_upgrade`,
  whether a node's own build certifies an upgrade edge, for the upgrade path
  that will call it. Interpolate them
  rather than re-spelling the freshness or certification rules: placement
  and the upgrade decision have to agree on which nodes can run what. Build
  the statement once, bind every value, and keep the rest of the statement
  whole at its query site.
- Another is
  `pplx_space_sandbox_teardown::observed_states::publish_carries_execution`,
  the single definition of whether a sandbox's published execution is evidence
  of what it runs. Interpolate it rather than re-spelling it: a start's read of
  the published execution and a runtime upgrade campaign's candidate and
  subject reads have to agree on which sandboxes are on an environment. Build
  the statement once, bind every value, and keep the rest of the statement
  whole at its query site.
- Another is `pplx_space_template_release_store::resolved_release_sql`, the
  single definition of which release of a template build a create runs (the
  default-bound one, else a twin in an allowed then a deprecated environment,
  else the original, never a blocked one). Interpolate it rather than
  re-spelling the order: create resolution, pod placement, pool claims, the
  warm-pool autoscaler and the environment block guard have to agree on which
  release serves. Build the statement once, bind every value, and keep the
  rest of the statement whole at its query site.
- Another is a whole predicate from `pplx_space_template_release_original`
  (`spelling_original`, `digest_original`), the single definition of which
  release of a template digest is its original. Interpolate it rather than
  re-spelling the ordering: callers that disagree register, tag, create and
  warm against different releases of one digest. Build the statement once,
  bind every value, and keep the rest of the statement whole at its query
  site.
- Another is the column list of one row type read from one table: a shared
  constant of that table's bare column names, with no expressions, aliases, or
  joined columns, matching one `FromRow` struct field for field. Interpolate it
  only as the entire `SELECT` list of a statement whose only `FROM` item is that
  table and which decodes that struct, each statement built once (a
  `LazyLock<String>`) at its query site with every value bound. It keeps a new
  column to one SQL edit, and no read falls back to `SELECT *`, whose cached
  statements fail with `0A000` once the table gains or loses a column. Nothing
  else is exempt: predicates, joins, `FROM` clauses, assignments, ordering, and
  any other column list stay whole at their query sites, except as the next
  item allows.
- The last is a store-private projection and join for one family of reads: a
  store module may keep one constant of the select list (columns and
  expressions across joined tables, each named for a field of one `FromRow`
  struct) and one constant of the joins that list needs, lateral subqueries
  included, used only by that module's own statements. Each such statement
  decodes that struct, directly or through a `#[sqlx(flatten)]` field of a
  row that adds the statement's own columns after the list, such as a page's
  `has_next`. Keep both constants private to the module (no `pub` of any
  kind) and build each statement once (a `LazyLock<String>`) from them plus
  everything else written whole in that statement, or interpolated as an item
  above allows: its CTEs, the `FROM` items the joins follow, its predicates,
  any keyset condition or page walk, its ordering and its limits. Bind every
  value. Nothing else builds part of these statements, inside the module or
  out of it. It exists for read families that share a wide multi-table
  projection, such as the sandbox listing and fetch reads, so a column change
  stays one edit.
- A migration that changes a predicate's shape (`SELECT EXISTS(..)` to
  `get(..).is_some_and(..)`, a hand-rolled probe onto a shared fragment) pins
  the equivalence with a Postgres-backed test before it merges.

Not plain store verbs. Leave these where they are unless an item below says
how one may move. A statement that fits several items moves only if each
item it fits allows it: the guarded-transition item decides whether and
where a guarded statement moves, and the lock and clock items say what any
move must keep.

- **Guarded state transitions**, where the `WHERE` re-checks a precondition
  (a lifecycle CAS, a lease-, claim-token or generation guard). The guard is
  the correctness mechanism; flattening one into a plain `update` re-opens
  the drift class where a pooled wake resurrected torn-down sandboxes. A
  guarded transition moves into the store that owns its table only in this
  form:
  - the statement verbatim, with its guard whole and bound to the values
    the caller decided on, never values the verb reads for itself;
  - a verb named for the transition (`release_claim`, not `update_row`);
  - the outcome returned (`bool`, `Option`, the affected count, or the rows
    its `RETURNING` lists): matching no row is a normal outcome, such as
    another writer winning or nothing left to do, never an error;
  - a store test that pins the losing case, where the guard matches no row
    and nothing changes.

  The caller keeps its transaction, its locks, and the decision of what to do
  with each outcome. A guarded statement that writes two owners' tables
  splits into one such verb per store, which a coordinator runs, in the
  caller's open transaction when the statement was one step of it (see
  Coordinators). One kind stays with the path that issues it: node drain's
  phase compare-and-sets, as `pplx-space-node-registration-store`'s docs say.
  Other store docs that say a guarded write stays at its call site do not
  override this item; the change that moves the write updates them. The
  `sandboxes` row's guarded transitions live in `pplx-space-sandbox-store`,
  in modules named for the transition or for the path that issues them (as
  `lost` and `delete` are), and its canonical hard delete is that crate's
  `delete::delete_with_dependents`. A write that is one step of such a
  transition stays in its module even when its own `WHERE` checks only the
  id, because the caller's locks and re-checks guard it (as with
  `delete::mark_deleting`). A step that writes a table another store owns can
  instead be a verb of that store, run in the caller's transaction with its
  guard whole, as `pplx-space-snapshot-store`'s
  `lost::fail_quiet_full_uploads_from_node` is for the lost give-ups. Such a
  step, run in the transaction whose own guarded statement already decided
  the transition, may return nothing, as that verb does. Other unguarded
  writes of the row, such as its desired-configuration columns, belong in
  ordinary verbs like `update_desired_state`. The caller keeps its locks and
  transaction (see the crate docs).
- **Locks the caller relies on after the statement.** A lock taken by a
  standalone `SELECT ... FOR UPDATE` (or `FOR NO KEY UPDATE`, `FOR SHARE`,
  `FOR KEY SHARE`), a `LOCK TABLE`, or a transaction-level advisory lock
  (`pg_advisory_xact_lock` and its `try_` and `_shared` forms) lasts until
  the caller's transaction ends, or until a rollback to a savepoint taken
  before it. A store read that drops the clause takes no lock; on a pool the
  lock ends with the statement, and `LOCK TABLE` fails. Such a statement may
  still move as a store verb, under these conditions:
  - the statement moves verbatim, lock clause included (`OF …`, `NOWAIT`,
    `SKIP LOCKED`), along with any setting written in the same statement;
  - the verb takes the caller's open transaction, as `tx: &mut C` where
    `for<'a> &'a mut C: Executor<'a>`, or `&mut Tx` where every caller
    already holds one, and its doc says the caller must pass its open
    transaction. It never takes `impl Executor<'_>`, which also accepts a
    pool;
  - its name says it locks (`lock_…`, `share_lock_…`, `…_locked`), and its
    doc says the lock lasts until the caller's transaction ends, as
    `pplx-space-runtime-environment-policy-store`'s `lock_generation` and
    `pplx-space-runtime-upgrade-campaign-store`'s `unfinished_from_locked`
    do.

  Settings that only serve the lock, such as
  `set_config('lock_timeout', …, true)` or `SET LOCAL lock_timeout`, stay
  with the caller that owns the transaction. A lock that serves only its own
  statement, such as a `FOR UPDATE` CTE or subquery that feeds the same
  statement's `UPDATE` or `DELETE`, needs no caller transaction, and its
  verb may take `impl Executor<'_>`, as `pplx-space-sandbox-store`'s
  `restore_lease::claim_user_restore` does. A session-level advisory lock
  (`pg_advisory_lock`) outlives the transaction; leave it with the code that
  unlocks it.
- **Database-clock predicates.** A `NOW()`-relative freshness check decides
  on the database clock, and computing it client-side changes clock
  authority. The statement may still move, verbatim, with the comparison
  left in its SQL, and each caller keeps the executor it used: `NOW()` and
  `CURRENT_TIMESTAMP` are the transaction's start time, so the same
  statement run on a pool or in a later transaction compares against a later
  instant. The verb binds the same values the caller bound (an interval, a
  grace period), never a client timestamp in place of `NOW()`.

When a store change adds a workspace crate or dependency edge, run a lib-only
`cargo check -p <crate>` — `--all-targets` pulls dev-dependencies into feature
unification and can mask a missing cargo feature on the crate itself. Release
artifact digests read the crate graph from the workspace manifests, so no
input list needs updating.

### Coordinators

A coordinator (`crates/l30_storage/<area>/coordinator`,
`pplx-space-<area>-coordinator`) runs one transaction across two or more
stores: it calls each store's verb for that store's own rows and keeps which
rows change together and in what order they are locked. When the split
statement is one step of a transaction a service already owns, such as
internal-api's node-state publish, the coordinator's verb takes that open
transaction, and its docs say which locks the caller must already hold;
otherwise the coordinator opens and commits the transaction itself. It
reaches Postgres through `pplx-space-postgres-pool` alone: it takes the
caller's open transaction as `&mut pplx_space_postgres_pool::Tx`, or opens
its own with `Db::begin` and ends it with `Tx::commit`. It passes the
transaction to each store verb (`&mut tx` for one it opened, `&mut *tx` for
one it was given). A verb that runs under a lock an earlier step took, that
must commit together with the other steps, or whose lock the caller relies on
after the statement takes the caller's transaction (see the store shape and
"Locks the caller relies on after the statement" above); any other takes
`impl Executor<'_>`. So neither the coordinator nor its callers name a driver
type. While its caller still opens the transaction with the driver and passes
`&mut *tx`, the verb takes any `&mut C` where
`for<'a> &'a mut C: Executor<'a>`, documents that the caller must pass an open
transaction, and narrows to `Tx` once the caller moves. The caller decides what
to write and when, passes that in, and gets each guarded
verb's outcome back to act on. Make one only when one of
these holds:

- a statement writes two owners' tables (today through a co-writer grant),
  including a guarded transition that spans stores: split it into one verb
  per store and run them from the coordinator. Each verb that keeps a guard
  takes the guarded-transition form above, its guard whole and bound to the
  caller's values, named for the transition, its outcome returned and its
  losing case pinned by a store test. The split takes the guard or row lock
  the old statement relied on first. The steps that follow it may return
  nothing, as that item allows, and run only if the guard matched. A
  Postgres-backed test shows the split keeps the old statement's
  all-or-nothing result, losing case included;
- two or more services run the same multi-store transaction;
- a cross-store invariant or lock order needs enforcing in one place.

A statement in one store that writes only another store's table needs no
coordinator: it moves into the owning store as a verb, which the first store
calls as one of its users.

Otherwise the service opens its own transaction and calls the stores.

Each coordinator documents its lock order in its crate docs: which rows it
locks, in which order, and why. It owns no tables and has no normal or build
dependency on a driver, storage helper or codec facade, which archcheck's
`layers` rule enforces, as it does the `[rules.layers.coordinator_users]`
list in `ARCHITECTURE.toml`. It holds no SQL either; archcheck reports only a
write statement in it that names a table a store owns
(`layers.state.writes`) and checks no other SQL. No long
external I/O (network calls, object storage, waits on another service) runs
while its transaction is open. archcheck does not check for it.

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

A statement on a hot path that matches rows of a table growing per template,
node or org, such as releases, against other rows of it needs a plan-cost
test: seed one owner with a few hundred rows among other owners' rows, so the
tables are too large to rescan cheaply, and bound the per-row work its plan
does under both plan modes, through `pplx_space_db_test_fixtures::plan`
(its docs say why both), on the statement text the store runs. Bound how much
more the plan touches at a fixture several times larger too, which catches an
index walked per row. Show the bounds catch the regression by mutating the
statement, such as wrapping the probed column, and say so in the pull
request; a check that drops an index breaks once another index serves the
probe. `crates/l30_storage/template/release/store/src/plan_tests.rs` is an
example.
