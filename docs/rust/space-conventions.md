# SPACE Crosscutting Conventions

Choices we've made for SPACE that aren't in the canonical Rust books because they're SPACE-specific. Each is here because we want every new crate / every new module to pick the same one.

## Identifiers: UUIDv7, not UUIDv4

Use `uuid::Uuid::now_v7()` for any new SPACE-owned durable identifier, unless
there is a concrete privacy or protocol reason to use v4, such as needing to
hide creation time across a tenant boundary. Postgres-owned identifiers use
`space_uuid_v7()`; see [`db/README.md`](../../db/README.md) for migration rules and the reviewed
UUIDv4-exception marker.

Why v7:

- **Time-local.** v7 embeds a millisecond timestamp in the high bits, improving
  B-tree locality and giving coarse chronological order. It is not authoritative
  ordering across generators, clock skew, or multiple values from one
  millisecond; use an explicit timestamp when exact order matters.
- **Faster to generate** than v4 in the typical case (no full CSPRNG read for the timestamp portion).
- **Easier to prune / archive.** Range deletes on the id column work; with v4 you need a separate `created_at`.
- **Still globally unique.** Random-style UUIDv7 generators have 74
  non-timestamp bits.

```rust
let sandbox_id = uuid::Uuid::now_v7();
```

Tokens, nonces, UUIDv5 namespaces, stable fixtures, and external identifiers are
not covered by the durable-identifier rule. UUIDv4 is allowed for identifiers
only when the creation-time disclosure of v7 would violate a concrete privacy
or protocol requirement. Do not rewrite existing primary and foreign keys only
to change their UUID version; change future generation with an additive
migration.

## Cryptography: AWS-LC, not ring

Use `aws-lc-rs` for cryptographic primitives and as the crypto provider for
rustls and crates built on it. Do not add a direct `ring` dependency or enable a
dependency's `ring` / `tls-ring` feature. Select its AWS-LC feature instead,
including `aws_lc_rs` for rustls and rcgen, `tls-aws-lc` for tonic, and
`verify-aws` for x509-parser.

Services install the process-wide rustls provider through
`pplx_space_rustls_provider::install_default_rustls_crypto_provider`
(re-exported from `pplx-space-common-svc`); downstream
crates must not attempt to install a competing provider. A transitive dependency
that offers no provider choice may retain ring temporarily, but record that
constraint rather than adding SPACE-owned ring usage around it. Changing the
standard provider requires security review and interoperability tests. The
current `aws-lc-sys` configuration is not a claim that SPACE runs an AWS-LC FIPS
build.

## Time: `jiff`, not `chrono` / `time`

Use [`jiff`](https://docs.rs/jiff) for all wall-clock and calendar time in SPACE. `jiff` is the modern, Temporal-aligned library: typed instants, zoned datetimes, durations, parsing that doesn't lose information. `chrono` and `time` predate Temporal and have well-known footguns around timezone handling, leap seconds, and serialization round-trips.

```rust
use jiff::{Timestamp, Zoned, Span};
let now = Timestamp::now();
let in_us_pacific = now.in_tz("America/Los_Angeles")?;
```

For protobuf wire formats, keep using `google.protobuf.Timestamp` / `Duration`; convert at the boundary into `jiff::Timestamp` / `jiff::Span` for in-process arithmetic. Do **not** sprinkle `chrono::DateTime<Utc>` into new code.

If you're touching a file that already uses `chrono` and changing the time-handling logic, it's a good moment to migrate that surface to `jiff`; don't force a megacommit, but don't add more `chrono` either.

## Platform dependencies: optional features, not unconditional `cargo.toml` entries

SPACE is multi-platform. Each crate should build by default without dragging in AWS SDKs, Smithy, kube-rs, Datadog, OpenTelemetry, Snowflake, etc. Those are *integrations*, not base requirements.

Pattern:

```toml
[features]
default = []
aws = ["dep:aws-sdk-s3", "dep:aws-config"]
kube = ["dep:kube", "dep:k8s-openapi"]
datadog = ["dep:datadog-statsd"]
otel = ["dep:opentelemetry", "dep:opentelemetry-otlp"]

[dependencies]
aws-sdk-s3 = { version = "...", optional = true }
aws-config = { version = "...", optional = true }
kube = { version = "...", optional = true }
# ...
```

And the integration code lives behind:

```rust
#[cfg(feature = "aws")]
mod s3;
```

Rules of thumb:

- A new vendor SDK lands as `optional = true` + a named feature.
- The default-features set of a crate must build on every platform we ship to.
- Tests for integration code use `#[cfg(feature = "...")]` and run with `--features ...`.
- An app crate (one that produces a binary) explicitly turns on the features it actually needs. Don't enable `aws` everywhere "just in case".

The point: you should be able to build the cluster-agnostic core of SPACE without an AWS toolchain, without `kube`, without Datadog. Every unconditional heavy dep that creeps into a base crate is a compile-time tax on every developer and every CI run.

## Logging / tracing: `tracing` everywhere, `tracing-subscriber` only in apps

- **Libraries (`crates/libs/**`) use the `tracing` crate.** Emit spans and events; never configure a global subscriber from a library. Never use `log`, `slog`, `env_logger`, or `println!` for diagnostic output in library code.
- **Apps (`crates/apps/**`) own the subscriber.** They call into our common helpers for service / CLI runtime to set up subscribers consistently. Those helpers decide when to send to `journald`, when to send to Datadog, when to render to a terminal, what JSON layer to use in production, etc.
- **Choosing the output sink is the app's job, not the library's.** A library that prints to stderr is broken in a service that wants structured JSON to Datadog and broken in a CLI that wants pretty terminal output.

Span discipline (see `docs/rust/space-review-lessons.md` § 11 for the full lesson):

- Use `#[tracing::instrument(skip(self, ...), err)]` on functions whose failure mode is interesting.
- For functions that propagate errors via `?` and want a span attribute set, either record on the error path explicitly or use a helper that records on `Drop`.
- For gRPC, use `tower_http::trace::TraceLayer::new_for_grpc()` — only the layer sees trailers.

Field discipline:

- Structured fields, not formatted strings. `tracing::info!(sandbox_id = %id, "claimed")`, not `tracing::info!("claimed sandbox {id}")`.
- High-cardinality identifiers (sandbox id, request id) go in fields, never in the message template.
- Error events use `error = %err` for the human form and `error.debug = ?err` only when the `Debug` form carries information the `Display` form doesn't.

## Concurrent maps: `DashMap`, not `Mutex<HashMap<_, _>>`

Default to `dashmap::DashMap<K, V>` for shared maps. It shards internally; lookups and inserts on different keys don't contend.

Reach for `Mutex<HashMap<_, _>>` or `RwLock<HashMap<_, _>>` only when you genuinely need a single guard held across *multiple* map operations — and at that point think about whether the right shape is an actor (owned state behind an `mpsc`) instead of a shared mutex. See `docs/rust/async-and-tokio.md` § Actor pattern.

## SQL statements: wrap in the `sql!` locator macro

Every SQL statement handed to `sqlx::query*` is wrapped in
`pplx_space_common_core::sql!` (defined in `pplx-space-sql-locator`;
re-exported from `pplx-space-common-svc`),
which prepends a `/*module_path:line*/` comment to the statement:

```rust
use pplx_space_common_core::sql;

// Static SQL: expands to a `&'static str` via `concat!` — drop-in.
sqlx::query(sql!("SELECT drain_phase FROM nodes WHERE id = $1"))

// Runtime-built SQL: expands to a `String`; wrap in `AssertSqlSafe` as usual.
sqlx::query(sqlx::AssertSqlSafe(sql!(format!("SELECT {cols} FROM sandboxes"))))
```

The comment makes `pg_stat_activity`, Postgres slow-query logs, and Datadog DBM
samples attribute an in-flight statement back to the Rust call site that issued
it, instead of leaving you to grep the workspace for a normalized query shape.
The locator leads the statement (so `track_activity_query_size` truncation
can't drop it) and contains no whitespace (so it consumes one word of sqlx's
word-budgeted query summaries). The observability layer strips it when deriving
OTel span names (`crates/libs/observability/src/sqlx_otel.rs`), so Datadog APM
resource names keep grouping by query shape, not by call site.

SQL kept in a `const` or a `fn() -> &'static str` helper gets wrapped at the
definition, where the SQL text lives. Don't route a caller's SQL through a
wrapper that applies `sql!` for it: every statement would carry the wrapper's
locator. The `execute` helper in
`crates/libs/db/migrator/src/bootstrap_roles.rs` is the known exception, not a
precedent. Node-local `rusqlite` call sites are not annotated today.

### Substrate values are bound parameters

Control-plane Postgres `sandboxes.managed_by` and `nodes.managed_by` hold the
substrate that owns a row, `'k8s'` or `'space'`.
`pplx_space_common_core::types::SubstrateKind` (an alias of `NodeManagedBy`)
names that value and encodes as the column's text, so new code binds it as a
parameter instead of spelling the literal:

```rust
use pplx_space_common_core::{sql, types::SubstrateKind};

sqlx::query_scalar::<_, NodeId>(sql!(
    "SELECT id FROM nodes WHERE drain_phase = $1 AND managed_by = $2"
))
.bind(PHASE_DRAINING)
.bind(SubstrateKind::Kubernetes)
```

The statement stays syntactically whole, as the store rules require: new code
writes whole statements with bound values, not fragments around the value.
The started- and failed-restorable repair `row_state_guards` helpers and the
shared `routable_execution_or_non_k8s` routability fragment
(`crates/libs/sandbox/routability/src/lib.rs`) take their
placeholder as an argument only because they are existing fragments; they are
not a pattern for new code. Read a `managed_by` column back as `SubstrateKind`
rather than comparing strings. Where one bad row must not fail the batch, read
it as text and parse it per row, as `orphaned_restore_pod_gc` does: a typed
decode fails the whole `fetch_all` on an unrecognized value. Test fixtures may
write literal `managed_by` values into the rows they set up; the rule covers
the statements under test.

Every crate's production SQL binds the value, and production Rust doesn't
compare the label string or parse the running substrate's `managed_by()` to
decide behavior (the substrate `AGENTS.md` names the one SQL-scope exception).
archcheck's `pattern_ratchet.substrate_literals` rule (`ARCHITECTURE.toml`)
catches the common forms in production code: SQL comparisons of `managed_by`
with `'k8s'` or `'space'`, and Rust comparisons, label-string matches and match
arms on `"k8s"` or `"space"` beside a substrate name. It is a line matcher and
can miss a literal; its comment lists what it does not catch. Test files and
the test-support modules its `allow` list names are exempt. Existing debt: the
sandbox store's public `SandboxRecord.managed_by` and
`AutoLifecycleExecution.managed_by` fields, which single-row reads still return
as text (`proves_no_execution` parses the latter). Convert them when touched;
they are not a pattern to copy.

- A decision about one row's substrate, its sandbox or node `managed_by`,
  calls a `SubstrateKind` method named for the question (for example
  `proves_execution_absence` or `restores_through_pods`); add one when no
  method asks it. Rust that repeats a statement's fixed scope compares
  against the named const the statement binds (for example the admin node
  registry's `REGISTRY_SUBSTRATE`), and mapping a `SubstrateKind` variant to
  a value (wire labels, route identity, construction) may match it. A fact
  about the running substrate reads the api crate's `capabilities()`
  (`SubstrateCapabilities`). Never compare the label string.

## Log targets: pin the ones searches select on

A `tracing` event without `target:` takes `module_path!()` as its target, as
the `sql!` locator above does. Moving or renaming a module therefore changes
the target of everything it logs. Datadog attribute queries accept wildcards,
but an exact-value `@target` filter silently stops matching, so list renamed
targets in the PR description.
An event whose target searches depend on takes a literal `target: "..."`, as
`space.admin.audit` does (`crates/libs/api/src/routes/admin/audit.rs`). A
`const LOG_TARGET: &str = module_path!()` keeps a target when code moves into a
child module, but not when the module that defines it is moved or renamed.

## Statement reuse and no-op writes

Hot node-local `rusqlite` statements go through `Connection::prepare_cached`,
and the connection's statement cache
(`set_prepared_statement_cache_capacity`) holds more statements than the
service runs: rusqlite's default of 16 lets background loops evict the
request path's statements, which are then parsed and planned on every call.
`sqlx` caches prepared statements per connection by itself.

Skip writes that change nothing at the value's resolution (for example, a
last-activity timestamp kept to the second), and do not re-read rows that the
preceding write did not change.

## Test placement: sibling module files, not inline `#[cfg(test)] mod tests`

Unit test code lives in a **sibling module file**. A src file never carries an
inline `#[cfg(test)] mod tests { … }` body; it carries only the declaration, and
the body moves to a file named after the module:

```rust
// crates/libs/node/mgr/server/src/slot_pool.rs:1489-1490 — the declaration sits
// among the production `mod` lines, gated exactly as the inline block was.
mod db;
mod reset;
#[cfg(test)]
mod tests;
mod time;
mod worker;
```

```rust
// crates/libs/node/mgr/server/src/slot_pool/tests.rs
use super::db::*;
use super::reset::*;
use super::*;

#[tokio::test]
async fn reclaims_lease_when_pod_netns_disappeared() { /* … */ }
```

Why: the inline form makes the *size* of a module a function of how well it is
tested. `slot_pool.rs` is ~1500 lines of production code; its tests are longer
still. Splitting them means a reviewer diffing production logic never scrolls
through fixtures, `cargo fmt` churn in one doesn't move the other, and the file
tree shows at a glance which modules have tests.

### The unit is the module, and the file is named after the module

| Host file | Test module | Sibling file |
| --- | --- | --- |
| `foo.rs` | `mod tests` | `foo/tests.rs` |
| `foo.rs` | `mod proto_wire_tests` **and** `mod tests` | `foo/proto_wire_tests.rs` **and** `foo/tests.rs` |
| `foo/mod.rs` | `mod tests` | `foo/tests.rs` |
| `lib.rs` / `main.rs` | `mod tests` | `tests.rs` beside it, in the crate's `src/` |
| `metrics-emit/src/lib.rs` → inline `mod test_capture` → `mod tests` | nested | `metrics-emit/src/test_capture/tests.rs` |
| host itself declared with `#[path]` | `mod tests` | `<host-stem>/tests.rs`, via explicit `#[path]` (below) |

One wrinkle: when the host file is *itself* loaded through a `#[path]`
attribute, rustc resolves its children in the host's **own directory**, not in
a `<host-stem>/` subdirectory. A bare `mod tests;` in such a file would land
the tests as a confusing top-level sibling. Put an explicit
`#[path = "<host-stem>/tests.rs"]` on the declaration (with a one-line comment
saying why) so the file still lands where this table predicts —
`crates/libs/common/core/src/k8s.rs` (loaded as `mod kubernetes` via
`#[path]`) is the worked example. This is the one *correct* use of `#[path]`
on a test declaration: it exists to land on the convention, unlike the legacy
flat variant at the end of this section, which exists to dodge it.

One module, one file. A src file with two named test modules gets two sibling
files, not one file with two `mod` blocks inside it. Nesting follows Rust's
ordinary module resolution, so a test module inside an inline production module
lands one directory deeper — `crates/libs/metrics-emit/src/lib.rs` declares
`pub mod test_capture`, whose own tests belong at `test_capture/tests.rs`.

### Why module-path preservation is the point

The sibling declaration produces the **identical module path** as the inline
block. That is not incidental — three things depend on it, and all three are
why this is a mechanical move rather than a refactor:

- **Visibility is unchanged.** The test module is still a child of the module
  under test, so it still sees its private items and `use super::*;` still
  resolves the same set of names. Moving tests *out* of the module path (to
  `tests/`) is what breaks this, not moving them to another file.
- **Nextest test IDs are unchanged.** Local `--filter-expr` / `-E` selectors
  and muscle-memory test names keep working, and a moved-tests diff can be
  verified by an empty `cargo nextest list` delta. (CI itself never names
  tests — `scripts/check_ci_test_completeness.py` forbids selectors there.)
- **insta snapshot *filenames* are unchanged**, because insta derives them from
  the module path. Only the snapshot *directory* moves, and only when the host
  file's directory changes: `foo.rs` → `foo/tests.rs` means insta now looks for
  `foo/snapshots/` instead of `snapshots/`. That is a `git mv` of the `.snap`
  files with no content edit.

### cfg attributes move verbatim

Whatever gated the inline module gates the declaration, character for character.
The workspace has `#[cfg(all(test, feature = "serde"))]`
(`crates/libs/common/core/src/types/template/metadata.rs`),
`#[cfg(all(test, target_os = "linux"))]`, `#[cfg(all(test, unix))]`, and
`#[cfg(all(test, feature = "kubernetes"))]`. **Never** simplify one of these to
a plain `#[cfg(test)]` while moving it — that silently compiles a test module
under feature sets it was never written for, and the failure surfaces as a CI
error in a job nobody associates with the move.

### Flat body, no redundant inner wrapper

Inside the sibling file the tests sit at file top level, directly after
`use super::*;` and any sibling-module globs the fixtures need. Do **not** wrap
them in a second `mod xyz_tests { … }`:

```rust
// Wrong — double-nests the module path.
use super::*;

#[cfg(test)]
mod slot_pool_tests {
    use super::*;
    #[test] fn … {}
}
```

The wrapper adds a path segment that shows up in every derived name. The live
evidence is in `crates/libs/sbx/mgr/server/src/service/tests.rs`, whose
`pub(crate) mod service_tests` wrapper produced snapshot files named
`pplx_space_sbx_mgr__service__tests__service_tests__*.snap` — one redundant
segment, baked into seven checked-in filenames.
`crates/libs/node/mgr/server/src/slot_pool/tests.rs` has the same wrapper. Both
are pre-existing; neither is a pattern to copy.

### `tests/` integration directories are reserved

A top-level `crates/<crate>/tests/*.rs` file is **not** the default home for
unit tests. Use one only when the test genuinely wants integration semantics:

- It deliberately exercises **only the public API**, and the inability to reach
  private helpers is the point. `crates/libs/common/core/tests/template_manifest_api.rs:1-9`
  states that rationale in its own module doc: the file "acts as a guard against
  accidentally relying on a private helper."
- It **spawns the built binary** via `CARGO_BIN_EXE_*`.
- It needs a **multi-file `support/` harness** shared across several test
  binaries, included per-binary with `#[path = "support/x.rs"]` — the shape used
  by `crates/libs/spaced/{guest,host}/server/tests/`. Note the repo has no
  `tests/common/mod.rs` anywhere; don't introduce one.

Why not by default: the large majority of existing test modules touch private
or `pub(crate)` items and cannot move out of the module path at all; bin-only
crates have no lib target for a `tests/` file to link against; and every
top-level `tests/*.rs` is a separate, separately-linked test binary, so moving
unit tests there converts cheap in-crate compilation into per-file link steps
in every CI run.

### Test-only helpers other crates need

When test support has to cross a crate boundary, gate it behind a named
`test-capture`-style feature rather than exporting it unconditionally or
duplicating it. `pplx-space-metrics-emit` is the pattern: `test-capture = []`
in its `[features]`, the helper gated `#[cfg(any(test, feature = "test-capture"))]`
(`crates/libs/metrics-emit/src/lib.rs`), so the crate's own tests get it for
free and downstream crates opt in explicitly. `cfg(test)` does not reach a
dependency, so a crate that re-exports such a helper forwards the feature and
enables it on its own dev-dependency, as `pplx-space-metrics` does.

### Watch items when moving or writing test files

- **`include_str!` / `include_bytes!` paths are relative to the containing
  file.** Moving a test body one directory deeper needs a `../` on every such
  path — *unless* the path is anchored with
  `concat!(env!("CARGO_MANIFEST_DIR"), …)`, which is location-independent and
  must be left alone (`crates/libs/sbx/mgr/server/src/restart_admission.rs`
  is a live example).
- **Expect cosmetic rustfmt reflow in the moved body.** Dedenting lets
  previously-wrapped lines fit the width limit, so rustfmt collapses them —
  which also drops the trailing commas vertical layouts carry. The diff is
  AST-identical; don't hand-rewrap to fight it, and don't mistake it for a
  content change in review.
- **`insta::with_settings!({ snapshot_path => "fixtures" })` is file-relative
  too** (`crates/libs/common/core/src/k8s.rs`,
  `crates/libs/node/mgr/server/src/firewall.rs`). Either move the fixture
  directory with the tests or adjust the setting; a stale relative path fails as
  a missing snapshot, not as a compile error.
- **A `//! # Test matrix` header is encouraged** on larger test files: a
  doc-comment mapping each required behavior to the test name that covers it.
  `crates/libs/cluster/controller/server/src/node_drain/tests.rs` is the worked
  example — categories (`C`, `R`, `M`, `D`, `L`), one line per case, names
  following `<category>_<scenario>`.

### The one legacy variant

Three files keep their test body as a *flat* sibling named
`<host>_tests.rs`, reached with an explicit `#[path = "…"]`:
`crates/libs/sbx/mgr/server/src/sidecar_env.rs`,
`crates/libs/internal/api/server/src/status.rs`, and
`crates/libs/internal/api/server/src/published_state.rs`. This works, but it
needs an attribute to say what `mod tests;` says for free, and it puts test
files in the same directory listing as production ones. Don't propagate it; new
test modules use the plain `#[cfg(test)] mod <name>;` + `<host>/<name>.rs` form.

## Service-dependency tests

Tests reach Postgres and Redis through `pplx-space-db-test-fixtures`
(`pg_test_db`, `try_pg_test_db` and their raw variants). A test that starts its
own container, as the api Redis and migrator bootstrap-role tests do, passes a
start failure to `report_unavailable_dependency` and returns only when that
returns `Ok`. An unavailable dependency then fails the test by default, with a
message naming the dependency and how to provide it. Only a run that sets
`SPACE_TEST_DEPS_OPTIONAL` to anything but empty or `0` skips instead, and
every such skip prints a `SPACE-TEST-DEP-SKIP:` marker that CI counts
(`SPACE_TEST_DEP_SKIP_LOG`). `SPACE_CI_REQUIRE_TEST_DEPS` predates the default:
set to anything but empty or `0` it requires the dependencies even under the
opt-out. Set to empty or `0` it is still read as optional, a legacy reading
rather than an opt-out to use: the NoCoW host-fixture tests treat any value of
it as CI. Don't print the marker for a skip that is not a missing service, such
as a main-CI-only test.

## Privileged tests and the CI host

The GitHub Rust test job runs the whole workspace, ignored tests included
(`ci/scripts/run-rust-checks.sh`), in two containers on one runner
(`.github/workflows/common-ci.yml`). Everything outside the nextest test group
`privileged` (`.config/nextest.toml`) runs as an unprivileged user with no
capabilities, no Docker socket and no host network; the runner starts its
Postgres and Redis. The group then runs from the same build as root in a
`--privileged` container with the host's Docker socket.

A test belongs in that group when it needs root, CAP_SYS_ADMIN, loop devices,
a relaxed seccomp profile, or its own containers. So does a test whose root
path differs from its unprivileged path, for example one that switches to
another uid only when it can: in the unprivileged half it would pass without
exercising its root assertions. Either list it in the group or split it into a
root test there and an unprivileged test outside it. Make a privileged test
assert its requirement rather than return early. For root, call
`pplx_space_test_env::root_or_skip(...)` and return when it returns false:
run locally without root, it prints a SKIPPED line, and when CI sets
`SPACE_CI_REQUIRE_TEST_PRIVILEGE` to anything but empty or `0` it fails, so a
test missing from the group fails the unprivileged half instead of passing
there without testing anything. Keep the group small; a test that needs
privilege only for an incidental step should lose that step instead.

The group's filter has one clause per line. The job checks that each clause
matches at least one test and that every test it matches is in the group, so
a rename or typo fails CI rather than moving a test to the unprivileged half.
The check cannot find a privileged test that no clause names; only that test's
own assertion can.

The split keeps ordinary tests from mutating the host by accident. It is not a
trust boundary: the unprivileged half has the whole checkout read-write, so it
can write everything the root half runs from. That is the target directory
(the test binaries, nextest's metadata and the privileged test list) and the
scripts and configuration the root half executes (`ci/scripts/run-rust-checks.sh`,
`.config/nextest.toml` and the nextest setup scripts). A hostile test or build
script in the unprivileged half still reaches root.

The privileged half can reach whatever its host offers, and the unprivileged
half can still reach the network, so tests must not touch host state they do
not own: fixed SPACE paths, production sockets, firewall or network-namespace
state, or processes they did not spawn. Test-owned temporary directories and
child processes are fine.

Make destructive host operations unreachable even on a regression path. When a
guard regresses, the fixture's next step must refuse. A stub whose stop fails,
a draining pool, a client on a closed port such as `127.0.0.1:1`, or an
already-cancelled work token all refuse that way. A test that must mutate the
host needs an environment opt-in naming a disposable target; `#[ignore]` does
not keep it out of CI.

When a test depends on a value read from the environment, follow
"Environment-dependent tests" in `docs/rust/unsafe.md`. Give the parser
explicit inputs, such as a `pplx_space_test_env::TestEnv` passed as its
`Environment`. Only a test of real startup-environment behavior runs in a child
test process: call `pplx_space_test_env::enter_test_process` first and return
when it returns false. Never call `std::env::set_var`.

## Service binaries: a thin `main.rs` over a library crate

Every deployable service is a library crate that *is* the service plus a
binary crate whose `main.rs` is only the seam between the process and that
library. `crates/apps/data/plane/svc/src/main.rs` is the exemplar. A thin main
holds exactly:

- the clap `Cli`, and `impl From<Cli> for <Svc>RunConfig` (clap `env = "..."`
  attributes are the one sanctioned environment read);
- an optional `#[global_allocator]` and its `malloc_conf`;
- `main()`: parse, build the runtime if the crate builds one, `init_service`,
  the shutdown token, one `lib::run(config, shutdown).await?`.

Everything else is the service and lives in the library: environment
resolution and its precedence, `sqlx::migrate!` statics, binding and layering
servers, service-mutex and handoff sequencing, and every test. The library's
`run` returns a `thiserror` enum, never `anyhow` (see
`scripts/check-rust-error-policy.sh`). The reason is the SPACE OSS model: a
deployment that needs private wiring supplies its own `main` against the
library, so whatever sits in the public main is what a private one has to copy
and keep in step with.

`scripts/source_guards/thin_service_mains.py` enforces the shape (no `mod`, no
`sqlx::migrate!`, no `env::var`, no server assembly, at most 80 lines outside the
clap items since the flag surface is help text and may grow, nothing beside
`main.rs` in `src/`) and carries a burn-down allowlist for services whose split
has not merged. The PR that thins a main removes its entry; the guard fails on
an allowlisted main that is already thin.

## Where these conventions live

- This file: the SPACE-specific *what* and *why*.
- `AGENTS.md`: short imperative rules with pointers back here.
- Per-topic docs (`docs/rust/{type-driven-design,error-handling,async-and-tokio,performance,api-and-naming}.md`): general Rust principles distilled from the canonical books; SPACE-specific exceptions cross-link here.

When a new crosscutting choice gets made (e.g. "we standardized on `bon` for builders"), add a section here rather than peppering it across files. One source of truth per decision.
