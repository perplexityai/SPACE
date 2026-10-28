# PublishNodeState synthetic measurements

This directory holds local, synthetic evidence for changes to node publication.
Never use deployment data, credentials, private endpoints, or operational logs
as benchmark inputs or checked-in reports.

## Native PostgreSQL

Install native PostgreSQL 17, including `pg_stat_statements`, and build the
repository fixture initializer before running a package-selected test suite:

```sh
cargo build --profile dev-release \
  -p pplx-space-db-test-fixtures --bin space-nextest-postgres-setup
python3 scripts/with-publish-postgres.py \
  --setup-bin target/dev-release/space-nextest-postgres-setup \
  -- cargo nextest run -p pplx-space-internal-api-server
```

Use `--pg-bin /path/to/postgresql/17/bin` for a nonstandard installation.
The launcher requires version 17, binds only loopback, generates a fresh
password, migrates the shared fixture template, and exports
`SPACE_TEST_POSTGRES_ADMIN_URL` to the child command. It removes the temporary
cluster after that command, including when the command fails.

The cluster enables planning statistics and I/O timing with
`pg_stat_statements` preloaded, and permits 200 connections. Durability remains
enabled. Do not run tests and a measured workload against the same cluster:
statistics resets and CPU accounting belong to one exclusive benchmark.
`SPACE_PUBLISH_POSTGRES_PID` identifies the launcher's own postmaster for
process-tree CPU accounting. `--postgres-cpus` accepts a `taskset` CPU list;
pin the child workload to a disjoint list when comparing revisions.

## Publication contract

The existing integration and lifecycle-race suites are the correctness gate.
The optimization series must preserve these invariants:

- One transaction contains every publication phase.
- Delete-completion receipts run after sandbox, execution, pruning, and status
  writes, so their fences observe this publication's final evidence.
- Pending-backup replacement precedes the status freshness stamp.
- Sandbox events retain their order and fire only after a successful commit.
- Guarded transitions retain their lifecycle and execution-generation fences.
  A zero-row guarded write is an ordinary outcome, not a storage failure.
- Row-lock order remains consistent across sandbox, quarantine, and execution
  records. Run `published_state_lifecycle_race_tests` after storage changes.
- The existing schema and protocol remain sufficient for mixed server and
  node versions.

Do not substitute dependency skips, a mock handler, SQL execution time, or RPC
latency for real integration evidence, Postgres CPU, or transaction duration.
The launcher alone is infrastructure, not a workload baseline or a performance
claim.

## Real-service workload

Build the service, migrator, initializer, and driver with the pinned toolchain:

```sh
cargo build --profile dev-release -p pplx-space-db-test-fixtures \
  --bin space-nextest-postgres-setup
cargo build --profile dev-release -p pplx-space-publish-load \
  -p pplx-space-internal-api-svc -p pplx-space-db-migrator
```

Run the fixed-seed standard workload, using disjoint CPU sets when available:

```sh
python3 scripts/with-publish-postgres.py \
  --setup-bin target/dev-release/space-nextest-postgres-setup \
  --postgres-cpus 0-3 -- \
  taskset -c 4-7 python3 scripts/run-publish-baseline.py \
  --output /tmp/publish-baseline -- \
  target/dev-release/publish-load \
  --service-bin target/dev-release/space-internal-api-svc \
  --migrator-bin target/dev-release/space-db-migrator \
  --nodes 50 --sandboxes-per-node 160 --minutes 10 --seed 42 --time-scale 1
```

Each node publishes every three simulated seconds with node-specific staggering.
The 20-slot minute has one full sandbox inventory, eleven single-sandbox updates,
six status-only publications, and two template inventories. One of the first
single-sandbox updates per node carries a rare transition, cycling through
stop, restore, suspend, activation rollback, and pause-failure recovery.
The driver asserts their resulting lifecycle and, where necessary, execution
retirement or quarantine removal. It rejects partial acceptance.

Fixtures use real ready-worker helpers and cloned, migrated databases.
The primed tables are explicitly analyzed before statistics reset so planner
statistics do not depend on when autovacuum first visits a fresh fixture.
Every sandbox has a Kubernetes execution, pod address, and pod identity;
running executions have a route activation identity. The cold-activation
rollback fixture deliberately has no established route. A synthetic
snapshot URI is only a database fixture and is never accessed. The service
runs with generated loopback TLS and a per-run static token.

Activity timestamps use a run epoch plus the exact simulated slot, rather than
per-request wall-clock reads. This keeps one-minute freshness-boundary inputs
identical between runs. Accelerated runs also accelerate that synthetic node
clock; only real-time runs provide evidence about wall-clock freshness.

The driver requires the launcher's owned data directory, loopback URL, and
postmaster PID to agree before any fixture or role mutation. No shared or
externally supplied database is supported.

The headline CPU counter sums the postmaster and its live children's
`utime + stime`, plus already reaped child CPU, and divides the delta by
successful publications. It retries snapshots that race process-tree changes.
Statement metrics select only the service role in the fixture database;
the fixture and measurement connection use the admin role. WAL counters reset
after priming and get a two-second flush window at either end.

RPC latency is measured independently. Opt-in `space_publish_timing=trace`
events measure the open transaction from completed `BEGIN` through successful
`COMMIT`, excluding pool acquisition and the `BEGIN` round trip. This event
contains only a duration, not request payloads; it is disabled by default.
The driver requires exactly one timing per measured publication.

`report.json` contains raw statement details and per-publication metrics.
Other JSON exports preserve raw database rows for row comparison:

```sh
python3 scripts/compare-publish-rows.py /tmp/base/run-1 /tmp/head/run-1 \
  --output /tmp/publish-row-comparison
```

The comparator requires identical fields, row multiplicities, counters, lifecycle
values, ownership, generation fences, receipts, and activity-clock offsets.
Execution surrogate IDs and generated history IDs are keyed by logical identity;
deterministic receipt IDs remain literal. Server-generated timestamps retain
their equality and ordering within each node; the status deadline retains its
exact offset from `observed_at`. Nothing is silently dropped, including nulls.
Normalization is restricted to explicitly named timestamp columns and surrogate
primary keys. Other text and nested JSON remain literal, even when a string
looks like a timestamp or happens to equal a generated identifier.
This is normalized row equivalence, not byte equality of independent wall clocks,
and does not prove identical elapsed time between server writes. Raw exports
remain available for inspection. There is no counter-freshness exemption.

`service.log` is local diagnostic output, not a checked-in performance artifact.

Use a smaller node count, sandbox count, or `--time-scale` for a quick harness
check, but label it nonstandard and never substitute its results for the
ten-minute, real-time baseline. The baseline wrapper discards its first run,
reports the median and `(max - min) / median` spread over three measured runs,
and fails if headline CPU spread is not below five percent.
