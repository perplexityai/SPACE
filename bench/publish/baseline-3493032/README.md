# Synthetic publication baseline

This is a baseline, not a before/after improvement claim. It measures the real
service at `3493032a2dcc10bd46685b8cea2b4aaed7cf0bdf`, with unchanged publication
SQL and opt-in transaction timing, on synthetic fixtures only.

## Reproduction

The workload used 50 nodes, 160 sandboxes per node, seed 42, and ten real-time
minutes per run. One warmup was discarded before three measured runs of 10,000
publishes each; all four used the same machine and binaries.

The host was x86-64 Linux with 32 logical CPUs, an Intel Xeon 6975P-C processor,
and approximately 124 GiB RAM. Native PGDG PostgreSQL was
`17.11 (Debian 17.11-1.pgdg13+2)`, with PostgreSQL pinned to CPUs 0-3 and the
driver and service to CPUs 4-7; durability remained enabled.

Build with the pinned toolchain and run from the repository root:

```sh
cargo build --profile dev-release \
  -p pplx-space-internal-api-server -p pplx-space-internal-api-svc \
  -p pplx-space-publish-load -p pplx-space-db-test-fixtures \
  -p pplx-space-db-migrator
python3 scripts/with-publish-postgres.py \
  --pg-bin /path/to/postgresql/17/bin \
  --setup-bin target/dev-release/space-nextest-postgres-setup \
  --postgres-cpus 0-3 -- \
  taskset -c 4-7 python3 scripts/run-publish-baseline.py \
  --output /tmp/publish-baseline -- \
  target/dev-release/publish-load \
  --service-bin target/dev-release/space-internal-api-svc \
  --migrator-bin target/dev-release/space-db-migrator \
  --nodes 50 --sandboxes-per-node 160 --minutes 10 --seed 42 --time-scale 1
```

The PostgreSQL installation path and output directory above are placeholders
for machine-local paths; the workload arguments and CPU sets are the ones
used. If using extracted PGDG packages, set `LD_LIBRARY_PATH` to their dependency
directory before invoking the launcher.

Frozen binary SHA-256 digests:

- `space-internal-api-svc`: `44aa5e48d18180ae6751969b40acbc0a268ae440dc07bb5c754d3a6dbb3e97f3`
- `publish-load`: `d024a14b78de22dfafc0268af95833dbe0dca47a7f4032d5ac0f5237557516cf`

## Results

Spread is `(maximum - minimum) / median * 100`, not a confidence interval.
PostgreSQL process CPU per successful publish is the headline metric; SQL
execution time and RPC latency are separate measurements.

| Metric | Run 1 | Run 2 | Run 3 | Median | Spread |
| --- | ---: | ---: | ---: | ---: | ---: |
| PostgreSQL CPU, ms/publish | 3.389 | 3.478 | 3.469 | 3.469 | 2.57% |
| Service CPU, ms/publish | 1.052 | 1.067 | 1.062 | 1.062 | 1.41% |
| Statements/publish | 14.466 | 14.466 | 14.466 | 14.466 | 0.00% |
| SQL execution, ms/publish | 0.920653 | 0.927603 | 0.929322 | 0.927603 | 0.93% |
| SQL planning, ms/publish | 0.283319 | 0.356353 | 0.353704 | 0.353704 | 20.65% |
| Rows/publish | 14.871 | 14.871 | 14.871 | 14.871 | 0.00% |
| Shared block hits/publish | 407.1133 | 401.0046 | 401.1451 | 401.1451 | 1.52% |
| WAL bytes/publish | 15084.4225 | 14527.0256 | 14523.7363 | 14527.0256 | 3.86% |
| Transaction p50, ms | 2.687477 | 2.750314 | 2.736308 | 2.736308 | 2.30% |
| Transaction p99, ms | 54.221654 | 48.855476 | 49.194277 | 49.194277 | 10.91% |
| RPC p50, ms | 3.192736 | 3.310363 | 3.300161 | 3.300161 | 3.56% |
| RPC p99, ms | 55.140931 | 49.832305 | 50.240432 | 50.240432 | 10.57% |

The headline spread passes the under-5% stability gate. Planning time and
p99 durations are substantially noisier; the gate does not assert that every
secondary metric is stable.

## Raw evidence and correctness

`summary.json` is the unmodified summary export. `warmup.json` and `run-1.json`
through `run-3.json` contain every field and statement from the raw exports.
Only JSON formatting changes: each statement object occupies one line, while
top-level measurements remain expanded. The non-whitespace content matches the
original exports exactly, including integer query IDs and floating-point values.
There are no service logs, credentials, or database row dumps.

```sh
sha256sum -c raw-reports.sha256
jq '.postgres_cpu_ms_per_publish, .statements[]' run-1.json
```

At the measured revision, optimized builds, formatting, workspace Clippy with
`-D warnings`, and the local architecture check passed. Both native PostgreSQL
17 and default container PostgreSQL 16 passed 385 tests with zero skips:
379 API tests and six workload-driver tests, including lifecycle races and
unchanged existing snapshots.

The stricter comparator at `1c8ba4f8` found normalized row equivalence between
warmup and run 1, run 1 and run 2, and run 1 and run 3. Normalization preserves
opaque strings and JSON, counters, ownership, generation fences, receipts,
activity-clock offsets, and timestamp equality/order, but does not establish
literal wall-clock equality or identical elapsed time between writes.

## Validation limits

There is no optimization head in this report, so no CPU reduction or completed
base/head comparison is claimed. These results apply to the measured revision,
not automatically to every earlier or later stack head.

Mixed-version rollouts, additional multi-node interleavings beyond the existing
race tests, and real microVM behavior were not validated. The raw row exports
remain local and are not checked in here; the included reports expose every
measurement and statement, not a portable replay of the row-equivalence check.
