# Synthetic publication CPU: 8b345478

Measured application revision: `8b345478194cfeb3629210d5766486c338c62d30`. Reference: [same-lane baseline](../baseline-lane-a/README.md). All inputs are synthetic, with the same machine and seed on both sides.

## Results

Relative median PostgreSQL process CPU reduction: -0.7190%. A negative value means an increase. SQL execution time and RPC latency are not the headline metric.

| Metric | Baseline median | Head median | Base spread | Head spread |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL CPU, ms/publish | 3.338 | 3.362 | 2.49% | 3.09% |
| Service CPU, ms/publish | 0.93 | 0.967 | 1.51% | 3.52% |
| Statements/publish | 14.466 | 14.466 | 0.00% | 0.00% |
| SQL execution, ms/publish | 0.897763 | 0.895813 | 3.82% | 6.45% |
| SQL planning, ms/publish | 0.339077 | 0.355563 | 10.34% | 15.95% |
| Rows/publish | 14.871 | 14.871 | 0.00% | 0.00% |
| Shared block hits/publish | 402.482 | 402.034 | 2.50% | 3.06% |
| WAL bytes/publish | 13103.6 | 13020.3 | 4.30% | 6.63% |
| Transaction p50, ms | 2.6039 | 2.57651 | 2.27% | 1.00% |
| Transaction p99, ms | 53.2485 | 53.8702 | 17.25% | 24.45% |

Spread is (maximum - minimum) / median × 100, not a confidence interval. Raw report values are unchanged; only JSON whitespace is reformatted. No run replacement or exclusion was used.

## Reproduction

Run the following command once for the baseline binary and once for
the head binary. Each invocation performs a discarded warmup and three measured
runs; use a fresh output directory each time. Paths are caller-supplied, while
all workload flags and CPU sets below match the measured argv.

```sh
python3 scripts/with-publish-postgres.py \
  --pg-bin "$PG_BIN" --setup-bin "$SETUP_BIN" --postgres-cpus 0 -- \
  taskset -c 8 python3 scripts/run-publish-baseline.py \
  --output "$OUTPUT" -- \
  "$DRIVER_BIN" --service-bin "$SERVICE_BIN" --migrator-bin "$MIGRATOR_BIN" \
  --nodes 50 --sandboxes-per-node 160 --minutes 10 --seed 42 --time-scale 1
```

Use runner scripts at `0e85ab83b794cdcc5cc711d449548fb67f44d660`, the Rust driver at
`afd538c404eeee787597ad4cb246a9becaf7dffb`, and the migrator/setup binaries at `3493032a2dcc10bd46685b8cea2b4aaed7cf0bdf`.
Build with the pinned toolchain and `cargo build --profile dev-release`.
The service revision and binary hashes are recorded in `summary.json`.
Native PGDG PostgreSQL 17.11 uses `pg_stat_statements` with planning and I/O
timing enabled; fsync, synchronous commit and full-page writes remain enabled.
The fixture launcher creates and cleans only its own disposable cluster.

Eight isolated benchmark lanes ran concurrently on disjoint physical-core
pairs on the same Linux x86-64 host. Workloads did not use SMT siblings.
Each lane reused its own qualified reference across its queued heads;
no builds or database test suites overlapped the measurements.

## Correctness and limits

The completed local matrix includes optimized builds, formatting, workspace Clippy with denied warnings, and native PG17/default PG16 nextest with zero skips. Per-database counts are in `summary.json`.

Row evidence covers 20 exported tables after explicit run-local normalization. Counter-aware records permit only the approved counter freshness differences and preserve their observed lag diagnostics. Empty tables do not prove their write paths, and this is not all-table or all-cascade coverage. Mixed-version rollout, real microVM behavior, and multi-node races beyond the test suite were not validated. No strict counter API-age guarantee is asserted.

This is scoped local evidence, not a claim that all GitHub CI passes.
