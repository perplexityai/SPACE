# Synthetic publication CPU: ac717ddd

Measured application revision: `ac717ddd1b93b8eeaeba6116a8e07de92dc45fee`. Reference: [same-lane baseline](../baseline-lane-b/README.md). All inputs are synthetic, with the same machine and seed on both sides.

## Results

Relative median PostgreSQL process CPU reduction: 52.0433%. A negative value means an increase. SQL execution time and RPC latency are not the headline metric.

| Metric | Baseline median | Head median | Base spread | Head spread |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL CPU, ms/publish | 3.328 | 1.596 | 1.92% | 1.50% |
| Service CPU, ms/publish | 0.921 | 0.419 | 1.41% | 0.48% |
| Statements/publish | 14.466 | 9.468 | 0.00% | 0.00% |
| SQL execution, ms/publish | 0.89559 | 0.774576 | 3.57% | 1.23% |
| SQL planning, ms/publish | 0.346219 | 0.11826 | 9.71% | 12.66% |
| Rows/publish | 14.871 | 14.871 | 0.00% | 0.00% |
| Shared block hits/publish | 402.736 | 421.939 | 2.41% | 1.56% |
| WAL bytes/publish | 13041.9 | 13815.7 | 4.01% | 11.70% |
| Transaction p50, ms | 2.55115 | 1.7442 | 0.93% | 0.50% |
| Transaction p99, ms | 53.8447 | 21.7868 | 17.43% | 15.20% |

Spread is (maximum - minimum) / median × 100, not a confidence interval. Raw report values are unchanged; only JSON whitespace is reformatted. No run replacement or exclusion was used.

## Reproduction

Run the following command once for the baseline binary and once for
the head binary. Each invocation performs a discarded warmup and three measured
runs; use a fresh output directory each time. Paths are caller-supplied, while
all workload flags and CPU sets below match the measured argv.

```sh
python3 scripts/with-publish-postgres.py \
  --pg-bin "$PG_BIN" --setup-bin "$SETUP_BIN" --postgres-cpus 1 -- \
  taskset -c 9 python3 scripts/run-publish-baseline.py \
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

The batch used eight benchmark lanes on disjoint physical-core pairs on the
same Linux x86-64 host, without SMT siblings. Active lane count decreased as
queues completed. Each head reused its own lane's qualified reference;
no builds or database test suites overlapped the measurements.

## Correctness and limits

The completed local matrix includes optimized builds, formatting, workspace Clippy with denied warnings, and native PG17/default PG16 nextest with zero skips. Per-database counts are in `summary.json`.

Row evidence covers 20 exported tables after explicit run-local normalization. Counter-aware records permit only the approved counter freshness differences and preserve their observed lag diagnostics. Empty tables do not prove their write paths, and this is not all-table or all-cascade coverage. Mixed-version rollout, real microVM behavior, and multi-node races beyond the test suite were not validated. No strict counter API-age guarantee is asserted.

This is scoped local evidence, not a claim that all GitHub CI passes.
