# Synthetic publication CPU: f1e86c45

Measured application revision: `f1e86c45bb36df873ff95082ca7c68006b878159`. Reference: [same-lane baseline](../baseline-lane-f/README.md). All inputs are synthetic, with the same machine and seed on both sides.

## Results

Relative median PostgreSQL process CPU reduction: -0.1499%. A negative value means an increase. SQL execution time and RPC latency are not the headline metric.

| Metric | Baseline median | Head median | Base spread | Head spread |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL CPU, ms/publish | 3.336 | 3.341 | 3.36% | 0.63% |
| Service CPU, ms/publish | 0.923 | 0.922 | 1.84% | 3.15% |
| Statements/publish | 14.466 | 14.466 | 0.00% | 0.00% |
| SQL execution, ms/publish | 0.903047 | 0.909224 | 3.19% | 4.55% |
| SQL planning, ms/publish | 0.330738 | 0.329927 | 13.85% | 9.65% |
| Rows/publish | 14.871 | 14.871 | 0.00% | 0.00% |
| Shared block hits/publish | 402.699 | 402.178 | 1.84% | 2.11% |
| WAL bytes/publish | 13020.1 | 13012.6 | 5.09% | 4.35% |
| Transaction p50, ms | 2.6036 | 2.61188 | 1.71% | 1.46% |
| Transaction p99, ms | 54.1066 | 54.0055 | 16.50% | 14.14% |

Spread is (maximum - minimum) / median × 100, not a confidence interval. Raw report values are unchanged; only JSON whitespace is reformatted. No run replacement or exclusion was used.

## Reproduction

Run the following command once for the baseline binary and once for
the head binary. Each invocation performs a discarded warmup and three measured
runs; use a fresh output directory each time. Paths are caller-supplied, while
all workload flags and CPU sets below match the measured argv.

```sh
python3 scripts/with-publish-postgres.py \
  --pg-bin "$PG_BIN" --setup-bin "$SETUP_BIN" --postgres-cpus 5 -- \
  taskset -c 13 python3 scripts/run-publish-baseline.py \
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
