# Synthetic publication CPU: ed788ea1

Measured application revision: `ed788ea154c36ed4446c327ac83d1fbd6361e8d1`. Reference: [same-lane baseline](../baseline-lane-f/README.md). All inputs are synthetic, with the same machine and seed on both sides.

## Results

Relative median PostgreSQL process CPU reduction: -0.1799%. A negative value means an increase. SQL execution time and RPC latency are not the headline metric.

| Metric | Baseline median | Head median | Base spread | Head spread |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL CPU, ms/publish | 3.336 | 3.342 | 3.36% | 1.08% |
| Service CPU, ms/publish | 0.923 | 0.958 | 1.84% | 1.46% |
| Statements/publish | 14.466 | 14.466 | 0.00% | 0.00% |
| SQL execution, ms/publish | 0.903047 | 0.907317 | 3.19% | 4.57% |
| SQL planning, ms/publish | 0.330738 | 0.33467 | 13.85% | 11.50% |
| Rows/publish | 14.871 | 14.871 | 0.00% | 0.00% |
| Shared block hits/publish | 402.699 | 403.277 | 1.84% | 2.13% |
| WAL bytes/publish | 13020.1 | 13114.3 | 5.09% | 3.96% |
| Transaction p50, ms | 2.6036 | 2.62155 | 1.71% | 0.55% |
| Transaction p99, ms | 54.1066 | 54.9079 | 16.50% | 14.99% |

Spread is (maximum - minimum) / median × 100, not a confidence interval. Raw report values are unchanged; only JSON whitespace is reformatted. This is a full-protocol retry, not a replacement of individual runs. Earlier unqualified attempts remain available: [unqualified-ed788ea1-attempt-1](../unqualified-ed788ea1-attempt-1/README.md). Those attempts failed the headline CPU-spread gate; no specific cause was established. The qualified table does not pool them with this retry.

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
