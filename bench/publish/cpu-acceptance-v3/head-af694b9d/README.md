# Synthetic publication CPU: af694b9d

Measured application revision: `af694b9d2f185995995e4d35ba79e83e9327b2e7`. Reference: [same-lane baseline](../baseline-lane-a/README.md). All inputs are synthetic, with the same machine and seed on both sides.

## Results

Relative median PostgreSQL process CPU reduction: 54.8232%. A negative value means an increase. SQL execution time and RPC latency are not the headline metric.

| Metric | Baseline median | Head median | Base spread | Head spread |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL CPU, ms/publish | 3.338 | 1.508 | 2.49% | 1.92% |
| Service CPU, ms/publish | 0.93 | 0.423 | 1.51% | 1.89% |
| Statements/publish | 14.466 | 9.468 | 0.00% | 0.00% |
| SQL execution, ms/publish | 0.897763 | 0.705824 | 3.82% | 1.54% |
| SQL planning, ms/publish | 0.339077 | 0.119827 | 10.34% | 12.69% |
| Rows/publish | 14.871 | 14.871 | 0.00% | 0.00% |
| Shared block hits/publish | 402.482 | 362.781 | 2.50% | 2.57% |
| WAL bytes/publish | 13103.6 | 11842.3 | 4.30% | 14.78% |
| Transaction p50, ms | 2.6039 | 1.69093 | 2.27% | 1.80% |
| Transaction p99, ms | 53.2485 | 21.4026 | 17.25% | 14.97% |

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

The batch used eight benchmark lanes on disjoint physical-core pairs on the
same Linux x86-64 host, without SMT siblings. Active lane count decreased as
queues completed. Each head reused its own lane's qualified reference;
no builds or database test suites overlapped the measurements.

## Correctness and limits

The completed local matrix includes optimized builds, formatting, workspace Clippy with denied warnings, and native PG17/default PG16 nextest with zero skips. Per-database counts are in `summary.json`.

Row evidence covers 20 exported tables after explicit run-local normalization. Counter-aware records permit only the approved counter freshness differences and preserve their observed lag diagnostics. Empty tables do not prove their write paths, and this is not all-table or all-cascade coverage. Mixed-version rollout, real microVM behavior, and multi-node races beyond the test suite were not validated. No strict counter API-age guarantee is asserted.

Counter-only writes use a 60-second threshold against the row's `observed_at`, not a separate counter-write timestamp. Degraded reports also advance `observed_at` without refreshing counters and can restart the window. This observation-clock behavior is accepted as best effort; tighter counter freshness is deferred. Meaningful state changes write immediately. No strict API-age or last-counter-write guarantee is asserted.

This is scoped local evidence, not a claim that all GitHub CI passes.
