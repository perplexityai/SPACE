# Unqualified CPU attempt: ed788ea1

This complete first attempt failed the below-five-percent PostgreSQL CPU spread gate. It is retained, not discarded, and does not qualify this application head for a performance claim.

## Results

| Metric | Baseline median | Head median | Base spread | Head spread |
| --- | ---: | ---: | ---: | ---: |
| PostgreSQL CPU, ms/publish | 3.336 | 3.378 | 3.36% | 5.12% |
| Service CPU, ms/publish | 0.923 | 0.964 | 1.84% | 1.97% |
| Statements/publish | 14.466 | 14.466 | 0.00% | 0.00% |
| SQL execution, ms/publish | 0.903047 | 0.908049 | 3.19% | 5.16% |
| SQL planning, ms/publish | 0.330738 | 0.358855 | 13.85% | 25.86% |
| Rows/publish | 14.871 | 14.871 | 0.00% | 0.00% |
| Shared block hits/publish | 402.699 | 404.452 | 1.84% | 2.33% |
| WAL bytes/publish | 13020.1 | 13074.2 | 5.09% | 2.72% |
| Transaction p50, ms | 2.6036 | 2.60666 | 1.71% | 1.10% |
| Transaction p99, ms | 54.1066 | 56.4483 | 16.50% | 17.49% |

All three measured runs are included; the separate warmup is discarded by the predefined protocol. No individual run is replaced. A subsequent attempt, if made, must repeat the full warmup-plus-three protocol and link this rejected attempt explicitly.

The workload configuration, publish count, statement count, and rows per publish matched across the measured runs. No specific cause of the variance was established, and the spread is not a confidence interval.

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

## Correctness scope

The warmup and all three measured pairs passed the 20-table comparison after explicit run-local normalization. These diagnostic comparisons were completed after the CPU gate stopped the queue; they do not turn this attempt into qualified CPU evidence. Empty tables do not establish their write-path coverage, and all-table/cascade or mixed-version equivalence is not claimed.
