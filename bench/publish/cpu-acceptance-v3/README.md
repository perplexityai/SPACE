# Synthetic publication CPU evidence

These records accompany the `PublishNodeState` refactor and optimization
stack. The benchmark-only branch keeps generated evidence separate from
the measured application heads; its tip is not a new application measurement.

## Reading a record

Each `head-*` directory identifies the exact service, frozen Rust driver,
runner revision, binary digests, CPU affinity, and same-lane reference.
Its README provides the before/after table and workload command.
`warmup.json` is discarded; `run-1.json` through `run-3.json` are the three
measured runs. No run replacement or exclusion is used in these records.

The JSON report values are unchanged from the captures, including every
`pg_stat_statements` record. Only whitespace follows the existing checked-in
baseline layout. `packaged-reports.sha256` hashes the repository copies;
`summary.json` retains the original capture hashes.

PostgreSQL process CPU per successful publish is the headline metric.
SQL execution time, service CPU, WAL, and transaction latency are reported
separately. Spread means `(maximum - minimum) / median * 100`, not a
confidence interval; the below-five-percent gate applies to headline CPU,
not to every secondary metric.

## Measurement protocol

The standard synthetic mix uses 50 nodes, 160 sandboxes per node, seed 42,
and ten real-time minutes per run, yielding 10,000 publishes. Every
baseline and head receives one discarded warmup and three measured runs.
Native PGDG PostgreSQL 17.11 has statement planning and I/O timing enabled,
with durability enabled.

Eight isolated lanes ran concurrently on disjoint physical-core pairs on
one Linux x86-64 machine, without workload use of SMT siblings. Each lane
shares one qualified baseline across its queued heads; comparisons never
substitute a different lane's reference. Builds and database test suites
did not overlap the measurements.

## Correctness scope

The workload exports 20 tables. Comparisons use the frozen comparator's
explicit normalization of run-local identifiers and clocks; original
captures remain retained. Counter-throttled heads additionally check
counter tuples and freshness diagnostics rather than silently dropping
those fields. Their records list observed counter differences and limits.

These checks do not cover every publish-owned table or cascade. Some
captured tables are empty in the standard workload, so separate targeted
tests and scenario captures supply additional coverage. Mixed-version
rollout, real microVM behavior, and concurrency beyond the recorded tests
are not established by these results.

## Available records

| Application head | Same-lane reference | PostgreSQL CPU median, ms/publish | Reduction |
| --- | --- | ---: | ---: |
| [23c27a10](head-23c27a10/README.md) | [Lane A](baseline-lane-a/README.md) | 3.338 → 3.398 | -1.7975% |
| [80a369e3](head-80a369e3/README.md) | [Lane B](baseline-lane-b/README.md) | 3.328 → 3.371 | -1.2921% |
| [b91ad676](head-b91ad676/README.md) | [Lane C](baseline-lane-c/README.md) | 3.293 → 3.361 | -2.0650% |
| [3be0baa7](head-3be0baa7/README.md) | [Lane D](baseline-lane-d/README.md) | 3.319 → 3.334 | -0.4519% |
| [d5d873c4](head-d5d873c4/README.md) | [Lane E](baseline-lane-e/README.md) | 3.338 → 3.380 | -1.2582% |
| [487d04e6](head-487d04e6/README.md) | [Lane F](baseline-lane-f/README.md) | 3.336 → 3.335 | 0.0300% |
| [c0f8acab](head-c0f8acab/README.md) | [Lane G](baseline-lane-g/README.md) | 3.325 → 3.337 | -0.3609% |
| [88aefee5](head-88aefee5/README.md) | [Lane H](baseline-lane-h/README.md) | 3.352 → 3.357 | -0.1492% |
| [c55485c0](head-c55485c0/README.md) | [Lane B](baseline-lane-b/README.md) | 3.328 → 3.328 | 0.0000% |
| [ce6bf8df](head-ce6bf8df/README.md) | [Lane C](baseline-lane-c/README.md) | 3.293 → 3.288 | 0.1518% |
| [938e0cf4](head-938e0cf4/README.md) | [Lane A](baseline-lane-a/README.md) | 3.338 → 1.522 | 54.4038% |

Reductions compare each head with its own lane's fixed reference; negative
values mean CPU increased. Small deltas in the behavior-preserving refactor
heads are not optimization claims. The final result is cumulative across
the stack, not the isolated contribution of its final slice.

The final `938e0cf4` head's local PG17 and PG16 matrices each pass 429 tests
with zero skips, including lifecycle and counter lock-wait race coverage.
This does not assert that all GitHub CI passes or that the stack is ready
to merge onto an updated base.
