# Publication statement equivalence

`publish-statements` captures the real service's SQL-call multiset for ten
synthetic scenarios. It checks empty, node-status, template, ordinary-single,
full-inventory, stopped, restore-completed, suspend-completed,
activation-rollback, and pause-failed-recovery publications.

The database is freshly seeded with the same five transition fixtures used by
the CPU workload. Each scenario subtracts service-role `pg_stat_statements`
call counts immediately before and after its RPC. It preserves exact SQL text
and multiplicity, removing only the leading Rust locator comment. A scenario
fails if acceptance changes, counters regress, or a seeded rare transition
does not complete. This is component integration evidence, not a deployed smoke.

## Run and compare

Build once and retain this harness binary. Run it against each retained service
binary on the same machine and seed, through the disposable native launcher.

```sh
cargo build --profile dev-release -p pplx-space-publish-load \
  -p pplx-space-db-test-fixtures -p pplx-space-db-migrator

python3 scripts/with-publish-postgres.py --pg-bin "$PG17_BIN" \
  --setup-bin target/dev-release/space-nextest-postgres-setup -- \
  target/dev-release/publish-statements \
  --service-bin "$BASE_SERVICE_BIN" \
  --migrator-bin target/dev-release/space-db-migrator \
  --nodes 5 --sandboxes-per-node 4 --seed 42 --output /tmp/publish-statements-base

python3 scripts/with-publish-postgres.py --pg-bin "$PG17_BIN" \
  --setup-bin target/dev-release/space-nextest-postgres-setup -- \
  target/dev-release/publish-statements \
  --service-bin "$HEAD_SERVICE_BIN" \
  --migrator-bin target/dev-release/space-db-migrator \
  --nodes 5 --sandboxes-per-node 4 --seed 42 --output /tmp/publish-statements-head

python3 scripts/compare-publish-statements.py \
  /tmp/publish-statements-base/statements.json \
  /tmp/publish-statements-head/statements.json
```

Output directories must not exist. Keep both raw `statements.json` reports with
the exact service revisions and the comparator result. A successful comparison
requires the same seed and scenario set and identical SQL-call multisets.

This check does not replace process-CPU measurements, normalized row comparison,
or lifecycle race tests. It does not measure statement order within a scenario,
mixed-version rollout, malformed inputs, backup certification, or delete-receipt
fencing; retain the owning integration tests for those contracts.
