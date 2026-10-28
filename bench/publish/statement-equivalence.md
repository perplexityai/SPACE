# Publication statement equivalence

`publish-statements` captures the real service's SQL-call multiset for twenty-five
synthetic scenarios. It checks empty, node-status, template, ordinary-single,
full-inventory, snapshot inventory and its clearing, failure evidence and its
clearing, degraded and explicitly absent
observations, stopped, restore-completed, suspend-completed,
activation-rollback, and pause-failed-recovery publications. Repeated reports,
quarantine entry, and full-report pruning exercise retained state as well.

The database is freshly seeded with the same five transition fixtures used by
the CPU workload. Each scenario subtracts service-role `pg_stat_statements`
call counts immediately before and after its RPC. It preserves exact SQL text
and multiplicity, removing only the leading Rust locator comment. A scenario
fails if acceptance changes, counters regress, or a seeded rare transition
does not complete. Snapshot scenarios also check that the reported description
persists and that an explicit empty report clears its accepted scope.
After each measured RPC, the harness exports the nine publish-owned tables used
by the workload row comparator. These reads use the fixture connection, outside
the measured service-role statement delta. The raw rows retain generated IDs,
timestamps, nulls, and opaque JSON without normalization.
This is component integration evidence, not a deployed smoke.

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

Compare the per-scenario rows separately:

```sh
python3 scripts/compare-publish-rows.py --scenarios \
  /tmp/publish-statements-base/statements.json \
  /tmp/publish-statements-head/statements.json \
  --output /tmp/publish-scenario-row-comparison
```

The row comparator uses the same explicit run-local normalization as the CPU
workload exports, including server-time order rather than independent clock
values. It requires every captured scenario and table to agree after that
normalization; a missing row or changed non-normalized field fails comparison.
Server-created quarantine episode UUIDs are named by sandbox and first-seen
episode order, consistently across all captured scenarios. Only UUIDv7 episodes
identified by the server-authored quarantine-entry history are eligible;
seeded IDs, receipt IDs, and opaque JSON are not rewritten. Replacing an episode,
breaking its state/history reference, or changing history multiplicity still
changes the comparison.
It does not compare wall-clock intervals except the node-status freshness
deadline, and it does not export tables outside the documented nine-table set.

These checks do not replace process-CPU measurements or lifecycle race tests.
They do not measure statement order within a scenario,
mixed-version rollout, malformed inputs, backup certification, or delete-receipt
fencing; retain the owning integration tests for those contracts.
