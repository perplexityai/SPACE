
# Run and maintain SPACE smoke tests

Read `infra/space/kind/README.md` for the local runtime harness and inspect
`scripts/space-cluster-smoke.py` for deployed suite selection. Select the
narrowest suite that exercises the affected public behavior. When Firecracker
or Kubernetes behavior needs production-shaped local proof, read `kind-devbox-testing.md` if supplied; otherwise use the
deployment's Kind runbook.

The typed `space_smoke` scenarios are the shared suite. Do not fork copies for
the in-cluster runner. Keep strict dependency behavior and distinguish a local
environment skip from a behavioral pass.

## Authoring smoke coverage

This rule covers smoke tests that exercise a running SPACE deployment, Kind or
deployed: every typed fixture requires one. Component and substrate smokes that
run without a deployment, such as the guest-kernel boot check or a smoke that
drives a single binary directly, are out of its scope.

New smoke tests are always typed Python `space_smoke` scenarios. Public
coverage, including Kind-only `local.*` scenarios, goes under
`space_smoke/scenarios/`. A deployment keeps its own coverage in a caller-owned
scenario package. Direct `python -m space_smoke` runs load it through
`SPACE_SMOKE_SCENARIO_PACKAGES` (see `space_smoke/scenarios/README.md`). The
in-cluster runner, `scripts/space-cluster-smoke.py`, drops a caller's value and
sets the variable from each suite's `scenario_packages` field, so a suite that
declares no packages runs only the public inventory.

If a new test needs a missing operation, extend the typed fixtures, adapters, or
capabilities alongside the scenario. Follow `space_smoke/AGENTS.md` for what a
scenario may assert on, and the API boundary in `infra/space/kind/AGENTS.md` for
Kind state manipulation.

Don't introduce a smoke test outside the framework: no new Bash or standalone
Python smoke script, and no new test added to an existing script or to inline
CI/configuration commands. Thin shell wrappers may bootstrap the environment or
launch the typed runner; a wrapper that launches it propagates its exit status.

Existing untyped smoke tests, Bash or standalone Python, may be updated and
should eventually migrate to typed tests. A change adds a new test when it
asserts a contract the existing suite didn't already cover. Changes that keep
an existing check working, or strengthen a contract it already asserts, are
maintenance. Use judgment: keeping an existing test cohesive in its current form
can be clearer than splitting its functionality between it and typed code.
Maintenance doesn't require piecemeal splitting or immediate migration.

Migrate whole tests when ready. A migrated test keeps every action, assertion,
cleanup path, exit status, and pipeline gate it had. Remove it from the legacy
script once the typed scenario has passed on the environments that ran it, and
retire the script when no tests remain in it.

## The whole Kind suite on one host

On a dedicated Linux KVM development host whose owner has agreed to the
host-wide changes below, a full Kubernetes Kind validation is
`bash scripts/tests/smoke-kind-parallel.sh`, and its `summary.md` (with
`summary.json`) is the evidence: one table across benches, every scenario that
did not pass with its log, and a check that the benches cover the inventory
exactly once (each (surface, scenario) pair has one `=== RUN`, or was skipped
by a bench's mode after the partition, which counts as covered). Its exit
status ranks as a typed suite's does: `1` when a scenario failed, else `2` for an
incomplete run (a bench down, an inconclusive scenario, a coverage gap), else
`42` for a warning, else `0`. Don't run the CI shards one after another on one
stack instead.

The runner changes the host beyond its benches, and some changes stay: it
raises kernel limits, loads device-mapper modules and stages loop-device nodes,
installs packages and Docker's static release under `/opt`, adds iptables rules,
a dummy interface and two registry containers that keep running between runs,
and holds about 50 GB of disk per bench. It has been run on two host shapes so
far (32 CPUs with 123 GiB and 48 CPUs with 185 GiB, each with one ~330 MB/s
volume). Don't start it without the host
owner's go-ahead, even with passwordless sudo. On any other machine, and against
a stack that is already up or to reproduce one shard, use
`smoke-kind-typed.sh all [i/N]`.

The runner starts N isolated benches side by side, each one CI shard runner
(its own network and PID namespaces, dockerd and checkout), builds Rust and the
shared images once, and picks N from the host's CPUs, available memory, free
disk and a short write-throughput probe of the bench volume (`--benches N`
overrides). It takes about 23 minutes at N=4 or 5 on that host shape with a
warm Rust target directory. A scenario that fails here fails the run. If it
passes when rerun, report both runs. Call a failure contention only when it
fails beside other benches more than once and passes each time with
`--benches 1`. "The whole suite on one host" in `infra/space/kind/README.md`
lists every host change, what each bench shares, and what it costs.

## Scenario-scoped cluster knobs (kind)

Some kind-chart values are cross-deployment invariants, not free knobs: the
API's and cluster-controller's copies of the snapshot-retention grace must
stay equal because fork admission (`crates/l40_api/api/src/fork_operations.rs`)
trusts the API's copy to describe what the controller will actually prune.
Do not bake smoke-sized reconciler knobs (tick, grace, dry-run) into the kind
chart values; before treating any chart value as arbitrary, grep the env var
name under `crates/` for the consuming validation.

Scope such knobs to the scenario instead: record the deployment's standing
env through a named, allowlisted operation, apply the overrides, await the
rollout, and restore the record in a scenario phase rather than in scenario
cleanup — the cleanup budget is `min(120 s, 10 %)` of the scenario timeout
and cannot absorb a rollout wait. `infra/space/kind/README.md` documents
the local kind harness these scenarios run on.

## In-cluster smoke Jobs

`scripts/space-cluster-smoke.py` is the shared in-cluster runner. The owning
deployment supplies its endpoint, credentials, template matrix, suite selection,
and strictness through the runner's documented environment variables.

A configured template that the cluster lacks fails its Job. Preserve the
suites' `0` / `42` / `1` verdict vocabulary through the container, Job invoker,
and Buildkite step. Cluster-local private DNS enables the stock SDK but does not
validate the consumer-side PrivateLink endpoint.
