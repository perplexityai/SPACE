
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

## Scenario-scoped cluster knobs (kind)

Some kind-chart values are cross-deployment invariants, not free knobs: the
API's and cluster-controller's copies of the snapshot-retention grace must
stay equal because fork admission (`crates/libs/api/src/fork_operations.rs`)
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
