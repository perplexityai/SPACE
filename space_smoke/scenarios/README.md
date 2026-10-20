# Smoke scenarios

Each Python module in this directory declares one runnable smoke scenario.

The namespace before the first dot is the scenario category:

- `ecw.*` describes a client-facing ECW contract that should hold across the
  configured template matrix.
- `network_policy.*` exercises portable network-policy enforcement.
- `native.*` exercises the native API surface.
- `local.*` carries Kind-only control-plane capabilities.

Discovery derives the category from the ID and filters by the selected
`E2B_TEMPLATE`; scenario bodies do not contain registration or skip bookkeeping.
Callers can add their own package without modifying this inventory by setting
`SPACE_SMOKE_SCENARIO_PACKAGES` to a comma-separated list of scenario packages.
Each module in an extension package follows the same one-scenario contract.

New smoke tests against a SPACE deployment belong here or in an extension
package, not in untyped scripts or inline CI commands. The `space-validation`
skill's smoke-testing reference owns the authoring rules and the migration
contract for existing untyped smoke tests.

Scenario-owned sandboxes default to pause-on-timeout with auto-resume disabled.
This is the smoke framework's default, not the ECW API's omitted-field default.
It preserves state for explicit reconnect and cleanup.

## Fork coverage

The typed suite owns the public fork smoke coverage:

- `ecw.fork_isolation`: two concurrent different-key requests, byte-exact disk
  contents, distinct child identities, and source/child/sibling write isolation.
- `ecw.fork_idempotency`: concurrent same-key requests and a completed replay
  retain one child and its post-fork writes, even after the source changes.
- `local.fork_suspended_cross_node`: suspend and wait for the durable snapshot,
  cordon the source worker, fork onto another worker, and verify the source
  stays suspended and restores with its original data. Kind-only; the cordon
  is undone before scenario cleanup. This proves off-source placement and
  disk isolation, not the provenance of the bytes fetched during restore.

These replace `scripts/tests/smoke-kind-sandbox-fork.py`. Run an individual
scenario with `uv run --frozen --python 3.11 -m space_smoke run <scenario-id>`
against a configured bench. Kind CI discovers them through the existing
`--all` inventory and distributes them across its four isolated shards; no
additional runner or cluster bootstrap is introduced. Together they create
three sources and four distinct children, with concurrency capped at two fork
requests per scenario. Child cleanup is registered before dispatch with the
same idempotency key used for the request. If the reply is lost, cleanup
replays that key to recover and delete the child; replayed children share one
cleanup owner. Cleanup failures are reported, not treated as passes. A child
inherits its source's timeout action, so expiry is not a deletion backstop.
Admission 503s retry the same key at most three times within one request
budget. Concurrent and completed replay compare the entire public result,
including the credentials, without writing those credentials into evidence.

Scenario phase timings report runtime without a flaky latency assertion.
The fork shard-duration hints use local seed measurements; refresh them from
successful CI runs. Scheduling estimates are not measured CI runtimes.

## Temporarily omitted lifecycle cases

Auto-kill (both the omitted/default lifecycle and explicit
`on_timeout="kill"`) and access-triggered auto-resume are intentionally not
registered. The SPACE ECW API does not currently support those behaviors as
dependable client contracts. Add their scenarios when that support is
available; until then they must not make the smoke suite fail.
