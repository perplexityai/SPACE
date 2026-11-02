# Smoke scenarios

Each Python module in this directory declares one runnable smoke scenario.

The namespace before the first dot is the scenario category:

- `ecw.*` describes a client-facing ECW contract that should hold across the
  configured template matrix.
- `network_policy.*` exercises portable network-policy enforcement.
- `native.*` exercises the native API surface.
- `local.*` carries Kind-only control-plane capabilities.
- `node.*` exercises node-local CLI contracts on an explicitly dedicated,
  otherwise idle standalone worker. These scenarios are opt-in and use the
  `node` API surface with the `deployed` backend.

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

## Standalone manager recovery

`node.runtime_survives_manager_restarts` creates a pinned sandbox, restarts
supervisor/node-manager/sandbox-manager while it runs, then repeats the restarts
while it is stopped. It checks execution and guest-boot continuity, disk
persistence across cold start, and deletes only its own sandbox. It does not
request devices or establish GPU recovery evidence.

Run only on an authorized dedicated node. Set `SPACE_SMOKE_BACKEND=deployed`,
`SPACE_SMOKE_API_SURFACE=node`, and
`SPACE_SMOKE_NODE_RECOVERY=restart-idle-node`. Supply the target through
`SPACE_SMOKE_NODE_HOST` (SSH destination), `SPACE_SMOKE_NODE_MACHINE_ID`
(`/etc/machine-id`), and `SPACE_SMOKE_NODE_CLIENT_ENV` (remote shell environment
for the installed clients). Pin the inputs with `SPACE_SMOKE_NODE_TEMPLATE`,
`SPACE_SMOKE_NODE_CONTENT_HASH` and `SPACE_SMOKE_NODE_ENVIRONMENT`. Then run:

```sh
uv run --frozen --python 3.11 -m space_smoke run node.runtime_survives_manager_restarts
```

The fixture refuses a nonempty node or mismatched machine identity. Its use
requires exclusive operator access for the duration; the initial empty-node
check does not fence other administrators. A lost create reply is recovered
from the run's private response directory, never by deleting all sandboxes.
An ambiguous create outcome fails cleanup visibly and retains that directory.

`node.runtime_crash_preserves_disk` additionally requires
`SPACE_SMOKE_NODE_FAULT=crash-owned-runtime`. It uses a pidfd and verifies the
captured execution owner before killing only the run-owned runtime process
(Firecracker or CH). The scenario requires convergence to `stopped`, a new execution on restart and the
preserved disk marker. Ordinary test cleanup still deletes only its sandbox.
Neither this fault nor manager restarts request or qualify GPU assignment.

`node.runtime_memory_lifecycle` uses the same standalone-node inputs with a
snapshot-capable environment. It checks pause/resume, full snapshot capture,
repeated suspend/restore, and manager restarts both while suspended and after
restore. Disk and RAM markers plus guest boot identity distinguish restoration
from a cold boot; blocking guest readiness checks the restored vsock transport.

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
`--all` inventory and distributes them across its isolated shards; no
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

## Opt-in fork pressure

`local.fork_saturation_safety` is an explicit, opt-in Kind pressure check. It
runs fork waves of 1, 2, 4, and 8 against a durable suspended source, deletes
each wave before continuing, and checks an existing sandbox's commands and
files throughout. It requires at least one child to share the health sentinel's
worker, verifies source/sibling disk isolation, and reports phase and probe
timings. It changes no worker limits, faults, or scheduling configuration.
Use a dedicated bench with headroom for eight children and the sentinel,
including control-plane and object-store disk space, not only worker resources.
This is bounded functional evidence, not proof of maximum capacity or a latency SLO.
Fork calls retain the normal response wait rather than a short client-side
timeout: the server owns the operation deadline, and cleanup replays the same
idempotency key if a response is lost.

## Temporarily omitted lifecycle cases

Auto-kill (both the omitted/default lifecycle and explicit
`on_timeout="kill"`) and access-triggered auto-resume are intentionally not
registered. The SPACE ECW API does not currently support those behaviors as
dependable client contracts. Add their scenarios when that support is
available; until then they must not make the smoke suite fail.

## Exit retention on native

`ecw.background_process_control` requires the native API to keep reporting a
killed process as `exited` by `SIGKILL`. Templates built from this tree do, so
the Kind backend requires it. Deployed clusters run templates that may predate
that guest; there the scenario accepts an absent exit and still requires the
`SIGKILL` exit when one is reported. Set `SPACE_SMOKE_REQUIRE_EXIT_RETENTION=1`
or `0` to override the backend default.

## Standalone Windows lifecycle

`node.windows_sandbox_lifecycle` requires the same explicit node identity and
pinned template/environment inputs as the recovery scenario, with
`SPACE_SMOKE_NODE_RECOVERY=firmware-lifecycle`. It creates a task-owned Windows
sandbox, checks RDP NLA negotiation through the fenced operator channel, then
stops/starts and deletes that sandbox. Agent refusal before wake is covered by
the sandbox-manager guest-image tests. The scenario neither
restarts managers nor allocates devices, so other authorized sandboxes may stay
running. RDP negotiation does not establish authenticated interactive access;
that remains a separate image acceptance check.
