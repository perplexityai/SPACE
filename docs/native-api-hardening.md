# Native API smoke coverage

Native API correctness, concurrency, and recovery checks are part of the ordinary
typed `space_smoke.scenarios` inventory. Scenario modules contain their workloads
and assertions directly; shared HTTP, concurrency, identity, and recovery
utilities live alongside the existing harness utilities.

## Run the native inventory

After preparing a dedicated Kind bench:

```bash
export SKIP_BOOTSTRAP=1
export KIND_CLUSTER=space-local
export SPACE_API_URL=http://127.0.0.1:8080
export SPACE_DATA_PLANE_URL=http://127.0.0.1:8090
export SPACE_TEMPLATE=seed:latest
export SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE=/absolute/path/to/token
bash scripts/tests/smoke-kind-typed.sh native
```

The existing `all` target runs the ECW pass followed by the native pass.
Its normal native selection now includes these checks; no extra package or
separate hardening workflow is needed. Existing duration-balanced sharding
also includes them.

Kind CI runs this inventory on main and on the existing path-selected Kind PR
workflow. This does not introduce Kind execution on every PR. Private
compositions invoke platform commands through their platform-command wrapper.

For HTTP-only access to an already deployed test environment:

```bash
SPACE_SMOKE_BACKEND=deployed SPACE_SMOKE_API_SURFACE=native \
  uv run --frozen --python 3.11 -m space_smoke run --all
```

Supply `SPACE_API_TOKEN`, the two endpoint URLs, and a template for that invocation.
Kind-only recovery scenarios are visibly skipped by the deployed backend.
Tenant isolation requires `SPACE_SMOKE_NATIVE_FOREIGN_TOKEN` there. On local
Kind, it creates and cleans up a temporary second organization and user when
no credential is supplied. The trusted Kind wrapper enables this with
`SPACE_SMOKE_NATIVE_TENANT_FIXTURES=1`; a direct invocation must either explicitly
acknowledge disposable identity setup with that flag or supply a foreign token.
Identity setup errors remain incomplete coverage.

The standard runner prints structured `RESULT:` records, suite summaries,
failure details, and reproducible rerun commands. Capture its output with
the ordinary smoke job logs.

## Coverage and quarantines

| Group | Checks | Normal selection |
|---|---|---|
| Contract | Command validation, changed-request idempotency conflicts, token scope, tenant isolation | Included |
| Contract | Completed DELETE replay, permanent startup errors | Included |
| Proposed contract | Overlapping replay credential validity | Ignored; explicit invocation only |
| Admission | Zero-vCPU rejection before accepting an operation | Included on Kind with bounded cleanup |
| Concurrency | Same-key create and command fanout, independent commands, pause/wake contention, slow-reader output integrity | Included |
| Recovery | Queued create across controller outage/API restart; original command session across data-plane restart | Included on Kind only |
| Dispatcher fairness | Independent work completes while an earlier valid suspend is blocked | Included on Kind only |
| Lifecycle | Stopped sandbox restarts with its retained rootfs | Included |

Quarantine uses the existing `quarantined=` scenario declaration. Implicit
selection prints the scenario and its reason and withholds it. Explicitly
naming it still runs its real assertions:

```bash
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=native \
  uv run --frozen --python 3.11 -m space_smoke run native_contract.completed_delete_replay
```

DELETE replay and permanent command-error violations fail.
Stopped restart must also read back the original filesystem
payload. These assertions run in normal selection alongside dispatcher fairness
and lifecycle contention.

The **Quarantined smoke backlog (non-blocking)** job inventories all declared
quarantines on every PR and applicable manual CI entry point. Pull CI owns the
single PR check; Kind Pull CI does not repeat it, and main pushes and
merge-group runs do not run it. It displays counts, reasons, and source links
in the job summary and warning annotations. The check fails when any scenarios
remain quarantined, and passes when the backlog is empty. It executes no
scenarios or clusters and is excluded from required aggregate checks, so its
failure does not block those gates, and it does not fail the Pull CI run that
calls it.
Discovery errors also fail the check and report the backlog as unknown, not
zero. The source of truth is the scenario declaration, not a second list.

Kind scenarios that need BYOK enroll a disposable organization before creating
their sandbox and remove its credential and identity after sandbox cleanup.
The shared Local organization is never toggled. Recovery backoff scenarios use
the real retry intervals and participate in duration-balanced sharding.

Named development benches require `SPACE_SMOKE_KIND_CLUSTER_UID` to match the
live `kube-system` namespace UID before privileged fixture operations. The
standard `kind-space-local` setup retains its existing isolation checks.

## Credential replay contract (deferred)

The current API rotates attachment credentials on an idempotent replay.
`native_concurrency.command_idempotency` remains required: same-key requests
share session and process identity, execute their side effect once, and return
an attachable fresh credential after concurrent requests finish.

`native_contract.command_replay_credentials` additionally expects the previous
credential to remain valid. That stronger guarantee is not agreed. Its reproducer
is explicitly `ignored`, appears as `IGNORED` in suite logs, and is excluded from
normal selection and the quarantine defect backlog. It is not fixed or counted
as a passing scenario. Run its unchanged assertions explicitly with:

```bash
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=native \
  uv run --frozen --python 3.11 -m space_smoke run native_contract.command_replay_credentials
```

The [review decision](https://github.com/perplexityai/SPACE/pull/79#pullrequestreview-5423918313)
defers overlap. The [separate storage proposal](https://github.com/perplexityai/SPACE/pull/91)
is not a prerequisite for the smoke repairs. Before adopting an implementation,
resolve replay versus explicit credential renewal, validity of earlier responses,
expiry, revocation, issuance bounds, and deployment compatibility together.

## Concurrency and recovery controls

Normal concurrency uses 24 requests, 8 workers, one attempt, and a 1 MiB
slow-reader payload. Larger samples remain an explicit parameter choice:

```bash
SPACE_SMOKE_NATIVE_COUNT=120 SPACE_SMOKE_NATIVE_CONCURRENCY=8 \
SPACE_SMOKE_BACKEND=kind SPACE_SMOKE_API_SURFACE=native \
  uv run --frozen --python 3.11 -m space_smoke run native_concurrency.independent_commands
```

Other controls are `SPACE_SMOKE_NATIVE_ATTEMPTS`, `SPACE_SMOKE_NATIVE_BATCH_SECONDS`,
`SPACE_SMOKE_NATIVE_OUTPUT_BYTES`, and `SPACE_SMOKE_NATIVE_READ_DELAY_SECONDS`.
They are bounded. First-attempt failures and retries remain separate evidence.
Optional API/data-plane endpoint lists must target the same deployment;
one load-balanced endpoint does not prove distinct-replica coverage.

Recovery stops the controller and replaces API/data-plane pods. The explicit
fairness regression also temporarily stops object storage. Use a dedicated
Kind bench and keep its port-forward supervisor alive. The Kind shell harness
sets the recovery context from `KIND_CLUSTER` and captures the kube-system UID.
The recovery fixture revalidates those identities, local Kind node inventory,
deployment ownership, absence of autoscalers, and a shared mutation lease.

When invoking a Kind-only fault scenario directly (including zero-vCPU
admission) rather than through the Kind shell harness, supply `SPACE_SMOKE_NATIVE_RECOVERY=1`,
`SPACE_SMOKE_NATIVE_RECOVERY_CONTEXT=kind-<cluster>`, and
`SPACE_SMOKE_NATIVE_RECOVERY_CLUSTER_UID=<kube-system namespace UID>`.
The fixture refuses missing or mismatched acknowledgments. These are target
safety guards; normal Kind suite execution prepares them automatically.

Both HTTP endpoints must be loopback forwards for that exact cluster.
Restoration verifies original replica counts, unchanged deployment configuration,
and ready pods before fixture cleanup. Failures remain visible; database sizing
is not changed. The zero-vCPU regression temporarily shortens only
the controller create-readiness and operation-age budgets, so an erroneously
accepted create can be drained. It restores the original literal settings
(including unset values) and verifies running pods before reporting admission
failure. This does not test the deployment's default operation deadline.

## Relationship to existing scenarios

These scenarios target gaps in the ordinary inventory:
- The existing command round trip proves one command; concurrent commands prove
  independent output and session behavior under overlapping callers.
- Native resident pause proves sequential durability; lifecycle contention proves
  competing resident intents converge and leave the sandbox usable.
- ECW large-output coverage does not exercise a delayed native SSE reader.
- ECW process reattachment does not replace a data-plane process.
- Concurrent ECW restore concerns sandbox execution/restore claims, not native
  create acceptance persisted across an API restart.
- Template-build isolation concerns private template ownership, not native
  sandbox, operation, process-list, and sandbox-list authorization.

The changed-request contract checks only the conflict boundary; successful
same-key create convergence is owned by the concurrent-create scenario.
Generated-client conformance stays in its existing script because it additionally
proves generated SDK/schema compatibility. Its error-envelope checks overlap
some server contracts intentionally across that client boundary.

The zero-vCPU regression includes a successful valid-create control, then
requires a non-retryable 400/422 response without accepted sandbox/operation IDs.
A later terminal operation does not turn invalid acceptance into a pass.

The dispatcher regression uses a valid suspend blocked on object storage, not
zero-vCPU acceptance. It first observes the slow operation running, then submits
independent work and requires that work to complete while the slow operation
is still pending. It restores storage and drains accepted work before reporting
the verdict. Fixing CPU admission therefore does not erase dispatcher coverage.

These scenarios remain regression coverage for their admission, replay, error,
and scheduling contracts.
