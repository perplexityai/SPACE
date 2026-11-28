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

## Selection tiers

Every scenario has a `tier`: `routine` (the default), `main`, or `manual`.
Pull CI selects routine scenarios; Main CI selects both routine and main.
Placement-related pull requests also select the main-tier node-selector
scenario. The tier controls when a scenario runs, not what it proves.
Template, API surface, backend, substrate, test-hook and browser requirements
still apply. Quarantine is separate from tier and withholds a known defect
from implicit selection. A scenario that declares `drain_v1` drives v1 node
drains, which the admin API refuses while drain v2 is on
(`SPACE_DRAIN_V2_ENABLED`); a suite run skips it, visibly, on a bench whose
public API runs drain v2, as Kind's values do, so run those on a bench with
drain v2 off.

Manual scenarios run only by name and must declare a nonempty `manual_reason`.
Use manual for an explicit operational prerequisite or a contract whose
acceptance is still deferred; state that prerequisite or decision in its reason. Named
runs can select any tier, while `--all` and `--category` default to routine:

```sh
uv run --frozen --python 3.11 -m space_smoke run --all --tier routine --tier main
uv run --frozen --python 3.11 -m space_smoke list --tier manual
uv run --frozen --python 3.11 -m space_smoke run local.garbage_collectors_reclaim_independent_resources
```

A set that runs by name, manual scenarios included, splits across benches
with the same duration hints as `--shard` (`space_smoke/sharding.py`): `list`
takes an ID prefix and a shard, and each bench runs its share by name, one
scenario after another. For the drain v2 set on four benches:

```sh
uv run --frozen --python 3.11 -m space_smoke list --prefix native.v2_ --tier routine --tier manual --shard 1/4
```

`scripts/tests/smoke-kind-parallel.sh --scenario ID --repeat N` brings the
benches up and runs named scenarios, repeated, across them in one command.
It sets `SPACE_SMOKE_RUN_SCENARIOS`, a comma-separated list an `--all` pass
runs instead of its tier selection, as often as listed and, within each
API surface's pass, in the order listed.

`SPACE_SMOKE_TIERS=routine,main` selects the same tiers for suite wrappers.
`SPACE_SMOKE_INCLUDE_SCENARIOS` adds named main-tier cases to a routine run
before sharding; it does not bypass environment requirements or quarantine.
Extension packages using `ignored="reason"` must migrate to
`tier="manual", manual_reason="reason"`.

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

`node.windows_memory_lifecycle` exercises manual and automatic pause/suspend,
wake, live-process continuity, SSH and native process/file operations on a
standalone Windows guest. It requires `SPACE_SMOKE_NODE_RECOVERY=firmware-lifecycle`
and `SPACE_SMOKE_WINDOWS_OPERATIONS_PROBE` pointing to the matching built probe.
It does not restart managers. Use the same pinned node and template inputs.

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
The fork scenarios' shard-duration hints are Kind CI medians, like the rest
(`space_smoke/sharding.py` names the runs); refresh them all together.

## Standalone base memory

`node.runtime_base_memory` and `node.windows_base_memory` use the standalone
node inputs above to qualify template memory and pools. The Windows scenario
requires the `firmware-lifecycle` opt-in and the native operations probe used
by `node.windows_memory_lifecycle`. Both require an idle worker. They copy the
pinned input into a uniquely named local test template, build a base snapshot,
restore siblings, and verify that one sibling's suspend/restore preserves its
process while the other continues independently. They then fill and claim both
snapshot-seeded and cold-seeded pools, and delete their own templates and
sandboxes. These scenarios reserve ten minutes of their 25-minute runner budget
for cleanup, including sequential firmware shutdowns. Cleanup disables pool
refill first, shares one deadline, propagates runner timeouts and verifies that
owned sandboxes are absent after template deletion. A failed or interrupted run
can still require manual cleanup of the reported run-owned resources before
the next idle-node check succeeds. These scenarios do not change the source
template's policies.

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

## Native inline execution coverage

`native_recovery.inline_load_survives_leader_failover` also covers a wake
queued behind a suspend. It admits and checks that ordered pair before
filling any inline permits, so permit exhaustion cannot explain why the
wake stays queued. A separate canary proves that the new leader dispatches
work while the inline claims remain live. The ordered pair must finish with
an executing guest, and every inline create and wake must still have exactly
one claim and one terminal event.

## Leader-election coverage

`local.leader_election_brownout_keeps_one_leader` runs every leader-election
fault in one scenario, so their budget waits overlap. Both healthy replicas
are cut off from the apiserver in their own network namespaces while the
control plane stays up for every other client: the holder stops before its
Lease can be taken over, and the pair recovers to one holder and one runner.
Alongside, a contender whose Lease calls are refused by real RBAC must stay
NotReady and unrestarted past the liveness budget it reports, and a second
contender of the same image must spend its reported budget against an
unreachable pod-local API endpoint and exit non-zero, which qualifies the
shortened budget. The outage ends well inside a healthy replica's budget, so
the control's is the scenario's budget exit.

## Cross-node restore coverage

The implicit Kind suite runs
`local.deferred_btrfs_send_capture_restores_cross_node`, which proves remote
restore and preserved state while also checking deferred capture and codec
selection. It verifies the selected parent template, remote placement,
restored filesystem state and working data-plane execution.

## Execution environment coverage

Kind nodes run node-manager's default policy, E0 and `legacy`, and bootstrap
stages only E0's Firecracker. A scenario that needs another compiled
environment names it in `SandboxConfig.kind_execution_environment`, or calls
`KindAdmin.admit_execution_environment`. Setup then does three things under the
node-manager mutation Lease (`space_smoke/execution_environment.py`):

- It adds the environment to `SPACE_ALLOWED_EXECUTION_ENVIRONMENTS` on the
  worker and builder node-manager DaemonSets, keeping the E0 default, and arms
  the restore of the recorded literals.
- It installs the runtime on every node through `space-rtmgr runtimes install`.
- It waits until every node's enforced policy in
  `GET /admin/api/v1/runtime-environments/nodes` allows the environment and
  every Node carries node-ctl's `space.pplx.net/env-...=usable` label for it.

For a sandbox, setup also builds seed bound to the environment through the
admin system-template build, publishing no tag. The sandbox is created from
that build's digest, so `latest` never moves. The scenario deletes the sandbox,
then calls `withdraw()`, which deletes the seed release, puts the allowlist
back and checks that it took. On a failure, cleanup does the same in that
order, so such a scenario raises its cleanup budget to cover two rollouts. A
run killed before its restore keeps the Lease for four hours, during which
another admission is refused as the Lease's; after that, admission refuses
the widened allowlist and names the manual fix. An
installed runtime stays installed, since a compiled binding's artifacts are
removal-protected. Which environment a sandbox runs is read from its running
VM: what its Firecracker executable reports for `--version`.

- `local.selected_environment_survives_suspend_and_cross_node_restore` runs a
  sandbox bound to Firecracker 1.17.0 / Linux 6.1.155 through create, suspend
  and restore, and a cross-node restore from object storage. It checks the
  running executable and the guest's state each time.
- `local.template_campaign_canary_rolls_and_pauses` starts a templates
  campaign from E0 to that environment through the admin API. The canary moves
  one template and waits; resumed, the campaign rolls; paused mid-wave, it
  leases nothing more over several loop ticks; resumed again, it completes. It
  ends inconclusive, naming what's missing, until the bench serves the campaign
  admin API, runs the cluster-controller's runtime-upgrade loop and has the
  template driver.

Both are manual. They roll every node-manager, so run them on a bench no other
scenario shares while they run.

`local.environment_placement_gate_on_places_bound_pods` turns
`SPACE_ENVIRONMENT_PLACEMENT_GATE` on (`space_smoke/environment_placement_gate.py`)
and proves what enforcement does to placement. It drives the admission itself
in two stages: Firecracker 1.17.0 / Linux 6.1.155 on the builders only, so its
seed can be built while no worker runs it, then on the workers. Beside it, it
builds seed bound to E0 under no tag. With the gate on, the E0 create runs E0
on a worker, and its pod requires E0's usable label. The other create is
refused with 503 as unschedulable and leaves no sandbox. Once the workers run
the environment, the same create runs it there. The gate's restore is armed
before the write that turns it on, and writes the API's literal before the
controller's. The standing literals are recorded on the public API mutation
Lease first, so a Kind scenario's preflight repairs a run killed with the
gate on: the next one on the same host and user, or the first after the
Lease's four hours elsewhere. It refuses while resume-time runtime upgrades are on, since the
substrate refuses `on` beside them at startup, and it refuses a gate literal
an earlier run left behind. It's manual
for the same reason as the scenario above, and it also rolls the API, the
admin API and the cluster controller.

## Garbage collection coverage

The implicit Kind suite runs
`local.garbage_collectors_reclaim_independent_resources`. It prepares a cold
suspended sandbox, a separate live fork source, and a separate live template
pin before waiting for their GC loops. Each candidate is positively observed
before reclamation; the original durability, residue, protected-template and
guest-state assertions all run afterward. The existing minimum idle and grace
periods overlap without being shortened, and daemon configuration is unchanged.

## Admin console

The `local.console_*` scenarios drive the bench's admin console (`ui/`)
through Playwright's Chromium and judge each page against the admin API it
renders (`space_smoke/console.py`):

- `local.console_sandbox_lookup_accepts_every_id_form`: the `sbx_` ID, bare
  and undashed UUID, and pod name each open the sandbox's page and narrow a
  list of it and a second sandbox to it; an ID of no sandbox is not found.
- `local.console_sandbox_detail_complete_in_every_state`: running, suspended
  and deleted, the page shows the record, the valid lifecycle moves, and the
  recorded operations.
- `local.console_operator_suspends_another_orgs_sandbox`: the operator's
  Suspend on another organization's sandbox suspends it, ownership unchanged.
- `local.console_destructive_actions_require_confirmation`: sandbox delete and
  role change ask first, and a cancelled confirmation sends nothing.
- `local.console_turns_away_non_operators`: another organization's admin and a
  managing-organization member get the "not for you" notice; the operator
  does not.
- `local.console_links_resolve`: list, sandbox, organization, owner and node
  pages link to each other and resolve. Main has no template page, so the
  templates list is only checked to carry the sandbox's template.

Each one creates its own organizations, users and foreign-organization
sandboxes under a run-unique slug, and deletes them in cleanup. Each also
fails on any browser console error, uncaught page error, failed request or
HTTP error answer outside the allowlisted `BASELINE` in `console.py` and the
scenario's own expected probes; every baseline entry states why it is
expected.

They need Playwright for Python, the optional `console` dependency group,
which the in-cluster smoke image does not install, and Playwright's
Chromium, which is not a Python dependency. Install both, the browser with
the system libraries it loads:

```sh
uv sync --frozen --python 3.11 --group console
uv run --frozen --python 3.11 python -m playwright install --with-deps chromium
```

A later `uv run --frozen` keeps the group. Without either, a suite run
prints each scenario `SKIPPED ...: needs a chromium browser` with the
reason, and a run by name ends inconclusive before its fixture creates
anything. `SPACE_SMOKE_BROWSER_SCENARIOS` selects them:

- unset or `run`: run them where the browser launches, and skip them,
  visibly, where it does not;
- `require`: run them, and fail the suite (exit 2) where the browser does not
  launch, as CI does when it selected them;
- `withhold`: leave them out, visibly, before the shard partition, so the
  shards are those of a registry without them.

`SPACE_SMOKE_CONSOLE_URL` overrides the console origin (default
`http://127.0.0.1:3000`, Tilt's `space-ui` forward).

See [Known gaps](#known-gaps) for their current CI selection.

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

## Known gaps

These are limits of the current Kind suite.
Remove an entry when deployed coverage establishes its contract.

- **IMDS precedence on a live node.** The policy evaluator covers an explicit
  tenant allow for `169.254.169.254`, and node-manager snapshots cover rendered
  nft rules. Neither proves the installed gateway/nft path or rule order after
  a policy update. A timeout without a reachable metadata-address control
  cannot distinguish a guardrail drop from an unavailable endpoint;
  reachability on individual CI hosts has not been established. Closing this
  gap needs a run-owned responder at that address in each Kind worker's
  network namespace, independent proof of responder reachability and of the
  intended enforcement path, and blocked guest probes before and after a
  tenant policy update. An unrestricted guest is not a positive control for
  a guardrail that intentionally blocks every guest.
- **Negative ingress authorization.** Successful wake traffic and Rust auth
  tests do not establish that a deployed listener rejects a legacy static
  bearer or an untrusted projected identity while admitting the trusted
  projected caller. Live negative coverage of TokenReview/fallback wiring is
  absent; no bypass has been demonstrated.
- **Browser console scenarios in CI.** Kind CI currently withholds the
  browser scenarios. Pull CI's selector can require them for paths in
  `ci/kind-console-paths.txt`, behind the `SPACE_KIND_CONSOLE_SMOKE` switch in
  `.github/workflows/pull-ci.yml`. That switch remains off until the Kind
  runners carry the optional console dependency group and Chromium. Framework
  and API checks do not establish the browser behavior those scenarios assert.

## Current scenario index

Generated from the built-in registry. The environment column reports the
declared fixture capability, supported backends and control-plane substrates;
it does not claim that every assertion requires Kind. The claim summary is the
opening paragraph of the scenario's invariant. Follow the scenario link for
its complete contract, setup and assertions. Tier alone does not establish actual selection:
the suite also applies the environment requirements described above.

Python CI runs the freshness check before its tests. Refresh after changing
declarations, or check without writing:

```sh
uv run --frozen --python 3.11 -m space_smoke.inventory
uv run --frozen --python 3.11 -m space_smoke.inventory --check
```

<!-- BEGIN GENERATED SCENARIO INDEX -->

| Scenario | Tier | Required environment | Claim summary |
|---|---|---|---|
| [ecw.access_idle_reconnect_preserves_workspace](ecw_access_idle_reconnect_preserves_workspace.py) | routine | `ecw`; deployed, kind; direct, kubernetes | After access-idle pause has durably settled, reconnecting by the stored sandbox ID restores exact text and binary workspace files. |
| [ecw.background_process_control](ecw_background_process_control.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | A background command is discoverable and controllable by process ID. |
| [ecw.cold_create_environment](ecw_cold_create_environment.py) | routine | `kind_ecw`; kind; kubernetes | The first guest command after public cold creation preserves UTF-8, newlines, equals signs, and present empty environment values without a reconnect, suspend, or restore. |
| [ecw.command_round_trip](ecw_command_round_trip.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | The selected public API returns the sandbox’s own identity, its data plane returns exact command output, and deletion makes the sandbox publicly absent. |
| [ecw.domain_access_survives_default_deny](ecw_domain_access_survives_default_deny.py) | routine | `ecw`; deployed, kind; direct, kubernetes | An explicitly allowed domain remains reachable when the ECW request also carries the E2B deny-all CIDR marker shape. |
| [ecw.filesystem_mutations](ecw_filesystem_mutations.py) | routine | `ecw`; deployed, kind; direct, kubernetes | ECW filesystem operations create explicit and missing directories, preserve streamed content, expose renames and file removal, and recursively remove a non-empty tree. |
| [ecw.filesystem_ownership](ecw_filesystem_ownership.py) | routine | `ecw`; deployed, kind; direct, kubernetes | Files written as root, explicit user, and default user retain their owner; parents auto-created for a user-owned workspace file remain writable by that user. |
| [ecw.filesystem_private_boundary](ecw_filesystem_private_boundary.py) | routine | `ecw`; deployed, kind; direct, kubernetes | MakeDir, Stat, ListDir, Move, and Remove reject omitted and user identities under a root-private directory while explicit root succeeds. |
| [ecw.filesystem_read_identity](ecw_filesystem_read_identity.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A query-selected root can read a root-private file, an explicit user cannot, and Basic root authentication cannot replace the query selector. |
| [ecw.filesystem_watcher](ecw_filesystem_watcher.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A recursive ECW filesystem watcher reports a write to the exact nested path and can be stopped through its SDK handle. |
| [ecw.fork_idempotency](ecw_fork_idempotency.py) | routine | `ecw`; deployed, kind; kubernetes | A replay returns the existing child, including its post-fork writes. |
| [ecw.fork_isolation](ecw_fork_isolation.py) | routine | `ecw`; deployed, kind; kubernetes | Different keys produce independent children while the source stays usable. |
| [ecw.guest_mptcp_segments_dropped](ecw_guest_mptcp_segments_dropped.py) | routine | `ecw`; deployed, kind; direct, kubernetes | An MPTCP SYN gets no host reply while the same plain SYN does. |
| [ecw.guest_vlan_frames_dropped](ecw_guest_vlan_frames_dropped.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A SYN inside VLAN tags gets no host reply while the same untagged SYN does. |
| [ecw.immediate_egress](ecw_immediate_egress.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A newly created ECW sandbox can immediately reach an external HTTPS endpoint through its configured egress path. |
| [ecw.internet_access_allowed](ecw_internet_access_allowed.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A sandbox created with allow_internet_access=True can reach a public HTTPS endpoint. |
| [ecw.internet_access_denied](ecw_internet_access_denied.py) | routine | `kind_network_ecw`; kind; direct, kubernetes | A sandbox created with allow_internet_access=False consistently blocks a public HTTPS endpoint. |
| [ecw.large_multipart_upload_round_trip](ecw_large_multipart_upload_round_trip.py) | routine | `ecw`; deployed, kind; direct, kubernetes | Files written together in one multipart Files API request, each larger than one write message, are stored byte-exactly at their own paths. |
| [ecw.large_stdout_integrity](ecw_large_stdout_integrity.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A large Files API payload is stored byte-exactly and crosses the ECW process stream without truncation, duplication, or corruption. |
| [ecw.loopback_route_stays_open](ecw_loopback_route_stays_open.py) | routine | `ecw`; deployed, kind; direct, kubernetes | An active ECW public-port connection to a loopback-only service stays open past the former bridge cleanup window and delivers every delayed line. |
| [ecw.pause_connect](ecw_pause_connect.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | A durable lifecycle round trip preserves sandbox filesystem state. |
| [ecw.pause_connect_race](ecw_pause_connect_race.py) | routine | `ecw`; deployed, kind; direct, kubernetes | Consecutive pause and immediate reconnect cycles each restore the same sandbox's data plane and preserve that cycle's exact marker. |
| [ecw.pause_timeout_requires_connect](ecw_pause_timeout_requires_connect.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A sandbox using the smoke fixture's default pause-on-timeout lifecycle remains unavailable until an explicit connect, then returns without losing its filesystem state. |
| [ecw.process_identity](ecw_process_identity.py) | routine | `ecw`; deployed, kind; direct, kubernetes | An omitted process user receives the unprivileged user login identity, while an explicit root process receives the root login identity. |
| [ecw.process_reattach](ecw_process_reattach.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A client can disconnect from a running ECW process, reattach by its process ID, and receive its final output and exit status. |
| [ecw.public_port_to_all_interfaces](ecw_public_port_to_all_interfaces.py) | routine | `ecw`; deployed, kind; direct, kubernetes | The same sandbox and public port survive a guest listener refusal burst. |
| [ecw.refresh_keepalive](ecw_refresh_keepalive.py) | routine | `ecw`; deployed, kind; direct, kubernetes | Refresh and same-value set_timeout independently reset the idle deadline and keep their sandboxes usable beyond their original deadlines. |
| [ecw.sandbox_list_pagination](ecw_sandbox_list_pagination.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | Pagination returns every run-owned sandbox exactly once across multiple pages through the selected API. |
| [ecw.timeout_update](ecw_timeout_update.py) | routine | `ecw`; deployed, kind; direct, kubernetes | An ECW sandbox remains usable after its original deadline when its timeout is extended through the API, and a timeout set while it is suspended is the one it runs under once restored. |
| [ecw.warm_pool_environment](ecw_warm_pool_environment.py) | routine | `kind_ecw`; kind; kubernetes | An already-started pool member exposes create-time env on its first process, while command env overlays one value without removing the rest. |
| [ecw.warm_pool_shaped_claim](ecw_warm_pool_shaped_claim.py) | routine | `kind_ecw`; kind; kubernetes | A create whose `space.resources` matches the ready slot's shape adopts that slot and runs with the requested vCPUs. |
| [ecw.watcher_identity](ecw_watcher_identity.py) | routine | `ecw`; deployed, kind; direct, kubernetes | Root can watch a root-private directory, omitted and user identities cannot, and watcher follow-ups remain authorized by their opaque handle. |
| [local.btrfs_send_restore_fails_closed_without_parent_source](local_btrfs_send_restore_fails_closed_without_parent_source.py) | routine | `kind_ecw`; kind; kubernetes | A sandbox suspended with the production stream codec, restored onto a worker that lacks its parent template while the cluster has no storage location for that template, fails: the client sees the failure, sbx-manager reports the restore failed at the template lookup, and no instance or template is materialized on that worker. |
| [local.btrfs_send_restore_pulls_parent_template](local_btrfs_send_restore_pulls_parent_template.py) | routine | `kind_ecw`; kind; kubernetes | A sandbox suspended with the production stream codec restores onto a worker that lacks its parent template: the worker pulls the template on demand, installs it as a btrfs subvolume with the deterministic received UUID, receives the stream into a subvolume instance, and the sandbox comes back from object storage with its state intact. |
| [local.btrfs_send_restore_rejects_corrupted_stream](local_btrfs_send_restore_rejects_corrupted_stream.py) | routine | `kind_ecw`; kind; kubernetes | A sandbox suspended with the production stream codec whose durable stream is corrupted in object storage fails to restore onto another worker: the client sees the failure, sbx-manager reports the restore failed at the receive with node-manager's aborted `btrfs receive`, and the target holds no instance or partial receive. |
| [local.concurrent_restore_converges_to_one_execution](local_concurrent_restore_converges_to_one_execution.py) | routine | `kind_ecw`; kind; kubernetes | Twenty concurrent connects to a suspended sandbox, spread over three API replicas, produce exactly one execution and one pod with every caller landing on it; a started sandbox whose pod is deleted recovers under the same contention onto one new generation restored from its suspend checkpoint; pod POSTs refused at admission retire their generations and the callers converge on the one that was admitted, the refused callers seeing the failure; and a delete issued after capped restore claims leaves no pod that ever runs, no live sandbox, and no hung or crashed caller behind. |
| [local.console_destructive_actions_require_confirmation](local_console_destructive_actions_require_confirmation.py) | routine | `kind_ecw`; kind; kubernetes | On a sandbox's page, Delete asks for confirmation; cancelled, the page sends no state-changing request and the sandbox stays running; confirmed, the sandbox is deleted. On a user's page, choosing another role asks for confirmation; cancelled, nothing is sent and the role is unchanged; confirmed, the role is the one chosen. The user is a member of another organization, so the new role grants no console access. No page reports a browser error outside the baseline. |
| [local.console_links_resolve](local_console_links_resolve.py) | routine | `kind_ecw`; kind; kubernetes | From the sandbox list, the operator follows the sandbox's link to its page; from there its organization and owner links, the owner's organization link, and the nodes list's link to the sandbox's node; and from each of those pages a link back to the sandbox. Every link points at the page for the object the admin API names, and every page resolves rather than showing a not-found or load-failure state. The templates page lists the sandbox's template. No page reports a browser error outside the baseline. |
| [local.console_operator_suspends_another_orgs_sandbox](local_console_operator_suspends_another_orgs_sandbox.py) | routine | `kind_admin`; kind; kubernetes | For a running sandbox of an organization other than the one managing the cluster, the operator's console page names that organization, and its Suspend leaves the sandbox suspended in the admin API, still owned by that organization and user, and shown suspended. No page reports a browser error outside the baseline. |
| [local.console_sandbox_detail_complete_in_every_state](local_console_sandbox_detail_complete_in_every_state.py) | routine | `kind_ecw`; kind; kubernetes | Running, the sandbox's page shows its ID and state, its template, its node, its organization by name and its owner by email, each linking to its page, offers exactly the lifecycle moves a running sandbox has, and lists every operation the admin API records for it, or says there are none. Suspended by an operator's admin action, restored, and suspended again, the same page shows it unassigned, offers only the moves a suspended sandbox has, and lists each operation, both suspends among them, once: by ID, kind and state. Deleted, it says the sandbox is gone and still lists the operations recorded against it. No page reports a browser error outside the baseline. |
| [local.console_sandbox_lookup_accepts_every_id_form](local_console_sandbox_lookup_accepts_every_id_form.py) | routine | `kind_ecw`; kind; kubernetes | Signed in as the operator, the sandbox's public `sbx_` ID, its bare UUID, the UUID without dashes and its pod's name each open the sandbox's canonical page from the detail route and, typed into the sandbox list's search while the list shows this sandbox and a second one, narrow it to exactly this one. A well-formed ID of no sandbox is reported not found by both. No page reports a browser error outside the baseline. |
| [local.console_seed_runtime_pair](local_console_seed_runtime_pair.py) | manual | `kind_admin`; kind; kubernetes | The selected qualified pair reaches the builder, produces a bootable seed, and leaves latest unchanged. The browser drives the real API; the admin API independently judges completion. Only newly registered, unpromoted releases and the test sandbox are cleaned.<br>Manual: Opt-in seed build: requires the qualified FC 1.17.0 / Linux 6.1.155 pair installed and admitted on the bench. |
| [local.console_turns_away_non_operators](local_console_turns_away_non_operators.py) | routine | `kind_ecw`; kind; kubernetes | An organization admin of another organization, and a member of the organization that manages the cluster, each sign in to the console's "not for you" notice, with no navigation, on the dashboard and on a deep link to the sandbox's page, which does not show the sandbox. The operator, an admin of the managing organization, gets the navigation and the sandbox's page. No page reports a browser error outside the baseline. |
| [local.deferred_btrfs_send_capture_restores_cross_node](local_deferred_btrfs_send_capture_restores_cross_node.py) | routine | `kind_ecw`; kind; kubernetes | A suspend under deferred capture retains its source and writes no `rootfs.btrfs-send`, the backup streams from the retained source, and the sandbox restores from object storage onto another node with its state intact. |
| [local.ecw_template_build_from_oci_boots](local_ecw_template_build_from_oci_boots.py) | routine | `kind_admin`; kind; kubernetes | With the builder Service pinned to one of two builders, a Dockerfile built from a public OCI image through the public E2B SDK is accepted there and its prepared rootfs committed; with the Service moved to the other builder for the rest of the build, the build still completes (status polls are pinned to the accepting builder, not routed through the Service); the published `metadata.json` names the installed `spaced` bundle and its compatibility family; containerd keeps no source, temporary image or assembly residue; and a sandbox created from the alias runs the Dockerfile's state, the SDK's environment and start command, SPACE's guest services, the UID 2000 user, filesystem I/O, processes, injected SSH keys and the source image's Python 3.12 on bookworm. |
| [local.encrypted_disk_fork](local_encrypted_disk_fork.py) | routine | `kind_ecw`; kind; kubernetes | Fork a sealed encrypted disk, replay it, and restore independently of its source. |
| [local.encrypted_live_disk_fork](local_encrypted_live_disk_fork.py) | routine | `kind_ecw`; kind; kubernetes | Capture encrypted disk state without suspending or restarting its guest. |
| [local.encrypted_resuspend_preserves_cow](local_encrypted_resuspend_preserves_cow.py) | routine | `kind_ecw`; kind; kubernetes | A BYOK sandbox survives suspend, restore, re-suspend, and a second restore with one stable sparse LUKS memory base and intact guest state. |
| [local.environment_placement_gate_on_places_bound_pods](local_environment_placement_gate_on_places_bound_pods.py) | manual | `kind_ecw`; kind; kubernetes | With the gate on, a create bound to an environment no worker can run is refused as unschedulable and leaves no sandbox, a create bound to E0 runs E0 on a worker labelled for it, and once the workers run the other environment its create runs it there. Gate and allowlists are back to what they were at the end.<br>Manual: Rolls the API, admin API and cluster controller to turn the environment placement gate on, rolls every node-manager to admit Firecracker 1.17.0 / Linux 6.1.155, installs that runtime on every node from its upstream release, and builds two seeds: run it on a bench no other scenario shares while it runs. |
| [local.forced_drain_evacuates_sandbox](local_forced_drain_evacuates_sandbox.py) | routine | `kind_ecw`; kind; kubernetes | A `:drain` with `mode: forced` parks the resident's worker in `draining` in forced mode and cordons it; the drain reconciler suspends the sandbox and restores it onto the other worker, from object storage, as `started`, with its filesystem intact and its data plane executing; the emptied node then reaches `drained` on its own with the guarded Kubernetes node still present; `:undrain` returns it to schedulable with no drain phase. |
| [local.fork_saturation_safety](local_fork_saturation_safety.py) | manual | `kind_ecw`; kind; kubernetes | Fork waves have distinct children with intact independent disks while a colocated, existing sandbox keeps answering commands and file reads. The source remains suspended, and each wave is deleted before the next one. This is a bounded load check, not a maximum-capacity or latency benchmark.<br>Manual: Explicit bounded fork-pressure run on a dedicated Kind bench |
| [local.fork_suspended_cross_node](local_fork_suspended_cross_node.py) | routine | `kind_ecw`; kind; kubernetes | Off-source placement preserves disk data without resuming the suspended source. |
| [local.garbage_collectors_reclaim_independent_resources](local_garbage_collectors_reclaim_independent_resources.py) | routine | `kind_ecw`; kind; kubernetes | Evict a cold sandbox, reap a fork snapshot, and reclaim an unpinned template. |
| [local.gentle_drain_leaves_sandbox_resident](local_gentle_drain_leaves_sandbox_resident.py) | routine | `kind_ecw`; kind; kubernetes | A `:drain` with no body parks the resident's worker in `draining` with `drain_mode = gentle` and cordons it; through a twenty-second observation the sandbox keeps its lifecycle, pod, pod UID and node pin, gains no snapshot, and the user API keeps serving it; once the sandbox suspends the node reaches `drained` on its own with the guarded Kubernetes node still present; `:undrain` returns it to schedulable with no drain phase. |
| [local.interrupted_suspend_keeps_only_copy_until_discard](local_interrupted_suspend_keeps_only_copy_until_discard.py) | routine | `kind_ecw`; kind; kubernetes | A suspend that fails with its runtime gone and no capture kept leaves the sandbox quarantined as its only copy: the public delete doesn't complete, the node's ordinary delete fails, and the operator's discard refuses while the suspend attempt could still finish. Once that window has passed, the discard erases the sandbox and its filesystem, and the public delete then completes and the sandbox's pod is released. |
| [local.leader_election_brownout_keeps_one_leader](local_leader_election_brownout_keeps_one_leader.py) | routine | `kind_admin`; kind; kubernetes | Two Ready replicas share one Lease holder and one reconciler runner. Cut off from the apiserver, readiness clears and the holder stops before its lease can be taken over; once reconnected, two Ready replicas share one fresh holder and runner. Throughout, a contender whose Lease calls are refused stays NotReady and unrestarted past its liveness budget, and one that cannot reach the apiserver spends that budget and exits non-zero. |
| [local.no_free_slot_refuses_cold_create](local_no_free_slot_refuses_cold_create.py) | routine | `kind_ecw`; kind; kubernetes | With worker network-slot capacity scoped to a small count per node and the activation limiter's slot check switched on first, sequential cold creates fill the remaining slots until the API refuses with a 503 `Server capacity limit:` naming a free sandbox slot and carrying Retry-After, and admitted sandboxes (fixture included) match the sum of sandbox-pool nodes' `status.allocatable` network slots; a reconnect to an already-running sandbox is still served; and freeing one slot lets a create succeed again before both cluster knobs are restored. |
| [local.node_ctl_fence_refuses_and_reconciles](local_node_ctl_fence_refuses_and_reconciles.py) | routine | `kind_ecw`; kind; kubernetes | With the token kubelet projects into the node-ctl pod on this sandbox's worker, every forbidden Node write (taints, cordon, finalizers, the node-group boundary and cred-authority labels, foreign or inflated status resources, forged conditions, a template-write pull request, and anything on another node) is refused by the fence policy and by the named rule, leaves no residue, and the boundary label is otherwise writable; every write shape node-ctl has (SSD placement apply, host-CPU label apply, template-presence apply and prune, template-write clear, capacity apply) is admitted; and the running node-ctl writes back a stripped owned label and the stripped slot capacity through its own credential. |
| [local.node_group_boundary_schedules](local_node_group_boundary_schedules.py) | routine | `kind_ecw`; kind; kubernetes | With the placement gate off a created pod carries no node-group affinity term; a dedicated group is refused with a 409 while the cluster has no generic group; and with the labeler and the gate on, a generic create renders `node-restriction.kubernetes.io/space-node-group In (<default group>)` and its pod reaches Running on a labelled worker. |
| [local.node_selector_extras_render_and_enforce](local_node_selector_extras_render_and_enforce.py) | main | `kind_ecw`; kind; kubernetes | With two satisfiable extras configured on both renderers, an ECW create's pod (rendered by space-api) and a native create's pod (rendered by the cluster controller's dispatcher) each carry `spec.nodeSelector` equal to the discriminator plus every extra and run on a node carrying all of them; with one extra no node carries added, the fleet still satisfies the old set, nothing satisfies the new one, and an ECW create is refused before any pod with `429 Server capacity limit: no eligible sandbox activation nodes …` and a Retry-After; the renderers' env is restored to what was captured. |
| [local.orphaned_restore_pod_gc_reaps_only_superseded](local_orphaned_restore_pod_gc_reaps_only_superseded.py) | routine | `kind_ecw`; kind; kubernetes | With the GC live, a restore pod below the started row's active generation and one whose sandbox row does not exist are each deleted, no sooner than the threshold; a restore done while the GC is live leaves one execution whose pod keeps its uid well past the threshold plus several ticks; and a suspended sandbox with an orphan on its own row at its retired generation, which suspend retired as `suspended` rather than `replaced`, keeps that orphan through the survival window (the backup fence would admit its teardown capture) while a row-less control planted beside it is reaped, then restores with its state and the orphan is gone once the restore supersedes it. |
| [local.pending_backup_census_reports_blocked_upload](local_pending_backup_census_reports_blocked_upload.py) | routine | `kind_ecw`; kind; kubernetes | With object storage unreachable, a suspend operation leaves a snapshot in `creating`, the node's next Full census reports the pinned artifact as pending, and space-internal-api mirrors it into the cluster queue; once storage returns, the snapshot becomes `ready` with a storage location, the queue row disappears, and the suspend operation ends `succeeded`. |
| [local.private_template_build_isolation](local_private_template_build_isolation.py) | routine | `kind_admin`; kind; kubernetes | An owner can derive a working template from its private base, while a different authenticated organization cannot build from the same tag or digest, including after the owner's successful materialization. |
| [local.rate_limiters_enforce_and_fail_open](local_rate_limiters_enforce_and_fail_open.py) | routine | `kind_ecw`; kind; kubernetes | With both limiters' caps shrunk and per-client concurrency in enforce mode, sequential creates drain the cluster activation bucket to a 429 `Server rate limit:` with Retry-After; twenty concurrent creates and twenty concurrent connects for one client id each draw at least one 429 `Per-client rate limit:` with Retry-After; in each barrier-released round where a twenty-deep storm for another client draws per-client 429s — one waking suspended sandboxes and one reconnecting to a running one — a separately-bucketed control client's reconnects all succeed; a create made under a client id records that client on the sandbox row and on each of its pods; with a live cap set, one client's wakes of suspended sandboxes plateau at the cap under a 429 `Client live sandbox cap reached:`, and then that client's reconnect and another client's reconnects to a running sandbox, which aren't activations, all succeed; once Redis is unreachable every create admits a sandbox; once it is back the per-client limiter rejects again; and with the standing configuration restored, when it tightens nothing, a burst the shrunk caps would refuse draws no limiter rejection. |
| [local.reconcile_queue_gc_prunes_only_gone_nodes](local_reconcile_queue_gc_prunes_only_gone_nodes.py) | routine | `kind_admin`; kind; kubernetes | With the reconcile-queue GC on against the fake EC2 endpoint, one tick deletes a 25 h-old dead letter and the 25 h-old live ref of a non-ready node whose Kubernetes Node does not exist, and keeps a 1 h-old dead letter and the 25 h-old live ref of a non-ready node named after a Ready Node. |
| [local.restarted_btrfs_send_is_rejected_and_retried](local_restarted_btrfs_send_is_rejected_and_retried.py) | routine | `kind_ecw`; kind; kubernetes | A deferred capture whose kernel send is interrupted while blocked on its pipe fails that upload attempt at node-manager's framing check instead of storing a concatenated stream; the retried upload restores onto another worker with the sandbox's data intact. |
| [local.retention_claim_finish_forward](local_retention_claim_finish_forward.py) | main | `kind_ecw`; kind; kubernetes | A crash-orphaned `deleting` claim finishes forward without violating retention or sandbox usability. |
| [local.retention_restore_source_guard](local_retention_restore_source_guard.py) | routine | `kind_ecw`; kind; kubernetes | The live retention sweeper prunes unpinned history past keep-N while the current-artifact pin — the exact snapshot restore loads for a marked suspended sandbox — survives the sweep and the sandbox still resumes. |
| [local.sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown](local_sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown.py) | routine | `kind_ecw`; kind; kubernetes | With the suspend's backup held, restarting the worker's sandbox-manager leaves the sandbox's row `suspending`, rather than marking it suspended behind its teardown; once uploads return, the suspend operation succeeds and the sandbox wakes with the state it had. |
| [local.sandbox_network_isolation_holds](local_sandbox_network_isolation_holds.py) | routine | `kind_ecw`; kind; kubernetes | From a running sandbox pod's network namespace, entered on its node outside the VM, Postgres, the Kubernetes API Service, internal-api, the object store and another sandbox's pod IP on 9800 are all refused while the cluster DNS, and the internet when this host has it, still connect; every refused target answers from the control-plane namespace; a pod in that namespace cannot open the sandbox pod's 9800; a pod in the sandbox namespace that no allow rule selects reaches nothing; and the guest still reaches the internet when this host does. |
| [local.selected_environment_survives_suspend_and_cross_node_restore](local_selected_environment_survives_suspend_and_cross_node_restore.py) | manual | `kind_ecw`; kind; kubernetes | The sandbox runs the selected environment after create, after a suspend and restore, and after a restore onto another node from object storage, with its filesystem intact each time. The bench's node-manager allowlist is back to what it was once the sandbox and seed are gone.<br>Manual: Rolls every node-manager to admit Firecracker 1.17.0 / Linux 6.1.155, installs that runtime on every node from its upstream release, and builds a seed: run it on a bench no other scenario shares while it runs. |
| [local.storage_exchange_scoping](local_storage_exchange_scoping.py) | routine | `kind_ecw`; kind; kubernetes | A read-scoped session that a sandbox worker's credential-manager agent vends on its volume socket, as volume-manager asks, for one object of a sandbox's suspend snapshot, reads that object; the object store refuses it for another sandbox's snapshot object and listing, for the templates bucket, and for a small write to its own object, which stays unchanged; and the authority refuses to vend that sandbox's subject for the other sandbox's object at all. Afterwards the session, and a fresh one, still read the object, so no refusal was a session that stopped working. A builder's agent likewise vends a read session for one published file of the sandboxes' template, as the builder volume-manager asks for a template pull; it reads that file, and the store refuses it another file of the same release, the sandbox's snapshot object and, when another template is published, that template's file; the authority refuses to vend the template's subject for another template's file at all. The same closing controls follow. |
| [local.suspend_snapshot_rows_are_per_attempt](local_suspend_snapshot_rows_are_per_attempt.py) | routine | `kind_ecw`; kind; kubernetes | Two suspends of one sandbox leave two ready snapshot rows with distinct ids and distinct id-named storage prefixes; the first row's facts are unchanged by the second publish; and the restore after the second suspend brings back the second suspend's bytes. |
| [local.telemetry_reaches_local_backends](local_telemetry_reaches_local_backends.py) | routine | `kind_ecw`; kind; kubernetes | Once a sandbox has run a command, Tempo returns containerd-shim-space-v2's create in the space-api trace that created it, parented by the API's pod materialization span, Prometheus holds create latency from that shim and from space-api since the scenario began, Loki holds a SPACE pod log line naming the sandbox, and fresh guest-memory observations arrive for its template and node. |
| [local.template_campaign_canary_rolls_and_pauses](local_template_campaign_canary_rolls_and_pauses.py) | manual | `kind_admin`; kind; kubernetes | Started through the admin API, a templates campaign from E0 to E1 moves nothing until every worker reads twins and E1 is the default; then it moves one template and waits in canary; resumed, it rolls; paused mid-wave, it moves nothing more over several loop ticks; resumed again, it completes with no failed item. A create then boots its template's twin under E1, an ECW create claims a slot of the twin's copy of the warm pool the template's original had and runs E1 too, and once the default is back on E0, a create boots the original under E0.<br>Manual: Turns on the environment placement gate, the campaign loop and plaintext object-store endpoints for the template driver, rolls every node-manager to admit Firecracker 1.17.0, and moves the cluster default; a completed campaign leaves each template's E1 twin on the bench, so a later run is inconclusive until they're gone, and a bench with no default keeps E0 as its default. |
| [local.template_startup_recovery_decision_table](local_template_startup_recovery_decision_table.py) | routine | `kind_ecw`; kind; kubernetes | After a worker's template-manager restarts, startup recovery keeps a build whose row and bytes agree, prunes a row whose bytes are gone, adopts row-less bytes only when the artifact hashes to the directory name, removes row-less bytes whose hash disagrees, and removes bytes whose size disagrees with the manifest even with a row — leaving disk and catalog consistent and the sandbox on the other worker running. |
| [local.unanswered_template_deleted_after_wait](local_unanswered_template_deleted_after_wait.py) | routine | `kind_ecw`; kind; kubernetes | With node-ctl's unanswered-template wait shortened, a worker keeps an org template through a full sync past the wait while a sandbox of it runs there, keeps it through two full syncs after the sandbox is deleted but before the wait has passed, and deletes it (unlisted, its bytes and then its Node template-build metadata gone) in the first full sync after the wait; a sandbox of the same template placed on that worker afterwards pulls it again and boots. |
| [local.warm_pool_policy_follows_new_release](local_warm_pool_policy_follows_new_release.py) | routine | `kind_admin`; kind; kubernetes | With a rolling warm-pool policy retaining one release of a run-owned template, an ECW create of the template claims a ready slot of the policy-owned pool for the release its first build tagged `latest`. Once a second build with different content moves `latest`, the policy-owned pool of the new release gets a ready slot and the old release's is disabled within the reconciler's pacing, and a create of the template claims a ready slot of the new release's pool and runs the new release's content hash and content. |
| [native.aws_secret_network_credential_injection](native_aws_secret_network_credential_injection.py) | manual | `command_sandbox`; deployed, kind; direct, kubernetes | A flat AWS reference injects its secret only after a policy binds it.<br>Manual: Requires an explicitly provisioned AWS secret and a selected echo endpoint. |
| [native.encrypted_double_restore_retires_prior_pods](native_encrypted_double_restore_retires_prior_pods.py) | routine | `command_sandbox`; deployed, kind; kubernetes | A BYOK sandbox survives suspend, restore, suspend and restore with its file intact, and each restore leaves only the serving execution's pod. |
| [native.forced_drain_re_drains_re_placed_stopped_sandbox](native_forced_drain_re_drains_re_placed_stopped_sandbox.py) | routine | `command_sandbox`; deployed, kind; kubernetes | With the three stopped-evacuation flags on, a forced `:drain` re-places a `stopped` sandbox on the other worker. Draining that worker forced, before any start, releases the sandbox from it without a new disk backup, and its next re-placement pod waits `Unschedulable`, excluding both workers by name in every required node-affinity term, while the row stays a claimed re-placement. |
| [native.forced_drain_re_drains_restarted_re_placed_stopped_sandbox](native_forced_drain_re_drains_restarted_re_placed_stopped_sandbox.py) | routine | `command_sandbox`; deployed, kind; kubernetes | With the three stopped-evacuation flags on, a forced `:drain` re-places a `stopped` sandbox on the other worker. A user start runs it there in a pod pre-bound to that worker, and it's stopped again. Draining that worker forced releases the sandbox from it with a new disk backup published, and its next re-placement pod waits `Unschedulable`, excluding both workers in every required node-affinity term. |
| [native.m1_generated_sdk_conformance](native_m1_generated_sdk_conformance.py) | routine | `kind-native-hardening`; kind; kubernetes | The pinned OpenAPI Generator turns the served /openapi-m1.json into a Python SDK that runs all 14 M1 operations against the API and data plane: create, read and list, command over SSE with a Last-Event-ID reconnect, process control, pause, suspend and wake, delete, and the documented error envelopes, including 400 for a malformed sandbox id. |
| [native.network_policy_live_update](native_network_policy_live_update.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | PATCHing a running sandbox's network policy changes its egress in place: a destination is allowed, revoked, and allowed again, each change is enforced and reflected by the read API, and the guest never reboots. |
| [native.resident_pause](native_resident_pause.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | A resident pause and wake preserve filesystem state without rebooting. |
| [native.stopped_restart_preserves_rootfs](native_stopped_restart_preserves_rootfs.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | Stopping a sandbox parks its rootfs on the node and reports `stopped`; starting it again runs from that rootfs, so a file written before the stop reads back unchanged after the restart. |
| [native.v2_drain_evacuates_every_resident](native_v2_drain_evacuates_every_resident.py) | routine | `command_sandbox`; kind; kubernetes | A v2 `:drain`, due now, of a worker holding two started sandboxes (one under a policy that wakes it on access) and a paused one seals it; the node suspends them itself, reports each evacuated with its snapshot, and reaches `drained`. Each resident is left suspended and unplaced, with no VM on either worker, and stays so until a user call: a call to the started one with no implicit wake is refused until a wake lands it on the other worker; a call to the one woken on access runs there; a wake lands the paused one there. Each sandbox's file is intact, and `:undrain` cancels the drain. |
| [native.v2_drain_holds_running_until_its_deadline](native_v2_drain_holds_running_until_its_deadline.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 `:drain` with an hour's hold of a worker holding two running sandboxes, one with a short idle policy, and a paused one seals the worker, and the admin view shows that deadline. The running sandboxes are held there and answer calls; the paused one is made durable at once, and a wake lands it on the other worker. The idle one suspends on its own on the sealed worker, and the node reports it evacuated. A repeat `:drain` with a longer hold is a 409 and one naming a `mode` a 400, each leaving the deadline as it was; one with a shorter hold, counted from the drain's start, moves the deadline earlier. The held sandbox keeps running and answering until that deadline; past it the node suspends it, the drain reaches `drained` with its `drain_mode` unchanged, and a wake lands each on the other worker with its file.<br>Manual: timing-sensitive on Kind: an idle suspend must be reported before the bench's short eviction floor drops its local rows |
| [native.v2_drain_keeps_a_live_node_with_a_stale_instance](native_v2_drain_keeps_a_live_node_with_a_stale_instance.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 drain of a worker whose node row records a stale instance, its Node naming none, seals the worker. Its machine then stops answering with its Node kept, and EC2 answers `terminated` for the recorded instance. The drain does not let the node go: it keeps its phase, and `drain_last_error` says the Node is live and names no EC2 instance. Once the machine answers again, `:undrain` cancels the drain.<br>Manual: stops a Kind worker while its Node stays, too invasive for the per-merge set |
| [native.v2_drain_meets_user_calls](native_v2_drain_meets_user_calls.py) | manual | `command_sandbox`; deployed, kind; kubernetes | Of three residents of a worker v2-drained with a hold, one deleted as the drain begins is gone and doesn't hold the drain. Before the deadline the running one answers calls; a Rollback of it is refused on the sealed worker, and it keeps running there with its data intact. A wake of the one its user paused is told to retry with a 503, never a 409, and lands it on the other worker once the drain made it durable. The deadline is then brought in with a shorter hold; the node holds the running one until it, and from just before it each call that meets the running one passes until the node suspends it, and at least one meets it past the deadline, or the run ends inconclusive. Once the node suspends it, with its upload cut, a wake and a command of it are recorded, and no second execution of it runs off the worker; once the upload goes through, a wake lands it on the other worker, its data intact. The node reaches `drained`.<br>Manual: timing-sensitive on Kind: its calls past the deadline race the controller's first evacuating tick |
| [native.v2_drain_moves_ten_residents](native_v2_drain_moves_ten_residents.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 `:drain`, due now, of a worker holding seven started and three paused sandboxes, each VM holding random memory, reports each evacuated with its snapshot and reaches `drained`. The ten are left suspended and unplaced, with no VM on either worker. A wake then lands each on the other worker with its memory intact, and `:undrain` cancels the drain.<br>Manual: ten resident VMs on one worker, too heavy for the per-merge set |
| [native.v2_drain_outlived_by_a_dead_worker](native_v2_drain_outlived_by_a_dead_worker.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A worker holding a durably suspended resident and more started ones than the drain's window dies while the window's suspends wait on their blocked uploads. While its instance is `shutting-down` the drain keeps the node and no row is written off, even after a failed wake; once EC2 says `terminated` the drain lets the node go with its trace, writing no row. Then the drain's columns stay cleared and no resident still published `started` on the dead node names it, active or target; where each ends up is the crashed-node handling's, recorded; the suspended resident stays suspended and wakes on the other worker; and nothing tombstones the node, while whether `orphaned_nodes` deletes its row, and which `creating` snapshots still name it, is recorded. The parked-row branch is not exercised on Kind.<br>Manual: stops a Kind worker mid-drain and replaces it, too invasive for the per-merge set |
| [native.v2_drain_paces_a_full_worker](native_v2_drain_paces_a_full_worker.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 `:drain`, due now, of a worker holding thirty residents with memory in use, ten paused and eight idle-suspending, reports nothing blocked, is never marked stalled, and reaches `drained`. A wake of a paused resident still waiting for a slot in the window gets the drain's retryable 503, never a 409, and lands on the other worker once the drain has made it durable. Every resident is left suspended and unplaced, and a wake lands each on the other worker with its memory intact.<br>Manual: a thirty-sandbox drain measured end to end, too slow for the per-merge set |
| [native.v2_drain_preview_changes_nothing](native_v2_drain_preview_changes_nothing.py) | manual | `command_sandbox`; deployed, kind; kubernetes | With no v2 drain of it recorded, a worker holds a started and a stopped sandbox and a sandbox it quarantined from a cache directory no sandbox owned. The admin preview, of an evacuating drain, reports the quarantined one pending and no sandbox blocked, the started sandbox pending running and waiting on its move, never a hold, and the stopped one, which a drain never moves, pending, not running and waiting on nothing the drain drives (`external`): a drain could not finish yet. Right after it, the worker's sandbox-manager still holds both rows in their lifecycles. Afterwards nothing has moved: the started sandbox still runs, neither gained a snapshot, the node row has no drain phase, and a create and a start still land on the worker. Once the quarantined sandbox is removed, the preview no longer names it, and both sandboxes are still in their lifecycles, on the worker too, with no new snapshot.<br>Manual: runs by name until the drain v2 scenarios have a record of consecutive clean runs on Kind |
| [native.v2_drain_refused_at_the_cap](native_v2_drain_refused_at_the_cap.py) | manual | `command_sandbox`; deployed, kind; kubernetes | With at most one v2 drain evacuating, a `:drain` of a worker while another evacuates is a 409 with the cap's code and a `Retry-After`; the refused worker has no drain phase and stays schedulable, and its resident runs commands, pauses and wakes. Once the first worker is undrained and its drain cancelled, the same `:drain` of the second is accepted: it seals the worker and reaches `drained`, its resident left suspended and unplaced until a wake lands it on the first worker with its file.<br>Manual: kept manual until it has several consecutive clean runs on Kind, after which it can return to the per-merge set |
| [native.v2_drain_refuses_stopped_sandboxes](native_v2_drain_refuses_stopped_sandboxes.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 `:drain` of a worker holding a stopped sandbox is refused with the stopped refusal's 409 naming it, and writes nothing; once it is deleted the same `:drain` is accepted and holds the worker's four running sandboxes. While the drain lasts, a `:stop`, `:kill`, `:force-stop` and `:force-kill`, each of a different one of them, is each refused with the drain's retryable 503, and each keeps running there. Once undrained, a `:stop` goes through.<br>Manual: waits out four refused stops' retry budgets, too slow for the per-merge set |
| [native.v2_drain_replaced_while_shutting_down](native_v2_drain_replaced_while_shutting_down.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2-drained worker is `:terminate`d, and while its fenced instance is still `shutting-down` the worker comes back under the same name with a new instance. The replacement's registration takes the row and clears the drain, fence included; the row stays, holding the new instance, with no tombstone, after the old instance is terminated.<br>Manual: stops a Kind worker and replaces it, too invasive for the per-merge set |
| [native.v2_drain_survives_controller_restarts](native_v2_drain_survives_controller_restarts.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 drain, with a fifteen-minute hold, of two started and a paused resident outlives a controller restart after the seal, with uploads blocked and nothing evacuated: the restarted controller stores the node's report under the same generation and the deadline is unchanged. Once uploads are let through the paused resident is made durable, and a repeat `:drain` brings the stored deadline in. With the controller scaled to zero across that deadline, the started residents keep running on the worker past it: the node keeps no deadline of its own. Once the controller is back it evacuates on the shortened deadline, well before the first hold's, so the node suspends them and reaches `drained`; every resident is suspended and unplaced, a wake lands each on the other worker with its memory intact, and `:undrain` cancels the drain.<br>Manual: restarts and scales the cluster-controller, too invasive for the per-merge set |
| [native.v2_drain_survives_sandbox_manager_restarts](native_v2_drain_survives_sandbox_manager_restarts.py) | manual | `command_sandbox`; deployed, kind; kubernetes | With uploads blocked on the drained worker, a v2 drain with a hold of two started and a paused resident seals it. Its sandbox-manager restarts before the deadline: the node reports under the same generation again, the stored deadline is unchanged, and the started residents keep running until it, once brought in by a repeat `:drain`. Past it the node suspends them, and it restarts again while every upload waits: it reports under the generation again, and no started resident runs again. Once uploads are let through the node reports each resident evacuated and reaches `drained`; every resident is suspended and unplaced, and a wake lands each on the other worker with its memory intact.<br>Manual: restarts a worker's sandbox-manager under a drain, too invasive for the per-merge set |
| [native.v2_drain_terminates_its_worker](native_v2_drain_terminates_its_worker.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A worker under a fake AWS instance, v2-drained of a started resident the drain leaves suspended and unplaced until a wake lands it on the other worker, is `:terminate`d: the controller fences its instance and records the dispatch of its termination through the fake EC2; once the worker is stopped and EC2 says the instance is terminated, the drain deletes the node's row with no tombstone. The worker comes back under the same name with a new instance. The resident's file is intact.<br>Manual: stops a Kind worker and replaces it, too invasive for the per-merge set |
| [native.v2_drain_waits_for_a_lingering_node](native_v2_drain_waits_for_a_lingering_node.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2-drained worker is `:terminate`d and stopped with its Node kept. With its fenced instance terminated, the node's row stays and its `drain_last_error` says it waits for the Node; once the Node is deleted with a uid precondition, the drain deletes the row with no tombstone. The worker comes back under the same name with a new instance.<br>Manual: stops a Kind worker and replaces it, too invasive for the per-merge set |
| [native.v2_drain_waits_for_an_unreachable_node](native_v2_drain_waits_for_an_unreachable_node.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 drain, due now, of a started and a paused resident seals the worker; then its node-api stops answering. The loop records the failed `DrainNode` call and leaves the node `draining`, while the node keeps evacuating: each resident is made durable there, `suspended` with a stored snapshot and no VM on the other worker. Once node-api answers, the node reports both evacuated and reaches `drained`; both are suspended and unplaced, and a wake lands each on the other worker with its memory intact.<br>Manual: cuts a worker's node-api for minutes, too slow for the per-merge set |
| [native.v2_drain_waits_out_blocked_uploads](native_v2_drain_waits_out_blocked_uploads.py) | manual | `command_sandbox`; deployed, kind; kubernetes | With the drained worker's path to the object store cut, a v2 drain, due now, of a started resident reports it pending, with the time it entered its state. The stall bound past its deadline with nothing evacuated, its `drain_last_error` says it is stalled; the resident is still pending, the report names no sandbox blocked, and the drain stays `draining` with nothing evacuated. Once the path is back, the node reports it evacuated and reaches `drained`; the resident is left suspended and unplaced until a wake lands it on the other worker with its memory intact.<br>Manual: holds a drain stalled behind a cut object store, too slow for the per-merge set |
| [native.v2_refuses_v1_drains](native_v2_refuses_v1_drains.py) | manual | `command_sandbox`; deployed, kind; kubernetes | With drain v2 on, a `:drain` of the fixture's worker with an empty body, with `mode: forced`, and naming `engine: v1` is each a 409 with the refusal's stable code; after each the worker has no drain phase, no v2 drain status and no cordon, and its resident still runs commands.<br>Manual: runs by name until the drain v2 scenarios have a record of consecutive clean runs on Kind |
| [native.v2_undrain_mid_suspend](native_v2_undrain_mid_suspend.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 `:drain`, due now, of a worker holding twelve started residents, each VM holding memory, is `:undrain`ed while the node reports them moving, some of their suspends running and the rest still waiting. The drain is cancelled on the node. No resident still started on the worker once the cancel cleared was suspended after it: each still runs there, with no snapshot. Each resident whose suspend ran stays suspended, handed back to nothing, until a wake starts it. Every resident's memory is intact.<br>Manual: twelve resident VMs filled with memory, too heavy for the per-merge set |
| [native.v2_undrain_unseals_the_worker](native_v2_undrain_unseals_the_worker.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 `:drain` with a hold seals the worker, and the controller stores the node's report under the drain's generation; its node-api then stops answering and the worker is `:undrain`ed. While the node can't answer, its Node stays unschedulable and the cancel stays pending. Once node-api answers, the controller cancels the drain on the node, clears the v2 columns and uncordons the worker; a preview of it then succeeds, which a sealed node refuses, and it admits an activation again: with the other worker cordoned, the resident's wake lands on it.<br>Manual: kept manual until it has several consecutive clean runs on Kind, after which it can return to the per-merge set |
| [native.v2_undrain_with_the_controller_down](native_v2_undrain_with_the_controller_down.py) | manual | `command_sandbox`; deployed, kind; kubernetes | A v2 `:drain`, due now, seals a worker holding a started and a paused resident; with the controller down the node makes both durable, and the worker is `:undrain`ed. Once the controller is back it cancels the drain within a few ticks and uncordons the worker; both residents stay suspended, and a wake starts each with its memory intact.<br>Manual: scales the cluster-controller to zero, too invasive for the per-merge set |
| [native_concurrency.command_idempotency](native_concurrency_command_idempotency.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Concurrent identical command requests share identity and execute their side effect once. |
| [native_concurrency.create_idempotency](native_concurrency_create_idempotency.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Concurrent identical create requests converge on one ready sandbox and operation. |
| [native_concurrency.independent_commands](native_concurrency_independent_commands.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Every submitted command completes with its own complete stdout and stderr. |
| [native_concurrency.lifecycle_contention](native_concurrency_lifecycle_contention.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Duplicate lifecycle intents share operations and conflicting intents preserve liveness. |
| [native_concurrency.slow_reader](native_concurrency_slow_reader.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | A delayed stream reader receives complete output with the expected checksum. |
| [native_contract.command_attach_after_pause](native_contract_command_attach_after_pause.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | A command created while the sandbox runs, then attached after it paused without a command or access wake source, answers the same non-retryable conflict as creating it would. |
| [native_contract.command_on_sandbox_not_running](native_contract_command_on_sandbox_not_running.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Without a command or access wake source, a command on a suspended or deleting sandbox is a non-retryable conflict; a deleted sandbox is not found. |
| [native_contract.command_replay_credentials](native_contract_command_replay_credentials.py) | manual | `native-hardening`; deployed, kind; direct, kubernetes | A command retry preserves its session and reports any credential invalidation.<br>Manual: Overlapping credential validity is not an agreed API guarantee; decision: docs/native-api-hardening.md#credential-replay-contract-deferred |
| [native_contract.command_validation](native_contract_command_validation.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Zero vCPUs, malformed commands and a streamed pty fail admission; streamed stdin reads EOF. |
| [native_contract.command_wakes_idle_suspended_sandbox](native_contract_command_wakes_idle_suspended_sandbox.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | With `wake.on=["access"]`, a command sent after an idle suspend runs without a wake or any other call first. |
| [native_contract.completed_delete_replay](native_contract_completed_delete_replay.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | A completed DELETE replay returns its original retained operation. |
| [native_contract.file_idempotency_across_origins](native_contract_file_idempotency_across_origins.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | A write first sent to the API is replayed by the data plane under its key, and a different write under it is refused there with 409 `idempotency_conflict`; a rename first sent to the data plane is replayed by the API the same way. Neither refused request changes a file. Inconclusive when the two origins are one. |
| [native_contract.file_idempotency_conflict](native_contract_file_idempotency_conflict.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | A file write's idempotency key replays that write and refuses any other. |
| [native_contract.file_op_wakes_idle_suspended_sandbox](native_contract_file_op_wakes_idle_suspended_sandbox.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | With `wake.on=["access"]`, a `files:write` sent to the data plane after an idle suspend wakes the sandbox and lands, without a wake or any other call first, and the state written before the suspend is still there. |
| [native_contract.file_operations_match_on_the_data_plane](native_contract_file_operations_match_on_the_data_plane.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | The 11 `files:*` operations, and the refusals of their field checks, answer the data plane's origin with the statuses and bodies the API's origin answers, on the same sandbox. Inconclusive when the two origins are one. |
| [native_contract.file_upload_admission_429](native_contract_file_upload_admission_429.py) | main | `kind-native-hardening`; kind; kubernetes | With the Kind data plane's standing Native upload admission (by default 8 writes per sandbox on each replica and a 2 s wait for a slot), more HTTP/1.1 writes held open to one sandbox at once than its replicas at their autoscaler's maximum can admit leave at least one refused before its body is read, with 429 `rate_limited`, `retryable` and a `Retry-After` of 1 to 5 seconds; and every admitted write lands once its body ends. The scenario changes no data-plane setting. |
| [native_contract.file_write_size_boundary_on_the_data_plane](native_contract_file_write_size_boundary_on_the_data_plane.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | A `files:write` of exactly 16 MiB sent to the data plane lands whole; one byte more is refused with 400 `invalid_request` and writes nothing, as the API answers both. |
| [native_contract.permanent_command_error](native_contract_permanent_command_error.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Missing executables and working directories yield permanent command failures. |
| [native_contract.process_session_wakes_idle_suspended_sandbox](native_contract_process_session_wakes_idle_suspended_sandbox.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | With `wake.on=["access"]`, a process session created after an idle suspend and attached over WebSocket runs without a wake first. |
| [native_contract.session_reattach_wakes_idle_suspended_sandbox](native_contract_session_reattach_wakes_idle_suspended_sandbox.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | With `wake.on=["access"]`, a command attached over SSE and a process session attached over WebSocket, both created while the sandbox ran, each wake it after an idle suspend and deliver their output. |
| [native_contract.session_reconnect_after_idle_suspend](native_contract_session_reconnect_after_idle_suspend.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Under `wake.on=["access"]` the reattach wakes the sandbox and reads the running command's later output; under `wake.on=["command"]` it is the final `sandbox_not_running` conflict, and the sandbox stays suspended. |
| [native_contract.tenant_isolation](native_contract_tenant_isolation.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Another organization cannot read or list an owned sandbox or its operation. |
| [native_contract.token_scope](native_contract_token_scope.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | An attachment credential cannot access management endpoints or another session. |
| [native_recovery.attached_stream_drains_through_data_plane_restart](native_recovery_attached_stream_drains_through_data_plane_restart.py) | routine | `kind-native-hardening`; kind; kubernetes | A stream attached before SIGTERM keeps delivering, and the old replica exits when it ends. |
| [native_recovery.command_survives_data_plane_restart](native_recovery_command_survives_data_plane_restart.py) | routine | `kind-native-hardening`; kind; kubernetes | A running command reconnects after data-plane replacement without reexecution. |
| [native_recovery.inline_load_survives_leader_failover](native_recovery_inline_load_survives_leader_failover.py) | routine | `kind-native-hardening`; kind; kubernetes | Creates and wakes the API runs inline while no controller runs, with a suspend queued behind them for the dispatcher, then a new controller leader elected, which runs the suspend while their claims are live: each create and wake is claimed once and ends once, and every operation succeeds as requested. On a separate sandbox, a wake admitted behind a queued suspend stays queued and later leaves its guest running. |
| [native_recovery.inline_wake_survives_api_pod_kill](native_recovery_inline_wake_survives_api_pod_kill.py) | routine | `kind-native-hardening`; kind; kubernetes | A wake the API is running when every API pod is killed is not lost: once its claim goes stale the dispatcher takes the same operation over, ends it once, and the sandbox runs commands. |
| [native_recovery.slow_operation_does_not_block_later_work](native_recovery_slow_operation_does_not_block_later_work.py) | routine | `kind-native-hardening`; kind; kubernetes | With an idle dispatcher slot, a later pause finishes while an earlier sandbox suspend is held running by a reversible object-storage outage. |
| [network_policy.v1_cidr_allow](network_policy_v1_cidr_allow.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A matching V1 CIDR allow overrides default deny. |
| [network_policy.v1_cidr_deny](network_policy_v1_cidr_deny.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A matching V1 CIDR deny overrides default allow. |
| [network_policy.v1_deny_precedes_allow](network_policy_v1_deny_precedes_allow.py) | routine | `ecw`; deployed, kind; direct, kubernetes | V1 blocks an explicitly allowed IP when a broader denied CIDR contains it. |
| [network_policy.v1_domain_binding](network_policy_v1_domain_binding.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A domain allow rejects mismatched SNI, missing SNI, and mismatched Host. |
| [network_policy.v1_exact_ip_allow](network_policy_v1_exact_ip_allow.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A V1 exact-IP allow overrides default deny without using CIDR syntax. |
| [network_policy.v1_exact_ip_deny](network_policy_v1_exact_ip_deny.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A V1 exact-IP deny overrides default allow without using CIDR syntax. |
| [network_policy.v1_fastly_shared_edge_bypass](network_policy_v1_fastly_shared_edge_bypass.py) | routine | `ecw`; deployed, kind; direct, kubernetes | An allowed Fastly tenant cannot authorize another host on its edge IP. |
| [network_policy.v1_rules_survive_restore](network_policy_v1_rules_survive_restore.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A V1 allow/deny policy enforces default deny and retains the same behavior after the sandbox is suspended and restored. |
| [network_policy.v1_tcp_dns_transport_respects_ip_allow](network_policy_v1_tcp_dns_transport_respects_ip_allow.py) | routine | `ecw`; deployed, kind; direct, kubernetes | TCP DNS enforces reserved resolver IPs and queried domains independently. |
| [network_policy.v1_udp_deny_precedes_allow](network_policy_v1_udp_deny_precedes_allow.py) | routine | `ecw`; deployed, kind; direct, kubernetes | An overlapping V1 CIDR deny takes precedence over an exact UDP allow. |
| [network_policy.v1_udp_respects_cidr_allow](network_policy_v1_udp_respects_cidr_allow.py) | routine | `ecw`; deployed, kind; direct, kubernetes | UDP DNS enforces the resolver CIDR and queried domain independently. |
| [network_policy.v1_udp_respects_cidr_deny](network_policy_v1_udp_respects_cidr_deny.py) | routine | `ecw`; deployed, kind; direct, kubernetes | UDP reaches an outside resolver while a denied CIDR stays blocked. |
| [network_policy.v1_udp_respects_ip_allow](network_policy_v1_udp_respects_ip_allow.py) | routine | `ecw`; deployed, kind; direct, kubernetes | UDP DNS enforces the resolver IP and queried domain independently. |
| [network_policy.v1_udp_respects_ip_deny](network_policy_v1_udp_respects_ip_deny.py) | routine | `ecw`; deployed, kind; direct, kubernetes | UDP reaches an unlisted resolver while an exact denied IP stays blocked. |
| [network_policy.v1_wildcard_domain_allow](network_policy_v1_wildcard_domain_allow.py) | routine | `ecw`; deployed, kind; direct, kubernetes | A V1 wildcard allows subdomains but not its apex or unrelated names. |
| [node.guest_auto_update](node_guest_auto_update.py) | manual | `direct-node-recovery`; deployed; direct | Grace yields to pause; automatic trial delays one customer command without replay.<br>Manual: requires a dedicated node with automation enabled and a newer qualified platform target |
| [node.guest_auto_update_disabled](node_guest_auto_update_disabled.py) | manual | `direct-node-recovery`; deployed; direct | The node kill switch prevents automatic admission while manual maintenance works.<br>Manual: requires a dedicated node started with SPACE_GUEST_AUTO_UPDATE=0 and a qualified newer target |
| [node.guest_update](node_guest_update.py) | manual | `direct-node-recovery`; deployed; direct | Idle commit, durable receipt, idempotent no-op, force and memory restore.<br>Manual: requires a dedicated idle node, qualified selector template and authorized newer guest bundle |
| [node.guest_update_failure](node_guest_update_failure.py) | manual | `direct-node-recovery`; deployed; direct | A failed trial cannot starve policy changes or poison later checkpoints.<br>Manual: requires a dedicated idle node and an authorized candidate that hangs before READY until systemd terminates it |
| [node.runtime_base_memory](node_runtime_base_memory.py) | manual | `direct-node-recovery`; deployed; direct | Base snapshots and pool members restore independent Linux guest processes.<br>Manual: requires an explicitly selected standalone node and snapshot-capable environment |
| [node.runtime_crash_preserves_disk](node_runtime_crash_preserves_disk.py) | manual | `direct-node-recovery`; deployed; direct | A killed runtime becomes stopped through normal reconciliation, retains its disk, and can start a new execution. Deletion reclaims its node lease.<br>Manual: requires an explicitly dedicated idle worker and permission to kill its owned runtime and restart managers |
| [node.runtime_memory_lifecycle](node_runtime_memory_lifecycle.py) | manual | `direct-node-recovery`; deployed; direct | Pause and restore preserve RAM and disk; restored executions survive manager restart.<br>Manual: requires an explicitly selected standalone node and snapshot-capable environment |
| [node.runtime_survives_manager_restarts](node_runtime_survives_manager_restarts.py) | manual | `direct-node-recovery`; deployed; direct | Manager restarts preserve a running VM's execution, slot and guest boot; restarting managers while stopped preserves its disk for a fresh start. Cleanup deletes only the run-owned sandbox through the ordinary API.<br>Manual: requires an explicitly dedicated idle worker and permission to restart its managers |
| [node.windows_agent_operations](node_windows_agent_operations.py) | manual | `direct-node-recovery`; deployed; direct | Managed spaced-host preserves process/file semantics across cold starts.<br>Manual: requires a Windows template provisioned for native spaced process/file operations |
| [node.windows_agent_readiness](node_windows_agent_readiness.py) | manual | `direct-node-recovery`; deployed; direct | Blocking start must complete through spaced, including after a cold restart.<br>Manual: requires a native spaced Windows template with OpenSSH on an authorized standalone worker |
| [node.windows_base_memory](node_windows_base_memory.py) | manual | `direct-node-recovery`; deployed; direct | Base snapshots and pool members restore independent Windows guest processes.<br>Manual: requires a Windows template with native spaced memory lifecycle on a standalone KVM worker |
| [node.windows_guest_recovery](node_windows_guest_recovery.py) | manual | `direct-node-recovery`; deployed; direct | Recover a fully hung synthetic trial and retry a settled stopped service.<br>Manual: requires a disposable Windows updater template and pinned Administrator SSH access |
| [node.windows_guest_update](node_windows_guest_update.py) | manual | `direct-node-recovery`; deployed; direct | Update, idempotence, idle deferral, forced termination and persistence.<br>Manual: requires a Windows updater template and an authorized newer Windows bundle |
| [node.windows_memory_lifecycle](node_windows_memory_lifecycle.py) | manual | `direct-node-recovery`; deployed; direct | Manual and automatic dormancy preserve a live Windows process and its memory.<br>Manual: requires a Windows template with native spaced memory lifecycle on a standalone KVM worker |
| [node.windows_sandbox_lifecycle](node_windows_sandbox_lifecycle.py) | manual | `direct-node-recovery`; deployed; direct | No manager restarts or device allocations. Cleanup deletes only its sandbox. RDP negotiation is service evidence; authenticated interactive access is separate. Agent refusal before wake is covered by the manager's guest-image unit tests.<br>Manual: requires a prepared local Windows template on an authorized standalone worker |

<!-- END GENERATED SCENARIO INDEX -->
