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
from implicit selection.

Manual scenarios run only by name and must declare a nonempty `manual_reason`.
Use manual for an explicit operational prerequisite or a contract whose
acceptance is still deferred; state that prerequisite or decision in its reason. Named
runs can select any tier, while `--all` and `--category` default to routine:

```sh
uv run --frozen --python 3.11 -m space_smoke run --all --tier routine --tier main
uv run --frozen --python 3.11 -m space_smoke list --tier manual
uv run --frozen --python 3.11 -m space_smoke run local.garbage_collectors_reclaim_independent_resources
```

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

The implicit suite runs both `local.leader_election_brownout_keeps_one_leader`
and `local.leader_election_refusal_does_not_restart` against the same controller
image on separate Kind shards. The outage scenario qualifies the shortened
liveness budget by requiring a real budget exit, then checks recovery. The
refusal scenario checks that a forbidden Lease call does not restart a
contender through that full budget. Alongside this observation, a second
contender using the same controller image must exhaust its budget against an
unreachable pod-local API endpoint and exit non-zero. Thus a standalone
refusal run independently qualifies the shortened budget without a second
serial budget wait.

## Cross-node restore coverage

The implicit Kind suite runs
`local.deferred_btrfs_send_capture_restores_cross_node`, which proves remote
restore and preserved state while also checking deferred capture and codec
selection. It verifies the selected parent template, remote placement,
restored filesystem state and working data-plane execution.

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
| [local.forced_drain_evacuates_sandbox](local_forced_drain_evacuates_sandbox.py) | routine | `kind_ecw`; kind; kubernetes | A `:drain` with `mode: forced` parks the resident's worker in `draining` in forced mode and cordons it; the drain reconciler suspends the sandbox and restores it onto the other worker, from object storage, as `started`, with its filesystem intact and its data plane executing; the emptied node then reaches `drained` on its own with the guarded Kubernetes node still present; `:undrain` returns it to schedulable with no drain phase. |
| [local.fork_saturation_safety](local_fork_saturation_safety.py) | manual | `kind_ecw`; kind; kubernetes | Fork waves have distinct children with intact independent disks while a colocated, existing sandbox keeps answering commands and file reads. The source remains suspended, and each wave is deleted before the next one. This is a bounded load check, not a maximum-capacity or latency benchmark.<br>Manual: Explicit bounded fork-pressure run on a dedicated Kind bench |
| [local.fork_suspended_cross_node](local_fork_suspended_cross_node.py) | routine | `kind_ecw`; kind; kubernetes | Off-source placement preserves disk data without resuming the suspended source. |
| [local.garbage_collectors_reclaim_independent_resources](local_garbage_collectors_reclaim_independent_resources.py) | routine | `kind_ecw`; kind; kubernetes | Evict a cold sandbox, reap a fork snapshot, and reclaim an unpinned template. |
| [local.gentle_drain_leaves_sandbox_resident](local_gentle_drain_leaves_sandbox_resident.py) | routine | `kind_ecw`; kind; kubernetes | A `:drain` with no body parks the resident's worker in `draining` with `drain_mode = gentle` and cordons it; through a twenty-second observation the sandbox keeps its lifecycle, pod, pod UID and node pin, gains no snapshot, and the user API keeps serving it; once the sandbox suspends the node reaches `drained` on its own with the guarded Kubernetes node still present; `:undrain` returns it to schedulable with no drain phase. |
| [local.leader_election_brownout_keeps_one_leader](local_leader_election_brownout_keeps_one_leader.py) | routine | `kind_admin`; kind; kubernetes | Two Ready replicas share one Lease holder and one reconciler runner. With the apiserver unavailable, readiness clears, the holder stops before its lease can be taken over, and a replica exits non-zero within its liveness budget. After recovery, recorded exits and telemetry establish why each stopped, and two Ready replicas share one fresh holder and runner. |
| [local.leader_election_refusal_does_not_restart](local_leader_election_refusal_does_not_restart.py) | routine | `kind_admin`; kind; kubernetes | A third controller whose Lease calls are refused remains NotReady and unrestarted past its full liveness budget and restart visibility margin. Two healthy replicas stay Ready without restarting, keep the same Lease holder, and only that holder runs reconcilers throughout the observation. An independently unreachable contender must spend the shortened budget and exit non-zero, proving this run observed the full effective window. |
| [local.node_ctl_fence_refuses_and_reconciles](local_node_ctl_fence_refuses_and_reconciles.py) | routine | `kind_ecw`; kind; kubernetes | With the token kubelet projects into the node-ctl pod on this sandbox's worker, every forbidden Node write (taints, cordon, finalizers, the node-group boundary and cred-authority labels, foreign or inflated status resources, forged conditions, a template-write pull request, and anything on another node) is refused by the fence policy and by the named rule, leaves no residue, and the boundary label is otherwise writable; every write shape node-ctl has (SSD placement apply, host-CPU label apply, template-presence apply and prune, template-write clear, capacity apply) is admitted; and the running node-ctl writes back a stripped owned label and the stripped slot capacity through its own credential. |
| [local.node_group_boundary_schedules](local_node_group_boundary_schedules.py) | routine | `kind_ecw`; kind; kubernetes | With the placement gate off a created pod carries no node-group affinity term; a dedicated group is refused with a 409 while the cluster has no generic group; and with the labeler and the gate on, a generic create renders `node-restriction.kubernetes.io/space-node-group In (<default group>)` and its pod reaches Running on a labelled worker. |
| [local.node_selector_extras_render_and_enforce](local_node_selector_extras_render_and_enforce.py) | main | `kind_ecw`; kind; kubernetes | With two satisfiable extras configured on both renderers, an ECW create's pod (rendered by space-api) and a native create's pod (rendered by the cluster controller's dispatcher) each carry `spec.nodeSelector` equal to the discriminator plus every extra and run on a node carrying all of them; with one extra no node carries added, the fleet still satisfies the old set, nothing satisfies the new one, and an ECW create is refused before any pod with `429 Server capacity limit: no eligible sandbox activation nodes …` and a Retry-After; the renderers' env is restored to what was captured. |
| [local.orphaned_restore_pod_gc_reaps_only_superseded](local_orphaned_restore_pod_gc_reaps_only_superseded.py) | routine | `kind_ecw`; kind; kubernetes | With the GC live, a restore pod below the started row's active generation and one whose sandbox row does not exist are each deleted, no sooner than the threshold; a restore done while the GC is live leaves one execution whose pod keeps its uid well past the threshold plus several ticks; and a suspended sandbox with an orphan on its own row at its retired generation, which suspend retired as `suspended` rather than `replaced`, keeps that orphan through the survival window (the backup fence would admit its teardown capture) while a row-less control planted beside it is reaped, then restores with its state and the orphan is gone once the restore supersedes it. |
| [local.pending_backup_census_reports_blocked_upload](local_pending_backup_census_reports_blocked_upload.py) | routine | `kind_ecw`; kind; kubernetes | With object storage unreachable, a suspend operation leaves a snapshot in `creating`, the node's next Full census reports the pinned artifact as pending, and space-internal-api mirrors it into the cluster queue; once storage returns, the snapshot becomes `ready` with a storage location, the queue row disappears, and the suspend operation ends `succeeded`. |
| [local.private_template_build_isolation](local_private_template_build_isolation.py) | routine | `kind_admin`; kind; kubernetes | An owner can derive a working template from its private base, while a different authenticated organization cannot build from the same tag or digest, including after the owner's successful materialization. |
| [local.rate_limiters_enforce_and_fail_open](local_rate_limiters_enforce_and_fail_open.py) | routine | `kind_ecw`; kind; kubernetes | With both limiters' caps shrunk and per-client concurrency in enforce mode, sequential creates drain the cluster activation bucket to a 429 `Server rate limit:` with Retry-After; twenty concurrent creates and twenty concurrent connects for one client id each draw at least one 429 `Per-client rate limit:` with Retry-After; once Redis is unreachable every create admits a sandbox; and once it is back the per-client limiter rejects again. |
| [local.reconcile_queue_gc_prunes_only_gone_nodes](local_reconcile_queue_gc_prunes_only_gone_nodes.py) | routine | `kind_admin`; kind; kubernetes | With the reconcile-queue GC on against the fake EC2 endpoint, one tick deletes a 25 h-old dead letter and the 25 h-old live ref of a non-ready node whose Kubernetes Node does not exist, and keeps a 1 h-old dead letter and the 25 h-old live ref of a non-ready node named after a Ready Node. |
| [local.restarted_btrfs_send_is_rejected_and_retried](local_restarted_btrfs_send_is_rejected_and_retried.py) | routine | `kind_ecw`; kind; kubernetes | A deferred capture whose kernel send is interrupted while blocked on its pipe fails that upload attempt at node-manager's framing check instead of storing a concatenated stream; the retried upload restores onto another worker with the sandbox's data intact. |
| [local.retention_claim_finish_forward](local_retention_claim_finish_forward.py) | main | `kind_ecw`; kind; kubernetes | A crash-orphaned `deleting` claim finishes forward without violating retention or sandbox usability. |
| [local.retention_restore_source_guard](local_retention_restore_source_guard.py) | routine | `kind_ecw`; kind; kubernetes | The live retention sweeper prunes unpinned history past keep-N while the current-artifact pin — the exact snapshot restore loads for a marked suspended sandbox — survives the sweep and the sandbox still resumes. |
| [local.sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown](local_sandbox_manager_restart_leaves_teardown_suspend_to_its_teardown.py) | routine | `kind_ecw`; kind; kubernetes | With the suspend's backup held, restarting the worker's sandbox-manager leaves the sandbox's row `suspending`, rather than marking it suspended behind its teardown; once uploads return, the suspend operation succeeds and the sandbox wakes with the state it had. |
| [local.sandbox_network_isolation_holds](local_sandbox_network_isolation_holds.py) | routine | `kind_ecw`; kind; kubernetes | From a running sandbox pod's network namespace, entered on its node outside the VM, Postgres, the Kubernetes API Service, internal-api, the object store and another sandbox's pod IP on 9800 are all refused while the cluster DNS, and the internet when this host has it, still connect; every refused target answers from the control-plane namespace; a pod in that namespace cannot open the sandbox pod's 9800; a pod in the sandbox namespace that no allow rule selects reaches nothing; and the guest still reaches the internet when this host does. |
| [local.stalled_node_delete_releases_its_dispatcher_slot](local_stalled_node_delete_releases_its_dispatcher_slot.py) | routine | `kind_ecw`; kind; kubernetes | A Delete whose node does not answer holds a dispatcher slot for one bounded attempt at a time: it keeps the sandbox and retries until the node admits the delete, and continues as soon as the node has admitted it. |
| [local.storage_exchange_scoping](local_storage_exchange_scoping.py) | routine | `kind_ecw`; kind; kubernetes | A read-scoped session that a sandbox worker's credential-manager agent vends on its volume socket, as volume-manager asks, for one object of a sandbox's suspend snapshot, reads that object; the object store refuses it for another sandbox's snapshot object and listing, for the templates bucket, and for a small write to its own object, which stays unchanged; and the authority refuses to vend that sandbox's subject for the other sandbox's object at all. Afterwards the session, and a fresh one, still read the object, so no refusal was a session that stopped working. A builder's agent likewise vends a read session for one published file of the sandboxes' template, as the builder volume-manager asks for a template pull; it reads that file, and the store refuses it another file of the same release, the sandbox's snapshot object and, when another template is published, that template's file; the authority refuses to vend the template's subject for another template's file at all. The same closing controls follow. |
| [local.suspend_snapshot_rows_are_per_attempt](local_suspend_snapshot_rows_are_per_attempt.py) | routine | `kind_ecw`; kind; kubernetes | Two suspends of one sandbox leave two ready snapshot rows with distinct ids and distinct id-named storage prefixes; the first row's facts are unchanged by the second publish; and the restore after the second suspend brings back the second suspend's bytes. |
| [local.telemetry_reaches_local_backends](local_telemetry_reaches_local_backends.py) | routine | `kind_ecw`; kind; kubernetes | Once a sandbox has run a command, Tempo returns containerd-shim-space-v2's create in the space-api trace that created it, parented by the API's pod materialization span, Prometheus holds create latency from that shim and from space-api since the scenario began, and Loki holds a SPACE pod log line naming the sandbox. |
| [local.template_startup_recovery_decision_table](local_template_startup_recovery_decision_table.py) | routine | `kind_ecw`; kind; kubernetes | After a worker's template-manager restarts, startup recovery keeps a build whose row and bytes agree, prunes a row whose bytes are gone, adopts row-less bytes only when the artifact hashes to the directory name, removes row-less bytes whose hash disagrees, and removes bytes whose size disagrees with the manifest even with a row — leaving disk and catalog consistent and the sandbox on the other worker running. |
| [local.unanswered_template_deleted_after_wait](local_unanswered_template_deleted_after_wait.py) | routine | `kind_ecw`; kind; kubernetes | With node-ctl's unanswered-template wait shortened, a worker keeps an org template through a full sync past the wait while a sandbox of it runs there, keeps it through two full syncs after the sandbox is deleted but before the wait has passed, and deletes it (unlisted, its bytes and then its Node template-build metadata gone) in the first full sync after the wait; a sandbox of the same template placed on that worker afterwards pulls it again and boots. |
| [native.aws_secret_network_credential_injection](native_aws_secret_network_credential_injection.py) | manual | `command_sandbox`; deployed, kind; direct, kubernetes | A flat AWS reference injects its secret only after a policy binds it.<br>Manual: Requires an explicitly provisioned AWS secret and a selected echo endpoint. |
| [native.encrypted_double_restore_retires_prior_pods](native_encrypted_double_restore_retires_prior_pods.py) | routine | `command_sandbox`; deployed, kind; kubernetes | A BYOK sandbox survives suspend, restore, suspend and restore with its file intact, and each restore leaves only the serving execution's pod. |
| [native.forced_drain_re_drains_re_placed_stopped_sandbox](native_forced_drain_re_drains_re_placed_stopped_sandbox.py) | routine | `command_sandbox`; deployed, kind; kubernetes | With the three stopped-evacuation flags on, a forced `:drain` re-places a `stopped` sandbox on the other worker. Draining that worker forced, before any start, releases the sandbox from it without a new disk backup, and its next re-placement pod waits `Unschedulable`, excluding both workers by name in every required node-affinity term, while the row stays a claimed re-placement. |
| [native.forced_drain_re_drains_restarted_re_placed_stopped_sandbox](native_forced_drain_re_drains_restarted_re_placed_stopped_sandbox.py) | routine | `command_sandbox`; deployed, kind; kubernetes | With the three stopped-evacuation flags on, a forced `:drain` re-places a `stopped` sandbox on the other worker. A user start runs it there in a pod pre-bound to that worker, and it's stopped again. Draining that worker forced releases the sandbox from it with a new disk backup published, and its next re-placement pod waits `Unschedulable`, excluding both workers in every required node-affinity term. |
| [native.m1_generated_sdk_conformance](native_m1_generated_sdk_conformance.py) | routine | `kind-native-hardening`; kind; kubernetes | The pinned OpenAPI Generator turns the served /openapi-m1.json into a Python SDK that runs all 14 M1 operations against the API and data plane: create, read and list, command over SSE with a Last-Event-ID reconnect, process control, pause, suspend and wake, delete, and the documented error envelopes, including 400 for a malformed sandbox id. |
| [native.network_policy_live_update](native_network_policy_live_update.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | PATCHing a running sandbox's network policy changes its egress in place: a destination is allowed, revoked, and allowed again, each change is enforced and reflected by the read API, and the guest never reboots. |
| [native.resident_pause](native_resident_pause.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | A resident pause and wake preserve filesystem state without rebooting. |
| [native.stopped_restart_preserves_rootfs](native_stopped_restart_preserves_rootfs.py) | routine | `command_sandbox`; deployed, kind; direct, kubernetes | Stopping a sandbox parks its rootfs on the node and reports `stopped`; starting it again runs from that rootfs, so a file written before the stop reads back unchanged after the restart. |
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
| [native_contract.file_upload_admission_429](native_contract_file_upload_admission_429.py) | main | `kind-native-hardening`; kind; kubernetes | With the data plane's Native upload admission on at one write per sandbox and no wait, writes held open to one sandbox on more connections than there are data-plane replicas leave at least one refused before its body is read, with 429 `rate_limited`, `retryable` and a `Retry-After` of 1 to 5 seconds; every admitted write lands once its body ends; and the data plane's standing admission settings are put back after. |
| [native_contract.file_write_size_boundary_on_the_data_plane](native_contract_file_write_size_boundary_on_the_data_plane.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | A `files:write` of exactly 16 MiB sent to the data plane lands whole; one byte more is refused with 400 `invalid_request` and writes nothing, as the API answers both. |
| [native_contract.permanent_command_error](native_contract_permanent_command_error.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Missing executables and working directories yield permanent command failures. |
| [native_contract.process_session_wakes_idle_suspended_sandbox](native_contract_process_session_wakes_idle_suspended_sandbox.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | With `wake.on=["access"]`, a process session created after an idle suspend and attached over WebSocket runs without a wake first. |
| [native_contract.session_reattach_wakes_idle_suspended_sandbox](native_contract_session_reattach_wakes_idle_suspended_sandbox.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | With `wake.on=["access"]`, a command attached over SSE and a process session attached over WebSocket, both created while the sandbox ran, each wake it after an idle suspend and deliver their output. |
| [native_contract.session_reconnect_after_idle_suspend](native_contract_session_reconnect_after_idle_suspend.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Under `wake.on=["access"]` the reattach wakes the sandbox and reads the running command's later output; under `wake.on=["command"]` it is the final `sandbox_not_running` conflict, and the sandbox stays suspended. |
| [native_contract.tenant_isolation](native_contract_tenant_isolation.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | Another organization cannot read or list an owned sandbox or its operation. |
| [native_contract.token_scope](native_contract_token_scope.py) | routine | `native-hardening`; deployed, kind; direct, kubernetes | An attachment credential cannot access management endpoints or another session. |
| [native_recovery.attached_stream_drains_through_data_plane_restart](native_recovery_attached_stream_drains_through_data_plane_restart.py) | routine | `kind-native-hardening`; kind; kubernetes | A stream attached before SIGTERM keeps delivering, and the old replica exits when it ends. |
| [native_recovery.command_survives_data_plane_restart](native_recovery_command_survives_data_plane_restart.py) | routine | `kind-native-hardening`; kind; kubernetes | A running command reconnects after data-plane replacement without reexecution. |
| [native_recovery.inline_fallback_at_one_permit_loses_no_operation](native_recovery_inline_fallback_at_one_permit_loses_no_operation.py) | routine | `kind-native-hardening`; kind; kubernetes | At `SPACE_NATIVE_INLINE_MAX_CONCURRENT=1`, concurrent creates with no dispatcher running start inline at most once per API pod; the rest stay queued, and every one succeeds once the dispatcher is back. |
| [native_recovery.inline_load_survives_leader_failover](native_recovery_inline_load_survives_leader_failover.py) | routine | `kind-native-hardening`; kind; kubernetes | Creates and wakes the API runs inline while no controller runs, with a suspend queued behind them for the dispatcher, then a new controller leader elected, which runs the suspend while their claims are live: each create and wake is claimed once and ends once, and every operation succeeds as requested. On a separate sandbox, a wake admitted behind a queued suspend stays queued and later leaves its guest running. |
| [native_recovery.inline_wake_survives_api_pod_kill](native_recovery_inline_wake_survives_api_pod_kill.py) | routine | `kind-native-hardening`; kind; kubernetes | A wake the API is running when every API pod is killed is not lost: once its claim goes stale the dispatcher takes the same operation over, ends it once, and the sandbox runs commands. |
| [native_recovery.queued_create_survives_api_restart](native_recovery_queued_create_survives_api_restart.py) | routine | `kind-native-hardening`; kind; kubernetes | Persisted acceptance and same-key replay survive API loss while work is queued. |
| [native_recovery.slow_operation_does_not_block_later_work](native_recovery_slow_operation_does_not_block_later_work.py) | routine | `kind-native-hardening`; kind; kubernetes | With an idle dispatcher slot, a later pause finishes while an earlier sandbox suspend is held running by a reversible object-storage outage. |
| [native_recovery.stale_queued_work_expires_after_controller_outage](native_recovery_stale_queued_work_expires_after_controller_outage.py) | routine | `kind-native-hardening`; kind; kubernetes | A create and a pause queued through an outage longer than the operation deadline expire without running; a delete queued with them still runs. |
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
