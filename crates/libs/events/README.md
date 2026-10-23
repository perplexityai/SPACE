# `pplx-space-events`

Typed structured log events for reconstructing sandbox state timelines from
Datadog logs.

The sandbox event stream is intentionally not request accounting. Existing API
and ECW request logs remain the high-volume request source. This crate is for
low-frequency logical sandbox state changes and the durable handoff facts needed
to debug races: lifecycle requests, completions, failures, CAS losses, cleanup,
snapshot/artifact handoffs, network allocation, network policy outcomes,
network policy enforcement readiness, port exposure outcomes, and restore
identity.

All sandbox events emit with:

- `event_stream = "space.sandbox"`
- `event_schema_version`
- `sandbox_id`
- `event_type`
- `source`
- `cluster_id` — the emitting process's `SPACE_CLUSTER_ID`, so a single Datadog
  org can reconstruct a per-cluster sandbox timeline; empty when unset
- `phase`

Use `SandboxEventType` for the domain transition, `SandboxEventSource` for the
component that observed or emitted it, and `SandboxEventDetails` for optional
context. Keep event names domain-level: prefer `pause_requested` or
`restore_failed`, not component-specific names.

## Event Types

| Event type(s) | Domain meaning | Investigation value |
| --- | --- | --- |
| `create_requested`, `create_completed`, `create_failed`, `create_pending` | Create sandbox control-plane/runtime state. | Captures requested template selector, resolved template content hash, runtime, boot mode, requested shape, and the effective `tls_interception` mode (`on`/`off`, sandbox override over template default) so create-time release resolution and MITM enrollment are explainable. `create_pending` is emitted when the API-side readiness wait exhausts its ALB-aware budget but the scheduled pod is left running to converge — the sandbox id is returned and the row is set to `started` (non-sweepable, reads as Running) while the runtime finishes coming up. |
| `start_requested`, `start_completed`, `start_failed` | Sandbox start transition. | Shows whether a create/start path reached runnable execution. |
| `wake_requested`, `wake_completed`, `wake_failed` | Wake a paused/suspended/recoverable sandbox. | Tracks wake-from-suspend and wake-after-idle across API, substrate, and node-local handling. |
| `resume_requested`, `resume_completed`, `resume_failed` | Resume a paused sandbox. | Separates resume intent from generic process or request logs. |
| `restore_requested`, `restore_completed`, `restore_failed` | Restore a sandbox from suspend snapshot. | Captures user/control-plane restore intent and outcome. |
| `suspend_requested`, `suspend_completed`, `suspend_failed`, `suspend_superseded` | Suspend/checkpoint transition. | Shows whether auto/user/system suspend reached durable suspended state. Auto lifecycle uses `trigger=auto_lifecycle`, not a separate event type. `suspend_superseded` proves a newer authoritative execution generation owns the sandbox, so the stale execution was released without suspending or mutating its replacement; it carries the stale `execution_generation`, `trigger=system`, and `operation=planned_teardown`. |
| `pause_requested`, `pause_completed`, `pause_failed` | Pause in-place without treating it as an implementation RPC. | Gives the exact state-machine transition the user asked about. Auto lifecycle uses `trigger=auto_lifecycle`. |
| `stop_requested`, `stop_completed`, `stop_failed` | Graceful sandbox stop. | Distinguishes stop lifecycle from cleanup internals. Auto lifecycle uses `trigger=auto_lifecycle`. |
| `kill_requested`, `kill_completed`, `kill_failed` | Forceful sandbox termination. | Distinguishes user/operator kill from automatic cleanup. |
| `delete_requested`, `delete_completed`, `delete_failed`, `delete_superseded` | Delete sandbox control-plane/runtime state. | Captures explicit user/operator deletes so disappearing sandboxes are explainable. A node auto-lifecycle completion carries its durable `receipt_id`, `execution_generation`, and `route_activation_id`. `delete_superseded` records that a newer execution generation invalidated a pending node-local delete receipt without deleting the replacement. Automated orphan reconciliation preserves the sandbox row and emits recovery/quarantine events instead. |
| `checkpoint_requested`, `checkpoint_completed`, `checkpoint_failed` | Explicit snapshot/checkpoint operation. | Tracks snapshot-producing operations separately from suspend policy. |
| `fork_requested`, `fork_artifact_ready`, `fork_target_activated`, `fork_completed`, `fork_failed` | Durable disk-only sandbox fork workflow. | Connects admission to the immutable source artifact, each restored child, and the terminal success, deadline, or failure outcome. Events carry the public operation id and include snapshot identity when available. |
| `snapshot_policy_update_requested`, `snapshot_policy_update_completed`, `snapshot_policy_update_failed` | Enable/disable snapshot policy. | Shows DB-only policy changes without pretending they are runtime transitions. |
| `lifecycle_transition_cas_lost` | Lifecycle compare-and-swap lost to another writer. | Pinpoints race conditions where a transition could not safely finalize. |
| `lifecycle_transition_timed_out` | A sandbox stayed in a transient lifecycle past the sweeper age threshold and was moved to `failed`. | Makes cluster-controller cleanup of stuck `creating`/`starting`/`pausing`/`resuming`/`suspending`/`restoring`/`stopping`/`checkpointing` rows visible in the sandbox timeline. |
| `lifecycle_transition_failed_on_node` | A reconciler forced a `started`/`paused` row to `failed` because the sandbox's node-local state had already diverged to `failed`. | Distinguishes the node-drain failed-on-node give-up (an immediate divergence, not a timeout) from `lifecycle_transition_timed_out`, so the sandbox timeline shows why drain unblocked. |
| `sandbox_failed` | The sandbox's lifecycle/state row transitioned into the terminal `failed` state. | Provides one stable query target for alerting and incident timelines (`@event_type:sandbox_failed`) across API inventory publish, cluster-controller reconcilers, and sbx-manager local state. Cause-specific events such as `sandbox_lost`, `lifecycle_transition_timed_out`, and `lifecycle_transition_failed_on_node` remain alongside it for root-cause detail. |
| `sandbox_quarantined` | The sandbox's lifecycle/state row transitioned into the inert `quarantined` hold state. | Provides one stable query target for preserved-evidence holds (`@event_type:sandbox_quarantined`) across API inventory publish and sbx-manager local state. The quarantine state/history tables carry current action and attempted-repair detail. |
| `quarantine_recovered` | A node proved that the durable suspend artifact for a quarantined restore still matched its recorded identity, cleaned the failed execution generation, and returned the sandbox to `suspended`. | Carries the original quarantine reason and the prior/healthy lifecycle boundary. The node-local event records atomic cleanup completion; the API event records generation-fenced receipt consumption and central convergence. |
| `lifecycle_converged` | A sweep verified a row stuck in a transient lifecycle has a live VM on its node and converged the row to the running state the VM already reached, instead of failing it. | Shows that a "stuck" row was Postgres staleness, not a wedged sandbox — the VM survived and no state was destroyed. Carries `from_state`, `to_state`, the probe outcome in `result`, and `age_secs`. |
| `lifecycle_reverted_to_suspended` | A sweep verified a row stuck in `restoring` — the only lifecycle whose ready durable snapshot is provably current — has no live VM, and reverted the row to `suspended` for a later restore, instead of failing it. The sweep then retires the leftover execution, emitting `cleanup_started`/`cleanup_completed`/`cleanup_failed` with `reason = transient_lifecycle_revert`. | Records the recovery decision: the sandbox is restorable from S3 and was not destroyed. Carries `from_state`, the probe outcome in `result`, and `age_secs`. |
| `lifecycle_transition_rolled_back` | A sweep rolled back a lifecycle transition it had just made, because the teardown that transition existed to enable was refused on pod identity. Emitted only where the rollback restores the whole pre-transition state; where a transition's triggers cleared state the rollback cannot reconstruct, the sweep leaves the transition standing and records the deferral instead. | Keeps a timeline from reading as a terminal transition that did not persist. Carries `from_state`, the restored `to_state`, the probe outcome in `result`, and `age_secs`. |
| `sweep_deferred` | A sweep aged a row out but could not prove destruction is safe (VM alive in a non-convergeable lifecycle, node holds the only current copy of the sandbox's state — including a dormant non-`restoring` row whose only backup is a stale prior suspend cycle's — or the probe was inconclusive), so it left the row for a later tick. | Explains why a stuck row persists across ticks instead of resolving, with the probe outcome in `result` and the blocking condition in `reason`. |
| `execution_ensured` | Execution target exists for the sandbox. | Confirms placement/execution materialized below the control-plane state. |
| `execution_replaced` | A stale or missing execution generation was retired and a successor claimed without changing the sandbox lifecycle. | Carries the retired generation, pod name, replacement reason, and the local source node or `restore_storage_uri_present=true` plus restore provenance when replacement preserves started-sandbox state. The remote URI is omitted because its endpoint query can contain credentials. |
| `execution_retired` | A stale active execution record was retired without a lifecycle change — the sandbox's lifecycle had already converged to a dormant state (e.g. node-side auto-suspend published `suspended`), but the execution record stayed active while its backing pod disappeared. | Makes cluster-controller reconciliation of stale suspended-sandbox bookkeeping visible in the timeline: carries the dead `pod_name`/`pod_uid`, `execution_generation`, prior execution state (`from_state`), the cleared node as `source_node_id`, and the cleared prior-cycle artifact identity as `local_artifact_id` when one was present. Dry-run sweeps carry `result=dry_run`. |
| `pool_slot_claimed` | Warm-pool slot was claimed for a sandbox create. | Connects a create to the pool claim id, slot/config ids, pod name/IP, resolved template hash, runtime, and effective pool VM shape. |
| `pool_slot_missed` | Warm-pool lookup found no claimable slot for a sandbox create. | Connects a warm-pool miss to the planned sandbox id, requested selector, resolved template hash, runtime, boot mode, and the effective pool VM shape selected by the attempt before cold fallback or `boot_mode=pool` failure. Shape is absent when no pool exists for the resolved release. |
| `cleanup_requested`, `cleanup_started`, `cleanup_completed`, `cleanup_failed` | Sandbox cleanup lifecycle. | Explains forced cleanup decisions and whether runtime/network cleanup actually finished. |
| `activation_rollback_completed` | The control plane consumed a matching activation-attempt receipt, retired the exact execution, and restored the stable lifecycle. | Distinct from the request-level `create_failed`, `start_failed`, and `restore_failed` events, avoiding duplicate failure events while preserving central convergence. |
| `guest_process_start_failed` | Guest process start failed after sandbox activation was required. | Connects `process.Process/Start` 502s to wake/connect/start-stream failure reason. |
| `snapshot_restore_started`, `snapshot_restore_completed`, `snapshot_restore_failed` | Node-local snapshot artifact restore. | Carries immutable restored snapshot/template identity and restore codec details. Failures name the stage in `reason` and the typed cause in `error_kind` (`parent_template_lookup`, `btrfs_receive`, ...); `error` is free text for humans, so consumers match `error_kind`, not `error`. |
| `snapshot_artifact_pull_started`, `snapshot_artifact_pull_completed`, `snapshot_artifact_pull_failed` | Pull snapshot artifact from durable storage. | Separates storage/transfer failure from runtime restore failure. |
| `local_artifact_published`, `local_artifact_publish_failed` | Publish an immutable node-local artifact from stop, suspend, or named snapshot creation. | Carries `local_artifact_id`, `artifact_kind`, and `artifact_origin` before any optional remote backup; failures include the stage and error. |
| `artifact_backup_queued`, `artifact_backup_completed`, `artifact_backup_failed` | Back up an immutable node-local artifact to durable storage. | Carries the exact `local_artifact_id` and `backup_attempt_id` that gate cross-node restore and node release. |
| `local_snapshot_promotion_abandoned` | Node-drain gave up promoting a suspended sandbox's local snapshot (the on-node reupload was permanently refused because the sandbox was wedged in `restoring`) and adopted the current suspend cycle's ready remote snapshot so the node could finish draining. | Records the drain-progress decision: the sandbox stays `suspended` and recoverable from the adopted snapshot, abandoning the un-uploadable local state. Carries `source_node_id`. |
| `sandbox_lost` | A reconciler determined no recoverable copy of a suspended sandbox's state exists anywhere — the local suspended artifact is gone (pruned, or the node's backing instance confirmed terminated) and no ready remote snapshot exists — and flipped the row to `failed`. | Explains a terminal `suspended → failed` flip on the sandbox timeline: the data was already lost before the flip, and the user's next wake/restore gets a clean lifecycle error instead of a snapshot-not-found. Carries `source_node_id`, from/to state, and a `reason` naming which give-up fired (`drain_sandbox_lost` on a draining node, `gone_node_unrecoverable` on a drained node whose EC2 instance is confirmed terminated). |
| `local_eviction_completed`, `local_eviction_failed` | The node removed (or failed to remove) a sandbox's entire local presence — cache directory including the rootfs instance, plus node-local DB rows. The cold-sandbox eviction loop does it for a suspended sandbox whose state is durably restorable from a ready remote snapshot; node-ctl's facet sync does it for a sandbox the cluster reports deleted; the teardown driver does it for a teardown suspend whose backup the cluster refused because it retired the execution. A sandbox the cluster still has stays restorable from its own snapshots. | Carries `reason` (`latest_backed_idle` — every eviction requires proof tied to the exact bytes: the pinned artifact's confirmed backup, or exact generation equality for legacy unpinned rows; `cluster_row_absent` — the cluster has no row for the sandbox; `execution_retired` — the cluster refused the capture's backup for a retired execution, carrying the discarded `local_artifact_id` and `execution_generation` instead of a remote snapshot or `local_path`), the remote `snapshot_id` relied on where there is one, and the freed `local_path` (logical size only; CoW sharing makes physical reclaim lower); failures carry the error, and one after the local row was claimed leaves it in `deleting` for cleanup retry. |
| `rootfs_stream_reclaimed` | The local rootfs diff stream (`rootfs.btrfs-send`) of a suspended sandbox's current artifact was deleted after its backup reached the durable ready state. Same-node wake is preserved: the snapshot's `memory`/`vmstate` files and the `instance/` rootfs subvolume stay; only the upload-format stream, which no restore path reads locally, is removed. | Carries the exact `local_artifact_id`/`backup_attempt_id` the reclaim was proven against and the reclaimed `local_path`, so a later "where did the stream go" investigation lands on the durable backup that superseded it. |
| `snapshot_pruned` | A retention sweep deleted an old durable snapshot of a still-live sandbox because it fell outside the newest-N `ready` full snapshots the cluster keeps per sandbox — its S3 prefix and DB rows were removed. The sandbox stays alive and restorable from a newer snapshot. | Carries the pruned `snapshot_id` and its `age_secs`; `source = cluster_controller`, `trigger = system`. Emitted by the `snapshot_retention_sweeper` reconciler. |
| `snapshot_artifact_upload_completed`, `snapshot_artifact_upload_failed` | Legacy per-transfer backup telemetry. | Retained for existing transfer dashboards; lifecycle decisions should use `artifact_backup_*` events. |
| `memory_admission_failed` | Reject a new sandbox because node memory cannot be safely admitted. | Distinguishes local memory capacity/pressure protection from networking failures. |
| `runtime_activated` | Firecracker runtime and sidecars have been launched and persisted for the sandbox. | Carries PID, uid, network slot, guest CID/IP/gateway, boot type, memory, vCPU, execution generation, and restore provenance when known so investigators can see the node-local runtime became active. Restores record only that a storage URI was present, plus opaque snapshot and local-artifact identifiers when known; the URI itself is omitted because endpoint query parameters can contain credentials. |
| `guest_bridge_ready`, `guest_bridge_failed` | The host-to-guest bridge sidecar became reachable, or failed to launch. | Separates a running VM from an actually reachable `spaced-host` bridge. |
| `stop_guest_flush_failed` | An ordinary stop could not flush the guest's root filesystem and stopped the VM anyway. | Marks a stop after which writes the guest had not committed can be missing. |
| `guest_ready`, `guest_ready_failed` | Guest readiness was observed through `spaced-host`, or timed out/failed. | Captures the exact usability edge with readiness latency and guest uptime. |
| `network_path_stopped` | The sandbox network path drained and stopped. | Gives a clear teardown edge instead of relying on raw TCP/UDP proxy shutdown lines. |
| `filesystem_removed` | Node-local sandbox filesystem/cache was removed. | Confirms the local cache tree was actually deleted during cleanup. |
| `network_policy_update_requested`, `network_policy_update_completed`, `network_policy_update_failed` | Change the desired sandbox network policy. | Shows override vs template inheritance, desired generation, authority, conflicts, and whether live reconciliation was queued. |
| `network_policy_refresh_started`, `network_policy_refresh_completed`, `network_policy_refresh_failed` | Refresh sandbox network policy. | Captures network policy state that affects sandbox usability. |
| `network_policy_enforcement_started`, `network_policy_enforcement_completed`, `network_policy_enforcement_failed` | Prepare sandbox network policy enforcement. | Captures node-local TPROXY setup, net-gateway control readiness, initial policy hydration, and no-identity transitions. |
| `net_gateway_cycle_requested`, `net_gateway_cycle_completed`, `net_gateway_cycle_failed` | Deliberately cycle (kill + relaunch in place) the sandbox's egress gateway while the VM keeps running. | Records the operator-triggered path for retiring gateways whose launch-time binary predates the node's deploy, with old/new pid and old/new build sha; distinguishes deliberate cycles from watchdog crash-restarts. |
| `network_allocation_completed`, `network_allocation_failed` | Allocate sandbox networking. | Surfaces netns/slot allocation success or exhaustion/degraded/quarantine failures with pool counts. |
| `network_release_completed`, `network_release_failed` | Release sandbox networking. | Confirms cleanup/release freed the network slot, or captures release failure. |
| `port_exposure_open_requested`, `port_exposure_open_completed`, `port_exposure_open_failed` | Make a sandbox port reachable. | Connects API desired state, node-local firewall application, and loopback bridge status for 502/connectivity investigations. |
| `port_exposure_close_requested`, `port_exposure_close_completed`, `port_exposure_close_failed` | Close a sandbox port exposure. | Shows requested port closure and whether desired state was persisted. |
| `port_exposure_apply_started`, `port_exposure_apply_completed`, `port_exposure_apply_failed` | Apply node-local port exposure state. | Captures guest allow-rule and ingress DNAT installation with host port, guest port, protocol, namespace, vpeer IP, ingress interface, and idempotency/failure result. |
| `port_exposure_remove_started`, `port_exposure_remove_completed`, `port_exposure_remove_failed` | Remove node-local port exposure state. | Captures guest allow-rule and ingress DNAT removal, including release-time cleanup of all DNAT rules for a sandbox vpeer. |
| `sandbox_process_launch_completed`, `sandbox_process_launch_failed`, `sandbox_process_pid_persist_failed` | Runtime process launch and PID persistence. | Shows sidecar/runtime launch success, launch/setup failure, and partial-success PID persistence ambiguity. |

## Emission Guidance

- Emit from the code path that requests, completes, fails, reconciles, or
  force-cleans a logical sandbox state transition.
- Do not emit per-request success events for high-volume surfaces such as guest
  process starts. Prefer targeted failure events when a request exposes a
  sandbox-state problem.
- Use `trigger` to distinguish `user`, `auto_lifecycle`, and `system` causes.
- For suspend events, include `ms_since_wake` when the node has a recorded
  completed wake for the sandbox. A small delta on `suspend_requested` is the
  wake/suspend oscillation signature: an external caller re-suspending a
  sandbox moments after a wake-on-access restore, paying a full suspend
  snapshot per cycle.
- Include immutable restore and snapshot identity whenever available: template
  ID, template content hash, template release tags, restored snapshot ID,
  local artifact ID, metadata SHA, storage URI presence, restore codec, and
  rootfs/kernel versions. Do not include the exact storage URI because endpoint
  query parameters can contain credentials.
- For runtime and guest readiness events, include boot type, runtime PID,
  guest CID/IP/gateway, network slot, readiness elapsed time, guest uptime, and
  timeout/error details when available. For `runtime_activated`, include
  `restore_provenance`; `remote_local_mismatch` is the dashboardable signal for
  a runtime that staged one S3 snapshot while the sandbox row still pointed at a
  different node-local artifact.
- For port exposure events, include the port/protocol plus the node-local facts
  an investigator needs to compare host ingress DNAT against guest namespace
  allow rules: `netns`, `vpeer_ip`, `host_port`, `guest_port`,
  `ingress_iface`, and `has_pod_netns`.
- For network policy events, include the desired/applied generation or policy
  refs and distinguish override vs template inheritance with
  `network_policy_source`. `network_policy_refresh_completed` also carries
  `interception_readiness` (`ready`, `not_required`, `dependencies_not_ready`,
  or `unspecified` for an old gateway binary): the gateway's TLS-interception
  dependencies (CA, credential store) are reported separately from
  `network_state` and never gate sandbox start. `dependencies_not_ready` means
  the sandbox reached Started while guest egress under a TLS-on policy fails
  closed per connection until the CA arrives; investigate the node CA path,
  not the sandbox lifecycle.
- For network policy enforcement events, include `operation` to distinguish
  `tproxy_setup` from `initial_policy_hydration`, plus `netns`,
  `installed_network_policy_ref`, `network_state`, and errors when available.
- Add new event types only when they describe a domain transition or durable
  handoff that helps reconstruct sandbox state. Put implementation details in
  `source`, `reason`, or other detail fields.

## Observing Events

A process can install one `SandboxEventObserver` with
`install_sandbox_event_observer`. After each event is logged, the observer
receives the `SandboxEvent` (`sandbox_id`, `event_type`, `source`) and its
`SandboxEventDetails`, whose fields are public. The observer runs on the
emitting thread and must not block; a panic in it is caught. Without an
observer, nothing changes.

Set detail fields through the `SandboxEventDetails` builder methods, not by
assigning fields: the typed setters keep vocabulary fields such as `trigger`
bounded.
