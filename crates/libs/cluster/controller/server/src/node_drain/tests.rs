//! # Test matrix
//!
//! Every behavior the reconciler must exhibit maps to ≥1 test below.
//! Names follow `<category>_<scenario>`.
//!
//! ## C — `ReconcilerConfig` + env parsing
//! - C1 `c1_defaults_when_env_unset`
//! - C2 `c2_each_env_var_overrides_default`
//! - C3 `c3_invalid_env_values_fall_back_to_defaults`
//! - C4 `c4_enabled_truthy_parsing`
//!
//! ## R — `run()` loop lifecycle
//! - R1 `r1_enabled_false_returns_on_shutdown_without_ticking`
//! - R2 `r2_enabled_true_ticks_until_shutdown`
//! - R3 `r3_drain_evacuations_awaits_in_flight_tasks_on_shutdown`
//!
//! ## M — Metrics emission
//! - M1 `m1_tick_queries_node_phase_counts_each_tick`
//!
//! ## D — `draining → drained` transition gate
//! - D1 `d1_empty_resident_marks_drained`
//! - D2 `d2_non_empty_resident_stays_draining`
//! - D3 `d3_in_flight_blocks_drained_even_when_resident_empty`
//! - D4 `d4_phase_drift_skips_drained_transition_silently`
//! - D5 `d5_db_error_on_set_state_logged_no_transition`
//! - D6 `d6_db_error_on_node_phase_counts_does_not_crash_tick`
//! - D7 `d7_db_error_on_list_draining_does_not_block_terminating`
//!
//! ## L — Local-snapshot promotion + drain-blocker count
//! - L1 `l1_pinned_suspended_blocks_drained_transition_until_promoted`
//! - L2 `l2_already_s3_backed_sandboxes_dont_block_drain`
//! - L3 `l3_promotion_failure_does_not_block_other_sandboxes_or_advance_state`
//! - L4 `l4_suspending_restoring_block_drain_even_without_promotable_pin`
//! - L5 `l5_artifact_missing_marks_sandbox_failed_so_drain_can_proceed`
//!
//! ## AC — Abandon-stuck-transient sweep
//! - AC1 `ac1_sweep_runs_with_configured_age_each_tick`
//! - AC2 `ac2_abandoned_rows_unblock_drain_within_one_tick`
//! - AC3 `ac3_none_disables_sweep`
//! - AC4 `ac4_db_error_on_sweep_does_not_block_tick`
//! - AC5 `ac5_undrained_node_aborts_sweep`
//! - AC6 `ac6_stuck_restoring_only_resident_drains_only_with_sweep`
//!
//! ## G — KubeNodeGone recovery for draining nodes
//! - G1 `g1_gone_node_without_lookup_parks_and_keeps_promoting`
//! - G2 `g2_gone_node_with_live_instance_parks_and_keeps_promoting`
//! - G3 `g3_gone_node_lookup_error_parks_and_keeps_promoting`
//! - G4 `g4_gone_node_terminated_bulk_fails_and_adopts_without_promote_rpcs`
//! - G5 `g5_gone_node_adopt_miss_falls_back_to_mark_lost`
//! - G6 `g6_gone_node_giveup_disabled_leaves_pinned_rows_alone`
//! - G7 `g7_gone_node_recovery_converges_to_drained`
//! - G8 `g8_gone_node_never_spawns_evacuations_even_when_bulk_fail_refuses`
//! - G9 `g9_gone_node_terminalizes_inflight_snapshot_uploads`
//!
//! ## S — Evacuation spawning + parallelism
//! - S1 `s1_evacuatable_sandbox_spawns_one_evacuation_each`
//! - S2 `s2_per_node_cap_bounds_spawn`
//! - S3 `s3_per_cluster_cap_bounds_spawn_cross_node`
//! - S4 `s4_already_in_flight_not_respawned`
//! - S5 `s5_resident_non_evacuatable_blocks_drain`
//! - S6 `s6_db_error_on_list_resident_skips_node`
//! - S7 `s7_db_error_on_list_evacuatable_skips_node`
//! - S8 `s8_cluster_cap_fully_saturated_spawns_zero`
//! - S9 `s9_per_node_cap_fully_saturated_with_cluster_slack_spawns_zero`
//! - S10 `s10_configured_placement_flows_through_to_restore`
//!
//! ## F — Evacuation outcomes
//! - F1 `f1_successful_evacuation_clears_in_flight`
//! - F2 `f2_evacuation_failure_clears_in_flight_for_retry`
//! - F3 `f3_evacuate_error_phase_tags_correctly`
//!
//! ## T — `terminating → deleted` transition
//! - T1 `t1_happy_path_terminates_and_marks_deleted`
//! - T2 `t2_asg_failure_records_last_error_stays_terminating`
//! - T3 `t3_kube_node_gone_keeps_terminating_for_manual_recovery`
//! - T4 `t4_stale_identity_skips_silently_no_db_write`
//! - T5 `t5_phase_drift_blocks_delete_after_terminate`
//! - T6 `t6_phase_drift_blocks_terminating_error_record`
//! - T7 `t7_db_error_on_list_terminating_skips_all`
//! - T8 `t8_db_error_on_set_state_during_terminate_error_record`
//! - T9 `t9_db_error_on_set_state_during_advance_to_deleted`
//!
//! ## P — providerID parsing
//! - P1 `p1_parse_instance_id_matrix`
//!
//! ## W — End-to-end workflows
//! - W1 `w1_drain_started_sandboxes_completes_in_two_ticks`
//! - W2 `w2_paused_evacuates_lifecycle_preserving` — paused sandboxes
//!   evacuate alongside started ones without operator action, with the
//!   pre-evacuation lifecycle preserved by issuing the restore with
//!   `target_state = Paused` (the VM never resumes)
//! - W3 `w3_drained_node_does_not_self_terminate` — this reconciler
//!   doesn't touch drained nodes; `drained_node_terminator` does
//! - W4 `w4_undrain_mid_evacuation_stops_new_spawns`
//! - W5 `w5_pausing_blocks_drain_until_settled` — a transient `pausing`
//!   sandbox holds the drain until it settles into `paused`, then
//!   evacuates lifecycle-preservingly
//! - W6 `w6_paused_evacuate_failure_clears_in_flight_for_retry` —
//!   paused-path Restore failures (single restore RPC issued with
//!   `target_state = Paused`; failure must NOT silently fall back to
//!   `lifecycle='started'`) propagate as `EvacuateError::Restore`,
//!   clear `in_flight`, and stay tagged pre_lifecycle=Paused on retry
//!
//! ## RP — Cluster-wide cohort-restore sweep
//! - RP1 `rp1_evacuation_restored_onto_cohort_when_snapshot_ready`
//! - RP2 `rp2_evacuated_paused_restored_lifecycle_preserving`
//! - RP3 `rp3_source_drains_once_uploaded_independent_of_restore`
//! - RP4 `rp4_restore_failure_clears_inflight_for_retry`
//! - RP5 `rp5_in_flight_restore_not_respawned`
//! - RP6 `rp6_restore_shares_cluster_budget_with_suspend`
//! - RP7 `rp7_redrive_stranded_suspending_feeds_restore`
//! - RP8 `rp8_restore_slots_filled_in_list_order` — under slot pressure the
//!   ordered restore-pending list (attempt tiers, then recency) is consumed
//!   front-first, so never-failed hot sandboxes restore before cold ones and
//!   serial failers
//! - RP9 `rp9_converging_restore_redriven_until_restored` — a Converging
//!   wake timeout is not a failure; the slot is released and the next
//!   sweep re-drives the same sandbox until the restore lands
//! - RP10 `rp10_restore_failure_schedules_retry_backoff` — a failed cohort
//!   restore schedules the exponential retry backoff with the configured
//!   base/cap
//! - RP11 `rp11_restore_success_schedules_no_retry_backoff`
//! - RP12 `rp12_deferred_restore_schedules_no_retry_backoff` — a user
//!   takeover (`Deferred`) is not a failure; no backoff is written
//! - RP13 `rp13_backoff_disabled_schedules_no_retry` — base `0` disables the
//!   backoff: no attempt state written, and the sweep bypasses stale windows

use pplx_space_common_core::types::{NodeId, SandboxId};

use super::*;
use crate::drained_node_terminator::InstanceLookupError;
use pplx_space_api::controllers::substrate::WaitReadyTimeoutPhase;
use pplx_space_common_core::kubernetes::EvacuationPlacement;

use std::sync::Mutex as StdMutex;

/// Queue-of-queues per node mirroring how the production
/// `list_evacuatable_sandboxes_on_node` advances across ticks: each
/// outer call pops the front list. Bound as a type alias to keep
/// clippy's `type_complexity` lint quiet on the `FakeSink` struct.
type EvacuableListings = HashMap<NodeId, Vec<Vec<(SandboxId, SandboxLifecycle)>>>;

/// `(sandbox_id, source_node_id, settle, expected_uid)` recorded per
/// gone-node adopt call. Aliased to keep clippy's `type_complexity` lint
/// quiet on the `FakeSink` struct.
type AdoptedOnGoneNode = Vec<(SandboxId, NodeId, PositiveDuration, Option<String>)>;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct StateUpdate {
    node_id: NodeId,
    expected: String,
    new_state: String,
    last_error: Option<String>,
}

/// Test double for `NodeDrainSink`.
///
/// - `draining_list` / `terminating_list` are explicit and independent
///   from `node_state`, so a test can model "listing returns the
///   node but state has drifted under us" (D4, T5, T6).
/// - `node_state` is the authoritative state map honored by
///   `set_drain_phase`'s expected-state guard, mirroring the
///   production `WHERE state = $4` clause.
/// - `evacuation_permits` lets a test gate `evacuate_sandbox` to model
///   in-flight tasks across ticks without races (S4, W1, W4).
#[derive(Default)]
struct FakeSink {
    draining_list: StdMutex<Vec<NodeId>>,
    /// Drain mode per node. A node absent from the map lists as
    /// [`NodeDrainMode::Gentle`], mirroring production: the column is read
    /// through `NodeDrainMode::from_column_str`, where NULL and unrecognized
    /// values both resolve to gentle. Tests that want the evacuating path call
    /// [`FakeSink::set_forced`].
    draining_modes: StdMutex<HashMap<NodeId, NodeDrainMode>>,
    /// `(node, exclude_legacy_rows)` per pinned-list call, so a test can pin that
    /// gentle drains refuse the legacy reupload route.
    pinned_legacy_exclusions: StdMutex<Vec<(NodeId, bool)>>,
    terminating_list: StdMutex<Vec<NodeId>>,
    node_state: StdMutex<HashMap<NodeId, String>>,

    // Per-node queue of resident sandbox listings (pops front each
    // call so a test can model a node emptying across ticks).
    resident_listings: StdMutex<HashMap<NodeId, Vec<Vec<SandboxId>>>>,
    // Per-node queue of evacuatable `(sandbox_id, pre_lifecycle)`
    // listings. If empty for a node, the fake mirrors the most recent
    // resident listing as Started — preserving the pre-paused-
    // evacuation default that resident == started.
    evacuatable_listings: StdMutex<EvacuableListings>,
    last_resident: StdMutex<HashMap<NodeId, Vec<SandboxId>>>,

    state_updates: StdMutex<Vec<StateUpdate>>,
    state_counts_calls: StdMutex<usize>,

    // One-shot DB-error injection per method.
    fail_list_draining: StdMutex<Option<String>>,
    fail_list_terminating: StdMutex<Option<String>>,
    fail_list_resident: StdMutex<Option<String>>,
    fail_termination_blockers: StdMutex<Option<String>>,
    fail_list_evacuatable: StdMutex<Option<String>>,
    fail_set_state: StdMutex<Option<String>>,
    fail_state_counts: StdMutex<Option<String>>,

    // Evacuation recording + per-sandbox failure injection.
    // Records `(sandbox_id, source_node_id, pre_lifecycle)` per evacuate call.
    // (Evacuate is now suspend-only; placement flows to the restore sweep,
    // recorded in `restored_evacuations`.)
    evacuated: StdMutex<Vec<(SandboxId, NodeId, SandboxLifecycle)>>,
    evacuate_failures: StdMutex<HashMap<SandboxId, EvacuateError>>,
    // None = no gating (instant return). Some(sem) = each call
    // acquires a permit (test must add permits to release).
    evacuation_permits: StdMutex<Option<Arc<tokio::sync::Semaphore>>>,

    // Cordon-call recording + per-node failure injection. Records the
    // ordered sequence of (node_id, success?) so tests can assert
    // ordering against list_sandboxes_on_node or terminator.terminate.
    cordon_calls: StdMutex<Vec<NodeId>>,
    cordon_failures: StdMutex<HashMap<NodeId, CordonError>>,
    // Nodes whose `ensure_node_unschedulable` reports the kube Node gone
    // (`Ok(CordonOutcome::KubeNodeGone)`), modeling a deleted kube Node.
    cordon_gone: StdMutex<HashSet<NodeId>>,

    // Recorded `(node_id, expected_uid)` per
    // `fail_resident_sandboxes_on_gone_node` call, so tests can assert the
    // KubeNodeGone bulk give-up fired with the uid the EC2 cross-check
    // observed.
    failed_on_gone_node_calls: StdMutex<Vec<(NodeId, Option<String>)>>,
    // Per-node queue of counts the next
    // `fail_resident_sandboxes_on_gone_node` call reports flipped.
    // Empty queue = 0 rows (nothing resident matched).
    fail_on_gone_node_returns: StdMutex<HashMap<NodeId, Vec<u64>>>,
    // Recorded `(sandbox_id, source_node_id, expected_uid)` per
    // `mark_sandbox_lost_on_gone_node` call.
    marked_lost_on_gone_node: StdMutex<Vec<(SandboxId, NodeId, Option<String>)>>,
    // Recorded `(sandbox_id, source_node_id, settle, expected_uid)` per
    // `make_sandbox_durable_on_gone_node` call. The shared
    // `adopt_remote_snapshot_noop` set models its 0-rows outcomes too, so a
    // test can drive the adopt -> mark-lost fallback identically for both
    // adopt variants.
    adopted_on_gone_node: StdMutex<AdoptedOnGoneNode>,
    // Recorded `(node_id, expected_uid)` per
    // `fail_inflight_snapshots_on_gone_node` call (returns 0 flipped rows —
    // the reconciler only logs on >0, so tests assert dispatch, not counts).
    failed_inflight_snapshots_calls: StdMutex<Vec<(NodeId, Option<String>)>>,

    // Local-only suspended sandboxes per node, recorded as a queue so
    // a test can model the work queue draining across ticks (each call
    // pops the front list).
    pinned_listings: StdMutex<HashMap<NodeId, Vec<Vec<SandboxId>>>>,
    // Extra per-lifecycle counts returned by
    // `count_termination_blocking_residents_on_node` on top of the rows the
    // resident scan already saw. Models the four lifecycles the terminator's
    // exclusion-list guard counts and the resident include-list does not
    // (`created`, `stopped`, `deleting`, `quarantined`) — the fake speaks in
    // ids, not lifecycles, so they cannot come out of `resident_listings`.
    // Queue-shaped per node, same as `extra_blocker_counts`.
    extra_termination_blockers: StdMutex<HashMap<NodeId, Vec<LifecycleBreakdown>>>,
    // Calls to `count_termination_blocking_residents_on_node`, so a test can
    // assert the scan ran even when the gate suppressed acting on it.
    termination_blocking_calls: StdMutex<usize>,
    // Extra blocker count returned by `count_drain_blocking_sandboxes_on_node`
    // on top of `pinned_listings.len()`. Models sandboxes in
    // `suspending`/`restoring` that pin the node without being promotable.
    // Queue-shaped per node, same as `pinned_listings`.
    extra_blocker_counts: StdMutex<HashMap<NodeId, Vec<i64>>>,
    // Remembered length of the most recent `list_pinned_local_only_…`
    // return value per node, so `count_drain_blocking_sandboxes_on_node`
    // can return a count consistent with the list. Mirrors how the
    // production queries see the same DB state.
    last_pinned_listing_size: StdMutex<HashMap<NodeId, usize>>,
    // Nodes whose `list_pinned_local_only_sandboxes_on_node` must fail,
    // modelling the `statement_timeout` cancellation that wedged a forced
    // production drain. Sticky rather than queue-shaped: the timeout
    // recurred on every tick, and a one-shot failure would let a test pass
    // on the retry instead of on the resilience being asserted.
    pinned_listing_failures: StdMutex<HashSet<NodeId>>,
    // Same, for `count_drain_blocking_sandboxes_on_node`, which sits between
    // the pinned enumeration and the release gate.
    blocker_count_failures: StdMutex<HashSet<NodeId>>,
    // Recorded `(sandbox_id, source_node_id)` per promote call so tests
    // can assert dispatch behavior.
    promoted: StdMutex<Vec<(SandboxId, NodeId)>>,
    promote_failures: StdMutex<HashMap<SandboxId, PromoteError>>,
    // Recorded `(sandbox_id, source_node_id)` per `mark_sandbox_lost`
    // call so the artifact-missing policy can be asserted in tests.
    marked_lost: StdMutex<Vec<(SandboxId, NodeId)>>,
    // Recorded `(sandbox_id, source_node_id)` per `mark_sandbox_failed_on_node`
    // call so the failed-on-node give-up policy can be asserted in tests.
    marked_failed_on_node: StdMutex<Vec<(SandboxId, NodeId)>>,
    // Set of sandbox ids `mark_sandbox_failed_on_node` should report as
    // drifted (returns `0` rows affected) instead of the default `1`,
    // modelling the production CAS missing because the sandbox moved.
    mark_failed_on_node_drift: StdMutex<HashSet<SandboxId>>,
    // Recorded `(sandbox_id, source_node_id, settle)` per
    // `make_sandbox_durable_from_remote_snapshot` call so the reupload
    // give-up policy can be asserted in tests.
    adopted_remote_snapshot: StdMutex<Vec<(SandboxId, NodeId, PositiveDuration)>>,
    // Set of sandbox ids `make_sandbox_durable_from_remote_snapshot` should
    // report as not-yet-eligible (returns `0` rows affected) instead of the
    // default `1`, modelling the production CAS missing because the sandbox is
    // still settling, has no ready remote snapshot, races an in-flight upload,
    // or drifted.
    adopt_remote_snapshot_noop: StdMutex<HashSet<SandboxId>>,
    // Number of stuck transient sandboxes the next
    // `abandon_stale_resident_sandboxes` call should report flipped on
    // a given node. Queue-shaped per node so a test can model
    // tick-over-tick behaviour. Pop the front per call; empty queue =
    // return zero.
    abandon_stale_returns: StdMutex<HashMap<NodeId, Vec<u64>>>,
    // Recorded `(node_id, age)` per call so tests can assert the
    // reconciler propagates the configured timeout.
    abandon_stale_calls: StdMutex<Vec<(NodeId, PositiveDuration)>>,
    // One-shot DB-error injection for `abandon_stale_resident_sandboxes`.
    fail_abandon_stale: StdMutex<Option<String>>,

    // Cluster-wide queue of "restore pending" listings: sandboxes suspended
    // mid-drain-evacuation whose snapshot has finalized `ready` and are ready
    // for the cohort restore sweep. Pops the front each call so a test can
    // model "still uploading" (empty) for some ticks, then "ready" (the
    // sandbox) on a later tick. Node-agnostic — the restore is decoupled from
    // any node's drain.
    restore_pending_listings: StdMutex<Vec<Vec<(SandboxId, SandboxLifecycle)>>>,
    // Records `(sandbox_id, target_lifecycle, placement)` per
    // `restore_evacuated_sandbox` call so tests can assert the re-drive
    // restored each sandbox in its original lifecycle onto the cohort.
    restored_evacuations: StdMutex<Vec<(SandboxId, SandboxLifecycle, Option<EvacuationPlacement>)>>,
    // Per-sandbox outcome injection for `restore_evacuated_sandbox`: the
    // next call for the sandbox returns the stored result instead of
    // `Ok(Restored)`. One-shot per insertion, so a later sweep's re-drive
    // succeeds. Covers both failures (`Err`) and non-`Restored` outcomes
    // like `Converging`.
    restore_results: StdMutex<HashMap<SandboxId, Result<RestoreOutcome, EvacuateError>>>,
    // None = no gating (instant return). Some(sem) = each
    // `restore_evacuated_sandbox` call acquires a permit, so a test can
    // hold a restore in flight and assert it blocks the drained transition.
    restore_permits: StdMutex<Option<Arc<tokio::sync::Semaphore>>>,
    // Sandboxes whose `restore_evacuated_sandbox` call should return
    // `RestoreOutcome::Deferred` (user takeover / not-yet-eligible) instead
    // of `Restored`, so tests can assert deferral is not treated as failure.
    restore_deferred: StdMutex<HashSet<SandboxId>>,
    // Recorded `(sandbox_id, backoff_base, backoff_cap)` per
    // `schedule_restore_retry` call so the failure-backoff policy can be
    // asserted in tests.
    restore_retry_calls: StdMutex<Vec<(SandboxId, PositiveDuration, PositiveDuration)>>,
    // `honor_backoff_windows` recorded per `list_restore_pending_evacuations`
    // call, so tests can assert the reconciler bypasses stale backoff windows
    // exactly when the backoff is disabled.
    restore_pending_honor_flags: StdMutex<Vec<bool>>,

    // Cluster-wide queue of "stranded suspending re-drive" results: the ids a
    // `redrive_stranded_suspending_evacuations` call should report advancing
    // from `suspending` to `suspended`. Pops the front each call so a test can
    // model "nothing ready" (empty) then a recovered row on a later sweep.
    redrive_stranded_returns: StdMutex<Vec<Vec<SandboxId>>>,
    // Count of `redrive_stranded_suspending_evacuations` calls, so a test can
    // assert the re-drive runs on every restore sweep.
    redrive_stranded_calls: StdMutex<usize>,
}

impl FakeSink {
    fn new() -> Self {
        Self::default()
    }

    fn set_initial_state(&self, node: NodeId, state: &str) {
        self.node_state
            .lock()
            .unwrap()
            .insert(node, state.to_string());
    }

    /// Remove the node from the FakeSink's phase map, modeling
    /// `drain_phase IS NULL` (admin :undrain, never-drained, etc.).
    fn clear_initial_state(&self, node: NodeId) {
        self.node_state.lock().unwrap().remove(&node);
    }

    fn set_draining_list(&self, nodes: Vec<NodeId>) {
        *self.draining_list.lock().unwrap() = nodes;
    }

    /// List nodes as draining **in forced mode** — the evacuating path. Most of
    /// this suite predates drain modes and exercises evacuation, so it says so
    /// once here rather than pairing every listing with a `set_forced`.
    fn set_draining_list_forced(&self, nodes: Vec<NodeId>) {
        for node in &nodes {
            self.set_forced(*node);
        }
        self.set_draining_list(nodes);
    }

    /// Mark a listed draining node as forced — the evacuating mode. Nodes
    /// default to gentle, matching the production column read, so the
    /// evacuation tests state their mode explicitly.
    fn set_forced(&self, node: NodeId) {
        self.draining_modes
            .lock()
            .unwrap()
            .insert(node, NodeDrainMode::Forced);
    }

    fn set_gentle(&self, node: NodeId) {
        self.draining_modes
            .lock()
            .unwrap()
            .insert(node, NodeDrainMode::Gentle);
    }

    fn mode_of(&self, node: NodeId) -> NodeDrainMode {
        self.draining_modes
            .lock()
            .unwrap()
            .get(&node)
            .copied()
            .unwrap_or(NodeDrainMode::Gentle)
    }

    fn pinned_legacy_exclusions(&self) -> Vec<(NodeId, bool)> {
        self.pinned_legacy_exclusions.lock().unwrap().clone()
    }

    fn set_terminating_list(&self, nodes: Vec<NodeId>) {
        *self.terminating_list.lock().unwrap() = nodes;
    }

    fn enqueue_resident(&self, node: NodeId, ids: Vec<SandboxId>) {
        self.resident_listings
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .push(ids);
    }

    /// Make `list_pinned_local_only_sandboxes_on_node` fail for `node` on
    /// every tick, as a persistent `statement_timeout` would.
    fn fail_pinned_listing(&self, node: NodeId) {
        self.pinned_listing_failures.lock().unwrap().insert(node);
    }

    /// Make `count_drain_blocking_sandboxes_on_node` fail for `node` on every
    /// tick. Separate from [`Self::fail_pinned_listing`] because the two sit on
    /// opposite sides of the promotion step, and each has to be shown
    /// individually not to stop the evacuation drive.
    fn fail_blocker_count(&self, node: NodeId) {
        self.blocker_count_failures.lock().unwrap().insert(node);
    }

    fn enqueue_evacuatable(&self, node: NodeId, ids: Vec<SandboxId>) {
        // Convenience wrapper: every sandbox in the list defaults to
        // Started, matching the pre-paused-evacuation default the
        // existing tests rely on. Tests covering paused evacuation use
        // `enqueue_evacuatable_typed`.
        self.enqueue_evacuatable_typed(
            node,
            ids.into_iter()
                .map(|id| (id, SandboxLifecycle::Started))
                .collect(),
        );
    }

    fn enqueue_evacuatable_typed(&self, node: NodeId, entries: Vec<(SandboxId, SandboxLifecycle)>) {
        self.evacuatable_listings
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .push(entries);
    }

    fn inject_fail_list_draining(&self, msg: &str) {
        *self.fail_list_draining.lock().unwrap() = Some(msg.into());
    }
    fn inject_fail_list_terminating(&self, msg: &str) {
        *self.fail_list_terminating.lock().unwrap() = Some(msg.into());
    }
    fn inject_fail_list_resident(&self, msg: &str) {
        *self.fail_list_resident.lock().unwrap() = Some(msg.into());
    }
    fn inject_fail_termination_blockers(&self, msg: &str) {
        *self.fail_termination_blockers.lock().unwrap() = Some(msg.into());
    }
    fn inject_fail_list_evacuatable(&self, msg: &str) {
        *self.fail_list_evacuatable.lock().unwrap() = Some(msg.into());
    }
    fn inject_fail_set_state(&self, msg: &str) {
        *self.fail_set_state.lock().unwrap() = Some(msg.into());
    }

    fn fail_evacuate(&self, sandbox: SandboxId, err: EvacuateError) {
        self.evacuate_failures.lock().unwrap().insert(sandbox, err);
    }

    /// `(sandbox_id, source_node_id)` pairs recorded for every
    /// `mark_sandbox_failed_on_node` call.
    fn marked_failed_on_node(&self) -> Vec<(SandboxId, NodeId)> {
        self.marked_failed_on_node.lock().unwrap().clone()
    }

    /// Make `mark_sandbox_failed_on_node` report `sandbox` as drifted
    /// (0 rows flipped), modelling the production CAS missing.
    fn drift_mark_failed_on_node(&self, sandbox: SandboxId) {
        self.mark_failed_on_node_drift
            .lock()
            .unwrap()
            .insert(sandbox);
    }

    /// `(sandbox_id, source_node_id, settle)` recorded for every
    /// `make_sandbox_durable_from_remote_snapshot` call.
    fn adopted_remote_snapshot(&self) -> Vec<(SandboxId, NodeId, PositiveDuration)> {
        self.adopted_remote_snapshot.lock().unwrap().clone()
    }

    /// Make `make_sandbox_durable_from_remote_snapshot` report `sandbox` as
    /// not-yet-eligible (0 rows flipped), modelling the production CAS missing
    /// (still settling, no ready remote snapshot, in-flight upload, drifted).
    fn noop_adopt_remote_snapshot(&self, sandbox: SandboxId) {
        self.adopt_remote_snapshot_noop
            .lock()
            .unwrap()
            .insert(sandbox);
    }

    fn gate_evacuations(&self) {
        *self.evacuation_permits.lock().unwrap() = Some(Arc::new(tokio::sync::Semaphore::new(0)));
    }

    fn release_evacuation_permits(&self, n: usize) {
        if let Some(sem) = self.evacuation_permits.lock().unwrap().as_ref() {
            sem.add_permits(n);
        }
    }

    fn updates(&self) -> Vec<StateUpdate> {
        self.state_updates.lock().unwrap().clone()
    }

    fn evacuated_ids(&self) -> Vec<SandboxId> {
        self.evacuated
            .lock()
            .unwrap()
            .iter()
            .map(|(s, _, _)| *s)
            .collect()
    }

    fn evacuation_pre_lifecycle(&self, sandbox: SandboxId) -> Option<SandboxLifecycle> {
        self.evacuated
            .lock()
            .unwrap()
            .iter()
            .find_map(|(s, _, lc)| if *s == sandbox { Some(*lc) } else { None })
    }

    /// Placement the reconciler propagated to the cohort restore for
    /// `sandbox` (the restore sweep is where placement flows now). Returns
    /// the recorded `Option<EvacuationPlacement>`, or `None` if no restore
    /// was recorded for it.
    fn restore_placement_for(&self, sandbox: SandboxId) -> Option<Option<EvacuationPlacement>> {
        self.restored_evacuations
            .lock()
            .unwrap()
            .iter()
            .find_map(|(s, _, p)| if *s == sandbox { Some(p.clone()) } else { None })
    }

    fn state_counts_call_count(&self) -> usize {
        *self.state_counts_calls.lock().unwrap()
    }

    /// Enqueue one restore-sweep's worth of stranded-`suspending` re-drive
    /// results — the ids advanced to `suspended`. Consumed front-first per
    /// `redrive_stranded_suspending_evacuations` call.
    fn enqueue_redrive_stranded(&self, ids: Vec<SandboxId>) {
        self.redrive_stranded_returns.lock().unwrap().push(ids);
    }

    fn redrive_stranded_call_count(&self) -> usize {
        *self.redrive_stranded_calls.lock().unwrap()
    }

    fn current_state(&self, node: NodeId) -> Option<String> {
        self.node_state.lock().unwrap().get(&node).cloned()
    }

    fn cordon_calls(&self) -> Vec<NodeId> {
        self.cordon_calls.lock().unwrap().clone()
    }

    fn fail_cordon(&self, node: NodeId, msg: &str) {
        self.cordon_failures
            .lock()
            .unwrap()
            .insert(node, CordonError::Substrate(msg.into()));
    }

    fn fail_cordon_stale_identity(&self, node: NodeId) {
        self.cordon_failures
            .lock()
            .unwrap()
            .insert(node, CordonError::StaleIdentity);
    }

    /// Make `ensure_node_unschedulable` report the kube Node gone for
    /// `node` (`Ok(CordonOutcome::KubeNodeGone)`).
    fn set_cordon_kube_node_gone(&self, node: NodeId) {
        self.cordon_gone.lock().unwrap().insert(node);
    }

    /// `(node_id, expected_uid)` recorded for every
    /// `fail_resident_sandboxes_on_gone_node` call.
    fn failed_on_gone_node_calls(&self) -> Vec<(NodeId, Option<String>)> {
        self.failed_on_gone_node_calls.lock().unwrap().clone()
    }

    /// Enqueue the flipped-row count the next
    /// `fail_resident_sandboxes_on_gone_node` call reports for `node`.
    fn enqueue_fail_on_gone_node(&self, node: NodeId, count: u64) {
        self.fail_on_gone_node_returns
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .push(count);
    }

    /// `(sandbox_id, source_node_id, expected_uid)` recorded for every
    /// `mark_sandbox_lost_on_gone_node` call.
    fn marked_lost_on_gone_node(&self) -> Vec<(SandboxId, NodeId, Option<String>)> {
        self.marked_lost_on_gone_node.lock().unwrap().clone()
    }

    /// `(sandbox_id, source_node_id, settle, expected_uid)` recorded for
    /// every `make_sandbox_durable_on_gone_node` call.
    fn adopted_on_gone_node(&self) -> Vec<(SandboxId, NodeId, PositiveDuration, Option<String>)> {
        self.adopted_on_gone_node.lock().unwrap().clone()
    }

    /// `(node_id, expected_uid)` recorded for every
    /// `fail_inflight_snapshots_on_gone_node` call.
    fn failed_inflight_snapshots_calls(&self) -> Vec<(NodeId, Option<String>)> {
        self.failed_inflight_snapshots_calls.lock().unwrap().clone()
    }

    /// Enqueue one tick's worth of local-only suspended sandbox IDs
    /// for `node`.
    fn enqueue_pinned_local_only(&self, node: NodeId, entries: Vec<SandboxId>) {
        self.pinned_listings
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .push(entries);
    }

    /// Enqueue one tick's worth of "extra" blocker count for `node`
    /// (sandboxes in `suspending`/`restoring` that pin the node but
    /// aren't promotable). Returned by
    /// `count_drain_blocking_sandboxes_on_node` on top of the
    /// promotable-list size.
    fn enqueue_extra_blocker_count(&self, node: NodeId, count: i64) {
        self.extra_blocker_counts
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .push(count);
    }

    /// Enqueue one tick's worth of extra termination-blocking residents for
    /// `node` — the `(lifecycle, count)` pairs the terminator's guard counts
    /// beyond whatever the resident scan reported.
    fn enqueue_termination_blockers(&self, node: NodeId, entries: Vec<(&str, u64)>) {
        self.extra_termination_blockers
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .push(
                entries
                    .into_iter()
                    .map(|(lifecycle, count)| (lifecycle.to_string(), count))
                    .collect(),
            );
    }

    fn termination_blocking_calls(&self) -> usize {
        *self.termination_blocking_calls.lock().unwrap()
    }

    fn fail_promote(&self, sandbox: SandboxId, err: PromoteError) {
        self.promote_failures.lock().unwrap().insert(sandbox, err);
    }

    fn promoted(&self) -> Vec<(SandboxId, NodeId)> {
        self.promoted.lock().unwrap().clone()
    }

    fn marked_lost(&self) -> Vec<(SandboxId, NodeId)> {
        self.marked_lost.lock().unwrap().clone()
    }

    fn enqueue_abandon_stale(&self, node: NodeId, count: u64) {
        self.abandon_stale_returns
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .push(count);
    }

    fn abandon_stale_calls(&self) -> Vec<(NodeId, PositiveDuration)> {
        self.abandon_stale_calls.lock().unwrap().clone()
    }

    fn inject_fail_abandon_stale(&self, msg: &str) {
        *self.fail_abandon_stale.lock().unwrap() = Some(msg.into());
    }

    /// Enqueue one tick's worth of cluster-wide restore-pending `(sandbox,
    /// target)` entries. Push an empty vec to model a tick where the snapshot
    /// upload is still in flight (nothing ready to restore yet).
    fn enqueue_restore_pending(&self, entries: Vec<(SandboxId, SandboxLifecycle)>) {
        self.restore_pending_listings.lock().unwrap().push(entries);
    }

    fn restored_evacuations(
        &self,
    ) -> Vec<(SandboxId, SandboxLifecycle, Option<EvacuationPlacement>)> {
        self.restored_evacuations.lock().unwrap().clone()
    }

    fn fail_restore(&self, sandbox: SandboxId, err: EvacuateError) {
        self.restore_results
            .lock()
            .unwrap()
            .insert(sandbox, Err(err));
    }

    fn converge_restore(&self, sandbox: SandboxId) {
        self.restore_results.lock().unwrap().insert(
            sandbox,
            Ok(RestoreOutcome::Converging {
                detail: "guest never reported command-ready within the wait".to_string(),
                wait_phase: Some(WaitReadyTimeoutPhase::GuestNotReady),
            }),
        );
    }

    /// Make `restore_evacuated_sandbox` report `sandbox` as
    /// `RestoreOutcome::Deferred` (a concurrent user takeover cleared the
    /// evacuation marker / not yet eligible) instead of `Restored`.
    fn defer_restore(&self, sandbox: SandboxId) {
        self.restore_deferred.lock().unwrap().insert(sandbox);
    }

    /// `(sandbox_id, backoff_base, backoff_cap)` recorded for every
    /// `schedule_restore_retry` call.
    fn restore_retry_calls(&self) -> Vec<(SandboxId, PositiveDuration, PositiveDuration)> {
        self.restore_retry_calls.lock().unwrap().clone()
    }

    /// `honor_backoff_windows` values recorded per
    /// `list_restore_pending_evacuations` call.
    fn restore_pending_honor_flags(&self) -> Vec<bool> {
        self.restore_pending_honor_flags.lock().unwrap().clone()
    }

    fn gate_restores(&self) {
        *self.restore_permits.lock().unwrap() = Some(Arc::new(tokio::sync::Semaphore::new(0)));
    }

    fn release_restore_permits(&self, n: usize) {
        if let Some(sem) = self.restore_permits.lock().unwrap().as_ref() {
            sem.add_permits(n);
        }
    }
}

/// One tick's worth of `(lifecycle, count)` pairs, as
/// `TerminationBlockingResidents::by_lifecycle` carries them.
type LifecycleBreakdown = Vec<(String, u64)>;

fn forced_db_err(msg: &str) -> sqlx::Error {
    sqlx::Error::Io(std::io::Error::other(msg))
}

impl NodeDrainSink for FakeSink {
    async fn list_draining_nodes(&self) -> Result<Vec<DrainingNode>, sqlx::Error> {
        if let Some(msg) = self.fail_list_draining.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        let ids = self.draining_list.lock().unwrap().clone();
        Ok(ids
            .into_iter()
            .map(|id| DrainingNode {
                id,
                mode: self.mode_of(id),
            })
            .collect())
    }

    async fn list_terminating_nodes(&self) -> Result<Vec<NodeId>, sqlx::Error> {
        if let Some(msg) = self.fail_list_terminating.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        Ok(self.terminating_list.lock().unwrap().clone())
    }

    /// The fake keeps queueing ids (they seed the evacuatable default via
    /// `last_resident`) and reports their count. Every id it is handed counts:
    /// the census discount is SQL the production sink builds, covered against
    /// Postgres in `store::residency_tests`.
    async fn count_resident_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> Result<ResidentSandboxCounts, sqlx::Error> {
        if let Some(msg) = self.fail_list_resident.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        let mut guard = self.resident_listings.lock().unwrap();
        let entry = guard.entry(node_id).or_default();
        let result = if entry.is_empty() {
            Vec::new()
        } else {
            entry.remove(0)
        };
        drop(guard);
        let occupying = result.len() as u64;
        self.last_resident.lock().unwrap().insert(node_id, result);
        Ok(ResidentSandboxCounts {
            occupying,
            discounted: 0,
        })
    }

    async fn list_evacuatable_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error> {
        if let Some(msg) = self.fail_list_evacuatable.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        let mut g = self.evacuatable_listings.lock().unwrap();
        if let Some(q) = g.get_mut(&node_id)
            && !q.is_empty()
        {
            return Ok(q.remove(0));
        }
        drop(g);
        Ok(self
            .last_resident
            .lock()
            .unwrap()
            .get(&node_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|id| (id, SandboxLifecycle::Started))
            .collect())
    }

    async fn list_pinned_local_only_sandboxes_on_node(
        &self,
        node_id: NodeId,
        exclude_legacy_rows: bool,
    ) -> Result<Vec<SandboxId>, sqlx::Error> {
        self.pinned_legacy_exclusions
            .lock()
            .unwrap()
            .push((node_id, exclude_legacy_rows));
        if self
            .pinned_listing_failures
            .lock()
            .unwrap()
            .contains(&node_id)
        {
            return Err(sqlx::Error::PoolTimedOut);
        }
        let mut guard = self.pinned_listings.lock().unwrap();
        let entry = guard.entry(node_id).or_default();
        let result = if entry.is_empty() {
            Vec::new()
        } else {
            entry.remove(0)
        };
        drop(guard);
        self.last_pinned_listing_size
            .lock()
            .unwrap()
            .insert(node_id, result.len());
        Ok(result)
    }

    async fn count_drain_blocking_sandboxes_on_node(
        &self,
        node_id: NodeId,
    ) -> Result<i64, sqlx::Error> {
        if self
            .blocker_count_failures
            .lock()
            .unwrap()
            .contains(&node_id)
        {
            return Err(sqlx::Error::PoolTimedOut);
        }
        // Reflect the production query: promotable list (already returned
        // by `list_pinned_local_only_sandboxes_on_node`) plus extra
        // blockers (suspending/restoring) the test enqueues separately.
        let pinned = self
            .last_pinned_listing_size
            .lock()
            .unwrap()
            .get(&node_id)
            .copied()
            .unwrap_or(0) as i64;
        let mut guard = self.extra_blocker_counts.lock().unwrap();
        let entry = guard.entry(node_id).or_default();
        let extra = if entry.is_empty() { 0 } else { entry.remove(0) };
        Ok(pinned + extra)
    }

    /// The terminator's guard is an exclusion list, so it counts a superset of
    /// the resident include-list: every row the resident scan reported is echoed
    /// here (as `started`, the lifecycle the fake's evacuation default already
    /// assumes) on top of whatever divergent-lifecycle rows the test enqueued.
    /// No current assertion can observe the echo — the gate short-circuits on
    /// `resident.occupying` first — but reporting zero there would let a test
    /// assert a state production cannot reach.
    async fn count_termination_blocking_residents_on_node(
        &self,
        node_id: NodeId,
    ) -> Result<TerminationBlockingResidents, sqlx::Error> {
        *self.termination_blocking_calls.lock().unwrap() += 1;
        if let Some(msg) = self.fail_termination_blockers.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        let resident = self
            .last_resident
            .lock()
            .unwrap()
            .get(&node_id)
            .map(Vec::len)
            .unwrap_or(0) as u64;
        let mut guard = self.extra_termination_blockers.lock().unwrap();
        let entry = guard.entry(node_id).or_default();
        let extra = if entry.is_empty() {
            Vec::new()
        } else {
            entry.remove(0)
        };
        drop(guard);
        let mut by_lifecycle: Vec<(String, u64)> = extra;
        if resident > 0 {
            by_lifecycle.push(("started".to_string(), resident));
        }
        by_lifecycle.sort();
        Ok(TerminationBlockingResidents { by_lifecycle })
    }

    async fn promote_local_snapshot(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<(), PromoteError> {
        self.promoted
            .lock()
            .unwrap()
            .push((sandbox_id, source_node_id));
        if let Some(err) = self.promote_failures.lock().unwrap().remove(&sandbox_id) {
            return Err(err);
        }
        Ok(())
    }

    async fn mark_sandbox_lost(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<u64, sqlx::Error> {
        self.marked_lost
            .lock()
            .unwrap()
            .push((sandbox_id, source_node_id));
        Ok(1)
    }

    async fn mark_sandbox_failed_on_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<u64, sqlx::Error> {
        self.marked_failed_on_node
            .lock()
            .unwrap()
            .push((sandbox_id, source_node_id));
        // Model the production CAS: a drifted sandbox reports 0 rows flipped.
        if self
            .mark_failed_on_node_drift
            .lock()
            .unwrap()
            .contains(&sandbox_id)
        {
            return Ok(0);
        }
        Ok(1)
    }

    async fn make_sandbox_durable_from_remote_snapshot(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        settle: PositiveDuration,
    ) -> Result<u64, sqlx::Error> {
        self.adopted_remote_snapshot
            .lock()
            .unwrap()
            .push((sandbox_id, source_node_id, settle));
        // Model the production CAS: a sandbox that is still settling, has no
        // ready remote snapshot, races an in-flight upload, or drifted reports
        // 0 rows flipped.
        if self
            .adopt_remote_snapshot_noop
            .lock()
            .unwrap()
            .contains(&sandbox_id)
        {
            return Ok(0);
        }
        Ok(1)
    }

    async fn abandon_stale_resident_sandboxes(
        &self,
        node_id: NodeId,
        age: PositiveDuration,
    ) -> Result<u64, sqlx::Error> {
        self.abandon_stale_calls
            .lock()
            .unwrap()
            .push((node_id, age));
        if let Some(msg) = self.fail_abandon_stale.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        // Mirror the production SQL guard: if the node has drifted out
        // of `drain_phase = 'draining'` between `list_draining_nodes()`
        // returning it and this UPDATE landing, the SQL `EXISTS` clause
        // matches no rows. Modeled here by short-circuiting to zero
        // when the FakeSink's node_state map doesn't currently hold
        // PHASE_DRAINING for this node.
        let current_phase = self.node_state.lock().unwrap().get(&node_id).cloned();
        if current_phase.as_deref() != Some(PHASE_DRAINING) {
            return Ok(0);
        }
        let mut guard = self.abandon_stale_returns.lock().unwrap();
        let entry = guard.entry(node_id).or_default();
        Ok(if entry.is_empty() { 0 } else { entry.remove(0) })
    }

    async fn evacuate_sandbox(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        pre_lifecycle: SandboxLifecycle,
    ) -> Result<(), EvacuateError> {
        let permit_holder = self.evacuation_permits.lock().unwrap().clone();
        if let Some(sem) = permit_holder {
            let permit = sem.acquire().await.unwrap();
            permit.forget();
        }
        self.evacuated
            .lock()
            .unwrap()
            .push((sandbox_id, source_node_id, pre_lifecycle));
        if let Some(err) = self.evacuate_failures.lock().unwrap().remove(&sandbox_id) {
            return Err(err);
        }
        Ok(())
    }

    async fn list_restore_pending_evacuations(
        &self,
        honor_backoff_windows: bool,
    ) -> Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error> {
        self.restore_pending_honor_flags
            .lock()
            .unwrap()
            .push(honor_backoff_windows);
        let mut guard = self.restore_pending_listings.lock().unwrap();
        Ok(if guard.is_empty() {
            Vec::new()
        } else {
            guard.remove(0)
        })
    }

    async fn schedule_restore_retry(
        &self,
        sandbox_id: SandboxId,
        backoff_base: PositiveDuration,
        backoff_cap: PositiveDuration,
    ) -> Result<Option<i32>, sqlx::Error> {
        let mut calls = self.restore_retry_calls.lock().unwrap();
        calls.push((sandbox_id, backoff_base, backoff_cap));
        // Mirror the production RETURNING clause: the post-increment
        // consecutive-failure count equals the calls recorded for this
        // sandbox.
        let count = calls.iter().filter(|(id, _, _)| *id == sandbox_id).count() as i32;
        Ok(Some(count))
    }

    async fn redrive_stranded_suspending_evacuations(&self) -> Result<Vec<SandboxId>, sqlx::Error> {
        *self.redrive_stranded_calls.lock().unwrap() += 1;
        let mut guard = self.redrive_stranded_returns.lock().unwrap();
        Ok(if guard.is_empty() {
            Vec::new()
        } else {
            guard.remove(0)
        })
    }

    async fn restore_evacuated_sandbox(
        &self,
        sandbox_id: SandboxId,
        target_lifecycle: SandboxLifecycle,
        _evacuate_timeout: PositiveDuration,
        placement: Option<EvacuationPlacement>,
    ) -> Result<RestoreOutcome, EvacuateError> {
        let permit_holder = self.restore_permits.lock().unwrap().clone();
        if let Some(sem) = permit_holder {
            let permit = sem.acquire().await.unwrap();
            permit.forget();
        }
        self.restored_evacuations
            .lock()
            .unwrap()
            .push((sandbox_id, target_lifecycle, placement));
        if let Some(result) = self.restore_results.lock().unwrap().remove(&sandbox_id) {
            return result;
        }
        if self.restore_deferred.lock().unwrap().contains(&sandbox_id) {
            return Ok(RestoreOutcome::Deferred);
        }
        Ok(RestoreOutcome::Restored)
    }

    async fn set_drain_phase(
        &self,
        node_id: NodeId,
        expected_phase: &str,
        new_phase: &str,
        last_error: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        if let Some(msg) = self.fail_set_state.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        self.state_updates.lock().unwrap().push(StateUpdate {
            node_id,
            expected: expected_phase.to_string(),
            new_state: new_phase.to_string(),
            last_error: last_error.map(String::from),
        });
        let mut states = self.node_state.lock().unwrap();
        match states.get(&node_id) {
            Some(s) if s == expected_phase => {
                states.insert(node_id, new_phase.to_string());
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn delete_terminating_node(&self, node_id: NodeId) -> Result<bool, sqlx::Error> {
        if let Some(msg) = self.fail_set_state.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        // Recorded as a synthetic "delete" state-update so tests
        // observing `updates()` can find the terminate-success
        // transition. The actual row removal is modeled by purging
        // the node from `node_state`.
        self.state_updates.lock().unwrap().push(StateUpdate {
            node_id,
            expected: PHASE_TERMINATING.to_string(),
            new_state: "deleted".to_string(),
            last_error: None,
        });
        let mut states = self.node_state.lock().unwrap();
        match states.get(&node_id) {
            Some(p) if p == PHASE_TERMINATING => {
                states.remove(&node_id);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn node_phase_counts(&self) -> Result<HashMap<String, u64>, sqlx::Error> {
        *self.state_counts_calls.lock().unwrap() += 1;
        if let Some(msg) = self.fail_state_counts.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        let mut counts: HashMap<String, u64> = HashMap::new();
        for state in self.node_state.lock().unwrap().values() {
            *counts.entry(state.clone()).or_insert(0) += 1;
        }
        Ok(counts)
    }

    async fn ensure_node_unschedulable(
        &self,
        node_id: NodeId,
    ) -> Result<CordonOutcome, CordonError> {
        self.cordon_calls.lock().unwrap().push(node_id);
        if let Some(err) = self.cordon_failures.lock().unwrap().remove(&node_id) {
            return Err(err);
        }
        if self.cordon_gone.lock().unwrap().contains(&node_id) {
            return Ok(CordonOutcome::KubeNodeGone);
        }
        Ok(CordonOutcome::Cordoned)
    }

    async fn fail_resident_sandboxes_on_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        self.failed_on_gone_node_calls
            .lock()
            .unwrap()
            .push((node_id, expected_uid.map(String::from)));
        // Mirror the production SQL guard: the sweep refuses once the node
        // has drifted out of `drain_phase = 'draining'`.
        let current_phase = self.node_state.lock().unwrap().get(&node_id).cloned();
        if current_phase.as_deref() != Some(PHASE_DRAINING) {
            return Ok(0);
        }
        let mut guard = self.fail_on_gone_node_returns.lock().unwrap();
        let entry = guard.entry(node_id).or_default();
        Ok(if entry.is_empty() { 0 } else { entry.remove(0) })
    }

    async fn mark_sandbox_lost_on_gone_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        self.marked_lost_on_gone_node.lock().unwrap().push((
            sandbox_id,
            source_node_id,
            expected_uid.map(String::from),
        ));
        Ok(1)
    }

    async fn fail_inflight_snapshots_on_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        self.failed_inflight_snapshots_calls
            .lock()
            .unwrap()
            .push((node_id, expected_uid.map(String::from)));
        Ok(0)
    }

    async fn make_sandbox_durable_on_gone_node(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        settle: PositiveDuration,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        self.adopted_on_gone_node.lock().unwrap().push((
            sandbox_id,
            source_node_id,
            settle,
            expected_uid.map(String::from),
        ));
        // Shared 0-rows modeling with the reupload-path adopt: still
        // settling, no current-cycle ready remote snapshot, in-flight
        // upload, identity refusal, or drift.
        if self
            .adopt_remote_snapshot_noop
            .lock()
            .unwrap()
            .contains(&sandbox_id)
        {
            return Ok(0);
        }
        Ok(1)
    }

    async fn wait_for_restore_signal(&self) {
        // No event signal in tests — they drive `tick()` directly. Never
        // resolves so the `run()` `select!` only fires on the interval tick.
        std::future::pending::<()>().await
    }
}

#[derive(Default)]
struct FakeTerminator {
    // node_id -> result for the next terminate() call on that node.
    outcomes: StdMutex<HashMap<NodeId, Result<(), TerminateError>>>,
    // Recorded call order.
    calls: StdMutex<Vec<NodeId>>,
}

impl FakeTerminator {
    fn fail(&self, node: NodeId, err: TerminateError) {
        self.outcomes.lock().unwrap().insert(node, Err(err));
    }
    fn calls(&self) -> Vec<NodeId> {
        self.calls.lock().unwrap().clone()
    }
}

impl InstanceTerminator for FakeTerminator {
    async fn terminate(&self, node_id: NodeId) -> Result<(), TerminateError> {
        self.calls.lock().unwrap().push(node_id);
        self.outcomes
            .lock()
            .unwrap()
            .remove(&node_id)
            .unwrap_or(Ok(()))
    }
}

/// Test double for the KubeNodeGone recovery's EC2 cross-check
/// ([`InstanceStateLookup`]). Nodes without a configured state (and nodes in
/// `errors`) report a backend error, mirroring the production fail-closed
/// posture where an unanswerable lookup parks the node.
#[derive(Default)]
struct FakeInstanceLookup {
    states: StdMutex<HashMap<NodeId, NodeInstanceState>>,
    errors: StdMutex<HashSet<NodeId>>,
    calls: StdMutex<Vec<NodeId>>,
}

impl FakeInstanceLookup {
    fn set_gone(&self, node: NodeId, expected_uid: Option<&str>) {
        self.states.lock().unwrap().insert(
            node,
            NodeInstanceState {
                outcome: InstanceLookupOutcome::Gone,
                expected_uid: expected_uid.map(String::from),
            },
        );
    }

    fn set_live(&self, node: NodeId) {
        self.states.lock().unwrap().insert(
            node,
            NodeInstanceState {
                outcome: InstanceLookupOutcome::Live {
                    instance_id: "i-0123456789abcdef0".into(),
                    state: "running".into(),
                },
                expected_uid: None,
            },
        );
    }

    fn fail(&self, node: NodeId) {
        self.errors.lock().unwrap().insert(node);
    }

    fn calls(&self) -> Vec<NodeId> {
        self.calls.lock().unwrap().clone()
    }
}

impl InstanceStateLookup for FakeInstanceLookup {
    fn lookup_instance_state(
        &self,
        node_id: NodeId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<NodeInstanceState, InstanceLookupError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.calls.lock().unwrap().push(node_id);
            if self.errors.lock().unwrap().contains(&node_id) {
                return Err(InstanceLookupError::Backend("forced ec2 error".into()));
            }
            self.states
                .lock()
                .unwrap()
                .get(&node_id)
                .cloned()
                .ok_or_else(|| InstanceLookupError::Backend("no state configured".into()))
        })
    }
}

// -----------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------

fn config_enabled(max_per_node: usize, max_per_cluster: usize) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        // On for the whole suite: the tests that pre-date the fourth term are
        // unaffected (their breakdown is empty), and the `tb*` tests assert the
        // term's behaviour, so exercising the enabled path by default is what
        // keeps the flag from hiding a regression. Two tests override it back to
        // false directly: `tb4_gate_off_advances_and_still_measures` and
        // `t2d_pre_terminate_recheck_aborts_with_the_gate_off`.
        termination_blocking_gate: true,
        tick: Duration::from_millis(1),
        max_per_node,
        max_per_cluster,
        evacuate_timeout: PositiveDuration::new_panic(Duration::from_secs(1)),
        // Use the real default here so existing tests exercise the
        // abandon-stuck-transient sweep on the same code path
        // production runs. Tests that need to disable the policy can
        // override on the returned struct.
        stale_abandon: PositiveDuration::new(Duration::from_secs(DEFAULT_STALE_ABANDON_SECS)),
        // Real default so existing tests exercise the reupload give-up on the
        // same settle-gated path production runs; tests that need it disabled
        // override on the returned struct.
        reupload_giveup_settle: PositiveDuration::new(Duration::from_secs(
            DEFAULT_REUPLOAD_GIVEUP_SETTLE_SECS,
        )),
        // Real defaults so failure-path tests exercise the restore retry
        // backoff bookkeeping on the same code path production runs; tests
        // that need it disabled override on the returned struct.
        restore_backoff_base: PositiveDuration::new(Duration::from_secs(
            DEFAULT_RESTORE_BACKOFF_BASE_SECS,
        )),
        restore_backoff_cap: PositiveDuration::new_panic(Duration::from_secs(
            DEFAULT_RESTORE_BACKOFF_CAP_SECS,
        )),
        drain_evacuation_placement: None,
    }
}

async fn wait_in_flight_empty<S: NodeDrainSink, T: InstanceTerminator>(
    reconciler: &Reconciler<S, T>,
) {
    for _ in 0..500 {
        if reconciler.in_flight.lock().await.is_empty() {
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("in_flight did not drain within timeout");
}

/// Wait for the cluster-wide cohort-restore tasks to finish (the restore is
/// decoupled from per-node `in_flight`, tracked in `restore_in_flight`).
async fn wait_restore_in_flight_empty<S: NodeDrainSink, T: InstanceTerminator>(
    reconciler: &Reconciler<S, T>,
) {
    for _ in 0..500 {
        if reconciler.restore_in_flight.lock().await.is_empty() {
            return;
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("restore_in_flight did not drain within timeout");
}

// =================================================================
// C — ReconcilerConfig + env parsing
// =================================================================

#[test]
fn c1_defaults_when_env_unset() {
    let env = pplx_space_test_env::TestEnv::default();
    let cfg = ReconcilerConfig::from_environment(&env);
    // Critical: reconciler is inert by default until operator opts in.
    assert!(!cfg.enabled);
    assert_eq!(cfg.tick, DEFAULT_TICK);
    assert_eq!(cfg.max_per_node, DEFAULT_MAX_PER_NODE);
    assert_eq!(cfg.max_per_cluster, DEFAULT_MAX_PER_CLUSTER);
    assert_eq!(
        cfg.evacuate_timeout.get(),
        Duration::from_secs(DEFAULT_EVACUATE_TIMEOUT_SECS)
    );
    assert_eq!(
        cfg.stale_abandon,
        PositiveDuration::new(Duration::from_secs(DEFAULT_STALE_ABANDON_SECS)),
    );
    assert_eq!(
        cfg.restore_backoff_base,
        PositiveDuration::new(Duration::from_secs(DEFAULT_RESTORE_BACKOFF_BASE_SECS)),
    );
    assert_eq!(
        cfg.restore_backoff_cap.get(),
        Duration::from_secs(DEFAULT_RESTORE_BACKOFF_CAP_SECS),
    );
    assert!(cfg.drain_evacuation_placement.is_none());
}

#[test]
fn c5_drain_evacuation_placement_parsed_from_env() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(
        "SPACE_CLUSTER_CONTROLLER_DRAIN_EVACUATION_COHORT_VALUE",
        "test-2",
    );
    let cfg = ReconcilerConfig::from_environment(&env);
    let placement = cfg
        .drain_evacuation_placement
        .as_ref()
        .expect("placement must be Some when both env vars are set");
    assert_eq!(placement.cohort_key, "space.pplx.net/rollout-cohort");
    assert_eq!(placement.cohort_value, "test-2");

    // Derived toleration: Equal-operator, NoSchedule, matching key+value.
    let tol = placement.to_toleration();
    assert_eq!(tol.key.as_deref(), Some("space.pplx.net/rollout-cohort"));
    assert_eq!(tol.operator.as_deref(), Some("Equal"));
    assert_eq!(tol.value.as_deref(), Some("test-2"));
    assert_eq!(tol.effect.as_deref(), Some("NoSchedule"));
    assert_eq!(tol.toleration_seconds, None);

    // Derived required affinity: same key+value.
    let (affinity_key, affinity_value) = placement.to_required_affinity();
    assert_eq!(affinity_key, "space.pplx.net/rollout-cohort");
    assert_eq!(affinity_value, "test-2");
}

#[test]
fn c6_drain_evacuation_placement_missing_value_yields_none() {
    let mut env = pplx_space_test_env::TestEnv::default();
    // Key set but value empty → no rollout in flight.
    env.set("SPACE_CLUSTER_CONTROLLER_DRAIN_EVACUATION_COHORT_VALUE", "");
    let cfg = ReconcilerConfig::from_environment(&env);
    assert!(cfg.drain_evacuation_placement.is_none());
}

#[test]
fn c7_drain_evacuation_placement_both_unset_yields_none() {
    let env = pplx_space_test_env::TestEnv::default();
    // Neither var set → no rollout in flight.
    let cfg = ReconcilerConfig::from_environment(&env);
    assert!(cfg.drain_evacuation_placement.is_none());
}

#[test]
fn c4_enabled_truthy_parsing() {
    // Direct call to the helper; covers all branches without the
    // overhead of env_snapshot per case.
    for truthy in ["1", "true", "True", "TRUE"] {
        {
            let mut env = pplx_space_test_env::TestEnv::default();
            env.set("__C4_TMP", truthy);
            assert!(
                crate::env_config::EnvConfig(&env).truthy_default("__C4_TMP", false),
                "{truthy}"
            );
        };
    }
    for falsy in ["0", "false", "False", "FALSE"] {
        {
            let mut env = pplx_space_test_env::TestEnv::default();
            env.set("__C4_TMP", falsy);
            assert!(
                !crate::env_config::EnvConfig(&env).truthy_default("__C4_TMP", true),
                "{falsy}"
            );
        };
    }
    // Junk value falls through to the supplied default.
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("__C4_TMP", "maybe");
    assert!(crate::env_config::EnvConfig(&env).truthy_default("__C4_TMP", true));
    assert!(!crate::env_config::EnvConfig(&env).truthy_default("__C4_TMP", false));
}

// =================================================================
// R — run() lifecycle
// =================================================================

// Thin forwarding shim so a shared `Arc<FakeSink>` can be moved into
// `run()` (which takes `S: NodeDrainSink` by value) while a second
// `Arc` clone stays behind for post-run inspection via
// `state_counts_call_count()`.
struct ArcSink(Arc<FakeSink>);
impl NodeDrainSink for ArcSink {
    async fn list_draining_nodes(&self) -> Result<Vec<DrainingNode>, sqlx::Error> {
        self.0.list_draining_nodes().await
    }
    async fn list_terminating_nodes(&self) -> Result<Vec<NodeId>, sqlx::Error> {
        self.0.list_terminating_nodes().await
    }
    async fn count_resident_sandboxes_on_node(
        &self,
        n: NodeId,
    ) -> Result<ResidentSandboxCounts, sqlx::Error> {
        self.0.count_resident_sandboxes_on_node(n).await
    }
    async fn list_evacuatable_sandboxes_on_node(
        &self,
        n: NodeId,
    ) -> Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error> {
        self.0.list_evacuatable_sandboxes_on_node(n).await
    }
    async fn list_pinned_local_only_sandboxes_on_node(
        &self,
        n: NodeId,
        exclude_legacy_rows: bool,
    ) -> Result<Vec<SandboxId>, sqlx::Error> {
        self.0
            .list_pinned_local_only_sandboxes_on_node(n, exclude_legacy_rows)
            .await
    }
    async fn count_drain_blocking_sandboxes_on_node(&self, n: NodeId) -> Result<i64, sqlx::Error> {
        self.0.count_drain_blocking_sandboxes_on_node(n).await
    }
    async fn count_termination_blocking_residents_on_node(
        &self,
        n: NodeId,
    ) -> Result<TerminationBlockingResidents, sqlx::Error> {
        self.0.count_termination_blocking_residents_on_node(n).await
    }
    async fn promote_local_snapshot(
        &self,
        s: SandboxId,
        source: NodeId,
    ) -> Result<(), PromoteError> {
        self.0.promote_local_snapshot(s, source).await
    }
    async fn mark_sandbox_lost(&self, s: SandboxId, source: NodeId) -> Result<u64, sqlx::Error> {
        self.0.mark_sandbox_lost(s, source).await
    }
    async fn mark_sandbox_failed_on_node(
        &self,
        s: SandboxId,
        source: NodeId,
    ) -> Result<u64, sqlx::Error> {
        self.0.mark_sandbox_failed_on_node(s, source).await
    }
    async fn make_sandbox_durable_from_remote_snapshot(
        &self,
        s: SandboxId,
        source: NodeId,
        settle: PositiveDuration,
    ) -> Result<u64, sqlx::Error> {
        self.0
            .make_sandbox_durable_from_remote_snapshot(s, source, settle)
            .await
    }
    async fn abandon_stale_resident_sandboxes(
        &self,
        n: NodeId,
        age: PositiveDuration,
    ) -> Result<u64, sqlx::Error> {
        self.0.abandon_stale_resident_sandboxes(n, age).await
    }
    async fn evacuate_sandbox(
        &self,
        s: SandboxId,
        source: NodeId,
        pre_lifecycle: SandboxLifecycle,
    ) -> Result<(), EvacuateError> {
        self.0.evacuate_sandbox(s, source, pre_lifecycle).await
    }
    async fn list_restore_pending_evacuations(
        &self,
        honor_backoff_windows: bool,
    ) -> Result<Vec<(SandboxId, SandboxLifecycle)>, sqlx::Error> {
        self.0
            .list_restore_pending_evacuations(honor_backoff_windows)
            .await
    }
    async fn schedule_restore_retry(
        &self,
        s: SandboxId,
        base: PositiveDuration,
        cap: PositiveDuration,
    ) -> Result<Option<i32>, sqlx::Error> {
        self.0.schedule_restore_retry(s, base, cap).await
    }
    async fn redrive_stranded_suspending_evacuations(&self) -> Result<Vec<SandboxId>, sqlx::Error> {
        self.0.redrive_stranded_suspending_evacuations().await
    }
    async fn restore_evacuated_sandbox(
        &self,
        s: SandboxId,
        target_lifecycle: SandboxLifecycle,
        t: PositiveDuration,
        placement: Option<EvacuationPlacement>,
    ) -> Result<RestoreOutcome, EvacuateError> {
        self.0
            .restore_evacuated_sandbox(s, target_lifecycle, t, placement)
            .await
    }
    async fn set_drain_phase(
        &self,
        n: NodeId,
        e: &str,
        ns: &str,
        le: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        self.0.set_drain_phase(n, e, ns, le).await
    }
    async fn delete_terminating_node(&self, n: NodeId) -> Result<bool, sqlx::Error> {
        self.0.delete_terminating_node(n).await
    }
    async fn node_phase_counts(&self) -> Result<HashMap<String, u64>, sqlx::Error> {
        self.0.node_phase_counts().await
    }
    async fn ensure_node_unschedulable(&self, n: NodeId) -> Result<CordonOutcome, CordonError> {
        self.0.ensure_node_unschedulable(n).await
    }
    async fn fail_resident_sandboxes_on_gone_node(
        &self,
        n: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        self.0
            .fail_resident_sandboxes_on_gone_node(n, expected_uid)
            .await
    }
    async fn mark_sandbox_lost_on_gone_node(
        &self,
        s: SandboxId,
        source: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        self.0
            .mark_sandbox_lost_on_gone_node(s, source, expected_uid)
            .await
    }
    async fn make_sandbox_durable_on_gone_node(
        &self,
        s: SandboxId,
        source: NodeId,
        settle: PositiveDuration,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        self.0
            .make_sandbox_durable_on_gone_node(s, source, settle, expected_uid)
            .await
    }
    async fn fail_inflight_snapshots_on_gone_node(
        &self,
        n: NodeId,
        expected_uid: Option<&str>,
    ) -> Result<u64, sqlx::Error> {
        self.0
            .fail_inflight_snapshots_on_gone_node(n, expected_uid)
            .await
    }
    async fn wait_for_restore_signal(&self) {
        self.0.wait_for_restore_signal().await
    }
}

#[tokio::test]
async fn r1_enabled_false_returns_on_shutdown_without_ticking() {
    let cfg = ReconcilerConfig::default();
    assert!(!cfg.enabled);
    let token = CancellationToken::new();
    let token_clone = token.clone();
    // Sink we can inspect post-run to prove no tick occurred.
    let sink = Arc::new(FakeSink::new());
    let sink_view = sink.clone();
    let handle = tokio::spawn(async move {
        run(
            cfg,
            ArcSink(sink),
            FakeTerminator::default(),
            None,
            token_clone,
        )
        .await
    });
    // Give the loop time to start (it shouldn't tick).
    tokio::time::sleep(Duration::from_millis(20)).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("run() did not exit after shutdown")
        .expect("run() panicked");
    assert_eq!(
        sink_view.state_counts_call_count(),
        0,
        "disabled reconciler must never call node_phase_counts (never ticks)"
    );
}

#[tokio::test]
async fn r2_enabled_true_ticks_until_shutdown() {
    let cfg = ReconcilerConfig {
        enabled: true,
        tick: Duration::from_millis(2),
        ..ReconcilerConfig::default()
    };
    // Sink we can inspect post-run. Keep a clone of the Arc inside
    // run() so we can observe state_counts_calls after shutdown.
    let sink = Arc::new(FakeSink::new());
    let sink_view = sink.clone();
    let token = CancellationToken::new();
    let token_clone = token.clone();

    let handle = tokio::spawn(async move {
        run(
            cfg,
            ArcSink(sink),
            FakeTerminator::default(),
            None,
            token_clone,
        )
        .await
    });
    // Let multiple ticks fire.
    tokio::time::sleep(Duration::from_millis(40)).await;
    token.cancel();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("run() did not exit after shutdown")
        .expect("run() panicked");
    assert!(
        sink_view.state_counts_call_count() >= 2,
        "enabled=true should tick at least twice; got {}",
        sink_view.state_counts_call_count()
    );
}

#[tokio::test]
async fn r3_drain_evacuations_awaits_in_flight_tasks_on_shutdown() {
    // The evacuation JoinSet must be drained on shutdown so a task
    // that's past the CAS-to-'suspending' lands its post-suspend
    // / post-restore UPDATEs before runtime drop aborts it.
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.gate_evacuations();

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;
    assert_eq!(report.spawned_evacuations, 1);

    // Drain with the gate still closed → JoinSet drain must time out.
    let timeout = Duration::from_millis(50);
    reconciler.drain_evacuations(timeout).await;
    // Evacuation is still in-flight (the permit was never granted).
    assert_eq!(reconciler.sink.evacuated_ids().len(), 0);

    // Release the permit, run the drain again — now it completes.
    reconciler.sink.release_evacuation_permits(1);
    reconciler.drain_evacuations(Duration::from_secs(2)).await;
    assert_eq!(reconciler.sink.evacuated_ids(), vec![sandbox]);
    assert!(reconciler.in_flight.lock().await.is_empty());
}

// =================================================================
// M — Metrics emission
// =================================================================

#[tokio::test]
async fn m1_tick_queries_node_phase_counts_each_tick() {
    // The reconciler must query node_phase_counts every tick so the
    // nodes_by_state{state} gauge emits a fresh value for every
    // known state (including zeros). We can't intercept the metric
    // sink in a unit test, but observing the underlying query call
    // is the load-bearing precondition.
    let sink = FakeSink::new();
    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    for _ in 0..3 {
        reconciler.tick().await;
    }
    assert_eq!(
        reconciler.sink.state_counts_call_count(),
        3,
        "node_phase_counts must run once per tick"
    );
}

// =================================================================
// D — draining → drained
// =================================================================

#[tokio::test]
async fn d1_empty_resident_marks_drained() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.drained_transitions, 1);
    let updates = reconciler.sink.updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].node_id, node);
    assert_eq!(updates[0].expected, PHASE_DRAINING);
    assert_eq!(updates[0].new_state, PHASE_DRAINED);
    assert!(updates[0].last_error.is_none());
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some("drained")
    );
}

#[tokio::test]
async fn d2_non_empty_resident_stays_draining() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    // Default: evacuatable mirrors resident → sandbox gets evacuated.

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.drained_transitions, 0);
    // Node state must not advance to drained while a resident sandbox exists.
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING)
    );
    wait_in_flight_empty(&reconciler).await;
}

#[tokio::test]
async fn d3_in_flight_blocks_drained_even_when_resident_empty() {
    // A evacuation is in flight; resident is empty (the sandbox the
    // evacuation was launched for has been removed from the listing,
    // mimicking the gap between spawn and completion). The drained
    // transition must NOT fire while in_flight is non-zero.
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    // Subsequent ticks see empty resident (the sandbox evacuated off).
    sink.enqueue_resident(node, Vec::new());
    sink.gate_evacuations(); // evacuate_sandbox will block

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    // Tick 1: spawns the evacuation. Resident at this point is non-empty.
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 1);
    assert_eq!(r1.drained_transitions, 0);
    // Tick 2: resident empty, but in_flight non-zero → must NOT drain.
    let r2 = reconciler.tick().await;
    assert_eq!(r2.drained_transitions, 0);
    // Release the evacuation and let it complete; THEN drain on tick 3.
    reconciler.sink.release_evacuation_permits(1);
    wait_in_flight_empty(&reconciler).await;
    // Re-enqueue the empty resident for the third call to list_sandboxes_on_node.
    reconciler.sink.enqueue_resident(node, Vec::new());
    let r3 = reconciler.tick().await;
    assert_eq!(r3.drained_transitions, 1);
}

#[tokio::test]
async fn d4_phase_drift_skips_drained_transition_silently() {
    // Listing returns the node as draining, but the underlying
    // `drain_phase` has drifted (admin :undrain made it NULL).
    // `set_drain_phase` must return `Ok(false)` and the reconciler
    // must NOT emit a transition metric.
    let node = NodeId::new();
    let sink = FakeSink::new();
    // Deliberately inconsistent: listing says draining, but the
    // FakeSink phase map has no entry → drain_phase IS NULL.
    sink.set_draining_list(vec![node]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.drained_transitions, 0);
    // set_drain_phase was attempted with the right expected_phase.
    let updates = reconciler.sink.updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].expected, PHASE_DRAINING);
    assert_eq!(updates[0].new_state, PHASE_DRAINED);
    // The guard fired; the row never got a drain_phase value.
    assert_eq!(reconciler.sink.current_state(node), None);
}

#[tokio::test]
async fn d6_db_error_on_node_phase_counts_does_not_crash_tick() {
    // Per-tick gauge emission must be resilient to DB hiccups: an
    // error from `node_phase_counts` should be logged and the tick
    // continues into the draining/terminating handlers.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    *sink.fail_state_counts.lock().unwrap() = Some("forced".into());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    // Tick completed; draining node still processed.
    assert_eq!(report.draining_nodes, 1);
    assert_eq!(report.drained_transitions, 1);
    assert_eq!(reconciler.sink.state_counts_call_count(), 1);
}

#[tokio::test]
async fn d7_db_error_on_list_draining_does_not_block_terminating() {
    // Per RFC §Failure Policy, the reconciler must be resilient
    // tick-over-tick. If `list_draining_nodes` errors, the
    // `terminating` half of the tick still runs.
    let term_node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(term_node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![term_node]);
    sink.inject_fail_list_draining("forced");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.draining_nodes, 0);
    assert_eq!(report.terminating_nodes, 1);
    assert_eq!(report.terminated, 1);
}

#[tokio::test]
async fn d5_db_error_on_set_state_logged_no_transition() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.inject_fail_set_state("forced");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.drained_transitions, 0);
    // The DB error consumed the injected fault but no state change landed.
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING)
    );
}

// =================================================================
// L — Local-snapshot promotion + drain-blocker count
// =================================================================

/// A suspended sandbox whose `target_node_id` still pins it to the
/// draining node and whose snapshot has not yet been registered to S3
/// must block `draining → drained` until the promotion succeeds. On
/// the first tick the reconciler invokes `promote_local_snapshot`; on
/// the second tick (with the work queue drained) the node advances.
#[tokio::test]
async fn l1_pinned_suspended_blocks_drained_transition_until_promoted() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    // Tick 1: local-only suspended sandbox present. No resident
    // sandboxes (the suspended one is *not* resident from the
    // reconciler's POV — `RESIDENT_LIFECYCLES` excludes
    // `suspended`). Without the local-only gate this would have
    // triggered a `drained` transition.
    sink.enqueue_pinned_local_only(node, vec![sandbox]);
    // Tick 2: the durable worker registered the snapshot, so the
    // sandbox no longer appears in the local-only list.
    sink.enqueue_pinned_local_only(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());

    let report = reconciler.tick().await;
    assert_eq!(
        report.drained_transitions, 0,
        "drain must wait while a local-only suspended sandbox is still tied to the node"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING),
        "node must stay in draining while the local-snapshot work queue is nonempty"
    );
    let promoted = reconciler.sink.promoted();
    assert_eq!(
        promoted,
        vec![(sandbox, node)],
        "tick 1 must invoke promote_local_snapshot for the local-only sandbox"
    );

    // Reset draining_list so tick 2 sees the node again.
    reconciler.sink.set_draining_list(vec![node]);
    let report = reconciler.tick().await;
    assert_eq!(
        report.drained_transitions, 1,
        "once the work queue drains, drained must fire on the next tick"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED),
    );
}

/// Sandboxes that already have a registered S3 snapshot don't appear in
/// the local-only list at all (the SQL `NOT EXISTS` filter excludes
/// them), so the reconciler advances straight to `drained` without
/// triggering any reupload RPCs.
#[tokio::test]
async fn l2_already_s3_backed_sandboxes_dont_block_drain() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    // The fake's "list" returns an empty vec when no entries are
    // enqueued, which mirrors the SQL filter excluding S3-backed
    // sandboxes from the local-only list.
    sink.enqueue_pinned_local_only(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;
    assert_eq!(report.drained_transitions, 1);
    let promoted = reconciler.sink.promoted();
    assert!(
        promoted.is_empty(),
        "S3-backed sandboxes must not trigger any promotion calls",
    );
}

/// Promotion failures must be logged and retried — they should NOT
/// block subsequent sandboxes' promotion attempts on the same tick, and
/// they should NOT advance the node to drained.
#[tokio::test]
async fn l3_promotion_failure_does_not_block_other_sandboxes_or_advance_state() {
    let node = NodeId::new();
    let sandbox_a = SandboxId::new();
    let sandbox_b = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_pinned_local_only(node, vec![sandbox_a, sandbox_b]);
    sink.fail_promote(
        sandbox_a,
        PromoteError::NodeApi("node-api unreachable".into()),
    );

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.drained_transitions, 0);
    let promoted = reconciler.sink.promoted();
    // Order is not guaranteed under `buffer_unordered`; assert
    // set-equivalence instead of vec-equivalence.
    let promoted_set: HashSet<(SandboxId, NodeId)> = promoted.iter().copied().collect();
    let expected: HashSet<(SandboxId, NodeId)> =
        [(sandbox_a, node), (sandbox_b, node)].into_iter().collect();
    assert_eq!(
        promoted_set, expected,
        "sandbox_a's promotion failure must not short-circuit sandbox_b",
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING),
    );
}

/// Regression: a sandbox stuck in `suspending`/`restoring` (e.g. after
/// a controller crash) pins the node by virtue of having on-disk
/// artifacts the reconciler cannot promote (no snapshot row yet). The
/// promotable list is empty for these lifecycles, but the broader
/// "any blocker" count includes them so the node stays in `draining`
/// instead of advancing to a stuck `drained` state the auto-terminator
/// would then refuse to flip.
#[tokio::test]
async fn l4_suspending_restoring_block_drain_even_without_promotable_pin() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    // Promotable list is empty (sandbox isn't suspended yet — still
    // `suspending`). But the count query reports 1 extra blocker.
    sink.enqueue_pinned_local_only(node, Vec::new());
    sink.enqueue_extra_blocker_count(node, 1);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.drained_transitions, 0,
        "drain must wait while a suspending/restoring sandbox pins the node, \
         even with no promotable work to do"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING),
    );
}

/// When promotion returns `ArtifactMissing` (no local files and no
/// S3 snapshot), the reconciler must mark the sandbox `failed` so
/// drain stops blocking on a row whose data is already lost. The
/// next tick (with the row no longer in the local-only list) is then
/// free to advance the node to `drained`.
#[tokio::test]
async fn l5_artifact_missing_marks_sandbox_failed_so_drain_can_proceed() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_pinned_local_only(node, vec![sandbox]);
    sink.fail_promote(
        sandbox,
        PromoteError::ArtifactMissing("required on-disk artifact missing".into()),
    );

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    let marked = reconciler.sink.marked_lost();
    assert_eq!(
        marked,
        vec![(sandbox, node)],
        "ArtifactMissing must trigger mark_sandbox_lost for the source node",
    );
}

/// When promotion is refused because the sandbox is wedged in `restoring` on
/// the node (`SandboxRestoringOnNode`), the reconciler routes it to the
/// adopt-remote-snapshot give-up with the configured settle window, instead of
/// retrying the refused reupload or marking the sandbox lost. The give-up store
/// call (gated on the newest ready remote snapshot in production) is
/// what unblocks the drain.
#[tokio::test]
async fn l6_restoring_routes_to_adopt_remote_snapshot_with_settle() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_pinned_local_only(node, vec![sandbox]);
    sink.fail_promote(
        sandbox,
        PromoteError::SandboxRestoringOnNode(
            "cannot reupload snapshot: sandbox is restoring, must be suspended".into(),
        ),
    );

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.adopted_remote_snapshot(),
        vec![(
            sandbox,
            node,
            PositiveDuration::new_panic(Duration::from_secs(DEFAULT_REUPLOAD_GIVEUP_SETTLE_SECS)),
        )],
        "SandboxRestoringOnNode must route to make_sandbox_durable_from_remote_snapshot \
         with the configured settle window"
    );
    assert!(
        reconciler.sink.marked_lost().is_empty(),
        "the restoring give-up must not mark the sandbox lost (it stays recoverable)"
    );
}

/// The give-up is a no-op while it is still settling / has no ready remote
/// snapshot (the store CAS returns 0). The reconciler must still attempt it
/// (so it fires the moment a ready snapshot lands or the row ages in) and must
/// NOT mark the sandbox lost — the sandbox keeps blocking the drain, which is
/// the correct, visible behavior until a durable copy exists.
#[tokio::test]
async fn l7_restoring_noop_keeps_blocking_and_does_not_lose_sandbox() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_pinned_local_only(node, vec![sandbox]);
    sink.fail_promote(
        sandbox,
        PromoteError::SandboxRestoringOnNode(
            "cannot reupload snapshot: sandbox is restoring, must be suspended".into(),
        ),
    );
    // Model "not yet eligible": the store CAS matches no row.
    sink.noop_adopt_remote_snapshot(sandbox);
    // The sandbox is still resident, so the node must not drain this tick.
    sink.enqueue_resident(node, vec![sandbox]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.adopted_remote_snapshot().len(),
        1,
        "the give-up must still be attempted each tick so it fires once eligible"
    );
    assert!(
        reconciler.sink.marked_lost().is_empty(),
        "a not-yet-eligible give-up must never lose the sandbox"
    );
    assert_eq!(
        report.drained_transitions, 0,
        "the sandbox must keep blocking the drain until a durable copy exists"
    );
}

/// With the give-up disabled (`reupload_giveup_settle = None`), a
/// restoring-on-node refusal falls back to the prior behavior: retry next tick.
/// The reconciler must NOT call the adopt-remote-snapshot store method.
#[tokio::test]
async fn l8_restoring_giveup_disabled_does_not_adopt() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_pinned_local_only(node, vec![sandbox]);
    sink.fail_promote(
        sandbox,
        PromoteError::SandboxRestoringOnNode(
            "cannot reupload snapshot: sandbox is restoring, must be suspended".into(),
        ),
    );

    let mut cfg = config_enabled(4, 8);
    cfg.reupload_giveup_settle = None;
    let reconciler = Reconciler::new(cfg, sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    assert!(
        reconciler.sink.adopted_remote_snapshot().is_empty(),
        "give-up disabled (None) must not invoke make_sandbox_durable_from_remote_snapshot"
    );
}

// =================================================================
// TB — Termination-blocking residents (the terminator's resident guard)
// =================================================================

/// A node whose only remaining sandbox is one the terminator counts as resident
/// but the resident scan does not (`quarantined` here) must stay in `draining`.
/// Advancing it would park it in `drained`, where the terminator's UPDATE
/// matches nothing on every tick and this reconciler no longer looks at it —
/// the invisible wedge this term exists to prevent. Tick 2 shows the hold is a
/// hold and not a new terminal state: once the row clears, the node drains.
#[tokio::test]
async fn tb1_terminator_resident_holds_node_in_draining_until_it_clears() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    // Nothing resident, nothing in flight, no snapshot blockers — every other
    // term is zero, so this is the only thing that can hold the node.
    sink.enqueue_termination_blockers(node, vec![("quarantined", 1)]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());

    let report = reconciler.tick().await;
    assert_eq!(
        report.drained_transitions, 0,
        "a node the terminator would refuse must not advance to drained"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING),
        "the node must be held in draining, where the stuck-drain warning and \
         the per-node gauge can still see it"
    );

    // Tick 2: quarantine resolved (or the sandbox was deleted), so the fake
    // reports nothing blocking.
    reconciler.sink.set_draining_list(vec![node]);
    let report = reconciler.tick().await;
    assert_eq!(
        report.drained_transitions, 1,
        "the hold must lift as soon as the terminator would accept the node"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
}

/// With the gate off, a node the terminator would refuse advances to `drained`
/// exactly as it did before the fourth term existed — and the gauge still
/// reports it, which is the whole point of shipping the measurement enabled and
/// the behaviour disabled.
///
/// The flag is off by default (`ReconcilerConfig::default`), so this is what
/// every environment does until someone flips it per-cluster.
#[tokio::test]
async fn tb4_gate_off_advances_and_still_measures() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_termination_blockers(node, vec![("quarantined", 1)]);

    let mut cfg = config_enabled(4, 8);
    cfg.termination_blocking_gate = false;
    let reconciler = Reconciler::new(cfg, sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.drained_transitions, 1,
        "with the gate off the node must advance exactly as it did before the \
         fourth term existed"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
    assert!(
        reconciler.sink.termination_blocking_calls() > 0,
        "the scan must still run with the gate off — the gauge is how an \
         operator learns whether flipping the flag would hold anything"
    );
}

/// A failed count must not be read as "nothing blocking". The scan errors, the
/// reconciler skips the node for this tick, and the next tick re-evaluates —
/// the same fail-closed handling every other gate query gets, and the one that
/// matters most here: treating the error as zero would terminate a node over a
/// transient DB blip.
#[tokio::test]
async fn tb2_db_error_on_the_scan_does_not_advance_the_node() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.inject_fail_termination_blockers("forced");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.drained_transitions, 0,
        "an unanswered residency question must not release the node"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING)
    );

    // The injection is one-shot: the next tick gets a real answer and the node
    // advances, so the failure defers rather than wedges.
    reconciler.sink.set_draining_list(vec![node]);
    let report = reconciler.tick().await;
    assert_eq!(report.drained_transitions, 1);
}

/// The rate-limit map is keyed by node and a held node never leaves `draining`
/// on its own, so the slots have to be pruned against the live draining set or
/// the map grows with fleet churn for the process's lifetime. Both nodes are
/// held on tick 1 so both take a slot; only one is still draining on tick 2.
#[tokio::test]
async fn tb3_hold_warn_slots_are_pruned_when_a_node_leaves_draining() {
    let held = NodeId::new();
    let departed = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(held, PHASE_DRAINING);
    sink.set_initial_state(departed, PHASE_DRAINING);
    sink.set_draining_list(vec![held, departed]);
    sink.enqueue_termination_blockers(held, vec![("quarantined", 1)]);
    sink.enqueue_termination_blockers(departed, vec![("quarantined", 1)]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;
    assert_eq!(
        report.drained_transitions, 0,
        "both nodes are held, so both take a warn slot"
    );
    assert!(reconciler.warn_pacer.is_tracking(held));
    assert!(reconciler.warn_pacer.is_tracking(departed));

    // Tick 2: the operator resolved `departed`'s quarantine and undrained it,
    // so it is no longer in the draining set.
    reconciler.sink.set_draining_list(vec![held]);
    reconciler
        .sink
        .enqueue_termination_blockers(held, vec![("quarantined", 1)]);
    let _ = reconciler.tick().await;

    assert!(
        reconciler.warn_pacer.is_tracking(held),
        "the still-held node must keep its window, or every tick warns again"
    );
    assert!(
        !reconciler.warn_pacer.is_tracking(departed),
        "a node that left the draining set must not keep a slot"
    );
}

// =================================================================
// GM — Gentle drain mode
// =================================================================

/// The defining property: a resident, evacuatable sandbox is left alone. The
/// same fixture in forced mode (`d2_non_empty_resident_stays_draining`) spawns
/// an evacuation for it.
#[tokio::test]
async fn gm1_gentle_mode_never_evacuates_residents() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_gentle(node);
    // Resident and (in forced mode) evacuatable: the fake mirrors the resident
    // listing as `started` when no explicit evacuatable listing is enqueued.
    sink.enqueue_resident(node, vec![sandbox]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.gentle_nodes, 1);
    assert_eq!(report.spawned_evacuations, 0);
    assert!(
        reconciler.sink.evacuated_ids().is_empty(),
        "gentle mode must not call evacuate_sandbox"
    );
    assert_eq!(report.drained_transitions, 0);
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING)
    );
}

/// Gentle mode is not "do nothing": the node is still cordoned every tick (that
/// is what stops new placement), and pinned local-only snapshots are still
/// promoted — which is how a gentle drain makes progress without touching a
/// running sandbox.
///
/// The abandon-stuck-transient sweep is the one shared step gentle mode drops,
/// because it selects on age rather than liveness and would discard a
/// merely-slow create or restore. `gm13` pins that; the cost is that a wedged
/// transient parks the node, visibly, via `node_drain.drain_blockers`.
#[tokio::test]
async fn gm2_gentle_mode_still_cordons_and_promotes() {
    let node = NodeId::new();
    let resident = SandboxId::new();
    let pinned = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_gentle(node);
    sink.enqueue_resident(node, vec![resident]);
    sink.enqueue_pinned_local_only(node, vec![pinned]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    reconciler.tick().await;

    assert_eq!(reconciler.sink.cordon_calls(), vec![node]);
    assert_eq!(reconciler.sink.promoted(), vec![(pinned, node)]);
    // The abandon-stuck-transient sweep is deliberately *not* here: it is
    // forced-only, because it selects on age rather than liveness. `gm13` pins
    // that.
    assert!(reconciler.sink.abandon_stale_calls().is_empty());
}

/// A gentle drain converges through the same zero-blockers gate as a forced
/// one — nothing about the mode changes what "drained" means. This is the
/// natural-departure case: the resident left on its own between ticks.
#[tokio::test]
async fn gm3_gentle_mode_drains_once_residents_leave_on_their_own() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_gentle(node);
    sink.enqueue_resident(node, vec![sandbox]);
    // Tick 2: the sandbox auto-suspended (or was deleted) and is gone.
    sink.enqueue_resident(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let first = reconciler.tick().await;
    assert_eq!(first.drained_transitions, 0);

    let second = reconciler.tick().await;
    assert_eq!(second.drained_transitions, 1);
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
    assert!(reconciler.sink.evacuated_ids().is_empty());
}

/// A gentle drain must not hand the legacy (no-artifact-id) reupload route any
/// work.
///
/// That route uploads from the live on-disk snapshot directory and only checks
/// the sandbox is `Suspended` when the job is enqueued, so it relies on the drain
/// gate 409ing wakes for the bytes to stay put. Gentle leaves that gate open, so
/// the pinned-list call must ask to exclude those rows; the node then keeps
/// counting them as blockers and parks rather than registering a snapshot a
/// concurrent wake could have torn.
#[tokio::test]
async fn gm9_gentle_mode_excludes_legacy_reupload_rows() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_gentle(node);
    sink.enqueue_resident(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    reconciler.tick().await;

    assert_eq!(
        reconciler.sink.pinned_legacy_exclusions(),
        vec![(node, true)],
        "a gentle drain must exclude the legacy-reupload rows"
    );
}

/// A gentle drain held only by a blocker the per-node gauges cannot see must
/// still be attributable.
///
/// The silent-park case: a suspended rollout-skew row with a NULL
/// `current_local_artifact_id` is excluded from the pinned list under gentle
/// mode, and `suspended` is not a resident lifecycle, so `sandboxes_remaining`
/// and `pinned_suspended` both read zero. The drain gate still counts it, so the
/// node stays in `draining` — parked, with every per-node gauge at zero and
/// nothing naming the blocker.
///
/// `blockers` is the only input to the advance gate that was never published, so
/// reporting it is what makes any park attributable, whichever class holds the
/// node.
#[tokio::test]
async fn gm12_a_gentle_node_held_by_an_invisible_blocker_reports_it() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_gentle(node);
    // No residents and no pinned rows: both gauges read zero.
    sink.enqueue_resident(node, Vec::new());
    // ...but the drain gate counts one, so the node cannot advance.
    sink.enqueue_extra_blocker_count(node, 1);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.drained_transitions, 0,
        "a counted blocker must hold the node in draining"
    );
    assert_eq!(
        report.blocked_nodes, 1,
        "a node held only by an invisible blocker must still be reported, or the park \
         is undiagnosable from the per-node gauges"
    );
}

/// A gentle drain must not run the abandon-stuck-transient sweep.
///
/// The sweep flips any row aged past `stale_abandon` from
/// a transient lifecycle to `failed` and best-effort deletes its execution,
/// **on age alone** — there is no node-side proof the transition is actually
/// wedged. A cold create pulling a large template, or a restore of a large
/// memfile, legitimately exceeds that: the substrate clamps each blocking wake
/// wait to ~135s and relies on sweep re-drives, so a live restore routinely
/// spans several budgets.
///
/// Under a forced drain that is defensible — the drain is already suspending and
/// migrating every resident, so the user has accepted disruption. Under a gentle
/// drain it contradicts the contract, which is to cordon and wait without
/// touching residents. So the sweep is forced-only, and the node parks instead:
/// visible via `node_drain.drain_blockers`, and convergeable as soon as an
/// operator escalates the drain with `:drain --forced`.
#[tokio::test]
async fn gm13_gentle_mode_never_abandons_an_aged_transient() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_gentle(node);
    // One resident wedged in a non-evacuable transient lifecycle, aged past the
    // sweep's threshold. Under forced mode this is exactly what gets abandoned.
    sink.enqueue_resident(node, vec![SandboxId::new()]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert!(
        reconciler.sink.abandon_stale_calls().is_empty(),
        "a gentle drain must not ask the sweep to abandon anything; it would \
         destroy a live transition on age alone"
    );
    assert_eq!(
        report.drained_transitions, 0,
        "the aged transient must keep the node in draining rather than being \
         swept out from under it"
    );
}

/// A gone node's give-ups still see the legacy rows, even in gentle mode.
///
/// The exclusion protects live bytes from a concurrent wake. On a node whose kube
/// Node is gone and whose EC2 instance is confirmed terminated there are no live
/// bytes and no reachable wake, and the give-ups (adopt a ready remote snapshot,
/// else mark lost) are the only path by which such a node converges — so
/// excluding legacy rows there would wedge the drain forever, protecting nothing.
#[tokio::test]
async fn gm11_gone_node_giveups_see_legacy_rows_even_when_gentle() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_gentle(node);
    sink.enqueue_resident(node, Vec::new());
    // Kube Node gone + EC2 confirmed terminated → the give-up branch.
    sink.set_cordon_kube_node_gone(node);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.set_gone(node, None);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default())
        .with_instance_lookup(Some(lookup));
    reconciler.tick().await;

    assert_eq!(
        reconciler.sink.pinned_legacy_exclusions(),
        vec![(node, false)],
        "a gone node's give-ups must see the legacy rows even under a gentle drain"
    );
}

/// A forced drain still promotes them: its gate blocks every wake, so the live
/// snapshot directory cannot move under an upload in flight.
#[tokio::test]
async fn gm10_forced_mode_still_promotes_legacy_reupload_rows() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    reconciler.tick().await;

    assert_eq!(
        reconciler.sink.pinned_legacy_exclusions(),
        vec![(node, false)],
        "a forced drain keeps the legacy route, its gate holds the bytes still"
    );
}

/// A node listed with **no** mode does not evacuate. That is the production
/// NULL-column case — a drain that started before `nodes.drain_mode` existed,
/// or a row nobody has touched — and it resolves to gentle, so the reconciler
/// leaves its residents alone. The trade is deliberate: a forced drain in
/// flight across the deploy stalls instead of evacuating, which is a slow drain
/// rather than sandboxes suspended and migrated on the strength of a mode
/// nobody set.
#[tokio::test]
async fn gm8_absent_mode_does_not_evacuate() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(reconciler.sink.mode_of(node), NodeDrainMode::Gentle);
    assert_eq!(report.gentle_nodes, 1);
    assert_eq!(report.spawned_evacuations, 0);
    assert!(reconciler.sink.evacuated_ids().is_empty());
}

// =================================================================
// AC — Abandon-stuck-transient sweep
// =================================================================

/// Default config invokes the sweep once per draining node per tick,
/// propagating the configured `stale_abandon` timeout. A node with
/// no stuck transient sandboxes (the sink returns zero) does nothing
/// visible — no metric, no log noise, and certainly no state-change
/// UPDATE on the row — but the call MUST still happen so the SQL
/// guard is exercised every tick.
#[tokio::test]
async fn ac1_sweep_runs_with_configured_age_each_tick() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    // Forced: the sweep is forced-only, since it selects on age rather than
    // liveness and gentle mode must not discard a merely-slow transition.
    sink.set_draining_list_forced(vec![node]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    let calls = reconciler.sink.abandon_stale_calls();
    assert_eq!(
        calls,
        vec![(
            node,
            PositiveDuration::new_panic(Duration::from_secs(DEFAULT_STALE_ABANDON_SECS)),
        )],
        "abandon-stuck-transient sweep must run once per tick with the configured timeout"
    );
}

/// When the sweep reports rows flipped, the next tick's reconciler
/// state machine treats them as gone — the immediately-following
/// `list_sandboxes_on_node` call returning empty means drain
/// completes. This locks in the ordering: abandon BEFORE the resident
/// scan, so the same tick can finish a node that had `creating`
/// sandboxes as its only blockers.
#[tokio::test]
async fn ac2_abandoned_rows_unblock_drain_within_one_tick() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    // The sweep reports 2 rows abandoned. The resident-listing
    // pipeline reflects the post-abandon state (empty), so drain
    // completes on this same tick — without this ordering, the
    // resident scan would still see the wedged transient rows and the
    // node would stall for another tick.
    sink.enqueue_abandon_stale(node, 2);
    sink.enqueue_resident(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.drained_transitions, 1,
        "node must drain on the same tick that abandons its only blockers"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
}

/// `stale_abandon = None` is the operator opt-out: the sweep
/// short-circuits at the call site (the `if let Some(age) = …` block
/// in `handle_draining_node` never fires) so the sink method isn't
/// even called. Using `None` instead of an in-band `Duration::ZERO`
/// sentinel keeps the disabled state unrepresentable downstream.
#[tokio::test]
async fn ac3_none_disables_sweep() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);

    let mut config = config_enabled(4, 8);
    config.stale_abandon = None;
    let reconciler = Reconciler::new(config, sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    assert!(
        reconciler.sink.abandon_stale_calls().is_empty(),
        "stale_abandon = None must short-circuit the sweep entirely",
    );
}

/// Drift safety: if the node has been undrained between
/// `list_draining_nodes()` returning it and the sweep landing, the
/// sweep must be a no-op. Without this guard a stale tick could flip
/// legitimate transient sandboxes to `failed` on a node the operator
/// has explicitly told us is no longer being drained. Production
/// enforces this via an `EXISTS (... drain_phase = 'draining')`
/// clause in the SQL; the fake mirrors the same predicate.
#[tokio::test]
async fn ac5_undrained_node_aborts_sweep() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    // Listing still emits the node id (operator undrained AFTER the
    // listing materialized — the very race the guard protects). Forced, because
    // the sweep this test is about is forced-only.
    sink.set_draining_list_forced(vec![node]);
    // node_state has no entry → drain_phase IS NULL (admin :undrain
    // already cleared it). Pre-stage abandon-stale to return 5;
    // the SQL `EXISTS` guard must override that and return 0.
    sink.enqueue_abandon_stale(node, 5);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    // The reconciler did make the sweep call (the call is
    // unconditional when `stale_abandon = Some(_)`), but the SQL
    // guard short-circuited the row count to zero — modeled by the
    // FakeSink ignoring the pre-staged return when the node isn't in
    // PHASE_DRAINING. No abandon metric is emitted at zero.
    assert_eq!(
        reconciler.sink.abandon_stale_calls().len(),
        1,
        "the sweep call still fires; the guard is in-SQL"
    );
}

/// A DB error from the sweep is logged and the tick continues —
/// abandoning is a best-effort optimization, not a correctness
/// requirement. The resident scan and subsequent drain logic must
/// still run so a node whose only blockers are `started` sandboxes
/// makes progress.
#[tokio::test]
async fn ac4_db_error_on_sweep_does_not_block_tick() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.inject_fail_abandon_stale("forced");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.spawned_evacuations, 1,
        "sweep DB error must not gate the downstream evacuation pipeline"
    );
}

/// A draining node's only resident is wedged in a non-evacuation transient
/// lifecycle. The FakeSink is lifecycle-agnostic; the SQL lifecycle set is
/// covered by `non_evacuatable_set_is_the_stale_abandon_sweep_set`. With the sweep
/// enabled the row is abandoned and the node drains on the same tick; with
/// the sweep disabled the same wedged row blocks the drain indefinitely.
/// Before the sweep covered the full non-evacuable transient set it only
/// handled `creating`, so a stuck `restoring` row held the node forever.
#[tokio::test]
async fn ac6_stuck_restoring_only_resident_drains_only_with_sweep() {
    // Sweep enabled: the wedged row is abandoned, so the resident scan that
    // follows in the same tick sees an empty node and drain completes.
    {
        let node = NodeId::new();
        let sink = FakeSink::new();
        sink.set_initial_state(node, PHASE_DRAINING);
        sink.set_draining_list(vec![node]);
        sink.enqueue_abandon_stale(node, 1);
        sink.enqueue_resident(node, Vec::new());

        let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
        let report = reconciler.tick().await;

        assert_eq!(
            report.drained_transitions, 1,
            "the stale-abandon sweep must unblock a node whose only resident is a wedged \
             transient row"
        );
        assert_eq!(
            reconciler.sink.current_state(node).as_deref(),
            Some(PHASE_DRAINED)
        );
    }
    // Sweep disabled (`stale_abandon = None`): the same wedged, non-evacuatable
    // resident keeps blocking `draining -> drained`.
    {
        let node = NodeId::new();
        let sink = FakeSink::new();
        sink.set_initial_state(node, PHASE_DRAINING);
        sink.set_draining_list(vec![node]);
        // Resident stays non-empty and is not evacuatable (default empty
        // evacuatable listing), so nothing clears it this tick.
        sink.enqueue_resident(node, vec![SandboxId::new()]);

        let mut config = config_enabled(4, 8);
        config.stale_abandon = None;
        let reconciler = Reconciler::new(config, sink, FakeTerminator::default());
        let report = reconciler.tick().await;

        assert_eq!(
            report.drained_transitions, 0,
            "without the sweep a wedged transient resident must keep blocking the drain"
        );
        assert_eq!(
            reconciler.sink.current_state(node).as_deref(),
            Some(PHASE_DRAINING)
        );
    }
}

// =================================================================
// S — Evacuation spawning + parallelism
// =================================================================

#[tokio::test]
async fn s1_evacuatable_sandbox_spawns_one_evacuation_each() {
    let node = NodeId::new();
    let sandboxes: Vec<SandboxId> = (0..3).map(|_| SandboxId::new()).collect();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, sandboxes.clone());

    let reconciler = Reconciler::new(config_enabled(8, 16), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.spawned_evacuations, 3);
    wait_in_flight_empty(&reconciler).await;
    let evacuated: HashSet<SandboxId> = reconciler.sink.evacuated_ids().into_iter().collect();
    assert_eq!(evacuated, sandboxes.iter().copied().collect::<HashSet<_>>());
}

/// S10: the configured cohort placement flows to the cohort **restore** (the
/// restore sweep is where placement is applied now that evacuate is
/// suspend-only). A default config (no placement) restores with `None`.
#[tokio::test]
async fn s10_configured_placement_flows_through_to_restore() {
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);

    let placement = EvacuationPlacement {
        cohort_key: "space.pplx.net/rollout-cohort".to_string(),
        cohort_value: "test-2".to_string(),
    };
    let mut config = config_enabled(8, 16);
    config.drain_evacuation_placement = Some(placement.clone());
    let reconciler = Reconciler::new(config, sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.spawned_restores, 1);
    wait_restore_in_flight_empty(&reconciler).await;
    let recorded = reconciler
        .sink
        .restore_placement_for(sandbox)
        .expect("restore must be recorded")
        .expect("placement must carry the configured cohort");
    assert_eq!(recorded, placement);

    // Default config (no placement) restores with None.
    let sandbox2 = SandboxId::new();
    let sink2 = FakeSink::new();
    sink2.enqueue_restore_pending(vec![(sandbox2, SandboxLifecycle::Started)]);
    let reconciler2 = Reconciler::new(config_enabled(8, 16), sink2, FakeTerminator::default());
    reconciler2.tick().await;
    wait_restore_in_flight_empty(&reconciler2).await;
    assert!(
        matches!(reconciler2.sink.restore_placement_for(sandbox2), Some(None)),
        "default config must restore with None placement"
    );
}

#[tokio::test]
async fn s2_per_node_cap_bounds_spawn() {
    let node = NodeId::new();
    let sandboxes: Vec<SandboxId> = (0..10).map(|_| SandboxId::new()).collect();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, sandboxes);
    sink.gate_evacuations(); // keep in-flight set across tick boundary

    let reconciler = Reconciler::new(config_enabled(3, 16), sink, FakeTerminator::default());
    let report = reconciler.tick().await;
    assert_eq!(report.spawned_evacuations, 3, "max_per_node caps to 3");
    // Release everything so subsequent tests aren't slow.
    reconciler.sink.release_evacuation_permits(3);
    wait_in_flight_empty(&reconciler).await;
}

#[tokio::test]
async fn s3_per_cluster_cap_bounds_spawn_cross_node() {
    let a = NodeId::new();
    let b = NodeId::new();
    let sandboxes_a: Vec<SandboxId> = (0..10).map(|_| SandboxId::new()).collect();
    let sandboxes_b: Vec<SandboxId> = (0..10).map(|_| SandboxId::new()).collect();
    let sink = FakeSink::new();
    sink.set_initial_state(a, PHASE_DRAINING);
    sink.set_initial_state(b, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![a, b]);
    sink.enqueue_resident(a, sandboxes_a);
    sink.enqueue_resident(b, sandboxes_b);
    sink.gate_evacuations();

    // max_per_node=3 each; max_per_cluster=5 total. So a gets 3, b gets 2.
    let reconciler = Reconciler::new(config_enabled(3, 5), sink, FakeTerminator::default());
    let report = reconciler.tick().await;
    assert_eq!(report.spawned_evacuations, 5);
    reconciler.sink.release_evacuation_permits(5);
    wait_in_flight_empty(&reconciler).await;
}

#[tokio::test]
async fn s4_already_in_flight_not_respawned() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    // Same sandbox listed twice (one per tick) — second tick should
    // see it already in_flight and skip it.
    sink.enqueue_resident(node, vec![sandbox]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.gate_evacuations();

    let reconciler = Reconciler::new(config_enabled(8, 16), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 1);
    let r2 = reconciler.tick().await;
    assert_eq!(r2.spawned_evacuations, 0, "in-flight sandbox not respawned");
    reconciler.sink.release_evacuation_permits(1);
    wait_in_flight_empty(&reconciler).await;
    // Only one evacuation was attempted overall.
    assert_eq!(reconciler.sink.evacuated_ids().len(), 1);
}

#[tokio::test]
async fn s5_resident_non_evacuatable_blocks_drain() {
    // `pausing` is the example: a transient lifecycle the pipeline
    // can't service (it would undo a user request mid-flight) but
    // that does count as resident and blocks `draining → drained`.
    // `paused` itself is now evacuatable (see [`w2_paused_evacuates_lifecycle_preserving`]).
    let node = NodeId::new();
    let pausing = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_resident(node, vec![pausing]);
    // Explicitly empty evacuatable queue → resident is pausing-only.
    sink.enqueue_evacuatable(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.spawned_evacuations, 0);
    assert_eq!(report.drained_transitions, 0);
    assert!(reconciler.sink.evacuated_ids().is_empty());
    // Node stays in draining; no state update.
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING)
    );
}

#[tokio::test]
async fn s6_db_error_on_list_resident_skips_node() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.inject_fail_list_resident("forced");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    // Per-node failure swallows that node but the tick itself runs.
    assert_eq!(report.spawned_evacuations, 0);
    assert_eq!(report.drained_transitions, 0);
    // node_phase_counts still queried (tick proceeds past the DB error).
    assert_eq!(reconciler.sink.state_counts_call_count(), 1);
}

#[tokio::test]
async fn s7_db_error_on_list_evacuatable_skips_node() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.inject_fail_list_evacuatable("forced");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.spawned_evacuations, 0);
    assert!(reconciler.sink.evacuated_ids().is_empty());
}

#[tokio::test]
async fn s8_cluster_cap_fully_saturated_spawns_zero() {
    // Tick 1 fills the cluster cap with N gated evacuations on node A.
    // Tick 2 sees node B with evacuatable sandboxes; spawn must
    // return 0 because cluster_slots = max_per_cluster - in_flight.
    let a = NodeId::new();
    let b = NodeId::new();
    let a_sandboxes: Vec<SandboxId> = (0..2).map(|_| SandboxId::new()).collect();
    let b_sandboxes: Vec<SandboxId> = (0..3).map(|_| SandboxId::new()).collect();
    let sink = FakeSink::new();
    sink.set_initial_state(a, PHASE_DRAINING);
    sink.set_initial_state(b, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![a, b]);
    sink.enqueue_resident(a, a_sandboxes);
    // B has nothing in tick 1, then sandboxes in tick 2.
    sink.enqueue_resident(a, Vec::new());
    sink.enqueue_resident(b, Vec::new());
    sink.enqueue_resident(b, b_sandboxes);
    sink.gate_evacuations();

    // max_per_cluster=2; tick 1 saturates with A's 2 sandboxes.
    let reconciler = Reconciler::new(config_enabled(4, 2), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 2, "tick 1 saturates cluster cap");
    // Tick 2: cluster cap is full (no permits released yet), B's
    // evacuations must NOT spawn.
    let r2 = reconciler.tick().await;
    assert_eq!(
        r2.spawned_evacuations, 0,
        "cluster cap fully saturated; spawn must return 0"
    );
    reconciler.sink.release_evacuation_permits(2);
    wait_in_flight_empty(&reconciler).await;
}

#[tokio::test]
async fn s9_per_node_cap_fully_saturated_with_cluster_slack_spawns_zero() {
    // max_per_node=2, max_per_cluster=10. Tick 1 fills A's per-node
    // cap; tick 2 sees A with more sandboxes — must spawn 0 even
    // though the cluster has slack.
    let node = NodeId::new();
    let sandboxes: Vec<SandboxId> = (0..5).map(|_| SandboxId::new()).collect();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, sandboxes.clone());
    sink.enqueue_resident(node, sandboxes);
    sink.gate_evacuations();

    let reconciler = Reconciler::new(config_enabled(2, 10), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 2);
    let r2 = reconciler.tick().await;
    assert_eq!(
        r2.spawned_evacuations, 0,
        "per-node cap fully saturated; spawn must return 0"
    );
    reconciler.sink.release_evacuation_permits(2);
    wait_in_flight_empty(&reconciler).await;
}

// =================================================================
// F — Evacuation outcomes
// =================================================================

#[tokio::test]
async fn f1_successful_evacuation_clears_in_flight() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;
    assert_eq!(report.spawned_evacuations, 1);
    wait_in_flight_empty(&reconciler).await;
    // in_flight map must be empty after the evacuation future resolves.
    assert!(reconciler.in_flight.lock().await.is_empty());
}

#[tokio::test]
async fn f2_evacuation_failure_clears_in_flight_for_retry() {
    // Per RFC failure policy: "Per-sandbox suspend fails: retry on
    // next tick." That requires the in_flight slot to be released so
    // the next tick can spawn the same sandbox again.
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.enqueue_resident(node, vec![sandbox]); // second tick re-lists
    sink.fail_evacuate(sandbox, EvacuateError::Suspend("forced".into()));

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 1);
    wait_in_flight_empty(&reconciler).await;
    // Second tick must re-spawn because the failed evacuation cleared in_flight.
    let r2 = reconciler.tick().await;
    assert_eq!(r2.spawned_evacuations, 1);
    wait_in_flight_empty(&reconciler).await;
    // Two attempts on the same sandbox (first failed, second succeeded).
    assert_eq!(reconciler.sink.evacuated_ids().len(), 2);
}

#[tokio::test]
async fn f3_evacuate_error_phase_tags_correctly() {
    // The phase tag drives `node_drain.failures{phase}`; assert all
    // variants map to the expected stable tag.
    assert_eq!(EvacuateError::Suspend("x".into()).phase(), "suspend");
    assert_eq!(EvacuateError::Restore("x".into()).phase(), "restore");
    assert_eq!(EvacuateError::Lookup("x".into()).phase(), "lookup");
    assert_eq!(
        EvacuateError::SandboxFailedOnNode("x".into()).phase(),
        "sandbox_failed_on_node"
    );
    // A restore wake that timed out in a readiness wait is not a failure at
    // all — it surfaces as `RestoreOutcome::Converging` and drives
    // `node_drain.restore_converging{phase}` with the step that stalled
    // (`budget` when the outer wake budget fired without a phase).
    assert_eq!(RestoreOutcome::converging_phase_tag(None), "budget");
    for (wait_phase, tag) in [
        (WaitReadyTimeoutPhase::PodNeverReady, "pod_never_ready"),
        (WaitReadyTimeoutPhase::ProxyUnreachable, "proxy_unreachable"),
        (WaitReadyTimeoutPhase::GuestNotReady, "guest_not_ready"),
    ] {
        assert_eq!(RestoreOutcome::converging_phase_tag(Some(wait_phase)), tag);
    }
}

#[tokio::test]
async fn f4_failed_on_node_suspend_marks_db_failed_and_unblocks_drain() {
    // Regression: a sandbox `failed` on the node but `started`/`paused` in
    // the DB rejects suspend with FailedPrecondition forever.
    // Neither evacuate_timeout (in-flight only) nor stale_abandon
    // (non-evacuable transients only) covers it, so the node never drains.
    // The give-up path: an `EvacuateError::SandboxFailedOnNode` must route to
    // `mark_sandbox_failed_on_node` so the DB row flips to `failed` and the
    // node converges to `drained`.
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    // Tick 1: the wedged sandbox is resident + evacuatable. Suspend is
    // rejected because it is failed on the node.
    sink.enqueue_resident(node, vec![sandbox]);
    sink.fail_evacuate(
        sandbox,
        EvacuateError::SandboxFailedOnNode("cannot suspend: sandbox is failed".into()),
    );
    // Tick 2: the DB row is now `failed`, so it is no longer resident and the
    // node has no blockers — model that by an empty resident listing.
    sink.enqueue_resident(node, vec![]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());

    let r1 = reconciler.tick().await;
    assert_eq!(
        r1.spawned_evacuations, 1,
        "wedged sandbox spawns one evacuation"
    );
    wait_in_flight_empty(&reconciler).await;

    // The give-up path must have fired exactly once for this sandbox+node.
    assert_eq!(
        reconciler.sink.marked_failed_on_node(),
        vec![(sandbox, node)],
        "failed-on-node suspend must route to mark_sandbox_failed_on_node"
    );

    // Next tick: nothing resident, nothing in flight → node drains.
    let r2 = reconciler.tick().await;
    assert_eq!(r2.drained_transitions, 1, "node must finish draining");
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
}

#[tokio::test]
async fn f5_failed_on_node_drift_is_tolerated() {
    // If the sandbox drifts (woken elsewhere) between the failed suspend and
    // the mark-failed CAS, `mark_sandbox_failed_on_node` reports 0 rows. The
    // reconciler must not panic or wedge — it simply re-evaluates next tick.
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.fail_evacuate(
        sandbox,
        EvacuateError::SandboxFailedOnNode("cannot suspend: sandbox is failed".into()),
    );
    sink.drift_mark_failed_on_node(sandbox);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 1);
    wait_in_flight_empty(&reconciler).await;
    // The call was attempted even though it reported drift.
    assert_eq!(
        reconciler.sink.marked_failed_on_node(),
        vec![(sandbox, node)]
    );
    // No drained transition this tick: the sandbox is still resident.
    assert_eq!(r1.drained_transitions, 0);
}

// =================================================================
// T — terminating → deleted
// =================================================================

#[tokio::test]
async fn t1_happy_path_terminates_and_marks_deleted() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 1);
    assert_eq!(reconciler.terminator.calls(), vec![node]);
    let updates = reconciler.sink.updates();
    let entry = updates
        .iter()
        .find(|u| u.node_id == node && u.new_state == "deleted")
        .expect("expected terminating→deleted (modeled as DELETE FROM nodes)");
    assert_eq!(entry.expected, PHASE_TERMINATING);
    assert!(entry.last_error.is_none());
    // After delete the FakeSink purges the node from its phase map;
    // current_state should return None (mirroring `nodes` row gone).
    assert_eq!(reconciler.sink.current_state(node), None);
}

/// A sandbox that attached *after* something wrote `terminating` must not die
/// with the instance.
///
/// This is the destructive arm of the reverse-order race:
/// `POST :terminate` commits `drained → terminating`
/// under the `nodes` row lock, but the four pooled/post-create/direct-restore
/// placement writers take no lock on that row, so one can attach immediately
/// after the commit. The resident guard rides the `drained → terminating` edge
/// and is never consulted again, so this path used to go cordon → provider
/// terminate with no occupancy check at all.
///
/// Asserting on `terminator.calls()` rather than on a phase is the point: the
/// property is "the provider was never asked to destroy this instance," and a
/// phase assertion alone would pass even if the ASG call had already gone out.
///
/// The recheck this pins narrows the window; it does not close it (a placement
/// landing between the recount and the provider call is still lost). The fence
/// that would close it is tracked separately — see the RFC.
#[tokio::test]
async fn t2b_terminate_aborts_when_a_sandbox_attached_after_the_phase_write() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);
    // The attach the phase write could not have seen.
    sink.enqueue_termination_blockers(node, vec![("started", 1)]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert!(
        reconciler.terminator.calls().is_empty(),
        "the provider must never be asked to terminate a node holding a sandbox"
    );
    assert_eq!(report.terminated, 0);
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED),
        "the node must return to `drained`, whose reconciler re-evaluates the \
         guard every tick, rather than parking in `terminating`"
    );
    let updates = reconciler.sink.updates();
    let revert = updates
        .iter()
        .find(|u| u.node_id == node && u.new_state == PHASE_DRAINED)
        .expect("expected a terminating→drained revert");
    assert_eq!(revert.expected, PHASE_TERMINATING);
    assert!(
        revert.last_error.is_some(),
        "the revert must record why, or an operator sees a node silently move backwards"
    );
}

/// The property: **an operator who drains a node holding a sandbox the drain
/// pipeline cannot service must not lose that sandbox, whether or not their
/// cluster has opted into the fourth gate term.**
///
/// `t2b` asserts the recheck aborts, but it runs under `config_enabled`, which
/// sets `termination_blocking_gate: true` for the whole suite. The chart default
/// is `"false"`, and any cluster that has not overridden it runs gate-off — so
/// putting this recheck behind the flag would leave `t2b` green while destroying
/// a `stopped` sandbox in a configuration that is actually deployed. The recheck
/// is the last refusal before an irreversible ASG call and is deliberately
/// ungated; this is what says so.
///
/// `stopped` rather than `started` as the fixture. A `started` resident is in
/// `RESIDENT_LIFECYCLES`, so the earlier gate terms would still catch it and a
/// gated recheck would look harmless. `stopped` is in neither that include-list
/// nor [`NON_EVACUATABLE_RESIDENT_LIFECYCLES`], so with the gate off this recheck
/// is the only refusal *this reconciler* still makes for it — the others live
/// elsewhere (the drained-node terminator's `drained → terminating` flip, its
/// KubeNodeGone reap, and the admin `:terminate` 409).
#[tokio::test]
async fn t2d_pre_terminate_recheck_aborts_with_the_gate_off() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);
    sink.enqueue_termination_blockers(node, vec![("stopped", 1)]);

    let mut cfg = config_enabled(4, 8);
    cfg.termination_blocking_gate = false;
    let reconciler = Reconciler::new(cfg, sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert!(
        reconciler.terminator.calls().is_empty(),
        "with the gate off the recheck must still refuse: it is the last refusal \
         before the irreversible ASG call, and this reconciler's only one for a \
         `stopped` resident"
    );
    assert_eq!(report.terminated, 0);
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED),
        "the node must revert to `drained` so its reconciler re-evaluates every \
         tick, not park in `terminating` waiting for a human"
    );
}

/// The gate-off presence mirror for
/// [`t2d_pre_terminate_recheck_aborts_with_the_gate_off`].
///
/// `t2d` asserts an absence, and `t2c` below supplies the presence that makes
/// such an absence mean something — but only on the gate-*on* path, since it
/// takes `config_enabled` unmodified. Without this test, an early return added to
/// `handle_terminating_node` for the gate-off case would leave `t2d` green while
/// the reconciler terminated nothing at all in the configuration the chart
/// default actually deploys, which is the whole population `t2d` exists for.
#[tokio::test]
async fn t2e_terminate_proceeds_with_the_gate_off() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);

    let mut cfg = config_enabled(4, 8);
    cfg.termination_blocking_gate = false;
    let reconciler = Reconciler::new(cfg, sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(reconciler.terminator.calls(), vec![node]);
    assert_eq!(report.terminated, 1);
}

/// The mirror of `t2b`: with nothing attached, the recheck must not stand in
/// the way. Without this, deleting the recheck's `if` would leave `t2b` passing
/// on a reconciler that never terminates anything.
#[tokio::test]
async fn t2c_terminate_proceeds_when_the_recheck_finds_nothing() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(reconciler.terminator.calls(), vec![node]);
    assert_eq!(report.terminated, 1);
}

#[tokio::test]
async fn t2_asg_failure_records_last_error_stays_terminating() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);

    let term = FakeTerminator::default();
    term.fail(node, TerminateError::Provider("rate exceeded".into()));

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, term);
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 0);
    assert_eq!(report.terminate_failures, 1);
    let entry = reconciler
        .sink
        .updates()
        .into_iter()
        .find(|u| u.node_id == node)
        .expect("expected an update");
    assert_eq!(entry.expected, PHASE_TERMINATING);
    assert_eq!(entry.new_state, PHASE_TERMINATING);
    assert!(
        entry
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("rate exceeded")
    );
    // State stays terminating (the no-op self-update preserved it).
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_TERMINATING)
    );
}

#[tokio::test]
async fn t3_kube_node_gone_keeps_terminating_for_manual_recovery() {
    // A deleted kube Node does NOT imply the EC2 instance was
    // terminated. Without an EC2 DescribeInstances roundtrip the
    // reconciler cannot tell whether the ASG member is gone, so
    // auto-advancing to `deleted` would risk stranding a live
    // instance. The row must stay in `terminating` with a
    // descriptive `drain_last_error` so an operator can investigate.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);

    let term = FakeTerminator::default();
    term.fail(node, TerminateError::KubeNodeGone);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, term);
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 0);
    assert_eq!(report.terminate_failures, 1);
    let entry = reconciler
        .sink
        .updates()
        .into_iter()
        .find(|u| u.node_id == node)
        .expect("expected a self-update recording the error");
    assert_eq!(entry.expected, PHASE_TERMINATING);
    assert_eq!(entry.new_state, PHASE_TERMINATING);
    assert!(
        entry
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("kube Node missing"),
        "last_error should describe the kube-Node-missing condition: {entry:?}"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_TERMINATING),
        "row must stay in terminating for manual recovery"
    );
}

#[tokio::test]
async fn t4_stale_identity_skips_silently_no_db_write() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);

    let term = FakeTerminator::default();
    term.fail(node, TerminateError::StaleIdentity);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, term);
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 0);
    // No DB write at all on stale identity (no state update recorded).
    assert!(
        reconciler.sink.updates().iter().all(|u| u.node_id != node),
        "stale identity must not write to nodes row"
    );
}

#[tokio::test]
async fn t5_phase_drift_blocks_delete_after_terminate() {
    // Listing says terminating; row's drain_phase has drifted (e.g.
    // operator transitioned it back to 'drained' mid-flight). The
    // terminator returns Ok (the fake doesn't see the drift, only
    // the SubstrateSink does in prod). The `WHERE drain_phase =
    // 'terminating'` guard inside delete_terminating_node catches it.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINED); // drifted off 'terminating'
    sink.set_terminating_list(vec![node]); // but listing still sees it

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 0);
    let entry = reconciler
        .sink
        .updates()
        .into_iter()
        .find(|u| u.node_id == node)
        .expect("expected a guarded delete attempt");
    assert_eq!(entry.expected, PHASE_TERMINATING);
    assert_eq!(entry.new_state, "deleted");
    // Row stayed at the drifted phase (guard refused the DELETE).
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
}

#[tokio::test]
async fn t6_phase_drift_blocks_terminating_error_record() {
    // ASG fails AND the phase has drifted (admin :undrain made it
    // NULL). The error-record UPDATE must be guarded — without the
    // `WHERE drain_phase = 'terminating'` clause it would otherwise
    // write `drain_phase = 'terminating'` back and resurrect a
    // cancelled drain.
    let node = NodeId::new();
    let sink = FakeSink::new();
    // No initial phase: simulates drain_phase IS NULL (drift after
    // listing already captured the node id).
    sink.set_terminating_list(vec![node]);

    let term = FakeTerminator::default();
    term.fail(node, TerminateError::Provider("rate exceeded".into()));

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, term);
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 0);
    let entry = reconciler
        .sink
        .updates()
        .into_iter()
        .find(|u| u.node_id == node)
        .expect("expected an update attempt");
    assert_eq!(entry.expected, PHASE_TERMINATING);
    // Guard fired; drain_phase still NULL.
    assert_eq!(reconciler.sink.current_state(node), None);
}

#[tokio::test]
async fn t7_db_error_on_list_terminating_skips_all() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);
    sink.inject_fail_list_terminating("forced");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 0);
    assert!(reconciler.terminator.calls().is_empty());
}

#[tokio::test]
async fn t8_db_error_on_set_state_during_terminate_error_record() {
    // ASG terminate fails AND the follow-up "record last_error"
    // UPDATE also errors. The reconciler must log and move on
    // rather than panic.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);
    sink.inject_fail_set_state("forced");

    let term = FakeTerminator::default();
    term.fail(node, TerminateError::Provider("rate exceeded".into()));

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, term);
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 0);
    assert_eq!(report.terminate_failures, 1);
    // State unchanged (the failed UPDATE never landed).
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_TERMINATING)
    );
}

#[tokio::test]
async fn t9_db_error_on_set_state_during_advance_to_deleted() {
    // ASG terminate succeeds, but the post-terminate UPDATE that
    // would mark the row deleted errors. Reconciler must log and
    // move on; state stays terminating for retry next tick.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);
    sink.inject_fail_set_state("forced");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.terminated, 0);
    assert_eq!(report.terminate_failures, 1);
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_TERMINATING)
    );
}

// =================================================================
// Q — Cordon re-assert (every-tick convergence of unschedulable)
//
// The admin :drain endpoint commits drain_phase='draining' before
// patching spec.unschedulable. If the patch fails, the API returns
// 502 and relies on this reconciler to converge. These cells lock
// in the contract: cordon is ensured on every draining/terminating
// tick, and cordon failure short-circuits subsequent work for the
// node (no list_resident scan, no ASG call) so we never advance a
// still-schedulable node.
// =================================================================

#[tokio::test]
async fn q1_handle_draining_calls_cordon_each_tick() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.cordon_calls(),
        vec![node],
        "draining node must be re-cordoned every tick to recover from admin :drain Phase 2 failures"
    );
}

#[tokio::test]
async fn q2_handle_terminating_calls_cordon_each_tick() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    assert!(
        reconciler.sink.cordon_calls().contains(&node),
        "terminating node must be re-cordoned each tick before ASG terminate; cordon_calls={:?}",
        reconciler.sink.cordon_calls()
    );
}

#[tokio::test]
async fn q3_cordon_failure_blocks_drained_transition() {
    // Resident is empty (the "easy" drained transition path), but
    // cordon fails. The reconciler must NOT mark the node drained,
    // since a non-cordoned draining node may have fresh pods landing
    // between the resident-list scan and the drained transition.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.fail_cordon(node, "kubernetes API timeout");

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.drained_transitions, 0,
        "drained transition must be blocked when cordon fails"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING),
        "node must stay in draining when cordon fails"
    );
    let updates = reconciler.sink.updates();
    let cordon_err = updates.iter().find(|u| {
        u.node_id == node
            && u.expected == PHASE_DRAINING
            && u.new_state == PHASE_DRAINING
            && u.last_error.is_some()
    });
    assert!(
        cordon_err.is_some(),
        "drain_last_error must be persisted on cordon failure; updates={updates:?}"
    );
}

#[tokio::test]
async fn q4_cordon_failure_blocks_terminate_call() {
    // Cordon must succeed before we let the ASG terminate the
    // instance — otherwise a still-schedulable node could receive
    // fresh pods between the operator's :terminate and the
    // reconciler's tick.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);
    sink.fail_cordon(node, "kubernetes API timeout");

    let terminator = FakeTerminator::default();
    let reconciler = Reconciler::new(config_enabled(4, 8), sink, terminator);
    let report = reconciler.tick().await;

    assert_eq!(
        report.terminated, 0,
        "ASG terminate must be blocked when cordon fails"
    );
    assert!(
        reconciler.terminator.calls().is_empty(),
        "FakeTerminator.terminate must not be called when cordon fails; got {:?}",
        reconciler.terminator.calls()
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_TERMINATING),
        "node must stay in terminating when cordon fails"
    );
}

#[tokio::test]
async fn q5_cordon_stale_identity_skips_draining_tick_without_last_error() {
    // The kube Node UID changed under us (same-name replacement
    // landed but inventory reflector hasn't caught up). Cordoning by
    // name would freeze the replacement's capacity — instead we
    // defer silently. drain_last_error must NOT be written: the
    // inventory upsert will clear drain_phase on its next tick.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.fail_cordon_stale_identity(node);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.drained_transitions, 0,
        "draining must not advance to drained while cordon is deferred"
    );
    let updates = reconciler.sink.updates();
    let last_error_writes: Vec<_> = updates.iter().filter(|u| u.last_error.is_some()).collect();
    assert!(
        last_error_writes.is_empty(),
        "stale-identity cordon failure must NOT write drain_last_error; got {last_error_writes:?}"
    );
}

#[tokio::test]
async fn q6_cordon_stale_identity_skips_terminating_tick_without_terminate_call() {
    // Same as q5 but on the terminating side: protects against
    // accidentally cordoning a replacement Node we're about to ASG-
    // terminate (which would actually be Ec2InstanceTerminator's
    // problem to catch — but defense in depth keeps the cordon
    // pre-flight from creating cordon-only damage if terminate
    // changes shape).
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_TERMINATING);
    sink.set_terminating_list(vec![node]);
    sink.fail_cordon_stale_identity(node);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let _ = reconciler.tick().await;

    assert!(
        reconciler.terminator.calls().is_empty(),
        "terminate must not be called when cordon is deferred for stale identity"
    );
    let updates = reconciler.sink.updates();
    let last_error_writes: Vec<_> = updates.iter().filter(|u| u.last_error.is_some()).collect();
    assert!(
        last_error_writes.is_empty(),
        "stale-identity cordon failure must NOT write drain_last_error; got {last_error_writes:?}"
    );
}

// =================================================================
// P — providerID parsing
// =================================================================

#[test]
fn p1_parse_instance_id_matrix() {
    // Valid AWS providerIDs.
    assert_eq!(
        aws::parse_instance_id("aws:///us-west-2a/i-0abc123def"),
        Some("i-0abc123def")
    );
    assert_eq!(
        aws::parse_instance_id("aws:///us-east-1c/i-0123456789abcdef0"),
        Some("i-0123456789abcdef0")
    );
    // Wrong scheme.
    assert_eq!(aws::parse_instance_id("gce://project/zone/instance"), None);
    // AWS scheme but not an instance ID.
    assert_eq!(aws::parse_instance_id("aws:///us-west-2a/not-an-id"), None);
    // Empty / pathological.
    assert_eq!(aws::parse_instance_id(""), None);
    assert_eq!(aws::parse_instance_id("aws:///"), None);
}

// =================================================================
// W — End-to-end workflows
// =================================================================

#[tokio::test]
async fn w1_drain_started_sandboxes_completes_in_two_ticks() {
    let node = NodeId::new();
    let sandboxes: Vec<SandboxId> = (0..3).map(|_| SandboxId::new()).collect();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    // Tick 1: 3 sandboxes still resident.
    sink.enqueue_resident(node, sandboxes.clone());
    // Tick 2: cleared after evacuations completed.
    sink.enqueue_resident(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 3);
    assert_eq!(r1.drained_transitions, 0);
    wait_in_flight_empty(&reconciler).await;
    let r2 = reconciler.tick().await;
    assert_eq!(r2.drained_transitions, 1);
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
}

#[tokio::test]
async fn w2_paused_evacuates_lifecycle_preserving() {
    // A node carrying both `started` and `paused` sandboxes drains in
    // two ticks like a started-only node: both lifecycles are
    // evacuatable, the pipeline preserves the pre-evacuation lifecycle
    // (started → started; paused → paused by issuing the restore with
    // `target_state = Paused`, so the VM never resumes), and the drain
    // proceeds without operator wake/terminate.
    //
    // Asserts the full acceptance criteria of the paused-evacuation
    // change: (1) paused does not block drain, (2) paused is not
    // silently resumed (post-restore pre_lifecycle stays Paused), (3)
    // metrics-tag-distinguishability via `evacuation_pre_lifecycle`.
    let node = NodeId::new();
    let started = SandboxId::new();
    let paused = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    // Tick 1: both resident; both are evacuatable, each tagged with
    // its pre-evacuation lifecycle.
    sink.enqueue_resident(node, vec![started, paused]);
    sink.enqueue_evacuatable_typed(
        node,
        vec![
            (started, SandboxLifecycle::Started),
            (paused, SandboxLifecycle::Paused),
        ],
    );
    // Tick 2: both evacuated off.
    sink.enqueue_resident(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 2);
    assert_eq!(r1.drained_transitions, 0);
    wait_in_flight_empty(&reconciler).await;

    // The paused sandbox is recorded as a paused evacuation (NOT a
    // started one) — the pre_lifecycle tag flows through to the sink
    // so the substrate can preserve the user-visible lifecycle.
    assert_eq!(
        reconciler.sink.evacuation_pre_lifecycle(started),
        Some(SandboxLifecycle::Started),
    );
    assert_eq!(
        reconciler.sink.evacuation_pre_lifecycle(paused),
        Some(SandboxLifecycle::Paused),
    );

    let r2 = reconciler.tick().await;
    assert_eq!(r2.drained_transitions, 1);
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
}

#[tokio::test]
async fn w3_drained_node_does_not_self_terminate() {
    // The `drained -> terminating` transition is owned by the
    // separate `drained_node_terminator` companion reconciler (its
    // own sub-issue). THIS reconciler must never touch drained
    // nodes — `list_draining_nodes` and `list_terminating_nodes`
    // filter on those exact states, so a drained row is invisible
    // to it.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINED);
    // Neither list returns this node (state isn't draining or terminating).
    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    for _ in 0..5 {
        let r = reconciler.tick().await;
        assert_eq!(r.draining_nodes, 0);
        assert_eq!(r.terminating_nodes, 0);
    }
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
    assert!(reconciler.terminator.calls().is_empty());
    assert!(reconciler.sink.updates().is_empty());
}

#[tokio::test]
async fn w4_undrain_mid_evacuation_stops_new_spawns() {
    // Tick 1: node is draining, one evacuatable sandbox → evacuation spawned (gated).
    // Operator undrains: state→registered, listing emptied.
    // Tick 2: list_draining_nodes returns empty → no new spawn.
    // Release the in-flight evacuation; it completes; in_flight clears.
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.gate_evacuations();

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 1);
    // Operator action: :undrain — clears drain_phase (NULL).
    reconciler.sink.clear_initial_state(node);
    reconciler.sink.set_draining_list(Vec::new());
    let r2 = reconciler.tick().await;
    assert_eq!(r2.draining_nodes, 0);
    assert_eq!(r2.spawned_evacuations, 0);
    // Already-spawned evacuation runs to completion regardless.
    reconciler.sink.release_evacuation_permits(1);
    wait_in_flight_empty(&reconciler).await;
    assert_eq!(reconciler.sink.evacuated_ids(), vec![sandbox]);
    // Final drain_phase is NULL (no stomping by the reconciler).
    assert_eq!(reconciler.sink.current_state(node), None);
}

#[tokio::test]
async fn w5_pausing_blocks_drain_until_settled() {
    // `pausing` is a transient state the pipeline can't service —
    // evacuating it would undo a user-initiated pause request
    // mid-flight. The node holds in `draining` until the sandbox
    // settles into `paused`, then the next tick evacuates it
    // lifecycle-preservingly and the third tick drains the node.
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);

    // Tick 1: sandbox is `pausing` — resident but not evacuatable.
    sink.enqueue_resident(node, vec![sandbox]);
    sink.enqueue_evacuatable(node, Vec::new());
    // Tick 2: sandbox has settled to `paused` — now evacuatable.
    sink.enqueue_resident(node, vec![sandbox]);
    sink.enqueue_evacuatable_typed(node, vec![(sandbox, SandboxLifecycle::Paused)]);
    // Tick 3: evacuated off.
    sink.enqueue_resident(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());

    let r1 = reconciler.tick().await;
    assert_eq!(
        r1.spawned_evacuations, 0,
        "pausing must not be evacuated mid-flight"
    );
    assert_eq!(r1.drained_transitions, 0);

    let r2 = reconciler.tick().await;
    assert_eq!(
        r2.spawned_evacuations, 1,
        "once pausing settles to paused, the pipeline picks it up"
    );
    wait_in_flight_empty(&reconciler).await;
    assert_eq!(
        reconciler.sink.evacuation_pre_lifecycle(sandbox),
        Some(SandboxLifecycle::Paused),
    );

    let r3 = reconciler.tick().await;
    assert_eq!(r3.drained_transitions, 1);
}

#[tokio::test]
async fn w6_paused_evacuate_failure_clears_in_flight_for_retry() {
    // The reconciler-level invariant behind the paused-path Restore-
    // failure rule: a paused evacuation is now a single restore RPC
    // issued with `target_state = Paused`. If that RPC fails (snapshot
    // load error, transport blip, etc.), the store must surface
    // `EvacuateError::Restore` rather than silently finalizing the row
    // as `lifecycle = 'started'`. When the sink returns Restore, the
    // reconciler clears `in_flight`, leaves the node in draining, and
    // re-spawns the sandbox on the next tick still tagged with
    // `pre_lifecycle = Paused`. The store-side "don't fall back to
    // 'started'" half is exercised by production / integration code
    // paths; this test pins the reconciler half (no silent success
    // metric, proper retry, lifecycle tag preserved across the retry).
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.enqueue_resident(node, vec![sandbox]);
    sink.enqueue_evacuatable_typed(node, vec![(sandbox, SandboxLifecycle::Paused)]);
    sink.enqueue_evacuatable_typed(node, vec![(sandbox, SandboxLifecycle::Paused)]);
    sink.fail_evacuate(
        sandbox,
        EvacuateError::Restore("snapshot load failed".into()),
    );

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_evacuations, 1);
    wait_in_flight_empty(&reconciler).await;

    let r2 = reconciler.tick().await;
    assert_eq!(
        r2.spawned_evacuations, 1,
        "failed paused evacuation must be retried next tick"
    );
    wait_in_flight_empty(&reconciler).await;

    // Both attempts carried the Paused pre-lifecycle; the second
    // attempt succeeded.
    let evacuated = reconciler.sink.evacuated.lock().unwrap().clone();
    assert_eq!(evacuated.len(), 2);
    for (s, _, lc) in evacuated {
        assert_eq!(s, sandbox);
        assert_eq!(lc, SandboxLifecycle::Paused);
    }
}

#[test]
fn evacuable_lifecycle_set_contains_started_and_paused() {
    // The single source of truth for which lifecycles the drain
    // pipeline can service. If a future change adds another
    // evacuable lifecycle, this assertion fails loudly and the
    // author has to update the test alongside the const.
    let want: HashSet<SandboxLifecycle> = [SandboxLifecycle::Started, SandboxLifecycle::Paused]
        .into_iter()
        .collect();
    let got: HashSet<SandboxLifecycle> = EVACUABLE_LIFECYCLES.iter().copied().collect();
    assert_eq!(got, want);
}

#[test]
fn non_evacuatable_set_is_the_stale_abandon_sweep_set() {
    // `NON_EVACUATABLE_RESIDENT_LIFECYCLES` is the single source of truth
    // for which resident transient lifecycles the stale-abandon sweep ages
    // out on a draining node. A stuck `restoring` row can block a drain, so
    // `Restoring` MUST be in the set. The assertion pins the entire set so
    // dropping any member (which
    // would silently re-open the "this transient can block a drain forever"
    // gap) fails loudly. It must also be exactly
    // `RESIDENT_LIFECYCLES` minus `EVACUABLE_LIFECYCLES` so the
    // warning enumeration and the sweep agree on "what can block a drain".
    use SandboxLifecycle as L;
    let want: HashSet<L> = [
        L::Creating,
        L::Starting,
        L::Pausing,
        L::Resuming,
        L::Stopping,
        L::Suspending,
        L::Restoring,
        L::Checkpointing,
    ]
    .into_iter()
    .collect();
    let got: HashSet<L> = NON_EVACUATABLE_RESIDENT_LIFECYCLES
        .iter()
        .copied()
        .collect();
    assert_eq!(got, want, "the stale-abandon sweep set drifted");
    assert!(
        got.contains(&L::Restoring),
        "Restoring must remain in the swept set — a stuck user restore can block a drain"
    );

    // Cross-check the set algebra: non-evacuable == non-terminal-resident
    // minus evacuable. Every set is `&[SandboxLifecycle]`, so compare on the
    // variants directly.
    let non_terminal: HashSet<SandboxLifecycle> = RESIDENT_LIFECYCLES.iter().copied().collect();
    let evacuable: HashSet<SandboxLifecycle> = EVACUABLE_LIFECYCLES.iter().copied().collect();
    let non_evacuatable: HashSet<SandboxLifecycle> = NON_EVACUATABLE_RESIDENT_LIFECYCLES
        .iter()
        .copied()
        .collect();
    let expected: HashSet<SandboxLifecycle> =
        non_terminal.difference(&evacuable).copied().collect();
    assert_eq!(non_evacuatable, expected);
}

#[test]
fn is_evacuable_lifecycle_accepts_only_started_and_paused() {
    // Every variant of SandboxLifecycle is tested explicitly so a new
    // variant added to common-core forces an explicit "evacuable or
    // not" decision (the match below becomes non-exhaustive at
    // compile time).
    use pplx_space_common_core::types::SandboxLifecycle as L;
    let cases: &[(L, bool)] = &[
        (L::Created, false),
        (L::Creating, false),
        (L::Starting, false),
        (L::Started, true),
        (L::Restoring, false),
        (L::Pausing, false),
        (L::Paused, true),
        (L::Resuming, false),
        (L::Suspending, false),
        (L::Suspended, false),
        (L::Stopping, false),
        (L::Stopped, false),
        (L::Deleting, false),
        (L::Checkpointing, false),
        (L::Quarantined, false),
        (L::Failed, false),
    ];
    for (lifecycle, expected) in cases.iter().copied() {
        assert_eq!(
            is_evacuable_lifecycle(lifecycle),
            expected,
            "is_evacuable_lifecycle({lifecycle}) returned wrong value"
        );
    }
    // Belt-and-braces: any SandboxLifecycle variant the test forgot
    // would fail compilation here because the match below is
    // exhaustive. A new variant added upstream breaks this test,
    // forcing the author to extend the cases table above.
    fn _assert_exhaustive(lifecycle: SandboxLifecycle) -> bool {
        match lifecycle {
            L::Started | L::Paused => true,
            L::Created
            | L::Creating
            | L::Starting
            | L::Restoring
            | L::Pausing
            | L::Resuming
            | L::Suspending
            | L::Suspended
            | L::Stopping
            | L::Stopped
            | L::Deleting
            | L::Checkpointing
            | L::Quarantined
            | L::Failed => false,
        }
    }
}

#[test]
fn sandbox_lifecycle_str_round_trip() {
    // The SQL bind path is `EVACUABLE_LIFECYCLES.iter().map(|lc|
    // lc.as_str())`; the read path is `text → SandboxLifecycle::from_str`.
    // This pins the round-trip for every variant the drain code can
    // see, so a typo in `as_str` or `from_str` fails loudly.
    use pplx_space_common_core::types::SandboxLifecycle as L;
    for variant in [
        L::Created,
        L::Creating,
        L::Starting,
        L::Started,
        L::Restoring,
        L::Pausing,
        L::Paused,
        L::Resuming,
        L::Suspending,
        L::Suspended,
        L::Stopping,
        L::Stopped,
        L::Deleting,
        L::Checkpointing,
        L::Failed,
    ] {
        let parsed: L = variant.as_str().parse().unwrap();
        assert_eq!(parsed, variant);
    }
}

// =================================================================
// RP — Cluster-wide cohort-restore sweep (decoupled from drain)
//
// A started/paused sandbox evacuated off a draining node is durably
// suspended to S3. The cohort restore is DECOUPLED from the source node's
// drain: the source drains as soon as its snapshots are uploaded (the data
// is durable in S3), and the restore onto the target cohort is driven by a
// cluster-wide sweep (the fallback for the snapshot-register trigger), so it
// completes even after the source terminates. These tests pin: lifecycle
// preserved, configured cohort placement applied, source drains
// independently of the restore, restart-safe dedup + retry.
// =================================================================

fn cohort_placement() -> EvacuationPlacement {
    EvacuationPlacement {
        cohort_key: "space.pplx.net/rollout-cohort".to_string(),
        cohort_value: "test-2".to_string(),
    }
}

fn config_with_placement(placement: EvacuationPlacement) -> ReconcilerConfig {
    let mut config = config_enabled(4, 8);
    config.drain_evacuation_placement = Some(placement);
    config
}

/// rp1: a `started` sandbox suspended mid-evacuation is restored onto a
/// cohort target node — `started → started`, with the configured placement —
/// once its snapshot finalizes `ready`, by the cluster-wide sweep. No
/// draining node is involved (the restore is decoupled from drain). Tick 1
/// (still uploading → nothing pending) drives no restore; tick 2 (ready)
/// drives it. Without the sweep the sandbox is never restored.
#[tokio::test]
async fn rp1_evacuation_restored_onto_cohort_when_snapshot_ready() {
    let sandbox = SandboxId::new();
    let placement = cohort_placement();
    let sink = FakeSink::new();
    // Tick 1: snapshot still uploading — nothing restore-pending.
    sink.enqueue_restore_pending(vec![]);
    // Tick 2: snapshot ready → the sandbox surfaces for the cohort restore.
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);

    let reconciler = Reconciler::new(
        config_with_placement(placement.clone()),
        sink,
        FakeTerminator::default(),
    );

    // Tick 1: upload in flight — no restore.
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_restores, 0);
    assert!(reconciler.sink.restored_evacuations().is_empty());

    // Tick 2: snapshot ready — cohort restore driven, lifecycle preserved.
    let r2 = reconciler.tick().await;
    assert_eq!(r2.spawned_restores, 1);
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(
        reconciler.sink.restored_evacuations(),
        vec![(sandbox, SandboxLifecycle::Started, Some(placement))]
    );
}

/// rp2: lifecycle preservation — a `paused` sandbox is restored back to
/// `paused` (the restore RPC carries `target_state = Paused`), never
/// silently resumed to `started`.
#[tokio::test]
async fn rp2_evacuated_paused_restored_lifecycle_preserving() {
    let sandbox = SandboxId::new();
    let placement = cohort_placement();
    let sink = FakeSink::new();
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Paused)]);

    let reconciler = Reconciler::new(
        config_with_placement(placement.clone()),
        sink,
        FakeTerminator::default(),
    );

    reconciler.tick().await;
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(
        reconciler.sink.restored_evacuations(),
        vec![(sandbox, SandboxLifecycle::Paused, Some(placement))]
    );
}

/// rp3: the source node drains as soon as its sandboxes are **uploaded**,
/// independent of whether the cohort restore has completed. A draining node
/// with no remaining blockers (snapshots uploaded) drains in the same tick
/// that the cluster-wide sweep is still driving the (gated, in-flight)
/// restore. This is the decouple invariant: drain completes on upload, not
/// on restore.
#[tokio::test]
async fn rp3_source_drains_once_uploaded_independent_of_restore() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    // Node has no resident sandboxes and no drain blockers (its sandbox is
    // already uploaded), so it is eligible to drain. The cohort restore is
    // separately pending and gated so it stays in flight during the tick.
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.gate_restores();

    let reconciler = Reconciler::new(
        config_with_placement(cohort_placement()),
        sink,
        FakeTerminator::default(),
    );

    let r1 = reconciler.tick().await;
    assert_eq!(
        r1.spawned_restores, 1,
        "cohort restore spawned by the sweep"
    );
    assert_eq!(
        r1.drained_transitions, 1,
        "source node drains on upload, not gated on the in-flight cohort restore"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );

    // The restore is still in flight (gated) — decoupled from the now-drained
    // source node. Release it so the test doesn't leak a pending task.
    reconciler.sink.release_restore_permits(1);
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(reconciler.sink.restored_evacuations().len(), 1);
}

/// rp4: a cohort restore that fails clears `restore_in_flight` so the next
/// sweep retries.
#[tokio::test]
async fn rp4_restore_failure_clears_inflight_for_retry() {
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.fail_restore(sandbox, EvacuateError::Restore("boom".into()));

    let reconciler = Reconciler::new(
        config_with_placement(cohort_placement()),
        sink,
        FakeTerminator::default(),
    );

    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_restores, 1);
    wait_restore_in_flight_empty(&reconciler).await;
    // The restore was attempted (and recorded) but failed; restore_in_flight
    // is clear so the next sweep re-selects and retries.
    assert_eq!(reconciler.sink.restored_evacuations().len(), 1);
    assert!(reconciler.restore_in_flight.lock().await.is_empty());
}

/// rp5: a restore already in flight is not re-spawned by a later sweep
/// (dedup via `restore_in_flight`), so a slow restore spanning ticks — or a
/// concurrent snapshot-register trigger — drives each sandbox exactly once.
#[tokio::test]
async fn rp5_in_flight_restore_not_respawned() {
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    // Both ticks see the sandbox restore-pending.
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.gate_restores();

    let reconciler = Reconciler::new(
        config_with_placement(cohort_placement()),
        sink,
        FakeTerminator::default(),
    );

    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_restores, 1);
    // Tick 2: the restore is still in flight (gated) → not re-spawned.
    let r2 = reconciler.tick().await;
    assert_eq!(r2.spawned_restores, 0);

    reconciler.sink.release_restore_permits(1);
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(reconciler.sink.restored_evacuations().len(), 1);
}

/// rp6: suspend and restore share the one `max_per_cluster` budget. With the
/// cap at 5 and 3 suspends already in flight on a draining node, the same
/// tick's restore sweep may only spawn 2 (not a fresh 5) — otherwise total
/// drain-driven substrate concurrency would be `2 * max_per_cluster`.
#[tokio::test]
async fn rp6_restore_shares_cluster_budget_with_suspend() {
    let node = NodeId::new();
    let suspendable: Vec<SandboxId> = (0..3).map(|_| SandboxId::new()).collect();
    let pending: Vec<(SandboxId, SandboxLifecycle)> = (0..5)
        .map(|_| (SandboxId::new(), SandboxLifecycle::Started))
        .collect();

    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, suspendable);
    sink.enqueue_restore_pending(pending);
    // Hold both phases in flight so their slots both count against the budget.
    sink.gate_evacuations();
    sink.gate_restores();

    // max_per_node high enough not to bind; cluster budget = 5.
    let mut config = config_with_placement(cohort_placement());
    config.max_per_node = 10;
    config.max_per_cluster = 5;
    let reconciler = Reconciler::new(config, sink, FakeTerminator::default());

    let report = reconciler.tick().await;
    assert_eq!(
        report.spawned_evacuations, 3,
        "all 3 suspends fit the budget"
    );
    assert_eq!(
        report.spawned_restores, 2,
        "restores get only the 2 remaining cluster slots, not a fresh 5"
    );

    reconciler.sink.release_evacuation_permits(3);
    reconciler.sink.release_restore_permits(2);
    wait_in_flight_empty(&reconciler).await;
    wait_restore_in_flight_empty(&reconciler).await;
}

/// rp7: the stranded-`suspending` re-drive runs on every restore sweep and
/// feeds the recovered row into the same pass's cohort restore. Models a
/// mid-suspend controller crash: tick 1 the upload is still in flight (re-drive
/// advances nothing, nothing restore-pending); tick 2 the snapshot has
/// finalized `ready`, so the re-drive advances the stranded row to `suspended`
/// and the restore-pending scan (which only sees `suspended` rows in
/// production) then drives the cohort restore. The re-drive must be invoked
/// once per sweep regardless of whether it advanced anything.
#[tokio::test]
async fn rp7_redrive_stranded_suspending_feeds_restore() {
    let sandbox = SandboxId::new();
    let placement = cohort_placement();
    let sink = FakeSink::new();

    // Tick 1: upload in flight — re-drive advances nothing, nothing pending.
    sink.enqueue_redrive_stranded(vec![]);
    sink.enqueue_restore_pending(vec![]);
    // Tick 2: snapshot ready — re-drive advances the stranded row to
    // `suspended`, and (modeling the DB coupling) the restore-pending scan now
    // surfaces it for the cohort restore.
    sink.enqueue_redrive_stranded(vec![sandbox]);
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);

    let reconciler = Reconciler::new(
        config_with_placement(placement.clone()),
        sink,
        FakeTerminator::default(),
    );

    // Tick 1: still uploading — no restore, but the re-drive was consulted.
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_restores, 0);
    assert!(reconciler.sink.restored_evacuations().is_empty());
    assert_eq!(
        reconciler.sink.redrive_stranded_call_count(),
        1,
        "re-drive must run on every restore sweep, even when it advances nothing"
    );

    // Tick 2: snapshot ready — the recovered row is restored, lifecycle preserved.
    let r2 = reconciler.tick().await;
    assert_eq!(r2.spawned_restores, 1);
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(
        reconciler.sink.restored_evacuations(),
        vec![(sandbox, SandboxLifecycle::Started, Some(placement))]
    );
    assert_eq!(
        reconciler.sink.redrive_stranded_call_count(),
        2,
        "second sweep consulted the re-drive again"
    );
}

/// rp8: under slot pressure the restore budget is filled in list order. The
/// sink's restore-pending scan returns the list ordered by attempt tiers,
/// then most-recently-active first, so with more pending restores than free
/// cluster slots the front of the list wins the slots and the tail waits for
/// a later sweep — this is the consumer half of the ordering contract pinned
/// on the SQL side by `restore_pending_orders_attempt_tiers_then_recency`.
#[tokio::test]
async fn rp8_restore_slots_filled_in_list_order() {
    let hot = SandboxId::new();
    let warm = SandboxId::new();
    let cold = SandboxId::new();

    let sink = FakeSink::new();
    sink.enqueue_restore_pending(vec![
        (hot, SandboxLifecycle::Started),
        (warm, SandboxLifecycle::Paused),
        (cold, SandboxLifecycle::Started),
    ]);

    let mut config = config_with_placement(cohort_placement());
    config.max_per_cluster = 2;
    let reconciler = Reconciler::new(config, sink, FakeTerminator::default());

    let report = reconciler.tick().await;
    assert_eq!(
        report.spawned_restores, 2,
        "only the 2 cluster slots may be spawned"
    );
    wait_restore_in_flight_empty(&reconciler).await;

    let restored: Vec<SandboxId> = reconciler
        .sink
        .restored_evacuations()
        .into_iter()
        .map(|(id, _, _)| id)
        .collect();
    // Spawn order is deterministic (list order) but task completion order is
    // not, so assert the *set* that won the slots rather than the sequence.
    assert_eq!(restored.len(), 2);
    assert!(
        restored.contains(&hot) && restored.contains(&warm),
        "the front of the list must win the scarce slots; got {restored:?}"
    );
    assert!(
        !restored.contains(&cold),
        "the tail must wait for a later sweep; got {restored:?}"
    );
}

/// rp9: a cohort restore that resolves `Converging` (the wake wait hit the
/// substrate's node-RPC blocking budget while the node was still restoring)
/// is not a failure: it clears `restore_in_flight` so the next sweep
/// re-drives the same sandbox, and the re-driven attempt — the node having
/// converged in the meantime — completes the restore.
#[tokio::test]
async fn rp9_converging_restore_redriven_until_restored() {
    let sandbox = SandboxId::new();
    let placement = cohort_placement();
    let sink = FakeSink::new();
    // Both sweeps see the sandbox pending: after a Converging attempt the row
    // stays `restoring` with the evacuation marker set, so production's
    // pending scan (`lifecycle IN ('suspended', 'restoring')` + marker)
    // re-lists it.
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.converge_restore(sandbox);

    let reconciler = Reconciler::new(
        config_with_placement(placement.clone()),
        sink,
        FakeTerminator::default(),
    );

    // Sweep 1: the attempt runs, hits the wait budget, and ends Converging —
    // the in-flight slot is released for the re-drive.
    let r1 = reconciler.tick().await;
    assert_eq!(r1.spawned_restores, 1);
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(reconciler.sink.restored_evacuations().len(), 1);

    // Sweep 2: the same sandbox is re-driven and the attempt now restores.
    let r2 = reconciler.tick().await;
    assert_eq!(
        r2.spawned_restores, 1,
        "converging sandbox must be re-driven by the next sweep"
    );
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(
        reconciler.sink.restored_evacuations(),
        vec![
            (sandbox, SandboxLifecycle::Started, Some(placement.clone())),
            (sandbox, SandboxLifecycle::Started, Some(placement)),
        ],
        "both attempts targeted the same sandbox with preserved lifecycle"
    );
}

/// rp10: a failed cohort restore schedules the exponential retry backoff with
/// the configured base/cap, so the sweep stops re-selecting the failer every
/// tick. This is the policy half of the fix; the SQL half (the pending
/// query's `next_restore_attempt_at` gate and attempt-count ordering) is
/// pinned by the Postgres-backed tests in `store.rs`.
#[tokio::test]
async fn rp10_restore_failure_schedules_retry_backoff() {
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.fail_restore(sandbox, EvacuateError::Restore("boom".into()));

    let mut config = config_enabled(4, 8);
    let base = PositiveDuration::new_panic(Duration::from_secs(60));
    let cap = PositiveDuration::new_panic(Duration::from_secs(1800));
    config.restore_backoff_base = Some(base);
    config.restore_backoff_cap = cap;
    let reconciler = Reconciler::new(config, sink, FakeTerminator::default());

    reconciler.tick().await;
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(
        reconciler.sink.restore_retry_calls(),
        vec![(sandbox, base, cap)],
        "the failure must schedule exactly one retry backoff with the configured base/cap"
    );
    assert_eq!(
        reconciler.sink.restore_pending_honor_flags(),
        vec![true],
        "with the backoff enabled, the sweep must honor backoff windows"
    );
}

/// rp11: a successful cohort restore never touches the retry backoff — the
/// backoff state is reset by the marker-clear trigger on the finalize, and
/// scheduling a retry for a success would delay nothing but plant stale
/// attempt state.
#[tokio::test]
async fn rp11_restore_success_schedules_no_retry_backoff() {
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());

    reconciler.tick().await;
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(reconciler.sink.restored_evacuations().len(), 1);
    assert!(
        reconciler.sink.restore_retry_calls().is_empty(),
        "a successful restore must not schedule a retry backoff"
    );
}

/// rp12: a `Deferred` outcome (user takeover cleared the marker / snapshot
/// no longer eligible mid-flight) is not a failure: nothing was restored,
/// but nothing failed either, so no retry backoff is scheduled — the row is
/// the user's now (or the next sweep re-evaluates cleanly).
#[tokio::test]
async fn rp12_deferred_restore_schedules_no_retry_backoff() {
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.defer_restore(sandbox);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());

    reconciler.tick().await;
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(reconciler.sink.restored_evacuations().len(), 1);
    assert!(
        reconciler.sink.restore_retry_calls().is_empty(),
        "a deferred (user-takeover) outcome must not schedule a retry backoff"
    );
}

/// rp13: `restore_backoff_base = None` (env `0`) disables the backoff —
/// failures keep the prior retry-next-sweep behavior and never write
/// attempt state, and the sweep's pending listing is asked to bypass any
/// stale backoff windows written before the disable.
#[tokio::test]
async fn rp13_backoff_disabled_schedules_no_retry() {
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.enqueue_restore_pending(vec![(sandbox, SandboxLifecycle::Started)]);
    sink.fail_restore(sandbox, EvacuateError::Restore("boom".into()));

    let mut config = config_enabled(4, 8);
    config.restore_backoff_base = None;
    let reconciler = Reconciler::new(config, sink, FakeTerminator::default());

    reconciler.tick().await;
    wait_restore_in_flight_empty(&reconciler).await;
    assert_eq!(reconciler.sink.restored_evacuations().len(), 1);
    assert!(
        reconciler.sink.restore_retry_calls().is_empty(),
        "with the backoff disabled, a failure must not write attempt state"
    );
    assert_eq!(
        reconciler.sink.restore_pending_honor_flags(),
        vec![false],
        "with the backoff disabled, the sweep must bypass stale backoff windows"
    );
}

// =================================================================
// G — KubeNodeGone recovery for draining nodes
// =================================================================

/// Build a recovery-capable reconciler: enabled config + the given fakes.
fn reconciler_with_lookup(
    sink: FakeSink,
    lookup: Arc<FakeInstanceLookup>,
) -> Reconciler<FakeSink, FakeTerminator> {
    Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default())
        .with_instance_lookup(Some(lookup as Arc<dyn InstanceStateLookup>))
}

#[tokio::test]
async fn g1_gone_node_without_lookup_parks_and_keeps_promoting() {
    // Fail-safe: without an EC2 cross-check there is no way to distinguish a
    // terminated instance from a manually deregistered live one, so the node
    // parks and the per-sandbox work keeps its pre-recovery retry behavior.
    let node = NodeId::new();
    let pinned = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    sink.enqueue_pinned_local_only(node, vec![pinned]);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_parked, 1);
    assert_eq!(report.kube_node_gone_failed_sandboxes, 0);
    assert!(reconciler.sink.failed_on_gone_node_calls().is_empty());
    // The promote path still runs (the prior behavior, fail-safe).
    assert_eq!(reconciler.sink.promoted(), vec![(pinned, node)]);
    assert!(reconciler.sink.marked_lost_on_gone_node().is_empty());
}

#[tokio::test]
async fn g2_gone_node_with_live_instance_parks_and_keeps_promoting() {
    // A live-but-deregistered instance is the genuinely interesting manual
    // case (kube Node deleted on a running box): never auto-give-up.
    let node = NodeId::new();
    let pinned = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    sink.enqueue_pinned_local_only(node, vec![pinned]);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.set_live(node);

    let reconciler = reconciler_with_lookup(sink, lookup.clone());
    let report = reconciler.tick().await;

    assert_eq!(lookup.calls(), vec![node]);
    assert_eq!(report.kube_node_gone_parked, 1);
    assert!(reconciler.sink.failed_on_gone_node_calls().is_empty());
    assert_eq!(reconciler.sink.promoted(), vec![(pinned, node)]);
    assert!(reconciler.sink.adopted_remote_snapshot().is_empty());
    assert!(reconciler.sink.adopted_on_gone_node().is_empty());
    assert!(reconciler.sink.marked_lost_on_gone_node().is_empty());
}

#[tokio::test]
async fn g3_gone_node_lookup_error_parks_and_keeps_promoting() {
    let node = NodeId::new();
    let pinned = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    sink.enqueue_pinned_local_only(node, vec![pinned]);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.fail(node);

    let reconciler = reconciler_with_lookup(sink, lookup);
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_parked, 1);
    assert!(reconciler.sink.failed_on_gone_node_calls().is_empty());
    assert_eq!(reconciler.sink.promoted(), vec![(pinned, node)]);
}

#[tokio::test]
async fn g4_gone_node_terminated_bulk_fails_and_adopts_without_promote_rpcs() {
    // EC2 confirms the instance terminated: residents are bulk-failed under
    // the observed kube uid, the promote RPCs are skipped wholesale, and the
    // pinned local-only sandbox adopts its ready remote snapshot.
    let node = NodeId::new();
    let resident = SandboxId::new();
    let pinned = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    // The resident row models a sandbox whose bulk-fail flip is observed by
    // this tick's (post-flip) listings: resident still lists it (it was
    // resident at flip time per the production query's snapshot), but the
    // evacuatable listing is explicitly empty.
    sink.enqueue_resident(node, vec![resident]);
    sink.enqueue_evacuatable(node, vec![]);
    sink.enqueue_fail_on_gone_node(node, 1);
    sink.enqueue_pinned_local_only(node, vec![pinned]);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.set_gone(node, Some("uid-1"));

    let reconciler = reconciler_with_lookup(sink, lookup.clone());
    let report = reconciler.tick().await;

    assert_eq!(lookup.calls(), vec![node]);
    assert_eq!(report.kube_node_gone_parked, 0);
    assert_eq!(report.kube_node_gone_failed_sandboxes, 1);
    assert_eq!(
        reconciler.sink.failed_on_gone_node_calls(),
        vec![(node, Some("uid-1".to_string()))]
    );
    // No node-bound RPCs: neither promote nor evacuate fired.
    assert!(reconciler.sink.promoted().is_empty());
    assert_eq!(report.spawned_evacuations, 0);
    // The pinned row adopted its ready remote snapshot (FakeSink default),
    // through the gone-node adopt variant — never the reupload-path CAS,
    // whose guards lack the identity defense — and carrying the uid the
    // EC2 cross-check observed.
    assert!(reconciler.sink.adopted_remote_snapshot().is_empty());
    let adopted = reconciler.sink.adopted_on_gone_node();
    assert_eq!(adopted.len(), 1);
    assert_eq!((adopted[0].0, adopted[0].1), (pinned, node));
    assert_eq!(
        adopted[0].2,
        PositiveDuration::new_panic(Duration::from_secs(DEFAULT_REUPLOAD_GIVEUP_SETTLE_SECS))
    );
    assert_eq!(adopted[0].3.as_deref(), Some("uid-1"));
    assert_eq!(report.kube_node_gone_adopted_sandboxes, 1);
    // Adopt succeeded, so the mark-lost fallback never ran.
    assert!(reconciler.sink.marked_lost_on_gone_node().is_empty());
    assert_eq!(report.kube_node_gone_lost_sandboxes, 0);
}

#[tokio::test]
async fn g5_gone_node_adopt_miss_falls_back_to_mark_lost() {
    // No ready remote snapshot: the adopt CAS reports 0 rows, and the
    // stricter gone-node mark-lost flips the row to failed. The legacy
    // `mark_sandbox_lost` (artifact-missing path) must NOT be used here —
    // its freshness-anchored guard could fail an adoptable row.
    let node = NodeId::new();
    let pinned = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    sink.enqueue_pinned_local_only(node, vec![pinned]);
    sink.noop_adopt_remote_snapshot(pinned);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.set_gone(node, None);

    let reconciler = reconciler_with_lookup(sink, lookup);
    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.marked_lost_on_gone_node(),
        vec![(pinned, node, None)]
    );
    assert!(reconciler.sink.marked_lost().is_empty());
    assert!(reconciler.sink.adopted_remote_snapshot().is_empty());
    assert_eq!(report.kube_node_gone_adopted_sandboxes, 0);
    assert_eq!(report.kube_node_gone_lost_sandboxes, 1);
}

#[tokio::test]
async fn g6_gone_node_giveup_disabled_leaves_pinned_rows_alone() {
    // `reupload_giveup_settle = None` disables every automatic give-up:
    // no adopt, no mark-lost — and no futile promote RPCs either (the node
    // is confirmed terminated, so the enqueue can never be received).
    let node = NodeId::new();
    let pinned = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    sink.enqueue_pinned_local_only(node, vec![pinned]);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.set_gone(node, None);

    let mut config = config_enabled(4, 8);
    config.reupload_giveup_settle = None;
    let reconciler = Reconciler::new(config, sink, FakeTerminator::default())
        .with_instance_lookup(Some(lookup as Arc<dyn InstanceStateLookup>));
    let report = reconciler.tick().await;

    assert!(reconciler.sink.adopted_remote_snapshot().is_empty());
    assert!(reconciler.sink.adopted_on_gone_node().is_empty());
    assert!(reconciler.sink.marked_lost_on_gone_node().is_empty());
    assert!(reconciler.sink.promoted().is_empty());
    assert_eq!(report.kube_node_gone_adopted_sandboxes, 0);
    assert_eq!(report.kube_node_gone_lost_sandboxes, 0);
}

#[tokio::test]
async fn g7_gone_node_recovery_converges_to_drained() {
    // End-to-end: tick 1 gives up the pinned row (adopt), tick 2 observes
    // zero residents / zero blockers and flips the node to drained — the
    // state `drained_node_terminator`'s own KubeNodeGone path reaps.
    let node = NodeId::new();
    let pinned = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    sink.enqueue_pinned_local_only(node, vec![pinned]);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.set_gone(node, Some("uid-1"));

    let reconciler = reconciler_with_lookup(sink, lookup);

    let r1 = reconciler.tick().await;
    assert_eq!(r1.kube_node_gone_adopted_sandboxes, 1);
    assert_eq!(r1.drained_transitions, 0);

    // Tick 2: the adopted row dropped out of the pinned/blocker sets.
    let r2 = reconciler.tick().await;
    assert_eq!(r2.drained_transitions, 1);
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINED)
    );
}

#[tokio::test]
async fn g8_gone_node_never_spawns_evacuations_even_when_bulk_fail_refuses() {
    // The evacuate claim guards only on `active_node_id` + lifecycle, and a
    // same-name replacement node reuses the same `nodes.id` — so when the
    // bulk fail's identity guards refuse (uid bumped, admin undrain) and
    // started/paused rows are still listed, spawning evacuations would
    // suspend the replacement's fresh sandboxes (or fire futile RPCs at the
    // dead node). The gone branch must never spawn node-bound evacuations.
    let node = NodeId::new();
    let resident = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    // Bulk fail refuses (no enqueued return -> 0 rows), and the listings
    // still surface an evacuable resident.
    sink.enqueue_resident(node, vec![resident]);
    sink.enqueue_evacuatable(node, vec![resident]);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.set_gone(node, Some("uid-1"));

    let reconciler = reconciler_with_lookup(sink, lookup);
    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.failed_on_gone_node_calls(),
        vec![(node, Some("uid-1".to_string()))]
    );
    assert_eq!(report.spawned_evacuations, 0);
    wait_in_flight_empty(&reconciler).await;
    assert!(
        reconciler.sink.evacuated_ids().is_empty(),
        "a gone node must never get node-bound evacuation RPCs"
    );
}

#[tokio::test]
async fn g9_gone_node_terminalizes_inflight_snapshot_uploads() {
    // A `creating` suspend-snapshot upload sourced from the dead node can
    // never finalize (its worker died with the instance), yet the drain
    // blocker count's in-flight-upload term counts it regardless of the
    // owning sandbox's lifecycle — so the recovery must terminalize those
    // rows itself (under the observed uid), not wait on the stuck-snapshot
    // reaper's age window.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list(vec![node]);
    sink.set_cordon_kube_node_gone(node);
    let lookup = Arc::new(FakeInstanceLookup::default());
    lookup.set_gone(node, Some("uid-1"));

    let reconciler = reconciler_with_lookup(sink, lookup);
    reconciler.tick().await;

    assert_eq!(
        reconciler.sink.failed_inflight_snapshots_calls(),
        vec![(node, Some("uid-1".to_string()))]
    );
}

// ---------------------------------------------------------------------------
// A failing node-release read must not stop the evacuation drive.
//
// The property under test is stated in operator terms on purpose: *a forced
// drain must keep suspending its live residents even while a step of the
// release path is failing*. It is deliberately not "`spawn_evacuations` is
// called before the blocker count", which would only restate the call order
// this change introduced — the ordering is the mechanism, the continued
// evacuation is the behaviour anyone drains a node for.
// ---------------------------------------------------------------------------

/// A timeout in `list_pinned_local_only_sandboxes_on_node` must not return
/// before the evacuation drive and leave live residents unsuspended. Failed
/// enumeration defers only promotion work, which is retried next tick.
#[tokio::test]
async fn p1_forced_drain_evacuates_residents_while_pinned_enumeration_fails() {
    let node = NodeId::new();
    let resident = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![resident]);
    sink.fail_pinned_listing(node);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.spawned_evacuations, 1,
        "a forced drain must still evacuate its residents when the pinned \
         local-only enumeration fails"
    );
    assert!(
        reconciler.sink.promoted().is_empty(),
        "with no enumerated work queue there is nothing to promote"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING),
        "the node must stay in `draining`: a failed read is not evidence that \
         releasing it is safe"
    );
}

/// The same property for the read on the *other* side of the promotion step.
/// `count_drain_blocking_sandboxes_on_node` is the release gate's own input, so
/// its failure must keep the node in `draining` — but it still must not stop
/// the evacuation drive, which is what actually empties the node.
#[tokio::test]
async fn p2_forced_drain_evacuates_residents_while_blocker_count_fails() {
    let node = NodeId::new();
    let resident = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, vec![resident]);
    sink.enqueue_pinned_local_only(node, Vec::new());
    sink.fail_blocker_count(node);

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.spawned_evacuations, 1,
        "a failing drain-blocker count must not stop the evacuation drive"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING),
        "an unreadable blocker count must never be treated as zero blockers"
    );
    assert_eq!(
        report.drained_transitions, 0,
        "the release gate must refuse while its own input is unreadable"
    );
}

/// A forced drain that genuinely has **zero** pinned local snapshots reaches
/// the evacuation drive rather than returning early. An empty result can
/// still be expensive to compute, so both successful empty enumeration and
/// enumeration failure need to preserve the evacuation drive.
///
/// The fixture seeds three residents rather than none: with an empty resident
/// list `spawned_evacuations` would be 0 whether or not the drive was reached,
/// and the assertion would hold for the wrong reason.
///
/// Unlike [`p1_forced_drain_evacuates_residents_while_pinned_enumeration_fails`]
/// this one also passes against the pre-change tick, because a *successful*
/// enumeration returning zero always reached the drive. It pins the criterion,
/// not the fix; the query that made the zero case unreachable is regressed
/// against in `store::pinned_local_only_scale_tests`.
#[tokio::test]
async fn p3_forced_drain_with_zero_pinned_local_snapshots_reaches_evacuations() {
    let node = NodeId::new();
    let residents: Vec<SandboxId> = (0..3).map(|_| SandboxId::new()).collect();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    sink.enqueue_resident(node, residents.clone());
    sink.enqueue_pinned_local_only(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.spawned_evacuations, 3,
        "zero pinned local snapshots must not short-circuit the evacuation drive"
    );
    assert!(
        reconciler.sink.promoted().is_empty(),
        "nothing pinned means no promotion RPCs"
    );
}

/// The other direction, which p1–p3 do not cover: a tick that *spawns* an
/// evacuation must not also complete the drain.
///
/// `resident.occupying` and `evacuatable` are separate round trips through the
/// same `occupies_node_predicate`, and under `NodeCensusAware` that predicate
/// is `NOT node_proves_sandbox_absent(..)` — a function of the node's latest
/// census report, so it is time-varying. This fixture is that disagreement made
/// concrete: the resident count reads zero (the census proved the sandbox
/// absent) and the evacuatable listing then names it (a fresh census landed
/// between the two reads). With the pinned list and the blocker count both
/// empty, every input to the release gate reads "ready" — so only the
/// spawned-this-tick term stops the node committing `drained` on the tick that
/// launched a suspend off it.
#[tokio::test]
async fn p4_a_tick_that_spawns_an_evacuation_cannot_also_reach_drained() {
    let node = NodeId::new();
    let sandbox = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_initial_state(node, PHASE_DRAINING);
    sink.set_draining_list_forced(vec![node]);
    // The census-absent reading: nothing resident.
    sink.enqueue_resident(node, Vec::new());
    // The post-census reading: the sandbox is evacuatable after all.
    sink.enqueue_evacuatable(node, vec![sandbox]);
    sink.enqueue_pinned_local_only(node, Vec::new());

    let reconciler = Reconciler::new(config_enabled(4, 8), sink, FakeTerminator::default());
    let report = reconciler.tick().await;

    assert_eq!(
        report.spawned_evacuations, 1,
        "fixture must actually spawn, or the assertion below holds for the \
         wrong reason"
    );
    assert_eq!(
        report.drained_transitions, 0,
        "a tick that spawned an evacuation must not also advance the node to \
         `drained`: the suspend it launched is still in flight from this node"
    );
    assert_eq!(
        reconciler.sink.current_state(node).as_deref(),
        Some(PHASE_DRAINING),
    );
}
