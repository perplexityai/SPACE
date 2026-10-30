//! # Test matrix
//!
//! Every behavior the sweeper must exhibit maps to ≥1 test below.
//!
//! ## C — `ReconcilerConfig` + env parsing
//! - C1 `c1_resolve_age_uses_default_for_unset_override`
//! - C2 `c2_resolve_age_uses_override_when_set`
//! - C3 `c3_disabled_lifecycle_resolves_to_none`
//! - C4 `c4_non_sweepable_lifecycle_resolves_to_none`
//! - C5 `c5_started_is_not_sweepable_so_slow_creates_survive`
//!
//! ## T — Tick behavior
//! - T1 `t1_no_wedged_rows_emits_empty_report`
//! - T2 `t2_wedged_row_in_each_lifecycle_aged_out`
//! - T3 `t3_disabled_lifecycle_skipped_entirely`
//! - T4 `t4_cleanup_failure_increments_metric_but_continues`
//! - T5 `t5_db_error_on_list_does_not_block_other_lifecycles`
//! - T6 `t6_failed_cleanup_cleans_listed_active_executions`
//! - T7 `t7_failed_cleanup_keeps_going_after_cleanup_failure`
//! - T8 `t8_failed_cleanup_rotates_and_does_not_starve_later_ids`
//!
//! ## V — Verify-before-destroy (probe -> classify -> act)
//! - V1 `v1_alive_restoring_converges_to_wake_target`
//! - V2 `v2_alive_non_restoring_defers`
//! - V3 `v3_dormant_restoring_with_ready_snapshot_reverts_and_retires`
//! - V4 `v4_dormant_artifact_without_backup_defers`
//! - V5 `v5_missing_restoring_with_ready_snapshot_reverts`
//! - V6 `v6_probe_error_defers_everything_on_that_node`
//! - V7 `v7_no_execution_row_is_missing_on_node`
//! - V8 `v8_lost_cas_skips_cleanup_and_counters`
//! - V9 `v9_same_node_candidates_share_one_probe`
//! - V10 `v10_failed_cleanup_defers_when_probe_says_alive`
//! - V11 `v11_dormant_non_restoring_with_ready_snapshot_defers`
//! - V12 `v12_dead_non_restoring_with_ready_snapshot_fails_loudly`
//! - V13 `v13_probe_answer_missing_id_defers`
//! - V14 `v14_probe_budget_exhaustion_defers_unprobed_nodes`
//! - V15 `v15_failed_cleanup_probe_budget_defers`
//! - V16 `v16_revert_cleanup_failure_is_counted_but_revert_stands`
//! - V17 `v17_failed_cleanup_retires_dormant_rows_and_defers_inconclusive`
//! - V18 `v18_failed_cleanup_repairs_when_node_reports_started`
//! - V19 `v19_failed_cleanup_defers_live_node_failed_without_retire`
//! - V20 `v20_failed_cleanup_repair_cas_lost_skips_cleanup`
//! - V21 `v21_failed_cleanup_defer_reason_labels_are_stable`
//! - V22 `v22_alive_unroutable_restoring_hydrates_then_converges`
//! - V23 `v23_alive_unroutable_restoring_without_pod_defers_no_converge`
//! - V24 `v24_alive_routable_restoring_skips_routability_consult`
//! - V25 `v25_routability_check_error_defers_without_converge`
//!
//! ## Crashed create — see `tests/crashed_create.rs`
//! An alive `creating` row whose owner's activation lease expired a full age
//! window ago fails and retires; one whose lease is live, or expired more
//! recently, is never listed or failed.
//!
//! ## CRP — command-ready-pending reaper (heal vs reap)
//! - CRP1 `crp1_ready_probe_heals_marker_without_reaping`
//! - CRP2 `crp2_wedged_probe_reaps_and_cleans_up`
//! - CRP3 `crp3_disabled_reaper_is_a_noop`
//! - CRP4 `crp4_unclaimed_reap_cas_skips_cleanup`
//! - CRP5 `crp5_mixed_batch_heals_ready_and_reaps_wedged`
//! - CRP6 `crp6_unreachable_probe_defers_without_reaping`
//! - CRP7 `crp7_unreachable_past_terminal_age_is_reaped`
//!
//! ## F — Pod-identity fence on the retire
//! - F1 `f1_every_retire_carries_the_observed_pod_identity`
//! - F2 `f2_pod_identity_defer_is_not_a_cleanup_failure`
//! - F3 `f3_failed_cleanup_pod_identity_defer_is_not_a_failure`
//! - F4 `f4_command_ready_reap_defers_on_pod_identity_without_failing`
//! - F5 `f5_only_an_observed_pod_yields_a_verifiable_fence`
//!
//! ## R — Postgres-backed sink (drift safety + trigger semantics)
//! - R1 `r1_candidate_select_and_fail_cas_leave_drifted_rows_alone`
//! - R2 `r2_trigger_resets_lifecycle_changed_at_on_lifecycle_change`
//! - R3 `r3_trigger_does_not_touch_lifecycle_changed_at_on_unrelated_update`
//! - R4 `r4_candidate_select_scoped_to_substrate_managed_by`
//! - R5 `r5_failed_cleanup_scans_only_k8s_failed_active_executions_and_rotates`
//! - R6 `r6_evacuation_exemption_spares_restoring_but_not_suspending`
//! - R7 `r7_suspending_candidates_guard_inflight_evacuation_upload`
//! - R8 `r8_command_ready_pending_list_heal_reap`
//! - R9 `r9_candidate_select_resolves_node_and_snapshot_facts`
//! - R10 `r10_converge_cas_respects_restore_lease`
//! - R11 `r11_revert_cas_requires_ready_snapshot_with_artifact_identity`
//! - R12 `r12_revert_cas_is_restoring_only_even_with_exact_pin_backup`
//! - R13 `r13_revert_and_fail_cas_respect_restore_lease`
//! - R14 `r14_restoring_snapshot_fact_survives_failed_restore_placement_bump`
//! - R15 `r15_revert_cas_refuses_late_branch_restore_artifact`
//! - R16 `r16_failed_active_repair_cas_requires_k8s_failed_active_execution`
//! - R17 `r17_candidate_select_resolves_routable_execution_fact`
//! - R18 `r18_converge_cas_requires_routable_execution_at_flip_time`
//! - R19 `r19_candidate_select_carries_active_execution_pod_identity`
//! - R20 `r20_stranded_stopping_row_without_a_node_is_selected_fenced_and_failed`
//! - R21 `r21_failed_cleanup_statements_skip_direct_rows`
//! - R22 `r22_failed_cleanup_runs_only_on_the_kubernetes_substrate`

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use pplx_space_api::controllers::substrate::{
    CommandReadyProbe, ObservedPodIdentity, PodIdentityFence,
};
use pplx_space_common_core::types::{
    PodName, PodResourceVersion, PodUid, PositiveDuration, SandboxId, SandboxLifecycle,
};
use uuid::Uuid;

use pplx_space_api::controllers::substrate::TeardownProgress;

use super::store::candidate_pod_fence_with_observation;
use super::*;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Default)]
struct FakeSinkState {
    /// `lifecycle -> candidates` returned by that lifecycle's next call to
    /// `list_aged_transient`. Drained on read.
    candidate_queue: HashMap<SandboxLifecycle, Vec<SweepCandidate>>,
    /// Lifecycles whose `list_aged_transient` call should return an error
    /// (exactly once, drained on read).
    db_errors: Vec<SandboxLifecycle>,
    /// Per-sandbox probe verdict. A probed candidate whose verdict is unset
    /// resolves `DeadOnNode` so a test that forgot to wire an outcome fails
    /// loudly (a spurious fail+retire) rather than silently deferring.
    probe_outcomes: HashMap<SandboxId, ProbeObservation>,
    /// Node names whose probe call errors.
    probe_errors: BTreeSet<String>,
    /// Sandbox ids the node's probe answer does not cover: the probe call
    /// succeeds but these ids are omitted from the returned map (a malformed
    /// id echo or node-side skew). Callers must treat the absence as
    /// inconclusive, never as missing-on-node.
    probe_unanswered: BTreeSet<SandboxId>,
    /// Sandbox ids whose converge/revert/fail CAS reports "not claimed" (the
    /// row changed underneath between select and act).
    cas_unclaimed: BTreeSet<SandboxId>,
    /// Per-sandbox act-phase routability outcome. A consulted candidate whose
    /// outcome is unset resolves `NotRoutable` so a test that forgot to wire
    /// an outcome fails loudly (a missing converge) rather than silently
    /// converging an unroutable row.
    routability_outcomes: HashMap<SandboxId, ExecutionRoutability>,
    /// Sandbox ids whose routability check errors.
    routability_errors: BTreeSet<SandboxId>,
    /// Sandbox IDs whose cleanup should return Err.
    cleanup_failures: Vec<SandboxId>,
    /// Sandbox IDs whose retire the substrate refuses on pod identity, with
    /// the mismatch it reports.
    cleanup_pod_identity_changed: HashMap<SandboxId, PodFenceMismatch>,
    /// Failed active execution candidates, in their listed order.
    failed_cleanup_queue: Vec<FailedCleanupCandidate>,
    /// What `first_activation` re-reads, when it differs from the listing.
    first_activation_now: HashMap<SandboxId, Option<FirstActivation>>,
    /// Failed active execution IDs whose cleanup should return Err.
    failed_execution_cleanup_failures: BTreeSet<SandboxId>,
    /// All `(lifecycle, age)` pairs the sweeper listed, in order.
    list_calls: Vec<(SandboxLifecycle, PositiveDuration)>,
    /// All `(node, ids)` probe calls, in order.
    probe_calls: Vec<(String, Vec<SandboxId>)>,
    /// All converge CAS attempts, in order.
    converge_calls: Vec<(SandboxId, SandboxLifecycle)>,
    /// All act-phase routability consults, in order.
    routability_calls: Vec<SandboxId>,
    /// All revert CAS attempts, in order.
    revert_calls: Vec<SandboxId>,
    undo_revert_calls: Vec<SandboxId>,
    undo_reap_calls: Vec<SandboxId>,
    undo_fail_calls: Vec<SandboxId>,
    /// Sandboxes whose pre-check passes even though their retire will refuse —
    /// the pod changing inside the window the pre-check cannot cover.
    fence_lost_after_precheck: HashSet<SandboxId>,
    /// How far the refused teardown got, when a test needs the post-stop case.
    refusal_progress: HashMap<SandboxId, TeardownProgress>,
    /// All fail CAS attempts, in order.
    fail_calls: Vec<SandboxId>,
    fail_guards: Vec<FailGuard>,
    /// All sandbox IDs the sweeper asked to clean up, in order.
    cleanup_calls: Vec<SandboxId>,
    /// The pod-identity fence the sweeper passed with each cleanup call, in
    /// the same order. Kept beside `cleanup_calls` so the many existing
    /// id-only assertions stay unchanged.
    cleanup_fences: Vec<RetireFence>,
    /// All failed active execution rotation calls, in order.
    failed_cleanup_attempts: Vec<SandboxId>,
    /// Failed active execution IDs cleaned successfully, in order.
    failed_execution_cleanup_calls: Vec<SandboxId>,
    /// The pod-identity fence passed with each failed-execution cleanup.
    failed_execution_cleanup_fences: Vec<RetireFence>,
    /// Failed active execution repair CAS attempts, in order.
    failed_active_repair_calls: Vec<SandboxId>,
    /// `command_ready_pending` candidates returned by
    /// `list_command_ready_pending`.
    command_ready_pending_queue: Vec<CommandReadyPendingCandidate>,
    /// Per-id guest readiness probe outcome. Every reaper test sets this
    /// explicitly; a missing id falls back to `Wedged` so a test that forgot
    /// to wire an outcome fails loudly (a spurious reap) rather than
    /// silently deferring.
    command_probe_outcomes: HashMap<SandboxId, CommandReadyProbe>,
    /// IDs whose reap CAS should report "not claimed" (a row a concurrent
    /// access healed or transitioned out of `started` between probe and CAS).
    reap_unclaimed: BTreeSet<SandboxId>,
    /// All heal (marker-clear) calls, in order.
    heal_calls: Vec<SandboxId>,
    /// All reap CAS attempts, in order.
    reap_calls: Vec<SandboxId>,
    /// Unrestorable-`restoring` candidates (id + placement age) returned by
    /// `list_unrestorable_restoring`. Drained on read.
    unrestorable_queue: Vec<(SandboxId, Duration)>,
    /// When true, `list_unrestorable_restoring` errors (once, reset on read).
    unrestorable_list_error: bool,
    /// All unrestorable-list calls' age binds, in order.
    unrestorable_list_calls: Vec<PositiveDuration>,
    /// All unrestorable fail CAS attempts, in order. CAS claims unless the id
    /// is in `cas_unclaimed`.
    unrestorable_fail_calls: Vec<SandboxId>,
}

struct FakeSink(StdMutex<FakeSinkState>);

impl FakeSink {
    fn new() -> Self {
        Self(StdMutex::new(FakeSinkState::default()))
    }

    fn enqueue_candidates(&self, lifecycle: SandboxLifecycle, candidates: Vec<SweepCandidate>) {
        self.0
            .lock()
            .unwrap()
            .candidate_queue
            .insert(lifecycle, candidates);
    }

    fn enqueue_db_error(&self, lifecycle: SandboxLifecycle) {
        self.0.lock().unwrap().db_errors.push(lifecycle);
    }

    fn set_probe_verdict(&self, sandbox_id: SandboxId, verdict: ProbeVerdict) {
        self.0
            .lock()
            .unwrap()
            .probe_outcomes
            .insert(sandbox_id, ProbeObservation::from_verdict(verdict));
    }

    fn set_probe_observation(&self, sandbox_id: SandboxId, observation: ProbeObservation) {
        self.0
            .lock()
            .unwrap()
            .probe_outcomes
            .insert(sandbox_id, observation);
    }

    fn set_probe_error(&self, node_name: &str) {
        self.0
            .lock()
            .unwrap()
            .probe_errors
            .insert(node_name.to_string());
    }

    fn set_probe_unanswered(&self, sandbox_id: SandboxId) {
        self.0.lock().unwrap().probe_unanswered.insert(sandbox_id);
    }

    fn set_cas_unclaimed(&self, sandbox_id: SandboxId) {
        self.0.lock().unwrap().cas_unclaimed.insert(sandbox_id);
    }

    fn set_routability(&self, sandbox_id: SandboxId, routability: ExecutionRoutability) {
        self.0
            .lock()
            .unwrap()
            .routability_outcomes
            .insert(sandbox_id, routability);
    }

    fn set_routability_error(&self, sandbox_id: SandboxId) {
        self.0.lock().unwrap().routability_errors.insert(sandbox_id);
    }

    fn enqueue_cleanup_failure(&self, sandbox_id: SandboxId) {
        self.0.lock().unwrap().cleanup_failures.push(sandbox_id);
    }

    fn set_cleanup_pod_identity_changed(&self, sandbox_id: SandboxId, mismatch: PodFenceMismatch) {
        self.0
            .lock()
            .unwrap()
            .cleanup_pod_identity_changed
            .insert(sandbox_id, mismatch);
    }

    fn enqueue_failed_cleanup(&self, candidates: Vec<FailedCleanupCandidate>) {
        self.0.lock().unwrap().failed_cleanup_queue = candidates;
    }

    /// Make `first_activation` re-read `now` for `sandbox_id` instead of what
    /// the listing reported.
    fn set_first_activation_now(&self, sandbox_id: SandboxId, now: Option<FirstActivation>) {
        self.0
            .lock()
            .unwrap()
            .first_activation_now
            .insert(sandbox_id, now);
    }

    fn enqueue_failed_execution_cleanup_failure(&self, sandbox_id: SandboxId) {
        self.0
            .lock()
            .unwrap()
            .failed_execution_cleanup_failures
            .insert(sandbox_id);
    }

    fn list_calls(&self) -> Vec<(SandboxLifecycle, PositiveDuration)> {
        self.0.lock().unwrap().list_calls.clone()
    }

    fn probe_calls(&self) -> Vec<(String, Vec<SandboxId>)> {
        self.0.lock().unwrap().probe_calls.clone()
    }

    fn converge_calls(&self) -> Vec<(SandboxId, SandboxLifecycle)> {
        self.0.lock().unwrap().converge_calls.clone()
    }

    fn routability_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().routability_calls.clone()
    }

    fn set_fence_lost_after_precheck(&self, sandbox_id: SandboxId, mismatch: PodFenceMismatch) {
        let mut state = self.0.lock().unwrap();
        state
            .cleanup_pod_identity_changed
            .insert(sandbox_id, mismatch);
        state.fence_lost_after_precheck.insert(sandbox_id);
    }

    /// As above, but the refusal arrives only after the runtime was stopped.
    fn set_fence_lost_after_stopping(&self, sandbox_id: SandboxId, mismatch: PodFenceMismatch) {
        self.set_fence_lost_after_precheck(sandbox_id, mismatch);
        self.0
            .lock()
            .unwrap()
            .refusal_progress
            .insert(sandbox_id, TeardownProgress::RuntimeStopped);
    }

    fn undo_fail_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().undo_fail_calls.clone()
    }

    fn undo_reap_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().undo_reap_calls.clone()
    }

    fn undo_revert_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().undo_revert_calls.clone()
    }

    fn revert_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().revert_calls.clone()
    }

    fn fail_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().fail_calls.clone()
    }

    /// The guard each fail CAS was asked to apply, in call order.
    fn fail_guards(&self) -> Vec<FailGuard> {
        self.0.lock().unwrap().fail_guards.clone()
    }

    fn cleanup_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().cleanup_calls.clone()
    }

    fn cleanup_fences(&self) -> Vec<RetireFence> {
        self.0.lock().unwrap().cleanup_fences.clone()
    }

    fn failed_execution_cleanup_fences(&self) -> Vec<RetireFence> {
        self.0
            .lock()
            .unwrap()
            .failed_execution_cleanup_fences
            .clone()
    }

    fn failed_cleanup_attempts(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().failed_cleanup_attempts.clone()
    }

    fn failed_execution_cleanup_calls(&self) -> Vec<SandboxId> {
        self.0
            .lock()
            .unwrap()
            .failed_execution_cleanup_calls
            .clone()
    }

    fn failed_active_repair_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().failed_active_repair_calls.clone()
    }

    /// Enqueue candidates with a default (sub-terminal) pending age, so the
    /// terminal-unreachable backstop never trips unless a test opts in via
    /// [`Self::enqueue_command_ready_pending_with_ages`].
    fn enqueue_command_ready_pending(&self, ids: Vec<SandboxId>) {
        self.0.lock().unwrap().command_ready_pending_queue = ids
            .into_iter()
            .map(|id| command_ready_pending_candidate(id, Duration::ZERO))
            .collect();
    }

    /// Enqueue candidates with explicit pending ages (for the terminal-age
    /// backstop test).
    fn enqueue_command_ready_pending_with_ages(&self, items: Vec<(SandboxId, Duration)>) {
        self.0.lock().unwrap().command_ready_pending_queue = items
            .into_iter()
            .map(|(id, age)| command_ready_pending_candidate(id, age))
            .collect();
    }

    fn set_probe_outcome(&self, sandbox_id: SandboxId, outcome: CommandReadyProbe) {
        self.0
            .lock()
            .unwrap()
            .command_probe_outcomes
            .insert(sandbox_id, outcome);
    }

    fn set_reap_unclaimed(&self, sandbox_id: SandboxId) {
        self.0.lock().unwrap().reap_unclaimed.insert(sandbox_id);
    }

    fn enqueue_unrestorable(&self, items: Vec<(SandboxId, Duration)>) {
        self.0.lock().unwrap().unrestorable_queue = items;
    }

    fn set_unrestorable_list_error(&self) {
        self.0.lock().unwrap().unrestorable_list_error = true;
    }

    fn unrestorable_list_calls(&self) -> Vec<PositiveDuration> {
        self.0.lock().unwrap().unrestorable_list_calls.clone()
    }

    fn unrestorable_fail_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().unrestorable_fail_calls.clone()
    }

    fn heal_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().heal_calls.clone()
    }

    fn reap_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().reap_calls.clone()
    }
}

impl SweepSink for FakeSink {
    async fn list_aged_transient(
        &self,
        lifecycle: SandboxLifecycle,
        age: PositiveDuration,
    ) -> Result<Vec<SweepCandidate>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.list_calls.push((lifecycle, age));
        if let Some(idx) = state.db_errors.iter().position(|l| *l == lifecycle) {
            state.db_errors.remove(idx);
            return Err(sqlx::Error::PoolClosed);
        }
        Ok(state.candidate_queue.remove(&lifecycle).unwrap_or_default())
    }

    async fn probe_node(
        &self,
        node_name: &str,
        sandbox_ids: &[SandboxId],
        probe_timeout: Duration,
    ) -> Result<HashMap<SandboxId, ProbeObservation>, ProbeNodeError> {
        let mut state = self.0.lock().unwrap();
        state
            .probe_calls
            .push((node_name.to_string(), sandbox_ids.to_vec()));
        if state.probe_errors.contains(node_name) {
            return Err(ProbeNodeError::Timeout(probe_timeout));
        }
        Ok(sandbox_ids
            .iter()
            .filter(|id| !state.probe_unanswered.contains(id))
            .map(|id| {
                (
                    *id,
                    state.probe_outcomes.get(id).copied().unwrap_or_else(|| {
                        ProbeObservation::from_verdict(ProbeVerdict::DeadOnNode)
                    }),
                )
            })
            .collect())
    }

    async fn converge_restoring(
        &self,
        sandbox_id: SandboxId,
        target: SandboxLifecycle,
        _age: PositiveDuration,
    ) -> Result<bool, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.converge_calls.push((sandbox_id, target));
        Ok(!state.cas_unclaimed.contains(&sandbox_id))
    }

    async fn ensure_routable_execution(
        &self,
        sandbox_id: SandboxId,
        _probe_timeout: Duration,
    ) -> Result<ExecutionRoutability, String> {
        let mut state = self.0.lock().unwrap();
        state.routability_calls.push(sandbox_id);
        if state.routability_errors.contains(&sandbox_id) {
            return Err("simulated routability check error".to_string());
        }
        Ok(state
            .routability_outcomes
            .get(&sandbox_id)
            .copied()
            .unwrap_or(ExecutionRoutability::NotRoutable))
    }

    async fn undo_revert_to_suspended(
        &self,
        sandbox_id: SandboxId,
        _lifecycle: SandboxLifecycle,
        _reverted: &RevertedToSuspended,
        _preserve_local_artifact: bool,
    ) -> Result<bool, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.undo_revert_calls.push(sandbox_id);
        Ok(true)
    }

    async fn revert_to_suspended(
        &self,
        sandbox_id: SandboxId,
        _lifecycle: SandboxLifecycle,
        _age: PositiveDuration,
        _fence: &RetireFence,
    ) -> Result<Option<RevertedToSuspended>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.revert_calls.push(sandbox_id);
        Ok(
            (!state.cas_unclaimed.contains(&sandbox_id)).then(|| RevertedToSuspended {
                write_token: "revert-write".to_string(),
                prior_placement_changed_at: None,
            }),
        )
    }

    async fn fail_aged_candidate(
        &self,
        sandbox_id: SandboxId,
        _lifecycle: SandboxLifecycle,
        _age: PositiveDuration,
        guard: FailGuard,
    ) -> Result<Option<String>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.fail_calls.push(sandbox_id);
        state.fail_guards.push(guard);
        Ok((!state.cas_unclaimed.contains(&sandbox_id)).then(|| "fail-write".to_string()))
    }

    async fn undo_fail_aged_candidate(
        &self,
        sandbox_id: SandboxId,
        _lifecycle: SandboxLifecycle,
        _write_token: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.undo_fail_calls.push(sandbox_id);
        Ok(true)
    }

    async fn list_unrestorable_restoring(
        &self,
        min_placement_age: PositiveDuration,
    ) -> Result<Vec<(SandboxId, Duration)>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.unrestorable_list_calls.push(min_placement_age);
        if state.unrestorable_list_error {
            state.unrestorable_list_error = false;
            return Err(sqlx::Error::PoolClosed);
        }
        Ok(std::mem::take(&mut state.unrestorable_queue))
    }

    async fn fail_unrestorable_restoring(
        &self,
        sandbox_id: SandboxId,
        _min_placement_age: PositiveDuration,
    ) -> Result<bool, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.unrestorable_fail_calls.push(sandbox_id);
        Ok(!state.cas_unclaimed.contains(&sandbox_id))
    }

    /// The fake answers the pre-check from the same fixture its retire reads,
    /// so a test that arms a mismatch gets one consistently whichever question
    /// the sweeper asks first.
    async fn retire_fence_mismatch(
        &self,
        sandbox_id: SandboxId,
        _fence: &RetireFence,
    ) -> Result<Option<PodFenceMismatch>, String> {
        let state = self.0.lock().unwrap();
        let uuid = sandbox_id.as_uuid();
        if state
            .fence_lost_after_precheck
            .contains(&SandboxId::from_uuid(uuid))
        {
            return Ok(None);
        }
        Ok(state
            .cleanup_pod_identity_changed
            .get(&SandboxId::from_uuid(uuid))
            .copied())
    }

    async fn cleanup_after_sweep(
        &self,
        sandbox_id: SandboxId,
        pod_fence: RetireFence,
    ) -> Result<RetireExecutionOutcome, String> {
        let mut state = self.0.lock().unwrap();
        let uuid = sandbox_id.as_uuid();
        state.cleanup_calls.push(SandboxId::from_uuid(uuid));
        state.cleanup_fences.push(pod_fence);
        if let Some(idx) = state
            .cleanup_failures
            .iter()
            .position(|id| *id == SandboxId::from_uuid(uuid))
        {
            state.cleanup_failures.remove(idx);
            return Err("simulated cleanup error".to_string());
        }
        if let Some(mismatch) = state
            .cleanup_pod_identity_changed
            .get(&SandboxId::from_uuid(uuid))
            .copied()
        {
            return Ok(RetireExecutionOutcome::PodIdentityChanged {
                mismatch,
                progress: state
                    .refusal_progress
                    .get(&SandboxId::from_uuid(uuid))
                    .copied()
                    .unwrap_or(TeardownProgress::NothingMutated),
            });
        }
        Ok(RetireExecutionOutcome::Retired)
    }

    async fn list_failed_active_executions(
        &self,
        batch_size: i64,
    ) -> Result<Vec<FailedCleanupCandidate>, sqlx::Error> {
        let state = self.0.lock().unwrap();
        let limit = usize::try_from(batch_size).unwrap_or(0);
        Ok(state
            .failed_cleanup_queue
            .iter()
            .take(limit)
            .cloned()
            .collect())
    }

    async fn first_activation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<FirstActivation>, sqlx::Error> {
        let state = self.0.lock().unwrap();
        if let Some(now) = state.first_activation_now.get(&sandbox_id) {
            return Ok(*now);
        }
        Ok(state
            .failed_cleanup_queue
            .iter()
            .find(|candidate| candidate.sandbox_id == sandbox_id)
            .map(|candidate| candidate.first_activation))
    }

    async fn note_failed_cleanup_attempt(&self, sandbox_id: SandboxId) -> Result<(), sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        if let Some(idx) = state
            .failed_cleanup_queue
            .iter()
            .position(|candidate| candidate.sandbox_id == sandbox_id)
        {
            let candidate = state.failed_cleanup_queue.remove(idx);
            state.failed_cleanup_queue.push(candidate);
        }
        state.failed_cleanup_attempts.push(sandbox_id);
        Ok(())
    }

    async fn cleanup_failed_execution(
        &self,
        sandbox_id: SandboxId,
        pod_fence: RetireFence,
    ) -> Result<RetireExecutionOutcome, String> {
        let mut state = self.0.lock().unwrap();
        let uuid = sandbox_id.as_uuid();
        if state
            .failed_execution_cleanup_failures
            .contains(&SandboxId::from_uuid(uuid))
        {
            return Err("simulated failed execution cleanup error".to_string());
        }
        state
            .failed_execution_cleanup_calls
            .push(SandboxId::from_uuid(uuid));
        state.failed_execution_cleanup_fences.push(pod_fence);
        if let Some(mismatch) = state
            .cleanup_pod_identity_changed
            .get(&SandboxId::from_uuid(uuid))
            .copied()
        {
            return Ok(RetireExecutionOutcome::PodIdentityChanged {
                mismatch,
                progress: TeardownProgress::NothingMutated,
            });
        }
        Ok(RetireExecutionOutcome::Retired)
    }

    async fn repair_failed_active_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.failed_active_repair_calls.push(sandbox_id);
        Ok(!state.cas_unclaimed.contains(&sandbox_id))
    }

    async fn list_command_ready_pending(
        &self,
        _age: PositiveDuration,
    ) -> Result<Vec<CommandReadyPendingCandidate>, sqlx::Error> {
        Ok(self.0.lock().unwrap().command_ready_pending_queue.clone())
    }

    async fn probe_command_ready(
        &self,
        sandbox_id: SandboxId,
        _probe_timeout: Duration,
    ) -> CommandReadyProbe {
        self.0
            .lock()
            .unwrap()
            .command_probe_outcomes
            .get(&sandbox_id)
            .copied()
            .unwrap_or(CommandReadyProbe::Wedged)
    }

    async fn heal_command_ready_pending(&self, sandbox_id: SandboxId) -> Result<(), sqlx::Error> {
        self.0.lock().unwrap().heal_calls.push(sandbox_id);
        Ok(())
    }

    async fn reap_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<String>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.reap_calls.push(sandbox_id);
        Ok((!state.reap_unclaimed.contains(&sandbox_id)).then(|| "reap-write".to_string()))
    }

    async fn undo_reap_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
        _write_token: &str,
    ) -> Result<bool, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.undo_reap_calls.push(sandbox_id);
        Ok(true)
    }
}

/// A default candidate: on a probeable node, no evacuation marker, no ready
/// durable snapshot, DB-routable execution.
fn candidate(lifecycle: SandboxLifecycle) -> SweepCandidate {
    SweepCandidate {
        sandbox_id: SandboxId::new(),
        lifecycle,
        age: Duration::from_secs(900),
        evacuation_restore_target: None,
        ready_snapshot: false,
        fork_restore: false,
        node_name: Some("node-a".to_string()),
        routable_execution: true,
        activation_abandoned: false,
        episode_overdue: false,
        activation_attempt_held: true,
        pod_fence: pod_fence(),
    }
}

/// A candidate whose active execution the DB reports unroutable (no
/// `running` + `pod_ip` execution row) — the row shape left behind when a
/// wake handler dies after node-side restore but before pod-metadata
/// hydration.
fn unroutable_candidate(lifecycle: SandboxLifecycle) -> SweepCandidate {
    SweepCandidate {
        routable_execution: false,
        ..candidate(lifecycle)
    }
}

fn failed_candidate(sandbox_id: SandboxId) -> FailedCleanupCandidate {
    FailedCleanupCandidate {
        sandbox_id,
        node_name: Some("node-a".to_string()),
        first_activation: FirstActivation::Finished,
        pod_fence: pod_fence(),
    }
}

/// The pod identity a production candidate select reads off the active
/// execution row. Every default candidate carries one, so a path that drops
/// the fence on its way to the retire shows up as `None` rather than as a
/// still-passing test.
fn pod_fence() -> RetireFence {
    RetireFence::Pod(
        PodIdentityFence::new(
            PodName::parse("space-sbx-fenced-g1").expect("test pod name must parse"),
        )
        .with_pod_uid(PodUid::parse("pod-uid-fenced").expect("test pod uid must parse")),
    )
}

fn command_ready_pending_candidate(
    sandbox_id: SandboxId,
    pending_age: Duration,
) -> CommandReadyPendingCandidate {
    CommandReadyPendingCandidate {
        sandbox_id,
        pending_age,
        pod_fence: pod_fence(),
    }
}

fn config_with_all_overrides(age: PositiveDuration) -> ReconcilerConfig {
    let override_age = LifecycleAge::Override(age);
    ReconcilerConfig {
        enabled: true,
        tick: DEFAULT_TICK,
        default_age: age,
        creating_age: override_age,
        starting_age: override_age,
        pausing_age: override_age,
        resuming_age: override_age,
        suspending_age: override_age,
        restoring_age: override_age,
        stopping_age: override_age,
        checkpointing_age: override_age,
        probe_timeout: DEFAULT_SWEEP_PROBE_TIMEOUT,
        failed_cleanup_enabled: false,
        failed_cleanup_batch_size: DEFAULT_FAILED_CLEANUP_BATCH_SIZE,
        command_ready_pending_enabled: false,
        command_ready_pending_age: age,
        command_ready_pending_probe_timeout: DEFAULT_COMMAND_READY_PENDING_PROBE_TIMEOUT,
        command_ready_pending_terminal_age: PositiveDuration::new(Duration::from_secs(
            DEFAULT_COMMAND_READY_PENDING_TERMINAL_AGE_SECS,
        ))
        .unwrap(),
        unrestorable_restoring_enabled: true,
        unrestorable_restoring_min_placement_age: age,
        abandoned_restore_retirement: Default::default(),
        restoring_episode_deadline: None,
    }
}

fn sweep_config() -> ReconcilerConfig {
    config_with_all_overrides(PositiveDuration::new(Duration::from_secs(60)).unwrap())
}

// -----------------------------------------------------------------
// C — Config
// -----------------------------------------------------------------

#[test]
fn c1_resolve_age_uses_default_for_unset_override() {
    let config = ReconcilerConfig::default();
    assert_eq!(
        config.resolve_age(SandboxLifecycle::Pausing),
        Some(config.default_age),
        "unset override should fall back to default"
    );
}

#[test]
fn c2_resolve_age_uses_override_when_set() {
    let override_age = PositiveDuration::new(Duration::from_secs(30)).unwrap();
    let config = ReconcilerConfig {
        pausing_age: LifecycleAge::Override(override_age),
        ..ReconcilerConfig::default()
    };
    assert_eq!(
        config.resolve_age(SandboxLifecycle::Pausing),
        Some(override_age),
        "explicit override should be honored over default"
    );
}

#[test]
fn c3_disabled_lifecycle_resolves_to_none() {
    let config = ReconcilerConfig {
        creating_age: LifecycleAge::Disabled,
        ..ReconcilerConfig::default()
    };
    assert_eq!(
        config.resolve_age(SandboxLifecycle::Creating),
        None,
        "explicit Disabled should suppress the lifecycle"
    );
}

#[test]
fn c4_non_sweepable_lifecycle_resolves_to_none() {
    let config = ReconcilerConfig::default();
    assert_eq!(config.resolve_age(SandboxLifecycle::Started), None);
    assert_eq!(config.resolve_age(SandboxLifecycle::Failed), None);
    assert_eq!(config.resolve_age(SandboxLifecycle::Deleting), None);
}

#[test]
fn c5_started_is_not_sweepable_so_slow_creates_survive() {
    // Contract with the API's slow-create preserve path: when a create
    // exhausts its ALB-aware readiness budget the substrate leaves the pod
    // running and reports the sandbox as `started` (not `creating`). That only
    // protects a healthy-but-slow create from being aged out and deleted if
    // `started` stays out of the sweepable set — so guard it here. Adding
    // `Started` to SWEEPABLE_LIFECYCLES would let this sweeper fail+delete a
    // sandbox whose runtime simply took longer than the budget to converge.
    assert!(!SWEEPABLE_LIFECYCLES.contains(&SandboxLifecycle::Started));
}

// -----------------------------------------------------------------
// T — Tick behavior
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn t1_no_wedged_rows_emits_empty_report() {
    let sink = FakeSink::new();
    let reconciler = Reconciler::new(sweep_config(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test(flavor = "current_thread")]
async fn t2_wedged_row_in_each_lifecycle_aged_out() {
    let sink = FakeSink::new();
    let mut expected_ids = Vec::new();
    for lifecycle in SWEEPABLE_LIFECYCLES.iter().copied() {
        let candidate = candidate(lifecycle);
        expected_ids.push(candidate.sandbox_id);
        // Probe says dead with no artifact; no durable snapshot exists.
        sink.set_probe_verdict(candidate.sandbox_id, ProbeVerdict::DeadOnNode);
        sink.enqueue_candidates(lifecycle, vec![candidate]);
    }
    let reconciler = Reconciler::new(sweep_config(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.aged_out, SWEEPABLE_LIFECYCLES.len());
    assert_eq!(report.cleanup_failures, 0);
    assert_eq!(report.converged, 0);
    assert_eq!(report.reverted_to_suspended, 0);
    let cleanup_calls = reconciler.sink.cleanup_calls();
    assert_eq!(cleanup_calls.len(), SWEEPABLE_LIFECYCLES.len());
    for id in &expected_ids {
        assert!(cleanup_calls.contains(id), "every claimed id is cleaned up");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn t3_disabled_lifecycle_skipped_entirely() {
    let sink = FakeSink::new();
    let mut config = sweep_config();
    config.pausing_age = LifecycleAge::Disabled;
    // Seed a candidate anyway; the sweeper should not even list pausing.
    sink.enqueue_candidates(
        SandboxLifecycle::Pausing,
        vec![candidate(SandboxLifecycle::Pausing)],
    );
    let reconciler = Reconciler::new(config, sink);
    let report = reconciler.tick().await;
    assert_eq!(report.aged_out, 0);
    let calls = reconciler.sink.list_calls();
    assert!(
        !calls.iter().any(|(lc, _)| *lc == SandboxLifecycle::Pausing),
        "Disabled lifecycle must not generate a sink call"
    );
    // And the other 7 still got called.
    assert_eq!(calls.len(), SWEEPABLE_LIFECYCLES.len() - 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t4_cleanup_failure_increments_metric_but_continues() {
    let sink = FakeSink::new();
    let bad = candidate(SandboxLifecycle::Suspending);
    let good = candidate(SandboxLifecycle::Suspending);
    let (bad_id, good_id) = (bad.sandbox_id, good.sandbox_id);
    sink.set_probe_verdict(bad_id, ProbeVerdict::DeadOnNode);
    sink.set_probe_verdict(good_id, ProbeVerdict::DeadOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Suspending, vec![bad, good]);
    sink.enqueue_cleanup_failure(bad_id);
    let reconciler = Reconciler::new(sweep_config(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.aged_out, 2, "both rows still aged out in DB");
    assert_eq!(report.cleanup_failures, 1, "one cleanup failed");
    let cleanup_calls = reconciler.sink.cleanup_calls();
    assert!(cleanup_calls.contains(&bad_id));
    assert!(
        cleanup_calls.contains(&good_id),
        "the failure must not abort cleanup for subsequent rows"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t5_db_error_on_list_does_not_block_other_lifecycles() {
    let sink = FakeSink::new();
    sink.enqueue_db_error(SandboxLifecycle::Pausing);
    let restoring = candidate(SandboxLifecycle::Restoring);
    let restoring_id = restoring.sandbox_id;
    sink.set_probe_verdict(restoring_id, ProbeVerdict::MissingOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![restoring]);
    let reconciler = Reconciler::new(sweep_config(), sink);
    let report = reconciler.tick().await;
    assert_eq!(
        report.aged_out, 1,
        "pausing failed but restoring's row still aged out"
    );
    let cleanup_calls = reconciler.sink.cleanup_calls();
    assert_eq!(cleanup_calls, vec![restoring_id]);
}

#[tokio::test(flavor = "current_thread")]
async fn t6_failed_cleanup_cleans_listed_active_executions() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(sandbox_id)]);
    // Probe proves the sandbox gone, so the gate lets cleanup proceed.
    sink.set_probe_verdict(sandbox_id, ProbeVerdict::MissingOnNode);
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            enabled: false,
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    let report = reconciler.tick().await;

    assert_eq!(report.aged_out, 0);
    assert_eq!(report.failed_cleanup_attempts, 1);
    assert_eq!(report.failed_cleanup_failures, 0);
    assert_eq!(report.failed_cleanup_deferred, 0);
    assert_eq!(
        reconciler.sink.failed_execution_cleanup_calls(),
        vec![sandbox_id]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t7_failed_cleanup_keeps_going_after_cleanup_failure() {
    let sink = FakeSink::new();
    let failed = SandboxId::new();
    let cleaned = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(failed), failed_candidate(cleaned)]);
    sink.set_probe_verdict(failed, ProbeVerdict::DeadOnNode);
    sink.set_probe_verdict(cleaned, ProbeVerdict::DeadOnNode);
    sink.enqueue_failed_execution_cleanup_failure(failed);
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    let report = reconciler.tick().await;

    assert_eq!(report.failed_cleanup_attempts, 2);
    assert_eq!(report.failed_cleanup_failures, 1);
    assert_eq!(
        reconciler.sink.failed_execution_cleanup_calls(),
        vec![cleaned]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t8_failed_cleanup_rotates_and_does_not_starve_later_ids() {
    let sink = FakeSink::new();
    let failing = SandboxId::new();
    let later = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(failing), failed_candidate(later)]);
    sink.set_probe_verdict(failing, ProbeVerdict::DeadOnNode);
    sink.set_probe_verdict(later, ProbeVerdict::DeadOnNode);
    sink.enqueue_failed_execution_cleanup_failure(failing);
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            failed_cleanup_batch_size: 1,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    reconciler.tick().await;
    reconciler.tick().await;

    assert_eq!(
        reconciler.sink.failed_cleanup_attempts(),
        vec![failing, later]
    );
    assert_eq!(
        reconciler.sink.failed_execution_cleanup_calls(),
        vec![later]
    );
}

// -----------------------------------------------------------------
// V — Verify-before-destroy (probe -> classify -> act)
// -----------------------------------------------------------------

/// V1 — an alive `restoring` row converges to its wake target instead of
/// being failed: lease-guarded CAS to `started`, no fail CAS, no cleanup.
#[tokio::test(flavor = "current_thread")]
async fn v1_alive_restoring_converges_to_wake_target() {
    let sink = FakeSink::new();
    let alive = candidate(SandboxLifecycle::Restoring);
    let alive_id = alive.sandbox_id;
    sink.set_probe_verdict(alive_id, ProbeVerdict::AliveOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![alive]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.converged, 1, "alive restoring row must converge");
    assert_eq!(report.aged_out, 0, "an alive sandbox must never be failed");
    assert_eq!(
        reconciler.sink.converge_calls(),
        vec![(alive_id, SandboxLifecycle::Started)],
        "the wake target defaults to started when no evacuation marker is set"
    );
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(
        reconciler.sink.cleanup_calls().is_empty(),
        "a converged sandbox must not be torn down"
    );
}

/// V2 — an alive row in any non-`restoring` transient lifecycle defers: the
/// sweep has no converge path for it yet and must not destroy a live VM.
#[tokio::test(flavor = "current_thread")]
async fn v2_alive_non_restoring_defers() {
    let sink = FakeSink::new();
    let alive = candidate(SandboxLifecycle::Pausing);
    sink.set_probe_verdict(alive.sandbox_id, ProbeVerdict::AliveOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Pausing, vec![alive]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.deferred, 1, "alive non-restoring row must defer");
    assert_eq!(report.aged_out, 0);
    assert_eq!(report.converged, 0);
    assert!(reconciler.sink.converge_calls().is_empty());
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.revert_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// V3 — a dormant `restoring` row whose snapshot is canonical (ready durable
/// backup) reverts to `suspended` instead of failing, and the leftover
/// execution is retired so the row matches the suspended contract (no active
/// execution, no placement, no pod).
#[tokio::test(flavor = "current_thread")]
async fn v3_dormant_restoring_with_ready_snapshot_reverts_and_retires() {
    let sink = FakeSink::new();
    let mut dormant = candidate(SandboxLifecycle::Restoring);
    dormant.ready_snapshot = true;
    let dormant_id = dormant.sandbox_id;
    sink.set_probe_verdict(dormant_id, ProbeVerdict::DormantWithLocalArtifact);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![dormant]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.reverted_to_suspended, 1);
    assert_eq!(report.aged_out, 0);
    assert_eq!(report.cleanup_failures, 0);
    assert_eq!(reconciler.sink.revert_calls(), vec![dormant_id]);
    assert!(reconciler.sink.fail_calls().is_empty());
    assert_eq!(
        reconciler.sink.cleanup_calls(),
        vec![dormant_id],
        "a claimed revert must retire the wedged restore's leftover execution"
    );
}

/// A revert candidate whose pod recovered between the select and here must
/// keep its lifecycle. `suspended` is a resting lifecycle no reconciler
/// revisits, so committing the revert and only then learning the retire is
/// refused leaves a dormant row owning a live execution and a running pod —
/// the one outcome the fence exists to prevent, reached by obeying it.
#[tokio::test(flavor = "current_thread")]
async fn a_lost_fence_withholds_the_revert_rather_than_stranding_it() {
    let sink = FakeSink::new();
    let mut dormant = candidate(SandboxLifecycle::Restoring);
    dormant.ready_snapshot = true;
    let dormant_id = dormant.sandbox_id;
    sink.set_probe_verdict(dormant_id, ProbeVerdict::DormantWithLocalArtifact);
    sink.set_cleanup_pod_identity_changed(dormant_id, PodFenceMismatch::PodResourceVersionChanged);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![dormant]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert!(
        reconciler.sink.revert_calls().is_empty(),
        "the lifecycle CAS must not run once the fence is known lost"
    );
    assert!(
        reconciler.sink.cleanup_calls().is_empty(),
        "and nothing is torn down either"
    );
    assert_eq!(report.reverted_to_suspended, 0);
    assert_eq!(report.cleanup_failures, 0);
    assert_eq!(
        report.cleanup_pod_identity_deferred, 1,
        "the deferral is counted so a candidate that never converges is visible"
    );
}

/// The pre-check narrows the window but cannot close it: a pod recovering
/// between the question and the CAS — a Pending pod going Running changes the
/// resourceVersion on its own — leaves the revert durable and the retire
/// refused. `suspended` is a resting lifecycle no reconciler revisits, so the
/// lifecycle has to go back, which is also simply correct: the restore the
/// sweep judged dead is running after all.
#[tokio::test(flavor = "current_thread")]
async fn a_revert_whose_retire_is_then_refused_is_undone() {
    let sink = FakeSink::new();
    let mut dormant = candidate(SandboxLifecycle::Restoring);
    dormant.ready_snapshot = true;
    let dormant_id = dormant.sandbox_id;
    sink.set_probe_verdict(dormant_id, ProbeVerdict::DormantWithLocalArtifact);
    sink.set_fence_lost_after_precheck(dormant_id, PodFenceMismatch::PodResourceVersionChanged);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![dormant]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.revert_calls(),
        vec![dormant_id],
        "the pre-check passed, so the revert did commit"
    );
    assert_eq!(
        reconciler.sink.undo_revert_calls(),
        vec![dormant_id],
        "and the refused retire must put the lifecycle back"
    );
    assert_eq!(
        report.reverted_to_suspended, 0,
        "an undone revert is not a revert"
    );
    assert_eq!(report.cleanup_pod_identity_deferred, 1);
}

/// A refusal that arrives only *after* the in-VM stop, and that describes the
/// *same pod* having moved, must not be compensated: the stop reached that
/// object, so putting `restoring` back would claim a running VM there isn't
/// one of. The deferral metric is what surfaces the leftover execution.
#[tokio::test(flavor = "current_thread")]
async fn a_revert_is_not_undone_once_the_runtime_is_already_stopped() {
    let sink = FakeSink::new();
    let mut dormant = candidate(SandboxLifecycle::Restoring);
    dormant.ready_snapshot = true;
    let dormant_id = dormant.sandbox_id;
    sink.set_probe_verdict(dormant_id, ProbeVerdict::DormantWithLocalArtifact);
    sink.set_fence_lost_after_stopping(dormant_id, PodFenceMismatch::PodResourceVersionChanged);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![dormant]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(reconciler.sink.revert_calls(), vec![dormant_id]);
    assert!(
        reconciler.sink.undo_revert_calls().is_empty(),
        "a stopped VM must not have its lifecycle restored"
    );
    assert_eq!(report.reverted_to_suspended, 1, "the revert stands");
    assert_eq!(report.cleanup_pod_identity_deferred, 1);
}

/// But stopping the execution the fence observed says nothing about a
/// *successor* that took over afterwards — the stop is addressed to the one,
/// and the other is still running. Reading "runtime stopped" as "sandbox
/// stopped" there would leave a dormant `suspended` lifecycle over a live
/// execution, which is the stranded state nothing revisits.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_predecessor_does_not_excuse_stranding_its_successor() {
    let sink = FakeSink::new();
    let mut dormant = candidate(SandboxLifecycle::Restoring);
    dormant.ready_snapshot = true;
    let dormant_id = dormant.sandbox_id;
    sink.set_probe_verdict(dormant_id, ProbeVerdict::DormantWithLocalArtifact);
    sink.set_fence_lost_after_stopping(dormant_id, PodFenceMismatch::ExecutionSuperseded);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![dormant]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.undo_revert_calls(),
        vec![dormant_id],
        "a successor took over and was never stopped, so the revert must come back"
    );
    assert_eq!(report.reverted_to_suspended, 0);
    assert_eq!(report.cleanup_pod_identity_deferred, 1);
}

/// Same on the reap path.
#[tokio::test(flavor = "current_thread")]
async fn a_reap_is_not_undone_once_the_runtime_is_already_stopped() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![sandbox_id]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Wedged);
    sink.set_fence_lost_after_stopping(sandbox_id, PodFenceMismatch::PodResourceVersionChanged);
    let reconciler = Reconciler::new(command_ready_pending_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(reconciler.sink.reap_calls(), vec![sandbox_id]);
    assert!(
        reconciler.sink.undo_reap_calls().is_empty(),
        "restoring a started row for a stopped guest would read as a live sandbox"
    );
    assert_eq!(report.command_ready_reaped, 1, "the reap stands");
    assert_eq!(report.cleanup_pod_identity_deferred, 1);
}

/// An event that records the wrong direction is precisely the misreading these
/// events exist to prevent. The rollback runs *out of* `failed` back to the
/// lifecycle the sweep found; everything else in this helper runs into it.
/// Asserted through `Debug` because the details' fields are crate-private to
/// `pplx-space-events`, and widening that API for a test would be worse.
#[test]
fn a_rollback_event_records_the_direction_it_actually_ran() {
    let age = Duration::from_secs(900);

    let rolled_back = format!(
        "{:?}",
        transient_sweep_details(
            SandboxEventType::LifecycleTransitionRolledBack,
            "pausing",
            age,
        )
    );
    assert!(
        rolled_back.contains(r#"from_state: Some("failed")"#),
        "a rollback leaves `failed`, got {rolled_back}"
    );
    assert!(
        rolled_back.contains(r#"to_state: Some("pausing")"#),
        "and lands on the lifecycle the sweep found, got {rolled_back}"
    );
    assert!(
        !rolled_back.contains(r#"target_state: Some("failed")"#),
        "it must not also claim `failed` as the target, got {rolled_back}"
    );

    // The age-out itself is unchanged, and runs the other way.
    let aged_out = format!(
        "{:?}",
        transient_sweep_details(
            SandboxEventType::LifecycleTransitionTimedOut,
            "pausing",
            age,
        )
    );
    assert!(aged_out.contains(r#"from_state: Some("pausing")"#));
    assert!(aged_out.contains(r#"to_state: Some("failed")"#));
}

/// Third path, same shape, and the least recoverable: nothing walks a `failed`
/// row back. Pause completion refuses to overwrite it and the failed-cleanup
/// pass repairs only node-reported `started` sandboxes, so an aged `pausing`
/// row whose pause finished mid-sweep would be represented as failed forever
/// while its execution ran on.
#[tokio::test(flavor = "current_thread")]
async fn an_age_out_whose_retire_is_then_refused_is_undone() {
    let sink = FakeSink::new();
    let aged = candidate(SandboxLifecycle::Pausing);
    let aged_id = aged.sandbox_id;
    sink.set_probe_verdict(aged_id, ProbeVerdict::MissingOnNode);
    sink.set_fence_lost_after_precheck(aged_id, PodFenceMismatch::PodResourceVersionChanged);
    sink.enqueue_candidates(SandboxLifecycle::Pausing, vec![aged]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.fail_calls(),
        vec![aged_id],
        "the pre-check passed, so the fail CAS did commit"
    );
    assert_eq!(
        reconciler.sink.undo_fail_calls(),
        vec![aged_id],
        "and the refused retire must put the lifecycle back"
    );
    assert_eq!(report.aged_out, 0, "an undone age-out is not an age-out");
    assert_eq!(report.cleanup_pod_identity_deferred, 1);
}

/// An undo may only run where it can restore the *whole* pre-transition state.
/// Entering `failed` from `suspending` or `restoring` fires triggers that clear
/// the evacuation marker and its backoff, the restore lease, and the only-copy
/// protection flag — and the lease must not come back, since another actor may
/// hold one. Returning such a row to its old lifecycle without them changes
/// drain-redrive and only-copy behavior silently, which is worse than a
/// `failed` row the deferral counter makes visible.
#[tokio::test(flavor = "current_thread")]
async fn an_age_out_is_not_undone_where_the_undo_would_be_lossy() {
    for lifecycle in [SandboxLifecycle::Suspending, SandboxLifecycle::Restoring] {
        let sink = FakeSink::new();
        let aged = candidate(lifecycle);
        let aged_id = aged.sandbox_id;
        sink.set_probe_verdict(aged_id, ProbeVerdict::MissingOnNode);
        sink.set_fence_lost_after_precheck(aged_id, PodFenceMismatch::PodResourceVersionChanged);
        sink.enqueue_candidates(lifecycle, vec![aged]);
        let reconciler = Reconciler::new(sweep_config(), sink);

        let report = reconciler.tick().await;

        assert_eq!(
            reconciler.sink.fail_calls(),
            vec![aged_id],
            "{lifecycle}: the pre-check passed, so the fail CAS committed"
        );
        assert!(
            reconciler.sink.undo_fail_calls().is_empty(),
            "{lifecycle}: undoing here would return the row without its evacuation and \
             only-copy markers"
        );
        assert_eq!(report.aged_out, 1, "{lifecycle}: the age-out stands");
        assert_eq!(report.cleanup_pod_identity_deferred, 1);
    }
}

/// Same race on the reap path, and it bites harder: the flip to `failed`
/// fires a trigger that clears `command_ready_pending`, and nothing puts it
/// back. A still-wedged guest left without the marker never enters this reaper
/// again — whether the row stays `failed` or the failed-cleanup pass repairs
/// it to `started`.
#[tokio::test(flavor = "current_thread")]
async fn a_reap_whose_retire_is_then_refused_is_undone() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![sandbox_id]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Wedged);
    sink.set_fence_lost_after_precheck(sandbox_id, PodFenceMismatch::PodResourceVersionChanged);
    let reconciler = Reconciler::new(command_ready_pending_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.reap_calls(),
        vec![sandbox_id],
        "the pre-check passed, so the reap did commit"
    );
    assert_eq!(
        reconciler.sink.undo_reap_calls(),
        vec![sandbox_id],
        "and the refused retire must restore the lifecycle and the marker"
    );
    assert_eq!(
        report.command_ready_reaped, 0,
        "an undone reap is not a reap"
    );
    assert_eq!(report.cleanup_pod_identity_deferred, 1);
}

/// V4 — a dormant row whose node-local artifact is NOT backed up defers:
/// the node holds the only copy, and the sweep never destroys the only copy.
#[tokio::test(flavor = "current_thread")]
async fn v4_dormant_artifact_without_backup_defers() {
    let sink = FakeSink::new();
    let dormant = candidate(SandboxLifecycle::Suspending);
    sink.set_probe_verdict(dormant.sandbox_id, ProbeVerdict::DormantWithLocalArtifact);
    sink.enqueue_candidates(SandboxLifecycle::Suspending, vec![dormant]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.deferred, 1);
    assert_eq!(report.aged_out, 0);
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.revert_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// V5 — a missing-on-node `restoring` row with a ready durable snapshot
/// reverts to `suspended` (recoverable) rather than failing, and retires the
/// leftover execution row.
#[tokio::test(flavor = "current_thread")]
async fn v5_missing_restoring_with_ready_snapshot_reverts() {
    let sink = FakeSink::new();
    let mut missing = candidate(SandboxLifecycle::Restoring);
    missing.ready_snapshot = true;
    let missing_id = missing.sandbox_id;
    sink.set_probe_verdict(missing_id, ProbeVerdict::MissingOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![missing]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.reverted_to_suspended, 1);
    assert_eq!(report.aged_out, 0);
    assert_eq!(reconciler.sink.revert_calls(), vec![missing_id]);
    assert_eq!(reconciler.sink.cleanup_calls(), vec![missing_id]);
}

/// A revert reports the deadline as its reason only for a candidate the
/// deadline let through; every other revert reports its snapshot.
#[test]
fn a_revert_reports_the_deadline_only_when_it_let_the_row_through() {
    let ordinary = SweepCandidate {
        ready_snapshot: true,
        ..candidate(SandboxLifecycle::Restoring)
    };
    let overdue = SweepCandidate {
        episode_overdue: true,
        ..ordinary.clone()
    };
    assert_eq!(
        restoring_episode::revert_reason(&ordinary),
        "recoverable_snapshot"
    );
    assert_eq!(
        restoring_episode::revert_reason(&overdue),
        "restoring_episode_deadline"
    );
}

/// V6 — a probe error downgrades every candidate on that node to
/// inconclusive: defer, never destroy on a failed sample.
#[tokio::test(flavor = "current_thread")]
async fn v6_probe_error_defers_everything_on_that_node() {
    let sink = FakeSink::new();
    let one = candidate(SandboxLifecycle::Pausing);
    let two = candidate(SandboxLifecycle::Restoring);
    sink.enqueue_candidates(SandboxLifecycle::Pausing, vec![one]);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![two]);
    sink.set_probe_error("node-a");
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.deferred, 2, "both candidates on the node defer");
    assert_eq!(report.aged_out, 0);
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.revert_calls().is_empty());
    assert!(reconciler.sink.converge_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// V7 — a candidate with no active execution row (no node to probe)
/// classifies as missing-on-node: with no snapshot it gets the old
/// fail+retire behavior without any probe RPC.
#[tokio::test(flavor = "current_thread")]
async fn v7_no_execution_row_is_missing_on_node() {
    let sink = FakeSink::new();
    let mut orphan = candidate(SandboxLifecycle::Creating);
    orphan.node_name = None;
    let orphan_id = orphan.sandbox_id;
    sink.enqueue_candidates(SandboxLifecycle::Creating, vec![orphan]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.aged_out, 1);
    assert_eq!(reconciler.sink.fail_calls(), vec![orphan_id]);
    assert_eq!(reconciler.sink.cleanup_calls(), vec![orphan_id]);
    assert!(
        reconciler.sink.probe_calls().is_empty(),
        "no node means nothing to probe"
    );
}

/// V8 — when an action CAS reports "not claimed" (the row changed underneath
/// between select and act), nothing further happens: no cleanup, no counters.
#[tokio::test(flavor = "current_thread")]
async fn v8_lost_cas_skips_cleanup_and_counters() {
    let sink = FakeSink::new();
    let raced = candidate(SandboxLifecycle::Stopping);
    let raced_id = raced.sandbox_id;
    sink.set_probe_verdict(raced_id, ProbeVerdict::DeadOnNode);
    sink.set_cas_unclaimed(raced_id);
    sink.enqueue_candidates(SandboxLifecycle::Stopping, vec![raced]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.aged_out, 0, "a lost CAS must not count as aged out");
    assert_eq!(reconciler.sink.fail_calls(), vec![raced_id]);
    assert!(
        reconciler.sink.cleanup_calls().is_empty(),
        "cleanup runs only on rows the fail CAS actually claimed"
    );
}

/// V9 — candidates on the same node are probed in one batched call, across
/// lifecycles.
#[tokio::test(flavor = "current_thread")]
async fn v9_same_node_candidates_share_one_probe() {
    let sink = FakeSink::new();
    let one = candidate(SandboxLifecycle::Pausing);
    let two = candidate(SandboxLifecycle::Restoring);
    let (one_id, two_id) = (one.sandbox_id, two.sandbox_id);
    sink.set_probe_verdict(one_id, ProbeVerdict::DeadOnNode);
    sink.set_probe_verdict(two_id, ProbeVerdict::DeadOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Pausing, vec![one]);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![two]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    reconciler.tick().await;

    let probe_calls = reconciler.sink.probe_calls();
    assert_eq!(probe_calls.len(), 1, "one probe RPC per node per tick");
    assert_eq!(probe_calls[0].0, "node-a");
    let mut probed = probe_calls[0].1.clone();
    probed.sort();
    let mut expected = vec![one_id, two_id];
    expected.sort();
    assert_eq!(probed, expected);
}

/// V10 — the failed-cleanup pass is probe-gated: a `failed` row whose VM is
/// in fact alive is deferred (rotated, not cleaned), while a proven-gone row
/// proceeds.
#[tokio::test(flavor = "current_thread")]
async fn v10_failed_cleanup_defers_when_probe_says_alive() {
    let sink = FakeSink::new();
    let alive = SandboxId::new();
    let gone = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(alive), failed_candidate(gone)]);
    sink.set_probe_verdict(alive, ProbeVerdict::AliveOnNode);
    sink.set_probe_verdict(gone, ProbeVerdict::MissingOnNode);
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    let report = reconciler.tick().await;

    assert_eq!(report.failed_cleanup_deferred, 1, "the alive row defers");
    assert_eq!(report.failed_cleanup_attempts, 1, "the gone row proceeds");
    assert_eq!(
        reconciler.sink.failed_execution_cleanup_calls(),
        vec![gone],
        "cleanup must never run against a live VM"
    );
    // Both rotate so a persistently-alive row cannot monopolize the batch.
    let mut rotated = reconciler.sink.failed_cleanup_attempts();
    rotated.sort();
    let mut expected = vec![alive, gone];
    expected.sort();
    assert_eq!(rotated, expected);
}

/// V11 — a dormant non-`restoring` row defers even when a ready durable
/// snapshot exists. The artifact pointer is never cleared on wake, so the
/// matched backup is the *previous* suspend cycle's bytes (the June
/// version-skew family: node-side suspend published a new artifact, the DB
/// write never landed); a revert would silently restore pre-wake state while
/// the node holds the only current copy.
#[tokio::test(flavor = "current_thread")]
async fn v11_dormant_non_restoring_with_ready_snapshot_defers() {
    let sink = FakeSink::new();
    let mut wedged = candidate(SandboxLifecycle::Suspending);
    wedged.ready_snapshot = true;
    sink.set_probe_verdict(wedged.sandbox_id, ProbeVerdict::DormantWithLocalArtifact);
    sink.enqueue_candidates(SandboxLifecycle::Suspending, vec![wedged]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.deferred, 1, "the node holds the only current copy");
    assert_eq!(report.reverted_to_suspended, 0);
    assert_eq!(report.aged_out, 0);
    assert!(reconciler.sink.revert_calls().is_empty());
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// V12 — a dead/missing non-`restoring` row with a (stale prior-cycle) ready
/// snapshot takes the old loud fail+retire path, never a silent revert: the
/// snapshot predates the wake, so reverting would roll the sandbox back to
/// pre-wake bytes without anyone noticing.
#[tokio::test(flavor = "current_thread")]
async fn v12_dead_non_restoring_with_ready_snapshot_fails_loudly() {
    let sink = FakeSink::new();
    let mut dead = candidate(SandboxLifecycle::Pausing);
    dead.ready_snapshot = true;
    let dead_id = dead.sandbox_id;
    let mut missing = candidate(SandboxLifecycle::Suspending);
    missing.ready_snapshot = true;
    let missing_id = missing.sandbox_id;
    sink.set_probe_verdict(dead_id, ProbeVerdict::DeadOnNode);
    sink.set_probe_verdict(missing_id, ProbeVerdict::MissingOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Pausing, vec![dead]);
    sink.enqueue_candidates(SandboxLifecycle::Suspending, vec![missing]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.aged_out, 2, "both take the loud fail+retire path");
    assert_eq!(report.reverted_to_suspended, 0);
    assert!(
        reconciler.sink.revert_calls().is_empty(),
        "a stale prior-cycle snapshot must never drive a revert"
    );
    let mut failed = reconciler.sink.fail_calls();
    failed.sort();
    let mut expected = vec![dead_id, missing_id];
    expected.sort();
    assert_eq!(failed, expected);
}

/// V13 — an id the node's (successful) probe answer did not cover is
/// inconclusive: defer with no CAS and no cleanup. Pins the
/// `unwrap_or(Inconclusive)` default in `probe_targets` — if it regressed to
/// a destructive default like `DeadOnNode`, this row would be failed and
/// retired off a non-answer.
#[tokio::test(flavor = "current_thread")]
async fn v13_probe_answer_missing_id_defers() {
    let sink = FakeSink::new();
    let mut unanswered = candidate(SandboxLifecycle::Restoring);
    unanswered.ready_snapshot = true;
    let unanswered_id = unanswered.sandbox_id;
    sink.set_probe_unanswered(unanswered_id);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![unanswered]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        reconciler.sink.probe_calls().len(),
        1,
        "the node was probed; only this id went unanswered"
    );
    assert_eq!(report.deferred, 1, "an unanswered id must defer");
    assert_eq!(report.aged_out, 0);
    assert_eq!(report.reverted_to_suspended, 0);
    assert_eq!(report.converged, 0);
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.revert_calls().is_empty());
    assert!(reconciler.sink.converge_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// V14 — the probe fan-out is budgeted to half the tick: with an exhausted
/// budget no node is probed and every candidate defers to the next tick
/// instead of being acted on (or holding the loop for minutes).
#[tokio::test(flavor = "current_thread")]
async fn v14_probe_budget_exhaustion_defers_unprobed_nodes() {
    let sink = FakeSink::new();
    let mut one = candidate(SandboxLifecycle::Pausing);
    one.node_name = Some("node-a".to_string());
    let mut two = candidate(SandboxLifecycle::Restoring);
    two.node_name = Some("node-b".to_string());
    // Destructive verdicts are wired so the test fails loudly if a probe
    // slips past the exhausted budget.
    sink.set_probe_verdict(one.sandbox_id, ProbeVerdict::DeadOnNode);
    sink.set_probe_verdict(two.sandbox_id, ProbeVerdict::DeadOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Pausing, vec![one]);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![two]);
    let mut config = sweep_config();
    config.tick = Duration::ZERO;
    let reconciler = Reconciler::new(config, sink);

    let report = reconciler.tick().await;

    assert!(
        reconciler.sink.probe_calls().is_empty(),
        "an exhausted budget must not probe any node"
    );
    assert_eq!(report.deferred, 2, "unprobed candidates defer");
    assert_eq!(report.aged_out, 0);
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// V15 — the failed-cleanup pass shares the probe budget: with an exhausted
/// budget its candidates probe inconclusive and the destructive cleanup is
/// deferred, never run on a stale or absent verdict.
#[tokio::test(flavor = "current_thread")]
async fn v15_failed_cleanup_probe_budget_defers() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(sandbox_id)]);
    sink.set_probe_verdict(sandbox_id, ProbeVerdict::MissingOnNode);
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            tick: Duration::ZERO,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    let report = reconciler.tick().await;

    assert!(reconciler.sink.probe_calls().is_empty());
    assert_eq!(report.failed_cleanup_deferred, 1);
    assert_eq!(report.failed_cleanup_attempts, 0);
    assert!(reconciler.sink.failed_execution_cleanup_calls().is_empty());
}

/// V16 — a failure of the post-revert execution retire is counted and logged
/// but does not undo the revert: the row is already `suspended` and the
/// snapshot behind it is intact.
#[tokio::test(flavor = "current_thread")]
async fn v16_revert_cleanup_failure_is_counted_but_revert_stands() {
    let sink = FakeSink::new();
    let mut wedged = candidate(SandboxLifecycle::Restoring);
    wedged.ready_snapshot = true;
    let wedged_id = wedged.sandbox_id;
    sink.set_probe_verdict(wedged_id, ProbeVerdict::DeadOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![wedged]);
    sink.enqueue_cleanup_failure(wedged_id);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.reverted_to_suspended, 1, "the revert still counts");
    assert_eq!(report.cleanup_failures, 1, "the retire failure is counted");
    assert_eq!(reconciler.sink.cleanup_calls(), vec![wedged_id]);
}

/// V17 — the failed-cleanup probe gate retires a dormant `failed` row even
/// when the node still holds a current-instance local artifact (see
/// `ProbeVerdict::permits_failed_retire` for why that is safe), while an
/// unanswered probe still defers.
#[tokio::test(flavor = "current_thread")]
async fn v17_failed_cleanup_retires_dormant_rows_and_defers_inconclusive() {
    let sink = FakeSink::new();
    let dormant = SandboxId::new();
    let unanswered = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![
        failed_candidate(dormant),
        failed_candidate(unanswered),
    ]);
    sink.set_probe_verdict(dormant, ProbeVerdict::DormantWithLocalArtifact);
    sink.set_probe_unanswered(unanswered);
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    let report = reconciler.tick().await;

    assert_eq!(
        report.failed_cleanup_attempts, 1,
        "the dormant row's execution is retired (its pod feeds the kubelet loop)"
    );
    assert_eq!(
        report.failed_cleanup_deferred, 1,
        "the unanswered probe defers — unreachable means defer, never destroy"
    );
    assert_eq!(
        reconciler.sink.failed_execution_cleanup_calls(),
        vec![dormant]
    );
}

/// V18 — a failed Postgres row whose active execution's node reports a live
/// `started` sandbox is stale control-plane projection. Repair PG back to
/// `started` and do not retire the execution.
#[tokio::test(flavor = "current_thread")]
async fn v18_failed_cleanup_repairs_when_node_reports_started() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(sandbox_id)]);
    sink.set_probe_observation(
        sandbox_id,
        ProbeObservation {
            verdict: ProbeVerdict::AliveOnNode,
            node_status: Some(SandboxLifecycle::Started),
        },
    );
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    let report = reconciler.tick().await;

    assert_eq!(report.failed_cleanup_repaired, 1);
    assert_eq!(report.failed_cleanup_attempts, 0);
    assert_eq!(
        reconciler.sink.failed_active_repair_calls(),
        vec![sandbox_id]
    );
    assert!(
        reconciler.sink.failed_execution_cleanup_calls().is_empty(),
        "repair must preserve the active execution"
    );
}

/// V19 — a node-local `failed` row with a live Firecracker process is still
/// live/degraded, not safe to retire. The node watchdog owns local recovery.
#[tokio::test(flavor = "current_thread")]
async fn v19_failed_cleanup_defers_live_node_failed_without_retire() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(sandbox_id)]);
    sink.set_probe_observation(
        sandbox_id,
        ProbeObservation {
            verdict: ProbeVerdict::AliveOnNode,
            node_status: Some(SandboxLifecycle::Failed),
        },
    );
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    let report = reconciler.tick().await;

    assert_eq!(report.failed_cleanup_deferred, 1);
    assert!(reconciler.sink.failed_active_repair_calls().is_empty());
    assert!(reconciler.sink.failed_execution_cleanup_calls().is_empty());
}

/// V20 — if the repair CAS loses a race, the reconciler records the race and
/// does not fall through to cleanup.
#[tokio::test(flavor = "current_thread")]
async fn v20_failed_cleanup_repair_cas_lost_skips_cleanup() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(sandbox_id)]);
    sink.set_probe_observation(
        sandbox_id,
        ProbeObservation {
            verdict: ProbeVerdict::AliveOnNode,
            node_status: Some(SandboxLifecycle::Started),
        },
    );
    sink.set_cas_unclaimed(sandbox_id);
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            failed_cleanup_enabled: true,
            ..ReconcilerConfig::default()
        },
        sink,
    );

    let report = reconciler.tick().await;

    assert_eq!(report.failed_cleanup_repair_cas_lost, 1);
    assert_eq!(
        reconciler.sink.failed_active_repair_calls(),
        vec![sandbox_id]
    );
    assert!(reconciler.sink.failed_execution_cleanup_calls().is_empty());
}

/// V21 — defer outcomes are operator-facing metric/event labels. Pin the
/// classifier directly so V19 cannot stay green while the live-failed label
/// regresses to an unknown or generic defer.
#[test]
fn v21_failed_cleanup_defer_reason_labels_are_stable() {
    assert_eq!(
        failed_cleanup_defer_reason(ProbeObservation {
            verdict: ProbeVerdict::AliveOnNode,
            node_status: Some(SandboxLifecycle::Failed),
        }),
        "vm_alive_status_failed"
    );
    assert_eq!(
        failed_cleanup_defer_reason(ProbeObservation {
            verdict: ProbeVerdict::AliveOnNode,
            node_status: None,
        }),
        "vm_alive_status_unknown"
    );
    assert_eq!(
        failed_cleanup_defer_reason(ProbeObservation {
            verdict: ProbeVerdict::AliveOnNode,
            node_status: Some(SandboxLifecycle::Paused),
        }),
        "vm_alive_status_unknown"
    );
    assert_eq!(
        failed_cleanup_defer_reason(ProbeObservation {
            verdict: ProbeVerdict::DeadOnNode,
            node_status: Some(SandboxLifecycle::Failed),
        }),
        "probe_verdict_forbids_retire"
    );
    assert_eq!(
        failed_cleanup_defer_reason(ProbeObservation::inconclusive()),
        "probe_verdict_forbids_retire"
    );
}

/// V22 — an alive `restoring` row whose execution the DB reports unroutable
/// consults the substrate; a live matching pod is found and hydrated, so the
/// row converges. The routability consult must run exactly once and precede
/// the converge CAS.
#[tokio::test(flavor = "current_thread")]
async fn v22_alive_unroutable_restoring_hydrates_then_converges() {
    let sink = FakeSink::new();
    let alive = unroutable_candidate(SandboxLifecycle::Restoring);
    let alive_id = alive.sandbox_id;
    sink.set_probe_verdict(alive_id, ProbeVerdict::AliveOnNode);
    sink.set_routability(alive_id, ExecutionRoutability::Routable);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![alive]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        report.converged_hydrated, 1,
        "hydrated row must converge and be counted as converged_hydrated"
    );
    assert_eq!(
        report.converged, 0,
        "plain converge counter stays untouched"
    );
    assert_eq!(report.deferred, 0);
    assert_eq!(reconciler.sink.routability_calls(), vec![alive_id]);
    assert_eq!(
        reconciler.sink.converge_calls(),
        vec![(alive_id, SandboxLifecycle::Started)]
    );
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// V23 — the stale-restore wedge regression. An alive `restoring` row whose
/// execution is unroutable and for which **no live backing pod exists** must
/// NOT be converged to `started` (that manufactures the
/// started-without-restore-context wedge: every subsequent wake hits the
/// gen≥2 fresh-pod refusal). It defers instead, leaving the row `restoring` —
/// a state the wake path can recover from.
#[tokio::test(flavor = "current_thread")]
async fn v23_alive_unroutable_restoring_without_pod_defers_no_converge() {
    let sink = FakeSink::new();
    let wedge = unroutable_candidate(SandboxLifecycle::Restoring);
    let wedge_id = wedge.sandbox_id;
    sink.set_probe_verdict(wedge_id, ProbeVerdict::AliveOnNode);
    sink.set_routability(wedge_id, ExecutionRoutability::NotRoutable);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![wedge]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert!(
        reconciler.sink.converge_calls().is_empty(),
        "an unroutable alive restoring row must never be converged to started"
    );
    assert_eq!(report.converged, 0);
    assert_eq!(report.converged_hydrated, 0);
    assert_eq!(report.deferred, 1, "the row defers to the next tick");
    assert_eq!(reconciler.sink.routability_calls(), vec![wedge_id]);
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.revert_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// V24 — a DB-routable alive `restoring` row takes the plain converge fast
/// path: no routability consult, no hydration.
#[tokio::test(flavor = "current_thread")]
async fn v24_alive_routable_restoring_skips_routability_consult() {
    let sink = FakeSink::new();
    let alive = candidate(SandboxLifecycle::Restoring);
    let alive_id = alive.sandbox_id;
    sink.set_probe_verdict(alive_id, ProbeVerdict::AliveOnNode);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![alive]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.converged, 1);
    assert!(
        reconciler.sink.routability_calls().is_empty(),
        "a DB-routable row must not pay the substrate consult"
    );
    assert_eq!(
        reconciler.sink.converge_calls(),
        vec![(alive_id, SandboxLifecycle::Started)]
    );
}

/// V25 — a routability-check error (kube apiserver unreachable, DB error)
/// flips nothing: no converge, no defer accounting beyond the roll to the
/// next tick.
#[tokio::test(flavor = "current_thread")]
async fn v25_routability_check_error_defers_without_converge() {
    let sink = FakeSink::new();
    let alive = unroutable_candidate(SandboxLifecycle::Restoring);
    let alive_id = alive.sandbox_id;
    sink.set_probe_verdict(alive_id, ProbeVerdict::AliveOnNode);
    sink.set_routability_error(alive_id);
    sink.enqueue_candidates(SandboxLifecycle::Restoring, vec![alive]);
    let reconciler = Reconciler::new(sweep_config(), sink);

    let report = reconciler.tick().await;

    assert!(reconciler.sink.converge_calls().is_empty());
    assert_eq!(report.converged, 0);
    assert_eq!(report.converged_hydrated, 0);
    assert_eq!(reconciler.sink.routability_calls(), vec![alive_id]);
    assert!(reconciler.sink.fail_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

// -----------------------------------------------------------------
// CRP — command-ready-pending reaper (heal vs reap)
// -----------------------------------------------------------------

fn command_ready_pending_config() -> ReconcilerConfig {
    ReconcilerConfig {
        command_ready_pending_enabled: true,
        ..ReconcilerConfig::default()
    }
}

/// CRP1 — a ready probe heals the row: marker cleared, no reap, no cleanup.
#[tokio::test(flavor = "current_thread")]
async fn crp1_ready_probe_heals_marker_without_reaping() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![sandbox_id]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Ready);
    let reconciler = Reconciler::new(command_ready_pending_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.command_ready_healed, 1, "ready probe should heal");
    assert_eq!(report.command_ready_reaped, 0, "ready probe must not reap");
    assert_eq!(reconciler.sink.heal_calls(), vec![sandbox_id]);
    assert!(
        reconciler.sink.reap_calls().is_empty(),
        "a healthy sandbox must never reach the reap CAS"
    );
    assert!(
        reconciler.sink.cleanup_calls().is_empty(),
        "a healed sandbox must not be torn down"
    );
}

/// CRP2 — a wedged probe reaps the row and tears down its backing execution.
#[tokio::test(flavor = "current_thread")]
async fn crp2_wedged_probe_reaps_and_cleans_up() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![sandbox_id]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Wedged);
    let reconciler = Reconciler::new(command_ready_pending_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.command_ready_reaped, 1, "wedged probe should reap");
    assert_eq!(report.command_ready_healed, 0, "wedged probe must not heal");
    assert_eq!(reconciler.sink.reap_calls(), vec![sandbox_id]);
    assert_eq!(
        reconciler.sink.cleanup_calls(),
        vec![sandbox_id],
        "a reaped sandbox's backing execution must be cleaned up"
    );
    assert!(reconciler.sink.heal_calls().is_empty());
}

/// CRP3 — the reaper is gated behind its own enable flag; disabled is a no-op
/// even with aged candidates queued.
#[tokio::test(flavor = "current_thread")]
async fn crp3_disabled_reaper_is_a_noop() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![sandbox_id]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Wedged);
    // Transient aging and command repair remain disabled; no failed-sandbox
    // candidates are present for the default-on cleanup path.
    let reconciler = Reconciler::new(ReconcilerConfig::default(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.command_ready_healed, 0);
    assert_eq!(report.command_ready_reaped, 0);
    assert!(reconciler.sink.reap_calls().is_empty());
    assert!(reconciler.sink.heal_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// CRP4 — when the reap CAS reports "not claimed" (a concurrent access healed
/// the row or transitioned it out of `started` between probe and CAS), no
/// cleanup runs and the row is not counted as reaped.
#[tokio::test(flavor = "current_thread")]
async fn crp4_unclaimed_reap_cas_skips_cleanup() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![sandbox_id]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Wedged);
    sink.set_reap_unclaimed(sandbox_id);
    let reconciler = Reconciler::new(command_ready_pending_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        report.command_ready_reaped, 0,
        "a CAS that claimed nothing must not count as a reap"
    );
    assert_eq!(reconciler.sink.reap_calls(), vec![sandbox_id]);
    assert!(
        reconciler.sink.cleanup_calls().is_empty(),
        "no cleanup when the CAS claimed nothing — the row was healed or moved on under us"
    );
}

/// CRP5 — a mixed batch heals the ready row and reaps the wedged one in the
/// same tick; cleanup runs only for the reaped row.
#[tokio::test(flavor = "current_thread")]
async fn crp5_mixed_batch_heals_ready_and_reaps_wedged() {
    let sink = FakeSink::new();
    let healthy = SandboxId::new();
    let wedged = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![healthy, wedged]);
    sink.set_probe_outcome(healthy, CommandReadyProbe::Ready);
    sink.set_probe_outcome(wedged, CommandReadyProbe::Wedged);
    let reconciler = Reconciler::new(command_ready_pending_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(report.command_ready_healed, 1);
    assert_eq!(report.command_ready_reaped, 1);
    assert_eq!(reconciler.sink.heal_calls(), vec![healthy]);
    assert_eq!(reconciler.sink.reap_calls(), vec![wedged]);
    assert_eq!(
        reconciler.sink.cleanup_calls(),
        vec![wedged],
        "only the reaped row is torn down"
    );
}

/// CRP6 — an unreachable (inconclusive) probe defers: no heal, no reap, no
/// cleanup, counted as deferred. This is the guard against reaping a healthy
/// abandoned sandbox when a single probe lands during a control-plane blip.
#[tokio::test(flavor = "current_thread")]
async fn crp6_unreachable_probe_defers_without_reaping() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![sandbox_id]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Unreachable);
    let reconciler = Reconciler::new(command_ready_pending_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        report.command_ready_deferred, 1,
        "unreachable probe should defer"
    );
    assert_eq!(
        report.command_ready_reaped, 0,
        "unreachable must never reap"
    );
    assert_eq!(report.command_ready_healed, 0);
    assert!(
        reconciler.sink.reap_calls().is_empty(),
        "an unreachable probe must not reach the reap CAS"
    );
    assert!(reconciler.sink.heal_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

/// CRP7 — an unreachable probe on a row past the terminal age is reaped (the
/// dead-pod backstop), unlike a fresh unreachable row which defers (CRP6).
#[tokio::test(flavor = "current_thread")]
async fn crp7_unreachable_past_terminal_age_is_reaped() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    let config = command_ready_pending_config();
    // One probe-age-eligible row that has been pending past the terminal age.
    let past_terminal = config.command_ready_pending_terminal_age.get() + Duration::from_secs(1);
    sink.enqueue_command_ready_pending_with_ages(vec![(sandbox_id, past_terminal)]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Unreachable);
    let reconciler = Reconciler::new(config, sink);

    let report = reconciler.tick().await;

    assert_eq!(
        report.command_ready_reaped, 1,
        "an unreachable row past the terminal age must be reaped"
    );
    assert_eq!(
        report.command_ready_deferred, 0,
        "terminal rows must not defer"
    );
    assert_eq!(reconciler.sink.reap_calls(), vec![sandbox_id]);
    assert_eq!(
        reconciler.sink.cleanup_calls(),
        vec![sandbox_id],
        "a terminally-reaped row's pod must be cleaned up"
    );
}

// -----------------------------------------------------------------
// U — Unrestorable-restoring sweep (probe-free phase)
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn u1_unrestorable_candidate_is_failed_and_counted() {
    let sink = FakeSink::new();
    let id = SandboxId::new();
    sink.enqueue_unrestorable(vec![(id, Duration::from_secs(1234))]);
    let reconciler = Reconciler::new(sweep_config(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.unrestorable_failed, 1);
    assert_eq!(reconciler.sink.unrestorable_fail_calls(), vec![id]);
    // Probe-free and cleanup-free by design: the predicate proves there is
    // no node to probe and no execution to retire.
    assert!(reconciler.sink.probe_calls().is_empty());
    assert!(reconciler.sink.cleanup_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn u2_unrestorable_cas_lost_is_not_counted() {
    let sink = FakeSink::new();
    let id = SandboxId::new();
    sink.enqueue_unrestorable(vec![(id, Duration::from_secs(1234))]);
    sink.set_cas_unclaimed(id);
    let reconciler = Reconciler::new(sweep_config(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.unrestorable_failed, 0);
    assert_eq!(
        reconciler.sink.unrestorable_fail_calls(),
        vec![id],
        "the CAS was attempted; the row changing underneath is what stopped it"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn u3_unrestorable_phase_disabled_by_its_own_knob() {
    let sink = FakeSink::new();
    sink.enqueue_unrestorable(vec![(SandboxId::new(), Duration::from_secs(1234))]);
    let mut config = sweep_config();
    config.unrestorable_restoring_enabled = false;
    let reconciler = Reconciler::new(config, sink);
    let report = reconciler.tick().await;
    assert_eq!(report.unrestorable_failed, 0);
    assert!(
        reconciler.sink.unrestorable_list_calls().is_empty(),
        "the disabled phase must not even list candidates"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn u4_unrestorable_phase_gated_behind_master_enable() {
    let sink = FakeSink::new();
    sink.enqueue_unrestorable(vec![(SandboxId::new(), Duration::from_secs(1234))]);
    let mut config = sweep_config();
    config.enabled = false;
    let reconciler = Reconciler::new(config, sink);
    let report = reconciler.tick().await;
    assert_eq!(report.unrestorable_failed, 0);
    assert!(reconciler.sink.unrestorable_list_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn u5_unrestorable_list_error_defers_to_next_tick() {
    let sink = FakeSink::new();
    sink.set_unrestorable_list_error();
    let reconciler = Reconciler::new(sweep_config(), sink);
    let report = reconciler.tick().await;
    assert_eq!(report.unrestorable_failed, 0);
    assert!(reconciler.sink.unrestorable_fail_calls().is_empty());
}

// -----------------------------------------------------------------
// F — Pod-identity fence on the retire
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn f1_every_retire_carries_the_observed_pod_identity() {
    let sink = FakeSink::new();
    let aged = candidate(SandboxLifecycle::Creating);
    let reaped = SandboxId::new();
    let failed = SandboxId::new();
    sink.enqueue_candidates(SandboxLifecycle::Creating, vec![aged.clone()]);
    sink.enqueue_command_ready_pending(vec![reaped]);
    sink.set_probe_outcome(reaped, CommandReadyProbe::Wedged);
    sink.enqueue_failed_cleanup(vec![failed_candidate(failed)]);
    let mut config =
        config_with_all_overrides(PositiveDuration::new_panic(Duration::from_secs(60)));
    config.failed_cleanup_enabled = true;
    config.command_ready_pending_enabled = true;
    let reconciler = Reconciler::new(config, sink);

    reconciler.tick().await;

    // Both `cleanup_after_sweep` callers (age-out, command-ready reap) and the
    // failed-cleanup pass must hand the retire the identity their candidate
    // select observed. Dropping it anywhere re-opens the window this fence
    // closes, and the substrate cannot tell a missing fence from a candidate
    // that legitimately had none.
    assert_eq!(
        reconciler.sink.cleanup_calls(),
        vec![aged.sandbox_id, reaped],
        "both cleanup_after_sweep paths must run for this fixture"
    );
    assert_eq!(
        reconciler.sink.cleanup_fences(),
        vec![pod_fence(), pod_fence()],
        "every cleanup_after_sweep call must carry its candidate's pod fence"
    );
    assert_eq!(
        reconciler.sink.failed_execution_cleanup_fences(),
        vec![pod_fence()],
        "the failed-cleanup pass must carry its candidate's pod fence"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn f2_pod_identity_defer_is_not_a_cleanup_failure() {
    let sink = FakeSink::new();
    let aged = candidate(SandboxLifecycle::Creating);
    sink.enqueue_candidates(SandboxLifecycle::Creating, vec![aged.clone()]);
    sink.set_cleanup_pod_identity_changed(aged.sandbox_id, PodFenceMismatch::ExecutionSuperseded);
    let reconciler = Reconciler::new(
        config_with_all_overrides(PositiveDuration::new_panic(Duration::from_secs(60))),
        sink,
    );

    let report = reconciler.tick().await;

    assert_eq!(
        report.cleanup_pod_identity_deferred, 1,
        "a refused retire is a defer and must be counted as one"
    );
    assert_eq!(
        report.cleanup_failures, 0,
        "the substrate reported no failure; counting one would page on a healthy race"
    );
    assert_eq!(
        report.aged_out, 0,
        "a fence known lost before the CAS must leave the lifecycle alone — nothing walks a \
         failed row back"
    );
    assert!(
        reconciler.sink.fail_calls().is_empty(),
        "and the CAS must not run at all"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn f3_failed_cleanup_pod_identity_defer_is_not_a_failure() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_failed_cleanup(vec![failed_candidate(sandbox_id)]);
    sink.set_cleanup_pod_identity_changed(sandbox_id, PodFenceMismatch::PodUidChanged);
    let mut config =
        config_with_all_overrides(PositiveDuration::new_panic(Duration::from_secs(60)));
    config.failed_cleanup_enabled = true;
    let reconciler = Reconciler::new(config, sink);

    let report = reconciler.tick().await;

    assert_eq!(report.failed_cleanup_pod_identity_deferred, 1);
    assert_eq!(
        report.failed_cleanup_failures, 0,
        "a refused retire must not be metered as a cleanup failure"
    );
    assert_eq!(
        report.failed_cleanup_deferred, 0,
        "the probe gate let this one through; the fence is what stopped it, and the two \
         deferrals are different decisions"
    );
}

/// A lost fence must stop the reap before its CAS, not after. That CAS flips
/// the row to `failed`, and the transition's trigger clears
/// `command_ready_pending` — so running it and only then refusing the teardown
/// would strip the marker from a sandbox that is still wedged, and the
/// failed-cleanup pass repairing the row back to `started` would drop it out
/// of this sweep for good.
#[tokio::test(flavor = "current_thread")]
async fn f4_command_ready_reap_defers_on_pod_identity_without_failing() {
    let sink = FakeSink::new();
    let sandbox_id = SandboxId::new();
    sink.enqueue_command_ready_pending(vec![sandbox_id]);
    sink.set_probe_outcome(sandbox_id, CommandReadyProbe::Wedged);
    sink.set_cleanup_pod_identity_changed(sandbox_id, PodFenceMismatch::PodResourceVersionChanged);
    let reconciler = Reconciler::new(command_ready_pending_config(), sink);

    let report = reconciler.tick().await;

    assert_eq!(
        report.command_ready_reaped, 0,
        "the row must keep its marker and its lifecycle when the fence is already lost"
    );
    assert!(
        reconciler.sink.reap_calls().is_empty(),
        "the reap CAS must not run once the teardown is known to be refused"
    );
    assert_eq!(report.cleanup_pod_identity_deferred, 1);
    assert_eq!(report.cleanup_failures, 0);
}

#[test]
fn f5_only_an_observed_pod_yields_a_verifiable_fence() {
    let sandbox_id = SandboxId::new();
    let pod_name = PodName::parse("space-sbx-observed-g1").expect("pod name");
    let observed = HashMap::from([(
        pod_name.clone(),
        ObservedPodIdentity {
            pod_uid: PodUid::parse("uid-live").expect("pod uid"),
            resource_version: PodResourceVersion::parse("8801").ok(),
        },
    )]);

    // `sandbox_k8s_executions` has no resourceVersion column, so only the live
    // observation can tell a pod that *moved* — a Pending pod becoming Running
    // keeps its UID, and that is the recovery this sweep must not destroy.
    let RetireFence::Pod(fence) = candidate_pod_fence_with_observation(
        sandbox_id,
        Some(pod_name.as_str().to_string()),
        None,
        None,
        PodUid::parse("uid-live").ok(),
        None,
        &observed,
    ) else {
        panic!("an observed pod must yield a pod fence");
    };
    assert_eq!(fence.pod_uid.as_ref().map(PodUid::as_str), Some("uid-live"));
    assert_eq!(
        fence
            .resource_version
            .as_ref()
            .map(PodResourceVersion::as_str),
        Some("8801"),
        "without this the fence cannot notice a Pending pod that came up"
    );

    // No observation: the fence must fail closed, not reach for a persisted
    // UID. A UID cannot tell a Pending pod from the same pod now Running, so
    // an unobserved candidate gets the name-only fence — which refuses a live
    // pod outright and acts only on an absent one.
    let RetireFence::Pod(fence) = candidate_pod_fence_with_observation(
        sandbox_id,
        Some(pod_name.as_str().to_string()),
        None,
        None,
        PodUid::parse("uid-live").ok(),
        None,
        &HashMap::new(),
    ) else {
        panic!("an unobserved pod must still fence on its name");
    };
    assert_eq!(
        fence.pod_uid, None,
        "a persisted UID here would authorize a teardown whose freshness nothing proved"
    );
    assert_eq!(fence.resource_version, None);
}

// -----------------------------------------------------------------
// R — Postgres-backed sink (drift safety + trigger semantics)
// -----------------------------------------------------------------

mod activation_owner;
mod crashed_create;
mod episode_deadline;

mod postgres_backed {

    use super::super::store::{
        AgedTransientRow, HEAL_COMMAND_READY_PENDING_SQL, LIST_COMMAND_READY_PENDING_SQL,
        LIST_FAILED_ACTIVE_EXECUTIONS_SQL, NOTE_FAILED_CLEANUP_ATTEMPT_SQL,
        REAP_COMMAND_READY_PENDING_SQL, REPAIR_FAILED_ACTIVE_EXECUTION_SQL,
        converge_restoring_query, fail_candidate_query, fail_unrestorable_restoring_query,
        failed_active_execution_scope, list_aged_transient_query,
        list_unrestorable_restoring_query, revert_to_suspended_query,
        undo_revert_to_suspended_query,
    };
    use super::*;
    use pplx_space_common_core::types::SandboxLifecycle;
    use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId, SubstrateKind};
    use pplx_space_db_test_fixtures::try_pg_test_db;
    use sqlx::PgPool;

    use pplx_space_test_env::TestResult;

    #[tokio::test(flavor = "current_thread")]
    async fn split_operation_expired_stopped_row_has_no_transient_sweep_owner_proof() -> TestResult
    {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let id = SandboxId::new();
        let route = Uuid::now_v7();
        let operation = Uuid::now_v7();
        insert_sandbox_with_lifecycle_and_managed_by(
            &db.pool,
            id,
            SandboxLifecycle::Stopped,
            3600,
            "k8s",
        )
        .await?;
        insert_execution(&db.pool, id, true, 3600, Some("node-proof")).await?;
        set_execution_pod_state(&db.pool, id, "running", Some("10.90.0.9")).await?;
        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE sandbox_k8s_executions
                SET route_activation_id = $2, activation_attempt_id = $3,
                    activation_attempt_expires_at = NOW() - INTERVAL '1 hour',
                    activation_attempt_target_state = 'started'
              WHERE sandbox_id = $1"
        ))
        .bind(id)
        .bind(route)
        .bind(operation)
        .execute(&db.pool)
        .await?;
        assert!(
            sweep_config()
                .resolve_age(SandboxLifecycle::Stopped)
                .is_none()
        );
        for _ in 0..3 {
            for lifecycle in SWEEPABLE_LIFECYCLES {
                let candidates: Vec<AgedTransientRow> =
                    sqlx::query_as(sqlx::AssertSqlSafe(list_aged_transient_query(*lifecycle)))
                        .bind("k8s")
                        .bind(lifecycle.to_string())
                        .bind(1_i64)
                        .bind(1000_i64)
                        .bind(SubstrateKind::Kubernetes)
                        .bind(None::<i64>)
                        .fetch_all(&db.pool)
                        .await?;
                assert!(candidates.iter().all(|row| row.id != id));
            }
        }
        let row: (bool, Option<Uuid>) = sqlx::query_as(pplx_space_common_core::sql!(
            "SELECT active, activation_attempt_id FROM sandbox_k8s_executions WHERE sandbox_id = $1"
        ))
        .bind(id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(row, (true, Some(operation)));
        assert_eq!(lifecycle_of(&db.pool, id).await?, "stopped");
        Ok(())
    }

    async fn insert_sandbox_with_lifecycle(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        lifecycle_changed_at_offset_secs: i64,
    ) -> TestResult {
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            sandbox_id,
            lifecycle,
            lifecycle_changed_at_offset_secs,
            "space",
        )
        .await
    }

    async fn insert_sandbox_with_lifecycle_and_managed_by(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        lifecycle_changed_at_offset_secs: i64,
        managed_by: &str,
    ) -> TestResult {
        // Use the first seeded user/org from the baseline migration. The
        // explicit `network_policy_generation` is required because migration
        // 034 drops the column's DEFAULT after the backfill, so fresh INSERTs
        // that don't supply a value fail the NOT NULL check.
        sqlx::query(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, network_policy_generation, lifecycle_changed_at)
             VALUES ($1,
                     '00000000-0000-0000-0000-000000000010',
                     '00000000-0000-0000-0000-000000000001',
                     $2,
                     $3,
                     1,
                     NOW() - make_interval(secs => $4::BIGINT))",
        )
        .bind(sandbox_id)
        .bind(managed_by)
        .bind(lifecycle.to_string())
        .bind(lifecycle_changed_at_offset_secs)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_execution(
        pool: &PgPool,
        sandbox_id: SandboxId,
        active: bool,
        updated_at_offset_secs: i64,
        k8s_node_name: Option<&str>,
    ) -> TestResult {
        insert_execution_with_pod_uid(
            pool,
            sandbox_id,
            active,
            updated_at_offset_secs,
            k8s_node_name,
            None,
        )
        .await
    }

    /// `pod_uid` is nullable in `sandbox_k8s_executions` — a pod's UID is
    /// persisted after creation — so both states are reachable for a wedged
    /// row and the candidate select has to carry either.
    async fn insert_execution_with_pod_uid(
        pool: &PgPool,
        sandbox_id: SandboxId,
        active: bool,
        updated_at_offset_secs: i64,
        k8s_node_name: Option<&str>,
        pod_uid: Option<&str>,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, active, updated_at,
                 k8s_node_name, pod_uid
             )
             VALUES (
                 $1,
                 1,
                 $2,
                 'sandboxes',
                 $3,
                 NOW() - make_interval(secs => $4::BIGINT),
                 $5,
                 $6
             )",
        )
        .bind(sandbox_id)
        .bind(format!("sandbox-{sandbox_id}"))
        .bind(active)
        .bind(updated_at_offset_secs)
        .bind(k8s_node_name)
        .bind(pod_uid)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// List candidates with the production query for `lifecycle`, scoped to
    /// the default test substrate (`managed_by = 'space'`), 60s threshold.
    async fn list_candidates(
        pool: &PgPool,
        lifecycle: SandboxLifecycle,
        age_secs: i64,
    ) -> TestResult<Vec<AgedTransientRow>> {
        Ok(
            sqlx::query_as(sqlx::AssertSqlSafe(list_aged_transient_query(lifecycle)))
                .bind("space")
                .bind(lifecycle.to_string())
                .bind(age_secs)
                .bind(1000_i64)
                .bind(SubstrateKind::Kubernetes)
                .bind(None::<i64>)
                .fetch_all(pool)
                .await?,
        )
    }

    async fn listed_ids(
        pool: &PgPool,
        lifecycle: SandboxLifecycle,
        age_secs: i64,
    ) -> TestResult<BTreeSet<SandboxId>> {
        Ok(list_candidates(pool, lifecycle, age_secs)
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect())
    }

    /// Run the production per-row fail CAS; returns whether it claimed.
    async fn fail_candidate(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
        age_secs: i64,
    ) -> TestResult<bool> {
        let claimed: Option<String> = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(
            fail_candidate_query(lifecycle, FailGuard::Aged),
        ))
        .bind("space")
        .bind(lifecycle.to_string())
        .bind(age_secs)
        .bind(sandbox_id)
        .fetch_optional(pool)
        .await?;
        Ok(claimed.is_some())
    }

    async fn lifecycle_of(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT lifecycle FROM sandboxes WHERE id = $1")
                .bind(sandbox_id)
                .fetch_one(pool)
                .await?,
        )
    }

    /// The candidate select lists only rows whose age exceeds the threshold,
    /// the trigger keeps `lifecycle_changed_at` honest as a row changes
    /// state, and the per-row fail CAS no-ops when the row changed between
    /// select and act.
    #[tokio::test(flavor = "current_thread")]
    async fn r1_candidate_select_and_fail_cas_leave_drifted_rows_alone() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let stuck = SandboxId::new();
        let drifted = SandboxId::new();
        let fresh = SandboxId::new();
        let raced = SandboxId::new();
        // stuck: pausing for 300s — past the 60s threshold.
        insert_sandbox_with_lifecycle(pool, stuck, SandboxLifecycle::Pausing, 300).await?;
        // drifted: was pausing 300s ago, but in the meantime moved to
        // `paused` — the trigger should have reset lifecycle_changed_at,
        // taking it out of the select's range.
        insert_sandbox_with_lifecycle(pool, drifted, SandboxLifecycle::Pausing, 300).await?;
        sqlx::query("UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1")
            .bind(drifted)
            .execute(pool)
            .await?;
        // fresh: pausing for only 10s.
        insert_sandbox_with_lifecycle(pool, fresh, SandboxLifecycle::Pausing, 10).await?;
        // raced: aged like stuck, but will transition between select and act.
        insert_sandbox_with_lifecycle(pool, raced, SandboxLifecycle::Pausing, 300).await?;

        let listed = listed_ids(pool, SandboxLifecycle::Pausing, 60).await?;
        assert_eq!(
            listed,
            [stuck, raced].into_iter().collect::<BTreeSet<SandboxId>>(),
            "only aged, still-pausing rows are candidates"
        );

        // The race: `raced` completes its transition after the select.
        sqlx::query("UPDATE sandboxes SET lifecycle = 'paused' WHERE id = $1")
            .bind(raced)
            .execute(pool)
            .await?;
        assert!(
            !fail_candidate(pool, raced, SandboxLifecycle::Pausing, 60).await?,
            "the fail CAS must claim nothing once the row moved on"
        );
        assert_eq!(lifecycle_of(pool, raced).await?, "paused");

        assert!(
            fail_candidate(pool, stuck, SandboxLifecycle::Pausing, 60).await?,
            "the genuinely stuck row is claimed"
        );
        assert_eq!(lifecycle_of(pool, stuck).await?, "failed");
        assert_eq!(lifecycle_of(pool, drifted).await?, "paused");
        assert_eq!(lifecycle_of(pool, fresh).await?, "pausing");
        Ok(())
    }

    /// Changing the `lifecycle` column updates `lifecycle_changed_at` via
    /// the BEFORE UPDATE trigger.
    #[tokio::test(flavor = "current_thread")]
    async fn r2_trigger_resets_lifecycle_changed_at_on_lifecycle_change() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let id = SandboxId::new();
        insert_sandbox_with_lifecycle(&db.pool, id, SandboxLifecycle::Creating, 1_000).await?;

        let before: time::OffsetDateTime =
            sqlx::query_scalar("SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1")
                .bind(id)
                .fetch_one(&db.pool)
                .await?;

        sqlx::query("UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1")
            .bind(id)
            .execute(&db.pool)
            .await?;

        let after: time::OffsetDateTime =
            sqlx::query_scalar("SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1")
                .bind(id)
                .fetch_one(&db.pool)
                .await?;
        assert!(
            after > before,
            "trigger must bump lifecycle_changed_at when lifecycle changes"
        );
        Ok(())
    }

    /// Updates that don't touch `lifecycle` leave `lifecycle_changed_at`
    /// alone — otherwise unrelated bookkeeping (placement, env, …) would
    /// make every row look fresh forever and silently disable the sweep.
    #[tokio::test(flavor = "current_thread")]
    async fn r3_trigger_does_not_touch_lifecycle_changed_at_on_unrelated_update() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let id = SandboxId::new();
        insert_sandbox_with_lifecycle(&db.pool, id, SandboxLifecycle::Pausing, 600).await?;
        let before: time::OffsetDateTime =
            sqlx::query_scalar("SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1")
                .bind(id)
                .fetch_one(&db.pool)
                .await?;

        sqlx::query("UPDATE sandboxes SET placement = 'on_node' WHERE id = $1")
            .bind(id)
            .execute(&db.pool)
            .await?;

        let after: time::OffsetDateTime =
            sqlx::query_scalar("SELECT lifecycle_changed_at FROM sandboxes WHERE id = $1")
                .bind(id)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(
            before, after,
            "lifecycle_changed_at must be untouched when lifecycle did not change"
        );
        Ok(())
    }

    /// The candidate select filters on `managed_by` so a controller running
    /// with one substrate doesn't claim rows owned by another. Without this
    /// scope, a k8s cluster-controller would act on stale direct-substrate
    /// rows and call its own (Kubernetes-specific) cleanup against an
    /// executor it doesn't own.
    #[tokio::test(flavor = "current_thread")]
    async fn r4_candidate_select_scoped_to_substrate_managed_by() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let k8s_stuck = SandboxId::new();
        let space_stuck = SandboxId::new();
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            k8s_stuck,
            SandboxLifecycle::Pausing,
            300,
            "k8s",
        )
        .await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            space_stuck,
            SandboxLifecycle::Pausing,
            300,
            "space",
        )
        .await?;

        // Run the production select scoped to the k8s substrate; only the
        // k8s-owned row should be a candidate.
        let listed: Vec<AgedTransientRow> = sqlx::query_as(sqlx::AssertSqlSafe(
            list_aged_transient_query(SandboxLifecycle::Pausing),
        ))
        .bind("k8s")
        .bind(SandboxLifecycle::Pausing.to_string())
        .bind(60_i64)
        .bind(1000_i64)
        .bind(SubstrateKind::Kubernetes)
        .bind(None::<i64>)
        .fetch_all(pool)
        .await?;
        let listed_ids: Vec<SandboxId> = listed.into_iter().map(|row| row.id).collect();
        assert_eq!(listed_ids, vec![k8s_stuck]);

        // And the k8s-scoped fail CAS cannot claim the space-owned row.
        let claimed: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(
            fail_candidate_query(SandboxLifecycle::Pausing, FailGuard::Aged),
        ))
        .bind("k8s")
        .bind(SandboxLifecycle::Pausing.to_string())
        .bind(60_i64)
        .bind(space_stuck)
        .fetch_optional(pool)
        .await?;
        assert!(claimed.is_none());
        assert_eq!(lifecycle_of(pool, space_stuck).await?, "pausing");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r5_failed_cleanup_scans_only_k8s_failed_active_executions_and_rotates() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let oldest_eligible = SandboxId::new();
        let newer_eligible = SandboxId::new();
        let direct = SandboxId::new();
        let deleting = SandboxId::new();
        let inactive = SandboxId::new();

        insert_sandbox_with_lifecycle_and_managed_by(
            &db.pool,
            oldest_eligible,
            SandboxLifecycle::Failed,
            300,
            "k8s",
        )
        .await?;
        insert_execution(&db.pool, oldest_eligible, true, 300, Some("node-1")).await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            &db.pool,
            newer_eligible,
            SandboxLifecycle::Failed,
            30,
            "k8s",
        )
        .await?;
        insert_execution(&db.pool, newer_eligible, true, 30, None).await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            &db.pool,
            direct,
            SandboxLifecycle::Failed,
            400,
            "space",
        )
        .await?;
        insert_execution(&db.pool, direct, true, 400, Some("node-1")).await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            &db.pool,
            deleting,
            SandboxLifecycle::Deleting,
            500,
            "k8s",
        )
        .await?;
        insert_execution(&db.pool, deleting, true, 500, Some("node-1")).await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            &db.pool,
            inactive,
            SandboxLifecycle::Failed,
            600,
            "k8s",
        )
        .await?;
        insert_execution(&db.pool, inactive, false, 600, Some("node-1")).await?;

        let listed: Vec<(SandboxId, Option<String>)> = sqlx::query_as(
            "SELECT s.id, e.k8s_node_name
             FROM sandboxes s
             JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
             WHERE s.managed_by = $1
               AND s.managed_by = 'k8s'
               AND s.lifecycle = 'failed'
               AND e.active
             ORDER BY e.updated_at ASC, s.id
             LIMIT $2",
        )
        .bind("k8s")
        .bind(10_i64)
        .fetch_all(&db.pool)
        .await?;
        assert_eq!(
            listed,
            vec![
                (oldest_eligible, Some("node-1".to_string())),
                (newer_eligible, None),
            ],
            "eligible rows come back oldest-first, each with its execution's node"
        );

        sqlx::query(
            "UPDATE sandbox_k8s_executions e
             SET updated_at = NOW()
             FROM sandboxes s
             WHERE e.sandbox_id = s.id
               AND s.id = $1
               AND s.managed_by = $2
               AND s.managed_by = 'k8s'
               AND s.lifecycle = 'failed'
               AND e.active",
        )
        .bind(oldest_eligible)
        .bind("k8s")
        .execute(&db.pool)
        .await?;

        let relisted: Vec<(SandboxId, Option<String>)> = sqlx::query_as(
            "SELECT s.id, e.k8s_node_name
             FROM sandboxes s
             JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
             WHERE s.managed_by = $1
               AND s.managed_by = 'k8s'
               AND s.lifecycle = 'failed'
               AND e.active
             ORDER BY e.updated_at ASC, s.id
             LIMIT $2",
        )
        .bind("k8s")
        .bind(10_i64)
        .fetch_all(&db.pool)
        .await?;
        let relisted_ids: Vec<SandboxId> = relisted.into_iter().map(|(id, _)| id).collect();
        assert_eq!(relisted_ids, vec![newer_eligible, oldest_eligible]);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r16_failed_active_repair_cas_requires_k8s_failed_active_execution() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let eligible = SandboxId::new();
        let direct = SandboxId::new();
        let already_started = SandboxId::new();
        let inactive = SandboxId::new();
        let wrong_placement = SandboxId::new();
        let active_node_id = NodeId::new();
        let target_node_id = NodeId::new();

        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            eligible,
            SandboxLifecycle::Failed,
            300,
            "k8s",
        )
        .await?;
        sqlx::query(
            "UPDATE sandboxes
             SET placement = 'on_node',
                 active_node_id = $2,
                 target_node_id = $3,
                 placement_changed_at = TIMESTAMPTZ '2026-06-29 12:00:00+00'
             WHERE id = $1",
        )
        .bind(eligible)
        .bind(active_node_id)
        .bind(target_node_id)
        .execute(pool)
        .await?;
        let (
            expected_placement,
            expected_active_node_id,
            expected_target_node_id,
            expected_placement_changed_at,
        ): (
            String,
            Option<NodeId>,
            Option<NodeId>,
            Option<time::OffsetDateTime>,
        ) = sqlx::query_as(
            "SELECT placement, active_node_id, target_node_id, placement_changed_at
             FROM sandboxes
             WHERE id = $1",
        )
        .bind(eligible)
        .fetch_one(pool)
        .await?;
        insert_execution(pool, eligible, true, 300, Some("node-1")).await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            direct,
            SandboxLifecycle::Failed,
            300,
            "space",
        )
        .await?;
        insert_execution(pool, direct, true, 300, Some("node-1")).await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            already_started,
            SandboxLifecycle::Started,
            300,
            "k8s",
        )
        .await?;
        insert_execution(pool, already_started, true, 300, Some("node-1")).await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            inactive,
            SandboxLifecycle::Failed,
            300,
            "k8s",
        )
        .await?;
        insert_execution(pool, inactive, false, 300, Some("node-1")).await?;
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            wrong_placement,
            SandboxLifecycle::Failed,
            300,
            "k8s",
        )
        .await?;
        insert_execution(pool, wrong_placement, true, 300, Some("node-1")).await?;

        async fn repair(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Option<SandboxId>> {
            Ok(
                sqlx::query_scalar::<_, SandboxId>(REPAIR_FAILED_ACTIVE_EXECUTION_SQL)
                    .bind(sandbox_id)
                    .bind("k8s")
                    .fetch_optional(pool)
                    .await?,
            )
        }

        assert_eq!(repair(pool, eligible).await?, Some(eligible));
        let (
            lifecycle,
            placement,
            repaired_active_node_id,
            repaired_target_node_id,
            repaired_placement_changed_at,
        ): (
            String,
            String,
            Option<NodeId>,
            Option<NodeId>,
            Option<time::OffsetDateTime>,
        ) = sqlx::query_as(
            "SELECT lifecycle, placement, active_node_id, target_node_id, placement_changed_at
             FROM sandboxes
             WHERE id = $1",
        )
        .bind(eligible)
        .fetch_one(pool)
        .await?;
        assert_eq!(lifecycle, "started");
        assert_eq!(placement, expected_placement);
        assert_eq!(repaired_active_node_id, expected_active_node_id);
        assert_eq!(repaired_target_node_id, expected_target_node_id);
        assert_eq!(repaired_placement_changed_at, expected_placement_changed_at);

        assert_eq!(repair(pool, direct).await?, None);
        assert_eq!(repair(pool, already_started).await?, None);
        assert_eq!(repair(pool, inactive).await?, None);
        assert_eq!(repair(pool, wrong_placement).await?, None);
        assert_eq!(lifecycle_of(pool, direct).await?, "failed");
        assert_eq!(lifecycle_of(pool, already_started).await?, "started");
        assert_eq!(lifecycle_of(pool, inactive).await?, "failed");
        let (wrong_lifecycle, wrong_placement_value): (String, String) =
            sqlx::query_as("SELECT lifecycle, placement FROM sandboxes WHERE id = $1")
                .bind(wrong_placement)
                .fetch_one(pool)
                .await?;
        assert_eq!(wrong_lifecycle, "failed");
        assert_eq!(wrong_placement_value, "unplaced");
        Ok(())
    }

    /// Set the active execution's routability columns directly (the wake
    /// path's `persist_pod_metadata` write shape, collapsed for fixtures).
    async fn set_execution_pod_state(
        pool: &PgPool,
        sandbox_id: SandboxId,
        state: &str,
        pod_ip: Option<&str>,
    ) -> TestResult {
        sqlx::query(
            "UPDATE sandbox_k8s_executions
             SET state = $2, pod_ip = $3::INET
             WHERE sandbox_id = $1 AND active",
        )
        .bind(sandbox_id)
        .bind(state)
        .bind(pod_ip)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// R17: the candidate select's `routable_execution` fact mirrors the
    /// routability contract behind `started` — an active `running` execution
    /// with a `pod_ip` — and exempts non-k8s substrates. This fact is what
    /// keeps the sweeper from converging an alive `restoring` row to a
    /// `started` the Kubernetes substrate can't back.
    #[tokio::test(flavor = "current_thread")]
    async fn r17_candidate_select_resolves_routable_execution_fact() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        let routable = SandboxId::new();
        let no_ip = SandboxId::new();
        let not_running = SandboxId::new();
        let no_execution = SandboxId::new();
        let inactive_only = SandboxId::new();
        let non_k8s = SandboxId::new();

        for id in [routable, no_ip, not_running, no_execution, inactive_only] {
            insert_sandbox_with_lifecycle_and_managed_by(
                pool,
                id,
                SandboxLifecycle::Restoring,
                300,
                "k8s",
            )
            .await?;
        }
        insert_sandbox_with_lifecycle(pool, non_k8s, SandboxLifecycle::Restoring, 300).await?;
        sqlx::query(
            "UPDATE sandboxes
             SET preserve_local_artifact_on_restore_failure = TRUE
             WHERE id = $1",
        )
        .bind(no_execution)
        .execute(pool)
        .await?;

        insert_execution(pool, routable, true, 300, Some("node-1")).await?;
        set_execution_pod_state(pool, routable, "running", Some("10.90.0.9")).await?;
        insert_execution(pool, no_ip, true, 300, Some("node-1")).await?;
        set_execution_pod_state(pool, no_ip, "running", None).await?;
        insert_execution(pool, not_running, true, 300, Some("node-1")).await?;
        set_execution_pod_state(pool, not_running, "creating", Some("10.90.0.10")).await?;
        insert_execution(pool, inactive_only, false, 300, Some("node-1")).await?;
        sqlx::query(
            "UPDATE sandbox_k8s_executions
             SET state = 'running', pod_ip = '10.90.0.11'::INET
             WHERE sandbox_id = $1",
        )
        .bind(inactive_only)
        .execute(pool)
        .await?;

        let k8s_rows: Vec<AgedTransientRow> = sqlx::query_as(sqlx::AssertSqlSafe(
            list_aged_transient_query(SandboxLifecycle::Restoring),
        ))
        .bind("k8s")
        .bind(SandboxLifecycle::Restoring.to_string())
        .bind(60_i64)
        .bind(1000_i64)
        .bind(SubstrateKind::Kubernetes)
        .bind(None::<i64>)
        .fetch_all(pool)
        .await?;
        let fact = |id: SandboxId| {
            k8s_rows
                .iter()
                .find(|row| row.id == id)
                .map(|row| row.routable_execution)
        };
        assert_eq!(fact(routable), Some(true), "running + pod_ip is routable");
        assert_eq!(fact(no_ip), Some(false), "running without pod_ip is not");
        assert_eq!(
            fact(not_running),
            Some(false),
            "an IP on a non-running execution is not routable"
        );
        assert_eq!(fact(no_execution), Some(false), "no execution row at all");
        assert!(
            k8s_rows
                .iter()
                .find(|row| row.id == no_execution)
                .is_some_and(|row| row.fork_restore),
            "restore safety policy must survive after the operation lease is cleared"
        );
        assert_eq!(
            fact(inactive_only),
            Some(false),
            "a routable-looking inactive execution must not count"
        );

        let space_rows = list_candidates(pool, SandboxLifecycle::Restoring, 60).await?;
        let non_k8s_row = space_rows
            .iter()
            .find(|row| row.id == non_k8s)
            .expect("non-k8s restoring row listed");
        assert!(
            non_k8s_row.routable_execution,
            "substrates without the routability contract are unconditionally routable"
        );
        Ok(())
    }

    /// R18: the converge CAS itself must re-check routability at flip time —
    /// the act-phase proof (DB fact or kube-verified hydration) is stale the
    /// moment a concurrent wake replaces the active execution (retiring the
    /// routable row and inserting a fresh `creating` generation with no
    /// `pod_ip`) while the sandbox row is still `restoring`. Without the
    /// in-CAS predicate, the sweep would flip such a row to `started` with an
    /// unroutable active execution — exactly the started-without-restore-
    /// context wedge this feature exists to prevent. An otherwise identical
    /// Direct (`'space'`) row has no routability contract, so the Direct
    /// sweeper's CAS converges it.
    #[tokio::test(flavor = "current_thread")]
    async fn r18_converge_cas_requires_routable_execution_at_flip_time() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        let converge = |sandbox_id: SandboxId, managed_by: &'static str| async move {
            sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(converge_restoring_query()))
                .bind(managed_by)
                .bind(60_i64)
                .bind(sandbox_id)
                .bind("started")
                .bind(SubstrateKind::Kubernetes)
                .fetch_optional(pool)
                .await
        };

        let raced = SandboxId::new();
        let direct_raced = SandboxId::new();
        for (sandbox_id, managed_by) in [(raced, "k8s"), (direct_raced, "space")] {
            // The row the sweeper proved routable at select/hydration time...
            insert_sandbox_with_lifecycle_and_managed_by(
                pool,
                sandbox_id,
                SandboxLifecycle::Restoring,
                300,
                managed_by,
            )
            .await?;
            insert_execution(pool, sandbox_id, true, 300, Some("node-1")).await?;
            set_execution_pod_state(pool, sandbox_id, "running", Some("10.90.0.9")).await?;

            // ...then a concurrent wake replaces the active execution before
            // the converge CAS runs: the routable row is retired and a fresh
            // generation is inserted `creating` with no pod_ip (the shape
            // `replace_active_execution_claim_if_generation` produces).
            sqlx::query(
                "UPDATE sandbox_k8s_executions
                 SET active = FALSE, state = 'suspended', pod_ip = NULL
                 WHERE sandbox_id = $1 AND active",
            )
            .bind(sandbox_id)
            .execute(pool)
            .await?;
            sqlx::query(
                "INSERT INTO sandbox_k8s_executions (
                     sandbox_id, generation, pod_name, pod_namespace, state, active
                 )
                 VALUES ($1, 2, $2, 'sandboxes', 'creating', TRUE)",
            )
            .bind(sandbox_id)
            .bind(format!("sandbox-{sandbox_id}-g2"))
            .execute(pool)
            .await?;
        }

        assert_eq!(
            converge(raced, "k8s").await?,
            None,
            "the converge CAS must claim nothing once the active execution is \
             no longer routable — the in-flight wake owns the row"
        );
        assert_eq!(lifecycle_of(pool, raced).await?, "restoring");
        assert_eq!(
            converge(direct_raced, "space").await?,
            Some(direct_raced),
            "a Direct row has no routability contract to wait on"
        );
        assert_eq!(lifecycle_of(pool, direct_raced).await?, "started");

        // Control: with a routable active execution at flip time, the CAS
        // claims as before.
        let routable = SandboxId::new();
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            routable,
            SandboxLifecycle::Restoring,
            300,
            "k8s",
        )
        .await?;
        insert_execution(pool, routable, true, 300, Some("node-1")).await?;
        set_execution_pod_state(pool, routable, "running", Some("10.90.0.10")).await?;
        assert_eq!(converge(routable, "k8s").await?, Some(routable));
        assert_eq!(lifecycle_of(pool, routable).await?, "started");
        Ok(())
    }

    /// R19: the fence a retire is given comes from the **active** execution
    /// row, read at candidate-select time, and a live observation may only
    /// confirm that row — never stand in for it. Reading it from an inactive
    /// row would fence on a pod that a successor already replaced. A row whose
    /// UID disagrees with the pod that exists, or that has no UID at all,
    /// falls back to the name-only fence rather than adopting the observation:
    /// the row and the pods are two separate reads, so an observation the row
    /// does not claim may belong to a replacement that took the row over in
    /// between. Name-only still fences — dropping to an unfenced retire there
    /// would leave the largest hazard, deleting a successor's pod, wide open
    /// on exactly the rows most likely to have one.
    #[tokio::test(flavor = "current_thread")]
    async fn r19_candidate_select_carries_active_execution_pod_identity() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        let fenced = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, fenced, SandboxLifecycle::Creating, 300).await?;
        insert_execution_with_pod_uid(pool, fenced, true, 300, Some("node-1"), Some("uid-1"))
            .await?;

        let uidless = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, uidless, SandboxLifecycle::Creating, 300).await?;
        insert_execution_with_pod_uid(pool, uidless, true, 300, Some("node-1"), None).await?;

        let inactive_only = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, inactive_only, SandboxLifecycle::Creating, 300).await?;
        insert_execution_with_pod_uid(
            pool,
            inactive_only,
            false,
            300,
            Some("node-1"),
            Some("uid-stale"),
        )
        .await?;

        let rows = list_candidates(pool, SandboxLifecycle::Creating, 60).await?;
        let observed_identity = |pod_name: &str| {
            (
                PodName::parse(pod_name).expect("pod name"),
                ObservedPodIdentity {
                    pod_uid: PodUid::parse("uid-observed").expect("pod uid"),
                    resource_version: PodResourceVersion::parse("55").ok(),
                },
            )
        };
        let fence_of = |id: SandboxId, observed: &HashMap<PodName, ObservedPodIdentity>| {
            rows.iter().find(|row| row.id == id).map(|row| {
                candidate_pod_fence_with_observation(
                    row.id,
                    row.pod_name.clone(),
                    row.activation_attempt_id,
                    row.generation,
                    row.pod_uid.clone(),
                    row.route_activation_id,
                    observed,
                )
            })
        };

        // Row and observation agree: the observation is provably of the
        // execution that was selected, so it upgrades the fence with the
        // resourceVersion no column can supply.
        let agreeing = HashMap::from([(
            PodName::parse(format!("sandbox-{fenced}")).expect("pod name"),
            ObservedPodIdentity {
                pod_uid: PodUid::parse("uid-1").expect("pod uid"),
                resource_version: PodResourceVersion::parse("55").ok(),
            },
        )]);
        let RetireFence::Pod(fence) =
            fence_of(fenced, &agreeing).expect("the fenced row is listed")
        else {
            panic!("an observed active execution must yield a pod fence");
        };
        assert_eq!(fence.pod_name.as_str(), format!("sandbox-{fenced}"));
        assert_eq!(fence.pod_uid.as_ref().map(PodUid::as_str), Some("uid-1"));
        assert_eq!(
            fence
                .resource_version
                .as_ref()
                .map(PodResourceVersion::as_str),
            Some("55")
        );

        // Row and observation disagree: a pod exists under this name that the
        // selected row does not claim. That is what a re-claimed row looks
        // like, and adopting the observation would build a fence matching the
        // replacement exactly.
        let observed = HashMap::from([observed_identity(&format!("sandbox-{fenced}"))]);
        let RetireFence::Pod(disagreeing) =
            fence_of(fenced, &observed).expect("the fenced row is listed")
        else {
            panic!("a disagreeing observation must still fence on the name");
        };
        assert_eq!(
            disagreeing.pod_uid, None,
            "an observation the row does not claim must not become the fence"
        );

        // Same for a row with no UID at all: nothing here can prove the pod
        // that exists belongs to it.
        let observed = HashMap::from([observed_identity(&format!("sandbox-{uidless}"))]);
        let RetireFence::Pod(uidless_fence) =
            fence_of(uidless, &observed).expect("the UID-less row is listed")
        else {
            panic!("a UID-less active execution must still fence");
        };
        assert_eq!(
            uidless_fence.pod_uid, None,
            "a UID the row never recorded cannot be adopted from the reflector"
        );

        // An inactive execution supplies no pod name at all, so there is
        // nothing to observe and the retire must re-prove the absence.
        assert_eq!(
            fence_of(inactive_only, &HashMap::new()).expect("the inactive-execution row is listed"),
            RetireFence::NoExecution,
            "an inactive execution must not supply the fence"
        );

        Ok(())
    }

    /// Set the evacuation marker (`evacuation_restore_target`) on a row without
    /// changing its lifecycle. Keeps both the marker (the clear-trigger only
    /// fires when lifecycle *leaves* the suspend/restore window) and the row's
    /// existing `lifecycle_changed_at` (the touch-trigger only fires when
    /// lifecycle itself changes).
    async fn set_evacuation_marker(
        pool: &PgPool,
        sandbox_id: SandboxId,
        target: &str,
    ) -> TestResult {
        sqlx::query("UPDATE sandboxes SET evacuation_restore_target = $2 WHERE id = $1")
            .bind(sandbox_id)
            .bind(target)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn insert_full_snapshot(
        pool: &PgPool,
        sandbox_id: SandboxId,
        state: &str,
        source_node_id: NodeId,
    ) -> TestResult<SnapshotId> {
        let snapshot_id = SnapshotId::new();
        sqlx::query(
            "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
             VALUES ($1, $2, 'full', $3, $4, NOW())",
        )
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(state)
        .bind(source_node_id)
        .execute(pool)
        .await?;
        Ok(snapshot_id)
    }

    async fn insert_full_snapshot_relative_to_lifecycle(
        pool: &PgPool,
        sandbox_id: SandboxId,
        state: &str,
        source_node_id: NodeId,
        timestamp_offset_secs: i64,
    ) -> TestResult<SnapshotId> {
        let snapshot_id = SnapshotId::new();
        sqlx::query(
            "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
             VALUES (
                 $1,
                 $2,
                 'full',
                 $3,
                 $4,
                 (SELECT lifecycle_changed_at + make_interval(secs => $5::BIGINT)
                    FROM sandboxes WHERE id = $2)
             )",
        )
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(state)
        .bind(source_node_id)
        .bind(timestamp_offset_secs)
        .execute(pool)
        .await?;
        Ok(snapshot_id)
    }

    /// Register a durable storage location for `snapshot_id` — the join half
    /// of the `fresh_ready_snapshot_exists` predicate.
    async fn insert_storage_location(pool: &PgPool, snapshot_id: SnapshotId) -> TestResult {
        sqlx::query(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)",
        )
        .bind(snapshot_id)
        .bind(format!("s3://test-bucket/{snapshot_id}"))
        .execute(pool)
        .await?;
        Ok(())
    }

    /// The evacuation exemption is lifecycle-aware:
    /// - A marked **`restoring`** row is spared (node-drain's restore sweep
    ///   re-drives it; it legitimately runs for the evacuate-timeout window).
    /// - A marked **`suspending`** row is NOT spared by the restore exemption —
    ///   nothing recovers a crashed/hung suspend *with no in-flight upload*, so
    ///   the backstop must list it or it would block the node's drain forever.
    ///   (The narrower in-flight-upload guard is covered by R7.)
    /// - Unmarked rows in either lifecycle are always candidates.
    #[tokio::test(flavor = "current_thread")]
    async fn r6_evacuation_exemption_spares_restoring_but_not_suspending() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        // Restoring: marked → spared, unmarked → listed.
        let marked_restore = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, marked_restore, SandboxLifecycle::Restoring, 300)
            .await?;
        set_evacuation_marker(pool, marked_restore, "started").await?;
        let user_restore = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, user_restore, SandboxLifecycle::Restoring, 300).await?;

        let listed = listed_ids(pool, SandboxLifecycle::Restoring, 60).await?;
        assert_eq!(
            listed,
            [user_restore].into_iter().collect::<BTreeSet<SandboxId>>(),
            "only the unmarked restore is a candidate; the marked restore is exempt"
        );
        assert!(
            !fail_candidate(pool, marked_restore, SandboxLifecycle::Restoring, 60).await?,
            "the fail CAS carries the same exemption as the select"
        );
        assert_eq!(
            lifecycle_of(pool, marked_restore).await?,
            "restoring",
            "node-drain's own evacuation restore must never be aged out by the backstop"
        );

        // Suspending: marked (no in-flight upload) → STILL listed (no other
        // recovery path), unmarked → listed.
        let marked_suspend = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, marked_suspend, SandboxLifecycle::Suspending, 300)
            .await?;
        set_evacuation_marker(pool, marked_suspend, "started").await?;
        let user_suspend = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, user_suspend, SandboxLifecycle::Suspending, 300)
            .await?;

        let listed = listed_ids(pool, SandboxLifecycle::Suspending, 60).await?;
        assert_eq!(
            listed,
            [marked_suspend, user_suspend]
                .into_iter()
                .collect::<BTreeSet<SandboxId>>(),
            "a wedged evacuation-marked suspending row with no in-flight upload must still be \
             a candidate — nothing else recovers it"
        );
        Ok(())
    }

    /// R7: the `suspending` candidate select must not list an
    /// **evacuation-marked** row while its current suspend snapshot is still
    /// recoverable — either `state='creating'` (upload in flight) or
    /// `state='ready'` (durable, awaiting the re-drive). Failing a `creating`
    /// row would orphan the `ready` snapshot the upload is about to produce;
    /// failing a `ready` row (which can happen if a sweeper tick wins the race
    /// against the re-drive once the upload finalizes) clears the evacuation
    /// marker via the failed-transition trigger and blocks recovery. The
    /// node-drain re-drive owns both. Rows with no recoverable current
    /// snapshot (a `failed` snapshot, a stale prior-cycle one, or none) are
    /// still listed, so the drain self-heals; and a plain user-suspend (NULL
    /// marker) is never guarded — the guard is marker-scoped because only
    /// marked rows have a re-drive. Other transient lifecycles carry no such
    /// guard.
    #[tokio::test(flavor = "current_thread")]
    async fn r7_suspending_candidates_guard_inflight_evacuation_upload() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        // Marked + upload in flight → protected (re-drive owns it).
        let marked_uploading = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, marked_uploading, SandboxLifecycle::Suspending, 700)
            .await?;
        set_evacuation_marker(pool, marked_uploading, "started").await?;
        insert_full_snapshot(pool, marked_uploading, "creating", node).await?;

        // Marked + current upload finalized `ready` → still protected; the
        // re-drive is about to advance it, so the sweeper must not fail it first.
        let marked_ready = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, marked_ready, SandboxLifecycle::Suspending, 700)
            .await?;
        set_evacuation_marker(pool, marked_ready, "started").await?;
        insert_full_snapshot(pool, marked_ready, "ready", node).await?;

        // Marked + upload failed → no in-flight upload, must be listed.
        let marked_failed = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, marked_failed, SandboxLifecycle::Suspending, 700)
            .await?;
        set_evacuation_marker(pool, marked_failed, "started").await?;
        insert_full_snapshot(pool, marked_failed, "failed", node).await?;

        // Marked + no snapshot at all → nothing recoverable, must be listed.
        let marked_no_snapshot = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, marked_no_snapshot, SandboxLifecycle::Suspending, 700)
            .await?;
        set_evacuation_marker(pool, marked_no_snapshot, "started").await?;

        // Unmarked (plain user-suspend) + upload in flight → NOT guarded; the
        // guard is marker-scoped, so this keeps its existing behavior.
        let unmarked_uploading = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, unmarked_uploading, SandboxLifecycle::Suspending, 700)
            .await?;
        insert_full_snapshot(pool, unmarked_uploading, "creating", node).await?;

        let listed = listed_ids(pool, SandboxLifecycle::Suspending, 600).await?;
        assert!(
            !listed.contains(&marked_uploading),
            "a marked suspending row with an in-flight upload must NOT be a candidate"
        );
        assert!(
            !listed.contains(&marked_ready),
            "a marked suspending row whose current upload finalized ready must NOT be a \
             candidate (the re-drive owns it)"
        );
        assert!(
            listed.contains(&marked_failed),
            "a marked suspending row whose upload failed must be listed"
        );
        assert!(
            listed.contains(&marked_no_snapshot),
            "a marked suspending row with no upload must be listed"
        );
        assert!(
            listed.contains(&unmarked_uploading),
            "an unmarked suspending row is not guarded and must be listed"
        );

        // The fail CAS repeats the guards, and a claimed row terminalizes.
        assert!(
            !fail_candidate(pool, marked_uploading, SandboxLifecycle::Suspending, 600).await?,
            "the fail CAS must respect the in-flight-upload guard"
        );
        assert!(fail_candidate(pool, marked_failed, SandboxLifecycle::Suspending, 600).await?);
        assert_eq!(lifecycle_of(pool, marked_uploading).await?, "suspending");
        assert_eq!(lifecycle_of(pool, marked_failed).await?, "failed");
        Ok(())
    }

    /// R8: the command-ready-pending reaper SQL.
    ///
    /// - `list` returns only aged `started` rows with the marker set, scoped to
    ///   the controller's `managed_by`.
    /// - `heal` clears the marker and leaves the row `started`.
    /// - `reap` CAS-flips `started` -> `failed`, the
    ///   `sandboxes_clear_command_ready_pending` trigger nulls the marker on
    ///   that transition, and a second reap of the now-`failed` row claims
    ///   nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn r8_command_ready_pending_list_heal_reap() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        let mark_pending = |id: SandboxId| async move {
            sqlx::query("UPDATE sandboxes SET command_ready_pending = true WHERE id = $1")
                .bind(id)
                .execute(pool)
                .await
                .map(|_| ())
        };

        // Aged + marker + our substrate -> the one true candidate.
        let aged_pending = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, aged_pending, SandboxLifecycle::Started, 900).await?;
        mark_pending(aged_pending).await?;
        // Fresh marker -> too young to reap.
        let fresh_pending = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, fresh_pending, SandboxLifecycle::Started, 10).await?;
        mark_pending(fresh_pending).await?;
        // Aged but no marker -> a normal started sandbox, never a candidate.
        let aged_no_marker = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, aged_no_marker, SandboxLifecycle::Started, 900).await?;
        // Aged + marker but a foreign substrate -> out of scope.
        let aged_pending_k8s = SandboxId::new();
        insert_sandbox_with_lifecycle_and_managed_by(
            pool,
            aged_pending_k8s,
            SandboxLifecycle::Started,
            900,
            "k8s",
        )
        .await?;
        mark_pending(aged_pending_k8s).await?;

        let listed: Vec<(SandboxId, i64)> = sqlx::query_as(LIST_COMMAND_READY_PENDING_SQL)
            .bind("space")
            .bind(300_i64)
            .bind(1000_i64)
            .fetch_all(pool)
            .await?;
        let listed_ids: Vec<SandboxId> = listed.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            listed_ids,
            vec![aged_pending],
            "only the aged, marked, in-scope started row is a candidate"
        );
        // The row was inserted 900s in the past; the reported age reflects that.
        assert!(
            listed[0].1 >= 300,
            "reported pending age {}s should be at least the 300s list threshold",
            listed[0].1
        );

        // Heal clears the marker and keeps the row `started`.
        sqlx::query(HEAL_COMMAND_READY_PENDING_SQL)
            .bind(aged_pending)
            .bind("space")
            .execute(pool)
            .await?;
        let (healed_lc, healed_marker): (String, bool) =
            sqlx::query_as("SELECT lifecycle, command_ready_pending FROM sandboxes WHERE id = $1")
                .bind(aged_pending)
                .fetch_one(pool)
                .await?;
        assert_eq!(healed_lc, "started", "heal must not change lifecycle");
        assert!(!healed_marker, "heal must clear the marker");

        // Reap a genuinely wedged row: started + marker, aged.
        let wedged = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, wedged, SandboxLifecycle::Started, 900).await?;
        mark_pending(wedged).await?;
        let claimed: Option<String> = sqlx::query_scalar(REAP_COMMAND_READY_PENDING_SQL)
            .bind(wedged)
            .bind("space")
            .fetch_optional(pool)
            .await?;
        assert!(claimed.is_some(), "the reap CAS must claim the row");
        let (reaped_lc, reaped_marker): (String, bool) =
            sqlx::query_as("SELECT lifecycle, command_ready_pending FROM sandboxes WHERE id = $1")
                .bind(wedged)
                .fetch_one(pool)
                .await?;
        assert_eq!(reaped_lc, "failed", "reap must flip the row to failed");
        assert!(
            !reaped_marker,
            "the clear trigger must null the marker on the started -> failed transition"
        );

        // A second reap of the now-`failed` row claims nothing (idempotent).
        let reclaimed: Option<String> = sqlx::query_scalar(REAP_COMMAND_READY_PENDING_SQL)
            .bind(wedged)
            .bind("space")
            .fetch_optional(pool)
            .await?;
        assert_eq!(
            reclaimed, None,
            "a row already failed must not be re-claimed"
        );
        Ok(())
    }

    /// R9: the candidate select resolves the probe target from the row's
    /// **active** execution (`sandbox_k8s_executions.k8s_node_name`) and
    /// reports the row's age and ready-snapshot fact.
    #[tokio::test(flavor = "current_thread")]
    async fn r9_candidate_select_resolves_node_and_snapshot_facts() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        // Active execution on node-1 + ready durable source snapshot.
        let on_node = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, on_node, SandboxLifecycle::Restoring, 300).await?;
        insert_execution(pool, on_node, true, 300, Some("node-1")).await?;
        let snapshot_id =
            insert_full_snapshot_relative_to_lifecycle(pool, on_node, "ready", node, -60).await?;
        insert_storage_location(pool, snapshot_id).await?;

        // Only an inactive (prior-generation) execution: no node to probe.
        let no_active = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, no_active, SandboxLifecycle::Restoring, 300).await?;
        insert_execution(pool, no_active, false, 300, Some("node-2")).await?;

        let rows = list_candidates(pool, SandboxLifecycle::Restoring, 60).await?;
        let by_id: HashMap<SandboxId, (i64, bool, Option<String>)> = rows
            .into_iter()
            .map(|row| {
                (
                    row.id,
                    (row.age_secs, row.ready_snapshot, row.k8s_node_name),
                )
            })
            .collect();

        let (age_secs, ready, node_name) = by_id.get(&on_node).expect("on_node listed").clone();
        assert!(age_secs >= 300, "age reflects lifecycle_changed_at");
        assert!(ready, "the durable ready snapshot is reported");
        assert_eq!(
            node_name.as_deref(),
            Some("node-1"),
            "the probe target comes from the ACTIVE execution row"
        );

        let (_, ready, node_name) = by_id.get(&no_active).expect("no_active listed").clone();
        assert!(!ready);
        assert_eq!(
            node_name, None,
            "an inactive execution must not supply a probe target"
        );
        Ok(())
    }

    /// R10: the converge CAS respects the restore lease — an unexpired lease
    /// means a restore attempt still owns the row, so the sweep must not
    /// converge under it. A recently expired lease also waits one complete
    /// age window; only then does convergence clear the lease columns.
    #[tokio::test(flavor = "current_thread")]
    async fn r10_converge_cas_respects_restore_lease() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        let converge = |sandbox_id: SandboxId, target: &'static str| async move {
            sqlx::query_scalar::<_, SandboxId>(sqlx::AssertSqlSafe(converge_restoring_query()))
                .bind("space")
                .bind(60_i64)
                .bind(sandbox_id)
                .bind(target)
                .bind(SubstrateKind::Kubernetes)
                .fetch_optional(pool)
                .await
        };

        // Leased: an aged restoring row whose restore lease has not expired.
        let leased = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, leased, SandboxLifecycle::Restoring, 300).await?;
        sqlx::query(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() + INTERVAL '1 hour',
                    restore_lease_token = $2
              WHERE id = $1",
        )
        .bind(leased)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;
        assert_eq!(
            converge(leased, "started").await?,
            None,
            "an unexpired restore lease must block the converge"
        );
        assert_eq!(lifecycle_of(pool, leased).await?, "restoring");

        // A renewal gap is not proof that restore work stopped. A lease that
        // expired only one second ago remains protected for the full age
        // window.
        sqlx::query(
            "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
              WHERE id = $1",
        )
        .bind(leased)
        .execute(pool)
        .await?;
        assert_eq!(
            converge(leased, "started").await?,
            None,
            "a recently expired lease remains inside the quiet window"
        );

        sqlx::query(
            "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '61 seconds'
              WHERE id = $1",
        )
        .bind(leased)
        .execute(pool)
        .await?;
        assert_eq!(
            converge(leased, "started").await?,
            Some(leased),
            "a lease quiet for the complete age window no longer owns the row"
        );
        let (lc, lease_token): (String, Option<Uuid>) =
            sqlx::query_as("SELECT lifecycle, restore_lease_token FROM sandboxes WHERE id = $1")
                .bind(leased)
                .fetch_one(pool)
                .await?;
        assert_eq!(lc, "started");
        assert_eq!(
            lease_token, None,
            "the clear-lease trigger must drop the lease on the transition out of restoring"
        );

        // Fresh restoring rows stay outside the age window.
        let fresh = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, fresh, SandboxLifecycle::Restoring, 10).await?;
        assert_eq!(
            converge(fresh, "started").await?,
            None,
            "a fresh restoring row must not be converged"
        );

        // The CAS is restoring-only: a paused row never converges.
        let paused = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, paused, SandboxLifecycle::Paused, 300).await?;
        assert_eq!(converge(paused, "started").await?, None);
        Ok(())
    }

    /// Run the production revert CAS; returns the write token it produced (if
    /// it claimed).
    async fn revert_candidate_row(
        pool: &PgPool,
        sandbox_id: SandboxId,
        lifecycle: SandboxLifecycle,
    ) -> TestResult<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(revert_to_suspended_query()))
                .bind("space")
                .bind(lifecycle.to_string())
                .bind(60_i64)
                .bind(sandbox_id)
                .bind(None::<i64>)
                .bind(SubstrateKind::Kubernetes)
                .fetch_optional(pool)
                .await?,
        )
    }

    /// The undo has to name the write it is undoing. Binding only the
    /// lifecycle value would let it reach past this sweep's own transition: a
    /// restore that started and then genuinely suspended again reads
    /// `suspended` too, and dragging that back to `restoring` would undo
    /// someone else's correct work. The reap undo carries the identical
    /// predicate.
    #[tokio::test(flavor = "current_thread")]
    async fn the_revert_undo_only_matches_its_own_write() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        let sandbox_id = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, sandbox_id, SandboxLifecycle::Restoring, 300).await?;
        let snapshot =
            insert_full_snapshot_relative_to_lifecycle(pool, sandbox_id, "ready", node, -60)
                .await?;
        insert_storage_location(pool, snapshot).await?;

        let write_token = revert_candidate_row(pool, sandbox_id, SandboxLifecycle::Restoring)
            .await?
            .expect("the revert must claim");
        assert_eq!(lifecycle_of(pool, sandbox_id).await?, "suspended");

        let undo = |token: String| async move {
            sqlx::query_scalar::<_, SandboxId>(
                sqlx::AssertSqlSafe(undo_revert_to_suspended_query()),
            )
            .bind(sandbox_id)
            .bind("space")
            .bind(SandboxLifecycle::Restoring.to_string())
            .bind(token)
            .bind(false)
            .bind(None::<time::OffsetDateTime>)
            .fetch_optional(pool)
            .await
        };

        // A restore starts and genuinely suspends again. Same lifecycle value,
        // different write.
        sqlx::query("UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1")
            .bind(sandbox_id)
            .execute(pool)
            .await?;
        sqlx::query("UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1")
            .bind(sandbox_id)
            .execute(pool)
            .await?;

        assert!(
            undo(write_token).await?.is_none(),
            "the stale token must not match a later suspend"
        );
        assert_eq!(
            lifecycle_of(pool, sandbox_id).await?,
            "suspended",
            "and the later suspend must stand"
        );
        Ok(())
    }

    /// A fork restore carries `preserve_local_artifact_on_restore_failure`, and
    /// leaving `restoring` clears it. An undo that restored only the lifecycle
    /// would return the sandbox without the contract that keeps its local
    /// artifact — a later restore failure would then discard the only copy.
    #[tokio::test(flavor = "current_thread")]
    async fn the_revert_undo_restores_fork_preservation() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        let sandbox_id = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, sandbox_id, SandboxLifecycle::Restoring, 300).await?;
        let snapshot =
            insert_full_snapshot_relative_to_lifecycle(pool, sandbox_id, "ready", node, -60)
                .await?;
        insert_storage_location(pool, snapshot).await?;
        sqlx::query(
            "UPDATE sandboxes SET preserve_local_artifact_on_restore_failure = TRUE WHERE id = $1",
        )
        .bind(sandbox_id)
        .execute(pool)
        .await?;

        let write_token = revert_candidate_row(pool, sandbox_id, SandboxLifecycle::Restoring)
            .await?
            .expect("the revert must claim");
        let cleared: bool = sqlx::query_scalar(
            "SELECT preserve_local_artifact_on_restore_failure FROM sandboxes WHERE id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        assert!(
            !cleared,
            "leaving restoring is what clears the flag — the premise of this test"
        );

        let restored: Option<Uuid> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(undo_revert_to_suspended_query()))
                .bind(sandbox_id)
                .bind("space")
                .bind(SandboxLifecycle::Restoring.to_string())
                .bind(write_token)
                .bind(true)
                .bind(None::<time::OffsetDateTime>)
                .fetch_optional(pool)
                .await?;
        assert!(restored.is_some(), "the undo must claim its own write");

        let preserved: bool = sqlx::query_scalar(
            "SELECT preserve_local_artifact_on_restore_failure FROM sandboxes WHERE id = $1",
        )
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        assert!(
            preserved,
            "the fork's local-artifact contract must come back with its lifecycle"
        );
        Ok(())
    }

    /// Pin `sandbox_id` to `artifact_id` the way suspend completion does.
    async fn pin_local_artifact(
        pool: &PgPool,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
    ) -> TestResult {
        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'full',
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1",
        )
        .bind(sandbox_id)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Insert a ready, durably-registered full snapshot carrying the exact
    /// artifact identity `artifact_id`.
    async fn insert_pinned_ready_snapshot(
        pool: &PgPool,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        source_node_id: NodeId,
    ) -> TestResult<SnapshotId> {
        let snapshot_id = SnapshotId::new();
        sqlx::query(
            "INSERT INTO snapshots
                (id, sandbox_id, type, state, source_node_id, timestamp,
                 local_artifact_id, artifact_kind, artifact_origin)
             VALUES ($1, $2, 'full', 'ready', $3, NOW(), $4, 'full', 'suspend')",
        )
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        insert_storage_location(pool, snapshot_id).await?;
        Ok(snapshot_id)
    }

    async fn insert_pinned_ready_snapshot_relative_to_lifecycle(
        pool: &PgPool,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        source_node_id: NodeId,
        timestamp_offset_secs: i64,
    ) -> TestResult<SnapshotId> {
        let snapshot_id = SnapshotId::new();
        sqlx::query(
            "INSERT INTO snapshots
                (id, sandbox_id, type, state, source_node_id, timestamp,
                 local_artifact_id, artifact_kind, artifact_origin)
             VALUES (
                 $1,
                 $2,
                 'full',
                 'ready',
                 $3,
                 (SELECT lifecycle_changed_at + make_interval(secs => $4::BIGINT)
                    FROM sandboxes WHERE id = $2),
                 $5,
                 'full',
                 'suspend'
             )",
        )
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(source_node_id)
        .bind(timestamp_offset_secs)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        insert_storage_location(pool, snapshot_id).await?;
        Ok(snapshot_id)
    }

    /// R11: the revert CAS requires a ready durable snapshot — and for rows
    /// that carry an artifact identity, only an exact pre-restore artifact
    /// backup counts (the legacy source-window branch applies only when
    /// `current_local_artifact_id` is NULL).
    #[tokio::test(flavor = "current_thread")]
    async fn r11_revert_cas_requires_ready_snapshot_with_artifact_identity() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        // No snapshot at all: the revert must claim nothing.
        let bare = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, bare, SandboxLifecycle::Restoring, 300).await?;
        assert_eq!(
            revert_candidate_row(pool, bare, SandboxLifecycle::Restoring).await?,
            None,
            "a row must never read suspended without a restorable snapshot behind it"
        );
        assert_eq!(lifecycle_of(pool, bare).await?, "restoring");

        // Legacy row (NULL artifact identity) + ready registered snapshot:
        // the pre-restore source window accepts it.
        let legacy = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, legacy, SandboxLifecycle::Restoring, 300).await?;
        let legacy_snapshot =
            insert_full_snapshot_relative_to_lifecycle(pool, legacy, "ready", node, -60).await?;
        insert_storage_location(pool, legacy_snapshot).await?;
        assert!(
            revert_candidate_row(pool, legacy, SandboxLifecycle::Restoring)
                .await?
                .is_some()
        );
        assert_eq!(lifecycle_of(pool, legacy).await?, "suspended");

        // Pinned row (artifact identity set) + only a NULL-identity ready
        // snapshot: the exact-artifact extra guard rejects it.
        let pinned = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, pinned, SandboxLifecycle::Restoring, 300).await?;
        let artifact_id = Uuid::now_v7();
        pin_local_artifact(pool, pinned, artifact_id).await?;
        let stale_snapshot = insert_full_snapshot(pool, pinned, "ready", node).await?;
        insert_storage_location(pool, stale_snapshot).await?;
        assert_eq!(
            revert_candidate_row(pool, pinned, SandboxLifecycle::Restoring).await?,
            None,
            "an artifact-pinned row needs an exact-artifact backup, not any ready snapshot"
        );

        // Register the exact-artifact backup: the revert now claims.
        insert_pinned_ready_snapshot_relative_to_lifecycle(pool, pinned, artifact_id, node, -60)
            .await?;
        assert!(
            revert_candidate_row(pool, pinned, SandboxLifecycle::Restoring)
                .await?
                .is_some()
        );
        assert_eq!(lifecycle_of(pool, pinned).await?, "suspended");
        Ok(())
    }

    /// R12: the revert CAS hard-pins `lifecycle = 'restoring'`. A wedged
    /// `suspending` row whose pinned artifact has an exact ready backup is
    /// the June stale-pin family — the pin is the *previous* cycle's
    /// artifact (nothing updates it on wake, and the write that would have
    /// pinned the new artifact lives in the same flow that wedged), so a
    /// revert would silently restore pre-wake bytes. The CAS must claim
    /// nothing even though the snapshot predicate is satisfied; the loud
    /// fail path stays available.
    #[tokio::test(flavor = "current_thread")]
    async fn r12_revert_cas_is_restoring_only_even_with_exact_pin_backup() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        let wedged = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, wedged, SandboxLifecycle::Suspending, 300).await?;
        let prior_artifact = Uuid::now_v7();
        pin_local_artifact(pool, wedged, prior_artifact).await?;
        insert_pinned_ready_snapshot(pool, wedged, prior_artifact, node).await?;

        assert_eq!(
            revert_candidate_row(pool, wedged, SandboxLifecycle::Suspending).await?,
            None,
            "a non-restoring row must never revert, even with an exact-pin ready backup — \
             the pin is the previous suspend cycle's artifact"
        );
        assert_eq!(lifecycle_of(pool, wedged).await?, "suspending");

        // The loud path is still available for it.
        assert!(
            fail_candidate(pool, wedged, SandboxLifecycle::Suspending, 60).await?,
            "the fail CAS still claims the wedged suspending row"
        );
        assert_eq!(lifecycle_of(pool, wedged).await?, "failed");
        Ok(())
    }

    /// R13: candidate selection and the revert/fail CASes share the converge
    /// CAS's restore-lease quiet window. This protects newer writers that
    /// retain lease expiry while preserving current/legacy writers that clear
    /// the lease and refresh the lifecycle clock.
    #[tokio::test(flavor = "current_thread")]
    async fn r13_revert_and_fail_cas_respect_restore_lease() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        // A legacy writer clears the lease and refreshes the lifecycle clock.
        // The lifecycle age guard remains authoritative for that row shape.
        let legacy_refresh = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, legacy_refresh, SandboxLifecycle::Restoring, 300)
            .await?;
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle_changed_at = NOW(),
                    restore_lease_expires_at = NULL,
                    restore_lease_token = NULL
              WHERE id = $1",
        )
        .bind(legacy_refresh)
        .execute(pool)
        .await?;
        assert!(
            !list_candidates(pool, SandboxLifecycle::Restoring, 60)
                .await?
                .iter()
                .any(|candidate| candidate.id == legacy_refresh),
            "a legacy NULL-lease row with a fresh lifecycle clock is not a candidate"
        );
        assert_eq!(
            revert_candidate_row(pool, legacy_refresh, SandboxLifecycle::Restoring).await?,
            None,
            "the revert CAS keeps the legacy lifecycle age guard"
        );
        assert!(
            !fail_candidate(pool, legacy_refresh, SandboxLifecycle::Restoring, 60).await?,
            "the fail CAS keeps the legacy lifecycle age guard"
        );

        let leased = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, leased, SandboxLifecycle::Restoring, 300).await?;
        let snapshot =
            insert_full_snapshot_relative_to_lifecycle(pool, leased, "ready", node, -60).await?;
        insert_storage_location(pool, snapshot).await?;
        sqlx::query(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() + INTERVAL '1 hour',
                    restore_lease_token = $2
              WHERE id = $1",
        )
        .bind(leased)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;

        assert!(
            !list_candidates(pool, SandboxLifecycle::Restoring, 60)
                .await?
                .iter()
                .any(|candidate| candidate.id == leased),
            "an active restore lease must block candidate selection"
        );
        assert_eq!(
            revert_candidate_row(pool, leased, SandboxLifecycle::Restoring).await?,
            None,
            "an unexpired restore lease must block the revert"
        );
        assert!(
            !fail_candidate(pool, leased, SandboxLifecycle::Restoring, 60).await?,
            "an unexpired restore lease must block the fail CAS"
        );
        assert_eq!(lifecycle_of(pool, leased).await?, "restoring");

        // A one-second renewal gap is not enough to reclaim the row.
        sqlx::query(
            "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '1 second'
              WHERE id = $1",
        )
        .bind(leased)
        .execute(pool)
        .await?;
        assert_eq!(
            revert_candidate_row(pool, leased, SandboxLifecycle::Restoring).await?,
            None,
            "a recently expired lease remains inside the quiet window"
        );
        assert!(
            !fail_candidate(pool, leased, SandboxLifecycle::Restoring, 60).await?,
            "the fail CAS uses the same quiet window"
        );

        assert!(
            !list_candidates(pool, SandboxLifecycle::Restoring, 60)
                .await?
                .iter()
                .any(|candidate| candidate.id == leased),
            "the candidate scan must use the same quiet window as its CASes"
        );

        sqlx::query(
            "UPDATE sandboxes SET restore_lease_expires_at = NOW() - INTERVAL '61 seconds'
              WHERE id = $1",
        )
        .bind(leased)
        .execute(pool)
        .await?;
        assert!(
            list_candidates(pool, SandboxLifecycle::Restoring, 60)
                .await?
                .iter()
                .any(|candidate| candidate.id == leased),
            "the candidate scan releases a lease after the complete quiet window"
        );
        assert!(
            revert_candidate_row(pool, leased, SandboxLifecycle::Restoring)
                .await?
                .is_some(),
            "a lease quiet for the complete age window no longer owns the row"
        );
        assert_eq!(lifecycle_of(pool, leased).await?, "suspended");

        // The fail CAS has the same positive boundary as selection, converge,
        // and revert; a lease quiet for the complete window is reclaimable.
        let quiet_fail = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, quiet_fail, SandboxLifecycle::Restoring, 300).await?;
        sqlx::query(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() - INTERVAL '61 seconds',
                    restore_lease_token = $2
              WHERE id = $1",
        )
        .bind(quiet_fail)
        .bind(Uuid::now_v7())
        .execute(pool)
        .await?;
        assert!(
            fail_candidate(pool, quiet_fail, SandboxLifecycle::Restoring, 60).await?,
            "the fail CAS releases a lease after the complete quiet window"
        );
        assert_eq!(lifecycle_of(pool, quiet_fail).await?, "failed");
        Ok(())
    }

    /// R14: a failed restore attempt's placement bump must not hide a legacy
    /// (NULL artifact-identity) restoring row's source snapshot. The candidate
    /// select and the revert CAS both use the restore retry source window for
    /// `restoring`, so the row lists with `ready_snapshot = true` and reverts —
    /// under the old placement-freshness predicate it would have listed `false`
    /// and been fail-and-retired despite its source snapshot sitting in storage.
    #[tokio::test(flavor = "current_thread")]
    async fn r14_restoring_snapshot_fact_survives_failed_restore_placement_bump() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        let stuck = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, stuck, SandboxLifecycle::Restoring, 300).await?;
        let snapshot =
            insert_full_snapshot_relative_to_lifecycle(pool, stuck, "ready", node, -60).await?;
        insert_storage_location(pool, snapshot).await?;
        // The failed restore attempt placed a rebuilt pod after the snapshot
        // was taken.
        sqlx::query("UPDATE sandboxes SET placement_changed_at = NOW() WHERE id = $1")
            .bind(stuck)
            .execute(pool)
            .await?;

        let rows = list_candidates(pool, SandboxLifecycle::Restoring, 60).await?;
        let stuck_row = rows
            .iter()
            .find(|row| row.id == stuck)
            .expect("stuck restoring row listed");
        assert!(
            stuck_row.ready_snapshot,
            "the canonical snapshot must stay visible past the placement bump"
        );

        assert!(
            revert_candidate_row(pool, stuck, SandboxLifecycle::Restoring)
                .await?
                .is_some(),
            "the revert CAS must accept the canonical snapshot past the placement bump"
        );
        assert_eq!(lifecycle_of(pool, stuck).await?, "suspended");
        Ok(())
    }

    /// R15: if a failed restore branch publishes a later artifact, the row is
    /// still recoverable by the direct restore retry path, but it is not safe to
    /// flip to `suspended`: the suspended selector would trust the current late
    /// artifact identity. The revert CAS must leave the row `restoring`.
    #[tokio::test(flavor = "current_thread")]
    async fn r15_revert_cas_refuses_late_branch_restore_artifact() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let node = NodeId::new();

        let stuck = SandboxId::new();
        let source_artifact = Uuid::now_v7();
        let late_artifact = Uuid::now_v7();
        insert_sandbox_with_lifecycle(pool, stuck, SandboxLifecycle::Restoring, 300).await?;
        pin_local_artifact(pool, stuck, late_artifact).await?;
        insert_pinned_ready_snapshot_relative_to_lifecycle(pool, stuck, source_artifact, node, -60)
            .await?;
        insert_pinned_ready_snapshot_relative_to_lifecycle(pool, stuck, late_artifact, node, 60)
            .await?;

        let rows = list_candidates(pool, SandboxLifecycle::Restoring, 60).await?;
        let stuck_row = rows
            .iter()
            .find(|row| row.id == stuck)
            .expect("stuck restoring row listed");
        assert!(
            stuck_row.ready_snapshot,
            "the direct restore retry source is still visible"
        );

        assert_eq!(
            revert_candidate_row(pool, stuck, SandboxLifecycle::Restoring).await?,
            None,
            "a row whose current artifact points at the late branch must not revert to suspended"
        );
        assert_eq!(lifecycle_of(pool, stuck).await?, "restoring");
        Ok(())
    }

    // -------------------------------------------------------------
    // PU — Unrestorable-restoring sweep (production queries)
    // -------------------------------------------------------------

    /// Insert a `restoring` row shaped like the wedged population: unplaced,
    /// **fresh** `lifecycle_changed_at` (a doomed wake's lease claim just
    /// reset it — the exact thing that hides these rows from the aged sweep),
    /// and an optionally-active restore lease.
    async fn insert_restoring_row(
        pool: &PgPool,
        sandbox_id: SandboxId,
        placement_offset_secs: i64,
        lease_active: bool,
        evacuation_restore_target: Option<&str>,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, network_policy_generation,
                 lifecycle_changed_at, placement_changed_at,
                 restore_lease_expires_at, restore_lease_token, evacuation_restore_target)
             VALUES ($1,
                     '00000000-0000-0000-0000-000000000010',
                     '00000000-0000-0000-0000-000000000001',
                     'space',
                     'restoring',
                     1,
                     NOW() - make_interval(secs => 5),
                     NOW() - make_interval(secs => $2::BIGINT),
                     CASE WHEN $3 THEN NOW() + INTERVAL '120 seconds' ELSE NULL END,
                     CASE WHEN $3 THEN gen_random_uuid() ELSE NULL END,
                     $4)",
        )
        .bind(sandbox_id)
        .bind(placement_offset_secs)
        .bind(lease_active)
        .bind(evacuation_restore_target)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn list_unrestorable(pool: &PgPool, age_secs: i64) -> TestResult<BTreeSet<SandboxId>> {
        let rows: Vec<(SandboxId, i64)> =
            sqlx::query_as(sqlx::AssertSqlSafe(list_unrestorable_restoring_query()))
                .bind("space")
                .bind(age_secs)
                .bind(1000_i64)
                .fetch_all(pool)
                .await?;
        Ok(rows.into_iter().map(|(id, _)| id).collect())
    }

    async fn fail_unrestorable(
        pool: &PgPool,
        sandbox_id: SandboxId,
        age_secs: i64,
    ) -> TestResult<bool> {
        let claimed: Option<Uuid> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(fail_unrestorable_restoring_query()))
                .bind("space")
                .bind(age_secs)
                .bind(sandbox_id)
                .fetch_optional(pool)
                .await?;
        Ok(claimed.is_some())
    }

    /// An unplaced `restoring` row with no execution history, no
    /// snapshots, an actively-renewed restore lease, and a
    /// `lifecycle_changed_at` reset seconds ago by the last doomed wake. The
    /// aged sweep cannot touch it (fresh clock + lease guard); the
    /// unrestorable sweep both lists it and claims it.
    #[tokio::test(flavor = "current_thread")]
    async fn pu1_wedged_row_with_active_lease_is_swept_despite_fresh_lifecycle_clock() -> TestResult
    {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let wedged = SandboxId::new();
        insert_restoring_row(pool, wedged, 600, true, None).await?;

        // The aged-transient sweep is blind to it: the doomed wake just reset
        // lifecycle_changed_at, so the row never ages past the threshold.
        assert!(
            !listed_ids(pool, SandboxLifecycle::Restoring, 60)
                .await?
                .contains(&wedged),
            "aged sweep must not see the freshly-reclaimed row (documents the evasion)"
        );

        assert!(list_unrestorable(pool, 60).await?.contains(&wedged));
        assert!(fail_unrestorable(pool, wedged, 60).await?);
        assert_eq!(lifecycle_of(pool, wedged).await?, "failed");
        Ok(())
    }

    /// A ready snapshot inside the restore-retry source window is a real
    /// restore source; the row must be invisible to both the select and the
    /// fail CAS.
    #[tokio::test(flavor = "current_thread")]
    async fn pu2_ready_snapshot_in_retry_window_shields() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let shielded = SandboxId::new();
        insert_restoring_row(pool, shielded, 600, true, None).await?;
        // timestamp = lifecycle_changed_at - 60s: newer than the placement
        // change (NOW-600) and older than the restore start — in-window.
        let snapshot_id =
            insert_full_snapshot_relative_to_lifecycle(pool, shielded, "ready", NodeId::new(), -60)
                .await?;
        insert_storage_location(pool, snapshot_id).await?;

        assert!(!list_unrestorable(pool, 60).await?.contains(&shielded));
        assert!(!fail_unrestorable(pool, shielded, 60).await?);
        assert_eq!(lifecycle_of(pool, shielded).await?, "restoring");
        Ok(())
    }

    /// A `creating` snapshot newer than the placement change is an in-flight
    /// suspend upload that may finalize `ready` moments later — the
    /// mark-lost race guard. The row must be left alone.
    #[tokio::test(flavor = "current_thread")]
    async fn pu3_inflight_suspend_upload_shields() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let shielded = SandboxId::new();
        insert_restoring_row(pool, shielded, 600, true, None).await?;
        // No storage location on purpose: the inflight predicate doesn't
        // require one (the upload hasn't finished).
        insert_full_snapshot_relative_to_lifecycle(pool, shielded, "creating", NodeId::new(), -60)
            .await?;

        assert!(!list_unrestorable(pool, 60).await?.contains(&shielded));
        assert!(!fail_unrestorable(pool, shielded, 60).await?);
        assert_eq!(lifecycle_of(pool, shielded).await?, "restoring");
        Ok(())
    }

    /// Any execution-history row naming a node means the restore planner's
    /// source-node fallback would find a node to pin a restore pod to — the
    /// sandbox is not provably unrestorable. Active or retired doesn't
    /// matter; the planner's fallback scans all generations.
    #[tokio::test(flavor = "current_thread")]
    async fn pu4_execution_history_naming_a_node_shields() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let shielded = SandboxId::new();
        insert_restoring_row(pool, shielded, 600, true, None).await?;
        insert_execution(pool, shielded, false, 600, Some("node-a")).await?;

        assert!(!list_unrestorable(pool, 60).await?.contains(&shielded));
        assert!(!fail_unrestorable(pool, shielded, 60).await?);
        Ok(())
    }

    /// An active execution (even one whose pod hasn't reached a node yet) is
    /// a restore attempt in flight that owns the row.
    #[tokio::test(flavor = "current_thread")]
    async fn pu5_active_execution_shields() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let shielded = SandboxId::new();
        insert_restoring_row(pool, shielded, 600, true, None).await?;
        insert_execution(pool, shielded, true, 5, None).await?;

        assert!(!list_unrestorable(pool, 60).await?.contains(&shielded));
        assert!(!fail_unrestorable(pool, shielded, 60).await?);
        Ok(())
    }

    /// The placement-age floor: a row whose placement changed seconds ago is
    /// inside the suspend→backup-enqueue window and must wait it out.
    #[tokio::test(flavor = "current_thread")]
    async fn pu6_fresh_placement_shields() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let fresh = SandboxId::new();
        insert_restoring_row(pool, fresh, 10, true, None).await?;

        assert!(!list_unrestorable(pool, 60).await?.contains(&fresh));
        assert!(!fail_unrestorable(pool, fresh, 60).await?);
        Ok(())
    }

    /// Evacuation-marked rows belong to node-drain's re-drive path.
    #[tokio::test(flavor = "current_thread")]
    async fn pu7_evacuation_marker_shields() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let marked = SandboxId::new();
        insert_restoring_row(pool, marked, 600, true, Some("started")).await?;

        assert!(!list_unrestorable(pool, 60).await?.contains(&marked));
        assert!(!fail_unrestorable(pool, marked, 60).await?);
        Ok(())
    }

    /// The upload race, end to end: the row is listed, then a snapshot
    /// finalizes `ready` between select and act. The fail CAS re-checks the
    /// full predicate atomically and must claim nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn pu8_source_appearing_between_select_and_act_defeats_fail_cas() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let raced = SandboxId::new();
        insert_restoring_row(pool, raced, 600, true, None).await?;
        assert!(list_unrestorable(pool, 60).await?.contains(&raced));

        let snapshot_id =
            insert_full_snapshot_relative_to_lifecycle(pool, raced, "ready", NodeId::new(), -60)
                .await?;
        insert_storage_location(pool, snapshot_id).await?;

        assert!(!fail_unrestorable(pool, raced, 60).await?);
        assert_eq!(lifecycle_of(pool, raced).await?, "restoring");
        Ok(())
    }

    /// Cross-check the division of labor with the aged sweep: even when the
    /// wedged row *has* aged past the aged sweep's threshold, its
    /// near-continuously renewed restore lease makes the aged fail CAS claim
    /// nothing — the unrestorable CAS (deliberately lease-blind, because the
    /// source-absence predicate is the safety) is what breaks the loop.
    #[tokio::test(flavor = "current_thread")]
    async fn pu9_aged_fail_cas_defers_on_lease_where_unrestorable_cas_claims() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let wedged = SandboxId::new();
        insert_restoring_row(pool, wedged, 600, true, None).await?;
        // Backdate the lifecycle clock past the aged threshold, keeping the
        // active lease — the shape between two doomed wakes.
        sqlx::query(
            "UPDATE sandboxes
                SET lifecycle_changed_at = NOW() - INTERVAL '600 seconds'
              WHERE id = $1",
        )
        .bind(wedged)
        .execute(pool)
        .await?;

        assert!(
            !fail_candidate(pool, wedged, SandboxLifecycle::Restoring, 60).await?,
            "aged fail CAS must defer to the active restore lease"
        );
        assert!(fail_unrestorable(pool, wedged, 60).await?);
        assert_eq!(lifecycle_of(pool, wedged).await?, "failed");
        Ok(())
    }

    /// The stranded shape the retired kind smoke (`smoke-kind-stranded-sweep.sh`,
    /// 2026-09-24) manufactured, against real Postgres: a `stopping` row past
    /// the threshold whose active execution has no node to probe
    /// (`k8s_node_name IS NULL`, which `probe_targets` classifies
    /// `missing_on_node` without a probe). The select lists it with that NULL
    /// and with the execution's recorded pod uid; the fence is built from that
    /// uid and refuses to adopt a disagreeing observation (the identity the
    /// refuse arm withholds on — the withholding itself is the substrate's,
    /// and F2 shows the reconciler then runs no CAS); and the fail CAS moves
    /// the permitted row to `failed`. A fresh `stopping` row is never a
    /// candidate, and a refused row is still one on the next tick.
    #[tokio::test(flavor = "current_thread")]
    async fn r20_stranded_stopping_row_without_a_node_is_selected_fenced_and_failed() -> TestResult
    {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;

        let permit = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, permit, SandboxLifecycle::Stopping, 300).await?;
        insert_execution_with_pod_uid(pool, permit, true, 300, None, Some("uid-permit")).await?;
        let refuse = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, refuse, SandboxLifecycle::Stopping, 300).await?;
        insert_execution_with_pod_uid(pool, refuse, true, 300, None, Some("uid-recorded")).await?;
        let fresh = SandboxId::new();
        insert_sandbox_with_lifecycle(pool, fresh, SandboxLifecycle::Stopping, 10).await?;
        insert_execution_with_pod_uid(pool, fresh, true, 10, None, Some("uid-fresh")).await?;

        let rows = list_candidates(pool, SandboxLifecycle::Stopping, 60).await?;
        assert_eq!(
            rows.iter()
                .map(|row| row.id)
                .collect::<BTreeSet<SandboxId>>(),
            [permit, refuse]
                .into_iter()
                .collect::<BTreeSet<SandboxId>>(),
            "only the aged `stopping` rows are candidates"
        );
        for row in &rows {
            assert!(
                row.k8s_node_name.is_none(),
                "the stranded execution has no node to probe; the select must say so"
            );
            assert!(
                row.pod_uid.is_some(),
                "the recorded pod uid is what the retire fence is built from"
            );
            assert!(
                row.age_secs >= 300,
                "age reflects the backdated lifecycle clock"
            );
        }

        // The refuse arm's identity: the fence carries the row's recorded uid
        // when the reflector agrees, and adopts nothing when it does not — a
        // same-name successor is exactly what a disagreeing observation is.
        let row = rows
            .iter()
            .find(|row| row.id == refuse)
            .expect("the refuse row is listed");
        let pod_name = PodName::parse(format!("sandbox-{refuse}")).expect("pod name");
        let fence_with = |observed: &HashMap<PodName, ObservedPodIdentity>| {
            candidate_pod_fence_with_observation(
                row.id,
                row.pod_name.clone(),
                row.activation_attempt_id,
                row.generation,
                row.pod_uid.clone(),
                row.route_activation_id,
                observed,
            )
        };
        let agreeing = HashMap::from([(
            pod_name.clone(),
            ObservedPodIdentity {
                pod_uid: PodUid::parse("uid-recorded").expect("pod uid"),
                resource_version: PodResourceVersion::parse("7").ok(),
            },
        )]);
        let RetireFence::Pod(fence) = fence_with(&agreeing) else {
            panic!("an observed active execution must yield a pod fence");
        };
        assert_eq!(
            fence.pod_uid.as_ref().map(PodUid::as_str),
            Some("uid-recorded")
        );
        let successor = HashMap::from([(
            pod_name,
            ObservedPodIdentity {
                pod_uid: PodUid::parse("uid-successor").expect("pod uid"),
                resource_version: None,
            },
        )]);
        let RetireFence::Pod(fence) = fence_with(&successor) else {
            panic!("a disagreeing observation must still fence on the name");
        };
        assert_eq!(
            fence.pod_uid, None,
            "a pod the row does not claim must not become the fence"
        );

        // The permit arm: the production CAS flips the stranded row to `failed`.
        assert!(fail_candidate(pool, permit, SandboxLifecycle::Stopping, 60).await?);
        assert_eq!(lifecycle_of(pool, permit).await?, "failed");

        // The refuse arm commits nothing (F2), so the row is untouched and is a
        // candidate again on the next tick rather than dropping out.
        assert_eq!(lifecycle_of(pool, refuse).await?, "stopping");
        assert!(
            listed_ids(pool, SandboxLifecycle::Stopping, 60)
                .await?
                .contains(&refuse),
            "a withheld retire leaves the row for the next tick"
        );
        assert_eq!(lifecycle_of(pool, fresh).await?, "stopping");
        Ok(())
    }

    /// The failed-cleanup statements bind the Kubernetes substrate: a direct
    /// row with the same failed-with-active-execution shape is not listed,
    /// not rotated, and not repaired.
    #[tokio::test(flavor = "current_thread")]
    async fn r21_failed_cleanup_statements_skip_direct_rows() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let pool = &db.pool;
        let k8s = SandboxId::new();
        let direct = SandboxId::new();
        for (sandbox_id, managed_by) in [(k8s, "k8s"), (direct, "space")] {
            insert_sandbox_with_lifecycle_and_managed_by(
                pool,
                sandbox_id,
                SandboxLifecycle::Failed,
                300,
                managed_by,
            )
            .await?;
            sqlx::query("UPDATE sandboxes SET placement = 'on_node' WHERE id = $1")
                .bind(sandbox_id)
                .execute(pool)
                .await?;
            insert_execution(pool, sandbox_id, true, 300, Some("node-1")).await?;
        }
        let kubernetes = SubstrateKind::Kubernetes;

        let listed: Vec<(SandboxId,)> = sqlx::query_as(LIST_FAILED_ACTIVE_EXECUTIONS_SQL)
            .bind(kubernetes)
            .bind(10_i64)
            .fetch_all(pool)
            .await?;
        assert_eq!(listed, vec![(k8s,)]);

        let rotated = |sandbox_id: SandboxId| async move {
            sqlx::query(NOTE_FAILED_CLEANUP_ATTEMPT_SQL)
                .bind(sandbox_id)
                .bind(kubernetes)
                .execute(pool)
                .await
                .map(|result| result.rows_affected())
        };
        assert_eq!(rotated(direct).await?, 0, "a direct row is not rotated");
        assert_eq!(rotated(k8s).await?, 1);

        let repaired = |sandbox_id: SandboxId| async move {
            sqlx::query_scalar::<_, SandboxId>(REPAIR_FAILED_ACTIVE_EXECUTION_SQL)
                .bind(sandbox_id)
                .bind(kubernetes)
                .fetch_optional(pool)
                .await
        };
        assert_eq!(
            repaired(direct).await?,
            None,
            "a direct row is not repaired"
        );
        assert_eq!(repaired(k8s).await?, Some(k8s));
        Ok(())
    }

    /// Those statements only run when the controller's substrate is
    /// Kubernetes; on any other label the sink skips them.
    #[test]
    fn r22_failed_cleanup_runs_only_on_the_kubernetes_substrate() {
        assert_eq!(
            failed_active_execution_scope("k8s"),
            Some(SubstrateKind::Kubernetes)
        );
        assert_eq!(failed_active_execution_scope("space"), None);
        assert_eq!(failed_active_execution_scope("unrecognized"), None);
    }
}
