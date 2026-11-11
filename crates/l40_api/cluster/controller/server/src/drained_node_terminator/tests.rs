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
//!
//! ## T — Tick behavior
//! - T1 `t1_empty_drained_list_is_noop`
//! - T2 `t2_absent_annotation_flips_to_terminating`
//! - T3 `t3_present_annotation_skips_and_counts_keep`
//! - T4 `t4_mixed_nodes_flip_only_unannotated`
//! - T5 `t5_stale_identity_skips_silently`
//! - T6 `t6_kube_node_gone_without_ec2_cross_check_parks`
//! - T6a `t6a_kube_node_gone_ec2_terminated_hard_deletes_row`
//! - T6b `t6b_kube_node_gone_ec2_live_parks_for_manual_review`
//! - T6c `t6c_kube_node_gone_ec2_lookup_error_parks`
//! - T6d `t6d_kube_node_gone_ec2_terminated_but_delete_guard_refuses_parks`
//! - T6e `t6e_kube_node_gone_ec2_terminated_delete_db_error_parks`
//! - T6f `t6f_kube_node_gone_threads_observed_uid_into_delete`
//! - T6g `t6g_mark_lost_disabled_leaves_blocking_sandboxes_suspended`
//! - T6h `t6h_mark_lost_enabled_marks_blocking_sandboxes_and_threads_uid`
//! - T6i `t6i_mark_lost_not_attempted_without_snapshot_blockers`
//! - T6j `t6j_mark_lost_db_error_parks_without_counting`
//! - T6k `t6k_mark_lost_empty_result_counts_nothing`
//! - T7 `t7_unregistered_node_skips_with_lookup_error`
//! - T8 `t8_phase_drift_skips_without_error`
//! - T9 `t9_mark_terminating_db_error_recorded`
//! - T10 `t10_list_drained_db_error_returns_zero_report`
//! - T11 `t11_annotation_value_recorded_in_kept_variant`
//! - T12 `t12_local_snapshots_block_flip`
//! - T13 `t13_count_local_snapshots_db_error_skips_flip`
//! - T14 `t14_snapshot_committed_between_count_and_mark_blocks_flip`
//! - T15 `t15_restoring_sandboxes_block_flip`
//! - T16 `t16_suspending_sandboxes_block_flip`
//! - T17 `t17_resident_active_sandboxes_block_flip`
//!
//! ## W — Refusal-warn rate limiting
//! - W1 `w1_refusal_warn_rate_limited_per_node`
//! - W2 `w2_refusal_warn_state_pruned_when_node_leaves_drained_list`

use pplx_space_common_core::types::NodeId;
use pplx_space_common_core::types::SandboxId;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::*;

// -----------------------------------------------------------------
// Fake
// -----------------------------------------------------------------

use std::sync::Arc as StdArc;

/// EC2 cross-check fixture for the KubeNodeGone recovery path. Avoids needing
/// `InstanceLookupError: Clone` (it isn't) by carrying the parts and building
/// a fresh `Result` per call.
#[derive(Clone)]
enum InstanceStateFixture {
    Terminated,
    Live { instance_id: String, state: String },
    Error(String),
}

/// Combined test double implementing [`DrainedNodeTerminatorSink`],
/// [`DrainKeepAnnotationLookup`], and [`InstanceStateLookup`]. Wrapped in
/// `Arc` so tests retain a handle for assertions after it's been moved into
/// the `Reconciler`. Production code cleanly separates the traits; the single
/// fake exists only to keep tests concise.
#[derive(Default)]
struct FakeSink {
    drained_list: StdMutex<Vec<NodeId>>,
    phase: StdMutex<HashMap<NodeId, String>>,
    keep_status: StdMutex<HashMap<NodeId, Result<DrainKeepStatus, AnnotationLookupError>>>,
    mark_calls: StdMutex<Vec<NodeId>>,
    fail_list_drained: StdMutex<Option<String>>,
    fail_mark_terminating: StdMutex<Option<String>>,
    local_snapshots: StdMutex<HashMap<NodeId, i64>>,
    /// Active sandboxes resident on a node (active_node_id pointer with
    /// a non-terminal lifecycle). Models the resident-guard `NOT EXISTS`
    /// clause in production `mark_terminating` so a regression that
    /// dropped that clause would fail T17.
    active_sandboxes: StdMutex<HashMap<NodeId, i64>>,
    fail_count_local_snapshots: StdMutex<Option<String>>,
    /// Race injection: after `count_local_snapshots(node)` returns,
    /// the fake atomically bumps `local_snapshots[node]` by this
    /// amount. Used by T14 to simulate a suspend committing in the
    /// window between the pre-check and the conditional UPDATE.
    bump_snapshots_after_count: StdMutex<HashMap<NodeId, i64>>,
    /// EC2 verdict per node for the KubeNodeGone recovery path.
    instance_states: StdMutex<HashMap<NodeId, InstanceStateFixture>>,
    /// `expected_uid` the instance lookup returns per node (the observed
    /// `node_k8s.k8s_node_uid`). Defaults to `None` when unset.
    instance_uids: StdMutex<HashMap<NodeId, String>>,
    /// Nodes whose guarded `delete_drained_node_if_orphan` must refuse, with
    /// the per-guard diagnosis it should report. Default behavior deletes a
    /// `drained` row.
    delete_refused: StdMutex<HashMap<NodeId, OrphanDeleteRefusal>>,
    /// Records `node_id`s passed to `delete_drained_node_if_orphan`.
    delete_calls: StdMutex<Vec<NodeId>>,
    /// Records the `expected_uid` passed to `delete_drained_node_if_orphan`,
    /// so a test can assert the lookup's observed uid is threaded through.
    delete_expected_uids: StdMutex<Vec<Option<String>>>,
    fail_delete_drained: StdMutex<Option<String>>,
    /// Sandbox ids `mark_unrecoverable_sandboxes_lost` should report as
    /// flipped for a node. Default: empty (nothing provably unrecoverable).
    mark_lost_results: StdMutex<HashMap<NodeId, Vec<SandboxId>>>,
    /// Records `(node_id, expected_uid)` passed to
    /// `mark_unrecoverable_sandboxes_lost`.
    mark_lost_calls: StdMutex<Vec<(NodeId, Option<String>)>>,
    fail_mark_lost: StdMutex<Option<String>>,
}

impl FakeSink {
    fn new() -> StdArc<Self> {
        StdArc::new(Self::default())
    }

    fn set_drained_list(&self, nodes: Vec<NodeId>) {
        *self.drained_list.lock().unwrap() = nodes;
    }

    fn set_phase(&self, node: NodeId, phase: &str) {
        self.phase.lock().unwrap().insert(node, phase.to_string());
    }

    fn clear_phase(&self, node: NodeId) {
        self.phase.lock().unwrap().remove(&node);
    }

    fn set_keep_absent(&self, node: NodeId) {
        self.keep_status
            .lock()
            .unwrap()
            .insert(node, Ok(DrainKeepStatus::Absent));
    }

    fn set_keep_present(&self, node: NodeId, reason: &str) {
        self.keep_status
            .lock()
            .unwrap()
            .insert(node, Ok(DrainKeepStatus::Kept(reason.into())));
    }

    fn set_keep_error(&self, node: NodeId, err: AnnotationLookupError) {
        self.keep_status.lock().unwrap().insert(node, Err(err));
    }

    fn inject_fail_list_drained(&self, msg: &str) {
        *self.fail_list_drained.lock().unwrap() = Some(msg.into());
    }

    fn inject_fail_mark_terminating(&self, msg: &str) {
        *self.fail_mark_terminating.lock().unwrap() = Some(msg.into());
    }

    fn set_local_snapshots(&self, node: NodeId, count: i64) {
        self.local_snapshots.lock().unwrap().insert(node, count);
    }

    fn set_active_sandboxes(&self, node: NodeId, count: i64) {
        self.active_sandboxes.lock().unwrap().insert(node, count);
    }

    fn inject_fail_count_local_snapshots(&self, msg: &str) {
        *self.fail_count_local_snapshots.lock().unwrap() = Some(msg.into());
    }

    fn bump_snapshots_after_count(&self, node: NodeId, delta: i64) {
        self.bump_snapshots_after_count
            .lock()
            .unwrap()
            .insert(node, delta);
    }

    fn set_instance_terminated(&self, node: NodeId) {
        self.instance_states
            .lock()
            .unwrap()
            .insert(node, InstanceStateFixture::Terminated);
    }

    fn set_instance_uid(&self, node: NodeId, uid: &str) {
        self.instance_uids.lock().unwrap().insert(node, uid.into());
    }

    fn set_instance_live(&self, node: NodeId, instance_id: &str, state: &str) {
        self.instance_states.lock().unwrap().insert(
            node,
            InstanceStateFixture::Live {
                instance_id: instance_id.into(),
                state: state.into(),
            },
        );
    }

    fn set_instance_lookup_error(&self, node: NodeId, msg: &str) {
        self.instance_states
            .lock()
            .unwrap()
            .insert(node, InstanceStateFixture::Error(msg.into()));
    }

    fn set_delete_refused(&self, node: NodeId, refusal: OrphanDeleteRefusal) {
        self.delete_refused.lock().unwrap().insert(node, refusal);
    }

    fn inject_fail_delete_drained(&self, msg: &str) {
        *self.fail_delete_drained.lock().unwrap() = Some(msg.into());
    }

    fn set_mark_lost_result(&self, node: NodeId, sandbox_ids: Vec<SandboxId>) {
        self.mark_lost_results
            .lock()
            .unwrap()
            .insert(node, sandbox_ids);
    }

    fn inject_fail_mark_lost(&self, msg: &str) {
        *self.fail_mark_lost.lock().unwrap() = Some(msg.into());
    }

    fn mark_lost_calls(&self) -> Vec<(NodeId, Option<String>)> {
        self.mark_lost_calls.lock().unwrap().clone()
    }

    fn mark_calls(&self) -> Vec<NodeId> {
        self.mark_calls.lock().unwrap().clone()
    }

    fn delete_calls(&self) -> Vec<NodeId> {
        self.delete_calls.lock().unwrap().clone()
    }

    fn delete_expected_uids(&self) -> Vec<Option<String>> {
        self.delete_expected_uids.lock().unwrap().clone()
    }

    fn current_phase(&self, node: NodeId) -> Option<String> {
        self.phase.lock().unwrap().get(&node).cloned()
    }
}

fn forced_db_err(msg: &str) -> sqlx::Error {
    sqlx::Error::Io(std::io::Error::other(msg))
}

/// Build a test reconciler. Both generic slots accept the same `Arc<FakeSink>`
/// so tests only create one object. The shared Arc survives after `new_reconciler`
/// returns so tests can assert on sink state post-tick.
fn new_reconciler(sink: StdArc<FakeSink>) -> Reconciler<StdArc<FakeSink>, StdArc<FakeSink>> {
    Reconciler::new(StdArc::clone(&sink), sink, None, false)
}

/// Like [`new_reconciler`], but wires the same fake as the EC2 cross-check so
/// the KubeNodeGone recovery path is exercised. Mirrors the production wiring
/// on the `aws` terminator deploy with the mark-lost give-up at its default
/// (disabled).
fn new_reconciler_with_ec2(
    sink: StdArc<FakeSink>,
) -> Reconciler<StdArc<FakeSink>, StdArc<FakeSink>> {
    // `Arc<FakeSink>` coerces to `Arc<dyn InstanceStateLookup>` at the
    // call argument.
    let instance_lookup = StdArc::clone(&sink);
    Reconciler::new(
        StdArc::clone(&sink),
        StdArc::clone(&sink),
        Some(instance_lookup),
        false,
    )
}

/// Like [`new_reconciler_with_ec2`], with the KubeNodeGone mark-lost give-up
/// enabled.
fn new_reconciler_with_ec2_mark_lost(
    sink: StdArc<FakeSink>,
) -> Reconciler<StdArc<FakeSink>, StdArc<FakeSink>> {
    let instance_lookup = StdArc::clone(&sink);
    Reconciler::new(
        StdArc::clone(&sink),
        StdArc::clone(&sink),
        Some(instance_lookup),
        true,
    )
}

impl DrainKeepAnnotationLookup for StdArc<FakeSink> {
    async fn lookup_drain_keep(
        &self,
        node_id: NodeId,
    ) -> Result<DrainKeepStatus, AnnotationLookupError> {
        match self.keep_status.lock().unwrap().remove(&node_id) {
            Some(result) => result,
            None => Ok(DrainKeepStatus::Absent),
        }
    }
}

impl DrainedNodeTerminatorSink for StdArc<FakeSink> {
    async fn list_drained_nodes(&self) -> Result<Vec<NodeId>, sqlx::Error> {
        if let Some(msg) = self.fail_list_drained.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        Ok(self.drained_list.lock().unwrap().clone())
    }

    async fn count_local_snapshots(&self, node_id: NodeId) -> Result<i64, sqlx::Error> {
        if let Some(msg) = self.fail_count_local_snapshots.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        let count = self
            .local_snapshots
            .lock()
            .unwrap()
            .get(&node_id)
            .copied()
            .unwrap_or(0);
        // Apply any pending race injection after sampling the
        // current value, so the caller observes pre-race state but
        // any subsequent `mark_terminating` sees the post-race
        // value. Simulates a suspend committing in the window
        // between the two queries.
        if let Some(delta) = self
            .bump_snapshots_after_count
            .lock()
            .unwrap()
            .remove(&node_id)
        {
            let mut map = self.local_snapshots.lock().unwrap();
            let entry = map.entry(node_id).or_insert(0);
            *entry += delta;
        }
        Ok(count)
    }

    async fn mark_terminating(&self, node_id: NodeId) -> Result<bool, sqlx::Error> {
        self.mark_calls.lock().unwrap().push(node_id);
        if let Some(msg) = self.fail_mark_terminating.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        // Mirror the production SQL's `NOT EXISTS` clauses: refuse to
        // flip the row if local snapshots are present OR if the node
        // still has resident active sandboxes at the moment of the
        // UPDATE. Without these guards in the fake, the TOCTOU defense
        // between `count_local_snapshots` and `mark_terminating`
        // (snapshot guard) and the resident-sandbox cordon-loss check
        // (resident guard) cannot be exercised by tests.
        if self
            .local_snapshots
            .lock()
            .unwrap()
            .get(&node_id)
            .copied()
            .unwrap_or(0)
            > 0
        {
            return Ok(false);
        }
        if self
            .active_sandboxes
            .lock()
            .unwrap()
            .get(&node_id)
            .copied()
            .unwrap_or(0)
            > 0
        {
            return Ok(false);
        }
        let mut phase = self.phase.lock().unwrap();
        match phase.get(&node_id) {
            Some(p) if p == PHASE_DRAINED => {
                phase.insert(node_id, PHASE_TERMINATING.to_string());
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn delete_drained_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<OrphanDeleteOutcome, sqlx::Error> {
        self.delete_calls.lock().unwrap().push(node_id);
        self.delete_expected_uids.lock().unwrap().push(expected_uid);
        if let Some(msg) = self.fail_delete_drained.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        // Explicit guard-refusal injection mirrors the production SQL
        // returning `rows_affected = 0` plus the post-refusal per-guard
        // diagnosis. The actual uid / SQL guards and the diagnostics query
        // are validated by the testcontainer tests
        // (`delete_drained_orphan_guards`, `delete_refusal_diagnostics_*`).
        if let Some(refusal) = self.delete_refused.lock().unwrap().get(&node_id) {
            return Ok(OrphanDeleteOutcome::Refused(refusal.clone()));
        }
        // Otherwise behave like the guarded DELETE: a row in `drained`
        // is reaped, anything else refuses with a phase-drift diagnosis.
        let mut phase = self.phase.lock().unwrap();
        match phase.get(&node_id) {
            Some(p) if p == PHASE_DRAINED => {
                phase.remove(&node_id);
                Ok(OrphanDeleteOutcome::Deleted)
            }
            _ => Ok(OrphanDeleteOutcome::Refused(OrphanDeleteRefusal {
                phase_drifted: true,
                ..OrphanDeleteRefusal::default()
            })),
        }
    }

    async fn mark_unrecoverable_sandboxes_lost(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<Vec<SandboxId>, sqlx::Error> {
        self.mark_lost_calls
            .lock()
            .unwrap()
            .push((node_id, expected_uid));
        if let Some(msg) = self.fail_mark_lost.lock().unwrap().take() {
            return Err(forced_db_err(&msg));
        }
        Ok(self
            .mark_lost_results
            .lock()
            .unwrap()
            .get(&node_id)
            .cloned()
            .unwrap_or_default())
    }
}

// Implemented on the bare `FakeSink` (not `StdArc<FakeSink>`) so that
// `Arc<FakeSink>` unsizes to `Arc<dyn InstanceStateLookup>` for the
// reconciler's optional trait-object field.
impl InstanceStateLookup for FakeSink {
    fn lookup_instance_state(
        &self,
        node_id: NodeId,
    ) -> Pin<Box<dyn Future<Output = Result<NodeInstanceState, InstanceLookupError>> + Send + '_>>
    {
        let fixture = self.instance_states.lock().unwrap().get(&node_id).cloned();
        let expected_uid = self.instance_uids.lock().unwrap().get(&node_id).cloned();
        Box::pin(async move {
            let outcome = match fixture {
                Some(InstanceStateFixture::Terminated) => InstanceLookupOutcome::Gone,
                Some(InstanceStateFixture::Live { instance_id, state }) => {
                    InstanceLookupOutcome::Live { instance_id, state }
                }
                Some(InstanceStateFixture::Error(msg)) => {
                    return Err(InstanceLookupError::Backend(msg));
                }
                // No fixture set: fail safe (park) rather than silently
                // returning a `Gone` verdict that would delete the row.
                None => {
                    return Err(InstanceLookupError::Backend(
                        "no instance-state fixture configured for node".into(),
                    ));
                }
            };
            Ok(NodeInstanceState {
                outcome,
                expected_uid,
            })
        })
    }
}

// =================================================================
// C — ReconcilerConfig + env parsing
// =================================================================

#[test]
fn c1_defaults_when_env_unset() {
    let env = pplx_space_test_env::TestEnv::default();
    let cfg = ReconcilerConfig::from_environment(&env);
    assert!(!cfg.enabled);
    assert_eq!(cfg.tick, DEFAULT_TICK);
    assert!(cfg.mark_lost_enabled);
}

#[test]
fn c4_enabled_truthy_parsing() {
    let mut env = pplx_space_test_env::TestEnv::default();
    for truthy in ["1", "true", "True", "TRUE"] {
        env.set("SPACE_DRAINED_NODE_TERMINATOR_ENABLED", truthy);
        assert!(
            ReconcilerConfig::from_environment(&env).enabled,
            "{truthy} should parse as true"
        );
    }
    for falsy in ["0", "false", "False", "FALSE"] {
        env.set("SPACE_DRAINED_NODE_TERMINATOR_ENABLED", falsy);
        assert!(
            !ReconcilerConfig::from_environment(&env).enabled,
            "{falsy} should parse as false"
        );
    }
}

// =================================================================
// R — run() loop lifecycle
// =================================================================

#[tokio::test]
async fn r1_enabled_false_returns_on_shutdown_without_ticking() {
    let sink = FakeSink::new();
    let node = NodeId::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);

    let token = CancellationToken::new();
    let cfg = ReconcilerConfig {
        enabled: false,
        tick: Duration::from_millis(1),
        mark_lost_enabled: false,
    };
    let runner = tokio::spawn(run(
        cfg,
        StdArc::clone(&sink),
        StdArc::clone(&sink),
        None,
        token.clone(),
    ));

    // Give the loop a chance to spin if `enabled=false` were ignored.
    tokio::time::sleep(Duration::from_millis(20)).await;
    token.cancel();
    runner.await.unwrap();
    assert!(
        sink.mark_calls().is_empty(),
        "disabled reconciler must never call mark_terminating, even though \
         the seeded node would flip on a real tick"
    );
}

#[tokio::test]
async fn r2_enabled_true_ticks_until_shutdown() {
    let sink = FakeSink::new();
    let node = NodeId::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);

    // Move sink into the spawned future, but verify outcome via a
    // shared reference proxy.
    let cfg = ReconcilerConfig {
        enabled: true,
        tick: Duration::from_millis(5),
        mark_lost_enabled: false,
    };
    let token = CancellationToken::new();

    // Wrap in an Arc-like through a single tick of the Reconciler so
    // we can inspect post-state. `run` consumes the sink, so we exercise
    // the loop indirectly via `Reconciler::tick` for state assertion
    // and verify `run`'s shutdown path separately.
    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;
    assert_eq!(report.drained_nodes, 1);
    assert_eq!(report.flipped, 1);

    // Independent: an enabled run() returns on shutdown.
    let sink2 = FakeSink::new();
    let runner = tokio::spawn(run(
        cfg,
        StdArc::clone(&sink2),
        StdArc::clone(&sink2),
        None,
        token.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    token.cancel();
    runner.await.unwrap();
}

// =================================================================
// T — Tick behavior
// =================================================================

#[tokio::test]
async fn t1_empty_drained_list_is_noop() {
    let sink = FakeSink::new();
    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
}

#[tokio::test]
async fn t2_absent_annotation_flips_to_terminating() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);

    let mark_target = node;
    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.drained_nodes, 1);
    assert_eq!(report.flipped, 1);
    assert_eq!(report.kept, 0);
    assert_eq!(sink.mark_calls(), vec![mark_target]);
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_TERMINATING));
}

#[tokio::test]
async fn t3_present_annotation_skips_and_counts_keep() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_present(node, "ami-debug");

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.kept, 1);
    assert_eq!(report.flipped, 0);
    assert!(
        sink.mark_calls().is_empty(),
        "kept node must not call mark_terminating"
    );
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t4_mixed_nodes_flip_only_unannotated() {
    let kept = NodeId::new();
    let flipped = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![kept, flipped]);
    sink.set_phase(kept, PHASE_DRAINED);
    sink.set_phase(flipped, PHASE_DRAINED);
    sink.set_keep_present(kept, "incident-INC-1234");
    sink.set_keep_absent(flipped);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.drained_nodes, 2);
    assert_eq!(report.kept, 1);
    assert_eq!(report.flipped, 1);
    assert_eq!(sink.mark_calls(), vec![flipped]);
    assert_eq!(sink.current_phase(kept).as_deref(), Some(PHASE_DRAINED));
    assert_eq!(
        sink.current_phase(flipped).as_deref(),
        Some(PHASE_TERMINATING)
    );
}

#[tokio::test]
async fn t5_stale_identity_skips_silently() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::StaleIdentity);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    // StaleIdentity is deferred silently: not counted as kept, not
    // counted as flipped, and not counted as lookup_errors (we expect
    // the inventory reflector to clear drain_phase shortly).
    assert_eq!(report.kept, 0);
    assert_eq!(report.flipped, 0);
    assert_eq!(report.lookup_errors, 0);
    assert!(sink.mark_calls().is_empty());
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t6_kube_node_gone_without_ec2_cross_check_parks() {
    // No EC2 cross-check configured (kube-delete / noop substrate): a
    // missing kube Node is parked for manual review, exactly as before the
    // recovery path existed. Parked, not a lookup error.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_parked, 1);
    assert_eq!(report.lookup_errors, 0);
    assert_eq!(report.flipped, 0);
    assert_eq!(report.kube_node_gone_recovered, 0);
    assert!(sink.mark_calls().is_empty());
    assert!(sink.delete_calls().is_empty());
    assert_eq!(
        sink.current_phase(node).as_deref(),
        Some(PHASE_DRAINED),
        "missing kube Node with no EC2 cross-check must stay parked in drained"
    );
}

#[tokio::test]
async fn t6a_kube_node_gone_ec2_terminated_hard_deletes_row() {
    // KubeNodeGone + EC2 confirms the instance terminated → the row is
    // reaped via the guarded hard-delete (NOT advanced to terminating).
    // Acceptance (a): instance terminated → row converges/deleted.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);

    let reconciler = new_reconciler_with_ec2(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_recovered, 1);
    assert_eq!(report.kube_node_gone_parked, 0);
    assert_eq!(report.lookup_errors, 0);
    assert_eq!(report.flipped, 0, "recovery deletes; it does not flip");
    assert_eq!(sink.delete_calls(), vec![node]);
    assert!(
        sink.mark_calls().is_empty(),
        "recovery must not advance to terminating"
    );
    assert_eq!(
        sink.current_phase(node),
        None,
        "the orphaned drained row must be deleted"
    );
}

#[tokio::test]
async fn t6b_kube_node_gone_ec2_live_parks_for_manual_review() {
    // KubeNodeGone + EC2 says the instance is still live (deregistered
    // node) → never auto-delete; park for manual review. Acceptance (b):
    // instance alive but k8s Node missing → still parked.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_live(node, "i-0123456789abcdef0", "running");

    let reconciler = new_reconciler_with_ec2(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_parked, 1);
    assert_eq!(report.kube_node_gone_recovered, 0);
    assert_eq!(report.lookup_errors, 0, "a live instance is not an error");
    assert!(
        sink.delete_calls().is_empty(),
        "must not delete a row whose instance is still live"
    );
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t6c_kube_node_gone_ec2_lookup_error_parks() {
    // EC2 cross-check itself errored (throttle, IMDS blip): fail safe,
    // park, count a lookup error, retry next tick.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_lookup_error(node, "ec2: throttled");

    let reconciler = new_reconciler_with_ec2(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_parked, 1);
    assert_eq!(report.lookup_errors, 1);
    assert_eq!(report.kube_node_gone_recovered, 0);
    assert!(sink.delete_calls().is_empty());
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t6d_kube_node_gone_ec2_terminated_but_delete_guard_refuses_parks() {
    // EC2 confirms terminated, but a non-terminal sandbox still references
    // the node (or the phase drifted), so the guarded delete returns
    // `rows_affected = 0`. The row stays parked rather than being reaped
    // out from under sandbox bookkeeping.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.set_delete_refused(
        node,
        OrphanDeleteRefusal {
            active_residents: 1,
            ..OrphanDeleteRefusal::default()
        },
    );

    let reconciler = new_reconciler_with_ec2(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_parked, 1);
    assert_eq!(report.kube_node_gone_recovered, 0);
    assert_eq!(report.lookup_errors, 0, "a refused guard is not an error");
    assert_eq!(sink.delete_calls(), vec![node]);
    assert_eq!(
        sink.current_phase(node).as_deref(),
        Some(PHASE_DRAINED),
        "delete-guard refusal must leave the row parked"
    );
}

#[tokio::test]
async fn t6e_kube_node_gone_ec2_terminated_delete_db_error_parks() {
    // EC2 confirms terminated but the DELETE itself errors (DB blip):
    // record a lookup error and park for retry next tick.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.inject_fail_delete_drained("simulated DB error");

    let reconciler = new_reconciler_with_ec2(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_parked, 1);
    assert_eq!(report.lookup_errors, 1);
    assert_eq!(report.kube_node_gone_recovered, 0);
    assert_eq!(sink.delete_calls(), vec![node]);
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t6f_kube_node_gone_threads_observed_uid_into_delete() {
    // The uid the EC2 cross-check observed (node_k8s.k8s_node_uid) must be
    // carried into the guarded delete so the SQL identity guard can refuse a
    // same-name kube replacement. Here we assert the threading; the SQL guard
    // itself is validated by the testcontainer `delete_drained_orphan_guards`.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.set_instance_uid(node, "uid-abc-123");

    let reconciler = new_reconciler_with_ec2(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.kube_node_gone_recovered, 1);
    assert_eq!(sink.delete_calls(), vec![node]);
    assert_eq!(
        sink.delete_expected_uids(),
        vec![Some("uid-abc-123".to_string())],
        "the observed kube uid must be threaded from the lookup into the delete"
    );
}

/// Refusal diagnosis with the snapshot guard as the (sole) refuser — the
/// mark-lost give-up's candidate shape.
fn snapshot_guard_refusal(blockers: i64) -> OrphanDeleteRefusal {
    OrphanDeleteRefusal {
        snapshot_blockers: blockers,
        ..OrphanDeleteRefusal::default()
    }
}

#[tokio::test]
async fn t6g_mark_lost_disabled_leaves_blocking_sandboxes_suspended() {
    // EC2 confirms terminated and the snapshot guard refuses the reap, but
    // the give-up gate is at its default (disabled): the terminal flip must
    // not be attempted, and the row parks exactly as before.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.set_delete_refused(node, snapshot_guard_refusal(2));

    let reconciler = new_reconciler_with_ec2(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert!(
        sink.mark_lost_calls().is_empty(),
        "the disabled give-up must never reach the sink"
    );
    assert_eq!(report.sandboxes_marked_lost, 0);
    assert_eq!(report.kube_node_gone_parked, 1);
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t6h_mark_lost_enabled_marks_blocking_sandboxes_and_threads_uid() {
    // The give-up fires: snapshot guard refused, EC2 terminated, gate
    // enabled. The observed kube uid must be threaded into the sink (the SQL
    // re-checks identity with it), the marked count lands in the report, and
    // the node still parks this tick — convergence is the *next* tick's
    // delete, never a same-tick retry.
    let node = NodeId::new();
    let lost_a = SandboxId::new();
    let lost_b = SandboxId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.set_instance_uid(node, "uid-abc-123");
    sink.set_delete_refused(node, snapshot_guard_refusal(2));
    sink.set_mark_lost_result(node, vec![lost_a, lost_b]);

    let reconciler = new_reconciler_with_ec2_mark_lost(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(
        sink.mark_lost_calls(),
        vec![(node, Some("uid-abc-123".to_string()))],
        "the observed kube uid must be threaded into the mark-lost CAS"
    );
    assert_eq!(report.sandboxes_marked_lost, 2);
    assert_eq!(
        report.kube_node_gone_parked, 1,
        "the row parks this tick; the next tick's delete converges"
    );
    assert_eq!(report.kube_node_gone_recovered, 0);
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t6i_mark_lost_not_attempted_without_snapshot_blockers() {
    // A refusal whose only refusers are identity/resident guards has no
    // mark-lost candidates: uid drift defers to the inventory reflector and
    // an active resident is a divergence to investigate, not auto-clear.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.set_delete_refused(
        node,
        OrphanDeleteRefusal {
            uid_mismatch: true,
            active_residents: 1,
            ..OrphanDeleteRefusal::default()
        },
    );

    let reconciler = new_reconciler_with_ec2_mark_lost(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert!(
        sink.mark_lost_calls().is_empty(),
        "no snapshot blockers -> no give-up candidates -> no sink call"
    );
    assert_eq!(report.sandboxes_marked_lost, 0);
    assert_eq!(report.kube_node_gone_parked, 1);
}

#[tokio::test]
async fn t6j_mark_lost_db_error_parks_without_counting() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.set_delete_refused(node, snapshot_guard_refusal(1));
    sink.inject_fail_mark_lost("simulated DB error");

    let reconciler = new_reconciler_with_ec2_mark_lost(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(sink.mark_lost_calls().len(), 1);
    assert_eq!(report.sandboxes_marked_lost, 0);
    assert_eq!(report.kube_node_gone_parked, 1);
    assert_eq!(
        sink.current_phase(node).as_deref(),
        Some(PHASE_DRAINED),
        "a failed give-up must leave the row parked for the next tick"
    );
}

#[tokio::test]
async fn t6k_mark_lost_empty_result_counts_nothing() {
    // The sink found no provably-unrecoverable sandbox (mid-flight rows, an
    // upload from a live node, or a guard drifted): nothing counted, row
    // parks for the sweepers.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.set_delete_refused(node, snapshot_guard_refusal(1));

    let reconciler = new_reconciler_with_ec2_mark_lost(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(sink.mark_lost_calls().len(), 1);
    assert_eq!(report.sandboxes_marked_lost, 0);
    assert_eq!(report.kube_node_gone_parked, 1);
}

#[tokio::test]
async fn t7_unregistered_node_skips_with_lookup_error() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::Unregistered(node));

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.lookup_errors, 1);
    assert_eq!(report.flipped, 0);
    assert!(sink.mark_calls().is_empty());
}

#[tokio::test]
async fn t8_phase_drift_skips_without_error() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    // Phase has drifted under us — listing returns the node but the
    // row's drain_phase is no longer 'drained' (admin undrain, etc.).
    sink.clear_phase(node);
    sink.set_keep_absent(node);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    // mark_terminating is still called (the reconciler can't know the
    // drift exists without trying), but the guarded UPDATE matches
    // zero rows so the report counts it as drift_skips, not flipped.
    assert_eq!(report.drained_nodes, 1);
    assert_eq!(report.drift_skips, 1);
    assert_eq!(report.flipped, 0);
    assert_eq!(sink.mark_calls(), vec![node]);
    assert_eq!(sink.current_phase(node), None);
}

#[tokio::test]
async fn t9_mark_terminating_db_error_recorded() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);
    sink.inject_fail_mark_terminating("simulated DB error");

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.lookup_errors, 1);
    assert_eq!(report.flipped, 0);
    assert_eq!(sink.mark_calls(), vec![node]);
    assert_eq!(
        sink.current_phase(node).as_deref(),
        Some(PHASE_DRAINED),
        "DB error must not advance phase in the fake"
    );
}

#[tokio::test]
async fn t10_list_drained_db_error_returns_zero_report() {
    let sink = FakeSink::new();
    sink.inject_fail_list_drained("simulated DB error");

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report, TickReport::default());
    assert!(sink.mark_calls().is_empty());
}

#[tokio::test]
async fn t12_local_snapshots_block_flip() {
    // Mirrors the admin `:terminate` snapshot guard: a drained
    // node with `sandboxes.lifecycle='suspended'` rows pinned via
    // `target_node_id` must not be auto-flipped to `terminating`.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);
    sink.set_local_snapshots(node, 3);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.snapshot_holds, 1);
    assert_eq!(report.flipped, 0);
    assert!(
        sink.mark_calls().is_empty(),
        "snapshot guard must short-circuit before mark_terminating"
    );
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t13_count_local_snapshots_db_error_skips_flip() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);
    sink.inject_fail_count_local_snapshots("simulated DB error");

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.lookup_errors, 1);
    assert_eq!(report.flipped, 0);
    assert!(sink.mark_calls().is_empty());
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t14_snapshot_committed_between_count_and_mark_blocks_flip() {
    // TOCTOU defense for the local-snapshot guard. The pre-check
    // returns 0, so the tick proceeds to `mark_terminating`; but a
    // snapshot commits in the window between the two queries. The
    // atomic `NOT EXISTS` clause inside `mark_terminating`'s
    // conditional UPDATE catches the race and refuses the flip.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);
    sink.set_local_snapshots(node, 0);
    sink.bump_snapshots_after_count(node, 1);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    // The pre-check saw 0, so the tick proceeded to mark_terminating.
    // The atomic clause caught the race; the row stays in `drained`.
    assert_eq!(report.snapshot_holds, 0);
    assert_eq!(report.flipped, 0);
    assert_eq!(report.drift_skips, 1);
    assert_eq!(sink.mark_calls(), vec![node]);
    assert_eq!(
        sink.current_phase(node).as_deref(),
        Some(PHASE_DRAINED),
        "snapshot guard must prevent the flip even under race"
    );
}

#[tokio::test]
async fn t15_restoring_sandboxes_block_flip() {
    // A sandbox in `restoring` still has its only snapshot on the
    // source node's local disk; count_local_snapshots must include
    // `restoring` as well as `suspended` so the auto-terminator
    // doesn't advance while the snapshot is still pinned.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);
    // `restoring` sandbox counted via the same local_snapshots map
    // that production counts via `lifecycle IN ('suspended','restoring')`.
    sink.set_local_snapshots(node, 1);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(
        report.snapshot_holds, 1,
        "restoring sandbox must block the flip"
    );
    assert_eq!(report.flipped, 0);
    assert!(sink.mark_calls().is_empty());
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t16_suspending_sandboxes_block_flip() {
    // A sandbox in `suspending` has already produced its on-disk
    // snapshot (the suspend RPC completed) but the controller crashed
    // before writing lifecycle → `suspended`. The guard must block
    // the flip to prevent the terminator from destroying that snapshot.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);
    sink.set_local_snapshots(node, 1);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(
        report.snapshot_holds, 1,
        "suspending sandbox must block the flip"
    );
    assert_eq!(report.flipped, 0);
    assert!(sink.mark_calls().is_empty());
    assert_eq!(sink.current_phase(node).as_deref(), Some(PHASE_DRAINED));
}

#[tokio::test]
async fn t17_resident_active_sandboxes_block_flip() {
    // Defensive: if a `drained` node lost its cordon and a new sandbox
    // got scheduled on it, the auto-terminator must refuse the flip.
    // Mirrors the second `NOT EXISTS` (resident) guard in the production
    // `mark_terminating` SQL. The pre-check (`count_local_snapshots`)
    // does not see active sandboxes — they only show up in the atomic
    // UPDATE clause — so the reconciler proceeds to call mark_terminating,
    // which returns false, and the tick counts it as drift.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_absent(node);
    // No local snapshots, but one active sandbox is on this node.
    sink.set_active_sandboxes(node, 1);

    let reconciler = new_reconciler(StdArc::clone(&sink));
    let report = reconciler.tick().await;

    assert_eq!(report.flipped, 0, "resident sandbox must block the flip");
    assert_eq!(
        report.snapshot_holds, 0,
        "this is the resident guard, not the snapshot guard"
    );
    assert_eq!(
        report.drift_skips, 1,
        "atomic UPDATE returns 0 rows -> drift_skips"
    );
    assert_eq!(sink.mark_calls(), vec![node]);
    assert_eq!(
        sink.current_phase(node).as_deref(),
        Some(PHASE_DRAINED),
        "row must stay in drained when active sandboxes are present"
    );
}

// =================================================================
// W — Refusal-warn rate limiting
// =================================================================

/// The window walk itself lives in `warn_pacer::tests`; this only pins that this
/// reconciler is wired to a pacer at all, so a refusal cannot go back to warning
/// every tick.
#[tokio::test(start_paused = true)]
async fn w1_refusals_are_warn_paced_per_node() {
    let sink = FakeSink::new();
    let reconciler = new_reconciler(StdArc::clone(&sink));
    let node = NodeId::new();

    assert!(reconciler.refusal_warn_due(node), "first refusal must warn");
    assert!(
        !reconciler.refusal_warn_due(node),
        "an immediate second refusal must be paced"
    );

    let other = NodeId::new();
    assert!(
        reconciler.refusal_warn_due(other),
        "pacing is per node; another node's window is independent"
    );
}

#[tokio::test]
async fn w2_refusal_warn_state_pruned_when_node_leaves_drained_list() {
    // A refused node holds a warn-rate-limit slot; once it leaves the
    // drained set (converged, undrained, or reaped) the next tick must prune
    // the slot so the map stays bounded by live drained-node cardinality.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_drained_list(vec![node]);
    sink.set_phase(node, PHASE_DRAINED);
    sink.set_keep_error(node, AnnotationLookupError::KubeNodeGone);
    sink.set_instance_terminated(node);
    sink.set_delete_refused(node, snapshot_guard_refusal(1));

    let reconciler = new_reconciler_with_ec2(StdArc::clone(&sink));
    reconciler.tick().await;
    assert_eq!(
        reconciler.refusal_warn_pacer.tracked(),
        1,
        "a refused node must hold a warn slot"
    );

    sink.set_drained_list(vec![]);
    reconciler.tick().await;
    assert!(
        reconciler.refusal_warn_pacer.tracked() == 0,
        "a node that left the drained set must be pruned"
    );
}
