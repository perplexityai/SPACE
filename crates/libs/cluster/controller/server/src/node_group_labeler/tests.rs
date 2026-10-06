//! # Test matrix
//!
//! ## C — `ReconcilerConfig` + env parsing
//! The name grammar is `NodeGroupName`'s (in `common-core`, with its own
//! tests); `store/tests.rs` additionally pins that it still agrees with the
//! `node_groups.name` CHECK. `write_concurrency` isn't read from the
//! environment; its clamp is pinned where it bites: `t53` measures the patch
//! fan-out's clamp through a real tick — the ceiling that actually bounds
//! concurrent apiserver writes — and `t52` the clamp function's boundaries.
//! `from_env` reads only `SPACE_NODE_GROUP_LABELER_ENABLED`, through the
//! shared `env_config::truthy_default`, and is not re-tested here.
//!
//! Do not test `from_env` by mutating the process environment, with or without
//! a lock. `set_var`'s contract demands exclusion of every concurrent environment
//! *access*, readers included and not merely writers of the same variable, so a
//! mutex confined to this suite cannot discharge it: under the `cargo test`
//! fallback that `AGENTS.md` mandates when `cargo nextest` is absent the whole
//! crate's tests share one process, and `warm_pool::tests` writes
//! `SPACE_INTERNAL_TOKEN` there with no lock at all. Asserting `from_env`
//! soundly needs a read seam on the production type, not a lock.
//!
//! ## T — Tick behavior (fakes)
//! **`tN_` is a stable identifier, not a position in a sequence and not a
//! count.** The numbering has gaps (t7, t15, t18, t26, t27, t29, t54, t55) and
//! the highest number therefore exceeds the number of tests. t7/t15/t18/t27/t29
//! were the `node_group_id` mirror phase's cases, removed with the phase itself
//! — `node_group_mirror` owns that column now. `t54` asserted an env-read
//! clamp, since removed, by mutating the process environment, which section C
//! above explains this suite cannot do soundly. t26 and t55 folded into `t25`'s outcome table. The
//! gaps stay: the tests
//! cross-reference each other by these names, so renumbering would rewrite every
//! surviving one and invalidate every reference. A new case takes the next
//! unused number, never a freed one.
//!
//! - `t1_unlabeled_workers_get_the_default_label`
//! - `t2_already_labeled_workers_are_not_patched`
//! - `t3_foreign_group_label_never_overwritten`
//! - `t4_ensure_failure_aborts_tick_before_any_kube_call`
//! - `t5_label_error_on_one_node_does_not_block_others`
//! - `t6_invalid_stored_group_name_refuses_to_label`
//! - `t8_stored_default_name_wins_over_config`
//! - `t9_dedicated_tainted_node_is_never_default_labeled`
//! - `t10_dedicated_node_with_default_label_is_left_alone`
//! - `t11_spent_tick_budget_defers_the_remainder`
//! - `t12_stalled_patch_times_out_and_does_not_block_other_nodes`
//! - `t13_all_nodes_labeled_under_serial_write_concurrency`
//! - `t14_non_default_group_suppresses_all_labeling`
//! - `t16_census_failure_writes_nothing`
//! - `t17_list_failure_reports_list_error`
//! - `t19_group_appearing_during_labeling_is_reported_as_a_race`
//! - `t20_post_label_census_failure_owes_the_same_audit`
//! - `t21_recheck_runs_even_on_a_quiet_tick`
//! - `t22_stale_resource_version_patch_is_rejected`
//! - `t23_deferred_nodes_are_attempted_first_on_the_next_tick`
//! - `t24_name_conflict_aborts_the_tick_without_labeling`
//! - `t25_a_census_race_owes_an_audit_only_from_an_issued_patch` (one row per
//!   `LabelOutcome`)
//! - `t28_seed_blocked_aborts_the_tick_without_labeling`
//! - `t30_suppressed_set_membership_not_cardinality`
//! - `t31_closed_gate_is_not_reported_as_a_race`
//! - `t32_early_return_leaves_the_tick_unmeasured`
//! - `t33_gauges_are_withheld_unless_the_tick_measured`
//! - `t34_race_diagnostics_name_only_the_newly_patched_nodes`
//! - `t35_foreign_group_warn_is_budgeted_across_ticks`
//! - `t36_foreign_group_baseline_clears_when_the_condition_lifts`
//! - `t37_suppressed_baseline_clears_when_the_gate_reopens`
//! - `t38_label_phase_is_clamped_to_the_tick_not_the_write_timeout`
//! - `t39_foreign_group_is_reported_even_when_the_tick_bails_at_the_recheck`
//! - `t40_a_seeded_default_group_emits_the_system_audit_entry`
//! - `t41_an_existing_default_group_emits_no_audit_entry`
//! - `t42_an_already_cancelled_token_attempts_no_patches`
//! - `t43_cancellation_during_the_label_phase_still_rechecks_the_census`
//! - `t44_a_spent_budget_outranks_a_cancelled_token`
//! - `t45_a_store_side_version_bump_does_not_fail_the_patch`
//! - `t46_a_node_gone_from_the_store_is_skipped_not_patched`
//! - `t47_a_group_or_taint_acquired_mid_tick_is_never_overwritten`
//! - `t48_log_budget_grants_exactly_its_cap_then_denies` (the rationing
//!   primitive itself, not a tick)
//! - `t49_a_saturated_dedicated_mislabel_budget_still_names_a_patch_failure`
//! - `t50_a_tick_clamped_write_does_not_draw_on_the_failure_budget`
//! - `t51_mislabeled_dedicated_nodes_are_published_as_a_gauge`
//! - `t52_write_concurrency_is_clamped_at_both_ends` (the clamp itself, not a
//!   tick)
//! - `t53_write_concurrency_ceiling_bounds_the_patch_fan_out`

use pplx_space_common_core::types::NodeGroupId;
use std::sync::Mutex;

use pplx_space_common_core::types::{NodeGroupName, PositiveDuration};
use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
use tracing_subscriber::layer::SubscriberExt;

use super::*;
// The log-rationing primitive moved into `tick` with the tick body; it is not
// part of the module's public surface, so the glob above does not reach it.
use super::tick::claim_log_budget;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

fn default_group(name: &str) -> DefaultNodeGroup {
    DefaultNodeGroup {
        id: NodeGroupId::new(),
        name: NodeGroupName::parse(name).expect("test group name is valid"),
    }
}

struct FakeSink {
    group: Mutex<Result<EnsuredDefaultGroup, EnsureDefaultGroupError>>,
    /// Census outcomes, consumed front-to-back so consecutive reads can
    /// disagree. The gate is read twice per tick: before labeling, and again
    /// afterwards whenever that first read returned zero — regardless of whether
    /// any patch was issued, which is what `t21` pins. `None` makes that read
    /// fail. The last entry repeats once the sequence is exhausted.
    census: Mutex<Vec<Option<i64>>>,
    /// Census reads served, so a test can pin that the post-write re-check
    /// still ran. The sequence alone cannot say: its last entry repeats, so a
    /// read that never happened is indistinguishable from one that re-read the
    /// same value.
    census_reads: std::sync::atomic::AtomicUsize,
}

impl FakeSink {
    /// Steady state: a default group already exists, so the tick resolves it
    /// without seeding. `Existing` is the right default for the matrix — all
    /// but the first tick of a cluster's life takes this path, and it is the
    /// one that must **not** emit an admin audit entry.
    fn with_group(name: &str) -> Self {
        Self {
            group: Mutex::new(Ok(EnsuredDefaultGroup::Existing(default_group(name)))),
            ..Self::blank()
        }
    }

    /// First tick of a cluster's life: this call performed the seed INSERT.
    /// The only shape that owes an audit entry.
    fn seeding_group(name: &str) -> Self {
        Self {
            group: Mutex::new(Ok(EnsuredDefaultGroup::Seeded(default_group(name)))),
            ..Self::blank()
        }
    }

    /// Generic (retryable) seed failure — the "schema isn't deployed yet"
    /// shape.
    fn failing(_message: &str) -> Self {
        Self {
            group: Mutex::new(Err(EnsureDefaultGroupError::Db(sqlx::Error::PoolClosed))),
            ..Self::blank()
        }
    }

    /// No default exists but other groups do, so the store refuses to seed.
    fn seed_blocked(other_groups: i64) -> Self {
        Self {
            group: Mutex::new(Err(EnsureDefaultGroupError::SeedBlockedByExistingGroups {
                other_groups,
            })),
            ..Self::blank()
        }
    }

    /// The stored default row's name doesn't parse — the SQL-boundary
    /// rejection, which no valid `DefaultNodeGroup` can represent.
    fn invalid_stored_name(name: &str) -> Self {
        let source = NodeGroupName::parse(name).expect_err("fixture name must be invalid");
        Self {
            group: Mutex::new(Err(EnsureDefaultGroupError::InvalidName {
                name: name.to_string(),
                source,
            })),
            ..Self::blank()
        }
    }

    /// The seed name is taken by an existing non-default group — the arm that
    /// retrying will never clear.
    fn name_conflict(name: &str) -> Self {
        Self {
            group: Mutex::new(Err(EnsureDefaultGroupError::NameConflict {
                name: name.to_string(),
            })),
            ..Self::blank()
        }
    }

    /// The id of the group this fake resolves, so a test can assert the audit
    /// entry names *that* group rather than merely some UUID.
    fn group_id(&self) -> NodeGroupId {
        match &*self.group.lock().unwrap() {
            Ok(ensured) => ensured.clone().into_group().id,
            Err(err) => panic!("this fake resolves no group: {err}"),
        }
    }

    fn blank() -> Self {
        Self {
            group: Mutex::new(Err(EnsureDefaultGroupError::Db(sqlx::Error::PoolClosed))),
            census: Mutex::new(vec![Some(0)]),
            census_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// How many times the tick read the census. Two on any tick that reached
    /// the post-write re-check.
    fn census_reads(&self) -> usize {
        self.census_reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Report `count` groups besides the default on every census, which is
    /// what closes the labeling gate.
    fn with_non_default_groups(self, count: i64) -> Self {
        *self.census.lock().unwrap() = vec![Some(count)];
        self
    }

    /// Change the census between ticks, for the standing conditions whose
    /// reporting depends on the gate opening or closing again.
    fn set_non_default_groups(&self, count: i64) {
        *self.census.lock().unwrap() = vec![Some(count)];
    }

    /// Drive each census read in turn, so the pre-labeling gate and the
    /// post-labeling re-check can disagree. `None` fails that read.
    fn with_census_sequence(self, outcomes: &[Option<i64>]) -> Self {
        *self.census.lock().unwrap() = outcomes.to_vec();
        self
    }

    fn fail_census(self) -> Self {
        *self.census.lock().unwrap() = vec![None];
        self
    }
}

impl NodeGroupSink for std::sync::Arc<FakeSink> {
    async fn ensure_default_group(
        &self,
        _seed_name: &NodeGroupName,
    ) -> Result<EnsuredDefaultGroup, EnsureDefaultGroupError> {
        match &*self.group.lock().unwrap() {
            Ok(group) => Ok(group.clone()),
            Err(EnsureDefaultGroupError::NameConflict { name }) => {
                Err(EnsureDefaultGroupError::NameConflict { name: name.clone() })
            }
            Err(EnsureDefaultGroupError::InvalidName { name, source }) => {
                Err(EnsureDefaultGroupError::InvalidName {
                    name: name.clone(),
                    source: source.clone(),
                })
            }
            Err(EnsureDefaultGroupError::SeedBlockedByExistingGroups { other_groups }) => {
                Err(EnsureDefaultGroupError::SeedBlockedByExistingGroups {
                    other_groups: *other_groups,
                })
            }
            Err(_) => Err(EnsureDefaultGroupError::Db(sqlx::Error::PoolClosed)),
        }
    }

    async fn non_default_group_count(&self) -> Result<i64, sqlx::Error> {
        self.census_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut census = self.census.lock().unwrap();
        // Last entry repeats once the sequence is exhausted.
        let outcome = if census.len() > 1 {
            census.remove(0)
        } else {
            census.first().copied().unwrap_or(Some(0))
        };
        outcome.ok_or(sqlx::Error::PoolClosed)
    }
}

#[derive(Default)]
struct FakeLabels {
    nodes: Mutex<Vec<WorkerNode>>,
    /// `(node_name, label_value, resource_version)` recorded for every patch.
    label_calls: Mutex<Vec<(String, String, String)>>,
    /// Node names whose patch should fail.
    fail_labels: Mutex<Vec<String>>,
    /// Node names whose patch should hang past [`LABEL_WRITE_TIMEOUT`].
    stall_labels: Mutex<Vec<String>>,
    list_error: Mutex<Option<String>>,
    /// Every `label_node` entry, recorded before any stall or failure. Unlike
    /// `label_calls` (successes only) this is the record of what the tick
    /// *attempted*, which is what the fairness cursor is about.
    attempts: Mutex<Vec<String>>,
    /// `node name -> resourceVersion the apiserver currently holds`. Any node
    /// listed here rejects a patch whose `resource_version` doesn't match,
    /// standing in for the 409 the real optimistic-concurrency precondition
    /// produces. This is what makes the never-overwrite invariant testable at
    /// the seam rather than only as a pure-function assertion on the patch
    /// body.
    current_versions: Mutex<std::collections::BTreeMap<String, String>>,
    /// `node name -> what a pre-write re-read of the store returns`, with
    /// `None` meaning "gone from the store".
    ///
    /// Separate from `nodes` on purpose: the whole point of the re-read is that
    /// the store may have moved on since `list_worker_nodes` handed back the
    /// snapshot, and the tick offers no hook to mutate the fake between the two.
    /// An entry here makes the two reads disagree without one. Absent, the store
    /// still holds exactly the listed node — the quiescent case every other test
    /// is written against.
    store_rereads: Mutex<std::collections::BTreeMap<String, Option<WorkerNode>>>,
    /// How long every patch takes before it returns, so writes issued in the
    /// same fan-out batch are genuinely in flight at the same time. A fake that
    /// returns on its first poll has already decremented `in_flight` before the
    /// next future is polled, so `peak_in_flight` would read 1 at any configured
    /// width and could not see a ceiling being exceeded. Under a paused clock
    /// this costs no real time.
    patch_hold: Mutex<Option<Duration>>,
    /// Patches between entry and exit of `label_node`, and the high-water mark
    /// of that count. Nothing else can observe the fan-out's *width*:
    /// `label_calls` and `attempts` record how many patches happened, never how
    /// many overlapped.
    in_flight: std::sync::atomic::AtomicUsize,
    peak_in_flight: std::sync::atomic::AtomicUsize,
}

/// Counts one patch as in flight for as long as it lives. A guard rather than a
/// pair of statements because `label_node` returns early on a version conflict
/// and on a configured failure, and a timed-out patch is *dropped* mid-flight
/// — every one of those has to decrement.
struct InFlightPatch<'a>(&'a FakeLabels);

impl<'a> InFlightPatch<'a> {
    fn enter(labels: &'a FakeLabels) -> Self {
        let depth = labels
            .in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        labels
            .peak_in_flight
            .fetch_max(depth, std::sync::atomic::Ordering::SeqCst);
        Self(labels)
    }
}

impl Drop for InFlightPatch<'_> {
    fn drop(&mut self) {
        self.0
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl FakeLabels {
    fn with_nodes(nodes: Vec<WorkerNode>) -> Self {
        Self {
            nodes: Mutex::new(nodes),
            ..Self::default()
        }
    }

    /// Add a node to the listed fleet between ticks, standing in for one
    /// joining the cluster.
    fn add_node(&self, node: WorkerNode) {
        self.nodes.lock().unwrap().push(node);
    }

    fn fail_label(&self, name: &str) {
        self.fail_labels.lock().unwrap().push(name.to_string());
    }

    fn stall_label(&self, name: &str) {
        self.stall_labels.lock().unwrap().push(name.to_string());
    }

    fn fail_list(&self, message: &str) {
        *self.list_error.lock().unwrap() = Some(message.to_string());
    }

    /// Make the apiserver's copy of `name` sit at `version`, so a patch
    /// carrying anything else is rejected the way a real 409 would be.
    fn set_current_version(&self, name: &str, version: &str) {
        self.current_versions
            .lock()
            .unwrap()
            .insert(name.to_string(), version.to_string());
    }

    /// Make a pre-write re-read of `name` return `reread` instead of the listed
    /// node — `None` for "no longer in the store".
    fn set_store_reread(&self, name: &str, reread: Option<WorkerNode>) {
        self.store_rereads
            .lock()
            .unwrap()
            .insert(name.to_string(), reread);
    }

    /// Make every patch take `hold` to return, so a fan-out batch overlaps and
    /// `peak_in_flight` reports the batch's real width.
    fn hold_each_patch(&self, hold: Duration) {
        *self.patch_hold.lock().unwrap() = Some(hold);
    }

    /// The widest the patch fan-out ever got.
    fn peak_in_flight(&self) -> usize {
        self.peak_in_flight
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn attempts(&self) -> Vec<String> {
        self.attempts.lock().unwrap().clone()
    }

    fn label_calls(&self) -> Vec<(String, String, String)> {
        self.label_calls.lock().unwrap().clone()
    }

    fn labeled_names_and_values(&self) -> Vec<(String, String)> {
        self.label_calls()
            .into_iter()
            .map(|(name, value, _rv)| (name, value))
            .collect()
    }
}

impl WorkerNodeLabels for std::sync::Arc<FakeLabels> {
    async fn list_worker_nodes(&self) -> Result<Vec<WorkerNode>, WorkerNodeLabelsError> {
        if let Some(message) = self.list_error.lock().unwrap().clone() {
            return Err(WorkerNodeLabelsError::Backend(message));
        }
        Ok(self.nodes.lock().unwrap().clone())
    }

    async fn current_worker_node(&self, node: &WorkerNode) -> Option<WorkerNode> {
        if let Some(reread) = self.store_rereads.lock().unwrap().get(&node.name) {
            return reread.clone();
        }
        // Quiescent default: the store still holds the node the snapshot listed,
        // matched on uid the way the kube impl does.
        self.nodes
            .lock()
            .unwrap()
            .iter()
            .find(|listed| listed.name == node.name && listed.uid == node.uid)
            .cloned()
    }

    async fn label_node(
        &self,
        node: &WorkerNode,
        value: String,
    ) -> Result<(), WorkerNodeLabelsError> {
        let _in_flight = InFlightPatch::enter(self);
        self.attempts.lock().unwrap().push(node.name.clone());
        // Before the outcome branches: a real round trip costs time whether it
        // succeeds or fails, and every branch below is inside the in-flight
        // window this hold makes observable. Read out of the mutex first so no
        // guard is held across the await.
        let hold = *self.patch_hold.lock().unwrap();
        if let Some(hold) = hold {
            tokio::time::sleep(hold).await;
        }
        if let Some(current) = self.current_versions.lock().unwrap().get(&node.name)
            && *current != node.resource_version
        {
            return Err(WorkerNodeLabelsError::Backend(format!(
                "Operation cannot be fulfilled on nodes \"{}\": the object has been \
                 modified; please apply your changes to the latest version and try again \
                 (sent {}, have {current})",
                node.name, node.resource_version
            )));
        }
        if self.fail_labels.lock().unwrap().contains(&node.name) {
            return Err(WorkerNodeLabelsError::Backend(format!(
                "patch {} failed",
                node.name
            )));
        }
        if self.stall_labels.lock().unwrap().contains(&node.name) {
            // Far past LABEL_WRITE_TIMEOUT; under a paused clock the
            // timeout fires without the test actually waiting.
            tokio::time::sleep(Duration::from_secs(600)).await;
        }
        self.label_calls.lock().unwrap().push((
            node.name.clone(),
            value,
            node.resource_version.clone(),
        ));
        Ok(())
    }
}

fn worker(name: &str, node_group: Option<&str>) -> WorkerNode {
    WorkerNode {
        name: name.to_string(),
        uid: format!("uid-{name}"),
        resource_version: format!("rv-{name}"),
        node_group: node_group.map(str::to_string),
        dedicated: false,
    }
}

fn dedicated_worker(name: &str, node_group: Option<&str>) -> WorkerNode {
    WorkerNode {
        dedicated: true,
        ..worker(name, node_group)
    }
}

fn reconciler(
    sink: std::sync::Arc<FakeSink>,
    labels: std::sync::Arc<FakeLabels>,
) -> Reconciler<std::sync::Arc<FakeSink>, std::sync::Arc<FakeLabels>> {
    reconciler_with_config(
        ReconcilerConfig {
            enabled: true,
            ..ReconcilerConfig::default()
        },
        sink,
        labels,
    )
}

/// A reconciler on a shutdown token that is never cancelled — the right default
/// for every test but the two wind-down ones, which build their own so they can
/// fire it.
fn reconciler_with_config(
    config: ReconcilerConfig,
    sink: std::sync::Arc<FakeSink>,
    labels: std::sync::Arc<FakeLabels>,
) -> Reconciler<std::sync::Arc<FakeSink>, std::sync::Arc<FakeLabels>> {
    Reconciler::new(config, sink, labels, CancellationToken::new())
}

// -----------------------------------------------------------------
// T — tick behavior
// -----------------------------------------------------------------

#[tokio::test]
async fn t1_unlabeled_workers_get_the_default_label() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-a.ec2.internal", None),
        worker("ip-b.ec2.internal", None),
    ]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.workers, 2);
    assert_eq!(report.labeled, 2);
    assert_eq!(report.label_errors, 0);
    assert_eq!(report.label_deferred, 0);
    // Each patch carries the resourceVersion the node was listed with —
    // the optimistic-concurrency precondition that keeps a concurrent
    // foreign label from being overwritten. Patch *order* is not
    // guaranteed: the writes fan out via `buffer_unordered`.
    let mut calls = labels.label_calls();
    calls.sort();
    assert_eq!(
        calls,
        vec![
            (
                "ip-a.ec2.internal".to_string(),
                "workers".to_string(),
                "rv-ip-a.ec2.internal".to_string()
            ),
            (
                "ip-b.ec2.internal".to_string(),
                "workers".to_string(),
                "rv-ip-b.ec2.internal".to_string()
            ),
        ]
    );
}

#[tokio::test]
async fn t2_already_labeled_workers_are_not_patched() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        Some("workers"),
    )]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.labeled, 0);
    assert_eq!(report.already_labeled, 1);
    assert!(
        labels.label_calls().is_empty(),
        "a node already carrying the default label must not be re-patched"
    );
}

#[tokio::test]
async fn t3_foreign_group_label_never_overwritten() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-dedicated.ec2.internal",
        Some("acme-dedicated"),
    )]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.foreign_group, 1);
    assert_eq!(report.labeled, 0);
    assert!(labels.label_calls().is_empty());
}

#[tokio::test]
async fn t4_ensure_failure_aborts_tick_before_any_kube_call() {
    let sink = std::sync::Arc::new(FakeSink::failing("node_groups does not exist"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.ensure_errors, 1);
    assert_eq!(
        report.seed_blocked_errors, 0,
        "a plain Db failure is the transient arm — the one that clears when the \
         node-groups migration lands"
    );
    assert_eq!(report.workers, 0);
    assert!(labels.label_calls().is_empty());
}

#[tokio::test]
async fn t5_label_error_on_one_node_does_not_block_others() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-bad.ec2.internal", None),
        worker("ip-good.ec2.internal", None),
    ]));
    labels.fail_label("ip-bad.ec2.internal");
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.labeled, 1);
    assert_eq!(report.label_errors, 1);
    // Only the good node carries the label; the failed one retries next tick.
    assert_eq!(
        labels.labeled_names_and_values(),
        vec![("ip-good.ec2.internal".to_string(), "workers".to_string())]
    );
}

/// A default row whose stored name is not a valid node-group slug — only
/// reachable via an out-of-band write, since the table has its own CHECK —
/// must never propagate to a Node label. The store rejects it at the SQL
/// boundary, so the tick sees `InvalidName` and aborts down the
/// operator-attention path rather than the retryable "schema isn't deployed
/// yet" one. (The name can no longer even be *represented* as a
/// `DefaultNodeGroup`, which is the point of parsing at the boundary.)
#[tokio::test]
async fn t6_invalid_stored_group_name_refuses_to_label() {
    let sink = std::sync::Arc::new(FakeSink::invalid_stored_name("Not-A-Valid-Label!"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(
        report.seed_blocked_errors, 1,
        "an unusable stored name needs an operator, so it books the terminal kind"
    );
    assert_eq!(
        report.ensure_errors, 0,
        "and must not read as the transient missing-migration signal"
    );
    assert_eq!(report.census_errors, 0);
    assert!(labels.attempts().is_empty());
}

/// A dedicated node matches the worker selector and cannot carry its
/// (NodeRestriction-protected) group label, so the taint is the only thing
/// distinguishing it — and the labeler must not claim it into the default
/// generic group, which would hand an org's reserved hardware to the shared
/// pool.
///
/// This covers the case where the taint *is* already present. It can also
/// arrive after the node registers (the taint is applied control-plane-side —
/// see `DEDICATED_TAINT_KEY`), and in that window nothing here distinguishes
/// the node; the `non_default_group_count` gate is what covers it, since a
/// dedicated group existing at all closes the gate.
#[tokio::test]
async fn t9_dedicated_tainted_node_is_never_default_labeled() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        dedicated_worker("ip-dedicated-new.ec2.internal", None),
        worker("ip-shared.ec2.internal", None),
    ]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.skipped_dedicated, 1);
    assert_eq!(report.labeled, 1);
    // Only the shared worker is patched.
    assert_eq!(
        labels.labeled_names_and_values(),
        vec![("ip-shared.ec2.internal".to_string(), "workers".to_string())]
    );
}

/// A dedicated-tainted node that somehow carries the DEFAULT group label
/// (operator mistake or pre-exclusion residue) is still out of scope: not
/// patched, not counted as a default member — surfaced via the warn log for
/// operator attention.
#[tokio::test]
async fn t10_dedicated_node_with_default_label_is_left_alone() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![dedicated_worker(
        "ip-dedicated.ec2.internal",
        Some("workers"),
    )]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.skipped_dedicated, 1);
    assert_eq!(report.already_labeled, 0);
    assert!(labels.label_calls().is_empty());
}

/// Once the tick's budget is spent, the remaining writes are never issued —
/// counted `label_deferred`, not as errors.
///
/// The budget is exhausted legitimately here rather than by configuring a zero
/// tick: `tick` is a `PositiveDuration` precisely so a zero (which would panic
/// `tokio::time::interval` in `run`) can't be constructed. With a 5 s budget,
/// serial writes and stalled patches, the first patch consumes the whole
/// budget and the second finds nothing left.
#[tokio::test(start_paused = true)]
async fn t11_spent_tick_budget_defers_the_remainder() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-a.ec2.internal", None),
        worker("ip-b.ec2.internal", None),
    ]));
    labels.stall_label("ip-a.ec2.internal");
    labels.stall_label("ip-b.ec2.internal");
    let reconciler = reconciler_with_config(
        ReconcilerConfig {
            enabled: true,
            tick: PositiveDuration::new_panic(LABEL_WRITE_TIMEOUT),
            write_concurrency: 1,
            ..ReconcilerConfig::default()
        },
        sink,
        labels.clone(),
    );
    let report = reconciler.tick().await;

    assert_eq!(report.labeled, 0);
    // The first write was issued and timed out; the second never started.
    assert_eq!(report.label_indeterminate, 1);
    assert_eq!(report.label_deferred, 1);
    assert_eq!(report.label_errors, 0);
    assert_eq!(labels.attempts().len(), 1, "only one patch was issued");
}

/// A patch that hangs is bounded by `LABEL_WRITE_TIMEOUT` and — the point of
/// fanning the writes out — does not stop the other nodes from being labeled
/// in the same tick.
///
/// The timed-out node is counted `label_indeterminate`, **not**
/// `label_errors`: the request reached the apiserver, so the label may have
/// been applied even though we never saw the response, and the next tick
/// re-reads it.
#[tokio::test(start_paused = true)]
async fn t12_stalled_patch_times_out_and_does_not_block_other_nodes() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-stalled.ec2.internal", None),
        worker("ip-good.ec2.internal", None),
    ]));
    labels.stall_label("ip-stalled.ec2.internal");
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.labeled, 1);
    assert_eq!(report.label_indeterminate, 1);
    assert_eq!(
        report.label_errors, 0,
        "a timeout is not evidence the patch failed"
    );
    assert_eq!(
        labels.labeled_names_and_values(),
        vec![("ip-good.ec2.internal".to_string(), "workers".to_string())]
    );
}

/// How a row of [`t25_a_census_race_owes_an_audit_only_from_an_issued_patch`]
/// drives its whole candidate fleet onto a single [`LabelOutcome`].
enum LabelDrive {
    /// Every patch is acked.
    Acked,
    /// Every patch comes back an error.
    Errored,
    /// Every patch hangs past `LABEL_WRITE_TIMEOUT`.
    Stalled,
    /// The shutdown token is already cancelled when the label phase starts, so
    /// no request is issued. `t42` pins that shape on its own.
    Cancelled,
    /// The pre-write re-read finds every node gone from the store, so no
    /// request is issued. `t46` pins that shape on its own.
    Gone,
    /// Serial writes on a tick budget of exactly one write timeout, every patch
    /// stalled: the first spends the whole phase and the rest find nothing left.
    /// `t11` pins that shape on its own.
    ///
    /// A fleet landing *entirely* on `BudgetSpent` is unreachable, which is why
    /// this row is the one mixed set in the table. The deadline is anchored when
    /// the phase starts, so the first candidate always has budget; reaching a
    /// spent budget takes a patch that spent it, and that patch was issued.
    BudgetSpent,
}

/// One [`LabelOutcome`] and what the audit predicate owes for it.
struct CensusRaceRow {
    /// The variant this row's fleet produces, for the failure messages.
    outcome: &'static str,
    drive: LabelDrive,
    /// Expected `TickReport` counts over the row's two candidates. Spelled out
    /// per row rather than derived from `drive`, so a classification change has
    /// to be re-stated here rather than following the code.
    labeled: usize,
    label_errors: usize,
    label_indeterminate: usize,
    label_deferred: usize,
    /// Patches the phase issued — the predicate's whole input.
    attempts: usize,
    /// Whether a group appearing mid-tick owes an operator label audit.
    audit: bool,
}

/// Which `LabelOutcome`s owe an operator label audit when a non-default group
/// appears between the two census reads, and which do not.
///
/// One axis, one table. Every row's fleet lands on a single outcome and a group
/// appears mid-tick; `gate_race_detected` must fire for every outcome that
/// **issued** a request and must not fire for any outcome that contacted
/// nothing.
///
/// The issued half is fail-closed on purpose. A `Failed` patch is not proof no
/// label landed — the kube seam flattens every `kube::Error` into one variant,
/// so a transport reset while reading the response and a 5xx from an
/// intermediary in front of an apiserver that already committed the patch both
/// land there — and an `Indeterminate` one is the same story with the response
/// never read at all. Gating the audit on confirmed successes would leave
/// exactly those ticks silent, and this is the design's one signal for a
/// mislabeled tenant boundary.
///
/// The not-issued half is the direction a fold-in of `label_deferred` breaks,
/// and the shape it breaks is routine rather than exotic: a rolling deploy
/// cancels the shutdown token before the label phase — during the untimed seed
/// or census, a real window — so every candidate books
/// `label_deferred_shutdown` with zero requests issued, and an operator group
/// create landing in that same window would then page for a tenant-isolation
/// label audit over a tick that contacted the apiserver not once.
///
/// `t21` cannot cover the not-issued half: it reaches `patches_issued == 0`
/// through a fleet with nothing to label, so its `label_deferred` is zero too
/// and both readings of the predicate agree there. Every row here carries a
/// full candidate set, which is what makes them disagree. `t19` owns the
/// `Labeled` row's wider contract (the audit log's node names); the row here
/// pins only its side of this predicate.
#[tokio::test(start_paused = true)]
async fn t25_a_census_race_owes_an_audit_only_from_an_issued_patch() {
    let rows = [
        CensusRaceRow {
            outcome: "Labeled",
            drive: LabelDrive::Acked,
            labeled: 2,
            label_errors: 0,
            label_indeterminate: 0,
            label_deferred: 0,
            attempts: 2,
            audit: true,
        },
        CensusRaceRow {
            outcome: "Failed",
            drive: LabelDrive::Errored,
            labeled: 0,
            label_errors: 2,
            label_indeterminate: 0,
            label_deferred: 0,
            attempts: 2,
            audit: true,
        },
        CensusRaceRow {
            outcome: "Indeterminate",
            drive: LabelDrive::Stalled,
            labeled: 0,
            label_errors: 0,
            label_indeterminate: 2,
            label_deferred: 0,
            attempts: 2,
            audit: true,
        },
        CensusRaceRow {
            outcome: "NotAttempted(Shutdown)",
            drive: LabelDrive::Cancelled,
            labeled: 0,
            label_errors: 0,
            label_indeterminate: 0,
            label_deferred: 2,
            attempts: 0,
            audit: false,
        },
        CensusRaceRow {
            outcome: "NotAttempted(Superseded)",
            drive: LabelDrive::Gone,
            labeled: 0,
            label_errors: 0,
            label_indeterminate: 0,
            label_deferred: 2,
            attempts: 0,
            audit: false,
        },
        CensusRaceRow {
            outcome: "NotAttempted(BudgetSpent)",
            drive: LabelDrive::BudgetSpent,
            labeled: 0,
            label_errors: 0,
            label_indeterminate: 1,
            label_deferred: 1,
            attempts: 1,
            audit: true,
        },
    ];

    for row in rows {
        let case = row.outcome;
        // Gate reads 0 so labeling proceeds, re-check reads 1: a group appeared
        // while the phase was running.
        let sink = std::sync::Arc::new(
            FakeSink::with_group("workers").with_census_sequence(&[Some(0), Some(1)]),
        );
        let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
            worker("ip-a.ec2.internal", None),
            worker("ip-b.ec2.internal", None),
        ]));
        let mut config = ReconcilerConfig {
            enabled: true,
            ..ReconcilerConfig::default()
        };
        let token = CancellationToken::new();
        match row.drive {
            LabelDrive::Acked => {}
            LabelDrive::Errored => {
                labels.fail_label("ip-a.ec2.internal");
                labels.fail_label("ip-b.ec2.internal");
            }
            LabelDrive::Stalled => {
                labels.stall_label("ip-a.ec2.internal");
                labels.stall_label("ip-b.ec2.internal");
            }
            LabelDrive::Cancelled => token.cancel(),
            LabelDrive::Gone => {
                labels.set_store_reread("ip-a.ec2.internal", None);
                labels.set_store_reread("ip-b.ec2.internal", None);
            }
            LabelDrive::BudgetSpent => {
                labels.stall_label("ip-a.ec2.internal");
                labels.stall_label("ip-b.ec2.internal");
                config.tick = PositiveDuration::new_panic(LABEL_WRITE_TIMEOUT);
                config.write_concurrency = 1;
            }
        }

        let report = Reconciler::new(config, sink.clone(), labels.clone(), token)
            .tick()
            .await;

        // The premise: the row really did land on the outcome it claims.
        assert_eq!(report.workers, 2, "{case}: both candidates were listed");
        assert_eq!(report.labeled, row.labeled, "{case}: labeled");
        assert_eq!(report.label_errors, row.label_errors, "{case}: errored");
        assert_eq!(
            report.label_indeterminate, row.label_indeterminate,
            "{case}: indeterminate"
        );
        assert_eq!(
            report.label_deferred, row.label_deferred,
            "{case}: deferred"
        );
        assert_eq!(
            labels.attempts().len(),
            row.attempts,
            "{case}: patches issued; attempted {:?}",
            labels.attempts()
        );
        // Unconditional on a tick that passed the gate, so the group has a way
        // to become visible at all. Asserted per row because the sequence's last
        // entry repeats: a read that never happened is otherwise
        // indistinguishable from one that re-read the same value. (`t31` covers
        // why the re-check stays conditional on the gate having been open.)
        assert_eq!(
            sink.census_reads(),
            2,
            "{case}: the re-check has to run, or the group never appears to \
             this tick"
        );

        assert_eq!(
            report.gate_race_detected, row.audit,
            "{case}: that flag's metric promises a human label audit on every \
             occurrence"
        );
        // The two flags are the same decision from opposite sides, so pinning
        // only one would let a tick book both or neither.
        assert_eq!(
            report.census_unusable_no_writes, !row.audit,
            "{case}: a tick that owes no audit still ends unmeasured — the \
             census its counts were classified against changed under it"
        );
        assert!(
            !report.completed,
            "{case}: the census this tick's counts were classified against has \
             changed"
        );
    }
}

/// Guards the floor of [`clamp_write_concurrency`] at the fan-out.
/// `buffer_unordered(0)` never fills its queue, so it yields `Pending` forever
/// with no waker — a zero would wedge the tick rather than fail it, and this
/// test would hang instead of failing. The value isn't read from the
/// environment, so the clamp guards a programmatic `ReconcilerConfig`; the
/// field is `pub`, so that is one careless caller away. `t52` covers the same
/// function's ceiling. The assertions themselves are concurrency-agnostic on
/// purpose — what is pinned is that a `0` still makes progress.
#[tokio::test]
async fn t13_all_nodes_labeled_under_serial_write_concurrency() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-a.ec2.internal", None),
        worker("ip-b.ec2.internal", None),
        worker("ip-c.ec2.internal", None),
    ]));
    let reconciler = reconciler_with_config(
        ReconcilerConfig {
            enabled: true,
            write_concurrency: 0,
            ..ReconcilerConfig::default()
        },
        sink,
        labels.clone(),
    );
    let report = reconciler.tick().await;

    assert_eq!(report.labeled, 3);
    assert_eq!(report.label_errors, 0);
    // Sorted: `buffer_unordered` completes in arbitrary order even at a
    // concurrency of one-after-clamping.
    let mut labeled: Vec<String> = labels
        .labeled_names_and_values()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    labeled.sort();
    assert_eq!(
        labeled,
        vec![
            "ip-a.ec2.internal".to_string(),
            "ip-b.ec2.internal".to_string(),
            "ip-c.ec2.internal".to_string()
        ]
    );
}

#[tokio::test]
async fn t8_stored_default_name_wins_over_config() {
    // Config seeds "workers" (the Reconciler default), but the store says
    // the default group is named "shared-pool" — nodes are labeled with
    // the stored name.
    let sink = std::sync::Arc::new(FakeSink::with_group("shared-pool"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    let report = reconciler(sink.clone(), labels.clone()).tick().await;

    assert_eq!(report.labeled, 1);
    assert_eq!(
        labels.labeled_names_and_values(),
        vec![("ip-a.ec2.internal".to_string(), "shared-pool".to_string())]
    );
}

// -----------------------------------------------------------------
// T14 onward — the labeling gate and the remaining error paths
// -----------------------------------------------------------------

/// Once *any* non-default node group exists, an
/// unlabeled selector-matched node is no longer necessarily a default-pool
/// worker — it may be a freshly-joined member of that other group, whose
/// NodeRestriction-protected label cannot have landed yet, or a dedicated
/// node whose taint the control plane hasn't applied yet. Labeling it would
/// hand reserved hardware to the shared pool, and the label is never
/// corrected. So labeling stops entirely.
#[tokio::test]
async fn t14_non_default_group_suppresses_all_labeling() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers").with_non_default_groups(1));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-a.ec2.internal", None),
        worker("ip-b.ec2.internal", None),
    ]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.label_suppressed, 2);
    assert_eq!(report.labeled, 0);
    assert_eq!(report.label_errors, 0);
    assert!(labels.label_calls().is_empty());
}

/// A failed group census must not fall through to labeling: without the
/// count the gate's precondition is unknown, so no label is written. The node
/// snapshot is already in hand by then (the
/// census is read as late as possible, right before the patches) — but that
/// read is a reflector scan, not an apiserver call, so nothing was spent.
#[tokio::test]
async fn t16_census_failure_writes_nothing() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers").fail_census());
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.census_errors, 1);
    assert_eq!(
        report.ensure_errors, 0,
        "a census failure must not read as the missing-migration signal"
    );
    assert_eq!(report.workers, 1, "the reflector snapshot was already read");
    assert_eq!(report.labeled, 0);
    assert!(labels.label_calls().is_empty());
}

#[tokio::test]
async fn t17_list_failure_reports_list_error() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    labels.fail_list("apiserver unavailable");
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.list_errors, 1);
    assert_eq!(report.workers, 0);
    assert!(labels.label_calls().is_empty());
}

/// The gate is a check, not a transaction — a
/// group row can land between the census and the patches. The labels are
/// already written and can't be attributed after the fact, so all the tick can
/// do is refuse to call itself a measurement and flag an operator audit.
#[tokio::test]
async fn t19_group_appearing_during_labeling_is_reported_as_a_race() {
    // Gate reads 0 (labeling proceeds), re-check reads 1 (a group appeared).
    let sink = std::sync::Arc::new(
        FakeSink::with_group("workers").with_census_sequence(&[Some(0), Some(1)]),
    );
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    let report = reconciler(sink, labels).tick().await;

    assert_eq!(report.labeled, 1, "the patch had already gone out");
    assert!(report.gate_race_detected);
    assert!(
        !report.completed,
        "the census this tick's counts were classified against has changed"
    );
}

/// Same shape when the re-check itself fails: without confirmation that the
/// gate still holds, the tick owes the same audit as an observed race.
#[tokio::test]
async fn t20_post_label_census_failure_owes_the_same_audit() {
    // Gate read succeeds; the post-label re-check errors.
    let sink =
        std::sync::Arc::new(FakeSink::with_group("workers").with_census_sequence(&[Some(0), None]));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    let report = reconciler(sink, labels).tick().await;

    assert_eq!(report.labeled, 1);
    // Labels were written and the gate can no longer be confirmed, which owes
    // the same audit as an observed race — but it is a distinct condition, and
    // conflating the two would have it reported under contracts that promise
    // either "nothing was written" (`census`) or "a group appeared"
    // (`gate_race_detected`).
    assert_eq!(report.census_after_write_errors, 1);
    assert_eq!(
        report.census_errors, 0,
        "this tick wrote labels, so it must not book the counter whose contract \
         is 'nothing was written'"
    );
    assert!(
        !report.gate_race_detected,
        "no group was observed to appear — the census read itself failed"
    );
    assert!(!report.completed);
}

/// The post-label census re-check runs on every tick that passed the gate,
/// including quiet ones where nothing was labeled. (It is skipped when the gate
/// was already closed — see `t31` for why that must stay conditional.)
///
/// A quiet tick's counts were still classified against the pre-phase census, so
/// they need the same re-validation the write-heavy path gets. Here the second
/// census reports a group that appeared mid-tick, and the tick ends unmeasured
/// even though it patched nothing.
#[tokio::test]
async fn t21_recheck_runs_even_on_a_quiet_tick() {
    let sink = std::sync::Arc::new(
        FakeSink::with_group("workers").with_census_sequence(&[Some(0), Some(1)]),
    );
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        Some("workers"),
    )]));
    let report = reconciler(sink, labels).tick().await;

    assert_eq!(report.labeled, 0, "nothing needed labeling");
    assert_eq!(report.already_labeled, 1);
    assert!(
        report.census_unusable_no_writes,
        "the re-check must run even when the tick issued no patches"
    );
    assert!(
        !report.gate_race_detected,
        "nothing was written, so no label audit is owed — that flag's metric \
         promises one on every occurrence"
    );
    assert!(!report.completed);
}

/// The never-overwrite invariant is enforced by
/// the resourceVersion precondition on the patch, not by the read-time label
/// check — and that has to hold at the seam, not just in `label_patch`'s
/// unit test. Here the apiserver's copy of the node has moved on past anything
/// the reflector store holds — the reflector is lagging, which the pre-write
/// re-read cannot fix because it reads that same store — so the patch is
/// rejected. The node must not be counted as labeled: the next tick re-reads and
/// sees whatever label actually landed.
#[tokio::test]
async fn t22_stale_resource_version_patch_is_rejected() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-raced.ec2.internal", None),
        worker("ip-quiet.ec2.internal", None),
    ]));
    // `worker()` stamps rv-<name>; move the apiserver's copy past it.
    labels.set_current_version("ip-raced.ec2.internal", "rv-moved-on");
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.labeled, 1);
    assert_eq!(report.label_errors, 1);
    assert_eq!(
        labels.labeled_names_and_values(),
        vec![("ip-quiet.ec2.internal".to_string(), "workers".to_string())]
    );
}

/// The tick budget caps how many patches are
/// *attempted*, and the node snapshot arrives in a stable order — so without
/// a fairness cursor the same head would be retried every tick and the tail
/// would never be attempted at all. Here every patch stalls, so the budget
/// runs out partway through; the next tick must start where this one stopped.
#[tokio::test(start_paused = true)]
async fn t23_deferred_nodes_are_attempted_first_on_the_next_tick() {
    let names = [
        "ip-a.ec2.internal",
        "ip-b.ec2.internal",
        "ip-c.ec2.internal",
        "ip-d.ec2.internal",
        "ip-e.ec2.internal",
    ];
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(
        names.iter().map(|n| worker(n, None)).collect(),
    ));
    for name in names {
        labels.stall_label(name);
    }
    // Serial writes, and a budget of two full LABEL_WRITE_TIMEOUTs, so each
    // tick gets through part of the list and defers the rest.
    let reconciler = reconciler_with_config(
        ReconcilerConfig {
            enabled: true,
            tick: PositiveDuration::new_panic(Duration::from_secs(10)),
            write_concurrency: 1,
            ..ReconcilerConfig::default()
        },
        sink.clone(),
        labels.clone(),
    );

    let first = reconciler.tick().await;
    let attempted_first = labels.attempts();
    assert!(
        first.label_deferred > 0,
        "the budget must run out mid-list for this test to mean anything"
    );
    assert!(!attempted_first.is_empty());

    let second = reconciler.tick().await;
    let attempted_second: Vec<String> = labels.attempts()[attempted_first.len()..].to_vec();
    assert!(second.label_deferred > 0);

    // The second tick begins with a node the first one never reached — not by
    // re-attempting the same head.
    assert!(
        !attempted_first.contains(&attempted_second[0]),
        "tick 2 restarted at {:?}, which tick 1 already attempted ({attempted_first:?}) \
         — the fairness cursor did not advance",
        attempted_second[0]
    );
    // No starvation: keep ticking and every node gets attempted. Each tick
    // fits two stalled patches (10 s budget / 5 s ceiling, serial), so five
    // nodes need three ticks — the point is that the set keeps growing rather
    // than the same head repeating forever.
    for _ in 0..3 {
        reconciler.tick().await;
    }
    let mut all: Vec<String> = labels.attempts();
    all.sort();
    all.dedup();
    assert_eq!(
        all.len(),
        names.len(),
        "some node was never attempted across five ticks — the tail is starved"
    );
}

/// A `NameConflict` is routed to its own arm, not the generic retryable one:
/// it needs an operator decision, and reporting it as the routine
/// "schema isn't deployed yet" case sends a responder to the wrong place.
#[tokio::test]
async fn t24_name_conflict_aborts_the_tick_without_labeling() {
    let sink = std::sync::Arc::new(FakeSink::name_conflict("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.seed_blocked_errors, 1);
    assert_eq!(
        report.ensure_errors, 0,
        "the terminal arm must not book the transient missing-migration kind"
    );
    assert_eq!(report.census_errors, 0);
    assert_eq!(report.workers, 0);
    assert!(labels.attempts().is_empty());
}

/// `SeedBlockedByExistingGroups` routes down the operator-attention arm, not
/// the retryable "schema isn't deployed yet" one — the store refused to create
/// a default because doing so would reassign every un-grouped node, and no
/// number of retries changes that.
#[tokio::test]
async fn t28_seed_blocked_aborts_the_tick_without_labeling() {
    let sink = std::sync::Arc::new(FakeSink::seed_blocked(2));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.seed_blocked_errors, 1);
    assert_eq!(
        report.ensure_errors, 0,
        "the terminal arm must not book the transient missing-migration kind"
    );
    assert_eq!(report.census_errors, 0);
    assert_eq!(report.workers, 0);
    assert!(labels.attempts().is_empty());
}

/// The suppressed-node alert keys on set membership, not on the size of the
/// set. A 1:1 node replacement while the gate is closed keeps the count flat
/// while genuinely introducing a worker that will never be labeled, so
/// cardinality misses it.
///
/// `newly_suppressed` is on `TickReport` precisely so this is assertable: it
/// is the value the error-vs-debug decision is made from, and asserting
/// `label_suppressed` alone cannot distinguish the two implementations.
#[tokio::test]
async fn t30_suppressed_set_membership_not_cardinality() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers").with_non_default_groups(1));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-old.ec2.internal",
        None,
    )]));
    let reconciler = reconciler(sink.clone(), labels.clone());

    let first = reconciler.tick().await;
    assert_eq!(first.label_suppressed, 1);
    assert_eq!(first.newly_suppressed, 1);

    // Replace the node 1:1 — same count, different node.
    *labels.nodes.lock().unwrap() = vec![worker("ip-new.ec2.internal", None)];
    let second = reconciler.tick().await;

    assert_eq!(
        second.label_suppressed, 1,
        "count is unchanged, which is exactly why cardinality is the wrong signal"
    );
    assert_eq!(
        second.newly_suppressed, 1,
        "a different node entered the set, so this must register as newly \
         suppressed — a cardinality check would report zero here"
    );

    // A tick with no membership change reports nothing newly suppressed.
    let third = reconciler.tick().await;
    assert_eq!(third.label_suppressed, 1);
    assert_eq!(third.newly_suppressed, 0);
    // Neither tick labeled anything.
    assert_eq!(second.labeled, 0);
}

/// A gate that was already closed before the tick started is not a race. The
/// post-label census re-check must therefore stay conditional on the gate having
/// been *open*: run unconditionally, a suppressed tick re-reads the same
/// non-zero count and reports "a group appeared while this tick was writing" —
/// false, since nothing was written, and repeating every tick for as long as
/// another group exists.
#[tokio::test]
async fn t31_closed_gate_is_not_reported_as_a_race() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers").with_non_default_groups(1));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-unlabeled.ec2.internal", None),
        worker("ip-labeled.ec2.internal", Some("workers")),
    ]));
    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.label_suppressed, 1);
    assert!(
        !report.gate_race_detected,
        "an already-closed gate must not be reported as a mid-write race"
    );
    // Nothing written either way — the suppression branch already handled it.
    assert!(labels.attempts().is_empty());
}

/// A tick that returns early must not be mistaken for a measured one.
///
/// `TickReport::completed` is what gates gauge publication; `t33` covers the
/// publication itself. This one pins the flag on the three pre-measurement
/// early returns — seed failure, list failure, pre-write census failure — where
/// the report is otherwise `default()` and a forgotten flag would publish an
/// all-zero fleet. `t28`/`t31` cover the post-write census arms.
#[tokio::test]
async fn t32_early_return_leaves_the_tick_unmeasured() {
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));

    // Seed failure: bails before anything is measured.
    let seed_failed = reconciler(
        std::sync::Arc::new(FakeSink::failing("schema missing")),
        labels.clone(),
    )
    .tick()
    .await;
    assert!(!seed_failed.completed);
    assert_eq!(seed_failed.workers, 0, "nothing was measured");

    // List failure: same, one stage later.
    let list_failing = std::sync::Arc::new(FakeLabels::with_nodes(Vec::new()));
    list_failing.fail_list("apiserver unavailable");
    let list_failed = reconciler(
        std::sync::Arc::new(FakeSink::with_group("workers")),
        list_failing,
    )
    .tick()
    .await;
    assert!(!list_failed.completed);

    // Census failure: bails after the snapshot but before any write.
    let census_failed = reconciler(
        std::sync::Arc::new(FakeSink::with_group("workers").fail_census()),
        labels.clone(),
    )
    .tick()
    .await;
    assert!(!census_failed.completed);

    // A tick that runs through is the only one marked measured.
    let ok = reconciler(
        std::sync::Arc::new(FakeSink::with_group("workers")),
        labels.clone(),
    )
    .tick()
    .await;
    assert!(ok.completed);
    assert_eq!(ok.workers, 1);
}

/// The audit diagnostics on an audit-owing tick must name the nodes *this
/// tick* patched, not a slice of every default-labeled node.
///
/// On a mostly-labeled fleet the two sets differ almost entirely, so a bounded
/// slice of the wider one would list nodes carrying a label this tick never
/// wrote and inflate the truncation count — hiding exactly the nodes the
/// advertised security audit needs.
#[tokio::test]
async fn t34_race_diagnostics_name_only_the_newly_patched_nodes() {
    // A fleet already labeled under names that sort ahead of the new node.
    let mut nodes: Vec<WorkerNode> = ["aaa-1", "aaa-2", "aaa-3"]
        .iter()
        .map(|n| worker(n, Some("workers")))
        .collect();
    nodes.push(worker("zzz-new", None));

    // Gate open, then a group appears while the tick is writing.
    let sink = std::sync::Arc::new(
        FakeSink::with_group("workers").with_census_sequence(&[Some(0), Some(1)]),
    );
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(nodes));
    let report = reconciler(sink, labels).tick().await;

    assert_eq!(report.labeled, 1);
    assert_eq!(report.already_labeled, 3);
    assert!(report.gate_race_detected);
    assert_eq!(
        report.newly_labeled,
        vec!["zzz-new".to_string()],
        "the audit set must be the node this tick patched, not the \
         alphabetically-first already-labeled ones"
    );
}

/// A worker labeled with a group that does not exist is a standing condition, so
/// the warn naming it is budgeted across ticks, not just within one.
///
/// Nothing reconciles this state away: the labeler never overwrites a label, so
/// the node stays mislabeled until an operator intervenes. Warning on every tick
/// would emit one line per node twice a minute forever — 2,880 a day for a
/// single node — and SPACE routes warn to Datadog. The gauge
/// (`foreign_group_nodes`) carries the standing value; this log is the
/// breadcrumb, and it fires only on nodes that *newly* entered the set. Keyed on
/// membership rather than count so a 1:1 replacement still reports.
#[tokio::test]
async fn t35_foreign_group_warn_is_budgeted_across_ticks() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-mislabeled.ec2.internal",
        Some("does-not-exist"),
    )]));
    let reconciler = reconciler(sink.clone(), labels.clone());

    let first = reconciler.tick().await;
    assert_eq!(first.foreign_group, 1);
    assert_eq!(
        first.newly_foreign_group, 1,
        "the first sighting must be reported"
    );

    let second = reconciler.tick().await;
    assert_eq!(
        second.foreign_group, 1,
        "the gauge must keep carrying the standing condition"
    );
    assert_eq!(
        second.newly_foreign_group, 0,
        "the same node must not re-warn on every tick"
    );

    // A second mislabeled node joins: membership grew, so this one is reported
    // even though the first is still suppressed.
    labels.add_node(worker("ip-mislabeled-2.ec2.internal", Some("other-ghost")));
    let third = reconciler.tick().await;
    assert_eq!(third.foreign_group, 2);
    assert_eq!(
        third.newly_foreign_group, 1,
        "a newly-joined mislabeled node must be reported even while the first is quiet"
    );
}

/// The foreign-group baseline is dropped once the condition stops holding, so a
/// later recurrence is reported rather than silently compared against a stale
/// set.
///
/// The warn only makes its claim ("names a group that did not exist as of this
/// tick's census") while the default is the only group. Once another group exists the same label may name a
/// real one, so the branch goes quiet — and it must not carry its old membership
/// forward, or a node that leaves and rejoins never warns again.
#[tokio::test]
async fn t36_foreign_group_baseline_clears_when_the_condition_lifts() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-mislabeled.ec2.internal",
        Some("does-not-exist"),
    )]));
    let reconciler = reconciler(sink.clone(), labels.clone());

    assert_eq!(reconciler.tick().await.newly_foreign_group, 1);
    assert_eq!(reconciler.tick().await.newly_foreign_group, 0);

    // Another group appears: the label may now name a real group, so the branch
    // is skipped and the baseline drops.
    sink.set_non_default_groups(1);
    let suppressed = reconciler.tick().await;
    assert_eq!(
        suppressed.newly_foreign_group, 0,
        "the branch does not run while another group exists"
    );

    // Back to the default being the only group: the same node is a fresh
    // sighting again, because the claim is newly provable.
    sink.set_non_default_groups(0);
    assert_eq!(
        reconciler.tick().await.newly_foreign_group,
        1,
        "a recurrence must be reported, not compared against a stale baseline"
    );
}

/// The suppression baseline is dropped once the gate reopens, so a *second*
/// suppression episode is reported instead of being compared against the first.
///
/// Mirror image of `t36`, and the reason both exist: the baselines are what make
/// a standing condition quiet, so the clear is what keeps a *new* episode loud.
/// Without it, a node suppressed in episode one is permanently absent from
/// `newly_suppressed`, and a re-closed gate leaving it dark never re-reports —
/// the one signal that says shared capacity is unusable goes silent exactly when
/// it recurs. Removing the gate-open clear leaves every other test in this file
/// green, which is why this one is here.
#[tokio::test]
async fn t37_suppressed_baseline_clears_when_the_gate_reopens() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers").with_non_default_groups(1));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-unlabeled.ec2.internal",
        None,
    )]));
    // The node must stay unlabeled across the gate-open tick, or it leaves the
    // suppressed set for the ordinary reason and the test proves nothing.
    labels.fail_label("ip-unlabeled.ec2.internal");
    let reconciler = reconciler(sink.clone(), labels.clone());

    let first = reconciler.tick().await;
    assert_eq!(first.label_suppressed, 1);
    assert_eq!(
        first.newly_suppressed, 1,
        "the first suppression episode must be reported"
    );
    assert_eq!(
        reconciler.tick().await.newly_suppressed,
        0,
        "the same suppressed node must not re-report every tick"
    );

    // Gate opens: the labeler tries and fails, so the node is still unlabeled.
    sink.set_non_default_groups(0);
    let open = reconciler.tick().await;
    assert_eq!(open.label_suppressed, 0, "nothing is suppressed while open");
    assert_eq!(open.label_errors, 1, "the patch was attempted and failed");

    // Gate closes again with the node still dark: a new episode, so it must be
    // reported rather than silenced by the stale baseline.
    sink.set_non_default_groups(1);
    let reclosed = reconciler.tick().await;
    assert_eq!(reclosed.label_suppressed, 1);
    assert_eq!(
        reclosed.newly_suppressed, 1,
        "a second suppression episode must be reported, not compared against the first"
    );
}

/// The label phase cannot outlast one `config.tick`, because each write's budget
/// is `min(LABEL_WRITE_TIMEOUT, time left in the phase)`.
///
/// `t11` pins the *other* half of that pair — the `remaining.is_zero()` early-out
/// that defers the rest once the deadline passes. This one pins the clamp itself,
/// which is what keeps the *first* write from overrunning a tick shorter than
/// `LABEL_WRITE_TIMEOUT`. Dropping `.min(remaining)` leaves every other test in
/// this file green: the outcome is `Indeterminate` either way and only the
/// elapsed time differs, so the assertion has to be on the clock.
///
/// Why it matters: the phase is anchored after the seed and the census, and the
/// Postgres calls around it are untimed. A tick that can run to
/// `tick + LABEL_WRITE_TIMEOUT` pushes the reconciler's worst case past the
/// drain window the supervisor allows. Cancellation does not close this hole:
/// it stops the phase *issuing* further patches (`t42`, `t43`) but never
/// shortens one already in flight, so the clamp is the only bound on the first
/// write.
#[tokio::test(start_paused = true)]
async fn t38_label_phase_is_clamped_to_the_tick_not_the_write_timeout() {
    // A tick deliberately shorter than one write's own ceiling, so the clamp is
    // the only thing that can bound the phase.
    let tick = LABEL_WRITE_TIMEOUT / 2;
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-a.ec2.internal", None),
        worker("ip-b.ec2.internal", None),
    ]));
    labels.stall_label("ip-a.ec2.internal");
    labels.stall_label("ip-b.ec2.internal");
    let reconciler = reconciler_with_config(
        ReconcilerConfig {
            enabled: true,
            tick: PositiveDuration::new_panic(tick),
            write_concurrency: 1,
            ..ReconcilerConfig::default()
        },
        sink.clone(),
        labels.clone(),
    );

    let started = tokio::time::Instant::now();
    let report = reconciler.tick().await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < LABEL_WRITE_TIMEOUT,
        "the phase must be bounded by the tick ({tick:?}), not by the per-write \
         ceiling ({LABEL_WRITE_TIMEOUT:?}); took {elapsed:?}"
    );
    // The first write is cut short by the tick budget rather than by apiserver
    // slowness, so it is indeterminate; the second never gets a budget at all.
    assert_eq!(report.label_indeterminate, 1);
    assert_eq!(report.label_deferred, 1);
    assert_eq!(report.labeled, 0);
    assert_eq!(
        report.label_errors, 0,
        "running out of tick is not a write failure"
    );
}

/// A permanently unlabelable node is still named on a tick that bails at the
/// post-write census re-check.
///
/// Every arm of that re-check returns early, and `foreign_group_nodes` is a gauge
/// withheld on an unmeasured tick — so if the foreign-group report sits *below*
/// the re-check, a fleet that fails it every tick (a stuck patch keeping
/// `patches_issued > 0`, plus a census read that keeps failing) never reports the
/// mislabeled node through either channel. That is the one condition the module
/// calls permanently unlabelable, so it must not be the one that goes silent.
#[tokio::test]
async fn t39_foreign_group_is_reported_even_when_the_tick_bails_at_the_recheck() {
    // Gate open on the pre-write read, then the re-check read fails — with
    // writes already issued, so the tick takes the audit-owing early return.
    let sink =
        std::sync::Arc::new(FakeSink::with_group("workers").with_census_sequence(&[Some(0), None]));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-unlabeled.ec2.internal", None),
        worker("ip-ghost.ec2.internal", Some("does-not-exist")),
    ]));
    let report = reconciler(sink, labels).tick().await;

    // The tick did bail: the gauges are withheld.
    assert_eq!(report.labeled, 1, "a patch was issued, so an audit is owed");
    assert_eq!(report.census_after_write_errors, 1);
    assert!(
        !report.completed,
        "the re-check failed after writing, so this tick is not a measurement"
    );

    // ...and the foreign node is still named.
    assert_eq!(report.foreign_group, 1);
    assert_eq!(
        report.newly_foreign_group, 1,
        "the mislabeled node must be reported even though the tick bailed — the \
         gauge is withheld here, so this log is the only signal left"
    );
}

/// The tick that performs the seed emits the system-actor entry on
/// `space.admin.audit`.
///
/// This is the reconciler's stand-in for the `/admin/api/v1` write it
/// bypasses: the seeded row is durable management state, so the trail has to
/// name it, its shape, and the machine principal that created it. Asserted on the
/// captured event rather than on a report field because the event *is* the
/// contract — a consumer reads these field names.
#[tokio::test]
async fn t40_a_seeded_default_group_emits_the_system_audit_entry() {
    let sink = std::sync::Arc::new(FakeSink::seeding_group("workers"));
    let group_id = sink.group_id();
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));

    let layer = CapturedEventsLayer::new().with_target("space.admin.audit");
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);
    // `set_default` rather than `with_default`: the tick is async, and
    // `#[tokio::test]` runs a current-thread runtime, so the thread-local
    // default covers every await of this task.
    let guard = tracing::subscriber::set_default(subscriber);
    let report = reconciler(sink, labels).tick().await;
    drop(guard);

    assert_eq!(
        report.labeled, 1,
        "the audit emission must not displace the tick's labeling work"
    );
    let captured = events.events();
    assert_eq!(
        captured.len(),
        1,
        "exactly one audit entry for the seed: {captured:?}"
    );
    let entry = &captured[0];
    // Same event name and field names as `record_node_group`, so existing
    // consumers need no change.
    assert_eq!(field_value(entry, "event"), Some("admin_audit"));
    assert_eq!(
        field_value(entry, "actor_user_id"),
        Some("system:node-group-labeler"),
        "a reconciler has no user, so it names itself as a machine principal"
    );
    assert_eq!(
        field_value(entry, "actor_email"),
        Some(""),
        "empty rather than fabricated — this stream is for attribution"
    );
    assert_eq!(
        field_value(entry, "action"),
        Some("node-group-seed-default")
    );
    assert_eq!(
        field_value(entry, "node_group_id"),
        Some(group_id.to_string().as_str())
    );
    assert_eq!(field_value(entry, "node_group_name"), Some("workers"));
    assert_eq!(
        field_value(entry, "from_state"),
        Some(""),
        "a create transitions from nothing"
    );
    assert_eq!(
        field_value(entry, "to_state"),
        Some("org=generic policy=default default=true"),
        "same summary shape the /admin/api/v1 handlers render"
    );
    assert_eq!(field_value(entry, "success"), Some("true"));
    assert_eq!(field_value(entry, "error"), Some(""));
}

/// A steady-state tick — the default group already exists — emits **no** audit
/// entry.
///
/// The behavioural constraint of the whole emission: `ensure_default_group`
/// succeeds on every tick, so an unconditional emit would post an
/// `admin_audit` event every `config.tick` (30s by default) for the life of
/// the cluster, drowning the operator writes the stream exists to record. Two
/// ticks, because one would also pass if the gate were "emit on the first tick
/// of this process".
#[tokio::test]
async fn t41_an_existing_default_group_emits_no_audit_entry() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));

    let layer = CapturedEventsLayer::new().with_target("space.admin.audit");
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);
    let reconciler = reconciler(sink, labels);
    let guard = tracing::subscriber::set_default(subscriber);
    let first = reconciler.tick().await;
    let second = reconciler.tick().await;
    drop(guard);

    // The fake does not reflect a patch back into its node list, so both ticks
    // do the same labeling work; what matters here is that both resolved the
    // default group without seeding it.
    assert_eq!(first.labeled, 1);
    assert_eq!(second.labeled, 1);
    let captured = events.events();
    assert!(
        captured.is_empty(),
        "resolving an existing default group is not an audited write: {captured:?}"
    );
}

/// A tick that starts with the shutdown token already cancelled issues **zero**
/// patches, and still returns a report.
///
/// Asserted on the fake's attempt log rather than on `label_deferred`: that
/// counter is also incremented by an ordinary budget-bound tick that wrote first
/// and deferred the tail, so only the attempt log can say nothing was contacted.
///
/// The tick still reaches its normal end — both census reads, `completed` — for
/// the reason it is not simply dropped on the token: `run` publishes the metrics
/// and the summary log from the value `tick` returns, and an abandoned future
/// returns nothing.
#[tokio::test(start_paused = true)]
async fn t42_an_already_cancelled_token_attempts_no_patches() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-a.ec2.internal", None),
        worker("ip-b.ec2.internal", None),
    ]));
    let token = CancellationToken::new();
    token.cancel();
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            enabled: true,
            ..ReconcilerConfig::default()
        },
        sink.clone(),
        labels.clone(),
        token,
    );

    let report = reconciler.tick().await;

    assert!(
        labels.attempts().is_empty(),
        "a tick cancelled before its label phase must not contact the apiserver \
         at all; attempted {:?}",
        labels.attempts()
    );
    assert_eq!(report.workers, 2, "the fleet was still measured");
    assert_eq!(report.labeled, 0);
    assert_eq!(
        report.label_deferred, 2,
        "both nodes defer to the next leader"
    );
    assert_eq!(
        report.label_deferred_shutdown, 2,
        "every deferral here is the wind-down, not a spent budget"
    );
    assert_eq!(
        sink.census_reads(),
        2,
        "the post-write census re-check must still run on a cancelled tick"
    );
    assert!(report.completed, "the tick reached its normal end");
}

/// Cancellation arriving *during* the label phase stops it attempting anything
/// further, and the tick still performs the post-write census re-check on its
/// way out.
///
/// The re-check is the whole reason the tick observes the token instead of being
/// dropped on it: it is what turns "a non-default group appeared while we were
/// writing" into an auditable event, and a `select!` against the token would
/// abandon the tick at an arbitrary await point and skip it.
///
/// The pre-phase check cannot be what fires here — the token is still live when
/// the phase starts — so this pins the per-node condition specifically. Every
/// patch stalls and the writes are serial, so the first one is in flight when
/// the cancellation lands; the tick budget is far larger than the whole list's
/// worth of writes, so a spent budget cannot be what defers the rest either.
#[tokio::test(start_paused = true)]
async fn t43_cancellation_during_the_label_phase_still_rechecks_the_census() {
    let names = [
        "ip-a.ec2.internal",
        "ip-b.ec2.internal",
        "ip-c.ec2.internal",
    ];
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(
        names.iter().map(|n| worker(n, None)).collect(),
    ));
    for name in names {
        labels.stall_label(name);
    }
    let token = CancellationToken::new();
    let canceller = token.clone();
    // Fires while the first patch is still inside its own LABEL_WRITE_TIMEOUT:
    // under a paused clock the runtime advances to this deadline first, then to
    // the write's.
    tokio::spawn(async move {
        tokio::time::sleep(LABEL_WRITE_TIMEOUT / 2).await;
        canceller.cancel();
    });
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            enabled: true,
            // 60s against three serial 5s writes: the budget has room for the
            // whole list, so it cannot be the thing that defers the tail.
            tick: PositiveDuration::new_panic(Duration::from_secs(60)),
            write_concurrency: 1,
            ..ReconcilerConfig::default()
        },
        sink.clone(),
        labels.clone(),
        token,
    );

    let report = reconciler.tick().await;

    assert_eq!(
        labels.attempts().len(),
        1,
        "only the patch already in flight may be attempted; attempted {:?}",
        labels.attempts()
    );
    assert_eq!(
        report.label_indeterminate, 1,
        "the in-flight patch was issued and timed out"
    );
    assert_eq!(report.label_deferred, 2);
    assert_eq!(
        report.label_deferred_shutdown, 2,
        "the tail deferred to the shutdown, not to a spent budget"
    );
    assert_eq!(
        sink.census_reads(),
        2,
        "the post-write census re-check must run even as the tick winds down — \
         dropping the tick's future on the token would skip it"
    );
    assert!(report.completed);
}

/// A tick where the budget is spent **and** the token is cancelled books the
/// untouched nodes as budget deferrals, not shutdown ones.
///
/// `label_one_node` checks the budget before the token deliberately: a node the
/// phase had already run out of time for was lost to the budget whether or not a
/// shutdown followed, so booking it as a shutdown deferral would overstate what
/// the wind-down cost — and `label_deferred_shutdown` is what an operator reads
/// to decide a deploy-time spike needs no action. Swapping the two guards leaves
/// every other test green, because they exercise one condition or the other and
/// never both.
///
/// Both conditions are made true before the tail is polled: the tick budget is
/// one `LABEL_WRITE_TIMEOUT` and the first patch stalls through all of it, so the
/// remaining nodes find `remaining == 0`; the token fires halfway through that
/// same write.
#[tokio::test(start_paused = true)]
async fn t44_a_spent_budget_outranks_a_cancelled_token() {
    let names = [
        "ip-a.ec2.internal",
        "ip-b.ec2.internal",
        "ip-c.ec2.internal",
    ];
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(
        names.iter().map(|n| worker(n, None)).collect(),
    ));
    for name in names {
        labels.stall_label(name);
    }
    let token = CancellationToken::new();
    let canceller = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(LABEL_WRITE_TIMEOUT / 2).await;
        canceller.cancel();
    });
    let reconciler = Reconciler::new(
        ReconcilerConfig {
            enabled: true,
            // Exactly one write's worth of budget, spent by the first stall.
            tick: PositiveDuration::new_panic(LABEL_WRITE_TIMEOUT),
            write_concurrency: 1,
            ..ReconcilerConfig::default()
        },
        sink.clone(),
        labels.clone(),
        token.clone(),
    );

    let report = reconciler.tick().await;

    assert!(
        token.is_cancelled(),
        "the fixture must have cancelled the token during the phase"
    );
    assert_eq!(
        labels.attempts().len(),
        1,
        "only the first patch had any budget; attempted {:?}",
        labels.attempts()
    );
    assert_eq!(report.label_indeterminate, 1);
    assert_eq!(report.label_deferred, 2);
    assert_eq!(
        report.label_deferred_shutdown, 0,
        "the tail was already out of budget before the cancellation, so charging \
         it to the wind-down would overstate what shutting down cost"
    );
}

/// A node whose stored `resourceVersion` advanced between the tick-opening
/// snapshot and its patch is still labeled.
///
/// This is the false-positive the pre-write re-read exists to remove. The
/// snapshot's version is up to a whole `config.tick` (30 s) old and every
/// unrelated Node write in that window bumps it — node-ctl's template-presence
/// labeler patches Node labels from a DaemonSet on every worker, fleet-wide. With
/// the snapshot's version as the precondition each of those writes turned into a
/// 409, landing in `label_write{outcome:error}` next to a real RBAC 403 with
/// nothing to tell them apart.
#[tokio::test]
async fn t45_a_store_side_version_bump_does_not_fail_the_patch() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    // Somebody else patched an unrelated label: the store (and the apiserver)
    // now hold a newer version than the snapshot the tick classified.
    labels.set_store_reread(
        "ip-a.ec2.internal",
        Some(WorkerNode {
            resource_version: "rv-bumped".to_string(),
            ..worker("ip-a.ec2.internal", None)
        }),
    );
    labels.set_current_version("ip-a.ec2.internal", "rv-bumped");

    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(
        report.labeled, 1,
        "an unrelated write in the snapshot window must not fail the patch"
    );
    assert_eq!(report.label_errors, 0);
    assert_eq!(report.label_superseded, 0);
    assert_eq!(
        labels.label_calls(),
        vec![(
            "ip-a.ec2.internal".to_string(),
            "workers".to_string(),
            "rv-bumped".to_string()
        )],
        "the patch must carry the version from the re-read, not the snapshot"
    );
}

/// A node that left the store between the snapshot and its patch is skipped
/// rather than patched blind.
///
/// Patching it would 404 and book `label_write{outcome:error}` — the same series
/// an RBAC failure uses — for a node that is simply gone. Skipping books it as a
/// deferral, which is what it is: nothing was written, and the next tick's
/// snapshot decides whether there is anything left to do.
#[tokio::test]
async fn t46_a_node_gone_from_the_store_is_skipped_not_patched() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-gone.ec2.internal", None),
        worker("ip-here.ec2.internal", None),
    ]));
    labels.set_store_reread("ip-gone.ec2.internal", None);

    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.labeled, 1);
    assert_eq!(report.label_superseded, 1);
    assert_eq!(
        report.label_deferred, 1,
        "a superseded node counts in the deferred total, so the gauge keeps its \
         'patch never issued' meaning"
    );
    assert_eq!(
        report.label_errors, 0,
        "a vanished node is not a write failure"
    );
    assert_eq!(
        labels.attempts(),
        vec!["ip-here.ec2.internal".to_string()],
        "the vanished node must never be contacted"
    );
}

/// The never-overwrite invariant survives the re-read: a node that acquired a
/// node-group label — or the dedicated taint — between the snapshot and its patch
/// is left alone.
///
/// Before the re-read this was enforced as a side effect of the stale
/// precondition: any group assignment bumps the resourceVersion, so the patch
/// 409'd. Now that the patch carries a *current* version that side effect is
/// gone, and the check has to be explicit — which is exactly the guard this
/// pins. The taint arm matters for the same reason and is applied
/// control-plane-side, so it can land in this very window.
#[tokio::test]
async fn t47_a_group_or_taint_acquired_mid_tick_is_never_overwritten() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![
        worker("ip-grouped.ec2.internal", None),
        worker("ip-tainted.ec2.internal", None),
        worker("ip-plain.ec2.internal", None),
    ]));
    labels.set_store_reread(
        "ip-grouped.ec2.internal",
        Some(worker("ip-grouped.ec2.internal", Some("acme-dedicated"))),
    );
    labels.set_store_reread(
        "ip-tainted.ec2.internal",
        Some(dedicated_worker("ip-tainted.ec2.internal", None)),
    );

    let report = reconciler(sink, labels.clone()).tick().await;

    assert_eq!(report.labeled, 1);
    assert_eq!(report.label_superseded, 2);
    assert_eq!(report.label_errors, 0);
    assert_eq!(
        labels.attempts(),
        vec!["ip-plain.ec2.internal".to_string()],
        "neither the newly-grouped nor the newly-tainted node may be patched"
    );
}

/// The log-rationing primitive grants exactly its cap and then denies
/// everything, forever.
///
/// Table-driven over the boundary rather than one call: forcing
/// `claim_log_budget` to always return `true` — making the whole
/// `LABEL_FAILURE_LOGGED_NODES` cap inert — leaves every tick test in this file
/// green, because a cap on how many lines a tick emits is invisible to a report
/// field. On a kilonode fleet the difference is 10 lines a tick and 1,000.
#[test]
fn t48_log_budget_grants_exactly_its_cap_then_denies() {
    // Zero is included: it is what every caller sees once the budget is spent,
    // and `fetch_update`'s `checked_sub` is the only thing keeping it from
    // wrapping to `usize::MAX` and re-opening the tap.
    for cap in [0usize, 1, 3, LABEL_FAILURE_LOGGED_NODES] {
        let budget = std::sync::atomic::AtomicUsize::new(cap);
        let granted = (0..cap + 3).filter(|_| claim_log_budget(&budget)).count();
        assert_eq!(
            granted, cap,
            "a budget of {cap} must grant exactly {cap} lines"
        );
        assert!(
            !claim_log_budget(&budget),
            "a spent budget of {cap} must stay spent"
        );
    }
}

/// A fleet carrying enough dedicated-mislabeled nodes to exhaust
/// `DEDICATED_MISLABEL_LOGGED_NODES` still names a patch failure.
///
/// That is the entire reason the second budget exists. The mislabel condition is
/// standing — neither the label nor the taint is reconciled away — and its warn
/// runs in the classification loop, *before* the patch phase. Pointed at one
/// shared budget it would consume every slot on every tick, leaving
/// `label_errors` climbing with no line carrying the cause, on the one rollout
/// where the cause is what an operator needs.
///
/// Asserted on the emitted lines rather than on a counter: `report`'s
/// `mislabeled_dedicated` counts every such node (and `t51` pins the gauge it
/// feeds), but nothing records *which* of them a line named, so for the
/// rationing itself the log is the contract.
#[tokio::test]
async fn t49_a_saturated_dedicated_mislabel_budget_still_names_a_patch_failure() {
    // Enough to exhaust either budget, so the test does not depend on which is
    // larger.
    let mislabeled = LABEL_FAILURE_LOGGED_NODES + DEDICATED_MISLABEL_LOGGED_NODES;
    let mut nodes: Vec<WorkerNode> = (0..mislabeled)
        .map(|index| {
            dedicated_worker(
                &format!("ip-dedicated-{index}.ec2.internal"),
                Some("workers"),
            )
        })
        .collect();
    nodes.push(worker("ip-bad.ec2.internal", None));
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(nodes));
    labels.fail_label("ip-bad.ec2.internal");

    let layer = CapturedEventsLayer::new();
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    let report = reconciler(sink, labels).tick().await;
    drop(guard);

    assert_eq!(report.skipped_dedicated, mislabeled);
    assert_eq!(
        report.label_errors, 1,
        "the one shared worker's patch failed"
    );

    let captured = events.events();
    let lines = |needle: &str| {
        captured
            .iter()
            .filter(|event| {
                field_value(event, "message").is_some_and(|message| message.contains(needle))
            })
            .count()
    };
    assert_eq!(
        lines("dedicated-tainted node carries the DEFAULT"),
        DEDICATED_MISLABEL_LOGGED_NODES,
        "the mislabel warn is capped by its own budget: {captured:?}"
    );
    assert_eq!(
        lines("label patch failed"),
        1,
        "the patch failure must still be named — the mislabel warn must not be \
         able to spend the failure budget: {captured:?}"
    );
}

/// A write cut short by the tick budget takes the `info` arm and leaves the
/// failure budget untouched.
///
/// `t38` pins the clamp; this pins how the clamped case is *reported*. Dropping
/// the `truncated_by_tick` distinction sends every such write down the `warn`
/// arm, which draws on `LABEL_FAILURE_LOGGED_NODES` — and on any tick shorter
/// than `LABEL_WRITE_TIMEOUT` that is the common case, so the budget is spent by
/// writes that were never slow, silencing the warns that carry a real failure's
/// cause.
#[tokio::test(start_paused = true)]
async fn t50_a_tick_clamped_write_does_not_draw_on_the_failure_budget() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(vec![worker(
        "ip-a.ec2.internal",
        None,
    )]));
    labels.stall_label("ip-a.ec2.internal");
    let reconciler = reconciler_with_config(
        ReconcilerConfig {
            enabled: true,
            // Shorter than one write's own ceiling, so the clamp binds.
            tick: PositiveDuration::new_panic(LABEL_WRITE_TIMEOUT / 2),
            write_concurrency: 1,
            ..ReconcilerConfig::default()
        },
        sink,
        labels,
    );

    let layer = CapturedEventsLayer::new();
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    let report = reconciler.tick().await;
    drop(guard);

    assert_eq!(report.label_indeterminate, 1);
    let captured = events.events();
    let lines = |needle: &str| {
        captured
            .iter()
            .filter(|event| {
                field_value(event, "message").is_some_and(|message| message.contains(needle))
            })
            .count()
    };
    assert_eq!(
        lines("cut short by the tick budget"),
        1,
        "the tick budget, not apiserver slowness, bounded this write: {captured:?}"
    );
    assert_eq!(
        lines("label patch timed out"),
        0,
        "and it must not be reported as a per-write timeout, which would spend a \
         failure-budget slot: {captured:?}"
    );
}

/// Dedicated-tainted nodes carrying the DEFAULT label get their own gauge, and
/// it carries the whole count rather than the handful the warn names.
///
/// `t10` pins that such a node is left alone and `t49` pins that its warn is
/// rationed to [`DEDICATED_MISLABEL_LOGGED_NODES`] names a tick. Neither covers
/// the fleet-wide view: with more of them than that budget, the log names five
/// and every dashboard would otherwise show nothing —
/// `dedicated_skipped_nodes` counts *every* tainted node, so it cannot separate
/// the mislabeled ones out.
///
/// Asserted on the **emitted gauge value**, not on `TickReport`. A report-field
/// assertion passes whether the gauge is published inside the `completed` gate,
/// published unconditionally, or never published at all — and the first of those
/// is the property that keeps a "sustained > 0" monitor alive across an
/// early-returning tick (`t33` pins that half).
#[tokio::test]
async fn t51_mislabeled_dedicated_nodes_are_published_as_a_gauge() {
    // More than the warn's budget: the point of the gauge is the ones no line
    // names.
    let mislabeled = DEDICATED_MISLABEL_LOGGED_NODES + 3;
    let mut nodes: Vec<WorkerNode> = (0..mislabeled)
        .map(|index| {
            dedicated_worker(
                &format!("ip-dedicated-{index}.ec2.internal"),
                Some("workers"),
            )
        })
        .collect();
    // A dedicated node with no label at all, and a shared worker: neither is
    // mislabeled, so neither may count here.
    nodes.push(dedicated_worker("ip-dedicated-clean.ec2.internal", None));
    nodes.push(worker("ip-shared.ec2.internal", None));
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(nodes));
    let report = reconciler(sink, labels).tick().await;

    assert!(
        report.completed,
        "the gauges are only published on a measured tick"
    );
    let emitted = pplx_space_metrics::test_capture::capture_values(|| publish_metrics(&report));
    let value = |name: &str| {
        emitted
            .iter()
            .find(|value| value.name == name)
            .unwrap_or_else(|| panic!("{name} is published by a measured tick: {emitted:?}"))
            .value
    };
    assert!(
        (value("node_group_labeler.dedicated_mislabeled_nodes") - mislabeled as f64).abs()
            < f64::EPSILON,
        "the gauge must carry every mislabeled node, not the {DEDICATED_MISLABEL_LOGGED_NODES} \
         the warn names: {emitted:?}"
    );
    // A subset, never a replacement: the clean dedicated node counts in the
    // wider gauge only.
    assert!(
        (value("node_group_labeler.dedicated_skipped_nodes") - (mislabeled + 1) as f64).abs()
            < f64::EPSILON,
        "dedicated_skipped_nodes still counts every tainted node: {emitted:?}"
    );
}

/// The write-concurrency clamp bounds both ends, so no configured value can
/// point `buffer_unordered` at a width the apiserver has to absorb.
///
/// The pure function only, which is not evidence that either caller still
/// routes through it: `t53` pins the ceiling's effect at the fan-out — the
/// caller that bounds concurrent apiserver writes — and `t13` the floor's
/// effect through a tick. The other caller, `run`'s startup log, only
/// reports the width. Keep all three: this one
/// is the only place the boundaries themselves (`0`, the default, the ceiling
/// exactly) are cheap to enumerate.
#[test]
fn t52_write_concurrency_is_clamped_at_both_ends() {
    // The typo case: a mis-rendered Helm value, or a stray zero.
    assert_eq!(clamp_write_concurrency(5_000), MAX_WRITE_CONCURRENCY);
    assert_eq!(
        clamp_write_concurrency(usize::MAX),
        MAX_WRITE_CONCURRENCY,
        "no value may exceed the ceiling"
    );
    assert_eq!(clamp_write_concurrency(0), 1, "a zero would wedge the tick");
    // Everything in range passes through untouched, the default included —
    // a clamp that moved the ordinary value would be a behavior change.
    assert_eq!(
        clamp_write_concurrency(DEFAULT_WRITE_CONCURRENCY),
        DEFAULT_WRITE_CONCURRENCY
    );
    assert_eq!(
        clamp_write_concurrency(MAX_WRITE_CONCURRENCY),
        MAX_WRITE_CONCURRENCY,
        "the ceiling itself is a legal value"
    );
}

/// The ceiling bounds the *fan-out*, not merely the configured number: an
/// over-large `write_concurrency` cannot put more than [`MAX_WRITE_CONCURRENCY`]
/// Node patches in flight at once.
///
/// This is the assertion the hazard actually needs. `t52` pins
/// [`clamp_write_concurrency`] as a function, which stays green if the fan-out
/// stops calling it — and the fan-out is where a mis-rendered
/// write concurrency of 5,000 would reach the apiserver.
/// So the peak overlap is measured through a real tick, at a configured width
/// one above the ceiling, with more candidates than the ceiling admits.
#[tokio::test(start_paused = true)]
async fn t53_write_concurrency_ceiling_bounds_the_patch_fan_out() {
    let sink = std::sync::Arc::new(FakeSink::with_group("workers"));
    // One more candidate than the ceiling: enough to saturate a clamped
    // fan-out, and enough that an unclamped one is observably wider.
    let fleet: Vec<WorkerNode> = (0..=MAX_WRITE_CONCURRENCY)
        .map(|n| worker(&format!("ip-{n:03}.ec2.internal"), None))
        .collect();
    let labels = std::sync::Arc::new(FakeLabels::with_nodes(fleet));
    // Well under the tick budget even multiplied by the number of batches, and
    // the paused clock auto-advances past it.
    labels.hold_each_patch(Duration::from_millis(1));

    let reconciler = reconciler_with_config(
        ReconcilerConfig {
            enabled: true,
            write_concurrency: MAX_WRITE_CONCURRENCY + 1,
            ..ReconcilerConfig::default()
        },
        sink,
        labels.clone(),
    );
    let report = reconciler.tick().await;

    assert_eq!(
        labels.peak_in_flight(),
        MAX_WRITE_CONCURRENCY,
        "the fan-out must saturate the ceiling and never exceed it"
    );
    // The clamp costs nothing but a second batch: every candidate is still
    // labeled inside the one tick.
    assert_eq!(report.labeled, MAX_WRITE_CONCURRENCY + 1);
    assert_eq!(report.label_errors, 0);
    assert_eq!(report.label_deferred, 0);
}
