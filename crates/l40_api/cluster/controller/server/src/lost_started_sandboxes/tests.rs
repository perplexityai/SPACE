//! # Test matrix
//!
//! ## C - `ReconcilerConfig`
//! - C1 `c1_default_is_disabled`
//! - C2 `c2_enabled_env_var_turns_it_on`
//! - C3 `c3_census_is_on_by_default_and_has_a_kill_switch`
//!
//! ## T - Tick behavior (fake sink and lookup)
//! - T1 `t1_terminated_and_deleted_sources_are_marked_lost`
//! - T2 `t2_live_unverifiable_and_lookup_failures_are_left_alone`
//! - T3 `t3_one_lookup_per_node_per_tick`
//! - T4 `t4_per_tick_cap_rate_limits_the_rest`
//! - T5 `t5_refused_and_failed_flips_are_counted`
//! - T6 `t6_cursor_advances_on_a_full_page_and_wraps_on_a_short_one`
//! - T7 `t7_node_in_the_reflector_is_live_without_an_ec2_call`
//! - T8 `t8_node_the_api_server_still_has_is_live`
//! - T9 `t9_cold_or_empty_reflector_defers_registered_sources_only`
//! - T10 `t10_failed_direct_node_lookup_defers`
//! - T11 `t11_run_stops_mid_tick_when_cancelled`
//! - T12 `t12_absence_must_last_the_grace_and_restarts_on_return`
//!
//! ## S - Stranded-sandbox census (fake sink and lookup)
//! - S1 `s1_census_separates_gone_from_unverifiable`
//! - S2 `s2_live_ready_nodes_and_empty_counts_are_not_reported`
//! - S3 `s3_not_ready_must_last_the_grace`
//! - S4 `s4_disabled_give_up_still_runs_the_census_and_flips_nothing`
//! - S5 `s5_census_lookups_are_capped_per_tick`
//! - S6 `s6_cold_reflector_skips_the_census`
//! - S7 `s7_census_reuses_the_give_up_verdict`
//! - S8 `s8_census_lookups_resume_after_the_last_judged_node`
//! - S9 `s9_deleted_node_at_rest_counts_only_within_the_window`
//!
//! ## P - Postgres-backed sink
//! - P1 `p1_stale_snapshot_and_terminated_source_ends_lost`
//! - P2 `p2_live_source_node_stays_retryable`
//! - P3 `p3_deleted_source_node_row_ends_lost`
//! - P4 `p4_snapshot_covering_activity_is_not_listed`
//! - P5 `p5_inflight_upload_vetoes_until_it_is_provably_dead`
//! - P6 `p6_rearmed_execution_refuses_the_flip`
//! - P7 `p7_replacement_node_uid_refuses_the_flip`
//! - P8 `p8_draining_source_node_is_left_to_the_drain`
//! - P9 `p9_only_recovery_failed_started_kubernetes_rows_are_listed`
//! - P10 `p10_flip_rechecks_restore_sources_and_generation`
//! - P11 `p11_flip_rechecks_placement_drain_lifecycle_and_substrate`
//! - P12 `p12_census_counts_running_and_at_rest_residents`
//! - P13 `p13_old_deleted_node_with_a_durable_suspend_is_not_a_suspect`
//! - P14 `p14_recently_deleted_node_with_a_local_only_suspend_counts_at_rest`

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};

use crate::orphaned_nodes::InstanceLookupError;

use super::*;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Default)]
struct FakeLookup {
    verdicts: StdMutex<HashMap<String, Result<InstanceLookupOutcome, String>>>,
    calls: StdMutex<Vec<String>>,
}

impl FakeLookup {
    fn with(self, name: &str, verdict: Result<InstanceLookupOutcome, String>) -> Self {
        self.verdicts
            .lock()
            .unwrap()
            .insert(name.to_string(), verdict);
        self
    }

    fn live(self, name: &str) -> Self {
        self.with(
            name,
            Ok(InstanceLookupOutcome::Live {
                instance_id: "i-live".to_string(),
                state: "running".to_string(),
            }),
        )
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl InstanceLookup for Arc<FakeLookup> {
    async fn lookup_by_private_dns(
        &self,
        private_dns_name: String,
    ) -> Result<InstanceLookupOutcome, InstanceLookupError> {
        self.calls.lock().unwrap().push(private_dns_name.clone());
        match self
            .verdicts
            .lock()
            .unwrap()
            .get(&private_dns_name)
            .cloned()
        {
            Some(Ok(outcome)) => Ok(outcome),
            Some(Err(message)) => Err(InstanceLookupError::Backend(message)),
            None => Ok(InstanceLookupOutcome::Gone),
        }
    }
}

/// A warm reflector that lists only `listed`, and an API server that has only
/// `existing`.
struct FakePresence {
    live: StdMutex<Option<HashSet<String>>>,
    /// Listed names whose `Ready` condition isn't `True`.
    not_ready: StdMutex<HashSet<String>>,
    existing: HashSet<String>,
    /// Names whose direct GET errors.
    failing: HashSet<String>,
    direct_calls: StdMutex<Vec<String>>,
}

impl FakePresence {
    fn listing(names: &[&str]) -> Self {
        Self {
            live: StdMutex::new(Some(names.iter().map(|name| (*name).to_string()).collect())),
            not_ready: StdMutex::new(HashSet::new()),
            existing: HashSet::new(),
            failing: HashSet::new(),
            direct_calls: StdMutex::new(Vec::new()),
        }
    }

    fn not_ready(self, name: &str) -> Self {
        self.not_ready.lock().unwrap().insert(name.to_string());
        self
    }

    fn existing(mut self, name: &str) -> Self {
        self.existing.insert(name.to_string());
        self
    }

    fn failing(mut self, name: &str) -> Self {
        self.failing.insert(name.to_string());
        self
    }

    fn cold() -> Self {
        let presence = Self::listing(&[]);
        *presence.live.lock().unwrap() = None;
        presence
    }

    fn set_listing(&self, names: &[&str]) {
        *self.live.lock().unwrap() = Some(names.iter().map(|name| (*name).to_string()).collect());
    }

    fn direct_calls(&self) -> Vec<String> {
        self.direct_calls.lock().unwrap().clone()
    }
}

/// The reflector lists some other node, so it reads as warm.
fn presence() -> Arc<FakePresence> {
    Arc::new(FakePresence::listing(&["some-other-node"]))
}

#[derive(Debug, thiserror::Error)]
#[error("fake node lookup error")]
struct FakeNodeLookupError;

impl KubeNodePresence for Arc<FakePresence> {
    type LookupError = FakeNodeLookupError;

    fn live_node_names(&self) -> Option<HashSet<String>> {
        self.live.lock().unwrap().clone()
    }

    fn not_ready_node_names(&self) -> Option<HashSet<String>> {
        self.live
            .lock()
            .unwrap()
            .as_ref()
            .map(|_| self.not_ready.lock().unwrap().clone())
    }

    async fn node_exists(&self, node_name: String) -> Result<bool, FakeNodeLookupError> {
        self.direct_calls.lock().unwrap().push(node_name.clone());
        if self.failing.contains(&node_name) {
            return Err(FakeNodeLookupError);
        }
        Ok(self.existing.contains(&node_name))
    }
}

#[derive(Default)]
struct FakeSinkState {
    pages: Vec<Vec<LostCandidate>>,
    list_calls: Vec<Option<SandboxId>>,
    mark_calls: Vec<SandboxId>,
    /// `None` means the flip errors.
    mark_result: Option<bool>,
    residents: Vec<NodeResidents>,
    stranded: HashMap<NodeId, StrandedCounts>,
    count_calls: Vec<NodeId>,
}

struct FakeSink(StdMutex<FakeSinkState>);

impl FakeSink {
    fn new(pages: Vec<Vec<LostCandidate>>) -> Arc<Self> {
        Arc::new(Self(StdMutex::new(FakeSinkState {
            pages,
            mark_result: Some(true),
            ..FakeSinkState::default()
        })))
    }

    /// One resident row for `node_id`, with `counts` from the second pass.
    fn with_node(&self, node_id: NodeId, source_node: SourceNode, counts: StrandedCounts) {
        let mut state = self.0.lock().unwrap();
        state.residents.push(NodeResidents {
            node_id,
            source_node,
            residents: counts.running + counts.at_rest_without_copy,
            recent_residents: counts.running + counts.at_rest_without_copy,
        });
        state.stranded.insert(node_id, counts);
    }

    /// Like [`Self::with_node`], but every resident is a suspend or upload
    /// older than [`DELETED_NODE_AT_REST_WINDOW`].
    fn with_old_node(&self, node_id: NodeId, source_node: SourceNode, counts: StrandedCounts) {
        self.with_node(node_id, source_node, counts);
        let mut state = self.0.lock().unwrap();
        state.residents.last_mut().unwrap().recent_residents = 0;
    }

    fn set_source_node(&self, node_id: NodeId, source_node: SourceNode) {
        let mut state = self.0.lock().unwrap();
        for row in &mut state.residents {
            if row.node_id == node_id {
                row.source_node = source_node.clone();
            }
        }
    }

    fn clear_count_calls(&self) {
        self.0.lock().unwrap().count_calls.clear();
    }

    fn count_calls(&self) -> Vec<NodeId> {
        self.0.lock().unwrap().count_calls.clone()
    }

    fn list_calls(&self) -> usize {
        self.0.lock().unwrap().list_calls.len()
    }

    fn mark_calls(&self) -> Vec<SandboxId> {
        self.0.lock().unwrap().mark_calls.clone()
    }
}

impl LostStartedSink for Arc<FakeSink> {
    async fn list_candidates(
        &self,
        _limit: u32,
        after: Option<SandboxId>,
    ) -> Result<Vec<LostCandidate>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.list_calls.push(after);
        if state.pages.is_empty() {
            return Ok(Vec::new());
        }
        Ok(state.pages.remove(0))
    }

    async fn mark_lost(&self, candidate: &LostCandidate) -> Result<bool, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.mark_calls.push(candidate.sandbox_id);
        state.mark_result.ok_or(sqlx::Error::PoolTimedOut)
    }

    async fn node_residents(&self) -> Result<Vec<NodeResidents>, sqlx::Error> {
        Ok(self.0.lock().unwrap().residents.clone())
    }

    async fn stranded_counts(
        &self,
        node_ids: &[NodeId],
    ) -> Result<HashMap<NodeId, StrandedCounts>, sqlx::Error> {
        let mut state = self.0.lock().unwrap();
        state.count_calls.extend_from_slice(node_ids);
        Ok(node_ids
            .iter()
            .filter_map(|node_id| {
                state
                    .stranded
                    .get(node_id)
                    .map(|counts| (*node_id, *counts))
            })
            .collect())
    }
}

fn registered(name: &str, uid: Option<&str>) -> SourceNode {
    SourceNode::Registered {
        k8s_node_name: name.to_string(),
        k8s_node_uid: uid.map(str::to_string),
    }
}

fn candidate(source_node: SourceNode) -> LostCandidate {
    LostCandidate {
        sandbox_id: SandboxId::new(),
        generation: 7,
        source_node_id: NodeId::new(),
        source_node,
    }
}

/// No absence grace, so a registered node absent from the reflector is checked
/// on its first tick; T12 covers the grace.
fn config() -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        absence_grace: Duration::ZERO,
        ..ReconcilerConfig::default()
    }
}

// -----------------------------------------------------------------
// C — config
// -----------------------------------------------------------------

#[test]
fn c1_default_is_disabled() {
    let env = pplx_space_test_env::TestEnv::default();
    let config = ReconcilerConfig::from_environment(&env);
    assert!(!config.enabled);
    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    assert_eq!(config.max_lost_per_tick, DEFAULT_MAX_LOST_PER_TICK);
}

#[test]
fn c2_enabled_env_var_turns_it_on() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_LOST_STARTED_SANDBOXES_ENABLED", "true");
    assert!(ReconcilerConfig::from_environment(&env).enabled);
}

#[test]
fn c3_census_is_on_by_default_and_has_a_kill_switch() {
    let env = pplx_space_test_env::TestEnv::default();
    let config = ReconcilerConfig::from_environment(&env);
    assert!(config.census_enabled);
    assert_eq!(
        config.max_census_lookups_per_tick,
        DEFAULT_MAX_CENSUS_LOOKUPS_PER_TICK
    );

    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_STRANDED_SANDBOX_CENSUS_ENABLED", "false");
    assert!(!ReconcilerConfig::from_environment(&env).census_enabled);
}

// -----------------------------------------------------------------
// T — tick behavior
// -----------------------------------------------------------------

#[tokio::test]
async fn t1_terminated_and_deleted_sources_are_marked_lost() {
    let terminated = candidate(registered("ip-10-0-0-1.ec2.internal", Some("uid-1")));
    let deleted = candidate(SourceNode::RowDeleted);
    let sink = FakeSink::new(vec![vec![terminated.clone(), deleted.clone()]]);
    let lookup = Arc::new(FakeLookup::default());

    let report = Reconciler::new(config(), sink.clone(), lookup.clone(), presence())
        .tick()
        .await;

    assert_eq!(report.scanned, 2);
    assert_eq!(report.marked_lost, 2);
    assert_eq!(
        sink.mark_calls(),
        vec![terminated.sandbox_id, deleted.sandbox_id]
    );
    // A deleted row needs no EC2 lookup.
    assert_eq!(lookup.calls(), vec!["ip-10-0-0-1.ec2.internal".to_string()]);
}

#[tokio::test]
async fn t2_live_unverifiable_and_lookup_failures_are_left_alone() {
    let sink = FakeSink::new(vec![vec![
        candidate(registered("live", None)),
        candidate(SourceNode::Unverifiable),
        candidate(registered("flaky", None)),
    ]]);
    let lookup = Arc::new(
        FakeLookup::default()
            .live("live")
            .with("flaky", Err("throttled".to_string())),
    );

    let report = Reconciler::new(config(), sink.clone(), lookup.clone(), presence())
        .tick()
        .await;

    assert_eq!(report.source_live, 1);
    assert_eq!(report.unverifiable, 1);
    assert_eq!(report.lookup_errors, 1);
    assert_eq!(report.marked_lost, 0);
    assert!(sink.mark_calls().is_empty());
}

#[tokio::test]
async fn t3_one_lookup_per_node_per_tick() {
    let sink = FakeSink::new(vec![vec![
        candidate(registered("dead", None)),
        candidate(registered("dead", None)),
        candidate(registered("dead", None)),
    ]]);
    let lookup = Arc::new(FakeLookup::default());

    let report = Reconciler::new(config(), sink.clone(), lookup.clone(), presence())
        .tick()
        .await;

    assert_eq!(report.marked_lost, 3);
    assert_eq!(lookup.calls(), vec!["dead".to_string()]);
}

#[tokio::test]
async fn t4_per_tick_cap_rate_limits_the_rest() {
    let sink = FakeSink::new(vec![
        (0..4).map(|_| candidate(SourceNode::RowDeleted)).collect(),
    ]);
    let lookup = Arc::new(FakeLookup::default());
    let config = ReconcilerConfig {
        max_lost_per_tick: 2,
        ..config()
    };

    let report = Reconciler::new(config, sink.clone(), lookup.clone(), presence())
        .tick()
        .await;

    assert_eq!(report.marked_lost, 2);
    assert_eq!(report.rate_limited, 2);
    assert_eq!(sink.mark_calls().len(), 2);
}

#[tokio::test]
async fn t5_refused_and_failed_flips_are_counted() {
    let lookup = Arc::new(FakeLookup::default());

    let refused = FakeSink::new(vec![vec![candidate(SourceNode::RowDeleted)]]);
    refused.0.lock().unwrap().mark_result = Some(false);
    let report = Reconciler::new(config(), refused.clone(), lookup.clone(), presence())
        .tick()
        .await;
    assert_eq!((report.marked_lost, report.cas_lost), (0, 1));

    let failing = FakeSink::new(vec![vec![candidate(SourceNode::RowDeleted)]]);
    failing.0.lock().unwrap().mark_result = None;
    let report = Reconciler::new(config(), failing.clone(), lookup.clone(), presence())
        .tick()
        .await;
    assert_eq!((report.marked_lost, report.errors), (0, 1));
}

#[tokio::test]
async fn t6_cursor_advances_on_a_full_page_and_wraps_on_a_short_one() {
    let full: Vec<LostCandidate> = (0..2)
        .map(|_| candidate(registered("live", None)))
        .collect();
    let last = full.last().unwrap().sandbox_id;
    let sink = FakeSink::new(vec![full, vec![candidate(registered("live", None))]]);
    let lookup = Arc::new(FakeLookup::default().live("live"));
    let config = ReconcilerConfig {
        batch_size: 2,
        ..config()
    };
    let mut reconciler = Reconciler::new(config, sink.clone(), lookup.clone(), presence());

    reconciler.tick().await;
    reconciler.tick().await;
    reconciler.tick().await;

    assert_eq!(
        sink.0.lock().unwrap().list_calls,
        vec![None, Some(last), None]
    );
}

#[tokio::test]
async fn t7_node_in_the_reflector_is_live_without_an_ec2_call() {
    let sink = FakeSink::new(vec![vec![candidate(registered("listed", None))]]);
    let lookup = Arc::new(FakeLookup::default());
    let presence = Arc::new(FakePresence::listing(&["listed"]));

    let report = Reconciler::new(config(), sink.clone(), lookup.clone(), presence.clone())
        .tick()
        .await;

    assert_eq!((report.source_live, report.marked_lost), (1, 0));
    assert!(lookup.calls().is_empty());
    assert!(presence.direct_calls().is_empty());
}

#[tokio::test]
async fn t8_node_the_api_server_still_has_is_live() {
    // EC2 answers gone (no match), the reflector misses it, but the API server
    // still has the Node: an EC2 lookup against the wrong account looks like
    // this, and the sandbox must be left alone.
    let sink = FakeSink::new(vec![vec![candidate(registered("lagging", None))]]);
    let lookup = Arc::new(FakeLookup::default());
    let presence = Arc::new(FakePresence::listing(&["some-other-node"]).existing("lagging"));

    let report = Reconciler::new(config(), sink.clone(), lookup.clone(), presence.clone())
        .tick()
        .await;

    assert_eq!((report.source_live, report.marked_lost), (1, 0));
    assert_eq!(presence.direct_calls(), vec!["lagging".to_string()]);
    assert!(sink.mark_calls().is_empty());
}

#[tokio::test]
async fn t9_cold_or_empty_reflector_defers_registered_sources_only() {
    for presence in [FakePresence::cold(), FakePresence::listing(&[])] {
        let deleted = candidate(SourceNode::RowDeleted);
        let sink = FakeSink::new(vec![vec![
            candidate(registered("dead", None)),
            deleted.clone(),
        ]]);
        let lookup = Arc::new(FakeLookup::default());

        let report = Reconciler::new(config(), sink.clone(), lookup.clone(), Arc::new(presence))
            .tick()
            .await;

        assert_eq!((report.unverifiable, report.marked_lost), (1, 1));
        assert_eq!(sink.mark_calls(), vec![deleted.sandbox_id]);
        assert!(lookup.calls().is_empty());
    }
}

#[tokio::test]
async fn t10_failed_direct_node_lookup_defers() {
    let sink = FakeSink::new(vec![vec![candidate(registered("flaky", None))]]);
    let lookup = Arc::new(FakeLookup::default());
    let presence = Arc::new(FakePresence::listing(&["some-other-node"]).failing("flaky"));

    let report = Reconciler::new(config(), sink.clone(), lookup.clone(), presence.clone())
        .tick()
        .await;

    assert_eq!((report.lookup_errors, report.marked_lost), (1, 0));
    assert_eq!(presence.direct_calls(), vec!["flaky".to_string()]);
    assert!(sink.mark_calls().is_empty());
}

/// A sink whose scan never returns, standing in for a tick stuck on slow
/// lookups.
struct HangingSink {
    scans: Arc<StdMutex<u32>>,
}

impl LostStartedSink for HangingSink {
    async fn list_candidates(
        &self,
        _limit: u32,
        _after: Option<SandboxId>,
    ) -> Result<Vec<LostCandidate>, sqlx::Error> {
        *self.scans.lock().unwrap() += 1;
        std::future::pending().await
    }

    async fn mark_lost(&self, _candidate: &LostCandidate) -> Result<bool, sqlx::Error> {
        unreachable!("the scan never returns")
    }

    async fn node_residents(&self) -> Result<Vec<NodeResidents>, sqlx::Error> {
        unreachable!("the scan never returns")
    }

    async fn stranded_counts(
        &self,
        _node_ids: &[NodeId],
    ) -> Result<HashMap<NodeId, StrandedCounts>, sqlx::Error> {
        unreachable!("the scan never returns")
    }
}

#[tokio::test]
async fn t11_run_stops_mid_tick_when_cancelled() {
    let scans = Arc::new(StdMutex::new(0));
    let token = CancellationToken::new();
    let config = ReconcilerConfig {
        tick: Duration::from_millis(10),
        ..config()
    };
    let handle = tokio::spawn(run(
        config,
        HangingSink {
            scans: scans.clone(),
        },
        Arc::new(FakeLookup::default()),
        presence(),
        token.clone(),
    ));
    while *scans.lock().unwrap() == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    token.cancel();

    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("run returns while its tick is in flight")
        .expect("run doesn't panic");
}

#[tokio::test(start_paused = true)]
async fn t12_absence_must_last_the_grace_and_restarts_on_return() {
    let stuck = candidate(registered("dead", None));
    let pages: Vec<Vec<LostCandidate>> = (0..4).map(|_| vec![stuck.clone()]).collect();
    let sink = FakeSink::new(pages);
    let lookup = Arc::new(FakeLookup::default());
    let presence = Arc::new(FakePresence::listing(&["some-other-node"]));
    let config = ReconcilerConfig {
        absence_grace: Duration::from_secs(600),
        ..config()
    };
    let mut reconciler = Reconciler::new(config, sink.clone(), lookup.clone(), presence.clone());

    // First seen absent: too fresh to ask EC2 about.
    let report = reconciler.tick().await;
    assert_eq!((report.unverifiable, report.marked_lost), (1, 0));

    // Listed again before the grace ran out: the absence clock restarts.
    tokio::time::advance(Duration::from_secs(400)).await;
    presence.set_listing(&["dead", "some-other-node"]);
    assert_eq!(reconciler.tick().await.source_live, 1);
    presence.set_listing(&["some-other-node"]);
    tokio::time::advance(Duration::from_secs(400)).await;
    assert_eq!(reconciler.tick().await.unverifiable, 1);
    assert!(lookup.calls().is_empty());

    // Absent for the whole grace: EC2 and the API server are asked, and it flips.
    tokio::time::advance(Duration::from_secs(600)).await;
    let report = reconciler.tick().await;
    assert_eq!(report.marked_lost, 1);
    assert_eq!(lookup.calls(), vec!["dead".to_string()]);
    assert_eq!(sink.mark_calls(), vec![stuck.sandbox_id]);
}

// -----------------------------------------------------------------
// P — Postgres-backed sink
// -----------------------------------------------------------------

mod postgres_backed {
    use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
    use pplx_space_sandbox_residency::ResidencyEvidence;
    use pplx_space_snapshot_model::SnapshotId;
    use pplx_space_sql_locator::sql;
    use pplx_space_test_env::TestResult;
    use sqlx::PgPool;

    use super::*;
    use crate::lifecycle_writer_test_support::{ExecutionSeed, insert_execution};
    use crate::lost_started_sandboxes::store::LostStartedDbStore;

    const NODE_NAME: &str = "ip-10-9-8-7.ec2.internal";
    const NODE_UID: &str = "node-uid-1";

    struct Seeded {
        sandbox_id: SandboxId,
        node_id: NodeId,
    }

    /// An active `recovery_failed` execution at `generation` that tracks no
    /// pod: the shape a failed started recovery leaves when it never got a
    /// restore source to start one.
    async fn insert_failed_recovery(
        pool: &PgPool,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> TestResult {
        insert_execution(
            pool,
            sandbox_id,
            ExecutionSeed {
                generation,
                state: "recovery_failed",
                active: true,
            },
        )
        .await?;
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET pod_uid = NULL
              WHERE sandbox_id = $1 AND generation = $2"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_node(pool: &PgPool, node_id: NodeId, name: &str) -> TestResult {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by)
             VALUES ($1, '10.9.8.7'::INET, 'worker', 'k8s')"
        ))
        .bind(node_id)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
        ))
        .bind(node_id)
        .bind(name)
        .bind(NODE_UID)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// The stuck shape: a `started` sandbox placed on `node_id`, its active
    /// execution `recovery_failed`, and a ready full snapshot taken an hour
    /// before its last command.
    async fn seed_stuck(pool: &PgPool, with_node_row: bool) -> TestResult<Seeded> {
        let sandbox_id = SandboxId::new();
        let node_id = NodeId::new();
        if with_node_row {
            insert_node(pool, node_id, NODE_NAME).await?;
        }
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_generation,
                 last_command_at, last_access_at)
             VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'started', 'on_node',
                     $4, $4, 1, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour')"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(node_id)
        .execute(pool)
        .await?;
        insert_failed_recovery(pool, sandbox_id, 7).await?;
        insert_snapshot(pool, sandbox_id, "ready", "2 hours", None).await?;
        Ok(Seeded {
            sandbox_id,
            node_id,
        })
    }

    async fn insert_snapshot(
        pool: &PgPool,
        sandbox_id: SandboxId,
        state: &str,
        age: &str,
        source_node_id: Option<NodeId>,
    ) -> TestResult<SnapshotId> {
        let snapshot_id = SnapshotId::new();
        sqlx::query(sql!(
            "INSERT INTO snapshots
                (id, sandbox_id, org_id, type, state, timestamp, source_node_id,
                 local_artifact_id, artifact_kind, artifact_origin)
             VALUES ($1, $2, $3::uuid, 'full', $4, NOW() - $5::INTERVAL, $6,
                     $7, 'full', 'suspend')"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(SEED_ORG_ID)
        .bind(state)
        .bind(age)
        .bind(source_node_id)
        .bind(uuid::Uuid::now_v7())
        .execute(pool)
        .await?;
        if state == "ready" {
            sqlx::query(sql!(
                "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
                 VALUES ($1, 's3://bucket/snapshot')"
            ))
            .bind(snapshot_id)
            .execute(pool)
            .await?;
        }
        Ok(snapshot_id)
    }

    async fn listed(store: &LostStartedDbStore) -> TestResult<Vec<LostCandidate>> {
        Ok(store.list_candidates(100, None).await?)
    }

    async fn sandbox_state(
        pool: &PgPool,
        sandbox_id: SandboxId,
    ) -> TestResult<(String, Option<NodeId>, i64)> {
        Ok(sqlx::query_as(sql!(
            "SELECT s.lifecycle,
                    s.active_node_id,
                    (SELECT COUNT(*) FROM sandbox_k8s_executions e
                      WHERE e.sandbox_id = s.id AND e.active)
               FROM sandboxes s
              WHERE s.id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p1_stale_snapshot_and_terminated_source_ends_lost() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        let lookup = Arc::new(FakeLookup::default());

        let report = Reconciler::new(config(), store, lookup.clone(), presence())
            .tick()
            .await;

        assert_eq!(report.marked_lost, 1);
        assert_eq!(lookup.calls(), vec![NODE_NAME.to_string()]);
        assert_eq!(
            sandbox_state(&db.pool, seeded.sandbox_id).await?,
            ("failed".to_string(), None, 0)
        );
        let (state, active, terminated): (String, bool, bool) = sqlx::query_as(sql!(
            "SELECT state, active, terminated_at IS NOT NULL
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = 7"
        ))
        .bind(seeded.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            (state.as_str(), active, terminated),
            ("forgotten", false, true)
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p2_live_source_node_stays_retryable() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        let lookup = Arc::new(FakeLookup::default().live(NODE_NAME));

        let report = Reconciler::new(config(), store, lookup.clone(), presence())
            .tick()
            .await;

        assert_eq!((report.source_live, report.marked_lost), (1, 0));
        assert_eq!(
            sandbox_state(&db.pool, seeded.sandbox_id).await?,
            ("started".to_string(), Some(seeded.node_id), 1)
        );
        let state: String = sqlx::query_scalar(sql!(
            "SELECT state FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
        ))
        .bind(seeded.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(
            state, "recovery_failed",
            "the next wake can still re-arm it"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p3_deleted_source_node_row_ends_lost() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, false).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        let candidates = listed(&store).await?;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].source_node, SourceNode::RowDeleted);

        assert!(store.mark_lost(&candidates[0]).await?);
        assert_eq!(
            sandbox_state(&db.pool, seeded.sandbox_id).await?,
            ("failed".to_string(), None, 0)
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p4_snapshot_covering_activity_is_not_listed() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        assert_eq!(listed(&store).await?.len(), 1);

        // Newer than the last command and access: restoring it rolls nothing
        // back, so started recovery may still use it.
        insert_snapshot(&db.pool, seeded.sandbox_id, "ready", "1 minute", None).await?;
        assert!(listed(&store).await?.is_empty());

        // With no recorded activity, any ready snapshot covers it.
        let never_active = seed_stuck(&db.pool, false).await?;
        assert_eq!(listed(&store).await?.len(), 1);
        sqlx::query(sql!(
            "UPDATE sandboxes SET last_command_at = NULL, last_access_at = NULL WHERE id = $1"
        ))
        .bind(never_active.sandbox_id)
        .execute(&db.pool)
        .await?;
        assert!(listed(&store).await?.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p5_inflight_upload_vetoes_until_it_is_provably_dead() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);

        // From another node: it may still finalize.
        let other_node = insert_snapshot(
            &db.pool,
            seeded.sandbox_id,
            "creating",
            "1 day",
            Some(NodeId::new()),
        )
        .await?;
        assert!(listed(&store).await?.is_empty());
        sqlx::query(sql!("DELETE FROM snapshots WHERE id = $1"))
            .bind(other_node)
            .execute(&db.pool)
            .await?;

        // From the gone node, inside the settle window: a finalize may be in
        // flight.
        let recent = insert_snapshot(
            &db.pool,
            seeded.sandbox_id,
            "creating",
            "1 minute",
            Some(seeded.node_id),
        )
        .await?;
        assert!(listed(&store).await?.is_empty());
        sqlx::query(sql!("DELETE FROM snapshots WHERE id = $1"))
            .bind(recent)
            .execute(&db.pool)
            .await?;

        // From the gone node, quiet past the window: dead, and finalized
        // `failed` with the flip.
        let dead = insert_snapshot(
            &db.pool,
            seeded.sandbox_id,
            "creating",
            "1 hour",
            Some(seeded.node_id),
        )
        .await?;
        let candidates = listed(&store).await?;
        assert_eq!(candidates.len(), 1);
        assert!(store.mark_lost(&candidates[0]).await?);
        let state: String = sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id = $1"))
            .bind(dead)
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(state, "failed");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p6_rearmed_execution_refuses_the_flip() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        let candidate = listed(&store).await?.remove(0);

        // A wake re-armed the execution after the scan.
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET state = 'recovering'
              WHERE sandbox_id = $1 AND active"
        ))
        .bind(seeded.sandbox_id)
        .execute(&db.pool)
        .await?;

        assert!(!store.mark_lost(&candidate).await?);
        assert_eq!(
            sandbox_state(&db.pool, seeded.sandbox_id).await?.0,
            "started"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p7_replacement_node_uid_refuses_the_flip() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        let candidate = listed(&store).await?.remove(0);

        // A same-name replacement registered after the EC2 lookup.
        sqlx::query(sql!(
            "UPDATE node_k8s SET k8s_node_uid = 'replacement-uid' WHERE node_id = $1"
        ))
        .bind(seeded.node_id)
        .execute(&db.pool)
        .await?;

        assert!(!store.mark_lost(&candidate).await?);

        // And a node row that reappeared refuses a deleted-row candidate.
        let deleted_row = LostCandidate {
            source_node: SourceNode::RowDeleted,
            ..candidate.clone()
        };
        assert!(!store.mark_lost(&deleted_row).await?);
        assert_eq!(
            sandbox_state(&db.pool, seeded.sandbox_id).await?.0,
            "started"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p8_draining_source_node_is_left_to_the_drain() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        sqlx::query(sql!(
            "UPDATE nodes SET drain_phase = 'draining' WHERE id = $1"
        ))
        .bind(seeded.node_id)
        .execute(&db.pool)
        .await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        assert!(listed(&store).await?.is_empty());
        Ok(())
    }

    async fn census_counts(
        store: &LostStartedDbStore,
        node_id: NodeId,
    ) -> TestResult<StrandedCounts> {
        Ok(store
            .stranded_counts(&[node_id])
            .await?
            .get(&node_id)
            .copied()
            .unwrap_or_default())
    }

    /// A completed suspend on `node_id`: `active_node_id` cleared, the source
    /// kept in `target_node_id`, and the only copy the local artifact the
    /// returned id names, with no snapshot uploaded.
    async fn seed_suspended_local_only(
        pool: &PgPool,
        node_id: NodeId,
    ) -> TestResult<(SandboxId, uuid::Uuid)> {
        let sandbox_id = SandboxId::new();
        let artifact_id = uuid::Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_generation,
                 placement_changed_at, current_local_artifact_id,
                 current_local_artifact_kind, current_local_artifact_origin,
                 last_command_at, last_access_at)
             VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'suspended', 'unplaced',
                     NULL, $4, 1, NOW() - INTERVAL '1 hour', $5, 'full', 'suspend',
                     NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour')"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(node_id)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        Ok((sandbox_id, artifact_id))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p12_census_counts_running_and_at_rest_residents() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let registered_node = seed_stuck(&db.pool, true).await?;
        let deleted_row = seed_stuck(&db.pool, false).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);

        let residents = store.node_residents().await?;
        let find = |residents: &[NodeResidents], node_id: NodeId| {
            residents.iter().find(|row| row.node_id == node_id).cloned()
        };
        assert_eq!(
            find(&residents, registered_node.node_id),
            Some(NodeResidents {
                node_id: registered_node.node_id,
                source_node: SourceNode::Registered {
                    k8s_node_name: NODE_NAME.to_string(),
                    k8s_node_uid: None,
                },
                residents: 1,
                recent_residents: 1,
            })
        );
        assert_eq!(
            find(&residents, deleted_row.node_id).map(|row| (row.source_node, row.residents)),
            Some((SourceNode::RowDeleted, 1))
        );
        assert_eq!(
            census_counts(&store, registered_node.node_id).await?,
            counts(1, 0)
        );

        // A suspended sandbox whose only copy is on the node: the suspend
        // cleared active_node_id, so the census must find it through
        // target_node_id.
        let (suspended_id, artifact_id) =
            seed_suspended_local_only(&db.pool, registered_node.node_id).await?;
        let residents = store.node_residents().await?;
        assert_eq!(
            find(&residents, registered_node.node_id).map(|row| row.residents),
            Some(2)
        );
        assert_eq!(
            census_counts(&store, registered_node.node_id).await?,
            counts(1, 1)
        );
        // Batched: each node gets its own counts.
        let both = store
            .stranded_counts(&[registered_node.node_id, deleted_row.node_id])
            .await?;
        assert_eq!(
            both.get(&registered_node.node_id).copied(),
            Some(counts(1, 1))
        );
        assert_eq!(both.get(&deleted_row.node_id).copied(), Some(counts(1, 0)));

        // Once the artifact has a durable copy it restores from object
        // storage and no longer counts.
        let snapshot_id = SnapshotId::new();
        sqlx::query(sql!(
            "INSERT INTO snapshots
                (id, sandbox_id, org_id, type, state, timestamp, source_node_id,
                 local_artifact_id, artifact_kind, artifact_origin)
             VALUES ($1, $2, $3::uuid, 'full', 'ready', NOW(), $4,
                     $5, 'full', 'suspend')"
        ))
        .bind(snapshot_id)
        .bind(suspended_id)
        .bind(SEED_ORG_ID)
        .bind(registered_node.node_id)
        .bind(artifact_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, 's3://bucket/snapshot')"
        ))
        .bind(snapshot_id)
        .execute(&db.pool)
        .await?;
        assert_eq!(
            census_counts(&store, registered_node.node_id).await?,
            counts(1, 0)
        );

        // Terminal residents and drained nodes drop out of the first pass.
        sqlx::query(sql!(
            "UPDATE sandboxes SET lifecycle = 'failed' WHERE id = $1"
        ))
        .bind(deleted_row.sandbox_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE nodes SET drain_phase = 'draining' WHERE id = $1"
        ))
        .bind(registered_node.node_id)
        .execute(&db.pool)
        .await?;
        let residents = store.node_residents().await?;
        assert!(residents.iter().all(
            |row| row.node_id != registered_node.node_id && row.node_id != deleted_row.node_id
        ));
        Ok(())
    }

    /// Give the suspend's local artifact a ready, stored snapshot, so it
    /// restores from object storage.
    async fn make_durable(
        pool: &PgPool,
        sandbox_id: SandboxId,
        node_id: NodeId,
        artifact_id: uuid::Uuid,
    ) -> TestResult {
        let snapshot_id = SnapshotId::new();
        sqlx::query(sql!(
            "INSERT INTO snapshots
                (id, sandbox_id, org_id, type, state, timestamp, source_node_id,
                 local_artifact_id, artifact_kind, artifact_origin)
             VALUES ($1, $2, $3::uuid, 'full', 'ready', NOW(), $4,
                     $5, 'full', 'suspend')"
        ))
        .bind(snapshot_id)
        .bind(sandbox_id)
        .bind(SEED_ORG_ID)
        .bind(node_id)
        .bind(artifact_id)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, 's3://bucket/snapshot')"
        ))
        .bind(snapshot_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Move the suspend's `lifecycle_changed_at` past the window. The
    /// trigger only touches it when `lifecycle` changes.
    async fn age_suspend(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle_changed_at = NOW() - make_interval(secs => $2::DOUBLE PRECISION)
              WHERE id = $1"
        ))
        .bind(sandbox_id)
        .bind(DELETED_NODE_AT_REST_WINDOW.as_secs_f64() + 3600.0)
        .execute(pool)
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p13_old_deleted_node_with_a_durable_suspend_is_not_a_suspect() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        // A node whose row is long gone, still named by a durable suspend.
        let node_id = NodeId::new();
        let (sandbox_id, artifact_id) = seed_suspended_local_only(&db.pool, node_id).await?;
        make_durable(&db.pool, sandbox_id, node_id, artifact_id).await?;
        age_suspend(&db.pool, sandbox_id).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);

        let row = store
            .node_residents()
            .await?
            .into_iter()
            .find(|row| row.node_id == node_id);
        assert_eq!(
            row.map(|row| (row.source_node, row.residents, row.recent_residents)),
            Some((SourceNode::RowDeleted, 1, 0)),
            "counted, but not as recent"
        );

        let lookup = Arc::new(FakeLookup::default());
        let report = Reconciler::new(config(), store, lookup.clone(), presence())
            .tick()
            .await;
        assert_eq!(gone(&report), (0, 0));
        assert!(lookup.calls().is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p14_recently_deleted_node_with_a_local_only_suspend_counts_at_rest() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);

        // Suspended on a node deleted just now: the suspend itself is recent.
        let fresh_node = NodeId::new();
        seed_suspended_local_only(&db.pool, fresh_node).await?;
        let mut reconciler = Reconciler::new(
            config(),
            store,
            Arc::new(FakeLookup::default()),
            Arc::new(FakePresence::listing(&[NODE_NAME])),
        );
        let report = reconciler.tick().await;
        assert_eq!(gone(&report), (1, 1));

        // An old suspend on a live node, then the row is deleted outside a
        // drain: the census saw it registered, so it still counts.
        let watched = NodeId::new();
        insert_node(&db.pool, watched, NODE_NAME).await?;
        let (old_id, _) = seed_suspended_local_only(&db.pool, watched).await?;
        age_suspend(&db.pool, old_id).await?;
        let report = reconciler.tick().await;
        assert_eq!(gone(&report), (1, 1), "the listed node is live");
        sqlx::query(sql!("DELETE FROM nodes WHERE id = $1"))
            .bind(watched)
            .execute(&db.pool)
            .await?;
        let report = reconciler.tick().await;
        assert_eq!(gone(&report), (2, 2));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p10_flip_rechecks_restore_sources_and_generation() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        let candidate = listed(&store).await?.remove(0);

        // Each source appears after the scan; the flip must still refuse.
        for (state, age, source) in [
            ("ready", "1 minute", None),
            ("creating", "1 minute", Some(seeded.node_id)),
            ("creating", "1 day", Some(NodeId::new())),
            ("creating", "1 day", None),
        ] {
            let snapshot = insert_snapshot(&db.pool, seeded.sandbox_id, state, age, source).await?;
            assert!(
                !store.mark_lost(&candidate).await?,
                "{state} snapshot aged {age} from {source:?} must refuse the flip"
            );
            sqlx::query(sql!(
                "DELETE FROM snapshot_storage_locations WHERE snapshot_id = $1"
            ))
            .bind(snapshot)
            .execute(&db.pool)
            .await?;
            sqlx::query(sql!("DELETE FROM snapshots WHERE id = $1"))
                .bind(snapshot)
                .execute(&db.pool)
                .await?;
        }

        // A covering ready snapshot whose only location is orphaned can't be
        // restored, so it doesn't keep the sandbox.
        let orphaned =
            insert_snapshot(&db.pool, seeded.sandbox_id, "ready", "1 minute", None).await?;
        sqlx::query(sql!(
            "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id = $1"
        ))
        .bind(orphaned)
        .execute(&db.pool)
        .await?;

        // An execution that tracks a pod refuses the flip and leaves the
        // scan: the pod may be live on another node.
        for pod_column in [
            "pod_uid = 'replacement-pod-uid'",
            "pod_ip = '10.9.8.9'",
            "k8s_node_name = 'another-node'",
        ] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE sandbox_k8s_executions SET {pod_column} WHERE sandbox_id = $1 AND active"
            )))
            .bind(seeded.sandbox_id)
            .execute(&db.pool)
            .await?;
            assert!(!store.mark_lost(&candidate).await?, "{pod_column}");
            assert!(listed(&store).await?.is_empty(), "{pod_column}");
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                    SET pod_uid = NULL, pod_ip = NULL, k8s_node_name = NULL
                  WHERE sandbox_id = $1 AND active"
            ))
            .bind(seeded.sandbox_id)
            .execute(&db.pool)
            .await?;
        }

        // A wake that replaced the execution with a new generation refuses.
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
        ))
        .bind(seeded.sandbox_id)
        .execute(&db.pool)
        .await?;
        insert_failed_recovery(&db.pool, seeded.sandbox_id, 8).await?;
        assert!(!store.mark_lost(&candidate).await?);

        // The rescanned candidate carries the new generation and flips.
        let rescanned = listed(&store).await?.remove(0);
        assert_eq!(rescanned.generation, 8);
        assert!(store.mark_lost(&rescanned).await?);
        assert_eq!(
            sandbox_state(&db.pool, seeded.sandbox_id).await?,
            ("failed".to_string(), None, 0)
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p11_flip_rechecks_placement_drain_lifecycle_and_substrate() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, true).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        let candidate = listed(&store).await?.remove(0);

        // Each change lands after the scan and must refuse the flip; undoing
        // it makes the candidate flippable again.
        for (change, undo) in [
            (
                "UPDATE sandboxes SET active_node_id = gen_random_uuid() WHERE id = $1",
                "UPDATE sandboxes SET active_node_id = target_node_id WHERE id = $1",
            ),
            (
                "UPDATE nodes SET drain_phase = 'draining'
                  WHERE id = (SELECT target_node_id FROM sandboxes WHERE id = $1)",
                "UPDATE nodes SET drain_phase = NULL
                  WHERE id = (SELECT target_node_id FROM sandboxes WHERE id = $1)",
            ),
            (
                "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1",
                "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1",
            ),
            (
                "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1",
                "UPDATE sandboxes SET managed_by = 'k8s' WHERE id = $1",
            ),
            (
                "UPDATE nodes SET managed_by = 'space'
                  WHERE id = (SELECT target_node_id FROM sandboxes WHERE id = $1)",
                "UPDATE nodes SET managed_by = 'k8s'
                  WHERE id = (SELECT target_node_id FROM sandboxes WHERE id = $1)",
            ),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(change))
                .bind(seeded.sandbox_id)
                .execute(&db.pool)
                .await?;
            assert!(!store.mark_lost(&candidate).await?, "{change}");
            sqlx::query(sqlx::AssertSqlSafe(undo))
                .bind(seeded.sandbox_id)
                .execute(&db.pool)
                .await?;
        }

        assert!(store.mark_lost(&candidate).await?);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p9_only_recovery_failed_started_kubernetes_rows_are_listed() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let seeded = seed_stuck(&db.pool, false).await?;
        let store = LostStartedDbStore::new(db.pool.clone(), ResidencyEvidence::LifecycleOnly);
        assert_eq!(listed(&store).await?.len(), 1);

        for (column_update, restore) in [
            (
                "UPDATE sandbox_k8s_executions SET state = 'running' WHERE sandbox_id = $1",
                "UPDATE sandbox_k8s_executions SET state = 'recovery_failed' WHERE sandbox_id = $1",
            ),
            (
                "UPDATE sandboxes SET lifecycle = 'suspended' WHERE id = $1",
                "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1",
            ),
            (
                "UPDATE sandboxes SET managed_by = 'space' WHERE id = $1",
                "UPDATE sandboxes SET managed_by = 'k8s' WHERE id = $1",
            ),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(column_update))
                .bind(seeded.sandbox_id)
                .execute(&db.pool)
                .await?;
            assert!(listed(&store).await?.is_empty(), "{column_update}");
            sqlx::query(sqlx::AssertSqlSafe(restore))
                .bind(seeded.sandbox_id)
                .execute(&db.pool)
                .await?;
        }
        assert_eq!(listed(&store).await?.len(), 1);
        Ok(())
    }
}

// -----------------------------------------------------------------
// S — stranded-sandbox census
// -----------------------------------------------------------------

fn counts(running: u64, at_rest_without_copy: u64) -> StrandedCounts {
    StrandedCounts {
        running,
        at_rest_without_copy,
    }
}

fn gone(report: &TickReport) -> (u32, u64) {
    verdict_totals(report, StrandedVerdict::Gone)
}

fn verdict_totals(report: &TickReport, verdict: StrandedVerdict) -> (u32, u64) {
    (
        report.stranded_nodes.get(&verdict).copied().unwrap_or(0),
        report
            .stranded_sandboxes
            .get(&verdict)
            .copied()
            .unwrap_or(0),
    )
}

#[tokio::test]
async fn s1_census_separates_gone_from_unverifiable() {
    let sink = FakeSink::new(Vec::new());
    // Gone: the row is deleted, or the registered node is absent with no EC2
    // instance and no Node.
    sink.with_node(NodeId::new(), SourceNode::RowDeleted, counts(2, 1));
    sink.with_node(NodeId::new(), registered("terminated", None), counts(3, 0));
    // Unverifiable: no Kubernetes name, a failed EC2 lookup, a failed Node GET.
    sink.with_node(NodeId::new(), SourceNode::Unverifiable, counts(1, 0));
    sink.with_node(NodeId::new(), registered("ec2-flaky", None), counts(1, 0));
    sink.with_node(NodeId::new(), registered("get-flaky", None), counts(0, 1));
    let lookup = Arc::new(FakeLookup::default().with("ec2-flaky", Err("throttled".to_string())));
    let presence = Arc::new(FakePresence::listing(&["some-other-node"]).failing("get-flaky"));
    let mut reconciler = Reconciler::new(config(), sink.clone(), lookup, presence);

    let report = reconciler.tick().await;

    assert_eq!(gone(&report), (2, 6));
    assert_eq!(
        verdict_totals(&report, StrandedVerdict::Unverifiable),
        (3, 3)
    );
    assert_eq!(verdict_totals(&report, StrandedVerdict::NotReady), (0, 0));
    assert!(sink.mark_calls().is_empty(), "the census never flips");
}

#[tokio::test]
async fn s2_live_ready_nodes_and_empty_counts_are_not_reported() {
    let sink = FakeSink::new(Vec::new());
    let listed = NodeId::new();
    let ec2_live = NodeId::new();
    let api_live = NodeId::new();
    sink.with_node(listed, registered("listed", None), counts(4, 0));
    sink.with_node(ec2_live, registered("ec2-live", None), counts(4, 0));
    sink.with_node(api_live, registered("api-live", None), counts(4, 0));
    // Gone, but every resident is at rest with a durable copy.
    sink.with_node(NodeId::new(), SourceNode::RowDeleted, counts(0, 0));
    let lookup = Arc::new(FakeLookup::default().live("ec2-live"));
    let presence = Arc::new(FakePresence::listing(&["listed"]).existing("api-live"));
    let mut reconciler = Reconciler::new(config(), sink.clone(), lookup.clone(), presence);

    let report = reconciler.tick().await;

    for verdict in StrandedVerdict::ALL {
        assert_eq!(verdict_totals(&report, verdict), (0, 0), "{verdict:?}");
    }
    assert!(
        !lookup.calls().contains(&"listed".to_string()),
        "a listed node costs no EC2 call"
    );
    let counted = sink.count_calls();
    for live in [listed, ec2_live, api_live] {
        assert!(!counted.contains(&live), "live nodes get no second pass");
    }
}

#[tokio::test(start_paused = true)]
async fn s3_not_ready_must_last_the_grace() {
    let sink = FakeSink::new(Vec::new());
    sink.with_node(NodeId::new(), registered("sick", None), counts(2, 0));
    let lookup = Arc::new(FakeLookup::default());
    let presence = Arc::new(FakePresence::listing(&["sick"]).not_ready("sick"));
    let config = ReconcilerConfig {
        absence_grace: Duration::from_secs(600),
        ..config()
    };
    let mut reconciler = Reconciler::new(config, sink.clone(), lookup.clone(), presence.clone());

    let report = reconciler.tick().await;
    assert_eq!(verdict_totals(&report, StrandedVerdict::NotReady), (0, 0));

    tokio::time::advance(Duration::from_secs(601)).await;
    let report = reconciler.tick().await;
    assert_eq!(verdict_totals(&report, StrandedVerdict::NotReady), (1, 2));
    assert_eq!(gone(&report), (0, 0), "not ready is never gone");
    assert!(lookup.calls().is_empty(), "a listed node costs no EC2 call");

    // Ready again restarts the clock.
    presence.not_ready.lock().unwrap().clear();
    assert_eq!(
        verdict_totals(&reconciler.tick().await, StrandedVerdict::NotReady),
        (0, 0)
    );
    presence
        .not_ready
        .lock()
        .unwrap()
        .insert("sick".to_string());
    assert_eq!(
        verdict_totals(&reconciler.tick().await, StrandedVerdict::NotReady),
        (0, 0)
    );
}

#[tokio::test]
async fn s4_disabled_give_up_still_runs_the_census_and_flips_nothing() {
    let sink = FakeSink::new(vec![vec![candidate(SourceNode::RowDeleted)]]);
    sink.with_node(NodeId::new(), SourceNode::RowDeleted, counts(1, 0));
    let config = ReconcilerConfig {
        enabled: false,
        ..config()
    };
    let mut reconciler = Reconciler::new(
        config,
        sink.clone(),
        Arc::new(FakeLookup::default()),
        presence(),
    );

    let report = reconciler.tick().await;

    assert_eq!(gone(&report), (1, 1));
    assert_eq!(report.scanned, 0);
    assert_eq!(
        sink.list_calls(),
        0,
        "no candidate scan with the give-up off"
    );
    assert!(sink.mark_calls().is_empty());
}

#[tokio::test]
async fn s5_census_lookups_are_capped_per_tick() {
    let sink = FakeSink::new(Vec::new());
    for name in ["dead-a", "dead-b", "dead-c"] {
        sink.with_node(NodeId::new(), registered(name, None), counts(1, 0));
    }
    let lookup = Arc::new(FakeLookup::default());
    let config = ReconcilerConfig {
        max_census_lookups_per_tick: 2,
        ..config()
    };
    let mut reconciler = Reconciler::new(config, sink, lookup.clone(), presence());

    let report = reconciler.tick().await;

    assert_eq!(lookup.calls().len(), 2);
    assert_eq!(gone(&report), (2, 2));
    assert_eq!(
        verdict_totals(&report, StrandedVerdict::Unverifiable),
        (1, 1),
        "the node past the cap reads as lookup_budget"
    );
}

#[tokio::test]
async fn s6_cold_reflector_skips_the_census() {
    let sink = FakeSink::new(Vec::new());
    sink.with_node(NodeId::new(), SourceNode::RowDeleted, counts(1, 0));
    let mut reconciler = Reconciler::new(
        config(),
        sink.clone(),
        Arc::new(FakeLookup::default()),
        Arc::new(FakePresence::cold()),
    );

    let report = reconciler.tick().await;

    assert!(report.stranded_nodes.is_empty());
    assert!(sink.count_calls().is_empty());
}

#[tokio::test]
async fn s7_census_reuses_the_give_up_verdict() {
    let node_id = NodeId::new();
    let mut stuck = candidate(registered("dead", None));
    stuck.source_node_id = node_id;
    let sink = FakeSink::new(vec![vec![stuck]]);
    sink.with_node(node_id, registered("dead", None), counts(1, 0));
    let lookup = Arc::new(FakeLookup::default());
    let presence = presence();
    let mut reconciler = Reconciler::new(config(), sink.clone(), lookup.clone(), presence.clone());

    let report = reconciler.tick().await;

    assert_eq!(report.marked_lost, 1);
    assert_eq!(gone(&report), (1, 1));
    assert_eq!(
        lookup.calls(),
        vec!["dead".to_string()],
        "one EC2 call per node"
    );
    assert_eq!(presence.direct_calls(), vec!["dead".to_string()]);
}

#[tokio::test]
async fn s8_census_lookups_resume_after_the_last_judged_node() {
    let sink = FakeSink::new(Vec::new());
    for name in ["dead-a", "dead-b", "dead-c"] {
        sink.with_node(NodeId::new(), registered(name, None), counts(1, 0));
    }
    let lookup = Arc::new(FakeLookup::default());
    let config = ReconcilerConfig {
        max_census_lookups_per_tick: 2,
        ..config()
    };
    let mut reconciler = Reconciler::new(config, sink, lookup.clone(), presence());

    reconciler.tick().await;
    let first: HashSet<String> = lookup.calls().into_iter().collect();
    assert_eq!(first.len(), 2);
    reconciler.tick().await;

    let calls = lookup.calls();
    assert_eq!(calls.len(), 4, "the cap still holds per tick");
    let skipped = ["dead-a", "dead-b", "dead-c"]
        .into_iter()
        .find(|name| !first.contains(*name))
        .unwrap();
    assert_eq!(
        calls[2], skipped,
        "the second tick starts with the node the first one skipped"
    );
}

#[tokio::test(start_paused = true)]
async fn s9_deleted_node_at_rest_counts_only_within_the_window() {
    let sink = FakeSink::new(Vec::new());
    // Deleted before this controller saw it, holding only old suspends.
    let long_gone = NodeId::new();
    sink.with_old_node(long_gone, SourceNode::RowDeleted, counts(0, 1));
    // Seen registered, then deleted, holding the same old suspends.
    let watched = NodeId::new();
    sink.with_old_node(watched, registered("listed", None), counts(0, 1));
    let presence = Arc::new(FakePresence::listing(&["listed"]));
    let mut reconciler = Reconciler::new(
        config(),
        sink.clone(),
        Arc::new(FakeLookup::default()),
        presence,
    );

    reconciler.tick().await;
    assert!(
        !sink.count_calls().contains(&long_gone),
        "a node deleted before the window is not a suspect"
    );

    sink.set_source_node(watched, SourceNode::RowDeleted);
    sink.clear_count_calls();
    let report = reconciler.tick().await;
    assert_eq!(sink.count_calls(), vec![watched]);
    assert_eq!(gone(&report), (1, 1), "a node seen deleted counts at rest");

    tokio::time::advance(DELETED_NODE_AT_REST_WINDOW).await;
    sink.clear_count_calls();
    let report = reconciler.tick().await;
    assert!(sink.count_calls().is_empty(), "past the window it stops");
    assert_eq!(gone(&report), (0, 0));
}
