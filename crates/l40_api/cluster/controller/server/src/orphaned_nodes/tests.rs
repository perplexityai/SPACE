//! # Test matrix
//!
//! Every behavior the reconciler exhibits maps to one test below.
//!
//! ## C — `ReconcilerConfig` + env parsing
//! - `c1_defaults_when_env_unset`
//! - `c2_each_env_var_overrides_default`
//! - `c3_invalid_env_values_fall_back_to_defaults`
//!
//! ## R — `run()` loop lifecycle
//! - `r1_disabled_returns_on_shutdown_without_ticking`
//!
//! ## T — Tick behavior
//! - `t1_empty_candidate_list_is_noop`
//! - `t2_gone_verdict_deletes_row`
//! - `t3_live_verdict_skips_row`
//! - `t4_unknown_state_treated_as_live_via_lookup_impl`
//! - `t5_lookup_error_skips_row_without_failing_tick`
//! - `t6_delete_returns_false_is_drift_skip`
//! - `t7_delete_error_recorded_but_other_rows_still_processed`
//! - `t8_max_per_tick_caps_deletes`
//! - `t9_passes_observed_uid_through_to_delete`
//!
//! ## P — Postgres-backed store
//! - `placement_gate_blocks_on_each_pinning_column_independently`
//! - `direct_nodes_are_neither_candidates_nor_deleted`

use pplx_space_common_core::types::NodeId;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use pplx_space_common_core::sql;
use pplx_space_test_env::TestResult;

use super::*;

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Default)]
struct FakeSink {
    candidates: Mutex<Vec<OrphanCandidate>>,
    /// (node_id, expected_uid) recorded for every call.
    delete_calls: Mutex<Vec<(NodeId, Option<String>)>>,
    /// node_ids whose conditional DELETE should return `Ok(false)`.
    refuse_delete: Mutex<Vec<NodeId>>,
    /// node_ids whose DELETE should return an `sqlx` error.
    fail_delete: Mutex<Vec<NodeId>>,
}

impl FakeSink {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set_candidates(&self, rows: Vec<OrphanCandidate>) {
        *self.candidates.lock().unwrap() = rows;
    }

    fn refuse(&self, node_id: NodeId) {
        self.refuse_delete.lock().unwrap().push(node_id);
    }

    fn fail(&self, node_id: NodeId) {
        self.fail_delete.lock().unwrap().push(node_id);
    }

    fn delete_calls(&self) -> Vec<(NodeId, Option<String>)> {
        self.delete_calls.lock().unwrap().clone()
    }
}

impl OrphanedNodesSink for Arc<FakeSink> {
    async fn list_candidates(
        &self,
        limit: i64,
        after: Option<NodeId>,
    ) -> Result<Vec<OrphanCandidate>, sqlx::Error> {
        let all = self.candidates.lock().unwrap().clone();
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        Ok(all
            .into_iter()
            .filter(|c| after.map(|a| c.node_id > a).unwrap_or(true))
            .take(limit)
            .collect())
    }

    async fn delete_node_if_orphan(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
    ) -> Result<bool, sqlx::Error> {
        self.delete_calls
            .lock()
            .unwrap()
            .push((node_id, expected_uid.clone()));
        if self.fail_delete.lock().unwrap().contains(&node_id) {
            return Err(sqlx::Error::PoolClosed);
        }
        if self.refuse_delete.lock().unwrap().contains(&node_id) {
            return Ok(false);
        }
        Ok(true)
    }
}

#[derive(Default)]
struct FakeLookup {
    verdicts: Mutex<HashMap<String, Result<InstanceLookupOutcome, String>>>,
}

impl FakeLookup {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn set_gone(&self, dns: &str) {
        self.verdicts
            .lock()
            .unwrap()
            .insert(dns.to_string(), Ok(InstanceLookupOutcome::Gone));
    }

    fn set_live(&self, dns: &str, instance_id: &str, state: &str) {
        self.verdicts.lock().unwrap().insert(
            dns.to_string(),
            Ok(InstanceLookupOutcome::Live {
                instance_id: instance_id.to_string(),
                state: state.to_string(),
            }),
        );
    }

    fn set_error(&self, dns: &str, msg: &str) {
        self.verdicts
            .lock()
            .unwrap()
            .insert(dns.to_string(), Err(msg.to_string()));
    }
}

impl InstanceLookup for Arc<FakeLookup> {
    async fn lookup_by_private_dns(
        &self,
        private_dns_name: String,
    ) -> Result<InstanceLookupOutcome, InstanceLookupError> {
        match self
            .verdicts
            .lock()
            .unwrap()
            .get(&private_dns_name)
            .cloned()
        {
            Some(Ok(outcome)) => Ok(outcome),
            Some(Err(msg)) => Err(InstanceLookupError::Backend(msg)),
            None => Ok(InstanceLookupOutcome::Gone),
        }
    }
}

fn candidate(node_id: NodeId, dns: &str, uid: Option<&str>) -> OrphanCandidate {
    OrphanCandidate {
        node_id,
        k8s_node_name: dns.to_string(),
        k8s_node_uid: uid.map(str::to_string),
    }
}

fn config(max_per_tick: u32) -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        tick: Duration::from_secs(60),
        max_per_tick,
    }
}

// -----------------------------------------------------------------
// C — config
// -----------------------------------------------------------------

#[test]
fn c1_defaults_when_env_unset() {
    let env = pplx_space_test_env::TestEnv::default();
    let c = ReconcilerConfig::from_environment(&env);
    assert!(!c.enabled);
    assert_eq!(c.tick, DEFAULT_TICK);
    assert_eq!(c.max_per_tick, DEFAULT_MAX_PER_TICK);
}

#[test]
fn c2_enabled_env_var_overrides_default() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_ORPHANED_NODES_ENABLED", "true");
    let c = ReconcilerConfig::from_environment(&env);
    assert!(c.enabled);
    assert_eq!(c.tick, DEFAULT_TICK);
    assert_eq!(c.max_per_tick, DEFAULT_MAX_PER_TICK);
}

// -----------------------------------------------------------------
// R — run loop
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn r1_disabled_returns_on_shutdown_without_ticking() {
    let sink = FakeSink::new();
    let lookup = FakeLookup::new();
    // Seed a gone-node candidate and let virtual tick intervals elapse
    // before cancelling, so the emptiness assert below is falsifiable: a
    // regressed disabled gate would tick and delete inside this window.
    let node = NodeId::new();
    sink.set_candidates(vec![candidate(node, "ip-10-0-0-1", Some("uid-1"))]);
    let token = CancellationToken::new();
    let disabled = ReconcilerConfig {
        enabled: false,
        tick: Duration::from_secs(60),
        max_per_tick: DEFAULT_MAX_PER_TICK,
    };
    let token_for_cancel = token.clone();
    let sink_for_run = sink.clone();
    let lookup_for_run = lookup.clone();
    let handle = tokio::spawn(async move {
        run(disabled, sink_for_run, lookup_for_run, token).await;
    });
    tokio::time::sleep(Duration::from_secs(300)).await;
    token_for_cancel.cancel();
    handle.await.unwrap();
    assert!(
        sink.delete_calls().is_empty(),
        "disabled reconciler must never call list_candidates/delete_node_if_orphan"
    );
}

// -----------------------------------------------------------------
// T — tick behavior
// -----------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn t1_empty_candidate_list_is_noop() {
    let sink = FakeSink::new();
    let lookup = FakeLookup::new();
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let report = reconciler.tick().await;
    assert_eq!(report, TickReport::default());
    assert!(sink.delete_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t2_gone_verdict_deletes_row() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(node, "ip-10-0-0-1", Some("uid-1"))]);
    let lookup = FakeLookup::new();
    lookup.set_gone("ip-10-0-0-1");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 1);
    assert_eq!(report.skipped_live, 0);
    assert_eq!(sink.delete_calls(), vec![(node, Some("uid-1".to_string()))]);
}

#[tokio::test(flavor = "current_thread")]
async fn t3_live_verdict_skips_row() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(node, "ip-10-0-0-1", Some("uid-1"))]);
    let lookup = FakeLookup::new();
    lookup.set_live("ip-10-0-0-1", "i-live", "running");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(report.skipped_live, 1);
    assert!(sink.delete_calls().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn t4_unknown_state_treated_as_live_via_lookup_impl() {
    // The lookup impl is the source of truth on what counts as "live";
    // the reconciler treats anything Live as a skip. The Ec2InstanceLookup
    // unit tests in aws.rs verify that an unknown EC2 state classifies as
    // Live — this test just confirms the reconciler honors that.
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(node, "ip-10-0-0-1", Some("uid-1"))]);
    let lookup = FakeLookup::new();
    lookup.set_live("ip-10-0-0-1", "i-unknown", "future-mystery-state");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(report.skipped_live, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t5_lookup_error_skips_row_without_failing_tick() {
    let node_a = NodeId::new();
    let node_b = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![
        candidate(node_a, "ip-10-0-0-1", Some("uid-a")),
        candidate(node_b, "ip-10-0-0-2", Some("uid-b")),
    ]);
    let lookup = FakeLookup::new();
    lookup.set_error("ip-10-0-0-1", "throttled");
    lookup.set_gone("ip-10-0-0-2");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let report = reconciler.tick().await;
    assert_eq!(report.lookup_errors, 1);
    assert_eq!(report.deleted, 1);
    assert_eq!(
        sink.delete_calls(),
        vec![(node_b, Some("uid-b".to_string()))]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn t6_delete_returns_false_is_drift_skip() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(node, "ip-10-0-0-1", Some("uid-1"))]);
    sink.refuse(node);
    let lookup = FakeLookup::new();
    lookup.set_gone("ip-10-0-0-1");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 0);
    assert_eq!(report.skipped_drift, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t7_delete_error_recorded_but_other_rows_still_processed() {
    let node_a = NodeId::new();
    let node_b = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![
        candidate(node_a, "ip-10-0-0-1", Some("uid-a")),
        candidate(node_b, "ip-10-0-0-2", Some("uid-b")),
    ]);
    sink.fail(node_a);
    let lookup = FakeLookup::new();
    lookup.set_gone("ip-10-0-0-1");
    lookup.set_gone("ip-10-0-0-2");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 1);
    assert_eq!(report.delete_errors, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t8_max_per_tick_caps_deletes() {
    let n1 = NodeId::new();
    let n2 = NodeId::new();
    let n3 = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![
        candidate(n1, "ip-a", None),
        candidate(n2, "ip-b", None),
        candidate(n3, "ip-c", None),
    ]);
    let lookup = FakeLookup::new();
    lookup.set_gone("ip-a");
    lookup.set_gone("ip-b");
    lookup.set_gone("ip-c");
    let mut reconciler = Reconciler::new(config(2), sink.clone(), lookup);
    let report = reconciler.tick().await;
    assert_eq!(report.deleted, 2);
    assert_eq!(report.rate_limited, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn t9_passes_observed_uid_through_to_delete() {
    let node = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(node, "ip-10-0-0-1", Some("uid-observed"))]);
    let lookup = FakeLookup::new();
    lookup.set_gone("ip-10-0-0-1");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let _ = reconciler.tick().await;
    assert_eq!(
        sink.delete_calls(),
        vec![(node, Some("uid-observed".to_string()))],
    );
}

/// Regression for the codex-flagged starvation case: if every tick scans
/// the same prefix from the front of the ascending-`id` order, a fully-live
/// prefix permanently blocks later orphan candidates. The cursor must
/// advance even when no row was deleted, so subsequent ticks see new rows.
#[tokio::test(flavor = "current_thread")]
async fn t10_cursor_advances_past_live_rows_to_reach_later_orphans() {
    // Two live rows up front; one orphan after them. With `max_per_tick=1`
    // and `limit = 2 * max_per_tick = 2`, the first tick scans the two
    // live rows. Without a cursor, the second tick would see the same two
    // rows again and never reach the orphan. With the cursor, the second
    // tick scans rows > live_b and finds the orphan.
    let live_a = NodeId::new();
    let live_b = NodeId::new();
    let orphan = NodeId::new();
    assert!(
        live_a < live_b && live_b < orphan,
        "UUIDv7 ordering invariant"
    );

    let sink = FakeSink::new();
    sink.set_candidates(vec![
        candidate(live_a, "ip-live-a", None),
        candidate(live_b, "ip-live-b", None),
        candidate(orphan, "ip-orphan", None),
    ]);
    let lookup = FakeLookup::new();
    lookup.set_live("ip-live-a", "i-a", "running");
    lookup.set_live("ip-live-b", "i-b", "running");
    lookup.set_gone("ip-orphan");

    let mut reconciler = Reconciler::new(config(1), sink.clone(), lookup);

    let r1 = reconciler.tick().await;
    assert_eq!(r1.deleted, 0);
    assert_eq!(r1.skipped_live, 2);
    assert!(sink.delete_calls().is_empty());

    let r2 = reconciler.tick().await;
    assert_eq!(r2.deleted, 1);
    assert_eq!(sink.delete_calls(), vec![(orphan, None)]);
}

/// Companion to t10: when a tick consumes a non-full page, the cursor
/// resets to `None` so the next tick starts a fresh sweep from the
/// beginning. Otherwise newly-created orphan rows would be ignored
/// forever (the cursor would sit at the end of the table).
#[tokio::test(flavor = "current_thread")]
async fn t11_cursor_wraps_to_start_after_a_short_page() {
    let n1 = NodeId::new();
    let sink = FakeSink::new();
    sink.set_candidates(vec![candidate(n1, "ip-1", None)]);
    let lookup = FakeLookup::new();
    lookup.set_gone("ip-1");
    let mut reconciler = Reconciler::new(config(10), sink.clone(), lookup);
    let _ = reconciler.tick().await;
    // A new orphan appears at the front of the candidate set after the
    // first tick — the cursor must have wrapped to `None`, otherwise the
    // second tick filters this row out via `n.id > scan_after`.
    let n_new = NodeId::new();
    assert!(n_new > n1, "UUIDv7 monotonicity");
    sink.set_candidates(vec![candidate(n_new, "ip-new", None)]);
    let _ = reconciler.tick().await;
    assert_eq!(
        sink.delete_calls()
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        vec![n1, n_new],
    );
}

/// Postgres-backed pin of the live-placement gate's two halves. Each probe
/// (active_node_id, target_node_id) must independently block candidacy and
/// deletion: a broken or dropped target probe would pass any test that only
/// seeds active-pinned rows, and target-only pinning is the real at-rest
/// case (suspended sandbox, active NULL, artifacts on the node's disk).
#[tokio::test(flavor = "current_thread")]
async fn placement_gate_blocks_on_each_pinning_column_independently() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let active_pinned = NodeId::new();
    let target_pinned = NodeId::new();
    let terminal_pinned = NodeId::new();
    for (node_id, name) in [
        (active_pinned, "orphan-active"),
        (target_pinned, "orphan-target"),
        (terminal_pinned, "orphan-terminal"),
    ] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by)
             VALUES ($1, '10.0.0.1'::INET, 'worker', 'k8s')"
        ))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
        ))
        .bind(node_id)
        .bind(name)
        .execute(&db.pool)
        .await?;
    }
    // active-pinned: running sandbox on the node.
    insert_pinned_sandbox(&db.pool, Some(active_pinned), None, "started").await?;
    // target-pinned: suspended sandbox at rest on the node, active NULL.
    insert_pinned_sandbox(&db.pool, None, Some(target_pinned), "suspended").await?;
    // terminal-pinned: rows exist but every one is terminal.
    insert_pinned_sandbox(
        &db.pool,
        Some(terminal_pinned),
        Some(terminal_pinned),
        "deleted",
    )
    .await?;

    let store = NodeDbStore::new(db.pool.clone());
    let candidates = store.list_candidates(100, None).await?;
    let ids: Vec<NodeId> = candidates.iter().map(|c| c.node_id).collect();

    assert!(
        !ids.contains(&active_pinned),
        "active-pinned node must not be an orphan candidate"
    );
    assert!(
        !ids.contains(&target_pinned),
        "target-only-pinned node must not be an orphan candidate"
    );
    assert!(
        ids.contains(&terminal_pinned),
        "node with only terminal pinned rows is a candidate"
    );

    assert!(
        !store.delete_node_if_orphan(target_pinned, None).await?,
        "delete re-check must refuse a target-only-pinned node"
    );
    assert!(
        !store.delete_node_if_orphan(active_pinned, None).await?,
        "delete re-check must refuse an active-pinned node"
    );
    Ok(())
}

async fn insert_pinned_sandbox(
    pool: &sqlx::PgPool,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    lifecycle: &str,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle,
             active_node_id, target_node_id, network_policy_generation
         )
         VALUES ($1, $2::uuid, $3::uuid, 'space', $4, $5, $6, 1)"
    ))
    .bind(Uuid::now_v7())
    .bind(pplx_space_db_test_fixtures::SEED_USER_ID)
    .bind(pplx_space_db_test_fixtures::SEED_ORG_ID)
    .bind(lifecycle)
    .bind(active_node_id)
    .bind(target_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Both store statements bind the Kubernetes substrate: an otherwise-orphaned
/// direct node is neither a candidate nor deleted.
#[tokio::test(flavor = "current_thread")]
async fn direct_nodes_are_neither_candidates_nor_deleted() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let k8s_node = NodeId::new();
    let direct_node = NodeId::new();
    for (node_id, managed_by) in [(k8s_node, "k8s"), (direct_node, "space")] {
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by)
             VALUES ($1, '10.0.0.1'::INET, 'worker', $2)"
        ))
        .bind(node_id)
        .bind(managed_by)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
        ))
        .bind(node_id)
        .bind(format!("orphan-{managed_by}"))
        .execute(&db.pool)
        .await?;
    }

    let store = NodeDbStore::new(db.pool.clone());
    let ids: Vec<NodeId> = store
        .list_candidates(100, None)
        .await?
        .iter()
        .map(|c| c.node_id)
        .collect();
    assert_eq!(ids, vec![k8s_node]);
    assert!(
        !store.delete_node_if_orphan(direct_node, None).await?,
        "the delete must refuse a direct node"
    );
    assert!(store.delete_node_if_orphan(k8s_node, None).await?);
    Ok(())
}
