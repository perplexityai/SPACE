//! The loop against fakes of its sink, Node membership and EC2, with paused
//! time for the absence window; and one Postgres-backed tick that must write
//! nothing but its report.

use pplx_space_common_core::types::{SandboxId, SubstrateKind};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_store::StoredSandboxLifecycle;
use pplx_space_sandbox_store::dead_node::DeadNodeReport;
use pplx_space_test_env::{TestEnv, TestResult};
use sqlx::PgPool;

use super::verdict::{LostReason, UnknownReason};
use super::*;

const WINDOW: Duration = Duration::from_secs(600);
const DEAD_NAME: &str = "worker-dead.synthetic.internal";
const DEAD_UID: &str = "uid-dead";
const REFERENCE_NAME: &str = "worker-live.synthetic.internal";
const REFERENCE_UID: &str = "uid-live";

// -----------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------

#[derive(Default)]
struct FakeSink {
    nodes: Mutex<Vec<StaleNode>>,
    references: Vec<ReferenceNode>,
    rows: Vec<DeadNodeRow>,
    /// Each written report's verdict and reason.
    reports: Mutex<Vec<(String, String)>>,
}

impl FakeSink {
    fn with_node(node: StaleNode) -> Self {
        Self {
            nodes: Mutex::new(vec![node]),
            references: vec![ReferenceNode {
                k8s_node_name: REFERENCE_NAME.to_owned(),
                k8s_node_uid: REFERENCE_UID.to_owned(),
            }],
            ..Self::default()
        }
    }
}

impl DeadNodeSink for Arc<FakeSink> {
    async fn stale_nodes(
        &self,
        _managed_by: SubstrateKind,
        after: Option<NodeId>,
        limit: u32,
    ) -> Result<Vec<StaleNode>, sqlx::Error> {
        let nodes = self.nodes.lock().unwrap();
        Ok(nodes
            .iter()
            .filter(|node| after.is_none_or(|after| node.node_id > after))
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn reference_nodes(
        &self,
        _managed_by: SubstrateKind,
        _limit: u32,
    ) -> Result<Vec<ReferenceNode>, sqlx::Error> {
        Ok(self.references.clone())
    }

    async fn rows_on_node(
        &self,
        _node: &StaleNode,
        _upload_settle: Duration,
        limit: u32,
    ) -> Result<Vec<DeadNodeRow>, sqlx::Error> {
        Ok(self.rows.iter().take(limit as usize).cloned().collect())
    }

    async fn write_report(&self, report: &DeadNodeReportWrite<'_>) -> Result<(), sqlx::Error> {
        self.reports
            .lock()
            .unwrap()
            .push((report.verdict.to_owned(), report.reason.to_owned()));
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("synthetic lookup failure")]
struct LookupFailed;

struct FakeMembership {
    uids: Mutex<Option<HashSet<String>>>,
    exists: Mutex<HashMap<String, Result<bool, ()>>>,
}

impl FakeMembership {
    /// The reflector lists only the reference node; the API server has no
    /// Node by the dead node's name.
    fn dead_node_gone() -> Arc<Self> {
        Arc::new(Self {
            uids: Mutex::new(Some(HashSet::from([REFERENCE_UID.to_owned()]))),
            exists: Mutex::new(HashMap::from([(DEAD_NAME.to_owned(), Ok(false))])),
        })
    }

    fn set_uids(&self, uids: Option<&[&str]>) {
        *self.uids.lock().unwrap() =
            uids.map(|uids| uids.iter().map(|uid| (*uid).to_owned()).collect());
    }

    fn set_exists(&self, name: &str, answer: Result<bool, ()>) {
        self.exists.lock().unwrap().insert(name.to_owned(), answer);
    }
}

impl NodeMembership for Arc<FakeMembership> {
    type LookupError = LookupFailed;

    fn live_node_uids(&self) -> Option<HashSet<String>> {
        self.uids.lock().unwrap().clone()
    }

    async fn node_exists(&self, node_name: String) -> Result<bool, Self::LookupError> {
        match self.exists.lock().unwrap().get(&node_name) {
            Some(Ok(exists)) => Ok(*exists),
            Some(Err(())) | None => Err(LookupFailed),
        }
    }
}

/// EC2 by private DNS name: unlisted names answer an empty reservation set.
#[derive(Default)]
struct FakeEc2 {
    answers: Mutex<HashMap<String, Result<Vec<&'static str>, String>>>,
    calls: Mutex<Vec<String>>,
}

impl FakeEc2 {
    /// The dead node has no instance; the reference is running.
    fn dead_node_gone() -> Arc<Self> {
        let ec2 = Self::default();
        ec2.answer(REFERENCE_NAME, Ok(vec!["running"]));
        Arc::new(ec2)
    }

    fn answer(&self, name: &str, answer: Result<Vec<&'static str>, String>) {
        self.answers.lock().unwrap().insert(name.to_owned(), answer);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl DescribeInstancesClient for FakeEc2 {
    fn describe_instances_by_private_dns(
        &self,
        private_dns_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<MatchedInstance>, String>> + Send + '_>> {
        self.calls.lock().unwrap().push(private_dns_name.clone());
        let answer = self
            .answers
            .lock()
            .unwrap()
            .get(&private_dns_name)
            .cloned()
            .unwrap_or(Ok(Vec::new()));
        Box::pin(async move {
            answer.map(|states| {
                states
                    .into_iter()
                    .map(|state| MatchedInstance {
                        instance_id: "i-0synthetic".to_owned(),
                        state: state.to_owned(),
                    })
                    .collect()
            })
        })
    }
}

fn dead_node(uid: &str) -> StaleNode {
    StaleNode {
        node_id: NodeId::from_uuid(uuid::Uuid::from_u128(1)),
        managed_by: SubstrateKind::Kubernetes,
        k8s_node_name: Some(DEAD_NAME.to_owned()),
        k8s_node_uid: Some(uid.to_owned()),
    }
}

fn row(lifecycle: StoredSandboxLifecycle) -> DeadNodeRow {
    DeadNodeRow {
        sandbox_id: SandboxId::new(),
        lifecycle: Some(lifecycle),
        stored_lifecycle: lifecycle.as_str().to_owned(),
        covered_by_snapshot: false,
        ready_snapshot: false,
        stop_artifact_durable: false,
        upload_pending: false,
        execution_elsewhere: false,
    }
}

fn config() -> ReconcilerConfig {
    ReconcilerConfig {
        absence_window: WINDOW,
        ..ReconcilerConfig::default()
    }
}

fn reconciler<K: NodeMembership>(
    sink: &Arc<FakeSink>,
    membership: K,
    authority: InstanceAuthority,
) -> Reconciler<Arc<FakeSink>, K> {
    Reconciler::new(
        config(),
        sink.clone(),
        membership,
        authority,
        SubstrateKind::Kubernetes,
    )
}

fn verdicts(report: &TickReport) -> Vec<Verdict> {
    report.nodes.iter().map(|node| node.verdict).collect()
}

// -----------------------------------------------------------------
// Config
// -----------------------------------------------------------------

/// Off unless set to `dry-run`, and off has no config: unset, empty, `off`, a
/// value for a mode that doesn't exist, or a typo. The windows move only
/// through the test hooks.
#[test]
fn mode_is_off_unless_dry_run_and_windows_move_only_by_test_hook() {
    let mut env = TestEnv::default();
    assert!(ReconcilerConfig::from_environment(&env).is_none(), "unset");
    for raw in ["off", "", "write", "dry_run", "true"] {
        env.set(MODE_ENV, raw);
        assert!(
            ReconcilerConfig::from_environment(&env).is_none(),
            "{raw:?}"
        );
    }
    for raw in ["dry-run", " dry-run "] {
        env.set(MODE_ENV, raw);
        let parsed = ReconcilerConfig::from_environment(&env).expect(raw);
        assert_eq!(parsed.tick, DEFAULT_TICK);
        assert_eq!(parsed.absence_window, DEFAULT_ABSENCE_GRACE);
        assert_eq!(parsed.upload_settle, DEAD_UPLOAD_SETTLE);
        assert!(!parsed.force_ec2);
    }
    env.set("SPACE_DEAD_NODE_TEST_TICK_SECS", "5");
    env.set("SPACE_DEAD_NODE_TEST_ABSENCE_WINDOW_SECS", "20");
    env.set("SPACE_DEAD_NODE_TEST_UPLOAD_SETTLE_SECS", "30");
    env.set("SPACE_DEAD_NODE_TEST_INSTANCE_AUTHORITY", "ec2");
    let hooked = ReconcilerConfig::from_environment(&env).expect("dry-run");
    assert_eq!(hooked.tick, Duration::from_secs(5));
    assert_eq!(hooked.absence_window, Duration::from_secs(20));
    assert_eq!(hooked.upload_settle, Duration::from_secs(30));
    assert!(hooked.force_ec2);
    env.set(MODE_ENV, "off");
    assert!(
        ReconcilerConfig::from_environment(&env).is_none(),
        "hooked, off"
    );
}

// -----------------------------------------------------------------
// Verdict evidence
// -----------------------------------------------------------------

/// On a `kube-delete` cluster the node is lost once its recorded Node has
/// been absent for the whole window and the API server has no Node by its
/// name; before then it is settling. Each judged node gets a report.
#[tokio::test(start_paused = true)]
async fn kube_delete_node_is_lost_only_after_the_window() {
    let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
    let mut reconciler = reconciler(
        &sink,
        FakeMembership::dead_node_gone(),
        InstanceAuthority::KubeDelete,
    );
    let settling = Verdict::Unknown(UnknownReason::KubeAbsenceSettling);

    assert_eq!(verdicts(&reconciler.tick().await), vec![settling]);
    tokio::time::advance(WINDOW - Duration::from_secs(1)).await;
    assert_eq!(verdicts(&reconciler.tick().await), vec![settling]);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        verdicts(&reconciler.tick().await),
        vec![Verdict::Lost(LostReason::NodeDeleted)]
    );
    let reports = sink.reports.lock().unwrap();
    let stored: Vec<_> = reports
        .iter()
        .map(|(verdict, reason)| (verdict.as_str(), reason.as_str()))
        .collect();
    assert_eq!(
        stored,
        [
            ("unknown", "kube_absence_settling"),
            ("unknown", "kube_absence_settling"),
            ("lost", "node_deleted"),
        ]
    );
}

/// A replacement Node with the dead node's name and a new uid is not judged
/// dead: the recorded uid is absent, but the direct GET finds the name. And
/// when the row records the replacement's uid, the absence window restarts.
#[tokio::test(start_paused = true)]
async fn same_name_replacement_is_not_judged_dead() {
    let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
    let membership = FakeMembership::dead_node_gone();
    membership.set_uids(Some(&[REFERENCE_UID, "uid-replacement"]));
    membership.set_exists(DEAD_NAME, Ok(true));
    let mut reconciler = reconciler(&sink, membership.clone(), InstanceAuthority::KubeDelete);

    reconciler.tick().await;
    tokio::time::advance(WINDOW).await;
    assert_eq!(
        verdicts(&reconciler.tick().await),
        vec![Verdict::Unknown(UnknownReason::KubeNodeFound)]
    );

    // The replacement leaves too, and the row now records its uid: its
    // absence is timed from when that uid was first seen missing.
    membership.set_uids(Some(&[REFERENCE_UID]));
    membership.set_exists(DEAD_NAME, Ok(false));
    *sink.nodes.lock().unwrap() = vec![dead_node("uid-replacement")];
    assert_eq!(
        verdicts(&reconciler.tick().await),
        vec![Verdict::Unknown(UnknownReason::KubeAbsenceSettling)]
    );
    tokio::time::advance(WINDOW).await;
    assert_eq!(
        verdicts(&reconciler.tick().await),
        vec![Verdict::Lost(LostReason::NodeDeleted)]
    );
}

/// A cold or empty reflector reads unknown and forgets every absence, and
/// the recorded uid's return ends one; either way the window starts over.
#[tokio::test(start_paused = true)]
async fn unknown_membership_or_a_returning_uid_restarts_the_window() {
    let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
    let membership = FakeMembership::dead_node_gone();
    let mut reconciler = reconciler(&sink, membership.clone(), InstanceAuthority::KubeDelete);
    let settling = Verdict::Unknown(UnknownReason::KubeAbsenceSettling);

    for interruption in [None, Some(&[][..]), Some(&[REFERENCE_UID, DEAD_UID][..])] {
        reconciler.tick().await;
        tokio::time::advance(WINDOW).await;
        membership.set_uids(interruption);
        let expected = match interruption {
            Some([_, _]) => UnknownReason::KubeNodePresent,
            _ => UnknownReason::KubeMembershipUnknown,
        };
        assert_eq!(
            verdicts(&reconciler.tick().await),
            vec![Verdict::Unknown(expected)],
            "{interruption:?}"
        );
        membership.set_uids(Some(&[REFERENCE_UID]));
        assert_eq!(
            verdicts(&reconciler.tick().await),
            vec![settling],
            "{interruption:?}"
        );
        tokio::time::advance(WINDOW).await;
        assert_eq!(
            verdicts(&reconciler.tick().await),
            vec![Verdict::Lost(LostReason::NodeDeleted)],
            "{interruption:?}"
        );
        reconciler.absent_since.clear();
    }
}

/// With EC2, lost needs no live instance for the node and the reference
/// listed `running` in the same kind of call. EC2 errors, a live instance, a
/// reference missing from EC2 or not in the reflector, and a failed direct
/// GET each read unknown.
#[tokio::test(start_paused = true)]
async fn ec2_verdict_needs_the_node_gone_and_the_reference_running() {
    type Setup = fn(&FakeEc2, &FakeMembership);
    let cases: [(&str, Setup, Verdict); 7] = [
        (
            "all agree",
            |_, _| {},
            Verdict::Lost(LostReason::InstanceAndNodeGone),
        ),
        (
            "only terminated matches",
            |ec2, _| ec2.answer(DEAD_NAME, Ok(vec!["terminated"])),
            Verdict::Lost(LostReason::InstanceAndNodeGone),
        ),
        (
            "a stopped instance is live",
            |ec2, _| ec2.answer(DEAD_NAME, Ok(vec!["terminated", "stopped"])),
            Verdict::Unknown(UnknownReason::InstanceLive),
        ),
        (
            "node lookup errors",
            |ec2, _| ec2.answer(DEAD_NAME, Err("throttled".to_owned())),
            Verdict::Unknown(UnknownReason::InstanceLookupFailed),
        ),
        (
            "reference absent from the answer",
            |ec2, _| ec2.answer(REFERENCE_NAME, Ok(Vec::new())),
            Verdict::Unknown(UnknownReason::ReferenceNotRunning),
        ),
        (
            "reference not in the reflector",
            |_, membership| membership.set_uids(Some(&["uid-unrelated"])),
            Verdict::Unknown(UnknownReason::ReferenceUnavailable),
        ),
        (
            "direct GET fails",
            |_, membership| membership.set_exists(DEAD_NAME, Err(())),
            Verdict::Unknown(UnknownReason::KubeNodeLookupFailed),
        ),
    ];
    for (case, setup, expected) in cases {
        let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
        let ec2 = FakeEc2::dead_node_gone();
        let membership = FakeMembership::dead_node_gone();
        setup(&ec2, &membership);
        let mut reconciler = reconciler(&sink, membership, InstanceAuthority::Ec2(ec2.clone()));
        reconciler.tick().await;
        tokio::time::advance(WINDOW).await;
        assert_eq!(verdicts(&reconciler.tick().await), vec![expected], "{case}");
    }
}

/// A reference error reads unknown too, and one reference lookup serves
/// every node of a tick.
#[tokio::test(start_paused = true)]
async fn reference_is_looked_up_once_a_tick_and_its_error_reads_unknown() {
    let mut second = dead_node("uid-second");
    second.node_id = NodeId::from_uuid(uuid::Uuid::from_u128(2));
    second.k8s_node_name = Some("worker-second.synthetic.internal".to_owned());
    let sink = Arc::new(FakeSink::with_node(dead_node(DEAD_UID)));
    sink.nodes.lock().unwrap().push(second);
    let ec2 = FakeEc2::dead_node_gone();
    let membership = FakeMembership::dead_node_gone();
    membership.set_exists("worker-second.synthetic.internal", Ok(false));
    let mut reconciler = reconciler(&sink, membership, InstanceAuthority::Ec2(ec2.clone()));
    reconciler.tick().await;
    tokio::time::advance(WINDOW).await;
    ec2.calls.lock().unwrap().clear();

    let lost = Verdict::Lost(LostReason::InstanceAndNodeGone);
    assert_eq!(verdicts(&reconciler.tick().await), vec![lost, lost]);
    let reference_calls = ec2
        .calls()
        .iter()
        .filter(|name| *name == REFERENCE_NAME)
        .count();
    assert_eq!(reference_calls, 1, "{:?}", ec2.calls());

    ec2.answer(REFERENCE_NAME, Err("wrong region".to_owned()));
    let unknown = Verdict::Unknown(UnknownReason::ReferenceLookupFailed);
    assert_eq!(verdicts(&reconciler.tick().await), vec![unknown, unknown]);
}

// -----------------------------------------------------------------
// Rows and caps
// -----------------------------------------------------------------

/// A node holding more rows than the cap reports the cap's worth and says it
/// was truncated; each outcome lists at most its sample count.
#[tokio::test(start_paused = true)]
async fn rows_past_the_cap_are_reported_truncated_and_samples_are_bounded() {
    let cap = MAX_ROWS_PER_NODE as usize;
    let sink = Arc::new(FakeSink {
        rows: (0..=cap)
            .map(|_| {
                row(StoredSandboxLifecycle::Live(
                    pplx_space_common_core::types::SandboxLifecycle::Started,
                ))
            })
            .collect(),
        ..FakeSink::with_node(dead_node(DEAD_UID))
    });
    let mut reconciler = reconciler(
        &sink,
        FakeMembership::dead_node_gone(),
        InstanceAuthority::KubeDelete,
    );
    let report = reconciler.tick().await;
    let node = &report.nodes[0];
    assert!(node.rows_truncated);
    assert_eq!(
        node.counts,
        BTreeMap::from([(RowOutcome::Lost, cap as u64)])
    );
    assert_eq!(node.samples[&RowOutcome::Lost].len(), SAMPLES_PER_OUTCOME);

    let exact = Arc::new(FakeSink {
        rows: sink.rows[..cap].to_vec(),
        ..FakeSink::with_node(dead_node(DEAD_UID))
    });
    let mut reconciler = reconciler_for(&exact);
    assert!(!reconciler.tick().await.nodes[0].rows_truncated);
}

fn reconciler_for(sink: &Arc<FakeSink>) -> Reconciler<Arc<FakeSink>, Arc<FakeMembership>> {
    reconciler(
        sink,
        FakeMembership::dead_node_gone(),
        InstanceAuthority::KubeDelete,
    )
}

/// No more than the node cap is judged in a tick, and the cursor moves on:
/// the next tick judges only the rest, and its short page ends the sweep.
#[tokio::test(start_paused = true)]
async fn nodes_past_the_cap_wait_for_the_next_tick() {
    let sink = Arc::new(FakeSink::default());
    *sink.nodes.lock().unwrap() = (0..=MAX_NODES_PER_TICK)
        .map(|n| {
            let mut node = dead_node(DEAD_UID);
            node.node_id = NodeId::from_uuid(uuid::Uuid::from_u128(u128::from(n) + 1));
            node
        })
        .collect();
    let mut reconciler = reconciler_for(&sink);
    let report = reconciler.tick().await;
    assert_eq!(report.nodes.len(), MAX_NODES_PER_TICK as usize);
    assert_eq!(
        reconciler.scan_after,
        report.nodes.last().map(|node| node.node_id)
    );

    let ninth = sink.nodes.lock().unwrap()[MAX_NODES_PER_TICK as usize].node_id;
    let next = reconciler.tick().await;
    let judged: Vec<NodeId> = next.nodes.iter().map(|node| node.node_id).collect();
    assert_eq!(judged, vec![ninth]);
    assert_eq!(reconciler.scan_after, None);
}

// -----------------------------------------------------------------
// Postgres: a dry-run tick writes only its report
// -----------------------------------------------------------------

/// Every public table's row count and contents, by name.
async fn table_contents(pool: &PgPool) -> TestResult<BTreeMap<String, (i64, String)>> {
    let tables: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT quote_ident(table_name)
           FROM information_schema.tables
          WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
          ORDER BY 1"
    ))
    .fetch_all(pool)
    .await?;
    let mut contents = BTreeMap::new();
    for table in tables {
        let statement = format!(
            "SELECT COUNT(*),
                    md5(COALESCE(string_agg(t::TEXT, E'\\n' ORDER BY t::TEXT), ''))
               FROM public.{table} t"
        );
        let digest: (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(sql!(statement)))
            .fetch_one(pool)
            .await?;
        contents.insert(table, digest);
    }
    Ok(contents)
}

/// Rows a dry run would reconcile on a lost node, with snapshots, uploads,
/// executions and a backup-queue row: what a write mode would touch.
async fn seed_dead_node(pool: &PgPool, name: &str, uid: &str) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.6'::INET, 'worker', 'k8s')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(name)
    .bind(uid)
    .execute(pool)
    .await?;
    for lifecycle in StoredSandboxLifecycle::all_as_str() {
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes
                (id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_generation)
             VALUES ($1, $2::UUID, $3::UUID, 'k8s', $4, 'on_node', $5, $5, 1)"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(lifecycle)
        .bind(node_id)
        .execute(pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state)
             VALUES ($1, 1, $2, 'sandboxes', $3, 'running')"
        ))
        .bind(sandbox_id)
        .bind(format!("space-sbx-{}", sandbox_id.as_uuid().simple()))
        .bind(name)
        .execute(pool)
        .await?;
        for state in ["ready", "creating"] {
            sqlx::query(sql!(
                "INSERT INTO snapshots (id, sandbox_id, org_id, type, state, source_node_id)
                 VALUES (space_uuid_v7(), $1, $2::UUID, 'full', $3, $4)"
            ))
            .bind(sandbox_id)
            .bind(SEED_ORG_ID)
            .bind(state)
            .bind(node_id)
            .execute(pool)
            .await?;
        }
        sqlx::query(sql!(
            "INSERT INTO node_local_backup_pending
                (node_id, sandbox_id, artifact_id, artifact_kind)
             VALUES ($1, $2, space_uuid_v7(), 'full')"
        ))
        .bind(node_id)
        .bind(sandbox_id)
        .execute(pool)
        .await?;
    }
    Ok(node_id)
}

/// A dry-run tick that judges a node lost changes no table but
/// `dead_node_dry_runs`, which gains exactly that node's report.
#[tokio::test(flavor = "current_thread")]
async fn dry_run_tick_writes_nothing_but_its_report() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node_id = seed_dead_node(pool, DEAD_NAME, DEAD_UID).await?;
    let membership = FakeMembership::dead_node_gone();
    let mut reconciler = Reconciler::new(
        ReconcilerConfig {
            absence_window: Duration::ZERO,
            ..config()
        },
        DeadNodeDbStore::new(pool.clone()),
        membership,
        InstanceAuthority::KubeDelete,
        SubstrateKind::Kubernetes,
    );

    let mut before = table_contents(pool).await?;
    let report = reconciler.tick().await;
    let mut after = table_contents(pool).await?;

    assert_eq!(report.errors, 0);
    assert_eq!(
        verdicts(&report),
        vec![Verdict::Lost(LostReason::NodeDeleted)]
    );
    let total: u64 = report.nodes[0].counts.values().sum();
    assert_eq!(total, StoredSandboxLifecycle::all().len() as u64);
    let reports_before = before.remove("dead_node_dry_runs").map(|(count, _)| count);
    let reports_after = after.remove("dead_node_dry_runs").map(|(count, _)| count);
    assert_eq!((reports_before, reports_after), (Some(0), Some(1)));
    assert_eq!(before, after);

    let stored: Vec<DeadNodeReport> =
        pplx_space_sandbox_store::dead_node::reports(pool, 10).await?;
    assert_eq!(stored.len(), 1);
    assert_eq!(
        (
            stored[0].node_id,
            stored[0].verdict.as_str(),
            stored[0].reason.as_str()
        ),
        (node_id, "lost", "node_deleted")
    );
    let stored_total: u64 = stored[0].counts.values().sum();
    assert_eq!(stored_total, total);
    Ok(())
}
