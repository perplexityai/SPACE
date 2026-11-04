use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kube::client::{Body as KubeBody, ClientBuilder};
use pplx_space_common_core::kubernetes::NODE_GROUP_LABEL;
use pplx_space_common_core::types::{NodeGroupId, NodeId, SubstrateKind};
use pplx_space_db_test_fixtures::{
    TestDbResult, try_pg_test_db, try_pg_test_db_with_max_connections,
};
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, PgPool};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tonic::codegen::http::{Request, Response, StatusCode};

use super::{
    LIST_TIMEOUT, MirrorBackend, MirrorError, MirrorOutcome, ObservedNode, QUERY_TIMEOUT,
    ReconcilerConfig, WRITE_STATEMENT_TIMEOUT, WriteBounds, apply_mirror, apply_mirror_within,
    list_labeled_nodes, load_node_groups, mirror_node_groups, run_loop, store,
};

type TestResult<T = ()> = TestDbResult<T>;

async fn insert_node(pool: &PgPool, k8s_node_name: &str) -> TestResult<NodeId> {
    insert_node_with_role(pool, k8s_node_name, "worker").await
}

async fn insert_node_with_role(
    pool: &PgPool,
    k8s_node_name: &str,
    role: &str,
) -> TestResult<NodeId> {
    insert_node_row(pool, k8s_node_name, role, SubstrateKind::Kubernetes).await
}

async fn insert_node_row(
    pool: &PgPool,
    k8s_node_name: &str,
    role: &str,
    managed_by: SubstrateKind,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, $2, $3, 'ready')",
    )
    .bind(node_id)
    .bind(role)
    .bind(managed_by)
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)")
        .bind(node_id)
        .bind(k8s_node_name)
        .execute(pool)
        .await?;
    Ok(node_id)
}

async fn insert_group(pool: &PgPool, name: &str) -> TestResult<NodeGroupId> {
    let group_id = NodeGroupId::new();
    sqlx::query("INSERT INTO node_groups (id, name) VALUES ($1, $2)")
        .bind(group_id)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(group_id)
}

/// [`apply_mirror`] with no shutdown in sight, so it always runs.
async fn apply(
    pool: &PgPool,
    node_names: &[String],
    group_ids: &[Option<NodeGroupId>],
) -> TestResult<u64> {
    match apply_mirror(pool, node_names, group_ids, &CancellationToken::new()).await {
        Some(written) => Ok(written?),
        None => Err("an uncancelled write is always sent".into()),
    }
}

async fn mirrored_group(pool: &PgPool, node_id: NodeId) -> TestResult<Option<NodeGroupId>> {
    Ok(sqlx::query_scalar::<_, Option<NodeGroupId>>(
        "SELECT node_group_id FROM nodes WHERE id = $1",
    )
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn mirror_sets_clears_and_noops() -> TestResult {
    let Some(fixture) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let labeled = insert_node(pool, "ip-10-0-0-1.ec2.internal").await?;
    let unlabeled = insert_node(pool, "ip-10-0-0-2.ec2.internal").await?;

    // First pass: labeled node gains the group, unlabeled stays NULL.
    let names = vec![
        "ip-10-0-0-1.ec2.internal".to_string(),
        "ip-10-0-0-2.ec2.internal".to_string(),
    ];
    let updated = apply(pool, &names, &[Some(group), None]).await?;
    assert_eq!(updated, 1, "only the labeled node changes");
    assert_eq!(mirrored_group(pool, labeled).await?, Some(group));
    assert_eq!(mirrored_group(pool, unlabeled).await?, None);

    // Steady state: identical input writes nothing.
    let updated = apply(pool, &names, &[Some(group), None]).await?;
    assert_eq!(updated, 0, "steady-state tick must be a no-op");

    // Label removed: the mirror clears back to NULL (read everywhere as
    // the default generic group).
    let updated = apply(pool, &names, &[None, None]).await?;
    assert_eq!(updated, 1);
    assert_eq!(mirrored_group(pool, labeled).await?, None);

    // A node with no node_k8s mapping is simply skipped.
    let updated = apply(
        pool,
        &["ip-10-9-9-9.ec2.internal".to_string()],
        &[Some(group)],
    )
    .await?;
    assert_eq!(updated, 0);
    Ok(())
}

/// The mirror stamps only Kubernetes-managed rows: a row the other substrate
/// manages isn't described by any kube Node label, even one whose name a
/// `node_k8s` mapping shares.
#[tokio::test(flavor = "current_thread")]
async fn mirror_only_stamps_kubernetes_rows() -> TestResult {
    let Some(fixture) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let host = "ip-10-0-0-12.ec2.internal";
    let other = insert_node_row(pool, host, "worker", SubstrateKind::Space).await?;

    let updated = apply(pool, &[host.to_string()], &[Some(group)]).await?;
    assert_eq!(updated, 0, "no Kubernetes-managed row to stamp");
    assert_eq!(mirrored_group(pool, other).await?, None);
    Ok(())
}

/// Node ids are role-scoped, so one Kubernetes Node may back several
/// `nodes` rows (worker + builder) sharing a `node_k8s.k8s_node_name`.
/// Node groups are worker pools — the mirror must stamp only the worker
/// row, or a co-hosted builder row would inflate `nodes_count` and trip
/// the group residue guards.
#[tokio::test(flavor = "current_thread")]
async fn mirror_only_stamps_worker_rows() -> TestResult {
    let Some(fixture) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let host = "ip-10-0-0-3.ec2.internal";
    let worker = insert_node_with_role(pool, host, "worker").await?;
    let builder = insert_node_with_role(pool, host, "builder").await?;

    let updated = apply(pool, &[host.to_string()], &[Some(group)]).await?;
    assert_eq!(updated, 1, "only the worker row changes");
    assert_eq!(mirrored_group(pool, worker).await?, Some(group));
    assert_eq!(
        mirrored_group(pool, builder).await?,
        None,
        "the co-hosted builder row must not join the worker group"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    List,
    Groups,
    Apply,
}

#[derive(Debug, Clone, Copy)]
enum Fault {
    /// The step never completes: an unresponsive apiserver or a wedged
    /// connection.
    Hang,
    /// The step returns its ordinary error.
    Fail,
    /// The step succeeds after this long.
    Slow(Duration),
}

/// One `apply_mirror` call's arguments: node names and their resolved groups.
type AppliedMirror = (Vec<String>, Vec<Option<NodeGroupId>>);

/// A [`MirrorBackend`] serving fixed data, with at most one step faulted.
#[derive(Default)]
struct FakeBackend {
    nodes: Vec<ObservedNode>,
    groups: HashMap<String, NodeGroupId>,
    rows_updated: u64,
    fault: Option<(Step, Fault)>,
    /// Notified when the faulted step is entered, so a test can cancel
    /// mid-step.
    fault_entered: Arc<Notify>,
    applied: Arc<Mutex<Vec<AppliedMirror>>>,
}

impl FakeBackend {
    fn faulted(step: Step, fault: Fault) -> Self {
        Self {
            nodes: vec![node("ip-10-0-0-1.ec2.internal", None)],
            fault: Some((step, fault)),
            ..Self::default()
        }
    }

    fn applied(&self) -> Vec<AppliedMirror> {
        self.applied.lock().expect("applied lock").clone()
    }

    async fn run_step<T>(&self, step: Step, value: T) -> Result<T, MirrorError> {
        let Some((_, fault)) = self.fault.filter(|(faulted, _)| *faulted == step) else {
            return Ok(value);
        };
        self.fault_entered.notify_one();
        match fault {
            Fault::Hang => std::future::pending().await,
            Fault::Fail => Err(match step {
                Step::List => MirrorError::Kube("synthetic apiserver failure".to_string()),
                Step::Groups | Step::Apply => MirrorError::Db(sqlx::Error::PoolTimedOut),
            }),
            Fault::Slow(delay) => {
                tokio::time::sleep(delay).await;
                Ok(value)
            }
        }
    }
}

impl MirrorBackend for FakeBackend {
    async fn list_nodes(&self) -> Result<Vec<ObservedNode>, MirrorError> {
        self.run_step(Step::List, self.nodes.clone()).await
    }

    async fn node_groups(&self) -> Result<HashMap<String, NodeGroupId>, MirrorError> {
        self.run_step(Step::Groups, self.groups.clone()).await
    }

    /// Shutdown before the UPDATE is sent writes nothing, as in production;
    /// the faulted step stands for the sent UPDATE, which shutdown never
    /// abandons.
    async fn apply_mirror(
        &self,
        node_names: &[String],
        group_ids: &[Option<NodeGroupId>],
        shutdown_token: &CancellationToken,
    ) -> Option<Result<u64, MirrorError>> {
        if shutdown_token.is_cancelled() {
            return None;
        }
        let written = self.run_step(Step::Apply, self.rows_updated).await;
        if written.is_ok() {
            self.applied
                .lock()
                .expect("applied lock")
                .push((node_names.to_vec(), group_ids.to_vec()));
        }
        Some(written)
    }
}

fn node(name: &str, node_group: Option<&str>) -> ObservedNode {
    ObservedNode {
        name: name.to_string(),
        node_group: node_group.map(str::to_string),
    }
}

/// Far longer than any bound the tick carries, so a tick that outlives it is
/// unbounded rather than slow.
const NEVER: Duration = Duration::from_secs(3600);

/// Run one uncancelled tick on the paused clock; returns its result and how
/// long it took.
async fn timed_tick(backend: &FakeBackend) -> (Result<MirrorOutcome, MirrorError>, Duration) {
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        NEVER,
        mirror_node_groups(backend, &CancellationToken::new()),
    )
    .await
    .expect("the tick must be bounded; it ran for an hour without returning")
    .expect("an uncancelled tick always returns a result");
    (result, started.elapsed())
}

/// The faulted step, how it fails, how long the tick takes to fail, and which
/// error it must report.
type FailureCase = (Step, Fault, Duration, fn(&MirrorError) -> bool);

/// A hung read fails the tick once its own bound elapses, and an ordinary
/// failure fails it at once; either way the tick reports which step failed
/// and nothing is written. The write's bounds live in [`apply_mirror`] and are
/// covered against Postgres below.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn failed_or_hung_step_fails_the_tick_without_writing() {
    let cases: [FailureCase; 5] = [
        (Step::List, Fault::Hang, LIST_TIMEOUT, |err| {
            matches!(err, MirrorError::ListTimedOut)
        }),
        (Step::Groups, Fault::Hang, QUERY_TIMEOUT, |err| {
            matches!(err, MirrorError::QueryTimedOut("node_groups read"))
        }),
        (Step::List, Fault::Fail, Duration::ZERO, |err| {
            matches!(err, MirrorError::Kube(_))
        }),
        (Step::Groups, Fault::Fail, Duration::ZERO, |err| {
            matches!(err, MirrorError::Db(_))
        }),
        (Step::Apply, Fault::Fail, Duration::ZERO, |err| {
            matches!(err, MirrorError::Db(_))
        }),
    ];
    for (step, fault, bound, expected) in cases {
        let backend = FakeBackend::faulted(step, fault);
        let (result, elapsed) = timed_tick(&backend).await;
        let err = result.expect_err("a failed step fails the tick");
        assert!(expected(&err), "{step:?} {fault:?}: unexpected error {err}");
        assert_eq!(elapsed, bound, "{step:?} {fault:?}");
        assert!(backend.applied().is_empty(), "{step:?} {fault:?}: no write");
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn completed_tick_resolves_each_label_and_writes_the_fleet_once() {
    let known = NodeGroupId::new();
    let backend = FakeBackend {
        nodes: vec![
            node("ip-10-0-0-1.ec2.internal", Some("org-a-dedicated")),
            node("ip-10-0-0-2.ec2.internal", Some("org-b-missing")),
            node("ip-10-0-0-3.ec2.internal", None),
        ],
        groups: HashMap::from([("org-a-dedicated".to_string(), known)]),
        rows_updated: 2,
        ..FakeBackend::default()
    };

    let (result, _) = timed_tick(&backend).await;
    let outcome = result.expect("tick completes");
    assert_eq!(
        (outcome.observed, outcome.unknown_groups, outcome.updated),
        (3, 1, 2)
    );
    assert_eq!(
        backend.applied(),
        [(
            vec![
                "ip-10-0-0-1.ec2.internal".to_string(),
                "ip-10-0-0-2.ec2.internal".to_string(),
                "ip-10-0-0-3.ec2.internal".to_string(),
            ],
            vec![Some(known), None, None],
        )],
        "known label resolves, unknown and absent labels mirror NULL"
    );
}

/// An empty fleet completes without issuing the UPDATE. The write is faulted
/// to hang, so calling it would fail the test.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn empty_fleet_completes_without_writing() {
    let backend = FakeBackend {
        fault: Some((Step::Apply, Fault::Hang)),
        ..FakeBackend::default()
    };
    let (result, elapsed) = timed_tick(&backend).await;
    assert_eq!(result.expect("tick completes").observed, 0);
    assert_eq!(elapsed, Duration::ZERO);
}

/// Shutdown mid-tick stops the loop at once during a read, which writes
/// nothing, but waits for a write that has been sent: a dropped statement
/// keeps running in Postgres, and a clean drain would then release the lease
/// with the write still able to land after the next leader's.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn shutdown_mid_tick_abandons_reads_but_waits_for_the_write() {
    let write_takes = Duration::from_secs(3);
    for (step, fault, stops_after, writes) in [
        (Step::List, Fault::Hang, Duration::ZERO, 0),
        (Step::Groups, Fault::Hang, Duration::ZERO, 0),
        (Step::Apply, Fault::Slow(write_takes), write_takes, 1),
    ] {
        let backend = FakeBackend::faulted(step, fault);
        let fault_entered = backend.fault_entered.clone();
        let applied = backend.applied.clone();
        let shutdown_token = CancellationToken::new();
        let config = ReconcilerConfig {
            enabled: true,
            ..ReconcilerConfig::default()
        };

        let started = tokio::time::Instant::now();
        let run = run_loop(config, backend, shutdown_token.clone());
        let cancel = async {
            fault_entered.notified().await;
            shutdown_token.cancel();
        };
        tokio::time::timeout(NEVER, async { tokio::join!(run, cancel) })
            .await
            .expect("run_loop must return once shutdown is requested");
        assert_eq!(started.elapsed(), stops_after, "{step:?}");
        assert_eq!(
            applied.lock().expect("applied lock").len(),
            writes,
            "{step:?}"
        );
    }
}

/// The two-connection pool these tests size their setups by.
async fn two_connection_db() -> TestResult<Option<pplx_space_db_test_fixtures::TestDb>> {
    try_pg_test_db_with_max_connections(2).await
}

/// Hold `node`'s row lock in a transaction of its own, so a write to it waits.
async fn lock_row(
    pool: &PgPool,
    node: NodeId,
) -> TestResult<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut blocker = pool.begin().await?;
    sqlx::query("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE")
        .bind(node)
        .execute(&mut *blocker)
        .await?;
    Ok(blocker)
}

/// A write that can't finish is cancelled by Postgres itself at
/// [`WRITE_STATEMENT_TIMEOUT`], before the client-side backstop, so its
/// outcome is known: rolled back.
#[tokio::test(flavor = "current_thread")]
async fn blocked_write_is_cancelled_server_side_and_rolled_back() -> TestResult {
    let Some(fixture) = two_connection_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let host = "ip-10-0-0-4.ec2.internal";
    let node = insert_node(pool, host).await?;

    let blocker = lock_row(pool, node).await?;
    let started = std::time::Instant::now();
    let written = apply_mirror(
        pool,
        &[host.to_string()],
        &[Some(group)],
        &CancellationToken::new(),
    )
    .await;
    let elapsed = started.elapsed();
    // The failed write's connection is closed rather than returned to the
    // pool, leaving only the blocker's, which is still checked out.
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        while pool.size() > 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        closed.is_ok() && pool.num_idle() == 0,
        "the failed write's connection went back to the pool"
    );
    blocker.rollback().await?;

    let Some(Err(MirrorError::Db(db_err))) = &written else {
        panic!("Postgres must fail the write before the backstop: {written:?}");
    };
    assert_eq!(
        db_err
            .as_database_error()
            .and_then(|db| db.code())
            .as_deref(),
        Some("57014"),
        "Postgres cancelled the statement: {db_err}"
    );
    assert!(
        elapsed >= WRITE_STATEMENT_TIMEOUT && elapsed < WRITE_STATEMENT_TIMEOUT * 2,
        "cancelled by the write's own server-side bound: {elapsed:?}"
    );
    assert_eq!(mirrored_group(pool, node).await?, None, "rolled back");

    Ok(())
}

/// A write transaction whose next message doesn't arrive within its idle
/// bound, as when the network stops delivering, is ended by Postgres and
/// commits nothing, whether the gap comes before the UPDATE or
/// before the COMMIT. This is what lets the backstop drop a write it no longer
/// has an answer for.
#[tokio::test(flavor = "current_thread")]
async fn idle_write_transaction_is_ended_server_side() -> TestResult {
    let Some(fixture) = two_connection_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let host = "ip-10-0-0-6.ec2.internal";
    let node = insert_node(pool, host).await?;
    // Bounded real-time waits: the behavior under test is Postgres's own timer,
    // shortened here so the test doesn't sit out the production bound.
    let idle_timeout = Duration::from_secs(1);
    let gap = idle_timeout + Duration::from_millis(500);

    for idle_before_commit in [false, true] {
        let mut conn = pool.acquire().await?;
        let mut tx =
            store::begin_mirror_write(&mut conn, WRITE_STATEMENT_TIMEOUT, idle_timeout).await?;
        if !idle_before_commit {
            tokio::time::sleep(gap).await;
        }
        let update =
            store::mirror_worker_groups(&mut tx, &[host.to_string()], &[Some(group)]).await;
        let committed = if idle_before_commit {
            assert!(update.is_ok(), "a prompt UPDATE runs: {update:?}");
            tokio::time::sleep(gap).await;
            tx.commit().await
        } else {
            assert!(session_ended(&update), "the session was ended: {update:?}");
            continue;
        };
        assert!(
            session_ended(&committed),
            "the session was ended: {committed:?}"
        );
    }
    assert_eq!(mirrored_group(pool, node).await?, None, "nothing committed");
    Ok(())
}

/// Whether `result` failed because Postgres ended the session for idling in
/// its transaction: `25P03` if the client read the server's notice, an I/O
/// error if it found the socket already closed.
fn session_ended<T>(result: &Result<T, sqlx::Error>) -> bool {
    match result {
        Err(sqlx::Error::Database(db)) => db.code().as_deref() == Some("25P03"),
        Err(sqlx::Error::Io(_)) => true,
        _ => false,
    }
}

/// Shutdown while a sent UPDATE waits on a row lock doesn't abandon it: once
/// the lock is released the write commits and reports its rows. The write's
/// bounds are local to its transaction and don't stay on pooled connections.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_during_a_sent_write_lets_it_commit() -> TestResult {
    let Some(fixture) = two_connection_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let host = "ip-10-0-0-7.ec2.internal";
    let node = insert_node(pool, host).await?;
    let baseline = timeout_settings(pool).await?;
    // Outside the pool, which the blocker and the write fill.
    let mut observer = sqlx::PgConnection::connect(&fixture.database_url).await?;

    let blocker = lock_row(pool, node).await?;
    let shutdown_token = CancellationToken::new();
    let (names, groups) = ([host.to_string()], [Some(group)]);
    let (written, released) = tokio::join!(
        apply_mirror(pool, &names, &groups, &shutdown_token),
        async {
            // Cancel only once the UPDATE is sent and waiting on the lock,
            // then release the lock so it can finish.
            let waited = wait_for_a_lock_wait(&mut observer).await;
            shutdown_token.cancel();
            blocker.rollback().await?;
            waited
        },
    );
    released?;

    assert!(
        matches!(written, Some(Ok(1))),
        "the sent write ran to its outcome: {written:?}"
    );
    assert_eq!(mirrored_group(pool, node).await?, Some(group));

    // Hold both of the pool's connections so each one is checked.
    let mut first = pool.acquire().await?;
    let mut second = pool.acquire().await?;
    for conn in [&mut first, &mut second] {
        assert_eq!(
            timeout_settings(&mut **conn).await?,
            baseline,
            "a committed write's bounds leaked to the pool"
        );
    }
    Ok(())
}

async fn timeout_settings<'e>(executor: impl sqlx::PgExecutor<'e>) -> TestResult<(String, String)> {
    Ok(sqlx::query_as(
        "SELECT current_setting('statement_timeout'),
                current_setting('idle_in_transaction_session_timeout')",
    )
    .fetch_one(executor)
    .await?)
}

/// Wait until some session in the test database is waiting on a lock, failing
/// after a few seconds if none ever does.
async fn wait_for_a_lock_wait(observer: &mut sqlx::PgConnection) -> TestResult {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if std::time::Instant::now() >= deadline {
            return Err("no session started waiting on a lock".into());
        }
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity
              WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(&mut *observer)
        .await?;
        if waiting > 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A `node_groups` read cut short mid-query closes its connection rather than
/// returning it to the pool, where its release check would wait on the
/// abandoned query. The read is held by a table lock.
#[tokio::test(flavor = "current_thread")]
async fn abandoned_read_closes_its_connection() -> TestResult {
    let Some(fixture) = two_connection_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    // Outside the pool, which the table lock and the read fill.
    let mut observer = sqlx::PgConnection::connect(&fixture.database_url).await?;

    let mut blocker = pool.begin().await?;
    sqlx::query("LOCK TABLE node_groups IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await?;
    // Boxed, so dropping it below drops the read itself.
    let mut read = Box::pin(load_node_groups(pool));
    let cut_short = tokio::select! {
        result = &mut read => Err(format!("the read wasn't blocked: {result:?}")),
        waited = wait_for_a_lock_wait(&mut observer) => waited.map_err(|err| err.to_string()),
    };
    cut_short?;
    drop(read);

    // Only the blocker's connection is left, still checked out.
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        while pool.size() > 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        closed.is_ok() && pool.num_idle() == 0,
        "the abandoned read's connection went back to the pool"
    );
    blocker.rollback().await?;
    Ok(())
}

/// Shutdown while the write is still waiting for a connection abandons it
/// with nothing written, rather than holding the drain for the pool's acquire
/// timeout.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_before_the_update_is_sent_writes_nothing() -> TestResult {
    let Some(fixture) = two_connection_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let host = "ip-10-0-0-5.ec2.internal";
    let node = insert_node(pool, host).await?;

    // Exhaust the pool so the write's acquire waits.
    let first = pool.acquire().await?;
    let second = pool.acquire().await?;
    let shutdown_token = CancellationToken::new();
    let (names, groups) = ([host.to_string()], [Some(group)]);
    let (written, ()) = tokio::join!(
        apply_mirror(pool, &names, &groups, &shutdown_token),
        async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            shutdown_token.cancel();
        },
    );
    drop((first, second));

    assert!(
        written.is_none(),
        "abandoned before the UPDATE was sent: {written:?}"
    );
    assert_eq!(mirrored_group(pool, node).await?, None, "nothing written");
    Ok(())
}

/// Each request a fake apiserver answered: its path and query, and its
/// `Accept` header.
type SeenRequests = Arc<Mutex<Vec<(String, String)>>>;

/// A fake apiserver answering every request with `reply`.
fn fake_apiserver(reply: (StatusCode, Value)) -> (kube::Client, SeenRequests) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let service = tower::service_fn(move |request: Request<KubeBody>| {
        let recorded = Arc::clone(&recorded);
        let reply = reply.clone();
        async move {
            let target = request
                .uri()
                .path_and_query()
                .map(ToString::to_string)
                .unwrap_or_default();
            let accept = request
                .headers()
                .get("accept")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            recorded
                .lock()
                .map_err(|_| "request log poisoned")?
                .push((target, accept));
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                Response::builder()
                    .status(reply.0)
                    .header("content-type", "application/json")
                    .body(KubeBody::from(serde_json::to_vec(&reply.1)?))?,
            )
        }
    });
    (ClientBuilder::new(service, "default").build(), seen)
}

/// The LIST is a metadata-only, label-selected LIST of Nodes, and it keeps
/// each named Node with its node-group label, if any, dropping a Node the
/// apiserver returned without a name.
#[tokio::test(flavor = "current_thread")]
async fn list_labeled_nodes_reads_names_and_group_labels_from_a_metadata_list() -> TestResult {
    let item = |metadata: Value| {
        json!({
            "apiVersion": "meta.k8s.io/v1",
            "kind": "PartialObjectMetadata",
            "metadata": metadata,
        })
    };
    let list = json!({
        "apiVersion": "meta.k8s.io/v1",
        "kind": "PartialObjectMetadataList",
        "metadata": {},
        "items": [
            item(json!({
                "name": "ip-10-0-0-1.ec2.internal",
                "labels": { NODE_GROUP_LABEL: "org-a-dedicated", "pool": "sandbox" },
            })),
            item(json!({ "name": "ip-10-0-0-2.ec2.internal", "labels": { "pool": "sandbox" } })),
            item(json!({ "name": "ip-10-0-0-3.ec2.internal" })),
            item(json!({ "labels": { NODE_GROUP_LABEL: "org-a-dedicated" } })),
        ],
    });
    let (client, seen) = fake_apiserver((StatusCode::OK, list));

    let nodes = list_labeled_nodes(client, "pool=sandbox").await?;
    assert_eq!(
        nodes
            .iter()
            .map(|node| (node.name.as_str(), node.node_group.as_deref()))
            .collect::<Vec<_>>(),
        [
            ("ip-10-0-0-1.ec2.internal", Some("org-a-dedicated")),
            ("ip-10-0-0-2.ec2.internal", None),
            ("ip-10-0-0-3.ec2.internal", None),
        ]
    );

    let seen = seen.lock().map_err(|_| "request log poisoned")?;
    let [(target, accept)] = seen.as_slice() else {
        return Err(format!("expected one request, saw {seen:?}").into());
    };
    assert!(
        target.starts_with("/api/v1/nodes?") && target.contains("labelSelector=pool%3Dsandbox"),
        "a label-selected Node LIST: {target}"
    );
    assert!(
        accept.contains("as=PartialObjectMetadataList"),
        "a metadata-only LIST: {accept}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn list_labeled_nodes_reports_an_apiserver_failure_as_a_list_error() {
    let status = json!({
        "apiVersion": "v1",
        "kind": "Status",
        "metadata": {},
        "status": "Failure",
        "message": "injected 503",
        "reason": "ServiceUnavailable",
        "code": 503,
    });
    let (client, _) = fake_apiserver((StatusCode::SERVICE_UNAVAILABLE, status));
    let result = list_labeled_nodes(client, "pool=sandbox").await;
    assert!(matches!(result, Err(MirrorError::Kube(_))), "{result:?}");
}

/// Bounds short enough to fire before Postgres's own, which otherwise always
/// answer first on a live connection.
const SHORT: Duration = Duration::from_millis(500);

/// Wait up to a few seconds for the pool to hold only `expected` connections.
async fn pool_settles_at(pool: &PgPool, expected: u32) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.size() > expected {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok()
}

/// The write's client-side backstop fails a write still in flight, and the
/// write's connection is closed, so the UPDATE it abandoned never commits.
#[tokio::test(flavor = "current_thread")]
async fn write_backstop_fails_a_write_still_in_flight() -> TestResult {
    let Some(fixture) = two_connection_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let host = "ip-10-0-0-9.ec2.internal";
    let node = insert_node(pool, host).await?;

    let blocker = lock_row(pool, node).await?;
    let bounds = WriteBounds {
        backstop: SHORT,
        ..WriteBounds::DEFAULT
    };
    let started = std::time::Instant::now();
    let written = apply_mirror_within(
        pool,
        &[host.to_string()],
        &[Some(group)],
        &CancellationToken::new(),
        bounds,
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(
            written,
            Some(Err(MirrorError::QueryTimedOut("bulk update")))
        ),
        "{written:?}"
    );
    assert!(
        elapsed < WRITE_STATEMENT_TIMEOUT,
        "the client-side bound fired, not Postgres's: {elapsed:?}"
    );
    assert!(
        pool_settles_at(pool, 1).await && pool.num_idle() == 0,
        "the abandoned write's connection went back to the pool"
    );
    blocker.rollback().await?;
    // With the lock gone the abandoned UPDATE runs, then the ROLLBACK and
    // Terminate queued behind it, so nothing commits. The pool check above is
    // what a regression trips: a connection returned to the pool would still
    // sit behind the blocked UPDATE.
    tokio::time::sleep(SHORT).await;
    assert_eq!(mirrored_group(pool, node).await?, None, "nothing committed");
    Ok(())
}

/// The write's setup budget fails a setup still waiting, here a prepare held
/// by a table lock, and the connection is closed rather than pooled.
#[tokio::test(flavor = "current_thread")]
async fn write_setup_budget_fails_a_setup_still_waiting() -> TestResult {
    let Some(fixture) = two_connection_db().await? else {
        return Ok(());
    };
    let pool = &fixture.pool;
    let group = insert_group(pool, "org-a-dedicated").await?;
    let host = "ip-10-0-0-10.ec2.internal";
    let node = insert_node(pool, host).await?;

    let mut blocker = pool.begin().await?;
    sqlx::query("LOCK TABLE nodes IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await?;
    let bounds = WriteBounds {
        setup: SHORT,
        ..WriteBounds::DEFAULT
    };
    let started = std::time::Instant::now();
    let written = apply_mirror_within(
        pool,
        &[host.to_string()],
        &[Some(group)],
        &CancellationToken::new(),
        bounds,
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(
            written,
            Some(Err(MirrorError::QueryTimedOut("bulk update setup")))
        ),
        "{written:?}"
    );
    assert!(
        elapsed < WRITE_STATEMENT_TIMEOUT,
        "the client-side bound fired, not Postgres's: {elapsed:?}"
    );
    assert!(
        pool_settles_at(pool, 1).await && pool.num_idle() == 0,
        "the abandoned setup's connection went back to the pool"
    );
    blocker.rollback().await?;
    assert_eq!(mirrored_group(pool, node).await?, None, "nothing written");
    Ok(())
}

/// A TCP proxy to the test Postgres that can hold Postgres's replies, so a
/// test can land between a message reaching the server and its reply reaching
/// the client. Messages to the server always pass.
struct ReplyHoldingProxy {
    port: u16,
    hold: tokio::sync::watch::Sender<bool>,
    accept_loop: tokio::task::JoinHandle<()>,
}

impl ReplyHoldingProxy {
    async fn start(upstream_host: String, upstream_port: u16) -> std::io::Result<Self> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let (hold, held) = tokio::sync::watch::channel(false);
        let accept_loop = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let upstream = (upstream_host.clone(), upstream_port);
                let mut held = held.clone();
                tokio::spawn(async move {
                    let Ok(server) = tokio::net::TcpStream::connect(upstream).await else {
                        return;
                    };
                    let (mut from_client, mut to_client) = client.into_split();
                    let (mut from_server, mut to_server) = server.into_split();
                    let requests = tokio::io::copy(&mut from_client, &mut to_server);
                    let replies = async {
                        let mut buf = vec![0_u8; 8192];
                        loop {
                            let read = from_server.read(&mut buf).await?;
                            if read == 0 {
                                return Ok::<_, std::io::Error>(());
                            }
                            held.wait_for(|holding| !holding)
                                .await
                                .map_err(std::io::Error::other)?;
                            to_client.write_all(&buf[..read]).await?;
                        }
                    };
                    let _ = tokio::join!(requests, replies);
                });
            }
        });
        Ok(Self {
            port,
            hold,
            accept_loop,
        })
    }

    fn hold_replies(&self, holding: bool) {
        self.hold.send_replace(holding);
    }
}

impl Drop for ReplyHoldingProxy {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

/// The `pg_stat_activity` state of backend `pid`, `None` once it has exited.
async fn backend_state(
    observer: &mut sqlx::PgConnection,
    pid: i32,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT state FROM pg_stat_activity WHERE pid = $1")
        .bind(pid)
        .fetch_optional(observer)
        .await
}

/// Shutdown landing after BEGIN reached Postgres but before its reply reached
/// the client: sqlx doesn't track that transaction, so a connection returned to
/// the pool would carry it into the next borrower's queries. The write closes
/// the connection instead, which ends the transaction with its session.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_mid_begin_ends_the_open_transaction() -> TestResult {
    let Some(fixture) = try_pg_test_db().await? else {
        return Ok(());
    };
    let upstream: PgConnectOptions = fixture.database_url.parse()?;
    let proxy =
        ReplyHoldingProxy::start(upstream.get_host().to_string(), upstream.get_port()).await?;
    // One connection, handed out without a ping: a ping would wait on the held
    // replies itself, and the hold has to catch the BEGIN.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .connect_with(upstream.clone().host("127.0.0.1").port(proxy.port))
        .await?;
    let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    let mut observer = sqlx::PgConnection::connect(&fixture.database_url).await?;

    // The connection's release ping runs through the proxy too; let it finish,
    // so the hold catches the write's BEGIN rather than the ping.
    let idle = tokio::time::timeout(Duration::from_secs(5), async {
        while pool.num_idle() < 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(idle.is_ok(), "the proxied connection never went idle");
    proxy.hold_replies(true);
    let shutdown_token = CancellationToken::new();
    let (names, groups) = (
        ["ip-10-0-0-11.ec2.internal".to_string()],
        [Some(NodeGroupId::new())],
    );
    let (written, waited) = tokio::join!(
        apply_mirror(&pool, &names, &groups, &shutdown_token),
        async {
            // Postgres has begun the transaction; the client hasn't heard.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let state = backend_state(&mut observer, backend).await?;
                if state.as_deref() == Some("idle in transaction") {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    return Err::<(), Box<dyn std::error::Error + Send + Sync>>(
                        format!("BEGIN never reached the server: {state:?}").into(),
                    );
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            shutdown_token.cancel();
            Ok(())
        },
    );
    waited?;
    assert!(
        written.is_none(),
        "abandoned before the UPDATE was sent: {written:?}"
    );

    // Let the held replies through, as a recovered network would.
    proxy.hold_replies(false);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let state = backend_state(&mut observer, backend).await?;
        if state.is_none() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the abandoned connection survived, its session {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}
