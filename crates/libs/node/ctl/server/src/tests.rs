use super::*;

#[test]
fn poll_interval_is_bounded_by_what_the_cluster_window_covers() {
    let max = MAX_NODE_CTL_POLL_INTERVAL;
    assert_eq!(bound_poll_interval(Duration::ZERO), Duration::from_secs(1));
    assert_eq!(
        bound_poll_interval(DEFAULT_NODE_CTL_POLL_INTERVAL),
        DEFAULT_NODE_CTL_POLL_INTERVAL
    );
    assert_eq!(bound_poll_interval(max), max);
    assert_eq!(bound_poll_interval(max + Duration::from_secs(1)), max);
}

#[tokio::test]
async fn mutex_timeout_never_transfers_ownership() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mutex_path = dir.path().join("service.mutex");
    let predecessor_mutex = match ServiceMutex::acquire_with_timeout(
        mutex_path.clone(),
        &CancellationToken::new(),
        Duration::from_secs(2),
    )
    .await
    .expect("acquire predecessor mutex")
    {
        AcquireOutcome::Acquired(mutex) => mutex,
        AcquireOutcome::PassiveExit => panic!("predecessor mutex acquisition cancelled"),
    };

    let error = ServiceMutex::acquire_with_timeout(
        &mutex_path,
        &CancellationToken::new(),
        Duration::from_millis(30),
    )
    .await
    .expect_err("successor must time out while predecessor owns service");
    assert!(matches!(
        error,
        pplx_space_common_svc::ServiceMutexAcquireError::Timeout { .. }
    ));
    drop(predecessor_mutex);
}

#[test]
fn completion_isolates_sandbox_desired_state_failures_by_ref() {
    let succeeded_id = "019e6370-cfc9-7a43-9337-399c7b51a1d9";
    let failed_id = "019e6370-cfc9-7a43-9337-399c7b51a1da";
    let refs = vec![
        dirty_state::FacetRef::new(dirty_state::FACET_SANDBOX_DESIRED_STATE, Some(succeeded_id)),
        dirty_state::FacetRef::new(dirty_state::FACET_SANDBOX_DESIRED_STATE, Some(failed_id)),
    ];
    let report = BTreeMap::from([(
        facets::FacetKind::SandboxDesiredState,
        Ok(InboundFacetResult {
            stats: ReconcileStats::default(),
            element_errors: BTreeMap::from([(failed_id.to_string(), "apply failed".to_string())]),
        }),
    )]);

    let (succeeded, failed) = partition_completed_refs(refs, &report);

    assert_eq!(succeeded.len(), 1);
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].error, "apply failed");
}

#[test]
fn completion_fails_full_scope_ref_when_any_element_failed() {
    let failed_id = "019e6370-cfc9-7a43-9337-399c7b51a1da";
    let refs = vec![dirty_state::FacetRef::new(
        dirty_state::FACET_SANDBOX_DESIRED_STATE,
        None::<String>,
    )];
    let report = BTreeMap::from([(
        facets::FacetKind::SandboxDesiredState,
        Ok(InboundFacetResult {
            stats: ReconcileStats::default(),
            element_errors: BTreeMap::from([(failed_id.to_string(), "apply failed".to_string())]),
        }),
    )]);

    let (succeeded, failed) = partition_completed_refs(refs, &report);

    assert!(succeeded.is_empty());
    assert_eq!(failed.len(), 1);
    assert!(failed[0].error.contains("1 sandbox-desired-state item(s)"));
}

#[test]
fn completion_retries_every_ref_after_shared_dependency_failure() {
    let refs = vec![
        dirty_state::FacetRef::new(
            dirty_state::FACET_SANDBOX_DESIRED_STATE,
            Some("019e6370-cfc9-7a43-9337-399c7b51a1d9"),
        ),
        dirty_state::FacetRef::new(
            dirty_state::FACET_SANDBOX_DESIRED_STATE,
            Some("019e6370-cfc9-7a43-9337-399c7b51a1da"),
        ),
    ];
    let report = BTreeMap::from([(
        facets::FacetKind::SandboxDesiredState,
        Err("cluster API unavailable".to_string()),
    )]);

    let (succeeded, failed) = partition_completed_refs(refs, &report);

    assert!(succeeded.is_empty());
    assert_eq!(failed.len(), 2);
    assert!(
        failed
            .iter()
            .all(|failure| failure.error == "cluster API unavailable")
    );
}

#[tokio::test(start_paused = true)]
async fn facet_watchdog_fires_after_quiet_period_without_progress() {
    let progress = FacetProgress::new();
    let quiet = Duration::from_secs(90);
    let fired = tokio::time::timeout(Duration::from_secs(91), facet_stalled(progress, quiet)).await;
    assert!(fired.is_ok(), "watchdog must fire once the run goes quiet");
}

#[tokio::test(start_paused = true)]
async fn facet_watchdog_tolerates_slow_but_progressing_runs() {
    // Ten serial 30s batches = 300s total, far past the 90s quiet period.
    // The watchdog must not kill a run that keeps completing batches; only
    // a stall (no touch for a full quiet period) may end it.
    let progress = FacetProgress::new();
    let quiet = Duration::from_secs(90);
    let worker = {
        let progress = progress.clone();
        async move {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_secs(30)).await;
                progress.touch();
            }
        }
    };
    tokio::select! {
        () = facet_stalled(progress.clone(), quiet) => {
            panic!("watchdog killed a run that was making progress");
        }
        () = worker => {}
    }
    // Once batches stop, the stall is still detected within one quiet period.
    let fired = tokio::time::timeout(Duration::from_secs(91), facet_stalled(progress, quiet)).await;
    assert!(
        fired.is_ok(),
        "watchdog must still fire after progress stops"
    );
}

#[test]
fn expiring_client_serves_clones_until_max_age() {
    let now = Instant::now();
    let cached = ExpiringClient::new("client".to_string(), now, Duration::from_secs(60));
    assert_eq!(cached.get(now), Some("client".to_string()));
    assert_eq!(
        cached.get(now + Duration::from_secs(59)),
        Some("client".to_string())
    );
    assert_eq!(cached.get(now + Duration::from_secs(60)), None);
    assert_eq!(cached.get(now + Duration::from_secs(3600)), None);
}

#[tokio::test]
async fn client_cache_serves_hits_until_max_age_then_rotates() {
    let cache = ExpiringClientCache::new();
    let now = Instant::now();
    for (seconds, expected, outcome) in [
        (0, 1, ClientCacheOutcome::Rotated),
        (59, 1, ClientCacheOutcome::Hit),
        (60, 2, ClientCacheOutcome::Rotated),
    ] {
        assert_eq!(
            cache
                .get_or_rotate(
                    now + Duration::from_secs(seconds),
                    || Duration::from_secs(60),
                    || async { Ok::<_, String>(if seconds == 0 { 1 } else { 2 }) },
                )
                .await,
            Ok((expected, outcome))
        );
    }
}

#[tokio::test]
async fn client_cache_build_failure_is_not_cached() {
    let cache = ExpiringClientCache::<i32>::new();
    let now = Instant::now();
    assert_eq!(
        cache
            .get_or_rotate(
                now,
                || Duration::from_secs(60),
                || async { Err("boom".to_string()) }
            )
            .await,
        Err("boom".to_string())
    );
    assert_eq!(
        cache
            .get_or_rotate(
                now,
                || Duration::from_secs(60),
                || async { Ok::<_, String>(1) }
            )
            .await,
        Ok((1, ClientCacheOutcome::Rotated))
    );
}

#[tokio::test]
async fn expired_client_cache_rotation_is_single_flighted() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    const CALLERS: usize = 8;
    let cache = Arc::new(ExpiringClientCache::new());
    let now = Instant::now();
    let builds = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..CALLERS {
        let cache = cache.clone();
        let builds = builds.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let (client, _) = cache
                .get_or_rotate(
                    now,
                    || Duration::from_secs(60),
                    || async {
                        builds.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                        Ok::<_, String>("client".to_string())
                    },
                )
                .await
                .expect("build cannot fail");
            assert_eq!(client, "client");
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "concurrent callers observing an expired cache must build exactly one client"
    );
}

#[test]
fn jittered_max_age_varies_rotation_poll_tick() {
    // Rotation only happens when a poll tick asks for the client, so the
    // effective rotation time is the max-age rounded up to the next
    // DEFAULT_POLL_INTERVAL_SECS tick. If the jitter is narrower than one
    // poll interval, every node in the fleet quantizes to the *same* tick
    // and rotates in lockstep — the synchronized reconnect burst this
    // max-age exists to prevent.
    let poll_ms = Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS).as_millis();
    let mut rotation_ticks = BTreeSet::new();
    for _ in 0..1_000 {
        let age = jittered_max_age(CLUSTER_CLIENT_MAX_AGE, CLUSTER_CLIENT_MAX_AGE_JITTER);
        rotation_ticks.insert(age.as_millis() / poll_ms + 1);
    }
    assert!(
        rotation_ticks.len() >= 2,
        "every client expires on poll tick(s) {rotation_ticks:?}; the jitter must span \
         multiple poll intervals so the fleet's rotation ticks decohere"
    );
}

#[test]
fn jittered_max_age_stays_within_bounds() {
    let base = Duration::from_secs(60);
    let jitter = Duration::from_secs(15);
    for _ in 0..1_000 {
        let age = jittered_max_age(base, jitter);
        assert!(age >= base, "jittered age {age:?} below base {base:?}");
        assert!(
            age <= base + jitter,
            "jittered age {age:?} above base + jitter"
        );
    }
}

fn scheduling_snapshot() -> SchedulingSnapshot {
    SchedulingSnapshot {
        status: NodeSchedulingStatus::Ready,
        reason: "ready".to_string(),
        ready: 1,
        min_ready: 1,
        degraded: 0,
        quarantined: 0,
        total: 1,
        leased: 0,
        max_slots: 1,
    }
}

fn cache_disk(
    admission_state: pplx_space_node_mgr_client::proto::CacheDiskAdmissionState,
) -> pplx_space_node_mgr_client::proto::CacheDiskStatus {
    pplx_space_node_mgr_client::proto::CacheDiskStatus {
        admission_state: admission_state as i32,
        admission_reason: "disk_available_warning".to_string(),
        admission_detail: "disk available fraction below warning threshold".to_string(),
        ..Default::default()
    }
}

#[test]
fn warning_cache_disk_marks_published_node_full_for_direct_scheduling() {
    let mut snapshot = scheduling_snapshot();
    let cache_disk =
        cache_disk(pplx_space_node_mgr_client::proto::CacheDiskAdmissionState::Warning);

    NodeCtlWorker::apply_cache_disk_scheduling_effect(&mut snapshot, &cache_disk);

    assert_eq!(snapshot.status, NodeSchedulingStatus::Full);
    assert_eq!(
        snapshot.reason,
        "disk_available_warning: disk available fraction below warning threshold"
    );
}

/// When each Kubernetes node-object reconcile pass ran.
type NodeObjectPasses = Arc<Mutex<Vec<tokio::time::Instant>>>;

/// Stands in for the Kubernetes node-local controller: records the reconcile
/// passes `run_outbound_sync` gives it.
struct RecordingNodeObjects(NodeObjectPasses);

#[tonic::async_trait]
impl NodeObjectReconciles for RecordingNodeObjects {
    async fn reconcile_node_objects(&mut self) {
        self.0
            .lock()
            .expect("node-object passes lock")
            .push(tokio::time::Instant::now());
    }
}

fn pass_count(passes: &NodeObjectPasses) -> usize {
    passes.lock().expect("node-object passes lock").len()
}

/// A worker on a migrated, seeded node-ctl database with cluster sync
/// unconfigured (this test binary never sets the endpoint), so each outbound
/// facet's sync records its pass in `reconciliation_status` without a cluster.
async fn timer_test_worker(
    dir: &Path,
) -> (
    NodeCtlWorker,
    mpsc::Sender<WorkItem>,
    Arc<Mutex<Connection>>,
    NodeObjectPasses,
) {
    worker_publishing_to(
        dir,
        Arc::new(ClusterApiPublisher),
        Box::new(orphan_inventory::Observer::default()),
    )
    .await
}

async fn worker_publishing_to(
    dir: &Path,
    publisher: Arc<dyn OutboundPublisher>,
    orphan_inventory: Box<dyn OrphanInventoryObserver>,
) -> (
    NodeCtlWorker,
    mpsc::Sender<WorkItem>,
    Arc<Mutex<Connection>>,
    NodeObjectPasses,
) {
    let db_path = dir.join("node-ctl.db");
    pplx_space_sqlite::migrate("node-ctl", &db_path, &NODE_CTL_MIGRATOR)
        .await
        .expect("migrate node-ctl database");
    let conn = pplx_space_sqlite::open_rusqlite(
        &db_path,
        pplx_space_sqlite::RusqliteOpenOptions::default(),
    )
    .expect("open node-ctl database");
    init_reconciliation_status(&conn, DEFAULT_POLL_INTERVAL_SECS).expect("seed statuses");
    let db = Arc::new(Mutex::new(conn));
    let (tx, rx) = mpsc::channel(WORK_QUEUE_CAPACITY);
    let passes = NodeObjectPasses::default();
    let worker = NodeCtlWorker {
        db: db.clone(),
        node_id: "node-under-test".to_string(),
        node_role: Some(NodeRole::Worker),
        poll_interval: poll_interval(DEFAULT_POLL_INTERVAL_SECS),
        cluster_reconcile_trigger: ClusterReconcileTrigger::new(tx.clone(), "node-under-test"),
        resource_status_reader: NodeResourceStatusReader::new(),
        network_pool_status_reader: NetworkPoolStatusReader::new(),
        orphan_inventory,
        runtime_provisioning: Arc::default(),
        node_objects: Some(Box::new(RecordingNodeObjects(passes.clone()))),
        publisher,
        outbound_retry: OutboundRetry::default(),
        rx,
    };
    (worker, tx, db, passes)
}

/// The published node-ctl status names the node-status reconciler with the
/// interval stored for it in `reconciliation_status`. The cluster derives its
/// freshness window from that entry. This covers the table-to-publication
/// path; `run` seeding the table from the interval it polls at isn't covered.
#[tokio::test]
async fn published_node_ctl_status_reports_the_node_status_poll_interval() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (worker, _tx, db, _passes) = timer_test_worker(dir.path()).await;
    let configured_secs = 45;
    init_reconciliation_status(&db.lock().expect("lock node-ctl db"), configured_secs)
        .expect("reseed statuses");

    let status = worker.node_ctl_status_for_publish();
    assert_eq!(
        pplx_space_node_api_client::node_status_freshness::reported_status_poll_interval(&status),
        Some(Duration::from_secs(configured_secs)),
    );
}

/// When each outbound reconciler last completed a pass (0: never).
fn outbound_completions(db: &Arc<Mutex<Connection>>) -> BTreeMap<String, u64> {
    let outbound = [
        SANDBOX_INVENTORY_OUTBOUND_RECONCILER,
        TEMPLATE_INVENTORY_OUTBOUND_RECONCILER,
        NODE_STATUS_OUTBOUND_RECONCILER,
    ];
    read_reconciliation_statuses(db)
        .expect("read reconciliation statuses")
        .into_iter()
        .filter(|status| outbound.contains(&status.name.as_str()))
        .map(|status| (status.name, status.last_completed_at_unix_secs))
        .collect()
}

/// One poll-loop tick, through the worker's real loop, publishes the periodic
/// outbound facets — the sandbox-inventory census, template inventory and
/// node status — and runs the node's Kubernetes object reconciles on the
/// template-inventory publication. Nothing else marks those facets dirty
/// here, so the tick alone has to supply them: a `TimerTick` that stopped
/// passing `periodic_outbound_refs()`, or a Node-reconcile gate evaluated
/// before those refs join the batch, leaves the counts at zero.
#[tokio::test]
async fn a_timer_tick_publishes_the_periodic_outbound_facets_and_reconciles_node_objects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (worker, tx, db, passes) = timer_test_worker(dir.path()).await;
    assert!(
        outbound_completions(&db)
            .values()
            .all(|completed| *completed == 0),
        "a freshly seeded node has run no outbound pass"
    );
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(worker.run(shutdown.child_token()));

    tx.send(WorkItem::TimerTick {
        next_due_at: now_secs() + DEFAULT_POLL_INTERVAL_SECS.cast_signed(),
    })
    .await
    .expect("queue a timer tick");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let completions = outbound_completions(&db);
        if completions.len() == 3 && completions.values().all(|completed| *completed > 0) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the timer tick did not run every periodic outbound facet: {completions:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        pass_count(&passes),
        1,
        "the timer tick did not run the node's Kubernetes object reconciles once"
    );

    shutdown.cancel();
    task.await.expect("worker task");
}

/// The control for the test above: an outbound sync with nothing dirty and
/// no tick publishes nothing and reconciles no Node objects, so what the
/// tick did came from the tick.
#[tokio::test]
async fn an_outbound_sync_with_nothing_dirty_publishes_and_reconciles_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (worker, tx, db, passes) = timer_test_worker(dir.path()).await;
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(worker.run(shutdown.child_token()));

    tx.send(WorkItem::SyncDirty)
        .await
        .expect("queue a dirty sync");
    // The worker handles items in order: once this refresh's inbound pass has
    // recorded itself, the dirty sync before it has run.
    tx.send(WorkItem::ManualRefresh {
        refs: vec![dirty_state::FacetRef::new(
            facets::FacetKind::NetworkPolicy.name(),
            None::<String>,
        )],
    })
    .await
    .expect("queue a refresh");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while read_reconciliation_statuses(&db)
        .expect("read reconciliation statuses")
        .iter()
        .all(|status| {
            status.name != NETWORK_POLICY_RECONCILER || status.last_completed_at_unix_secs == 0
        })
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refresh never ran"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        outbound_completions(&db)
            .values()
            .all(|completed| *completed == 0),
        "an empty dirty sync published an outbound facet"
    );
    assert_eq!(pass_count(&passes), 0);

    shutdown.cancel();
    task.await.expect("worker task");
}

/// One publish the scripted cluster saw, in seconds after the worker started.
#[derive(Clone, Copy, Debug)]
struct PublishAttempt {
    facet: facets::FacetKind,
    started: f64,
    finished: f64,
}

/// How a scripted publish fails, given the facet, when its earlier publishes
/// started and when this one does; `None` publishes.
type FailureScript = fn(facets::FacetKind, &[f64], f64) -> Option<facets::OutboundError>;

fn unavailable() -> facets::OutboundError {
    facets::OutboundError::ClusterUnavailable("transport error: connection refused".to_string())
}

fn rejected() -> facets::OutboundError {
    facets::OutboundError::Other("cluster node state publish failed: permission denied".to_string())
}

/// A cluster whose publishes fail as `fails` says, on the paused clock. Each
/// publish takes `latency`. It records every publish and the most it ever saw
/// in flight for one facet.
struct ScriptedCluster {
    /// When the worker started, on the paused clock: the first tick.
    origin: std::sync::OnceLock<tokio::time::Instant>,
    latency: Duration,
    fails: FailureScript,
    attempts: Mutex<Vec<PublishAttempt>>,
    in_flight: Mutex<BTreeMap<facets::FacetKind, usize>>,
    max_in_flight: std::sync::atomic::AtomicUsize,
}

/// How long after its pass is due a publish can start. Before its first
/// publish, a pass reads the node's status from node-manager, which these
/// tests do not run, and the local gRPC retry policy retries that read for up
/// to 300 ms of the paused clock. It has nothing to do with the retry under
/// test, and only widens the upper bounds below.
const PUBLISH_LEAD: f64 = 0.5;

impl ScriptedCluster {
    fn new(latency: Duration, fails: FailureScript) -> Arc<Self> {
        Arc::new(Self {
            origin: std::sync::OnceLock::new(),
            latency,
            fails,
            attempts: Mutex::default(),
            in_flight: Mutex::default(),
            max_in_flight: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn origin(&self) -> tokio::time::Instant {
        *self.origin.get().expect("the worker has started")
    }

    fn since_origin(&self, at: tokio::time::Instant) -> f64 {
        (at - self.origin()).as_secs_f64()
    }

    fn elapsed(&self) -> f64 {
        self.since_origin(tokio::time::Instant::now())
    }

    fn attempts_of(&self, facet: facets::FacetKind) -> Vec<PublishAttempt> {
        self.attempts
            .lock()
            .expect("attempts lock")
            .iter()
            .filter(|attempt| attempt.facet == facet)
            .copied()
            .collect()
    }

    fn starts_of(&self, facet: facets::FacetKind) -> Vec<f64> {
        self.attempts_of(facet)
            .iter()
            .map(|attempt| attempt.started)
            .collect()
    }
}

#[tonic::async_trait]
impl OutboundPublisher for ScriptedCluster {
    /// Outbound only: inbound facets and reconcile claims read the
    /// process-wide endpoint, which this test binary never sets, so they
    /// still see no cluster and skip.
    fn cluster_configured(&self) -> bool {
        true
    }

    async fn publish(
        &self,
        facet: facets::FacetKind,
        _node_id: &str,
        _node_role: Option<NodeRole>,
        _scope: facets::FacetScope,
        _node_status: Option<PublishedNodeStatus>,
    ) -> Result<ReconcileStats, facets::OutboundError> {
        let started = self.elapsed();
        let earlier = self.starts_of(facet);
        {
            let mut in_flight = self.in_flight.lock().expect("in-flight lock");
            let count = in_flight.entry(facet).or_default();
            *count += 1;
            self.max_in_flight
                .fetch_max(*count, std::sync::atomic::Ordering::SeqCst);
        }
        tokio::time::sleep(self.latency).await;
        *self
            .in_flight
            .lock()
            .expect("in-flight lock")
            .get_mut(&facet)
            .expect("counted above") -= 1;
        self.attempts
            .lock()
            .expect("attempts lock")
            .push(PublishAttempt {
                facet,
                started,
                finished: self.elapsed(),
            });
        match (self.fails)(facet, &earlier, started) {
            Some(error) => Err(error),
            None => Ok(ReconcileStats::default()),
        }
    }
}

/// The facets every tick publishes in full: the ones a failed pass retries.
const PERIODIC_OUTBOUND: [facets::FacetKind; 3] = [
    facets::FacetKind::SandboxInventory,
    facets::FacetKind::TemplateInventory,
    facets::FacetKind::NodeStatus,
];

/// Stands in for node-manager's orphan inventory. The real observer, with no
/// node-manager to reach, retries its connect for 10 s of every tick.
struct NoOrphanInventory;

#[tonic::async_trait]
impl OrphanInventoryObserver for NoOrphanInventory {
    async fn observe(&mut self) {}
}

/// A worker publishing to `cluster`, on a paused clock from here on, with
/// its own poll loop when `ticking`. Returns the shutdown token, the work
/// queue and the node-object reconcile passes. Those run at the start of
/// every scheduled pass that publishes template inventory and of no retry,
/// so in these tests, which mark nothing dirty, they mark the ticks.
async fn run_against(
    dir: &Path,
    cluster: &Arc<ScriptedCluster>,
    ticking: bool,
) -> (CancellationToken, mpsc::Sender<WorkItem>, NodeObjectPasses) {
    let (worker, tx, _db, passes) =
        worker_publishing_to(dir, cluster.clone(), Box::new(NoOrphanInventory)).await;
    tokio::time::pause();
    cluster
        .origin
        .set(tokio::time::Instant::now())
        .expect("one worker per cluster");
    let shutdown = CancellationToken::new();
    tokio::spawn(worker.run(shutdown.child_token()));
    if ticking {
        spawn_poll_loop(
            tx.clone(),
            DEFAULT_POLL_INTERVAL_SECS,
            shutdown.child_token(),
        );
    }
    (shutdown, tx, passes)
}

/// When each tick's pass started, in seconds after the worker did.
fn tick_starts(cluster: &ScriptedCluster, passes: &NodeObjectPasses) -> Vec<f64> {
    passes
        .lock()
        .expect("node-object passes lock")
        .iter()
        .map(|at| cluster.since_origin(*at))
        .collect()
}

/// Sleep on the paused clock until `secs` after the worker started.
async fn advance_to(cluster: &ScriptedCluster, secs: f64) {
    tokio::time::sleep_until(cluster.origin() + Duration::from_secs_f64(secs)).await;
}

/// `value` lies in `[low, high + PUBLISH_LEAD]`: a publish that starts at
/// most a pass's lead after its pass was due in `[low, high]`.
fn due_within(value: f64, low: f64, high: f64) -> bool {
    (low - 1e-6..=high + PUBLISH_LEAD).contains(&value)
}

/// The tick at 0 s fails to reach the cluster. The retry lands within the
/// first backoff step (0.5-1 s after the failed pass), republishes every
/// facet that failed, and skips the node-object reconciles that do not go
/// through the cluster. Nothing else publishes until the next tick at 60 s.
#[tokio::test]
async fn a_failed_publish_is_retried_within_the_first_backoff_step() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster = ScriptedCluster::new(Duration::ZERO, |_, earlier, _| {
        earlier.is_empty().then(unavailable)
    });
    let (shutdown, _tx, passes) = run_against(dir.path(), &cluster, true).await;
    advance_to(&cluster, 70.0).await;
    shutdown.cancel();

    for facet in PERIODIC_OUTBOUND {
        let attempts = cluster.attempts_of(facet);
        assert_eq!(attempts.len(), 3, "{facet:?} publishes: {attempts:?}");
        assert!(due_within(attempts[0].started, 0.0, 0.0), "{attempts:?}");
        let retry_delay = attempts[1].started - attempts[0].finished;
        assert!(
            due_within(retry_delay, 0.5, 1.0),
            "{facet:?} was not retried within the first backoff step: {attempts:?}"
        );
        assert!(due_within(attempts[2].started, 60.0, 60.0), "{attempts:?}");
    }
    assert_eq!(
        pass_count(&passes),
        2,
        "node objects reconcile on the two ticks only"
    );
}

/// A publish the cluster rejects would only be rejected again: it waits for
/// the next tick, as before, rather than retrying every few seconds.
#[tokio::test]
async fn a_rejected_publish_waits_for_the_next_tick() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster = ScriptedCluster::new(Duration::ZERO, |_, _, _| Some(rejected()));
    let (shutdown, _tx, _passes) = run_against(dir.path(), &cluster, true).await;
    advance_to(&cluster, 130.0).await;
    shutdown.cancel();

    for facet in PERIODIC_OUTBOUND {
        let starts = cluster.starts_of(facet);
        assert_eq!(starts.len(), 3, "{facet:?} retried a rejection: {starts:?}");
        for (start, tick) in starts.iter().zip([0.0, 60.0, 120.0]) {
            assert!(due_within(*start, tick, tick), "{facet:?}: {starts:?}");
        }
    }
}

/// Only the facet that failed is retried: template inventory and node status
/// published at the tick and wait for the next one, while sandbox inventory
/// retries until the cluster takes it.
#[tokio::test]
async fn only_the_failed_facet_is_retried() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster = ScriptedCluster::new(Duration::ZERO, |facet, _, at| {
        (facet == facets::FacetKind::SandboxInventory && at < 20.0).then(unavailable)
    });
    let (shutdown, _tx, _passes) = run_against(dir.path(), &cluster, true).await;
    advance_to(&cluster, 70.0).await;
    shutdown.cancel();

    for facet in [
        facets::FacetKind::TemplateInventory,
        facets::FacetKind::NodeStatus,
    ] {
        let starts = cluster.starts_of(facet);
        assert_eq!(
            starts.len(),
            2,
            "{facet:?} republished on a retry: {starts:?}"
        );
        assert!(due_within(starts[0], 0.0, 0.0), "{facet:?}: {starts:?}");
        assert!(due_within(starts[1], 60.0, 60.0), "{facet:?}: {starts:?}");
    }
    let starts = cluster.starts_of(facets::FacetKind::SandboxInventory);
    let published = starts
        .iter()
        .position(|at| *at >= 20.0)
        .expect("sandbox inventory published after the outage");
    assert!(
        published >= 4,
        "sandbox inventory was not retried through the outage: {starts:?}"
    );
    assert!(
        starts[published] < 20.0 + 16.0 + PUBLISH_LEAD,
        "sandbox inventory waited for the tick: {starts:?}"
    );
    assert_eq!(
        starts.len(),
        published + 2,
        "sandbox inventory published again before the tick: {starts:?}"
    );
    assert!(due_within(starts[published + 1], 60.0, 60.0), "{starts:?}");
}

/// Through a long outage every publish fails to reach the cluster, with
/// publishes that fail at once and ones that take 2 s. Each retry waits out
/// the upper half of a step that doubles from 1 s and holds at 16 s, counted
/// from the end of the pass before it, ticks included. No facet ever has two
/// publishes in flight. Retries add at most 10 retry passes in the first
/// minute and at most 8 in any minute once the step is capped.
#[tokio::test]
async fn a_long_outage_backs_off_to_the_cap_with_one_attempt_in_flight() {
    for latency in [Duration::ZERO, Duration::from_secs(2)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let cluster = ScriptedCluster::new(latency, |_, _, _| Some(unavailable()));
        let (shutdown, _tx, passes) = run_against(dir.path(), &cluster, true).await;
        advance_to(&cluster, 300.0).await;
        shutdown.cancel();

        let attempts = cluster.attempts_of(facets::FacetKind::NodeStatus);
        let ticks = tick_starts(&cluster, &passes);
        let mut retries = Vec::new();
        let mut retry_delays = Vec::new();
        for (index, pair) in attempts.windows(2).enumerate() {
            let (previous, attempt) = (pair[0], pair[1]);
            assert!(
                attempt.started >= previous.finished,
                "{latency:?}: attempt {attempt:?} overlapped {previous:?}"
            );
            let tick = ticks
                .iter()
                .any(|at| (previous.finished - 1e-6..=attempt.started).contains(at));
            if tick {
                continue;
            }
            // `index + 1` passes have failed before this one.
            let step = 2f64
                .powi(i32::try_from(index).expect("few attempts"))
                .min(16.0);
            let delay = attempt.started - previous.finished;
            assert!(
                due_within(delay, step / 2.0, step),
                "{latency:?}: retry after {} failed passes waited {delay:.3} s, outside [{}, {step}] s: {attempts:?}",
                index + 1,
                step / 2.0,
            );
            retries.push(attempt.started);
            retry_delays.push(delay);
        }
        assert!(
            retry_delays.iter().filter(|delay| **delay >= 8.0).count() >= 5,
            "{latency:?}: the backoff never held at the cap: {retry_delays:?}"
        );
        let in_minute_from = |from: f64| {
            retries
                .iter()
                .filter(|at| (from..from + 60.0).contains(*at))
                .count()
        };
        assert!(
            in_minute_from(attempts[0].started) <= 10,
            "{latency:?}: retries in the first minute: {retries:?}"
        );
        // The step is capped after the fifth failed pass.
        let capped = attempts[4].finished;
        for from in retries.iter().copied().filter(|at| *at >= capped) {
            assert!(
                in_minute_from(from) <= 8,
                "{latency:?}: retries in the minute from {from}: {retries:?}"
            );
        }
        assert_eq!(
            cluster
                .max_in_flight
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "{latency:?}: a facet had two publishes in flight"
        );
        // The next row migrates its database on the real clock.
        tokio::time::resume();
    }
}

/// Once a pass publishes everything, the node is back on the tick cadence
/// and the backoff starts over: a failure at the 120 s tick is retried within
/// the first step again, not at the step the earlier outage had reached.
#[tokio::test]
async fn success_returns_the_publisher_to_the_tick_cadence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster = ScriptedCluster::new(Duration::ZERO, |_, earlier, at| {
        (at < 20.0 || (at >= 120.0 && earlier.iter().all(|started| *started < 120.0)))
            .then(unavailable)
    });
    let (shutdown, _tx, _passes) = run_against(dir.path(), &cluster, true).await;
    advance_to(&cluster, 200.0).await;
    shutdown.cancel();

    let attempts = cluster.attempts_of(facets::FacetKind::NodeStatus);
    let starts = cluster.starts_of(facets::FacetKind::NodeStatus);
    let recovered = starts
        .iter()
        .position(|at| *at >= 20.0)
        .expect("a publish after the outage");
    assert!(
        starts[recovered] < 20.0 + 16.0 + PUBLISH_LEAD,
        "the first publish after the outage waited for the tick: {starts:?}"
    );
    let rest = &starts[recovered + 1..];
    assert_eq!(rest.len(), 4, "{starts:?}");
    assert!(
        due_within(rest[0], 60.0, 60.0),
        "a publish ran between recovery and the tick: {starts:?}"
    );
    assert!(due_within(rest[1], 120.0, 120.0), "{starts:?}");
    let failed_tick = attempts[recovered + 2];
    assert!(
        due_within(rest[2] - failed_tick.finished, 0.5, 1.0),
        "the backoff did not start over after recovering: {starts:?}"
    );
    assert!(due_within(rest[3], 180.0, 180.0), "{starts:?}");
}

/// A tick that arrives while a retry is pending is that retry: the pending
/// one does not also run, and the next waits a step from the tick's pass.
#[tokio::test]
async fn a_tick_during_backoff_replaces_the_pending_retry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster = ScriptedCluster::new(Duration::ZERO, |_, _, _| Some(unavailable()));
    let (shutdown, tx, _passes) = run_against(dir.path(), &cluster, false).await;
    let tick = || WorkItem::TimerTick {
        next_due_at: now_secs() + DEFAULT_POLL_INTERVAL_SECS.cast_signed(),
    };
    tx.send(tick()).await.expect("queue the first tick");
    while cluster.starts_of(facets::FacetKind::NodeStatus).is_empty() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The first failure's retry is due 0.5-1 s after its pass; this tick
    // comes first.
    let failed = cluster.attempts_of(facets::FacetKind::NodeStatus)[0];
    advance_to(&cluster, failed.finished + 0.25).await;
    tx.send(tick()).await.expect("queue a tick during backoff");
    advance_to(&cluster, failed.finished + 3.0).await;
    shutdown.cancel();

    let attempts = cluster.attempts_of(facets::FacetKind::NodeStatus);
    assert_eq!(attempts.len(), 3, "{attempts:?}");
    assert!(
        due_within(
            attempts[1].started,
            failed.finished + 0.25,
            failed.finished + 0.25
        ),
        "the tick did not publish when it arrived: {attempts:?}"
    );
    // Two failed passes: a step of 2 s from the tick's pass.
    assert!(
        due_within(attempts[2].started - attempts[1].finished, 1.0, 2.0),
        "the retry after the tick did not wait a step from it: {attempts:?}"
    );
}
