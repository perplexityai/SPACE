use super::*;

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

/// Stands in for the Kubernetes node-local controller: counts the reconcile
/// passes `run_outbound_sync` gives it.
struct CountingNodeObjects(Arc<std::sync::atomic::AtomicUsize>);

#[tonic::async_trait]
impl NodeObjectReconciles for CountingNodeObjects {
    async fn reconcile_node_objects(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
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
    Arc<std::sync::atomic::AtomicUsize>,
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
    let passes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let worker = NodeCtlWorker {
        db: db.clone(),
        node_id: "node-under-test".to_string(),
        node_role: Some(NodeRole::Worker),
        poll_interval: poll_interval(DEFAULT_POLL_INTERVAL_SECS),
        cluster_reconcile_trigger: ClusterReconcileTrigger::new(tx.clone(), "node-under-test"),
        resource_status_reader: NodeResourceStatusReader::new(),
        network_pool_status_reader: NetworkPoolStatusReader::new(),
        orphan_inventory: orphan_inventory::Observer::default(),
        runtime_provisioning: Arc::default(),
        node_objects: Some(Box::new(CountingNodeObjects(passes.clone()))),
        rx,
    };
    (worker, tx, db, passes)
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
        passes.load(std::sync::atomic::Ordering::SeqCst),
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
    assert_eq!(passes.load(std::sync::atomic::Ordering::SeqCst), 0);

    shutdown.cancel();
    task.await.expect("worker task");
}
