use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake};

use tokio::sync::oneshot;

use super::*;

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn poll_until_stalled<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    let flag = Arc::new(WakeFlag::default());
    let waker = flag.clone().into();
    let mut context = Context::from_waker(&waker);
    loop {
        flag.0.store(false, Ordering::SeqCst);
        let result = future.as_mut().poll(&mut context);
        if result.is_ready() || !flag.0.load(Ordering::SeqCst) {
            return result;
        }
        // FuturesUnordered may cooperatively yield while work remains runnable.
        // Repoll only when woken, stopping at the unopened backend gates.
    }
}

struct GatedBackend {
    sink: Arc<FakeSink>,
    backend: Arc<FakeBackend>,
    routes: HashMap<String, SnapshotId>,
    gates: Mutex<HashMap<SnapshotId, oneshot::Receiver<()>>>,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl StorageBackend for GatedBackend {
    fn scheme(&self) -> &'static str {
        "s3"
    }

    fn validate_route(&self, route: &str) -> Result<(), DeletePrefixError> {
        self.backend.validate_route(route)
    }

    fn delete_prefix<'a>(
        &'a self,
        route: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<DeletePrefixOutcome, DeletePrefixError>> + Send + 'a>>
    {
        Box::pin(async move {
            let id = self.routes[route];
            assert!(
                self.sink
                    .claimed
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|row| row.snapshot_id == id),
                "delete requires this candidate's successful committed claim"
            );
            self.sink.backend_visits.lock().unwrap().push(id);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            let gate = self.gates.lock().unwrap().remove(&id);
            if let Some(gate) = gate {
                gate.await.expect("test releases every started delete");
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            let result = self.backend.delete_prefix(route).await;
            self.sink.backend_returns.lock().unwrap().push(id);
            result
        })
    }
}

// Polling until Pending exhausts runnable work; closed gates, rather than wall
// clock delays, expose both accidental serialization and excess concurrency.
// Unconstrained polling keeps Tokio's deferred cooperative wakeups from hiding
// runnable work from this manual executor.
#[tokio::test]
async fn bounded_phases_share_budget_and_preserve_fairness() {
    for ceiling in [1, 2, 20] {
        for budget in [1, 5, 8] {
            for recovery_count in [0, 1, 10] {
                let sink = Arc::new(FakeSink {
                    enforce_protocol: true,
                    ..FakeSink::default()
                });
                let recovered: Vec<_> = (0..recovery_count)
                    .map(|i| claimed_candidate(&fixture_uri(&format!("recovery-{i}"))))
                    .collect();
                let fresh: Vec<_> = (0..32)
                    .map(|i| candidate(&fixture_uri(&format!("fresh-{i}")), false))
                    .collect();
                let mut gates = HashMap::new();
                let mut releases = HashMap::new();
                let mut routes = HashMap::new();
                for row in recovered.iter().chain(&fresh) {
                    let (tx, rx) = oneshot::channel();
                    gates.insert(row.snapshot_id, rx);
                    releases.insert(row.snapshot_id, tx);
                    routes.insert(candidate_route(row).to_owned(), row.snapshot_id);
                }
                let backend = Arc::new(GatedBackend {
                    sink: sink.clone(),
                    backend: FakeBackend::new(),
                    routes,
                    gates: Mutex::new(gates),
                    active: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                });
                let mut registry = StorageBackendRegistry::new();
                registry.register(backend.clone());
                let mut policy = config(budget);
                policy.max_concurrent_deletes = NonZeroU32::new(ceiling).unwrap();
                let mut reconciler = Reconciler::new(policy, sink.clone(), registry);
                // Reset only fixtures between ticks to exercise odd-budget
                // alternation without exhausting either queue.
                for tick_index in 0..3 {
                    // Raise then lower both controls on the same reconciler.
                    // Existing gates prove the effective concurrency, budget,
                    // phase ordering and finish-forward contract each time.
                    let (budget, ceiling) = match tick_index {
                        0 => (budget, ceiling),
                        1 => (budget + 2, ceiling + 1),
                        _ => (1, 1),
                    };
                    reconciler.set_work_limits(
                        NonZeroU32::new(budget).unwrap(),
                        NonZeroU32::new(ceiling).unwrap(),
                    );
                    backend.peak.store(0, Ordering::SeqCst);
                    sink.set_claimed(recovered.clone());
                    sink.set_candidates(fresh.clone());
                    sink.backend_visits.lock().unwrap().clear();
                    sink.backend_returns.lock().unwrap().clear();
                    sink.claim_calls.lock().unwrap().clear();
                    let expected_recovery = recovery_count.min(
                        (budget / 2 + u32::from(budget % 2 == 1 && tick_index % 2 == 0)) as usize,
                    );
                    if tick_index > 0 {
                        // Rearm gates for the next tick's phase allocation.
                        for row in recovered.iter().chain(&fresh) {
                            let (tx, rx) = oneshot::channel();
                            backend.gates.lock().unwrap().insert(row.snapshot_id, rx);
                            releases.insert(row.snapshot_id, tx);
                        }
                    }
                    let mut tick = Box::pin(tokio::task::unconstrained(reconciler.tick()));
                    let mut completed = HashSet::new();
                    for phase_size in [expected_recovery, budget as usize - expected_recovery] {
                        for done in 0..phase_size {
                            assert!(poll_until_stalled(tick.as_mut()).is_pending());
                            assert_eq!(
                                backend.active.load(Ordering::SeqCst),
                                (ceiling as usize).min(phase_size - done),
                                "tick={tick_index} budget={budget} ceiling={ceiling} recovery={recovery_count} phase={phase_size} done={done}"
                            );
                            if completed.len() < expected_recovery {
                                assert!(
                                    sink.claim_calls().is_empty(),
                                    "fresh waits for all recovery results"
                                );
                            }
                            let id = *sink
                                .backend_visits
                                .lock()
                                .unwrap()
                                .iter()
                                .find(|id| !completed.contains(*id))
                                .expect("a blocked delete has an unreleased gate");
                            releases.remove(&id).unwrap().send(()).unwrap();
                            completed.insert(id);
                        }
                    }
                    let report = match poll_until_stalled(tick.as_mut()) {
                        std::task::Poll::Ready(report) => report,
                        std::task::Poll::Pending => {
                            panic!("tick must drain exactly its shared budget")
                        }
                    };
                    assert_eq!(report.pruned, budget as usize);
                    assert_eq!(report.resumed, expected_recovery);
                    assert_eq!(
                        sink.claim_calls().len(),
                        budget as usize - expected_recovery
                    );
                    assert_eq!(sink.backend_visits.lock().unwrap().len(), budget as usize);
                    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
                    assert!(backend.peak.load(Ordering::SeqCst) <= ceiling as usize);
                }
            }
        }
    }
}

#[tokio::test]
async fn failures_do_not_cancel_siblings_and_claims_finish_forward() {
    let sink = Arc::new(FakeSink {
        enforce_protocol: true,
        ..FakeSink::default()
    });
    let rows: Vec<_> = (0..7)
        .map(|i| candidate(&fixture_uri(&format!("failure-{i}")), false))
        .collect();
    // Refusal and pre-claim errors release reserved budget. The next three
    // claims spend it even when backend deletion or finalize fails.
    sink.refuse_claim(rows[0].snapshot_id);
    sink.fail_claim(rows[1].snapshot_id);
    sink.fail(rows[3].snapshot_id);
    sink.set_candidates(rows.clone());
    let fake = FakeBackend::new();
    fake.set_error(candidate_route(&rows[2]), FakeErr::Backend("retry".into()));
    let mut gates = HashMap::new();
    let mut releases = Vec::new();
    for row in &rows[2..5] {
        let (tx, rx) = oneshot::channel();
        gates.insert(row.snapshot_id, rx);
        releases.push(tx);
    }
    let backend = Arc::new(GatedBackend {
        sink: sink.clone(),
        backend: fake.clone(),
        routes: rows
            .iter()
            .map(|row| (candidate_route(row).to_owned(), row.snapshot_id))
            .collect(),
        gates: Mutex::new(gates),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let mut registry = StorageBackendRegistry::new();
    registry.register(backend);
    let mut policy = config(3);
    policy.max_concurrent_deletes = NonZeroU32::new(3).unwrap();
    let mut reconciler = Reconciler::new(policy, sink.clone(), registry);
    let mut tick = Box::pin(tokio::task::unconstrained(reconciler.tick()));
    assert!(poll_until_stalled(tick.as_mut()).is_pending());
    assert_eq!(sink.backend_visits.lock().unwrap().len(), 3);
    // Fail backend deletion, then finalize, while successful work is blocked.
    // Every error must leave the remaining independent futures alive.
    for release in releases {
        assert!(poll_until_stalled(tick.as_mut()).is_pending());
        release.send(()).unwrap();
    }
    let report = tick.await;
    assert_eq!(
        (
            report.drift_skips,
            report.claim_errors,
            report.backend_errors,
            report.db_errors,
            report.pruned
        ),
        (1, 1, 1, 1, 1)
    );
    assert_eq!(fake.calls().len(), 3);
    assert_eq!(report.rate_limited, 1); // scan page contains six of seven rows
    assert_eq!(
        sink.claimed
            .lock()
            .unwrap()
            .iter()
            .map(|row| row.snapshot_id)
            .collect::<HashSet<_>>(),
        HashSet::from([rows[2].snapshot_id, rows[3].snapshot_id])
    );
    assert!(
        !sink
            .candidates
            .lock()
            .unwrap()
            .iter()
            .any(|row| row.snapshot_id == rows[2].snapshot_id
                || row.snapshot_id == rows[3].snapshot_id)
    );
    sink.set_candidates(Vec::new());
    sink.fail_prune.lock().unwrap().clear();
    fake.set_outcome(
        candidate_route(&rows[2]),
        DeletePrefixOutcome::Deleted { count: 1 },
    );
    let recovered = reconciler.tick().await.resumed + reconciler.tick().await.resumed;
    assert_eq!(recovered, 2);
    assert!(sink.claimed.lock().unwrap().is_empty());
    assert_eq!(
        sink.claim_calls().len(),
        5,
        "finish-forward never reclaims or unclaims"
    );
}
