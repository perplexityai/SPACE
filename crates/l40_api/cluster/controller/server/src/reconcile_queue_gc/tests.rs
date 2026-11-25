use pplx_space_accounts::NodeId;

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::orphaned_nodes::{InstanceLookup, InstanceLookupError, InstanceLookupOutcome};

use super::detector::{Ec2Detector, KubeNodePresence, KubePresenceDetector};
use super::*;

type GoneCall = (NodeId, Option<String>, u32, QueueGcMode);

#[derive(Default)]
struct FakeSink {
    dead: Mutex<PruneCounts>,
    candidates: Mutex<Vec<GoneCandidate>>,
    gone_counts: Mutex<BTreeMap<NodeId, PruneCounts>>,
    dead_calls: Mutex<Vec<(Duration, u32)>>,
    candidate_calls: Mutex<Vec<(Duration, u32, Option<NodeId>)>>,
    gone_calls: Mutex<Vec<GoneCall>>,
}

impl FakeSink {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl QueueGcSink for Arc<FakeSink> {
    async fn prune_dead(
        &self,
        retention: Duration,
        limit: u32,
    ) -> Result<PruneCounts, sqlx::Error> {
        self.dead_calls.lock().unwrap().push((retention, limit));
        let configured = *self.dead.lock().unwrap();
        let queue = configured.queue.min(u64::from(limit));
        let triggers = configured
            .triggers
            .min(u64::from(limit).saturating_sub(queue));
        Ok(PruneCounts { queue, triggers })
    }

    async fn list_gone_candidates(
        &self,
        retention: Duration,
        limit: u32,
        after: Option<NodeId>,
    ) -> Result<Vec<GoneCandidate>, sqlx::Error> {
        self.candidate_calls
            .lock()
            .unwrap()
            .push((retention, limit, after));
        Ok(self
            .candidates
            .lock()
            .unwrap()
            .iter()
            .filter(|candidate| after.is_none_or(|after| candidate.node_id > after))
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .cloned()
            .collect())
    }

    async fn evaluate_gone_node(
        &self,
        node_id: NodeId,
        expected_uid: Option<String>,
        _retention: Duration,
        _claim_active_grace: Duration,
        limit: u32,
        mode: QueueGcMode,
    ) -> Result<GoneNodeEvaluation, sqlx::Error> {
        self.gone_calls
            .lock()
            .unwrap()
            .push((node_id, expected_uid, limit, mode));
        let configured = self
            .gone_counts
            .lock()
            .unwrap()
            .get(&node_id)
            .copied()
            .unwrap_or_default();
        let queue = configured.queue.min(u64::from(limit));
        let triggers = configured
            .triggers
            .min(u64::from(limit).saturating_sub(queue));
        let eligible = PruneCounts { queue, triggers };
        let deleted = if mode == QueueGcMode::Live {
            eligible
        } else {
            PruneCounts::default()
        };
        Ok(GoneNodeEvaluation { eligible, deleted })
    }
}

#[derive(Default)]
struct FakeLookup {
    outcomes: Mutex<BTreeMap<String, Result<InstanceLookupOutcome, String>>>,
}

impl FakeLookup {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

impl InstanceLookup for Arc<FakeLookup> {
    async fn lookup_by_private_dns(
        &self,
        private_dns_name: String,
    ) -> Result<InstanceLookupOutcome, InstanceLookupError> {
        match self
            .outcomes
            .lock()
            .unwrap()
            .get(&private_dns_name)
            .cloned()
        {
            Some(Ok(outcome)) => Ok(outcome),
            Some(Err(error)) => Err(InstanceLookupError::Backend(error)),
            None => Ok(InstanceLookupOutcome::Gone),
        }
    }
}

struct FakePresence {
    live: Mutex<Option<HashSet<String>>>,
    lookup_outcomes: Mutex<BTreeMap<String, Result<bool, FakePresenceError>>>,
    lookup_calls: Mutex<Vec<String>>,
    lookup_hangs: Mutex<HashSet<String>>,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
struct FakePresenceError(String);

impl FakePresence {
    fn live(names: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            live: Mutex::new(Some(names.iter().map(|name| name.to_string()).collect())),
            lookup_outcomes: Mutex::new(BTreeMap::new()),
            lookup_calls: Mutex::new(Vec::new()),
            lookup_hangs: Mutex::new(HashSet::new()),
        })
    }

    fn cold() -> Arc<Self> {
        Arc::new(Self {
            live: Mutex::new(None),
            lookup_outcomes: Mutex::new(BTreeMap::new()),
            lookup_calls: Mutex::new(Vec::new()),
            lookup_hangs: Mutex::new(HashSet::new()),
        })
    }
}

impl KubeNodePresence for Arc<FakePresence> {
    type LookupError = FakePresenceError;

    fn live_node_names(&self) -> Option<HashSet<String>> {
        self.live.lock().unwrap().clone()
    }

    async fn node_exists(&self, node_name: String) -> Result<bool, Self::LookupError> {
        self.lookup_calls.lock().unwrap().push(node_name.clone());
        let hangs = self.lookup_hangs.lock().unwrap().contains(&node_name);
        let outcome = self
            .lookup_outcomes
            .lock()
            .unwrap()
            .get(&node_name)
            .cloned()
            .unwrap_or_else(|| {
                self.live
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|names| names.contains(&node_name))
                    .ok_or_else(|| FakePresenceError("membership unavailable".to_string()))
            });
        if hangs {
            std::future::pending::<Result<bool, FakePresenceError>>().await
        } else {
            outcome
        }
    }
}

fn ec2_config() -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        dry_run: false,
        tick: Duration::from_secs(60),
        dead_retention: Duration::from_secs(60),
        gone_live_retention: Duration::from_secs(60),
        dead_max_per_tick: 10,
        gone_live_max_per_tick: 10,
        gone_max_nodes_per_tick: 10,
        run_dead_prune: true,
    }
}

fn kube_config() -> ReconcilerConfig {
    ReconcilerConfig {
        run_dead_prune: false,
        ..ec2_config()
    }
}

fn candidate(node_id: NodeId, name: &str, uid: &str) -> GoneCandidate {
    GoneCandidate {
        node_id,
        k8s_node_name: name.to_string(),
        k8s_node_uid: Some(uid.to_string()),
    }
}

#[test]
fn ec2_config_defaults_disabled_live_with_dead_prune() {
    let env = pplx_space_test_env::TestEnv::default();
    let config = ReconcilerConfig::from_environment_ec2(&env);
    assert!(!config.enabled);
    assert!(!config.dry_run, "ec2 instance defaults to live");
    assert!(config.run_dead_prune);
    assert_eq!(config.gone_live_retention, DEFAULT_GONE_LIVE_RETENTION);
}

#[test]
fn ec2_config_uses_default_operational_limits() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_RECONCILE_QUEUE_GC_ENABLED", "true");
    let config = ReconcilerConfig::from_environment_ec2(&env);
    assert!(config.enabled);
    assert!(config.run_dead_prune);
    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.dead_retention, DEFAULT_DEAD_RETENTION);
    assert_eq!(config.gone_live_retention, DEFAULT_GONE_LIVE_RETENTION);
    assert_eq!(config.dead_max_per_tick, DEFAULT_DEAD_MAX_PER_TICK);
    assert_eq!(
        config.gone_live_max_per_tick,
        DEFAULT_GONE_LIVE_MAX_PER_TICK
    );
    assert_eq!(
        config.gone_max_nodes_per_tick,
        DEFAULT_GONE_MAX_NODES_PER_TICK
    );
}

/// The EC2 instance's tick is a whole-second test hook; anything but a
/// positive count keeps the default.
#[test]
fn ec2_tick_test_hook_takes_whole_seconds() {
    const TICK_ENV: &str = "SPACE_RECONCILE_QUEUE_GC_TICK_SECS";
    let tick =
        |env: &pplx_space_test_env::TestEnv| ReconcilerConfig::from_environment_ec2(env).tick;
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(TICK_ENV, "10");
    assert_eq!(tick(&env), Duration::from_secs(10));
    for invalid in ["0", "abc", ""] {
        env.set(TICK_ENV, invalid);
        assert_eq!(tick(&env), DEFAULT_TICK, "{invalid:?}");
    }
}

#[test]
fn kube_config_defaults_enabled_dry_run_without_dead_prune() {
    let env = pplx_space_test_env::TestEnv::default();
    let config = ReconcilerConfig::from_environment_kube(&env);
    assert!(config.enabled);
    assert!(
        config.dry_run,
        "kube instance defaults to observe-first dry-run"
    );
    assert!(
        !config.run_dead_prune,
        "kube instance never runs dead prune"
    );
}

#[test]
fn kube_config_reads_its_dry_run_gate() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_GONE_NODE_QUEUE_GC_DRY_RUN", "false");
    let config = ReconcilerConfig::from_environment_kube(&env);
    assert!(config.enabled);
    assert!(!config.dry_run);
    assert!(!config.run_dead_prune);
    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.gone_live_retention, DEFAULT_GONE_LIVE_RETENTION);
    assert_eq!(
        config.gone_live_max_per_tick,
        DEFAULT_GONE_LIVE_MAX_PER_TICK
    );
    assert_eq!(
        config.gone_max_nodes_per_tick,
        DEFAULT_GONE_MAX_NODES_PER_TICK
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ec2_tick_prunes_dead_and_only_confirmed_gone_refs() {
    let gone = NodeId::new();
    let live = NodeId::new();
    let sink = FakeSink::new();
    *sink.dead.lock().unwrap() = PruneCounts {
        queue: 2,
        triggers: 1,
    };
    *sink.candidates.lock().unwrap() = vec![
        candidate(gone, "ip-gone", "uid-gone"),
        candidate(live, "ip-live", "uid-live"),
    ];
    sink.gone_counts.lock().unwrap().insert(
        gone,
        PruneCounts {
            queue: 3,
            triggers: 1,
        },
    );
    let lookup = FakeLookup::new();
    lookup.outcomes.lock().unwrap().insert(
        "ip-live".to_string(),
        Ok(InstanceLookupOutcome::Live {
            instance_id: "i-live".to_string(),
            state: "running".to_string(),
        }),
    );

    let report = Reconciler::new(ec2_config(), sink.clone(), Ec2Detector::new(lookup))
        .tick()
        .await;

    assert_eq!(report.dead_deleted, 3);
    assert_eq!(report.gone_nodes, 1);
    assert_eq!(report.gone_live_eligible, 4);
    assert_eq!(report.gone_live_deleted, 4);
    assert_eq!(report.skipped_live, 1);
    assert_eq!(
        *sink.gone_calls.lock().unwrap(),
        vec![(gone, Some("uid-gone".to_string()), 10, QueueGcMode::Live)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ec2_lookup_errors_are_unknown_and_fail_closed() {
    let failed = NodeId::new();
    let sink = FakeSink::new();
    *sink.candidates.lock().unwrap() = vec![candidate(failed, "ip-error", "uid-error")];
    let lookup = FakeLookup::new();
    lookup
        .outcomes
        .lock()
        .unwrap()
        .insert("ip-error".to_string(), Err("throttled".to_string()));

    let report = Reconciler::new(ec2_config(), sink.clone(), Ec2Detector::new(lookup))
        .tick()
        .await;

    assert_eq!(report.skipped_unknown, 1);
    assert_eq!(report.gone_nodes, 0);
    assert_eq!(report.result(), QueueGcResult::Error);
    assert!(sink.gone_calls.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn gone_delete_budget_is_shared_across_nodes() {
    let first = NodeId::new();
    let second = NodeId::new();
    let sink = FakeSink::new();
    *sink.candidates.lock().unwrap() = vec![
        candidate(first, "ip-first", "uid-first"),
        candidate(second, "ip-second", "uid-second"),
    ];
    sink.gone_counts.lock().unwrap().insert(
        first,
        PruneCounts {
            queue: 2,
            triggers: 0,
        },
    );
    sink.gone_counts.lock().unwrap().insert(
        second,
        PruneCounts {
            queue: 2,
            triggers: 0,
        },
    );
    let mut limited = ec2_config();
    limited.gone_live_max_per_tick = 3;

    let report = Reconciler::new(limited, sink.clone(), Ec2Detector::new(FakeLookup::new()))
        .tick()
        .await;

    assert_eq!(report.gone_nodes, 2);
    assert_eq!(report.gone_live_deleted, 3);
    assert_eq!(
        sink.gone_calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_node, _uid, limit, _mode)| *limit)
            .collect::<Vec<_>>(),
        vec![3, 1]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kube_tick_prunes_only_nodes_absent_from_reflector() {
    let gone = NodeId::new();
    let present = NodeId::new();
    let sink = FakeSink::new();
    *sink.candidates.lock().unwrap() = vec![
        candidate(gone, "ip-gone", "uid-gone"),
        candidate(present, "ip-present", "uid-present"),
    ];
    sink.gone_counts.lock().unwrap().insert(
        gone,
        PruneCounts {
            queue: 3,
            triggers: 1,
        },
    );

    let report = Reconciler::new(
        kube_config(),
        sink.clone(),
        KubePresenceDetector::with_grace(
            FakePresence::live(&["ip-present", "ip-other"]),
            Duration::ZERO,
        ),
    )
    .tick()
    .await;

    assert_eq!(report.gone_nodes, 1);
    assert_eq!(report.gone_live_eligible, 4);
    assert_eq!(report.gone_live_deleted, 4);
    assert_eq!(report.skipped_live, 1);
    assert_eq!(report.dead_deleted, 0, "kube instance never dead-prunes");
    assert!(sink.dead_calls.lock().unwrap().is_empty());
    assert_eq!(
        *sink.gone_calls.lock().unwrap(),
        vec![(gone, Some("uid-gone".to_string()), 10, QueueGcMode::Live)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kube_dry_run_counts_gone_without_pruning() {
    let gone = NodeId::new();
    let sink = FakeSink::new();
    *sink.candidates.lock().unwrap() = vec![candidate(gone, "ip-gone", "uid-gone")];
    sink.gone_counts.lock().unwrap().insert(
        gone,
        PruneCounts {
            queue: 5,
            triggers: 0,
        },
    );
    let mut dry = kube_config();
    dry.dry_run = true;

    let report = Reconciler::new(
        dry,
        sink.clone(),
        KubePresenceDetector::with_grace(FakePresence::live(&["ip-present"]), Duration::ZERO),
    )
    .tick()
    .await;

    assert_eq!(report.gone_nodes, 1, "counted as would-prune");
    assert_eq!(report.gone_live_eligible, 5);
    assert_eq!(report.gone_live_deleted, 0, "dry-run deletes nothing");
    assert_eq!(
        *sink.gone_calls.lock().unwrap(),
        vec![(gone, Some("uid-gone".to_string()), 10, QueueGcMode::DryRun)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kube_dry_run_excludes_detector_gone_but_db_ineligible_node() {
    let gone = NodeId::new();
    let sink = FakeSink::new();
    *sink.candidates.lock().unwrap() = vec![candidate(gone, "ip-gone", "uid-gone")];
    let mut dry = kube_config();
    dry.dry_run = true;

    let report = Reconciler::new(
        dry,
        sink.clone(),
        KubePresenceDetector::with_grace(FakePresence::live(&["ip-present"]), Duration::ZERO),
    )
    .tick()
    .await;

    assert_eq!(
        report.gone_nodes, 0,
        "dry-run must not count a node rejected by final DB eligibility"
    );
    assert_eq!(report.gone_live_eligible, 0);
    assert_eq!(report.skipped_drift, 1);
    assert_eq!(
        *sink.gone_calls.lock().unwrap(),
        vec![(gone, Some("uid-gone".to_string()), 10, QueueGcMode::DryRun)]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dry_run_ec2_instance_skips_dead_prune() {
    // Dry-run means delete nothing — including dead letters on the EC2 instance
    // that owns the dead-letter pass.
    let sink = FakeSink::new();
    *sink.dead.lock().unwrap() = PruneCounts {
        queue: 5,
        triggers: 5,
    };
    let mut dry = ec2_config();
    dry.dry_run = true;

    let report = Reconciler::new(dry, sink.clone(), Ec2Detector::new(FakeLookup::new()))
        .tick()
        .await;

    assert_eq!(report.dead_deleted, 0);
    assert!(sink.dead_calls.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn kube_cold_cache_fails_closed_without_scanning() {
    let sink = FakeSink::new();
    *sink.candidates.lock().unwrap() = vec![candidate(NodeId::new(), "ip-gone", "uid-gone")];

    let report = Reconciler::new(
        kube_config(),
        sink.clone(),
        KubePresenceDetector::with_grace(FakePresence::cold(), Duration::ZERO),
    )
    .tick()
    .await;

    assert!(report.cache_cold);
    assert_eq!(report.result(), QueueGcResult::CacheCold);
    assert!(sink.candidate_calls.lock().unwrap().is_empty());
    assert!(sink.gone_calls.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn kube_empty_reflector_set_fails_closed() {
    let sink = FakeSink::new();
    *sink.candidates.lock().unwrap() = vec![candidate(NodeId::new(), "ip-gone", "uid-gone")];

    let report = Reconciler::new(
        kube_config(),
        sink.clone(),
        KubePresenceDetector::with_grace(FakePresence::live(&[]), Duration::ZERO),
    )
    .tick()
    .await;

    assert!(report.cache_cold);
    assert!(sink.candidate_calls.lock().unwrap().is_empty());
    assert!(sink.gone_calls.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn kube_requires_sustained_absence_before_gone() {
    // A node absent for less than the grace window is preserved; only after it
    // stays absent across the window is it judged gone. Guards against reaping
    // a node that flaps out of the reflector and returns moments later.
    let node = NodeId::new();
    let cand = candidate(node, "ip-gone", "uid-gone");
    let mut detector = KubePresenceDetector::with_grace(
        FakePresence::live(&["ip-present"]),
        Duration::from_secs(600),
    );

    assert!(detector.begin_tick().await);
    assert_eq!(
        detector.classify(&cand).await,
        GoneVerdict::Live,
        "absent but not yet sustained → preserve"
    );

    tokio::time::advance(Duration::from_secs(601)).await;
    assert!(detector.begin_tick().await);
    assert_eq!(
        detector.classify(&cand).await,
        GoneVerdict::Gone,
        "sustained absence past the grace → gone"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kube_direct_reappearance_overrides_stale_reflector_snapshot() {
    let node = NodeId::new();
    let cand = candidate(node, "ip-reappeared", "uid-reappeared");
    let presence = FakePresence::live(&["ip-present"]);
    let mut detector = KubePresenceDetector::with_grace(presence.clone(), Duration::ZERO);

    assert!(detector.begin_tick().await);
    *presence.live.lock().unwrap() = Some(
        ["ip-present", "ip-reappeared"]
            .iter()
            .map(|name| name.to_string())
            .collect(),
    );

    assert_eq!(
        detector.classify(&cand).await,
        GoneVerdict::Live,
        "an authoritative Kubernetes lookup must preserve a node that reappeared after the reflector snapshot"
    );
    assert_eq!(
        *presence.lookup_calls.lock().unwrap(),
        vec!["ip-reappeared".to_string()]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kube_direct_lookup_error_fails_closed() {
    let node = NodeId::new();
    let cand = candidate(node, "ip-unknown", "uid-unknown");
    let presence = FakePresence::live(&["ip-present"]);
    presence.lookup_outcomes.lock().unwrap().insert(
        "ip-unknown".to_string(),
        Err(FakePresenceError("apiserver unavailable".to_string())),
    );
    let mut detector = KubePresenceDetector::with_grace(presence, Duration::ZERO);

    assert!(detector.begin_tick().await);
    assert_eq!(detector.classify(&cand).await, GoneVerdict::Unknown);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn kube_direct_lookup_timeout_fails_closed() {
    let node = NodeId::new();
    let cand = candidate(node, "ip-timeout", "uid-timeout");
    let presence = FakePresence::live(&["ip-present"]);
    presence
        .lookup_hangs
        .lock()
        .unwrap()
        .insert("ip-timeout".to_string());
    let mut detector = KubePresenceDetector::with_grace(presence, Duration::ZERO);

    assert!(detector.begin_tick().await);
    assert_eq!(detector.classify(&cand).await, GoneVerdict::Unknown);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn kube_reappearance_resets_the_grace_even_without_reclassify() {
    // The node is absent, then reappears in the reflector snapshot while its
    // candidate is NOT reclassified (pagination/eligibility), then disappears
    // again briefly. Its grace must have reset on reappearance, so the brief
    // second absence is preserved rather than immediately reaped.
    let node = NodeId::new();
    let cand = candidate(node, "ip-flap", "uid-flap");
    let present = FakePresence::live(&["ip-present"]);
    let mut detector = KubePresenceDetector::with_grace(present.clone(), Duration::from_secs(600));

    // Tick 1: absent, timer starts.
    assert!(detector.begin_tick().await);
    assert_eq!(detector.classify(&cand).await, GoneVerdict::Live);

    // Tick 2: node is back in the reflector, but we do NOT call classify on it.
    tokio::time::advance(Duration::from_secs(601)).await;
    *present.live.lock().unwrap() = Some(
        ["ip-present", "ip-flap"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );
    assert!(detector.begin_tick().await);

    // Tick 3: it disappears again, only seconds later — must NOT be gone yet.
    tokio::time::advance(Duration::from_secs(10)).await;
    *present.live.lock().unwrap() = Some(["ip-present"].iter().map(|s| s.to_string()).collect());
    assert!(detector.begin_tick().await);
    assert_eq!(
        detector.classify(&cand).await,
        GoneVerdict::Live,
        "grace reset on reappearance; the fresh brief absence is preserved"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn kube_unknown_membership_does_not_count_as_absence() {
    // A cold/empty cache interval must not accrue toward sustained absence: a
    // node absent before the outage is not immediately reaped once the cache
    // recovers still-absent — its grace restarts.
    let node = NodeId::new();
    let cand = candidate(node, "ip-gone", "uid-gone");
    let presence = FakePresence::live(&["ip-present"]);
    let mut detector = KubePresenceDetector::with_grace(presence.clone(), Duration::from_secs(600));

    assert!(detector.begin_tick().await);
    assert_eq!(detector.classify(&cand).await, GoneVerdict::Live);

    // Cache goes cold for longer than the grace; the tick is skipped.
    tokio::time::advance(Duration::from_secs(601)).await;
    *presence.live.lock().unwrap() = None;
    assert!(!detector.begin_tick().await);

    // Cache recovers, node still absent — grace restarts, so not gone yet.
    *presence.live.lock().unwrap() = Some(["ip-present"].iter().map(|s| s.to_string()).collect());
    assert!(detector.begin_tick().await);
    assert_eq!(
        detector.classify(&cand).await,
        GoneVerdict::Live,
        "unknown-membership interval must not count as sustained absence"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn kube_continuous_absence_stays_gone() {
    // A continuously-absent node stays classified gone across ticks — its timer
    // is not aged out by absence duration (which would wobble the observe
    // signal in the never-deleting dry-run bake).
    let node = NodeId::new();
    let cand = candidate(node, "ip-gone", "uid-gone");
    let mut detector = KubePresenceDetector::with_grace(
        FakePresence::live(&["ip-present"]),
        Duration::from_secs(600),
    );

    assert!(detector.begin_tick().await);
    assert_eq!(detector.classify(&cand).await, GoneVerdict::Live);
    tokio::time::advance(Duration::from_secs(601)).await;
    assert!(detector.begin_tick().await);
    assert_eq!(detector.classify(&cand).await, GoneVerdict::Gone);

    // Long after the grace, still absent — stays gone (no duration eviction).
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert!(detector.begin_tick().await);
    assert_eq!(
        detector.classify(&cand).await,
        GoneVerdict::Gone,
        "continuous absence stays gone"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn disabled_loop_never_touches_the_sink() {
    let sink = FakeSink::new();
    let token = CancellationToken::new();
    let mut disabled = ec2_config();
    disabled.enabled = false;
    disabled.tick = Duration::from_secs(1);
    let handle = tokio::spawn(run(
        disabled,
        sink.clone(),
        Ec2Detector::new(FakeLookup::new()),
        token.clone(),
    ));

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;

    assert!(sink.dead_calls.lock().unwrap().is_empty());
    assert!(sink.candidate_calls.lock().unwrap().is_empty());
    assert!(sink.gone_calls.lock().unwrap().is_empty());

    token.cancel();
    handle.await.unwrap();
}

mod postgres {

    use pplx_space_accounts::NodeId;
    use pplx_space_db_test_fixtures::try_pg_test_db;
    use sqlx::PgPool;

    use super::*;

    use pplx_space_test_env::TestResult;

    async fn insert_node(pool: &PgPool, state: &str, uid: &str) -> TestResult<NodeId> {
        let node_id = NodeId::new();
        let name = format!("ip-{node_id}.ec2.internal");
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '127.0.0.1', 'worker', 'k8s', $2)",
        )
        .bind(node_id)
        .bind(state)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
             VALUES ($1, $2, $3)",
        )
        .bind(node_id)
        .bind(name)
        .bind(uid)
        .execute(pool)
        .await?;
        Ok(node_id)
    }

    async fn insert_queue(
        pool: &PgPool,
        node_id: NodeId,
        element: &str,
        age: Duration,
        dead: bool,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO node_reconcile_queue (
                 node_id, facet, element, requested_at, updated_at, dead_at
             ) VALUES (
                 $1, 'sandbox-desired-state', $2,
                 NOW() - ($3 * INTERVAL '1 second'),
                 NOW() - ($3 * INTERVAL '1 second'),
                 CASE WHEN $4 THEN NOW() - ($3 * INTERVAL '1 second') ELSE NULL END
             )",
        )
        .bind(node_id)
        .bind(element)
        .bind(i64::try_from(age.as_secs()).unwrap_or(i64::MAX))
        .bind(dead)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_trigger(
        pool: &PgPool,
        node_id: NodeId,
        age: Duration,
        dead: bool,
    ) -> TestResult {
        sqlx::query(
            "INSERT INTO node_reconcile_triggers (
                 node_id, reason, requested_at, updated_at, dead_at
             ) VALUES (
                 $1, 'test',
                 NOW() - ($2 * INTERVAL '1 second'),
                 NOW() - ($2 * INTERVAL '1 second'),
                 CASE WHEN $3 THEN NOW() - ($2 * INTERVAL '1 second') ELSE NULL END
             )",
        )
        .bind(node_id)
        .bind(i64::try_from(age.as_secs()).unwrap_or(i64::MAX))
        .bind(dead)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Which of a node's rows [`claim`] claims.
    #[derive(Clone, Copy)]
    enum Claimed {
        Refs,
        Trigger,
    }

    /// Claims every ref, or the trigger, on `node_id` as node-ctl,
    /// `claimed_ago` ago.
    async fn claim(
        pool: &PgPool,
        claimed: Claimed,
        node_id: NodeId,
        claimed_ago: Duration,
    ) -> TestResult {
        let statement = match claimed {
            Claimed::Refs => {
                "UPDATE node_reconcile_queue
                    SET claim_token = 'node-ctl',
                        claimed_at = NOW() - ($2 * INTERVAL '1 second')
                  WHERE node_id = $1"
            }
            Claimed::Trigger => {
                "UPDATE node_reconcile_triggers
                    SET claim_token = 'node-ctl',
                        claimed_at = NOW() - ($2 * INTERVAL '1 second')
                  WHERE node_id = $1"
            }
        };
        sqlx::query(statement)
            .bind(node_id)
            .bind(i64::try_from(claimed_ago.as_secs())?)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn live_queue_count(pool: &PgPool, node_id: NodeId) -> TestResult<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM node_reconcile_queue
             WHERE node_id = $1 AND dead_at IS NULL",
        )
        .bind(node_id)
        .fetch_one(pool)
        .await?)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dry_run_reports_live_eligibility_without_deleting() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node = insert_node(&db.pool, "offline", "uid-dry-run").await?;
        insert_queue(&db.pool, node, "old", Duration::from_secs(120), false).await?;
        insert_trigger(&db.pool, node, Duration::from_secs(120), false).await?;
        let store = QueueDbStore::new(db.pool.clone());

        let preview = store
            .evaluate_gone_node(
                node,
                Some("uid-dry-run".to_string()),
                Duration::from_secs(60),
                Duration::from_secs(60),
                10,
                QueueGcMode::DryRun,
            )
            .await?;
        assert_eq!(
            preview,
            GoneNodeEvaluation {
                eligible: PruneCounts {
                    queue: 1,
                    triggers: 1,
                },
                deleted: PruneCounts::default(),
            }
        );
        assert_eq!(live_queue_count(&db.pool, node).await?, 1);
        let live_trigger_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM node_reconcile_triggers
             WHERE node_id = $1 AND dead_at IS NULL",
        )
        .bind(node)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(live_trigger_count, 1);

        let live = store
            .evaluate_gone_node(
                node,
                Some("uid-dry-run".to_string()),
                Duration::from_secs(60),
                Duration::from_secs(60),
                10,
                QueueGcMode::Live,
            )
            .await?;
        assert_eq!(live.eligible, preview.eligible);
        assert_eq!(live.deleted, preview.eligible);
        Ok(())
    }

    /// Retention, in the same tests as the claim grace below. Every fixture
    /// age falls between the two, or past the grace, so binding one in place
    /// of the other, or as milliseconds, changes an outcome.
    const RETENTION: Duration = Duration::from_secs(60);
    const CLAIM_GRACE: Duration = Duration::from_secs(600);
    /// Older than `RETENTION`, younger than `CLAIM_GRACE`.
    const BETWEEN: Duration = Duration::from_secs(300);
    /// Older than both.
    const PAST_GRACE: Duration = Duration::from_secs(1200);

    /// `prune_dead` and `list_gone_candidates` reach the store with the
    /// retention in seconds and the caller's limit.
    #[tokio::test(flavor = "current_thread")]
    async fn queue_db_store_prunes_dead_and_lists_candidates_by_retention_and_limit() -> TestResult
    {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let store = QueueDbStore::new(db.pool.clone());

        let ready = insert_node(&db.pool, "ready", "uid-dead").await?;
        insert_queue(&db.pool, ready, "old-a", BETWEEN, true).await?;
        insert_queue(&db.pool, ready, "old-b", BETWEEN, true).await?;
        insert_queue(&db.pool, ready, "recent", RETENTION / 2, true).await?;
        insert_trigger(&db.pool, ready, BETWEEN, true).await?;
        assert_eq!(store.prune_dead(RETENTION, 2).await?.total(), 2);
        assert_eq!(store.prune_dead(RETENTION, 10).await?.total(), 1);
        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM node_reconcile_queue WHERE node_id = $1",
        )
        .bind(ready)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(remaining, 1, "the dead ref within retention is kept");

        let mut listed = Vec::new();
        for (state, uid) in [("offline", "uid-offline"), ("registered", "uid-registered")] {
            let node = insert_node(&db.pool, state, uid).await?;
            insert_queue(&db.pool, node, "stale", BETWEEN, false).await?;
            listed.push(candidate(node, &format!("ip-{node}.ec2.internal"), uid));
        }
        let fresh = insert_node(&db.pool, "offline", "uid-fresh").await?;
        insert_queue(&db.pool, fresh, "fresh", RETENTION / 2, false).await?;
        listed.sort_by_key(|candidate| candidate.node_id);

        let page = store.list_gone_candidates(RETENTION, 1, None).await?;
        assert_eq!(page, listed[..1]);
        let rest = store
            .list_gone_candidates(RETENTION, 10, Some(page[0].node_id))
            .await?;
        assert_eq!(rest, listed[1..], "the fresh node is not a candidate");
        Ok(())
    }

    /// `evaluate_gone_node` binds the retention to the request age and the
    /// claim grace to the claim age, each in seconds, with the caller's limit.
    #[tokio::test(flavor = "current_thread")]
    async fn queue_db_store_binds_retention_and_claim_grace_to_their_own_checks() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let store = QueueDbStore::new(db.pool.clone());
        let evaluate = |node: NodeId, uid: &'static str, limit: u32| {
            store.evaluate_gone_node(
                node,
                Some(uid.to_owned()),
                RETENTION,
                CLAIM_GRACE,
                limit,
                QueueGcMode::Live,
            )
        };
        let counts = |queue, triggers| PruneCounts { queue, triggers };

        // Requested past retention, within the grace: both are pruned, one per
        // call under a limit of one.
        let requested = insert_node(&db.pool, "offline", "uid-requested").await?;
        insert_queue(&db.pool, requested, "ref", BETWEEN, false).await?;
        insert_trigger(&db.pool, requested, BETWEEN, false).await?;
        let first = evaluate(requested, "uid-requested", 1).await?;
        assert_eq!(first.deleted.total(), 1);
        assert_eq!(first.deleted, first.eligible);
        let second = evaluate(requested, "uid-requested", 1).await?;
        assert_eq!(second.deleted.total(), 1);
        assert_eq!(
            counts(
                first.deleted.queue + second.deleted.queue,
                first.deleted.triggers + second.deleted.triggers
            ),
            counts(1, 1)
        );

        // A ref claimed within the grace, but past retention, vetoes the node.
        let claimed = insert_node(&db.pool, "offline", "uid-claimed").await?;
        insert_queue(&db.pool, claimed, "ref", PAST_GRACE, false).await?;
        claim(&db.pool, Claimed::Refs, claimed, BETWEEN).await?;
        assert_eq!(
            evaluate(claimed, "uid-claimed", 10).await?,
            GoneNodeEvaluation::default()
        );
        assert_eq!(live_queue_count(&db.pool, claimed).await?, 1);

        // A ref claimed past the grace is pruned and vetoes nothing; the
        // trigger's own claim, within the grace, keeps only the trigger.
        let stale = insert_node(&db.pool, "offline", "uid-stale-claim").await?;
        insert_queue(&db.pool, stale, "ref", PAST_GRACE, false).await?;
        insert_trigger(&db.pool, stale, PAST_GRACE, false).await?;
        claim(&db.pool, Claimed::Refs, stale, PAST_GRACE).await?;
        claim(&db.pool, Claimed::Trigger, stale, BETWEEN).await?;
        assert_eq!(
            evaluate(stale, "uid-stale-claim", 10).await?,
            GoneNodeEvaluation {
                eligible: counts(1, 0),
                deleted: counts(1, 0),
            }
        );
        assert_eq!(live_queue_count(&db.pool, stale).await?, 0);
        let triggers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM node_reconcile_triggers WHERE node_id = $1",
        )
        .bind(stale)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(triggers, 1);
        Ok(())
    }
}
