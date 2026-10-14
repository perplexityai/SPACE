use pplx_space_common_core::types::{NodeId, SandboxId};

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

    use pplx_space_common_core::types::NodeId;
    use pplx_space_db_test_fixtures::{
        SEED_ORG_ID as SEEDED_ORG_ID, SEED_USER_ID as SEEDED_USER_ID, try_pg_test_db,
    };
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

    async fn claim_queue_ref(
        pool: &PgPool,
        node_id: NodeId,
        element: &str,
        claimed_ago: Duration,
    ) -> TestResult {
        sqlx::query(
            "UPDATE node_reconcile_queue
                SET claim_token = 'node-ctl',
                    claimed_at = NOW() - ($3 * INTERVAL '1 second')
              WHERE node_id = $1 AND element = $2",
        )
        .bind(node_id)
        .bind(element)
        .bind(i64::try_from(claimed_ago.as_secs()).unwrap_or(i64::MAX))
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
    async fn dead_prune_honors_retention_and_combined_limit() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node = insert_node(&db.pool, "ready", "uid-dead").await?;
        insert_queue(&db.pool, node, "old-a", Duration::from_secs(120), true).await?;
        insert_queue(&db.pool, node, "old-b", Duration::from_secs(120), true).await?;
        insert_queue(&db.pool, node, "recent", Duration::from_secs(10), true).await?;
        insert_trigger(&db.pool, node, Duration::from_secs(120), true).await?;
        let store = QueueDbStore::new(db.pool.clone());

        let first = store.prune_dead(Duration::from_secs(60), 2).await?;
        assert_eq!(first.total(), 2);
        let second = store.prune_dead(Duration::from_secs(60), 10).await?;
        assert_eq!(second.total(), 1);
        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)::BIGINT FROM node_reconcile_queue WHERE node_id = $1",
        )
        .bind(node)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(remaining, 1, "recent dead ref must be retained");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn candidate_scan_excludes_ready_draining_and_fresh() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let offline = insert_node(&db.pool, "offline", "uid-offline").await?;
        let registered = insert_node(&db.pool, "registered", "uid-registered").await?;
        let ready = insert_node(&db.pool, "ready", "uid-ready").await?;
        let draining = insert_node(&db.pool, "offline", "uid-draining").await?;
        let fresh = insert_node(&db.pool, "offline", "uid-fresh").await?;
        sqlx::query("UPDATE nodes SET drain_phase = 'draining' WHERE id = $1")
            .bind(draining)
            .execute(&db.pool)
            .await?;
        insert_queue(&db.pool, offline, "a", Duration::from_secs(120), false).await?;
        insert_queue(&db.pool, registered, "a", Duration::from_secs(120), false).await?;
        insert_queue(&db.pool, ready, "a", Duration::from_secs(120), false).await?;
        insert_queue(&db.pool, draining, "a", Duration::from_secs(120), false).await?;
        insert_queue(&db.pool, fresh, "a", Duration::from_secs(10), false).await?;
        let store = QueueDbStore::new(db.pool.clone());

        let mut ids: Vec<NodeId> = store
            .list_gone_candidates(Duration::from_secs(60), 100, None)
            .await?
            .into_iter()
            .map(|candidate| candidate.node_id)
            .collect();
        ids.sort();
        let mut expected = vec![offline, registered];
        expected.sort();
        assert_eq!(ids, expected, "ready + draining + fresh excluded");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gone_prune_rechecks_uid_state_and_drain_without_touching_desired_state() -> TestResult
    {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node = insert_node(&db.pool, "offline", "uid-old").await?;
        insert_queue(&db.pool, node, "old", Duration::from_secs(120), false).await?;
        insert_trigger(&db.pool, node, Duration::from_secs(120), false).await?;
        insert_queue(&db.pool, node, "recent", Duration::from_secs(10), false).await?;
        let sandbox_id = SandboxId::new();
        sqlx::query(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, active_node_id,
                 network_policy_generation
             ) VALUES ($1, $2::UUID, $3::UUID, 'started', $4, 1)",
        )
        .bind(sandbox_id)
        .bind(SEEDED_USER_ID)
        .bind(SEEDED_ORG_ID)
        .bind(node)
        .execute(&db.pool)
        .await?;
        let store = QueueDbStore::new(db.pool.clone());
        let grace = Duration::from_secs(60);

        // UID drift preserves refs.
        sqlx::query("UPDATE node_k8s SET k8s_node_uid = 'uid-new' WHERE node_id = $1")
            .bind(node)
            .execute(&db.pool)
            .await?;
        let drifted = store
            .evaluate_gone_node(
                node,
                Some("uid-old".to_string()),
                Duration::from_secs(60),
                grace,
                10,
                QueueGcMode::Live,
            )
            .await?;
        assert_eq!(
            drifted,
            GoneNodeEvaluation::default(),
            "UID drift must preserve refs"
        );
        sqlx::query("UPDATE node_k8s SET k8s_node_uid = 'uid-old' WHERE node_id = $1")
            .bind(node)
            .execute(&db.pool)
            .await?;

        // A `ready` node is preserved (state re-checked under the lock).
        sqlx::query("UPDATE nodes SET state = 'ready' WHERE id = $1")
            .bind(node)
            .execute(&db.pool)
            .await?;
        let ready = store
            .evaluate_gone_node(
                node,
                Some("uid-old".to_string()),
                Duration::from_secs(60),
                grace,
                10,
                QueueGcMode::Live,
            )
            .await?;
        assert_eq!(
            ready,
            GoneNodeEvaluation::default(),
            "ready node must be preserved"
        );
        sqlx::query("UPDATE nodes SET state = 'offline' WHERE id = $1")
            .bind(node)
            .execute(&db.pool)
            .await?;

        let evaluation = store
            .evaluate_gone_node(
                node,
                Some("uid-old".to_string()),
                Duration::from_secs(60),
                grace,
                10,
                QueueGcMode::Live,
            )
            .await?;
        assert_eq!(
            evaluation,
            GoneNodeEvaluation {
                eligible: PruneCounts {
                    queue: 1,
                    triggers: 1
                },
                deleted: PruneCounts {
                    queue: 1,
                    triggers: 1
                },
            }
        );

        let desired: (String, Option<NodeId>) =
            sqlx::query_as("SELECT lifecycle, active_node_id FROM sandboxes WHERE id = $1")
                .bind(sandbox_id)
                .fetch_one(&db.pool)
                .await?;
        assert_eq!(desired, ("started".to_string(), Some(node)));
        assert_eq!(
            live_queue_count(&db.pool, node).await?,
            1,
            "fresh ref retained"
        );
        let node_still_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes WHERE id = $1)")
                .bind(node)
                .fetch_one(&db.pool)
                .await?;
        assert!(node_still_exists);
        Ok(())
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

    #[tokio::test(flavor = "current_thread")]
    async fn active_queue_claim_preserves_all_node_refs() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let node = insert_node(&db.pool, "offline", "uid-node").await?;
        insert_queue(
            &db.pool,
            node,
            "fresh-claim",
            Duration::from_secs(300),
            false,
        )
        .await?;
        insert_queue(
            &db.pool,
            node,
            "stale-claim",
            Duration::from_secs(300),
            false,
        )
        .await?;
        insert_queue(&db.pool, node, "unclaimed", Duration::from_secs(300), false).await?;
        claim_queue_ref(&db.pool, node, "fresh-claim", Duration::from_secs(5)).await?;
        claim_queue_ref(&db.pool, node, "stale-claim", Duration::from_secs(600)).await?;
        let store = QueueDbStore::new(db.pool.clone());

        for mode in [QueueGcMode::DryRun, QueueGcMode::Live] {
            let evaluation = store
                .evaluate_gone_node(
                    node,
                    Some("uid-node".to_string()),
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                    10,
                    mode,
                )
                .await?;
            assert_eq!(
                evaluation,
                GoneNodeEvaluation::default(),
                "a fresh node-ctl queue claim vetoes pruning for the entire node in {mode}"
            );
        }
        assert_eq!(live_queue_count(&db.pool, node).await?, 3);
        Ok(())
    }

    /// The candidate scan and the gone-node evaluation both bind the
    /// Kubernetes substrate: a direct node with the same stale refs is neither
    /// listed nor evaluated eligible, and keeps its refs.
    #[tokio::test(flavor = "current_thread")]
    async fn direct_nodes_are_neither_listed_nor_pruned() -> TestResult {
        let Some(db) = try_pg_test_db().await? else {
            return Ok(());
        };
        let k8s = insert_node(&db.pool, "offline", "uid-k8s").await?;
        let direct = insert_node(&db.pool, "offline", "uid-direct").await?;
        sqlx::query("UPDATE nodes SET managed_by = 'space' WHERE id = $1")
            .bind(direct)
            .execute(&db.pool)
            .await?;
        for node in [k8s, direct] {
            insert_queue(&db.pool, node, "old", Duration::from_secs(120), false).await?;
        }
        let store = QueueDbStore::new(db.pool.clone());

        let ids: Vec<NodeId> = store
            .list_gone_candidates(Duration::from_secs(60), 100, None)
            .await?
            .into_iter()
            .map(|candidate| candidate.node_id)
            .collect();
        assert_eq!(ids, vec![k8s]);

        for (node, uid, expected) in [(k8s, "uid-k8s", 1), (direct, "uid-direct", 0)] {
            let evaluation = store
                .evaluate_gone_node(
                    node,
                    Some(uid.to_string()),
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                    10,
                    QueueGcMode::Live,
                )
                .await?;
            assert_eq!(evaluation.deleted.queue, expected);
            assert_eq!(live_queue_count(&db.pool, node).await?, 1 - expected as i64);
        }
        Ok(())
    }
}
