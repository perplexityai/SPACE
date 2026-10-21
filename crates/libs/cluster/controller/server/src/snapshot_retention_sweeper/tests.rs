use pplx_space_common_core::types::SnapshotId;

use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use pplx_space_snapshot_retention::backend::{
    DeletePrefixError, DeletePrefixOutcome, StorageBackend,
};

use tokio_util::sync::CancellationToken;

use super::*;

#[test]
fn defaults_are_safe() {
    let env = pplx_space_test_env::TestEnv::default();
    let config = ReconcilerConfig::from_environment(&env);
    assert!(!config.enabled);
    assert!(!config.dry_run);
    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.keep_n, DEFAULT_KEEP_N);
    assert_eq!(config.max_per_tick, DEFAULT_MAX_PER_TICK);
    assert_eq!(config.max_concurrent_deletes.get(), 1);
    assert_eq!(config.min_age, Some(DEFAULT_MIN_AGE));
    assert_eq!(config.scoped_bucket, None);
}

#[test]
fn environment_overrides_policy_and_scope() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED", "true");
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN", "true");
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS", "120");
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N", "5");
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_MAX_PER_TICK", "42");
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS", "900");
    env.set("SPACE_SNAPSHOT_BUCKET", "my-bucket");

    let config = ReconcilerConfig::from_environment(&env);
    assert!(config.enabled);
    assert!(config.dry_run);
    assert_eq!(config.tick, Duration::from_secs(120));
    assert_eq!(config.keep_n, 5);
    assert_eq!(config.max_per_tick, 42);
    assert_eq!(config.min_age, Some(Duration::from_secs(900)));
    assert_eq!(config.scoped_bucket.as_deref(), Some("my-bucket"));
    assert_eq!(config.max_concurrent_deletes.get(), 1);
    let policy = RetentionPolicy::from(config);
    assert_eq!(policy.max_concurrent_deletes.get(), 1);
}

#[test]
fn dry_run_fails_toward_not_deleting() {
    for (value, expected) in [
        (None, false),
        (Some("true"), true),
        (Some("false"), false),
        (Some("yes"), true),
        (Some("no"), false),
        (Some("on"), true),
        (Some("off"), false),
        (Some(" TRUE "), true),
        (Some("bogus"), true),
        (Some(""), true),
    ] {
        let mut env = pplx_space_test_env::TestEnv::default();
        if let Some(value) = value {
            env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN", value);
        }
        let config = ReconcilerConfig::from_environment(&env);
        assert_eq!(config.dry_run, expected, "DRY_RUN={value:?}");
        assert_eq!(RetentionPolicy::from(config).dry_run, expected);
    }
}

#[test]
fn invalid_and_dangerous_values_fall_back_or_clamp() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS", "invalid");
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N", "0");
    env.set(
        "SPACE_SNAPSHOT_RETENTION_SWEEPER_MAX_PER_TICK",
        "30000000000",
    );
    env.set("SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS", "0");
    env.set("SPACE_SNAPSHOT_BUCKET", "   ");

    let config = ReconcilerConfig::from_environment(&env);
    assert_eq!(config.tick, DEFAULT_TICK);
    assert_eq!(config.keep_n, DEFAULT_KEEP_N);
    assert_eq!(config.max_per_tick, MAX_PER_TICK_CEILING);
    assert_eq!(config.min_age, None);
    assert_eq!(config.scoped_bucket, None);
}

#[derive(Clone)]
struct RecordingSink {
    scans: Arc<AtomicUsize>,
    limits: Option<Arc<std::sync::Mutex<Vec<i64>>>>,
}

impl RetentionSink for RecordingSink {
    async fn list_candidates(
        &self,
        _keep_n: i64,
        limit: i64,
        _after: Option<SnapshotId>,
        _min_age: Option<Duration>,
    ) -> Result<pplx_space_snapshot_retention::CandidatePage, sqlx::Error> {
        self.scans.fetch_add(1, Ordering::Relaxed);
        if let Some(limits) = &self.limits {
            limits.lock().unwrap().push(limit);
        }
        Ok(pplx_space_snapshot_retention::CandidatePage {
            exhausted: true,
            ..Default::default()
        })
    }

    async fn claimed_scan_epoch(&self) -> Result<Option<ClaimedScanEpoch>, sqlx::Error> {
        Ok(None)
    }

    async fn list_claimed(
        &self,
        _limit: i64,
        _after: Option<SnapshotId>,
        _through: SnapshotId,
    ) -> Result<Vec<PruneCandidate>, sqlx::Error> {
        Ok(Vec::new())
    }

    async fn claim_prune(
        &self,
        _candidate: PruneCandidate,
        _keep_n: i64,
        _min_age: Option<Duration>,
    ) -> Result<ClaimOutcome, sqlx::Error> {
        unreachable!("disabled and misconfigured loops do not claim")
    }

    async fn finalize_prune(
        &self,
        _snapshot_id: SnapshotId,
        _keep_n: i64,
    ) -> Result<PruneFinalize, sqlx::Error> {
        unreachable!("disabled and misconfigured loops do not finalize")
    }
}

struct NoopBackend;

impl StorageBackend for NoopBackend {
    fn scheme(&self) -> &'static str {
        "noop"
    }

    fn validate_route(&self, _connection_string: &str) -> Result<(), DeletePrefixError> {
        Ok(())
    }

    fn delete_prefix<'a>(
        &'a self,
        _connection_string: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<DeletePrefixOutcome, DeletePrefixError>> + Send + 'a>>
    {
        Box::pin(async { unreachable!("guarded loops do not delete") })
    }
}

async fn assert_guard_waits_without_scanning(
    config: ReconcilerConfig,
    registry: StorageBackendRegistry,
) {
    let scans = Arc::new(AtomicUsize::new(0));
    let token = CancellationToken::new();
    let task = tokio::spawn(run(
        config,
        RecordingSink {
            scans: scans.clone(),
            limits: None,
        },
        registry,
        FeatureFlagClient::disabled(),
        token.clone(),
    ));

    tokio::task::yield_now().await;
    tokio::time::advance(DEFAULT_TICK + Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(scans.load(Ordering::Relaxed), 0);
    assert!(!task.is_finished(), "guarded loop must wait for shutdown");

    token.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("guarded loop observes shutdown")
        .expect("guarded loop task succeeds");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn disabled_and_backendless_loops_wait_without_scanning() {
    let mut registered = StorageBackendRegistry::new();
    registered.register(Arc::new(NoopBackend));
    assert_guard_waits_without_scanning(ReconcilerConfig::default(), registered).await;

    let enabled = ReconcilerConfig {
        enabled: true,
        ..ReconcilerConfig::default()
    };
    assert_guard_waits_without_scanning(enabled, StorageBackendRegistry::new()).await;
}

#[test]
fn json_config_validates_pair_and_uses_stable_cluster_target() {
    use serde_json::json;

    let config = ReconcilerConfig {
        max_per_tick: 1500,
        max_concurrent_deletes: NonZeroU32::new(3).unwrap(),
        ..Default::default()
    };
    let cluster = ClusterId::parse("prod-1").unwrap();
    let evaluate = |value: Option<serde_json::Value>| {
        let mut calls = 0;
        let limits = effective_limits(&config, Some(&cluster), |key, subject| {
            calls += 1;
            assert_eq!(key, RETENTION_CONFIG_FLAG);
            assert_eq!(subject, "prod-1");
            value.clone()
        });
        assert_eq!(calls, 1, "one cached assignment supplies both limits");
        (limits.0.get(), limits.1.get())
    };
    for (value, expected) in [
        (None, (1500, 3)),
        (Some(json!({})), (1500, 3)),
        (Some(json!({"max_per_tick": 2000})), (2000, 3)),
        (Some(json!({"max_concurrent_deletes": 8})), (1500, 8)),
        (
            Some(json!({"max_per_tick": 1, "max_concurrent_deletes": 10_000})),
            (1, 10_000),
        ),
        (
            Some(json!({"max_per_tick": 10_000, "max_concurrent_deletes": 1})),
            (10_000, 1),
        ),
        (Some(json!(null)), (1500, 3)),
        (Some(json!([])), (1500, 3)),
        (Some(json!("malformed")), (1500, 3)),
        (Some(json!(true)), (1500, 3)),
        (Some(json!(1500)), (1500, 3)),
    ] {
        assert_eq!(evaluate(value.clone()), expected, "{value:?}");
    }
    for invalid in [
        json!(null),
        json!(true),
        json!("2"),
        json!([]),
        json!({}),
        json!(0),
        json!(-1),
        json!(10_001),
        json!(i64::MIN),
        json!(u64::MAX),
        json!(1.5),
        json!(2.0),
    ] {
        for key in ["max_per_tick", "max_concurrent_deletes"] {
            let mut value = json!({"max_per_tick": 2000, "max_concurrent_deletes": 8});
            value[key] = invalid.clone();
            assert_eq!(evaluate(Some(value.clone())), (1500, 3), "{value}");
        }
    }
    let fallback = (NonZeroU32::new(1500).unwrap(), NonZeroU32::new(3).unwrap());
    assert_eq!(
        effective_limits(&config, None, |_, _| panic!(
            "no shared unknown-cluster subject"
        )),
        fallback
    );
    let disabled = FeatureFlagClient::disabled();
    assert_eq!(
        effective_limits(&config, Some(&cluster), |key, subject| disabled.json_value(
            key,
            subject,
            Default::default()
        )),
        fallback
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn running_loop_evaluates_once_per_sweep_and_picks_up_updates() {
    let scans = Arc::new(AtomicUsize::new(0));
    let limits_seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let assigned = Arc::new(AtomicUsize::new(2));
    let evaluations = Arc::new(AtomicUsize::new(0));
    let token = CancellationToken::new();
    let mut registry = StorageBackendRegistry::new();
    registry.register(Arc::new(NoopBackend));
    let flag = assigned.clone();
    let calls = evaluations.clone();
    let task = tokio::spawn(run_with_limits(
        ReconcilerConfig {
            enabled: true,
            ..Default::default()
        },
        RecordingSink {
            scans: scans.clone(),
            limits: Some(limits_seen.clone()),
        },
        registry,
        token.clone(),
        move || {
            effective_limits(
                &ReconcilerConfig::default(),
                Some(&ClusterId::parse("prod-1").unwrap()),
                |_, _| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Some(serde_json::json!({
                        "max_per_tick": flag.load(Ordering::Relaxed),
                        "max_concurrent_deletes": 1,
                    }))
                },
            )
        },
    ));
    tokio::task::yield_now().await;
    assert_eq!(
        evaluations.load(Ordering::Relaxed),
        0,
        "preserve initial delay"
    );
    for (tick, value) in [2, 7, 1].into_iter().enumerate() {
        assigned.store(value, Ordering::Relaxed);
        tokio::time::advance(DEFAULT_TICK).await;
        tokio::task::yield_now().await;
        assert_eq!(evaluations.load(Ordering::Relaxed), tick + 1);
        assert_eq!(scans.load(Ordering::Relaxed), tick + 1);
    }
    // Discovery's existing 2x candidate cap reflects each sweep's new budget.
    assert_eq!(*limits_seen.lock().unwrap(), [4, 14, 2]);
    token.cancel();
    task.await.unwrap();
}

/// The env master layers over the provider override, not just over the loop.
///
/// `run_with_limits` returns before its first tick when the sweeper is
/// disabled, so on today's wiring the override is never consulted anyway. That
/// makes "env off suppresses the override" true for a reason that has nothing
/// to do with the policy, and an assertion driven through `run` would hold
/// with the policy deleted. So drive `limit_source` directly, where a resolved
/// override *is* reachable with the master off — the possible presence the
/// absence needs to mean anything.
#[test]
fn the_env_master_suppresses_a_resolved_retention_override() {
    let cluster: &'static ClusterId = Box::leak(Box::new(ClusterId::parse("prod-1").unwrap()));
    let retuned = || {
        FeatureFlagClient::fixed_json([(
            RETENTION_CONFIG_FLAG.key(),
            serde_json::json!({"max_per_tick": 2000, "max_concurrent_deletes": 8}),
        )])
    };
    let config = |enabled: bool| ReconcilerConfig {
        enabled,
        max_per_tick: 1500,
        max_concurrent_deletes: NonZeroU32::new(3).unwrap(),
        ..Default::default()
    };
    let limits = |enabled: bool, flags: FeatureFlagClient| {
        let (max_per_tick, max_concurrent_deletes) =
            limit_source(config(enabled), Some(cluster), flags)();
        (max_per_tick.get(), max_concurrent_deletes.get())
    };

    // Env on: the override retunes both limits. Without this row the next two
    // would pass with the whole override path removed.
    assert_eq!(limits(true, retuned()), (2000, 8));
    // Env off: the same resolved override cannot reach the sweeper.
    assert_eq!(limits(false, retuned()), (1500, 3));
    // Env on, no provider installed: the environment's own limits stand, which
    // for an override is the same answer as "nothing resolved" — there is no
    // declared fallback document to prefer over local configuration.
    assert_eq!(limits(true, FeatureFlagClient::disabled()), (1500, 3));
}
