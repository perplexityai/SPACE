use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use axum::http::{Method, StatusCode};
use k8s_openapi::api::core::v1::{Pod, PodCondition, PodSpec, PodStatus};
use pplx_space_accounts::{Email, OrgId, UserId};
use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_byte_size::MemorySize;
use pplx_space_cluster_pool::{NewPoolSlot, WarmPoolStore};
use pplx_space_content_digest::{ContentHash, Digest};
use pplx_space_execution_spec::VcpuCount;
use pplx_space_k8s_ids::PodUid;
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_pod::{SANDBOX_EXECUTION_GENERATION_ANNOTATION, SANDBOX_ID_ANNOTATION};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::controllers::substrate::kubernetes::pod_reflector::ReflectorReady;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    BoxError, KubeLog, KubeReply, WORKER_NODE, kube_status, pod_path, pod_reply, requests_matching,
    scripted_kube_client,
};
use crate::controllers::substrate::kubernetes::test_substrate::{
    PodCacheFeed, substrate_with_pod_cache_feed,
};
use crate::controllers::substrate::kubernetes::tests::{
    insert_k8s_execution, insert_test_sandbox_row,
};
use crate::controllers::substrate::{
    ActorRef, AdmittedPoolClaim, AdmittedPoolClaimLookup, PooledExecutionClaimRequest,
    SubstrateAdapter,
};
use crate::metrics::sandbox::api::{
    ClientClass, ClientIdTag, SandboxApiMetricIdentity, SandboxApiSurface,
};

type TestResult = Result<(), BoxError>;

const POD_NAME: &str = "space-sbx-read-g1";
const TEST_POD_IP: &str = "192.0.2.10";

fn test_pod(
    name: &str,
    uid: &str,
    sandbox_id: SandboxId,
    phase: &str,
    pod_ip: Option<&str>,
    node_name: Option<&str>,
) -> Pod {
    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("space".to_string()),
            uid: Some(uid.to_string()),
            annotations: Some(BTreeMap::from([
                (SANDBOX_ID_ANNOTATION.to_string(), sandbox_id.to_string()),
                (
                    SANDBOX_EXECUTION_GENERATION_ANNOTATION.to_string(),
                    "1".to_string(),
                ),
            ])),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: node_name.map(str::to_string),
            ..Default::default()
        }),
        status: Some(PodStatus {
            phase: Some(phase.to_string()),
            pod_ip: pod_ip.map(str::to_string),
            ..Default::default()
        }),
    }
}

fn substrate_with_feed_and_client(
    pool: PgPool,
    pods: Vec<Pod>,
    client: kube::Client,
) -> (KubernetesSubstrate, PodCacheFeed) {
    substrate_with_pod_cache_feed(pool, client, 8080, Some(pods))
}

#[derive(Clone, Copy)]
enum ReflectorHealthCase {
    HealthyQuietWatch,
    InitialSyncPending,
    WatchFaulted,
    MidRelist,
    RecoveredAfterRelist,
}

#[rstest::rstest]
#[case::healthy_quiet_watch_trusts_pending(ReflectorHealthCase::HealthyQuietWatch, 0)]
#[case::initial_sync_pending_falls_back(ReflectorHealthCase::InitialSyncPending, 1)]
#[case::watch_faulted_falls_back(ReflectorHealthCase::WatchFaulted, 1)]
#[case::mid_relist_falls_back(ReflectorHealthCase::MidRelist, 1)]
#[case::recovered_after_relist_trusts_pending(ReflectorHealthCase::RecoveredAfterRelist, 0)]
#[tokio::test(flavor = "current_thread")]
async fn read_sandbox_pod_trusts_cached_pending_only_when_reflector_is_healthy(
    #[case] health: ReflectorHealthCase,
    #[case] expected_gets: usize,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let pending = test_pod(POD_NAME, "uid-pending", sandbox_id, "Pending", None, None);
    let reply_pod = pending.clone();
    let (client, log) = scripted_kube_client(move |req| match req.method {
        Method::GET if req.path == pod_path(POD_NAME) => pod_reply(&reply_pod),
        _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
    });
    let (mut substrate, mut feed) =
        substrate_with_feed_and_client(db.pool.clone(), vec![pending.clone()], client);
    substrate.budgets.reflector_wait = Duration::from_secs(5);

    match health {
        ReflectorHealthCase::HealthyQuietWatch => {}
        ReflectorHealthCase::InitialSyncPending => {
            substrate.pod_reflector_ready = ReflectorReady::new();
        }
        ReflectorHealthCase::WatchFaulted => {
            feed.mark_watch_error();
        }
        ReflectorHealthCase::MidRelist => {
            feed.start_relist();
        }
        ReflectorHealthCase::RecoveredAfterRelist => {
            feed.mark_watch_error();
            feed.start_relist();
            feed.finish_relist([pending]);
        }
    }

    let observed = if expected_gets == 0 {
        futures_util::FutureExt::now_or_never(substrate.read_sandbox_pod(POD_NAME))
            .expect(
                "healthy reflector Pending read must complete on its first poll without awaiting reflector_wait or GET",
            )?
            .expect("pod exists")
    } else {
        tokio::time::timeout(Duration::from_secs(1), substrate.read_sandbox_pod(POD_NAME))
            .await
            .map_err(|_| "degraded reflector read must skip reflector_wait before fallback GET")??
            .expect("pod exists")
    };
    assert_eq!(observed.phase(), Some("Pending"));
    assert_eq!(observed.pod_ip(), None);
    assert_eq!(requests_matching(&log, &Method::GET).len(), expected_gets);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn resolve_reachable_pod_ip_bounds_fallback_get_and_preserves_nodemgr_guard() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "running").await?;
    insert_k8s_execution(
        &db.pool,
        sandbox_id,
        1,
        POD_NAME,
        "uid-1",
        Some(TEST_POD_IP),
        true,
    )
    .await?;

    let (client, log) = scripted_kube_client(|req| match req.method {
        Method::GET if req.path == pod_path(POD_NAME) => KubeReply::Hang,
        _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
    });
    let (mut substrate, feed) = substrate_with_feed_and_client(db.pool, vec![], client);
    substrate.budgets.reflector_wait = Duration::from_millis(10);
    substrate.budgets.readiness_pod_get = Duration::from_millis(35);
    feed.mark_watch_error();

    let err = tokio::time::timeout(
        Duration::from_millis(500),
        substrate.resolve_reachable_pod_ip(sandbox_id),
    )
    .await
    .map_err(|_| "resolve_reachable_pod_ip exceeded bounded readiness_pod_get")?
    .expect_err("stalled fallback GET must fail fast");
    assert!(
        matches!(err, SubstrateError::Unavailable { .. }),
        "expected retryable Unavailable from bounded reachable pod GET: {err:?}"
    );
    assert!(
        !is_nodemgr_unreachable(&err),
        "kube_api_unavailable errors must never be classified as node-mgr transport outages"
    );
    assert_eq!(requests_matching(&log, &Method::GET).len(), 1);
    Ok(())
}

#[rstest::rstest]
#[case::watch_error(false)]
#[case::relist_start(true)]
#[tokio::test(flavor = "current_thread")]
async fn read_sandbox_pod_aborts_reflector_wait_immediately_on_watch_fault(
    #[case] trigger_via_relist: bool,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    let running = test_pod(
        POD_NAME,
        "uid-1",
        sandbox_id,
        "Running",
        Some(TEST_POD_IP),
        Some(WORKER_NODE),
    );
    let (client, log) = scripted_kube_client(move |req| match req.method {
        Method::GET if req.path == pod_path(POD_NAME) => pod_reply(&running),
        _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
    });
    let (mut substrate, mut feed) = substrate_with_feed_and_client(db.pool, vec![], client);
    substrate.budgets.reflector_wait = Duration::from_secs(5);

    let reader = substrate.read_sandbox_pod(POD_NAME);
    let fault = async {
        feed.wait_for_subscriber().await;
        if trigger_via_relist {
            feed.start_relist();
        } else {
            feed.mark_watch_error();
        }
    };
    let started = Instant::now();
    let (observed, ()) = tokio::join!(reader, fault);
    let observed = observed?.expect("fallback GET finds the pod");

    assert!(
        started.elapsed() < Duration::from_millis(250),
        "watch fault or re-list start must wake reflector_wait early instead of stranding for 5s"
    );
    assert_eq!(observed.pod_ip(), Some(TEST_POD_IP));
    assert_eq!(requests_matching(&log, &Method::GET).len(), 1);
    Ok(())
}

#[rstest::rstest]
#[case::pending_predecessor("Pending", None)]
#[case::settled_running_predecessor("Running", Some("192.0.2.99"))]
#[tokio::test(flavor = "current_thread")]
async fn resolve_pod_instance_guards_successor_uid_against_lagging_predecessor(
    #[case] predecessor_phase: &str,
    #[case] predecessor_ip: Option<&str>,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "starting").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, POD_NAME, "uid-2", None, true).await?;

    let predecessor = test_pod(
        POD_NAME,
        "uid-1",
        sandbox_id,
        predecessor_phase,
        predecessor_ip,
        predecessor_ip.map(|_| WORKER_NODE),
    );
    let successor = test_pod(
        POD_NAME,
        "uid-2",
        sandbox_id,
        "Running",
        Some(TEST_POD_IP),
        Some(WORKER_NODE),
    );
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::BAD_REQUEST, "BadRequest"));
    let (mut substrate, mut feed) =
        substrate_with_feed_and_client(db.pool.clone(), vec![predecessor], client);
    substrate.budgets.reflector_wait = Duration::from_secs(2);

    let resolve = substrate.resolve_pod_instance(sandbox_id);
    let catch_up = async {
        feed.wait_for_subscriber().await;
        feed.apply(successor);
    };
    let (resolved, ()) = tokio::join!(resolve, catch_up);
    let resolved = resolved?.expect("successor pod resolves once reflector catches up");

    assert_eq!(resolved.ip.to_string(), TEST_POD_IP);
    assert_eq!(resolved.pod_uid.as_ref().map(PodUid::as_str), Some("uid-2"));
    let persisted_uid: String = sqlx::query_scalar(sql!(
        "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(persisted_uid, "uid-2");
    assert!(
        requests_matching(&log, &Method::GET).is_empty(),
        "reflector catch-up must satisfy the UID-pinned read with zero apiserver GETs"
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum UncachedRetirementCase {
    CacheMissNotFound,
    CacheMissTerminalFailed,
    CachedTerminalFailed,
}

#[rstest::rstest]
#[case::cache_miss_404_retires_after_one_get(UncachedRetirementCase::CacheMissNotFound, 1, 0, 1)]
#[case::cache_miss_terminal_deletes_after_one_get(
    UncachedRetirementCase::CacheMissTerminalFailed,
    1,
    1,
    2
)]
#[case::cached_terminal_verifies_once_then_deletes(
    UncachedRetirementCase::CachedTerminalFailed,
    1,
    1,
    2
)]
#[tokio::test(flavor = "current_thread")]
async fn resolve_pod_instance_deduplicates_uncached_retirement_gets(
    #[case] case: UncachedRetirementCase,
    #[case] expected_pre_delete_gets: usize,
    #[case] expected_deletes: usize,
    #[case] expected_total_gets: usize,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "running").await?;
    insert_k8s_execution(
        &db.pool,
        sandbox_id,
        1,
        POD_NAME,
        "uid-1",
        Some(TEST_POD_IP),
        true,
    )
    .await?;

    let terminal_pod = test_pod(POD_NAME, "uid-1", sandbox_id, "Failed", None, None);
    let cached = match case {
        UncachedRetirementCase::CachedTerminalFailed => vec![terminal_pod.clone()],
        UncachedRetirementCase::CacheMissNotFound
        | UncachedRetirementCase::CacheMissTerminalFailed => vec![],
    };
    let deleted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let pre_delete_gets = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let deleted_for_client = std::sync::Arc::clone(&deleted);
    let pre_delete_gets_for_client = std::sync::Arc::clone(&pre_delete_gets);
    let (client, log) = scripted_kube_client(move |req| match req.method {
        Method::GET if req.path == pod_path(POD_NAME) => {
            if deleted_for_client.load(std::sync::atomic::Ordering::Relaxed) {
                return kube_status(StatusCode::NOT_FOUND, "NotFound");
            }
            pre_delete_gets_for_client.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match case {
                UncachedRetirementCase::CacheMissNotFound => {
                    kube_status(StatusCode::NOT_FOUND, "NotFound")
                }
                UncachedRetirementCase::CacheMissTerminalFailed
                | UncachedRetirementCase::CachedTerminalFailed => pod_reply(&terminal_pod),
            }
        }
        Method::DELETE if req.path == pod_path(POD_NAME) => {
            deleted_for_client.store(true, std::sync::atomic::Ordering::Relaxed);
            kube_status(StatusCode::OK, "Success")
        }
        _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
    });
    let (mut substrate, _feed) = substrate_with_feed_and_client(db.pool.clone(), cached, client);
    substrate.budgets.reflector_wait = Duration::from_millis(20);

    let resolved = substrate.resolve_pod_instance(sandbox_id).await?;
    assert!(resolved.is_none());
    assert_eq!(
        pre_delete_gets.load(std::sync::atomic::Ordering::Relaxed),
        expected_pre_delete_gets,
        "must verify at most once via uncached GET before deleting/retiring"
    );
    assert_eq!(
        requests_matching(&log, &Method::DELETE).len(),
        expected_deletes
    );
    assert_eq!(
        requests_matching(&log, &Method::GET).len(),
        expected_total_gets
    );

    let active_rows: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(active_rows, 0, "retired execution row must be deactivated");
    Ok(())
}

#[rstest::rstest]
#[case::missing_ip("starting", "uid-1", None, false)]
#[case::missing_uid("starting", "", Some(TEST_POD_IP), false)]
#[case::cached_ip_and_uid("running", "uid-1", Some(TEST_POD_IP), true)]
#[tokio::test(flavor = "current_thread")]
async fn resolve_reachable_pod_ip_skips_k8s_lookups_without_persisted_ip_and_uid(
    #[case] lifecycle: &str,
    #[case] pod_uid: &str,
    #[case] pod_ip: Option<&str>,
    #[case] expects_fast_hit: bool,
) -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, lifecycle).await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, POD_NAME, pod_uid, pod_ip, true).await?;

    let reply_pod = test_pod(
        POD_NAME,
        "uid-1",
        sandbox_id,
        "Running",
        Some(TEST_POD_IP),
        None,
    );
    let cached = if expects_fast_hit {
        vec![reply_pod.clone()]
    } else {
        vec![]
    };
    let (client, log) = scripted_kube_client(move |req| match req.method {
        Method::GET if req.path == pod_path(POD_NAME) => pod_reply(&reply_pod),
        _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
    });
    let (mut substrate, _feed) = substrate_with_feed_and_client(db.pool, cached, client);
    substrate.budgets.reflector_wait = Duration::from_millis(20);

    let resolved = substrate.resolve_reachable_pod_ip(sandbox_id).await?;
    assert_eq!(resolved.is_some(), expects_fast_hit);
    assert!(
        requests_matching(&log, &Method::GET).is_empty(),
        "resolve_reachable_pod_ip must never issue apiserver GETs when cached DB metadata is incomplete"
    );
    Ok(())
}

struct WarmSlotHarness {
    db: pplx_space_db_test_fixtures::TestDb,
    substrate: KubernetesSubstrate,
    feed: Option<PodCacheFeed>,
    log: KubeLog,
    slot_id: Uuid,
    release_id: Uuid,
    reserved_sandbox_id: SandboxId,
    actor: ActorRef,
    env: BTreeMap<String, String>,
    auto_lifecycle: AutoLifecyclePolicy,
    network_policy: NetworkPolicyV1,
}

impl WarmSlotHarness {
    async fn new(
        cached_phase: &str,
        cached_ip: Option<&str>,
        kube_reply: impl Fn() -> KubeReply + Send + Sync + 'static,
    ) -> Result<Self, BoxError> {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let org_id = OrgId::new();
        let user_id = UserId::new();
        sqlx::query(sql!(
            "INSERT INTO organizations (id, name, slug) VALUES ($1, 'Pool Read Test', $2)"
        ))
        .bind(org_id)
        .bind(format!("pool-read-test-{org_id}"))
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
             VALUES ($1, $2, 'x', 'Pool Read', $3, 'member')"
        ))
        .bind(user_id)
        .bind(format!("pool-read-{user_id}@example.com"))
        .bind(org_id)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO templates (id, name, org_id, visibility)
             VALUES ($1, 'seed', NULL, 'generic')"
        ))
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO node_groups (id, name, org_id, template_policy, is_default)
             VALUES ($1, 'workers', NULL, 'default', TRUE)"
        ))
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        let release_id = Uuid::now_v7();
        let content_hash = ContentHash::new(Digest::sha256([0x55; 32]));
        sqlx::query(sql!(
            "INSERT INTO template_releases (id, template_name, type, hash)
             VALUES ($1, 'seed', 'fc', $2)"
        ))
        .bind(release_id)
        .bind(content_hash.to_string())
        .execute(&db.pool)
        .await?;
        let config_id = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO kubernetes_pool_configs (
                 id, template_release_id, vcpu, memory_mib, target_ready_slots,
                 refill_per_tick, max_warming_slots, namespace, enabled
             ) VALUES ($1, $2, 2, 1024, 1, 1, 1, 'space', TRUE)"
        ))
        .bind(config_id)
        .bind(release_id)
        .execute(&db.pool)
        .await?;

        let store = WarmPoolStore::new(pplx_space_postgres_pool::Db::from(db.pool.clone()));
        let reserved_sandbox_id = SandboxId::new();
        let slot = store
            .insert_warming_slot(NewPoolSlot {
                config_id,
                template_release_id: release_id,
                reserved_sandbox_id,
                template_name: &TemplateName::parse("seed")?,
                content_hash,
                vcpu: VcpuCount::try_from_u32(2)?,
                memory: MemorySize::mib(1024)?,
                pod_name: POD_NAME,
                pod_namespace: "space",
            })
            .await?;
        store
            .mark_slot_ready(slot.id, "pool-pod-uid", TEST_POD_IP, WORKER_NODE)
            .await?;

        let cached_pod = test_pod(
            POD_NAME,
            "pool-pod-uid",
            reserved_sandbox_id,
            cached_phase,
            cached_ip,
            Some(WORKER_NODE),
        );
        let (client, log) = scripted_kube_client(move |req| match req.method {
            Method::GET if req.path == pod_path(POD_NAME) => kube_reply(),
            _ => kube_status(StatusCode::BAD_REQUEST, "BadRequest"),
        });
        let (mut substrate, feed) =
            substrate_with_feed_and_client(db.pool.clone(), vec![cached_pod], client);
        substrate.budgets.reflector_wait = Duration::from_millis(30);
        substrate.budgets.readiness_pod_get = Duration::from_millis(40);

        Ok(Self {
            db,
            substrate,
            feed: Some(feed),
            log,
            slot_id: slot.id,
            release_id,
            reserved_sandbox_id,
            actor: ActorRef::new(user_id, org_id, Email::parse("pool-read@example.com")?),
            env: BTreeMap::new(),
            auto_lifecycle: AutoLifecyclePolicy::disabled(),
            network_policy: NetworkPolicyV1::default(),
        })
    }

    fn request(&self) -> PooledExecutionClaimRequest<'_> {
        PooledExecutionClaimRequest {
            metric_identity: SandboxApiMetricIdentity {
                op: "create",
                surface: SandboxApiSurface::Native,
                client_class: ClientClass::Other,
                client_id: ClientIdTag::UNKNOWN,
            },
            actor: &self.actor,
            template: "seed",
            content_hash: None,
            template_release_id: Some(self.release_id),
            runtime: "firecracker",
            vcpu: Some(2),
            memory_mib: Some(1024),
            env: &self.env,
            auto_lifecycle: &self.auto_lifecycle,
            network_policy_override: None,
            network_policy_generation: 1,
            template_network_policy: &self.network_policy,
            template_network_policy_generation: 1,
            volume_encryption: false,
            client_id: None,
        }
    }

    async fn pre_claim_admitted(&self) -> Result<Uuid, BoxError> {
        let claim_id = Uuid::now_v7();
        sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots
                SET state = 'claimed', claim_id = $2, claimed_at = NOW()
              WHERE id = $1"
        ))
        .bind(self.slot_id)
        .bind(claim_id)
        .execute(&self.db.pool)
        .await?;
        Ok(claim_id)
    }

    async fn slot_state(&self) -> Result<(String, Option<Uuid>), BoxError> {
        Ok(sqlx::query_as(sql!(
            "SELECT state, claim_id FROM kubernetes_pool_slots WHERE id = $1"
        ))
        .bind(self.slot_id)
        .fetch_one(&self.db.pool)
        .await?)
    }
}

#[derive(Clone, Copy)]
enum WarmSlotLagScenario {
    ReflectorCatchesUp,
    StalePredecessorUidCatchesUp,
    HealthyReflectorStillPending,
    HealthyReflectorUnschedulablePreBinding,
    DegradedFallback429,
    DegradedFallback503,
    DegradedFallbackGetTimeout,
    DegradedFallback404,
    DegradedWatchIgnoresCachedRunningAndSees404,
}

fn unschedulable_pending_pod(name: &str, uid: &str, sandbox_id: SandboxId) -> Pod {
    let mut pod = test_pod(name, uid, sandbox_id, "Pending", None, None);
    pod.status.get_or_insert_with(PodStatus::default).conditions = Some(vec![PodCondition {
        type_: "PodScheduled".to_string(),
        status: "False".to_string(),
        reason: Some("Unschedulable".to_string()),
        message: Some("0/3 nodes are available: insufficient memory.".to_string()),
        ..Default::default()
    }]);
    pod
}

#[rstest::rstest]
#[case::reflector_catches_up(WarmSlotLagScenario::ReflectorCatchesUp)]
#[case::stale_predecessor_uid_catches_up(WarmSlotLagScenario::StalePredecessorUidCatchesUp)]
#[case::healthy_reflector_still_pending(WarmSlotLagScenario::HealthyReflectorStillPending)]
#[case::healthy_reflector_unschedulable_pre_binding(
    WarmSlotLagScenario::HealthyReflectorUnschedulablePreBinding
)]
#[case::degraded_fallback_429(WarmSlotLagScenario::DegradedFallback429)]
#[case::degraded_fallback_503(WarmSlotLagScenario::DegradedFallback503)]
#[case::degraded_fallback_timeout(WarmSlotLagScenario::DegradedFallbackGetTimeout)]
#[case::degraded_fallback_404(WarmSlotLagScenario::DegradedFallback404)]
#[case::degraded_watch_ignores_cached_running_and_sees_404(
    WarmSlotLagScenario::DegradedWatchIgnoresCachedRunningAndSees404
)]
#[tokio::test(flavor = "current_thread")]
async fn warm_pool_claim_and_admitted_lookup_preserve_slots_across_reflector_lag_and_transient_gets(
    #[case] scenario: WarmSlotLagScenario,
) -> TestResult {
    let reply = move || match scenario {
        WarmSlotLagScenario::DegradedFallback429 => {
            kube_status(StatusCode::TOO_MANY_REQUESTS, "TooManyRequests")
        }
        WarmSlotLagScenario::DegradedFallback503 => {
            kube_status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable")
        }
        WarmSlotLagScenario::DegradedFallbackGetTimeout => KubeReply::Hang,
        WarmSlotLagScenario::DegradedFallback404
        | WarmSlotLagScenario::DegradedWatchIgnoresCachedRunningAndSees404 => {
            kube_status(StatusCode::NOT_FOUND, "NotFound")
        }
        WarmSlotLagScenario::ReflectorCatchesUp
        | WarmSlotLagScenario::StalePredecessorUidCatchesUp
        | WarmSlotLagScenario::HealthyReflectorStillPending
        | WarmSlotLagScenario::HealthyReflectorUnschedulablePreBinding => {
            kube_status(StatusCode::BAD_REQUEST, "UnexpectedUncachedGet")
        }
    };

    let catches_up = matches!(
        scenario,
        WarmSlotLagScenario::ReflectorCatchesUp | WarmSlotLagScenario::StalePredecessorUidCatchesUp
    );
    let (initial_phase, initial_ip) = match scenario {
        WarmSlotLagScenario::DegradedWatchIgnoresCachedRunningAndSees404 => {
            ("Running", Some(TEST_POD_IP))
        }
        _ => ("Pending", None),
    };

    // 1. Direct `claim_pooled_execution` path.
    let mut claim_harness = WarmSlotHarness::new(initial_phase, initial_ip, reply).await?;
    let mut claim_feed = claim_harness.feed.take().unwrap();
    if matches!(scenario, WarmSlotLagScenario::StalePredecessorUidCatchesUp) {
        claim_feed.apply(test_pod(
            POD_NAME,
            "stale-predecessor-uid",
            claim_harness.reserved_sandbox_id,
            "Running",
            Some("192.0.2.99"),
            Some(WORKER_NODE),
        ));
    }
    if matches!(
        scenario,
        WarmSlotLagScenario::HealthyReflectorUnschedulablePreBinding
    ) {
        claim_feed.apply(unschedulable_pending_pod(
            POD_NAME,
            "pool-pod-uid",
            claim_harness.reserved_sandbox_id,
        ));
    }
    if matches!(
        scenario,
        WarmSlotLagScenario::DegradedFallback429
            | WarmSlotLagScenario::DegradedFallback503
            | WarmSlotLagScenario::DegradedFallbackGetTimeout
            | WarmSlotLagScenario::DegradedFallback404
            | WarmSlotLagScenario::DegradedWatchIgnoresCachedRunningAndSees404
    ) {
        claim_feed.mark_watch_error();
    }
    if catches_up {
        claim_harness.substrate.budgets.reflector_wait = Duration::from_secs(2);
    }
    let req = claim_harness.request();
    let claim_attempt = if catches_up {
        let running = test_pod(
            POD_NAME,
            "pool-pod-uid",
            claim_harness.reserved_sandbox_id,
            "Running",
            Some(TEST_POD_IP),
            Some(WORKER_NODE),
        );
        let fut = claim_harness.substrate.claim_pooled_execution(&req);
        let catch_up = async move {
            claim_feed.wait_for_subscriber().await;
            claim_feed.apply(running);
        };
        let (res, ()) = tokio::join!(fut, catch_up);
        res?
    } else {
        claim_harness.substrate.claim_pooled_execution(&req).await?
    };

    match scenario {
        WarmSlotLagScenario::ReflectorCatchesUp
        | WarmSlotLagScenario::StalePredecessorUidCatchesUp => {
            assert!(claim_attempt.claim.is_some(), "caught-up slot adopts");
            assert_eq!(requests_matching(&claim_harness.log, &Method::GET).len(), 0);
        }
        WarmSlotLagScenario::HealthyReflectorStillPending
        | WarmSlotLagScenario::HealthyReflectorUnschedulablePreBinding => {
            assert!(claim_attempt.claim.is_none());
            assert_eq!(requests_matching(&claim_harness.log, &Method::GET).len(), 0);
            assert_eq!(
                claim_harness.slot_state().await?,
                ("ready".to_string(), None),
                "healthy reflector lag releases the claimed slot back to ready instead of invalidating it"
            );
        }
        WarmSlotLagScenario::DegradedFallback429
        | WarmSlotLagScenario::DegradedFallback503
        | WarmSlotLagScenario::DegradedFallbackGetTimeout => {
            assert!(claim_attempt.claim.is_none());
            assert_eq!(requests_matching(&claim_harness.log, &Method::GET).len(), 1);
            assert_eq!(
                claim_harness.slot_state().await?,
                ("ready".to_string(), None),
                "transient fallback GET failure releases the claimed slot back to ready"
            );
        }
        WarmSlotLagScenario::DegradedFallback404
        | WarmSlotLagScenario::DegradedWatchIgnoresCachedRunningAndSees404 => {
            assert!(claim_attempt.claim.is_none());
            // Up to 3 attempts in `claim_pooled_execution`, first invalidates the only slot.
            assert_eq!(requests_matching(&claim_harness.log, &Method::GET).len(), 1);
            assert_eq!(
                claim_harness.slot_state().await?,
                ("claimed".to_string(), None),
                "authoritative 404 invalidates the stale slot"
            );
        }
    }

    // 2. Admitted `lookup_admitted_pooled_execution` path.
    let mut admitted_harness = WarmSlotHarness::new(initial_phase, initial_ip, reply).await?;
    let mut admitted_feed = admitted_harness.feed.take().unwrap();
    let claim_id = admitted_harness.pre_claim_admitted().await?;
    if matches!(scenario, WarmSlotLagScenario::StalePredecessorUidCatchesUp) {
        admitted_feed.apply(test_pod(
            POD_NAME,
            "stale-predecessor-uid",
            admitted_harness.reserved_sandbox_id,
            "Running",
            Some("192.0.2.99"),
            Some(WORKER_NODE),
        ));
    }
    if matches!(
        scenario,
        WarmSlotLagScenario::HealthyReflectorUnschedulablePreBinding
    ) {
        admitted_feed.apply(unschedulable_pending_pod(
            POD_NAME,
            "pool-pod-uid",
            admitted_harness.reserved_sandbox_id,
        ));
    }
    if matches!(
        scenario,
        WarmSlotLagScenario::DegradedFallback429
            | WarmSlotLagScenario::DegradedFallback503
            | WarmSlotLagScenario::DegradedFallbackGetTimeout
            | WarmSlotLagScenario::DegradedFallback404
            | WarmSlotLagScenario::DegradedWatchIgnoresCachedRunningAndSees404
    ) {
        admitted_feed.mark_watch_error();
    }
    if catches_up {
        admitted_harness.substrate.budgets.reflector_wait = Duration::from_secs(2);
    }
    let admitted = AdmittedPoolClaim {
        claim_id,
        pool_slot_id: admitted_harness.slot_id,
    };
    let req = admitted_harness.request();
    let lookup = if catches_up {
        let running = test_pod(
            POD_NAME,
            "pool-pod-uid",
            admitted_harness.reserved_sandbox_id,
            "Running",
            Some(TEST_POD_IP),
            Some(WORKER_NODE),
        );
        let fut = admitted_harness.substrate.lookup_admitted_pooled_execution(
            &req,
            admitted,
            admitted_harness.reserved_sandbox_id,
        );
        let catch_up = async move {
            admitted_feed.wait_for_subscriber().await;
            admitted_feed.apply(running);
        };
        let (res, ()) = tokio::join!(fut, catch_up);
        res?
    } else {
        admitted_harness
            .substrate
            .lookup_admitted_pooled_execution(&req, admitted, admitted_harness.reserved_sandbox_id)
            .await?
    };

    match scenario {
        WarmSlotLagScenario::ReflectorCatchesUp
        | WarmSlotLagScenario::StalePredecessorUidCatchesUp => {
            assert!(matches!(lookup, AdmittedPoolClaimLookup::Adoptable { .. }));
            assert_eq!(
                requests_matching(&admitted_harness.log, &Method::GET).len(),
                0
            );
        }
        WarmSlotLagScenario::HealthyReflectorStillPending
        | WarmSlotLagScenario::HealthyReflectorUnschedulablePreBinding => {
            assert!(matches!(lookup, AdmittedPoolClaimLookup::Transient));
            assert_eq!(
                requests_matching(&admitted_harness.log, &Method::GET).len(),
                0
            );
            assert_eq!(
                admitted_harness.slot_state().await?,
                ("claimed".to_string(), Some(claim_id)),
                "healthy reflector lag keeps the admitted claim intact for retry"
            );
        }
        WarmSlotLagScenario::DegradedFallback429
        | WarmSlotLagScenario::DegradedFallback503
        | WarmSlotLagScenario::DegradedFallbackGetTimeout => {
            assert!(matches!(lookup, AdmittedPoolClaimLookup::Transient));
            assert_eq!(
                requests_matching(&admitted_harness.log, &Method::GET).len(),
                1
            );
            assert_eq!(
                admitted_harness.slot_state().await?,
                ("claimed".to_string(), Some(claim_id)),
                "transient fallback GET failure keeps the admitted claim intact for retry"
            );
        }
        WarmSlotLagScenario::DegradedFallback404
        | WarmSlotLagScenario::DegradedWatchIgnoresCachedRunningAndSees404 => {
            assert!(matches!(lookup, AdmittedPoolClaimLookup::ReleasePending));
            assert_eq!(
                requests_matching(&admitted_harness.log, &Method::GET).len(),
                1
            );
            assert_eq!(
                admitted_harness.slot_state().await?,
                ("claimed".to_string(), None),
                "authoritative 404 invalidates the admitted slot"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn resolve_pod_instance_404_preserves_and_reresolves_same_generation_successor_pod()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "running").await?;
    insert_k8s_execution(
        &db.pool,
        sandbox_id,
        1,
        POD_NAME,
        "uid-1",
        Some(TEST_POD_IP),
        true,
    )
    .await?;

    let successor_ip = "10.244.0.99";
    let successor_pod = test_pod(
        POD_NAME,
        "uid-2",
        sandbox_id,
        "Running",
        Some(successor_ip),
        None,
    );
    let pool_for_client = db.pool.clone();
    let gets = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gets_for_client = std::sync::Arc::clone(&gets);
    let (client, _log) =
        crate::controllers::substrate::kubernetes::scripted_test_doubles::scripted_kube_client_async(
            move |req| {
                let pool = pool_for_client.clone();
                let gets = std::sync::Arc::clone(&gets_for_client);
                let successor_pod = successor_pod.clone();
                async move {
                    if req.method != Method::GET || req.path != pod_path(POD_NAME) {
                        return kube_status(StatusCode::BAD_REQUEST, "BadRequest");
                    }
                    if gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        // Simulate a concurrent same-generation re-drive materializing
                        // `uid-2` after `active_execution_status` read `uid-1` and while
                        // the uncached GET observes `404` for the torn-down `uid-1`.
                        sqlx::query(sql!(
                            "UPDATE sandbox_k8s_executions
                                SET pod_uid = 'uid-2',
                                    pod_ip = $2::INET
                              WHERE sandbox_id = $1 AND generation = 1 AND active"
                        ))
                        .bind(sandbox_id)
                        .bind(successor_ip)
                        .execute(&pool)
                        .await
                        .expect("update same-generation successor pod UID");
                        kube_status(StatusCode::NOT_FOUND, "NotFound")
                    } else {
                        pod_reply(&successor_pod)
                    }
                }
            },
        );
    let (mut substrate, _feed) = substrate_with_feed_and_client(db.pool.clone(), vec![], client);
    substrate.budgets.reflector_wait = Duration::from_millis(20);

    let resolved = substrate
        .resolve_pod_instance(sandbox_id)
        .await?
        .expect("404 for stale UID must preserve and re-resolve same-generation successor");
    assert_eq!(resolved.ip.to_string(), successor_ip);
    assert_eq!(resolved.pod_uid.as_ref().map(PodUid::as_str), Some("uid-2"));
    assert_eq!(gets.load(std::sync::atomic::Ordering::SeqCst), 2);

    let active_uid: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox_id)
    .fetch_optional(&db.pool)
    .await?
    .flatten();
    assert_eq!(active_uid.as_deref(), Some("uid-2"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn resolve_pod_ip_for_readiness_poll_bypasses_cached_pending_pod_after_reflector_wait()
-> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    insert_test_sandbox_row(&db.pool, sandbox_id, "creating").await?;
    insert_k8s_execution(&db.pool, sandbox_id, 1, POD_NAME, "uid-1", None, true).await?;

    let cached_pending = test_pod(POD_NAME, "uid-1", sandbox_id, "Pending", None, None);
    let running_uncached = test_pod(
        POD_NAME,
        "uid-1",
        sandbox_id,
        "Running",
        Some(TEST_POD_IP),
        None,
    );
    let (client, log) = scripted_kube_client(move |req| {
        if req.method == Method::GET && req.path == pod_path(POD_NAME) {
            pod_reply(&running_uncached)
        } else {
            kube_status(StatusCode::BAD_REQUEST, "BadRequest")
        }
    });
    let (mut substrate, _feed) =
        substrate_with_feed_and_client(db.pool.clone(), vec![cached_pending], client);
    substrate.budgets.reflector_wait = Duration::from_millis(20);
    let target = PodProxyWaitTarget {
        pod_name: POD_NAME,
        pod_uid: Some("uid-1"),
        generation: 1,
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

    // Initial readiness check trusts the healthy reflector's `Pending` entry
    // without issuing a `GET` so `wait_for_pod_ip_assignment` proceeds straight
    // into its watch.
    assert_eq!(
        substrate
            .resolve_pod_ip_for_readiness(sandbox_id, &target, deadline)
            .await?,
        None
    );
    assert!(
        requests_matching(&log, &Method::GET).is_empty(),
        "initial readiness check must trust healthy cached Pending pod without GET"
    );

    // Post-watch fallback polling uses `SettledOnly`, so once `reflector_wait`
    // elapses with `Pending` still cached it falls through to an uncached `GET`
    // and recovers the `Running`+IP pod.
    let (ip, _source) = substrate
        .resolve_pod_ip_for_readiness_poll(sandbox_id, &target, deadline)
        .await?
        .expect("fallback poll must reach uncached GET when cached pod stays Pending");
    assert_eq!(ip.to_string(), TEST_POD_IP);
    assert_eq!(requests_matching(&log, &Method::GET).len(), 1);
    Ok(())
}
