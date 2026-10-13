use pplx_space_common_core::types::{NodeId, SandboxId};

use super::*;
use std::collections::BTreeMap;

use crate::controllers::substrate::ActorRef;
use pplx_space_cluster_pool::PoolConfigOwner;
use pplx_space_common_core::types::{AutoLifecyclePolicy, Email, OrgId, UserId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_net_common::NetworkPolicyV1;
use sqlx::PgPool;

use pplx_space_test_env::TestResult;
type PooledExecutionRow = (
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    bool,
    Option<Uuid>,
);

struct ReadyPoolSlotFixture {
    org_id: OrgId,
    user_id: UserId,
    config_id: Uuid,
    release_id: Uuid,
    sandbox_id: SandboxId,
    node_id: NodeId,
    content_hash: String,
}

async fn insert_ready_pool_slot(pool: &PgPool) -> TestResult<ReadyPoolSlotFixture> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    let config_id = Uuid::now_v7();
    let latest_release_id = Uuid::now_v7();
    let newer_untagged_release_id = Uuid::now_v7();
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    let content_hash = "sha256:latest".to_string();

    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Pool API Test', $2)"
    ))
    .bind(org_id)
    .bind(format!("pool-api-test-{org_id}"))
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Pool API Test', $3, 'member')"
    ))
    .bind(user_id)
    .bind(format!("pool-api-test-{user_id}@example.com"))
    .bind(org_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id) VALUES ($1, 'seed', $2)"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, created_at)
         VALUES
           ($1, 'seed', 'fc', $2, NOW() - INTERVAL '1 hour'),
           ($3, 'seed', 'fc', 'sha256:newer-untagged', NOW())"
    ))
    .bind(latest_release_id)
    .bind(&content_hash)
    .bind(newer_untagged_release_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'seed', 'latest')"
    ))
    .bind(latest_release_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, 4, 2048, 1, 1, 1, 'space', true)"
    ))
    .bind(config_id)
    .bind(latest_release_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, ready_at
         )
         VALUES (
             $1, $2, $3, 'seed', $4, 4, 2048, 'space-sbx-pooled-g1',
             'space', 'pod-uid', '10.90.0.11'::inet, 'pool-node-1', 'ready', NOW()
         )"
    ))
    .bind(config_id)
    .bind(latest_release_id)
    .bind(sandbox_id)
    .bind(&content_hash)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10'::inet, 'worker', 'k8s', 'registered')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'pool-node-1', 'node-uid')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;

    Ok(ReadyPoolSlotFixture {
        org_id,
        user_id,
        config_id,
        release_id: latest_release_id,
        sandbox_id,
        node_id,
        content_hash,
    })
}

async fn insert_pool_config(
    pool: &PgPool,
    release_id: Uuid,
    vcpu: i32,
    memory_mib: i32,
    target_ready_slots: i32,
    enabled: bool,
) -> TestResult<Uuid> {
    let config_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled
         )
         VALUES ($1, $2, $3, $4, $5, 1, $5, 'space', $6)"
    ))
    .bind(config_id)
    .bind(release_id)
    .bind(vcpu)
    .bind(memory_mib)
    .bind(target_ready_slots)
    .bind(enabled)
    .execute(pool)
    .await?;
    Ok(config_id)
}

async fn insert_ready_slot(
    pool: &PgPool,
    config_id: Uuid,
    release_id: Uuid,
    content_hash: &str,
    vcpu: i32,
    memory_mib: i32,
    pod: (&str, &str),
) -> TestResult<SandboxId> {
    let (pod_name, pod_ip) = pod;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, ready_at
         )
         VALUES (
             $1, $2, $3, 'seed', $4, $5, $6, $7,
             'space', 'pod-uid', $8::inet, 'pool-node-1', 'ready', NOW()
         )"
    ))
    .bind(config_id)
    .bind(release_id)
    .bind(sandbox_id)
    .bind(content_hash)
    .bind(vcpu)
    .bind(memory_mib)
    .bind(pod_name)
    .bind(pod_ip)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

async fn insert_claimed_sandbox(
    tx: &mut Transaction<'_, Postgres>,
    claim: &PooledExecutionClaim,
    user_id: UserId,
    org_id: OrgId,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation
         )
         VALUES ($1, $2, $3, 'k8s', 'paused', 'on_node', $4, $4, 1)"
    ))
    .bind(claim.sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .bind(claim.active_node_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn pooled_claim_request<'a>(
    actor: &'a ActorRef,
    env: &'a BTreeMap<String, String>,
    auto_lifecycle: &'a AutoLifecyclePolicy,
    template_network_policy: &'a NetworkPolicyV1,
) -> PooledExecutionClaimRequest<'a> {
    PooledExecutionClaimRequest {
        metric_identity: SandboxApiMetricIdentity::native("test"),
        actor,
        template: "seed",
        content_hash: None,
        template_release_id: None,
        runtime: "fc",
        vcpu: None,
        memory_mib: None,
        env,
        auto_lifecycle,
        network_policy_override: None,
        network_policy_generation: 0,
        template_network_policy,
        template_network_policy_generation: 1,
        volume_encryption: false,
    }
}

fn claim_request_with_shape<'a>(
    actor: &'a ActorRef,
    env: &'a BTreeMap<String, String>,
    auto_lifecycle: &'a AutoLifecyclePolicy,
    template_network_policy: &'a NetworkPolicyV1,
    release_id: Uuid,
    content_hash: &'a str,
    shape: Option<PooledExecutionPoolShape>,
) -> PooledExecutionClaimRequest<'a> {
    let mut request = pooled_claim_request(actor, env, auto_lifecycle, template_network_policy);
    request.template_release_id = Some(release_id);
    request.content_hash = Some(content_hash);
    request.vcpu = shape.map(|shape| shape.vcpu);
    request.memory_mib = shape.map(|shape| shape.memory_mib);
    request
}

fn claim_test_context(
    slot: &ReadyPoolSlotFixture,
) -> TestResult<(
    ActorRef,
    BTreeMap<String, String>,
    AutoLifecyclePolicy,
    NetworkPolicyV1,
)> {
    Ok((
        ActorRef::new(slot.user_id, slot.org_id, Email::parse("pool@example.com")?),
        BTreeMap::new(),
        AutoLifecyclePolicy::disabled(),
        NetworkPolicyV1::deny_all(),
    ))
}

#[test]
fn pool_eligibility_requires_fc_pool_boot_and_wake_path() {
    assert!(pool_eligible_for_request(
        super::super::super::CreateBootMode::Fallthrough,
        "fc",
        false,
        None,
        false,
        true,
    ));
    assert!(pool_eligible_for_request(
        super::super::super::CreateBootMode::Fallthrough,
        "fc",
        true,
        None,
        false,
        false,
    ));
    assert!(!pool_eligible_for_request(
        super::super::super::CreateBootMode::Cold,
        "fc",
        false,
        None,
        false,
        true,
    ));
    assert!(!pool_eligible_for_request(
        super::super::super::CreateBootMode::Fallthrough,
        "runc",
        false,
        None,
        false,
        true,
    ));
    assert!(!pool_eligible_for_request(
        super::super::super::CreateBootMode::Fallthrough,
        "fc",
        false,
        None,
        false,
        false,
    ));
}

#[test]
fn pool_eligibility_skips_requests_requiring_immediate_runtime_work() {
    assert!(!pool_eligible_for_request(
        super::super::super::CreateBootMode::Fallthrough,
        "fc",
        false,
        None,
        true,
        true,
    ));
    assert!(!pool_eligible_for_request(
        super::super::super::CreateBootMode::Fallthrough,
        "fc",
        false,
        Some(1024),
        false,
        true,
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_claim_and_bind_pool_slot() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let actor = ActorRef::new(slot.user_id, slot.org_id, Email::parse("pool@example.com")?);
    let env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let template_network_policy = NetworkPolicyV1::deny_all();
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);

    let claim = claim_pooled_execution(&db.pool, &request, "space").await?;
    assert_eq!(
        claim.pool_shape,
        Some(PooledExecutionPoolShape::new(4, 2048, Default::default()))
    );
    let claim = claim.claim.expect("ready pool slot should be claimed");

    assert_eq!(claim.sandbox_id, slot.sandbox_id);
    assert_eq!(claim.content_hash, slot.content_hash);
    assert_eq!(claim.pod_name, "space-sbx-pooled-g1");
    assert_eq!(claim.execution_generation, 1);
    assert_eq!(claim.pod_ip, "10.90.0.11");
    assert_eq!(claim.vcpu, Some(4));
    assert_eq!(claim.memory_mib, Some(2048));
    assert_eq!(claim.active_node_id, Some(slot.node_id));
    let expected_slot_id = claim
        .pool_slot_id
        .expect("Kubernetes pooled claim should carry its slot id");

    let mut tx = db.pool.begin().await?;
    insert_claimed_sandbox(&mut tx, &claim, slot.user_id, slot.org_id).await?;
    persist_pooled_execution_claim(&mut tx, &request, &claim).await?;
    tx.commit().await?;

    let (state, claim_id): (String, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT state, claim_id
         FROM kubernetes_pool_slots
         WHERE reserved_sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(state, "bound");
    assert_eq!(claim_id, Some(claim.claim_id));

    let (
        generation,
        pod_name,
        pod_namespace,
        pod_uid,
        pod_ip,
        k8s_node_name,
        state,
        active,
        route_activation_id,
    ): PooledExecutionRow = sqlx::query_as(sql!(
        "SELECT generation, pod_name, pod_namespace, pod_uid, host(pod_ip),
                k8s_node_name, state, active, route_activation_id
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(generation, 1);
    assert_eq!(pod_name, "space-sbx-pooled-g1");
    assert_eq!(pod_namespace, "space");
    assert_eq!(pod_uid.as_deref(), Some("pod-uid"));
    assert_eq!(pod_ip.as_deref(), Some("10.90.0.11"));
    assert_eq!(k8s_node_name.as_deref(), Some("pool-node-1"));
    assert_eq!(state, "running");
    assert!(active);
    assert_eq!(route_activation_id, Some(expected_slot_id));
    assert_ne!(route_activation_id, Some(claim.claim_id));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_claim_requires_matching_disk_capacity_and_reports_it() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let actor = ActorRef::new(
        slot.user_id,
        slot.org_id,
        Email::parse("pool-disk@example.com")?,
    );
    let env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let template_network_policy = NetworkPolicyV1::deny_all();
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);

    sqlx::query("UPDATE kubernetes_pool_configs SET disk_mib = 32768 WHERE id = $1")
        .bind(slot.config_id)
        .execute(&db.pool)
        .await?;

    let stale = claim_pooled_execution(&db.pool, &request, "space").await?;
    assert!(stale.claim.is_none());
    assert_eq!(
        stale.pool_shape,
        Some(PooledExecutionPoolShape::with_disk_mib(
            4,
            2048,
            Some(32768),
            Default::default(),
        ))
    );

    sqlx::query("UPDATE kubernetes_pool_slots SET disk_mib = 32768 WHERE config_id = $1")
        .bind(slot.config_id)
        .execute(&db.pool)
        .await?;

    let matched = claim_pooled_execution(&db.pool, &request, "space").await?;
    assert_eq!(
        matched.claim.as_ref().and_then(|claim| claim.disk_mib),
        Some(32768)
    );
    assert_eq!(
        matched.pool_shape.and_then(|shape| shape.disk_mib),
        Some(32768)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_default_shape_hit_reports_claimed_ready_slot_shape() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let preferred_config = insert_pool_config(&db.pool, slot.release_id, 8, 8192, 10, true).await?;
    let disabled_config =
        insert_pool_config(&db.pool, slot.release_id, 16, 16384, 20, false).await?;
    let disabled_sandbox_id = insert_ready_slot(
        &db.pool,
        disabled_config,
        slot.release_id,
        &slot.content_hash,
        16,
        16384,
        ("space-sbx-disabled-shape-g1", "10.90.0.12"),
    )
    .await?;
    let (actor, env, auto_lifecycle, template_network_policy) = claim_test_context(&slot)?;
    let request = claim_request_with_shape(
        &actor,
        &env,
        &auto_lifecycle,
        &template_network_policy,
        slot.release_id,
        &slot.content_hash,
        None,
    );

    let attempt = claim_pooled_execution(&db.pool, &request, "space").await?;

    assert_eq!(
        attempt.pool_shape,
        Some(PooledExecutionPoolShape::new(4, 2048, Default::default()))
    );
    assert_eq!(
        attempt
            .claim
            .expect("default request should claim a ready slot")
            .sandbox_id,
        slot.sandbox_id
    );
    let disabled_state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM kubernetes_pool_slots WHERE reserved_sandbox_id = $1"
    ))
    .bind(disabled_sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(disabled_state, "ready");
    let preferred_ready_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM kubernetes_pool_slots WHERE config_id = $1 AND state = 'ready'"
    ))
    .bind(preferred_config)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(preferred_ready_count, 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_default_shape_miss_reports_preferred_enabled_config() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_slots SET state = 'claimed' WHERE config_id = $1"
    ))
    .bind(slot.config_id)
    .execute(&db.pool)
    .await?;
    insert_pool_config(&db.pool, slot.release_id, 8, 8192, 10, true).await?;
    insert_pool_config(&db.pool, slot.release_id, 16, 16384, 20, false).await?;
    let (actor, env, auto_lifecycle, template_network_policy) = claim_test_context(&slot)?;
    let request = claim_request_with_shape(
        &actor,
        &env,
        &auto_lifecycle,
        &template_network_policy,
        slot.release_id,
        &slot.content_hash,
        None,
    );

    let attempt = claim_pooled_execution(&db.pool, &request, "space").await?;

    assert!(attempt.claim.is_none());
    assert_eq!(
        attempt.pool_shape,
        Some(PooledExecutionPoolShape::new(8, 8192, Default::default()))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_explicit_shape_hit_and_miss_stay_exact() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let explicit_shape = PooledExecutionPoolShape::new(8, 8192, Default::default());
    let explicit_config = insert_pool_config(&db.pool, slot.release_id, 8, 8192, 10, true).await?;
    let explicit_sandbox_id = insert_ready_slot(
        &db.pool,
        explicit_config,
        slot.release_id,
        &slot.content_hash,
        8,
        8192,
        ("space-sbx-explicit-shape-g1", "10.90.0.13"),
    )
    .await?;
    let (actor, env, auto_lifecycle, template_network_policy) = claim_test_context(&slot)?;
    let request = claim_request_with_shape(
        &actor,
        &env,
        &auto_lifecycle,
        &template_network_policy,
        slot.release_id,
        &slot.content_hash,
        Some(explicit_shape),
    );

    let hit = claim_pooled_execution(&db.pool, &request, "space").await?;
    assert_eq!(hit.pool_shape, Some(explicit_shape));
    assert_eq!(
        hit.claim
            .expect("explicit request should claim its exact shape")
            .sandbox_id,
        explicit_sandbox_id
    );

    let miss = claim_pooled_execution(&db.pool, &request, "space").await?;
    assert!(miss.claim.is_none());
    assert_eq!(miss.pool_shape, Some(explicit_shape));
    let other_shape_state: String = sqlx::query_scalar(sql!(
        "SELECT state FROM kubernetes_pool_slots WHERE reserved_sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(other_shape_state, "ready");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_claim_blank_pool_slot_pod_uid_as_absent() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_slots
         SET pod_uid = '   '
         WHERE reserved_sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .execute(&db.pool)
    .await?;

    let actor = ActorRef::new(slot.user_id, slot.org_id, Email::parse("pool@example.com")?);
    let env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let template_network_policy = NetworkPolicyV1::deny_all();
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);

    let claim = claim_pooled_execution(&db.pool, &request, "space")
        .await?
        .claim
        .expect("ready pool slot should be claimed");
    assert_eq!(claim.pod_uid, None);

    let mut tx = db.pool.begin().await?;
    insert_claimed_sandbox(&mut tx, &claim, slot.user_id, slot.org_id).await?;
    persist_pooled_execution_claim(&mut tx, &request, &claim).await?;
    tx.commit().await?;

    let execution_pod_uid: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid
         FROM sandbox_k8s_executions
         WHERE sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(execution_pod_uid, None);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_release_claimed_pool_slot_restores_ready_slot() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let actor = ActorRef::new(slot.user_id, slot.org_id, Email::parse("pool@example.com")?);
    let env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let template_network_policy = NetworkPolicyV1::deny_all();
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);
    let claim = claim_pooled_execution(&db.pool, &request, "space")
        .await?
        .claim
        .expect("ready pool slot should be claimed");

    release_claimed_pool_slot(&db.pool, &claim).await?;

    let (state, claim_id, claimed_at): (String, Option<Uuid>, Option<time::OffsetDateTime>) =
        sqlx::query_as(sql!(
            "SELECT state, claim_id, claimed_at
             FROM kubernetes_pool_slots
             WHERE reserved_sandbox_id = $1"
        ))
        .bind(slot.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(state, "ready");
    assert_eq!(claim_id, None);
    assert_eq!(claimed_at, None);

    let reclaimed = claim_pooled_execution(&db.pool, &request, "space")
        .await?
        .claim
        .expect("released pool slot should be claimable again");
    assert_eq!(reclaimed.sandbox_id, claim.sandbox_id);
    assert_ne!(reclaimed.claim_id, claim.claim_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_invalidate_claimed_pool_slot_keeps_slot_unclaimable() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let actor = ActorRef::new(slot.user_id, slot.org_id, Email::parse("pool@example.com")?);
    let env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let template_network_policy = NetworkPolicyV1::deny_all();
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);
    let claim = claim_pooled_execution(&db.pool, &request, "space")
        .await?
        .claim
        .expect("ready pool slot should be claimed");

    invalidate_claimed_pool_slot(&db.pool, &claim).await?;

    let (state, claim_id, claimed_at): (String, Option<Uuid>, Option<time::OffsetDateTime>) =
        sqlx::query_as(sql!(
            "SELECT state, claim_id, claimed_at
             FROM kubernetes_pool_slots
             WHERE reserved_sandbox_id = $1"
        ))
        .bind(slot.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(state, "claimed");
    assert_eq!(claim_id, None);
    assert!(claimed_at.is_some());

    let reclaimed = claim_pooled_execution(&db.pool, &request, "space").await?;
    assert!(reclaimed.claim.is_none());
    assert_eq!(
        reclaimed.pool_shape,
        Some(PooledExecutionPoolShape::new(4, 2048, Default::default()))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn upsert_pool_config_uses_store_namespace() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::kubernetes::store::MaterializationMode::Fenced,
    );

    let config = upsert_pool_config(
        &store,
        ClusterPoolConfigRequest {
            template_name: "seed".to_string(),
            content_hash: Some(slot.content_hash),
            vcpu: 4,
            memory_mib: 2048,
            disk_mib: None,
            reset_disk_to_template_default: false,
            target_ready_slots: 1,
            refill_per_tick: 1,
            max_warming_slots: 1,
            namespace: Some("space".to_string()),
            enabled: true,
            encryptable: false,
        },
    )
    .await?;

    assert_eq!(config.namespace, "sandboxes");
    let namespace: String = sqlx::query_scalar(sql!(
        "SELECT namespace
         FROM kubernetes_pool_configs
         WHERE id = $1"
    ))
    .bind(config.id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(namespace, "sandboxes");
    Ok(())
}

/// No encryptable config may be created through the operator surface —
/// enabled or not — while the legacy release/shape index still forbids twin
/// rows and members cannot carry the encryptable shape.
#[tokio::test(flavor = "current_thread")]
async fn upsert_pool_config_rejects_encryptable_configs() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::kubernetes::store::MaterializationMode::Fenced,
    );
    let request = |enabled| ClusterPoolConfigRequest {
        template_name: "seed".to_string(),
        content_hash: Some(slot.content_hash.clone()),
        vcpu: 8,
        memory_mib: 4096,
        disk_mib: None,
        reset_disk_to_template_default: false,
        target_ready_slots: 2,
        refill_per_tick: 1,
        max_warming_slots: 1,
        namespace: None,
        enabled,
        encryptable: true,
    };

    for enabled in [true, false] {
        let rejected = upsert_pool_config(&store, request(enabled))
            .await
            .expect_err("encryptable configs must be refused until the index drop");
        assert_eq!(rejected.status(), axum::http::StatusCode::BAD_REQUEST);
        assert!(
            rejected
                .to_string()
                .contains("encryptable pool configs cannot be created yet"),
            "unexpected rejection: {rejected}"
        );
    }

    let rows = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*) FROM kubernetes_pool_configs WHERE encryptable"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(rows, 0, "no encryptable row may be persisted");

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn upsert_pool_config_preserves_and_explicitly_resets_disk_capacity() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::kubernetes::store::MaterializationMode::Fenced,
    );
    let request = |disk_mib, reset_disk_to_template_default| ClusterPoolConfigRequest {
        template_name: "seed".to_string(),
        content_hash: Some(slot.content_hash.clone()),
        vcpu: 4,
        memory_mib: 2048,
        disk_mib,
        reset_disk_to_template_default,
        target_ready_slots: 1,
        refill_per_tick: 1,
        max_warming_slots: 1,
        namespace: None,
        enabled: true,
        encryptable: false,
    };

    assert_eq!(
        upsert_pool_config(&store, request(Some(32768), false))
            .await?
            .disk_mib,
        Some(32768)
    );
    for disk_mib in [0, 131073, u32::MAX] {
        let error = upsert_pool_config(&store, request(Some(disk_mib), false))
            .await
            .unwrap_err();
        assert_eq!(error.status(), axum::http::StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        upsert_pool_config(&store, request(None, false))
            .await?
            .disk_mib,
        Some(32768)
    );
    assert_eq!(
        upsert_pool_config(&store, request(None, true))
            .await?
            .disk_mib,
        None
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn disable_pool_config_preserves_autoscaler_ownership() -> TestResult {
    // arrange
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::kubernetes::store::MaterializationMode::Fenced,
    );
    let config_id: Uuid = sqlx::query_scalar(sql!(
        "UPDATE kubernetes_pool_configs
         SET managed_by = 'autoscaler'
         WHERE template_release_id = (
             SELECT id FROM template_releases WHERE hash = $1
         )
         RETURNING id"
    ))
    .bind(&slot.content_hash)
    .fetch_one(&db.pool)
    .await?;

    // act
    disable_pool_config(&store, config_id).await?;

    // assert
    let managed_by: String = sqlx::query_scalar(sql!(
        "SELECT managed_by FROM kubernetes_pool_configs WHERE id = $1"
    ))
    .bind(config_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(managed_by, "autoscaler");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn upsert_pool_config_takes_manual_ownership_from_autoscaler() -> TestResult {
    // arrange
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::kubernetes::store::MaterializationMode::Fenced,
    );
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs
         SET managed_by = 'autoscaler'
         WHERE template_release_id = (
             SELECT id FROM template_releases WHERE hash = $1
         )"
    ))
    .bind(&slot.content_hash)
    .execute(&db.pool)
    .await?;

    // act
    let config = upsert_pool_config(
        &store,
        ClusterPoolConfigRequest {
            template_name: "seed".to_string(),
            content_hash: Some(slot.content_hash),
            vcpu: 4,
            memory_mib: 2048,
            disk_mib: None,
            reset_disk_to_template_default: false,
            target_ready_slots: 0,
            refill_per_tick: 1,
            max_warming_slots: 1,
            namespace: None,
            enabled: false,
            encryptable: false,
        },
    )
    .await?;

    // assert
    assert_eq!(config.managed_by, PoolConfigOwner::Manual);
    assert!(!config.enabled);
    assert_eq!(config.target_ready_slots, 0);
    Ok(())
}

/// Put the fixture's slot in the state a create admitted against it leaves:
/// `claimed` under a fresh claim ID, its sandbox not yet created.
async fn admit_fixture_slot(
    pool: &PgPool,
    slot: &ReadyPoolSlotFixture,
) -> TestResult<AdmittedPoolClaim> {
    let claim_id = Uuid::now_v7();
    let pool_slot_id: Uuid = sqlx::query_scalar(sql!(
        "UPDATE kubernetes_pool_slots
         SET state = 'claimed', claim_id = $2, claimed_at = NOW()
         WHERE reserved_sandbox_id = $1
         RETURNING id"
    ))
    .bind(slot.sandbox_id)
    .bind(claim_id)
    .fetch_one(pool)
    .await?;
    Ok(AdmittedPoolClaim {
        claim_id,
        pool_slot_id,
    })
}

async fn fixture_slot_state(
    pool: &PgPool,
    slot: &ReadyPoolSlotFixture,
) -> TestResult<Option<(String, Option<Uuid>)>> {
    Ok(sqlx::query_as(sql!(
        "SELECT state, claim_id FROM kubernetes_pool_slots WHERE reserved_sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .fetch_optional(pool)
    .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_claim_adopts_and_binds_the_exact_slot() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let admitted = admit_fixture_slot(&db.pool, &slot).await?;
    let (actor, env, auto_lifecycle, template_network_policy) = claim_test_context(&slot)?;
    let request = claim_request_with_shape(
        &actor,
        &env,
        &auto_lifecycle,
        &template_network_policy,
        slot.release_id,
        &slot.content_hash,
        Some(PooledExecutionPoolShape::new(4, 2048, Default::default())),
    );

    let lookup =
        lookup_admitted_pooled_execution(&db.pool, &request, admitted, slot.sandbox_id, "space")
            .await?;
    let AdmittedPoolClaimLookup::Adoptable { claim, pool_shape } = lookup else {
        panic!("a matching admitted claim must be adoptable");
    };
    assert_eq!(claim.claim_id, admitted.claim_id);
    assert_eq!(claim.pool_slot_id, Some(admitted.pool_slot_id));
    assert_eq!(claim.sandbox_id, slot.sandbox_id);
    assert_eq!(claim.execution_generation, 1);
    assert_eq!(claim.pod_name, "space-sbx-pooled-g1");
    assert_eq!(claim.active_node_id, Some(slot.node_id));
    assert_eq!(
        pool_shape,
        PooledExecutionPoolShape::new(4, 2048, Default::default())
    );

    let mut tx = db.pool.begin().await?;
    insert_claimed_sandbox(&mut tx, &claim, slot.user_id, slot.org_id).await?;
    persist_pooled_execution_claim(&mut tx, &request, &claim).await?;
    tx.commit().await?;

    assert_eq!(
        fixture_slot_state(&db.pool, &slot).await?,
        Some(("bound".to_string(), Some(admitted.claim_id)))
    );
    let executions: Vec<(i64, String, bool)> = sqlx::query_as(sql!(
        "SELECT generation, pod_name, active FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        executions,
        vec![(1, "space-sbx-pooled-g1".to_string(), true)]
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_claim_mismatch_waits_and_leaves_other_claims_alone() -> TestResult {
    // A slot claimed under a different claim ID still owns its pod under this
    // ID; the create waits for the pool to collect it, without touching it.
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let held = admit_fixture_slot(&db.pool, &slot).await?;
    let (actor, env, auto_lifecycle, template_network_policy) = claim_test_context(&slot)?;
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);
    let stale = AdmittedPoolClaim {
        claim_id: Uuid::now_v7(),
        pool_slot_id: held.pool_slot_id,
    };

    let lookup =
        lookup_admitted_pooled_execution(&db.pool, &request, stale, slot.sandbox_id, "space")
            .await?;

    assert!(matches!(lookup, AdmittedPoolClaimLookup::ReleasePending));
    assert_eq!(
        fixture_slot_state(&db.pool, &slot).await?,
        Some(("claimed".to_string(), Some(held.claim_id)))
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_claim_for_a_different_slot_is_invalidated() -> TestResult {
    // Our claim ID, but not the slot the operation recorded: never adopt it,
    // and take it out of the inventory so the pool collects it.
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let admitted = admit_fixture_slot(&db.pool, &slot).await?;
    let (actor, env, auto_lifecycle, template_network_policy) = claim_test_context(&slot)?;
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);
    let other_slot = AdmittedPoolClaim {
        claim_id: admitted.claim_id,
        pool_slot_id: Uuid::now_v7(),
    };

    let lookup =
        lookup_admitted_pooled_execution(&db.pool, &request, other_slot, slot.sandbox_id, "space")
            .await?;

    assert!(matches!(lookup, AdmittedPoolClaimLookup::ReleasePending));
    assert_eq!(
        fixture_slot_state(&db.pool, &slot).await?,
        Some(("claimed".to_string(), None))
    );
    let reclaimed = claim_pooled_execution(&db.pool, &request, "space").await?;
    assert!(
        reclaimed.claim.is_none(),
        "an invalidated slot is never handed out"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_claim_fences_a_slot_returned_to_ready() -> TestResult {
    // If the slot went back to `ready`, another create could bind this ID.
    // Fence it out of the inventory instead of adopting or leaving it.
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let (actor, env, auto_lifecycle, template_network_policy) = claim_test_context(&slot)?;
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);
    let admitted = AdmittedPoolClaim {
        claim_id: Uuid::now_v7(),
        pool_slot_id: Uuid::now_v7(),
    };

    let lookup =
        lookup_admitted_pooled_execution(&db.pool, &request, admitted, slot.sandbox_id, "space")
            .await?;

    assert!(matches!(lookup, AdmittedPoolClaimLookup::ReleasePending));
    assert_eq!(
        fixture_slot_state(&db.pool, &slot).await?,
        Some(("claimed".to_string(), None))
    );
    let reclaimed = claim_pooled_execution(&db.pool, &request, "space").await?;
    assert!(reclaimed.claim.is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_claim_missing_or_bound_slot_is_released() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let admitted = admit_fixture_slot(&db.pool, &slot).await?;
    let (actor, env, auto_lifecycle, template_network_policy) = claim_test_context(&slot)?;
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);

    // Bound: the slot's pod is a sandbox execution now, and its row only
    // disappears with that sandbox, so it no longer holds a pool pod here.
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_slots SET state = 'bound'
         WHERE reserved_sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .execute(&db.pool)
    .await?;
    let lookup =
        lookup_admitted_pooled_execution(&db.pool, &request, admitted, slot.sandbox_id, "space")
            .await?;
    assert!(matches!(lookup, AdmittedPoolClaimLookup::Released));

    // Missing: the stale-unbound sweep collected the slot and its pod.
    sqlx::query(sql!(
        "DELETE FROM kubernetes_pool_slots WHERE reserved_sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .execute(&db.pool)
    .await?;
    let lookup =
        lookup_admitted_pooled_execution(&db.pool, &request, admitted, slot.sandbox_id, "space")
            .await?;
    assert!(matches!(lookup, AdmittedPoolClaimLookup::Released));
    assert!(!admitted_pool_cleanup_pending(&db.pool, slot.sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_admitted_pool_cleanup_pending_reports_failed_adoptions() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    assert!(!admitted_pool_cleanup_pending(&db.pool, slot.sandbox_id).await?);
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_slots
         SET state = 'cleanup_pending', claim_id = $2, claimed_at = NOW()
         WHERE reserved_sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await?;
    assert!(admitted_pool_cleanup_pending(&db.pool, slot.sandbox_id).await?);
    Ok(())
}
