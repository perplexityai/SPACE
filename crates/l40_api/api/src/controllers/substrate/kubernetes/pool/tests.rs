use pplx_space_common_core::types::{DiskIoLimits, NodeId, SandboxId};

use super::*;
use std::collections::BTreeMap;
use std::time::Duration;

use crate::controllers::substrate::{
    ActorRef, AdoptionLabelOutcome, AdoptionLabelTransient, SubstrateAdapter,
};
use axum::http::StatusCode;
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
    let limits = DiskIoLimits::default();
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
             refill_per_tick, max_warming_slots, namespace, enabled,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES ($1, $2, 4, 2048, 1, 1, 1, 'space', true, $3, $4)"
    ))
    .bind(config_id)
    .bind(latest_release_id)
    .bind(i64::try_from(limits.max_operations_per_sec())?)
    .bind(i64::try_from(limits.max_bandwidth_mib_per_sec())?)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, ready_at,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES (
             $1, $2, $3, 'seed', $4, 4, 2048, 'space-sbx-pooled-g1',
             'space', 'pod-uid', '10.90.0.11'::inet, 'pool-node-1', 'ready', NOW(), $5, $6
         )"
    ))
    .bind(config_id)
    .bind(latest_release_id)
    .bind(sandbox_id)
    .bind(&content_hash)
    .bind(i64::try_from(limits.max_operations_per_sec())?)
    .bind(i64::try_from(limits.max_bandwidth_mib_per_sec())?)
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
    let limits = DiskIoLimits::default();
    let config_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_configs (
             id, template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES ($1, $2, $3, $4, $5, 1, $5, 'space', $6, $7, $8)"
    ))
    .bind(config_id)
    .bind(release_id)
    .bind(vcpu)
    .bind(memory_mib)
    .bind(target_ready_slots)
    .bind(enabled)
    .bind(i64::try_from(limits.max_operations_per_sec())?)
    .bind(i64::try_from(limits.max_bandwidth_mib_per_sec())?)
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
    let limits = DiskIoLimits::default();
    let (pod_name, pod_ip) = pod;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, ready_at,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES (
             $1, $2, $3, 'seed', $4, $5, $6, $7,
             'space', 'pod-uid', $8::inet, 'pool-node-1', 'ready', NOW(), $9, $10
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
    .bind(i64::try_from(limits.max_operations_per_sec())?)
    .bind(i64::try_from(limits.max_bandwidth_mib_per_sec())?)
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
        client_id: None,
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
    let materialized: bool = sqlx::query_scalar(sql!(
        "SELECT pod_materialized_at IS NOT NULL FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert!(
        materialized,
        "an execution born with its slot's pod records that it materialized one"
    );

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
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
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
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
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
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
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
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
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
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
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

/// Claim `slot` for a create with no request env, network policy or
/// lifecycle policy.
async fn claim_ready_pool_slot(
    pool: &PgPool,
    slot: &ReadyPoolSlotFixture,
) -> TestResult<PooledExecutionClaim> {
    let actor = ActorRef::new(slot.user_id, slot.org_id, Email::parse("pool@example.com")?);
    let env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let template_network_policy = NetworkPolicyV1::deny_all();
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);
    Ok(claim_pooled_execution(pool, &request, "space")
        .await?
        .claim
        .expect("ready pool slot should be claimed"))
}

/// Where a claimed slot ends up.
#[derive(Clone, Copy, Debug)]
enum SlotAfter {
    /// Still claimed under the claim.
    Claimed,
    /// Back in the `ready` inventory.
    Ready,
    /// Claimed with its claim id stripped, for the stale-claim sweep.
    Invalidated,
}

async fn assert_slot_after(
    pool: &PgPool,
    slot: &ReadyPoolSlotFixture,
    claim: &PooledExecutionClaim,
    expected: SlotAfter,
) -> TestResult {
    let expected = match expected {
        SlotAfter::Claimed => ("claimed", Some(claim.claim_id)),
        SlotAfter::Ready => ("ready", None),
        SlotAfter::Invalidated => ("claimed", None),
    };
    assert_eq!(
        fixture_slot_state(pool, slot).await?,
        Some((expected.0.to_string(), expected.1))
    );
    Ok(())
}

/// A scripted pod reply: a pod for a success, a `Status` otherwise.
fn pod_patch_reply(status: StatusCode) -> super::super::scripted_test_doubles::KubeReply {
    use super::super::scripted_test_doubles::{KubeReply, kube_status};
    if status.is_success() {
        KubeReply::Json(
            status,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": "pooled", "namespace": "space" }
            }),
        )
    } else {
        kube_status(status, "scripted")
    }
}

/// The client-id label value a recorded patch sets: `Some(Some(value))` to
/// set it, `Some(None)` to remove it, `None` when it doesn't touch it.
fn patched_client_label(
    request: &super::super::scripted_test_doubles::KubeRequest,
) -> Option<Option<String>> {
    use pplx_space_common_core::kubernetes::SANDBOX_CLIENT_ID_LABEL;
    let value = request
        .body
        .as_ref()?
        .pointer("/metadata/labels")?
        .get(SANDBOX_CLIENT_ID_LABEL)?;
    Some(value.as_str().map(str::to_string))
}

/// The patches a log holds that set the client-id label, and those that
/// remove it, each checked to be fenced by the claimed pod UID.
fn label_and_clear_patches(
    log: &super::super::scripted_test_doubles::KubeLog,
    claim: &PooledExecutionClaim,
) -> (Vec<String>, usize) {
    use super::super::scripted_test_doubles::{pod_path, requests_matching};
    let pod_uid = claim
        .pod_uid
        .as_ref()
        .expect("the fixture slot records its pod UID")
        .as_str();
    let mut labels = Vec::new();
    let mut clears = 0;
    for patch in requests_matching(log, &axum::http::Method::PATCH) {
        assert_eq!(patch.path, pod_path(&claim.pod_name));
        assert_eq!(
            patch
                .body
                .as_ref()
                .and_then(|body| body.pointer("/metadata/uid"))
                .and_then(serde_json::Value::as_str),
            Some(pod_uid),
            "every label write is fenced by the claimed pod UID: {patch:?}"
        );
        match patched_client_label(&patch) {
            Some(Some(value)) => labels.push(value),
            Some(None) => clears += 1,
            None => panic!("a pod patch that doesn't touch the client-id label: {patch:?}"),
        }
    }
    (labels, clears)
}

/// The adopter's label is patched onto the claimed pool pod before the
/// adoption commits, fenced by the claimed pod UID. A permanent refusal (a
/// replaced or missing pod, a missing RBAC verb) invalidates the slot, never
/// returning it to the pool, where a refusal that repeats would hand the same
/// slot to every later create. Throttling is retried first. A transient
/// failure never adopts the pod; an admitted create keeps its claim. Otherwise
/// throttling through every retry gives the slot back unlabeled, since a
/// throttled write never ran, while a 5xx, whose write may still land,
/// invalidates the slot and removes the label best-effort. An answered
/// refusal didn't land, so it sends no removal.
#[rstest::rstest]
#[case::labeled(
    StatusCode::OK,
    StatusCode::OK,
    AdoptionLabelTransient::ReleaseSlot,
    AdoptionLabelOutcome::Labeled,
    1,
    0,
    SlotAfter::Claimed
)]
#[case::pod_replaced_under_the_same_name(
    StatusCode::UNPROCESSABLE_ENTITY,
    StatusCode::OK,
    AdoptionLabelTransient::ReleaseSlot,
    AdoptionLabelOutcome::Invalidated,
    1,
    0,
    SlotAfter::Invalidated
)]
#[case::forbidden(
    StatusCode::FORBIDDEN,
    StatusCode::OK,
    AdoptionLabelTransient::KeepClaim,
    AdoptionLabelOutcome::Invalidated,
    1,
    0,
    SlotAfter::Invalidated
)]
#[case::server_error_invalidates_the_slot_and_clears_the_label(
    StatusCode::INTERNAL_SERVER_ERROR,
    StatusCode::OK,
    AdoptionLabelTransient::ReleaseSlot,
    AdoptionLabelOutcome::Transient,
    1,
    1,
    SlotAfter::Invalidated
)]
#[case::server_error_invalidates_the_slot_whose_label_cant_be_cleared(
    StatusCode::INTERNAL_SERVER_ERROR,
    StatusCode::INTERNAL_SERVER_ERROR,
    AdoptionLabelTransient::ReleaseSlot,
    AdoptionLabelOutcome::Transient,
    1,
    1,
    SlotAfter::Invalidated
)]
#[case::unavailable_keeps_the_claim(
    StatusCode::SERVICE_UNAVAILABLE,
    StatusCode::OK,
    AdoptionLabelTransient::KeepClaim,
    AdoptionLabelOutcome::Transient,
    1,
    0,
    SlotAfter::Claimed
)]
#[case::pod_gone(
    StatusCode::NOT_FOUND,
    StatusCode::OK,
    AdoptionLabelTransient::ReleaseSlot,
    AdoptionLabelOutcome::Invalidated,
    1,
    0,
    SlotAfter::Invalidated
)]
#[case::throttled_throughout_releases_the_slot_without_a_clear(
    StatusCode::TOO_MANY_REQUESTS,
    StatusCode::TOO_MANY_REQUESTS,
    AdoptionLabelTransient::ReleaseSlot,
    AdoptionLabelOutcome::Transient,
    // `K8S_THROTTLE_RETRY_ATTEMPTS`: every attempt is spent before giving up.
    3,
    0,
    SlotAfter::Ready
)]
#[case::throttled_throughout_keeps_the_claim(
    StatusCode::TOO_MANY_REQUESTS,
    StatusCode::OK,
    AdoptionLabelTransient::KeepClaim,
    AdoptionLabelOutcome::Transient,
    3,
    0,
    SlotAfter::Claimed
)]
#[tokio::test(flavor = "current_thread")]
async fn postgres_label_adopted_execution_patches_the_label_or_gives_up_the_slot(
    #[case] label_status: StatusCode,
    #[case] clear_status: StatusCode,
    #[case] on_transient: AdoptionLabelTransient,
    #[case] expected: AdoptionLabelOutcome,
    #[case] expected_label_patches: usize,
    #[case] expected_clear_patches: usize,
    #[case] slot_after: SlotAfter,
) -> TestResult {
    use super::super::scripted_test_doubles::scripted_kube_client;
    use pplx_space_common_core::types::ClientId;

    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let claim = claim_ready_pool_slot(&db.pool, &slot).await?;
    let (client, log) = scripted_kube_client(move |request| match patched_client_label(request) {
        Some(None) => pod_patch_reply(clear_status),
        _ => pod_patch_reply(label_status),
    });
    let substrate = super::super::tests::substrate_with_store_and_client(db.pool.clone(), client);
    let adopter = ClientId::parse("adopter").expect("test client id");

    let outcome = substrate
        .label_adopted_execution(&claim, Some(&adopter), on_transient)
        .await?;

    assert_eq!(outcome, expected);
    let (labels, clears) = label_and_clear_patches(&log, &claim);
    assert_eq!(labels, vec!["adopter".to_string(); expected_label_patches]);
    assert_eq!(clears, expected_clear_patches);
    assert_slot_after(&db.pool, &slot, &claim, slot_after).await
}

/// A label write throttled and then answered with a 5xx may have run on the
/// later attempt, so the slot is invalidated, not released as after
/// throttling alone, and the label removed best-effort.
#[tokio::test(flavor = "current_thread")]
async fn postgres_label_adopted_execution_throttled_then_failed_invalidates_the_slot() -> TestResult
{
    use super::super::scripted_test_doubles::scripted_kube_client;
    use pplx_space_common_core::types::ClientId;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let claim = claim_ready_pool_slot(&db.pool, &slot).await?;
    let label_attempts = AtomicUsize::new(0);
    let (client, log) = scripted_kube_client(move |request| match patched_client_label(request) {
        Some(None) => pod_patch_reply(StatusCode::OK),
        _ if label_attempts.fetch_add(1, Ordering::SeqCst) == 0 => {
            pod_patch_reply(StatusCode::TOO_MANY_REQUESTS)
        }
        _ => pod_patch_reply(StatusCode::INTERNAL_SERVER_ERROR),
    });
    let substrate = super::super::tests::substrate_with_store_and_client(db.pool.clone(), client);
    let adopter = ClientId::parse("adopter").expect("test client id");

    let outcome = substrate
        .label_adopted_execution(&claim, Some(&adopter), AdoptionLabelTransient::ReleaseSlot)
        .await?;

    assert_eq!(outcome, AdoptionLabelOutcome::Transient);
    let (labels, clears) = label_and_clear_patches(&log, &claim);
    assert_eq!(labels, vec!["adopter".to_string(); 2]);
    assert_eq!(clears, 1);
    assert_slot_after(&db.pool, &slot, &claim, SlotAfter::Invalidated).await
}

/// A label write the apiserver never answers ends at the adoption label
/// budget and counts as transient, so the create doesn't hang with its slot
/// claimed. The abandoned write could still land after anything later, so
/// the slot is never released: it's invalidated, and the label's removal is
/// best-effort.
#[rstest::rstest]
#[case::clear_answered(false)]
#[case::clear_unanswered(true)]
#[tokio::test(flavor = "current_thread")]
async fn postgres_label_adopted_execution_bounds_a_write_the_apiserver_never_answers(
    #[case] clear_hangs: bool,
) -> TestResult {
    use super::super::scripted_test_doubles::{KubeReply, scripted_kube_client};
    use pplx_space_common_core::types::ClientId;

    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let claim = claim_ready_pool_slot(&db.pool, &slot).await?;
    let (client, log) = scripted_kube_client(move |request| match patched_client_label(request) {
        Some(None) if !clear_hangs => pod_patch_reply(StatusCode::OK),
        _ => KubeReply::Hang,
    });
    let mut substrate =
        super::super::tests::substrate_with_store_and_client(db.pool.clone(), client);
    substrate.budgets.adoption_label = Duration::from_millis(200);
    let adopter = ClientId::parse("adopter").expect("test client id");

    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        substrate.label_adopted_execution(
            &claim,
            Some(&adopter),
            AdoptionLabelTransient::ReleaseSlot,
        ),
    )
    .await
    .expect("the label write is bounded by the adoption label budget")?;

    assert_eq!(outcome, AdoptionLabelOutcome::Transient);
    let (labels, clears) = label_and_clear_patches(&log, &claim);
    assert_eq!(labels, vec!["adopter".to_string()]);
    assert_eq!(clears, 1);
    assert_slot_after(&db.pool, &slot, &claim, SlotAfter::Invalidated).await
}

/// Rolling back a claim, as a create does when its commit fails after the
/// pod was labeled, takes the slot out of the inventory and removes the
/// label, so it doesn't outlive the adoption it recorded. A slot the rollback
/// doesn't take out, because it's already bound, keeps its pod's label, and
/// with attribution off nothing is sent.
#[rstest::rstest]
#[case::claimed_slot_loses_its_label(true, false, 1, SlotAfter::Invalidated)]
#[case::bound_slot_keeps_its_label(true, true, 0, SlotAfter::Claimed)]
#[case::attribution_off_sends_nothing(false, false, 0, SlotAfter::Invalidated)]
#[tokio::test(flavor = "current_thread")]
async fn postgres_rollback_pooled_execution_claim_removes_the_adoption_label(
    #[case] attribution_on: bool,
    #[case] bound: bool,
    #[case] expected_clears: usize,
    #[case] slot_after: SlotAfter,
) -> TestResult {
    use super::super::scripted_test_doubles::scripted_kube_client;
    use crate::controllers::substrate::ActivationClientAttribution;

    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let claim = claim_ready_pool_slot(&db.pool, &slot).await?;
    if bound {
        sqlx::query(sql!(
            "UPDATE kubernetes_pool_slots SET state = 'bound'
             WHERE reserved_sandbox_id = $1"
        ))
        .bind(slot.sandbox_id)
        .execute(&db.pool)
        .await?;
    }
    let (client, log) = scripted_kube_client(|_| pod_patch_reply(StatusCode::OK));
    let mut substrate =
        super::super::tests::substrate_with_store_and_client(db.pool.clone(), client);
    if !attribution_on {
        substrate.k8s.activation_client_attribution = ActivationClientAttribution::Disabled;
    }

    substrate.rollback_pooled_execution_claim(&claim).await?;

    let (labels, clears) = label_and_clear_patches(&log, &claim);
    assert!(labels.is_empty(), "{labels:?}");
    assert_eq!(clears, expected_clears);
    if bound {
        assert_eq!(
            fixture_slot_state(&db.pool, &slot).await?,
            Some(("bound".to_string(), Some(claim.claim_id)))
        );
        return Ok(());
    }
    assert_slot_after(&db.pool, &slot, &claim, slot_after).await
}

/// A claim with no recorded pod UID can't fence the patch to the claimed pod,
/// so it's invalidated without asking the apiserver.
#[tokio::test(flavor = "current_thread")]
async fn postgres_label_adopted_execution_invalidates_a_claim_without_a_pod_uid() -> TestResult {
    use super::super::scripted_test_doubles::{kube_status, scripted_kube_client};
    use pplx_space_common_core::types::ClientId;

    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_slots SET pod_uid = NULL WHERE reserved_sandbox_id = $1"
    ))
    .bind(slot.sandbox_id)
    .execute(&db.pool)
    .await?;
    let claim = claim_ready_pool_slot(&db.pool, &slot).await?;
    assert_eq!(claim.pod_uid, None);
    let (client, log) = scripted_kube_client(|_| kube_status(StatusCode::OK, "unexpected"));
    let substrate = super::super::tests::substrate_with_store_and_client(db.pool.clone(), client);
    let adopter = ClientId::parse("adopter").expect("test client id");

    let outcome = substrate
        .label_adopted_execution(&claim, Some(&adopter), AdoptionLabelTransient::ReleaseSlot)
        .await?;

    assert_eq!(outcome, AdoptionLabelOutcome::Invalidated);
    assert!(log.lock().expect("kube log mutex poisoned").is_empty());
    assert_eq!(
        fixture_slot_state(&db.pool, &slot).await?,
        Some(("claimed".to_string(), None))
    );
    Ok(())
}

/// An adopter with no id to stamp leaves the pod unlabeled without asking the
/// apiserver.
#[tokio::test(flavor = "current_thread")]
async fn label_adopted_execution_without_a_client_sends_nothing() -> TestResult {
    use super::super::scripted_test_doubles::{kube_status, scripted_kube_client};

    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let claim = claim_ready_pool_slot(&db.pool, &slot).await?;
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::INTERNAL_SERVER_ERROR, "unexpected"));
    let substrate = super::super::tests::substrate_with_store_and_client(db.pool.clone(), client);

    let outcome = substrate
        .label_adopted_execution(&claim, None, AdoptionLabelTransient::ReleaseSlot)
        .await?;

    assert_eq!(outcome, AdoptionLabelOutcome::Labeled);
    assert!(log.lock().expect("kube log mutex poisoned").is_empty());
    Ok(())
}

/// With attribution off, adoption patches nothing, even for a client with an
/// id, and the claim stays as it was.
#[tokio::test(flavor = "current_thread")]
async fn postgres_label_adopted_execution_with_attribution_off_sends_nothing() -> TestResult {
    use super::super::scripted_test_doubles::{kube_status, scripted_kube_client};
    use crate::controllers::substrate::ActivationClientAttribution;
    use pplx_space_common_core::types::ClientId;

    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let claim = claim_ready_pool_slot(&db.pool, &slot).await?;
    let (client, log) =
        scripted_kube_client(|_| kube_status(StatusCode::INTERNAL_SERVER_ERROR, "unexpected"));
    let mut substrate =
        super::super::tests::substrate_with_store_and_client(db.pool.clone(), client);
    substrate.k8s.activation_client_attribution = ActivationClientAttribution::Disabled;
    let adopter = ClientId::parse("adopter").expect("test client id");

    let outcome = substrate
        .label_adopted_execution(&claim, Some(&adopter), AdoptionLabelTransient::ReleaseSlot)
        .await?;

    assert_eq!(outcome, AdoptionLabelOutcome::Labeled);
    assert!(log.lock().expect("kube log mutex poisoned").is_empty());
    assert_eq!(
        fixture_slot_state(&db.pool, &slot).await?,
        Some(("claimed".to_string(), Some(claim.claim_id)))
    );
    Ok(())
}

/// Characterization of the release choice of the Kubernetes pool config upsert
/// (`resolve_pool_template_release`): the release whose stored hash equals the
/// requested string when a hash is given; otherwise the `latest` holder, else
/// the newest by `created_at`. It takes no runtime and no caller org, and never
/// reads `templates`. The create path's pool claim passes the release sandbox
/// create resolved and skips it.
#[tokio::test(flavor = "current_thread")]
async fn pool_template_release_resolution_characterization() -> TestResult {
    use crate::controllers::test_fixtures::{
        RESOLVER_DELETING_TEMPLATE, RESOLVER_PRIVATE_TEMPLATE, RESOLVER_TEMPLATE,
        RESOLVER_UNTAGGED_TEMPLATE, hex, seed_resolver_world,
    };
    let db = pg_test_db().await?;
    let w = seed_resolver_world(&db.pool).await?;
    let sha = |c| format!("sha256:{}", hex(c));
    let blake = |c| format!("blake3:{}", hex(c));

    let cases = [
        // The `latest` holder, not the most recently tagged `bare`.
        (RESOLVER_TEMPLATE, None, Some((w.latest_holder, sha('d')))),
        (RESOLVER_TEMPLATE, Some(hex('a')), Some((w.bare, hex('a')))),
        // Exact string match only; sandbox create, ClusterApi by-hash and
        // observed-demand matching find the bare row here.
        (RESOLVER_TEMPLATE, Some(sha('a')), None),
        (
            RESOLVER_TEMPLATE,
            Some(sha('b')),
            Some((w.prefixed, sha('b'))),
        ),
        // ClusterApi by-hash and observed-demand matching find the prefixed row
        // for this bare request.
        (RESOLVER_TEMPLATE, Some(hex('b')), None),
        (
            RESOLVER_TEMPLATE,
            Some(blake('c')),
            Some((w.blake3, blake('c'))),
        ),
        // A missed hash does not fall back to `latest`.
        (RESOLVER_TEMPLATE, Some(sha('c')), None),
        (
            RESOLVER_TEMPLATE,
            Some(hex('9')),
            Some((w.dual_bare, hex('9'))),
        ),
        (
            RESOLVER_TEMPLATE,
            Some(sha('9')),
            Some((w.dual_prefixed, sha('9'))),
        ),
        // No `latest`: the newest by created_at, which is neither first nor
        // last by insertion, id or hash. Sandbox create, build base and
        // ClusterApi FindTemplateRelease answer not found.
        (
            RESOLVER_UNTAGGED_TEMPLATE,
            None,
            Some((w.untagged_newest, sha('2'))),
        ),
        (
            RESOLVER_UNTAGGED_TEMPLATE,
            Some(sha('1')),
            Some((w.untagged_older, sha('1'))),
        ),
        // No org scope: an org-specific template resolves with no caller.
        (RESOLVER_PRIVATE_TEMPLATE, None, Some((w.private, sha('f')))),
        // `desired_state` is not read: a template being deleted still resolves.
        // Pinned as today's behaviour, not a contract: consolidating the
        // resolvers is expected to refuse a template that is not `present`.
        (
            RESOLVER_DELETING_TEMPLATE,
            None,
            Some((w.deleting, sha('7'))),
        ),
    ];
    for (template, hash, expected) in cases {
        let mut tx = db.pool.begin().await?;
        let resolved = resolve_pool_template_release(&mut tx, template, hash.as_deref()).await?;
        tx.rollback().await?;
        assert_eq!(
            resolved.map(|row| (row.id, row.content_hash)),
            expected,
            "{template} hash={hash:?}"
        );
    }
    Ok(())
}
