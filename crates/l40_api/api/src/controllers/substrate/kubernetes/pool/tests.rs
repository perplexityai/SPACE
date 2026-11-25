use pplx_space_accounts::NodeId;
use pplx_space_execution_spec::DiskIoLimits;
use pplx_space_sandbox_model::SandboxId;

use super::*;
use std::collections::BTreeMap;
use std::time::Duration;

use crate::controllers::substrate::{
    ActorRef, AdoptionLabelOutcome, AdoptionLabelTransient, SubstrateAdapter,
};
use crate::metrics::sandbox::api::SandboxApiMetricIdentity;
use axum::http::StatusCode;
use pplx_space_accounts::{Email, OrgId, UserId};
use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_cluster_pool::PoolConfigOwner;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_sql_locator::sql;
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

    let claim = claim_pooled_execution(&db.pool, &request, "space", false).await?;
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

/// A builder registered under the slot's Kubernetes node name is not the
/// slot's node: node ids are scoped by role, so the claim places the sandbox
/// on the worker row for that name, or on no node when only a builder has it.
#[tokio::test(flavor = "current_thread")]
async fn postgres_claim_resolves_the_worker_sharing_a_builder_node_name() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    let builder_id = NodeId::new();
    // Re-register the worker after the builder so a name-only read meets the
    // builder first.
    sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1"))
        .bind(slot.node_id)
        .execute(&db.pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.10'::inet, 'builder', 'k8s', 'registered')"
    ))
    .bind(builder_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'pool-node-1', 'node-uid'), ($2, 'pool-node-1', 'node-uid')"
    ))
    .bind(builder_id)
    .bind(slot.node_id)
    .execute(&db.pool)
    .await?;
    let actor = ActorRef::new(slot.user_id, slot.org_id, Email::parse("pool@example.com")?);
    let env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let template_network_policy = NetworkPolicyV1::deny_all();
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);

    let claim = claim_pooled_execution(&db.pool, &request, "space", false)
        .await?
        .claim
        .expect("ready pool slot should be claimed");
    assert_eq!(claim.active_node_id, Some(slot.node_id));

    sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1"))
        .bind(slot.node_id)
        .execute(&db.pool)
        .await?;
    let mut tx = db.pool.begin().await?;
    let builder_only = resolve_pool_slot_node_id(&mut tx, Some("pool-node-1")).await?;
    assert_eq!(builder_only, None);

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

    let stale = claim_pooled_execution(&db.pool, &request, "space", false).await?;
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

    let matched = claim_pooled_execution(&db.pool, &request, "space", false).await?;
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

    let attempt = claim_pooled_execution(&db.pool, &request, "space", false).await?;

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

    let attempt = claim_pooled_execution(&db.pool, &request, "space", false).await?;

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

    let hit = claim_pooled_execution(&db.pool, &request, "space", false).await?;
    assert_eq!(hit.pool_shape, Some(explicit_shape));
    assert_eq!(
        hit.claim
            .expect("explicit request should claim its exact shape")
            .sandbox_id,
        explicit_sandbox_id
    );

    let miss = claim_pooled_execution(&db.pool, &request, "space", false).await?;
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

    let claim = claim_pooled_execution(&db.pool, &request, "space", false)
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
    let claim = claim_pooled_execution(&db.pool, &request, "space", false)
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

    let reclaimed = claim_pooled_execution(&db.pool, &request, "space", false)
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
    let claim = claim_pooled_execution(&db.pool, &request, "space", false)
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

    let reclaimed = claim_pooled_execution(&db.pool, &request, "space", false).await?;
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
            template_release_id: None,
        },
        false,
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

/// A pinned hash resolves to its original release, never a twin bound to
/// another environment that was registered after it. Twins exist only once
/// `idx_template_releases_template_name_hash_unique` is dropped, which the
/// test does to stand in for the contract migration.
#[tokio::test(flavor = "current_thread")]
async fn upsert_pool_config_resolves_the_original_not_a_twin() -> TestResult {
    let db = pg_test_db().await?;
    insert_ready_pool_slot(&db.pool).await?;
    let original: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases WHERE hash = 'sha256:newer-untagged'"
    ))
    .fetch_one(&db.pool)
    .await?;
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_releases (template_name, type, hash, created_at, execution_metadata)
         VALUES ('seed', 'fc', 'sha256:newer-untagged', NOW() + INTERVAL '1 minute',
                 jsonb_build_object('environment', 'sha256:' || repeat('7', 64)))"
    ))
    .execute(&db.pool)
    .await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
    );

    let config = upsert_pool_config(
        &store,
        ClusterPoolConfigRequest {
            template_name: "seed".to_string(),
            content_hash: Some("sha256:newer-untagged".to_string()),
            vcpu: 4,
            memory_mib: 2048,
            disk_mib: None,
            reset_disk_to_template_default: false,
            target_ready_slots: 1,
            refill_per_tick: 1,
            max_warming_slots: 1,
            namespace: None,
            enabled: true,
            encryptable: false,
            template_release_id: None,
        },
        false,
    )
    .await?;
    assert_eq!(config.template_release_id, original);
    Ok(())
}

/// Creates may resolve to a release's twin (here, the one bound to the
/// cluster default), and
/// claim its slots, so an operator's upsert of the release's pool config
/// changes the twin's too. The original's config is the one reported.
#[tokio::test(flavor = "current_thread")]
async fn upsert_pool_config_also_updates_the_twin_bound_to_the_default() -> TestResult {
    let db = pg_test_db().await?;
    insert_ready_pool_slot(&db.pool).await?;
    let original: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases WHERE hash = 'sha256:newer-untagged'"
    ))
    .fetch_one(&db.pool)
    .await?;
    let e1 = format!("sha256:{}", "7".repeat(64));
    // Twins exist only once the `(template_name, hash)` unique index is
    // dropped; the test drops it to stand in for the contract migration.
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    let twin: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, created_at, execution_metadata)
         VALUES ('seed', 'fc', 'sha256:newer-untagged', NOW() + INTERVAL '1 minute',
                 jsonb_build_object('environment', $1::TEXT))
         RETURNING id"
    ))
    .bind(&e1)
    .fetch_one(&db.pool)
    .await?;
    crate::test_support::set_default_environment(&db.pool, &e1).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
    );
    let request = |target_ready_slots| ClusterPoolConfigRequest {
        template_name: "seed".to_string(),
        content_hash: Some("sha256:newer-untagged".to_string()),
        vcpu: 4,
        memory_mib: 2048,
        disk_mib: None,
        reset_disk_to_template_default: false,
        target_ready_slots,
        refill_per_tick: 1,
        max_warming_slots: 1,
        namespace: None,
        enabled: true,
        encryptable: false,
        template_release_id: None,
    };

    // Where creates don't boot the twin they resolve to, they run the
    // original's pools, and the twin's are left alone.
    upsert_pool_config(
        &store,
        ClusterPoolConfigRequest {
            disk_mib: Some(32768),
            ..request(2)
        },
        false,
    )
    .await?;
    let twin_configs: i64 = sqlx::query_scalar(sql!(
        "SELECT count(*) FROM kubernetes_pool_configs WHERE template_release_id = $1"
    ))
    .bind(twin)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(twin_configs, 0);
    // The original's disk I/O limits, as a policy the autoscaler applied
    // before an operator took the config over would have set them.
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs
         SET disk_io_max_operations_per_sec = 1234, disk_io_max_bandwidth_mib_per_sec = 56
         WHERE template_release_id = $1"
    ))
    .bind(original)
    .execute(&db.pool)
    .await?;

    for target in [1u32, 3] {
        let config = upsert_pool_config(&store, request(target), true).await?;
        assert_eq!(config.template_release_id, original);
        let targets: Vec<(Uuid, i32)> = sqlx::query_as(sql!(
            "SELECT template_release_id, target_ready_slots
             FROM kubernetes_pool_configs
             WHERE vcpu = 4 AND memory_mib = 2048
               AND template_release_id = ANY($1)
             ORDER BY created_at, id"
        ))
        .bind(vec![original, twin])
        .fetch_all(&db.pool)
        .await?;
        let target = i32::try_from(target)?;
        assert_eq!(targets, vec![(original, target), (twin, target)]);
    }
    // The requests named no disk size, which keeps the original's override;
    // the twin's config, made by the first of them, takes it too.
    let disks: Vec<(Uuid, Option<i32>)> = sqlx::query_as(sql!(
        "SELECT template_release_id, disk_mib
         FROM kubernetes_pool_configs
         WHERE template_release_id = ANY($1)
         ORDER BY created_at, id"
    ))
    .bind(vec![original, twin])
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(disks, vec![(original, Some(32768)), (twin, Some(32768))]);
    // Its disk I/O limits too, rather than the platform defaults.
    let limits: Vec<(Uuid, Option<i64>, Option<i64>)> = sqlx::query_as(sql!(
        "SELECT template_release_id, disk_io_max_operations_per_sec,
                disk_io_max_bandwidth_mib_per_sec
         FROM kubernetes_pool_configs
         WHERE template_release_id = ANY($1)
         ORDER BY created_at, id"
    ))
    .bind(vec![original, twin])
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(
        limits,
        vec![
            (original, Some(1234), Some(56)),
            (twin, Some(1234), Some(56))
        ]
    );

    // Disabling the original's config disables the twin's for the shape too.
    let original_config: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM kubernetes_pool_configs WHERE template_release_id = $1"
    ))
    .bind(original)
    .fetch_one(&db.pool)
    .await?;
    let enabled = || async {
        sqlx::query_as::<_, (Uuid, bool)>(sql!(
            "SELECT template_release_id, enabled
             FROM kubernetes_pool_configs
             WHERE template_release_id = ANY($1)
             ORDER BY created_at, id"
        ))
        .bind(vec![original, twin])
        .fetch_all(&db.pool)
        .await
    };
    // A superseded twin, in an environment with no state, whose config an
    // operator disables: the twin creates use keeps its pool.
    let superseded: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, created_at, execution_metadata)
         VALUES ('seed', 'fc', 'sha256:newer-untagged', NOW() + INTERVAL '2 minutes',
                 jsonb_build_object('environment', $1::TEXT))
         RETURNING id"
    ))
    .bind(format!("sha256:{}", "8".repeat(64)))
    .fetch_one(&db.pool)
    .await?;
    let superseded_config: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO kubernetes_pool_configs (
             template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled, managed_by
         )
         VALUES ($1, 4, 2048, 1, 1, 1, 'sandboxes', TRUE, 'manual')
         RETURNING id"
    ))
    .bind(superseded)
    .fetch_one(&db.pool)
    .await?;
    disable_pool_config(&store, superseded_config).await?;
    let superseded_state: (bool, i32) = sqlx::query_as(sql!(
        "SELECT enabled, target_ready_slots FROM kubernetes_pool_configs WHERE id = $1"
    ))
    .bind(superseded_config)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        superseded_state,
        (false, 0),
        "the superseded twin's config is disabled"
    );
    assert_eq!(enabled().await?, vec![(original, true), (twin, true)]);

    let disabled = disable_pool_config(&store, original_config).await?;
    assert_eq!(disabled.id, original_config);
    assert_eq!(enabled().await?, vec![(original, false), (twin, false)]);
    Ok(())
}

/// An edit of an existing pool names its release, and changes exactly that
/// release's config. Here the admin UI detaches and disables twin B's
/// autoscaler-owned config after the cluster default moved from B's
/// environment to twin C's: B's config is disabled, and the original A and
/// C, which creates resolve to now, keep theirs. Naming A, or no release,
/// is a change to A, which goes to C.
#[tokio::test(flavor = "current_thread")]
async fn a_named_twins_detach_after_the_default_moves_changes_only_that_twin() -> TestResult {
    let db = pg_test_db().await?;
    insert_ready_pool_slot(&db.pool).await?;
    let original: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases WHERE hash = 'sha256:newer-untagged'"
    ))
    .fetch_one(&db.pool)
    .await?;
    // Twins exist only once the `(template_name, hash)` unique index is
    // dropped; the test drops it to stand in for the contract migration.
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    let insert_twin = |environment: String, minutes: i32| {
        let pool = db.pool.clone();
        async move {
            sqlx::query_scalar::<_, Uuid>(sql!(
                "INSERT INTO template_releases (template_name, type, hash, created_at, execution_metadata)
                 VALUES ('seed', 'fc', 'sha256:newer-untagged',
                         NOW() + make_interval(mins => $2),
                         jsonb_build_object('environment', $1::TEXT))
                 RETURNING id"
            ))
            .bind(environment)
            .bind(minutes)
            .fetch_one(&pool)
            .await
        }
    };
    let e1 = format!("sha256:{}", "7".repeat(64));
    let e2 = format!("sha256:{}", "8".repeat(64));
    let twin_b = insert_twin(e1.clone(), 1).await?;
    let twin_c = insert_twin(e2.clone(), 2).await?;
    crate::test_support::set_default_environment(&db.pool, &e1).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
    );
    let request = |target_ready_slots, enabled, template_release_id| ClusterPoolConfigRequest {
        template_name: "seed".to_string(),
        content_hash: Some("sha256:newer-untagged".to_string()),
        vcpu: 4,
        memory_mib: 2048,
        disk_mib: None,
        reset_disk_to_template_default: false,
        target_ready_slots,
        refill_per_tick: 1,
        max_warming_slots: 1,
        namespace: None,
        enabled,
        encryptable: false,
        template_release_id,
    };
    let states = || async {
        sqlx::query_as::<_, (Uuid, bool, i32, String)>(sql!(
            "SELECT template_release_id, enabled, target_ready_slots, managed_by::TEXT
             FROM kubernetes_pool_configs
             WHERE vcpu = 4 AND memory_mib = 2048
               AND template_release_id = ANY($1)
             ORDER BY created_at, id"
        ))
        .bind(vec![original, twin_b, twin_c])
        .fetch_all(&db.pool)
        .await
    };
    let state =
        |release, enabled, target: i32, owner: &str| (release, enabled, target, owner.to_string());

    // A's pool, with the default in B's environment, reaches B's config.
    upsert_pool_config(&store, request(2, true, None), true).await?;
    // The autoscaler owns B's config, as one it took over would be.
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs SET managed_by = 'autoscaler'
         WHERE template_release_id = $1"
    ))
    .bind(twin_b)
    .execute(&db.pool)
    .await?;
    assert_eq!(
        states().await?,
        vec![
            state(original, true, 2, "manual"),
            state(twin_b, true, 2, "autoscaler")
        ]
    );

    crate::test_support::set_default_environment(&db.pool, &e2).await?;
    let detached = upsert_pool_config(&store, request(0, false, Some(twin_b)), true).await?;
    assert_eq!(detached.template_release_id, twin_b);
    let after_detach = vec![
        state(original, true, 2, "manual"),
        state(twin_b, false, 0, "manual"),
    ];
    assert_eq!(
        states().await?,
        after_detach,
        "only the named twin's config is detached, disabled and operator-owned"
    );

    // A release id under another hash, or of another template, is refused
    // rather than resolved; one that names no hash needs only the template.
    let mut wrong_hash = request(0, false, Some(twin_b));
    wrong_hash.content_hash = Some("sha256:another-build".to_string());
    let mut wrong_template = request(0, false, Some(twin_b));
    wrong_template.template_name = "other".to_string();
    for mismatched in [wrong_hash, wrong_template] {
        let refused = upsert_pool_config(&store, mismatched, true)
            .await
            .expect_err("a release id the request's template and hash don't name is refused");
        assert_eq!(refused.status(), axum::http::StatusCode::NOT_FOUND);
    }
    assert_eq!(states().await?, after_detach);
    let mut hashless = request(0, false, Some(twin_b));
    hashless.content_hash = None;
    let again = upsert_pool_config(&store, hashless, true).await?;
    assert_eq!(again.template_release_id, twin_b);
    assert_eq!(states().await?, after_detach);

    // Naming the original is a change to A, which C, the twin creates
    // resolve to now, takes too.
    upsert_pool_config(&store, request(5, true, Some(original)), true).await?;
    assert_eq!(
        states().await?,
        vec![
            state(original, true, 5, "manual"),
            state(twin_b, false, 0, "manual"),
            state(twin_c, true, 5, "manual")
        ]
    );

    // So is the same request without the id.
    upsert_pool_config(&store, request(0, false, None), true).await?;
    assert_eq!(
        states().await?,
        vec![
            state(original, false, 0, "manual"),
            state(twin_b, false, 0, "manual"),
            state(twin_c, false, 0, "manual")
        ]
    );
    Ok(())
}

/// A release in another environment under the other spelling of the hash
/// isn't an original, though no earlier release shares its spelling: the
/// build's original is the bare release registration finds, across both
/// spellings. Upserting or disabling the other release's config leaves the
/// original's pool for the same shape alone.
#[tokio::test(flavor = "current_thread")]
async fn another_spellings_release_elsewhere_leaves_the_originals_pool() -> TestResult {
    let db = pg_test_db().await?;
    insert_ready_pool_slot(&db.pool).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
    );
    let bare = "c".repeat(64);
    let mut releases = Vec::new();
    for (hash, environment, offset) in [
        (bare.clone(), None, "3 minutes"),
        (
            format!("sha256:{bare}"),
            Some(format!("sha256:{}", "7".repeat(64))),
            "4 minutes",
        ),
    ] {
        releases.push(
            sqlx::query_scalar::<_, Uuid>(sql!(
                "INSERT INTO template_releases
                     (template_name, type, hash, created_at, execution_metadata)
                 VALUES ('seed', 'fc', $1, NOW() + $3::INTERVAL,
                         CASE WHEN $2::TEXT IS NULL THEN NULL
                              ELSE jsonb_build_object('environment', $2::TEXT) END)
                 RETURNING id"
            ))
            .bind(hash)
            .bind(environment)
            .bind(offset)
            .fetch_one(&db.pool)
            .await?,
        );
    }
    let (original, elsewhere) = (releases[0], releases[1]);
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string)
         VALUES ($1, 's3://templates-bucket/seed?region=us-east-1')"
    ))
    .bind(original)
    .execute(&db.pool)
    .await?;
    let mut configs = Vec::new();
    for release in [original, elsewhere] {
        configs.push(
            sqlx::query_scalar::<_, Uuid>(sql!(
                "INSERT INTO kubernetes_pool_configs (
                     template_release_id, vcpu, memory_mib, target_ready_slots,
                     refill_per_tick, max_warming_slots, namespace, enabled, managed_by
                 )
                 VALUES ($1, 4, 2048, 1, 1, 1, 'sandboxes', TRUE, 'manual')
                 RETURNING id"
            ))
            .bind(release)
            .fetch_one(&db.pool)
            .await?,
        );
    }

    // An edit naming the other release, where creates boot twins, changes
    // only its config, as its disable does, though creates of either spelling
    // resolve to the original. An upsert naming only the hash would land on
    // the digest's original instead.
    let upserted = upsert_pool_config(
        &store,
        ClusterPoolConfigRequest {
            template_name: "seed".to_string(),
            content_hash: Some(format!("sha256:{bare}")),
            vcpu: 4,
            memory_mib: 2048,
            disk_mib: None,
            reset_disk_to_template_default: false,
            target_ready_slots: 3,
            refill_per_tick: 1,
            max_warming_slots: 1,
            namespace: None,
            enabled: true,
            encryptable: false,
            template_release_id: Some(elsewhere),
        },
        true,
    )
    .await?;
    assert_eq!(upserted.id, configs[1]);
    let targets: Vec<(Uuid, i32)> = sqlx::query_as(sql!(
        "SELECT template_release_id, target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = ANY($1)
         ORDER BY created_at, id"
    ))
    .bind(vec![original, elsewhere])
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(targets, vec![(original, 1), (elsewhere, 3)]);

    disable_pool_config(&store, configs[1]).await?;
    let enabled: Vec<(Uuid, bool)> = sqlx::query_as(sql!(
        "SELECT template_release_id, enabled
         FROM kubernetes_pool_configs
         WHERE id = ANY($1)
         ORDER BY created_at, id"
    ))
    .bind(&configs)
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(enabled, vec![(original, true), (elsewhere, false)]);
    Ok(())
}

/// The twin creates resolve to can change after an upsert wrote its config:
/// here the default moves from E1 to E2. Disabling the original's config
/// then disables the shape on every twin, the E1 twin's included, which no
/// longer resolves. Another shape's config on a twin is left alone.
#[tokio::test(flavor = "current_thread")]
async fn disabling_an_original_disables_every_twins_config_for_the_shape() -> TestResult {
    let db = pg_test_db().await?;
    insert_ready_pool_slot(&db.pool).await?;
    let original: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases WHERE hash = 'sha256:newer-untagged'"
    ))
    .fetch_one(&db.pool)
    .await?;
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    let e1 = format!("sha256:{}", "7".repeat(64));
    let e2 = format!("sha256:{}", "8".repeat(64));
    let mut twins = Vec::new();
    for (environment, offset) in [(&e1, "1 minute"), (&e2, "2 minutes")] {
        twins.push(
            sqlx::query_scalar::<_, Uuid>(sql!(
                "INSERT INTO template_releases (template_name, type, hash, created_at, execution_metadata)
                 VALUES ('seed', 'fc', 'sha256:newer-untagged', NOW() + $2::INTERVAL,
                         jsonb_build_object('environment', $1::TEXT))
                 RETURNING id"
            ))
            .bind(environment)
            .bind(offset)
            .fetch_one(&db.pool)
            .await?,
        );
    }
    let (twin_e1, twin_e2) = (twins[0], twins[1]);
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
    );
    let request = |memory_mib| ClusterPoolConfigRequest {
        template_name: "seed".to_string(),
        content_hash: Some("sha256:newer-untagged".to_string()),
        vcpu: 4,
        memory_mib,
        disk_mib: None,
        reset_disk_to_template_default: false,
        target_ready_slots: 2,
        refill_per_tick: 1,
        max_warming_slots: 1,
        namespace: None,
        enabled: true,
        encryptable: false,
        template_release_id: None,
    };
    let state = |release: Uuid, memory_mib: i32| {
        let pool = db.pool.clone();
        async move {
            sqlx::query_as::<_, (bool, i32)>(sql!(
                "SELECT enabled, target_ready_slots FROM kubernetes_pool_configs
                 WHERE template_release_id = $1 AND vcpu = 4 AND memory_mib = $2"
            ))
            .bind(release)
            .bind(memory_mib)
            .fetch_one(&pool)
            .await
        }
    };

    // With E1 the default, the upsert writes the E1 twin's config, for this
    // shape and another one.
    crate::test_support::set_default_environment(&db.pool, &e1).await?;
    let config = upsert_pool_config(&store, request(2048), true).await?;
    upsert_pool_config(&store, request(4096), true).await?;
    assert_eq!(state(twin_e1, 2048).await?, (true, 2));
    // The default moves on, and the next upsert writes the E2 twin's config.
    crate::test_support::set_default_environment(&db.pool, &e2).await?;
    upsert_pool_config(&store, request(2048), true).await?;
    assert_eq!(state(twin_e2, 2048).await?, (true, 2));

    disable_pool_config(&store, config.id).await?;
    assert_eq!(state(original, 2048).await?, (false, 0));
    assert_eq!(
        state(twin_e1, 2048).await?,
        (false, 0),
        "the twin creates no longer resolve to is disabled too"
    );
    assert_eq!(state(twin_e2, 2048).await?, (false, 0));
    assert_eq!(
        state(twin_e1, 4096).await?,
        (true, 2),
        "another shape's config stays"
    );
    Ok(())
}

/// A campaign's twin insert copies the original's manual configs in the
/// transaction that makes the twin, holding the template row lock. An
/// operator's disable of the original that races it waits for that lock, so
/// it sees the committed twin and disables the twin's copy too, rather than
/// each missing the other's uncommitted write.
#[tokio::test(flavor = "current_thread")]
async fn a_disable_racing_a_twin_insert_reaches_the_twins_copy() -> TestResult {
    // The campaign's transaction, the disable's and the lock poll's.
    let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
    insert_ready_pool_slot(&db.pool).await?;
    let original: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases WHERE hash = 'sha256:newer-untagged'"
    ))
    .fetch_one(&db.pool)
    .await?;
    let e1 = format!("sha256:{}", "7".repeat(64));
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    crate::test_support::set_default_environment(&db.pool, &e1).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
    );
    let config = upsert_pool_config(
        &store,
        ClusterPoolConfigRequest {
            template_name: "seed".to_string(),
            content_hash: Some("sha256:newer-untagged".to_string()),
            template_release_id: None,
            vcpu: 4,
            memory_mib: 2048,
            disk_mib: None,
            reset_disk_to_template_default: false,
            target_ready_slots: 1,
            refill_per_tick: 1,
            max_warming_slots: 1,
            namespace: None,
            enabled: true,
            encryptable: false,
        },
        true,
    )
    .await?;

    // The campaign's transaction, as the twin insert makes it: the template
    // row lock, then the twin.
    let mut campaign = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM templates WHERE name = 'seed' FOR NO KEY UPDATE"
    ))
    .execute(&mut *campaign)
    .await?;
    let twin: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, created_at, execution_metadata)
         VALUES ('seed', 'fc', 'sha256:newer-untagged', NOW() + INTERVAL '1 minute',
                 jsonb_build_object('environment', $1::TEXT))
         RETURNING id"
    ))
    .bind(&e1)
    .fetch_one(&mut *campaign)
    .await?;

    let holder: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *campaign)
        .await?;
    // The copy runs only once the disable waits on the template lock, so it
    // reads the original's config as it was before the disable.
    let (disabled, copied) = tokio::join!(disable_pool_config(&store, config.id), async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            // Something waits on the campaign's backend: the operator's
            // write, on the template row.
            let waiting: bool = sqlx::query_scalar(sql!(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE $1 = ANY(pg_blocking_pids(pid))
                 )"
            ))
            .bind(holder)
            .fetch_one(&db.pool)
            .await?;
            if waiting {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err("the disable never waited on the template lock".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let copied =
            WarmPoolStore::copy_manual_configs_from_releases(&mut campaign, &[original], twin)
                .await?;
        campaign.commit().await?;
        TestResult::Ok(copied)
    });
    assert_eq!(copied?, 1);
    assert_eq!(disabled?.id, config.id);
    let enabled: Vec<(Uuid, bool)> = sqlx::query_as(sql!(
        "SELECT template_release_id, enabled
         FROM kubernetes_pool_configs
         WHERE template_release_id = ANY($1)
         ORDER BY created_at, id"
    ))
    .bind(vec![original, twin])
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(enabled, vec![(original, false), (twin, false)]);
    Ok(())
}

/// A template being deleted gets no twin, so an operator's pool-config write
/// has no twin insert to serialize with. Once the template is marked
/// `deleting`, the write takes no row lock and doesn't queue behind the
/// delete's cleanup: here the cleanup holds the template row, and the
/// disable still goes through.
#[tokio::test(flavor = "current_thread")]
async fn a_pool_write_on_a_template_being_deleted_takes_no_template_lock() -> TestResult {
    // The delete's transaction and the disable's.
    let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
    insert_ready_pool_slot(&db.pool).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
    );
    let config = upsert_pool_config(
        &store,
        ClusterPoolConfigRequest {
            template_name: "seed".to_string(),
            content_hash: Some("sha256:newer-untagged".to_string()),
            template_release_id: None,
            vcpu: 4,
            memory_mib: 2048,
            disk_mib: None,
            reset_disk_to_template_default: false,
            target_ready_slots: 1,
            refill_per_tick: 1,
            max_warming_slots: 1,
            namespace: None,
            enabled: true,
            encryptable: false,
        },
        true,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE templates SET desired_state = 'deleting' WHERE name = 'seed'"
    ))
    .execute(&db.pool)
    .await?;
    let mut delete = db.pool.begin().await?;
    sqlx::query(sql!(
        "SELECT 1 FROM templates WHERE name = 'seed' FOR UPDATE"
    ))
    .execute(&mut *delete)
    .await?;

    let disabled = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        disable_pool_config(&store, config.id),
    )
    .await
    .map_err(|_| "the disable waited on the deleting template's row")??;
    assert_eq!(disabled.id, config.id);
    assert!(
        !disabled.enabled,
        "the disable applied while the delete held the row"
    );
    assert_eq!(disabled.target_ready_slots, 0);
    delete.rollback().await?;
    Ok(())
}

/// As for a disable: an operator's upsert of the original's config that races
/// a twin insert waits for the template lock, so it sees the committed twin
/// and gives the twin's copy the new settings too.
#[tokio::test(flavor = "current_thread")]
async fn an_upsert_racing_a_twin_insert_reaches_the_twins_copy() -> TestResult {
    // The campaign's transaction, the upsert's and the lock poll's.
    let db = pplx_space_db_test_fixtures::pg_test_db_with_max_connections(4).await?;
    insert_ready_pool_slot(&db.pool).await?;
    let original: Uuid = sqlx::query_scalar(sql!(
        "SELECT id FROM template_releases WHERE hash = 'sha256:newer-untagged'"
    ))
    .fetch_one(&db.pool)
    .await?;
    let e1 = format!("sha256:{}", "7".repeat(64));
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    crate::test_support::set_default_environment(&db.pool, &e1).await?;
    let store = K8sStore::new(
        db.pool.clone(),
        "sandboxes".to_string(),
        crate::controllers::substrate::ActivationClientAttribution::Enabled,
    );
    let request = |target_ready_slots| ClusterPoolConfigRequest {
        template_name: "seed".to_string(),
        content_hash: Some("sha256:newer-untagged".to_string()),
        template_release_id: None,
        vcpu: 4,
        memory_mib: 2048,
        disk_mib: None,
        reset_disk_to_template_default: false,
        target_ready_slots,
        refill_per_tick: 1,
        max_warming_slots: 1,
        namespace: None,
        enabled: true,
        encryptable: false,
    };
    upsert_pool_config(&store, request(1), true).await?;

    // The campaign's transaction, as the twin insert makes it: the template
    // row lock, then the twin.
    let mut campaign = db.pool.begin().await?;
    // A twin shares its original's storage locations; give it one.
    let location = "s3://templates/seed/newer-untagged?region=us-east-1".to_owned();
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(original)
    .bind(&location)
    .execute(&db.pool)
    .await?;
    // The production insert, so the lock it takes and holds through the copy
    // is the one under test.
    let target = pplx_space_template_release_store::PartitionSelector {
        environment: e1.parse::<pplx_space_execution_spec::EnvironmentReference>()?,
        includes_unbound: false,
    };
    let twin = match pplx_space_template_release_store::insert_twin(
        &mut campaign,
        original,
        target,
        &serde_json::json!({ "environment": e1 }),
        std::slice::from_ref(&location),
    )
    .await?
    {
        pplx_space_template_release_store::InsertTwin::Inserted(twin) => twin,
        other => return Err(format!("the twin wasn't inserted: {other:?}").into()),
    };

    let holder: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *campaign)
        .await?;
    // The copy runs only once the upsert waits on the template lock, so it
    // reads the original's config as it was before the upsert.
    let (upserted, copied) = tokio::join!(upsert_pool_config(&store, request(5), true), async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            // Something waits on the campaign's backend: the operator's
            // write, on the template row.
            let waiting: bool = sqlx::query_scalar(sql!(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE $1 = ANY(pg_blocking_pids(pid))
                 )"
            ))
            .bind(holder)
            .fetch_one(&db.pool)
            .await?;
            if waiting {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                return Err("the upsert never waited on the template lock".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let copied =
            WarmPoolStore::copy_manual_configs_from_releases(&mut campaign, &[original], twin)
                .await?;
        campaign.commit().await?;
        TestResult::Ok(copied)
    });
    assert_eq!(copied?, 1);
    assert_eq!(upserted?.template_release_id, original);
    let targets: Vec<(Uuid, i32)> = sqlx::query_as(sql!(
        "SELECT template_release_id, target_ready_slots
         FROM kubernetes_pool_configs
         WHERE template_release_id = ANY($1)
         ORDER BY created_at, id"
    ))
    .bind(vec![original, twin])
    .fetch_all(&db.pool)
    .await?;
    assert_eq!(targets, vec![(original, 5), (twin, 5)]);
    Ok(())
}

/// A claim that names no release takes the slots of the release its build
/// resolves to only where creates boot the twin they resolve to. Elsewhere it
/// keeps the tagged release's slots even when a twin bound to the default
/// exists.
#[tokio::test(flavor = "current_thread")]
async fn a_pool_claim_takes_the_twins_slots_only_where_creates_boot_it() -> TestResult {
    let db = pg_test_db().await?;
    let slot = insert_ready_pool_slot(&db.pool).await?;
    // Twins exist only once the `(template_name, hash)` unique index is
    // dropped; the test drops it to stand in for the contract migration.
    sqlx::query(sql!(
        "DROP INDEX IF EXISTS idx_template_releases_template_name_hash_unique"
    ))
    .execute(&db.pool)
    .await?;
    let e1 = format!("sha256:{}", "7".repeat(64));
    let twin: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO template_releases (template_name, type, hash, created_at, execution_metadata)
         SELECT template_name, type, hash, NOW() + INTERVAL '1 minute',
                jsonb_build_object('environment', $2::TEXT)
         FROM template_releases WHERE id = $1
         RETURNING id"
    ))
    .bind(slot.release_id)
    .bind(&e1)
    .fetch_one(&db.pool)
    .await?;
    crate::test_support::set_default_environment(&db.pool, &e1).await?;
    let actor = ActorRef::new(slot.user_id, slot.org_id, Email::parse("pool@example.com")?);
    let env = BTreeMap::new();
    let auto_lifecycle = AutoLifecyclePolicy::disabled();
    let template_network_policy = NetworkPolicyV1::deny_all();
    let request = pooled_claim_request(&actor, &env, &auto_lifecycle, &template_network_policy);

    // Creates resolve to the twin, which has no slots, so the claim misses
    // and leaves the original's slot ready.
    let miss = claim_pooled_execution(&db.pool, &request, "space", true).await?;
    assert!(miss.claim.is_none(), "the twin has no ready slot");

    // Once the twin has a ready slot, the claim takes it.
    let twin_sandbox_id = SandboxId::new();
    let limits = DiskIoLimits::default();
    let twin_config: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO kubernetes_pool_configs (
             template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         VALUES ($1, 4, 2048, 1, 1, 1, 'space', true, $2, $3)
         RETURNING id"
    ))
    .bind(twin)
    .bind(i64::try_from(limits.max_operations_per_sec())?)
    .bind(i64::try_from(limits.max_bandwidth_mib_per_sec())?)
    .fetch_one(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, pod_uid,
             pod_ip, k8s_node_name, state, ready_at,
             disk_io_max_operations_per_sec, disk_io_max_bandwidth_mib_per_sec
         )
         SELECT $1, $2, $3, template_name, hash, 4, 2048, 'space-sbx-pooled-twin',
                'space', 'twin-pod-uid', '10.90.0.12'::inet, 'pool-node-1', 'ready', NOW(),
                $4, $5
         FROM template_releases WHERE id = $2"
    ))
    .bind(twin_config)
    .bind(twin)
    .bind(twin_sandbox_id)
    .bind(i64::try_from(limits.max_operations_per_sec())?)
    .bind(i64::try_from(limits.max_bandwidth_mib_per_sec())?)
    .execute(&db.pool)
    .await?;
    let twin_hit = claim_pooled_execution(&db.pool, &request, "space", true)
        .await?
        .claim
        .expect("the twin's ready slot is claimed");
    assert_eq!(twin_hit.sandbox_id, twin_sandbox_id);

    // Where creates don't boot the twin, they claim the tagged release's slot.
    let hit = claim_pooled_execution(&db.pool, &request, "space", false)
        .await?
        .claim
        .expect("the original's ready slot is claimed");
    assert_eq!(hit.sandbox_id, slot.sandbox_id);
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
        template_release_id: None,
    };

    for enabled in [true, false] {
        let rejected = upsert_pool_config(&store, request(enabled), false)
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
        template_release_id: None,
    };

    assert_eq!(
        upsert_pool_config(&store, request(Some(32768), false), false)
            .await?
            .disk_mib,
        Some(32768)
    );
    for disk_mib in [0, 131073, u32::MAX] {
        let error = upsert_pool_config(&store, request(Some(disk_mib), false), false)
            .await
            .unwrap_err();
        assert_eq!(error.status(), axum::http::StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        upsert_pool_config(&store, request(None, false), false)
            .await?
            .disk_mib,
        Some(32768)
    );
    assert_eq!(
        upsert_pool_config(&store, request(None, true), false)
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
            template_release_id: None,
        },
        false,
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
    let reclaimed = claim_pooled_execution(&db.pool, &request, "space", false).await?;
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
    let reclaimed = claim_pooled_execution(&db.pool, &request, "space", false).await?;
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
    Ok(claim_pooled_execution(pool, &request, "space", false)
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
    use pplx_space_sandbox_pod::SANDBOX_CLIENT_ID_LABEL;
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
    use pplx_space_accounts::ClientId;

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
    use pplx_space_accounts::ClientId;
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
    use pplx_space_accounts::ClientId;

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
    use pplx_space_accounts::ClientId;

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
    use pplx_space_accounts::ClientId;

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
/// (`resolve_pool_template_release`): the digest's original under either
/// spelling when a hash is given, answered in its stored spelling; otherwise
/// the `latest` holder, else the newest by `created_at`. It takes no runtime and no caller org, and never
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
        // Either spelling names the digest, as sandbox create, ClusterApi
        // by-hash and observed-demand matching read it.
        (RESOLVER_TEMPLATE, Some(sha('a')), Some((w.bare, hex('a')))),
        (
            RESOLVER_TEMPLATE,
            Some(sha('b')),
            Some((w.prefixed, sha('b'))),
        ),
        (
            RESOLVER_TEMPLATE,
            Some(hex('b')),
            Some((w.prefixed, sha('b'))),
        ),
        (
            RESOLVER_TEMPLATE,
            Some(blake('c')),
            Some((w.blake3, blake('c'))),
        ),
        // A missed hash does not fall back to `latest`.
        (RESOLVER_TEMPLATE, Some(sha('c')), None),
        // Both spellings stored, neither with a storage location: the older
        // row, whichever spelling the request names, as sandbox create and
        // observed-demand matching pick it.
        (
            RESOLVER_TEMPLATE,
            Some(hex('9')),
            Some((w.dual_prefixed, sha('9'))),
        ),
        (
            RESOLVER_TEMPLATE,
            Some(sha('9')),
            Some((w.dual_prefixed, sha('9'))),
        ),
        (
            RESOLVER_TEMPLATE,
            Some(hex('8')),
            Some((w.early_bare, hex('8'))),
        ),
        (
            RESOLVER_TEMPLATE,
            Some(sha('8')),
            Some((w.early_bare, hex('8'))),
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

/// A pool config pinned to a digest stored under both spellings targets the
/// digest's original under either spelling, the release creates resolve and
/// the warm-pool store counts demand for, with twins and without. An unpinned
/// config targets the original of the digest tagged `latest`, even when the
/// tag sits on the other spelling's original, as one written before the
/// shared rule can.
#[rstest::rstest]
#[case::without_twins(false)]
#[case::with_twins(true)]
#[tokio::test(flavor = "current_thread")]
async fn a_pinned_pool_targets_the_digest_original(#[case] twins: bool) -> TestResult {
    use pplx_space_template_release_store::spelling_pairs::{OLD_INDEX, seed_spelling_pairs};
    use pplx_space_template_store::test_support::tag_release;
    const TEMPLATE: &str = "spelling-pairs";
    let db = pg_test_db().await?;
    sqlx::query(sql!("INSERT INTO templates (id, name) VALUES ($1, $2)"))
        .bind(Uuid::now_v7())
        .bind(TEMPLATE)
        .execute(&db.pool)
        .await?;
    if twins {
        sqlx::query(sqlx::AssertSqlSafe(sql!(format!(
            "DROP INDEX IF EXISTS {OLD_INDEX}"
        ))))
        .execute(&db.pool)
        .await?;
    }
    let twin_metadata = twins.then_some(r#"{"environment": "fixture-twin"}"#);
    for pair in seed_spelling_pairs(&db.pool, TEMPLATE, twin_metadata).await? {
        for hash in [&pair.canonical_hash, &pair.bare_hash] {
            let mut tx = db.pool.begin().await?;
            let resolved = resolve_pool_template_release(&mut tx, TEMPLATE, Some(hash)).await?;
            tx.rollback().await?;
            assert_eq!(
                resolved.map(|row| (row.id, row.content_hash)),
                Some((pair.original, pair.original_hash().to_owned())),
                "{hash}"
            );
        }
        tag_release(&db.pool, TEMPLATE, pair.other(), "latest").await?;
        let mut tx = db.pool.begin().await?;
        let resolved = resolve_pool_template_release(&mut tx, TEMPLATE, None).await?;
        tx.rollback().await?;
        assert_eq!(
            resolved.map(|row| (row.id, row.content_hash)),
            Some((pair.original, pair.original_hash().to_owned())),
            "latest on {}",
            pair.other()
        );
    }
    Ok(())
}
