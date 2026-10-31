use pplx_space_common_core::types::{NodeId, OrgId};

use pplx_space_cluster_pool::{PoolConfigOwner, PoolSlotState};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, pg_test_db, pg_test_db_with_max_connections};
use pplx_space_public_api::{OperationId, OperationKind, OperationTargetType};

use super::*;
use crate::public_operations::{
    ApiProduct, CreatePublicOperation, CreatePublicOperationResult, PublicOperationStore,
};

use pplx_space_test_env::TestResult;

#[tokio::test]
async fn template_delete_inventory_query_uses_direct_node_deleted_state() -> TestResult {
    let db = pg_test_db().await?;
    let template = TemplateName::parse("app-runtime")?;
    insert_template(&db.pool, &template).await?;

    let active_direct_node = NodeId::new();
    let deleted_direct_node = NodeId::new();
    let k8s_node = NodeId::new();
    let retired_k8s_node = NodeId::new();

    insert_node(&db.pool, active_direct_node, "worker", "space", 10).await?;
    insert_direct_node(&db.pool, active_direct_node, false).await?;
    insert_inventory(&db.pool, active_direct_node, &template).await?;

    insert_node(&db.pool, deleted_direct_node, "worker", "space", 11).await?;
    insert_direct_node(&db.pool, deleted_direct_node, true).await?;
    insert_inventory(&db.pool, deleted_direct_node, &template).await?;

    insert_node(&db.pool, k8s_node, "worker", "k8s", 12).await?;
    insert_inventory(&db.pool, k8s_node, &template).await?;

    // A retired registry row excludes only Direct nodes.
    insert_node(&db.pool, retired_k8s_node, "worker", "k8s", 13).await?;
    insert_direct_node(&db.pool, retired_k8s_node, true).await?;
    insert_inventory(&db.pool, retired_k8s_node, &template).await?;

    let mut nodes = nodes_with_template_inventory(&db.pool, &template).await?;
    nodes.sort_unstable();
    let mut expected = vec![active_direct_node, k8s_node, retired_k8s_node];
    expected.sort_unstable();
    assert_eq!(nodes, expected);

    Ok(())
}

/// Disabled pool configs with no slots and disabled policies are the only
/// warm-pool rows left once an operator has followed the refusal's advice.
/// The delete removes them instead of failing on the configs' release keys
/// until its retries run out.
#[tokio::test]
async fn delete_removes_disabled_empty_pool_configs_and_policies() -> TestResult {
    let db = pg_test_db().await?;
    let template = TemplateName::parse("retired-pools")?;
    insert_template(&db.pool, &template).await?;
    let manual_release = insert_release(&db.pool, &template, 0x11).await?;
    let autoscaler_release = insert_release(&db.pool, &template, 0x12).await?;
    let manual =
        insert_pool_config(&db.pool, manual_release, PoolConfigOwner::Manual, false).await?;
    let autoscaler = insert_pool_config(
        &db.pool,
        autoscaler_release,
        PoolConfigOwner::Autoscaler,
        false,
    )
    .await?;
    insert_policy(&db.pool, &template, false).await?;

    let operation = create_delete_operation(&db.pool, &template).await?;
    let result = DeleteTemplateOperationExecutor::new(db.pool.clone())
        .execute(&operation)
        .await;

    assert_eq!(
        result.state,
        OperationState::Succeeded,
        "{}",
        result.progress_message
    );
    assert_eq!(
        fetch_template_desired_state(&db.pool, &template).await?,
        Some(TemplateDesiredState::Deleted)
    );
    let configs: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM kubernetes_pool_configs WHERE id = ANY($1)"
    ))
    .bind(vec![manual, autoscaler])
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(configs, 0);
    let policies: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM warm_pool_policies WHERE template_name = $1"
    ))
    .bind(template.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(policies, 0, "a recreated template must not inherit them");
    Ok(())
}

/// A slot that commits while the delete runs gets past the up-front check
/// and is caught by the release key instead. The delete fails for good with
/// the refusal naming the slot, rather than retrying until its budget runs
/// out, and the slot is not cascaded away with its disabled config.
#[tokio::test]
async fn a_slot_committed_while_the_delete_runs_fails_it_terminally() -> TestResult {
    // The open slot transaction, the delete and the lock-wait probe.
    let db = pg_test_db_with_max_connections(3).await?;
    let template = TemplateName::parse("racing-slot")?;
    insert_template(&db.pool, &template).await?;
    let release = insert_release(&db.pool, &template, 0x21).await?;
    let config = insert_pool_config(&db.pool, release, PoolConfigOwner::Manual, false).await?;
    let operation = create_delete_operation(&db.pool, &template).await?;

    let mut slot = db.pool.begin().await?;
    insert_pool_slot(&mut *slot, config, release, &template, PoolSlotState::Ready).await?;
    let executor = DeleteTemplateOperationExecutor::new(db.pool.clone());
    let delete = tokio::spawn(async move { executor.execute(&operation).await });
    wait_until_a_session_waits_on_a_lock(&db.pool, &delete).await?;
    slot.commit().await?;
    let result = delete.await?;

    assert_eq!(
        result.state,
        OperationState::Failed,
        "{}",
        result.progress_message
    );
    assert_eq!(result.terminal_error_code, Some(PublicErrorCode::Conflict));
    assert!(
        result.progress_message.contains("1 open warm-pool slot"),
        "{}",
        result.progress_message
    );
    assert_eq!(
        fetch_template_desired_state(&db.pool, &template).await?,
        Some(TemplateDesiredState::Deleting)
    );
    let slots: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM kubernetes_pool_slots WHERE config_id = $1"
    ))
    .bind(config)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(slots, 1);
    Ok(())
}

/// A disabled config written after the delete's cleanup took its lock is not
/// a use the re-read counts, so the release delete's foreign-key error is
/// requeued rather than failing the operation, and the retry's cleanup
/// deletes the config.
#[tokio::test]
async fn a_disabled_config_committed_while_the_delete_runs_is_retried_away() -> TestResult {
    // The open config transaction, the delete and the lock-wait probe.
    let db = pg_test_db_with_max_connections(3).await?;
    let template = TemplateName::parse("racing-config")?;
    insert_template(&db.pool, &template).await?;
    let release = insert_release(&db.pool, &template, 0x22).await?;
    let operation = create_delete_operation(&db.pool, &template).await?;

    let mut config = db.pool.begin().await?;
    let config_id =
        insert_pool_config(&mut *config, release, PoolConfigOwner::Manual, false).await?;
    let executor = DeleteTemplateOperationExecutor::new(db.pool.clone());
    let delete = tokio::spawn({
        let executor = executor.clone();
        let operation = operation.clone();
        async move { executor.execute(&operation).await }
    });
    wait_until_a_session_waits_on_a_lock(&db.pool, &delete).await?;
    config.commit().await?;
    let result = delete.await?;

    assert_eq!(
        (result.state, result.terminal_error_code),
        (OperationState::Queued, None),
        "{}",
        result.progress_message
    );
    assert_eq!(result.retry_after, Some(TEMPLATE_DELETE_RETRY_AFTER));
    let result = executor.execute(&operation).await;
    assert_eq!(
        result.state,
        OperationState::Succeeded,
        "{}",
        result.progress_message
    );
    let configs: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM kubernetes_pool_configs WHERE id = $1"
    ))
    .bind(config_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(configs, 0);
    Ok(())
}

/// An operation that finds its template still `present`, because the route's
/// mark never committed, checks for uses before it marks the template
/// `deleting`. A use found there fails it and leaves the template `present`,
/// matching the 409 a same-key replay of the route gets.
#[tokio::test]
async fn an_unmarked_delete_of_a_template_in_use_leaves_it_present() -> TestResult {
    let db = pg_test_db().await?;
    let template = TemplateName::parse("unmarked-in-use")?;
    insert_template(&db.pool, &template).await?;
    insert_release(&db.pool, &template, 0x23).await?;
    insert_policy(&db.pool, &template, true).await?;
    let operation = create_delete_operation(&db.pool, &template).await?;

    let result = DeleteTemplateOperationExecutor::new(db.pool.clone())
        .execute(&operation)
        .await;

    assert_eq!(
        (result.state, result.terminal_error_code),
        (OperationState::Failed, Some(PublicErrorCode::Conflict)),
        "{}",
        result.progress_message
    );
    assert_eq!(
        fetch_template_desired_state(&db.pool, &template).await?,
        Some(TemplateDesiredState::Present)
    );
    let error = result.terminal_error_json.ok_or("no terminal error")?;
    assert_eq!(error["reason"], "template_in_use");
    assert_eq!(error["enabled_pool_policies"], "2 vCPU/4096 MiB");
    Ok(())
}

async fn wait_until_a_session_waits_on_a_lock<T>(
    pool: &PgPool,
    task: &tokio::task::JoinHandle<T>,
) -> TestResult {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let waiting: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database()
                    AND wait_event_type = 'Lock'
             )"
        ))
        .fetch_one(pool)
        .await?;
        if waiting {
            return Ok(());
        }
        if task.is_finished() || std::time::Instant::now() >= deadline {
            return Err("the delete never waited on the uncommitted slot".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn create_delete_operation(
    pool: &PgPool,
    template: &TemplateName,
) -> TestResult<ClaimedPublicOperation> {
    let target = template.to_string();
    let created = PublicOperationStore::new(pool.clone())
        .create_operation(CreatePublicOperation {
            org_id: OrgId::from_uuid(SEED_ORG_ID),
            actor_user_id: None,
            api_product: ApiProduct::Admin,
            kind: OperationKind::DeleteTemplate,
            target_type: Some(OperationTargetType::Template),
            target_id: Some(&target),
            payload_json: DeleteTemplateOperationPayload::new(template.clone()).into_json(),
            idempotency: None,
            traceparent: None,
        })
        .await?;
    let CreatePublicOperationResult::Created(id) = created else {
        return Err(format!("unexpected create result {created:?}").into());
    };
    Ok(claimed_delete_operation(id, template))
}

/// The claim the dispatcher hands the executor for delete operation `id`.
pub(crate) fn claimed_delete_operation(
    id: OperationId,
    template: &TemplateName,
) -> ClaimedPublicOperation {
    ClaimedPublicOperation {
        id,
        org_id: OrgId::from_uuid(SEED_ORG_ID),
        actor_user_id: None,
        api_product: ApiProduct::Admin,
        kind: OperationKind::DeleteTemplate,
        target_type: Some(OperationTargetType::Template),
        target_id: Some(template.to_string()),
        payload: DeleteTemplateOperationPayload::new(template.clone()).into_json(),
        attempts: 1,
        claim_generation: 1,
        traceparent: None,
        age_at_first_claim_secs: None,
    }
}

pub(crate) async fn insert_release(
    pool: &PgPool,
    template: &TemplateName,
    hash_byte: u8,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, $2, 'fc', $3)"
    ))
    .bind(id)
    .bind(template.as_str())
    .bind(format!("sha256:{}", format!("{hash_byte:02x}").repeat(32)))
    .execute(pool)
    .await?;
    Ok(id)
}

pub(crate) async fn insert_pool_config(
    executor: impl sqlx::PgExecutor<'_>,
    release_id: Uuid,
    owner: PoolConfigOwner,
    enabled: bool,
) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar::<_, Uuid>(sql!(
        "INSERT INTO kubernetes_pool_configs (
             template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled, managed_by
         )
         VALUES ($1, 2, 4096, 0, 1, 1, 'space-sandboxes', $2, $3)
         RETURNING id"
    ))
    .bind(release_id)
    .bind(enabled)
    .bind(owner.as_str())
    .fetch_one(executor)
    .await?)
}

/// A slot in `state` on `release_id`, which must belong to `template`.
/// Returns the slot's reserved sandbox id.
pub(crate) async fn insert_pool_slot(
    executor: impl sqlx::PgExecutor<'_>,
    config_id: Uuid,
    release_id: Uuid,
    template: &TemplateName,
    state: PoolSlotState,
) -> TestResult<Uuid> {
    let reserved_sandbox_id = Uuid::now_v7();
    let inserted = sqlx::query(sql!(
        "INSERT INTO kubernetes_pool_slots (
             config_id, template_release_id, reserved_sandbox_id, template_name,
             content_hash, vcpu, memory_mib, pod_name, pod_namespace, state
         )
         SELECT $1, r.id, $3, r.template_name, r.hash, 2, 4096,
                $5, 'space-sandboxes', $6
         FROM template_releases r
         WHERE r.id = $2 AND r.template_name = $4"
    ))
    .bind(config_id)
    .bind(release_id)
    .bind(reserved_sandbox_id)
    .bind(template.as_str())
    .bind(format!("warm-slot-{reserved_sandbox_id}"))
    .bind(state.as_str())
    .execute(executor)
    .await?
    .rows_affected();
    if inserted != 1 {
        return Err(format!("release {release_id} is not a release of {template}").into());
    }
    Ok(reserved_sandbox_id)
}

pub(crate) async fn insert_policy(
    pool: &PgPool,
    template: &TemplateName,
    enabled: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO warm_pool_policies (
             template_name, vcpu, memory_mib, min_ready_slots, max_ready_slots, enabled
         )
         VALUES ($1, 2, 4096, 1, 2, $2)"
    ))
    .bind(template.as_str())
    .bind(enabled)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_template(pool: &PgPool, template: &TemplateName) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, $2, NULL, NULL)"
    ))
    .bind(Uuid::now_v7())
    .bind(template.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_node(
    pool: &PgPool,
    node_id: NodeId,
    role: &str,
    managed_by: &str,
    address_octet: u8,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, $3, $4, 'ready')"
    ))
    .bind(node_id)
    .bind(format!("10.0.0.{address_octet}"))
    .bind(role)
    .bind(managed_by)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_direct_node(pool: &PgPool, node_id: NodeId, deleted: bool) -> TestResult {
    let endpoint = format!("10.0.0.{}:9802", node_id.as_uuid().as_bytes()[15]);
    if deleted {
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
             VALUES ($1, $2, NOW())"
        ))
        .bind(node_id)
        .bind(endpoint)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
             VALUES ($1, $2, NULL)"
        ))
        .bind(node_id)
        .bind(endpoint)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn insert_inventory(pool: &PgPool, node_id: NodeId, template: &TemplateName) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime
         )
         VALUES ($1, $2, 'sha256:test', 'fc')"
    ))
    .bind(node_id)
    .bind(template.as_str())
    .execute(pool)
    .await?;
    Ok(())
}
