use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_cluster_pool::{TemplateWarmPoolUsage, WarmPoolStore};
use pplx_space_common_core::types::{SubstrateKind, TemplateDesiredState, TemplateName};
use pplx_space_public_api::{
    OperationState, PublicErrorCode, PublicErrorDetail, PublicOperationResult,
    PublicTemplateOperationResult,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::{
    public_operations::{ClaimedPublicOperation, PublicOperationRequeueReason},
    public_sandbox_operations::PublicOperationExecutionResult,
    reconcile_queue::{
        NetworkPolicyTarget, ReconcileElementRef, ReconcileRef,
        delete_reconcile_state_for_elements, enqueue_node_reconcile_refs_for_nodes,
    },
    reconcile_trigger::enqueue_triggers,
};

const TEMPLATE_DELETE_RETRY_AFTER: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteTemplateOperationPayload {
    pub template: String,
}

impl DeleteTemplateOperationPayload {
    pub fn new(template: TemplateName) -> Self {
        Self {
            template: template.to_string(),
        }
    }

    pub fn into_json(self) -> serde_json::Value {
        serde_json::to_value(self).expect("delete template operation payload serializes")
    }
}

#[derive(Debug, Clone)]
pub struct DeleteTemplateOperationExecutor {
    pool: PgPool,
}

impl DeleteTemplateOperationExecutor {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn execute(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> PublicOperationExecutionResult {
        match self.execute_inner(operation).await {
            Ok(result) => result,
            Err(error) => internal_failure(operation, error),
        }
    }

    async fn execute_inner(
        &self,
        operation: &ClaimedPublicOperation,
    ) -> Result<PublicOperationExecutionResult, sqlx::Error> {
        let payload = match serde_json::from_value::<DeleteTemplateOperationPayload>(
            operation.payload.clone(),
        ) {
            Ok(payload) => payload,
            Err(error) => {
                return Ok(terminal_failure(
                    operation,
                    PublicErrorCode::InvalidRequest,
                    "delete template operation payload is invalid",
                    serde_json::json!({
                        "operation_id": operation.id.to_string(),
                        "reason": "invalid_operation_payload",
                        "error": error.to_string(),
                    }),
                ));
            }
        };
        let template = match TemplateName::parse(&payload.template) {
            Ok(template) => template,
            Err(error) => {
                return Ok(terminal_failure(
                    operation,
                    PublicErrorCode::InvalidRequest,
                    "template name is invalid",
                    serde_json::json!({
                        "operation_id": operation.id.to_string(),
                        "template": payload.template,
                        "reason": "invalid_template_name",
                        "error": error.to_string(),
                    }),
                ));
            }
        };

        let Some(desired_state) = fetch_template_desired_state(&self.pool, &template).await? else {
            return Ok(terminal_failure(
                operation,
                PublicErrorCode::NotFound,
                "template not found",
                serde_json::json!({
                    "operation_id": operation.id.to_string(),
                    "template": template.to_string(),
                    "reason": "template_not_found",
                }),
            ));
        };

        if desired_state == TemplateDesiredState::Present {
            // Check before marking. The route commits the operation before
            // its mark transaction, so this can see a `present` template for
            // two reasons. If the route's mark failed and a use appeared
            // before the caller's same-key replay, the replay gets the
            // route's 409; this operation fails here and leaves the template
            // `present`, as that 409 says, instead of marking it `deleting`
            // and dropping its node copies. If the dispatcher claimed this
            // operation before the route's mark ran and a use committed
            // after the route's check, this operation fails here with the
            // conflict, then the route's mark still sets `deleting` and
            // answers 202; the template stays `deleting` until a new delete
            // replaces the failed one.
            if let Some(in_use) = template_in_use(&self.pool, &template).await? {
                return Ok(in_use_failure(operation, &template, &in_use));
            }
            mark_template_deleting(&self.pool, &template, operation).await?;
        }

        let nodes = nodes_with_template_inventory(&self.pool, &template).await?;
        if !nodes.is_empty() {
            enqueue_template_delete_reconcile(&self.pool, &template, &nodes).await?;
            return Ok(PublicOperationExecutionResult {
                state: OperationState::Queued,
                progress_message: format!("waiting for {} node(s) to delete template", nodes.len()),
                result_json: None,
                terminal_error_code: None,
                terminal_error_json: None,
                retry_after: Some(TEMPLATE_DELETE_RETRY_AFTER),
                requeue_reason: PublicOperationRequeueReason::TransientFailure,
            });
        }

        if let MarkTemplateDeleted::InUse(in_use) =
            mark_template_deleted(&self.pool, &template, operation.id.as_uuid()).await?
        {
            return Ok(in_use_failure(operation, &template, &in_use));
        }
        Ok(PublicOperationExecutionResult {
            state: OperationState::Succeeded,
            progress_message: "template deleted".to_string(),
            result_json: Some(
                serde_json::to_value(PublicOperationResult::Template(
                    PublicTemplateOperationResult {
                        template: template.to_string(),
                        state: TemplateDesiredState::Deleted.as_str().to_string(),
                    },
                ))
                .expect("template operation result serializes"),
            ),
            terminal_error_code: None,
            terminal_error_json: None,
            retry_after: None,
            requeue_reason: PublicOperationRequeueReason::TransientFailure,
        })
    }
}

/// Fail a delete for a template that is still in use. Terminal, not a
/// retry: only an operator, or the end of the sandboxes holding its slots,
/// clears the use. A template already `deleting` stays so, and a later
/// delete request replaces this failed operation.
fn in_use_failure(
    operation: &ClaimedPublicOperation,
    template: &TemplateName,
    in_use: &TemplateInUse,
) -> PublicOperationExecutionResult {
    let mut error_json = serde_json::Map::new();
    error_json.insert("operation_id".to_string(), operation.id.to_string().into());
    error_json.insert("template".to_string(), template.to_string().into());
    for detail in in_use.details() {
        error_json.insert(detail.key, detail.value.into());
    }
    terminal_failure(
        operation,
        PublicErrorCode::Conflict,
        in_use.message(),
        serde_json::Value::Object(error_json),
    )
}

/// What still uses a template and so blocks deleting it: warm-pool state on
/// its releases or name (see [`TemplateWarmPoolUsage`]). The admin delete
/// route refuses with this before it accepts a delete, and the delete
/// operation checks it again before it marks a `present` template
/// `deleting`, and inside the transaction that deletes the releases. There
/// the release keys and the config lock catch a config or slot that commits
/// after the check; nothing catches a policy enabled after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateInUse {
    pub pools: TemplateWarmPoolUsage,
}

impl TemplateInUse {
    /// `None` when nothing uses the template.
    pub async fn check(
        conn: &mut PgConnection,
        template: &TemplateName,
    ) -> Result<Option<Self>, sqlx::Error> {
        let pools = WarmPoolStore::template_usage(conn, template).await?;
        Ok((!pools.is_empty()).then_some(Self { pools }))
    }

    /// The operator-facing refusal: names each use by id or count and says
    /// how each kind clears.
    pub fn message(&self) -> String {
        let pools = &self.pools;
        let mut uses = Vec::new();
        if !pools.enabled_configs.is_empty() {
            let configs = pools
                .enabled_configs
                .iter()
                .map(|config| format!("{} ({})", config.id, config.managed_by))
                .collect::<Vec<_>>()
                .join(", ");
            uses.push(format!("enabled warm-pool configs {configs}"));
        }
        if !pools.enabled_policies.is_empty() {
            uses.push(format!(
                "enabled warm-pool policies {}",
                policy_shapes(pools)
            ));
        }
        if pools.open_slots > 0 {
            uses.push(count_noun(pools.open_slots, "open warm-pool slot"));
        }
        if pools.sandbox_held_slots > 0 {
            let held = if pools.sandbox_held_slots == 1 {
                "1 warm-pool slot held by a sandbox created from the pool".to_string()
            } else {
                format!(
                    "{} warm-pool slots held by sandboxes created from the pool",
                    pools.sandbox_held_slots
                )
            };
            let ids = sandbox_ids(pools);
            let named = if usize::try_from(pools.sandbox_held_slots)
                .is_ok_and(|held| held > pools.sandbox_ids.len())
            {
                format!("including {ids}")
            } else {
                ids
            };
            uses.push(format!("{held} ({named})"));
        }
        let mut message = format!("template is still in use by {}.", uses.join("; "));
        if !pools.enabled_configs.is_empty()
            || !pools.enabled_policies.is_empty()
            || pools.open_slots > 0
        {
            message.push_str(
                " Disable its warm-pool configs and policies and wait for their open slots \
                 to drain; a slot claimed by a create still in flight settles within \
                 minutes, when the create finishes or the stale claim is swept.",
            );
        }
        if pools.sandbox_held_slots > 0 {
            message.push_str(
                " A slot held by a sandbox clears when that sandbox's pod is retired, for \
                 example when the sandbox is deleted or suspended.",
            );
        }
        message.push_str(" Then delete the template again.");
        message
    }

    /// The refusal as structured details, shared by the route's 409 and the
    /// operation's terminal error. A use that is absent has no key.
    pub fn details(&self) -> Vec<PublicErrorDetail> {
        let pools = &self.pools;
        let mut details = vec![detail("reason", "template_in_use")];
        if !pools.enabled_configs.is_empty() {
            let ids = pools
                .enabled_configs
                .iter()
                .map(|config| config.id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            details.push(detail("enabled_pool_config_ids", ids));
        }
        if !pools.enabled_policies.is_empty() {
            details.push(detail("enabled_pool_policies", policy_shapes(pools)));
        }
        if pools.open_slots > 0 {
            details.push(detail("open_pool_slots", pools.open_slots.to_string()));
        }
        if pools.sandbox_held_slots > 0 {
            details.push(detail(
                "sandbox_held_pool_slots",
                pools.sandbox_held_slots.to_string(),
            ));
            let ids = pools
                .sandbox_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",");
            details.push(detail("pool_slot_sandbox_ids", ids));
        }
        details
    }
}

fn detail(key: &str, value: impl Into<String>) -> PublicErrorDetail {
    PublicErrorDetail {
        key: key.to_string(),
        value: value.into(),
    }
}

fn policy_shapes(pools: &TemplateWarmPoolUsage) -> String {
    pools
        .enabled_policies
        .iter()
        .map(|policy| format!("{} vCPU/{} MiB", policy.vcpu, policy.memory_mib))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sandbox_ids(pools: &TemplateWarmPoolUsage) -> String {
    let noun = if pools.sandbox_ids.len() == 1 {
        "sandbox"
    } else {
        "sandboxes"
    };
    let ids = pools
        .sandbox_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!("{noun} {ids}")
}

fn count_noun(count: i64, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// [`TemplateInUse::check`] on a pooled connection.
pub async fn template_in_use(
    pool: &PgPool,
    template: &TemplateName,
) -> Result<Option<TemplateInUse>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    TemplateInUse::check(&mut conn, template).await
}

async fn fetch_template_desired_state(
    pool: &PgPool,
    template: &TemplateName,
) -> Result<Option<TemplateDesiredState>, sqlx::Error> {
    let row = sqlx::query(sql!("SELECT desired_state FROM templates WHERE name = $1"))
        .bind(template.as_str())
        .fetch_optional(pool)
        .await?;
    Ok(row.and_then(|row| {
        row.try_get::<String, _>("desired_state")
            .ok()
            .and_then(|state| state.parse().ok())
    }))
}

async fn mark_template_deleting(
    pool: &PgPool,
    template: &TemplateName,
    operation: &ClaimedPublicOperation,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE templates
         SET desired_state = $2,
             delete_requested_at = COALESCE(delete_requested_at, NOW()),
             delete_requested_by = COALESCE(delete_requested_by, $3),
             delete_operation_id = COALESCE(delete_operation_id, $4),
             updated_at = NOW()
         WHERE name = $1"
    ))
    .bind(template.as_str())
    .bind(TemplateDesiredState::Deleting.as_str())
    .bind(operation.actor_user_id)
    .bind(operation.id.as_uuid())
    .execute(pool)
    .await?;
    Ok(())
}

async fn nodes_with_template_inventory(
    pool: &PgPool,
    template: &TemplateName,
) -> Result<Vec<NodeId>, sqlx::Error> {
    sqlx::query_scalar::<_, NodeId>(sql!(
        "SELECT DISTINCT nti.node_id
         FROM node_template_inventory nti
         JOIN nodes n ON n.id = nti.node_id
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         WHERE nti.template_name = $1
           AND (
             n.managed_by != $2
             OR (nd.node_id IS NOT NULL AND nd.deleted_at IS NULL)
           )"
    ))
    .bind(template.as_str())
    .bind(SubstrateKind::Space)
    .fetch_all(pool)
    .await
}

async fn enqueue_template_delete_reconcile(
    pool: &PgPool,
    template: &TemplateName,
    node_ids: &[NodeId],
) -> Result<(), sqlx::Error> {
    let refs = [ReconcileRef::template_desired_state(template.clone())];
    enqueue_node_reconcile_refs_for_nodes(pool, node_ids.iter().copied(), &refs).await?;
    enqueue_triggers(
        pool,
        node_ids.iter().copied(),
        pplx_space_common_svc::types::ReconcileReason::TemplateDesiredStateUpdated,
    )
    .await?;
    Ok(())
}

enum MarkTemplateDeleted {
    Deleted,
    InUse(TemplateInUse),
}

async fn mark_template_deleted(
    pool: &PgPool,
    template: &TemplateName,
    operation_id: Uuid,
) -> Result<MarkTemplateDeleted, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if let Some(in_use) = TemplateInUse::check(&mut tx, template).await? {
        return Ok(MarkTemplateDeleted::InUse(in_use));
    }
    let retired = WarmPoolStore::delete_disabled_for_template(&mut tx, template).await?;
    if retired != Default::default() {
        tracing::info!(
            template = template.as_str(),
            pool_configs = retired.configs,
            pool_policies = retired.policies,
            "template delete removed disabled warm-pool rows"
        );
    }
    let cleanup_refs = [
        ReconcileElementRef::network_policy(NetworkPolicyTarget::template(template.clone())),
        ReconcileElementRef::template_inventory(template.clone()),
        ReconcileElementRef::template_desired_state(template.clone()),
        ReconcileElementRef::template_base_snapshot_policy(template.clone()),
    ];
    delete_reconcile_state_for_elements(&mut tx, &cleanup_refs).await?;
    sqlx::query(sql!(
        "DELETE FROM template_release_storage_locations
         WHERE template_release_id IN (
           SELECT id FROM template_releases WHERE template_name = $1
         )"
    ))
    .bind(template.as_str())
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_release_tags WHERE template_name = $1"
    ))
    .bind(template.as_str())
    .execute(&mut *tx)
    .await?;
    let releases = sqlx::query(sql!(
        "DELETE FROM template_releases WHERE template_name = $1"
    ))
    .bind(template.as_str())
    .execute(&mut *tx)
    .await;
    if let Err(error) = releases {
        // A reference that committed after the check above. A new slot or
        // enabled config is reported, rather than retrying a delete that
        // cannot succeed until it clears. Anything the re-read does not
        // count, such as a disabled config written after the cleanup's lock,
        // is deleted by the cleanup on a retry, so the original error goes
        // back to the dispatcher, which requeues it within its budget.
        if error
            .as_database_error()
            .is_some_and(|db| db.is_foreign_key_violation())
        {
            tx.rollback().await?;
            if let Some(in_use) = template_in_use(pool, template).await? {
                return Ok(MarkTemplateDeleted::InUse(in_use));
            }
        }
        return Err(error);
    }
    sqlx::query(sql!(
        "DELETE FROM template_pool_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_default_auto_lifecycle_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_base_snapshot_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!(
        "UPDATE templates
         SET desired_state = $2,
             delete_operation_id = COALESCE(delete_operation_id, $3),
             updated_at = NOW()
         WHERE name = $1"
    ))
    .bind(template.as_str())
    .bind(TemplateDesiredState::Deleted.as_str())
    .bind(operation_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(MarkTemplateDeleted::Deleted)
}

fn terminal_failure(
    operation: &ClaimedPublicOperation,
    code: PublicErrorCode,
    message: impl Into<String>,
    error_json: serde_json::Value,
) -> PublicOperationExecutionResult {
    let message = message.into();
    tracing::warn!(
        operation_id = %operation.id,
        kind = %operation.kind,
        message,
        "delete template operation failed"
    );
    PublicOperationExecutionResult {
        state: OperationState::Failed,
        progress_message: message,
        result_json: None,
        terminal_error_code: Some(code),
        terminal_error_json: Some(error_json),
        retry_after: None,
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}

fn internal_failure(
    operation: &ClaimedPublicOperation,
    error: sqlx::Error,
) -> PublicOperationExecutionResult {
    tracing::warn!(
        operation_id = %operation.id,
        %error,
        "delete template operation hit a transient database error"
    );
    PublicOperationExecutionResult {
        state: OperationState::Queued,
        progress_message: "template delete is waiting for reconciliation".to_string(),
        result_json: None,
        terminal_error_code: None,
        terminal_error_json: None,
        retry_after: Some(TEMPLATE_DELETE_RETRY_AFTER),
        requeue_reason: PublicOperationRequeueReason::TransientFailure,
    }
}

#[cfg(test)]
pub(crate) mod tests;
