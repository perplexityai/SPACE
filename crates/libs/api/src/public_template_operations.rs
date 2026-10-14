use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_common_core::types::{SubstrateKind, TemplateDesiredState, TemplateName};
use pplx_space_public_api::{
    OperationState, PublicErrorCode, PublicOperationResult, PublicTemplateOperationResult,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
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

        mark_template_deleted(&self.pool, &template, operation.id.as_uuid()).await?;
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

async fn mark_template_deleted(
    pool: &PgPool,
    template: &TemplateName,
    operation_id: Uuid,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
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
    sqlx::query(sql!(
        "DELETE FROM template_releases WHERE template_name = $1"
    ))
    .bind(template.as_str())
    .execute(&mut *tx)
    .await?;
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
    Ok(())
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
mod tests;
