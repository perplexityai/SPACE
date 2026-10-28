use pplx_space_common_core::types::NodeId;

use pplx_space_common_core::sql;
use std::time::Duration;

use pplx_space_common_core::types::{TemplateDesiredState, TemplateName};
use pplx_space_public_api::{
    OperationState, PublicErrorCode, PublicOperationResult, PublicTemplateOperationResult,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};

use crate::template_inventory_store::nodes_with_template_inventory;

use crate::{
    public_operations::{
        ClaimedPublicOperation, PublicOperationRequeueReason, PublicOperationStore,
        PublicOperationStoreError,
    },
    public_sandbox_operations::PublicOperationExecutionResult,
    reconcile_queue::{
        NetworkPolicyTarget, ReconcileElementRef, ReconcileRef,
        delete_reconcile_state_for_elements, enqueue_node_reconcile_refs_for_nodes_tx,
    },
    reconcile_trigger::enqueue_triggers_tx,
};

const TEMPLATE_DELETE_RETRY_AFTER: Duration = Duration::from_secs(5);
// Admin deletes have no driver age deadline. Bound cleanup independently of
// failed attempts so a retained node copy cannot keep the operation active forever.
const TEMPLATE_DELETE_WAIT_LIMIT: Duration = Duration::from_secs(30 * 60);

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
    ) -> Result<PublicOperationExecutionResult, PublicOperationStoreError> {
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

        // Read the operation clock before holding the template transaction's connection.
        let waited = PublicOperationStore::new(self.pool.clone())
            .time_since_first_claim(operation.id)
            .await?
            .unwrap_or_default();

        // Admission records intent atomically. Keep its row locked through all
        // cleanup so a retired claim cannot delete a same-name replacement.
        let mut tx = self.pool.begin().await?;
        let state = sqlx::query_as::<_, (String, Option<uuid::Uuid>)>(sql!(
            "SELECT desired_state, delete_operation_id FROM templates
             WHERE name = $1 FOR UPDATE"
        ))
        .bind(template.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        let Some((desired_state, delete_operation_id)) = state else {
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
        if delete_operation_id != Some(operation.id.as_uuid())
            || (desired_state != TemplateDesiredState::Deleting.as_str()
                && desired_state != TemplateDesiredState::Deleted.as_str())
        {
            return Ok(terminal_failure(
                operation,
                PublicErrorCode::Conflict,
                "template delete operation was superseded",
                serde_json::json!({
                    "operation_id": operation.id.to_string(),
                    "template": template.to_string(),
                    "reason": "template_delete_superseded",
                }),
            ));
        }

        if desired_state == TemplateDesiredState::Deleting.as_str() {
            let nodes = nodes_with_template_inventory(&mut *tx, &template).await?;
            if !nodes.is_empty() {
                if waited >= TEMPLATE_DELETE_WAIT_LIMIT {
                    return Ok(terminal_failure(
                        operation,
                        PublicErrorCode::Conflict,
                        "template deletion timed out waiting for node cleanup",
                        serde_json::json!({
                            "operation_id": operation.id.to_string(),
                            "template": template.to_string(),
                            "reason": "template_delete_cleanup_timeout",
                            "blocking_nodes": nodes,
                            "waited_secs": waited.as_secs(),
                        }),
                    ));
                }
                enqueue_template_delete_reconcile(&mut tx, &template, &nodes).await?;
                tx.commit().await?;
                return Ok(PublicOperationExecutionResult::workflow_progress(
                    format!("waiting for {} node(s) to delete template", nodes.len()),
                    crate::public_operation_driver::deferred_retry_delay(
                        operation.id,
                        waited,
                        TEMPLATE_DELETE_RETRY_AFTER,
                    ),
                ));
            }
            mark_template_deleted(&mut tx, &template).await?;
        }
        tx.commit().await?;
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

async fn enqueue_template_delete_reconcile(
    tx: &mut Transaction<'_, Postgres>,
    template: &TemplateName,
    node_ids: &[NodeId],
) -> Result<(), sqlx::Error> {
    let refs = [ReconcileRef::template_desired_state(template.clone())];
    enqueue_node_reconcile_refs_for_nodes_tx(tx, node_ids.iter().copied(), &refs).await?;
    enqueue_triggers_tx(
        tx,
        node_ids.iter().copied(),
        pplx_space_common_svc::types::ReconcileReason::TemplateDesiredStateUpdated,
    )
    .await?;
    Ok(())
}

async fn mark_template_deleted(
    tx: &mut Transaction<'_, Postgres>,
    template: &TemplateName,
) -> Result<(), sqlx::Error> {
    let cleanup_refs = [
        ReconcileElementRef::network_policy(NetworkPolicyTarget::template(template.clone())),
        ReconcileElementRef::template_inventory(template.clone()),
        ReconcileElementRef::template_desired_state(template.clone()),
        ReconcileElementRef::template_base_snapshot_policy(template.clone()),
    ];
    delete_reconcile_state_for_elements(tx, &cleanup_refs).await?;
    sqlx::query(sql!(
        "DELETE FROM template_release_storage_locations
         WHERE template_release_id IN (
           SELECT id FROM template_releases WHERE template_name = $1
         )"
    ))
    .bind(template.as_str())
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_release_tags WHERE template_name = $1"
    ))
    .bind(template.as_str())
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_releases WHERE template_name = $1"
    ))
    .bind(template.as_str())
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_pool_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_default_auto_lifecycle_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "DELETE FROM template_base_snapshot_policies WHERE template = $1"
    ))
    .bind(template.as_str())
    .execute(&mut **tx)
    .await?;
    sqlx::query(sql!(
        "UPDATE templates
         SET desired_state = $2,
             updated_at = NOW()
         WHERE name = $1"
    ))
    .bind(template.as_str())
    .bind(TemplateDesiredState::Deleted.as_str())
    .execute(&mut **tx)
    .await?;
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
    error: PublicOperationStoreError,
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
