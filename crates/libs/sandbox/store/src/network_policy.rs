//! Desired-policy CAS and durable replacement receipts. Callers own transactions.
use pplx_space_common_core::{sql, types::SandboxId};
use sqlx::{PgExecutor, types::JsonValue};

use crate::SandboxStoreError;

pub async fn lock(executor: impl PgExecutor<'_>, id: SandboxId) -> Result<bool, SandboxStoreError> {
    Ok(sqlx::query_scalar::<_, SandboxId>(sql!(
        "SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"
    ))
    .bind(id)
    .fetch_optional(executor)
    .await?
    .is_some())
}

pub async fn set(
    executor: impl PgExecutor<'_>,
    id: SandboxId,
    policy: Option<JsonValue>,
    expected: Option<i64>,
) -> Result<Option<i64>, SandboxStoreError> {
    Ok(sqlx::query_scalar(sql!(
        "UPDATE sandboxes
         SET network_policy_override_json = $1,
             network_policy_generation = CASE
                 WHEN network_policy_override_json IS DISTINCT FROM $1
                 THEN network_policy_generation + 1 ELSE network_policy_generation END
         WHERE id = $2 AND ($3::BIGINT IS NULL OR network_policy_generation = $3)
         RETURNING network_policy_generation"
    ))
    .bind(policy)
    .bind(id)
    .bind(expected)
    .fetch_optional(executor)
    .await?)
}

#[derive(sqlx::FromRow)]
pub struct Receipt {
    pub request_hash: String,
    pub target_ref: String,
}

pub async fn receipt(
    executor: impl PgExecutor<'_>,
    id: SandboxId,
    idempotency_digest: &str,
) -> Result<Option<Receipt>, SandboxStoreError> {
    Ok(sqlx::query_as(sql!(
        "SELECT request_hash, target_ref FROM sandbox_network_policy_receipts
         WHERE sandbox_id = $1 AND idempotency_digest = $2"
    ))
    .bind(id)
    .bind(idempotency_digest)
    .fetch_optional(executor)
    .await?)
}

pub async fn record_receipt(
    executor: impl PgExecutor<'_>,
    id: SandboxId,
    idempotency_digest: &str,
    receipt: &Receipt,
) -> Result<(), SandboxStoreError> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_network_policy_receipts (sandbox_id, idempotency_digest, request_hash, target_ref)
         VALUES ($1, $2, $3, $4)"
    )).bind(id).bind(idempotency_digest).bind(&receipt.request_hash).bind(&receipt.target_ref)
        .execute(executor).await?;
    Ok(())
}
