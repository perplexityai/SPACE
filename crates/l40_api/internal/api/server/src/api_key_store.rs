//! API-key authorization for the ECW data-plane resolver.

use pplx_space_common_core::{sql, types::OrgId};

pub(super) async fn authorizes_ecw_sandbox(
    executor: impl sqlx::PgExecutor<'_>,
    key_hash: &str,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM api_keys
             WHERE org_id = $1 AND key_hash = $2
               AND 'ecw' = ANY(allowed_surfaces)
         )"
    ))
    .bind(org_id)
    .bind(key_hash)
    .fetch_one(executor)
    .await
}
