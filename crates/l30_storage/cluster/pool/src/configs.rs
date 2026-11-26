//! Reads of `kubernetes_pool_configs` that compose into a caller's
//! transaction.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// The template whose release pool config `config_id` belongs to; `None`
/// when there's no such config. Takes no lock.
pub async fn pool_config_template_name(
    executor: impl Executor<'_>,
    config_id: Uuid,
) -> Result<Option<String>, DbError> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "SELECT r.template_name
         FROM kubernetes_pool_configs c
         JOIN template_releases r ON r.id = c.template_release_id
         WHERE c.id = $1"
    ))
    .bind(config_id)
    .fetch_optional(executor.into_sqlx())
    .await?)
}
