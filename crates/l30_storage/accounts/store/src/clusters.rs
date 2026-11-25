//! The singleton `clusters` row: the cluster's managing organization.

use pplx_space_accounts::OrgId;
use pplx_space_sql_locator::sql;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Cluster {
    pub id: Uuid,
    pub name: String,
    pub managing_org_id: OrgId,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// The cluster row, or `None` before one is seeded.
pub async fn get_cluster(executor: impl PgExecutor<'_>) -> Result<Option<Cluster>, sqlx::Error> {
    sqlx::query_as::<_, Cluster>(sql!(
        "SELECT id, name, managing_org_id, created_at, updated_at
         FROM clusters WHERE singleton"
    ))
    .fetch_optional(executor)
    .await
}

/// The organization that manages the cluster. A missing cluster row is
/// `RowNotFound`.
pub async fn cluster_managing_org_id(executor: impl PgExecutor<'_>) -> Result<OrgId, sqlx::Error> {
    sqlx::query_scalar::<_, OrgId>(sql!("SELECT managing_org_id FROM clusters WHERE singleton"))
        .fetch_one(executor)
        .await
}

/// Make `managing_org_id` the cluster's managing organization; `None` when
/// there is no cluster row.
pub async fn set_cluster_managing_org(
    executor: impl PgExecutor<'_>,
    managing_org_id: OrgId,
) -> Result<Option<Cluster>, sqlx::Error> {
    sqlx::query_as::<_, Cluster>(sql!(
        "UPDATE clusters
         SET managing_org_id = $1, updated_at = NOW()
         WHERE singleton
         RETURNING id, name, managing_org_id, created_at, updated_at"
    ))
    .bind(managing_org_id)
    .fetch_optional(executor)
    .await
}
