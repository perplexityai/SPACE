use super::{NetworkPolicyV1, TemplateRow};
use pplx_space_common_core::{
    sql,
    types::{OrgId, TemplateDesiredState, TemplateId, TemplateVisibility},
};
use sqlx::{PgExecutor, types::Json};

pub(super) async fn insert(
    executor: impl PgExecutor<'_>,
    name: &str,
    description: Option<&str>,
    owner: Option<OrgId>,
) -> Result<Option<TemplateRow>, sqlx::Error> {
    // Launch scope follows ownership: a tenant's registration is private to
    // that org, while an ownerless system publication is usable everywhere.
    // The build API advertises exactly this (`public: false` for owned
    // builds); widening an owned template is an operator action through the
    // admin visibility route, never a registration default.
    let visibility = if owner.is_some() {
        TemplateVisibility::OrgSpecific
    } else {
        TemplateVisibility::Generic
    };
    // Both the name and the name/owner constraints can conflict concurrently.
    // The caller rechecks the winning row under a lock in a fresh statement.
    sqlx::query_as(sql!(
        "INSERT INTO templates
            (id, name, description, org_id, visibility, network_policy_json, network_policy_generation, desired_state)
         VALUES ($1, $2, $3, $4, $7, $5, 1, $6)
         ON CONFLICT DO NOTHING
         RETURNING name, description, created_at, network_policy_json, network_policy_generation"
    ))
    .bind(TemplateId::new().as_uuid()).bind(name).bind(description).bind(owner)
    .bind(Json(NetworkPolicyV1::deny_all())).bind(TemplateDesiredState::Present.as_str())
    .bind(visibility.as_str())
    .fetch_optional(executor).await
}

pub(super) async fn fill_description(
    executor: impl PgExecutor<'_>,
    name: &str,
    description: Option<&str>,
) -> Result<TemplateRow, sqlx::Error> {
    sqlx::query_as(sql!(
        "UPDATE templates SET description = COALESCE(description, $2) WHERE name = $1
         RETURNING name, description, created_at, network_policy_json, network_policy_generation"
    ))
    .bind(name)
    .bind(description)
    .fetch_one(executor)
    .await
}
