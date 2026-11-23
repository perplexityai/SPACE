//! The `templates` catalog row.

use pplx_space_accounts::{OrgId, UserId};
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_sql_locator::sql;
use pplx_space_template_model::{
    TemplateDesiredState, TemplateId, TemplateName, TemplateVisibility,
};
use sqlx::{FromRow, PgExecutor, types::Json};
use time::OffsetDateTime;
use uuid::Uuid;

/// The stored `desired_state` of `template`, unparsed, or `None` when no row
/// names it. Callers parse it, as each treats an unknown value its own way.
pub async fn desired_state(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(sql!("SELECT desired_state FROM templates WHERE name = $1"))
        .bind(template.as_str())
        .fetch_optional(executor)
        .await
}

/// The fields a sandbox create authorizes a template by.
#[derive(Debug, FromRow)]
pub struct TemplateAccess {
    /// The stored visibility, unparsed.
    pub visibility: String,
    pub org_id: Option<Uuid>,
    /// The stored desired state, unparsed.
    pub desired_state: String,
}

/// `template`'s access fields, whatever its state and owner, or `None` when
/// no row names it. The caller decides access from them.
pub async fn access(
    executor: impl PgExecutor<'_>,
    template: &str,
) -> Result<Option<TemplateAccess>, sqlx::Error> {
    sqlx::query_as::<_, TemplateAccess>(sql!(
        "SELECT visibility, org_id, desired_state
         FROM templates
         WHERE name = $1"
    ))
    .bind(template)
    .fetch_optional(executor)
    .await
}

/// A present template's network policy and its generation.
#[derive(Debug, FromRow)]
pub struct PresentNetworkPolicy {
    pub network_policy_json: Json<NetworkPolicyV1>,
    pub network_policy_generation: i64,
}

/// `template`'s network policy and generation, or `None` when no present
/// row names it.
pub async fn present_network_policy(
    executor: impl PgExecutor<'_>,
    template: &str,
) -> Result<Option<PresentNetworkPolicy>, sqlx::Error> {
    sqlx::query_as::<_, PresentNetworkPolicy>(sql!(
        "SELECT network_policy_json, network_policy_generation
         FROM templates
         WHERE name = $1
           AND desired_state = $2"
    ))
    .bind(template)
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_optional(executor)
    .await
}

/// A template's visibility, read with the database's clock.
#[derive(Debug, FromRow)]
pub struct ObservedVisibility {
    /// The database's `clock_timestamp()` when the statement read the row.
    pub observed_at: OffsetDateTime,
    /// The stored visibility, unparsed, or `None` when no row names the
    /// template.
    pub visibility: Option<String>,
}

/// `template`'s visibility and the database instant it was read at.
///
/// Shaped as a scalar subselect rather than `SELECT visibility ... WHERE
/// name = $1` so it returns exactly one row whether or not the template
/// exists, which is what lets one query carry both the value and the
/// server-side instant a cache orders writes on. `visibility` is `NOT NULL`,
/// so a `None` unambiguously means "no such row" rather than "a row with no
/// value", and `templates.name` is unique (`templates_name_key`), so the
/// subselect cannot return more than one row.
pub async fn observed_visibility(
    executor: impl PgExecutor<'_>,
    template: &str,
) -> Result<ObservedVisibility, sqlx::Error> {
    sqlx::query_as::<_, ObservedVisibility>(sql!(
        "SELECT clock_timestamp() AS observed_at,
                (SELECT visibility FROM templates WHERE name = $1) AS visibility"
    ))
    .bind(template)
    .fetch_one(executor)
    .await
}

/// Whether a row named `template` exists and is not deleted: the rows the
/// template read routes list.
pub async fn exists_not_deleted(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM templates WHERE name = $1 AND desired_state <> $2)"
    ))
    .bind(template.as_str())
    .bind(TemplateDesiredState::Deleted.as_str())
    .fetch_one(executor)
    .await
}

/// Whether no organization other than `org_id` owns `template`: true for an
/// ownerless template, one `org_id` owns, and a name no row has yet.
pub async fn not_owned_by_another_org(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    org_id: OrgId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT NOT EXISTS (SELECT 1 FROM templates WHERE name = $1 AND org_id <> $2)"
    ))
    .bind(template.as_str())
    .bind(org_id.as_uuid())
    .fetch_one(executor)
    .await
}

/// The network policy stored for `template`, or `None` when no row names it.
pub async fn network_policy(
    executor: impl PgExecutor<'_>,
    template: &str,
) -> Result<Option<NetworkPolicyV1>, sqlx::Error> {
    let policy = sqlx::query_scalar::<_, Option<Json<NetworkPolicyV1>>>(sql!(
        "SELECT network_policy_json
             FROM templates
             WHERE name = $1"
    ))
    .bind(template)
    .fetch_optional(executor)
    .await?;
    Ok(policy.flatten().map(|policy| policy.0))
}

/// The number of deleted template rows that tombstone retention still
/// holds.
pub async fn count_deleted(executor: impl PgExecutor<'_>) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
               FROM templates
              WHERE desired_state = 'deleted'"
    ))
    .fetch_one(executor)
    .await
}

/// A present template an organization may launch, as the public template
/// routes list it.
#[derive(Debug, FromRow)]
pub struct VisibleTemplate {
    pub name: String,
    pub description: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// Up to `limit` present templates that are generic or owned by `org_id`,
/// by name, after `after` when given.
pub async fn list_visible(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    after: Option<&str>,
    limit: i64,
) -> Result<Vec<VisibleTemplate>, sqlx::Error> {
    sqlx::query_as::<_, VisibleTemplate>(sql!(
        "SELECT name, description, created_at, updated_at
         FROM templates
         WHERE (visibility = $1 OR org_id = $2)
           AND desired_state = 'present'
           AND ($3::TEXT IS NULL OR name > $3)
         ORDER BY name ASC
         LIMIT $4"
    ))
    .bind(TemplateVisibility::Generic.as_str())
    .bind(org_id)
    .bind(after)
    .bind(limit)
    .fetch_all(executor)
    .await
}

/// The present template `template`, when it is generic or owned by
/// `org_id`.
pub async fn get_visible(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    template: &str,
) -> Result<Option<VisibleTemplate>, sqlx::Error> {
    sqlx::query_as::<_, VisibleTemplate>(sql!(
        "SELECT name, description, created_at, updated_at
         FROM templates
         WHERE name = $1
           AND desired_state = 'present'
           AND (visibility = $2 OR org_id = $3)"
    ))
    .bind(template)
    .bind(TemplateVisibility::Generic.as_str())
    .bind(org_id)
    .fetch_optional(executor)
    .await
}

/// Write `visibility`, assigning `new_org_id` as the owner in the same
/// statement when it is given: the `org_specific_requires_owner` CHECK
/// demands owner and visibility move together for ownerless templates.
pub async fn update_visibility(
    executor: impl PgExecutor<'_>,
    template: &str,
    visibility: TemplateVisibility,
    new_org_id: Option<OrgId>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE templates
            SET visibility = $2,
                org_id = COALESCE($3, org_id)
          WHERE name = $1"
    ))
    .bind(template)
    .bind(visibility)
    .bind(new_org_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// A template row as release registration reads it back.
#[derive(Debug, FromRow)]
pub struct RegisteredTemplate {
    pub name: String,
    pub description: Option<String>,
    pub created_at: OffsetDateTime,
    pub network_policy_json: Json<NetworkPolicyV1>,
    pub network_policy_generation: i64,
}

/// A template row for release registration to insert, at network policy
/// generation 1.
#[derive(Clone, Copy, Debug)]
pub struct NewTemplate<'a> {
    pub id: TemplateId,
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub org_id: Option<OrgId>,
    pub visibility: TemplateVisibility,
    pub network_policy: &'a NetworkPolicyV1,
    pub desired_state: TemplateDesiredState,
}

/// Insert `template`, or `None` when its name or its name and owner
/// conflict with an existing row, which may have committed concurrently.
pub async fn insert_if_absent(
    executor: impl PgExecutor<'_>,
    template: NewTemplate<'_>,
) -> Result<Option<RegisteredTemplate>, sqlx::Error> {
    sqlx::query_as(sql!(
        "INSERT INTO templates
            (id, name, description, org_id, visibility, network_policy_json, network_policy_generation, desired_state)
         VALUES ($1, $2, $3, $4, $7, $5, 1, $6)
         ON CONFLICT DO NOTHING
         RETURNING name, description, created_at, network_policy_json, network_policy_generation"
    ))
    .bind(template.id.as_uuid())
    .bind(template.name)
    .bind(template.description)
    .bind(template.org_id)
    .bind(Json(template.network_policy))
    .bind(template.desired_state.as_str())
    .bind(template.visibility.as_str())
    .fetch_optional(executor)
    .await
}

/// Set `template`'s description to `description` when it has none, and
/// read the row back. Fails with [`sqlx::Error::RowNotFound`] when no row
/// names it.
pub async fn fill_description(
    executor: impl PgExecutor<'_>,
    template: &str,
    description: Option<&str>,
) -> Result<RegisteredTemplate, sqlx::Error> {
    sqlx::query_as(sql!(
        "UPDATE templates SET description = COALESCE(description, $2) WHERE name = $1
         RETURNING name, description, created_at, network_policy_json, network_policy_generation"
    ))
    .bind(template)
    .bind(description)
    .fetch_one(executor)
    .await
}

/// Record that `template` is being deleted by operation `operation_id`,
/// keeping the request time, actor and operation an earlier call already
/// recorded.
pub async fn mark_deleting(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    actor_user_id: Option<UserId>,
    operation_id: Uuid,
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
    .bind(actor_user_id)
    .bind(operation_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Record that `template` is deleted, keeping the operation a delete
/// request already recorded.
pub async fn mark_deleted(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    operation_id: Uuid,
) -> Result<(), sqlx::Error> {
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
    .execute(executor)
    .await?;
    Ok(())
}
