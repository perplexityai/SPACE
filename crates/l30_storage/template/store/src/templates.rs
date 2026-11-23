//! The `templates` catalog row.

use pplx_space_accounts::{OrgId, UserId};
use std::time::Duration;

use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::ReconcileFacet;
use pplx_space_template_model::{
    TemplateDesiredState, TemplateId, TemplateName, TemplateVisibility,
};
use serde::de::DeserializeOwned;
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

/// The owner of `template`: `None` when no row names it, `Some(None)` for an
/// ownerless template.
pub async fn owner(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<Option<Option<OrgId>>, sqlx::Error> {
    let owner: Option<Option<Uuid>> =
        sqlx::query_scalar(sql!("SELECT org_id FROM templates WHERE name = $1"))
            .bind(template.as_str())
            .fetch_optional(executor)
            .await?;
    Ok(owner.map(|organization_id| organization_id.map(OrgId::from_uuid)))
}

/// A template's desired state and the delete operation it records.
#[derive(Debug)]
pub struct DeleteState {
    /// The stored desired state, unparsed.
    pub desired_state: String,
    pub delete_operation_id: Option<Uuid>,
    /// Whether the recorded delete operation is still queued or running:
    /// `false` when it reached a terminal state or was garbage-collected, or
    /// none is recorded.
    pub delete_operation_live: bool,
}

/// `template`'s delete state, or `None` when no row names it.
pub async fn delete_state(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
) -> Result<Option<DeleteState>, sqlx::Error> {
    let row = sqlx::query_as::<_, (String, Option<Uuid>, bool)>(sql!(
        "SELECT t.desired_state,
                t.delete_operation_id,
                COALESCE(po.state IN ('queued', 'running'), FALSE)
         FROM templates t
         LEFT JOIN public_operations po ON po.operation_id = t.delete_operation_id
         WHERE t.name = $1"
    ))
    .bind(template.as_str())
    .fetch_optional(executor)
    .await?;
    Ok(row.map(
        |(desired_state, delete_operation_id, delete_operation_live)| DeleteState {
            desired_state,
            delete_operation_id,
            delete_operation_live,
        },
    ))
}

/// What an organization's template rows are, for deleting the organization.
#[derive(Debug)]
pub struct OrgTemplates {
    /// Whether any of them is not deleted; `None` when it owns none.
    pub any_live: Option<bool>,
    /// Whether a template release or a node's inventory row names any of
    /// them; `None` when it owns none.
    pub any_held: Option<bool>,
    /// `retention` after the latest of their delete requests (or, without
    /// one, their last update or creation); `None` when it owns none.
    pub retained_until: Option<OffsetDateTime>,
}

/// Summarize `org_id`'s template rows; see [`OrgTemplates`].
pub async fn org_templates(
    executor: impl PgExecutor<'_>,
    org_id: OrgId,
    retention: Duration,
) -> Result<OrgTemplates, sqlx::Error> {
    let (any_live, any_held, retained_until) =
        sqlx::query_as::<_, (Option<bool>, Option<bool>, Option<OffsetDateTime>)>(sql!(
            "SELECT bool_or(t.desired_state <> $2),
                    bool_or(
                        EXISTS (
                            SELECT 1 FROM template_releases r WHERE r.template_name = t.name
                        )
                        OR EXISTS (
                            SELECT 1
                              FROM node_template_inventory i
                             WHERE i.template_name = t.name
                        )
                    ),
                    MAX(COALESCE(t.delete_requested_at, t.updated_at, t.created_at))
                        + make_interval(secs => $3)
               FROM templates t
              WHERE t.org_id = $1"
        ))
        .bind(org_id)
        .bind(TemplateDesiredState::Deleted.as_str())
        .bind(retention.as_secs_f64())
        .fetch_one(executor)
        .await?;
    Ok(OrgTemplates {
        any_live,
        any_held,
        retained_until,
    })
}

/// A template as the admin template routes list it: its row, its policies,
/// what the builder and the other nodes report of its build, and the
/// network policy generation nodes observe. `Snapshots` decodes the
/// inventory's `snapshots` JSON.
///
/// The api's admin template create also decodes its upsert's `RETURNING`
/// row into this type, so a field added here needs a column in that
/// `RETURNING` list too.
#[derive(Debug, FromRow)]
pub struct AdminTemplate<Snapshots> {
    pub name: String,
    pub description: Option<String>,
    pub org_id: Option<OrgId>,
    pub desired_state: String,
    pub network_policy_generation: i64,
    pub created_at: OffsetDateTime,
    pub built_at: Option<String>,
    pub content_hash: Option<String>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub arch: Option<String>,
    pub has_base_snapshot: Option<bool>,
    pub pool_target: Option<i32>,
    pub pool_current: Option<i32>,
    pub artifact_size_bytes: Option<i64>,
    pub cache_size_bytes: Option<i64>,
    pub base_snapshot_size_bytes: Option<i64>,
    pub snapshots: Option<Json<Snapshots>>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_size_mb: Option<i32>,
    pub default_auto_lifecycle: Option<Json<AutoLifecyclePolicy>>,
    pub network_policy: Json<NetworkPolicyV1>,
    pub observed_network_policy_generation: Option<i64>,
    pub observed_network_policy_authority: Option<String>,
}

/// Every template that is not deleted, or only `template` when given, by
/// name. A template without a stored network policy lists deny-all.
pub async fn list_admin<Snapshots>(
    executor: impl PgExecutor<'_>,
    template: Option<&TemplateName>,
) -> Result<Vec<AdminTemplate<Snapshots>>, sqlx::Error>
where
    Snapshots: DeserializeOwned + Send + Unpin + 'static,
{
    sqlx::query_as::<_, AdminTemplate<Snapshots>>(
         sql!("WITH template_names AS (
             SELECT name
             FROM templates
             WHERE desired_state <> $4
               AND ($3::TEXT IS NULL OR name = $3)
         ),
         builder_metadata AS (
             SELECT DISTINCT ON (nti.template_name)
                    nti.*
             FROM node_template_inventory nti
             JOIN nodes n ON n.id = nti.node_id
             WHERE n.role = 'builder'
             ORDER BY nti.template_name, nti.observed_at DESC, nti.built_at DESC NULLS LAST, nti.content_hash ASC
         ),
         availability AS (
             SELECT DISTINCT ON (nti.template_name)
                    nti.*
             FROM node_template_inventory nti
             LEFT JOIN builder_metadata bm ON bm.template_name = nti.template_name
             WHERE NULLIF(bm.content_hash, '') IS NULL
                OR nti.content_hash = bm.content_hash
             ORDER BY nti.template_name,
                      nti.has_base_snapshot DESC,
                      nti.observed_at DESC,
                      nti.content_hash ASC
         ),
         pools AS (
             SELECT nti.template_name,
                    SUM(nti.pool_target) FILTER (WHERE nti.pool_target IS NOT NULL)::INTEGER AS pool_target,
                    SUM(nti.pool_current)::INTEGER AS pool_current
             FROM node_template_inventory nti
             LEFT JOIN builder_metadata bm ON bm.template_name = nti.template_name
             WHERE NULLIF(bm.content_hash, '') IS NULL
                OR nti.content_hash = bm.content_hash
             GROUP BY nti.template_name
         ),
         network_policy_status AS (
             SELECT substring(element from 10) AS template_name,
                    MAX(observed_generation) AS observed_network_policy_generation,
                    CASE
                        WHEN BOOL_OR(applied_authority = 'local') THEN 'local'
                        WHEN BOOL_OR(applied_authority = 'cluster') THEN 'cluster'
                        ELSE NULL
                    END AS observed_network_policy_authority
             FROM node_facet_observed_status
             WHERE facet = $2
               AND element LIKE 'template:%'
             GROUP BY substring(element from 10)
         )
         SELECT names.name,
                t.description,
                t.org_id,
                t.desired_state,
                t.network_policy_generation,
                COALESCE(t.created_at, builder_metadata.observed_at, availability.observed_at) AS created_at,
                builder_metadata.built_at,
                NULLIF(builder_metadata.content_hash, '') AS content_hash,
                builder_metadata.runtime,
                builder_metadata.runtime_version,
                builder_metadata.arch,
                availability.has_base_snapshot,
                COALESCE(pool_policy.target_count, pools.pool_target) AS pool_target,
                pools.pool_current,
                builder_metadata.artifact_size_bytes,
                availability.cache_size_bytes,
                availability.base_snapshot_size_bytes,
                availability.snapshots,
                builder_metadata.vcpu,
                builder_metadata.memory_mib,
                builder_metadata.disk_size_mb,
                COALESCE(
                    auto_lifecycle_policy.default_auto_lifecycle_json,
                    builder_metadata.default_auto_lifecycle_json
                ) AS default_auto_lifecycle,
                COALESCE(t.network_policy_json, $1) AS network_policy,
                network_policy_status.observed_network_policy_generation,
                network_policy_status.observed_network_policy_authority
         FROM template_names names
         JOIN templates t ON t.name = names.name
         LEFT JOIN builder_metadata ON builder_metadata.template_name = names.name
         LEFT JOIN availability ON availability.template_name = names.name
         LEFT JOIN pools ON pools.template_name = names.name
         LEFT JOIN template_default_auto_lifecycle_policies auto_lifecycle_policy
                ON auto_lifecycle_policy.template = names.name
         LEFT JOIN template_pool_policies pool_policy
                ON pool_policy.template = names.name
         LEFT JOIN network_policy_status ON network_policy_status.template_name = names.name
         ORDER BY names.name"),
    )
    .bind(Json(NetworkPolicyV1::deny_all()))
    .bind(ReconcileFacet::NetworkPolicy.as_str())
    .bind(template.map(TemplateName::as_str))
    .bind(TemplateDesiredState::Deleted.as_str())
    .fetch_all(executor)
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
