//! The `templates` catalog row.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use std::time::Duration;

use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_node_model::NodeRole;
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::ReconcileFacet;
use pplx_space_template_model::{
    TemplateDesiredState, TemplateId, TemplateName, TemplateVisibility,
};
use serde::de::DeserializeOwned;
use sqlx::{FromRow, PgExecutor, Postgres, Transaction, types::Json};
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
/// [`create_or_undelete`] also decodes its upsert's `RETURNING` row into
/// this type, so a field added here needs a column in that `RETURNING` list
/// too.
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

/// Share-lock `template`'s row when it is present, against a runtime upgrade
/// campaign's twin insert, which holds it `FOR NO KEY UPDATE`. A template in
/// any other state, or no row, takes no lock. The lock lasts until the
/// caller's transaction ends.
pub async fn share_lock_if_present(
    tx: &mut Transaction<'_, Postgres>,
    template: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "SELECT 1 FROM templates WHERE name = $1 AND desired_state = 'present' FOR SHARE"
    ))
    .bind(template)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Lock `template`'s row `FOR NO KEY UPDATE`, whatever its state, or take
/// no lock when no row names it. The lock lasts until the caller's
/// transaction ends; it doesn't conflict with foreign-key checks'
/// key-share locks.
pub async fn lock_no_key_update(
    tx: &mut Transaction<'_, Postgres>,
    template: &TemplateName,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "SELECT 1 FROM templates WHERE name = $1 FOR NO KEY UPDATE"
    ))
    .bind(template.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Lock `FOR NO KEY UPDATE` the row of the template `release_id` is a
/// release of, or take no lock when there is no such release. The lock
/// lasts until the caller's transaction ends.
pub async fn lock_no_key_update_of_release(
    tx: &mut Transaction<'_, Postgres>,
    release_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "SELECT 1
             FROM templates t
             JOIN template_releases r ON r.template_name = t.name
             WHERE r.id = $1
             FOR NO KEY UPDATE OF t"
    ))
    .bind(release_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// A template's visibility and owner, as the admin visibility flip and the
/// node-group allowlist validation read them under a row lock.
#[derive(Debug, Clone, FromRow)]
pub struct VisibilityOwner {
    pub name: String,
    pub org_id: Option<OrgId>,
    pub visibility: TemplateVisibility,
}

/// `template`'s visibility and owner, or `None` when no row names it,
/// locking the row `FOR UPDATE` until the caller's transaction ends.
pub async fn lock_visibility(
    tx: &mut Transaction<'_, Postgres>,
    template: &str,
) -> Result<Option<VisibilityOwner>, sqlx::Error> {
    sqlx::query_as::<_, VisibilityOwner>(sql!(
        "SELECT name, org_id, visibility FROM templates WHERE name = $1 FOR UPDATE"
    ))
    .bind(template)
    .fetch_optional(&mut **tx)
    .await
}

/// The visibility and owner of each row `names` names, share-locking the
/// rows `FOR SHARE` until the caller's transaction ends.
pub async fn share_lock_visibilities(
    tx: &mut Transaction<'_, Postgres>,
    names: &[String],
) -> Result<Vec<VisibilityOwner>, sqlx::Error> {
    sqlx::query_as::<_, VisibilityOwner>(sql!(
        "SELECT name, visibility, org_id FROM templates WHERE name = ANY($1::TEXT[]) FOR SHARE"
    ))
    .bind(names)
    .fetch_all(&mut **tx)
    .await
}

/// A template's owner and desired state, and the cluster's managing
/// organization, as release registration checks its publisher against them.
#[derive(Debug)]
pub struct PublicationOwner {
    pub org_id: Option<OrgId>,
    /// The stored desired state, unparsed.
    pub desired_state: String,
    /// The cluster's managing organization, `None` when it records none.
    pub managing_org_id: Option<OrgId>,
}

/// `template`'s publication owner, or `None` when no row names it, locking
/// the template's row `FOR UPDATE` until the caller's transaction ends.
pub async fn lock_publication_owner(
    tx: &mut Transaction<'_, Postgres>,
    template: &TemplateName,
) -> Result<Option<PublicationOwner>, sqlx::Error> {
    let row = sqlx::query_as::<_, (Option<OrgId>, String, Option<OrgId>)>(sql!(
        "SELECT org_id, desired_state,
                    (SELECT managing_org_id FROM clusters WHERE singleton)
             FROM templates WHERE name = $1 FOR UPDATE"
    ))
    .bind(template.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(
        |(org_id, desired_state, managing_org_id)| PublicationOwner {
            org_id,
            desired_state,
            managing_org_id,
        },
    ))
}

/// A template for the admin create to insert, or to undelete in place.
#[derive(Clone, Copy, Debug)]
pub struct AdminCreate<'a> {
    pub id: TemplateId,
    pub name: &'a TemplateName,
    pub description: Option<&'a str>,
    pub org_id: OrgId,
    pub network_policy: &'a NetworkPolicyV1,
}

/// Insert `template` as present, or, when a row of its name exists that
/// `template.org_id` may take over (ownerless or its own) and that is
/// present, or deleted with no node's inventory naming it, overwrite that
/// row as present and clear its delete request. The network policy
/// generation advances only when the policy changes. `None` when a row of
/// the name exists that the guard refuses, which is left unchanged.
pub async fn create_or_undelete<Snapshots>(
    executor: impl PgExecutor<'_>,
    template: AdminCreate<'_>,
) -> Result<Option<AdminTemplate<Snapshots>>, sqlx::Error>
where
    Snapshots: DeserializeOwned + Send + Unpin + 'static,
{
    sqlx::query_as::<_, AdminTemplate<Snapshots>>(
        sql!("INSERT INTO templates
                 (id, name, description, org_id, desired_state, network_policy_json, network_policy_generation)
             VALUES ($1, $2, $3, $4, $6, $5, 1)
             ON CONFLICT (name) DO UPDATE
             SET description = EXCLUDED.description,
                 org_id = COALESCE(templates.org_id, EXCLUDED.org_id),
                 desired_state = EXCLUDED.desired_state,
                 delete_requested_at = NULL,
                 delete_requested_by = NULL,
                 delete_operation_id = NULL,
                 network_policy_json = EXCLUDED.network_policy_json,
                 network_policy_generation =
                     templates.network_policy_generation
                     + CASE WHEN templates.network_policy_json IS DISTINCT FROM EXCLUDED.network_policy_json
                            THEN 1 ELSE 0 END,
                 updated_at = NOW()
             WHERE (templates.org_id IS NULL OR templates.org_id = EXCLUDED.org_id)
               AND (
                   templates.desired_state = $6
                   OR (
                       templates.desired_state = $7
                       AND NOT EXISTS (
                           SELECT 1
                           FROM node_template_inventory nti
                           WHERE nti.template_name = EXCLUDED.name
                       )
                   )
               )
             RETURNING name, description, org_id, desired_state, network_policy_generation, created_at,
                       NULL::TEXT AS built_at,
                       NULL::TEXT AS content_hash,
                       NULL::TEXT AS runtime,
                       NULL::TEXT AS runtime_version,
                       NULL::TEXT AS arch,
                       NULL::BOOLEAN AS has_base_snapshot,
                       NULL::INTEGER AS pool_target,
                       NULL::INTEGER AS pool_current,
                       NULL::BIGINT AS artifact_size_bytes,
                       NULL::BIGINT AS cache_size_bytes,
                       NULL::BIGINT AS base_snapshot_size_bytes,
                       NULL::JSONB AS snapshots,
                       NULL::INTEGER AS vcpu,
                       NULL::INTEGER AS memory_mib,
                       NULL::INTEGER AS disk_size_mb,
                       NULL::JSONB AS default_auto_lifecycle,
                       network_policy_json AS network_policy,
                       NULL::BIGINT AS observed_network_policy_generation,
                       NULL::TEXT AS observed_network_policy_authority"),
    )
    .bind(template.id.as_uuid())
    .bind(template.name.as_str())
    .bind(template.description)
    .bind(template.org_id)
    .bind(Json(template.network_policy))
    .bind(TemplateDesiredState::Present.as_str())
    .bind(TemplateDesiredState::Deleted.as_str())
    .fetch_optional(executor)
    .await
}

/// Replace a present `template`'s network policy with `policy`, advancing
/// its generation when the policy changes, and only when the generation is
/// still `expected_generation` (any generation when `None`). The new
/// generation, or `None` when the template isn't present or its
/// generation moved, which leaves the row unchanged.
pub async fn replace_network_policy_if_present(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    policy: &NetworkPolicyV1,
    expected_generation: Option<i64>,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "UPDATE templates
                 SET network_policy_json = $2,
                     network_policy_generation =
                       CASE WHEN network_policy_json IS DISTINCT FROM $2
                            THEN network_policy_generation + 1
                            ELSE network_policy_generation END,
                     updated_at = NOW()
                 WHERE name = $1
                   AND desired_state = $4
                   AND ($3::BIGINT IS NULL OR network_policy_generation = $3)
                 RETURNING network_policy_generation"
    ))
    .bind(template.as_str())
    .bind(Json(policy))
    .bind(expected_generation)
    .bind(TemplateDesiredState::Present.as_str())
    .fetch_optional(executor)
    .await
}

/// Set a present `template`'s description. `false` when the template
/// isn't present, which leaves the row unchanged.
pub async fn set_description_if_present(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    description: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE templates
                 SET description = $2, updated_at = NOW()
                 WHERE name = $1 AND desired_state = $3"
    ))
    .bind(template.as_str())
    .bind(description)
    .bind(TemplateDesiredState::Present.as_str())
    .execute(executor)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Mark `template` deleting for operation `operation_id`, unless it is
/// already deleted. The request time and actor an earlier request recorded
/// are kept, and so is its operation, unless that is `stale_operation_id`,
/// the stale operation the caller observed, which `operation_id` replaces.
/// The number of rows marked: `0` when no row names the template or it is
/// deleted, which leaves the row unchanged.
pub async fn request_delete(
    executor: impl PgExecutor<'_>,
    template: &TemplateName,
    actor_user_id: UserId,
    operation_id: Uuid,
    stale_operation_id: Option<Uuid>,
) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE templates
             SET desired_state = $2,
                 delete_requested_at = COALESCE(delete_requested_at, NOW()),
                 delete_requested_by = COALESCE(delete_requested_by, $3),
                 delete_operation_id = CASE
                     WHEN delete_operation_id IS NULL
                       OR delete_operation_id = $6 THEN $4
                     ELSE delete_operation_id
                 END,
                 updated_at = NOW()
             WHERE name = $1
               AND desired_state <> $5"
    ))
    .bind(template.as_str())
    .bind(TemplateDesiredState::Deleting.as_str())
    .bind(actor_user_id)
    .bind(operation_id)
    .bind(TemplateDesiredState::Deleted.as_str())
    .bind(stale_operation_id)
    .execute(executor)
    .await?
    .rows_affected())
}

/// Delete up to `max_records` deleted templates whose delete was requested
/// (or, without a request, that were last updated or created) more than
/// `retention_secs` before the database's `NOW()`, once no release and
/// no node's inventory names them, oldest first. The number deleted; a
/// deleted template still held or within retention is left in place.
pub async fn prune_deleted(
    executor: impl PgExecutor<'_>,
    retention_secs: i64,
    max_records: i64,
) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, String>(sql!(
        "WITH candidates AS (
                 SELECT t.name
                   FROM templates t
                  WHERE t.desired_state = 'deleted'
                    AND COALESCE(t.delete_requested_at, t.updated_at, t.created_at)
                        < NOW() - ($1::BIGINT * INTERVAL '1 second')
                    AND NOT EXISTS (
                        SELECT 1
                          FROM template_releases r
                         WHERE r.template_name = t.name
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM node_template_inventory i
                         WHERE i.template_name = t.name
                    )
                  ORDER BY COALESCE(t.delete_requested_at, t.updated_at, t.created_at) ASC,
                           t.name ASC
                  LIMIT $2
             )
             DELETE FROM templates t
              USING candidates c
              WHERE t.name = c.name
              RETURNING t.name"
    ))
    .bind(retention_secs)
    .bind(max_records)
    .fetch_all(executor)
    .await?
    .len() as u64)
}

/// A requested template's network policy as a node reads it.
#[derive(Debug, Clone, FromRow)]
pub struct NodeTemplateNetworkPolicy {
    pub template: String,
    pub network_policy_json: Json<NetworkPolicyV1>,
    pub network_policy_generation: i64,
}

/// The network policy of each of `templates` that node `node_id`, in
/// `role`, may see: a present generic template, one a sandbox placed on
/// the node runs, for a worker one an enabled pool config's release names,
/// and for a builder the base of a build its grant, fresh by the database
/// clock, authorizes. Names with no row, and names the node may not see,
/// are left out.
pub async fn node_network_policies(
    executor: impl PgExecutor<'_>,
    templates: &[String],
    node_id: NodeId,
    role: NodeRole,
) -> Result<Vec<NodeTemplateNetworkPolicy>, sqlx::Error> {
    sqlx::query_as::<_, NodeTemplateNetworkPolicy>(sql!(
        "WITH requested AS (
                     SELECT DISTINCT UNNEST($1::TEXT[]) AS template
                 ),
                 private_requested AS (
                     SELECT t.name
                     FROM requested r
                     JOIN templates t ON t.name = r.template
                     WHERE NOT (t.visibility = 'generic' AND t.desired_state = 'present')
                 ),
                 placed AS (
                     SELECT sb.template_id
                     FROM sandboxes sb
                     WHERE sb.target_node_id = $3
                       AND EXISTS (SELECT 1 FROM private_requested)
                     UNION
                     SELECT sb.template_id
                     FROM sandboxes sb
                     WHERE sb.active_node_id = $3
                       AND EXISTS (SELECT 1 FROM private_requested)
                 )
                 SELECT r.template,
                        COALESCE(t.network_policy_json, $2::JSONB) AS network_policy_json,
                        COALESCE(t.network_policy_generation, 0) AS network_policy_generation
                 FROM requested r
                 LEFT JOIN templates t ON t.name = r.template
                 WHERE (t.visibility = 'generic' AND t.desired_state = 'present')
                    OR t.name IN (SELECT template_id FROM placed)
                    OR ($4 = 'worker' AND EXISTS (
                        SELECT 1
                        FROM kubernetes_pool_configs pc
                        JOIN template_releases pr ON pr.id = pc.template_release_id
                        WHERE pc.enabled
                          AND pr.template_name = t.name
                    ))
                    OR ($4 = 'builder' AND EXISTS (
                        SELECT 1
                        FROM template_build_base_grants g
                        WHERE g.base_template_name = t.name
                          AND g.org_id = t.org_id
                          AND ((g.builder_node_id IS NULL
                                AND g.granted_at > NOW() - INTERVAL '1 hour')
                               OR (g.builder_node_id = $3
                                   AND g.granted_at > NOW() - INTERVAL '7 days'))
                    ))"
    ))
    .bind(templates)
    .bind(Json(NetworkPolicyV1::deny_all()))
    .bind(node_id)
    .bind(role.as_str())
    .fetch_all(executor)
    .await
}

/// A requested template's base snapshot policy as a node reads it.
#[derive(Debug, Clone, FromRow)]
pub struct NodeTemplateBaseSnapshotPolicy {
    pub template: String,
    pub ensure_base_snapshot: bool,
    pub snapshot_vcpu: Option<i32>,
    pub snapshot_memory_mib: Option<i32>,
}

/// The base snapshot policy of each of `templates` that node `node_id`,
/// in `role`, may see, as [`node_network_policies`] decides it, with the
/// shape the node's own inventory last reported in place of the policy's.
pub async fn node_base_snapshot_policies(
    executor: impl PgExecutor<'_>,
    templates: &[String],
    node_id: NodeId,
    role: NodeRole,
) -> Result<Vec<NodeTemplateBaseSnapshotPolicy>, sqlx::Error> {
    sqlx::query_as::<_, NodeTemplateBaseSnapshotPolicy>(sql!(
        "WITH requested AS (
                 SELECT DISTINCT UNNEST($1::TEXT[]) AS template
             ),
             private_requested AS (
                 SELECT t.name
                 FROM requested r
                 JOIN templates t ON t.name = r.template
                 WHERE NOT (t.visibility = 'generic' AND t.desired_state = 'present')
             ),
             placed AS (
                 SELECT sb.template_id
                 FROM sandboxes sb
                 WHERE sb.target_node_id = $2
                   AND EXISTS (SELECT 1 FROM private_requested)
                 UNION
                 SELECT sb.template_id
                 FROM sandboxes sb
                 WHERE sb.active_node_id = $2
                   AND EXISTS (SELECT 1 FROM private_requested)
             ),
             local_inventory AS (
                 SELECT DISTINCT ON (nti.template_name)
                        nti.template_name,
                        nti.vcpu,
                        nti.memory_mib
                 FROM node_template_inventory nti
                 WHERE nti.node_id = $2
                   AND (nti.vcpu IS NOT NULL OR nti.memory_mib IS NOT NULL)
                 ORDER BY nti.template_name,
                          nti.observed_at DESC,
                          nti.built_at DESC NULLS LAST,
                          nti.content_hash ASC
             )
             SELECT r.template,
                    COALESCE(p.ensure_base_snapshot, FALSE) AS ensure_base_snapshot,
                    COALESCE(li.vcpu, p.snapshot_vcpu) AS snapshot_vcpu,
                    COALESCE(li.memory_mib, p.snapshot_memory_mib) AS snapshot_memory_mib
             FROM requested r
             LEFT JOIN template_base_snapshot_policies p ON p.template = r.template
             LEFT JOIN local_inventory li ON li.template_name = r.template
             LEFT JOIN templates t ON t.name = r.template
             WHERE (t.visibility = 'generic' AND t.desired_state = 'present')
                OR t.name IN (SELECT template_id FROM placed)
                OR ($3 = 'worker' AND EXISTS (
                    SELECT 1
                    FROM kubernetes_pool_configs pc
                    JOIN template_releases pr ON pr.id = pc.template_release_id
                    WHERE pc.enabled
                      AND pr.template_name = t.name
                ))
                OR ($3 = 'builder' AND EXISTS (
                    SELECT 1
                    FROM template_build_base_grants g
                    WHERE g.base_template_name = t.name
                      AND g.org_id = t.org_id
                      AND ((g.builder_node_id IS NULL
                            AND g.granted_at > NOW() - INTERVAL '1 hour')
                           OR (g.builder_node_id = $2
                               AND g.granted_at > NOW() - INTERVAL '7 days'))
                ))"
    ))
    .bind(templates)
    .bind(node_id)
    .bind(role.as_str())
    .fetch_all(executor)
    .await
}

/// A requested template's desired state as a node reads it.
#[derive(Debug, Clone, FromRow)]
pub struct NodeTemplateDesiredState {
    pub template: String,
    /// The stored desired state, unparsed.
    pub desired_state: String,
    pub default_auto_lifecycle_json: Option<Json<AutoLifecyclePolicy>>,
    pub pool_target: Option<i32>,
    pub pool_boot_mode: Option<String>,
    pub pool_vcpu: Option<i32>,
    pub pool_memory_mib: Option<i32>,
    /// The node has a reason to hold the template, so it is answered.
    pub admitted: bool,
    /// The node's published inventory lists the template.
    pub held_here: bool,
}

/// The desired state of each of `templates` that has a row, for node
/// `node_id` in `role`: `admitted` when the node may see it, as
/// [`node_network_policies`] decides it, or when it isn't present and the
/// node's inventory still holds it, so the node hears to delete it.
pub async fn node_desired_states(
    executor: impl PgExecutor<'_>,
    templates: &[String],
    node_id: NodeId,
    role: NodeRole,
) -> Result<Vec<NodeTemplateDesiredState>, sqlx::Error> {
    sqlx::query_as::<_, NodeTemplateDesiredState>(sql!("WITH requested AS (
                 SELECT DISTINCT UNNEST($1::TEXT[]) AS template
             ),
             private_requested AS (
                 SELECT t.name
                 FROM requested r
                 JOIN templates t ON t.name = r.template
                 WHERE NOT (t.visibility = 'generic' AND t.desired_state = 'present')
             ),
             placed AS (
                 SELECT sb.template_id
                 FROM sandboxes sb
                 WHERE sb.target_node_id = $3
                   AND EXISTS (SELECT 1 FROM private_requested)
                 UNION
                 SELECT sb.template_id
                 FROM sandboxes sb
                 WHERE sb.active_node_id = $3
                   AND EXISTS (SELECT 1 FROM private_requested)
             ),
             local_inventory AS (
                 SELECT DISTINCT ON (nti.template_name)
                        nti.template_name,
                        nti.vcpu,
                        nti.memory_mib
                 FROM node_template_inventory nti
                 WHERE nti.node_id = $3
                   AND (nti.vcpu IS NOT NULL OR nti.memory_mib IS NOT NULL)
                 ORDER BY nti.template_name,
                          nti.observed_at DESC,
                          nti.built_at DESC NULLS LAST,
                          nti.content_hash ASC
             )
             SELECT r.template,
                    t.desired_state,
                    al.default_auto_lifecycle_json,
                    CASE
                        WHEN $2 = 'worker' AND t.desired_state = 'present'
                        THEN pp.target_count
                        ELSE NULL
                    END AS pool_target,
                    CASE
                        WHEN $2 = 'worker' AND t.desired_state = 'present'
                        THEN pp.boot_mode
                        ELSE NULL
                    END AS pool_boot_mode,
                    CASE
                        WHEN $2 = 'worker' AND t.desired_state = 'present' AND pp.boot_mode = 'snapshot'
                        THEN COALESCE(li.vcpu, bsp.snapshot_vcpu)
                        ELSE NULL
                    END AS pool_vcpu,
                    CASE
                        WHEN $2 = 'worker' AND t.desired_state = 'present' AND pp.boot_mode = 'snapshot'
                        THEN COALESCE(li.memory_mib, bsp.snapshot_memory_mib)
                        ELSE NULL
                    END AS pool_memory_mib,
                    COALESCE(
                        (t.visibility = 'generic' AND t.desired_state = 'present')
                        OR t.name IN (SELECT template_id FROM placed)
                        OR ($2 = 'worker' AND EXISTS (
                            SELECT 1
                            FROM kubernetes_pool_configs pc
                            JOIN template_releases pr ON pr.id = pc.template_release_id
                            WHERE pc.enabled
                              AND pr.template_name = t.name
                        ))
                        OR ($2 = 'builder' AND EXISTS (
                            SELECT 1
                            FROM template_build_base_grants g
                            WHERE g.base_template_name = t.name
                              AND g.org_id = t.org_id
                              AND ((g.builder_node_id IS NULL
                                    AND g.granted_at > NOW() - INTERVAL '1 hour')
                                   OR (g.builder_node_id = $3
                                       AND g.granted_at > NOW() - INTERVAL '7 days'))
                        ))
                        OR (t.desired_state <> 'present' AND EXISTS (
                            SELECT 1
                            FROM node_template_inventory nti
                            WHERE nti.template_name = r.template
                              AND nti.node_id = $3
                        )),
                        FALSE
                    ) AS admitted,
                    EXISTS (
                        SELECT 1
                        FROM node_template_inventory nti
                        WHERE nti.template_name = r.template
                          AND nti.node_id = $3
                    ) AS held_here
             FROM requested r
             JOIN templates t ON t.name = r.template
             LEFT JOIN template_default_auto_lifecycle_policies al
                    ON al.template = r.template
             LEFT JOIN template_pool_policies pp
                    ON pp.template = r.template
             LEFT JOIN template_base_snapshot_policies bsp
                    ON bsp.template = r.template
             LEFT JOIN local_inventory li
                    ON li.template_name = r.template"))
    .bind(templates)
    .bind(role.as_str())
    .bind(node_id)
    .fetch_all(executor)
    .await
}
