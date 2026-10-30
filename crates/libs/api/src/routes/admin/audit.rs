use pplx_space_common_core::types::{NodeGroupId, NodeId, OrgId, UserId};

use axum::{
    Json,
    extract::{Query, State},
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::NodeDrainMode;
use pplx_space_public_api::{OperationId, PublicSandboxId};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{models::User, routes::AppState, routes::public::error::PublicApiError};

const DEFAULT_AUDIT_EVENT_LIMIT: i64 = 100;
const MAX_AUDIT_EVENT_LIMIT: i64 = 250;

#[derive(Debug, Deserialize)]
pub(super) struct ListAdminAuditEventsQuery {
    limit: Option<i64>,
}

impl ListAdminAuditEventsQuery {
    fn normalized_limit(&self) -> Result<i64, PublicApiError> {
        match self.limit {
            None => Ok(DEFAULT_AUDIT_EVENT_LIMIT),
            Some(limit) if !(1..=MAX_AUDIT_EVENT_LIMIT).contains(&limit) => Err(
                PublicApiError::bad_request("limit must be between 1 and 250"),
            ),
            Some(limit) => Ok(limit),
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct ListAdminAuditEventsResponse {
    events: Vec<AdminAuditEvent>,
}

#[derive(Debug, FromRow, Serialize)]
pub(super) struct AdminAuditEvent {
    id: Uuid,
    actor_user_id: UserId,
    actor_org_id: OrgId,
    actor_email: String,
    method: String,
    path: String,
    status_code: i32,
    success: bool,
    duration_ms: i64,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
}

pub(super) struct AdminAuditRequestEntry<'a> {
    pub actor: &'a User,
    pub method: &'a str,
    pub path: &'a str,
    pub status_code: u16,
    pub duration_ms: i64,
}

pub(super) async fn list_audit_events(
    State(state): State<AppState>,
    Query(query): Query<ListAdminAuditEventsQuery>,
) -> Result<Json<ListAdminAuditEventsResponse>, PublicApiError> {
    let limit = query.normalized_limit()?;
    let events = sqlx::query_as::<_, AdminAuditEvent>(sql!(
        "SELECT
             id,
             actor_user_id,
             actor_org_id,
             actor_email,
             method,
             path,
             status_code,
             success,
             duration_ms,
             created_at
         FROM admin_audit_events
         ORDER BY created_at DESC, id DESC
         LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;

    Ok(Json(ListAdminAuditEventsResponse { events }))
}

pub(super) async fn record_request(
    pool: &PgPool,
    entry: AdminAuditRequestEntry<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO admin_audit_events (
             id,
             actor_user_id,
             actor_org_id,
             actor_email,
             method,
             path,
             status_code,
             success,
             duration_ms
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
    ))
    .bind(Uuid::now_v7())
    .bind(entry.actor.id)
    .bind(entry.actor.org_id)
    .bind(&entry.actor.email)
    .bind(entry.method)
    .bind(entry.path)
    .bind(i32::from(entry.status_code))
    .bind((200..400).contains(&entry.status_code))
    .bind(entry.duration_ms)
    .execute(pool)
    .await?;

    Ok(())
}

/// Parameters for an audit entry. Groups the fixed fields together so we can
/// add new ones without growing the call-site signature past clippy's
/// `too_many_arguments` ceiling.
pub struct AdminAuditEntry<'a> {
    pub actor: &'a User,
    pub action: &'a str,
    pub node_id: NodeId,
    pub node_name: Option<&'a str>,
    pub from_phase: Option<&'a str>,
    pub to_phase: Option<&'a str>,
    /// The mode of the drain already in progress, on the actions that
    /// transition one. Together with [`Self::drain_mode`] this is what
    /// distinguishes an in-place escalation (`gentle → forced`) from a
    /// `:drain --forced` that re-asserted a mode the node already had — the two
    /// are otherwise the same entry, because both sit on a
    /// `draining → draining` edge and both record `drain_mode: forced`.
    ///
    /// `None` when the node has no drain in progress: a NULL `drain_mode`
    /// column reads as `gentle`, which is the right answer for a draining node
    /// and an invented one for a node nobody has drained. Also `None` on every
    /// action that *cannot* move the drain state machine — `:cordon`,
    /// `:uncordon` and `:force-upload-local-snapshots`. Gated on the action
    /// class rather than the outcome, so a refused `:drain` or `:terminate` does
    /// carry a from-mode: that is the state the refusal was measured against,
    /// and a rejected entry is exactly where an operator wants to see it. The
    /// one refusal that leaves it `None` is a substrate without node drain,
    /// which answers before the row is read and so measured no state.
    ///
    /// Read as a from/to pair with [`Self::drain_mode`] on `:drain` only, which
    /// is the only action carrying a *requested* mode. `:terminate` and
    /// `:undrain` populate this field and leave `drain_mode` empty, because they
    /// move the phase rather than the mode.
    pub from_drain_mode: Option<NodeDrainMode>,
    /// The `replace_after_terminate` the request expressed, on `:drain` only —
    /// what the operator asked for, not what was stored. `None` covers both
    /// "this action carries no such flag" and "the request omitted it".
    ///
    /// Empty does **not** mean nothing was persisted. On the edge that starts a
    /// drain an omitted flag still writes the auto-replace default, so for a
    /// fresh drain the stored value is the default whenever this field is empty.
    /// It is only on an already-draining node that empty means the column was
    /// left alone.
    ///
    /// Recorded because `nodes.terminate_decrement_capacity` decides whether the
    /// eventual ASG terminate decrements desired capacity, and the edge that
    /// *starts* a drain writes it from this field. On an already-draining node
    /// the column cannot move — a disagreeing request is refused — so what this
    /// field records there is the refusal's cause: which value was asked for
    /// against a drain that would not take it.
    pub replace_after_terminate: Option<bool>,
    /// Drain mode the operator asked for, on `:drain`. Recorded because it is
    /// what decides whether the node's resident sandboxes were evacuated and
    /// their users served 409s, and "who forced a drain on this node?" is
    /// otherwise unanswerable from the audit trail — both modes log the same
    /// `NULL → draining` transition.
    ///
    /// One exception, and it is not a requested mode:
    /// `:force-upload-local-snapshots`' wake-gate-open refusal puts the node's
    /// *current* mode here, because that refusal is precisely "the gate is open,
    /// and here is the mode that left it open". Its other two entries leave this
    /// `None`. So on that action the field is a state snapshot on one path and
    /// empty on the rest — do not pair it with [`Self::from_drain_mode`] there.
    pub drain_mode: Option<NodeDrainMode>,
    pub success: bool,
    pub error: Option<&'a str>,
    /// Pre-rendered per-invocation outcome counts for actions whose
    /// `success` bit doesn't tell the whole story. For
    /// `:force-upload-local-snapshots`, an operator investigating
    /// "why is this node still stuck?" needs to know from the audit
    /// trail whether the prior call drained the work queue or just
    /// chipped at it (non-empty `deferred` / `truncated: true`). The
    /// `Drain` / `Terminate` / `Undrain` actions don't have per-call
    /// numeric outcomes, so they leave this `None`.
    pub outcome_counts: Option<&'a str>,
}

pub struct AdminSandboxAuditEntry<'a> {
    pub actor: &'a User,
    pub action: &'a str,
    pub sandbox_id: PublicSandboxId,
    pub org_id: OrgId,
    pub operation_id: Option<OperationId>,
    pub success: bool,
    pub error: Option<&'a str>,
}

/// Emit a high-signal audit log entry for an admin action.
///
/// Request-level admin audit events are persisted separately; this structured
/// event carries resource-specific outcome details for log indexing.
pub fn record(entry: AdminAuditEntry<'_>) {
    tracing::info!(
        target: "space.admin.audit",
        event = "admin_audit",
        actor_user_id = %entry.actor.id,
        actor_email = %entry.actor.email,
        action = entry.action,
        node_id = %entry.node_id,
        node_name = entry.node_name.unwrap_or(""),
        from_phase = entry.from_phase.unwrap_or(""),
        to_phase = entry.to_phase.unwrap_or(""),
        from_drain_mode = entry.from_drain_mode.map(NodeDrainMode::as_str).unwrap_or(""),
        replace_after_terminate = entry
            .replace_after_terminate
            .map(|value| if value { "true" } else { "false" })
            .unwrap_or(""),
        drain_mode = entry.drain_mode.map(NodeDrainMode::as_str).unwrap_or(""),
        success = entry.success,
        error = entry.error.unwrap_or(""),
        outcome_counts = entry.outcome_counts.unwrap_or(""),
    );
}

pub fn record_sandbox(entry: AdminSandboxAuditEntry<'_>) {
    tracing::info!(
        target: "space.admin.audit",
        event = "admin_audit",
        actor_user_id = %entry.actor.id,
        actor_email = %entry.actor.email,
        action = entry.action,
        resource_type = "sandbox",
        sandbox_id = %entry.sandbox_id,
        org_id = %entry.org_id,
        operation_id = entry.operation_id.map(|id| id.to_string()).unwrap_or_default(),
        success = entry.success,
        error = entry.error.unwrap_or(""),
    );
}

#[cfg(test)]
mod tests;

/// Parameters for a node-group admin audit entry. `from_state` / `to_state`
/// carry a pre-rendered summary of the changed fields (org assignment,
/// policy, default flag, allowlist size) so the trail records what the write
/// transitioned, not just that it happened.
pub struct AdminNodeGroupAuditEntry<'a> {
    pub actor: &'a User,
    pub action: &'a str,
    pub node_group_id: NodeGroupId,
    pub node_group_name: Option<&'a str>,
    pub from_state: Option<&'a str>,
    pub to_state: Option<&'a str>,
    pub success: bool,
    pub error: Option<&'a str>,
}

/// Emit a high-signal audit log entry for a node-group admin action. Same
/// transport story as [`record`]: a structured `tracing` event under the
/// stable `space.admin.audit` target; a Postgres-backed `audit_log` table can
/// replace the body without touching call sites.
pub fn record_node_group(entry: AdminNodeGroupAuditEntry<'_>) {
    tracing::info!(
        target: "space.admin.audit",
        event = "admin_audit",
        actor_user_id = %entry.actor.id,
        actor_email = %entry.actor.email,
        action = entry.action,
        node_group_id = %entry.node_group_id,
        node_group_name = entry.node_group_name.unwrap_or(""),
        from_state = entry.from_state.unwrap_or(""),
        to_state = entry.to_state.unwrap_or(""),
        success = entry.success,
        error = entry.error.unwrap_or(""),
    );
}

/// Machine principal for the `node_group_labeler` reconciler's audit entries.
///
/// The `system:` prefix is the namespace for non-operator actors on this
/// stream: it cannot collide with a `Uuid`-rendered `actor_user_id`, so a
/// consumer can tell an operator write from a reconciler write by inspection.
/// A second reconciler adds its own `system:<name>` constant here rather than
/// reusing this one.
pub const SYSTEM_ACTOR_NODE_GROUP_LABELER: &str = "system:node-group-labeler";

/// Parameters for a node-group audit entry written by a **system actor** — a
/// reconciler, not an operator.
///
/// A system actor exists because one durable `node_groups` write happens
/// before any operator can act: the `node_group_labeler` reconciler bootstraps
/// the singleton default generic group on a cluster that has none, which is
/// what every `NULL` `nodes.node_group_id` resolves to. That write is
/// management state and belongs in the audit trail, but the reconciler has no
/// `User`, and every operator entry type in this module requires one.
///
/// Fabricating a synthetic `User` was the alternative and is worse: this
/// stream exists for attribution, so a made-up user id and email are false
/// attribution in the one place that must not carry any. Instead `actor` is a
/// stable machine principal string (see
/// [`SYSTEM_ACTOR_NODE_GROUP_LABELER`]) and `actor_email` renders empty rather
/// than invented.
///
/// The field *names* deliberately match [`record_node_group`] exactly, on the
/// same `space.admin.audit` target and the same `event = "admin_audit"`, so
/// existing consumers pick these entries up with no change — only the
/// `actor_user_id` value shape differs.
pub struct SystemNodeGroupAuditEntry<'a> {
    /// Machine principal, e.g. [`SYSTEM_ACTOR_NODE_GROUP_LABELER`]. A field
    /// rather than a hardcoded constant in the body so a second reconciler
    /// reuses this emitter.
    pub actor: &'a str,
    pub action: &'a str,
    pub node_group_id: NodeGroupId,
    pub node_group_name: Option<&'a str>,
    pub from_state: Option<&'a str>,
    pub to_state: Option<&'a str>,
    pub success: bool,
    pub error: Option<&'a str>,
}

/// Emit a node-group audit entry attributed to a system actor. See
/// [`SystemNodeGroupAuditEntry`] for why this path exists and
/// [`record_node_group`] for the operator equivalent whose field shape it
/// matches.
pub fn record_node_group_system(entry: SystemNodeGroupAuditEntry<'_>) {
    tracing::info!(
        target: "space.admin.audit",
        event = "admin_audit",
        actor_user_id = entry.actor,
        actor_email = "",
        action = entry.action,
        node_group_id = %entry.node_group_id,
        node_group_name = entry.node_group_name.unwrap_or(""),
        from_state = entry.from_state.unwrap_or(""),
        to_state = entry.to_state.unwrap_or(""),
        success = entry.success,
        error = entry.error.unwrap_or(""),
    );
}

/// Parameters for a template-visibility admin audit entry.
pub struct AdminTemplateAuditEntry<'a> {
    pub actor: &'a User,
    pub action: &'a str,
    pub template_name: &'a str,
    pub from_state: Option<&'a str>,
    pub to_state: Option<&'a str>,
    pub success: bool,
    pub error: Option<&'a str>,
}

/// Emit a high-signal audit log entry for a template admin action.
pub fn record_template(entry: AdminTemplateAuditEntry<'_>) {
    tracing::info!(
        target: "space.admin.audit",
        event = "admin_audit",
        actor_user_id = %entry.actor.id,
        actor_email = %entry.actor.email,
        action = entry.action,
        template_name = entry.template_name,
        from_state = entry.from_state.unwrap_or(""),
        to_state = entry.to_state.unwrap_or(""),
        success = entry.success,
        error = entry.error.unwrap_or(""),
    );
}

/// Parameters for a runtime environment policy write's audit entry.
pub(super) struct AdminRuntimeEnvironmentAuditEntry<'a> {
    pub actor: &'a User,
    pub action: &'a str,
    pub environment_ref: &'a str,
    pub requested_state: &'a str,
    pub reason: &'a str,
    /// The caller waived the check for work a write would strand.
    pub strand: bool,
    /// The generation the write produced; `None` when it was refused.
    pub generation: Option<u64>,
    pub success: bool,
    pub error: Option<&'a str>,
}

pub(super) fn record_runtime_environment(entry: AdminRuntimeEnvironmentAuditEntry<'_>) {
    tracing::info!(
        target: "space.admin.audit",
        event = "admin_audit",
        actor_user_id = %entry.actor.id,
        actor_email = %entry.actor.email,
        action = entry.action,
        environment_ref = entry.environment_ref,
        requested_state = entry.requested_state,
        reason = entry.reason,
        strand = entry.strand,
        generation = entry.generation,
        success = entry.success,
        error = entry.error.unwrap_or(""),
    );
}
