//! Data access for the sandbox control-plane row (`sandboxes`).
//!
//! Today the row's SQL is written inline at ~150 call sites across six
//! crates. This crate consolidates that access one CRUD verb at a time,
//! following the per-table store convention: executor-generic free functions,
//! a typed record, `sql!` locators, and a small error enum. Verbs
//! so far: [`get`], [`update_desired_state`],
//! [`any_undescribed_artifact_owner`], [`network_policy_inputs_placed_on`],
//! [`snapshot_storage_placed_on`].
//!
//! # Boundaries
//!
//! * **Writes touch `sandboxes` only, and only its unguarded columns.** Of
//!   the 81 production `UPDATE sandboxes` statements outside the node-local
//!   SQLite table, 70 re-check a precondition in their `WHERE` and five more
//!   are placement transitions; none of those belong here. What is left is
//!   desired configuration — the columns a caller assigns directly.
//! * **Column writes respect the row's triggers.** `sandboxes` carries five
//!   `BEFORE UPDATE` triggers. `updated_at` is trigger-maintained, so no verb
//!   here assigns it; and because two of the triggers have no change guard,
//!   no verb may promise it touches only the columns it names. See
//!   [`update_desired_state`].
//!
//! * **Guarded placement transitions are not CRUD.** Statements whose `WHERE`
//!   re-checks a lifecycle/lease/generation precondition (the wake-completion
//!   CAS, the restore claims) are correctness mechanisms, not row updates —
//!   they stay named, guarded operations wherever they live, and no plain
//!   `update` verb here may absorb one. A new one lands here in its own
//!   module, named for the transition, as [`lost`] does; the caller
//!   keeps the locks and the other tables' writes around it.
//! * **Locking reads are not gets.** `SELECT ... FOR UPDATE` acquires a row
//!   lock inside a caller's transaction; converting one to [`get`] would drop
//!   the lock. They stay at their call sites.
//! * **The node-local SQLite `sandboxes` table** (`crates/libs/sbx/`) is a
//!   different database on a different machine and is out of scope.
//!
//! * **Composed predicates are in scope, on terms.** Most verbs here are row
//!   CRUD, but [`any_undescribed_artifact_owner`] is a question rather than a
//!   row: it LEFT JOINs `sandbox_direct_route_identities`, probes `snapshots`
//!   through `pplx-space-sandbox-portability`'s shared fragments, and returns
//!   a bare [`SandboxId`]. It belongs here anyway, because the alternative was
//!   inline SQL at the caller, which the repository contract routes through
//!   the owning store, and because reusing [`get`] per candidate would be one
//!   round trip each inside a publish transaction. The terms: the predicate's
//!   SQL is *composed* from portability fragments rather than spelled out, so
//!   the census and the drain cannot drift on what the shared parts mean; and
//!   the caller keeps the domain logic that decides which ids to ask about.
//!   A predicate that is neither shared nor latency-bound is still better off
//!   as a `get` plus a filter.
//!
//! # Growth policy
//!
//! [`SandboxRecord`] carries the row's identity/state core, not every column.
//! Columns join the record when a migrated caller needs them; the `SELECT`
//! list stays explicit so adding one is a reviewed change, not a `*`. Verbs
//! are added the same way — with their first migrated caller, never
//! speculatively.

use pplx_space_common_core::types::{LocalArtifactId, OrgId, UserId};

use std::collections::BTreeMap;
use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};
use pplx_space_sandbox_portability::{
    artifact_has_durable_copy, latest_full_snapshot_ready, restore_retry_source_snapshot_exists,
    sandbox_attached_anywhere, sandbox_attached_to_node, snapshot_durable_for_source_release,
};
use serde::{Deserialize, Serialize};
use sqlx::types::JsonValue;
use time::OffsetDateTime;
use uuid::Uuid;

mod create;
pub mod fork_source_lifecycle;
pub mod lost;

pub use create::{SandboxCreate, insert_pooled, insert_unplaced};

/// The `sandboxes.metadata` document: caller-supplied labels, untyped by
/// design. Distinct from the validated `SandboxMetadata` newtype in
/// `pplx-space-common-core` — that one enforces a key/value vocabulary,
/// this one is whatever a caller already stored. Unifying them is its own
/// change.
pub type SandboxMetadata = BTreeMap<String, SandboxMetadataValue>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SandboxMetadataValue {
    String(String),
    Number(i64),
    Bool(bool),
}

/// A sandbox-store operation failed. A malformed stored row surfaces as a
/// `sqlx::Error` decode failure, like every other typed column.
#[derive(Debug, thiserror::Error)]
pub enum SandboxStoreError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Migration scaffolding: callers converted to the store one statement at a
/// time still sit in functions whose error type is `sqlx::Error`, and `?`
/// needs this conversion until their surrounding transaction moves in too.
/// Lossless while the enum has one transparent variant; delete it when the
/// last such caller is gone rather than letting it harden into API.
impl From<SandboxStoreError> for sqlx::Error {
    fn from(error: SandboxStoreError) -> Self {
        match error {
            SandboxStoreError::Database(source) => source,
        }
    }
}

/// The identity/state core of one `sandboxes` row.
///
/// `lifecycle` and `placement` stay `String` deliberately: the stored values
/// are a wider vocabulary than any one caller's enum, and today's probe
/// callers compare against `as_str()` forms. Typing them changes how garbage
/// rows fail and is its own change, not a side effect of consolidation.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SandboxRecord {
    pub id: SandboxId,
    pub name: Option<String>,
    pub user_id: UserId,
    pub org_id: OrgId,
    pub managed_by: SubstrateKind,
    pub lifecycle: String,
    pub placement: String,
    pub active_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub template_id: Option<String>,
    /// The node-local artifact the sandbox would restore from. Joined the
    /// record with the drain-promotion caller (its first migrated reader),
    /// per the growth policy above.
    pub current_local_artifact_id: Option<LocalArtifactId>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// Fetch one sandbox row by id. `None` when no row exists.
///
/// Accepts a pool for a plain read or a transaction connection for a read
/// inside a caller's transaction (without taking a lock — see the crate docs
/// for why `FOR UPDATE` reads are not this verb).
pub async fn get(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<SandboxRecord>, SandboxStoreError> {
    let record = sqlx::query_as::<_, SandboxRecord>(sql!(
        "SELECT id, name, user_id, org_id, managed_by, lifecycle, placement,
                active_node_id, target_node_id, template_id,
                current_local_artifact_id, created_at, updated_at
         FROM sandboxes
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await?;
    Ok(record)
}

/// The network-policy inputs of one sandbox placed on a node: its own
/// override, its generation, and the policy of its template.
///
/// Both documents are returned untyped: this crate does not own the
/// network-policy schema, so the caller that does decodes them at its
/// boundary.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SandboxNetworkPolicyInputs {
    pub sandbox_id: SandboxId,
    pub template_id: Option<String>,
    /// `sandboxes.network_policy_override_json`; `None` when the sandbox
    /// inherits its template's policy.
    pub network_policy_json: Option<JsonValue>,
    pub network_policy_generation: i64,
    /// `templates.network_policy_json` of the sandbox's template, when the
    /// template exists and has one.
    pub template_network_policy_json: Option<JsonValue>,
}

/// The network-policy inputs for those of `sandbox_ids` that are placed on
/// `node_id`, active or as the target of a pending migration. A requested id
/// that is not placed there is absent from the result, not an error: the
/// caller is a node asking about its own set.
pub async fn network_policy_inputs_placed_on(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    sandbox_ids: &[SandboxId],
) -> Result<Vec<SandboxNetworkPolicyInputs>, SandboxStoreError> {
    if sandbox_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query_as::<_, SandboxNetworkPolicyInputs>(sql!(
        "SELECT s.id AS sandbox_id,
                s.template_id,
                s.network_policy_override_json AS network_policy_json,
                s.network_policy_generation,
                t.network_policy_json AS template_network_policy_json
         FROM sandboxes s
         LEFT JOIN templates t ON t.name = s.template_id
         WHERE s.id = ANY($1)
           AND (s.active_node_id = $2 OR s.target_node_id = $2)"
    ))
    .bind(sandbox_ids)
    .bind(node_id)
    .fetch_all(executor)
    .await?;
    Ok(rows)
}

/// The snapshot storage of one sandbox's organization, read by the node that
/// holds the sandbox. `org_storage_uri` is `None` when the organization has
/// no override and the deployment default applies.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SandboxSnapshotStorage {
    pub org_storage_uri: Option<String>,
}

/// The organization snapshot storage of `sandbox_id`, when that sandbox is
/// placed on one of `node_ids`: active there, or the target of a pending
/// migration or restore. A sandbox placed elsewhere is `None`, not an error:
/// the caller is a node asking about its own set.
pub async fn snapshot_storage_placed_on(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    node_ids: &[NodeId],
) -> Result<Option<SandboxSnapshotStorage>, SandboxStoreError> {
    let row = sqlx::query_as::<_, SandboxSnapshotStorage>(sql!(
        "SELECT o.snapshot_base_object_storage_uri AS org_storage_uri
         FROM sandboxes s
         JOIN organizations o ON o.id = s.org_id
         WHERE s.id = $1
           AND (s.active_node_id = ANY($2) OR s.target_node_id = ANY($2))"
    ))
    .bind(sandbox_id)
    .bind(node_ids)
    .fetch_optional(executor)
    .await?;
    Ok(row)
}

/// One of `candidates` that this node may still be holding local bytes for, if
/// any: it has a local-artifact marker, that artifact has no durable copy, and
/// it has not *demonstrably* left this node. `None` when every candidate is
/// accounted for.
///
/// **Which one is unspecified.** The statement is `LIMIT 1` with no
/// `ORDER BY`, so the row is whichever the planner reaches first. Today's
/// caller only asks whether the set is empty. A caller that needs a stable
/// answer — naming the sandbox that blocked a census, or iterating to drain
/// the set — must add an ordering rather than assume one.
///
/// "Demonstrably left" is the exclusion, and it is narrow: at least one
/// residency signal names some node **and** none of them names this one. A
/// sandbox whose signals disagree — one naming this node, another naming a
/// different one — is still returned, because one of them still says the
/// bytes could be here.
///
/// Used by the pending-backup census mirror to decide whether a node's report
/// may be trusted when it omits a sandbox the control plane thinks is holding
/// something. A wrong answer here lets a drain retire the host of an
/// artifact's only copy, so the residency test is a **union** of every signal
/// that can name a node — `active_node_id`, `target_node_id`, and a
/// direct-route sandbox's `current_node_id` — rather than a preference among
/// them. A sandbox counts as departed only when no signal names this node and
/// at least one names another.
///
/// **Why the union, when the publish path prefers instead.**
/// `publish_sandbox_states` scopes its observed-state write with
/// `COALESCE(direct_route.current_node_id, s.active_node_id)`
/// (`internal/api/server/src/published_state.rs:2259`) — the route *over*
/// `active_node_id`. That is right for choosing whose observation to record,
/// where picking wrong drops one status row. It is wrong here, where picking
/// wrong certifies a census for the node whose disk holds the artifact. The
/// two columns can only disagree in one direction: the receipt upsert moves
/// `current_node_id` to a different node only when `sandboxes.active_node_id`
/// already names it (`api/src/controllers/substrate/direct.rs:700-706`), and
/// the restore path writes route and placement in one transaction that rolls
/// back unless the placement update matches
/// (`direct.rs:806-813`). So on a disagreement `active_node_id` is the later
/// writer, and preferring the route would read a sandbox still resident here
/// as departed.
///
/// **What is shared with the drain, and what is not.** The durability
/// fragment — `artifact_has_durable_copy` — comes from
/// `pplx-space-sandbox-portability` rather than being spelled here, because
/// the drain that reads the queue has to agree with the census that wrote it
/// about what makes an artifact durable, and a second spelling is how the two
/// drift. Residency is deliberately **not** shared: this predicate unions the
/// direct route, and `queued_artifact_recoverable_without_node` — the delete
/// guard — reads only the two `sandboxes` columns. That divergence is
/// intentional and documented at the delete guard; do not "repair" it by
/// unioning the route there, which would open its departed-marker arm for a
/// sandbox the `sandboxes` columns place nowhere. This verb shares no notion
/// of "recoverable" with that guard and never calls it.
///
/// Being a composition rather than a column list is also why this is a
/// predicate verb rather than a row `get`.
pub async fn any_undescribed_artifact_owner(
    executor: impl sqlx::PgExecutor<'_>,
    node_id: NodeId,
    candidates: &[SandboxId],
) -> Result<Option<SandboxId>, SandboxStoreError> {
    if candidates.is_empty() {
        return Ok(None);
    }
    /// Built once: every input is a literal or a bind placeholder.
    static ANY_UNDESCRIBED_ARTIFACT_OWNER: LazyLock<String> = LazyLock::new(|| {
        let attached_here = format!(
            "(\n\
                 {}\n\
                 OR direct_route.current_node_id IS NOT DISTINCT FROM $2\n\
             )",
            sandbox_attached_to_node("sandboxes", "$2")
        );
        let attached_anywhere = format!(
            "(\n\
                 {}\n\
                 OR direct_route.current_node_id IS NOT NULL\n\
             )",
            sandbox_attached_anywhere("sandboxes")
        );
        let marker_durable = artifact_has_durable_copy(
            "sandboxes.id",
            "sandboxes.current_local_artifact_id",
            "sandboxes.current_local_artifact_kind",
        );
        sql!(
            format!(
                "SELECT sandboxes.id
                   FROM sandboxes
                   LEFT JOIN sandbox_direct_route_identities direct_route
                     ON direct_route.sandbox_id = sandboxes.id
                  WHERE sandboxes.id = ANY($1)
                    AND sandboxes.current_local_artifact_id IS NOT NULL
                    AND NOT {marker_durable}
                    AND NOT ({attached_anywhere} AND NOT {attached_here})
                  LIMIT 1"
            )
            .as_str()
        )
    });
    let holding: Option<Uuid> =
        sqlx::query_scalar(sqlx::AssertSqlSafe(ANY_UNDESCRIBED_ARTIFACT_OWNER.as_str()))
            .bind(candidates)
            .bind(node_id.as_uuid())
            .fetch_optional(executor)
            .await?;
    Ok(holding.map(SandboxId::from_uuid))
}

/// Whether `sandbox_id` is durably restorable from a ready snapshot the
/// restore would select. A `restoring` row needs a source in its retry window
/// ([`restore_retry_source_snapshot_exists`]). Any other row must pass both
/// the marker-aware test the node-drain source release uses
/// ([`snapshot_durable_for_source_release`]) and the newest-snapshot test
/// ([`latest_full_snapshot_ready`]): a sandbox marked to a local artifact
/// needs that artifact's ready backup, an evacuation-marked one a ready newest
/// full snapshot, and any other unmarked one a ready full snapshot fresher
/// than its last placement change with no newer full row in any state. That
/// last rule is stricter than the user restore selector, which would still
/// pick the fresh ready snapshot under a newer failed row; erring that way
/// only holds a release. An older ready snapshot the restore wouldn't select
/// doesn't count. `false` for a missing sandbox.
pub async fn durably_restorable(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, SandboxStoreError> {
    /// Built once: every input is a literal or a bind placeholder.
    static DURABLY_RESTORABLE: LazyLock<String> = LazyLock::new(|| {
        let durable = snapshot_durable_for_source_release("s");
        let newest_ready = latest_full_snapshot_ready("s");
        let retry_source = restore_retry_source_snapshot_exists("s");
        sql!(
            format!(
                "SELECT COALESCE((
                     SELECT CASE
                                WHEN s.lifecycle = 'restoring' THEN {retry_source}
                                ELSE ({durable}) AND ({newest_ready})
                            END
                       FROM sandboxes s
                      WHERE s.id = $1
                 ), FALSE)"
            )
            .as_str()
        )
    });
    Ok(
        sqlx::query_scalar(sqlx::AssertSqlSafe(DURABLY_RESTORABLE.as_str()))
            .bind(sandbox_id)
            .fetch_one(executor)
            .await?,
    )
}

/// A three-state patch for one column.
///
/// `Option` cannot express this: it collapses "leave the column alone" and
/// "write NULL" onto the same value. Both are reachable states for a
/// nullable column, and on the resource columns `NULL` is meaningful — it
/// means "inherit the template default", the same semantics
/// `SandboxNetworkPolicyPatch::InheritTemplateDefault` carries on the api's
/// network-policy patch.
///
/// Columns declared `NOT NULL` take a plain `Option` instead; there is no
/// third state for the schema to represent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldPatch<T> {
    /// Leave the stored value alone.
    Unchanged,
    /// Overwrite with this value.
    Set(T),
    /// Write `NULL`.
    Clear,
}

/// Written out rather than derived: `#[derive(Default)]` would add a
/// `T: Default` bound, but `Unchanged` holds no `T`, so the default is
/// available for every `T`. The bound would otherwise propagate to
/// `SandboxDesiredStatePatch`'s own derive the first time a field's type is
/// not itself `Default`, and report there rather than here.
impl<T> Default for FieldPatch<T> {
    fn default() -> Self {
        Self::Unchanged
    }
}

impl<T> FieldPatch<T> {
    /// `(write?, value)` — the pair each `CASE WHEN` arm binds.
    fn binds(self) -> (bool, Option<T>) {
        match self {
            FieldPatch::Unchanged => (false, None),
            FieldPatch::Set(value) => (true, Some(value)),
            FieldPatch::Clear => (true, None),
        }
    }
}

/// The plain, unguarded desired-configuration columns of a `sandboxes` row.
///
/// Deliberately excludes anything whose write is a correctness mechanism
/// rather than a field assignment: the placement/lifecycle columns (see the
/// boundary rules above), and `network_policy_override_json`, whose write
/// re-checks a generation.
#[derive(Debug, Default, Clone, Copy)]
pub struct SandboxDesiredStatePatch<'a> {
    /// `NOT NULL` column — two states are all the schema permits.
    pub metadata: Option<&'a SandboxMetadata>,
    pub vcpu: FieldPatch<i32>,
    pub memory_mib: FieldPatch<i32>,
}

/// Apply a desired-configuration patch. `false` when no row has this id —
/// an outcome, not an error, exactly as for a guarded write that loses.
///
/// `updated_at` is deliberately not assigned here. The
/// `sandboxes_set_updated_at` trigger stamps it, and only when a column
/// outside `{updated_at, last_used_at, last_access_at, last_command_at}`
/// actually changed; writing it explicitly makes the trigger return early
/// and defeats that no-op detection.
///
/// This row carries four further `BEFORE UPDATE` triggers, two of which
/// (`sandboxes_clear_restore_lease`, `sandboxes_clear_evacuation_restore_target`)
/// have no change guard. They fire on any update regardless of which columns
/// this sets, so an all-`Unchanged` patch is still not a database no-op.
pub async fn update_desired_state(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    patch: SandboxDesiredStatePatch<'_>,
) -> Result<bool, SandboxStoreError> {
    let (write_vcpu, vcpu) = patch.vcpu.binds();
    let (write_memory_mib, memory_mib) = patch.memory_mib.binds();
    let result = sqlx::query(sql!(
        "UPDATE sandboxes
            SET metadata   = COALESCE($2, metadata),
                vcpu       = CASE WHEN $3 THEN $4 ELSE vcpu END,
                memory_mib = CASE WHEN $5 THEN $6 ELSE memory_mib END
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(patch.metadata.map(sqlx::types::Json))
    .bind(write_vcpu)
    .bind(vcpu)
    .bind(write_memory_mib)
    .bind(memory_mib)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests;

/// Observe restore ownership using the database clock; this never claims work.
pub async fn restore_owner_active(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
) -> Result<bool, SandboxStoreError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT COALESCE(restore_lease_expires_at > NOW(), false) FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(executor)
    .await?
    .unwrap_or(false))
}

/// A sandbox's stored lifecycle and policy, and whether something is about to
/// activate it.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SandboxActivationObservation {
    /// The stored value, a `String` as on [`SandboxRecord::lifecycle`].
    pub lifecycle: String,
    /// An operation that brings the sandbox back to running (start, resume,
    /// restore or wake) is queued or running for it.
    pub activation_pending: bool,
    /// The sandbox's stored lifecycle policy, read with the lifecycle.
    pub auto_lifecycle: sqlx::types::Json<pplx_space_common_core::types::AutoLifecyclePolicy>,
}

/// Observe the org's sandbox's lifecycle, policy and pending activation, the
/// sandbox also named by its operation `target_id`. One statement, so an
/// activation that finishes meanwhile is seen with the lifecycle it left
/// behind, never as an old lifecycle with no activation coming. `None` when
/// the org has no such sandbox. Observation only: it never claims the work.
pub async fn activation_observation(
    executor: impl sqlx::PgExecutor<'_>,
    org_id: OrgId,
    sandbox_id: SandboxId,
    target_id: &str,
) -> Result<Option<SandboxActivationObservation>, SandboxStoreError> {
    Ok(sqlx::query_as::<_, SandboxActivationObservation>(sql!(
        "SELECT s.lifecycle,
                s.auto_lifecycle_json AS auto_lifecycle,
                EXISTS (
                    SELECT 1
                      FROM public_operations
                     WHERE org_id = s.org_id
                       AND target_type = 'sandbox'
                       AND target_id = $3
                       AND kind IN ('start_sandbox', 'resume_sandbox', 'restore_sandbox', 'wake_sandbox')
                       AND state IN ('queued', 'running')
                ) AS activation_pending
           FROM sandboxes s
          WHERE s.id = $1
            AND s.org_id = $2"
    ))
    .bind(sandbox_id)
    .bind(org_id)
    .bind(target_id)
    .fetch_optional(executor)
    .await?)
}

/// Replace desired lifecycle policy and clear its stale published observation.
pub async fn set_auto_lifecycle_policy(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
    policy: &pplx_space_common_core::types::AutoLifecyclePolicy,
) -> Result<bool, SandboxStoreError> {
    Ok(sqlx::query(sql!(
        "UPDATE sandboxes SET auto_lifecycle_json = $1,
            auto_lifecycle_state_json = '{}'::jsonb, next_auto_event_at = NULL WHERE id = $2"
    ))
    .bind(sqlx::types::Json(policy))
    .bind(id)
    .execute(executor)
    .await?
    .rows_affected()
        == 1)
}

/// Desired policy for an execution whose ownership has already been acquired.
/// Read after the claim, so a policy-only update accepted before that claim is
/// included even when the caller's original sandbox snapshot predates it.
pub async fn auto_lifecycle_policy(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
) -> Result<Option<pplx_space_common_core::types::AutoLifecyclePolicy>, SandboxStoreError> {
    let policy = sqlx::query_scalar::<
        _,
        sqlx::types::Json<pplx_space_common_core::types::AutoLifecyclePolicy>,
    >(sql!(
        "SELECT auto_lifecycle_json FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(executor)
    .await?;
    Ok(policy.map(|policy| policy.0))
}

/// Ownership evidence for deciding whether a timeout needs a live mutation.
/// A caller that persists a policy, or that concludes a `started` row has no
/// runtime, must lock the sandbox row before this read and hold the lock
/// through persistence. A read without the lock sees only committed state; a
/// caller that writes nothing may act on it only if being ordered before any
/// writer still holding the row lock is acceptable. A missing route alone is
/// not absence.
#[derive(Debug, sqlx::FromRow)]
pub struct AutoLifecycleExecution {
    pub managed_by: SubstrateKind,
    pub lifecycle: String,
    pub placement: String,
    pub active_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub execution_generation: Option<i64>,
    pub has_direct_route: bool,
    pub has_activation_lease: bool,
    pub policy: sqlx::types::Json<pplx_space_common_core::types::AutoLifecyclePolicy>,
}

impl AutoLifecycleExecution {
    /// A row with no placement, no active or target node, no execution
    /// generation, no direct route and no activation lease has no execution,
    /// but only if its substrate
    /// [proves execution absence](SubstrateKind::proves_execution_absence).
    pub fn proves_no_execution(&self) -> bool {
        self.managed_by.proves_execution_absence()
            && self.placement == "unplaced"
            && self.active_node_id.is_none()
            && self.target_node_id.is_none()
            && self.execution_generation.is_none()
            && !self.has_direct_route
            && !self.has_activation_lease
    }
}

pub async fn auto_lifecycle_execution(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
) -> Result<Option<AutoLifecycleExecution>, SandboxStoreError> {
    Ok(sqlx::query_as::<_, AutoLifecycleExecution>(sql!(
        "SELECT s.managed_by, s.lifecycle, s.placement,
                s.active_node_id, s.target_node_id,
                COALESCE(e.generation, r.execution_generation) AS execution_generation,
                r.sandbox_id IS NOT NULL AS has_direct_route,
                s.restore_lease_token IS NOT NULL AS has_activation_lease,
                s.auto_lifecycle_json AS policy
         FROM sandboxes s
         LEFT JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id AND e.active
         LEFT JOIN sandbox_direct_route_identities r ON r.sandbox_id = s.id
         WHERE s.id = $1"
    ))
    .bind(id)
    .fetch_optional(executor)
    .await?)
}

/// Immutable operator profile names selected at create.
/// Tenant policy replacements never write this column.
pub async fn egress_profiles(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<Vec<String>>, SandboxStoreError> {
    Ok(
        sqlx::query_scalar(sql!("SELECT egress_profiles FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_optional(executor)
            .await?,
    )
}
