//! The api's sandbox response reads and list pages: one sandbox's response
//! row, and the pages of the native admin, ECW and public lists.
//!
//! Every statement here joins a Direct table: the response row reads the
//! node a sandbox runs on from its Direct route when it has one, and the
//! admin list's node filters ask which node the response reports. They move
//! here whole from the api, which keeps the decisions around them: which
//! list a request runs, its filters, its limit and the byte budget it binds.
//!
//! The native statements decode [`Sandbox`], or [`ScopedPageRow`] for a
//! page, from one projection and one set of joins, private to this module
//! and its children: the response's columns (`SANDBOX_COLUMNS`) and the
//! joins they read (`SANDBOX_JOIN_TAIL`). The public API's statements, in
//! [`public`], decode [`public::PublicSandboxRow`] from their own pair.
//!
//! # Pages
//!
//! Every list page here walks the same byte budget, which each page
//! statement writes out in full: a `WITH RECURSIVE` clause in front of its
//! select defines `page_rows (id, position, has_next)`, the sandboxes of one
//! page in list order, numbered from 1, and whether each is followed by
//! another candidate, whether or not the budget let that one in.
//!
//! `page_candidates` holds the ids of the sandboxes the list shows after the
//! cursor, newest first by `(created_at, id)`, at most `max_rows` of them, so
//! it is planned like a list without a budget and reads only the ids. The
//! walk then follows those ids one row at a time, adding each row's estimated
//! size to a running total. It stops one row after the total passes
//! `max_bytes`, so the page keeps rows within the budget, always the first so
//! the cursor advances, and the row after the last kept one tells the caller
//! whether the list continues. It is recursive so that Postgres sizes only
//! those rows: a running-sum window cannot stop early and would size every
//! candidate.
//!
//! The estimate is the JSON text of the metadata, of `env` when the list
//! returns it, and of the escaped name, plus `row_overhead_bytes` for
//! everything else; jsonb's text form is never shorter than the compact JSON
//! a page is served as. `pg_column_size` would be cheaper, but it is the
//! compressed size, which repetitive metadata makes many times smaller than
//! the decoded row.
//!
//! The select then starts `FROM page_rows page JOIN sandboxes s ON s.id =
//! page.id` and orders by `page.position`, so the planner joins each page row
//! by id instead of scanning `sandboxes` for a hash join. Each statement
//! binds its walk values as [`PageWalk`] carries them, and the caller decides
//! from `has_next` and the number of rows whether the list continues.
//!
//! Every statement is built once, with its `sql!` locator, binds every value,
//! and runs on the executor its caller passes.

pub mod admin;
pub mod public;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::LazyLock;

use pplx_space_accounts::{
    ClientId, NodeId, OrgId, SandboxOwnerSource, UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId,
    UserId,
};
use pplx_space_auto_lifecycle::{AutoLifecyclePolicy, AutoLifecycleRuntimeState};
use pplx_space_k8s_ids::PodUid;
use pplx_space_net_common::{NetworkPolicy, NetworkPolicyV1};
use pplx_space_node_model::{NodeAddress, NodeManagedBy, NodeRole};
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{EcwCredentialEpoch, SandboxActivationAttemptId, SandboxId};
use pplx_space_sql_locator::sql;
use serde::{Deserialize, Serialize};
use sqlx::types::JsonValue;
use time::OffsetDateTime;

use crate::SandboxMetadata;

/// A sandbox's response row: its `sandboxes` row with its owner, its
/// template's network policy, the node it runs on, its active execution, its
/// ECW credential epoch and its release's tags.
#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct Sandbox {
    pub id: SandboxId,
    pub name: Option<String>,
    /// SPACE user row that owns the sandbox. Federated owners map onto local
    /// user/org rows so downstream paths keep the normal sandbox FK shape.
    pub user_id: UserId,
    /// The current email of `user_id`'s user, read with the row. A federated
    /// owner's user row holds a generated placeholder, which no api response
    /// shows.
    #[serde(default)]
    pub user_email: Option<String>,
    pub org_id: OrgId,
    pub owner_source: SandboxOwnerSource,
    pub upstream_issuer: Option<String>,
    pub upstream_org_id: Option<String>,
    pub upstream_subject_id: Option<String>,
    pub managed_by: String,
    pub lifecycle: String,
    pub placement: String,
    /// The client id whose activation created the current runtime; `None`
    /// when nothing is recorded.
    pub activating_client_id: Option<ClientId>,
    /// The Direct route's current node when the sandbox has a route, else
    /// the row's `active_node_id`.
    pub active_node_id: Option<NodeId>,
    pub active_node_name: Option<String>,
    pub active_node_role: Option<NodeRole>,
    pub active_node_address: Option<NodeAddress>,
    pub active_node_managed_by: Option<NodeManagedBy>,
    pub target_node_id: Option<NodeId>,
    pub active_execution_generation: Option<i64>,
    pub active_pod_uid: Option<PodUid>,
    pub active_activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub ecw_credential_epoch: EcwCredentialEpoch,
    pub placement_changed_at: Option<OffsetDateTime>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub release_tags: Vec<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_mib: Option<i32>,
    pub disk_io_max_operations_per_sec: i64,
    pub disk_io_max_bandwidth_mib_per_sec: i64,
    pub env: sqlx::types::Json<BTreeMap<String, String>>,
    pub metadata: sqlx::types::Json<SandboxMetadata>,
    pub auto_lifecycle: sqlx::types::Json<AutoLifecyclePolicy>,
    pub auto_lifecycle_state: sqlx::types::Json<AutoLifecycleRuntimeState>,
    pub network_policy: Option<sqlx::types::Json<NetworkPolicy>>,
    pub network_policy_generation: i64,
    pub template_network_policy: Option<sqlx::types::Json<NetworkPolicyV1>>,
    pub template_network_policy_generation: Option<i64>,
    pub next_auto_event_at: Option<i64>,
    pub snapshots_enabled_desired: bool,
    /// BYOK: this sandbox runs on dm-crypt encrypted volumes. Immutable for
    /// the sandbox's lifetime — stamped in the create INSERT, never UPDATEd.
    /// Loaded with COALESCE so pre-column (NULL) rows read plaintext.
    pub volume_encryption: bool,
    /// `started` was reported to the caller before the guest command API was
    /// confirmed ready (a budget-exhausted slow create whose pod proxy was
    /// reachable). The access path forces a command-ready confirmation and
    /// clears this on success; until then a data-plane op gets the
    /// SDK-recoverable `sandbox_timeout` rather than a bare upstream error.
    #[serde(default)]
    pub command_ready_pending: bool,
    pub last_used_at: Option<OffsetDateTime>,
    pub last_access_at: Option<OffsetDateTime>,
    pub last_command_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// A sandbox on an ECW or admin list page and whether another sandbox
/// follows it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ScopedPageRow {
    #[sqlx(flatten)]
    pub sandbox: Sandbox,
    pub has_next: bool,
}

/// The values a list page's walk binds: the cursor's `created_at` and `id`
/// (`None` for the first page), the most rows to walk, which is the page
/// limit plus one, the byte budget, and the estimated bytes of a listed row
/// apart from its name, metadata and env.
#[derive(Debug, Clone, Copy)]
pub struct PageWalk {
    pub after: Option<(OffsetDateTime, SandboxId)>,
    pub max_rows: i64,
    pub max_bytes: i64,
    pub row_overhead_bytes: i64,
}

impl PageWalk {
    fn cursor_created_at(&self) -> Option<OffsetDateTime> {
        self.after.map(|(created_at, _)| created_at)
    }

    fn cursor_id(&self) -> Option<SandboxId> {
        self.after.map(|(_, id)| id)
    }
}

/// The response row's columns, over `s` and the joins of
/// [`SANDBOX_JOIN_TAIL`].
const SANDBOX_COLUMNS: &str = "
    s.id,
    s.name,
    s.user_id,
    owner_user.email AS user_email,
    s.org_id,
    owner_user.owner_source AS owner_source,
    owner_user.upstream_issuer AS upstream_issuer,
    owner_org.upstream_org_id AS upstream_org_id,
    owner_user.upstream_subject_id AS upstream_subject_id,
    s.managed_by,
    s.lifecycle,
    s.placement,
    s.activating_client_id,
    COALESCE(direct_route.current_node_id, s.active_node_id) AS active_node_id,
    COALESCE(active_direct.name, active_k8s.k8s_node_name) AS active_node_name,
    active_node.role AS active_node_role,
    host(active_node.address) AS active_node_address,
    COALESCE(direct_route.substrate_kind, active_node.managed_by) AS active_node_managed_by,
    COALESCE(active_execution.generation, direct_route.execution_generation)
        AS active_execution_generation,
    COALESCE(active_execution.pod_uid, direct_route.pod_uid) AS active_pod_uid,
    COALESCE(active_execution.route_activation_id, direct_route.activation_attempt_id)
        AS active_activation_attempt_id,
    COALESCE(ecw_credential.epoch, 1) AS ecw_credential_epoch,
    s.target_node_id,
    s.placement_changed_at,
    s.template_id,
    s.content_hash,
    s.vcpu,
    s.memory_mib,
    s.disk_mib,
    s.disk_io_max_operations_per_sec,
    s.disk_io_max_bandwidth_mib_per_sec,
    s.env,
    s.metadata,
    s.auto_lifecycle_json AS auto_lifecycle,
    s.auto_lifecycle_state_json AS auto_lifecycle_state,
    s.network_policy_override_json AS network_policy,
    s.network_policy_generation,
    COALESCE(s.volume_encryption, FALSE) AS volume_encryption,
    t.network_policy_json AS template_network_policy,
    t.network_policy_generation AS template_network_policy_generation,
    s.next_auto_event_at,
    s.snapshots_enabled_desired,
    s.command_ready_pending,
    s.last_used_at,
    s.last_access_at,
    s.last_command_at,
    s.created_at,
    s.updated_at,
    COALESCE(release_tags.tags, ARRAY[]::TEXT[]) AS release_tags
";

/// The joins [`SANDBOX_COLUMNS`] reads, onto `sandboxes s`.
const SANDBOX_JOIN_TAIL: &str = "
    JOIN users owner_user ON owner_user.id = s.user_id
    JOIN organizations owner_org ON owner_org.id = s.org_id
    LEFT JOIN templates t ON t.name = s.template_id
    LEFT JOIN sandbox_direct_route_identities direct_route
        ON direct_route.sandbox_id = s.id
    LEFT JOIN nodes active_node
        ON active_node.id = COALESCE(direct_route.current_node_id, s.active_node_id)
    LEFT JOIN node_direct active_direct ON active_direct.node_id = active_node.id
    LEFT JOIN node_k8s active_k8s ON active_k8s.node_id = active_node.id
    LEFT JOIN sandbox_ecw_credential_epochs ecw_credential
        ON ecw_credential.sandbox_id = s.id
    LEFT JOIN LATERAL (
        SELECT e.generation,
               NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
               e.route_activation_id
        FROM sandbox_k8s_executions e
        WHERE e.sandbox_id = s.id
          AND e.active
        ORDER BY e.generation DESC
        LIMIT 1
    ) active_execution ON TRUE
    LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(trt.tag ORDER BY trt.tag) AS tags
        FROM template_releases tr
        JOIN template_release_tags trt ON trt.template_release_id = tr.id
        WHERE tr.template_name = s.template_id
          AND tr.hash = s.content_hash
    ) release_tags ON TRUE
";

static BY_ID_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "SELECT {SANDBOX_COLUMNS}
         FROM sandboxes s
         {SANDBOX_JOIN_TAIL}
         WHERE s.id = $1"
    ))
});

/// One sandbox's response row, any lifecycle; `None` when there is no row.
pub async fn by_id(executor: impl Executor<'_>, id: SandboxId) -> Result<Option<Sandbox>, DbError> {
    Ok(
        sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(BY_ID_SQL.as_str()))
            .bind(id)
            .fetch_optional(executor.into_sqlx())
            .await?,
    )
}

/// The local org's sandboxes ($1) after the cursor ($2, $3), matching the
/// metadata filter $5, walked to at most $4 rows within the byte budget $6
/// with $7 bytes a row, as an ECW page. ECW reads a `deleted` tombstone as
/// not found, so tombstones are left out before the page and its lookahead,
/// as the native list leaves them out. The page counts `env` against the
/// budget.
static LOCAL_ORG_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "WITH RECURSIVE page_candidates AS MATERIALIZED (
             SELECT array_agg(candidate.id ORDER BY candidate.created_at DESC, candidate.id DESC)
                      AS ids
             FROM (SELECT s.id, s.created_at
                   FROM sandboxes s
                   JOIN users owner_user ON owner_user.id = s.user_id
                   WHERE owner_user.owner_source = 'local'
                     AND s.org_id = $1
                     AND s.lifecycle NOT IN ('deleted')
                     AND (s.created_at, s.id) < (COALESCE($2::TIMESTAMPTZ, 'infinity'),
                         COALESCE($3::UUID, '00000000-0000-0000-0000-000000000000'))
                     AND ($5::jsonb IS NULL OR s.metadata @> $5::jsonb)
                   ORDER BY s.created_at DESC, s.id DESC
                   LIMIT $4::BIGINT) candidate
         ),
         page_walk AS (
             SELECT NULL::UUID AS id, 0::BIGINT AS through_bytes, 0::BIGINT AS position
             UNION ALL
             SELECT sized.id,
                    walk.through_bytes
                      + octet_length(sized.metadata::TEXT)
                      + octet_length(sized.env::TEXT)
                      + COALESCE(octet_length(to_jsonb(sized.name)::TEXT), 0)
                      + $7::BIGINT,
                    walk.position + 1
             FROM page_walk walk
             CROSS JOIN page_candidates candidates
             JOIN sandboxes sized ON sized.id = candidates.ids[walk.position + 1]
             WHERE walk.through_bytes <= $6::BIGINT OR walk.position = 1
         ),
         page_rows AS (
             SELECT walk.id, walk.position, walk.has_next
             FROM (SELECT page_walk.id, page_walk.through_bytes, page_walk.position,
                          page_walk.position < max(page_walk.position) OVER () AS has_next
                   FROM page_walk) walk
             WHERE walk.position >= 1
               AND (walk.through_bytes <= $6::BIGINT OR walk.position = 1)
         )
         SELECT {SANDBOX_COLUMNS}, page.has_next
         FROM page_rows page
         JOIN sandboxes s ON s.id = page.id
         {SANDBOX_JOIN_TAIL}
         ORDER BY page.position"
    ))
});

/// One ECW page of the sandboxes of `org_id` that local users own, newest
/// first, matching `metadata_filter` (a JSON containment filter) when there
/// is one. `deleted` tombstones are left out.
pub async fn local_org_page(
    executor: impl Executor<'_>,
    org_id: OrgId,
    metadata_filter: Option<JsonValue>,
    walk: &PageWalk,
) -> Result<Vec<ScopedPageRow>, DbError> {
    Ok(
        sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(LOCAL_ORG_PAGE_SQL.as_str()))
            .bind(org_id)
            .bind(walk.cursor_created_at())
            .bind(walk.cursor_id())
            .bind(walk.max_rows)
            .bind(metadata_filter)
            .bind(walk.max_bytes)
            .bind(walk.row_overhead_bytes)
            .fetch_all(executor.into_sqlx())
            .await?,
    )
}

/// The federated subject's sandboxes ($1-$3) and the key org's legacy local
/// sandboxes ($8) after the cursor ($4, $5), matching the metadata filter
/// $7, walked to at most $6 rows within the byte budget $9 with $10 bytes a
/// row, as an ECW page. Each candidate arm keeps its own index-backed
/// `ORDER BY` / `LIMIT`; the outer sort merges the two candidate lists. Both
/// arms leave out the delete tombstones, as [`LOCAL_ORG_PAGE_SQL`] does.
static FEDERATED_SUBJECT_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "WITH RECURSIVE page_candidates AS MATERIALIZED (
             SELECT array_agg(candidate.id ORDER BY candidate.created_at DESC, candidate.id DESC)
                      AS ids
             FROM (SELECT candidate.id, candidate.created_at
                   FROM (
                       (SELECT s.id, s.created_at
                        FROM sandboxes s
                        JOIN users owner_user ON owner_user.id = s.user_id
                        JOIN organizations owner_org ON owner_org.id = s.org_id
                        WHERE owner_user.owner_source = 'federated_identity'
                          AND owner_user.upstream_issuer = $1
                          AND owner_org.upstream_issuer = $1
                          AND owner_org.upstream_org_id = $2
                          AND owner_user.upstream_subject_id = $3
                          AND s.lifecycle NOT IN ('deleted')
                          AND (s.created_at, s.id) < (COALESCE($4::TIMESTAMPTZ, 'infinity'),
                              COALESCE($5::UUID, '00000000-0000-0000-0000-000000000000'))
                          AND ($7::jsonb IS NULL OR s.metadata @> $7::jsonb)
                        ORDER BY s.created_at DESC, s.id DESC
                        LIMIT $6::BIGINT)
                       UNION ALL
                       (SELECT s.id, s.created_at
                        FROM sandboxes s
                        JOIN users owner_user ON owner_user.id = s.user_id
                        WHERE owner_user.owner_source = 'local'
                          AND s.org_id = $8
                          AND s.lifecycle NOT IN ('deleted')
                          AND (s.created_at, s.id) < (COALESCE($4::TIMESTAMPTZ, 'infinity'),
                              COALESCE($5::UUID, '00000000-0000-0000-0000-000000000000'))
                          AND ($7::jsonb IS NULL OR s.metadata @> $7::jsonb)
                        ORDER BY s.created_at DESC, s.id DESC
                        LIMIT $6::BIGINT)
                   ) candidate
                   ORDER BY candidate.created_at DESC, candidate.id DESC
                   LIMIT $6::BIGINT) candidate
         ),
         page_walk AS (
             SELECT NULL::UUID AS id, 0::BIGINT AS through_bytes, 0::BIGINT AS position
             UNION ALL
             SELECT sized.id,
                    walk.through_bytes
                      + octet_length(sized.metadata::TEXT)
                      + octet_length(sized.env::TEXT)
                      + COALESCE(octet_length(to_jsonb(sized.name)::TEXT), 0)
                      + $10::BIGINT,
                    walk.position + 1
             FROM page_walk walk
             CROSS JOIN page_candidates candidates
             JOIN sandboxes sized ON sized.id = candidates.ids[walk.position + 1]
             WHERE walk.through_bytes <= $9::BIGINT OR walk.position = 1
         ),
         page_rows AS (
             SELECT walk.id, walk.position, walk.has_next
             FROM (SELECT page_walk.id, page_walk.through_bytes, page_walk.position,
                          page_walk.position < max(page_walk.position) OVER () AS has_next
                   FROM page_walk) walk
             WHERE walk.position >= 1
               AND (walk.through_bytes <= $9::BIGINT OR walk.position = 1)
         )
         SELECT {SANDBOX_COLUMNS}, page.has_next
         FROM page_rows page
         JOIN sandboxes s ON s.id = page.id
         {SANDBOX_JOIN_TAIL}
         ORDER BY page.position"
    ))
});

/// The federated owner a [`federated_subject_page`] lists: its upstream
/// issuer, org and subject.
#[derive(Debug, Clone, Copy)]
pub struct FederatedSubject<'a> {
    pub issuer: &'a UpstreamIssuer,
    pub upstream_org_id: &'a UpstreamOrgId,
    pub subject_id: &'a UpstreamSubjectId,
}

/// One ECW page of the sandboxes `subject` owns and the local-owned sandboxes
/// of `legacy_org_id`, newest first, matching `metadata_filter` when there is
/// one. `deleted` tombstones are left out.
pub async fn federated_subject_page(
    executor: impl Executor<'_>,
    subject: FederatedSubject<'_>,
    legacy_org_id: OrgId,
    metadata_filter: Option<JsonValue>,
    walk: &PageWalk,
) -> Result<Vec<ScopedPageRow>, DbError> {
    Ok(
        sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(
            FEDERATED_SUBJECT_PAGE_SQL.as_str(),
        ))
        .bind(subject.issuer.as_str())
        .bind(subject.upstream_org_id.as_str())
        .bind(subject.subject_id.as_str())
        .bind(walk.cursor_created_at())
        .bind(walk.cursor_id())
        .bind(walk.max_rows)
        .bind(metadata_filter)
        .bind(legacy_org_id)
        .bind(walk.max_bytes)
        .bind(walk.row_overhead_bytes)
        .fetch_all(executor.into_sqlx())
        .await?,
    )
}
