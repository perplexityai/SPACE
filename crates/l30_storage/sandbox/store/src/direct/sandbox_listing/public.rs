//! The public API's sandbox reads: one sandbox and a list page.
//!
//! Both decode [`PublicSandboxRow`] from one projection
//! (`PUBLIC_SANDBOX_SELECT`) and its joins (`PUBLIC_SANDBOX_JOINS`), private
//! to this module, which read whether the sandbox's current failure is the
//! one its node reports for the execution that runs it: Kubernetes' active
//! execution, or Direct's route. Both bind the facet, the failure payload's
//! keys and the two substrates as $1-$7. Control-plane retention keeps
//! deleted tombstones, but deleted public resources are not found, so
//! neither returns one, and a list page's lookahead never counts one.

use std::sync::LazyLock;

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_auto_lifecycle::AutoLifecyclePolicy;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_state_reconciliation::{
    FACET_SANDBOX_DESIRED_STATE, SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY,
    SANDBOX_FAILURE_EPISODE_KEY, SANDBOX_FAILURE_EXECUTION_GENERATION_KEY,
    SANDBOX_FAILURE_POD_UID_KEY,
};
use sqlx::postgres::PgArguments;
use sqlx::query::QueryAs;
use time::OffsetDateTime;

use super::PageWalk;
use crate::SandboxMetadata;

/// A sandbox as the public API reads it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PublicSandboxRow {
    pub id: SandboxId,
    pub name: Option<String>,
    pub lifecycle: String,
    /// The row is `failed` and its node reports that failure for the
    /// execution that runs it now.
    pub has_current_failure: bool,
    pub placement: String,
    /// The row's own `active_node_id`.
    pub active_node_id: Option<NodeId>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_mib: Option<i32>,
    #[sqlx(json)]
    pub metadata: SandboxMetadata,
    pub next_auto_event_at: Option<i64>,
    #[sqlx(json)]
    pub auto_lifecycle: AutoLifecyclePolicy,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    /// A page row is followed by another sandbox in list order, whether or
    /// not the page's byte budget let it in. Always false for [`by_id`].
    pub has_next: bool,
}

const PUBLIC_SANDBOX_SELECT: &str = "SELECT s.id,
            s.name,
            s.lifecycle,
            COALESCE(
                s.lifecycle = 'failed'
                AND LOWER(TRIM(sos.status)) = 'failed'
                AND (
                    (
                        s.managed_by = $6
                        AND EXISTS (
                            SELECT 1
                            FROM sandbox_k8s_executions execution
                            WHERE execution.sandbox_id = s.id
                              AND execution.active
                              AND failure_observation.observed_payload
                                  -> $2::TEXT
                                  @> jsonb_build_object(
                                      $3::TEXT, execution.generation,
                                      $4::TEXT, execution.pod_uid,
                                      $5::TEXT, execution.route_activation_id::TEXT
                                  )
                              AND execution.pod_uid IS NOT NULL
                              AND execution.route_activation_id IS NOT NULL
                        )
                    )
                    OR (
                        s.managed_by = $7
                        AND direct_route.sandbox_id IS NOT NULL
                        AND failure_observation.observed_payload
                            -> $2::TEXT
                            @> jsonb_build_object(
                                $3::TEXT, direct_route.execution_generation,
                                $5::TEXT, direct_route.activation_attempt_id::TEXT
                            )
                    )
                ),
                FALSE
            ) AS has_current_failure,
            s.placement,
            s.active_node_id,
            s.template_id,
            s.content_hash,
            s.vcpu,
            s.memory_mib,
            s.disk_mib,
            s.metadata,
            s.next_auto_event_at,
            s.auto_lifecycle_json AS auto_lifecycle,
            s.created_at,
            s.updated_at";

/// The joins of a public sandbox row onto `s`.
const PUBLIC_SANDBOX_JOINS: &str = "LEFT JOIN sandbox_direct_route_identities direct_route
       ON direct_route.sandbox_id = s.id
     LEFT JOIN sandbox_observed_states sos
       ON sos.sandbox_id = s.id
      AND sos.node_id = COALESCE(direct_route.current_node_id, s.active_node_id)
     LEFT JOIN node_facet_observed_status failure_observation
       ON failure_observation.node_id = COALESCE(direct_route.current_node_id, s.active_node_id)
      AND failure_observation.facet = $1
      AND failure_observation.element = s.id::TEXT";

/// Binds what [`PUBLIC_SANDBOX_SELECT`] and [`PUBLIC_SANDBOX_JOINS`] read as
/// $1-$7.
fn bind_failure_terms<'q>(
    query: QueryAs<'q, sqlx::Postgres, PublicSandboxRow, PgArguments>,
) -> QueryAs<'q, sqlx::Postgres, PublicSandboxRow, PgArguments> {
    query
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(SANDBOX_FAILURE_EPISODE_KEY)
        .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
        .bind(SANDBOX_FAILURE_POD_UID_KEY)
        .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
        .bind(SubstrateKind::Kubernetes)
        .bind(SubstrateKind::Space)
}

/// The sandbox $8 of org $9, unless it is a `deleted` tombstone.
static BY_ID_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "{PUBLIC_SANDBOX_SELECT}, FALSE AS has_next
             FROM sandboxes s
             {PUBLIC_SANDBOX_JOINS}
             WHERE s.id = $8 AND s.org_id = $9
               AND s.lifecycle NOT IN ('deleted')"
    ))
});

/// The sandbox `sandbox_id` of `org_id`; `None` when there is none or it is
/// a `deleted` tombstone.
pub async fn by_id(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    org_id: OrgId,
) -> Result<Option<PublicSandboxRow>, DbError> {
    Ok(
        bind_failure_terms(sqlx::query_as::<_, PublicSandboxRow>(sqlx::AssertSqlSafe(
            BY_ID_SQL.as_str(),
        )))
        .bind(sandbox_id.as_uuid())
        .bind(org_id)
        .fetch_optional(executor.into_sqlx())
        .await?,
    )
}

/// The org's ($8) live sandboxes, owned by subject $12 or, without one, by a
/// local user, after the cursor ($9, $10), walked to at most $11 rows within
/// the byte budget $13 with $14 bytes a row. The public sandbox has no env.
static PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "WITH RECURSIVE page_candidates AS MATERIALIZED (
             SELECT array_agg(candidate.id ORDER BY candidate.created_at DESC, candidate.id DESC)
                      AS ids
             FROM (SELECT s.id, s.created_at
                   FROM sandboxes s
                   WHERE s.org_id = $8
                     AND s.lifecycle NOT IN ('deleted')
                     AND (($12::UUID IS NOT NULL AND s.user_id = $12)
                       OR ($12::UUID IS NULL AND EXISTS (
                           SELECT 1 FROM users owner WHERE owner.id = s.user_id
                             AND owner.owner_source = 'local'
                       )))
                     AND (s.created_at, s.id) < (COALESCE($9::TIMESTAMPTZ, 'infinity'),
                         COALESCE($10::UUID, '00000000-0000-0000-0000-000000000000'))
                   ORDER BY s.created_at DESC, s.id DESC
                   LIMIT $11::BIGINT) candidate
         ),
         page_walk AS (
             SELECT NULL::UUID AS id, 0::BIGINT AS through_bytes, 0::BIGINT AS position
             UNION ALL
             SELECT sized.id,
                    walk.through_bytes
                      + octet_length(sized.metadata::TEXT)
                      + COALESCE(octet_length(to_jsonb(sized.name)::TEXT), 0)
                      + $14::BIGINT,
                    walk.position + 1
             FROM page_walk walk
             CROSS JOIN page_candidates candidates
             JOIN sandboxes sized ON sized.id = candidates.ids[walk.position + 1]
             WHERE walk.through_bytes <= $13::BIGINT OR walk.position = 1
         ),
         page_rows AS (
             SELECT walk.id, walk.position, walk.has_next
             FROM (SELECT page_walk.id, page_walk.through_bytes, page_walk.position,
                          page_walk.position < max(page_walk.position) OVER () AS has_next
                   FROM page_walk) walk
             WHERE walk.position >= 1
               AND (walk.through_bytes <= $13::BIGINT OR walk.position = 1)
         )
         {PUBLIC_SANDBOX_SELECT}, page.has_next
         FROM page_rows page
         JOIN sandboxes s ON s.id = page.id
         {PUBLIC_SANDBOX_JOINS}
         ORDER BY page.position"
    ))
});

/// One public list page of `org_id`'s sandboxes, newest first by
/// `(created_at, id)`: those `subject_id` owns, or without one those local
/// users own, after `walk`'s cursor and within its byte budget. `deleted`
/// tombstones are left out.
pub async fn page(
    executor: impl Executor<'_>,
    org_id: OrgId,
    subject_id: Option<UserId>,
    walk: &PageWalk,
) -> Result<Vec<PublicSandboxRow>, DbError> {
    Ok(
        bind_failure_terms(sqlx::query_as::<_, PublicSandboxRow>(sqlx::AssertSqlSafe(
            PAGE_SQL.as_str(),
        )))
        .bind(org_id)
        .bind(walk.cursor_created_at())
        .bind(walk.cursor_id())
        .bind(walk.max_rows)
        .bind(subject_id)
        .bind(walk.max_bytes)
        .bind(walk.row_overhead_bytes)
        .fetch_all(executor.into_sqlx())
        .await?,
    )
}
