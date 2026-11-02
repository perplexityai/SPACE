//! Postgres-side queries that back the admin sandbox list and the
//! spaced-version count.
//!
//! The handlers in [`super::sandbox_list`] stay focused on HTTP: parsing the
//! query string, auth, and the response shape. The statements live here.

use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, OrgId, SandboxId, TemplateName, UserId};
use pplx_space_placement::StoredSandboxLifecycle;
use pplx_space_public_api::PUBLIC_SANDBOX_ID_PREFIX;
use serde::Serialize;
use sqlx::{FromRow, PgExecutor};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::controllers::sandbox::{SandboxPageCursor, sandbox_query};
use crate::models::Sandbox;

/// What the admin sandbox list narrows to. Every field that is set must
/// match; an unset field matches every row.
#[derive(Debug, Clone, Default)]
pub(super) struct AdminSandboxFilter {
    pub(super) org_id: Option<OrgId>,
    /// The stored lifecycle (`sandboxes.lifecycle`), any of these. Never
    /// empty.
    pub(super) lifecycles: Option<Vec<StoredSandboxLifecycle>>,
    /// The row's own template (`sandboxes.template_id`).
    pub(super) template: Option<TemplateName>,
    pub(super) node: Option<NodeAssignment>,
    pub(super) user_id: Option<UserId>,
    /// Created strictly after this instant.
    pub(super) created_after: Option<OffsetDateTime>,
    /// Created strictly before this instant.
    pub(super) created_before: Option<OffsetDateTime>,
    pub(super) search: Option<SandboxSearch>,
}

impl AdminSandboxFilter {
    /// Whether no filter is set. Only the unfiltered list carries the
    /// node-reported rows with no sandbox row: they belong to no org, and
    /// none of the other filters can be applied to them.
    pub(super) fn is_unfiltered(&self) -> bool {
        let Self {
            org_id,
            lifecycles,
            template,
            node,
            user_id,
            created_after,
            created_before,
            search,
        } = self;
        org_id.is_none()
            && lifecycles.is_none()
            && template.is_none()
            && node.is_none()
            && user_id.is_none()
            && created_after.is_none()
            && created_before.is_none()
            && search.is_none()
    }
}

/// The node a sandbox is assigned to, as the list response reports it in
/// `active_node_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeAssignment {
    Node(NodeId),
    Assigned,
    Unassigned,
}

/// A name or ID prefix. A row matches when its name starts with the prefix
/// as given, or when the prefix, with an optional `sbx_` and dashes where a
/// UUID has them, is the start of its ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SandboxSearch {
    prefix: String,
    /// The lowest and highest IDs that start with the prefix, when it can
    /// start an ID. Postgres compares UUIDs byte by byte, so a hex prefix is
    /// exactly this range, which the primary key serves.
    id_range: Option<(SandboxId, SandboxId)>,
}

impl SandboxSearch {
    pub(super) fn new(prefix: String) -> Self {
        let id_range = id_prefix_range(&prefix);
        Self { prefix, id_range }
    }
}

const DASHED_UUID_DASH_POSITIONS: [usize; 4] = [8, 13, 18, 23];
const UNDASHED_UUID_LEN: usize = 32;

fn id_prefix_range(prefix: &str) -> Option<(SandboxId, SandboxId)> {
    let id = prefix
        .strip_prefix(PUBLIC_SANDBOX_ID_PREFIX)
        .unwrap_or(prefix);
    let dashed = id.contains('-');
    let mut hex = String::with_capacity(UNDASHED_UUID_LEN);
    for (position, character) in id.chars().enumerate() {
        if dashed && DASHED_UUID_DASH_POSITIONS.contains(&position) {
            if character != '-' {
                return None;
            }
        } else if character.is_ascii_hexdigit() {
            hex.push(character.to_ascii_lowercase());
        } else {
            return None;
        }
    }
    if hex.len() > UNDASHED_UUID_LEN {
        return None;
    }
    let bound = |fill: char| {
        let mut digits = hex.clone();
        digits.extend(std::iter::repeat_n(fill, UNDASHED_UUID_LEN - hex.len()));
        Uuid::try_parse(&digits).ok().map(SandboxId::from_uuid)
    };
    Some((bound('0')?, bound('f')?))
}

/// The admin list: newest first by `(created_at, id)`, after `after` when
/// set. `active_node_id` is the response's: the direct route's current node
/// when the sandbox has one, else the row's.
static SANDBOX_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    sandbox_query(
        "SELECT {sandbox_columns}
         FROM {sandbox_from}
         WHERE ($2::uuid IS NULL OR s.org_id = $2)
           AND (s.created_at, s.id) < (COALESCE($3::TIMESTAMPTZ, 'infinity'),
                                       COALESCE($4::UUID, '00000000-0000-0000-0000-000000000000'))
           AND ($5::TEXT[] IS NULL OR s.lifecycle = ANY($5::TEXT[]))
           AND ($6::TEXT IS NULL OR s.template_id = $6)
           AND ($7::UUID IS NULL OR COALESCE(direct_route.current_node_id, s.active_node_id) = $7)
           AND ($8::BOOLEAN IS NULL
                OR (COALESCE(direct_route.current_node_id, s.active_node_id) IS NOT NULL) = $8)
           AND ($9::UUID IS NULL OR s.user_id = $9)
           AND ($10::TIMESTAMPTZ IS NULL OR s.created_at > $10)
           AND ($11::TIMESTAMPTZ IS NULL OR s.created_at < $11)
           AND ($12::TEXT IS NULL
                OR starts_with(s.name, $12)
                OR s.id BETWEEN $13::UUID AND $14::UUID)
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT $1",
    )
});

/// At most `max_rows` sandboxes matching `filter`, newest first, after the
/// cursor when one is set.
pub(super) async fn list_sandboxes(
    executor: impl PgExecutor<'_>,
    filter: &AdminSandboxFilter,
    after: Option<SandboxPageCursor>,
    max_rows: i64,
) -> Result<Vec<Sandbox>, sqlx::Error> {
    let lifecycles = filter.lifecycles.as_ref().map(|lifecycles| {
        lifecycles
            .iter()
            .map(|lifecycle| lifecycle.as_str())
            .collect::<Vec<_>>()
    });
    let (node_id, assigned) = match filter.node {
        Some(NodeAssignment::Node(node_id)) => (Some(node_id), None),
        Some(NodeAssignment::Assigned) => (None, Some(true)),
        Some(NodeAssignment::Unassigned) => (None, Some(false)),
        None => (None, None),
    };
    let id_range = filter.search.as_ref().and_then(|search| search.id_range);
    sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(sql!(SANDBOX_PAGE_SQL.as_str())))
        .bind(max_rows)
        .bind(filter.org_id)
        .bind(after.map(|cursor| cursor.created_at))
        .bind(after.map(|cursor| cursor.id))
        .bind(lifecycles)
        .bind(filter.template.as_ref().map(TemplateName::as_str))
        .bind(node_id)
        .bind(assigned)
        .bind(filter.user_id)
        .bind(filter.created_after)
        .bind(filter.created_before)
        .bind(filter.search.as_ref().map(|search| search.prefix.as_str()))
        .bind(id_range.map(|(low, _)| low))
        .bind(id_range.map(|(_, high)| high))
        .fetch_all(executor)
        .await
}

/// How many sandboxes run each template at each `spaced` version, by stored
/// lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, FromRow, Serialize)]
pub(super) struct SpacedVersionCount {
    /// The row's template, else the one its node reports.
    pub(super) template: Option<String>,
    /// The version its node last reported; `None` before any report.
    pub(super) spaced_version: Option<String>,
    pub(super) lifecycle: String,
    pub(super) count: i64,
}

/// [`SpacedVersionCount`]s over every sandbox not deleted, in `org_id` when
/// set.
pub(super) async fn count_by_spaced_version(
    executor: impl PgExecutor<'_>,
    org_id: Option<OrgId>,
) -> Result<Vec<SpacedVersionCount>, sqlx::Error> {
    sqlx::query_as::<_, SpacedVersionCount>(sql!(
        "SELECT COALESCE(s.template_id, sos.template, sos.pool_template) AS template,
                sos.spaced_version,
                s.lifecycle,
                COUNT(*)::BIGINT AS count
         FROM sandboxes s
         LEFT JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
         WHERE s.lifecycle <> $1
           AND ($2::UUID IS NULL OR s.org_id = $2)
         GROUP BY 1, 2, 3
         ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3"
    ))
    .bind(StoredSandboxLifecycle::Deleted.as_str())
    .bind(org_id)
    .fetch_all(executor)
    .await
}

#[cfg(test)]
pub(super) mod tests;
