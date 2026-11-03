//! The admin sandbox list's filters and pages, and its count of sandboxes by
//! template and `spaced` version.
//!
//! A page is the ECW list's page walk ([`super`]) over the admin
//! candidate statement below, composed with the same full-row select
//! (`org::scoped_page_sql`), so both lists resolve rows and stop at the byte
//! budget the same way. The admin route's legacy unpaged array has no cursor
//! to continue from, so it runs a plain list statement with the same filters
//! and no size walk.

use std::str::FromStr;
use std::sync::LazyLock;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    NodeId, OrgId, SandboxId, SandboxLifecycle, SandboxLifecycleParseError, SemVer, TemplateName,
    UserId,
};
use pplx_space_public_api::PUBLIC_SANDBOX_ID_PREFIX;
use serde::Serialize;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use super::super::org::{ScopedPageRow, scoped_page_sql};
use super::{
    ListPage, MAX_SANDBOX_LIST_PAGE_BYTES, PageWalkParams, SANDBOX_LIST_ROW_OVERHEAD_BYTES,
};
use crate::controllers::sandbox::{SandboxController, SandboxPageCursor, sandbox_query};
use crate::controllers::substrate::direct::route_overrides::{
    OverrideScope, RouteOverrides, route_overrides,
};
use crate::controllers::{ControllerError, db_contention_error};
use crate::models::{Sandbox, SandboxResourceType};

const DELETED_LIFECYCLE: &str = "deleted";

/// The `sandboxes.lifecycle` values outside [`SandboxLifecycle`].
const TOMBSTONE_LIFECYCLES: [&str; 2] = [DELETED_LIFECYCLE, "cleanup_required"];

/// A stored lifecycle (`sandboxes.lifecycle`) the admin list filters on: a
/// [`SandboxLifecycle`] state or a tombstone, in the column's form.
///
/// `pplx-space-placement`'s `StoredSandboxLifecycle` names the same values,
/// but this crate is not one of that store's users; the tests pin this parse
/// to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminStoredLifecycle(&'static str);

impl AdminStoredLifecycle {
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl FromStr for AdminStoredLifecycle {
    type Err = SandboxLifecycleParseError;

    /// Normalizes case and surrounding whitespace, as `StoredSandboxLifecycle`
    /// does.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        if let Some(tombstone) = TOMBSTONE_LIFECYCLES
            .into_iter()
            .find(|tombstone| *tombstone == normalized)
        {
            return Ok(Self(tombstone));
        }
        normalized
            .parse::<SandboxLifecycle>()
            .map(|lifecycle| Self(lifecycle.as_str()))
    }
}

/// What the admin sandbox list narrows to. Every field that is set must
/// match; an unset field matches every row.
#[derive(Debug, Clone, Default)]
pub struct AdminSandboxFilter {
    pub org_id: Option<OrgId>,
    /// The stored lifecycle (`sandboxes.lifecycle`), any of these. No
    /// response field shows it: `lifecycle` and `status` report the node's
    /// observed status when there is one. Never empty.
    pub lifecycles: Option<Vec<AdminStoredLifecycle>>,
    /// The row's own template (`sandboxes.template_id`), not the fallbacks
    /// the response's `template` resolves when the row has none.
    pub template: Option<TemplateName>,
    pub node: Option<NodeAssignment>,
    pub user_id: Option<UserId>,
    /// Created strictly after this instant.
    pub created_after: Option<OffsetDateTime>,
    /// Created strictly before this instant.
    pub created_before: Option<OffsetDateTime>,
    pub search: Option<SandboxSearch>,
}

impl AdminSandboxFilter {
    /// Whether no filter is set. Only then does the list carry the
    /// node-reported rows with no sandbox row (pool slots and observed-local
    /// sandboxes). They are not in the keyset order the cursor walks, and the
    /// filters are written against sandbox rows, so every filter leaves them
    /// out, even one that could describe them: `template`, or
    /// [`NodeAssignment::Unassigned`], since their response reports no
    /// `active_node_id`.
    pub fn is_unfiltered(&self) -> bool {
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
/// `active_node_id`: its Direct route's current node when it has a route,
/// else its row's `active_node_id`. Like every filter, it lists sandbox rows
/// only, so [`Self::Unassigned`] leaves out the node-reported rows that the
/// unfiltered list appends (see [`AdminSandboxFilter::is_unfiltered`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeAssignment {
    Node(NodeId),
    Assigned,
    Unassigned,
}

/// A name or ID prefix. A row matches when its name starts with the prefix
/// as given, or when the prefix, with an optional `sbx_` and dashes where a
/// UUID has them, is the start of its ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxSearch {
    prefix: String,
    /// The lowest and highest IDs that start with the prefix, when it can
    /// start an ID. Postgres compares UUIDs byte by byte, so a hex prefix is
    /// exactly this range.
    id_range: Option<(SandboxId, SandboxId)>,
}

impl SandboxSearch {
    pub fn new(prefix: String) -> Self {
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

/// The admin list's sandboxes ($1-$13) after the cursor, for the page walk.
/// The filters are the same as [`ADMIN_LIST_SQL`]'s. The node filters ($10,
/// $13) match the node the response reports, from a [`RouteOverrides`] read
/// in the statement's snapshot: $11 holds the sandboxes whose Direct route's
/// node differs from their row's `active_node_id`, and $12 those of them
/// routed to $10. Every other sandbox has no route or one on its row's node,
/// so its row decides.
const ADMIN_CANDIDATE_ROWS: &str = "SELECT s.id, s.created_at
    FROM sandboxes s
    WHERE ($1::UUID IS NULL OR s.org_id = $1)
      AND ($2::TEXT[] IS NULL OR s.lifecycle = ANY($2::TEXT[]))
      AND ($3::TEXT IS NULL OR s.template_id = $3)
      AND ($4::UUID IS NULL OR s.user_id = $4)
      AND ($5::TIMESTAMPTZ IS NULL OR s.created_at > $5)
      AND ($6::TIMESTAMPTZ IS NULL OR s.created_at < $6)
      AND ($7::TEXT IS NULL
           OR starts_with(s.name, $7)
           OR s.id BETWEEN $8::UUID AND $9::UUID)
      AND ($10::UUID IS NULL
           OR s.id = ANY($12::UUID[])
           OR (s.active_node_id = $10 AND s.id <> ALL($11::UUID[])))
      AND ($13::BOOLEAN IS NULL
           OR (s.id = ANY($11::UUID[]) OR s.active_node_id IS NOT NULL) = $13)
      AND {after_cursor}
    ORDER BY s.created_at DESC, s.id DESC
    LIMIT {max_rows}";

static ADMIN_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    scoped_page_sql(
        ADMIN_CANDIDATE_ROWS,
        &PageWalkParams {
            cursor_created_at: "$14",
            cursor_id: "$15",
            max_rows: "$16",
            max_bytes: "$17",
            row_overhead_bytes: "$18",
            // The admin response carries no `env`.
            lists_env: false,
        },
    )
});

/// The legacy unpaged list: at most $14 sandboxes matching the filters
/// ($1-$13, as [`ADMIN_CANDIDATE_ROWS`]'s), newest first.
static ADMIN_LIST_SQL: LazyLock<String> = LazyLock::new(|| {
    sandbox_query(
        "SELECT {sandbox_columns}
         FROM {sandbox_from}
         WHERE ($1::UUID IS NULL OR s.org_id = $1)
           AND ($2::TEXT[] IS NULL OR s.lifecycle = ANY($2::TEXT[]))
           AND ($3::TEXT IS NULL OR s.template_id = $3)
           AND ($4::UUID IS NULL OR s.user_id = $4)
           AND ($5::TIMESTAMPTZ IS NULL OR s.created_at > $5)
           AND ($6::TIMESTAMPTZ IS NULL OR s.created_at < $6)
           AND ($7::TEXT IS NULL
                OR starts_with(s.name, $7)
                OR s.id BETWEEN $8::UUID AND $9::UUID)
           AND ($10::UUID IS NULL
                OR s.id = ANY($12::UUID[])
                OR (s.active_node_id = $10 AND s.id <> ALL($11::UUID[])))
           AND ($13::BOOLEAN IS NULL
                OR (s.id = ANY($11::UUID[]) OR s.active_node_id IS NOT NULL) = $13)
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT $14",
    )
});

/// Whether a page stops at the list byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageBudget {
    /// [`MAX_SANDBOX_LIST_PAGE_BYTES`], for a page a cursor continues: the
    /// page walk.
    Bounded,
    /// No byte budget and no size walk: the legacy unpaged array, which has
    /// no cursor to continue from and answered up to its limit before paging
    /// existed.
    Unbounded,
}

/// How many sandboxes run each template at each `spaced` version, by the
/// status and resource type the admin list shows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SpacedVersionCount {
    /// The list response's `template`.
    pub template: Option<String>,
    /// The list response's `spaced_version`: what the node last reported.
    pub spaced_version: Option<SemVer>,
    /// The list response's `status` (and `lifecycle`): the node's observed
    /// status, else the stored lifecycle.
    pub status: Option<String>,
    pub resource_type: SandboxResourceType,
    pub count: i64,
}

#[derive(sqlx::FromRow)]
struct SpacedVersionCountRow {
    template: Option<String>,
    spaced_version: Option<SemVer>,
    status: Option<String>,
    has_row: bool,
    pool_slot: bool,
    count: i64,
}

impl SandboxController {
    /// One admin list page matching `filter`, newest first by `(created_at,
    /// id)`, after `cursor`: at most `limit` rows, and whether more follow.
    /// A [`PageBudget::Unbounded`] list ignores `cursor` and never says more
    /// follow.
    ///
    /// A node filter reads the Direct routes it needs and runs the page in
    /// one snapshot, so a route that moves between the two reads cannot list
    /// a sandbox under one node while its response reports another.
    pub(crate) async fn admin_sandbox_page(
        &self,
        pool: &PgPool,
        filter: &AdminSandboxFilter,
        cursor: Option<SandboxPageCursor>,
        limit: u32,
        budget: PageBudget,
    ) -> Result<ListPage<Sandbox>, sqlx::Error> {
        let Some(node) = filter.node else {
            return admin_page_statement(
                pool,
                filter,
                &RouteOverrides::default(),
                cursor,
                limit,
                budget,
            )
            .await;
        };
        let mut snapshot = begin_node_snapshot(pool).await?;
        let overrides = node_route_overrides(&mut snapshot, node).await?;
        let page =
            admin_page_statement(&mut *snapshot, filter, &overrides, cursor, limit, budget).await?;
        snapshot.commit().await?;
        Ok(page)
    }

    /// Every listed sandbox whose status is not `deleted`, counted by the
    /// template, `spaced` version, status and resource type the admin list
    /// shows for it. Without `org_id` that includes the rows nodes report
    /// with no sandbox row, as the unscoped list does.
    ///
    /// The template and status follow the list's projection
    /// (`sandbox_to_cached_response`, `observed_only_sandbox_response` and
    /// `fetch_sandbox_observed_states`): the row's template, else the node's,
    /// else the template the node's inventory holds for the image it runs,
    /// else the pool's; the node's status, else the stored lifecycle.
    pub async fn admin_spaced_version_counts(
        &self,
        pool: &PgPool,
        org_id: Option<OrgId>,
    ) -> Result<Vec<SpacedVersionCount>, ControllerError> {
        let rows = sqlx::query_as::<_, SpacedVersionCountRow>(sql!(
            "SELECT counted.template,
                    counted.spaced_version,
                    counted.status,
                    counted.has_row,
                    counted.pool_slot,
                    COUNT(*)::BIGINT AS count
             FROM (
                 SELECT COALESCE(s.template_id, sos.template, observed_template.template_name,
                                 sos.pool_template) AS template,
                        sos.spaced_version,
                        COALESCE(sos.status, s.lifecycle) AS status,
                        s.id IS NOT NULL AS has_row,
                        s.id IS NULL AND sos.pool_template IS NOT NULL AS pool_slot
                 FROM sandboxes s
                 FULL JOIN sandbox_observed_states sos ON sos.sandbox_id = s.id
                 LEFT JOIN LATERAL (
                     SELECT nti.template_name
                     FROM node_template_inventory nti
                     WHERE nti.node_id = sos.node_id
                       AND sos.image_hash IS NOT NULL
                       AND nti.content_hash = sos.image_hash
                     ORDER BY nti.observed_at DESC, nti.template_name ASC
                     LIMIT 1
                 ) observed_template ON TRUE
                 WHERE ($1::UUID IS NULL OR s.org_id = $1)
             ) counted
             WHERE counted.status IS DISTINCT FROM $2
             GROUP BY 1, 2, 3, 4, 5
             ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3 NULLS LAST, 4 DESC, 5"
        ))
        .bind(org_id)
        .bind(DELETED_LIFECYCLE)
        .fetch_all(pool)
        .await
        .map_err(|err| db_contention_error("count sandboxes by spaced version", &err))?;
        Ok(rows
            .into_iter()
            .map(|row| SpacedVersionCount {
                template: row.template,
                spaced_version: row.spaced_version,
                status: row.status,
                resource_type: match (row.has_row, row.pool_slot) {
                    (true, _) => SandboxResourceType::Sandbox,
                    (false, true) => SandboxResourceType::PoolSlot,
                    (false, false) => SandboxResourceType::ObservedLocal,
                },
                count: row.count,
            })
            .collect())
    }
}

/// The read-only transaction a node-filtered page runs in. `REPEATABLE READ`
/// so the route read and the page statement see one snapshot.
async fn begin_node_snapshot(pool: &PgPool) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    pool.begin_with(sql!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        .await
}

/// The routes `node` needs: the ones that put a sandbox on another node than
/// its row's.
async fn node_route_overrides(
    snapshot: &mut PgConnection,
    node: NodeAssignment,
) -> Result<RouteOverrides, sqlx::Error> {
    let scope = match node {
        NodeAssignment::Node(node_id) => OverrideScope::Node(node_id),
        NodeAssignment::Assigned | NodeAssignment::Unassigned => OverrideScope::Assignment,
    };
    route_overrides(snapshot, scope).await
}

/// The page statement, with the route overrides a node filter needs; a
/// filter without one ignores them.
async fn admin_page_statement(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &AdminSandboxFilter,
    overrides: &RouteOverrides,
    cursor: Option<SandboxPageCursor>,
    limit: u32,
    budget: PageBudget,
) -> Result<ListPage<Sandbox>, sqlx::Error> {
    let lifecycles = filter.lifecycles.as_ref().map(|lifecycles| {
        lifecycles
            .iter()
            .map(|lifecycle| lifecycle.as_str())
            .collect::<Vec<_>>()
    });
    let id_range = filter.search.as_ref().and_then(|search| search.id_range);
    let template = filter.template.as_ref().map(TemplateName::as_str);
    let prefix = filter.search.as_ref().map(|search| search.prefix.as_str());
    let (node_id, assigned) = match filter.node {
        Some(NodeAssignment::Node(node_id)) => (Some(node_id), None),
        Some(NodeAssignment::Assigned) => (None, Some(true)),
        Some(NodeAssignment::Unassigned) => (None, Some(false)),
        None => (None, None),
    };
    if budget == PageBudget::Unbounded {
        let rows = sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(sql!(ADMIN_LIST_SQL.as_str())))
            .bind(filter.org_id)
            .bind(lifecycles)
            .bind(template)
            .bind(filter.user_id)
            .bind(filter.created_after)
            .bind(filter.created_before)
            .bind(prefix)
            .bind(id_range.map(|(low, _)| low))
            .bind(id_range.map(|(_, high)| high))
            .bind(node_id)
            .bind(&overrides.overridden)
            .bind(&overrides.on_node)
            .bind(assigned)
            .bind(i64::from(limit))
            .fetch_all(executor)
            .await?;
        return Ok(ListPage {
            rows,
            has_more: false,
        });
    }
    let rows =
        sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(sql!(ADMIN_PAGE_SQL.as_str())))
            .bind(filter.org_id)
            .bind(lifecycles)
            .bind(template)
            .bind(filter.user_id)
            .bind(filter.created_after)
            .bind(filter.created_before)
            .bind(prefix)
            .bind(id_range.map(|(low, _)| low))
            .bind(id_range.map(|(_, high)| high))
            .bind(node_id)
            .bind(&overrides.overridden)
            .bind(&overrides.on_node)
            .bind(assigned)
            .bind(cursor.map(|cursor| cursor.created_at))
            .bind(cursor.map(|cursor| cursor.id))
            .bind(i64::from(limit.saturating_add(1)))
            .bind(MAX_SANDBOX_LIST_PAGE_BYTES)
            .bind(SANDBOX_LIST_ROW_OVERHEAD_BYTES)
            .fetch_all(executor)
            .await?;
    let page = ListPage::from_rows(rows, limit as usize, |row| row.has_next);
    Ok(ListPage {
        rows: page.rows.into_iter().map(|row| row.sandbox).collect(),
        has_more: page.has_more,
    })
}

#[cfg(test)]
pub(crate) mod tests;
