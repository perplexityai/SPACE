//! The admin sandbox list's filters and pages, and its count of sandboxes by
//! template and `spaced` version.
//!
//! A page is the ECW list's page walk ([`super`]) over the admin
//! candidate statement below, composed with the same full-row select
//! (`org::scoped_page_sql`), so both lists resolve rows and stop at the byte
//! budget the same way. The admin route's legacy unpaged array has no cursor
//! to continue from, so it runs a plain list statement with the same filters
//! and no size walk.
//!
//! A [`SandboxSearch`] runs its own pair of statements, the same filters
//! with the search required rather than optional. The other filters are
//! written `$n IS NULL OR ...`, which a generic plan cannot drop, so it reads
//! the list in `created_at` order and tests every row. That is cheap for a
//! filter most rows pass and for a page that fills early, but a search
//! usually matches a few rows of a large table, and walking the table to find
//! them reads all of it. Required, the search's two ranges are index
//! conditions on `idx_sandboxes_id_suffix` and the primary key under either
//! plan. They are read first, in a `MATERIALIZED` CTE, and the other filters
//! and the cursor apply to those rows only: unfenced, a generic plan ANDs the
//! search's bitmap with one for the cursor's `created_at` bound, which on the
//! first page holds every row. So a search reads the rows it matches and
//! sorts those (`search_plan_tests`).

use std::str::FromStr;
use std::sync::LazyLock;

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_cluster_pool::WarmPoolStore;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::{
    SandboxId, SandboxLifecycle, SandboxLifecycleParseError, TemplateName,
};
use pplx_space_public_api::PUBLIC_SANDBOX_ID_PREFIX;
use pplx_space_semver::SemVer;
use serde::Serialize;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use super::super::org::{ScopedPageRow, scoped_page_sql};
use super::{ListPage, PageWalkParams, SANDBOX_LIST_ROW_OVERHEAD_BYTES};
use crate::controllers::sandbox::{SandboxController, SandboxPageCursor, sandbox_query};
use crate::controllers::substrate::direct::reported_node::{reported_on_a_node, reported_on_node};
use crate::controllers::{ControllerError, db_contention_error, pool_store_error};
use crate::models::{Sandbox, SandboxResourceType};

const DELETED_LIFECYCLE: &str = "deleted";

/// The `sandboxes.lifecycle` values outside [`SandboxLifecycle`].
const TOMBSTONE_LIFECYCLES: [&str; 1] = [DELETED_LIFECYCLE];

/// Values no writer stores any more that the filter still takes, for one
/// release, so an older console or a saved link gets an empty page rather
/// than a 400. They match no row. Remove them when the cleanup states retire.
const RETIRED_LIFECYCLES: [&str; 1] = ["cleanup_required"];

/// A stored lifecycle (`sandboxes.lifecycle`) the admin list filters on: a
/// [`SandboxLifecycle`] state or a tombstone, in the column's form.
///
/// `pplx-space-sandbox-store`'s `StoredSandboxLifecycle` names the same values
/// and normalizes them the same way; the tests pin this parse to it.
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
            .chain(RETIRED_LIFECYCLES)
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
    /// Sandbox rows only: a prefix can start a node-reported row's ID, but
    /// like every filter it leaves those rows out.
    pub prefix: Option<SandboxPrefix>,
    /// Sandbox rows only, as `prefix`.
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
            prefix,
            search,
        } = self;
        org_id.is_none()
            && lifecycles.is_none()
            && template.is_none()
            && node.is_none()
            && user_id.is_none()
            && created_after.is_none()
            && created_before.is_none()
            && prefix.is_none()
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

/// A name or ID prefix. A sandbox row matches when its name starts with the
/// prefix as given, or when the prefix, with an optional `sbx_` and dashes
/// where a UUID has them, is the start of its ID. The node-reported rows
/// never match (see [`AdminSandboxFilter::is_unfiltered`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxPrefix {
    prefix: String,
    /// The lowest and highest IDs that start with the prefix, when it can
    /// start an ID. Postgres compares UUIDs byte by byte, so a hex prefix is
    /// exactly this range.
    id_range: Option<(SandboxId, SandboxId)>,
}

impl SandboxPrefix {
    pub fn new(prefix: String) -> Self {
        let id_range = id_prefix_range(&prefix);
        Self { prefix, id_range }
    }
}

/// What the console's search box finds: a sandbox row whose ID ends or starts
/// with the text's hex digits. The ID's end is what the console labels a
/// sandbox with, since a UUIDv7 starts with its creation time. Names are
/// matched by [`SandboxPrefix`] instead.
///
/// The text is trimmed, and its hex digits are those left after an optional
/// `sbx_` in either case and its dashes; any other character, or fewer than
/// [`MIN_ID_SUFFIX_DIGITS`] or more than 32 digits, is [`NotAnIdSearch`]. IDs
/// that end with them match from [`MIN_ID_SUFFIX_DIGITS`] digits, which are
/// random. IDs that start with them match from [`MIN_ID_PREFIX_DIGITS`]: a
/// UUIDv7 starts with its creation time in milliseconds, so four digits span
/// about 50 days of sandboxes and eight about a minute. The node-reported
/// rows never match (see [`AdminSandboxFilter::is_unfiltered`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxSearch {
    /// The IDs ending in the digits, as a range of their undashed hex
    /// reversed (`idx_sandboxes_id_suffix`): at least the reversed digits and
    /// below them with their last digit incremented.
    id_suffix: (String, String),
    /// The lowest and highest IDs that start with the digits, when there are
    /// at least [`MIN_ID_PREFIX_DIGITS`] of them.
    id_prefix: Option<(SandboxId, SandboxId)>,
}

/// A search text that, once trimmed, is not [`MIN_ID_SUFFIX_DIGITS`] to 32
/// of an ID's hex digits (see [`SandboxSearch`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "search takes {MIN_ID_SUFFIX_DIGITS} to {UNDASHED_UUID_LEN} hex digits of a sandbox ID, after \
     an optional sbx_ and with dashes left out: IDs that end with them match, and from \
     {MIN_ID_PREFIX_DIGITS} digits IDs that start with them too. To match the start of a name, \
     use prefix, which is case-sensitive"
)]
pub struct NotAnIdSearch;

impl SandboxSearch {
    /// `None` for a text that is empty once trimmed, which is no search.
    pub fn new(text: &str) -> Result<Option<Self>, NotAnIdSearch> {
        let text = text.trim();
        if text.is_empty() {
            return Ok(None);
        }
        let hex = id_hex(text).ok_or(NotAnIdSearch)?;
        let id_prefix = if hex.len() >= MIN_ID_PREFIX_DIGITS {
            hex_prefix_range(&hex)
        } else {
            None
        };
        Ok(Some(Self {
            id_suffix: id_suffix_range(&hex),
            id_prefix,
        }))
    }
}

const MIN_ID_SUFFIX_DIGITS: usize = 4;
const MIN_ID_PREFIX_DIGITS: usize = 8;

/// The text's hex digits, lowercased, when after an optional `sbx_` and
/// without its dashes it is [`MIN_ID_SUFFIX_DIGITS`] to 32 of them.
fn id_hex(text: &str) -> Option<String> {
    let marker = PUBLIC_SANDBOX_ID_PREFIX.len();
    let id = match (text.get(..marker), text.get(marker..)) {
        (Some(prefix), Some(rest)) if prefix.eq_ignore_ascii_case(PUBLIC_SANDBOX_ID_PREFIX) => rest,
        _ => text,
    };
    let hex = id
        .chars()
        .filter(|character| *character != '-')
        .map(|character| character.to_ascii_lowercase())
        .collect::<String>();
    ((MIN_ID_SUFFIX_DIGITS..=UNDASHED_UUID_LEN).contains(&hex.len())
        && hex.chars().all(|character| character.is_ascii_hexdigit()))
    .then_some(hex)
}

/// The reversed digits, and the least key above every key that starts with
/// them. The digits are ASCII, so that is them with their last byte
/// incremented: `9` becomes `:` and `f` becomes `g`.
fn id_suffix_range(hex: &str) -> (String, String) {
    let low = hex.bytes().rev().collect::<Vec<_>>();
    let mut high = low.clone();
    if let Some(last) = high.last_mut() {
        *last += 1;
    }
    let text = |bytes: Vec<u8>| bytes.into_iter().map(char::from).collect::<String>();
    (text(low), text(high))
}

fn hex_prefix_range(hex: &str) -> Option<(SandboxId, SandboxId)> {
    let bound = |fill: char| {
        let mut digits = hex.to_string();
        digits.extend(std::iter::repeat_n(fill, UNDASHED_UUID_LEN - hex.len()));
        Uuid::try_parse(&digits).ok().map(SandboxId::from_uuid)
    };
    Some((bound('0')?, bound('f')?))
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

/// The admin list's sandboxes ($1-$11) after the cursor, for the page walk.
/// The filters are the same as [`ADMIN_LIST_SQL`]'s. This statement reads
/// `sandboxes` alone, so the node filters ($10, $11) take Direct's predicates
/// for the node the response reports. Built once: every input is a literal or
/// a bind placeholder.
static ADMIN_CANDIDATE_ROWS: LazyLock<String> = LazyLock::new(|| {
    let on_node = reported_on_node("s", "$10");
    let on_a_node = reported_on_a_node("s");
    format!(
        "SELECT s.id, s.created_at
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
           AND ($10::UUID IS NULL OR {on_node})
           AND ($11::BOOLEAN IS NULL OR {on_a_node} = $11)
           AND {{after_cursor}}
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT {{max_rows}}"
    )
});

static ADMIN_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    scoped_page_sql(
        &ADMIN_CANDIDATE_ROWS,
        &PageWalkParams {
            cursor_created_at: "$12",
            cursor_id: "$13",
            max_rows: "$14",
            max_bytes: "$15",
            row_overhead_bytes: "$16",
            // The admin response carries no `env`.
            lists_env: false,
        },
    )
});

/// The legacy unpaged list: at most $12 sandboxes matching the filters
/// ($1-$11, as [`ADMIN_CANDIDATE_ROWS`]'s, with the same Direct predicates),
/// newest first. `{sandbox_from}` joins the Direct route for the response, so
/// the predicates look a filtered row's route up again, by its primary key.
/// Built once, as above.
static ADMIN_LIST_SQL: LazyLock<String> = LazyLock::new(|| {
    let on_node = reported_on_node("s", "$10");
    let on_a_node = reported_on_a_node("s");
    sandbox_query(&format!(
        "SELECT {{sandbox_columns}}
         FROM {{sandbox_from}}
         WHERE ($1::UUID IS NULL OR s.org_id = $1)
           AND ($2::TEXT[] IS NULL OR s.lifecycle = ANY($2::TEXT[]))
           AND ($3::TEXT IS NULL OR s.template_id = $3)
           AND ($4::UUID IS NULL OR s.user_id = $4)
           AND ($5::TIMESTAMPTZ IS NULL OR s.created_at > $5)
           AND ($6::TIMESTAMPTZ IS NULL OR s.created_at < $6)
           AND ($7::TEXT IS NULL
                OR starts_with(s.name, $7)
                OR s.id BETWEEN $8::UUID AND $9::UUID)
           AND ($10::UUID IS NULL OR {on_node})
           AND ($11::BOOLEAN IS NULL OR {on_a_node} = $11)
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT $12"
    ))
});

/// [`ADMIN_CANDIDATE_ROWS`] with a [`SandboxSearch`] ($12-$15) required: the
/// sandboxes whose ID ends or starts with its hex digits.
///
/// The suffix arm reads `idx_sandboxes_id_suffix` in [$12, $13), the
/// reversed digits' range; the prefix arm reads the primary key in [$14,
/// $15], which is NULL for too few digits to start an ID with (see
/// [`SandboxSearch`]).
static ADMIN_SEARCH_CANDIDATE_ROWS: LazyLock<String> = LazyLock::new(|| {
    let on_node = reported_on_node("s", "$10");
    let on_a_node = reported_on_a_node("s");
    format!(
        "WITH matches AS MATERIALIZED (
             SELECT matched.id
             FROM sandboxes matched
             WHERE (reverse(replace(matched.id::text, '-', '')) COLLATE \"C\" >= $12
                    AND reverse(replace(matched.id::text, '-', '')) COLLATE \"C\" < $13)
                OR matched.id BETWEEN $14::UUID AND $15::UUID
         )
         SELECT s.id, s.created_at
         FROM matches
         JOIN sandboxes s ON s.id = matches.id
         WHERE ($1::UUID IS NULL OR s.org_id = $1)
           AND ($2::TEXT[] IS NULL OR s.lifecycle = ANY($2::TEXT[]))
           AND ($3::TEXT IS NULL OR s.template_id = $3)
           AND ($4::UUID IS NULL OR s.user_id = $4)
           AND ($5::TIMESTAMPTZ IS NULL OR s.created_at > $5)
           AND ($6::TIMESTAMPTZ IS NULL OR s.created_at < $6)
           AND ($7::TEXT IS NULL
                OR starts_with(s.name, $7)
                OR s.id BETWEEN $8::UUID AND $9::UUID)
           AND ($10::UUID IS NULL OR {on_node})
           AND ($11::BOOLEAN IS NULL OR {on_a_node} = $11)
           AND {{after_cursor}}
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT {{max_rows}}"
    )
});

static ADMIN_SEARCH_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    scoped_page_sql(
        &ADMIN_SEARCH_CANDIDATE_ROWS,
        &PageWalkParams {
            cursor_created_at: "$16",
            cursor_id: "$17",
            max_rows: "$18",
            max_bytes: "$19",
            row_overhead_bytes: "$20",
            lists_env: false,
        },
    )
});

/// [`ADMIN_LIST_SQL`] with a [`SandboxSearch`] required, as
/// [`ADMIN_SEARCH_CANDIDATE_ROWS`] matches it ($12-$15): at most $16
/// sandboxes. `listed` picks them by ID before their full rows are joined,
/// as the page walk does, so a search that matches many rows sorts their IDs
/// rather than their joined rows.
static ADMIN_SEARCH_LIST_SQL: LazyLock<String> = LazyLock::new(|| {
    let on_node = reported_on_node("s", "$10");
    let on_a_node = reported_on_a_node("s");
    sandbox_query(&format!(
        "WITH matches AS MATERIALIZED (
             SELECT matched.id
             FROM sandboxes matched
             WHERE (reverse(replace(matched.id::text, '-', '')) COLLATE \"C\" >= $12
                    AND reverse(replace(matched.id::text, '-', '')) COLLATE \"C\" < $13)
                OR matched.id BETWEEN $14::UUID AND $15::UUID
         ),
         listed AS MATERIALIZED (
             SELECT s.id, s.created_at
             FROM matches
             JOIN sandboxes s ON s.id = matches.id
             WHERE ($1::UUID IS NULL OR s.org_id = $1)
               AND ($2::TEXT[] IS NULL OR s.lifecycle = ANY($2::TEXT[]))
               AND ($3::TEXT IS NULL OR s.template_id = $3)
               AND ($4::UUID IS NULL OR s.user_id = $4)
               AND ($5::TIMESTAMPTZ IS NULL OR s.created_at > $5)
               AND ($6::TIMESTAMPTZ IS NULL OR s.created_at < $6)
               AND ($7::TEXT IS NULL
                    OR starts_with(s.name, $7)
                    OR s.id BETWEEN $8::UUID AND $9::UUID)
               AND ($10::UUID IS NULL OR {on_node})
               AND ($11::BOOLEAN IS NULL OR {on_a_node} = $11)
             ORDER BY s.created_at DESC, s.id DESC
             LIMIT $16
         )
         SELECT {{sandbox_columns}}
         FROM listed
         JOIN sandboxes s ON s.id = listed.id
         {{sandbox_join_tail}}
         ORDER BY listed.created_at DESC, listed.id DESC"
    ))
});

/// Whether a page stops at the list byte budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageBudget {
    /// The list byte budget, [`super::MAX_SANDBOX_LIST_PAGE_BYTES`], for a
    /// page a cursor continues: the page walk.
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

/// The values of the filters every admin list statement binds as $1-$11.
struct FilterBinds<'a> {
    org_id: Option<OrgId>,
    lifecycles: Option<Vec<&'static str>>,
    template: Option<&'a str>,
    user_id: Option<UserId>,
    created_after: Option<OffsetDateTime>,
    created_before: Option<OffsetDateTime>,
    prefix: Option<&'a str>,
    prefix_id_range: Option<(SandboxId, SandboxId)>,
    node_id: Option<NodeId>,
    assigned: Option<bool>,
}

impl<'a> FilterBinds<'a> {
    fn new(filter: &'a AdminSandboxFilter) -> Self {
        let (node_id, assigned) = match filter.node {
            Some(NodeAssignment::Node(node_id)) => (Some(node_id), None),
            Some(NodeAssignment::Assigned) => (None, Some(true)),
            Some(NodeAssignment::Unassigned) => (None, Some(false)),
            None => (None, None),
        };
        Self {
            org_id: filter.org_id,
            lifecycles: filter.lifecycles.as_ref().map(|lifecycles| {
                lifecycles
                    .iter()
                    .map(|lifecycle| lifecycle.as_str())
                    .collect()
            }),
            template: filter.template.as_ref().map(TemplateName::as_str),
            user_id: filter.user_id,
            created_after: filter.created_after,
            created_before: filter.created_before,
            prefix: filter.prefix.as_ref().map(|prefix| prefix.prefix.as_str()),
            prefix_id_range: filter.prefix.as_ref().and_then(|prefix| prefix.id_range),
            node_id,
            assigned,
        }
    }

    fn bind<'q, O>(
        self,
        query: sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>
    where
        'a: 'q,
    {
        query
            .bind(self.org_id)
            .bind(self.lifecycles)
            .bind(self.template)
            .bind(self.user_id)
            .bind(self.created_after)
            .bind(self.created_before)
            .bind(self.prefix)
            .bind(self.prefix_id_range.map(|(low, _)| low))
            .bind(self.prefix_id_range.map(|(_, high)| high))
            .bind(self.node_id)
            .bind(self.assigned)
    }
}

impl SandboxSearch {
    /// Binds the search's ranges as a search statement's $12-$15.
    fn bind<'q, O>(
        &'q self,
        query: sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments> {
        let (suffix_low, suffix_high) = &self.id_suffix;
        query
            .bind(suffix_low.as_str())
            .bind(suffix_high.as_str())
            .bind(self.id_prefix.map(|(low, _)| low))
            .bind(self.id_prefix.map(|(_, high)| high))
    }
}

impl SandboxController {
    /// One admin list page matching `filter`, newest first by `(created_at,
    /// id)`, after `cursor`: at most `limit` rows, and whether more follow.
    /// A [`PageBudget::Unbounded`] list ignores `cursor` and never says more
    /// follow.
    pub(crate) async fn admin_sandbox_page(
        &self,
        pool: &PgPool,
        filter: &AdminSandboxFilter,
        cursor: Option<SandboxPageCursor>,
        limit: u32,
        budget: PageBudget,
    ) -> Result<ListPage<Sandbox>, sqlx::Error> {
        let binds = FilterBinds::new(filter);
        let rows = match (budget, &filter.search) {
            (PageBudget::Unbounded, None) => {
                let rows = binds
                    .bind(sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(sql!(
                        ADMIN_LIST_SQL.as_str()
                    ))))
                    .bind(i64::from(limit))
                    .fetch_all(pool)
                    .await?;
                return Ok(ListPage {
                    rows,
                    has_more: false,
                });
            }
            (PageBudget::Unbounded, Some(search)) => {
                let rows = search
                    .bind(
                        binds.bind(sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(sql!(
                            ADMIN_SEARCH_LIST_SQL.as_str()
                        )))),
                    )
                    .bind(i64::from(limit))
                    .fetch_all(pool)
                    .await?;
                return Ok(ListPage {
                    rows,
                    has_more: false,
                });
            }
            (PageBudget::Bounded, None) => {
                binds
                    .bind(sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(
                        sql!(ADMIN_PAGE_SQL.as_str()),
                    )))
                    .bind(cursor.map(|cursor| cursor.created_at))
                    .bind(cursor.map(|cursor| cursor.id))
                    .bind(i64::from(limit.saturating_add(1)))
                    .bind(self.list_page_max_bytes)
                    .bind(SANDBOX_LIST_ROW_OVERHEAD_BYTES)
                    .fetch_all(pool)
                    .await?
            }
            (PageBudget::Bounded, Some(search)) => {
                search
                    .bind(
                        binds.bind(sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(
                            sql!(ADMIN_SEARCH_PAGE_SQL.as_str()),
                        ))),
                    )
                    .bind(cursor.map(|cursor| cursor.created_at))
                    .bind(cursor.map(|cursor| cursor.id))
                    .bind(i64::from(limit.saturating_add(1)))
                    .bind(self.list_page_max_bytes)
                    .bind(SANDBOX_LIST_ROW_OVERHEAD_BYTES)
                    .fetch_all(pool)
                    .await?
            }
        };
        let page = ListPage::from_rows(rows, limit as usize, |row| row.has_next);
        Ok(ListPage {
            rows: page.rows.into_iter().map(|row| row.sandbox).collect(),
            has_more: page.has_more,
        })
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
    ///
    /// Postgres applies the `deleted` filter at the `FULL JOIN`, below the
    /// inventory lookup, so the many stored `deleted` rows no node reports
    /// cost a hash probe each. Dropping them from `sandboxes` first measured
    /// slower as an `EXISTS`, and a hashed `IN` turns into a per-row subplan
    /// once the observed rows outgrow `hash_mem`.
    pub async fn admin_spaced_version_counts(
        &self,
        pool: &PgPool,
        org_id: Option<OrgId>,
    ) -> Result<Vec<SpacedVersionCount>, ControllerError> {
        // A row nodes report with no sandbox row is a pool slot as
        // `observed_only_sandbox_response` decides it: by its node's
        // `pool_template`, or by a warm-pool slot reserving its ID. The
        // org-scoped count has no such rows. A row that appears between the
        // two reads counts as observed-local until the next count.
        let pool_reserved = if org_id.is_none() {
            let rowless = sqlx::query_scalar::<_, SandboxId>(sql!(
                "SELECT sos.sandbox_id
                 FROM sandbox_observed_states sos
                 WHERE NOT EXISTS (SELECT 1 FROM sandboxes s WHERE s.id = sos.sandbox_id)"
            ))
            .fetch_all(pool)
            .await
            .map_err(|err| db_contention_error("list the rowless observed sandboxes", &err))?;
            WarmPoolStore::new(pool.clone())
                .pool_reservations(&rowless)
                .await
                .map_err(|err| {
                    pool_store_error("query the warm-pool slots of node-reported sandboxes", &err)
                })?
                .into_iter()
                .map(|reservation| reservation.sandbox_id)
                .collect()
        } else {
            Vec::new()
        };
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
                        s.id IS NULL
                            AND (sos.pool_template IS NOT NULL
                                 OR sos.sandbox_id IN (SELECT unnest($3::UUID[]))) AS pool_slot
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
        .bind(pool_reserved)
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

#[cfg(test)]
mod search_plan_tests;
#[cfg(test)]
pub(crate) mod tests;
