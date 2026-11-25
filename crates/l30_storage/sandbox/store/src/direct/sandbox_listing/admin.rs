//! The admin sandbox list's statements: a page walk and the legacy unpaged
//! list, each with and without an ID search.
//!
//! All four bind the filters of [`AdminListFilter`] as $1-$11 in one order;
//! a search's ranges ([`AdminIdSearch`]) follow as $12-$15, then each
//! statement's own walk or limit. The node filters ($10, $11) ask which node
//! the response reports: a sandbox's Direct route's current node when it has
//! a route, else its row's `active_node_id`, as `SANDBOX_COLUMNS` reports it.
//! Each statement answers that for one row inside itself, so no caller reads
//! routes first and binds them back, and each writes the same two predicates.
//!
//! The node predicate ($10) is TRUE iff the response reports the row on that
//! node, FALSE otherwise, never NULL. Its first conjunct only narrows: the
//! sandboxes whose row or route names the node. A plan that knows the node
//! can find them by index, through `idx_sandboxes_active_node` and an index
//! on `sandboxes.id`, instead of testing every row in a list's order, so a
//! node with few sandboxes costs few. Nothing guarantees that plan: the
//! filter is optional (`$10 IS NULL OR` the predicate), and under a generic
//! plan the planner cannot drop the `IS NULL` arm and tests every row in the
//! list's order. The route side reads the routes on the node once per
//! statement; no index covers `current_node_id`, so that read scans the route
//! table, inside the database. The second conjunct is the exact test. The
//! first is NULL for a sandbox with no route and no `active_node_id`; the
//! second is then FALSE rather than NULL, so the predicate is too. The
//! assigned predicate ($11) is TRUE iff the response reports the row on some
//! node, FALSE otherwise, never NULL, so `= $11` with FALSE keeps exactly the
//! rows on no node (`tests`).
//!
//! The route table does not shrink to the routes in flight: writing off a
//! sandbox from a gone node, reverting a restore and rolling back an
//! activation clear the row's `active_node_id` and leave its route, which
//! only goes with `clear_direct_route_identity` or the row. A read of every
//! route that disagrees with its row grows with those sandboxes.
//!
//! A search runs its own pair of statements, the same filters with the
//! search required rather than optional. The other filters are written
//! `$n IS NULL OR ...`, which a generic plan cannot drop, so it reads the list
//! in `created_at` order and tests every row. That is cheap for a filter most
//! rows pass and for a page that fills early, but a search usually matches a
//! few rows of a large table, and walking the table to find them reads all of
//! it. Required, the search's two ranges are index conditions on
//! `idx_sandboxes_id_suffix` and the primary key under either plan. They are
//! read first, in a `MATERIALIZED` CTE, and the other filters and the cursor
//! apply to those rows only: unfenced, a generic plan ANDs the search's
//! bitmap with one for the cursor's `created_at` bound, which on the first
//! page holds every row. So a search reads the rows it matches and sorts
//! those (`search_plan_tests`).

use std::sync::LazyLock;

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use sqlx::postgres::PgArguments;
use sqlx::query::QueryAs;
use time::OffsetDateTime;
use uuid::Uuid;

use super::{PageWalk, SANDBOX_COLUMNS, SANDBOX_JOIN_TAIL, Sandbox, ScopedPageRow};
use crate::StoredSandboxLifecycle;

#[cfg(test)]
mod search_plan_tests;
#[cfg(test)]
mod tests;

/// What the admin list narrows to, bound as $1-$11. Every field that is set
/// must match; an unset field matches every row.
#[derive(Debug, Clone, Copy, Default)]
pub struct AdminListFilter<'a> {
    /// $1.
    pub org_id: Option<OrgId>,
    /// $2: the stored lifecycle, any of these.
    pub lifecycles: Option<&'a [StoredSandboxLifecycle]>,
    /// $3: the row's own `template_id`.
    pub template: Option<&'a str>,
    /// $4.
    pub user_id: Option<UserId>,
    /// $5: created strictly after this instant.
    pub created_after: Option<OffsetDateTime>,
    /// $6: created strictly before this instant.
    pub created_before: Option<OffsetDateTime>,
    /// $7: a prefix of the name, which also matches the IDs in
    /// `prefix_id_range` ($8, $9) when it can start one.
    pub prefix: Option<&'a str>,
    pub prefix_id_range: Option<(SandboxId, SandboxId)>,
    /// $10: the node the response reports.
    pub node_id: Option<NodeId>,
    /// $11: whether the response reports a node.
    pub assigned: Option<bool>,
}

impl<'a> AdminListFilter<'a> {
    fn bind<'q, O>(
        self,
        query: QueryAs<'q, sqlx::Postgres, O, PgArguments>,
    ) -> QueryAs<'q, sqlx::Postgres, O, PgArguments>
    where
        'a: 'q,
    {
        let lifecycles = self.lifecycles.map(|lifecycles| {
            lifecycles
                .iter()
                .map(|lifecycle| lifecycle.as_str())
                .collect::<Vec<_>>()
        });
        query
            .bind(self.org_id)
            .bind(lifecycles)
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

/// An ID search, bound as $12-$15: the IDs whose undashed hex ends with its
/// digits, and from [`Self::MIN_PREFIX_DIGITS`] digits the IDs that start
/// with them. Built from the digits by [`Self::from_id_digits`] only, so the
/// ranges a search binds are always the ones its digits give.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminIdSearch {
    /// $12, $13: the IDs ending in the digits, as a range of their undashed
    /// hex reversed (`idx_sandboxes_id_suffix`): at least the reversed digits
    /// and below them with their last digit incremented.
    id_suffix: (String, String),
    /// $14, $15: the lowest and highest IDs that start with the digits,
    /// inclusive, when there are at least [`Self::MIN_PREFIX_DIGITS`] of them.
    id_prefix: Option<(SandboxId, SandboxId)>,
}

/// The hex digits of an ID without its dashes.
const UNDASHED_UUID_LEN: usize = 32;

impl AdminIdSearch {
    /// The fewest digits a search takes: the IDs that end with them. They
    /// are random.
    pub const MIN_SUFFIX_DIGITS: usize = 4;
    /// The fewest digits that also match the IDs that start with them. A
    /// UUIDv7 starts with its creation time in milliseconds, so four digits
    /// span about 50 days of sandboxes and eight about a minute.
    pub const MIN_PREFIX_DIGITS: usize = 8;

    /// The search for `text`, an ID's hex digits in either case with any
    /// dashes; `None` unless it is [`Self::MIN_SUFFIX_DIGITS`] to 32 of them
    /// and nothing else.
    pub fn from_id_digits(text: &str) -> Option<Self> {
        let hex = text
            .chars()
            .filter(|character| *character != '-')
            .map(|character| character.to_ascii_lowercase())
            .collect::<String>();
        if !((Self::MIN_SUFFIX_DIGITS..=UNDASHED_UUID_LEN).contains(&hex.len())
            && hex.chars().all(|character| character.is_ascii_hexdigit()))
        {
            return None;
        }
        let id_prefix = if hex.len() >= Self::MIN_PREFIX_DIGITS {
            hex_prefix_range(&hex)
        } else {
            None
        };
        Some(Self {
            id_suffix: id_suffix_range(&hex),
            id_prefix,
        })
    }

    /// The range of reversed undashed hex the IDs ending in the digits fall
    /// in, `[low, high)`.
    pub fn id_suffix(&self) -> (&str, &str) {
        let (low, high) = &self.id_suffix;
        (low, high)
    }

    /// The IDs that start with the digits, `[low, high]`, when there are
    /// enough of them.
    pub fn id_prefix(&self) -> Option<(SandboxId, SandboxId)> {
        self.id_prefix
    }

    fn bind<'q, O>(
        &'q self,
        query: QueryAs<'q, sqlx::Postgres, O, PgArguments>,
    ) -> QueryAs<'q, sqlx::Postgres, O, PgArguments> {
        let (suffix_low, suffix_high) = self.id_suffix();
        query
            .bind(suffix_low)
            .bind(suffix_high)
            .bind(self.id_prefix.map(|(low, _)| low))
            .bind(self.id_prefix.map(|(_, high)| high))
    }
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

/// The lowest and highest IDs that start with `hex`. Postgres compares UUIDs
/// byte by byte, so a hex prefix is exactly this range.
fn hex_prefix_range(hex: &str) -> Option<(SandboxId, SandboxId)> {
    let bound = |fill: char| {
        let mut digits = hex.to_string();
        digits.extend(std::iter::repeat_n(fill, UNDASHED_UUID_LEN - hex.len()));
        Uuid::try_parse(&digits).ok().map(SandboxId::from_uuid)
    };
    Some((bound('0')?, bound('f')?))
}

/// The admin list's sandboxes ($1-$11) after the cursor ($12, $13), walked
/// to at most $14 rows within the byte budget $15 with $16 bytes a row. The
/// candidate rows read `sandboxes` alone; the admin response carries no
/// `env`.
static PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "WITH RECURSIVE page_candidates AS MATERIALIZED (
             SELECT array_agg(candidate.id ORDER BY candidate.created_at DESC, candidate.id DESC)
                      AS ids
             FROM (SELECT s.id, s.created_at
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
                     AND ($10::UUID IS NULL OR (
                              (s.active_node_id = $10
                               OR s.id = ANY(ARRAY(
                                   SELECT route.sandbox_id
                                   FROM sandbox_direct_route_identities route
                                   WHERE route.current_node_id = $10)))
                              AND COALESCE(
                                  (SELECT route.current_node_id
                                   FROM sandbox_direct_route_identities route
                                   WHERE route.sandbox_id = s.id),
                                  s.active_node_id
                              ) IS NOT DISTINCT FROM $10
                          ))
                     AND ($11::BOOLEAN IS NULL OR (
                              s.active_node_id IS NOT NULL
                              OR EXISTS (
                                  SELECT 1
                                  FROM sandbox_direct_route_identities route
                                  WHERE route.sandbox_id = s.id
                              )
                          ) = $11)
                     AND (s.created_at, s.id) < (COALESCE($12::TIMESTAMPTZ, 'infinity'),
                         COALESCE($13::UUID, '00000000-0000-0000-0000-000000000000'))
                   ORDER BY s.created_at DESC, s.id DESC
                   LIMIT $14::BIGINT) candidate
         ),
         page_walk AS (
             SELECT NULL::UUID AS id, 0::BIGINT AS through_bytes, 0::BIGINT AS position
             UNION ALL
             SELECT sized.id,
                    walk.through_bytes
                      + octet_length(sized.metadata::TEXT)
                      + COALESCE(octet_length(to_jsonb(sized.name)::TEXT), 0)
                      + $16::BIGINT,
                    walk.position + 1
             FROM page_walk walk
             CROSS JOIN page_candidates candidates
             JOIN sandboxes sized ON sized.id = candidates.ids[walk.position + 1]
             WHERE walk.through_bytes <= $15::BIGINT OR walk.position = 1
         ),
         page_rows AS (
             SELECT walk.id, walk.position, walk.has_next
             FROM (SELECT page_walk.id, page_walk.through_bytes, page_walk.position,
                          page_walk.position < max(page_walk.position) OVER () AS has_next
                   FROM page_walk) walk
             WHERE walk.position >= 1
               AND (walk.through_bytes <= $15::BIGINT OR walk.position = 1)
         )
         SELECT {SANDBOX_COLUMNS}, page.has_next
         FROM page_rows page
         JOIN sandboxes s ON s.id = page.id
         {SANDBOX_JOIN_TAIL}
         ORDER BY page.position"
    ))
});

/// One admin list page matching `filter`, newest first by `(created_at,
/// id)`, after `walk`'s cursor and within its byte budget.
pub async fn page<'e>(
    executor: impl PgExecutor<'e>,
    filter: AdminListFilter<'_>,
    walk: &PageWalk,
) -> Result<Vec<ScopedPageRow>, sqlx::Error> {
    filter
        .bind(sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(
            PAGE_SQL.as_str(),
        )))
        .bind(walk.cursor_created_at())
        .bind(walk.cursor_id())
        .bind(walk.max_rows)
        .bind(walk.max_bytes)
        .bind(walk.row_overhead_bytes)
        .fetch_all(executor)
        .await
}

/// The legacy unpaged list: at most $12 sandboxes matching the filters
/// ($1-$11, as [`PAGE_SQL`]'s), newest first. The joins of the response
/// include the Direct route, so the node predicates look a filtered row's
/// route up again, by its primary key.
static LIST_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "SELECT {SANDBOX_COLUMNS}
         FROM sandboxes s
         {SANDBOX_JOIN_TAIL}
         WHERE ($1::UUID IS NULL OR s.org_id = $1)
           AND ($2::TEXT[] IS NULL OR s.lifecycle = ANY($2::TEXT[]))
           AND ($3::TEXT IS NULL OR s.template_id = $3)
           AND ($4::UUID IS NULL OR s.user_id = $4)
           AND ($5::TIMESTAMPTZ IS NULL OR s.created_at > $5)
           AND ($6::TIMESTAMPTZ IS NULL OR s.created_at < $6)
           AND ($7::TEXT IS NULL
                OR starts_with(s.name, $7)
                OR s.id BETWEEN $8::UUID AND $9::UUID)
           AND ($10::UUID IS NULL OR (
                    (s.active_node_id = $10
                     OR s.id = ANY(ARRAY(
                         SELECT route.sandbox_id
                         FROM sandbox_direct_route_identities route
                         WHERE route.current_node_id = $10)))
                    AND COALESCE(
                        (SELECT route.current_node_id
                         FROM sandbox_direct_route_identities route
                         WHERE route.sandbox_id = s.id),
                        s.active_node_id
                    ) IS NOT DISTINCT FROM $10
                ))
           AND ($11::BOOLEAN IS NULL OR (
                    s.active_node_id IS NOT NULL
                    OR EXISTS (
                        SELECT 1
                        FROM sandbox_direct_route_identities route
                        WHERE route.sandbox_id = s.id
                    )
                ) = $11)
         ORDER BY s.created_at DESC, s.id DESC
         LIMIT $12"
    ))
});

/// At most `limit` sandboxes matching `filter`, newest first by
/// `(created_at, id)`, with no byte budget and no cursor.
pub async fn list<'e>(
    executor: impl PgExecutor<'e>,
    filter: AdminListFilter<'_>,
    limit: i64,
) -> Result<Vec<Sandbox>, sqlx::Error> {
    filter
        .bind(sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(
            LIST_SQL.as_str(),
        )))
        .bind(limit)
        .fetch_all(executor)
        .await
}

/// [`PAGE_SQL`] with an ID search ($12-$15) required: the sandboxes whose ID
/// ends or starts with its hex digits, after the cursor ($16, $17), walked to
/// at most $18 rows within the byte budget $19 with $20 bytes a row.
///
/// The suffix arm reads `idx_sandboxes_id_suffix` in [$12, $13), the
/// reversed digits' range; the prefix arm reads the primary key in [$14,
/// $15], which is NULL for too few digits to start an ID with.
static SEARCH_PAGE_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
        "WITH RECURSIVE page_candidates AS MATERIALIZED (
             SELECT array_agg(candidate.id ORDER BY candidate.created_at DESC, candidate.id DESC)
                      AS ids
             FROM (WITH matches AS MATERIALIZED (
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
                     AND ($10::UUID IS NULL OR (
                              (s.active_node_id = $10
                               OR s.id = ANY(ARRAY(
                                   SELECT route.sandbox_id
                                   FROM sandbox_direct_route_identities route
                                   WHERE route.current_node_id = $10)))
                              AND COALESCE(
                                  (SELECT route.current_node_id
                                   FROM sandbox_direct_route_identities route
                                   WHERE route.sandbox_id = s.id),
                                  s.active_node_id
                              ) IS NOT DISTINCT FROM $10
                          ))
                     AND ($11::BOOLEAN IS NULL OR (
                              s.active_node_id IS NOT NULL
                              OR EXISTS (
                                  SELECT 1
                                  FROM sandbox_direct_route_identities route
                                  WHERE route.sandbox_id = s.id
                              )
                          ) = $11)
                     AND (s.created_at, s.id) < (COALESCE($16::TIMESTAMPTZ, 'infinity'),
                         COALESCE($17::UUID, '00000000-0000-0000-0000-000000000000'))
                   ORDER BY s.created_at DESC, s.id DESC
                   LIMIT $18::BIGINT) candidate
         ),
         page_walk AS (
             SELECT NULL::UUID AS id, 0::BIGINT AS through_bytes, 0::BIGINT AS position
             UNION ALL
             SELECT sized.id,
                    walk.through_bytes
                      + octet_length(sized.metadata::TEXT)
                      + COALESCE(octet_length(to_jsonb(sized.name)::TEXT), 0)
                      + $20::BIGINT,
                    walk.position + 1
             FROM page_walk walk
             CROSS JOIN page_candidates candidates
             JOIN sandboxes sized ON sized.id = candidates.ids[walk.position + 1]
             WHERE walk.through_bytes <= $19::BIGINT OR walk.position = 1
         ),
         page_rows AS (
             SELECT walk.id, walk.position, walk.has_next
             FROM (SELECT page_walk.id, page_walk.through_bytes, page_walk.position,
                          page_walk.position < max(page_walk.position) OVER () AS has_next
                   FROM page_walk) walk
             WHERE walk.position >= 1
               AND (walk.through_bytes <= $19::BIGINT OR walk.position = 1)
         )
         SELECT {SANDBOX_COLUMNS}, page.has_next
         FROM page_rows page
         JOIN sandboxes s ON s.id = page.id
         {SANDBOX_JOIN_TAIL}
         ORDER BY page.position"
    ))
});

/// [`page`] for the sandboxes `search` matches.
pub async fn search_page<'e>(
    executor: impl PgExecutor<'e>,
    filter: AdminListFilter<'_>,
    search: &AdminIdSearch,
    walk: &PageWalk,
) -> Result<Vec<ScopedPageRow>, sqlx::Error> {
    search
        .bind(
            filter.bind(sqlx::query_as::<_, ScopedPageRow>(sqlx::AssertSqlSafe(
                SEARCH_PAGE_SQL.as_str(),
            ))),
        )
        .bind(walk.cursor_created_at())
        .bind(walk.cursor_id())
        .bind(walk.max_rows)
        .bind(walk.max_bytes)
        .bind(walk.row_overhead_bytes)
        .fetch_all(executor)
        .await
}

/// [`LIST_SQL`] with an ID search required, as [`SEARCH_PAGE_SQL`] matches
/// it ($12-$15): at most $16 sandboxes. `listed` picks them by ID before
/// their full rows are joined, as the page walk does, so a search that
/// matches many rows sorts their IDs rather than their joined rows.
static SEARCH_LIST_SQL: LazyLock<String> = LazyLock::new(|| {
    sql!(format!(
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
               AND ($10::UUID IS NULL OR (
                        (s.active_node_id = $10
                         OR s.id = ANY(ARRAY(
                             SELECT route.sandbox_id
                             FROM sandbox_direct_route_identities route
                             WHERE route.current_node_id = $10)))
                        AND COALESCE(
                            (SELECT route.current_node_id
                             FROM sandbox_direct_route_identities route
                             WHERE route.sandbox_id = s.id),
                            s.active_node_id
                        ) IS NOT DISTINCT FROM $10
                    ))
               AND ($11::BOOLEAN IS NULL OR (
                        s.active_node_id IS NOT NULL
                        OR EXISTS (
                            SELECT 1
                            FROM sandbox_direct_route_identities route
                            WHERE route.sandbox_id = s.id
                        )
                    ) = $11)
             ORDER BY s.created_at DESC, s.id DESC
             LIMIT $16
         )
         SELECT {SANDBOX_COLUMNS}
         FROM listed
         JOIN sandboxes s ON s.id = listed.id
         {SANDBOX_JOIN_TAIL}
         ORDER BY listed.created_at DESC, listed.id DESC"
    ))
});

/// [`list`] for the sandboxes `search` matches.
pub async fn search_list<'e>(
    executor: impl PgExecutor<'e>,
    filter: AdminListFilter<'_>,
    search: &AdminIdSearch,
    limit: i64,
) -> Result<Vec<Sandbox>, sqlx::Error> {
    search
        .bind(
            filter.bind(sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(
                SEARCH_LIST_SQL.as_str(),
            ))),
        )
        .bind(limit)
        .fetch_all(executor)
        .await
}
