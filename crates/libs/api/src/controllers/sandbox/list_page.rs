//! The byte budget every sandbox list page stops at.
//!
//! The native and ECW lists select different columns over different owner
//! filters, but both page the `sandboxes` table newest first by
//! `(created_at, id)` and must not load an unbounded page: rows stored before
//! the create input limits, and ECW rows, which have no size limits, can hold
//! megabytes of metadata each. Each list statement puts [`page_rows_cte`] in
//! front of its own select and reads `page_rows`, and builds its page with
//! [`ListPage::from_rows`], so the budget, the size estimate and when a page
//! continues are decided here once.

/// Estimated bytes after which a sandbox list page stops. Capped metadata
/// keeps a typical full row near 18 KB, but JSON escapes a control character
/// to six bytes, and rows stored before the input limits can hold megabytes of
/// metadata.
pub(crate) const MAX_SANDBOX_LIST_PAGE_BYTES: i64 = 8 * 1024 * 1024;
/// A listed sandbox apart from its name, metadata and env: about 700 bytes
/// of native JSON, up to about 1.1 KB with the longest template reference.
pub(crate) const SANDBOX_LIST_ROW_OVERHEAD_BYTES: i64 = 2 * 1024;

/// The placeholders of the bound values [`page_rows_cte`] reads, such as
/// `"$11"`. The caller binds them: the cursor's `created_at` and `id` (both
/// NULL for the first page), the most rows to walk, which is the page limit
/// plus one, [`MAX_SANDBOX_LIST_PAGE_BYTES`] and
/// [`SANDBOX_LIST_ROW_OVERHEAD_BYTES`].
pub(crate) struct PageWalkParams {
    pub(crate) cursor_created_at: &'static str,
    pub(crate) cursor_id: &'static str,
    pub(crate) max_rows: &'static str,
    pub(crate) max_bytes: &'static str,
    pub(crate) row_overhead_bytes: &'static str,
}

/// The `WITH RECURSIVE` clause that defines `page_rows (id, has_next)`: the
/// sandboxes of one list page, at most `max_rows` of them, and whether each
/// is followed by another sandbox in list order, whether or not the budget
/// let that one in.
///
/// `next_row` is a subquery returning the `id` and `created_at` of the first
/// sandbox the list shows strictly after `(walk.created_at, walk.id)` in
/// `ORDER BY created_at DESC, id DESC`. The walk starts from a position 0
/// row holding the cursor, or `infinity` without one, which is never
/// returned, and follows that subquery one row at a time, adding each row's
/// estimated size to a running total. It stops one row after the total
/// passes `max_bytes` or at `max_rows` rows, so the page keeps
/// rows within the budget, always the first so the cursor advances, and the
/// row after the last kept one tells the caller whether the list continues.
/// It is recursive so that Postgres sizes only those rows: a running-sum
/// window cannot stop early and would size every candidate.
///
/// The estimate is the metadata's, env's and escaped name's JSON text plus
/// `row_overhead_bytes` for everything else; jsonb's text form is never
/// shorter than the compact JSON a page is served as. `pg_column_size` would
/// be cheaper, but it is the compressed size, which repetitive metadata makes
/// many times smaller than the decoded row.
pub(crate) fn page_rows_cte(next_row: &str, params: &PageWalkParams) -> String {
    let PageWalkParams {
        cursor_created_at,
        cursor_id,
        max_rows,
        max_bytes,
        row_overhead_bytes,
    } = params;
    format!(
        "WITH RECURSIVE page_walk AS (
             SELECT COALESCE({cursor_id}::UUID, '00000000-0000-0000-0000-000000000000') AS id,
                    COALESCE({cursor_created_at}::TIMESTAMPTZ, 'infinity') AS created_at,
                    0::BIGINT AS through_bytes,
                    0::BIGINT AS position
             UNION ALL
             SELECT next.id, next.created_at,
                    walk.through_bytes
                      + octet_length(sized.metadata::TEXT)
                      + octet_length(sized.env::TEXT)
                      + COALESCE(octet_length(to_jsonb(sized.name)::TEXT), 0)
                      + {row_overhead_bytes}::BIGINT,
                    walk.position + 1
             FROM page_walk walk
             CROSS JOIN LATERAL ({next_row}) next
             JOIN sandboxes sized ON sized.id = next.id
             WHERE (walk.through_bytes <= {max_bytes}::BIGINT OR walk.position = 1)
               AND walk.position < {max_rows}::BIGINT
         ),
         page_rows AS (
             SELECT walk.id, walk.has_next
             FROM (SELECT page_walk.id, page_walk.through_bytes, page_walk.position,
                          page_walk.position < max(page_walk.position) OVER () AS has_next
                   FROM page_walk) walk
             WHERE walk.position >= 1
               AND (walk.through_bytes <= {max_bytes}::BIGINT OR walk.position = 1)
         )"
    )
}

/// One list page: at most the requested number of rows, and whether more
/// follow them.
pub(crate) struct ListPage<T> {
    pub(crate) rows: Vec<T>,
    pub(crate) has_more: bool,
}

impl<T> ListPage<T> {
    /// Builds a page from rows a [`page_rows_cte`] statement returned in list
    /// order. The list continues when the limit's lookahead row came back or
    /// the walk stopped at its byte budget before the end of the list.
    pub(crate) fn from_rows(mut rows: Vec<T>, limit: usize, has_next: impl Fn(&T) -> bool) -> Self {
        let has_more = rows.len() > limit || rows.last().is_some_and(has_next);
        rows.truncate(limit);
        Self { rows, has_more }
    }
}
