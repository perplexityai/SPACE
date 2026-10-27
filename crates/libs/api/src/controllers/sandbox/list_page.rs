//! The byte budget every sandbox list page stops at.
//!
//! The native and ECW lists select different columns over different owner
//! filters, but both page the `sandboxes` table newest first by
//! `(created_at, id)`, and their pages are bounded at
//! [`MAX_SANDBOX_LIST_PAGE_BYTES`] of estimated JSON. Each list statement puts
//! [`page_rows_cte`] in front of its own select and reads `page_rows`, and
//! builds its page with [`ListPage::from_rows`], so the budget, the size
//! estimate and when a page continues are decided here once.

/// Estimated bytes after which a sandbox list page stops. Capped metadata
/// keeps a typical full row near 18 KB, but JSON escapes a control character
/// to six bytes, and rows stored before the input limits can be larger.
pub(crate) const MAX_SANDBOX_LIST_PAGE_BYTES: i64 = 8 * 1024 * 1024;
/// A listed sandbox apart from its name, metadata and env: about 700 bytes
/// of native JSON, up to about 1.1 KB with the longest template reference.
pub(crate) const SANDBOX_LIST_ROW_OVERHEAD_BYTES: i64 = 2 * 1024;

/// The placeholders of the bound values [`page_rows_cte`] reads, such as
/// `"$11"`, and the columns it sizes. The caller binds the cursor's
/// `created_at` and `id` (both NULL for the first page), the most rows to
/// walk, which is the page limit plus one, [`MAX_SANDBOX_LIST_PAGE_BYTES`]
/// and [`SANDBOX_LIST_ROW_OVERHEAD_BYTES`].
pub(crate) struct PageWalkParams {
    pub(crate) cursor_created_at: &'static str,
    pub(crate) cursor_id: &'static str,
    pub(crate) max_rows: &'static str,
    pub(crate) max_bytes: &'static str,
    pub(crate) row_overhead_bytes: &'static str,
    /// Whether the list's select returns `env`, so its size counts.
    pub(crate) lists_env: bool,
}

/// The `WITH RECURSIVE` clause that defines `page_rows (id, position,
/// has_next)`: the sandboxes of one list page in list order, numbered from 1,
/// and whether each is followed by another candidate, whether or not the
/// budget let that one in.
///
/// `candidate_rows` selects the `id` and `created_at` of the sandboxes the
/// list shows after the cursor, in `ORDER BY created_at DESC, id DESC`, at
/// most `{max_rows}` of them. It writes `{after_cursor}` for the keyset
/// condition on `s` and `{max_rows}` for its `LIMIT`, which this function
/// fills in, so it is planned like a list without a budget and reads only
/// the ids. The walk then follows those ids one row at a time, adding each
/// row's estimated size to a running total. It stops one row after the total
/// passes `max_bytes`, so the page keeps rows within the budget, always the
/// first so the cursor advances, and the row after the last kept one tells
/// the caller whether the list continues. It is recursive so that Postgres
/// sizes only those rows: a running-sum window cannot stop early and would
/// size every candidate.
///
/// The estimate is the JSON text of the metadata, of `env` when the list
/// returns it, and of the escaped name, plus `row_overhead_bytes` for
/// everything else; jsonb's text form is never shorter than the compact JSON
/// a page is served as. `pg_column_size` would be cheaper, but it is the
/// compressed size, which repetitive metadata makes many times smaller than
/// the decoded row.
///
/// The caller's select should start `FROM page_rows page JOIN sandboxes s ON
/// s.id = page.id` and order by `page.position`, so the planner joins each
/// page row by id instead of scanning `sandboxes` for a hash join.
pub(crate) fn page_rows_cte(candidate_rows: &str, params: &PageWalkParams) -> String {
    let PageWalkParams {
        cursor_created_at,
        cursor_id,
        max_rows,
        max_bytes,
        row_overhead_bytes,
        lists_env,
    } = params;
    let candidate_rows = candidate_rows
        .replace(
            "{after_cursor}",
            &format!(
                "(s.created_at, s.id) < (COALESCE({cursor_created_at}::TIMESTAMPTZ, 'infinity'),
                     COALESCE({cursor_id}::UUID, '00000000-0000-0000-0000-000000000000'))"
            ),
        )
        .replace("{max_rows}", &format!("{max_rows}::BIGINT"));
    let env_bytes = if *lists_env {
        "+ octet_length(sized.env::TEXT)"
    } else {
        ""
    };
    format!(
        "WITH RECURSIVE page_candidates AS MATERIALIZED (
             SELECT array_agg(candidate.id ORDER BY candidate.created_at DESC, candidate.id DESC)
                      AS ids
             FROM ({candidate_rows}) candidate
         ),
         page_walk AS (
             SELECT NULL::UUID AS id, 0::BIGINT AS through_bytes, 0::BIGINT AS position
             UNION ALL
             SELECT sized.id,
                    walk.through_bytes
                      + octet_length(sized.metadata::TEXT)
                      {env_bytes}
                      + COALESCE(octet_length(to_jsonb(sized.name)::TEXT), 0)
                      + {row_overhead_bytes}::BIGINT,
                    walk.position + 1
             FROM page_walk walk
             CROSS JOIN page_candidates candidates
             JOIN sandboxes sized ON sized.id = candidates.ids[walk.position + 1]
             WHERE walk.through_bytes <= {max_bytes}::BIGINT OR walk.position = 1
         ),
         page_rows AS (
             SELECT walk.id, walk.position, walk.has_next
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
