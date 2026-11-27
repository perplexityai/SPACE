//! The byte budget every sandbox list page stops at.
//!
//! The native, ECW and public lists select different columns over different
//! owner filters, but all page the `sandboxes` table newest first by
//! `(created_at, id)`, and their pages are bounded at
//! [`MAX_SANDBOX_LIST_PAGE_BYTES`] of estimated JSON. Each list statement is
//! a `pplx_space_sandbox_store::direct::sandbox_listing` page, which walks
//! the budget it is bound with the size estimate each page statement
//! writes out, and each list builds its page with [`ListPage::from_rows`],
//! so the budget and when a page continues are decided once.

/// The estimated JSON budget of a sandbox list page, 8 MiB: a page keeps the
/// rows whose running estimated size stays within it. The first eligible row
/// is returned even when it alone exceeds the budget, so the cursor advances.
/// Capped metadata keeps a typical full row near 18 KB, but JSON escapes a
/// control character to six bytes, and rows stored before the input limits
/// can be larger.
pub(crate) const MAX_SANDBOX_LIST_PAGE_BYTES: i64 = 8 * 1024 * 1024;
/// The budget tests page against: a sixteenth of
/// [`MAX_SANDBOX_LIST_PAGE_BYTES`], so a page holds KiB rows rather than MiB
/// ones. The walk only compares running sizes with the budget, and
/// [`SANDBOX_LIST_ROW_OVERHEAD_BYTES`] is a thirty-second of a row an eighth
/// of this budget long, so pages keep the shapes they have at full size.
#[cfg(test)]
pub(crate) const TEST_SANDBOX_LIST_PAGE_BYTES: i64 = MAX_SANDBOX_LIST_PAGE_BYTES / 16;
/// A listed sandbox apart from its name, metadata and env: about 700 bytes
/// of native JSON, up to about 1.1 KB with the longest template reference.
pub(crate) const SANDBOX_LIST_ROW_OVERHEAD_BYTES: i64 = 2 * 1024;

pub(crate) mod admin;

/// One list page: at most the requested number of rows, and whether more
/// follow them.
pub(crate) struct ListPage<T> {
    pub(crate) rows: Vec<T>,
    pub(crate) has_more: bool,
}

impl<T> ListPage<T> {
    /// Builds a page from rows a list page statement returned in list
    /// order. The list continues when the limit's lookahead row came back or
    /// the walk stopped at its byte budget before the end of the list.
    pub(crate) fn from_rows(mut rows: Vec<T>, limit: usize, has_next: impl Fn(&T) -> bool) -> Self {
        let has_more = rows.len() > limit || rows.last().is_some_and(has_next);
        rows.truncate(limit);
        Self { rows, has_more }
    }
}
