//! The admin sandbox list's filters and pages, and its count of sandboxes by
//! template and `spaced` version.
//!
//! A page is the list page walk ([`super`]) over the admin filters, run by
//! `pplx_space_sandbox_store::direct::sandbox_listing::admin`, which returns
//! the same full rows as the ECW list and stops at the byte budget the same
//! way. The admin route's legacy unpaged array has no cursor to continue
//! from, so it runs a plain list statement with the same filters and no size
//! walk.
//!
//! A [`SandboxSearch`] runs its own pair of statements, the same filters
//! with the search required rather than optional, so that a search reads the
//! rows it matches rather than the list in order; the store's admin module
//! says why and pins it with a plan test.

use pplx_space_accounts::{NodeId, OrgId, UserId};
use pplx_space_cluster_pool::WarmPoolStore;
use pplx_space_public_api::PUBLIC_SANDBOX_ID_PREFIX;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_store::StoredSandboxLifecycle;
use pplx_space_sandbox_store::direct::sandbox_listing::PageWalk;
use pplx_space_sandbox_store::direct::sandbox_listing::admin::{
    self as admin_listing, AdminIdSearch, AdminListFilter,
};
use pplx_space_semver::SemVer;
use pplx_space_template_model::TemplateName;
use serde::Serialize;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use super::{ListPage, SANDBOX_LIST_ROW_OVERHEAD_BYTES};
use crate::controllers::sandbox::{SandboxController, SandboxPageCursor};
use crate::controllers::{ControllerError, db_contention_error, pool_store_error};
use crate::models::{Sandbox, SandboxResourceType};

/// What the admin sandbox list narrows to. Every field that is set must
/// match; an unset field matches every row.
#[derive(Debug, Clone, Default)]
pub struct AdminSandboxFilter {
    pub org_id: Option<OrgId>,
    /// The stored lifecycle (`sandboxes.lifecycle`), any of these. No
    /// response field shows it: `lifecycle` and `status` report the node's
    /// observed status when there is one. Never empty.
    pub lifecycles: Option<Vec<StoredSandboxLifecycle>>,
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
///
/// The `sbx_` marker and the empty search are the api's; the digits and the
/// ranges they bind are the store's [`AdminIdSearch::from_id_digits`], which
/// the store's plan test explains the search statements with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxSearch {
    /// The digits' ranges, as the store's search statements bind them.
    ids: AdminIdSearch,
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
        let marker = PUBLIC_SANDBOX_ID_PREFIX.len();
        let digits = match (text.get(..marker), text.get(marker..)) {
            (Some(prefix), Some(rest)) if prefix.eq_ignore_ascii_case(PUBLIC_SANDBOX_ID_PREFIX) => {
                rest
            }
            _ => text,
        };
        let ids = AdminIdSearch::from_id_digits(digits).ok_or(NotAnIdSearch)?;
        Ok(Some(Self { ids }))
    }
}

const MIN_ID_SUFFIX_DIGITS: usize = AdminIdSearch::MIN_SUFFIX_DIGITS;
const MIN_ID_PREFIX_DIGITS: usize = AdminIdSearch::MIN_PREFIX_DIGITS;

const DASHED_UUID_DASH_POSITIONS: [usize; 4] = [8, 13, 18, 23];
const UNDASHED_UUID_LEN: usize = 32;

fn id_prefix_range(prefix: &str) -> Option<(SandboxId, SandboxId)> {
    let id = prefix.trim_prefix(PUBLIC_SANDBOX_ID_PREFIX);
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

impl AdminSandboxFilter {
    /// The filters as the store's admin list statements bind them.
    fn store_filter(&self) -> AdminListFilter<'_> {
        let (node_id, assigned) = match self.node {
            Some(NodeAssignment::Node(node_id)) => (Some(node_id), None),
            Some(NodeAssignment::Assigned) => (None, Some(true)),
            Some(NodeAssignment::Unassigned) => (None, Some(false)),
            None => (None, None),
        };
        AdminListFilter {
            org_id: self.org_id,
            lifecycles: self.lifecycles.as_deref(),
            template: self.template.as_ref().map(TemplateName::as_str),
            user_id: self.user_id,
            created_after: self.created_after,
            created_before: self.created_before,
            prefix: self.prefix.as_ref().map(|prefix| prefix.prefix.as_str()),
            prefix_id_range: self.prefix.as_ref().and_then(|prefix| prefix.id_range),
            node_id,
            assigned,
        }
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
        let store_filter = filter.store_filter();
        let walk = PageWalk {
            after: cursor.map(|cursor| (cursor.created_at, cursor.id)),
            max_rows: i64::from(limit.saturating_add(1)),
            max_bytes: self.list_page_max_bytes,
            row_overhead_bytes: SANDBOX_LIST_ROW_OVERHEAD_BYTES,
        };
        let rows = match (budget, &filter.search) {
            (PageBudget::Unbounded, None) => {
                let rows = admin_listing::list(pool, store_filter, i64::from(limit)).await?;
                return Ok(ListPage {
                    rows,
                    has_more: false,
                });
            }
            (PageBudget::Unbounded, Some(search)) => {
                let rows =
                    admin_listing::search_list(pool, store_filter, &search.ids, i64::from(limit))
                        .await?;
                return Ok(ListPage {
                    rows,
                    has_more: false,
                });
            }
            (PageBudget::Bounded, None) => admin_listing::page(pool, store_filter, &walk).await?,
            (PageBudget::Bounded, Some(search)) => {
                admin_listing::search_page(pool, store_filter, &search.ids, &walk).await?
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
            let rowless = pplx_space_sandbox_store::observed_states::rowless_sandbox_ids(pool)
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
        let rows = pplx_space_sandbox_store::spaced_version_counts::count_by_spaced_version(
            pool,
            org_id,
            &pool_reserved,
        )
        .await
        .map_err(|err| db_contention_error("count sandboxes by spaced version", &err))?;
        Ok(rows
            .into_iter()
            .map(|row| SpacedVersionCount {
                template: row.template,
                spaced_version: row.version.spaced_version,
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
pub(crate) mod tests;
