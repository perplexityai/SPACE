//! Complete-or-error reads for bounded RPC inventories. Limits precede decoding.
//! Unpaged ListBuildReferencingSandboxes is the exception: it reports overruns
//! instead.

use crate::db::{Connection, Error};
use pplx_space_common_core::inventory::{
    InventoryBudget, InventoryLimitExceeded, MAX_ITEMS, MAX_RAW_BYTES,
};
use rusqlite::{Row, types::ValueRef};

#[derive(Debug, thiserror::Error)]
pub enum InventoryReadError {
    #[error(transparent)]
    Database(#[from] Error),
    #[error(transparent)]
    Limit(#[from] InventoryLimitExceeded),
    #[error("ambiguous sandbox selector")]
    Ambiguous,
}

impl From<rusqlite::Error> for InventoryReadError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error.into())
    }
}

impl InventoryReadError {
    pub fn into_status(self) -> tonic::Status {
        match self {
            Self::Limit(error) => tonic::Status::resource_exhausted(error.to_string()),
            Self::Database(error) => tonic::Status::internal(format!("read inventory: {error}")),
            Self::Ambiguous => tonic::Status::invalid_argument("ambiguous sandbox selector"),
        }
    }
}

fn row_raw_bytes(row: &Row<'_>) -> rusqlite::Result<usize> {
    let mut bytes = 0usize;
    for index in 0..row.as_ref().column_count() {
        let size = match row.get_ref(index)? {
            ValueRef::Text(value) | ValueRef::Blob(value) => value.len(),
            ValueRef::Null => 0,
            ValueRef::Integer(_) | ValueRef::Real(_) => 8,
        };
        bytes = bytes.saturating_add(size);
    }
    Ok(bytes)
}

pub(in crate::db) fn check_row(
    row: &Row<'_>,
    budget: &mut InventoryBudget,
) -> rusqlite::Result<(), InventoryReadError> {
    budget.charge(row_raw_bytes(row)?)?;
    Ok(())
}

/// Measures an unpaged inventory read against the strict inventory budget
/// without failing it. Build GC reads every parent-referencing sandbox in one
/// ListBuildReferencingSandboxes response, and a hard limit would break it on
/// dense nodes. Report the overrun instead.
#[derive(Default)]
struct UnaryListUsage {
    items: usize,
    raw_bytes: usize,
}

impl UnaryListUsage {
    fn observe(&mut self, row: &Row<'_>) -> rusqlite::Result<()> {
        self.items += 1;
        self.raw_bytes = self.raw_bytes.saturating_add(row_raw_bytes(row)?);
        Ok(())
    }

    fn report(&self, rpc: &'static str) {
        for (limit, exceeded) in [
            ("items", self.items > MAX_ITEMS),
            ("raw_bytes", self.raw_bytes > MAX_RAW_BYTES),
        ] {
            if exceeded {
                tracing::warn!(
                    rpc,
                    limit,
                    items = self.items,
                    raw_bytes = self.raw_bytes,
                    max_items = MAX_ITEMS,
                    max_raw_bytes = MAX_RAW_BYTES,
                    "unary sandbox list exceeds its inventory budget; serving the complete list"
                );
                pplx_space_metrics::sandbox::worker::unary_list_budget_exceeded(rpc, limit);
            }
        }
    }
}

const LIST_COLUMNS: &str =
    "id, state, template, template_hash, user, runtime, created_at, pool_template";

pub fn list(
    conn: &Connection,
    rpc: &'static str,
) -> Result<Vec<super::SandboxListRow>, InventoryReadError> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {LIST_COLUMNS} FROM sandboxes ORDER BY created_at"
    ))?;
    let mut rows = stmt.query([])?;
    let mut usage = UnaryListUsage::default();
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        usage.observe(row)?;
        result.push(super::row_to_list_sandbox(row)?);
    }
    usage.report(rpc);
    Ok(result)
}

/// One scanned List row. An undecodable row keeps its identity so the caller
/// can degrade it rather than hide it.
pub enum ListPageRow {
    Decoded(super::SandboxListRow),
    Undecodable {
        id: String,
        state: Option<String>,
        error: Error,
    },
}

impl ListPageRow {
    pub fn id(&self) -> &str {
        match self {
            Self::Decoded(row) => &row.id,
            Self::Undecodable { id, .. } => id,
        }
    }
}

pub struct ListPage {
    pub rows: Vec<ListPageRow>,
    /// Rows remain after the last scanned row.
    pub more: bool,
}

/// Scan up to `limit` rows with IDs after `after`, in ID order. The page ends
/// early rather than exceed the raw budget; only a single row over the budget
/// is an error. Keying on the immutable primary key keeps every row present for
/// the whole scan on exactly one page.
pub fn list_page(
    conn: &Connection,
    after: &str,
    limit: usize,
) -> Result<ListPage, InventoryReadError> {
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT {LIST_COLUMNS} FROM sandboxes WHERE id > ?1 ORDER BY id LIMIT ?2"
    ))?;
    let fetch = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
    let mut rows = stmt.query(rusqlite::params![after, fetch])?;
    let mut budget = InventoryBudget::new(limit, MAX_RAW_BYTES);
    let mut page = Vec::new();
    while let Some(row) = rows.next()? {
        match check_row(row, &mut budget) {
            Ok(()) => {}
            Err(InventoryReadError::Limit(_)) if !page.is_empty() => {
                return Ok(ListPage {
                    rows: page,
                    more: true,
                });
            }
            Err(error) => return Err(error),
        }
        page.push(match super::row_to_list_sandbox(row) {
            Ok(row) => ListPageRow::Decoded(row),
            Err(error) => ListPageRow::Undecodable {
                id: row.get("id")?,
                state: row.get::<_, String>("state").ok(),
                error: error.into(),
            },
        });
    }
    Ok(ListPage {
        rows: page,
        more: false,
    })
}

pub fn get(
    conn: &Connection,
    id: &str,
    budget: &mut InventoryBudget,
) -> Result<Option<super::SandboxRow>, InventoryReadError> {
    let mut stmt = conn.prepare_cached(super::GET_SANDBOX_SQL)?;
    let mut rows = stmt.query([id])?;
    rows.next()?
        .map(|row| {
            check_row(row, budget)?;
            Ok(super::row_to_sandbox(row)?)
        })
        .transpose()
}

pub fn lookup(
    conn: &Connection,
    selector: &str,
) -> Result<Option<super::SandboxRow>, InventoryReadError> {
    let mut budget = InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES);
    if let Some(row) = get(conn, selector, &mut budget)? {
        return Ok(Some(row));
    }
    let mut stmt = conn.prepare("SELECT id FROM sandboxes ORDER BY id")?;
    let mut rows = stmt.query([])?;
    let mut matched = None;
    while let Some(row) = rows.next()? {
        check_row(row, &mut budget)?;
        let id: String = row.get(0)?;
        if pplx_space_common_core::ids::sandbox_id_matches(&id, selector) {
            if matched.is_some() {
                return Err(InventoryReadError::Ambiguous);
            }
            matched = Some(id);
        }
    }
    matched
        .map(|id| {
            get(
                conn,
                &id,
                &mut InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES),
            )
        })
        .transpose()
        .map(Option::flatten)
}

/// Check auxiliary State fields before the existing typed readers allocate them.
/// The caller retains the same SQLite mutex across preflight and those reads.
pub fn check_state_metadata(
    conn: &Connection,
    ids: &[String],
    budget: &mut InventoryBudget,
) -> Result<(), InventoryReadError> {
    let mut stmt = conn.prepare_cached(
        "SELECT s.id, s.state, s.state_changed_at, s.transition_receipt_id,
                s.transition_receipt_kind, s.transition_receipt_target_state,
                s.transition_receipt_execution_generation, s.transition_receipt_reason_code,
                s.transition_receipt_activation_attempt_id, s.space_api_route_generation,
                s.space_api_route_execution_generation, s.space_api_route_substrate_kind,
                s.last_observed_route_generation, s.last_observed_execution_generation,
                s.last_observed_route_generation_substrate_kind, s.last_observed_route_substrate_kind,
                q.reason_code, q.quarantine_id, q.next_action_owner, q.attempt_count, q.next_action_after
           FROM sandboxes s LEFT JOIN sandbox_quarantine_state q ON q.sandbox_id = s.id
          WHERE s.id = ?1"
    )?;
    for id in ids {
        let mut rows = stmt.query([id])?;
        if let Some(row) = rows.next()? {
            check_row(row, budget)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
