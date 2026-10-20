//! One sandbox's whole row, decoded strictly, and the two raw projections
//! that a whole-row read cannot stand in for.

use pplx_space_common_core::inventory::InventoryBudget;
use pplx_space_common_core::types::SandboxId;

use crate::db::auto_lifecycle_delete_receipts::RouteIdentityAdoptionMetadata;
use crate::db::inventory_read::InventoryReadError;
use crate::db::{CleanupCompletion, Connection, Result, SandboxEraseSafety, SandboxRow};

/// The sandbox's whole row, `None` without one.
///
/// Strict: a column that does not decode fails the read. Callers that fail
/// closed on a corrupt row rely on that; a caller that must keep working
/// without a decodable row reads the narrower [`super::sandbox_view`].
pub fn sandbox(conn: &Connection, id: &str) -> Result<Option<SandboxRow>> {
    crate::db::get_sandbox(conn, id)
}

/// [`sandbox`] for a bounded inventory: the row's raw bytes are charged to
/// `budget` before it is decoded, so a limit failure allocates nothing.
pub fn sandbox_within_budget(
    conn: &Connection,
    id: &str,
    budget: &mut InventoryBudget,
) -> Result<Option<SandboxRow>, InventoryReadError> {
    crate::db::inventory_read::get(conn, id, budget)
}

/// The row's `state_changed_at` exactly as stored, `None` without a row.
///
/// [`SandboxRow`] does not carry this column: the value is a CAS witness, so
/// it is read raw rather than rendered, and a corrupt value reaches the
/// caller's refusal path instead of failing the read.
pub fn state_changed_at_witness(conn: &Connection, id: &str) -> Result<Option<String>> {
    crate::db::sandbox_state_changed_at_witness(conn, id)
}

/// The route-identity provenance columns an adoption decision compares.
/// Unlike [`sandbox`], a missing row is an error.
pub fn route_identity_adoption(
    conn: &Connection,
    sandbox_id: SandboxId,
) -> Result<RouteIdentityAdoptionMetadata> {
    crate::db::auto_lifecycle_delete_receipts::route_identity_adoption_metadata(conn, sandbox_id)
}

/// Questions answered from a row already read, without another statement.
impl SandboxRow {
    /// Whether any slot, process or socket of a runtime is still recorded.
    pub fn has_runtime_fields(&self) -> bool {
        crate::db::sandbox_has_runtime_fields(self)
    }

    /// A remote restore interrupted before it bound a runtime, an artifact or
    /// a cleanup intent, so it may be retried from its source.
    pub fn is_retryable_interrupted_restore(&self) -> bool {
        crate::db::sandbox_is_retryable_interrupted_restore(self)
    }

    /// The `(keep_filesystem, completion)` cleanup that startup recovery owes
    /// this row, if any.
    pub fn cleanup_intent_for_startup_recovery(&self) -> Option<(bool, CleanupCompletion)> {
        crate::db::cleanup_intent_for_startup_recovery(self)
    }

    /// Whether erasing this row and its filesystem would drop unpublished
    /// suspend state.
    pub fn erase_safety(&self) -> SandboxEraseSafety {
        crate::db::sandbox_erase_safety(
            self.state,
            self.cleanup_completion,
            self.failure_reason.as_deref(),
        )
    }
}
