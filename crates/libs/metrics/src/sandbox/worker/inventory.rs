//! Inventory: list budgets, lifecycle counts, orphan rows and inventory reconciliation.

use super::*;

/// A complete unary sandbox List exceeded a strict inventory budget and was
/// served anyway. `rpc` and `limit` are bounded static labels.
pub fn unary_list_budget_exceeded(rpc: &'static str, limit: &'static str) {
    counter(
        "sandbox.worker.inventory.unary_list_budget_exceeded",
        1,
        &[tag("rpc", rpc), tag("limit", limit)],
    );
}

/// Current node-local durable sandbox rows in one lifecycle bucket. The caller
/// supplies a value decoded through the sandbox-manager's bounded state enum;
/// do not attach sandbox, pod, node, or execution identifiers.
pub fn lifecycle_count(value: u64, state: &str) {
    gauge(
        "sandbox.worker.lifecycle.count",
        value as f64,
        &[tag("state", state)],
    );
}

/// Disposition of an inventory-reconcile orphan row materialization — which
/// arm fired when the reconcile found sandbox evidence (disk or runtime)
/// with no sbx-manager metadata row. Shared by the
/// `sandbox.worker.orphan_row_materialized` counter tag and the
/// materialization warn log field so metric trends and per-sandbox log
/// forensics use the same vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrphanRowDisposition {
    /// A complete canonical suspend artifact reconstructed the missing row in
    /// the healthy suspended state.
    RecoveredSuspended,
    /// Valid cross-node restore metadata reconstructed a retryable Restoring
    /// row while one or more durable-backed artifacts were still absent.
    RecoveredRestoring,
    /// Cache dir with no live VM — preserved behind a quarantine row because
    /// its sbx-manager metadata row is missing.
    QuarantinedRow,
    /// A live runtime was found; the row was queued for cleanup.
    CleanupQuarantine,
    /// Artifact evidence only; the row exists just to drive artifact deletion.
    DeleteRow,
}

impl OrphanRowDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RecoveredSuspended => "recovered_suspended",
            Self::RecoveredRestoring => "recovered_restoring",
            Self::QuarantinedRow => "quarantined_row",
            Self::CleanupQuarantine => "cleanup_quarantine",
            Self::DeleteRow => "delete_row",
        }
    }
}

impl fmt::Display for OrphanRowDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OrphanRowDispositionParseError {
    #[error("unknown orphan-row disposition {0:?}")]
    Unknown(String),
}

impl FromStr for OrphanRowDisposition {
    type Err = OrphanRowDispositionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "recovered_suspended" => Ok(Self::RecoveredSuspended),
            "recovered_restoring" => Ok(Self::RecoveredRestoring),
            "quarantined_row" => Ok(Self::QuarantinedRow),
            "cleanup_quarantine" => Ok(Self::CleanupQuarantine),
            "delete_row" => Ok(Self::DeleteRow),
            other => Err(OrphanRowDispositionParseError::Unknown(other.to_string())),
        }
    }
}

/// The inventory reconcile materialized a DB row for sandbox evidence
/// (disk or runtime) that had no sbx-manager metadata row. See
/// [`OrphanRowDisposition`] for what each disposition means.
pub fn orphan_row_materialized(source: &str, disposition: OrphanRowDisposition) {
    counter(
        "sandbox.worker.orphan_row_materialized",
        1,
        &[
            tag("source", source),
            tag("disposition", disposition.as_str()),
        ],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryReconcileOperation {
    StaleRowRemoval,
    OrphanMaterialization,
}

impl InventoryReconcileOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StaleRowRemoval => "stale_row_removal",
            Self::OrphanMaterialization => "orphan_materialization",
        }
    }
}

impl fmt::Display for InventoryReconcileOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryReconcileOutcome {
    Applied,
    Deferred,
    Noop,
    Failed,
}

impl InventoryReconcileOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Deferred => "deferred",
            Self::Noop => "noop",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for InventoryReconcileOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryReconcileReason {
    Removed,
    Materialized,
    LiveLockBusy,
    RestoreStagingBusy,
    ImportStaging,
    DurableCleanupEpisode,
    RowCleanupIntent,
    LifecycleOwned,
    RestoreTransferScope,
    FilesystemChanged,
    DatabaseRowAppeared,
    WitnessChanged,
    AlreadyMissing,
    DatabaseError,
    MaterializationError,
    MalformedCleanupIntent,
    FilesystemInspectionFailed,
    /// A retained send source has no node-manager answer in this pass.
    RetainedSourcePending,
}

impl InventoryReconcileReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Removed => "removed",
            Self::Materialized => "materialized",
            Self::LiveLockBusy => "live_lock_busy",
            Self::RestoreStagingBusy => "restore_staging_busy",
            Self::ImportStaging => "import_staging",
            Self::DurableCleanupEpisode => "durable_cleanup_episode",
            Self::RowCleanupIntent => "row_cleanup_intent",
            Self::LifecycleOwned => "lifecycle_owned",
            Self::RestoreTransferScope => "restore_transfer_scope",
            Self::FilesystemChanged => "filesystem_changed",
            Self::DatabaseRowAppeared => "database_row_appeared",
            Self::WitnessChanged => "witness_changed",
            Self::AlreadyMissing => "already_missing",
            Self::DatabaseError => "database_error",
            Self::MaterializationError => "materialization_error",
            Self::MalformedCleanupIntent => "malformed_cleanup_intent",
            Self::FilesystemInspectionFailed => "filesystem_inspection_failed",
            Self::RetainedSourcePending => "retained_source_pending",
        }
    }
}

impl fmt::Display for InventoryReconcileReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn inventory_reconcile(
    operation: InventoryReconcileOperation,
    outcome: InventoryReconcileOutcome,
    reason: InventoryReconcileReason,
) {
    counter(
        "sandbox.worker.inventory_reconcile",
        1,
        &[
            tag("operation", operation),
            tag("outcome", outcome),
            tag("reason", reason),
        ],
    );
}
