//! One State RPC batch: each requested sandbox's row and the publication
//! fields around it, every field with its own result.

use pplx_space_common_core::inventory::InventoryBudget;

use crate::db::inventory_read::InventoryReadError;
use crate::db::timestamps::EpochSeconds;
use crate::db::{
    ActivationRouteProvenance, Connection, Error, RawQuarantineRecoveryObservationRead,
    RawSandboxLifecycle, RawSandboxStatePublicationMetadataRead, Result, SandboxRow,
};

/// The two budgets a State RPC charges across all of its batches.
///
/// `metadata` is charged first, for each id's auxiliary State columns, so
/// oversized metadata fails the batch before any row is decoded. `raw` is
/// charged with each row's whole-row bytes as it is read.
pub struct StateBudgets {
    pub raw: InventoryBudget,
    pub metadata: InventoryBudget,
}

/// One requested id's result, in request order.
pub enum StatePageRow {
    /// No row with this id.
    Missing(String),
    /// The whole row decoded.
    Decoded(Box<DecodedStateRow>),
    /// The row exists but did not decode; only its raw lifecycle did.
    StateOnly(StateOnlyRow),
}

pub struct DecodedStateRow {
    pub row: SandboxRow,
    /// `None` when the batch read no provenance for this id.
    pub route_provenance: Option<Result<ActivationRouteProvenance>>,
    /// `None` unless the row is quarantined or carries a transition receipt.
    pub publication: Option<RawSandboxStatePublicationMetadataRead>,
    /// The diagnostic rendering of `state_changed_at`.
    pub state_changed_at: Option<String>,
    /// The pinned artifact's capture time, as
    /// [`crate::db::pinned_local_artifact_created_at`] reports it.
    pub pinned_artifact_created_at: Option<EpochSeconds>,
    /// `None` unless the row has an automatic-recovery observation.
    pub recovery_observation: Option<RawQuarantineRecoveryObservationRead>,
}

pub struct StateOnlyRow {
    pub sandbox_id: String,
    pub lifecycle: RawSandboxLifecycle,
    /// Why the whole row did not decode.
    pub error: Error,
    pub route_provenance: Option<Result<ActivationRouteProvenance>>,
}

/// A State batch read that failed as a whole.
#[derive(Debug, thiserror::Error)]
pub enum StatePageError {
    /// Preflighting the auxiliary State columns failed or exceeded
    /// [`StateBudgets::metadata`].
    #[error(transparent)]
    Metadata(InventoryReadError),
    #[error("load route identity provenance: {0}")]
    RouteProvenance(Error),
    /// A row exceeded [`StateBudgets::raw`].
    #[error(transparent)]
    Row(InventoryReadError),
    #[error("read raw sandbox state for {sandbox_id}: {error}")]
    RawLifecycle { sandbox_id: String, error: Error },
    #[error("load sandbox publication metadata: {0}")]
    Publication(Error),
    #[error("load state change times: {0}")]
    StateChangedAt(Error),
    #[error("load pinned artifact capture times: {0}")]
    PinnedArtifactCreatedAt(Error),
    #[error("load quarantine recovery observations: {0}")]
    RecoveryObservations(Error),
}

/// Read one batch of `ids` for the State RPC, charging `budgets`.
///
/// Rows come back in request order, one per id. The State RPC publishes the
/// state-only rows before the decoded ones, so a caller reproducing its
/// response partitions them rather than publishing in this order.
///
/// The metadata preflight is charged before any row is read, so a batch that
/// exceeds both budgets fails with [`StatePageError::Metadata`]. The caller
/// holds the connection guard across the whole batch: the preflight and the
/// reads it bounds must see the same rows.
pub fn state_page(
    conn: &Connection,
    ids: &[String],
    budgets: &mut StateBudgets,
) -> std::result::Result<Vec<StatePageRow>, StatePageError> {
    crate::db::inventory_read::check_state_metadata(conn, ids, &mut budgets.metadata)
        .map_err(StatePageError::Metadata)?;
    let mut route_provenance = crate::db::activation_route_provenance_for_sandboxes(conn, ids)
        .map_err(StatePageError::RouteProvenance)?;
    let mut page = Vec::with_capacity(ids.len());
    for sandbox_id in ids {
        match crate::db::inventory_read::get(conn, sandbox_id, &mut budgets.raw) {
            Ok(Some(row)) => page.push(PageEntry::Row(Box::new(row))),
            Ok(None) => page.push(PageEntry::Done(StatePageRow::Missing(sandbox_id.clone()))),
            Err(error @ (InventoryReadError::Limit(_) | InventoryReadError::Ambiguous)) => {
                return Err(StatePageError::Row(error));
            }
            Err(InventoryReadError::Database(error)) => {
                let lifecycle = match crate::db::raw_sandbox_lifecycle(conn, sandbox_id) {
                    Ok(Some(lifecycle)) => lifecycle,
                    Ok(None) => {
                        page.push(PageEntry::Done(StatePageRow::Missing(sandbox_id.clone())));
                        continue;
                    }
                    Err(error) => {
                        return Err(StatePageError::RawLifecycle {
                            sandbox_id: sandbox_id.clone(),
                            error,
                        });
                    }
                };
                page.push(PageEntry::Done(StatePageRow::StateOnly(StateOnlyRow {
                    route_provenance: route_provenance.remove(sandbox_id),
                    sandbox_id: sandbox_id.clone(),
                    lifecycle,
                    error,
                })));
            }
        }
    }
    let row_ids = page
        .iter()
        .filter_map(|entry| match entry {
            PageEntry::Row(row) => Some(row.id.clone()),
            PageEntry::Done(_) => None,
        })
        .collect::<Vec<_>>();
    let mut publications = crate::db::sandbox_state_publication_metadata(conn, &row_ids)
        .map_err(StatePageError::Publication)?;
    let mut state_changed_at = crate::db::sandbox_state_changed_at(conn, &row_ids)
        .map_err(StatePageError::StateChangedAt)?;
    let mut pinned_artifact_created_at =
        crate::db::pinned_local_artifact_created_at(conn, &row_ids)
            .map_err(StatePageError::PinnedArtifactCreatedAt)?;
    let mut recovery_observations = crate::db::quarantine_recovery_observations(conn, &row_ids)
        .map_err(StatePageError::RecoveryObservations)?;
    Ok(page
        .into_iter()
        .map(|entry| match entry {
            PageEntry::Done(row) => row,
            PageEntry::Row(row) => StatePageRow::Decoded(Box::new(DecodedStateRow {
                route_provenance: route_provenance.remove(&row.id),
                publication: publications.remove(&row.id),
                state_changed_at: state_changed_at.remove(&row.id),
                pinned_artifact_created_at: pinned_artifact_created_at.remove(&row.id),
                recovery_observation: recovery_observations.remove(&row.id),
                row: *row,
            })),
        })
        .collect())
}

/// A decoded row waits for the batch-wide reads keyed by the decoded ids;
/// every other result is final when its id is read.
enum PageEntry {
    Row(Box<SandboxRow>),
    Done(StatePageRow),
}
