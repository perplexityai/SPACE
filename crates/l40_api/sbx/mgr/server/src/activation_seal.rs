//! The node's activation seal, and the activation work in flight that a
//! drain waits for.
//!
//! While sealed under an epoch, [`ActivationSeal::admit`] refuses every VM
//! activation: the cold boot of a sandbox, the restore of one from a snapshot
//! and the resume of a resident paused VM, which
//! `SandboxManagerService::admit_vm_activation` is the one entry point for.
//! The seal is the `activation_seal` row in sandbox-manager's database, read
//! when the service is built, so it holds across a restart.
//!
//! Each admitted activation holds an [`ActivationInFlightToken`] from
//! admission until its last step finishes or fails, including the finalizer
//! it detaches. The seal state lock orders every admission against every
//! seal: an activation is admitted and registered entirely before a seal
//! lands, and counts as in flight, or is refused.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER;
use tonic::Status;

use crate::db::activation_seal::{ActivationSealRecord, write_activation_seal};

/// What a VM activation in flight is.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum InFlightActivationKind {
    Cold,
    Restore,
    ResidentResume,
}

impl InFlightActivationKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Restore => "restore",
            Self::ResidentResume => "resident_resume",
        }
    }
}

impl std::fmt::Display for InFlightActivationKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One piece of activation work in flight.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InFlightActivation {
    pub(crate) kind: InFlightActivationKind,
    pub(crate) sandbox_id: String,
    pub(crate) age: Duration,
}

/// The seal and the work in flight, read together.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActivationSealStatus {
    /// The epoch the node is sealed under.
    pub(crate) sealed_epoch: Option<u64>,
    /// Oldest first.
    pub(crate) in_flight: Vec<InFlightActivation>,
}

pub(crate) struct ActivationSeal {
    state: Mutex<SealState>,
    in_flight: Arc<InFlightRegistry>,
}

struct SealState {
    /// The persisted row as last read or written, or why the startup read
    /// failed. An unreadable seal refuses every activation; startup fails on
    /// it before the service serves (see [`ActivationSeal::load_error`]).
    record: Result<Option<ActivationSealRecord>, String>,
}

impl SealState {
    fn sealed_epoch(&self) -> Option<u64> {
        self.record
            .as_ref()
            .ok()
            .copied()
            .flatten()
            .filter(|record| record.sealed)
            .map(|record| record.epoch)
    }

    /// The readable row, `None` on a node never sealed or cancelled.
    fn current(&self) -> Result<Option<ActivationSealRecord>, Status> {
        self.record
            .as_ref()
            .copied()
            .map_err(|error| Status::internal(format!("activation seal unreadable: {error}")))
    }
}

impl ActivationSeal {
    /// Read the persisted seal. A read that fails leaves the seal refusing
    /// every activation, and [`Self::load_error`] reports why.
    pub(crate) fn load(conn: &crate::db::Connection) -> Self {
        let record = crate::db::read::activation_seal(conn).map_err(|error| {
            tracing::error!(%error, "cannot read the activation seal; refusing every VM activation");
            error.to_string()
        });
        let seal = Self {
            state: Mutex::new(SealState { record }),
            in_flight: Arc::new(InFlightRegistry::default()),
        };
        let state = seal.lock_state();
        pplx_space_sandbox_metrics::admission::node_activation_seal_sealed(
            state.record.is_err() || state.sealed_epoch().is_some(),
        );
        if let Some(epoch) = state.sealed_epoch() {
            tracing::info!(epoch, "sandbox-manager starts with VM activations sealed");
        }
        drop(state);
        seal
    }

    /// A never-sealed seal for code under test that has no service database.
    #[cfg(test)]
    pub(crate) fn unsealed() -> Self {
        Self {
            state: Mutex::new(SealState { record: Ok(None) }),
            in_flight: Arc::new(InFlightRegistry::default()),
        }
    }

    /// Why the startup read of the seal failed, if it did.
    pub(crate) fn load_error(&self) -> Option<String> {
        self.lock_state().record.as_ref().err().cloned()
    }

    /// Refuse an activation while sealed, without admitting it.
    pub(crate) fn check(
        &self,
        kind: InFlightActivationKind,
        sandbox_id: &str,
    ) -> Result<(), Status> {
        refuse_while_sealed(&self.lock_state(), kind, sandbox_id)
    }

    /// Admit one VM activation, or refuse it while sealed. The token counts
    /// the activation as in flight until it is dropped.
    pub(crate) fn admit(
        &self,
        kind: InFlightActivationKind,
        sandbox_id: &str,
    ) -> Result<ActivationInFlightToken, Status> {
        let state = self.lock_state();
        refuse_while_sealed(&state, kind, sandbox_id)?;
        // Registered before the state lock is released, so a seal that
        // lands next finds it in flight.
        Ok(self.in_flight.register(kind, sandbox_id))
    }

    /// Seal under `epoch`, persisted before the seal takes effect. The same
    /// epoch again changes nothing; a later epoch replaces the current one.
    /// Epochs only move forward: an epoch below the current one, or a
    /// cancelled one, is refused.
    pub(crate) fn seal(&self, db: &crate::db::SharedConnection, epoch: u64) -> Result<(), Status> {
        validate_epoch(epoch)?;
        let mut state = self.lock_state();
        let current = state.current()?;
        if let Some(current) = current {
            if current.sealed && current.epoch == epoch {
                return Ok(());
            }
            if epoch <= current.epoch {
                return Err(refuse_stale_epoch(current, epoch));
            }
        }
        let record = ActivationSealRecord {
            epoch,
            sealed: true,
        };
        persist(db, record)?;
        state.record = Ok(Some(record));
        // Under the state lock, so gauges from racing seals and unseals land
        // in the order the state changed.
        pplx_space_sandbox_metrics::admission::node_activation_seal_sealed(true);
        drop(state);
        tracing::info!(
            epoch,
            replaced_epoch = current
                .filter(|record| record.sealed)
                .map(|record| record.epoch),
            "sealed sandbox-manager VM activations"
        );
        Ok(())
    }

    /// Cancel `epoch`: clear the seal if it is under `epoch` or an earlier
    /// one, and remember `epoch` so no seal at or below it is taken again,
    /// even one whose call arrives after this cancel. The drain evacuations
    /// begun under it or earlier are forgotten with it, on every cancel, so a
    /// repeated cancel also forgets what a call that outlived the first one
    /// recorded. An epoch below the current seal's is refused and changes
    /// nothing.
    pub(crate) fn unseal(
        &self,
        db: &crate::db::SharedConnection,
        epoch: u64,
    ) -> Result<(), Status> {
        validate_epoch(epoch)?;
        let mut state = self.lock_state();
        let current = state.current()?;
        if let Some(current) = current
            && current.sealed
            && epoch < current.epoch
        {
            return Err(refuse_stale_epoch(current, epoch));
        }
        let record = ActivationSealRecord {
            epoch: current.map_or(epoch, |current| current.epoch.max(epoch)),
            sealed: false,
        };
        persist_cancel(db, record, record.epoch)?;
        state.record = Ok(Some(record));
        pplx_space_sandbox_metrics::admission::node_activation_seal_sealed(false);
        drop(state);
        if current.is_some_and(|current| current.sealed) {
            tracing::info!(epoch, "unsealed sandbox-manager VM activations");
        }
        Ok(())
    }

    pub(crate) fn status(&self) -> ActivationSealStatus {
        let sealed_epoch = self.lock_state().sealed_epoch();
        ActivationSealStatus {
            sealed_epoch,
            in_flight: self.in_flight.list(),
        }
    }

    /// The service tests' leak check: fails while any activation work is
    /// still counted in flight.
    #[cfg(test)]
    #[track_caller]
    pub(crate) fn assert_quiesced(&self) {
        let in_flight = self.in_flight.list();
        assert!(
            in_flight.is_empty(),
            "activation work still in flight: {in_flight:?}"
        );
    }

    fn lock_state(&self) -> MutexGuard<'_, SealState> {
        // Every critical section leaves the state whole before it can panic.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn refuse_while_sealed(
    state: &SealState,
    kind: InFlightActivationKind,
    sandbox_id: &str,
) -> Result<(), Status> {
    let seal = match (&state.record, state.sealed_epoch()) {
        (Err(_), _) => "the seal state is unreadable".to_string(),
        (Ok(_), Some(epoch)) => format!("epoch {epoch}"),
        (Ok(_), None) => return Ok(()),
    };
    Err(Status::failed_precondition(format!(
        "{ACTIVATIONS_SEALED_MARKER} ({seal}); refusing the {kind} activation of sandbox \
         {sandbox_id}"
    )))
}

fn refuse_stale_epoch(current: ActivationSealRecord, epoch: u64) -> Status {
    Status::failed_precondition(if current.sealed {
        format!(
            "activations are sealed under epoch {}, later than {epoch}",
            current.epoch
        )
    } else {
        format!(
            "epoch {epoch} is not after the cancelled epoch {}",
            current.epoch
        )
    })
}

fn validate_epoch(epoch: u64) -> Result<(), Status> {
    if epoch == 0 || i64::try_from(epoch).is_err() {
        return Err(Status::invalid_argument(
            "epoch must be nonzero and at most 2^63 - 1",
        ));
    }
    Ok(())
}

fn persist(db: &crate::db::SharedConnection, record: ActivationSealRecord) -> Result<(), Status> {
    let conn = db
        .lock()
        .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
    write_activation_seal(&conn, record)
        .map_err(|error| Status::internal(format!("persist the activation seal: {error}")))
}

/// Persist a cancel and forget the drain evacuations it ends, in one
/// transaction.
fn persist_cancel(
    db: &crate::db::SharedConnection,
    record: ActivationSealRecord,
    cancelled: u64,
) -> Result<(), Status> {
    let conn = db
        .lock()
        .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
    crate::db::activation_seal::cancel_drain_epoch(&conn, record, cancelled)
        .map_err(|error| Status::internal(format!("persist the drain cancel: {error}")))
}

struct InFlightEntry {
    kind: InFlightActivationKind,
    sandbox_id: String,
    started_at: tokio::time::Instant,
}

#[derive(Default)]
struct InFlightRegistry {
    entries: Mutex<BTreeMap<u64, InFlightEntry>>,
    next_id: AtomicU64,
}

impl InFlightRegistry {
    fn register(
        self: &Arc<Self>,
        kind: InFlightActivationKind,
        sandbox_id: &str,
    ) -> ActivationInFlightToken {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut entries = self.lock_entries();
        entries.insert(
            id,
            InFlightEntry {
                kind,
                sandbox_id: sandbox_id.to_owned(),
                started_at: tokio::time::Instant::now(),
            },
        );
        emit_in_flight(&entries, kind);
        drop(entries);
        ActivationInFlightToken {
            registry: Arc::clone(self),
            id,
        }
    }

    fn release(&self, id: u64) {
        let mut entries = self.lock_entries();
        if let Some(entry) = entries.remove(&id) {
            emit_in_flight(&entries, entry.kind);
        }
    }

    fn list(&self) -> Vec<InFlightActivation> {
        let now = tokio::time::Instant::now();
        // Ids are issued in order, so the map is oldest first.
        self.lock_entries()
            .values()
            .map(|entry| InFlightActivation {
                kind: entry.kind,
                sandbox_id: entry.sandbox_id.clone(),
                age: now.saturating_duration_since(entry.started_at),
            })
            .collect()
    }

    fn lock_entries(&self) -> MutexGuard<'_, BTreeMap<u64, InFlightEntry>> {
        // A token dropped while unwinding must still release its entry.
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn emit_in_flight(entries: &BTreeMap<u64, InFlightEntry>, kind: InFlightActivationKind) {
    let count = entries.values().filter(|entry| entry.kind == kind).count();
    pplx_space_sandbox_metrics::admission::node_activation_seal_in_flight(kind.as_str(), count);
}

/// Counts one piece of activation work as in flight until dropped, on
/// success, error, cancellation or unwinding alike. Separate from the
/// activation limiter's permit, which an activation gives back as soon as
/// node-manager answers its launch or resume.
#[must_use = "dropping the token ends the activation's in-flight count"]
pub(crate) struct ActivationInFlightToken {
    registry: Arc<InFlightRegistry>,
    id: u64,
}

impl std::fmt::Debug for ActivationInFlightToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActivationInFlightToken")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Drop for ActivationInFlightToken {
    fn drop(&mut self) {
        self.registry.release(self.id);
    }
}

#[cfg(test)]
mod tests;
