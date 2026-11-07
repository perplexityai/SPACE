//! The node's activation seal, and the activation work in flight that a seal
//! waits for.
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
//! it detaches. So does a checkpoint or a stop that resumes a VM as part of
//! its own work, whether it began before or after the seal (see
//! [`ActivationSeal::continuation`]). The seal state lock orders every
//! admission against every seal: an activation is admitted and registered
//! entirely before a seal lands, and counts as in flight, or is refused.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::db::activation_seal::{ActivationSealRecord, write_activation_seal};

/// The longest a `SealActivations` call waits for in-flight work.
pub(crate) const MAX_SEAL_WAIT: Duration = Duration::from_secs(300);

/// What a piece of in-flight activation work is.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum InFlightActivationKind {
    Cold,
    Restore,
    ResidentResume,
    /// A full checkpoint of a running VM, from before it pauses the VM until
    /// it has resumed it or given up.
    Checkpoint,
    /// A stop's resume of a paused VM so its guest can flush, until the
    /// stop's node cleanup of the VM returns.
    StopFlush,
}

impl InFlightActivationKind {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 5] = [
        Self::Cold,
        Self::Restore,
        Self::ResidentResume,
        Self::Checkpoint,
        Self::StopFlush,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Restore => "restore",
            Self::ResidentResume => "resident_resume",
            Self::Checkpoint => "checkpoint",
            Self::StopFlush => "stop_flush",
        }
    }

    /// Whether the seal refuses new work of this kind. The others are a
    /// checkpoint's and a stop's own resumes, which run whether or not the
    /// node is sealed.
    pub(crate) const fn is_activation(self) -> bool {
        match self {
            Self::Cold | Self::Restore | Self::ResidentResume => true,
            Self::Checkpoint | Self::StopFlush => false,
        }
    }
}

impl std::fmt::Display for InFlightActivationKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The seal a node is under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Sealed {
    pub(crate) epoch: u64,
    /// Wall-clock Unix milliseconds at which this seal began.
    pub(crate) since_ms: u64,
}

/// One piece of activation work in flight.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InFlightActivation {
    pub(crate) kind: InFlightActivationKind,
    pub(crate) sandbox_id: String,
    pub(crate) age: Duration,
}

/// The seal, the admission count and the work in flight, read together.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActivationSealStatus {
    pub(crate) sealed: Option<Sealed>,
    pub(crate) activations_admitted: u64,
    /// Oldest first.
    pub(crate) in_flight: Vec<InFlightActivation>,
}

/// What `SealActivations` answers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SealOutcome {
    pub(crate) status: ActivationSealStatus,
    pub(crate) quiesced: bool,
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
    admitted: u64,
}

impl SealState {
    fn sealed(&self) -> Option<Sealed> {
        let record = self.record.as_ref().ok().copied().flatten()?;
        Some(Sealed {
            epoch: record.sealed_epoch?,
            since_ms: record.sealed_since_ms,
        })
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
            state: Mutex::new(SealState {
                record,
                admitted: 0,
            }),
            in_flight: Arc::new(InFlightRegistry::default()),
        };
        let state = seal.lock_state();
        pplx_space_metrics::sandbox::admission::node_activation_seal_sealed(
            state.record.is_err() || state.sealed().is_some(),
        );
        if let Some(sealed) = state.sealed() {
            tracing::info!(
                epoch = sealed.epoch,
                sealed_since_ms = sealed.since_ms,
                "sandbox-manager starts with VM activations sealed"
            );
        }
        drop(state);
        seal
    }

    /// A never-sealed seal for code under test that has no service database.
    #[cfg(test)]
    pub(crate) fn unsealed() -> Self {
        Self {
            state: Mutex::new(SealState {
                record: Ok(None),
                admitted: 0,
            }),
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
        debug_assert!(kind.is_activation(), "{kind} is not a VM activation");
        let mut state = self.lock_state();
        refuse_while_sealed(&state, kind, sandbox_id)?;
        state.admitted = state.admitted.saturating_add(1);
        // Registered before the state lock is released, so a seal that
        // lands next finds it in flight.
        Ok(self.in_flight.register(kind, sandbox_id))
    }

    /// Count a checkpoint's resume of the VM it paused, or a stop's resume of
    /// a paused VM so its guest can flush, as in flight until the operation
    /// is done with the VM. Never refused, and not an admission, even when
    /// the operation began after the seal: a checkpoint's resume only undoes
    /// its own pause, and a stop goes on to end its VM, while refusing either
    /// resume would leave the VM paused and the operation unable to finish.
    /// Counting it lets a seal wait for it.
    pub(crate) fn continuation(
        &self,
        kind: InFlightActivationKind,
        sandbox_id: &str,
    ) -> ActivationInFlightToken {
        debug_assert!(!kind.is_activation(), "{kind} is a VM activation");
        self.in_flight.register(kind, sandbox_id)
    }

    /// Seal under `epoch`, persisted before the seal takes effect. The same
    /// epoch again changes nothing; another epoch replaces the current one
    /// and starts a new seal.
    pub(crate) fn seal(
        &self,
        db: &crate::db::SharedConnection,
        epoch: u64,
    ) -> Result<Sealed, Status> {
        validate_epoch(epoch)?;
        let mut state = self.lock_state();
        let current = *state
            .record
            .as_ref()
            .map_err(|error| Status::internal(format!("activation seal unreadable: {error}")))?;
        if let Some(sealed) = state.sealed()
            && sealed.epoch == epoch
        {
            return Ok(sealed);
        }
        let now_ms = u64::try_from(jiff::Timestamp::now().as_millisecond()).unwrap_or(0);
        // Strictly after the last seal's start, so `(epoch, since)` names one
        // continuous seal even when a clock steps back or the same epoch is
        // sealed again within a millisecond of an unseal.
        let since_ms = current.map_or(now_ms, |record| {
            now_ms.max(record.sealed_since_ms.saturating_add(1))
        });
        let record = ActivationSealRecord {
            sealed_epoch: Some(epoch),
            sealed_since_ms: since_ms,
        };
        persist(db, record)?;
        let replaced_epoch = current.and_then(|record| record.sealed_epoch);
        state.record = Ok(Some(record));
        // Under the state lock, so gauges from racing seals and unseals land
        // in the order the state changed.
        pplx_space_metrics::sandbox::admission::node_activation_seal_sealed(true);
        drop(state);
        tracing::info!(
            epoch,
            replaced_epoch,
            sealed_since_ms = since_ms,
            "sealed sandbox-manager VM activations"
        );
        Ok(Sealed { epoch, since_ms })
    }

    /// Clear the seal if it is under `epoch`; an unsealed node stays
    /// unsealed.
    pub(crate) fn unseal(
        &self,
        db: &crate::db::SharedConnection,
        epoch: u64,
    ) -> Result<(), Status> {
        validate_epoch(epoch)?;
        let mut state = self.lock_state();
        state
            .record
            .as_ref()
            .map_err(|error| Status::internal(format!("activation seal unreadable: {error}")))?;
        let Some(sealed) = state.sealed() else {
            return Ok(());
        };
        if sealed.epoch != epoch {
            return Err(Status::failed_precondition(format!(
                "activations are sealed under epoch {}, not {epoch}",
                sealed.epoch
            )));
        }
        let record = ActivationSealRecord {
            sealed_epoch: None,
            sealed_since_ms: sealed.since_ms,
        };
        persist(db, record)?;
        state.record = Ok(Some(record));
        pplx_space_metrics::sandbox::admission::node_activation_seal_sealed(false);
        drop(state);
        tracing::info!(epoch, "unsealed sandbox-manager VM activations");
        Ok(())
    }

    pub(crate) fn status(&self) -> ActivationSealStatus {
        let state = self.lock_state();
        let sealed = state.sealed();
        let activations_admitted = state.admitted;
        drop(state);
        ActivationSealStatus {
            sealed,
            activations_admitted,
            in_flight: self.in_flight.list(),
        }
    }

    /// Wait up to `timeout` for no activation work to be in flight; whether
    /// none was when the wait ended.
    pub(crate) async fn wait_quiesced(&self, timeout: Duration) -> bool {
        let mut changed = self.in_flight.changed.subscribe();
        let quiesced = async {
            while !self.in_flight.is_empty() {
                if changed.changed().await.is_err() {
                    break;
                }
            }
        };
        tokio::time::timeout(timeout, quiesced).await.is_ok() && self.in_flight.is_empty()
    }

    /// Seal under `epoch` and wait up to `wait` for in-flight work, or until
    /// `shutdown` is cancelled.
    pub(crate) async fn seal_and_wait(
        &self,
        db: &crate::db::SharedConnection,
        epoch: u64,
        wait: Duration,
        shutdown: &CancellationToken,
    ) -> Result<SealOutcome, Status> {
        if wait > MAX_SEAL_WAIT {
            return Err(Status::invalid_argument(format!(
                "wait_timeout is at most {}s",
                MAX_SEAL_WAIT.as_secs()
            )));
        }
        self.seal(db, epoch)?;
        let quiesced = tokio::select! {
            quiesced = self.wait_quiesced(wait) => quiesced,
            // The process is ending, and the persisted seal outlives it;
            // answer whether anything is in flight now.
            () = shutdown.cancelled() => true,
        };
        let status = self.status();
        Ok(SealOutcome {
            quiesced: quiesced && status.in_flight.is_empty(),
            status,
        })
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
    let seal = match (&state.record, state.sealed()) {
        (Err(_), _) => "the seal state is unreadable".to_string(),
        (Ok(_), Some(sealed)) => format!("epoch {}", sealed.epoch),
        (Ok(_), None) => return Ok(()),
    };
    Err(Status::failed_precondition(format!(
        "{ACTIVATIONS_SEALED_MARKER} ({seal}); refusing the {kind} activation of sandbox \
         {sandbox_id}"
    )))
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

struct InFlightEntry {
    kind: InFlightActivationKind,
    sandbox_id: String,
    started_at: tokio::time::Instant,
}

struct InFlightRegistry {
    entries: Mutex<BTreeMap<u64, InFlightEntry>>,
    next_id: AtomicU64,
    changed: watch::Sender<()>,
}

impl Default for InFlightRegistry {
    fn default() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(0),
            changed: watch::Sender::new(()),
        }
    }
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
        self.changed.send_replace(());
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
        drop(entries);
        self.changed.send_replace(());
    }

    fn is_empty(&self) -> bool {
        self.lock_entries().is_empty()
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
    pplx_space_metrics::sandbox::admission::node_activation_seal_in_flight(kind.as_str(), count);
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
