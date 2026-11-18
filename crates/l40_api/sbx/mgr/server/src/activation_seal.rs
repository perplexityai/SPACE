//! The node's activation seal, and the VM activations in flight, which a
//! drain reports as pending.
//!
//! While sealed under an epoch, [`ActivationSeal::admit`] refuses every VM
//! activation: the cold boot of a sandbox, the restore of one from a snapshot
//! and the resume of a resident paused VM, which
//! `SandboxManagerService::admit_vm_activation` is the one entry point for.
//! The seal is the `activation_seal` row in sandbox-manager's database, read
//! once when the service is built and kept in memory, so it holds across a
//! restart and an activation reads no database.
//!
//! The seal is in one of three states: unsealed, sealed under an epoch, or
//! unknown, when the startup read failed. Unknown is not unsealed: it refuses
//! every VM activation with a retryable `Unavailable`
//! ([`ACTIVATION_SEAL_UNREADABLE_MARKER`]), since the row it couldn't read may
//! say sealed. [`ActivationSeal::recover`] reads the row again in the
//! background with a short bounded backoff, off the async runtime and without
//! the state lock, so activations are refused, not blocked, meanwhile. It
//! then publishes what it read under the state lock, only while the seal is
//! still unknown: a seal or cancel that read the row in between wins. While
//! it still can't
//! read, it logs a warning every [`UNREADABLE_WARN_INTERVAL`] naming the
//! error and how long the seal has been unknown.
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

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::db::SharedConnection;
use crate::db::activation_seal::ActivationSealRecord;
#[cfg(test)]
use crate::db::activation_seal::{DrainPrior, write_activation_seal};

/// The marker in the `FailedPrecondition` status a sealed node refuses a VM
/// activation with. The node refuses the same activation until it is
/// unsealed, so a caller places the work on another node or waits for the
/// unseal rather than retrying here.
pub(crate) const ACTIVATIONS_SEALED_MARKER: &str = "node activations are sealed";

/// The marker in the `Unavailable` status a node whose seal row couldn't be
/// read refuses a VM activation with. The background read clears it within
/// seconds once the row reads, so a caller may retry.
pub(crate) const ACTIVATION_SEAL_UNREADABLE_MARKER: &str = "activation seal unreadable; retrying";

/// The first and the longest wait between two reads of a seal row that
/// couldn't be read.
const RECOVERY_BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const RECOVERY_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// How often a seal row that stays unreadable logs a warning, the startup
/// error being the first line.
const UNREADABLE_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// The seal and the activations in flight, read together.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActivationSealStatus {
    /// The epoch the node is sealed under.
    pub(crate) sealed_epoch: Option<u64>,
    /// The sandbox of each activation in flight, oldest first.
    pub(crate) in_flight: Vec<String>,
}

pub(crate) struct ActivationSeal {
    state: Mutex<SealState>,
    in_flight: Arc<InFlightRegistry>,
}

struct SealState {
    /// The persisted row as last read or written, or why the last read
    /// failed: the seal is unknown then, and refuses every activation until
    /// a read succeeds ([`SealState::read_again`]).
    record: Result<Option<ActivationSealRecord>, String>,
    /// While the seal is unknown: since when, and when it last logged above
    /// debug level.
    unreadable: Option<Unreadable>,
}

#[derive(Clone, Copy)]
struct Unreadable {
    since: Instant,
    last_logged: Instant,
}

impl Unreadable {
    fn start() -> Self {
        let now = Instant::now();
        Self {
            since: now,
            last_logged: now,
        }
    }
}

impl SealState {
    fn is_unknown(&self) -> bool {
        self.record.is_err()
    }

    fn sealed_epoch(&self) -> Option<u64> {
        self.record
            .as_ref()
            .ok()
            .copied()
            .flatten()
            .filter(|record| record.sealed)
            .map(|record| record.epoch)
    }

    /// The readable row, `None` on a node never sealed or cancelled. A row
    /// the last read couldn't read is read again first; one that still can't
    /// be read refuses the seal or cancel that asked.
    #[cfg(test)]
    fn current(&mut self, db: &SharedConnection) -> Result<Option<ActivationSealRecord>, Status> {
        if self.is_unknown() {
            let conn = db
                .lock()
                .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
            self.read_again(&conn);
        }
        self.record
            .as_ref()
            .copied()
            .map_err(|error| Status::internal(format!("activation seal unreadable: {error}")))
    }

    /// Read the row again while the seal is unknown, and publish it once it
    /// reads, which the caller does under the state lock, so no activation is
    /// admitted between the read and its publication.
    #[cfg(test)]
    fn read_again(&mut self, conn: &crate::db::Connection) {
        if !self.is_unknown() {
            return;
        }
        self.publish_read(
            crate::db::read::activation_seal(conn).map_err(|error| error.to_string()),
        );
    }

    /// Publish a read of the row while the seal is unknown. Logs the
    /// recovery once: nothing makes a known seal unknown again.
    fn publish_read(&mut self, read: Result<Option<ActivationSealRecord>, String>) {
        if !self.is_unknown() {
            return;
        }
        match read {
            Ok(record) => {
                self.record = Ok(record);
                let unreadable_secs = self
                    .unreadable
                    .take()
                    .map_or(0, |unreadable| unreadable.since.elapsed().as_secs());
                match record.filter(|record| record.sealed) {
                    Some(sealed) => tracing::info!(
                        epoch = sealed.epoch,
                        unreadable_secs,
                        "read the activation seal again: VM activations are sealed"
                    ),
                    None => tracing::info!(
                        unreadable_secs,
                        "read the activation seal again: VM activations are admitted"
                    ),
                }
            }
            Err(error) => {
                let now = Instant::now();
                let unreadable = self.unreadable.get_or_insert_with(Unreadable::start);
                if now.duration_since(unreadable.last_logged) >= UNREADABLE_WARN_INTERVAL {
                    unreadable.last_logged = now;
                    tracing::warn!(
                        %error,
                        unreadable_secs = now.duration_since(unreadable.since).as_secs(),
                        "the activation seal is still unreadable; refusing VM activations"
                    );
                } else {
                    tracing::debug!(%error, "the activation seal is still unreadable");
                }
                self.record = Err(error);
            }
        }
    }
}

impl ActivationSeal {
    /// Read the persisted seal. A read that fails leaves the seal unknown,
    /// refusing every activation until [`Self::recover`] or a seal or cancel
    /// reads the row.
    pub(crate) fn load(conn: &crate::db::Connection) -> Self {
        let record = crate::db::read::activation_seal(conn).map_err(|error| {
            tracing::error!(
                %error,
                "cannot read the activation seal; refusing VM activations until it reads"
            );
            error.to_string()
        });
        if let Ok(Some(current)) = record
            && current.sealed
        {
            tracing::info!(
                epoch = current.epoch,
                "sandbox-manager starts with VM activations sealed"
            );
        }
        Self {
            state: Mutex::new(SealState {
                unreadable: record.is_err().then(Unreadable::start),
                record,
            }),
            in_flight: Arc::new(InFlightRegistry::default()),
        }
    }

    /// A never-sealed seal for code under test that has no service database.
    #[cfg(test)]
    pub(crate) fn unsealed() -> Self {
        Self {
            state: Mutex::new(SealState {
                record: Ok(None),
                unreadable: None,
            }),
            in_flight: Arc::new(InFlightRegistry::default()),
        }
    }

    /// Read the row again in the background while the startup read left the
    /// seal unknown; nothing to do once it is known.
    pub(crate) fn spawn_recovery(
        self: &Arc<Self>,
        db: SharedConnection,
        shutdown: CancellationToken,
        task_tracker: &tokio_util::task::TaskTracker,
    ) {
        if !self.lock_state().is_unknown() {
            return;
        }
        task_tracker.spawn(Arc::clone(self).recover(
            db,
            shutdown,
            RECOVERY_BACKOFF_INITIAL,
            RECOVERY_BACKOFF_MAX,
        ));
    }

    /// Read the row again, waiting `initial` and then twice as long each
    /// time up to `max`, until it reads or `shutdown` is cancelled. A seal or
    /// cancel that reads it first ends the retry.
    pub(crate) async fn recover(
        self: Arc<Self>,
        db: SharedConnection,
        shutdown: CancellationToken,
        initial: Duration,
        max: Duration,
    ) {
        let mut delay = initial;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(delay) => {}
            }
            if !self.lock_state().is_unknown() {
                return;
            }
            // Off the runtime, holding only the database's own mutex: the
            // state lock stays free for activations, which it refuses as
            // retryable meanwhile, and for a seal or cancel.
            let read_db = db.clone();
            let read = tokio::task::spawn_blocking(move || {
                read_db.lock().ok().map(|conn| {
                    crate::db::read::activation_seal(&conn).map_err(|error| error.to_string())
                })
            })
            .await;
            // A poisoned database mutex, or a read that panicked, is retried
            // like a failed read.
            if let Ok(Some(read)) = read
                && self.publish_recovered(read)
            {
                return;
            }
            delay = delay.saturating_mul(2).min(max);
        }
    }

    /// Publish a background read of the row under the state lock, only while
    /// the seal is still unknown: a seal or cancel that read the row since
    /// wins. Whether the seal is known now.
    fn publish_recovered(&self, read: Result<Option<ActivationSealRecord>, String>) -> bool {
        let mut state = self.lock_state();
        state.publish_read(read);
        !state.is_unknown()
    }

    /// Refuse an activation while sealed or unknown, without admitting it.
    /// `activation` names its kind in the refusal.
    pub(crate) fn check(&self, activation: &str, sandbox_id: &str) -> Result<(), Status> {
        refuse_while_sealed(&self.lock_state(), activation, sandbox_id)
    }

    /// Admit one VM activation, or refuse it while sealed or unknown. The token counts
    /// the activation as in flight until it is dropped.
    pub(crate) fn admit(
        &self,
        activation: &str,
        sandbox_id: &str,
    ) -> Result<ActivationInFlightToken, Status> {
        let state = self.lock_state();
        refuse_while_sealed(&state, activation, sandbox_id)?;
        // Registered before the state lock is released, so a seal that
        // lands next finds it in flight.
        Ok(self.in_flight.register(sandbox_id))
    }

    /// Seal under `epoch`, persisted before the seal takes effect. The same
    /// epoch again changes nothing; a later epoch replaces the current one.
    /// Epochs only move forward: an epoch below the current one, or a
    /// cancelled one, is refused.
    #[cfg(test)]
    pub(crate) fn seal(&self, db: &SharedConnection, epoch: u64) -> Result<(), Status> {
        validate_epoch(epoch)?;
        let mut state = self.lock_state();
        let current = state.current(db)?;
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

    /// Record that a drain suspend of `sandbox_id` under `epoch` begins from
    /// `prior`, if the node is still sealed under `epoch`, keeping an earlier
    /// attempt's record; the prior the record holds, or `None` when the node
    /// is not sealed under `epoch`. Under the state lock, which every seal
    /// and cancel holds, so no record is written once a cancel or a later
    /// seal has taken over from `epoch`.
    #[cfg(test)]
    pub(crate) fn record_drain_suspend(
        &self,
        db: &SharedConnection,
        epoch: u64,
        sandbox_id: &str,
        prior: DrainPrior,
    ) -> Result<Option<DrainPrior>, Status> {
        let state = self.lock_state();
        if state.sealed_epoch() != Some(epoch) {
            return Ok(None);
        }
        let conn = db
            .lock()
            .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
        let recorded = crate::db::activation_seal::record_drain_suspend(&conn, sandbox_id, prior)
            .map_err(|error| {
            Status::internal(format!("record the drain suspend of {sandbox_id}: {error}"))
        })?;
        drop(conn);
        drop(state);
        Ok(Some(recorded))
    }

    /// Cancel `epoch`: clear the seal if it is under `epoch` or an earlier
    /// one, and remember `epoch` so no seal at or below it is taken again,
    /// even one whose call arrives after this cancel. Every drain evacuation
    /// is kept, for the cancel to hand back. An epoch below the current
    /// seal's is refused and changes nothing.
    #[cfg(test)]
    pub(crate) fn unseal(&self, db: &SharedConnection, epoch: u64) -> Result<(), Status> {
        validate_epoch(epoch)?;
        let mut state = self.lock_state();
        let current = state.current(db)?;
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
        persist_cancel(db, record)?;
        state.record = Ok(Some(record));
        drop(state);
        if current.is_some_and(|current| current.sealed) {
            tracing::info!(epoch, "unsealed sandbox-manager VM activations");
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn status(&self) -> ActivationSealStatus {
        let sealed_epoch = self.lock_state().sealed_epoch();
        ActivationSealStatus {
            sealed_epoch,
            in_flight: self.in_flight.list(),
        }
    }

    /// The service tests' leak check: fails while any activation is still
    /// counted in flight.
    #[cfg(test)]
    #[track_caller]
    pub(crate) fn assert_quiesced(&self) {
        let in_flight = self.in_flight.list();
        assert!(
            in_flight.is_empty(),
            "activations still in flight: {in_flight:?}"
        );
    }

    fn lock_state(&self) -> MutexGuard<'_, SealState> {
        // Every critical section leaves the state whole before it can panic.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn refuse_while_sealed(
    state: &SealState,
    activation: &str,
    sandbox_id: &str,
) -> Result<(), Status> {
    if state.is_unknown() {
        return Err(Status::unavailable(format!(
            "{ACTIVATION_SEAL_UNREADABLE_MARKER}; refusing the {activation} activation of \
             sandbox {sandbox_id}"
        )));
    }
    let Some(epoch) = state.sealed_epoch() else {
        return Ok(());
    };
    Err(Status::failed_precondition(format!(
        "{ACTIVATIONS_SEALED_MARKER} (epoch {epoch}); refusing the {activation} activation of \
         sandbox {sandbox_id}"
    )))
}

#[cfg(test)]
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

#[cfg(test)]
fn validate_epoch(epoch: u64) -> Result<(), Status> {
    if epoch == 0 || i64::try_from(epoch).is_err() {
        return Err(Status::invalid_argument(
            "epoch must be nonzero and at most 2^63 - 1",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn persist(db: &SharedConnection, record: ActivationSealRecord) -> Result<(), Status> {
    let conn = db
        .lock()
        .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
    write_activation_seal(&conn, record)
        .map_err(|error| Status::internal(format!("persist the activation seal: {error}")))
}

/// Persist a cancel. Every drain evacuation is kept, for the cancel calls to
/// hand back until the control plane names each in `forget`.
#[cfg(test)]
fn persist_cancel(db: &SharedConnection, record: ActivationSealRecord) -> Result<(), Status> {
    let conn = db
        .lock()
        .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
    crate::db::activation_seal::cancel_drain(&conn, record)
        .map_err(|error| Status::internal(format!("persist the drain cancel: {error}")))
}

/// The sandbox of each activation in flight, by a token id issued in order.
#[derive(Default)]
struct InFlightRegistry {
    entries: Mutex<BTreeMap<u64, String>>,
    next_id: AtomicU64,
}

impl InFlightRegistry {
    fn register(self: &Arc<Self>, sandbox_id: &str) -> ActivationInFlightToken {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.lock_entries().insert(id, sandbox_id.to_owned());
        ActivationInFlightToken {
            registry: Arc::clone(self),
            id,
        }
    }

    fn release(&self, id: u64) {
        self.lock_entries().remove(&id);
    }

    #[cfg(test)]
    fn list(&self) -> Vec<String> {
        self.lock_entries().values().cloned().collect()
    }

    fn lock_entries(&self) -> MutexGuard<'_, BTreeMap<u64, String>> {
        // A token dropped while unwinding must still release its entry.
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Counts one VM activation as in flight until dropped, on
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
