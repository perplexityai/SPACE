//! Router-lifetime retry authority, independent of executor and caller lifetimes.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use pplx_space_common_core::types::{GuestUserName, IdempotencyKey};
use pplx_space_spaced_guest_protocol::proto::worker::{
    MutationResult, OperationFailure, mutation_result,
};
use tokio::sync::{Notify, Semaphore};
use tonic::Status;

const RETENTION: Duration = Duration::from_secs(60);
// Guest-wide rather than worker-multiplied budgets. Refuse new work instead of
// evicting active executions or shortening an acknowledged replay window.
// Every keyed mutation keeps its record for RETENTION, so this caps the
// sustained rate at MAX_RECORDS / 60s: 4096 allowed only ~68 writes/s and
// failed a fifth of the writes of 30 agents sharing a sandbox. Records are
// small, and MAX_KEY_BYTES still bounds their memory.
const MAX_RECORDS: usize = 16_384;
const MAX_KEY_BYTES: usize = 16 * 1024 * 1024;
const MAX_WAITERS: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Kind {
    Write,
    Batch,
    Rename,
}

#[derive(Clone)]
pub(super) struct Operations {
    state: Arc<Mutex<State>>,
    waiters: Arc<Semaphore>,
}

#[derive(Default)]
struct State {
    // Record membership and byte accounting are one invariant. Do not recover
    // poisoned state by clearing records: that could authorize duplicate effects.
    records: HashMap<(GuestUserName, IdempotencyKey), Record>,
    bytes: usize,
    last_pruned: Option<Instant>,
    last_capacity_warning: Option<Instant>,
}

struct Record {
    kind: Kind,
    generation: Arc<Notify>,
    result: Option<(Result<MutationResult, Status>, Instant)>,
}

pub(super) enum Lookup {
    New(Reservation),
    Replay(Result<MutationResult, Status>),
}

#[must_use]
pub(super) struct Reservation {
    claim: Option<Claim>,
}

#[must_use]
pub(super) struct Execution {
    claim: Option<Claim>,
}

struct Claim {
    store: Operations,
    key: (GuestUserName, IdempotencyKey),
    generation: Arc<Notify>,
}

impl Operations {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::default(),
            waiters: Arc::new(Semaphore::new(MAX_WAITERS)),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| {
            // Effects execute outside this lock. Membership changes only on
            // proven release/expiry; retain every retry fence
            // and reconstruct accounting after an interrupted update.
            let mut state = poisoned.into_inner();
            state.bytes = state.records.keys().map(key_bytes).sum();
            self.state.clear_poison();
            state
        })
    }

    pub(super) async fn reserve(
        &self,
        user: GuestUserName,
        key: Option<IdempotencyKey>,
        kind: Kind,
    ) -> Result<Lookup, Status> {
        let Some(key) = key else {
            return Ok(Lookup::New(Reservation { claim: None }));
        };
        let key = (user, key);
        let _waiter = self.waiters.clone().try_acquire_owned().map_err(|_| {
            crate::refused(Status::resource_exhausted(
                "too many filesystem retry waiters",
            ))
        })?;
        let deadline = tokio::time::Instant::now() + RETENTION;
        loop {
            let notification = {
                let mut state = self.state();
                if state
                    .last_pruned
                    .is_none_or(|at| at.elapsed() >= Duration::from_secs(1))
                {
                    state.records.retain(|_, record| {
                        record
                            .result
                            .as_ref()
                            .is_none_or(|(_, at)| at.elapsed() < RETENTION)
                    });
                    state.bytes = state.records.keys().map(key_bytes).sum();
                    state.last_pruned = Some(Instant::now());
                }
                // Expiry of the requested key is exact even between sweeps.
                if state.records.get(&key).is_some_and(|record| {
                    record
                        .result
                        .as_ref()
                        .is_some_and(|(_, at)| at.elapsed() >= RETENTION)
                }) {
                    state.records.remove(&key);
                    state.bytes -= key_bytes(&key);
                }
                if let Some(record) = state.records.get(&key) {
                    if record.kind != kind {
                        return Err(Status::failed_precondition(
                            "idempotency_key used for different operation",
                        ));
                    }
                    if let Some((result, _)) = &record.result {
                        return Ok(Lookup::Replay(result.clone()));
                    }
                    let notification = record.generation.clone().notified_owned();
                    let mut notification = Box::pin(notification);
                    notification.as_mut().enable();
                    notification
                } else {
                    if state.records.len() >= MAX_RECORDS
                        || state.bytes.saturating_add(key_bytes(&key)) > MAX_KEY_BYTES
                    {
                        if state
                            .last_capacity_warning
                            .is_none_or(|at| at.elapsed() >= RETENTION)
                        {
                            tracing::warn!(
                                records = state.records.len(),
                                key_bytes = state.bytes,
                                "filesystem retry admission exhausted"
                            );
                            state.last_capacity_warning = Some(Instant::now());
                        }
                        return Err(crate::refused(Status::resource_exhausted(
                            "filesystem retry capacity exhausted",
                        )));
                    }
                    let generation = Arc::new(Notify::new());
                    state.bytes += key_bytes(&key);
                    state.records.insert(
                        key.clone(),
                        Record {
                            kind,
                            generation: generation.clone(),
                            result: None,
                        },
                    );
                    return Ok(Lookup::New(Reservation {
                        claim: Some(Claim {
                            store: self.clone(),
                            key,
                            generation,
                        }),
                    }));
                }
            };
            tokio::time::timeout_at(deadline, notification)
                .await
                .map_err(|_| {
                    Status::deadline_exceeded(
                        "filesystem operation still settling; no retry was executed",
                    )
                })?;
        }
    }
}

fn key_bytes(key: &(GuestUserName, IdempotencyKey)) -> usize {
    key.0.as_str().len() + key.1.as_str().len()
}

impl Reservation {
    pub(super) fn dispatch(mut self) -> Execution {
        Execution {
            claim: self.claim.take(),
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(claim) = self.claim.take() {
            claim.release();
        }
    }
}

impl Execution {
    pub(super) fn complete(mut self, result: Result<MutationResult, Status>) {
        if let Some(claim) = self.claim.take() {
            // A status code alone cannot prove absence of effects: ENOSPC,
            // for example, can follow a partial append. Only an executor's
            // explicit pre-execution refusal permits same-key redispatch.
            if matches!(
                &result,
                Ok(MutationResult {
                    outcome: Some(mutation_result::Outcome::RejectedBeforeExecution(_)),
                })
            ) {
                claim.release();
            } else {
                claim.complete(result);
            }
        }
    }
}

impl Drop for Execution {
    fn drop(&mut self) {
        if let Some(claim) = self.claim.take() {
            tracing::warn!("filesystem execution owner lost; retaining unknown outcome");
            claim.complete(Err(unknown_outcome()));
        }
    }
}

impl Claim {
    fn release(self) {
        let mut state = self.store.state();
        if state
            .records
            .get(&self.key)
            .is_some_and(|record| Arc::ptr_eq(&record.generation, &self.generation))
        {
            state.records.remove(&self.key);
            state.bytes -= key_bytes(&self.key);
            self.generation.notify_waiters();
        }
    }

    fn complete(self, result: Result<MutationResult, Status>) {
        let mut state = self.store.state();
        if let Some(record) = state.records.get_mut(&self.key)
            && Arc::ptr_eq(&record.generation, &self.generation)
        {
            record.result = Some((result, Instant::now()));
            self.generation.notify_waiters();
        }
    }
}

pub(super) fn unknown_outcome() -> Status {
    Status::failed_precondition("operation interrupted; outcome unknown")
}

pub(super) fn outcome(result: Result<mutation_result::Outcome, Status>) -> MutationResult {
    MutationResult {
        outcome: Some(match result {
            Ok(outcome) => outcome,
            Err(status) => mutation_result::Outcome::Failure(operation_failure(&status)),
        }),
    }
}

/// A failed operation's status as it crosses from a worker to the router,
/// keeping its error reason.
pub(super) fn operation_failure(status: &Status) -> OperationFailure {
    OperationFailure {
        code: status.code() as i32,
        message: status.message().to_owned(),
        reason: status
            .metadata()
            .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
            .and_then(|reason| reason.to_str().ok())
            .unwrap_or_default()
            .to_owned(),
    }
}

fn failure_status(failure: OperationFailure) -> Status {
    let mut status = Status::new(tonic::Code::from_i32(failure.code), failure.message);
    if let Ok(reason) = tonic::metadata::MetadataValue::try_from(failure.reason.as_str())
        && !failure.reason.is_empty()
    {
        status.metadata_mut().insert(
            pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY,
            reason,
        );
    }
    // The reason survives the worker round trip but the retry hint does not;
    // a refusal gets its hint back, so it reaches the caller whole.
    if pplx_space_common_grpc::is_refused_before_start(&status) {
        return crate::refused(status);
    }
    status
}

pub(super) fn checked_outcome(result: MutationResult) -> Result<mutation_result::Outcome, Status> {
    match result.outcome {
        Some(mutation_result::Outcome::Failure(error))
        | Some(mutation_result::Outcome::RejectedBeforeExecution(error)) => {
            Err(failure_status(error))
        }
        Some(mutation_result::Outcome::Interrupted(_)) | None => Err(unknown_outcome()),
        Some(outcome) => Ok(outcome),
    }
}

#[cfg(test)]
mod tests;
