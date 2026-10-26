//! Sequenced effects retain their owner and result when the caller disappears.
use super::*;
use pplx_space_common_grpc::RequestPermit;
use recovery::ProcessTarget;
use std::num::NonZeroU64;
use tokio_util::task::task_tracker::TaskTrackerToken;

mod ledger;
pub(super) use ledger::Ledger;
use ledger::{Binding, Outcome, Reservation};

pub(super) enum Mutation {
    // Own the zeroizing request until the effect settles; receipts keep only its digest.
    Input(SendInputRequest),
    Close,
    Signal(ProcessSignal),
}
impl Mutation {
    fn binding(&self) -> Binding {
        match self {
            Self::Input(request) => Binding::Input(
                aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &request.data)
                    .as_ref()
                    .try_into()
                    .expect("SHA256 length"),
            ),
            Self::Close => Binding::Close,
            Self::Signal(signal) => Binding::Signal(signal.as_i32()),
        }
    }
}

// Field order publishes an uncertain receipt before releasing the activation
// lease, even if the task panics or the runtime drops it.
struct OwnedMutation {
    owner: Option<ledger::Owner>,
    target: ProcessTarget,
    mutation: Mutation,
    _payload: RequestPermit,
    _admission: TaskTrackerToken,
}
impl OwnedMutation {
    async fn run(mut self, shutdown: CancellationToken) -> Outcome {
        if shutdown.is_cancelled() {
            return Outcome::NotBegun;
        }
        self.owner.as_mut().expect("unsettled owner").begin();
        let record = &self.target.record;
        let outcome = match &self.mutation {
            Mutation::Input(request) => {
                record.input.write_sequenced(&request.data, &shutdown).await
            }
            Mutation::Close => tokio::select! {
                biased;
                _ = shutdown.cancelled() => Outcome::Unknown,
                result = record.close_stdin() => Outcome::from_result(result),
            },
            Mutation::Signal(signal) => {
                let kill = signal.as_i32() == libc::SIGKILL;
                let result = match record.signal_handle.signal(*signal) {
                    Err(error) if kill && error.code() == tonic::Code::NotFound => Ok(()),
                    result => result,
                };
                if kill && result.is_ok() {
                    record.journal.release_backpressure();
                }
                Outcome::from_result(result)
            }
        };
        self.owner
            .take()
            .expect("unsettled owner")
            .complete(outcome.clone());
        outcome
    }
}

impl ExecutionService {
    pub(super) async fn mutate_process(
        &self,
        address: ProcessAddress,
        sequence: NonZeroU64,
        mutation: Mutation,
        payload: RequestPermit,
        admission: TaskTrackerToken,
    ) -> Result<(), Status> {
        payload
            .request_lifetime()
            .observe(async move {
                // Input hashing is bounded by payload admission and does not block Tokio.
                // A cancelled caller leaves these guards with the blocking task.
                let (mutation, binding, payload, admission) =
                    if matches!(&mutation, Mutation::Input(_)) {
                        self.inner
                            .process_tasks
                            .spawn_blocking(move || {
                                let binding = mutation.binding();
                                (mutation, binding, payload, admission)
                            })
                            .await
                            .map_err(|_| Status::internal("mutation preparation failed"))?
                    } else {
                        let binding = mutation.binding();
                        (mutation, binding, payload, admission)
                    };
                let target = self.select_process(address, TargetState::Retained).await?;
                let reservation = target
                    .record
                    .recovery
                    .as_ref()
                    .expect("validated recovery handle")
                    .mutations
                    .reserve(sequence, binding)?;
                let outcome = match reservation {
                    Reservation::Replay(outcome) => outcome,
                    Reservation::Wait(receiver) => ledger::wait(receiver).await,
                    Reservation::Owner(owner) => {
                        // A retained receipt is replayable after exit, but a new effect is not.
                        if target.record.journal.is_completed() {
                            return Err(exited_input_status());
                        }
                        let owned = OwnedMutation {
                            owner: Some(owner),
                            target,
                            mutation,
                            _payload: payload,
                            _admission: admission,
                        };
                        let shutdown = self.inner.shutdown.clone();
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        self.inner.process_tasks.spawn(async move {
                            let result = owned.run(shutdown).await;
                            // The receipt remains replayable if the requester went away.
                            let _ = tx.send(result);
                        });
                        rx.await.unwrap_or(Outcome::Unknown)
                    }
                };
                outcome.result()
            })
            .await
    }
}

impl ProcessInput {
    async fn write_sequenced(&self, data: &[u8], shutdown: &CancellationToken) -> Outcome {
        match self {
            Self::Disabled => Outcome::from_result(Err(Status::failed_precondition(
                "process stdin is not enabled or closed",
            ))),
            Self::Pipe(input) => write_descriptor(input, data, shutdown).await,
            Self::Pty(pty) => write_descriptor(&pty.writer, data, shutdown).await,
        }
    }
}

async fn write_descriptor<T: tokio::io::AsyncWrite + Unpin>(
    input: &InputDescriptor<T>,
    data: &[u8],
    shutdown: &CancellationToken,
) -> Outcome {
    let mut held = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Outcome::Interrupted {written: 0},
        held = input.lock() => held,
    };
    let Some(writer) = held.as_mut() else {
        return Outcome::from_result(Err(if input.is_released() {
            exited_input_status()
        } else {
            Status::failed_precondition("process stdin is not enabled or closed")
        }));
    };
    let mut written = 0;
    while written < data.len() {
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Outcome::Interrupted {written: written as u64},
            result = writer.write(data.get(written..).expect("writer progress stays within input")) => result,
        };
        match result {
            Ok(0) | Err(_) => {
                return Outcome::Interrupted {
                    written: written as u64,
                };
            }
            Ok(count) => written += count,
        }
    }
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => Outcome::Interrupted {written: written as u64},
        result = writer.flush() => match result {
            Ok(()) => Outcome::Applied,
            Err(_) => Outcome::Interrupted {written: written as u64},
        },
    }
}

#[cfg(test)]
mod tests;
