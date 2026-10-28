//! A bounded memo of accepted jobs, not durable installation authority. rtmgr
//! owns jobs across restarts; losing a reply or this memo rejoins active work.

use std::{sync::Mutex, time::Duration};

use tokio::time::Instant;
use tonic::{Code, Status};

use super::{ArtifactManager, runtime};

pub(super) const RETRY_BACKOFF: Duration = Duration::from_secs(300);

#[derive(Clone)]
enum Attempt {
    Active(String),
    // Let the next fresh inventory observe publication before treating an
    // acknowledged success without verified bytes as a failed convergence.
    Published(String),
    Backoff { until: Instant, error: Status },
}

#[derive(Default)]
pub(crate) struct Provisioning {
    // Only artifacts of the last plan that reached reconcile, at most one
    // entry each: each such pass keeps only its missing artifacts. An empty
    // recipe clears the memo; an absent one, or a pass that fails before
    // reconcile, leaves it as it was.
    attempts: Mutex<Vec<(runtime::ArtifactIdentity, Attempt)>>,
}

impl Provisioning {
    fn with_attempts<T>(
        &self,
        action: impl FnOnce(&mut Vec<(runtime::ArtifactIdentity, Attempt)>) -> T,
    ) -> T {
        // Losing memo updates is safe: installs deduplicate in rtmgr and only
        // inventory establishes convergence. No lock is held across an await.
        action(
            &mut self
                .attempts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    pub(super) fn retain(&self, required: &[runtime::ArtifactIdentity]) {
        self.with_attempts(|attempts| attempts.retain(|(identity, _)| required.contains(identity)));
    }

    fn set(&self, identity: runtime::ArtifactIdentity, attempt: Attempt) {
        self.with_attempts(|attempts| {
            if let Some((_, current)) = attempts.iter_mut().find(|(key, _)| key == &identity) {
                *current = attempt;
            } else {
                attempts.push((identity, attempt));
            }
        });
    }

    pub(super) async fn ensure(
        &self,
        manager: &impl ArtifactManager,
        identity: runtime::ArtifactIdentity,
    ) -> Result<(), Status> {
        let previous = self.with_attempts(|attempts| {
            attempts
                .iter()
                .find(|(key, _)| key == &identity)
                .map(|(_, attempt)| attempt.clone())
        });
        let result = match previous {
            Some(Attempt::Backoff { until, error }) if Instant::now() < until => return Err(error),
            Some(Attempt::Published(id)) => Err(Status::failed_precondition(format!(
                "installation {id} succeeded but its artifact is still not verified in inventory"
            ))),
            Some(Attempt::Active(id)) => match manager.operation(id.clone()).await {
                Ok(operation) => accept(&identity, Some(&id), operation),
                // rtmgr no longer knows the job: it expired before this poll,
                // so no failure was observed. Submit afresh, which rejoins a
                // live job for the same identity.
                Err(error) if error.code() == Code::NotFound => submit(manager, &identity).await?,
                // A transport failure must not forget an otherwise live job.
                Err(error) => return Err(error),
            },
            None | Some(Attempt::Backoff { .. }) => submit(manager, &identity).await?,
        };
        match result {
            Ok(attempt) => {
                self.set(identity, attempt);
                Ok(())
            }
            Err(error) => {
                self.set(
                    identity,
                    Attempt::Backoff {
                        until: Instant::now() + RETRY_BACKOFF,
                        error: error.clone(),
                    },
                );
                Err(error)
            }
        }
    }
}

/// Submit an install. The outer error is a transient RPC failure that observed
/// no job outcome, so the caller retries next pass without backoff: a lost
/// reply may hide an accepted job, which the next submission rejoins by
/// identity. The inner result is the job's acceptance or a failure to back off.
async fn submit(
    manager: &impl ArtifactManager,
    identity: &runtime::ArtifactIdentity,
) -> Result<Result<Attempt, Status>, Status> {
    match manager.install(identity.clone()).await {
        Ok(operation) => Ok(accept(identity, None, operation)),
        Err(error)
            if matches!(
                error.code(),
                Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled
            ) =>
        {
            Err(error)
        }
        Err(error) => Ok(Err(error)),
    }
}

fn accept(
    identity: &runtime::ArtifactIdentity,
    expected_id: Option<&str>,
    operation: runtime::Operation,
) -> Result<Attempt, Status> {
    if operation.identity.as_ref() != Some(identity)
        || operation.action != runtime::ArtifactAction::Install as i32
        || operation.operation_id.is_empty()
        || expected_id.is_some_and(|id| id != operation.operation_id)
    {
        return Err(Status::data_loss(
            "install acknowledgement does not match request",
        ));
    }
    match runtime::OperationState::try_from(operation.state) {
        Ok(
            runtime::OperationState::Queued
            | runtime::OperationState::Downloading
            | runtime::OperationState::Publishing,
        ) => Ok(Attempt::Active(operation.operation_id)),
        Ok(runtime::OperationState::Succeeded) => Ok(Attempt::Published(operation.operation_id)),
        _ => Err(Status::failed_precondition(format!(
            "installation {} is not progressing: {}",
            operation.operation_id,
            operation
                .error
                .as_deref()
                .unwrap_or("unexpected operation state"),
        ))),
    }
}
