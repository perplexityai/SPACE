//! Inline execution of native creates, with independently gated warm and
//! bounded cold paths.
//!
//! The request records the create already claimed (see
//! `PublicOperationAdmission::create_claimed_or_roll_back`), so the
//! dispatcher never picks it up while this process runs it. The execution
//! runs in its own task through the same driver the dispatcher uses, so it
//! renews the claim and records the result identically. The request waits
//! for it only up to a short deadline. Past that deadline, or after a
//! client disconnect, the task keeps running. If the task dies, the claim
//! goes stale and the dispatcher re-drives the same operation.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument as _;

use crate::controllers::sandbox::SandboxController;
use crate::public_operation_driver::{
    OperationExecutionConfig, OperationExecutionResult, OperationExecutor, drive_claimed_operation,
    execution_span,
};
use crate::public_operations::{
    ClaimedPublicOperation, CreateClaimedPublicOperationResult, CreatePublicOperation,
    CreatePublicOperationResult, PublicOperationAdmission, PublicOperationStore,
    PublicOperationStoreError,
};
use crate::public_sandbox_operations::CreateSandboxOperationExecutor;

/// Kill switch for inline execution, read only while admission-time claims
/// are on. Off records claimed creates queued for the dispatcher, as before.
const NATIVE_CREATE_INLINE_ENV: &str = "SPACE_NATIVE_CREATE_INLINE_ENABLED";
const DEFAULT_NATIVE_CREATE_INLINE_ENABLED: bool = true;
/// How long the request waits for the inline execution before answering.
const NATIVE_CREATE_INLINE_WAIT_ENV: &str = "SPACE_NATIVE_CREATE_INLINE_WAIT_MS";
const DEFAULT_NATIVE_CREATE_INLINE_WAIT: Duration = Duration::from_millis(2_000);
const MAX_NATIVE_CREATE_INLINE_WAIT: Duration = Duration::from_secs(30);
const NATIVE_CREATE_INLINE_COLD_ENV: &str = "SPACE_NATIVE_CREATE_INLINE_COLD_ENABLED";
const NATIVE_CREATE_INLINE_COLD_WAIT_ENV: &str = "SPACE_NATIVE_CREATE_INLINE_COLD_WAIT_MS";
const NATIVE_CREATE_INLINE_COLD_CONCURRENCY_ENV: &str =
    "SPACE_NATIVE_CREATE_INLINE_COLD_CONCURRENCY";
const DEFAULT_NATIVE_CREATE_INLINE_COLD_WAIT: Duration = Duration::from_secs(10);
const DEFAULT_NATIVE_CREATE_INLINE_COLD_CONCURRENCY: usize = 4;

/// Shared across requests, including independently constructed app states.
/// Capacity is fixed on first use; changing it requires a process restart.
static COLD_PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct InlineColdCreate {
    pub(crate) inline: InlineCreate,
    pub(crate) permits: Arc<Semaphore>,
}

impl InlineColdCreate {
    pub(crate) fn from_env(pool: &sqlx::PgPool, controller: &SandboxController) -> Option<Self> {
        // Opt in separately: cold execution consumes more API resources and
        // requires the API to have all of the dispatcher's create settings.
        if !pplx_space_common_core::env::env_flag_var(NATIVE_CREATE_INLINE_COLD_ENV, false) {
            return None;
        }
        Some(Self {
            inline: InlineCreate {
                wait: configured_wait(
                    std::env::var(NATIVE_CREATE_INLINE_COLD_WAIT_ENV)
                        .ok()
                        .as_deref(),
                    DEFAULT_NATIVE_CREATE_INLINE_COLD_WAIT,
                ),
                executor: Arc::new(CreateSandboxOperationExecutor::new(
                    pool.clone(),
                    controller.clone(),
                )),
            },
            permits: Arc::clone(COLD_PERMITS.get_or_init(|| {
                Arc::new(Semaphore::new(cold_concurrency(
                    std::env::var(NATIVE_CREATE_INLINE_COLD_CONCURRENCY_ENV)
                        .ok()
                        .as_deref(),
                )))
            })),
        })
    }

    pub(crate) fn try_reserve(&self) -> Option<(InlineCreate, OwnedSemaphorePermit)> {
        Arc::clone(&self.permits)
            .try_acquire_owned()
            .ok()
            .map(|permit| (self.inline.clone(), permit))
    }
}

fn cold_concurrency(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value <= Semaphore::MAX_PERMITS)
        .unwrap_or(DEFAULT_NATIVE_CREATE_INLINE_COLD_CONCURRENCY)
}

/// Commit admission and, only when executing inline, claim in that same
/// transaction. Replays, conflicts and denials never acquire an execution claim.
pub(crate) async fn record(
    admission: PublicOperationAdmission,
    input: CreatePublicOperation<'_>,
    inline: bool,
) -> Result<
    (
        CreatePublicOperationResult,
        Option<(String, ClaimedPublicOperation)>,
    ),
    PublicOperationStoreError,
> {
    if !inline {
        return Ok((admission.create_or_roll_back(input).await?, None));
    }
    Ok(match admission.create_claimed_or_roll_back(input).await? {
        CreateClaimedPublicOperationResult::Claimed {
            claim_token,
            operation,
        } => (
            CreatePublicOperationResult::Created(operation.id),
            Some((claim_token, operation)),
        ),
        CreateClaimedPublicOperationResult::Existing(id) => {
            (CreatePublicOperationResult::Existing(id), None)
        }
        CreateClaimedPublicOperationResult::IdempotencyConflict {
            operation_id,
            expected_fingerprint,
        } => (
            CreatePublicOperationResult::IdempotencyConflict {
                operation_id,
                expected_fingerprint,
            },
            None,
        ),
        CreateClaimedPublicOperationResult::AdmissionDenied => {
            (CreatePublicOperationResult::AdmissionDenied, None)
        }
    })
}

pub(crate) type InlineExecution = Pin<Box<dyn Future<Output = OperationExecutionResult> + Send>>;

/// Runs one claimed create. Production runs the dispatcher's create
/// executor; tests substitute the execution.
pub(crate) trait InlineCreateExecutor: Send + Sync + 'static {
    fn execute(&self, operation: ClaimedPublicOperation) -> InlineExecution;
}

impl InlineCreateExecutor for CreateSandboxOperationExecutor {
    fn execute(&self, operation: ClaimedPublicOperation) -> InlineExecution {
        let executor = self.clone();
        Box::pin(async move { executor.execute(&operation).await.into() })
    }
}

/// How a claimed create runs inline.
#[derive(Clone)]
pub(crate) struct InlineCreate {
    pub(crate) wait: Duration,
    pub(crate) executor: Arc<dyn InlineCreateExecutor>,
}

impl InlineCreate {
    /// Inline execution through `controller`, or `None` when the kill switch
    /// is off.
    pub(crate) fn from_env(pool: &sqlx::PgPool, controller: &SandboxController) -> Option<Self> {
        if !pplx_space_common_core::env::env_flag_var(
            NATIVE_CREATE_INLINE_ENV,
            DEFAULT_NATIVE_CREATE_INLINE_ENABLED,
        ) {
            return None;
        }
        Some(Self {
            wait: inline_wait(std::env::var(NATIVE_CREATE_INLINE_WAIT_ENV).ok().as_deref()),
            executor: Arc::new(CreateSandboxOperationExecutor::new(
                pool.clone(),
                controller.clone(),
            )),
        })
    }

    /// Start executing `operation` under `claim_token` in its own task, and
    /// wait for it up to the deadline. Returning (or dropping this future)
    /// never stops the task. True when the task finished within the
    /// deadline, so whatever it recorded is already durable.
    pub(crate) async fn run(
        &self,
        store: PublicOperationStore,
        claim_token: String,
        operation: ClaimedPublicOperation,
        permit: Option<OwnedSemaphorePermit>,
    ) -> bool {
        let operation_id = operation.id;
        let task = self.spawn(store, claim_token, operation, permit);
        match tokio::time::timeout(self.wait, task).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                tracing::error!(
                    operation_id = %operation_id,
                    %error,
                    "inline native create task failed; the dispatcher reclaims it once its claim is stale"
                );
                false
            }
            Err(_) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    wait_ms = self.wait.as_millis(),
                    "inline native create still running at the deadline; answering queued"
                );
                false
            }
        }
    }

    /// The detached task that drives `operation` to its recorded outcome.
    fn spawn(
        &self,
        store: PublicOperationStore,
        claim_token: String,
        operation: ClaimedPublicOperation,
        permit: Option<OwnedSemaphorePermit>,
    ) -> tokio::task::JoinHandle<()> {
        let operation_id = operation.id;
        let span = execution_span(&operation);
        let execution = self.executor.execute(operation.clone());
        tokio::spawn(
            async move {
                // The execution, not the HTTP request, owns capacity through
                // the final durable outcome (including timeout/disconnect).
                let _permit = permit;
                match drive_claimed_operation(
                    store,
                    claim_token,
                    OperationExecutionConfig::default(),
                    operation,
                    OperationExecutor::Inline,
                    execution,
                )
                .await
                {
                    Ok(report) => tracing::debug!(
                        operation_id = %operation_id,
                        succeeded = report.succeeded,
                        failed = report.failed,
                        retried = report.retried,
                        stale_claims = report.stale_claims,
                        "inline native create completed"
                    ),
                    // The claim is left to go stale, and the dispatcher
                    // re-drives the operation.
                    Err(error) => tracing::warn!(
                        operation_id = %operation_id,
                        %error,
                        "inline native create could not record its outcome"
                    ),
                }
            }
            .instrument(span),
        )
    }
}

/// The configured wait, falling back to the default when unset or malformed
/// and capped so a bad value cannot hold requests open.
fn inline_wait(value: Option<&str>) -> Duration {
    configured_wait(value, DEFAULT_NATIVE_CREATE_INLINE_WAIT)
}

fn configured_wait(value: Option<&str>, default: Duration) -> Duration {
    value
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
        .min(MAX_NATIVE_CREATE_INLINE_WAIT)
}

#[cfg(test)]
mod tests;
