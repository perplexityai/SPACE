//! Inline execution of a native create that won an admission-time
//! warm-pool claim.
//!
//! The request takes an inline permit, then records the create already
//! claimed (see `PublicOperationAdmission::create_claimed_or_roll_back`), so
//! the dispatcher never picks it up while this process runs it. The
//! execution runs on [`InlineOperations`], through the same driver and
//! settings the dispatcher uses, so it renews the claim and records the
//! result identically. The request waits for it only up to a short deadline.
//! Past that deadline, or after a client disconnect, the task keeps running.
//! If the task dies, the claim goes stale and the dispatcher re-drives the
//! same operation through the admitted-claim path.
//!
//! Without a permit (none free, the process draining, no route-signing
//! authority), a create that wins a claim is recorded queued for the
//! dispatcher instead, as with the kill switch off.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::inline_operations::{InlineOperations, InlinePermit};
use crate::public_operation_driver::OperationExecutionResult;
use crate::public_operations::{ClaimedPublicOperation, PublicOperationStore};
use crate::public_sandbox_operations::CreateSandboxOperationExecutor;
use crate::routes::AppState;

/// Kill switch for inline execution, read only while admission-time claims
/// are on. Off records claimed creates queued for the dispatcher, as before.
const NATIVE_CREATE_INLINE_ENV: &str = "SPACE_NATIVE_CREATE_INLINE_ENABLED";
const DEFAULT_NATIVE_CREATE_INLINE_ENABLED: bool = true;
/// How long the request waits for the inline execution before answering.
const NATIVE_CREATE_INLINE_WAIT_ENV: &str = "SPACE_NATIVE_CREATE_INLINE_WAIT_MS";
const DEFAULT_NATIVE_CREATE_INLINE_WAIT: Duration = Duration::from_millis(2_000);
const MAX_NATIVE_CREATE_INLINE_WAIT: Duration = Duration::from_secs(30);

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
    pub(crate) runner: InlineOperations,
}

impl InlineCreate {
    /// Inline execution through the state's controller, or `None` when the
    /// kill switch is off.
    pub(crate) fn from_env(state: &AppState) -> Option<Self> {
        if !pplx_space_common_core::env::env_flag_var(
            NATIVE_CREATE_INLINE_ENV,
            DEFAULT_NATIVE_CREATE_INLINE_ENABLED,
        ) {
            return None;
        }
        Some(Self {
            wait: inline_wait(std::env::var(NATIVE_CREATE_INLINE_WAIT_ENV).ok().as_deref()),
            executor: Arc::new(CreateSandboxOperationExecutor::new(
                state.pool.clone(),
                state.sandbox_controller.clone(),
            )),
            runner: state.inline_operations.clone(),
        })
    }

    /// Start executing `operation` under `claim_token` in its own task, and
    /// wait for it up to the deadline. Returning (or dropping this future)
    /// never stops the task. True when the task finished within the
    /// deadline, so whatever it recorded is already durable.
    pub(crate) async fn run(
        &self,
        permit: InlinePermit,
        store: PublicOperationStore,
        claim_token: String,
        operation: ClaimedPublicOperation,
    ) -> bool {
        let operation_id = operation.id;
        let execution = self.executor.execute(operation.clone());
        let task = self
            .runner
            .spawn(permit, store, claim_token, operation, execution);
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
}

/// The configured wait, falling back to the default when unset or malformed
/// and capped so a bad value cannot hold requests open.
fn inline_wait(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_NATIVE_CREATE_INLINE_WAIT)
        .min(MAX_NATIVE_CREATE_INLINE_WAIT)
}

#[cfg(test)]
mod tests;
