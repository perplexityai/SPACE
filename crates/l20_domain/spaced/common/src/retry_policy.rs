use std::time::Duration;

use pplx_space_common_core::types::parse_grpc_timeout;
use tokio_util::sync::CancellationToken;

pub const REQUEST_DEADLINE_EXCEEDED_MESSAGE: &str = "request deadline exceeded";

pub const DEFAULT_CONNECT_RETRY_POLICY: RetryPolicy = RetryPolicy {
    min_delay: Duration::from_millis(20),
    max_delay: Duration::from_secs(1),
    steps: 6,
    max_retry_count: 6,
};

pub const DEFAULT_COMMAND_RETRY_POLICY: RetryPolicy = RetryPolicy {
    min_delay: Duration::from_millis(20),
    max_delay: Duration::from_millis(100),
    steps: 3,
    max_retry_count: 3,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    pub min_delay: Duration,
    pub max_delay: Duration,
    pub steps: usize,
    pub max_retry_count: usize,
}

#[derive(Clone)]
pub struct RetryContext {
    deadline: Option<tokio::time::Instant>,
    cancellation: CancellationToken,
}

pub fn status_is_request_deadline_exceeded(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::DeadlineExceeded
        && status.message() == REQUEST_DEADLINE_EXCEEDED_MESSAGE
}

fn request_deadline_exceeded_status() -> tonic::Status {
    tonic::Status::deadline_exceeded(REQUEST_DEADLINE_EXCEEDED_MESSAGE)
}

fn timeout_deadline(timeout: Duration) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    now.checked_add(timeout).unwrap_or(now)
}

impl RetryContext {
    pub fn none() -> Self {
        Self {
            deadline: None,
            cancellation: CancellationToken::new(),
        }
    }

    /// Unrepresentable budgets fail closed as an already expired deadline.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            deadline: Some(timeout_deadline(timeout)),
            cancellation: CancellationToken::new(),
        }
    }

    pub fn with_deadline(deadline: tokio::time::Instant) -> Self {
        Self {
            deadline: Some(deadline),
            cancellation: CancellationToken::new(),
        }
    }

    pub fn limited_by_deadline(mut self, deadline: tokio::time::Instant) -> Self {
        self.deadline = Some(match self.deadline {
            Some(existing) => existing.min(deadline),
            None => deadline,
        });
        self
    }

    pub fn limited_by_timeout(self, timeout: Duration) -> Self {
        self.limited_by_deadline(timeout_deadline(timeout))
    }

    pub fn with_grpc_timeout_metadata(self, metadata: &tonic::metadata::MetadataMap) -> Self {
        let Some(timeout) = metadata
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_grpc_timeout)
        else {
            return self;
        };
        self.limited_by_timeout(timeout)
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
    }

    fn terminal_status(&self) -> Option<tonic::Status> {
        if self.cancellation.is_cancelled() {
            return Some(tonic::Status::cancelled("request cancelled"));
        }
        if matches!(self.remaining(), Some(remaining) if remaining.is_zero()) {
            return Some(request_deadline_exceeded_status());
        }
        None
    }

    async fn sleep(&self, delay: Duration) -> Result<(), tonic::Status> {
        if delay.is_zero() {
            return Ok(());
        }
        if let Some(remaining) = self.remaining() {
            if remaining.is_zero() {
                return Err(request_deadline_exceeded_status());
            }
            let sleep_for = delay.min(remaining);
            tokio::select! {
                _ = self.cancellation.cancelled() => Err(tonic::Status::cancelled("request cancelled")),
                _ = tokio::time::sleep(sleep_for) => {
                    if sleep_for < delay {
                        Err(request_deadline_exceeded_status())
                    } else {
                        Ok(())
                    }
                }
            }
        } else {
            tokio::select! {
                _ = self.cancellation.cancelled() => Err(tonic::Status::cancelled("request cancelled")),
                _ = tokio::time::sleep(delay) => Ok(()),
            }
        }
    }

    async fn run_operation<T, F>(&self, operation: F) -> Result<T, tonic::Status>
    where
        F: std::future::Future<Output = Result<T, tonic::Status>>,
    {
        if let Some(status) = self.terminal_status() {
            return Err(status);
        }
        if let Some(remaining) = self.remaining() {
            if remaining.is_zero() {
                return Err(request_deadline_exceeded_status());
            }
            tokio::select! {
                _ = self.cancellation.cancelled() => Err(tonic::Status::cancelled("request cancelled")),
                result = operation => result,
                _ = tokio::time::sleep(remaining) => Err(request_deadline_exceeded_status()),
            }
        } else {
            tokio::select! {
                _ = self.cancellation.cancelled() => Err(tonic::Status::cancelled("request cancelled")),
                result = operation => result,
            }
        }
    }
}

impl RetryPolicy {
    pub fn none() -> Self {
        Self {
            min_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            steps: 0,
            max_retry_count: 0,
        }
    }

    pub fn delay(self, retry_index: usize) -> Option<Duration> {
        if retry_index >= self.max_retry_count {
            return None;
        }

        let capped_index = retry_index.min(self.steps.saturating_sub(1));
        let multiplier = 1u32.checked_shl(capped_index as u32).unwrap_or(u32::MAX);
        Some(
            self.min_delay
                .saturating_mul(multiplier)
                .min(self.max_delay),
        )
    }

    /// Sample below the exponential envelope, preserving its configured cap.
    fn jittered_delay(self, retry_index: usize) -> Option<Duration> {
        self.delay(retry_index)
            .map(|delay| delay.saturating_sub((delay / 5).mul_f64(rand::random::<f64>())))
    }

    pub fn delays(self) -> Vec<Duration> {
        (0..self.max_retry_count)
            .filter_map(|retry_index| self.delay(retry_index))
            .collect()
    }
}

pub async fn retry_status<T, F, Fut>(
    service_name: &'static str,
    command: &'static str,
    retry_policy: RetryPolicy,
    operation: F,
) -> Result<T, tonic::Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, tonic::Status>>,
{
    retry_status_with_context_and_retry_hook(
        service_name,
        command,
        retry_policy,
        RetryContext::none(),
        operation,
        || async {},
    )
    .await
}

pub async fn retry_status_with_context<T, F, Fut>(
    service_name: &'static str,
    command: &'static str,
    retry_policy: RetryPolicy,
    context: RetryContext,
    operation: F,
) -> Result<T, tonic::Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, tonic::Status>>,
{
    retry_status_with_context_and_retry_hook(
        service_name,
        command,
        retry_policy,
        context,
        operation,
        || async {},
    )
    .await
}

pub async fn retry_status_with_context_and_retry_hook<T, F, Fut, H, HookFut>(
    service_name: &'static str,
    command: &'static str,
    retry_policy: RetryPolicy,
    context: RetryContext,
    mut operation: F,
    mut on_retry: H,
) -> Result<T, tonic::Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, tonic::Status>>,
    H: FnMut() -> HookFut,
    HookFut: std::future::Future<Output = ()>,
{
    let mut retry_index = 0;

    loop {
        if let Some(status) = context.terminal_status() {
            return Err(status);
        }
        match context.run_operation(operation()).await {
            Ok(response) => {
                return Ok(response);
            }
            Err(status) if crate::status_is_retryable(&status) => {
                on_retry().await;
                let Some(delay) = retry_policy.jittered_delay(retry_index) else {
                    return Err(status);
                };
                retry_index += 1;
                tracing::debug!(
                    command,
                    status_code = ?status.code(),
                    retry = retry_index,
                    retry_in = ?delay,
                    "{service_name} command failed; will retry"
                );
                context.sleep(delay).await?;
            }
            Err(status) => return Err(status),
        }
    }
}

// Initial establishment has no dispatched RPC to replay. UDS, attested UDS,
// and vsock share this loop while retaining their distinct connector contracts.
pub(crate) async fn retry_connect<T, E, F, Fut>(
    service: &'static str,
    policy: RetryPolicy,
    mut connect: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut retry = 0;
    loop {
        match connect().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let Some(delay) = policy.jittered_delay(retry) else {
                    return Err(error);
                };
                retry += 1;
                tracing::debug!(service, retry, retry_in = ?delay, "connection establishment will retry");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
