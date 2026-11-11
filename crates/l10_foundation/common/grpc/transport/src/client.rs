use tonic::Status;
use tracing::Instrument as _;

use crate::connect::ConnectSpan;
use crate::context::GrpcRetryContext;
use crate::deadline::deadline_exceeded_status;
use crate::policy::{DEFAULT_LOCAL_GRPC_RETRY_POLICY, GrpcRetryPolicy};
use crate::retry::{
    retry_delay_fits_budget, retry_delay_for_status, retry_reason, run_with_context,
    should_reconnect_status,
};

/// Stateful retry wrapper for long-lived generated tonic clients.
///
/// `GrpcClient` caches the generated client type `C`, not a raw
/// `tonic::transport::Channel`. The connect closure should return a fully
/// configured client, including tracing/auth layers, and should not run its
/// own retry loop. This wrapper spends one retry budget across connect
/// failures and RPC failures for each logical call.
pub struct GrpcClient<C, F> {
    service_name: &'static str,
    client: Option<C>,
    connect: F,
    retry_policy: GrpcRetryPolicy,
    connect_attempt_timeout: Option<std::time::Duration>,
}

impl<C, F> GrpcClient<C, F> {
    pub fn new(service_name: &'static str, connect: F) -> Self {
        Self {
            service_name,
            client: None,
            connect,
            retry_policy: DEFAULT_LOCAL_GRPC_RETRY_POLICY,
            connect_attempt_timeout: None,
        }
    }

    pub fn with_retry_policy(mut self, retry_policy: GrpcRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn with_connect_attempt_timeout(
        mut self,
        connect_attempt_timeout: std::time::Duration,
    ) -> Self {
        self.connect_attempt_timeout = Some(connect_attempt_timeout);
        self
    }

    pub fn clear(&mut self) {
        self.client = None;
    }

    pub fn cached_client(&self) -> Option<C>
    where
        C: Clone,
    {
        self.client.clone()
    }
}

impl<C, F, Fut, E> GrpcClient<C, F>
where
    C: Clone,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<C, E>>,
    E: std::fmt::Display,
{
    pub async fn call<T, Op, OpFut>(
        &mut self,
        command: &'static str,
        context: GrpcRetryContext,
        mut operation: Op,
    ) -> Result<T, Status>
    where
        Op: FnMut(C) -> OpFut,
        OpFut: Future<Output = Result<T, Status>>,
    {
        let started_at = tokio::time::Instant::now();
        // Connect failures and RPC failures share one budget for this logical
        // command, so a request cannot spend a full retry window reconnecting
        // and then another full window replaying the RPC.
        let mut retry_index = 0;

        loop {
            if let Some(status) = context.terminal_status(self.service_name, command) {
                return Err(status);
            }

            let client = self
                .connected_client(command, &context, started_at, &mut retry_index)
                .await?;
            match run_with_context(
                &context,
                self.service_name,
                command,
                "running RPC",
                operation(client.clone()),
            )
            .await
            {
                Ok(response) => {
                    self.client = Some(client);
                    return Ok(response);
                }
                Err(status) => {
                    let reconnect = should_reconnect_status(&status);
                    let Some(delay) =
                        retry_delay_for_status(self.retry_policy, retry_index, &status)
                    else {
                        self.cache_after_status(client, reconnect);
                        return Err(status);
                    };
                    if let Err(deadline) = retry_delay_fits_budget(
                        self.retry_policy,
                        started_at,
                        &context,
                        delay,
                        self.service_name,
                        command,
                    ) {
                        self.cache_after_status(client, reconnect);
                        return Err(deadline.unwrap_or(status));
                    }

                    retry_index += 1;
                    self.cache_after_status(client, reconnect);
                    pplx_space_grpc_metrics::client_retry(
                        self.service_name,
                        command,
                        "rpc",
                        retry_reason(&status),
                    );
                    pplx_space_observability::with_peer_service!(tracing::debug!(
                        peer.service = self.service_name,
                        command,
                        status_code = ?status.code(),
                        reconnect,
                        retry = retry_index,
                        retry_in = ?delay,
                        "gRPC command failed; will reconnect and retry"
                    ));
                    context.sleep(delay, self.service_name, command).await?;
                }
            }
        }
    }

    fn cache_after_status(&mut self, client: C, reconnect: bool) {
        if reconnect {
            self.client = None;
        } else {
            self.client = Some(client);
        }
    }

    async fn connected_client(
        &mut self,
        command: &'static str,
        context: &GrpcRetryContext,
        started_at: tokio::time::Instant,
        retry_index: &mut usize,
    ) -> Result<C, Status> {
        if let Some(client) = self.client.clone() {
            return Ok(client);
        }

        loop {
            let connect_span =
                ConnectSpan::start(self.service_name, command, None, None, *retry_index);
            match self
                .run_connect_attempt(command, context)
                .instrument(connect_span.span())
                .await
            {
                ConnectAttemptResult::Connected(client) => {
                    connect_span.record_ok();
                    return Ok(client);
                }
                ConnectAttemptResult::ConnectError(error) => {
                    let status = Status::unavailable(format!(
                        "{} connect failed: {error}",
                        self.service_name
                    ));
                    connect_span.record_status(&status);
                    let Some(delay) =
                        retry_delay_for_status(self.retry_policy, *retry_index, &status)
                    else {
                        return Err(status);
                    };
                    if let Err(deadline) = retry_delay_fits_budget(
                        self.retry_policy,
                        started_at,
                        context,
                        delay,
                        self.service_name,
                        command,
                    ) {
                        return Err(deadline.unwrap_or(status));
                    }

                    *retry_index += 1;
                    pplx_space_grpc_metrics::client_retry(
                        self.service_name,
                        command,
                        "connect",
                        "connect_error",
                    );
                    pplx_space_observability::with_peer_service!(tracing::debug!(
                        peer.service = self.service_name,
                        command,
                        retry = *retry_index,
                        retry_in = ?delay,
                        error = %error,
                        "failed to connect to gRPC service; will retry"
                    ));
                    context.sleep(delay, self.service_name, command).await?;
                }
                ConnectAttemptResult::AttemptTimeout(timeout) => {
                    let status = Status::unavailable(format!(
                        "{} connect attempt timed out after {timeout:?}",
                        self.service_name
                    ));
                    connect_span.record_status(&status);
                    let Some(delay) =
                        retry_delay_for_status(self.retry_policy, *retry_index, &status)
                    else {
                        return Err(status);
                    };
                    if let Err(deadline) = retry_delay_fits_budget(
                        self.retry_policy,
                        started_at,
                        context,
                        delay,
                        self.service_name,
                        command,
                    ) {
                        return Err(deadline.unwrap_or(status));
                    }

                    *retry_index += 1;
                    pplx_space_grpc_metrics::client_retry(
                        self.service_name,
                        command,
                        "connect",
                        "connect_timeout",
                    );
                    pplx_space_observability::with_peer_service!(tracing::debug!(
                        peer.service = self.service_name,
                        command,
                        retry = *retry_index,
                        retry_in = ?delay,
                        attempt_timeout = ?timeout,
                        "gRPC connect attempt timed out; will retry"
                    ));
                    context.sleep(delay, self.service_name, command).await?;
                }
                ConnectAttemptResult::Deadline(status) => {
                    connect_span.record_status(&status);
                    return Err(status);
                }
            }
        }
    }

    async fn run_connect_attempt(
        &mut self,
        command: &'static str,
        context: &GrpcRetryContext,
    ) -> ConnectAttemptResult<C, E> {
        let remaining = context.remaining();
        if remaining.is_some_and(|remaining| remaining.is_zero()) {
            return ConnectAttemptResult::Deadline(deadline_exceeded_status(
                self.service_name,
                command,
                "connecting",
            ));
        }

        let timeout = match (remaining, self.connect_attempt_timeout) {
            (Some(remaining), Some(attempt_timeout)) => Some(remaining.min(attempt_timeout)),
            (Some(remaining), None) => Some(remaining),
            (None, Some(attempt_timeout)) => Some(attempt_timeout),
            (None, None) => None,
        };
        let timed_out_by_attempt_timeout = match (remaining, self.connect_attempt_timeout) {
            (None, Some(_)) => true,
            (Some(remaining), Some(attempt_timeout)) => attempt_timeout < remaining,
            _ => false,
        };

        let Some(timeout) = timeout else {
            return match (self.connect)().await {
                Ok(client) => ConnectAttemptResult::Connected(client),
                Err(error) => ConnectAttemptResult::ConnectError(error),
            };
        };

        match tokio::time::timeout(timeout, (self.connect)()).await {
            Ok(Ok(client)) => ConnectAttemptResult::Connected(client),
            Ok(Err(error)) => ConnectAttemptResult::ConnectError(error),
            Err(_) if timed_out_by_attempt_timeout => ConnectAttemptResult::AttemptTimeout(timeout),
            Err(_) => ConnectAttemptResult::Deadline(deadline_exceeded_status(
                self.service_name,
                command,
                "connecting",
            )),
        }
    }
}

enum ConnectAttemptResult<C, E> {
    Connected(C),
    ConnectError(E),
    AttemptTimeout(std::time::Duration),
    Deadline(Status),
}
