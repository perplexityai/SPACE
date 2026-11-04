use std::time::Duration;

use pplx_space_common_core::types::parse_grpc_timeout;
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::deadline::deadline_exceeded_status;

#[derive(Clone, Debug)]
pub struct GrpcRetryContext {
    deadline: Option<tokio::time::Instant>,
}

impl GrpcRetryContext {
    pub fn none() -> Self {
        Self { deadline: None }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            deadline: Some(now.checked_add(timeout).unwrap_or(now)),
        }
    }

    pub fn with_grpc_timeout_metadata(mut self, metadata: &MetadataMap) -> Self {
        let Some(timeout) = metadata
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_grpc_timeout)
        else {
            return self;
        };
        let now = tokio::time::Instant::now();
        let deadline = now.checked_add(timeout).unwrap_or(now);
        self.deadline = Some(
            self.deadline
                .map_or(deadline, |existing| existing.min(deadline)),
        );
        self
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
    }

    pub(crate) fn terminal_status(
        &self,
        service_name: &'static str,
        command: &'static str,
    ) -> Option<Status> {
        if matches!(self.remaining(), Some(remaining) if remaining.is_zero()) {
            return Some(deadline_exceeded_status(
                service_name,
                command,
                "starting attempt",
            ));
        }
        None
    }

    pub(crate) async fn sleep(
        &self,
        delay: Duration,
        service_name: &'static str,
        command: &'static str,
    ) -> Result<(), Status> {
        if delay.is_zero() {
            return Ok(());
        }
        if let Some(remaining) = self.remaining()
            && (remaining.is_zero() || delay > remaining)
        {
            return Err(deadline_exceeded_status(
                service_name,
                command,
                "waiting to retry",
            ));
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }
}
