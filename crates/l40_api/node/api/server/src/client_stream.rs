//! Request-scoped client-stream forwarding. Dropping the RPC drops both halves.
use futures_util::{Stream, StreamExt};
use pplx_space_common_grpc::{UploadBody, UploadBodyClosed, upload_channel};
use std::{future::Future, time::Duration};
use tonic::Status;

const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(5);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// EOF remains valid for protocols that support an empty upload.
pub(crate) async fn first<T, S>(input: &mut S, method: &str) -> Result<Option<T>, Status>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    tokio::time::timeout(FIRST_FRAME_TIMEOUT, input.next())
        .await
        .map_err(|_| Status::deadline_exceeded(format!("{method} first frame timed out")))?
        .transpose()
}

/// No forwarding task may outlive its handler. On upstream completion, input
/// failure or caller cancellation, the other future and its body are dropped.
/// Only a complete input ends the upstream body; a failed one leaves it open
/// until the dropped call resets it.
pub(crate) async fn forward<T, S, V, C, F, R>(
    mut input: S,
    first: Option<T>,
    method: &str,
    validate: V,
    call: C,
) -> Result<R, Status>
where
    T: Send + 'static,
    S: Stream<Item = Result<T, Status>> + Unpin,
    V: Fn(&T) -> Result<(), Status>,
    C: FnOnce(UploadBody<T>) -> F,
    F: Future<Output = Result<R, Status>>,
{
    let (tx, body) = upload_channel(8);
    let forward = async move {
        if let Some(first) = first
            && !within_idle(method, tx.send(first)).await?
        {
            return Ok::<(), Status>(());
        }
        loop {
            let next = tokio::time::timeout(IDLE_TIMEOUT, input.next())
                .await
                .map_err(|_| {
                    Status::deadline_exceeded(format!("{method} stream idle timed out"))
                })?;
            let Some(message) = next else {
                // A closed upstream already has its own result.
                within_idle(method, tx.finish()).await?;
                return Ok::<(), Status>(());
            };
            let message = message?;
            validate(&message)?;
            if !within_idle(method, tx.send(message)).await? {
                return Ok::<(), Status>(());
            }
        }
    };
    let upstream = call(body);
    tokio::pin!(forward, upstream);
    tokio::select! {
        biased;
        result = &mut forward => { result?; upstream.await }
        result = &mut upstream => result,
    }
}

/// Whether the upstream body accepted the frame before the idle timeout.
async fn within_idle(
    method: &str,
    sent: impl Future<Output = Result<(), UploadBodyClosed>>,
) -> Result<bool, Status> {
    tokio::time::timeout(IDLE_TIMEOUT, sent)
        .await
        .map(|result| result.is_ok())
        .map_err(|_| Status::deadline_exceeded(format!("{method} upstream stopped reading")))
}

#[cfg(test)]
mod tests;
