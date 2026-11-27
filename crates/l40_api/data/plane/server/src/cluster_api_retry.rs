//! Sending a route resolve to ClusterApi again when this client's transport
//! to it failed, as it does while a cluster-api replica restarts.
//!
//! A ClusterApi replica that receives SIGTERM sends GOAWAY and stops
//! accepting. A resolve that races the GOAWAY, or that the channel dials
//! before Kubernetes has moved the Service off the old replica, fails here
//! without an answer, while every other replica can serve it. Sending it
//! again reaches a replica that answers.
//!
//! Only a status the client made from its own transport failure is sent again
//! ([`pplx_space_common_grpc::is_client_transport_failure`]). A status the
//! server answered, its `Unavailable` route-not-ready verdicts included, goes
//! back to the caller as before. The resolves this covers are safe to send
//! twice even when the first reached a replica: each is a read of the
//! sandbox's current route plus the preparation the data plane already
//! repeats on every route-cache miss, and a wake that preparation asks for
//! is queued or joined, never started twice.

use std::future::Future;
use std::time::Duration;

/// How long a resolve keeps sending again after transport failures, measured
/// from its first attempt. A replica's restart moves its clients to the
/// others in well under this; a ClusterApi that stays unreachable for longer
/// is an outage, which the caller reports as it did before.
pub(super) const CLUSTER_API_TRANSPORT_RETRY_WINDOW: Duration = Duration::from_secs(5);

const CLUSTER_API_TRANSPORT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(50);
const CLUSTER_API_TRANSPORT_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

/// Run `call` until it returns anything but a client transport failure, or
/// until another attempt would start after `deadline`. The last result is
/// returned as it came, so a resolve that never got through fails with the
/// status it failed with before.
pub(super) async fn retry_cluster_api_transport<T, F, Fut>(
    operation: &'static str,
    deadline: tokio::time::Instant,
    mut call: F,
) -> Result<T, tonic::Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    let mut delay = CLUSTER_API_TRANSPORT_RETRY_INITIAL_DELAY;
    let mut retries = 0_u32;
    loop {
        match call().await {
            Err(status)
                if pplx_space_common_grpc::is_client_transport_failure(&status)
                    && tokio::time::Instant::now() + delay < deadline =>
            {
                retries += 1;
                tracing::debug!(
                    operation,
                    retries,
                    retry_in_ms = delay.as_millis() as u64,
                    code = ?status.code(),
                    %status,
                    "cluster api transport failed; sending the call again"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(CLUSTER_API_TRANSPORT_RETRY_MAX_DELAY);
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests;
