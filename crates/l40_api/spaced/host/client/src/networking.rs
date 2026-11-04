//! Loopback bridges between the host and the guest.

use super::*;

impl SpacedHostClient {
    /// Open the in-VM loopback bridge for `port`, blocking server-side for
    /// up to `wait_timeout` for the listener to appear.
    ///
    /// Bypasses the standard `command_retry_policy` because (a) the
    /// service-side wait already absorbs transient "listener not yet up"
    /// states by design, and (b) re-sending the same `wait_timeout` on
    /// each retry would compound the budget (2s × N) without changing the
    /// outcome — `WaitingForListener` is a "the port is armed; reconnect"
    /// signal, not a transient transport error.
    pub async fn open_loopback_bridge(
        &self,
        port: u32,
        wait_timeout: Option<std::time::Duration>,
    ) -> Result<proto::OpenLoopbackBridgeResponse, tonic::Status> {
        let wait_timeout_proto = wait_timeout
            .map(pplx_space_spaced_common::duration_to_proto)
            .transpose()?;
        let mut client = self.networking.clone();
        client
            .open_loopback_bridge(proto::OpenLoopbackBridgeRequest {
                port,
                wait_timeout: wait_timeout_proto,
            })
            .await
            .map(Response::into_inner)
    }

    /// Close authorization for `port` with a single dispatch. Client construction
    /// retries connection establishment, but a lost close response is ambiguous:
    /// replay could close a replacement bridge opened on the same port.
    pub async fn close_loopback_bridge(
        &self,
        port: u32,
    ) -> Result<proto::CloseLoopbackBridgeResponse, tonic::Status> {
        self.call_networking_once("close_loopback_bridge", move |mut client| async move {
            client
                .close_loopback_bridge(proto::CloseLoopbackBridgeRequest { port })
                .await
        })
        .await
    }
}
