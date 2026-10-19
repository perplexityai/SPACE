use std::io;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use pplx_space_metrics::gateway::DnsAttributionCacheObservationSource;
use pplx_space_net_common::NetworkPolicyV1;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::sync::CancellationToken;

use crate::reserved_dns::{MAX_DNS_MESSAGE_BYTES, ReservedResolver, ResolveOutcome};

use super::{FlowOutcome, RelayFailure, TcpProxy};

/// Bounds an idle, fragmented, or deliberately incomplete DNS frame without
/// imposing a lifetime limit on a connection that keeps completing messages.
const FRAME_IO_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) async fn serve<T>(
    proxy: &TcpProxy,
    client: T,
    resolver: ReservedResolver,
    admitted_policy: &NetworkPolicyV1,
    shutdown: &CancellationToken,
) -> FlowOutcome
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut framed = LengthDelimitedCodec::builder()
        .length_field_type::<u16>()
        .max_frame_length(MAX_DNS_MESSAGE_BYTES)
        .new_framed(client);
    let mut messages = 0_u32;
    let mut wire_tx = 0_u64;
    let mut wire_rx = 0_u64;
    loop {
        let frame = tokio::select! {
            biased;
            () = shutdown.cancelled() => return FlowOutcome::Cancelled,
            result = tokio::time::timeout(FRAME_IO_TIMEOUT, framed.next()) => result,
        };
        let wire = match frame {
            Err(_) | Ok(None) | Ok(Some(Err(_))) => {
                return FlowOutcome::HandledDns {
                    messages,
                    wire_tx,
                    wire_rx,
                };
            }
            Ok(Some(Ok(wire))) if wire.is_empty() => {
                return FlowOutcome::HandledDns {
                    messages,
                    wire_tx,
                    wire_rx,
                };
            }
            Ok(Some(Ok(wire))) => wire,
        };
        wire_tx = wire_tx.saturating_add(wire.len() as u64 + 2);

        let reply = match proxy
            .reserved_dns
            .resolve(
                proxy.sandbox_id,
                resolver,
                wire.as_ref(),
                admitted_policy,
                shutdown,
            )
            .await
        {
            ResolveOutcome::Reply(reply) => reply,
            ResolveOutcome::Cancelled => return FlowOutcome::Cancelled,
        };
        if reply.is_empty() {
            return FlowOutcome::RelayFailed(RelayFailure::Transfer {
                error: io::Error::other("reserved DNS returned an empty reply"),
                fault: None,
            });
        }
        if shutdown.is_cancelled() {
            return FlowOutcome::Cancelled;
        }
        let accepted = proxy
            .network_state
            .dns_allow_cache()
            .observe_response(&reply, admitted_policy);
        pplx_space_metrics::gateway::dns_attribution_cache_observation(
            DnsAttributionCacheObservationSource::ReservedDns,
            accepted,
        );

        let reply_wire_bytes = reply.len() as u64 + 2;
        let sent = tokio::select! {
            biased;
            () = shutdown.cancelled() => return FlowOutcome::Cancelled,
            result = tokio::time::timeout(FRAME_IO_TIMEOUT, framed.send(Bytes::from(reply))) => result,
        };
        match sent {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return FlowOutcome::RelayFailed(RelayFailure::Transfer { error, fault: None });
            }
            Err(_) => {
                return FlowOutcome::RelayFailed(RelayFailure::Transfer {
                    error: io::Error::new(
                        io::ErrorKind::TimedOut,
                        "reserved DNS reply write timed out",
                    ),
                    fault: None,
                });
            }
        }
        wire_rx = wire_rx.saturating_add(reply_wire_bytes);
        messages = messages.saturating_add(1);
    }
}

#[cfg(test)]
mod tests;
